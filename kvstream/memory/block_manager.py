"""
BlockManager — core paged memory allocator for KVStream.

Manages a pool of fixed-size physical blocks (pages). Each block holds
KV vectors for `block_size` tokens. Sequences build a logical→physical
mapping (block table) so their KV cache can be non-contiguous in VRAM.

Key properties:
  - O(1) alloc and free via free-list
  - Copy-on-write for prefix sharing (ref_count > 1)
  - CPU swap buffer for preempted sequences
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class PhysicalBlock:
    block_id: int
    device: str = "gpu"  # "gpu" | "cpu"
    ref_count: int = 0
    # Hash of the token content — used for prefix dedup
    content_hash: int | None = None


@dataclass
class SequenceBlockState:
    seq_id: str
    # Ordered list of physical block IDs (logical block 0 → physical[0], etc.)
    logical_to_physical: list[int] = field(default_factory=list)
    num_tokens: int = 0

    @property
    def num_logical_blocks(self) -> int:
        return len(self.logical_to_physical)

    @property
    def last_block_id(self) -> int | None:
        return self.logical_to_physical[-1] if self.logical_to_physical else None


class BlockManager:
    """
    Manages two pools of physical blocks: GPU (primary) and CPU (swap).
    Thread-safe via a reentrant lock.
    """

    def __init__(
        self,
        num_gpu_blocks: int,
        num_cpu_blocks: int,
        block_size: int = 16,
    ) -> None:
        self.block_size = block_size
        self._lock = threading.RLock()

        # GPU pool
        self._gpu_blocks: dict[int, PhysicalBlock] = {
            i: PhysicalBlock(block_id=i, device="gpu") for i in range(num_gpu_blocks)
        }
        self._gpu_free: list[int] = list(range(num_gpu_blocks))

        # CPU swap pool (offset IDs beyond GPU range)
        cpu_offset = num_gpu_blocks
        self._cpu_blocks: dict[int, PhysicalBlock] = {
            cpu_offset + i: PhysicalBlock(block_id=cpu_offset + i, device="cpu")
            for i in range(num_cpu_blocks)
        }
        self._cpu_free: list[int] = list(range(cpu_offset, cpu_offset + num_cpu_blocks))

        # Sequence block tables
        self._seq_states: dict[str, SequenceBlockState] = {}

    # ------------------------------------------------------------------
    # Public allocation API
    # ------------------------------------------------------------------

    def can_allocate(self, num_tokens: int, device: str = "gpu") -> bool:
        """Check if enough free blocks exist for num_tokens."""
        pages = self._pages_for(num_tokens)
        with self._lock:
            free = self._gpu_free if device == "gpu" else self._cpu_free
            return len(free) >= pages

    def allocate(self, seq_id: str, num_tokens: int, device: str = "gpu") -> bool:
        """
        Allocate pages for a new sequence.
        Returns False if the pool is exhausted (caller should preempt).
        """
        pages = self._pages_for(num_tokens)
        with self._lock:
            free = self._gpu_free if device == "gpu" else self._cpu_free
            pool = self._gpu_blocks if device == "gpu" else self._cpu_blocks

            if len(free) < pages:
                return False

            state = SequenceBlockState(seq_id=seq_id)
            for _ in range(pages):
                block_id = free.pop()
                pool[block_id].ref_count = 1
                state.logical_to_physical.append(block_id)

            state.num_tokens = num_tokens
            self._seq_states[seq_id] = state
            return True

    def append_slot(self, seq_id: str) -> tuple[int, int]:
        """
        Called before writing a new token's KV.
        Returns (physical_block_id, slot_within_block).
        Allocates a new page if the current one is full.
        Raises MemoryError if GPU pool is exhausted.
        """
        with self._lock:
            state = self._seq_states[seq_id]
            slot = state.num_tokens % self.block_size

            if slot == 0:
                # Need a fresh GPU page
                if not self._gpu_free:
                    raise MemoryError(f"GPU block pool exhausted for seq {seq_id}")
                block_id = self._gpu_free.pop()
                self._gpu_blocks[block_id].ref_count = 1
                state.logical_to_physical.append(block_id)

            state.num_tokens += 1
            physical_id = state.logical_to_physical[-1]
            return physical_id, slot

    def free(self, seq_id: str) -> None:
        """Return all pages held by a finished sequence."""
        with self._lock:
            state = self._seq_states.pop(seq_id, None)
            if state is None:
                return
            for block_id in state.logical_to_physical:
                self._release_block(block_id)

    # ------------------------------------------------------------------
    # Copy-on-write (prefix sharing)
    # ------------------------------------------------------------------

    def fork(self, parent_seq_id: str, child_seq_id: str, num_blocks: int | None = None) -> None:
        """
        Share the parent's block table with a child (for prefix caching).
        Uses copy-on-write: blocks are shared until one of them writes.
        If num_blocks is given, only the first num_blocks pages are shared
        (partial prefix match).
        """
        with self._lock:
            parent = self._seq_states[parent_seq_id]
            shared = (
                parent.logical_to_physical[:num_blocks]
                if num_blocks is not None
                else list(parent.logical_to_physical)
            )
            num_tokens = (
                len(shared) * self.block_size if num_blocks is not None else parent.num_tokens
            )
            child = SequenceBlockState(
                seq_id=child_seq_id,
                logical_to_physical=list(shared),
                num_tokens=min(num_tokens, parent.num_tokens),
            )
            # Increment ref counts on all shared blocks
            for block_id in child.logical_to_physical:
                block = self._gpu_blocks.get(block_id) or self._cpu_blocks[block_id]
                block.ref_count += 1
            self._seq_states[child_seq_id] = child

    def cow_write(self, seq_id: str, logical_block_idx: int) -> int:
        """
        If the logical block is shared (ref_count > 1), allocate a new
        private copy and return its ID. Otherwise return existing ID.
        """
        with self._lock:
            state = self._seq_states[seq_id]
            old_id = state.logical_to_physical[logical_block_idx]
            block = self._gpu_blocks.get(old_id) or self._cpu_blocks[old_id]

            if block.ref_count <= 1:
                return old_id  # already private

            # Allocate a new block and copy content later (caller's responsibility)
            if not self._gpu_free:
                raise MemoryError("GPU block pool exhausted during CoW")
            new_id = self._gpu_free.pop()
            self._gpu_blocks[new_id].ref_count = 1
            state.logical_to_physical[logical_block_idx] = new_id

            # Release old shared block
            block.ref_count -= 1
            if block.ref_count == 0:
                self._gpu_free.append(old_id)

            return new_id

    # ------------------------------------------------------------------
    # Swap (GPU ↔ CPU)
    # ------------------------------------------------------------------

    def swap_out(self, seq_id: str) -> dict[int, int]:
        """
        Move a sequence's blocks from GPU to CPU.
        Returns mapping of {gpu_block_id: cpu_block_id}.
        """
        with self._lock:
            state = self._seq_states[seq_id]
            mapping: dict[int, int] = {}

            for i, gpu_id in enumerate(state.logical_to_physical):
                if not self._cpu_free:
                    raise MemoryError("CPU swap buffer exhausted")
                cpu_id = self._cpu_free.pop()
                self._cpu_blocks[cpu_id].ref_count = 1
                self._gpu_blocks[gpu_id].ref_count -= 1
                if self._gpu_blocks[gpu_id].ref_count == 0:
                    self._gpu_free.append(gpu_id)
                state.logical_to_physical[i] = cpu_id
                mapping[gpu_id] = cpu_id

            return mapping

    def swap_in(self, seq_id: str) -> dict[int, int]:
        """
        Move a sequence's blocks back from CPU to GPU.
        Returns mapping of {cpu_block_id: gpu_block_id}.
        """
        with self._lock:
            state = self._seq_states[seq_id]
            mapping: dict[int, int] = {}

            for i, cpu_id in enumerate(state.logical_to_physical):
                if not self._gpu_free:
                    raise MemoryError("GPU block pool exhausted during swap-in")
                gpu_id = self._gpu_free.pop()
                self._gpu_blocks[gpu_id].ref_count = 1
                self._cpu_blocks[cpu_id].ref_count -= 1
                if self._cpu_blocks[cpu_id].ref_count == 0:
                    self._cpu_free.append(cpu_id)
                state.logical_to_physical[i] = gpu_id
                mapping[cpu_id] = gpu_id

            return mapping

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_block_table(self, seq_id: str) -> list[int]:
        """Return the physical block IDs for a sequence (for attention kernel)."""
        with self._lock:
            return list(self._seq_states[seq_id].logical_to_physical)

    def has_sequence(self, seq_id: str) -> bool:
        """True if the sequence already holds a block table (e.g. prefix fork)."""
        with self._lock:
            return seq_id in self._seq_states

    def num_free_gpu_blocks(self) -> int:
        with self._lock:
            return len(self._gpu_free)

    def num_free_cpu_blocks(self) -> int:
        with self._lock:
            return len(self._cpu_free)

    def utilization(self) -> float:
        """GPU block utilization (0.0–1.0)."""
        with self._lock:
            total = len(self._gpu_blocks)
            used = total - len(self._gpu_free)
            return used / total if total > 0 else 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _pages_for(self, num_tokens: int) -> int:
        return max(1, (num_tokens + self.block_size - 1) // self.block_size)

    def _release_block(self, block_id: int) -> None:
        """Decrement ref count and return to free list if zero."""
        block = self._gpu_blocks.get(block_id) or self._cpu_blocks.get(block_id)
        if block is None:
            return
        block.ref_count -= 1
        if block.ref_count <= 0:
            if block.device == "gpu":
                self._gpu_free.append(block_id)
            else:
                self._cpu_free.append(block_id)
