"""
ContinuousBatchScheduler — iteration-level batching with preemption.

Unlike static batching (wait for a full batch, decode all, return),
continuous batching decides at *every forward pass* which sequences to run.
New sequences join the batch mid-decode; finished sequences leave immediately.

States:
  WAITING  → queued, not yet allocated
  RUNNING  → has GPU pages, in the current batch
  SWAPPED  → pages moved to CPU, waiting for GPU space
  FINISHED → done, pages freed
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum

from kvstream.memory.block_manager import BlockManager


class SeqStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    SWAPPED = "swapped"
    FINISHED = "finished"
    ABORTED = "aborted"


@dataclass
class SequenceGroup:
    """One user request = one sequence group (1:1 for now; 1:N for beam search)."""

    seq_id: str
    prompt_tokens: list[int]
    max_new_tokens: int
    arrival_time: float = field(default_factory=time.monotonic)

    status: SeqStatus = SeqStatus.WAITING
    output_tokens: list[int] = field(default_factory=list)
    output_queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    # Signalled by the scheduler when this sequence is admitted to the
    # running batch (KV pages allocated, backend call may proceed).
    admission: asyncio.Event = field(default_factory=asyncio.Event)

    # Set when the sequence is preempted mid-decode
    preempted_at_token: int = 0

    @property
    def prompt_len(self) -> int:
        return len(self.prompt_tokens)

    @property
    def num_generated(self) -> int:
        return len(self.output_tokens)

    @property
    def total_tokens(self) -> int:
        return self.prompt_len + self.num_generated

    @property
    def is_finished(self) -> bool:
        return self.status in (SeqStatus.FINISHED, SeqStatus.ABORTED)


@dataclass
class SchedulerOutput:
    """What the engine should do on this iteration."""

    prefill: list[SequenceGroup]  # full prompt pass
    decode: list[SequenceGroup]  # single-token decode step
    swapped_in: list[SequenceGroup]
    swapped_out: list[SequenceGroup]
    ignored: list[SequenceGroup]  # waiting, no room


class ContinuousBatchScheduler:
    def __init__(
        self,
        block_manager: BlockManager,
        max_batch_size: int = 16,
        max_waiting_tokens: int = 4096,
        preemption_policy: str = "swap",
        priority: str = "fcfs",
        allow_preemption: bool = True,
    ) -> None:
        self.bm = block_manager
        self.max_batch_size = max_batch_size
        self.max_waiting_tokens = max_waiting_tokens
        self.preemption_policy = preemption_policy
        self.priority = priority
        # Preemption (swap-out / recompute) only makes sense when KVStream
        # actually owns the backend's KV tensors — i.e. hard-inject backends
        # like llama.cpp. For soft-inject backends (Ollama, Foundry, LM Studio)
        # we cannot pause an in-flight stream, so evicting a "running" sequence
        # is bookkeeping that the backend ignores. In that mode we leave running
        # sequences alone and simply queue waiting requests until pages free.
        self.allow_preemption = allow_preemption

        self.waiting: list[SequenceGroup] = []
        self.running: list[SequenceGroup] = []
        self.swapped: list[SequenceGroup] = []

        self._lock = asyncio.Lock()

    async def add_request(self, seq: SequenceGroup) -> None:
        async with self._lock:
            self.waiting.append(seq)
            if self.priority == "sjf":
                self.waiting.sort(key=lambda s: s.prompt_len)

    async def abort_request(self, seq_id: str) -> None:
        async with self._lock:
            for queue in (self.waiting, self.running, self.swapped):
                for seq in queue:
                    if seq.seq_id == seq_id:
                        seq.status = SeqStatus.ABORTED
                        self.bm.free(seq_id)
                        queue.remove(seq)
                        return

    async def schedule(self) -> SchedulerOutput:
        """
        Produce the next batch. Called once per forward-pass iteration.
        """
        async with self._lock:
            swapped_in: list[SequenceGroup] = []
            swapped_out: list[SequenceGroup] = []
            ignored: list[SequenceGroup] = []

            # --- Step 1: Preempt running sequences if needed ---
            # Remove finished sequences first
            for seq in list(self.running):
                if seq.is_finished:
                    self.bm.free(seq.seq_id)
                    self.running.remove(seq)

            # If GPU is full, preempt lowest-priority running seq. Skipped for
            # soft-inject backends, where a "swapped-out" sequence would keep
            # streaming anyway — there the waiting request just stays queued
            # (Step 3 below) until a running sequence finishes and frees pages.
            while (
                self.allow_preemption
                and self.waiting
                and not self.bm.can_allocate(self.waiting[0].prompt_len)
                and self.running
            ):
                victim = self._pick_preemption_victim()
                if self.preemption_policy == "swap":
                    self.bm.swap_out(victim.seq_id)
                    victim.status = SeqStatus.SWAPPED
                    self.swapped.append(victim)
                    swapped_out.append(victim)
                else:
                    # Recompute: free pages, re-queue
                    self.bm.free(victim.seq_id)
                    victim.status = SeqStatus.WAITING
                    victim.output_tokens = []
                    self.waiting.insert(0, victim)
                self.running.remove(victim)

            # --- Step 2: Swap previously swapped sequences back in ---
            for seq in list(self.swapped):
                if len(self.running) >= self.max_batch_size:
                    break
                if self.bm.can_allocate(seq.total_tokens):
                    self.bm.swap_in(seq.seq_id)
                    seq.status = SeqStatus.RUNNING
                    seq.admission.set()
                    self.running.append(seq)
                    self.swapped.remove(seq)
                    swapped_in.append(seq)

            # --- Step 3: Promote waiting sequences ---
            # A sequence that already holds pages (forked from a cached
            # prefix) is admitted without re-allocating.
            while self.waiting and len(self.running) < self.max_batch_size:
                seq = self.waiting[0]
                if self.bm.has_sequence(seq.seq_id) or self.bm.allocate(seq.seq_id, seq.prompt_len):
                    seq.status = SeqStatus.RUNNING
                    seq.admission.set()
                    self.running.append(self.waiting.pop(0))
                else:
                    ignored.append(seq)
                    break

            prefill = [s for s in self.running if s.num_generated == 0]
            decode = [s for s in self.running if s.num_generated > 0]

            return SchedulerOutput(
                prefill=prefill,
                decode=decode,
                swapped_in=swapped_in,
                swapped_out=swapped_out,
                ignored=ignored,
            )

    def _pick_preemption_victim(self) -> SequenceGroup:
        """
        Pick the sequence to preempt. Default: longest-running (most tokens used).
        This is the sequence least likely to finish soon.
        """
        return max(self.running, key=lambda s: s.total_tokens)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "waiting": len(self.waiting),
            "running": len(self.running),
            "swapped": len(self.swapped),
            "gpu_blocks_free": self.bm.num_free_gpu_blocks(),
            "gpu_utilization": round(self.bm.utilization(), 3),
        }
