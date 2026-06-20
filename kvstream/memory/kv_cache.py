"""
PagedKVCache — pre-allocated tensor slab for KV cache pages.

Only instantiated for backends that support hard KV inject (llama.cpp).
For soft-inject backends (Ollama, Foundry, LM Studio) the backend manages
its own KV cache internally — this class is never created.

Shape: [num_layers, 2, num_blocks, block_size, num_heads, head_dim]
  dim 0 : transformer layer index
  dim 1 : 0 = keys, 1 = values
  dim 2 : physical block ID
  dim 3 : token slot within block
  dim 4 : attention head
  dim 5 : head dimension
"""

from __future__ import annotations

import logging

import torch

logger = logging.getLogger("kvstream.memory.kv_cache")


def _dtype(name: str) -> torch.dtype:
    return {"float16": torch.float16, "bfloat16": torch.bfloat16, "float32": torch.float32}.get(
        name, torch.float16
    )


class PagedKVCache:
    def __init__(
        self,
        num_layers: int,
        num_heads: int,
        head_dim: int,
        num_gpu_blocks: int,
        num_cpu_blocks: int,
        block_size: int = 16,
        dtype: torch.dtype | str = torch.float16,
    ) -> None:
        self.num_layers = num_layers
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.block_size = block_size
        self.dtype = _dtype(dtype) if isinstance(dtype, str) else dtype

        cuda_available = torch.cuda.is_available()
        device = "cuda" if cuda_available else "cpu"

        # --- GPU (or CPU fallback) primary slab ---
        gpu_shape = (num_layers, 2, num_gpu_blocks, block_size, num_heads, head_dim)
        gpu_bytes = 1
        for d in gpu_shape:
            gpu_bytes *= d
        gpu_bytes *= torch.finfo(self.dtype).bits // 8

        logger.info(
            f"Allocating GPU KV slab: {num_gpu_blocks} blocks "
            f"on {device}  ({gpu_bytes / 1024**2:.1f} MB)"
        )
        self.gpu_cache = torch.zeros(gpu_shape, dtype=self.dtype, device=device)

        # --- CPU swap slab ---
        # pin_memory is only supported on CUDA-enabled machines and not on
        # Windows CPU-only builds — guard carefully.
        cpu_shape = (num_layers, 2, num_cpu_blocks, block_size, num_heads, head_dim)
        cpu_bytes = 1
        for d in cpu_shape:
            cpu_bytes *= d
        cpu_bytes *= torch.finfo(self.dtype).bits // 8

        pin = cuda_available
        if pin:
            try:
                self.cpu_cache = torch.zeros(
                    cpu_shape, dtype=self.dtype, device="cpu", pin_memory=True
                )
            except Exception as e:
                logger.warning(f"Pinned CPU allocation failed ({e}), falling back to pageable.")
                pin = False

        if not pin:
            self.cpu_cache = torch.zeros(cpu_shape, dtype=self.dtype, device="cpu")

        logger.info(
            f"Allocating CPU swap slab: {num_cpu_blocks} blocks "
            f"({'pinned' if pin else 'pageable'})  ({cpu_bytes / 1024**2:.1f} MB)"
        )

        self._cpu_block_offset = num_gpu_blocks

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def write_kv(
        self,
        layer_idx: int,
        block_id: int,
        slot: int,
        key: torch.Tensor,  # [num_heads, head_dim]
        value: torch.Tensor,  # [num_heads, head_dim]
    ) -> None:
        cache = self._cache_for(block_id)
        local_id = self._local_id(block_id)
        cache[layer_idx, 0, local_id, slot] = key
        cache[layer_idx, 1, local_id, slot] = value

    def write_kv_batch(
        self,
        layer_idx: int,
        block_ids: list[int],
        slots: list[int],
        keys: torch.Tensor,  # [batch, num_heads, head_dim]
        values: torch.Tensor,  # [batch, num_heads, head_dim]
    ) -> None:
        local_ids = torch.tensor(block_ids, dtype=torch.long, device=self.gpu_cache.device)
        slot_ids = torch.tensor(slots, dtype=torch.long, device=self.gpu_cache.device)
        self.gpu_cache[layer_idx, 0, local_ids, slot_ids] = keys
        self.gpu_cache[layer_idx, 1, local_ids, slot_ids] = values

    # ------------------------------------------------------------------
    # Read / gather path
    # ------------------------------------------------------------------

    def gather(
        self,
        layer_idx: int,
        block_table: list[int],
        num_tokens: int | None = None,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """
        Gather all KV pages for a sequence into contiguous tensors.
        Returns keys, values of shape [seq_len, num_heads, head_dim].
        """
        local_ids = torch.tensor(
            [self._local_id(b) for b in block_table],
            dtype=torch.long,
            device=self.gpu_cache.device,
        )
        k_pages = self.gpu_cache[layer_idx, 0][local_ids]
        v_pages = self.gpu_cache[layer_idx, 1][local_ids]

        seq_len_max = k_pages.shape[0] * self.block_size
        k = k_pages.reshape(seq_len_max, self.num_heads, self.head_dim)
        v = v_pages.reshape(seq_len_max, self.num_heads, self.head_dim)

        if num_tokens is not None:
            k = k[:num_tokens]
            v = v[:num_tokens]
        return k, v

    # ------------------------------------------------------------------
    # Swap helpers
    # ------------------------------------------------------------------

    def copy_blocks(self, src_to_dst: dict[int, int]) -> None:
        """Copy block contents during GPU↔CPU swap — batched per transfer direction."""
        if not src_to_dst:
            return
        cpu_off = self._cpu_block_offset
        gpu_to_gpu: list[tuple[int, int]] = []
        gpu_to_cpu: list[tuple[int, int]] = []
        cpu_to_gpu: list[tuple[int, int]] = []
        for src, dst in src_to_dst.items():
            if src < cpu_off and dst < cpu_off:
                gpu_to_gpu.append((src, dst))
            elif src < cpu_off:
                gpu_to_cpu.append((src, dst - cpu_off))
            else:
                cpu_to_gpu.append((src - cpu_off, dst))

        if gpu_to_gpu:
            s_ids = [s for s, _ in gpu_to_gpu]
            d_ids = [d for _, d in gpu_to_gpu]
            self.gpu_cache[:, :, d_ids] = self.gpu_cache[:, :, s_ids]
        if gpu_to_cpu:
            s_ids = [s for s, _ in gpu_to_cpu]
            d_ids = [d for _, d in gpu_to_cpu]
            self.cpu_cache[:, :, d_ids] = self.gpu_cache[:, :, s_ids].cpu()
        if cpu_to_gpu:
            s_ids = [s for s, _ in cpu_to_gpu]
            d_ids = [d for _, d in cpu_to_gpu]
            self.gpu_cache[:, :, d_ids] = self.cpu_cache[:, :, s_ids].to(self.gpu_cache.device)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def gpu_memory_bytes(self) -> int:
        return self.gpu_cache.element_size() * self.gpu_cache.nelement()

    def cpu_memory_bytes(self) -> int:
        return self.cpu_cache.element_size() * self.cpu_cache.nelement()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _cache_for(self, block_id: int) -> torch.Tensor:
        return self.cpu_cache if block_id >= self._cpu_block_offset else self.gpu_cache

    def _local_id(self, block_id: int) -> int:
        return block_id - self._cpu_block_offset if block_id >= self._cpu_block_offset else block_id

    def _pad(self, tensor: torch.Tensor, max_len: int) -> torch.Tensor:
        if tensor.shape[0] == max_len:
            return tensor
        pad = torch.zeros(
            max_len - tensor.shape[0], *tensor.shape[1:], dtype=tensor.dtype, device=tensor.device
        )
        return torch.cat([tensor, pad], dim=0)
