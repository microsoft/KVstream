"""
Naive paged attention kernel — pure PyTorch reference implementation.

This is the fallback when flash-attn and xformers are not available.
It's correct but not optimised: O(n²) memory, standard softmax attention.

For production use, install flash-attn:
    pip install kvstream[flash]
"""

from __future__ import annotations

import math

import torch
import torch.nn.functional as F


def paged_attention_naive(
    query: torch.Tensor,  # [batch, num_heads, head_dim]
    key_cache: torch.Tensor,  # [batch, seq_len, num_heads, head_dim]
    value_cache: torch.Tensor,  # [batch, seq_len, num_heads, head_dim]
    seq_lens: list[int],
    scale: float | None = None,
) -> torch.Tensor:
    """
    Multi-head attention over paged KV cache tensors.

    Args:
        query:       Current query token per sequence in the batch
        key_cache:   All keys gathered from paged blocks, padded to max_seq_len
        value_cache: All values gathered from paged blocks, padded to max_seq_len
        seq_lens:    Actual token count per sequence (for masking padding)
        scale:       1 / sqrt(head_dim) — computed if None

    Returns:
        output: [batch, num_heads, head_dim]
    """
    batch, num_heads, head_dim = query.shape
    max_seq_len = key_cache.shape[1]

    if scale is None:
        scale = 1.0 / math.sqrt(head_dim)

    # Reshape query: [batch, num_heads, 1, head_dim]
    q = query.unsqueeze(2)

    # key_cache: [batch, seq_len, num_heads, head_dim]
    # → [batch, num_heads, head_dim, seq_len] for matmul
    k = key_cache.permute(0, 2, 3, 1)
    v = value_cache.permute(0, 2, 1, 3)  # [batch, num_heads, seq_len, head_dim]

    # Attention scores: [batch, num_heads, 1, seq_len]
    scores = torch.matmul(q, k) * scale

    # Causal / length mask: zero out padding positions
    mask = _build_length_mask(batch, num_heads, max_seq_len, seq_lens, device=query.device)
    scores = scores + mask

    weights = F.softmax(scores, dim=-1)  # [batch, num_heads, 1, seq_len]

    # Weighted sum of values: [batch, num_heads, 1, head_dim]
    output = torch.matmul(weights, v)

    return output.squeeze(2)  # [batch, num_heads, head_dim]


def _build_length_mask(
    batch: int,
    num_heads: int,
    max_seq_len: int,
    seq_lens: list[int],
    device: torch.device,
) -> torch.Tensor:
    """Build an additive mask that sets padding positions to -inf."""
    mask = torch.zeros(batch, 1, 1, max_seq_len, device=device)
    for i, length in enumerate(seq_lens):
        if length < max_seq_len:
            mask[i, 0, 0, length:] = float("-inf")
    return mask


def select_attention_backend(backend: str):
    """
    Return the attention function to use based on requested backend.
    Falls back gracefully if flash-attn or xformers are not installed.
    """
    if backend == "flash":
        try:
            from kvstream.attention.flash_kernel import paged_attention_flash

            return paged_attention_flash
        except ImportError:
            import logging

            logging.getLogger("kvstream.attention").warning(
                "flash-attn not installed — falling back to naive attention. "
                "Install with: pip install kvstream[flash]"
            )
    elif backend == "xformers":
        try:
            from kvstream.attention.xformers_kernel import paged_attention_xformers

            return paged_attention_xformers
        except ImportError:
            import logging

            logging.getLogger("kvstream.attention").warning(
                "xformers not installed — falling back to naive attention. "
                "Install with: pip install kvstream[xformers]"
            )
    return paged_attention_naive
