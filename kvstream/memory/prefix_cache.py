"""
PrefixKVCache — hash-based prefix deduplication.

When multiple requests share the same system prompt (or any common
prefix), we compute the KV cache for that prefix once and reuse it
across all sequences via copy-on-write in the block manager.

Implementation:
  - Token sequences are chunked into block-aligned prefix slabs.
  - Each slab is identified by a rolling hash of its token IDs.
  - On a cache hit, the child sequence forks the parent's block table
    and continues from where the prefix ends.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

from kvstream.memory.block_manager import BlockManager


def _hash_tokens(tokens: list[int]) -> str:
    """Deterministic hash of a token sequence.
    Handles both int token IDs and raw byte values from fallback tokenizers.
    """
    parts = []
    for t in tokens:
        v = t if isinstance(t, int) else int(t)
        parts.append(
            v.to_bytes(4, "little", signed=False)
            if 0 <= v < 2**32
            else abs(v).to_bytes(4, "little")
        )
    return hashlib.sha256(b"".join(parts)).hexdigest()[:16]


@dataclass
class PrefixEntry:
    prefix_hash: str
    num_tokens: int
    seq_id: str  # the "canonical" sequence whose blocks we fork
    created_at: float
    hit_count: int = 0


class PrefixKVCache:
    def __init__(
        self,
        block_manager: BlockManager,
        max_prefix_length: int = 2048,
        ttl_seconds: int = 3600,
        min_match_tokens: int = 16,
    ) -> None:
        self.bm = block_manager
        self.max_prefix_length = max_prefix_length
        self.ttl_seconds = ttl_seconds
        self.min_match_tokens = min_match_tokens

        # hash → PrefixEntry
        self._entries: dict[str, PrefixEntry] = {}

    def match(self, tokens: list[int]) -> PrefixEntry | None:
        """
        Find the longest cached prefix that matches the start of tokens.
        Returns None if no match of >= min_match_tokens.
        """
        best: PrefixEntry | None = None
        # Check progressively longer prefixes (block-aligned)
        block_size = self.bm.block_size
        for length in range(block_size, min(len(tokens), self.max_prefix_length) + 1, block_size):
            prefix_hash = _hash_tokens(tokens[:length])
            entry = self._entries.get(prefix_hash)
            if entry and self._is_valid(entry):
                best = entry
            else:
                break  # prefix tree broken — stop

        if best and best.num_tokens >= self.min_match_tokens:
            best.hit_count += 1
            return best
        return None

    def register(self, seq_id: str, tokens: list[int]) -> None:
        """
        Register a completed prefill as a reusable prefix.

        The donor's block table is forked into a canonical "prefix:<hash>"
        sequence so the pages survive after the donor is freed. Every
        block-aligned sub-prefix is indexed too — match() walks the chain
        block by block, so the chain must be unbroken for a hit.
        """
        block_size = self.bm.block_size
        aligned = (min(len(tokens), self.max_prefix_length) // block_size) * block_size
        if aligned < self.min_match_tokens:
            return
        tokens = tokens[:aligned]

        full_hash = _hash_tokens(tokens)
        if full_hash in self._entries:
            return

        canonical_id = f"prefix:{full_hash}"
        try:
            self.bm.fork(seq_id, canonical_id, num_blocks=aligned // block_size)
        except KeyError:
            return  # donor already freed — nothing to share

        now = time.monotonic()
        for length in range(block_size, aligned + 1, block_size):
            sub_hash = _hash_tokens(tokens[:length])
            if sub_hash not in self._entries:
                self._entries[sub_hash] = PrefixEntry(
                    prefix_hash=sub_hash,
                    num_tokens=length,
                    seq_id=canonical_id,
                    created_at=now,
                )

    def fork_prefix(self, parent_entry: PrefixEntry, child_seq_id: str) -> int:
        """
        Fork the matched portion of the canonical block table into the child.
        Returns the number of tokens the child can skip (prefix length).
        """
        num_blocks = parent_entry.num_tokens // self.bm.block_size
        self.bm.fork(parent_entry.seq_id, child_seq_id, num_blocks=num_blocks)
        return parent_entry.num_tokens

    def evict_expired(self) -> int:
        """Remove stale entries and release their pages. Returns count evicted."""
        now = time.monotonic()
        to_remove = {k for k, v in self._entries.items() if now - v.created_at > self.ttl_seconds}
        if not to_remove:
            return 0
        # Build the set of canonicals still referenced by *live* entries in a
        # single O(n) pass — avoids the original O(n*m) any() scan per item.
        live_canonicals = {v.seq_id for k, v in self._entries.items() if k not in to_remove}
        freed_canonicals: set[str] = set()
        for k in to_remove:
            entry = self._entries.pop(k)
            if entry.seq_id not in live_canonicals and entry.seq_id not in freed_canonicals:
                self.bm.free(entry.seq_id)
                freed_canonicals.add(entry.seq_id)
        return len(to_remove)

    def stats(self) -> dict:
        total_hits = sum(e.hit_count for e in self._entries.values())
        return {
            "cached_prefixes": len(self._entries),
            "total_prefix_hits": total_hits,
            "cached_tokens": sum(e.num_tokens for e in self._entries.values()),
        }

    def _is_valid(self, entry: PrefixEntry) -> bool:
        return (
            time.monotonic() - entry.created_at < self.ttl_seconds
            and self.bm.has_sequence(entry.seq_id)
        )
