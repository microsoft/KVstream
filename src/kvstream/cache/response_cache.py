"""
ResponseCache — an in-memory, TTL-bounded cache of completed responses.

Only **deterministic** requests (explicit ``temperature: 0``, single choice) are
eligible, so a cached response is a faithful substitute for re-running the
request. This is a text-level response cache; it is **not** a KV cache and
shares nothing with Foundry Local's memory.

What is stored
--------------
The backend's **actual response**, not a reconstruction of it. A cached answer
that dropped ``tool_calls`` would be worse than no cache at all: an agent would
receive a plausible empty turn instead of the function call it was waiting for.
Non-streamed responses are stored as the response body; streamed ones as the
exact SSE chunks that were forwarded.

That is also why the cache key includes ``stream``. Converting a recorded chunk
sequence into a response body means merging tool-call deltas by index across
partial JSON fragments — a lossy operation. Recording each shape separately
costs one duplicate entry and keeps replay byte-faithful.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field

from kvstream.models import ChatCompletionRequest


@dataclass
class CachedResponse:
    """A completed response, in whichever shape the client originally asked for."""

    body: dict | None = None
    chunks: list[dict] = field(default_factory=list)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    finish_reason: str = "stop"
    # True when the `usage` block was filled in from KVStream's estimate
    # because the backend reported none. Surfaced as a response header.
    usage_estimated: bool = False

    @property
    def streamed(self) -> bool:
        return self.body is None

    @property
    def replayable(self) -> bool:
        """False for a response that ended without producing anything usable."""
        return self.body is not None or bool(self.chunks)


def request_key(req: ChatCompletionRequest, raw: dict) -> str:
    """
    Stable hash of everything the client sent that can change the output.

    Hashing the client's own object — rather than a hand-picked field list —
    means a request carrying ``tools``, ``seed`` or ``response_format`` cannot
    collide with an otherwise identical one that does not.
    """
    material = json.dumps(raw, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class ResponseCache:
    def __init__(self, *, ttl_seconds: int, max_entries: int) -> None:
        self._ttl = ttl_seconds
        self._max = max_entries
        self._data: OrderedDict[str, tuple[float, CachedResponse]] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> CachedResponse | None:
        item = self._data.get(key)
        if item is None:
            self._misses += 1
            return None
        expires, value = item
        if time.monotonic() > expires:
            self._data.pop(key, None)
            self._misses += 1
            return None
        self._data.move_to_end(key)
        self._hits += 1
        return value

    def put(self, key: str, value: CachedResponse) -> None:
        if not value.replayable:
            return
        self._data[key] = (time.monotonic() + self._ttl, value)
        self._data.move_to_end(key)
        while len(self._data) > self._max:
            self._data.popitem(last=False)

    def stats(self) -> dict:
        return {
            "entries": len(self._data),
            "hits": self._hits,
            "misses": self._misses,
        }
