"""
StreamBroadcast — one upstream stream, many identical clients.

Singleflight coalescing collapses duplicate concurrent work into a single
backend call. For the non-streaming path that is just "await the leader's
result". Streaming needs more: followers arrive while the leader is still
producing, so they have to receive what has already been sent *and* everything
that comes after, in order, without the leader stalling if a follower is slow.

The shape here is a shared append-only buffer plus a wake-up event. The leader
appends chunks and never waits on anybody; each follower reads from its own
index and waits only when it has caught up. A follower that arrives late
replays the buffer from the beginning, so it sees the whole response rather
than joining halfway.

This matters more than it sounds for the workload the proposal targets: agent
swarms overwhelmingly stream, so coalescing that only worked on the
non-streaming path was inactive for the dominant traffic shape.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator


class StreamBroadcast:
    """An in-progress streamed response that late arrivals can join."""

    def __init__(self) -> None:
        self._chunks: list[bytes] = []
        self._event = asyncio.Event()
        self._done = False
        self._error: BaseException | None = None
        self.followers = 0

    # -- producer ------------------------------------------------------

    def publish(self, chunk: bytes) -> None:
        """Append a chunk and wake every follower. Never blocks."""
        self._chunks.append(chunk)
        self._wake()

    def close(self, error: BaseException | None = None) -> None:
        """Mark the stream finished; ``error`` is re-raised in followers."""
        self._done = True
        self._error = error
        self._wake()

    def _wake(self) -> None:
        self._event.set()
        self._event.clear()

    # -- consumers -----------------------------------------------------

    @property
    def done(self) -> bool:
        return self._done

    async def follow(self) -> AsyncIterator[bytes]:
        """
        Replay everything sent so far, then track the leader to completion.

        The leader is never blocked by this: it only ever appends. A follower
        that cannot keep up falls behind in its own iteration, not the
        producer's.
        """
        index = 0
        while True:
            while index < len(self._chunks):
                yield self._chunks[index]
                index += 1
            if self._done:
                if self._error is not None:
                    raise self._error
                return
            await self._event.wait()


class StreamCoalescer:
    """
    Registry of in-flight streamed responses, keyed like the response cache.

    A request whose key is already streaming becomes a follower; anything else
    becomes the leader and is responsible for closing the broadcast.
    """

    def __init__(self) -> None:
        self._inflight: dict[str, StreamBroadcast] = {}
        self._coalesced = 0

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    @property
    def coalesced_total(self) -> int:
        return self._coalesced

    def follower_for(self, key: str) -> StreamBroadcast | None:
        """Join an in-flight stream for ``key``, or ``None`` to lead it."""
        broadcast = self._inflight.get(key)
        if broadcast is None or broadcast.done:
            return None
        broadcast.followers += 1
        self._coalesced += 1
        return broadcast

    def lead(self, key: str) -> StreamBroadcast:
        broadcast = StreamBroadcast()
        self._inflight[key] = broadcast
        return broadcast

    def finish(self, key: str, broadcast: StreamBroadcast) -> None:
        if self._inflight.get(key) is broadcast:
            self._inflight.pop(key, None)

    def stats(self) -> dict:
        return {"inflight": len(self._inflight), "coalesced_total": self._coalesced}
