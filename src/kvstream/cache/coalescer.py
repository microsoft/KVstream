"""
Coalescer — singleflight de-duplication of identical concurrent requests.

When several identical deterministic requests are in flight at once, only the
first ("leader") executes; the rest await the leader's result. This collapses
duplicate work — common in multi-agent fan-out — into a single backend call.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class _LeaderAbandoned(RuntimeError):
    """The leader was cancelled before producing a result; a follower takes over."""


def _retrieve(fut: asyncio.Future) -> None:
    """Consume a future's exception so a leader-only failure isn't reported as
    an unretrieved-future warning (followers, if any, still receive it)."""
    if not fut.cancelled():
        fut.exception()


class Coalescer:
    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Future] = {}
        self._coalesced = 0

    @property
    def coalesced_total(self) -> int:
        return self._coalesced

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    async def run(self, key: str, factory: Callable[[], Awaitable[T]]) -> tuple[T, bool]:
        """
        Execute ``factory`` once per ``key`` while duplicates are in flight.

        Returns ``(result, was_follower)`` where ``was_follower`` is True when the
        result came from an already-running leader (i.e. this call was coalesced).

        Cancellation never crosses between callers. A follower awaits the shared
        result through :func:`asyncio.shield`, so its own cancellation (its
        client went away) cannot cancel the result everyone else is waiting
        for. And a leader that is cancelled before finishing hands over rather
        than stranding its followers: they are woken and one of them becomes
        the new leader.
        """
        while True:
            existing = self._inflight.get(key)
            if existing is None:
                break
            try:
                result = await asyncio.shield(existing)
            except _LeaderAbandoned:
                # The leader's work never completed. Its `finally` has already
                # unregistered it, so the next pass elects a new leader.
                continue
            self._coalesced += 1
            return result, True

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        fut.add_done_callback(_retrieve)
        self._inflight[key] = fut
        try:
            result = await factory()
            if not fut.done():
                fut.set_result(result)
            return result, False
        except asyncio.CancelledError:
            if not fut.done():
                fut.set_exception(_LeaderAbandoned("coalescing leader was cancelled"))
            raise
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            if self._inflight.get(key) is fut:
                self._inflight.pop(key, None)
