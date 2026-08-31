"""Unit tests for the singleflight Coalescer."""

from __future__ import annotations

import asyncio

import pytest

from kvstream.cache.coalescer import Coalescer


@pytest.mark.asyncio
async def test_identical_concurrent_calls_run_once():
    c = Coalescer()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return "RESULT"

    results = await asyncio.gather(c.run("k", factory), c.run("k", factory), c.run("k", factory))
    values = [r[0] for r in results]
    followers = [r[1] for r in results]

    assert values == ["RESULT", "RESULT", "RESULT"]
    assert calls == 1
    assert sum(followers) == 2  # exactly two were coalesced
    assert c.coalesced_total == 2
    assert c.inflight == 0


@pytest.mark.asyncio
async def test_different_keys_run_independently():
    c = Coalescer()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        return calls

    await asyncio.gather(c.run("a", factory), c.run("b", factory))
    assert calls == 2


@pytest.mark.asyncio
async def test_exception_propagates_and_clears():
    c = Coalescer()

    async def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await c.run("k", boom)
    assert c.inflight == 0  # cleaned up so the key can be retried
