"""Cancellation regressions for non-streaming request coalescing."""

from __future__ import annotations

import asyncio

import pytest

from kvstream.cache.coalescer import Coalescer


async def joined(c: Coalescer, n: int) -> None:
    for _ in range(100):
        if c.coalesced_total == n:
            return
        await asyncio.sleep(0)
    raise AssertionError("Followers did not join")


@pytest.mark.asyncio
async def test_one_follower_cannot_cancel_others_or_leader():
    c = Coalescer()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def work():
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return b"exact-body"

    leader = asyncio.create_task(c.run("k", work))
    await started.wait()
    leaving = asyncio.create_task(c.run("k", work))
    surviving = asyncio.create_task(c.run("k", work))
    await joined(c, 2)
    leaving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leaving
    release.set()
    assert await leader == (b"exact-body", False)
    assert await surviving == (b"exact-body", True)
    assert calls == 1 and c.inflight == 0


@pytest.mark.asyncio
async def test_late_join_after_follower_cancellation_still_works():
    c = Coalescer()
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        await release.wait()
        return "ok"

    leader = asyncio.create_task(c.run("k", work))
    await started.wait()
    leaving = asyncio.create_task(c.run("k", work))
    await joined(c, 1)
    leaving.cancel()
    await asyncio.gather(leaving, return_exceptions=True)
    late = asyncio.create_task(c.run("k", work))
    await joined(c, 2)
    release.set()
    assert await late == ("ok", True)
    await leader
    assert c.inflight == 0


@pytest.mark.asyncio
async def test_leader_cancellation_settles_followers_and_allows_retry():
    c = Coalescer()
    started = asyncio.Event()
    forever = asyncio.Event()

    async def work():
        started.set()
        await forever.wait()

    leader = asyncio.create_task(c.run("k", work))
    await started.wait()
    follower = asyncio.create_task(c.run("k", work))
    await joined(c, 1)
    leader.cancel()
    await asyncio.gather(leader, return_exceptions=True)
    # A timeout must not cancel the follower and mask a still-pending future.
    try:
        done, _ = await asyncio.wait({follower}, timeout=1)
        assert follower in done, "Leader cancellation left its follower pending"
        with pytest.raises(asyncio.CancelledError):
            await follower
    finally:
        if not follower.done():
            follower.cancel()
            await asyncio.gather(follower, return_exceptions=True)
    assert c.inflight == 0

    async def retry():
        return "new-result"

    assert await c.run("k", retry) == ("new-result", False)


@pytest.mark.asyncio
async def test_failure_after_cancelled_follower_reaches_survivor():
    c = Coalescer()
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        await release.wait()
        raise ValueError("backend-error")

    leader = asyncio.create_task(c.run("k", work))
    await started.wait()
    leaving = asyncio.create_task(c.run("k", work))
    survivor = asyncio.create_task(c.run("k", work))
    await joined(c, 2)
    leaving.cancel()
    await asyncio.gather(leaving, return_exceptions=True)
    release.set()
    results = await asyncio.gather(leader, survivor, return_exceptions=True)
    assert all(isinstance(r, ValueError) and str(r) == "backend-error" for r in results)
    assert c.inflight == 0


@pytest.mark.asyncio
async def test_all_followers_leave_leader_still_completes():
    c = Coalescer()
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        await release.wait()
        return 42

    leader = asyncio.create_task(c.run("k", work))
    await started.wait()
    followers = [asyncio.create_task(c.run("k", work)) for _ in range(10)]
    await joined(c, 10)
    for task in followers:
        task.cancel()
    await asyncio.gather(*followers, return_exceptions=True)
    release.set()
    assert await leader == (42, False)
    assert c.inflight == 0


@pytest.mark.asyncio
async def test_cancellation_on_one_key_does_not_affect_another():
    c = Coalescer()
    a_started = asyncio.Event()
    release = asyncio.Event()

    async def work_a():
        a_started.set()
        await release.wait()
        return "a"

    a = asyncio.create_task(c.run("a", work_a))
    await a_started.wait()
    follower = asyncio.create_task(c.run("a", work_a))
    await joined(c, 1)
    follower.cancel()
    await asyncio.gather(follower, return_exceptions=True)

    async def work_b():
        return "b"

    assert await c.run("b", work_b) == ("b", False)
    release.set()
    assert await a == ("a", False)
