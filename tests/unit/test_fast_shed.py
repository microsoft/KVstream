"""
G-55: shed load has to be shed *fast*.

Measured on real hardware: 28 of 60 requests were refused, and every one of them
came back at 120.1 seconds — the admission timeout, to the decimal. The gateway
already knew, on arrival, that a queue of 52 against a budget of 8 at ~10s per
request could not reach them in time. It just had no way to say so.

Backpressure a client waits two minutes for is not backpressure.
"""

from __future__ import annotations

import asyncio

import pytest

from kvstream.admission.capacity import (
    AdmissionTooSlow,
    CapacityManager,
    QueueFull,
)


def _mgr(budget=4, timeout=5.0, depth=100, samples=2, hopeless=True) -> CapacityManager:
    return CapacityManager(
        budget=budget,
        unit="concurrency",
        admission_timeout=timeout,
        max_queue_depth=depth,
        reject_when_hopeless=hopeless,
        min_rate_samples=samples,
    )


async def _teach_rate(
    cm: CapacityManager, seconds_per_release: float, n: int = 4
) -> None:
    """Run n requests through so the manager learns how fast the backend drains."""
    for i in range(n):
        await cm.admit(f"warm-{i}", 1)
        await asyncio.sleep(seconds_per_release)
        await cm.release(f"warm-{i}")


# -- learning the drain rate -------------------------------------------


@pytest.mark.asyncio
async def test_a_burst_of_instant_failures_does_not_inflate_the_rate():
    """
    The trap a gap-based estimator falls into.

    Requests that fail instantly keep the gap between completions tiny, so an
    inter-completion EWMA reads the backend as fast while the real work behind
    them takes seconds each. Throughput over a window counts what actually got
    done.
    """
    cm = _mgr(budget=8, samples=2)
    for i in range(20):  # a burst of instant completions
        await cm.admit(f"fast-{i}", 1)
        await cm.release(f"fast-{i}")
    burst_rate = cm.drain_rate

    await asyncio.sleep(0.3)  # then nothing completes at all
    assert cm.drain_rate <= burst_rate


@pytest.mark.asyncio
async def test_the_drain_rate_is_learned_from_completions():
    cm = _mgr()
    assert cm.drain_rate == 0.0
    await _teach_rate(cm, 0.05)
    assert cm.drain_rate > 0
    assert cm.stats()["completions"] >= 2


@pytest.mark.asyncio
async def test_nothing_is_refused_before_the_rate_is_known():
    """An unmeasured system must not reject anyone on a guess."""
    cm = _mgr(budget=1, timeout=0.01, samples=100)
    await cm.admit("holder", 1)
    waiter = asyncio.create_task(cm.admit("w", 1))
    await asyncio.sleep(0)
    assert not waiter.done()  # queued, not refused
    assert cm.stats()["hopeless_rejections"] == 0
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.asyncio
async def test_predicted_wait_is_zero_without_samples():
    assert _mgr(samples=10).predicted_wait(1) == 0.0


# -- refusing on arrival -----------------------------------------------


@pytest.mark.asyncio
async def test_a_hopeless_request_is_refused_immediately():
    """The whole point: refused in milliseconds, not after the timeout."""
    cm = _mgr(budget=1, timeout=1.0, samples=2)
    await _teach_rate(cm, 0.1)  # ~10 completions/second

    await cm.admit("holder", 1)
    queued = [asyncio.create_task(cm.admit(f"q{i}", 1)) for i in range(60)]
    await asyncio.sleep(0)

    started = asyncio.get_event_loop().time()
    with pytest.raises(AdmissionTooSlow) as caught:
        await cm.admit("late", 1)
    elapsed = asyncio.get_event_loop().time() - started

    assert elapsed < 0.05  # not the 1.0s timeout
    assert caught.value.predicted_wait > 1.0
    assert "beyond the" in str(caught.value)
    for task in queued:
        task.cancel()
    await asyncio.gather(*queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_servable_request_is_still_queued():
    """A short queue must not be refused just because a queue exists."""
    cm = _mgr(budget=1, timeout=10.0, samples=2)
    await _teach_rate(cm, 0.02)  # fast backend

    await cm.admit("holder", 1)
    queued = [asyncio.create_task(cm.admit(f"q{i}", 1)) for i in range(3)]
    await asyncio.sleep(0)

    ok = asyncio.create_task(cm.admit("ok", 1))
    await asyncio.sleep(0)
    assert not ok.done()  # waiting, not refused
    assert cm.stats()["hopeless_rejections"] == 0

    for task in [*queued, ok]:
        task.cancel()
    await asyncio.gather(*queued, ok, return_exceptions=True)


@pytest.mark.asyncio
async def test_the_prediction_scales_with_the_queue():
    cm = _mgr(budget=1, timeout=1000.0, samples=2)
    await _teach_rate(cm, 0.1)
    await cm.admit("holder", 1)

    short = cm.predicted_wait(1)
    queued = [asyncio.create_task(cm.admit(f"q{i}", 1)) for i in range(20)]
    await asyncio.sleep(0)
    long = cm.predicted_wait(1)

    assert long > short
    for task in queued:
        task.cancel()
    await asyncio.gather(*queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_the_prediction_scales_with_request_cost():
    """In token mode a bigger request needs more headroom, so it waits longer."""
    cm = CapacityManager(
        budget=100,
        unit="tokens",
        admission_timeout=1000.0,
        max_queue_depth=100,
        min_rate_samples=2,
    )
    for i in range(4):
        await cm.admit(f"w{i}", 10)
        await asyncio.sleep(0.02)
        await cm.release(f"w{i}")
    await cm.admit("holder", 100)
    assert cm.predicted_wait(90) > cm.predicted_wait(10)


@pytest.mark.asyncio
async def test_it_can_be_turned_off():
    cm = _mgr(budget=1, timeout=0.2, samples=2, hopeless=False)
    await _teach_rate(cm, 0.1)
    await cm.admit("holder", 1)
    queued = [asyncio.create_task(cm.admit(f"q{i}", 1)) for i in range(50)]
    await asyncio.sleep(0)

    # With the policy off, the old behaviour returns: wait, then time out.
    from kvstream.admission.capacity import AdmissionTimeout

    with pytest.raises(AdmissionTimeout):
        await cm.admit("late", 1)
    for task in queued:
        task.cancel()
    await asyncio.gather(*queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_queue_full_still_wins_when_the_queue_is_capped():
    """A hard cap is a hard cap, whatever the prediction says."""
    cm = _mgr(budget=1, timeout=1000.0, depth=2, samples=2)
    await _teach_rate(cm, 0.01)
    await cm.admit("holder", 1)
    queued = [asyncio.create_task(cm.admit(f"q{i}", 1)) for i in range(2)]
    await asyncio.sleep(0)
    with pytest.raises(QueueFull):
        await cm.admit("late", 1)
    for task in queued:
        task.cancel()
    await asyncio.gather(*queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_queued_cost_is_returned_when_a_waiter_leaves():
    """Otherwise the prediction drifts upward and starts refusing good traffic."""
    cm = _mgr(budget=1, timeout=1000.0, samples=2)
    await cm.admit("holder", 1)
    tasks = [asyncio.create_task(cm.admit(f"q{i}", 1)) for i in range(5)]
    await asyncio.sleep(0)
    assert cm.stats()["queued_cost"] == 5

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(0)
    assert cm.stats()["queued_cost"] == 0
