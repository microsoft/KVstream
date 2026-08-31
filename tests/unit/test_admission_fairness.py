"""
G-30: admission is ordered, and ordering is what stops starvation.

The old implementation woke every waiter on release and let each re-check. That
has no ordering at all — whichever coroutine the loop schedules first and
happens to fit wins — so a large request could be starved indefinitely by a
stream of small ones. These tests pin the ordering guarantee.
"""

from __future__ import annotations

import asyncio

import pytest

from kvstream.admission.capacity import (
    AdmissionTimeout,
    CapacityManager,
    QueueFull,
    RequestCost,
)


def _mgr(budget: int = 100, timeout: float = 5.0, depth: int = 10) -> CapacityManager:
    return CapacityManager(
        budget=budget, unit="tokens", admission_timeout=timeout, max_queue_depth=depth
    )


async def _abandon(*tasks: asyncio.Task) -> None:
    """Cancel leftover waiters and let them unwind, so the loop stays quiet."""
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_waiters_are_admitted_in_arrival_order():
    cm = _mgr(budget=100)
    await cm.admit("holder", 100)

    order: list[str] = []

    async def wait(name: str, cost: int) -> None:
        await cm.admit(name, cost)
        order.append(name)

    tasks = []
    for name, cost in [("first", 90), ("second", 10), ("third", 10)]:
        tasks.append(asyncio.create_task(wait(name, cost)))
        await asyncio.sleep(0)  # establish arrival order deterministically

    await cm.release("holder")
    await asyncio.sleep(0.05)

    # "first" needs 90 and goes first even though the smaller two would fit.
    assert order[0] == "first"
    await _abandon(*tasks)


@pytest.mark.asyncio
async def test_a_large_request_is_not_starved_by_a_stream_of_small_ones():
    """The regression this whole discipline exists to prevent."""
    cm = _mgr(budget=100)
    await cm.admit("holder", 100)

    big_admitted = asyncio.Event()

    async def big() -> None:
        await cm.admit("big", 100)
        big_admitted.set()

    big_task = asyncio.create_task(big())
    await asyncio.sleep(0)

    # A steady stream of small requests arrives behind it and keeps arriving.
    small_tasks = [asyncio.create_task(cm.admit(f"small-{i}", 5)) for i in range(20)]
    await asyncio.sleep(0)

    await cm.release("holder")
    await asyncio.wait_for(big_admitted.wait(), timeout=1.0)

    assert cm.in_flight == 100  # the big one holds the whole budget
    await _abandon(*small_tasks, big_task)


@pytest.mark.asyncio
async def test_the_queue_drains_as_far_as_it_fits():
    """Proposal §6.2 step 6: admit the next queued request(s) that now fit."""
    cm = _mgr(budget=100)
    await cm.admit("holder", 100)

    tasks = [asyncio.create_task(cm.admit(f"r{i}", 25)) for i in range(4)]
    await asyncio.sleep(0)
    assert cm.waiting == 4

    await cm.release("holder")
    await asyncio.sleep(0.05)

    # All four fit in the freed 100, so all four start at once.
    assert cm.in_flight == 100
    assert cm.waiting == 0
    for task in tasks:
        assert task.done()


@pytest.mark.asyncio
async def test_a_partial_drain_stops_at_the_first_waiter_that_does_not_fit():
    cm = _mgr(budget=100)
    await cm.admit("holder", 100)

    small = asyncio.create_task(cm.admit("small", 40))
    await asyncio.sleep(0)
    big = asyncio.create_task(cm.admit("big", 80))
    await asyncio.sleep(0)
    behind = asyncio.create_task(cm.admit("behind", 10))
    await asyncio.sleep(0)

    await cm.release("holder")
    await asyncio.sleep(0.05)

    # "small" (40) fits; "big" (80) does not, and "behind" must not jump it.
    assert small.done()
    assert not big.done()
    assert not behind.done()
    assert cm.in_flight == 40
    await _abandon(big, behind)


@pytest.mark.asyncio
async def test_a_fast_path_admission_never_touches_the_queue():
    """Requests that fit immediately must not consume queue depth."""
    cm = _mgr(budget=100, depth=1)
    await cm.admit("a", 10)
    await cm.admit("b", 10)
    await cm.admit("c", 10)
    assert cm.waiting == 0
    assert cm.stats()["queue"]["peak_depth"] == 0


@pytest.mark.asyncio
async def test_queue_full_is_measured_by_actual_waiters():
    cm = _mgr(budget=10, depth=2)
    await cm.admit("holder", 10)

    waiting = [asyncio.create_task(cm.admit(f"w{i}", 5)) for i in range(2)]
    await asyncio.sleep(0)
    assert cm.waiting == 2

    with pytest.raises(QueueFull):
        await cm.admit("overflow", 5)
    assert cm.stats()["queue"]["rejected"] == 1
    await _abandon(*waiting)


@pytest.mark.asyncio
async def test_a_timed_out_waiter_leaves_the_queue_and_unblocks_the_rest():
    cm = _mgr(budget=100, timeout=0.05)
    await cm.admit("holder", 100)

    doomed = asyncio.create_task(cm.admit("doomed", 100))
    await asyncio.sleep(0)
    with pytest.raises(AdmissionTimeout):
        await doomed
    assert cm.stats()["queue"]["timed_out"] == 1
    assert cm.waiting == 0

    # With the blocker gone, a later arrival is served normally.
    behind = asyncio.create_task(cm.admit("behind", 10))
    await asyncio.sleep(0)
    await cm.release("holder")
    await asyncio.sleep(0.02)
    assert behind.done()
    assert cm.in_flight == 10


@pytest.mark.asyncio
async def test_a_cancelled_waiter_does_not_leak_budget():
    """A client that disconnects while queued must give its place back."""
    cm = _mgr(budget=100)
    await cm.admit("holder", 100)

    gone = asyncio.create_task(cm.admit("gone", 50))
    await asyncio.sleep(0)
    gone.cancel()
    await asyncio.sleep(0)

    await cm.release("holder")
    await asyncio.sleep(0.05)
    assert cm.in_flight == 0
    assert cm.waiting == 0


@pytest.mark.asyncio
async def test_reclaimed_budget_drains_the_queue_immediately():
    """Live accounting frees budget; waiters should start then, not at teardown."""
    cm = _mgr(budget=100)
    rc = RequestCost(prompt_tokens=10, max_tokens=90)
    await cm.admit("long", cm.cost_of(rc))
    assert cm.in_flight == 100

    waiter = asyncio.create_task(cm.admit("waiter", 50))
    await asyncio.sleep(0)
    assert cm.waiting == 1

    await cm.adjust("long", cm.live_cost(rc, 5))  # generation stopped early
    await asyncio.sleep(0.05)
    assert waiter.done()
    assert cm.in_flight == 65


@pytest.mark.asyncio
async def test_queue_stats_report_the_head_and_the_wait():
    cm = _mgr(budget=10)
    await cm.admit("holder", 10)
    waiter = asyncio.create_task(cm.admit("waiter", 7))
    await asyncio.sleep(0.02)

    queue = cm.stats()["queue"]
    assert queue["depth"] == 1
    assert queue["head_cost"] == 7
    assert queue["oldest_wait_seconds"] > 0
    await _abandon(waiter)


# -- shutdown -----------------------------------------------------------


@pytest.mark.asyncio
async def test_draining_turns_away_the_queue_but_not_the_in_flight():
    cm = _mgr(budget=100)
    await cm.admit("running", 100)
    queued = [asyncio.create_task(cm.admit(f"q{i}", 10)) for i in range(3)]
    await asyncio.sleep(0)

    turned_away = await cm.start_draining()
    assert turned_away == 3
    assert cm.draining is True
    assert cm.in_flight == 100  # the running request is left alone

    await _abandon(*queued)


@pytest.mark.asyncio
async def test_a_draining_manager_admits_nothing_new():
    cm = _mgr(budget=100)
    await cm.start_draining()
    with pytest.raises(QueueFull, match="shutting down"):
        await cm.admit("late", 1)
