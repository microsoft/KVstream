"""
G-12: live accounting — a reservation tracks what the request actually occupies.

Admission reserves a prediction (`prompt + max_tokens`, or a fraction of it).
These tests pin the two directions that prediction can be wrong in, and the
guarantee that the default configuration cannot breach the budget.
"""

from __future__ import annotations

import pytest

from kvstream.admission.capacity import CapacityManager, RequestCost


def _mgr(budget: int = 1000, unit: str = "tokens", reserve_ratio: float = 1.0) -> CapacityManager:
    return CapacityManager(
        budget=budget,
        unit=unit,
        admission_timeout=1.0,
        max_queue_depth=10,
        reserve_ratio=reserve_ratio,
    )


def test_reserve_ratio_scales_only_the_generation_half():
    rc = RequestCost(prompt_tokens=100, max_tokens=1000)
    assert rc.tokens_at(1.0) == 1100
    assert rc.tokens_at(0.1) == 200        # prompt is never discounted
    assert rc.tokens_at(0.5) == 600


@pytest.mark.asyncio
async def test_shrinking_returns_budget_before_release():
    cm = _mgr()
    rc = RequestCost(prompt_tokens=100, max_tokens=500)
    await cm.admit("a", cm.cost_of(rc))
    assert cm.in_flight == 600

    # The generation stopped after 20 tokens: 480 tokens of headroom were never
    # going to be used and are reclaimed while the response is still being sent.
    delta = await cm.adjust("a", cm.live_cost(rc, 20))
    assert delta == -480
    assert cm.in_flight == 120
    assert cm.reclaimed == 480
    assert cm.overshoots == 0

    await cm.release("a")
    assert cm.in_flight == 0


@pytest.mark.asyncio
async def test_reclaimed_budget_admits_a_waiting_request():
    """The point of reclaiming early: queued work starts sooner."""
    cm = _mgr(budget=1000)
    big = RequestCost(prompt_tokens=100, max_tokens=800)
    await cm.admit("big", cm.cost_of(big))       # 900 of 1000

    small = RequestCost(prompt_tokens=50, max_tokens=200)
    cost = cm.cost_of(small)                     # 250 — does not fit
    assert cm.in_flight + cost > cm.budget

    await cm.adjust("big", cm.live_cost(big, 10))  # generation stopped early
    assert cm.in_flight == 110
    await cm.admit("small", cost)                # now fits, without waiting
    assert cm.in_flight == 360


@pytest.mark.asyncio
async def test_growth_beyond_an_under_reservation_is_counted():
    cm = _mgr(reserve_ratio=0.1)
    rc = RequestCost(prompt_tokens=100, max_tokens=1000)
    cost = cm.cost_of(rc)
    assert cost == 200                            # reserved a tenth of max_tokens
    await cm.admit("a", cost)

    # The generation ran past the reservation. Topping up beats truncating a
    # half-delivered response, but the event must be visible.
    delta = await cm.adjust("a", cm.live_cost(rc, 400))
    assert delta == 300
    assert cm.in_flight == 500
    assert cm.overshoots == 1


@pytest.mark.asyncio
async def test_default_ratio_can_never_overshoot():
    """With the doc-faithful default, generation cannot outgrow the reservation."""
    cm = _mgr(reserve_ratio=1.0)
    rc = RequestCost(prompt_tokens=100, max_tokens=500)
    await cm.admit("a", cm.cost_of(rc))
    # Even at the maximum permitted generation length:
    await cm.adjust("a", cm.live_cost(rc, 500))
    assert cm.overshoots == 0
    assert cm.in_flight == 600


@pytest.mark.asyncio
async def test_adjust_is_a_noop_for_released_requests():
    cm = _mgr()
    rc = RequestCost(prompt_tokens=10, max_tokens=10)
    await cm.admit("a", cm.cost_of(rc))
    await cm.release("a")
    assert await cm.adjust("a", 5) == 0
    assert cm.in_flight == 0


@pytest.mark.asyncio
async def test_concurrency_mode_ignores_live_accounting():
    cm = _mgr(budget=4, unit="concurrency")
    rc = RequestCost(prompt_tokens=100, max_tokens=500)
    await cm.admit("a", cm.cost_of(rc))
    assert cm.in_flight == 1
    await cm.adjust("a", cm.live_cost(rc, 20))
    assert cm.in_flight == 1          # a request is always worth one slot
    assert cm.reclaimed == 0


@pytest.mark.asyncio
async def test_rejects_an_invalid_reserve_ratio():
    with pytest.raises(ValueError):
        _mgr(reserve_ratio=0.0)
    with pytest.raises(ValueError):
        _mgr(reserve_ratio=1.5)
