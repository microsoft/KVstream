"""
AC-4 / AC-5 — the value tests for token-budget admission.

These are the tests that either validate or falsify KVStream's central claim:

    A token-aware budget uses a device's capacity *better* than a fixed
    request-count cap when request sizes vary, and *no better* when they don't.

**What these tests prove.** They model a device with a true token ceiling and
run the same workload through both admission policies, recording the real token
load the device would have seen. They demonstrate a property of the *admission
policy*:

  * AC-4a — with small requests, the token budget admits strictly more
    concurrently than a fixed count cap (higher utilization).
  * AC-4b — with large requests, the token budget keeps the device within its
    ceiling while the fixed count cap overloads it (safety).
  * AC-5  — with uniform request sizes, the two policies behave the same
    (within ±1). No magic when sizes do not vary.

**What these tests do NOT prove.** They say nothing about Foundry Local's actual
capacity, nor about real end-to-end throughput. The device ceiling here is a
simulation parameter, not a measurement. Establishing a real ceiling is the job
of ``kvstream calibrate`` against live hardware.
"""

from __future__ import annotations

import asyncio

import pytest

from kvstream.admission.capacity import CapacityManager, RequestCost

# A simulated device that can hold this many KV tokens in flight before it
# would degrade. In production this number comes from calibration.
DEVICE_CAPACITY_TOKENS = 6_000
FIXED_CAP = 8  # the request-count cap we are comparing against


class DeviceModel:
    """Records the real token load a device would have seen under a policy."""

    def __init__(self, capacity_tokens: int) -> None:
        self.capacity = capacity_tokens
        self.in_flight_tokens = 0
        self.concurrent = 0
        self.peak_tokens = 0
        self.peak_concurrent = 0
        self.overloads = 0

    def enter(self, tokens: int) -> None:
        self.in_flight_tokens += tokens
        self.concurrent += 1
        self.peak_tokens = max(self.peak_tokens, self.in_flight_tokens)
        self.peak_concurrent = max(self.peak_concurrent, self.concurrent)
        if self.in_flight_tokens > self.capacity:
            self.overloads += 1

    def exit(self, tokens: int) -> None:
        self.in_flight_tokens -= tokens
        self.concurrent -= 1


def _make_manager(unit: str) -> CapacityManager:
    budget = DEVICE_CAPACITY_TOKENS if unit == "tokens" else FIXED_CAP
    return CapacityManager(
        budget=budget, unit=unit, admission_timeout=10.0, max_queue_depth=1000
    )


def _cost(cm: CapacityManager, size_tokens: int) -> tuple[int, int]:
    """Return (admission_cost, true_token_footprint) for a request of `size_tokens`."""
    rc = RequestCost(prompt_tokens=size_tokens - 64, max_tokens=64)
    return cm.cost_of(rc), rc.tokens


async def _run_workload(
    cm: CapacityManager, sizes: list[int], hold: float = 0.03
) -> DeviceModel:
    """Drive `sizes` concurrently through `cm`, recording the simulated device load."""
    device = DeviceModel(DEVICE_CAPACITY_TOKENS)

    async def one(idx: int, size: int) -> None:
        admission_cost, true_tokens = _cost(cm, size)
        await cm.admit(f"r{idx}", admission_cost)
        device.enter(true_tokens)
        try:
            await asyncio.sleep(hold)  # simulate generation time
        finally:
            device.exit(true_tokens)
            await cm.release(f"r{idx}")

    await asyncio.gather(*[one(i, s) for i, s in enumerate(sizes)])
    return device


# ----------------------------------------------------------------------
# AC-4a — small requests: token budget achieves higher concurrency
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac4a_small_requests_token_budget_admits_more():
    sizes = [200] * 40  # 40 cheap requests; 8 x 200 = 1,600 tokens is well under capacity

    tokens_dev = await _run_workload(_make_manager("tokens"), sizes)
    count_dev = await _run_workload(_make_manager("concurrency"), sizes)

    # The fixed cap is exactly its limit, leaving the device badly under-used.
    assert count_dev.peak_concurrent == FIXED_CAP
    assert count_dev.peak_tokens <= FIXED_CAP * 200

    # The token budget packs many more small requests into the same device.
    assert tokens_dev.peak_concurrent > count_dev.peak_concurrent
    assert tokens_dev.peak_concurrent >= 20  # ~6000/200 = 30 in the ideal case

    # ...and still never exceeds the device ceiling.
    assert tokens_dev.overloads == 0
    assert tokens_dev.peak_tokens <= DEVICE_CAPACITY_TOKENS


# ----------------------------------------------------------------------
# AC-4b — large requests: token budget prevents the overload a count cap causes
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac4b_large_requests_token_budget_prevents_overload():
    # Each request is large but still fits the budget individually (3,000 <= 6,000).
    # A count cap of 8 would allow 6 of them at once = 18,000 tokens = 3x the ceiling.
    sizes = [3_000] * 6

    tokens_dev = await _run_workload(_make_manager("tokens"), sizes)
    count_dev = await _run_workload(_make_manager("concurrency"), sizes)

    # Fixed count cap overloads the device: it counts requests, not their cost.
    assert count_dev.overloads > 0
    assert count_dev.peak_tokens > DEVICE_CAPACITY_TOKENS

    # Token budget holds the line at 2 concurrent (2 x 3,000 = 6,000).
    assert tokens_dev.overloads == 0
    assert tokens_dev.peak_tokens <= DEVICE_CAPACITY_TOKENS
    assert tokens_dev.peak_concurrent <= 2

    # All requests still completed under both policies (queuing, not dropping).
    # (Completion is implied by _run_workload returning without raising.)


# ----------------------------------------------------------------------
# AC-5 — homogeneous workload: no advantage, and we say so
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac5_uniform_workload_has_no_advantage():
    # Budget deliberately equals FIXED_CAP x request size, so the two policies
    # express the same limit. This is the honesty check: when every request costs
    # the same, a token budget is just a fixed cap in different units.
    size = DEVICE_CAPACITY_TOKENS // FIXED_CAP  # 750 tokens -> exactly 8 concurrent
    sizes = [size] * 30

    tokens_dev = await _run_workload(_make_manager("tokens"), sizes)
    count_dev = await _run_workload(_make_manager("concurrency"), sizes)

    assert abs(tokens_dev.peak_concurrent - count_dev.peak_concurrent) <= 1
    assert tokens_dev.overloads == 0
    assert count_dev.overloads == 0


# ----------------------------------------------------------------------
# Supporting property: the manager never exceeds its own budget
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mixed_workload_never_breaches_token_budget():
    """AC-1 style: a randomised mix must never breach the configured budget."""
    import random

    random.seed(1234)
    sizes = [random.choice([150, 400, 1_200, 2_500]) for _ in range(60)]

    device = await _run_workload(_make_manager("tokens"), sizes, hold=0.01)

    assert device.overloads == 0
    assert device.peak_tokens <= DEVICE_CAPACITY_TOKENS
    assert device.in_flight_tokens == 0  # everything released
