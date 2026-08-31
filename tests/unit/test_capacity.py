"""Unit tests for the CapacityManager admission logic."""

from __future__ import annotations

import asyncio

import pytest

from kvstream.admission.capacity import (
    AdmissionTimeout,
    CapacityManager,
    QueueFull,
    RequestCost,
)


def test_request_cost_tokens():
    assert RequestCost(prompt_tokens=100, max_tokens=50).tokens == 150
    assert RequestCost(prompt_tokens=0, max_tokens=0).tokens == 1  # floor


@pytest.mark.asyncio
async def test_concurrency_cap_and_release():
    cm = CapacityManager(
        budget=2, unit="concurrency", admission_timeout=5, max_queue_depth=10
    )
    await cm.admit("a", 1)
    await cm.admit("b", 1)
    assert cm.in_flight == 2

    third = asyncio.create_task(cm.admit("c", 1))
    await asyncio.sleep(0.05)
    assert not third.done()  # queued — budget full

    await cm.release("a")
    await asyncio.wait_for(third, 1.0)
    assert cm.in_flight == 2


@pytest.mark.asyncio
async def test_token_budget_packs_and_gates():
    cm = CapacityManager(
        budget=100, unit="tokens", admission_timeout=5, max_queue_depth=10
    )
    await cm.admit("a", 60)
    assert cm.in_flight == 60

    big = asyncio.create_task(cm.admit("b", 60))  # 60+60 > 100 → must wait
    await asyncio.sleep(0.05)
    assert not big.done()

    await cm.release("a")
    await asyncio.wait_for(big, 1.0)
    assert cm.in_flight == 60


@pytest.mark.asyncio
async def test_oversized_request_runs_alone():
    cm = CapacityManager(
        budget=10, unit="tokens", admission_timeout=5, max_queue_depth=10
    )
    await cm.admit("big", 50)  # exceeds budget but admitted alone (idle)
    assert cm.in_flight == 50


@pytest.mark.asyncio
async def test_queue_full():
    cm = CapacityManager(
        budget=1, unit="concurrency", admission_timeout=5, max_queue_depth=1
    )
    await cm.admit("a", 1)  # fills budget
    waiting = asyncio.create_task(cm.admit("b", 1))  # 1 waiter (== max_queue_depth)
    await asyncio.sleep(0.05)
    with pytest.raises(QueueFull):
        await cm.admit("c", 1)
    await cm.release("a")
    await asyncio.wait_for(waiting, 1.0)


@pytest.mark.asyncio
async def test_admission_timeout():
    cm = CapacityManager(
        budget=1, unit="concurrency", admission_timeout=0.1, max_queue_depth=10
    )
    await cm.admit("a", 1)
    with pytest.raises(AdmissionTimeout):
        await cm.admit("b", 1)


@pytest.mark.asyncio
async def test_invariant_sum_equals_in_flight():
    cm = CapacityManager(
        budget=1000, unit="tokens", admission_timeout=5, max_queue_depth=10
    )
    await cm.admit("a", 100)
    await cm.admit("b", 250)
    assert cm.in_flight == 350
    await cm.release("a")
    assert cm.in_flight == 250
    await cm.release("missing")  # idempotent
    assert cm.in_flight == 250
