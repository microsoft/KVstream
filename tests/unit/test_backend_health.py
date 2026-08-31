"""
G-52 and G-54: readiness, and failing fast.

Both fixes come from one measured event. Against a live `foundry 0.8.119` the
runtime stopped completing generations entirely — a 4-token request did not
return in 180 seconds — while `GET /v1/models` kept answering 200 in 4ms. The
gateway had no signal that anything was wrong, and every arriving request queued
behind a backend that would never answer.
"""

from __future__ import annotations

import asyncio

import pytest

from kvstream.backend.health import (
    CLOSED,
    HALF_OPEN,
    OPEN,
    BackendHealth,
    BackendUnavailable,
    CircuitBreaker,
)


class FakeClient:
    """A backend whose liveness and generation health can differ."""

    def __init__(self, alive: bool = True, generates: bool = True, hang: float = 0.0) -> None:
        self.model = "m"
        self.alive = alive
        self.generates = generates
        self.hang = hang
        self.generations = 0

    async def health(self) -> bool:
        return self.alive

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        self.generations += 1
        if self.hang:
            await asyncio.sleep(self.hang)
        if not self.generates:
            raise RuntimeError("Foundry Local returned HTTP 400")
        return {"choices": [{"message": {"content": "x"}, "finish_reason": "stop"}]}


# -- the circuit breaker -----------------------------------------------


def test_a_breaker_stays_closed_below_the_threshold():
    b = CircuitBreaker(failure_threshold=3)
    for _ in range(2):
        b.record_failure("timeout")
    assert b.state == CLOSED
    assert b.allows() is True


def test_a_breaker_opens_at_the_threshold():
    b = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        b.record_failure("timeout")
    assert b.state == OPEN
    assert b.allows() is False
    assert b.trips == 1
    assert b.retry_after_seconds >= 1


def test_one_success_resets_the_count():
    """A blip is not a trend."""
    b = CircuitBreaker(failure_threshold=3)
    b.record_failure("timeout")
    b.record_failure("timeout")
    b.record_success()
    b.record_failure("timeout")
    assert b.state == CLOSED
    assert b.consecutive_failures == 1


def test_an_open_breaker_half_opens_after_the_cooldown():
    b = CircuitBreaker(failure_threshold=1, reset_seconds=0.0)
    b.record_failure("dead")
    assert b.state == OPEN
    assert b.allows() is True  # cooldown elapsed: one trial
    assert b.state == HALF_OPEN


def test_only_one_trial_passes_while_half_open():
    """A recovering backend must not be hit by the whole backlog at once."""
    b = CircuitBreaker(failure_threshold=1, reset_seconds=0.0)
    b.record_failure("dead")
    assert b.allows() is True  # the trial
    assert b.allows() is False  # everyone else keeps failing fast
    assert b.allows() is False


def test_a_failed_trial_reopens_immediately():
    b = CircuitBreaker(failure_threshold=5, reset_seconds=0.0)
    for _ in range(5):
        b.record_failure("dead")
    b.allows()  # -> half open
    b.record_failure("still dead")
    assert b.state == OPEN  # straight back, without another 5


def test_a_successful_trial_closes_the_breaker():
    b = CircuitBreaker(failure_threshold=1, reset_seconds=0.0)
    b.record_failure("dead")
    b.allows()
    b.record_success()
    assert b.state == CLOSED
    assert b.allows() is True


def test_a_disabled_breaker_never_blocks():
    b = CircuitBreaker(failure_threshold=1, enabled=False)
    b.record_failure("dead")
    assert b.allows() is True


def test_fast_failures_are_counted():
    b = CircuitBreaker(failure_threshold=1, reset_seconds=60.0)
    b.record_failure("dead")
    for _ in range(4):
        b.allows()
    assert b.as_dict()["fast_failures"] == 4


# -- readiness ---------------------------------------------------------


@pytest.mark.asyncio
async def test_reachable_but_unable_to_generate_is_not_ready():
    """The exact shape of the measured failure."""
    health = BackendHealth(FakeClient(alive=True, generates=False))
    assert await health.check_reachable() is True  # liveness passes
    readiness = await health.check_ready("m")
    assert readiness.ready is False  # readiness does not
    assert "rejected a 1-token generation" in readiness.detail


@pytest.mark.asyncio
async def test_a_hanging_backend_is_not_ready_and_does_not_hang_the_probe():
    """180 seconds of silence must not become 180 seconds of health check."""
    health = BackendHealth(FakeClient(hang=5.0), readiness_timeout=1.0)
    readiness = await asyncio.wait_for(health.check_ready("m"), timeout=3.0)
    assert readiness.ready is False
    assert "did not complete" in readiness.detail


@pytest.mark.asyncio
async def test_a_serving_backend_is_ready():
    client = FakeClient()
    health = BackendHealth(client)
    readiness = await health.check_ready("m")
    assert readiness.ready is True
    assert client.generations == 1


@pytest.mark.asyncio
async def test_readiness_is_cached():
    """The probe is a real generation, so it must not run per health check."""
    client = FakeClient()
    health = BackendHealth(client, readiness_interval=60.0)
    for _ in range(10):
        await health.check_ready("m")
    assert client.generations == 1


@pytest.mark.asyncio
async def test_a_forced_probe_bypasses_the_cache():
    client = FakeClient()
    health = BackendHealth(client, readiness_interval=60.0)
    await health.check_ready("m")
    await health.check_ready("m", force=True)
    assert client.generations == 2


@pytest.mark.asyncio
async def test_concurrent_probes_collapse_into_one():
    """Ten health checks at once must not become ten generations."""
    client = FakeClient(hang=0.1)
    health = BackendHealth(client, readiness_interval=0.0)
    await asyncio.gather(*[health.check_ready("m") for _ in range(10)])
    assert client.generations == 1


@pytest.mark.asyncio
async def test_probing_can_be_disabled():
    client = FakeClient()
    health = BackendHealth(client, probe_readiness=False)
    await health.check_reachable()
    readiness = await health.check_ready("m")
    assert client.generations == 0
    assert readiness.ready is True  # falls back to liveness
    assert "disabled" in readiness.detail


@pytest.mark.asyncio
async def test_guard_raises_only_when_open():
    health = BackendHealth(FakeClient(), breaker=CircuitBreaker(failure_threshold=1))
    health.guard()  # closed: fine
    health.record_failure("dead")
    with pytest.raises(BackendUnavailable, match="not serving"):
        health.guard()


@pytest.mark.asyncio
async def test_rebinding_discards_stale_readiness():
    """Health that points at a backend nothing else uses is worse than none."""
    good, bad = FakeClient(), FakeClient(generates=False)
    health = BackendHealth(good, readiness_interval=60.0)
    assert (await health.check_ready("m")).ready is True

    health.rebind(bad)
    assert (await health.check_ready("m")).ready is False
