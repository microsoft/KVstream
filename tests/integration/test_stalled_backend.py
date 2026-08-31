"""
The measured incident, reproduced end to end.

Against a live `foundry 0.8.119`, sustained load drove the runtime into a state
where it answered `/v1/models` in 4ms but could not complete a 4-token
generation in 180 seconds. The gateway kept queueing requests behind it, each
waiting the full backend timeout, and `/health` reported green throughout.

These tests pin the three fixes that came out of that: readiness reported
separately from liveness (G-52), drift measured against the calibration baseline
(G-53), and a circuit breaker so the stall is discovered once rather than once
per request (G-54).
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.backend.foundry import FoundryError
from kvstream.backend.foundry_cli import GEN_ABSENT, FoundryCli, start_command_hint
from kvstream.config import Settings

CHAT = {
    "model": "stub-model",
    "messages": [{"role": "user", "content": "hi"}],
    "max_tokens": 8,
}


class StalledBackend:
    """
    Alive on `/v1/models`, incapable of generating — the observed failure.

    `hang` models the part that made it expensive: the request does not fail
    quickly, it occupies the caller for the full backend timeout.
    """

    def __init__(self, hang: float = 0.0) -> None:
        self.model = "stub-model"
        self.base_url = "http://stub"
        self.hang = hang
        self.attempts = 0

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        self.attempts += 1
        if self.hang:
            await asyncio.sleep(self.hang)
        raise FoundryError("Foundry Local is unreachable: timed out")

    async def chat(self, payload, headers=None):
        self.attempts += 1
        raise FoundryError("Foundry Local is unreachable: timed out")
        yield  # pragma: no cover

    async def health(self) -> bool:
        return True  # /v1/models keeps answering

    async def list_models(self):
        return ["stub-model"]

    def stats(self) -> dict:
        return {"base_url": self.base_url, "scans": 0, "usage_reporting": True}

    def unreachable_hint(self) -> str:
        return start_command_hint(GEN_ABSENT)

    async def detect_cli(self) -> FoundryCli:
        return FoundryCli()

    async def aclose(self) -> None:
        pass


def _app(backend, **admission):
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.backend.circuit_breaker_failures = admission.pop("failures", 3)
    settings.backend.circuit_breaker_reset_seconds = admission.pop("reset", 30.0)
    for key, value in admission.items():
        setattr(settings.admission, key, value)
    app = build_app(settings)
    app.state.gateway.backend = backend
    return app, app.state.gateway


@pytest_asyncio.fixture
async def stalled():
    backend = StalledBackend()
    app, gw = _app(backend)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, gw, backend


# -- G-52: health tells the truth --------------------------------------


@pytest.mark.asyncio
async def test_health_is_503_when_the_backend_cannot_generate(stalled):
    c, _, _ = stalled
    r = await c.get("/health")
    body = r.json()
    assert r.status_code == 503
    assert body["backend_reachable"] is True  # exactly what misled us
    assert body["backend_serving"] is False
    assert "not serving" in body["hint"]


@pytest.mark.asyncio
async def test_status_separates_reachable_from_serving(stalled):
    c, _, _ = stalled
    await c.get("/health")
    health = (await c.get("/status")).json()["backend_health"]
    assert health["reachable"] is True
    assert health["readiness"]["ready"] is False


# -- G-54: the stall is discovered once, not once per request ----------


@pytest.mark.asyncio
async def test_the_breaker_opens_and_then_fails_fast(stalled):
    c, gw, backend = stalled

    for _ in range(3):  # threshold
        assert (await c.post("/v1/chat/completions", json=CHAT)).status_code == 502
    assert gw.health.breaker.state == "open"
    attempts_at_trip = backend.attempts

    # Everything after this is refused without touching the backend.
    for _ in range(10):
        r = await c.post("/v1/chat/completions", json=CHAT)
        assert r.status_code == 503
        assert int(r.headers["retry-after"]) >= 1
        assert "circuit breaker" in r.json()["error"]["message"]
    assert backend.attempts == attempts_at_trip


@pytest.mark.asyncio
async def test_failing_fast_is_fast():
    """
    The point of the breaker.

    Without it, each request waits the full backend timeout — so a stalled
    backend turns into that much latency for every caller, and the admission
    queue fills with work that will never complete.
    """
    backend = StalledBackend(hang=0.4)
    app, gw = _app(backend, failures=2)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(2):
            await c.post("/v1/chat/completions", json=CHAT)
        assert gw.health.breaker.state == "open"

        started = asyncio.get_event_loop().time()
        r = await c.post("/v1/chat/completions", json=CHAT)
        elapsed = asyncio.get_event_loop().time() - started

    assert r.status_code == 503
    assert elapsed < 0.2  # nowhere near the 0.4s the backend would take


@pytest.mark.asyncio
async def test_a_rejected_request_never_occupies_the_budget():
    """A dead backend must not be able to fill the admission queue."""
    backend = StalledBackend()
    app, gw = _app(backend, failures=2)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(6):
            await c.post("/v1/chat/completions", json=CHAT)
    assert gw.capacity.in_flight == 0
    assert gw.capacity.waiting == 0
    assert gw.health.breaker.fast_failures > 0


@pytest.mark.asyncio
async def test_the_breaker_recovers_when_the_backend_does():
    backend = StalledBackend()
    app, gw = _app(backend, failures=2, reset=0.0)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(2):
            await c.post("/v1/chat/completions", json=CHAT)
        assert gw.health.breaker.state == "open"

        gw.backend = StubBackend()  # the runtime was restarted
        r = await c.post("/v1/chat/completions", json=CHAT)

    assert r.status_code == 200
    assert gw.health.breaker.state == "closed"


@pytest.mark.asyncio
async def test_client_errors_never_trip_the_breaker():
    """One malformed client must not be able to take the gateway down."""
    app, gw = _app(StubBackend(), failures=2)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(6):
            r = await c.post("/v1/chat/completions", json={"model": "m"})  # no messages
            assert r.status_code == 400
    assert gw.health.breaker.state == "closed"


# -- G-53: drift is visible --------------------------------------------


@pytest.mark.asyncio
async def test_drift_is_reported_in_status_and_metrics():
    app, gw = _app(StubBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=CHAT)
        status = (await c.get("/status")).json()
        metrics = (await c.get("/metrics")).text

    assert status["drift"]["state"] in {"unknown", "ok", "degraded"}
    assert "kvstream_backend_drift_ratio" in metrics
    assert "kvstream_circuit_breaker_state" in metrics
    assert "kvstream_backend_ready" in metrics


@pytest.mark.asyncio
async def test_drift_stays_unknown_without_a_calibration_baseline():
    """A configured budget was never measured, so there is nothing to drift from."""
    app, gw = _app(StubBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(30):
            await c.post("/v1/chat/completions", json=CHAT)
        status = (await c.get("/status")).json()
    assert status["drift"]["state"] == "unknown"
    assert status["drift"]["baseline_seconds_per_token"] == 0.0


@pytest.mark.asyncio
async def test_a_slow_backend_against_a_baseline_reads_degraded():
    app, gw = _app(StubBackend())
    # A baseline far faster than anything real, so live traffic must exceed it.
    gw.drift = type(gw.drift)(1e-7, warn_ratio=2.0, min_samples=3)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(10):
            await c.post("/v1/chat/completions", json=CHAT)
        status = (await c.get("/status")).json()
    assert status["drift"]["state"] == "degraded"
    assert status["drift"]["ratio"] > 2.0


# -- G-55: shed fast, with a Retry-After worth reading -----------------


class SlowBackend(StubBackend):
    """A backend that takes real time per request, so a queue can build."""

    def __init__(self, seconds: float = 0.05) -> None:
        super().__init__()
        self.seconds = seconds

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        await asyncio.sleep(self.seconds)
        return await super().chat_once(payload, headers, timeout)


@pytest.mark.asyncio
async def test_overload_is_refused_on_arrival_not_after_the_timeout():
    """
    The measured problem: every 503 came back at 120.1s, the admission timeout.

    A request the gateway already knows it cannot serve should be told so
    immediately, with a Retry-After derived from the measured drain rate.
    """
    backend = SlowBackend(0.05)
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.max_concurrency = 1
    settings.admission.admission_timeout_seconds = 1.0
    settings.admission.min_rate_samples = 2
    app = build_app(settings)
    gw = app.state.gateway
    gw.backend = backend

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        # Teach the manager how fast this backend actually drains.
        for _ in range(4):
            await c.post("/v1/chat/completions", json=CHAT)

        started = asyncio.get_event_loop().time()
        results = await asyncio.gather(
            *[c.post("/v1/chat/completions", json=CHAT) for _ in range(40)]
        )
        elapsed = asyncio.get_event_loop().time() - started

    shed = [r for r in results if r.status_code == 503]
    assert shed, "expected some load to be shed"
    # The whole point: refused well inside the admission timeout, not at it.
    assert elapsed < 1.0 * len(results)
    assert any("queue would take" in r.json()["error"]["message"] for r in shed)
    for r in shed:
        assert int(r.headers["retry-after"]) >= 1
    assert gw.capacity.stats()["hopeless_rejections"] > 0


@pytest.mark.asyncio
async def test_a_short_queue_is_not_refused():
    """Rejecting early must not become rejecting eagerly."""
    backend = SlowBackend(0.01)
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.max_concurrency = 2
    settings.admission.admission_timeout_seconds = 30.0
    settings.admission.min_rate_samples = 2
    app = build_app(settings)
    gw = app.state.gateway
    gw.backend = backend

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(4):
            await c.post("/v1/chat/completions", json=CHAT)
        results = await asyncio.gather(
            *[c.post("/v1/chat/completions", json=CHAT) for _ in range(8)]
        )

    assert all(r.status_code == 200 for r in results)
    assert gw.capacity.stats()["hopeless_rejections"] == 0
