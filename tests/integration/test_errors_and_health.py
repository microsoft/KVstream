"""
G-31/G-32/G-33/G-36: what a client and an orchestrator see when things go wrong.

A gateway whose whole value is "clean backpressure" has to make that
backpressure machine-readable: OpenAI-shaped error bodies, a `Retry-After` on
503, a real status code on `/health`, and a refusal to run as one of several
workers (which would silently multiply the budget).
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from kvstream.admission import QueueFull
from kvstream.app import build_app
from kvstream.backend.foundry import FoundryError, Token
from kvstream.backend.foundry_cli import GEN_ABSENT, FoundryCli, start_command_hint
from kvstream.config import Settings

PAYLOAD = {"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]}


class BrokenBackend:
    """Stands in for a Foundry Local that is up but failing."""

    def __init__(self, healthy: bool = True) -> None:
        self.model = "stub-model"
        self.base_url = "http://stub"
        self._healthy = healthy

    async def chat(self, payload, headers=None):
        raise FoundryError("Foundry Local returned HTTP 500: model crashed")
        yield  # pragma: no cover — makes this an async generator

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        raise FoundryError("Foundry Local returned HTTP 500: model crashed")

    async def health(self) -> bool:
        return self._healthy

    async def list_models(self):
        return []

    def stats(self) -> dict:
        return {"base_url": self.base_url, "scans": 0, "usage_reporting": True}

    def unreachable_hint(self) -> str:
        return start_command_hint(GEN_ABSENT)

    async def detect_cli(self):
        return FoundryCli()

    async def aclose(self) -> None:
        pass


class EmptyBackend(BrokenBackend):
    """Up, healthy, but answers with a body that has no choices."""

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        return {"id": "x", "choices": []}

    async def chat(self, payload, headers=None):
        yield Token(text="ok", finish_reason="stop")


def _app(backend) -> tuple:
    settings = Settings()
    settings.backend.model = "stub-model"
    app = build_app(settings)
    app.state.gateway.backend = backend
    return app, app.state.gateway


@pytest_asyncio.fixture
async def broken():
    app, gw = _app(BrokenBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, gw


@pytest.mark.asyncio
async def test_backend_failure_is_502_not_500(broken):
    """G-31: the non-streaming path had no mapping and leaked a 500."""
    c, _ = broken
    r = await c.post("/v1/chat/completions", json=PAYLOAD)
    assert r.status_code == 502
    body = r.json()
    assert body["error"]["type"] == "upstream_error"
    assert "model crashed" in body["error"]["message"]


@pytest.mark.asyncio
async def test_streaming_backend_failure_is_also_502(broken):
    c, _ = broken
    r = await c.post("/v1/chat/completions", json={**PAYLOAD, "stream": True})
    assert r.status_code == 502
    assert r.json()["error"]["type"] == "upstream_error"


@pytest.mark.asyncio
async def test_backend_failure_releases_the_reservation(broken):
    """A failed request must not leak budget."""
    c, gw = broken
    for _ in range(3):
        await c.post("/v1/chat/completions", json=PAYLOAD)
        await c.post("/v1/chat/completions", json={**PAYLOAD, "stream": True})
    assert gw.capacity.in_flight == 0
    assert gw.capacity.stats()["active"] == 0


@pytest.mark.asyncio
async def test_malformed_backend_body_is_502(broken):
    app, _ = _app(EmptyBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/chat/completions", json=PAYLOAD)
    assert r.status_code == 502
    assert "no choices" in r.json()["error"]["message"]


@pytest.mark.asyncio
async def test_overload_is_503_with_retry_after(broken):
    """G-32: backpressure a client can actually act on."""
    c, gw = broken

    async def always_full(req_id, cost):
        raise QueueFull("admission queue is full")

    gw.capacity.admit = always_full  # type: ignore[method-assign]

    r = await c.post("/v1/chat/completions", json=PAYLOAD)
    assert r.status_code == 503
    assert r.headers["retry-after"] == "1"
    body = r.json()
    assert body["error"]["type"] == "overloaded_error"
    assert "queue full" in body["error"]["message"]


@pytest.mark.asyncio
async def test_validation_errors_use_the_openai_envelope(broken):
    c, _ = broken
    r = await c.post("/v1/chat/completions", json={"model": "m"})  # messages missing
    assert r.status_code == 400
    body = r.json()
    assert body["error"]["type"] == "invalid_request_error"
    assert "messages" in body["error"]["message"]


@pytest.mark.asyncio
async def test_health_reports_degraded_in_the_status_line():
    """G-33: orchestrators key on the code, not the JSON."""
    app, _ = _app(BrokenBackend(healthy=False))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get("/health")
    assert r.status_code == 503
    assert r.json()["backend_healthy"] is False
    assert "foundry server start" in r.json()["hint"]


@pytest.mark.asyncio
async def test_health_is_200_only_when_the_backend_actually_serves():
    """G-52: a backend that answers /v1/models but cannot generate is not ready."""
    from stubs import StubBackend

    app, _ = _app(StubBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get("/health")
    body = r.json()
    assert r.status_code == 200
    assert body["backend_reachable"] is True
    assert body["backend_serving"] is True
    assert body["readiness"]["ready"] is True
    assert "hint" not in body


@pytest.mark.asyncio
async def test_reachable_but_not_serving_is_degraded():
    """
    The measured failure, in a test.

    Real Foundry Local answered /v1/models in 4ms for minutes while a 4-token
    generation never returned. Liveness alone said everything was fine.
    """
    app, _ = _app(BrokenBackend(healthy=True))   # /v1/models ok, generation fails
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get("/health")
    body = r.json()
    assert r.status_code == 503
    assert body["backend_reachable"] is True     # liveness passes...
    assert body["backend_serving"] is False      # ...readiness does not
    assert body["readiness"]["ready"] is False
    assert "not serving" in body["hint"]


@pytest.mark.asyncio
async def test_readiness_is_cached_so_the_probe_is_not_load():
    """A health check that becomes load is a health check that causes outages."""
    from stubs import StubBackend

    backend = StubBackend()
    app, _ = _app(backend)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(5):
            await c.get("/health")
        cached = backend.once_calls
        await c.get("/health?probe=true")          # explicit refresh
    assert cached == 1                              # five checks, one generation
    assert backend.once_calls == 2                  # forced probe ran


def test_refuses_to_start_as_one_of_several_workers(monkeypatch):
    """G-36: N workers would enforce N times the calibrated budget."""
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    with pytest.raises(RuntimeError, match="single process"):
        build_app(Settings())


def test_a_single_worker_is_fine(monkeypatch):
    monkeypatch.setenv("WEB_CONCURRENCY", "1")
    build_app(Settings())
