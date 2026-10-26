"""
Regression tests for the second code review (2026-10-11).

Each test reproduces the scenario that exposed its bug. #3 (`kvstream health`)
is covered in tests/unit/test_cli.py.
"""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.admission.calibration import CalibrationKey, save_budget
from kvstream.app import build_app
from kvstream.backend.foundry import FoundryClient, FoundryError
from kvstream.backend.foundry_cli import FoundryCli
from kvstream.backend.health import CLOSED, OPEN
from kvstream.backend.inventory import ModelInventory
from kvstream.backend.resolver import Resolution
from kvstream.config import Settings

CHAT = {"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]}


# ----------------------------------------------------------------------
# #1 A re-resolved budget never replaces a busy capacity manager
# ----------------------------------------------------------------------


class _LoadsLater(StubBackend):
    """Foundry with nothing loaded at first; the test loads the model."""

    def __init__(self, delay: float) -> None:
        super().__init__()
        self.model = None
        self.delay = delay
        self.loaded: list[str] = []

    async def chat_once(self, payload, headers=None, timeout=None):
        await asyncio.sleep(self.delay)
        return await super().chat_once(payload, headers, timeout)

    async def models_or_none(self):
        return ["stub-model"]

    async def detect_cli(self):
        return FoundryCli(path="foundry", version="0.10.3", generation="0.10.x")

    def use_model(self, model):
        self.model = model


async def _gateway_waiting_for_its_model(tmp_path):
    store = str(tmp_path / "c.json")
    save_budget(store, CalibrationKey("stub-model:2", "cpu", "0.10.x", "0.10.3"), 5000)
    s = Settings()
    s.backend.device = "cpu"
    s.admission.mode = "tokens"
    s.admission.calibration_store = store
    s.admission.max_concurrency = 1  # the fallback while the model is unknown
    s.admission.admission_timeout_seconds = 10
    app = build_app(s)
    gw = app.state.gateway
    backend = _LoadsLater(delay=0.3)
    gw.backend = backend

    async def loaded():
        return list(backend.loaded)

    gw.inventory = ModelInventory(backend.models_or_none, loaded, min_refresh_seconds=0)
    await gw.startup()
    return app, gw, backend


@pytest.mark.asyncio
async def test_a_model_loaded_under_traffic_does_not_strand_queued_requests(tmp_path):
    """Reproduced: the queued request timed out (503 after 6 s) with capacity free."""
    app, gw, backend = await _gateway_waiting_for_its_model(tmp_path)
    old = gw.capacity
    assert (old.unit, old.budget) == ("concurrency", 1)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        first = asyncio.create_task(c.post("/v1/chat/completions", json=CHAT))
        while old.in_flight < 1:
            await asyncio.sleep(0.01)
        queued = asyncio.create_task(c.post("/v1/chat/completions", json=CHAT))
        while old.waiting < 1:
            await asyncio.sleep(0.01)

        backend.loaded = ["stub-model:2"]  # the operator loads the model
        await c.get("/health")
        assert gw.capacity is old  # not swapped under live traffic
        pending = (await c.get("/status")).json()["budget_pending"]
        assert pending is not None and pending["model"] == "stub-model:2"

        r1, r2 = await first, await queued
        assert (r1.status_code, r2.status_code) == (200, 200)
        assert old.idle

        # The next admission finds the gateway idle and installs the budget,
        # and the request is costed by the manager it is admitted into.
        third = asyncio.create_task(c.post("/v1/chat/completions", json=CHAT))
        while gw.capacity is old:
            await asyncio.sleep(0.01)
        new = gw.capacity
        while new.in_flight == 0 and not third.done():
            await asyncio.sleep(0.01)
        assert (new.unit, new.budget) == ("tokens", 5000)
        assert new.in_flight > 1  # charged in tokens, not as 1 "slot"
        assert (await third).status_code == 200
        assert gw._pending_capacity is None
        assert (await c.get("/status")).json()["budget_pending"] is None


@pytest.mark.asyncio
async def test_a_pending_budget_is_never_installed_while_draining(tmp_path):
    _, gw, backend = await _gateway_waiting_for_its_model(tmp_path)
    backend.loaded = ["stub-model:2"]
    await gw.begin_shutdown()
    old = gw.capacity
    await gw.ensure_model()  # resolves the token budget...
    assert gw._pending_capacity is not None
    assert gw.capacity is old  # ...but a fresh manager would not be draining
    assert gw._install_pending_capacity() is False


# ----------------------------------------------------------------------
# #2 Streamed responses are cached with coalescing off
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_streams_are_cached_when_coalescing_is_off():
    s = Settings()
    s.backend.model = "stub-model"
    s.cache.enabled = True
    s.coalesce.enabled = False
    app = build_app(s)
    gw = app.state.gateway
    gw.backend = StubBackend()
    body = {**CHAT, "stream": True, "temperature": 0}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        first = await c.post("/v1/chat/completions", json=body)
        second = await c.post("/v1/chat/completions", json=body)
    assert gw.backend.stream_calls == 1  # previously 2: nothing was cached
    assert second.text == first.text


# ----------------------------------------------------------------------
# #4 /v1/models distinguishes "none" from "could not ask"
# ----------------------------------------------------------------------


class _Models(StubBackend):
    def __init__(self, ids):
        super().__init__()
        self.ids = ids

    async def models_or_none(self):
        return self.ids


@pytest.mark.asyncio
async def test_models_is_502_when_foundry_cannot_be_read_and_200_when_empty():
    for ids, expected in ((None, 502), ([], 200), (["a"], 200)):
        s = Settings()
        s.backend.model = "stub-model"
        app = build_app(s)
        app.state.gateway.backend = _Models(ids)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.get("/v1/models")
        assert r.status_code == expected, ids
        if expected == 200:
            assert [m["id"] for m in r.json()["data"]] == ids


# ----------------------------------------------------------------------
# #5 only-if-cached never calls the backend
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_if_cached_is_504_on_a_miss_and_served_on_a_hit():
    s = Settings()
    s.backend.model = "stub-model"
    s.cache.enabled = True
    app = build_app(s)
    gw = app.state.gateway
    gw.backend = StubBackend()
    body = {**CHAT, "temperature": 0}
    only = {"Cache-Control": "only-if-cached"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        miss = await c.post("/v1/chat/completions", json=body, headers=only)
        assert miss.status_code == 504
        assert miss.json()["error"]["code"] == "cache_miss"
        assert gw.backend.calls == 0  # previously it asked the backend

        await c.post("/v1/chat/completions", json=body)  # populate the cache
        hit = await c.post("/v1/chat/completions", json=body, headers=only)
        assert hit.status_code == 200
        assert gw.backend.calls == 1


# ----------------------------------------------------------------------
# #6 Pass-through 5xx counts toward the breaker; 501 and 4xx do not
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_embeddings_server_errors_open_the_breaker_but_501_does_not():
    payload = {"model": "stub-model", "input": "hello"}
    for status, expected in ((500, OPEN), (503, OPEN), (501, CLOSED), (400, CLOSED)):
        s = Settings()
        s.backend.model = "stub-model"
        app = build_app(s)
        gw = app.state.gateway
        gw.backend = StubBackend()
        gw.backend.embedding_status = status
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            for _ in range(gw.health.breaker.failure_threshold):
                await c.post("/v1/embeddings", json=payload)
        assert gw.health.breaker.state == expected, status


@pytest.mark.asyncio
async def test_a_pass_through_success_resets_the_failure_count():
    s = Settings()
    s.backend.model = "stub-model"
    app = build_app(s)
    gw = app.state.gateway
    gw.backend = StubBackend()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        gw.backend.embedding_status = 500
        await c.post("/v1/embeddings", json={"model": "stub-model", "input": "x"})
        assert gw.health.breaker.consecutive_failures == 1
        gw.backend.embedding_status = 200
        await c.post("/v1/embeddings", json={"model": "stub-model", "input": "x"})
        assert gw.health.breaker.consecutive_failures == 0


# ----------------------------------------------------------------------
# #7 No retry against a URL that rediscovery did not find
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_retry_against_the_fallback_url_when_rediscovery_finds_nothing(monkeypatch):
    seen: list[str] = []

    def refused(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.url.host}:{request.url.port}")
        raise httpx.ConnectError("connection refused", request=request)

    c = FoundryClient(base_url="http://127.0.0.1:5273", model="m", use_foundry_cli=False)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(refused))
    c._resolved_url = "http://127.0.0.1:63962"  # where Foundry was

    async def nothing_found(client):
        return Resolution(None, "none", attempts=["scan"])

    monkeypatch.setattr(c._resolver, "resolve", nothing_found)
    with pytest.raises(FoundryError) as info:
        await c.chat_once({"model": "m", "messages": []})
    assert seen == ["127.0.0.1:63962"]  # previously also tried :5273
    assert "63962" in str(info.value)  # names the URL that refused
    await c.aclose()


# ----------------------------------------------------------------------
# #8 A failed startup still reports the budget
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_failed_startup_still_reports_a_missing_token_budget(caplog):
    s = Settings()
    s.backend.model = "stub-model"
    s.admission.mode = "tokens"
    s.admission.calibration_store = "does-not-exist.json"

    class Broken(StubBackend):
        async def detect_cli(self):
            raise RuntimeError("CLI detection blew up")

    caplog.set_level(logging.WARNING)
    app = build_app(s)
    app.state.gateway.backend = Broken()
    async with app.router.lifespan_context(app):
        pass
    messages = [r.getMessage() for r in caplog.records]
    assert any("startup probe failed" in m for m in messages)
    assert any("Falling back to concurrency mode" in m for m in messages)
