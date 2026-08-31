"""
G-12 / G-14 end to end: the gateway forwards the shape the client asked for, and
settles its reservation against what the response actually cost.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.config import Settings

BIG_MAX_TOKENS = 4096


@pytest_asyncio.fixture
async def token_mode():
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.mode = "tokens"
    settings.admission.budget_tokens = 100_000
    app = build_app(settings)
    stub = StubBackend()
    app.state.gateway.backend = stub
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, app.state.gateway, stub


def _payload(stream: bool) -> dict:
    return {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": BIG_MAX_TOKENS,
        "stream": stream,
    }


@pytest.mark.asyncio
async def test_token_mode_is_actually_in_use(token_mode):
    c, gw, _ = token_mode
    stats = (await c.get("/status")).json()
    assert stats["admission"]["unit"] == "tokens"
    assert stats["budget_source"]["source"] == "configured"


@pytest.mark.asyncio
async def test_non_streamed_requests_are_forwarded_non_streamed(token_mode):
    """G-14: this is what makes the backend's own `usage` block reachable."""
    c, _, stub = token_mode
    r = await c.post("/v1/chat/completions", json=_payload(stream=False))
    assert r.status_code == 200
    assert stub.once_calls == 1
    assert stub.stream_calls == 0
    assert stub.payloads[0]["stream"] is False


@pytest.mark.asyncio
async def test_streamed_requests_still_stream(token_mode):
    c, _, stub = token_mode
    async with c.stream("POST", "/v1/chat/completions", json=_payload(stream=True)) as resp:
        assert resp.status_code == 200
        async for _ in resp.aiter_lines():
            pass
    assert stub.stream_calls == 1
    assert stub.once_calls == 0


@pytest.mark.asyncio
async def test_unused_headroom_is_reclaimed_on_the_non_streaming_path(token_mode):
    c, gw, _ = token_mode
    r = await c.post("/v1/chat/completions", json=_payload(stream=False))
    assert r.status_code == 200

    # Admitted on prompt + 4096; the response was a handful of tokens.
    completion = r.json()["usage"]["completion_tokens"]
    assert completion < 50
    assert gw.capacity.reclaimed >= BIG_MAX_TOKENS - completion
    assert gw.capacity.overshoots == 0
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_unused_headroom_is_reclaimed_on_the_streaming_path(token_mode):
    c, gw, _ = token_mode
    async with c.stream("POST", "/v1/chat/completions", json=_payload(stream=True)) as resp:
        async for _ in resp.aiter_lines():
            pass
    assert gw.capacity.reclaimed > 0
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_reclaim_is_visible_in_status_and_metrics(token_mode):
    c, _, _ = token_mode
    await c.post("/v1/chat/completions", json=_payload(stream=False))

    admission = (await c.get("/status")).json()["admission"]
    assert admission["reclaimed"] > 0
    assert admission["overshoots"] == 0
    assert admission["active"] == 0

    metrics = (await c.get("/metrics")).content
    assert b"kvstream_reservation_reclaimed_tokens_total" in metrics
    assert b"kvstream_reservation_overshoot_total" in metrics


@pytest.mark.asyncio
async def test_concurrency_mode_reclaims_nothing():
    """Live accounting is a token-mode concept; slots are indivisible."""
    settings = Settings()
    settings.backend.model = "stub-model"  # default mode: concurrency
    app = build_app(settings)
    app.state.gateway.backend = StubBackend()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=_payload(stream=False))
    assert app.state.gateway.capacity.reclaimed == 0
