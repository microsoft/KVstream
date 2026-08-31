"""Integration tests: the full FastAPI gateway against a stub backend."""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.config import Settings


@pytest_asyncio.fixture
async def client():
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.cache.enabled = True
    app = build_app(settings)
    stub = StubBackend()
    app.state.gateway.backend = stub  # inject stub
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c, stub


@pytest.mark.asyncio
async def test_health(client):
    c, _ = client
    r = await c.get("/health")
    assert r.status_code == 200
    assert r.json()["backend_healthy"] is True


@pytest.mark.asyncio
async def test_models(client):
    c, _ = client
    r = await c.get("/v1/models")
    assert r.status_code == 200
    assert any(m["id"] == "stub-model" for m in r.json()["data"])


@pytest.mark.asyncio
async def test_nonstreaming_completion(client):
    c, _ = client
    r = await c.post(
        "/v1/chat/completions",
        json={
            "model": "stub-model",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 32,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["message"]["content"] == "Hello, world!"
    assert body["usage"]["total_tokens"] > 0


@pytest.mark.asyncio
async def test_streaming_completion(client):
    c, _ = client
    chunks = []
    async with c.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "stub-model",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
    ) as resp:
        assert resp.status_code == 200
        async for line in resp.aiter_lines():
            if line.startswith("data: "):
                chunks.append(line[6:])
    assert chunks[-1] == "[DONE]"
    text = "".join(
        json.loads(c)["choices"][0]["delta"].get("content", "")
        for c in chunks
        if c != "[DONE]"
    )
    assert text == "Hello, world!"


@pytest.mark.asyncio
async def test_cache_hit_skips_backend(client):
    c, stub = client
    payload = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "cache me"}],
        "temperature": 0.0,  # deterministic → cacheable
        "max_tokens": 16,
    }
    r1 = await c.post("/v1/chat/completions", json=payload)
    r2 = await c.post("/v1/chat/completions", json=payload)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["choices"][0]["message"]["content"] == "Hello, world!"
    assert stub.calls == 1  # second request served from cache


@pytest.mark.asyncio
async def test_non_deterministic_not_cached(client):
    c, stub = client
    payload = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "vary"}],
        "temperature": 0.8,  # not cacheable
        "max_tokens": 16,
    }
    await c.post("/v1/chat/completions", json=payload)
    await c.post("/v1/chat/completions", json=payload)
    assert stub.calls == 2  # each hit the backend


@pytest.mark.asyncio
async def test_status_and_metrics(client):
    c, _ = client
    s = await c.get("/status")
    assert s.status_code == 200
    assert "admission" in s.json()
    m = await c.get("/metrics")
    assert m.status_code == 200
    assert b"kvstream_budget" in m.content
