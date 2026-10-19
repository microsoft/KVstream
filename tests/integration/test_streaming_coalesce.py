"""
G-16: coalescing on the streaming path, and G-17/G-18 cache controls.

Agent swarms overwhelmingly stream, so singleflight that only worked on the
non-streaming path was inactive for the dominant traffic shape — the exact
workload the proposal says caching and coalescing exist to serve.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.cache.broadcast import StreamBroadcast, StreamCoalescer
from kvstream.config import Settings

PAYLOAD = {
    "model": "stub-model",
    "messages": [{"role": "user", "content": "identical"}],
    "temperature": 0.0,
    "stream": True,
}


def _app(**cache_overrides):
    settings = Settings()
    settings.backend.model = "stub-model"
    for key, value in cache_overrides.items():
        setattr(settings.cache, key, value)
    app = build_app(settings)
    stub = StubBackend()
    stub.stream_delay = 0.05
    app.state.gateway.backend = stub
    return app, app.state.gateway, stub


@pytest_asyncio.fixture
async def client():
    app, gw, stub = _app(enabled=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, gw, stub


def _text_of(body: str) -> str:
    out = []
    for line in body.splitlines():
        if line.startswith("data: "):
            raw = line[6:].strip()
            if raw != "[DONE]":
                out.append(json.loads(raw)["choices"][0]["delta"].get("content", ""))
    return "".join(out)


async def _stream(c: AsyncClient) -> str:
    async with c.stream("POST", "/v1/chat/completions", json=PAYLOAD) as resp:
        assert resp.status_code == 200
        return "".join([chunk async for chunk in resp.aiter_text()])


# -- the broadcast primitive -------------------------------------------


@pytest.mark.asyncio
async def test_a_follower_replays_what_it_missed_then_tracks_the_leader():
    b = StreamBroadcast()
    b.publish(b"one")
    b.publish(b"two")

    seen: list[bytes] = []

    async def follow() -> None:
        async for chunk in b.follow():
            seen.append(chunk)

    task = asyncio.create_task(follow())
    await asyncio.sleep(0)
    assert seen == [b"one", b"two"]  # joined late, saw the whole thing

    b.publish(b"three")
    await asyncio.sleep(0)
    b.close()
    await task
    assert seen == [b"one", b"two", b"three"]


@pytest.mark.asyncio
async def test_a_slow_follower_never_blocks_the_leader():
    b = StreamBroadcast()
    for i in range(1000):
        b.publish(str(i).encode())  # publishing never awaits
    b.close()
    assert b.done is True


@pytest.mark.asyncio
async def test_a_broadcast_error_reaches_followers():
    b = StreamBroadcast()
    b.publish(b"partial")
    b.close(RuntimeError("upstream died"))

    seen = []
    with pytest.raises(RuntimeError, match="upstream died"):
        async for chunk in b.follow():
            seen.append(chunk)
    assert seen == [b"partial"]


@pytest.mark.asyncio
async def test_a_finished_broadcast_is_not_joined():
    coalescer = StreamCoalescer()
    broadcast = coalescer.lead("k")
    broadcast.close()
    assert coalescer.follower_for("k") is None


# -- end to end ---------------------------------------------------------


@pytest.mark.asyncio
async def test_identical_concurrent_streams_hit_the_backend_once(client):
    c, _, stub = client
    bodies = await asyncio.gather(*[_stream(c) for _ in range(5)])

    assert stub.stream_calls == 1  # one upstream call
    assert all(_text_of(b) == "Hello, world!" for b in bodies)
    assert all(b.rstrip().endswith("data: [DONE]") for b in bodies)


@pytest.mark.asyncio
async def test_followers_consume_no_budget(client):
    """A follower is not doing work, so it must not be charged for any."""
    c, gw, _ = client
    await asyncio.gather(*[_stream(c) for _ in range(5)])
    assert gw.capacity.in_flight == 0
    assert gw.capacity.stats()["queue"]["admitted"] == 0  # nobody even queued


@pytest.mark.asyncio
async def test_a_coalesced_response_says_so(client):
    """Exactly one request does the work; the rest are labelled as riders."""
    c, _, stub = client

    async def headers_of() -> str | None:
        async with c.stream("POST", "/v1/chat/completions", json=PAYLOAD) as r:
            await r.aread()
            return r.headers.get("x-kvstream-coalesced")

    results = await asyncio.gather(*[headers_of() for _ in range(4)])
    assert stub.stream_calls == 1
    assert results.count(None) == 1  # the leader
    assert results.count("1") == 3  # the followers


@pytest.mark.asyncio
async def test_different_requests_are_not_coalesced(client):
    c, _, stub = client

    async def other() -> str:
        payload = {**PAYLOAD, "messages": [{"role": "user", "content": "different"}]}
        async with c.stream("POST", "/v1/chat/completions", json=payload) as r:
            return "".join([chunk async for chunk in r.aiter_text()])

    await asyncio.gather(_stream(c), other())
    assert stub.stream_calls == 2


@pytest.mark.asyncio
async def test_non_deterministic_streams_are_never_coalesced(client):
    c, _, stub = client
    payload = {**PAYLOAD, "temperature": 0.8}

    async def go() -> None:
        async with c.stream("POST", "/v1/chat/completions", json=payload) as r:
            await r.aread()

    await asyncio.gather(*[go() for _ in range(3)])
    assert stub.stream_calls == 3


# -- cache controls -----------------------------------------------------


@pytest.mark.asyncio
async def test_no_store_skips_the_cache_in_both_directions():
    app, _, stub = _app(enabled=True)
    body = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "x"}],
        "temperature": 0.0,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=body, headers={"Cache-Control": "no-store"})
        await c.post("/v1/chat/completions", json=body, headers={"Cache-Control": "no-store"})
        # Nothing was written, so a normal request still misses.
        await c.post("/v1/chat/completions", json=body)
    assert stub.once_calls == 3


@pytest.mark.asyncio
async def test_no_cache_refetches_but_still_refreshes_the_entry():
    app, _, stub = _app(enabled=True)
    body = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "x"}],
        "temperature": 0.0,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=body)  # miss, stores
        await c.post(
            "/v1/chat/completions", json=body, headers={"Cache-Control": "no-cache"}
        )  # forced refetch
        await c.post("/v1/chat/completions", json=body)  # hit
    assert stub.once_calls == 2


@pytest.mark.asyncio
async def test_the_kvstream_header_works_too():
    app, _, stub = _app(enabled=True)
    body = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "x"}],
        "temperature": 0.0,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=body)
        await c.post("/v1/chat/completions", json=body, headers={"x-kvstream-cache": "no-store"})
    assert stub.once_calls == 2


@pytest.mark.asyncio
async def test_request_headers_can_be_ignored_by_policy():
    app, _, stub = _app(enabled=True, respect_request_headers=False)
    body = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "x"}],
        "temperature": 0.0,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=body)
        await c.post("/v1/chat/completions", json=body, headers={"Cache-Control": "no-store"})
    assert stub.once_calls == 1  # the directive was not honoured


@pytest.mark.asyncio
async def test_an_oversized_response_is_not_cached():
    """One huge entry can evict everything useful; skipping it is better."""
    app, _, stub = _app(enabled=True, max_entry_bytes=1024)
    stub.reply = "x" * 50_000
    body = {
        "model": "stub-model",
        "messages": [{"role": "user", "content": "x"}],
        "temperature": 0.0,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/chat/completions", json=body)
        await c.post("/v1/chat/completions", json=body)
        metrics = (await c.get("/metrics")).text
    assert stub.once_calls == 2
    assert 'kvstream_cache_skipped_total{reason="too_large"} 2.0' in metrics
