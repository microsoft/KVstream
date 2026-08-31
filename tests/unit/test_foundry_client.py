"""
Wire-level tests for :class:`FoundryClient`.

These cover the two things the integration stubs cannot see, because the stubs
*replace* the client: what KVStream actually puts on the wire, and how often it
rescans for a backend that has gone away.
"""

from __future__ import annotations

import httpx
import pytest

from kvstream.backend import discovery
from kvstream.backend.foundry import FoundryClient, FoundryError

SSE_BODY = (
    'data: {"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}\n'
    "\n"
    'data: {"choices":[{"delta":{"content":"!"},"finish_reason":"stop"}]}\n'
    "\n"
    'data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":2,'
    '"total_tokens":13}}\n'
    "\n"
    "data: [DONE]\n"
    "\n"
)


def _client(handler) -> FoundryClient:
    c = FoundryClient(base_url="http://backend:1234", model="m", discover=False)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return c


@pytest.mark.asyncio
async def test_streaming_requests_usage_by_default():
    """G-14: without stream_options, most backends never report usage at all."""
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.content))
        return httpx.Response(200, text=SSE_BODY, headers={"content-type": "text/event-stream"})

    c = _client(handler)
    tokens = [t async for t in c.chat({"model": "m", "messages": []})]
    await c.aclose()

    assert seen[0]["stream"] is True
    assert seen[0]["stream_options"] == {"include_usage": True}
    assert tokens[-1].usage == {
        "prompt_tokens": 11,
        "completion_tokens": 2,
        "total_tokens": 13,
    }


@pytest.mark.asyncio
async def test_stream_options_rejection_falls_back_once():
    """A backend that refuses the field must not break the request path."""
    attempts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.content)
        attempts.append(body)
        if "stream_options" in body:
            return httpx.Response(400, text='{"error":"unknown field stream_options"}')
        return httpx.Response(200, text=SSE_BODY, headers={"content-type": "text/event-stream"})

    c = _client(handler)
    tokens = [t async for t in c.chat({"model": "m", "messages": []})]

    assert len(attempts) == 2
    assert "stream_options" in attempts[0]
    assert "stream_options" not in attempts[1]
    assert "".join(t.text for t in tokens) == "Hi!"
    assert c.usage_reporting is False

    # The rejection is remembered: the next call does not pay for it again.
    attempts.clear()
    [t async for t in c.chat({"model": "m", "messages": []})]
    assert len(attempts) == 1
    assert "stream_options" not in attempts[0]
    await c.aclose()


@pytest.mark.asyncio
async def test_real_backend_error_is_not_mistaken_for_a_rejection():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="model crashed")

    c = _client(handler)
    with pytest.raises(FoundryError, match="500"):
        [t async for t in c.chat({"model": "m", "messages": []})]
    await c.aclose()


@pytest.mark.asyncio
async def test_chat_once_forwards_non_stream_and_returns_usage():
    """G-14: a non-streamed client request is forwarded non-streamed."""
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "Hi!"}, "finish_reason": "stop"}],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 2,
                    "total_tokens": 13,
                },
            },
        )

    c = _client(handler)
    body = await c.chat_once({"model": "m", "messages": [], "stream": True})
    await c.aclose()

    assert seen[0]["stream"] is False
    assert "stream_options" not in seen[0]
    assert body["usage"]["prompt_tokens"] == 11


@pytest.mark.asyncio
async def test_chat_once_maps_errors_to_foundry_error():
    c = _client(lambda request: httpx.Response(503, text="busy"))
    with pytest.raises(FoundryError, match="503"):
        await c.chat_once({"model": "m", "messages": []})
    await c.aclose()


@pytest.mark.asyncio
async def test_discovery_is_cooldown_throttled(monkeypatch):
    """G-28: a dead backend must not trigger a port sweep per request."""
    scans = 0

    async def fake_discover(client, configured_url, exclude, prefer_model=None):
        nonlocal scans
        scans += 1
        return None

    async def never_responds(client, url):
        return None

    monkeypatch.setattr(discovery, "discover", fake_discover)
    monkeypatch.setattr(discovery, "probe_url", never_responds)

    c = FoundryClient(
        base_url="http://backend:1234",
        discover=True,
        discovery_cooldown=60.0,
        use_foundry_cli=False,
    )
    for _ in range(5):
        assert await c.resolve_url() == "http://backend:1234"
    assert scans == 1
    assert c.scans == 1
    await c.aclose()


@pytest.mark.asyncio
async def test_zero_cooldown_rescans_every_time(monkeypatch):
    scans = 0

    async def fake_discover(client, configured_url, exclude, prefer_model=None):
        nonlocal scans
        scans += 1
        return None

    async def never_responds(client, url):
        return None

    monkeypatch.setattr(discovery, "discover", fake_discover)
    monkeypatch.setattr(discovery, "probe_url", never_responds)

    c = FoundryClient(
        base_url="http://backend:1234",
        discover=True,
        discovery_cooldown=0.0,
        use_foundry_cli=False,
    )
    for _ in range(3):
        await c.resolve_url()
    assert scans == 3
    await c.aclose()
