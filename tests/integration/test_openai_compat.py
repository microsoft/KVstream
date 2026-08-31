"""
G-40…G-46 end to end: a tool-calling agent can actually route through KVStream.

The proposal's whole premise is multi-agent orchestration, and those workloads
are built on tool calling. These tests drive the shapes a real agent framework
produces — an assistant turn with `content: null` and `tool_calls`, a `tool`
result message, `response_format`, `seed` — and assert that what reaches the
backend and what comes back are the client's own objects, not reconstructions.
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from kvstream.app import build_app
from kvstream.backend.foundry import FoundryError, Token
from kvstream.backend.foundry_cli import GEN_ABSENT, FoundryCli, start_command_hint
from kvstream.config import Settings

TOOL = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Look up the weather",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}

TOOL_CALL = {
    "id": "call_abc",
    "type": "function",
    "function": {"name": "get_weather", "arguments": '{"city":"Lisbon"}'},
}

# The full second turn of an agent loop: the assistant's tool call, then the
# tool's result. Every field here was rejected or dropped before Milestone C.
AGENT_MESSAGES = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Weather in Lisbon?"},
    {"role": "assistant", "content": None, "tool_calls": [TOOL_CALL]},
    {"role": "tool", "tool_call_id": "call_abc", "content": "22C, sunny"},
]

TOOL_RESPONSE_BODY = {
    "id": "chatcmpl-backend-1",
    "object": "chat.completion",
    "created": 1700000000,
    "model": "backend-model-name",
    "system_fingerprint": "fp_deadbeef",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": None, "tool_calls": [TOOL_CALL]},
            "logprobs": None,
            "finish_reason": "tool_calls",
        }
    ],
    "usage": {"prompt_tokens": 31, "completion_tokens": 12, "total_tokens": 43},
}

# Raw SSE chunks as a real server sends them: a role-only opener, tool-call
# argument deltas, a finish chunk, then usage.
TOOL_STREAM_CHUNKS = [
    {
        "id": "c1",
        "object": "chat.completion.chunk",
        "model": "backend-model-name",
        "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
    },
    {
        "id": "c1",
        "object": "chat.completion.chunk",
        "model": "backend-model-name",
        "choices": [
            {
                "index": 0,
                "delta": {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_abc",
                            "type": "function",
                            "function": {"name": "get_weather", "arguments": ""},
                        }
                    ]
                },
                "finish_reason": None,
            }
        ],
    },
    {
        "id": "c1",
        "object": "chat.completion.chunk",
        "model": "backend-model-name",
        "choices": [
            {
                "index": 0,
                "delta": {
                    "tool_calls": [{"index": 0, "function": {"arguments": '{"city":"Lisbon"}'}}]
                },
                "finish_reason": None,
            }
        ],
    },
    {
        "id": "c1",
        "object": "chat.completion.chunk",
        "model": "backend-model-name",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
    },
    {
        "id": "c1",
        "object": "chat.completion.chunk",
        "model": "backend-model-name",
        "choices": [],
        "usage": {"prompt_tokens": 31, "completion_tokens": 12, "total_tokens": 43},
    },
]


class EchoBackend:
    """Records exactly what it was handed, and replays canned raw responses."""

    def __init__(self) -> None:
        self.model = "stub-model"
        self.base_url = "http://stub"
        self.payloads: list[dict] = []
        self.headers: list[dict | None] = []
        self.body = TOOL_RESPONSE_BODY
        self.chunks = TOOL_STREAM_CHUNKS
        self.completions_body = {
            "id": "cmpl-1",
            "object": "text_completion",
            "choices": [{"index": 0, "text": "hello", "finish_reason": "stop"}],
        }
        self.model_detail = {"id": "phi-3-mini", "object": "model", "owned_by": "foundry"}
        self.status = 200

    async def chat(self, payload, headers=None):
        self.payloads.append(payload)
        self.headers.append(headers)
        for chunk in self.chunks:
            usage = chunk.get("usage")
            text = ""
            finish = None
            for choice in chunk.get("choices") or []:
                delta = choice.get("delta") or {}
                for call in delta.get("tool_calls") or []:
                    text += (call.get("function") or {}).get("arguments") or ""
                text += delta.get("content") or ""
                finish = choice.get("finish_reason") or finish
            yield Token(text=text, finish_reason=finish, usage=usage, raw=chunk)

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        self.payloads.append(payload)
        self.headers.append(headers)
        return self.body

    async def completions(self, payload, headers=None):
        self.payloads.append(payload)
        self.headers.append(headers)
        return self.status, self.completions_body

    async def embeddings(self, payload, headers=None):
        self.payloads.append(payload)
        self.headers.append(headers)
        return self.status, {"object": "list", "data": [], "usage": {"prompt_tokens": 3}}

    async def get_model(self, model_id, headers=None):
        self.headers.append(headers)
        return self.status, {**self.model_detail, "id": model_id}

    async def health(self) -> bool:
        return True

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


def _build(**overrides):
    settings = Settings()
    settings.backend.model = "stub-model"
    for dotted, value in overrides.items():
        section, _, field = dotted.partition(".")
        setattr(getattr(settings, section) if field else settings, field or section, value)
    app = build_app(settings)
    backend = EchoBackend()
    app.state.gateway.backend = backend
    return app, app.state.gateway, backend


@pytest_asyncio.fixture
async def client():
    app, gw, backend = _build()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, gw, backend


def _sse_payloads(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        if line.startswith("data: "):
            body = line[6:].strip()
            if body != "[DONE]":
                out.append(json.loads(body))
    return out


# -- requests an agent framework actually sends ------------------------


@pytest.mark.asyncio
async def test_a_tool_calling_turn_is_accepted_and_forwarded(client):
    """G-40: this exact request used to be rejected with a 422."""
    c, _, backend = client
    r = await c.post(
        "/v1/chat/completions",
        json={
            "model": "stub-model",
            "messages": AGENT_MESSAGES,
            "tools": [TOOL],
            "tool_choice": "auto",
        },
    )
    assert r.status_code == 200

    sent = backend.payloads[0]
    assert sent["tools"] == [TOOL]
    assert sent["tool_choice"] == "auto"
    assert sent["messages"][2]["tool_calls"] == [TOOL_CALL]
    assert sent["messages"][2]["content"] is None
    assert sent["messages"][3]["tool_call_id"] == "call_abc"


@pytest.mark.asyncio
async def test_multimodal_content_parts_are_forwarded(client):
    c, _, backend = client
    parts = [
        {"type": "text", "text": "what is this"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
    ]
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": parts}]},
    )
    assert r.status_code == 200
    assert backend.payloads[0]["messages"][0]["content"] == parts


@pytest.mark.asyncio
async def test_sampling_and_format_fields_are_not_dropped(client):
    """G-42: a client asking for JSON mode used to silently get prose."""
    c, _, backend = client
    extras = {
        "seed": 42,
        "response_format": {"type": "json_object"},
        "frequency_penalty": 0.5,
        "presence_penalty": 0.25,
        "logit_bias": {"1234": -100},
        "logprobs": True,
        "top_logprobs": 2,
        "user": "agent-7",
        "top_p": 0.9,
    }
    await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}], **extras},
    )
    sent = backend.payloads[0]
    for field, value in extras.items():
        assert sent[field] == value, f"{field} was dropped or altered"


@pytest.mark.asyncio
async def test_absent_sampling_fields_are_not_invented(client):
    """KVStream must not impose its own defaults on the model."""
    c, _, backend = client
    await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
    )
    sent = backend.payloads[0]
    assert "max_tokens" not in sent
    assert "temperature" not in sent
    assert "top_p" not in sent


# -- responses ---------------------------------------------------------


@pytest.mark.asyncio
async def test_the_response_is_the_backends_own_object(client):
    """G-43: tool_calls, system_fingerprint and the real id all survive."""
    c, _, _ = client
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": AGENT_MESSAGES, "tools": [TOOL]},
    )
    body = r.json()
    assert body == TOOL_RESPONSE_BODY
    assert body["choices"][0]["message"]["tool_calls"] == [TOOL_CALL]
    assert body["system_fingerprint"] == "fp_deadbeef"
    assert body["id"] == "chatcmpl-backend-1"
    assert body["choices"][0]["finish_reason"] == "tool_calls"
    # The gateway still identifies the request it handled.
    assert r.headers["x-request-id"].startswith("kvs-")


@pytest.mark.asyncio
async def test_streamed_tool_call_deltas_survive(client):
    """G-43: the role delta and tool-call deltas were unrepresentable before."""
    c, _, _ = client
    async with c.stream(
        "POST",
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": AGENT_MESSAGES, "tools": [TOOL], "stream": True},
    ) as resp:
        assert resp.status_code == 200
        text = "".join([chunk async for chunk in resp.aiter_text()])

    chunks = _sse_payloads(text)
    assert chunks[0]["choices"][0]["delta"] == {"role": "assistant"}
    assert chunks[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "get_weather"
    assert chunks[2]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"] == (
        '{"city":"Lisbon"}'
    )
    assert chunks[3]["choices"][0]["finish_reason"] == "tool_calls"
    assert text.rstrip().endswith("data: [DONE]")


@pytest.mark.asyncio
async def test_usage_chunk_is_not_leaked_to_a_client_that_did_not_ask(client):
    """KVStream asks for usage for its own accounting; that is not the client's business."""
    c, gw, _ = client
    async with c.stream(
        "POST",
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": AGENT_MESSAGES, "stream": True},
    ) as resp:
        text = "".join([chunk async for chunk in resp.aiter_text()])

    chunks = _sse_payloads(text)
    assert not any("usage" in chunk for chunk in chunks)
    # ...but the gateway still learned from it.
    assert gw.estimator.calibrated is True


@pytest.mark.asyncio
async def test_usage_chunk_is_delivered_when_the_client_asks(client):
    c, _, _ = client
    async with c.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "stub-model",
            "messages": AGENT_MESSAGES,
            "stream": True,
            "stream_options": {"include_usage": True},
        },
    ) as resp:
        text = "".join([chunk async for chunk in resp.aiter_text()])

    chunks = _sse_payloads(text)
    usage_chunks = [chunk for chunk in chunks if "usage" in chunk]
    assert len(usage_chunks) == 1
    assert usage_chunks[0]["usage"]["total_tokens"] == 43


@pytest.mark.asyncio
async def test_estimated_usage_is_disclosed_in_a_header(client):
    """When the backend reports nothing, say the counts are ours."""
    c, _, backend = client
    backend.body = {
        "id": "x",
        "object": "chat.completion",
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}
        ],
    }
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.headers["x-kvstream-usage"] == "estimated"
    assert r.json()["usage"]["total_tokens"] > 0


@pytest.mark.asyncio
async def test_real_usage_is_not_labelled_estimated(client):
    c, _, _ = client
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": AGENT_MESSAGES},
    )
    assert "x-kvstream-usage" not in r.headers
    assert r.json()["usage"]["total_tokens"] == 43


# -- caching a tool call -----------------------------------------------


@pytest.mark.asyncio
async def test_a_cached_tool_call_is_replayed_intact():
    """A cache that dropped tool_calls would hand an agent an empty turn."""
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.cache.enabled = True
    app = build_app(settings)
    backend = EchoBackend()
    app.state.gateway.backend = backend

    payload = {
        "model": "stub-model",
        "messages": AGENT_MESSAGES,
        "tools": [TOOL],
        "temperature": 0.0,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        first = await c.post("/v1/chat/completions", json=payload)
        second = await c.post("/v1/chat/completions", json=payload)

    assert len(backend.payloads) == 1  # second served from cache
    assert first.json() == second.json()
    assert second.json()["choices"][0]["message"]["tool_calls"] == [TOOL_CALL]


@pytest.mark.asyncio
async def test_a_cached_stream_replays_the_recorded_chunks():
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.cache.enabled = True
    app = build_app(settings)
    backend = EchoBackend()
    app.state.gateway.backend = backend

    payload = {
        "model": "stub-model",
        "messages": AGENT_MESSAGES,
        "temperature": 0.0,
        "stream": True,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        async with c.stream("POST", "/v1/chat/completions", json=payload) as r1:
            live = "".join([chunk async for chunk in r1.aiter_text()])
        async with c.stream("POST", "/v1/chat/completions", json=payload) as r2:
            replayed = "".join([chunk async for chunk in r2.aiter_text()])

    assert len(backend.payloads) == 1
    assert _sse_payloads(live) == _sse_payloads(replayed)
    assert _sse_payloads(replayed)[1]["choices"][0]["delta"]["tool_calls"]


@pytest.mark.asyncio
async def test_streamed_and_non_streamed_entries_do_not_collide():
    """The key includes `stream`, so a body is never replayed as chunks."""
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.cache.enabled = True
    app = build_app(settings)
    backend = EchoBackend()
    app.state.gateway.backend = backend

    base = {"model": "stub-model", "messages": AGENT_MESSAGES, "temperature": 0.0}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        plain = await c.post("/v1/chat/completions", json=base)
        async with c.stream(
            "POST", "/v1/chat/completions", json={**base, "stream": True}
        ) as streamed:
            body = "".join([chunk async for chunk in streamed.aiter_text()])

    assert plain.json()["object"] == "chat.completion"
    assert _sse_payloads(body)[0]["object"] == "chat.completion.chunk"
    assert len(backend.payloads) == 2  # each shape fetched once


# -- auth passthrough ---------------------------------------------------


@pytest.mark.asyncio
async def test_authorization_header_is_forwarded(client):
    """G-45: KVStream may sit behind a gateway that does per-caller auth."""
    c, _, backend = client
    await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
        headers={"Authorization": "Bearer caller-token"},
    )
    assert backend.headers[0] == {"Authorization": "Bearer caller-token"}


@pytest.mark.asyncio
async def test_authorization_forwarding_can_be_disabled():
    app, _, backend = _build(**{"backend.forward_authorization": False})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post(
            "/v1/chat/completions",
            json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
            headers={"Authorization": "Bearer caller-token"},
        )
    assert backend.headers[0] is None


# -- the remaining OpenAI surface --------------------------------------


@pytest.mark.asyncio
async def test_legacy_completions_are_proxied_and_admitted(client):
    """G-46: an unadmitted route is a way around admission control."""
    c, gw, backend = client
    r = await c.post(
        "/v1/completions",
        json={"model": "stub-model", "prompt": "once upon a time", "max_tokens": 32},
    )
    assert r.status_code == 200
    assert r.json()["object"] == "text_completion"
    assert backend.payloads[0]["prompt"] == "once upon a time"
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_completions_can_be_turned_off():
    app, _, _ = _build(**{"routes.completions": False})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/completions", json={"model": "m", "prompt": "x"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_model_detail_is_proxied(client):
    c, _, _ = client
    r = await c.get("/v1/models/phi-3-mini")
    assert r.status_code == 200
    assert r.json()["id"] == "phi-3-mini"
    assert r.json()["owned_by"] == "foundry"


@pytest.mark.asyncio
async def test_a_choiceless_body_is_still_reported_as_a_broken_upstream(client):
    """Proxying faithfully does not mean laundering a malformed response."""
    c, _, backend = client
    backend.body = {"id": "x", "choices": []}
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.status_code == 502
    assert "no choices" in r.json()["error"]["message"]


@pytest.mark.asyncio
async def test_admission_costs_n_choices_and_the_default_budget():
    """G-44: `n` is real KV, and an absent max_tokens still has to be costed."""
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.mode = "tokens"
    settings.admission.budget_tokens = 1_000_000
    settings.admission.default_max_tokens = 500
    app = build_app(settings)
    gw = app.state.gateway
    gw.backend = EchoBackend()

    from kvstream.models import ChatCompletionRequest

    msgs = [{"role": "user", "content": "hi"}]
    _, one = gw._cost(ChatCompletionRequest(model="m", messages=msgs, max_tokens=100))
    _, four = gw._cost(ChatCompletionRequest(model="m", messages=msgs, max_tokens=100, n=4))
    _, default = gw._cost(ChatCompletionRequest(model="m", messages=msgs))

    assert four - one == 300  # three extra completions of 100
    assert default - one == 400  # 500 assumed instead of 100


@pytest.mark.asyncio
async def test_backend_errors_still_map_to_502(client):
    c, _, backend = client

    async def boom(payload, headers=None):
        raise FoundryError("Foundry Local returned HTTP 500: model crashed")

    backend.chat_once = boom
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.status_code == 502
