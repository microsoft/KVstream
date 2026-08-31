"""
Integration: the gateway prefers real backend token counts and calibrates from them.

Covers both request paths, including the streaming one — where the trailing
``usage`` chunk arrives *after* the finish_reason and would be missed if the
gateway stopped consuming early.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from kvstream.app import build_app
from kvstream.backend.foundry import Token
from kvstream.config import Settings

PROMPT = "a" * 3000  # 3,000 characters
REAL_PROMPT_TOKENS = 1000  # => a true ratio of 3.0 chars/token
REAL_COMPLETION_TOKENS = 7


USAGE = {
    "prompt_tokens": REAL_PROMPT_TOKENS,
    "completion_tokens": REAL_COMPLETION_TOKENS,
    "total_tokens": REAL_PROMPT_TOKENS + REAL_COMPLETION_TOKENS,
}


class UsageStubBackend:
    """
    Stub standing in for :class:`FoundryClient`, reporting real usage on both paths.

    Whether the *wire* carries ``stream_options.include_usage`` is the client's
    responsibility and is covered in ``tests/unit/test_foundry_client.py``;
    here the client's contract is simply assumed to be honoured.
    """

    def __init__(self) -> None:
        self.model = "stub-model"
        self.base_url = "http://stub"
        self.calls = 0
        self.once_calls = 0
        self.payloads: list[dict] = []

    async def chat(self, payload, headers=None):
        self.calls += 1
        self.payloads.append(payload)
        for piece in ["Hello", ", ", "world", "!"]:
            yield Token(text=piece)
        yield Token(text="", finish_reason="stop")
        yield Token(text="", usage=dict(USAGE))

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        self.calls += 1
        self.once_calls += 1
        self.payloads.append(payload)
        return {
            "id": "stub-1",
            "object": "chat.completion",
            "model": self.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "Hello, world!"},
                    "finish_reason": "stop",
                }
            ],
            "usage": dict(USAGE),
        }

    async def health(self) -> bool:
        return True

    async def list_models(self):
        return ["stub-model"]

    def stats(self) -> dict:
        return {"base_url": self.base_url, "scans": 0, "usage_reporting": True}

    async def aclose(self) -> None:
        pass


@pytest_asyncio.fixture
async def client():
    settings = Settings()
    settings.backend.model = "stub-model"
    app = build_app(settings)
    app.state.gateway.backend = UsageStubBackend()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, app.state.gateway


def _payload(stream: bool) -> dict:
    return {
        "model": "stub-model",
        "messages": [{"role": "user", "content": PROMPT}],
        "max_tokens": 16,
        "stream": stream,
    }


@pytest.mark.asyncio
async def test_nonstreaming_reports_real_usage_and_calibrates(client):
    c, gw = client
    assert gw.estimator.calibrated is False

    r = await c.post("/v1/chat/completions", json=_payload(stream=False))
    assert r.status_code == 200

    usage = r.json()["usage"]
    assert usage["prompt_tokens"] == REAL_PROMPT_TOKENS
    assert usage["completion_tokens"] == REAL_COMPLETION_TOKENS

    # The estimator learned from the real counts and moved toward 3.0 chars/token.
    assert gw.estimator.calibrated is True
    assert gw.estimator.samples == 1
    assert gw.estimator.chars_per_token < 4.0


@pytest.mark.asyncio
async def test_streaming_path_also_calibrates(client):
    """Regression: the trailing usage chunk must not be dropped when streaming."""
    c, gw = client
    async with c.stream("POST", "/v1/chat/completions", json=_payload(stream=True)) as resp:
        assert resp.status_code == 200
        async for _ in resp.aiter_lines():
            pass

    assert gw.estimator.calibrated is True
    assert gw.estimator.samples == 1
    assert gw.estimator.chars_per_token < 4.0


@pytest.mark.asyncio
async def test_repeated_calls_converge_on_the_true_ratio(client):
    c, gw = client
    for _ in range(25):
        r = await c.post("/v1/chat/completions", json=_payload(stream=False))
        assert r.status_code == 200
    assert abs(gw.estimator.chars_per_token - 3.0) < 0.1  # converged on truth


@pytest.mark.asyncio
async def test_status_exposes_estimator_state(client):
    c, _ = client
    await c.post("/v1/chat/completions", json=_payload(stream=False))
    body = (await c.get("/status")).json()
    assert body["token_estimator"]["calibrated"] is True
    assert body["token_estimator"]["samples"] == 1

    metrics = (await c.get("/metrics")).content
    assert b"kvstream_chars_per_token" in metrics
    assert b"kvstream_token_ratio_samples" in metrics
