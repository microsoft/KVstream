"""
G-07/G-08/G-09: route coverage (proposal §8.4).

A gateway that admits only chat traffic leaves embeddings and transcription
unprotected — clients route around it, which defeats admission control.
Embeddings cost honestly in input tokens and share the KV-token budget. Audio
does not, and is admitted under a separate plain concurrency limit instead of
being given an invented cost.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.config import Settings

AUDIO = {"file": ("clip.wav", b"RIFF....fake audio bytes", "audio/wav")}


def _app(**overrides):
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.mode = overrides.pop("mode", "concurrency")
    if "budget_tokens" in overrides:
        settings.admission.budget_tokens = overrides.pop("budget_tokens")
    for key, value in overrides.items():
        setattr(settings.routes, key, value)
    app = build_app(settings)
    stub = StubBackend()
    app.state.gateway.backend = stub
    return app, app.state.gateway, stub


@pytest_asyncio.fixture
async def client():
    app, gw, stub = _app(mode="tokens", budget_tokens=100_000)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, gw, stub


# -- embeddings --------------------------------------------------------


@pytest.mark.asyncio
async def test_embeddings_are_proxied_verbatim(client):
    c, _, stub = client
    r = await c.post("/v1/embeddings", json={"model": "stub-model", "input": "hello world"})
    assert r.status_code == 200
    body = r.json()
    assert body["object"] == "list"
    assert len(body["data"]) == 1
    assert body["data"][0]["embedding"] == [0.1, 0.2, 0.3]
    assert stub.embedding_payloads[0] == {"model": "stub-model", "input": "hello world"}


@pytest.mark.asyncio
async def test_embeddings_accept_a_batch(client):
    c, _, stub = client
    r = await c.post("/v1/embeddings", json={"model": "stub-model", "input": ["a", "b", "c"]})
    assert r.status_code == 200
    assert len(r.json()["data"]) == 3


@pytest.mark.asyncio
async def test_embeddings_forward_optional_fields_but_not_absent_ones(client):
    c, _, stub = client
    await c.post(
        "/v1/embeddings",
        json={"model": "stub-model", "input": "x", "dimensions": 256, "encoding_format": "float"},
    )
    payload = stub.embedding_payloads[0]
    assert payload["dimensions"] == 256
    assert payload["encoding_format"] == "float"
    assert "user" not in payload


@pytest.mark.asyncio
async def test_embeddings_are_admitted_against_the_token_budget(client):
    """G-07: cost basis is the input tokens, and the reservation is released."""
    c, gw, _ = client
    r = await c.post("/v1/embeddings", json={"model": "stub-model", "input": "x" * 400})
    assert r.status_code == 200
    assert gw.capacity.in_flight == 0
    assert gw.capacity.stats()["active"] == 0


@pytest.mark.asyncio
async def test_embeddings_usage_calibrates_the_estimator(client):
    """Embedding responses carry real prompt counts too."""
    c, gw, _ = client
    assert gw.estimator.calibrated is False
    await c.post("/v1/embeddings", json={"model": "stub-model", "input": "a" * 21})
    assert gw.estimator.calibrated is True


@pytest.mark.asyncio
async def test_embedding_backend_errors_pass_through(client):
    c, _, stub = client
    stub.embedding_status = 400
    r = await c.post("/v1/embeddings", json={"model": "stub-model", "input": "x"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_embeddings_reject_a_malformed_body(client):
    c, _, _ = client
    r = await c.post("/v1/embeddings", json={"model": "stub-model"})
    assert r.status_code == 400
    assert r.json()["error"]["type"] == "invalid_request_error"


# -- transcriptions ----------------------------------------------------


@pytest.mark.asyncio
async def test_transcription_is_proxied_with_its_content_type(client):
    c, _, stub = client
    r = await c.post("/v1/audio/transcriptions", files=AUDIO, data={"model": "stub-model"})
    assert r.status_code == 200
    assert r.json()["text"] == "hello world"

    files, data = stub.transcribe_calls[0]
    assert files["file"][0] == "clip.wav"
    assert files["file"][1] == b"RIFF....fake audio bytes"
    assert data["model"] == "stub-model"


@pytest.mark.asyncio
async def test_transcription_passes_through_non_json_formats(client):
    """response_format=srt returns text/plain; the gateway must not re-encode it."""
    c, _, stub = client
    stub.transcribe_body = b"1\n00:00:00,000 --> 00:00:01,000\nhello\n"
    stub.transcribe_content_type = "text/plain; charset=utf-8"

    r = await c.post(
        "/v1/audio/transcriptions",
        files=AUDIO,
        data={"model": "stub-model", "response_format": "srt"},
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert r.content == stub.transcribe_body
    assert stub.transcribe_calls[0][1]["response_format"] == "srt"


@pytest.mark.asyncio
async def test_transcription_uses_its_own_limiter_not_the_token_budget(client):
    """G-08: audio has no honest token cost, so it must not touch that budget."""
    c, gw, _ = client
    await c.post("/v1/audio/transcriptions", files=AUDIO, data={"model": "stub-model"})
    assert gw.capacity.in_flight == 0
    assert gw.capacity.reclaimed == 0  # never went near the KV budget
    assert gw.audio_capacity.in_flight == 0
    assert gw.audio_capacity.unit == "concurrency"


@pytest.mark.asyncio
async def test_audio_concurrency_is_capped():
    """The whole point of a separate limiter is that it actually limits."""
    app, gw, stub = _app(audio_max_concurrency=2)
    stub.transcribe_delay = 0.05
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await asyncio.gather(
            *[
                c.post("/v1/audio/transcriptions", files=AUDIO, data={"model": "stub-model"})
                for _ in range(6)
            ]
        )
    assert stub.peak_transcriptions <= 2
    assert len(stub.transcribe_calls) == 6


@pytest.mark.asyncio
async def test_oversized_uploads_are_rejected_with_413():
    app, _, stub = _app(audio_max_upload_mb=1)
    big = {"file": ("big.wav", b"\0" * (2 * 1024 * 1024), "audio/wav")}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/audio/transcriptions", files=big, data={"model": "stub-model"})
    assert r.status_code == 413
    assert "audio_max_upload_mb" in r.json()["error"]["message"]
    assert stub.transcribe_calls == []


@pytest.mark.asyncio
async def test_a_missing_file_part_is_a_400(client):
    c, _, _ = client
    r = await c.post("/v1/audio/transcriptions", data={"model": "stub-model"})
    assert r.status_code == 400
    assert "file" in r.json()["error"]["message"]


# -- wiring ------------------------------------------------------------


@pytest.mark.asyncio
async def test_routes_can_be_turned_off():
    app, _, _ = _app(embeddings=False, transcriptions=False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        embed = await c.post("/v1/embeddings", json={"model": "m", "input": "x"})
        audio = await c.post("/v1/audio/transcriptions", files=AUDIO, data={"model": "m"})
    assert embed.status_code == 404
    assert audio.status_code == 404


@pytest.mark.asyncio
async def test_status_reports_which_routes_are_proxied(client):
    c, _, _ = client
    body = (await c.get("/status")).json()
    assert body["routes"] == {
        "chat": True,
        "models": True,
        "embeddings": True,
        "transcriptions": True,
    }
    assert body["audio_admission"]["unit"] == "concurrency"


@pytest.mark.asyncio
async def test_metrics_are_labelled_per_route(client):
    c, _, _ = client
    await c.post("/v1/embeddings", json={"model": "stub-model", "input": "x"})
    await c.post("/v1/audio/transcriptions", files=AUDIO, data={"model": "stub-model"})
    await c.post(
        "/v1/chat/completions",
        json={"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]},
    )

    metrics = (await c.get("/metrics")).text
    assert 'kvstream_requests_total{outcome="served",route="embeddings"}' in metrics
    assert 'kvstream_requests_total{outcome="served",route="transcriptions"}' in metrics
    assert 'kvstream_requests_total{outcome="served",route="chat"}' in metrics
    assert "kvstream_audio_budget" in metrics


@pytest.mark.asyncio
async def test_the_gateway_identifies_itself_on_every_response(client):
    """G-29: this header is what stops one gateway proxying into another."""
    c, _, _ = client
    for path in ("/health", "/status", "/v1/models"):
        r = await c.get(path)
        assert r.headers["x-kvstream-version"]
