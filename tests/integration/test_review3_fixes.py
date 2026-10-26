"""
Regression tests for the third code review.

Each test reproduces the reported failure: it fails on the code as it was and
passes on the fix.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend
from typer.testing import CliRunner

from kvstream.app import build_app
from kvstream.backend.foundry import FoundryClient, FoundryError, Token
from kvstream.backend.health import BackendHealth
from kvstream.backend.resolver import SOURCE_CLI, SOURCE_SCAN, Resolution
from kvstream.cache.response_cache import request_key
from kvstream.cli import app as cli_app
from kvstream.config import Settings
from kvstream.models import ChatCompletionRequest

# ----------------------------------------------------------------------
# 1. Environment variables outrank the YAML file, as documented
# ----------------------------------------------------------------------


def test_env_vars_override_the_yaml_file(tmp_path, monkeypatch):
    cfg = tmp_path / "kvstream.yaml"
    cfg.write_text("port: 8080\nbackend:\n  timeout_seconds: 60\n  model: from-yaml\n")
    monkeypatch.setenv("KVSTREAM_PORT", "9000")
    monkeypatch.setenv("KVSTREAM_BACKEND__TIMEOUT_SECONDS", "5")

    s = Settings.load(str(cfg))

    assert s.port == 9000  # env beats YAML
    assert s.backend.timeout_seconds == 5.0  # nested env beats nested YAML
    assert s.backend.model == "from-yaml"  # YAML still beats the default


def test_yaml_still_overrides_defaults_and_marks_a_url_explicit(tmp_path):
    cfg = tmp_path / "k.yaml"
    cfg.write_text("backend:\n  base_url: http://127.0.0.1:5999\n")
    s = Settings.load(str(cfg))
    assert s.backend.base_url == "http://127.0.0.1:5999"
    assert s.backend_url_is_explicit()  # still pins discovery off


def test_unknown_yaml_keys_are_still_rejected(tmp_path):
    cfg = tmp_path / "k.yaml"
    cfg.write_text("bogus: 1\n")
    with pytest.raises(ValueError):
        Settings.load(str(cfg))


# ----------------------------------------------------------------------
# 2. A --config file that does not exist is an error, not silent defaults
# ----------------------------------------------------------------------


def test_an_explicit_config_path_must_exist(tmp_path):
    with pytest.raises(FileNotFoundError):
        Settings.load(str(tmp_path / "typo.yaml"))


def test_a_config_file_must_be_a_mapping(tmp_path):
    cfg = tmp_path / "list.yaml"
    cfg.write_text("- a\n- b\n")
    with pytest.raises(ValueError, match="mapping"):
        Settings.load(str(cfg))


def test_the_cli_reports_a_missing_config_file(tmp_path):
    result = CliRunner().invoke(cli_app, ["serve", "--config", str(tmp_path / "typo.yaml")])
    assert result.exit_code == 2
    assert "config file not found" in result.output


# ----------------------------------------------------------------------
# 3. /v1/models/{id} cannot steer the upstream URL
# ----------------------------------------------------------------------


def _client(handler, **kwargs) -> FoundryClient:
    client = FoundryClient(base_url="http://foundry.test", **kwargs)
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.asyncio
async def test_a_model_id_is_sent_as_one_encoded_path_segment():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"id": "x"})

    client = _client(handler, pinned=True)
    await client.get_model("x?y=1")
    await client.get_model("phi-3-mini-4k-instruct-generic-cpu:1")

    assert seen[0].url.raw_path == b"/v1/models/x%3Fy%3D1"
    assert seen[0].url.query == b""  # no query smuggled upstream
    assert seen[1].url.raw_path == b"/v1/models/phi-3-mini-4k-instruct-generic-cpu:1"


@pytest.mark.asyncio
async def test_a_dot_segment_model_id_never_reaches_the_backend():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    client = _client(handler, pinned=True)
    for model_id in ("..", "."):
        status, body = await client.get_model(model_id)
        assert status == 404
        assert body["error"]["code"] == "model_not_found"
    assert seen == []


# ----------------------------------------------------------------------
# 4. A probe that runs out of time is reported as a timeout
# ----------------------------------------------------------------------


class _RaisingClient:
    model = "m"

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc

    async def health(self) -> bool:
        return True

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        raise self.exc


def _read_timeout() -> FoundryError:
    try:
        raise httpx.ReadTimeout("timed out")
    except httpx.ReadTimeout as cause:
        try:
            raise FoundryError("Foundry Local at x did not respond in time") from cause
        except FoundryError as exc:
            return exc


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [asyncio.TimeoutError(), TimeoutError(), _read_timeout()],
    ids=["asyncio.TimeoutError", "TimeoutError", "httpx-read-timeout"],
)
async def test_a_probe_timeout_is_reported_as_a_timeout(exc):
    health = BackendHealth(_RaisingClient(exc), readiness_timeout=3.0)
    readiness = await health.check_ready("m")
    assert readiness.ready is False
    assert "did not complete a 1-token generation within 3s" in readiness.detail


@pytest.mark.asyncio
async def test_a_refused_probe_is_still_reported_as_a_rejection():
    health = BackendHealth(_RaisingClient(FoundryError("HTTP 500")))
    readiness = await health.check_ready("m")
    assert readiness.ready is False
    assert readiness.detail.startswith("backend rejected a 1-token generation")


# ----------------------------------------------------------------------
# 5. Audio uploads are bounded while they stream in, and read after admission
# ----------------------------------------------------------------------


def _audio_app(limit_mb: int = 1):
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.routes.audio_max_upload_mb = limit_mb
    app = build_app(settings)
    stub = StubBackend()
    app.state.gateway.backend = stub
    return app, app.state.gateway, stub


def _multipart(file_bytes: int) -> tuple[bytes, str]:
    boundary = "kvstreamtestboundary"
    head = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="model"\r\n\r\nstub-model\r\n'
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="a.wav"\r\n'
        "Content-Type: audio/wav\r\n\r\n"
    ).encode()
    tail = f"\r\n--{boundary}--\r\n".encode()
    return head + b"\0" * file_bytes + tail, f"multipart/form-data; boundary={boundary}"


@pytest.mark.asyncio
async def test_an_upload_without_content_length_stops_being_read_at_the_limit():
    """Chunked: no Content-Length, so only counting while receiving can stop it."""
    app, _, stub = _audio_app(limit_mb=1)
    body, content_type = _multipart(8 * 1024 * 1024)
    chunk = 64 * 1024
    chunks = [body[i : i + chunk] for i in range(0, len(body), chunk)]
    consumed = 0
    sent: list[dict] = []

    async def receive() -> dict:
        nonlocal consumed
        if chunks:
            piece = chunks.pop(0)
            consumed += len(piece)
            return {"type": "http.request", "body": piece, "more_body": bool(chunks)}
        await asyncio.sleep(3600)  # never disconnects on its own
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/audio/transcriptions",
        "raw_path": b"/v1/audio/transcriptions",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", content_type.encode()), (b"host", b"test")],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }
    await asyncio.wait_for(app(scope, receive, send), timeout=10)

    start = next(m for m in sent if m["type"] == "http.response.start")
    assert start["status"] == 413
    assert consumed <= 2 * 1024 * 1024 + chunk  # stopped near limit + allowance
    assert stub.transcribe_calls == []


@pytest.mark.asyncio
async def test_the_file_is_read_only_after_admission_and_the_form_is_closed(monkeypatch):
    from starlette.datastructures import UploadFile

    app, gw, _ = _audio_app()
    seen: list[tuple[int, UploadFile]] = []
    original = UploadFile.read

    async def recording_read(self, size: int = -1) -> bytes:
        seen.append((gw.audio_capacity.active, self))
        return await original(self, size)

    monkeypatch.setattr(UploadFile, "read", recording_read)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post(
            "/v1/audio/transcriptions",
            files={"file": ("a.wav", b"RIFF audio", "audio/wav")},
            data={"model": "stub-model"},
        )
    assert r.status_code == 200
    assert seen and seen[0][0] == 1  # read while holding an audio slot
    assert seen[0][1].file.closed  # temporary file released
    assert gw.audio_capacity.active == 0


@pytest.mark.asyncio
async def test_an_oversized_file_is_refused_before_admission():
    app, gw, stub = _audio_app(limit_mb=1)
    admitted: list[str] = []
    original = gw.audio_capacity.admit

    async def recording_admit(req_id: str, cost: int) -> None:
        admitted.append(req_id)
        await original(req_id, cost)

    gw.audio_capacity.admit = recording_admit  # type: ignore[method-assign]
    big = {"file": ("big.wav", b"\0" * (1024 * 1024 + 10), "audio/wav")}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/audio/transcriptions", files=big, data={"model": "stub-model"})
    assert r.status_code == 413
    assert admitted == []
    assert stub.transcribe_calls == []


# ----------------------------------------------------------------------
# 6. Credentials go only to a backend that was chosen, never to a scanned one
# ----------------------------------------------------------------------


async def _auth_sent(source: str | None, *, pinned: bool = False) -> list[str | None]:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, json={"choices": [{"message": {"content": "x"}}]})

    client = _client(handler, api_key="sidecar-key", pinned=pinned)
    url = "http://127.0.0.1:61234"
    if not pinned:
        client._resolved_url = url
        client.last_resolution = Resolution(url, source or SOURCE_SCAN)
    await client.chat_once({"model": "m", "messages": []})
    await client.chat_once({"model": "m", "messages": []}, {"Authorization": "Bearer caller"})
    return seen


@pytest.mark.asyncio
async def test_a_scanned_backend_is_sent_no_credentials():
    assert await _auth_sent(SOURCE_SCAN) == [None, None]


@pytest.mark.asyncio
async def test_a_cli_reported_backend_gets_credentials():
    assert await _auth_sent(SOURCE_CLI) == ["Bearer sidecar-key", "Bearer caller"]


@pytest.mark.asyncio
async def test_a_configured_backend_gets_credentials():
    assert await _auth_sent(None, pinned=True) == ["Bearer sidecar-key", "Bearer caller"]


# ----------------------------------------------------------------------
# 8. A drain that times out stops the gateway-owned streams
# ----------------------------------------------------------------------


class _EndlessStream(StubBackend):
    def __init__(self) -> None:
        super().__init__()
        self.closed = 0

    async def chat(self, payload, headers=None):
        self.stream_calls += 1
        try:
            while True:
                await asyncio.sleep(0.01)
                yield Token(text="t ")
        finally:
            self.closed += 1


@pytest.mark.asyncio
async def test_a_timed_out_drain_cancels_streams_before_the_client_closes():
    settings = Settings()
    settings.backend.model = "stub-model"
    app = build_app(settings)
    gw = app.state.gateway
    backend = _EndlessStream()
    gw.backend = backend

    raw = {"model": "stub-model", "messages": [{"role": "user", "content": "go"}], "stream": True}
    req = ChatCompletionRequest(**raw)
    rc, _ = gw._cost(req)
    response = await gw.handle_streaming("s1", req, raw, rc, request_key(req, raw))
    await response.body_iterator.__anext__()  # the client is still reading

    await gw.drain(0.1)

    assert all(run.done() for run in gw._runs) and not gw._runs
    assert backend.closed == 1  # upstream stream closed
    assert gw.capacity.in_flight == 0  # reservation released
    assert gw.health.breaker.consecutive_failures == 0  # not blamed on the backend
    rest = b"".join([chunk async for chunk in response.body_iterator])
    assert b"[DONE]" not in rest  # an unfinished stream never claims completion
