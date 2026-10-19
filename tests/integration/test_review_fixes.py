"""
Regression tests for the code-review findings fixed on 2026-10-11.

Each test names the finding it pins. Where a finding was reproduced before it
was fixed, the test exercises the same scenario as the reproduction.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import httpx
import pytest
import uvicorn
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.admission import AdmissionTooSlow, CapacityManager, QueueFull
from kvstream.admission import calibration as calibration_module
from kvstream.admission.calibration import (
    CalibrationKey,
    CalibrationService,
    lookup_budget,
    save_budget,
)
from kvstream.app import build_app
from kvstream.backend import discovery
from kvstream.backend.foundry import FoundryClient, FoundryClientError, FoundryError
from kvstream.backend.foundry_cli import FoundryCli
from kvstream.backend.inventory import ModelInventory
from kvstream.backend.resolver import Resolution
from kvstream.cache.response_cache import request_key
from kvstream.config import Settings
from kvstream.models import ChatCompletionRequest
from kvstream.server import GatewayServer

CHAT = {"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]}


def _app(backend, **settings_overrides):
    settings = Settings()
    settings.backend.model = "stub-model"
    for dotted, value in settings_overrides.items():
        section, _, field = dotted.partition("__")
        setattr(getattr(settings, section), field, value)
    app = build_app(settings)
    app.state.gateway.backend = backend
    return app, app.state.gateway


# ----------------------------------------------------------------------
# #1 Shutdown drains from the moment it starts
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_request_queued_at_shutdown_is_refused_not_admitted_unbudgeted():
    m = CapacityManager(budget=1, unit="concurrency", admission_timeout=5, max_queue_depth=10)
    await m.admit("A", 1)
    queued = asyncio.create_task(m.admit("B", 1))
    await asyncio.sleep(0.05)
    assert await m.start_draining() == 1
    with pytest.raises(QueueFull, match="shutting down"):
        await asyncio.wait_for(queued, 2)
    assert "B" not in m._reservations  # previously: admitted with no reservation


class _SlowOnce(StubBackend):
    async def chat_once(self, payload, headers=None, timeout=None):
        await asyncio.sleep(1.0)
        return await super().chat_once(payload, headers, timeout)

    async def detect_cli(self):
        return FoundryCli()


@pytest.mark.asyncio
async def test_under_real_uvicorn_a_queued_request_gets_503_and_in_flight_finishes():
    """Reproduces the measured failure: the queued request used to be served."""
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.max_concurrency = 1
    app = build_app(settings)
    gw = app.state.gateway
    gw.backend = _SlowOnce()
    server = GatewayServer(
        uvicorn.Config(
            app, host="127.0.0.1", port=0, log_level="warning", timeout_graceful_shutdown=5
        ),
        gw,
    )
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    port = server.servers[0].sockets[0].getsockname()[1]

    async def call() -> httpx.Response:
        async with httpx.AsyncClient(timeout=20) as c:
            return await c.post(f"http://127.0.0.1:{port}/v1/chat/completions", json=CHAT)

    first = asyncio.create_task(call())
    while gw.capacity.in_flight < 1:
        await asyncio.sleep(0.01)
    queued = asyncio.create_task(call())
    while gw.capacity.waiting < 1:
        await asyncio.sleep(0.01)

    t0 = time.perf_counter()
    server.should_exit = True
    refused = await queued
    refused_after = time.perf_counter() - t0
    served = await first
    await serving

    assert refused.status_code == 503
    assert "shutting down" in refused.json()["error"]["message"]
    assert refused_after < 0.5  # at once, not after its turn
    assert served.status_code == 200  # in-flight work finished
    assert gw.backend.once_calls == 1  # the queued request never reached it


# ----------------------------------------------------------------------
# #2 No probe before every request; a moved backend is still found
# ----------------------------------------------------------------------


def _counting_client(handler, **kwargs) -> tuple[FoundryClient, list[str]]:
    seen: list[str] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.method} {request.url.host}:{request.url.port}{request.url.path}")
        return handler(request)

    c = FoundryClient(base_url="http://127.0.0.1:5273", model="m", use_foundry_cli=False, **kwargs)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(wrapped))
    return c, seen


def _ok(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/v1/models":
        return httpx.Response(200, json={"data": [{"id": "m"}]})
    return httpx.Response(200, json={"choices": [{"message": {"content": "x"}}]})


@pytest.mark.asyncio
async def test_requests_no_longer_probe_the_backend_first():
    c, seen = _counting_client(_ok)
    for _ in range(3):
        await c.chat_once({"model": "m", "messages": []})
    # One lookup to resolve the backend, then only the requests themselves.
    assert seen.count("GET 127.0.0.1:5273/v1/models") == 1
    assert seen.count("POST 127.0.0.1:5273/v1/chat/completions") == 3
    await c.aclose()


@pytest.mark.asyncio
async def test_a_backend_that_moved_ports_is_rediscovered_and_the_request_retried(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 5273:
            raise httpx.ConnectError("connection refused", request=request)
        return _ok(request)

    c, seen = _counting_client(handler)
    c._resolved_url = "http://127.0.0.1:5273"  # where Foundry used to be

    async def resolve(client):
        return Resolution("http://127.0.0.1:61000", "scan", attempts=["scan"])

    monkeypatch.setattr(c._resolver, "resolve", resolve)
    body = await c.chat_once({"model": "m", "messages": []})

    assert body["choices"]
    assert seen == [
        "POST 127.0.0.1:5273/v1/chat/completions",  # refused: never reached a server
        "POST 127.0.0.1:61000/v1/chat/completions",  # retried once at the new port
    ]
    assert c.base_url == "http://127.0.0.1:61000"
    await c.aclose()


@pytest.mark.asyncio
async def test_a_pinned_backend_is_not_retried_elsewhere():
    def refused(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    c, seen = _counting_client(refused, pinned=True)
    with pytest.raises(FoundryError, match="unreachable"):
        await c.chat_once({"model": "m", "messages": []})
    assert len(seen) == 1
    await c.aclose()


# ----------------------------------------------------------------------
# #4 Calibration records are found under either model name, newest first
# ----------------------------------------------------------------------


def test_a_budget_is_found_with_or_without_the_version_suffix(tmp_path):
    store = str(tmp_path / "c.json")
    save_budget(store, CalibrationKey("phi-gpu", "cpu", "0.10.x", "0.10.3"), 900)
    hit = lookup_budget(store, CalibrationKey("phi-gpu:2", "cpu", "0.10.x", "0.10.3"))
    assert hit.match == "exact"
    assert hit.budget_tokens == 900


def test_the_newest_partial_match_wins(tmp_path, monkeypatch):
    store = str(tmp_path / "c.json")
    clock = iter([1000.0, 2000.0])
    monkeypatch.setattr(calibration_module.time, "time", lambda: next(clock))
    # The store is written with sorted keys, so the *older* record must sort
    # first here — otherwise "take the first match" passes by accident.
    save_budget(store, CalibrationKey("phi", "cpu", "0.10.x", "0.10.1"), 111)  # older
    save_budget(store, CalibrationKey("phi", "cpu", "service", "0.8.119"), 222)  # newer
    hit = lookup_budget(store, CalibrationKey("phi", "cpu", "0.10.x", "0.10.3"))
    assert hit.match == "partial"
    assert hit.budget_tokens == 222


# ----------------------------------------------------------------------
# #5 Token-mode shedding uses one unit
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_token_mode_counts_as_saturated_when_the_next_request_does_not_fit():
    """
    The shape measured live: a 96-token budget, ~70-token requests. One runs,
    nothing else fits, yet 26 tokens stay "free". Both earlier guards missed
    this — queue length vs a token budget, then `in_flight >= budget`.
    """
    m = CapacityManager(budget=96, unit="tokens", admission_timeout=5, max_queue_depth=100)
    await m.admit("running", 70)
    tasks = [asyncio.create_task(m.admit(f"q{i}", 70)) for i in range(3)]
    await asyncio.sleep(0.05)
    assert m.in_flight == 70 and m.waiting == 3
    assert m._may_reject(70) is True
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_token_mode_flood_is_refused_on_arrival_not_after_the_timeout():
    m = CapacityManager(budget=96, unit="tokens", admission_timeout=1.0, max_queue_depth=100)
    for _ in range(5):  # a measured drain rate: ~70 tokens/s
        m._observe_release(70)
    await m.admit("running", 70)
    queued = [asyncio.create_task(m.admit(f"q{i}", 70)) for i in range(3)]
    await asyncio.sleep(0.05)
    t0 = time.perf_counter()
    with pytest.raises(AdmissionTooSlow):
        await m.admit("late", 70)
    assert time.perf_counter() - t0 < 0.2  # refused at once, with a predicted wait
    for t in queued:
        t.cancel()
    await asyncio.gather(*queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_drain_rate_counts_admitted_cost_not_the_shrunken_reservation():
    m = CapacityManager(budget=1000, unit="tokens", admission_timeout=5, max_queue_depth=10)
    await m.admit("r", 600)
    await m.adjust("r", 50)  # live accounting: it only used 50
    await m.release("r")
    assert m._releases[-1][1] == 600  # the budget it held over its life


# ----------------------------------------------------------------------
# #6 Results are never shared across callers
# ----------------------------------------------------------------------


def test_the_key_separates_callers_but_not_repeat_requests():
    raw = {**CHAT, "temperature": 0}
    req = ChatCompletionRequest(**raw)
    alice = request_key(req, raw, "Bearer alice")
    assert alice == request_key(req, raw, "Bearer alice")
    assert alice != request_key(req, raw, "Bearer bob")
    assert alice != request_key(req, raw, None)


class _SlowCounted(StubBackend):
    async def chat_once(self, payload, headers=None, timeout=None):
        await asyncio.sleep(0.2)
        return await super().chat_once(payload, headers, timeout)


@pytest.mark.asyncio
async def test_identical_requests_from_different_callers_are_not_coalesced():
    backend = _SlowCounted()
    app, _ = _app(backend)
    raw = {**CHAT, "temperature": 0}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:

        def call(token: str):
            return c.post(
                "/v1/chat/completions", json=raw, headers={"Authorization": f"Bearer {token}"}
            )

        await asyncio.gather(call("alice"), call("bob"))
        assert backend.once_calls == 2  # one upstream call per caller
        await asyncio.gather(call("carol"), call("carol"))
        assert backend.once_calls == 3  # the same caller still coalesces


# ----------------------------------------------------------------------
# #7 Legacy completions stream; a non-JSON success is not a 200
# ----------------------------------------------------------------------


class _LegacyStreams(StubBackend):
    def __init__(self, fail: Exception | None = None) -> None:
        super().__init__()
        self.fail = fail

    async def completions_stream(self, payload, headers=None):
        if self.fail is not None:
            raise self.fail
        yield b'data: {"choices":[{"text":"Hi","index":0}]}\n\n'
        yield b"data: [DONE]\n\n"


@pytest.mark.asyncio
async def test_streamed_legacy_completion_is_relayed_as_a_stream():
    app, gw = _app(_LegacyStreams())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post(
            "/v1/completions", json={"model": "stub-model", "prompt": "hi", "stream": True}
        )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.text.endswith("data: [DONE]\n\n")
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_streamed_legacy_completion_errors_keep_their_status():
    app, gw = _app(_LegacyStreams(FoundryClientError(404, {"error": {"message": "no"}}, "404")))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post(
            "/v1/completions", json={"model": "stub-model", "prompt": "hi", "stream": True}
        )
    assert r.status_code == 404
    assert gw.health.breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_a_non_json_success_body_is_reported_as_502():
    c = FoundryClient(base_url="http://b:1", model="m", discover=False)
    c._client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text="<html>proxy page</html>"))
    )
    status, body = await c.completions({"model": "m", "prompt": "hi"})
    assert status == 502  # previously 200 with an error object
    assert body["error"]["type"] == "upstream_error"
    await c.aclose()


# ----------------------------------------------------------------------
# #8 / #12 Calibration measures actual tokens; the drift baseline matches
# ----------------------------------------------------------------------


class _EarlyStopper:
    """Answers 5 tokens however many are allowed, and reports real usage."""

    model = "stub-model"
    base_url = "http://stub"

    async def chat(self, payload, headers=None):
        await asyncio.sleep(0.01)
        for _ in range(5):
            yield type("T", (), {"text": "x", "usage": None, "finish_reason": None})()
        yield type(
            "T",
            (),
            {
                "text": "",
                "finish_reason": "stop",
                "usage": {"prompt_tokens": 20, "completion_tokens": 5},
            },
        )()


@pytest.mark.asyncio
async def test_the_sweep_counts_generated_tokens_not_max_tokens(tmp_path):
    svc = CalibrationService(
        _EarlyStopper(), str(tmp_path / "c.json"), CalibrationKey("stub-model", "cpu")
    )
    point = await svc._measure(2, [(20, 64)], trials=1)
    # 2 requests x 5 generated tokens — not 2 x 64 requested.
    assert point.tokens_per_second == pytest.approx(10 / (point.p99_seconds or 1), rel=0.6)
    assert point.p50_per_token == pytest.approx(point.p99_seconds / 25, rel=0.6)


@pytest.mark.asyncio
async def test_calibration_records_a_drift_baseline_on_the_live_basis(tmp_path):
    store = str(tmp_path / "c.json")
    key = CalibrationKey("stub-model", "cpu")
    svc = CalibrationService(_EarlyStopper(), store, key)
    await svc.calibrate(max_concurrency=2, trials=1, warmup=0, refine=False)
    record = lookup_budget(store, key).record
    assert record is not None
    assert record["baseline_seconds_per_token"] > 0
    assert record["baseline_seconds_per_token"] == round(svc.baseline_per_token, 6)


class _SlowFirstToken(StubBackend):
    async def chat(self, payload, headers=None):
        await asyncio.sleep(0.3)  # prefill: before the first chunk
        async for tok in super().chat(payload, headers):
            yield tok


@pytest.mark.asyncio
async def test_a_streamed_drift_sample_includes_time_to_first_token():
    app, gw = _app(_SlowFirstToken())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/v1/chat/completions", json={**CHAT, "stream": True})
        assert r.status_code == 200
    # One sample: its per-token time times its tokens is the request's latency.
    assert gw.drift.stats()["samples"] == 1
    tokens = gw.estimator.estimate_messages(ChatCompletionRequest(**CHAT).messages)
    assert gw.drift._ewma * (tokens + 10) >= 0.25  # previously excluded the 0.3s


# ----------------------------------------------------------------------
# #9 The estimator learns its text ratio from text tokens
# ----------------------------------------------------------------------


class _Reports(StubBackend):
    async def chat_once(self, payload, headers=None, timeout=None):
        body = await super().chat_once(payload, headers, timeout)
        return {**body, "usage": {"prompt_tokens": 104, "completion_tokens": 3}}


@pytest.mark.asyncio
async def test_template_overhead_is_not_learned_as_text():
    app, gw = _app(_Reports())
    seen: list[tuple[int, int, int]] = []
    learner = gw.estimators.learner("stub-model")
    learner.observe = lambda chars, units, tokens: seen.append((chars, units, tokens))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post(
            "/v1/chat/completions",
            json={"model": "stub-model", "messages": [{"role": "user", "content": "a" * 400}]},
        )
    # 104 reported, minus 4 template tokens for the one message.
    assert seen == [(400, 1, 100)]


# ----------------------------------------------------------------------
# #10 Streams are timed; reachability does not need /health
# ----------------------------------------------------------------------


def _histogram_count(text: str, route: str) -> float:
    for line in text.splitlines():
        if line.startswith(f'kvstream_request_seconds_count{{route="{route}"}}'):
            return float(line.split()[-1])
    return 0.0


@pytest.mark.asyncio
async def test_streams_are_timed_and_backend_up_reflects_real_traffic():
    app, gw = _app(StubBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/v1/chat/completions", json={**CHAT, "stream": True})
        assert r.status_code == 200
        metrics = (await c.get("/metrics")).text
    assert _histogram_count(metrics, "chat") == 1
    assert "kvstream_backend_up 1.0" in metrics  # no /health call was made


# ----------------------------------------------------------------------
# #11 The calibration store survives crashes and bad values
# ----------------------------------------------------------------------


def test_a_failed_write_leaves_the_existing_store_intact(tmp_path, monkeypatch):
    store = tmp_path / "c.json"
    save_budget(str(store), CalibrationKey("a", "cpu"), 500)
    before = store.read_text(encoding="utf-8")

    def crash(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(calibration_module.json, "dump", crash)
    with pytest.raises(OSError):
        save_budget(str(store), CalibrationKey("b", "cpu"), 700)
    assert store.read_text(encoding="utf-8") == before
    assert [f for f in os.listdir(tmp_path) if f.endswith(".tmp")] == []


def test_a_non_numeric_budget_is_skipped_not_fatal(tmp_path):
    store = tmp_path / "c.json"
    store.write_text(
        json.dumps(
            {
                "version": 2,
                "entries": {
                    "x": {"model": "a", "device": "cpu", "budget_tokens": "lots"},
                },
            }
        ),
        encoding="utf-8",
    )
    assert lookup_budget(str(store), CalibrationKey("a", "cpu")).match == "none"


# ----------------------------------------------------------------------
# #13 Discovery recognises the configured model with or without its suffix
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discovery_matches_the_model_ignoring_the_version_suffix():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/models":
            return httpx.Response(200, json={"data": [{"id": "Phi-3-mini-gpu"}]})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        candidate = await discovery._probe(client, 50191, prefer_model="Phi-3-mini-gpu:2")
    assert candidate is not None
    assert candidate.has_configured_model is True


# ----------------------------------------------------------------------
# Found live: false budget warnings at startup in token mode
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_token_mode_startup_reports_the_real_budget_once_and_no_false_warning(
    tmp_path, caplog
):
    store = str(tmp_path / "c.json")
    save_budget(store, CalibrationKey("stub-model:2", "cpu", "0.10.x", "0.10.3"), 96)
    settings = Settings()
    settings.backend.device = "cpu"
    settings.admission.mode = "tokens"
    settings.admission.calibration_store = store

    class Detected(StubBackend):
        def __init__(self) -> None:
            super().__init__()
            self.model = None

        async def detect_cli(self):
            return FoundryCli(path="foundry", version="0.10.3", generation="0.10.x")

        async def models_or_none(self):
            return ["stub-model"]

        def use_model(self, model: str) -> None:
            self.model = model

    caplog.set_level("WARNING")
    app = build_app(settings)  # model unknown here: must stay quiet
    gw = app.state.gateway
    backend = Detected()
    gw.backend = backend

    async def loaded():
        return ["stub-model:2"]

    gw.inventory = ModelInventory(backend.models_or_none, loaded)
    await gw.startup()

    assert gw.budget_provenance["source"] == "calibration:exact"
    assert gw.capacity.budget == 96
    assert not [r for r in caplog.records if "budget" in r.getMessage()]


@pytest.mark.asyncio
async def test_a_missing_budget_is_still_reported_when_the_key_does_not_change(caplog):
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.mode = "tokens"
    settings.admission.calibration_store = "does-not-exist.json"
    caplog.set_level("WARNING")
    app = build_app(settings)
    gw = app.state.gateway

    class NoCli(StubBackend):
        async def detect_cli(self):
            return FoundryCli(generation="unknown")  # key identical to provisional

    gw.backend = NoCli()
    await gw.startup()
    assert any("Falling back to concurrency mode" in r.getMessage() for r in caplog.records)


# ----------------------------------------------------------------------
# Found live: CLI output decoding, and "unreachable" for a running server
# ----------------------------------------------------------------------


def test_cli_output_is_decoded_as_utf8_whatever_the_console_code_page():
    """
    Measured: `foundry model load` prints "● success"; decoded as cp1252 the
    reader thread raised and KVStream received the text "None".
    """
    import sys

    from kvstream.backend.foundry_cli import _run

    code = "import sys; sys.stdout.buffer.write('\u25cf success {\"ok\": true}'.encode('utf-8'))"
    result = _run([sys.executable, "-c", code], timeout=30)
    assert result is not None
    _, output = result
    assert "\u25cf success" in output
    assert '{"ok": true}' in output
    assert "None" not in output


def _failing_client(exc_type) -> FoundryClient:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc_type("boom", request=request)

    c = FoundryClient(base_url="http://127.0.0.1:5273", model="m", discover=False)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return c


@pytest.mark.asyncio
async def test_only_a_refused_connection_is_called_unreachable():
    refused = _failing_client(httpx.ConnectError)
    with pytest.raises(FoundryError) as info:
        await refused.chat_once({"model": "m", "messages": []})
    assert "unreachable" in str(info.value)
    assert "start it with" in str(info.value) or "foundry" in str(info.value)

    dropped = _failing_client(httpx.RemoteProtocolError)
    with pytest.raises(FoundryError) as info:
        await dropped.chat_once({"model": "m", "messages": []})
    message = str(info.value)
    assert "unreachable" not in message
    assert "server start" not in message
    assert "accepted the request" in message and "model is loaded" in message

    slow = _failing_client(httpx.ReadTimeout)
    with pytest.raises(FoundryError, match="did not respond in time"):
        await slow.chat_once({"model": "m", "messages": []})


# ----------------------------------------------------------------------
# netstat: localized state words, OEM bytes, IPv6-only listeners
# ----------------------------------------------------------------------

# Lines captured from `netstat -ano` on the development machine (English
# Windows 10, OEM code page 437), trimmed. 63962 is Foundry Local; 42050 is
# bound to [::1] only; 50065/50066 are a connected loopback pair.
_NETSTAT_EN = """
Active Connections

  Proto  Local Address          Foreign Address        State           PID
  TCP    0.0.0.0:135            0.0.0.0:0              LISTENING       1308
  TCP    0.0.0.0:5040           0.0.0.0:0              LISTENING       9792
  TCP    127.0.0.1:63962        0.0.0.0:0              LISTENING       7252
  TCP    127.0.0.1:50065        127.0.0.1:50066        ESTABLISHED     1152
  TCP    127.0.0.1:50066        127.0.0.1:50065        ESTABLISHED     1152
  TCP    192.168.1.20:52000     20.42.65.84:443        TIME_WAIT       0
  TCP    [::]:5040              [::]:0                 LISTENING       9792
  TCP    [::1]:42050            [::]:0                 LISTENING       10040
  UDP    0.0.0.0:5353           *:*                                    2264
"""
_EXPECTED = {5040, 63962, 42050}

# State words as a localized Windows might print them. Which exact words each
# locale uses is not asserted here — the point is that the parser must not care.
_LOCALIZED = {
    "German": {"LISTENING": "ABH\u00d6REN", "ESTABLISHED": "HERGESTELLT", "TIME_WAIT": "WARTEND"},
    "non-Latin": {
        "LISTENING": "\u5f85\u3061\u53d7\u3051",
        "ESTABLISHED": "\u63a5\u7d9a",
        "TIME_WAIT": "\u5f85\u6a5f",
    },
    "cp437 0x8F": {
        "LISTENING": "\u00c5LYSNING",
        "ESTABLISHED": "ETABLERAD",
        "TIME_WAIT": "V\u00c4NTAR",
    },
}


def test_netstat_parsing_finds_the_same_listeners_in_every_language():
    from kvstream.backend.discovery import parse_netstat

    assert parse_netstat(_NETSTAT_EN) == _EXPECTED
    for language, words in _LOCALIZED.items():
        text = _NETSTAT_EN
        for english, local in words.items():
            text = text.replace(english, local)
        assert parse_netstat(text) == _EXPECTED, language


def test_list_listening_ports_survives_oem_encoded_localized_output(monkeypatch):
    """
    The real function and its real decoding settings, with only the command
    swapped for one that writes netstat output as OEM code page 437 bytes,
    including 0x8F ("Å"), which cp1252 — Python's default here — cannot decode.
    """
    import subprocess
    import sys

    from kvstream.backend import discovery as disc

    localized = _NETSTAT_EN.replace("LISTENING", "\u00c5LYSNING")
    payload = localized.encode("cp437")
    assert b"\x8f" in payload
    real_run = subprocess.run

    def fake_run(argv, **kwargs):
        assert argv[0] == "netstat"
        script = f"import sys; sys.stdout.buffer.write({payload!r})"
        return real_run([sys.executable, "-c", script], **kwargs)

    monkeypatch.setattr(disc.sys, "platform", "win32")
    monkeypatch.setattr(disc.subprocess, "run", fake_run)
    assert set(disc.list_listening_ports()) == _EXPECTED


def test_ss_parsing_is_unchanged_by_the_refactor():
    from kvstream.backend.discovery import parse_ss

    sample = (
        "LISTEN 0      4096       127.0.0.1:5273       0.0.0.0:*\n"
        "LISTEN 0      511          [::1]:8080           [::]:*\n"
        "LISTEN 0      128          0.0.0.0:22         0.0.0.0:*\n"
    )
    assert parse_ss(sample) == {5273, 8080}  # 22 is below the 1024 floor


# ----------------------------------------------------------------------
# macOS: the scan ran `ss`, which macOS does not have
# ----------------------------------------------------------------------

# The first three data rows are the documented `netstat -an` sample from
# Parse::Netstat::darwin (dyn.manpages.debian.org, libparse-netstat-perl), kept
# verbatim. The loopback rows below them follow the same documented format
# (`address.port`, wildcard foreign `*.*`); they are constructed, not captured.
_MACOS_NETSTAT = """Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp4       0      0  10.8.0.6.54433         1.2.3.4.443            ESTABLISHED
tcp46      0      0  *.80                   *.*                    LISTEN
tcp6       0      0  *.88                   *.*                    LISTEN
tcp4       0      0  127.0.0.1.63962        *.*                    LISTEN
tcp6       0      0  ::1.42050              *.*                    LISTEN
tcp46      0      0  *.5000                 *.*                    LISTEN
tcp4       0      0  192.168.1.5.7000       *.*                    LISTEN
tcp4       0      0  127.0.0.1.52344        127.0.0.1.63962        ESTABLISHED
udp4       0      0  *.5353                 *.*
"""


def test_macos_netstat_is_parsed_by_structure():
    from kvstream.backend.discovery import parse_bsd_netstat

    # 80/88 are below the 1024 floor; 7000 is bound to a LAN address only
    # (not reachable as localhost); the UDP and ESTABLISHED rows are not listeners.
    assert parse_bsd_netstat(_MACOS_NETSTAT) == {63962, 42050, 5000}


def test_on_macos_the_scan_runs_netstat_not_ss(monkeypatch):
    import subprocess
    import sys

    from kvstream.backend import discovery as disc

    real_run = subprocess.run
    seen: list[list[str]] = []

    def fake_run(argv, **kwargs):
        seen.append(argv)
        script = f"import sys; sys.stdout.write({_MACOS_NETSTAT!r})"
        return real_run([sys.executable, "-c", script], **kwargs)

    monkeypatch.setattr(disc.sys, "platform", "darwin")
    monkeypatch.setattr(disc.subprocess, "run", fake_run)
    assert set(disc.list_listening_ports()) == {63962, 42050, 5000}
    assert seen == [["netstat", "-an", "-p", "tcp"]]


def test_the_scan_finds_a_real_listener_on_this_os():
    """No fixtures: a real socket, the real platform tool, the real parser."""
    import socket

    from kvstream.backend.discovery import list_listening_ports

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        port = server.getsockname()[1]
        assert port in list_listening_ports()
