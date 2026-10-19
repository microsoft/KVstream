"""
Regression tests for the fourth code review (calibration, drift, tokenization,
metrics, packaging).

Each test reproduces the reported failure: it fails on the code as it was and
passes on the fix.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import httpx
import pytest
import yaml
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend
from typer.testing import CliRunner

from kvstream.admission.calibration import (
    CalibrationKey,
    CalibrationService,
    SweepPoint,
    lookup_budget,
    save_budget,
)
from kvstream.admission.drift import DriftMonitor
from kvstream.admission.regime import CAPPED, classify_runtime
from kvstream.app import build_app
from kvstream.backend.foundry import FoundryError, Token
from kvstream.backend.foundry_cli import GEN_SDK, FoundryCli
from kvstream.backend.inventory import ModelInventory
from kvstream.cli import app as cli_app
from kvstream.config import Settings
from kvstream.models import ChatCompletionRequest
from kvstream.tokenization import KIND_EMBEDDINGS

REPO = Path(__file__).resolve().parents[2]


def _gateway(backend=None, **admission):
    settings = Settings()
    settings.backend.model = "stub-model"
    for key, value in admission.items():
        setattr(settings.admission, key, value)
    app = build_app(settings)
    gw = app.state.gateway
    gw.backend = backend or StubBackend()
    return app, gw


# ----------------------------------------------------------------------
# 1. Bisection judges health against the sweep's first point
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bisection_does_not_accept_a_point_past_the_latency_limit(tmp_path):
    class Client:
        model = "m"
        base_url = "http://stub"

    # Signal per concurrency: 1.9x is healthy, 3.0x and 3.5x are not (limit 2x).
    signal = {1: 1.0, 2: 1.9, 4: 3.0, 3: 3.5}
    svc = CalibrationService(Client(), str(tmp_path / "c.json"), CalibrationKey("m", "cpu"))

    async def measure(concurrency, shapes, trials):
        s = signal[concurrency]
        return SweepPoint(concurrency, concurrency * 100, s, 0, p99_per_token=s)

    async def probe(prompt_tokens, max_tokens):
        return 0.1, prompt_tokens, max_tokens

    svc._measure = measure  # type: ignore[method-assign]
    svc._probe_once = probe  # type: ignore[method-assign]
    budget = await svc.calibrate(max_concurrency=8, warmup=0, safety_margin=1.0)

    # c=3 at 3.5x must be rejected: the budget is c=2's in-flight load.
    assert budget == 200


# ----------------------------------------------------------------------
# 2. An auto-detected model is replaced when Foundry Local swaps models
# ----------------------------------------------------------------------


def _inventory(gw, loaded: dict) -> None:
    async def downloaded():
        return ["model-A", "model-B"]

    async def read_loaded():
        return loaded["value"]

    gw.inventory = ModelInventory(downloaded, read_loaded, min_refresh_seconds=0.0)


@pytest.mark.asyncio
async def test_an_auto_detected_model_follows_a_model_swap():
    gw = build_app(Settings()).state.gateway
    gw._cli = FoundryCli(generation=GEN_SDK, version="0.10.3")  # as startup detects
    loaded = {"value": ["model-A"]}
    _inventory(gw, loaded)

    assert await gw.ensure_model() == "model-A"
    loaded["value"] = ["model-B"]
    assert await gw.ensure_model() == "model-B"
    assert gw.calibration_key.model == "model-B"  # re-keyed for the new model
    loaded["value"] = []
    assert await gw.ensure_model() is None  # nothing loaded: says so
    assert "no model is loaded" in gw.model_source


@pytest.mark.asyncio
async def test_a_configured_model_is_never_replaced():
    settings = Settings()
    settings.backend.model = "model-A"
    gw = build_app(settings).state.gateway
    _inventory(gw, {"value": ["model-B"]})
    assert await gw.ensure_model() == "model-A"


# ----------------------------------------------------------------------
# 3, 4, 5. Container files that cannot work as shipped
# ----------------------------------------------------------------------


def test_the_compose_file_ships_no_unusable_url_or_model_alias():
    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text(encoding="utf-8"))
    env = compose["services"]["kvstream"]["environment"]
    # Foundry 0.10.x has no fixed port; a hard-coded one points at nothing.
    assert env["KVSTREAM_BACKEND__BASE_URL"].startswith("${FOUNDRY_URL:?")
    # `phi-3-mini` is a catalog alias the inference API refuses (HTTP 400).
    assert env["KVSTREAM_BACKEND__MODEL"] == "${KVSTREAM_MODEL:-}"


def test_no_compose_file_overrides_a_service_the_base_file_lacks():
    base = yaml.safe_load((REPO / "docker-compose.yml").read_text(encoding="utf-8"))
    defined = set(base["services"])
    for path in REPO.glob("docker-compose*.yml"):
        services = yaml.safe_load(path.read_text(encoding="utf-8")).get("services", {})
        assert set(services) <= defined, f"{path.name} overrides {set(services) - defined}"


def test_the_container_healthcheck_outlasts_the_readiness_probe():
    dockerfile = (REPO / "Dockerfile").read_text(encoding="utf-8")
    outer = re.search(r"HEALTHCHECK[^\n]*--timeout=(\d+)s", dockerfile)
    assert outer is not None
    default_probe = Settings().backend.readiness_timeout_seconds
    # The client waits for the probe plus margin, read from the same variable.
    assert "KVSTREAM_BACKEND__READINESS_TIMEOUT_SECONDS" in dockerfile
    assert f"'{default_probe:g}'))+10" in dockerfile
    assert int(outer.group(1)) > default_probe + 10


# ----------------------------------------------------------------------
# 6. One outcome per streamed request
# ----------------------------------------------------------------------


class _Stream(StubBackend):
    def __init__(self, pieces: int, fail_after: int | None = None) -> None:
        super().__init__()
        self.pieces = pieces
        self.fail_after = fail_after

    async def chat(self, payload, headers=None):
        for i in range(self.pieces):
            if self.fail_after is not None and i == self.fail_after:
                raise FoundryError("read timed out")
            await asyncio.sleep(0.005)
            yield Token(text=f"t{i} ")
        yield Token(text="", finish_reason="stop")


def _outcomes(gw) -> dict[str, float]:
    names = ("served", "abandoned", "error")
    return {n: gw.metrics.requests.labels("chat", n)._value.get() for n in names}


async def _stream(gw, *, abandon: bool):
    raw = {"model": "stub-model", "messages": [{"role": "user", "content": "x"}], "stream": True}
    req = ChatCompletionRequest(**raw)
    rc, _ = gw._cost(req)
    resp = await gw.handle_streaming("r", req, raw, rc, None)
    if abandon:
        await resp.body_iterator.__anext__()
        await resp.body_iterator.aclose()
    else:
        async for _ in resp.body_iterator:
            pass
    for _ in range(200):
        if not gw._runs:
            break
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_an_abandoned_stream_is_counted_once_as_abandoned():
    _, gw = _gateway(_Stream(500))
    await _stream(gw, abandon=True)
    assert _outcomes(gw) == {"served": 0, "abandoned": 1, "error": 0}


@pytest.mark.asyncio
async def test_a_stream_failing_part_way_is_an_error_not_served():
    _, gw = _gateway(_Stream(20, fail_after=3))
    await _stream(gw, abandon=False)
    assert _outcomes(gw) == {"served": 0, "abandoned": 0, "error": 1}


@pytest.mark.asyncio
async def test_a_completed_stream_is_served():
    _, gw = _gateway(_Stream(3))
    await _stream(gw, abandon=False)
    assert _outcomes(gw) == {"served": 1, "abandoned": 0, "error": 0}


# ----------------------------------------------------------------------
# 7. Histograms reach LLM-scale latencies
# ----------------------------------------------------------------------


def test_latency_histograms_cover_minutes():
    _, gw = _gateway()
    for histogram in (gw.metrics.latency, gw.metrics.admission_wait):
        bounds = list(histogram._upper_bounds)
        assert 120 in bounds and 600 in bounds  # admission timeout, long streams
    assert gw.settings.admission.admission_timeout_seconds <= 600


# ----------------------------------------------------------------------
# 8. The drift gauge never keeps a stale value
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_drift_gauge_resets_with_the_monitor():
    app, gw = _gateway()
    gw.drift = DriftMonitor(0.01, min_samples=1)
    gw._record_backend_success(1.0, 10, "stub-model")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        assert "kvstream_backend_drift_ratio 10.0" in (await c.get("/metrics")).text
        gw.drift = gw._new_drift_monitor()  # environment changed: no baseline
        assert "kvstream_backend_drift_ratio 0.0" in (await c.get("/metrics")).text


# ----------------------------------------------------------------------
# 9. A malformed timestamp in the store cannot stop the gateway starting
# ----------------------------------------------------------------------


def test_a_non_numeric_measured_at_does_not_break_lookup(tmp_path):
    path = tmp_path / "s.json"
    entry = {"model": "m", "device": "d", "cli_generation": "g", "backend_version": "v"}
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "entries": {
                    "a": {**entry, "budget_tokens": 100, "measured_at": 5.0},
                    "b": {**entry, "budget_tokens": 200, "measured_at": "yesterday"},
                },
            }
        )
    )
    hit = lookup_budget(str(path), CalibrationKey("m", "d", "g", "v"))
    assert hit.match == "exact"
    assert hit.budget_tokens == 100  # the readable timestamp is the newest


def test_the_gateway_starts_with_a_malformed_store(tmp_path):
    path = tmp_path / "s.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "entries": {
                    # Two candidates, so the newest has to be chosen by timestamp.
                    "x": {
                        "model": "stub-model",
                        "device": "d",
                        "budget_tokens": 100,
                        "measured_at": "yesterday",
                    },
                    "y": {
                        "model": "stub-model",
                        "device": "d",
                        "budget_tokens": 300,
                        "measured_at": 5.0,
                    },
                },
            }
        )
    )
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.backend.device = "d"
    settings.admission.mode = "tokens"
    settings.admission.calibration_store = str(path)
    gw = build_app(settings).state.gateway
    assert (gw.capacity.unit, gw.capacity.budget) == ("tokens", 300)


# ----------------------------------------------------------------------
# 10. Estimators and drift are per model, and per kind of traffic
# ----------------------------------------------------------------------


class _Usage(StubBackend):
    async def chat_once(self, payload, headers=None, timeout=None):
        body = await super().chat_once(payload, headers, timeout)
        return {**body, "usage": {"prompt_tokens": 104, "completion_tokens": 3}}


@pytest.mark.asyncio
async def test_another_models_usage_does_not_train_this_models_estimator():
    app, gw = _gateway(_Usage())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post(
            "/v1/chat/completions",
            json={"model": "other-model", "messages": [{"role": "user", "content": "a" * 400}]},
        )
    assert gw.estimator.calibrated is False  # stub-model's estimator untouched
    assert gw.estimators.for_model("other-model").calibrated is True


@pytest.mark.asyncio
async def test_embedding_usage_does_not_train_the_chat_estimator():
    app, gw = _gateway()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/v1/embeddings", json={"model": "stub-model", "input": "a" * 21})
    assert gw.estimator.calibrated is False
    assert gw.estimators.for_model("stub-model", KIND_EMBEDDINGS).calibrated is True


def test_the_estimator_registry_is_bounded():
    _, gw = _gateway()
    for i in range(200):
        gw.estimators.learner(f"model-{i}")
    assert len(gw.estimators.stats()["learned"]) == 64
    assert gw.estimators.learner("one-too-many") is None


def test_drift_is_fed_only_by_the_calibrated_model():
    _, gw = _gateway()
    gw.drift = DriftMonitor(0.01, min_samples=1)
    gw._record_backend_success(5.0, 10, "other-model")
    assert gw.drift.stats()["samples"] == 0
    gw._record_backend_success(5.0, 10, "stub-model:2")  # same model, versioned
    assert gw.drift.stats()["samples"] == 1


# ----------------------------------------------------------------------
# 11. A timeout under load is not a concurrency cap
# ----------------------------------------------------------------------


def _point(concurrency, tps, errors=0, timeouts=0):
    return SweepPoint(
        concurrency,
        concurrency * 100,
        0.1,
        errors,
        p99_per_token=0.001,
        tokens_per_second=tps,
        timeouts=timeouts,
    )


def test_timeouts_alone_do_not_label_a_runtime_capped():
    profile = classify_runtime(
        [_point(1, 10.0), _point(2, 10.1), _point(4, 0.0, errors=4, timeouts=4)]
    )
    assert profile.regime != CAPPED
    assert profile.refuses_beyond is None
    assert "timed out" in profile.detail


def test_refusals_still_label_a_runtime_capped():
    profile = classify_runtime(
        [_point(1, 10.0), _point(2, 10.1), _point(4, 0.0, errors=4, timeouts=1)]
    )
    assert profile.regime == CAPPED
    assert profile.refuses_beyond == 4


@pytest.mark.asyncio
async def test_a_probe_that_times_out_is_counted_as_a_timeout(tmp_path):
    class Slow:
        model = "m"
        base_url = "http://stub"

        async def chat(self, payload, headers=None):
            try:
                raise httpx.ReadTimeout("read timed out")
            except httpx.ReadTimeout as cause:
                raise FoundryError("did not respond in time") from cause
            yield  # pragma: no cover

    svc = CalibrationService(Slow(), str(tmp_path / "c.json"), CalibrationKey("m", "cpu"))
    point = await svc._measure(2, [(10, 10)], 1)
    assert (point.errors, point.timeouts, point.refusals) == (2, 2, 0)


# ----------------------------------------------------------------------
# 12. Estimated usage is reported without the admission safety factor
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_estimated_usage_excludes_the_safety_factor():
    app, gw = _gateway(token_safety_factor=2.0)
    messages = [{"role": "user", "content": "a" * 400}]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/v1/chat/completions", json={"model": "stub-model", "messages": messages})
    usage = r.json()["usage"]
    assert r.headers["X-KVStream-Usage"] == "estimated"
    unscaled = gw.estimator.estimate_messages(
        ChatCompletionRequest(model="stub-model", messages=messages).messages, with_safety=False
    )
    assert usage["prompt_tokens"] == unscaled  # not doubled


# ----------------------------------------------------------------------
# 13. A different model build is only a partial calibration match
# ----------------------------------------------------------------------


def test_a_different_model_version_is_a_partial_match(tmp_path):
    store = str(tmp_path / "s.json")
    save_budget(store, CalibrationKey("phi-gpu:1", "d", "g", "v"), 1000)
    assert lookup_budget(store, CalibrationKey("phi-gpu:2", "d", "g", "v")).match == "partial"
    # Unversioned on either side still matches exactly (Foundry accepts both forms).
    assert lookup_budget(store, CalibrationKey("phi-gpu", "d", "g", "v")).match == "exact"
    assert lookup_budget(store, CalibrationKey("phi-gpu:1", "d", "g", "v")).match == "exact"


# ----------------------------------------------------------------------
# 14. bench validates its arguments and reports successful latencies only
# ----------------------------------------------------------------------


def test_bench_rejects_a_total_of_zero():
    result = CliRunner().invoke(cli_app, ["bench", "--model", "m", "--total", "0"])
    assert result.exit_code == 2  # a usage error, not an IndexError traceback
    assert not isinstance(result.exception, IndexError)


def test_bench_does_not_report_failures_as_latency(monkeypatch):
    class Refusing:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def post(self, url, json=None):
            return httpx.Response(503, json={"error": {"message": "overloaded"}})

    monkeypatch.setattr(httpx, "AsyncClient", Refusing)
    result = CliRunner().invoke(cli_app, ["bench", "--model", "m", "--total", "3"])
    assert result.exit_code == 0
    assert "no request succeeded" in result.stdout


# ----------------------------------------------------------------------
# 15. /status lists every proxied route
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_lists_the_completions_route():
    app, _ = _gateway()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        routes = (await c.get("/status")).json()["routes"]
    assert routes["completions"] is True


# ----------------------------------------------------------------------
# Embeddings requests are not forwarded for a chat model
# ----------------------------------------------------------------------

# Captured from `foundry cache list --output json` on Foundry Local CLI 0.10.3.
CACHE_LIST_0_10_3 = (
    '{"models":[{"alias":"phi-3-mini-4k","id":"Phi-3-mini-4k-instruct-generic-gpu:2",'
    '"displayName":"Phi-3-mini-4k-instruct-generic-gpu","type":"Chat","device":"Gpu",'
    '"fileSizeMb":2181,"cached":true,"loaded":true,"license":"MIT",'
    '"supportsToolCalling":false},{"alias":"qwen2.5-7b","id":"qwen2.5-7b-instruct-cuda-gpu:4",'
    '"displayName":"qwen2.5-7b-instruct-cuda-gpu","type":"Chat","device":"Gpu",'
    '"fileSizeMb":4843,"cached":true,"loaded":false,"license":"apache-2.0",'
    '"supportsToolCalling":true}]}'
)
PHI = "Phi-3-mini-4k-instruct-generic-gpu"


@pytest.mark.asyncio
async def test_the_cache_listing_yields_loaded_models_and_types(monkeypatch):
    from kvstream.backend import foundry_cli

    async def run(argv, timeout):
        return 0, CACHE_LIST_0_10_3

    monkeypatch.setattr(foundry_cli, "_run_async", run)
    cli = foundry_cli.FoundryCli(path="foundry", version="0.10.3", generation=foundry_cli.GEN_SDK)
    entries = await foundry_cli.read_cache(cli)
    assert entries is not None
    assert foundry_cli.loaded_ids(entries) == [f"{PHI}:2"]
    assert foundry_cli.model_types(entries) == {
        f"{PHI}:2": "Chat",
        "qwen2.5-7b-instruct-cuda-gpu:4": "Chat",
    }
    assert await foundry_cli.list_loaded(cli) == [f"{PHI}:2"]  # unchanged behaviour


def test_embedding_refusal_is_for_chat_models_only_and_fails_open():
    from kvstream.backend.inventory import Snapshot, embedding_refusal

    chat = Snapshot(types={f"{PHI}:2": "Chat", "embed-model:1": "Embedding"})
    assert embedding_refusal(PHI, chat).code == "model_not_embedding"  # unversioned
    assert embedding_refusal(f"{PHI}:2", chat) is not None
    assert embedding_refusal("embed-model", chat) is None
    assert embedding_refusal("not-listed", chat) is None  # unknown: let through
    assert embedding_refusal(PHI, Snapshot()) is None  # types unreadable: let through


def _gateway_with_cli(monkeypatch, listing: str):
    from kvstream.backend import foundry_cli

    calls = {"cache": 0}

    async def run(argv, timeout):
        if argv[1:3] == ["cache", "list"]:
            calls["cache"] += 1
            return 0, listing
        return 1, ""

    monkeypatch.setattr(foundry_cli, "_run_async", run)
    backend = StubBackend()

    async def models_or_none():
        return [PHI, "embed-model"]

    backend.models_or_none = models_or_none
    app, gw = _gateway(backend)
    gw._cli = foundry_cli.FoundryCli(
        path="foundry", version="0.10.3", generation=foundry_cli.GEN_SDK
    )
    return app, gw, backend, calls


@pytest.mark.asyncio
async def test_an_embeddings_request_for_a_chat_model_never_reaches_foundry(monkeypatch):
    app, gw, backend, calls = _gateway_with_cli(monkeypatch, CACHE_LIST_0_10_3)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/v1/embeddings", json={"model": PHI, "input": "hello"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "model_not_embedding"
    assert backend.embedding_payloads == []  # nothing forwarded
    assert gw.capacity.stats()["completions"] == 0  # never admitted
    assert calls["cache"] == 1  # loaded models and types from one CLI call


@pytest.mark.asyncio
async def test_an_embeddings_request_for_an_embedding_model_is_forwarded(monkeypatch):
    listing = (
        '{"models":[{"id":"embed-model:1","type":"Embedding","loaded":true},'
        f'{{"id":"{PHI}:2","type":"Chat","loaded":false}}]}}'
    )
    app, _, backend, _ = _gateway_with_cli(monkeypatch, listing)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/v1/embeddings", json={"model": "embed-model", "input": "hello"})
    assert r.status_code == 200
    assert len(backend.embedding_payloads) == 1


# ----------------------------------------------------------------------
# Live accounting holds a running stream's reservation until it finishes
# ----------------------------------------------------------------------


async def _token_stream(
    max_tokens: int | None,
    generated: int,
    default_max_tokens: int = 512,
    apply_default: bool = True,
):
    settings = Settings()
    settings.backend.model = "stub-model"
    settings.admission.mode = "tokens"
    settings.admission.budget_tokens = 10_000
    settings.admission.default_max_tokens = default_max_tokens
    settings.admission.apply_default_max_tokens = apply_default
    app = build_app(settings)
    gw = app.state.gateway
    backend = StubBackend()

    async def chat(payload, headers=None):
        for i in range(generated):
            yield Token(text=f"t{i} ")
        yield Token(text="", finish_reason="stop")

    backend.chat = chat
    gw.backend = backend
    held: list[int] = []
    adjust = gw.capacity.adjust

    async def recording(req_id, new_cost):
        held.append(new_cost)
        return await adjust(req_id, new_cost)

    gw.capacity.adjust = recording
    body = {"model": "stub-model", "stream": True, "messages": [{"role": "user", "content": "go"}]}
    if max_tokens is not None:
        body["max_tokens"] = max_tokens
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/v1/chat/completions", json=body)
    return gw, held


@pytest.mark.asyncio
async def test_a_running_stream_keeps_its_reservation_and_never_overshoots():
    gw, held = await _token_stream(max_tokens=64, generated=40)
    # Mid-stream: never below the 64 tokens it was admitted for. It used to be
    # settled to the tokens generated so far (handing back 48 at token 16) and
    # then grown back twice, exceeding the budget at the default ratio of 1.0.
    assert held and all(cost >= held[0] for cost in held[:-1]), held
    assert gw.capacity.overshoots == 0
    assert gw.capacity.reclaimed == 64 - 40  # returned once, when it finished


@pytest.mark.asyncio
async def test_omitting_max_tokens_can_overshoot_only_with_the_default_not_sent():
    """
    With `apply_default_max_tokens` off, a request with no limit is costed at
    the default but may generate more: the one way the default ratio of 1.0
    can exceed the budget. (This stub ignores `max_tokens`, so it shows the
    accounting; whether Foundry Local honours the limit was checked live.)
    """
    gw, _ = await _token_stream(
        max_tokens=None, generated=40, default_max_tokens=4, apply_default=False
    )
    assert gw.capacity.overshoots > 0


# ----------------------------------------------------------------------
# --max-tokens: the limit sent for a request that sets none
# ----------------------------------------------------------------------


async def _sent(body_extra: dict, route: str = "/v1/chat/completions", **admission):
    app, gw = _gateway(**admission)
    sent: list[dict] = []
    backend = gw.backend

    async def chat_once(payload, headers=None, timeout=None):
        sent.append(payload)
        return await StubBackend.chat_once(backend, payload, headers, timeout)

    async def completions(payload, headers=None):
        sent.append(payload)
        return 200, {"choices": [{"text": "x"}]}

    backend.chat_once = chat_once
    backend.completions = completions
    body = {"model": "stub-model", **body_extra}
    if route == "/v1/chat/completions":
        body["messages"] = [{"role": "user", "content": "hi"}]
    else:
        body["prompt"] = "hi"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post(route, json=body)
    assert r.status_code == 200
    return sent[0]


@pytest.mark.asyncio
async def test_a_request_with_no_limit_is_sent_the_default():
    assert (await _sent({}))["max_tokens"] == 512
    assert (await _sent({}, default_max_tokens=128))["max_tokens"] == 128


@pytest.mark.asyncio
async def test_a_limit_the_client_set_is_never_changed():
    assert (await _sent({"max_tokens": 2000}))["max_tokens"] == 2000  # larger: untouched
    sent = await _sent({"max_completion_tokens": 50})
    assert sent["max_completion_tokens"] == 50
    assert "max_tokens" not in sent  # not doubled up with a second limit


@pytest.mark.asyncio
async def test_the_default_can_be_switched_off():
    assert "max_tokens" not in await _sent({}, apply_default_max_tokens=False)


@pytest.mark.asyncio
async def test_legacy_completions_get_the_default_too():
    sent = await _sent({}, route="/v1/completions", default_max_tokens=64)
    assert sent["max_tokens"] == 64


@pytest.mark.asyncio
async def test_a_streamed_request_with_no_limit_is_sent_the_default():
    app, gw = _gateway()
    sent: list[dict] = []
    backend = gw.backend
    original = backend.chat

    async def chat(payload, headers=None):
        sent.append(payload)
        async for tok in original(payload, headers):
            yield tok

    backend.chat = chat
    body = {"model": "stub-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/v1/chat/completions", json=body)
    assert sent[0]["max_tokens"] == 512


def _serve_settings(monkeypatch, args):
    captured: dict = {}
    monkeypatch.setattr("kvstream.server.serve", lambda s: captured.setdefault("s", s))
    result = CliRunner().invoke(cli_app, ["serve", *args])
    return result, captured.get("s")


def test_serve_takes_max_tokens(monkeypatch):
    result, settings = _serve_settings(monkeypatch, ["--max-tokens", "200"])
    assert result.exit_code == 0, result.output
    assert settings.admission.default_max_tokens == 200
    assert "200 when a request sets none" in result.output


def test_serve_defaults_max_tokens_to_512(monkeypatch):
    result, settings = _serve_settings(monkeypatch, [])
    assert result.exit_code == 0, result.output
    assert settings.admission.default_max_tokens == 512
    assert "512 when a request sets none" in result.output


def test_serve_rejects_a_zero_max_tokens():
    result = CliRunner().invoke(cli_app, ["serve", "--max-tokens", "0"])
    assert result.exit_code == 2
