"""
G-50: the CLI had no test coverage at all.

Every `typer` command was untested — including `calibrate`, which is the only
way to produce the number admission control depends on. These drive the commands
through `CliRunner` with the network stubbed out.
"""

from __future__ import annotations

import json

import httpx
import pytest
from typer.testing import CliRunner

from kvstream import cli as cli_module
from kvstream.cli import app

runner = CliRunner()


def _transport(handler) -> None:
    """Point the CLI's httpx calls at a mock transport."""
    return httpx.MockTransport(handler)


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def test_help_lists_every_command():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in ("serve", "health", "status", "calibrate", "bench"):
        assert command in result.stdout


# -- health -------------------------------------------------------------


def test_health_reports_a_healthy_backend(monkeypatch):
    monkeypatch.setattr(
        httpx,
        "get",
        lambda url, timeout=None: _FakeResponse(
            {
                "status": "ok",
                "backend_healthy": True,
                "backend_url": "http://localhost:5273",
                "model": "phi-3-mini",
            }
        ),
    )
    result = runner.invoke(app, ["health"])
    assert result.exit_code == 0
    assert "healthy" in result.stdout


def test_health_reports_an_unreachable_backend(monkeypatch):
    monkeypatch.setattr(
        httpx,
        "get",
        lambda url, timeout=None: _FakeResponse(
            {
                "status": "degraded",
                "backend_healthy": False,
                "backend_url": "http://localhost:5273",
                "model": "phi-3-mini",
            }
        ),
    )
    result = runner.invoke(app, ["health"])
    # Not serving is a failure a script must be able to see (it exited 0 before).
    assert result.exit_code == 1
    assert "unreachable" in result.stdout
    assert "Foundry Local running" in result.stdout


def test_health_does_not_call_a_stalled_backend_healthy(monkeypatch):
    """
    Answers /v1/models but cannot generate: the failure the readiness probe
    exists for. This used to print "healthy" and exit 0.
    """
    monkeypatch.setattr(
        httpx,
        "get",
        lambda url, timeout=None: _FakeResponse(
            {
                "status": "degraded",
                "backend_healthy": True,
                "backend_reachable": True,
                "backend_serving": False,
                "backend_url": "http://127.0.0.1:63962",
                "model": "m",
                "hint": "The backend is reachable but not serving: no 1-token generation in 15s.",
            }
        ),
    )
    result = runner.invoke(app, ["health"])
    out = " ".join(result.stdout.split())
    assert result.exit_code == 1
    assert "NOT serving" in out
    assert "healthy" not in out.replace("healthy (serving)", "")
    assert "no 1-token generation" in out  # the gateway's own hint


def test_health_exits_nonzero_when_the_gateway_is_down(monkeypatch):
    def boom(url, timeout=None):
        raise httpx.ConnectError("nope")

    monkeypatch.setattr(httpx, "get", boom)
    result = runner.invoke(app, ["health"])
    assert result.exit_code == 1
    assert "Cannot reach" in result.stdout


# -- status -------------------------------------------------------------


STATUS_PAYLOAD = {
    "admission": {
        "unit": "tokens",
        "budget": 8000,
        "in_flight": 120,
        "utilization": 0.015,
        "waiting": 2,
        "active": 1,
        "overshoots": 0,
        "queue": {"peak_depth": 5, "oldest_wait_seconds": 1.25},
    },
    "budget_source": {"source": "calibration:exact"},
    "backend": {"base_url": "http://localhost:5273", "usage_reporting": True},
}


def test_status_renders_the_table(monkeypatch):
    monkeypatch.setattr(httpx, "get", lambda url, timeout=None: _FakeResponse(STATUS_PAYLOAD))
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    for expected in ("tokens", "8000", "calibration:exact", "http://localhost:5273"):
        assert expected in result.stdout


def test_status_exits_nonzero_when_unreachable(monkeypatch):
    def boom(url, timeout=None):
        raise httpx.ConnectError("nope")

    monkeypatch.setattr(httpx, "get", boom)
    assert runner.invoke(app, ["status"]).exit_code == 1


# -- calibrate ----------------------------------------------------------


def test_calibrate_refuses_when_the_backend_is_down(monkeypatch, tmp_path):
    class DeadClient:
        def __init__(self, **kwargs) -> None:
            pass

        async def health(self) -> bool:
            return False

        async def aclose(self) -> None:
            pass

    monkeypatch.setattr("kvstream.backend.FoundryClient", DeadClient)
    result = runner.invoke(app, ["calibrate", "--model", "m"])
    assert result.exit_code == 1
    # The message has to name the command that would actually fix it.
    assert "foundry service start" in result.stdout
    assert "foundry server start" in result.stdout


def test_calibrate_stores_a_budget_under_the_full_key(monkeypatch, tmp_path, capsys):
    store = tmp_path / "calibration.json"

    class LiveClient:
        model = "phi-3-mini"
        base_url = "http://localhost:5273"

        def __init__(self, **kwargs) -> None:
            pass

        async def health(self) -> bool:
            return True

        async def detect_cli(self):
            from kvstream.backend.foundry_cli import FoundryCli

            return FoundryCli(path="foundry", version="0.10.3", generation="0.10.x")

        async def models_or_none(self):
            return None  # unreadable: the pre-sweep model check fails open

        async def chat(self, payload, headers=None):
            yield _Chunk()

        async def aclose(self) -> None:
            pass

    monkeypatch.setattr("kvstream.backend.FoundryClient", LiveClient)

    result = runner.invoke(
        app,
        [
            "calibrate",
            "--model",
            "phi-3-mini",
            "--device",
            "npu",
            "--max-concurrency",
            "2",
            "--trials",
            "1",
            "--warmup",
            "0",
            "--no-refine",
        ],
        env={"KVSTREAM_ADMISSION__CALIBRATION_STORE": str(store)},
    )
    assert result.exit_code == 0, result.stdout
    assert "Calibrated budget" in result.stdout

    saved = json.loads(store.read_text(encoding="utf-8"))
    (entry,) = saved["entries"].values()
    assert entry["model"] == "phi-3-mini"
    assert entry["device"] == "npu"
    assert entry["budget_tokens"] > 0
    # Keyed by the detected runtime, as `serve` looks it up — it used to be
    # "unknown|unknown", so a gateway could only ever find it as a partial match.
    assert entry["cli_generation"] == "0.10.x"
    assert entry["backend_version"] == "0.10.3"


class _Chunk:
    text = "x"
    finish_reason = "stop"
    usage = None
    raw = None


# -- bench --------------------------------------------------------------


def test_bench_reports_latency(monkeypatch):
    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def post(self, url, json=None):
            return httpx.Response(200, json={"choices": []})

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    result = runner.invoke(app, ["bench", "--model", "m", "--total", "4", "--concurrency", "2"])
    assert result.exit_code == 0
    assert "KVStream Benchmark" in result.stdout
    assert "p99" in result.stdout


def test_bench_counts_errors(monkeypatch):
    class FailingClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def post(self, url, json=None):
            return httpx.Response(503, json={"error": {"message": "overloaded"}})

    monkeypatch.setattr(httpx, "AsyncClient", FailingClient)
    result = runner.invoke(app, ["bench", "--model", "m", "--total", "3", "--concurrency", "3"])
    assert result.exit_code == 0
    assert "errors" in result.stdout


def _status_returns(monkeypatch, model_info: dict) -> list[dict]:
    """Make the gateway's /status report ``model_info``; record bench's requests."""
    sent: list[dict] = []

    def fake_get(url, timeout=None):
        assert url.endswith("/status")
        return httpx.Response(200, json={"model": model_info})

    class RecordingClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def post(self, url, json=None):
            sent.append(json)
            return httpx.Response(200, json={"choices": []})

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(httpx, "AsyncClient", RecordingClient)
    return sent


def test_bench_without_model_uses_the_model_the_gateway_resolved(monkeypatch):
    sent = _status_returns(
        monkeypatch,
        {
            "id": "Phi-3-mini-4k-instruct-generic-gpu:2",
            "source": "auto-detected: the only model loaded",
        },
    )
    result = runner.invoke(app, ["bench", "--total", "2", "--concurrency", "1"])
    assert result.exit_code == 0
    assert {p["model"] for p in sent} == {"Phi-3-mini-4k-instruct-generic-gpu:2"}


def test_bench_without_model_refuses_when_the_gateway_has_none(monkeypatch):
    sent = _status_returns(monkeypatch, {"id": None, "source": "undetermined: no model is loaded"})
    result = runner.invoke(app, ["bench", "--total", "2"])
    assert result.exit_code == 1
    assert "no model is loaded" in result.stdout
    assert sent == []  # nothing benchmarked against a made-up model


# -- output encoding ----------------------------------------------------


def test_output_is_widened_for_redirected_streams():
    """The Windows bug: cp1252 stdout could not encode the banner or the logs."""
    cli_module._widen_output()  # must be safe to call repeatedly
    cli_module._widen_output()


@pytest.mark.parametrize("command", ["serve", "calibrate", "bench", "health", "status"])
def test_every_command_has_help(command):
    result = runner.invoke(app, [command, "--help"])
    assert result.exit_code == 0


def test_calibrate_refuses_an_unloaded_model_before_sweeping(monkeypatch, tmp_path):
    """
    Measured: with the model unloaded, the sweep failed with "Foundry Local is
    unreachable … start it with `foundry server start`" — for a running server.
    """
    store = tmp_path / "calibration.json"
    probes: list[dict] = []

    class RunningButUnloaded:
        model = None
        base_url = "http://localhost:5273"

        def __init__(self, **kwargs) -> None:
            self.model = kwargs.get("model")

        async def health(self) -> bool:
            return True

        async def detect_cli(self):
            from kvstream.backend.foundry_cli import FoundryCli

            return FoundryCli(path="foundry", version="0.10.3", generation="0.10.x")

        async def models_or_none(self):
            return ["Phi-3-mini-4k-instruct-generic-gpu"]  # downloaded...

        async def chat(self, payload, headers=None):
            probes.append(payload)
            yield _Chunk()

        async def aclose(self) -> None:
            pass

    async def nothing_loaded(cli, timeout=5.0):
        return []  # ...but not loaded

    monkeypatch.setattr("kvstream.backend.FoundryClient", RunningButUnloaded)
    monkeypatch.setattr("kvstream.backend.foundry_cli.list_loaded", nothing_loaded)
    result = runner.invoke(
        app,
        ["calibrate", "--model", "Phi-3-mini-4k-instruct-generic-gpu:2", "--warmup", "0"],
        env={"KVSTREAM_ADMISSION__CALIBRATION_STORE": str(store)},
    )
    out = " ".join(result.stdout.split())  # Rich wraps long lines
    assert result.exit_code == 1
    assert "not loaded" in out
    assert "foundry model load" in out
    assert "server start" not in out
    assert probes == []  # no sweep was sent
    assert not store.exists()


def test_health_waits_longer_than_the_readiness_probe(monkeypatch):
    """The gateway's probe may take up to 15 s; a 5 s client timed out first."""
    seen: dict = {}

    def get(url, timeout=None):
        seen["timeout"] = timeout
        return _FakeResponse({"status": "ok", "backend_serving": True})

    monkeypatch.setattr(httpx, "get", get)
    assert runner.invoke(app, ["health"]).exit_code == 0
    assert seen["timeout"] > 15


def test_a_health_timeout_is_not_reported_as_unreachable(monkeypatch):
    def slow(url, timeout=None):
        raise httpx.ReadTimeout("timed out")

    monkeypatch.setattr(httpx, "get", slow)
    result = runner.invoke(app, ["health", "--timeout", "2"])
    out = " ".join(result.stdout.split())
    assert result.exit_code == 1
    assert "did not answer within 2s" in out
    assert "Cannot reach" not in out
