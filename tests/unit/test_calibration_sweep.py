"""
G-34: the sweep has to be worth trusting before its number is.

Doubling alone resolves the knee only to the previous power of two — up to half
the machine's capacity thrown away. A cold first request makes every later point
look slow by comparison. A single unlucky sample can define the knee. And a
uniform request shape measures the one workload where a token budget makes no
difference. Each of those is fixed here, and each is tested.
"""

from __future__ import annotations

import asyncio

import pytest

from kvstream.admission.calibration import (
    CalibrationKey,
    CalibrationService,
    SweepPoint,
    _percentile,
    _probe_shapes,
    find_knee,
    lookup_budget,
)


class FakeBackend:
    """
    A backend with a hard concurrency ceiling and a cold first request.

    Below ``ceiling`` every request takes ``base`` seconds; at or above it,
    latency jumps — which is exactly the knee calibration is looking for.
    """

    def __init__(self, ceiling: int = 12, base: float = 0.01, cold: float = 5.0) -> None:
        self.model = "stub-model"
        self.base_url = "http://stub"
        self.ceiling = ceiling
        self.base = base
        self.cold = cold
        self.calls = 0
        self.concurrent = 0
        self.peak = 0
        self.observed_shapes: set[tuple[int, int]] = set()

    async def chat(self, payload, headers=None):
        self.calls += 1
        first = self.calls == 1
        self.concurrent += 1
        self.peak = max(self.peak, self.concurrent)
        here = self.concurrent
        content = payload["messages"][0]["content"]
        self.observed_shapes.add((len(content), payload["max_tokens"]))
        try:
            # A real await point, so concurrent probes actually overlap and the
            # ceiling means something.
            await asyncio.sleep(0)
            here = self.concurrent
            # Latency is a pure function of load, so the sweep is deterministic.
            self._latency = self.base * (10 if here > self.ceiling else 1)
            if first:
                self._latency += self.cold
            yield _tok(self._latency)
        finally:
            self.concurrent -= 1


class _Tok:
    def __init__(self, latency: float) -> None:
        self.text = "x"
        self.finish_reason = "stop"
        self.usage = None
        self.raw = None
        self.latency = latency


def _tok(latency: float) -> _Tok:
    return _Tok(latency)


# -- pure helpers -------------------------------------------------------


def test_percentile_handles_an_empty_sample():
    assert _percentile([], 0.99) == 0.0


def test_percentile_is_nearest_rank():
    assert _percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 3.0
    assert _percentile([5.0], 0.99) == 5.0


def test_probe_shapes_are_not_uniform():
    """A budget measured on identical requests would not describe real traffic."""
    shapes = _probe_shapes(256, 64)
    assert len({p for p, _ in shapes}) > 1
    assert (256, 64) in shapes


def test_probe_shapes_stay_positive_at_tiny_sizes():
    for prompt, max_tokens in _probe_shapes(1, 1):
        assert prompt >= 1 and max_tokens >= 1


def test_find_knee_still_behaves():
    fast = SweepPoint(1, 100, 1.0, 0)
    ok = SweepPoint(2, 200, 1.5, 0)
    slow = SweepPoint(4, 400, 9.0, 0)
    assert find_knee([fast, ok, slow]) is ok
    assert find_knee([]) is None
    assert find_knee([SweepPoint(1, 100, 1.0, 3)]) is None


# -- the sweep ----------------------------------------------------------


@pytest.mark.asyncio
async def test_the_sweep_warms_up_before_measuring(tmp_path, monkeypatch):
    """A cold first request must not become the baseline everything is judged against."""
    backend = FakeBackend(ceiling=8, cold=10.0)
    svc = CalibrationService(backend, str(tmp_path / "c.json"), CalibrationKey("stub-model", "cpu"))
    _stub_latency(monkeypatch, svc, backend)

    await svc.calibrate(max_concurrency=4, trials=1, warmup=1, refine=False)

    record = lookup_budget(str(tmp_path / "c.json"), CalibrationKey("stub-model", "cpu"))
    sweep = record.record["sweep"]
    # The very slow cold request was burned before the first measured point.
    assert sweep[0]["p99_seconds"] < 1.0


@pytest.mark.asyncio
async def test_bisection_narrows_the_knee(tmp_path, monkeypatch):
    """Doubling finds the order of magnitude; bisection recovers the rest."""
    coarse_backend = FakeBackend(ceiling=12)
    coarse = CalibrationService(
        coarse_backend,
        str(tmp_path / "coarse.json"),
        CalibrationKey("stub-model", "cpu"),
    )
    _stub_latency(monkeypatch, coarse, coarse_backend)
    coarse_budget = await coarse.calibrate(max_concurrency=32, trials=1, refine=False)

    fine_backend = FakeBackend(ceiling=12)
    fine = CalibrationService(
        fine_backend, str(tmp_path / "fine.json"), CalibrationKey("stub-model", "cpu")
    )
    _stub_latency(monkeypatch, fine, fine_backend)
    fine_budget = await fine.calibrate(max_concurrency=32, trials=1, refine=True)

    # Doubling stops at 8 (16 is over the ceiling of 12); refining reaches 12.
    assert fine_budget > coarse_budget


@pytest.mark.asyncio
async def test_trials_pool_their_latencies(tmp_path, monkeypatch):
    backend = FakeBackend(ceiling=8)
    svc = CalibrationService(backend, str(tmp_path / "c.json"), CalibrationKey("stub-model", "cpu"))
    _stub_latency(monkeypatch, svc, backend)

    await svc.calibrate(max_concurrency=2, trials=4, warmup=0, refine=False)
    # 4 trials at c=1 plus 4 at c=2 — every point is measured repeatedly.
    assert backend.calls >= 12


@pytest.mark.asyncio
async def test_the_sweep_uses_mixed_request_shapes(tmp_path, monkeypatch):
    backend = FakeBackend(ceiling=8)
    svc = CalibrationService(backend, str(tmp_path / "c.json"), CalibrationKey("stub-model", "cpu"))
    _stub_latency(monkeypatch, svc, backend)

    await svc.calibrate(max_concurrency=4, trials=1, warmup=0, refine=False)
    assert len(backend.observed_shapes) > 1


@pytest.mark.asyncio
async def test_the_result_is_stored_under_the_full_key(tmp_path, monkeypatch):
    backend = FakeBackend(ceiling=8)
    key = CalibrationKey("stub-model", "npu", "0.10.x", "0.10.0")
    store = str(tmp_path / "c.json")
    svc = CalibrationService(backend, store, key)
    _stub_latency(monkeypatch, svc, backend)

    budget = await svc.calibrate(max_concurrency=4, trials=1, refine=False)
    hit = lookup_budget(store, key)
    assert hit.match == "exact"
    assert hit.budget_tokens == budget
    assert hit.record["sweep"]  # the evidence is kept with the number
    assert hit.age_seconds >= 0


@pytest.mark.asyncio
async def test_a_backend_that_fails_everything_yields_a_single_request_budget(tmp_path):
    class Dead:
        model = "stub-model"
        base_url = "http://stub"

        async def chat(self, payload, headers=None):
            raise RuntimeError("backend down")
            yield  # pragma: no cover

    store = str(tmp_path / "c.json")
    svc = CalibrationService(Dead(), store, CalibrationKey("stub-model", "cpu"))
    budget = await svc.calibrate(max_concurrency=4, trials=1, warmup=0)
    # Enough for one probe request and no more — honest about knowing nothing.
    assert budget > 0
    assert budget <= 256 + 64


def _stub_latency(monkeypatch, service: CalibrationService, backend: FakeBackend) -> None:
    """Make `_probe_once` report the backend's synthetic latency directly."""

    async def probe(prompt_tokens: int, max_tokens: int) -> float | None:
        latency = None
        try:
            async for tok in backend.chat(
                {
                    "model": backend.model,
                    "messages": [{"role": "user", "content": "x " * prompt_tokens}],
                    "max_tokens": max_tokens,
                }
            ):
                latency = tok.latency
        except Exception:  # noqa: BLE001
            return None
        return latency

    monkeypatch.setattr(service, "_probe_once", probe)
