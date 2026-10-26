"""
Calibration — measure a Foundry Local instance's KV-token budget ``B``.

KVStream cannot read Foundry Local's memory, so the token budget is **measured,
not asserted**. A load sweep drives increasing concurrency and watches for the
"knee" — the point where p99 latency inflects or errors appear. ``B`` is set to
the largest sustained in-flight token load *before* the knee, scaled by a safety
margin, and persisted under a :class:`CalibrationKey`.

Why the key has four parts
--------------------------
A budget is only meaningful for the environment it was measured in. Model and
device are the obvious axes; the Foundry Local version and CLI generation matter
too, because the service-based CLI and the SDK-backed ``foundry server`` need
not enforce concurrency the same way. A record measured elsewhere is worse than
no record — it produces a confidently wrong budget — so a partial match is used
only with a warning and a mismatched model is ignored outright.

The knee-detection logic (:func:`find_knee`) is pure and unit-tested; the sweep
(:meth:`CalibrationService.calibrate`) requires a live Foundry Local instance and
is invoked by ``kvstream calibrate``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import platform
import tempfile
import time
from dataclasses import dataclass

import httpx

from kvstream.admission.regime import RuntimeProfile, classify_runtime
from kvstream.backend.foundry import FoundryClient
from kvstream.backend.inventory import unversioned

logger = logging.getLogger("kvstream.calibration")

STORE_VERSION = 2
UNKNOWN = "unknown"


class CalibrationFailed(RuntimeError):
    """The sweep could not measure anything: nothing is saved."""


def resolve_device(configured: str | None) -> str:
    """
    Turn the configured device label into a stable string.

    ``"auto"`` (or empty) derives a platform identifier. The gateway sits at the
    HTTP layer and genuinely cannot see which accelerator Foundry Local chose,
    so this is a coarse label, not a detection — operators running more than one
    accelerator should set ``backend.device`` explicitly.
    """
    if configured and configured.strip() and configured.strip().lower() != "auto":
        return configured.strip()
    return f"{platform.system()}-{platform.machine()}".lower()


@dataclass(frozen=True)
class CalibrationKey:
    """Identifies the environment a budget was measured in."""

    model: str
    device: str = UNKNOWN
    cli_generation: str = UNKNOWN
    backend_version: str = UNKNOWN

    def as_str(self) -> str:
        return "|".join((self.model, self.device, self.cli_generation, self.backend_version))

    def as_dict(self) -> dict:
        return {
            "model": self.model,
            "device": self.device,
            "cli_generation": self.cli_generation,
            "backend_version": self.backend_version,
        }


@dataclass
class SweepPoint:
    concurrency: int
    inflight_tokens: int
    p99_seconds: float
    errors: int
    # p99 of *service time per requested token*. Raw latency cannot be compared
    # across sweep levels once the probe mixes request sizes: a bigger shape
    # appearing at higher concurrency looks exactly like congestion. Normalising
    # by the request's own cost removes the shape and leaves the load.
    p99_per_token: float = 0.0
    # Median service time per token, the drift baseline (see `baseline`).
    p50_per_token: float = 0.0
    # Completed *generated* tokens per second — the signal that tells one kind of
    # runtime from another, and it took three attempts to get the unit right.
    #
    # Requests per second moves with the probe's shape mix as much as with the
    # runtime, so a batching engine reads as flat. Prompt-plus-generated tokens
    # per second is worse: prefill is far cheaper per token than decode, so as
    # the mix tilts prompt-heavy at higher concurrency it manufactures a gain
    # that is not there — on the reference machine it turned a runtime measured
    # as flat into a false 1.43x "batching" reading. Only the generated half is
    # a consistent unit of work across sweep levels. Counted from what the
    # backend actually generated (its `usage`, else the streamed chunks), not
    # from `max_tokens`: a model that stops early did not do that work.
    tokens_per_second: float = 0.0
    # How many of ``errors`` were requests that ran out of time rather than
    # being refused. A runtime that serialises makes requests queue inside it
    # until they time out; that is not a concurrency cap, and calling it one
    # sent operators after the wrong lever.
    timeouts: int = 0

    @property
    def refusals(self) -> int:
        """Errors that were not timeouts: the backend turned the request down."""
        return max(0, self.errors - self.timeouts)

    @property
    def signal(self) -> float:
        """The quantity the knee is detected on."""
        return self.p99_per_token or self.p99_seconds


def find_knee(points: list[SweepPoint], latency_ratio: float = 2.0) -> SweepPoint | None:
    """
    Return the last healthy sweep point before the knee, or ``None`` if the very
    first point already fails.

    The knee is the first point whose p99 exceeds ``latency_ratio`` times the
    baseline (first point) p99, or the first point that produced any error.
    """
    if not points:
        return None
    baseline = points[0].signal or 1e-9
    last_good: SweepPoint | None = None
    for pt in points:
        if pt.errors > 0 or pt.signal > baseline * latency_ratio:
            return last_good
        last_good = pt
    return last_good


# ----------------------------------------------------------------------
# Store
# ----------------------------------------------------------------------


def _as_int(value: object) -> int:
    """A stored integer, or 0 when the file holds something else."""
    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return 0


def _as_timestamp(value: object) -> float:
    """A stored ``measured_at``, or 0.0 when the file holds something else."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return 0.0
    return float(value)


def _version_suffix(model_id: str) -> str | None:
    """The numeric ``:N`` catalog version of a model id, or ``None`` if it has none."""
    _, sep, suffix = model_id.rpartition(":")
    return suffix if sep and suffix.isdigit() else None


def _read_store(store_path: str) -> dict:
    """Load the calibration store, migrating the flat v1 record if present."""
    try:
        with open(store_path, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError):
        return {"version": STORE_VERSION, "entries": {}}
    if not isinstance(raw, dict):
        return {"version": STORE_VERSION, "entries": {}}

    if isinstance(raw.get("entries"), dict):
        return raw

    # v1: a single flat record with no device or version information. Preserve
    # it under an unknown-environment key rather than discarding a real
    # measurement, but it will only ever match as a partial hit.
    if "budget_tokens" in raw and raw.get("model"):
        key = CalibrationKey(model=str(raw["model"]))
        entry = {
            "budget_tokens": _as_int(raw.get("budget_tokens", 0)),
            **key.as_dict(),
            "base_url": raw.get("base_url"),
            "measured_at": raw.get("measured_at"),
            "migrated_from": "v1",
        }
        return {"version": STORE_VERSION, "entries": {key.as_str(): entry}}

    return {"version": STORE_VERSION, "entries": {}}


def save_budget(
    store_path: str,
    key: CalibrationKey,
    budget_tokens: int,
    extra: dict | None = None,
) -> None:
    """Persist ``budget_tokens`` for ``key`` without disturbing other entries."""
    store = _read_store(store_path)
    store["version"] = STORE_VERSION
    store.setdefault("entries", {})[key.as_str()] = {
        "budget_tokens": budget_tokens,
        **key.as_dict(),
        "measured_at": time.time(),
        **(extra or {}),
    }
    directory = os.path.dirname(store_path) or "."
    os.makedirs(directory, exist_ok=True)
    # Write a sibling file and swap it in: writing in place meant a crash
    # mid-write left a truncated store, which then read as empty and silently
    # discarded every other environment's measurement.
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".calibration-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(store, f, indent=2, sort_keys=True)
        os.replace(tmp, store_path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise
    logger.info(
        "wrote calibrated budget B=%d for %s to %s", budget_tokens, key.as_str(), store_path
    )


@dataclass
class CalibrationLookup:
    """Result of a store lookup, including how good the match was."""

    budget_tokens: int
    match: str  # exact | partial | none
    record: dict | None = None
    detail: str = ""

    @property
    def usable(self) -> bool:
        return self.budget_tokens > 0 and self.match in ("exact", "partial")

    @property
    def age_seconds(self) -> float:
        """How long ago this budget was measured, or 0.0 if unknown."""
        measured_at = (self.record or {}).get("measured_at")
        if not isinstance(measured_at, int | float) or measured_at <= 0:
            return 0.0
        return max(0.0, time.time() - float(measured_at))

    def as_dict(self) -> dict:
        return {
            "budget_tokens": self.budget_tokens,
            "match": self.match,
            "detail": self.detail,
            "age_seconds": round(self.age_seconds, 1),
            "measured_at": (self.record or {}).get("measured_at"),
            "record": self.record,
        }


def lookup_budget(store_path: str, key: CalibrationKey) -> CalibrationLookup:
    """
    Find a budget for ``key``.

    * **exact** — every axis matches; used silently.
    * **partial** — same model and device, different Foundry version or CLI
      generation; used, with a warning.
    * **none** — no record for this model *and* device. A budget measured on
      other hardware is not applied; the caller falls back rather than admitting
      against a number that means nothing here.
    """
    entries = _read_store(store_path).get("entries", {})
    if not isinstance(entries, dict) or not entries:
        return CalibrationLookup(0, "none", detail="no calibration records found")

    # A model is the same model with or without its `:N` catalog suffix —
    # Foundry accepts both, and auto-detection reports the suffixed form — so
    # a budget measured under one name must not be ignored under the other.
    #
    # Two *different* versions are different builds of the model, though, and
    # need not have the same footprint: they match only partially, with the
    # warning that goes with it. Where either side has no version there is
    # nothing to compare, so the names match.
    model = unversioned(key.model)
    wanted_version = _version_suffix(key.model)
    exact: list[dict] = []
    partial: list[dict] = []
    for record in entries.values():
        if not isinstance(record, dict) or _as_int(record.get("budget_tokens")) <= 0:
            continue
        recorded_model = str(record.get("model", ""))
        if unversioned(recorded_model) != model:
            continue
        if record.get("device") != key.device:
            continue
        recorded_version = _version_suffix(recorded_model)
        same_build = (
            wanted_version is None or recorded_version is None or wanted_version == recorded_version
        )
        same_runtime = (
            record.get("cli_generation") == key.cli_generation
            and record.get("backend_version") == key.backend_version
        )
        (exact if same_runtime and same_build else partial).append(record)

    def newest(records: list[dict]) -> dict:
        # Several records can qualify (re-calibrations, runtime upgrades); the
        # most recent measurement is the one that describes this machine now.
        # The store is a hand-editable file, so an unreadable timestamp sorts
        # as oldest instead of raising — this runs while the gateway starts.
        return max(records, key=lambda r: _as_timestamp(r.get("measured_at")))

    if exact:
        record = newest(exact)
        return CalibrationLookup(_as_int(record["budget_tokens"]), "exact", record)
    if partial:
        record = newest(partial)
        return CalibrationLookup(
            _as_int(record["budget_tokens"]),
            "partial",
            record,
            detail=(
                f"measured for {record.get('model')} against Foundry "
                f"{record.get('backend_version', UNKNOWN)} / CLI "
                f"{record.get('cli_generation', UNKNOWN)}, now running {key.model} on "
                f"{key.backend_version} / {key.cli_generation}"
            ),
        )

    models = sorted({str(r.get("model")) for r in entries.values() if isinstance(r, dict)})
    return CalibrationLookup(
        0,
        "none",
        detail=(
            f"no record for model {key.model!r} on device {key.device!r} "
            f"(store holds: {', '.join(models) or 'nothing'})"
        ),
    )


def _percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile; 0.0 for an empty sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(int(len(ordered) * fraction), len(ordered) - 1)
    return ordered[index]


def _probe_shapes(prompt_tokens: int, max_tokens: int) -> list[tuple[int, int]]:
    """
    A small spread of request sizes around the configured probe.

    The proposal is explicit that the token budget only earns its keep when
    request sizes vary. Calibrating on a single uniform shape would measure the
    one workload where it makes no difference.
    """
    return [
        (max(1, prompt_tokens // 4), max(1, max_tokens // 2)),
        (prompt_tokens, max_tokens),
        (prompt_tokens * 2, max_tokens),
    ]


def _log_point(point: SweepPoint, prefix: str = "sweep") -> None:
    logger.info(
        "%s c=%d inflight=%d p99=%.2fs per-token=%.5fs errors=%d",
        prefix,
        point.concurrency,
        point.inflight_tokens,
        point.p99_seconds,
        point.p99_per_token,
        point.errors,
    )


class CalibrationService:
    def __init__(
        self,
        client: FoundryClient,
        store_path: str,
        key: CalibrationKey | None = None,
    ) -> None:
        if not client.model:
            # The sweep sends real requests, and a budget is only meaningful
            # for the model it was measured on — there is nothing to fall back to.
            raise ValueError("calibration needs a model: pass --model or load exactly one")
        self._client = client
        self._store_path = store_path
        self._key = key or CalibrationKey(model=client.model)
        self.profile = RuntimeProfile()
        self.last_error: str = ""
        self.baseline_per_token = 0.0
        # Failed probes that ran out of time (see `SweepPoint.timeouts`).
        self._timed_out = 0

    @property
    def key(self) -> CalibrationKey:
        return self._key

    async def _probe_once(
        self, prompt_tokens: int, max_tokens: int
    ) -> tuple[float, int, int] | None:
        """
        Send one request.

        Returns ``(latency_seconds, prompt_tokens, generated_tokens)`` using the
        backend's own counts where it reports them, or ``None`` on error (the
        reason is kept in :attr:`last_error`).
        """
        payload = {
            "model": self._client.model,
            "messages": [{"role": "user", "content": "x " * prompt_tokens}],
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }
        t0 = time.perf_counter()
        chunks = 0
        usage: dict | None = None
        try:
            async for tok in self._client.chat(payload):
                if tok.text:
                    chunks += 1
                if tok.usage is not None:
                    usage = tok.usage
        except Exception as exc:  # noqa: BLE001
            self.last_error = str(exc)[:300]
            if _is_timeout(exc):
                self._timed_out += 1
            return None
        latency = time.perf_counter() - t0
        prompt = prompt_tokens
        generated = chunks
        if usage:
            if isinstance(usage.get("prompt_tokens"), int) and usage["prompt_tokens"] > 0:
                prompt = usage["prompt_tokens"]
            if isinstance(usage.get("completion_tokens"), int):
                generated = usage["completion_tokens"]
        return latency, prompt, generated

    async def _measure(
        self,
        concurrency: int,
        shapes: list[tuple[int, int]],
        trials: int,
    ) -> SweepPoint:
        """
        Run one point of the sweep, repeated ``trials`` times.

        Request shapes are cycled rather than held uniform: a real multi-agent
        workload mixes sizes, and a budget measured only on identical requests
        would not describe it. Latencies from every trial are pooled before the
        percentile is taken, so a single unlucky request cannot define the knee.
        """
        latencies: list[float] = []
        per_token: list[float] = []
        errors = 0
        completed_tokens = 0
        wall = 0.0
        inflight_tokens = 0
        timed_out_before = self._timed_out
        for _ in range(max(1, trials)):
            batch = [shapes[i % len(shapes)] for i in range(concurrency)]
            inflight_tokens = sum(p + m for p, m in batch)
            started = time.perf_counter()
            results = await asyncio.gather(
                *[self._probe_once(prompt, max_tokens) for prompt, max_tokens in batch]
            )
            wall += time.perf_counter() - started
            for result in results:
                if result is None:
                    errors += 1
                    continue
                latency, prompt, generated = result
                # Generated tokens only — see the note on `tokens_per_second`.
                completed_tokens += generated
                latencies.append(latency)
                # Same basis as live drift samples: whole-request latency over
                # the tokens actually processed (prompt + generated).
                per_token.append(latency / max(1, prompt + generated))
        return SweepPoint(
            concurrency=concurrency,
            inflight_tokens=inflight_tokens,
            p99_seconds=_percentile(latencies, 0.99),
            errors=errors,
            p99_per_token=_percentile(per_token, 0.99),
            p50_per_token=_percentile(per_token, 0.50),
            tokens_per_second=(completed_tokens / wall) if wall > 0 else 0.0,
            timeouts=min(errors, self._timed_out - timed_out_before),
        )

    async def calibrate(
        self,
        *,
        probe_prompt_tokens: int = 256,
        probe_max_tokens: int = 64,
        start: int = 1,
        max_concurrency: int = 32,
        latency_ratio: float = 2.0,
        safety_margin: float = 0.85,
        trials: int = 3,
        warmup: int = 1,
        refine: bool = True,
    ) -> int:
        """
        Measure the KV-token budget ``B`` against a live Foundry Local.

        Three passes:

        1. **Warm-up.** The first request to a freshly loaded model pays for
           graph initialisation and page-ins. Including it in the baseline makes
           every later point look fast by comparison, which moves the knee.
        2. **Doubling.** Concurrency doubles until the knee, which finds the
           right order of magnitude in a logarithmic number of steps.
        3. **Bisection.** Doubling alone resolves the knee only to the previous
           power of two, discarding up to half the machine's capacity. This
           narrows the gap between the last healthy point and the first
           unhealthy one.
        """
        shapes = _probe_shapes(probe_prompt_tokens, probe_max_tokens)
        smallest = min(p + m for p, m in shapes)

        for _ in range(max(0, warmup)):
            await self._probe_once(probe_prompt_tokens, probe_max_tokens)

        points: list[SweepPoint] = []
        concurrency = max(1, start)
        first_bad: SweepPoint | None = None
        while concurrency <= max_concurrency:
            point = await self._measure(concurrency, shapes, trials)
            _log_point(point)
            points.append(point)
            baseline = points[0].signal or 1e-9
            healthy = point.errors == 0 and point.signal <= baseline * latency_ratio
            if not healthy:
                first_bad = point
                break
            concurrency *= 2

        knee = find_knee(points, latency_ratio)
        if refine and knee is not None and first_bad is not None:
            refined = await self._bisect(
                knee, first_bad, shapes, trials, latency_ratio, baseline=points[0].signal
            )
            if refined is not None:
                points.append(refined)
                knee = refined

        if knee is None:
            # Even a single request failed, so nothing was measured. Saving a
            # budget here — as this used to, reporting success — would put a
            # number that describes nothing into token-mode admission.
            first = points[0] if points else None
            failed = f"{first.errors} probe request(s) failed" if first else "no probes ran"
            raise CalibrationFailed(
                f"calibration measured nothing: {failed} at concurrency {start}"
                + (f" ({self.last_error})" if self.last_error else "")
                + ". Check that the model is loaded and Foundry Local is serving; "
                "nothing was saved."
            )
        budget = max(smallest, int(knee.inflight_tokens * safety_margin))
        self.baseline_per_token = knee.p50_per_token

        # Classify what kind of runtime this was, not just how much of it there
        # is. The same budget means very different things on a batching engine
        # and a serialising one, and an operator needs to know which they have.
        self.profile = classify_runtime(points)
        logger.info(
            "runtime regime: %s (optimal concurrency %d) — %s",
            self.profile.regime,
            self.profile.optimal_concurrency,
            self.profile.detail,
        )
        if not self.profile.admission_raises_throughput:
            logger.warning("%s", self.profile.advice)

        self.save(budget, points)
        return budget

    async def _bisect(
        self,
        good: SweepPoint,
        bad: SweepPoint,
        shapes: list[tuple[int, int]],
        trials: int,
        latency_ratio: float,
        *,
        baseline: float,
        max_steps: int = 3,
    ) -> SweepPoint | None:
        """
        Narrow the gap between the last healthy point and the first unhealthy one.

        Returns the highest concurrency still measured healthy, or ``None`` if
        the two points are already adjacent and there is nothing to refine.

        ``baseline`` is the sweep's *first* point, the same reference the
        doubling pass and :func:`find_knee` use. Measuring against the last
        healthy point instead compounded the tolerance — a point could be
        accepted at up to ``latency_ratio`` squared times the baseline — and
        the budget was then computed from it.
        """
        baseline = baseline or 1e-9
        best = good
        low, high = good.concurrency, bad.concurrency
        for _ in range(max_steps):
            if high - low <= 1:
                break
            middle = (low + high) // 2
            point = await self._measure(middle, shapes, trials)
            _log_point(point, prefix="refine")
            healthy = point.errors == 0 and point.signal <= baseline * latency_ratio
            if healthy:
                best, low = point, middle
            else:
                high = middle
        return best if best is not good else None

    # -- persistence ---------------------------------------------------

    def save(self, budget_tokens: int, points: list[SweepPoint] | None = None) -> None:
        extra = {
            "base_url": self._client.base_url,
            "runtime_profile": self.profile.as_dict(),
            # The drift baseline: median seconds per actually-processed token
            # at the knee, i.e. at the load the budget admits. Drift samples
            # are taken on exactly this basis (`DriftMonitor.observe`).
            "baseline_seconds_per_token": round(self.baseline_per_token, 6),
            "sweep": [
                {
                    "concurrency": p.concurrency,
                    "inflight_tokens": p.inflight_tokens,
                    "p99_seconds": round(p.p99_seconds, 4),
                    "p99_per_token": round(p.p99_per_token, 6),
                    "p50_per_token": round(p.p50_per_token, 6),
                    "tokens_per_second": round(p.tokens_per_second, 1),
                    "errors": p.errors,
                    "timeouts": p.timeouts,
                }
                for p in (points or [])
            ],
        }
        save_budget(self._store_path, self._key, budget_tokens, extra)


def _is_timeout(exc: BaseException) -> bool:
    """Whether a failed probe ran out of time, rather than being refused."""
    if isinstance(exc, TimeoutError | asyncio.TimeoutError | httpx.TimeoutException):
        return True
    return isinstance(exc.__cause__, httpx.TimeoutException)
