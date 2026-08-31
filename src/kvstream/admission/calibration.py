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
import json
import logging
import os
import platform
import time
from dataclasses import dataclass

from kvstream.admission.regime import RuntimeProfile, classify_runtime
from kvstream.backend.foundry import FoundryClient

logger = logging.getLogger("kvstream.calibration")

STORE_VERSION = 2
UNKNOWN = "unknown"


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
    # Completed *generated* tokens per second — the signal that tells one kind of
    # runtime from another, and it took three attempts to get the unit right.
    #
    # Requests per second moves with the probe's shape mix as much as with the
    # runtime, so a batching engine reads as flat. Prompt-plus-generated tokens
    # per second is worse: prefill is far cheaper per token than decode, so as
    # the mix tilts prompt-heavy at higher concurrency it manufactures a gain
    # that is not there — on the reference machine it turned a runtime measured
    # as flat into a false 1.43x "batching" reading. Only the generated half is
    # a consistent unit of work across sweep levels.
    tokens_per_second: float = 0.0

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
            "budget_tokens": int(raw.get("budget_tokens", 0)),
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
    os.makedirs(os.path.dirname(store_path) or ".", exist_ok=True)
    with open(store_path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, sort_keys=True)
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

    exact = entries.get(key.as_str())
    if isinstance(exact, dict) and int(exact.get("budget_tokens", 0)) > 0:
        return CalibrationLookup(int(exact["budget_tokens"]), "exact", exact)

    for record in entries.values():
        if not isinstance(record, dict):
            continue
        if record.get("model") != key.model:
            continue
        if record.get("device") != key.device:
            continue
        budget = int(record.get("budget_tokens", 0))
        if budget > 0:
            return CalibrationLookup(
                budget,
                "partial",
                record,
                detail=(
                    f"measured against Foundry {record.get('backend_version', UNKNOWN)} "
                    f"/ CLI {record.get('cli_generation', UNKNOWN)}, now running "
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
        self._client = client
        self._store_path = store_path
        self._key = key or CalibrationKey(model=client.model)
        self.profile = RuntimeProfile()

    @property
    def key(self) -> CalibrationKey:
        return self._key

    async def _probe_once(self, prompt_tokens: int, max_tokens: int) -> float | None:
        """Send one request; return latency seconds, or ``None`` on error."""
        payload = {
            "model": self._client.model,
            "messages": [{"role": "user", "content": "x " * prompt_tokens}],
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }
        t0 = time.perf_counter()
        try:
            async for _ in self._client.chat(payload):
                pass
            return time.perf_counter() - t0
        except Exception:  # noqa: BLE001
            return None

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
        for _ in range(max(1, trials)):
            batch = [shapes[i % len(shapes)] for i in range(concurrency)]
            inflight_tokens = sum(p + m for p, m in batch)
            started = time.perf_counter()
            results = await asyncio.gather(
                *[self._probe_once(prompt, max_tokens) for prompt, max_tokens in batch]
            )
            wall += time.perf_counter() - started
            for (prompt, max_tokens), result in zip(batch, results, strict=True):
                if result is None:
                    errors += 1
                    continue
                # Generated tokens only — see the note on `tokens_per_second`.
                completed_tokens += max_tokens
                latencies.append(result)
                per_token.append(result / max(1, prompt + max_tokens))
        return SweepPoint(
            concurrency=concurrency,
            inflight_tokens=inflight_tokens,
            p99_seconds=_percentile(latencies, 0.99),
            errors=errors,
            p99_per_token=_percentile(per_token, 0.99),
            tokens_per_second=(completed_tokens / wall) if wall > 0 else 0.0,
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
            refined = await self._bisect(knee, first_bad, shapes, trials, latency_ratio)
            if refined is not None:
                points.append(refined)
                knee = refined

        if knee is None:
            # Even the smallest load failed; fall back to a single request.
            budget = smallest
        else:
            budget = int(knee.inflight_tokens * safety_margin)
        budget = max(smallest, budget)

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
        max_steps: int = 3,
    ) -> SweepPoint | None:
        """
        Narrow the gap between the last healthy point and the first unhealthy one.

        Returns the highest concurrency still measured healthy, or ``None`` if
        the two points are already adjacent and there is nothing to refine.
        """
        baseline = good.signal or 1e-9
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
            "sweep": [
                {
                    "concurrency": p.concurrency,
                    "inflight_tokens": p.inflight_tokens,
                    "p99_seconds": round(p.p99_seconds, 4),
                    "p99_per_token": round(p.p99_per_token, 6),
                    "tokens_per_second": round(p.tokens_per_second, 1),
                    "errors": p.errors,
                }
                for p in (points or [])
            ],
        }
        save_budget(self._store_path, self._key, budget_tokens, extra)
