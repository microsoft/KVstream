"""
Drift detection — is the backend still the machine we calibrated?

A calibrated budget is measured once and then trusted. That is fine if the
runtime's capacity is a constant, and it is not. Measured against a live
``foundry 0.8.119``: capacity fell to zero over a single session as the
inference process grew to 13.4 GB resident, while the gateway went on admitting
against a budget measured when the runtime was fresh. Nothing noticed.

What can be watched from the HTTP layer is **service time per generated token**,
the same normalised signal the calibration sweep uses to find the knee
(Appendix B §B.3.3). Comparing live traffic against the baseline recorded at
calibration time answers the only question that matters: *is the backend still
behaving like the one the budget was measured on?*

This is deliberately a **signal, not a control loop**. It warns, it is exposed
in `/status` and metrics, and it does not silently re-tune the budget. A gateway
that quietly moves its own limits in response to a degrading backend is much
harder to reason about during an incident than one that says "this is 4x slower
than when you calibrated it, go and look".
"""

from __future__ import annotations

import logging

logger = logging.getLogger("kvstream.drift")

UNKNOWN = "unknown"  # no baseline, or not enough traffic yet
OK = "ok"
DEGRADED = "degraded"


class DriftMonitor:
    """Tracks served latency per token against the calibration baseline."""

    def __init__(
        self,
        baseline_per_token: float = 0.0,
        *,
        warn_ratio: float = 3.0,
        min_samples: int = 20,
        alpha: float = 0.1,
    ) -> None:
        self._baseline = max(0.0, baseline_per_token)
        self._warn_ratio = max(1.0, warn_ratio)
        self._min_samples = max(1, min_samples)
        self._alpha = alpha

        self._ewma = 0.0
        self._samples = 0
        self._state = UNKNOWN
        self._worst_ratio = 0.0

    @property
    def baseline_per_token(self) -> float:
        return self._baseline

    @property
    def state(self) -> str:
        return self._state

    @property
    def ratio(self) -> float:
        """Observed service time per token, relative to calibration. 0 = unknown."""
        if not self._baseline or not self._ewma or self._samples < self._min_samples:
            return 0.0
        return self._ewma / self._baseline

    def observe(self, latency_seconds: float, tokens: int) -> None:
        """
        Record one completed request.

        ``tokens`` is the request's own cost basis — prompt plus generated — so
        the measurement is comparable across request sizes. Without that
        normalisation a shift in traffic mix reads as a shift in the backend,
        which is the same mistake the calibration sweep had to be fixed for.
        """
        if latency_seconds <= 0 or tokens <= 0:
            return
        per_token = latency_seconds / tokens
        self._ewma = (
            per_token
            if self._samples == 0
            else (1 - self._alpha) * self._ewma + self._alpha * per_token
        )
        self._samples += 1
        self._evaluate()

    def _evaluate(self) -> None:
        ratio = self.ratio
        if not ratio:
            self._state = UNKNOWN
            return
        self._worst_ratio = max(self._worst_ratio, ratio)
        previous = self._state
        self._state = DEGRADED if ratio >= self._warn_ratio else OK
        if self._state == DEGRADED and previous != DEGRADED:
            logger.warning(
                "backend drift: serving %.1fx slower per token than at calibration "
                "(%.5fs vs %.5fs). The admission budget was measured on a backend that "
                "no longer behaves this way — re-run `kvstream calibrate`, and check "
                "whether the runtime needs restarting.",
                ratio,
                self._ewma,
                self._baseline,
            )
        elif self._state == OK and previous == DEGRADED:
            logger.info("backend drift cleared: %.1fx of the calibration baseline", ratio)

    def stats(self) -> dict:
        return {
            "state": self._state,
            "baseline_seconds_per_token": round(self._baseline, 6),
            "observed_seconds_per_token": round(self._ewma, 6),
            "ratio": round(self.ratio, 3),
            "worst_ratio": round(self._worst_ratio, 3),
            "samples": self._samples,
            "warn_ratio": self._warn_ratio,
        }


def baseline_from_provenance(provenance: dict) -> float:
    """
    Recover the calibration-time service rate from a stored budget record.

    The sweep keeps its evidence alongside the number (Appendix B §B.6), so the
    baseline is the per-token rate of the last healthy point. Returns 0.0 when
    the budget did not come from calibration, which leaves drift detection
    inactive rather than comparing against something invented.
    """
    lookup = provenance.get("lookup")
    if not isinstance(lookup, dict):
        return 0.0
    record = lookup.get("record")
    if not isinstance(record, dict):
        return 0.0
    sweep = record.get("sweep")
    if not isinstance(sweep, list) or not sweep:
        return 0.0
    rates = [
        float(point["p99_per_token"])
        for point in sweep
        if isinstance(point, dict)
        and isinstance(point.get("p99_per_token"), int | float)
        and point["p99_per_token"] > 0
        and not point.get("errors")
    ]
    return max(rates) if rates else 0.0
