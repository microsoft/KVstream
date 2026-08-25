"""
Runtime regime classification — what kind of backend did we just measure?

A calibration sweep produces a number. That number is only actionable if you
also know *what shape of runtime produced it*, because the same gateway is worth
very different things against different hardware:

* A runtime that **batches** gets faster in aggregate as concurrency rises, up
  to a point, and then degrades. Admission control protects that point, and the
  token budget earns its keep. This is the case the proposal assumes.
* A runtime that **serialises** does not. Throughput is flat from one concurrent
  request to sixteen; latency simply grows linearly as work queues inside the
  engine. Admission control **cannot raise throughput here** — there is no
  knee to protect. Its value is backpressure and shedding, and the levers that
  actually help are caching and coalescing, because they remove work entirely.
* A runtime that is **hard-capped** refuses outright past a threshold.
  Admission control converts those refusals into queueing, which is the largest
  measurable win of the three.

Measured on `foundry 0.8.119` with `phi-3-mini-4k` on a machine whose Windows
build was too old to register the ML execution providers: **serialising**,
generated-token throughput flat within 1% from concurrency 1 to 8. On hardware
where the execution providers do register, the answer may well be different —
which is exactly why this is measured per machine rather than asserted once in a
document.

The classifier reads the sweep the calibration already performs. It adds no
probing of its own. The one thing it is fussy about is the unit: throughput has
to be counted in *generated* tokens per second, because prefill is far cheaper
per token than decode and any metric that mixes them invents a throughput gain
out of a shifting request mix.
"""

from __future__ import annotations

from dataclasses import dataclass

SERIALISING = "serialising"
BATCHING = "batching"
CAPPED = "capped"
UNKNOWN = "unknown"

# Throughput has to rise by more than this from one concurrent request to the
# peak before the runtime is called a batching one. Below it, the rise is noise
# or scheduling jitter rather than parallelism.
BATCHING_GAIN = 1.25


@dataclass(frozen=True)
class RuntimeProfile:
    """What the sweep says about the backend's shape, not just its size."""

    regime: str = UNKNOWN
    optimal_concurrency: int = 1
    peak_tokens_per_second: float = 0.0
    throughput_gain: float = 1.0
    refuses_beyond: int | None = None
    detail: str = "not enough sweep points to classify"

    @property
    def admission_raises_throughput(self) -> bool:
        """Whether gating this runtime can increase aggregate throughput at all."""
        return self.regime in (BATCHING, CAPPED)

    @property
    def advice(self) -> str:
        """What an operator should expect the gateway to be worth here."""
        if self.regime == SERIALISING:
            return (
                "This runtime does not batch: throughput was flat across the sweep, so "
                "admission control cannot raise it. What the gateway buys here is "
                "bounded backpressure instead of unbounded queueing, plus readiness "
                "and shedding. The levers that actually add capacity are the response "
                "cache and request coalescing, because they remove work rather than "
                "reorder it."
            )
        if self.regime == BATCHING:
            return (
                f"This runtime batches: throughput peaked at concurrency "
                f"{self.optimal_concurrency}, {self.throughput_gain:.1f}x the "
                "single-request rate. Admission control protects that peak, and the "
                "token budget is worth using for mixed request sizes."
            )
        if self.regime == CAPPED:
            return (
                f"This runtime refuses work beyond concurrency {self.refuses_beyond}. "
                "Admission control converts those refusals into queueing, which is the "
                "largest measurable win the gateway offers."
            )
        return (
            "Not enough of the sweep completed to characterise this runtime. Re-run "
            "calibration with a higher --max-concurrency, or against a warm backend."
        )

    def as_dict(self) -> dict:
        return {
            "regime": self.regime,
            "optimal_concurrency": self.optimal_concurrency,
            "peak_tokens_per_second": round(self.peak_tokens_per_second, 1),
            "throughput_gain": round(self.throughput_gain, 3),
            "refuses_beyond": self.refuses_beyond,
            "admission_raises_throughput": self.admission_raises_throughput,
            "detail": self.detail,
        }


def classify_runtime(points) -> RuntimeProfile:
    """
    Classify a completed sweep.

    ``points`` is the list of :class:`~kvstream.admission.calibration.SweepPoint`
    the sweep produced, in ascending concurrency. Anything that cannot be told
    apart is reported as ``unknown`` rather than guessed — a wrong regime label
    would send an operator after the wrong lever.
    """
    # Refinement appends a bisected point out of order, so sort before reading
    # "first" and "last" as low and high concurrency.
    ordered = sorted(points, key=lambda p: p.concurrency)
    usable = [p for p in ordered if p.tokens_per_second > 0 and not p.errors]
    refuses_beyond = None
    for point in ordered:
        if point.errors:
            refuses_beyond = point.concurrency
            break

    if len(usable) < 2:
        # One healthy point tells us nothing about how the runtime scales — but
        # if the very next level errored, the cap itself is the finding.
        if refuses_beyond is not None and usable:
            return RuntimeProfile(
                regime=CAPPED,
                optimal_concurrency=usable[-1].concurrency,
                peak_tokens_per_second=usable[-1].tokens_per_second,
                refuses_beyond=refuses_beyond,
                detail=f"errors first appeared at concurrency {refuses_beyond}",
            )
        return RuntimeProfile()

    baseline = usable[0].tokens_per_second
    peak = max(usable, key=lambda p: p.tokens_per_second)
    gain = peak.tokens_per_second / baseline if baseline > 0 else 1.0

    if gain >= BATCHING_GAIN:
        regime = BATCHING
        detail = (
            f"work completed per second rose {gain:.2f}x from concurrency "
            f"{usable[0].concurrency} "
            f"to {peak.concurrency}"
        )
        optimal = peak.concurrency
    else:
        regime = SERIALISING
        detail = (
            f"work completed per second stayed within {abs(gain - 1) * 100:.0f}% of "
            f"the single-request rate across concurrency "
            f"{usable[0].concurrency}–{usable[-1].concurrency}"
        )
        optimal = 1

    # A cap is a fact about the runtime regardless of how it scales below it,
    # and it is the more actionable of the two, so it wins the label.
    if refuses_beyond is not None:
        return RuntimeProfile(
            regime=CAPPED,
            optimal_concurrency=optimal,
            peak_tokens_per_second=peak.tokens_per_second,
            throughput_gain=gain,
            refuses_beyond=refuses_beyond,
            detail=f"{detail}; errors first appeared at concurrency {refuses_beyond}",
        )

    return RuntimeProfile(
        regime=regime,
        optimal_concurrency=optimal,
        peak_tokens_per_second=peak.tokens_per_second,
        throughput_gain=gain,
        detail=detail,
    )
