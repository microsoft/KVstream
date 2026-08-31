"""
Regime classification: what kind of runtime did the sweep just measure?

This exists because the project's one hardware measurement contradicted the
assumption its own proposal was built on. `foundry 0.8.119` on that box did not
batch — throughput was flat from concurrency 1 to 8 — so admission control could
not raise throughput there no matter how well it was tuned.

Asserting one runtime shape in a document does not survive contact with other
people's hardware. Measuring it per machine does.
"""

from __future__ import annotations

from kvstream.admission.calibration import SweepPoint
from kvstream.admission.regime import (
    BATCHING,
    CAPPED,
    SERIALISING,
    UNKNOWN,
    classify_runtime,
)


def _point(concurrency: int, rps: float, errors: int = 0) -> SweepPoint:
    return SweepPoint(
        concurrency=concurrency,
        inflight_tokens=concurrency * 100,
        p99_seconds=concurrency * 0.1,
        errors=errors,
        p99_per_token=0.001,
        tokens_per_second=rps,
    )


# -- the three shapes --------------------------------------------------


def test_a_serialising_runtime_is_recognised():
    """The measured case: throughput flat at ~2.6 r/s from concurrency 1 to 8."""
    profile = classify_runtime(
        [
            _point(1, 2.61),
            _point(2, 2.60),
            _point(4, 2.58),
            _point(8, 2.57),
        ]
    )
    assert profile.regime == SERIALISING
    assert profile.optimal_concurrency == 1
    assert profile.admission_raises_throughput is False
    assert "cache and request coalescing" in profile.advice


def test_a_batching_runtime_is_recognised():
    """Throughput climbs with concurrency, then flattens — the assumed case."""
    profile = classify_runtime(
        [
            _point(1, 2.0),
            _point(2, 3.6),
            _point(4, 6.1),
            _point(8, 6.4),
        ]
    )
    assert profile.regime == BATCHING
    assert profile.optimal_concurrency == 8
    assert profile.admission_raises_throughput is True
    assert round(profile.throughput_gain, 1) == 3.2


def test_a_capped_runtime_is_recognised():
    profile = classify_runtime(
        [
            _point(1, 2.0),
            _point(2, 2.1),
            _point(4, 2.0),
            _point(8, 0.0, errors=8),
        ]
    )
    assert profile.regime == CAPPED
    assert profile.refuses_beyond == 8
    assert profile.admission_raises_throughput is True
    assert "converts those refusals into queueing" in profile.advice


def test_a_cap_wins_the_label_over_how_it_scales_below():
    """The refusal threshold is the more actionable fact, so it is the headline."""
    profile = classify_runtime(
        [
            _point(1, 2.0),
            _point(2, 3.8),
            _point(4, 7.0),
            _point(8, 0.0, errors=8),
        ]
    )
    assert profile.regime == CAPPED
    assert profile.refuses_beyond == 8
    assert profile.optimal_concurrency == 4  # the batching peak is still reported
    assert profile.throughput_gain > 3.0


# -- refusing to guess -------------------------------------------------


def test_a_single_point_is_not_classified():
    """One measurement says nothing about how a runtime scales."""
    profile = classify_runtime([_point(1, 2.0)])
    assert profile.regime == UNKNOWN
    assert profile.admission_raises_throughput is False
    assert "Re-run calibration" in profile.advice


def test_an_empty_sweep_is_not_classified():
    assert classify_runtime([]).regime == UNKNOWN


def test_one_good_point_then_errors_is_still_a_cap():
    """A backend that fails at concurrency 2 has told us its limit."""
    profile = classify_runtime([_point(1, 2.0), _point(2, 0.0, errors=2)])
    assert profile.regime == CAPPED
    assert profile.refuses_beyond == 2
    assert profile.optimal_concurrency == 1


def test_a_marginal_rise_is_not_called_batching():
    """Scheduling jitter must not be mistaken for parallelism."""
    profile = classify_runtime([_point(1, 2.00), _point(2, 2.20), _point(4, 2.15)])
    assert profile.regime == SERIALISING


def test_the_threshold_is_where_it_says_it_is():
    below = classify_runtime([_point(1, 2.0), _point(2, 2.0 * 1.24)])
    above = classify_runtime([_point(1, 2.0), _point(2, 2.0 * 1.26)])
    assert below.regime == SERIALISING
    assert above.regime == BATCHING


def test_the_profile_serialises_for_status_and_storage():
    profile = classify_runtime([_point(1, 2.0), _point(2, 4.0)])
    payload = profile.as_dict()
    assert payload["regime"] == BATCHING
    assert payload["admission_raises_throughput"] is True
    assert set(payload) == {
        "regime",
        "optimal_concurrency",
        "peak_tokens_per_second",
        "throughput_gain",
        "refuses_beyond",
        "admission_raises_throughput",
        "detail",
    }
