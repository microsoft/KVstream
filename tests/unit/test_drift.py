"""
G-53: is the backend still the machine we calibrated?

A budget is measured once and then trusted. Measured against a live
`foundry 0.8.119`, capacity fell to zero across a single session as the
inference process grew to 13.4 GB, while the gateway kept admitting against a
number taken when the runtime was fresh. Nothing noticed, because nothing was
watching.
"""

from __future__ import annotations

from kvstream.admission.drift import DEGRADED, OK, UNKNOWN, DriftMonitor, baseline_from_provenance


def _monitor(baseline: float = 0.001, **kwargs) -> DriftMonitor:
    kwargs.setdefault("min_samples", 5)
    kwargs.setdefault("warn_ratio", 3.0)
    return DriftMonitor(baseline, **kwargs)


def test_without_a_baseline_it_stays_quiet():
    """No calibration record means no opinion — never an invented comparison."""
    m = DriftMonitor(0.0, min_samples=1)
    for _ in range(10):
        m.observe(10.0, 100)
    assert m.state == UNKNOWN
    assert m.ratio == 0.0


def test_it_waits_for_enough_traffic():
    m = _monitor(min_samples=10)
    for _ in range(3):
        m.observe(1.0, 100)
    assert m.state == UNKNOWN


def test_a_healthy_backend_reads_ok():
    m = _monitor(baseline=0.01)
    for _ in range(20):
        m.observe(1.0, 100)  # 0.01 s/token — exactly the baseline
    assert m.state == OK
    assert 0.9 < m.ratio < 1.1


def test_a_degrading_backend_is_flagged():
    m = _monitor(baseline=0.01)
    for _ in range(40):
        m.observe(5.0, 100)  # 0.05 s/token — five times slower
    assert m.state == DEGRADED
    assert m.ratio > 3.0


def test_recovery_clears_the_flag():
    m = _monitor(baseline=0.01, alpha=0.5)
    for _ in range(40):
        m.observe(5.0, 100)
    assert m.state == DEGRADED
    for _ in range(40):
        m.observe(1.0, 100)
    assert m.state == OK


def test_the_worst_ratio_is_remembered():
    """An incident that has recovered still has to be visible afterwards."""
    m = _monitor(baseline=0.01, alpha=0.5)
    for _ in range(30):
        m.observe(8.0, 100)
    for _ in range(30):
        m.observe(1.0, 100)
    assert m.stats()["worst_ratio"] > 3.0


def test_request_size_does_not_read_as_drift():
    """
    Normalisation is the whole point.

    Raw latency rises with request size, so a traffic-mix shift would look like
    a failing backend — the same mistake the calibration sweep had to be fixed
    for.
    """
    m = _monitor(baseline=0.01)
    for _ in range(10):
        m.observe(1.0, 100)  # small requests
    for _ in range(10):
        m.observe(20.0, 2000)  # large requests, same rate per token
    assert m.state == OK


def test_nonsense_samples_are_ignored():
    m = _monitor()
    m.observe(0.0, 100)
    m.observe(1.0, 0)
    m.observe(-1.0, 100)
    assert m.stats()["samples"] == 0


# -- recovering the baseline from a stored calibration -----------------


def test_baseline_comes_from_the_stored_sweep():
    provenance = {
        "source": "calibration:exact",
        "lookup": {
            "record": {
                "budget_tokens": 688,
                "sweep": [
                    {"concurrency": 1, "p99_per_token": 0.0006, "errors": 0},
                    {"concurrency": 2, "p99_per_token": 0.0009, "errors": 0},
                ],
            }
        },
    }
    assert baseline_from_provenance(provenance) == 0.0009


def test_unhealthy_sweep_points_are_excluded():
    """The baseline is what healthy looked like, not what failure looked like."""
    provenance = {
        "lookup": {
            "record": {
                "sweep": [
                    {"concurrency": 1, "p99_per_token": 0.0006, "errors": 0},
                    {"concurrency": 8, "p99_per_token": 0.5, "errors": 4},
                ]
            }
        }
    }
    assert baseline_from_provenance(provenance) == 0.0006


def test_a_configured_budget_has_no_baseline():
    """Nothing was measured, so there is nothing to compare against."""
    assert baseline_from_provenance({"source": "configured"}) == 0.0
    assert baseline_from_provenance({}) == 0.0
    assert baseline_from_provenance({"lookup": {"record": {}}}) == 0.0
    assert baseline_from_provenance({"lookup": {"record": {"sweep": []}}}) == 0.0


def test_a_v1_record_without_per_token_data_has_no_baseline():
    """Old sweeps predate the normalised signal; they cannot supply one."""
    provenance = {
        "lookup": {"record": {"sweep": [{"concurrency": 1, "p99_seconds": 0.4, "errors": 0}]}}
    }
    assert baseline_from_provenance(provenance) == 0.0
