"""
Metrics — real Prometheus collectors, updated on the request path.

Unlike a placeholder ``/metrics`` endpoint, every series here is registered and
actively updated, so scraping reflects true gateway behaviour (admission,
queueing, cache, coalescing, latency).
"""

from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()

        self.requests = Counter(
            "kvstream_requests_total",
            "Requests by proxied route and outcome.",
            # route: chat | embeddings | transcriptions
            # outcome: served | cache_hit | coalesced | rejected | error
            ["route", "outcome"],
            registry=self.registry,
        )
        self.rejected = Counter(
            "kvstream_rejected_total",
            "Rejected requests by route and reason.",
            ["route", "reason"],  # queue_full | timeout | too_large
            registry=self.registry,
        )
        self.cache_hits = Counter(
            "kvstream_cache_hits_total", "Response-cache hits.", registry=self.registry
        )
        self.cache_skipped = Counter(
            "kvstream_cache_skipped_total",
            "Responses not cached, by reason.",
            ["reason"],  # too_large
            registry=self.registry,
        )
        self.coalesced = Counter(
            "kvstream_coalesced_total",
            "Requests served by a coalesced leader.",
            registry=self.registry,
        )
        self.latency = Histogram(
            "kvstream_request_seconds",
            "End-to-end request latency (seconds) by route.",
            ["route"],
            registry=self.registry,
        )
        self.admission_wait = Histogram(
            "kvstream_admission_wait_seconds",
            "Time spent waiting for admission, by route.",
            ["route"],
            registry=self.registry,
        )
        self.backend_errors = Counter(
            "kvstream_backend_errors_total",
            "Backend failures by phase.",
            ["phase"],  # preflight | nonstreaming | midstream
            registry=self.registry,
        )
        self.overshoot = Counter(
            "kvstream_reservation_overshoot_total",
            "Times a generation outgrew its admitted reservation and was topped up. "
            "Always zero while admission.reserve_completion_ratio is 1.0.",
            registry=self.registry,
        )
        self.reclaimed = Counter(
            "kvstream_reservation_reclaimed_tokens_total",
            "Budget returned early by live accounting, before request teardown.",
            registry=self.registry,
        )

        self.budget = Gauge(
            "kvstream_budget",
            "Admission budget (tokens or slots).",
            registry=self.registry,
        )
        self.in_flight = Gauge(
            "kvstream_in_flight",
            "Reserved budget currently in flight.",
            registry=self.registry,
        )
        self.utilization = Gauge(
            "kvstream_budget_utilization", "in_flight / budget.", registry=self.registry
        )
        self.queue_depth = Gauge(
            "kvstream_queue_depth",
            "Requests waiting for admission.",
            registry=self.registry,
        )
        self.chars_per_token = Gauge(
            "kvstream_chars_per_token",
            "Current characters-per-token ratio used for cost estimation.",
            registry=self.registry,
        )
        self.token_ratio_samples = Gauge(
            "kvstream_token_ratio_samples",
            "Number of real usage reports the estimator has learned from.",
            registry=self.registry,
        )
        self.active_requests = Gauge(
            "kvstream_active_requests",
            "Requests currently holding a reservation.",
            registry=self.registry,
        )
        self.discovery_scans = Gauge(
            "kvstream_discovery_scans",
            "Full localhost discovery sweeps performed since start.",
            registry=self.registry,
        )
        self.calibration_age = Gauge(
            "kvstream_calibration_age_seconds",
            "Age of the calibrated budget in use; 0 when none is in use.",
            registry=self.registry,
        )
        self.circuit_state = Gauge(
            "kvstream_circuit_breaker_state",
            "0 = closed, 1 = half-open, 2 = open.",
            registry=self.registry,
        )
        self.backend_ready = Gauge(
            "kvstream_backend_ready",
            "1 when the backend completed a bounded trial generation. Distinct from "
            "kvstream_backend_up, which only means it answered /v1/models.",
            registry=self.registry,
        )
        self.drift_ratio = Gauge(
            "kvstream_backend_drift_ratio",
            "Served seconds-per-token relative to the calibration baseline; 0 when unknown.",
            registry=self.registry,
        )
        self.backend_up = Gauge(
            "kvstream_backend_up",
            "1 when the last backend health probe succeeded, 0 otherwise.",
            registry=self.registry,
        )
        # Audio is admitted on its own plain concurrency limit: a sound file has
        # no token cost measurable at the gateway, and inventing one would be
        # worse than saying so.
        self.audio_budget = Gauge(
            "kvstream_audio_budget",
            "Concurrent transcription slots.",
            registry=self.registry,
        )
        self.audio_in_flight = Gauge(
            "kvstream_audio_in_flight",
            "Transcriptions currently in flight.",
            registry=self.registry,
        )
        self.audio_queue_depth = Gauge(
            "kvstream_audio_queue_depth",
            "Transcriptions waiting for a slot.",
            registry=self.registry,
        )

    def sync_gauges(
        self,
        capacity_stats: dict,
        estimator_stats: dict | None = None,
        backend_stats: dict | None = None,
        audio_stats: dict | None = None,
        calibration_age: float | None = None,
    ) -> None:
        """Refresh point-in-time gauges from component snapshots."""
        self.budget.set(capacity_stats.get("budget", 0))
        self.in_flight.set(capacity_stats.get("in_flight", 0))
        self.utilization.set(capacity_stats.get("utilization", 0.0))
        self.queue_depth.set(capacity_stats.get("waiting", 0))
        self.active_requests.set(capacity_stats.get("active", 0))
        if audio_stats:
            self.audio_budget.set(audio_stats.get("budget", 0))
            self.audio_in_flight.set(audio_stats.get("in_flight", 0))
            self.audio_queue_depth.set(audio_stats.get("waiting", 0))
        if estimator_stats:
            self.chars_per_token.set(estimator_stats.get("chars_per_token", 0.0))
            self.token_ratio_samples.set(estimator_stats.get("samples", 0))
        if backend_stats:
            self.discovery_scans.set(backend_stats.get("scans", 0))
        if calibration_age is not None:
            self.calibration_age.set(calibration_age)

    def render(self) -> bytes:
        return generate_latest(self.registry)
