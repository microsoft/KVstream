"""
Configuration for the KVStream Foundry Local gateway.

Priority (highest to lowest):
  1. Constructor / CLI overrides
  2. Environment variables (KVSTREAM_*, nested with `__`)
  3. kvstream.yaml in the working directory (or an explicit --config path)
  4. Built-in defaults
"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BackendConfig(BaseModel):
    """Foundry Local connection settings."""

    base_url: str = "http://localhost:5273"
    model: str = "phi-3-mini"
    timeout_seconds: float = 120.0
    discover: bool = True  # auto-discover Foundry Local's ephemeral port
    # Minimum seconds between full localhost scans. Without this, a backend that
    # has gone away turns every inbound request into a subprocess + port sweep.
    discovery_cooldown_seconds: float = Field(default=5.0, ge=0.0)
    # Ask the backend for real token counts on streamed calls via
    # `stream_options.include_usage`. Most OpenAI-compatible servers emit the
    # trailing usage chunk *only* when this is set; without it the gateway's
    # online token calibration never receives a sample. Disabled automatically
    # (once, per process) if the backend rejects the field.
    request_usage: bool = True
    # Accelerator label used to key calibration records. "auto" derives a
    # platform string; set it explicitly (e.g. "npu", "dml-arc-a770") when the
    # accelerator matters — the gateway cannot detect it from the HTTP layer.
    device: str = "auto"
    # Treat `base_url` as authoritative and skip discovery entirely.
    # None (default) = auto: pinned whenever the URL was set explicitly, by
    # --backend-url, config file, or environment. An explicit URL is the only
    # supported path in containers.
    pin_url: bool | None = None
    # Ask the `foundry` CLI where the backend is before falling back to a port
    # scan. "auto" enables it when a foundry binary is on PATH and we are not in
    # a container — shelling out is fine for a host-installed sidecar and wrong
    # in a container image.
    use_foundry_cli: Literal["auto", "always", "never"] = "auto"
    foundry_cli_path: str | None = None
    foundry_cli_timeout_seconds: float = Field(default=5.0, gt=0)
    # Static bearer token sent upstream. Foundry Local does not require one;
    # this exists for deployments that put something in between.
    api_key: str | None = None
    # Forward the caller's Authorization header to the backend, so KVStream can
    # sit behind a gateway that does its own per-caller auth.
    forward_authorization: bool = True

    # Readiness probing. Answering /v1/models proves a process is alive, not
    # that it can generate a token — measured against a real Foundry Local that
    # served /v1/models in 4ms while a 4-token generation never returned. The
    # probe is a real generation, so it is cached and single-flighted: a health
    # check that becomes load is a health check that causes outages.
    probe_readiness: bool = True
    readiness_interval_seconds: float = Field(default=30.0, ge=1.0)
    readiness_timeout_seconds: float = Field(default=15.0, ge=1.0)

    # Circuit breaker. Without it, a stalled backend is rediscovered one request
    # at a time: each arrival queues, waits the full backend timeout and fails,
    # so the admission queue fills with work that will never complete.
    circuit_breaker: bool = True
    circuit_breaker_failures: int = Field(default=5, ge=1)
    circuit_breaker_reset_seconds: float = Field(default=30.0, ge=1.0)


class AdmissionConfig(BaseModel):
    """Admission-control policy for the single Foundry Local instance."""

    mode: Literal["concurrency", "tokens"] = "concurrency"
    max_concurrency: int = Field(default=8, ge=1)
    budget_tokens: int = Field(default=0, ge=0)
    admission_timeout_seconds: float = Field(default=120.0, gt=0)
    max_queue_depth: int = Field(default=1000, ge=1)
    # Starting characters-per-token ratio for cost estimation. Refined at runtime
    # from backend-reported `usage` when `learn_token_ratio` is enabled.
    chars_per_token: float = Field(default=4.0, gt=0)
    learn_token_ratio: bool = True
    # Multiplier applied to token estimates. >1.0 buys headroom against
    # under-estimation at the cost of admitting slightly fewer requests.
    token_safety_factor: float = Field(default=1.0, ge=1.0, le=3.0)
    calibration_store: str = ".kvstream/calibration.json"
    # Fraction of `max_tokens` reserved up front in token mode.
    #
    # 1.0 (default) reserves the worst case, exactly as the proposal specifies,
    # and the budget can then never be breached. Most requests stop far short of
    # `max_tokens`, so that worst case leaves real capacity idle. Lowering this
    # reserves less and reclaims the difference live as tokens actually arrive;
    # if a generation outgrows its reservation the gateway tops it up rather
    # than truncating, which can transiently exceed the budget. Every such
    # event is counted in `kvstream_reservation_overshoot_total` — tune against
    # that series, not by guesswork.
    reserve_completion_ratio: float = Field(default=1.0, gt=0.0, le=1.0)
    # Generation length assumed when a request omits `max_tokens`. Used only to
    # cost the request for admission — it is never sent to the backend, so the
    # model's own default still applies.
    default_max_tokens: int = Field(default=512, gt=0)
    # Drift detection: warn when served latency per token exceeds the
    # calibration-time baseline by this factor. A budget measured on a fresh
    # runtime stops describing one that has degraded, and nothing else notices.
    drift_warn_ratio: float = Field(default=3.0, ge=1.0)
    drift_min_samples: int = Field(default=20, ge=1)
    # Refuse a request on arrival when the measured drain rate says the queue
    # cannot reach it inside `admission_timeout_seconds`. Without this, shed
    # load is shed only after the full timeout — measured at 120.1s on real
    # hardware, which is backpressure too slow for a client to act on.
    reject_when_hopeless: bool = True
    # Completions needed before the drain rate is trusted enough to refuse
    # anyone. An unmeasured system must not reject on a guess.
    min_rate_samples: int = Field(default=5, ge=1)
    # How often a queued request re-predicts whether it can still make its
    # deadline. An arrival-time decision uses the rate from when the queue was
    # empty, which can be wildly optimistic once it is deep.
    recheck_interval_seconds: float = Field(default=1.0, ge=0.05)
    # Trailing window for measuring how fast the backend actually completes
    # work. Counting completions over a window cannot be fooled by a burst of
    # instant failures the way an inter-completion gap can.
    rate_window_seconds: float = Field(default=10.0, ge=1.0)
    # Slack on the prediction. Only refuse when the deadline will be missed by
    # this factor — never on a marginal call. The predictor cannot tell request
    # classes apart, so it is deliberately biased toward waiting.
    hopeless_margin: float = Field(default=1.5, ge=1.0)


class RoutesConfig(BaseModel):
    """
    Which OpenAI routes the gateway proxies (proposal §8.4).

    A gateway that admits only chat traffic leaves the other paths unprotected —
    clients simply route around it, which defeats admission control. Embeddings
    cost honestly in input tokens and share the KV-token budget. Audio does not:
    there is no token cost for a sound file measurable at the HTTP layer, so
    transcription is admitted under its own plain concurrency limit rather than
    pretending it fits the token budget.
    """

    embeddings: bool = True
    transcriptions: bool = True
    # Legacy /v1/completions. Proxied for the same reason: an unadmitted route
    # is a way around admission control. Foundry Local may answer 404, which is
    # passed through unchanged.
    completions: bool = True
    audio_max_concurrency: int = Field(default=2, ge=1)
    audio_max_upload_mb: int = Field(default=25, ge=1)


class CacheConfig(BaseModel):
    """Opt-in response cache for deterministic (temperature 0) requests."""

    enabled: bool = False
    ttl_seconds: int = Field(default=900, ge=1)
    max_entries: int = Field(default=1024, ge=1)
    # Honour a per-request `Cache-Control: no-store / no-cache` (and the
    # KVStream-specific `x-kvstream-cache` header). Caching changes response
    # semantics, so a caller must be able to opt a single request out without
    # reconfiguring the gateway.
    respect_request_headers: bool = True
    # Refuse to cache a response larger than this. A single huge entry can
    # evict everything useful; better to skip it than to thrash the cache.
    max_entry_bytes: int = Field(default=1_000_000, ge=1024)


class CoalesceConfig(BaseModel):
    enabled: bool = True


class ObservabilityConfig(BaseModel):
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class Settings(BaseSettings):
    """Top-level configuration."""

    model_config = SettingsConfigDict(env_prefix="KVSTREAM_", env_nested_delimiter="__")

    # Bind to loopback by default — the gateway has no authentication layer.
    host: str = "127.0.0.1"
    port: int = 8080

    # Seconds to let in-flight requests finish on shutdown before the process
    # exits. Queued-but-not-started requests are turned away immediately.
    drain_timeout_seconds: float = Field(default=30.0, ge=0.0)

    # Declared KV geometry per model, used only for *relative* request costing
    # across models (proposal 5.2). Each entry accepts model-config field names,
    # e.g. {"phi-3-mini": {"num_hidden_layers": 32, "num_key_value_heads": 32,
    # "head_dim": 96, "torch_dtype": "float16"}}. Unknown models cost exactly as
    # they did before, so this is always optional.
    models: dict[str, dict] = Field(default_factory=dict)

    backend: BackendConfig = Field(default_factory=BackendConfig)
    admission: AdmissionConfig = Field(default_factory=AdmissionConfig)
    routes: RoutesConfig = Field(default_factory=RoutesConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    coalesce: CoalesceConfig = Field(default_factory=CoalesceConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    def backend_url_is_explicit(self) -> bool:
        """
        True when the operator actually named the backend URL.

        The default ``base_url`` is a starting guess, not a statement — so it
        must not suppress discovery. Pydantic records which fields were supplied
        rather than defaulted, and that covers all three sources the proposal
        calls authoritative: the config file, ``KVSTREAM_BACKEND__BASE_URL``,
        and ``--backend-url`` (which assigns onto the model).
        """
        return "base_url" in self.backend.model_fields_set

    def backend_is_pinned(self) -> bool:
        """Whether discovery should be skipped entirely for this backend."""
        if self.backend.pin_url is not None:
            return self.backend.pin_url
        return self.backend_url_is_explicit()

    def foundry_cli_enabled(self) -> bool:
        """Whether shelling out to the `foundry` CLI is appropriate here."""
        from kvstream.backend.foundry_cli import in_container

        mode = self.backend.use_foundry_cli
        if mode == "always":
            return True
        if mode == "never":
            return False
        return not in_container()

    @classmethod
    def load(cls, path: str | None = None) -> Settings:
        """Load YAML (if present) then layer env vars and defaults on top."""
        import yaml

        candidate = path
        if candidate is None and os.path.exists("kvstream.yaml"):
            candidate = "kvstream.yaml"

        data: dict = {}
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        return cls(**data)
