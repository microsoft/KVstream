"""Admission control: the KV-capacity manager and calibration."""

from __future__ import annotations

import logging

from kvstream.admission.calibration import (
    UNKNOWN,
    CalibrationKey,
    CalibrationLookup,
    CalibrationService,
    find_knee,
    lookup_budget,
    resolve_device,
    save_budget,
)
from kvstream.admission.capacity import (
    AdmissionTimeout,
    AdmissionTooSlow,
    CapacityManager,
    QueueFull,
    RequestCost,
)
from kvstream.admission.regime import RuntimeProfile, classify_runtime
from kvstream.config import AdmissionConfig, BackendConfig

logger = logging.getLogger("kvstream.admission")

__all__ = [
    "CapacityManager",
    "RequestCost",
    "RuntimeProfile",
    "classify_runtime",
    "AdmissionTimeout",
    "AdmissionTooSlow",
    "QueueFull",
    "CalibrationKey",
    "CalibrationLookup",
    "CalibrationService",
    "UNKNOWN",
    "find_knee",
    "lookup_budget",
    "resolve_device",
    "save_budget",
    "calibration_key_for",
    "resolve_budget",
    "build_capacity_manager",
]


def calibration_key_for(
    backend: BackendConfig,
    cli_generation: str = UNKNOWN,
    backend_version: str = UNKNOWN,
) -> CalibrationKey:
    """
    Build the key identifying this gateway's Foundry Local environment.

    ``cli_generation`` and ``backend_version`` are placeholders until
    version-aware backend resolution lands (proposal §8.3) — at which point the
    detected dialect and version flow straight in here, and existing records
    keyed ``unknown`` degrade to partial matches rather than silently being
    treated as exact.
    """
    return CalibrationKey(
        model=backend.model,
        device=resolve_device(backend.device),
        cli_generation=cli_generation,
        backend_version=backend_version,
    )


def resolve_budget(
    cfg: AdmissionConfig, key: CalibrationKey
) -> tuple[int, str, dict]:
    """
    Resolve the effective ``(budget, unit, provenance)`` from configuration.

    In ``tokens`` mode, use the configured budget or, if unset, a calibrated
    value for ``key``. If neither is available, fall back to ``concurrency``
    mode so the gateway always starts with a working, honest limiter.
    """
    provenance: dict = {"source": "config", "key": key.as_dict()}

    if cfg.mode == "tokens":
        if cfg.budget_tokens > 0:
            provenance["source"] = "configured"
            return cfg.budget_tokens, "tokens", provenance

        lookup = lookup_budget(cfg.calibration_store, key)
        provenance["lookup"] = lookup.as_dict()
        if lookup.usable:
            provenance["source"] = f"calibration:{lookup.match}"
            if lookup.match == "partial":
                logger.warning(
                    "using a calibrated budget measured in a different environment "
                    "(%s); re-run `kvstream calibrate` to remove this warning.",
                    lookup.detail,
                )
            return lookup.budget_tokens, "tokens", provenance

        logger.warning(
            "admission.mode=tokens but no budget configured or calibrated for %s: %s. "
            "Falling back to concurrency mode (run `kvstream calibrate`).",
            key.as_str(), lookup.detail,
        )
        provenance["source"] = "fallback:concurrency"

    return cfg.max_concurrency, "concurrency", provenance


def build_capacity_manager(
    cfg: AdmissionConfig, key: CalibrationKey
) -> tuple[CapacityManager, dict]:
    budget, unit, provenance = resolve_budget(cfg, key)
    manager = CapacityManager(
        budget=budget,
        unit=unit,
        admission_timeout=cfg.admission_timeout_seconds,
        max_queue_depth=cfg.max_queue_depth,
        # The reserve ratio only has meaning against a token budget; in
        # concurrency mode every request costs exactly one slot.
        reserve_ratio=cfg.reserve_completion_ratio if unit == "tokens" else 1.0,
        reject_when_hopeless=cfg.reject_when_hopeless,
        min_rate_samples=cfg.min_rate_samples,
        recheck_interval=cfg.recheck_interval_seconds,
        rate_window=cfg.rate_window_seconds,
        hopeless_margin=cfg.hopeless_margin,
    )
    return manager, provenance
