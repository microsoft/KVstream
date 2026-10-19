"""
The KVStream gateway — an OpenAI-compatible FastAPI app in front of one
Foundry Local instance.

Request path (all components are wired and active):

    request → tokenize/cost → [cache] → [coalesce] → admission → forward/stream
            → Foundry Local → live accounting → release → [cache store] → metrics

Non-streamed client requests are forwarded non-streamed, and streamed ones ask
the backend for ``usage``; both exist so the gateway sees real token counts
rather than living on estimates.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import NoReturn

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.datastructures import FormData
from starlette.types import Message, Receive

from kvstream.admission import (
    UNKNOWN,
    AdmissionTimeout,
    AdmissionTooSlow,
    CapacityManager,
    QueueFull,
    RequestCost,
    build_capacity_manager,
    calibration_key_for,
    resolve_budget,
)
from kvstream.admission.calibration import CalibrationKey
from kvstream.admission.drift import DriftMonitor, baseline_from_provenance
from kvstream.admission.geometry import GeometryRegistry, find_geometry
from kvstream.backend import FoundryClient, foundry_cli
from kvstream.backend.discovery import KVSTREAM_HEADER
from kvstream.backend.foundry import FoundryClientError, FoundryError, Token
from kvstream.backend.foundry_cli import FoundryCli
from kvstream.backend.health import BackendHealth, BackendUnavailable, CircuitBreaker, Readiness
from kvstream.backend.inventory import (
    ModelInventory,
    Refusal,
    diagnose,
    embedding_refusal,
    matches,
    refusal_for,
    select_model,
)
from kvstream.cache import (
    CachedResponse,
    Coalescer,
    ResponseCache,
    StreamBroadcast,
    StreamCoalescer,
    request_key,
)
from kvstream.config import Settings
from kvstream.models import (
    ChatCompletionRequest,
    CompletionRequest,
    EmbeddingsRequest,
    backend_payload,
    fill_max_tokens,
)
from kvstream.observability import Metrics
from kvstream.tokenization import (
    KIND_EMBEDDINGS,
    MESSAGE_OVERHEAD_TOKENS,
    EstimatorRegistry,
    TokenEstimator,
    count_units,
    message_chars,
    message_units,
)
from kvstream.version import __version__

logger = logging.getLogger("kvstream.app")

ROUTE_CHAT = "chat"
ROUTE_EMBEDDINGS = "embeddings"
ROUTE_AUDIO = "transcriptions"
ROUTE_COMPLETIONS = "completions"

# Audio is admitted on a plain concurrency limit: one slot per transcription.
_ONE_SLOT = RequestCost(prompt_tokens=1, max_tokens=0)

# Numeric encoding for the circuit-breaker gauge: 0 closed, 1 half-open, 2 open.
_CIRCUIT_STATES = {"closed": 0, "half_open": 1, "open": 2}

# Environment variables an operator might use to fan the app across processes.
# Admission state lives in this process, so more than one worker means more than
# one budget — see `_guard_single_process`.
_WORKER_ENV_VARS = ("WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS", "KVSTREAM_WORKERS")


def _guard_single_process() -> None:
    """
    Refuse to start when the process is one of several workers.

    The capacity manager holds its reservations in memory. Running N workers
    multiplies the effective budget by N and silently invalidates calibration —
    the exact overload the gateway exists to prevent. Fail loudly instead.
    """
    for var in _WORKER_ENV_VARS:
        raw = os.environ.get(var)
        if not raw:
            continue
        try:
            workers = int(raw)
        except ValueError:
            continue
        if workers > 1:
            raise RuntimeError(
                f"{var}={workers}: KVStream must run as a single process. Admission "
                "state is per-process, so N workers enforce N times the calibrated "
                "budget. Run one KVStream per Foundry Local instance (that is the "
                "sidecar topology), or put a load balancer in front of several "
                "gateways that each front their own backend."
            )


class Gateway:
    """Holds the wired components and implements the request logic."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._backend = FoundryClient(
            base_url=settings.backend.base_url,
            model=settings.backend.model,
            timeout=settings.backend.timeout_seconds,
            discover=settings.backend.discover,
            exclude_ports=[settings.port],
            discovery_cooldown=settings.backend.discovery_cooldown_seconds,
            request_usage=settings.backend.request_usage,
            api_key=settings.backend.api_key,
            pinned=settings.backend_is_pinned(),
            use_foundry_cli=settings.foundry_cli_enabled(),
            foundry_cli_path=settings.backend.foundry_cli_path,
            foundry_cli_timeout=settings.backend.foundry_cli_timeout_seconds,
        )
        # Relative KV footprint per model, anchored on the model the budget was
        # calibrated for. Unknown models weigh 1.0, i.e. cost exactly as before.
        self.geometry = GeometryRegistry(settings.backend.model or "")
        self.geometry.load_config(settings.models)
        # Which models Foundry Local has downloaded and loaded. Drives model
        # auto-detection and the refusal of streams that cannot succeed.
        self._cli: FoundryCli | None = None
        # The entries of the last `foundry cache list`, so the model types can be
        # read from the same call that reported which models are loaded.
        self._cache_entries: list[dict] | None = None
        self.inventory = ModelInventory(
            self._read_downloaded, self._read_loaded, read_types=self._read_types
        )
        self.model_source = "configured" if settings.backend.model else "not yet detected"
        # True while the model in use was auto-detected rather than configured:
        # only then may it be replaced when Foundry Local's loaded model changes.
        self._model_auto = False
        self._environment_applied = False
        # A re-resolved budget waiting for an idle moment to take effect
        # (see `_install_pending_capacity`): the calibration key it is for.
        self._pending_capacity: CalibrationKey | None = None
        self.calibration_key = calibration_key_for(settings.backend)
        self.capacity: CapacityManager
        # Provisional: the CLI version (and possibly the model) are not known
        # until startup, which resolves again and reports the result. Logging
        # here produced false "no budget" / "different environment" warnings.
        self.capacity, self.budget_provenance = build_capacity_manager(
            settings.admission, self.calibration_key, quiet=True
        )
        # Audio gets a plain concurrency limit of its own. A sound file has no
        # token cost measurable at the HTTP layer, and admitting it against the
        # KV-token budget would mean inventing one.
        self.audio_capacity = CapacityManager(
            budget=settings.routes.audio_max_concurrency,
            unit="concurrency",
            admission_timeout=settings.admission.admission_timeout_seconds,
            max_queue_depth=settings.admission.max_queue_depth,
            reject_when_hopeless=settings.admission.reject_when_hopeless,
            min_rate_samples=settings.admission.min_rate_samples,
            recheck_interval=settings.admission.recheck_interval_seconds,
            rate_window=settings.admission.rate_window_seconds,
            hopeless_margin=settings.admission.hopeless_margin,
        )
        self.cache = (
            ResponseCache(
                ttl_seconds=settings.cache.ttl_seconds,
                max_entries=settings.cache.max_entries,
            )
            if settings.cache.enabled
            else None
        )
        self.coalescer = Coalescer() if settings.coalesce.enabled else None
        # Streaming needs its own primitive: followers join a response that is
        # already in progress rather than awaiting a finished result.
        self.stream_coalescer = StreamCoalescer() if settings.coalesce.enabled else None
        # One estimator per model and kind of traffic: each model has its own
        # tokenizer, and an embedding model's usage says nothing about a chat
        # model's (see `EstimatorRegistry`).
        self.estimators = EstimatorRegistry(
            lambda: TokenEstimator(
                settings.admission.chars_per_token,
                safety_factor=settings.admission.token_safety_factor,
                learn=settings.admission.learn_token_ratio,
            )
        )
        # Liveness, readiness and the circuit breaker: three lessons from one
        # measured stall, where /v1/models answered in 4ms while the runtime
        # could not finish a 4-token generation.
        self.health = BackendHealth(
            self._backend,
            probe_readiness=settings.backend.probe_readiness,
            readiness_interval=settings.backend.readiness_interval_seconds,
            readiness_timeout=settings.backend.readiness_timeout_seconds,
            force_min_interval=settings.backend.readiness_force_min_interval_seconds,
            breaker=CircuitBreaker(
                failure_threshold=settings.backend.circuit_breaker_failures,
                reset_seconds=settings.backend.circuit_breaker_reset_seconds,
                enabled=settings.backend.circuit_breaker,
                # A trial may legitimately queue and then generate, so it is
                # presumed lost only after both windows have passed.
                trial_timeout=(
                    settings.admission.admission_timeout_seconds + settings.backend.timeout_seconds
                ),
            ),
        )
        # Is the backend still the machine the budget was measured on?
        self.drift = DriftMonitor(
            baseline_from_provenance(self.budget_provenance),
            warn_ratio=settings.admission.drift_warn_ratio,
            min_samples=settings.admission.drift_min_samples,
        )
        self.metrics = Metrics()
        self.metrics.budget.set(self.capacity.budget)
        # Streamed generations run as gateway-owned tasks (see
        # `handle_streaming`). Holding them here keeps them alive: the event
        # loop only weakly references tasks.
        self._runs: set[asyncio.Task] = set()
        self.backend_healthy = False
        logger.info(
            "admission: %s budget=%d (%s) reserve_ratio=%.2f | max_tokens when unset: %s "
            "| cache=%s | coalesce=%s",
            self.capacity.unit,
            self.capacity.budget,
            self.budget_provenance.get("source"),
            self.capacity.reserve_ratio,
            (
                f"{settings.admission.default_max_tokens} (sent upstream)"
                if settings.admission.apply_default_max_tokens
                else f"model default (costed at {settings.admission.default_max_tokens})"
            ),
            self.cache is not None,
            self.coalescer is not None,
        )
        if self.capacity.reserve_ratio < 1.0:
            logger.warning(
                "admission.reserve_completion_ratio=%.2f reserves below the worst case; "
                "generations that outgrow their reservation are topped up and may "
                "transiently exceed the budget. Watch kvstream_reservation_overshoot_total.",
                self.capacity.reserve_ratio,
            )

    @property
    def backend(self) -> FoundryClient:
        return self._backend

    @backend.setter
    def backend(self, client: FoundryClient) -> None:
        """
        Rebind the backend, and everything that holds a reference to it.

        `BackendHealth` probes the client directly, so a swap that left it
        pointing at the old one would report the health of a backend nothing
        else is using — silently, and only in the direction of false confidence.
        """
        self._backend = client
        self.health.rebind(client)

    @property
    def sent_max_tokens(self) -> int | None:
        """The limit added upstream to a request that sets none, or ``None``."""
        admission = self.settings.admission
        return admission.default_max_tokens if admission.apply_default_max_tokens else None

    @property
    def estimator(self) -> TokenEstimator:
        """The chat estimator for the model KVStream itself uses (reported in metrics)."""
        return self.estimators.for_model(self.settings.backend.model or "")

    # -- startup -------------------------------------------------------

    async def startup(self) -> None:
        """
        Learn which Foundry Local generation we are in front of, then settle.

        The calibration key carries the CLI generation and Foundry version
        (proposal §8.6), but neither is knowable until a CLI probe has run — so
        the gateway starts with an ``unknown`` key and re-resolves the budget
        here, before any request is served. A record measured against a
        different generation then degrades to a partial match instead of being
        mistaken for an exact one.
        """
        cli = await self.backend.detect_cli()
        self._cli = cli
        self.backend_healthy = await self.health.check_reachable()
        self.metrics.backend_up.set(1 if self.backend_healthy else 0)
        if self.backend_healthy:
            # The CLI banner is printed before discovery runs, so this is the
            # first point at which the real backend URL is known.
            resolution = getattr(self.backend, "last_resolution", None)
            logger.info(
                "backend: %s (%s)",
                self.backend.base_url,
                resolution.detail if resolution and resolution.detail else "configured",
            )
        else:
            logger.warning(
                "Foundry Local is not reachable at %s. %s",
                self.backend.base_url,
                self.backend.unreachable_hint(),
            )
        # The model must be settled before the calibration key: the key
        # includes it, and a budget is only valid for the model it measured.
        if self.settings.backend.model:
            await self._warn_if_model_unknown(cli)
        else:
            await self._detect_model()
        self._apply_environment(cli)
        await self._learn_geometry(cli)

    def _apply_environment(self, cli: FoundryCli) -> None:
        """
        Re-key calibration for the detected environment and re-resolve the budget.

        Runs at startup, and again if the model is only determined later.
        """
        key = calibration_key_for(
            self.settings.backend,
            cli_generation=cli.generation,
            backend_version=cli.version or UNKNOWN,
        )
        # Always resolve (and report) once: the constructor's resolution was
        # provisional and silent, so an unchanged key must not skip this.
        if key == self.calibration_key and self._environment_applied:
            return
        self._environment_applied = True
        self.calibration_key = key
        budget, unit, provenance = resolve_budget(self.settings.admission, key)
        if (budget, unit) == (self.capacity.budget, self.capacity.unit):
            self._pending_capacity = None
            self.budget_provenance = provenance
            self.drift = self._new_drift_monitor()
            return
        logger.info(
            "admission budget for %s: %s %d (%s)",
            key.as_str(),
            unit,
            budget,
            provenance.get("source"),
        )
        self._pending_capacity = key
        if not self._install_pending_capacity():
            logger.info(
                "the new budget takes effect once the %d request(s) now in the "
                "gateway have finished; until then the current limit applies",
                self.capacity.active + self.capacity.waiting,
            )

    def settle_environment_after_failed_startup(self) -> None:
        """
        Resolve and report the admission budget when :meth:`startup` raised.

        The constructor's resolution is provisional and deliberately silent,
        so startup is the only place the budget is reported. If startup failed
        before reaching it, a missing or mismatched token budget was never
        announced. This resolves it with whatever was learned (an unknown CLI
        if detection never ran), and can itself never stop the gateway serving.
        """
        try:
            self._apply_environment(self._cli or FoundryCli(generation=UNKNOWN))
        except Exception:  # noqa: BLE001
            logger.exception("could not resolve the admission budget; keeping the provisional one")

    def _new_drift_monitor(self) -> DriftMonitor:
        return DriftMonitor(
            baseline_from_provenance(self.budget_provenance),
            warn_ratio=self.settings.admission.drift_warn_ratio,
            min_samples=self.settings.admission.drift_min_samples,
        )

    def _install_pending_capacity(self) -> bool:
        """
        Swap in a re-resolved capacity manager — only when that is safe.

        Replacing the manager while requests hold reservations or wait in its
        queue stranded them: they released into the new manager, so the old
        one never freed its slots and its queued requests waited out their
        whole timeout (measured: a 503 after 6 s with capacity free after 2 s),
        while the new manager admitted on top of work it could not see. So a
        new budget waits for an idle moment. It is checked at admission time,
        before the request is costed, so every request is costed by and
        admitted into the same manager. Never during shutdown: a fresh manager
        would not be draining. Returns whether the swap happened.
        """
        key = self._pending_capacity
        if key is None or not self.capacity.idle or self.capacity.draining:
            return False
        self.capacity, self.budget_provenance = build_capacity_manager(
            self.settings.admission,
            key,
            quiet=True,  # reported when resolved
        )
        self._pending_capacity = None
        self.drift = self._new_drift_monitor()
        self.metrics.budget.set(self.capacity.budget)
        logger.info(
            "admission budget in effect: %s %d (%s)",
            self.capacity.unit,
            self.capacity.budget,
            self.budget_provenance.get("source"),
        )
        return True

    # -- model ----------------------------------------------------------

    async def _read_downloaded(self) -> list[str] | None:
        reader = getattr(self.backend, "models_or_none", None)
        if reader is not None:
            result: list[str] | None = await reader()
            return result
        return await self.backend.list_models()

    async def _read_loaded(self) -> list[str] | None:
        if self._cli is None:
            self._cache_entries = None
            return None
        self._cache_entries = await foundry_cli.read_cache(
            self._cli, self.settings.backend.foundry_cli_timeout_seconds
        )
        if self._cache_entries is None:
            return None
        return foundry_cli.loaded_ids(self._cache_entries)

    async def _read_types(self) -> dict[str, str] | None:
        """Model types from the `foundry cache list` that `_read_loaded` just made."""
        if self._cache_entries is None:
            return None
        return foundry_cli.model_types(self._cache_entries)

    async def _detect_model(self) -> str | None:
        """Adopt the one loaded model, or record why there is no single answer."""
        selection = select_model(await self.inventory.snapshot(fresh=True))
        if selection.model is not None:
            self._adopt_model(selection.model)
            self._model_auto = True
            self.model_source = f"auto-detected: {selection.detail}"
            logger.info("model: %s (auto-detected: %s)", selection.model, selection.detail)
            return selection.model
        source = f"undetermined: {selection.detail}"
        if source != self.model_source:  # say it once, not on every health check
            logger.warning(
                "model not determined: %s Requests are still forwarded (each names "
                "its own model), but /health reports not ready until KVStream has a "
                "model to probe.",
                selection.detail,
            )
        self.model_source = source
        return None

    def _adopt_model(self, model: str) -> None:
        self.settings.backend.model = model
        adopt = getattr(self.backend, "use_model", None)
        if adopt is not None:
            adopt(model)
        else:
            self.backend.model = model
        self.geometry = GeometryRegistry(model)
        self.geometry.load_config(self.settings.models)

    def _forget_model(self) -> None:
        """Drop an auto-detected model that Foundry Local no longer serves."""
        self.settings.backend.model = None
        forget = getattr(self.backend, "use_model", None)
        if forget is not None:
            forget(None)
        else:
            self.backend.model = None
        self._model_auto = False

    async def ensure_model(self) -> str | None:
        """
        The model KVStream probes, detecting it now if it was not known before.

        Covers the case where KVStream starts before a model is loaded: the
        next health check finds it (detection is rate-limited by the inventory).
        """
        current = self.settings.backend.model
        if current and not self._model_auto:
            return current  # configured by the operator: never second-guessed
        if current:
            # Auto-detected models come and go: once this one is no longer
            # served, keeping it meant probing a model that is not loaded and
            # reporting "not serving" while Foundry Local served another one
            # perfectly well, until KVStream was restarted.
            if refusal_for(current, await self.inventory.snapshot(fresh=True)) is None:
                return current
            logger.info(
                "auto-detected model %s is no longer loaded in Foundry Local; detecting again",
                current,
            )
            self._forget_model()
        model = await self._detect_model()
        if model is not None and self._cli is not None:
            self._apply_environment(self._cli)
            await self._learn_geometry(self._cli)
        return model

    async def stream_refusal(self, model: str) -> Refusal | None:
        """
        Why a streamed request for ``model`` cannot succeed, or ``None``.

        Foundry Local (CLI 0.10.3) answers a streamed request for a model that
        is unknown *or merely not loaded* with ``200`` and a dropped
        connection, indistinguishable from a crash; without this check each
        such request counts against the circuit breaker. A refusal is
        confirmed against a fresh reading first, since the model may have just
        been loaded. Fails open on anything it cannot read.
        """
        if refusal_for(model, await self.inventory.snapshot()) is None:
            return None
        return refusal_for(model, await self.inventory.snapshot(fresh=True))

    async def _learn_geometry(self, cli: FoundryCli) -> None:
        """
        Fill in KV geometry the operator did not declare, best-effort.

        `foundry model show --output json` is unverified, so a miss is silent
        and the model keeps its neutral weight of 1.0 — costed exactly as it
        would have been without any geometry at all.
        """
        anchor = self.settings.backend.model
        if not cli.available or not anchor:
            return
        if self.geometry.get(anchor) is not None:
            return
        payload = await foundry_cli.show_model(
            cli, anchor, self.settings.backend.foundry_cli_timeout_seconds
        )
        if payload is None:
            return
        geometry = find_geometry(payload, source="foundry model show")
        if geometry is not None:
            self.geometry.declare(anchor, geometry)

    async def _warn_if_model_unknown(self, cli: FoundryCli) -> None:
        """
        Warn at startup when the configured model will not be accepted.

        A wrong model id is the most common first-run failure, and a startup
        warning beats an opaque 400 on the first inference call.
        """
        model = self.settings.backend.model
        if not model:
            return
        snap = await self.inventory.snapshot(fresh=True)
        catalog: list[str] | None = None
        if cli.available:
            catalog = await foundry_cli.list_models(
                cli, self.settings.backend.foundry_cli_timeout_seconds
            )
        problem = diagnose(model, snap, catalog)
        if problem:
            logger.warning("%s", problem)

    # -- cost / admission ---------------------------------------------

    def _cost(self, req: ChatCompletionRequest) -> tuple[RequestCost, int]:
        """Return (cost model, admission cost)."""
        prompt_tokens = self.estimators.for_model(req.model).estimate_messages(req.messages)
        rc = RequestCost(
            prompt_tokens=prompt_tokens,
            max_tokens=req.generation_budget(self.settings.admission.default_max_tokens),
            kv_weight=self.geometry.weight_for(req.model),
        )
        return rc, self.capacity.cost_of(rc)

    def _settle(
        self,
        req: ChatCompletionRequest,
        completion_text: str,
        usage: dict | None,
    ) -> tuple[int, int]:
        """
        Reconcile estimates against the backend's real counts.

        When ``usage`` is present the real prompt count also calibrates the
        estimator, so future admission costs converge on the model's actual
        tokenizer behaviour. Returns the (prompt, completion) counts to report.

        Estimates are made *without* ``token_safety_factor``: that factor is
        admission headroom, and applied here it inflated the `usage` block a
        client receives when the backend reports none.
        """
        estimator = self.estimators.for_model(req.model)
        prompt_tokens = estimator.estimate_messages(req.messages, with_safety=False)
        completion_tokens = estimator.estimate_text(completion_text, with_safety=False)
        if usage:
            actual_prompt = usage.get("prompt_tokens")
            actual_completion = usage.get("completion_tokens")
            if isinstance(actual_prompt, int) and actual_prompt > 0:
                # The estimator models a prompt as text tokens plus a fixed
                # template overhead per message, so it must learn its text
                # ratio from the text tokens alone. Learning from the total
                # attributed the template tokens to the text, dragging the
                # ratio down on short messages and then over-estimating long
                # prompts — while the overhead was still added on top.
                text_tokens = actual_prompt - MESSAGE_OVERHEAD_TOKENS * len(req.messages)
                learner = self.estimators.learner(req.model)
                if text_tokens > 0 and learner is not None:
                    learner.observe(
                        message_chars(req.messages), message_units(req.messages), text_tokens
                    )
                prompt_tokens = actual_prompt
            if isinstance(actual_completion, int) and actual_completion >= 0:
                completion_tokens = actual_completion
        return prompt_tokens, completion_tokens

    async def _admit(
        self,
        req_id: str,
        rc: RequestCost,
        route: str = ROUTE_CHAT,
        manager: CapacityManager | None = None,
    ) -> None:
        """
        Reserve ``rc``'s cost, or raise an OpenAI-shaped 503.

        The cost is computed here, from the manager the request is admitted
        into, rather than by the caller: a budget change between the two would
        otherwise cost a request in one unit and charge it in another (a
        concurrency slot of 1 admitted as 1 token). Nothing awaits between the
        swap check, the costing and the reservation, so no other request can
        interleave.
        """
        if manager is None:
            self._install_pending_capacity()
        limiter = manager or self.capacity
        cost = limiter.cost_of(rc)
        # Fail fast before queueing. Without this a stalled backend is
        # rediscovered one request at a time: each arrival waits the full
        # backend timeout, and the admission queue fills with work that can
        # never complete.
        try:
            self.health.guard(req_id)
        except BackendUnavailable as exc:
            self.metrics.rejected.labels(route, "circuit_open").inc()
            self.metrics.requests.labels(route, "rejected").inc()
            raise HTTPException(
                status_code=503,
                detail=str(exc),
                headers={"Retry-After": str(self.health.breaker.retry_after_seconds)},
            ) from exc

        t0 = time.perf_counter()
        try:
            await self._queue_for(req_id, cost, route, limiter)
        except BaseException:
            # Refused or cancelled before reaching the backend. If this request
            # was the breaker's half-open trial it proved nothing either way,
            # and an unreleased trial would hold the breaker half-open —
            # refusing everyone — until the trial timeout.
            self.health.release_trial(req_id)
            raise
        self.metrics.admission_wait.labels(route).observe(time.perf_counter() - t0)

    async def _queue_for(
        self, req_id: str, cost: int, route: str, limiter: CapacityManager
    ) -> None:
        """Reserve ``cost``, mapping every refusal to an OpenAI-shaped 503."""
        try:
            await limiter.admit(req_id, cost)
        except QueueFull as exc:
            # Overload and shutdown are both 503, but they mean different things
            # to whoever is reading the logs.
            shutting_down = limiter.draining
            reason = "shutting_down" if shutting_down else "queue_full"
            detail = (
                "gateway is shutting down; retry against a healthy instance"
                if shutting_down
                else "server overloaded (queue full)"
            )
            self.metrics.rejected.labels(route, reason).inc()
            self.metrics.requests.labels(route, "rejected").inc()
            raise HTTPException(status_code=503, detail=detail) from exc
        except AdmissionTooSlow as exc:
            # Refused on arrival, not after the timeout. Retry-After is the
            # measured prediction, so it is worth something to the client.
            self.metrics.rejected.labels(route, "predicted_wait").inc()
            self.metrics.requests.labels(route, "rejected").inc()
            raise HTTPException(
                status_code=503,
                detail=f"server overloaded ({exc})",
                headers={"Retry-After": str(max(1, int(exc.predicted_wait)))},
            ) from exc
        except AdmissionTimeout as exc:
            self.metrics.rejected.labels(route, "timeout").inc()
            self.metrics.requests.labels(route, "rejected").inc()
            raise HTTPException(status_code=503, detail="server overloaded (timeout)") from exc

    def _record_backend_failure(self, phase: str, error: object) -> None:
        """One backend failure: count it, and let the breaker decide."""
        self.metrics.backend_errors.labels(phase).inc()
        self.health.record_failure(str(error))
        self.metrics.circuit_state.set(_CIRCUIT_STATES.get(self.health.breaker.state, 0))

    def _record_status_verdict(self, phase: str, status: int) -> None:
        """
        Breaker verdict for a pass-through route, from the backend's HTTP status.

        These routes used to report only connection failures, so a backend
        answering 500 to every embeddings request never tripped the breaker.
        A 5xx is a backend fault, except 501 (Not Implemented): that says the
        backend does not offer the route, which is about the request, and
        counting it would let one client calling an unsupported route open the
        breaker for everyone. A 4xx proves nothing either way.
        """
        if status >= 500 and status != 501:
            self._record_backend_failure(phase, f"Foundry Local returned HTTP {status}")
        elif status < 400:
            self.health.record_success()
            self.health.reachable = True
            self.metrics.circuit_state.set(0)

    def _record_backend_success(self, latency_seconds: float, tokens: int, model: str) -> None:
        """One healthy round trip: reset the breaker and feed drift detection."""
        self.health.record_success()
        # A completed generation proves reachability as well as readiness.
        self.health.reachable = True
        self.metrics.backend_up.set(1)
        self.metrics.circuit_state.set(0)
        # The drift baseline was measured on the calibrated model only. Another
        # model's per-token speed says nothing about whether that one drifted,
        # and mixing them reported "drift" that was just a different model.
        anchor = self.settings.backend.model
        if anchor and matches(model, [anchor]):
            self.drift.observe(latency_seconds, tokens)
        # Always set: 0 means "unknown", and leaving the last value in place
        # kept reporting the old ratio after the monitor was replaced.
        self.metrics.drift_ratio.set(self.drift.ratio)

    async def _account(
        self, req_id: str, rc: RequestCost, generated_tokens: int, *, finished: bool = True
    ) -> None:
        """
        Update a live reservation to what the request actually occupies.

        Called as generation progresses and again the moment it finishes, so
        unused headroom returns to the budget before the response has finished
        being written back to the client.
        """
        cost = (
            self.capacity.live_cost(rc, generated_tokens)
            if finished
            else self.capacity.held_cost(rc, generated_tokens)
        )
        delta = await self.capacity.adjust(req_id, cost)
        if delta > 0:
            self.metrics.overshoot.inc()
        elif delta < 0:
            self.metrics.reclaimed.inc(-delta)

    # -- generation ---------------------------------------------------

    async def _generate_full(
        self,
        req_id: str,
        req: ChatCompletionRequest,
        raw: dict,
        rc: RequestCost,
        auth: dict[str, str] | None,
    ) -> CachedResponse:
        """Non-streamed path: forward as non-stream so `usage` comes back directly."""
        await self._admit(req_id, rc)
        started = time.perf_counter()
        try:
            try:
                body = await self.backend.chat_once(
                    backend_payload(raw, stream=False, default_max_tokens=self.sent_max_tokens),
                    auth,
                )
            except FoundryClientError:
                # The request was wrong, not the backend: the caller gets the
                # backend's own 4xx, and the breaker hears nothing.
                self.metrics.requests.labels(ROUTE_CHAT, "client_error").inc()
                raise
            except FoundryError as exc:
                self._record_backend_failure("nonstreaming", exc)
                self.metrics.requests.labels(ROUTE_CHAT, "error").inc()
                raise HTTPException(status_code=502, detail=str(exc)) from exc

            if not isinstance(body.get("choices"), list) or not body["choices"]:
                # Not a usable completion. Passing this through would make a
                # broken upstream look like a KVStream response.
                self._record_backend_failure("nonstreaming", "response had no choices")
                self.metrics.requests.labels(ROUTE_CHAT, "error").inc()
                raise HTTPException(
                    status_code=502,
                    detail="Foundry Local returned a response with no choices",
                )

            usage = body.get("usage") if isinstance(body.get("usage"), dict) else None
            prompt_tokens, completion_tokens = self._settle(req, _response_text(body), usage)
            self._record_backend_success(
                time.perf_counter() - started, prompt_tokens + completion_tokens, req.model
            )
            await self._account(req_id, rc, completion_tokens)

            if usage is None:
                # The backend reported no counts. Fill the field in from
                # KVStream's estimate so OpenAI clients are not left without
                # one, and say plainly — in a header, not by corrupting the
                # standard object — that these are estimates rather than truth.
                body = dict(body)
                body["usage"] = {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                }

            return CachedResponse(
                body=body,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                finish_reason=_response_finish(body),
                usage_estimated=usage is None,
            )
        finally:
            await self.capacity.release(req_id)
            # No-op once a verdict was recorded; otherwise (client error,
            # cancellation) the half-open trial is handed back.
            self.health.release_trial(req_id)

    async def handle_nonstreaming(
        self,
        req_id: str,
        req: ChatCompletionRequest,
        raw: dict,
        rc: RequestCost,
        key: str | None,
        auth: dict[str, str] | None = None,
    ) -> JSONResponse:
        t0 = time.perf_counter()

        async def produce() -> CachedResponse:
            return await self._generate_full(req_id, req, raw, rc, auth)

        if key and self.coalescer:
            result, follower = await self.coalescer.run(key, produce)
            if follower:
                self.metrics.coalesced.inc()
                self.metrics.requests.labels(ROUTE_CHAT, "coalesced").inc()
            else:
                self.metrics.requests.labels(ROUTE_CHAT, "served").inc()
        else:
            result = await produce()
            self.metrics.requests.labels(ROUTE_CHAT, "served").inc()

        if key and self.cache:
            self._store(key, result)
        self.metrics.latency.labels(ROUTE_CHAT).observe(time.perf_counter() - t0)
        # The backend's own response body, forwarded intact — tool_calls,
        # logprobs, system_fingerprint and all.
        return JSONResponse(result.body or {}, headers=_result_headers(req_id, result))

    async def handle_streaming(
        self,
        req_id: str,
        req: ChatCompletionRequest,
        raw: dict,
        rc: RequestCost,
        key: str | None,
        auth: dict[str, str] | None = None,
    ) -> StreamingResponse:
        """
        Streamed path. The upstream generation is owned by the gateway.

        The generation runs as a gateway-owned task that publishes into a
        :class:`StreamBroadcast`; every client response -- this request's own
        included -- is a subscriber. That ownership is what makes three things
        true at once:

        * a client leaving never truncates what coalesced followers receive;
        * ``[DONE]`` is published only when the upstream stream really ended,
          so a client that sees it holds the whole response;
        * when the *last* subscriber leaves, the upstream call is cancelled,
          freeing a serialising backend instead of generating for nobody.
        """
        received = time.perf_counter()
        # Singleflight: if an identical stream is already running, ride it
        # rather than admitting a second copy of the same work. Followers cost
        # no budget at all, which is the whole point.
        if key and self.stream_coalescer:
            joined = self.stream_coalescer.follower_for(key)
            if joined is not None:
                self.metrics.coalesced.inc()
                self.metrics.requests.labels(ROUTE_CHAT, "coalesced").inc()
                return StreamingResponse(
                    joined.follow(),
                    media_type="text/event-stream",
                    headers={"X-Request-Id": req_id, "X-KVStream-Coalesced": "1"},
                )

        # Claim leadership *before* queueing and pre-flighting. Both of those
        # await, and anything identical arriving in that window has to be able
        # to find this stream -- otherwise every concurrent duplicate becomes its
        # own leader and coalescing never happens under the load it exists for.
        lead_key = key if key and self.stream_coalescer else None
        broadcast = (
            self.stream_coalescer.lead(lead_key)
            if lead_key and self.stream_coalescer
            else StreamBroadcast()
        )

        def _end_broadcast(frame: bytes | None = None) -> None:
            """Close the broadcast before a run exists, telling any followers why."""
            if frame is not None:
                broadcast.publish(frame)
            broadcast.close()
            if lead_key and self.stream_coalescer:
                self.stream_coalescer.finish(lead_key, broadcast)

        try:
            await self._admit(req_id, rc)
        except BaseException as exc:
            # Followers that joined while this request queued were already
            # handed a 200; give them an error frame, not an endless wait.
            detail = getattr(exc, "detail", None) or str(exc) or type(exc).__name__
            _end_broadcast(_sse_error(RuntimeError(f"request was not admitted: {detail}")))
            raise

        # Drift samples time the whole backend request, time to first token
        # included — the same basis calibration measures its baseline on.
        dispatched = time.perf_counter()
        agen = self.backend.chat(
            backend_payload(raw, stream=True, default_max_tokens=self.sent_max_tokens), auth
        )

        # Pre-flight the first chunk so a backend failure returns a real error
        # status before we commit HTTP 200 on the stream.
        try:
            first = await agen.__anext__()
        except StopAsyncIteration:
            # The backend ended the stream normally without sending anything.
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            _end_broadcast(b"data: [DONE]\n\n")
            self.metrics.requests.labels(ROUTE_CHAT, "served").inc()
            return StreamingResponse(_sse_only_done(), media_type="text/event-stream")
        except FoundryClientError as exc:
            # The client's request was wrong. The caller gets the backend's
            # own 4xx and the breaker hears nothing.
            await agen.aclose()
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            _end_broadcast(_sse_error(exc))
            self.metrics.requests.labels(ROUTE_CHAT, "client_error").inc()
            raise
        except Exception as exc:  # noqa: BLE001
            await agen.aclose()
            await self.capacity.release(req_id)
            # Before blaming the backend, check whether the model just went
            # away. Foundry answers a stream for an unloaded model with a
            # dropped connection, and the cached inventory can be up to 30s
            # old — e.g. right after `foundry model unload` or an idle unload.
            refusal = refusal_for(req.model, await self.inventory.snapshot(fresh=True))
            if refusal is not None:
                self.health.release_trial(req_id)
                _end_broadcast(_sse_error(RuntimeError(refusal.message)))
                self.metrics.requests.labels(ROUTE_CHAT, "client_error").inc()
                raise FoundryClientError(
                    400,
                    {
                        "error": {
                            "message": refusal.message,
                            "type": "invalid_request_error",
                            "code": refusal.code,
                            "param": None,
                        }
                    },
                    refusal.message,
                ) from exc
            _end_broadcast(_sse_error(exc))
            self._record_backend_failure("preflight", exc)
            self.metrics.requests.labels(ROUTE_CHAT, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except BaseException:
            # Cancelled while waiting for the first token (client went away).
            await agen.aclose()
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            _end_broadcast(_sse_error(RuntimeError("generation cancelled before the first token")))
            raise

        run = asyncio.create_task(
            self._run_stream(
                req_id,
                req,
                rc,
                first,
                agen,
                broadcast,
                dispatched,
                received,
                cache_key=key,
                lead_key=lead_key,
            ),
            name=f"kvstream-stream-{req_id}",
        )
        self._runs.add(run)
        run.add_done_callback(self._runs.discard)
        broadcast.on_abandoned(run.cancel)

        return StreamingResponse(
            broadcast.follow(), media_type="text/event-stream", headers={"X-Request-Id": req_id}
        )

    async def _run_stream(
        self,
        req_id: str,
        req: ChatCompletionRequest,
        rc: RequestCost,
        first: Token,
        agen: AsyncGenerator[Token, None],
        broadcast: StreamBroadcast,
        dispatched: float,
        received: float,
        *,
        cache_key: str | None,
        lead_key: str | None,
    ) -> None:
        """
        The gateway-owned upstream generation behind one streamed response.

        Everything that must happen exactly once -- settling, releasing the
        reservation, the breaker verdict, caching, unregistering the coalesced
        stream -- happens in ``finally`` here, independent of any client
        connection.
        """
        parts: list[str] = []
        recorded: list[dict] = []
        finish: str | None = None
        usage: dict | None = None
        generated = 0
        completed = False
        cancelled = False
        failure: Exception | None = None
        # Re-check the reservation every few chunks rather than on each one:
        # a lock acquisition per token would cost more than the headroom it
        # reclaims on short responses.
        accounting_stride = 16
        try:
            async for tok in _chain(first, agen):
                if tok.usage is not None:
                    usage = tok.usage
                parts.append(tok.text)
                finish = tok.finish_reason or finish
                if tok.text:
                    generated += 1
                    if tok.finish_reason or generated % accounting_stride == 0:
                        # Settle to actual use only once the generation has
                        # finished; until then hold what it was admitted for.
                        await self._account(
                            req_id, rc, generated, finished=tok.finish_reason is not None
                        )

                payload = _client_chunk(tok, req, req_id)
                if payload is None:
                    continue
                recorded.append(payload)
                broadcast.publish(_sse(payload))
            completed = True
        except asyncio.CancelledError:
            # Every subscriber left. That says nothing about the backend.
            cancelled = True
            raise
        except Exception as exc:  # noqa: BLE001
            self._record_backend_failure("midstream", exc)
            failure = exc
        finally:
            await agen.aclose()
            # Always settle: this calibrates the estimator from real usage
            # even when caching is disabled.
            prompt_tokens, completion_tokens = self._settle(req, "".join(parts), usage)
            if completed:
                self._record_backend_success(
                    time.perf_counter() - dispatched, prompt_tokens + completion_tokens, req.model
                )
            await self._account(req_id, rc, completion_tokens)
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            if completed:
                # End-to-end, as for non-streamed requests: streams are the
                # dominant agent traffic and were missing from this histogram.
                self.metrics.latency.labels(ROUTE_CHAT).observe(time.perf_counter() - received)
            # The cache key and the coalescing key are separate: with coalescing
            # off there is no lead key, and using it here meant streamed
            # responses were never cached.
            if completed and cache_key and finish:
                self._store_stream(cache_key, recorded, rc.prompt_tokens, completion_tokens, finish)
            # Exactly one outcome per request, decided by how it ended. Counting
            # `served` when the first token arrived counted an abandoned stream
            # twice and a stream that failed part-way as served.
            if completed:
                self.metrics.requests.labels(ROUTE_CHAT, "served").inc()
                broadcast.publish(b"data: [DONE]\n\n")
            elif cancelled:
                self.metrics.requests.labels(ROUTE_CHAT, "abandoned").inc()
                broadcast.publish(
                    _sse_error(RuntimeError("generation cancelled: every client disconnected"))
                )
            else:
                self.metrics.requests.labels(ROUTE_CHAT, "error").inc()
                # A truncated response must never end in [DONE]: a client
                # parsing tool-call JSON would take it as complete.
                broadcast.publish(_sse_error(failure or RuntimeError("stream ended early")))
            broadcast.close()
            if lead_key and self.stream_coalescer:
                self.stream_coalescer.finish(lead_key, broadcast)

    def _store_stream(
        self,
        key: str,
        chunks: list[dict],
        prompt_tokens: int,
        completion_tokens: int,
        finish: str,
    ) -> None:
        self._store(
            key,
            CachedResponse(
                chunks=chunks,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                finish_reason=finish,
            ),
        )

    def _store(self, key: str, result: CachedResponse) -> None:
        """
        Cache a response unless it is too big to be worth the room.

        One oversized entry can evict everything useful, so skipping it is
        better for the hit rate than admitting it.
        """
        if not self.cache:
            return
        size = _approx_size(result)
        if size > self.settings.cache.max_entry_bytes:
            self.metrics.cache_skipped.labels("too_large").inc()
            logger.debug("not caching %s: %d bytes exceeds max_entry_bytes", key[:12], size)
            return
        self.cache.put(key, result)

    # -- legacy completions --------------------------------------------

    async def handle_completions(
        self, req: CompletionRequest, raw: dict, auth: dict[str, str] | None = None
    ) -> JSONResponse | StreamingResponse:
        """
        Proxy legacy /v1/completions, admitted on the KV-token budget.

        Same cost basis as chat — prompt tokens plus the generation allowance —
        because it is the same work with an older request shape.
        """
        req_id = f"kvs-{uuid.uuid4().hex[:12]}"
        t0 = time.perf_counter()
        estimator = self.estimators.for_model(req.model)
        prompt_tokens = sum(estimator.estimate_text(t) for t in req.texts)
        rc = RequestCost(
            prompt_tokens=max(1, prompt_tokens),
            max_tokens=req.generation_budget(self.settings.admission.default_max_tokens),
        )

        if req.stream:
            return await self._stream_completions(req_id, rc, raw, auth, t0)

        await self._admit(req_id, rc, ROUTE_COMPLETIONS)
        try:
            status, body = await self.backend.completions(
                fill_max_tokens(raw, self.sent_max_tokens), auth
            )
            self._record_status_verdict("completions", status)
        except FoundryError as exc:
            self._record_backend_failure("completions", exc)
            self.metrics.requests.labels(ROUTE_COMPLETIONS, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)

        outcome = "error" if status >= 400 else "served"
        self.metrics.requests.labels(ROUTE_COMPLETIONS, outcome).inc()
        self.metrics.latency.labels(ROUTE_COMPLETIONS).observe(time.perf_counter() - t0)
        return JSONResponse(body, status_code=status)

    async def _stream_completions(
        self,
        req_id: str,
        rc: RequestCost,
        raw: dict,
        auth: dict[str, str] | None,
        t0: float,
    ) -> StreamingResponse:
        """
        Streamed legacy completion, relayed byte for byte.

        It used to be read as a single JSON body, so a client asking for a
        stream got HTTP 200 with an error object. The upstream call is owned by
        the gateway, as on the chat path, so the reservation is released
        exactly once and a disconnect stops the generation.
        """
        await self._admit(req_id, rc, ROUTE_COMPLETIONS)
        agen = self.backend.completions_stream(fill_max_tokens(raw, self.sent_max_tokens), auth)
        try:
            first = await agen.__anext__()
        except StopAsyncIteration:
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            self.metrics.requests.labels(ROUTE_COMPLETIONS, "served").inc()
            return StreamingResponse(iter(()), media_type="text/event-stream")
        except FoundryClientError:
            await agen.aclose()
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            self.metrics.requests.labels(ROUTE_COMPLETIONS, "client_error").inc()
            raise
        except Exception as exc:  # noqa: BLE001
            await agen.aclose()
            await self.capacity.release(req_id)
            self._record_backend_failure("completions", exc)
            self.metrics.requests.labels(ROUTE_COMPLETIONS, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except BaseException:
            await agen.aclose()
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)
            raise

        broadcast = StreamBroadcast()

        async def relay() -> None:
            completed = False
            cancelled = False
            try:
                broadcast.publish(first)
                async for chunk in agen:
                    broadcast.publish(chunk)
                completed = True
            except asyncio.CancelledError:
                cancelled = True
                raise
            except Exception as exc:  # noqa: BLE001
                self._record_backend_failure("completions", exc)
                # Terminate any half-written SSE frame before the error frame.
                broadcast.publish(b"\n\n" + _sse_error(exc))
            finally:
                await agen.aclose()
                await self.capacity.release(req_id)
                self.health.release_trial(req_id)
                if completed:
                    self.health.record_success()
                    self.metrics.requests.labels(ROUTE_COMPLETIONS, "served").inc()
                    self.metrics.latency.labels(ROUTE_COMPLETIONS).observe(time.perf_counter() - t0)
                elif cancelled:
                    self.metrics.requests.labels(ROUTE_COMPLETIONS, "abandoned").inc()
                else:
                    self.metrics.requests.labels(ROUTE_COMPLETIONS, "error").inc()
                broadcast.close()

        run = asyncio.create_task(relay(), name=f"kvstream-completions-{req_id}")
        self._runs.add(run)
        run.add_done_callback(self._runs.discard)
        broadcast.on_abandoned(run.cancel)
        return StreamingResponse(
            broadcast.follow(), media_type="text/event-stream", headers={"X-Request-Id": req_id}
        )

    # -- embeddings ----------------------------------------------------

    async def handle_embeddings(
        self, req: EmbeddingsRequest, raw: dict, auth: dict[str, str] | None = None
    ) -> JSONResponse:
        """
        Proxy /v1/embeddings, admitted on the KV-token budget.

        Embeddings have an honest cost basis — the input tokens — so they share
        the same budget as chat. There is no generation half and therefore no
        live accounting to do. The client's request and the backend's body both
        pass through untouched.
        """
        req_id = f"kvs-{uuid.uuid4().hex[:12]}"
        t0 = time.perf_counter()
        # Refused before admission and before anything reaches the backend.
        refusal = embedding_refusal(req.model, await self.inventory.snapshot())
        if refusal is not None:
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "client_error").inc()
            return _error_response(400, refusal.message, code=refusal.code)
        texts = req.texts
        estimator = self.estimators.for_model(req.model, KIND_EMBEDDINGS)
        prompt_tokens = sum(estimator.estimate_text(t) for t in texts)
        rc = RequestCost(prompt_tokens=max(1, prompt_tokens), max_tokens=0)

        await self._admit(req_id, rc, ROUTE_EMBEDDINGS)
        try:
            status, body = await self.backend.embeddings(raw, auth)
            self._record_status_verdict("embeddings", status)
        except FoundryError as exc:
            self._record_backend_failure("embeddings", exc)
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await self.capacity.release(req_id)
            self.health.release_trial(req_id)

        if status >= 400:
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "error").inc()
        else:
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "served").inc()
            self._learn_from_embedding_usage(req.model, texts, body)
        self.metrics.latency.labels(ROUTE_EMBEDDINGS).observe(time.perf_counter() - t0)
        return JSONResponse(body, status_code=status)

    def _learn_from_embedding_usage(self, model: str, texts: list[str], body: object) -> None:
        """
        Embedding responses carry real prompt counts too — use them.

        They train the embedding model's own estimator. They used to train the
        chat estimator, which an embedding model's tokenizer does not describe.
        """
        if not isinstance(body, dict):
            return
        usage = body.get("usage")
        if not isinstance(usage, dict):
            return
        actual = usage.get("prompt_tokens")
        if not isinstance(actual, int) or actual <= 0:
            return
        learner = self.estimators.learner(model, KIND_EMBEDDINGS)
        if learner is None:
            return
        learner.observe(sum(len(t) for t in texts), sum(count_units(t) for t in texts), actual)

    # -- audio ---------------------------------------------------------

    async def handle_transcription(
        self, request: Request, auth: dict[str, str] | None = None
    ) -> Response:
        """
        Proxy /v1/audio/transcriptions under its own plain concurrency limit.

        Audio has no token cost measurable at the gateway — a byte count says
        nothing reliable about KV footprint — so it is deliberately *not*
        admitted against the KV-token budget. Saying so is preferable to
        inventing a cost model.
        """
        req_id = f"kvs-{uuid.uuid4().hex[:12]}"
        t0 = time.perf_counter()
        max_bytes = self.settings.routes.audio_max_upload_mb * 1024 * 1024

        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > max_bytes + _MULTIPART_ALLOWANCE:
            self._reject_oversized(int(declared), max_bytes)

        # The body is counted as it arrives, so the limit holds without a
        # Content-Length too (chunked uploads). Checking only after parsing let
        # an unbounded upload be spooled to disk and then read into memory.
        limited = Request(
            request.scope, _limited_receive(request, max_bytes + _MULTIPART_ALLOWANCE)
        )
        try:
            form = await limited.form()
        except _BodyTooLarge:
            self._reject_oversized(None, max_bytes)
        except Exception as exc:  # noqa: BLE001 — malformed multipart
            raise HTTPException(
                status_code=400, detail=f"could not parse multipart body: {exc}"
            ) from exc

        try:
            return await self._transcribe_form(req_id, t0, form, max_bytes, auth)
        finally:
            # Parsed uploads over 1 MB live in temporary files until the form
            # is closed.
            await form.close()

    async def _transcribe_form(
        self,
        req_id: str,
        t0: float,
        form: FormData,
        max_bytes: int,
        auth: dict[str, str] | None,
    ) -> Response:
        upload = form.get("file")
        # A form field is either text or an upload; only an upload has a body.
        # An isinstance check narrows the type on every Starlette version.
        if upload is None or isinstance(upload, str):
            raise HTTPException(
                status_code=400, detail="a `file` part is required for transcription"
            )
        if upload.size is not None and upload.size > max_bytes:
            self._reject_oversized(upload.size, max_bytes)

        data = {
            key: str(value)
            for key, value in form.multi_items()
            if key != "file" and not hasattr(value, "read")
        }

        # Admitted before the file is read into memory, so at most
        # `audio_max_concurrency` uploads are held in memory at once; the
        # waiting ones stay in the parser's temporary files.
        await self._admit(req_id, _ONE_SLOT, ROUTE_AUDIO, self.audio_capacity)
        try:
            content = await upload.read()
            if len(content) > max_bytes:
                self._reject_oversized(len(content), max_bytes)
            files = {
                "file": (
                    getattr(upload, "filename", None) or "audio",
                    content,
                    getattr(upload, "content_type", None) or "application/octet-stream",
                )
            }
            status, body, content_type = await self.backend.transcribe(
                files, data, timeout=self.settings.backend.timeout_seconds, headers=auth
            )
            self._record_status_verdict("transcriptions", status)
        except FoundryError as exc:
            self._record_backend_failure("transcriptions", exc)
            self.metrics.requests.labels(ROUTE_AUDIO, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await self.audio_capacity.release(req_id)
            self.health.release_trial(req_id)

        outcome = "error" if status >= 400 else "served"
        self.metrics.requests.labels(ROUTE_AUDIO, outcome).inc()
        self.metrics.latency.labels(ROUTE_AUDIO).observe(time.perf_counter() - t0)
        return Response(content=body, status_code=status, media_type=content_type)

    def _reject_oversized(self, size: int | None, limit: int) -> NoReturn:
        """413. ``size`` is ``None`` when the upload was cut off before its end."""
        self.metrics.rejected.labels(ROUTE_AUDIO, "too_large").inc()
        self.metrics.requests.labels(ROUTE_AUDIO, "rejected").inc()
        actual = f"is {size / 1024 / 1024:.1f} MB" if size is not None else "exceeds the limit"
        raise HTTPException(
            status_code=413,
            detail=(
                f"audio upload {actual}; the limit is "
                f"{limit // 1024 // 1024} MB (routes.audio_max_upload_mb)"
            ),
        )

    def calibration_age(self) -> float:
        """Age in seconds of the calibrated budget in use, or 0.0 if none is."""
        lookup = self.budget_provenance.get("lookup")
        if not isinstance(lookup, dict):
            return 0.0
        if not str(self.budget_provenance.get("source", "")).startswith("calibration"):
            return 0.0
        age = lookup.get("age_seconds")
        return float(age) if isinstance(age, int | float) else 0.0

    async def begin_shutdown(self) -> int:
        """
        Stop admitting and turn away everything still queued — immediately.

        Must run when shutdown *starts*. Uvicorn waits for open connections
        before it runs the application's lifespan shutdown, and a queued
        request is an open connection; measured, a request queued at Ctrl+C was
        made to wait for its slot and then served, because :meth:`drain` only
        ran once nothing was left to drain. Returns how many were turned away.
        """
        turned_away = await self.capacity.start_draining()
        turned_away += await self.audio_capacity.start_draining()
        if turned_away:
            logger.info("shutdown: turned away %d queued request(s)", turned_away)
        return turned_away

    async def drain(self, timeout: float) -> None:
        """
        Stop admitting, turn away anything still queued, let the rest finish.

        A queued request has not started and loses nothing by being told to
        retry; one already streaming would lose real work, so it is given
        ``timeout`` seconds to complete before the process exits.
        """
        turned_away = await self.capacity.start_draining()
        turned_away += await self.audio_capacity.start_draining()
        if turned_away:
            logger.info("shutdown: turned away %d queued request(s)", turned_away)

        deadline = time.monotonic() + max(0.0, timeout)
        while self.capacity.in_flight or self.audio_capacity.in_flight:
            if time.monotonic() >= deadline:
                logger.warning(
                    "shutdown: %d request(s) still in flight after %.0fs; exiting anyway",
                    self.capacity.stats()["active"] + self.audio_capacity.stats()["active"],
                    timeout,
                )
                break
            await asyncio.sleep(0.05)
        else:
            logger.info("shutdown: all in-flight requests completed")
        await self._stop_runs()

    async def _stop_runs(self) -> None:
        """
        End the gateway-owned upstream generations before the client closes.

        They belong to no connection, so nothing else stops them: left running
        past the drain, they failed against the closed HTTP client and each
        failure was counted against the backend. A cancelled run still settles
        in its ``finally`` — releases its reservation and sends its subscribers
        an error frame rather than ``[DONE]``.
        """
        # One turn of the loop first: a run whose reservation is already
        # released has only synchronous steps left and should finish, not be
        # interrupted.
        await asyncio.sleep(0)
        pending = [run for run in self._runs if not run.done()]
        if not pending:
            return
        logger.warning("shutdown: cancelling %d unfinished stream(s)", len(pending))
        for run in pending:
            run.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def aclose(self) -> None:
        await self.inventory.aclose()
        await self.backend.aclose()


# ----------------------------------------------------------------------
# SSE / JSON helpers
# ----------------------------------------------------------------------


# Room for multipart framing and the small text fields sent with the file
# (model, language, response_format...). Only the body as a whole can be counted
# while it streams in; the file itself is checked exactly once it is parsed.
_MULTIPART_ALLOWANCE = 1024 * 1024


class _BodyTooLarge(Exception):
    """The request body passed its byte limit while it was being received."""


def _limited_receive(request: Request, limit: int) -> Receive:
    """``request``'s ASGI receive, raising :class:`_BodyTooLarge` past ``limit`` bytes."""
    receive = request.receive
    received = 0

    async def limited() -> Message:
        nonlocal received
        message = await receive()
        if message["type"] == "http.request":
            received += len(message.get("body", b""))
            if received > limit:
                raise _BodyTooLarge
        return message

    return limited


def _response_text(body: dict) -> str:
    """
    All generated text in a non-streamed body, for cost estimation only.

    Tool-call arguments count: they are generated tokens like any other, and an
    agent turn that emits only a function call would otherwise look free.
    """
    pieces: list[str] = []
    for choice in body.get("choices") or []:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message")
        if not isinstance(message, dict):
            continue
        if isinstance(message.get("content"), str):
            pieces.append(message["content"])
        for call in message.get("tool_calls") or []:
            if isinstance(call, dict):
                args = (call.get("function") or {}).get("arguments")
                if isinstance(args, str):
                    pieces.append(args)
    return "".join(pieces)


def _response_finish(body: dict) -> str:
    for choice in body.get("choices") or []:
        if isinstance(choice, dict) and choice.get("finish_reason"):
            return str(choice["finish_reason"])
    return "stop"


async def _chain(first: Token, rest: AsyncIterator[Token]) -> AsyncIterator[Token]:
    """Re-attach the pre-flighted first chunk to the front of the stream."""
    yield first
    async for tok in rest:
        yield tok


def _client_chunk(tok: Token, req: ChatCompletionRequest, req_id: str) -> dict | None:
    """
    The chunk to send onward, or ``None`` to swallow it.

    KVStream always asks the backend for usage because its own accounting needs
    it. A client that did not ask must not suddenly start receiving usage
    chunks it never requested, so the extra data is consumed here rather than
    leaked into someone else's stream.
    """
    raw = tok.raw
    if raw is None:
        # A backend (or test double) that yields a bare view rather than a raw
        # chunk still gets a well-formed one.
        return {
            "id": req_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": req.model,
            "choices": [
                {"index": 0, "delta": {"content": tok.text}, "finish_reason": tok.finish_reason}
            ],
        }
    if tok.usage is not None and not req.wants_usage:
        if tok.usage_only:
            return None
        return {k: v for k, v in raw.items() if k != "usage"}
    return raw


@dataclass(frozen=True)
class CacheDirective:
    """What a caller asked KVStream to do with the cache for one request."""

    read: bool = True
    write: bool = True
    # `only-if-cached`: answer from the cache or not at all (RFC 9111 §5.2.1.7:
    # a cache that cannot satisfy it responds 504). It used to read the cache
    # and then, on a miss, call the backend anyway.
    only_if_cached: bool = False


def cache_directive(headers: Mapping[str, str], honour: bool = True) -> CacheDirective:
    """
    Read `Cache-Control` / `x-kvstream-cache` off one request.

    Caching changes response semantics, so a caller has to be able to opt a
    single request out without an operator reconfiguring the gateway.
    `no-store` skips the cache in both directions; `no-cache` re-fetches but
    still refreshes the entry, which is the standard meaning of both.
    """
    if not honour:
        return CacheDirective()
    raw = " ".join(
        value.lower()
        for name in ("cache-control", "x-kvstream-cache")
        if (value := headers.get(name))
    )
    if not raw:
        return CacheDirective()
    if "no-store" in raw:
        return CacheDirective(read=False, write=False)
    if "no-cache" in raw or "bypass" in raw or "refresh" in raw:
        return CacheDirective(read=False, write=True)
    if "only-if-cached" in raw:
        return CacheDirective(read=True, write=False, only_if_cached=True)
    return CacheDirective()


def _result_headers(req_id: str, result: CachedResponse) -> dict[str, str]:
    headers = {"X-Request-Id": req_id}
    if result.usage_estimated:
        # The backend gave no token counts, so the `usage` block is KVStream's
        # own estimate. Say so rather than letting it pass as measured.
        headers["X-KVStream-Usage"] = "estimated"
    return headers


def _runtime_profile(provenance: dict) -> dict:
    """
    What kind of runtime the calibration sweep found, if it ran.

    Surfaced beside the budget because the number alone is not actionable: the
    same budget means different things on a runtime that batches and one that
    serialises, and only the sweep can tell an operator which they have.
    """
    lookup = provenance.get("lookup")
    record = lookup.get("record") if isinstance(lookup, dict) else None
    profile = record.get("runtime_profile") if isinstance(record, dict) else None
    if isinstance(profile, dict):
        return profile
    return {"regime": "unknown", "detail": "no calibration record; run `kvstream calibrate`"}


def _approx_size(result: CachedResponse) -> int:
    """Rough serialized size of a cache entry, for the size cap."""
    try:
        if result.body is not None:
            return len(json.dumps(result.body, default=str))
        return sum(len(json.dumps(chunk, default=str)) for chunk in result.chunks)
    except (TypeError, ValueError):  # pragma: no cover — defensive
        return 0


def _sse(payload: dict) -> bytes:
    return f"data: {json.dumps(payload)}\n\n".encode()


def _sse_error(exc: Exception) -> bytes:
    payload = {"error": {"message": str(exc), "type": "server_error"}}
    return f"data: {json.dumps(payload)}\n\n".encode()


async def _sse_only_done() -> AsyncIterator[bytes]:
    yield b"data: [DONE]\n\n"


def _cached_stream(r: CachedResponse) -> AsyncIterator[bytes]:
    """Replay the exact chunks that were recorded, then close the stream."""

    async def gen() -> AsyncIterator[bytes]:
        for chunk in r.chunks:
            yield _sse(chunk)
        yield b"data: [DONE]\n\n"

    return gen()


# ----------------------------------------------------------------------
# Errors — OpenAI-shaped, so OpenAI SDK clients can read them
# ----------------------------------------------------------------------

_ERROR_TYPES = {
    400: "invalid_request_error",
    404: "not_found_error",
    422: "invalid_request_error",
    502: "upstream_error",
    503: "overloaded_error",
}


def _error_body(status_code: int, message: str, code: str | None = None) -> dict:
    return {
        "error": {
            "message": message,
            "type": _ERROR_TYPES.get(status_code, "api_error"),
            "code": code,
            "param": None,
        }
    }


def _error_response(status_code: int, message: str, code: str | None = None) -> JSONResponse:
    headers = {}
    if status_code == 503:
        # Backpressure is only useful if the client can act on it.
        headers["Retry-After"] = "1"
    return JSONResponse(
        status_code=status_code, content=_error_body(status_code, message, code), headers=headers
    )


# ----------------------------------------------------------------------
# App factory
# ----------------------------------------------------------------------


def build_app(settings: Settings | None = None) -> FastAPI:
    _guard_single_process()
    settings = settings or Settings.load()
    gw = Gateway(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            await gw.startup()
        except Exception:  # noqa: BLE001 — startup diagnostics must not block serving
            logger.exception("startup probe failed; continuing with defaults")
            gw.backend_healthy = False
            gw.settle_environment_after_failed_startup()
        yield
        await gw.drain(settings.drain_timeout_seconds)
        await gw.aclose()

    app = FastAPI(
        title="KVStream",
        description="Admission-control gateway for Microsoft Foundry Local.",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.gateway = gw

    @app.middleware("http")
    async def _identify_gateway(request: Request, call_next):
        """
        Stamp every response so gateways can recognise each other.

        Discovery scans localhost for anything answering `/v1/models`; another
        KVStream answers it too. This header is the one identification signal we
        control, and it is what stops a gateway adopting a gateway as its
        backend and building a proxy loop.
        """
        response = await call_next(request)
        response.headers[KVSTREAM_HEADER] = __version__
        return response

    @app.exception_handler(HTTPException)
    async def _http_error(_: Request, exc: HTTPException) -> JSONResponse:
        return _error_response(exc.status_code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0] if exc.errors() else {}
        loc = ".".join(str(p) for p in first.get("loc", []) if p != "body")
        message = first.get("msg", "invalid request")
        return _error_response(400, f"{loc}: {message}" if loc else message)

    @app.exception_handler(FoundryClientError)
    async def _backend_client_error(_: Request, exc: FoundryClientError) -> JSONResponse:
        # The backend refused the *request* (e.g. an unknown model alias).
        # Pass its own status and body through: a 502 would blame the backend
        # for the caller's mistake, and the caller could not fix what it
        # cannot see.
        return JSONResponse(exc.body, status_code=exc.status_code)

    @app.exception_handler(FoundryError)
    async def _backend_error(_: Request, exc: FoundryError) -> JSONResponse:
        gw.metrics.backend_errors.labels("nonstreaming").inc()
        return _error_response(502, str(exc))

    @app.get("/health")
    async def health(probe: bool = False) -> JSONResponse:
        """
        Liveness *and* readiness, reported separately.

        Answering `/v1/models` only proves a process is alive. Measured against
        a real Foundry Local, that endpoint returned 200 in 4ms for minutes
        while the runtime could not complete a 4-token generation — so an
        orchestrator keyed on liveness alone would have kept routing traffic
        into a dead backend. Readiness is established by asking for a token,
        cached so the check does not become load. `?probe=true` forces a fresh
        one.
        """
        reachable = await gw.health.check_reachable()
        model = await gw.ensure_model() if reachable else gw.settings.backend.model
        if model:
            readiness = await gw.health.check_ready(model, force=probe)
        else:
            # No model to probe with. Reporting "ready" would be a guess, and
            # so would probing with a made-up model id. Recorded as *not
            # probed* (no timestamp): the readiness cache must not hold this
            # verdict, or a model loaded a second later would go unprobed for
            # the whole cache interval.
            readiness = Readiness(ready=False, detail=f"no model to probe — {gw.model_source}")
            gw.health.readiness = readiness
        breaker = gw.health.breaker

        serving = reachable and readiness.ready and breaker.state != "open"
        gw.backend_healthy = reachable
        gw.metrics.backend_up.set(1 if reachable else 0)
        gw.metrics.backend_ready.set(1 if readiness.ready else 0)
        gw.metrics.circuit_state.set(_CIRCUIT_STATES.get(breaker.state, 0))

        body = {
            "status": "ok" if serving else "degraded",
            # Kept for compatibility, and it means liveness — which is exactly
            # the ambiguity that caused the problem, so it is now spelled out.
            "backend_healthy": reachable,
            "backend_reachable": reachable,
            "backend_serving": serving,
            "readiness": readiness.as_dict(),
            "circuit_breaker": breaker.as_dict(),
            "drift": gw.drift.stats(),
            "backend_url": gw.backend.base_url,
            "model": model,
            "model_source": gw.model_source,
            "version": __version__,
        }
        if not reachable:
            body["hint"] = gw.backend.unreachable_hint()
        elif not model:
            body["hint"] = gw.model_source
        elif not serving and "not loaded" in readiness.detail:
            body["hint"] = (
                f"Foundry Local is running but {model!r} is not loaded. "
                "Load it with `foundry model load`."
            )
        elif not serving:
            body["hint"] = (
                "The backend is reachable but not serving: "
                f"{readiness.detail}. Restarting Foundry Local usually clears a "
                "runtime that has exhausted its KV cache."
            )
        # A degraded gateway must say so in the status line: orchestrators,
        # load balancers and `docker healthcheck` all key on the code, not JSON.
        return JSONResponse(body, status_code=200 if serving else 503)

    @app.get("/status")
    async def status() -> dict:
        return {
            "version": __version__,
            "admission": gw.capacity.stats(),
            "audio_admission": gw.audio_capacity.stats(),
            "budget_source": gw.budget_provenance,
            # A re-resolved budget waiting for the gateway to go idle.
            "budget_pending": (
                gw._pending_capacity.as_dict() if gw._pending_capacity is not None else None
            ),
            "runtime_profile": _runtime_profile(gw.budget_provenance),
            "calibration_key": gw.calibration_key.as_dict(),
            "model": {
                "id": gw.settings.backend.model,
                "source": gw.model_source,
                "inventory": gw.inventory.current.as_dict(),
            },
            "backend": gw.backend.stats(),
            "backend_health": gw.health.stats(),
            "drift": gw.drift.stats(),
            "routes": {
                "chat": True,
                "models": True,
                "embeddings": settings.routes.embeddings,
                "transcriptions": settings.routes.transcriptions,
                "completions": settings.routes.completions,
            },
            "token_estimator": gw.estimator.stats(),
            "token_estimators": gw.estimators.stats(),
            "cache": gw.cache.stats() if gw.cache else {"enabled": False},
            "coalescer": (
                {
                    "enabled": True,
                    "inflight": gw.coalescer.inflight,
                    "coalesced_total": gw.coalescer.coalesced_total,
                    "streaming": (gw.stream_coalescer.stats() if gw.stream_coalescer else None),
                }
                if gw.coalescer
                else {"enabled": False}
            ),
            "model_geometry": gw.geometry.stats(),
        }

    def _auth(request: Request) -> dict[str, str] | None:
        """
        The caller's Authorization header, when forwarding is enabled.

        Foundry Local does not require auth, but KVStream may sit behind a
        gateway that does its own per-caller authentication — passing the
        header through keeps that intact instead of silently dropping it.
        """
        if not settings.backend.forward_authorization:
            return None
        header = request.headers.get("authorization")
        return {"Authorization": header} if header else None

    @app.get("/v1/models")
    async def models() -> JSONResponse:
        # `None` means the backend could not be read, which is not the same as
        # "no models": answering 200 with an empty list told clients there was
        # nothing to use when the truth was that Foundry Local was unreachable.
        ids = await gw._read_downloaded()
        if ids is None:
            return _error_response(
                502, f"could not read models from Foundry Local at {gw.backend.base_url}"
            )
        return JSONResponse({"object": "list", "data": [{"id": m, "object": "model"} for m in ids]})

    @app.get("/v1/models/{model_id}")
    async def model_detail(model_id: str, request: Request):
        status, body = await gw.backend.get_model(model_id, _auth(request))
        return JSONResponse(body, status_code=status)

    @app.get("/metrics")
    async def metrics() -> Response:
        from prometheus_client import CONTENT_TYPE_LATEST

        backend_stats = gw.backend.stats() if hasattr(gw.backend, "stats") else None
        gw.metrics.sync_gauges(
            gw.capacity.stats(),
            gw.estimator.stats(),
            backend_stats,
            gw.audio_capacity.stats(),
            gw.calibration_age(),
        )
        gw.metrics.circuit_state.set(_CIRCUIT_STATES.get(gw.health.breaker.state, 0))
        gw.metrics.backend_ready.set(1 if gw.health.readiness.ready else 0)
        # Last known reachability (startup, /health, completed requests): it
        # used to change only when /health was called, so a scrape of /metrics
        # alone reported a working backend as down.
        gw.metrics.backend_up.set(1 if gw.health.reachable else 0)
        gw.metrics.drift_ratio.set(gw.drift.ratio)  # 0 = unknown, never a stale value
        return Response(gw.metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/chat/completions")
    async def chat_completions(req: ChatCompletionRequest, request: Request):
        # `req` is the typed overlay KVStream needs; `raw` is the client's own
        # object, forwarded untouched. Starlette caches the body, so reading it
        # again here costs nothing.
        raw = await request.json()
        req_id = f"kvs-{uuid.uuid4().hex[:12]}"
        rc, _ = gw._cost(req)
        auth = _auth(request)
        # Only a credential that is actually forwarded changes who the backend
        # answers, so only that one separates cache and coalescing keys.
        caller = auth.get("Authorization") if auth else None
        key = request_key(req, raw, caller) if req.deterministic else None
        directive = cache_directive(request.headers, settings.cache.respect_request_headers)

        # Cache lookup (deterministic requests only). The key includes `stream`,
        # so an entry is only ever replayed in the shape it was recorded in.
        if key and gw.cache and directive.read:
            hit = gw.cache.get(key)
            if hit is not None:
                gw.metrics.cache_hits.inc()
                gw.metrics.requests.labels(ROUTE_CHAT, "cache_hit").inc()
                if req.stream:
                    return StreamingResponse(
                        _cached_stream(hit),
                        media_type="text/event-stream",
                        headers={"X-Request-Id": req_id},
                    )
                return JSONResponse(hit.body or {}, headers=_result_headers(req_id, hit))

        if directive.only_if_cached:
            # Nothing stored for this request (cache off, not a deterministic
            # request, or simply a miss), and the caller forbade asking the
            # backend.
            gw.metrics.requests.labels(ROUTE_CHAT, "cache_miss").inc()
            return _error_response(
                504,
                "not in the response cache, and Cache-Control: only-if-cached "
                "forbids asking the backend",
                code="cache_miss",
            )

        write_key = key if directive.write else None
        if req.stream:
            refusal = await gw.stream_refusal(req.model)
            if refusal is not None:
                gw.metrics.requests.labels(ROUTE_CHAT, "client_error").inc()
                return _error_response(400, refusal.message, code=refusal.code)
            return await gw.handle_streaming(req_id, req, raw, rc, write_key, auth)
        return await gw.handle_nonstreaming(req_id, req, raw, rc, write_key, auth)

    # Proposal §8.4: a gateway that admits only chat leaves the other paths
    # unprotected, and clients simply route around it.
    if settings.routes.embeddings:

        @app.post("/v1/embeddings")
        async def embeddings(req: EmbeddingsRequest, request: Request):
            return await gw.handle_embeddings(req, await request.json(), _auth(request))

    if settings.routes.transcriptions:

        @app.post("/v1/audio/transcriptions")
        async def transcriptions(request: Request):
            return await gw.handle_transcription(request, _auth(request))

    if settings.routes.completions:

        @app.post("/v1/completions")
        async def completions(req: CompletionRequest, request: Request):
            return await gw.handle_completions(req, await request.json(), _auth(request))

    return app
