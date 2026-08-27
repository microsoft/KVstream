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
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse

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
from kvstream.admission.drift import DriftMonitor, baseline_from_provenance
from kvstream.admission.geometry import GeometryRegistry, find_geometry
from kvstream.backend import FoundryClient, foundry_cli
from kvstream.backend.discovery import KVSTREAM_HEADER
from kvstream.backend.foundry import FoundryError, Token
from kvstream.backend.foundry_cli import FoundryCli
from kvstream.backend.health import BackendHealth, BackendUnavailable, CircuitBreaker
from kvstream.cache import (
    CachedResponse,
    Coalescer,
    ResponseCache,
    StreamCoalescer,
    request_key,
)
from kvstream.config import Settings
from kvstream.models import (
    ChatCompletionRequest,
    CompletionRequest,
    EmbeddingsRequest,
    backend_payload,
)
from kvstream.observability import Metrics
from kvstream.tokenization import TokenEstimator, count_units, message_chars, message_units
from kvstream.version import __version__

logger = logging.getLogger("kvstream.app")

ROUTE_CHAT = "chat"
ROUTE_EMBEDDINGS = "embeddings"
ROUTE_AUDIO = "transcriptions"
ROUTE_COMPLETIONS = "completions"

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
        self.geometry = GeometryRegistry(settings.backend.model)
        self.geometry.load_config(settings.models)
        self.calibration_key = calibration_key_for(settings.backend)
        self.capacity: CapacityManager
        self.capacity, self.budget_provenance = build_capacity_manager(
            settings.admission, self.calibration_key
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
        self.estimator = TokenEstimator(
            settings.admission.chars_per_token,
            safety_factor=settings.admission.token_safety_factor,
            learn=settings.admission.learn_token_ratio,
        )
        # Liveness, readiness and the circuit breaker: three lessons from one
        # measured stall, where /v1/models answered in 4ms while the runtime
        # could not finish a 4-token generation.
        self.health = BackendHealth(
            self._backend,
            probe_readiness=settings.backend.probe_readiness,
            readiness_interval=settings.backend.readiness_interval_seconds,
            readiness_timeout=settings.backend.readiness_timeout_seconds,
            breaker=CircuitBreaker(
                failure_threshold=settings.backend.circuit_breaker_failures,
                reset_seconds=settings.backend.circuit_breaker_reset_seconds,
                enabled=settings.backend.circuit_breaker,
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
        self.backend_healthy = False
        logger.info(
            "admission: %s budget=%d (%s) reserve_ratio=%.2f | cache=%s | coalesce=%s",
            self.capacity.unit, self.capacity.budget,
            self.budget_provenance.get("source"), self.capacity.reserve_ratio,
            self.cache is not None, self.coalescer is not None,
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
        key = calibration_key_for(
            self.settings.backend,
            cli_generation=cli.generation,
            backend_version=cli.version or UNKNOWN,
        )
        if key != self.calibration_key:
            self.calibration_key = key
            budget, unit, provenance = resolve_budget(self.settings.admission, key)
            self.budget_provenance = provenance
            if (budget, unit) != (self.capacity.budget, self.capacity.unit):
                logger.info(
                    "re-resolved admission budget after CLI detection: %s %d (%s)",
                    unit, budget, provenance.get("source"),
                )
                self.capacity, self.budget_provenance = build_capacity_manager(
                    self.settings.admission, key
                )
                self.metrics.budget.set(self.capacity.budget)

        self.drift = DriftMonitor(
            baseline_from_provenance(self.budget_provenance),
            warn_ratio=self.settings.admission.drift_warn_ratio,
            min_samples=self.settings.admission.drift_min_samples,
        )
        self.backend_healthy = await self.health.check_reachable()
        if not self.backend_healthy:
            logger.warning(
                "Foundry Local is not reachable at %s. %s",
                self.backend.base_url, self.backend.unreachable_hint(),
            )
        await self._warn_if_model_unknown(cli)
        await self._learn_geometry(cli)

    async def _learn_geometry(self, cli: FoundryCli) -> None:
        """
        Fill in KV geometry the operator did not declare, best-effort.

        `foundry model show --output json` is unverified, so a miss is silent
        and the model keeps its neutral weight of 1.0 — costed exactly as it
        would have been without any geometry at all.
        """
        if not cli.available:
            return
        anchor = self.settings.backend.model
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
        Check the configured model against Foundry Local's own catalog.

        `foundry model list --output json` is the metadata source the proposal
        names (§8.2). A configured model the runtime has never heard of is a
        typo, and saying so beats an opaque 404 on the first inference call.
        """
        if not cli.available:
            return
        catalog = await foundry_cli.list_models(
            cli, self.settings.backend.foundry_cli_timeout_seconds
        )
        if not catalog:
            return
        if self.settings.backend.model not in catalog:
            logger.warning(
                "configured model %r is not in Foundry Local's catalog (%s). "
                "Load it with `foundry model load %s`, or set backend.model to one of those.",
                self.settings.backend.model,
                ", ".join(catalog[:8]) + ("…" if len(catalog) > 8 else ""),
                self.settings.backend.model,
            )

    # -- cost / admission ---------------------------------------------

    def _cost(self, req: ChatCompletionRequest) -> tuple[RequestCost, int]:
        """Return (cost model, admission cost)."""
        prompt_tokens = self.estimator.estimate_messages(req.messages)
        rc = RequestCost(
            prompt_tokens=prompt_tokens,
            max_tokens=req.generation_budget(self.settings.admission.default_max_tokens),
            kv_weight=self.geometry.weight_for(req.model),
        )
        return rc, self.capacity.cost_of(rc)

    def _settle(
        self,
        req: ChatCompletionRequest,
        estimated_prompt: int,
        completion_text: str,
        usage: dict | None,
    ) -> tuple[int, int]:
        """
        Reconcile estimates against the backend's real counts.

        When ``usage`` is present the real prompt count also calibrates the
        estimator, so future admission costs converge on the model's actual
        tokenizer behaviour. Returns the (prompt, completion) counts to report.
        """
        prompt_tokens = estimated_prompt
        completion_tokens = self.estimator.estimate_text(completion_text)
        if usage:
            actual_prompt = usage.get("prompt_tokens")
            actual_completion = usage.get("completion_tokens")
            if isinstance(actual_prompt, int) and actual_prompt > 0:
                self.estimator.observe(
                    message_chars(req.messages), message_units(req.messages), actual_prompt
                )
                prompt_tokens = actual_prompt
            if isinstance(actual_completion, int) and actual_completion >= 0:
                completion_tokens = actual_completion
        return prompt_tokens, completion_tokens

    async def _admit(
        self,
        req_id: str,
        cost: int,
        route: str = ROUTE_CHAT,
        manager: CapacityManager | None = None,
    ) -> None:
        limiter = manager or self.capacity
        # Fail fast before queueing. Without this a stalled backend is
        # rediscovered one request at a time: each arrival waits the full
        # backend timeout, and the admission queue fills with work that can
        # never complete.
        try:
            self.health.guard()
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
            await limiter.admit(req_id, cost)
            self.metrics.admission_wait.labels(route).observe(time.perf_counter() - t0)
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

    def _record_backend_success(self, latency_seconds: float, tokens: int) -> None:
        """One healthy round trip: reset the breaker and feed drift detection."""
        self.health.record_success()
        self.metrics.circuit_state.set(0)
        self.drift.observe(latency_seconds, tokens)
        if self.drift.ratio:
            self.metrics.drift_ratio.set(self.drift.ratio)

    async def _account(self, req_id: str, rc: RequestCost, generated_tokens: int) -> None:
        """
        Update a live reservation to what the request actually occupies.

        Called as generation progresses and again the moment it finishes, so
        unused headroom returns to the budget before the response has finished
        being written back to the client.
        """
        delta = await self.capacity.adjust(
            req_id, self.capacity.live_cost(rc, generated_tokens)
        )
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
        cost: int,
        auth: dict[str, str] | None,
    ) -> CachedResponse:
        """Non-streamed path: forward as non-stream so `usage` comes back directly."""
        await self._admit(req_id, cost)
        started = time.perf_counter()
        try:
            try:
                body = await self.backend.chat_once(
                    backend_payload(raw, stream=False), auth
                )
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
            prompt_tokens, completion_tokens = self._settle(
                req, rc.prompt_tokens, _response_text(body), usage
            )
            self._record_backend_success(
                time.perf_counter() - started, prompt_tokens + completion_tokens
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

    async def handle_nonstreaming(
        self,
        req_id: str,
        req: ChatCompletionRequest,
        raw: dict,
        rc: RequestCost,
        cost: int,
        key: str | None,
        auth: dict[str, str] | None = None,
    ) -> JSONResponse:
        t0 = time.perf_counter()

        async def produce() -> CachedResponse:
            return await self._generate_full(req_id, req, raw, rc, cost, auth)

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
        cost: int,
        key: str | None,
        auth: dict[str, str] | None = None,
    ) -> StreamingResponse:
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
        # to find this stream — otherwise every concurrent duplicate becomes its
        # own leader and coalescing never happens under the load it exists for.
        broadcast = (
            self.stream_coalescer.lead(key) if key and self.stream_coalescer else None
        )

        def _abandon_lead() -> None:
            if broadcast is not None and self.stream_coalescer and key:
                broadcast.close()
                self.stream_coalescer.finish(key, broadcast)

        try:
            await self._admit(req_id, cost)
        except BaseException:
            _abandon_lead()
            raise

        agen = self.backend.chat(backend_payload(raw, stream=True), auth)

        # Pre-flight the first chunk so a backend failure returns a real error
        # status before we commit HTTP 200 on the stream.
        try:
            first = await agen.__anext__()
        except StopAsyncIteration:
            await self.capacity.release(req_id)
            _abandon_lead()
            self.metrics.requests.labels(ROUTE_CHAT, "served").inc()
            return StreamingResponse(_sse_only_done(), media_type="text/event-stream")
        except Exception as exc:  # noqa: BLE001
            await agen.aclose()
            await self.capacity.release(req_id)
            # Followers have already been handed a 200, so they cannot be given
            # an error status — send them the SSE error frame instead of
            # tearing their stream apart.
            if broadcast is not None:
                broadcast.publish(_sse_error(exc))
            _abandon_lead()
            self._record_backend_failure("preflight", exc)
            self.metrics.requests.labels(ROUTE_CHAT, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        self.metrics.requests.labels(ROUTE_CHAT, "served").inc()
        stream_started = time.perf_counter()

        async def body() -> AsyncIterator[bytes]:
            parts: list[str] = []
            recorded: list[dict] = []
            finish: str | None = None
            usage: dict | None = None
            generated = 0
            failure: BaseException | None = None
            # Re-check the reservation every few chunks rather than on each one:
            # a lock acquisition per token would cost more than the headroom it
            # reclaims on short responses.
            accounting_stride = 16
            try:
                stream = _chain(first, agen)
                async for tok in stream:
                    if tok.usage is not None:
                        usage = tok.usage
                    parts.append(tok.text)
                    finish = tok.finish_reason or finish
                    if tok.text:
                        generated += 1
                        if tok.finish_reason or generated % accounting_stride == 0:
                            await self._account(req_id, rc, generated)

                    payload = _client_chunk(tok, req, req_id)
                    if payload is None:
                        continue
                    recorded.append(payload)
                    frame = _sse(payload)
                    if broadcast is not None:
                        broadcast.publish(frame)
                    yield frame
            except Exception as exc:  # noqa: BLE001
                self._record_backend_failure("midstream", exc)
                failure = exc
                frame = _sse_error(exc)
                if broadcast is not None:
                    broadcast.publish(frame)
                yield frame
            finally:
                await agen.aclose()
                # Always settle: this calibrates the estimator from real usage
                # even when caching is disabled.
                _, completion_tokens = self._settle(
                    req, rc.prompt_tokens, "".join(parts), usage
                )
                if failure is None:
                    self._record_backend_success(
                        time.perf_counter() - stream_started,
                        rc.prompt_tokens + completion_tokens,
                    )
                await self._account(req_id, rc, completion_tokens)
                await self.capacity.release(req_id)
                if key and finish and failure is None:
                    self._store_stream(key, recorded, rc.prompt_tokens, completion_tokens, finish)
                if broadcast is not None and self.stream_coalescer:
                    broadcast.publish(b"data: [DONE]\n\n")
                    broadcast.close()
                    self.stream_coalescer.finish(key or "", broadcast)
                yield b"data: [DONE]\n\n"

        return StreamingResponse(
            body(), media_type="text/event-stream", headers={"X-Request-Id": req_id}
        )

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
    ) -> JSONResponse:
        """
        Proxy legacy /v1/completions, admitted on the KV-token budget.

        Same cost basis as chat — prompt tokens plus the generation allowance —
        because it is the same work with an older request shape.
        """
        req_id = f"kvs-{uuid.uuid4().hex[:12]}"
        t0 = time.perf_counter()
        prompt_tokens = sum(self.estimator.estimate_text(t) for t in req.texts)
        rc = RequestCost(
            prompt_tokens=max(1, prompt_tokens),
            max_tokens=req.generation_budget(self.settings.admission.default_max_tokens),
        )

        await self._admit(req_id, self.capacity.cost_of(rc), ROUTE_COMPLETIONS)
        try:
            status, body = await self.backend.completions(raw, auth)
        except FoundryError as exc:
            self._record_backend_failure("completions", exc)
            self.metrics.requests.labels(ROUTE_COMPLETIONS, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await self.capacity.release(req_id)

        outcome = "error" if status >= 400 else "served"
        self.metrics.requests.labels(ROUTE_COMPLETIONS, outcome).inc()
        self.metrics.latency.labels(ROUTE_COMPLETIONS).observe(time.perf_counter() - t0)
        return JSONResponse(body, status_code=status)

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
        texts = req.texts
        prompt_tokens = sum(self.estimator.estimate_text(t) for t in texts)
        rc = RequestCost(prompt_tokens=max(1, prompt_tokens), max_tokens=0)

        await self._admit(req_id, self.capacity.cost_of(rc), ROUTE_EMBEDDINGS)
        try:
            status, body = await self.backend.embeddings(raw, auth)
        except FoundryError as exc:
            self._record_backend_failure("embeddings", exc)
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await self.capacity.release(req_id)

        if status >= 400:
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "error").inc()
        else:
            self.metrics.requests.labels(ROUTE_EMBEDDINGS, "served").inc()
            self._learn_from_embedding_usage(texts, body)
        self.metrics.latency.labels(ROUTE_EMBEDDINGS).observe(time.perf_counter() - t0)
        return JSONResponse(body, status_code=status)

    def _learn_from_embedding_usage(self, texts: list[str], body: object) -> None:
        """Embedding responses carry real prompt counts too — use them."""
        if not isinstance(body, dict):
            return
        usage = body.get("usage")
        if not isinstance(usage, dict):
            return
        actual = usage.get("prompt_tokens")
        if not isinstance(actual, int) or actual <= 0:
            return
        self.estimator.observe(
            sum(len(t) for t in texts), sum(count_units(t) for t in texts), actual
        )

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
        if declared and declared.isdigit() and int(declared) > max_bytes:
            self._reject_oversized(int(declared), max_bytes)

        try:
            form = await request.form()
        except Exception as exc:  # noqa: BLE001 — malformed multipart
            raise HTTPException(
                status_code=400, detail=f"could not parse multipart body: {exc}"
            ) from exc

        upload = form.get("file")
        if upload is None or not hasattr(upload, "read"):
            raise HTTPException(
                status_code=400, detail="a `file` part is required for transcription"
            )

        content = await upload.read()  # type: ignore[union-attr]
        if len(content) > max_bytes:
            self._reject_oversized(len(content), max_bytes)

        data = {
            key: str(value)
            for key, value in form.multi_items()
            if key != "file" and not hasattr(value, "read")
        }
        files = {
            "file": (
                getattr(upload, "filename", None) or "audio",
                content,
                getattr(upload, "content_type", None) or "application/octet-stream",
            )
        }

        await self._admit(req_id, 1, ROUTE_AUDIO, self.audio_capacity)
        try:
            status, body, content_type = await self.backend.transcribe(
                files, data, timeout=self.settings.backend.timeout_seconds, headers=auth
            )
        except FoundryError as exc:
            self._record_backend_failure("transcriptions", exc)
            self.metrics.requests.labels(ROUTE_AUDIO, "error").inc()
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await self.audio_capacity.release(req_id)

        outcome = "error" if status >= 400 else "served"
        self.metrics.requests.labels(ROUTE_AUDIO, outcome).inc()
        self.metrics.latency.labels(ROUTE_AUDIO).observe(time.perf_counter() - t0)
        return Response(content=body, status_code=status, media_type=content_type)

    def _reject_oversized(self, size: int, limit: int) -> None:
        self.metrics.rejected.labels(ROUTE_AUDIO, "too_large").inc()
        self.metrics.requests.labels(ROUTE_AUDIO, "rejected").inc()
        raise HTTPException(
            status_code=413,
            detail=(
                f"audio upload is {size / 1024 / 1024:.1f} MB; the limit is "
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

    async def aclose(self) -> None:
        await self.backend.aclose()


# ----------------------------------------------------------------------
# SSE / JSON helpers
# ----------------------------------------------------------------------


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
                {"index": 0, "delta": {"content": tok.text},
                 "finish_reason": tok.finish_reason}
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
        return CacheDirective(read=True, write=False)
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
        readiness = await gw.health.check_ready(gw.backend.model, force=probe)
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
            "model": gw.backend.model,
            "version": __version__,
        }
        if not reachable:
            body["hint"] = gw.backend.unreachable_hint()
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
            "runtime_profile": _runtime_profile(gw.budget_provenance),
            "calibration_key": gw.calibration_key.as_dict(),
            "backend": gw.backend.stats(),
            "backend_health": gw.health.stats(),
            "drift": gw.drift.stats(),
            "routes": {
                "chat": True,
                "models": True,
                "embeddings": settings.routes.embeddings,
                "transcriptions": settings.routes.transcriptions,
            },
            "token_estimator": gw.estimator.stats(),
            "cache": gw.cache.stats() if gw.cache else {"enabled": False},
            "coalescer": (
                {
                    "enabled": True,
                    "inflight": gw.coalescer.inflight,
                    "coalesced_total": gw.coalescer.coalesced_total,
                    "streaming": (
                        gw.stream_coalescer.stats() if gw.stream_coalescer else None
                    ),
                }
                if gw.coalescer else {"enabled": False}
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
    async def models() -> dict:
        ids = await gw.backend.list_models()
        return {"object": "list", "data": [{"id": m, "object": "model"} for m in ids]}

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
        gw.metrics.circuit_state.set(
            _CIRCUIT_STATES.get(gw.health.breaker.state, 0)
        )
        gw.metrics.backend_ready.set(1 if gw.health.readiness.ready else 0)
        if gw.drift.ratio:
            gw.metrics.drift_ratio.set(gw.drift.ratio)
        return Response(gw.metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/chat/completions")
    async def chat_completions(req: ChatCompletionRequest, request: Request):
        # `req` is the typed overlay KVStream needs; `raw` is the client's own
        # object, forwarded untouched. Starlette caches the body, so reading it
        # again here costs nothing.
        raw = await request.json()
        req_id = f"kvs-{uuid.uuid4().hex[:12]}"
        rc, cost = gw._cost(req)
        key = request_key(req, raw) if req.deterministic else None
        auth = _auth(request)
        directive = cache_directive(
            request.headers, settings.cache.respect_request_headers
        )

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

        write_key = key if directive.write else None
        if req.stream:
            return await gw.handle_streaming(req_id, req, raw, rc, cost, write_key, auth)
        return await gw.handle_nonstreaming(req_id, req, raw, rc, cost, write_key, auth)

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
