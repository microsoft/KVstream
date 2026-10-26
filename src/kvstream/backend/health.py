"""
Backend health: liveness, readiness, and a circuit breaker.

All three exist because of one measured failure. Running against a live
``foundry 0.8.119``, the runtime stopped completing generations entirely — a
four-token request did not return in 180 seconds — while ``GET /v1/models``
kept answering **200 in four milliseconds**. Every signal the gateway had said
the backend was fine.

That failure has three separate lessons, and this module is all three:

**Liveness is not readiness (G-52).** Answering ``/v1/models`` proves a socket
is open and a process is alive. It says nothing about whether the thing can
generate a token. Readiness has to be established by asking for a token, with a
bound on how long that may take.

**A readiness probe is real load.** It occupies the engine like any other
request, so it must be rate-limited and single-flighted — otherwise a health
check on a struggling backend becomes part of why it is struggling.

**A stalled backend must not be discovered one request at a time (G-54).** With
no breaker, every arriving request queues, waits the full backend timeout, and
fails — so a 120-second hang becomes 120 seconds of latency for everybody, and
the admission queue fills with work that will never complete. A breaker turns
the second failure onwards into an immediate, honest 503.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:  # pragma: no cover
    from kvstream.backend.foundry import FoundryClient

logger = logging.getLogger("kvstream.health")

CLOSED = "closed"  # normal operation
OPEN = "open"  # failing fast; the backend is presumed unusable
HALF_OPEN = "half_open"  # cooldown elapsed; one trial is allowed through


class BackendUnavailable(RuntimeError):
    """Raised instead of queueing when the circuit breaker is open."""


@dataclass
class Readiness:
    """The outcome of the last readiness probe."""

    ready: bool = False
    checked_at: float = 0.0
    latency_seconds: float = 0.0
    detail: str = "not probed yet"

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.monotonic() - self.checked_at) if self.checked_at else 0.0

    def as_dict(self) -> dict:
        return {
            "ready": self.ready,
            "detail": self.detail,
            "latency_seconds": round(self.latency_seconds, 3),
            "age_seconds": round(self.age_seconds, 1),
            "probed": bool(self.checked_at),
        }


@dataclass
class CircuitBreaker:
    """
    Fail fast once the backend has proved it cannot serve.

    Deliberately counts only failures that mean *the backend is not working* —
    timeouts, connection errors, and upstream 5xx. A 4xx is the client's
    request being wrong and says nothing about backend health, so it must never
    trip the breaker; otherwise one malformed client can take the gateway down
    for everybody.
    """

    failure_threshold: int = 5
    reset_seconds: float = 30.0
    enabled: bool = True
    # How long a half-open trial may stay unresolved before it is presumed
    # lost. A safety net only: every path that ends a trial without a verdict
    # should call `release_trial`, but a trial that is never resolved would
    # otherwise hold the breaker half-open — refusing everyone — forever.
    trial_timeout: float = 120.0

    state: str = CLOSED
    consecutive_failures: int = 0
    opened_at: float = 0.0
    trips: int = 0
    fast_failures: int = 0
    last_error: str = ""
    _trial_in_flight: bool = field(default=False, repr=False)
    _trial_owner: str | None = field(default=None, repr=False)
    _trial_started: float = field(default=0.0, repr=False)

    def allows(self, owner: str | None = None) -> bool:
        """
        Whether a request may proceed to the backend right now.

        ``owner`` identifies the request, so that if it becomes the half-open
        trial, only that request can later release the trial without a verdict.
        """
        if not self.enabled or self.state == CLOSED:
            return True
        if self.state == OPEN:
            if (time.monotonic() - self.opened_at) < self.reset_seconds:
                self.fast_failures += 1
                return False
            # Cooldown elapsed: let exactly one request through to find out.
            self.state = HALF_OPEN
            self._start_trial(owner)
            logger.info("circuit breaker half-open: trying one request")
            return True
        # HALF_OPEN — one trial at a time, everyone else still fails fast.
        if self._trial_in_flight and (time.monotonic() - self._trial_started) < self.trial_timeout:
            self.fast_failures += 1
            return False
        if self._trial_in_flight:
            logger.warning(
                "circuit breaker: half-open trial unresolved after %.0fs; "
                "presuming it lost and allowing another",
                self.trial_timeout,
            )
        self._start_trial(owner)
        return True

    def _start_trial(self, owner: str | None) -> None:
        self._trial_in_flight = True
        self._trial_owner = owner
        self._trial_started = time.monotonic()

    def release_trial(self, owner: str | None) -> None:
        """
        End a half-open trial without a verdict.

        For a trial that never produced evidence about the backend: it was
        refused by admission, the client went away, or the backend answered
        with a client error. The breaker stays half-open and the next request
        becomes the trial. A no-op unless ``owner`` holds the trial.
        """
        if self._trial_in_flight and owner is not None and owner == self._trial_owner:
            self._trial_in_flight = False
            self._trial_owner = None

    def record_success(self) -> None:
        self._trial_in_flight = False
        self._trial_owner = None
        if self.state != CLOSED:
            logger.info("circuit breaker closed: backend is serving again")
        self.state = CLOSED
        self.consecutive_failures = 0
        self.last_error = ""

    def record_failure(self, error: str = "") -> None:
        self._trial_in_flight = False
        self._trial_owner = None
        self.consecutive_failures += 1
        self.last_error = error[:200]
        if not self.enabled:
            return
        if self.state == HALF_OPEN or self.consecutive_failures >= self.failure_threshold:
            if self.state != OPEN:
                self.trips += 1
                logger.warning(
                    "circuit breaker open after %d consecutive backend failures: %s. "
                    "Requests will fail fast for %.0fs rather than queue behind a "
                    "backend that is not serving.",
                    self.consecutive_failures,
                    self.last_error,
                    self.reset_seconds,
                )
            self.state = OPEN
            self.opened_at = time.monotonic()

    @property
    def retry_after_seconds(self) -> int:
        if self.state != OPEN:
            return 1
        remaining = self.reset_seconds - (time.monotonic() - self.opened_at)
        return max(1, int(remaining) + 1)

    def as_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "trips": self.trips,
            "fast_failures": self.fast_failures,
            "last_error": self.last_error,
            "retry_after_seconds": self.retry_after_seconds if self.state == OPEN else 0,
        }


def _timed_out(exc: BaseException) -> bool:
    """
    Whether a failed probe ran out of time, rather than being refused.

    Two clocks bound the probe and either can fire first: ``asyncio.wait_for``
    (``asyncio.TimeoutError``, which is *not* the builtin ``TimeoutError``
    before Python 3.11) and the HTTP client's read timeout, which the backend
    client reports as a ``FoundryError`` caused by ``httpx.ReadTimeout``.
    Catching only the builtin reported a stalled backend as having "rejected"
    the generation on Python 3.10, and on any version when httpx won the race.
    """
    if isinstance(exc, TimeoutError | asyncio.TimeoutError):
        return True
    return isinstance(exc.__cause__, httpx.ReadTimeout)


class BackendHealth:
    """Owns liveness, readiness and the breaker for one backend."""

    def __init__(
        self,
        client: FoundryClient,
        *,
        probe_readiness: bool = True,
        readiness_interval: float = 30.0,
        readiness_timeout: float = 15.0,
        force_min_interval: float = 5.0,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self._client = client
        self._probe_readiness = probe_readiness
        self._interval = max(1.0, readiness_interval)
        self._timeout = max(1.0, readiness_timeout)
        self._force_min_interval = max(0.0, force_min_interval)
        self.breaker = breaker or CircuitBreaker()
        self.readiness = Readiness()
        self.reachable = False
        self._lock = asyncio.Lock()

    def rebind(self, client: FoundryClient) -> None:
        """Point at a different backend client, discarding stale readiness."""
        self._client = client
        self.readiness = Readiness()
        self.reachable = False

    # -- liveness ------------------------------------------------------

    async def check_reachable(self) -> bool:
        """Cheap liveness: does anything answer the models endpoint?"""
        self.reachable = await self._client.health()
        return self.reachable

    # -- readiness -----------------------------------------------------

    async def check_ready(self, model: str, force: bool = False) -> Readiness:
        """
        Can the backend actually produce a token?

        Cached for ``readiness_interval`` and single-flighted, because the probe
        is a real generation. A health check that becomes load is a health check
        that causes outages.
        """
        if not self._probe_readiness:
            self.readiness = Readiness(
                ready=self.reachable,
                checked_at=time.monotonic(),
                detail="readiness probing disabled; reporting liveness only",
            )
            return self.readiness

        if self.readiness.checked_at:
            age = self.readiness.age_seconds
            # The probe is a real generation that is not admitted against the
            # budget, and `?probe=true` is reachable by anyone who can reach
            # /health. A forced probe therefore still honours a short minimum
            # interval, so a caller in a loop cannot turn health checks into
            # unadmitted load on a backend that may serve one request at a time.
            # Forcing never makes the cache *longer* than an unforced check.
            limit = min(self._force_min_interval, self._interval) if force else self._interval
            if age < limit:
                return self.readiness

        if self._lock.locked():
            # A probe is already running; do not pile a second generation onto
            # a backend we already suspect.
            return self.readiness

        async with self._lock:
            started = time.monotonic()
            try:
                await asyncio.wait_for(
                    self._client.chat_once(
                        {
                            "model": model,
                            "messages": [{"role": "user", "content": "ping"}],
                            "max_tokens": 1,
                            "temperature": 0.0,
                        },
                        timeout=self._timeout,
                    ),
                    timeout=self._timeout,
                )
            except Exception as exc:  # noqa: BLE001 — any failure means not ready
                if not _timed_out(exc):
                    self.readiness = Readiness(
                        ready=False,
                        checked_at=time.monotonic(),
                        latency_seconds=time.monotonic() - started,
                        detail=f"backend rejected a 1-token generation: {exc}"[:300],
                    )
                    return self.readiness
                self.readiness = Readiness(
                    ready=False,
                    checked_at=time.monotonic(),
                    latency_seconds=time.monotonic() - started,
                    detail=(
                        f"backend did not complete a 1-token generation within {self._timeout:.0f}s"
                    ),
                )
                logger.warning("readiness probe failed: %s", self.readiness.detail)
                return self.readiness

            self.readiness = Readiness(
                ready=True,
                checked_at=time.monotonic(),
                latency_seconds=time.monotonic() - started,
                detail="completed a 1-token generation",
            )
            return self.readiness

    # -- breaker -------------------------------------------------------

    def guard(self, owner: str | None = None) -> None:
        """Raise :class:`BackendUnavailable` when the breaker is open."""
        if not self.breaker.allows(owner):
            raise BackendUnavailable(
                f"backend is not serving (circuit breaker {self.breaker.state}): "
                f"{self.breaker.last_error or 'consecutive failures'}"
            )

    def release_trial(self, owner: str | None) -> None:
        """End ``owner``'s half-open trial, if it holds one, without a verdict."""
        self.breaker.release_trial(owner)

    def record_success(self) -> None:
        self.breaker.record_success()

    def record_failure(self, error: str = "") -> None:
        self.breaker.record_failure(error)

    def stats(self) -> dict:
        return {
            "reachable": self.reachable,
            "readiness": self.readiness.as_dict(),
            "circuit_breaker": self.breaker.as_dict(),
        }
