"""
CapacityManager — admission control for a single Foundry Local instance.

This is the "KV-capacity manager". It performs **integer accounting only** — it
never touches KV tensors and has no access to Foundry Local's memory. Two modes
share one mechanism:

  * ``concurrency`` — budget = max concurrent requests; each request costs 1.
  * ``tokens``      — budget = a calibrated KV-token ceiling ``B``; each request
                      costs ``prompt_tokens + max_tokens`` (an estimate), scaled
                      by the model's relative KV geometry where it is known.

A request reserves its cost on admission and releases it on completion. When the
budget is full, requests queue (up to ``max_queue_depth``) until space frees or
the admission timeout elapses, at which point the caller receives a clean 503.

Because admission happens before a single token exists, the reserved cost is a
*prediction*. :meth:`CapacityManager.adjust` settles it against reality as the
response streams — returning headroom that a short generation was never going
to use, rather than holding the worst case until teardown.

Queue discipline
----------------
Waiters are admitted in **strict arrival order**. On release, the queue drains
from the front for as long as the next waiter fits, which is the proposal's
"admit the next queued request(s) that now fit" (§6.2 step 6).

Ordering matters more than it looks. The obvious implementation — wake every
waiter and let each re-check — has no ordering at all: whichever coroutine the
event loop happens to schedule first and happens to fit wins, so a large request
can be starved indefinitely by a stream of small ones. That is precisely the
mixed-size multi-agent workload the token budget exists to serve. It also wakes
O(N) coroutines per release to admit one.

The cost of strict FIFO is head-of-line blocking: a large request at the front
holds back smaller ones behind it until it fits. That is a deliberate trade —
bounded, fair waiting beats unbounded unfairness — and it is why the admission
wait is measured (`kvstream_admission_wait_seconds`) rather than assumed.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from math import ceil


class AdmissionTimeout(RuntimeError):
    """Raised when a request cannot be admitted within the configured window."""


class QueueFull(RuntimeError):
    """Raised when the waiting queue is already at ``max_queue_depth``."""


class AdmissionTooSlow(RuntimeError):
    """
    Raised when the queue cannot possibly drain in time.

    Distinct from :class:`AdmissionTimeout`, which means "you waited and it did
    not happen". This one means "you would have waited, and it still would not
    have happened" — decided on arrival, so the caller finds out in
    milliseconds instead of holding a connection for the whole timeout.
    """

    def __init__(self, message: str, predicted_wait: float) -> None:
        super().__init__(message)
        self.predicted_wait = predicted_wait


@dataclass(frozen=True)
class RequestCost:
    prompt_tokens: int
    max_tokens: int
    # Relative KV footprint per token for this request's model, where 1.0 is the
    # model the budget was calibrated against. See `kvstream.admission.geometry`.
    kv_weight: float = 1.0

    @property
    def tokens(self) -> int:
        """Worst-case KV footprint: the whole prompt plus every allowed token."""
        return self._weighted(self.prompt_tokens + self.max_tokens)

    def tokens_at(self, reserve_ratio: float) -> int:
        """Footprint reserved up front when only part of ``max_tokens`` is claimed."""
        if reserve_ratio >= 1.0:
            return self.tokens
        return self._weighted(
            self.prompt_tokens + ceil(self.max_tokens * reserve_ratio)
        )

    def live_tokens(self, generated_tokens: int) -> int:
        """Footprint actually occupied once ``generated_tokens`` have arrived."""
        return self._weighted(self.prompt_tokens + generated_tokens)

    def _weighted(self, raw_tokens: int) -> int:
        return max(1, ceil(max(0, raw_tokens) * self.kv_weight))


@dataclass
class _Waiter:
    req_id: str
    cost: int
    future: asyncio.Future
    granted: bool = False
    enqueued_at: float = 0.0


@dataclass
class QueueStats:
    """Point-in-time view of the waiting queue."""

    depth: int = 0
    head_cost: int = 0
    oldest_wait_seconds: float = 0.0
    admitted: int = 0
    timed_out: int = 0
    rejected: int = 0
    peak_depth: int = 0

    def as_dict(self) -> dict:
        return {
            "depth": self.depth,
            "head_cost": self.head_cost,
            "oldest_wait_seconds": round(self.oldest_wait_seconds, 3),
            "admitted": self.admitted,
            "timed_out": self.timed_out,
            "rejected": self.rejected,
            "peak_depth": self.peak_depth,
        }


class CapacityManager:
    def __init__(
        self,
        *,
        budget: int,
        unit: str,
        admission_timeout: float,
        max_queue_depth: int,
        reserve_ratio: float = 1.0,
        reject_when_hopeless: bool = True,
        min_rate_samples: int = 5,
        recheck_interval: float = 1.0,
        rate_window: float = 10.0,
        hopeless_margin: float = 1.5,
    ) -> None:
        if budget <= 0:
            raise ValueError("budget must be > 0")
        if unit not in ("tokens", "concurrency"):
            raise ValueError("unit must be 'tokens' or 'concurrency'")
        if not 0.0 < reserve_ratio <= 1.0:
            raise ValueError("reserve_ratio must be in (0.0, 1.0]")
        self._budget = budget
        self._unit = unit
        self._timeout = admission_timeout
        self._max_queue_depth = max_queue_depth
        self._reserve_ratio = reserve_ratio

        self._in_flight = 0
        self._reservations: dict[str, int] = {}
        self._queue: deque[_Waiter] = deque()
        self._lock = asyncio.Lock()
        self._draining = False

        self._overshoots = 0
        self._reclaimed = 0
        self._q = QueueStats()

        # Drain rate, learned from completions: how much reserved cost the
        # backend actually frees per second. This is what turns "you have been
        # waiting 120s" into "you are not going to be served, here is why".
        self._reject_when_hopeless = reject_when_hopeless
        self._min_rate_samples = max(1, min_rate_samples)
        self._recheck_interval = max(0.05, recheck_interval)
        self._hopeless_margin = max(1.0, hopeless_margin)
        self._queued_cost = 0
        # Completions in a trailing window, rather than an EWMA of gaps between
        # them. Gaps are easy to fool: a burst of requests that fail instantly
        # keeps the gap small and the rate looking healthy, while the real work
        # behind them takes seconds each. Throughput over a window cannot be
        # fooled that way — it counts what actually got done.
        self._releases: deque[tuple[float, int]] = deque()
        self._rate_window = rate_window
        self._completions = 0
        self._hopeless_rejections = 0

    # -- introspection -------------------------------------------------

    @property
    def unit(self) -> str:
        return self._unit

    @property
    def budget(self) -> int:
        return self._budget

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def waiting(self) -> int:
        return len(self._queue)

    @property
    def reserve_ratio(self) -> float:
        return self._reserve_ratio

    @property
    def overshoots(self) -> int:
        """Times a live reservation had to grow beyond its admitted cost."""
        return self._overshoots

    @property
    def reclaimed(self) -> int:
        """Total budget returned early by live accounting (unit-dependent)."""
        return self._reclaimed

    @property
    def draining(self) -> bool:
        return self._draining

    def cost_of(self, req: RequestCost) -> int:
        """Cost to reserve at admission time."""
        return req.tokens_at(self._reserve_ratio) if self._unit == "tokens" else 1

    def live_cost(self, req: RequestCost, generated_tokens: int) -> int:
        """
        Cost a request actually occupies once ``generated_tokens`` have arrived.

        In concurrency mode a request is always worth exactly one slot, so live
        accounting is a no-op there.
        """
        if self._unit != "tokens":
            return 1
        return req.live_tokens(generated_tokens)

    @property
    def drain_rate(self) -> float:
        """Reserved cost the backend frees per second, over the trailing window."""
        return self.effective_drain_rate()

    def effective_drain_rate(self) -> float:
        """
        Cost completed per second over the trailing window.

        Once the window has elapsed with nothing completing, this goes to zero —
        which is the correct reading of a backend that has stopped, and the
        signal that lets queued requests give up instead of waiting out their
        full deadline.
        """
        now = _now()
        self._trim_releases(now)
        if self._completions < self._min_rate_samples:
            return 0.0
        if not self._releases:
            return 0.0
        span = max(now - self._releases[0][0], self._rate_window * 0.5)
        return sum(cost for _, cost in self._releases) / span

    def _trim_releases(self, now: float) -> None:
        cutoff = now - self._rate_window
        while self._releases and self._releases[0][0] < cutoff:
            self._releases.popleft()

    def _cost_ahead(self, waiter: _Waiter) -> int:
        """Reserved cost that must free before ``waiter`` can be admitted."""
        ahead = 0
        for queued in self._queue:
            if queued is waiter:
                break
            ahead += queued.cost
        headroom = max(0, self._in_flight + waiter.cost - self._budget)
        return ahead + headroom

    def _wait_for_waiter(self, waiter: _Waiter) -> float:
        rate = self.effective_drain_rate()
        if rate <= 0:
            return 0.0
        return self._cost_ahead(waiter) / rate

    def _may_reject(self) -> bool:
        """
        Whether refusing on prediction is safe right now.

        Every guard here exists because the predictor got it wrong on real
        hardware, in both directions. A throughput estimate cannot tell request
        classes apart: a rate learned from small requests wildly overstates
        capacity for large ones, and a rate learned from large ones
        under-states it for small ones. The second case is the dangerous one —
        a low rate causes rejections, rejections prevent completions, and
        without completions the rate never recovers. The estimator starves
        itself and the gateway refuses traffic it could have served.

        So prediction only gets a vote when the system is unambiguously
        saturated: every unit of budget in use, and a queue already deeper than
        the budget itself. Anywhere else, waiting is the safe answer.
        """
        if not self._reject_when_hopeless:
            return False
        if self._in_flight < self._budget:
            return False
        return len(self._queue) >= max(2, self._budget)

    def predicted_wait(self, cost: int) -> float:
        """
        Seconds a request of ``cost`` would wait before admission.

        Everything ahead of it has to drain first, and the drain rate is
        measured rather than assumed. Returns 0.0 while the rate is still
        unknown — an unmeasured system must not refuse anyone on a guess.
        """
        rate = self.effective_drain_rate()
        if rate <= 0:
            return 0.0
        # Work ahead is what is queued plus whatever must free up for this
        # request to fit. In concurrency mode both terms are request counts; in
        # token mode both are tokens. The units match either way.
        headroom_needed = max(0, self._in_flight + cost - self._budget)
        return (self._queued_cost + headroom_needed) / rate

    def _observe_release(self, cost: int) -> None:
        """Record one completion into the trailing window."""
        now = _now()
        self._releases.append((now, max(0, cost)))
        self._completions += 1
        self._trim_releases(now)

    def stats(self) -> dict:
        loop_time = _now()
        head = self._queue[0] if self._queue else None
        self._q.depth = len(self._queue)
        self._q.head_cost = head.cost if head else 0
        self._q.oldest_wait_seconds = (loop_time - head.enqueued_at) if head else 0.0
        return {
            "unit": self._unit,
            "budget": self._budget,
            "in_flight": self._in_flight,
            "utilization": (
                round(self._in_flight / self._budget, 3) if self._budget else 0.0
            ),
            "waiting": len(self._queue),
            "max_queue_depth": self._max_queue_depth,
            "active": len(self._reservations),
            "overshoots": self._overshoots,
            "reclaimed": self._reclaimed,
            "draining": self._draining,
            "queue": self._q.as_dict(),
            "drain_rate_per_second": round(self.effective_drain_rate(), 3),
            "completions": self._completions,
            "queued_cost": self._queued_cost,
            "hopeless_rejections": self._hopeless_rejections,
        }

    # -- admission -----------------------------------------------------

    def _can_fit(self, cost: int) -> bool:
        # An idle manager admits anything (a single oversized request runs alone,
        # since it could never fit otherwise). Otherwise enforce the budget.
        if self._in_flight == 0:
            return True
        return self._in_flight + cost <= self._budget

    def _reserve(self, req_id: str, cost: int) -> None:
        self._in_flight += cost
        self._reservations[req_id] = cost

    def _drain(self) -> None:
        """
        Admit as many waiters from the front of the queue as now fit.

        Strictly front-to-back: the moment the head does not fit, draining
        stops, so nobody behind it can jump the queue.
        """
        while self._queue and self._can_fit(self._queue[0].cost):
            waiter = self._queue.popleft()
            self._queued_cost = max(0, self._queued_cost - waiter.cost)
            if waiter.future.done():
                # Cancelled or timed out between release and drain.
                continue
            self._reserve(waiter.req_id, waiter.cost)
            waiter.granted = True
            self._q.admitted += 1
            waiter.future.set_result(True)

    async def admit(self, req_id: str, cost: int) -> None:
        """
        Reserve ``cost`` for ``req_id``, blocking until it fits.

        Raises :class:`QueueFull` immediately if the queue is saturated, or
        :class:`AdmissionTimeout` if space does not free within the window.
        """
        async with self._lock:
            if self._draining:
                self._q.rejected += 1
                raise QueueFull("gateway is shutting down")
            # Fast path: nobody is waiting and there is room. Requests admitted
            # here never enter the queue, so they cannot consume queue depth
            # from requests that genuinely have to wait.
            if not self._queue and self._can_fit(cost):
                self._reserve(req_id, cost)
                return
            if len(self._queue) >= self._max_queue_depth:
                self._q.rejected += 1
                raise QueueFull("admission queue is full")

            # Refuse now rather than after the timeout. A request that cannot be
            # served inside its own deadline gains nothing by holding a
            # connection open until that deadline expires — and the caller loses
            # every second it spends finding out.
            wait = self.predicted_wait(cost)
            if self._may_reject() and wait > self._timeout * self._hopeless_margin:
                self._q.rejected += 1
                self._hopeless_rejections += 1
                raise AdmissionTooSlow(
                    f"queue would take about {wait:.0f}s to reach this request, "
                    f"beyond the {self._timeout:.0f}s admission timeout",
                    wait,
                )
            waiter = _Waiter(
                req_id=req_id,
                cost=cost,
                future=asyncio.get_running_loop().create_future(),
                enqueued_at=_now(),
            )
            self._queue.append(waiter)
            self._queued_cost += cost
            self._q.peak_depth = max(self._q.peak_depth, len(self._queue))

        # Wait in slices, re-predicting each time. An arrival-time decision uses
        # whatever rate was current when the queue was still empty; by the time
        # the queue is deep, that figure can be wildly optimistic. Re-checking is
        # what lets a request give up at five seconds instead of at the timeout.
        deadline = _now() + self._timeout
        try:
            while True:
                remaining = deadline - _now()
                if remaining <= 0:
                    break
                done, _ = await asyncio.wait(
                    {waiter.future}, timeout=min(remaining, self._recheck_interval)
                )
                if done:
                    return
                async with self._lock:
                    if waiter.granted:
                        return
                    left = deadline - _now()
                    predicted = self._wait_for_waiter(waiter)
                    # The margin is deliberate slack: only refuse when the
                    # prediction says the deadline will be missed by a wide
                    # margin, never on a marginal call.
                    if (
                        self._may_reject()
                        and predicted > left * self._hopeless_margin > 0
                    ):
                        self._discard(waiter)
                        self._q.timed_out -= 1  # counted below as hopeless
                        self._hopeless_rejections += 1
                        self._q.rejected += 1
                        self._drain()
                        raise AdmissionTooSlow(
                            f"queue needs about {predicted:.0f}s more to reach this "
                            f"request, past its {self._timeout:.0f}s deadline",
                            predicted,
                        )
            async with self._lock:
                if waiter.granted:
                    return
                self._discard(waiter)
                self._drain()
            raise AdmissionTimeout("admission timed out") from None
        except asyncio.CancelledError:
            # The client went away. Give back anything granted in the race
            # window and let the cancellation propagate untouched.
            async with self._lock:
                if waiter.granted:
                    self._release_locked(req_id)
                else:
                    self._discard(waiter)
                self._drain()
            raise

    def _discard(self, waiter: _Waiter) -> None:
        if not waiter.future.done():
            waiter.future.cancel()
        try:
            self._queue.remove(waiter)
        except ValueError:
            return
        self._queued_cost = max(0, self._queued_cost - waiter.cost)
        self._q.timed_out += 1

    # -- accounting ----------------------------------------------------

    async def adjust(self, req_id: str, new_cost: int) -> int:
        """
        Re-size a live reservation to ``new_cost`` and return the delta applied.

        This is the "update accounting live" step of the request lifecycle. A
        request is admitted on a *predicted* cost; as the response actually
        streams, its true footprint becomes known:

        * **Shrinking** returns the unused headroom immediately and drains the
          queue, so waiters start the moment budget is known to be free rather
          than when the HTTP body finishes. Always safe.
        * **Growing** happens only when a generation outruns a reservation that
          was deliberately sized below the worst case
          (``admission.reserve_completion_ratio`` < 1.0). Truncating a
          half-delivered response would be worse than briefly exceeding the
          budget, so the reservation is topped up and the event is counted.
          With the default ratio of 1.0 this cannot occur.

        Never blocks. Unknown ``req_id`` (already released) is a no-op.
        """
        new_cost = max(0, new_cost)
        async with self._lock:
            current = self._reservations.get(req_id)
            if current is None:
                return 0
            delta = new_cost - current
            if delta == 0:
                return 0
            self._reservations[req_id] = new_cost
            self._in_flight = max(0, self._in_flight + delta)
            if delta > 0:
                self._overshoots += 1
            else:
                self._reclaimed += -delta
                self._drain()
            return delta

    def _release_locked(self, req_id: str) -> None:
        cost = self._reservations.pop(req_id, None)
        if cost is not None:
            self._in_flight = max(0, self._in_flight - cost)
            self._observe_release(cost)

    async def release(self, req_id: str) -> None:
        """Release ``req_id``'s reservation and admit whoever fits. Idempotent."""
        async with self._lock:
            self._release_locked(req_id)
            self._drain()

    # -- shutdown ------------------------------------------------------

    async def start_draining(self) -> int:
        """
        Stop admitting and fail everyone still queued.

        Called on shutdown: a request that has not started has nothing to lose
        by being told to retry, while one already in flight is left alone to
        finish. Returns how many queued requests were turned away.
        """
        async with self._lock:
            self._draining = True
            turned_away = len(self._queue)
            while self._queue:
                waiter = self._queue.popleft()
                self._queued_cost = max(0, self._queued_cost - waiter.cost)
                if not waiter.future.done():
                    waiter.future.cancel()
            return turned_away


def _now() -> float:
    """One clock for enqueue timestamps and the stats that read them."""
    return time.monotonic()
