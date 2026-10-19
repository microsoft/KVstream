"""
Regression tests for the failure semantics in docs/ARCHITECTURE.md §9 and §13.

Each test pins down a defect that existed and was fixed:

1. A backend 4xx (e.g. a wrong model alias) was reported as 502 and counted as
   a backend failure, so a handful of bad client requests opened the circuit
   breaker for every caller.
2. A half-open breaker trial that never reached a verdict (refused by
   admission, client error, cancellation) held the breaker half-open forever.
3. A cancelled non-streaming coalescing leader stranded its followers, and a
   follower's own cancellation could cancel the result shared with the others.
4. When the client that started a coalesced stream disconnected, followers
   received a truncated response that ended in ``[DONE]`` as if complete.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import TEXT, StubBackend

from kvstream.app import build_app
from kvstream.backend.foundry import FoundryClient, FoundryClientError, FoundryError, Token
from kvstream.backend.health import CLOSED, HALF_OPEN, OPEN, CircuitBreaker
from kvstream.backend.inventory import ModelInventory
from kvstream.cache.coalescer import Coalescer
from kvstream.cache.response_cache import request_key
from kvstream.config import Settings
from kvstream.models import ChatCompletionRequest

PAYLOAD = {"model": "wrong-alias", "messages": [{"role": "user", "content": "hi"}]}
BACKEND_400 = {"error": {"message": "model 'wrong-alias' not found", "type": "invalid_request"}}


def _app(backend):
    settings = Settings()
    settings.backend.model = "stub-model"
    app = build_app(settings)
    app.state.gateway.backend = backend
    return app, app.state.gateway


# ----------------------------------------------------------------------
# 1. Client errors pass through and never trip the breaker
# ----------------------------------------------------------------------


class WrongAliasBackend(StubBackend):
    """Foundry Local answering 400 because the model alias does not exist."""

    async def chat(self, payload, headers=None):
        raise FoundryClientError(400, BACKEND_400, "Foundry Local returned HTTP 400")
        yield  # pragma: no cover — makes this an async generator

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        raise FoundryClientError(400, BACKEND_400, "Foundry Local returned HTTP 400")


@pytest_asyncio.fixture
async def wrong_alias():
    app, gw = _app(WrongAliasBackend())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, gw


@pytest.mark.asyncio
async def test_a_storm_of_client_errors_leaves_the_breaker_closed(wrong_alias):
    c, gw = wrong_alias
    threshold = gw.health.breaker.failure_threshold
    for _ in range(threshold * 3):
        r = await c.post("/v1/chat/completions", json=PAYLOAD)
        assert r.status_code == 400
        r = await c.post("/v1/chat/completions", json={**PAYLOAD, "stream": True})
        assert r.status_code == 400

    assert gw.health.breaker.state == CLOSED
    assert gw.health.breaker.consecutive_failures == 0
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_client_error_body_is_the_backends_own(wrong_alias):
    c, _ = wrong_alias
    r = await c.post("/v1/chat/completions", json=PAYLOAD)
    assert r.status_code == 400
    assert r.json() == BACKEND_400


@pytest.mark.asyncio
async def test_real_backend_failures_still_count():
    """The fix must not blind the breaker to the failures it exists for."""

    class Crashing(StubBackend):
        async def chat_once(self, payload, headers=None, timeout=None) -> dict:
            raise FoundryError("Foundry Local returned HTTP 500: model crashed")

    app, gw = _app(Crashing())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(gw.health.breaker.failure_threshold):
            assert (await c.post("/v1/chat/completions", json=PAYLOAD)).status_code == 502
    assert gw.health.breaker.state == OPEN


def _wire_client(handler) -> FoundryClient:
    c = FoundryClient(base_url="http://backend:1234", model="m", discover=False)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return c


@pytest.mark.asyncio
async def test_wire_4xx_raises_client_error_with_body():
    c = _wire_client(lambda request: httpx.Response(404, json=BACKEND_400))
    with pytest.raises(FoundryClientError) as info:
        await c.chat_once({"model": "m", "messages": []})
    assert info.value.status_code == 404
    assert info.value.body == BACKEND_400
    await c.aclose()


@pytest.mark.asyncio
async def test_wrong_alias_does_not_switch_off_usage_reporting():
    """
    A 400 on a request carrying stream_options used to be read as "the backend
    rejects stream_options", disabling usage reporting for the whole process
    because one client named the wrong model.
    """
    c = _wire_client(lambda request: httpx.Response(400, json=BACKEND_400))
    with pytest.raises(FoundryClientError):
        [t async for t in c.chat({"model": "wrong-alias", "messages": []})]
    assert c.usage_reporting is True
    await c.aclose()


# ----------------------------------------------------------------------
# 2. Every half-open trial is resolved
# ----------------------------------------------------------------------


def _half_open(b: CircuitBreaker) -> None:
    for _ in range(b.failure_threshold):
        b.record_failure("dead")
    b.opened_at = time.monotonic() - b.reset_seconds - 1


def test_trial_released_without_verdict_lets_the_next_request_try():
    b = CircuitBreaker(failure_threshold=2, reset_seconds=1)
    _half_open(b)
    assert b.allows("trial") is True
    assert b.state == HALF_OPEN
    assert b.allows("other") is False
    b.release_trial("other")  # not the owner: no effect
    assert b.allows("other") is False
    b.release_trial("trial")
    assert b.allows("next") is True  # a new trial, still half-open
    assert b.state == HALF_OPEN


def test_lost_trial_is_presumed_dead_after_the_trial_timeout():
    b = CircuitBreaker(failure_threshold=2, reset_seconds=1, trial_timeout=60)
    _half_open(b)
    assert b.allows("lost") is True
    assert b.allows("other") is False
    # Age the trial past its timeout without sleeping: wall-clock sleeps made
    # this flaky on Windows, whose monotonic clock ticks every ~15 ms.
    b._trial_started -= 61
    assert b.allows("other") is True


@pytest.mark.asyncio
async def test_client_error_on_the_half_open_trial_does_not_wedge_the_breaker(wrong_alias):
    c, gw = wrong_alias
    _half_open(gw.health.breaker)
    for _ in range(3):
        # Before the fix the second request got 503 circuit_open, forever.
        r = await c.post("/v1/chat/completions", json=PAYLOAD)
        assert r.status_code == 400


# ----------------------------------------------------------------------
# 3. Coalescer cancellation never crosses between callers
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_leader_hands_over_to_a_follower():
    c = Coalescer()
    started = asyncio.Event()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.sleep(0.1)
        return "RESULT"

    leader = asyncio.create_task(c.run("k", factory))
    await started.wait()
    follower = asyncio.create_task(c.run("k", factory))
    await asyncio.sleep(0.01)
    leader.cancel()

    result, _ = await asyncio.wait_for(follower, timeout=2)
    assert result == "RESULT"
    assert calls == 2  # the follower re-ran the work it never received
    assert c.inflight == 0


@pytest.mark.asyncio
async def test_cancelled_follower_does_not_cancel_the_others():
    c = Coalescer()

    async def factory():
        await asyncio.sleep(0.1)
        return "RESULT"

    leader = asyncio.create_task(c.run("k", factory))
    await asyncio.sleep(0)
    quitter = asyncio.create_task(c.run("k", factory))
    stayer = asyncio.create_task(c.run("k", factory))
    await asyncio.sleep(0.01)
    quitter.cancel()

    assert (await leader)[0] == "RESULT"
    assert (await stayer) == ("RESULT", True)


# ----------------------------------------------------------------------
# 4. Streams are owned by the gateway
# ----------------------------------------------------------------------


class SlowStream(StubBackend):
    """Streams slowly and records whether the upstream call was closed."""

    def __init__(self, pieces: int = 20, fail_after: int | None = None) -> None:
        super().__init__()
        self.pieces = pieces
        self.fail_after = fail_after
        self.closed = 0

    async def chat(self, payload, headers=None):
        self.stream_calls += 1
        try:
            for i in range(self.pieces):
                if self.fail_after is not None and i == self.fail_after:
                    raise FoundryError("Foundry Local is unreachable: read timed out")
                await asyncio.sleep(0.01)
                yield Token(text=f"t{i} ")
            yield Token(text="", finish_reason="stop")
        finally:
            self.closed += 1


def _request(gw, raw: dict):
    req = ChatCompletionRequest(**raw)
    rc, cost = gw._cost(req)
    return req, rc, cost, request_key(req, raw)


async def _drain(response) -> str:
    out = []
    async for chunk in response.body_iterator:
        out.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
    return "".join(out)


STREAM_RAW = {
    "model": "stub-model",
    "messages": [{"role": "user", "content": "go"}],
    "stream": True,
    "temperature": 0,
}


@pytest.mark.asyncio
async def test_leader_disconnect_does_not_truncate_followers():
    backend = SlowStream(pieces=20)
    _, gw = _app(backend)
    req, rc, cost, key = _request(gw, STREAM_RAW)

    leader = await gw.handle_streaming("lead", req, STREAM_RAW, rc, key)
    follower = await gw.handle_streaming("follow", req, STREAM_RAW, rc, key)

    # The leader's client reads one chunk and goes away.
    await leader.body_iterator.__anext__()
    await leader.body_iterator.aclose()

    body = await asyncio.wait_for(_drain(follower), timeout=5)
    assert "t19 " in body  # the whole response arrived
    assert body.rstrip().endswith("data: [DONE]")
    assert backend.stream_calls == 1  # still one upstream call
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_last_subscriber_leaving_cancels_the_upstream_call():
    backend = SlowStream(pieces=500)
    _, gw = _app(backend)
    req, rc, cost, _ = _request(gw, STREAM_RAW)

    response = await gw.handle_streaming("solo", req, STREAM_RAW, rc, None)
    await response.body_iterator.__anext__()
    await response.body_iterator.aclose()

    for _ in range(100):
        if not gw._runs:
            break
        await asyncio.sleep(0.01)
    assert not gw._runs  # the run finished promptly...
    assert backend.closed == 1  # ...by closing the upstream stream
    assert gw.capacity.in_flight == 0  # ...and releasing its budget
    assert gw.health.breaker.consecutive_failures == 0  # not a backend failure


@pytest.mark.asyncio
async def test_midstream_failure_never_ends_in_done():
    backend = SlowStream(pieces=20, fail_after=3)
    _, gw = _app(backend)
    req, rc, cost, key = _request(gw, STREAM_RAW)

    leader = await gw.handle_streaming("lead", req, STREAM_RAW, rc, key)
    follower = await gw.handle_streaming("follow", req, STREAM_RAW, rc, key)

    for body in await asyncio.gather(_drain(leader), _drain(follower)):
        assert "[DONE]" not in body
        assert '"error"' in body
        assert "read timed out" in body
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_completed_stream_still_ends_in_done():
    _, gw = _app(StubBackend())
    req, rc, cost, _ = _request(gw, STREAM_RAW)
    response = await gw.handle_streaming("ok", req, STREAM_RAW, rc, None)
    body = await _drain(response)
    assert body.rstrip().endswith("data: [DONE]")
    assert all(piece in body for piece in ("Hello", "world"))
    assert TEXT == "Hello, world!"


# ----------------------------------------------------------------------
# 5. Streamed requests for a model Foundry does not serve
# ----------------------------------------------------------------------
#
# Measured on Foundry Local CLI 0.10.3: a streamed request naming a model that
# is not loaded gets `200 text/event-stream` and then a dropped connection with
# no data — indistinguishable from a crash at the HTTP layer.


class DropsUnknownStreams(StubBackend):
    """Behaves like Foundry 0.10.3 for a streamed request naming an unloaded model."""

    def __init__(self, served: list[str]) -> None:
        super().__init__()
        self.served = served

    async def list_models(self):
        return list(self.served)

    async def models_or_none(self):
        return list(self.served)

    async def chat(self, payload, headers=None):
        self.stream_calls += 1
        if payload["model"] not in self.served:
            raise FoundryError("peer closed connection without sending complete message body")
        async for tok in super().chat(payload, headers):
            yield tok


@pytest.mark.asyncio
async def test_streamed_unknown_model_is_a_400_and_never_reaches_the_breaker():
    backend = DropsUnknownStreams(served=["stub-model"])
    app, gw = _app(backend)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(gw.health.breaker.failure_threshold * 2):
            r = await c.post(
                "/v1/chat/completions",
                json={
                    "model": "phi-3-mini-4k",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": True,
                },
            )
            assert r.status_code == 400
            assert r.json()["error"]["code"] == "model_not_found"
            assert "stub-model" in r.json()["error"]["message"]
    assert backend.stream_calls == 0
    assert gw.health.breaker.state == CLOSED
    assert gw.health.breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_streamed_downloaded_but_unloaded_model_is_a_400_not_a_breaker_failure():
    """
    Measured on CLI 0.10.3: `/v1/models` keeps listing a model after it is
    unloaded, and a streamed request for it gets 200 + a dropped connection.
    Only the CLI's loaded list can tell, so it is what the check relies on.
    """
    backend = DropsUnknownStreams(served=["stub-model"])
    app, gw = _app(backend)

    async def nothing_loaded():
        return []

    gw.inventory = ModelInventory(backend.models_or_none, nothing_loaded)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(gw.health.breaker.failure_threshold * 2):
            r = await c.post(
                "/v1/chat/completions",
                json={
                    "model": "stub-model",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": True,
                },
            )
            assert r.status_code == 400
            assert r.json()["error"]["code"] == "model_not_loaded"
    assert backend.stream_calls == 0
    assert gw.health.breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_unload_inside_the_cache_window_is_still_not_a_breaker_failure():
    """
    The inventory says "loaded" (cached), the model is then unloaded, and the
    stream fails before its first token. A fresh look must reclassify it.
    """
    backend = DropsUnknownStreams(served=["stub-model"])
    app, gw = _app(backend)
    loaded = ["stub-model"]

    async def read_loaded():
        return list(loaded)

    gw.inventory = ModelInventory(backend.models_or_none, read_loaded, min_refresh_seconds=0.0)
    await gw.inventory.snapshot()  # cached: loaded
    loaded.clear()  # `foundry model unload`
    backend.served = []  # Foundry now drops the stream

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post(
            "/v1/chat/completions",
            json={
                "model": "stub-model",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )
    assert r.status_code == 400
    assert backend.stream_calls == 1  # it did reach Foundry...
    assert gw.health.breaker.consecutive_failures == 0  # ...but was not blamed on it
    assert gw.capacity.in_flight == 0


@pytest.mark.asyncio
async def test_version_suffix_is_accepted_either_way():
    for served, asked in (("stub-model", "stub-model:2"), ("stub-model:2", "stub-model")):
        _, gw = _app(DropsUnknownStreams(served=[served]))
        assert await gw.stream_refusal(asked) is None


@pytest.mark.asyncio
async def test_model_check_fails_open_when_the_list_is_unavailable():
    _, gw = _app(DropsUnknownStreams(served=[]))
    assert await gw.stream_refusal("anything") is None
