"""
G-11 and G-39 end to end, plus the G-37 status additions.

Relative KV costing only matters once a gateway sees more than one model, which
it does the moment a client names one other than the configured default.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.config import Settings
from kvstream.models import ChatCompletionRequest

SMALL = {"num_hidden_layers": 8, "num_key_value_heads": 8, "head_dim": 64}
LARGE = {"num_hidden_layers": 32, "num_key_value_heads": 32, "head_dim": 64}

MESSAGES = [{"role": "user", "content": "hello there"}]


def _app(**overrides):
    settings = Settings(**overrides)
    app = build_app(settings)
    stub = StubBackend()
    app.state.gateway.backend = stub
    return app, app.state.gateway, stub


@pytest_asyncio.fixture
async def geometry_app():
    app, gw, stub = _app(
        backend={"model": "small-model"},
        admission={"mode": "tokens", "budget_tokens": 1_000_000},
        models={"small-model": SMALL, "large-model": LARGE},
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c, gw, stub


# -- relative costing ---------------------------------------------------


@pytest.mark.asyncio
async def test_a_heavier_model_costs_more_for_the_same_tokens(geometry_app):
    _, gw, _ = geometry_app
    small = ChatCompletionRequest(
        model="small-model", messages=MESSAGES, max_tokens=100
    )
    large = ChatCompletionRequest(
        model="large-model", messages=MESSAGES, max_tokens=100
    )

    _, small_cost = gw._cost(small)
    _, large_cost = gw._cost(large)
    assert large_cost == small_cost * 16  # 4x layers x 4x kv heads


@pytest.mark.asyncio
async def test_an_undeclared_model_is_costed_as_before(geometry_app):
    """No geometry means no opinion, not a guess."""
    _, gw, _ = geometry_app
    known = ChatCompletionRequest(
        model="small-model", messages=MESSAGES, max_tokens=100
    )
    unknown = ChatCompletionRequest(
        model="mystery-model", messages=MESSAGES, max_tokens=100
    )
    assert gw._cost(unknown)[1] == gw._cost(known)[1]


@pytest.mark.asyncio
async def test_geometry_is_absent_by_default():
    """A gateway with no declared models behaves exactly as it did before."""
    _, gw, _ = _app(admission={"mode": "tokens", "budget_tokens": 1_000_000})
    a = ChatCompletionRequest(model="phi-3-mini", messages=MESSAGES, max_tokens=100)
    b = ChatCompletionRequest(model="anything-else", messages=MESSAGES, max_tokens=100)
    assert gw._cost(a)[1] == gw._cost(b)[1]
    assert gw.geometry.stats()["models"] == {}


@pytest.mark.asyncio
async def test_status_reports_the_geometry_in_use(geometry_app):
    c, _, _ = geometry_app
    body = (await c.get("/status")).json()
    geometry = body["model_geometry"]
    assert geometry["anchor"] == "small-model"
    assert geometry["anchor_known"] is True
    assert geometry["models"]["large-model"]["weight"] == 16.0
    assert geometry["models"]["small-model"]["kv_bytes_per_token"] > 0


@pytest.mark.asyncio
async def test_a_heavy_model_actually_consumes_more_budget(geometry_app):
    """The weight has to reach admission, not just the status page."""
    c, gw, _ = geometry_app
    r = await c.post(
        "/v1/chat/completions",
        json={"model": "large-model", "messages": MESSAGES, "max_tokens": 100},
    )
    assert r.status_code == 200
    assert gw.capacity.reclaimed > 0  # reclaimed in the weighted unit
    assert gw.capacity.in_flight == 0


# -- status completeness (G-37) ----------------------------------------


@pytest.mark.asyncio
async def test_status_reports_queue_and_calibration_provenance(geometry_app):
    c, _, _ = geometry_app
    body = (await c.get("/status")).json()

    queue = body["admission"]["queue"]
    assert set(queue) >= {"depth", "head_cost", "oldest_wait_seconds", "peak_depth"}
    assert body["budget_source"]["source"] == "configured"
    assert "calibration_key" in body
    assert body["coalescer"]["streaming"] == {"inflight": 0, "coalesced_total": 0}


@pytest.mark.asyncio
async def test_metrics_expose_the_new_series(geometry_app):
    c, _, _ = geometry_app
    await c.post(
        "/v1/chat/completions",
        json={"model": "small-model", "messages": MESSAGES, "max_tokens": 16},
    )
    metrics = (await c.get("/metrics")).text
    assert "kvstream_admission_wait_seconds" in metrics
    assert "kvstream_calibration_age_seconds" in metrics
    assert "kvstream_cache_skipped_total" in metrics


# -- graceful drain (G-39) ---------------------------------------------


@pytest.mark.asyncio
async def test_drain_turns_away_the_queue_and_waits_for_the_rest():
    app, gw, stub = _app(admission={"mode": "concurrency", "max_concurrency": 1})

    # Occupy the only slot, then queue one behind it.
    await gw.capacity.admit("occupier", 1)
    queued = asyncio.create_task(gw.capacity.admit("queued", 1))
    await asyncio.sleep(0)
    assert gw.capacity.waiting == 1

    drain = asyncio.create_task(gw.drain(timeout=2.0))
    await asyncio.sleep(0.05)

    assert gw.capacity.draining is True
    assert gw.capacity.waiting == 0  # the queued one was turned away
    assert gw.capacity.in_flight == 1  # the running one was left alone
    assert not drain.done()  # ...and drain is waiting for it

    await gw.capacity.release("occupier")
    await asyncio.wait_for(drain, timeout=2.0)

    queued.cancel()
    await asyncio.gather(queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_drain_gives_up_after_the_timeout():
    """A stuck request must not hold the process open forever."""
    app, gw, _ = _app(admission={"mode": "concurrency", "max_concurrency": 2})
    await gw.capacity.admit("stuck", 1)

    await asyncio.wait_for(gw.drain(timeout=0.1), timeout=2.0)
    assert gw.capacity.in_flight == 1  # still there; we exited anyway


@pytest.mark.asyncio
async def test_a_draining_gateway_rejects_new_work():
    app, gw, _ = _app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        await gw.capacity.start_draining()
        r = await c.post(
            "/v1/chat/completions", json={"model": "stub-model", "messages": MESSAGES}
        )
    assert r.status_code == 503
    assert r.headers["retry-after"] == "1"
    assert "shutting down" in r.json()["error"]["message"]
