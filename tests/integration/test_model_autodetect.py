"""
`--model` is optional: the gateway adopts the one loaded model, and says
plainly when there is no single answer instead of guessing.
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from stubs import StubBackend

from kvstream.app import build_app
from kvstream.backend.inventory import ModelInventory
from kvstream.config import Settings

PHI = "Phi-3-mini-4k-instruct-generic-gpu"
PHI_V = "Phi-3-mini-4k-instruct-generic-gpu:2"


class Foundry(StubBackend):
    """A backend whose downloaded and loaded models the test controls."""

    def __init__(self, downloaded: list[str], loaded: list[str] | None) -> None:
        super().__init__()
        self.model = None
        self.downloaded = downloaded
        self.loaded = loaded
        self.probed_with: list[str] = []

    async def models_or_none(self):
        return list(self.downloaded)

    async def list_models(self):
        return list(self.downloaded)

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        self.probed_with.append(payload["model"])
        return await super().chat_once(payload, headers, timeout)

    def use_model(self, model: str) -> None:
        self.model = model


async def _gateway(backend: Foundry, model: str | None = None):
    settings = Settings()
    settings.backend.model = model
    app = build_app(settings)
    gw = app.state.gateway
    gw.backend = backend

    async def loaded():
        return None if backend.loaded is None else list(backend.loaded)

    gw.inventory = ModelInventory(backend.models_or_none, loaded, min_refresh_seconds=0.0)
    await gw.startup()
    return app, gw


@pytest.mark.asyncio
async def test_the_one_loaded_model_is_adopted_and_used_for_the_probe():
    backend = Foundry(downloaded=[PHI], loaded=[PHI_V])
    app, gw = await _gateway(backend)

    assert gw.settings.backend.model == PHI_V
    assert gw.model_source.startswith("auto-detected")
    assert gw.calibration_key.model == PHI_V  # calibration keyed by the real model
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/health")
    assert r.status_code == 200
    assert r.json()["model"] == PHI_V
    assert backend.probed_with == [PHI_V]


@pytest.mark.asyncio
async def test_a_configured_model_is_never_overridden():
    backend = Foundry(downloaded=[PHI, "other"], loaded=[PHI_V])
    _, gw = await _gateway(backend, model="other")
    assert gw.settings.backend.model == "other"
    assert gw.model_source == "configured"


@pytest.mark.asyncio
async def test_nothing_loaded_is_reported_not_guessed_and_requests_still_pass():
    backend = Foundry(downloaded=[PHI], loaded=[])
    app, gw = await _gateway(backend)

    assert gw.settings.backend.model is None
    assert gw.calibration_key.model == "unknown"  # no budget applied for a guess
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        health = await c.get("/health")
        # A request names its own model, so it is forwarded regardless.
        chat = await c.post(
            "/v1/chat/completions",
            json={"model": PHI, "messages": [{"role": "user", "content": "hi"}]},
        )
    assert health.status_code == 503
    assert "no model is loaded" in health.json()["hint"]
    assert backend.probed_with == [PHI]  # only the forwarded request, no probe
    assert chat.status_code == 200


@pytest.mark.asyncio
async def test_a_model_loaded_after_startup_is_picked_up_by_the_next_health_check():
    backend = Foundry(downloaded=[PHI], loaded=[])
    app, gw = await _gateway(backend)
    assert gw.settings.backend.model is None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        # A health check while nothing is loaded must not be cached: found
        # live, it kept /health at 503 for the readiness interval after load.
        assert (await c.get("/health")).status_code == 503
        backend.loaded = [PHI_V]  # operator runs `foundry model load`
        r = await c.get("/health")
    assert r.status_code == 200
    assert r.json()["model"] == PHI_V
    assert gw.calibration_key.model == PHI_V  # environment re-keyed on adoption


@pytest.mark.asyncio
async def test_several_loaded_models_require_a_choice():
    backend = Foundry(downloaded=[PHI, "qwen"], loaded=[PHI_V, "qwen:1"])
    app, gw = await _gateway(backend)
    assert gw.settings.backend.model is None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/health")
        status = (await c.get("/status")).json()
    assert r.status_code == 503
    assert "--model" in r.json()["hint"]
    assert status["model"]["id"] is None
    assert status["model"]["source"].startswith("undetermined")


@pytest.mark.asyncio
async def test_without_the_cli_a_single_listed_model_is_used_and_probe_confirms_it():
    backend = Foundry(downloaded=[PHI], loaded=None)
    _, gw = await _gateway(backend)
    assert gw.settings.backend.model == PHI
    assert "could not be checked" in gw.model_source
