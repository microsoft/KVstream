"""
G-29: choosing between localhost ports that all answer /v1/models.

Ollama, LM Studio, vLLM and another KVStream all speak the same API. There is no
published fingerprint that positively identifies Foundry Local at the HTTP
layer, so discovery ranks rather than asserts — and refuses outright only in the
one case it can be certain about.
"""

from __future__ import annotations

import httpx
import pytest

from kvstream.backend import discovery
from kvstream.backend.discovery import (
    KIND_KVSTREAM,
    KIND_OLLAMA,
    KIND_OPENAI,
    KVSTREAM_HEADER,
    Candidate,
    discover,
)


def test_ranking_order():
    configured = Candidate("http://a:1", 3, KIND_OPENAI, has_configured_model=True)
    plain = Candidate("http://b:2", 3, KIND_OPENAI)
    empty = Candidate("http://c:3", 0, KIND_OPENAI)
    ollama = Candidate("http://d:4", 5, KIND_OLLAMA)

    ordered = sorted([empty, ollama, plain, configured], key=Candidate.rank)
    assert [c.url for c in ordered] == [
        "http://a:1",
        "http://b:2",
        "http://c:3",
        "http://d:4",
    ]


def _fake_network(monkeypatch, ports: dict[int, dict]):
    """Serve a canned /v1/models (and /api/tags) per port."""

    def handler(request: httpx.Request) -> httpx.Response:
        port = request.url.port
        spec = ports.get(port or 0)
        if spec is None:
            return httpx.Response(404)
        if request.url.path == "/api/tags":
            if spec.get("ollama"):
                return httpx.Response(200, json={"models": []})
            return httpx.Response(404)
        if request.url.path == "/v1/models":
            headers = {KVSTREAM_HEADER: "1.0.0"} if spec.get("kvstream") else {}
            data = [{"id": m} for m in spec.get("models", [])]
            return httpx.Response(
                200, json={"object": "list", "data": data}, headers=headers
            )
        return httpx.Response(404)

    monkeypatch.setattr(discovery, "list_listening_ports", lambda: sorted(ports.keys()))
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_prefers_the_port_serving_the_configured_model(monkeypatch):
    client = _fake_network(
        monkeypatch,
        {
            5000: {"models": ["some-other-model"]},
            5273: {"models": ["phi-3-mini"]},
        },
    )
    found = await discover(
        client, "http://localhost:9999", set(), prefer_model="phi-3-mini"
    )
    assert found == "http://localhost:5273"
    await client.aclose()


@pytest.mark.asyncio
async def test_never_adopts_another_kvstream_as_a_backend(monkeypatch):
    """The one certain signal: we control the header, so this cannot be wrong."""
    client = _fake_network(
        monkeypatch,
        {
            8080: {"models": ["phi-3-mini"], "kvstream": True},
            5273: {"models": ["phi-3-mini"]},
        },
    )
    found = await discover(
        client, "http://localhost:9999", set(), prefer_model="phi-3-mini"
    )
    assert found == "http://localhost:5273"
    await client.aclose()


@pytest.mark.asyncio
async def test_a_lone_kvstream_is_not_a_backend_at_all(monkeypatch):
    """Better to fail than to build a proxy loop."""
    client = _fake_network(
        monkeypatch, {8080: {"models": ["phi-3-mini"], "kvstream": True}}
    )
    assert await discover(client, "http://localhost:9999", set()) is None
    await client.aclose()


@pytest.mark.asyncio
async def test_ollama_is_demoted_but_not_excluded(monkeypatch):
    client = _fake_network(
        monkeypatch,
        {
            11434: {"models": ["llama3"], "ollama": True},
            5273: {"models": ["phi-3-mini"]},
        },
    )
    found = await discover(client, "http://localhost:9999", set())
    assert found == "http://localhost:5273"
    await client.aclose()


@pytest.mark.asyncio
async def test_a_port_with_models_beats_an_empty_one(monkeypatch):
    client = _fake_network(
        monkeypatch, {5000: {"models": []}, 5273: {"models": ["phi-3-mini"]}}
    )
    found = await discover(client, "http://localhost:9999", set())
    assert found == "http://localhost:5273"
    await client.aclose()


@pytest.mark.asyncio
async def test_configured_url_short_circuits_the_scan(monkeypatch):
    scanned = False

    def never() -> list[int]:
        nonlocal scanned
        scanned = True
        return []

    client = _fake_network(monkeypatch, {5273: {"models": ["phi-3-mini"]}})
    monkeypatch.setattr(discovery, "list_listening_ports", never)

    found = await discover(client, "http://localhost:5273", set())
    assert found == "http://localhost:5273"
    assert scanned is False
    await client.aclose()


@pytest.mark.asyncio
async def test_excluded_ports_are_never_probed(monkeypatch):
    client = _fake_network(monkeypatch, {8080: {"models": ["phi-3-mini"]}})
    assert await discover(client, "http://localhost:9999", {8080}) is None
    await client.aclose()


@pytest.mark.asyncio
async def test_candidate_kinds(monkeypatch):
    client = _fake_network(
        monkeypatch,
        {
            8080: {"models": [], "kvstream": True},
            11434: {"models": [], "ollama": True},
            5273: {"models": ["phi-3-mini"]},
        },
    )
    kv = await discovery._probe(client, 8080)
    ol = await discovery._probe(client, 11434)
    fo = await discovery._probe(client, 5273, prefer_model="phi-3-mini")
    assert kv is not None and kv.kind == KIND_KVSTREAM
    assert ol is not None and ol.kind == KIND_OLLAMA
    assert fo is not None and fo.kind == KIND_OPENAI and fo.has_configured_model
    await client.aclose()
