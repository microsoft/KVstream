"""
G-03/G-06 and the ordered chain of proposal §8.3.

Explicit configuration → CLI-assisted → localhost scan → actionable failure.
Each step is tested for both "wins" and "falls through".
"""

from __future__ import annotations

import httpx
import pytest

from kvstream.backend import discovery, resolver
from kvstream.backend.foundry_cli import (
    GEN_ABSENT,
    GEN_SDK,
    GEN_SERVICE,
    CliEndpoint,
    FoundryCli,
)
from kvstream.backend.resolver import (
    SOURCE_CLI,
    SOURCE_NONE,
    SOURCE_PINNED,
    SOURCE_SCAN,
    BackendResolver,
)
from kvstream.config import Settings


def _resolver(**kwargs) -> BackendResolver:
    defaults = dict(
        configured_url="http://localhost:5273",
        model="phi-3-mini",
        pinned=False,
        use_cli=True,
    )
    defaults.update(kwargs)
    return BackendResolver(**defaults)  # type: ignore[arg-type]


def _patch(monkeypatch, *, cli=None, endpoint=None, alive=(), scan=None):
    """Stub out the CLI and network so the chain can be driven deterministically."""

    async def fake_detect(path, timeout):
        return cli if cli is not None else FoundryCli(generation=GEN_ABSENT)

    async def fake_query(detected, timeout):
        return endpoint

    async def fake_probe(client, url):
        return 1 if url.rstrip("/") in alive else None

    async def fake_discover(client, configured_url, exclude, prefer_model=None):
        return scan

    monkeypatch.setattr(resolver.foundry_cli, "detect", fake_detect)
    monkeypatch.setattr(resolver.foundry_cli, "query_endpoint", fake_query)
    monkeypatch.setattr(discovery, "probe_url", fake_probe)
    monkeypatch.setattr(discovery, "discover", fake_discover)


@pytest.mark.asyncio
async def test_explicit_configuration_wins_and_skips_everything(monkeypatch):
    """G-03: an operator who names the backend has taken responsibility for it."""
    scanned = False

    async def must_not_scan(*args, **kwargs):
        nonlocal scanned
        scanned = True
        return "http://somewhere-else:1"

    monkeypatch.setattr(discovery, "discover", must_not_scan)

    r = _resolver(pinned=True, configured_url="http://pinned:9000")
    result = await r.resolve(httpx.AsyncClient())

    assert result.url == "http://pinned:9000"
    assert result.source == SOURCE_PINNED
    assert scanned is False


@pytest.mark.asyncio
async def test_pinned_url_wins_even_when_it_is_not_answering(monkeypatch):
    """A pinned URL is a statement of intent, not a hint to be second-guessed."""
    _patch(monkeypatch, alive=(), scan="http://found-by-scan:1")
    r = _resolver(pinned=True, configured_url="http://pinned:9000")
    result = await r.resolve(httpx.AsyncClient())
    assert result.url == "http://pinned:9000"


@pytest.mark.asyncio
async def test_cli_endpoint_is_used_when_it_answers(monkeypatch):
    """G-02: ask Foundry Local where it is before sweeping ports."""
    _patch(
        monkeypatch,
        cli=FoundryCli(path="foundry", version="0.10.0", generation=GEN_SDK),
        endpoint=CliEndpoint(url="http://127.0.0.1:52149", command="foundry status --output json"),
        alive=("http://127.0.0.1:52149",),
        scan=None,
    )
    result = await _resolver().resolve(httpx.AsyncClient())

    assert result.url == "http://127.0.0.1:52149"
    assert result.source == SOURCE_CLI
    assert "foundry status --output json" in result.detail
    assert "scan" not in result.attempts


@pytest.mark.asyncio
async def test_a_dead_cli_endpoint_falls_through_to_the_scan(monkeypatch):
    """The CLI can report a stale endpoint; the scan exists to rescue that."""
    _patch(
        monkeypatch,
        cli=FoundryCli(path="foundry", version="0.10.0", generation=GEN_SDK),
        endpoint=CliEndpoint(url="http://127.0.0.1:52149", command="foundry status"),
        alive=(),
        scan="http://localhost:61111",
    )
    result = await _resolver().resolve(httpx.AsyncClient())

    assert result.url == "http://localhost:61111"
    assert result.source == SOURCE_SCAN
    assert "foundry-cli-endpoint-dead" in result.attempts


@pytest.mark.asyncio
async def test_scan_is_used_when_the_cli_reports_nothing(monkeypatch):
    _patch(
        monkeypatch,
        cli=FoundryCli(path="foundry", version="0.8.119", generation=GEN_SERVICE),
        endpoint=None,
        scan="http://localhost:5273",
    )
    result = await _resolver().resolve(httpx.AsyncClient())
    assert result.source == SOURCE_SCAN
    assert result.attempts == [f"foundry-cli({GEN_SERVICE})", "scan"]


@pytest.mark.asyncio
async def test_cli_is_skipped_entirely_when_disabled(monkeypatch):
    """G-04: shelling out is wrong in a container."""
    called = False

    async def fake_detect(path, timeout):
        nonlocal called
        called = True
        return FoundryCli(path="foundry", generation=GEN_SDK)

    _patch(monkeypatch, scan="http://localhost:5273")
    monkeypatch.setattr(resolver.foundry_cli, "detect", fake_detect)

    result = await _resolver(use_cli=False).resolve(httpx.AsyncClient())
    assert result.source == SOURCE_SCAN
    assert called is False
    assert not any(a.startswith("foundry-cli") for a in result.attempts)


@pytest.mark.asyncio
async def test_failure_names_the_detected_generations_start_command(monkeypatch):
    """G-05: tell the operator the command that would actually fix this."""
    _patch(
        monkeypatch,
        cli=FoundryCli(path="foundry", version="0.8.119", generation=GEN_SERVICE),
        endpoint=None,
        scan=None,
    )
    r = _resolver()
    result = await r.resolve(httpx.AsyncClient())

    assert result.url is None
    assert result.source == SOURCE_NONE
    assert "foundry service start" in result.detail
    assert "foundry server start" not in result.detail


@pytest.mark.asyncio
async def test_failure_without_a_cli_names_both_and_the_explicit_url_escape(monkeypatch):
    _patch(monkeypatch, cli=FoundryCli(generation=GEN_ABSENT), endpoint=None, scan=None)
    result = await _resolver().resolve(httpx.AsyncClient())

    assert result.url is None
    assert "foundry service start" in result.detail
    assert "foundry server start" in result.detail
    assert "--backend-url" in result.detail


# -- how "explicit" is decided ----------------------------------------


def test_default_url_is_not_treated_as_explicit():
    """The default base_url is a starting guess, not a statement."""
    s = Settings()
    assert s.backend_url_is_explicit() is False
    assert s.backend_is_pinned() is False


def test_config_file_url_is_explicit():
    s = Settings(**{"backend": {"base_url": "http://configured:1234"}})
    assert s.backend_url_is_explicit() is True
    assert s.backend_is_pinned() is True


def test_env_url_is_explicit(monkeypatch):
    monkeypatch.setenv("KVSTREAM_BACKEND__BASE_URL", "http://env:1234")
    s = Settings()
    assert s.backend_is_pinned() is True


def test_cli_flag_url_is_explicit():
    """`--backend-url` assigns onto the model, which records the field as set."""
    s = Settings()
    s.backend.base_url = "http://flag:1234"
    assert s.backend_is_pinned() is True


def test_pin_url_can_be_overridden_both_ways():
    s = Settings(**{"backend": {"base_url": "http://configured:1", "pin_url": False}})
    assert s.backend_is_pinned() is False

    s2 = Settings(**{"backend": {"pin_url": True}})
    assert s2.backend_is_pinned() is True


def test_foundry_cli_mode(monkeypatch):
    monkeypatch.setattr(resolver.foundry_cli, "in_container", lambda: False)
    assert Settings().foundry_cli_enabled() is True
    assert Settings(**{"backend": {"use_foundry_cli": "never"}}).foundry_cli_enabled() is False

    monkeypatch.setattr(resolver.foundry_cli, "in_container", lambda: True)
    assert Settings().foundry_cli_enabled() is False
    # "always" is how you opt back in when you know the binary is there.
    assert Settings(**{"backend": {"use_foundry_cli": "always"}}).foundry_cli_enabled() is True
