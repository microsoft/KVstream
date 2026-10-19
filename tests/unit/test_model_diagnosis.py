"""
Model inventory decisions, the startup model check and the serve banner.

Values are the ones observed live on Foundry Local CLI 0.10.3:

* `/v1/models` lists every *downloaded* model, without its version suffix,
  and keeps listing it after `foundry model unload`;
* `foundry cache list --output json` reports the versioned id with a
  per-model `loaded` flag;
* `foundry model list` lists the catalog alias and the versioned id.
"""

from __future__ import annotations

import pytest

from kvstream.backend.inventory import (
    ModelInventory,
    Snapshot,
    diagnose,
    refusal_for,
    select_model,
)
from kvstream.cli import _backend_label
from kvstream.config import Settings

PHI = "Phi-3-mini-4k-instruct-generic-gpu"
PHI_V = "Phi-3-mini-4k-instruct-generic-gpu:2"
QWEN_V = "qwen2.5-0.5b-instruct-generic-cpu:3"
CATALOG = ["phi-3-mini-4k", PHI_V, "qwen2.5-0.5b"]

LOADED = Snapshot(downloaded=frozenset({PHI}), loaded=frozenset({PHI_V}))
UNLOADED = Snapshot(downloaded=frozenset({PHI}), loaded=frozenset())
NO_CLI = Snapshot(downloaded=frozenset({PHI}), loaded=None)
UNKNOWN = Snapshot()


# -- auto-detection -----------------------------------------------------


def test_the_one_loaded_model_is_selected():
    selection = select_model(LOADED)
    assert selection.model == PHI_V
    assert "only model loaded" in selection.detail


def test_a_downloaded_but_unloaded_model_is_not_selected():
    # /v1/models still lists it, but a request for it is refused.
    selection = select_model(UNLOADED)
    assert selection.model is None
    assert "no model is loaded" in selection.detail
    assert "foundry model load" in selection.detail


def test_several_loaded_models_are_never_guessed_between():
    snap = Snapshot(downloaded=frozenset({PHI, "qwen"}), loaded=frozenset({PHI_V, QWEN_V}))
    selection = select_model(snap)
    assert selection.model is None
    assert "2 models are loaded" in selection.detail
    assert "--model" in selection.detail


def test_without_the_cli_a_single_listed_model_is_used_but_not_claimed_loaded():
    selection = select_model(NO_CLI)
    assert selection.model == PHI
    assert "could not be checked" in selection.detail


def test_nothing_readable_means_no_model():
    selection = select_model(UNKNOWN)
    assert selection.model is None
    assert "could not read" in selection.detail


# -- refusing streams that cannot succeed --------------------------------


def test_loaded_model_passes_with_or_without_version_suffix():
    assert refusal_for(PHI, LOADED) is None
    assert refusal_for(PHI_V, LOADED) is None


def test_downloaded_but_unloaded_model_is_refused_as_not_loaded():
    refusal = refusal_for(PHI, UNLOADED)
    assert refusal is not None
    assert refusal.code == "model_not_loaded"


def test_catalog_alias_is_refused_as_not_found():
    refusal = refusal_for("phi-3-mini-4k", LOADED)
    assert refusal is not None
    assert refusal.code == "model_not_found"


def test_unreadable_lists_fail_open():
    assert refusal_for("anything", UNKNOWN) is None
    assert refusal_for(PHI, NO_CLI) is None


# -- startup diagnosis -----------------------------------------------------


def test_no_warning_for_a_loaded_model():
    assert diagnose(PHI, LOADED, CATALOG) is None


def test_alias_warning_names_the_id_to_use():
    problem = diagnose("phi-3-mini-4k", LOADED, CATALOG)
    assert problem is not None
    assert "alias" in problem
    assert PHI in problem


def test_unloaded_model_warning_says_to_load_it():
    problem = diagnose(PHI, UNLOADED, CATALOG)
    assert problem is not None
    assert "not loaded" in problem


def test_catalog_catches_typos_when_the_api_list_is_unavailable():
    assert diagnose(PHI_V, UNKNOWN, CATALOG) is None
    problem = diagnose("typo-model", UNKNOWN, CATALOG)
    assert problem is not None
    assert "not in Foundry Local's catalog" in problem


def test_no_information_means_no_warning():
    assert diagnose("anything", UNKNOWN, None) is None


# -- the cached reader -----------------------------------------------------


@pytest.mark.asyncio
async def test_loaded_list_is_not_read_when_the_api_is_down():
    """On CLI 0.10.3 `foundry cache list` starts a stopped daemon; never trigger that."""
    cli_calls = 0

    async def api_down():
        return None

    async def cli():
        nonlocal cli_calls
        cli_calls += 1
        return [PHI_V]

    snap = await ModelInventory(api_down, cli).snapshot()
    assert snap.downloaded is None and snap.loaded is None
    assert cli_calls == 0


@pytest.mark.asyncio
async def test_forced_refreshes_are_rate_limited():
    reads = 0

    async def api():
        nonlocal reads
        reads += 1
        return [PHI]

    async def cli():
        return [PHI_V]

    inventory = ModelInventory(api, cli, min_refresh_seconds=60)
    await inventory.snapshot()
    for _ in range(5):
        await inventory.snapshot(fresh=True)
    assert reads == 1


@pytest.mark.asyncio
async def test_a_stale_snapshot_answers_while_refreshing_in_the_background():
    loaded = [PHI_V]

    async def api():
        return [PHI]

    async def cli():
        return list(loaded)

    inventory = ModelInventory(api, cli, stale_seconds=0.0, min_refresh_seconds=0.0)
    first = await inventory.snapshot()
    loaded.clear()  # the model gets unloaded
    stale = await inventory.snapshot()  # answered from the old reading...
    assert stale is first
    assert inventory._background is not None
    await inventory._background  # ...while the refresh completes
    assert inventory.current.loaded == frozenset()


# -- banner ----------------------------------------------------------------


def test_banner_does_not_present_the_fallback_url_as_the_backend():
    s = Settings()
    assert "auto-discover" in _backend_label(s)
    assert s.backend.base_url not in _backend_label(s)


def test_banner_shows_a_pinned_url():
    s = Settings()
    s.backend.base_url = "http://127.0.0.1:50191"
    assert _backend_label(s) == "http://127.0.0.1:50191"


def test_model_is_not_configured_by_default():
    # The old default, "phi-3-mini", is an id Foundry Local rejects.
    assert Settings().backend.model is None
