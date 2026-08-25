"""
G-01/G-02/G-05/G-10: the Foundry Local CLI adapter.

The 0.10.x CLI ships as signed binaries whose source is not public, so the JSON
schema of `foundry status --output json` is unverified and has no stability
contract. Everything here therefore has to survive a shape it has never seen —
these tests are mostly about *not* breaking.
"""

from __future__ import annotations

import pytest

from kvstream.backend import foundry_cli
from kvstream.backend.foundry_cli import (
    GEN_ABSENT,
    GEN_SDK,
    GEN_SERVICE,
    GEN_UNKNOWN,
    CliEndpoint,
    FoundryCli,
    _classify,
    _collect_model_ids,
    _first_json_document,
    _origin,
    detect,
    extract_endpoint,
    list_models,
    query_endpoint,
    start_command_hint,
)

# -- version classification -------------------------------------------


@pytest.mark.parametrize(
    "output,expected_version,expected_gen",
    [
        ("foundry version 0.8.119", "0.8.119", GEN_SERVICE),
        ("0.8.0", "0.8.0", GEN_SERVICE),
        ("Foundry Local CLI 0.10.0 (Preview)", "0.10.0", GEN_SDK),
        ("0.11.3", "0.11.3", GEN_SDK),
        ("1.0.0", "1.0.0", GEN_SDK),
        ("no version here", None, GEN_UNKNOWN),
        ("", None, GEN_UNKNOWN),
    ],
)
def test_classify(output, expected_version, expected_gen):
    assert _classify(output) == (expected_version, expected_gen)


def test_start_command_hint_names_the_right_command():
    assert start_command_hint(GEN_SERVICE) == "`foundry service start`"
    assert start_command_hint(GEN_SDK) == "`foundry server start`"
    # With nothing detected, name both rather than guessing.
    both = start_command_hint(GEN_ABSENT)
    assert "foundry service start" in both and "foundry server start" in both


# -- endpoint extraction from an unverified schema ---------------------


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"endpoint": "http://127.0.0.1:52149"}, "http://127.0.0.1:52149"),
        ({"service": {"url": "http://127.0.0.1:5273/"}}, "http://127.0.0.1:5273"),
        ({"webService": {"uri": "http://localhost:1234"}}, "http://localhost:1234"),
        ({"model": {"name": "phi"}, "server": {"address": "http://a:1"}}, "http://a:1"),
        ([{"Endpoint": "http://b:2"}], "http://b:2"),
        # No endpoint-ish key: fall back to the first URL-shaped value.
        ({"anything": "see http://c:3 for details"}, "http://c:3"),
    ],
)
def test_extract_endpoint_is_shape_agnostic(payload, expected):
    url, _ = extract_endpoint(payload)
    assert url == expected


def test_extract_endpoint_prefers_an_endpoint_key_over_an_earlier_url():
    payload = {"docs": "https://aka.ms/foundry", "endpoint": "http://127.0.0.1:9"}
    url, keys = extract_endpoint(payload)
    assert url == "http://127.0.0.1:9"
    assert "docs" in keys


@pytest.mark.parametrize("payload", [{}, {"status": "running"}, [], None, 42])
def test_extract_endpoint_returns_none_when_there_is_no_url(payload):
    assert extract_endpoint(payload) == (None, [])


def test_first_json_document_ignores_a_banner():
    text = 'Foundry Local\nchecking...\n{"endpoint": "http://x:1"}\nDone.'
    assert _first_json_document(text) == '{"endpoint": "http://x:1"}'


def test_first_json_document_handles_arrays_and_plain_text():
    assert _first_json_document('[{"a": 1}]') == '[{"a": 1}]'
    assert _first_json_document("not json at all") == "not json at all"


def test_collect_model_ids():
    payload = {"models": [{"id": "phi-3-mini", "size": 1}, {"alias": "qwen-2.5"}]}
    assert _collect_model_ids(payload) == ["phi-3-mini", "qwen-2.5"]
    assert _collect_model_ids({"nothing": 1}) is None


# -- detection and querying, all failure-tolerant ----------------------


@pytest.mark.asyncio
async def test_detect_reports_absent_when_there_is_no_binary(monkeypatch):
    monkeypatch.setattr(foundry_cli.shutil, "which", lambda name: None)
    cli = await detect()
    assert cli.generation == GEN_ABSENT
    assert cli.available is False
    assert "PATH" in cli.detail


@pytest.mark.asyncio
async def test_detect_classifies_a_real_binary(monkeypatch):
    monkeypatch.setattr(foundry_cli.shutil, "which", lambda name: "/usr/bin/foundry")
    monkeypatch.setattr(
        foundry_cli, "_run", lambda argv, timeout: (0, "Foundry Local CLI 0.10.0 (Preview)")
    )
    cli = await detect()
    assert (cli.version, cli.generation) == ("0.10.0", GEN_SDK)
    assert cli.start_command == "`foundry server start`"


@pytest.mark.asyncio
async def test_detect_survives_a_binary_that_will_not_run(monkeypatch):
    monkeypatch.setattr(foundry_cli.shutil, "which", lambda name: "/usr/bin/foundry")
    monkeypatch.setattr(foundry_cli, "_run", lambda argv, timeout: None)
    cli = await detect()
    assert cli.generation == GEN_UNKNOWN
    assert cli.available is True


@pytest.mark.asyncio
async def test_query_endpoint_reads_the_sdk_dialect(monkeypatch):
    def fake_run(argv, timeout):
        if argv[1:] == ["status", "--output", "json"]:
            return 0, '{"webService": {"endpoint": "http://127.0.0.1:52149"}}'
        return 1, ""

    monkeypatch.setattr(foundry_cli, "_run", fake_run)
    found = await query_endpoint(FoundryCli(path="foundry", version="0.10.0", generation=GEN_SDK))
    assert found is not None
    assert found.url == "http://127.0.0.1:52149"
    assert found.command == "foundry status --output json"


@pytest.mark.asyncio
async def test_query_endpoint_reads_the_service_dialect(monkeypatch):
    def fake_run(argv, timeout):
        if argv[1:] == ["service", "status"]:
            return 0, "Model management service is running on http://127.0.0.1:5273/openai/status"
        return 1, "unknown command"

    monkeypatch.setattr(foundry_cli, "_run", fake_run)
    found = await query_endpoint(
        FoundryCli(path="foundry", version="0.8.119", generation=GEN_SERVICE)
    )
    assert found is not None
    assert found.url.startswith("http://127.0.0.1:5273")
    assert found.command == "foundry service status"


@pytest.mark.asyncio
async def test_a_misclassified_binary_still_gets_both_dialects(monkeypatch):
    """Classification is a hint, not a contract — try the other dialect too."""

    def fake_run(argv, timeout):
        if argv[1:] == ["service", "status"]:
            return 0, "running on http://127.0.0.1:5273"
        return 2, "error: unknown command 'status'"

    monkeypatch.setattr(foundry_cli, "_run", fake_run)
    found = await query_endpoint(FoundryCli(path="foundry", generation=GEN_UNKNOWN))
    assert found is not None and found.url == "http://127.0.0.1:5273"


@pytest.mark.asyncio
async def test_malformed_json_is_a_miss_not_an_error(monkeypatch):
    """Any parse failure must fall through to the scan, never raise."""
    monkeypatch.setattr(foundry_cli, "_run", lambda argv, timeout: (0, "{not json"))
    assert await query_endpoint(FoundryCli(path="foundry", generation=GEN_SDK)) is None


@pytest.mark.asyncio
async def test_json_without_an_endpoint_is_a_miss(monkeypatch):
    monkeypatch.setattr(
        foundry_cli, "_run", lambda argv, timeout: (0, '{"status": "running", "models": []}')
    )
    assert await query_endpoint(FoundryCli(path="foundry", generation=GEN_SDK)) is None


@pytest.mark.asyncio
async def test_nonzero_exit_is_a_miss(monkeypatch):
    monkeypatch.setattr(foundry_cli, "_run", lambda argv, timeout: (1, "not running"))
    assert await query_endpoint(FoundryCli(path="foundry", generation=GEN_SDK)) is None


@pytest.mark.asyncio
async def test_query_endpoint_without_a_binary_is_a_miss():
    assert await query_endpoint(FoundryCli()) is None


@pytest.mark.asyncio
async def test_list_models_reads_the_catalog(monkeypatch):
    monkeypatch.setattr(
        foundry_cli,
        "_run",
        lambda argv, timeout: (0, '{"models":[{"id":"phi-3-mini"},{"id":"qwen-2.5"}]}'),
    )
    models = await list_models(FoundryCli(path="foundry", generation=GEN_SDK))
    assert models == ["phi-3-mini", "qwen-2.5"]


@pytest.mark.asyncio
async def test_list_models_is_not_attempted_on_the_service_cli():
    """`foundry model list --output json` is a 0.10.x surface."""
    assert await list_models(FoundryCli(path="foundry", generation=GEN_SERVICE)) is None

# -- what the real CLI actually prints ---------------------------------


def test_service_status_reports_a_status_page_not_an_api_root():
    """
    Regression, found against a live `foundry 0.8.119`.

    `foundry service status` prints the *status page* URL:

        Model management service is running on http://127.0.0.1:64164/openai/status

    Taking that literally means probing `.../openai/status/v1/models`, which
    404s — so the CLI-assisted step silently never worked for the very
    generation it was written for. It degraded to the port scan, which is why
    nothing looked broken until it was run against real hardware.
    """
    endpoint = CliEndpoint(
        url="http://127.0.0.1:64164/openai/status", command="foundry service status"
    )
    assert endpoint.candidates() == [
        "http://127.0.0.1:64164/openai/status",
        "http://127.0.0.1:64164",
    ]


def test_an_endpoint_already_at_the_root_is_tried_once():
    endpoint = CliEndpoint(url="http://127.0.0.1:5273", command="foundry status")
    assert endpoint.candidates() == ["http://127.0.0.1:5273"]


@pytest.mark.parametrize(
    "reported,origin",
    [
        ("http://127.0.0.1:64164/openai/status", "http://127.0.0.1:64164"),
        ("https://host:8443/foundry/v1/", "https://host:8443"),
        ("http://localhost:1234", "http://localhost:1234"),
        ("not-a-url", "not-a-url"),
    ],
)
def test_origin_strips_the_path(reported, origin):
    assert _origin(reported) == origin


@pytest.mark.asyncio
async def test_the_live_service_status_string_round_trips(monkeypatch):
    """End to end from the exact bytes the real CLI emitted on this machine."""
    real_output = (
        "\U0001f7e2 Model management service is running on "
        "http://127.0.0.1:64164/openai/status\n"
        "EP autoregistration status: Skipping EP registration.\n"
    )
    monkeypatch.setattr(foundry_cli, "_run", lambda argv, timeout: (0, real_output))
    found = await query_endpoint(
        FoundryCli(path="foundry", version="0.8.119", generation=GEN_SERVICE)
    )
    assert found is not None
    assert "http://127.0.0.1:64164" in found.candidates()
