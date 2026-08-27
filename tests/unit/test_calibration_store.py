"""
G-13: a calibrated budget is only valid for the environment it was measured in.

The store is keyed by (model, device, CLI generation, Foundry version). A record
from elsewhere is worse than no record — it yields a confidently wrong budget —
so partial matches warn and mismatched models are ignored outright.
"""

from __future__ import annotations

import json

from kvstream.admission import resolve_budget
from kvstream.admission.calibration import (
    CalibrationKey,
    lookup_budget,
    resolve_device,
    save_budget,
)
from kvstream.config import AdmissionConfig

KEY = CalibrationKey("phi-3-mini", "npu", "0.10.x", "0.10.0")


def test_entries_do_not_overwrite_each_other(tmp_path):
    store = str(tmp_path / "calibration.json")
    save_budget(store, KEY, 8000)
    save_budget(store, CalibrationKey("qwen-2.5", "npu", "0.10.x", "0.10.0"), 4000)

    assert lookup_budget(store, KEY).budget_tokens == 8000
    data = json.loads(open(store, encoding="utf-8").read())
    assert len(data["entries"]) == 2


def test_exact_match(tmp_path):
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)
    hit = lookup_budget(store, KEY)
    assert hit.match == "exact"
    assert hit.usable


def test_version_change_is_a_partial_match(tmp_path):
    """Same model and device, newer Foundry — usable, but the operator is told."""
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)

    hit = lookup_budget(store, CalibrationKey("phi-3-mini", "npu", "0.10.x", "0.11.0"))
    assert hit.match == "partial"
    assert hit.budget_tokens == 8000
    assert "0.10.0" in hit.detail and "0.11.0" in hit.detail


def test_different_device_is_not_a_match(tmp_path):
    """A budget measured on an NPU means nothing on CPU."""
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)

    hit = lookup_budget(store, CalibrationKey("phi-3-mini", "cpu", "0.10.x", "0.10.0"))
    assert hit.match == "none"
    assert hit.budget_tokens == 0
    assert not hit.usable


def test_different_model_is_not_a_match(tmp_path):
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)
    hit = lookup_budget(store, CalibrationKey("llama-3", "npu", "0.10.x", "0.10.0"))
    assert hit.match == "none"
    assert "phi-3-mini" in hit.detail


def test_missing_store_is_not_an_error(tmp_path):
    hit = lookup_budget(str(tmp_path / "absent.json"), KEY)
    assert hit.match == "none"
    assert hit.budget_tokens == 0


def test_v1_flat_record_is_migrated_not_discarded(tmp_path):
    """The old single-record format held a real measurement; keep it."""
    store = tmp_path / "c.json"
    store.write_text(
        json.dumps(
            {"budget_tokens": 5000, "model": "phi-3-mini",
             "base_url": "http://localhost:5273", "measured_at": 1.0}
        ),
        encoding="utf-8",
    )
    # It carries no device information, so it only matches the unknown-device key.
    hit = lookup_budget(str(store), CalibrationKey("phi-3-mini"))
    assert hit.budget_tokens == 5000

    # And writing a properly-keyed record does not destroy it.
    save_budget(str(store), KEY, 8000)
    assert lookup_budget(str(store), CalibrationKey("phi-3-mini")).budget_tokens == 5000
    assert lookup_budget(str(store), KEY).budget_tokens == 8000


def test_resolve_device_derives_a_label_but_honours_an_explicit_one():
    assert resolve_device("npu") == "npu"
    assert resolve_device("  Arc-A770 ") == "Arc-A770"
    derived = resolve_device("auto")
    assert derived and derived == resolve_device(None) == derived.lower()


def test_token_mode_falls_back_when_the_environment_does_not_match(tmp_path):
    """An unusable record must not silently become an admission budget."""
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)

    cfg = AdmissionConfig(mode="tokens", calibration_store=store, max_concurrency=6)
    budget, unit, provenance = resolve_budget(
        cfg, CalibrationKey("phi-3-mini", "cpu", "0.10.x", "0.10.0")
    )
    assert (budget, unit) == (6, "concurrency")
    assert provenance["source"] == "fallback:concurrency"


def test_token_mode_uses_an_exact_record(tmp_path):
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)
    cfg = AdmissionConfig(mode="tokens", calibration_store=store)
    budget, unit, provenance = resolve_budget(cfg, KEY)
    assert (budget, unit) == (8000, "tokens")
    assert provenance["source"] == "calibration:exact"


def test_configured_budget_beats_the_store(tmp_path):
    store = str(tmp_path / "c.json")
    save_budget(store, KEY, 8000)
    cfg = AdmissionConfig(mode="tokens", budget_tokens=1234, calibration_store=store)
    budget, unit, provenance = resolve_budget(cfg, KEY)
    assert (budget, unit) == (1234, "tokens")
    assert provenance["source"] == "configured"
