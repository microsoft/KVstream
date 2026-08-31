"""
G-11: per-token KV size, used only for *relative* request costing.

The budget is calibrated for one model. When a client names a different one,
charging it the same per token treats a 32-layer model and a 4-layer model as
equal — the fixed-count mistake one level up. This scales by published
architecture, and falls back to "no opinion" whenever it does not know.
"""

from __future__ import annotations

from kvstream.admission.geometry import (
    GeometryRegistry,
    ModelGeometry,
    find_geometry,
    from_config,
)

# phi-3-mini-ish: 32 layers, 32 KV heads, head_dim 96, fp16.
PHI = {
    "num_hidden_layers": 32,
    "num_key_value_heads": 32,
    "head_dim": 96,
    "torch_dtype": "float16",
}
# A grouped-query model: same size, far smaller KV cache.
GQA = {
    "num_hidden_layers": 32,
    "num_attention_heads": 32,
    "num_key_value_heads": 8,
    "hidden_size": 3072,
    "torch_dtype": "float16",
}


def test_kv_bytes_per_token():
    g = ModelGeometry(layers=32, kv_heads=32, head_dim=96, dtype="float16")
    # 2 (K and V) x 32 x 32 x 96 x 2 bytes
    assert g.kv_bytes_per_token == 2 * 32 * 32 * 96 * 2


def test_dtype_changes_the_footprint():
    fp16 = ModelGeometry(2, 2, 64, "float16")
    fp32 = ModelGeometry(2, 2, 64, "float32")
    int8 = ModelGeometry(2, 2, 64, "int8")
    assert fp32.kv_bytes_per_token == 2 * fp16.kv_bytes_per_token
    assert int8.kv_bytes_per_token == fp16.kv_bytes_per_token // 2


def test_from_config_reads_a_standard_model_config():
    g = from_config(PHI)
    assert g is not None
    assert (g.layers, g.kv_heads, g.head_dim, g.dtype) == (32, 32, 96, "float16")


def test_grouped_query_attention_uses_kv_heads_not_attention_heads():
    """Using the attention head count would overstate KV by the grouping factor."""
    g = from_config(GQA)
    assert g is not None
    assert g.kv_heads == 8
    assert g.head_dim == 3072 // 32  # derived from hidden_size / heads


def test_head_dim_is_derived_when_absent():
    g = from_config({"n_layer": 12, "n_head": 12, "n_embd": 768})
    assert g is not None
    assert g.head_dim == 64


def test_incomplete_config_is_not_guessed_at():
    assert from_config({"num_hidden_layers": 32}) is None
    assert from_config({}) is None
    assert from_config({"num_key_value_heads": 8}) is None


def test_find_geometry_searches_a_nested_document():
    """`foundry model show --output json` has no documented schema."""
    payload = {"model": {"id": "phi", "config": PHI}, "status": "loaded"}
    g = find_geometry(payload, source="foundry model show")
    assert g is not None and g.layers == 32
    assert g.source == "foundry model show"


def test_find_geometry_gives_up_quietly():
    assert find_geometry({"status": "loaded"}, "x") is None
    assert find_geometry([1, 2, 3], "x") is None


# -- the registry -------------------------------------------------------


def test_the_anchor_always_weighs_one():
    r = GeometryRegistry("phi-3-mini")
    assert r.weight_for("phi-3-mini") == 1.0


def test_an_unknown_model_is_costed_exactly_as_before():
    """No geometry means no opinion — never a penalty or a discount."""
    r = GeometryRegistry("phi-3-mini")
    r.declare("phi-3-mini", ModelGeometry(**{"layers": 32, "kv_heads": 32, "head_dim": 96}))
    assert r.weight_for("some-other-model") == 1.0


def test_weight_is_unknown_when_the_anchor_is():
    r = GeometryRegistry("phi-3-mini")
    r.declare("other", ModelGeometry(64, 64, 128))
    assert r.weight_for("other") == 1.0


def test_a_heavier_model_costs_more_per_token():
    r = GeometryRegistry("small")
    r.declare("small", ModelGeometry(layers=8, kv_heads=8, head_dim=64))
    r.declare("large", ModelGeometry(layers=32, kv_heads=32, head_dim=64))
    # Four times the layers and four times the KV heads.
    assert r.weight_for("large") == 16.0
    assert r.weight_for("small") == 1.0


def test_a_grouped_query_model_costs_less_than_its_size_suggests():
    r = GeometryRegistry("mha")
    r.declare("mha", ModelGeometry(32, 32, 96))
    r.declare("gqa", ModelGeometry(32, 8, 96))
    assert r.weight_for("gqa") == 0.25


def test_load_config_accepts_model_config_field_names():
    r = GeometryRegistry("phi-3-mini")
    r.load_config({"phi-3-mini": PHI, "gqa-model": GQA})
    assert r.get("phi-3-mini") is not None
    assert 0 < r.weight_for("gqa-model") < 1.0


def test_load_config_ignores_an_entry_it_cannot_understand():
    r = GeometryRegistry("phi-3-mini")
    r.load_config({"phi-3-mini": PHI, "broken": {"layers": "lots"}})
    assert r.get("broken") is None
    assert r.weight_for("broken") == 1.0


def test_stats_exposes_what_is_known():
    r = GeometryRegistry("phi-3-mini")
    r.load_config({"phi-3-mini": PHI})
    stats = r.stats()
    assert stats["anchor"] == "phi-3-mini"
    assert stats["anchor_known"] is True
    assert stats["models"]["phi-3-mini"]["weight"] == 1.0
