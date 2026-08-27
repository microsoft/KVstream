"""
Model KV geometry — relative request costing across models.

The proposal (§5.2) says per-token KV size from the public model config is used
**only for relative request costing**. This module is that, and nothing more.

Why it is needed
----------------
The calibrated budget ``B`` belongs to one ``(model, device, …)``. But a client
may name any model in its request, and a gateway fronting a Foundry Local that
has two models loaded would otherwise charge a 1k-token request against a
32-layer model exactly what it charges the same request against a 4-layer one.
Those are not the same amount of KV memory, and pretending they are is the
fixed-count mistake all over again, one level up.

KV bytes per token is a published property of a model's architecture::

    kv_bytes_per_token = 2 (K and V) × layers × kv_heads × head_dim × dtype_bytes

What KVStream does with it is take a **ratio**: the request's model against the
model the budget was calibrated for. A budget in "tokens" therefore means
"tokens of the calibrated model", and everything else is converted into that
unit. No absolute claim about device memory is made anywhere — the numbers are
only ever compared with each other.

Where the numbers come from
---------------------------
1. Operator-declared geometry in configuration. Always wins, and is the only
   source that can be called authoritative.
2. ``foundry model show <id> --output json``, best-effort. The schema is
   unverified (see :mod:`kvstream.backend.foundry_cli`), so this walks the
   document for architecture-shaped fields and gives up quietly.
3. Unknown — weight 1.0, i.e. costed exactly as before. An unknown model is
   never penalised or discounted on a guess.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger("kvstream.geometry")

# Bytes per element for the dtypes a local runtime realistically uses.
DTYPE_BYTES = {
    "float32": 4, "fp32": 4, "f32": 4,
    "float16": 2, "fp16": 2, "f16": 2, "half": 2,
    "bfloat16": 2, "bf16": 2,
    "int8": 1, "i8": 1, "q8": 1,
    "int4": 1, "q4": 1,  # 4-bit weights still typically keep an 8-bit KV cache
}
DEFAULT_DTYPE = "float16"

# Field names a model config might use, in the order we prefer them.
_LAYER_KEYS = ("num_hidden_layers", "n_layer", "num_layers", "layers")
_KV_HEAD_KEYS = ("num_key_value_heads", "n_kv_heads", "num_kv_heads")
_HEAD_KEYS = ("num_attention_heads", "n_head", "num_heads")
_HEAD_DIM_KEYS = ("head_dim", "head_size", "size_per_head")
_HIDDEN_KEYS = ("hidden_size", "n_embd", "d_model", "embedding_size")


@dataclass(frozen=True)
class ModelGeometry:
    """Enough of a model's architecture to size its KV cache per token."""

    layers: int
    kv_heads: int
    head_dim: int
    dtype: str = DEFAULT_DTYPE
    source: str = "config"

    @property
    def kv_bytes_per_token(self) -> int:
        dtype_bytes = DTYPE_BYTES.get(self.dtype.lower(), 2)
        return 2 * self.layers * self.kv_heads * self.head_dim * dtype_bytes

    def as_dict(self) -> dict:
        return {
            "layers": self.layers,
            "kv_heads": self.kv_heads,
            "head_dim": self.head_dim,
            "dtype": self.dtype,
            "kv_bytes_per_token": self.kv_bytes_per_token,
            "source": self.source,
        }


def find_geometry(payload: object, source: str) -> ModelGeometry | None:
    """
    Search an arbitrary JSON document for a model config, shape-agnostic.

    ``foundry model show --output json`` has no documented schema, so the
    architecture fields could sit at the top level or nested under a `config`,
    `model` or `architecture` key. Try the document itself, then every mapping
    inside it, and give up quietly.
    """
    if isinstance(payload, dict):
        direct = from_config(payload, source)
        if direct is not None:
            return direct
        for value in payload.values():
            found = find_geometry(value, source)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = find_geometry(value, source)
            if found is not None:
                return found
    return None


def from_config(raw: dict, source: str = "config") -> ModelGeometry | None:
    """
    Build geometry from a model-config-shaped mapping, or ``None``.

    Grouped-query attention means ``kv_heads`` can be far smaller than the
    attention head count, and using the wrong one overstates KV by up to the
    grouping factor — so the key-value head count is preferred and the attention
    head count is only a fallback (correct for multi-head attention models).
    """
    layers = _first_int(raw, _LAYER_KEYS)
    kv_heads = _first_int(raw, _KV_HEAD_KEYS) or _first_int(raw, _HEAD_KEYS)
    if not layers or not kv_heads:
        return None

    head_dim = _first_int(raw, _HEAD_DIM_KEYS)
    if not head_dim:
        hidden = _first_int(raw, _HIDDEN_KEYS)
        heads = _first_int(raw, _HEAD_KEYS) or kv_heads
        if hidden and heads:
            head_dim = hidden // heads
    if not head_dim:
        return None

    dtype = None
    for key in ("torch_dtype", "dtype", "kv_cache_dtype", "precision"):
        value = raw.get(key)
        if isinstance(value, str) and value.lower() in DTYPE_BYTES:
            dtype = value.lower()
            break
    return ModelGeometry(layers, kv_heads, head_dim, dtype or DEFAULT_DTYPE, source)


def _first_int(raw: dict, keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = raw.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int) and value > 0:
            return value
        if isinstance(value, str) and value.isdigit() and int(value) > 0:
            return int(value)
    return None


class GeometryRegistry:
    """
    Relative KV weights per model, anchored on the calibrated model.

    ``weight_for(model)`` returns 1.0 whenever either side is unknown, so an
    un-configured gateway costs requests exactly as it did before this existed.
    """

    def __init__(self, anchor_model: str) -> None:
        self._anchor = anchor_model
        self._geometry: dict[str, ModelGeometry] = {}

    @property
    def anchor(self) -> str:
        return self._anchor

    def declare(self, model: str, geometry: ModelGeometry) -> None:
        self._geometry[model] = geometry
        logger.info(
            "model %r KV geometry: %d bytes/token (%s)",
            model, geometry.kv_bytes_per_token, geometry.source,
        )

    def load_config(self, declared: dict[str, dict]) -> None:
        """Register operator-declared geometry from ``models:`` in the config."""
        for model, spec in (declared or {}).items():
            geometry = from_config(spec, source="config")
            if geometry is None:
                logger.warning(
                    "models.%s does not describe a KV geometry "
                    "(needs layers + kv_heads + head_dim, or hidden_size); ignoring.",
                    model,
                )
                continue
            self.declare(model, geometry)

    def get(self, model: str) -> ModelGeometry | None:
        return self._geometry.get(model)

    def weight_for(self, model: str) -> float:
        """
        Relative per-token KV footprint of ``model`` against the anchor.

        1.0 when the model *is* the anchor, when either geometry is unknown, or
        when the two are the same size. Never a guess: an unknown model is
        costed as if it were the calibrated one, which is the same behaviour as
        having no geometry at all.
        """
        if model == self._anchor:
            return 1.0
        mine = self._geometry.get(model)
        anchor = self._geometry.get(self._anchor)
        if mine is None or anchor is None:
            return 1.0
        anchor_bytes = anchor.kv_bytes_per_token
        if anchor_bytes <= 0:
            return 1.0
        return mine.kv_bytes_per_token / anchor_bytes

    def stats(self) -> dict:
        return {
            "anchor": self._anchor,
            "anchor_known": self._anchor in self._geometry,
            "models": {
                model: {**geometry.as_dict(), "weight": round(self.weight_for(model), 4)}
                for model, geometry in sorted(self._geometry.items())
            },
        }
