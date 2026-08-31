"""
A small, deterministic corpus of the text shapes a local gateway actually sees.

Generated rather than checked in as data so the file stays readable and the
sample sizes are easy to change. Everything is seeded, so two runs of the
benchmark on the same machine produce the same numbers.

The four shapes matter because they stress token estimation differently:

* **prose** — long words, few punctuation marks; the character rate dominates.
* **json** — punctuation-dense; the unit count dominates and a plain chars/4
  rule under-counts badly.
* **code** — mixed, with identifiers that split into several tokens.
* **chat** — a re-sent agent transcript, including a tool call, which is what a
  planner loop actually re-sends on every turn.
"""

from __future__ import annotations

import json
import random

PROSE_FRAGMENTS = [
    "Admission control keeps a single-box runtime responsive under concurrent load.",
    "The limiter on one device is key-value cache memory, which scales with context length.",
    "Past its optimal concurrency the runtime accepts every request and latency spikes.",
    "A gateway cannot read the engine's memory, so the budget has to be measured.",
    "Multi-agent orchestration fires bursts of requests that arrive together.",
    "Retrieval fan-out produces many small calls with a shared system prompt.",
    "Backpressure is only useful when the client can act on it.",
    "Calibration is per model and per device, and has to be repeated when either changes.",
    "Estimating token counts without the model's tokenizer is approximate by construction.",
    "Streaming lets the proxy observe completion and free the reservation promptly.",
]

CODE_FRAGMENTS = [
    "async def admit(self, req_id: str, cost: int) -> None:\n"
    "    async with self._lock:\n"
    "        if not self._queue and self._can_fit(cost):\n"
    "            self._reserve(req_id, cost)\n"
    "            return\n",
    "def kv_bytes_per_token(layers: int, kv_heads: int, head_dim: int) -> int:\n"
    "    return 2 * layers * kv_heads * head_dim * 2\n",
    "for choice in data.get('choices', []):\n"
    "    delta = choice.get('delta') or {}\n"
    "    text += delta.get('content') or ''\n",
    "class GeometryRegistry:\n"
    "    def weight_for(self, model: str) -> float:\n"
    "        mine = self._geometry.get(model)\n"
    "        return 1.0 if mine is None else mine.kv_bytes_per_token / self._anchor_bytes\n",
]

TOOL_NAMES = [
    "get_weather",
    "search_documents",
    "run_query",
    "send_email",
    "list_files",
]
CITIES = ["Lisbon", "Seattle", "Reykjavik", "Bengaluru", "Kraków"]


def _prose(rng: random.Random, sentences: int) -> str:
    return " ".join(rng.choice(PROSE_FRAGMENTS) for _ in range(sentences))


def _json_blob(rng: random.Random, rows: int) -> str:
    payload = {
        "results": [
            {
                "id": f"doc-{rng.randint(1000, 9999)}",
                "score": round(rng.random(), 6),
                "title": _prose(rng, 1)[:60],
                "metadata": {
                    "source": "index",
                    "chunk": rng.randint(0, 40),
                    "lang": "en",
                },
            }
            for _ in range(rows)
        ],
        "took_ms": rng.randint(1, 400),
        "truncated": rng.choice([True, False]),
    }
    return json.dumps(payload, indent=2)


def _code(rng: random.Random, blocks: int) -> str:
    return "\n\n".join(rng.choice(CODE_FRAGMENTS) for _ in range(blocks))


def _chat_transcript(rng: random.Random, turns: int) -> str:
    city = rng.choice(CITIES)
    tool = rng.choice(TOOL_NAMES)
    messages = [
        {"role": "system", "content": _prose(rng, 2)},
        {"role": "user", "content": f"What is the weather in {city}?"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": f"call_{rng.randint(100000, 999999)}",
                    "type": "function",
                    "function": {"name": tool, "arguments": json.dumps({"city": city})},
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_x",
            "content": f"{rng.randint(-5, 35)}C, clear",
        },
    ]
    for _ in range(turns):
        messages.append({"role": "user", "content": _prose(rng, 1)})
        messages.append({"role": "assistant", "content": _prose(rng, 2)})
    return json.dumps(messages)


def build_corpus(seed: int = 20260824, per_shape: int = 60) -> dict[str, list[str]]:
    """Return ``{shape: [sample, ...]}``, deterministic for a given seed."""
    rng = random.Random(seed)
    return {
        "prose": [_prose(rng, rng.randint(1, 12)) for _ in range(per_shape)],
        "json": [_json_blob(rng, rng.randint(1, 8)) for _ in range(per_shape)],
        "code": [_code(rng, rng.randint(1, 4)) for _ in range(per_shape)],
        "chat": [_chat_transcript(rng, rng.randint(0, 4)) for _ in range(per_shape)],
    }
