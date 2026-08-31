"""
A modelled local runtime, for measuring what admission control is worth.

This is **not** Foundry Local. It is a deliberately simple model of the failure
mode the proposal describes in §2: a single-box runtime that accepts every
request, serves them fine up to some optimal concurrency, degrades sharply past
it, and stops accepting anything at all beyond a hard ceiling.

Modelling it rather than measuring the real thing is a trade, and the reason to
make it is reproducibility: the demonstration runs on any machine, in CI, with
no model download and no GPU. What it shows is that **KVStream keeps a runtime
with these characteristics responsive** — not what Foundry Local's actual
numbers are. Running the same driver against a real instance
(``--backend-url``) is what turns the second claim into a measurement, and that
still has to be done on hardware.

The degradation curve is quadratic past the optimum, which is a guess at the
shape, not a fitted model. It is stated here so nobody has to read the source to
find out.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse


@dataclass
class RuntimeModel:
    """Parameters of the modelled runtime."""

    optimal_concurrency: int = 4
    hard_limit: int = 16
    seconds_per_token: float = 0.0006
    prefill_seconds_per_token: float = 0.00004

    in_flight: int = 0
    peak_in_flight: int = 0
    served: int = 0
    refused: int = 0
    latencies: list[float] = field(default_factory=list)

    def degradation(self, concurrent: int) -> float:
        """
        Latency multiplier at a given concurrency.

        Flat up to the optimum, then quadratic — the "latency spikes past
        optimal concurrency" the proposal describes.
        """
        if concurrent <= self.optimal_concurrency:
            return 1.0
        return (concurrent / self.optimal_concurrency) ** 2

    def stats(self) -> dict:
        return {
            "optimal_concurrency": self.optimal_concurrency,
            "hard_limit": self.hard_limit,
            "peak_in_flight": self.peak_in_flight,
            "served": self.served,
            "refused": self.refused,
        }

    def reset(self) -> None:
        self.in_flight = 0
        self.peak_in_flight = 0
        self.served = 0
        self.refused = 0
        self.latencies.clear()


def build_simulator(model: RuntimeModel | None = None) -> FastAPI:
    runtime = model or RuntimeModel()
    app = FastAPI(title="simulated-foundry-local")
    app.state.runtime = runtime

    @app.get("/v1/models")
    async def models() -> dict:
        return {"object": "list", "data": [{"id": "sim-model", "object": "model"}]}

    @app.get("/sim/stats")
    async def stats() -> dict:
        return runtime.stats()

    @app.post("/sim/reset")
    async def reset() -> dict:
        runtime.reset()
        return {"ok": True}

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        body = await request.json()
        prompt_chars = sum(
            len(str(m.get("content") or "")) for m in body.get("messages", [])
        )
        prompt_tokens = max(1, prompt_chars // 4)
        max_tokens = int(body.get("max_tokens") or 128)

        # Past the hard ceiling the runtime stops accepting work entirely. This
        # is the stall the gateway exists to prevent; without admission control
        # the client sees it as an error storm.
        if runtime.in_flight >= runtime.hard_limit:
            runtime.refused += 1
            return JSONResponse(
                {"error": {"message": "server is at capacity", "type": "overloaded"}},
                status_code=503,
            )

        runtime.in_flight += 1
        runtime.peak_in_flight = max(runtime.peak_in_flight, runtime.in_flight)
        started = time.perf_counter()
        try:
            factor = runtime.degradation(runtime.in_flight)
            prefill = prompt_tokens * runtime.prefill_seconds_per_token * factor
            decode = max_tokens * runtime.seconds_per_token * factor
            await asyncio.sleep(prefill)

            if not body.get("stream"):
                await asyncio.sleep(decode)
                runtime.served += 1
                runtime.latencies.append(time.perf_counter() - started)
                return JSONResponse(_completion(prompt_tokens, max_tokens))

            async def stream():
                per_token = decode / max(1, max_tokens)
                for index in range(max_tokens):
                    await asyncio.sleep(per_token)
                    yield _chunk(f"tok{index} ")
                yield _chunk("", finish="stop")
                yield (
                    "data: "
                    + json.dumps(
                        {
                            "choices": [],
                            "usage": {
                                "prompt_tokens": prompt_tokens,
                                "completion_tokens": max_tokens,
                                "total_tokens": prompt_tokens + max_tokens,
                            },
                        }
                    )
                    + "\n\n"
                )
                yield "data: [DONE]\n\n"

            runtime.served += 1
            runtime.latencies.append(time.perf_counter() - started)
            return StreamingResponse(
                _counted(stream(), runtime), media_type="text/event-stream"
            )
        finally:
            if not body.get("stream"):
                runtime.in_flight -= 1

    return app


async def _counted(source, runtime: RuntimeModel):
    """Hold the in-flight slot for the whole streamed response, then release it."""
    try:
        async for chunk in source:
            yield chunk
    finally:
        runtime.in_flight -= 1


def _chunk(text: str, finish: str | None = None) -> str:
    payload = {
        "id": "sim",
        "object": "chat.completion.chunk",
        "model": "sim-model",
        "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": finish}],
    }
    return f"data: {json.dumps(payload)}\n\n"


def _completion(prompt_tokens: int, completion_tokens: int) -> dict:
    return {
        "id": "sim",
        "object": "chat.completion",
        "model": "sim-model",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "simulated response"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }
