"""
KVStream Proxy — OpenAI-compatible HTTP server.

Exposes:
  POST /v1/chat/completions   — primary endpoint (streaming + non-streaming)
  GET  /v1/models             — list available models
  GET  /health                — liveness probe
  GET  /metrics               — Prometheus metrics
  GET  /status                — live scheduler + memory stats (JSON)
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from kvstream.backends.base import Token
from kvstream.engine import KVStreamEngine

logger = logging.getLogger("kvstream.proxy")


# ------------------------------------------------------------------
# Request / Response models (OpenAI-compatible)
# ------------------------------------------------------------------


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    max_tokens: int = Field(default=512, gt=0, le=32768)
    temperature: float = Field(default=0.8, ge=0.0, le=2.0)
    top_p: float = Field(default=0.95, ge=0.0, le=1.0)
    stream: bool = False
    stop: list[str] | None = None
    n: int = Field(default=1, ge=1, le=1)  # multi-choice sampling not supported


def build_app(engine: KVStreamEngine) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start the continuous-batching scheduler + prefix-cache eviction loops
        engine._ensure_background_tasks()
        # Pre-warm backend discovery so the first /health probe is instant
        try:
            await engine.backend.health()
        except Exception:
            pass  # Discovery failure is non-fatal; /health will report degraded
        yield
        await engine.shutdown()

    app = FastAPI(
        title="KVStream",
        description="Paged KV allocation and continuous batching for local LLM inference",
        version="0.1.0",
        lifespan=lifespan,
    )

    # ------------------------------------------------------------------
    # Health / readiness
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health():
        backend_ok = await engine.backend.health()
        backend_url = getattr(engine.backend, "base_url", "unknown")
        return {
            "status": "ok" if backend_ok else "degraded",
            "backend": engine.config.backend.type
            if isinstance(engine.config.backend.type, str)
            else engine.config.backend.type.value,
            "backend_url": backend_url,
            "backend_model": getattr(engine.backend, "model", engine.config.backend.model),
            "backend_healthy": backend_ok,
        }

    @app.get("/status")
    async def status():
        return {
            "scheduler": engine.scheduler.stats(),
            "prefix_cache": engine.prefix_cache.stats() if engine.prefix_cache else {},
            "memory": {
                "gpu_blocks_free": engine.block_manager.num_free_gpu_blocks(),
                "gpu_utilization": round(engine.block_manager.utilization(), 3),
                "cpu_blocks_free": engine.block_manager.num_free_cpu_blocks(),
            },
        }

    # ------------------------------------------------------------------
    # Models
    # ------------------------------------------------------------------

    @app.get("/v1/models")
    async def list_models():
        models = await engine.backend.list_models()
        return {
            "object": "list",
            "data": [{"id": m, "object": "model", "owned_by": "kvstream"} for m in models],
        }

    # ------------------------------------------------------------------
    # Chat completions
    # ------------------------------------------------------------------

    @app.post("/v1/chat/completions")
    async def chat_completions(req: ChatCompletionRequest, http_req: Request):
        request_id = f"kvs-{uuid.uuid4().hex[:8]}"

        # Flatten messages to a single prompt string
        prompt = _messages_to_prompt(req.messages)

        if req.stream:
            return StreamingResponse(
                _stream_response(engine, request_id, prompt, req),
                media_type="text/event-stream",
                headers={"X-Request-Id": request_id},
            )
        else:
            try:
                return await _blocking_response(engine, request_id, prompt, req)
            except RuntimeError:
                # Admission timeout — batch saturated
                raise HTTPException(status_code=503, detail="server overloaded, retry later")

    # ------------------------------------------------------------------
    # Prometheus metrics
    # ------------------------------------------------------------------

    @app.get("/metrics")
    async def metrics():
        from fastapi.responses import Response
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


# ------------------------------------------------------------------
# Streaming helpers
# ------------------------------------------------------------------


async def _stream_response(
    engine: KVStreamEngine,
    request_id: str,
    prompt: str,
    req: ChatCompletionRequest,
) -> AsyncIterator[bytes]:
    """
    Yield SSE chunks for a streaming chat completion.

    Key invariant: the HTTP 200 status is committed by FastAPI/Starlette the
    moment the *first* byte is yielded.  If the backend fails before producing
    any tokens we MUST raise before yielding so that FastAPI can still return
    a proper 4xx/5xx status code to the caller.

    Strategy:
      1. Fetch the first token without yielding (pre-flight).
      2. If that raises → raise HTTPException(502) before any bytes leave —
         bench_lib / any client sees a real error status.
      3. Only after the first token is in hand do we yield (commit HTTP 200)
         and continue streaming the remainder.
    """
    created = int(time.time())

    gen = cast(
        AsyncGenerator[Token, None],
        engine.generate(
            prompt=prompt,
            max_new_tokens=req.max_tokens,
            temperature=req.temperature,
            top_p=req.top_p,
            stop=req.stop,
        ),
    )

    # ── Pre-flight: get the first token before committing to HTTP 200 ────────
    try:
        first_token = await gen.__anext__()
    except StopAsyncIteration:
        # Backend produced zero tokens — send [DONE] and return.
        yield b"data: [DONE]\n\n"
        return
    except Exception as _exc:
        # Backend is unreachable or errored before the first token.
        # Close the generator so engine.generate()'s finally blocks run
        # (releases _backend_slots semaphore, aborts scheduler entry).
        await gen.aclose()
        logger.error(f"[{request_id}] backend failed before first token: {_exc}")
        # Raise BEFORE yielding — FastAPI returns 502, not 200+error-chunk.
        raise HTTPException(status_code=502, detail=str(_exc))
    # ─────────────────────────────────────────────────────────────────────────

    def _make_chunk(token: Token) -> bytes:
        chunk = {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": req.model,
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": token.text},
                    "finish_reason": token.finish_reason,
                }
            ],
        }
        return f"data: {json.dumps(chunk)}\n\n".encode()

    # Yield first token — this is the moment HTTP 200 is sent.
    yield _make_chunk(first_token)
    if first_token.finish_reason:
        yield b"data: [DONE]\n\n"
        return

    # Stream the remainder.  Mid-stream errors are delivered as error chunks
    # (HTTP status already committed to 200 at this point).
    try:
        async for token in gen:
            yield _make_chunk(token)
            if token.finish_reason:
                break
    except Exception as _exc:
        logger.error(f"[{request_id}] stream failed mid-generation: {_exc}")
        error_chunk = {"error": {"message": str(_exc), "type": "server_error"}}
        yield f"data: {json.dumps(error_chunk)}\n\n".encode()
    finally:
        yield b"data: [DONE]\n\n"


async def _blocking_response(
    engine: KVStreamEngine,
    request_id: str,
    prompt: str,
    req: ChatCompletionRequest,
) -> JSONResponse:
    # Tokenize once up-front so usage counts reflect real token count,
    # not the highly inaccurate whitespace-split approximation.
    prompt_token_ids = await engine.backend.tokenize(prompt)
    tokens = []
    finish_reason = "stop"
    async for token in engine.generate(
        prompt=prompt,
        max_new_tokens=req.max_tokens,
        temperature=req.temperature,
        top_p=req.top_p,
        stop=req.stop,
    ):
        tokens.append(token.text)
        if token.finish_reason:
            finish_reason = token.finish_reason

    content = "".join(tokens)
    prompt_token_count = len(prompt_token_ids)
    return JSONResponse(
        {
            "id": request_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": req.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": finish_reason,
                }
            ],
            "usage": {
                "prompt_tokens": prompt_token_count,
                "completion_tokens": len(tokens),
                "total_tokens": prompt_token_count + len(tokens),
            },
        }
    )


def _messages_to_prompt(messages: list[ChatMessage]) -> str:
    """Simple ChatML-style prompt assembly. Override for model-specific templates."""
    parts = []
    for msg in messages:
        if msg.role == "system":
            parts.append(f"<|system|>\n{msg.content}\n")
        elif msg.role == "user":
            parts.append(f"<|user|>\n{msg.content}\n")
        elif msg.role == "assistant":
            parts.append(f"<|assistant|>\n{msg.content}\n")
    parts.append("<|assistant|>\n")
    return "".join(parts)
