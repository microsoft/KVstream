"""
OllamaBackend — adapter for Ollama (http://localhost:11434).

Ollama wraps llama.cpp and exposes:
  - OpenAI-compatible /v1/chat/completions (streaming)
  - Native /api/generate (streaming, with context token IDs)
  - /api/embeddings for tokenization approximation
  - llama.cpp /slots API for hard KV inject (when using llama.cpp server directly)

For prefix caching, we use Ollama's native `keep_alive` and context
carry-over features to minimize re-computation.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx

from kvstream.backends.base import BaseBackend, GenerateRequest, Token


class OllamaBackend(BaseBackend):
    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama3.2",
        timeout: float = 120.0,
        keep_alive: str = "10m",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.keep_alive = keep_alive
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))

    async def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        options: dict = {
            "num_predict": request.max_new_tokens,
            "temperature": request.temperature,
            "top_p": request.top_p,
        }
        if request.stop:
            options["stop"] = request.stop
        payload = {
            "model": self.model,
            "prompt": request.prompt,
            "stream": True,
            "keep_alive": self.keep_alive,
            "options": options,
        }

        async with self._client.stream(
            "POST",
            f"{self.base_url}/api/generate",
            json=payload,
        ) as resp:
            if resp.status_code >= 400:
                body = (await resp.aread()).decode(errors="replace")
                raise RuntimeError(f"Ollama returned HTTP {resp.status_code}: {body[:200]}")
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                text = data.get("response", "")
                done = data.get("done", False)
                finish_reason = "stop" if done else None

                if text or done:
                    yield Token(
                        text=text,
                        token_id=0,  # Ollama doesn't expose token IDs in /api/generate
                        finish_reason=finish_reason,
                    )
                if done:
                    break

    async def health(self) -> bool:
        try:
            resp = await self._client.get(f"{self.base_url}/api/tags", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    async def list_models(self) -> list[str]:
        try:
            resp = await self._client.get(f"{self.base_url}/api/tags")
            resp.raise_for_status()
            data = resp.json()
            return [m["name"] for m in data.get("models", [])]
        except Exception:
            return []

    async def tokenize(self, text: str) -> list[int]:
        """Use Ollama's tokenize endpoint (available in newer versions)."""
        try:
            resp = await self._client.post(
                f"{self.base_url}/api/tokenize",
                json={"model": self.model, "prompt": text},
                timeout=10.0,
            )
            if resp.status_code == 200:
                return resp.json().get("tokens", [])
        except Exception:
            pass
        # Fallback: UTF-8 bytes
        return list(text.encode("utf-8"))

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # OpenAI-compatible path (used by the proxy for pass-through)
    # ------------------------------------------------------------------

    async def chat_completions(
        self,
        messages: list[dict],
        max_tokens: int = 512,
        temperature: float = 0.8,
        stream: bool = True,
    ) -> AsyncIterator[bytes]:
        """Stream raw SSE chunks from the OpenAI-compat endpoint."""
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": stream,
        }
        async with self._client.stream(
            "POST",
            f"{self.base_url}/v1/chat/completions",
            json=payload,
        ) as resp:
            resp.raise_for_status()
            async for chunk in resp.aiter_bytes():
                yield chunk
