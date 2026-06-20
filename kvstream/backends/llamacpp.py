"""
LlamaCppBackend — adapter for llama.cpp server with hard KV injection.

llama.cpp server exposes a `/slots` API that allows saving and restoring
the raw KV cache state per inference slot. This enables true zero-recompute
prefix caching: KVStream saves the KV state after a prefix is processed, and
restores it for every subsequent request with the same prefix.

Start llama.cpp server with slot support:
    ./llama-server -m model.gguf --slots 8 --port 8080 --cont-batching
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import AsyncIterator

import httpx

from kvstream.backends.base import BaseBackend, GenerateRequest, Token


class LlamaCppBackend(BaseBackend):
    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        model: str = "local",
        timeout: float = 120.0,
        kv_cache_dir: str | None = None,
        num_slots: int = 8,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.kv_cache_dir = kv_cache_dir or os.path.join(
            tempfile.gettempdir(), "kvstream_kv_cache"
        )
        self.num_slots = num_slots
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))
        os.makedirs(self.kv_cache_dir, exist_ok=True)

    def supports_hard_kv_inject(self) -> bool:
        return True

    async def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        payload = {
            "prompt": request.prompt,
            "n_predict": request.max_new_tokens,
            "temperature": request.temperature,
            "top_p": request.top_p,
            "stop": request.stop or [],
            "stream": True,
            "cache_prompt": True,  # enable llama.cpp prompt caching
        }

        async with self._client.stream(
            "POST",
            f"{self.base_url}/completion",
            json=payload,
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                raw = line[6:].strip()
                if raw == "[DONE]":
                    break
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                text = data.get("content", "")
                stop = data.get("stop", False)
                yield Token(
                    text=text,
                    token_id=data.get("id_slot", 0),
                    finish_reason="stop" if stop else None,
                )
                if stop:
                    break

    async def health(self) -> bool:
        try:
            resp = await self._client.get(f"{self.base_url}/health", timeout=5.0)
            data = resp.json()
            return data.get("status") == "ok"
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Hard KV inject via /slots API
    # ------------------------------------------------------------------

    async def save_kv_state(self, seq_id: str, slot_id: int, path: str) -> bool:
        """
        Save the KV cache of a slot to a binary file.
        File path is relative to llama.cpp server's working directory.
        """
        try:
            resp = await self._client.post(
                f"{self.base_url}/slots/{slot_id}",
                json={"action": "save", "filename": path},
                timeout=30.0,
            )
            return resp.status_code == 200
        except Exception:
            return False

    async def restore_kv_state(self, seq_id: str, slot_id: int, path: str) -> bool:
        """Restore KV state from a previously saved file."""
        try:
            resp = await self._client.post(
                f"{self.base_url}/slots/{slot_id}",
                json={"action": "restore", "filename": path},
                timeout=30.0,
            )
            return resp.status_code == 200
        except Exception:
            return False

    async def list_slots(self) -> list[dict]:
        """Get current slot status from llama.cpp."""
        try:
            resp = await self._client.get(f"{self.base_url}/slots")
            return resp.json()
        except Exception:
            return []

    async def list_models(self) -> list[str]:
        try:
            resp = await self._client.get(f"{self.base_url}/props")
            data = resp.json()
            return [data.get("model_path", self.model)]
        except Exception:
            return [self.model]

    async def aclose(self) -> None:
        await self._client.aclose()
