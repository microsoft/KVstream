"""Shared in-memory stand-ins for :class:`FoundryClient` used by the API tests."""

from __future__ import annotations

import asyncio

from kvstream.backend.foundry import Token
from kvstream.backend.foundry_cli import GEN_ABSENT, FoundryCli, start_command_hint

PIECES = ["Hello", ", ", "world", "!"]
TEXT = "".join(PIECES)


class StubBackend:
    """
    Deterministic stand-in for FoundryClient.

    Records which path each request took, so tests can assert that a
    non-streamed client request is forwarded non-streamed (and vice versa).
    """

    def __init__(self) -> None:
        self.model = "stub-model"
        self.base_url = "http://stub"
        self.calls = 0
        self.stream_calls = 0
        self.once_calls = 0
        self.payloads: list[dict] = []
        # Embeddings / transcription capture.
        self.embedding_payloads: list[dict] = []
        self.transcribe_calls: list[tuple[dict, dict]] = []
        self.embedding_status = 200
        self.transcribe_status = 200
        self.transcribe_body = b'{"text":"hello world"}'
        self.transcribe_content_type = "application/json"
        self.transcribe_delay = 0.0
        self.stream_delay = 0.0
        self.reply: str | None = None  # overrides PIECES when set
        self.concurrent_transcriptions = 0
        self.peak_transcriptions = 0

    async def chat(self, payload, headers=None):
        self.calls += 1
        self.stream_calls += 1
        self.payloads.append(payload)
        for piece in ([self.reply] if self.reply is not None else PIECES):
            if self.stream_delay:
                await asyncio.sleep(self.stream_delay)
            yield Token(text=piece)
        yield Token(text="", finish_reason="stop")

    async def chat_once(self, payload, headers=None, timeout=None) -> dict:
        self.calls += 1
        self.once_calls += 1
        self.payloads.append(payload)
        return {
            "id": "stub-1",
            "object": "chat.completion",
            "model": self.model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": self.reply if self.reply is not None else TEXT,
                    },
                    "finish_reason": "stop",
                }
            ],
        }

    async def embeddings(self, payload, headers=None) -> tuple[int, dict]:
        self.calls += 1
        self.embedding_payloads.append(payload)
        inputs = payload["input"]
        texts = [inputs] if isinstance(inputs, str) else list(inputs)
        return self.embedding_status, {
            "object": "list",
            "model": payload["model"],
            "data": [
                {"object": "embedding", "index": i, "embedding": [0.1, 0.2, 0.3]}
                for i, _ in enumerate(texts)
            ],
            "usage": {"prompt_tokens": 7 * len(texts), "total_tokens": 7 * len(texts)},
        }

    async def transcribe(
        self, files, data, timeout=None, headers=None
    ) -> tuple[int, bytes, str]:
        self.calls += 1
        self.transcribe_calls.append((files, data))
        self.concurrent_transcriptions += 1
        self.peak_transcriptions = max(
            self.peak_transcriptions, self.concurrent_transcriptions
        )
        try:
            if self.transcribe_delay:
                await asyncio.sleep(self.transcribe_delay)
            return (
                self.transcribe_status,
                self.transcribe_body,
                self.transcribe_content_type,
            )
        finally:
            self.concurrent_transcriptions -= 1

    async def health(self) -> bool:
        return True

    async def list_models(self):
        return ["stub-model"]

    def stats(self) -> dict:
        return {"base_url": self.base_url, "scans": 0, "usage_reporting": True}

    def unreachable_hint(self) -> str:
        return start_command_hint(GEN_ABSENT)

    async def detect_cli(self) -> FoundryCli:
        return FoundryCli()

    async def aclose(self) -> None:
        pass
