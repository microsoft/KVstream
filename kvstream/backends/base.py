"""
BaseBackend — abstract interface all runtime adapters must implement.

KVStream is backend-agnostic. Each adapter translates KVStream's internal
representation into the wire protocol of the target runtime.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field


@dataclass
class GenerateRequest:
    seq_id: str
    prompt: str
    prompt_tokens: list[int]
    max_new_tokens: int
    temperature: float = 0.8
    top_p: float = 0.95
    stop: list[str] = field(default_factory=list)
    stream: bool = True
    # When set, the backend should skip computing the first N tokens
    # (prefix already cached). Only supported by hard-inject backends.
    cached_prefix_tokens: int = 0


@dataclass
class Token:
    text: str
    token_id: int
    log_prob: float | None = None
    finish_reason: str | None = None  # "stop" | "length" | None


class BaseBackend(ABC):
    """
    Implement this to add a new inference backend.

    Minimum viable implementation: `generate()` and `health()`.
    For true KV injection (hard mode), also implement `save_kv_state()`
    and `restore_kv_state()`.
    """

    @abstractmethod
    def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        """Stream tokens for a request."""
        ...

    @abstractmethod
    async def health(self) -> bool:
        """Return True if the backend is reachable and ready."""
        ...

    async def tokenize(self, text: str) -> list[int]:
        """
        Optional: return token IDs for text.
        Used for prefix cache matching. Falls back to char-level if not implemented.
        """
        return list(text.encode("utf-8"))

    async def save_kv_state(self, seq_id: str, slot_id: int, path: str) -> bool:
        """
        Optional: save the backend's internal KV state for a sequence.
        Supported by llama.cpp via /slots API.
        Returns True on success.
        """
        return False

    async def restore_kv_state(self, seq_id: str, slot_id: int, path: str) -> bool:
        """
        Optional: restore previously saved KV state.
        Returns True on success.
        """
        return False

    async def list_models(self) -> list[str]:
        """Return available model names."""
        return []

    def supports_hard_kv_inject(self) -> bool:
        """Return True if this backend supports save/restore KV state."""
        return False

    async def aclose(self) -> None:
        """Release resources held by this backend (e.g. HTTP connection pools).
        Call this when the backend is no longer needed."""
        pass
