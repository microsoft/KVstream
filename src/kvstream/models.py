"""
OpenAI-compatible request schemas used by the gateway.

**Pass-through with a typed overlay.** KVStream is a proxy, so it validates only
the fields it actually needs — the model, the messages (to estimate cost), and
the knobs that decide streaming and admission — and forwards the client's
original JSON object untouched. An allow-list would silently drop everything it
did not know about: `tools`, `response_format`, `seed`, `logprobs`, and the
`tool_calls` an agent loop depends on. Dropping a field a client explicitly set
is worse than failing, because it fails *quietly* and produces a plausible wrong
answer.

The overlay exists so KVStream can cost a request and stream it; it is not a
statement about which OpenAI features Foundry Local supports. Anything the
backend rejects comes back to the client as the backend's own error.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Per-message allowance for role/formatting tokens in chat templates.
# Mirrors tokenization.MESSAGE_OVERHEAD_TOKENS; kept here to avoid a cycle.


class ChatMessage(BaseModel):
    """
    One chat message, permissive by design.

    ``content`` is a string for ordinary messages, a list of content parts for
    multimodal ones, and **null** for an assistant message that carries only
    ``tool_calls`` — which is exactly the shape every tool-calling agent
    produces on its second turn.
    """

    model_config = ConfigDict(extra="allow")

    role: str
    content: str | list[dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None

    def billable_text(self) -> str:
        """
        The text this message contributes to the prompt, for cost estimation.

        Tool calls and tool results are real prompt tokens on the next turn — an
        agent loop re-sends the whole transcript — so their JSON is counted too.
        Non-text content parts (images, audio) have no token cost measurable at
        the HTTP layer and are not guessed at; see the note in the README.
        """
        pieces: list[str] = []
        if isinstance(self.content, str):
            pieces.append(self.content)
        elif isinstance(self.content, list):
            for part in self.content:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    pieces.append(part["text"])
        if self.name:
            pieces.append(self.name)
        if self.tool_calls:
            pieces.append(json.dumps(self.tool_calls, separators=(",", ":")))
        return "\n".join(p for p in pieces if p)


class ChatCompletionRequest(BaseModel):
    """The typed overlay. Everything else rides along via ``extra="allow"``."""

    model_config = ConfigDict(extra="allow")

    model: str
    messages: list[ChatMessage]
    # Absent means "the model's default" — KVStream does not impose one on the
    # backend. `admission.default_max_tokens` is used to *cost* such a request,
    # and is never sent upstream.
    max_tokens: int | None = Field(default=None, gt=0)
    max_completion_tokens: int | None = Field(default=None, gt=0)
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    n: int = Field(default=1, ge=1)
    stream: bool = False
    stream_options: dict[str, Any] | None = None
    stop: str | list[str] | None = None

    @property
    def deterministic(self) -> bool:
        """
        True when the response is reproducible enough to cache / coalesce.

        Requires an explicit ``temperature: 0``. An *absent* temperature means
        the backend's default (1.0 for OpenAI-compatible servers), which is not
        deterministic — treating "unset" as zero would cache sampled output.
        """
        return self.temperature == 0.0 and self.n == 1

    @property
    def wants_usage(self) -> bool:
        """Whether the *client* asked for a usage chunk on a streamed response."""
        return bool((self.stream_options or {}).get("include_usage"))

    def generation_budget(self, default_max_tokens: int) -> int:
        """
        Tokens this request may generate, across all choices.

        ``n`` multiplies it: asking for 4 completions of 512 tokens really can
        occupy four times the KV footprint of one.
        """
        limit = self.max_tokens or self.max_completion_tokens or default_max_tokens
        return max(1, limit) * self.n


def backend_payload(raw: dict, *, stream: bool) -> dict:
    """
    The body to forward upstream: the client's own object, minimally adjusted.

    Only three things change. ``stream`` is set to whatever path the gateway is
    actually taking; ``stream_options`` is dropped on the non-streamed path
    (where it is meaningless); and an empty ``stop`` is removed, because Foundry
    Local answers HTTP 400 for one.
    """
    payload = dict(raw)
    payload["stream"] = stream
    if not stream:
        payload.pop("stream_options", None)
    if "stop" in payload and not payload["stop"]:
        payload.pop("stop")
    return payload


class EmbeddingsRequest(BaseModel):
    """
    OpenAI-compatible embeddings request.

    Embeddings have an honest token cost — the input itself — so they are
    admitted against the same KV-token budget as chat, with no generation half.
    """

    model_config = ConfigDict(extra="allow")

    model: str
    input: str | list[str]

    @property
    def texts(self) -> list[str]:
        return [self.input] if isinstance(self.input, str) else list(self.input)


class CompletionRequest(BaseModel):
    """
    Legacy ``/v1/completions``.

    Proxied for the same reason as embeddings: a route the gateway does not
    admit is a route clients can use to walk around admission control.
    """

    model_config = ConfigDict(extra="allow")

    model: str
    prompt: str | list[str] | None = None
    max_tokens: int | None = Field(default=None, gt=0)
    n: int = Field(default=1, ge=1)
    stream: bool = False

    @property
    def texts(self) -> list[str]:
        if self.prompt is None:
            return []
        return [self.prompt] if isinstance(self.prompt, str) else list(self.prompt)

    def generation_budget(self, default_max_tokens: int) -> int:
        return max(1, self.max_tokens or default_max_tokens) * self.n
