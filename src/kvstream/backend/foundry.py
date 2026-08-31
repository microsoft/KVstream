"""
FoundryClient — async HTTP client for a single Microsoft Foundry Local instance.

Foundry Local exposes an OpenAI-compatible API. This client forwards requests
unchanged and either streams the response back or reads it in one shot,
mirroring what the caller asked for. It resolves Foundry Local's ephemeral port
via :mod:`kvstream.backend.discovery`, caching the result and re-resolving when
the cached URL stops responding — subject to a cooldown, so a backend that has
gone away cannot turn every inbound request into a port sweep.

Token accounting note
---------------------
Most OpenAI-compatible servers report ``usage`` on a streamed response *only*
when the request carries ``stream_options.include_usage``. KVStream's online
token calibration depends on those counts, so the field is sent by default and
disabled for the process only if the backend rejects it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any

import httpx

from kvstream.backend import discovery
from kvstream.backend.foundry_cli import FoundryCli
from kvstream.backend.resolver import BackendResolver, Resolution

logger = logging.getLogger("kvstream.backend.foundry")


@dataclass
class Token:
    """
    One streamed chunk from the backend.

    ``raw`` is the chunk exactly as the backend sent it, and is what gets
    forwarded to the client. The other fields are a *view* over it for the
    gateway's own accounting: ``text`` and ``finish_reason`` drive live
    reservation sizing, and ``usage`` — present only on the trailing chunk, and
    only when the backend reports real counts — drives token calibration.

    Keeping ``raw`` is what preserves `tool_calls`, `logprobs`, the initial role
    delta and `system_fingerprint`. Rebuilding a chunk from the view would drop
    every one of them.
    """

    text: str
    finish_reason: str | None = None
    usage: dict | None = None
    raw: dict | None = None

    @property
    def usage_only(self) -> bool:
        """A trailing chunk that carries counts and no content."""
        return self.usage is not None and not self.text and self.finish_reason is None


def _as_token(data: dict) -> Token:
    """Build the accounting view over one raw SSE chunk, keeping the chunk."""
    text_parts: list[str] = []
    finish: str | None = None
    for choice in data.get("choices") or []:
        if not isinstance(choice, dict):
            continue
        delta = choice.get("delta") or {}
        if isinstance(delta, dict):
            content = delta.get("content")
            if isinstance(content, str):
                text_parts.append(content)
            # Tool-call arguments stream as text too and cost real tokens.
            for call in delta.get("tool_calls") or []:
                if isinstance(call, dict):
                    args = (call.get("function") or {}).get("arguments")
                    if isinstance(args, str):
                        text_parts.append(args)
        finish = choice.get("finish_reason") or finish
    reported = data.get("usage")
    return Token(
        text="".join(text_parts),
        finish_reason=finish,
        usage=reported if isinstance(reported, dict) else None,
        raw=data,
    )


class FoundryError(RuntimeError):
    """Raised when Foundry Local returns an error or is unreachable."""


class _StreamOptionsRejected(RuntimeError):
    """Internal: the backend refused ``stream_options``; retry without it."""


class FoundryClient:
    def __init__(
        self,
        base_url: str = "http://localhost:5273",
        model: str = "phi-3-mini",
        timeout: float = 120.0,
        discover: bool = True,
        exclude_ports: list[int] | None = None,
        discovery_cooldown: float = 5.0,
        request_usage: bool = True,
        api_key: str | None = None,
        pinned: bool = False,
        use_foundry_cli: bool = True,
        foundry_cli_path: str | None = None,
        foundry_cli_timeout: float = 5.0,
    ) -> None:
        self.configured_url = base_url.rstrip("/")
        self.model = model
        self.discover = discover
        self._exclude = set(exclude_ports or [])
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))
        self._resolved_url: str | None = None
        self._lock = asyncio.Lock()

        self._resolver = BackendResolver(
            configured_url=self.configured_url,
            model=model,
            pinned=pinned,
            use_cli=use_foundry_cli,
            cli_path=foundry_cli_path,
            cli_timeout=foundry_cli_timeout,
            exclude_ports=self._exclude,
        )
        self.last_resolution: Resolution | None = None

        self._cooldown = max(0.0, discovery_cooldown)
        self._last_scan_at: float | None = None
        self.scans = 0  # full localhost sweeps performed
        self.resolutions = 0  # times a new backend URL was adopted

        # None = untested, True = supported, False = rejected once, stop sending.
        self._use_stream_options: bool | None = True if request_usage else False
        self._api_key = api_key

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """
        Headers for an upstream call.

        A configured key is the default; a header forwarded from the inbound
        request wins over it, so KVStream can sit behind a gateway that does its
        own per-caller auth (LiteLLM, for one) without the sidecar's static key
        overriding the caller's.
        """
        headers: dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        if extra:
            headers.update({k: v for k, v in extra.items() if v})
        return headers

    @property
    def base_url(self) -> str:
        """The currently resolved URL (or the configured one before resolution)."""
        return self._resolved_url or self.configured_url

    @property
    def usage_reporting(self) -> bool | None:
        """Whether ``stream_options.include_usage`` is being sent (None = untried)."""
        return self._use_stream_options

    @property
    def cli(self) -> FoundryCli:
        """What was detected about the installed Foundry Local CLI."""
        return self._resolver.cli

    @property
    def pinned(self) -> bool:
        return self._resolver.pinned

    async def detect_cli(self) -> FoundryCli:
        """Detect the CLI generation up front, so it can key calibration."""
        return await self._resolver.detect_cli()

    def stats(self) -> dict:
        return {
            "base_url": self.base_url,
            "configured_url": self.configured_url,
            "discover": self.discover,
            "pinned": self.pinned,
            "scans": self.scans,
            "resolutions": self.resolutions,
            "usage_reporting": self._use_stream_options,
            "resolution": self.last_resolution.as_dict() if self.last_resolution else None,
            "foundry_cli": self.cli.as_dict(),
        }

    def unreachable_hint(self) -> str:
        """An actionable message for the CLI generation actually detected."""
        return self._resolver.failure_hint()

    def unreachable(self, url: str, cause: object) -> FoundryError:
        """Build an unreachable-backend error that names the right start command."""
        return FoundryError(
            f"Foundry Local is unreachable at {url}: {cause}. {self.unreachable_hint()}"
        )

    # ------------------------------------------------------------------
    # URL resolution
    # ------------------------------------------------------------------

    async def resolve_url(self) -> str:
        # An explicitly configured URL is authoritative: no probing, no scan,
        # no CLI. This is also the only path that can work in a container.
        if self.pinned or not self.discover:
            return self.configured_url

        # Fast path: reuse a cached URL that still responds.
        cached = self._resolved_url
        if cached and await discovery.probe_url(self._client, cached) is not None:
            return cached

        async with self._lock:
            # Another coroutine may have re-resolved while we waited.
            if (
                self._resolved_url
                and await discovery.probe_url(self._client, self._resolved_url) is not None
            ):
                return self._resolved_url

            # Cooldown: resolution spawns subprocesses and probes every
            # listening port. When the backend is simply down, that must not
            # happen once per inbound request.
            now = asyncio.get_event_loop().time()
            if self._last_scan_at is not None and (now - self._last_scan_at) < self._cooldown:
                logger.debug(
                    "resolution on cooldown (%.1fs remaining); using last known URL",
                    self._cooldown - (now - self._last_scan_at),
                )
                return self._resolved_url or self.configured_url

            self._last_scan_at = now
            resolution = await self._resolver.resolve(self._client)
            self.last_resolution = resolution
            if "scan" in resolution.attempts:
                self.scans += 1
            if resolution.url:
                if resolution.url != self._resolved_url:
                    self.resolutions += 1
                self._resolved_url = resolution.url
                return resolution.url
            self._resolved_url = None
            return self.configured_url

    # ------------------------------------------------------------------
    # Chat — streaming
    # ------------------------------------------------------------------

    async def chat(
        self, payload: dict, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[Token, None]:
        """
        Stream an OpenAI chat-completions request.

        The payload is forwarded with ``stream: true`` and, unless the backend
        has refused it, ``stream_options.include_usage`` — merged into whatever
        the client already sent rather than replacing it.
        """
        url = await self.resolve_url()
        body = {**payload, "stream": True}
        if self._use_stream_options is not False:
            client_options = body.get("stream_options")
            merged = dict(client_options) if isinstance(client_options, dict) else {}
            merged["include_usage"] = True
            body["stream_options"] = merged

        try:
            async for tok in self._stream(url, body, headers):
                yield tok
            return
        except _StreamOptionsRejected:
            logger.info(
                "backend rejected stream_options.include_usage; retrying without it. "
                "Real token counts will be unavailable and admission costs stay estimated."
            )
            self._use_stream_options = False

        body.pop("stream_options", None)
        async for tok in self._stream(url, body, headers):
            yield tok

    async def _stream(
        self, url: str, body: dict, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[Token, None]:
        try:
            async for tok in self._stream_inner(url, body, headers):
                yield tok
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc

    async def _stream_inner(
        self, url: str, body: dict, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[Token, None]:
        async with self._client.stream(
            "POST", f"{url}/v1/chat/completions", json=body, headers=self._headers(headers)
        ) as resp:
            if resp.status_code >= 400:
                detail = (await resp.aread()).decode(errors="replace")
                # A 400/422 on a request that carried stream_options is most
                # likely the backend refusing that field, not the prompt.
                if resp.status_code in (400, 422) and "stream_options" in body:
                    raise _StreamOptionsRejected(detail[:200])
                raise FoundryError(
                    f"Foundry Local returned HTTP {resp.status_code}: {detail[:200]}"
                )
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                payload = line[6:].strip()
                if payload == "[DONE]":
                    break
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if not isinstance(data, dict):
                    continue
                # Every chunk is forwarded, in order, exactly as received —
                # including role-only and tool-call deltas, which a
                # content-shaped view would silently discard.
                yield _as_token(data)

    # ------------------------------------------------------------------
    # Chat — non-streaming
    # ------------------------------------------------------------------

    async def chat_once(
        self,
        payload: dict,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> dict:
        """
        Send a non-streamed chat completion and return the raw response JSON.

        Used when the *client* did not ask for a stream. Forwarding non-stream
        as non-stream is what makes the backend's ``usage`` block available
        directly, rather than depending on a trailing SSE chunk.
        """
        url = await self.resolve_url()
        body = {**payload, "stream": False}
        body.pop("stream_options", None)
        try:
            resp = await self._client.post(
                f"{url}/v1/chat/completions",
                json=body,
                headers=self._headers(headers),
                timeout=timeout if timeout is not None else self._client.timeout,
            )
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc
        if resp.status_code >= 400:
            detail = resp.text
            raise FoundryError(f"Foundry Local returned HTTP {resp.status_code}: {detail[:200]}")
        try:
            data = resp.json()
        except ValueError as exc:
            raise FoundryError("Foundry Local returned a non-JSON response") from exc
        if not isinstance(data, dict):
            raise FoundryError("Foundry Local returned an unexpected response shape")
        return data

    async def completions(
        self, payload: dict, headers: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        """POST /v1/completions (legacy), returning (status_code, parsed_body)."""
        return await self._post_json("/v1/completions", payload, headers)

    # ------------------------------------------------------------------
    # Embeddings and audio — true passthrough
    # ------------------------------------------------------------------
    #
    # Unlike the chat path, which rebuilds the response, these forward the
    # backend's status, body and content type unchanged. There is nothing the
    # gateway needs to interpret, so there is nothing it should distort.

    async def embeddings(
        self, payload: dict, headers: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        """POST /v1/embeddings, returning (status_code, parsed_body)."""
        return await self._post_json("/v1/embeddings", payload, headers)

    async def _post_json(
        self, path: str, payload: dict, headers: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        url = await self.resolve_url()
        try:
            resp = await self._client.post(
                f"{url}{path}", json=payload, headers=self._headers(headers)
            )
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return resp.status_code, {
                "error": {
                    "message": f"Foundry Local returned a non-JSON body: {resp.text[:200]}",
                    "type": "upstream_error",
                }
            }

    async def transcribe(
        self,
        files: dict[str, tuple[str | None, bytes, str]],
        data: dict[str, str],
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes, str]:
        """
        POST /v1/audio/transcriptions as multipart, returning the raw response.

        Body and content type pass through untouched: transcription answers in
        JSON, plain text, SRT or VTT depending on ``response_format``, and the
        gateway has no business re-encoding any of them.
        """
        url = await self.resolve_url()
        try:
            resp = await self._client.post(
                f"{url}/v1/audio/transcriptions",
                files=files,
                data=data,
                timeout=timeout,
                headers=self._headers(headers),
            )
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc
        return (
            resp.status_code,
            resp.content,
            resp.headers.get("content-type", "application/json"),
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def list_models(self) -> list[str]:
        try:
            url = await self.resolve_url()
            r = await self._client.get(f"{url}/v1/models", timeout=5.0, headers=self._headers())
            r.raise_for_status()
            return [m["id"] for m in r.json().get("data", [])]
        except Exception:  # noqa: BLE001
            return []

    async def get_model(
        self, model_id: str, headers: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        """GET /v1/models/{id}, proxied verbatim."""
        url = await self.resolve_url()
        try:
            resp = await self._client.get(
                f"{url}/v1/models/{model_id}", timeout=5.0, headers=self._headers(headers)
            )
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return resp.status_code, {
                "error": {
                    "message": "Foundry Local returned a non-JSON body",
                    "type": "upstream_error",
                }
            }

    async def health(self) -> bool:
        try:
            url = await self.resolve_url()
            r = await self._client.get(f"{url}/v1/models", timeout=5.0)
            return r.status_code == 200
        except Exception:  # noqa: BLE001
            return False

    async def aclose(self) -> None:
        await self._client.aclose()
