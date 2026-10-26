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
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from kvstream.backend.foundry_cli import FoundryCli
from kvstream.backend.resolver import SOURCE_CLI, BackendResolver, Resolution

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


class FoundryClientError(FoundryError):
    """
    Foundry Local answered 4xx: the *request* was wrong, not the backend.

    Kept distinct from :class:`FoundryError` because the two must be handled in
    opposite ways. A backend failure counts towards the circuit breaker and is
    reported as 502; a client error must reach the caller with the backend's
    own status and body, and must never trip the breaker — otherwise a handful
    of requests naming the wrong model alias would take the gateway down for
    every other caller.
    """

    def __init__(self, status_code: int, body: Any, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _client_error(status_code: int, text: str) -> FoundryClientError:
    """Build a client error that preserves the backend's own body."""
    try:
        body: Any = json.loads(text)
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = {
            "error": {
                "message": text[:500] or f"HTTP {status_code}",
                "type": "invalid_request_error",
                "code": None,
                "param": None,
            }
        }
    return FoundryClientError(
        status_code, body, f"Foundry Local returned HTTP {status_code}: {text[:200]}"
    )


class _StreamOptionsRejected(RuntimeError):
    """Internal: the backend refused ``stream_options``; retry without it."""


class FoundryClient:
    def __init__(
        self,
        base_url: str = "http://localhost:5273",
        model: str | None = None,
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
        self._warned_withheld = False

    def credentials_allowed(self, url: str) -> bool:
        """
        Whether ``url`` may be sent the configured key or a caller's credential.

        Only a backend the operator named (an explicit URL, or discovery off)
        or one the ``foundry`` CLI reported counts. The localhost scan adopts
        whatever answers ``/v1/models`` — Ollama, LM Studio, any local process —
        and handing that the operator's key and every caller's Authorization
        header would leak them to a server nobody chose. Foundry Local itself
        requires no auth, so withholding them from a scanned backend that *is*
        Foundry Local costs nothing.
        """
        if self.pinned or not self.discover:
            return True
        found = self.last_resolution
        return found is not None and found.source == SOURCE_CLI and found.url == url

    def _headers(self, url: str, extra: dict[str, str] | None = None) -> dict[str, str]:
        """
        Headers for an upstream call to ``url``.

        A configured key is the default; a header forwarded from the inbound
        request wins over it, so KVStream can sit behind a gateway that does its
        own per-caller auth (LiteLLM, for one) without the sidecar's static key
        overriding the caller's. Neither is sent to a backend that was not
        chosen (see :meth:`credentials_allowed`).
        """
        if not self.credentials_allowed(url):
            if (self._api_key or extra) and not self._warned_withheld:
                self._warned_withheld = True
                logger.warning(
                    "not sending credentials to %s: it was found by scanning localhost, "
                    "not named in configuration or reported by the foundry CLI. Set "
                    "backend.base_url if this backend needs them.",
                    url,
                )
            return {}
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
        """
        Describe a failed backend call by what actually went wrong.

        Only a connection that could not be made means "not running", so only
        that gets the start-command hint. A server that accepted the request
        and then dropped it is running: Foundry Local (CLI 0.10.3) does exactly
        that for a streamed request naming a model that is not loaded, and
        telling the operator to `foundry server start` a running server sent
        them the wrong way.
        """
        if isinstance(cause, httpx.ConnectError | httpx.ConnectTimeout):
            return FoundryError(
                f"Foundry Local is unreachable at {url}: {cause}. {self.unreachable_hint()}"
            )
        if isinstance(cause, httpx.TimeoutException):
            return FoundryError(f"Foundry Local at {url} did not respond in time: {cause}")
        return FoundryError(
            f"Foundry Local at {url} accepted the request but failed to complete it: "
            f"{cause}. It is running; if this was a streamed request, check that the "
            "model is loaded (`foundry model load`)."
        )

    # ------------------------------------------------------------------
    # URL resolution
    # ------------------------------------------------------------------

    async def resolve_url(self) -> str:
        # An explicitly configured URL is authoritative: no probing, no scan,
        # no CLI. This is also the only path that can work in a container.
        if self.pinned or not self.discover:
            return self.configured_url

        # Fast path: the cached URL is trusted until a request to it fails to
        # connect (see `_rediscover`). Probing it before every request doubled
        # upstream traffic, and proved little: a stalled Foundry Local keeps
        # answering /v1/models.
        cached = self._resolved_url
        if cached:
            return cached

        async with self._lock:
            # Another coroutine may have re-resolved while we waited.
            if self._resolved_url:
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

    async def _rediscover(self, failed_url: str) -> str | None:
        """
        After a refused connection, find where the backend went, if anywhere.

        Foundry Local takes a new port on almost every restart. Returns a
        different URL to retry against, or ``None`` when there is none (pinned,
        discovery off, on cooldown, or the backend is simply down).
        """
        if self.pinned or not self.discover:
            return None
        if self._resolved_url == failed_url:
            self._resolved_url = None
        fresh = await self.resolve_url()
        # Retry only against a backend that resolution actually found. On
        # cooldown, or when nothing was found, `resolve_url` falls back to the
        # configured URL without adopting it; retrying there sent the request
        # to a guess (localhost:5273 by default) and the error then named that
        # URL instead of the one that refused the connection.
        if fresh == failed_url or fresh != self._resolved_url:
            return None
        logger.info("backend moved: %s refused the connection, now using %s", failed_url, fresh)
        return fresh

    async def _send(self, send: Callable[[str], Awaitable[httpx.Response]]) -> httpx.Response:
        """
        Send one request to the backend, retrying once if it has moved.

        Only a refused connection is retried: the request never reached a
        server, so resending it cannot duplicate any work.
        """
        url = await self.resolve_url()
        try:
            return await send(url)
        except httpx.ConnectError as exc:
            fresh = await self._rediscover(url)
            if fresh is None:
                raise self.unreachable(url, exc) from exc
            try:
                return await send(fresh)
            except httpx.HTTPError as retry_exc:
                raise self.unreachable(fresh, retry_exc) from retry_exc
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc

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
            pass

        # A 400 on a request carrying stream_options is only *probably* about
        # that field — it may equally be a wrong model alias. Retry without it,
        # and conclude the backend rejects the field only if the retry is
        # accepted. Otherwise one bad client request would switch usage
        # reporting off for the whole process.
        body.pop("stream_options", None)
        accepted = False
        async for tok in self._stream(url, body, headers):
            if not accepted:
                accepted = True
                if self._use_stream_options is not False:
                    logger.info(
                        "backend rejected stream_options.include_usage; continuing "
                        "without it. Real token counts will be unavailable and "
                        "admission costs stay estimated."
                    )
                self._use_stream_options = False
            yield tok

    async def _stream(
        self, url: str, body: dict, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[Token, None]:
        try:
            async for tok in self._stream_inner(url, body, headers):
                yield tok
            return
        except httpx.ConnectError as exc:
            # Refused before anything was sent or received, so a retry against
            # the backend's new address cannot duplicate a generation.
            fresh = await self._rediscover(url)
            if fresh is None:
                raise self.unreachable(url, exc) from exc
            url = fresh
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc
        try:
            async for tok in self._stream_inner(url, body, headers):
                yield tok
        except httpx.HTTPError as exc:
            raise self.unreachable(url, exc) from exc

    async def _stream_inner(
        self, url: str, body: dict, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[Token, None]:
        async with self._client.stream(
            "POST", f"{url}/v1/chat/completions", json=body, headers=self._headers(url, headers)
        ) as resp:
            if resp.status_code >= 400:
                detail = (await resp.aread()).decode(errors="replace")
                # A 400/422 on a request that carried stream_options is most
                # likely the backend refusing that field, not the prompt.
                if resp.status_code in (400, 422) and "stream_options" in body:
                    raise _StreamOptionsRejected(detail[:200])
                if 400 <= resp.status_code < 500:
                    raise _client_error(resp.status_code, detail)
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
        body = {**payload, "stream": False}
        body.pop("stream_options", None)
        resp = await self._send(
            lambda url: self._client.post(
                f"{url}/v1/chat/completions",
                json=body,
                headers=self._headers(url, headers),
                timeout=timeout if timeout is not None else self._client.timeout,
            )
        )
        if resp.status_code >= 400:
            detail = resp.text
            if resp.status_code < 500:
                raise _client_error(resp.status_code, detail)
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

    async def completions_stream(
        self, payload: dict, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[bytes, None]:
        """
        POST a streamed legacy completion and relay the backend's bytes verbatim.

        Raises before the first byte on an error status — 4xx as
        :class:`FoundryClientError`, 5xx as :class:`FoundryError` — so the
        gateway can still answer with a real status code.
        """
        url = await self.resolve_url()
        for attempt in (1, 2):
            try:
                async with self._client.stream(
                    "POST",
                    f"{url}/v1/completions",
                    json=payload,
                    headers=self._headers(url, headers),
                ) as resp:
                    if resp.status_code >= 400:
                        detail = (await resp.aread()).decode(errors="replace")
                        # 501: the backend does not offer the route — about the
                        # request, not the backend's health (as on the other
                        # pass-through routes).
                        if resp.status_code < 500 or resp.status_code == 501:
                            raise _client_error(resp.status_code, detail)
                        raise FoundryError(
                            f"Foundry Local returned HTTP {resp.status_code}: {detail[:200]}"
                        )
                    async for chunk in resp.aiter_bytes():
                        yield chunk
                return
            except httpx.ConnectError as exc:
                fresh = await self._rediscover(url) if attempt == 1 else None
                if fresh is None:
                    raise self.unreachable(url, exc) from exc
                url = fresh
            except httpx.HTTPError as exc:
                raise self.unreachable(url, exc) from exc

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
        resp = await self._send(
            lambda url: self._client.post(
                f"{url}{path}", json=payload, headers=self._headers(url, headers)
            )
        )
        try:
            return resp.status_code, resp.json()
        except ValueError:
            # A success status with a body that is not JSON is not a success
            # the client can use: report it as an upstream failure, never as
            # a 200 carrying an error object.
            status = resp.status_code if resp.status_code >= 400 else 502
            return status, {
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
        resp = await self._send(
            lambda url: self._client.post(
                f"{url}/v1/audio/transcriptions",
                files=files,
                data=data,
                timeout=timeout,
                headers=self._headers(url, headers),
            )
        )
        return (
            resp.status_code,
            resp.content,
            resp.headers.get("content-type", "application/json"),
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def list_models(self) -> list[str]:
        """Model ids from ``/v1/models``; ``[]`` when they cannot be read."""
        return await self.models_or_none() or []

    async def models_or_none(self) -> list[str] | None:
        """
        Model ids from ``/v1/models``, or ``None`` when they cannot be read.

        Distinguishes "the backend lists no models" from "the backend did not
        answer", which :meth:`list_models` cannot. On Foundry Local CLI 0.10.3
        this lists *downloaded* models, loaded or not.
        """
        try:
            r = await self._send(
                lambda url: self._client.get(
                    f"{url}/v1/models", timeout=5.0, headers=self._headers(url)
                )
            )
            r.raise_for_status()
            return [m["id"] for m in r.json().get("data", [])]
        except Exception:  # noqa: BLE001
            return None

    def use_model(self, model: str | None) -> None:
        """Adopt ``model`` (e.g. once auto-detected), or forget it with ``None``."""
        self.model = model
        self._resolver.model = model

    async def get_model(
        self, model_id: str, headers: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        """GET /v1/models/{id}, proxied verbatim."""
        # The id arrives already percent-decoded, so it is encoded again as one
        # path segment: a raw `?` would start a query string upstream and `..`
        # would be normalised away to a different endpoint. `:` stays literal —
        # it is the catalog version separator in Foundry Local ids.
        if model_id in (".", ".."):
            # A dot segment is never a model id, and percent-encoding does not
            # protect it (servers decode `%2E%2E` and normalise it as `..`).
            return 404, {
                "error": {
                    "message": f"model {model_id!r} not found",
                    "type": "invalid_request_error",
                    "code": "model_not_found",
                    "param": None,
                }
            }
        segment = quote(model_id, safe=":")
        resp = await self._send(
            lambda url: self._client.get(
                f"{url}/v1/models/{segment}", timeout=5.0, headers=self._headers(url, headers)
            )
        )
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return (resp.status_code if resp.status_code >= 400 else 502), {
                "error": {
                    "message": "Foundry Local returned a non-JSON body",
                    "type": "upstream_error",
                }
            }

    async def health(self) -> bool:
        """Liveness: does the backend answer /v1/models (re-discovering if it moved)."""
        try:
            r = await self._send(lambda url: self._client.get(f"{url}/v1/models", timeout=5.0))
            return r.status_code == 200
        except Exception:  # noqa: BLE001
            return False

    async def aclose(self) -> None:
        await self._client.aclose()
