"""
FoundryBackend — adapter for Microsoft Foundry Local.

Foundry Local exposes an OpenAI-compatible API at a configurable port
(default 5273, but the port changes between restarts because the OS
assigns an ephemeral port).  This backend auto-discovers the active port
by probing every TCP-LISTENING port on localhost for an OpenAI-compatible
/v1/models response, so it works regardless of process name or port number.

Start Foundry Local with: foundrylocal serve
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
import sys
from collections.abc import AsyncIterator

import httpx

from kvstream.backends.base import BaseBackend, GenerateRequest, Token

logger = logging.getLogger("kvstream.backends.foundry")

import time as _time  # aliased to avoid shadowing local variables

# Module-level URL cache — survives across requests within a single process.
# Cleared whenever the cached URL stops responding so the next request
# triggers a fresh scan.
_discovered_url: str | None = None
# Serialises concurrent discovery attempts so only one coroutine performs
# the expensive port scan at a time (prevents thundering herd).
_discovery_lock: asyncio.Lock | None = None
# Timestamp of the last failed discovery scan.  When Foundry is not running
# we skip the expensive port scan for _SCAN_COOLDOWN_S seconds so that a
# burst of concurrent requests (e.g. a benchmark) doesn't fire 24 separate
# netstat + port-probe sweeps.
_last_failed_scan: float = 0.0
_SCAN_COOLDOWN_S: float = 5.0


def _get_discovery_lock() -> asyncio.Lock:
    """Return the per-process asyncio.Lock for discovery, creating it lazily."""
    global _discovery_lock
    if _discovery_lock is None:
        _discovery_lock = asyncio.Lock()
    return _discovery_lock


# ---------------------------------------------------------------------------
# Port discovery helpers
# ---------------------------------------------------------------------------


def _all_listening_ports_sync() -> list[int]:
    """
    Return all TCP-LISTENING ports on localhost.  Blocking; run in a thread.

    Uses `netstat -ano` on Windows, `ss -tlnH` on Linux/macOS.
    Only returns ports > 1024 to skip obvious system services.
    """
    ports: list[int] = []

    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            for line in result.stdout.splitlines():
                m = re.search(
                    r"TCP\s+(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]|::1):(\d+)\s+\S+\s+LISTENING",
                    line,
                    re.IGNORECASE,
                )
                if m:
                    port = int(m.group(1))
                    if port > 1024:
                        ports.append(port)
        except Exception as exc:
            logger.debug("netstat failed: %s", exc)

    else:  # Linux / macOS
        try:
            result = subprocess.run(
                ["ss", "-tlnH"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for line in result.stdout.splitlines():
                m = re.search(r":(\d+)\s+", line)
                if m:
                    port = int(m.group(1))
                    if port > 1024:
                        ports.append(port)
        except Exception as exc:
            logger.debug("ss failed: %s", exc)

    return ports


async def _port_scan_discover(
    client: httpx.AsyncClient,
    exclude_ports: set[int] | None = None,
) -> str | None:
    """
    Concurrently probe every LISTENING port > 1024 for an OpenAI-compatible
    /v1/models endpoint.  Returns the URL of the port that has the most
    models loaded (non-empty ``data`` list preferred over empty).

    ``exclude_ports`` lets callers skip ports they know belong to themselves
    (e.g. the KVStream proxy port) so the scanner never mistakes the proxy for
    the upstream Foundry Local service.
    """
    loop = asyncio.get_event_loop()
    ports = await loop.run_in_executor(None, _all_listening_ports_sync)
    if exclude_ports:
        ports = [p for p in ports if p not in exclude_ports]
    logger.debug("Port scan: checking %d candidate ports (excluded: %s)", len(ports), exclude_ports)

    # Returns (url, model_count) so we can prefer ports with loaded models.
    async def probe(port: int) -> tuple[str, int] | None:
        url = f"http://localhost:{port}"
        try:
            r = await client.get(f"{url}/v1/models", timeout=1.5)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, dict) and "data" in body:
                    model_count = len(body.get("data") or [])
                    return (url, model_count)
        except Exception:
            pass
        return None

    results = await asyncio.gather(*[probe(p) for p in ports])
    candidates = [r for r in results if r is not None]

    if not candidates:
        return None

    # Prefer ports that have at least one model loaded; fall back to any
    # port that speaks the OpenAI models API.
    with_models = [(url, cnt) for url, cnt in candidates if cnt > 0]
    chosen_url, chosen_cnt = (with_models or candidates)[0]
    logger.info(
        "Auto-discovered Foundry Local at %s (%d model(s) loaded)",
        chosen_url,
        chosen_cnt,
    )
    return chosen_url


async def _resolve_url(
    client: httpx.AsyncClient,
    configured_url: str,
    exclude_ports: set[int] | None = None,
) -> str:
    """
    Return the URL where Foundry Local is currently responding.

    Resolution order:
      1. Cached URL from a previous successful discovery (same process).
      2. Configured URL (default http://localhost:5273).
      3. Full port scan of all LISTENING ports on localhost
         (excluding ``exclude_ports`` — e.g. the KVStream proxy's own port).
      4. Configured URL as a last-resort fall-through.
    """
    global _discovered_url

    # Fast path: check cached URL without acquiring the lock.
    cached = _discovered_url
    if cached:
        try:
            r = await client.get(f"{cached}/v1/models", timeout=2.0)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, dict) and len(body.get("data") or []) > 0:
                    return cached
        except Exception:
            pass
        _discovered_url = None  # stale — fall through to serialised discovery

    # Slow path: serialise discovery so only one coroutine does the expensive
    # port scan at a time (prevents thundering herd on cold start / reconnect).
    async with _get_discovery_lock():
        global _last_failed_scan
        # Re-check after acquiring the lock — a sibling coroutine may have
        # already completed discovery while we were waiting.
        if _discovered_url and _discovered_url != configured_url:
            return _discovered_url

        # Probe the configured URL first (cheapest check).
        try:
            r = await client.get(f"{configured_url}/v1/models", timeout=2.0)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, dict) and len(body.get("data") or []) > 0:
                    _discovered_url = configured_url
                    return configured_url
        except Exception:
            pass

        # Skip the expensive port scan if we already tried recently and
        # found nothing — avoids a netstat+probe storm during benchmarks.
        if _time.monotonic() - _last_failed_scan < _SCAN_COOLDOWN_S:
            return configured_url

        # Full port scan as last resort.
        found = await _port_scan_discover(client, exclude_ports=exclude_ports)
        if found:
            _discovered_url = found
            return found

        # Nothing found — record timestamp and return configured URL as fallback.
        _last_failed_scan = _time.monotonic()
        _discovered_url = None
        return configured_url


# ---------------------------------------------------------------------------
# Backend class
# ---------------------------------------------------------------------------


class FoundryBackend(BaseBackend):
    def __init__(
        self,
        base_url: str = "http://localhost:5273",
        model: str = "phi-3-mini",
        timeout: float = 120.0,
        exclude_ports: list[int] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))
        # Ports to skip during auto-discovery (e.g. the KVStream proxy's own port)
        # so the scanner never confuses the proxy's /v1/models for Foundry.
        self._exclude_ports: set[int] = set(exclude_ports or [])

    async def _url(self) -> str:
        """Return the active Foundry Local base URL (with auto-discovery)."""
        url = await _resolve_url(self._client, self.base_url, self._exclude_ports)
        # Keep self.base_url in sync so /health reports the real URL.
        self.base_url = url
        return url

    async def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        url = await self._url()
        payload: dict = {
            "model": self.model,
            "messages": [{"role": "user", "content": request.prompt}],
            "max_tokens": request.max_new_tokens,
            "temperature": request.temperature,
            "top_p": request.top_p,
            "stream": True,
        }
        # Foundry Local (and many OpenAI-compatible runtimes) return HTTP 400
        # when `stop` is present but empty.  Only include it when non-empty.
        if request.stop:
            payload["stop"] = request.stop
        async with self._client.stream(
            "POST",
            f"{url}/v1/chat/completions",
            json=payload,
        ) as resp:
            if resp.status_code >= 400:
                body = (await resp.aread()).decode(errors="replace")
                raise RuntimeError(
                    f"Foundry Local returned HTTP {resp.status_code}: {body[:200]}"
                )
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
                for choice in data.get("choices", []):
                    delta = choice.get("delta", {})
                    text = delta.get("content", "")
                    finish = choice.get("finish_reason")
                    if text or finish:
                        yield Token(text=text, token_id=0, finish_reason=finish)

    async def health(self) -> bool:
        try:
            url = await self._url()
            resp = await self._client.get(f"{url}/v1/models", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    async def list_models(self) -> list[str]:
        try:
            url = await self._url()
            resp = await self._client.get(f"{url}/v1/models")
            data = resp.json()
            return [m["id"] for m in data.get("data", [])]
        except Exception:
            return []

    async def aclose(self) -> None:
        await self._client.aclose()
