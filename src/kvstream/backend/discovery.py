"""
Ephemeral-port discovery for Foundry Local.

Foundry Local exposes an OpenAI-compatible service on an OS-assigned port that
changes between restarts. This module resolves the live URL by probing the
configured URL and then scanning every LISTENING localhost port for an
OpenAI-compatible ``/v1/models`` response.

Only localhost is ever scanned; no outbound connections are made.

Choosing between candidates
---------------------------
A localhost port answering ``/v1/models`` is not necessarily Foundry Local —
Ollama, LM Studio, vLLM and another KVStream all speak the same API. There is no
published fingerprint that positively identifies Foundry Local at the HTTP
layer, so this module does not pretend to have one. It ranks instead:

* another KVStream is **excluded outright** (it identifies itself with a header
  we control, so this one is certain, and adopting one as a backend would build
  a proxy loop);
* a port serving the *configured model* wins, since that is the strongest
  available evidence that it is the intended backend;
* a server identifiable as something other than Foundry Local is demoted;
* otherwise, any port with a model loaded beats one without.

The real fix for this ambiguity is the CLI-assisted lookup in
:mod:`kvstream.backend.resolver`, which asks Foundry Local itself.
"""

from __future__ import annotations

import asyncio
import logging
import re
import subprocess
import sys
from dataclasses import dataclass

import httpx

logger = logging.getLogger("kvstream.discovery")

# Response header KVStream stamps on every response, so gateways can recognise
# each other and never proxy into one another.
KVSTREAM_HEADER = "x-kvstream-version"

KIND_KVSTREAM = "kvstream"
KIND_OLLAMA = "ollama"
KIND_OPENAI = "openai-compatible"


def list_listening_ports() -> list[int]:
    """Return TCP LISTENING ports (> 1024) on localhost. Blocking; run in a thread."""
    ports: set[int] = set()
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["netstat", "-ano", "-p", "TCP"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            pattern = re.compile(
                r"TCP\s+(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]|::1):(\d+)\s+\S+\s+LISTENING",
                re.IGNORECASE,
            )
        else:
            result = subprocess.run(
                ["ss", "-tlnH"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            pattern = re.compile(r":(\d+)\s")
        for line in result.stdout.splitlines():
            m = pattern.search(line)
            if m:
                port = int(m.group(1))
                if port > 1024:
                    ports.add(port)
    except Exception as exc:  # noqa: BLE001 — discovery must never crash the caller
        logger.debug("port enumeration failed: %s", exc)
    return sorted(ports)


@dataclass
class Candidate:
    url: str
    model_count: int
    kind: str
    has_configured_model: bool = False

    def rank(self) -> tuple:
        """Sort key; lower is better."""
        return (
            0 if self.has_configured_model else 1,
            0 if self.kind == KIND_OPENAI else 1,
            0 if self.model_count > 0 else 1,
        )


async def _probe(
    client: httpx.AsyncClient, port: int, prefer_model: str | None = None
) -> Candidate | None:
    """Return a :class:`Candidate` if the port speaks the OpenAI /v1/models API."""
    url = f"http://localhost:{port}"
    try:
        r = await client.get(f"{url}/v1/models", timeout=1.5)
    except Exception:  # noqa: BLE001
        return None
    if r.status_code != 200:
        return None

    if r.headers.get(KVSTREAM_HEADER):
        # Certain: this is another KVStream gateway, not a runtime.
        return Candidate(url, 0, KIND_KVSTREAM)

    try:
        body = r.json()
    except ValueError:
        return None
    if not isinstance(body, dict) or "data" not in body:
        return None

    entries = body.get("data") or []
    ids = [e.get("id") for e in entries if isinstance(e, dict)]
    kind = await _identify(client, url)
    return Candidate(
        url=url,
        model_count=len(entries),
        kind=kind,
        has_configured_model=bool(prefer_model) and prefer_model in ids,
    )


async def _identify(client: httpx.AsyncClient, url: str) -> str:
    """
    Best-effort identification of a server we know is *not* Foundry Local.

    Only negative signals are used, and only ones that are documented parts of
    another product's API. Absence of a signal proves nothing, so the default is
    the neutral ``openai-compatible``.
    """
    try:
        r = await client.get(f"{url}/api/tags", timeout=1.0)
        if r.status_code == 200 and isinstance(r.json(), dict) and "models" in r.json():
            return KIND_OLLAMA
    except Exception:  # noqa: BLE001
        pass
    return KIND_OPENAI


async def discover(
    client: httpx.AsyncClient,
    configured_url: str,
    exclude_ports: set[int] | None = None,
    prefer_model: str | None = None,
) -> str | None:
    """Resolve the live Foundry Local base URL, or ``None`` if nothing qualifies."""
    exclude = exclude_ports or set()

    # 1. Configured URL first (cheapest).
    if await probe_url(client, configured_url) is not None:
        return configured_url

    # 2. Full localhost scan.
    loop = asyncio.get_event_loop()
    ports = await loop.run_in_executor(None, list_listening_ports)
    ports = [p for p in ports if p not in exclude]
    results = await asyncio.gather(*[_probe(client, p, prefer_model) for p in ports])

    candidates = [c for c in results if c is not None and c.kind != KIND_KVSTREAM]
    skipped = [c for c in results if c is not None and c.kind == KIND_KVSTREAM]
    for other in skipped:
        logger.debug("skipping %s — another KVStream gateway, not a backend", other.url)
    if not candidates:
        return None

    best = sorted(candidates, key=Candidate.rank)[0]
    logger.info(
        "Discovered Foundry Local at %s (%d model(s) loaded, %s%s)",
        best.url,
        best.model_count,
        best.kind,
        ", serving the configured model" if best.has_configured_model else "",
    )
    if not best.has_configured_model and prefer_model:
        logger.warning(
            "%s does not list the configured model %r — set an explicit backend URL "
            "if this is the wrong server.",
            best.url,
            prefer_model,
        )
    return best.url


async def probe_url(client: httpx.AsyncClient, url: str) -> int | None:
    """Return the model count if ``url`` answers /v1/models, else ``None``."""
    try:
        r = await client.get(f"{url.rstrip('/')}/v1/models", timeout=2.0)
        if r.status_code != 200:
            return None
        body = r.json()
        if isinstance(body, dict):
            return len(body.get("data") or [])
    except Exception:  # noqa: BLE001
        return None
    return None


# Backwards-compatible alias: this was private before the resolver needed it.
_probe_url = probe_url
