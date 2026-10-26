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

from kvstream.backend.inventory import matches

logger = logging.getLogger("kvstream.discovery")

# Response header KVStream stamps on every response, so gateways can recognise
# each other and never proxy into one another.
KVSTREAM_HEADER = "x-kvstream-version"

KIND_KVSTREAM = "kvstream"
KIND_OLLAMA = "ollama"
KIND_OPENAI = "openai-compatible"


# A listening TCP socket in `netstat -ano`, recognised by *structure*: a local
# address reachable as localhost, and a wildcard foreign address. The state
# column is deliberately not read — Windows localizes it ("LISTENING" is not
# what a German or Japanese install prints), and only a listener has the
# wildcard foreign address, in every language.
_NETSTAT_LISTENER = re.compile(
    r"^\s*TCP\s+(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]|\[::1\]):(\d+)\s+(?:0\.0\.0\.0:0|\[::\]:0)\s",
    re.IGNORECASE,
)
_SS_LOCAL_PORT = re.compile(r":(\d+)\s")


def parse_netstat(output: str) -> set[int]:
    """Listening localhost ports (> 1024) from ``netstat -ano`` output, any locale."""
    ports: set[int] = set()
    for line in output.splitlines():
        match = _NETSTAT_LISTENER.match(line)
        if match and int(match.group(1)) > 1024:
            ports.add(int(match.group(1)))
    return ports


_BSD_LOCAL_HOSTS = {"*", "127.0.0.1", "::1"}


def parse_bsd_netstat(output: str) -> set[int]:
    """
    Listening localhost ports (> 1024) from macOS ``netstat -an -p tcp`` output.

    The BSD format joins address and port with a dot (``127.0.0.1.5273``,
    ``*.80``, ``::1.8080``) and names the protocol ``tcp4``, ``tcp6`` or
    ``tcp46``. As with Windows, a listener is recognised by structure — its
    foreign address is the wildcard ``*.*`` — not by the state word.
    """
    ports: set[int] = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 5 or not fields[0].lower().startswith("tcp"):
            continue
        local, foreign = fields[3], fields[4]
        if foreign != "*.*":
            continue
        host, _, port = local.rpartition(".")
        if host in _BSD_LOCAL_HOSTS and port.isdigit() and int(port) > 1024:
            ports.add(int(port))
    return ports


def parse_ss(output: str) -> set[int]:
    """Listening ports (> 1024) from ``ss -tlnH`` output (listeners only, by flag)."""
    ports: set[int] = set()
    for line in output.splitlines():
        match = _SS_LOCAL_PORT.search(line)
        if match and int(match.group(1)) > 1024:
            ports.add(int(match.group(1)))
    return ports


def list_listening_ports() -> list[int]:
    """Return TCP listening ports (> 1024) on localhost. Blocking; run in a thread."""
    try:
        if sys.platform == "win32":
            # No `-p TCP`: that lists IPv4 only, so a backend listening on
            # [::1] or [::] alone was never found. Decoded as ASCII with
            # replacement: the parts parsed are ASCII, and netstat writes the
            # OEM code page (437 here) while Python's default decoding is the
            # ANSI one (1252) — a localized state word could otherwise make the
            # decoder raise and lose the whole output.
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True,
                text=True,
                encoding="ascii",
                errors="replace",
                timeout=10,
            )
            return sorted(parse_netstat(result.stdout or ""))
        if sys.platform == "darwin":
            # macOS has no `ss`, so this branch used to run `ss`, fail, and
            # find nothing: the scan never worked on a Mac. `netstat` ships
            # with macOS and, unlike `lsof`, lists every user's sockets.
            result = subprocess.run(
                ["netstat", "-an", "-p", "tcp"],
                capture_output=True,
                text=True,
                encoding="ascii",
                errors="replace",
                timeout=10,
            )
            return sorted(parse_bsd_netstat(result.stdout or ""))
        result = subprocess.run(
            ["ss", "-tlnH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
        )
        return sorted(parse_ss(result.stdout or ""))
    except Exception as exc:  # noqa: BLE001 — discovery must never crash the caller
        logger.debug("port enumeration failed: %s", exc)
        return []


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
    ids = [e["id"] for e in entries if isinstance(e, dict) and isinstance(e.get("id"), str)]
    kind = await _identify(client, url)
    return Candidate(
        url=url,
        model_count=len(entries),
        kind=kind,
        # Suffix-insensitive: Foundry 0.10.3 lists `…-gpu` and answers to
        # `…-gpu:2`, so an exact comparison flagged the right server as wrong.
        has_configured_model=bool(prefer_model) and matches(prefer_model or "", ids),
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
