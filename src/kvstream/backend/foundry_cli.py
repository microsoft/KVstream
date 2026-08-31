"""
Adapter for the Foundry Local command-line tool.

Foundry Local is mid-transition between two CLI generations, and KVStream has to
work in front of both (proposal §8):

* the **service-based CLI** (verified against ``foundry 0.8.119``), which exposes
  ``foundry service start|stop|restart|status|ps|set|init|diag`` and runs a
  background HTTP service on a port the OS assigns at each start; and
* **Foundry Local CLI 0.10.0 (Preview)**, built on the Foundry Local SDKs, which
  exposes ``foundry status``, ``foundry server start|stop|restart``,
  ``foundry logs``, ``foundry model list|load|show`` and friends, with
  ``--output json`` on most commands.

Everything here is **best-effort and non-authoritative**. The 0.10.x CLI ships as
signed binaries whose source is not in the public ``microsoft/Foundry-Local``
repository, so the JSON schema of ``foundry status --output json`` is unverified
and has no documented stability contract. Every function in this module
therefore treats *any* failure — missing binary, non-zero exit, timeout,
malformed JSON, unexpected shape — as a plain miss and returns ``None``. A miss
falls through to the localhost scan, which is version-agnostic because it tests
the HTTP surface rather than the CLI.

Shelling out also adds a process-spawn dependency, which is appropriate for a
host-installed sidecar and wrong in a container — where an explicit backend URL
is the supported path anyway. See :func:`in_container` and the
``backend.use_foundry_cli`` setting.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from urllib.parse import urlparse

logger = logging.getLogger("kvstream.foundry_cli")

# CLI generations.
GEN_SDK = "0.10.x"  # foundry server / foundry status
GEN_SERVICE = "service"  # foundry service start|status
GEN_UNKNOWN = "unknown"  # a foundry binary we could not classify
GEN_ABSENT = "absent"  # no foundry binary on PATH

_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")
_URL_RE = re.compile(r"https?://[^\s\"'<>,)\]]+")

# Keys whose values are most likely to hold the endpoint, in preference order.
_URL_KEY_HINTS = ("endpoint", "url", "uri", "address", "baseaddress", "host")

START_COMMANDS = {
    GEN_SDK: "foundry server start",
    GEN_SERVICE: "foundry service start",
}


def _origin(url: str) -> str:
    """Reduce a URL to scheme://host[:port], dropping any path."""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}"


def start_command_hint(generation: str) -> str:
    """An actionable start command for the CLI generation actually detected."""
    known = START_COMMANDS.get(generation)
    if known:
        return f"`{known}`"
    return "`foundry service start` (service-based CLI) or `foundry server start` (CLI 0.10.0+)"


def in_container() -> bool:
    """
    Best-effort container detection.

    Used only to decide whether shelling out to a CLI is appropriate. A false
    negative costs a failed subprocess call; a false positive costs a port scan.
    Neither is harmful, so this stays deliberately simple.
    """
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return True
    if sys.platform == "win32":
        return False
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        return True
    try:
        with open("/proc/1/cgroup", encoding="utf-8") as f:
            blob = f.read()
        return any(tag in blob for tag in ("docker", "containerd", "kubepods", "lxc"))
    except OSError:
        return False


@dataclass(frozen=True)
class FoundryCli:
    """What we could learn about the installed Foundry Local CLI."""

    path: str | None = None
    version: str | None = None
    generation: str = GEN_ABSENT
    detail: str = ""

    @property
    def available(self) -> bool:
        return self.path is not None

    @property
    def start_command(self) -> str:
        return start_command_hint(self.generation)

    def as_dict(self) -> dict:
        return {
            "available": self.available,
            "path": self.path,
            "version": self.version,
            "generation": self.generation,
            "detail": self.detail,
        }


@dataclass
class CliEndpoint:
    """An endpoint the CLI reported, plus which command produced it."""

    url: str
    command: str
    raw_keys: list[str] = field(default_factory=list)

    def candidates(self) -> list[str]:
        """
        Base URLs to try, in order: as reported, then its bare origin.

        The service-based CLI reports a *status page*, not an API root —
        `foundry service status` prints `http://127.0.0.1:PORT/openai/status`.
        Appending `/v1/models` to that 404s, so taking the reported string
        literally makes the whole CLI-assisted step silently useless for the
        generation it was written for. Trying the origin as well fixes that
        without assuming every deployment serves the API at the root: whichever
        candidate actually answers is the one used.
        """
        origin = _origin(self.url)
        return [self.url] if origin == self.url else [self.url, origin]


def _run(argv: list[str], timeout: float) -> tuple[int, str] | None:
    """Run ``argv``, returning (returncode, stdout+stderr), or None on failure."""
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            # Never let a CLI prompt block the gateway's startup.
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.debug("foundry CLI call %s failed: %s", argv, exc)
        return None
    return proc.returncode, f"{proc.stdout}\n{proc.stderr}"


async def _run_async(argv: list[str], timeout: float) -> tuple[int, str] | None:
    """Run a CLI command off the event loop (subprocess calls block)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run, argv, timeout)


def _classify(output: str) -> tuple[str | None, str]:
    """Map ``foundry --version`` output to (version, generation)."""
    match = _VERSION_RE.search(output or "")
    if not match:
        return None, GEN_UNKNOWN
    version = match.group(0)
    major, minor, _ = (int(g) for g in match.groups())
    # 0.10.0 (Preview) replaced the service-based CLI. Anything at or above it
    # speaks the `foundry server` / `foundry status` dialect.
    if (major, minor) >= (0, 10):
        return version, GEN_SDK
    return version, GEN_SERVICE


async def detect(cli_path: str | None = None, timeout: float = 5.0) -> FoundryCli:
    """Locate and classify the Foundry Local CLI. Never raises."""
    path = cli_path or shutil.which("foundry")
    if not path:
        return FoundryCli(generation=GEN_ABSENT, detail="no `foundry` binary on PATH")

    result = await _run_async([path, "--version"], timeout)
    if result is None:
        return FoundryCli(path=path, generation=GEN_UNKNOWN, detail="`foundry --version` failed")

    code, output = result
    version, generation = _classify(output)
    if version is None:
        return FoundryCli(
            path=path,
            generation=GEN_UNKNOWN,
            detail=f"could not parse a version from `foundry --version` (exit {code})",
        )
    return FoundryCli(path=path, version=version, generation=generation)


def _walk_for_urls(node: object, key_path: str = "") -> list[tuple[str, str]]:
    """Collect (url, key_path) pairs from arbitrary JSON. Shape-agnostic."""
    found: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            found.extend(_walk_for_urls(value, f"{key_path}.{key}" if key_path else str(key)))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found.extend(_walk_for_urls(value, f"{key_path}[{index}]"))
    elif isinstance(node, str):
        match = _URL_RE.search(node)
        if match:
            found.append((match.group(0).rstrip("/"), key_path))
    return found


def extract_endpoint(payload: object) -> tuple[str | None, list[str]]:
    """
    Pull the most likely endpoint out of an unverified JSON document.

    The schema is not documented, so rather than assuming a field name this
    walks the whole document for URL-shaped strings and prefers ones sitting
    under a key that sounds like an endpoint. Returns (url, candidate_keys).
    """
    candidates = _walk_for_urls(payload)
    if not candidates:
        return None, []

    keys = [key for _, key in candidates]
    for url, key in candidates:
        if any(hint in key.lower() for hint in _URL_KEY_HINTS):
            return url, keys
    return candidates[0][0], keys


async def query_endpoint(cli: FoundryCli, timeout: float = 5.0) -> CliEndpoint | None:
    """
    Ask the CLI where Foundry Local is listening. Returns ``None`` on any miss.

    Tries the dialect matching the detected generation first, then the other
    one — a binary we failed to classify still gets both chances, and a
    misclassification is self-correcting.
    """
    if not cli.available or cli.path is None:
        return None

    order = (
        [_query_sdk, _query_service]
        if cli.generation != GEN_SERVICE
        else [_query_service, _query_sdk]
    )
    for query in order:
        try:
            found = await query(cli.path, timeout)
        except Exception as exc:  # noqa: BLE001 — a CLI must never break startup
            logger.debug("foundry CLI endpoint query raised: %s", exc)
            continue
        if found is not None:
            logger.info("foundry CLI reported endpoint %s via `%s`", found.url, found.command)
            return found
    return None


async def _query_sdk(path: str, timeout: float) -> CliEndpoint | None:
    """`foundry status --output json` — CLI 0.10.x."""
    command = "foundry status --output json"
    result = await _run_async([path, "status", "--output", "json"], timeout)
    if result is None:
        return None
    code, output = result
    if code != 0:
        logger.debug("`%s` exited %d", command, code)
        return None
    try:
        payload = json.loads(_first_json_document(output))
    except (ValueError, TypeError) as exc:
        logger.debug("`%s` did not return parseable JSON: %s", command, exc)
        return None
    url, keys = extract_endpoint(payload)
    if not url:
        logger.debug("`%s` returned JSON with no URL-shaped value", command)
        return None
    return CliEndpoint(url=url, command=command, raw_keys=keys)


async def _query_service(path: str, timeout: float) -> CliEndpoint | None:
    """`foundry service status` — service-based CLI, plain text output."""
    command = "foundry service status"
    result = await _run_async([path, "service", "status"], timeout)
    if result is None:
        return None
    code, output = result
    if code != 0:
        logger.debug("`%s` exited %d", command, code)
        return None
    match = _URL_RE.search(output or "")
    if not match:
        return None
    return CliEndpoint(url=match.group(0).rstrip("/"), command=command)


def _first_json_document(output: str) -> str:
    """
    Isolate the JSON document from command output that may carry a banner.

    ``--output json`` is documented but the surrounding chatter is not, so take
    the first balanced ``{...}`` or ``[...]`` region rather than trusting the
    whole stream to be JSON.
    """
    text = (output or "").strip()
    starts = [i for i in (text.find("{"), text.find("[")) if i != -1]
    if not starts:
        return text
    start = min(starts)
    closing = "}" if text[start] == "{" else "]"
    end = text.rfind(closing)
    if end <= start:
        return text
    return text[start : end + 1]


async def list_models(cli: FoundryCli, timeout: float = 5.0) -> list[str] | None:
    """
    Model ids from ``foundry model list --output json``.

    The proposal names ``--output json`` as the preferred model-metadata source
    (§8.2). It is used here to tell an operator that the model they configured
    is not one Foundry Local knows about — a much clearer failure than an opaque
    404 from the inference call.
    """
    if not cli.available or cli.path is None or cli.generation == GEN_SERVICE:
        return None
    result = await _run_async([cli.path, "model", "list", "--output", "json"], timeout)
    if result is None:
        return None
    code, output = result
    if code != 0:
        return None
    try:
        payload = json.loads(_first_json_document(output))
    except (ValueError, TypeError):
        return None
    return _collect_model_ids(payload)


async def show_model(cli: FoundryCli, model_id: str, timeout: float = 5.0) -> dict | None:
    """
    Raw ``foundry model show <id> --output json``, or ``None`` on any miss.

    Used to look for the architecture fields that size a KV cache. Like every
    other CLI call here the schema is unverified, so the caller searches the
    document rather than trusting a shape, and a miss is not an error.
    """
    if not cli.available or cli.path is None or cli.generation == GEN_SERVICE:
        return None
    result = await _run_async([cli.path, "model", "show", model_id, "--output", "json"], timeout)
    if result is None:
        return None
    code, output = result
    if code != 0:
        return None
    try:
        payload = json.loads(_first_json_document(output))
    except (ValueError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _collect_model_ids(payload: object) -> list[str] | None:
    """Pull model identifiers out of an unverified JSON document."""
    ids: list[str] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key.lower() in ("id", "alias", "name", "modelid") and isinstance(value, str):
                    ids.append(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    if not ids:
        return None
    # Preserve order, drop duplicates.
    return list(dict.fromkeys(ids))
