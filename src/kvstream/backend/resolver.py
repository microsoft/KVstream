"""
Version-aware backend resolution (proposal §8.3).

KVStream integrates at the OpenAI HTTP wire protocol, so both Foundry Local
generations are valid backends and the *inference* path never changes. Only two
seams are version-specific; this is the first of them — how the endpoint is
located. The chain is ordered, first hit wins:

1. **Explicit configuration** (``--backend-url``, config file, or environment).
   Always authoritative, and the only supported path in containers. When the
   operator has said where the backend is, KVStream does not go looking.
2. **CLI-assisted.** If a ``foundry`` binary is on PATH, read ``foundry
   --version`` to select the dialect, then read the endpoint from ``foundry
   status --output json`` (0.10.x) or ``foundry service status`` (service-based).
   Written defensively: any parse failure is a miss, not an error.
3. **Localhost scan** for a port answering ``/v1/models``, unchanged, as the
   fallback. Version-agnostic, because it tests the HTTP surface.
4. **Failure**, with a message naming the correct start command for the CLI
   generation actually detected.

A pinned URL — whether an operator set one, or a host application pinned
``Configuration.WebService.urls`` and told KVStream about it — removes the scan
entirely, which is the whole point of step 1.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx

from kvstream.backend import discovery, foundry_cli
from kvstream.backend.foundry_cli import FoundryCli

logger = logging.getLogger("kvstream.resolver")

SOURCE_PINNED = "pinned"
SOURCE_CLI = "foundry-cli"
SOURCE_SCAN = "scan"
SOURCE_NONE = "unresolved"


@dataclass
class Resolution:
    """Where the backend was found, and how."""

    url: str | None
    source: str
    detail: str = ""
    attempts: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "url": self.url,
            "source": self.source,
            "detail": self.detail,
            "attempts": self.attempts,
        }


class BackendResolver:
    """Runs the ordered resolution chain for one Foundry Local instance."""

    def __init__(
        self,
        *,
        configured_url: str,
        model: str,
        pinned: bool,
        use_cli: bool,
        cli_path: str | None = None,
        cli_timeout: float = 5.0,
        exclude_ports: set[int] | None = None,
    ) -> None:
        self.configured_url = configured_url.rstrip("/")
        self.model = model
        self.pinned = pinned
        self.use_cli = use_cli
        self.cli_path = cli_path
        self.cli_timeout = cli_timeout
        self.exclude_ports = exclude_ports or set()
        self.cli: FoundryCli = FoundryCli()
        self._cli_detected = False

    # -- CLI detection --------------------------------------------------

    async def detect_cli(self) -> FoundryCli:
        """Detect the CLI once, so its generation can key calibration records."""
        if self._cli_detected:
            return self.cli
        self._cli_detected = True
        if not self.use_cli:
            self.cli = FoundryCli(
                generation=foundry_cli.GEN_ABSENT, detail="foundry CLI lookup disabled"
            )
            return self.cli
        self.cli = await foundry_cli.detect(self.cli_path, self.cli_timeout)
        if self.cli.available:
            logger.info(
                "detected Foundry Local CLI %s (%s generation) at %s",
                self.cli.version or "unknown",
                self.cli.generation,
                self.cli.path,
            )
        return self.cli

    # -- the chain ------------------------------------------------------

    async def resolve(self, client: httpx.AsyncClient) -> Resolution:
        attempts: list[str] = []

        # 1. Explicit configuration is authoritative — no probing, no fallback.
        #    An operator who names the backend has taken responsibility for it,
        #    and in a container this is the only path that can work.
        if self.pinned:
            return Resolution(
                self.configured_url,
                SOURCE_PINNED,
                detail="explicit backend URL; discovery skipped",
                attempts=["pinned"],
            )

        # 2. CLI-assisted lookup.
        cli = await self.detect_cli()
        if self.use_cli and cli.available:
            endpoint = await foundry_cli.query_endpoint(cli, self.cli_timeout)
            attempts.append(f"foundry-cli({cli.generation})")
            if endpoint is not None:
                for url in endpoint.candidates():
                    if await discovery.probe_url(client, url) is not None:
                        return Resolution(
                            url,
                            SOURCE_CLI,
                            detail=f"reported by `{endpoint.command}`",
                            attempts=attempts,
                        )
                # The CLI named an endpoint that does not answer. Say so — this
                # is exactly the case the scan exists to rescue.
                logger.info(
                    "`%s` reported %s but no candidate answered /v1/models; falling back to scan",
                    endpoint.command,
                    endpoint.url,
                )
                attempts.append("foundry-cli-endpoint-dead")
        elif self.use_cli:
            attempts.append(f"foundry-cli({cli.generation})")

        # 3. The localhost scan, unchanged.
        attempts.append("scan")
        found = await discovery.discover(
            client, self.configured_url, self.exclude_ports, prefer_model=self.model
        )
        if found:
            return Resolution(found, SOURCE_SCAN, detail="localhost scan", attempts=attempts)

        # 4. Failure, with a command the operator can actually run.
        return Resolution(None, SOURCE_NONE, detail=self.failure_hint(), attempts=attempts)

    def failure_hint(self) -> str:
        """An actionable message for the CLI generation actually detected."""
        if self.cli.available:
            which = f"Foundry Local CLI {self.cli.version or ''} ({self.cli.generation})".strip()
            return (
                f"No Foundry Local endpoint found. {which} is installed — start it with "
                f"{self.cli.start_command}, then load a model."
            )
        return (
            "No Foundry Local endpoint found and no `foundry` binary on PATH. Install "
            "Foundry Local and start it with "
            f"{foundry_cli.start_command_hint(foundry_cli.GEN_ABSENT)}, or set an explicit "
            "backend URL (--backend-url / backend.base_url / KVSTREAM_BACKEND__BASE_URL)."
        )
