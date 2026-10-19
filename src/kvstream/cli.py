"""
KVStream command-line interface.

Commands:
  serve      Start the gateway in front of Foundry Local.
  health     Check gateway + backend health.
  status     Show live admission / cache stats.
  calibrate  Measure the KV-token budget B against a live Foundry Local.
  bench      Send a concurrent load and report latency / errors.
"""

from __future__ import annotations

import asyncio
import sys
import time

import typer
from rich.console import Console
from rich.table import Table

from kvstream.config import Settings


def _widen_output() -> None:
    """
    Make stdout and stderr tolerate non-ASCII, whatever they are attached to.

    On Windows a redirected stream defaults to cp1252, which cannot encode the
    arrows and dashes used in the banner and in log messages — so
    `kvstream serve > log.txt` died with a UnicodeEncodeError before the server
    ever started, and log lines came out mangled. Widen both where we can, and
    degrade to replacement characters where we cannot. stderr matters as much as
    stdout: that is where the logs go.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):  # pragma: no cover — exotic stream
            pass


def _make_console() -> Console:
    _widen_output()
    return Console()


app = typer.Typer(
    name="kvstream",
    help="Admission-control gateway for Microsoft Foundry Local.",
    add_completion=False,
)
console = _make_console()


def _settings(config: str | None, **overrides) -> Settings:
    try:
        s = Settings.load(config)
    except (FileNotFoundError, ValueError) as exc:
        # Includes pydantic's ValidationError (a ValueError): a bad config file
        # is reported as such, not as a traceback.
        console.print(f"[red]Invalid configuration:[/] {exc}")
        raise typer.Exit(2) from exc
    for key, value in overrides.items():
        if value is None:
            continue
        section, _, field = key.partition(".")
        if field:
            setattr(getattr(s, section), field, value)
        else:
            setattr(s, section, value)
    return s


def _backend_label(s: Settings) -> str:
    """
    The backend as far as it is known before the server starts.

    With discovery on, the real URL is not known until startup resolves it (and
    is logged then); printing the fallback URL here would present a guess as
    the backend.
    """
    if s.backend_is_pinned() or not s.backend.discover:
        return s.backend.base_url
    return "auto-discover (resolved URL is logged at startup)"


@app.command()
def serve(
    config: str | None = typer.Option(None, "--config", help="Path to kvstream.yaml"),
    host: str | None = typer.Option(None, help="Bind address"),
    port: int | None = typer.Option(None, help="Gateway port"),
    model: str | None = typer.Option(
        None,
        help="Model KVStream health-checks and calibrates against. Optional: auto-detected "
        "when exactly one model is loaded. Requests always name their own model.",
    ),
    backend_url: str | None = typer.Option(None, "--backend-url", help="Foundry Local base URL"),
    mode: str | None = typer.Option(None, help="admission mode: concurrency | tokens"),
    max_concurrency: int | None = typer.Option(None, help="concurrency-mode limit"),
    max_tokens: int | None = typer.Option(
        None,
        "--max-tokens",
        min=1,
        help="max_tokens for requests that set no limit of their own (default 512, or "
        "admission.default_max_tokens). A limit the client sets is never changed.",
    ),
) -> None:
    """Start the KVStream gateway."""
    from kvstream.server import serve as _serve

    s = _settings(
        config,
        host=host,
        port=port,
    )
    if model is not None:
        s.backend.model = model
    if backend_url is not None:
        s.backend.base_url = backend_url
    if mode is not None:
        s.admission.mode = mode  # type: ignore[assignment]
    if max_concurrency is not None:
        s.admission.max_concurrency = max_concurrency
    if max_tokens is not None:
        s.admission.default_max_tokens = max_tokens

    limit = (
        f"{s.admission.default_max_tokens} when a request sets none"
        if s.admission.apply_default_max_tokens
        else "model default (admission.apply_default_max_tokens is off)"
    )
    console.print(
        f"[bold cyan]KVStream[/] → Foundry Local\n"
        f"  gateway : http://{s.host}:{s.port}/v1\n"
        f"  backend : {_backend_label(s)}\n"
        f"  model   : {s.backend.model or 'auto-detect (logged at startup)'}\n"
        f"  admit   : {s.admission.mode}\n"
        f"  max_tokens : {limit}"
    )
    _serve(s)


@app.command()
def health(
    url: str = typer.Option("http://localhost:8080"),
    timeout: float = typer.Option(
        30.0,
        help="Seconds to wait. /health runs a real 1-token generation (up to "
        "backend.readiness_timeout_seconds, 15 by default), which a cold model "
        "can take most of.",
    ),
) -> None:
    """Check gateway and backend health."""
    import httpx

    try:
        data = httpx.get(f"{url}/health", timeout=timeout).json()
    except httpx.TimeoutException as exc:
        # Measured: right after a model load the probe took ~12.5 s, so the old
        # 5 s limit reported a running gateway as unreachable.
        console.print(
            f"[red]KVStream at {url} did not answer within {timeout:g}s.[/] Its readiness "
            "probe runs a real generation; raise --timeout if the model is slow."
        )
        raise typer.Exit(1) from exc
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Cannot reach KVStream at {url}: {exc}[/]")
        raise typer.Exit(1) from exc
    # The verdict is *serving* — completed a real generation — not merely
    # answering /v1/models. Judging by `backend_healthy` (liveness) reported a
    # stalled backend as healthy and exited 0, the exact failure the readiness
    # probe exists to catch. Responses without `backend_serving` (older
    # gateways) fall back to the status word.
    serving = bool(data.get("backend_serving", data.get("status") == "ok"))
    reachable = bool(data.get("backend_reachable", data.get("backend_healthy")))
    if serving:
        state, colour = "healthy (serving)", "green"
    elif reachable:
        state, colour = "reachable but NOT serving", "red"
    else:
        state, colour = "unreachable", "red"
    console.print(f"gateway : [{'green' if serving else 'yellow'}]{data.get('status')}[/]  ({url})")
    console.print(
        f"backend : [{colour}]{state}[/]  ({data.get('backend_url')} · {data.get('model')})"
    )
    if not serving:
        hint = data.get("hint") or "Is Foundry Local running and serving a model?"
        console.print(f"\n[yellow]{hint}[/]")
        # Non-zero, so scripts and monitors can act on it.
        raise typer.Exit(1)


@app.command()
def status(url: str = typer.Option("http://localhost:8080")) -> None:
    """Show live admission / cache stats."""
    import httpx

    try:
        data = httpx.get(f"{url}/status", timeout=5.0).json()
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Cannot reach {url}: {exc}[/]")
        raise typer.Exit(1) from exc
    a = data.get("admission", {})
    t = Table(title=f"KVStream — {url}")
    t.add_column("Metric", style="cyan")
    t.add_column("Value", style="green")
    for k in ("unit", "budget", "in_flight", "utilization", "waiting", "active", "overshoots"):
        t.add_row(k, str(a.get(k)))
    queue = a.get("queue", {})
    t.add_row("queue peak", str(queue.get("peak_depth")))
    t.add_row("queue oldest wait", f"{queue.get('oldest_wait_seconds', 0)}s")
    t.add_row("budget source", str(data.get("budget_source", {}).get("source")))
    backend = data.get("backend", {})
    t.add_row("backend url", str(backend.get("base_url")))
    t.add_row("usage reporting", str(backend.get("usage_reporting")))
    console.print(t)


@app.command()
def calibrate(
    config: str | None = typer.Option(None, "--config"),
    model: str | None = typer.Option(None, help="Foundry Local model id"),
    device: str | None = typer.Option(
        None,
        help="Accelerator label to key this measurement by (e.g. 'npu', 'cpu'). "
        "Defaults to a derived platform string.",
    ),
    max_concurrency: int = typer.Option(32, help="highest sweep concurrency"),
    trials: int = typer.Option(3, help="repeats per sweep point (pooled before the percentile)"),
    warmup: int = typer.Option(1, help="warm-up requests before measuring"),
    refine: bool = typer.Option(
        True,
        "--refine/--no-refine",
        help="bisect between the last healthy point and the first unhealthy one",
    ),
) -> None:
    """Measure and store the KV-token budget B for the current model/device."""
    from kvstream.admission import UNKNOWN, calibration_key_for
    from kvstream.admission.calibration import (
        CalibrationFailed,
        CalibrationKey,
        CalibrationService,
    )
    from kvstream.backend import FoundryClient, foundry_cli
    from kvstream.backend.inventory import ModelInventory, refusal_for, select_model

    s = _settings(config)
    if model is not None:
        s.backend.model = model
    if device is not None:
        s.backend.device = device

    async def _run() -> tuple[int, CalibrationKey]:
        # Reach the backend exactly as `serve` does — same pinning, same CLI
        # policy — so the environment recorded here is the one serve sees.
        client = FoundryClient(
            base_url=s.backend.base_url,
            model=s.backend.model,
            timeout=s.backend.timeout_seconds,
            discover=s.backend.discover,
            discovery_cooldown=s.backend.discovery_cooldown_seconds,
            request_usage=s.backend.request_usage,
            api_key=s.backend.api_key,
            pinned=s.backend_is_pinned(),
            use_foundry_cli=s.foundry_cli_enabled(),
            foundry_cli_path=s.backend.foundry_cli_path,
            foundry_cli_timeout=s.backend.foundry_cli_timeout_seconds,
        )
        if not await client.health():
            console.print(
                "[red]Foundry Local is not reachable — start it and load a model first.[/]\n"
                "  service-based CLI : [cyan]foundry service start[/]\n"
                "  CLI 0.10.0+       : [cyan]foundry server start[/]"
            )
            raise typer.Exit(1)
        cli = await client.detect_cli()
        inventory = ModelInventory(
            client.models_or_none,
            lambda: foundry_cli.list_loaded(cli, s.backend.foundry_cli_timeout_seconds),
        )
        if not s.backend.model:
            # A budget is only valid for the model it was measured on, so the
            # model must be unambiguous: detected only when exactly one is loaded.
            selection = select_model(await inventory.snapshot(fresh=True))
            if selection.model is None:
                console.print(f"[red]No model to calibrate: {selection.detail}[/]")
                await client.aclose()
                raise typer.Exit(1)
            console.print(f"[dim]model: {selection.model} (auto-detected: {selection.detail})[/]")
            s.backend.model = selection.model
            client.use_model(selection.model)
        # Check the model can be served before sending a sweep at it. Foundry
        # 0.10.3 answers a streamed request for an unloaded model by dropping
        # the connection, so the sweep would otherwise only report a cryptic
        # network error. Fails open when the lists cannot be read.
        refusal = refusal_for(s.backend.model, await inventory.snapshot(fresh=True))
        if refusal is not None:
            console.print(f"[red]Cannot calibrate: {refusal.message}[/]")
            await client.aclose()
            raise typer.Exit(1)
        # Keyed with the detected CLI generation and Foundry version, exactly
        # as `serve` keys its lookup. Leaving them "unknown" meant a gateway
        # could only ever find this record as a partial match.
        key = calibration_key_for(
            s.backend,
            cli_generation=cli.generation,
            backend_version=cli.version or UNKNOWN,
        )
        svc = CalibrationService(client, s.admission.calibration_store, key)
        console.print(
            "[cyan]Calibrating… (sends a rising concurrent load to Foundry Local)[/]\n"
            f"  key: [dim]{key.as_str()}[/]"
        )
        try:
            b = await svc.calibrate(
                max_concurrency=max_concurrency, trials=trials, warmup=warmup, refine=refine
            )
        except CalibrationFailed as exc:
            console.print(f"[red]Calibration failed: {exc}[/]")
            await client.aclose()
            raise typer.Exit(1) from exc
        await client.aclose()
        return b, key

    budget, key = asyncio.run(_run())
    console.print(
        f"[green]Calibrated budget B = {budget} tokens[/] → stored in "
        f"{s.admission.calibration_store}\n"
        f"  keyed by: [dim]{key.as_str()}[/]\n"
        f"Set [cyan]admission.mode: tokens[/] to use it."
    )


def _gateway_model(url: str) -> str:
    """The model a running gateway resolved, or exit saying why there is none."""
    import httpx

    try:
        info = httpx.get(f"{url}/status", timeout=10).json().get("model") or {}
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Could not read {url}/status ({exc}). Pass --model.[/]")
        raise typer.Exit(1) from exc
    if not info.get("id"):
        console.print(
            f"[red]The gateway has no model to benchmark: {info.get('source', 'unknown')}[/]\n"
            "Pass --model."
        )
        raise typer.Exit(1)
    console.print(f"[dim]model: {info['id']} ({info.get('source')})[/]")
    return str(info["id"])


@app.command()
def bench(
    url: str = typer.Option("http://localhost:8080"),
    model: str | None = typer.Option(
        None, help="Model to request. Default: the model the gateway resolved."
    ),
    concurrency: int = typer.Option(8, min=1),
    total: int = typer.Option(40, min=1),
    max_tokens: int = typer.Option(64, min=1),
) -> None:
    """Send a concurrent load through the gateway and report latency / errors."""
    import httpx

    if model is None:
        model = _gateway_model(url)

    async def _run() -> None:
        prompt = "Explain admission control for local LLM inference. " * 4
        latencies: list[float] = []
        errors = 0
        sem = asyncio.Semaphore(concurrency)

        async def one() -> None:
            nonlocal errors
            async with sem:
                t0 = time.perf_counter()
                try:
                    async with httpx.AsyncClient(timeout=120) as c:
                        r = await c.post(
                            f"{url}/v1/chat/completions",
                            json={
                                "model": model,
                                "messages": [{"role": "user", "content": prompt}],
                                "max_tokens": max_tokens,
                            },
                        )
                except Exception:  # noqa: BLE001
                    errors += 1
                    return
                if r.status_code >= 400:
                    errors += 1
                    return
                # Successes only: a fast 503 is not a fast answer, and mixing
                # them in reported a refused load as low latency.
                latencies.append(time.perf_counter() - t0)

        await asyncio.gather(*[one() for _ in range(total)])
        latencies.sort()
        n = len(latencies)
        t = Table(title="KVStream Benchmark")
        t.add_column("Metric", style="cyan")
        t.add_column("Value", style="green")
        t.add_row("requests", str(total))
        t.add_row("concurrency", str(concurrency))
        t.add_row("errors", str(errors))
        if n:
            t.add_row("p50 (successful)", f"{latencies[n // 2] * 1000:.0f} ms")
            t.add_row("p99 (successful)", f"{latencies[min(int(n * 0.99), n - 1)] * 1000:.0f} ms")
        else:
            t.add_row("p50 / p99", "n/a — no request succeeded")
        console.print(t)

    asyncio.run(_run())


if __name__ == "__main__":
    app()
