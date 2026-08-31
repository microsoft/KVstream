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
    s = Settings.load(config)
    for key, value in overrides.items():
        if value is None:
            continue
        section, _, field = key.partition(".")
        if field:
            setattr(getattr(s, section), field, value)
        else:
            setattr(s, section, value)
    return s


@app.command()
def serve(
    config: str | None = typer.Option(None, "--config", help="Path to kvstream.yaml"),
    host: str | None = typer.Option(None, help="Bind address"),
    port: int | None = typer.Option(None, help="Gateway port"),
    model: str | None = typer.Option(None, help="Foundry Local model id"),
    backend_url: str | None = typer.Option(None, "--backend-url", help="Foundry Local base URL"),
    mode: str | None = typer.Option(None, help="admission mode: concurrency | tokens"),
    max_concurrency: int | None = typer.Option(None, help="concurrency-mode limit"),
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

    console.print(
        f"[bold cyan]KVStream[/] → Foundry Local\n"
        f"  gateway : http://{s.host}:{s.port}/v1\n"
        f"  backend : {s.backend.base_url}  (model: {s.backend.model})\n"
        f"  admit   : {s.admission.mode}"
    )
    _serve(s)


@app.command()
def health(url: str = typer.Option("http://localhost:8080")) -> None:
    """Check gateway and backend health."""
    import httpx

    try:
        data = httpx.get(f"{url}/health", timeout=5.0).json()
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Cannot reach KVStream at {url}: {exc}[/]")
        raise typer.Exit(1) from exc
    ok = data.get("backend_healthy")
    console.print(f"gateway : [green]{data.get('status')}[/]  ({url})")
    console.print(
        f"backend : [{'green' if ok else 'red'}]{'healthy' if ok else 'unreachable'}[/]"
        f"  ({data.get('backend_url')} · {data.get('model')})"
    )
    if not ok:
        console.print("\n[yellow]Is Foundry Local running and serving a model?[/]")


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
    for k in (
        "unit",
        "budget",
        "in_flight",
        "utilization",
        "waiting",
        "active",
        "overshoots",
    ):
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
    from kvstream.admission import calibration_key_for
    from kvstream.admission.calibration import CalibrationService
    from kvstream.backend import FoundryClient

    s = _settings(config)
    if model is not None:
        s.backend.model = model
    if device is not None:
        s.backend.device = device

    key = calibration_key_for(s.backend)

    async def _run() -> int:
        client = FoundryClient(
            base_url=s.backend.base_url,
            model=s.backend.model,
            timeout=s.backend.timeout_seconds,
            discover=s.backend.discover,
            discovery_cooldown=s.backend.discovery_cooldown_seconds,
            request_usage=s.backend.request_usage,
        )
        if not await client.health():
            console.print(
                "[red]Foundry Local is not reachable — start it and load a model first.[/]\n"
                "  service-based CLI : [cyan]foundry service start[/]\n"
                "  CLI 0.10.0+       : [cyan]foundry server start[/]"
            )
            raise typer.Exit(1)
        svc = CalibrationService(client, s.admission.calibration_store, key)
        console.print(
            "[cyan]Calibrating… (sends a rising concurrent load to Foundry Local)[/]\n"
            f"  key: [dim]{key.as_str()}[/]"
        )
        b = await svc.calibrate(
            max_concurrency=max_concurrency, trials=trials, warmup=warmup, refine=refine
        )
        await client.aclose()
        return b

    budget = asyncio.run(_run())
    console.print(
        f"[green]Calibrated budget B = {budget} tokens[/] → stored in "
        f"{s.admission.calibration_store}\n"
        f"  keyed by: [dim]{key.as_str()}[/]\n"
        f"Set [cyan]admission.mode: tokens[/] to use it."
    )


@app.command()
def bench(
    url: str = typer.Option("http://localhost:8080"),
    model: str = typer.Option("phi-3-mini"),
    concurrency: int = typer.Option(8),
    total: int = typer.Option(40),
    max_tokens: int = typer.Option(64),
) -> None:
    """Send a concurrent load through the gateway and report latency / errors."""
    import httpx

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
                        if r.status_code >= 400:
                            errors += 1
                except Exception:  # noqa: BLE001
                    errors += 1
                finally:
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
        t.add_row("p50", f"{latencies[n // 2] * 1000:.0f} ms")
        t.add_row("p99", f"{latencies[min(int(n * 0.99), n - 1)] * 1000:.0f} ms")
        console.print(t)

    asyncio.run(_run())


if __name__ == "__main__":
    app()
