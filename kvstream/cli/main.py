"""
KVStream CLI

Commands:
  kvstream serve    Start the proxy
  kvstream bench    Throughput / latency benchmark
  kvstream status   Live scheduler + memory stats
  kvstream health   Backend health check
"""

from __future__ import annotations

import asyncio
import time

import typer
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

app = typer.Typer(
    name="kvstream",
    help="KVStream - PagedAttention + continuous batching for local LLM inference",
    add_completion=False,
)
console = Console()

_DEFAULT_URLS = {
    "ollama": "http://localhost:11434",
    "foundry": "http://localhost:5273",
    "llamacpp": "http://localhost:8080",
    "lmstudio": "http://localhost:1234",
}

_DEFAULT_MODELS = {
    "ollama": "llama3.2",
    "foundry": "phi-3-mini",
    "llamacpp": "local",
    "lmstudio": "local-model",
}


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


@app.command()
def serve(
    backend: str = typer.Option("foundry", help="ollama | foundry | llamacpp | lmstudio"),
    backend_url: str | None = typer.Option(None, "--backend-url", help="Override backend base URL"),
    model: str | None = typer.Option(None, help="Model name (backend default if omitted)"),
    port: int = typer.Option(8080, help="Proxy port"),
    host: str = typer.Option(
        "127.0.0.1", help="Bind address (use 0.0.0.0 to expose on the network — no auth layer!)"
    ),
    gpu_blocks: int = typer.Option(256, help="GPU KV page blocks  (256~2GB, 512~4GB, 1024~8GB)"),
    cpu_blocks: int = typer.Option(512, help="CPU swap page blocks"),
    block_size: int = typer.Option(16, help="Tokens per page (power of 2)"),
    max_batch: int = typer.Option(8, help="Max concurrent sequences"),
    config_file: str | None = typer.Option(None, "--config", help="Path to kvstream.yaml"),
    no_prefix_cache: bool = typer.Option(False, "--no-prefix-cache"),
    log_level: str = typer.Option("INFO", help="DEBUG|INFO|WARNING|ERROR"),
):
    """Start the KVStream inference proxy."""
    from kvstream.backends import FoundryBackend, LlamaCppBackend, LMStudioBackend, OllamaBackend
    from kvstream.config import KVStreamConfig
    from kvstream.engine import KVStreamEngine

    if backend not in _DEFAULT_URLS:
        console.print(f"[red]Unknown backend '{backend}'. Choose: {list(_DEFAULT_URLS)}")
        raise typer.Exit(1)

    # Load kvstream.yaml (if present) for base URL / model defaults
    cfg = KVStreamConfig.auto(config_file)
    yaml_url = cfg.backend.base_url
    yaml_model = cfg.backend.model

    # CLI flag > kvstream.yaml > hardcoded default
    url = backend_url or yaml_url or _DEFAULT_URLS[backend]
    mdl = model or yaml_model or _DEFAULT_MODELS[backend]

    # Build backend object
    _BACKENDS = {
        "ollama": lambda: OllamaBackend(base_url=url, model=mdl),
        "foundry": lambda: FoundryBackend(base_url=url, model=mdl, exclude_ports=[port]),
        "llamacpp": lambda: LlamaCppBackend(base_url=url),
        "lmstudio": lambda: LMStudioBackend(base_url=url, model=mdl),
    }
    backend_obj = _BACKENDS[backend]()

    # Load config (YAML / env / defaults), then CLI args ALWAYS override
    cfg = KVStreamConfig.auto(config_file)

    # --- CLI args stamp over whatever the config loaded ---
    cfg.host = host
    cfg.port = port
    cfg.backend.type = backend  # type: ignore[assignment]
    cfg.backend.base_url = url
    cfg.backend.model = mdl
    cfg.memory.num_gpu_blocks = gpu_blocks
    cfg.memory.num_cpu_blocks = cpu_blocks
    cfg.memory.block_size = block_size
    cfg.scheduler.max_batch_size = max_batch
    cfg.prefix_cache.enabled = not no_prefix_cache
    cfg.observability.log_level = log_level.upper()  # type: ignore[assignment]

    # Memory size hint for the startup panel
    bytes_per_block = block_size * 2 * 32 * 32 * 128 * 2
    gpu_mb = (gpu_blocks * bytes_per_block) / (1024**2)
    cpu_mb = (cpu_blocks * bytes_per_block) / (1024**2)

    console.print(
        Panel.fit(
            f"[bold cyan]KVStream[/] - starting\n"
            f"Backend : [yellow]{backend}[/] @ {url}\n"
            f"Model   : {mdl}\n"
            f"GPU pool: {gpu_blocks} blocks x {block_size} tok  ~[green]{gpu_mb:.0f} MB[/]\n"
            f"CPU swap: {cpu_blocks} blocks                ~[blue]{cpu_mb:.0f} MB[/]\n"
            f"Batch   : {max_batch} max concurrent seqs\n"
            f"Proxy   : [green]http://{host}:{port}/v1[/]",
            title="KVStream",
        )
    )

    engine = KVStreamEngine(
        backend=backend_obj,
        config=cfg,
        num_gpu_blocks=gpu_blocks,
        num_cpu_blocks=cpu_blocks,
        block_size=block_size,
        max_batch_size=max_batch,
    )

    asyncio.run(engine.serve(host=host, port=port))


# ---------------------------------------------------------------------------
# bench
# ---------------------------------------------------------------------------


@app.command()
def bench(
    url: str = typer.Option("http://localhost:8080"),
    model: str = typer.Option("phi-3-mini"),
    concurrency: int = typer.Option(4, help="Parallel requests"),
    prompt_len: int = typer.Option(128, help="Approximate prompt tokens"),
    output_len: int = typer.Option(64, help="Max output tokens"),
    total_requests: int = typer.Option(20, help="Total requests to send"),
):
    """Benchmark throughput and latency against a running KVStream proxy."""
    import httpx

    async def _run():
        prompt = ("Tell me about large language model inference optimisation. " * 10)[
            : prompt_len * 6
        ]
        latencies: list[float] = []
        errors = 0
        sem = asyncio.Semaphore(concurrency)

        async def one():
            nonlocal errors
            async with sem:
                t0 = time.perf_counter()
                try:
                    async with httpx.AsyncClient(timeout=120) as c:
                        async with c.stream(
                            "POST",
                            f"{url}/v1/chat/completions",
                            json={
                                "model": model,
                                "messages": [{"role": "user", "content": prompt}],
                                "max_tokens": output_len,
                                "stream": True,
                            },
                        ) as r:
                            async for _ in r.aiter_lines():
                                pass
                except Exception as e:
                    errors += 1
                    console.print(f"  [red]error:[/] {e}")
                finally:
                    latencies.append(time.perf_counter() - t0)

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), TimeElapsedColumn()) as p:
            task = p.add_task(
                f"Running {total_requests} requests (concurrency={concurrency})",
                total=total_requests,
            )
            for coro in asyncio.as_completed([one() for _ in range(total_requests)]):
                await coro
                p.advance(task)

        latencies.sort()
        n = len(latencies)
        t = Table(title="KVStream Benchmark")
        t.add_column("Metric", style="cyan")
        t.add_column("Value", style="green")
        t.add_row("Requests", str(total_requests))
        t.add_row("Concurrency", str(concurrency))
        t.add_row("Errors", str(errors))
        t.add_row("Throughput", f"{total_requests / (sum(latencies) / concurrency):.1f} req/s")
        t.add_row("p50", f"{latencies[n // 2] * 1000:.0f} ms")
        t.add_row("p99", f"{latencies[min(int(n * 0.99), n - 1)] * 1000:.0f} ms")
        console.print(t)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@app.command()
def status(
    url: str = typer.Option("http://localhost:8080"),
    watch: bool = typer.Option(False, "--watch", help="Refresh every second"),
):
    """Show live scheduler and memory stats."""
    import httpx

    async def _fetch():
        async with httpx.AsyncClient() as c:
            return (await c.get(f"{url}/status", timeout=5.0)).json()

    def _render(data: dict) -> Table:
        t = Table(title=f"KVStream - {url}")
        t.add_column("Layer", style="cyan", min_width=14)
        t.add_column("Metric", style="dim", min_width=22)
        t.add_column("Value", style="green")

        s = data.get("scheduler", {})
        t.add_row("Scheduler", "Waiting", str(s.get("waiting", "?")))
        t.add_row("", "Running", str(s.get("running", "?")))
        t.add_row("", "Swapped", str(s.get("swapped", "?")))

        m = data.get("memory", {})
        pct = float(m.get("gpu_utilization", 0)) * 100
        col = "green" if pct < 70 else "yellow" if pct < 90 else "red"
        t.add_row("Memory", "GPU blocks free", str(m.get("gpu_blocks_free", "?")))
        t.add_row("", "GPU utilization", f"[{col}]{pct:.1f}%[/{col}]")
        t.add_row("", "CPU blocks free", str(m.get("cpu_blocks_free", "?")))

        pc = data.get("prefix_cache", {})
        if pc:
            t.add_row("Prefix cache", "Entries", str(pc.get("cached_prefixes", 0)))
            t.add_row("", "Total hits", str(pc.get("total_prefix_hits", 0)))
            t.add_row("", "Cached tokens", str(pc.get("cached_tokens", 0)))
        return t

    if watch:
        with Live(refresh_per_second=1) as live:
            while True:
                try:
                    live.update(_render(asyncio.run(_fetch())))
                except Exception as e:
                    live.update(f"[red]{e}[/]")
                time.sleep(1)
    else:
        try:
            console.print(_render(asyncio.run(_fetch())))
        except Exception as e:
            console.print(f"[red]Cannot reach {url}: {e}[/]")
            raise typer.Exit(1)


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


@app.command()
def health(url: str = typer.Option("http://localhost:8080")):
    """Check KVStream proxy and backend health."""
    import httpx

    async def _check():
        async with httpx.AsyncClient() as c:
            return (await c.get(f"{url}/health", timeout=5.0)).json()

    try:
        d = asyncio.run(_check())
        ok = d.get("status") == "ok"
        be = d.get("backend_healthy", False)
        console.print(
            f"KVStream proxy : [{'green' if ok else 'yellow'}]{d.get('status', '?')}[/]  ({url})"
        )
        console.print(
            f"Backend   : [{'green' if be else 'red'}]"
            f"{'healthy (ok)' if be else 'unreachable (!!)'}[/]  "
            f"({d.get('backend', '?')} @ {d.get('backend_url', 'unknown')})"
        )
        if not be:
            console.print("\n[yellow]Troubleshooting:[/]")
            console.print("  * Is Foundry Local running?  Run: [cyan]foundrylocal serve[/]")
            console.print("  * Is Ollama running?         Run: [cyan]ollama serve[/]")
            console.print("  * Did you pass the right --backend flag?")
            console.print("  * Try: [cyan]curl.exe http://localhost:5273/v1/models[/]  (Foundry)")
            console.print("         [cyan]curl.exe http://localhost:11434/api/tags[/]  (Ollama)")
    except Exception as e:
        console.print(f"[red]Cannot reach KVStream at {url}: {e}[/]")
        raise typer.Exit(1)
