"""
Demo 4 — Live Inference via the KVStream Proxy (requires running backend)
======================================================================
Sends a sequence of chat requests to the running KVStream proxy and displays:
  • Streaming token output in real time
  • Time-to-first-token (TTFT) for each request
  • Tokens-per-second throughput
  • /health and /status after each request

Prerequisites:
  1. Foundry Local (or Ollama) is running with a loaded model
  2. KVStream proxy is running:  py -3 -m kvstream serve --backend foundry

Run:
    python demo/04_live_inference.py
    python demo/04_live_inference.py --backend ollama --model llama3.2
"""
from __future__ import annotations

import argparse
import json
import time
import httpx
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich import box

console = Console()

KVSTREAM_BASE = "http://localhost:8080"

PROMPTS = [
    "In one sentence, what is paged attention?",
    "Name three benefits of continuous batching for LLM serving.",
    "What is copy-on-write in the context of KV cache sharing?",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_health(client: httpx.Client) -> dict:
    try:
        r = client.get(f"{KVSTREAM_BASE}/health", timeout=5)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def _get_status(client: httpx.Client) -> dict:
    try:
        r = client.get(f"{KVSTREAM_BASE}/status", timeout=5)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def _render_health(h: dict) -> str:
    ok = h.get("backend_healthy", False)
    color = "green" if ok else "red"
    return (
        f"backend=[bold {color}]{h.get('backend','?')}[/]  "
        f"url={h.get('backend_url','?')}  "
        f"model={h.get('backend_model','?')}  "
        f"healthy=[bold {color}]{ok}[/]"
    )


def _render_status(s: dict) -> Table:
    t = Table(box=box.MINIMAL, show_header=False, padding=(0, 1))
    t.add_column("Key",   style="dim cyan")
    t.add_column("Value", style="white")

    mem = s.get("memory", {})
    sched = s.get("scheduler", {})
    pc = s.get("prefix_cache", {})

    t.add_row("gpu_blocks_free",   str(mem.get("gpu_blocks_free", "?")))
    t.add_row("gpu_utilisation",   f"{mem.get('gpu_utilization', 0):.1%}")
    t.add_row("cpu_blocks_free",   str(mem.get("cpu_blocks_free", "?")))
    t.add_row("scheduler_running", str(sched.get("running", "?")))
    t.add_row("scheduler_waiting", str(sched.get("waiting", "?")))
    t.add_row("prefix_cache_hits", str(pc.get("total_hits", "?")))
    return t


# ---------------------------------------------------------------------------
# Streaming chat request
# ---------------------------------------------------------------------------

def _stream_chat(
    client: httpx.Client,
    prompt: str,
    model: str,
    max_tokens: int = 120,
) -> tuple[str, float, float, int]:
    """
    Returns (full_text, ttft_seconds, total_seconds, token_count).
    Uses SSE streaming for real-time output.
    """
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.7,
        "stream": True,
    }

    full_text   = ""
    token_count = 0
    ttft        = None
    t_start     = time.perf_counter()

    with client.stream(
        "POST",
        f"{KVSTREAM_BASE}/v1/chat/completions",
        json=payload,
        timeout=120,
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line.startswith("data: "):
                continue
            raw = line[6:].strip()
            if raw == "[DONE]":
                break
            try:
                chunk = json.loads(raw)
            except json.JSONDecodeError:
                continue
            for choice in chunk.get("choices", []):
                text = choice.get("delta", {}).get("content", "")
                if text:
                    if ttft is None:
                        ttft = time.perf_counter() - t_start
                    full_text += text
                    token_count += 1

    total = time.perf_counter() - t_start
    if ttft is None:
        ttft = total
    return full_text, ttft, total, token_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(model: str = "phi-3-mini") -> None:
    console.rule("[bold blue]Demo 4 — Live Inference via KVStream Proxy")

    with httpx.Client() as client:
        # --- Health check before we start ---
        console.print("\n[bold]Pre-flight health check…[/]")
        health = _check_health(client)
        console.print(_render_health(health))

        if not health.get("backend_healthy"):
            console.print(
                "\n[bold red]Backend is not healthy.[/]\n"
                "Make sure Foundry Local (or Ollama) is running AND\n"
                "the KVStream proxy is started:  [cyan]py -3 -m kvstream serve --backend foundry[/]\n"
            )
            return

        console.print()

        # --- Send each prompt ---
        metrics = []

        for i, prompt in enumerate(PROMPTS, 1):
            console.rule(f"[cyan]Request {i}/{len(PROMPTS)}")
            console.print(f"[bold]Prompt:[/] {prompt}\n")

            output_buf = ""
            with Live(console=console, refresh_per_second=10) as live:
                # Run streaming synchronously, updating live panel per token
                payload = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 120,
                    "temperature": 0.7,
                    "stream": True,
                }
                ttft       = None
                tok_count  = 0
                t_start    = time.perf_counter()

                with client.stream(
                    "POST",
                    f"{KVSTREAM_BASE}/v1/chat/completions",
                    json=payload,
                    timeout=120,
                ) as resp:
                    resp.raise_for_status()
                    for line in resp.iter_lines():
                        if not line.startswith("data: "):
                            continue
                        raw = line[6:].strip()
                        if raw == "[DONE]":
                            break
                        try:
                            chunk = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        for choice in chunk.get("choices", []):
                            text = choice.get("delta", {}).get("content", "")
                            if text:
                                if ttft is None:
                                    ttft = time.perf_counter() - t_start
                                output_buf += text
                                tok_count  += 1
                        live.update(Panel(output_buf, title="Response", border_style="green"))

            total   = time.perf_counter() - t_start
            tps     = tok_count / total if total > 0 else 0
            ttft    = ttft or total
            metrics.append({"prompt": prompt[:45]+"…", "ttft": ttft, "tps": tps, "tokens": tok_count})

            console.print(
                f"\n  TTFT: [cyan]{ttft*1000:.0f} ms[/]  |  "
                f"Tokens: [cyan]{tok_count}[/]  |  "
                f"Throughput: [cyan]{tps:.1f} tok/s[/]\n"
            )

            # Show live status after each request
            status = _get_status(client)
            console.print("[dim]Engine status:[/]")
            console.print(_render_status(status))

        # --- Summary table ---
        console.rule("[bold green]Results")
        t = Table(title="Inference Summary", box=box.ROUNDED)
        t.add_column("Prompt",   width=50)
        t.add_column("TTFT",     justify="right", width=10)
        t.add_column("Tok/s",    justify="right", width=10)
        t.add_column("Tokens",   justify="right", width=8)

        for m in metrics:
            t.add_row(
                m["prompt"],
                f"{m['ttft']*1000:.0f} ms",
                f"{m['tps']:.1f}",
                str(m["tokens"]),
            )

        avg_ttft = sum(m["ttft"] for m in metrics) / len(metrics) * 1000
        avg_tps  = sum(m["tps"]  for m in metrics) / len(metrics)
        t.add_row("[bold]Average[/]", f"[bold]{avg_ttft:.0f} ms[/]", f"[bold]{avg_tps:.1f}[/]", "")
        console.print(t)

    console.rule("[bold green]Demo 4 complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KVStream live inference demo")
    parser.add_argument("--model", default="phi-3-mini", help="Model name to request")
    parser.add_argument("--backend", default="foundry", help="Backend hint (display only)")
    args = parser.parse_args()
    main(model=args.model)
