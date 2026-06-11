"""
Demo 2 — Prefix KV Cache
=========================
Demonstrates how KVStream avoids recomputing KV vectors for shared prefixes
(e.g. the same system prompt sent by many users).

Scenario:
  • 10 requests all start with the same system prompt
  • Without prefix cache -> every request pays full prompt cost
  • With prefix cache    -> the first request "warms" the cache;
                          all subsequent requests skip the shared prefix

Run:
    python demo/02_prefix_cache.py
"""
from __future__ import annotations

import hashlib
import time

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.progress import track

from kvstream.memory.block_manager import BlockManager
from kvstream.memory.prefix_cache import PrefixKVCache, _hash_tokens

console = Console()

# ---------------------------------------------------------------------------
# Fake tokeniser (byte-level, good enough for the demo)
# ---------------------------------------------------------------------------

def tokenise(text: str) -> list[int]:
    return list(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Simulated token costs
# ---------------------------------------------------------------------------

TOKENS_PER_MS = 4    # rough model throughput

def mock_prefill_time(num_tokens: int) -> float:
    """Simulate prefill latency as proportional to token count."""
    return num_tokens / TOKENS_PER_MS   # ms


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a helpful, respectful and honest assistant specialised in GPU "
    "architecture and CUDA programming. Always answer in clear, concise "
    "English and provide code examples where appropriate. "
    "Do not hallucinate. If you are unsure, say so."
)

USER_QUESTIONS = [
    "What is paged attention?",
    "Explain CUDA memory coalescing.",
    "How does FlashAttention reduce HBM traffic?",
    "What is warp divergence and how do I avoid it?",
    "Describe the CUDA execution model.",
    "What is the difference between L1 and L2 cache on an H100?",
    "How does tensor parallelism work?",
    "What is the roofline model?",
    "Explain continuous batching in LLM serving.",
    "How does KV cache quantization work?",
]


def main() -> None:
    console.rule("[bold blue]Demo 2 — Prefix KV Cache")

    BLOCK_SIZE = 16
    bm = BlockManager(num_gpu_blocks=512, num_cpu_blocks=256, block_size=BLOCK_SIZE)
    cache = PrefixKVCache(
        block_manager=bm,
        max_prefix_length=2048,
        ttl_seconds=3600,
        min_match_tokens=BLOCK_SIZE,
    )

    system_tokens = tokenise(SYSTEM_PROMPT)

    console.print(Panel(
        f"[bold]System prompt[/bold]\n\"{SYSTEM_PROMPT[:80]}…\"\n\n"
        f"  Tokenised length : [cyan]{len(system_tokens)} tokens[/]\n"
        f"  Requests         : [cyan]{len(USER_QUESTIONS)}[/]\n"
        f"  Block size       : [cyan]{BLOCK_SIZE} tokens/page[/]",
        title="Setup",
        border_style="blue",
    ))

    # ------------------------------------------------------------------
    # Without prefix cache
    # ------------------------------------------------------------------
    console.rule("[red]Without prefix cache")
    no_cache_total_ms = 0.0
    for q in USER_QUESTIONS:
        full_tokens = tokenise(SYSTEM_PROMPT + "\n\nUser: " + q)
        t = mock_prefill_time(len(full_tokens))
        no_cache_total_ms += t

    console.print(
        f"  Total prefill cost (10 requests): [bold red]{no_cache_total_ms:.1f} ms[/]\n"
        f"  Each request recomputes the full [cyan]{len(system_tokens)}-token[/] system prompt."
    )

    # ------------------------------------------------------------------
    # With prefix cache (warm on first request)
    # ------------------------------------------------------------------
    console.rule("[green]With prefix cache")

    results: list[dict] = []
    cache_total_ms = 0.0

    for i, q in enumerate(track(USER_QUESTIONS, description="Processing requests…")):
        full_tokens = tokenise(SYSTEM_PROMPT + "\n\nUser: " + q)
        user_tokens = tokenise("\n\nUser: " + q)

        seq_id = f"req-{i}"

        # Try to match a cached prefix
        hit = cache.match(full_tokens)

        if hit:
            # Only pay for the user's tokens beyond the cached prefix
            tokens_to_prefill = len(full_tokens) - hit.num_tokens
            t = mock_prefill_time(tokens_to_prefill)
            # Fork block table (CoW) — O(1), no data copy
            try:
                bm.fork(hit.seq_id, seq_id)
            except Exception:
                bm.allocate(seq_id, tokens_to_prefill + 1)
            status = "HIT"
        else:
            # Cold path — full prefill, then register every block-aligned
            # sub-prefix of the system prompt so future requests can match
            # at any depth of the prefix tree.
            tokens_to_prefill = len(full_tokens)
            t = mock_prefill_time(tokens_to_prefill)
            bm.allocate(seq_id, len(full_tokens))
            for sub_len in range(BLOCK_SIZE, len(system_tokens) + 1, BLOCK_SIZE):
                cache.register(seq_id, system_tokens[:sub_len])
            status = "MISS"

        cache_total_ms += t
        results.append({
            "req": i + 1,
            "question": q[:45] + "…",
            "status": status,
            "tokens_prefilled": tokens_to_prefill,
            "time_ms": t,
        })

    # ------------------------------------------------------------------
    # Results table
    # ------------------------------------------------------------------
    t = Table(title="Per-request breakdown", box=box.ROUNDED, show_lines=False)
    t.add_column("#",        style="dim",    justify="right", width=4)
    t.add_column("Question", width=50)
    t.add_column("Cache",    justify="center", width=8)
    t.add_column("Tokens prefilled", justify="right", width=18)
    t.add_column("Prefill cost",     justify="right", width=14)

    for r in results:
        color = "green" if r["status"] == "HIT" else "yellow"
        t.add_row(
            str(r["req"]),
            r["question"],
            f"[{color}]{r['status']}[/]",
            str(r["tokens_prefilled"]),
            f"{r['time_ms']:.1f} ms",
        )

    console.print(t)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    saved_ms = no_cache_total_ms - cache_total_ms
    speedup  = no_cache_total_ms / cache_total_ms if cache_total_ms > 0 else float("inf")

    console.print(Panel(
        f"[bold]No prefix cache:[/]   [red]{no_cache_total_ms:.1f} ms[/]\n"
        f"[bold]With prefix cache:[/] [green]{cache_total_ms:.1f} ms[/]\n"
        f"[bold]Time saved:[/]        [cyan]{saved_ms:.1f} ms[/]\n"
        f"[bold]Speedup:[/]           [bold cyan]{speedup:.2f}×[/]\n\n"
        "The first request (cache MISS) pays full cost to warm the cache.\n"
        "Every subsequent request shares the same KV pages via CoW fork — "
        "only the [italic]user portion[/italic] of the prompt is prefilled.",
        title="Summary",
        border_style="green",
    ))

    console.print("\n[dim]Cache stats:[/]")
    stats = cache.stats()
    for k, v in stats.items():
        console.print(f"  {k}: [cyan]{v}[/]")

    # Alias check (stats keys)
    # cached_prefixes, total_prefix_hits, cached_tokens

    console.rule("[bold green]Demo 2 complete")


if __name__ == "__main__":
    main()
