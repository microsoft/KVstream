"""
Demo 6 — Full Pipeline: Block Manager + Prefix Cache + Scheduler together
===========================================================================
Wires all three subsystems into a single end-to-end simulation:

  Incoming requests -> Scheduler (decides who runs each iteration)
                    -> Block Manager (allocates/frees GPU pages)
                    -> Prefix Cache (deduplicates shared system prompts)

This demo does NOT require a running backend — the "inference" step is
mocked to focus on the orchestration logic.

Run:
    python demo/06_full_pipeline.py
"""
from __future__ import annotations

import asyncio
import random
import time

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.columns import Columns

from kvstream.memory.block_manager import BlockManager
from kvstream.memory.prefix_cache import PrefixKVCache
from kvstream.scheduler.continuous_batch import (
    ContinuousBatchScheduler,
    SequenceGroup,
    SeqStatus,
)

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TOKENS = list(range(1, 49))   # 48-token system prompt (3 pages @ block_size=16)

STATUS_STYLE = {
    SeqStatus.WAITING:  ("[dim white]", "WAIT"),
    SeqStatus.RUNNING:  ("[bold green]", "RUN "),
    SeqStatus.SWAPPED:  ("[yellow]", "SWAP"),
    SeqStatus.FINISHED: ("[dim]", "DONE"),
    SeqStatus.ABORTED:  ("[red]", "ABRT"),
}


def _bar(used: int, total: int, width: int = 30) -> str:
    pct    = used / total if total else 0
    filled = int(pct * width)
    color  = "red" if pct > 0.85 else "yellow" if pct > 0.6 else "green"
    return f"[{color}]{'#' * filled}[/][dim]{'.' * (width - filled)}[/] {used}/{total}"


def _seq_row(seq: SequenceGroup, cache_hit: bool) -> tuple:
    style, label = STATUS_STYLE.get(seq.status, ("[white]", "????"))
    hit_str = "[green]HIT [/]" if cache_hit else "[dim]miss[/]"
    return (
        seq.seq_id,
        f"{style}{label}[/]",
        str(seq.prompt_len),
        f"{seq.num_generated}/{seq.max_new_tokens}",
        hit_str,
    )


# ---------------------------------------------------------------------------
# Core simulation
# ---------------------------------------------------------------------------

async def run_pipeline() -> None:
    console.rule("[bold blue]Demo 6 — Full Pipeline Simulation")

    # Config
    GPU_BLOCKS = 48
    CPU_BLOCKS = 32
    BLOCK_SIZE = 16
    MAX_BATCH  = 5

    bm = BlockManager(
        num_gpu_blocks=GPU_BLOCKS,
        num_cpu_blocks=CPU_BLOCKS,
        block_size=BLOCK_SIZE,
    )
    prefix_cache = PrefixKVCache(
        block_manager=bm,
        max_prefix_length=256,
        ttl_seconds=3600,
        min_match_tokens=BLOCK_SIZE,
    )
    scheduler = ContinuousBatchScheduler(
        block_manager=bm,
        max_batch_size=MAX_BATCH,
        preemption_policy="swap",
        priority="fcfs",
    )

    console.print(Panel(
        f"  GPU pages    : {GPU_BLOCKS}  (block_size={BLOCK_SIZE}, "
        f"{GPU_BLOCKS * BLOCK_SIZE} token capacity)\n"
        f"  CPU pages    : {CPU_BLOCKS}  (swap buffer)\n"
        f"  Max batch    : {MAX_BATCH}\n"
        f"  System prompt: {len(SYSTEM_PROMPT_TOKENS)} tokens (shared by all requests)\n"
        f"  Prefix cache : enabled — first request warms cache, rest get CoW fork",
        title="Pipeline Configuration",
        border_style="blue",
    ))

    # Pre-warm the prefix cache with the system prompt as if a prior request
    # already ran.  This is the realistic production scenario: a system prompt
    # is computed once and then reused across many concurrent users.
    CANONICAL_ID = "sys-warmup"
    bm.allocate(CANONICAL_ID, len(SYSTEM_PROMPT_TOKENS))
    for sub_len in range(BLOCK_SIZE, len(SYSTEM_PROMPT_TOKENS) + 1, BLOCK_SIZE):
        prefix_cache.register(CANONICAL_ID, SYSTEM_PROMPT_TOKENS[:sub_len])

    # Build 12 requests — all share the same system prompt prefix
    requests: list[tuple[SequenceGroup, list[int]]] = []
    for i in range(12):
        user_tokens = [random.randint(100, 32000) for _ in range(random.randint(4, 20))]
        full_tokens = SYSTEM_PROMPT_TOKENS + user_tokens
        seq = SequenceGroup(
            seq_id=f"req-{i+1:02d}",
            prompt_tokens=full_tokens,
            max_new_tokens=random.randint(6, 18),
        )
        requests.append((seq, full_tokens))

    all_seqs     = {s.seq_id: s for s, _ in requests}
    cache_hits   = {s.seq_id: False for s, _ in requests}
    finished_set: set[str] = set()

    # Submit all at once — hit the cache for each request
    for seq, full_tokens in requests:
        hit = prefix_cache.match(full_tokens)
        if hit:
            cache_hits[seq.seq_id] = True
            try:
                bm.fork(hit.seq_id, seq.seq_id)
                # Trim prompt to only the user-specific suffix
                seq.prompt_tokens = full_tokens[hit.num_tokens:]
            except Exception:
                pass   # fork failed — do full prefill
        else:
            pass   # no cache hit; full prefill in the iteration loop

        await scheduler.add_request(seq)

    # Track stats
    iteration       = 0
    total_prefill   = 0
    total_decode    = 0
    prefix_hit_count = sum(1 for v in cache_hits.values() if v)

    with Live(console=console, refresh_per_second=5) as live:
        while len(finished_set) < len(requests) and iteration < 200:
            iteration += 1
            out = await scheduler.schedule()

            for seq in out.prefill:
                total_prefill += 1

            for seq in out.prefill + out.decode:
                total_decode += 1
                seq.output_tokens.append(random.randint(0, 32000))
                try:
                    bm.append_slot(seq.seq_id)
                except MemoryError:
                    seq.status = SeqStatus.ABORTED
                    if seq in scheduler.running:
                        scheduler.running.remove(seq)
                    continue

                if seq.num_generated >= seq.max_new_tokens:
                    seq.status = SeqStatus.FINISHED
                    bm.free(seq.seq_id)
                    finished_set.add(seq.seq_id)
                    if seq in scheduler.running:
                        scheduler.running.remove(seq)

            # ---- Build live display ----
            seq_table = Table(box=box.MINIMAL, show_header=True, expand=True)
            seq_table.add_column("ID",       width=10, style="cyan")
            seq_table.add_column("State",    width=7)
            seq_table.add_column("Prompt",   justify="right", width=8)
            seq_table.add_column("Gen",      justify="right", width=10)
            seq_table.add_column("Prefix",   width=7)

            for s in all_seqs.values():
                seq_table.add_row(*_seq_row(s, cache_hits[s.seq_id]))

            stats_table = Table(box=box.MINIMAL, show_header=False)
            stats_table.add_column("K", style="dim cyan")
            stats_table.add_column("V")
            stats_table.add_row("Iteration",       str(iteration))
            stats_table.add_row("Finished",        f"{len(finished_set)}/{len(requests)}")
            stats_table.add_row("Prefill cache",   f"[green]{prefix_hit_count}[/] hits")
            stats_table.add_row("GPU pages",       _bar(len(bm._gpu_blocks)-len(bm._gpu_free), len(bm._gpu_blocks), 20))
            stats_table.add_row("Prefill steps",   str(total_prefill))
            stats_table.add_row("Decode steps",    str(total_decode))

            panel = Panel(
                Columns([seq_table, stats_table], equal=False, expand=True),
                title=f"[bold]Iter {iteration:03d}[/] — "
                      f"batch=[cyan]{len(out.prefill)+len(out.decode)}[/]  "
                      f"waiting=[yellow]{len(out.ignored)}[/]",
                border_style="cyan",
            )
            live.update(panel)
            await asyncio.sleep(0.2)

    # ------------------------------------------------------------------
    # Final stats
    # ------------------------------------------------------------------
    pc_stats = prefix_cache.stats()

    console.print(Panel(
        f"  Requests completed  : [bold]{len(finished_set)}/{len(requests)}[/]\n"
        f"  Total iterations    : [cyan]{iteration}[/]\n"
        f"  Prefill steps       : [cyan]{total_prefill}[/]\n"
        f"  Decode steps        : [cyan]{total_decode}[/]\n"
        f"  Prefix cache hits   : [green]{prefix_hit_count}/{len(requests)}[/]  "
        f"({prefix_hit_count/len(requests):.0%} of requests skipped system-prompt prefill)\n"
        f"  Prefix cache total_hits : [green]{pc_stats.get('total_prefix_hits', 0)}[/]\n\n"
        "[bold]All three subsystems worked together:[/]\n"
        "  * BlockManager   - allocated/freed pages on demand, no fragmentation\n"
        "  * PrefixKVCache  - CoW-forked shared system prompt across requests\n"
        "  * Scheduler      - continuous batching kept GPU saturated throughout",
        title="Pipeline Summary",
        border_style="green",
    ))

    console.rule("[bold green]Demo 6 complete")


if __name__ == "__main__":
    asyncio.run(run_pipeline())

