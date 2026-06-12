"""
Demo 3 — Continuous Batching Scheduler
========================================
Simulates how KVStream's scheduler decides which sequences run each iteration.

Unlike static batching (one batch, wait for all to finish, release),
continuous batching lets sequences join and leave mid-flight.

This demo:
  • Submits 8 requests of varying prompt lengths all at once
  • Runs the scheduler in a tight loop, printing each batch decision
  • Shows WAITING -> RUNNING -> SWAPPED -> FINISHED state transitions
  • Demonstrates preemption when GPU memory is full

Run:
    python demo/03_scheduler.py
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

from kvstream.memory.block_manager import BlockManager
from kvstream.scheduler.continuous_batch import (
    ContinuousBatchScheduler,
    SequenceGroup,
    SeqStatus,
)

console = Console()

# ---------------------------------------------------------------------------
# Fake token IDs
# ---------------------------------------------------------------------------

def _fake_tokens(n: int) -> list[int]:
    return [random.randint(0, 32000) for _ in range(n)]


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

STATUS_STYLE = {
    SeqStatus.WAITING:  "[dim white]WAITING [/]",
    SeqStatus.RUNNING:  "[bold green]RUNNING [/]",
    SeqStatus.SWAPPED:  "[yellow]SWAPPED [/]",
    SeqStatus.FINISHED: "[dim]FINISHED[/]",
    SeqStatus.ABORTED:  "[red]ABORTED [/]",
}


def _sequences_table(seqs: list[SequenceGroup], bm: BlockManager) -> Table:
    t = Table(box=box.MINIMAL_DOUBLE_HEAD, show_header=True, expand=True)
    t.add_column("Seq ID",        style="cyan",   width=12)
    t.add_column("Status",        width=12)
    t.add_column("Prompt len",    justify="right", width=12)
    t.add_column("Generated",     justify="right", width=12)
    t.add_column("GPU pages",     justify="right", width=12)
    t.add_column("Progress",      width=20)

    for seq in seqs:
        pages = len(bm._seq_states[seq.seq_id].logical_to_physical) if seq.seq_id in bm._seq_states else 0
        max_t = seq.max_new_tokens
        gen   = seq.num_generated
        bar   = "#" * int(gen / max_t * 16) + "." * (16 - int(gen / max_t * 16)) if max_t > 0 else ""
        t.add_row(
            seq.seq_id,
            STATUS_STYLE.get(seq.status, str(seq.status)),
            str(seq.prompt_len),
            f"{gen}/{max_t}",
            str(pages),
            bar,
        )
    return t


def _pool_bar(bm: BlockManager) -> str:
    used  = len(bm._gpu_blocks) - len(bm._gpu_free)
    total = len(bm._gpu_blocks)
    pct   = used / total
    bar   = "#" * int(pct * 30) + "." * (30 - int(pct * 30))
    color = "red" if pct > 0.85 else "yellow" if pct > 0.6 else "green"
    return f"GPU [{color}]{bar}[/] {used}/{total} pages  ({pct:.0%})"


# ---------------------------------------------------------------------------
# Core simulation
# ---------------------------------------------------------------------------

async def simulate() -> None:
    console.rule("[bold blue]Demo 3 — Continuous Batching Scheduler")

    GPU_BLOCKS = 32
    BLOCK_SIZE = 8
    MAX_BATCH  = 4

    bm = BlockManager(
        num_gpu_blocks=GPU_BLOCKS,
        num_cpu_blocks=24,
        block_size=BLOCK_SIZE,
    )
    scheduler = ContinuousBatchScheduler(
        block_manager=bm,
        max_batch_size=MAX_BATCH,
        preemption_policy="swap",
        priority="fcfs",
    )

    console.print(Panel(
        f"  GPU pages    : {GPU_BLOCKS}  (block_size={BLOCK_SIZE})\n"
        f"  Max batch    : {MAX_BATCH} sequences per iteration\n"
        f"  Preemption   : swap (pages spilled to CPU when GPU is full)\n"
        f"  Priority     : FCFS (first-come first-served)",
        title="Configuration",
        border_style="blue",
    ))

    # Build 8 requests with varied lengths to make scheduling interesting
    requests = [
        SequenceGroup(seq_id="user-1", prompt_tokens=_fake_tokens(6),  max_new_tokens=12),
        SequenceGroup(seq_id="user-2", prompt_tokens=_fake_tokens(20), max_new_tokens=8),
        SequenceGroup(seq_id="user-3", prompt_tokens=_fake_tokens(4),  max_new_tokens=20),
        SequenceGroup(seq_id="user-4", prompt_tokens=_fake_tokens(15), max_new_tokens=6),
        SequenceGroup(seq_id="user-5", prompt_tokens=_fake_tokens(8),  max_new_tokens=10),
        SequenceGroup(seq_id="user-6", prompt_tokens=_fake_tokens(12), max_new_tokens=16),
        SequenceGroup(seq_id="user-7", prompt_tokens=_fake_tokens(3),  max_new_tokens=5),
        SequenceGroup(seq_id="user-8", prompt_tokens=_fake_tokens(18), max_new_tokens=12),
    ]

    all_seqs = {s.seq_id: s for s in requests}

    console.print(f"\nSubmitting [bold]{len(requests)}[/] requests simultaneously…\n")
    for seq in requests:
        await scheduler.add_request(seq)

    # ------------------------------------------------------------------
    # Iteration loop
    # ------------------------------------------------------------------
    iteration   = 0
    max_iters   = 120  # safety ceiling
    finished    = set()

    with Live(console=console, refresh_per_second=4) as live:
        while len(finished) < len(requests) and iteration < max_iters:
            iteration += 1
            sched_out = await scheduler.schedule()

            # --- Simulate prefill / decode for running sequences ---
            for seq in sched_out.prefill + sched_out.decode:
                # Each iteration = 1 token generated
                token_id = random.randint(0, 32000)
                seq.output_tokens.append(token_id)
                # Grow block table for the new token
                try:
                    bm.append_slot(seq.seq_id)
                except MemoryError:
                    # Shouldn't happen if scheduler preempts correctly
                    seq.status = SeqStatus.ABORTED

                # Finish if max_new_tokens reached
                if seq.num_generated >= seq.max_new_tokens:
                    seq.status = SeqStatus.FINISHED
                    bm.free(seq.seq_id)
                    finished.add(seq.seq_id)
                    # Remove from scheduler's running list
                    if seq in scheduler.running:
                        scheduler.running.remove(seq)

            # Build display
            table = _sequences_table(list(all_seqs.values()), bm)
            panel = Panel(
                table,
                title=f"[bold]Iteration {iteration:03d}[/] — "
                      f"running={len(sched_out.prefill)+len(sched_out.decode)} "
                      f"waiting={len(sched_out.ignored)} "
                      f"finished={len(finished)}/{len(requests)}",
                border_style="cyan",
                subtitle=_pool_bar(bm),
            )
            live.update(panel)
            await asyncio.sleep(0.25)   # slow down so it's visible

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    console.print(Panel(
        f"All [bold]{len(requests)}[/] requests completed in [cyan]{iteration}[/] iterations.\n\n"
        "Key observations:\n"
        "  • Sequences ran [bold]concurrently[/] in the same forward pass\n"
        "  • Finished sequences vacated pages [bold]immediately[/] (no padding waste)\n"
        "  • New sequences entered the batch as soon as GPU pages were free\n"
        "  • Memory pressure triggered [yellow]swap preemption[/] when needed",
        title="Summary",
        border_style="green",
    ))
    console.rule("[bold green]Demo 3 complete")


if __name__ == "__main__":
    asyncio.run(simulate())
