"""
Demo 1 — PagedAttention Block Manager
======================================
Shows how KVStream manages GPU memory as a pool of fixed-size *pages* (blocks)
instead of reserving a contiguous chunk per sequence.

Key concepts demonstrated:
  • Allocating pages for a new sequence
  • Growing the page table as tokens are generated
  • Copy-on-write forking for prefix sharing
  • Freeing pages when a sequence finishes
  • GPU utilisation tracking

Run:
    python demo/01_block_manager.py
"""
from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from kvstream.memory.block_manager import BlockManager

console = Console()


def _block_table(bm: BlockManager, seq_id: str) -> Table:
    """Render the logical->physical block mapping for one sequence."""
    state = bm._seq_states.get(seq_id)
    t = Table(box=box.SIMPLE_HEAVY, show_header=True)
    t.add_column("Logical block", style="cyan", justify="center")
    t.add_column("Physical block ID", style="green", justify="center")
    t.add_column("Device", justify="center")
    t.add_column("Ref count", justify="center")
    if state:
        for logical, phys in enumerate(state.logical_to_physical):
            block = bm._gpu_blocks.get(phys) or bm._cpu_blocks.get(phys)
            t.add_row(
                str(logical),
                str(phys),
                block.device if block else "?",
                str(block.ref_count) if block else "?",
            )
    return t


def _pool_summary(bm: BlockManager) -> str:
    gpu_total = len(bm._gpu_blocks)
    gpu_free  = len(bm._gpu_free)
    cpu_free  = len(bm._cpu_free)
    util = bm.utilization()
    return (
        f"GPU  total={gpu_total}  free={gpu_free}  used={gpu_total - gpu_free}  "
        f"util=[bold yellow]{util:.1%}[/]  |  CPU free={cpu_free}"
    )


def main() -> None:
    console.rule("[bold blue]Demo 1 — PagedAttention Block Manager")

    # Small pool so state changes are easy to see
    GPU_BLOCKS  = 16
    CPU_BLOCKS  = 8
    BLOCK_SIZE  = 4   # 4 tokens per page

    bm = BlockManager(
        num_gpu_blocks=GPU_BLOCKS,
        num_cpu_blocks=CPU_BLOCKS,
        block_size=BLOCK_SIZE,
    )

    console.print(Panel(
        f"[bold]Configuration[/bold]\n"
        f"  block_size   = {BLOCK_SIZE} tokens/page\n"
        f"  GPU pages    = {GPU_BLOCKS}  ({GPU_BLOCKS * BLOCK_SIZE} tokens max on GPU)\n"
        f"  CPU pages    = {CPU_BLOCKS}  (swap buffer)\n\n"
        "Each page holds exactly [cyan]block_size[/] KV vectors.\n"
        "Sequences reference pages via a logical->physical table — no contiguous allocation needed.",
        title="Setup",
        border_style="blue",
    ))

    # ------------------------------------------------------------------
    # Step 1 — allocate two independent sequences
    # ------------------------------------------------------------------
    console.rule("[cyan]Step 1 — Allocate two sequences")
    bm.allocate("seq-A", num_tokens=7)   # needs ceil(7/4) = 2 pages
    bm.allocate("seq-B", num_tokens=10)  # needs ceil(10/4) = 3 pages

    console.print(f"\n[bold]Pool after allocating seq-A (7 tokens) and seq-B (10 tokens):[/]")
    console.print(_pool_summary(bm))
    console.print("\n[bold cyan]seq-A block table:[/]")
    console.print(_block_table(bm, "seq-A"))
    console.print("[bold cyan]seq-B block table:[/]")
    console.print(_block_table(bm, "seq-B"))

    # ------------------------------------------------------------------
    # Step 2 — grow seq-A as tokens are generated
    # ------------------------------------------------------------------
    console.rule("[cyan]Step 2 — Append slots as seq-A generates tokens")
    for i in range(5):
        phys_id, slot = bm.append_slot("seq-A")
        console.print(
            f"  token {i+1}: written to physical block [green]{phys_id}[/], "
            f"slot [yellow]{slot}[/] within page"
        )

    console.print(f"\n[bold]Pool after generating 5 tokens for seq-A:[/]")
    console.print(_pool_summary(bm))
    console.print("\n[bold cyan]seq-A block table (expanded):[/]")
    console.print(_block_table(bm, "seq-A"))

    # ------------------------------------------------------------------
    # Step 3 — prefix sharing via copy-on-write fork
    # ------------------------------------------------------------------
    console.rule("[cyan]Step 3 — Prefix sharing (copy-on-write fork)")
    console.print(
        "seq-C shares the same system prompt as seq-A.\n"
        "Instead of re-computing KV, we [bold green]fork[/] seq-A's block table.\n"
        "Both sequences point to the same physical pages — ref_count becomes 2.\n"
        "A private copy is made only when seq-C writes a new token (CoW).\n"
    )
    bm.fork("seq-A", "seq-C")

    console.print("[bold cyan]seq-A block table after fork:[/]")
    console.print(_block_table(bm, "seq-A"))
    console.print("[bold cyan]seq-C block table (forked from seq-A):[/]")
    console.print(_block_table(bm, "seq-C"))

    # ------------------------------------------------------------------
    # Step 4 — free a finished sequence
    # ------------------------------------------------------------------
    console.rule("[cyan]Step 4 — Free seq-B (request finished)")
    console.print(f"Before free: {_pool_summary(bm)}")
    bm.free("seq-B")
    console.print(f"After  free: {_pool_summary(bm)}")
    console.print(
        "\n[green]seq-B's pages are immediately returned to the free-list "
        "and can be used by the next incoming request.[/]"
    )

    # ------------------------------------------------------------------
    # Step 5 — memory pressure: show what happens when pool is full
    # ------------------------------------------------------------------
    console.rule("[cyan]Step 5 — Memory pressure")
    # Fill the remaining GPU pages
    filled = []
    idx = 0
    while bm.can_allocate(1, "gpu"):
        sid = f"seq-fill-{idx}"
        bm.allocate(sid, 1)
        filled.append(sid)
        idx += 1

    console.print(f"Filled GPU pool:  {_pool_summary(bm)}")
    result = bm.can_allocate(1, "gpu")
    console.print(
        f"\ncan_allocate(1 token, gpu) = [bold {'green' if result else 'red'}]{result}[/]\n"
        "[yellow]KVStream's scheduler detects this and preempts the lowest-priority sequence,\n"
        "swapping its pages to CPU to free GPU space.[/]"
    )

    console.rule("[bold green]Demo 1 complete")


if __name__ == "__main__":
    main()
