"""
Demo 5 — Memory Efficiency: Paged vs Contiguous Allocation
============================================================
Visualises *why* contiguous allocation wastes GPU memory and how paging
eliminates that waste.

Scenario:
  Serve 8 requests with unknown completion lengths (max_tokens=128).
  With contiguous allocation, VRAM must be reserved upfront for the worst
  case.  With paged allocation, pages are allocated on demand.

Run:
    python demo/05_memory_efficiency.py
"""
from __future__ import annotations

import random
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.columns import Columns
from rich.text import Text

console = Console()

# ---------------------------------------------------------------------------
# Simulation parameters
# ---------------------------------------------------------------------------

BLOCK_SIZE    = 16     # tokens per page
MAX_TOKENS    = 128    # worst-case reservation (contiguous)
GPU_BLOCKS    = 64     # total GPU pages available
GPU_VRAM_GB   = 8.0    # display only

BYTES_PER_TOKEN = 2 * 2 * 32 * 128  # 2 (key+value) × fp16 × 32 heads × 128 head_dim

REQUESTS = [
    {"id": "req-1",  "actual_tokens": 12},
    {"id": "req-2",  "actual_tokens": 67},
    {"id": "req-3",  "actual_tokens": 8},
    {"id": "req-4",  "actual_tokens": 103},
    {"id": "req-5",  "actual_tokens": 34},
    {"id": "req-6",  "actual_tokens": 55},
    {"id": "req-7",  "actual_tokens": 21},
    {"id": "req-8",  "actual_tokens": 91},
]


def _pages(tokens: int) -> int:
    return (tokens + BLOCK_SIZE - 1) // BLOCK_SIZE


def _mb(tokens: int) -> float:
    return tokens * BYTES_PER_TOKEN / (1024 ** 2)


def _bar(used: int, total: int, width: int = 40, color: str = "green") -> str:
    filled = int(used / total * width)
    empty  = width - filled
    return f"[{color}]{'#' * filled}[/][dim]{'.' * empty}[/]"


def main() -> None:
    console.rule("[bold blue]Demo 5 — Memory Efficiency: Paged vs Contiguous")

    # ------------------------------------------------------------------
    # Contiguous allocation analysis
    # ------------------------------------------------------------------
    console.rule("[red]Contiguous Allocation (naïve baseline)")

    cont_table = Table(box=box.ROUNDED, show_header=True)
    cont_table.add_column("Request",        width=10)
    cont_table.add_column("Actual tokens",  justify="right", width=15)
    cont_table.add_column("Reserved (max)", justify="right", width=16)
    cont_table.add_column("Wasted tokens",  justify="right", width=15)
    cont_table.add_column("VRAM wasted",    justify="right", width=14)

    cont_reserved_total = 0
    cont_actual_total   = 0
    cont_wasted_total   = 0

    for req in REQUESTS:
        actual   = req["actual_tokens"]
        reserved = MAX_TOKENS            # must reserve worst-case upfront
        wasted   = reserved - actual
        cont_reserved_total += reserved
        cont_actual_total   += actual
        cont_wasted_total   += wasted

        cont_table.add_row(
            req["id"],
            str(actual),
            f"[red]{reserved}[/]",
            f"[yellow]{wasted}[/]",
            f"[yellow]{_mb(wasted):.1f} MB[/]",
        )

    cont_table.add_row(
        "[bold]Total[/]",
        f"[bold]{cont_actual_total}[/]",
        f"[bold red]{cont_reserved_total}[/]",
        f"[bold yellow]{cont_wasted_total}[/]",
        f"[bold yellow]{_mb(cont_wasted_total):.1f} MB[/]",
    )
    console.print(cont_table)

    cont_util = cont_actual_total / cont_reserved_total
    console.print(
        f"\n  GPU utilisation  : [bold red]{cont_util:.1%}[/]\n"
        f"  Memory wasted    : [bold yellow]{_mb(cont_wasted_total):.1f} MB[/] "
        f"({1-cont_util:.1%} of reserved VRAM goes unused)\n"
        f"\n  {_bar(cont_actual_total, cont_reserved_total, color='red')} "
        f"{cont_util:.0%} efficient\n"
    )
    console.print(
        "[red]Problem:[/] With contiguous allocation you can only serve "
        f"[bold]{GPU_BLOCKS * BLOCK_SIZE // MAX_TOKENS}[/] concurrent requests "
        f"(GPU pages full at worst-case reservation)."
    )

    # ------------------------------------------------------------------
    # Paged allocation analysis
    # ------------------------------------------------------------------
    console.rule("[green]Paged Allocation (KVStream)")

    paged_table = Table(box=box.ROUNDED, show_header=True)
    paged_table.add_column("Request",       width=10)
    paged_table.add_column("Actual tokens", justify="right", width=15)
    paged_table.add_column("Pages used",    justify="right", width=12)
    paged_table.add_column("Page waste",    justify="right", width=12)
    paged_table.add_column("VRAM used",     justify="right", width=12)

    paged_actual_total    = 0
    paged_allocated_total = 0

    for req in REQUESTS:
        actual    = req["actual_tokens"]
        pages     = _pages(actual)
        allocated = pages * BLOCK_SIZE
        waste     = allocated - actual       # internal fragmentation only (< block_size)
        paged_actual_total    += actual
        paged_allocated_total += allocated

        paged_table.add_row(
            req["id"],
            str(actual),
            f"[green]{pages}[/]",
            str(waste),
            f"{_mb(allocated):.1f} MB",
        )

    paged_table.add_row(
        "[bold]Total[/]",
        f"[bold]{paged_actual_total}[/]",
        f"[bold green]{_pages(paged_actual_total)}[/]",
        f"[bold]{paged_allocated_total - paged_actual_total}[/]",
        f"[bold]{_mb(paged_allocated_total):.1f} MB[/]",
    )
    console.print(paged_table)

    paged_util = paged_actual_total / paged_allocated_total
    console.print(
        f"\n  GPU utilisation  : [bold green]{paged_util:.1%}[/]\n"
        f"  Max concurrency  : [bold green]{GPU_BLOCKS // _pages(paged_actual_total // len(REQUESTS))}[/] "
        f"requests (vs {GPU_BLOCKS * BLOCK_SIZE // MAX_TOKENS} contiguous)\n"
        f"\n  {_bar(paged_actual_total, paged_allocated_total, color='green')} "
        f"{paged_util:.0%} efficient\n"
    )

    # ------------------------------------------------------------------
    # Side-by-side comparison
    # ------------------------------------------------------------------
    console.rule("[bold]Comparison")

    saved_mb     = _mb(cont_wasted_total) - _mb(paged_allocated_total - paged_actual_total)
    extra_reqs   = (GPU_BLOCKS * BLOCK_SIZE - cont_reserved_total) // (_pages(paged_actual_total // len(REQUESTS)) * BLOCK_SIZE)

    console.print(Panel(
        f"                  [bold]Contiguous[/]           [bold]Paged (KVStream)[/]\n"
        f"  Utilisation   : [red]{cont_util:.1%}[/]                [green]{paged_util:.1%}[/]\n"
        f"  VRAM wasted   : [red]{_mb(cont_wasted_total):.1f} MB[/]            "
        f"[green]{_mb(paged_allocated_total-paged_actual_total):.1f} MB[/]   "
        f"([cyan]{saved_mb:.1f} MB saved[/])\n"
        f"  Max requests  : [red]{GPU_BLOCKS * BLOCK_SIZE // MAX_TOKENS}[/]                    "
        f"[green]~{GPU_BLOCKS // _pages(max(1, paged_actual_total // len(REQUESTS)))}[/]\n\n"
        "Paged allocation only wastes [italic]intra-block[/italic] fragments (<block_size tokens).\n"
        "The savings are amplified when actual sequence lengths are much shorter\n"
        "than the worst-case reservation — which is almost always true in practice.",
        title="Summary",
        border_style="cyan",
    ))

    # ------------------------------------------------------------------
    # Visual VRAM map
    # ------------------------------------------------------------------
    console.rule("[cyan]GPU VRAM Map (each cell = 1 page)")

    cont_pages_used  = (cont_reserved_total + BLOCK_SIZE - 1) // BLOCK_SIZE
    paged_pages_used = (paged_allocated_total + BLOCK_SIZE - 1) // BLOCK_SIZE

    def _vram_map(pages_used: int, total: int, label: str, color: str) -> Panel:
        cells = ""
        for i in range(total):
            if i < pages_used:
                cells += f"[{color}]#[/]"
            else:
                cells += "[dim].[/]"
            if (i + 1) % 16 == 0:
                cells += "\n"
        return Panel(cells, title=f"{label}  {pages_used}/{total} pages", border_style=color)

    console.print(Columns([
        _vram_map(cont_pages_used,  GPU_BLOCKS, "Contiguous", "red"),
        _vram_map(paged_pages_used, GPU_BLOCKS, "Paged (KVStream)", "green"),
    ]))

    console.rule("[bold green]Demo 5 complete")


if __name__ == "__main__":
    main()
