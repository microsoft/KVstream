"""
run_all_demos.py — run every offline demo in sequence.

Demos 01, 02, 03, 05, 06 are self-contained (no backend needed).
Demo 04 requires a running KVStream proxy + backend.

Usage:
    python demo/run_all_demos.py          # offline demos only
    python demo/run_all_demos.py --all    # include live inference demo
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import asyncio
import pathlib
import sys
import os

# Make sure the project root is on the path when running from the demo folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rich.console import Console
from rich.rule import Rule

console = Console()

OFFLINE_DEMOS = [
    ("01_block_manager",   "01_block_manager",   False),
    ("02_prefix_cache",    "02_prefix_cache",    False),
    ("03_scheduler",       "03_scheduler",       True),   # async
    ("05_memory_efficiency","05_memory_efficiency",False),
    ("06_full_pipeline",   "06_full_pipeline",   True),   # async
]

LIVE_DEMO = ("04_live_inference", "04_live_inference", False)


def run_demo(module_name: str, is_async: bool) -> None:
    spec_name = f"demo.{module_name}"
    try:
        mod = importlib.import_module(spec_name)
    except ImportError:
        # fallback: load relative path
        path = pathlib.Path(__file__).parent / f"{module_name}.py"
        spec = importlib.util.spec_from_file_location(module_name, path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

    if is_async:
        asyncio.run(mod.main() if hasattr(mod, "main") else mod.run_pipeline() if hasattr(mod, "run_pipeline") else mod.simulate())
    else:
        if hasattr(mod, "main"):
            mod.main()


def main() -> None:
    parser = argparse.ArgumentParser(description="KVStream demo runner")
    parser.add_argument("--all",  action="store_true", help="Also run the live inference demo (04)")
    parser.add_argument("--only", type=int,            help="Run only demo N (1-6)")
    args = parser.parse_args()

    demos = OFFLINE_DEMOS[:]
    if args.all:
        demos.append(LIVE_DEMO)

    if args.only:
        idx = args.only - 1
        all_demos = OFFLINE_DEMOS + [LIVE_DEMO]
        if 0 <= idx < len(all_demos):
            demos = [all_demos[idx]]
        else:
            console.print(f"[red]Demo {args.only} not found. Choose 1-6.[/]")
            return

    for _, module_name, is_async in demos:
        console.print(Rule(f"[bold blue]{module_name}"))
        try:
            run_demo(module_name, is_async)
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted — moving to next demo…[/]\n")
            continue
        except Exception as exc:
            console.print(f"[red]Demo {module_name} failed: {exc}[/]")
            import traceback; traceback.print_exc()
            continue
        console.print()

    console.print("[bold green]All demos finished.[/]")


if __name__ == "__main__":
    main()
