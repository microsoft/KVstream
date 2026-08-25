"""Enable `python -m kvstream` to invoke the CLI."""

from __future__ import annotations

from kvstream.cli import app

if __name__ == "__main__":
    app()
