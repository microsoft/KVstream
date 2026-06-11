"""
Enables `py -3 -m kvstream` as an alias for the `kvstream` CLI entry point.
"""
from kvstream.cli.main import app

app()
