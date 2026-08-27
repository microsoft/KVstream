"""Uvicorn entrypoint for the gateway."""

from __future__ import annotations

import logging

import uvicorn

from kvstream.app import build_app
from kvstream.config import Settings


def serve(settings: Settings) -> None:
    logging.basicConfig(
        level=settings.observability.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = build_app(settings)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.observability.log_level.lower(),
    )
