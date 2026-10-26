"""Uvicorn entrypoint for the gateway."""

from __future__ import annotations

import logging
import math
import socket

import uvicorn

from kvstream.app import Gateway, build_app
from kvstream.config import Settings


class GatewayServer(uvicorn.Server):
    """
    Uvicorn, with KVStream's drain started at the *beginning* of shutdown.

    Uvicorn's own order is: stop accepting, wait for every open connection
    (bounded by ``timeout_graceful_shutdown``), and only then run the lifespan
    shutdown. A request waiting for admission is an open connection, so with
    the drain in the lifespan hook it simply waited its turn and was served.
    Starting the drain here turns queued requests away at once with a 503, and
    ``drain_timeout_seconds`` becomes the graceful-shutdown timeout, so it
    bounds in-flight work the way the setting says it does.
    """

    def __init__(self, config: uvicorn.Config, gateway: Gateway) -> None:
        super().__init__(config)
        self._gateway = gateway

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        await self._gateway.begin_shutdown()
        await super().shutdown(sockets)


def serve(settings: Settings) -> None:
    logging.basicConfig(
        level=settings.observability.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = build_app(settings)
    config = uvicorn.Config(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.observability.log_level.lower(),
        # Uvicorn takes whole seconds; round up so a fractional setting is never
        # shortened to zero (which would cancel in-flight work immediately).
        timeout_graceful_shutdown=math.ceil(settings.drain_timeout_seconds),
    )
    GatewayServer(config, app.state.gateway).run()
