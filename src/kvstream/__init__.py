"""
KVStream — an OpenAI-compatible admission-control gateway (middleware) for
Microsoft Foundry Local.

KVStream is a **proxy**. It runs no model, owns no KV tensors, and has no
dependency on ONNX Runtime. It sits in front of a single Foundry Local instance
and keeps it responsive under concurrent (multi-agent) load by:

  * auto-discovering Foundry Local's ephemeral port,
  * admitting requests at a calibrated capacity (concurrency or KV-token budget),
  * queuing the overflow with clean backpressure (HTTP 503),
  * optionally caching and coalescing repetitive deterministic requests,
  * exposing real Prometheus metrics.

See the README for the honest scope and boundaries.
"""

from __future__ import annotations

from kvstream.config import Settings
from kvstream.version import __version__

__all__ = ["Settings", "__version__"]
