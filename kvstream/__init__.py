"""
KVStream — PagedAttention & continuous batching for on-premise LLM inference.

Usage:
    from kvstream import KVStreamEngine
    from kvstream.backends import OllamaBackend

    engine = KVStreamEngine(
        backend=OllamaBackend(base_url="http://localhost:11434"),
        num_gpu_blocks=2048,
        block_size=16,
    )
    await engine.serve(port=8080)
"""

from kvstream.config import KVStreamConfig
from kvstream.engine import KVStreamEngine
from kvstream.version import __version__

__all__ = ["KVStreamEngine", "KVStreamConfig", "__version__"]
