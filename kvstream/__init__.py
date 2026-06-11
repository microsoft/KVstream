"""
KVStream — PagedAttention & continuous batching for on-premise LLM inference.

Usage:
    from kvstream import KVaultEngine
    from kvstream.backends import OllamaBackend

    engine = KVaultEngine(
        backend=OllamaBackend(base_url="http://localhost:11434"),
        num_gpu_blocks=2048,
        block_size=16,
    )
    await engine.serve(port=8080)
"""

from kvstream.engine import KVaultEngine
from kvstream.config import KVaultConfig
from kvstream.version import __version__

__all__ = ["KVaultEngine", "KVaultConfig", "__version__"]
