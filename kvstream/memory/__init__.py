from kvstream.memory.block_manager import BlockManager
from kvstream.memory.prefix_cache import PrefixKVCache

# PagedKVCache requires torch — imported lazily to allow CPU-only unit tests
__all__ = ["BlockManager", "PagedKVCache", "PrefixKVCache"]


def __getattr__(name: str):
    if name == "PagedKVCache":
        from kvstream.memory.kv_cache import PagedKVCache
        return PagedKVCache
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
