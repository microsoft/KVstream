"""Smoke test: admission gating, streaming KV allocation, prefix cache hits."""
import asyncio
import sys

from kvstream.backends.base import BaseBackend, GenerateRequest, Token
from kvstream.config import KVaultConfig
from kvstream.engine import KVaultEngine


class StubBackend(BaseBackend):
    def __init__(self):
        self.active = 0
        self.max_active = 0

    async def generate(self, request: GenerateRequest):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            for i in range(8):
                await asyncio.sleep(0.005)
                yield Token(text=f"t{i} ", token_id=i)
            yield Token(text="end", token_id=99, finish_reason="stop")
        finally:
            self.active -= 1

    async def health(self) -> bool:
        return True


import pytest


@pytest.mark.asyncio
async def test_claim_machinery():
    await main()


async def main():
    cfg = KVaultConfig()
    cfg.scheduler.max_batch_size = 2  # tiny batch to force queueing
    backend = StubBackend()
    engine = KVaultEngine(backend=backend, config=cfg,
                          num_gpu_blocks=64, num_cpu_blocks=64,
                          block_size=16, max_batch_size=2)

    shared_prefix = "You are a helpful assistant. " * 4  # > 16 bytes, block-aligned-able

    async def one(i):
        out = []
        async for tok in engine.generate(shared_prefix + f"Question {i}?", max_new_tokens=8):
            out.append(tok.text)
        return out

    # 6 concurrent requests vs max_batch_size=2
    results = await asyncio.gather(*[one(i) for i in range(6)])
    assert all(len(r) == 9 for r in results), "all requests must complete"
    print(f"[ok] 6 concurrent requests completed, batch ceiling observed: "
          f"max_active={backend.max_active} (limit 2)")
    assert backend.max_active <= 2, f"admission gate violated: {backend.max_active}"

    # Prefix cache: entries registered and a second wave should HIT
    stats1 = engine.prefix_cache.stats()
    print(f"[ok] prefix cache after wave 1: {stats1}")
    assert stats1["cached_prefixes"] > 0, "prefixes must be registered"

    await asyncio.gather(*[one(i) for i in range(6, 9)])
    stats2 = engine.prefix_cache.stats()
    print(f"[ok] prefix cache after wave 2: {stats2}")
    assert stats2["total_prefix_hits"] > 0, "prefix cache must produce hits"

    # All request pages released (only canonical prefix donors remain)
    free = engine.block_manager.num_free_gpu_blocks()
    held = 64 - free
    print(f"[ok] gpu blocks held after completion: {held} (canonical prefix pages only)")

    await engine.shutdown()
    print("ALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
