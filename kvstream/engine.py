"""
KVaultEngine — top-level orchestrator.

Wires together:
  - BlockManager (page allocation)
  - PagedKVCache (tensor pool) — optional, requires torch
  - PrefixKVCache (prefix dedup)
  - ContinuousBatchScheduler (iteration-level batching)
  - Backend adapter (Ollama / Foundry / llama.cpp / LM Studio)
  - FastAPI proxy (OpenAI-compatible HTTP)

Usage:
    engine = KVaultEngine(
        backend=OllamaBackend(),
        num_gpu_blocks=512,
        block_size=16,
    )
    await engine.serve(port=8080)
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import AsyncIterator, Optional

import uvicorn

from kvstream.backends.base import BaseBackend, GenerateRequest, Token
from kvstream.config import KVaultConfig
from kvstream.memory.block_manager import BlockManager
from kvstream.memory.prefix_cache import PrefixKVCache
from kvstream.scheduler.continuous_batch import (
    ContinuousBatchScheduler,
    SequenceGroup,
    SeqStatus,
)

logger = logging.getLogger("kvstream.engine")


class KVaultEngine:
    def __init__(
        self,
        backend: BaseBackend,
        config: Optional[KVaultConfig] = None,
        # Convenience kwargs — these ALWAYS override whatever is in config
        num_gpu_blocks: int = 512,
        num_cpu_blocks: int = 1024,
        block_size: int = 16,
        max_batch_size: int = 8,
    ) -> None:
        self.backend = backend
        self.config = config or KVaultConfig()

        # CLI kwargs always win over whatever the config object holds
        self.config.memory.num_gpu_blocks = num_gpu_blocks
        self.config.memory.num_cpu_blocks = num_cpu_blocks
        self.config.memory.block_size = block_size
        self.config.scheduler.max_batch_size = max_batch_size

        logger.info(
            f"Engine init: gpu_blocks={num_gpu_blocks} cpu_blocks={num_cpu_blocks} "
            f"block_size={block_size} max_batch={max_batch_size}"
        )

        # --- Block manager (always created — no torch needed) ---
        self.block_manager = BlockManager(
            num_gpu_blocks=self.config.memory.num_gpu_blocks,
            num_cpu_blocks=self.config.memory.num_cpu_blocks,
            block_size=self.config.memory.block_size,
        )

        # --- KV tensor pool (optional — requires torch) ---
        # For passthrough backends (Ollama, Foundry, LM Studio) this pool is NOT
        # used for inference — the backend handles its own KV cache. We only
        # build it when torch is available AND the backend supports hard inject.
        self.kv_cache = None
        if backend.supports_hard_kv_inject():
            try:
                import torch  # noqa: F401 — presence check only
                from kvstream.memory.kv_cache import PagedKVCache
                self.kv_cache = PagedKVCache(
                    num_layers=self.config.attention.num_layers,
                    num_heads=self.config.attention.num_heads,
                    head_dim=self.config.attention.head_dim,
                    num_gpu_blocks=self.config.memory.num_gpu_blocks,
                    num_cpu_blocks=self.config.memory.num_cpu_blocks,
                    block_size=self.config.memory.block_size,
                )
                logger.info("PagedKVCache initialised (hard-inject mode)")
            except ModuleNotFoundError:
                logger.warning(
                    "torch not found — PagedKVCache disabled. "
                    "Proxy and soft-prefix cache still work. "
                    "Install torch to enable the GPU tensor pool."
                )
            except RuntimeError as e:
                logger.warning(
                    f"PagedKVCache allocation failed ({e}). "
                    "Try reducing --gpu-blocks / --cpu-blocks. "
                    "Falling back to soft-inject mode."
                )
        else:
            logger.info(
                f"Backend '{type(backend).__name__}' uses soft KV inject — "
                "skipping PagedKVCache allocation."
            )

        # --- Prefix cache ---
        self.prefix_cache = (
            PrefixKVCache(
                block_manager=self.block_manager,
                max_prefix_length=self.config.prefix_cache.max_prefix_length,
                ttl_seconds=self.config.prefix_cache.ttl_seconds,
                min_match_tokens=self.config.prefix_cache.min_match_tokens,
            )
            if self.config.prefix_cache.enabled
            else None
        )

        # --- Scheduler ---
        self.scheduler = ContinuousBatchScheduler(
            block_manager=self.block_manager,
            max_batch_size=self.config.scheduler.max_batch_size,
            max_waiting_tokens=self.config.scheduler.max_waiting_tokens,
            preemption_policy=self.config.scheduler.preemption_policy.value,
            priority=self.config.scheduler.priority.value,
        )

        self._running = False
        self._scheduler_task: Optional[asyncio.Task] = None
        self._eviction_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Public generation API
    # ------------------------------------------------------------------

    async def generate(
        self,
        prompt: str,
        max_new_tokens: int = 512,
        temperature: float = 0.8,
        top_p: float = 0.95,
        stop: Optional[list[str]] = None,
    ) -> AsyncIterator[Token]:
        seq_id = str(uuid.uuid4())
        prompt_tokens = await self.backend.tokenize(prompt)

        # Prefix cache lookup
        cached_prefix_tokens = 0
        if self.prefix_cache:
            match = self.prefix_cache.match(prompt_tokens)
            if match:
                cached_prefix_tokens = self.prefix_cache.fork_prefix(match, seq_id)
                logger.debug(
                    f"[{seq_id[:8]}] prefix cache HIT — "
                    f"skipping {cached_prefix_tokens} tokens"
                )

        seq = SequenceGroup(
            seq_id=seq_id,
            prompt_tokens=prompt_tokens,
            max_new_tokens=max_new_tokens,
        )
        await self.scheduler.add_request(seq)
        self._ensure_background_tasks()

        # --- Continuous-batching admission gate ---
        # The request joins the batch only when the scheduler grants it KV
        # pages. Until then it queues (instead of overloading the backend).
        timeout = self.config.scheduler.admission_timeout_seconds
        try:
            await asyncio.wait_for(seq.admission.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            await self.scheduler.abort_request(seq_id)
            raise RuntimeError(
                f"request {seq_id[:8]} timed out after {timeout:.0f}s waiting "
                f"for batch admission (running batch is saturated)"
            )

        request = GenerateRequest(
            seq_id=seq_id,
            prompt=prompt,
            prompt_tokens=prompt_tokens,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            top_p=top_p,
            stop=stop,
            cached_prefix_tokens=cached_prefix_tokens,
        )

        try:
            async for token in self.backend.generate(request):
                seq.output_tokens.append(token.token_id)
                # Streaming KV allocation: grow the page table one slot per
                # generated token instead of reserving max_new_tokens upfront.
                try:
                    self.block_manager.append_slot(seq_id)
                except MemoryError:
                    logger.warning(f"[{seq_id[:8]}] KV page pool saturated mid-decode")
                except KeyError:
                    pass  # sequence was preempted/aborted concurrently
                yield token
                if token.finish_reason:
                    break
        finally:
            if seq.status == SeqStatus.RUNNING:
                # Register the prefix BEFORE the pages are freed — register()
                # forks the block table into a canonical prefix sequence.
                if (
                    self.prefix_cache
                    and len(prompt_tokens) >= self.config.prefix_cache.min_match_tokens
                ):
                    self.prefix_cache.register(seq_id, prompt_tokens)
                # Scheduler frees the pages on its next tick and admits the
                # next waiting sequence into the freed slot.
                seq.status = SeqStatus.FINISHED
            else:
                await self.scheduler.abort_request(seq_id)

    # ------------------------------------------------------------------
    # HTTP server
    # ------------------------------------------------------------------

    async def serve(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        from kvstream.proxy.app import build_app

        app = build_app(self)
        server_host = host or self.config.host
        server_port = port or self.config.port

        logger.info(f"KVStream proxy on http://{server_host}:{server_port}")
        logger.info(
            f"Backend: {type(self.backend).__name__} @ "
            f"{getattr(self.backend, 'base_url', 'N/A')}"
        )
        logger.info(
            f"Memory: {self.config.memory.num_gpu_blocks} GPU blocks × "
            f"{self.config.memory.block_size} tokens/block | "
            f"KV tensor pool: {'enabled' if self.kv_cache else 'disabled (soft inject)'}"
        )

        config = uvicorn.Config(
            app,
            host=server_host,
            port=server_port,
            log_level=self.config.observability.log_level.lower(),
            access_log=self.config.observability.trace_requests,
        )
        server = uvicorn.Server(config)
        self._running = True
        await server.serve()

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------

    def _ensure_background_tasks(self) -> None:
        """Start the scheduler + eviction loops on the running event loop.
        Idempotent — safe to call per request."""
        self._running = True
        loop = asyncio.get_running_loop()
        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = loop.create_task(self._scheduler_loop())
        if self._eviction_task is None or self._eviction_task.done():
            self._eviction_task = loop.create_task(self._eviction_loop())

    async def shutdown(self) -> None:
        """Stop background loops and release resources."""
        self._running = False
        for task in (self._scheduler_task, self._eviction_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._scheduler_task = None
        self._eviction_task = None

    async def _scheduler_loop(self) -> None:
        """
        Iteration-level batching: every tick the scheduler frees finished
        sequences, swaps preempted ones back in, and admits waiting requests
        into the batch — sequences join and leave mid-flight.
        """
        while self._running:
            try:
                await self.scheduler.schedule()
            except Exception:
                logger.exception("scheduler iteration failed")
            busy = bool(
                self.scheduler.waiting or self.scheduler.running or self.scheduler.swapped
            )
            await asyncio.sleep(0.01 if busy else 0.1)

    async def _eviction_loop(self, interval_seconds: int = 60) -> None:
        while self._running:
            await asyncio.sleep(interval_seconds)
            if self.prefix_cache:
                evicted = self.prefix_cache.evict_expired()
                if evicted:
                    logger.debug(f"Evicted {evicted} expired prefix cache entries")
