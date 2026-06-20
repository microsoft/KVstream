# Changelog

All notable changes to KVStream are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] — 2026-06-20

### Added
- `BlockManager` — O(1) paged KV memory allocator with copy-on-write prefix sharing and GPU↔CPU swap.
- `PagedKVCache` — pre-allocated tensor slab (`[layers, 2, blocks, block_size, heads, head_dim]`) with pinned-memory CPU swap buffer.
- `PrefixKVCache` — rolling-hash prefix deduplication; block-aligned sub-prefix chain indexing.
- `ContinuousBatchScheduler` — iteration-level batching with FCFS/SJF priority, swap and recompute preemption policies.
- `KVStreamEngine` — top-level orchestrator wiring all components.
- Backend adapters: Ollama, Foundry Local (auto-discovers ephemeral port), llama.cpp (hard KV inject via `/slots`), LM Studio.
- OpenAI-compatible proxy (`/v1/chat/completions`, `/v1/models`, `/health`, `/metrics`, `/status`).
- Naive paged-attention reference kernel with graceful fallback from `flash-attn` → `xformers` → naive.
- CLI: `kvstream serve`, `kvstream bench`, `kvstream status`, `kvstream health`.
- Prometheus metrics endpoint.
- Docker Compose (CPU and GPU variants) with Grafana + Prometheus stack.
