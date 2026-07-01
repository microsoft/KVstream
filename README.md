# KVStream

**KVStream sits in front of your existing LLM runtime and fixes the biggest bottleneck in local inference: wasted KV cache memory and poor batching. By streaming KV allocation instead of reserving it upfront, KVStream enables paged attention, continuous batching, and significantly more concurrent requests on the same GPU.**

Works with Ollama, Foundry Local, llama.cpp, and LM Studio — no model changes required.

[![CI](https://github.com/microsoft/KVstream/actions/workflows/ci.yml/badge.svg)](https://github.com/microsoft/KVstream/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/kvstream.svg)](https://pypi.org/project/kvstream/)

---

## Why KVStream?

Local inference runtimes like **Ollama** and **Foundry Local** allocate KV cache memory **contiguously per sequence** — reserving the full `max_seq_len` upfront. This causes:

| Problem | Effect |
|---|---|
| **Internal fragmentation** | Unused VRAM inside each sequence's reserved block |
| **Low concurrency** | Pool fills fast; only 1–3 parallel requests on 24 GB GPU |
| **Head-of-line blocking** | Long prompts stall all other requests |

KVStream sits in front of your existing runtime as a **transparent OpenAI-compatible proxy** and implements:

| Feature | What it does |
|---|---|
| **Streaming KV allocation** | Fixed-size KV pages allocated on demand, one slot per generated token — nothing reserved upfront |
| **Continuous batching** | Iteration-level scheduler: requests join the batch the moment a slot frees, instead of overloading the runtime |
| **Prefix KV cache** | Shared system prompts registered once and forked copy-on-write across requests |
| **Swap & preemption** | Low-priority sequences spilled to a CPU page pool, not dropped |
| **Hard KV inject** (llama.cpp) | Binary KV state save/restore via the `/slots` API |

How far the concurrency gain goes depends on your GPU, model, and prompt mix — measure it on your own hardware with `kvstream bench` (see [Benchmarks](#benchmarks)).

---

## Architecture

```mermaid
flowchart TD
    App["Your Application<br/>(any OpenAI-compatible client)"]
    App -->|"OpenAI-compatible API"| Proxy

    subgraph Proxy["KVStream Proxy :8080"]
        direction TB
        Sched["Continuous Batch<br/>Scheduler"]
        Block["Block Manager<br/>(page alloc)"]
        Prefix["Prefix KV Cache<br/>(dedup)"]
        Pool["KV Page Pool (GPU)<br/>+ CPU Swap Buffer"]
        Sched --> Pool
        Block --> Pool
        Prefix --> Pool
    end

    Proxy --> Ollama["Ollama<br/>:11434"]
    Proxy --> Foundry["Foundry Local"]
    Proxy --> Llama["llama.cpp / LM Studio"]
```

### Request lifecycle

1. A request arrives and is **queued by the continuous-batch scheduler** — it is admitted only when a batch slot and KV pages are available, so the backend runtime is never overloaded.
2. On admission, KV pages are allocated for the prompt; from then on the page table **grows one slot per generated token** (streaming allocation) instead of reserving `max_tokens` upfront.
3. If the prompt shares a prefix with an earlier request, the cached prefix's block table is **forked copy-on-write** — shared pages are never duplicated.
4. When the request finishes, its prefix is registered for reuse and its pages return to the pool — the next queued request joins the batch on the scheduler's next tick (10 ms).

> **Soft vs hard KV inject:** for Ollama, Foundry Local, and LM Studio the runtime owns its internal KV tensors; KVStream's page pool governs *admission and concurrency accounting* in front of it (soft inject). Only llama.cpp's `/slots` API allows true binary KV state save/restore (hard inject). See [Supported Backends](#supported-backends).

---

## Quick Start

KVStream is available on [PyPI](https://pypi.org/project/kvstream/) and can be installed with:

```bash
pip install kvstream
```

### Option 1 — Docker (recommended, zero Python setup)

```bash
git clone https://github.com/microsoft/kvstream
cd kvstream

# Start with Ollama (default) — pulls llama3.2 automatically
docker compose up -d

# Your app now talks to KVStream on :8080
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3.2",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

Switch backends via env var:
```bash
KVSTREAM_BACKEND=llamacpp KVSTREAM_BACKEND_URL=http://localhost:8080 docker compose up -d
KVSTREAM_BACKEND=foundry  KVSTREAM_BACKEND_URL=http://localhost:5273 docker compose up -d
KVSTREAM_BACKEND=lmstudio KVSTREAM_BACKEND_URL=http://localhost:1234 docker compose up -d
```

### Option 2 — CLI

```bash
# From a clone of this repo (a PyPI release is planned):
pip install -e .

# Reproducible install with fully pinned versions (CI / production):
#   pip install -r requirements.txt && pip install -e . --no-deps

# Auto-detect Ollama and start proxy on :8080
kvstream serve --backend ollama --port 8080 --gpu-blocks 2048

# With llama.cpp — run llama-server on :8081 so it doesn't clash with the
# proxy's :8080 (full KV inject — zero recompute on cache hits)
kvstream serve --backend llamacpp --backend-url http://localhost:8081 --port 8080

# Monitor live stats
kvstream status --watch

# Run a throughput benchmark
kvstream bench --concurrency 16 --prompt-len 512 --output-len 128
```

### Option 3 — Python library

```bash
# From a clone of this repo (a PyPI release is planned):
pip install -e .
```

```python
import asyncio
from kvstream import KVStreamEngine
from kvstream.backends import OllamaBackend

engine = KVStreamEngine(
    backend=OllamaBackend(base_url="http://localhost:11434", model="llama3.2"),
    num_gpu_blocks=2048,   # tune to your VRAM
    block_size=16,
    max_batch_size=16,
)

# Use as an async generator
async def main():
    async for token in engine.generate("Explain quantum entanglement simply:"):
        print(token.text, end="", flush=True)

asyncio.run(main())

# Or serve as an OpenAI-compatible proxy
asyncio.run(engine.serve(port=8080))
```

Drop-in replacement for the OpenAI Python client — point it at KVStream:
```python
import openai
client = openai.AsyncOpenAI(base_url="http://localhost:8080/v1", api_key="none")
```

---

## Configuration

`kvstream.yaml` (place in working directory, or pass `--config path/to/file.yaml`):

```yaml
backend:
  type: ollama                   # ollama | foundry | llamacpp | lmstudio
  base_url: http://localhost:11434
  model: llama3.2

memory:
  num_gpu_blocks: 2048           # each page = block_size × heads × head_dim × 2 × dtype_bytes
  num_cpu_blocks: 8192           # swap buffer — larger = safer under preemption
  block_size: 16                 # tokens per page (power of two: 8 / 16 / 32)
  dtype: float16

scheduler:
  max_batch_size: 16
  preemption_policy: swap        # swap (pages to CPU) | recompute (drop & redo)
  priority: fcfs                 # fcfs | sjf

prefix_cache:
  enabled: true
  max_prefix_length: 2048        # tokens
  ttl_seconds: 3600

attention:
  backend: naive                 # naive (reference); flash/xformers are roadmap
```

All fields also settable as environment variables: `KVSTREAM_BACKEND__TYPE=llamacpp`, `KVSTREAM_MEMORY__NUM_GPU_BLOCKS=4096`, etc.

---

## Supported Backends

| Backend | Status | Hard KV inject | Notes |
|---|---|---|---|
| **Ollama** | ✅ Stable | Soft | Uses llama.cpp slot keepalive |
| **Foundry Local** | ✅ Stable | Soft | OpenAI-compat passthrough |
| **llama.cpp server** | ✅ Stable | ✅ Hard | `/slots` save/restore API — true zero-recompute |
| **LM Studio** | 🔶 Beta | Soft | OpenAI-compat passthrough |

**Soft KV inject**: KVStream manages the page table and deduplicates token prefixes. The backend still recomputes on a cache miss, but shared prefixes are sent only once.

**Hard KV inject (llama.cpp only)**: binary KV tensors are saved after a prefix is processed and byte-copied back for every subsequent request — true zero-recompute. Start llama.cpp with `--slots 8 --cont-batching` to enable.

---

## Benchmarks

KVStream ships with a built-in load generator so you can validate the
concurrency claim **on your own hardware** — numbers vary with GPU, model,
quantisation, and prompt mix, so we don't publish a fixed table.

```bash
# 1. Baseline: point bench directly at your runtime (e.g. Ollama :11434)
kvstream bench --url http://localhost:11434 --concurrency 16 --total-requests 50

# 2. With KVStream in front
kvstream serve --backend ollama --port 8080
kvstream bench --url http://localhost:8080 --concurrency 16 --total-requests 50
```

The gain comes from **admission control + paged accounting**: instead of
letting 16 concurrent requests overwhelm a runtime that degrades past ~4,
KVStream batches them at the runtime's sweet spot, dedupes shared prefixes,
and queues the rest — so all 16 complete instead of timing out or OOMing.
Compare p50/p99 latency and the error count between the two runs.

---

## Observability

```bash
# KVStream exposes Prometheus metrics at http://localhost:8080/metrics
# Start the full stack including Prometheus + Grafana
docker compose --profile metrics up -d
# Prometheus UI at http://localhost:9090
# Grafana at http://localhost:3000 (default admin password "admin" — change it,
# see the Security Considerations section)

# Live CLI dashboard
kvstream status --watch

# Health endpoint
curl http://localhost:8080/health

# Scheduler + memory snapshot
curl http://localhost:8080/status | jq
```

---

## Adding a Backend

```python
# kvstream/backends/my_backend.py
from kvstream.backends.base import BaseBackend, GenerateRequest, Token
from typing import AsyncIterator

class MyBackend(BaseBackend):
    async def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        # stream tokens from your runtime
        yield Token(text="Hello", token_id=1)

    async def health(self) -> bool:
        return True
```

Register it in `kvstream/backends/__init__.py`, add a CLI option in `kvstream/cli/main.py`, and open a PR. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full checklist.

---

## Security Considerations

KVStream is designed to run as a **trusted, local inference proxy**. Review the
following before deploying it anywhere beyond `localhost`:

- **No built-in authentication.** None of the HTTP endpoints (`/v1/chat/completions`,
  `/status`, `/metrics`, `/health`) require credentials. The server binds to
  `127.0.0.1` by default. Do **not** bind to `0.0.0.0` or publish the port on an
  untrusted network without placing an authenticating reverse proxy (e.g. nginx,
  Caddy, or an API gateway) in front of it.
- **`docker compose` publishes port 8080.** The container listens on `0.0.0.0`
  inside the Docker network and the port is mapped to the host. Restrict access
  with host firewall rules or a reverse proxy if the host is reachable by others.
- **`/status` and `/metrics` disclose operational data** (batch sizes, memory
  utilisation, model name). Treat them as internal and do not expose them publicly.
- **Grafana / Prometheus (`--profile metrics`) are for local development.** The
  Grafana defaults can be overridden with `KVSTREAM_GRAFANA_PASSWORD` and
  `KVSTREAM_GRAFANA_ANONYMOUS`; anonymous access is disabled by default. Set a
  strong password before exposing the dashboard.
- **The Foundry Local backend discovers the runtime by probing localhost ports.**
  It only scans the local machine and never makes outbound network connections.
- Report security issues per [SECURITY.md](SECURITY.md) — do not open public issues
  for vulnerabilities.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). We welcome backend adapters, kernel optimisations, benchmarks on new hardware, and documentation improvements.

## License

[Apache 2.0](LICENSE) — use freely in commercial and private deployments.
