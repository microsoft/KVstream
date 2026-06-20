# KVStream — Integration Guide

KVStream is a middleware layer that sits in front of any local LLM runtime (Ollama, Foundry Local, llama.cpp, LM Studio) and adds **paged KV-cache allocation**, **continuous batching**, and **prefix deduplication** without requiring changes to the backend or the client.

```
Your app  ──► KVStream proxy (port 8080)  ──►  Ollama / Foundry Local / llama.cpp
               OpenAI-compatible API              unchanged backend
```

---

## Table of Contents

1. [Installation](#1-installation)
2. [Quick Start — CLI proxy](#2-quick-start--cli-proxy)
3. [Quick Start — Python library](#3-quick-start--python-library)
4. [Connecting existing clients](#4-connecting-existing-clients)
5. [Backend configuration](#5-backend-configuration)
6. [Configuration reference](#6-configuration-reference)
7. [Memory tuning](#7-memory-tuning)
8. [Prefix cache](#8-prefix-cache)
9. [Monitoring](#9-monitoring)
10. [Docker deployment](#10-docker-deployment)
11. [Writing a custom backend](#11-writing-a-custom-backend)
12. [Benchmarking](#12-benchmarking)

---

## 1. Installation

```bash
pip install kvstream
```

### Optional extras

| Extra | What it adds | When to install |
|-------|-------------|-----------------|
| `kvstream[gpu]` | PyTorch + NumPy | llama.cpp hard-inject mode (GPU tensor pool) |
| `kvstream[flash]` | flash-attn | Faster attention kernel for hard-inject |
| `kvstream[xformers]` | xformers | Alternative efficient attention |
| `kvstream[dev]` | pytest, ruff, mypy | Development / contributing |

```bash
# With GPU tensor pool (llama.cpp hard-inject)
pip install "kvstream[gpu]"

# Full install — everything
pip install "kvstream[gpu,flash]"
```

---

## 2. Quick Start — CLI proxy

Start KVStream as a one-command proxy in front of your running LLM backend.

### Ollama

```bash
# 1. Start Ollama (if not already running)
ollama serve
ollama pull llama3.2

# 2. Start KVStream
kvstream serve --backend ollama --model llama3.2 --port 8080
```

### Foundry Local

```bash
# 1. Start Foundry Local
foundrylocal serve

# 2. Start KVStream (auto-discovers the Foundry port)
kvstream serve --backend foundry --model phi-3-mini --port 8080
```

### llama.cpp server

```bash
# 1. Start llama.cpp with slot support and continuous batching
./llama-server -m model.gguf --slots 8 --cont-batching --port 8081

# 2. Start KVStream
kvstream serve --backend llamacpp --backend-url http://localhost:8081 --port 8080
```

### LM Studio

```bash
# 1. Enable the LM Studio local server in the UI (default port 1234)

# 2. Start KVStream
kvstream serve --backend lmstudio --model local-model --port 8080
```

### Full CLI options

```
kvstream serve [OPTIONS]

  --backend         TEXT     ollama | foundry | llamacpp | lmstudio  [default: foundry]
  --backend-url     TEXT     Override backend base URL
  --model           TEXT     Model name
  --port            INT      Proxy listen port                        [default: 8080]
  --host            TEXT     Bind address (127.0.0.1 = loopback)     [default: 127.0.0.1]
  --gpu-blocks      INT      GPU KV page blocks                       [default: 256]
  --cpu-blocks      INT      CPU swap page blocks                     [default: 512]
  --block-size      INT      Tokens per page (power of 2)             [default: 16]
  --max-batch       INT      Max concurrent sequences                 [default: 8]
  --config          PATH     Path to kvstream.yaml
  --no-prefix-cache FLAG     Disable prefix deduplication
  --log-level       TEXT     DEBUG | INFO | WARNING | ERROR           [default: INFO]
```

---

## 3. Quick Start — Python library

Use KVStream directly in Python without the CLI, embedding it into your own service.

### Minimal example (Ollama)

```python
import asyncio
from kvstream import KVStreamEngine
from kvstream.backends import OllamaBackend

async def main():
    engine = KVStreamEngine(
        backend=OllamaBackend(base_url="http://localhost:11434", model="llama3.2"),
        num_gpu_blocks=256,
        num_cpu_blocks=512,
    )
    # Start the OpenAI-compatible proxy
    await engine.serve(port=8080)

asyncio.run(main())
```

### Streaming tokens directly

Skip the HTTP proxy and consume tokens in-process:

```python
import asyncio
from kvstream import KVStreamEngine
from kvstream.backends import OllamaBackend

async def main():
    engine = KVStreamEngine(
        backend=OllamaBackend(model="llama3.2"),
        num_gpu_blocks=256,
    )

    async for token in engine.generate(
        prompt="Explain paged attention in simple terms.",
        max_new_tokens=256,
        temperature=0.7,
    ):
        print(token.text, end="", flush=True)
        if token.finish_reason:
            print()  # newline at end

asyncio.run(main())
```

### Loading configuration from YAML

```python
import asyncio
from kvstream import KVStreamEngine, KVStreamConfig
from kvstream.backends import FoundryBackend

async def main():
    # Reads kvstream.yaml in the cwd, then env vars, then built-in defaults.
    config = KVStreamConfig.auto()

    engine = KVStreamEngine(
        backend=FoundryBackend(model=config.backend.model),
        config=config,
        # Explicit kwargs only override if you pass them; omitting them
        # lets the config file values win.
    )
    await engine.serve()

asyncio.run(main())
```

### Programmatic config (no YAML)

```python
from kvstream import KVStreamEngine, KVStreamConfig
from kvstream.config import (
    BackendConfig, MemoryConfig, SchedulerConfig,
    PrefixCacheConfig, BackendType,
)
from kvstream.backends import OllamaBackend

config = KVStreamConfig(
    host="0.0.0.0",
    port=8080,
    backend=BackendConfig(
        type=BackendType.OLLAMA,
        base_url="http://localhost:11434",
        model="llama3.2",
    ),
    memory=MemoryConfig(
        num_gpu_blocks=512,   # ~4 GB for phi-3-mini
        num_cpu_blocks=1024,
        block_size=16,
    ),
    scheduler=SchedulerConfig(
        max_batch_size=16,
        priority="fcfs",          # or "sjf"
        preemption_policy="swap", # or "recompute"
        max_queue_depth=500,
    ),
    prefix_cache=PrefixCacheConfig(
        enabled=True,
        ttl_seconds=3600,
        min_match_tokens=16,
    ),
)

engine = KVStreamEngine(backend=OllamaBackend(), config=config)
```

---

## 4. Connecting existing clients

Once the proxy is running on `http://localhost:8080`, any OpenAI-compatible client works without modification — just change the `base_url`.

### openai-python SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-required",  # KVStream has no auth layer
)

# Non-streaming
response = client.chat.completions.create(
    model="llama3.2",  # must match the model loaded in your backend
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is paged attention?"},
    ],
    max_tokens=512,
)
print(response.choices[0].message.content)

# Streaming
with client.chat.completions.create(
    model="llama3.2",
    messages=[{"role": "user", "content": "Tell me a short story."}],
    max_tokens=256,
    stream=True,
) as stream:
    for chunk in stream:
        delta = chunk.choices[0].delta.content or ""
        print(delta, end="", flush=True)
```

### openai AsyncClient

```python
import asyncio
from openai import AsyncOpenAI

client = AsyncOpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-required",
)

async def ask(question: str) -> str:
    response = await client.chat.completions.create(
        model="llama3.2",
        messages=[{"role": "user", "content": question}],
        max_tokens=512,
    )
    return response.choices[0].message.content

async def main():
    # Fire multiple requests concurrently — KVStream batches them automatically
    questions = [
        "What is CUDA?",
        "Explain transformer architecture.",
        "What is a KV cache?",
        "How does beam search work?",
    ]
    answers = await asyncio.gather(*[ask(q) for q in questions])
    for q, a in zip(questions, answers):
        print(f"Q: {q}\nA: {a[:100]}...\n")

asyncio.run(main())
```

### httpx (direct HTTP)

```python
import httpx
import json

# Non-streaming
with httpx.Client(base_url="http://localhost:8080") as client:
    response = client.post(
        "/v1/chat/completions",
        json={
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "Hello!"}],
            "max_tokens": 128,
        },
    )
    data = response.json()
    print(data["choices"][0]["message"]["content"])

# Streaming with httpx
with httpx.Client(base_url="http://localhost:8080") as client:
    with client.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "Count to 5."}],
            "max_tokens": 64,
            "stream": True,
        },
    ) as resp:
        for line in resp.iter_lines():
            if line.startswith("data: ") and line != "data: [DONE]":
                chunk = json.loads(line[6:])
                delta = chunk["choices"][0]["delta"].get("content", "")
                print(delta, end="", flush=True)
```

### LangChain

```python
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-required",
    model="llama3.2",
    streaming=True,
)

response = llm.invoke("What is KV cache paging?")
print(response.content)
```

### Checking health before use

```python
import httpx

def is_kvstream_ready(url: str = "http://localhost:8080") -> bool:
    try:
        r = httpx.get(f"{url}/health", timeout=3.0)
        data = r.json()
        return data.get("status") == "ok" and data.get("backend_healthy", False)
    except Exception:
        return False

if not is_kvstream_ready():
    raise RuntimeError("KVStream proxy is not ready or backend is unreachable.")
```

---

## 5. Backend configuration

### Ollama

```python
from kvstream.backends import OllamaBackend

backend = OllamaBackend(
    base_url="http://localhost:11434",
    model="llama3.2",          # any model pulled via `ollama pull`
    timeout=120.0,
    keep_alive="10m",          # how long Ollama keeps the model loaded
)
```

**Soft-inject backend** — KVStream manages logical page tables for admission control and prefix matching, while Ollama handles its own internal KV cache. The `PagedKVCache` tensor pool is not allocated.

### Foundry Local

```python
from kvstream.backends import FoundryBackend

backend = FoundryBackend(
    base_url="http://localhost:5273",  # auto-discovered if wrong
    model="phi-3-mini",
    timeout=120.0,
    exclude_ports=[8080],  # prevent KVStream from discovering its own port
)
```

**Auto-discovery**: Foundry Local uses an ephemeral OS-assigned port. If the configured URL does not respond, KVStream automatically scans all `LISTENING` ports on localhost to find the active Foundry service, preferring ports that have models loaded.

### llama.cpp server

```python
from kvstream.backends import LlamaCppBackend

backend = LlamaCppBackend(
    base_url="http://localhost:8081",
    num_slots=8,   # must match --slots on the llama-server command line
)
```

**Hard-inject backend** — llama.cpp exposes a `/slots` API that allows KVStream to save and restore raw KV state. This enables true zero-recompute prefix caching. Start the server with:

```bash
./llama-server -m model.gguf --slots 8 --cont-batching --port 8081
```

### LM Studio

```python
from kvstream.backends import LMStudioBackend

backend = LMStudioBackend(
    base_url="http://localhost:1234",
    model="local-model",
)
```

Enable the local server in LM Studio's UI first. LM Studio uses the same OpenAI-compatible API as Foundry Local.

---

## 6. Configuration reference

### kvstream.yaml

Place this file in your working directory. KVStream loads it automatically:

```yaml
backend:
  type: ollama                          # ollama | foundry | llamacpp | lmstudio
  base_url: http://localhost:11434
  model: llama3.2
  timeout_seconds: 120.0

memory:
  # Each block holds `block_size` token KV vectors per layer.
  # For phi-3-mini (32 layers, 32 heads, 128 dim, float16):
  #   1 block ≈ 16 × 2 × 32 × 32 × 128 × 2 bytes = 8 MB
  num_gpu_blocks: 256    # ~2 GB for phi-3-mini at 32 layers
  num_cpu_blocks: 512    # CPU swap buffer
  block_size: 16         # tokens per page — must be a power of 2
  dtype: float16

scheduler:
  max_batch_size: 8          # max sequences processed in one forward pass
  max_waiting_tokens: 4096   # total queued tokens before pressure signals
  preemption_policy: swap    # swap | recompute
  priority: fcfs             # fcfs (FIFO) | sjf (shortest job first)
  max_tokens_per_seq: 8192
  admission_timeout_seconds: 120.0
  max_queue_depth: 1000      # reject new requests when queue exceeds this

prefix_cache:
  enabled: true
  max_prefix_length: 2048    # tokens (must be a multiple of block_size)
  ttl_seconds: 3600
  min_match_tokens: 16       # minimum shared prefix to trigger a cache hit

observability:
  metrics_enabled: true
  log_level: INFO            # DEBUG | INFO | WARNING | ERROR
  trace_requests: false
```

### Environment variables

Every config field can be set via `KVSTREAM_` prefixed env vars, using `__` as the nesting delimiter:

```bash
export KVSTREAM_BACKEND__TYPE=ollama
export KVSTREAM_BACKEND__BASE_URL=http://localhost:11434
export KVSTREAM_BACKEND__MODEL=llama3.2
export KVSTREAM_MEMORY__NUM_GPU_BLOCKS=512
export KVSTREAM_MEMORY__BLOCK_SIZE=16
export KVSTREAM_SCHEDULER__MAX_BATCH_SIZE=16
export KVSTREAM_SCHEDULER__PRIORITY=sjf
export KVSTREAM_PREFIX_CACHE__ENABLED=true
export KVSTREAM_OBSERVABILITY__LOG_LEVEL=DEBUG
```

Priority order (highest → lowest): constructor kwargs → env vars → kvstream.yaml → built-in defaults.

---

## 7. Memory tuning

The GPU block pool is the most important tuning knob. Use these guidelines for soft-inject backends (Ollama, Foundry, LM Studio) where the pool controls admission concurrency — not actual VRAM:

| VRAM / RAM | `num_gpu_blocks` | Typical models |
|------------|-----------------|----------------|
| 4 GB | 64 | phi-3-mini, gemma-2b |
| 8 GB | 128 | llama3-8b, mistral-7b |
| 16 GB | 256 | llama3-8b (q8), codellama-13b |
| 24 GB | 512 | llama3-70b (q4), mixtral-8x7b |

For **llama.cpp hard-inject mode** the pool is backed by real tensors. Use the block size formula to estimate:

```python
# Memory per block (bytes):
bytes_per_block = block_size * 2 * num_layers * num_heads * head_dim * dtype_bytes

# Example: phi-3-mini, float16
# = 16 * 2 * 32 * 32 * 128 * 2 = 8,388,608 bytes ≈ 8 MB per block

# To stay within a 4 GB budget:
# max_blocks = 4 * 1024 * 1024 * 1024 / 8_388_608 ≈ 512 blocks
```

```python
from kvstream import KVStreamEngine, KVStreamConfig
from kvstream.config import MemoryConfig
from kvstream.backends import LlamaCppBackend

config = KVStreamConfig(
    memory=MemoryConfig(
        num_gpu_blocks=512,
        num_cpu_blocks=1024,   # swap buffer — pinned RAM on CUDA machines
        block_size=16,
        dtype="float16",
    )
)

engine = KVStreamEngine(
    backend=LlamaCppBackend(),
    config=config,
)
```

**Preemption policies:**

- `swap` *(default)*: when GPU blocks run out, the lowest-priority running sequence's pages are moved to the CPU swap buffer. It resumes when space is freed.
- `recompute`: pages are freed and the sequence is re-queued from the start. Lower memory overhead but higher latency for the preempted request.

---

## 8. Prefix cache

The prefix cache deduplicates the KV computation for any shared token prefix (system prompts, few-shot examples, RAG preambles).

### How it works

1. After a request's prefill phase completes, KVStream hashes the prompt tokens in block-aligned chunks and stores the canonical block table.
2. On the next request with the same prefix, the child sequence **forks** the canonical block table via copy-on-write — no re-computation.
3. Entries expire after `ttl_seconds`. Manual eviction is also possible.

### Maximising cache hits

```python
# Pattern: always put the system prompt first — it is the shared prefix.
messages = [
    {"role": "system", "content": SYSTEM_PROMPT},   # <-- shared, cached
    {"role": "user",   "content": user_question},   # <-- unique per request
]
```

```python
# With the openai client — the system prompt is cached after the first request
from openai import AsyncOpenAI
import asyncio

client = AsyncOpenAI(base_url="http://localhost:8080/v1", api_key="not-required")

SYSTEM = "You are an expert Python engineer. Answer concisely."

async def ask(q: str) -> str:
    r = await client.chat.completions.create(
        model="llama3.2",
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user",   "content": q},
        ],
        max_tokens=256,
    )
    return r.choices[0].message.content

async def main():
    # First call — prefix computed and cached
    print(await ask("What is a generator?"))
    # Subsequent calls — prefix is reused, faster time-to-first-token
    print(await ask("What is a context manager?"))
    print(await ask("What is a decorator?"))

asyncio.run(main())
```

### Checking cache statistics

```bash
curl http://localhost:8080/status
```

```json
{
  "prefix_cache": {
    "cached_prefixes": 12,
    "total_prefix_hits": 47,
    "cached_tokens": 4096
  }
}
```

```python
import httpx

stats = httpx.get("http://localhost:8080/status").json()
cache = stats["prefix_cache"]
print(f"Prefix cache hits: {cache['total_prefix_hits']} across {cache['cached_prefixes']} entries")
```

### Disabling the prefix cache

```bash
kvstream serve --backend ollama --no-prefix-cache
```

```python
from kvstream.config import PrefixCacheConfig
config = KVStreamConfig(prefix_cache=PrefixCacheConfig(enabled=False))
```

---

## 9. Monitoring

### /status endpoint

Returns live scheduler and memory statistics:

```bash
curl http://localhost:8080/status | python -m json.tool
```

```json
{
  "scheduler": {
    "waiting": 2,
    "running": 8,
    "swapped": 0,
    "gpu_blocks_free": 184,
    "gpu_utilization": 0.281
  },
  "memory": {
    "gpu_blocks_free": 184,
    "gpu_utilization": 0.281,
    "cpu_blocks_free": 512
  },
  "prefix_cache": {
    "cached_prefixes": 3,
    "total_prefix_hits": 12,
    "cached_tokens": 768
  }
}
```

### CLI live dashboard

```bash
# One-shot snapshot
kvstream status --url http://localhost:8080

# Live refresh every second
kvstream status --url http://localhost:8080 --watch
```

### Prometheus metrics

KVStream exposes a `/metrics` endpoint compatible with Prometheus scraping:

```bash
curl http://localhost:8080/metrics
```

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: kvstream
    static_configs:
      - targets: ["localhost:8080"]
    metrics_path: /metrics
```

### Grafana

The Docker Compose stack (see [Section 10](#10-docker-deployment)) includes a pre-configured Grafana + Prometheus stack. Navigate to `http://localhost:3000` (default login: `admin` / `admin`).

---

## 10. Docker deployment

### Basic CPU setup

```bash
# Clone and start (Ollama backend by default)
git clone https://github.com/microsoft/kvstream
cd kvstream
docker compose up -d

# Verify
curl http://localhost:8080/health
```

### With Prometheus + Grafana monitoring

```bash
docker compose --profile metrics up -d
# Grafana: http://localhost:3000  (admin/admin)
# Prometheus: http://localhost:9090
```

### GPU (NVIDIA)

Requires `nvidia-container-toolkit`:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

### Configuration via environment variables

```bash
# Use Foundry backend with a specific model and 512 GPU blocks
KVSTREAM_BACKEND=foundry \
KVSTREAM_MODEL=phi-3-mini \
KVSTREAM_GPU_BLOCKS=512 \
docker compose up -d
```

Available environment variables for the Docker stack:

| Variable | Default | Description |
|----------|---------|-------------|
| `KVSTREAM_BACKEND` | `ollama` | Backend type |
| `KVSTREAM_BACKEND_URL` | `http://ollama:11434` | Backend URL |
| `KVSTREAM_MODEL` | `llama3.2` | Model name |
| `KVSTREAM_PORT` | `8080` | Host port to expose |
| `KVSTREAM_GPU_BLOCKS` | `2048` | GPU KV pages |
| `KVSTREAM_CPU_BLOCKS` | `8192` | CPU swap pages |
| `KVSTREAM_MAX_BATCH` | `16` | Max concurrent sequences |

---

## 11. Writing a custom backend

To integrate KVStream with an unsupported LLM runtime, subclass `BaseBackend`:

```python
from collections.abc import AsyncIterator
import httpx
from kvstream.backends.base import BaseBackend, GenerateRequest, Token


class MyCustomBackend(BaseBackend):
    """Adapter for a hypothetical inference server."""

    def __init__(self, base_url: str = "http://localhost:9000", model: str = "my-model"):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client = httpx.AsyncClient(timeout=120.0)

    async def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        """Stream tokens for a request. This is the only required method."""
        payload = {
            "prompt": request.prompt,
            "max_tokens": request.max_new_tokens,
            "temperature": request.temperature,
            "top_p": request.top_p,
            "stream": True,
        }
        async with self._client.stream("POST", f"{self.base_url}/generate", json=payload) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                # Parse your server's streaming format here
                data = parse_my_format(line)
                yield Token(
                    text=data["text"],
                    token_id=data.get("token_id", 0),
                    finish_reason="stop" if data.get("done") else None,
                )
                if data.get("done"):
                    break

    async def health(self) -> bool:
        """Return True if the backend is reachable."""
        try:
            r = await self._client.get(f"{self.base_url}/health", timeout=3.0)
            return r.status_code == 200
        except Exception:
            return False

    async def tokenize(self, text: str) -> list[int]:
        """
        Optional but recommended — improves prefix cache accuracy and
        corrects token usage counts in /v1/chat/completions responses.
        Falls back to UTF-8 byte encoding if not implemented.
        """
        try:
            r = await self._client.post(
                f"{self.base_url}/tokenize",
                json={"text": text},
                timeout=5.0,
            )
            return r.json()["tokens"]
        except Exception:
            return list(text.encode("utf-8"))

    async def list_models(self) -> list[str]:
        """Optional — populates GET /v1/models."""
        try:
            r = await self._client.get(f"{self.base_url}/models")
            return [m["id"] for m in r.json()]
        except Exception:
            return [self.model]

    async def aclose(self) -> None:
        """Release the HTTP connection pool on shutdown."""
        await self._client.aclose()


def parse_my_format(line: str) -> dict:
    import json
    return json.loads(line)


# Usage
from kvstream import KVStreamEngine

engine = KVStreamEngine(
    backend=MyCustomBackend(base_url="http://localhost:9000"),
    num_gpu_blocks=256,
)
```

### Hard KV inject (advanced)

For backends that support saving and restoring raw KV state (like llama.cpp `/slots`), override `supports_hard_kv_inject`, `save_kv_state`, and `restore_kv_state`:

```python
class HardInjectBackend(BaseBackend):

    def supports_hard_kv_inject(self) -> bool:
        return True  # Enables PagedKVCache tensor pool allocation

    async def save_kv_state(self, seq_id: str, slot_id: int, path: str) -> bool:
        """Save KV state for a sequence slot to a file."""
        try:
            r = await self._client.post(
                f"{self.base_url}/slots/{slot_id}",
                json={"action": "save", "filename": path},
                timeout=30.0,
            )
            return r.status_code == 200
        except Exception:
            return False

    async def restore_kv_state(self, seq_id: str, slot_id: int, path: str) -> bool:
        """Restore a previously saved KV state."""
        try:
            r = await self._client.post(
                f"{self.base_url}/slots/{slot_id}",
                json={"action": "restore", "filename": path},
                timeout=30.0,
            )
            return r.status_code == 200
        except Exception:
            return False
```

---

## 12. Benchmarking

Use the built-in `bench` command against a running proxy:

```bash
kvstream bench \
  --url http://localhost:8080 \
  --model llama3.2 \
  --concurrency 8 \
  --prompt-len 128 \
  --output-len 64 \
  --total-requests 100
```

Example output:

```
┌──────────────────────────┐
│   KVStream Benchmark     │
├──────────────┬───────────┤
│ Requests     │ 100       │
│ Concurrency  │ 8         │
│ Errors       │ 0         │
│ Throughput   │ 12.4 req/s│
│ p50          │ 612 ms    │
│ p99          │ 1840 ms   │
└──────────────┴───────────┘
```

### Custom benchmark with the openai SDK

```python
import asyncio
import time
from openai import AsyncOpenAI

client = AsyncOpenAI(base_url="http://localhost:8080/v1", api_key="not-required")

PROMPT = "Explain transformer self-attention in detail. " * 5

async def one_request(sem: asyncio.Semaphore) -> float:
    async with sem:
        t0 = time.perf_counter()
        stream = await client.chat.completions.create(
            model="llama3.2",
            messages=[{"role": "user", "content": PROMPT}],
            max_tokens=128,
            stream=True,
        )
        async for _ in stream:
            pass
        return time.perf_counter() - t0

async def benchmark(concurrency: int = 8, total: int = 50):
    sem = asyncio.Semaphore(concurrency)
    latencies = await asyncio.gather(*[one_request(sem) for _ in range(total)])
    latencies = sorted(latencies)
    n = len(latencies)
    print(f"Throughput : {total / sum(latencies) * concurrency:.1f} req/s")
    print(f"p50        : {latencies[n // 2] * 1000:.0f} ms")
    print(f"p95        : {latencies[int(n * 0.95)] * 1000:.0f} ms")
    print(f"p99        : {latencies[min(int(n * 0.99), n-1)] * 1000:.0f} ms")

asyncio.run(benchmark(concurrency=8, total=50))
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `503 server overloaded` | Batch full, queue saturated | Increase `max_queue_depth`, `max_batch_size`, or reduce concurrency |
| `backend_healthy: false` | Backend not running | Start Ollama/Foundry; check `--backend-url` |
| High p99 latency | Preemption thrashing | Increase `num_gpu_blocks` or reduce `max_batch_size` |
| All tokens duplicated | Wrong chat template | Different models use different templates; `_messages_to_prompt` uses ChatML by default |
| `MemoryError: GPU block pool exhausted` | Pool too small for long sequences | Increase `num_gpu_blocks` or reduce `max_tokens_per_seq` |
| First request slow, subsequent fast | Prefix cache warming | Expected — cache primes on the first call |

```bash
# Quick health check
kvstream health --url http://localhost:8080

# Live stats (useful for diagnosing queue build-up)
kvstream status --url http://localhost:8080 --watch
```
