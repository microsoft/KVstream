# KVStream — Integration Guide (Foundry Local)

KVStream is an OpenAI-compatible **HTTP proxy** that sits in front of [Microsoft Foundry Local](https://learn.microsoft.com/azure/ai-foundry/foundry-local/) and adds **admission-control queuing** and **automatic port discovery**, without any change to Foundry Local or to your client.

> **What KVStream does and does not do for Foundry Local.** It operates purely at the HTTP layer. It has **no access** to Foundry Local's memory, KV cache, or model weights, and it runs **no inference** of its own. It cannot make individual tokens generate faster and it does not reuse KV state. What it does is limit how many requests reach Foundry Local at once and queue the rest, so a burst of concurrent queries drains through in order instead of overwhelming the runtime. Foundry Local receives and recomputes the full prompt on every request.

```
Your app  ──►  KVStream proxy (:8080)  ──►  Foundry Local (ONNX Runtime)
               OpenAI-compatible API          auto-discovered port; unchanged
```

This guide covers Foundry Local only. The repository contains adapters for other runtimes (Ollama, llama.cpp, LM Studio), but they are unsupported and out of scope here.

---

## Table of Contents

1. [Installation](#1-installation)
2. [Quick Start — CLI proxy](#2-quick-start--cli-proxy)
3. [Quick Start — Python library](#3-quick-start--python-library)
4. [Connecting existing clients](#4-connecting-existing-clients)
5. [The Foundry Local backend & port discovery](#5-the-foundry-local-backend--port-discovery)
6. [Configuration reference](#6-configuration-reference)
7. [Concurrency tuning](#7-concurrency-tuning)
8. [Prefix cache (why it is off for Foundry)](#8-prefix-cache-why-it-is-off-for-foundry)
9. [Monitoring](#9-monitoring)
10. [Docker note](#10-docker-note)
11. [Benchmarking](#11-benchmarking)
12. [What is scaffolding, not a feature](#12-what-is-scaffolding-not-a-feature)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Installation

KVStream is on [PyPI](https://pypi.org/project/kvstream/):

```bash
pip install kvstream
```

Or from a clone (for development):

```bash
git clone https://github.com/microsoft/kvstream
cd kvstream
pip install -e .

# Reproducible install with fully pinned versions:
#   pip install -r requirements.txt && pip install -e . --no-deps
```

For the Foundry Local path you do **not** need `torch` or any GPU extra. KVStream never runs a forward pass, so the `kvstream[gpu]`, `kvstream[flash]`, and `kvstream[xformers]` extras are irrelevant here — installing them adds unused code. Only `kvstream[dev]` (pytest, ruff, mypy) is useful, for contributing.

---

## 2. Quick Start — CLI proxy

### 1. Start Foundry Local

Install and start Foundry Local and load a model per Microsoft's instructions
([Foundry Local docs](https://learn.microsoft.com/azure/ai-foundry/foundry-local/)).
Foundry Local exposes an OpenAI-compatible service on an **OS-assigned port that
changes between restarts** — you do not need to know it, KVStream discovers it.

### 2. Start KVStream

`serve` already defaults to the Foundry Local backend:

```bash
kvstream serve --backend foundry --port 8080 --max-batch 8
```

`--model` should match a model id returned by Foundry Local's `/v1/models`
(e.g. `phi-3-mini`, `qwen2.5-0.5b-instruct-generic-cpu`). If omitted, the
backend default (`phi-3-mini`) is used:

```bash
kvstream serve --backend foundry --model qwen2.5-0.5b-instruct-generic-cpu
```

### Full CLI options for `serve`

```
kvstream serve [OPTIONS]

  --backend         TEXT   foundry (this guide) | ollama | llamacpp | lmstudio  [default: foundry]
  --backend-url     TEXT   Override the starting URL guess (auto-discovered if wrong)
  --model           TEXT   Model id (backend default if omitted)
  --port            INT    Proxy listen port                                    [default: 8080]
  --host            TEXT   Bind address (127.0.0.1 = loopback; no auth layer)   [default: 127.0.0.1]
  --gpu-blocks      INT    Concurrency-accounting units (NOT VRAM — see §7)      [default: 256]
  --cpu-blocks      INT    Swap-accounting units (unused for Foundry — see §12)  [default: 512]
  --block-size      INT    Tokens per accounting page (power of 2)              [default: 16]
  --max-batch       INT    Max concurrent requests reaching Foundry Local        [default: 8]
  --config          PATH   Path to kvstream.yaml
  --no-prefix-cache FLAG   Disable prefix tracking (recommended for Foundry)
  --log-level       TEXT   DEBUG | INFO | WARNING | ERROR                        [default: INFO]
```

> `--gpu-blocks`, `--cpu-blocks`, and `--block-size` are named after GPU memory
> for historical reasons. For Foundry Local they configure a **logical page
> counter** used as a secondary admission gate, not real memory. See §7 and §12.

---

## 3. Quick Start — Python library

```python
import asyncio
from kvstream import KVStreamEngine
from kvstream.backends import FoundryBackend

async def main():
    engine = KVStreamEngine(
        backend=FoundryBackend(model="qwen2.5-0.5b-instruct-generic-cpu"),
        max_batch_size=8,   # max concurrent requests reaching Foundry Local
    )
    await engine.serve(port=8080)

asyncio.run(main())
```

### Streaming tokens in-process (no HTTP)

```python
import asyncio
from kvstream import KVStreamEngine
from kvstream.backends import FoundryBackend

async def main():
    engine = KVStreamEngine(backend=FoundryBackend(model="phi-3-mini"))
    async for token in engine.generate(
        prompt="Explain admission control in simple terms.",
        max_new_tokens=256,
        temperature=0.7,
    ):
        print(token.text, end="", flush=True)
        if token.finish_reason:
            print()

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
    )
    await engine.serve()

asyncio.run(main())
```

---

## 4. Connecting existing clients

Once the proxy is on `http://localhost:8080`, any OpenAI-compatible client works
by changing `base_url`. KVStream has **no authentication** — the `api_key` is ignored.

### openai-python SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-required",  # KVStream has no auth layer
)

# Non-streaming
response = client.chat.completions.create(
    model="phi-3-mini",  # must match a model loaded in Foundry Local
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is admission control?"},
    ],
    max_tokens=512,
)
print(response.choices[0].message.content)

# Streaming
with client.chat.completions.create(
    model="phi-3-mini",
    messages=[{"role": "user", "content": "Tell me a short story."}],
    max_tokens=256,
    stream=True,
) as stream:
    for chunk in stream:
        print(chunk.choices[0].delta.content or "", end="", flush=True)
```

### Firing concurrent requests

```python
import asyncio
from openai import AsyncOpenAI

client = AsyncOpenAI(base_url="http://localhost:8080/v1", api_key="not-required")

async def ask(q: str) -> str:
    r = await client.chat.completions.create(
        model="phi-3-mini",
        messages=[{"role": "user", "content": q}],
        max_tokens=256,
    )
    return r.choices[0].message.content

async def main():
    # KVStream admits up to max_batch_size of these at once and queues the rest,
    # so Foundry Local is never hit by all of them simultaneously.
    questions = ["What is CUDA?", "Explain transformers.", "What is a KV cache?",
                 "How does beam search work?"]
    answers = await asyncio.gather(*[ask(q) for q in questions])
    for q, a in zip(questions, answers):
        print(f"Q: {q}\nA: {a[:100]}...\n")

asyncio.run(main())
```

### LangChain

```python
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-required",
    model="phi-3-mini",
    streaming=True,
)
print(llm.invoke("What does an admission-control proxy do?").content)
```

### Checking readiness

```python
import httpx

def is_ready(url: str = "http://localhost:8080") -> bool:
    try:
        d = httpx.get(f"{url}/health", timeout=3.0).json()
        return d.get("status") == "ok" and d.get("backend_healthy", False)
    except Exception:
        return False
```

The `/health` response also reports which Foundry Local port was discovered
(`backend_url`).

---

## 5. The Foundry Local backend & port discovery

```python
from kvstream.backends import FoundryBackend

backend = FoundryBackend(
    base_url="http://localhost:5273",  # starting guess; auto-discovered if wrong
    model="phi-3-mini",
    timeout=120.0,
    exclude_ports=[8080],  # never mistake the KVStream proxy for Foundry
)
```

**Soft mode.** Foundry Local runs on the ONNX Runtime and exposes no KV-cache API.
KVStream therefore provides admission control and request queuing only; Foundry
Local owns all of its own memory and recomputes every prompt. No `PagedKVCache`
tensor pool is allocated for Foundry, and no preemption/swap occurs.

**Port auto-discovery.** Because Foundry Local's port changes between restarts,
`FoundryBackend` resolves the live URL in this order
([kvstream/backends/foundry.py](../kvstream/backends/foundry.py)):

1. **Cached URL** — the last known-good URL, if it still responds.
2. **Configured URL** — `base_url` (default `http://localhost:5273`).
3. **Port scan** — probes every `LISTENING` localhost port for an OpenAI-compatible
   `/v1/models` response, preferring a port that has a model loaded.
4. **Fallback** — returns the configured URL if nothing is found.

Discovery is serialised behind a lock (one scan at a time), has a 5-second
cooldown when Foundry is not running, excludes the proxy's own port, and only
ever scans **localhost** — it never makes outbound connections. See the
[Docker note](#10-docker-note) for why "localhost" matters.

---

## 6. Configuration reference

### kvstream.yaml

Place in the working directory; KVStream loads it automatically. Only the fields
that **actually affect the Foundry Local path** are shown here — see the notes.

```yaml
backend:
  type: foundry
  base_url: http://localhost:5273      # starting guess; auto-discovered if wrong
  model: phi-3-mini                    # a model id from Foundry's /v1/models
  timeout_seconds: 120.0

scheduler:
  max_batch_size: 8                    # max concurrent requests reaching Foundry Local
  priority: fcfs                       # fcfs (FIFO) | sjf (shortest prompt first)
  admission_timeout_seconds: 120.0     # how long a request may queue before a 503
  max_queue_depth: 1000                # reject new requests when the queue exceeds this

prefix_cache:
  enabled: false                       # recommended off for Foundry — see §8

# The `memory` block below configures a LOGICAL page counter, not VRAM (see §7).
memory:
  num_gpu_blocks: 256                  # concurrency-accounting units, not memory
  block_size: 16                       # tokens per accounting page (power of 2)

observability:
  metrics_enabled: true
  log_level: INFO                      # DEBUG | INFO | WARNING | ERROR
  trace_requests: false
```

**Fields that exist but do nothing on the Foundry path** (documented here so you
don't waste time tuning them):

| Field | Status for Foundry |
|---|---|
| `scheduler.max_waiting_tokens` | **Inactive.** Stored by the scheduler but never read anywhere. |
| `scheduler.max_tokens_per_seq` | **Inactive.** Never passed to or used by any component. |
| `scheduler.preemption_policy` (`swap`/`recompute`) | **Inactive.** Preemption is disabled for soft backends; running requests are never evicted. |
| `memory.num_cpu_blocks`, `dtype` | Configure the swap/tensor machinery, which is never used for Foundry. |
| `attention.*` | Only relevant to the (inert) hard-inject tensor path — see §12. |

### Environment variables

Every field can be set via `KVSTREAM_`-prefixed env vars with `__` for nesting:

```bash
export KVSTREAM_BACKEND__TYPE=foundry
export KVSTREAM_BACKEND__MODEL=phi-3-mini
export KVSTREAM_SCHEDULER__MAX_BATCH_SIZE=16
export KVSTREAM_SCHEDULER__MAX_QUEUE_DEPTH=500
export KVSTREAM_PREFIX_CACHE__ENABLED=false
export KVSTREAM_OBSERVABILITY__LOG_LEVEL=DEBUG
```

Priority (highest → lowest): CLI flags → constructor kwargs → env vars → `kvstream.yaml` → built-in defaults.

---

## 7. Concurrency tuning

The knob that matters for Foundry Local is **`max_batch_size`** — the maximum
number of requests allowed to reach Foundry Local at the same time. It is enforced
by a hard semaphore ([kvstream/engine.py](../kvstream/engine.py)), independent of
any internal bookkeeping.

- Too high → you reproduce the overload problem you installed KVStream to avoid.
- Too low → requests queue unnecessarily and throughput drops.
- Start at the concurrency where Foundry Local's latency stays flat on **your**
  hardware and model, then raise it until p99 latency starts to climb. There is no
  universal number; measure with `kvstream bench` (§11).

**`num_gpu_blocks` / `block_size` are not memory.** For Foundry Local these define
a logical page pool that the scheduler consults as a *secondary* admission gate
(`can_allocate`). Because Foundry owns its real KV memory, this pool corresponds to
nothing physical — it is a second, coarser concurrency limiter. In practice, leave
the defaults and tune `max_batch_size`; only raise `num_gpu_blocks` if you see the
`KV page pool saturated` warning under sustained long-output load (it is logged,
not fatal — the request still completes).

There is **no VRAM sizing table** in this guide because KVStream allocates no VRAM
for Foundry Local. Foundry Local's own memory footprint is governed by Foundry
Local, not by KVStream.

---

## 8. Prefix cache (why it is off for Foundry)

KVStream includes a prefix hash table intended to deduplicate shared prompt
prefixes. **For Foundry Local it provides no benefit and is recommended off.** Two
reasons, both structural:

1. **No recompute savings.** KVStream cannot inject KV state into the ONNX runtime,
   so Foundry Local recomputes the full prompt on every request regardless of any
   prefix match. The cache changes nothing about what Foundry does.
2. **It operates on bytes, not tokens.** `FoundryBackend` does not implement
   `tokenize()`, so the cache falls back to hashing raw UTF-8 bytes
   ([kvstream/backends/base.py](../kvstream/backends/base.py)). Registering and
   forking these entries costs proxy CPU and memory for zero return.

Disable it:

```bash
kvstream serve --backend foundry --no-prefix-cache
```

```yaml
prefix_cache:
  enabled: false
```

The `cached_prefixes` / `total_prefix_hits` counters in `/status` reflect only this
internal bookkeeping; they do **not** indicate any acceleration of Foundry Local.

---

## 9. Monitoring

### /status endpoint

```bash
curl http://localhost:8080/status | python -m json.tool
```

```json
{
  "scheduler": { "waiting": 2, "running": 8, "swapped": 0,
                 "gpu_blocks_free": 184, "gpu_utilization": 0.281 },
  "memory":    { "gpu_blocks_free": 184, "gpu_utilization": 0.281,
                 "cpu_blocks_free": 512 },
  "prefix_cache": {}
}
```

For Foundry, `swapped` is always 0 (no preemption), and the `gpu_*` figures
describe the logical accounting pool, not memory. The useful signals are
`waiting` (queue depth) and `running` (active backend calls, ≤ `max_batch_size`).

### CLI dashboard

```bash
kvstream status --url http://localhost:8080 --watch
```

### Prometheus `/metrics` — endpoint only, no KVStream series yet

A `/metrics` route exists:

```bash
curl http://localhost:8080/metrics
```

However, KVStream registers **no custom metrics** — the handler returns
`prometheus_client`'s default registry, which contains only the library's
process/runtime collectors. Scraping it therefore yields **nothing about queue
depth, concurrency, or the scheduler**; the real operational signals are on
`/status` and `kvstream status --watch`. Treat this endpoint as a placeholder
until KVStream-specific counters/gauges are added.

The `docker compose --profile metrics up -d` Prometheus + Grafana stack shipped in
this repo is wired to the Compose **Ollama** backend (it scrapes the containerised
`kvstream` service pointed at Ollama), not a host-run Foundry Local — so it does
not apply to the Foundry setup in this guide as shipped. If you do expose Grafana,
set a strong password (`KVSTREAM_GRAFANA_PASSWORD`).

---

## 10. Docker note

The shipped `docker-compose.yml` was written around a containerised backend and is
**not the recommended way to run KVStream against Foundry Local.**

Foundry Local runs as a host process (typically on Windows). KVStream's port
auto-discovery scans **localhost via `netstat`/`ss`** — inside a container,
"localhost" is the container, not the host, so **discovery cannot see a
host-side Foundry Local**. If you containerise KVStream, you must:

- give the container access to the host network, and
- pass an explicit, fixed `--backend-url` reachable from inside the container
  (host networking or `host.docker.internal`), since discovery will not work
  across the boundary.

For the common single-machine case, run KVStream directly on the host
(`kvstream serve --backend foundry`) alongside Foundry Local.

---

## 11. Benchmarking

Measure the admission-control effect on your own hardware:

```bash
# Baseline: hit Foundry Local directly (find its current port from /health first)
kvstream bench --url http://localhost:<foundry-port> --concurrency 16 --total-requests 50

# With KVStream in front
kvstream serve --backend foundry --port 8080
kvstream bench --url http://localhost:8080 --concurrency 16 --total-requests 50
```

Compare **error count** and **p50/p99 latency**. The expected result is that the
KVStream run completes more/all requests without errors, because it admits at a
safe concurrency and queues the rest — not that individual requests are faster.

> **Context length matters.** The bench defaults (`--prompt-len 128`,
> `--output-len 64`) use short contexts. At realistic agent/RAG workloads
> (10k–100k tokens) pre-fill cost dominates and the admission-control benefit
> shrinks. Benchmark at the context lengths your workload actually uses.

The `bench` command prints an illustrative table (requests, concurrency, errors,
throughput, p50, p99). The numbers are entirely hardware/model dependent; no
reference figures are published.

---

## 12. What is scaffolding, not a feature

KVStream is a proxy: it talks to Foundry Local over HTTP and receives text tokens.
It never loads weights, never runs attention, and never sees Foundry's KV tensors.
Several modules exist in the tree but are **not on any active execution path for
Foundry Local (or any backend):**

- **`PagedKVCache` (paged "KV tensor pool")** — allocated only for hard-inject
  backends, and even there never written to or read from. Not allocated for Foundry.
- **Attention kernels** (`naive`/`flash`/`xformers`) — never invoked; KVStream runs
  no attention. This is why token speed is entirely Foundry's.
- **Hard KV inject** (`save_kv_state`/`restore_kv_state`) — implemented on the
  llama.cpp adapter but never called by the engine or scheduler.
- **Preemption / swap** and **`num_cpu_blocks`** — inactive for soft backends.

Treat these as unbuilt/planned internals, not capabilities. Judge KVStream on what
runs for Foundry Local: admission control, request queuing, backpressure, port
auto-discovery, streaming passthrough, and observability.

---

## 13. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `503 server overloaded` | Queue saturated (`max_queue_depth`) or admission timed out | Increase `max_queue_depth` / `max_batch_size`, or lower client concurrency |
| `backend_healthy: false` | Foundry Local not running, or on a port discovery can't reach | Start Foundry Local; confirm it serves `/v1/models` on localhost; check `/health` for the discovered URL |
| Discovery picks the wrong service | Another OpenAI-compatible server is listening | Pass an explicit `--backend-url`, or ensure only Foundry has a model loaded |
| `KV page pool saturated` (warning) | Logical accounting pool exhausted under long-output load | Non-fatal; raise `num_gpu_blocks` if it recurs |
| All tokens look wrong/duplicated | Chat template mismatch | `_messages_to_prompt` uses a generic ChatML template; adjust for your model if needed |
| Requests queue but never admit | `max_batch_size` too low, or a slow request holding a slot | Raise `max_batch_size`; check `/status` `running` vs `waiting` |

```bash
# Quick checks
kvstream health --url http://localhost:8080
kvstream status --url http://localhost:8080 --watch
```

> Note: the earlier "high p99 → preemption thrashing" and "reduce
> `max_tokens_per_seq`" advice does **not** apply to Foundry Local — preemption is
> disabled and `max_tokens_per_seq` is inactive. High p99 under load is normally
> `max_batch_size` set too high for what Foundry Local sustains; lower it.
