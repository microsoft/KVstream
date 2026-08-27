# KVStream

**KVStream is an OpenAI-compatible gateway that sits in front of [Microsoft Foundry Local](https://learn.microsoft.com/azure/ai-foundry/foundry-local/) and governs access to it under concurrent, multi-agent load.**

It finds Foundry Local's ephemeral port, admits requests against a measured capacity, queues the overflow and sheds what it cannot serve, and can cache and de-duplicate repetitive calls.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **Scope.** KVStream is a **proxy**. It runs no model, owns no KV tensors, and has **no dependency on ONNX Runtime**. It governs *access* to one Foundry Local instance at the HTTP layer — it does not (and cannot) reach Foundry Local's KV cache or raise a device's physical capacity. What it does and does not buy you is measured in [Evidence](#evidence), including the cases where it costs you throughput and the case where it failed to prevent a stall.

---

## Why

Foundry Local runs models locally on the ONNX Runtime. When several clients call it at once — an agent swarm, a RAG fan-out, a planner loop — it accepts every request, and past its capacity latency degrades and, in at least one measured case, the runtime stops serving altogether and does not recover.

KVStream addresses that at the gateway layer:

- **Admission control** — only as much work as the backend can handle reaches it at once; the rest queues.
- **Backpressure** — overflow gets a `503` with `Retry-After`, and gets it quickly rather than after the full timeout.
- **Ephemeral-port discovery** — Foundry Local's port changes on almost every restart.
- **Route coverage** — chat, completions, embeddings and transcription are all admitted, so clients cannot route around the gateway.
- **Backend health that means something** — liveness and readiness reported separately, plus a circuit breaker.
- **Response cache and request coalescing** for repetitive deterministic calls.
- **Prometheus metrics** that reflect real admission, queueing, cache and coalescing behaviour.

---

## Install

**KVStream is not on PyPI.** Install from source:

```bash
git clone https://github.com/microsoft/KVstream
cd KVstream/kvstream-foundry
pip install -e .
```

Requires Python 3.10+. `pip install -e ".[dev]"` adds the test and lint tooling.

---

## Quick start — Python (no Docker)

This is the supported path on a developer machine, and the one where port discovery works.

### 1. Start Foundry Local and load a model

```bash
foundry service start          # service-based CLI (0.8.x)
foundry model run phi-3-mini-4k
```

On **Foundry Local CLI 0.10.0 (Preview)** the equivalents are `foundry server start` and `foundry model load`.

### 2. Find the model id the backend actually answers to

This step is not optional, and it is the most common way a first run fails. The catalog *alias* is often **not** the id the inference API accepts:

```bash
curl http://127.0.0.1:<port>/v1/models
```

On the machine this was developed against, the alias `phi-3-mini-4k` returned **HTTP 400** from `/v1/chat/completions`, while the id from `/v1/models` — `Phi-3-mini-4k-instruct-generic-gpu:2` — worked. Use the id, not the alias.

You can get the port from `foundry service status` (or `foundry status` on 0.10.x), but KVStream will find it itself.

### 3. Start the gateway

```bash
kvstream serve --model "Phi-3-mini-4k-instruct-generic-gpu:2"
```

It binds `127.0.0.1:8080` and resolves the backend automatically. To pin the backend instead of discovering it:

```bash
kvstream serve --backend-url http://127.0.0.1:51264 --model "Phi-3-mini-4k-instruct-generic-gpu:2"
```

Setting the URL explicitly **disables discovery entirely** — see [Both Foundry Local CLI generations](#both-foundry-local-cli-generations).

### 4. Check it before sending traffic

```bash
curl http://localhost:8080/health
```

`"backend_serving": true` means the backend completed a real trial generation. `"backend_reachable": true` on its own only means it answered `/v1/models` — which a stalled runtime keeps doing. A `503` here means the gateway will not be able to serve you either, and the `hint` field says why.

### 5. Send a request

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"Phi-3-mini-4k-instruct-generic-gpu:2",
       "messages":[{"role":"user","content":"Hello!"}],
       "max_tokens":32}'
```

```python
import openai

client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key="not-required")
print(client.chat.completions.create(
    model="Phi-3-mini-4k-instruct-generic-gpu:2",
    messages=[{"role": "user", "content": "Hello!"}],
    max_tokens=32,
).choices[0].message.content)
```

`api_key` is required by the OpenAI SDK but ignored by KVStream, which has no authentication of its own.

### Running it as a background service

```bash
kvstream serve --config kvstream.yaml > kvstream.log 2>&1 &
```

Redirecting output is safe on Windows — the CLI widens stdout and stderr to UTF-8 at startup, because a redirected stream defaults to cp1252 there and used to crash on the first non-ASCII character.

**Run exactly one process.** Admission state lives in memory, so N workers would enforce N budgets. KVStream refuses to start if `WEB_CONCURRENCY`, `UVICORN_WORKERS`, `GUNICORN_WORKERS` or `KVSTREAM_WORKERS` is above 1.

---

## Quick start — Docker

> **Not verified.** Docker was not installed on the machine this was developed against, so the image has never been built or run. What follows is written from the `Dockerfile` and `docker-compose.yml`, not from a successful run.

The container cannot see the host's listening ports and has no `foundry` binary, so **discovery and the CLI lookup are both disabled in the image** and an explicit backend URL is required. That is the supported container path, not a limitation being worked around.

```bash
# Build
docker build -t kvstream .

# Run against a Foundry Local on the host
docker run --rm -p 8080:8080 \
  --add-host host.docker.internal:host-gateway \
  -e KVSTREAM_BACKEND__BASE_URL=http://host.docker.internal:51264 \
  -e KVSTREAM_BACKEND__MODEL="Phi-3-mini-4k-instruct-generic-gpu:2" \
  kvstream
```

`--add-host` is needed on Linux; Docker Desktop resolves `host.docker.internal` already.

Or with compose, which sets the same things:

```bash
docker compose up --build
```

Edit `docker-compose.yml` to point `KVSTREAM_BACKEND__BASE_URL` at your actual port and `KVSTREAM_BACKEND__MODEL` at your actual model id.

**Two things to know about the container:**

- **The backend port changes.** Foundry Local takes a new ephemeral port on almost every restart, and the container cannot discover it. Either pin Foundry Local's URL on the host side, or update the environment variable after each restart.
- **It runs unprivileged** and its `HEALTHCHECK` uses `/health`, so Docker will mark the container unhealthy when the backend stops serving. KVStream still has no authentication — publish the port only where that is acceptable.

Configuration inside the image is by environment variable (`KVSTREAM_` prefix, `__` for nesting). To use a config file instead, mount it:

```bash
docker run --rm -p 8080:8080 -v "$PWD/kvstream.yaml:/home/kvstream/kvstream.yaml:ro" kvstream
```

---

## Evidence

Two targets: a reproducible model of a local runtime, and one real Foundry Local. They are reported separately because they support different claims.

### Against a modelled runtime (reproducible anywhere)

```bash
python benchmarks/admission_benchmark.py
```

The target is `benchmarks/simulated_foundry.py` — flat latency to 4 concurrent, quadratic past it, refusing beyond 16. It is a **model**, not Foundry Local; that is what makes it runnable in CI. KVStream is not told the ceiling: it calibrates first and admits against what it measured.

**Overloaded** — 120 mixed-size requests, 32 concurrent:

| | completed | 503s | success | goodput |
|---|---|---|---|---|
| direct to runtime | 24 / 120 | 96 | 20% | 3.8 req/s |
| **through KVStream** | **120 / 120** | **0** | **100%** | **6.9 req/s** |

Calibration produced a budget that held the runtime at 5 concurrent against a true optimum of 4, having been given neither number.

**Not overloaded** — 40 requests, 12 concurrent, a load the runtime absorbs unaided:

| | completed | p99 | goodput |
|---|---|---|---|
| direct to runtime | 40 / 40 | 3.20s | 5.7 req/s |
| through KVStream | 40 / 40 | **2.69s** | 4.9 req/s |

Nothing was going to fail, so admission control has no failures to prevent: it costs ~14% of goodput and buys ~16% off the tail. **The gateway is not a free win.** Measure your own load before assuming it helps.

Latency columns for the overloaded run are survivor-biased — the direct arm's percentiles cover only the 24 requests that came back. Compare success rate and goodput first.

### Against real Foundry Local

`foundry 0.8.119`, `phi-3-mini-4k`, one machine, Windows build 19045 — below the 26100 Foundry needs to register the Windows ML execution providers, so the "generic-gpu" model ran on a CPU fallback path. **One machine, one model, one version: evidence, not a general characterisation.**

**It serialises; it does not batch.** Identical requests at rising concurrency:

| Concurrency | Mean latency | Throughput |
|---|---|---|
| 1 | 0.38s | 2.61 r/s |
| 2 | 0.77s | 2.60 r/s |
| 4 | 1.55s | 2.58 r/s |
| 8 | 3.12s | 2.57 r/s |

Throughput flat, latency exactly linear, mean equal to max. Optimal concurrency is 1, and in this regime there is no knee to find. Calibration landed on it unaided, setting `B` to a single small request.

### What kind of runtime is yours?

Local inference capacity is hardware-specific and Foundry Local does not expose it, so KVStream measures it rather than assuming it. `kvstream calibrate` reports **what shape of runtime it found**, not only how much of it there is — because the same budget means different things on different hardware:

| Regime | What the sweep sees | What the gateway is worth |
|---|---|---|
| **serialising** | generated-token throughput flat as concurrency rises | Admission control **cannot raise throughput**. Value is bounded backpressure, readiness and shedding. Cache and coalescing are the levers that add capacity, because they remove work. |
| **batching** | throughput climbs to a peak, then degrades | Admission control protects that peak. The token budget earns its keep on mixed request sizes. |
| **capped** | requests are refused beyond a threshold | Admission control converts refusals into queueing — the largest measurable win. |

`GET /status` reports it under `runtime_profile`, and calibration logs the advice.

**On the one machine measured here it is `serialising`** — `foundry 0.8.119`, `phi-3-mini-4k`, on a box whose Windows build (19045 < 26100) is too old to register the ML execution providers, so the model ran on a CPU fallback path. **That result should not be generalised.** On hardware where the providers register the answer may well be `batching`, and the gateway would then be worth more, not less. Run the sweep on your own machine; that is the entire point of measuring instead of asserting.

> **A note on the measurement, because it bit three times.** Throughput here is counted in *generated* tokens per second. Requests per second moves with the probe's request-size mix as much as with the runtime. Prompt-plus-generated tokens is worse — prefill is far cheaper per token than decode, so a mix that tilts prompt-heavy at higher concurrency manufactures a gain that is not there. On this machine that error turned a flat runtime into a false 1.43x "batching" reading before it was caught.


**A healthy backend, arms run separately to control for order** — 30 requests, 12 concurrent:

| | completed | p99 | goodput |
|---|---|---|---|
| direct to runtime | 30 / 30 | 14.0s | 1.06 req/s |
| through KVStream | 30 / 30 | **12.3s** | 0.98 req/s |

~8% of goodput for ~12% off the tail — the same shape the modelled runtime showed.

**Sustained heavy load stalls it, permanently.** 80 mixed requests with prompts to 1200 tokens at 16 concurrent, unprotected: the runtime degraded, then stopped serving. A **4-token** request did not return in 180 seconds, with the inference process holding **13.4 GB** resident for a 2.13 GB model. It did not recover on its own, and `foundry service restart` hung. That is the KV-memory-exhaustion regime of [Appendix B §B.2](docs/APPENDIX-B-CALIBRATION.md), not a concurrency cap.

**With the gateway in front, the same class of load survived.** 60 requests with ~1200-token prompts produced 32 served, 28 shed with `503`, and a backend still answering in 0.34s afterwards.

**Shedding is fast.** Those 503s used to come back at a flat 120.1s — the admission timeout, to the decimal. A queued request now re-predicts whether the queue can still reach it in time and gives up early: same hardware, same load, **median 8.1s**, minimum 0.1s, with a `Retry-After` derived from the measured drain rate.

**The circuit breaker works end to end.** When the backend genuinely failed, it opened after 8 consecutive errors, refused subsequent requests in **0.04s**, then after the cooldown admitted one trial (200 in 0.53s), closed, and served 15 of 15.

### What this evidence does not show

- **Admission control did not prevent the unprotected stall.** In the run that killed the runtime, the pressure was cumulative across a long session rather than instantaneous concurrency. A budget from a sixty-second calibration sweep does not describe what an hour of traffic does to the runtime's memory. Drift detection now *notices* this; nothing in the gateway prevents it. Preventing it is an in-engine problem.
- **Token mode has never been exercised against real hardware.** Every real-hardware run above used `concurrency` mode. In the one attempt at token mode, the calibration record was keyed to a different `device` label than the gateway resolved, so it was correctly refused and the gateway fell back — visible in `/status` as `fallback:concurrency`. The token-budget claim rests on the modelled runtime and the unit tests.
- **Foundry Local 0.8.119 returns no `usage` block.** Online token calibration therefore receives no samples against it, and the *uncalibrated* estimator figures below are the ones that apply. The gateway degrades correctly and reports this as `backend.usage_reporting`.
- **`admission_timeout_seconds` defaults to 120s.** The hardware runs above used 20s. No predictor makes a two-minute deadline reasonable for an interactive client.

---

## Admission modes

| Mode | Budget unit | When to use |
|---|---|---|
| **`concurrency`** *(default)* | max concurrent requests | Works with no calibration. A request-count cap. |
| **`tokens`** | calibrated KV-token budget `B` | Admits by each request's estimated KV footprint. Better for mixed request sizes; requires calibration. |

```bash
kvstream calibrate --model "Phi-3-mini-4k-instruct-generic-gpu:2" --device npu
kvstream serve --mode tokens --model "Phi-3-mini-4k-instruct-generic-gpu:2"
```

The budget is an **estimate calibrated to observed behaviour**, not a reading of the runtime's memory. Re-calibrate when the model, device or runtime version changes. See [Appendix B](docs/APPENDIX-B-CALIBRATION.md) for the methodology and its limits.

### Calibration is keyed to the environment it was measured in

A budget only means something for the `(model, device, CLI generation, Foundry Local version)` it was measured against:

| Match | Behaviour |
|---|---|
| **exact** | used silently |
| **partial** (same model + device, different Foundry/CLI version) | used, with a warning naming both versions |
| **no record for this model *and* device** | **not used** — falls back to `concurrency` mode and tells you to calibrate |

A budget measured elsewhere is worse than none: it is confidently wrong. `GET /status` reports `budget_source` so you can always see which applied — and as noted above, this strictness is why the hardware runs ended up in concurrency mode. KVStream cannot see which accelerator Foundry Local picked, so set `backend.device` yourself when more than one is in play.

### Costing across models

A client can name any model. Charging them the same per token treats a 32-layer model and a 4-layer one as equal. Declare a model's published architecture and KVStream costs it in proportion:

```yaml
models:
  phi-3-mini:
    num_hidden_layers: 32
    num_key_value_heads: 32      # KV heads, not attention heads — GQA matters
    head_dim: 96                 # or give hidden_size and it is derived
    torch_dtype: float16
```

`kv_bytes_per_token = 2 × layers × kv_heads × head_dim × dtype_bytes`, and only the **ratio** against the calibrated model is used — no claim is made about device memory. An undeclared model weighs 1.0, i.e. is costed exactly as it would be without this feature. `GET /status` shows what it knows under `model_geometry`.

### Fair admission

Queued requests are admitted in **strict arrival order**, and on release the queue drains from the front while the next waiter fits. Waking every waiter to re-check has no ordering at all, so a large request can be starved indefinitely by a stream of small ones.

The cost is head-of-line blocking: a large request at the front holds back smaller ones until it fits. Deliberate, and measured rather than assumed — see `kvstream_admission_wait_seconds` and `admission.queue` in `/status`.

### Live accounting

A request is admitted on `prompt_tokens + max_tokens`, a worst case. KVStream settles the reservation against reality as the response streams, returning headroom a short generation was never going to use. Watch `kvstream_reservation_reclaimed_tokens_total`.

`admission.reserve_completion_ratio` reserves only a fraction of `max_tokens` up front. That is an explicit trade: a generation that outgrows its reservation is topped up rather than truncated, which can transiently exceed the budget. Every such event increments `kvstream_reservation_overshoot_total`. At the default `1.0` overshoot is impossible.

### Shedding load

When the queue cannot reach a request inside its deadline, KVStream refuses it rather than making it wait out the timeout — with a `Retry-After` derived from the measured drain rate.

The prediction is deliberately timid. It only gets a vote when every unit of budget is in use, the queue is at least as deep as the budget, and the deadline would be missed by `hopeless_margin` (default 1.5×). Without those guards it starves itself: a low measured rate causes rejections, rejections prevent completions, the rate never recovers, and the gateway refuses a backend that is working. That was measured, not theorised — 19 of 20 healthy requests wrongly shed before the guards went in. Set `reject_when_hopeless: false` to disable.

---

## Caching and coalescing

Both are limited to **deterministic** requests: an explicit `temperature: 0` and `n == 1`. An *absent* temperature means the backend's default, which is not deterministic, so it is not treated as cacheable.

- **The response cache is opt-in** — `cache.enabled` defaults to `false`, because caching changes response semantics.
- **Coalescing is on by default** (`coalesce.enabled: true`). It does not change semantics: identical concurrent requests share one upstream call. On the streaming path, followers replay what they missed and then track the leader; they consume no admission budget, and their response carries `X-KVStream-Coalesced: 1`.

A caller can opt one request out without an operator changing anything:

| Header | Effect |
|---|---|
| `Cache-Control: no-store` | skip the cache in both directions |
| `Cache-Control: no-cache` | re-fetch, but still refresh the entry |
| `x-kvstream-cache: no-store` | same, for clients that need `Cache-Control` for their own purposes |

Set `cache.respect_request_headers: false` to ignore them. Responses larger than `cache.max_entry_bytes` are not cached — one huge entry evicts everything useful — and each skip is counted in `kvstream_cache_skipped_total`.

---

## Knowing when the backend is broken

| | What it does |
|---|---|
| **Readiness** | `/health` reports `backend_reachable` (answers `/v1/models`) separately from `backend_serving` (completed a bounded trial generation), and returns 503 unless it is *serving*. The probe is a real generation, so it is cached for `readiness_interval_seconds` and single-flighted. `?probe=true` forces a fresh one. |
| **Circuit breaker** | After `circuit_breaker_failures` consecutive backend failures, requests fail fast with 503 + `Retry-After` instead of queueing behind a backend that will not answer. One trial is allowed through after the cooldown. Client 4xx never trips it. |
| **Drift detection** | Served seconds-per-token is compared against the baseline recorded at calibration time. Exceed `drift_warn_ratio` and KVStream warns that the budget was measured on a backend that no longer behaves this way. It warns; it does not silently re-tune itself. |

All three exist because of one measured failure: the runtime stopped completing generations entirely while `/v1/models` kept answering **200 in 4 ms**. Liveness said everything was fine. Metrics: `kvstream_backend_ready`, `kvstream_circuit_breaker_state`, `kvstream_backend_drift_ratio`.

**Drift detection is inactive without a calibration record**, since there is no baseline to compare against — `/status` reports `drift.state: unknown`.

---

## Shutdown

The gateway stops admitting, turns away anything still **queued** (it has not started, so it loses nothing by retrying), and gives in-flight requests `drain_timeout_seconds` to finish. Requests arriving during shutdown get a 503 that says shutdown, distinct from an overload 503.

---

## Token estimation (and how it self-corrects)

KVStream does not ship the model's tokenizer and must **estimate** each request's size. Two things keep that honest:

1. **A conservative heuristic** — text is measured by character rate *and* by word/punctuation count, and the larger is used. A plain `chars/4` rule badly under-counts code, JSON and chat markup.
2. **Online self-calibration** — when the backend returns a `usage` object, KVStream learns the real ratios and adapts (EWMA, clamped). KVStream asks for those counts: non-streamed requests are forwarded non-streamed, and streamed ones carry `stream_options.include_usage`, which most OpenAI-compatible servers require before emitting a trailing usage chunk. If a backend rejects the field, KVStream retries once without it and stops asking.

   `GET /status` reports `backend.usage_reporting`, which means *KVStream is still asking* — **not** that the backend answers. Foundry Local 0.8.119 accepts the field and returns no `usage` block anyway, so status reads `true` while calibration receives nothing. The reliable signal is `token_estimator.samples`: if it stays at 0 under traffic, no real counts are arriving.

**Measured.** `python benchmarks/estimator_benchmark.py` reproduces this — 400 held-out samples across prose, JSON, code and agent transcripts, scored against `cl100k_base`:

| Estimator | Mean abs. error | Bias | Under-counts |
|---|---|---|---|
| Naive `chars/4` | 16.5% | +3.2% | 56.8% |
| KVStream, uncalibrated | 19.1% | +13.8% | 36.2% |
| KVStream, calibrated | **6.8%** | +5.2% | 16.2% |

**It does not eliminate under-counting, and you should not assume it does.** At the default `token_safety_factor: 1.0` the calibrated estimator still under-counted 16% of held-out samples, worst case by 18%. Code is the hardest shape (15.2% mean error); JSON the easiest (1.7%). For a hard guarantee, buy it explicitly:

| `admission.token_safety_factor` | Under-count rate |
|---|---|
| 1.0 (default) | 16.2% |
| 1.1 | 2.5% |
| **1.25** | **0%** |

1.188 was the smallest factor covering every sample in this run; 1.25 is the round number. Estimates are scaled by 1.25, so roughly 20% less work fits in the same budget. The calibrated budget already carries its own 0.85 safety margin, which is why the default is 1.0.

**Caveats:** `cl100k_base` is not the Foundry model's tokenizer, so treat magnitudes as indicative of *shape*. The corpus is synthetic and seeded — reproducible, not representative of your traffic. And **if your backend never reports `usage` — as Foundry Local 0.8.119 does not — calibration never happens** and the uncalibrated row is what you get.

---

## Agent workloads: tool calling and pass-through

KVStream validates only what it needs to cost and stream a request — the model, the messages, and the knobs that decide streaming and admission — and forwards **the client's own JSON object untouched**. `tools`, `tool_choice`, `response_format`, `seed`, `logprobs`, `logit_bias`, `frequency_penalty`, `user` and anything a future API version adds all ride along. An allow-list would drop unknown fields *quietly*.

Responses come back the same way — the backend's actual body and its actual SSE chunks, not a reconstruction. That preserves `tool_calls`, the opening `{"role": "assistant"}` delta, `logprobs`, `system_fingerprint` and the real completion `id`.

Message shapes an agent loop depends on are accepted: an assistant turn with `content: null` and `tool_calls`, a `tool` result carrying `tool_call_id`, and multimodal content-part arrays.

```python
client.chat.completions.create(model=MODEL, messages=messages, tools=tools)
```

Details worth knowing:

- **Tool calls are billed.** Re-sent transcripts carry `tool_calls`, whose JSON is real prompt tokens on the next turn. Streamed tool-call arguments count as generated tokens.
- **Images are not.** A non-text content part has no token cost measurable at the HTTP layer. It is forwarded, and it is not guessed at.
- **No default is imposed on the model.** Omit `max_tokens` or `temperature` and KVStream sends neither, so the model's own default applies. `admission.default_max_tokens` is used only to *cost* such a request and is never sent upstream.
- **`n > 1` is allowed and costed as such** — four completions of 512 tokens can occupy four times the KV footprint.
- **Usage is not fabricated.** A client that did not ask for a usage chunk does not receive one. If the backend reports no counts, the `usage` block is filled from KVStream's estimate and the response carries `X-KVStream-Usage: estimated`.
- **Caching keeps tool calls intact.** The cache stores the backend's real body (or the exact recorded chunks), and the key includes `stream`.

---

## Both Foundry Local CLI generations

Foundry Local is mid-transition between the **service-based CLI** (`foundry service start|status`, e.g. `0.8.119`) and **Foundry Local CLI 0.10.0 (Preview)** (`foundry server start`, `foundry status`, `--output json`). KVStream integrates at the OpenAI HTTP wire protocol and has no dependency on the CLI, the SDKs, the native library, or ONNX Runtime, so the inference path is identical for both.

Everything version-specific is confined to how the endpoint is located:

| Step | Source | Notes |
|---|---|---|
| 1 | **Explicit URL** — `--backend-url`, `backend.base_url`, `KVSTREAM_BACKEND__BASE_URL` | Authoritative. Discovery skipped entirely. The only supported path in containers. |
| 2 | **CLI-assisted** — `foundry --version`, then `foundry status --output json` (0.10.x) or `foundry service status` (service-based) | Best-effort. Any parse failure is a miss, not an error. |
| 3 | **Localhost scan** for a port answering `/v1/models` | Version-agnostic: it tests the HTTP surface. |
| 4 | **Failure** | Names the correct start command for the generation actually detected. |

**Verified against the service-based CLI only.** Steps 1–4 have been exercised live against `foundry 0.8.119`, including the CLI-assisted lookup — which required a fix, because `foundry service status` reports a *status page* URL (`.../openai/status`), not an API root, so the reported endpoint is now tried both as given and as its bare origin. **The 0.10.x path is implemented and unit-tested against synthetic output, but no 0.10.x binary was available to test against.** Its JSON schema is unverified and has no documented stability contract, so step 2 walks the whole document for a URL-shaped value rather than assuming a field name, and treats any failure as a fall-through.

`backend.use_foundry_cli` defaults to `auto`: enabled on a host, disabled inside a container. `never` and `always` override.

**Where a gateway cannot help.** The Foundry Local SDK is a native library called in-process, and its HTTP web service is optional. When an application loads a model in-process and never starts that service, there is no endpoint for KVStream to sit in front of. Co-location is not shared address space — the same boundary that keeps KV tensors out of reach.

### Which port is Foundry Local?

A localhost port answering `/v1/models` is not necessarily Foundry Local; Ollama, LM Studio, vLLM and another KVStream all speak the same API. There is no published fingerprint that positively identifies Foundry Local at the HTTP layer, so discovery **ranks rather than asserts**: another KVStream is excluded outright (it identifies itself with a header we control, which stops a gateway proxying into a gateway), a port serving your configured model wins, an identifiable non-Foundry server is demoted, and otherwise a port with a model loaded beats one without. If that ambiguity matters, pin the URL.

---

## Configuration

`kvstream.yaml` in the working directory (or `--config path`). See [`kvstream.example.yaml`](kvstream.example.yaml), which documents every field. Every field is also an environment variable (`KVSTREAM_ADMISSION__MODE=tokens`). Precedence: CLI flags → env vars → YAML → defaults.

## CLI

```
kvstream serve       Start the gateway
kvstream health      Check gateway + backend health
kvstream status      Live admission / cache stats
kvstream calibrate   Measure and store the KV-token budget B
kvstream bench       Send a concurrent load; report latency / errors
```

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/chat/completions` | OpenAI-compatible (streaming + non-streaming), including tool calling |
| `POST /v1/completions` | Legacy; proxied and admitted on the KV-token budget |
| `POST /v1/embeddings` | Proxied verbatim; admitted on the KV-token budget |
| `POST /v1/audio/transcriptions` | Proxied verbatim; admitted on a separate concurrency limit |
| `GET /v1/models` | Models reported by the backend |
| `GET /v1/models/{id}` | Proxied verbatim |
| `GET /health` | Liveness *and* readiness (**503** unless the backend is serving) |
| `GET /status` | Admission, queue, budget provenance, backend health, drift, cache, coalescer |
| `GET /metrics` | Prometheus metrics |

Errors use the OpenAI envelope — `{"error": {"message", "type", "code", "param"}}`. Overload and shutdown return `503` with `Retry-After`; a backend failure returns `502` on both streaming and non-streaming paths.

`/v1/embeddings`, `/v1/audio/transcriptions` and `/v1/completions` can each be turned off in `routes`. Routes Foundry Local exposes that KVStream does **not** proxy are not reachable through the gateway.

---

## What it does not do

- ❌ No access to or management of Foundry Local's KV tensors; **no ONNX Runtime dependency**.
- ❌ Does not raise a device's physical concurrency ceiling — it governs use of the existing capacity.
- ❌ **Does not prevent a runtime from exhausting itself over a long session.** Measured: it did not. It detects and reports the condition; preventing it is an in-engine problem.
- ❌ No multi-provider routing — KVStream targets Foundry Local specifically.
- ❌ No authentication, TLS, or multi-tenancy.
- ❌ No multi-machine routing across several Foundry Local instances. Deliberately absent rather than half-built.
- ❌ No semantic cache — exact-match only.
- ⚠️ The token budget is a **calibrated estimate**, and token mode has not been exercised on real hardware.
- ⚠️ Caching changes response semantics; it is opt-in and limited to deterministic requests.
- ⚠️ Admission control **costs throughput when the backend is not overloaded**. See [Evidence](#evidence).

---

## Security

KVStream has **no authentication** and binds to `127.0.0.1` by default. Do not expose it on an untrusted network without an authenticating reverse proxy. `/status` and `/metrics` disclose operational data — treat them as internal. `backend.forward_authorization` passes a caller's `Authorization` header upstream; `backend.api_key` sets a static one.

---

## Documentation

| Document | What it covers |
|---|---|
| [Technical proposal (rev 2)](docs/PROPOSAL.md) | The case put to Microsoft, with the premise corrected against measurement |
| [Appendix B — Calibration methodology](docs/APPENDIX-B-CALIBRATION.md) | How `B` is measured, what it means, and what would make the procedure unnecessary |
| [Gap analysis](docs/GAP-ANALYSIS.md) | This codebase mapped against the technical proposal, gap by gap, including what is still open |
| [Benchmarks](benchmarks/README.md) | What each harness measures, and what it does not |
| [CHANGELOG](CHANGELOG.md) | Including what is deliberately not implemented |

---

## Development

```bash
pip install -e ".[dev]"
pytest              # 354 unit + integration tests; no live Foundry Local required
ruff check .
mypy

pip install tiktoken
python benchmarks/estimator_benchmark.py
python benchmarks/admission_benchmark.py
```

A GitHub Actions workflow is included (`.github/workflows/ci.yml`) that runs the suite on Linux and Windows against Python 3.10 and 3.12, executes both benchmarks and builds the container image. **It has not been executed** — this tree is not a git repository and the workflow has never run on CI.

## License

[MIT](LICENSE).
