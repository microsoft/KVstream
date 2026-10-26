# KVStream — Architecture and System Design

**Status:** design target for KVStream 2.x. Each component is marked **[exists]**, **[partial]** or **[planned]** so this document never describes code that is not there.
**Scope:** Microsoft Foundry Local only, one gateway per Foundry Local instance, one machine (CPU, GPU or NPU).
**Evidence base:** [`reports/LLM gateway landscape for KVStream.md`](../reports/LLM%20gateway%20landscape%20for%20KVStream.md) (ecosystem survey, 2026-10) the measurements in [README → Evidence](../README.md#evidence), and the live run against Foundry Local CLI 0.10.3 in §19.

---

## 1. What KVStream is

KVStream is the **capacity and continuity layer for one Foundry Local process**. It sits on the same machine as Foundry Local, speaks the OpenAI wire protocol on both sides, and decides three things the runtime cannot decide for itself:

1. **How much work the box can take right now**, measured rather than assumed.
2. **Which request goes next**, so that agent workflows already in progress finish before new ones start.
3. **Whether the runtime is still healthy**, distinguishing *alive*, *serving*, *degrading* and *stalled*.

It does not touch tensors, run a model, or route between providers. The 2026 ecosystem survey found that every mainstream gateway (agentgateway, Agent Router/Envoy AI Gateway, LiteLLM, Bifrost, Portkey, TensorZero, Kong, APISIX, Azure APIM, Cloudflare) enforces **quotas** against elastic remote APIs, and none protects the **capacity of a finite local backend**. That gap is KVStream's product. Everything else (auth, MCP, guardrails, multi-provider failover) is delegated to a gateway in front of it (§12).

### 1.1 Design principles

| # | Principle | Consequence in the design |
|---|---|---|
| P1 | **Measure, don't assume.** Foundry Local exposes no queue, batch or memory metrics. | Capacity comes from calibration plus live feedback (§7). Every estimate is labelled as one. |
| P2 | **Tell the truth under failure.** | A truncated stream never ends in `[DONE]`. A client error is never reported as a backend error. Estimated usage carries `X-KVStream-Usage: estimated`. |
| P3 | **Finish what you started.** | Shed new sessions before turn N of an admitted agent (§6). |
| P4 | **Reject early, usefully.** | 503 with a `Retry-After` derived from the measured drain rate, decided on arrival, not after a timeout. |
| P5 | **Shared work is owned by the gateway, never by a client.** | Upstream generations run as gateway-owned tasks. Clients are subscribers (§8). |
| P6 | **Retries live at exactly one layer, and it is not KVStream.** | KVStream never re-sends a generation. It returns signals the client or north gateway can act on. |
| P7 | **One process, one budget.** | Admission state is in memory. Multiple workers are refused at startup. Scale is "one KVStream per Foundry Local". |
| P8 | **Degrade towards pass-through, never towards outage.** | When a subsystem has no data (no calibration, no usage counts, no metrics), it falls back to the simplest safe behaviour and says so in `/status`. |

---

## 2. Context and topology

```
 Agent framework ──(tool calls)──► MCP gateway (agentgateway / Docker MCP / ContextForge) ──► tool servers
       │
       └──(chat/completions)──► [optional] north gateway: agentgateway | APIM self-hosted | Bifrost | TensorZero
                                  auth, keys, quotas, guardrails, OTel, cloud fallback (their feature, not ours)
                                         │  custom OpenAI-compatible provider → http://127.0.0.1:8080
                                         ▼
 ┌────────────────────────────────────── KVStream (single process) ──────────────────────────────────────┐
 │  ingress → pipeline → scheduler → dispatcher → backend adapter            runtime supervisor          │
 └───────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                         │  OpenAI REST (dynamic port)            │ control plane (CLI / REST)
                                         ▼                                        ▼
                      Foundry Local  (CLI 0.8.x service | CLI 0.10.x server | SDK 2.x in-process REST)
                      execution provider: CPU | CUDA | WebGPU | OpenVINO | NPU (Qualcomm, AMD Vitis)
```

The north gateway is **optional**. On a basic machine a second proxy costs memory and a hop, so KVStream must remain fully usable when agents call it directly.

---

## 3. Component architecture

```
                         ┌───────────────────────────── Ingress ─────────────────────────────┐
  HTTP ─────────────────►│ routes · OpenAI error envelope · header contract (§5) · request id │
                         └───────────────┬────────────────────────────────────────────────────┘
                                         ▼
                         ┌──────────────────────── Request pipeline ─────────────────────────┐
                         │ classify → cost → cache → idempotency/coalesce → admit → dispatch │
                         │                         RequestContext flows through every stage   │
                         └──────┬─────────────────────────┬──────────────────────┬───────────┘
                                ▼                         ▼                      ▼
   ┌────────── Scheduler ──────────┐   ┌──────── Dispatcher ────────┐   ┌──── Stores ────┐
   │ QueuePolicy (order)           │   │ UpstreamRun (owned task)   │   │ calibration    │
   │ CapacityAccountant (reserve/  │   │ Broadcast (subscribers)    │   │ response cache │
   │   settle/release)             │   │ timeouts: TTFT/idle/total  │   │ idempotency    │
   │ LimitController (budget)      │   │ live accounting            │   └────────────────┘
   │ Shedder (predicted wait)      │   └─────────────┬──────────────┘
   └──────────────┬────────────────┘                 ▼
                  │                     ┌──── Backend adapter ────┐   ┌──── Runtime supervisor ─────┐
                  └────────────────────►│ FoundryClient (data)    │◄──│ state machine (§10)         │
                                        │ FoundryControl (ctrl)   │   │ discovery · readiness       │
                                        └─────────────────────────┘   │ breaker · drift · memory    │
                                                                       │ warm-keeping · restart      │
                                                                       └─────────────────────────────┘
```

### 3.1 Current module → target component

| Target component | Today | Status | Move |
|---|---|---|---|
| Ingress | `app.py` `build_app` | [exists] | Keep thin; routes only call the pipeline. |
| Request pipeline | `app.py` `Gateway` (≈1,350 lines; costing, cache, coalescing, admission, streaming and metrics interleaved) | [partial] | Split into `pipeline/` stage modules. Defect 3 (§8.4) was a symptom of this interleaving. |
| CapacityAccountant | `admission/capacity.py` reserve / adjust / release | [exists] | Keep. It is correct and well tested. |
| QueuePolicy | strict FIFO inside `CapacityManager._queue` | [partial] | Extract the ordering decision behind an interface (§6.3). |
| LimitController | fixed budget from `resolve_budget` | [partial] | Static → calibrated → adaptive (§7). |
| Shedder | `predicted_wait`, `_may_reject` in `capacity.py` | [exists] | Keep the guards; add class-aware shedding order. |
| Dispatcher / UpstreamRun | `handle_streaming` + `_run_stream` | [exists] | Streams are gateway-owned tasks (§8). |
| Broadcast | `cache/broadcast.py` | [exists] | Now carries subscriber counting and abandon callbacks. |
| Backend adapter (data) | `backend/foundry.py` | [exists] | Add split timeouts; errors carry upstream status. |
| Backend adapter (control) | `backend/foundry_cli.py`, `resolver.py`, `discovery.py` | [partial] | `foundry status --output json` verified live on 0.10.3. The 0.8.x REST control endpoints (`/openai/status`, `/openai/loadedmodels`) return **404 on 0.10.3**, so control goes through the CLI. `backend/inventory.py` [exists] tracks *downloaded* models (`/v1/models`) and *loaded* models (`foundry cache list --output json`) separately. Token-count endpoint unverified. |
| Runtime supervisor | `backend/health.py`, `admission/drift.py`, discovery cooldown | [partial] | Unify into one state machine (§10). |
| Stores | `calibration.py` (JSON), `response_cache.py` (memory) | [partial] | Add idempotency store (memory, TTL). |
| Observability | `observability/metrics.py` (Prometheus) | [exists] | Add OTel GenAI spans and vLLM-style metric aliases. |

---

## 4. Request lifecycle

Each stage may **short-circuit** with a response, and must release whatever it acquired. Stage order is fixed. Stages are plain async functions over a `RequestContext`, not a framework.

| # | Stage | Reads | Writes / acquires | Short-circuits with |
|---|---|---|---|---|
| 1 | **classify** | route, headers | `session`, `priority`, `deadline`, `idempotency_key`, `deterministic` | 400 on a malformed header |
| 2 | **cost** | messages, `max_tokens`, model | `RequestCost` (prompt estimate or exact count, generation allowance, KV weight) | — |
| 3 | **cache** | `deterministic`, cache directive | — | cached body or replayed SSE |
| 4 | **idempotency / coalesce** | idempotency key or content key | subscription to an existing `UpstreamRun` | joined response (no admission cost) |
| 5 | **guard** | supervisor state | breaker trial slot (must be released) | 503 + `Retry-After` while open or stalled |
| 6 | **admit** | `RequestCost`, `priority`, `session`, deadline | reservation (must be released) | 503 queue full / predicted wait / deadline / timeout |
| 7 | **dispatch** | payload | `UpstreamRun` task | 4xx passthrough, 502 upstream failure (before any byte is sent) |
| 8 | **stream / settle** | tokens, `usage` | live accounting adjustments | SSE error frame (after headers are sent) |
| 9 | **finalize** | outcome | release reservation, breaker outcome, drift sample, cache store, metrics, span end | — |

### 4.1 `RequestContext` (planned)

```python
@dataclass
class RequestContext:
    req_id: str
    route: str                      # chat | completions | embeddings | transcriptions
    raw: dict                       # client JSON, forwarded untouched
    model: str
    stream: bool
    session: str | None             # X-KVStream-Session
    priority: Priority              # critical | standard | sheddable
    deadline: float | None          # monotonic seconds
    idempotency_key: str | None
    cost: RequestCost | None
    reservation: Reservation | None # released exactly once in finalize
    breaker_trial: bool             # released exactly once in finalize
    outcome: Outcome | None         # served | cached | coalesced | shed | client_error | backend_error | aborted
```

The two `must be released` resources (reservation and breaker trial) are the main source of robustness bugs in this kind of system. They are owned by the context and released by `finalize`, which always runs. §13 states the invariants.

---

## 5. Header contract

All headers are optional. A client that sends none gets today's behaviour exactly.

### 5.1 Inbound

| Header | Values | Effect | Status |
|---|---|---|---|
| `X-KVStream-Session` | opaque id ≤128 chars | Groups the calls of one agent run for scheduling (§6). A north gateway can fill it from the MCP or agent-run id. | [planned] |
| `X-KVStream-Priority` | `critical` \| `standard` \| `sheddable` | Queue class and shedding order. Default `standard`. | [planned] |
| `X-KVStream-Deadline-Ms` (also `Request-Timeout` seconds) | integer | Drop queued work that can no longer finish in time. Caps the backend timeout. | [planned] |
| `Idempotency-Key` | opaque id | A retry with the same key joins the running generation or replays the finished one (§11). Works for non-deterministic requests. | [planned] |
| `Cache-Control` / `x-kvstream-cache` | `no-store`, `no-cache`, `only-if-cached` | Per-request cache directive. | [exists] |
| `Authorization` | — | Forwarded upstream when `backend.forward_authorization`. | [exists] |

### 5.2 Outbound

| Header | Meaning | Status |
|---|---|---|
| `X-Request-Id` | gateway request id | [exists] |
| `Retry-After` | seconds, on every 503 | [exists] |
| `X-KVStream-Coalesced: 1` | response shared with an identical request | [exists] |
| `X-KVStream-Usage: estimated` | `usage` block is KVStream's estimate, not the backend's | [exists] |
| `X-KVStream-Queue-Ms` | time spent waiting for admission | [planned] |
| `X-KVStream-Cache: hit\|miss\|bypass` | cache outcome | [planned] |

---

## 6. Scheduling: who goes next

### 6.1 Why FIFO is wrong for agents

Strict FIFO (today) is fair per *call*, and that is the problem. An agent run is 10 or more sequential calls separated by tool executions (Autellix's BFCL trace: 10.75 calls per program, 735 prefill tokens, 34 decode tokens). Under FIFO, turn 9 of an agent that has nearly finished waits behind turn 1 of every newcomer, so *every* agent's completion time grows with the number of agents. Autellix and Continuum report 4–15x and 1.1–3.7x improvements from program-level scheduling, measured on batching GPU engines. On a serialising Foundry backend throughput will not change. The gain is **mean agent completion time**, because the queue is where all the waiting happens. KVStream must measure this itself, because no published study tests it at a black-box proxy.

### 6.2 Policy

Ordering key, compared lexicographically (lower goes first):

```
( class_rank,                 # critical=0, standard=1, sheddable=2
  promoted ? 0 : 1,           # anti-starvation: wait/service ratio exceeded β
  session_arrival_time,       # program-level FCFS: a call inherits its session's first-seen time
  sjf_score if regime == serialising else 0,   # optional, with aging
  arrival_time )              # tie-break
```

* **Session-level admission.** A session is *active* from its first admitted call until it has been idle for `session_idle_seconds` (default 120 s, which covers tool-call pauses). When the system is saturated, a call from an active session outranks a call that would start a new session within the same class. Shedding refuses **new sessions first** (P3).
* **β-promotion.** A queued call whose `wait / expected_service` exceeds β (default 4) is promoted, so no session or large request can be starved. This keeps the guarantee FIFO was chosen for (README → Fair admission) without its head-of-line cost.
* **SJF with aging** is enabled only when calibration found a `serialising` runtime. There, order is the only lever, and shortest-first minimises mean wait. It uses the exact prompt count (§7.3) plus a per-session EWMA of output length.
* **Deadlines.** A queued call whose `now + predicted_wait + expected_service > deadline` is refused immediately with 503, and is re-checked on every recheck tick (CoDel-style time-in-queue control).
* **Fairness across clients** (VTC-style per-session token counters charged at settle) is deferred until several users share one box. Most single-machine deployments have one user and many agents, where session ordering is the right abstraction.

### 6.3 Interface

```python
class QueuePolicy(Protocol):
    def key(self, w: Waiter, now: float) -> tuple: ...       # ordering key above
    def on_admit(self, w: Waiter) -> None: ...                # session bookkeeping
    def on_settle(self, session: str | None, tokens: int, seconds: float) -> None: ...
    def shed_order(self, waiters: list[Waiter]) -> list[Waiter]: ...  # who goes first when shedding
```

`CapacityManager` keeps reservation, settlement, release and the drain-rate predictor. It replaces `deque` with a heap keyed by `QueuePolicy.key`, and draining becomes "admit from the head while the head fits". `FifoPolicy` (key = arrival time) reproduces today's behaviour exactly and stays the default until the trace-replay benchmark shows a win.

---

## 7. Capacity: how much work fits

### 7.1 Limit controller

```python
class LimitController(Protocol):
    def limit(self) -> int: ...                  # current budget (requests or KV tokens)
    def observe(self, sample: ServiceSample) -> None: ...   # tokens, seconds, outcome
```

| Controller | Source | When | Status |
|---|---|---|---|
| `Static` | `admission.max_concurrency` | no calibration | [exists] |
| `Calibrated` | sweep record keyed by (model, device, CLI generation, Foundry version) | exact or partial calibration match | [exists] |
| `Adaptive` | seconds-per-token gradient seeded and clamped by calibration | opt-in | [planned] |

**Adaptive controller (author's design; no primary-source precedent).** Latency-gradient limiters (Netflix Gradient2, Envoy adaptive concurrency) treat latency growth as congestion, so a 4,000-token answer would look like overload. Normalise by work instead: the signal is generated **seconds per token**, which `admission/drift.py` already measures. Each window:

```
gradient = clamp(baseline_spt * (1 + tolerance) / recent_spt, 0.5, 1.0)
limit    = clamp(round(limit * gradient + headroom), 1, calibrated_max)
```

There is no periodic minRTT probe window (Envoy documents that this causes 503s). The baseline refreshes only when the queue is empty and a request ran alone. On a serialising runtime this converges to 1–2 and mainly tracks **long-session degradation**, the failure the fixed budget missed on real hardware.

### 7.2 Regime and device defaults

Calibration classifies the runtime (`admission/regime.py`), and the defaults follow from that:

| Regime | Mode | Queue policy | Adaptive | Main lever |
|---|---|---|---|---|
| serialising (measured: Foundry 0.8.119 on CPU fallback, **and** Foundry 0.10.3 on CUDA, §19) | `concurrency`, limit 1 | session + SJF/aging | track degradation only | ordering, cache, coalescing |
| batching (possible with ORT GenAI ≥0.16 continuous batching, unverified in Foundry's server) | `tokens` | session | on | protect the throughput knee |
| capped | `concurrency` at the cap | session | off | convert refusals into queueing |

**Why the limit should be 1 on a serialising runtime.** Measured on Foundry 0.10.3 (§19), the runtime *time-slices* concurrent requests: with 8 in flight, mean latency equals max latency, and all 8 finish together at ~7.9 s. Admitting one at a time lets requests finish in turn. That cut mean latency at 8 concurrent from ~7.5–8.4 s to ~4.6–4.8 s (≈40%), with ~5–7% lower throughput and worst-case latency unchanged. For agents the gain compounds, because a turn that finishes earlier starts the agent's next step earlier. The shipped uncalibrated default (`max_concurrency: 8`) measured the same as no gateway, so on a serialising runtime the calibrated budget matters, and that default should be reconsidered.

Device notes: **NPU** loads have crashed the server (Foundry #829), so readiness must confirm the model is loaded before admitting. On CLI 0.10.3 that means the readiness probe itself or `foundry cache list --output json`. `/openai/loadedmodels` returns 404 there, and `/v1/models` lists downloaded models whether loaded or not (verified live). **GPU** VRAM is the hard ceiling for KV, and the token budget is the natural unit. **CPU** decode is memory-bandwidth bound and almost always serialising.

Calibration is invalidated when the key changes (Foundry version, CLI generation, device). A Foundry upgrade must trigger re-calibration advice in `/status`, because ORT GenAI's new batching engine could change the regime overnight.

### 7.3 Costing

1. **Today:** a conservative character/word heuristic, self-calibrated from backend `usage` where reported. [exists]
2. **Planned:** Foundry's `/v1/chat/completions/tokenizer/encode/count` for an exact prompt count, cached by a prefix hash of the message list so an agent's growing transcript is counted incrementally. Its latency and its contention with generation on a serialising runtime must be measured before it is enabled by default.

---

## 8. Dispatch and streaming

### 8.1 Ownership

Every streamed generation is an **`UpstreamRun`**: a gateway-owned `asyncio.Task` that reads the backend stream, does live accounting, and publishes SSE frames into a `StreamBroadcast`. Every client response, the original requester's included, is a **subscriber** of that broadcast.

```
 client A ─┐                       ┌─────────── UpstreamRun task (owned by Gateway) ───────────┐
 client B ─┼─► broadcast.follow() ◄┤ backend stream → account → frames → publish → finalize    │
 client C ─┘   (replay + tail)     └────────────────────────────────────────────────────────────┘
```

Rules:

* A subscriber leaving never affects the others.
* When the **last** subscriber leaves before completion, the run is **cancelled**. That closes the upstream connection and frees a serialising backend for the next request, instead of generating text nobody will read.
* `data: [DONE]` is published **only** when the upstream stream ended normally. On upstream failure or cancellation, subscribers receive an SSE error frame and the stream closes **without** `[DONE]`. A client that sees `[DONE]` can trust it holds the whole response.
* Finalize (release, settle, breaker outcome, cache store, coalescer unregister) runs in the task's `finally`, independent of any client connection.

### 8.2 Pre-flight

The first chunk is awaited **before** HTTP 200 is committed, so a backend failure at start becomes a real status code (4xx passthrough or 502). After headers are sent, failures can only be signalled in-band as an SSE error frame.

### 8.3 Timeout hierarchy [planned]

| Timeout | Default | Trips breaker? | Why |
|---|---|---|---|
| connect | 5 s | yes | Foundry not listening (port moved) → triggers re-discovery |
| time-to-first-token | `min(60 s, deadline)` | yes | detects stuck prefill and NPU hangs long before 120 s |
| inter-token idle | 30 s | yes | detects a mid-generation stall |
| total | `backend.timeout_seconds` or deadline | no | bounds pathological generations |

Today a single httpx timeout (120 s) applies to connect and to each read.

### 8.4 History: defect fixed by this design

Before this change the original requester's HTTP connection owned the upstream generation. When that client disconnected, the generator's `finally` published `[DONE]` to coalesced followers, so they received a truncated response that looked complete. For an agent parsing tool-call JSON this is the worst kind of failure: silent. It is fixed by §8.1, and regression tests cover it.

---

## 9. Failure model and error taxonomy

| Condition | Status to client | Counts against breaker | Notes |
|---|---|---|---|
| malformed request (KVStream validation) | 400 | no | OpenAI envelope |
| **backend 4xx** (bad model alias, bad params) | **backend's own status and body** | **no** | A client error says nothing about backend health. |
| **streamed request for a model Foundry does not have, or has not loaded** | 400 `model_not_found` or `model_not_loaded` | **no** | Foundry 0.10.3 answers both with `200 text/event-stream` and a dropped connection, which is indistinguishable from a crash. KVStream checks the model inventory before dispatch: downloaded (`/v1/models`) and loaded (`foundry cache list`). A numeric `:N` suffix is ignored, and a miss is re-checked against a fresh reading. If a stream still fails before its first token, the inventory is re-read before the failure is blamed on the backend, which covers an unload inside the 30 s cache window. Fails open when the lists are unavailable. |
| backend 5xx | 502 | yes | |
| connect error / timeout / TTFT / idle timeout | 502 (pre-flight) or SSE error frame | yes | |
| breaker open / supervisor `STALLED` | 503 + `Retry-After` (cooldown remaining) | — | fail fast, no queueing |
| queue full | 503 + `Retry-After` | no | |
| predicted wait > deadline or timeout | 503 + `Retry-After` (predicted wait) | no | decided on arrival or at recheck |
| admission timeout | 503 + `Retry-After` | no | |
| shutting down | 503 | no | distinct message |
| client disconnected | — | no | run cancelled if it was the last subscriber |

**503 is reserved for capacity.** If per-client quotas are ever added they use 429, matching APIM's convention, so north gateways can tell "this box is full" from "you are over your allowance".

### 9.1 Breaker semantics

* Only backend failures count (table above).
* **Half-open admits exactly one trial, and the trial is always resolved.** It is resolved as success, failure, or *released without a verdict* when the trial never reached the backend or ended in a client error. A trial that is never resolved would keep the breaker half-open forever, refusing every request. This was a real defect, now fixed.
* Planned: a percentage threshold with `minimum_requests` (Portkey's pattern), and honouring a backend `Retry-After` when Foundry sends one.

---

## 10. Runtime supervisor

The supervisor consolidates what `health.py`, `drift.py` and the discovery cooldown each do today into one explicit state machine. Admission consults its state rather than separate booleans.

```
            discover ok              model loaded + probe ok
 UNKNOWN ───────────────► LOADING ──────────────────────────► READY ◄──────────────┐
    ▲  discover fail          │ load fail / NPU crash             │                  │
    │                         ▼                                   │ drift > warn     │ probe ok,
    └──────────────────── UNREACHABLE ◄──────── port gone ────────┤ or RSS > soft    │ drift normal
                              ▲                                   ▼                  │
                              │                               DEGRADED ──────────────┘
                              │                                   │ breaker open, probe timeout,
                              │      restart ok                   │ or RSS > hard
                              └──────── RECOVERING ◄──── STALLED ◄┘
                                   (opt-in supervised restart)
```

| State | Admission behaviour |
|---|---|
| READY | normal |
| DEGRADED | adaptive limit shrinks; new sessions shed first; `/health` still 200 with a warning |
| STALLED | fail fast 503 with `Retry-After` = expected recovery; `/health` 503 |
| RECOVERING | queued work is refused with `Retry-After`; in-flight work is drained |
| UNREACHABLE / UNKNOWN | rediscover (with cooldown); fail fast |

**Memory watchdog [planned].** On the measured machine the runtime held 13.4 GB resident for a 2.13 GB model and never recovered. Open Foundry issues report memory growth (#861), unload retaining 98–99% of memory (#1079) and leaks (#1034). Because KVStream is co-located, it *can* read the Foundry process's RSS (resolve the PID from the listening port) and, where available, per-process GPU memory. Soft threshold → DEGRADED. Hard threshold → STALLED.

**Supervised restart [planned, opt-in].** Restart rather than `/openai/unload`, because of #1079. It is opt-in because it is disruptive, and bounded by a timeout with escalation, because on 0.8.119 `foundry service restart` itself hung. Foundry's OpenAI API is stateless across requests, so a restart loses no server-side conversation state. Only in-flight generations are lost, and those are drained first.

**Warm-keeping [planned].** Foundry auto-unloads idle models. Re-issue a load with a TTL so an agent run never pays a cold load mid-session. The loaded-model list distinguishes *unloaded* (recoverable by loading) from *stalled*. On CLI 0.8.x these are `/openai/load/{model}?ttl=` and `/openai/loadedmodels`. On CLI 0.10.3 `/openai/loadedmodels` returns 404 and `/v1/models` lists downloaded models whether loaded or not, so use `foundry model load` and the `loaded` flag from `foundry cache list --output json`. That command starts a stopped daemon, so it may only be called while Foundry is already running. Each CLI generation needs its own adapter, verified live before relying on it.

**Owned-process mode [planned].** With Foundry SDK 2.x the REST server can live inside a process KVStream starts and supervises. This gives an exact PID, a deterministic port, and restart without the CLI.

---

## 11. Cache, coalescing and idempotency

| Mechanism | Eligibility | Key | Semantics change? | Status |
|---|---|---|---|---|
| Response cache | `temperature: 0`, `n == 1`; opt-in | SHA-256 of the client JSON (includes `stream`, `tools`) | yes, so off by default | [exists] |
| Coalescing | same as cache; on by default | same | no: concurrent identical deterministic requests | [exists] |
| Idempotency | any request with `Idempotency-Key` | the key, plus a body hash to reject key reuse with a different body (422) | no: it is the client's own retry | [planned] |

* Followers consume no admission budget.
* **Non-streaming followers survive a cancelled leader.** If the leader's task is cancelled, followers take over and one of them becomes the new leader. They are never left awaiting a result that will not come (defect fixed). A follower's own cancellation never cancels the shared result.
* **Semantic caching is deliberately not built.** Vendors warn it can return "incorrect, outdated, or unsafe" answers, defaults embed only the last message (unsafe for tool calls), and an embedding model would compete with the main model for the same CPU/GPU/NPU.

---

## 12. Integration contract

KVStream integrates as a **custom OpenAI-compatible provider** behind any north gateway. Retries happen at one layer only: the agent SDK or the north gateway, never both and never KVStream.

| North gateway | How it reacts to KVStream's 503 + Retry-After | Recommended setting |
|---|---|---|
| Azure APIM (self-hosted) | breaker trip duration taken from `Retry-After` | defaults are correct |
| LiteLLM | cooldown after `allowed_fails` (3/min) for 5 s, ignores Retry-After | raise `allowed_fails`; count 503 as rate-limit |
| Portkey | counts >500 by default; minimum cooldown 30 s | exclude 503 from breaker status codes |
| agentgateway | outlier detection evicts throttled backends; Retry-After handling unverified | test before recommending |

Reference configs for each are a Phase 4 deliverable, each tested end-to-end with shed events. MCP and A2A stay upstream. The only thing KVStream borrows from MCP gateways is the session id.

---

## 13. Concurrency model and invariants

KVStream runs on **one asyncio event loop in one process**. All admission state is mutated under `CapacityManager._lock`. Breaker and coalescer state is mutated without awaits between read and write, which is safe on a single loop. Do not introduce threads that touch this state.

Invariants. Each one has, or must get, a regression test:

1. **Every reservation is released exactly once**, on success, failure, client disconnect, cancellation and shutdown. (`release` is idempotent; `finalize` always runs.)
2. **Every breaker trial is resolved**: success, failure, or released without a verdict.
3. **A client error never trips the breaker**, and the client receives the backend's own 4xx. This includes the streamed unknown-model case, which Foundry 0.10.3 does not report as a 4xx.
4. **`[DONE]` is sent only for a complete response.**
5. **No client's cancellation cancels work another client depends on.** Shared work is gateway-owned. Shared futures are awaited through `asyncio.shield`.
6. **Followers and cache hits consume no admission budget.**
7. **A queued request either gets admitted or is refused before its deadline expires.** It never waits past its deadline.
8. **Background tasks are strongly referenced** until they finish, so the garbage collector cannot drop them mid-generation.
9. **One process.** Multiple workers are refused at startup.
10. **Shutdown turns queued requests away when it starts**, not after uvicorn has waited for connections, and never lets a queued request through without a reservation (`server.GatewayServer`, `CapacityManager.admit`).

---

## 14. Observability

* **Prometheus** [exists]: admission, queue, waits, rejections by reason, breaker, readiness, drift, cache, coalescing, overshoot and reclaim.
* **Planned:** vLLM-compatible aliases (`num_requests_waiting`, `num_requests_running`, estimated KV utilisation), so north gateways and dashboards built for vLLM can read KVStream's load; per-session and per-class wait histograms; supervisor state gauge; Foundry process RSS gauge.
* **OTel GenAI spans [planned]:** one span per request, with child spans for queue, pre-flight and stream. Attributes include session, priority, cost, reservation, outcome and estimated-vs-actual tokens. This lines up with agentgateway traces when it sits in front.
* `/status` stays the operator's single page: budget provenance, regime, supervisor state, queue by class.

---

## 15. Security posture

KVStream binds to loopback and has no authentication. That remains the default, because the north gateway owns identity. Planned: an optional static bearer token for direct agent access, and refusal to bind a non-loopback address without one. `/status` and `/metrics` stay internal. Release hygiene follows the LiteLLM 2026 supply-chain incident: pinned dependencies, PyPI trusted publishing, no long-lived tokens in CI.

---

## 16. Testing strategy

| Layer | What | Where |
|---|---|---|
| Unit | policies, accountant, breaker, coalescer, broadcast | `tests/unit` |
| Integration | full app against stub backends (stalls, 4xx, 5xx, disconnects) | `tests/integration` |
| Fault injection | leader disconnect, cancel mid-queue, half-open trial lost, 4xx storm | `tests/integration` (regression for §13) |
| **Live Foundry** | smoke + overload + disconnect against the installed Foundry Local; skipped when absent | `tests/live` [planned], `kvstream bench` |
| Soak | multi-hour agent replay; RSS and drift over time | `benchmarks/` [planned] |
| Trace replay | BFCL / SWE-bench-shaped agent traces: FIFO vs session policy, mean and p95 *program* completion time | `benchmarks/` [planned] |

A policy change (e.g. making the session policy the default) ships only with a trace-replay result against real Foundry Local.

---

## 17. Roadmap

| Phase | Theme | Contents | Exit criterion |
|---|---|---|---|
| 0 | Correctness | 4xx passthrough; breaker trial resolution; coalescer cancellation; gateway-owned streams; regression tests | §13 invariants 1–5 tested |
| 1 | Agent-safe I/O | timeout hierarchy; deadline header; Idempotency-Key; pipeline split of `app.py`; queue-ms and cache headers | hung prefill detected within the TTFT budget; disconnect frees backend |
| 2 | Session-aware admission | `QueuePolicy` seam; priority classes; sessions; β-promotion; SJF on serialising runtimes; exact costing | measured drop in mean/p95 program completion time vs FIFO at equal throughput |
| 3 | Foundry operations | supervisor state machine; memory watchdog; supervised restart; warm-keeping; `/openai/status` discovery; SDK 2.x owned-process mode; adaptive limit; re-calibration on version change | multi-hour soak without a permanent stall |
| 4 | Ecosystem | OTel spans; vLLM-style metrics; tested reference configs for agentgateway, APIM, Bifrost, TensorZero, LiteLLM; published overhead benchmark | each config verified with shed events |

---

## 18. Open questions

* CLI 0.10.3 (Foundry Local Core 1.0.0, ORT 1.26.0) still serialises on CUDA (§19), so it does not batch concurrent requests. Open: does a later SDK 2.x / ORT GenAI ≥0.16 build batch, and does any version keep prefix KV across requests? If one does, session ordering also wins prefix reuse, and the regime must be re-measured.
* How fast is the token-count endpoint, and does it contend with generation on a serialising runtime?
* Does agentgateway honour `Retry-After`, and what is its memory footprint on a laptop?
* What `session_idle_seconds` covers real tool-call latency without holding a session slot for abandoned agents?

---

## 19. Live verification (2026-10-07)

Run against the Foundry Local installed on the development machine. This is one machine, one model and one version, so it is evidence, not a general characterisation.

**Environment.** Foundry Local CLI 0.10.3, Foundry Local Core 1.0.0 at serve time, ORT 1.26.0, Windows 10 build 19045, Intel i7-9750H, 32 GB RAM, NVIDIA RTX 2070 Max-Q (8 GB). The CUDA execution provider was downloaded and registered on the daemon's first start, which took several minutes while the daemon reported `Initializing`. Model: `Phi-3-mini-4k-instruct-generic-gpu:2` (2.1 GB).

| Check | Result |
|---|---|
| Discovery, 0.10.x CLI generation (previously verified only against synthetic output) | **works**: CLI 0.10.3 detected, endpoint taken from `foundry status --output json`, `/health` serving after a real 1-token probe (0.5 s) |
| Model ids | `/v1/models` lists `Phi-3-mini-4k-instruct-generic-gpu` (no `:2`); both forms are accepted. The catalog alias `phi-3-mini-4k` gets **HTTP 400** "not loaded". |
| Downloaded vs loaded (2026-10-08) | After `foundry model unload`, `/v1/models` **still lists** the model, a non-streamed request gets 400 "not loaded", and a streamed one gets 200 + a dropped connection. `foundry cache list --output json` flips `loaded` true/false correctly (~100 ms per call, as is `/v1/models`). |
| Model auto-detection (2026-10-08) | `kvstream serve` with no `--model`: adopted `Phi-3-mini-4k-instruct-generic-gpu:2` as "the only model loaded". After an unload, 6 of 6 streamed requests got 400 `model_not_loaded` with the breaker untouched. Started with nothing loaded: `/health` 503 with the reason; right after `foundry model load`, 200 with the model adopted. `kvstream calibrate` with no `--model` auto-detected the same model. |
| Control endpoints | `/openai/loadedmodels` and `/openai/status` return **404** on 0.10.3 |
| `usage` reporting | Foundry 0.10.3 **returns `usage`** on non-streamed and streamed (`include_usage`) responses. The token estimator calibrated from it. |
| Wrong alias, non-streamed | 400 with Foundry's own body; breaker untouched |
| Wrong alias, **streamed** | Foundry sends `200 text/event-stream`, then drops the connection. Before the model check: 502 and a breaker failure each time. After: 400 `model_not_found`, breaker untouched (7 of 7 requests). |
| Original client disconnects from a coalesced stream | follower received all 113 frames ending in `[DONE]`, no error frame |
| Last subscriber leaves a 900-token stream | reservation released immediately. The next request took 0.15 s against a 0.14–0.15 s idle baseline. That is consistent with the upstream generation stopping, but does not prove Foundry stops computing. |
| Concurrency sweep, direct to Foundry, 96-token answers | 1 → 93.5, 2 → 97.7, 4 → 99.6, 8 → 97.5 generated tok/s; latency linear; **mean = max** at every level, so the runtime is **serialising and time-sliced** |
| 8 concurrent, unique prompts, 2 runs each | direct: mean 7.50 / 8.37 s, max 7.52 / 8.38 s · KVStream budget 8: mean 7.53 / 7.57 s · **KVStream budget 1: mean 4.56 / 4.78 s, max 8.11 / 8.60 s**, throughput 94.7 / 89.3 tok/s |

**Code-review fixes, verified live on 2026-10-11** (same machine, CLI 0.10.3, CUDA):

| Fix | Live result |
|---|---|
| No per-request probe | 5 chat requests → 5 upstream POSTs, 0 `GET /v1/models` |
| Port change | `foundry server restart` moved Foundry from :63801 to :63962. The next request was refused at the old port, re-discovered, retried once and returned 200; breaker closed |
| Shutdown | Ctrl+C equivalent with one 300-token generation in flight and one queued: queued → 503 in 0.09 s, in-flight → 200, no upstream call after shutdown began |
| Callers not shared | identical temp-0 requests: alice + bob → 2 upstream calls; carol + carol → 1 |
| Calibration | model unloaded → exit 1, nothing saved. Loaded → record keyed `…:2\|windows-amd64\|0.10.x\|0.10.3` with `baseline_seconds_per_token`, regime `serialising`, ~40 *generated* tok/s. `serve --mode tokens` then resolved it as an **exact** match |
| Token-mode shedding | 30 concurrent, 3 s timeout: 20 refused on predicted wait (16 within 0.1 s), 2 timed out, 8 served |
| Drift | 25 requests: ratio 1.5–1.6, state ok. The units now match, but the ratio still moves with the prompt/generation mix (prompt-heavy probes vs generation-heavy traffic), so values well below `drift_warn_ratio` are not evidence of degradation |
| Metrics | streamed requests counted in `kvstream_request_seconds`; `kvstream_backend_up 1` with no `/health` call |
| Legacy completions | Foundry 0.10.3 answers `/v1/completions` with 404 (streamed or not); KVStream passes it through with 0 breaker failures. The streaming relay itself is covered by tests only |

Not verified live:
* the non-streaming coalescer's leader-cancellation fix, which needs a client disconnect that cancels the handler (covered by unit tests);
* the half-open trial release during a real outage;
* long-session memory growth.
