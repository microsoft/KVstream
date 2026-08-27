# KVStream — User Guide and SOP

For operators running KVStream in front of Microsoft Foundry Local. Written for
someone who did not build it.

Every procedure here **except the Docker one** was performed against a live
`foundry 0.8.119` with `phi-3-mini-4k` on Windows; SOP 2 is written from
configuration rather than from a run, and says so. Where a step has a known
failure mode, it is written down next to the step rather than left for you to
discover.

---

## Before you start

### What KVStream does

It sits between your application and Foundry Local. Your app talks to KVStream
using the ordinary OpenAI API; KVStream decides how much of that traffic reaches
Foundry Local at once, queues the rest, and refuses what it cannot serve.

### When it helps

- Several clients call one Foundry Local at the same time (agent swarms, RAG
  fan-out, planner loops).
- You need callers to get a clean `503` instead of a hung connection.
- You want to know when the backend has stopped working.

### When it does not

- **A single client making one request at a time.** There is nothing to admit.
  KVStream will cost you a few percent of throughput and give you nothing back.
- **When the backend is not under pressure.** Measured: at a load Foundry Local
  absorbs unaided, the gateway cost ~8% of goodput and returned ~12% off the tail
  latency. That is the whole trade.
- **In-process SDK use.** If your application loads a model in-process and never
  starts an HTTP service, there is no endpoint for KVStream to sit in front of.

### Prerequisites

| | |
|---|---|
| Python | 3.10 or newer |
| Foundry Local | installed, with at least one model in the local cache |
| Disk | KVStream itself is negligible; models are the cost. The two observed here were 2.13 GB (GPU) and 2.53 GB (CPU) variants of phi-3-mini-4k. |
| Network | KVStream connects only to the backend URL you give it; discovery only ever scans localhost. It does not phone home. |

---

## SOP 1 — First run without Docker

### Step 1 — Install

```bash
git clone https://github.com/microsoft/KVstream
cd KVstream/kvstream-foundry
pip install -e .
```

Verify: `kvstream --help` prints the command list.

### Step 2 — Start Foundry Local and load a model

```bash
foundry service start
foundry model run phi-3-mini-4k
```

On Foundry Local CLI 0.10.0 (Preview) the published equivalents are
`foundry server start` and `foundry model load <model>`. **Unverified** — no
0.10.x binary was available to test against, so every 0.10.x instruction in this
guide comes from the published command surface rather than from a run.

Verify: `foundry service status` prints a green line with a URL. **Note the
port** — it changes on almost every restart.

> **If `foundry service start` says it is already running** but nothing works,
> the service may be wedged. See Runbook R4.

### Step 3 — Find the model id the backend answers to

**Do not skip this.** It is the most common cause of a failed first run.

```bash
curl http://127.0.0.1:<port>/v1/models
```

The catalog *alias* is frequently not the id the inference API accepts. On the
reference machine, `phi-3-mini-4k` returned **HTTP 400**, while the id printed by
`/v1/models` — `Phi-3-mini-4k-instruct-generic-gpu:2` — worked.

Copy the `id` value exactly, including any `:2` suffix.

### Step 4 — Start the gateway

```bash
kvstream serve --model "Phi-3-mini-4k-instruct-generic-gpu:2"
```

It binds `127.0.0.1:8080` and finds the backend by itself.

To pin the backend instead of discovering it — required in containers, and
sensible whenever you already know the URL:

```bash
kvstream serve --backend-url http://127.0.0.1:51264 \
               --model "Phi-3-mini-4k-instruct-generic-gpu:2"
```

Setting `--backend-url` **disables discovery entirely**. That is deliberate: an
explicit URL is treated as authoritative.

### Step 5 — Verify before sending real traffic

```bash
curl http://localhost:8080/health
```

You want `"status": "ok"` and **`"backend_serving": true`**.

| Field | Meaning |
|---|---|
| `backend_reachable` | the backend answered `/v1/models`. Proves a process is alive, nothing more. |
| `backend_serving` | the backend completed a real one-token generation. **This is the field that matters.** |
| `hint` | present only when degraded; says what to do |

A stalled Foundry Local keeps answering `/v1/models` in a few milliseconds while
being unable to generate anything. That is why the two are reported separately.

### Step 6 — Send a request

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

`api_key` is required by the OpenAI SDK and ignored by KVStream, which has no
authentication of its own.

> **The first request after loading a model can take several seconds** — the
> runtime initialises on demand. Measured on the reference machine: 4.9–6.5 s
> cold, 0.2–0.8 s once warm. This is
> the backend, not the gateway.

---

## SOP 2 — First run with Docker

> **Not verified.** Docker was not installed on the machine this guide was
> written against, so the image has never been built or run. The `Dockerfile`,
> `docker-compose.yml` and the steps below are written from the configuration
> they set, not from a successful run. Treat this section as a starting point
> and expect to debug it. Everything in SOP 1 was executed end to end.

A container cannot see the host's listening ports and has no `foundry` binary, so
**discovery and the CLI lookup are disabled in the image**. An explicit backend
URL is mandatory.

### Step 1 — Build

```bash
docker build -t kvstream .
```

### Step 2 — Find the backend URL on the host

As in SOP 1 Step 2–3. You need both the port and the model id.

### Step 3 — Run

```bash
docker run --rm -p 8080:8080 \
  --add-host host.docker.internal:host-gateway \
  -e KVSTREAM_BACKEND__BASE_URL=http://host.docker.internal:51264 \
  -e KVSTREAM_BACKEND__MODEL="Phi-3-mini-4k-instruct-generic-gpu:2" \
  kvstream
```

`--add-host` is needed on Linux. Docker Desktop resolves `host.docker.internal`
already.

Or with compose, after editing the URL and model in `docker-compose.yml`:

```bash
docker compose up --build
```

### Step 4 — Verify

```bash
curl http://localhost:8080/health
```

Docker's own `HEALTHCHECK` uses the same endpoint, so `docker ps` will show the
container as unhealthy when the backend stops serving.

### Standing caveat

**The backend port changes on every Foundry Local restart, and the container
cannot follow it.** After restarting Foundry Local you must update
`KVSTREAM_BACKEND__BASE_URL` and restart the container. There is no way around
this from inside a network namespace.

Configuration is by environment variable (`KVSTREAM_` prefix, `__` for nesting).
To use a file instead:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/kvstream.yaml:/home/kvstream/kvstream.yaml:ro" kvstream
```

---

## SOP 3 — Daily operation

### Starting

```bash
kvstream serve --config kvstream.yaml > kvstream.log 2>&1 &
```

Redirecting output is safe on Windows; the CLI widens stdout and stderr to UTF-8
at startup.

**Run exactly one process.** Admission state lives in memory, so two processes
enforce two budgets and the calibration means nothing. KVStream refuses to start
if `WEB_CONCURRENCY`, `UVICORN_WORKERS`, `GUNICORN_WORKERS` or
`KVSTREAM_WORKERS` is above 1.

### Checking

```bash
kvstream health          # gateway + backend, one line each
kvstream status          # live admission table
curl localhost:8080/status | python -m json.tool   # everything
```

Four fields answer most questions:

| Field | Read it as |
|---|---|
| `backend_health.readiness.ready` | can the backend actually generate? |
| `backend_health.circuit_breaker.state` | `closed` normal, `open` failing fast |
| `admission.queue.depth` | how much work is waiting |
| `budget_source.source` | where the admission limit came from |

### Stopping

Send `SIGTERM` (`Ctrl-C`, `docker stop`, systemd). The gateway stops admitting,
turns away anything still queued, and gives in-flight requests
`drain_timeout_seconds` (default 30) to finish.

Do not `kill -9` unless it has stopped responding — you will cut off in-flight
responses that were about to complete.

### After restarting Foundry Local

1. The port has changed. If you pinned it with `--backend-url`, update it.
   If you did not, KVStream will re-discover it within
   `discovery_cooldown_seconds`.
2. Re-check `/health` for `backend_serving: true` before resuming traffic.
3. If you use token mode, the calibration record is still valid **only if** the
   model, device and Foundry version are unchanged.

---

## SOP 4 — Switching to token mode

Default is `concurrency` mode — a plain cap on concurrent requests. It works with
no setup. Token mode admits by each request's estimated KV footprint, which is
better when your request sizes vary a lot.

### Step 1 — Calibrate

With Foundry Local running and the model loaded:

```bash
kvstream calibrate --model "Phi-3-mini-4k-instruct-generic-gpu:2" --device npu
```

This sends a rising load to your backend and finds the point where it starts to
degrade. Do it when the machine is otherwise idle. How long it takes depends
entirely on your backend — the default sweep climbs to concurrency 32 with three
trials per point, so on a slow backend it is minutes, not seconds. Lower
`--max-concurrency` and `--trials` to shorten it.

`--device` is a label **you** choose (`npu`, `cpu`, `arc-a770`). KVStream cannot
see which accelerator Foundry Local picked. Use the same label every time or the
record will not match.

Useful flags: `--trials N` (repeats per point, default 3), `--warmup N`
(default 1), `--no-refine` (skip the bisection step), `--max-concurrency N`
(default 32).

### Step 1b - Read what it found

Calibration reports the *shape* of your runtime, not just a number:

```bash
curl -s localhost:8080/status   | python -c "import json,sys; print(json.load(sys.stdin)['runtime_profile'])"
```

| `regime` | What to expect from the gateway |
|---|---|
| `serialising` | Admission control will **not** raise your throughput. You get backpressure, readiness and shedding. Turn the cache on and keep coalescing on - removing work is the only thing that adds capacity here. |
| `batching` | Admission control protects the throughput peak. Token mode is worth using. |
| `capped` | Admission control turns refusals into queueing. This is where the gateway helps most. |
| `unknown` | Not enough of the sweep completed. Re-run with a higher `--max-concurrency`. |

This matters because it tells you whether to expect a throughput win at all. On
the reference machine the answer was `serialising`, and the honest expectation
there is *better failure behaviour, not more requests per second*.


### Step 2 — Enable it

```bash
kvstream serve --mode tokens --model "Phi-3-mini-4k-instruct-generic-gpu:2"
```

### Step 3 — Confirm it is actually in use

```bash
curl -s localhost:8080/status | python -c "import json,sys; print(json.load(sys.stdin)['budget_source'])"
```

`source` must read `calibration:exact` or `calibration:partial`.

**If it reads `fallback:concurrency`, token mode is not running.** KVStream
refuses to use a budget measured in a different environment, because a budget
from other hardware is worse than none — it is confidently wrong. The log line
says which axis did not match. The usual cause is a `--device` label that differs
from the one used at calibration time.

> **Honest limitation.** Token mode has been exercised against a modelled runtime
> and in unit tests, but not against real hardware — every real-hardware run in
> this project ended up in concurrency mode for exactly the reason above. Treat
> token mode as the less-travelled path.

---

## SOP 5 — Runbook

### R1 — Every request returns 400, but `/v1/models` works

**Cause:** the model id is wrong. You are almost certainly using the catalog
alias.

**Fix:** `curl http://127.0.0.1:<port>/v1/models` and use the `id` field exactly.

### R2 — The gateway cannot find Foundry Local

**Symptom:** startup logs "Foundry Local is not reachable"; `/health` returns 503
with `backend_reachable: false`.

**Check, in order:**

1. `foundry service status` — is it running at all?
2. Is a model loaded? `curl http://127.0.0.1:<port>/v1/models` returning
   `"data": []` means the service is up but empty.
3. `curl -s localhost:8080/status` and read `backend.resolution` — it names which
   step of the chain was used and what was tried.

**Fix:** start the service, load a model, or pin the URL with `--backend-url`.
The error message names the correct start command for your CLI generation.

### R3 — `/health` says `backend_reachable: true` but `backend_serving: false`

**Cause:** the backend is alive but cannot generate. This is the failure mode the
readiness probe exists for.

**Check:** the `hint` and `readiness.detail` fields. Two common cases:

- *"backend rejected a 1-token generation"* — usually a wrong model id
  (see R1), or the model was unloaded.
- *"backend did not complete a 1-token generation within Ns"* — the runtime has
  stalled. Go to R4.

### R4 — Foundry Local has stalled

**Symptom:** inference hangs indefinitely; `/v1/models` still answers instantly;
the inference process holds many gigabytes.

**This is a Foundry Local condition, not a KVStream one.** Observed on the
reference machine after sustained heavy load: a 4-token request did not return in
180 seconds while the process held 13.4 GB for a 2.13 GB model. It did not
recover on its own.

**Fix:**

```bash
foundry service restart
foundry model run <your-model>
```

The restart may take a minute and may itself appear to hang. The port will change
afterwards.

**Prevention:** reduce sustained concurrency, or restart Foundry Local
periodically under heavy use. KVStream will *detect* this state and stop sending
traffic into it, but it does not prevent it.

### R5 — Everything returns 503 immediately

**Cause:** the circuit breaker is open. The backend failed
`circuit_breaker_failures` times in a row (default 5), so the gateway is failing
fast instead of queueing behind it.

**Check:** `circuit_breaker.state` and `circuit_breaker.last_error` in `/health`.

**Fix:** fix the backend (usually R4). The breaker retries one request
automatically after `circuit_breaker_reset_seconds` (default 30) and closes
itself when that succeeds. No action needed on the gateway.

### R6 — Everything returns 503 after a long wait

**Cause:** requests are queueing and timing out. `admission_timeout_seconds`
defaults to **120 s**, which is far longer than most callers will wait.

**Fix:** lower it to something your clients would actually tolerate:

```yaml
admission:
  admission_timeout_seconds: 20.0
```

KVStream also refuses requests early when it can measure that the queue will not
reach them in time. Measured on the reference machine with a 20 s timeout: shed
load came back at a **median of 8.1 s**, against a flat 120.1 s before that
mechanism existed. It is bounded by your timeout, not instant — the fastest
refusals were 0.1 s, but do not expect the median to be one of them.

### R7 — 503s with `Retry-After`, under load, and the backend is healthy

**This is normal.** It is admission control working: more work arrived than the
backend can take. Honour `Retry-After` and retry.

If it happens at loads you expect to be fine, your budget is too low. Check
`admission.budget` and re-calibrate, or raise `max_concurrency`.

### R8 — The gateway refuses to start

**Symptom:** `RuntimeError: ... KVStream must run as a single process`.

**Cause:** `WEB_CONCURRENCY` or an equivalent is set above 1.

**Fix:** unset it. Admission state is per-process; N workers would enforce N
budgets and void your calibration. Run one KVStream per Foundry Local.

### R9 — Responses are slower through the gateway

Partly expected. Under overload requests *wait* instead of being refused, so
per-request latency rises while the success rate rises with it.

If the backend is **not** overloaded, the gateway costs a little for nothing —
measured at ~8% of goodput. That is the honest trade, and if your load never
saturates the backend, KVStream may not be worth running.

Check `admission.queue.depth`: consistently 0 means you are not overloaded.

### R10 — `token_estimator.samples` stays at 0

**Cause:** your backend does not report token counts. Foundry Local 0.8.119 does
not.

**Effect:** the token estimator never calibrates, so its uncalibrated accuracy
applies. Cost estimates skew high, which is the safe direction.

**Note:** `backend.usage_reporting: true` in `/status` means *KVStream is asking*,
not that the backend answers. `token_estimator.samples` is the field that tells
you the truth.

---

## What to watch

If you scrape Prometheus, these five carry most of the signal.

| Metric | Watch for |
|---|---|
| `kvstream_backend_ready` | `0` — the backend cannot generate. Page on this. |
| `kvstream_circuit_breaker_state` | `2` (open) — the gateway has given up on the backend |
| `kvstream_rejected_total{reason=...}` | rising `timeout` means your admission timeout is too high; rising `queue_full` means sustained overload |
| `kvstream_budget_utilization` | pinned at 1.0 means you are permanently saturated |
| `kvstream_backend_drift_ratio` | above `drift_warn_ratio` means the backend is slower than when you calibrated |

Rejection reasons you will see: `queue_full`, `timeout`, `predicted_wait`,
`circuit_open`, `too_large` (audio uploads), `shutting_down`.

---

## Configuration cookbook

Config lives in `kvstream.yaml` in the working directory, or `--config <path>`.
Every field is also an environment variable: `KVSTREAM_ADMISSION__MODE=tokens`.
Precedence is CLI flags → environment → YAML → defaults. See
`kvstream.example.yaml` for the annotated full set.

### Interactive clients that will not wait

```yaml
admission:
  admission_timeout_seconds: 15.0
  max_queue_depth: 50
```

### A fixed backend URL (containers, or a pinned deployment)

```yaml
backend:
  base_url: http://127.0.0.1:51264
  model: "Phi-3-mini-4k-instruct-generic-gpu:2"
  use_foundry_cli: never
```

### Repetitive agent traffic

```yaml
cache:
  enabled: true          # off by default; changes response semantics
  ttl_seconds: 900
coalesce:
  enabled: true          # already the default
```

Both only apply to requests with an explicit `temperature: 0` and `n: 1`.

### Never under-estimate a request's size

```yaml
admission:
  token_safety_factor: 1.25
```

Measured to eliminate under-counting on the benchmark corpus. Estimates are
scaled by 1.25, so roughly 20% less work fits in the same budget. Only relevant
in token mode.

### Quieter health probing

```yaml
backend:
  readiness_interval_seconds: 60.0
  probe_readiness: false      # falls back to liveness only — not recommended
```

The readiness probe is a real one-token generation. It is cached and
single-flighted, but it is not free.

---

## Command reference

| Command | Purpose | Key flags |
|---|---|---|
| `kvstream serve` | run the gateway | `--config --host --port --model --backend-url --mode --max-concurrency` |
| `kvstream health` | gateway + backend health | `--url` |
| `kvstream status` | live admission table | `--url` |
| `kvstream calibrate` | measure the token budget | `--model --device --trials --warmup --refine/--no-refine --max-concurrency` |
| `kvstream bench` | send load, report latency | `--url --model --concurrency --total --max-tokens` |

### Endpoints

| Endpoint | Notes |
|---|---|
| `POST /v1/chat/completions` | streaming and non-streaming, including tool calling |
| `POST /v1/completions` | legacy |
| `POST /v1/embeddings` | admitted on the token budget |
| `POST /v1/audio/transcriptions` | separate concurrency limit; upload cap applies |
| `GET /v1/models`, `GET /v1/models/{id}` | proxied |
| `GET /health` | 503 unless the backend is serving |
| `GET /status` | full operational state |
| `GET /metrics` | Prometheus |

Errors use the OpenAI envelope, so an OpenAI SDK client can read them. `502`
means the backend failed; `503` means KVStream is refusing, and carries
`Retry-After`.

---

## Limits worth knowing before you rely on it

- **No authentication, no TLS.** Binds to `127.0.0.1`. Put an authenticating
  reverse proxy in front of it before exposing it anywhere.
- **`/status` and `/metrics` disclose operational detail.** Treat them as
  internal.
- **One gateway, one Foundry Local.** No multi-machine routing.
- **It does not prevent a runtime from exhausting itself** over a long session.
  It detects and reports that condition; it cannot stop it.
- **Token mode is the less-tested path** (see SOP 4).
- **Exact-match caching only.** No semantic cache.

For the full picture, including what is deliberately not implemented, see
[`CHANGELOG.md`](../CHANGELOG.md) and [`docs/GAP-ANALYSIS.md`](GAP-ANALYSIS.md).
