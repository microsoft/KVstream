# Local / Single-Machine Inference Servers and Local Proxies: Concurrency, Queueing, Overload (as of 2026-10-07)

Scope note: research budget was ~15 tool calls. Primary sources (GitHub raw docs, GitHub REST API for issues/releases, learn.microsoft.com) were preferred. GitHub issue/release content was retrieved through the GitHub API and summarised by a fetch model; issue numbers/titles should be spot-checked before quoting verbatim in a final report. Items marked [UNVERIFIED] come from aggregator/secondary sources only.

## Q1. Which local servers expose engine metrics a gateway could use for saturation detection?

### Takeaway
llama.cpp `llama-server` is by far the richest: Prometheus `/metrics` (opt-in via `--metrics`) including `requests_processing` and `requests_deferred` (a direct queue-depth gauge), a `/slots` endpoint with a `?fail_on_no_slot=1` 503 probe, and `/health` (503 while loading). Ollama exposes only `/api/ps` (loaded models, `size_vram`, `expires_at`) and no native Prometheus endpoint in its documented API. Foundry Local exposes no metrics endpoint at all; only model-state endpoints (`/openai/loadedmodels`, `/openai/status`) - so a sidecar must infer saturation from its own latency/inflight accounting plus OS process metrics (RSS).

### Cited Findings
**llama.cpp server**
- `GET /metrics` requires `--metrics`; exports `llamacpp:prompt_tokens_total`, `llamacpp:tokens_predicted_total`, `llamacpp:prompt_seconds_total`, `llamacpp:tokens_predicted_seconds_total`, `llamacpp:requests_processing`, `llamacpp:requests_deferred`, and speculative-decoding counters — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- `GET /slots` returns per-slot state (throughput, token counts, sampling params); `?fail_on_no_slot=1` returns HTTP 503 if no slot is available — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- `GET /health` returns 503 "Loading model" during init, 200 `{"status":"ok"}` when ready — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)

**Ollama**
- `/api/ps` returns per loaded model: `name`, `model`, `size`, `digest`, `details`, `expires_at`, `size_vram`; no dedicated metrics endpoint documented in the API reference — [Ollama docs/api.md](https://raw.githubusercontent.com/ollama/ollama/main/docs/api.md) (note: Ollama has been migrating docs to .mdx; this file may lag current behaviour)
- Third-party exporters (sidecar or proxy style) poll `/api/ps` on each scrape to produce Prometheus metrics; proxy-style exporters must sit in the request path to get per-request metrics — [NetEye blog, 2026-04-02](https://www.neteye-blog.com/blog/2026/04/02/monitoring-ollama-in-neteye-with-ollama-metrics-and-check_prometheus/); [evandhoffman/ollama-prometheus-exporter](https://hub.docker.com/r/evandhoffman/ollama-prometheus-exporter)
- [UNVERIFIED] A search snippet claims an `OLLAMA_ENABLE_METRICS=true` setting exists; not found in the official FAQ or API doc fetched — [eastondev blog](https://eastondev.com/blog/en/posts/ai/ollama-production-monitoring/)

**llama-swap**
- `/metrics` exposes "system and GPU metrics for prometheus"; `/running` lists running models; `/health` "just returns OK" — [llama-swap README](https://raw.githubusercontent.com/mostlygeek/llama-swap/main/README.md)

**Foundry Local (CLI REST service)**
- Endpoints: `/v1/chat/completions`, `/v1/audio/transcriptions`, `/openai/status` (Endpoints, ModelDirPath, PipeName), `/foundry/list`, `/openai/models`, `/openai/download`, `/openai/load/{name}?ttl=&ep=&unload=`, `/openai/unload/{name}?force=`, `/openai/unloadall`, `/openai/loadedmodels`, `/openai/getgpudevice`, `/openai/setgpudevice/{id}`, `/v1/chat/completions/tokenizer/encode/count`. No metrics/health/queue endpoint is documented. Port is dynamic ("never hardcode the port"; example shows 5272) — [Foundry Local REST reference (updated 2026-07-14)](https://learn.microsoft.com/en-us/azure/foundry-local/reference/reference-rest)
- README: "No metrics endpoints are documented"; Foundry may send usage telemetry to Microsoft — [Foundry-Local README](https://raw.githubusercontent.com/microsoft/Foundry-Local/main/README.md)

**LM Studio / LocalAI / Jan**: see Q2; no metrics endpoints were confirmed in this pass.

### Inferences
- A KVStream "engine adapter" layer could read `llamacpp:requests_deferred`/`requests_processing` or `/slots?fail_on_no_slot=1` as a true saturation signal for llama.cpp backends, and `/api/ps` for Ollama memory state; for Foundry, KVStream's own inflight counter + latency EWMA + process RSS (psutil on the Foundry PID) remain the only viable signals.
- `/v1/chat/completions/tokenizer/encode/count` on Foundry lets KVStream compute exact prompt-token counts for cost-aware admission/predictive shedding without running inference.
- `/openai/loadedmodels` + `/openai/unload/{name}?force=true` + `/openai/load/{name}` give KVStream a possible "recycle" remediation path for the observed stall/RSS bloat (unload/reload) - but see Q3 bugs showing unload may not release memory.

### Gaps
- Could not confirm whether current Ollama (late 2026) ships a native Prometheus endpoint; official docs fetched show none.
- LM Studio and Jan metrics/observability endpoints not researched in depth.

## Q2. What overload semantics do they implement themselves, and how do they compare to KVStream's?

### Takeaway
Only Ollama has an explicit bounded queue with fast rejection (`OLLAMA_MAX_QUEUE`=512, then 503). llama.cpp has slots + continuous batching and defers excess requests with no documented queue cap or rejection. LM Studio queues beyond "Max Concurrent Predictions" (default 4). LocalAI uses watchdogs that kill idle/stuck backends. Foundry Local has no queue, no batching, and no overload signalling. None of them implement predictive shedding, Retry-After, circuit breaking or request coalescing - KVStream's feature set sits above all of them.

### Cited Findings
**Ollama**
- `OLLAMA_NUM_PARALLEL`: "maximum number of parallel requests each model will process at the same time, default 1"; memory scales with parallel x context — [Ollama FAQ](https://raw.githubusercontent.com/ollama/ollama/main/docs/faq.mdx)
- `OLLAMA_MAX_QUEUE`: "maximum number of requests Ollama will queue when busy before rejecting additional requests. The default is 512"; overflow returns 503 — [Ollama FAQ](https://raw.githubusercontent.com/ollama/ollama/main/docs/faq.mdx)
- `OLLAMA_MAX_LOADED_MODELS`: default 3 x number of GPUs, or 3 for CPU; memory-based scheduling: if insufficient memory to load a new model, "all new requests will be queued until the new model can be loaded" — [Ollama FAQ](https://raw.githubusercontent.com/ollama/ollama/main/docs/faq.mdx)
- Eviction via `keep_alive` / `OLLAMA_KEEP_ALIVE` (default 5m; negative = forever; 0 = unload immediately); `OLLAMA_FLASH_ATTENTION`, `OLLAMA_KV_CACHE_TYPE` (f16/q8_0/q4_0) — [Ollama FAQ](https://raw.githubusercontent.com/ollama/ollama/main/docs/faq.mdx)
- Agent features: tools via `tools` on `/api/chat`; structured outputs via JSON schema in `format` — [Ollama docs/api.md](https://raw.githubusercontent.com/ollama/ollama/main/docs/api.md)
- No Retry-After behaviour documented (none found).

**llama.cpp server**
- `-np/--parallel N` slots (default -1 = auto); `-cb/--cont-batching` enabled by default; `--kv-unified` single shared KV buffer (default on when slots auto) — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- Prompt cache: `--cache-prompt` (default on) reuses KV from previous request prefixes; `--cache-reuse N` enables KV-shift chunk reuse (default 0/off); `--slot-save-path` enables `POST /slots/{id}?action=save|restore|erase` — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- Overload: README does not document a queue cap or rejection threshold; excess requests are deferred (tracked by `llamacpp:requests_deferred`) — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- Router mode: `--models-dir` / `--models-preset` with `--models-max N` (default 4) concurrent models — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- Tool calling with `--jinja`; structured outputs via `response_format` json_schema and GBNF grammars — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- With explicit `--parallel N` and `--no-kv-unified`, per-slot context = ctx-size / N; increasing parallel without increasing batch/ubatch "spreads the same throughput across more queues; latency goes up" (secondary source) — [Medium: Tuning llama-server on Apple Silicon](https://medium.com/@michael.hannecke/tuning-llama-server-on-apple-silicon-9b3e778ab100); [mintlify llama.cpp performance tuning](https://www.mintlify.com/ggml-org/llama.cpp/advanced/performance-tuning)

**LM Studio**
- 0.4.0 introduced parallel requests via continuous batching (built on llama.cpp's implementation); "Max Concurrent Predictions" default 4, "Requests beyond this limit will be queued"; optional Unified KV Cache not hard-partitioned per request; MLX continuous batching (text-only) added in 0.4.2 (Feb 2026) — [LM Studio docs: parallel requests](https://lmstudio.ai/docs/app/advanced/parallel-requests); [LM Studio 0.4.0 blog](https://lmstudio.ai/blog/0.4.0); [LM Studio 0.4.2 changelog](https://lmstudio.ai/changelog/lmstudio-v0.4.2)
- No queue cap / 429/503 semantics confirmed from primary docs (a third-party article discusses "429 Too Many Requests" in LM Studio but was not verified) — [markaicode](https://markaicode.com/errors/lm-studio-rate-limit-exceeded-fix/)

**LocalAI**
- `LLAMACPP_PARALLEL` (llama.cpp workers, default 1); `LOCALAI_PARALLEL_REQUESTS` enables parallel requests; `--single-active-backend` deprecated in favour of `--max-active-backends=1`; watchdogs `LOCALAI_WATCHDOG_IDLE(_TIMEOUT)` and `LOCALAI_WATCHDOG_BUSY(_TIMEOUT)` kill backends idle or busy too long — [LocalAI .env (mirror)](https://gitea.varghacsongor.hu/GithubMirror/LocalAI/raw/branch/master/.env); [LocalAI CLI reference (mirror)](https://gitea.varghacsongor.hu/GithubMirror/LocalAI/src/commit/0483d47674d930062bf73bc06b37610cf960d779/docs/content/reference/cli-reference.md)

**Foundry Local**
- README: Foundry Local "isn't designed as a server inference stack" and doesn't provide concurrent request queuing or continuous batching as found in vLLM-class servers — [Foundry-Local README](https://raw.githubusercontent.com/microsoft/Foundry-Local/main/README.md)
- REST reference documents no overload/queue status codes; models auto-unload after idle TTL (`unload` default true; `ttl` override) — [Foundry Local REST reference](https://learn.microsoft.com/en-us/azure/foundry-local/reference/reference-rest)

### Inferences
- Comparison table for the report: Ollama = bounded FIFO + 503 (no Retry-After, no predictive shedding); llama.cpp = slots + unbounded deferral; LM Studio = concurrency cap + queue; LocalAI = watchdog kill; Foundry = none. KVStream's watchdog/circuit breaker for a stalled Foundry runtime is directly analogous to LocalAI's `WATCHDOG_BUSY` - a useful precedent.
- On llama.cpp/Ollama, KVStream's admission limit should be aligned to engine parallelism (slots / NUM_PARALLEL), otherwise the engine queues invisibly and KVStream's latency predictions degrade. On Foundry (serialised), concurrency=1 to the engine with KVStream owning the queue is the correct model - consistent with the user's measurements.
- Ollama's default MAX_QUEUE=512 with NUM_PARALLEL=1 means up to 512 requests can wait behind one slot - far looser than deadline-aware shedding; this is a differentiator KVStream can cite.

### Gaps
- Jan: no primary info found on Jan's server concurrency (Jan embeds llama.cpp; settings presumably mirror llama-server's n_parallel/cont_batching, but unconfirmed).
- No source confirmed Retry-After headers from any local server.
- llama.cpp hard queue cap: none documented; GitHub issues on this not searched.

## Q3. What does Foundry Local's current roadmap/docs say about concurrency, batching and server mode in 2026?

### Takeaway
Foundry Local was re-architected in 2026 from a CLI-managed background service (0.8.x era, which KVStream measured) into an in-process native library ("Foundry Local Core") with an optional in-process OpenAI-compatible REST endpoint; SDK v2.0.1 (2026-09-01) unified the SDKs, and v2.1.0 (2026-09-30) ships ORT GenAI 0.17.1. The architecture doc claims the Core API is "thread-safe, session-based" and supports concurrent requests from multiple threads, while the README still says it provides no request queueing or continuous batching. ORT GenAI itself gained a continuous-batching Engine (chunked prefill, paged attention, prefix caching) in 0.16/0.17 (Sept 2026), mainly GPU-oriented; there is no evidence Foundry Local's REST server uses it yet. Open issues show memory not being released on unload and concurrency-related crashes.

### Cited Findings
**Architecture (2026)**
- Foundry Local "ships as a single native library inside your application"; Core API (.dll/.so/.dylib) loaded in-process; handles model lifecycle, hardware abstraction, and "Thread-safe, session-based inference — supporting concurrent requests from multiple threads without requiring external synchronization" (doc ms.date 2026-05-06, updated 2026-08-04) — [Foundry Local architecture](https://learn.microsoft.com/en-us/azure/foundry-local/concepts/foundry-local-architecture)
- Optional REST: "the Foundry Local SDK can start an optional OpenAI-compatible REST endpoint within your application process" — [Foundry Local architecture](https://learn.microsoft.com/en-us/azure/foundry-local/concepts/foundry-local-architecture)
- EPs: NVIDIA CUDA (Win/Linux), WebGPU via Dawn (Win/Linux/macOS), AMD Vitis NPU (Win), Qualcomm NPU (Win), Intel OpenVINO GPU (Win), CPU (always available fallback); on Windows, WinML acquires/registers EP plugins via OS/Windows Update — [Foundry Local architecture](https://learn.microsoft.com/en-us/azure/foundry-local/concepts/foundry-local-architecture)
- Older CLI REST API still documents `ep` values `dml`, `cuda`, `qnn`, `cpu`, `webgpu` and flags the CLI as preview with breaking changes "without notice" — [Foundry Local REST reference](https://learn.microsoft.com/en-us/azure/foundry-local/reference/reference-rest)
- README: SDK is the core product; CLI "available for development workflows"; web server is optional "for serving models to multiple processes"; Foundry Local "isn't designed as a server inference stack" and lacks concurrent request queuing / continuous batching — [Foundry-Local README](https://raw.githubusercontent.com/microsoft/Foundry-Local/main/README.md)

**Releases (GitHub API)**
- v2.1.0 (2026-09-30): BYO ORT GenAI models, text-based custom tools for Chat Completions/native sessions, ORT GenAI 0.17.1. v2.0.1 (2026-09-01): unified Session API (C#, Python, JS/TS, Rust, C++), single shared `foundry_local` native runtime replacing separate WinML packages, EP auto-detection (WinML, WebGPU, CPU, CUDA), deprecated OpenAI-style in-process clients in favour of `ChatSession`/`EmbeddingsSession`/`AudioSession`, cross-process download locking. v1.2.4 (2026-07-30) race-condition fix; v1.2.1 (2026-06-05) ORT GenAI 0.14.1. CLI: 0.10.0 (2026-06-04, initial preview of new CLI), 0.10.3 (2026-08-07, SDK 1.2.4, end-to-end cancellation) — [Foundry-Local releases API](https://api.github.com/repos/microsoft/Foundry-Local/releases?per_page=15)

**ORT GenAI batching**
- v0.16.0 (2026-09-22): "Continuous batching now supports chunked prefill, paged attention, multi-turn conversations, mixed workloads, and smarter token scheduling"; "a much more capable Engine"; paged/windowed/sparse/quantized KV (INT4/INT8/FP8); AMDGPU EP; WebGPU paged-attention exports; requests can retain KV between turns. v0.17.0 (2026-09-28): prefix caching auto-enabled for supported paged KV layouts; CPU offload of embeddings to lower GPU memory — [onnxruntime-genai releases API](https://api.github.com/repos/microsoft/onnxruntime-genai/releases?per_page=12)

**Known bugs / open issues (all open as of retrieval unless noted)**
- #1091 (2026-09-09): tokenizer lifetime crash in `OgaCreateTokenizerStream` when a concurrent request, registry update or cleanup releases the tokenizer; maintainers ask for 1,000 request-vs-unload interleavings and 10,000 requests across 2/4/8/16 concurrency matrix — [issue #1091](https://github.com/microsoft/foundry-local/issues/1091)
- #1079 (2026-09-07): `Model.UnloadAsync` retains 98-99% of GPU/process memory although `IsLoadedAsync()` is false — [issue #1079](https://github.com/microsoft/foundry-local/issues/1079)
- #1110 (PR, 2026-09-15): call `OgaReleaseDeviceResources` after last CUDA model unload to release cached allocations — [PR #1110](https://github.com/microsoft/foundry-local/pull/1110)
- #861 (2026-07-01): request to expose ORT memory options (arena settings, GPU limits) due to memory growth over time — [issue #861](https://github.com/microsoft/foundry-local/issues/861)
- #855 (2026-07-01): memory/VRAM spikes consuming all memory within seconds with GitHub Copilot + qwen3.5 — [issue #855](https://github.com/microsoft/foundry-local/issues/855)
- #1034 (2026-08-25): Vision API VRAM leak, ~11.9 GB over 3 requests, latency 54s->90s then crash — [issue #1034](https://github.com/microsoft/foundry-local/issues/1034)
- #1035: OpenAI `image_url` format triggers tens-of-GB allocation crash — [issue #1035](https://github.com/microsoft/foundry-local/issues/1035)
- #829 (2026-06-22): NPU model load kills server ("Daemon closed connection") — [issue #829](https://github.com/microsoft/foundry-local/issues/829)
- #1173 (2026-10-02): request for GPU idle timeout that releases GPU contexts while keeping server available — [issue #1173](https://github.com/microsoft/foundry-local/issues/1173)
- #1083 (merged): preserve tool calls (incl. parallel calls) across chat turns — [PR #1083](https://github.com/microsoft/foundry-local/pull/1083)

### Inferences
- KVStream's observed 0.8.119 behaviour (serialised requests; 13.4 GB RSS for a 2.13 GB model and permanent stall) is consistent with the open memory-retention/leak issues (#861, #1079, #1034) and the README's statement of no batching. These are corroborating, not proving, since they are on newer versions/other models.
- The README ("no queuing/continuous batching") and architecture doc ("thread-safe ... concurrent requests from multiple threads") are not contradictory: thread-safety means concurrent calls are allowed, not that they are batched. KVStream should still assume serialised throughput on CPU, but should re-benchmark on SDK 2.x since ORT GenAI 0.16+ has a continuous-batching Engine that Foundry could adopt.
- The move to in-process library + optional in-process REST server changes KVStream's integration assumptions: port discovery via `foundry service status` (0.8.x) may not apply when an app hosts the endpoint itself; KVStream may need a mode that hosts Foundry Core via the Python SDK in a child process it owns (enabling RSS monitoring and kill/restart as remediation).
- Because unload does not reliably free memory (#1079), KVStream's stall remediation should prefer process restart over `/openai/unload` when RSS exceeds a multiple of model size.

### Gaps
- No primary statement found on whether Foundry Local 2.x's REST server uses the ORT GenAI Engine/continuous batching; no public roadmap document found.
- Did not verify CLI 0.8.x-specific issues (the user's exact version) or a changelog entry for the 0.8 -> 0.10 CLI rewrite.
- Issue details are from fetch-model summaries of the GitHub API; exact wording should be verified.
- DirectML status: REST doc still lists `dml`, but architecture doc's EP table omits DirectML (implying WinML/WebGPU replace it); no explicit deprecation notice found.

## Q4. Are there local gateways that already solve parts of KVStream's problem?

### Takeaway
Yes, partially. llama-swap solves model swapping/TTL/unload with Prometheus system/GPU metrics; Olla (Go) is the closest neighbour - a local LLM proxy/load balancer with health checks, circuit breakers, retries/failover, rate limiting and KV-cache-aware affinity routing across Ollama, LM Studio, llama.cpp, vLLM, LiteLLM, Lemonade, etc. None found specifically targets Foundry Local, and none advertises deadline-aware predictive shedding, request coalescing or engine-stall detection.

### Cited Findings
- llama-swap: one model at a time by default; `matrix` DSL for concurrent models; `ttl` auto-unload, `unloadTimeout`; `/running`, `POST /api/models/unload[/:model_id]`, `/health`, `/metrics`; upstreams = any OpenAI/Anthropic-compatible server (llama-server, vLLM, stable-diffusion.cpp, whisper.cpp, ComfyUI) launched via `cmd` with `${PORT}` substitution; README does not document request queueing during swaps or concurrency limits — [llama-swap README](https://raw.githubusercontent.com/mostlygeek/llama-swap/main/README.md)
- Olla: "high-performance, low-overhead, low-latency proxy and load balancer"; backends Ollama, LM Studio, llama.cpp, vLLM, vLLM-MLX, LiteLLM, Lemonade SDK, SGLang, LMDeploy, oMLX, Docker Model Runner + generic OpenAI-compatible; "Priority-based routing with automatic failover", "KV-cache-aware affinity routing that pins multi-turn conversations to the same backend", "Continuous endpoint health checks with circuit breakers", rate limiting and request size limits, Anthropic Messages API translation; runs in <50 MB RAM — [thushan/olla](https://github.com/thushan/olla)
- llama.cpp itself now has a router mode (`--models-dir`, `--models-max`) overlapping with llama-swap — [llama.cpp server README](https://raw.githubusercontent.com/ggml-org/llama.cpp/master/tools/server/README.md)
- LM Studio 0.4.0 added server deployment (headless) and a new REST API alongside parallel requests — [LM Studio 0.4.0 blog](https://lmstudio.ai/blog/0.4.0)

### Inferences
- Olla is the most direct competitor for KVStream's resilience features (health, circuit breaker, failover) but is multi-backend load-balancing oriented; KVStream's differentiators are single-backend overload management (admission, predictive shedding with Retry-After, coalescing, caching) and Foundry-specific lifecycle handling (port discovery, stall/RSS watchdog).
- llama-swap + Olla + LiteLLM can be composed for local setups, but the composition still lacks a saturation-aware admission controller tied to engine signals - a gap KVStream can fill, especially if it adds adapters reading llama.cpp `/metrics` and Ollama `/api/ps`.
- Supporting Foundry as a first-class backend is a niche none of the surveyed proxies cover.

### Gaps
- LiteLLM-in-front-of-Ollama setups not researched specifically in this pass (covered by other researchers presumably).
- Did not verify Olla's exact circuit-breaker thresholds, queueing behaviour, or whether it supports Foundry Local; no Foundry-specific gateway found.
- Jan's local API server and Lemonade Server (AMD) concurrency behaviour not covered.
