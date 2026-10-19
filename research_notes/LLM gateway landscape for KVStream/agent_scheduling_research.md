# Agent-aware / session-aware LLM scheduling, fairness, and adaptive overload control (for KVStream)

Context: KVStream is a single-process Python gateway in front of ONE local Foundry Local instance. It is a black box: no engine metrics, no preemption, no KV control. The backend was measured to serialise requests (throughput stays flat as concurrency rises). Current design: strict-FIFO queue, a fixed admission budget from calibration, reserve-then-settle token accounting, predictive shedding from a measured drain rate, and a drift monitor on seconds-per-token.

## Q1. Which agent-aware scheduling ideas work at a black-box proxy, and which need engine control?

### Takeaway
Ordering policies need only request/response visibility, a program/session ID, and token counts. That covers Autellix's PLAS/ATLAS priority, VTC token fairness, SJF/learning-to-rank, Continuum's program-level FCFS, and Andes-style priority without token-level preemption. A proxy can do all of these by choosing which call to dispatch next. These need engine control and are out of reach for KVStream: token-level preemption (FastServe, Andes), KV swap/offload, pinning or TTL (InferCept, Continuum, CachedAttention), and live migration (Llumnix). On a serialising backend, non-preemptive ordering at dispatch time is the main lever.

### Cited Findings
**Autellix (arXiv 2502.13965, 2025)**
- Problem: agentic programs see "long cumulative wait times, primarily due to head-of-line blocking at both the individual LLM request and the program". Autellix intercepts LLM calls and adds program-level context for the scheduler. — [arXiv abs](https://arxiv.org/abs/2502.13965)
- PLAS (Program-Level Attained Service), for single-threaded programs: each call's priority is the total service its program has already received, p(c_j) = Σ t_k over prior calls. Programs with less service get higher priority, so short programs finish first. — [arXiv html](https://arxiv.org/html/2502.13965)
- ATLAS, for multi-threaded/DAG programs: priority is the longest observed critical-path service time in the program, which favours calls on the critical path. No explicit dependency tracking is needed. — [arXiv html](https://arxiv.org/html/2502.13965)
- Mechanics: K discretised priority queues, FCFS within each queue. Calls are demoted after using up a time quantum. An anti-starvation rule promotes a call when its program's wait-to-service ratio passes threshold β. — [arXiv html](https://arxiv.org/html/2502.13965)
- Engine requirements: preemption, batched GPU-CPU KV swapping (18x fewer swaps), a global process table, and prefix caching. Built on vLLM v0.6.1 (~5K LOC). — [arXiv html](https://arxiv.org/html/2502.13965)
- Data-locality load balancing: short requests (<=2048 tokens) go to the least-loaded engine. Long requests stay pinned to their program's engine, where prefix-cache reuse inside a program is >90%. Gain: up to 1.4x throughput over naive balancers. — [arXiv html](https://arxiv.org/html/2502.13965)

**Parrot (OSDI'24, arXiv 2405.19888)**
- "Semantic Variables" annotate prompt inputs/outputs and link LLM requests together. The service can then run data-flow analysis across an application instead of "blindly optimiz[ing] individual LLM requests". Reported gain: "up to an order-of-magnitude improvement" for practical LLM apps. — [arXiv](https://arxiv.org/abs/2405.19888)
- Applicability: the app-level DAG and the dependency annotations could live at a proxy. Parrot's prefix-sharing and batching gains need engine cooperation.

**VTC: Fairness in Serving LLMs (OSDI'24, arXiv 2401.00588)**
- Fairness is measured by input and output tokens consumed, not request counts. VTC gives a tight 2x upper bound on service difference between backlogged clients and is work-conserving. Rate limiting, by contrast, leaves capacity idle when demand is low. — [arXiv](https://arxiv.org/abs/2401.00588)
- Applicability: a per-client virtual counter, charged on token settle, with dispatch of the lowest-counter backlogged client. A proxy can do this with only token counts.

**Andes (arXiv 2404.16283)**
- QoE for streaming text means a fast first token, then steady delivery. A preemptive scheduler prioritises at token granularity by expected QoE gain and GPU cost. Gains: average QoE up to 4.7x at equal GPU, or up to 61% GPU saved at equal QoE. — [arXiv](https://arxiv.org/abs/2404.16283)
- Applicability: token-level preemption needs the engine. A proxy can still pace streamed tokens and favour requests whose client is waiting on the first token.

**Learning to Rank for SJF (arXiv 2408.15792, Fu, Stoica et al.)**
- Exact output lengths cannot be predicted, but their relative ranks within a batch can, using learning to rank. This approximates SJF better than prior methods. Gains: 2.8x lower latency for chatbot serving and 6.5x higher throughput for synthetic data generation. — [arXiv](https://arxiv.org/abs/2408.15792)

**S3 (arXiv 2306.06000)**
- Predicts output length and schedules and batches on that prediction instead of reserving worst-case KV. Handles mispredictions. Gain: 6.49x throughput over worst-case-length systems. — [arXiv](https://arxiv.org/abs/2306.06000)

**FastServe (arXiv 2305.05920)**
- Token-level preemption with a skip-join MLFQ: input length sets the starting queue, which cuts demotions. KV state is proactively offloaded to host memory. Gains: up to 31.4x throughput at equal average latency and 17.9x at equal tail latency vs vLLM. Needs engine control. — [arXiv](https://arxiv.org/abs/2305.05920)

**Llumnix (OSDI'24, arXiv 2406.03243)**
- Live migration of requests and their in-memory KV state across instances. Gains: tail latency improved "by an order of magnitude", high-priority requests up to 1.5x faster, up to 36% cost savings. Needs multiple instances plus engine control, so it does not apply to a single Foundry Local backend. — [arXiv](https://arxiv.org/abs/2406.03243)

**InferCept (ICML'24, arXiv 2402.01869)**
- Engines treat tool calls and other interceptions as end-of-generation, so they recompute context. That recomputation is 37–40% of model forwarding time. InferCept chooses to preserve, swap, or discard KV across interceptions. Gains: 1.6x–2x throughput and 2x more requests/s. — [arXiv](https://arxiv.org/abs/2402.01869)

**CachedAttention / AttentionStore (ATC'24, arXiv 2403.19708)**
- Hierarchical KV cache across turns, with layer-wise prefetch, scheduler-aware fetch/evict, and decoupled positional encoding so truncation does not invalidate the cache. Gains: TTFT down up to 87%, prefill throughput up to 7.8x, end-to-end cost down up to 70%. — [arXiv](https://arxiv.org/abs/2403.19708)

**Continuum (arXiv 2511.02230, ICLR 2026)**
- Pins an agent's KV in GPU memory with a TTL across a tool call. The TTL maximises P(tool finishes within τ)·Benefit − Cost(τ). Benefit = avoided prefill/reload plus queueing saved by keeping the program continuous. Cost = memory opportunity cost. P comes from an empirical CDF of each tool's durations. — [arXiv html v2](https://arxiv.org/html/2511.02230v2)
- Scheduling is program-level FCFS: preempted requests first, then requests with a live pin, then ties broken by program arrival time. This avoids "per-turn queueing delay" across multiple turns. — [arXiv html v2](https://arxiv.org/html/2511.02230v2)
- Deadlock avoidance: when GPU memory is full of pins, it unpins the latest-arriving programs first. — [arXiv html v2](https://arxiv.org/html/2511.02230v2)
- Engine requirements: ~1,000 LOC in the vLLM scheduler, pin/unpin of KV blocks, and offline profiling of about 10 min per hardware/model pair. — [arXiv html v2](https://arxiv.org/html/2511.02230v2)

### Inferences
- **Usable in KVStream without engine changes:**
  - PLAS-style program priority. Key it by `session_id`/`agent_id` and accumulate settled tokens or measured service seconds.
  - VTC token counters per client.
  - Continuum's program-level FCFS: rank a call by the arrival time of its *program*, not of the call. This directly fixes the problem where turn N of an old agent queues behind turn 1 of new agents.
  - SJF/rank prediction with aging.
  - Autellix's anti-starvation β (wait/service ratio). It fits naturally on top of the current strict FIFO.
- **Not usable** (black box, no KV control): preemption, KV pin/TTL/swap, and migration. KVStream could approximate "continuity" only indirectly. If Foundry Local does any internal prefix caching (unverified), dispatching a returning agent's next turn immediately after its tool call might hit a warm cache. That is a hypothesis to measure, not a fact.
- On a serialising backend (effective concurrency ≈ 1), non-preemptive SJF/PLAS ordering of the queue gives most of the latency benefit, because the queue is where the waiting happens. Preemption gains (FastServe, Andes) matter mainly when batching is real.
- Recommended KVStream policy sketch:
  - priority = (program attained service in tokens, discretised into K bins), with FCFS by *program* arrival inside each bin.
  - Promote a call if its program's wait/service ratio exceeds β, which addresses the original starvation concern behind strict FIFO.
  - Optionally add VTC across tenants/agents when multiple users share the gateway.

### Gaps
- I did not verify VTC's exact token weights (input vs output) or its measured latency numbers beyond the 2x bound. Only the abstract was read.
- Parrot's per-workload numbers beyond "order of magnitude" were not extracted.
- No source found on whether Foundry Local / ONNX Runtime GenAI keeps prefix/KV cache across requests. This decides whether session-affinity ordering helps KVStream at all.

## Q2. What gains do program-level schedulers report vs FCFS for agentic workloads, and on what traces?

### Takeaway
Autellix reports 4–15x program throughput at equal latency vs vLLM (FCFS) on ShareGPT, BFCL, LATS/MCTS, and mixed traces with Poisson arrivals. Continuum reports 1.12–3.66x lower delay and 1.10–3.22x throughput vs vLLM on SWE-bench and BFCL v4 agent traces, and up to 8.18x in a real SWE-bench trial. Both rely partly on engine features.

### Cited Findings
- Autellix workloads:
  - ShareGPT: 6.66 calls/program, 277 output tokens average.
  - BFCL ReAct: 10.75 calls/program, 735-token prefill, 34-token decode.
  - LATS MCTS: 159.7 calls/program.
  - Mixed: uniform samples across the three.
  - All use Poisson arrivals. — [arXiv html](https://arxiv.org/html/2502.13965)
- Autellix gains vs vLLM:
  - 4–8x throughput on single-threaded workloads (ShareGPT, BFCL).
  - 5x on LATS.
  - 15x on mixed.
  - Up to 2.5x over program-unaware MLFQ on LATS. — [arXiv html](https://arxiv.org/html/2502.13965); headline "4-15x" — [arXiv abs](https://arxiv.org/abs/2502.13965)
- Continuum traces:
  - SWE-bench (mini-SWE-agent): 10.9 turns, ~70K tokens/program.
  - BFCL v4 web search: 6.3 turns, ~1.9 s mean tool time, ~93K tokens/program.
  - Tool latency is long-tailed: the slowest 10% of tool calls account for 52.5–94.1% of delay. — [arXiv html v2](https://arxiv.org/html/2511.02230v2)
- Continuum baselines: vLLM 0.10.2, vLLM+LMCache, Autellix PLAS, InferCept, SGLang, Dynamo. Results: delay down 1.12–3.66x and throughput up 1.10–3.22x vs vLLM, with up to 8.18x delay reduction in real SWE-bench runs and higher pass rates. — [arXiv html v2](https://arxiv.org/html/2511.02230v2); [search summary/ICLR listing](https://iclr.cc/virtual/2026/10012473)
- Non-program-aware length schedulers: 2.8x chatbot latency (LTR, [arXiv](https://arxiv.org/abs/2408.15792)), 31.4x throughput (FastServe, [arXiv](https://arxiv.org/abs/2305.05920)), 6.49x throughput (S3, [arXiv](https://arxiv.org/abs/2306.06000)).

### Inferences
- These multipliers come from batching GPU engines (vLLM) where scheduling interacts with KV memory and batching. On a serialising single backend, expect smaller but real gains in *mean program completion time* from SJF/PLAS ordering. Classic queueing results favour SRPT/LAS over FCFS for heavy-tailed job sizes. Throughput will not change, because the backend is the bottleneck.
- Agent workloads are prefill-heavy with short decodes (BFCL: 735 in / 34 out). Admission cost for KVStream should therefore weight input tokens heavily, and growing transcripts make later turns progressively more expensive. PLAS by cumulative tokens naturally deprioritises long-running agents. Pair it with β-promotion so near-finished long agents are not starved.

### Gaps
- No paper found that evaluates program-level scheduling at a pure black-box proxy in front of a single serialising backend. KVStream would have to measure gains itself, for example by replaying BFCL/SWE-bench-like traces.

## Q3. How do adaptive concurrency limiters behave when latency naturally varies with request size? Any token-normalised work?

### Takeaway
Netflix Vegas/Gradient2 and Envoy's gradient controller infer queueing from latency inflation over a minimum RTT. They assume roughly homogeneous request cost, so they misread naturally long LLM requests as congestion. I found no primary source that normalises the gradient by tokens. The natural adaptation for KVStream is to compute the gradient on seconds-per-token (which its drift monitor already measures), with minRTT seeded from calibration.

### Cited Findings
- Netflix concurrency-limits rests on Little's law: "Limit = Average RPS * Average Latency". — [GitHub](https://github.com/Netflix/concurrency-limits)
- Vegas: estimated queue = L·(1 − minRTT/sampleRTT). Increase by 1 if queue < alpha (2–3), decrease if queue > beta (4–6). — [GitHub](https://github.com/Netflix/concurrency-limits)
- Gradient2 addresses "bias and drift when using minimum latency measurements". It compares long- and short-window exponential averages, and "divergence duration is used as a proxy to identify a queueing trend at which point the algorithm aggressively reduces the limit". — [GitHub](https://github.com/Netflix/concurrency-limits)
- AIMD is a "pure loss based" limiter, suited to client-side use. — [GitHub](https://github.com/Netflix/concurrency-limits)
- The Netflix README does not cover latency heterogeneity across request types. It offers percentage-based partitioning for QoS. — [GitHub](https://github.com/Netflix/concurrency-limits)
- Envoy adaptive concurrency:
  - gradient = (minRTT + B)/sampleRTT, where B is a buffer as a % of minRTT.
  - limit_new = gradient·limit_old + headroom, with headroom fixed at sqrt(limit).
  - sampleRTT is aggregated over a 100 ms window.
  - minRTT is re-measured about every 60 s by pinning concurrency to min_concurrency, with jitter. Envoy warns of "a noticeable increase in request 503s during the minRTT measurement window". — [Envoy docs](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
- Industry gateway writing describes reserving an estimated token budget at admission and then reconciling actual usage, which is what KVStream already does. Adaptive limits "require careful tuning to avoid oscillation". These are vendor/aggregator pages of low evidentiary weight. — [TrueFoundry](https://www.truefoundry.com/ja/blog/rate-limiting-in-llm-gateway); [Palo Alto Networks blog](https://live.paloaltonetworks.com/t5/engineering-blogs/rate-limiting-for-llm-applications-and-agents-why-it-matters-and/ba-p/1264762)

### Inferences
- Raw-RTT gradients will mistake a 4K-token completion for congestion. Normalise first:
  - Use sampleRTT = observed seconds per (weighted) token, or TTFT per prefill-token plus inter-token latency, and use minRTT = the calibration baseline seconds-per-token.
  - That turns KVStream's drift monitor into the gradient's input: gradient = (baseline_spt·(1+B))/recent_spt.
- On a backend whose throughput is flat with concurrency, the "right" limit is about 1–2 in flight plus a small queue. A gradient limiter will mostly drive the limit down to min_concurrency, which is correct.
- Its real value is tracking *cumulative degradation* (the observed long-session slowdown). As spt inflates, the limit and the predicted drain rate shrink automatically, which replaces the fixed calibration budget.
- Avoid Envoy-style periodic minRTT probes that pin concurrency and cause 503s. Instead, refresh the baseline opportunistically when the queue is empty, or keep the calibrated value. Gradient2's long/short EMA divergence is better suited than a single minimum to the slow drift KVStream sees.
- Seed the limit from the calibration sweep. Clamp it to [1, calibrated_max]. Apply headroom sqrt(limit) only when the queue is non-empty.

### Gaps
- No peer-reviewed or primary source found on token-normalised adaptive concurrency for LLM serving. The Netflix tech blog ("Performance Under Load") returned HTTP 403, so its numbers are not included. The AWS load-shedding article fetch returned no body.

## Q4. Best practices for retries in agent loops (retry budgets, idempotency, Retry-After) to avoid retry storms

### Takeaway
- Retry at exactly one layer.
- Cap attempts per request (Google: 3).
- Enforce a per-client retry ratio budget (Google: 10%, giving about 1.1x amplification) or a token bucket (AWS SDK).
- Always use randomised exponential backoff.
- Propagate deadlines and cancellation.
- Shed early with small queues, preferring LIFO or CoDel under overload.
- Retry only idempotent operations.

For agents, the gateway should return 429/503 with Retry-After derived from its predicted drain time. It should not retry internally, so that the agent framework's own retries do not multiply.

### Cited Findings
- Per-request retry cap: "If a request has already failed three times, we let the failure bubble up". Per-client budget: retry only while the retry ratio is below 10%. Combined, these limit amplification to about 1.1x. — [Google SRE: Handling Overload](https://sre.google/sre-book/handling-overload/)
- Client-side adaptive throttling: track requests and accepts over 2 min, and reject locally once requests ≥ K·accepts (K=2 typical). — [Google SRE: Handling Overload](https://sre.google/sre-book/handling-overload/)
- Criticality tiers: CRITICAL_PLUS, CRITICAL, SHEDDABLE_PLUS, SHEDDABLE. — [Google SRE: Handling Overload](https://sre.google/sre-book/handling-overload/)
- Small queues: about 50% or less of thread-pool size, rejecting early. Switching from FIFO to LIFO or CoDel "can reduce load by removing requests that are unlikely to be worth processing". — [Google SRE: Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/)
- "Always use randomized exponential backoff". A server-wide retry budget, e.g. 60 retries/min per process. Multi-layer retries multiply: 3 layers × 4 attempts = 64 backend attempts. — [Google SRE: Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/)
- Deadline propagation ("you don't get credit for late assignments") and cancellation propagation, so abandoned work stops. — [Google SRE: Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/)
- AWS: limit retries locally with a token bucket, which the AWS SDK has used since 2016. Use jitter. Requests with side effects "aren't safe to retry unless they provide idempotency". Retry at a single point in the stack. Set timeouts from a downstream latency percentile (e.g. p99.9 for a 0.1% false-timeout rate). — [AWS Builders' Library](https://builder.aws.com/content/3EumjoZascWd1oZiEgL8ORlv3qE/timeouts-retries-and-backoff-with-jitter)
- Retry-After (HTTP) tells the client how long to wait. It can be sent with 503 and 429. — [RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110#field.retry-after)

### Inferences
- **Retry-After from the drain model.** KVStream's predictive shedder already has a drain rate. Emit `Retry-After = ceil(queued_tokens / drain_rate_tokens_per_s)` with jitter on 429/503. Agent SDKs (OpenAI-compatible clients) generally honour it. This spreads retries without the gateway retrying anything itself.
- **Session-aware shedding.** Shedding a mid-workflow agent wastes all its previous turns, which is the sunk-cost problem behind Continuum's continuity argument. Prefer refusing *new* sessions (session-level admission) over failing turn N of an admitted agent. This maps onto SRE criticality tiers: running sessions are CRITICAL, new sessions SHEDDABLE_PLUS.
- **Deadlines.** Accept a client deadline header (or a default), and drop queued requests whose predicted start plus estimated service exceeds it (CoDel-like time-in-queue checks). Cancel backend work when the client disconnects. On a serialising backend, an abandoned long request blocks everyone.
- **LIFO under overload.** It conflicts with KVStream's anti-starvation FIFO goal. A safer hybrid is FIFO normally and CoDel-style dropping of requests that have waited too long once queueing delay stays above target, with per-session β-promotion as in Autellix.
- **Idempotency.** LLM completions are side-effect-free at the gateway, but tool calls in the agent loop are not. Gateway retries are therefore safe only before any bytes are streamed. Once streaming has started, a failure should surface to the agent rather than be replayed.

### Gaps
- I found no published retry-budget guidance specific to LLM agent frameworks (e.g. LangGraph, AutoGen, the OpenAI SDK's default max_retries). The SDK defaults were not verified in this pass.
- The AWS "Using load shedding to avoid overload" article could not be retrieved (empty body after redirect), so its specific recommendations are not cited.
