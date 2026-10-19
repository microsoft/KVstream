# Application-layer multi-provider LLM gateways (LLM Gateway, LiteLLM, Portkey, Bifrost, Helicone, TensorZero, OpenRouter) — relevance to KVStream

Research date: 2026-10-07. Star counts and versions come from GitHub pages fetched on that date and will drift. Vendor performance numbers are the vendors' own claims and have not been independently checked.

## Product profiles (architecture, runtime, deployment, licence, maturity)

### Takeaway
There are three kinds of product here. LiteLLM is a Python proxy. Bifrost (Go), TensorZero (Rust) and Helicone (Rust) are compiled single-binary gateways that make performance claims in microseconds. LLM Gateway (TypeScript) and Portkey (TypeScript) are platform/SaaS-oriented. OpenRouter is SaaS only. 2026 brought big changes in who owns or maintains several of them: Helicone was acquired and is in maintenance mode, LiteLLM suffered a PyPI supply-chain compromise, and Portkey was acquired by Palo Alto Networks.

### Cited Findings
**LiteLLM Proxy (BerriAI)**
- Python proxy and router. Routing strategies: simple-shuffle (default; weighted by rpm/tpm), latency-based-routing, usage-based-routing-v2 (Redis-tracked TPM), least-busy (fewest in-flight calls), cost-based-routing, and custom via `CustomRoutingStrategyBase` — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- Per-deployment `rpm`, `tpm` and `max_parallel_requests` (derived from rpm/tpm if unset). The docs summary says that going over these limits returns an immediate 429 with no queueing — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- Response caching is in-memory or Redis (`cache_responses=True`). Pre-call checks (`enable_pre_call_checks`) filter deployments by context window and region. Session affinity is available via `optional_pre_call_checks: ["session_affinity"]`, with `deployment_affinity_ttl_seconds` defaulting to 3600 — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- Performance (vendor claim, 2026-02-02): "sub-millisecond proxy overhead". On a single 4 CPU / 8 GB instance it ran 1,000 QPS with no failures and scaled to 5,000 QPS. Load generator, LiteLLM and a mock LLM all ran on the same machine. The post admits LiteLLM previously "failed at around 1,000 QPS". The gain comes from an optional sidecar that handles forwarding, connection pooling, timeouts/limits and metric aggregation, while Python stays the control plane. The sidecar is not yet a hard dependency — [LiteLLM blog](https://docs.litellm.ai/blog/sub-millisecond-proxy-overhead)
- Security incident: on 2026-03-24, PyPI versions 1.82.7 and 1.82.8 were published with stolen credentials and contained a credential-stealing `litellm_init.pth` payload. The credentials were taken through a compromised Trivy GitHub Action in LiteLLM's CI. The exposure window was about 2–3 hours — [yage.ai write-up](https://yage.ai/share/litellm-pypi-hijack-ai-engineers-en-20260324.html); [bastion.tech](https://bastion.tech/blog/litellm-pypi-supply-chain-attack)

**Bifrost (Maxim AI)**
- Written in Go. Deploys via `npx -y @maximhq/bifrost`, Docker, Kubernetes, or as a Go SDK, and has a web UI. Apache 2.0. About 8.6k stars. Enterprise-only features: custom plugins, clustering, secrets management, OIDC provisioning — [GitHub maximhq/bifrost](https://github.com/maximhq/bifrost)
- Vendor claims: 11 µs overhead at 5,000 RPS on a t3.xlarge with 100% success, "<15 µs additional latency per request", and sub-microsecond queue wait — [GitHub maximhq/bifrost](https://github.com/maximhq/bifrost)
- Other features: fallbacks, load balancing, semantic caching, MCP integration, governance through virtual keys (rate limits, hierarchical budgets), native Prometheus metrics, distributed tracing. Supports 23+ providers, including Ollama — [GitHub maximhq/bifrost](https://github.com/maximhq/bifrost)

**TensorZero**
- Gateway written in Rust, plus Python. Positioned as a whole LLMOps platform: gateway, observability, evals, optimization (fine-tuning, GEPA prompt optimization, dynamic in-context learning, best-of-N) and adaptive A/B experimentation. Inferences and feedback are stored in your own database (Postgres / ClickHouse). Exports to OTel and Prometheus. Apache 2.0, about 11.7k stars. Vendor claim: "<1ms p99 latency overhead at 10k+ QPS" — [GitHub tensorzero/tensorzero](https://github.com/tensorzero/tensorzero)
- Supports OpenAI-compatible self-hosted backends: vLLM, Ollama, SGLang, TGI — [GitHub tensorzero/tensorzero](https://github.com/tensorzero/tensorzero)

**Portkey AI Gateway**
- Written in TypeScript/Node. Runs via `npx @portkey-ai/gateway`, on Cloudflare Workers, in Docker, or in Portkey Cloud. MIT licence, about 13.1k stars. Claims "<1ms latency" and a 122 kb footprint. Retries up to 5 times with exponential backoff, plus fallbacks, load balancing and timeouts. Has 40+ guardrails and an MCP gateway. The README says simple and semantic caching are in the hosted versions. A "Gateway 2.0" pre-release merges enterprise features into OSS — [GitHub Portkey-AI/gateway](https://github.com/Portkey-AI/gateway)
- Palo Alto Networks acquired Portkey. The deal was announced on 2026-06-02 per incyber, and one source says it closed on May 29 (the sources disagree on dates). Portkey is being integrated into Prisma AIRS and processes about 14B tokens per day — [incyber](https://incyber.org/en/article/palo-alto-networks-announced-on-june-2-2026-the-acquisition-of-portkey-a-u-s-start-up-specializing-in-the-control-of-autonomous-ai-agents/); [Intelligence Community News](https://intelligencecommunitynews.com/palo-alto-acquires-portkey/); [nand research](https://nand-research.com/palo-alto-networks-portkey-acquisition-anchors-its-agentic-security-stack/)

**Helicone AI Gateway**
- Written in Rust. Self-hosted (Docker/K8s) or cloud. Vendor claims: P95 "<5ms", about 64 MB of memory, about 3,000 RPS, about 30 MB binary, about 100 ms cold start. Load balancing: model latency routing, provider latency P2C + PeakEWMA, weighted, and cost. Rate limits by requests, tokens or dollars, scoped per user, team or globally. Cache backends are Redis and S3. Supports OTel. About 637 stars. The licence is ambiguous: the page shows both Apache and GPL-3.0 — [GitHub Helicone/ai-gateway](https://github.com/Helicone/ai-gateway)
- Mintlify acquired Helicone on 2026-03-03. The product is in maintenance mode: security fixes, bug fixes and new models, but no new features — [Dealroom](https://ai-startups.dealroom.co/news/feed/mintlify-acquires-open-source-ai-observability-platform-helicone)

**LLM Gateway (llmgateway.io, theopenco/llmgateway)**
- TypeScript monorepo: Hono API and gateway service, Next.js dashboard / playground / portals, PostgreSQL (Drizzle) and Redis. Available as SaaS or self-hosted with Docker Compose. Core is AGPLv3; an `ee/` directory is under a commercial licence (multi-org admin, advanced billing, retention). About 1.7k stars — [GitHub theopenco/llmgateway](https://github.com/theopenco/llmgateway)
- Features: automatic provider failover, provider health monitoring, opt-in response caching per project, per-key/project cost analytics, MCP server support, and local providers (Ollama, LM Studio, custom OpenAI-compatible endpoints) — [LLM Gateway docs](https://docs.llmgateway.io/)

**OpenRouter**
- SaaS only. By default it avoids providers with significant outages in the last 30 seconds, then load-balances by inverse-square price weighting among stable providers, with fallback. Parameters include `sort` (price/throughput/latency), `order`, `allow_fallbacks` (default true), `max_price`, `data_collection`, `zdr` and `partition`. It tracks rolling 5-minute p50/p75/p90/p99 latency and throughput. `preferred_min_throughput` deprioritizes slow endpoints but does not exclude them — [OpenRouter provider routing](https://openrouter.ai/docs/features/provider-routing)

### Inferences
- From the Go/Rust gateways, the pattern worth adopting is the claim style, not the language. They publish overhead in µs at a fixed RPS against a mock backend. KVStream could publish "gateway overhead vs direct Foundry Local" with the same method.
- LiteLLM's "Python control plane + fast sidecar" split is the same architectural choice KVStream faces. Its 2026 sidecar work suggests pure-Python request forwarding was LiteLLM's bottleneck.
- The 2026 changes in ownership and supply-chain trust matter if you want to integrate with these products. Bifrost and TensorZero (Apache 2.0, still independent and actively developed) are the safest integration partners. Helicone looks risky for new dependencies.

### Gaps
- No current release versions or dates were captured for each repo. Star counts are a snapshot from the fetch.
- How completely Portkey Gateway 2.0 open-sources enterprise features, and whether this changes after the acquisition, was not verified.
- LLM Gateway's rate-limiting, guardrail and retry internals were not documented in the pages fetched.

## Which have a real request queue with priority, and how does LiteLLM's scheduler work?

### Takeaway
Only LiteLLM has a priority queue, and it is a beta, polling-based scheduler. Bifrost has a real bounded FIFO queue per provider (worker pool plus buffer) but no priority classes. The others rate-limit and reject or fail over; none of the pages fetched describe queueing.

### Cited Findings
- LiteLLM scheduler: lower `priority` number means higher priority (0 beats 2000). A request is allowed to proceed when it is at the top of the queue and there are healthy deployments. The scheduler polls at `polling_interval` (example 0.03). It works only for `acompletion` / `atext_completion` (`/v1/chat/completions` and `/v1/completions`). Multi-instance queueing uses Redis (`redis_host`/`redis_port`/`redis_password`). With `timeout`/`request_timeout` it "will keep retrying until timeout occurs". It is marked "[BETA] - Use for testing only" — [LiteLLM scheduler docs](https://docs.litellm.ai/docs/scheduler)
- LiteLLM's normal router returns an immediate 429 when per-deployment rpm/tpm/`max_parallel_requests` is exceeded, with no queueing — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- Bifrost has per-provider `concurrency` (worker count, default 1000) and `buffer_size` (queue capacity, default 5000). When the queue is full, excess requests are dropped if `drop_excess_requests` is enabled. Example tuning from the docs: "OpenAI gets higher limits (100 workers, 500 queue)" — [Bifrost provider configuration](https://docs.getbifrost.ai/quickstart/gateway/provider-configuration)
- Helicone rate limits by requests, tokens or dollars, per user, team or globally. No queue is described — [GitHub Helicone/ai-gateway](https://github.com/Helicone/ai-gateway)
- TensorZero lists "custom rate limiting" but no queueing in its README — [GitHub tensorzero/tensorzero](https://github.com/tensorzero/tensorzero)

### Inferences
- LiteLLM's scheduler is a single queue ordered by priority, polled every ~30 ms, and it only waits for "healthy deployments". It has no capacity model, so it does not know how many requests the backend can absorb. KVStream's FIFO plus calibrated KV-token admission is more principled about capacity but has no priority. Adding priority classes (for example, interactive agent turn vs background summarization) on top of the existing admission controller would match the one feature LiteLLM offers here, with a better capacity signal behind it.
- Bifrost's `concurrency`/`buffer_size`/`drop_excess_requests` is the closest analogue to KVStream's admission control plus bounded queue. But the limits are static, and the docs describe dropping requests, not predictive shedding with Retry-After. KVStream's predictive 503 + Retry-After is a differentiator.
- Polling at 3–30 ms adds latency and CPU cost. An event-driven wakeup (asyncio Condition/Event on slot release), which KVStream likely already uses, is the better design.

### Gaps
- The LiteLLM scheduler docs do not say whether priority can be set per virtual key or team, whether starvation protection or aging exists, or whether streaming is supported.
- I could not confirm whether Bifrost's queue is strictly FIFO, or what HTTP status/body a dropped request receives.

## Backend health: cooldowns, circuit breakers, health checks, real-generation readiness probes

### Takeaway
Most gateways handle backend health passively, using failure-count cooldowns. LiteLLM is the only one found that does active health checks with real generations (16-token probes, run in the background every 300 s). Portkey has the most explicit circuit breaker, including an "if all targets are open, route to all anyway" bypass.

### Cited Findings
- LiteLLM cooldowns: a deployment is cooled down after `allowed_fails` failures per minute (default 3), for `cooldown_time` seconds (default 5). Triggers include 429s and non-retryable errors (401, 404, 408). This can be set per deployment or router-wide. There is also `num_retries`, `retry_after`, error-type `RetryPolicy`, ordered/weighted fallbacks and cross-group fallbacks — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- LiteLLM health checks: `/health` "runs a real test request against every configured model". Settings: `background_health_checks: true` with `health_check_interval` (default 300 s) serves cached results, `health_check_max_tokens` defaults to 16, `health_check_timeout` defaults to 60 s, and `health_check_model` is used for wildcard routes. `/health/liveliness` checks the process only and `/health/readiness` checks the worker and DB. The readiness endpoint does not do a generation — [LiteLLM health docs](https://docs.litellm.ai/docs/proxy/health)
- Portkey circuit breaker settings: `failure_threshold`, optional `failure_threshold_percentage`, `minimum_requests`, `cooldown_interval` (minimum 30 s) and `failure_status_codes` (default >500). Failures are tracked per strategy path. When some targets are unhealthy, only healthy targets are used. If all targets are open, the breaker is bypassed and all targets are tried. The circuit closes automatically after the cooldown; no half-open state is documented. `conditional` routing mode is not evaluated by the breaker — [Portkey circuit breaker docs](https://portkey.ai/docs/product/ai-gateway/circuit-breaker)
- OpenRouter avoids providers with "significant outages in the last 30 seconds" and uses rolling 5-minute latency/throughput percentiles — [OpenRouter provider routing](https://openrouter.ai/docs/features/provider-routing)
- Helicone uses latency-aware P2C + PeakEWMA balancing, which steers away from slow providers. Explicit health checks or circuit breaking were not described on the page fetched — [GitHub Helicone/ai-gateway](https://github.com/Helicone/ai-gateway)
- LLM Gateway advertises "provider health monitoring" and automatic failover but gave no mechanism details — [LLM Gateway docs](https://docs.llmgateway.io/)

### Inferences
- KVStream's readiness probe that runs a real generation is uncommon. LiteLLM is the only comparable product, and it deliberately keeps that probe off `/health/readiness` and caches the results because of cost. For a local backend, tokens cost nothing, so KVStream can afford frequent real probes. It should still keep the probe out of the admission path, or count it against the KV budget.
- Worth borrowing from Portkey: (a) percentage-based tripping with `minimum_requests`, and (b) the bypass rule when every target is open. For a single-backend sidecar, (b) maps to "half-open: let one probe or one real request through" rather than failing everything.
- KVStream's 503 + Retry-After will trigger cooldowns in LiteLLM upstream. With LiteLLM's 5 s default cooldown and Portkey's 30 s minimum, a KVStream shed event could take the local backend out of rotation for longer than Retry-After says. Integration docs should recommend tuning those settings.

### Gaps
- Not documented in the pages fetched: Bifrost's health and circuit-breaking mechanism (beyond retries and fallbacks), and TensorZero's backend health handling (beyond sequential provider routing and fallbacks).

## Does any measure local backend capacity or do adaptive concurrency?

### Takeaway
No. I found no evidence that any of the seven measures local inference capacity (KV cache, tokens/s) or adapts concurrency limits. All capacity controls are static numbers set by the operator. Latency-aware routing (Helicone PeakEWMA, LiteLLM latency/least-busy, OpenRouter percentiles) chooses between backends; it does not size admission for any one backend.

### Cited Findings
- LiteLLM: static `rpm`/`tpm`/`max_parallel_requests`. `least-busy` counts in-flight calls; latency-based routing picks the fastest deployment — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- Bifrost: static `concurrency`/`buffer_size` per provider — [Bifrost provider configuration](https://docs.getbifrost.ai/quickstart/gateway/provider-configuration)
- Helicone: P2C + PeakEWMA latency balancing across providers — [GitHub Helicone/ai-gateway](https://github.com/Helicone/ai-gateway)
- OpenRouter: rolling throughput/latency percentiles used for sorting and deprioritizing — [OpenRouter provider routing](https://openrouter.ai/docs/features/provider-routing)

### Inferences
- KVStream's calibrated KV-token budget and predictive shedding are a clear differentiator. These gateways treat backends as remote, effectively elastic APIs governed by provider rate limits. KVStream treats the backend as a single finite machine. That is a gap you could plug into rather than compete with: put KVStream in front of Foundry Local and register it as an OpenAI-compatible "custom provider" in Bifrost, LiteLLM or TensorZero.
- Borrowable idea: PeakEWMA latency tracking (from Helicone) could feed KVStream's predictive shedding as a smoothed service-time estimate.

### Gaps
- Absence of evidence comes from the docs and READMEs fetched. I did not search source code or issues for adaptive-concurrency experiments (e.g. AIMD or Vegas-style limiters).

## Streaming robustness, client disconnects, mid-stream retries

### Takeaway
None of the gateways document mid-stream retry or failover. Retries and fallbacks apply before the first byte. TensorZero has the most useful streaming control: separate time-to-first-token (TTFT) and total timeouts for streams.

### Cited Findings
- TensorZero timeouts: `timeouts.non_streaming.total_ms`, `timeouts.streaming.ttft_ms` and `timeouts.streaming.total_ms`, with `global_outbound_http_timeout_ms` (default 15 min) as an upper bound. Retries use `num_retries` and `max_delay_s` with "truncated exponential backoff with jitter". Fallbacks run in order: model provider routing, then variant retries, then variant fallbacks (`candidate_variants` vs `fallback_variants`) — [TensorZero retries & fallbacks docs](https://raw.githubusercontent.com/tensorzero/tensorzero/main/docs/gateway/guides/retries-fallbacks.mdx)
- OpenRouter's provider-routing docs do not mention mid-stream fallback — [OpenRouter provider routing](https://openrouter.ai/docs/features/provider-routing)
- Bifrost: `default_request_timeout_in_seconds`, `max_retries`, `retry_backoff_initial`/`retry_backoff_max`. For custom providers, streaming must be explicitly allowed via `allowed_requests.chat_completion_stream` — [Bifrost provider configuration](https://docs.getbifrost.ai/quickstart/gateway/provider-configuration)

### Inferences
- A TTFT timeout is a cheap, high-value addition for KVStream. A hung local model (stuck NPU, prefill stall) can be detected and counted toward the circuit breaker before the total timeout fires.
- KVStream's coalescing of streaming requests (fan-out of one upstream stream to N clients) was not found in any of these gateways. It needs disconnect semantics: cancel upstream only when the last subscriber leaves. This is a differentiator worth documenting.

### Gaps
- How each gateway propagates a client disconnect to the upstream (cancelling the upstream HTTP request to free backend compute) was not found in the docs fetched for any product. You would need to read the source (LiteLLM `proxy_server.py`, Bifrost transport) or the GitHub issues.

## Notably well-designed patterns KVStream should borrow

### Takeaway
The ideas most worth borrowing are TensorZero's feedback/experimentation data model and TTFT timeouts, Bifrost's simple per-provider worker-pool/buffer knobs, Portkey's circuit-breaker semantics, LiteLLM's session affinity and priority scheduler concept, and OpenRouter's rolling-percentile endpoint stats.

### Cited Findings
- TensorZero stores inferences and feedback in the user's own DB to drive optimization and adaptive A/B testing — [GitHub tensorzero/tensorzero](https://github.com/tensorzero/tensorzero)
- LiteLLM session affinity pins a conversation to a deployment, with a TTL — [LiteLLM routing docs](https://docs.litellm.ai/docs/routing)
- Bifrost's single-binary distribution via npx and Docker, a web UI, and native Prometheus — [GitHub maximhq/bifrost](https://github.com/maximhq/bifrost)
- OpenRouter's `preferred_min_throughput` deprioritizes slow endpoints rather than excluding them — [OpenRouter provider routing](https://openrouter.ai/docs/features/provider-routing)

### Inferences
- Session affinity is already a familiar pattern from LiteLLM. For KVStream on a single backend, the equivalent is "session awareness": admit a session's follow-up calls ahead of new sessions, so in-flight agent workflows finish (and benefit from any prefix/KV reuse in Foundry Local) instead of being shed mid-workflow.
- An optional feedback endpoint in the TensorZero style (`inference_id` → outcome) would let KVStream's agentic users connect shed and latency events to task success. This is cheap to add with SQLite.
- Idempotency keys: none of the fetched docs advertise them. KVStream's exact-match cache plus coalescing is close to idempotency. Exposing an `Idempotency-Key` header that maps onto the coalescing key would be a small step and a differentiator.

### Gaps
- I did not verify whether any gateway supports `Idempotency-Key` headers. Absence here is not proof.

## Known weaknesses and criticisms (with sources)

### Takeaway
LiteLLM is criticized most often, for Python-level throughput, latency and memory under load. Its own team acknowledges past failures around 1,000 QPS. Most of the harsh third-party numbers come from posts that promote competitors, so treat them as low-confidence. There are also trust and continuity risks: the LiteLLM PyPI hijack, Helicone in maintenance mode, and Portkey under new ownership.

### Cited Findings
- LiteLLM's own blog says that before the 2026 sidecar work, it "failed at around 1,000 QPS" in their benchmark — [LiteLLM blog](https://docs.litellm.ai/blog/sub-millisecond-proxy-overhead)
- Third-party claims, surfaced only in search snippets from dev.to posts promoting Bifrost by the same author (debmckinney). The fetch of the benchmark post returned 404 and author affiliation was not verified:
  - P99 latency of about 90 s and throughput dropping to 44 RPS under sustained load, unreliable above 500 RPS
  - Gradual slowdown requiring worker recycling after about 10k requests
  - About 500 µs mean overhead
  - Low confidence, likely vendor-adjacent — [dev.to "We benchmarked 5 LLM gateways at 5000 RPS"](https://dev.to/debmckinney/we-benchmarked-5-llm-gateways-at-5000-rps-heres-what-broke-28f3); [dev.to "Why production teams are migrating away from LiteLLM"](https://dev.to/debmckinney/why-production-teams-are-migrating-away-from-litellm-and-where-theyre-going-3dc2)
- LiteLLM PyPI compromise of 2026-03-24 (versions 1.82.7/1.82.8) — [bastion.tech](https://bastion.tech/blog/litellm-pypi-supply-chain-attack)
- Helicone has been in maintenance mode since the Mintlify acquisition (2026-03-03) — [Dealroom](https://ai-startups.dealroom.co/news/feed/mintlify-acquires-open-source-ai-observability-platform-helicone)
- The LiteLLM scheduler is still beta and labelled "Use for testing only" — [LiteLLM scheduler docs](https://docs.litellm.ai/docs/scheduler)
- Portkey's circuit breaker does not cover `conditional` routing, and its minimum cooldown is 30 s — [Portkey circuit breaker docs](https://portkey.ai/docs/product/ai-gateway/circuit-breaker)

### Inferences
- KVStream is single-process Python/FastAPI, so the LiteLLM criticism applies in principle. On one machine with local inference, though, the backend's tokens/s is the bottleneck by orders of magnitude, not the gateway. A published overhead benchmark (KVStream against a mock backend) would pre-empt the criticism.
- Supply-chain hygiene (pinned dependencies, trusted publishing, no long-lived PyPI tokens in CI) is a lesson that applies directly if KVStream publishes to PyPI.

### Gaps
- I did not find specific GitHub issue URLs for LiteLLM memory leaks or latency regressions. The claims above trace back to vendor-adjacent blogs.
- I found no independent (non-vendor) benchmark comparing Bifrost, TensorZero, Helicone, Portkey and LiteLLM under identical conditions.
- I did not confirm whether Foundry Local is explicitly listed as a provider by any gateway. All of them can reach it through generic OpenAI-compatible or custom-provider configuration (Bifrost: `base_provider_type: "openai"` with `allow_private_network`; LLM Gateway: custom OpenAI-compatible endpoints; TensorZero: OpenAI-compatible) — [Bifrost provider configuration](https://docs.getbifrost.ai/quickstart/gateway/provider-configuration); [LLM Gateway docs](https://docs.llmgateway.io/); [GitHub tensorzero/tensorzero](https://github.com/tensorzero/tensorzero)
