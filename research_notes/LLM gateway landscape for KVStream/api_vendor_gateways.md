# API-gateway / edge vendor AI gateways and semantic routers (Kong, Cloudflare, APISIX, Azure APIM, vLLM Semantic Router)

Research date: 2026-10-07. Doc pages fetched on that date; their "last updated" stamps are noted where available. Fetches went through a summarising tool, so wording is paraphrased unless in quotes.

## Q1. How do token rate limiters handle output tokens being unknown at admission time? (vs KVStream reserve-then-settle)

### Takeaway
None of the four vendors reserves output tokens before the request. All of them do **post-hoc settlement**: they read `usage` from the response and charge the counter afterwards. That means concurrent requests can overshoot the limit, and a caller only gets blocked on the *next* request. Azure APIM is the most explicit about this and adds an optional pre-request *prompt-only* estimate. Cloudflare does not limit by tokens at all; its limiter counts requests, and its separate Spend Limits feature caps cost. KVStream reserves an estimate of prompt plus max output at admission, then settles to actual usage, and that is stricter than anything these vendors document.

### Cited Findings
**Azure APIM `llm-token-limit`** (doc ms.date 2026-04-01, updated 2026-06-26)
- Limits tokens per `counter-key` (any policy expression: subscription, IP, header). It supports `tokens-per-minute` (rate) and/or `token-quota` over Hourly/Daily/Weekly/Monthly/Yearly fixed windows. Exceeding the rate gives **429**, exceeding the quota gives **403**, with a `Retry-After` header (custom name allowed) plus optional remaining-tokens and tokens-consumed headers — [MS Learn llm-token-limit](https://learn.microsoft.com/en-us/azure/api-management/llm-token-limit-policy)
- `estimate-prompt-tokens` is a required boolean. With `false`, the policy uses actual `usage` from the response, and "Prompts may be sent to the backend even when the limit is exceeded; this is detected from the response, after which subsequent requests are blocked until the limit resets." With `true`, it estimates prompt tokens from the prompt schema before sending, which saves backend calls when the limit is already exceeded but "may reduce performance." — [MS Learn llm-token-limit](https://learn.microsoft.com/en-us/azure/api-management/llm-token-limit-policy)
- Streaming (`stream: true`): prompt tokens are **always estimated**, and completion tokens are also estimated. Images are overcounted at up to 1200 tokens each when streaming or estimating — [MS Learn llm-token-limit](https://learn.microsoft.com/en-us/azure/api-management/llm-token-limit-policy)
- Concurrency caveat, stated outright: "Because the exact number of tokens consumed can't be determined until responses are received from the backend, concurrent or near-concurrent requests can temporarily exceed the configured token limit." The remaining-quota header "may be larger than expected" — [MS Learn llm-token-limit](https://learn.microsoft.com/en-us/azure/api-management/llm-token-limit-policy)
- The v2 tiers use a **token bucket**; classic tiers use a **sliding window**. Counters are per gateway and are not aggregated across regional or workspace gateways. The policy is available on the **self-hosted gateway**. Supported schemas are OpenAI Chat Completions/Responses, Anthropic Messages (v2 tiers) and Vertex AI — [MS Learn llm-token-limit](https://learn.microsoft.com/en-us/azure/api-management/llm-token-limit-policy)

**Kong `ai-rate-limiting-advanced`** (Enterprise only)
- Strategies are total, prompt or completion tokens, plus a **cost** strategy (v3.8+): `(prompt_tokens × input_cost + completion_tokens × output_cost)/1e6` — [Kong docs](https://developer.konghq.com/plugins/ai-rate-limiting-advanced/)
- Enforcement is **after** the LLM call: "The cost for the AI Proxy or AI Proxy Advanced is only reflected during the next request." There is no pre-request estimation; it uses provider-reported counts — [Kong docs](https://developer.konghq.com/plugins/ai-rate-limiting-advanced/)
- Counter storage can be local (in-memory, per node), cluster (Kong DB) or Redis. Caveat: limits apply to the *requested* model, so after failover to another model a request may succeed even if that model's limit is reached — [Kong docs](https://developer.konghq.com/plugins/ai-rate-limiting-advanced/)

**Apache APISIX `ai-rate-limiting`** (APISIX 3.19.0 docs; open source, Apache-2.0)
- `limit_strategy` can be `total_tokens`, `prompt_tokens`, `completion_tokens` or `expression` (`cost_expr` over raw usage fields). Tokens are taken from the LLM **response**, so enforcement happens after the response — [APISIX docs](https://apisix.apache.org/docs/apisix/plugins/ai-rate-limiting/)
- Default `rejected_code` is **503**, configurable to 429. Headers are `X-AI-RateLimit-Limit/Remaining/Reset-*`. Quotas can be set per instance, and with `ai-proxy-multi` plus `fallback_strategy` including rate limiting, traffic goes to another instance once one instance's quota is used up. Counter policies are local, redis, redis-cluster or redis-sentinel — [APISIX docs](https://apisix.apache.org/docs/apisix/plugins/ai-rate-limiting/)

**Cloudflare AI Gateway**
- Rate limiting counts **requests**, not tokens, with fixed or sliding windows. Over-limit requests get 429 and are not processed — [Cloudflare rate limiting](https://developers.cloudflare.com/ai-gateway/features/rate-limiting/)
- Separately there are "Spend Limits", which are cost budgets tracked cumulatively and scoped by model, provider or custom metadata (features page last updated 2026-09-30) — [Cloudflare features](https://developers.cloudflare.com/ai-gateway/features/)

### Inferences
- The industry norm is **post-hoc settlement plus optional prompt estimate**, and the vendors accept overshoot because they protect *quota and billing*, not backend capacity. KVStream protects *KV memory and latency* on one box, where overshoot means a slowdown or OOM. Reserving max output at admission and settling later is the right choice for that goal, and it sets KVStream apart. It is worth stating in KVStream docs as a contrast with the APIM "may temporarily exceed" behaviour.
- Patterns worth adopting from APIM:
  - expose remaining-budget and tokens-consumed headers;
  - support a per-key counter, which would need a client identity such as a header, since KVStream has no auth;
  - support a quota with a different status code (403) from the rate limit (429);
  - record a streaming `include_usage` fallback, meaning estimate completion tokens when the stream is cut off.
- APISIX uses 503 as its default rejection code, the same choice as KVStream's load-shed 503. APIM and Cloudflare use 429 for rate limits. KVStream should keep 503 + Retry-After for *capacity* shedding and use 429 only if it ever adds per-client quotas. This is a semantic distinction worth documenting.

### Gaps
- No vendor documents reserving `max_tokens` up front. I found no source showing any of them does reserve-then-settle.
- I could not confirm whether Kong or APISIX estimate completion tokens for streamed responses that lack a usage chunk.

## Q2. How do semantic caches decide a hit, what are the failure modes, and is semantic caching OK for agentic tool-calling traffic?

### Takeaway
A hit is decided by embedding similarity against a vector store, compared to a threshold. Vendors themselves warn about wrong or unsafe answers. Defaults that embed only the last message (Kong `message_countback=1`) make conversation and tool context invisible to the cache key. Cloudflare still does **exact-match only** (semantic caching is still "planned"). For agentic tool-calling traffic, semantic caching is unsafe by default. KVStream's exact-match cache plus coalescing is the defensible choice, and semantic caching should at most be an opt-in for stateless, single-turn, tool-free requests.

### Cited Findings
**Azure APIM `llm-semantic-cache-lookup` / `-store`** (doc updated 2026-08-19)
- The lookup measures vector proximity of the prompt to earlier prompts using an embeddings backend (`embeddings-backend-id`, managed identity) and an external cache: Azure Managed Redis or another RediSearch-compatible cache — [MS Learn lookup](https://learn.microsoft.com/en-us/azure/api-management/azure-openai-semantic-cache-lookup-policy); [MS Learn AI gateway capabilities](https://learn.microsoft.com/en-us/azure/api-management/genai-gateway-capabilities)
- `score-threshold` runs from 0.0 to 1.0, where **lower means stricter** (it is a distance). Microsoft's guidance is to start at 0.05, and that "Score threshold above 0.2 may lead to cache mismatch." — [MS Learn lookup](https://learn.microsoft.com/en-us/azure/api-management/azure-openai-semantic-cache-lookup-policy)
- Further options:
  - `ignore-system-messages` (recommended `true`);
  - `max-message-count`, which skips caching after N dialog messages;
  - `vary-by`, which partitions the cache for cross-user isolation;
  - the store side's `duration`, in seconds.

  [MS Learn lookup](https://learn.microsoft.com/en-us/azure/api-management/azure-openai-semantic-cache-lookup-policy)
- Explicit warning: "Because semantic caching returns responses based on similarity (not exact match), it can surface responses that are incorrect, outdated, or unsafe for the current request." It recommends putting a rate-limit policy right after the lookup in case the cache is unavailable. There is also a security note: linked access to the embeddings backend isn't checked — [MS Learn lookup](https://learn.microsoft.com/en-us/azure/api-management/azure-openai-semantic-cache-lookup-policy)

**Kong `ai-semantic-cache`** (Enterprise only)
- Vector DB is Redis (RediSearch) or pgvector. `distance_metric` is cosine or euclidean, with a `vectordb.threshold`. Embedding providers include azure, bedrock, gemini, huggingface, mistral, **ollama** and openai — [Kong reference](https://developer.konghq.com/plugins/ai-semantic-cache/reference/)
- `message_countback` **defaults to 1**, so only the last message is vectorised. The flags `ignore_system_prompts`, `ignore_tool_prompts` and `ignore_assistant_prompts` all default to false. `exact_caching` is an optional exact-match first pass. `cache_ttl` defaults to 300 s, and `stop_on_failure` defaults to false — [Kong reference](https://developer.konghq.com/plugins/ai-semantic-cache/reference/)
- It honours Cache-Control (`no-store`, `no-cache`, `private`, `max-age`) and emits `X-Cache-Status` (Hit/Miss/Bypass/Refresh). Kong itself notes that "even when Exact Caching is enabled, the plugin may still return results for queries that are similar but not identical," and that the plugin may not suit limited hardware — [Kong docs](https://developer.konghq.com/plugins/ai-semantic-cache/)

**Cloudflare AI Gateway caching**
- Exact match only. The key is SHA-256 over provider, endpoint, model, auth header and the full body, and "Any difference in the body — including messages, tools, or model parameters — will result in a separate cache entry." The key can be overridden with `cf-aig-cache-key`. TTL comes from `cf-aig-cache-ttl` (60 s to 1 month, default 5 min), and `cf-aig-skip-cache` bypasses the cache. The page says: "We plan on adding semantic search for caching in the future." — [Cloudflare caching](https://developers.cloudflare.com/ai-gateway/features/caching/)

**Failure modes (secondary sources: practitioner blogs, lower authority)**
- The threshold sets the hit rate and the false-positive rate at the same time. Named failure modes are false hits, staleness, and negation/entity flips — [tianpan.co 2026-04-09](https://tianpan.co/blog/2026/04/09/semantic-caching-llm-production); [techplained](https://www.techplained.com/semantic-caching-llm)
- For agentic or tool-call traffic, one blog claims semantic cache hit rates of about 5–15%, because answers depend on prior context and state. It also warns that a cached tool-call response can re-issue a stale tool call with stale arguments — [tianpan.co caching hierarchy for agentic workloads](https://tianpan.co/blog/2026-04-12-caching-hierarchy-agentic-workloads-five-layers). These figures are unverified and come from a single blog.

### Inferences
- Under defaults (`message_countback=1`, tool prompts not ignored but also not part of the key beyond the last message), Kong could match two agent steps whose final user message looks alike but whose tool results differ. That would return a wrong `tool_calls` payload. APIM's `max-message-count` plus `vary-by` is the closest thing to a guard for this.
- For KVStream's agent traffic, the right moves are:
  - keep the exact-match cache, which already matches Cloudflare's design (full-body hash including tools and params);
  - borrow Cloudflare's header controls (`skip-cache`, `ttl`, custom key) and Kong's `X-Cache-Status` header;
  - if semantic caching is ever added, gate it to: no `tools` in the request, no `tool` or assistant messages, temperature 0, low message count, `vary-by` the client.
- Running an embedding model on the same CPU/GPU/NPU would compete with the main model on a basic machine. Kong itself flags limited hardware as a poor fit.

### Gaps
- I found no peer-reviewed measurement of semantic-cache false-hit rates on tool-calling traffic. The figures above are from blogs.
- Kong's default threshold value is not given in the reference page.

## Q3. What do Azure APIM's llm-token-limit, emit-token-metric and semantic cache policies do as a Microsoft-ecosystem reference design?

### Takeaway
APIM is the Microsoft reference AI gateway: an XML policy pipeline on a managed or **self-hosted** gateway. The `azure-openai-*` policies were renamed or generalised to `llm-*` and now cover OpenAI, Anthropic and Vertex schemas. It also provides:
- backend load balancing (round-robin, weighted, priority, session-aware);
- a circuit breaker whose trip duration follows the backend's `Retry-After`;
- content safety;
- MCP and A2A governance;
- an "AI gateway in Microsoft Foundry" integration (preview).

It is cloud-and-enterprise oriented and has no notion of local KV capacity.

### Cited Findings
- The AI gateway covers OpenAI Chat/Responses, Anthropic Messages (v2 tiers) and Vertex AI schemas; models in Microsoft Foundry or other providers such as Bedrock; a "unified model API (preview)" with one OpenAI-compatible endpoint; remote MCP servers and A2A agent APIs; and "**Self-hosted** models and endpoints" (doc updated 2026-06-25) — [MS Learn AI gateway capabilities](https://learn.microsoft.com/en-us/azure/api-management/genai-gateway-capabilities)
- Resiliency: the load balancer supports round-robin, weighted, priority-based and session-aware strategies. The circuit breaker "features dynamic trip duration, applying values from the `Retry-After` header provided by the backend." — [MS Learn AI gateway capabilities](https://learn.microsoft.com/en-us/azure/api-management/genai-gateway-capabilities)
- Security: managed identity to backends, OAuth through the credential manager (also for MCP servers), and the `llm-content-safety` policy with Azure AI Content Safety and Prompt Shields — [MS Learn AI gateway capabilities](https://learn.microsoft.com/en-us/azure/api-management/genai-gateway-capabilities); [MS Learn lookup](https://learn.microsoft.com/en-us/azure/api-management/azure-openai-semantic-cache-lookup-policy)
- `llm-emit-token-metric` (doc updated 2026-09-17):
  - sends total, prompt and completion token metrics to Application Insights, with cached and reasoning token categories in preview;
  - allows up to 5 custom dimensions, with defaults such as API ID, Subscription ID and Backend ID;
  - is limited to 100 values per dimension and 1,000 time series per namespace, beyond which data is "silently discarded";
  - uses response `usage` and needs `include_usage: true` for streamed OpenAI responses;
  - gives inaccurate counts if a stream is interrupted;
  - works on classic, v2, consumption, self-hosted and workspace gateways.

  [MS Learn emit-token-metric](https://learn.microsoft.com/en-us/azure/api-management/llm-emit-token-metric-policy)
- AI gateway in Microsoft Foundry (preview): token quotas and rate limits can be configured from the Foundry UI, and agents running anywhere, including on-premises, and MCP tools can be registered for governance — [MS Learn AI gateway capabilities](https://learn.microsoft.com/en-us/azure/api-management/genai-gateway-capabilities)
- Token limits and semantic cache behaviour: see Q1 and Q2.

### Inferences
- The integration story for KVStream is "APIM (self-hosted gateway, or cloud) north of KVStream for auth, per-tenant quotas, content safety and App Insights metrics; KVStream south for local capacity admission". The APIM circuit breaker honours `Retry-After`, so KVStream's 503 + Retry-After load shedding would be respected by an APIM load balancer/circuit breaker placed in front of it. This deserves a short integration note.
- Ideas worth borrowing:
  - emit OpenTelemetry/Prometheus token metrics with the same dimension names (API, client, backend, model), so KVStream metrics line up with APIM dashboards;
  - force `stream_options.include_usage=true` upstream, so streamed responses carry usage.
- Foundry *Local* is never mentioned in these APIM docs. "Self-hosted models and endpoints" is the generic path.

### Gaps
- No doc found showing APIM fronting Foundry Local specifically.
- Self-hosted gateway licensing and pricing was not researched.

## Q4. What does the vLLM Semantic Router do, are there similar semantic routers, and is it relevant to a single-model local box? (plus remaining vendor profiles)

### Takeaway
vLLM Semantic Router (vllm-sr) is an Apache-2.0, Go/Rust decision layer that runs as an Envoy `ext_proc` sidecar. It classifies each request (originally with ModernBERT) and rewrites the `model` field, the system prompt and the reasoning on/off setting. Its value is **multi-model / multi-backend** selection. On a single-model Foundry Local box, the only part that carries over is *per-request reasoning-mode or effort selection*, if the local model supports thinking toggles. Kong's and APISIX's "semantic" load-balancing algorithms are the gateway-native equivalents.

### Cited Findings
**vLLM Semantic Router**
- ModernBERT is used as "a lightweight, standalone classifier integrated into the router". Simple queries take a fast path and complex ones are put into chain-of-thought reasoning mode. The engine is "Written in Rust using Hugging Face Candle", and it "Works out-of-the-box with Kubernetes and Envoy via the `ext_proc` plugin". Reported gains are "~10% higher accuracy", "~50% lower latency" and "~50% fewer tokens"; the blog does not detail the evaluation set. It is designed for switching between models (blog dated 2025-09-11, so older) — [vLLM blog](https://vllm.ai/blog/semantic-router)
- As an ext_proc gRPC sidecar, it rewrites the request by replacing the model field, injecting a system prompt and optionally enabling reasoning. An academic write-up exists: "When to Reason: Semantic Router for vLLM", arXiv 2510.08731, which search results describe as a NeurIPS 2025 workshop paper. Only the search snippet was seen; I did not fetch it — [arXiv 2510.08731](https://arxiv.org/abs/2510.08731); [AIBrix docs](https://aibrix.readthedocs.io/latest/features/semantic-router.html)
- 2026 status from the repo README:
  - described as "An open, programmable decision layer for models and compute";
  - public model ID `vllm-sr/auto`;
  - routes across local, private and cloud backends;
  - capabilities listed: model selection or combination ("Mixture-of-Models"), signal-driven decisions, category-aware semantic cache, hallucination detection (LettuceDetect), LoRA classifiers;
  - installs via script or pip/uv; Go 1.25; Apache 2.0;
  - v0.4 "Hermes" released 2026-09-24, with its own "Decision"/"Vela" router foundation models (2026).

  [GitHub vllm-project/semantic-router](https://github.com/vllm-project/semantic-router); [README](https://raw.githubusercontent.com/vllm-project/semantic-router/main/README.md)
- A related standardisation effort is the IETF 124 HotRFC slides on a "Semantic Inference Routing Protocol (SIRP)" — [IETF datatracker](https://datatracker.ietf.org/meeting/124/materials/slides-124-hotrfc-sessa-06-semantic-inference-routing-protocol-sirp-00). I only saw the title.

**Similar semantic routing in gateways**
- Kong `ai-proxy-advanced` load-balancing algorithms: round-robin (weighted), consistent-hashing, least-connections, **lowest-latency (EWMA)**, **lowest-usage** (tokens or cost), **semantic** (similarity matching) and priority (tiered failover). Retries and failover are configurable; client errors don't fail over by default, but `http_429`, `http_502` and `non_idempotent` can be enabled. Providers can be mixed in fallback from v3.10. A **circuit breaker** (`balancer.max_fails` / `fail_timeout`) arrived in v3.13. Self-hosted providers include Llama2, Ollama, vLLM and Databricks. Enterprise only, Kong ≥ 3.8 — [Kong ai-proxy-advanced](https://developer.konghq.com/plugins/ai-proxy-advanced/)
- APISIX `ai-proxy-multi` (open source): roundrobin, chash and **semantic** (prompt similarity to per-instance examples via embeddings). Further features:
  - `fallback_strategy` on rate limiting, 429 or 5xx, and `fallback_http_statuses`;
  - `max_retries` and `retry_on_failure_within_ms`;
  - **active health checks**;
  - token usage logging to http-logger or kafka-logger;
  - providers include "OpenAI-compatible APIs", so self-hosted works.

  Related plugins are `ai-prompt-guard`, `ai-aws-content-moderation`, `ai-rag`, `ai-request-rewrite` and `ai-prompt-decorator` — [APISIX ai-proxy-multi](https://apisix.apache.org/docs/apisix/plugins/ai-proxy-multi/)
- Cloudflare: "Auto Router" picks the most cost-effective capable model with automatic fallbacks. "Dynamic Routing" is a visual flow builder covering user and geo routing, A/B and percentage splits, and content-aware routing; the route name is used as the `model` — [Cloudflare features](https://developers.cloudflare.com/ai-gateway/features/); [Cloudflare product page via search](https://cloudflare.com/product/ai-gateway)

**Remaining vendor profile facts**
- **Kong AI Gateway:**
  - covers LLM, **MCP** and A2A traffic; managed through Konnect (UI, API, kongctl); "AI Gateway 2.0" with a v1 to v2 migration guide;
  - observability through GenAI OTLP span attributes and metrics, audit logs and Konnect dashboards — [Kong AI Gateway](https://developer.konghq.com/ai-gateway/)
  - open-core split: AI Proxy, AI Prompt Guard, Prompt Template and Prompt Decorator were open-sourced in Gateway 3.6 (2024, older) — [Kong press release](https://konghq.com/company/press-room/press-release/ai-gateway); [SD Times](https://sdtimes.com/ai/kong-introduces-new-collection-of-ai-plugins-in-kong-gateway-3-6/)
  - AI Proxy Advanced, AI Rate Limiting Advanced, AI Semantic Cache, Semantic Prompt Guard, AI Sanitizer and AI MCP Proxy are Enterprise-only — [Kong ai-proxy-advanced](https://developer.konghq.com/plugins/ai-proxy-advanced/); [Kong ai-semantic-cache](https://developer.konghq.com/plugins/ai-semantic-cache/); third-party comparison: [api7.ai](https://api7.ai/kong-ai-gateway-vs-litellm). The third-party claim that Prompt Guard now needs a licence conflicts with the 3.6 open-source announcement and is unresolved.
- **Cloudflare AI Gateway** (features page updated 2026-09-30) offers:
  - caching (exact match), Spend Limits, request-count rate limiting, Auto Router and Dynamic Routing;
  - Guardrails (prompt and response moderation) and DLP (PII, financial and health data; flag or block);
  - authenticated gateway, BYOK (20+ providers), analytics with a GraphQL API, logging with Logpush, custom metadata, custom costs;
  - "Available on all plans".

  [Cloudflare features](https://developers.cloudflare.com/ai-gateway/features/); [Cloudflare overview](https://developers.cloudflare.com/ai-gateway/)
- **APISIX:** open source, Apache Software Foundation; docs at 3.19.0 — [APISIX ai-rate-limiting](https://apisix.apache.org/docs/apisix/plugins/ai-rate-limiting/)

### Inferences
- On one box with one model, model routing does nothing for KVStream. Two narrower ideas are relevant:
  1. **Reasoning-effort routing.** Classify a request as easy or hard and set thinking on or off, or cap `max_tokens`. This directly shrinks KV reservations and latency, and it fits KVStream's token-budget admission.
  2. **Lowest-latency/EWMA and Retry-After–driven circuit breaking**, if KVStream ever fronts multiple local devices (CPU/GPU/NPU endpoints as separate backends).
- Integrate rather than compete:
  - vllm-sr or Envoy can sit north of KVStream as the model picker;
  - Kong, APISIX or APIM can sit north for auth, tenancy, guards and MCP;
  - KVStream stays the capacity-aware admission layer next to Foundry Local.

  None of the profiled products models backend KV-cache capacity or does predictive queue-wait shedding. Their admission is either quota/billing-based or health/failover-based.
- Runtime and deployment: APISIX docs and Kong are both Lua plugin ecosystems (Kong ≥3.8 plugins, APISIX plugins). Cloudflare runs at Cloudflare's edge and is SaaS-only, so it cannot sit next to a local Foundry Local box except as a proxy back to a public endpoint.

### Gaps
- The OpenResty/Lua runtime for Kong and APISIX and the Workers runtime for Cloudflare are not stated in the pages fetched. They are well known, but I have no citation here.
- Cloudflare "custom providers" (arbitrary or self-hosted endpoints) are not confirmed in the fetched pages.
- The vllm-sr docs site (vllm-semantic-router.com) failed DNS, so the current signal types (keyword, embedding, domain, PII, jailbreak) and per-request classifier latency overhead were not verified for v0.4. The details above rely on the 2025 blog and the README.
- Kong's latest Gateway version number for 2026 was not confirmed (circuit breaker noted at v3.13+).
- The Kong MCP plugins (ai-mcp-proxy, OAuth2) and APISIX MCP support were not researched in depth.
