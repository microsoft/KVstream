# Agent-native, Envoy-based and MCP gateways (as of 2026-10-07)

Scope: agentgateway, kgateway, Solo.io Gloo AI Gateway / Solo Enterprise for agentgateway, Envoy AI Gateway (now "Agent Router"), Docker MCP Gateway, IBM ContextForge, Microsoft MCP Gateway. Purpose: patterns KVStream should adopt, where it is different, and whether it should sit behind one of these.

**Big 2026 changes to know first:**
- Envoy AI Gateway was **renamed "Agent Router"** and joined the Agentic AI Foundation (AAIF) in September 2026. The code, maintainers, CRDs and API group are unchanged, and aigateway.envoyproxy.io now 301-redirects to theagentrouter.ai. Sources: [AAIF blog](https://aaif.io/blog/agent-router-powerful-traffic-handling-for-agent-builders), [GitHub repo](https://github.com/envoyproxy/ai-gateway), [docs](https://theagentrouter.ai/docs/).
- agentgateway also **joined AAIF** ([AAIF v1.3.0 blog](https://aaif.io/blog/agentgateway-v1-3-0-llm-consumption-reimagined)). Its README still calls it a "Linux Foundation project" ([GitHub](https://github.com/agentgateway/agentgateway)). Its current stable release is v1.6.0, released 2026-10-02 ([releases](https://github.com/agentgateway/agentgateway/releases)).

## Q1: How does agentgateway handle LLM vs MCP vs A2A traffic? Does it do token rate limiting, prompt guards and failover? Can it run as one local binary in front of a localhost OpenAI-compatible server?

### Takeaway
agentgateway is a single Rust data plane that handles LLM, MCP, A2A and plain HTTP/gRPC traffic. It has request- and token-based rate limiting (local token bucket or a remote Envoy RLS), guardrails, priority-group failover with outlier detection, JWT/API-key/OAuth auth and CEL RBAC. It runs standalone from a `config.yaml` and can target `http://localhost:<port>` through a "custom" OpenAI-compatible provider, so it can sit directly in front of KVStream.

### Cited Findings
**Identity, licence and governance**
- Written in Rust; about 5.2k stars and 926 forks; Apache-2.0; described as a "Linux Foundation project" — [GitHub](https://github.com/agentgateway/agentgateway)
- Joined the Agentic AI Foundation (v1.3.0 post dated June–July 2026) — [AAIF blog](https://aaif.io/blog/agentgateway-v1-3-0-llm-consumption-reimagined)

**Release cadence (roughly one stable minor release per month or two)**
- v1.5.0 (2026-08-27); v1.6.0-alpha.1 (09-14); alpha.2 (09-22); rc.1 (10-01); v1.6.0 (10-02) — [releases](https://github.com/agentgateway/agentgateway/releases)
- v1.6.0 adds: "session affinity, per-key local rate limits, graceful draining, and richer OpenTelemetry output", zero-config cost tracking, better OIDC/JWT, and enables the k8s `AgentgatewayModel` API by default — [releases](https://github.com/agentgateway/agentgateway/releases)
- v1.5.0 adds: API-key-scoped LLM budgets, native Gemini inbound API, Anthropic Messages to OpenAI Responses conversion, and SPIFFE identities — [releases](https://github.com/agentgateway/agentgateway/releases)
- v1.3.0 adds: "virtual models" with weighted, failover (retry other providers on errors or rate limits) and CEL-conditional routing; token and dollar attribution per user/team/tool/model with hard team quotas; request buffering; "capacity-weighted load balancing"; and TLS/CORS support for local LLMs — [AAIF blog](https://aaif.io/blog/agentgateway-v1-3-0-llm-consumption-reimagined)

**LLM features**
- 25+ first-class providers, including OpenAI, Anthropic, Bedrock, Vertex/Gemini, Mistral, Groq and **Ollama** — [agentgateway docs: providers](https://agentgateway.dev/docs/standalone/latest/llm/providers/)
- Self-hosted OpenAI-compatible servers (vLLM, LM Studio and so on) use a `custom` provider with `baseUrl: "http://localhost:8000"` and `formats: [{type: completions}]` — [providers](https://agentgateway.dev/docs/standalone/latest/llm/providers/)
- The README lists "budget and spend controls, prompt enrichment, load balancing, and failover" — [GitHub](https://github.com/agentgateway/agentgateway)
- Guardrails: "regex, OpenAI moderation, AWS Bedrock Guardrails, Google Model Armor, and custom webhooks" — [GitHub](https://github.com/agentgateway/agentgateway)
- Failover (as documented for the Solo enterprise distribution): priority groups are tiers; models within a group are load-balanced; outlier detection evicts throttled or unhealthy backends; when a whole group is evicted, traffic moves to the next group — [Solo docs: failover](https://docs.solo.io/agentgateway/latest/llm/failover/)

**Rate limiting**
- Local limits use an in-memory token bucket that is not shared across replicas. Remote limits use the Envoy Rate Limit gRPC service, for example the reference `ratelimit` server backed by Redis — [rate limits docs](https://agentgateway.dev/docs/standalone/latest/configuration/resiliency/rate-limits/)
- Configured with `type: requests | tokens`, `maxTokens`, `tokensPerFill`, `fillInterval`, and an optional CEL `key` (for example `apiKey.user`, which gives each API key "1000 tokens per hour") — [rate limits docs](https://agentgateway.dev/docs/standalone/latest/configuration/resiliency/rate-limits/)
- Token limits are checked in two phases. At request time the check only happens if `tokenize: true` (an estimate); otherwise the request goes through unconditionally. At response time actual usage is charged, including prompt-cache read/write tokens — [rate limits docs](https://agentgateway.dev/docs/standalone/latest/configuration/resiliency/rate-limits/)

**MCP and A2A**
- MCP: tool federation; stdio, HTTP, SSE and Streamable HTTP transports; OpenAPI-to-MCP; OAuth. A2A: capability discovery, modality negotiation and task collaboration — [GitHub](https://github.com/agentgateway/agentgateway)
- Treats MCP as stateful (a `SessionManager`). With several backends it multiplexes: it fans `tools/list` out to each backend, renames tools to `${backend}_${tool}`, and routes each call back to its backend. Visible tools are filtered per client or session by policy (JWT claims, scopes, CEL) — [DEV review](https://dev.to/spacewander/agentgateway-review-a-feature-rich-new-ai-gateway-53lm); [Solo blog](https://www.solo.io/blog/why-do-we-need-a-new-gateway-for-ai-agents)

**Security, observability and deployment**
- "Auth (JWT, API keys, OAuth), fine-grained RBAC with CEL policy engine, rate limiting, TLS, and OpenTelemetry" — [GitHub](https://github.com/agentgateway/agentgateway)
- Deploys as a standalone quickstart or on Kubernetes through a built-in controller with Gateway API support — [GitHub](https://github.com/agentgateway/agentgateway)

### Inferences
- Running agentgateway as a standalone binary in front of `http://127.0.0.1:<kvstream-port>` (custom provider, `completions` format) would add API keys/JWT, TLS, CEL RBAC, per-key token budgets, guardrails, OTel and MCP/A2A without changing KVStream.
- agentgateway's token limit is a *quota* (a refilled budget, charged after the response). It is not a *capacity* admission control. It does not know the backend's KV-cache size, whether the runtime serialises requests, or the live queue depth. "Capacity-weighted load balancing" spreads load across backends; it does not measure capacity. That remains KVStream's job.
- Its outlier detection and failover would treat KVStream's 503 + Retry-After as a signal to evict and fail over, which works well if a cloud fallback model is configured.

### Gaps
- I did not confirm the exact binary install command, the default admin UI port, or whether outlier detection honours `Retry-After`.
- I found no evidence of a priority-class queue or of adaptive concurrency (gradient/AIMD) inside agentgateway itself.
- Semantic or exact caching was not listed in the sources I fetched. "Request buffering" in v1.3.0 is not a response cache.

## Q2: What does Envoy AI Gateway (now Agent Router) offer for token-based rate limiting and backend failover, and how does it relate to the Gateway API Inference Extension?

### Takeaway
Agent Router (formerly Envoy AI Gateway) is a Go control plane plus an Envoy external-processor, built on Envoy Gateway. It meters tokens per request (input, cached input, output, total, or a CEL expression) and enforces limits through Envoy's global rate limit service (Redis). It retries failed calls at the next provider in that provider's native format. It integrates the Gateway API Inference Extension (InferencePool plus an endpoint picker) for self-hosted model pools. It also has an `aigw run` standalone mode for laptops.

### Cited Findings
- Go; about 2.2k stars; Apache-2.0; the repo moved to `theagentrouter/agent-router`; image and CLI names are unchanged — [GitHub](https://github.com/envoyproxy/ai-gateway)
- Two-tier model: Tier One handles auth and global rate limits; Tier Two handles self-hosted model access. 15+ providers — [GitHub](https://github.com/envoyproxy/ai-gateway)
- "Run Agent Router as a standalone, OpenAI-compatible router on your laptop with one command" (`aigw run`) — [GitHub](https://github.com/envoyproxy/ai-gateway)
- Docs list version 1.2 as current — [docs](https://theagentrouter.ai/docs/)
- Token limits use Envoy Gateway's Global Rate Limit API. `llmRequestCosts` types are InputToken, CachedInputToken, OutputToken, TotalToken and CEL (weighted costs). Usage is taken from OpenAI-schema responses. If the accumulated count is already over the limit, the next request is rejected with 429 — [usage-based rate limiting docs](https://aigateway.envoyproxy.io/docs/capabilities/traffic/usage-based-ratelimiting) (the URL now redirects to theagentrouter.ai; the content comes from the search snippet because the redirected path returned 404)
- Limits apply "on input, output, or total tokens per team, application, or model". Failover "retries a failed call at the next provider, in that provider's format, with that provider's credentials". The MCP tool catalog is "filtered by identity at discovery and again at invocation" — [AAIF blog](https://aaif.io/blog/agent-router-powerful-traffic-handling-for-agent-builders)
- Self-hosted inference: HTTPRoute + InferencePool, or AIGatewayRoute + InferencePool; custom endpoint picker (EPP) providers; "Intelligent Endpoint Selection" based on real-time metrics — [inference docs](https://theagentrouter.ai/docs/capabilities/inference/)

**Gateway API Inference Extension (a Kubernetes SIG project that Agent Router, kgateway, GKE, Istio and NGINX Gateway Fabric implement)**
- The EPP scrapes each model server's KV-cache utilisation, pending queue length, prefix-cache indexes and active LoRA adapters — [CNCF deep dive, Apr 2025 (older)](https://cncf.io/blog/2025/04/21/deep-dive-into-the-gateway-api-inference-extension)
- Filters use request criticality (Critical or Sheddable), queue size (Critical requests look for endpoints with a queue under 50), LoRA affinity and KV usage — [kgateway blog](https://kgateway.dev/blog/deep-dive-inference-extensions)
- It sheds load when queue depth or KV utilisation exceed safety limits — [GKE docs](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/about-gke-inference-gateway)

### Inferences
- The Inference Extension is the closest analogue to KVStream's KV-aware admission. However, it is a *multi-replica router* that depends on Prometheus metrics exported by vLLM-class servers. Foundry Local exposes no KV or queue metrics, which is exactly why KVStream calibrates a KV-token budget itself. The "Critical vs Sheddable" criticality class is a pattern worth copying (a priority-class header).
- KVStream could in principle expose vLLM-style metrics (queue length, KV utilisation estimate) so that it can act as an InferencePool endpoint behind an EPP. This is speculative and would require k8s.

### Gaps
- I could not fetch the exact fallback CRD fields or the Agent Router release dates or cadence for 2026.
- It is unclear whether the 2026 version checks token limits before forwarding with a prompt estimate or only uses the accumulated count.

## Q3: kgateway and Solo.io Gloo AI Gateway / Solo Enterprise for agentgateway

### Takeaway
kgateway is the CNCF Sandbox Kubernetes control plane (formerly Gloo open source). It drives either Envoy or agentgateway data planes. Solo's commercial line is Gloo Gateway 2.0 plus "Solo Enterprise for agentgateway", and both are k8s-first. Neither is relevant for one machine except through standalone agentgateway.

### Cited Findings
- kgateway was accepted into the CNCF Sandbox on 2025-03-04. It is an Envoy-powered, Gateway API control plane — [CNCF](https://www.cncf.io/projects/kgateway/)
- kgateway v2.1 (Nov 2025, older) is the first version to integrate agentgateway as a data plane — [CNCF blog](https://www.cncf.io/blog/2025/11/18/kgateway-v2-1-is-released/)
- The control plane drives either the Envoy kgateway proxy or the agentgateway proxy — [kgateway docs](https://kgateway.dev/docs/agentgateway/main/about/architecture)
- Gloo Gateway 2.0 integrates kgateway and the Gateway API. Solo Enterprise for agentgateway is described as the "first enterprise-ready distribution" of an AI-native data plane for MCP governance — [efficientlyconnected](https://www.efficientlyconnected.com/solo-io-bridges-cloud-native-and-ai-native-worlds-with-gloo-gateway-2-0-and-agentgateway-enterprise/)
- Enterprise versions are date-based: 2026.6.3 is the latest in the 6.x line, and 2026.8.0 is cited as latest elsewhere — [Solo docs](https://docs.solo.io/agentgateway/latest/). The two figures are possibly inconsistent and likely reflect different release lines.
- Gloo AI gateway docs (Gloo Gateway 2.0.x) cover AI features — [Solo docs](https://docs.solo.io/gateway/2.0.x/ai/)

### Inferences
- For KVStream (one machine), kgateway and Gloo add Kubernetes overhead with no benefit. The relevant open-source piece is standalone agentgateway.

### Gaps
- kgateway stars and 2026 release versions were not retrieved.
- I did not verify whether the Gloo AI Gateway brand has been formally retired in favour of agentgateway enterprise.

## Q4: How do MCP gateways handle sessions, tool federation and auth, and what would integration mean for a local inference gateway?

### Takeaway
MCP gateways federate tool servers, not model inference. They handle MCP session state (session IDs and sticky routing), aggregate and namespace tools, filter tools per identity, and inject credentials. A local inference gateway like KVStream is a different layer: the LLM endpoint the agent calls. Integration means coexisting (agent → MCP gateway for tools, agent → KVStream for completions), or putting both behind agentgateway, which proxies both kinds of traffic.

### Cited Findings
**Docker MCP Gateway**
- Go; MIT licence; about 1.6k stars. Architecture is "AI Client → MCP Gateway → MCP Servers (Docker Containers)". Supports stdio (the default), SSE and streaming transports. Tools are allowlisted per server or profile; secrets are handled through Docker Desktop; OAuth flows are built in; there is logging and call tracing. Requires Docker Desktop 4.59+ — [GitHub](https://github.com/docker/mcp-gateway)
- MCP servers start on demand as containers when a tool is called. They have restricted privileges, network access and resources. The gateway injects credentials. `--profile` selects which servers are exposed. It can be installed on plain Docker Engine as a CLI plugin — [Docker docs](https://docs.docker.com/ai/mcp-catalog-and-toolkit/mcp-gateway/)

**IBM ContextForge**
- Python 3.11+ with FastAPI, plus Rust components; about 4.6k stars; Apache-2.0; latest release seen was 1.0.0-RC-3 — [GitHub](https://github.com/IBM/mcp-context-forge)
- Federates MCP, A2A and REST/gRPC (via reflection) into virtual MCP servers. Transports: HTTP, JSON-RPC, WebSocket, SSE, stdio. Auth: Basic/JWT/custom plus RBAC and user-scoped OAuth. Built-in auth, retries and rate limiting. Redis-backed caching and session storage. OTel (Phoenix/Jaeger/Zipkin). HTMX admin UI. Deploys via PyPI (`mcp-contextforge-gateway`), Docker or Helm, with multi-cluster federation — [GitHub](https://github.com/IBM/mcp-context-forge)

**Microsoft MCP Gateway**
- Written in C#, targets Kubernetes. Data plane plus control plane. "Session-aware stateful routing" sends every request with a given `session_id` to the same MCP server instance — [Algolia DocSearch mirror of microsoft/mcp-gateway](https://docsearch.algolia.com/mcp/docs/repo/microsoft/mcp-gateway) (secondary source)

**Other gateways**
- agentgateway's MCP session manager, multiplexing and tool renaming are covered under Q1 — [DEV review](https://dev.to/spacewander/agentgateway-review-a-feature-rich-new-ai-gateway-53lm)
- Agent Router filters tools by identity at both discovery and invocation — [AAIF blog](https://aaif.io/blog/agent-router-powerful-traffic-handling-for-agent-builders)

### Inferences
- **Session awareness pattern:** MCP gateways key affinity on a session ID. KVStream could accept an optional session or agent-run header (for example `X-Session-Id`) to schedule a multi-call agent run fairly or keep it together (for example, avoid starving mid-run calls). This is a cheap analogue that no inference gateway in this set provides.
- **Do not build MCP into KVStream.** ContextForge, Docker MCP Gateway and agentgateway already cover tool federation. KVStream's value is on the model-call side.
- ContextForge shares KVStream's stack (Python/FastAPI), which suggests Python is acceptable for gateway control logic at single-node scale.

### Gaps
- I did not fetch the MCP spec's session semantics (the `Mcp-Session-Id` header in Streamable HTTP) for citation.
- I did not verify Microsoft MCP Gateway's star count or 2026 release status.
- ContextForge's GA date beyond RC-3 is unconfirmed.

## Q5: What would KVStream get cheaply by deploying behind agentgateway, and what can these gateways not do that KVStream does?

### Takeaway
Behind agentgateway, KVStream gets auth (API keys/JWT/OAuth), TLS, CEL RBAC, per-key token budgets and quotas, guardrails, OTel, cost attribution, MCP/A2A and cloud failover. None of the surveyed gateways measures a single local runtime's real capacity, does KV-token admission against a calibrated budget, understands a serialising ONNX runtime, or queues with predictive shedding for one localhost backend. Those remain KVStream's niche, which makes "agentgateway in front, KVStream as the local capacity-aware backend" the natural architecture.

### Cited Findings
- Features available from agentgateway: JWT/API keys/OAuth, CEL RBAC, TLS, OTel — [GitHub](https://github.com/agentgateway/agentgateway)
- Per-key token limits — [rate limits docs](https://agentgateway.dev/docs/standalone/latest/configuration/resiliency/rate-limits/)
- Virtual models with failover to other providers on error or rate limit, and local LLM TLS/CORS — [AAIF blog](https://aaif.io/blog/agentgateway-v1-3-0-llm-consumption-reimagined)
- Session affinity and graceful draining (v1.6.0) — [releases](https://github.com/agentgateway/agentgateway/releases)
- agentgateway's token limits are charged after the response and are only estimated up front when `tokenize: true`. They are quota buckets, not concurrency or KV admission — [rate limits docs](https://agentgateway.dev/docs/standalone/latest/configuration/resiliency/rate-limits/)
- Envoy AI Gateway / Agent Router's token limits are likewise quota-style: limits are checked against accumulated usage, and 429 is returned once the limit is exceeded — [usage-based rate limiting docs](https://aigateway.envoyproxy.io/docs/capabilities/traffic/usage-based-ratelimiting)
- Capacity-aware routing exists only in the Inference Extension EPP, which relies on model-server metrics (KV utilisation, queue length) scraped from servers like vLLM and uses criticality classes — [CNCF deep dive (2025)](https://cncf.io/blog/2025/04/21/deep-dive-into-the-gateway-api-inference-extension); [kgateway blog](https://kgateway.dev/blog/deep-dive-inference-extensions)

### Inferences
**Patterns worth adopting in KVStream (all cheap):**
1. A priority or criticality header (Critical vs Sheddable), so shedding drops low-priority requests first (from the Inference Extension).
2. An optional session or run ID for fairness or affinity across an agent's calls (from the MCP gateways and agentgateway v1.6 session affinity).
3. An OTel GenAI-semantic span and metric export, so traces line up with agentgateway's.
4. vLLM-style Prometheus metric names (`num_requests_waiting`, a KV-utilisation estimate), so upstream gateways or an EPP could read KVStream's load.
5. Honour or emit `Retry-After` consistently, so an upstream failover policy can use it.

**Where KVStream is differentiated:**
- Measured capacity and a calibrated KV-token budget for a backend that publishes no metrics (Foundry Local).
- Single-runtime awareness, including when the runtime serialises requests.
- FIFO queue with predictive shedding.
- Exact-match cache and request coalescing, including streaming. None of the gateways surveyed listed coalescing.
- Ephemeral-port discovery for Foundry Local.

**Recommendation:** don't compete on auth, MCP or multi-provider features; document an "agentgateway → KVStream → Foundry Local" reference config. The risk is that agentgateway adds an extra hop, plus a second Rust process, on a "basic machine". Its latency and memory footprint at single-user scale were not measured in any source found.

### Gaps
- I found no source on agentgateway or Agent Router resource footprint on a laptop.
- I found no gateway that integrates Foundry Local specifically (it was not in any provider list seen).
- Semantic caching availability in agentgateway or Agent Router for 2026 is unverified.
