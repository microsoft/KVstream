# KVStream — Gap Analysis: Codebase vs. Technical Proposal (CLI 0.10.0 update)

Source document: *KVStream — A Concurrency Gateway for Microsoft Foundry Local, Technical Proposal (7 Jul 2026, CLI 0.10.0 update)*
Codebase reviewed: `kvstream-foundry` @ `version.py 1.0.0`, 48 tests passing.

> **Status — Milestones A–E delivered (2026-08-24).** Every gap is closed except
> G-15 (semantic cache) and G-19…G-24 (the fleet tier), both deliberately
> deferred, plus G-47/G-48 which the author decided to leave as they are. The
> suite is 292 tests, `ruff` and `mypy` clean, and CI covers Linux and Windows.
> Sections below describe the state *at review time* and are kept as written so
> the reasoning stays auditable; see §6–§10 for what changed. **The one
> substantive thing still outstanding is running the calibration sweep against
> live Foundry Local hardware.**

---

## 0. Verdict

The repo is an honest, clean implementation of roughly **P1 + half of P2**. It is *not* the product in the
document. Two of the document's numbered phases have **zero code**, and several capabilities the document
describes as "working today" or "core" are present in a reduced form that will not survive contact with a real
Foundry Local instance.

| Phase (doc §11) | Doc scope | Repo state | Completion |
|---|---|---|---|
| **P0** Version-aware backend resolution | Ordered chain: explicit → CLI-assisted (`foundry status --output json`) → scan → actionable failure | Only steps 1 and 3. No CLI dialect detection at all. | **~35%** |
| **P0** Route coverage | `/v1/embeddings` + `/v1/audio/transcriptions`, audio on a separate concurrency limiter | Neither route exists. | **0%** |
| **P1** KV-Capacity Manager | Token-budget admission, live accounting, per-token KV size costing | Reserve/release works; no live accounting, no KV-geometry costing, no fairness | **~65%** |
| **P1** Calibration | Sweep keyed by (model, device, Foundry version, CLI generation) | Sweep exists, single unkeyed record, never run on hardware | **~45%** |
| **P1** Real metrics | Real series on the request path | Done, and good | **~85%** |
| **P2** Cache + coalescing | Exact **→ semantic**, coalescing on all paths | Exact only; coalescing on non-streaming only | **~50%** |
| **P3** Fleet router | Router tier, registry, best-fit by free budget, tenancy, auth/TLS | Nothing | **0%** |
| **P4** onnxruntime-genai | Out of gateway scope by design | Correctly absent | N/A |

**Highest-severity finding:** the online token self-calibration loop — the mechanism the README stakes the
whole token-budget accuracy claim on — is very likely **dead against a real backend**, because KVStream never
requests usage in streaming mode (G-14). Everything downstream of it (budget accuracy, `tokens` mode's
advantage over `concurrency` mode) is therefore unproven.

---

## 1. Completely missing features (no code exists)

### 1.1 — P0: Version-aware backend resolution (doc §8.3)

| # | Missing capability | Doc ref |
|---|---|---|
| **G-01** | **Foundry CLI dialect detection.** Read `foundry --version`, branch to the 0.10.x dialect or the service-based (0.8.x) dialect. | §8.3 |
| **G-02** | **CLI-assisted endpoint lookup.** `foundry status --output json` (0.10.x) / `foundry service status` (0.8.x), parsed *before* the port scan. Must be defensive — any parse failure is a miss that falls through to the scan. | §8.3, §8.6 |
| **G-03** | **Explicit config as authoritative.** Today `--backend-url` does not disable discovery (`cli.py:63`, `foundry.py:70`); a wrong-but-reachable localhost server still wins after a probe failure. Doc requires explicit config to be authoritative and the only supported path in containers. | §8.3 |
| **G-04** | **`foundry_cli` opt-out switch, off by default in containers.** No such config key exists (`config.py:20`). | §8.3 |
| **G-05** | **Actionable failure message naming the correct start command for the detected CLI generation** (`foundry server start` vs `foundry service start`). Today discovery failure silently returns the configured URL and the request dies as an opaque connection error. | §8.3 |
| **G-06** | **Pinned-URL honouring.** Recognise `Configuration.WebService.urls` being pinned by a host app and skip the scan entirely. No concept of a pin exists. | §5.4, §8.2 |

### 1.2 — P0: Route coverage gap (doc §8.4)

| # | Missing capability |
|---|---|
| **G-07** | **`POST /v1/embeddings` passthrough**, admitted on the KV-token budget with cost = input tokens. |
| **G-08** | **`POST /v1/audio/transcriptions` passthrough**, admitted under a **separate, plain concurrency limiter** — the doc is explicit that audio has no honest token cost and must not be faked into the token budget. Needs a second limiter, its own config block, its own metrics labels. |
| **G-09** | **Multipart/form-data forwarding path.** `foundry.py` is JSON+SSE only; transcription requires streaming a file body upstream. |
| **G-10** | Model-metadata source from `foundry model list --output json` — the doc names `--output json` as the "new, preferred discovery and model-metadata source". |

### 1.3 — P1: KV-capacity economics

| # | Missing capability |
|---|---|
| **G-11** | **Per-token KV size from the public model config, used for relative request costing** (§5.2, final sentence). Cost is currently raw `prompt + max_tokens` (`capacity.py:38`), identical for a 2-layer and a 40-layer model. Without this, `B` is meaningless across models and the calibration store cannot be shared. |
| **G-12** | **Live accounting during streaming** (§6.2 step 5: "stream tokens back; **update accounting live**"). The full `prompt + max_tokens` reservation is held until stream termination (`app.py:154`, `app.py:235`). A request asking for `max_tokens=2048` that stops at 40 tokens holds ~2000 tokens of phantom budget for its whole lifetime — the largest single source of under-utilisation in `tokens` mode, and it directly undercuts the doc's core claim that KVStream "maximises use" of the device. |
| **G-13** | **Calibration key including device, Foundry Local version and CLI generation.** The doc states this twice (§5.2 "keyed by (model, device)"; §8.6 "must include the Foundry Local version and CLI generation"). `calibration.py:126` writes a **single flat record**; `load_calibrated_budget` matches on `model` only and ignores the stored `base_url`. Calibrating a second model silently destroys the first, and a budget measured on a laptop iGPU loads on an NPU box without complaint. |

### 1.4 — P2: Cache and coalescing

| # | Missing capability |
|---|---|
| **G-15** | **Semantic cache** (§5.3, "exact **+ semantic**"). Only exact-hash caching exists. Needs an embedding source, similarity threshold, and correctness gate — the only part of the doc requiring a new dependency. |
| **G-16** | **Coalescing on the streaming path.** `handle_streaming` (`app.py:185`) never consults `self.coalescer`; the key is used only for a cache write. Agent swarms overwhelmingly stream, so "singleflight coalescing of identical in-flight calls" is inactive for the dominant traffic shape. |
| **G-17** | **Per-request cache control.** No `x-kvstream-cache: no-store` / bypass header, no purge endpoint, no persistence across restarts. |
| **G-18** | **Cacheability gate beyond `temperature == 0`.** `models.py:27` gates on temperature alone; a request carrying `tools` or `seed` would be treated as equally cacheable. |

### 1.5 — P3: Fleet tier (doc §5.5, §6, §6.1, Figure 1)

Entirely absent — correctly so under the CHANGELOG's "no unwired features" policy, but it is half of the
document's architecture diagram and must be built to match the doc.

| # | Missing capability |
|---|---|
| **G-19** | **Router process/tier** — a second deployable with its own entrypoint (`kvstream router`). |
| **G-20** | **Instance registry + heartbeat.** Sidecars register and report free budget; router prunes dead instances. `/status` already exposes the right numbers, but nothing publishes them. |
| **G-21** | **Cross-machine best-fit routing by free budget** (§5.5). |
| **G-22** | **Router-tier global cache and coalescer** (§6.1 places these at the router for fleets, in addition to the sidecar). |
| **G-23** | **Tenancy + fair-queuing** (§6.1; §6.2 step 1 "Authenticate, **resolve tenant**"). No tenant concept exists anywhere. |
| **G-24** | **Auth / TLS / single client endpoint** (§6.1). |

### 1.6 — Documentation and positioning deliverables

| # | Missing deliverable |
|---|---|
| **G-25** | **Appendix B (calibration methodology).** Cited three times, including §8.6's "The calibration methodology in Appendix B is reusable either way" — the artefact the doc offers the Foundry team. The PDF has no appendices and the repo has no such document. |
| **G-26** | **Section 8 story in the repo docs.** The README never mentions CLI generations, `foundry server` vs `foundry service`, or §8.5's in-process-SDK boundary. A reader of the code cannot tell KVStream is version-aware by construction. |
| **G-27** | **Container story.** §8.3 reasons explicitly about containerised deployments, but there is no Dockerfile, no compose file, no CI. |

---

## 2. Wired but incomplete (on the execution path, materially short of the doc)

| # | Component | What is short |
|---|---|---|
| **G-14** | **Usage-driven self-calibration is likely dead in production.** `foundry.py:100` forces `stream: True` upstream for *every* request but never sends `stream_options: {"include_usage": true}`. Most OpenAI-compatible servers emit the trailing usage chunk **only** when that flag is set, so `Token.usage` stays `None`, `TokenEstimator.observe()` never fires, `samples` stays 0, and `chars_per_token` never leaves 4.0. Every test in `tests/integration/test_usage_calibration.py` passes because the stub always emits usage. **Fix:** send `stream_options.include_usage` on streamed upstream calls, and stop force-streaming — forward non-stream client requests as non-stream so the backend `usage` block is read directly. |
| **G-28** | **Discovery cooldown missing.** Doc §4.1 describes discovery as "lock-serialised, **cooldown-throttled**". `foundry.py:70` is lock-serialised but has no cooldown: every request after a backend death triggers a fresh `netstat`/`ss` subprocess plus an HTTP probe of every listening port. A dead Foundry Local turns the gateway into a process-spawn storm. |
| **G-29** | **Discovery can latch onto a non-Foundry backend.** `discovery.py:70` accepts any localhost port answering `/v1/models` with ≥1 model — Ollama, LM Studio, vLLM, or a second KVStream all qualify. Only the gateway's own port is excluded (`app.py:51`). Needs Foundry fingerprinting and a preference for the port serving the *configured* model, not merely any model. |
| **G-30** | **Admission fairness / ordering.** `capacity.py:104` uses `Condition.notify_all()` with a re-check loop: no FIFO, no ageing, no priority. Large requests can be starved indefinitely by a stream of small ones — exactly the mixed-size multi-agent workload the token budget exists to serve. Also O(N) wakeups per release. §6.2 step 6's "admit the next queued request(s) that now fit" implies an ordered, best-fit drain. |
| **G-31** | **Non-streaming error mapping.** `handle_streaming` maps backend failures to 502 (`app.py:208`); `_generate_full` has no equivalent, so a `FoundryError` on the non-streaming path escapes as a bare FastAPI **500**. |
| **G-32** | **Non-OpenAI error envelope.** All errors return FastAPI's `{"detail": ...}`; OpenAI SDK clients expect `{"error": {...}}`. 503s carry no `Retry-After`. For a gateway whose value is "clean backpressure", the backpressure is not machine-readable by its target clients. |
| **G-33** | **`/health` always returns HTTP 200**, even when `backend_healthy` is false (`app.py:335`). Orchestrators key on status code. |
| **G-34** | **Calibration sweep is coarse and never validated.** `calibration.py:91` doubles concurrency (1→2→4→8…), so the knee resolves only to the previous power of two — up to 50% of capacity discarded. `p99` at concurrency=1 is a single sample used as the baseline for the whole ratio test. No warm-up, no repeated trials, no mixed request sizes, no `--device` flag, no re-calibration trigger or staleness check. CHANGELOG concedes it has never run against live hardware. |
| **G-35** | **Cached streaming replay emits the whole response as one SSE chunk** (`app.py:299`), with no role delta. Functionally correct, but a cache hit is trivially distinguishable from a real stream. |
| **G-36** | **Single-process assumption unenforced.** All admission state is in-process (`capacity.py:59`). `uvicorn --workers 2` silently doubles the budget and voids calibration. Nothing in config, CLI, or docs prevents it. |
| **G-37** | **`/status` is thin.** No backend URL, no discovery source, no calibration provenance (measured_at, staleness, model/device measured on), no CLI generation, no version — precisely what an operator needs when the budget looks wrong. |
| **G-38** | **Metrics blind spots.** No TTFT, no admission-wait histogram, no backend-latency histogram, no backend errors by status code, no discovery/re-resolution counter, no calibration-age gauge, no per-route labels (needed once G-07/G-08 land). |
| **G-39** | **No graceful drain.** No SIGTERM path that stops admitting and lets in-flight reservations finish. |

---

## 3. OpenAI-compatibility gaps (not called out in the doc, but fatal to its use case)

The document's premise is multi-agent orchestration, RAG fan-out, and planner loops. Those workloads run on
**tool/function calling**. `models.py:13` cannot represent them.

| # | Gap |
|---|---|
| **G-40** | **No `tools` / `tool_choice` / `functions`**, and `ChatMessage` has no `tool_calls`, `tool_call_id`, or `name`. Any agent framework using tools gets a 422. This alone blocks LangGraph, AutoGen, Semantic Kernel, and OpenAI-SDK agents. |
| **G-41** | **`ChatMessage.content` is `str` only** — no content-part arrays, no multimodal, no `null` content (what an assistant tool-call message carries). |
| **G-42** | **Silently dropped request fields:** `seed`, `frequency_penalty`, `presence_penalty`, `response_format`, `logit_bias`, `logprobs`, `user`, `stream_options`, `stop`-as-string. `as_backend_payload` (`models.py:29`) is a six-field allow-list; everything else is discarded without warning — a client asking for JSON mode gets prose. |
| **G-43** | **Responses are synthesised, not proxied.** `_chat_json` (`app.py:278`) rebuilds the response, so `tool_calls`, `logprobs`, `system_fingerprint`, the real `id`/`created`/`model` never reach the client. `_sse_chunk` (`app.py:258`) emits only `delta.content` — the initial `{"role":"assistant"}` delta is missing and tool-call deltas are unrepresentable. |
| **G-44** | **`n > 1` rejected**; `max_tokens` defaults to 512 (OpenAI: model max) and caps at 32768; `temperature` defaults to 0.8 (OpenAI: 1.0). Divergences a drop-in gateway should not have. |
| **G-45** | **No API-key passthrough** to the backend and no `Authorization` header handling. |
| **G-46** | **No `/v1/completions`** and no `GET /v1/models/{id}`. |

---

## 4. Product / positioning issues (PM lens)

| # | Issue |
|---|---|
| **G-47** | **`pyproject.toml` declares `Homepage`/`Repository`/`Issues` under `github.com/microsoft/KVstream`**, and the README clones from it. The proposal is an outside request for feedback; metadata implying Microsoft ownership is a credibility and trademark risk. Fix before any publication. |
| **G-48** | **Version story is confusing.** `version.py` says `1.0.0`, the CHANGELOG says 1.0.0 is unpublished, the proposal is a "CLI 0.10.0 update". Recommend `0.2.x` until P0 lands, so `1.0.0` can mean "matches the proposal". |
| **G-49** | **README's estimator benchmark table** ("~21% / ~43% / ~26%", "33% of requests") is not reproducible from anything in the repo — no harness, no dataset, no reference tokenizer. For a document whose selling point is truthfulness, an unreproducible measured claim is the most damaging kind. Ship the harness or mark the numbers indicative. |
| **G-50** | **No test coverage for discovery, `FoundryClient`, or the CLI.** 48 tests, none touching `discovery.py`, `foundry.py`'s resolution logic, or any `typer` command — exactly the code §8.3 makes the most version-sensitive part of the product. |
| **G-51** | **No demonstration of the core claim.** The thesis is "users can submit far more calls without Foundry Local stalling". `kvstream bench` exists, but there is no before/after harness (direct-to-Foundry vs through-KVStream) producing the chart that proves it — the most persuasive artefact the proposal could carry. |

---

## 5. Recommended build order

**Milestone A — make the existing claims true.** G-14 (usage / `stream_options`, unblocks everything in
`tokens` mode), G-12 (live accounting), G-31/G-32/G-33 (error and health semantics), G-28 (discovery
cooldown), G-13 (calibration keying), G-36 (single-worker enforcement).

**Milestone B — P0 from the doc.** G-01…G-06 (version-aware resolution chain behind a `backend.foundry_cli`
toggle, defensive JSON parsing), G-07…G-10 (embeddings + transcriptions with a separate audio limiter), G-29
(Foundry fingerprinting), per-route metrics from G-38.

**Milestone C — agent-workload compatibility.** G-40…G-46. Replace the allow-list request model with
pass-through plus a typed overlay, and proxy responses instead of synthesising them. Without this, no real
agent framework can use the gateway and the doc's use case stays untested.

**Milestone D — finish P1/P2.** G-11 (KV-geometry costing), G-30 (fair best-fit drain), G-34 (calibration
rigour + a live-hardware run), G-16 (streaming coalescing), G-17/G-18, G-37, G-39.

**Milestone E — proposal artefacts.** G-25 (Appendix B), G-49 (reproducible estimator benchmark), G-51
(before/after stall demonstration), G-26/G-27 (docs + container), G-47/G-48, G-50 (tests).

**Milestone F — P3 fleet** (only if fleets are in scope). G-19…G-24.

**Out of scope by design:** P4 (`onnxruntime-genai` paged KV) and §8.5 (in-process SDK with no web service).
The code's honesty about these is one of its genuine strengths.

---

## 6. Milestone A — delivered 2026-08-23

| # | Resolution |
|---|---|
| **G-14** | Streamed upstream calls now carry `stream_options.include_usage`; a backend that rejects it gets one retry without it, a log line saying real counts are unavailable, and no further attempts (`foundry.py`, `_use_stream_options`). Non-streamed client requests are forwarded non-streamed via the new `FoundryClient.chat_once`, so the backend's own `usage` block is read directly instead of depending on a trailing SSE chunk. Covered by `tests/unit/test_foundry_client.py` against a mock transport — the integration stubs replace the client, so they structurally could not have caught this. |
| **G-12** | `CapacityManager.adjust()` settles a live reservation against the response's true length, reclaiming headroom before teardown; the gateway calls it every 16 tokens, on `finish_reason`, and once more on the settled completion count. New `admission.reserve_completion_ratio` reserves only part of `max_tokens` up front, with top-up-on-overshoot counted in `kvstream_reservation_overshoot_total`. Default `1.0` keeps the proposal's worst-case reservation and cannot breach the budget. |
| **G-13** | Store is now v2: one record per `(model, device, cli_generation, backend_version)`, with strict lookup (exact → silent, partial → warn, otherwise unused and fall back to `concurrency`). v1 flat records are migrated, not discarded. `backend.device` / `kvstream calibrate --device` set the label; `cli_generation` and `backend_version` are placeholders wired for Milestone B. |
| **G-31** | The non-streaming path maps `FoundryError` to 502, as the streaming path already did. A malformed backend body is 502 too, not a 500. |
| **G-32** | All errors return the OpenAI envelope `{"error": {message, type, code, param}}`; 503 carries `Retry-After`. |
| **G-33** | `/health` returns 503 when the backend is unreachable, with a hint naming the right start command for each CLI generation. |
| **G-28** | `backend.discovery_cooldown_seconds` (default 5s) bounds full localhost sweeps. |
| **G-36** | `build_app` refuses to start when `WEB_CONCURRENCY`/`UVICORN_WORKERS`/`GUNICORN_WORKERS`/`KVSTREAM_WORKERS` exceeds 1. |

**Partial credit against the other gaps.** `/status` now reports budget provenance, the calibration key and
backend/discovery state, which is most of **G-37**; and `kvstream_backend_errors_total{phase}`,
`kvstream_active_requests`, `kvstream_discovery_scans` and `kvstream_backend_up` chip at **G-38**. Neither is
closed — G-37 still lacks calibration staleness, and G-38 still lacks TTFT, admission-wait and backend-latency
histograms and per-route labels.

**Found while verifying, fixed in passing.** `kvstream serve` died with a `UnicodeEncodeError` on Windows
whenever stdout was redirected (`kvstream serve > log.txt`), because the banner's `→` cannot be encoded in
cp1252. The CLI now widens stdout to UTF-8 with replacement. This was pre-existing and unrelated to the
proposal, but it broke every "run it as a service and capture the log" workflow on the target platform.

### Two environment issues for the author

1. **The editable install points at a different checkout.** `pip show kvstream` resolves to
   `F:\KVStream\KVstream\kvstream-foundry\src\kvstream`, not this tree, so a bare `kvstream ...` or
   `python -m kvstream` runs the *older* copy. `pytest` is unaffected (`pythonpath = ["src"]` in
   `pyproject.toml`). Re-run `pip install -e .` from this directory before trusting any manual CLI run.
2. **`LICENSE` reads "Copyright (c) Microsoft Corporation."** — which, together with **G-47**'s
   `microsoft/KVstream` URLs, means the package currently presents itself as Microsoft-owned work. The
   CHANGELOG's claim that the license was "made consistently MIT" is about the license *type*, not the
   copyright holder. For a document that is an outside request for feedback, this is the highest-risk piece of
   metadata in the repo. Left unchanged deliberately: whose name goes on a copyright line is the author's call,
   not a refactor.

---

## 7. Milestone B — delivered 2026-08-23

P0 from proposal §8: version compatibility across both CLI generations, and the route coverage gap.

| # | Resolution |
|---|---|
| **G-01** | `foundry --version` is parsed and classified: `>= 0.10.0` → the `foundry server` / `foundry status` dialect, below → the service-based dialect. An unparseable version is `unknown` and is offered both dialects rather than being guessed at (`backend/foundry_cli.py`). |
| **G-02** | `foundry status --output json` (0.10.x) and `foundry service status` (service-based), tried in the order the detected generation suggests. The 0.10.x schema is unverified, so `extract_endpoint` walks the whole document for URL-shaped strings and prefers ones under an endpoint-ish key, rather than assuming a field name; a banner around the JSON is tolerated. Every failure mode — missing binary, non-zero exit, timeout, malformed JSON, no URL — is a miss that falls through to the scan. |
| **G-03** | Explicit configuration is now authoritative. `Settings.backend_url_is_explicit()` uses pydantic's `model_fields_set`, which distinguishes a value the operator supplied (config file, `KVSTREAM_BACKEND__BASE_URL`, or `--backend-url` assigning onto the model) from the default guess. A pinned URL skips discovery entirely — verified live: `scans: 0` with `--backend-url` set. |
| **G-04** | `backend.use_foundry_cli`: `auto` (default) enables the CLI on a host and disables it in a container, `always`/`never` override. Container detection covers `KUBERNETES_SERVICE_HOST`, `/.dockerenv`, `/run/.containerenv` and `/proc/1/cgroup`. |
| **G-05** | `BackendResolver.failure_hint()` names the start command for the generation actually detected, and flows into 502 messages, the `/health` hint, the startup warning and `kvstream calibrate`. Verified live: with `foundry 0.8.119` present the message says `foundry service start`, not `foundry server start`. |
| **G-06** | A pinned URL removes the scan entirely, which is the mechanism a host application's `Configuration.WebService.urls` pin needs — KVStream is told about it through configuration rather than reading SDK state it has no access to. |
| **G-07** | `POST /v1/embeddings`, admitted on the KV-token budget with cost = input tokens, response proxied verbatim. Embedding `usage` also feeds the token estimator. |
| **G-08** | `POST /v1/audio/transcriptions` under a separate plain concurrency limiter (`routes.audio_max_concurrency`, default 2), with `routes.audio_max_upload_mb` → 413. A test asserts the audio path never touches the KV-token budget, and that peak concurrency is actually capped. |
| **G-09** | Multipart forwarding: the form is read, the file part re-posted upstream, and status, body and content type all pass through — so `response_format=srt`/`vtt`/`text` work as-is. |
| **G-10** | `foundry model list --output json` is compared against the configured model at startup; a mismatch warns with the catalog and a `foundry model load` command. |
| **G-29** | Discovery ranks candidates instead of taking the first hit. Another KVStream is excluded outright via a new `X-KVStream-Version` response header — the one identification signal we control, and the one that prevents a proxy loop. A port serving the configured model wins; an identifiable non-Foundry server (Ollama, by its documented `/api/tags`) is demoted; a port with models beats one without. Only negative fingerprints are used, and only documented ones — there is no published positive fingerprint for Foundry Local, and this does not pretend otherwise. |

**Also closed.** The calibration key's `cli_generation` and `backend_version` fields, added in Milestone A as
placeholders, are now populated by a startup CLI probe, and the admission budget is re-resolved once they are
known — before any request is served. Per-route metric labels close the remaining half of **G-38**'s
per-route item (TTFT, admission-wait and backend-latency histograms are still open).

**Dependency added.** `python-multipart` is now declared in `pyproject.toml`; FastAPI needs it to parse the
transcription body, and it was previously an undeclared ambient dependency.

### Verified live

This machine has `foundry 0.8.119` — the exact build the proposal cites. Booting the gateway against it:
CLI detection returned `generation: "service"`, `version: "0.8.119"`; the calibration key picked both up;
`--backend-url` pinned the endpoint with `scans: 0`; and the unreachable-backend error named
`foundry service start`. The Foundry service itself was not running, so the *successful* CLI-assisted
resolution path (step 2 returning a live endpoint) is covered by tests but has not yet been exercised against
a running service. Starting it (`foundry service start`) is the one remaining live check.

---

## 8. Milestone C — delivered 2026-08-23

Agent-workload compatibility. This is the milestone that turns the proposal's premise — multi-agent
orchestration, RAG fan-out, planner loops — from something the gateway *describes* into something it can
actually carry.

| # | Resolution |
|---|---|
| **G-40** | `tools` and `tool_choice` are accepted and forwarded; `ChatMessage` now models `tool_calls`, `tool_call_id` and `name`. The canonical agent second turn — an assistant message with `content: null` plus `tool_calls`, followed by a `tool` result — used to be a 422 and now round-trips. Verified live against a running gateway. |
| **G-41** | `content` accepts a string, a content-part array, or `null`. Non-text parts are forwarded but not billed: an image has no token cost measurable at the HTTP layer, and inventing one would be the same mistake the proposal refuses to make for audio. |
| **G-42** | The allow-list is gone. `models.py` validates the overlay KVStream needs and `backend_payload()` forwards the client's own object, so `seed`, `response_format`, `logprobs`, `logit_bias`, `frequency_penalty`, `presence_penalty`, `user` and anything a future API version adds all survive. A test asserts every field of a realistic request arrives intact. |
| **G-43** | Responses are proxied, not rebuilt. `Token.raw` carries each SSE chunk exactly as received and the non-streamed body is forwarded whole, preserving `tool_calls`, the opening `{"role":"assistant"}` delta, streamed tool-call argument deltas, `logprobs`, `system_fingerprint` and the real completion `id`. |
| **G-44** | An omitted `max_tokens` or `temperature` is no longer replaced with 512 / 0.8 and sent upstream — the model's own default applies. The arbitrary 32k ceiling is gone. `n > 1` is allowed and costed as `n × max_tokens`. New `admission.default_max_tokens` costs a request that omits the field and is never sent upstream. |
| **G-45** | `backend.forward_authorization` (default on) passes the caller's `Authorization` header through, so KVStream can sit behind a gateway doing per-caller auth; `backend.api_key` sets a static token where one is needed. A forwarded header wins over the static key. |
| **G-46** | `POST /v1/completions` (admitted on the KV-token budget, `routes.completions`) and `GET /v1/models/{id}` (proxied verbatim). |

### Two decisions worth recording

**Usage is never fabricated, and never leaked.** KVStream asks the backend for token counts because its own
accounting needs them. A client that did not send `stream_options.include_usage` therefore must not suddenly
receive a usage chunk it never requested — so the extra chunk is consumed by the gateway and dropped. In the
other direction, when the backend reports no counts at all, the `usage` block is filled in from KVStream's
estimate (clients do read that field) and the response carries `X-KVStream-Usage: estimated`. The estimate is
disclosed in a header rather than by adding a non-standard key to a standard object — and never passed off as
measured.

**The cache had to change shape.** It previously stored a text string, which was fine for prose and silently
catastrophic for tool calls: a cached agent turn would have replayed as an empty assistant message rather than
the function call the agent was waiting on. It now stores the backend's real body, or the exact recorded
chunks, and the key hashes the client's whole request object — so a `tools` payload cannot collide with an
otherwise identical request without one. The key also includes `stream`, because converting recorded chunks
into a response body means merging tool-call deltas by index across partial JSON fragments, which is lossy.
One duplicate entry is cheaper than a lossy conversion.

### Still open

`/v1/completions` is proxied but **unverified against Foundry Local** — if the runtime does not implement it,
the backend's own 404 passes through, which is the correct behaviour but is not the same as knowing. The
remaining Milestone D items (G-11 KV-geometry costing, G-30 fair admission drain, G-34 calibration rigour,
G-16 streaming coalescing, G-17/G-18 cache controls, G-37, G-39) are untouched, as are E and F.

---

## 9. Milestone D — delivered 2026-08-24

Finishing P1 and P2. Most of this is correctness that only shows itself under the workload the proposal
actually targets: mixed request sizes, and streaming agent traffic.

| # | Resolution |
|---|---|
| **G-30** | Admission is now an explicit FIFO queue drained from the front for as long as the next waiter fits. The old `notify_all` + re-check had *no* ordering — whichever coroutine the loop scheduled first and happened to fit won — so a large request could be starved indefinitely by a stream of small ones, and each release woke O(N) coroutines to admit one. Fast-path admissions bypass the queue entirely so they cannot consume its depth. A timed-out or cancelled waiter leaves cleanly and unblocks the rest; one granted in the race with its own timeout keeps the reservation rather than discarding paid-for work. |
| **G-11** | `kvstream.admission.geometry` derives `kv_bytes_per_token` from published architecture and costs each request in proportion to the calibrated model. Grouped-query attention is handled (KV heads, not attention heads); `head_dim` is derived from `hidden_size` when absent. Sources are the declared `models:` config and, best-effort, `foundry model show --output json`. An undeclared model weighs exactly 1.0 — the same cost it had before geometry existed. |
| **G-16** | `StreamBroadcast` / `StreamCoalescer`: identical concurrent streams ride one upstream call. Followers replay what they missed then track the leader, publishing never awaits (a slow follower cannot stall the producer), and followers consume no admission budget. Leadership is claimed *before* queueing and pre-flighting — both await, and the duplicates arriving in that window are precisely the ones coalescing exists to catch. |
| **G-17** | Per-request `Cache-Control: no-store` / `no-cache` and `x-kvstream-cache`, gated by `cache.respect_request_headers`. Plus `cache.max_entry_bytes`, so one huge response cannot evict everything useful. |
| **G-18** | Largely closed in Milestone C by hashing the client's whole request and requiring an explicit `temperature: 0` with `n == 1`; the per-request opt-out above completes it. |
| **G-34** | The sweep warms up before measuring, repeats each point and pools latencies before the percentile, cycles a spread of request shapes, and bisects between the last healthy point and the first unhealthy one. Doubling alone resolved the knee only to the previous power of two, discarding up to half the machine's capacity. The sweep points are stored with the budget as evidence. |
| **G-37** | `/status` now carries queue statistics, model geometry, streaming-coalescer state, and calibration provenance including `measured_at` and `age_seconds`. |
| **G-38** | `kvstream_admission_wait_seconds`, `kvstream_calibration_age_seconds`, `kvstream_cache_skipped_total` join the per-route labels added in Milestone B. |
| **G-39** | `drain_timeout_seconds`: stop admitting, turn away the queue, let in-flight requests finish. A request arriving during shutdown gets a 503 that says shutdown, not overload. |

### A trade-off worth stating

Strict FIFO buys freedom from starvation and pays for it in head-of-line blocking: a large request at the
front holds back smaller ones behind it until it fits. Conservative backfill (letting a later, smaller request
run only in capacity the head cannot use) would recover some of that, but it needs runtime estimates of when
capacity frees, which the gateway does not have. Bounded, fair waiting was judged better than unbounded
unfairness — and the wait is now measured (`kvstream_admission_wait_seconds`, `admission.queue`) rather than
assumed, so the trade can be revisited against data instead of intuition.

### What is left

- **G-15 semantic cache** — the only remaining P2 item, and the only part of the proposal that requires a new
  dependency (an embedding source plus a similarity threshold and correctness gate).
- **G-19…G-24, the fleet tier (P3)** — untouched, and correctly absent until fleets are in scope.
- **Milestone E** — Appendix B, the reproducible estimator benchmark, the before/after stall demonstration,
  the container story, and the `microsoft/KVstream` metadata question (G-47).
- **A live calibration run.** The sweep is now unit-tested against a synthetic backend with a known
  concurrency ceiling, which proves the *algorithm* — warm-up, pooling, bisection, mixed shapes — but not the
  number it produces on real hardware. `foundry service start` on this machine is still the one command that
  would close the oldest open item in the CHANGELOG.

---

## 10. Milestone E — delivered 2026-08-24

The proposal's artefacts. Building them was worth more than the artefacts themselves: one of them falsified a
claim the README had been making, and another exposed a bug introduced in Milestone D.

| # | Resolution |
|---|---|
| **G-25** | [`docs/APPENDIX-B-CALIBRATION.md`](APPENDIX-B-CALIBRATION.md) — the methodology the proposal cites three times and offers to the Foundry Local team as reusable. Covers what is measured, why each step exists, the two regimes of the ceiling and the fact that the sweep does not currently distinguish them, what the number does not mean, and §B.7: the two changes on the Foundry side that would make most of the procedure unnecessary. |
| **G-49** | `benchmarks/estimator_benchmark.py` — reproducible, seeded, train/held-out split, scored against `cl100k_base`. The README's numbers are now generated by it. |
| **G-51** | `benchmarks/admission_benchmark.py` + `benchmarks/simulated_foundry.py` — the before/after demonstration, with KVStream calibrating the ceiling itself rather than being told it. |
| **G-27** | `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `.github/workflows/ci.yml`. The image disables discovery and the CLI lookup because neither can work across a network namespace, and requires an explicit backend URL — which is what §8.3 says is the supported container path. CI runs Windows as well as Linux; two of this project's fixed bugs were Windows-only. |
| **G-50** | `tests/unit/test_cli.py` — every command, including `calibrate`, which was the highest-value untested code in the repo. |
| **G-26** | README gains the measured evidence, a documentation index and the container section. |

### The README's safety claim was false

The old table claimed "**0% under-counts**" for both KVStream estimators and stated that the gateway "never
breaches the budget". Measured over 400 held-out samples:

| Estimator | Mean abs. error | Bias | Under-counts |
|---|---|---|---|
| naive `chars/4` | 16.5% | +3.2% | 56.8% |
| KVStream, uncalibrated | 19.1% | +13.8% | 36.3% |
| KVStream, calibrated | 6.8% | +5.2% | **16.3%** |

Two corrections, in opposite directions. The estimator is **considerably better than claimed** — 6.8% mean
absolute error, not the ~26% the README advertised. And it **under-counts**, which the README said it never
does; worst case 16% on this corpus, concentrated in code (15.2% mean error). A hard zero-under-count
guarantee needs `admission.token_safety_factor` ≥ 1.188, so 1.25 in practice, at roughly a quarter of admitted
throughput.

The default was left at 1.0: the calibrated budget already carries its own 0.85 safety margin, so stacking
both would compound conservatism. That is now a documented, measured decision rather than an unexamined one.

### A Milestone D bug the unit tests could not see

Making the calibration probe use mixed request shapes (G-34) silently broke knee detection. Raw latency is not
comparable across concurrency levels once the shape mix changes with the level: at concurrency 1 the batch
holds only the smallest shape, at concurrency 2 it also holds a larger one, and the p99 jump from *shape* is
indistinguishable from a jump from *load*. Calibration against a runtime with a known optimum of 4 concurrent
produced a budget that admitted **one** request.

The fix is to normalise the health signal to service time per requested token. The same run now produces a
budget that admits 5. The unit tests missed it because they used a uniform probe shape — the tests were
correct about the algorithm and blind to the methodology.

### Decisions the author made

**G-47 (attribution) and G-48 (version) were reviewed and deliberately left unchanged.** `LICENSE` continues
to read "Copyright (c) Microsoft Corporation" and `pyproject.toml` continues to point at
`github.com/microsoft/KVstream`; the version stays at 1.0.0. This is recorded so a later reader knows it was a
decision, not an oversight. The original concern stands on the merits — for a document submitted *to*
Microsoft, that metadata reads as claiming their ownership — and the change is a two-line edit whenever it is
wanted.

### What is left

- **G-15 semantic cache** — the last P2 item, and the only part of the proposal needing a new dependency.
- **G-19…G-24, the fleet tier (P3)** — untouched, and correctly absent until fleets are in scope.
- **A live calibration run.** Everything about the method is now tested and demonstrated; what remains is the
  number itself. `foundry service start` on this machine, then
  `python benchmarks/admission_benchmark.py --backend-url http://localhost:5273`, would turn the strongest
  claim in the proposal from *modelled* into *measured*.

---

## 11. Live hardware run — attempted 2026-08-24

`foundry service start` was run on this machine with authorisation to use existing models only.

### What was verified against a real Foundry Local

The service came up on **`http://127.0.0.1:64164`** — a genuine OS-assigned ephemeral port, which is the
problem §5.4 exists to solve. Against it:

| Claim | Result |
|---|---|
| CLI detection (G-01) | `foundry 0.8.119`, classified `service` generation — correct |
| Calibration key carries the environment (G-13) | `phi-3-mini \| windows-amd64 \| service \| 0.8.119` |
| **CLI-assisted resolution (G-02)** | **Failed, then fixed, then verified** — see below |
| Localhost scan fallback (§8.3 step 3) | Found the ephemeral port unaided |
| `/health` through the gateway | 200 against the live service |
| Backend error mapping (G-31/G-32) | Foundry's HTTP 400 surfaced as a 502 in the OpenAI error envelope |

### The bug only a live run could find

`foundry service status` prints:

```
🟢 Model management service is running on http://127.0.0.1:64164/openai/status
```

That is a **status page, not an API root**. The extractor took it literally, so the probe went to
`http://127.0.0.1:64164/openai/status/v1/models`, 404'd, and resolution fell through to the port scan — every
time, for the exact CLI generation the proposal says it verified against. Nothing appeared broken, because the
scan fallback is designed to rescue precisely this case. The defensive design hid the defect.

Fixed: a CLI-reported endpoint is now tried both as reported and as its bare origin, and whichever answers is
used. Re-run against the live service, resolution reports `source: foundry-cli` with `attempts:
["foundry-cli(service)"]` — no scan at all. A regression test built from the exact bytes the real CLI emitted
is in `tests/unit/test_foundry_cli.py`.

### The model, and what it revealed

`phi-3-mini-4k` was downloaded (GPU variant, 2.13 GB, within the 3 GB limit set for the run) and loaded. Two
regimes appeared, and they say different things.

**Regime 1 — light load: the runtime serialises.** Identical small requests at rising concurrency, warmed up:

| Concurrency | Wall | Mean latency | Max latency | Throughput |
|---|---|---|---|---|
| 1 | 0.38s | 0.38s | 0.38s | 2.61 r/s |
| 2 | 0.77s | 0.77s | 0.77s | 2.60 r/s |
| 4 | 1.55s | 1.55s | 1.55s | 2.58 r/s |
| 8 | 3.12s | 3.11s | 3.12s | 2.57 r/s |

Throughput flat, latency exactly linear, mean equal to max. `foundry 0.8.119` does not batch — one engine
behind a FIFO queue, optimal concurrency 1. In this regime there is no knee, and the calibrated `B = 96
tokens` ("one small request at a time") is **correct**, not degenerate. Calibration found the true ceiling on
its first real run without being told it.

**Regime 2 — sustained load: it stalls, and does not come back.** 80 mixed-size requests with prompts to 1200
tokens at 16 concurrent, across two benchmark runs:

| Run | direct | through KVStream |
|---|---|---|
| 1 | 40/40 completed, p50 38.7s, 0.44 r/s | 40/40 completed, p50 19.1s, 0.81 r/s |
| 2 | 9/40 completed, 31 errors, p50 102.9s | 0/40 completed, 40 errors |

After run 2 the runtime was dead: a **4-token** request did not return in 180 s, with
`Inference.Service.Agent` holding **13.4 GB** resident for a 2.13 GB model and 4,245 s of accumulated CPU. It
did not recover, and `foundry service restart` hung too.

This is §2's stall, observed directly — and it is the **KV-memory-exhaustion** regime of §B.2, not a software
concurrency cap. The proposal's premise is validated; what is wrong is only the assumed *trigger*, which is
cumulative session pressure rather than instantaneous concurrency.

### Three findings that cost the proposal something

1. **Admission control did not prevent the stall.** KVStream held the runtime to roughly one request at a
   time and it exhausted itself anyway, because the pressure accumulated over a long session. A budget derived
   from a sixty-second calibration sweep describes instantaneous concurrency; it says nothing about what an
   hour of traffic does to the runtime's memory. Nothing in the current design watches for drift, and §B.2's
   "the sweep does not label which regime it found" is exactly the missing piece.
2. **`/health` reported green while inference was dead.** It probes `/v1/models`, which answered 200 in 4 ms
   throughout the stall. That is a liveness probe being read as readiness — a real gap in this gateway, now
   tracked as **G-52** below.
3. **The before/after comparison is not reportable.** Run 1 showed KVStream roughly doubling goodput; run 2
   showed both arms collapsing. Two runs that disagree that violently are not a result, and quoting the
   favourable one would be exactly the kind of unreproducible claim this whole exercise exists to eliminate.

**Caveat on generality.** This box reports Windows build 19045, below the 26100 that Foundry Local needs to
register the Windows ML execution providers, so the "generic-gpu" model ran on a CPU fallback path. One
machine, one model, one version.

**Foundry Local 0.8.119 returns no `usage` block.** Responses carry `choices`, `model` and `id` and nothing
else — exactly the case G-14 was written for. Online token calibration receives no samples against this
backend, so the *uncalibrated* estimator figures (19.1% mean error, 36% under-counts) are the ones that apply,
not the calibrated ones the README leads with. The gateway degrades correctly and reports
`backend.usage_reporting`.

---

## 12. New gaps found on hardware

| # | Gap |
|---|---|
| **G-52** ✅ | **FIXED.** **`/health` was a liveness probe presented as readiness.** It probes `/v1/models`, which kept answering 200 in 4 ms while the runtime could not complete a 4-token generation. An orchestrator keyed on it would have kept routing traffic into a dead backend for as long as it stayed dead. A readiness check needs a bounded trial *generation*, cached and rate-limited so it does not itself become load — and `/status` should distinguish "reachable" from "serving". |
| **G-53** ✅ | **FIXED.** **Nothing detected backend drift.** The calibrated budget is measured once and then trusted indefinitely. This run showed a runtime whose real capacity fell to zero over a session while the gateway's budget stayed at its calibration-time value. Watching served-latency-per-token against the calibration baseline would catch it; §B.2's regime labelling is the same missing information. |
| **G-54** ✅ | **FIXED.** **A stalled backend consumed the whole admission queue.** With the backend hung, in-flight requests occupy their reservations until the 120 s backend timeout, so the queue fills with work that will never complete and every arrival waits the full admission timeout before its 503. A circuit breaker — trip after N consecutive backend timeouts, fail fast, probe for recovery — would turn a 120 s hang into an immediate, honest 503. |


---

## 13. G-52, G-53 and G-54 — fixed 2026-08-25

All three came out of one incident, and all three are now closed and tested.

| # | Fix |
|---|---|
| **G-52** | New `kvstream.backend.health`. `/health` reports `backend_reachable` and `backend_serving` separately and returns 503 unless the backend completed a **bounded trial generation**. The probe is real load, so it is cached (30 s), single-flighted, and independently timed out; `?probe=true` forces a refresh. |
| **G-53** | New `kvstream.admission.drift`. Served seconds-per-token against the calibration baseline, using the same normalised signal as the knee detector so a traffic-mix change does not read as backend failure. Warns and reports; deliberately does **not** re-tune the budget by itself. |
| **G-54** | A circuit breaker in front of admission. Five consecutive backend failures and the gateway fails fast with 503 + `Retry-After` instead of queueing; one trial after the cooldown decides whether to close. Client 4xx never counts. |

### Verified against the live backend

- Gateway pointed at the wrong model id → `backend_reachable: true`, `backend_serving: false`, 503, with the
  detail naming the rejected trial generation. Under the old code this returned **200**.
- Gateway pointed at the correct id → `ready` in 172 ms, 503 → 200, and a real completion through the gateway
  in 214 ms.

That first case is worth noting on its own: readiness caught a **model misconfiguration**, not just a stall.
A gateway configured for a model the backend does not serve was previously indistinguishable from a healthy
one.

### A coupling bug found while wiring it

`BackendHealth` held its own reference to the client, so replacing `gateway.backend` left health probing a
backend nothing else was using — and reporting confidently about it. `Gateway.backend` is now a property whose
setter rebinds health and discards stale readiness. The failure mode was only ever in the direction of false
confidence, which is the worst direction for a health check.

### What these fixes do not do

They make a stalled backend **visible and cheap**, not impossible. §11's finding stands: admission control did
not prevent the runtime from exhausting itself, because the pressure was cumulative across a session. What is
different now is that the gateway notices within one readiness interval, stops sending traffic into it, tells
an orchestrator through the status line, and says the budget it is holding no longer matches the backend it is
holding it for. Preventing the exhaustion is the in-engine track (P4), not the gateway's to fix.


---

## 14. Re-run on hardware with the fixes in — 2026-08-25

`foundry 0.8.119`, `phi-3-mini-4k`, arms run **separately** via the new `--arm` flag so a gateway effect can
be told apart from an order effect. The previous session's comparison was worthless precisely because it could
not.

### A healthy backend: the gateway costs a little and buys a tighter tail

30 mixed requests to 400 prompt tokens, 12 concurrent, same backend, direct arm first:

| | done | ok% | p50 | p99 | goodput |
|---|---|---|---|---|---|
| direct to runtime | 30/30 | 100% | 11.1s | 14.0s | 1.06 r/s |
| through KVStream | 30/30 | 100% | 11.7s | **12.3s** | 0.98 r/s |

About 8% of goodput spent for about 12% off the tail. That is the *same shape* as the simulated result in §10
— admission control is not free when nothing was going to fail — and it is now confirmed on real hardware
rather than inferred from a model.

An earlier pair in this session ran the gateway arm **first** and it also came out ahead, so the advantage is
not an artefact of arm order. That pair is not quoted: a stale background `foundry service restart` from
earlier in the session fired mid-run and moved the endpoint from 51266 to 51264, which is what produced the
direct arm's nine errors. Disclosed rather than reported.

### The load that killed the runtime last time

60 requests with ~1200-token prompts and `max_tokens=128`, all through the gateway:

- **32 served, 28 shed with 503, and the runtime was still serving afterwards** (0.34s for a probe).
- Peak queue depth 52; 24 admitted from the queue on top of the fast path.
- Backend resident memory reached 8.9 GB and held. The unprotected equivalent last session reached 13.4 GB and
  **stopped serving permanently**.

That is the proposal's central claim, on real hardware, in the one regime where it is actually true: the
runtime survived a load that had previously destroyed it.

### Three things the run exposed

1. **G-55 (new): shedding is too slow to be useful.** Every 503 came back at **120.1s** — the admission
   timeout, to the decimal. A client that is going to be refused should learn that in milliseconds, not after
   two minutes of holding a connection. With a queue of 52 against a budget of 8 and ~10s per request, the
   gateway already had everything it needed to know the request could not be served in time. Rejecting on a
   predicted wait, rather than on an elapsed one, is the fix. "Clean backpressure" is not clean at 120 seconds.
2. **The circuit breaker never tripped — correctly.** The backend never failed during this run, so there was
   nothing to trip on. The breaker's behaviour is covered by tests and was verified live earlier against a
   wrong-model-id backend, where readiness correctly reported `serving: false` and returned 503.
3. **Token mode silently became concurrency mode**, because the calibration record was keyed
   `device=cpu-fallback-gpu-ep-skipped` while the gateway resolved `device=windows-amd64`. That is G-13 working
   exactly as designed — refusing a budget measured in a different environment — and it is logged and visible
   in `/status` as `fallback:concurrency`. Worth noting that the run above therefore measured a **concurrency
   cap of 8**, not the token budget. The token-budget claim still has not been exercised on real hardware.

| # | Gap |
|---|---|
| **G-55** ✅ | **FIXED.** **Shed load was shed slowly.** Rejection happens only when `admission_timeout_seconds` elapses, so a request that was never going to be served still occupies a client connection for the full timeout — measured at 120.1s. The queue depth, the budget and the observed service rate are all known at admission time; a request whose predicted wait exceeds the timeout should be refused immediately with `Retry-After`. |


---

## 15. G-55 — fixed 2026-08-25, after two wrong answers

Measured before: every 503 came back at **120.1 s**, the admission timeout to the decimal. Measured after, on
the same hardware and the same load: **median 8.1 s**, minimum 0.1 s.

The mechanism is a predicted-wait check — a queued request re-estimates whether the queue can still reach it
inside its deadline, and gives up early when it cannot. Getting there took three attempts, and the two failures
are more instructive than the fix.

### Attempt 1 — predict once, on arrival. Did not fire at all.

The drain rate was an EWMA of gaps between completions, learned from the five small warm-up requests: 31.9
completions/second. The heavy requests that followed took ten seconds each, but the queue was already deep
before any of them finished, so every admission decision used the rate from when the system was idle.
Predicted wait: 1.7 s. Actual: 120 s. `hopeless_rejections: 0`.

This is the same normalisation trap as the calibration sweep in §10 — a rate measured on one request class does
not describe another.

### Attempt 2 — re-check while waiting, and decay the rate during silence. Fired, but late.

Re-checking is right and was kept. The decay was not: it measured *time since the last completion*, and the
overload produced eight requests that failed **instantly**. Those failures kept refreshing the timestamp, so
the backend never looked silent even while nothing real was being served. Rejections happened, but only once a
request was near its deadline — still ~120 s.

### Attempt 3 — trailing-window throughput. Fired fast, then starved itself.

Counting completed cost over a ten-second window cannot be fooled by instant failures. Shedding became
immediate: median 0.2 s. Then the healthy-load check: **19 of 20 perfectly serviceable requests were refused**.

The dynamic is worth naming, because it is not obvious and it is self-reinforcing:

> a low measured rate causes rejections → rejections prevent completions → without completions the rate stays
> low → everything is refused, permanently.

The estimator starved itself. A throughput predictor with authority over admission can talk itself into
refusing a backend that is working perfectly.

### The fix: prediction only gets a vote when the system is unambiguously saturated

```
reject only if:   in_flight >= budget            (every unit of budget in use)
            and:  queue_depth >= budget          (a real queue, not a blip)
            and:  predicted_wait > deadline * 1.5 (missed by a wide margin, never marginally)
```

Anywhere else, waiting is the safe answer. The predictor cannot tell request classes apart, so it is
deliberately biased toward *not* refusing — a request wrongly refused is a hard failure, a request wrongly
queued only costs latency.

### Verified on real hardware, all three phases

| Phase | Result |
|---|---|
| 20 light requests, healthy backend | **20/20 served** — no false rejections |
| 60 heavy requests, overload | 52 shed, **median 8.1 s** (was 120.1 s), 8 upstream errors |
| Backend errors → breaker opens | subsequent requests refused in **0.04 s** |
| After the 30 s cooldown | one trial → 200 in 0.53 s → breaker closed → **15/15 served** |

That last pair is G-52 and G-54 closing the loop on hardware as well: the breaker opened on real backend
failures, failed fast, probed once, and restored full service by itself.

### The blunter lever still matters

`admission_timeout_seconds` defaults to **120 s**, and these runs used 20 s. No predictor makes a 120-second
deadline reasonable for an interactive client — if callers will not wait two minutes, the timeout should not be
two minutes. The prediction refines that decision; it does not replace it.
