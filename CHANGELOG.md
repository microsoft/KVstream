# Changelog

All notable changes to KVStream are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/).

> **Status note.** Version 1.0.0 describes the current in-repository state of the
> ground-up rewrite. It has **not** yet been published to PyPI; dates reflect
> development, not release. Items under *Not yet implemented* are deliberately
> absent so the codebase contains no unwired features.

## [Unreleased]

### Added — `kvstream serve --max-tokens`, 2026-10-20

- **A request that sets no generation limit is now sent one.** `kvstream serve
  --max-tokens N` (config `admission.default_max_tokens`, default **512**) is sent
  upstream as `max_tokens` for any chat or legacy-completions request that sets
  neither `max_tokens` nor `max_completion_tokens`. It was already the length such
  a request was *costed* at, but it was never sent, so the request could generate
  up to the model's own limit and exceed the budget it was admitted on. A limit the
  client sets is forwarded unchanged, even if larger.
- **Behaviour change:** clients that relied on the model's own default length now
  get 512 tokens unless they set a limit. `admission.apply_default_max_tokens:
  false` restores the previous behaviour.
- Verified live against Foundry Local 0.10.3 with `--max-tokens 20`: a request with
  no limit returned 20 completion tokens (`finish_reason: length`), streamed and
  non-streamed; a request setting `max_tokens: 40` returned 40; with the setting off
  the same request returned 512; token mode recorded 0 overshoots. Tests in
  `tests/integration/test_review4_fixes.py` (6 mutation checks).

### Fixed — live accounting, and a second README truthfulness pass, 2026-10-20

- **A running stream keeps its reservation until it finishes.** Live accounting
  settled a stream every 16 tokens to what it had generated *so far*, handing back
  the rest of its reservation while it still needed it (48 of 64 tokens at token
  16, measured); other requests could be admitted into that headroom, and the
  stream then grew back past it — exceeding the budget at the default
  `reserve_completion_ratio` of 1.0 and counting overshoots. The reservation is now
  held while generating and settled to actual use once the generation finishes.
  Regression test and mutation check in `tests/integration/test_review4_fixes.py`.
- **"At the default 1.0 overshoot is impossible" was false**, in the README, the
  metric's help text, a docstring and a config comment, even after that fix: a
  request that omits `max_tokens` is costed at `admission.default_max_tokens` and
  can generate more. Now stated, with a test.
- **README corrected where it did not match the code or the stored evidence:**
  the modelled-runtime "held at 5 concurrent" result came from the calibration
  bisection bug fixed earlier today, and re-runs give different budgets (now
  stated, with the re-run numbers); a stored token-mode real-hardware run exists
  that the README said did not (described, with its caveats); the calibrated
  estimator's worst under-count is 16%, not 18%; a mid-stream backend failure is an
  error event, not a 502; `/health` 503 does not always mean requests will fail;
  model auto-detection does infer a model when no CLI is available; admission and
  shedding claims reworded to what the code does; Docker caveats added (Foundry
  Local binds loopback; image environment variables override a mounted config).

### Fixed — embeddings for a chat model, and README accuracy, 2026-10-20

- **Embeddings requests naming a chat model are refused, not forwarded.** On
  Foundry Local CLI 0.10.3 such a request was followed, both times it was tried,
  by the service stopping for every client. KVStream now reads each model's
  `type` from the same `foundry cache list --output json` call that reports what
  is loaded, and answers `400` (`model_not_embedding`) for a model of type `Chat`
  before admission, without contacting Foundry Local. Verified live: refused in
  7 ms, Foundry Local still serving afterwards. Fails open where the type cannot
  be read (no CLI, containers). `/status` shows the types under
  `model.inventory.types`. Tests in `tests/integration/test_review4_fixes.py`
  (5 mutation checks).
- **README and user guide corrected.** The install step `cd KVstream/kvstream-foundry`
  named a directory that does not exist. Both said the 0.10.x CLI path was never
  tested; it has been exercised live against 0.10.3, and the docs now say what was
  run against which generation (and that 0.8.x has not been re-run since). Both
  implied Foundry Local returns no `usage`; 0.10.3 does. "Clients cannot route
  around the gateway" now says what it means (Foundry Local stays reachable
  directly). Added the 0.10.3 calibration result, `only-if-cached`, and the
  architecture and user-guide documents to the documentation table.

### Fixed — fourth code review (whole codebase), 2026-10-20

Each fix has a regression test in `tests/integration/test_review4_fixes.py` that
fails with its bug re-introduced (22 mutation checks).

- **Calibration no longer stores an inflated budget.** Bisection judged a point
  healthy against the last healthy point instead of the sweep's first, so it could
  accept up to `latency_ratio`² times the baseline (3.5x was accepted against a 2x
  limit) and B was computed from it.
- **An auto-detected model follows a model swap.** It was kept forever, so after
  unloading it and loading another, `/health` probed a model that was not loaded and
  reported "not serving" until a restart. A configured `--model` is never replaced.
- **`docker-compose.yml` works with Foundry Local 0.10.x.** It shipped port 5273 and
  the catalog alias `phi-3-mini`, which the inference API refuses (HTTP 400). The URL
  now comes from a required `FOUNDRY_URL`, the model from optional `KVSTREAM_MODEL`.
- **Removed `docker-compose.gpu.yml`.** It reserved a GPU for the proxy, which runs
  no model, and overrode an `ollama` service the base file does not define.
- **The container healthcheck outlasts the readiness probe.** It allowed 4 s while
  `/health` can run a generation of up to `readiness_timeout_seconds` (15 s), marking
  a serving gateway unhealthy whenever the probe was slow.
- **One outcome per streamed request in `kvstream_requests_total`.** `served` was
  counted at the first token, so an abandoned stream counted twice and a stream that
  failed part-way counted as served. It is now `served`, `abandoned` or `error`.
- **Latency histograms reach 10 minutes.** The default buckets stopped at 10 s, so
  generations and admission waits all fell in `+Inf`. *The bucket boundaries of
  `kvstream_request_seconds` and `kvstream_admission_wait_seconds` changed;
  dashboards using `le` labels need updating.*
- **`kvstream_backend_drift_ratio` never reports a stale value.** It was only set
  when non-zero, so it kept the old ratio after the drift monitor was reset.
- **A malformed `measured_at` in the calibration store no longer stops the gateway
  starting** (`TypeError` while choosing the newest record).
- **Token estimators are per model and per route.** One estimator learned every
  model's tokenizer at once, and embedding usage trained the chat estimator. Each
  (route, model) now learns separately, bounded at 64; drift detection observes only
  the calibrated model. `/status` adds `token_estimators`.
- **A timeout during calibration no longer labels the runtime "capped".** Only
  refusals do; a serialising runtime under load times out, which is not a cap.
- **Estimated `usage` no longer includes `token_safety_factor`**, which is admission
  headroom, not a count.
- **A different model build is a partial calibration match.** `…-gpu:1` matched a
  record for `…-gpu:2` exactly and silently inherited its budget.
- **`kvstream bench`** rejects `--total 0` (was an `IndexError`) and reports
  latency for successful requests only.
- **`/status` lists the `completions` route.**

### Fixed — third code review, 2026-10-20

Each fix has a regression test (`tests/integration/test_review3_fixes.py`, and
`tests/unit/test_backend_health.py` for the probe limit) that fails with its bug
re-introduced (14 mutation checks).

- **Environment variables now override `kvstream.yaml`, as documented.** The file
  was passed as constructor arguments, which outrank every settings source, so
  `KVSTREAM_PORT=9000` lost to `port: 8080` in YAML. The file is now a settings
  source ranked below the environment.
- **A missing `--config` file is an error.** It was silently skipped and the gateway
  ran on defaults. A missing, non-mapping or invalid file now exits with code 2 and
  says why. The implicit `kvstream.yaml` in the working directory stays optional.
- **`/v1/models/{id}` sends the id as one encoded path segment.** A decoded `?`
  started a query string upstream and `..` was normalised to another endpoint;
  dot segments are now answered 404 without contacting the backend.
- **A readiness probe that runs out of time says so.** On Python 3.10
  `asyncio.TimeoutError` is not the builtin `TimeoutError`, and on any version the
  HTTP client's read timeout could fire first; both were reported as the backend
  having "rejected" the generation.
- **Audio uploads are bounded while they arrive.** A chunked upload (no
  `Content-Length`) was spooled in full and read into memory before the size check.
  The body is now counted as it is received, an oversized file is refused before
  admission, the file is read into memory only after admission (so at most
  `audio_max_concurrency` are held at once), and the parsed form is closed so its
  temporary files are removed.
- **Credentials are not sent to a backend found by the localhost scan.** The scan
  adopts any server answering `/v1/models`; `backend.api_key` and forwarded
  `Authorization` headers now go only to a configured URL or one the `foundry` CLI
  reported. Foundry Local needs no auth, so a scanned Foundry Local is unaffected.
- **`/health?probe=true` is rate-limited.** Each forced probe is a generation outside
  the admission budget; forcing now honours
  `backend.readiness_force_min_interval_seconds` (default 5 s, never longer than
  `readiness_interval_seconds`).
- **Shutdown stops the gateway's own streams before closing the HTTP client.** After
  the drain timeout they kept running, failed against the closed client and were
  counted as backend failures. They are now cancelled and awaited; their clients get
  an error frame, never `[DONE]`.

### Fixed — second code review, 2026-10-11

Each fix has a regression test (`tests/integration/test_review2_fixes.py`, and
`tests/unit/test_cli.py` for the CLI) that fails with its bug re-introduced.

- **A model loaded after start-up no longer strands queued requests.** When the model
  was auto-detected later and a calibrated budget applied, the capacity manager was
  replaced while requests were in flight. They released into the new manager, the
  old one's queue never moved, and a queued request was refused after its full
  timeout (measured: 503 after 6 s, with capacity free after 2 s). A new budget now
  waits for an idle moment (nothing reserved, nothing queued, not shutting down) and
  is installed at admission time. Requests are costed by the manager they enter,
  never in one unit and charged in another. `/status` shows a waiting budget as
  `budget_pending`. Under unbroken load the previous limit stays in force until the
  first idle moment.
- **Streamed responses are cached with coalescing off.** The cache used the
  coalescing key, which does not exist when coalescing is disabled. (A regression
  from making streams gateway-owned.)
- **`kvstream health` reports serving, not liveness, and exits 1 unless serving.** It
  called a backend that answered `/v1/models` but could not generate "healthy" and
  exited 0. It now prints the gateway's hint, waits 30 s by default (`--timeout`)
  rather than 5 s, because the readiness probe can legitimately take most of 15 s,
  and says it timed out rather than that the gateway is unreachable.
- **`GET /v1/models` returns 502 when Foundry Local cannot be read**, not `200` with
  an empty list.
- **`Cache-Control: only-if-cached` returns 504 on a miss** (RFC 9111) instead of
  calling the backend.
- **Server errors on embeddings, legacy completions and transcription count toward
  the circuit breaker**, and a success counts too, as on chat. 501 (Not Implemented)
  is excluded, because it describes the route, not the backend's health.
- **No retry against a URL rediscovery did not find.** During the rediscovery
  cooldown, a refused request was retried at the configured fallback URL, and the
  error named that URL.
- **A failed start-up still reports the admission budget.** The provisional
  resolution is silent, so a missing token budget went unreported if start-up
  raised.

### Fixed — CI, 2026-10-11

- **The CI workflow can pass, and covers every platform KVStream supports.** It
  ran on Linux only and could not pass: lint and mypy pointed at `kvstream/` (the
  code is in `src/kvstream/`), `pytest --cov` needed `pytest-cov`, which was not a
  dependency (now added to `dev`), and `ruff format --check` failed on 44 files. It
  now runs on Linux, Windows and macOS, Python 3.10 and 3.12 (plus 3.11 on Linux),
  and can be triggered manually. The steps pass in a fresh Windows virtualenv; the
  Linux and macOS jobs have not run yet.
- **The code is formatted with `ruff format`** (44 files, whitespace and wrapping
  only; the full suite, lint and mypy are unchanged).

### Fixed — found during live verification, 2026-10-11

- **Foundry CLI output is decoded as UTF-8.** It was decoded with the locale code
  page (cp1252 on Windows). Measured: `foundry model load` prints `● success`, the
  decoder thread raised on the `●`, the whole output was lost, and KVStream received
  the text `"None"`. The CLI commands KVStream relies on print JSON that has been
  plain ASCII so far, so this had not failed in practice. Any non-ASCII character
  would have made their output silently disappear.
- **"Unreachable" now means unreachable.** Every network failure used to be
  reported as "Foundry Local is unreachable … start it with `foundry server start`",
  including a running server that dropped the connection mid-request. Foundry 0.10.3
  does exactly that for a streamed request naming an unloaded model. Only a refused
  connection now gets that message. A timeout says it timed out, and a dropped
  connection says the server is running and to check the model is loaded.
- **The localhost port scan works on non-English Windows, and finds IPv6-only
  listeners.** It found listening ports by matching the word `LISTENING`, which
  Windows localizes. It now recognises a listener by structure (a wildcard foreign
  address, `0.0.0.0:0` or `[::]:0`) and never reads the state column. It also used
  `netstat -p TCP`, which lists IPv4 only, so a backend bound to `[::1]` was never
  found; on the development machine the new scan finds two such listeners the old
  one missed. Output is decoded as ASCII with replacement: netstat writes the OEM
  code page (437 here) and Python's default is the ANSI one (1252), so a
  localized word could have crashed the decoder. Tested with real captured output
  re-rendered in localized forms, and live with the CLI lookup disabled. Not tested
  on an actual non-English Windows install.
- **The localhost port scan works on macOS.** Every non-Windows platform ran `ss`,
  which macOS does not have, so on a Mac the scan always found nothing and
  discovery depended entirely on the `foundry` CLI or a pinned URL. macOS now runs
  `netstat -an -p tcp` and parses the BSD format (`address.port`, wildcard foreign
  address `*.*`). The parser is tested against a documented macOS sample, and a
  platform-independent test opens a real listening socket and requires the scan to
  find it. **Not yet run on a Mac:** that test has passed on Windows only; the
  macOS CI job (see "Fixed — CI") will be its first run on a Mac.
- **`calibrate` checks the model is loaded before sweeping.** With the model
  unloaded it now says so ("downloaded but not loaded … `foundry model load`"),
  sends no probes and saves nothing.

### Fixed — code review, 2026-10-11

Each fix has a regression test in `tests/integration/test_review_fixes.py`. Each test
was checked to *fail* with its bug re-introduced, and to pass with the fix.

- **Shutdown drains when shutdown starts.** Uvicorn waits for open connections
  before the lifespan shutdown hook, and a queued request is an open connection.
  Measured: a request queued at Ctrl+C was served after waiting for its slot, and
  `drain_timeout_seconds` bounded nothing. `GatewayServer` now turns queued requests
  away at once (503), and `drain_timeout_seconds` is uvicorn's graceful-shutdown
  timeout. Work still running at the deadline is cancelled, which uvicorn reports as
  HTTP 500. Also fixed underneath: a request queued while the queue was drained was
  treated as admitted, with no reservation.
- **No backend probe before every request.** With discovery on (the default), each
  request first sent `GET /v1/models`, doubling upstream traffic. The resolved URL is
  now trusted until a connection is refused. The backend is then re-discovered and
  the request retried once; a refused connection never reached a server, so the
  retry cannot duplicate work.
- **`calibrate` fails instead of saving a non-measurement.** When every probe
  failed (e.g. the model was not loaded) it saved the smallest probe size as the
  budget and reported success. It now exits non-zero, gives the reason, and
  saves nothing.
- **Calibration records are found.** `calibrate` labelled records with `unknown`
  for the CLI generation and Foundry version, which `serve` never looks up. It now
  records the detected values, and connects exactly as `serve` does. A record is
  found with or without the model's `:N` suffix, and the newest of several
  matches wins (it used to be whichever was stored first).
- **Token-mode predictive shedding works.** It compared queue *length* with a
  *token* budget, so it could never fire. The drain rate is now also counted in
  admitted-cost units, the same unit as the queue it is divided into. The live run
  then showed a second half of the same bug: "saturated" meant `in_flight >= budget`,
  which a 70-token request against a 96-token budget never reaches, although
  nothing else fits. Saturation now means *the waiting request does not fit*.
  Measured live (30 concurrent requests, 3 s timeout): 20 refused on predicted wait,
  16 of them within 0.1 s of arriving, where before all were refused only after
  the full 3 s.
- **No false budget warnings at token-mode startup.** The budget was resolved once
  before the CLI version and model were known, which logged "no budget calibrated
  … falling back" or "different environment" just before the real, exact
  resolution. That provisional pass is now silent, and startup always reports
  its own result.
- **Coalescing and the cache separate callers.** The forwarded `Authorization`
  credential is now part of the key. Before, two callers sending the same body
  shared one upstream call made with the first caller's credentials.
- **Streamed legacy completions stream.** `/v1/completions` with `stream: true` is
  relayed as a stream; it used to return HTTP 200 with an error object. A non-JSON
  body with a success status is now a 502.
- **Drift compares like with like.** The baseline is now the median seconds per
  processed token at the knee, time to first token included, which is how live
  samples are taken (streamed samples previously excluded time to first token). It
  is stored as `baseline_seconds_per_token`. Older records leave drift inactive
  until re-calibrated, rather than reporting a meaningless ratio.
- **The token estimator learns from text tokens.** It attributed the chat
  template's per-message tokens to the text, then added them again when estimating.
- **Streamed chat is timed** in `kvstream_request_seconds`. **`kvstream_backend_up`**
  reflects the last known state (startup, `/health`, completed requests); it used
  to change only when `/health` was called.
- **The calibration store is written atomically**, so a crash mid-write no longer
  wipes it. A non-numeric value is skipped rather than crashing startup.
- **Calibration counts generated tokens**, not `max_tokens`. A model that stopped
  early inflated throughput and skewed the serialising/batching verdict.
- **Discovery matches the configured model with or without its `:N` suffix**,
  ending a false "does not list the configured model" warning on CLI 0.10.3.

### Changed — `--model` is optional

- **The model is auto-detected when exactly one is loaded.** `serve` and
  `calibrate` adopt it and log `model: <id> (auto-detected: …)`. `bench` uses the
  model the running gateway resolved. Nothing is guessed: with no model or several
  models loaded, KVStream logs why, keeps forwarding requests (each names its own
  model), and `/health` reports not ready with the reason until a model is
  determined. A model loaded later is picked up by the next health check.
  `/health` and `/status` report the model and how it was determined.
- **The default `backend.model` is now unset** instead of `phi-3-mini`, an id
  Foundry Local rejects. Leaving it out used to make the readiness probe fail,
  so `/health` reported a working backend as not serving.
- **New `backend/inventory.py`** tracks downloaded models (`/v1/models`) and loaded
  models (`foundry cache list --output json`) separately. Measured on CLI 0.10.3:
  `/v1/models` keeps listing a model after it is unloaded.

### Fixed — streams for an unloaded model

- **A streamed request for a downloaded-but-unloaded model is a 400
  `model_not_loaded`, not a backend failure.** Foundry 0.10.3 answers it with
  `200 text/event-stream` and a dropped connection. The earlier check used only
  `/v1/models`, which still lists unloaded models, so these counted against the
  circuit breaker. A stream that fails before its first token now re-reads the
  inventory before blaming the backend, which covers an unload inside the 30 s
  cache window.

### Fixed — failure semantics (docs/ARCHITECTURE.md §9, §13)

Each fix has a regression test in `tests/integration/test_failure_semantics.py` and
was checked live against Foundry Local CLI 0.10.3 (ARCHITECTURE §19).

- **A backend 4xx no longer trips the circuit breaker or becomes a 502.** Chat
  requests naming a wrong model alias returned 502 and counted as backend failures,
  so five of them opened the breaker for every caller. `FoundryClientError` now
  carries the backend's status and body through to the client.
- **Streamed requests for a model Foundry does not serve get a 400.** Foundry
  0.10.3 answers them with `200 text/event-stream` and a dropped connection, which
  looked like a crash. The model is checked against `/v1/models` before dispatch
  (`:N` suffix ignored, fails open).
- **A wrong model alias no longer switches off usage reporting** for the whole
  process. `stream_options` is treated as rejected only if the retry without it is
  accepted.
- **A half-open breaker trial is always resolved.** A trial refused by admission,
  answered with a client error, or cancelled used to hold the breaker half-open,
  refusing every request.
- **Coalescing cancellation no longer crosses between callers.** A cancelled
  non-streaming leader stranded its followers, and one follower's cancellation
  could cancel the shared result.
- **Streams are owned by the gateway.** When the client that started a coalesced
  stream disconnected, followers received a truncated response ending in `[DONE]`.
  Generations now run as gateway-owned tasks: `[DONE]` is sent only for a complete
  response, and the upstream call is cancelled when the last client leaves.
- **The startup model check is now based on what the inference API accepts.** It
  used to compare the configured id with `foundry model list` exactly. On CLI 0.10.3
  that falsely warned about a loaded model (the catalog lists `…:2`, `/v1/models`
  lists the id without the suffix), and never warned about a catalog alias such as
  `phi-3-mini-4k`, which the API rejects with 400. It now checks `/v1/models` first,
  ignores a `:N` suffix, and uses the catalog only to make the message precise.
- **The `serve` banner no longer presents the fallback URL as the backend** when
  discovery is on. The resolved backend URL and how it was found are logged at
  startup.
- **Transcription upload handling is typed without a `type: ignore`**, which newer
  Starlette versions report as unused.

### Added — Milestone A: make the existing claims true

Milestone A of the gap analysis in [`docs/GAP-ANALYSIS.md`](docs/GAP-ANALYSIS.md), which
maps this codebase against the Foundry Local technical proposal. It closes the gaps
where KVStream claimed a capability that would not have held up against a real
Foundry Local instance.

- **Real token counts are now actually requested (G-14).** Streamed calls carry
  `stream_options.include_usage`; without it most OpenAI-compatible servers never
  emit a usage chunk, so the online token calibration had nothing to learn from.
  A backend that rejects the field gets one retry without it, a log line saying
  real counts are unavailable, and no further attempts. Non-streamed client
  requests are now forwarded **non-streamed** (`FoundryClient.chat_once`) rather
  than being converted to a stream, so the backend's own `usage` block is read
  directly. `GET /status` reports `backend.usage_reporting`.
- **Live accounting (G-12).** `CapacityManager.adjust` settles a reservation
  against the response's true length as it streams, returning headroom that a
  short generation was never going to use instead of holding
  `prompt + max_tokens` until teardown. New
  `admission.reserve_completion_ratio` reserves only part of `max_tokens` up
  front; a generation that outgrows its reservation is topped up rather than
  truncated, and every such event is counted. At the default `1.0` the budget
  still cannot be breached.
- **Environment-keyed calibration (G-13).** The store now holds one record per
  `(model, device, CLI generation, Foundry Local version)` instead of a single
  flat record that each calibration overwrote. Lookups are strict: exact matches
  are used silently, same-model/same-device matches across versions warn, and
  anything else is not used at all — the gateway falls back to `concurrency`
  mode rather than admitting against a budget measured on other hardware. v1
  stores are migrated, not discarded. `kvstream calibrate --device` and
  `backend.device` set the label.
- **OpenAI-shaped errors (G-32).** All errors return
  `{"error": {"message", "type", "code", "param"}}`; `503` carries `Retry-After`.
- **Backend failures are `502` on both paths (G-31).** The non-streaming path
  previously leaked a bare `500`.
- **`GET /health` returns `503` when the backend is unreachable (G-33)**, with a
  hint naming the right start command for each CLI generation. Orchestrators key
  on the status line, not the JSON body.
- **Discovery cooldown (G-28).** `backend.discovery_cooldown_seconds` (default
  5s) bounds how often a full localhost sweep may run. A backend that has gone
  away no longer turns every inbound request into a subprocess plus a port scan.
- **Single-process guard (G-36).** KVStream refuses to start when
  `WEB_CONCURRENCY`/`UVICORN_WORKERS`/`GUNICORN_WORKERS`/`KVSTREAM_WORKERS` is
  above 1 — N workers would enforce N budgets and silently void calibration.
- **New metrics:** `kvstream_backend_errors_total{phase}`,
  `kvstream_reservation_overshoot_total`,
  `kvstream_reservation_reclaimed_tokens_total`, `kvstream_active_requests`,
  `kvstream_discovery_scans`, `kvstream_backend_up`.
- **`GET /status`** now reports budget provenance, the calibration key, and
  backend/discovery state — what an operator needs when the budget looks wrong.
- **Tests: 48 → 91.** New coverage for the backend client's wire behaviour
  (against a mock transport, so the stubs can no longer hide what is actually
  sent), discovery cooldown, live accounting, the calibration store, and error
  and health semantics.

### Added — Milestone B: P0 from the proposal (§8)

Version compatibility across both Foundry Local CLI generations, and the route
coverage gap. Verified live against `foundry 0.8.119` on Windows.

- **Version-aware backend resolution (G-01…G-06).** New
  `kvstream.backend.resolver` implements the proposal's ordered chain, first hit
  wins: explicit URL → CLI-assisted → localhost scan → actionable failure.
  - **Explicit configuration is now authoritative.** `--backend-url`,
    `backend.base_url` and `KVSTREAM_BACKEND__BASE_URL` pin the endpoint and
    skip discovery entirely; previously a configured URL still fell through to
    a scan if it did not answer. A pinned URL is also what makes a container
    deployment work. `backend.pin_url` overrides the automatic behaviour.
  - **CLI-assisted lookup.** New `kvstream.backend.foundry_cli` reads
    `foundry --version` to select the dialect, then `foundry status --output
    json` (0.10.x) or `foundry service status` (service-based). The 0.10.x JSON
    schema is unverified and has no stability contract, so extraction walks the
    document for a URL-shaped value rather than assuming a field name, and every
    failure path — missing binary, non-zero exit, timeout, malformed JSON,
    unexpected shape — is a miss that falls through to the scan. A binary that
    could not be classified is offered both dialects.
  - **`backend.use_foundry_cli`** (`auto` | `always` | `never`), where `auto`
    enables the CLI on a host and disables it in a container.
  - **Actionable failures.** Errors and `/health` hints now name the start
    command for the generation actually detected — `foundry service start` vs
    `foundry server start` — instead of guessing.
- **Route coverage (G-07…G-09).** `POST /v1/embeddings` and
  `POST /v1/audio/transcriptions` are proxied. Embeddings are costed by input
  tokens and share the KV-token budget; their `usage` also feeds the token
  estimator. Audio has no honest token cost measurable at the gateway, so it is
  admitted under a **separate plain concurrency limit**
  (`routes.audio_max_concurrency`, default 2) rather than being given an
  invented cost — with a `routes.audio_max_upload_mb` cap returning 413. Both
  routes pass the backend's status, body and content type through unchanged.
- **Model catalog check (G-10).** At startup, `foundry model list --output json`
  is compared against the configured model, so a typo produces a clear warning
  instead of an opaque 404 on the first inference call.
- **Backend candidate ranking (G-29).** Discovery no longer takes the first
  localhost port that answers `/v1/models`. Another KVStream is excluded
  outright — every response now carries an `X-KVStream-Version` header, so
  gateways recognise each other and cannot form a proxy loop — a port serving
  the configured model wins, an identifiable non-Foundry server is demoted, and
  a port with a model loaded beats one without.
- **CLI generation and Foundry version now key calibration.** The fields added
  in Milestone A are populated by a startup CLI probe, and the admission budget
  is re-resolved once they are known, before any request is served.
- **Per-route metrics.** `kvstream_requests_total` and `kvstream_rejected_total`
  gained a `route` label, `kvstream_request_seconds` is now per route, and
  `kvstream_audio_budget` / `_in_flight` / `_queue_depth` expose the audio
  limiter. `/status` reports the resolution source, the detected CLI, and which
  routes are proxied.
- **`python-multipart`** is now a declared dependency (FastAPI needs it to parse
  the transcription body).
- **Tests: 91 → 166.**

### Added — Milestone C: agent-workload compatibility (§8.4 clients)

The proposal's premise is multi-agent orchestration, and those workloads are
built on tool calling — which the gateway could not represent at all. The
allow-list request model and the synthesised response are both gone.

- **Tool calling works (G-40, G-41).** `tools`, `tool_choice`, and messages with
  `content: null` + `tool_calls`, `tool_call_id`, `name`, or multimodal
  content-part arrays are all accepted. Previously every one of these was a 422,
  which blocked LangGraph, AutoGen, Semantic Kernel and the OpenAI SDK outright.
- **Pass-through with a typed overlay (G-42).** KVStream validates only what it
  needs to cost and stream a request and forwards the client's own JSON object
  untouched, so `seed`, `response_format`, `logprobs`, `logit_bias`,
  `frequency_penalty`, `presence_penalty`, `user` and anything a future API
  version adds survive. They were being dropped silently — a client asking for
  JSON mode got prose.
- **Responses are proxied, not rebuilt (G-43).** The backend's actual body and
  its actual SSE chunks are forwarded, preserving `tool_calls`, the opening
  `{"role": "assistant"}` delta, streamed tool-call argument deltas, `logprobs`,
  `system_fingerprint` and the real completion `id`.
- **OpenAI defaults are no longer overridden (G-44).** An omitted `max_tokens`
  or `temperature` is no longer replaced with 512 / 0.8 and sent upstream; the
  model's own default applies. The arbitrary 32k `max_tokens` ceiling is gone.
  `n > 1` is allowed, and costed as `n × max_tokens` because that is what it
  occupies. New `admission.default_max_tokens` costs a request that omits the
  field, and is never sent to the backend.
- **Auth passthrough (G-45).** The caller's `Authorization` header is forwarded
  (`backend.forward_authorization`, default on) so KVStream can sit behind a
  gateway that does its own per-caller auth; `backend.api_key` sets a static
  token for deployments that need one.
- **`POST /v1/completions` and `GET /v1/models/{id}` (G-46).** The legacy
  completions route is admitted on the KV-token budget for the same reason as
  embeddings — an unadmitted route is a way around admission control.
- **Cache correctness.** The cache now stores the backend's real response body,
  or the exact recorded chunks, instead of a text string; a cached tool call
  would otherwise have replayed as an empty assistant turn. The key hashes the
  client's whole request (so a `tools` payload cannot collide with one without)
  and includes `stream`, so an entry is only replayed in the shape it was
  recorded in.
- **Token counts are asked for, never fabricated.** A client that did not send
  `stream_options.include_usage` no longer receives the usage chunk KVStream
  requested for its own accounting. When the backend reports no counts at all,
  the `usage` block is filled from KVStream's estimate and the response carries
  `X-KVStream-Usage: estimated` rather than passing an estimate off as measured.
- **Cost estimation understands agent transcripts.** `tool_calls` in a re-sent
  transcript are billed as prompt tokens, and streamed tool-call arguments as
  generated tokens. Non-text content parts are forwarded but not guessed at.
- **Tests: 166 → 213.**

### Added — Milestone D: finishing P1 and P2

The capacity manager and the cache both had correctness gaps that only show up
under the workload the proposal targets — mixed request sizes and streaming
agent traffic.

- **Fair admission (G-30).** Waiters are now admitted in strict arrival order
  from an explicit queue, and a release drains the front of the queue for as
  long as the next waiter fits. The previous `notify_all` + re-check had no
  ordering at all: whichever coroutine the loop scheduled first and happened to
  fit won, so a large request could be starved indefinitely by a stream of small
  ones, and every release woke O(N) coroutines to admit one. Requests that fit
  immediately no longer enter the queue at all, so they cannot consume queue
  depth from requests that genuinely wait. A timed-out or cancelled waiter
  leaves the queue and lets the rest through; one granted in the race with its
  own timeout keeps the reservation rather than throwing the work away.
  `/status` reports queue depth, head cost, oldest wait, peak depth and
  admitted/timed-out/rejected counts; `kvstream_admission_wait_seconds` measures
  the head-of-line cost the ordering deliberately accepts.
- **Relative KV costing across models (G-11).** New
  `kvstream.admission.geometry` derives `kv_bytes_per_token` from a model's
  published architecture (`2 × layers × kv_heads × head_dim × dtype_bytes`) and
  costs each request in proportion to the model the budget was calibrated for.
  Grouped-query attention is handled — KV heads, not attention heads. Geometry
  comes from a declared `models:` section, or best-effort from
  `foundry model show --output json`; an undeclared model weighs 1.0 and is
  costed exactly as before, so nothing is ever guessed.
- **Streaming coalescing (G-16).** New `StreamBroadcast` / `StreamCoalescer`:
  identical concurrent streams ride one upstream call, with followers replaying
  what they missed and then tracking the leader. Publishing never awaits, so a
  slow follower cannot stall the producer. Followers consume no admission budget
  and are labelled `X-KVStream-Coalesced: 1`. Leadership is claimed *before*
  queueing and pre-flighting — both of those await, and duplicates arriving in
  that window are exactly the ones coalescing exists to catch.
- **Cache controls (G-17/G-18).** Per-request `Cache-Control: no-store` /
  `no-cache` and `x-kvstream-cache` are honoured
  (`cache.respect_request_headers`), and responses above
  `cache.max_entry_bytes` are skipped rather than evicting everything useful —
  counted in `kvstream_cache_skipped_total`.
- **Calibration rigour (G-34).** The sweep now warms up before measuring (a cold
  first request otherwise becomes the baseline every later point is judged
  against), repeats each point and pools the latencies before taking the
  percentile, cycles a spread of request shapes rather than one uniform size,
  and **bisects** between the last healthy point and the first unhealthy one —
  doubling alone resolved the knee only to the previous power of two, discarding
  up to half the machine's capacity. `--trials`, `--warmup` and `--no-refine`
  expose all of it. The sweep points are stored alongside the budget as evidence.
- **Graceful shutdown (G-39).** `drain_timeout_seconds`: stop admitting, turn
  away anything still queued, and let in-flight requests finish. A request
  arriving during shutdown gets a 503 that says so, distinct from overload.
- **Status and metrics (G-37, G-38).** `/status` adds queue statistics, model
  geometry, streaming-coalescer state and calibration provenance including
  `measured_at` and `age_seconds`. New series:
  `kvstream_admission_wait_seconds`, `kvstream_calibration_age_seconds`,
  `kvstream_cache_skipped_total`.
- **Tests: 213 → 276.**

### Fixed

- `kvstream serve` crashed with a `UnicodeEncodeError` on Windows whenever
  stdout was redirected (`kvstream serve > log.txt`), because the startup banner
  could not be encoded in cp1252. Pre-existing; unrelated to the proposal.
- A non-streamed backend failure that returned a body with no `choices` is
  reported as a 502 rather than being passed off as a KVStream response.
- Log messages were mangled on Windows whenever stderr was redirected — the
  earlier encoding fix covered stdout only, and the logs go to stderr.

### Added — Milestone E: the proposal's artefacts

The document makes measurable claims and offers a reusable methodology. Neither
existed. This milestone builds them, and one of them immediately falsified a
claim the README had been making.

- **Appendix B (G-25)** — [`docs/APPENDIX-B-CALIBRATION.md`](docs/APPENDIX-B-CALIBRATION.md).
  The calibration methodology the proposal cites three times and offers to the
  Foundry Local team as reusable whether or not KVStream is the right place for
  admission control: what is measured, why each step exists, the two regimes of
  the ceiling, what the number does not mean, and the two changes on the Foundry
  side that would make most of the procedure unnecessary.
- **Reproducible estimator benchmark (G-49)** —
  `benchmarks/estimator_benchmark.py`, scoring the token estimator against
  `cl100k_base` over a seeded corpus of prose, JSON, code and agent transcripts,
  with calibration trained on one half and scored on the other.
- **Before/after demonstration (G-51)** — `benchmarks/admission_benchmark.py`
  plus `benchmarks/simulated_foundry.py`, a model of the failure mode in §2.
  KVStream is not told the runtime's ceiling: it calibrates first, then admits
  against what it measured. On the default run, 120 mixed-size requests at 32
  concurrent went from 27 completed and 93 refused, to 120 completed and 0
  refused, with goodput up from 4.0 to 6.5 req/s.
- **Container and CI (G-27)** — `Dockerfile` (unprivileged, wheel-built,
  `HEALTHCHECK` on `/health`), `docker-compose.yml`, and a GitHub Actions
  workflow running the suite on Linux *and Windows* across Python 3.10/3.12,
  executing both benchmarks, and building the image. Discovery and the CLI
  lookup are disabled in the image: neither can work across a network
  namespace, so an explicit backend URL is required and the image says so.
- **CLI test coverage (G-50)** — `tests/unit/test_cli.py`. Every `typer`
  command was previously untested, including `calibrate`, which produces the
  number admission control depends on.
- **Documentation (G-26)** — README gains the measured evidence, a docs index,
  and the container section.
- **Tests: 276 → 292.**

### Changed

- **The README's estimator numbers were replaced with measured ones, and its
  central safety claim was corrected.** The old table (~21% / ~43% / ~26% mean
  error, "0% under-counts", "never breaches the budget") was not reproducible
  from anything in the repository. Measured over 400 held-out samples: the
  calibrated estimator is considerably *more* accurate than claimed (6.8% mean
  absolute error, not ~26%) — but it **does** under-count, on 16% of samples,
  worst case by 16%. The claim that it never under-estimates was false. The
  README now carries the real numbers, the per-shape breakdown (code is hardest
  at 15.2%, JSON easiest at 1.7%), and the measured
  `admission.token_safety_factor` needed for a genuine zero-under-count
  guarantee: 1.188, so 1.25 in practice.

### Measured (2026-08-25, real hardware)

First run against a live `foundry 0.8.119` with `phi-3-mini-4k` loaded. Two
regimes, both worth recording.

- **Light load: the runtime serialises.** Throughput flat at ~2.6 r/s from
  concurrency 1 to 8, latency exactly linear, mean equal to max. It does not
  batch; optimal concurrency is 1. Calibration found that ceiling unaided on its
  first real run.
- **Sustained load: it stalls, permanently.** After 80 mixed-size requests with
  prompts to 1200 tokens at 16 concurrent, a 4-token request no longer returned
  within 180 s, with the inference process holding 13.4 GB resident for a 2.13 GB
  model. It did not recover; `foundry service restart` hung too. This is §2's
  stall observed directly, in Appendix B's KV-memory-exhaustion regime.
- **Admission control did not prevent it.** The gateway held the runtime to
  roughly one request at a time and it exhausted itself anyway, because the
  pressure was cumulative across the session rather than instantaneous
  concurrency. A budget from a sixty-second sweep does not describe what an hour
  of traffic does to the runtime's memory.
- **The before/after goodput comparison is not reported.** The first run showed
  KVStream roughly doubling goodput; the second showed both arms collapsing as
  the runtime died. Two runs that disagree that violently are not a result.
- **The runtime reports no `usage` block**, so online token calibration receives
  no samples against it and the *uncalibrated* estimator figures apply, not the
  calibrated ones. Exactly the case G-14 was written for; the gateway degrades as
  designed and reports it in `backend.usage_reporting`.
- **New gaps found only on hardware:** G-52 (`/health` is a liveness probe read
  as readiness — it stayed green throughout the stall), G-53 (nothing detects
  backend drift away from the calibration baseline), G-54 (a stalled backend
  consumes the whole admission queue for the length of the backend timeout; a
  circuit breaker would fail fast instead).

### Added — fast shedding (G-55)

Measured before: every 503 came back at 120.1 s, the admission timeout to the
decimal. Measured after, same hardware and load: **median 8.1 s**, minimum 0.1 s.

- **Predicted-wait rejection.** A queued request re-estimates, every
  `recheck_interval_seconds`, whether the queue can still reach it inside its
  deadline, and gives up early when it cannot — with a `Retry-After` derived
  from the measurement rather than a constant.
- **Drain rate is a trailing-window throughput** (`rate_window_seconds`), not an
  EWMA of gaps between completions. A burst of instant failures keeps gaps small
  and makes a dying backend look fast; counting completed work over a window
  cannot be fooled that way.
- **Prediction only gets a vote when the system is unambiguously saturated** —
  all budget in use, a queue at least as deep as the budget, and the deadline
  missed by `hopeless_margin` (default 1.5x). This is not caution for its own
  sake: without those guards the estimator **starves itself**. A low rate causes
  rejections, rejections prevent completions, the rate never recovers, and the
  gateway refuses a backend that is working. That was measured too — 19 of 20
  healthy requests wrongly shed — before the guards went in.
- New settings: `reject_when_hopeless`, `min_rate_samples`,
  `recheck_interval_seconds`, `rate_window_seconds`, `hopeless_margin`.
- **Tests: 341 → 354.**

Verified end to end on `foundry 0.8.119`: 20 light requests all served, 60 heavy
requests shed at a median of 8.1 s, backend errors opening the circuit breaker
(0.04 s refusals), and full recovery after the cooldown — one trial request, then
15 of 15 served.

Note that `admission_timeout_seconds` still defaults to 120 s. No predictor makes
a two-minute deadline reasonable for an interactive client; these runs used 20 s.

### Added — hardware findings fixed (G-52, G-53, G-54)

Three gaps that only appeared when the gateway was run in front of a Foundry
Local that had stalled. All three are the same incident seen from different
angles: the runtime stopped completing generations while `/v1/models` kept
answering 200 in 4 ms.

- **Readiness, reported separately from liveness (G-52).** New
  `kvstream.backend.health`. `/health` now distinguishes `backend_reachable`
  (answers `/v1/models`) from `backend_serving` (completed a bounded trial
  generation) and returns 503 unless the backend is actually serving. The probe
  is a real generation, so it is cached (`readiness_interval_seconds`, default
  30 s), single-flighted, and separately timed out
  (`readiness_timeout_seconds`, default 15 s) — a health check that becomes load
  is a health check that causes outages. `GET /health?probe=true` forces a fresh
  check. Verified live in both directions: a gateway pointed at the wrong model
  id correctly reported `serving: false`, and one pointed at the right id
  reported `ready` in 172 ms.
- **Circuit breaker (G-54).** After `circuit_breaker_failures` consecutive
  backend failures (default 5), requests fail fast with 503 and `Retry-After`
  instead of queueing behind a backend that will not answer — which previously
  meant every arrival waited the full backend timeout and the admission queue
  filled with work that could never complete. One trial request is admitted
  after `circuit_breaker_reset_seconds`; success closes the breaker, failure
  reopens it immediately. **Client 4xx never trips it**, so one malformed caller
  cannot take the gateway down for everyone.
- **Drift detection (G-53).** New `kvstream.admission.drift`. Served
  seconds-per-token is compared against the baseline recorded in the calibration
  sweep, using the same normalised signal the knee detector uses, so a change in
  traffic mix does not read as a failing backend. Crossing `drift_warn_ratio`
  (default 3.0) logs an actionable warning and shows in `/status`. It warns
  rather than re-tuning: a gateway that quietly moves its own limits during an
  incident is much harder to reason about than one that says "this is 4x slower
  than when you calibrated it".
- **New metrics:** `kvstream_backend_ready`, `kvstream_circuit_breaker_state`,
  `kvstream_backend_drift_ratio`. `/status` gains `backend_health` and `drift`.
- **Tests: 301 → 341.**

### Fixed

- **`Gateway.backend` is now a property that rebinds `BackendHealth`.** Health
  held its own reference to the client, so swapping the backend left it probing
  one nothing else was using — reporting confidently on the wrong thing.

- **The CLI-assisted endpoint lookup never actually worked for the service-based
  CLI** — found by running against a live `foundry 0.8.119`, not by any test.
  `foundry service status` reports the *status page*
  (`http://127.0.0.1:64164/openai/status`), not the API root, so probing
  `.../openai/status/v1/models` 404'd and resolution silently fell through to
  the port scan every time. Nothing looked broken because the fallback is
  designed to rescue exactly this, which is precisely why it went unnoticed.
  A CLI-reported endpoint is now tried both as given and as its bare origin,
  and whichever answers is used. Verified live: resolution now reports
  `source: foundry-cli` with no scan attempted at all.
- **The calibration sweep detected the knee far too early once Milestone D made
  the probe use mixed request shapes.** Raw latency is not comparable across
  concurrency levels when the shape mix changes with the level: a larger shape
  appearing at higher concurrency looks exactly like congestion. The health
  signal is now p99 of service time *per requested token*, which removes the
  shape and leaves the load. Against a runtime with a known optimum of 4
  concurrent, calibration went from producing a budget that admitted 1 request
  to one that admitted 5. Found by building the demonstration harness — the
  unit tests missed it because they used a uniform request shape.

### Not yet implemented (roadmap — intentionally absent)

- **Multi-machine fleet router (P3)** — cross-machine routing, instance registry,
  health reporting, tenancy/fair-queuing. One machine runs one Foundry Local, so
  this is a fleet-only add-on and is omitted to avoid unwired code.
- **Semantic cache** (§5.3 asks for exact *and* semantic; only exact-hash
  caching exists, and semantic matching is the one part of the proposal that
  needs a new dependency).
- **Authentication / TLS** — the gateway binds to loopback and has none of its
  own; put an authenticating reverse proxy in front of it.
- **Live-hardware calibration validation** — the sweep is unit-tested against a
  synthetic backend with a known ceiling, and demonstrated end to end against a
  modelled runtime, but has **not** been run against a live Foundry Local. Both
  establish that the method finds a ceiling it was not told; neither establishes
  what Foundry Local's ceiling actually is. This is the largest open item in the
  project.
- **Upstream `onnxruntime-genai` KV work (P4)** — the only path to raising a
  single device's true concurrent ceiling; an in-engine track, not this gateway.

## [1.0.0] — 2026-07-07

Complete ground-up rewrite. KVStream is now a truthful, Foundry-Local-only
admission-control **gateway** — an HTTP proxy with no model, no KV tensors, and
no ONNX Runtime dependency. Every module is wired and exercised by tests.

### Added

- **OpenAI-compatible gateway** (FastAPI): streaming and non-streaming
  `POST /v1/chat/completions`, `GET /v1/models`, `/health`, `/status`, `/metrics`.
- **Foundry Local ephemeral-port auto-discovery** — resolves the OS-assigned port
  by probing localhost for an OpenAI-compatible `/v1/models`, preferring a port
  with a model loaded; caches and re-resolves on failure.
- **KV-Capacity admission control** with two modes over one mechanism:
  - `concurrency` (default) — a request-count cap; works with no calibration.
  - `tokens` — a calibrated KV-token budget; admits by estimated request cost.
  - Reserve/queue/release, clean `503` backpressure, admission timeout, and a
    queue-depth cap; a single oversized request may run alone.
- **Calibration service** (`kvstream calibrate`) — a load sweep with knee
  detection that measures and persists the token budget `B` per `(model, device)`.
- **Self-calibrating token estimator** — a conservative two-signal heuristic
  (characters *and* word/punctuation units) that learns both ratios online from
  backend-reported `usage`, biased toward over- rather than under-estimation.
- **Opt-in response cache** — TTL + LRU, deterministic (temperature 0) requests
  only; replays for both streaming and non-streaming callers.
- **Request coalescer** — singleflight de-duplication of identical concurrent
  deterministic requests.
- **Real Prometheus metrics** — admission outcomes, rejections by reason, queue
  depth, budget utilization, cache hits, coalesced count, latency, and the
  learned token ratios (replaces a placeholder `/metrics`).
- **CLI**: `serve`, `health`, `status`, `calibrate`, `bench`.
- **Configuration** via `kvstream.yaml` and `KVSTREAM_*` environment variables.
- **Packaging & quality**: `src/` layout, `py.typed`, wheel build, 48 tests
  (unit + integration), clean `ruff` and `mypy`.
- **Value tests (AC-4 / AC-5)** demonstrating that token-budget admission packs
  more small requests and prevents the overload a fixed count cap causes on large
  ones — and that it offers no advantage on uniform workloads (stated honestly).

### Changed

- Narrowed scope to **Foundry Local only**; repositioned as truthful middleware.
- Admission is now **workload-aware** (token cost), not only a fixed request count.
- Streaming path pre-flights the first token so backend failures return a real
  error status before HTTP 200 is committed, and consumes the trailing `usage`
  chunk for calibration.
- License made **consistently MIT**, resolving the prior Apache/MIT mismatch.

### Removed

The following existed in 0.1.0 but were inert or overclaimed, and have been
deleted so the project makes no false capability claims:

- **`PagedKVCache` GPU tensor pool** — allocated but never read or written.
- **Paged-attention kernels** (`naive` / `flash` / `xformers`) — never invoked.
- **Hard KV-inject** (`save_kv_state` / `restore_kv_state`) — never wired into the
  scheduler.
- **Multi-backend adapters** (Ollama, llama.cpp, LM Studio) — out of scope for a
  Foundry Local middleware.
- All language and structure implying **KV-tensor access, PagedAttention, or an
  ONNX Runtime dependency**.

## [0.1.0] — 2026-06-20

Initial release of the original design (a "PagedAttention + continuous batching"
proxy). Retained here for history only. Its paged-KV, attention-kernel, and
hard-inject components were later found to be inert at the proxy layer and were
removed in 1.0.0; see that entry.
