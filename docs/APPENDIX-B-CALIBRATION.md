# Appendix B — Calibration Methodology

*Companion to "KVStream — A Concurrency Gateway for Microsoft Foundry Local".
Referenced from §2, §5.2 and §8.6 of that document.*

This appendix describes how KVStream measures a Foundry Local instance's
KV-token budget `B`, why each step is there, and what the resulting number does
and does not mean. §8.6 offers this methodology to the Foundry Local team as
reusable **whether or not KVStream itself is the right place for admission
control** — the measurement problem is the same either way.

---

## B.1 What is being measured, and why it has to be measured

The limiter on a single device is KV-cache memory, which scales with each
request's context length. A gateway sitting at the HTTP layer cannot read that
memory: it has no access to the runtime's address space, and co-location does
not change this (§7). So `B` cannot be *asserted* from device specifications. It
has to be *observed* from the outside, through the only signal available —
how the runtime's latency and error behaviour change as load rises.

`B` is therefore defined operationally:

> **`B` is the largest sustained in-flight token load at which the runtime's
> per-token service time has not yet inflected, scaled down by a safety margin.**

That is a statement about observed behaviour, not about bytes of VRAM. Every
limitation below follows from that.

---

## B.2 The two regimes

§2 notes that the ceiling has two regimes, and distinguishing them matters:

| Regime | What you observe | What it means |
|---|---|---|
| **Software concurrency cap** | Latency rises roughly linearly with load; no errors; the runtime queues internally | The runtime is serialising work. Extra concurrency buys nothing but does not break anything. |
| **KV-memory exhaustion** | Latency inflects sharply, then requests fail or the runtime stops accepting connections | The device is out of KV cache. This is the cliff the budget exists to stay off. |

The sweep detects the first of these to happen. It does **not** currently report
which one it found — an error-terminated sweep is evidence of the second, and a
latency-terminated sweep is evidence of the first, but the code does not label
it. That is a known gap, and a place where the Foundry Local team's own
instrumentation would be far more informative than black-box probing.

---

## B.3 The procedure

### B.3.1 Warm-up

The first request against a freshly loaded model pays for graph initialisation,
weight page-in and allocator warm-up. Including it makes every later measurement
look fast by comparison, which moves the detected knee *later* and produces a
budget that is too large.

**Default: one warm-up request, discarded.** `--warmup N`.

### B.3.2 Mixed request shapes

A budget measured on identical requests describes the one workload where a token
budget makes no difference to a request-count cap. The proposal's argument for
token-based admission rests on size variation, so the probe varies size:

| Shape | Prompt | Generation |
|---|---|---|
| small | `probe_prompt_tokens / 4` | `probe_max_tokens / 2` |
| medium | `probe_prompt_tokens` | `probe_max_tokens` |
| large | `probe_prompt_tokens × 2` | `probe_max_tokens` |

Shapes are cycled deterministically across the concurrent batch at each level.

### B.3.3 The health signal must be shape-independent

This is the subtlest step, and the one that was wrong in an earlier version of
this code.

Once the probe mixes request sizes, **raw latency cannot be compared across
concurrency levels**. At concurrency 1 the batch contains only the smallest
shape; at concurrency 2 it also contains a larger one. The p99 rises — but from
the shape mix, not from congestion. A knee detector reading raw p99 declares the
knee at concurrency 2 on a machine that is nowhere near its limit.

The fix is to normalise: the signal is the **p99 of service time per requested
token**, `latency / (prompt_tokens + max_tokens)`. This removes the shape and
leaves the load.

> This error was found by building the demonstration harness in
> `benchmarks/admission_benchmark.py` and noticing that calibration against a
> runtime with a known optimum of 4 concurrent produced a budget admitting 1.
> Unit tests on a synthetic backend did not catch it, because those tests used a
> uniform shape. It is recorded here because the failure mode is easy to
> reproduce in any similar tool.

### B.3.4 Repeated trials

Each concurrency level is measured `--trials` times (default 3) and the
latencies from every trial are **pooled before** the percentile is taken. A p99
over a handful of samples is mostly noise; pooling makes a single unlucky
request unable to define the knee.

### B.3.5 Doubling, then bisection

Concurrency doubles — 1, 2, 4, 8, … — until the signal exceeds
`latency_ratio × baseline` (default 2.0) or a request errors.

Doubling alone finds the right order of magnitude in a logarithmic number of
steps, but resolves the knee only to the previous power of two. If the true
ceiling is 12, doubling reports 8 and **a third of the machine is discarded**.
So the sweep then bisects between the last healthy point and the first unhealthy
one, up to three refinement steps.

### B.3.6 Safety margin

`B = last_healthy_inflight_tokens × 0.85`.

The margin covers the gap between a probe workload and real traffic, and the
error in KVStream's own token estimation (see B.5). It is a judgement, not a
derivation.

---

## B.4 The key: what a budget is valid for

A budget is only meaningful for the environment it was measured in, so records
are stored under a four-part key:

```
(model, device, cli_generation, backend_version)
```

Lookups are strict:

| Match | Behaviour |
|---|---|
| **exact** | used silently |
| **partial** — same model and device, different Foundry version or CLI generation | used, with a warning naming both versions |
| anything else | **not used**; the gateway falls back to a plain concurrency cap |

The last row is the important one. A budget measured on other hardware is worse
than no budget at all, because it is confidently wrong. §8.6 asks for the
Foundry Local version to be part of this key precisely because the service-based
CLI and the SDK-backed `foundry server` need not enforce concurrency the same
way — and KVStream has not measured either.

`device` is a label, not a detection. The gateway cannot see which accelerator
the runtime chose; operators running more than one must set `backend.device`
themselves.

---

## B.5 What the number does not mean

- **It is not a memory reading.** It is a behavioural estimate with a safety
  margin, and its accuracy depends on re-calibration whenever the model, device,
  runtime version or workload shape changes materially.
- **It is denominated in estimated tokens.** KVStream does not ship the model's
  tokenizer, so admission costs are estimates. Measured error is in
  `benchmarks/estimator_benchmark.py`; at the default safety factor the
  calibrated estimator still under-counted 16% of held-out samples. `B` absorbs
  some of that through its 0.85 margin; `admission.token_safety_factor` buys the
  rest explicitly.
- **It says nothing about which regime was hit** (B.2).
- **It has not been validated against real Foundry Local hardware.** The
  algorithm is unit-tested against a synthetic backend with a known concurrency
  ceiling, and the end-to-end demonstration runs against a modelled runtime.
  Both establish that the method finds a ceiling it was not told; neither
  establishes what Foundry Local's ceiling actually is. **This is the single
  largest open item in the project.**

---

## B.6 Reproducing it

```bash
# Against a live Foundry Local instance:
kvstream calibrate --model phi-3-mini --device npu --trials 5

# Against the modelled runtime, end to end, with the before/after comparison:
python benchmarks/admission_benchmark.py
```

The sweep points are stored alongside the budget in
`.kvstream/calibration.json`, so the evidence travels with the number:

```json
{
  "budget_tokens": 688,
  "model": "sim-model",
  "device": "windows-amd64",
  "measured_at": 1756060000.0,
  "sweep": [
    {"concurrency": 1, "inflight_tokens": 82,  "p99_seconds": 0.05, "p99_per_token": 0.00061, "errors": 0},
    {"concurrency": 2, "inflight_tokens": 346, "p99_seconds": 0.17, "p99_per_token": 0.00063, "errors": 0}
  ]
}
```

---

## B.7 What would make this unnecessary

Everything above is black-box inference, and it is only needed because the
runtime does not say what it can take. Two changes on the Foundry Local side
would replace most of it:

1. **A reported concurrency limit** — whatever the server actually enforces,
   exposed in `foundry status --output json` or the SDK. Calibration could then
   be validated against ground truth rather than being merely self-consistent
   (§8.6).
2. **A KV-memory high-water signal** — even a coarse "cache utilisation"
   percentage would distinguish B.2's two regimes directly, and would let a
   gateway react to real pressure instead of inferring it from latency.

Neither requires KVStream to exist. If the admission problem is better solved
inside `foundry server` than in a sidecar, this methodology transfers unchanged
— that is why it is written down separately from the gateway that currently
implements it.
