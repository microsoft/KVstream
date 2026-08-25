# Benchmarks

Two harnesses, both runnable on any machine with no model download and no GPU.
Each says plainly what it measures and what it does not.

```bash
pip install -e ".[dev]" tiktoken
python benchmarks/estimator_benchmark.py
python benchmarks/admission_benchmark.py
```

---

## `estimator_benchmark.py` — how wrong is the token estimate?

KVStream is a proxy and does not ship the model's tokenizer, so every admission
cost is an estimate. This measures the error against a reference BPE tokenizer
(`cl100k_base` via `tiktoken`) over a seeded corpus of four text shapes: prose,
JSON, code, and a re-sent agent transcript including a tool call.

Calibration learns from one half of each shape and is scored on the other, so
the reported accuracy is on text the estimator has not seen.

**What it does not show.** `cl100k_base` is *not* the tokenizer of any Foundry
Local model. It is a real BPE tokenizer of the same family, so it is a fair
proxy for *shape* — where a heuristic breaks down and roughly how much — and it
is not a claim about absolute accuracy against phi-3-mini. The corpus is
synthetic and seeded, so it is reproducible rather than representative of your
traffic.

Results are written to `results-estimator.json` when you pass `--json`.

---

## `admission_benchmark.py` — is the gateway worth putting in the path?

Fires the same mixed-size concurrent load twice, once straight at a runtime and
once through KVStream, and reports what the clients actually experienced.

By default KVStream is **not told** the runtime's ceiling. It runs its own
calibration sweep first and admits against whatever budget that produced — so
the run demonstrates discovery and enforcement together, not just enforcement.

```bash
python benchmarks/admission_benchmark.py --requests 240 --concurrency 48
python benchmarks/admission_benchmark.py --backend-url http://localhost:5273
```

### The target is a model, unless you point it at a real one

With no `--backend-url`, the target is `simulated_foundry.py`: a deliberately
simple model of the failure mode the proposal describes in §2 — flat latency up
to an optimal concurrency, quadratic degradation past it, and outright refusal
beyond a hard ceiling. The degradation curve is a guess at the shape, not a
fitted model.

So the honest claim is: **KVStream keeps a runtime with these characteristics
responsive, and finds the ceiling on its own.** What Foundry Local's real
numbers are is a separate question, and `--backend-url` against a live instance
is what answers it. That has not been run on hardware yet.

### It is not free when the runtime is not overloaded

Run the same comparison at a load the runtime can already absorb and the result inverts:

```
python benchmarks/admission_benchmark.py --requests 40 --concurrency 12
```

|                      |  done |  503 |   ok% |       p50 |       p99 |  goodput |
|----------------------|-------|------|-------|-----------|-----------|----------|
| direct to runtime    |    40 |    0 |  100% |    1045ms |    3205ms |  5.7 r/s |
| through KVStream     |    40 |    0 |  100% |    2174ms |    2649ms |  4.8 r/s |

Twelve concurrent against a hard limit of sixteen: nothing was going to be refused, so admission control has no
failures to prevent. It costs about 15% of goodput and buys a tighter tail (p99 3205ms → 2649ms) by holding
the runtime at 2 concurrent instead of 12.

That is the honest shape of the trade, and it matches what the proposal says in §10 — the budget's advantage
exists when the runtime is actually pressed, and it is candid about adding little when it is not. Measure your
own load before assuming the gateway is a free win.

### Reading the output

Compare `ok%` and goodput first. The direct run's latency percentiles cover only
the requests that survived, so they are survivor-biased and flatter the
unprotected case — a rejected request contributes no latency at all, which is
precisely the problem. Higher p50 through the gateway means requests are
*waiting* instead of being *refused*.
