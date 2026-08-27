"""
The proposal's central claim, measured: does KVStream keep a runtime responsive?

Fires the same mixed-size, concurrent load twice — once straight at the runtime,
once through KVStream — and reports what each client actually experienced.

    python benchmarks/admission_benchmark.py
    python benchmarks/admission_benchmark.py --concurrency 48 --requests 240
    python benchmarks/admission_benchmark.py --backend-url http://localhost:5273

With no ``--backend-url`` the target is the modelled runtime in
:mod:`simulated_foundry` — reproducible anywhere, and honest about being a model
rather than Foundry Local. Point ``--backend-url`` at a real instance and the
same driver measures the real thing.

By default KVStream is not *told* the runtime's ceiling: it runs its own
calibration sweep first and admits against whatever budget that produced. That
is the whole claim in one run — the gateway discovers a limit it was never given
and then holds the system to it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from simulated_foundry import RuntimeModel, build_simulator  # noqa: E402

from kvstream.admission import calibration_key_for  # noqa: E402
from kvstream.admission.calibration import CalibrationService  # noqa: E402
from kvstream.app import build_app  # noqa: E402
from kvstream.backend import FoundryClient  # noqa: E402
from kvstream.config import Settings  # noqa: E402


@dataclass
class Result:
    label: str
    latencies: list[float] = field(default_factory=list)
    errors: int = 0
    refused: int = 0
    wall_seconds: float = 0.0

    @property
    def completed(self) -> int:
        return len(self.latencies)

    def pct(self, fraction: float) -> float:
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        return ordered[min(int(len(ordered) * fraction), len(ordered) - 1)]

    @property
    def throughput(self) -> float:
        """Goodput: only requests that actually returned an answer count."""
        return self.completed / self.wall_seconds if self.wall_seconds else 0.0

    @property
    def attempted(self) -> int:
        return self.completed + self.errors + self.refused

    @property
    def success_rate(self) -> float:
        return self.completed / self.attempted if self.attempted else 0.0

    def as_dict(self) -> dict:
        return {
            "label": self.label,
            "completed": self.completed,
            "errors": self.errors,
            "refused_503": self.refused,
            "p50_ms": round(self.pct(0.50) * 1000),
            "p95_ms": round(self.pct(0.95) * 1000),
            "p99_ms": round(self.pct(0.99) * 1000),
            "mean_ms": round(statistics.fmean(self.latencies) * 1000) if self.latencies else 0,
            "wall_seconds": round(self.wall_seconds, 2),
            "throughput_rps": round(self.throughput, 2),
            "success_rate": round(self.success_rate, 4),
        }


def _workload(
    count: int,
    seed: int,
    model: str = "sim-model",
    max_prompt_tokens: int = 4000,
    max_output_tokens: int = 256,
) -> list[dict]:
    """
    A mixed-size workload, because that is where a token budget earns its keep.

    The proposal is explicit that a fixed request count is simultaneously too
    conservative for small requests and too aggressive for large ones, so a
    uniform load would measure the one case where the two are equivalent.
    """
    rng = random.Random(seed)
    # Clamped to the backend's own limits: a real model advertises
    # maxInputTokens / maxOutputTokens, and a workload that exceeds them
    # measures rejection, not admission.
    shapes = [
        (min(40, max_prompt_tokens), min(32, max_output_tokens)),
        (min(200, max_prompt_tokens), min(64, max_output_tokens)),
        (min(1200, max_prompt_tokens), min(128, max_output_tokens)),
        (min(4000, max_prompt_tokens), min(256, max_output_tokens)),
    ]
    requests = []
    for _ in range(count):
        prompt_tokens, max_tokens = rng.choice(shapes)
        requests.append(
            {
                "model": model,
                "messages": [{"role": "user", "content": "word " * prompt_tokens}],
                "max_tokens": max_tokens,
                "temperature": 0.7,  # never cacheable: measure the runtime, not the cache
            }
        )
    return requests


async def _drive(url: str, requests: list[dict], concurrency: int, label: str) -> Result:
    result = Result(label=label)
    semaphore = asyncio.Semaphore(concurrency)
    started = time.perf_counter()

    async with httpx.AsyncClient(timeout=120.0) as client:

        async def one(payload: dict) -> None:
            async with semaphore:
                t0 = time.perf_counter()
                try:
                    response = await client.post(f"{url}/v1/chat/completions", json=payload)
                except Exception:  # noqa: BLE001
                    result.errors += 1
                    return
                if response.status_code == 503:
                    result.refused += 1
                elif response.status_code >= 400:
                    result.errors += 1
                else:
                    result.latencies.append(time.perf_counter() - t0)

        await asyncio.gather(*[one(payload) for payload in requests])

    result.wall_seconds = time.perf_counter() - started
    return result


@asynccontextmanager
async def _serve(app, port: int):
    """Run an ASGI app on a real port for the duration of the block."""
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


async def _calibrate(backend_url: str, store: Path, model: str, args) -> int:
    settings = Settings()
    settings.backend.base_url = backend_url
    settings.backend.model = model
    client = FoundryClient(
        base_url=backend_url, model=model, discover=False, use_foundry_cli=False
    )
    service = CalibrationService(client, str(store), calibration_key_for(settings.backend))
    budget = await service.calibrate(
        probe_prompt_tokens=args.probe_prompt_tokens,
        probe_max_tokens=args.probe_max_tokens,
        max_concurrency=args.calibrate_max_concurrency,
        trials=args.calibrate_trials,
        warmup=1,
    )
    await client.aclose()
    return budget


async def run(args: argparse.Namespace) -> dict:
    requests = _workload(
        args.requests, args.seed, args.model, args.max_prompt_tokens, args.max_output_tokens
    )
    store = Path(args.store)

    if args.backend_url:
        return await _bench(args, requests, args.backend_url, None, store)

    model = RuntimeModel(optimal_concurrency=args.optimal, hard_limit=args.hard_limit)
    async with _serve(build_simulator(model), args.sim_port) as backend_url:
        return await _bench(args, requests, backend_url, model, store)


async def _bench(args, requests, backend_url: str, model, store: Path) -> dict:
    """
    Run one or both arms.

    ``--arm`` exists because the arms are not independent when the target is a
    real runtime: the first arm's load changes the state the second arm
    measures. Running them separately, and alternating which goes first, is the
    only way to tell a gateway effect from an order effect.
    """
    results = []
    peak_direct = None
    direct = None

    if args.arm in ("both", "direct"):
        direct = await _drive(backend_url, requests, args.concurrency, "direct to runtime")
        results.append(direct)
        peak_direct = dict(model.stats()) if model else None
        if model:
            model.reset()

    budget = args.budget_tokens
    if args.arm == "direct":
        return _payload(args, backend_url, model, results, peak_direct, budget, None)

    # Through KVStream, admitting against a budget it measured for itself.
    if budget <= 0:
        budget = await _calibrate(backend_url, store, args.model, args)
        if model:
            model.reset()

    settings = Settings()
    settings.port = args.gateway_port
    settings.backend.base_url = backend_url
    settings.backend.model = args.model
    settings.backend.use_foundry_cli = "never"
    settings.admission.mode = "tokens"
    settings.admission.budget_tokens = budget
    settings.admission.admission_timeout_seconds = 120.0

    async with _serve(build_app(settings), args.gateway_port) as gateway_url:
        through = await _drive(gateway_url, requests, args.concurrency, "through KVStream")
        async with httpx.AsyncClient(timeout=60.0) as client:
            status = (await client.get(f"{gateway_url}/status")).json()
            health = (await client.get(f"{gateway_url}/health")).json()

    if args.arm == "gateway":
        results.insert(0, through)
    else:
        results.append(through)
    payload = _payload(args, backend_url, model, results, peak_direct, budget, status)
    payload["gateway_health"] = {
        "backend_serving": health.get("backend_serving"),
        "readiness": health.get("readiness"),
        "circuit_breaker": health.get("circuit_breaker"),
        "drift": health.get("drift"),
    }
    return payload


def _payload(args, backend_url, model, results, peak_direct, budget, status) -> dict:
    return {
        "target": "simulated runtime" if model else backend_url,
        "model": args.model,
        "arm": args.arm,
        "runtime_model": (
            {"optimal_concurrency": args.optimal, "hard_limit": args.hard_limit}
            if model
            else None
        ),
        "requests": args.requests,
        "client_concurrency": args.concurrency,
        "calibrated_budget_tokens": budget,
        "calibrated": args.budget_tokens <= 0 and args.arm != "direct",
        "results": [r.as_dict() for r in results],
        "runtime_peak_direct": peak_direct,
        "runtime_peak_through_kvstream": dict(model.stats()) if model else None,
        "gateway_admission": (status or {}).get("admission"),
    }


def _print(payload: dict) -> None:
    print(f"Target              : {payload['target']}")
    print(f"Model               : {payload['model']}")
    if payload["runtime_model"]:
        rm = payload["runtime_model"]
        print(
            f"Modelled runtime    : optimal concurrency {rm['optimal_concurrency']}, "
            f"hard limit {rm['hard_limit']}"
        )
    print(f"Load                : {payload['requests']} requests, "
          f"{payload['client_concurrency']} concurrent, mixed sizes")
    source = "measured by kvstream calibrate" if payload["calibrated"] else "configured"
    print(f"Admission budget    : {payload['calibrated_budget_tokens']} tokens ({source})")
    print()

    header = (
        f"| {'':<20} | {'done':>5} | {'503':>4} | {'ok%':>5} | {'p50':>9} | {'p99':>9} "
        f"| {'goodput':>8} |"
    )
    print(header)
    print("|" + "-" * 22 + "|" + "-" * 7 + "|" + "-" * 6 + "|" + "-" * 7 + "|"
          + ("-" * 11 + "|") * 2 + "-" * 10 + "|")
    for row in payload["results"]:
        print(
            f"| {row['label']:<20} | {row['completed']:>5} | {row['refused_503']:>4} "
            f"| {row['success_rate'] * 100:>4.0f}% | {row['p50_ms']:>7}ms | {row['p99_ms']:>7}ms "
            f"| {row['throughput_rps']:>5} r/s |"
        )
    print()
    print(
        "Read the latency columns with care: the direct run's percentiles cover only "
        "the requests that survived, so they are survivor-biased and flatter the "
        "unprotected case. A rejected request has no latency at all - which is the "
        "point. Compare ok% and goodput first."
    )

    if payload.get("gateway_health"):
        gh = payload["gateway_health"]
        print()
        print(
            f"Gateway view        : serving={gh['backend_serving']} "
            f"breaker={gh['circuit_breaker']['state']} "
            f"drift={gh['drift']['state']}"
        )
    if payload["runtime_peak_direct"]:
        print()
        print(
            f"Runtime peak concurrency: {payload['runtime_peak_direct']['peak_in_flight']} direct, "
            f"{payload['runtime_peak_through_kvstream']['peak_in_flight']} through KVStream"
        )
        print(
            f"Runtime refusals        : {payload['runtime_peak_direct']['refused']} direct, "
            f"{payload['runtime_peak_through_kvstream']['refused']} through KVStream"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--requests", type=int, default=120)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--optimal", type=int, default=4, help="simulated optimal concurrency")
    parser.add_argument("--hard-limit", type=int, default=16, help="simulated stall threshold")
    parser.add_argument("--budget-tokens", type=int, default=0, help="0 = calibrate first")
    parser.add_argument("--backend-url", help="measure a real backend instead of the model")
    parser.add_argument("--model", default="sim-model", help="model id to send in each request")
    parser.add_argument(
        "--arm", choices=("both", "direct", "gateway"), default="both",
        help="which arm(s) to run; separate runs let you alternate the order",
    )
    parser.add_argument("--max-prompt-tokens", type=int, default=4000)
    parser.add_argument("--max-output-tokens", type=int, default=256)
    parser.add_argument("--probe-prompt-tokens", type=int, default=200)
    parser.add_argument("--probe-max-tokens", type=int, default=64)
    parser.add_argument("--calibrate-max-concurrency", type=int, default=32)
    parser.add_argument("--calibrate-trials", type=int, default=2)
    parser.add_argument("--sim-port", type=int, default=8791)
    parser.add_argument("--gateway-port", type=int, default=8792)
    parser.add_argument("--seed", type=int, default=20260824)
    parser.add_argument("--store", default=".kvstream/benchmark-calibration.json")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    payload = asyncio.run(run(args))
    _print(payload)
    if args.json:
        args.json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nWrote {args.json}")


if __name__ == "__main__":
    main()
