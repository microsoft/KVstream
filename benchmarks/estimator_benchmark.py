"""
Measure KVStream's token estimator against a reference tokenizer.

KVStream is a proxy and does not ship the model's tokenizer, so every admission
cost is an estimate. This harness says how wrong that estimate is, in which
direction, and how much online calibration recovers.

Run it::

    python benchmarks/estimator_benchmark.py
    python benchmarks/estimator_benchmark.py --json results.json

What the numbers mean, and what they do not
-------------------------------------------
The reference is ``cl100k_base`` via `tiktoken`. **That is not the tokenizer of
any Foundry Local model.** It is a real BPE tokenizer of the same family, so it
is a fair proxy for *shape* — where a heuristic breaks down and by roughly how
much — and it is not a claim about absolute accuracy against phi-3-mini or any
other model. Treat these as indicative and reproducible, not authoritative.

The direction matters more than the magnitude. Under-counting over-admits, which
causes the exact overload the gateway exists to prevent; over-counting only
costs some utilization. KVStream is built to err upward, and the "under-counted"
column is the one to read first.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from corpus import build_corpus  # noqa: E402

from kvstream.tokenization import (
    TokenEstimator,
    count_units,
    estimate_tokens,
)  # noqa: E402

REFERENCE_ENCODING = "cl100k_base"


@dataclass
class Scores:
    """Error statistics for one estimator over one set of samples."""

    name: str
    errors: list[float] = field(default_factory=list)  # signed relative error
    pairs: list[tuple[int, int]] = field(default_factory=list)
    under: int = 0
    total: int = 0

    def record(self, estimated: int, actual: int) -> None:
        if actual <= 0:
            return
        self.total += 1
        self.errors.append((estimated - actual) / actual)
        self.pairs.append((estimated, actual))
        if estimated < actual:
            self.under += 1

    def under_rate_at(self, safety_factor: float) -> float:
        """Under-count rate if every estimate were scaled by ``safety_factor``."""
        if not self.pairs:
            return 0.0
        missed = sum(
            1 for est, act in self.pairs if math.ceil(est * safety_factor) < act
        )
        return missed / len(self.pairs)

    def safety_factor_for_zero_under(self) -> float:
        """Smallest multiplier that would have covered every sample seen."""
        if not self.pairs:
            return 1.0
        return max(1.0, max(act / est for est, act in self.pairs if est > 0))

    @property
    def mean_abs_error(self) -> float:
        return statistics.fmean(abs(e) for e in self.errors) if self.errors else 0.0

    @property
    def bias(self) -> float:
        return statistics.fmean(self.errors) if self.errors else 0.0

    @property
    def under_rate(self) -> float:
        return self.under / self.total if self.total else 0.0

    @property
    def worst_under(self) -> float:
        """Largest single under-count, as a fraction. 0.0 if it never under-counts."""
        return min([e for e in self.errors if e < 0], default=0.0)

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "samples": self.total,
            "mean_abs_error": round(self.mean_abs_error, 4),
            "bias": round(self.bias, 4),
            "under_rate": round(self.under_rate, 4),
            "worst_under": round(self.worst_under, 4),
        }


def _load_reference():
    try:
        import tiktoken
    except ImportError:
        print(
            "This benchmark needs a reference tokenizer:\n"
            "    pip install tiktoken\n"
            "It is not a runtime dependency of KVStream — only of this measurement.",
            file=sys.stderr,
        )
        raise SystemExit(2) from None
    return tiktoken.get_encoding(REFERENCE_ENCODING)


def run(seed: int, per_shape: int, train_fraction: float) -> dict:
    encoding = _load_reference()
    corpus = build_corpus(seed=seed, per_shape=per_shape)

    naive = Scores("naive chars/4")
    uncalibrated = Scores("KVStream, uncalibrated")
    calibrated = Scores("KVStream, calibrated")
    per_shape_scores: dict[str, Scores] = {}

    # Calibration learns from one half and is judged on the other, so the
    # reported accuracy is on text the estimator has never seen. Measuring on
    # the training text would flatter it.
    estimator = TokenEstimator()
    fresh = TokenEstimator()

    for shape, samples in corpus.items():
        split = max(1, int(len(samples) * train_fraction))
        train, held_out = samples[:split], samples[split:]

        for text in train:
            actual = len(encoding.encode(text))
            estimator.observe(len(text), count_units(text), actual)

        shape_scores = Scores(shape)
        for text in held_out:
            actual = len(encoding.encode(text))
            naive.record(max(1, len(text) // 4), actual)
            uncalibrated.record(fresh.estimate_text(text), actual)
            estimated = estimator.estimate_text(text)
            calibrated.record(estimated, actual)
            shape_scores.record(estimated, actual)
        per_shape_scores[shape] = shape_scores

    return {
        "reference": REFERENCE_ENCODING,
        "seed": seed,
        "samples_per_shape": per_shape,
        "train_fraction": train_fraction,
        "held_out_samples": naive.total,
        "estimators": [naive.as_dict(), uncalibrated.as_dict(), calibrated.as_dict()],
        "calibrated_by_shape": {k: v.as_dict() for k, v in per_shape_scores.items()},
        "learned": estimator.stats(),
        "safety": {
            "needed_for_zero_under": round(
                calibrated.safety_factor_for_zero_under(), 3
            ),
            "under_rate_by_factor": {
                str(f): round(calibrated.under_rate_at(f), 4)
                for f in (1.0, 1.1, 1.25, 1.5, 2.0)
            },
        },
        "sanity": {
            "estimate_tokens_is_pure": estimate_tokens("abcd", 4.0, 1.0) == 1,
        },
    }


def _table(rows: list[dict]) -> str:
    header = f"| {'Estimator':<24} | {'Mean abs err':>12} | {'Bias':>8} | {'Under-counts':>12} |"
    rule = f"|{'-' * 26}|{'-' * 14}|{'-' * 10}|{'-' * 14}|"
    lines = [header, rule]
    for row in rows:
        lines.append(
            f"| {row['name']:<24} | {row['mean_abs_error'] * 100:>11.1f}% "
            f"| {row['bias'] * 100:>+7.1f}% | {row['under_rate'] * 100:>11.1f}% |"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--seed", type=int, default=20260824)
    parser.add_argument("--per-shape", type=int, default=200)
    parser.add_argument("--train-fraction", type=float, default=0.5)
    parser.add_argument("--json", type=Path, help="also write the full result here")
    args = parser.parse_args()

    result = run(args.seed, args.per_shape, args.train_fraction)

    print(
        f"Reference tokenizer : {result['reference']} (NOT a Foundry Local model tokenizer)"
    )
    print(f"Held-out samples    : {result['held_out_samples']}")
    print()
    print(_table(result["estimators"]))
    print()
    print("Calibrated estimator, by text shape:")
    print(_table(list(result["calibrated_by_shape"].values())))
    print()
    learned = result["learned"]
    print(
        f"Learned ratios      : {learned['chars_per_token']} chars/token, "
        f"{learned['units_per_token']} units/token "
        f"(from {learned['samples']} usage reports)"
    )
    worst = min(row["worst_under"] for row in result["estimators"][1:])
    print(f"Worst under-count   : {worst * 100:.1f}% (KVStream estimators)")
    print()
    safety = result["safety"]
    print(
        "Under-count rate of the calibrated estimator by admission.token_safety_factor:"
    )
    for factor, rate in safety["under_rate_by_factor"].items():
        print(f"  {factor:>4} -> {rate * 100:5.1f}%")
    print(
        f"  Smallest factor that covered every held-out sample: "
        f"{safety['needed_for_zero_under']}"
    )

    if args.json:
        args.json.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"\nWrote {args.json}")


if __name__ == "__main__":
    main()
