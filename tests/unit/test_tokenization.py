"""
Tests for token estimation accuracy and online self-calibration.

The estimator can never be exact without the model's tokenizer. What these tests
pin down is that it is (a) conservative in the right direction, (b) better than a
pure characters-per-token rule on punctuation-dense text, and (c) able to learn
the real ratio when the backend reports usage.
"""

from __future__ import annotations

from kvstream.models import ChatMessage
from kvstream.tokenization import (
    MAX_CHARS_PER_TOKEN,
    MAX_UNITS_PER_TOKEN,
    MIN_CHARS_PER_TOKEN,
    MIN_UNITS_PER_TOKEN,
    TokenEstimator,
    count_units,
    estimate_prompt_tokens,
    estimate_tokens,
    message_chars,
)


def test_empty_and_trivial():
    assert estimate_tokens("") == 0
    assert estimate_tokens("abcd", chars_per_token=4.0) == 1


def test_unit_count_catches_punctuation_dense_text():
    # A pure chars/4 rule badly under-counts code and markup.
    code = 'if (x==1) { return {"a": [1,2,3]}; }'
    chars_only = len(code) / 4
    assert count_units(code) > chars_only  # punctuation dominates
    # The estimator takes the larger of the two, so it does not under-count.
    assert estimate_tokens(code) >= count_units(code)


def test_long_words_use_the_character_rate():
    # One "unit" but many characters — the char rate must dominate here.
    word = "internationalization"  # 20 chars, 1 unit
    assert estimate_tokens(word, chars_per_token=4.0) == 5


def test_estimate_is_conservative_vs_naive_rule():
    text = "Hello, world! How are you today?"
    naive = max(1, round(len(text) / 4))
    assert estimate_tokens(text) >= naive


def test_prompt_tokens_include_per_message_overhead():
    msgs = [ChatMessage(role="user", content="abcdefgh")]  # 8 chars -> 2 + 4 overhead
    assert estimate_prompt_tokens(msgs, chars_per_token=4.0) == 6
    assert message_chars(msgs) == 8


def test_safety_factor_scales_estimates_up():
    msgs = [ChatMessage(role="user", content="hello world " * 10)]
    plain = TokenEstimator(4.0, safety_factor=1.0)
    padded = TokenEstimator(4.0, safety_factor=1.5)
    assert padded.estimate_messages(msgs) > plain.estimate_messages(msgs)


def test_observe_learns_the_real_ratio():
    est = TokenEstimator(4.0)
    assert not est.calibrated
    # Ground truth: 3000 chars really was 1000 tokens -> 3.0 chars/token.
    for _ in range(30):
        est.observe(chars=3000, units=800, actual_prompt_tokens=1000)
    assert est.calibrated
    assert est.samples == 30
    assert abs(est.chars_per_token - 3.0) < 0.05  # converged


def test_learning_makes_estimates_move_toward_truth():
    text = "x" * 3000
    est = TokenEstimator(4.0)
    before = est.estimate_text(text)  # ~750 at 4.0 chars/token
    for _ in range(30):
        est.observe(chars=3000, units=800, actual_prompt_tokens=1000)
    after = est.estimate_text(text)  # ~1000 at 3.0 chars/token
    assert after > before
    assert abs(after - 1000) < abs(before - 1000)


def test_observe_rejects_implausible_samples():
    est = TokenEstimator(4.0)
    est.observe(chars=0, units=0, actual_prompt_tokens=10)  # nothing to learn from
    est.observe(chars=100, units=20, actual_prompt_tokens=0)  # no tokens
    est.observe(chars=100, units=90, actual_prompt_tokens=1)  # 100 chars/token — absurd
    est.observe(chars=10, units=1, actual_prompt_tokens=100)  # 0.1 chars/token — absurd
    assert est.samples == 0
    assert est.chars_per_token == 4.0


def test_learning_can_be_disabled():
    est = TokenEstimator(4.0, learn=False)
    est.observe(chars=3000, units=800, actual_prompt_tokens=1000)
    assert est.samples == 0
    assert est.chars_per_token == 4.0


def test_ratio_is_clamped():
    assert TokenEstimator(0.01).chars_per_token == MIN_CHARS_PER_TOKEN
    assert TokenEstimator(1000.0).chars_per_token == MAX_CHARS_PER_TOKEN


def test_stats_shape():
    est = TokenEstimator(4.0)
    s = est.stats()
    assert s["chars_per_token"] == 4.0
    assert s["units_per_token"] == 1.0
    assert s["samples"] == 0
    assert s["calibrated"] is False


def test_learns_units_per_token_not_just_chars():
    """
    Structured text (JSON, code) is dominated by the unit term, so calibrating
    only the character rate leaves a large systematic over-estimate. Both ratios
    must adapt.
    """
    est = TokenEstimator(4.0, 1.0)
    # Ground truth: 1,700 units and 3,370 chars really were 1,000 tokens.
    for _ in range(30):
        est.observe(chars=3370, units=1700, actual_prompt_tokens=1000)
    assert abs(est.units_per_token - 1.7) < 0.05
    assert abs(est.chars_per_token - 3.37) < 0.05


def test_unit_learning_reduces_over_estimation_on_structured_text():
    json_like = '{"tool":"search","args":{"query":"foundry local","top_k":5}}'
    naive = TokenEstimator(4.0, 1.0)
    learned = TokenEstimator(4.0, 1.0)
    units = count_units(json_like)
    # Suppose the real tokenizer produced ~0.6 tokens per unit for this shape.
    truth = max(1, round(units / 1.7))
    for _ in range(30):
        learned.observe(chars=len(json_like), units=units, actual_prompt_tokens=truth)
    assert learned.estimate_text(json_like) < naive.estimate_text(json_like)


def test_units_ratio_is_clamped():
    assert TokenEstimator(4.0, 0.001).units_per_token == MIN_UNITS_PER_TOKEN
    assert TokenEstimator(4.0, 99.0).units_per_token == MAX_UNITS_PER_TOKEN
