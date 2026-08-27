"""Unit tests for the response cache, calibration knee, and tokenization."""

from __future__ import annotations

from kvstream.admission.calibration import SweepPoint, find_knee
from kvstream.cache.response_cache import CachedResponse, ResponseCache, request_key
from kvstream.models import ChatCompletionRequest, ChatMessage
from kvstream.tokenization import estimate_prompt_tokens, estimate_tokens


def _resp(text: str) -> CachedResponse:
    """A cached non-streamed response carrying the backend's real body."""
    return CachedResponse(
        body={"choices": [{"message": {"role": "assistant", "content": text}}]},
        prompt_tokens=1,
        completion_tokens=1,
        finish_reason="stop",
    )


def _text(r: CachedResponse) -> str:
    return r.body["choices"][0]["message"]["content"]


def _raw(content: str, temperature: float = 0.0) -> dict:
    return {
        "model": "m",
        "messages": [{"role": "user", "content": content}],
        "temperature": temperature,
    }


def _req(content: str, temperature: float = 0.0) -> ChatCompletionRequest:
    return ChatCompletionRequest(**_raw(content, temperature))


def test_cache_get_put_and_lru_eviction():
    c = ResponseCache(ttl_seconds=100, max_entries=2)
    c.put("a", _resp("A"))
    c.put("b", _resp("B"))
    assert _text(c.get("a")) == "A"  # touches 'a' (most-recent)
    c.put("c", _resp("C"))  # evicts LRU, which is now 'b'
    assert c.get("b") is None
    assert _text(c.get("a")) == "A"
    assert _text(c.get("c")) == "C"


def test_cache_miss_counts():
    c = ResponseCache(ttl_seconds=100, max_entries=8)
    assert c.get("nope") is None
    assert c.stats()["misses"] == 1


def test_request_key_is_stable_and_sensitive():
    assert request_key(_req("hi"), _raw("hi")) == request_key(_req("hi"), _raw("hi"))
    assert request_key(_req("hi"), _raw("hi")) != request_key(_req("HI"), _raw("HI"))


def test_request_key_covers_fields_the_overlay_does_not_name():
    """A `tools` payload must not collide with the same messages without it."""
    plain = _raw("hi")
    with_tools = {**plain, "tools": [{"type": "function", "function": {"name": "f"}}]}
    assert request_key(_req("hi"), plain) != request_key(
        ChatCompletionRequest(**with_tools), with_tools
    )


def test_request_key_separates_streamed_and_non_streamed():
    """Entries are replayed only in the shape they were recorded in."""
    plain = _raw("hi")
    streamed = {**plain, "stream": True}
    assert request_key(_req("hi"), plain) != request_key(
        ChatCompletionRequest(**streamed), streamed
    )


def test_unreplayable_responses_are_not_cached():
    c = ResponseCache(ttl_seconds=100, max_entries=8)
    c.put("empty", CachedResponse())
    assert c.get("empty") is None


def test_estimate_tokens():
    assert estimate_tokens("") == 0
    assert estimate_tokens("abcd", chars_per_token=4.0) == 1
    msgs = [ChatMessage(role="user", content="abcdefgh")]  # 8 chars ≈ 2 + 4 overhead
    assert estimate_prompt_tokens(msgs, chars_per_token=4.0) == 6


def test_find_knee_on_latency_inflection():
    pts = [
        SweepPoint(1, 300, 0.10, 0),
        SweepPoint(2, 600, 0.12, 0),
        SweepPoint(4, 1200, 0.50, 0),  # 0.50 > 0.10 * 2 → knee here
    ]
    knee = find_knee(pts, latency_ratio=2.0)
    assert knee is not None and knee.concurrency == 2


def test_find_knee_on_first_error():
    pts = [SweepPoint(1, 300, 0.1, 0), SweepPoint(2, 600, 0.1, 3)]
    knee = find_knee(pts)
    assert knee is not None and knee.concurrency == 1


def test_find_knee_when_first_point_fails():
    assert find_knee([SweepPoint(1, 300, 0.1, 5)]) is None
    assert find_knee([]) is None


def test_deterministic_flag():
    assert _req("x", 0.0).deterministic
    assert not _req("x", 0.8).deterministic
