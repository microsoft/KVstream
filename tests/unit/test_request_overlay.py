"""
G-40…G-44: the typed overlay must not narrow what a client can send.

KVStream validates only what it needs to cost and stream a request. Everything
else rides along untouched — because an allow-list drops unknown fields
*quietly*, producing a plausible wrong answer instead of an error.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kvstream.models import ChatCompletionRequest, ChatMessage, backend_payload
from kvstream.tokenization import estimate_prompt_tokens, message_chars

TOOL_CALL = {
    "id": "call_1",
    "type": "function",
    "function": {"name": "get_weather", "arguments": '{"city":"Lisbon"}'},
}


# -- message shapes an agent loop actually produces --------------------


def test_assistant_tool_call_message_has_null_content():
    """The second turn of every tool-calling agent looks exactly like this."""
    m = ChatMessage(role="assistant", content=None, tool_calls=[TOOL_CALL])
    assert m.content is None
    assert m.tool_calls == [TOOL_CALL]


def test_tool_result_message():
    m = ChatMessage(role="tool", tool_call_id="call_1", content="22C and sunny")
    assert m.tool_call_id == "call_1"


def test_multimodal_content_parts():
    m = ChatMessage(
        role="user",
        content=[
            {"type": "text", "text": "what is this"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
        ],
    )
    # Only the text part is billable; an image has no token cost KVStream can
    # measure, and guessing one would be inventing a number.
    assert m.billable_text() == "what is this"


def test_unknown_message_fields_survive():
    m = ChatMessage(
        role="user", content="hi", **{"refusal": None, "audio": {"id": "x"}}
    )
    dumped = m.model_dump()
    assert dumped["audio"] == {"id": "x"}


def test_tool_calls_are_billed_as_prompt_tokens():
    """Re-sent transcripts carry tool calls; they are real tokens next turn."""
    plain = [ChatMessage(role="assistant", content=None)]
    with_call = [ChatMessage(role="assistant", content=None, tool_calls=[TOOL_CALL])]
    assert message_chars(with_call) > message_chars(plain)
    assert estimate_prompt_tokens(with_call) > estimate_prompt_tokens(plain)


def test_null_content_costs_nothing_but_does_not_crash():
    assert ChatMessage(role="assistant", content=None).billable_text() == ""
    assert estimate_prompt_tokens([ChatMessage(role="assistant", content=None)]) >= 1


# -- the request overlay ------------------------------------------------


def test_tools_and_other_fields_are_preserved():
    raw = {
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "tools": [{"type": "function", "function": {"name": "get_weather"}}],
        "tool_choice": "auto",
        "response_format": {"type": "json_object"},
        "seed": 42,
        "frequency_penalty": 0.5,
        "presence_penalty": 0.1,
        "logprobs": True,
        "top_logprobs": 3,
        "logit_bias": {"1234": -100},
        "user": "agent-7",
    }
    req = ChatCompletionRequest(**raw)
    payload = backend_payload(raw, stream=False)
    for field in raw:
        assert field in payload, f"{field} was dropped"
    assert payload["seed"] == 42
    assert req.model == "m"


def test_backend_payload_sets_the_path_actually_taken():
    raw = {"model": "m", "messages": [], "stream": True}
    assert backend_payload(raw, stream=False)["stream"] is False
    assert backend_payload(raw, stream=True)["stream"] is True


def test_backend_payload_drops_stream_options_when_not_streaming():
    raw = {"model": "m", "messages": [], "stream_options": {"include_usage": True}}
    assert "stream_options" not in backend_payload(raw, stream=False)
    assert "stream_options" in backend_payload(raw, stream=True)


def test_backend_payload_drops_an_empty_stop():
    """Foundry Local answers HTTP 400 for one."""
    assert "stop" not in backend_payload(
        {"model": "m", "messages": [], "stop": []}, stream=False
    )
    assert backend_payload({"model": "m", "messages": [], "stop": "END"}, stream=False)[
        "stop"
    ]


def test_backend_payload_does_not_mutate_the_client_object():
    raw = {"model": "m", "messages": [], "stream": False}
    backend_payload(raw, stream=True)
    assert raw["stream"] is False


def test_stop_accepts_a_bare_string():
    req = ChatCompletionRequest(model="m", messages=[], stop="END")
    assert req.stop == "END"


# -- costing ------------------------------------------------------------


def test_absent_max_tokens_uses_the_configured_default_for_costing_only():
    req = ChatCompletionRequest(model="m", messages=[])
    assert req.max_tokens is None  # nothing to send upstream
    assert req.generation_budget(512) == 512  # but it still has to be costed


def test_max_completion_tokens_is_honoured():
    req = ChatCompletionRequest(model="m", messages=[], max_completion_tokens=64)
    assert req.generation_budget(512) == 64


def test_n_multiplies_the_generation_budget():
    """Four completions of 512 tokens really can cost four times the KV."""
    req = ChatCompletionRequest(model="m", messages=[], max_tokens=512, n=4)
    assert req.generation_budget(999) == 2048


def test_n_greater_than_one_is_allowed():
    assert ChatCompletionRequest(model="m", messages=[], n=8).n == 8


def test_no_arbitrary_upper_bound_on_max_tokens():
    """A long-context model must not be rejected by the gateway's own ceiling."""
    assert (
        ChatCompletionRequest(model="m", messages=[], max_tokens=200_000).max_tokens
        == 200_000
    )


@pytest.mark.parametrize("bad", [0, -1])
def test_max_tokens_must_still_be_positive(bad):
    with pytest.raises(ValidationError):
        ChatCompletionRequest(model="m", messages=[], max_tokens=bad)


# -- determinism gate ---------------------------------------------------


def test_absent_temperature_is_not_deterministic():
    """Unset means the backend's default (1.0), not zero."""
    assert ChatCompletionRequest(model="m", messages=[]).deterministic is False


def test_explicit_zero_is_deterministic():
    assert (
        ChatCompletionRequest(model="m", messages=[], temperature=0.0).deterministic
        is True
    )


def test_multi_choice_is_never_deterministic():
    req = ChatCompletionRequest(model="m", messages=[], temperature=0.0, n=2)
    assert req.deterministic is False


def test_wants_usage_reflects_the_clients_own_request():
    assert ChatCompletionRequest(model="m", messages=[]).wants_usage is False
    assert (
        ChatCompletionRequest(
            model="m", messages=[], stream_options={"include_usage": True}
        ).wants_usage
        is True
    )
