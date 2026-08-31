"""
Token estimation for admission cost.

KVStream is a proxy and does not ship the model's tokenizer, so prompt sizes are
**estimated**. Two mechanisms keep the estimate both honest and useful:

1. **A conservative heuristic.** Text is measured two ways — a character rate and
   a count of word/punctuation units — and the *larger* is used. Pure
   characters-per-token under-counts punctuation-dense text (code, JSON, chat
   markup); the unit count under-counts long words. Taking the max errs toward
   over-estimation, which is the safe direction for admission: it admits fewer
   requests rather than overloading the runtime.

2. **Online self-calibration.** OpenAI-compatible responses may include a
   ``usage`` object with the real ``prompt_tokens``. Whenever Foundry Local
   reports one, :meth:`TokenEstimator.observe` learns the true
   characters-per-token ratio for that model and adapts (EWMA, clamped to a sane
   range). Estimates converge on the model's actual tokenizer behaviour without
   shipping a tokenizer.

If the backend never reports usage, the heuristic stands on its own and remains
an approximation — that limitation is documented, not hidden.
"""

from __future__ import annotations

import re
from math import ceil

from kvstream.models import ChatMessage

# Words and standalone punctuation both tend to map to at least one token.
_UNIT_RE = re.compile(r"\w+|[^\w\s]")

# Per-message allowance for role/formatting tokens in chat templates.
MESSAGE_OVERHEAD_TOKENS = 4

# Bounds for the learned ratios. Real tokenizers sit roughly between 2 chars/token
# (dense code/CJK) and 8 (prose with long words); word/punctuation units run about
# 0.4-2.0 per token. Clamping stops one bad sample destabilising admission.
MIN_CHARS_PER_TOKEN = 1.5
MAX_CHARS_PER_TOKEN = 8.0
MIN_UNITS_PER_TOKEN = 0.3
MAX_UNITS_PER_TOKEN = 3.0

DEFAULT_CHARS_PER_TOKEN = 4.0
DEFAULT_UNITS_PER_TOKEN = 1.0


def count_units(text: str) -> int:
    """Count word and punctuation units — a structural signal of token count."""
    return len(_UNIT_RE.findall(text))


def estimate_tokens(
    text: str,
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
    units_per_token: float = DEFAULT_UNITS_PER_TOKEN,
) -> int:
    """
    Estimate tokens in ``text`` using the conservative max-of-two heuristic.

    Approximate by design; see the module docstring.
    """
    if not text:
        return 0
    char_estimate = len(text) / chars_per_token
    unit_estimate = count_units(text) / units_per_token
    return max(1, ceil(max(char_estimate, unit_estimate)))


def estimate_prompt_tokens(
    messages: list[ChatMessage],
    chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
    units_per_token: float = DEFAULT_UNITS_PER_TOKEN,
) -> int:
    """Estimate prompt tokens across a chat message list (content + role overhead)."""
    total = 0
    for m in messages:
        total += (
            estimate_tokens(m.billable_text(), chars_per_token, units_per_token)
            + MESSAGE_OVERHEAD_TOKENS
        )
    return max(1, total)


def message_chars(messages: list[ChatMessage]) -> int:
    """Total characters across message contents (a basis for calibration)."""
    return sum(len(m.billable_text()) for m in messages)


def message_units(messages: list[ChatMessage]) -> int:
    """Total word/punctuation units across message contents (a basis for calibration)."""
    return sum(count_units(m.billable_text()) for m in messages)


class TokenEstimator:
    """
    A self-calibrating token estimator.

    Starts from a configured characters-per-token ratio and refines it from real
    ``usage.prompt_tokens`` reported by the backend. Thread-safety is not needed:
    it is only mutated from the event loop.
    """

    def __init__(
        self,
        chars_per_token: float = DEFAULT_CHARS_PER_TOKEN,
        units_per_token: float = DEFAULT_UNITS_PER_TOKEN,
        *,
        safety_factor: float = 1.0,
        learn: bool = True,
        alpha: float = 0.2,
    ) -> None:
        self._cpt = _clamp_chars(chars_per_token)
        self._upt = _clamp_units(units_per_token)
        self._initial_cpt = self._cpt
        self._safety = max(1.0, safety_factor)
        self._learn = learn
        self._alpha = alpha
        self._samples = 0

    # -- estimation ----------------------------------------------------

    @property
    def chars_per_token(self) -> float:
        return self._cpt

    @property
    def units_per_token(self) -> float:
        return self._upt

    @property
    def samples(self) -> int:
        return self._samples

    @property
    def calibrated(self) -> bool:
        """True once at least one real usage report has been observed."""
        return self._samples > 0

    def estimate_text(self, text: str) -> int:
        return max(0, ceil(estimate_tokens(text, self._cpt, self._upt) * self._safety))

    def estimate_messages(self, messages: list[ChatMessage]) -> int:
        base = estimate_prompt_tokens(messages, self._cpt, self._upt)
        return max(1, ceil(base * self._safety))

    # -- calibration ---------------------------------------------------

    def observe(self, chars: int, units: int, actual_prompt_tokens: int) -> None:
        """
        Learn both ratios from a real ``usage.prompt_tokens`` report.

        Learning *both* the character rate and the unit rate matters: for
        structured text (JSON, code) the unit term dominates the estimate, so
        calibrating only the character rate leaves a large systematic bias.

        Implausible samples (non-positive counts, or a ratio outside the clamp
        range) are ignored so a malformed response cannot destabilise admission.
        """
        if not self._learn or actual_prompt_tokens <= 0:
            return
        updated = False
        if chars > 0:
            obs_cpt = chars / actual_prompt_tokens
            if MIN_CHARS_PER_TOKEN <= obs_cpt <= MAX_CHARS_PER_TOKEN:
                self._cpt = _clamp_chars((1 - self._alpha) * self._cpt + self._alpha * obs_cpt)
                updated = True
        if units > 0:
            obs_upt = units / actual_prompt_tokens
            if MIN_UNITS_PER_TOKEN <= obs_upt <= MAX_UNITS_PER_TOKEN:
                self._upt = _clamp_units((1 - self._alpha) * self._upt + self._alpha * obs_upt)
                updated = True
        if updated:
            self._samples += 1

    def stats(self) -> dict:
        return {
            "chars_per_token": round(self._cpt, 3),
            "units_per_token": round(self._upt, 3),
            "initial_chars_per_token": round(self._initial_cpt, 3),
            "safety_factor": self._safety,
            "samples": self._samples,
            "calibrated": self.calibrated,
        }


def _clamp_chars(value: float) -> float:
    return min(MAX_CHARS_PER_TOKEN, max(MIN_CHARS_PER_TOKEN, value))


def _clamp_units(value: float) -> float:
    return min(MAX_UNITS_PER_TOKEN, max(MIN_UNITS_PER_TOKEN, value))
