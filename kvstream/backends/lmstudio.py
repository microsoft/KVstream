"""
LMStudioBackend — adapter for LM Studio (http://localhost:1234).
LM Studio exposes an OpenAI-compatible API; implementation mirrors FoundryBackend.
"""

from kvstream.backends.foundry import FoundryBackend


class LMStudioBackend(FoundryBackend):
    """LM Studio uses the same OpenAI-compat API as Foundry, just on port 1234."""

    def __init__(
        self,
        base_url: str = "http://localhost:1234",
        model: str = "local-model",
        timeout: float = 120.0,
    ) -> None:
        super().__init__(base_url=base_url, model=model, timeout=timeout)
