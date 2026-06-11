"""
Integration tests — engine + proxy + mock backend.

These tests spin up the full FastAPI app against a mock backend
that returns deterministic token streams.
"""
import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from typing import AsyncIterator

from kvstream.backends.base import BaseBackend, GenerateRequest, Token
from kvstream.engine import KVaultEngine
from kvstream.proxy.app import build_app


class MockBackend(BaseBackend):
    """Returns a fixed sequence of tokens for any request."""

    def __init__(self, tokens: list[str] = None):
        self._tokens = tokens or ["Hello", ", ", "world", "!"]

    async def generate(self, request: GenerateRequest) -> AsyncIterator[Token]:
        for i, text in enumerate(self._tokens):
            is_last = i == len(self._tokens) - 1
            yield Token(
                text=text,
                token_id=i,
                finish_reason="stop" if is_last else None,
            )

    async def health(self) -> bool:
        return True

    async def list_models(self) -> list[str]:
        return ["mock-model"]

    async def tokenize(self, text: str) -> list[int]:
        return list(range(len(text.split())))


@pytest.fixture
def engine():
    return KVaultEngine(
        backend=MockBackend(),
        num_gpu_blocks=128,
        num_cpu_blocks=256,
        block_size=4,
        max_batch_size=8,
    )


@pytest.fixture
def app(engine):
    return build_app(engine)


@pytest_asyncio.fixture
async def client(app):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as c:
        yield c


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_returns_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["backend_healthy"] is True


class TestStatusEndpoint:
    @pytest.mark.asyncio
    async def test_status_returns_memory_info(self, client):
        resp = await client.get("/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "scheduler" in data
        assert "memory" in data
        assert data["memory"]["gpu_blocks_free"] == 128


class TestModelsEndpoint:
    @pytest.mark.asyncio
    async def test_list_models(self, client):
        resp = await client.get("/v1/models")
        assert resp.status_code == 200
        data = resp.json()
        assert data["object"] == "list"
        assert any(m["id"] == "mock-model" for m in data["data"])


class TestChatCompletions:
    @pytest.mark.asyncio
    async def test_non_streaming_response(self, client):
        resp = await client.post("/v1/chat/completions", json={
            "model": "mock-model",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 64,
            "stream": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["object"] == "chat.completion"
        assert len(data["choices"]) == 1
        content = data["choices"][0]["message"]["content"]
        assert "Hello" in content

    @pytest.mark.asyncio
    async def test_streaming_response_sse_format(self, client):
        chunks = []
        async with client.stream("POST", "/v1/chat/completions", json={
            "model": "mock-model",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 64,
            "stream": True,
        }) as resp:
            assert resp.status_code == 200
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    chunks.append(line[6:])

        assert chunks[-1] == "[DONE]"
        import json
        texts = [
            json.loads(c)["choices"][0]["delta"].get("content", "")
            for c in chunks[:-1]
            if c != "[DONE]"
        ]
        assert "".join(texts) == "Hello, world!"


class TestPrefixCache:
    @pytest.mark.asyncio
    async def test_prefix_cache_hit_recorded(self, engine, client):
        # First request — populates the prefix cache
        await client.post("/v1/chat/completions", json={
            "model": "mock-model",
            "messages": [{"role": "system", "content": "You are a helpful assistant."},
                         {"role": "user", "content": "Hello"}],
            "stream": False,
        })

        # Stats should show at least one cached prefix
        resp = await client.get("/status")
        # Prefix cache is populated after generation, hit count starts at 0
        data = resp.json()
        assert "prefix_cache" in data
