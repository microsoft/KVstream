# Contributing to KVStream

Thank you for your interest! This document explains how to contribute code,
tests, documentation, and new backend adapters.

---

## Development setup

```bash
git clone https://github.com/your-org/KVStream
cd KVStream
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Run the tests:

```bash
pytest tests/unit/          # fast, no GPU, no network
pytest tests/integration/   # requires Python only, no real backend
```

Lint and format:

```bash
ruff check kvstream/
ruff format kvstream/
mypy kvstream/
```

---

## Project layout

```
kvstream/
├── backends/       ← Runtime adapters (Ollama, Foundry, llama.cpp, LM Studio)
├── memory/         ← BlockManager, PagedKVCache, PrefixKVCache
├── scheduler/      ← ContinuousBatchScheduler
├── proxy/          ← FastAPI OpenAI-compatible server
├── cli/            ← Typer CLI (serve, bench, status, health)
├── engine.py       ← Top-level orchestrator
└── config.py       ← Pydantic settings
```

---

## Adding a new backend

1. Create `kvstream/backends/my_backend.py`
2. Subclass `BaseBackend` from `kvstream.backends.base`
3. Implement `generate()` and `health()` (required)
4. Optionally implement `tokenize()`, `save_kv_state()`, `restore_kv_state()`
5. Add it to `kvstream/backends/__init__.py`
6. Add a CLI option in `kvstream/cli/main.py`
7. Write tests in `tests/unit/test_my_backend.py` using a `MockBackend` pattern
8. Add a row to the **Supported Backends** table in `README.md`

See `kvstream/backends/ollama.py` as a reference for streaming backends and
`kvstream/backends/llamacpp.py` for hard KV inject.

---

## Pull request checklist

- [ ] Tests pass: `pytest tests/`
- [ ] Lint passes: `ruff check kvstream/` and `ruff format --check kvstream/`
- [ ] New features have tests
- [ ] `README.md` updated if adding a feature or backend
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)

---

## Reporting issues

Use [GitHub Issues](https://github.com/your-org/KVStream/issues).
Include:
- OS, GPU, VRAM
- Runtime backend and version (e.g. Ollama 0.3.x)
- `kvstream --version` output
- The full error traceback
- Steps to reproduce

---

## Code style

- Python 3.10+, type hints everywhere
- `ruff` for formatting (line length 100)
- Docstrings on public classes and functions
- No print statements — use `logging.getLogger(__name__)`

---

## License

By contributing you agree that your contributions will be licensed under
the Apache 2.0 License.
