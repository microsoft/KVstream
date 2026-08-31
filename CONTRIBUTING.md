# Contributing to KVStream

Thank you for your interest! This document explains how to contribute code,
tests, documentation, and new backend adapters.

This project welcomes contributions and suggestions. This project has adopted
the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any
additional questions or comments.

---

## Contributor License Agreement

Most contributions require you to agree to a Contributor License Agreement (CLA)
declaring that you have the right to, and actually do, grant us the rights to use
your contribution. For details, visit <https://cla.opensource.microsoft.com>.

When you submit a pull request, a CLA bot will automatically determine whether you
need to provide a CLA and decorate the PR appropriately (e.g., status check,
comment). Follow the instructions provided by the bot. You only need to do this
once across all repositories using our CLA.

---

## Development setup

```bash
git clone https://github.com/microsoft/kvstream
cd kvstream
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
ruff check src/kvstream tests
ruff format src/kvstream tests
mypy src/kvstream
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
- [ ] Lint passes: `ruff check src/kvstream tests` and `ruff format --check src/kvstream tests`
- [ ] New features have tests
- [ ] `README.md` updated if adding a feature or backend
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)

---

## Reporting issues

Use [GitHub Issues](https://github.com/microsoft/kvstream/issues).
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

---

## Trademarks

This project may contain trademarks or logos for projects, products, or services.
Authorized use of Microsoft trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not
cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks
or logos is subject to those third parties' policies.
