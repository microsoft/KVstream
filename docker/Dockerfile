# KVStream proxy image
# Multi-stage build: builder installs deps, runtime is lean

# ---- Builder ----
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps for torch + httpx
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE ./
COPY kvstream/ ./kvstream/

# Install into a prefix we can copy across
RUN pip install --no-cache-dir --prefix=/install ".[flash]" || \
    pip install --no-cache-dir --prefix=/install "."

# ---- Runtime ----
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="KVStream"
LABEL org.opencontainers.image.description="PagedAttention proxy for on-premise LLM inference"
LABEL org.opencontainers.image.source="https://github.com/microsoft/kvstream"

WORKDIR /app

# Runtime system deps only
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY --from=builder /build/kvstream ./kvstream
COPY --from=builder /build/pyproject.toml .

# Non-root user
RUN useradd -m -u 1000 kvstream
USER kvstream

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["python", "-m", "kvstream", "serve"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
