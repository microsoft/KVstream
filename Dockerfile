# KVStream — sidecar image.
#
# A container cannot see the host's listening ports, so ephemeral-port discovery
# and the `foundry` CLI lookup are both useless here. An explicit backend URL is
# the supported path (proposal §8.3), and both fallbacks are disabled below so
# the image fails loudly rather than scanning its own empty network namespace.
FROM python:3.12-slim AS build

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir --upgrade pip build \
    && python -m build --wheel --outdir /dist

FROM python:3.12-slim

# Run unprivileged: the gateway needs nothing but a socket.
RUN useradd --create-home --uid 10001 kvstream
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl

USER kvstream
WORKDIR /home/kvstream

# Bind to all interfaces *inside the container* — the network namespace is the
# isolation boundary here, not the loopback address. KVStream still has no
# authentication, so publish this port only where that is acceptable.
ENV KVSTREAM_HOST=0.0.0.0 \
    KVSTREAM_PORT=8080 \
    KVSTREAM_BACKEND__DISCOVER=false \
    KVSTREAM_BACKEND__USE_FOUNDRY_CLI=never

EXPOSE 8080

# /health returns 503 when the backend is not serving, so this is a real check.
# It can also run a real 1-token generation (bounded by
# KVSTREAM_BACKEND__READINESS_TIMEOUT_SECONDS, 15 s by default) after a 5 s
# liveness check, so the client waits for that plus margin. A 4 s limit marked a
# healthy gateway unhealthy whenever the probe was slow — measured at 6-12.5 s on
# a cold model. Docker's own --timeout is the outer bound and must stay above it.
HEALTHCHECK --interval=30s --timeout=60s --start-period=30s --retries=3 \
    CMD python -c "import os,sys,urllib.request; t=float(os.environ.get('KVSTREAM_BACKEND__READINESS_TIMEOUT_SECONDS','15'))+10; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=t).status == 200 else 1)"

ENTRYPOINT ["kvstream", "serve"]
