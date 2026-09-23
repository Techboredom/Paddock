# syntax=docker/dockerfile:1

# ---- build -----------------------------------------------------------------
FROM rust:slim-bookworm AS builder

WORKDIR /app

# Manifests first so the dependency layer is cached across code-only changes.
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Cache the cargo registry and build artifacts across builds.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo \
    --mount=type=cache,id=cargo-target,target=/app/target \
    cargo build --release

# ---- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/paddock /usr/local/bin/paddock

# OLLAMA_URL must point at the Ollama host as seen *from this container*:
#   - separate docker network:  http://<ollama-host>:11434
#   - macOS/Windows:            http://host.docker.internal:11434
#   - --network host (Linux):   http://127.0.0.1:11434
ENV OLLAMA_URL=http://127.0.0.1:11434 \
    PADDOK_LISTEN=0.0.0.0:8080

EXPOSE 8080

USER nobody

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health > /dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/paddock"]
