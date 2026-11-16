# syntax=docker/dockerfile:1

FROM debian:trixie-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    clang \
    pkg-config \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ARG TARGETARCH
ARG revision=unknown

RUN --mount=type=cache,id=cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=cargo-target-${TARGETARCH},target=/app/target \
    MIMIR_REVISION="${revision}" cargo build --release -p mimir-cmd \
 && mkdir -p /out/data \
 && cp /app/target/release/mimir /out/mimir \
 && strip --strip-debug /out/mimir

FROM gcr.io/distroless/cc-debian13

COPY --from=builder /out/mimir /bin/mimir
COPY --from=builder --chown=10001:10001 /out/data /data

USER 10001:10001
EXPOSE 8080
ENTRYPOINT [ "/bin/mimir" ]

ARG revision=unknown
LABEL org.opencontainers.image.title="mimir" \
      org.opencontainers.image.source="https://github.com/birdayz/mimir-rs" \
      org.opencontainers.image.revision="${revision}"
