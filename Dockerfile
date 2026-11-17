# syntax=docker/dockerfile:1

FROM docker.io/library/rust:1.100.0-trixie@sha256:e19840879045c951c905d0eaa9887f3439c62000e429963371b376bac8d38c31 AS builder

ARG SOURCE_DATE_EPOCH=0
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
    ZERO_AR_DATE=1 \
    TZ=UTC \
    LC_ALL=C \
    LANG=C \
    CARGO_INCREMENTAL=0 \
    CFLAGS="-ffile-prefix-map=/app=. -ffile-prefix-map=/usr/local/cargo=/cargo -g0" \
    CXXFLAGS="-ffile-prefix-map=/app=. -ffile-prefix-map=/usr/local/cargo=/cargo -g0" \
    RUSTFLAGS="--remap-path-prefix=/app=. --remap-path-prefix=/usr/local/cargo=/cargo -C link-arg=-Wl,--build-id=sha1"

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ARG revision=unknown
# Optional cap on cargo parallelism (e.g. --build-arg BUILD_JOBS=6); unset = cargo default.
ARG BUILD_JOBS

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    MIMIR_REVISION="${revision}" cargo build --locked --release -p mimir-cmd ${BUILD_JOBS:+--jobs ${BUILD_JOBS}} \
      --config 'profile.release.lto="fat"' \
      --config 'profile.release.codegen-units=1' \
      --config 'profile.release.debug=false' \
      --config 'profile.release.strip="symbols"' \
 && install -D -m 0755 /app/target/release/mimir /out/mimir \
 && touch -h -d "@0" /out /out/mimir

FROM gcr.io/distroless/cc-debian13@sha256:809d04afa1be033c441f76bb0051878c342fb76d0ea236bc6dc94b655fa0fbd6

COPY --from=builder /out/mimir /bin/mimir

EXPOSE 8080
ENTRYPOINT [ "/bin/mimir" ]

ARG revision=unknown
LABEL org.opencontainers.image.title="mimir" \
      org.opencontainers.image.source="https://github.com/birdayz/mimir-rs" \
      org.opencontainers.image.revision="${revision}"
