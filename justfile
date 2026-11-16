default:
    @just --list

check:
    cargo check --workspace --all-targets

lint:
    cargo clippy --workspace --all-targets -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

build:
    cargo build --workspace

build-release:
    cargo build --release -p mimir-cmd

build-release-arm64:
    cargo build --release --target aarch64-unknown-linux-gnu -p mimir-cmd -p mimir-conformance --examples

go-oracle *args:
    ./scripts/build-go-oracle.sh {{args}}

test *args:
    cargo test --workspace {{args}}

test-crate crate *args:
    cargo test -p {{crate}} {{args}}

conformance *args:
    ./scripts/build-go-oracle.sh && cargo test -p mimir-conformance {{args}}

parity filter="":
    ./scripts/verify-test-parity.sh {{filter}}

bench *args:
    cargo bench --workspace {{args}}

qa:
    just go-oracle
    just check
    just lint
    just parity
    just test

bench-vs-go *args:
    just build-release
    just go-oracle
    cargo run --release -p mimir-conformance --example bench_vs_go -- {{args}}

prod-bench *args:
    just build-release
    just go-oracle
    cargo run --release -p mimir-conformance --example prod_bench -- {{args}}

docker-build tag="mimir-rs:local":
    docker build --build-arg revision=$(git rev-parse HEAD) -t {{tag}} .
