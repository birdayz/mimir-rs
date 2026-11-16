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

conformance-kafka-tls *args:
    ./scripts/build-go-oracle.sh && cargo test -p mimir-conformance --test w6_kafka_tls_sasl_testcontainers {{args}} -- --nocapture --test-threads 3

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
    docker buildx build \
        --provenance=false \
        --sbom=false \
        --build-arg SOURCE_DATE_EPOCH=0 \
        --build-arg revision=$(git rev-parse HEAD) \
        --build-arg BUILD_JOBS=${CARGO_BUILD_JOBS:-} \
        --load \
        -t {{tag}} \
        .

# k3d + upstream mimir-distributed Helm chart e2e (see docs/k3d-helm-e2e.md).
# MODE=go|mixed|rust, RUST_IMAGE=..., KEEP_CLUSTER=1
k3d-e2e mode="go":
    MODE={{mode}} ./scripts/k3d-helm-e2e.sh

docker-build-repro out="target/mimir-rs-oci.tar":
    mkdir -p "$(dirname '{{out}}')"
    docker buildx build \
        --no-cache \
        --provenance=false \
        --sbom=false \
        --build-arg SOURCE_DATE_EPOCH=0 \
        --build-arg revision=$(git rev-parse HEAD) \
        --output "type=oci,dest={{out}},rewrite-timestamp=true" \
        .
    sha256sum "{{out}}"
