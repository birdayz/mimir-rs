---
name: mimir-rs-wave-engineering
description: Engineering playbook for mimir-rs waves — how to plan and run parallel hardening, feature, conformance, and test-porting waves (including TL-of-TLs delegation), enforce 1:1 Go Mimir parity, manage git worktrees and subagents, use UPSTREAM_MIMIR, and pass the adversarial gatekeeper and 12-job CI merge gate. Load at the start of any multi-file change, wave, or hardening effort in mimir-rs.
---

# mimir-rs Wave Engineering Playbook

Use this playbook for all non-trivial feature, porting, conformance, and hardening work in `mimir-rs`.

## 1. Non-Negotiable Quality & Compatibility Bar
- **100% drop-in Go Mimir parity** against the pinned upstream commit in `UPSTREAM_MIMIR` (`.ref/mimir`, plus exact module versions in `.ref/mimir/go.mod`). Never guess, simplify, or "improve" external wire, disk, config, CLI, HTTP/gRPC, error, or Prometheus metric contracts.
- **Zero data loss/corruption, zero deadlocks/races, zero runtime `.unwrap()`/`.expect()`/`panic!()`** on fallible production paths, and **Miri-clean `unsafe`**.
- **Single source of truth for upstream version:** `UPSTREAM_MIMIR` at repo root defines `UPSTREAM_MIMIR_REPO`, `UPSTREAM_MIMIR_VERSION`, and `UPSTREAM_MIMIR_REF`. All workflows, scripts, `Dockerfile`, `justfile`, and tests must consume it via `./scripts/fetch-upstream-mimir.sh` and `./scripts/build-version.sh` (`v<upstream>-rs.<short_sha>`). Never hardcode commit SHAs or machine-local `/home/.../go/pkg/mod` paths; vendor required test fixtures under `crates/*/tests/testdata/`.
- **100% upstream test parity:** Maintain **0 missing, 0 `#[ignore]`** across both Mimir-go (`./scripts/verify-test-parity.sh`, 800/800) and all re-implemented dependencies (`./scripts/verify-test-parity.sh --deps --strict`, 2,020/2,020).

## 2. Git Worktree & Subagent Hygiene
- **Isolated worktrees only:** Perform all edits inside `.worktrees/<name>` branched off `origin/main` (with `ln -sf ../../.ref .worktrees/<name>/.ref`). Never dirty or reset the root checkout if it holds unrelated local work. Remove finished worktrees and local branches promptly after merge.
- **Model routing:**
  - **Parallel coding / porting / fixing workers:** `gemini-4-hydrogen-high` (`effort: 'high'`) in disjoint worktrees with **strict, zero-overlap file ownership**.
  - **Deep adversarial audits, pre-PR gatekeepers, and autonomous Wave Leads (TL of TLs):** `gemini-4-neon-high` (`effort: 'xhigh'`).
- **TL-of-TLs delegation:** For an entire vertical problem class spanning multiple crates (e.g., shutdown/restart/rolling updates, WAL/compaction durability, Kafka ingest storage), spawn a dedicated `neon` Wave Lead in its own worktree (`.worktrees/wN-<topic>`) to run all 4 phases below end-to-end (spawning and deleting its own `hydrogen`/`neon` subagents as needed) and deliver a verified commit ready for integration.
- **Agent & context hygiene:**
  - Agent names must be `<= 32` chars (`[a-z0-9_-]`). Before deleting a finished coding worker, ALWAYS verify its commit actually exists on disk (`git -C .worktrees/<t> log -1`), then delete it immediately (`agents.delete`).
  - Never `read.text` files `> 600` lines — use `rg -n` + `read(path, { offset, limit: 140 })`.
  - Split large test suites across files `<= 1,200` lines so edits/writes never truncate.
  - Always prefix `GH_PROMPT_DISABLED=1` on `gh` commands.

## 3. Standard 4-Phase Wave Process

### Phase 1 — Deep Audit & Spec Grounding (Read-Only)
- Before designing or fixing, inspect the exact Go reference in `.ref/mimir/` (and pinned Go deps when relevant) alongside Rust production code.
- For broad hardening waves, fan out parallel `neon` (`xhigh`) read-only auditors by disjoint subsystem (e.g., TSDB, Ingester+Proto, Ring+Cmd/Net, Storage+Config+Common).
- Every finding must cite exact Rust `file:line`, Go reference `file:line` (for parity), concrete failure mechanism/reproducer, and proposed fix — separated into **CRITICAL/HIGH**, **MEDIUM**, and **Differential Conformance Gaps**.

### Phase 2 — Disjoint Parallel Implementation
- Partition work into parallel tracks (`wN-t1..tK`) with **strict non-overlapping file ownership** so merges are clean.
- Every fix must ship with targeted regression tests proving the bug is fixed (and failing if reverted), preserving all `// Upstream: ...` markers.
- Apply our **Production Hardening Checklist** on every change:
  - Bound untrusted wire/disk allocations incrementally (never `vec![0u8; untrusted_varint]`, cap zstd `window_log_max`, bound nested decompression/expansion budgets, cap YAML alias expansion & nesting depth).
  - Never `mmap` a live-written/truncatable file (use positioned `pread` for active segments; `mmap` only sealed immutable files).
  - Never call `madvise(MADV_DONTNEED)` on allocator-managed (`jemalloc`/`glibc`) heap extents — use dedicated anonymous `mmap`.
  - Fail closed on security filters (`CIDRsAllowed`, TLS/mTLS CA reloads, path traversal `..`).
  - Use RAII drop guards for inflight counters, barriers, permits, and service tasks across `await`/cancellation/unwinding.
  - Always pin + `.enable()` `tokio::sync::Notify::notified()` before checking state (or prefer `tokio::sync::watch`).
  - Never hold non-reentrant locks across callbacks, `await`, or nested re-entrant checks.

### Phase 3 — Integration & 3-Tier Differential Conformance
- Merge track branches into `.worktrees/wN-int` off `origin/main`, delete finished track worktrees/branches.
- Verify **unit tests alone are never sufficient** — lock down wire/disk/behavioral changes against real upstream Go Mimir across all 3 tiers:
  1. **Tier 1 (Go Package Oracle):** Extend `tests/go-oracle/` (built via `./scripts/build-go-oracle.sh` with `-mod=vendor`) and assert bit-for-bit parity in `crates/mimir-conformance/`.
  2. **Tier 2 (Twin-Process Black-Box Harness):** Boot unmodified `target/go-mimir` and Rust `mimir` side-by-side and diff gRPC responses, `ErrorDetails`, HTTP admin endpoints, PVC handoff, and `/metrics`.
  3. **Tier 3 (Mixed Cluster / Testcontainers / k3d Helm e2e):** Verify mixed Go + Rust clusters over live memberlist/etcd/Kafka/object-store and rolling restarts.

### Phase 4 — Adversarial Gatekeeper & CI Merge Gate
1. **Local QA gate (before opening PR):**
   - `./scripts/fetch-upstream-mimir.sh --check`
   - `cargo clippy --workspace --all-targets --locked -- -D warnings` (0 warnings)
   - `cargo rustc -p <crate> --lib --profile test -- --cfg miri -D warnings` across `mimir-{common,proto,tsdb,ingester}`
   - `cargo test --workspace --exclude mimir-conformance` (0 failed, 0 ignored)
   - Relevant `mimir-conformance` oracle & twin-process tests (run flaky-prone concurrency/lifecycle tests multiple times!)
   - `./scripts/verify-test-parity.sh` (800/800) and `./scripts/verify-test-parity.sh --deps --strict` (2,020/2,020)
2. **Adversarial `neon` pre-PR gatekeeper:** Spawn a fresh `gemini-4-neon-high` (`xhigh`) reviewer to audit `origin/main..HEAD` for any blocking bugs, UB, deadlocks, DoS vectors, Go divergences, or flaky/stub tests. Fix all blocking findings and re-verify before merging.
3. **PR & Post-Merge CI Gate:**
   - Open PR against `main`; require **all 12 PR CI jobs** green (`CI`: `clippy`, `unit & integration tests`, `miri`, `upstream test parity`, `conformance (oracle)`, `conformance (twin-process)`, `conformance (docker)`, `conformance (docker-k8s)`; `k3d Helm e2e`: `build mimir-rs image`, `MODE=mixed/memberlist`, `MODE=rust/memberlist`, `MODE=rust/etcd`).
   - Treat every CI failure as a real signal — diagnose root cause from job logs, fix cleanly, never blindly re-run real races/bugs.
   - Merge via `GH_PROMPT_DISABLED=1 gh pr merge <num> --merge --delete-branch`, clean up worktree & branch, and verify post-merge `main` workflows (`CI`, `k3d Helm e2e`, `Publish Multi-Arch Image to GHCR`) all finish green.
