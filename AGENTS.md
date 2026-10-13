# Repository Instructions

## Before changing code

- Read `docs/architecture.md` before non-trivial work; its boundaries are load-bearing.
- Check `git status` first. Preserve unrelated changes and never commit generated artifacts (`target/`, toolchain `work/` or `out/`, or `benchmark-results/`).

## Architecture invariants

- Every crate declares a trust ring in `Cargo.toml`; enforcement lives in `crates/sinomo-core/tests/workspace_ring_boundaries.rs`.
- R0 crates depend only on R0. R1 code crosses R0 through `sinomo_core::...`. R2 and application code crosses R0/R1 through `sinomo_runtime::...`. ABI exceptions are documented in the boundary test.
- Keep `sinomo-network` and `sinomo-kv` independent; only `sinomo-cluster` bridges them.
- Normal DFS operations use existing configuration/network projections. Dylint
  rejects implicit KV access; genuine global consistency and configuration reads
  use narrow, documented boundary functions. Do not duplicate consensus or
  connectivity logic to avoid the lint. See `lints/sinomo-architecture/README.md`.
- Events are the cross-service contract. Use typed, dotted event names and include provenance, evidence, causality, and routing metadata as applicable.
- Do not bypass Sinomo networking, storage, policy, or authority paths. Distributed settings belong in cluster KV; node-local consent stays local and fail-closed. Treat sandbox filesystems as VFS/SIFS-backed.

## Workspace conventions

- Crates live under `crates/`. `crates/sinomo` is only the binary wrapper; CLI behavior belongs in `crates/sinomo-cli/src/lib.rs`. `crates/sinomo-app` is the GUI.
- Use workspace dependencies (`dependency.workspace = true`) instead of crate-local version pins.
- State paths are owned by `sinomo_local::SinomoDataDir` (`SINOMO_DATA_DIR`, default `~/.sinomo`). Without `--repo`, `sinomo evolve propose` targets its managed source checkout, not the operator checkout.
- Prefer integration tests for cross-crate networking, KV, DFS, and sandbox behavior. Use real verifier scripts for WASIX/toolchain behavior.

## Common checks

```bash
cargo build -p sinomo
cargo run -p sinomo -- <args>
cargo test -p <crate> <test-name>
cargo test -p sinomo-core --test workspace_ring_boundaries
bash scripts/ci/check-dfs-kv-boundary.sh
cargo check -p sinomo-sandbox-wasix-module --features wasix-runtime
cargo fmt --all
cargo clippy --workspace --all-targets
```

Run the smallest relevant checks while iterating, then broaden validation in proportion to the change.

## Performance analysis and reporting

For DFS or cluster performance work, read and apply
[sinomo-performance-analysis](skills/sinomo-performance-analysis/SKILL.md).
Use its standard acceptance, per-node stage/I/O, node-window and coverage tables
for every DFS performance report. Docker gates run with debugging, tracing and
profiling enabled at unchanged thresholds. Use measured timings directly without
subtracting observation overhead, and report unavailable measurements explicitly.
