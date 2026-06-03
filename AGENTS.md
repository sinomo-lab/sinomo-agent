# Repository Guidelines

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Reading order

Before non-trivial work, skim these design docs — the architecture is invariant-heavy and not derivable from the code alone:

- `docs/architecture.md` — distributed runtime, planes, event model, crate responsibilities, trust rings, MVP scope.
- `docs/self-evolution-architecture.md` — R0–R5 trust rings, capsule format, evaluation/rollout/rollback model.
- `docs/plans/r0-r1-bootstrap-technical-architecture.md` and `docs/plans/r0-r1-self-evolution-bootstrap-plan.md` — current bootstrap plan for the R0/R1 foundation.
- `docs/plans/human-guided-long-horizon-evolution.md` — campaign layer driving `sinomo evolve propose`.

## Workspace layout

Cargo workspace, edition 2024, `rust-version = 1.90`, `resolver = "3"`. All crates live under `crates/`. `crates/sinomo` is the single shipped binary; everything else is a library crate. The binary is a one-line wrapper around `sinomo_cli::run` — all CLI surface lives in `crates/sinomo-cli/src/lib.rs`.

Common workflow:

```bash
cargo build                         # whole workspace
cargo build -p sinomo               # the binary only (also what `sinomo rollout apply` runs)
cargo run -p sinomo -- <args>       # run the CLI
cargo test                          # all tests
cargo test -p sinomo-core           # one crate
cargo test -p sinomo-core --test workspace_ring_boundaries  # single integration test
cargo test -p <crate> <name>        # one test by substring
cargo fmt --all
cargo clippy --workspace --all-targets
cargo check -p sinomo-sandbox --features wasix-runtime      # focused sandbox runtime check.
```

Profile note: `[profile.dev]` sets `opt-level = 2` and `incremental = false`, so dev builds are slower than typical Rust crates. Release uses `panic = "abort"`.

## Trust rings — load-bearing invariant

Every crate declares its ring in `Cargo.toml`:

```toml
[package.metadata.sinomo]
ring = "r0" | "r1" | "r0-r1-boundary" | "r2" | "application"
```

The ring tag is enforced by `crates/sinomo-core/tests/workspace_ring_boundaries.rs`. Rules:

1. R0/R1 crates must not depend on R2 or application crates.
2. R2 and application crates must **not** depend on R0/R1 crates directly — they go through `sinomo-runtime`, the only crate tagged `r0-r1-boundary`.
3. `sinomo-runtime` aggregates exclusively R0/R1 crates and re-exports them as `r0::*`/`r1::*` modules. Adding a new R0/R1 crate means wiring it into `crates/sinomo-runtime/src/lib.rs` and `Cargo.toml`.
4. Every crate must declare a ring or the test fails.

Current ring assignments:

- **R0**: `sinomo-core` (event envelope, IDs, domain types, scalars — no service deps).
- **R1**: `sinomo-api`, `sinomo-device`, `sinomo-dfs`, `sinomo-eval`, `sinomo-events`, `sinomo-local`, `sinomo-module`, `sinomo-network`, `sinomo-node`, `sinomo-policy`, `sinomo-registry`, `sinomo-rollout`, `sinomo-sandbox`, `sinomo-supervision`, `sinomo-sync`.
- **R0/R1 boundary**: `sinomo-runtime`.
- **R2 services**: `sinomo-actions`, `sinomo-agent`, `sinomo-audio`, `sinomo-deliberation`, `sinomo-evolution`, `sinomo-fusion`, `sinomo-gateway`, `sinomo-lm`, `sinomo-memory`, `sinomo-trigger`, `sinomo-video`, `sinomo-voice`.
- **Application**: `sinomo`, `sinomo-app`, `sinomo-cli`.

When adding imports in an R2/application crate, prefer `use sinomo_runtime::core::...` / `use sinomo_runtime::events::...` over importing `sinomo_core` / `sinomo_events` directly — the latter compiles today but will break the boundary tests once that crate is moved into the facade.

## Binary surface

`sinomo` is the single entry point. Top-level commands (see `print_usage` in `crates/sinomo-cli/src/lib.rs`):

- `sinomo` (no args) — interactive TUI loop (`run_tui`).
- `sinomo daemon run|start|status|stop` — local control-plane daemon, default `127.0.0.1:21353`. Owns `$SINOMO_DATA_DIR/daemon/{daemon.lock,status.json}`.
- `sinomo daemon shadow run|start|status|stop` — supervisor for the main daemon, default port `21354`, separate state dir `shadow-daemon/`.
- `sinomo rollout init-source|apply|status` — manages the data-dir source repo at `$SINOMO_DATA_DIR/source/sinomo` and the installed binaries at `$SINOMO_DATA_DIR/bin/sinomo-{main,shadow}`. `apply` runs `cargo build -p sinomo` on the data-dir source and atomically swaps binaries.
- `sinomo evolve propose|approve|reject|restore|status|show|verify|packet|cleanup|job|schedule|models` — the self-evolution campaign and candidate pipeline.
- `sinomo models`, `sinomo providers`, `sinomo usage` — language-model provider configuration.

## Local data directory

State lives under `SINOMO_DATA_DIR` (defaults to `$HOME/.sinomo`). The layout is owned by `sinomo-local::SinomoDataDir`:

- `config/` — local config.
- `dfs/` — distributed filesystem content store.
- `providers/<provider>/credentials.json` — language-model provider credentials.
- `daemon/`, `shadow-daemon/` — daemon lock + status.
- `source/sinomo/` — Sinomo's own source repo for self-rollout (created by `sinomo rollout init-source`).
- `bin/sinomo-{main,shadow}` — installed binaries after `sinomo rollout apply`.
- `rollout/build.log`, `rollout/last-main-exit.json` — rollout build log and last main-daemon exit observation.

When `sinomo evolve propose` doesn't get an explicit `--repo`, it defaults to the data-dir source repo, not the operator's checkout.

## Conventions

- Workspace dependency versions live in `[workspace.dependencies]` in the root `Cargo.toml`. New crates should use `serde.workspace = true` etc. rather than pinning versions locally.
- Events are the cross-service contract; new event families should follow the dotted-namespace convention in `docs/architecture.md` (`evolution.*`, `module.*`, `dfs.*`, `rollout.*`, …) and include node/device provenance and routing metadata.
- Use integration tests for cross-crate behavior such as DFS, KV, networking, and sandbox execution. For sandbox/toolchain work, prefer real-command verifier scripts under `crates/sinomo-sandbox/toolchains/` when behavior depends on actual WASIX tools. Avoid committing generated `target/`, toolchain `work/`, `out/`, or `benchmark-results/` artifacts.
- Do not bypass Sinomo networking, storage, or policy paths for cluster features. Environment variables are acceptable only for local pre-cluster configuration; distributed settings should use cluster KV. Treat sandbox filesystem access as VFS/SIFS-backed even when using local development backends.
