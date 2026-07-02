# Repository Guidelines

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Reading order

Before non-trivial work, skim these design docs — the architecture is invariant-heavy and not derivable from the code alone:

- `docs/architecture.md` — distributed runtime, planes, event model, crate responsibilities, trust rings, MVP scope.

## Workspace layout

Cargo workspace, all crates live under `crates/`. `crates/sinomo` is the cli binary, `crates/sinomo-app` is the GUI app; everything else is a library crate. The binary is a one-line wrapper around `sinomo_cli::run` — all CLI surface lives in `crates/sinomo-cli/src/lib.rs`.

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
ring = "r0" | "r0-boundary" | "r1" | "r1-boundary" | "r2" | "application"
```

The ring tag is enforced by `crates/sinomo-core/tests/workspace_ring_boundaries.rs`. Rules:

1. **R0** is the minimal set of crates that defines the cluster. R0 crates depend only on R0 crates.
2. `sinomo-core` is the only `r0-boundary` crate: the R0 facade. It aggregates exactly the R0 set and re-exports it (`sinomo_core::{network, kv, cluster, node, service, object, storage, local, events}` plus the base contracts at the root).
3. R1 crates cross the R0 boundary through `sinomo-core` (direct R0 deps are still tolerated until the `FACADE_MIGRATION_COMPLETE` flag in the boundary test flips; new code should use `sinomo_core::...` paths). ABI crates (`sinomo-module-api`, `sinomo-sandbox-wasix-api`, `sinomo-dfs-cloud-files-api`) may depend directly on `sinomo-object`.
4. `sinomo-runtime` is the only `r1-boundary` crate: the R1 facade. It aggregates `sinomo-core` + the R1 crates and adds no behavior (the module/storage registry machinery lives in `sinomo_daemon::registry` and is re-exported as `sinomo_runtime::{modules, storage}`).
5. R2 and application crates must not depend on `r0*`/`r1*` crates directly — they go through `sinomo-runtime`.
6. `sinomo-network` must not depend on `sinomo-kv` or `sinomo-cluster`, and `sinomo-kv` must not depend on `sinomo-network` or `sinomo-cluster`. `sinomo-cluster` is the only bridge: it implements the network crate's `ClusterDirectory` provider by reading cluster KV. Dev-dependencies are exempt from every ring rule; an optional dependency is exempt only when all of its enabling features are the dev-only `test-support` feature or it is on the reviewed allowlist in the boundary test (`OPTIONAL_PRODUCTION_DEPENDENCY_ALLOWLIST`) — other optional deps are checked like plain ones.

Current ring assignments:

- **R0**: `sinomo-base` (IDs, event envelope, domain types, scalars), `sinomo-object` (SOBJ wire codec), `sinomo-storage`, `sinomo-local`, `sinomo-node`, `sinomo-events`, `sinomo-network` (pure networking library: object RPC, routing, transports, network modules), `sinomo-kv` (storage + Raft consensus core, transport-free), `sinomo-cluster` (cluster control plane: membership, invites, joins, KV/Raft replication drivers, state publishing, KV-backed cluster directory), `sinomo-service` (service registration, discovery, routing target selection).
- **R0 boundary**: `sinomo-core`.
- **R1**: `sinomo-daemon`, `sinomo-dfs`, `sinomo-dfs-cloud-files-api`, `sinomo-module`, `sinomo-module-api`, `sinomo-registry`, `sinomo-rollout`, `sinomo-policy`, `sinomo-eval`, `sinomo-config`, `sinomo-observe`, `sinomo-device`, `sinomo-supervision`, `sinomo-cli-registry`, `sinomo-test`, the `sinomo-sandbox*` crates, and the native module packages under `modules/`.
- **R1 boundary**: `sinomo-runtime`.
- **R2 services**: `sinomo-actions`, `sinomo-agent`, `sinomo-audio`, `sinomo-deliberation`, `sinomo-evolution`, `sinomo-fusion`, `sinomo-gateway`, `sinomo-lm`, `sinomo-memory`, `sinomo-models`, `sinomo-pytorch`, `sinomo-video`, `sinomo-voice`.
- **Application**: `sinomo`, `sinomo-app`, `sinomo-cli`, `sinomo-dfs-fuse`, `sinomo-testdir`.

When adding imports in an R2/application crate, use `sinomo_runtime::...`; in an R1 crate, prefer `sinomo_core::...` over direct R0 crate imports. The refactor that produced this layout is documented in `docs/plans/r0-r1-crate-boundary-refactor.md`.

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
