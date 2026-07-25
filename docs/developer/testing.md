# Testing and Validation

Sinomo uses several evidence layers because a unit test cannot establish
cluster behavior and a live cluster run cannot exhaustively explore a protocol
state machine. Choose the smallest relevant layer while iterating, then add the
layers required by the changed boundary.

## Fast Local Loop

```sh
CRATE=sinomo-cluster
TEST_FILTER=your_test_name
cargo test -p "$CRATE" "$TEST_FILTER"
cargo check -p "$CRATE"
cargo build -p sinomo
git diff --check
```

Use a precise test filter during iteration. Before handoff, run the complete
test target or crate that owns the change so a filter does not hide adjacent
regressions. Replace the example values with real package, test, or runtime
identifiers.

Format Rust changes with:

```sh
cargo fmt --all
cargo fmt --all -- --check
```

If the workspace has pre-existing formatting drift outside the patch, do not
format unrelated code silently. Format the touched files, report the baseline
failure, and keep the contribution scoped.

## Crate and Integration Tests

Representative distributed test targets are:

```sh
cargo test -p sinomo-kv --test kv
cargo test -p sinomo-cluster --test control_plane
cargo test -p sinomo-dfs --test networked_dfs
```

Use unit tests for pure parsing, validation, state machines, ordering, and
selection. Prefer integration tests for cross-crate network, KV, cluster, DFS,
daemon, persistence, sandbox, and restart behavior.

Filesystem tests should use `sinomo-testdir::TestDir` when possible. Set
`SINOMO_TEST_KEEP_TMP=1` only while investigating a failure, and preserve the
reported path before another run.

## Trust-Ring Gate

Run this for every manifest, facade, feature, crate, or cross-ring import
change:

```sh
cargo test -p sinomo-core --test workspace_ring_boundaries
```

It checks ring metadata, dependency direction, facade shape, ABI exceptions,
native-module default membership, and the network/KV production split.

## Broad Rust Gates

```sh
# Default members only.
cargo test

# Every workspace member with default features.
cargo test --workspace
cargo check --workspace

# Lint all workspace targets.
cargo clippy --workspace --all-targets
```

Plain `cargo test` is not every workspace member. Platform-specific and opt-in
module packages may require their own feature and environment.

## Runtime Tests

Runtime tests exercise registered product behavior rather than Cargo test
functions:

```sh
cargo run -p sinomo -- test list
cargo run -p sinomo -- test list --tag cluster-dfs
TEST_ID=your-test-id
cargo run -p sinomo -- test run "$TEST_ID" --json
```

Tests that contact providers or require a formed cluster stay opt-in through
`--live` and `--require-cluster`. Point them at an isolated
`SINOMO_DATA_DIR`; never let a test mutate an operator's normal installation.

## Formal Verification

The Quint harness pins Quint 0.32.0 and Apalache 0.56.1. It classifies models
as current-behavior refinements, abstract contracts, or target hardening. Read
the [formal verification guide](../formal/cluster-verification.md) before
interpreting a pass.

```sh
bash scripts/formal/check-quint.sh typecheck
bash scripts/formal/check-quint.sh fast

QUINT_CASE_FILTER=raft-kv QUINT_REQUIRE_WITNESSES=1 \
  bash scripts/formal/check-quint.sh coverage

QUINT_CASE_FILTER=raft-kv \
  bash scripts/formal/check-quint.sh verify-fast
```

Use a focused case while iterating, then the relevant tier. Record tool
versions, steps, samples, seed, timeout, witnesses, and elapsed time. Bounded
model checking is compositional evidence, not a proof of unmodeled WAL,
cryptographic, scheduling, or real-network behavior.

## Real Multi-Node Validation

The Docker harnesses include a segmented six-node development cluster plus
separate migration and single-node paths. Together they exercise formation,
routing, migration, DFS, failure, restart, and idle-resource gates. Start with
[the Docker guide](../../docker/README.md).

Common harnesses include:

```sh
docker/test-cluster.sh
docker/benchmark-cluster-kv.sh
docker/test-routing-scenario.sh
docker/test-cluster-migration.sh
docker/measure-final.sh 60 90
```

These commands can take substantial time and create containers, volumes, and
artifacts. Read their options, use a distinct Compose project where supported,
and preserve the generated failure bundle before teardown.

A passing in-process cluster test does not replace real transport, process,
disk, restart, or platform evidence for a change that affects those paths.

## Sandbox and WASIX Verification

Checking the contract marker is useful but insufficient for runtime changes:

```sh
cargo check -p sinomo-sandbox --features wasix-runtime
cargo check -p sinomo-sandbox-wasix-module --features wasix-runtime
```

Run the real verifier matching the changed toolchain:

```sh
crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh
crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh
crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh
```

Verifier output and generated toolchain `work-*`/`out-*` directories are not
source artifacts and should not be committed.

## Benchmarks and Experimental Evaluation

Benchmarks live under `modules/benchmark/sinomo-benchmarks/` and are opt-in.
Available surfaces include indexed-file memory and LongMemEval-S, SIFS/FUSE,
SWE-bench, MCU/MineStudio, and environment adapters.

Use [research and evaluation](research-and-evaluation.md) to classify the claim,
evidence, and current implementation boundary. Use
[experimental reproducibility](experimental-reproducibility.md) as the
canonical checklist for source, environment, datasets, model configuration,
commands, raw results, uncertainty, and result-bundle contents. A benchmark
README may impose stricter protocol-specific requirements.

Write run output under the gitignored `benchmark-results/` directory. Curated
claims belong in a reviewed evidence document that links or describes the
immutable result artifact; do not commit an unreviewed output dump.

## Current CI Boundary

The GitLab pipeline runs on `main` and builds locked release CLI artifacts for
configured Linux and Windows targets. It does not currently run merge-request
formatting, Clippy, unit tests, formal checks, real-cluster tests, or benchmarks.

Contributors must therefore report local validation explicitly. A future CI
gate can automate a command, but it does not change which evidence the
architecture requires.

## Validation Matrix

| Change | Minimum additional evidence |
|---|---|
| Manifest or facade | Trust-ring test and workspace check |
| KV/Raft or membership | Crate tests, cluster integration, relevant Quint refinement |
| Network routing or transport | Network tests, routed multi-node scenario, relevant Quint refinement |
| DFS metadata/content | DFS integration, restart/repair scenario, relevant formal slice |
| Daemon lifecycle or rollout | Daemon/rollout tests and process-level activation/recovery scenario |
| Sandbox/toolchain | Actual backend build and matching verifier |
| Provider or agent protocol | Deterministic fixtures; opt-in live smoke only when required |
| Performance optimization | Correctness gates plus before/after benchmark on a recorded environment |
| User command | Parser/help tests, domain tests, and updated user guide |
| Documentation only | `python3 scripts/docs/check_links.py` and `git diff --check` |
