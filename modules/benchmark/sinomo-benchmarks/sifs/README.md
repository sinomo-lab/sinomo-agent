# SIFS FUSE benchmark

`sinomo-sifs-fuse-bench` is an opt-in integration and performance harness for the real kernel FUSE mount backed by SIFS. It exercises mount lifecycle, metadata operations, small and large I/O, links, locking, read-only behavior, remount reads, and concurrent creates. Results are emitted as a versioned JSON report with source revision, dirty-worktree state, resolved data directory, cluster id, and node id so runs can be compared later.

This is deliberately not part of ordinary workspace tests: it needs a Unix FUSE device, a running Sinomo daemon, a reachable cluster, and an existing SIFS subtree that is writable by the caller.

## Prerequisites

- Run on a Unix host with `/dev/fuse` available to the current user.
- Start the Sinomo daemon and verify that it can reach the intended cluster.
- Point `SINOMO_DATA_DIR` at that daemon's data directory when it is not the default `$HOME/.sinomo`.
- Choose an existing caller-writable SIFS subtree. A user or agent home is appropriate; `/`, `/users`, and other root-owned bootstrap directories normally are not.
- Keep unrelated load low when comparing performance runs.

Build the release runner explicitly:

```bash
cargo build --release -p sinomo-benchmarks \
  --features sifs-fuse-bench \
  --bin sinomo-sifs-fuse-bench
```

The harness parser, report aggregation, percentile math, payload generation, and catalog wiring have non-FUSE tests:

```bash
cargo test -p sinomo-benchmarks --features sifs-fuse-bench \
  --bin sinomo-sifs-fuse-bench
cargo test -p sinomo-benchmarks --features sifs-fuse-bench \
  sifs_fuse_plan_is_opt_in_and_reproducible
```

Set the root once for the commands below:

```bash
export SINOMO_SIFS_FUSE_BENCH_ROOT=/sinomo/agents/<agent-id>/home
```

## Repeatable suites

Run the complete correctness and performance workload:

```bash
target/release/sinomo-sifs-fuse-bench \
  --root "$SINOMO_SIFS_FUSE_BENCH_ROOT" \
  --suite full \
  --require-fuse \
  --json-out benchmark-results/sifs-fuse/report.json
```

Run correctness probes without the timed performance workload:

```bash
target/release/sinomo-sifs-fuse-bench \
  --root "$SINOMO_SIFS_FUSE_BENCH_ROOT" \
  --suite correctness \
  --require-fuse \
  --json-out benchmark-results/sifs-fuse/correctness.json
```

Run only the performance workload:

```bash
target/release/sinomo-sifs-fuse-bench \
  --root "$SINOMO_SIFS_FUSE_BENCH_ROOT" \
  --suite performance \
  --require-fuse \
  --json-out benchmark-results/sifs-fuse/performance.json
```

## Parameters and exit behavior

- `--root <SIFS_PATH>` selects the existing SIFS subtree to mount. It must be absolute and writable by the invoking uid and gid.
- `--suite <full|correctness|performance>` chooses which workloads run.
- `--json-out <PATH>` writes the machine-readable report, creating parent directories as needed.
- `SINOMO_DATA_DIR` selects the local Sinomo data directory and daemon connection state.
- `SINOMO_SIFS_FUSE_BENCH_ROOT` is a convenience used by the catalog plan; passing `--root` remains explicit at the runner boundary.

Use `--help` to list workload sizing, concurrency, cache, and performance-threshold options supported by the current runner. Performance thresholds are opt-in: an unconfigured performance run records measurements but does not impose machine-specific latency or throughput gates. The host-filesystem comparison is a page-cache baseline; it is not a durable-storage benchmark.

A completed run exits successfully when every selected correctness probe and configured performance threshold passes. Unavailable FUSE support is an ordinary skip unless `--require-fuse` is supplied. Product, cleanup, or configured-threshold failures return 1; invalid arguments or harness errors return 2; a required missing FUSE capability returns 3. `--report-only` returns zero for product and threshold failures while preserving `"status": "failed"` in JSON. The runner still attempts to write the report on failed runs so partial evidence remains inspectable.

## Isolation and cleanup

Each invocation creates a unique child below `--root` and performs all mutations there. It mounts into a unique host temporary directory, unmounts between post-remount read phases, removes the benchmark child, and removes the host mount directory before returning. Cleanup outcomes are included in the report. `--keep-fixtures` explicitly preserves the generated SIFS child for debugging while still unmounting and removing the host mount point.

If the process is killed in a way that prevents cleanup, unmount the temporary mount with the host's FUSE unmount command and remove only the uniquely named benchmark child after verifying its path. Never point the runner at a subtree that may be deleted wholesale: the supplied `--root` is a parent and is not itself removed.

JSON reports under `benchmark-results/` are run artifacts and should not be committed.
