---
name: sinomo-docker-verification
description: Build, operate, diagnose, and verify Sinomo's Docker debug clusters using the repository harnesses. Use for segmented routing, DFS/KV integration, formation, restart and repair, migration, idle-resource gates, or cluster performance evidence.
---

# Sinomo Docker verification

Read [docker/README.md](../../../docker/README.md), the selected Compose file,
and the startup/teardown section of the selected harness. The scripts and Compose
mount definitions resolve stale command examples. Use the
[runbook](references/runbook.md) for commands, selectors, and evidence paths.

## Pick the correct environment

- The segmented dev cluster uses
  [docker-compose.dev.yml](../../../docker/docker-compose.dev.yml): six nodes,
  `seg_a`/`seg_ab`/`seg_b`, with node3 and node4 as the only bridges. Iroh stays
  disabled so native routed streams must cross those bridges.
- Six containers do not imply six Raft voters. The default formation has a seed
  voter and learners; select a membership scenario with the required voter set
  before claiming quorum/failover coverage.
- Six containers do not imply six metadata members. Each DFS shard has the
  members its placement rules give it: its write-set keeper sizes them to the
  rules' N, never below their W, and every lane runs on that layout. Choose a
  scenario's nodes from `cluster.dfs.shard-layout`, not by position. A scenario
  that needs another membership installs its own run-scoped placement rule and
  clears it before it ends.
- The thin [dev image](../../../docker/Dockerfile.dev) mounts a host-built Linux
  executable. `up --build` builds the runtime image, not Rust. On Windows, obtain
  a Linux binary from an appropriate build environment; a Windows `.exe` cannot
  serve this mount. Check architecture, loader and library compatibility.
- The full [root Compose](../../../docker-compose.yml) is a production-node
  layout, not the segmented debug cluster. The full
  [single-node layout](../../../docker/docker-compose.single-node.yml) is useful
  for source-build/self-evolution smoke tests; it is not multi-node evidence.
  The thin dev image has neither a toolchain nor `/sinomo-src` for source rollout.

## Establish the run's identity

Use all available build-server cores for Cargo compilation. Never add `--jobs`
flags or `CARGO_BUILD_JOBS` overrides to address resource issues or work around
compiler concurrency bugs. Diagnose and fix the underlying problem; use the
existing build placement/admission path for resource availability.

Record the source revision and dirty patch, binary path/hash and build profile,
Docker context, Compose files/overrides, exact project name, selected lane and
parameters, and artifact directory. Verify the selected Docker daemon can access
the bind source. Run shell recipes from the repository root in Bash with the
required host tools; adapt shell syntax deliberately on Windows.

Inspect existing projects before bring-up. Use a distinct disposable project, or
reuse the specific cluster requested by the user. Follow its existing ownership
and retention instructions. The main harness normally removes the selected
project's volumes before a fresh run and at teardown; `KEEP=1` affects teardown,
not the initial reset. `REUSE=1` skips bring-up and retains the project on exit.
It requires `SINOMO_TEST_BUILD=0` and does not refresh the mounted executable.

When a host binary or entrypoint was replaced, recreate the affected containers:
restart can retain the old bind-mounted inode. Also inspect
  [entrypoint.sh](../../../docker/entrypoint.sh): an authorized pending shadow
  starts from its stage and completes rollout-owned boot promotion. Otherwise,
  `/data/bin/sinomo` and `/data/bin/sinomo-shadow` precede the bootstrap mount. Verify
actual main/shadow process identity, not just the mounted file or CLI version.
Use fresh disposable state or an authorized rollout when those copies differ;
do not erase a retained cluster to force the mount to win.
Docker clients use the container mount `/usr/local/bin/sinomo`; a host artifact
path is not a container executable path. The maintained drivers verify its
main/shadow identity against the host artifact. Network performance probes use
explicit `MainDaemon` affinity, matching their stated target and Observe captures.

## Treat gates and failures as evidence

The main harness separates the fixed 60-second formation gate from network
readiness, DFS readiness/mutation/convergence, and quietness. Diagnose the gate
that failed instead of extending formation time or counting membership rows as
successful routed RPCs. For acceptance, retain its current thresholds and record
any deliberately changed workload/profile as a different experiment.
Use a separate fresh project for clean-formation evidence; reused volumes
exercise a warm or recovery path and cannot establish clean bootstrap behavior.

Debugging and tracing are the standard Docker configuration. The harness builds
with `sinomo-observe/cpu-profiler` and verifies debug/tracing availability. CPU
profiling is opt-in: `SINOMO_TEST_CPU_PROFILE=1` starts bounded formation profiles
on all nodes, adds CPU profiles to the idle and write stage reports, and requires
profiler support and the formation capture. Every gate, including formation,
quietness and performance, must pass with observation enabled at its unchanged
threshold. Use observed timings directly without subtracting
overhead or treating instrumentation as a reason to waive a failure. The quiet
gate measures entire containers, including samplers, shadows and helpers.
Every quiet gate collects `idle-<leg>/REPORT.md` with the standard Observe idle
profiler and includes per-node CPU stream summaries in the generated verification
report. Diagnose its background counters and stages, and CPU profiles when
profiling was requested, before changing scheduling. A partial profile does not change the gate verdict; missing metrics
and trace coverage must remain visible.
For a follow-up idle check, select `SINOMO_TEST_QUIET_ONLY=1` with reuse and build
disabled. This retains readiness, observation and the unchanged quiet gate while
omitting mutation workloads. Report the focused scope and any earlier failures.

Keep probe JSON, failed-scenario names, logs, main/shadow generations and trace
artifacts before teardown. A timeout may leave a run or mutation in progress;
inspect its state before launching another copy. Do not tear down unrelated
Compose projects to satisfy the harness's isolation guard.

Report the exact lane and pass/fail result, tested artifact identity, raw evidence
paths, and whether the cluster was retained. A reduced lane or pjdfstest report
mode does not establish full-suite acceptance.

Use the generated `REPORT.md` and `report.json` in the selected artifact directory
as the reporting source, including for failures. The main harness, history
verifier and ordinary write driver generate them automatically. Reuse their
tables and links directly; extend the maintained verifier/reporter when a needed
measurement is missing instead of creating a different benchmark for each task.

Ordinary Docker write performance uses both **W=1/N=2 and W=2/N=3** through
`docker/measure-dfs-write-latency.py`, for both callers and all three shards.
Require recorded effective-policy checks and keep policy/create/overwrite
distributions separate. The write driver generates detailed stage/I/O reports by
default via `profile-dfs-writes.py`, with CPU profiles when profiling is requested;
skipping that stage is diagnostic only.
Apply the repository
[performance analysis skill](../../../skills/sinomo-performance-analysis/SKILL.md)
for the full matrix, standard tables and separate traced diagnostics.
