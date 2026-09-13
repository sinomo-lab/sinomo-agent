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

Record the source revision and dirty patch, binary path/hash and build profile,
Docker context, Compose files/overrides, exact project name, selected lane and
parameters, and artifact directory. Verify the selected Docker daemon can access
the bind source. Run shell recipes from the repository root in Bash with the
required host tools; adapt shell syntax deliberately on Windows.

Inspect existing projects before bring-up. Use a distinct disposable project, or
reuse the specific cluster requested by the user. Follow its existing ownership
and retention instructions. The main harness normally removes the selected
project's volumes before a fresh run and at teardown; `KEEP=1` affects teardown,
not the initial reset. `REUSE=1` skips bring-up and currently retains the project
on exit. It does not refresh the mounted executable.

When a host binary or entrypoint was replaced, recreate the affected containers:
restart can retain the old bind-mounted inode. Also inspect
  [entrypoint.sh](../../../docker/entrypoint.sh): an authorized pending shadow
  starts from its stage and completes rollout-owned boot promotion. Otherwise,
  `/data/bin/sinomo` and `/data/bin/sinomo-shadow` precede the bootstrap mount. Verify
actual main/shadow process identity, not just the mounted file or CLI version.
Use fresh disposable state or an authorized rollout when those copies differ;
do not erase a retained cluster to force the mount to win.

## Treat gates and failures as evidence

The main harness separates the fixed 60-second formation gate from network
readiness, DFS readiness/mutation/convergence, and quietness. Diagnose the gate
that failed instead of extending formation time or counting membership rows as
successful routed RPCs. For acceptance, retain its current thresholds and record
any deliberately changed workload/profile as a different experiment.
Use a separate fresh project for clean-formation evidence; reused volumes
exercise a warm or recovery path and cannot establish clean bootstrap behavior.

Formation observation and profiling are diagnostic lanes. Their startup traces,
samplers and shadow processes perturb the system; form a fresh uninstrumented
cluster before acceptance measurements. The quiet gate measures entire
containers, including shadow and helper processes, not just daemon pprof samples.

Keep probe JSON, failed-scenario names, logs, main/shadow generations and trace
artifacts before teardown. A timeout may leave a run or mutation in progress;
inspect its state before launching another copy. Do not tear down unrelated
Compose projects to satisfy the harness's isolation guard.

Report the exact lane and pass/fail result, tested artifact identity, raw evidence
paths, and whether the cluster was retained. A reduced lane or pjdfstest report
mode does not establish full-suite acceptance.
