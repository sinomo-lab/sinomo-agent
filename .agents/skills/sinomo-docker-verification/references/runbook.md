# Docker cluster runbook

Commands below use Bash from the repository root. Read the selected script before
running it: project variables, build sources and cleanup behavior differ. Set
the named variables for the particular run; do not inherit another task's
`COMPOSE_FILE`, binary override or project inadvertently.

## Main segmented harness

For an isolated Linux build/run environment, use a fresh task-specific project:

```bash
SINOMO_TEST_PROJECT=sinomo-skill-dev-001 \
  bash docker/test-cluster.sh
```

Choose a currently unused project name; this example resets that project and
deletes its volumes when finished. The harness builds `target/debug/sinomo` by
default. A custom/prebuilt Linux executable requires both the build skip and a
consistent validation/mount path:

```bash
: "${SINOMO_LINUX_BINARY:?Set an absolute path to the verified Linux executable}"
SINOMO_TEST_PROJECT=sinomo-skill-dev-001 \
SINOMO_TEST_BUILD=0 \
SINOMO_TEST_BINARY="$SINOMO_LINUX_BINARY" \
SINOMO_DOCKER_BINARY="$SINOMO_LINUX_BINARY" \
  bash docker/test-cluster.sh
```

`SINOMO_TEST_BINARY` is what the harness checks; `SINOMO_DOCKER_BINARY` is what
Compose mounts. An independently inherited mount override can otherwise test a
different file. Record the digest and actual running process identity.

For a user-designated running project, inspect it first. Reuse skips bring-up
and retains its state on exit; it does not load a newly built binary:

```bash
: "${SINOMO_DEBUG_PROJECT:?Set the existing authorized debug project}"
SINOMO_TEST_PROJECT="$SINOMO_DEBUG_PROJECT" \
SINOMO_TEST_BUILD=0 SINOMO_TEST_REUSE=1 SINOMO_TEST_KEEP=1 \
  bash docker/test-cluster.sh
```

Keep any custom binary variables from the preceding example consistent during
reuse. `SINOMO_TEST_KEEP=1` without reuse still permits the initial volume reset.
Do not point a fresh harness run at a project whose data must be preserved.

## Focused lanes

These are narrower evidence, not substitutes for the full harness:

| Goal | Selection |
|---|---|
| Formation only | `SINOMO_TEST_FORMATION_ONLY=1` with `docker/test-cluster.sh` |
| Formation diagnostics | Also set `SINOMO_TEST_FORMATION_OBSERVE=1`; CPU capture defaults to node1 |
| DFS readiness/mutation/convergence | `SINOMO_TEST_DFS_READY_ONLY=1` |
| Network/KV baselines plus DFS readiness | `SINOMO_TEST_NETWORK_ONLY=1` |
| Cluster KV measurements | [benchmark-cluster-kv.sh](../../../../docker/benchmark-cluster-kv.sh), `SINOMO_TEST_PROJECT`; writes `benchmark-results/cluster-kv/` |
| Manual endpoints, route fallback, cross-segment flows | [test-routing-scenario.sh](../../../../docker/test-routing-scenario.sh), `SINOMO_SCENARIO_PROJECT` |
| Interrupted rollout, activation connection loss, voter sequencing, custom ports | [test-rollout-recovery.sh](../../../../docker/test-rollout-recovery.sh); requires distinct compatible baseline/candidate binaries and the candidate commit |
| Single-node-to-cluster migration and quarantine | [test-cluster-migration.sh](../../../../docker/test-cluster-migration.sh), `SINOMO_MIGRATION_TEST_PROJECT` |
| Idle I/O, mobile role, bridge loss/recovery | [measure-final.sh](../../../../docker/measure-final.sh), `SINOMO_GATE_PROJECT`; inspect `COMPOSE_PROJECT_NAME` precedence and reset options |
| Kernel-facing filesystem compatibility | [test-pjdfstest.sh](../../../../docker/test-pjdfstest.sh) with `--smoke`/`--full`, `--report`/`--strict` |

Do not apply the main harness's reuse variables to a different script without
checking its implementation. In particular, `measure-final.sh` normally rebuilds
and resets volumes; its gate project name alone does not preserve state.

Pjdfstest runs a native-volume control before SIFS. Report mode can exit zero
with SIFS incompatibilities: inspect the report's compatibility and completeness,
as well as harness status. Strict mode makes incompatibility/incompleteness fail.
The optional network saturation lane is a large workload; select it only when
the requested measurement needs it.

## Inspect and refresh an owned dev cluster

Use the same project, Compose file and overrides on every command:

```bash
: "${SINOMO_DEBUG_PROJECT:?Set the authorized debug project}"
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml ps
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml logs --tail 200 node1 node3 node4 node5
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml exec -T node1 sinomo cluster health
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml exec -T node5 sinomo cluster status
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml exec -T node5 sinomo test list --tag cluster-dfs
```

Select a registered runtime case and its actual parameters, then use
`sinomo test run CASE --require-cluster --json` inside the intended container.
This is a live cluster operation, so choose a case appropriate to its retained
data. The main harness handles parameters and evidence capture for its scenarios.

After replacing bind sources, refresh an owned dev project while retaining its
volumes with:

```bash
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml \
  up -d --no-deps --force-recreate node1 node2 node3 node4 node5 node6
```

Restore the original `SINOMO_DOCKER_BINARY` and override-file choices before
this command. Recheck the entrypoint's `/data/bin` precedence and actual main and
shadow executables; recreation alone does not override previously rolled-out
binaries in persistent state.

Only when disposal of this project's state is intended:

```bash
docker compose -p "$SINOMO_DEBUG_PROJECT" -f docker/docker-compose.dev.yml down -v
```

Carry any additional Compose files into teardown too. Capture evidence first.

## Acceptance and diagnosis

The main harness's quiet gate requires a continuous 30-second interval at or
below 10% CPU per service container, within its bounded search. Missing samples,
stalls, busy intervals or process-generation changes invalidate the interval.
Read current gate validation in [test-cluster.sh](../../../../docker/test-cluster.sh)
before interpreting a result. Do not replace it with a daemon-only CPU sample.

Default artifacts are under `target/cluster-test-artifacts/<run>/`; use
`SINOMO_TEST_ARTIFACTS` to choose a run directory. Preserve raw probe results and
the first failing gate. Formation observation includes capture-tagged and
all-process traces, startup generations, CPU captures, network timelines and
Perfetto output. Rendering helpers live in [scripts/debug](../../../../scripts/debug).

For a retained or interrupted run, record project/context, Compose files, binary
identity, artifact path, completed/failed phase, live process/container state and
the exact resume command. A CLI timeout or a successful reduced lane is not a
full acceptance result. Observed-formation runs need fresh uninstrumented
processes, including shadows, before acceptance.
