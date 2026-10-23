# DFS validation recipes and evidence boundaries

Run from the repository root. Inspect current help/manifests before executing
a changed harness. They are
task-dependent operations, not commands to run merely because this file loaded.

## Inventory existing results without another run

Start with generated `REPORT.md`, `report.json`, failing-case JSON and retained
test stdout/stderr. The Docker write reporter already preserves incomplete
distributions; do not replace it with a new benchmark. For Rust text logs, exact
failure headings and target summaries are an efficient first inventory:

```bash
: "${DFS_TEST_LOG:?Set a retained Rust test log}"
rg -n '^running [0-9]+ tests?$|^test .* \.\.\. (FAILED|ignored)|^---- .* ----$|^test result:|^error: test failed' "$DFS_TEST_LOG"
```

This is navigation, not a parser that proves completion. Check the target launch
and exit result, every final summary, the expected test count and unstarted
targets. Terse output, panic/abort and killed processes may have no per-case row.
Do not sum a first failed run with a successful retry as one “passed suite.”

Use a compact working table in the task artifact directory:

```text
case | target | source/binary | setup/workload/drain | observed failure
     | prerequisite evidence | candidate cause | demonstrated cause
     | product/fixture/environment/unclassified | original artifact
     | control/fix result | remaining gate | owner/independent lane
```

Group failures with common missing prerequisites before editing. The group is
a hypothesis until a representative real-path experiment supports it. Keep
individual cases linked so a group fix cannot silently drop coverage.

## Discover and reuse actual Cargo executables

The DFS manifest has integration targets requiring `test-support`: currently
`networked_dfs`, `publication_consistency`, `data_consistency_failures`,
`lazy_file_sync`, and `posix_regressions`. Plain `cargo test -p sinomo-dfs` can
omit them. History, Attachments and Migration have separate crates/targets.

Run the following build inside the already-established shared build/resource
guard. Use a fresh durable artifact directory and retain the command's exit
status; redirecting output does not change its status:

The maintained Linux measurement helper is
[measure-compile-cost.py](../../../../scripts/ci/measure-compile-cost.py). Its
`--help` describes source snapshots, workload/profile/cache labels and aggregate
process sampling. It acquires `/tmp/sinomo-maintenance-build.lock` nonblockingly
and passes its lock descriptor to the workload. Do not wrap it in a second
acquisition of the same lock, remove a contended lock, or call a shared cgroup's
historical peak this compiler's peak. Run inside the active task's established
memory scope; the usual DFS diagnostic ceiling is 16 GiB with no swap, retaining
normal Cargo workers. Use actual measured cache state, not a guessed “warm” label.

```bash
: "${DFS_RUN_DIR:?Set a new absolute durable artifact directory}"
mkdir "$DFS_RUN_DIR"
git rev-parse HEAD > "$DFS_RUN_DIR/source-head.txt"
git status --porcelain=v1 > "$DFS_RUN_DIR/source-status.txt"
git diff --binary HEAD > "$DFS_RUN_DIR/source.patch"
sha256sum Cargo.lock > "$DFS_RUN_DIR/cargo-lock.sha256"
cargo test --locked -p sinomo-dfs --features test-support --no-run \
  --message-format=json > "$DFS_RUN_DIR/build.jsonl" 2> "$DFS_RUN_DIR/build.stderr"
```

Stop dependent execution if the build fails. The sample records tracked dirty
content; also record relevant untracked source, manifests, local dependency
overrides, compiler/target and profile/features. A Git SHA alone does not identify
a dirty build. Check correspondence again after the build; do not label drifted
output as the frozen source.

List emitted test executables from Cargo's structured output, not the newest
file matching a hash glob:

```bash
python3 - "$DFS_RUN_DIR/build.jsonl" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    try:
        row = json.loads(line)
    except json.JSONDecodeError:
        continue
    if (row.get("reason") == "compiler-artifact"
            and row.get("profile", {}).get("test")
            and row.get("executable")):
        print(row["target"]["name"], row["executable"], sep="\t")
PY
```

Hash each selected executable and inspect its registered tests:

```bash
: "${DFS_TEST_EXE:?Set an exact executable emitted by this build}"
sha256sum "$DFS_TEST_EXE"
"$DFS_TEST_EXE" --list
```

Use the fully qualified name from `--list` with `--exact`; a short suffix can
match zero tests. For a full target, omit the filter. Preserve normal test-worker
behavior; a serial diagnostic is a different workload, not evidence that a
concurrent failure is fixed. Repeated execution of an unchanged executable can
test an interleaving without recompilation. Changed source/settings require a
new build and correspondence record.

## Preserve fixtures without exhausting the host

[run-with-test-fixtures.py](../../../../scripts/ci/run-with-test-fixtures.py) owns
a fresh private TMPDIR. After processes drain, it archives, flushes and compares
failed fixtures before reclaiming originals. Success reclaims its private root.
It never sweeps older/shared roots. Artifact storage must be durable and owned
by the invoking user; inspect its current reserve checks.

Inside the existing resource guard, for one selected regression:

```bash
: "${DFS_TEST_NAME:?Set the full test name listed by the selected executable}"
python3 scripts/ci/run-with-test-fixtures.py \
  --artifacts "$DFS_RUN_DIR/focused" -- \
  "$DFS_TEST_EXE" "$DFS_TEST_NAME" --exact --nocapture
```

For the owning whole target, use a new child artifact directory and omit test
name and `--exact`. Do not accidentally call a focused result the whole target.
The wrapper's combined `output.log`, result artifacts and executable digest belong with
the source manifest. Follow `archive.json` to extract into a **new empty** copy;
retain the original archive/digest and never edit historical DATA in place.

For separately measured cleanup, use `--defer-cleanup`, then
`--finalize ARTIFACTS` under the same established guard. Finalization success is
cleanup evidence; the original test exit remains in `result.json` and
`pending-cleanup.json`. Do not leave pending roots unowned. Export wanted native
traces to the durable task directory before fixture success cleanup.

Before an expensive run inspect bytes *and inodes* on the selected fixture and
artifact filesystems. Do not weaken reserve checks, delete other owners' roots,
or keep a global `SINOMO_TEST_KEEP_TMP=1` as routine retention policy.

## Focused native controls

Use CodeGraph and the [source map](../../sinomo-dfs-debugging/references/source-map.md)
to locate the test, then discover its full name in the emitted executable.
Select controls by the behavior changed:

| Question | Existing starting point |
|---|---|
| Does a required ACK escape unrelated writer rebuild? | `current_replica_does_not_join_unrelated_writer_configuration_rebuild`, `new_replica_prepare_does_not_join_unrelated_writer_configuration_rebuild` |
| Does a later maintenance turn miss foreground preemption? | `required_replica_wait_preempts_a_later_maintenance_turn_after_shard_release`, plus cancelled-wait control |
| Are actual required replies parked? | `native_completed_response_progresses_while_advisory_maintenance_is_parked` |
| Are required journal bytes parked? | `native_required_journal_suffix_is_not_parked_by_an_execution_hint`, stale-connectivity control |
| Can the original prepare resume? | `native_explicit_retry_resumes_ready_intent_after_overlapping_prepare` |

Do not rerun this entire list automatically for every DFS edit. Preserve the
original failing workload alongside the causal control and broaden according to
the owning target and changed boundary.

## Docker and retained-state cases

Use the [Docker skill](../../sinomo-docker-verification/SKILL.md) and its
[runbook](../../sinomo-docker-verification/references/runbook.md) for exact
project/binary/reuse/cleanup semantics. Important selectors in `test-cluster.sh`:

| Needed evidence | Existing lane/tool | Limit |
|---|---|---|
| Formation | `SINOMO_TEST_FORMATION_ONLY=1` | Not DFS publication or recovery |
| DFS readiness/mutation/convergence | `SINOMO_TEST_DFS_READY_ONLY=1` | Not the full history/handoff suite |
| Normal/history/restart/handoff/core-only | `SINOMO_TEST_DFS_ONLY=1`, `test-file-history.py --case` | Inspect the selected fault; graceful handoff is not sudden-primary-loss failover |
| Repeated writes | `measure-dfs-write-latency.py` | Correctness outcomes matter even when timing is permitted to fail |
| Idle | `SINOMO_TEST_QUIET_ONLY=1` with verified reuse/build disabled | Focused idle evidence; no mutation-suite result |
| Old-state upgrade | `test-cluster-migration.sh`, native saved-DATA fixtures | Fresh volumes do not exercise a production migration |
| Activation/recovery | `test-rollout-recovery.sh` | Requires explicit baseline/candidate artifacts |

Use unique disposable projects. `KEEP=1` does not prevent the main harness's
initial reset; reuse has its own build/binary requirements. A container restart
can keep an old bind inode; `/data/bin` can supersede the mounted bootstrap
binary. Verify actual main/shadow executable identity. Six containers are not
automatically six Raft voters or a three-member metadata group.

Retain generated `REPORT.md`, `report.json`, raw case JSON and failed operation
identity before teardown. Tracing/profiling stays enabled at unchanged thresholds.
Use the [performance skill](../../../../skills/sinomo-performance-analysis/SKILL.md)
for measurement tables, both W/N policies and coverage. For native deployment,
also require source-matched old-DATA/platform evidence through the rollout owner.

## Formal evidence answers a different question

The current `dfs-whole-shard` model is a bounded abstract contract; native
conformance is a separate capture-scoped result. See
[formal guide](../../../../docs/formal/cluster-verification.md#whole-shard-prepare-publication-and-handoff).

```bash
QUINT_CASE_FILTER=dfs-whole-shard QUINT_REQUIRE_WITNESSES=1 \
  bash scripts/formal/check-quint.sh coverage
QUINT_RUN_SEED=0x51a020261006 bash scripts/formal/check-quint.sh whole-shard
```

These execute tools and can consume resources; schedule only for relevant work.
Record actual bounds, seed, witnesses and classification. The existing model
intentionally denies partial metadata participation. Its pass cannot establish
mobile-node-loss availability, OS scheduling fairness, arbitrary disk recovery,
or Rust conformance without complete native captures. The original availability
requirement needs its own positive oracle; revise the implementation/model to
satisfy it rather than treating partial-participation rejection as acceptance.
