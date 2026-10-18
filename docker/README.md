# Sinomo Docker clusters

Compose layout:

- `../docker-compose.yml` — **production node**: full image (toolchain + managed
  source baked in, self-rollout capable), iroh enabled, one configurable
  seed/joiner service.
- `docker/docker-compose.dev.yml` — **6-node segmented dev cluster** (this
  README): network-hardening test bed running the **host-built binary**.
- `docker/docker-compose.routing-scenario.yml` — three-node manual-endpoint
  and routed-LAN topology for `test-routing-scenario.sh`.
- `docker/docker-compose.migration.yml` — inert `target`/`source` pair for
  `test-cluster-migration.sh`.
- Overlays for the dev cluster: `docker-compose.profile.yml` (opt-in `perf`
  sidecar), `docker-compose.pjdfstest.yml` (FUSE-enabled `node5` for
  `test-pjdfstest.sh`), and `docker-compose.dfs-tmpfs.yml` (measurement-only
  tmpfs data volumes; never durability evidence).
- `docker/docker-compose.android.yml` — Android APK builder container.
- `docker/docker-compose.single-node.yml` — minimal one-node cluster for local
  DFS and self-evolution smoke testing (full image; the smoke path needs the
  in-container toolchain):

```bash
docker compose -f docker/docker-compose.single-node.yml up --build -d sinomo-single
docker compose -f docker/docker-compose.single-node.yml run --rm sinomo-single-smoke
docker compose -f docker/docker-compose.single-node.yml down -v
```

`sinomo-single` keeps state in the persistent `single-node-data` volume. The smoke
service uses container-local data and is discarded after each run so repeated
smoke tests remain a true one-node cluster. This debug image also builds and
activates the optional `module.benchmarks` native-dylib fixture before daemon
startup. It supports strict `agent-readiness` and fixture-backed
`agent-readiness-extended` cancellation gates without making the benchmark
module a production default.

# Segmented dev cluster — network-hardening test bed

A disposable **6-node** Sinomo cluster partitioned into three network segments, with
**iroh disabled**, used to develop native (non-iroh) peer **discovery and routing**
and to exercise cluster features (DFS, rollout supervision, routing) on a real
multi-node topology.

## Host-built binary (fast iteration)

The dev nodes run a thin runtime image (`docker/Dockerfile.dev`, no Rust
toolchain) and bind-mount the **host-built** `target/debug/sinomo` read-only
into every container, so the edit-test loop never rebuilds an image:

```bash
cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
docker compose -f docker/docker-compose.dev.yml up --build -d
docker compose -f docker/docker-compose.dev.yml exec node1 sinomo cluster health

docker compose -f docker/docker-compose.dev.yml stop
cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
docker compose -f docker/docker-compose.dev.yml up -d --force-recreate
```

`--build` only (re)creates the small runtime image; it never compiles Rust. The
entrypoint script is bind-mounted too, so script edits also apply on restart.

Compatibility: the base image is `debian:trixie-slim` (glibc 2.41); a binary
built on any current mainstream distro runs as is. If your host glibc is newer
than the base image's, bump the base in `docker/Dockerfile.dev`.

Trade-off: the thin image has no toolchain or `/sinomo-src`, so in-container
self-rollout builds (`rollout apply` from the managed source repo) are
unavailable here — use the root production compose or the single-node compose
when testing rollout-from-source.

## DFS correctness and benchmarks

The full `test-cluster.sh` suite includes normal-operation and file-history
correctness, abrupt restart durability, metadata-primary handoff, and history
benchmarks. Run just the DFS lane (with the same formation, network readiness,
DFS mutation/convergence, and benchmark quietness gates):

```sh
SINOMO_TEST_PROJECT=sinomo-dfs-verification \
SINOMO_TEST_DFS_ONLY=1 bash docker/test-cluster.sh
```

For a usable DFS correctness milestone with latency/throughput optimization deferred,
select the separate maintained baseline lane on a disposable project:

```sh
SINOMO_TEST_PROJECT=sinomo-dfs-baseline \
SINOMO_TEST_DFS_BASELINE_ONLY=1 \
SINOMO_TEST_BUILD=0 \
SINOMO_TEST_BINARY=/absolute/path/to/verified-sinomo \
SINOMO_DOCKER_BINARY=/absolute/path/to/verified-sinomo \
SINOMO_TEST_ARTIFACTS=/absolute/path/to/fresh-baseline-artifacts \
flock /tmp/sinomo-maintenance-build.lock bash docker/test-cluster.sh
```

This retains formation, authenticated network readiness, explicit guarded six-member
metadata assignment, all-node mutation/convergence, and required debug/tracing/CPU
profiler support. It then runs baseline invariants, read-your-write, cross-node
visibility, prefix rename, real disk damage, network interruption/heal, process
restart durability, core-only component removal/restoration with lost replies,
normal mutations, retained-version restart/replay, and native primary handoff.
Final invariants, routed status, convergence and the same joint CPU/network quiet
gate are attempted even after an earlier functional failure. Existing scenario
commands, operation IDs, deadlines and byte/receipt assertions are unchanged.
A failed functional or quiet gate remains a failed baseline.

The maintained network interruption case seeds a real file under the complete
six-member metadata assignment, then disconnects assigned node6. The attempted
overwrite must leave its exact caller-owned operation durably Prepared and its
new version nonvisible; a generic CLI error is insufficient evidence. The probe
reads the native primary's typed prepare/canonical cut, without classifying error
text or claiming that every internal failure is a quorum failure. After the real
network heals, the same node, operation ID, bytes and captured ordinary write
options must produce the matching Applied receipt. Cross-node reads, a fresh new
write and node6 process restart/read then verify recovery. Neither partial
metadata publication nor a replacement operation is accepted. Both the baseline
and full lanes use this contract with their existing deadlines and transports.


`dfs-baseline-scope.json`, `REPORT.md` and `report.json` list every required case
and explicitly mark separate codec, CLI latency, payload, raw disk, bulk, large-file
and KV performance distributions as **NOT_RUN**. This lane makes no performance
acceptance claim and does not relax any workload threshold. `SINOMO_TEST_DFS_ONLY=1`
and the default full verifier keep their existing performance gates. The baseline
selector is mutually exclusive with the other focused lanes and requires fresh
artifact paths; `REUSE=1` still means retained-state evidence, not clean formation.

The native metadata roster/expected epochs and pending/activated handoff reports
are retained in `dfs-requested-topology*.json`, the convergence probe artifacts,
and `dfs-handoff/handoff.json`. Six containers do not establish six Raft voters:
default fresh formation has one seed voter and five learners. Multi-voter failure
and rollout/restart evidence requires the separate maintained three-voter rollout
recovery lane. An unrelated running application is never stopped by this lane;
explicitly authorize coexistence with `SINOMO_TEST_ALLOW_OTHER_COMPOSE=1` when needed.

Convergence checks obtain pending handoffs and shard epochs from one authoritative
KV snapshot per probe; the asynchronous on-disk checkpoint is not readiness evidence.

A fresh harness run explicitly requests its six-node metadata assignment through
`sinomo dfs metadata shard set-replicas SHARD --expected-epoch EPOCH --replica NODE`
(repeat `--replica` for every non-primary member), then uses the existing
`transfer-primary` command to place root, services, and users on node1, node2,
and node3. Each phase leaves the active assignment unchanged until the native
retiring fence and joint prefix acknowledgements authorize activation. The
replica command rejects a stale epoch, duplicate replica, or current primary in
the replica list; its existing participant admission also requires authorized
members with the current prepare capability. The harness records intents and
exact activated epochs within its unchanged convergence deadline. Retained
`REUSE=1` projects keep their administrative assignments; discovery never expands
a committed assignment automatically.


Each of the six development nodes has a hard 2 GiB cgroup memory limit and no
swap (12 GiB aggregate node allocation). The verifier checks resolved Compose,
Docker HostConfig and actual cgroup limits. Runtime admission requires 12 GiB of
host MemAvailable; the current driver/host use is already reflected there. A
driver cgroup ceiling is not an additional fixed allocation. Compiler admission
and compiler caps remain separate. Temporary and Docker data filesystems retain
their existing free-byte and reported-inode reserves.

The runtime resource watcher preserves per-node counters and Docker lifecycle
events in `runtime-resources/`. OOM, kernel memory.max boundary events, automatic
restarts and unplanned container exits fail the run immediately. Deliberate
recovery exits require a recorded, node-specific, single-use intent; OOM is
never excused by such an intent. Failed resource evidence must be debugged at
the existing limits, not retried with larger containers.

This resets and removes the selected disposable project's volumes. `KEEP=1`
is spelled `SINOMO_TEST_KEEP=1` and retains it at teardown only;
`SINOMO_TEST_REUSE=1` skips initial reset and retains existing state. The lane
SIGKILLs all six containers for history durability checks and transfers the
services metadata primary. Use a dedicated project. Prebuilt binaries require
`SINOMO_TEST_BUILD=0`, `SINOMO_TEST_BINARY`, and `SINOMO_DOCKER_BINARY` pointing
to the same immutable Linux executable.

Use a separate `CARGO_TARGET_DIR` when building a copied source snapshot. Sharing
the operator checkout's target directory can leave Cargo dependency fingerprints
pointing at the archived source, so edits in the operator checkout are incorrectly
treated as fresh. If an API exists in source but the compiler reports it missing,
inspect the dependency fingerprints and clean the affected workspace crates before
rebuilding; a successful incremental build alone does not establish source identity.

Normal checks run in both segment directions: create, overwrite with shorter
and empty content, Unicode bytes, read through all six nodes, rename, delete,
absence, and path reuse without implicit history. The existing cross-shard
rename check remains included. History checks additionally verify that stale
conditional restores reject without changing current bytes, rename preserves
retained identity, and path reuse creates a distinct object. Retained reads and
recovery also cross the CLI's 64 KiB range boundary after shortening the head.

The DFS lane also runs the `core-only` verifier case. It creates a named
retained revision and an object-scoped attachment, disables
`module.dfs.history` and `module.dfs.attachments` on all six nodes, restarts the
existing processes with their data volumes, and requires the removed routes to
return explicit unknown-route errors. Ordinary create/read/overwrite/rename/
unlink checks still run in both segment directions and are observed on all six
nodes. The case re-enables the components, restarts those processes again, and
checks that the original retained bytes and attachment remain available.
Feature restoration is attempted on every node even after a toggle failure;
partial restoration fails the case. It never resets volumes or reboots hosts.
Before disabling the features, it requires node6's
`cluster.dfs.semantic-lost-reply` probe to select a non-local metadata primary.
That maintained caller discards successful replies from normal routed capture,
conditional restore and release RPCs, recreates the client, and retries the same
operation identities after intervening edits. The case verifies one retained
reference, the original restore outcome without rewinding a newer destination,
and release without resurrecting a reference after unlink/path reuse. This is
client-side successful reply discard, not physical network packet loss.
Run this case against an owned disposable project with the same verified binary:

```sh
python3 docker/test-file-history.py --case core-only \
  --project sinomo-dfs-verification --binary /absolute/path/to/sinomo \
  --artifacts data/operations/dfs-core-only
```

The CLI benchmark compares `ordinary`, `retention`, and `checkpoint` profiles
from node1 and node5 with readers in the opposite segment. It measures create,
overwrite, current read, history listing, and remove; retained profiles also
measure historical reads, restore, and recover; the checkpoint profile measures
capture. Every workload validates bytes and namespace results. Functional
errors fail the lane; latency measurements are informational. Existing bulk
and large-file probes retain their throughput and I/O amplification gates.
The bulk probe selects two current non-primary requesters from node4/node5/node6
so a preceding primary handoff cannot violate its one-shot requester contract.

Configure `SINOMO_TEST_DFS_BENCH_ITERATIONS` (default 8),
`SINOMO_TEST_DFS_BENCH_WARMUP` (2), and `SINOMO_TEST_DFS_BENCH_BYTES` (4096).
Each profile/direction has separate warmup fixtures excluded from statistics.
Under `$SINOMO_TEST_ARTIFACTS/dfs-benchmark`, `benchmark.json` contains raw
samples, command indices, nearest-rank p50/p95/max, and operations/s computed
from summed operation time. These are **host end-to-end CLI timings**, including
Docker exec, CLI startup, RPC, and any read-probe convergence polling; they are
not service-only latency or sustained concurrent throughput. Setup, correctness
reads after mutations, and process hashing are outside mutation timers.
Eight samples are a smoke distribution; use more iterations for comparisons.
Compare identical binary profiles, payloads, topology, and host load. Failed
runs retain partial samples marked `passed: false`; they are not acceptance.

### Ordinary committed write latency

On an already running disposable cluster, the focused write gate measures the
public daemon Files API through its committed response:

```sh
python3 docker/measure-dfs-write-latency.py \
  --project sinomo-dfs-verification \
  --binary /absolute/path/to/sinomo \
  --profile debug \
  --artifacts target/dfs-write-latency/acceptance
```

For repeated measurements, add `--fixtures /path/to/retained/write-latency.json`
to reuse verified fixtures from the same project. The driver archives the input
and hash, and uses the same write probe with fresh payloads and per-operation W/N
checks. This avoids repeating placement configuration setup; readiness, quietness,
sample counts and latency thresholds retain their normal behavior. Record active
history-retention rules when comparing runs, including rules on unrelated paths.

The driver never creates or restarts containers. It checks the exact executable
hashes in all main/shadow processes, runs the existing readiness, daemon mutation,
convergence and continuous 30-second container quiet gates, then measures node1
and node5 against each of the root, services and users shards for **both
W=1/N=2 and W=2/N=3**. This is 12 cases and 24 separate create/overwrite
distributions. Both policies use `replicated-eventual`: W durable acknowledgements
permit committed return, with asynchronous repair toward N replicas. The probe
sets an explicit policy on its run-scoped directory through the normal placement
preview/update API, verifies the applied rule, and verifies each file's effective policy
outside the timer. It never relies on inherited caller or shard defaults.
Setup can refresh an explicitly rejected stale preview up to five times, without
changing the requested policy; ambiguous updates and timed writes are not retried
by the benchmark. The attempt count is retained in probe diagnostics.
The new fixture has no existing content to reconcile; setup records the queued
reconciliation job but does not wait on background repair scheduling.
Each group has 12
excluded warmup iterations and 100 measured creates plus 100 measured overwrites,
with distinct 4096-byte content. Every group's create and overwrite nearest-rank
p95 must be **strictly below 100 ms**. Latency includes client encoding, RPC,
production retries and committed acknowledgement. Docker exec, CLI startup,
payload construction, correctness reads and cleanup are outside the timer.
There is no benchmark retry loop; a failed operation fails the run and remains
in the evidence. Reads immediately after each write must match; the opposite
segment subsequently verifies every final file. Final invariant and executable
identity checks must also pass. Fixtures and the project are retained.

`write-latency.json` contains separate per-policy/origin/shard/operation distributions
and ordered raw samples, including warmup and failed operations. Command stdout,
stderr, command deadlines, source patch, runner sources and binary hashes remain
in the artifact directory. `--max-p95-us 0`, reduced `--iterations`, `--policies`, `--origins`,
`--shards`, or `--skip-readiness` are diagnostic experiments; their report marks
the reduced acceptance scope. `SINOMO_TEST_COMPOSE_OVERRIDE` must match the
override used to create the project, including any tmpfs experiment. Ordinary
acceptance uses the normal durable data volumes.

For a short diagnostic run of one origin/shard, the underlying probe is:

```sh
docker compose -p sinomo-dfs-verification -f docker/docker-compose.dev.yml exec -T node1 \
  sinomo test run cluster.dfs.write-latency --require-cluster --json \
  --param replication=w2-n3 \
  --param base=/services/cluster-tests --param iterations=100 \
  --param warmup-iterations=12 --param payload-bytes=4096 --param max-p95-us=0
```

Unlike the retaining Docker driver, this standalone probe cleans its run-scoped
fixture after measurement. `--param retain-fixture=true` preserves the paths and
payload seeds reported with samples for independent verification.

### Generated verification reports

The main `test-cluster.sh` harness, `test-file-history.py` and write-latency driver
automatically produce **`REPORT.md` and `report.json`**, including failed and
partial runs. The main report links generated lane reports. The write report
uses the fixed policy matrix and standard acceptance, per-node stage/I/O,
node-window and coverage tables. The write runner automatically invokes
`profile-dfs-writes.py` after the latency matrix to supply detailed stage/I/O/CPU
reports for both policies, both callers and all shards. Missing measurements
remain `unavailable`; the report does not substitute older capture data or zeros.
History benchmark tables preserve their host CLI timing scope.

Use these generated artifacts directly when reporting results. The JSON includes
the raw result digest and reporter digest; the reporter is archived beside the
results. To render saved evidence again without running any workload:

```sh
python3 target/dfs-write-latency/acceptance/verification_report.py \
  target/dfs-write-latency/acceptance
```

Reuse the Observe capture/report tools and their generated `PERFORMANCE.md`,
`CREATE.md` and `OVERWRITE.md`. Debug logging, DFS and generic tracing, a 32768-record
ring, formation observation on all nodes, and CPU profiler support are enabled by
default. All gates retain their existing thresholds with these enabled; use the
observed timings directly and do not subtract estimated overhead. The gate
preflight rejects missing debug/tracing/profiler configuration. Reduced workloads,
skipped gates or `--no-stage-profile` are diagnostic only. Extend the maintained
tools when a measurement is missing, rather than devising a new method each run.

The Docker harness builds with CPU profiler support. When supplying a prebuilt
binary, use the same required feature selection:

```sh
cargo build --locked --offline -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
```

Both package selections are required to select the observation crate's feature.
Recreate the diagnostic containers using that executable and verify the main
process hashes before capture. To profile an existing default-feature binary
without replacing its processes, use the opt-in `perf` sidecar described under
[Opt-in profiling sidecar](#opt-in-profiling-sidecar); trace control works with either build.

For the enabled in-process profiler, start a bounded CPU
capture on the writer daemon and metadata primary identified by
`cluster.dfs.shard-layout`, and enable `sinomo.dfs.perf` with the ordinary
observation API. For example, on each selected container:

```sh
sinomo observe control --capture dfs-write-profile --filter 'sinomo.dfs.perf=trace' --json
sinomo observe profile cpu start --capture dfs-write-profile --duration-seconds 30 --frequency-hz 99 --json
# Run the short write probe concurrently with this capture.
sinomo observe profile cpu stop --capture dfs-write-profile --json
sinomo observe traces --capture dfs-write-profile --limit 4096 --json
sinomo observe control --clear-capture --expected-capture dfs-write-profile --json
sinomo observe profile cpu status --json
```

After CPU status reaches `complete` (an `unsupported` status is not a capture), copy
`/data/observe/profiles/dfs-write-profile/` from that container and render
`cpu.pprof.pb` with `scripts/debug/render-pprof-flamegraph.py`. Keep the profile
metadata, trace JSON and workload output together. The Docker trace ring holds
32768 records; use the standard collector and retain logs for longer workloads. The Compose
`SINOMO_DFS_PERF_TRACE=1` setting enables the same target at process startup.
Keep debugging, tracing and profiling enabled for final acceptance at unchanged thresholds.

Each case records binary/process identity, source patch, Docker context,
resolved Compose configuration, commands, output, and command wall times.
Fixtures and retention rules remain available when the project is retained;
normal disposable teardown removes them. No-Docker harness regression checks:

```sh
python3 docker/test-dfs-harness.py
```

## Focused local network checks

Use the existing cluster harness to list or preview cases without Docker or a
Rust build:

```bash
SINOMO_TEST_NETWORK_LIST=1 bash docker/test-cluster.sh
SINOMO_TEST_NETWORK_CASES=ready,bridge SINOMO_TEST_NETWORK_PLAN=1 \
  bash docker/test-cluster.sh
```

`SINOMO_TEST_NETWORK_CASES` accepts comma-separated cases: `ready`, `bridge`,
`restart`, `restart-staggered`, `latency`, `payload`, and `attachments`.
Profiles expand to cases: `smoke` = `ready`; `recovery` = readiness, bridge and
both restart variants; `performance` = latency, payload and attachments.
Unknown names and combinations with other lane selectors fail before execution.
These selectors default to **no Rust build**. An explicit `SINOMO_TEST_BUILD=1`
builds the default host binary; a missing prebuilt binary otherwise fails.

Build once, then use the same immutable binary on a dedicated test project:

```bash
export SINOMO_TEST_PROJECT=sinomo-network-debug
export SINOMO_TEST_BINARY=/absolute/path/to/sinomo
export SINOMO_DOCKER_BINARY="$SINOMO_TEST_BINARY"
SINOMO_TEST_NETWORK_CASES=smoke SINOMO_TEST_KEEP=1 bash docker/test-cluster.sh
SINOMO_TEST_NETWORK_CASES=bridge SINOMO_TEST_REUSE=1 bash docker/test-cluster.sh
```

The first command resets this disposable project's volumes. The second retains
and reuses them. Use the same `SINOMO_TEST_COMPOSE_OVERRIDE` throughout when a
network override is needed. `SINOMO_TEST_KEEP_ON_FAILURE=1` retains a fresh
project after failure; reuse always retains it. Injected bridge cuts are restored
on ordinary failure or interruption. An uncatchable process kill can still need
manual recovery; the exact network/container command is retained in evidence.

Focused preflight checks the six-node segmented topology, Iroh-disabled
configuration, unique identities, and exactly one main/shadow process pair per
node matching the selected binary hash. A mounted executable alone is not proof.
The `ready` and `bridge` cases do not run DFS acceptance or benchmarks. Performance
cases keep their existing DFS convergence/quietness prerequisites, and restart
cases retain the current/historical DFS byte assertions with three crash cycles.

Network readiness requires three consecutive complete sweeps of all 30
ordered pairs within the same 120-second default deadline. Each attempt uses
zero warmup and verifies 1 KiB request/response payloads through the production
RPC path. Every origin is rechecked; earlier success is not permanent acceptance.
The bridge case verifies all 12 within-component pairs succeed and all 18
cross-cut pairs return failed RPC reports, then requires full recovery. Missing,
skipped, partial, or host-timed-out reports cannot establish partition behavior.

The read-only `cluster.network.connectivity` runtime probe uses locally observed
membership solely to choose diagnostic targets. It remains callable without KV
quorum and verifies the local daemon identity; routed calls retain normal daemon
authentication/authorization. It does not prove current membership authority.
The existing `cluster.network.ping-benchmark` retains authoritative refresh and
quorum admission. Reported route classifications are currently predicted paths,
not actual executed-route traces.

Under `SINOMO_TEST_ARTIFACTS`, selected runs save `result.json`, per-case exit
codes, the harness sources/patch, process hashes, topology, identities, every
command's stdout/stderr, and monotonic sweep events. Each attempt has a distinct
file; reruns need a fresh artifact directory. The original failure remains saved
even when later sweeps recover. Source revision describes the harness checkout;
a prebuilt binary's provenance is its recorded hash, not an inferred build revision.

The standalone `docker/test-bridge-recovery.sh` uses the same traffic checks and
supports `SINOMO_BRIDGE_PROJECT`, `SINOMO_BRIDGE_ARTIFACTS`, the selected binary,
and the Compose override. Its positional recovery deadline defaults to 90 seconds.
Baseline and outage observation retain their 120/45-second default budgets
(`SINOMO_BRIDGE_BASELINE_WAIT` / `SINOMO_BRIDGE_OUTAGE_WAIT`). It retains the
cluster and requires the standard node4 bridge topology.

No-Docker regression checks:

```bash
python3 docker/test-network-harness.py
python3 docker/test-dfs-harness.py
```

## Restart readiness regression

On a dedicated cluster that has passed `SINOMO_TEST_DFS_READY_ONLY=1`, run:

```sh
python3 docker/test-file-history.py --case restart \
  --project sinomo-restart-verification --binary /absolute/path/to/sinomo \
  --artifacts target/restart-verification --restart-cycles 3 \
  --restart-stagger-seconds 4
```

This SIGKILLs all six containers per cycle, restarts the existing volumes, and
optionally delays bridge node4 after its peers start. The standard formation,
network, DFS readiness, mutation, and convergence gates run after each restart.
Current bytes and an explicitly retained version must survive every cycle.
Use `--restart-stagger-seconds 0` (the default) for simultaneous restarts.
The delay is fault injection, not an extension to the readiness deadline.
Binary/mount and Compose override requirements are the same as the history lane.

## File-history acceptance

See the [user guide](../docs/user/file-history.md) for commands and the
[developer guide](../docs/developer/dfs-file-history.md) for retention,
durability, and restore contracts.

On a disposable project that passed the DFS readiness lane, use the same
immutable host binary for the file-history verifier:

```sh
python3 docker/test-file-history.py \
  --project "$SINOMO_TEST_PROJECT" \
  --binary "$SINOMO_DOCKER_BINARY" \
  --artifacts target/file-history-acceptance \
  --restart-cluster
```

The verifier checks default no-retention behavior, independent pins, historical
reads, conditional restore and exact retries, deleted-file recovery, path reuse,
and rule-based captures through different nodes. `--restart-cluster` authorizes
SIGKILL of all six containers, followed by startup with their existing volumes
and the standard readiness gates. It checks completed history and restore
receipts across those restarts; it is not a fault injection at every WAL boundary
or proof of incomplete-replication failover. The project is retained for review.
Commands, output, binary/process hashes, source patch, and the final result are
written to the artifact directory. Standalone `--case normal` and
`--case benchmark --iterations 32 --warmup 4 --payload-bytes 4096` use this same
driver. Standalone execution requires prior readiness; benchmark quietness is
enforced by the integrated suite, not the Python driver alone.
The default requires zero observed write retries. `--max-write-retries 2` runs
a separately labelled functional profile; it retains the same write deadline
and standard readiness gates and does not establish zero-retry acceptance.
Single-file writes, including `cluster.dfs.write`, wait for W and canonical
publication while keeping background replica repair. History fixtures require
that committed outcome before capturing. Path queries use normal pages;
the verifier separately exercises one-record pagination over two captures of
the same object.

If a restart readiness gate fails immediately after the post-restore write,
the verifier leaves `checkpoint.json`. After diagnosing and restoring readiness,
`--resume-from /path/to/checkpoint.json` can check the durable state and continue
with the same project and executable. The report records the resumed run; this
does not turn the original failed readiness gate into a pass. A checkpoint is
not a restart point for later steps that have already mutated the fixture.

`--case handoff` transfers the disposable project's `services` metadata primary
to an existing replica through the fenced handoff protocol. Before transfer it
opens the destination's history mutation lane while that node is still a replica.
It waits for the assignment, exercises the new authority with an ordinary
committed write, waits for the standard readiness/mutation/convergence gate,
then verifies capture, retained reads, and recovery through the new primary.
The JSON report preserves both the pending request and the final assignment.
This is completed-content handoff coverage; it does not inject a failure during
incomplete content replication.

## Storage conversion acceptance

DFS opens the current native storage format. `sinomo-dfs-migration` owns
retained-format decoding and incremental conversion. The ordinary component
factory registers one storage preparation provider while retaining its actual
data owner; that provider prepares metadata and content roots before routes
are admitted. A pending conversion is a typed startup condition. Every cluster
node must upgrade to the current protocol before it can participate.

Use disposable, operator-owned volumes for upgrade acceptance. Retain the
baseline executable hash, current file identities, retained history identities,
and the source data. Replace executables while those nodes are stopped, then
verify actual converter progress and inspect current root schema and certified
prefixes before accepting the candidate. Confirm ordinary current reads,
retained reads, writes, and exact recovery replay after restart with the history
acceptance driver. Keep converter execution evidence separate from successful
network reads and from current protocol capability reports.

The source converters have filesystem interruption and bounded-record tests in
the migration crate. Running those checks validates that source boundary; cluster
acceptance also requires actual converter reports from every owned node. Preserve
failed or unavailable inspection results instead of treating readiness as a
physical conversion receipt.

## Opt-in profiling sidecar

`docker/docker-compose.profile.yml` adds a disposable `perf` sidecar without
putting profiling tools or privileges in the six node containers. It uses the
host PID namespace, mounts the same host-built Sinomo binary for symbols, and
writes captures under `target/network-profiles/` by default:

```bash
mkdir -p target/network-profiles
docker compose -p sinomo-dfs-test \
  -f docker/docker-compose.dev.yml \
  -f docker/docker-compose.profile.yml \
  up --build -d profiler
```

Resolve a daemon's host PID with `docker top`, then record userspace stacks
from the profiler container. On Intel hybrid hosts, select one PMU explicitly:
generic `cycles` expands to both `cpu_core` and `cpu_atom`, which can create a
high-overhead perf wakeup loop when combined with LBR. LBR supplies low-overhead
call chains; the `:u` suffix excludes kernel stacks, which are not needed for
object-RPC CPU attribution:

```bash
docker top sinomo-dfs-test-node4-1 -eo pid,args
docker compose -p sinomo-dfs-test \
  -f docker/docker-compose.dev.yml \
  -f docker/docker-compose.profile.yml \
  exec profiler perf record -F 99 -e cpu_core/cycles/u -g --call-graph lbr \
    -p <daemon-host-pid> -o /profiles/node4-rpc.data -- sleep 20
```

Use `perf list` to select another userspace event on hosts without
`cpu_core/cycles` or LBR support. DWARF call graphs are portable but materially
more intrusive for sub-millisecond latency work.

Generate a text report or the perf-provided interactive flame graph inside the
same sidecar:

```bash
docker compose -p sinomo-dfs-test \
  -f docker/docker-compose.dev.yml \
  -f docker/docker-compose.profile.yml \
  exec profiler perf report --stdio -i /profiles/node4-rpc.data
docker compose -p sinomo-dfs-test \
  -f docker/docker-compose.dev.yml \
  -f docker/docker-compose.profile.yml \
  exec profiler sh -lc \
    'cd /profiles && ln -sf node4-rpc.data perf.data && \
     perf script -s /usr/lib/perf/scripts/python/flamegraph.py -- \
       --allow-download -f html -o node4-rpc.html'
```

The override grants `PERFMON` and `SYS_PTRACE`, exposes host PIDs, and relaxes
seccomp for the profiler only. Remove the sidecar when profiling is complete:

```bash
docker compose -p sinomo-dfs-test \
  -f docker/docker-compose.dev.yml \
  -f docker/docker-compose.profile.yml \
  rm -sf profiler
```

## Topology

```
     seg_a                         seg_b
  ┌───────────┐                 ┌───────────┐
  │ node1(seed)│                 │   node5   │
  │   node2    │   seg_ab        │   node6   │
  │   node3 ●──┼──●──────●──┼──● node4 ●     │
  └───────────┘   node3   node4 └───────────┘
   1,2,3 mutually        3<->4         4,5,6 mutually
     reachable        (only bridge)      reachable
```

| service | networks | role |
|---|---|---|
| node1 | `seg_a` | seed |
| node2 | `seg_a` | joiner |
| node3 | `seg_a`, `seg_ab` | joiner + **bridge A↔AB** |
| node4 | `seg_ab`, `seg_b` | joiner (via node3) + **bridge AB↔B** |
| node5 | `seg_b` | joiner |
| node6 | `seg_b` | joiner |

Docker's embedded DNS only resolves peers on a **shared** network, so the segmentation is
real: `node1` cannot even resolve/reach `node5`. The seed quorum-publishes one reusable invite
with all three introducer channels, then writes a compact one-channel reference for each
segment. Each node therefore joins through a *reachable* introducer (node2,3→`node1`;
node4→`node3`; node5,6→`node4`) without three serial invitation transactions.

## iroh is disabled on purpose

Every node sets `SINOMO_DISABLE_IROH=1`. With iroh off there is **no relay/hole-punching
to paper over the segmentation** — peers can only talk over direct LAN-TCP within a shared
network. The daemon logs `iroh disabled (SINOMO_DISABLE_IROH)` and advertises **no**
`iroh://` endpoint.

## Expected state: all 6 nodes cohere (via native relay)

Cross-segment cohesion works **without iroh**: a non-voter bridge forwards a joiner's
challenge + registration to a voter, and cluster control traffic is relayed hop-by-hop over
the connectivity graph's **healthy** links. So every node integrates into one cluster:

- `{node1,node2,node3}` form directly in `seg_a` (node1 is the voter).
- `node4` joins across `seg_ab` through `node3` — node3 relays node4's join challenge +
  registration to the voter node1.
- `node5,node6` join through `node4`, **two bridges deep**: their joins relay
  `node5/6 → node4 → node3 → node1`.
- `sinomo cluster health` on node1 shows **all 6** main daemons (node1 voter + 5 learners);
  cross-segment members read `offline` there only because node1 cannot dial their segment
  directly — they are full members.

Joins are best-effort and time-bounded, so **no node hangs**; each joiner waits for the dev
harness readiness marker, then runs one cluster join command. Clean six-node formation has a
non-configurable 60-second bounded acceptance gate. Crossing it still indicates a startup, join,
restart, or relay bug even though benchmark work need not stop to optimize formation below it.

## Layout

- `docker/Dockerfile` — full node image (Rust toolchain + source + built `sinomo`);
  used by the root production compose and the single-node smoke compose.
- `docker/Dockerfile.dev` — thin runtime image for the dev cluster (host binary
  bind-mounted in).
- `docker/entrypoint.sh` — best-effort multi-hop formation (seed publishes per-segment
  compact invite references → joiners form through the bridges), then runs the shadow supervisor
  (PID 1). Shared by all
  composes.
- `docker/docker-compose.dev.yml` — 6 services + 3 bridge networks (`seg_a`,
  `seg_ab`, `seg_b`).
- `docker/single-node-entrypoint.sh` — single-node bootstrap and smoke runner.
- `docker/Dockerfile.profiler`, `docker/Dockerfile.pjdfstest`, and
  `docker/Dockerfile.android` — images for the profiling sidecar, the pjdfstest
  `node5` overlay, and the Android builder.
- `docker/lib/network_checks.py` — shared traffic/evidence checks for the
  focused network cases and `test-bridge-recovery.sh`.

Each node: main daemon `0.0.0.0:21353`, shadow `0.0.0.0:21354`, own `/data` volume, and a
shared `/shared` volume that carries the per-segment invite files. `restart:
unless-stopped` lets Docker bring a node back when rollout intentionally replaces the
shadow supervisor running as PID 1.

## Usage

For rollout recovery verification, `docker/test-rollout-recovery.sh` uses this
segmented topology with three voters. Supply compatible Linux baseline
and candidate binaries at different commits. The baseline must support the
daemon-mediated `cluster promote` control route
used to establish the three-voter fixture. The harness creates a fresh project,
disconnects a target during activation, interrupts and resumes a coordinator,
checks that a completed node is not activated twice, and verifies custom main
and shadow ports. It retains evidence and removes only its own project unless
`SINOMO_ROLLOUT_TEST_KEEP=1` is set.

```bash
SINOMO_ROLLOUT_BASELINE=/absolute/path/to/baseline-sinomo \
SINOMO_ROLLOUT_CANDIDATE=/absolute/path/to/candidate-sinomo \
SINOMO_ROLLOUT_CANDIDATE_COMMIT=FULL_CANDIDATE_COMMIT \
  bash docker/test-rollout-recovery.sh
```

The helper scripts under `docker/` export `COMPOSE_FILE` pointing at the dev
cluster, so they work from any directory; plain `docker compose` commands need
`-f docker/docker-compose.dev.yml` (shown as `-f …dev.yml` below):

```bash
cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler

# Build the runtime image and start all 6 nodes.
docker compose -f docker/docker-compose.dev.yml up --build -d

# Watch formation.
docker compose -f docker/docker-compose.dev.yml logs -f node1
docker compose -f docker/docker-compose.dev.yml logs node4   # cross-segment join

# Cluster view from each segment.
docker compose -f docker/docker-compose.dev.yml exec node1 sinomo cluster health   # expect all 6
docker compose -f docker/docker-compose.dev.yml exec node5 sinomo cluster status   # expect: integrated

# Reachability sanity (segmentation): from node1, node3 resolves, node5 does not.
docker compose -f docker/docker-compose.dev.yml exec node1 getent hosts node3
docker compose -f docker/docker-compose.dev.yml exec node1 getent hosts node5 || echo "unreachable (expected)"

docker compose -f docker/docker-compose.dev.yml ps        # container state (all Up)
docker compose -f docker/docker-compose.dev.yml down -v   # tear down + wipe volumes
```

## Automated DFS correctness suite

`docker/test-cluster.sh` runs the cluster-wide POSIX/DFS correctness suite in a
**dedicated compose project** (`sinomo-dfs-test` by default, so it never touches a
manually started dev cluster): it builds the current host binary by default, brings up the
6 nodes, waits for clean formation, then drives the in-binary `cluster-dfs` runtime
test cases from different nodes:

```bash
docker/test-cluster.sh                       # build current binary, up, test, down -v
SINOMO_TEST_BUILD=0 docker/test-cluster.sh   # reuse an existing host binary
SINOMO_TEST_BUILD=0 SINOMO_TEST_REUSE=1 \
  docker/test-cluster.sh                     # reuse the running test project
SINOMO_TEST_KEEP=1  docker/test-cluster.sh   # leave the cluster up afterwards
```

Reuse refuses to rebuild: it requires `SINOMO_TEST_BUILD=0` and the verified,
profiler-enabled binary that the running project already mounts.

To diagnose formation without extending or weakening its 60-second gate, enable the
bounded observation mode and stop after formation:

```bash
SINOMO_TEST_FORMATION_ONLY=1 \
SINOMO_TEST_FORMATION_OBSERVE=1 \
docker/test-cluster.sh
```

The entrypoint enables the formation trace filter before each observed daemon starts, then
schedules capture tagging and network timelines independently for every main-daemon PID,
including identity-changing join replacements. In-process CPU profiling defaults to all nodes
(`SINOMO_TEST_FORMATION_OBSERVE_CPU_NODES=all`). It preserves the early
process-local trace ring instead of clearing it when capture tagging arrives. Observer startup
runs off the readiness dependency path so a slow local observation RPC cannot delay the next
bridge. At the end of the hard gate, the harness freezes every node's capture concurrently,
writes both `traces.json` (capture-tagged) and `all-process-traces.json` (including pre-tag
startup events), and then copies every daemon generation's `*.startup-traces.json` into the
node's `startup/` artifact directory. A joining daemon also finalizes its explicitly active
network timeline during graceful identity replacement, under
`observe/timelines/<capture>/timeline.json`. Those preserved client-side captures include the
raw bootstrap open/write/read stages, the propagated trace identity used to correlate the remote
ingress event, and a stable endpoint correlation ID mapped back to the exact endpoint and node.
The harness copies and renders every generation under `network-timeline-artifacts/`, instead of
silently losing the joining process when only its replacement remains reachable. The complete artifacts land in
`target/cluster-test-artifacts/<run>/formation-observe/`. The harness also renders each copied
`cpu.pprof.pb` as a sibling `flamegraph.html`, writes a per-node
`network-timeline-summary.txt`, and combines trace plus network events into
`formation.perfetto.json`. Health traces include each request's loaded KV revision/member count,
live-probe batch, peer-query durations, and whether the handler completed or was cancelled. The
gate-side `cluster-ready-formation.attempts.tsv` records each CLI attempt's absolute and relative
timing, timeout, exit code, and online count so client cancellation can be aligned with those
server spans. Rendering is diagnostic-only and never changes the hard-gate result;
the helpers remain available under `scripts/debug/` for manual rerendering. Shorten
`SINOMO_TEST_FORMATION_OBSERVE_DURATION_SECONDS` when a joining daemon is expected to exit
before the default twelve-second profile can finalize.

Observed formation is the default acceptance configuration. Main and shadow
processes retain debug/tracing filters, and all main daemons collect bounded CPU
profiles. Formation, network, DFS, quietness and latency gates must pass without
disabling observation or changing thresholds. An explicit observation-off override
is a diagnostic comparison, not acceptance.

After transport readiness and DFS metadata-shard placement have converged, and before each
no-load realtime network leg, the benchmark requires one uninterrupted 30-second monotonic
interval in which every one of the six service containers remains at or below 10 percent CPU
and 65,536 bytes/second of combined TX plus RX over all interfaces, including loopback.
DFS convergence is deliberately outside the Docker-only 60-second cluster-formation gate,
but it is a prerequisite for quietness: otherwise final shard handoff commits can race the
idle-load measurement. The harness searches for the sustained quiet interval for at most 120
seconds. One continuous `docker stats` stream supplies cumulative per-container CPU intervals; the harness consumes
every interval rather than treating time between polls as quiet. A busy interval, malformed or
missing row, stream exit, or five-second stream stall resets the full duration. Docker's
aggregate container CPU is the authority, so main daemons, shadow supervisors, entrypoint
helpers, and any short-lived process inside the container are all charged to the gate. The
harness snapshots the exact main/shadow PID plus process start tick before and after the
candidate interval; any unavailable or changed generation resets the complete duration.
The same stats groups trigger read-only `/proc/net/dev` counter captures through Docker exec;
exact integer deltas, not Docker's rounded NetIO text, determine network eligibility. The
existing main/shadow scanner also verifies that both roles share the sampled network namespace.
Missing Linux counters, resets, a changed namespace or interface inventory, and stale samples
are unavailable evidence and reset the whole window. This works through the selected Docker
context for Linux containers; unsupported counters fail closed. It never queries another project.
Network captures use the same monotonic window and inherited 120-second deadline. After the
final network endpoint is sealed, the continuous CPU stream also covers the read-only collector
exits. The network window ends at that sealed joint cut; later CPU-only cleanup is reported
separately. All observation work within the window remains charged; no overhead is subtracted. The
stats stream remains active during the final generation scan, and the gate cannot pass until a
quiet cumulative interval for every container covers through the end of that scan. It writes
every per-container interval, wall-clock timestamp, and monotonic coverage boundary to
`network-quiet-<leg>.samples.tsv`, the generation evidence to
`network-quiet-<leg>.generations.tsv`, and the pass/fail decision to
`network-quiet-<leg>.summary.txt` under the run artifact directory. In-process pprof captures
and their flamegraphs cover only the daemon process that serves the observe RPC; they do not
cover shadow or helper processes and are never substituted for the aggregate quiet gate.

Each quiet gate also launches a bounded idle diagnostic in
`idle-<leg>/REPORT.md`. It uses the standard Observe collector and reporter,
captures CPU profiles on all six main daemons, and records background-work
counter deltas alongside stage/I/O, per-route RPC and process-window tables. Collection runs
during the gate and its overhead is included. The generated verification report
copies the gate verdict, per-node CPU stream summaries and exact network interval summaries.
Raw all-interface endpoints and unavailable rows are retained under
`network-quiet-<leg>.network/`. Trace loss or missing counters remains explicit, independently
of the CPU gate verdict. To investigate an already running, verified cluster,
use `python3 docker/profile-idle.py --help`; the driver never builds or changes
cluster lifecycle.

`SINOMO_TEST_QUIET_ONLY=1` selects readiness plus the same standard quiet gate,
without the mutation probes or benchmarks. Use it with `SINOMO_TEST_REUSE=1`
and `SINOMO_TEST_BUILD=0` to inspect recovery on an already verified candidate.
It is mutually exclusive with the other focused lanes. The generated report
labels this scope explicitly; it does not establish write/benchmark acceptance.

The network baseline keeps embedded-SOBJ payload throughput and semantic-attachment
throughput as separate measurements. The attachment leg uses small request/response SOBJ
heads plus streamed 8 MiB attachments in each direction, four calls at a time by default.
It validates the bounded head and the declared and delivered byte counts without
adding a content hash or transport-integrity layer. Tune it with the
`SINOMO_TEST_NETWORK_ATTACHMENT_*` variables documented at the top of
`docker/test-cluster.sh`; each direction is capped at 64 MiB by the benchmark route.

For a durable saturation run, set `SINOMO_TEST_NETWORK_SATURATION=1`. This optional
profile is off by default and runs from one origin (`node1`) to all five peers with
64 MiB in each direction, 128 measured requests per target, and concurrency 8. That is
80 GiB of measured aggregate duplex logical traffic: 40 GiB per direction, or
about 3.4 seconds of ideal 100 Gb/s occupancy per direction (about 6.9 seconds
if the directions serialize), rather than reporting a short burst. The
`SINOMO_TEST_NETWORK_SATURATION_ORIGIN`, `_REQUESTS_PER_TARGET`,
`_WARMUP_REQUESTS_PER_TARGET`, `_CONCURRENCY`, `_TIMEOUT_MS`, `_REQUEST_BYTES`, and
`_RESPONSE_BYTES` variables tune the profile while retaining the 64 MiB route cap.
The ordinary all-node 8 MiB attachment leg remains the default acceptance smoke.

## External pjdfstest compatibility suite

`docker/test-pjdfstest.sh` mounts SIFS through the public FUSE interface on
`node5` and runs the upstream pjdfstest syscall suite from that mount. This is
an external, kernel-facing compatibility measurement; it does not call the
in-binary DFS test registry. The selected tests run first on a separate native
Docker volume as a control, so a broken image, Perl/TAP installation, or Docker
runtime is not misreported as an SIFS compatibility boundary.

The default is a complete report-only run. SIFS assertion failures are retained
as compatibility evidence without failing the command, because SIFS currently
implements a documented subset of POSIX. Harness failures always fail,
including a failed native control, unavailable FUSE mount, malformed report,
leaked kernel mount, or unhealthy post-suite cluster. A SIFS worktree that
cannot be removed is retained as compatibility evidence; the default teardown
still removes the run's isolated Docker volumes. Strict mode also turns an
incompatible or incomplete SIFS result into a failing exit status.

```bash
# Discover and record the complete currently implemented boundary.
docker/test-pjdfstest.sh
docker/test-pjdfstest.sh --full --report

# Faster iteration, or a zero-known-failure acceptance gate.
docker/test-pjdfstest.sh --smoke --report
docker/test-pjdfstest.sh --smoke --strict

# Equivalent environment controls.
SINOMO_PJDFSTEST_PROFILE=full \
SINOMO_PJDFSTEST_MODE=report \
SINOMO_PJDFSTEST_TIMEOUT_SECONDS=3600 \
docker/test-pjdfstest.sh
```

The wrapper selects `docker/docker-compose.pjdfstest.yml` automatically. Its
extra image and privileges apply only to `node5`: `/dev/fuse` is exposed and
`SYS_ADMIN` is added without making the container fully privileged. The host
must provide `/dev/fuse`. A named native-control volume is mounted separately
from SIFS and node state.

Raw TAP/prove output, normalized per-test/per-family assertion counts, upstream
revision, portability patchset, mount evidence, cleanup evidence, cluster
health, and wrapper environment land under
`target/pjdfstest-results/<run>/` by default. The
harness first selects a root, services, or users path whose metadata primary is
provably remote from the `node5` mount while `node5` is a configured metadata
replica. Through FUSE it then creates ordinary zero-byte `O_CREAT` and
`O_CREAT|O_EXCL` files, repeats both calls, requires the second exclusive create
to return `EEXIST`, and checks both sizes remain zero.
After the suite, distant nodes read both files, `node5` is restarted, and the
files are read again from the restarted node and another peer before a
cross-node remove/absence check. The placement selection, syscall results,
cross-node visibility, restart, and cleanup have separate JSON evidence files.

Each repeated mounted syscall has a fresh kernel request identity; the harness
records that fact explicitly and does not claim to inject an exact distributed
operation replay. Stable-identity retry and response-loss replay remain DFS
integration-test responsibilities. If the mounted interface cannot complete
the zero-byte probe, the result records the limitation instead of hiding the
rest of the syscall boundary. Missing or local-only metadata-primary coverage
is a harness failure. In either case, the harness waits for metadata
convergence and reruns every-node DFS invariants, cluster doctor, and routed
status. Mount cleanup is attempted even when `SINOMO_TEST_KEEP=1` or
`SINOMO_TEST_REUSE=1` leaves the cluster running.

The image pins upstream revision
`ededbeb2b44929972898afb87474b0937f78a877` and applies the reviewed
`sinomo-linux-portability-v1` contract. That contract replaces active upstream
TAP TODO directives with explicit expectations:

- directory ownership changes may preserve or clear set-id bits, while uid and
  gid must still match exactly;
- `chown(-1, -1)` may preserve or advance ctime, but cannot move it backwards;
- Linux `EISDIR` for `unlink(directory)` is recorded as a host-platform
  deviation alongside the POSIX outcomes;
- Btrfs's fixed directory link count of one is recorded as a host-filesystem
  deviation while rename and dot-dot inode behavior remain mandatory.

The machine-readable contract is embedded in `compatibility.json`. Any
remaining TAP TODO directive is reported with its assertion number and reason
and fails the harness as an unresolved test contract, even in report mode.

Interpret the result as compatibility for the exact Linux mounted-interface
profile that was executed, not as general POSIX certification. Internal tests
remain necessary for distributed durability, replication, failover, repair,
and authority contracts outside pjdfstest's syscall scope.

## Cluster KV benchmark suite

`docker/benchmark-cluster-kv.sh` builds and forms the same segmented six-node
debug cluster (project `sinomo-kv-benchmark` by default), waits for routing,
DFS background work, and whole-container CPU to stabilize, then runs only the
daemon-mediated cluster-KV benchmark suite (`SINOMO_TEST_CLUSTER_KV_ONLY=1` in
`test-cluster.sh`). It does not benchmark an in-process store or read snapshot
files from the CLI.

```bash
docker/benchmark-cluster-kv.sh

# Reuse the current host binary or an already-running benchmark project.
SINOMO_TEST_BUILD=0 docker/benchmark-cluster-kv.sh
SINOMO_TEST_BUILD=0 SINOMO_TEST_REUSE=1 SINOMO_TEST_KEEP=1 \
  docker/benchmark-cluster-kv.sh
```

The default matrix runs from `node1` and the deeper segmented `node5` path:

- one 256-byte put per quorum commit;
- 32 256-byte puts in one quorum transaction;
- one 16 KiB put per quorum commit;
- a local materialized prefix read returning 64 entries;
- the same prefix read after a fresh current-term quorum barrier.

Warmups are excluded from every distribution. Each measured response is checked
for operation count, revision monotonicity, key ordering, and exact value bytes;
any failed operation fails the suite. Latency and throughput thresholds default
to informational mode, while `SINOMO_TEST_CLUSTER_KV_MAX_FAILURES=0` remains a
functional gate. Tune the matrix with the `SINOMO_TEST_CLUSTER_KV_*` variables
documented at the top of `docker/test-cluster.sh`.

Raw per-profile JSON, aggregated `summary.json`/`summary.tsv`, the complete
harness log, environment metadata, quiet-gate evidence, and the tested binary
hash are written under `benchmark-results/cluster-kv/<run>/`. This directory is
gitignored and should not be committed.

Scenarios: per-node DFS invariants, read-your-write roundtrips (both segments),
cross-segment write→read in both directions plus a routed delete, a
/users→/services rename issued by a non-writer node, cross-segment deterministic
bulk write/read throughput, member restart durability, and a final invariant
sweep. Failure evidence (probe JSON, compose ps, cluster health/doctor, per-node
logs) lands in `target/cluster-test-artifacts/<run>/`.

The scenarios are composed from runtime test cases registered in the binary
itself (list them with `sinomo test list --tag cluster-dfs`), so any probe can
also be run by hand on any live node:

```bash
docker compose -p sinomo-dfs-test -f docker/docker-compose.dev.yml \
  exec node5 sinomo test run cluster.dfs.read-your-write --require-cluster --json
docker compose -p sinomo-dfs-test -f docker/docker-compose.dev.yml \
  exec node2 sinomo test run cluster.dfs.expect-file --require-cluster \
  --param path=/services/x.txt --param payload=hello --param timeout-seconds=60
docker compose -p sinomo-dfs-test -f docker/docker-compose.dev.yml \
  exec node2 sinomo test run cluster.dfs.bulk-write --require-cluster --json \
  --param base=/services/perf-a --param files=128 --param bytes=4096 \
  --param timeout-seconds=120
docker compose -p sinomo-dfs-test -f docker/docker-compose.dev.yml \
  exec node6 sinomo test run cluster.dfs.bulk-expect --require-cluster --json \
  --param base=/services/perf-a --param files=128 --param bytes=4096 \
  --param timeout-seconds=120
```

The bulk-performance workload first runs a no-threshold warmup leg, then
defaults to 64 measured files of 4096 bytes in each cross-segment direction and
records elapsed time, file throughput, byte throughput, and exact payload
verification in the JSON probe reports. The default Docker write floor is 0.75
files/s and the default cross-node read/convergence floor is 1 file/s. Increase
or gate it with
`SINOMO_TEST_PERF_WARMUP_FILES`, `SINOMO_TEST_PERF_FILES`,
`SINOMO_TEST_PERF_BYTES`, `SINOMO_TEST_PERF_TIMEOUT`,
`SINOMO_TEST_PERF_MIN_WRITE_FILES_PER_SEC`, and
`SINOMO_TEST_PERF_MIN_READ_FILES_PER_SEC`.

## Automated routing scenario

`docker/test-routing-scenario.sh` runs the three-node segmented/forwarded-endpoint
gate in its own project (`sinomo-routing-scenario` by default, from
`docker/docker-compose.routing-scenario.yml`). A separate single-node `foreign`
cluster supplies a reachable wrong-cluster endpoint. The gate covers
endpoint-error handling, stale direct-endpoint demotion, routed payload
throughput, cross-segment DFS, one-node outage, and restart recovery:

```bash
docker/test-routing-scenario.sh
SINOMO_SCENARIO_KEEP=1 docker/test-routing-scenario.sh
```

Readiness requires two consecutive successful full-cluster network sweeps by
default; tune this with `SINOMO_SCENARIO_NETWORK_READY_STABLE_SWEEPS`. Individual
runtime probes are bounded by `SINOMO_SCENARIO_PROBE_COMMAND_TIMEOUT` (default
300 seconds), while the host-side Docker timeout retains a small cleanup grace
over the in-container deadline. Failure evidence lands in
`target/routing-scenario-artifacts/<run>/`.

## Automated cluster migration suite

`docker/test-cluster-migration.sh` runs a focused two-container migration gate in
its own compose project (`sinomo-migration-test` by default). It uses the same
host-built `target/debug/sinomo` dev-image pattern as the segmented cluster, but
starts inert `target` and `source` containers so the script can drive the exact
single-node migration sequence:

```bash
docker/test-cluster-migration.sh
SINOMO_MIGRATION_TEST_KEEP=1 docker/test-cluster-migration.sh
```

The target initializes a cluster, writes a signed invite, and starts the shadow
supervisor/main daemon. The source initializes a separate single-node cluster,
uses the in-binary `cluster.migration.seed-source` probe to write source KV and
DFS metadata fixtures, then runs `sinomo cluster join /shared/target-invite.txt`.
After the join response, the source main daemon drains and the shadow supervisor
restarts it with the target identity. The harness requires both a new PID and
the target cluster ID before `cluster.migration.verify-joined` checks the
finalized migration record, target KV authority, imported node-local config
under `/config/nodes/<node_id>/...`, skipped source cluster-level config, source
KV/Raft quarantine, and DFS metadata import marker. Commands and probes are
bounded by `SINOMO_MIGRATION_TEST_COMMAND_TIMEOUT` and
`SINOMO_MIGRATION_TEST_PROBE_TIMEOUT`; failure evidence is written to
`target/cluster-test-artifacts/migration-<run>/`.

## Validation gates

The hardening harnesses are designed to fail with useful evidence instead of only printing
diagnostics. They target the `sinomo-dfs-test` dev-cluster project automatically.
`COMPOSE_PROJECT_NAME` takes precedence over each script's project variable
(`SINOMO_GATE_PROJECT`, `SINOMO_IDLE_PROJECT`, `SINOMO_MOBILE_IDLE_PROJECT`,
`SINOMO_BRIDGE_PROJECT`); `measure-final.sh` exports its choice to the scripts it
calls. `COMPOSE_FILE` overrides the dev Compose file:

```bash
# Full production gate: build/start the 6-node cluster, wait for clean formation,
# wait for daemon block writes to quiesce, enforce idle budgets, exercise bridge
# loss/recovery, then re-check final health.
docker/measure-final.sh 60 90

# Short soak: reuse running containers and repeat the full idle/mobile/bridge cycle.
SINOMO_GATE_REBUILD=0 SINOMO_GATE_CYCLES=3 docker/measure-final.sh 60 90

# Enforces 0 daemon block-write bytes, <10 KiB/s daemon block reads, and
# <64 KiB/s sustained network traffic per container during the idle window,
# with one bounded 4 MiB liveness burst. The gate requires exactly one main
# and one shadow daemon per node and fails if either process restarts while its
# counters are sampled. The production gate retries complete 135-second
# zero-write windows for up to ten minutes so finite recovery can settle while
# recurring 60-second churn still fails. Thresholds can be adjusted with
# SINOMO_IDLE_MAX_*_BPS, SINOMO_IDLE_MAX_NET_BURST_BYTES, and
# SINOMO_IDLE_MAX_WRITE_BLOCK_BYTES when intentionally testing a noisier environment.
docker/measure-idle.sh 60

# Changes node5 to `mobile` and node6 to `sensor`, waits for the roles to publish,
# waits for role-transition block writes to quiesce, enforces the same idle budget,
# then restores their original roles. Override the assignment with
# SINOMO_MOBILE_IDLE_ROLES="node5=mobile node6=sensor".
docker/test-mobile-idle.sh 60

# Requires three complete traffic sweeps, disconnects node4 from the
# node3<->node4 bridge network, requires within-component traffic to succeed and
# cross-cut traffic to fail, reconnects the bridge, and requires three complete
# sweeps again within the recovery budget. The selected binary must match the
# running main/shadow processes (see "Focused local network checks").
docker/test-bridge-recovery.sh 90
```

Rebuild runs reset compose volumes by default so the gate starts from a
deterministic cluster formation. Set `SINOMO_GATE_REBUILD=0` to reuse
already-running containers while iterating on the gate scripts themselves, or
`SINOMO_GATE_RESET_VOLUMES=0` to rebuild the image while intentionally preserving
existing `/data` volumes. Set `SINOMO_GATE_CYCLES` above `1` for repeated soak
runs. Set `SINOMO_GATE_SKIP_MOBILE=1` for a shorter bridge/idle run while debugging
unrelated failures. Before each cycle's idle measurement, `docker/measure-final.sh`
waits `SINOMO_GATE_QUIESCE_WAIT` (default `10`) seconds after readiness, then
requires one complete `SINOMO_GATE_QUIESCE_WINDOW` (default `135`) with zero
daemon block writes. It retries complete windows until
`SINOMO_GATE_QUIESCE_TIMEOUT` (default `600`) is exhausted, so finite recovery
can settle while recurring churn still fails. A main/shadow process change
during a window fails immediately. Quiescence failures include per-node write
deltas for the sampled window.
`docker/test-mobile-idle.sh` uses `SINOMO_MOBILE_IDLE_QUIESCE_WAIT` (default
`10`) and `SINOMO_MOBILE_IDLE_QUIESCE_WINDOW` (default `75`) after role changes.
When launched by `docker/measure-final.sh`, these values inherit the top-level
gate wait and window settings.

## Notes

- Within a segment, nodes discover each other via LAN multicast + direct TCP; across
  segments, cluster control traffic is relayed hop-by-hop through the bridge nodes
  (node3, node4) over the connectivity graph's healthy links.
- `SINOMO_DISABLE_IROH` is a runtime switch (the iroh code is still compiled in); unset it
  (or set to `0`) to restore iroh.
- After a self-rollout the stable binaries live in `/data/bin` and are preferred
  over the mounted bootstrap binary on restart (see `entrypoint.sh`); `down -v`
  wipes them and returns the nodes to the host binary.
