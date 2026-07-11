# Sinomo Docker clusters

Compose layout:

- `../docker-compose.yml` — **production node**: full image (toolchain + managed
  source baked in, self-rollout capable), iroh enabled, one configurable
  seed/joiner service.
- `docker/docker-compose.dev.yml` — **6-node segmented dev cluster** (this
  README): network-hardening test bed running the **host-built binary**.
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
smoke tests remain a true one-node cluster.

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
cargo build -p sinomo                                      # build on the host
docker compose -f docker/docker-compose.dev.yml up --build -d
docker compose -f docker/docker-compose.dev.yml exec node1 sinomo cluster health

cargo build -p sinomo                                      # iterate...
docker compose -f docker/docker-compose.dev.yml restart    # ...nodes pick up the new binary
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
| node3 | `seg_a`, `seg_ab` | joiner + **bridge A↔AB** (re-publishes an invite for node4) |
| node4 | `seg_ab`, `seg_b` | joiner (via node3) + **bridge AB↔B** (re-publishes for node5/6) |
| node5 | `seg_b` | joiner |
| node6 | `seg_b` | joiner |

Docker's embedded DNS only resolves peers on a **shared** network, so the segmentation is
real: `node1` cannot even resolve/reach `node5`. Each node joins through a *reachable*
introducer (node2,3→`node1`; node4→`node3`; node5,6→`node4`).

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
harness readiness marker, then runs one cluster join command. The docker gates allow 30
seconds for clean six-node formation. This covers a complete 5-second transport timeout plus
deep-segment relay convergence without turning normal startup jitter into a false failure;
the measured elapsed time is still reported so regressions remain visible.

## Layout

- `docker/Dockerfile` — full node image (Rust toolchain + source + built `sinomo`);
  used by the root production compose and the single-node smoke compose.
- `docker/Dockerfile.dev` — thin runtime image for the dev cluster (host binary
  bind-mounted in).
- `docker/entrypoint.sh` — best-effort multi-hop formation (seed → bridges re-publish
  invites → joiners), then runs the shadow supervisor (PID 1). Shared by all
  composes.
- `docker/docker-compose.dev.yml` — 6 services + 3 bridge networks (`seg_a`,
  `seg_ab`, `seg_b`).

Each node: main daemon `0.0.0.0:21353`, shadow `0.0.0.0:21354`, own `/data` volume, and a
shared `/shared` volume that carries the per-segment invite files. `restart:
unless-stopped` lets Docker bring a node back when rollout intentionally replaces the
shadow supervisor running as PID 1.

## Usage

The helper scripts under `docker/` export `COMPOSE_FILE` pointing at the dev
cluster, so they work from any directory; plain `docker compose` commands need
`-f docker/docker-compose.dev.yml` (shown as `-f …dev.yml` below):

```bash
cargo build -p sinomo

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
SINOMO_TEST_REUSE=1 docker/test-cluster.sh   # reuse the running test project
SINOMO_TEST_KEEP=1  docker/test-cluster.sh   # leave the cluster up afterwards
```

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
gate, including endpoint-error fallback, stalled persistent-tunnel fallback,
stale direct-endpoint demotion, routed payload throughput, cross-segment DFS,
one-node outage, and restart recovery:

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
diagnostics. They target the `sinomo-dfs-test` dev-cluster project automatically
(override with `SINOMO_GATE_PROJECT`/`SINOMO_IDLE_PROJECT`, `COMPOSE_PROJECT_NAME`,
or `COMPOSE_FILE`):

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

# Disconnects node4 from the node3<->node4 bridge network, verifies the outage is
# observed, reconnects the bridge, and waits for health/doctor to return clean.
docker/test-bridge-recovery.sh 90
```

Rebuild runs reset compose volumes by default so the gate starts from a
deterministic cluster formation. Set `SINOMO_GATE_REBUILD=0` to reuse
already-running containers while iterating on the gate scripts themselves, or
`SINOMO_GATE_RESET_VOLUMES=0` to rebuild the image while intentionally preserving
existing `/data` volumes. Set `SINOMO_GATE_CYCLES` above `1` for repeated soak
runs. Set `SINOMO_GATE_SKIP_MOBILE=1` for a shorter bridge/idle run while debugging
unrelated failures. `docker/measure-final.sh` uses `SINOMO_GATE_QUIESCE_WAIT`
(default `10`) as the hard stabilization deadline after the preceding operation
reports ready, then requires zero daemon block writes for
`SINOMO_GATE_QUIESCE_WINDOW` (default `75`) before each idle measurement. The
Docker cluster is expected to have the connectivity graph stored and stabilized
within 10 seconds; any daemon block write after that deadline is treated as a
bug. Quiescence failures include per-node write deltas for the sampled window.
`docker/test-mobile-idle.sh` uses `SINOMO_MOBILE_IDLE_QUIESCE_WAIT` (default
`10`) and `SINOMO_MOBILE_IDLE_QUIESCE_WINDOW` (default `75`) to apply the same
hard stabilization requirement after role changes. When launched by
`docker/measure-final.sh`, these values inherit the top-level gate quiescence
settings.

## Notes

- Within a segment, nodes discover each other via LAN multicast + direct TCP; across
  segments, cluster control traffic is relayed hop-by-hop through the bridge nodes
  (node3, node4) over the connectivity graph's healthy links.
- `SINOMO_DISABLE_IROH` is a runtime switch (the iroh code is still compiled in); unset it
  (or set to `0`) to restore iroh.
- After a self-rollout the stable binaries live in `/data/bin` and are preferred
  over the mounted bootstrap binary on restart (see `entrypoint.sh`); `down -v`
  wipes them and returns the nodes to the host binary.
