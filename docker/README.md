# Sinomo segmented dev cluster (Docker) — network-hardening test bed

A disposable **6-node** Sinomo cluster partitioned into three network segments, with
**iroh disabled**, used to develop native (non-iroh) peer **discovery and routing**.

For a minimal one-node cluster used to validate local DFS and self-evolution without
the incomplete sandbox path, use the separate compose file:

```bash
docker compose -f docker-compose.single-node.yml up --build -d sinomo-single
docker compose -f docker-compose.single-node.yml run --rm sinomo-single-smoke
docker compose -f docker-compose.single-node.yml down -v
```

`sinomo-single` keeps state in the persistent `single-node-data` volume. The smoke
service uses container-local data and is discarded after each run so repeated
smoke tests remain a true one-node cluster.

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

Joins are best-effort and time-bounded, so **no node hangs**; a cross-segment joiner retries
while its upstream bridge finishes joining, so cohesion typically settles within ~a minute of
startup. (Historically this was the "expected broken" red baseline for the routing work; it
now coheres.)

## Layout

- `docker/Dockerfile` — node image (Rust toolchain + source + built `sinomo`). Builds from
  a git worktree or a clone.
- `docker/entrypoint.sh` — best-effort multi-hop formation (seed → bridges re-publish
  invites → joiners), `SINOMO_DISABLE_IROH=1`, then runs the shadow supervisor (PID 1).
- `../docker-compose.yml` — 6 services + 3 bridge networks (`seg_a`, `seg_ab`, `seg_b`).

Each node: main daemon `0.0.0.0:21353`, shadow `0.0.0.0:21354`, own `/data` volume, a
managed rollout source repo at `/data/source/sinomo`, and a shared `/shared` volume that
carries the per-segment invite files. `restart: "no"` so a failure stays visible.

## Usage

```bash
# Build the image and start all 6 nodes (first build is slow / cold).
docker compose up --build -d

# Watch formation.
docker compose logs -f node1
docker compose logs node4         # see the cross-segment join attempt + result

# Cluster view from each segment.
docker compose exec node1 sinomo cluster health   # expect all 6 main daemons
docker compose exec node5 sinomo cluster status   # expect: integrated (a cluster member)

# Confirm iroh is off (no iroh:// endpoint advertised).
docker compose logs node1 | grep -i iroh

# Reachability sanity (segmentation): from node1, node3 resolves, node5 does not.
docker compose exec node1 getent hosts node3      # resolves
docker compose exec node1 getent hosts node5 || echo "unreachable (expected)"

docker compose ps                  # container state (all Up)
docker compose down -v             # tear down + wipe volumes
```

## Validation gates

The hardening harnesses are designed to fail with useful evidence instead of only printing
diagnostics:

```bash
# Full production gate: build/start the 6-node cluster, wait for clean formation,
# wait for daemon block writes to quiesce, enforce idle budgets, exercise bridge
# loss/recovery, then re-check final health.
docker/measure-final.sh 60 90

# Short soak: reuse running containers and repeat the full idle/mobile/bridge cycle.
SINOMO_GATE_REBUILD=0 SINOMO_GATE_CYCLES=3 docker/measure-final.sh 60 90

# Enforces 0 daemon block-write bytes, <10 KB/s daemon block reads, and <10 KB/s
# network traffic per container during the idle window. Thresholds can be adjusted
# with SINOMO_IDLE_MAX_*_BPS and SINOMO_IDLE_MAX_WRITE_BLOCK_BYTES when intentionally
# testing a noisier environment.
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

## Iterating on code

The image bakes a build of the current source; after host changes, `docker compose up
--build -d` rebuilds (cold). For a faster daemon-only iteration without a full image
rebuild, copy the changed file into a node, rebuild incrementally against its warm
`target/`, install over the baked binary, and restart that node:

```bash
docker compose cp crates/<crate>/src/<file>.rs node1:/sinomo-src/crates/<crate>/src/<file>.rs
docker compose exec -T node1 sh -c 'cd /sinomo-src && cargo build -p sinomo && install -Dm755 target/debug/sinomo /usr/local/bin/sinomo'
docker compose restart node1
```

## Notes

- Within a segment, nodes discover each other via LAN multicast + direct TCP; across
  segments, cluster control traffic is relayed hop-by-hop through the bridge nodes
  (node3, node4) over the connectivity graph's healthy links.
- `SINOMO_DISABLE_IROH` is a runtime switch (the iroh code is still compiled in); unset it
  (or set to `0`) to restore iroh.
- The git commit each node was built from shows in `sinomo cluster health` (`commit`
  column).
