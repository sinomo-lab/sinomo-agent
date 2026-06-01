# Sinomo segmented dev cluster (Docker) — network-hardening test bed

A disposable **6-node** Sinomo cluster partitioned into three network segments, with
**iroh disabled**, used to develop native (non-iroh) peer **discovery and routing**.

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

## Expected state: BROKEN across segments (by design)

Native multi-hop routing through the bridge nodes is **not implemented yet**, so:

- `{node1,node2,node3}` form a healthy cluster (all in `seg_a`).
- `{node4,node5,node6}` **cannot integrate** — their cross-segment joins fail (node3/node4
  can't yet relay a join to the raft leader), so they come up **unclustered**.
- `sinomo cluster health` on node1 shows only the `seg_a` members; node5/node6 are absent.

This red baseline is the target for the routing work. Joins are best-effort and waits are
time-bounded, so **no node hangs** — every container still ends up running its daemon and
LAN-announcing within its segment(s).

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
docker compose exec node1 sinomo cluster health   # expect {1,2,3}
docker compose exec node5 sinomo cluster status   # expect: not integrated with {1,2,3}

# Confirm iroh is off (no iroh:// endpoint advertised).
docker compose logs node1 | grep -i iroh

# Reachability sanity (segmentation): from node1, node3 resolves, node5 does not.
docker compose exec node1 getent hosts node3      # resolves
docker compose exec node1 getent hosts node5 || echo "unreachable (expected)"

docker compose ps                  # container state (all Up)
docker compose down -v             # tear down + wipe volumes
```

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
  segments there is no path until routing exists.
- `SINOMO_DISABLE_IROH` is a runtime switch (the iroh code is still compiled in); unset it
  (or set to `0`) to restore iroh.
- The git commit each node was built from shows in `sinomo cluster health` (`commit`
  column).
