# Sinomo dev cluster (Docker)

A disposable 3-node Sinomo cluster for local development and for exercising
cluster-wide behavior (rollouts, raft, DFS, connectivity) without touching a real
deployment. All three nodes are `linux/x86_64`, so `rollout cluster` uses the
binary path for every node and exercises the shadow-supervised rollout handoff.

## Layout

- `docker/Dockerfile` — node image: Rust toolchain + source + built `sinomo`
  binary. The toolchain/source stay in the image so `rollout apply` /
  `rollout cluster` can build a new binary in-container from the managed source repo.
- `docker/entrypoint.sh` — forms (or rejoins) the cluster, then runs the shadow
  supervisor as PID 1 (which starts + supervises the main daemon).
- `../docker-compose.yml` — three services: `node1` (seed), `node2`, `node3`.

Each node:
- main daemon on `0.0.0.0:21353`, shadow on `0.0.0.0:21354`,
- its own `/data` volume (`SINOMO_DATA_DIR`),
- a managed rollout source repo at `/data/source/sinomo`,
- a shared `/shared` volume carrying the join invite from the seed.

`restart: "no"` is deliberate: if a rollout leaves a node without a supervisor,
the container stays down so the failure is visible instead of masked.

## Usage

```bash
# Build the image and start the cluster (first build is slow; ~3 GB image).
docker compose up --build -d

# Watch a node come up / form the cluster.
docker compose logs -f node1

# Cluster status / health (run from any node).
docker compose exec node1 sinomo cluster health
docker compose exec node1 sinomo cluster status
docker compose exec node1 sinomo daemon status

# Roll out the current source to the whole cluster (each node builds its own).
docker compose exec node1 sinomo rollout cluster --remote-build --repo /data/source/sinomo

# Roll out to a single node.
docker compose exec node2 sinomo rollout apply --repo /data/source/sinomo

# Inspect a node.
docker compose exec node2 sh
docker compose ps                 # container state (Up / Exited)

# Tear down and wipe all state.
docker compose down -v
```

## Iterating on code

The image bakes a build of the current source. After changing code on the host,
rebuild and recreate:

```bash
docker compose up --build -d
```

To pick up source changes for an *in-container rollout* without rebuilding the
image, re-import into each node's managed source repo, e.g.:

```bash
docker compose exec node1 sinomo rollout import-source --repo /sinomo-src
```

## Notes

- Nodes talk over the compose bridge network via LAN-discovered TCP endpoints;
  no external iroh relay is required for same-network peers.
- The git commit each node was built from is shown in `sinomo cluster health`
  (the `commit` column), so you can tell which nodes have a given build.
