# sinomo-dfs

`sinomo-dfs` is Sinomo's local-first distributed filesystem layer. It provides the object, metadata, content, replication, repair, and POSIX-like path APIs used to expose SIFS, the cluster root filesystem.

The crate is not a replacement for the host filesystem, Git, or the cluster KV. It is a policy-aware workspace layer: file bytes stay in node-local content stores, high-cardinality filesystem metadata stays in DFS metadata backends, and the cluster KV is used only for compact DFS control-plane configuration.

The canonical hash of zero-length content is intrinsic: readers reconstruct it without a physical blob file, and namespace creation does not publish a redundant content-store reservation for it. The namespace metadata commit remains durable before an empty file creation is acknowledged.

See also:

- `docs/dfs-architecture.md` for the full distributed design.
- `docs/sifs.md` for the SIFS cluster root layout.
- `docs/storage-architecture.md` for data-directory ownership.

## Architecture

The DFS has four planes.

### Namespace and Object Metadata

DFS metadata records the stable identity and visible state of filesystem objects:

- filesystem ID,
- object ID,
- path,
- object kind,
- owner node,
- current version,
- current content hash,
- current replica locations,
- policy,
- write intents and receipts,
- durable namespace-request admission and terminal results.

Object identity is stable across path moves. Path operations update namespace metadata, not content identity.

The default filesystem is SIFS. From this crate's perspective SIFS is ordinary DFS metadata with a predefined layout:

```text
/
/sinomo
/sinomo/nodes
/sinomo/nodes/<node_id>
/sinomo/nodes/<node_id>/files
/users
/services
```

`/sinomo/nodes/<node_id>/files` is local-owned. `/users` and `/services` are distributed DFS subtrees.

### Content Store

`LocalContentStore` stores bytes under the local data directory as immutable content-addressed blobs plus object-version manifests.

Object versions are protected by hash. Rewriting the same `(object_id, version)` with different bytes is rejected. Storage writes use unique temporary files, atomic rename, and fsync of the file and parent directory.

Object versions can be represented as manifests over extents. Full-object writes remain the compact single-blob fast path, while write-at patches publish extent manifests so random writes and remote patch replication do not rewrite the full base object. Local APIs expose bounded range reads and range streams for serving large blobs without materializing the whole object in memory.

### Metadata Backend

`DfsMetadataBackend` is the durable metadata interface. The current `LocalSobjMetadataBackend` stores materialized shard state as a structured table snapshot under `records/tables/snapshot.sobj` plus a packed tail log under `records/tables/materialized.slog`; the synchronized shard journal remains the replay and replication source of truth. Tail-log appends are rebuildable cache writes and are not synchronized per mutation: reopen truncates a torn tail, rewinds any materialization cursor ahead of the surviving table head, and replays the durable journal. Snapshot compaction is synchronized before replacing that cache generation. Local materialization cursors are checkpointed periodically instead of after every mutation, and namespace transactions can publish object records and their visible version records in one replayable table batch. This avoids creating one host file per object, path, dentry, receipt, or version while reducing per-file metadata write amplification for large namespace imports and first-visible-version commits.

- filesystem records,
- object and path records,
- version records,
- write-intent records,
- replica receipts,
- namespace transactions,
- namespace-request admission records,
- immutable prepared operations and exact digest commits.

Every journal mutation is either a complete `PrepareOperationV7` payload or its exact `CommitPreparedOperationV7` digest. Namespace, content visibility, receipt staging, durable jobs and GC effects use this one family. An opaque `DfsPublicationGuard` retains normal runtime ownership and authorization through commit. Reopen and replica replay use the same projection as publication.

Mutating namespace RPCs first persist a request record keyed by authenticated
requester and request id with the semantic operation, stable operation id, and
exact authority shard. A request-owned hidden prepare commits the namespace
transaction and canonical terminal result in one table batch. Startup and
reactive maintenance resume admissions interrupted before a prepare, while
prepared transactions and durable namespace jobs follow their existing
recovery paths. A completed record replays its result across restart, reuse
with a different semantic operation is rejected, and `Pending` is an internal
automatically recovering state rather than an operator action.

Metadata journal replication is bounded by both entry count and encoded batch size. Oversized single entries still advance by themselves so a large namespace operation cannot stall a follower cursor forever. Daemon maintenance drains multiple bounded batches per publish call, with a cap that keeps each maintenance pass finite while avoiding one-batch-per-interval convergence for large metadata backlogs.

The table tail log is compacted into a streamed snapshot using storage schema 8, bounding reopen cost by the latest checkpoint and recent writes. Its storage version is independent of the V7 operation family. `sinomo-dfs` accepts only current storage and wire formats.

`sinomo-dfs-migration` owns incremental, on-demand conversion of retained storage. The Main or Embedded component supplies its cold-open provider; original source digests, archived bytes, restart progress and native installation authority remain explicit. Incompatible peer versions must be upgraded.

Operator status includes per-shard journal positions, inbound replay cursors, and configured replica cursor lag so cluster checks can distinguish object-level repair work from metadata replication backlog.

Replication carries the original current journal frames in one bounded carrier. Their contiguous scope and prefix are validated before ingestion; sender-side re-encoding cannot substitute for the durable bytes.

### Scale Benchmarks

The crate includes a JSONL scale benchmark for DFS namespace, metadata replication, local content, and network content paths:

```sh
cargo bench -p sinomo-dfs --bench scale -- --scenario all --preset smoke
```

The default `smoke` preset keeps local validation cheap. Use the production preset for the baseline targets that matter before DFS scaling changes: 1M namespace objects, full and paginated directory listing, create throughput, reopen hydration, metadata replication catch-up, and multi-GB local/network blob streaming.

```sh
cargo bench -p sinomo-dfs --bench scale -- \
  --scenario all \
  --preset production \
  --root /path/to/fast/sinomo-dfs-scale-baseline \
  --keep
```

The benchmark prints one JSON object per measured step. Byte-sized flags such as `--blob-bytes`, `--blob-range-bytes`, and `--network-blob-bytes` accept suffixes like `MiB` and `GiB`, so expensive one-off runs can be narrowed without changing source code.
Namespace-load and metadata-replication setup use grouped metadata commits by default; tune `--metadata-batch-size` when comparing write-amplification changes.

To isolate the networked DFS algorithm from physical-disk and container-network overhead, run the `InProcessNetwork` blob scenario with every content and metadata store rooted on tmpfs. This example measures a 128 MiB write with desired replica count N=5 and write quorum W=1, then removes its generated data even if the benchmark fails:

```sh
(
  bench_root="$(mktemp -d /dev/shm/sinomo-dfs-network-blob.XXXXXX)"
  trap 'rm -rf "$bench_root"' EXIT
  CARGO_INCREMENTAL=0 cargo bench -p sinomo-dfs --bench scale -- \
    --scenario network-blob \
    --preset smoke \
    --root "$bench_root" \
    --network-blob-bytes 128MiB \
    --network-blob-desired-replicas 5 \
    --network-blob-write-quorum 1
)
```

In the JSONL output, `desired_replicas` is N and `write_quorum` is W. `network_blob_w_visible_commit.time_to_w_ms` starts immediately before the replica workers are released and the POSIX write is invoked, then stops when that call returns with the version visible and exactly W durable receipts. It is marked as an upper bound because the harness observes the return boundary rather than the internal instant at which W was crossed. `network_blob_all_desired_committed.time_to_all_replica_commits_ms` uses the same start and stops at the latest replica's commit timestamp. It excludes the later owner-side response drain, metrics collection, and worker finalization. Logical throughput counts the object bytes once, while effective quorum/replicated throughput counts the durable bytes represented by W or N copies.

`effective_cores` is process CPU time divided by elapsed wall time over the corresponding boundary. It includes the owner, in-process replica worker threads, and small harness overhead; it can exceed one and is not a percentage of all host CPUs. The reported owner/replica idle-loop counters include a 1 ms sleep used only by this benchmark driver when an `InProcessNetwork` queue is empty. That polling is outside the DFS service hot path and must not be interpreted as an internal DFS wait.

### Networked DFS Service

`NetworkedDfsService<T>` is the distributed object service over a `sinomo-network` transport. It combines:

- local metadata cache,
- local content store,
- optional control-plane KV client,
- optional durable metadata backend,
- distributed write coordinator,
- data-plane DFS network messages.

Distributed writes are coordinated by the responsible metadata-shard primary. A node that receives a write for a shard it does not currently primary routes the request to that primary instead of creating a local intent. The primary creates the persisted intent, stages content to desired replicas, records durable receipts, commits the visible version after the policy threshold, and keeps repair jobs until the desired replica set converges.

Ordinary writes do not store per-file state in cluster KV or perform an unconditional Raft round. Before visible publication, the primary journals a hidden, operation-bound prepare in DFS metadata: the pending intent and exact `W` receipt set for content writes, plus the namespace transaction for first publication or namespace-only mutations. Every configured metadata-shard replica must durably apply and verify that exact prepare before acknowledging it. If the complete shard cannot acknowledge, the primary falls back to an operation-bound certificate for the exact DFS configuration through cluster quorum. Cluster KV therefore retains only DFS configuration and the bounded fallback barrier; content durability remains governed independently by `W`.

Shard membership changes retain the current primary and do not immediately activate a replacement authority. The quorum side first records a bounded pending handoff; this is not itself a write fence because the isolated retiring primary may not have observed it. After reconnecting, the retiring primary takes its publication guard, installs a durable local backend freeze, and acknowledges its exact journal head and fingerprint. That exact prefix must then reach and be acknowledged by the union of the retiring and desired assignments before the desired assignment activates. The local marker then advances to the activated epoch instead of disappearing: only the backend instance explicitly rebound after observing that exact control-plane generation may resume authoritative mutations, while independently opened stale instances remain fenced. Pending markers reject authoritative metadata mutations, and both pending and activated markers still permit replay and handoff transfer. This carries writes completed after the pending decision without putting healthy DFS operations through Raft.

Normal quorum-backed DFS operation must not create conflict records. Base-version races and stale writers are serialized at the owner or metadata-shard primary and must retry, reject, remain pending, or fail fast; a conflict record in this path is a correctness bug. Conflict records are reserved for future split-cluster or disconnected-authority recovery and need an explicit reconciliation algorithm before that mode can be enabled.

Replica-side commands are authorized against object ownership, metadata-shard primary state when available, desired replica membership, persisted intent records, and content hashes. Rejected replica commands do not create replica metadata.

Repair is restart-aware. On service hydration, persisted intents and receipts rebuild coordinator state and missing-replica jobs. Repair retries have backoff-like scheduling, and a recovered coordinator that no longer has local blob bytes can fetch the blob from another durable replica before repairing a missing node.

Reader- and file-backed data-plane operations attach their semantic byte bodies
to the canonical `sinomo-network` object-RPC call at every size. DFS heads
retain object/version identity, declared size, and content hashes, while the
network layer owns framing, flow control, health timeouts, and replay-safe
retries. DFS does not expose transport chunk indexes, windows, or resume routes.

### Lifecycle Maintenance

`NetworkedDfsService::run_lifecycle_maintenance` combines the operational lifecycle paths:

- content scrub,
- local content anti-entropy,
- old-version collection with retention,
- orphan blob GC,
- metadata tombstone compaction,
- metadata-backed namespace-job maintenance through daemon ticks.

`LocalContentStore` can enforce blob, logical-object, manifest, and staging-byte quotas and reports content footprint through operational metrics.

## Policies

The main object policies currently used by the crate are:

- `LocalOwned`: one owner node is the write authority. Other nodes route reads and writes to the owner.
- `ReplicatedEventual`: a write becomes visible once `min_commit_replicas` durable receipts exist, then repair converges toward `desired_replicas`.
- `ReplicatedConfirmed`: a stricter replicated policy where all desired replicas must confirm before commit.

The policy is attached to objects and inherited by higher-level path APIs. DFS does not hardcode behavior by path except for the SIFS bootstrap defaults.

## Storage Layout

The crate registers these data-directory areas through `register_storage_paths`:

```text
dfs/content/blobs
dfs/content/objects
dfs/content/staging
dfs/metadata/records
dfs/metadata/records/tables
dfs/metadata/journal
dfs/metadata/root
dfs/metadata/shard-index
dfs/metadata-replication-cursors
dfs/metadata
dfs/operator
dfs/pins
files
```

The exact storage format is intentionally hidden behind crate APIs and metadata backend traits. Other crates should use the DFS interfaces instead of reading these files directly.

## Using the SIFS POSIX Facade

The Main or Embedded DFS component supplies the client, transport, data owner,
metadata backend and publication executor. Mutations use that mounted runtime;
constructing a content store or backend does not grant publication authority.

```rust
use sinomo_dfs::{DfsNodeTransport, DfsWriteOptions, NetworkedDfsService};

fn write_example(
    dfs: &mut NetworkedDfsService<DfsNodeTransport>,
    options: DfsWriteOptions,
) -> Result<(), sinomo_dfs::DfsPosixError> {
    dfs.posix_mkdir_all("/users/example")?;
    dfs.posix_write_file("/users/example/hello.txt", b"hello from SIFS", options)?;
    assert_eq!(dfs.posix_read_file("/users/example/hello.txt")?, b"hello from SIFS");
    Ok(())
}
```

Tests use the shared embedded runtime in `test_support::DfsTestRuntime` with
`test-support` enabled. Its normal clients use `DfsNodeTransport`, native
component startup, authenticated ingress and the same guarded publication path.

## Operational Notes

- Do not put large file bytes in `sinomo-kv`; DFS content stores carry bytes.
- Do not put per-file metadata, content manifests, intents or receipts in `sinomo-kv`; DFS metadata shards carry high-cardinality state.
- Treat metadata backend records as the source of restart recovery.
- Use SOBJ or packed binary formats for internal DFS storage; JSON/JSONL is reserved for CLI output, service display, benchmark artifacts, and legacy migration.
- Use SIFS path APIs for filesystem-like workflows and object/version APIs for replication, repair, audit, and operator tooling.
- Quorum-backed write races reject, retry or remain pending according to their exact durable outcome.
- Local-owned subtrees remain useful for sensitive or node-specific files even inside the cluster root filesystem.
