# Phase 1 Implementation Plan: Contracts and Event Backbone

## Purpose

Phase 1 establishes the stable Rust contracts that later Sinomo services will share, plus the first event validation and replay backbone. It should create a compileable foundation without pulling in ingestion, perception, networking, DFS, memory, or agent behavior yet.

Architecture source of truth: [../architecture.md](../architecture.md).

## Outcomes

By the end of Phase 1:

- `sinomo-core` owns shared IDs, event schemas, routing metadata, capability contracts, provenance, evidence, and serialization.
- `sinomo-node` owns node identity/profile contracts and local node metadata.
- `sinomo-device` owns device profile contracts and node/device attachment metadata.
- `sinomo-events` owns event validation, append/replay interfaces, and a first in-memory event store.
- `sinomo-app` can wire a local node, a local device, a synthetic event, validation, append, and replay.
- `cargo test --workspace` passes with focused contract tests.

## Non-Goals

Do not implement these in Phase 1:

- real audio/video/text ingestion,
- peer discovery or network routing,
- PostgreSQL migrations or durable database adapters,
- DFS content storage or write routing,
- memory promotion and retrieval,
- trigger policy,
- agent dialogue loop,
- action execution,
- NATS, Redis, object storage, or other external service dependencies.

PostgreSQL can be represented by persistence traits, but the first working store should be in-memory so contracts can settle before durable schema work begins.

## Crate Order

Create crates in this order:

1. `crates/sinomo-core`
2. `crates/sinomo-node`
3. `crates/sinomo-device`
4. `crates/sinomo-events`
5. `crates/sinomo-app`

The existing `crates/sinomo` binary may be renamed to `sinomo-app`, or it may remain as a temporary bootstrap binary until the workspace is ready for cleanup. Prefer `sinomo-app` as the target name because the architecture uses that name.

## Workspace Dependencies

Add only dependencies needed for contracts and tests:

- `serde` with `derive`
- `serde_json`
- `thiserror`
- `time` with serde support
- `ulid` or `uuid` for generated IDs
- `tracing`

Optional only if immediately useful:

- `async-trait` if event store traits are async from the beginning.

Avoid database, broker, web server, and model-client dependencies in this phase.

## Milestone 1: Workspace and Crate Skeletons

Tasks:

- Add library crates for `sinomo-core`, `sinomo-node`, `sinomo-device`, and `sinomo-events`.
- Add binary crate `sinomo-app`.
- Move shared package metadata to workspace-level dependencies where practical.
- Add minimal `lib.rs` files with public module structure.
- Ensure each crate compiles before adding behavior.

Expected dependency direction:

```text
sinomo-core
  <- sinomo-node
  <- sinomo-device
  <- sinomo-events
  <- sinomo-app

sinomo-node + sinomo-device + sinomo-events
  <- sinomo-app
```

Acceptance:

- `cargo check --workspace` succeeds.
- No service crate depends on another service crate unless needed by `sinomo-app`.

## Milestone 2: `sinomo-core` ID and Scalar Contracts

Implement strongly typed ID newtypes:

- `EventId`
- `NodeId`
- `DeviceId`
- `SessionId`
- `EntityId`
- `TaskId`
- `WorkspaceId`
- `DfsObjectId`
- `DfsVersion`

Each ID should support:

- `Clone`, `Debug`, `Eq`, `PartialEq`, `Hash`, `Ord`, `PartialOrd`
- `Display`
- `FromStr`
- `Serialize`
- `Deserialize`
- generated constructor where useful
- prefix validation

Recommended prefixes:

| Type | Prefix |
|---|---|
| `EventId` | `evt_` |
| `NodeId` | `node_` |
| `DeviceId` | `device_` |
| `SessionId` | `sess_` |
| `EntityId` | `entity_` |
| `TaskId` | `task_` |
| `WorkspaceId` | `workspace_` |
| `DfsObjectId` | `obj_` |

Also define common scalar/value types:

- `Source`
- `EventKind`
- `CapabilityName`
- `TrustDomain`
- `Confidence`
- `Salience`

`Confidence` and `Salience` should reject values outside `0.0..=1.0`.

Acceptance:

- invalid prefixes are rejected during parsing/deserialization.
- confidence and salience ranges are tested.
- serialization round trips preserve the exact string representation.

## Milestone 3: `sinomo-core` Event Contract

Implement the canonical event envelope:

```rust
pub struct EventEnvelope {
    pub event_id: EventId,
    pub time: OffsetDateTime,
    pub origin_node_id: NodeId,
    pub origin_device_id: Option<DeviceId>,
    pub target_node_id: Option<NodeId>,
    pub target_device_id: Option<DeviceId>,
    pub source: Source,
    pub session_id: Option<SessionId>,
    pub kind: EventKind,
    pub span: Option<EventSpan>,
    pub entities: Vec<EntityRef>,
    pub salience: Salience,
    pub confidence: Confidence,
    pub payload: serde_json::Value,
    pub evidence: Evidence,
    pub links: EventLinks,
    pub routing: RoutingMetadata,
}
```

Supporting types:

- `EventSpan { start_ms, end_ms }`
- `EntityRef`
- `Evidence`
- `EvidenceRef`
- `EvidenceKind`
- `EventLinks`
- `RoutingMetadata`
- `RoutingScope`
- `LatencyClass`
- `PrivacyClass`
- `RoutingHint`

Initial event families:

- `audio.*`
- `video.*`
- `speech.*`
- `vision.*`
- `ui.*`
- `tool.*`
- `agent.*`
- `memory.*`
- `safety.*`
- `generation.*`
- `notification.*`
- `dfs.*`
- `workspace.*`

Keep `EventKind` open and string-backed. Validate shape, not a closed enum. A good first rule is namespace-like dotted segments with at least two non-empty segments, for example `speech.transcript.final`.

Acceptance:

- the documented event JSON shape serializes and deserializes.
- optional distributed fields are represented without creating a separate remote event type.
- event kind validation allows future event families without code changes.

## Milestone 4: `sinomo-core` Domain Placeholders

Add lightweight domain contracts that later crates can use without behavior:

- `SessionState`
- `TaskState`
- `ActionRequest`
- `ActionResult`
- `MemoryRecord`
- `TriggerDecision`
- `DeliberationJob`
- `NodeCapability`
- `DeviceCapability`
- `ModelCapability`
- `SyncPolicy`
- `DfsObjectPolicy`
- `DfsWriteRequest`
- `DfsWriteReceipt`
- `HotStateSnapshot`
- `ContextPacket`
- `JobCancellation`

These should be intentionally small. The goal is stable shapes and names, not full domain implementation.

Acceptance:

- these types are serializable where they cross crate boundaries.
- no service-specific dependency leaks into `sinomo-core`.

## Milestone 5: `sinomo-node`

Implement node profile contracts:

```rust
pub struct NodeProfile {
    pub node_id: NodeId,
    pub display_name: String,
    pub role: NodeRole,
    pub trust_domain: TrustDomain,
    pub capabilities: Vec<NodeCapability>,
    pub hosted_services: Vec<HostedService>,
    pub hosted_models: Vec<HostedModel>,
    pub resource_envelope: NodeResourceEnvelope,
    pub health: NodeHealth,
}
```

Supporting types:

- `NodeRole`
- `HostedService`
- `HostedModel`
- `NodeResourceEnvelope`
- `NodeHealth`
- `NodeAvailability`

Add helpers for:

- constructing a local node profile,
- testing capability presence,
- updating health metadata.

Acceptance:

- node profile JSON round trips.
- capability lookup works by capability name.
- no persistence or networking is introduced.

## Milestone 6: `sinomo-device`

Implement device profile contracts:

```rust
pub struct DeviceProfile {
    pub device_id: DeviceId,
    pub node_id: NodeId,
    pub display_name: String,
    pub kind: DeviceKind,
    pub capabilities: Vec<DeviceCapability>,
    pub permissions: Vec<DevicePermission>,
    pub state: DeviceState,
}
```

Supporting types:

- `DeviceKind`
- `DevicePermission`
- `DeviceState`
- `DeviceAvailability`
- `DeviceSource`
- `ActionTarget`

Add helpers for:

- constructing common mock devices,
- checking whether a device belongs to a node,
- checking capability presence.

Acceptance:

- device profile JSON round trips.
- a device always carries its owning `NodeId`.
- provenance helpers make it easy to attach node/device IDs to events.

## Milestone 7: `sinomo-events` Validation

Implement an `EventValidator`.

Validation rules:

- `event_id` parses and is not empty.
- `origin_node_id` is required.
- `kind` is namespace-like and non-empty.
- `source` is non-empty.
- `salience` and `confidence` are in range.
- `span.start_ms <= span.end_ms`.
- event links do not self-reference the same `event_id`.
- routing metadata is present.
- `origin_device_id`, when present, must be structurally valid.
- `target_device_id`, when present, must be structurally valid.

Do not require access to a node or device registry yet. Cross-checking whether a device belongs to a node belongs in a later runtime integration step or an optional validator extension.

Acceptance:

- validator returns structured errors.
- tests cover each validation failure.
- valid fixture events pass.

## Milestone 8: `sinomo-events` Append and Replay Interfaces

Define event store interfaces:

```rust
pub trait EventStore {
    fn append(&mut self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError>;
    fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError>;
}
```

If async is adopted immediately:

```rust
#[async_trait]
pub trait EventStore {
    async fn append(&self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError>;
    async fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError>;
}
```

Supporting types:

- `AppendReceipt`
- `StoredEvent`
- `EventSequence`
- `EventCursor`
- `ReplayQuery`
- `ReplayDirection`
- `EventStoreError`

Replay filters:

- all events,
- by session,
- by kind prefix,
- by origin node,
- after sequence/cursor,
- time range.

Acceptance:

- append assigns a monotonic store sequence independent of `event_id`.
- replay preserves append order by default.
- replay filters can be composed in `ReplayQuery`.

## Milestone 9: In-Memory Event Store

Implement:

```rust
pub struct InMemoryEventStore {
    validator: EventValidator,
    events: Vec<StoredEvent>,
}
```

Behavior:

- validates before append,
- rejects duplicate `event_id`,
- stores append sequence,
- supports replay queries,
- returns cloned event records for now,
- remains deterministic for tests.

Acceptance:

- duplicate event IDs are rejected.
- replay by session returns only matching events.
- replay by kind prefix returns all matching families.
- replay after cursor excludes earlier records.
- event append order is stable.

## Milestone 10: `sinomo-app` Smoke Path

Implement a minimal local binary that:

1. creates a local node profile,
2. creates one mock device,
3. builds a synthetic `speech.transcript.final` event,
4. validates it,
5. appends it to `InMemoryEventStore`,
6. replays events for the session,
7. logs or prints a compact summary.

Example output:

```text
sinomo-app: appended evt_... kind=speech.transcript.final session=sess_...
sinomo-app: replayed 1 event(s)
```

Acceptance:

- `cargo run -p sinomo-app` completes successfully.
- the smoke path uses public crate APIs rather than test-only helpers.

## Testing Plan

Add focused unit tests:

- ID prefix parsing and serialization.
- `Confidence` and `Salience` bounds.
- event JSON round trip.
- event kind validation.
- span validation.
- link self-reference rejection.
- node profile serialization and capability lookup.
- device profile serialization and capability lookup.
- event append/replay filters.
- duplicate event ID rejection.

Add one integration-style test if useful:

- local node + device + event + in-memory append + replay.

Avoid snapshot tests for unstable debug formatting. Prefer explicit JSON assertions for stable schema fields.

## Documentation Updates

Add Rustdoc examples for:

- constructing an event envelope,
- appending and replaying an event,
- creating node and device profiles.

Keep `docs/architecture.md` as the detailed source of truth. Keep `docs/proposal.md` high-level and avoid duplicating crate-by-crate detail there.

## Definition of Done

Phase 1 is complete when:

- all Phase 1 crates exist and compile,
- `cargo fmt --all --check` passes,
- `cargo clippy --workspace --all-targets` passes or has documented exceptions,
- `cargo test --workspace` passes,
- `cargo run -p sinomo-app` runs the local event smoke path,
- event schemas serialize to the architecture's documented shape,
- the proposal remains concise and non-conflicting with the architecture.

## Risks and Decisions

### ID generator choice

Use `ulid` if time-sortable IDs are preferred for event/debug workflows. Use `uuid` if ecosystem familiarity matters more. Either is acceptable behind the ID newtypes.

Decision needed before implementation: choose one ID generation crate.

### Sync versus async event store traits

Synchronous traits are simpler for Phase 1 and the in-memory store. Async traits may reduce churn when PostgreSQL arrives.

Recommended Phase 1 default: synchronous traits unless the implementation immediately introduces async runtime dependencies elsewhere.

### Closed enums versus open strings

Event families should remain open string-backed contracts. Closed enums can be used for routing scope, latency class, privacy class, and other truly bounded policy fields.

### Durable persistence timing

Do not start with PostgreSQL migrations in Phase 1. Add the durable adapter in a later phase after event contract tests stabilize.

## Suggested Implementation Sequence

1. Add crate skeletons and workspace dependencies.
2. Implement ID newtype macro or helper pattern in `sinomo-core`.
3. Implement `Confidence`, `Salience`, `Source`, and `EventKind`.
4. Implement event envelope and routing/evidence/link structs.
5. Add JSON round-trip fixtures.
6. Implement node profile and capability lookup.
7. Implement device profile and capability lookup.
8. Implement event validator.
9. Implement event store traits and in-memory store.
10. Implement `sinomo-app` smoke path.
11. Run fmt, clippy, tests, and app smoke command.
