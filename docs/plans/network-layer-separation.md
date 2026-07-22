# Network Layer Separation Plan

Status: completed.

Last reviewed: 2026-07-22.

Stage G completed on 2026-07-22. The production path now has one routed-stream
executor, one stream-native object-RPC protocol, and one service registry. The
staged sections below are retained as implementation history and acceptance
criteria.

## Goal

Replace the current combined networking implementation with the contracts in
the [network architecture](../network-architecture.md): direct authenticated
links, connectivity snapshots, node-addressed routed streams, and object RPC
over an arbitrary authenticated duplex stream.

The central acceptance property is component-wide path realization: every pair
of authorized nodes in the same current routable connected component must be
able to establish an authenticated direct or relayed session. Connectivity
records direct edges; routing derives component membership, owns every
transitive relationship, and makes those relationships usable.

All of these layers remain internal modules of the existing `sinomo-network`
crate. Creating `sinomo-link`, `sinomo-connectivity`, `sinomo-routing`, or
`sinomo-object-rpc` workspace crates is explicitly out of scope.

This is a replacement plan. It must not leave the new design as a third carrier
beside the current canonical session and persistent channel.

## Baseline

Commit `23628f6f` consolidates the existing private channel and cluster tunnel
wire version as implementation protocol v1 and deletes substantial obsolete
network and test code. The remaining implementation still has migration seams:

- connectivity storage, route-graph construction, endpoint attempts, relay
  execution, RPC carrier choice, and retry classification remain coupled;
- canonical sessions and persistent multiplexed channels still implement
  different body and response paths;
- node destination is supplied to delivery separately from the inner object-RPC
  request contract;
- object-RPC service methods are still named routes, overlapping node-routing
  terminology;
- the network layer still owns a service-method catalog in addition to
  `sinomo-service` dispatch state;
- payload integrity and confidentiality depend on properties of the selected
  transport rather than one end-to-end routed-session contract; and
- the current data path may spill slow-consumer bodies to local temporary
  storage instead of relying on bounded stream backpressure.

The exact current mechanics remain documented in the developer
[network data-path reference](../developer/network-data-path.md) until their
owning code is deleted.

## Non-goals

This migration does not redesign:

- cluster membership, Raft, or KV authority;
- SOBJ's public binary encoding;
- domain authorization or node-local consent;
- durable partial-transfer resume;
- transparent migration of a live stream to another path;
- a universal idempotency or response-replay service;
- a globally authoritative online/offline oracle; or
- the workspace crate structure.

Existing direct transport implementations, identity primitives, SOBJ codecs,
and useful integration tests should be reused behind the new contracts where
they already satisfy them.

## Delivery Rules

1. New production entry points are introduced only with a named old entry point
   they replace.
2. A migrated service method cannot retain payload-size or fallback branches
   into the old carrier.
3. No compatibility shim, legacy adapter, deprecated alias, dual-protocol
   negotiation, or old-to-new translation path is introduced or retained.
   Producers and consumers move to the new contract together in one bounded
   migration slice.
4. Each stage adds boundary tests before migrating another service family.
5. Old code may remain only while it has a named production caller. Removing
   the final caller deletes the complete superseded path in the same change,
   including unused modules, exports, configuration, feature flags, tests,
   fixtures, and documentation. Dead code is not retained for hypothetical
   fallback or rollback.
6. Rollback uses source-control revert or a corrected forward change, never a
   dormant runtime switch to the old implementation.
7. Protocol numbers do not change merely because the implementation is being
   refactored.

## Stage A: Contracts and Conformance Harness

Define the transport-neutral types without changing production delivery:

- `AuthenticatedLink` and direct-stream capability metadata;
- immutable `ConnectivitySnapshot` and `ConnectivityView`;
- pure `plan_routes` inputs and outputs;
- `RoutedConnector`, routed availability, `PeerStream`, and channel-binding
  metadata;
- one-call-per-stream object-RPC client and server functions; and
- explicit RPC delivery outcomes: not sent, rejected, possibly applied, and
  completed.

Build the conformance harness first with loopback and `tokio::io::duplex`.
Tests must not require a daemon, cluster KV, filesystem cache, or real network.

Exit criteria:

- `sinomo-network` module-boundary tests enforce the dependency direction;
- the object-RPC module has no dependency on routing or connectivity;
- the routing module has no dependency on SOBJ, object RPC, or service
  registries; and
- a fake transport can run the complete RPC request/response state machine.

## Stage B: Connectivity Read Model

Move connectivity record validation, expiry, snapshot publication, and status
derivation behind `ConnectivityView` and an explicit observation sink.

Keep these facts separate:

- authoritative membership;
- fresh presence;
- direct reachability;
- direct unreachability; and
- unknown or expired direct-edge evidence.

Connectivity does not derive or publish multi-hop reachability. Routing owns
candidate paths, active direct and relayed sessions, and routed availability.

The current cluster control plane remains responsible for publishing
source-owned observations. `sinomo-cluster` adapts its committed projection into
the directory and connectivity interfaces; the connectivity module does not
gain a KV dependency.

Exit criteria:

- reads are side-effect free;
- stale or superseded evidence cannot authorize a path;
- asymmetric reachability is preserved;
- one node's observation cannot extend another node's source-owned generation;
  and
- CLI and app status can explain which component produced a displayed state.

## Stage C: Direct Routed Streams

Implement `RoutedConnector::connect` for one-edge paths using an existing direct
transport. Establish an end-to-end routed-session handshake even on a direct
path so direct and relayed streams have the same peer and channel-binding
semantics.

Connection pooling and multiplexing remain internal. A returned stream behaves
like a socket: it is independently cancellable, bound to one peer, and reports a
terminal error when its route fails.

Exit criteria:

- an unauthorized or removed node cannot establish a routed stream;
- destination mismatch fails before application bytes are accepted;
- alternative endpoints may be tried before `connect` succeeds;
- no bytes are replayed after a returned stream fails; and
- direct stream throughput is measured against the underlying transport
  baseline.

## Stage D: Component-wide Routed Sessions

Implement multi-hop session establishment and opaque stream forwarding for all
node pairs in a routable connected component. This stage absorbs transitive
connectivity establishment entirely into routing: a component is useful only
when the routing executor can realize its eligible paths as endpoint sessions.

The signed session descriptor binds source, destination, session ID, ordered
path, hop limit, deadline, and end-to-end key material. The destination returns
a signed acceptance. Relays validate only routing authority and forward bounded
encrypted data under stream backpressure.

The first implementation deliberately fails a stream when any path edge fails.
It does not migrate or resume the stream. A later proposal may add those
semantics without changing the `PeerStream` destination contract.

Exit criteria:

- every ordered node pair in connected line, ring, star, and mesh conformance
  topologies can establish a target-bound stream when the required direct edges
  remain valid;
- every path emitted from a bounded conformance graph is either realized by the
  routing executor or fails with a specific edge rejection that updates the
  next connectivity snapshot;
- connectivity never stores synthetic transitive edges, derives component
  membership, or reports routed sessions as its own state;
- an intermediate relay cannot retarget a session, impersonate an endpoint,
  alter protected bytes, or create a successful destination acceptance;
- loops, repeated nodes, expired deadlines, and exhausted hop limits fail
  before stream acceptance;
- relay memory remains bounded under a stalled next hop; and
- route failure produces connectivity evidence without changing membership.

## Stage E: Stream-native Object RPC

Implement the new SOBJ RPC protocol over one authenticated duplex stream per
call. The service method catalog remains in `sinomo-service`; object RPC carries
method identifiers but owns no second registry.

Request and response heads are bounded. Bodies are incremental and
content-digested. Inline encoding is allowed only as an encoding optimization;
it cannot select another protocol or limit the response.

Known bodies bind their length and digest in the authenticated head. One-pass
unknown bodies use an origin-authenticated finish record bound to the request
and channel; the target cannot commit an irreversible effect based on those
bytes before verifying completion.

Exit criteria:

- all request/response combinations around every inline and frame boundary use
  the same protocol;
- a one-byte request can receive a response larger than every in-memory window;
- known and unknown lengths share the same public call API;
- deferred finish records bind unknown-length content to the authenticated
  request and destination;
- response production can begin before the complete request body is consumed;
- malformed or oversized heads fail before large allocation;
- body corruption and destination/channel-binding mismatch are detected; and
- a possibly applied operation is never automatically resent.

Required payload matrix includes at least zero, one byte, the inline boundary
minus/at/plus one, the largest frame minus/at/plus one, the receive-window
boundary minus/at/plus one, and a value materially larger than the window. Every
request size is crossed with every response size.

## Stage F: Service Migration

Migrate production traffic in bounded slices:

1. loopback ping and benchmark methods;
2. read-only daemon status and observation methods;
3. cluster-control methods with existing stable operation identities;
4. KV and Raft transport adapters;
5. DFS metadata and content methods;
6. rollout, agent, remote-management, and remaining services.

Each slice must:

- compare direct and relayed behavior;
- run the payload matrix relevant to the method;
- prove authorization at the final service boundary;
- define retry and idempotency behavior;
- add nonzero latency and throughput gates where performance is material; and
- delete that service's old delivery path and every test, export, branch, or
  helper made unused by the migration.

There is no whole-workspace fallback toggle. Rollback is performed by reverting
a bounded migration change, not by maintaining two production carriers for the
same method.

## Stage G: Deletion and Single-Crate Consolidation

After the final production caller migrates:

- delete the canonical object-RPC call carrier;
- delete the persistent object-RPC channel carrier;
- delete RPC-aware relay envelopes and carrier selection;
- delete duplicate network service-method catalogs;
- delete every compatibility shim, legacy adapter, deprecated alias,
  superseded protocol negotiation branch, and fallback selector;
- remove legacy message-routing exports and all tests and fixtures that cover
  only removed behavior;
- remove unused configuration keys, environment variables, feature flags,
  metrics, error variants, documentation, and public re-exports;
- remove network-owned application-body spool files; and
- reduce the `sinomo-network` public facade to deliberate exports backed by the
  one remaining implementation.

Keep direct links, connectivity, routing, and object RPC as explicit modules in
the existing R0 `sinomo-network` crate. Add a network module-boundary test so
forbidden reverse imports fail in CI. The workspace package graph and existing
trust-ring classification do not change.

Exit criteria:

- exactly one production node-routing executor exists;
- exactly one object-RPC protocol implementation exists;
- exactly one service-method registry exists;
- no payload-size branch selects a transport or RPC carrier;
- no compatibility, legacy, fallback, deprecated, or dead-code path remains;
- no removed protocol version is accepted, negotiated, documented, or tested;
- no RPC or SOBJ type appears in the routing module;
- no `NodeId` route planning appears in the object-RPC module; and
- the current carrier-only sections of the network data-path reference are
  removed rather than relabeled as the new design.

## Validation Gates

The migration is not complete without all of the following:

| Surface | Required evidence |
|---|---|
| Dependency boundaries | Existing workspace trust-ring checks plus `sinomo-network` module forbidden-dependency tests. |
| Connectivity | Directed direct-edge, expiry, provenance, generation, and direct-status tests with no synthetic transitive edges. |
| Planning | Deterministic direct, relay, no-route, stale-input, capability, and loop tests whose paths are accepted by the executor. |
| Routing | Real multi-node connected-component establishment plus bounded-graph all-pairs path realization, identity, tamper, backpressure, and failed-edge feedback tests. |
| Object RPC | In-memory conformance tests plus the complete request/response payload matrix. |
| Retry safety | Not-sent versus possibly-applied fault injection around every request phase. |
| Performance | Raw direct-link baseline, direct routed stream, relayed stream, and object-RPC measurements by target. |
| Resource safety | Bounded unauthenticated heads, streams, queues, relay buffers, and cancellation cleanup. |
| Live cluster | Current-commit cluster health, doctor, node status, and targeted direct/relay probes. |

Performance acceptance must report per-target results rather than only an
aggregate. A benchmark with a zero throughput threshold is diagnostic evidence,
not an acceptance gate.
