# Sinomo Network Architecture

This document defines the normative contracts between direct transports,
connectivity tracking, node routing, and object RPC. These are separate layers
with one-way dependencies, all implemented inside the single `sinomo-network`
crate. The separation is architectural and internal; it does not create new
workspace crates.

The current carrier formats and tuning bounds are recorded in the developer
[network data-path reference](developer/network-data-path.md). The work required
to replace that implementation without creating another parallel path is tracked
in the [network layer separation plan](plans/network-layer-separation.md).

## Purpose

Sinomo networking has one fundamental responsibility: establish an
authenticated communication path from one authorized node to another. Object
RPC is a protocol that may use that path; it is not part of path discovery or
relay routing.

The design separates five contracts:

1. direct link transports connect adjacent nodes;
2. connectivity tracking records fresh, directed reachability evidence;
3. node routing turns that evidence into target-bound duplex streams;
4. object RPC exchanges SOBJ values over any authenticated duplex stream; and
5. domain services own methods, authorization, idempotency, and side effects.

Networking does not interpret KV, DFS, agent, or application semantics. It does
not read cluster storage directly, grant membership, or turn reachability into
authority.

## Layering and Dependency Direction

```text
domain services and the service registry
                  |
                  v
        SOBJ object-RPC protocol
      one call per authenticated stream
                  |
                  v
       routed connector and peer stream
                  |
       +----------+-----------+
       |                      |
       v                      v
 pure route planner     routed-session executor
       |                      |
       v                      v
 connectivity snapshot   direct authenticated links
       ^                      ^
       |                      |
 cluster directory       transport modules
```

| Layer | Owns | Must not own |
|---|---|---|
| Domain service | Target selection, typed request and response values, method semantics, authorization, operation identity, idempotency, and side effects. | Endpoint choice, relay paths, transport framing, or connectivity mutation. |
| Object RPC | Method and request identifiers, SOBJ request and response framing, streamed bodies, application authentication context, and RPC failure classification. | Node path selection, endpoint dialing, relay forwarding, membership, or transport-specific limits. |
| Node routing | Authorized path planning, routed-session establishment, relay execution, target identity, connection pooling, and stream lifecycle. | SOBJ, RPC methods, handler retries, service catalogs, or domain authorization. |
| Connectivity | Directed observations, freshness, provenance, path metrics, immutable snapshots, watches, and derived reachability. | Membership authority, service dispatch, route execution, or application retries. |
| Direct transport | Dialing, listening, adjacent-peer authentication, transport capabilities, and multiplexed direct streams. | Multi-hop route choice, cluster status, SOBJ, or RPC. |
| `sinomo-cluster` | Authorized members, node identity keys, endpoint projection, and publication of source-owned connectivity evidence. | Private RPC framing or transport-specific application protocols. |

The daemon composes these layers. Higher-level crates use their public contracts
and do not bypass them with ad hoc cluster sockets or private wire formats.

## Direct Link Contract

A direct transport connects adjacent nodes. TCP, Iroh, loopback, and future
transport modules implement the same logical contract:

```rust,ignore
trait LinkProvider {
    async fn connect(&self, endpoint: &Endpoint) -> Result<AuthenticatedLink>;
    async fn accept(&self) -> Result<AuthenticatedLink>;
}
```

An `AuthenticatedLink` provides:

- the verified local and adjacent-peer node identities;
- a transport capability snapshot;
- bounded, independently cancellable duplex streams;
- transport-local liveness and close/reset reporting; and
- observations such as handshake latency and delivered-byte progress.

The link authenticates only the adjacent peer. It does not assert that the peer
is an authorized cluster member; the routing executor checks the current
directory before using the link. A transport cannot promote a node into the
cluster by completing a handshake.

Direct links may be pooled and multiplexed. Their stream and buffer bounds are
resource limits, not aggregate application-message limits. A transport never
selects another application protocol because a stream crosses a byte threshold.

## Connectivity Contract

Connectivity is a read model of evidence, not an authority source and not an
active routing engine.

Every edge is directed. Evidence that A can reach B says nothing about B
reaching A. An observation records at least:

- source and target node identity;
- endpoint identity and transport capability;
- state and failure reason;
- latency and, when measured, delivery-bandwidth estimates;
- provenance and source-owned generation;
- observation and expiry times; and
- the directory generation against which the observation was valid.

The public read boundary is an immutable, generation-stamped snapshot plus a
watch for replacements:

```rust,ignore
trait ConnectivityView {
    fn snapshot(&self) -> Arc<ConnectivitySnapshot>;
    fn subscribe(&self) -> watch::Receiver<Arc<ConnectivitySnapshot>>;
}
```

Reads are observational. They never schedule probes, persist refreshes, or
change a node's status. Probe owners and routed-session executors submit
observations through an explicit sink. The tracker validates ownership,
deduplicates updates, expires stale facts, and publishes a new snapshot.

### Membership, Presence, and Reachability

The implementation must not collapse distributed state into one authoritative
`online` boolean. These are distinct:

| State | Meaning |
|---|---|
| Authorized | The current cluster directory permits communication with the node. |
| Present | A fresh, authenticated liveness statement exists. |
| Directly reachable | A fresh usable edge exists from the observer to the node. |
| Routed reachable | The current graph contains an authorized usable path. |
| Connected | An authenticated direct or routed session is currently established. |
| Unreachable | Fresh evidence excludes every currently authorized path from this observer. |
| Unknown | Evidence is absent, expired, or insufficient. |
| Departed | The authoritative directory no longer contains the node. |

Operator-facing `online`, `degraded`, or `offline` labels are timestamped
projections of these facts. They are not stored as routing authority. In
particular, one observer's failure cannot prove that a node is globally offline.

### Cluster Directory

Connectivity and routing do not read cluster KV. `sinomo-cluster` supplies a
directory interface containing:

- authorized members and identity keys;
- advertised endpoints and endpoint generations;
- endpoint scope and transport requirements;
- source-owned connectivity facts accepted by the control plane; and
- a monotonic generation for cache invalidation.

Local observations may supplement route choice for their source node. They do
not authorize an absent member, extend an expired remote observation, or
override a newer non-empty authoritative projection.

## Routing Contract

The routing layer exposes node-addressed, authenticated, socket-like streams. A
caller supplies a destination node and connection constraints; it never chooses
LAN versus Iroh, direct versus relay, or a specific intermediate node.

```rust,ignore
trait RoutedConnector {
    async fn connect(
        &self,
        destination: NodeId,
        options: ConnectOptions,
    ) -> Result<PeerStream, ConnectError>;
}

struct PeerStream {
    io: Box<dyn AsyncReadWrite>,
    local: NodeId,
    peer: NodeId,
    session_id: SessionId,
    channel_binding: [u8; 32],
    route_trace: RouteTrace,
}
```

`route_trace` is diagnostic evidence. It cannot be used by an application to
grant authority or select a different service behavior.

The connector may cache an authenticated routed connection and open cheap
logical streams over it. Each returned `PeerStream` nevertheless has ordinary
socket failure semantics: it is bound to one destination, never silently moves
to another destination, and fails if its underlying routed session cannot
continue.

Transparent stream migration and durable partial-transfer resume are not part
of this contract. They require separately specified sequence, acknowledgement,
reattachment, and garbage-collection semantics.

### Pure Planning

Route planning is a pure operation over immutable inputs:

```rust,ignore
fn plan_routes(
    directory: &DirectorySnapshot,
    connectivity: &ConnectivitySnapshot,
    source: NodeId,
    destination: NodeId,
    constraints: RouteConstraints,
) -> Vec<CandidatePath>;
```

The planner performs no dialing, probing, persistence, retry, or RPC. Its graph
contains only authorized nodes and usable directed edges. Every cached plan is
keyed by all directory, connectivity, transport-capability, and constraint
generations that affect validity.

A direct connection is a one-edge path. A relay connection is the same path
model with additional edges. Purpose constraints may require capabilities such
as full duplex, privacy, maximum hops, or a connection deadline, but they do not
change the routing safety rules.

### Routed-Session Establishment

For a path A to B to C:

1. A selects the candidate path `[A, B, C]` from a current snapshot.
2. A creates an origin-signed `OpenSession` binding A, destination C, a unique
   session ID, the ordered path, hop limit, connection deadline, and end-to-end
   key material.
3. B verifies membership, signatures, path position, loop freedom, deadline,
   and hop limit. It may reject the session but cannot change A, C, or the
   session identity.
4. B establishes or reuses a direct link to C and forwards the unchanged
   end-to-end session descriptor.
5. C accepts only when the destination equals its local identity and returns a
   destination-signed acceptance bound to the same session.
6. A and C derive end-to-end authenticated-encryption keys and a channel
   binding. Only then may `connect` return a `PeerStream`.

Intermediate relays forward opaque encrypted stream data. They may observe the
path identities and bounded traffic metadata required for forwarding, but they
cannot inspect RPC values, alter the destination, impersonate either endpoint,
or produce a successful end-to-end acceptance.

A routed A-to-C session is not inserted as a new direct connectivity edge.
Connectivity may report it as an active routed session or use its measurements
as evidence for the constituent directed edges.

### Relay Backpressure

Relays copy between bounded streams and stop reading when the next hop cannot
accept data. Backpressure propagates through the path. Routing does not
materialize complete application values and does not spill application bytes to
disk. Durable transfer belongs to a domain content protocol such as SIFS, not to
the generic routed-stream layer.

Frame, queue, and stream-credit bounds protect resident resources. They never
define a maximum logical stream length.

### Connection Attempts and Failures

The routing executor may try bounded alternative paths while establishing a
session because no application stream has yet been returned. Once `connect`
succeeds:

- routing does not replay bytes on a new path;
- a broken route closes or resets the stream;
- the failure identifies whether establishment failed or an established stream
  broke; and
- observations are submitted to connectivity without changing membership.

This boundary prevents carrier or relay failures from automatically replaying a
possibly executed RPC operation.

## Object-RPC Contract

Object RPC is a transport-independent protocol over one authenticated duplex
stream per call. It can run over a `PeerStream`, an authenticated direct stream,
an in-memory duplex stream, or another adapter satisfying the same peer-context
contract.

Plain `AsyncRead + AsyncWrite` is insufficient for remote calls because it does
not identify the peer. An RPC transport also supplies:

- verified local and peer identities, or an explicit unauthenticated-local
  classification;
- a unique session or connection identifier;
- a channel binding derived from the authenticated stream; and
- the stream's close/reset result.

The client writes one request direction and reads one response direction. The
server may respond before consuming the complete request body. Independent
calls use independent logical streams; object RPC does not implement another
connection multiplexer.

Object RPC owns:

- protocol negotiation for the RPC contract only;
- method and request identifiers;
- bounded request and response heads;
- incrementally encoded SOBJ values and optional attachments;
- explicit body completion and cancellation;
- application authentication and authorization context; and
- failure classification needed for domain retry decisions.

The service dispatch name is a `method` or `service method`, not a network
`route`. The method catalog and handler registry have one owner in
`sinomo-service`; object RPC transports calls to that registry but does not
maintain a second service catalog.

### Payload Invariants

Every request and response uses the same RPC framing at every size:

- request size cannot select the response carrier;
- inline encoding is an optimization, not a protocol boundary;
- heads have small explicit allocation limits;
- bodies are processed incrementally under stream backpressure;
- frame and buffer limits are not aggregate value limits;
- known and unknown body lengths use the same call interface; and
- a response may be larger or smaller than its request without changing
  protocol or delivery semantics.

RPC framing includes an end-to-end content length when known and a cryptographic
content digest for transported SOBJ bytes. A one-pass body whose length or
digest is not known at the start declares deferred completion; its
origin-authenticated finish record supplies the final length and digest bound to
the request and channel. The receiver computes both while consuming the stream
and verifies completion before accepting the value. A handler may process
streamed bytes speculatively, but it cannot commit an irreversible effect based
on unverified content unless its domain protocol supplies an independently
verified chunk or content contract.

The routed stream's authenticated encryption protects bytes in transit. The
content digest additionally binds application authentication and stable object
identity independently of carrier framing. Responses apply the same rule with
the target as the authenticated producer.

### Authentication and Destination Binding

An authenticated RPC request binds at least:

- caller and intended peer identity;
- channel binding and RPC request ID;
- service method and operation identity;
- request-head digest and body content descriptor;
- freshness and replay data; and
- relevant authorization context.

Binding the intended peer prevents a valid request from being re-enveloped for
another node. The target service verifies the request again at the final effect
boundary. Transport authentication never replaces domain authorization or
node-local consent.

### Retry and Idempotency

Object RPC does not choose another endpoint or route. If its stream fails, it
returns one of the domain-relevant delivery states:

| State | Meaning |
|---|---|
| Not sent | The caller has definitive evidence that no request bytes crossed the authenticated stream boundary. |
| Rejected | The peer returned a verified rejection before handler execution. |
| Possibly applied | Request delivery or handler execution may have occurred, but no verified completion was received. |
| Completed | A response and its integrity checks completed successfully. |

Only `Not sent` is generically safe to retry. Once any request byte is handed to
an established stream, a failure is `Possibly applied` unless a verified peer
rejection proves otherwise. A domain may retry `Possibly applied` only with a
stable operation identity and an idempotency or outcome query contract.
Networking does not provide a universal response replay cache.

Explicit operation deadlines belong to the RPC/domain request. Connect and
no-progress health limits are separate routing and stream-liveness controls and
must not silently extend an operation deadline.

## Protocol Names and Evolution

There is no global "Sinomo network protocol version." Independently released
contracts evolve independently:

- each direct transport versions its own handshake and capabilities;
- the routed-session and relay protocol uses its own namespace;
- SOBJ object RPC uses its own namespace; and
- the SOBJ binary encoding retains its own format version.

Capabilities, limits, and optional fields are negotiated as capabilities when
backward-compatible. A version changes only for an incompatible contract
change, not for an implementation refactor or tuning adjustment. Compatibility
adapters live at protocol boundaries and never cause routing to choose an RPC
protocol based on payload shape.

The current consolidated implementation protocol v1 described in the
[data-path reference](developer/network-data-path.md) predates this separation.
It is migration input, not the version number of every contract above.

## Transport Modules

Transport modules describe capabilities, start and stop listeners, dial direct
endpoints, accept adjacent links, and expose local endpoints. Disabling a module
removes it from future link selection and stops it safely; loaded native code
remains alive while a host reference exists.

The native module ABI may use SOBJ for descriptors and endpoint lists, but
transport modules do not use object RPC internally unless they independently
act as an RPC application. Core routing depends on transport-neutral link
contracts and does not depend on a transport SDK.

## Single-Crate Module Boundaries

The entire network implementation remains in `crates/sinomo-network`. Its
internal modules enforce the architecture without changing the workspace crate
graph:

| Module boundary | Responsibility |
|---|---|
| Direct link | Authenticated adjacent transports, listeners, connection pools, and streams. |
| Connectivity | Evidence validation, immutable snapshots, watches, and derived status. |
| Routing | Pure planning, routed sessions, relays, and `RoutedConnector`. |
| Object RPC | SOBJ RPC over authenticated duplex streams. |
| Public facade | Stable exports and composition types without a second implementation path. |

The exact file layout may evolve while the migration is in progress, but the
dependency direction is fixed. Object RPC cannot import routing or
connectivity. Routing cannot import object RPC, SOBJ request types, or service
catalogs. Connectivity cannot import route execution or RPC. Direct-link
modules cannot import any higher network layer.

`sinomo-service` remains the single external owner of service-method catalogs,
dispatch, authorization layers, and handlers. The daemon adapts its service
registry to the object-RPC server contract; `sinomo-network` does not gain a
dependency on `sinomo-service` or maintain a parallel catalog.

Module-boundary tests inside `sinomo-network` enforce forbidden imports and
public API direction. The workspace trust-ring boundary remains unchanged:
`sinomo-network` is one R0 crate, networking and KV have no production
dependency on one another, and `sinomo-cluster` is their only bridge.

## Architectural Invariants

1. Object RPC depends on an authenticated duplex-stream contract, never on node
   routing, endpoint selection, connectivity stores, or a specific transport.
2. Routing forwards opaque bytes and never imports SOBJ, RPC methods, service
   catalogs, body modes, or domain retry policy.
3. Connectivity publishes immutable directed evidence and never grants
   membership, executes routes, or turns one observer's failure into global
   offline authority.
4. A routed stream is end-to-end bound to its source, destination, session, and
   channel binding before application bytes are accepted.
5. Relays cannot change the destination, authenticate as the origin or target,
   or inspect or alter end-to-end protected application bytes.
6. One RPC call uses one logical duplex stream and the same protocol at every
   request and response size.
7. Frame, queue, window, and buffer limits bound resident resources, not total
   SOBJ or stream length.
8. Routing may try alternatives only before returning a stream. It never
   replays bytes after an established stream fails.
9. Only a domain with a stable operation identity and idempotency or outcome
   contract may retry a possibly applied call.
10. The service-method registry has one owner in `sinomo-service`; node routes
    and service methods are different concepts and use different names.
11. Network and KV have no production dependency on one another;
    `sinomo-cluster` is their only R0 bridge.
12. Higher-level crates do not open ad hoc cluster sockets, select relay paths,
    or create private chunk-transfer RPCs.
13. Protocol versions identify independently released wire contracts, not
    implementation milestones.
14. Target services re-check domain authority and node-local consent at the
    final effect boundary.

The [clustering architecture](clustering-architecture.md) defines directory
authority. The [Sinomo Object format](sinomo-object-format.md) defines the
binary object representation. Current implementation gaps and deletion gates
are tracked in the
[network layer separation plan](plans/network-layer-separation.md).
