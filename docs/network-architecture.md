# Sinomo Network Architecture

This document defines the normative contracts between direct transports,
connectivity tracking, node routing, and object RPC. These are separate layers
with one-way dependencies, all implemented inside the single `sinomo-network`
crate. The separation is architectural and internal; it does not create new
workspace crates.

The current implementation map and resource bounds are recorded in the
developer [network data-path reference](developer/network-data-path.md). The
completed migration stages and deletion criteria are recorded in the
[network layer separation plan](plans/network-layer-separation.md).

## Purpose

Sinomo networking has one fundamental responsibility: establish an efficient
communication path from one authorized node to another. Adjacent-peer identity
is established once by transports that provide authentication. Object RPC uses
that established path without signing, hashing, or encrypting each call.

The design separates five contracts:

1. direct link transports connect adjacent nodes;
2. connectivity tracking records fresh, directed reachability evidence;
3. node routing turns that evidence into target-bound duplex streams;
4. object RPC exchanges SOBJ values over any reliable duplex stream with an
   established peer context; and
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
         one call per stream
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
| Object RPC | Method and request identifiers, SOBJ request and response framing, streamed bodies, and RPC failure classification. | Node path selection, endpoint dialing, relay forwarding, membership, content verification, signatures, encryption, or transport-specific limits. |
| Node routing | Transitive reachability, authorized path planning, path realization, routed-session establishment, relay execution, target identity, connection pooling, and stream lifecycle. | SOBJ, RPC methods, handler retries, service catalogs, or domain authorization. |
| Connectivity | Direct-edge observations, freshness, provenance, path metrics, immutable snapshots, and watches. | Transitive reachability, membership authority, service dispatch, route execution, or application retries. |
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

Connectivity is a read model of direct-edge evidence, not an authority source
and not an active routing engine. It never establishes a relayed relationship or
publishes a synthetic transitive edge.

Every connectivity-graph edge describes an observed adjacent-node relationship
that the direct-link layer can attempt. Every edge is directed: evidence that A
can directly reach B says nothing about B reaching A. An observation records at
least:

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

### Membership, Presence, and Direct Connectivity

The implementation must not collapse distributed state into one authoritative
`online` boolean. These are distinct:

| State | Meaning |
|---|---|
| Authorized | The current cluster directory permits communication with the node. |
| Present | A fresh, authenticated liveness statement exists. |
| Directly reachable | A fresh usable edge exists from the observer to the node. |
| Directly unreachable | Fresh evidence excludes a direct edge from this observer. |
| Direct connectivity unknown | Direct evidence is absent, expired, or insufficient. |
| Departed | The authoritative directory no longer contains the node. |

Operator-facing `online`, `degraded`, or `offline` labels are timestamped
projections composed from these facts and routing-owned session state. They are
not stored as routing authority. In particular, one observer's failure cannot
prove that a node is globally offline, and connectivity cannot claim multi-hop
reachability merely because graph traversal finds a path.

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

The routing layer is the sole owner of transitive reachability. It turns paths
through the direct connectivity graph into actual node-addressed socket-like
streams. A caller supplies a destination node and
connection constraints; it never chooses LAN versus Iroh, direct versus relay,
or a specific intermediate node.

The primary routing liveness goal is component-wide communication: every pair
of authorized nodes in the same current routable connected component can obtain
a duplex stream with an established endpoint identity, regardless of the
number or arrangement of intermediate relays.

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

The connector may cache an established routed connection and open cheap logical
streams over it. Each returned `PeerStream` nevertheless has ordinary
socket failure semantics: it is bound to one destination, never silently moves
to another destination, and fails if its underlying routed session cannot
continue.

Transparent stream migration and durable partial-transfer resume are not part
of this contract. They require separately specified sequence, acknowledgement,
reattachment, and garbage-collection semantics.

### Routed Availability

Routing, not connectivity, owns the distinction between a graph path and a
usable relationship:

| State | Meaning |
|---|---|
| Candidate path | The current authorized snapshots contain at least one directed path that routing can attempt. |
| Connected | Routing established a direct or relayed session and can open streams to the destination. |
| No path | The current snapshots contain no authorized candidate path. |
| Unknown | Directory or direct-edge evidence is absent, expired, or changing. |

A candidate path is not itself a connection and must not be exposed as a live
socket or successful delivery. An operator-facing `routed reachable` state is
backed by a current established routed session; otherwise the state is
`candidate`, `unknown`, or `unreachable` with its observation time.

### Routable Connected Components

Routing derives connected components from the authorized directory, current
direct-edge snapshot, transport capabilities, and connection constraints. It
does not persist component membership in connectivity.

Because direct-edge observations are directed, a routable connected component
uses mutual path reachability for the required duplex-stream capabilities, not
weak undirected connectivity. Component membership may therefore change with
endpoint availability, transport capabilities, privacy constraints, deadlines,
or fresh failed-edge evidence.

For every ordered source and destination pair in one current component, routing
must be able to realize at least one eligible path as a usable session under the
path-realization assumptions below. Intermediate topology is private to routing
and is not part of the application contract.

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

### Path Realization Guarantee

A path returned by the planner is an executable routing plan, not advisory
topology metadata. The routed-session executor understands every edge and relay
step that the planner may emit.

If the directory and connectivity snapshot remain valid for the duration of an
attempt, every edge continues to satisfy its advertised direct-link contract,
and each relay cooperates, then `connect(source, destination)` must establish a
session for every node pair in the same current component.
Routing cannot report `No path` or leave the relationship as graph-only state
without attempting an eligible candidate.

When a path cannot be realized, routing returns a structured establishment
failure identifying the rejected or failed edge, submits that evidence to
connectivity, and tries a bounded eligible alternative within the connection
deadline. This closes the loop between the control-plane graph and the data
plane: stale graph reachability is corrected rather than repeatedly advertised
as usable.

### Routed-Session Establishment

For any source and destination in one routable component, all transitive
establishment is performed by the routing system:

1. The source selects an ordered candidate path from a current snapshot.
2. The source creates an `OpenSession` containing the source, destination,
   unique session ID, ordered path, hop limit, connection deadline, and selected
   record protection.
3. Every intermediate relay validates membership, path position, loop freedom,
   deadline, and hop limit. It may reject the session but cannot change either
   endpoint or the session identity.
4. Each relay establishes or reuses the next direct link and forwards the
   unchanged end-to-end session descriptor.
5. The destination accepts only when the descriptor targets its local identity
   and returns acceptance for the same session.
6. Plaintext records are the default. When the caller explicitly selects
   `RoutedSessionProtection::Aead`, the endpoints derive end-to-end AEAD keys.
   Only then may `connect` return a `PeerStream`.

The accepted endpoint relationship lives in routing's session pool. Routing can
open additional logical streams over it without asking connectivity to create
or persist a synthetic endpoint edge. Protection is part of the pool identity:
an AEAD call cannot reuse a plaintext carrier. Low-level callers select it with
`ConnectOptions::protection`; service-RPC callers use
`ClusterObjectRpcOptions::with_routed_session_protection`.

Reverse reuse also requires the initiating process to advertise a live logical-
stream dispatcher during session establishment. Its advertised scope, rather
than the accepting listener's role, determines whether the carrier can satisfy
main-daemon calls. Main and shadow processes share a node identity, so node
authentication alone cannot make a shadow or caller-only connection a main-
daemon route. A CLI with no dispatcher advertises no reverse route. Dispatcher
replacement changes the local pool identity and fences delivery to its previous
incarnation. This scope is routing metadata, never a service permission or a
membership grant.

The optional reverse scope is echoed in session acceptance. New peers include
it in the AEAD handshake transcript; an older receiver's absent echo selects the
legacy transcript. Missing advertisements never grant reverse reuse. This keeps
forward sessions interoperable during upgrades while preserving reverse main-
daemon connections between peers that explicitly support them.

Consensus route plans bind the complete bootstrap routing projection, including
endpoints. Established carriers use a separate exact authority fingerprint over
the cluster and local identities, authorized members, public keys, roles, states,
and capability sets. Address observations, relay URL updates, and omitted remote
loopback addresses therefore refresh route plans without hiding a valid reverse
connection. Changes to authority still fence reuse; dynamic-directory carriers
are not a fallback for consensus calls. Both fingerprints are computed from one
canonical projection, without querying the dynamic KV directory.

An accepted, authenticated carrier is published without waiting for an outbound
dial to the same peer. The pool mutex and dispatcher/module generation checks
fence publication. A stalled dial must not block incoming RPC handling or the
reverse connection needed to reach a peer behind an asymmetric network route.

Intermediate relays forward stream records without decoding mux, RPC, SOBJ, or
service data. With default plaintext protection this opacity is a layer
boundary, not a security guarantee; trusted relays could inspect bytes. AEAD is
available only for callers that explicitly require confidentiality.

A routed session is not inserted as a new direct connectivity edge. Routing
reports its active session state. Measurements from establishment and delivery
may be submitted as evidence for the constituent direct edges, but connectivity
never owns the transitive relationship or component membership.

### Relay Backpressure

Relays copy between bounded streams and stop reading when the next hop cannot
accept data. Backpressure propagates through the path. Routing does not
materialize complete application values and does not spill application bytes to
disk. Durable transfer belongs to a domain content protocol such as SIFS, not to
the generic routed-stream layer.

Frame, queue, and stream-credit bounds protect resident resources. They never
define a maximum logical stream length.

Cancellation resets one logical stream. Data already in flight may arrive after
that reset, including after the stream slot is removed. Discarding it returns
the shared connection credit; control slabs never grant data credit. A local
reset must not fail the carrier receiver or unrelated streams. Data after an
authenticated peer FINISH remains a protocol error.
Send-credit reservations belong to their queued chunks until the physical
writer takes delivery ownership. Cancelling a credit wait or discarding unsent
chunks refunds only those reservations; transmitted bytes await peer credit.
Queue removals release chunks outside the scheduler lock, since their release
callbacks can themselves schedule credit-return control records.

Transport control keeps scheduling priority whenever its allocation is available.
If it must wait for a control slab, the writer drains already allocated RPC
records, including records enqueued after that wait began, while preserving each
stream's order. This prevents a circular wait on memory retained in its own
transmit queue. The connection and global memory bounds remain unchanged.

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

Object RPC is a transport-independent protocol over one reliable duplex stream
per call. It can run over a `PeerStream`, a direct stream, an in-memory duplex
stream, or another adapter satisfying the same peer-context contract.

The canonical owned-chunk contract supplies:

- transport-established local and peer identities, or an explicit local
  classification;
- a unique session or connection identifier;
- a diagnostic channel binding; and
- the stream's close/reset result.

The client writes one request direction and reads one response direction. The
server may respond before consuming the complete request body. Independent
calls use independent logical streams; object RPC does not implement another
connection multiplexer.

Control records are length-delimited canonical SOBJ values. Object RPC does not
add an authentication tag, signature, content digest, or encryption layer.
After decoding an owned request or response head, it releases the input control
slab before awaiting application work. Retaining those leases across handlers or
consumers can exhaust the bounded control pool and prevent replies from making
progress on the same carrier.

Object RPC owns:

- protocol negotiation for the RPC contract only;
- method and request identifiers;
- bounded request and response heads;
- incrementally encoded SOBJ values and optional attachments;
- explicit body completion and cancellation; and
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

RPC framing includes the body length when known. A one-pass body with an
unknown length declares deferred completion; its finish record supplies the
observed final length. On reliable ordered transports such as TCP, framing and
length are sufficient for transport completion. Object RPC does not reread the
payload for a checksum or cryptographic digest.

Applications that need stable object identity, corruption detection beyond the
selected transport, or artifact verification must carry and verify that digest
in their own typed protocol. A transport digest could not replace that
application-level contract.

### Peer Identity and Authorization

RPC reads caller identity from the already-established transport context. It
does not copy that identity into a signed request envelope or authenticate the
same call again. The final service boundary maps the transport peer to local,
member, or voter authority and applies the live route catalog and node-local
consent. Domain authorization remains independent of transport framing.

### Retry and Idempotency

Object RPC does not choose another endpoint or route. If its stream fails, it
returns one of the domain-relevant delivery states:

| State | Meaning |
|---|---|
| Not sent | The caller has definitive evidence that no request bytes crossed the stream boundary. |
| Rejected | The peer returned a protocol rejection before handler execution. |
| Possibly applied | Request delivery or handler execution may have occurred, but no completion was received. |
| Completed | A complete response was received. |

Only `Not sent` is generically safe to retry. Once any request byte is handed to
an established stream, a failure is `Possibly applied` unless a peer
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
change, not for an implementation refactor or tuning adjustment. This
replacement retains no adapter between superseded internal protocols. A wire
contract change migrates every producer and consumer as one coordinated change,
then deletes the old parser, negotiation branch, framing, tests, and
documentation. Routing never chooses an RPC protocol based on payload shape.

The routed-session and transport layers retain their consolidated
implementation protocol v1. Object RPC evolves in its separate namespace, as
described in the [data-path reference](developer/network-data-path.md).

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
| Connectivity | Direct-edge evidence validation, immutable snapshots, watches, and direct status. |
| Routing | Pure planning, routed sessions, relays, and `RoutedConnector`. |
| Object RPC | SOBJ RPC over reliable duplex streams with an established peer context. |
| Public facade | Stable exports and composition types without a second implementation path. |

The exact file layout may evolve, but the dependency direction is fixed.
Object RPC cannot import routing or
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

1. Object RPC depends on a reliable duplex-stream contract with an established
   peer context, never on node
   routing, endpoint selection, connectivity stores, or a specific transport.
2. Routing forwards opaque bytes and never imports SOBJ, RPC methods, service
   catalogs, body modes, or domain retry policy.
3. Connectivity publishes immutable directed direct-edge evidence and never
   grants membership, derives transitive reachability, executes routes, or turns
   one observer's failure into global offline authority.
4. Every path emitted by the planner is executable by the routed-session
   executor under the same directory, edge-capability, and relay assumptions.
   Graph reachability that fails realization produces specific edge feedback.
5. Every authorized node pair in the same current routable connected component
   can establish a direct or relayed duplex stream under stable
   path-realization assumptions.
6. A routed stream is bound to its source, destination, session, and selected
   protection mode before application bytes are accepted.
7. Relays never decode mux, RPC, SOBJ, or service payloads. Confidentiality
   against relays is guaranteed only when the caller explicitly enables AEAD.
8. One RPC call uses one logical duplex stream and the same protocol at every
   request and response size.
9. Frame, queue, window, and buffer limits bound resident resources, not total
   SOBJ or stream length.
10. Routing may try alternatives only before returning a stream. It never
   replays bytes after an established stream fails.
11. Only a domain with a stable operation identity and idempotency or outcome
   contract may retry a possibly applied call.
12. The service-method registry has one owner in `sinomo-service`; node routes
    and service methods are different concepts and use different names.
13. Network and KV have no production dependency on one another;
    `sinomo-cluster` is their only R0 bridge.
14. Higher-level crates do not open ad hoc cluster sockets, select relay paths,
    or create private chunk-transfer RPCs.
15. Protocol versions identify independently released wire contracts, not
    implementation milestones.
16. Target services re-check domain authority and node-local consent at the
    final effect boundary.
17. No superseded internal protocol, compatibility shim, legacy adapter,
    deprecated alias, fallback selector, unused export, dead test, fixture,
    configuration, or documentation remains after its final caller migrates.

The [clustering architecture](clustering-architecture.md) defines directory
authority. The [Sinomo Object format](sinomo-object-format.md) defines the
binary object representation. The completed implementation stages and deletion
gates are recorded in the
[network layer separation plan](plans/network-layer-separation.md).
