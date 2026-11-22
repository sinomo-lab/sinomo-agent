# Network Data-Path Reference

This page maps the current stream-native network implementation. The normative
contracts are in the [network architecture](../network-architecture.md); the
public binary object encoding is specified by the
[Sinomo Object format](../sinomo-object-format.md).

All routed cluster calls use one production path:

```text
sinomo-service registry
        |
typed request/response head plus optional body
        |
object_rpc_stream.rs -- one call per reliable duplex stream
        |
routing.rs -- direct or multi-hop routed stream
        |
direct_link.rs and transport modules
```

No payload-size branch selects another carrier. Relayed and direct calls use the
same object-RPC protocol, and relays forward opaque routed-stream bytes.
Persistent local `SLC1` carriers and every routed call use the owned-record
path.

The daemon listener distinguishes three stream preludes: an authenticated
direct-transport stream, a persistent local `SLC1` carrier accepted only from a
loopback peer address, and one-call raw frames reserved for the pre-membership
join challenge and signed join registration. Local-carrier calls carry an
unauthenticated-local peer context and dispatch as local-node requests. Any
other raw frame is rejected, so byte-stream adapters remain library
compatibility and test boundaries rather than a second local RPC protocol.

## Call and Body Contracts

`rpc_body.rs` owns the application-facing values:

- `ObjectRpcCall` and `ObjectRpcResult` pair a typed SOBJ head with an optional
  body;
- `ObjectRpcIncomingRequest` and `ObjectRpcOutgoingResponse` are the server-side
  equivalents; and
- `ObjectRpcBodySource` and `ObjectRpcIncomingBody` provide bounded streaming
  producers and consumers.

An ordinary value remains a single SOBJ value regardless of encoded size. The
owned service adapter moves its encoded bytes through the body stream instead
of embedding a potentially large value in the control head. Routes use the
incremental mode by default and decode SOBJ directly from ownership-bearing
chunks through `ServiceContext::decode_request_body`. A route must opt into
`ServiceRoute::with_buffered_request_value` when a legacy handler still needs
one contiguous encoded request buffer.
Semantic attachments remain attachments. Callers do not observe transport
frames or choose a carrier.

`service_rpc_stream.rs` encodes the typed service head into the application-head
slot of the object-RPC protocol. It does not implement routing or keep a service
catalog. The single method registry, route policy, concurrency admission,
readiness, and final handler dispatch live in `sinomo-service`.

Received body chunks move from the object-RPC pump directly into
the application body. The service adapter does not spawn a second body pump or
copy chunks through an `AsyncRead` scratch buffer. Incremental SOBJ decoding
retains one bounded chunk at a time, and internal proxy forwarding can transfer
an exclusively owned received slab into the next call.
`ObjectRpcBodySource::from_incoming_body` is the public ownership-transfer
adapter; it consumes the incoming body and moves its leases into the outgoing
producer. `AsyncRead` and contiguous value materialization remain explicit
compatibility adapters.

## Routed Streams

`routing.rs` is the only production node-routing executor. A
`MultiHopRoutedConnector` plans against one immutable connectivity snapshot and
realizes either a direct stream or a relay chain. The routed-session handshake
binds source, destination, session identity, and selected record protection
before application bytes are accepted. `ConnectOptions::protection` defaults
to `RoutedSessionProtection::Plaintext`; AEAD is an explicit per-endpoint
opt-in. Service-RPC callers select the same policy with
`ClusterObjectRpcOptions::with_routed_session_protection`. Protection is part
of the persistent carrier pool key, so modes are never mixed by reuse.

Intermediate nodes run `accept_or_relay_routed_session`. They authenticate the
adjacent links once and validate the routed envelope. Endpoint IDs select dialing
addresses; an already traversed edge may name a caller-private route hint. Each
relay validates its own outbound endpoint against its directory, while the
destination validates the authenticated incoming peer and path without requiring
those private addresses to be published in its own directory. Linux TCP-to-TCP relays
use `splice` in both directions; portable relays move each record through one
pooled slab. Relay memory is constant per direction. Relays do not
decode SOBJ heads, inspect service methods, or select an RPC implementation.

`route.rs` composes routing with object RPC through `ClusterObjectRpcClient`.
It selects endpoints only while opening the routed stream. Once application
bytes have entered a stream, routing does not replay them on another path.

## Object-RPC Framing and Backpressure

`object_rpc_stream.rs` implements `sinomo.object-rpc.stream.v2`. One duplex
stream carries exactly one request and one response. The current resource
bounds are:

- 16 KiB maximum request or response control head;
- 8 KiB inline-body allowance; and
- 256 KiB maximum data frame.

These are resident-resource bounds, not total payload limits. Per-stream and
per-carrier mux receive credit (below) and bounded ownership rings apply
backpressure across direct and relayed calls. The network layer does not spool
application bodies to temporary files.

Record capacities are derived from the inside out. A 256 KiB body frame plus
its nine-byte object prefix fits one mux record; that record plus its 17-byte
mux prefix fits one routed-session record. Adding an outer header does not
split an otherwise maximum-sized inner record.

The canonical path shares one pooled allocation:

```text
[session headroom][mux headroom][RPC headroom][payload][optional AEAD tailroom]
```

The producer fills the payload region. Object RPC and the mux move the
allocation's start backward to prepend their headers. The default routed-session
writer prepends a plaintext record header and retains that slab across partial
socket writes. In explicitly selected AEAD mode it encrypts in place and
appends the detached tag. Receive performs the corresponding operation in the
same slab and exposes only initialized payload ranges.

Small typed request and response values are encoded into one
transport-shaped SOBJ allocation and adopted without normalization. Large
owned values retain the semantic Rust value plus a compact encoding plan, then
fill one credit-bounded transport slab at a time. Shared fan-out values retain
one `Arc` per call instead of cloning the semantic payload. Asynchronous
readers fill an allocated slab directly. Synchronous readers use one bounded
blocking prefetch worker per body source; the worker fills connection-owned
slabs and hands them to the persistent transmitter without a per-chunk
request/oneshot bridge. Generated repeated-byte bodies also fill complete
owned slabs directly; only callers that choose the byte compatibility view
receive immutable `Bytes` slices.

For a known-length body, the receiver completes at the declared byte boundary.
A small typed call is therefore a request head plus one owned data record, and
likewise for its response; it does not add a redundant object-level finish
record. Unknown-length producers send a finish record carrying the observed
length. Object RPC does not compute or transmit a content digest. Inline byte
bodies remain the explicit byte-compatibility form. When a sender knows a record
is its last, the mux carries stream FINISH, and for a stream not yet opened its
OPEN, in that same record rather than in separate control records.

Data and control slabs use separate pools, per-connection and process byte
limits, and bounded retained free lists. The default limits are 32 MiB of data
and 2 MiB of control slabs per connection, and 256 MiB of data and 32 MiB of
control slabs per process. The owned mux has one persistent RX task and one TX
task, stream slots backed by chunk rings, and deficit round-robin transmission
across active streams. Receive credit is attached to the delivered chunk lease
and returns only when the downstream consumer drops or transfers the chunk.
Aggregated window returns, RESET, ready stream FINISH, and front-of-stream
object-RPC control records are prioritized over body data without reordering
records inside a stream.

Current mux credit permits 64 MiB per stream and 512 MiB per carrier.
Receivers aggregate ordinary stream and connection credit returns to 8 MiB and
32 MiB respectively, but flush sub-threshold credit immediately when a sender
is blocked.

A caller that supplies shared immutable bytes may pay one normalization copy.
A caller that chooses `AsyncRead`, `AsyncWrite`, or contiguous service-value
materialization pays the corresponding compatibility copy explicitly. The
process-wide `network_chunk_metrics` snapshot and cluster network benchmarks
report pooled-slab allocations, reuse, instrumented payload-copy bytes, and
peak pooled memory. Payload-copy totals are broken down into shared-source
normalization, byte-stream writes, byte-stream reads, and contiguous
materialization so a payload-sized regression cannot be mistaken for a small
control-value compatibility boundary. The benchmark also reports pooled
allocations per logical GiB, the test process's lifetime peak RSS (`ru_maxrss`
where supported), and logical throughput grouped by the direct or relayed
route selected from the stabilized connectivity graph. Its copy and allocation
totals combine benchmark-client counters with snapshots taken inside the source
daemon, so the reported zero-copy result covers the routed carrier rather than
only the loopback client adapter. The per-process and per-reason values remain
available separately. Pool allocation counters deliberately describe the
network allocator rather than every process heap allocation; the source
snapshots also include the bounded diagnostic exchanges themselves, and peak
RSS is a process high-water mark rather than a benchmark-window delta.

Public helpers that accept an already encoded `ObjectRpcRequest` remain
explicit compatibility entrypoints. They cannot recover the original typed
value and may therefore pay one normalization copy. Production callers that
still own a typed value use `ObjectRpcCall::encode_value`,
`encode_borrowed_value`, or `encode_shared_value`; response consumers use
`ObjectRpcResult::decode_body` so large values are decoded from chunks instead
of first being gathered into a second full-size vector.

Progress frames distinguish transport health from application silence.
Dropping an incoming body cancels or drains the same call; it does not switch
to another protocol. Request and response completion use ordered framing and
declared or observed lengths. Control payloads are canonical SOBJ bytes without
an RPC authentication tag, signature, or checksum.

## Request Sender Scheduling

The canonical request sender first polls in its caller with the caller's waker;
a ready result needs no sender task. A suspended sender moves its existing
future, source, and writer into one abort-on-drop task owner, and early
streaming responses keep their detachment rules. Immediate errors use the same
delivery classifier as inline and suspended sends. Each body-frame production
charges Tokio cooperative budget before obtaining the next chunk. That bounds
ready production by the runtime's budget, not by wall time, and leaves record
framing, FINISH, byte credit, progress, and payload-copy semantics unchanged.

Network timeline request-task queue and poll points describe only the suspended
remainder; request HEAD/DATA staging may precede them, and naturally ready
senders have no task points. Transport readiness-to-consumption intervals can
include application waits and do not prove executor or one-way network latency.

## Retry Boundary

Object RPC reports whether an attempt was definitely not sent, rejected,
completed, or possibly applied. Route selection may try another path only before
returning a stream. A domain may retry a possibly applied operation only when it
owns a stable operation identity and an idempotency or outcome contract.

## Probe Process and Timing Contract

The maintained `cluster.network.ping-benchmark` and
`cluster.network.semantic-attachment-benchmark` probes select `MainDaemon`
destination affinity through the fail-closed `/daemon/object-rpc/forward-main`
route, so a shadow sharing the node identity cannot satisfy them. Their
diagnostics record `destination_process_affinity=main-daemon`. Ordinary API
callers keep `Any` affinity unless they explicitly select a process contract.

Latency gates use the source daemon's measured inner forwarding interval, which
covers route preparation, target RPC execution, and response forwarding. The
full test-client proxy round trip is reported separately. Neither interval
subtracts tracing or profiling overhead, and active diagnostics must capture the
processes selected by the workload's affinity.

Docker gates run the `debug+cpu-profiler` build with debugging and tracing
enabled at unchanged thresholds, and CPU profiling when requested, as required by the
[performance analysis skill](../../skills/sinomo-performance-analysis/SKILL.md).
The workspace dev profile builds the network, DFS, daemon, SOBJ codec, Observe,
`tracing`, `tracing-core`, and `tracing-subscriber` packages at optimization
level 2. Record the binary hash, source patch, and Cargo profile configuration
with performance artifacts; a build without those overrides is a different build
identity.

## Peer Reachability and Connectivity Verification

`peer_reachability.rs` classifies every authorized peer with
`routing.rs peer_path_classifications`. Its inputs are the directory routing
snapshot, the committed edges of every source and this process's local-source
overlay, which is the capture `call_target` plans against.
`MultiHopRoutedConnector::availability` uses the same classifier.

| Verdict | Condition |
|---|---|
| `Path` (`Candidate`) | A direct admissible edge that is not unreachable, a direct endpoint a registered local module dials and this node has no observation for, or a relay-forwarding path within the planner's hop limit. Healthy, below-threshold degraded and unverified edges count; module endpoints are never relay hops. |
| `NoPath` | No such path, and at least one unreachable edge into the peer from this node or from a relay it can reach. |
| `Unknown` | Neither a candidate nor failure evidence about an edge into the peer, or snapshots from different directory generations. |

The planner's unobserved-module bucket and the classifier share
`unobserved_module_endpoint_role`: an endpoint qualifies when it is not
local-only, not a built-in probe target, has no observation from this source,
and a registered module reports a known role for it.

`subscribe_peer_reachability` registers for route-input hints before its first
read. Hints come from the local store's routing-generation bump, committed
directory invalidation (`invalidate_cluster_route_graph_cache`), the
network-module registry and the verification registry. On a hint, the
subscriber compares the directory routing version and the local, module and
verification generations with its memoized inputs. It recomputes, once per
process for all subscribers, only when one of them moved or the earliest edge
expiry the memo used has passed. Expiry alone schedules nothing; the next hint
or point read reflects it. `changed()` returns
the peers whose entry (verdict, `verifying`, `relayed_only`,
`examined_generation`, `concluded_generation`) differs from that
subscriber's previous snapshot, so a lagged hint loses nothing. Without a
subscriber nothing is computed, and `peer_reachability_snapshot` computes on
demand without retaining a view.

The Main daemon publishes `host.network.peer.reachability.changed` (schema
`sinomo.component-host.peer-reachability` 1.0.0) once per changed peer, as a
transient, local, private component-host event. The payload carries the
observer, peer, previous and current entries (`verdict`, `verifying`,
`relayed_only`, `examined_generation`, `concluded_generation`), the snapshot
generation, the moved inputs as causes, and the input generations as evidence.
Consumers read state from the subscription; the event bus may drop events on
lag.

Verification requests come from the terminal outcome of `call_target` once a
dial has started, endpoint calls, a prepared connector's first-hop realization
failures and the relay's outbound-edge sink. Route preparation failures before
a dial, such as an unreadable session identity or connectivity cache, request
nothing:

| Cause | Source |
|---|---|
| `DeliveryFailed` | `Timeout{Connect}`, `RetryExhausted`, `DeliveryFailed` (refused, reset, disconnect, idle no-progress), `Io` |
| `DeliveryDeadline` (weak) | `Timeout{Overall}` with `DefinitelyNotSent` |
| `DeliverySucceeded` | A delivery succeeded while a subscribed verdict for the peer is `NoPath` |
| `Requested` | `request_peer_verification` from a service |

A reached-but-slow `Timeout{Overall}` and authorization or application errors
request nothing. Probe calls opt out with
`ClusterObjectRpcOptions::without_peer_verification`.

The registry (`connectivity_verification.rs`) accepts requests only while a
probe owner is registered for the root; each daemon process registers its own.
Requests coalesce per peer, keeping the strongest cause, and at most 256 stay
pending per root. The owner's wake sets the `PeerVerificationRequested` dirty
bit. `ClusterPeerVerificationCadence` in `sinomo-cluster` then starts one
episode per peer. A weak request is ignored for 30 seconds after an episode
reached the peer.

Each round probes every local endpoint of the peer, including endpoints a
registered network module dials. Rounds rank ahead of regular targets under the
same per-tick caps. A round that needs no regular probe runs as a
verification-only batch, which leaves `last_probe`, convergence, forced-probe
state and due-class claims untouched. Spacing after a round is 1 second, then
2 seconds (2 and 4 seconds on mobile and sensor roles), and machine-level probe
backoff holds rounds.

An episode settles when:

- an endpoint answers with no failures;
- every planned endpoint is at the failure threshold, which takes at most three
  failing rounds under the single-failure grace;
- three rounds produced no observation; or
- six rounds have run.

A failure-driven first round for a peer whose planned endpoints are all already
unreachable settles without probing.

Every settlement gives the peer a new `examined_generation`. Only an episode
whose own probe concluded, with an endpoint that answered or with every planned
endpoint at the failure threshold, also moves `concluded_generation` to that
value. The other settlements leave it unchanged: the no-observation and
six-round bounds, the failure-driven round that did not probe, a peer with
nothing this node dials, and the cancellation of every episode when the node
has no cluster, is alone in it, is not yet a registered member or its probe's
identity was superseded. A service that needs a standing no-path verdict
examined compares `concluded_generation`, and asks again when an episode
settled without moving it; the registry starts no episode on its own.

Every connectivity probe, periodic or verification, is a status RPC under the
connect and no-progress limits of `NetworkHealthPolicy`, with no whole-call
deadline. A batch has its own budget: the pending-step bound less 250 ms. Its
probe concurrency covers the largest per-tick cap, so a batch is one wave and an
endpoint that runs to the connect limit still records its failure within the
budget. An endpoint still answering when that budget ends records nothing
(inconclusive) instead of a failure.

Every module endpoint of an authorized member is an active probe key, so
verification evidence for it survives later regular batches, but only built-in
endpoints are ever regularly due and only those records schedule renewal. The
regular seed never copies module evidence back into a batch. Main schedules one
wake at the earliest expiry of records no regular probe renews;
`remove_expired_local_cluster_connectivity_source_records` then removes those
expired in both the persisted and in-memory views, once, and the source's
publication withdraws the committed copy. A record renewed in memory by a later
command outcome is kept.

## Implementation Boundaries

The compile-adjacent checks in `layer_boundary_tests.rs` enforce the dependency
direction:

- direct links do not import connectivity, routing, or RPC;
- connectivity snapshots do not import routing or RPC;
- routing does not import SOBJ, service methods, or the object-RPC codec; and
- object RPC does not import connectivity or route planning.

When the data path changes, update this reference with the owning code and its
conformance tests. Performance claims must follow the developer
[experimental reproducibility guide](experimental-reproducibility.md).
