# Network Data-Path Reference

This page maps the current stream-native network implementation. The normative
contracts are in the [network architecture](../network-architecture.md); the
public binary object encoding is specified by the
[Sinomo Object format](../sinomo-object-format.md).

All layers remain inside `crates/sinomo-network`. There is one production path:

```text
sinomo-service registry
        |
typed request/response head plus optional body
        |
object_rpc_stream.rs -- one call per authenticated duplex stream
        |
routing.rs -- direct or multi-hop routed stream
        |
direct_link.rs and transport modules
```

No payload-size branch selects another carrier. Relayed and direct calls use the
same object-RPC protocol, and relays forward opaque routed-stream bytes.

## Call and Body Contracts

`rpc_body.rs` owns the application-facing values:

- `ObjectRpcCall` and `ObjectRpcResult` pair a typed SOBJ head with an optional
  body;
- `ObjectRpcIncomingRequest` and `ObjectRpcOutgoingResponse` are the server-side
  equivalents; and
- `ObjectRpcBodySource` and `ObjectRpcIncomingBody` provide bounded streaming
  producers and consumers.

An ordinary value remains a single SOBJ value regardless of encoded size. If
it exceeds the inline allowance, the codec moves its bytes through the body
stream and restores them before typed dispatch. A semantic attachment stays an
attachment. Callers do not observe transport frames or choose a carrier.

`service_rpc_stream.rs` encodes the typed service head into the application-head
slot of the object-RPC protocol. It does not implement routing or keep a service
catalog. The single method registry, route policy, concurrency admission,
readiness, and final handler dispatch live in `sinomo-service`.

## Routed Streams

`routing.rs` is the only production node-routing executor. A
`MultiHopRoutedConnector` plans against one immutable connectivity snapshot and
realizes either a direct stream or a relay chain. The routed-session handshake
binds source, destination, session identity, and channel binding before
application bytes are accepted.

Intermediate nodes run `accept_or_relay_routed_session`. They authenticate the
adjacent links, validate the routed envelope, and copy opaque bytes between the
two bounded streams. They do not decode SOBJ heads, inspect service methods, or
select an RPC implementation.

`route.rs` composes routing with object RPC through `ClusterObjectRpcClient`.
It selects endpoints only while opening the routed stream. Once application
bytes have entered a stream, routing does not replay them on another path.

## Object-RPC Framing and Backpressure

`object_rpc_stream.rs` implements `sinomo.object-rpc.stream.v2`. One duplex
stream carries exactly one request and one response. The current resource
bounds are:

- 256 KiB maximum request or response head;
- 8 KiB inline-body allowance;
- 64 KiB maximum data frame; and
- 256 KiB receive window.

These are resident-resource bounds, not total payload limits. Receive credit
and bounded channels apply backpressure across direct and relayed calls. The
network layer does not spool application bodies to temporary files.

Authenticated progress frames distinguish transport health from application
silence. Dropping an incoming body cancels or drains the same call; it does not
switch to another protocol. Request and response completion remain bound to the
peer context and verified body transcript. Every authenticated control payload
is encoded as its fixed-size tag followed by the canonical unsigned SOBJ bytes;
the tag is verified over those exact bytes before SOBJ decoding.

## Retry Boundary

Object RPC reports whether an attempt was definitely not sent, completed, or
possibly applied. Route selection may try another path only before returning a
stream. A domain may retry a possibly applied operation only when it owns a
stable operation identity and an idempotency or outcome contract.

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
