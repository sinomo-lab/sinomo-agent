# Sinomo Network

`sinomo-network` is the cluster communication boundary. Crates above it should model requests, responses, events, and stream metadata as normal serde-compatible Rust types. The network crate is responsible for turning those types into the cluster wire representation, choosing the peer transport, and eventually routing through intermediate nodes when the target is not directly reachable.

## Wire Format

Cluster frames use Sinomo Object documents on the wire. `sinomo-network` owns the `sinomo-object` dependency and exposes typed helpers such as `ProtocolMessage::encode`, `ProtocolMessage::decode`, and `SinomoObjectCodec`. Callers should not build `sinomo-object` documents directly unless they are implementing a new low-level transport inside `sinomo-network`.

This keeps crates such as `sinomo-dfs` protocol agnostic. DFS sends `DfsNetworkMessage` values through `ProtocolMessage::encode`; the LAN frame codec, discovery advertisements, and typed protocol payloads are encoded as `SOBJ` documents by the network layer.

The legacy line-oriented daemon command path is deprecated. It remains only as a migration shim while daemon and CLI control calls move to object RPC routes. New internal communication must not introduce plain text frames. Requests and responses are binary Sinomo Object payloads unless the peer is an external system that requires a textual protocol.

Daemon control frames can carry an authenticated object RPC request alongside the signed route authority. `sinomo daemon status`, daemon health probes, and `sinomo daemon stop` use the `daemon` category routes `/daemon/status` and `/daemon/shutdown`. The older string command names are still accepted for the remaining cluster, rollout, and compatibility handlers while those surfaces are migrated route by route.

## Interfaces

`sinomo-network` exposes two communication shapes:

- Message frames, via `NetworkTransport`, `NetworkFrame`, `MeshMessage`, and `ProtocolMessage`. This is for routing events, acks, heartbeats, RPC-like cluster requests, and finite protocol messages.
- Object RPC routes, via `ObjectRpcRouter`, `ObjectRpcRequest`, `ObjectRpcResponse`, `ObjectRouteDescriptor`, and `ServiceCatalog`. A route is an object in and an object out, intentionally REST-like in naming and documentation but without HTTP concepts. The router implements `tower_service::Service`, and `ServiceCategoryLayer` lets a node expose groups of related services for agent-readable discovery, measurement, and debugging. `object_rpc_category!` can declare a category and route descriptors with input/output Rust types and observability keys so service catalogs stay machine-readable.
- Stream frames, via `NetworkStreamTransport`, `NetworkStreamFrame`, and `NetworkStreamOpen`. This is for realtime or large data flows where a protocol opens a typed stream, sends byte chunks, then ends or resets the stream.

Both interfaces carry serde-compatible protocol data at the boundary. Metadata and protocol payloads are object-encoded inside `sinomo-network`; streaming data chunks remain bytes so transports can pass through file, media, or realtime buffers without reserializing the content.

Tokio-native object frame helpers (`read_object_frame_async` and `write_object_frame_async`) are available for async transports. The daemon control client also has an async path so new call sites can avoid blocking TCP I/O while still using the same binary object request/response payloads. Object RPC has matching frame helpers for async request/response streams: `call_object_rpc_frame_async` writes one binary request and reads one binary response, while `serve_object_rpc_frame_async` reads one request, dispatches it through an `ObjectRpcRouter`, and writes the binary response.

## Ownership

Cluster communication should flow through `sinomo-network` abstractions. Durable cluster storage may still use its own stable storage representation, but live peer discovery, peer routing, protocol messages, object RPC routes, and stream traffic should not reach around the network crate to open ad hoc sockets or choose a wire format in higher-level crates.

Cluster daemon endpoint selection also belongs to `sinomo-network`. CLI and daemon code may sign or authorize commands, but choosing loopback, LAN TCP, or Iroh and opening the transport should happen through network helpers rather than inside higher-level command handlers.
