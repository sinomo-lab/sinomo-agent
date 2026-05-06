# Sinomo Network

`sinomo-network` is the cluster communication boundary. Crates above it should
model requests, responses, events, and stream metadata as normal serde-compatible
Rust types. The network crate is responsible for turning those types into the
cluster wire representation.

## Wire Format

Cluster frames use Sinomo Object documents on the wire. `sinomo-network` owns
the `sinomo-object` dependency and exposes typed helpers such as
`ProtocolMessage::encode`, `ProtocolMessage::decode`, and `SinomoObjectCodec`.
Callers should not build `sinomo-object` documents directly unless they are
implementing a new low-level transport inside `sinomo-network`.

This keeps crates such as `sinomo-dfs` protocol agnostic. DFS sends
`DfsNetworkMessage` values through `ProtocolMessage::encode`; the LAN frame
codec, discovery advertisements, and typed protocol payloads are encoded as
`SOBJ` documents by the network layer.

For line-oriented daemon control sockets that need printable command bodies,
`sinomo-network` exposes object text payload helpers. They
base64url-wrap `SOBJ` bytes so cluster join, Raft replication, and watch fanout
commands carry Sinomo Object payloads across the current daemon transport.

## Interfaces

`sinomo-network` exposes two communication shapes:

- Message frames, via `NetworkTransport`, `NetworkFrame`, `MeshMessage`, and
  `ProtocolMessage`. This is for routing events, acks, heartbeats, RPC-like
  cluster requests, and finite protocol messages.
- Stream frames, via `NetworkStreamTransport`, `NetworkStreamFrame`, and
  `NetworkStreamOpen`. This is for realtime or large data flows where a protocol
  opens a typed stream, sends byte chunks, then ends or resets the stream.

Both interfaces carry serde-compatible protocol data at the boundary. Metadata
and protocol payloads are object-encoded inside `sinomo-network`; streaming data
chunks remain bytes so transports can pass through file, media, or realtime
buffers without reserializing the content.

## Ownership

Cluster communication should flow through `sinomo-network` abstractions. Durable
cluster storage may still use its own stable storage representation, but live
peer discovery, peer routing, protocol messages, and stream traffic should not
reach around the network crate to open ad hoc sockets or choose a wire format in
higher-level crates.

Cluster daemon endpoint selection also belongs to `sinomo-network`. CLI and
daemon code may sign or authorize commands, but choosing loopback, LAN TCP, or
Iroh and opening the transport should happen through network helpers rather than
inside higher-level command handlers.
