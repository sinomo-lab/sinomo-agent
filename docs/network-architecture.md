# Sinomo Network

`sinomo-network` is the cluster communication boundary. Crates above it should model requests, responses, events, and stream metadata as normal serde-compatible Rust types. The network crate is responsible for turning those types into the cluster wire representation, choosing the peer transport, and eventually routing through intermediate nodes when the target is not directly reachable.

## Wire Format

Cluster frames use Sinomo Object documents on the wire. `sinomo-network` owns the `sinomo-object` dependency and exposes typed helpers such as `ObjectRpcRequest`, `ObjectRpcResponse`, object frame I/O, and `SinomoObjectCodec`. Callers should not build `sinomo-object` documents directly unless they are implementing a new low-level transport inside `sinomo-network`.

This keeps higher-level crates protocol agnostic. Typed requests should normally be sent as object RPC routes; the LAN frame codec, discovery advertisements, and typed protocol payloads are encoded as `SOBJ` documents by the network layer.

The legacy line-oriented daemon command path is deprecated. It remains only as a migration shim while daemon and CLI control calls move to object RPC routes. New internal communication must not introduce plain text frames. Requests and responses are binary Sinomo Object payloads unless the peer is an external system that requires a textual protocol.

Daemon control requests can carry an authenticated object RPC request alongside the signed route authority. Responses are direct `ObjectRpcResponse` frames, not an outer text response envelope. `sinomo daemon status`, daemon health probes, and `sinomo daemon stop` use the `daemon` category routes `/daemon/status` and `/daemon/shutdown`. The older string command names are still accepted for the remaining cluster, rollout, and compatibility handlers while those surfaces are migrated route by route.

## Interface

`sinomo-network` exposes one canonical cluster request/response shape:

- Object RPC routes, via `ClusterObjectRpcClient`, `ObjectRpcRouter`, `ObjectRpcRequest`, `ObjectRpcResponse`, `ObjectRouteDescriptor`, and `ServiceCatalog`. A route is an object in and an object out, intentionally REST-like in naming and documentation but without HTTP concepts. The router implements `tower_service::Service`, and `ServiceCategoryLayer` lets a node expose groups of related services for agent-readable discovery, measurement, and debugging. `object_rpc_category!` can declare a category and route descriptors with input/output Rust types and observability keys so service catalogs stay machine-readable. Ordinary callers give the client a target node, request, and deadline; `sinomo-network` owns signing handoff, endpoint choice, direct-vs-relay choice, retry, and packet delivery. Concrete endpoint calls are bootstrap, direct probe, or transport-internal escape hatches, not protocol-specific routing APIs.

The old generic request/response frame path is gone. Event propagation and heartbeats still use the internal mesh transport, but DFS, KV, daemon control, membership, rollout, and other command-style protocols must use typed object RPC routes rather than a second request/response abstraction.

DFS peer adapters follow the same rule. `DfsPeerTransport` implementations provide one object-RPC send primitive, while DFS-owned helpers build and decode route-specific requests for typed object messages, direct reads, metadata batches, and snapshots. `/dfs/protocol/frame` is no longer a registered route.

Stream helpers are an extension of object RPC for realtime or large data flows where a route opens a typed byte stream, sends chunks, then ends or resets the stream. They are not a separate command path.

Object RPC carries serde-compatible protocol data at the boundary. Metadata and protocol payloads are object-encoded inside `sinomo-network`; streaming data chunks remain bytes so transports can pass through file, media, or realtime buffers without reserializing the content.

Tokio-native object frame helpers (`read_object_frame_async` and `write_object_frame_async`) are available for async transports. The daemon control client also has an async path so new call sites can avoid blocking TCP I/O while still using the same binary object request/response payloads. Object RPC has matching frame helpers for async request/response streams: `call_object_rpc_frame_async` writes one binary request and reads one binary response, while `serve_object_rpc_frame_async` reads one request, dispatches it through an `ObjectRpcRouter`, and writes the binary response.

## Canonical Routing Algorithm

All cluster traffic uses one routing algorithm. Protocols such as KV, DFS, daemon control, membership, rollout, and agent tools define payload routes and target nodes; they do not define their own transport-selection or relay strategies. A direct send is a one-hop path. A relay send is a multi-hop path. LAN, loopback, Iroh, WebRTC, and relays are endpoint edge types inside the same route graph.

The caller-facing shape is:

```rust
send(target_node_id, payload, deadline)
```

`sinomo-network` wraps that into a packet:

```rust
Packet {
    delivery_key,
    source_node_id,
    target_node_id,
    ttl,
    visited_nodes,
    payload,
}
```

The routing loop is:

```rust
fn send(target: NodeId, payload: Bytes, deadline: Deadline) -> Result<Response, DeliveryError> {
    let packet = Packet {
        delivery_key: signed_request_identity(payload),
        source_node_id: local_node_id(),
        target_node_id: target,
        ttl: DEFAULT_TTL,
        visited_nodes: Vec::new(),
        payload,
    };

    route(packet, deadline)
}

fn route(mut packet: Packet, deadline: Deadline) -> Result<Response, DeliveryError> {
    if packet.target_node_id == local_node_id() {
        return deliver_local(packet);
    }

    if packet.ttl == 0 {
        return Err(DeliveryError::TtlExpired);
    }

    if packet.visited_nodes.contains(local_node_id()) {
        return Err(DeliveryError::LoopDetected);
    }

    packet.visited_nodes.push(local_node_id());
    packet.ttl -= 1;

    let mut failed_edges = Set::new();

    while deadline.has_time_left() {
        let graph = build_route_graph(&failed_edges);

        if let Some(path) = shortest_path(&graph, local_node_id(), packet.target_node_id) {
            let next_hop = path[1];
            match send_to_next_hop(next_hop, packet.clone(), deadline.remaining()) {
                Ok(response) => return Ok(response),
                Err(error) => {
                    failed_edges.insert(edge(local_node_id(), next_hop));
                    record_connectivity_failure(next_hop, error);
                    continue;
                }
            }
        }

        if let Some(next_hop) = exploratory_neighbor(&graph, &packet.visited_nodes) {
            match send_to_next_hop(next_hop, packet.clone(), deadline.remaining()) {
                Ok(response) => return Ok(response),
                Err(error) => {
                    failed_edges.insert(edge(local_node_id(), next_hop));
                    record_connectivity_failure(next_hop, error);
                    continue;
                }
            }
        }

        return Err(DeliveryError::NoRoute);
    }

    Err(DeliveryError::DeadlineExpired)
}
```

Every node that receives a packet runs the same algorithm:

```rust
fn receive(packet: Packet, deadline: Deadline) -> Result<Response, DeliveryError> {
    if packet.target_node_id == local_node_id() {
        return deliver_local(packet);
    }

    route(packet, deadline)
}
```

The route graph is built from all known authorized endpoints and connectivity records:

```rust
fn build_route_graph(failed_edges: &Set<Edge>) -> Graph {
    let mut graph = Graph::new();

    for member in authorized_cluster_members() {
        for endpoint in member.endpoints {
            let edge = Edge {
                from: local_node_id(),
                to: member.node_id,
                endpoint,
                cost: endpoint_cost(endpoint),
            };

            if edge.is_usable() && !failed_edges.contains(edge) {
                graph.add(edge);
            }
        }
    }

    for record in connectivity_records_from_cluster() {
        let edge = Edge {
            from: record.source_node_id,
            to: record.target_node_id,
            endpoint: record.endpoint,
            cost: connectivity_cost(record),
        };

        if edge.is_usable() && !failed_edges.contains(edge) {
            graph.add(edge);
        }
    }

    graph
}
```

The edge cost function is deterministic and shared by all traffic:

```rust
fn endpoint_cost(endpoint: Endpoint) -> u32 {
    let transport_cost = match endpoint_path_kind(endpoint) {
        InProcess => 0,
        Loopback => 1,
        LanQuic => 10,
        PrivateLanTcp => 20,
        PublicTcp => 40,
        Iroh => 60,
        WebRtc => 70,
        TurnRelay | HttpsRelay => 100,
    };

    let connectivity_cost = match endpoint.connectivity {
        Healthy(latency_ms) => latency_ms.min(10_000),
        Unknown => 20_000,
        Degraded(failures) => 50_000 + failures * 1_000,
        Unreachable => INF,
    };

    transport_cost + connectivity_cost
}
```

Packet delivery to a next hop tries that hop's endpoints in cost order and feeds the result back into local connectivity:

```rust
fn send_to_next_hop(
    next_hop: NodeId,
    packet: Packet,
    remaining: Duration,
) -> Result<Response, TransportError> {
    for endpoint in endpoints_for(next_hop).sort_by(endpoint_cost) {
        match send_packet(endpoint, packet.clone(), remaining.per_attempt()) {
            Ok(response) => {
                record_connectivity_success(endpoint);
                return Ok(response);
            }
            Err(error) => {
                record_connectivity_failure(endpoint, error);
            }
        }
    }

    Err(TransportError::AllEndpointsFailed)
}
```

Retries and alternate paths can race or repeat delivery. The origin therefore keeps the signed inner request stable across attempts, and the target's normal request-auth/replay layer must suppress duplicate dispatch for the same delivery key:

```rust
fn deliver_local(packet: Packet) -> Result<Response, DeliveryError> {
    if let Some(response) = replay_cache.get(packet.delivery_key) {
        return Ok(response);
    }

    let response = authenticated_local_dispatch(packet.payload)?;
    replay_cache.insert(packet.delivery_key, response.clone());
    Ok(response)
}
```

This algorithm intentionally removes protocol-specific route selection. Higher layers define the target and payload. `sinomo-network` decides how to deliver the packet, learns from success and failure, and retries through the same graph until the packet is delivered or the deadline, TTL, or cluster connectivity makes delivery impossible.

## Ownership

Cluster communication should flow through `sinomo-network` abstractions. Durable cluster storage may still use its own stable storage representation, but live peer discovery, peer routing, object RPC routes, and stream traffic should not reach around the network crate to open ad hoc sockets or choose a wire format in higher-level crates.

Cluster daemon endpoint selection also belongs to `sinomo-network`. CLI and daemon code may sign or authorize commands, but choosing loopback, LAN TCP, Iroh, WebRTC, relay, direct delivery, or multi-hop relay should happen through the canonical routing algorithm rather than inside higher-level command handlers. Older purpose-specific selectors remain only as crate-local compatibility shims while call sites migrate toward target-node packet delivery.
