# Sinomo Network Architecture

This document defines the communication boundary, routing model, and transport
invariants owned by `sinomo-network`. It describes stable behavior rather than
individual Rust APIs or migration state.

## Purpose

`sinomo-network` is a pure networking library. Domain crates define typed
requests, responses, events, streams, and target nodes. The network layer
encodes those values, selects endpoints and transports, routes through relays
when needed, and reports delivery outcomes.

The network layer does not decide whether an operation is allowed, interpret KV
or DFS semantics, or read cluster storage directly.

## Boundary

| Layer | Owns |
|---|---|
| Domain service | Request and response types, route names, target selection, business authorization, and side effects. |
| `sinomo-network` | Object RPC, framing, route registration, endpoint selection, route graphs, retries, relays, stream transport, and connectivity feedback. |
| `sinomo-cluster` | Authoritative member and endpoint projection, plus transport drivers for membership and Raft. |
| Transport module | Dialing, listening, and advertising endpoints for one transport family. |

Cluster communication must not open ad hoc sockets or choose a private wire
format in higher-level command handlers. The inverse also holds: networking
cannot grant membership, mutate cluster state, or apply domain policy.

## Wire and Service Model

Internal request/response traffic uses typed object RPC. Routes accept a SOBJ
request document and return a SOBJ response document. Domain callers work with
serde-compatible Rust values; object encoding remains inside the network
boundary.

Streams extend object RPC for large or real-time payloads. A typed route opens
a byte stream, transfers bounded chunks, and ends or resets it. Stream bytes are
not repeatedly re-encoded as object documents.

Each route belongs to a discoverable service category and declares its input,
output, and observation metadata. This gives the daemon one router and one
catalog for DFS, KV, cluster control, rollout, agent, and other internal
protocols.

Plain-text framing is reserved for external systems that require it. New
Sinomo-to-Sinomo protocols use the binary object boundary.

See [Sinomo Object format](sinomo-object-format.md) for encoding details.

## Canonical Routing

Normal post-membership node-addressed traffic uses the target-node delivery
algorithm. A direct send is a one-edge route; relay delivery is a route with
additional edges. Loopback, LAN, roaming, browser, and relayed transports are
edge types in the same graph rather than protocol-specific delivery systems.

Bootstrap, join, direct probes, and transport-internal diagnostics may start
from an explicit endpoint before a target-node route is available. Those are
narrow lifecycle and diagnostic paths, not alternate domain routing systems.

Routing proceeds in stages:

1. Resolve the target node against the current authorized cluster directory.
2. Reject local loops, exhausted local timeout budgets, exhausted hop limits,
   and repeated nodes.
3. Build a graph from authorized endpoints and current connectivity evidence.
4. Rank usable paths deterministically for the request's purpose.
5. Attempt the preferred next hop within the caller's local retry budget.
6. Record success or failure and invalidate the failed edge for this delivery.
7. Retry through a bounded alternate endpoint or path.
8. Deliver locally only after request authentication. A route that accepts
   automatic retry also applies its own idempotency or replay checks.

For normal node-addressed traffic, higher layers supply a target and timeout.
They do not select LAN versus roaming, direct versus relay, or one transport
module over another.

## Packet Invariants

A routed packet carries:

- source and target node identity;
- a hop limit and visited-node history;
- the authenticated protocol payload;
- a cap for one downstream hop attempt.

Within one relay attempt, forwarding preserves the signed inner route and
payload. The final target belongs to the hop-signed relay envelope rather than
the inner signature, so each hop separately authorizes forwarding. A relay
cannot alter the signed inner route or body without invalidating the original
request signature. A new endpoint retry may use a new authentication envelope
and nonce.

Retries may race, and transport behavior may duplicate delivery. Generic object
RPC does not provide a universal response replay cache. Exact authentication
nonce replay is rejected, while side-effecting routes must carry a stable
operation or request identity and enforce idempotency at their domain boundary.
Some DFS data-plane routes add a specialized response cache, but that stronger
behavior is not a network-wide guarantee.

## Route Graph and Cost

The graph contains only authorized members and usable endpoints. Its inputs are:

- member and endpoint records from the cluster directory;
- the authoritative connectivity graph published through the control plane;
- the current transport-module availability snapshot;
- local observations for the current delivery;
- purpose-specific constraints such as raw-stream support.

Route cost prefers local and direct paths over more expensive or relayed ones,
then incorporates observed health and latency. An unreachable or unauthorized
edge is excluded rather than merely assigned a poor rank.

Purpose-specific selectors may adjust ranking for Raft, rollout, status, or
large data, but must preserve the shared safety rules: member authorization,
target identity, endpoint scope, transport capability, local timeout, hop limit,
and connectivity suppression.

## Cluster Directory

Networking must not read cluster KV. Stable facts arrive through a directory
interface implemented by `sinomo-cluster`:

- authorized members and identity keys;
- advertised endpoints and node health;
- configured network information;
- the authoritative connectivity graph;
- a monotonic version used to invalidate route caches.

The version follows the committed directory projection. A membership removal,
endpoint generation change, identity mismatch, or graph update invalidates
affected cached plans before they can authorize new work.

Local connectivity files are bootstrap caches only. They may help while the
authoritative directory is unavailable or empty, and the local node may use its
own fresh probe evidence during cold join. Observations attributed to other
nodes cannot extend or override a non-empty authoritative graph.

When quorum is available, daemons publish their current source-owned
observations through the cluster control plane so useful evidence becomes part
of the shared directory rather than a second authority layer.

## Retry and Failure Semantics

The origin bounds its endpoint and fallback attempts with a local deadline.
Relay envelopes carry a maximum per-hop timeout, not an absolute end-to-end
expiry, so a downstream hop may continue after the origin has stopped waiting.
The hop limit bounds path length rather than total wall-clock time. Operations
that require strict end-to-end expiry or cancellation must carry and enforce
that contract at the protocol layer.

Failures are classified so route learning remains useful:

| Failure | Routing effect |
|---|---|
| Authentication or identity mismatch | Suppress the endpoint and report an authorization failure. |
| Unsupported transport or stream capability | Exclude the endpoint for this request shape. |
| Connect or response timeout | Record degraded connectivity and try a bounded alternative. |
| Explicit unreachable feedback | Exclude the edge until directory or probe evidence changes. |
| No authorized route | Fail without exploratory delivery to an untrusted node. |
| Local timeout or hop limit exhausted | Stop the current routing attempt and return the terminal delivery result. |

Connectivity feedback changes route choice, not membership. A successful dial
cannot promote a peer, and a failed dial cannot remove one.

## Transport Modules

Transports are modules behind one lifecycle: describe capability, start, stop,
dial object RPC, expose local endpoints, and report whether raw or streamed
responses are supported. Loopback and LAN transport are built in. Additional
transports, including Iroh, are loaded through the module system.

Dispatch is address-scheme and capability aware. An endpoint is not attempted
when no enabled module can dial it or when the request requires a feature the
module does not support. Disabling a module removes its transports from future
selection and stops it safely; loaded native code remains alive while any host
reference may still exist.

The native module ABI is versioned and uses SOBJ for descriptors and endpoint
lists. The host validates the ABI and keeps unsafe library lifetime management
inside the trusted module loader. See
[module lifecycle architecture](module-lifecycle-architecture.md) for loading,
activation, and replacement.

The Iroh transport is an extracted native module. Core networking retains only
transport-neutral endpoint contracts and selection rules; it does not depend on
the Iroh SDK.

## Security

Object RPC routing and request authentication are separate from transport
security. Built-in LAN TCP uses plain framing and relies on signed daemon
requests for identity, integrity, freshness, and nonce replay protection; it
does not provide channel confidentiality. Secure transports may additionally
authenticate and encrypt the channel. The target service still applies its own
domain authorization.

Relays see only the authority required to forward a packet. They cannot convert
reachability into membership or impersonate the original requester. Local
loopback control uses the same authentication boundary for sensitive commands.

## Architectural Invariants

1. Domain protocols use typed object RPC or its stream extension.
2. Network and KV have no production dependency on one another;
   `sinomo-cluster` is their only R0 bridge.
3. Directory membership and connectivity are inputs to routing, not outputs of
   successful delivery.
4. A relay cannot modify the signed inner route or payload of the attempt it
   forwards; target-node forwarding is authorized hop by hop.
5. Origin retries use a local deadline; relay attempts use a per-hop cap and hop
   limit rather than a propagated absolute deadline.
6. Side-effecting routes that permit retry must provide operation-level
   idempotency; networking does not promise universal response replay.
7. Route caches are versioned by every input that affects validity.
8. Disabled or incompatible transports cannot consume attempt budgets.
9. Remote requests are authorized again by the target service.
10. Higher-level crates never bypass the network layer for cluster traffic.

Operation-level idempotency is a protocol requirement, not a universal current
capability. Stable KV request IDs remain tracked in the
[reactive KV/Raft plan](plans/reactive-cluster-kv-raft-improvement-plan.md).

The [clustering architecture](clustering-architecture.md) defines the authority
projected into the directory. Current verification coverage and future proof
work are documented in the [formal verification guide](formal/cluster-verification.md)
and [formal verification roadmap](plans/formal-verification-roadmap.md).
