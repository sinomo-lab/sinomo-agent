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
| `sinomo-network` | Object RPC sessions, framing, route registration, endpoint selection, route graphs, retries, relays, transfer flow control, and connectivity feedback. |
| `sinomo-cluster` | Authoritative member and endpoint projection, plus transport drivers for membership and Raft. |
| Transport module | Dialing, listening, and advertising endpoints for one transport family. |

Cluster communication must not open ad hoc sockets or choose a private wire
format in higher-level command handlers. The inverse also holds: networking
cannot grant membership, mutate cluster state, or apply domain policy.

## Wire and Service Model

Internal request/response traffic uses one typed object-RPC model. Every call
uses the same request and response interface on every transport, independent of
payload size. The network incrementally carries each encoded value over the
underlying duplex stream; object encoding, framing, flow control, retries, and
transfer state remain inside the network boundary.

Transfer frames, chunks, flow-control windows, and in-memory queues may have
fixed implementation sizes. Those sizes control incremental I/O and resource
use; they are not aggregate message limits and are not visible as protocol
choices. The network does not select a different API or wire protocol when a
value crosses a byte threshold, and it has no payload-size enablement toggle.
Object-frame heads use one `SOF2` prefix with an unsigned 64-bit length for
every frame; there is no compact form, extended-length sentinel, or legacy
prefix negotiation.

Files, readers, generated data, and typed encoded values all use the same
session machinery and are framed incrementally in both directions. Domain
protocols may attach operation identities and expected content digests, but do
not define transport chunk sizes, per-chunk RPC routes, or separate small- and
large-message calls.

Verified body frames first move through a bounded in-memory reader channel. If
the application falls behind, the session lazily spills only the remaining
suffix to a temporary file; a reader that deliberately drops a request body
switches the session to verified discard so transport acknowledgements and an
early response can still complete. Normal completion and cancellation remove
spill files. Crash-orphan collection is not treated as transfer resume state
and must use process-ownership proof rather than deleting files by age.

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
2. Reject local loops, exhausted explicit operation deadlines, exhausted hop
   limits, and repeated nodes.
3. Build a graph from authorized endpoints and current connectivity evidence.
4. Rank usable paths deterministically for the request's purpose.
5. Attempt the preferred next hop within its connect and no-progress health
   limits.
6. Record success or failure and invalidate the failed edge for this delivery.
7. Retry through a bounded alternate endpoint or path.
8. Deliver locally only after request authentication. A route that accepts
   automatic retry also applies its own idempotency or replay checks.

For normal node-addressed traffic, higher layers supply a target and may supply
an operation deadline. They do not select LAN versus roaming, direct versus
relay, transport chunks, retry windows, or one transport module over another.

## Packet Invariants

A routed packet carries:

- source and target node identity;
- a hop limit and visited-node history;
- the authenticated protocol payload;
- a cap for one downstream hop attempt.

Within one relay attempt, forwarding preserves the origin-signed inner head and
session descriptor. Each hop separately authenticates its immediate peer's
frame transcript, while the final target authenticates the original request.
A relay cannot alter the signed inner route, metadata, or body descriptor
without invalidating the original request signature. Intermediate hops create
fresh outer relay sessions and body-transfer descriptors.

Every response head carries an Ed25519 attestation binding its normalized head
digest to the logical origin session. Direct responses must verify against the
expected target key. Relays preserve that target attestation unchanged for a
delivered response; relay-generated routing feedback remains hop-authenticated
and is classified before it can be treated as target delivery. This covers
empty responses as well as responses with an attachment.

Every non-empty body ends with an origin-authenticated Ed25519 finish
attestation binding the original session ID, request or response head digest,
direction, final byte count, and content digest. Relays may re-frame bytes for
each transport hop, but they forward this attestation unchanged. Request bodies
are therefore bound to the authenticated caller, and relayed response bodies
are bound to the expected target key; same-length substitution or a modified
head fails before the receiver acknowledges completion.

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
- purpose-specific constraints such as full-duplex stream support.

Route cost prefers local and direct paths over more expensive or relayed ones,
then incorporates observed health and latency. An unreachable or unauthorized
edge is excluded rather than merely assigned a poor rank.

Purpose-specific selectors may adjust ranking for Raft, rollout, status, or
full-duplex stream requirements, but must preserve the shared safety rules:
member authorization, target identity, endpoint scope, transport capability,
local timeout, hop limit, and connectivity suppression.

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

## Retry, Health, and Failure Semantics

Object RPC has no end-to-end deadline by default. Connection establishment and
each interval without validated network or transfer progress use the shared
five-second health timeout. A progressing call may therefore run for an
unbounded wall-clock duration. Callers that require strict end-to-end expiry
provide an explicit operation deadline.

Canonical calls use hop-authenticated progress frames plus an end-to-end origin
finish attestation and the canonical health policy for every non-empty value,
regardless of aggregate size.

Route discovery remains finite through its attempt count, hop limit, and
per-attempt health limits. Relay envelopes carry a maximum per-hop timeout, not
an implicit absolute end-to-end expiry. Validated progress includes advancing
body offsets, cumulative acknowledgements, or authenticated control progress;
merely waiting longer does not extend an explicit operation deadline.

Replayable byte and file sources can retry on a fresh connection; the current
engine safely restarts those attempts at offset zero. It does not claim durable
partial-transfer resume: implementing that requires persisted receiver state,
handler reattachment, and garbage collection as one atomic design. Generated
one-pass bodies fail explicitly once safe replay is impossible; the network
does not silently collect an unbounded producer into memory. Side-effecting
routes still use stable operation identities so a lost completion reply cannot
commit the domain operation twice.

Failures are classified so route learning remains useful:

| Failure | Routing effect |
|---|---|
| Authentication or identity mismatch | Suppress the endpoint and report an authorization failure. |
| Unsupported transport or stream capability | Exclude the endpoint for this request shape. |
| Connect or no-progress timeout | Record degraded connectivity and try a bounded alternative only when replay semantics allow it. |
| Explicit unreachable feedback | Exclude the edge until directory or probe evidence changes. |
| No authorized route | Fail without exploratory delivery to an untrusted node. |
| Explicit operation deadline or hop limit exhausted | Stop the current routing attempt and return the terminal delivery result. |

Connectivity feedback changes route choice, not membership. A successful dial
cannot promote a peer, and a failed dial cannot remove one.

## Transport Modules

Transports are modules behind one lifecycle: describe capability, start, stop,
open a full-duplex RPC stream, and expose local endpoints. Loopback and LAN
transport are built in. Additional transports, including Iroh, are loaded
through the module system.

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

1. Domain protocols use the single typed object-RPC session and do not add
   separate chunk-RPC paths or small- and large-message APIs.
2. Frame, chunk, window, and buffer sizes are never aggregate message limits.
   Canonical requests and responses have no payload-size threshold or
   enablement toggle and retain the same interface at every size.
3. Network and KV have no production dependency on one another;
   `sinomo-cluster` is their only R0 bridge.
4. Directory membership and connectivity are inputs to routing, not outputs of
   successful delivery.
5. A relay cannot modify the signed inner route or substitute request/response
   body bytes: the original head and final content digest are authenticated end
   to end, while target-node forwarding is authorized hop by hop.
6. Origin calls have no implicit whole-call deadline; explicit operation
   deadlines are distinct from per-connect and per-progress health limits.
7. Side-effecting routes that permit retry must provide operation-level
   idempotency; networking does not promise universal response replay.
8. Route caches are versioned by every input that affects validity.
9. Disabled or incompatible transports cannot consume attempt budgets.
10. Remote requests are authorized again by the target service.
11. Higher-level crates never bypass the network layer for cluster traffic and
    do not implement transport chunking, transfer retry, or resume protocols.

Operation-level idempotency is a protocol requirement, not a universal current
capability. Cluster KV now provides stable request IDs, exact committed-response
replay while the bounded committed-request record is retained, and an
authoritative request-outcome query that reports expiry explicitly after that
retention window. Other side-effecting domain protocols must define their own
operation identity and retry contract rather than relying on networking to
replay responses.

The [clustering architecture](clustering-architecture.md) defines the authority
projected into the directory. Current verification coverage and future proof
work are documented in the [formal verification guide](formal/cluster-verification.md)
and [formal verification roadmap](plans/formal-verification-roadmap.md).
