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

The application-visible semantic model remains a SOBJ request head and SOBJ
response head, with an optional streaming attachment in either direction.
Ordinary RPC values are still presented as one SOBJ value even when the network
temporarily carries a large encoded body out of line and restores it before
dispatch. Attachments remain explicitly attached to that same call. The private
carrier distinction between control and Data frames does not add a second RPC
API, expose frame boundaries, or require callers to choose a transport shape.

Transfer chunk size and receive credit are independent negotiated values. A
canonical bulk session negotiates 1--4 MiB chunks and starts with 16--64 MiB of
receive credit. The starting credit is twice the measured bandwidth-delay
product (BDP), clamped to that range; a path without enough observations starts
at 16 MiB. These sizes control incremental I/O and resource use; they are not
aggregate message limits and are not visible as protocol choices. The network
does not select a different API or wire protocol when a value crosses a byte
threshold, and it has no payload-size enablement toggle. Object-frame heads use
one `SOF2` prefix with an unsigned 64-bit length for every frame; there is no
compact form, extended-length sentinel, or legacy prefix negotiation.

`sinomo-network` exposes a transport-neutral path estimator for reuse by other
crates. Callers supply RTT samples and delivered-byte intervals, keyed by a
stable path identity, and consume the smoothed RTT, delivery bandwidth, BDP,
or a resource-bounded recommended window. Object RPC samples handshake RTT and
cumulative transfer delivery through this same interface. Its process-shared
registry is bounded; object RPC currently keys observations and receive-credit
accounting by the authenticated immediate peer.

Receivers return cumulative credit after 8--16 MiB of progress, or when the
1--10 ms path-derived progress timer becomes due, rather than acknowledging
every chunk. Completion always flushes the final progress. Active receive
credit is bounded to 64 MiB per transfer and 256 MiB in aggregate per immediate
peer. Credit is an admission bound, not a preallocated buffer: the existing
bounded reader queue and spill path continue to control resident body memory.
The pooled channel remains the small-message latency path; bulk and
unknown-length attachments enter the canonical BDP-tuned session.

Files, readers, generated data, and typed encoded values all use the same
session machinery and are framed incrementally in both directions. Domain
protocols may attach operation identities or domain-specific content
identifiers, but do not define transport chunk sizes, per-chunk RPC routes, or
separate small- and large-message calls.

### Persistent Channel Data Path

The persistent multiplexed channel keeps request and response heads, body
descriptors, completion, cancellation, and flow-control messages as ordinary
SOBJ control documents. Only attachment and out-of-line value payload bytes use
the private fixed Data representation. `SCDF` version 1 places a 32-byte header
in front of the existing payload allocation. The header contains its magic and
version, request/response direction, flags, call ID, ordered body offset,
payload length, and reserved bytes. A carrier validates the complete header,
including the direction, nonzero call ID, offset range, reserved fields, and
256 KiB payload bound, before acquiring a receive buffer. Sending uses one
partial-write-safe vectored loop over the header and payload.

The channel frame quantum and per-call flow window are independent. The
current starting point is a 256 KiB maximum Data frame and an 8 MiB per-call
window. A receiver advertises the cumulative received offset after at least
1 MiB of additional progress or after 2 ms with unreported progress; it does
not send one credit message per frame. Queue admission can reserve a completed
batch of up to four frames and 1 MiB with one exact byte-and-frame reservation,
while the egress scheduler still writes at most one frame from a call lane
before rotating to another ready lane.

Each physical carrier owns an 8 MiB bounded pool of reusable 256 KiB
`BytesMut` receive slabs. A received ownership-bearing `Bytes` keeps both its
slab and the pool permit alive through clones and zero-copy slices, so a slab
cannot be reused while any downstream consumer still references it. File,
reader, and other non-contiguous producers also read through bounded reusable
buffers. Contiguous producers retain their data as `Bytes` and emit slices
without allocating a `Vec` for each frame.

Applications continue to consume incoming attachments through the public
asynchronous-reader adapter. A crate-private relay adapter instead moves the
received `Bytes` directly into the next-hop producer, splitting only with
zero-copy slices when carrier frame sizes differ. Transport chunk boundaries
therefore remain private while relays avoid an incoming-read/outgoing-write
copy cycle.

Local persistent channels negotiate protocol version 4 for the mixed
SOBJ-control/SCDF-Data dialect. Cluster TCP tunnel version 4 uses the same
mixed representation. A tunnel peer that rejects version 4 is retried with
version 3 and then version 1, both of which retain the legacy authenticated
one-session-per-stream path rather than attempting to interpret cumulative
channel credit with an older Data dialect. An incompatible local channel
similarly falls back to the canonical session path. These compatibility
choices are carrier details and do not change the object-RPC call interface.

Ordered body frames first move through a bounded in-memory reader channel. If
the application falls behind, the session lazily spills only the remaining
suffix to a temporary file; a reader that deliberately drops a request body
switches the session to bounded discard so transport acknowledgements and an
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
- an authenticated caller identity and request context;
- the protocol payload;
- a cap for one downstream hop attempt.

Within one relay attempt, forwarding preserves the origin-signed inner head and
session descriptor. The final target authenticates the original caller, while
each transport session authenticates the peer identity used for that route.
Intermediate hops create fresh outer relay sessions and body-transfer
descriptors.

Every response carries an Ed25519 identity proof bound to the logical origin
session. Direct responses verify it against the expected target key. Relays
preserve that proof unchanged for a delivered response; relay-generated routing
feedback is classified before it can be treated as target delivery. The proof
answers who produced the response, not whether the response bytes were altered.

Body framing enforces only protocol shape: transfer identity, ordered offsets,
declared lengths, flow-control windows, and completion byte counts. It does not
hash, sign, checksum, encrypt, or otherwise attest body bytes. Relays may
re-frame trusted bytes for each transport hop without retaining end-to-end
content proofs.

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

Canonical calls use framed progress and the canonical health policy for every
non-empty value, regardless of aggregate size.

Route discovery remains finite through its attempt count, hop limit, and
per-attempt health limits. Relay envelopes carry a maximum per-hop timeout, not
an implicit absolute end-to-end expiry. Progress includes advancing body
offsets, cumulative acknowledgements, or control progress;
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

`sinomo-network` assumes every selected transport medium is fully trusted. It
does not provide confidentiality or defend against transport-layer attacks, and
it does not add payload checksums, content digests, per-frame signatures, finish
attestations, or encryption. Built-in LAN TCP accepts the transport's own error
detection and delivers its payload as received. A transport module may have its
own implementation properties, but object RPC neither requires nor duplicates
them.

Identity authentication remains in scope. Signed daemon requests establish the
caller identity and request context, retain freshness and nonce replay checks,
and response identity proofs establish which target answered the logical
session. These proofs are authentication contracts, not content-integrity or
channel-security guarantees. The target service still applies its own domain
authorization.

Relays see only the authority required to forward a packet. They cannot convert
reachability into membership or impersonate the original requester. Local
loopback control uses the same authentication boundary for sensitive commands.

## Architectural Invariants

1. Domain protocols use the single typed object-RPC session and do not add
   separate chunk-RPC paths or small- and large-message APIs.
2. Frame, chunk, window, and buffer sizes are never aggregate message limits.
   Canonical requests and responses have no payload-size threshold or
   enablement toggle and retain the same interface at every size.
   Chunk size is negotiated independently from receive credit, while measured
   BDP may tune both within their bounded ranges.
3. Network and KV have no production dependency on one another;
   `sinomo-cluster` is their only R0 bridge.
4. Directory membership and connectivity are inputs to routing, not outputs of
   successful delivery.
5. Caller and response-target identities are authenticated, while payload bytes
   are trusted to the selected transport and are not authenticated, checksummed,
   or encrypted by `sinomo-network`.
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
capability. Stable KV request IDs remain tracked in the
[reactive KV/Raft plan](plans/reactive-cluster-kv-raft-improvement-plan.md).

The [clustering architecture](clustering-architecture.md) defines the authority
projected into the directory. Current verification coverage and future proof
work are documented in the [formal verification guide](formal/cluster-verification.md)
and [formal verification roadmap](plans/formal-verification-roadmap.md).
