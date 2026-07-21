# Network Data-Path Reference

This developer reference records the current private carrier mechanics behind
Sinomo object RPC. The stable service, routing, retry, and authority contracts
remain in the [network architecture](../network-architecture.md); the public
binary object encoding is specified by the
[Sinomo Object format](../sinomo-object-format.md).

The constants and carrier versions below describe the current implementation,
not a second application API. They may change with the corresponding protocol
negotiation, compatibility paths, tests, and architecture invariants.

## Semantic Boundary

Applications see a SOBJ request head and SOBJ response head, with an optional
streaming attachment in either direction. Ordinary RPC values remain one SOBJ
value even when the network temporarily carries a large encoded body out of
line and restores it before dispatch. Attachments stay attached to the same
call. The private distinction between control and Data frames does not expose
frame boundaries or require callers to choose a transport shape.

Object-frame heads use one `SOF2` prefix with an unsigned 64-bit length for
every frame. There is no compact form, extended-length sentinel, or legacy
prefix negotiation.

## Canonical Bulk Session

Transfer chunk size and receive credit are independently negotiated. A
canonical bulk session negotiates 1--4 MiB chunks and starts with 16--64 MiB of
receive credit. Starting credit is twice the measured bandwidth-delay product
(BDP), clamped to that range; a path without enough observations starts at
16 MiB.

`sinomo-network` exposes a transport-neutral path estimator for reuse by other
crates. Callers supply RTT samples and delivered-byte intervals keyed by a
stable path identity, then consume smoothed RTT, delivery bandwidth, BDP, or a
resource-bounded recommended window. Object RPC supplies handshake RTT and
cumulative transfer delivery through the same interface. Its process-shared
registry is bounded; object RPC currently keys observations and receive-credit
accounting by the authenticated immediate peer.

Receivers return cumulative credit after 8--16 MiB of progress, or when the
1--10 ms path-derived progress timer becomes due, rather than acknowledging
every chunk. Completion flushes the final progress. Active receive credit is
bounded to 64 MiB per transfer and 256 MiB in aggregate per immediate peer.
Credit is an admission bound rather than a preallocated buffer. The bounded
reader queue and spill path separately control resident body memory. Unknown-
length attachments and compatibility fallbacks enter this BDP-tuned session;
known-size attachments use the persistent channel described below.

## Persistent Multiplexed Channel

The persistent channel keeps request and response heads, body descriptors,
completion, cancellation, and flow-control messages as ordinary SOBJ control
documents. Only attachment and out-of-line value payload bytes use the private
fixed Data representation.

`SCDF` version 1 places a 32-byte header in front of the existing payload
allocation. The header contains its magic and version, request/response
direction, flags, call ID, ordered body offset, payload length, and reserved
bytes. A carrier validates the complete header, including direction, nonzero
call ID, offset range, reserved fields, and the 256 KiB payload bound before
acquiring a receive buffer. Sending uses one partial-write-safe vectored loop
over the header and payload.

### Flow Control and Fairness

The channel frame quantum and per-call flow window are independent. Protocol
version 4 uses a 256 KiB maximum Data frame and a fixed 8 MiB per-call window.
Eight concurrent body-bearing calls therefore expose 64 MiB of aggregate
flight, but one call does not yet derive its window from the shared BDP
estimate. Adaptive per-call credit needs a later protocol version in which the
receiver grants both request and response credit; relaxing the version 4
checks would let a response sender exceed receiver-owned memory policy.

A receiver advertises the cumulative received offset after at least 1 MiB of
additional progress or after 2 ms with unreported progress; it does not send
one credit message per frame.

Queue admission can reserve a completed batch of up to four frames and 1 MiB
with one exact byte-and-frame reservation. The egress scheduler still writes at
most one frame from a call lane before rotating to another ready lane.

### Liveness

Long-running handlers use a private call-scoped liveness exchange on the same
control lane. A probe is sent halfway through an otherwise idle health window.
The local write is not progress; only the peer's matching response refreshes
that call's transport-health deadline. Independent multiplexed calls therefore
remain isolated, absolute operation deadlines are preserved, and a blackholed
carrier is still detected within the configured idle interval.

### Memory Ownership and Relay Forwarding

Each physical carrier owns a bounded pool of reusable 256 KiB `BytesMut`
receive slabs. With 64 admitted calls, the active bound is 32.25 MiB: two
retained slabs per call plus one slab for the physical reader. Released slabs
have a separate 16 MiB idle-cache bound. An ownership-bearing received `Bytes`
keeps both its slab and the pool permit alive through clones and zero-copy
slices, so the slab cannot be reused while a downstream consumer still
references it. File, reader, and other non-contiguous producers read through
bounded reusable buffers.
Contiguous producers retain their data as `Bytes` and emit slices without
allocating a `Vec` for each frame.

Applications consume incoming attachments through the public asynchronous
reader adapter. A crate-private relay adapter instead moves received `Bytes`
directly into the next-hop producer, splitting only with zero-copy slices when
carrier frame sizes differ. Transport chunk boundaries remain private while
relays avoid an incoming-read/outgoing-write copy cycle.

### Slow or Dropped Consumers

Ordered body frames first move through a bounded in-memory reader channel. If
the application falls behind, the session lazily spills only the remaining
suffix to a temporary file. A reader that deliberately drops a request body
switches the session to bounded discard so transport acknowledgements and an
early response can still complete.

Normal completion and cancellation remove spill files. Crash-orphan collection
is not transfer resume state and must use process-ownership proof rather than
deleting files by age.

## Compatibility

Local persistent channels negotiate protocol version 4 for the mixed
SOBJ-control/SCDF-Data dialect. Cluster TCP tunnel version 4 uses the same mixed
representation. A tunnel peer that rejects version 4 is retried with version 3
and then version 1. Both older versions retain the legacy authenticated
one-session-per-stream path rather than interpreting cumulative channel credit
with an older Data dialect.

An incompatible local channel similarly falls back to the canonical session
path. These compatibility choices are carrier details and do not change the
object-RPC call interface.

## Maintaining This Reference

When the carrier changes, update this page together with the owning source,
protocol compatibility tests, the stable invariants in the
[network architecture](../network-architecture.md), and any benchmark or
experiment manifest that depends on the old constants. Performance claims must
also follow the developer
[experimental reproducibility guide](experimental-reproducibility.md).
