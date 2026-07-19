# Sinomo System Overview

This page gives users, operators, developers, and contributors a shared mental
model of Sinomo. It describes the system as a whole without requiring knowledge
of the Rust workspace. Developers who need normative boundaries should continue
with the [architecture overview](architecture.md).

## In One Sentence

Sinomo is a local-first runtime that lets one or more trusted devices operate
AI agents, models, memory, tools, files, and updates as one distributed system
without making a hosted service the hidden owner of the system's state.

A laptop running Sinomo is already a complete one-node cluster. Adding a
server, phone, workstation, or sensor extends that same cluster: it does not
move the installation into a separate centralized mode.

## What the System Provides

Sinomo brings several concerns that are often implemented as unrelated
applications into one governed runtime:

- model-backed agents with durable sessions, cancellation, steering, and
  bounded context assembly;
- local and cluster services connected through typed object RPC and events;
- a small strongly ordered cluster control plane for identity, membership,
  configuration, health, and placement facts;
- SIFS, a shared versioned filesystem whose data placement policy can differ
  by object or subtree;
- indexed-file memory whose source evidence remains inspectable and whose
  derived indexes can be rebuilt;
- sandboxes, modules, policy checks, evaluation, and controlled rollout for
  behavior that can affect the host or other nodes;
- structured logs, metrics, traces, operational events, diagnostics, and
  executable formal models.

Sinomo is the runtime and trust boundary around these capabilities. It is not a
model provider, a general-purpose database, or a promise that every file is
replicated to every device.

## The Main Runtime Path

```text
User, device, or service input
          |
          v
  typed event / object request
          |
          v
 reactive policy and placement ------> local or remote service
          |                                      |
          v                                      v
 agent context + model turn              storage / device / tool
          |                                      |
          +-------------- evidence <-------------+
          |
          v
 visible response, action, durable event, or versioned file
```

The important boundary is the final effect. Discovery may reveal that another
node has a service, and a model may propose an action, but neither fact grants
authority. The node that owns a device, secret, file, or host capability checks
the request again before applying the effect.

## Core Concepts

### Installation, node, and cluster

An **installation** is the Sinomo software plus one persistent data directory.
It contributes one normal **node** to one **cluster**. The cluster is a trust
domain with an identity, authorized membership, and a replicated control plane.

The main daemon owns normal cluster participation, services, and storage work.
The shadow daemon is a local supervision and recovery endpoint. It does not
represent a second node and does not vote in cluster consensus.

### Events, services, and object RPC

Services expose typed capabilities. Object RPC carries commands and bounded
value or streaming responses across local and routed network paths. Events are
the cross-service observation and coordination record of what happened, with
provenance, causality, evidence, and routing metadata where applicable. The
owning event store and retention policy determine whether an event is persisted.

Object RPC is the command path; events are the observation and coordination
contract. A request can produce an effect and one or more events, but an event
does not silently become a command.

### Agents, chats, and model sessions

A chat or channel is user-visible collaboration state. A model session is the
internal execution state for turns, tools, context, compaction, cancellation,
and resume. Keeping these separate allows visible history and durable evidence
to survive provider changes or context compaction.

Providers translate a provider-neutral request into an external or local model
protocol. They do not own tool permissions, durable session state, or host
authority.

### Cluster KV, DFS, and local state

Sinomo separates data by authority and scale:

| State | Normal owner | Why |
|---|---|---|
| Membership and compact distributed settings | Raft-backed cluster KV | Small, ordered control state needs quorum semantics. |
| Shared paths and file versions | DFS metadata authorities | Namespace and version history scale independently from control state. |
| Immutable file bytes | Node content stores | Content can be placed, replicated, repaired, or cached by policy. |
| Credentials and host permissions | The local node | Secrets and consent must not become remotely writable cluster settings. |
| Agent and tool evidence | Durable session, event, and evidence stores | Evidence survives bounded model context and remains attributable. |
| Long-term memory | Versioned source and overlay files plus rebuildable indexes | Source files remain authoritative; indexes, summaries, and retrieval traces are derived. |
| Logs, traces, evaluations, and rollout evidence | Owning subsystem | Operational evidence retains its provenance and lifecycle. |

The cluster KV therefore contains references and compact facts, not model
weights, media, source trees, or high-cardinality filesystem metadata.

### SIFS and placement policy

SIFS is the default cluster filesystem. A path can remain local-owned or use a
replicated policy. Shared namespace does not mean unconditional replication:
privacy, durability, availability, and device locality can require different
placement for neighboring subtrees.

Normal quorum-backed operations produce one authoritative visible history.
Replicas may lag and repair later, but they do not independently publish
competing canonical versions.

### Modules, sandboxes, builds, and rollout

A module is a behavior lifecycle unit. Current implementations include built-in
Rust, native libraries, declarative packages, sandboxed processes, and a WASIX
runtime module. A component-model Wasm module backend remains future work.
Producing an artifact does not activate it.

Build services create immutable, target-compatible artifacts and provenance.
Rollout selects targets, stages artifacts, observes activation, and can restore
a previous binary. Each target still checks compatibility and local policy.
Candidate code cannot grant itself authority or replace its recovery boundary.

## One-Node and Multi-Node Operation

The same responsibilities exist in both forms:

| Concern | One node | Multiple nodes |
|---|---|---|
| Identity | One cluster member | Signed identities and authorized membership |
| Control state | Local Raft member | Quorum-backed authority members and non-voting participants |
| Service calls | Usually process-local or loopback | Direct or relayed object RPC selected by the network layer |
| Files | Local content and metadata | Policy-driven placement, replication, and repair |
| Models and devices | Local capabilities | Work placed near available models, accelerators, data, or devices |
| Failure | Process or disk recovery | Also includes partitions, route changes, lag, and quorum loss |

Intermittent devices do not need to be consensus voters. A cluster can keep a
small reliable authority set while mobile or resource-constrained members host
services, storage, models, or devices.

## Authority Model

These distinctions are load-bearing:

1. **Reachability is not trust.** Finding an endpoint never authorizes it.
2. **Membership is not host consent.** A cluster member cannot remotely enable
   remote shell or expose a device on another node.
3. **Model output is not authority.** Tools and actions are admitted by runtime
   policy, not by prompt text.
4. **Build success is not activation.** Rollout and the target node make later,
   separate decisions.
5. **Replication is not ownership.** A replica can serve or repair data without
   becoming the authority that publishes its visible history.

These rules explain why apparently simple operations sometimes require an
explicit local command, a quorum, or a rollout transition.

## What Is Implemented and What Is Still Evolving

The repository contains working CLI and graphical applications, daemon and
cluster lifecycle, KV/Raft control state, routed object RPC, SIFS/DFS, agent and
provider integration, the first local text/Markdown indexed-file memory slice,
modules and sandboxes, rollout, observability, integration harnesses, and
executable formal specifications.

Sinomo remains an actively developed pre-1.0 system. Higher-level agent,
multimodal, memory, placement, self-evolution, and evaluation behavior continues
to evolve, and some protocol hardening is tracked explicitly as future work.
Do not infer production readiness or an implemented guarantee from a proposal.

The documentation uses these evidence classes:

- **User guide:** a supported workflow using the current public interface.
- **Architecture:** a stable ownership boundary or invariant that changes
  require deliberate review.
- **Reference or executable specification:** a precise format, schema, model,
  or test surface.
- **Plan:** active or historical delivery work, not current behavior by itself.
- **Research note:** literature or design evidence, not a Sinomo guarantee.

Start with the [documentation index](README.md) to follow the path for your
audience, and use the [glossary](glossary.md) when a system term is unfamiliar.
