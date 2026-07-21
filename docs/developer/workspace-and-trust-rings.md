# Workspace and Trust Rings

Sinomo's Cargo workspace is organized by authority, not only by feature. Every
package declares `[package.metadata.sinomo] ring = ...`, and
`crates/sinomo-core/tests/workspace_ring_boundaries.rs` enforces dependency
direction.

## Dependency Shape

```text
Applications -----------------------> R2 services
     |                                   |
     +----------> sinomo-runtime <-------+
                         |
                  R1 trusted runtime
                         |
                    sinomo-core
                         |
                  R0 foundation
```

| Ring | Current packages | Rule |
|---|---:|---|
| R0 | 10 | Depends only on R0. |
| R0 boundary | 1 | `sinomo-core` aggregates the complete R0 surface. |
| R1 | 29 | Crosses R0 through `sinomo-core`. |
| R1 boundary | 1 | `sinomo-runtime` aggregates R0 and R1 surfaces. |
| R2 | 13 | Crosses R0/R1 through `sinomo-runtime`. |
| Application | 7 | May use R2 directly; uses R0/R1 through `sinomo-runtime`. |

Counts describe the current workspace and can change. The manifest metadata and
boundary test, not this table, are authoritative.

Stable ABI crates have narrow reviewed exceptions for the object codec. Native
modules are leaf plugin artifacts and remain outside default members. Optional
dependencies are not a general escape from ring enforcement; only explicit
test-support or reviewed production exceptions are allowed.

## R0: Minimal Distributed Foundation

R0 owns identities, base contracts, object encoding, local path resolution,
storage registration, events, node state, service discovery, networking,
KV/Raft, and the cluster bridge.

The network/KV split is load-bearing:

```text
sinomo-network       sinomo-kv
       \               /
        \             /
          sinomo-cluster
                 |
            sinomo-core
```

`sinomo-network` remains one R0 crate and internally follows independent
direct-link, connectivity, routing, and object-RPC contracts. The internal
layers cannot depend on one another in reverse. Object RPC consumes an
authenticated duplex stream and does not know node routes. Routing consumes
direct links and connectivity snapshots and does not know SOBJ or service
methods. This internal separation does not change the workspace crate graph.
`sinomo-kv` owns keyspace, transactions, watch, lease, snapshot, request
outcome, and Raft state-machine semantics without network policy.
`sinomo-cluster` is the only production bridge between networking and KV.

R0 changes deserve special scrutiny because every higher layer consumes their
contracts. Keep them small, deterministic, and independent from agents,
providers, GUIs, or host-specific behavior.

## R1: Trusted Runtime

R1 owns daemon hosting, DFS/SIFS, domain persistence services, build and rollout, config,
policy, module lifecycle, registry, observation, evaluation, sandboxing, VFS,
device authority, and support APIs.

R1 can perform durable or host-affecting work. It must enforce authentication,
local consent, capability checks, storage ownership, cancellation, and recovery
at the final boundary rather than trusting a higher-level caller.

R1 code imports R0 through `sinomo_core::...`. Add a core re-export when a
contract is truly part of the complete foundation surface; do not add a direct
lower dependency for convenience.

## R2 and Applications

R2 contains agents, models, memory, language-model integration, perception,
fusion, deliberation, actions, voice, video, audio, gateway behavior, and
evolution. These services may reason about or propose effects but cannot bypass
R1 policy and host authority.

R2 imports foundation and trusted runtime surfaces through
`sinomo_runtime::...`. Applications may depend directly on R2 service crates,
while still using `sinomo-runtime` for R0/R1.

`crates/sinomo` remains a thin binary wrapper. CLI behavior belongs in
`sinomo-cli`; graphical behavior belongs in `sinomo-app`. Moving domain logic
into an application to avoid a dependency rule is an architectural regression.

## State Ownership

Dependency ownership and data ownership reinforce each other:

| State | Owning layer |
|---|---|
| Identity, membership, compact cluster configuration | R0 cluster and KV |
| Local path contracts and storage registry | R0 local/storage foundation |
| DFS namespace, versions, content placement, repair | R1 DFS/VFS services |
| Credentials, host consent, device exposure | R1 node-local authority |
| Build artifacts, rollout desired state, activation | R1 build/module/rollout boundaries |
| Agent sessions, context, memory projections | R2 agent and memory services |
| Presentation and operator intent | Applications |

A higher layer can request a lower-layer transition through its contract. It
must not create a second file, cache, or in-memory map that quietly becomes an
alternate source of authority.

## Adding or Moving a Dependency

Before editing a manifest:

1. Identify both package rings.
2. Ask whether the needed type belongs in a lower shared contract or should be
   adapted at the caller.
3. Prefer the relevant facade for cross-ring access.
4. Keep domain types out of mechanism-only crates such as networking.
5. Add the dependency with `workspace = true` when it is external and already
   shared by the workspace.
6. Run the boundary test:

```sh
cargo test -p sinomo-core --test workspace_ring_boundaries
```

If the architecture genuinely requires a new exception, document the reason in
the boundary test and architecture review. Do not weaken the test until an
existing violation passes silently.
