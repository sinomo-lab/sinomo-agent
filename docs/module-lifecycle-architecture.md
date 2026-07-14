# Sinomo Module Lifecycle Architecture

## Scope

This document describes the architecture for Sinomo modules as lifecycle units.
It is intentionally not an implementation log, command reference, migration
plan, or status report.

A module is a bounded piece of behavior or configuration that can be packaged,
identified, evaluated, routed, observed, activated, replaced, and rolled back
independently from the rest of the system when the boundary is stable enough.

The lifecycle model is backend neutral. Static Rust code, native dynamic
libraries, declarative payloads, sandboxed processes, and future Wasm
components can all participate in the same module lifecycle without sharing the
same execution technology.

## Principles

- A module is a lifecycle unit, not an ABI.
- The execution backend is a property of a module version, not the definition of
  the module concept.
- Immutable content and mutable runtime state must stay separate.
- Registry state describes what exists; loader state describes what a node has
  applied; rollout state describes cluster coordination.
- Build state describes artifact production only; a builder cannot create
  loader or rollout authority by publishing output.
- Rollout is a cluster concept. Local activation can exist without a cluster,
  but it is not rollout.
- Once a node is clustered, cluster-owned module desired state is authoritative
  for shared modules. Local state may still hold node-private defaults and
  applied runtime records.
- Runtime assembly must be deterministic. Sinomo must not discover executable
  modules by scanning arbitrary directories.
- Native dynamic libraries are trusted build and deployment isolation, not
  security isolation.
- Wasm is optional. It should be chosen for isolation, portability, or ABI
  reasons, not because all modules must be Wasm.

## Core Model

Conceptually, a module version consists of:

```text
module version =
  module identity
  + immutable manifest or capsule
  + artifact or declarative payload
  + backend kind
  + interface contract
  + compatibility constraints
  + declared capabilities
  + evaluation requirements
  + rollback metadata
  + telemetry identity
```

Mutable lifecycle state is outside the module version:

```text
runtime state =
  active version
  + previous version
  + candidate version
  + shadow version
  + traffic weight
  + last transition
  + applied time
  + node-local loader observations
```

This separation is load-bearing. A registered artifact or declarative payload
must not change in place. Promotion, rollback, unload, and canary routing are
state transitions pointing at immutable versions.

## Identities

Module IDs are stable logical identities. A module ID names the behavior family,
not a specific build.

Artifact IDs identify immutable packaged content. Multiple artifact IDs can
belong to the same module over time.

Interface versions identify the contract between callers and the module. They
are separate from module versions and backend ABI versions. A module can change
implementation version without changing its public interface, and a backend can
change its loader ABI without changing the module's domain interface.

Path keys are derived from validated module IDs through one canonical mapping.
They exist for storage layout and routing metadata; they are not a second module
identity.

## Backend Model

The backend determines how a module version is materialized and invoked. It does
not change the lifecycle vocabulary.

| Backend | Intended Use | Isolation Boundary |
|---|---|---|
| `native-rust` | First-party trusted behavior compiled into Sinomo | The Sinomo process and normal Rust type system. |
| `native-dylib` | Trusted first-party behavior built as a separate native artifact | Same process as the host; no security boundary. |
| `declarative` | Prompts, configs, tool descriptors, eval suites, routing rules, policy data | Schema validation and host interpretation. |
| `sandbox-process` | Tools, adapters, generated prototypes, benchmark runners | Process sandbox, filesystem/network policy, resource limits. |
| `wasm-component` | Optional future procedural extension backend | Wasm/component runtime boundary. |

### Native Rust

Static native modules are the simplest first-party form. They are compiled into
the host and registered deterministically at process startup. They still have
module identity, manifests, evaluation gates, routing state, telemetry, and
rollback attribution.

This backend is appropriate for trusted behavior whose rollout naturally follows
the host binary. It is also the baseline for R0/R1 code where dynamic loading
would make continuity harder to reason about.

### Native Dynamic Libraries

Native dynamic libraries are trusted native modules loaded from immutable,
content-addressed artifacts. They exist to reduce build and deployment blast
radius for large or independently stable first-party code.

They are not a sandbox. A faulty native dynamic module can corrupt host memory,
leak resources, deadlock, or crash the daemon. Therefore this backend must be
limited to trusted code and gated like other host-process code.

The dynamic boundary should stay small and stable: a versioned C-compatible
loader ABI, byte buffers, status codes, host callback tables, descriptors, and
SOBJ request and response envelopes. Native descriptors may declare object-RPC
routes and embedded declarative resources so the host can expose a module at
runtime without linking module-specific crates into the main binary.

Unloading native dynamic libraries is best effort. The lifecycle transition can
remove the module from active routing and ask the module to shut down, but the
platform loader or module-global state may keep code resident until process
restart. Correctness must come from routing and state transitions, not from an
assumption that `dlclose` or the platform equivalent always fully reclaims the
module.

### Network Modules

Network transport modules are native dynamic libraries with an additional,
narrower contract. Besides the generic loader handshake, they implement the
`sinomo-network` network-module vtable (descriptor, start/stop,
request/response dial, advertised endpoints) and declare their transports
through the module surface data, so the daemon loads them from
content-addressed artifacts like any other native-dylib module and then
registers them into the runtime network-module registry. Iroh is the first
module of this kind; serial and LoRa transports are planned. The transport
contract is described in [network-architecture.md](network-architecture.md).

They use the same node-local loader states as other native-dylib modules:
stable, previous, shadow, candidate, and rolled-back versions with
content-hash verification before load. One lifecycle nuance is load-bearing:
network modules support runtime enable/disable, and disable stops the module
and removes its transports from dial dispatch, but it never unloads the
library. The host keeps the library handle alive for the life of the process
so no dial path can race a dangling vtable pointer; process restart remains
the reclamation path, consistent with the general unload contract.

### Declarative

Declarative modules have no executable code. They are structured data consumed
by a trusted host service.

Examples include prompts, model routing policy, agent profiles, tool
descriptors, evaluation fixtures, benchmark definitions, UI configuration, and
workflow templates.

Declarative modules matter because many useful self-evolution changes are data
changes rather than code changes. They still require manifest validation,
schema compatibility, provenance, evaluation where applicable, telemetry, and
rollback state.

### Sandbox Process

Sandbox-process modules execute external programs, generated scripts, command
packages, or tool adapters under the Sinomo sandbox boundary.

The host provides containment: filesystem roots, network class, secrets, device
access, runtime limits, output capture, and cancellation. The module still owns
domain semantics. For example, a dependency scanner must understand what a
dependency scan means; the host only controls what resources the scanner can
touch.

This is the default procedural backend for generated or untrusted behavior.

### Wasm Component

Wasm components are an optional future backend for procedural modules that need
portable dynamic loading, stronger ABI separation, or better crash/resource
isolation than native code.

Wasm should not become the universal extension format. It should be introduced
behind typed interfaces and backend-specific evaluation gates when the runtime
cost is justified.

## Registry And Assembly

The runtime module registry is assembled from explicit sources in deterministic
precedence order:

```text
static native registrations
  + local declarative configuration
  + node-local activations
  + cluster desired state
  -> module registry and loader view
```

Local configuration supports development, bootstrap, and pre-cluster operation.
Cluster desired state overrides shared local configuration after the node joins
a cluster.

Executable artifacts are never selected by ambient plugin discovery. A loader
may only apply an artifact chosen through manifest, content hash, provenance,
compatibility, policy, and loader or rollout state.

### Catalog Surfaces And Runtime Binding

Module catalog registration is pure data. It records the manifest, backend
descriptor, declared capabilities, and public surfaces exposed by the module.
Public surfaces include object-RPC routes, event kinds, capability imports and
exports, settings definitions, and storage contract owners. This data is
serializable, inspectable, comparable across nodes, and safe to place in
registry or cluster desired state.

### Settings Surfaces

A module that exposes settings also owns their field-complete schema. The
schema describes nested structure, labels, defaults, constraints, choices,
sensitive values, and collection element types closely enough for clients to
validate values and construct typed controls. A schema-less definition is a
legacy compatibility case, not the normal module contract. Catalog entries use
an explicit module ID to associate settings with a module; clients must not
infer ownership from display names or key prefixes.

SOBJ is the native settings representation across the module ABI, object RPC,
and cluster KV. Human-editable JSON files and JSON-shaped interface values
are projections for local inspection and editing, not a parallel distributed
contract. Settings interfaces render the declared schema by default and expose
a raw JSON editor only for legacy schema-less definitions or fields explicitly
declared as dynamic JSON. A catalog may also expose operational documents as
read-only, in which case interfaces present their structured values without
offering mutation.

Runtime binding is separate. A static native Rust module may have a compile-time
binding table that maps a `native-rust` registration key to service builders,
storage contract hooks, maintenance hooks, or other host callbacks. That
binding table is not catalog state and is not shipped as cluster-desired data.
It is deterministic process assembly for a particular Sinomo binary.

This split keeps module activation auditable:

- the catalog says what the module version is and what surfaces it owns,
- the loader says which catalog entry is active on this node,
- the runtime binding says how this binary materializes that active entry.

For example, `sinomo-dfs` publishes the DFS object-RPC routes, DFS/SIFS
capabilities, DFS event families, and the `sinomo-dfs` storage ownership through
its module surfaces. Its storage path registration is a native runtime binding.
`sinomo-network` owns generic object-RPC framing and transport mechanics; it
does not own DFS route names. The daemon may enforce local authentication and
drive the service event loop, but the route catalog belongs to the module that
implements the behavior.

### Host Event Loop

The daemon should trend toward a generic service/event loop that reads module
catalog surfaces and asks active module bindings to register their runtime
handlers. Daemon-owned foundational routes, such as process status, shutdown,
and cluster transport, can remain host services. Domain behavior such as DFS
metadata repair, object transfer, and operator inspection should live in the
domain module crate or in a module-owned service binding.

This does not require every module to be dynamically loaded. Static native Rust
modules still participate through the same catalog and binding split: the
catalog is data, and the binding is compiled into the host.

## Lifecycle

The module lifecycle is shared across backends:

```text
draft
  -> packaged
  -> registered
  -> evaluated
  -> staged
  -> shadow
  -> canary
  -> active
  -> deprecated
  -> retired
```

Failure and recovery paths are first-class:

```text
packaged -> rejected
registered -> quarantined
evaluated -> failed
shadow -> rolled-back
canary -> rolled-back
active -> reverted
active -> unloaded
```

Not every module goes through every state. For example, a local declarative
module may be activated without cluster staging, and a static native module may
be activated only by binary rollout. The vocabulary remains shared so telemetry,
evaluation, rollback, and operator reasoning stay consistent.

## Loader State

Loader state is node-local applied state. It answers:

- which version this node should invoke for active traffic,
- whether a candidate should receive canary traffic,
- whether a shadow version should receive mirrored traffic,
- what previous version can be restored,
- which backend materialization is currently applied,
- what the loader last attempted and why.

Loader state must be small, inspectable, and replayable enough to explain a
route decision. It should not duplicate large artifacts or become the source of
truth for immutable content.

## Routing

Routing is lifecycle infrastructure, not backend infrastructure.

A route decision uses:

- module ID,
- route key,
- active/candidate/shadow state,
- traffic weight,
- backend availability,
- policy status.

For a given module, route key, and traffic weight, canary selection must be
deterministic. Repeated calls should take the same lane until state changes.
This makes canaries stable and replay meaningful.

The route decision records the selected lane before invocation. Backend runners
then execute the selected version and emit telemetry tied to the module ID,
artifact ID, lane, route key, and outcome.

## Evaluation

Evaluation is backend neutral at the top level and backend specific at the
edges.

Common gates include:

- manifest validation,
- schema compatibility,
- capability diff review,
- provenance and signature verification,
- build or package verification,
- unit and integration tests,
- replay tests,
- private regression evaluations,
- resource usage checks,
- shadow comparison,
- canary analysis,
- rollback readiness.

Backend-specific gates refine the common gates:

- `native-rust`: crate tests, feature compatibility, and binary rollout
  readiness.
- `native-dylib`: platform compatibility, loader ABI compatibility, symbol and
  descriptor validation, host-process health checks, and restart/unload
  behavior.
- `declarative`: schema validation, semantic linting, dry-run replay, and
  consumer compatibility.
- `sandbox-process`: sandbox policy validation, command determinism, filesystem
  and network access checks, and output/resource limits.
- `wasm-component`: component validation, interface compatibility, fuel/memory
  limits, and runtime compatibility.

Evaluation output feeds scorecards, policy decisions, rollout gates, and
module telemetry.

## Policy Boundary

Policy is layered.

Host policy decides coarse authority:

- whether the backend is allowed,
- which trust ring may load the module,
- what filesystem, network, secret, device, and process authority is available,
- whether capability increases require review,
- what resource budgets apply,
- whether rollout can proceed.

Module logic owns domain semantics:

- whether an input is meaningful for the module,
- whether generated output is safe for that domain,
- whether an external endpoint is semantically valid,
- whether a declarative payload is internally coherent.

Rollout policy decides promotion and rollback using evidence: scorecards,
telemetry, replay results, shadow/canary metrics, incidents, user corrections,
and operator approvals.

## Build And Artifact Supply

Binary and compiled-module production is supplied by the cluster build service.
A build request identifies reviewed source, dependency state, profile and
features, toolchain, artifact kind, and explicit target compatibility. Workers
publish immutable artifacts and provenance. The initial implementation records
them in a verified builder-local cache; the trusted registry is the future
cluster-wide catalog of what exists.

The build service resolves a matching verified artifact from its local cache
before scheduling compilation; shared cache lookup can extend that resolution
without changing artifact identity. When compilation is needed, it coalesces
identical requests and chooses a live compatible worker using resource and
queue evidence. Target-independent modules may be reused across platforms.
The current rollout groups nodes by their reported operating system and
architecture, then resolves that request to a builder's concrete Rust target
triple and toolchain; richer node ABI reporting can tighten the grouping
contract later.

Artifact production is not a lifecycle transition. The builder cannot select a
candidate, change cluster desired state, write loader state, or approve its own
evidence. See [build service architecture](build-service-architecture.md) for
the scheduling, cache, and distribution contract.

## Rollout And Local Activation

Local activation is a node-local operation. It is useful for development,
bootstrap, pre-cluster configuration, and local-only features. It can select a
local artifact or declarative payload and write applied loader state.

Rollout is cluster coordination. It consumes verified prebuilt artifacts by
default and requires cluster authority, distribution of immutable content,
desired state, node sets, lane selection, telemetry aggregation, and rollback
policy. Without a cluster, there is no rollout. A build result becomes eligible
input to rollout; it does not become desired or applied state by itself.

When a node is clustered, cluster desired state wins for shared modules. The
node loader materializes and applies that desired state locally, but it should
not invent conflicting rollout state.

## Unload And Replacement

Unload is a lifecycle transition that removes a module version from active
routing on a node.

The architectural contract is:

- new invocations should stop selecting the unloaded version after loader state
  changes,
- in-flight invocations may complete on the old generation,
- cleanup hooks are best effort,
- content-addressed artifacts are not overwritten in place,
- process restart remains the correctness fallback for host-process resource
  reclamation.

For sandbox-process and future Wasm backends, unload usually means tearing down
processes, instances, or cached runners. For native dynamic libraries, unload
means calling the module shutdown hook if available and releasing the host's
library handle, while accepting that the operating system or runtime may keep
some state resident.

## State Ownership

The architecture separates six state classes:

| State | Owner | Mutability |
|---|---|---|
| Module manifest or capsule | Registry / module catalog | Immutable after registration. |
| Build request, queue, and lease state | Cluster build service | Mutable compact coordination state. |
| Artifact or payload bytes | Registry / DFS / local cache | Immutable and content addressed. |
| Cluster desired module state | Cluster control plane / rollout | Mutable by rollout authority. |
| Node-local loader state | Local loader | Mutable applied state. |
| Telemetry and lifecycle records | Event and evidence stores | Append-only observations. |

This separation keeps rollback and audit simple. Mutable state points at
immutable content; it does not rewrite that content.

## Relation To Self-Evolution

Self-evolution should target the narrowest natural unit:

- declarative capsule for prompts, configs, model routing, tool descriptors,
  and evaluation data,
- sandbox-process module for generated or experimental tools,
- static native Rust module for trusted behavior that should remain in the host,
- native dynamic-library module for trusted first-party behavior whose build and
  deployment boundary is stable enough to justify a separate native artifact,
- Wasm component only when dynamic procedural isolation is worth the runtime
  cost.

Evolution target ring R3 does not mean Wasm and is not a Cargo dependency-ring
tag. It means lower blast radius and stronger containment. The backend is a
separate architectural decision.

Every self-evolution candidate still needs evidence before activation:
manifest validation, capability review, tests, replay, scorecard, provenance,
policy decision, and rollback plan.

## Contract Ownership

The crate boundary follows the trust-ring architecture:

- `sinomo-base` owns foundational module manifest vocabulary that must be
  visible below higher-level services; the `sinomo-core` facade re-exports it.
- `sinomo-module` owns low-level module glue: backend-neutral catalog
  registration, loader activation state, routing and invocation primitives,
  native dynamic-library host loading, unload records, raw replay contracts,
  module telemetry contracts, and serializable module surface declarations. It
  must not own policy decisions, rollout admission, rollout transition state,
  or self-evolution scoring.
- `sinomo-module-api` owns the stable native dynamic-library ABI surface,
  including generic descriptor fields for self-described object-RPC routes,
  embedded declarative resources, and declared network transports.
- `sinomo-network` owns the network-module registry, descriptors, and the
  network-module dylib vtable that transport modules implement.
- `sinomo-daemon` owns the runtime registry that assembles the module table:
  internal native-rust registrations and bindings, dylib loader adaptation,
  module-to-route conversion, and the composed storage layout.
  `sinomo-runtime` re-exports that machinery as
  `sinomo_runtime::{modules, storage}` without adding behavior.
- `sinomo-policy` owns policy decisions and policy vocabulary interpretation.
- `sinomo-build` owns build identity, cache resolution, queued job coordination,
  worker selection, compilation, packaging, and publication. It does not own
  rollout admission or activation state.
- `sinomo-registry` owns immutable artifact, capsule, provenance, evidence, and
  signer records.
- `sinomo-rollout` owns cluster rollout transitions, rollout telemetry gates,
  rollout load admission, promotion decisions, and rollback coordination. It
  may write node-local module activation state when applying a rollout
  transition, but the rollout decision model stays outside `sinomo-module`.
- `sinomo-sandbox` owns sandbox-process containment and optional sandboxed
  execution backends.
- `sinomo-evolution` owns candidate generation, evidence assembly, evidence
  interpretation, self-evolution scorecard adapters, and promotion requests; it
  consumes module lifecycle contracts but does not define R1 module primitives.
- Domain module crates own their domain route names, event kinds, storage roots,
  and host-service binding helpers. Generic infrastructure crates own framing,
  dispatch, storage verification, and daemon event-loop mechanics.

R1 crates cross into the cluster foundation through the `sinomo-core` facade.
R2 and application crates cross into these foundations through
`sinomo-runtime`. R0/R1 crates must not depend on R2 or application crates.

## Architectural Invariants

1. A module is a lifecycle unit, not a runtime format.
2. Backend choice must not bypass manifest, provenance, policy, evaluation, or
   telemetry requirements.
3. Immutable content is addressed by hash and never overwritten in place.
4. Node-local loader state is applied state, not cluster authority.
5. Cluster rollout state overrides local shared module configuration once a node
   is clustered.
6. Building or publishing a module cannot activate, promote, or select it for
   rollout.
7. Compiled artifacts declare explicit target compatibility; operating system
   and architecture labels alone are not assumed sufficient.
8. Routing decisions must be deterministic for stable inputs and state.
9. Native dynamic libraries are trusted code and do not provide containment.
10. Unload is best effort for resource reclamation and authoritative for future
   routing only after loader state changes.
11. Wasm remains optional and backend-specific.
12. Self-evolution targets modules only when the behavior has a natural,
    evaluable boundary.
13. Module catalog data must not contain executable bindings; native runtime
    binding is deterministic host assembly keyed by catalog entries.
