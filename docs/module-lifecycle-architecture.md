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
- Existing behavior may be extended only through versioned extension points
  declared by the subsystem that owns the behavior. A module cannot intercept
  arbitrary functions, routes, events, or rendered UI nodes.
- Extension-point owners define composition, ordering, authority, timeout, and
  failure semantics. Contributors provide typed data or operations; they do not
  choose how competing contributions are resolved.
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
methods, agent packages, and embedded declarative resources so the host can
expose a module at runtime without linking module-specific crates into the main
binary.

Active native providers bind through one generic
`sinomo_module::ActiveNativeComponent` path. The selector accepts a
`ServiceRequirement`, verifies every candidate descriptor, rejects zero or
ambiguous matches, and owns content-addressed loading plus SOBJ invocation.
Typed protocol clients remain in their owning domain crates (`sinomo-dfs` and
`sinomo-sandbox`); neither the module loader nor `sinomo-runtime` contains a
filesystem- or sandbox-specific host adapter.

Unloading native dynamic libraries is best effort. The lifecycle transition can
remove the module from active routing and ask the module to shut down, but the
platform loader or module-global state may keep code resident until process
restart. Correctness must come from routing and state transitions, not from an
assumption that `dlclose` or the platform equivalent always fully reclaims the
module.

Windows Cloud Files is a native-dylib module whose durable CFAPI registration
can outlive one loaded artifact. Its host supervisor therefore pins the exact
content-addressed provider that owns the registration and rejects loader drift
instead of falling back to a different DLL. See the
[Windows Cloud Files architecture](cloud-files-architecture.md) for that
provider-selection and recovery contract.

### Network Modules

Network transport modules are native dynamic libraries with an additional,
narrower contract. Besides the generic loader handshake, they implement the
`sinomo-network` network-module vtable (descriptor, start/stop, authenticated
link and stream operations, local endpoints, and destroy) and declare their
transports through the module surface data. The host loads them from
content-addressed artifacts like any other native-dylib module and registers
them into the process network-module registry (`sinomo-module` owns that
loader). Iroh is the first module of this kind; serial and LoRa transports are
planned. The transport contract is described in
[network-architecture.md](network-architecture.md).

They use the same node-local loader states as other native-dylib modules:
stable, previous, shadow, candidate, and rolled-back versions with
content-hash verification before load. One lifecycle nuance is load-bearing:
runtime enable/disable stops the module and removes its transports from dial
dispatch but keeps the module registered and its library mapped, so no dial
path can race a dangling vtable pointer. Only hot unload retires the library,
through the network-specific drain described under
[Unload And Replacement](#unload-and-replacement).

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

Executable artifacts are never selected by ambient plugin or source-tree
discovery. A loader may only apply an artifact chosen through a signed module
distribution profile, manifest, content hash, provenance, compatibility,
policy, and loader or rollout state.

### Catalog Surfaces And Runtime Binding

Module catalog registration is pure data. It records the manifest, backend
descriptor, declared capabilities, and public surfaces exposed by the module.
Public surfaces include object-RPC methods, agent package definitions, embedded
skills and prompts, event kinds, capability imports and exports, settings
definitions, extension-point definitions and contributions, and storage
contract owners. This data is serializable, inspectable, comparable across
nodes, and safe to place in registry or cluster desired state.

Agent definitions are declarative module surfaces even when their provider is a
native dynamic library. They name a role, prompt, standard tool descriptors,
skill resources, and optional context/tool-surface hints. The host validates the
data and assigns provenance from the enclosing module registration; modules do
not supply arbitrary Rust constructors or catalog mutation callbacks. Agent IDs
must be unique within a module, across the active registry, and against existing
catalog principals. Ambiguity fails activation instead of using registration
order as a tie-breaker.

Agents, skills, and callable tools remain independent surfaces. An agent package
may list default skill IDs and visible toolsets, but a skill is not owned by or
hard-bound to that agent and may resolve from another active module or the shared
skill catalog. Object-RPC routes are the module's standalone callable tool
surface: agent runtimes may project them as tools, while modules and non-agent
clients may call the same routes directly through normal capability, policy,
and authority checks.

### Typed Extension Points And Contributions

Module services answer what a component provides. Extension points answer
where several modules may participate in one host-owned behavior or
presentation surface. The mechanisms share module identity, typed interface
operations, capability checks, lifecycle generations, and provenance, but an
extension point adds an explicit host-owned composition contract.

Every extension point has a stable ID and version and is declared by the
subsystem that owns the affected behavior. Its definition specifies:

- input, contribution, and output schema IDs;
- whether contributions are declarative, operation-backed, or either;
- locality and whether remote providers are permitted;
- a composition policy and deterministic ordering policy;
- latency, request, response, fan-out, and result-count bounds;
- timeout, cancellation, and partial-failure behavior;
- capabilities required to contribute and capabilities required to apply the
  result;
- whether user or operator selection is required; and
- which host-owned context keys may affect eligibility.

A module contributes a bounded record containing a namespaced contribution ID,
the target extension-point ID and compatible version, an optional declarative
payload, and an optional reference to a logical operation in one of the
module's typed interface exports. Operation-backed contributions name only the
export and logical operation; the live registry resolves the concrete provider
and route. A contribution cannot name a raw host function, Rust type, rendered
node selector, or undeclared object-RPC route.

Common contribution metadata is shared across UI and behavior surfaces:

```text
contribution id
target extension point and version
module and artifact provenance
optional context predicate
group and numeric order
optional before/after contribution IDs
declarative payload or logical operation reference
```

The host compiles active definitions and contributions into one immutable,
generation-stamped extension registry. Eligibility is evaluated first, then
group order, acyclic before/after constraints, numeric order, module ID, and
contribution ID. Activation order is never a tie-breaker. Cyclic relative-order
constraints are diagnosed and discarded without discarding otherwise valid
contributions. An extension registry generation is published atomically with
the service generation it references; unloading or replacing a module removes
its contributions before its scope is unwound.

#### Composition Policies

An extension point chooses one composition policy. A contributor cannot change
the policy for the point it targets.

| Policy | Use | Multiple-contributor behavior |
|---|---|---|
| `many-ordered` | UI sections, menu items, handlers that all produce independent output | Retain every eligible contribution in deterministic order. |
| `collect` | Enrichers, diagnostics, candidate generation, metadata providers | Invoke providers independently and return attributed partial results. |
| `reduce` | Scores, decorations, constraints, or domain-specific patches | The point owner applies a trusted reducer to all attributed results. |
| `pipeline` | Explicit staged transformation of a typed value | Apply bounded validated deltas in deterministic order; no contributor receives a `next` callback. |
| `select-one` | Replaceable provider or renderer | Use an explicit user, operator, configuration, or policy selection; ambiguity never resolves by install order. |
| `veto` | Admission and safety constraints | Any valid denial constrains the result; contributors can never manufacture authority or convert a host denial into approval. |

`pipeline` is not general middleware. The extension-point owner defines the
typed delta vocabulary and validates every intermediate result. Core identity,
authentication, policy, storage integrity, consensus, final device authority,
and recovery checks remain outside replaceable pipelines. An extension may
propose a change or add a constraint, but the owning subsystem performs the
authoritative state transition after revalidation.

Failure behavior is also point-owned. Observational enrichment may preserve
partial success, an optional presentation contribution may be omitted, and an
authority constraint normally fails closed. Timeouts, malformed output, and
provider disappearance are recorded per contribution with module and artifact
provenance. A module cannot mark its own failure ignorable.

#### Relation To Services And Events

Existing service offers and requirements remain the execution substrate.
`ServiceRequirement` declares exactly which logical operations and caller
capabilities a component may exercise. The extension registry resolves an
operation-backed contribution through that checked path and never creates a
parallel callback registry. Existing deterministic local fan-out provides the
base for `collect` and `reduce`; selected providers use the same live discovery
identity and health evidence.

Events remain typed facts, not mutation waterfalls. They are appropriate for
observation, refresh, audit, and asynchronous reactions. A synchronous
composition decision uses an extension point so its ordering, deadline,
failure, and authority semantics are explicit. Publishing an event does not
insert a module into a behavior pipeline, and subscribing to an event does not
grant permission to alter the action that emitted it.

#### Application UI Contributions

Application UI uses the same extension registry rather than a separate plugin
system. The app defines semantic extension points such as navigation views,
toolbar groups, context menus, inspector sections, settings sections, status
items, and named slots inside built-in surfaces. UI points normally use
`many-ordered`, `reduce`, or `select-one` composition.

Each UI extension point owns a bounded, versioned payload schema appropriate to
its semantic slot. A contributed view, panel, or inspector section may use a
host-rendered document payload, while actions and provider-backed surfaces may
reference logical operations from the contributor's typed interface export.
Those payload schemas do not own placement or cross-extension conflict
resolution. Raw routes, arbitrary rendered-tree patches, native widget
callbacks, arbitrary colors, and executable UI payloads are not catalog data.

When host-rendered documents are not expressive enough, a custom-surface
payload may reference a separately hashed UI component inside the module's
immutable payload. The reference remains catalog data; executable bytes do not.
The desktop app starts `sinomo-ui-extension-host` lazily, one isolated worker
per component instance. The worker exposes no ambient filesystem, network,
clipboard, device, or daemon API and accepts only the `sinomo.ui.widget/1.0.0`
contract. It returns complete bounded frames containing validated paint
commands, image resources, semantic nodes, and explicit host requests. SUI
paints the most recent accepted frame, so component execution and IPC never
block measure, arrange, event dispatch, or paint.

Custom content is clipped to its host-owned rectangle. Arbitrary color and
geometry are permitted inside that rectangle, but a component cannot restyle
host chrome or receive native window, renderer, GPU, SUI widget, or Rust trait
objects. Read-only content access uses an instance-scoped versioned grant and
bounded range requests rather than a path. Side effects remain logical action
requests mediated by the daemon and re-authorized at the final owner.

Built-in surfaces expose stable semantic slots rather than mutable child
indexes or selectors. Additive contributions such as actions, panels, badges,
and tooltip sections compose as ordered collections. Scalar properties such as
the label or visibility of a core control are not general multi-writer
extension points. Replacement surfaces use `select-one`, preserve a safe
built-in provider, and require explicit selection. User-owned layout,
visibility, and ordering preferences override module defaults without changing
the module artifact.

The application owns native rendering, accessibility, focus, semantic theming,
input state, and confirmation UI. The daemon owns catalog mediation and command
dispatch. Invoking a contributed action sends the contribution identity and
semantic context to the daemon; the daemon resolves the logical operation and
rechecks authority. Presentation metadata never grants direct access to a
module route.

#### Behavior Contributions

Behavior extension points are declared at stable domain join points, not
around arbitrary implementation functions. Illustrative points include agent
context enrichment (`collect`), diagnostic providers (`collect`), ranking
adjustments (`reduce` or a typed `pipeline`), replaceable file openers
(`select-one`), and additional admission constraints (`veto`). The point owner
defines domain-specific schemas and the trusted reducer or delta validator.

First-party behavior may use the same contribution path when it is naturally
replaceable or additive. Foundational authority and recovery behavior remains a
host-owned baseline rather than an ordinary removable contribution. This lets
modules extend existing functionality without turning module activation into
arbitrary monkey-patching.

### Settings Surfaces

A module that exposes settings also owns their field-complete schema. The
schema describes nested structure, labels, defaults, constraints, choices,
sensitive values, and collection element types closely enough for clients to
validate values and construct typed controls. A schema-less definition is a
legacy compatibility case, not the normal module contract. Catalog entries use
an explicit module ID to associate settings with a module; clients must not
infer ownership from display names or key prefixes.

A settings definition may additionally carry module-owned presentation
metadata: a user-facing module label and description. Generic settings clients
use that metadata for lifecycle cards instead of maintaining per-module label
tables. A read-only operational definition may declare which root fields carry
an error and its resolution. Clients can then render actionable failure UI
without knowing domain-specific field names; this metadata is presentation
only and grants no write or lifecycle authority.

SOBJ is the native settings representation across the module ABI, object RPC,
and cluster KV. Human-editable JSON files and JSON-shaped interface values
are projections for local inspection and editing, not a parallel distributed
contract. Settings interfaces render the declared schema by default and expose
a raw JSON editor only for legacy schema-less definitions or fields explicitly
declared as dynamic JSON. A catalog may also expose operational documents as
read-only, in which case interfaces present their structured values without
offering mutation.

### Runtime Binding

Runtime binding is separate. Every static or external implementation provides a
`ComponentFactory`. Its `ComponentDescriptor` declares versioned service offers
and requirements; mounting returns a `ComponentScope` that owns services,
workers, readiness callbacks, and reverse-order compensations. Static factories
may additionally contribute storage and configuration schemas. These factories
are deterministic process assembly for a particular Sinomo binary and are not
shipped as cluster-desired data.

This split keeps module activation auditable:

- the catalog says what the module version is and what surfaces it owns,
- the loader says which catalog entry is active on this node,
- the runtime binding says how this binary materializes that active entry.

For example, `sinomo-dfs` publishes the DFS object-RPC methods, DFS/SIFS
capabilities, DFS event families, and the `sinomo-dfs` storage ownership through
its module surfaces. Its storage path registration is a native runtime binding.
The object-RPC layer owns generic request and response framing over an
authenticated duplex stream. Direct-link and routing layers establish that
stream without knowing its methods. The daemon may enforce local authentication
and drive the service event loop, but the single service-method catalog is
assembled by `sinomo-service` from declarations belonging to the modules that
implement the behavior.

### Host Event Loop

The daemon runs a generic component reconciler over one live service-registry
snapshot. Components remain pending until required services are available;
registry replacement preserves unchanged scopes and atomically publishes one
new generation. Daemon-owned foundational methods, such as process status,
shutdown, and cluster transport, remain host services. Domain behavior and
long-lived workers live in their owner component. Host readiness callbacks run
once the daemon accepts requests and, for a clustered main, owns its local Raft
node, so workers that use cluster storage start after a predecessor's handoff
rather than retrying through it.

The service-catalog publisher compares source-owned records by stable service
semantics; equal records produce no writes. Registry generation changes drive
publication. A host that stops for shutdown or process replacement withdraws
local admission without publishing the withdrawal, so the node's committed
offer survives the replacement and a retiring process cannot remove records its
successor owns. The publisher also reconciles when a committed change reaches
the node's own records, which repairs a predecessor's late write without a
timer. Removing a member retires its records. The same component resolves the
providers of a capability for local clients: its own node first when the live
registry offers the capability, then ready providers on authorized members that
the routing graph can reach, by route cost. Clients send each operation to one
resolved provider instead of probing candidates.

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

- the module first enters `draining`; a process-wide invocation gate rejects
  new calls even through retained old service or extension snapshots,
- the host compiles and publishes module, extension, UI-catalog, and service
  projections without the draining module before teardown,
- dependency-driven component deactivations join the same bounded drain rather
  than losing their scopes underneath active handlers,
- in-flight invocations may complete within the point/route deadline; native
  callback operations also receive cancellation,
- component scopes, workers, and reverse-order disposers remain alive until
  their invocation leases reach zero,
- a retiring scope stops admission, then signals and joins its owned tasks and
  task-drain hooks before any disposer runs; a join timeout keeps the scope,
  and the storage it holds, owned for a bounded retry,
- a drain timeout leaves the routes withdrawn and the component scope
  quarantined, reports `restart_required`, and does not claim physical unload,
- cleanup hooks and native-library unmapping remain best effort after logical
  unload is complete,
- content-addressed artifacts are not overwritten in place,
- process restart remains the correctness fallback for host-process resource
  reclamation.

`ModuleRuntimeGenerationStore` publishes the matching module and extension
snapshots together with the service-registry generation that serves them.
Catalog services read this live generation instead of retaining a startup-only
snapshot. UI clients observe catalog-generation changes and discard isolated
widget sessions from the previous generation, preserving the built-in surface
until a provider is selected again.

For sandbox-process and future Wasm backends, unload usually means tearing down
processes, instances, or cached runners. For native dynamic libraries, unload
means calling the module shutdown hook if available and releasing the host's
library handle, while accepting that the operating system or runtime may keep
some state resident.

Every standard native-dylib entry uses `NativeModuleLifecycle`. Initialization
opens module-local admission, every ABI invocation retains a guard, and
shutdown closes admission and refuses to proceed while a guard is active. A
failed shutdown keeps host callbacks and the executable mapping quarantined;
the host must never turn a non-OK shutdown status into a successful unmap.

Network dylibs have a second lifetime because authenticated links and streams
hold vtable callbacks outside standard object RPC. Network-module unregister
publishes route removal first, stops the listener/dialer, and waits for the last
transport or stream owner to run `destroy` and release the library keep-alive.
If that retirement probe misses the drain deadline, generic module unload may
finish logically but physical unload is reported incomplete and requires a
process restart.

The authenticated `/module/hot-unload` route is the runtime transition owner.
The CLI calls this route and deliberately refuses the old shortcut of deleting
native loader state in a separate process, because that cannot fence daemon
routes or drain daemon-owned invocations.

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
  module telemetry contracts, serializable module surface declarations, and
  the live extension registry and deterministic resolver. It must not own
  domain reducers, policy decisions, rollout admission, rollout transition
  state, or self-evolution scoring.
- `sinomo-module-api` owns the sole stable native dynamic-library ABI and the
  provider-neutral component descriptor vocabulary, including generic
  extension-point definitions, contribution envelopes, operation references,
  ordering metadata, and composition-policy identifiers. There are no
  provider-specific API crates in the runtime facade; an external module keeps
  private provider protocols inside its own package and exposes standardized
  services through component offers.
- `sinomo-client` is the R2 client facade for application surfaces. It owns
  connection supervision and typed remote clients, never domain authority or
  daemon-side implementations.
- `sinomo-ui-extension-host` is an application-ring sibling process. It reaches
  the data-only `sinomo-module-api` contract through a narrow reviewed ABI
  exception, verifies the selected artifact hash, applies Wasm memory and fuel
  limits, and exposes no runtime, daemon, module-loader, sandbox, or
  product-composition implementation to the graphical process.
- The direct-link layer inside `sinomo-network` owns the network-transport
  registry, descriptors, and network-module dylib vtable that transport modules
  implement.
- `sinomo-product-profile` builds the shared product `ComponentCatalog` for the
  main, shadow, embedded, and mobile profiles: built-in module registrations,
  component factories, and configuration and storage contributions. Hosts add
  only process-specific components to it.
- `sinomo-module` materializes that catalog: module-to-method conversion,
  native-dylib and network-module loading, the compiled extension registry,
  the `ModuleRuntimeGenerationStore`, invocation leases and drain, and the
  hot-unload request and report contract.
- `sinomo-daemon` hosts the result. It seeds the module snapshot from the
  supplied catalog, publishes each module, extension, and service-registry
  generation together, runs the component reconciler, orchestrates the
  `/module/hot-unload` transition, and composes the foundation storage layout
  with catalog storage contributions. `sinomo-runtime` re-exports the module
  registry types as `sinomo_runtime::modules` and the storage helpers as
  `sinomo_runtime::storage` without adding behavior.
- `sinomo-policy` owns policy decisions and policy vocabulary interpretation.
- `sinomo-build` owns build identity, cache resolution, queued job coordination,
  worker selection, compilation, packaging, and publication. It does not own
  rollout admission or activation state.
- `sinomo-registry` owns immutable artifact, capsule, provenance, evidence, and
  signer records.
- `sinomo-rollout` (with its daemon-process adapter `sinomo-rollout-host` and
  caller-side `sinomo-rollout-client`) owns cluster rollout transitions,
  rollout telemetry gates, rollout load admission, promotion decisions, and
  rollback coordination. It may write node-local module activation state when
  applying a rollout transition, but the rollout decision model stays outside
  `sinomo-module`.
- `sinomo-sandbox` owns sandbox-process containment and optional sandboxed
  execution backends.
- `modules/evolution/sinomo-evolution` is the independently built
  `module.evolution` native-dylib. It owns candidate generation, evidence
  assembly and interpretation, self-evolution scorecard adapters, promotion
  requests, settings, routes, and CLI execution. Hosts consume its typed
  contract and generic native-module descriptor; they do not link its
  implementation or register a parallel evolution service.
- Domain module crates own their domain method names, event kinds, storage
  roots, extension-point schemas, trusted reducers and delta validators, and
  host-service binding helpers. Generic infrastructure crates own framing,
  dispatch, deterministic contribution resolution, storage verification, and
  daemon event-loop mechanics.

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
13. Module catalog data must not contain executable callbacks or raw host
    bindings. A contribution may reference a typed logical operation, but the
    live service registry resolves its provider and route; native runtime
    binding remains deterministic `ComponentFactory` assembly keyed by catalog
    entries.
14. Every mounted runtime effect belongs to a `ComponentScope`; durable effects
    register replayable compensation rather than relying on process-local drop.
15. Default external modules come from the verified signed data profile at
    `distribution/module-profile.json`, never Cargo source-tree scanning or
    provider constants in rollout. Library stems in that profile are resolved
    to the current platform's native filename by the generic rollout loader.
16. Active-provider discovery and native loading have one implementation in
    `sinomo-module`; domain crates may own typed clients but not parallel loader
    scans or standalone host-adapter crates.
17. Module UI contributions use bounded, versioned extension-point payloads;
    modules never load Rust/SUI widgets, patch rendered child indexes, or name
    unvalidated transport routes in the app. Custom surfaces execute only as
    verified isolated UI components and the app renders their validated last
    complete frame.
18. A module may affect existing behavior only through a typed, versioned
    extension point declared by the subsystem that owns that behavior.
19. Extension-point composition, ordering, failure handling, and final
    authority are host-owned and deterministic; activation order is never an
    implicit precedence rule.
20. Contributions may add results, propose bounded deltas, provide a selected
    implementation, or add constraints. They cannot grant authority, bypass a
    host denial, or replace foundational identity, integrity, consensus,
    policy, or recovery checks.
21. Active extension contributions and the service operations they reference
    share an atomic lifecycle generation. Unloading a component removes its
    contributions before unwinding its effects.
22. Component events remain attributable facts and notifications, not an
    implicit ordered mutation or middleware chain.
