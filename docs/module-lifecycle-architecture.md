# Sinomo Module Lifecycle Architecture

## Purpose

Sinomo needs a way to evolve, package, evaluate, deploy, observe, and roll back
bounded pieces of behavior without treating every change as a full cluster
binary rollout. This document defines a backend-neutral module lifecycle
architecture for that purpose.

The goal is not to make WebAssembly the default extension format. The goal is
to make behavior independently buildable, inspectable, testable, routable, and
reversible. Static native Rust modules, native dynamic libraries, declarative
capsules, sandboxed processes, and future Wasm components should all fit behind
the same lifecycle model when that model is useful.

## Design Goals

- Separate module lifecycle from module execution backend.
- Support independently versioned and rolled-out behavior units.
- Reduce rebuild cost by allowing modules to become independently buildable
  artifacts when the module boundary is stable enough.
- Make self-evolution targets smaller than the whole Sinomo source tree when
  the target naturally fits a module boundary.
- Preserve Rust-native implementation paths for first-party behavior.
- Support trusted native dynamic-library modules on platforms where dynamic
  loading is a practical deployment mechanism.
- Support declarative modules for prompts, configs, tool descriptors, eval
  suites, routing rules, and policy-adjacent data.
- Keep Wasm/components optional for third-party, generated, or cross-language
  procedural extensions.
- Reuse the existing registry, policy, sandbox, rollout, event, and evaluation
  foundations instead of creating a parallel extension platform.

## Non-Goals

- Do not require Wasm for every module.
- Do not make modules a substitute for sandboxing.
- Do not treat native dynamic libraries as security isolation. They share the
  host process address space and are suitable only for trusted code.
- Do not assume a general policy engine can understand every tool's domain
  semantics.
- Do not introduce runtime extensibility for R0/R1 continuity code before the
  lifecycle and rollback model is proven on higher rings.
- Do not make module packaging a shortcut around normal evaluation, provenance,
  and rollout gates.

## Core Concept

A module is a lifecycle unit, not an ABI.

```text
module =
  stable identity
  + manifest
  + artifact or declarative payload
  + execution backend
  + compatibility contract
  + evaluation gates
  + rollout state
  + telemetry identity
  + rollback target
```

The same lifecycle can apply to multiple backend kinds:

| Backend | Use Case | Runtime Loading |
|---|---|---|
| `native-rust` | First-party trusted behavior compiled into Sinomo | Registered by Rust code at process startup. |
| `native-dylib` | Trusted first-party behavior built separately for desktop/server platforms | Loaded from signed, content-addressed shared-library artifacts through the stable `sinomo-module-api` C/SOBJ ABI. |
| `declarative` | Prompts, configs, tool descriptors, eval fixtures, routing tables | Loaded as structured data from registry/DFS/local config. |
| `sandbox-process` | CLI tools, adapters, benchmark runners, generated tool wrappers | Executed by `sinomo-sandbox` or a constrained command runner. |
| `wasm-component` | Third-party or generated procedural extensions needing stronger ABI isolation | Optional future runner, not a hard dependency. |

This makes the useful part of modularity independent from the execution
technology.

## Module Capsule

A module capsule is the packaged form of one module version.

Required fields:

- `module_id`
- `version`
- `backend`
- `interface`
- `artifact_ref` or `payload_ref`
- `content_hash`
- `source_revision`
- `compatibility`
- `evaluation_gates`
- `rollback_policy`
- `telemetry_schema`

Optional fields:

- `build_recipe`
- `resource_budget`
- `declared_capabilities`
- `domain_policy`
- `schema_refs`
- `migration_notes`
- `deprecation`

The capsule is immutable once registered. Mutable state such as active version,
candidate version, shadow version, traffic weights, and rollback status belongs
to loader or rollout state, not to the capsule.

## Manifest Shape

A future manifest can be represented as Sinomo object data and should remain
backend neutral:

```text
module_id: module.trigger.silence_policy
version: 0.3.0
backend: native-rust | native-dylib | declarative | sandbox-process | wasm-component
interface: tool | evaluator | trigger | planner | adapter | prompt-pack | policy-data
artifact_ref: registry://artifact/...
payload_ref: dfs://...
content_hash: sha256:...
source_revision: git:...
compatibility:
  min_host: ...
  max_host: ...
  interface_version: ...
  module_abi_version: ...
  platform_triple: ...
declared_capabilities:
  - dfs.read:/workspace/repo
  - network.public-vulnerability-db
evaluation_gates:
  - manifest-validation
  - replay-tests
  - private-regression-evals
rollout:
  strategy: shadow-then-canary
  rollback_on:
    - crash-rate
    - timeout-rate
    - user-correction-rate
```

The exact Rust type should live in an R1 crate only if it is generic lifecycle
infrastructure. Self-evolution-specific candidate metadata should remain in
`sinomo-evolution`.

Module manifest types should live in `sinomo-core`. Most higher-level Sinomo
subsystems are expected to declare themselves as modules over time, and they
should be able to expose module identity, interface, version, backend, and
compatibility metadata without depending on `sinomo-module`. The `sinomo-module`
crate should own lifecycle operations over those manifests, not the foundational
manifest schema itself.

Module IDs should map to storage paths through a deterministic module path key.
For dotted IDs, the path key should split validated ID segments:

```text
module.tool.agent.read-file -> module/tool/agent/read-file
module.trigger.silence-policy -> module/trigger/silence-policy
```

If future IDs allow characters that are not path-safe, the path key should use a
single canonical escaping function rather than ad hoc path construction.

## Execution Backends

### Static Native Rust

Native Rust modules are compiled into the Sinomo binary and registered at
startup.

This should be the first production backend for first-party modules because it:

- matches the current Rust architecture,
- avoids a new runtime dependency,
- keeps debugging straightforward,
- uses normal Cargo build and test workflows,
- supports module lifecycle without runtime code loading.

Native modules still get module identity, manifests, evaluation gates, routing,
shadow/canary telemetry, and rollback records. A native module rollout may still
require a normal binary rollout, but the module boundary makes evaluation,
telemetry, and attribution cleaner.

Native module registration should use generated registration tables. Crates can
declare module metadata in code or manifest-adjacent files, and the build should
produce a deterministic table consumed at startup. This avoids broad runtime
discovery, keeps registration inspectable, and gives self-evolution a concrete
artifact to update and verify when modules are added or changed.

The generated registration flow should mirror the existing service-registration
style:

```text
ModuleRegistry::builder()
  -> register(generated native module descriptors)
  -> register(runtime-provided declarative modules)
  -> build()
```

The generated table should be explicit Rust code rather than runtime inventory
magic. A build step can scan crate metadata and module declaration files, reject
duplicate module IDs, verify interface/backend compatibility, and generate a
stable table that the binary includes at compile time.

### Native Dynamic Libraries

Native dynamic-library modules are trusted native artifacts built separately
from the main `sinomo` binary and loaded at runtime as `.so`, `.dylib`, or
`.dll` files on platforms where this is supported.

This backend is useful when:

- a first-party module is large enough to materially affect build time,
- the module boundary is stable enough to build and test independently,
- the code should stay native Rust without introducing a Wasm runtime,
- the module can be rolled out as an artifact without replacing the whole host
  binary,
- the operator accepts that this is build and deployment isolation, not a
  security sandbox.

Native dynamic libraries run in the host process. A faulty module can corrupt
memory, leak resources, deadlock, or crash the daemon. They should therefore be
limited to trusted code and gated like other host-process code. Untrusted,
third-party, or generated procedural behavior should start as `sandbox-process`
or, when justified, `wasm-component`.

The dynamic boundary should not expose Rust trait objects, generic types,
`serde_json::Value`, futures, host-owned allocations, or unstable Rust ABI
details. The boundary should be a small C ABI that exchanges Sinomo object bytes
and versioned host function tables:

```text
sinomo_module_api_version() -> u32
sinomo_module_descriptor(out: *mut ByteBuffer) -> Status
sinomo_module_init(host: HostApiV1) -> Status
sinomo_module_invoke(request_sobj: ByteSlice, response_sobj: *mut ByteBuffer) -> Status
sinomo_module_shutdown() -> Status
sinomo_module_free_buffer(buffer: ByteBuffer)
```

The request and response envelopes should be SOBJ-encoded typed module
invocation records. The descriptor should include module ID, version, backend,
interface, ABI version, platform triple, required host capabilities, declared
resource expectations, and content hash.

The host side should:

- load only signed artifacts whose content hash matches the capsule,
- verify host version, module ABI version, interface version, and platform
  triple before loading,
- load artifacts from content-addressed paths so an active library file is never
  overwritten in place,
- catch unwinds at the FFI boundary where possible and convert them into module
  telemetry,
- treat unloading as best-effort and not as a correctness requirement,
- rely on shadow/main process restart to reclaim old libraries after version
  replacement,
- run replay and health checks in shadow before canary traffic reaches a loaded
  native dynamic module.

This backend implies a separate build artifact flow. A module crate can produce
a `cdylib` plus a manifest/capsule, while the main binary depends only on the
stable module API and host loader. The workspace should avoid compiling every
dynamic module during normal host builds; dynamic modules can be outside default
workspace members or built by an explicit command such as:

```text
sinomo module build <module-id>
sinomo module register <module-artifact>
```

The initial implementation should support native dynamic libraries only on
desktop/server platforms where dynamic loading and operator debugging are
straightforward. Other platforms can reject `native-dylib` modules during
compatibility checks and continue using static native, declarative,
sandbox-process, or optional Wasm backends.

Native dynamic module crates live under the top-level `modules/` tree rather
than under `crates/`. The main Cargo workspace may include them as members so
they can share dependency and lint configuration, but they must stay outside
`default-members`. A normal `cargo build`, `cargo check`, or `cargo build -p
sinomo` should build the host, generic module API, and host loader only. A
specific native module is built explicitly by package name or module tooling.

The first native dynamic module is the WASIX sandbox execution engine at:

```text
modules/sandbox/wasix/
```

The main binary keeps `sinomo-sandbox` contracts, policy, stores, command
records, SIFS/local filesystem logic, and loader state. The module owns the
build-heavy Wasmtime/Wasmer/WASIX execution machinery and exports the generic
native module ABI with a WASIX-specific SOBJ payload interface.

### Declarative

Declarative modules have no executable code. They are structured data consumed
by a trusted host service.

Examples:

- prompts,
- tool descriptors,
- model routing policies,
- eval fixtures,
- benchmark suites,
- workflow templates,
- trigger rules,
- UI or CLI configuration.

Declarative modules are important for self-evolution because many R3+ changes
are not code changes. They should use schema validation, content hashes,
provenance, replay where applicable, and rollout state just like procedural
modules.

Declarative module state should load from both local configuration and cluster
state stored in DFS. Local configuration is the pre-cluster and development
source. Once a node joins a cluster, cluster configuration is authoritative and
must override local configuration for shared module state. Local files may still
provide node-private defaults or bootstrap values, but they must not shadow
cluster-owned module desired state.

Suggested local paths:

```text
$SINOMO_DATA_DIR/config/modules/catalog.toml
$SINOMO_DATA_DIR/config/modules/<module-path-key>/module.toml
$SINOMO_DATA_DIR/config/modules/<module-path-key>/payload/
$SINOMO_DATA_DIR/module/loader-state/<module-id>.sobj
$SINOMO_DATA_DIR/module/loader-state/native-dylib/<module-id>.sobj
$SINOMO_DATA_DIR/module/cache/cluster-modules/<module-path-key>/
$SINOMO_DATA_DIR/module/cache/native-dylib/<module-path-key>/<content-hash>/
```

The `config/modules` tree is human-editable local configuration. The
`module/loader-state` tree is node-local applied runtime state. Native-dylib
loader records point at content-addressed cache entries and carry the artifact
hash, module cache key, library filename, platform metadata, native ABI version,
and interface version. The cache path is optional and should only mirror
cluster-owned module content; it must not become authoritative when the node is
clustered.

### Sandbox Process

Sandbox-process modules are procedural modules executed by the sandbox or by a
restricted command runner.

Examples:

- dependency scanners,
- code generators,
- benchmark harnesses,
- adapters around existing command-line tools,
- generated experimental tools.

The host sandbox enforces coarse containment: filesystem roots, network class,
secrets, process limits, timeouts, and output capture. The module itself remains
responsible for domain semantics. For example, a dependency scanner can validate
that an input path is a code repository and that network requests are to known
vulnerability database endpoints.

This split is intentional:

- host containment prevents broad ambient authority,
- module logic enforces domain-specific correctness,
- evaluation and rollout decide whether the behavior is acceptable.

### Wasm Component

Wasm components are an optional future backend for cases where the extra layer
earns its cost:

- third-party procedural extensions,
- generated tools from self-evolution,
- cross-language extension authorship,
- stronger crash and resource isolation than native Rust,
- ABI stability across host upgrades,
- portable dynamic loading without rebuilding the main binary.

Wasm should not be the universal module format. It should be behind an optional
runner and should use specific typed interfaces when introduced. A generic
`run(input_json) -> output_json` boundary is useful for early experiments, but
stable tools and evaluators should eventually use narrower interface contracts.

## Runtime Assembly

The runtime module registry should be assembled from multiple sources in a
deterministic order:

```text
generated static native registry
  + local declarative module config
  + cluster desired module state
  + trusted native dynamic-library artifacts
  + sandbox-process module packages
  + optional Wasm component artifacts
  -> ModuleRegistry
```

The generated static registry gives the host a known baseline. Local
declarative config supports development and pre-cluster operation. When the node
is clustered, cluster desired state is authoritative for shared modules and can
select dynamic, sandbox-process, or Wasm artifacts by content hash.

Runtime assembly should never search arbitrary directories for executable
modules. All executable artifacts should be selected by manifest, content hash,
signature, backend compatibility, and rollout/loader state. This prevents module
loading from becoming ambient plugin discovery.

Static native modules are available immediately at process startup. Dynamic
native modules and sandbox/Wasm artifacts are materialized lazily or during
loader reconciliation from local cache, registry, or cluster-owned storage. The
loader records the applied version and lane locally so routing remains stable
between reconciliation passes.

## Lifecycle

The module lifecycle is shared across backends.

```text
draft
  -> built
  -> registered
  -> evaluated
  -> staged
  -> shadow
  -> canary
  -> active
  -> deprecated
  -> retired
```

Failure paths:

```text
built -> rejected
registered -> quarantined
evaluated -> failed
shadow -> rolled-back
canary -> rolled-back
active -> reverted
```

Lifecycle state should be explicit and evented. The active module state for a
node should be small and easy to inspect:

- active artifact or payload,
- previous artifact or payload,
- shadow artifact or payload,
- candidate artifact or payload,
- traffic weight,
- last rollout action,
- last transition reason,
- applied time,
- transition event ID.

Rollout is a cluster concept. A standalone local node can load local or
declarative module state, but it is not performing rollout because there is no
cluster authority, node set, staged distribution, or cluster-wide desired state
to coordinate. When a node is in a cluster, cluster module configuration and
rollout state override local configuration.

Suggested cluster DFS paths:

```text
dfs://cluster/modules/catalog.sobj
dfs://cluster/modules/<module-path-key>/versions/<version>/manifest.sobj
dfs://cluster/modules/<module-path-key>/versions/<version>/payload/
dfs://cluster/modules/<module-path-key>/versions/<version>/evidence/
dfs://cluster/modules/<module-path-key>/desired.sobj
```

Cluster DFS owns module declarations and declarative payloads. Cluster rollout
state should stay with the rollout subsystem, for example under a
`cluster/rollout/...` namespace, and may point at module versions in
`cluster/modules/...`. This keeps module content separate from rollout
coordination.

## Routing

Routing is lifecycle infrastructure and should not depend on the backend.

The loader should route a request by:

1. Looking up node-local module state.
2. Selecting active, candidate, or shadow lane.
3. Using stable route keys for canary decisions.
4. Invoking the backend runner.
5. Recording module telemetry.

The route decision should be deterministic for a given module, route key, and
traffic weight. This lets canaries remain stable across repeated calls and makes
replay meaningful.

## Evaluation

Module evaluation should be backend neutral.

Common gates:

- manifest validation,
- schema compatibility,
- build validation,
- unit and integration tests,
- replay tests,
- private regression evals,
- shadow comparison,
- canary analysis,
- resource usage checks,
- rollback readiness.

Backend-specific gates can be added when needed:

- static native Rust: crate tests, feature compatibility, binary rollout
  readiness,
- native dynamic library: ABI compatibility, platform compatibility, symbol
  validation, signature verification, host-process crash telemetry, shadow
  daemon health checks, and unload/restart behavior,
- declarative: schema validation, semantic linting, dry-run replay,
- sandbox process: sandbox policy validation, command determinism, network and
  filesystem access checks,
- Wasm component: component validation, interface compatibility, fuel/memory
  limits. For WASIX-backed Sinomo module artifacts, component validation is a
  `ValidateComponent` request handled by the active `module.sandbox.wasix`
  native-dylib module, not by the shipped CLI binary.

Evaluation outputs should feed `sinomo-eval` scorecards and `module.*`
telemetry events.

## Policy Boundary

Policy is layered, but not omniscient.

Host-level policy can decide coarse authority:

- which module backend is allowed,
- whether `native-dylib` is allowed on this node and platform,
- which filesystem roots are visible,
- which network class is available,
- which secrets or devices are available,
- maximum runtime and resource budget,
- whether a capability increase needs review.

Module-level logic must enforce domain semantics:

- whether an input is a valid code repository,
- whether a URL is a real vulnerability database endpoint,
- whether generated output is safe for its domain,
- whether a prompt/config is internally consistent.

Rollout policy then decides promotion using evidence:

- scorecards,
- telemetry,
- replay results,
- shadow/canary metrics,
- user corrections,
- rollback readiness.

This keeps policy useful without pretending a generic engine can understand all
tool-specific behavior.

## Build and Registry Flow

The build path depends on backend, but the artifact flow should converge:

```text
source or declarative payload
  -> build or package
  -> content hash
  -> manifest validation
  -> artifact registry
  -> evaluation
  -> rollout material
  -> node-local loader state
```

Static native Rust modules may share the Sinomo binary artifact. In that case,
the module capsule points to the binary artifact plus a module registration
record inside that build.

Native dynamic-library modules point to signed shared-library artifacts plus a
module descriptor record. The artifact record must include content hash,
platform triple, module ABI version, interface version, build profile, source
revision, and signer provenance. The loader should copy or materialize the
artifact under a content-addressed path before loading it, rather than loading
directly from mutable staging paths.

Declarative modules point to signed/configured structured data.

Sandbox-process modules point to a command package, script, toolchain package,
or wrapper artifact.

Wasm components point to component artifacts only when that backend is enabled.

For non-cluster local operation, the flow can stop at local build/package,
local validation, and local activation. For clustered operation, artifact
registration and rollout state are cluster-owned. Nodes apply cluster desired
state locally, but they should not invent their own rollout state when cluster
state exists.

## Relation To Self-Evolution

Self-evolution should target the narrowest natural unit:

- source patch when the change affects trusted Rust infrastructure,
- declarative capsule when the change is prompt/config/eval/tool metadata,
- static native module when the change is first-party modular behavior that
  should remain compiled into the host,
- native dynamic-library module when the change is trusted first-party behavior
  whose build and rollout boundary is stable enough to justify a separate
  native artifact,
- sandbox-process module when the change is an external or generated tool,
- Wasm component only when dynamic procedural extension isolation is worth the
  runtime cost.

The evolution planner should not assume R3 means Wasm. R3 means lower blast
radius and stronger containment. The backend is a separate decision.

For generated tools, the expected progression is:

1. generated declarative tool descriptor,
2. sandbox-process prototype,
3. replay and private regression evidence,
4. optional native dynamic-library packaging only after the code is trusted and
   first-party enough to share the host process,
5. optional Wasm component packaging if the tool needs long-lived dynamic
   loading, third-party distribution, or stronger ABI/resource isolation.

The planner should treat native dynamic libraries as a way to reduce build and
deployment blast radius for trusted native code. It should not use them as the
default containment answer for generated code.

## Crate Responsibilities

### `sinomo-module`

Owns backend-neutral module lifecycle primitives:

- module IDs and artifact IDs,
- module capsule/manifest types,
- loader runtime state,
- route decisions,
- active/shadow/candidate/canary lanes,
- replay fixtures and reports,
- module telemetry event helpers,
- backend runner trait,
- local persistence paths for loader state.

It should not require Wasm.

It should consume module manifest types from `sinomo-core`, not define the base
manifest schema itself. This keeps module declaration available to crates that
must not depend on `sinomo-module`.

It should expose a `ModuleRegistry`/`ModuleRegistryBuilder` shape similar to
`ServiceRegistry`, rejecting duplicate module IDs and preserving deterministic
registration order.

### `sinomo-module-api`

Owns the minimal stable ABI shared between native dynamic-library modules and
the host:

- ABI version constants,
- C-compatible byte buffer and status types,
- SOBJ request and response envelope schemas,
- module descriptor schema,
- host function table versions.

This crate must remain small and conservative. It should not depend on the
dynamic loader, Wasm runtime, sandbox runtime, or high-level services.

The generic native ABI uses a small C surface with SOBJ request/response
envelopes. Backend-specific protocols, such as `sinomo.sandbox.wasix.v1`, ride
inside the generic invocation payload instead of defining their own dynamic
library ABI.

### `sinomo-module-host`

Owns native dynamic-library loading as an optional host backend:

- content-addressed library materialization,
- platform and ABI compatibility checks,
- symbol lookup,
- host function table construction,
- invocation through the stable C/SOBJ boundary,
- crash/error telemetry conversion,
- best-effort shutdown and restart-oriented replacement semantics.

If this remains inside `sinomo-module` initially, it should still be feature
gated and structured as a separable backend so non-desktop builds and minimal
deployments do not inherit dynamic-loading requirements.

### `sinomo-registry`

Owns immutable artifact and capsule records:

- content hashes,
- signatures,
- provenance,
- artifact evidence,
- release material,
- signer trust records.

### `sinomo-rollout`

Owns rollout transitions:

- staged,
- shadow,
- canary,
- promoted,
- rolled back,
- reverted.

Rollout should write node-local desired state; the loader applies it.

Rollout operates only in clustered mode. Local-only mode may activate local
modules for development and bootstrap, but that is local activation rather than
rollout.

### `sinomo-sandbox`

Owns sandboxed process execution and optional Wasm execution backends:

- filesystem and network containment,
- command/toolchain packages,
- stdout/stderr capture,
- runtime limits,
- optional Wasm component runner.

### `sinomo-evolution`

Owns candidate production and evidence:

- mutation jobs,
- candidate bundles,
- generated capsules,
- benchmark and replay evidence,
- adaptation records,
- promotion requests.

It consumes module lifecycle APIs but should not define R1 module primitives.

## Migration From Current Implementation

The current `sinomo-module` crate already contains useful pieces:

- module identity aliases,
- node-local loader runtime state,
- route decisions,
- shadow/canary telemetry reports,
- replay fixtures and reports,
- artifact signature checks,
- a backend runner trait,
- local command runner support.

The concept should be refactored in place toward backend neutrality:

1. Introduce backend-neutral module manifest types in `sinomo-core`.
2. Add generated native module registration tables.
3. Define a minimal `sinomo-module-api` ABI for native dynamic libraries.
4. Add an optional native dynamic-library host backend that loads signed
   content-addressed artifacts through that ABI.
5. Define declarative module loading from local config plus cluster DFS state,
   with cluster state taking precedence when present.
6. Rename or generalize Wasm-specific constants so they do not define the main
   module concept.
7. Keep `ModuleArtifactRunner` as the backend runner abstraction.
8. Move Wasm-specific execution behind `sinomo-sandbox` or an optional backend
   crate/feature. WASIX execution is the first implementation and lives in
   `modules/sandbox/wasix`.
9. Add static native, native dynamic-library, and declarative backend records
   before expanding Wasm.
10. Update self-evolution docs so R3 modules are not described as Wasm-first.
11. Keep current replay, routing, and telemetry tests as lifecycle regression
   coverage.

Current WASIX split:

- `sinomo-sandbox` keeps contracts, policy, stores, file-system sync helpers,
  command records, and output/event types.
- `sinomo-sandbox-wasix-host` owns host-side loading plus local/SIFS staging and
  reconcile. It loads `module.sandbox.wasix` from
  `$SINOMO_DATA_DIR/module/cache/native-dylib/module/sandbox/wasix/<hash>/...`
  using the node-local native-dylib loader-state record.
- `modules/sandbox/wasix` owns the WASIX/Wasmtime/Wasmer execution engine and
  exposes only the C ABI plus SOBJ envelopes.
- Local activation is explicit:
  `sinomo module wasix activate <libsinomo_sandbox_wasix_module.so>`. The
  generic path is
  `sinomo module native-dylib activate [--module <module-id>] [--interface <version>] <library>`.
  Both commands validate the dylib descriptor, copy the library into the
  content-addressed native-dylib cache, and write
  `$SINOMO_DATA_DIR/module/loader-state/native-dylib/<module-id>.sobj`.
- The shipped `sinomo` binary does not depend on `sinomo-sandbox-wasix-module`,
  Wasmtime, or Wasmer; sandbox execution returns a normal runtime-unavailable
  error when no active WASIX native module is installed.

## Open Questions

- What is the smallest first real module boundary: a tool descriptor, an eval
  suite, a trigger, or an agent prompt pack?
- Should the first tool module be one of the existing core agent tools
  (`read_file`, `search`, or `apply_patch`) or a simpler diagnostic tool?
- Should generated native registration tables be produced by crate build scripts,
  a workspace xtask, or a dedicated Sinomo generator command?
