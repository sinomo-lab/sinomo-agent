# Extension Points

Sinomo favors typed extension seams over parallel frameworks. This guide maps
common contributions to their current owner and registration path. Inspect the
representative implementation with CodeGraph before copying it; signatures can
evolve during the pre-1.0 period.

## Quick Map

| Change | Primary seam | Representative area |
|---|---|---|
| CLI command | Module-owned `CliCommandHandler`; runtime `sinomo.cli.commands` contributions | `sinomo-cli-registry`, `sinomo-module-api::cli`, external component fixture |
| Object-RPC service | `Service` and `ServiceRoute` mounted by `ComponentFactory` | owning domain crate plus component scope |
| Agent tool | `AgentTool`, `AgentToolRegistry` | `sinomo-agent` |
| Model provider | `LanguageModelProvider` | `sinomo-lm` and provider wiring |
| Native module | generic component descriptor and `sinomo-module-api` | independently built external module fixture and packages |
| Existing behavior or semantic slot | `ExtensionPointDefinition`, `ExtensionContribution`, compiled registry, owner executor | `sinomo-module-api`, `sinomo-module` |
| Application UI | `UiExtensionContribution`, semantic slots, isolated `sinomo.ui.widget` components | `sinomo-module-api`, `sinomo-ui-extension-host`, `sinomo-app` |
| Network transport | `NetworkModule` and registry/ABI | `sinomo-network`, `modules/network/iroh` |
| DFS backend or service | Core backend/transport traits and owner-checked semantic service; optional history/attachment components | `sinomo-dfs`, `sinomo-dfs-history`, `sinomo-dfs-attachments` |
| Sandbox backend | `SandboxBackend`, `SandboxBackendFactory` | `sinomo-sandbox`, WASIX module |
| Configuration | `ConfigDefinition`, `ConfigRegistryBuilder` | `sinomo-config` and owner |
| Persistent storage | `SinomoDataDir`, `StorageLayout` registration | `sinomo-local`, `sinomo-storage` |
| Runtime test | `sinomo_test_case!` and test registry | `sinomo-test`, owning crate |
| Event family | typed dotted event name and metadata contract | `sinomo-events`, owning domain |

Modules declare their user-facing name with
`ComponentDescriptor::with_display_name`. Linked Rust registrations can use
`ModuleRegistration::with_display_name`; native and declarative packages carry
the name in their component descriptor. Keep this metadata with the module owner.
The module-observation API projects these names for local and remote settings,
including installed modules that are inactive. Clients render the supplied name
and keep the module ID as technical detail. Older descriptors may omit the name;
clients must not reconstruct it from an ID or library filename.

## CLI Commands

The executable in `crates/sinomo` only installs build information and calls the
CLI library. The domain owner supplies its parser, help, completion, and runner.
For linked commands, expose a registration function using `CliCommandHandler`
or `DeclarativeCommandHandler` from `sinomo-cli-registry`. R2 owners import these
through `sinomo-runtime` with `default-features = false, features = ["cli-client"]`.
Each owner exposes `cli::register_cli_commands`, accepting execution callbacks
from the host. The owner controls typed arguments, validation, command policy,
help, completion, and aliases. The CLI supplies terminal, transport, and process
adapters; it does not maintain a second copy of the domain grammar.

| Commands | Parser and registration owner |
|---|---|
| `agent`, `skill` | `sinomo-agent::cli` |
| `models`, `providers`, `usage`, `voice` | `sinomo-lm::cli` (optional `cli` feature) |
| `memory` | `sinomo-memory::cli` |
| `dfs` | Core grammar in `sinomo-dfs::cli`; `history` and `attachment` children in their owning feature crates, composed by the CLI host. |
| `rollout`, including recovery and build-log options | `sinomo-rollout-host::cli` |
| `test` | `sinomo-test::cli` |

Realtime call syntax belongs with the LM realtime contracts, while media
devices and sign-in continue to execute locally. The `test` grammar uses the
shared provider data contract rather than depending on the LM implementation.
Bootstrap/recovery commands (`help`, `daemon`, `cluster`, `install`,
`uninstall`, and `debug`) remain CLI-owned and available without dynamic
modules. R0 commands keep their CLI adapters above R0.

Parser ownership is separate from discovery: these linked modules register
locally, and external modules use the daemon catalog below. Moving a grammar
does not turn local execution into a daemon call. No extra crates are needed.

A runtime module contributes a versioned `CliCommandDescriptor` to
`sinomo.cli.commands/1.0.0` and offers `sinomo.cli.command/1.0.0` with a
component-only `cli.invoke` route. Export the descriptor from the owner with
`export_module_command`; declare the operation in both `supported_operations`
and `surfaces.capability_exports`. The native module handles `CliModuleRequest`
and returns `CliModuleResponse`. Its `Help` and `Complete` actions must have no
effects, and `Invoke` parses before performing domain operations. Set
`help_on_empty` to the owner's actual behavior. See
`fixtures/external-component/src/lib.rs` and the Evolution module for examples.

The CLI discovers `/module/cli/catalog` for help, completion, and unresolved
command namespaces. It forwards the argument vector unchanged to
`/module/cli/invoke`; it does not join a shell string or parse module options.
Registration rejects namespace/alias conflicts, reserved recovery commands,
invalid completion metadata, and unowned invocation services. Catalog bindings
include a service incarnation and runtime generation; stale invocations fail
before execution and are not automatically retried. The host resolves exactly
one declared provider, checks local consent, pins the native artifact, and holds
invocation leases through native completion. Unload withdraws contributions
and drains existing invocations. A later discovery sees the new catalog.

Version 1 uses bounded, buffered I/O: at most 256 KiB of UTF-8 arguments,
1 MiB of declared stdin, and 4 MiB of combined output/completions. It preserves
separate stdout/stderr and exit codes (0–255); Ctrl-C uses operation cancellation
and returns 130. Piped stdin is passed explicitly through `CliCommandContext`.
Live interactive terminal sessions and incremental output need a future
streaming contract; modules must not redirect the daemon's process-wide stdio.

A command should parse user intent, call a domain/runtime contract, render
progress or structured output, and translate errors. It should not directly
mutate Raft files, DFS metadata, module activation state, or daemon-owned
process state.

Keep command help and parser tests together. If a command changes a supported
workflow, update the corresponding user guide in the same patch.

## Object-RPC Services

Implement the `Service` trait in the domain that owns the request, importing it
through the facade appropriate to that crate's trust ring; the trait is defined
in `sinomo-service`. Declare each `ServiceRoute` with its authentication,
capability, local-permission, body, timeout, and concurrency metadata. Register
services through the owning crate, then mount their registry fragment through a
`ComponentFactory`.

The daemon provides process lifecycle, authentication context, routing, and
host adapters. It should not reinterpret the domain state machine. A remote
route must perform final authorization where the effect occurs, even if an
upstream caller already checked policy.

Use stable operation identity for retryable effects and return evidence that
lets a caller distinguish committed, rejected, unknown, and timed-out outcomes.

## Agent Tools

Implement `AgentTool::descriptor` and `AgentTool::execute`, then register the
tool with `AgentToolRegistry`. The descriptor is user/model-visible contract;
execution is still subject to runtime policy and workspace boundaries.

A tool should:

- validate structured input independently from the model;
- retain cancellation and operation identity;
- return attributable evidence, not only display text;
- avoid ambient filesystem, network, process, device, or secret access;
- keep large outputs in durable evidence and provide a bounded model-facing
  projection.

Prompts and skills can request a tool but cannot grant it more authority.

## Language-Model Providers

Provider integrations implement the provider-neutral language-model contract,
then wire provider ID selection, account and model options, credentials, CLI
authentication, and host-access classification.

Provider integration is currently a reviewed multi-file path, not an arbitrary
dynamic-provider plugin registry. Preserve the boundary: providers translate
requests and responses; the agent runtime owns sessions, tools, cancellation,
compaction, and durable evidence.

Tests should cover protocol parsing and deterministic request construction
without live credentials. Live smoke tests must be opt-in because they contact
external services and may consume quota.

## Native Modules

A native dynamic module lives under `modules/<family>/<name>`, builds as an
`rlib` and `cdylib`, declares its trust ring, and supplies
`[package.metadata.sinomo.native-dylib]` with module identity, interface
version, cache key, and source.

Use the single generic `sinomo-module-api` ABI and declare versioned service
offers and requirements in `ComponentDescriptor`. Provider-specific protocols
remain private to the external package; the runtime must not depend on a
provider API crate. Native modules are leaf artifacts and stay outside default
builds. Activation is a
module/rollout decision; loading a library from an arbitrary host path must not
become a second production activation path.

Host-side consumers resolve providers through
`sinomo_module::ActiveNativeComponent` and keep their typed client in the
owning domain crate. Do not introduce a new `<interface>-host` crate, duplicate
loader-state scanning, or add a domain adapter to the `sinomo-runtime` facade.

Representative packages include Iroh networking, remote management, Windows
Cloud Files, and WASIX. The latter two implement the generic sandbox-executor
and host-filesystem-provider interfaces. Cloud Files owns its registration and
crash-recovery compensation inside the external package; read its
[architecture](../cloud-files-architecture.md) and
[module maintainer guide](../../modules/dfs/cloud-files/README.md) before
changing its ABI, durable state, lifecycle callbacks, or provider selection.

## Generic Behavior And Presentation Extensions

Use the generic extension registry when several modules participate in a join
point owned by an existing subsystem. The owner declares an
`ExtensionPointDefinition`; contributors add `ExtensionContribution` records
to either `ModuleSurfaces` or `NativeModuleSurfaces`. Both new fields default to
empty when older JSON or SOBJ surfaces are decoded.

Definitions own the versioned input, contribution, and output schema IDs,
locality, accepted contribution kinds, composition and failure policies,
resource limits, capability boundary, and eligible context keys. Contributions
carry bounded declarative SOBJ data, a reference to a logical operation in one
of the contributor's typed interface exports, or both. Do not name a raw route,
host function, callback, Rust type, or authority token.

`compile_default_extension_registry` normalizes legacy UI records into generic
contributions and compiles all active definitions and contributions into an
immutable `ExtensionRegistrySnapshot`. Its generation is the module-registry
generation from which it was built. The compiler resolves compatible point
versions and typed operation contracts, orders contributions deterministically,
and retains missing points, version mismatches, rejected capabilities, and
ordering cycles as catalog diagnostics. Inspect the local
`/module/extensions/catalog` route when debugging a pending contribution.

An owning component obtains `ExtensionExecutor` from its `ComponentContext`.
The executor uses the matching compiled extension snapshot and the live checked
service registry. Choose the narrowest composition policy:

- `many-ordered` returns deterministic catalog content without invoking it;
- `collect` invokes independent providers and preserves attributed partial
  success;
- `select-one` requires an `ExtensionSelection` naming the point version and
  contribution; install or activation order is never selection;
- `reduce` invokes providers, then passes each attributed value to a reducer
  implemented by the trusted point owner;
- `pipeline` accepts typed deltas only, with owner validation after every step;
- `veto` is valid only with `fail-closed`; denial, malformed output, timeout,
  recursion failure, and provider disappearance all constrain the result.

The first host behavior point is
`sinomo.module.diagnostics.enrich` version `1.0.0`, a bounded `collect` point.
Use `ModuleDiagnosticsInput` and `ModuleDiagnosticContribution`, and keep the
authoritative diagnostic decision in the owning subsystem. Reducers, pipeline
state application, and final side effects are deliberately not contributor
callbacks.

### Hot unload

Use `sinomo module native-dylib unload <module-id>` for an active dynamic
module. The CLI sends an authenticated `ModuleHotUnloadRequest` to the running
daemon; it never deletes loader state directly. The daemon withdraws service,
extension, and UI-catalog entries, cancels native operations, drains invocation
leases, retires component scopes, and then attempts physical library shutdown.

`ModuleHotUnloadReport` distinguishes logical withdrawal, invocation drain,
component teardown, physical unload, cascading dependent modules, and whether
a restart is required. A timeout is intentionally fail-safe: routes stay
withdrawn, the old scope remains quarantined, and the report is not marked as a
completed physical unload.

Native-dylib conformance requires more than exporting a shutdown symbol:

- use one static `NativeModuleLifecycle` around `init`, `invoke`, and
  `shutdown`;
- do not clear host callback/user-data state while an invocation guard exists;
- make shutdown idempotent and allow a later load to initialize again;
- cancel and join per-invocation workers, child processes, and callback bridges
  before returning;
- for network vtables, make `stop` idempotent and make `destroy` synchronously
  drain the private runtime; open streams must retain the executable token;
- return a non-OK shutdown status when teardown is incomplete. The host then
  quarantines the mapping and reports `restart_required`.

The opt-in `native_dylib_hot_unload` integration test accepts
`SINOMO_NATIVE_HOT_UNLOAD_FIXTURES` as semicolon-separated
`module-id=/absolute/library/path` entries. It loads, unloads, and reloads every
fixture twice in one process; the Iroh fixture also creates and unregisters its
real network-vtable instance twice.

## Application UI

Use declarative contributions for ordinary actions, panels, settings, status
items, and host-native views. When a surface must draw content that stock
widgets cannot express, contribute a `custom-widget` payload referencing a
Wasm component stored inside the declarative module package. The component must
export `sinomo:ui-widget/widget.handle` with the contract in
`crates/sinomo-module-api/wit/sinomo-ui-widget-v1.wit`.

The UI catalog is a compatibility projection of the same generic extension
registry generation; it is not a second plugin registry. Existing
`ui_contributions` continue to decode and are normalized under
`sinomo.ui.contribution/v1`. New generic behavior should use
`extension_points` and `extension_contributions` directly.

The component receives and returns SOBJ `UiWidgetRequest` and
`UiWidgetResponse` envelopes. It returns complete `UiWidgetFrame` values with
paint commands and semantic nodes; it never receives a SUI or wgpu object.
Frames are bounded and clipped by the app. File preview components target
`sinomo.app.files.preview-provider` version `1.0.0` and declare extension,
media-type, or magic-prefix selectors. Competing previewers use deterministic
catalog order but version 1 opens a custom surface only when exactly one active
provider matches. Ambiguity preserves the built-in preview until a later user
selection surface records an explicit preference; activation order is not
precedence.

A declarative package manifest uses the following surface shape (the digest is
the BLAKE3 hash of the exact component file):

```json
{
  "surfaces": {
    "ui_contributions": [{
      "contribution_id": "module.example.preview.widget",
      "extension_point_id": "sinomo.app.files.preview-provider",
      "extension_point_version": "1.0.0",
      "payload": {
        "type": "custom-widget",
        "widget": {
          "label": "Example format preview",
          "component": {
            "artifact_id": "artifact.example.preview",
            "content_hash": "blake3:<64 lowercase hex digits>",
            "resource_path": "ui/preview.wasm",
            "runtime": "wasm-component",
            "protocol_id": "sinomo.ui.widget",
            "protocol_version": "1.0.0",
            "entrypoint": "sinomo:ui-widget/widget"
          },
          "selectors": [{ "file_extensions": [".example"] }]
        }
      }
    }]
  }
}
```

Version 1 is read-only. A selected preview receives an opaque content grant and
up to an 8 MiB prefix. Bounded `ReadContent` host requests can page within the
granted bytes; requests beyond that prefix and mutation actions fail closed.
The executable custom-surface backend is desktop-only in version 1; Android
keeps the built-in preview until a platform isolated-service host implements
the same protocol.
The module package installer verifies
the component path and BLAKE3 digest, the daemon re-verifies it while composing
the local UI catalog, and the isolated helper verifies it again before Wasm
compilation.

Build the app with both sibling helpers:

```sh
cargo build -p sinomo -p sinomo-ui-extension-host -p sinomo-app
```

Do not expose a `WidgetPod`, Rust trait object, native widget callback, raw
route, filesystem path, or renderer handle from a module. Native and webview
renderers may be added later only behind the same artifact, lifecycle, content
grant, frame, and authority contracts.

## Network Transports

Implement the `NetworkModule`/ABI surface and register it with the network
module registry. The transport owns dialing, listening, channel capabilities,
and mechanism-specific errors. It does not read cluster KV, decide membership,
or grant trust.

Endpoint selection and route planning remain in `sinomo-network`; committed
cluster facts enter through the `sinomo-cluster` bridge. A new transport must
fit the canonical object-RPC and stream path rather than introducing a parallel
domain protocol.

## DFS and VFS

DFS metadata implementations stay behind the metadata backend traits, content
transport behind DFS transport contracts, and host filesystem exposure behind
VFS/SIFS adapters. Cross-node behavior should be integration-tested through
the real service/RPC path.

Do not store high-cardinality namespace or content in cluster KV. KV stores
compact placement and authority facts; DFS owns object versions, intents,
receipts, shard journals, and immutable content.

## Sandbox Backends

Implement `SandboxBackend` and `SandboxBackendFactory`. Preserve the VFS
boundary, pinned toolchain and process-plan contracts, bounded resources,
explicit network policy, and evidence capture.

The lightweight/simple backend is useful for contract tests; production WASIX
behavior belongs in the opt-in native module. Verify the actual backend and
toolchain artifacts, not only the marker feature on `sinomo-sandbox`.

## Configuration and Persistent Paths

Add validated configuration through `ConfigDefinition` and
`ConfigRegistryBuilder`. Put distributed settings in cluster KV and retain
secrets, host permissions, and device exposure as node-local state.

The Settings app groups values by App, Node, and Cluster ownership. Node pages
share an explicit target selected from cluster membership, and configuration
requests use that node's main daemon through Sinomo object RPC. Ordinary
configuration keeps its cluster administrative authorization and does not require
remote-management permission. Owners must mark configuration that controls host
effects with `ConfigDefinition::with_remote_host_management()`; its remote writes
also require the target node's local consent. Permission documents themselves
remain outside the remotely writable configuration catalog.
Older external-module definitions without this classification retain their
remote-management requirement until rebuilt with an explicit policy.

A native module may declare settings through
`NativeModuleDescriptor.surfaces.settings`. Each `ModuleSettingsExport`
contains the SOBJ-encoded `ConfigDefinition` and must name the descriptor's
own canonical module ID. The daemon composes those definitions into the same
configuration catalog used by built-in settings, and the app's generic module
settings editor renders the declared schema recursively. Do not add a second
module-specific settings store or UI write path. An exact duplicate of a host
fallback definition is idempotent; a different definition reusing a static
key does not replace the static catalog entry.

Resolve persistent paths through `SinomoDataDir`. Register exact files or
reasoned escape subtrees in `StorageLayout`, including owner, ring, encoding,
and schema where applicable. A new unregistered file is a diagnostics warning
and a sign that ownership is incomplete.

## Runtime Tests and Events

Register a runtime test with `sinomo_test_case!` through the owning crate or
runtime configuration. Mark external-provider and cluster requirements so the
test stays non-live by default.

New cross-service event families use typed dotted names. Define their meaning
and include the provenance, evidence, causality, privacy, latency, retention,
and routing fields required to replay or audit the transition. Do not create a
separate “remote event” type; distribution is metadata on the same contract.
