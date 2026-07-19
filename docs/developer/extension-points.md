# Extension Points

Sinomo favors typed extension seams over parallel frameworks. This guide maps
common contributions to their current owner and registration path. Inspect the
representative implementation with CodeGraph before copying it; signatures can
evolve during the pre-1.0 period.

## Quick Map

| Change | Primary seam | Representative area |
|---|---|---|
| CLI command | `CliCommandHandler`, `CliCommand`, registry composition | `sinomo-cli-registry`, `sinomo-cli` |
| Object-RPC service | `Service` and `ServiceRoute` through the owner's ring facade | owning domain crate plus daemon service hook |
| Agent tool | `AgentTool`, `AgentToolRegistry` | `sinomo-agent` |
| Model provider | `LanguageModelProvider` | `sinomo-lm` and provider wiring |
| Native module | native-dylib package metadata and `sinomo-module-api` | `modules/network/iroh`, `modules/remote/management` |
| Network transport | `NetworkModule` and registry/ABI | `sinomo-network`, `modules/network/iroh` |
| DFS backend or service | DFS backend/transport traits and service registration | `sinomo-dfs` |
| Sandbox backend | `SandboxBackend`, `SandboxBackendFactory` | `sinomo-sandbox`, WASIX module |
| Configuration | `ConfigDefinition`, `ConfigRegistryBuilder` | `sinomo-config` and owner |
| Persistent storage | `SinomoDataDir`, `StorageLayout` registration | `sinomo-local`, `sinomo-storage` |
| Runtime test | `sinomo_test_case!` and test registry | `sinomo-test`, owning crate |
| Event family | typed dotted event name and metadata contract | `sinomo-events`, owning domain |

## CLI Commands

The executable in `crates/sinomo` only installs build information and calls the
CLI library. Add behavior through the CLI registry and an owning command
handler, then compose it through the runtime configuration or a native module
binding.

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
services through the owning crate, then expose them through a daemon service
hook.

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

Use `sinomo-module-api` and the relevant narrow extension API. Native modules
are leaf artifacts and stay outside default workspace members. Activation is a
module/rollout decision; loading a library from an arbitrary host path must not
become a second production activation path.

Representative packages include Iroh networking, remote management, Windows
Cloud Files, and WASIX.

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
