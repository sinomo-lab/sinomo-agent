---
name: sinomo-component-development
description: Add or modify Sinomo components, native modules, service offers and requirements, component events, extension contributions, settings, or storage registration. Use for product composition and lifecycle integration, including provider discovery and unload.
---

# Sinomo component development

Use the [extension guide](../../../docs/developer/extension-points.md) and
[module lifecycle architecture](../../../docs/module-lifecycle-architecture.md)
to choose the existing seam. A module is a lifecycle unit; its backend does not
change who owns the domain contract or final authority.

## Choose the owner and composition path

| Work | Current source to inspect |
|---|---|
| Built-in registration and product/host profiles | [Product catalog](../../../crates/sinomo-product-profile/src/lib.rs), especially `base_component_catalog` |
| Factory, scope, readiness, cancellation and teardown | [Component runtime](../../../crates/sinomo-module/src/component.rs) |
| Module/factory/config/storage composition | [Component catalog](../../../crates/sinomo-module/src/catalog.rs) |
| Checked service calls and discovery | [Service client](../../../crates/sinomo-module/src/service_client.rs), [catalog publisher](../../../crates/sinomo-module/src/service_catalog_component.rs) |
| Attributable component events | [Event bus](../../../crates/sinomo-module/src/event_bus.rs) |
| Native provider binding | [ActiveNativeComponent](../../../crates/sinomo-module/src/active_component.rs), [module API](../../../crates/sinomo-module-api/src/lib.rs) |
| A complete resident service component | [DFS factory](../../../crates/sinomo-dfs/src/service.rs) |
| An external native provider | [Cloud Files package](../../../modules/dfs/cloud-files/README.md) and its [manifest](../../../modules/dfs/cloud-files/Cargo.toml) |

Use `ComponentCatalog` as the composition input. Contribute factories, module
registration, configuration, storage and any status/recovery/migration hooks
through that catalog with the canonical owning component ID. Avoid a second
daemon-maintained list or domain-specific branch in generic host infrastructure.
Check main, shadow, embedded and disabled profiles that the change actually
affects. Desktop GUI code stays on its client contracts.

For an external native package, inspect the current module manifest and ABI
fixture: ring metadata, `rlib`/`cdylib`, native-dylib identity, interface version,
cache key, source, and platform declarations must agree. External providers are
leaf artifacts; keep provider-private APIs in their package. Domain clients use
the shared `ActiveNativeComponent` binding rather than another host-loader crate.

## Complete the contract and its lifetime

1. Declare versioned `ServiceOffer`/`ServiceRequirement` contracts and exact
   operations. Match route authentication, capabilities, body mode, limits and
   concurrency to their effects. Use the component service client for local
   resolution and an explicit host transport for cluster calls.
2. Keep final authorization at the effect owner. A discovered route, installed
   module, or UI action does not edit node-local consent. A component-only route
   must remain inaccessible through an ordinary daemon signer.
3. Let `ComponentScope` own services and workers. Use its shutdown receiver,
   `own_task`, and `defer` for process-local lifetime, and `on_task_drain` to
   join component-owned child registries before disposers release storage. Use
   `on_host_prepare` for recovery that must complete before serving; choose
   `on_host_start`/`on_host_ready` edges according to the dependency being
   established. Durable effects need durable compensation or recovery, not just
   a destructor.
4. Preserve native invocation leases and cancellation through unload. Withdraw
   routes before draining work; distinguish logical unload, completed teardown,
   physical library release, and a quarantined instance requiring restart.
5. Publish/subscribe through declared component events. The host supplies trusted
   provenance; consumers must tolerate bounded delivery and lag. Durable append
   or cluster forwarding requires its explicit adapter. Keep typed DFS/transport
   authority protocols on their owning paths rather than replacing them with
   best-effort component events.
6. Register settings through `ConfigDefinition`/module settings surfaces and
   persistent paths through `SinomoDataDir` plus storage contributions. Reuse the
   generic settings UI. Export metrics through `sinomo-module-api::observe` instead
   of linking an external module to the recorder's process-global state.

For behavior/UI contributions, the extension-point owner defines schemas,
composition, ordering, bounds, and final effects. Installation order is not
selection authority. Agent packages may advertise defaults, while reusable
tools and skills remain discoverable independently of a specific agent.

## Verify the component contract

Cargo builds use all available build-server cores. Never add `--jobs` flags or
`CARGO_BUILD_JOBS` overrides to address resource issues or work around compiler
concurrency bugs. Diagnose and fix the underlying issue; keep resource placement
and admission separate from compiler concurrency.

Exercise registry finalization (`try_build`), including cross-module collisions;
registration alone does not establish catalog validity. Query the complete
relevant catalog when checking discovery, then test resolution and authorization
through the actual caller path.

Cover missing dependencies, readiness failure, disabled/shadow profiles,
cancellation, partial initialization cleanup, and unload/reload as relevant.
For a native ABI change, build and exercise the external fixture/provider, not
only host-side mocks. Run the trust-ring gate for changed manifests, facades,
features or imports, plus the [validation matrix](../../../docs/developer/testing.md)
for the domain behavior. Updating an installed artifact uses the
[rollout workflow](../sinomo-rollout/SKILL.md).
