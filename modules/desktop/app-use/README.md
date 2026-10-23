# App Use OS provider

`sinomo-app-use-module` is an independently built native dylib under the generic
Sinomo module ABI. It provides `module.app-use.os` / `sinomo.app-use.os.v1`, with
the typed `sinomo.app-use.os` service interface version `1.0.0`.

The module owns Windows target enumeration, window identity checks, WGC/D3D11
capture, message-based background input, raw SendInput and conditional native
gamepad injection. Other platforms return explicit unsupported capabilities.
It does not own an encoder, network stack, agent runtime or alternate toolset.

The linked App Use service in `sinomo-actions` supplies the single skill/tool,
actor-bound sessions, generic events and the existing media transport. It calls
this provider through `ActiveNativeComponent`; `sinomo-device` contains only
shared contracts and validation. No host crate depends on this package.

Build explicitly (native modules are not default workspace members):

```powershell
cargo build -p sinomo-app-use-module
sinomo module native-dylib activate --module module.app-use.os --interface sinomo.app-use.os.v1 --cache-key module/desktop/app-use target/debug/sinomo_app_use_module.dll
```

Installation does not grant input or capture consent. The provider requires a
host-bound permission-store identity and rechecks node-local grants for each OS
effect. Background mode is default; raw mode must be explicitly selected on a
monitor with its own exposure grants. No implicit escalation or input replay.

Native capture runs continuously in a bounded latest-frame mailbox. Raw samples
cross the stable ABI output callback as a single bounded SOBJ byte blob and enter
the host's shared media encoder. The generic invocation response carries only a
small acknowledgement, avoiding nested byte-sequence serialization. Invocation
callbacks are serialized and never retained by OS workers. Shutdown releases
held input, closes capture and joins workers before releasing code mappings.

Run artifact-backed verification after building:

```powershell
cargo test -p sinomo-actions --test app_use_native_provider -- --ignored
cargo test -p sinomo-actions --test app_use_native_windows -- --ignored --test-threads=1
cargo test -p sinomo-actions --test desktop_media_windows -- --ignored
```

The latter two require an idle interactive desktop. They create owned background
windows and verify actual pixels/messages, DPI handling, resize, revocation and
cleanup without foreground/pointer changes. `SINOMO_APP_USE_MODULE` can select a
specific built DLL; test installation is confined to temporary data directories.
See the [desktop agent harness](../../../docs/developer/testing.md#live-desktop-agent-verification-windows)
for recorded real-agent tests of both modes through the same tools.
