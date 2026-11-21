# Development Setup

This guide prepares a reproducible Rust build and an isolated runtime state for
local development.

## Toolchain

`Cargo.toml` declares Rust 1.95 as the minimum. CI currently builds with Rust
1.96.0, so that version is the easiest way to reproduce its compiler behavior:

```sh
rustup toolchain install 1.96.0 --profile minimal --component rustfmt clippy
rustup override set 1.96.0
rustc --version
```

The workspace has no `rust-toolchain` file. If you use a newer compiler, keep
the declared minimum in mind when adopting language or standard library
features. Only the separate DFS/KV architecture lint workspace pins its own
nightly; see the [lint README](../../lints/sinomo-architecture/README.md).

Fetch locked dependencies and build the CLI/TUI baseline:

```sh
cargo fetch --locked
cargo build -p sinomo --locked
cargo run -p sinomo -- --help
```

Cargo fetches the published SUI v0.4.1 packages from crates.io. Workspace
dependencies select the release, and `Cargo.lock` records versions and checksums.
A sibling SUI checkout is not required for an ordinary build.

## Isolate Runtime State

Never point development commands at a production or personally important
Sinomo data directory. Choose and verify a fresh persistent isolated root
before starting a daemon or initializing a cluster:

```sh
export SINOMO_DATA_DIR="$HOME/.sinomo-dev"
cargo run -p sinomo -- daemon start
cargo run -p sinomo -- cluster init --name dev
cargo run -p sinomo -- cluster status
```

Initialize only a new directory. On later runs, start the daemon and inspect
the existing cluster rather than calling `cluster init` again.

For foreground logs:

```sh
cargo run -p sinomo -- daemon run
```

Use another terminal with the same `SINOMO_DATA_DIR` for client commands. Stop
the detached daemon first if switching to foreground mode.

## Build Surfaces

```sh
# CLI and TUI only; recommended first build.
cargo build -p sinomo

# Default workspace members.
cargo build

# Every workspace member with default features.
cargo check --workspace

# Graphical desktop application and its supervised sibling helpers.
cargo build -p sinomo -p sinomo-ui-extension-host -p sinomo-app
cargo run -p sinomo-app
```

The desktop app needs platform libraries used by SUI/wgpu and native audio and
video backends. The repository does not yet maintain a verified package list
for every Linux distribution, macOS, and Windows release; treat the CLI as the
portable baseline and diagnose app dependencies for the target platform.
The app resolves `sinomo` and `sinomo-ui-extension-host` beside its
own executable and never searches `PATH`; packages must therefore install or
bundle all three artifacts together. The UI helper starts lazily only when an
active module contributes a custom surface.

`cargo run -p sinomo-app` rebuilds only the app and its library dependencies,
not the sibling helper executables. Repeat the three-package build above after
updating CLI or launcher code. The app starts `sinomo app supervise`, which
leases `sinomo daemon run --app-owned` when no daemon is already running. If
`SINOMO_APP_DAEMON_BINARY` is set, it must point to an up-to-date `sinomo`
executable that supports these commands.

Source builds and executables outside the installation run exactly the app
executable passed to `sinomo app supervise`. This includes `cargo run --bin
sinomo-app`: an older active desktop release cannot replace the just-built GUI.
These launches retain daemon supervision and the per-data-directory launcher
lock, but do not participate in installed-release activation or rollback.
Their GUI receives no update launch token, so supervised app updates require
launching the installed desktop entry point instead.

The stable launcher at `$SINOMO_DATA_DIR/bin/sinomo-app` (with `.exe` on Windows)
and images inside its `rollout/desktop/releases` directory keep managed release
selection, verification, workspace restoration, and update rollback. With no
`SINOMO_DATA_DIR` override or installer binding, the data directory defaults to
`~/.sinomo`.

### Desktop Installer Packages

`scripts/ci/build-desktop-installer.py` builds the CLI, app, UI-extension host and
setup helper together with `--locked`, stages a desktop manifest, and invokes
`cargo-packager` pinned to `0.11.8`. Run on the target OS:

```sh
cargo install cargo-packager --version 0.11.8 --locked
python scripts/ci/build-desktop-installer.py --target x86_64-unknown-linux-gnu
```

On Windows, use `--target x86_64-pc-windows-msvc` with MSVC/Windows SDK, Rustup,
Python 3.11 or newer, CMake, LLVM and Git. The GitLab Windows job requires a
provisioned shell runner tagged `windows` with a clean CI account. Packaging runs
a silent install/uninstall smoke test that checks directory choices, CLI launch,
live main/shadow RPC, startup registration, cross-uninstallation, PATH restoration,
default data retention, explicit data deletion and `--no-start`. It refuses to
touch an existing registered Sinomo installation.
The Linux job uses the desktop build
libraries declared in `.gitlab-ci.yml`, plus the AppImage packaging tools.
`APPIMAGE_EXTRACT_AND_RUN=1` allows packaging tools to run without FUSE in CI.

Packages and checksums are written under `dist/installers/<target>/`. The staging
directory is under `CARGO_TARGET_DIR/installer-stage/<target>` and must be fresh
for each run. `--skip-build` packages an existing target-specific build;
`--profile dev` selects debug binaries for local packaging checks.
`--test-root DIRECTORY` puts acceptance fixtures on another volume without
relocating build outputs or losing their retained diagnostics. Release jobs
produce unsigned initial installers; public Windows distribution should add
Authenticode signing using protected release credentials.

Windows packages use a custom NSIS template for directory selection and optional
integration. The installed `sinomo-data-dir` file binds bootstrap binaries to the
chosen absolute data path without changing a global environment variable. Invalid
bindings fail explicitly. `SINOMO_DATA_DIR` and embedded process defaults retain
precedence. Bootstrap app images bound to that data root participate in ordinary
managed release selection.

The Linux AppImage is a graphical installer, not the installed app itself. Setup
retains the AppImage for the app and graphical uninstaller, with launch scripts
that keep desktop libraries available for the app. The installed CLI is a
standalone executable and its daemons run without the AppImage, FUSE or GUI
libraries. Package builds compile the CLI separately from the desktop targets
to prevent native media/GUI feature unification from changing that contract.
The Linux installer registers an applications-menu uninstaller even when the app
menu shortcut is disabled. Neither platform installs `cargo-packager-updater`.

Initial installation and removal are owned by `sinomo-installer::initial_setup`.
The graphical setup helper supplies native dialogs and AppImage transport; the
CLI exposes directory and integration flags over the same receipt, binding,
release-retention and removal logic. Both frontends use the same default service
options and register/start main and shadow through `initial_setup::services`.
Recognizable older CLI startup entries for the same data directory are adopted
with their existing bind settings; explicit CLI bind options override that
choice. Entries for other data directories are preserved.
Startup names are scoped by canonical data directory so unrelated deployments
cannot overwrite each other's entries. The receipt records exact unit/script
contents and enablement links for ownership-checked removal. Uninstall stops
recovery before main. `--no-start` stages registration without launching services.

The minimal-container CI gate runs the standalone CLI in a slim image with no
service manager, display or graphical helpers. It checks dynamic dependencies,
rejects graphical dispatch, verifies live main/shadow RPC, and exercises enabled
unit registration, removal, data retention and staging. A desktop application's
dependency graph is not evidence that the separately built CLI is headless.

The Linux payload archive preserves the original manifest-verified executables
while `linuxdeploy` prepares loose copies to discover shared libraries. After
packaging, the build runs the resulting image in extraction mode and verifies
that archive without opening desktop dialogs or initializing a user installation.

These packages are deliberately outside default members because they require a
platform, runtime, or explicit benchmark choice:

- `sinomo-dfs-fuse`;
- `sinomo-app-android`;
- `sinomo-sandbox-wasix-module`;
- `sinomo-benchmarks`;
- `sinomo-evolution`;
- `sinomo-dfs-cloud-files`;
- `sinomo-network-iroh-module`;
- `sinomo-remote-management-module`.

Build the actual WASIX runtime module explicitly when working on that backend:

```sh
cargo check -p sinomo-sandbox-wasix-module --features wasix-runtime
```

The similarly named feature on `sinomo-sandbox` enables the contracts crate's
WASIX capability marker. It adds no runtime implementation dependency and does
not build the native module itself.

## Docker and Android

The disposable development cluster uses the host-built debug binary. Build it
with profiler support so its nodes can start bounded CPU profiles when profiling
is requested with `SINOMO_TEST_CPU_PROFILE=1`:

```sh
cargo build -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
docker compose -f docker/docker-compose.dev.yml up --build -d
docker compose -f docker/docker-compose.dev.yml exec node1 sinomo cluster health
docker compose -f docker/docker-compose.dev.yml down -v
```

The repository also has full self-rollout and single-node Compose definitions.
Read the [Docker guide](../../docker/README.md) before running correctness,
migration, routing, or performance harnesses; they create volumes and preserve
failure artifacts with semantics that differ from an ordinary dev stack.

Build the Android NativeActivity package in its container:

```sh
HOST_UID="$(id -u)" HOST_GID="$(id -g)" \
  docker compose -f docker/docker-compose.android.yml run --build --rm android-apk
```

GitLab's `build:android-arm64` job publishes the signed ARM64 testing APK and
its checksum under `dist/android/` in the job artifacts. It runs on protected
`main` and protected tags such as `android-apk-*`. CI uses the same
`android-debug-small` profile as the container build.

Configure two protected GitLab variables scoped to `android-ci-testing`.
`SINOMO_ANDROID_TEST_KEYSTORE_B64` is a file variable containing the
base64-encoded testing keystore. `CARGO_APK_ANDROID_DEBUG_SMALL_KEYSTORE_PASSWORD`
holds the store and key password. Keep this testing identity stable so one CI
build can update another on a device.

For an attached ARM64 development device:

```sh
scripts/android-dev.sh doctor
scripts/android-dev.sh run
scripts/android-dev.sh logs
scripts/android-dev.sh debug
```

Install the host's Android USB/udev support first and set `ANDROID_SERIAL` when
several devices are visible. `run` builds, installs, and launches the app;
`debug` attaches the NDK LLDB frontend with native/Rust line information.

The host helper uses `android-debug`: media encoding, image decoding and the
native App Use pixel path are optimized while debug assertions and line
information remain available. Development packaging explicitly keeps the APK
debuggable; release packaging does not enable that override. Its APK is under
`target/android-host/android-debug/apk/`. The Docker helper defaults to
`android-debug-small`, which inherits the same optimization settings and strips
debug information. Select `--profile android-debug` for matching manual Android
Cargo builds, including separately built native providers.

## Run the User Interfaces

With the isolated daemon running:

```sh
# Terminal UI.
cargo run -p sinomo --

# Graphical app.
cargo run -p sinomo-app

# One agent turn.
cargo run -p sinomo -- agent run --prompt "Explain the current workspace"
```

Provider-backed commands may contact external services and consume quota. Use
fixtures and non-live runtime tests until a live provider is part of the test's
explicit purpose.

## Keep the Checkout Clean

Do not commit:

- Cargo output under `target/` or `dist/`;
- formal output under `_apalache-out/`;
- benchmark reports under `benchmark-results/`;
- sandbox toolchain `work-*` or `out-*` directories;
- credentials, invites, data directories, or captured private prompts.

Before switching tasks, inspect `git status --short --branch`. Existing changes
belong to their author; do not format, revert, or stage unrelated files.
