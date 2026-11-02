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
`SINOMO_DATA_DIR` override, that installation is `~/.sinomo`.

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
