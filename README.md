# Sinomo

Sinomo is a local-first, distributed runtime for a continuously operating AI
system. It combines perception, typed events, model-backed agents, durable
memory, situated actions, a shared filesystem, and controlled software rollout
across one or more trusted devices.

A single machine is a one-node Sinomo cluster. The same runtime can extend to
multiple nodes while keeping data placement, device ownership, policy, and
authority explicit.

## What is in this repository

- `sinomo`, the main command-line entry point and interactive terminal UI;
- `sinomo-app`, a graphical client for chat, SIFS files, cluster state, and
  settings;
- the daemon, cluster control plane, object RPC, KV/Raft, SIFS/DFS, storage,
  policy, module, rollout, and observability foundations;
- agent, memory, model-provider, audio, video, voice, action, and evolution
  services;
- native modules, Docker cluster harnesses, and executable Quint
  specifications.

The [architecture overview](docs/architecture.md) describes the system's stable
boundaries and invariants. Use the [documentation index](docs/README.md) to find
operator guides, subsystem designs, specifications, plans, and research notes.

## Quick start

### Requirements

- Rust 1.90 or newer;
- Git;
- a supported desktop platform for the graphical app;
- Docker with Compose, only for the containerized cluster and Android
  workflows.

The desktop app uses the separate [SUI](https://git.sinomo.net/sinomo/sui)
workspace through a relative path dependency. Check out `sinomo` and `sui` as
siblings before building `sinomo-app` or the full workspace:

```text
parent/
├── sinomo/
└── sui/
```

Platform development libraries used by SUI/wgpu and the native audio and video
backends must also be available when building the desktop app.

The CLI-only build below does not build the graphical client.

### Build and run the CLI

From the repository root:

```bash
cargo build -p sinomo
cargo run -p sinomo -- --help
```

Run without a subcommand to open the terminal UI:

```bash
cargo run -p sinomo --
```

The TUI connects to the local daemon when one is available. CLI commands can be
entered inside it with a `/` prefix, such as `/models list`; use `/quit` or
`/exit` to leave.

For a persistent local installation with daemon startup integration:

```bash
cargo run -p sinomo -- install
cargo run -p sinomo -- cluster init --name home
cargo run -p sinomo -- cluster status
```

Run `cluster init` only for a fresh, uninitialized data directory. An existing
installation should inspect its current cluster with `cluster status` instead.

Inspect a command before using it with `sinomo <command> help`. The main command
families cover clusters, daemons, DFS, rollouts, models and providers, agents,
skills, voice, usage, and runtime tests.

### Run the desktop app

With the sibling SUI checkout in place:

```bash
cargo run -p sinomo-app
```

The app uses the same runtime and local state as the CLI. If arguments are
provided, the `sinomo-app` binary behaves like the `sinomo` CLI instead of
opening a window.

## Local state and configuration

Sinomo stores local state under `~/.sinomo` by default. Set `SINOMO_DATA_DIR` to
use a different persistent data directory:

```bash
export SINOMO_DATA_DIR="$HOME/.sinomo-dev"
cargo run -p sinomo -- daemon start
```

The data directory contains local and shared configuration, cluster state, DFS
content, provider credentials, daemon state, managed rollout source, installed
binaries, logs, and evolution artifacts. Do not commit it.

Provider setup is available through the graphical settings UI or the CLI:

```bash
cargo run -p sinomo -- providers help
cargo run -p sinomo -- models help
```

## Common development commands

```bash
# Build the default workspace members.
cargo build

# Run all tests or a focused package/test.
cargo test
cargo test -p sinomo-core
cargo test -p sinomo-core --test workspace_ring_boundaries

# Formatting, linting, and a focused sandbox check.
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo check -p sinomo-sandbox --features wasix-runtime
```

Development builds use optimization level 2, so clean builds are slower than a
typical Rust development profile.

Every crate declares a trust ring in its `Cargo.toml`. The ring-boundary test
enforces dependency direction: R0 is the minimal cluster foundation,
`sinomo-core` is its facade, R1 is the trusted runtime,
`sinomo-runtime` is the R1 facade, and R2/application crates consume those
facades. Read [the crate-boundary plan](docs/plans/r0-r1-crate-boundary-refactor.md)
before changing cross-crate dependencies.

## Docker and Android

The repository includes three container workflows:

- [`docker-compose.yml`](docker-compose.yml) builds a full, self-rollout-capable
  node;
- [`docker/docker-compose.dev.yml`](docker/docker-compose.dev.yml) runs a
  disposable six-node segmented development cluster using the host-built
  `target/debug/sinomo` binary;
- [`docker/docker-compose.single-node.yml`](docker/docker-compose.single-node.yml)
  runs a minimal one-node smoke environment.

A typical development-cluster loop is:

```bash
cargo build -p sinomo
docker compose -f docker/docker-compose.dev.yml up --build -d
docker compose -f docker/docker-compose.dev.yml exec node1 sinomo cluster health
docker compose -f docker/docker-compose.dev.yml down -v
```

See the [Docker guide](docker/README.md) for topology, correctness suites,
migration tests, performance gates, and failure artifacts.

The Android NativeActivity package can be built in the provided container:

```bash
HOST_UID="$(id -u)" HOST_GID="$(id -g)" \
  docker compose -f docker/docker-compose.android.yml run --build --rm android-apk
```

The builder uses the `android-debug-small` profile and produces an aarch64 APK
by default.

## Repository layout

| Path | Purpose |
|---|---|
| `crates/sinomo` | Thin binary wrapper for the main CLI/TUI entry point. |
| `crates/sinomo-cli` | CLI parsing, command handlers, daemon operations, and TUI. |
| `crates/sinomo-app` | Shared graphical application and desktop binary. |
| `crates/sinomo-app-android` | Android NativeActivity package wrapper. |
| `crates/sinomo-core` | R0 facade over the minimal cluster foundation. |
| `crates/sinomo-runtime` | R1 facade over trusted runtime services. |
| `crates/` | Runtime, service, application, and test crates. |
| `modules/` | Native network, sandbox, DFS, remote-management, and benchmark modules. |
| `docs/` | Architecture, user guides, specifications, plans, and research. |
| `specs/quint/` | Executable formal models, checks, and scenarios. |
| `docker/` | Images, Compose files, and multi-node validation harnesses. |

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture.md)
- [Cluster operations](docs/user/cluster-operations.md)
- [Rollout operations](docs/user/rollout-operations.md)
- [Agent system architecture](docs/agent-system-architecture.md)
- [DFS architecture](docs/dfs-architecture.md)
- [Memory architecture](docs/memory-architecture.md)
- [Formal verification](docs/formal/cluster-verification.md)

## License

The workspace metadata declares dual licensing under MIT or Apache-2.0.
