# sinomo-sandbox

`sinomo-sandbox` is Sinomo's Rust-native sandbox subsystem for agent tool
execution. It owns sandbox manifests, policy validation, the Sandbox component,
runtime contracts, toolchain packages, virtual filesystem wiring, durable
command records, and operator diagnostics. The WASIX engine is the external
module in `modules/sandbox/wasix`, and simple-native host execution lives in
`crates/sinomo-sandbox-simple`.

The target environment is a real POSIX command workspace for coding tasks: `sh`/`bash`, Python with pip, file/search/archive tools, and build tools run inside the sandbox contract. Commands are only exposed when they are genuine sandbox commands.

## Start Here

- [Architecture](docs/architecture.md): storage layout, runtime boundaries, virtual filesystem model, policy, events, and source layout.
- [Implementation notes](docs/implementation-notes.md): current runtime, filesystem, toolchain, operational status, remaining gaps, and historical detailed implementation notes extracted from the project plan.
- [Arch-style package build guide](docs/sandbox-package-build-guide.md): package-manager bootstrap, WASIX package repository, and automated Arch PKGBUILD source rebuild goals.

## What This Crate Owns

This crate owns the sandbox-facing contracts and implementation pieces:

- manifest, policy, mount, command, event, and resource-limit data models,
- the Sandbox component: service routes, local/clustered backend selection,
  SIFS lifecycle leases, mediated HTTP, execution streams, and lifecycle
  recovery,
- local and SIFS-backed sandbox stores,
- runtime capability contracts and the client for the
  `sinomo.sandbox-executor/v1` interface that the WASIX module implements,
- the Docker/Podman local container backend,
- virtual filesystem integration,
- toolchain package metadata and import helpers,
- durable command records, output logs, checkpoints, and artifacts,
- operator diagnostics used by `sinomo sandbox doctor`.

It does not define global cluster storage layout outside the sandbox service prefix, and it should not bypass Sinomo networking or storage policy.

## Runtime Model

WASIX is the only maintained Wasm sandbox runtime because shell, Python,
compiler, package manager, subprocess, pipe, threading, and filesystem
workflows need process-oriented semantics. WASI Preview1 support has been
removed rather than kept as a compatibility runner. The CLI loads the active
`module.sandbox.wasix` native module for WASIX execution; the default `sinomo`
build does not link the engine. Docker/Podman and simple-native are host
backends for local and agent workflows; see the
[architecture](docs/architecture.md#execution-backend-model).

Filesystem behavior should go through the sandbox virtual filesystem layer.
Local host-backed storage is useful for development, but production sandbox
state is expected to be backed by SIFS. SIFS-backed WASIX runs use direct
virtual preopens backed by the sandbox filesystem.

## Common Commands

Create a local sandbox:

```sh
sinomo sandbox create
```

Create a daemon-backed SIFS sandbox:

```sh
sinomo sandbox create --daemon
```

Run a pinned command, or select a host backend explicitly:

```sh
sinomo sandbox run <sandbox-id> grep -- -R TODO /work
sinomo sandbox run <sandbox-id> --simple-native sh -- -c 'echo hello'
sinomo sandbox run <sandbox-id> --docker <image> sh -- -c 'echo hello'
```

Start a shell when the sandbox toolchain includes a real `sh`:

```sh
sinomo sandbox shell <sandbox-id>
```

`--daemon` still selects the daemon-hosted Sandbox component for lifecycle,
file, fetch, log, and toolchain commands. Daemon-hosted `run`, `shell`, and
`process-plan` against clustered SIFS sandboxes are rejected; see the
[current execution boundary](docs/architecture.md#component-and-crate-ownership).

Inspect state and logs:

```sh
sinomo sandbox status <sandbox-id>
sinomo sandbox doctor <sandbox-id>
sinomo sandbox logs <sandbox-id> <command-id>
```

Manage lifecycle:

```sh
sinomo sandbox checkpoint <sandbox-id>
sinomo sandbox resume <sandbox-id> <checkpoint-id>
sinomo sandbox reboot <sandbox-id>
sinomo sandbox destroy <sandbox-id>
```

Use mediated fetch instead of ambient network access:

```sh
sinomo sandbox fetch <sandbox-id> https://example.com/data.json /work/data.json
sinomo sandbox fetch <sandbox-id> https://example.com/data.json /work/data.json --daemon
```

## Toolchain Packages

Maintained toolchain packages live under `toolchains/`:

- [BusyBox](toolchains/busybox/README.md): base file, text, archive, and search tools.
- [Bash](toolchains/bash/README.md): real `bash` and `sh`.
- [Python](toolchains/python/README.md): CPython and pip.
- [Build tools](toolchains/build-tools/README.md): Make, TCC, Clang/LLVM, LLD, and support files.

Useful verifier entry points:

```sh
crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh
crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh
crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh
```

Do not expose a new default command until its real implementation passes the relevant verifier. Avoid semantic source patches to upstream tools; WASM/WASIX build-system patches are acceptable when documented in the toolchain package. POSIX compatibility must be implemented in the runtime, virtual filesystem, compiler, sysroot, or system-library layer. Program-specific shims, including the current CPython compatibility layer, are legacy stopgaps and should be removed as the generic ABI catches up.

## Development Workflow

When adding sandbox functionality:

1. Add or update the model, policy, or runtime contract first.
2. Keep filesystem behavior behind the sandbox virtual filesystem layer.
3. Add the real toolchain package or runtime support needed by the command.
4. Add verifier coverage that runs the real command path.
5. Update [docs/implementation-notes.md](docs/implementation-notes.md) with implementation status and remaining gaps.

Keep this README as an entry point; do not use it as a detailed changelog or gap tracker.

## Source Notes

- Keep runtime contracts and capability reporting under `src/runtime/`; the
  Wasmer/WASIX engine belongs in `modules/sandbox/wasix`.
- Keep filesystem behavior behind the virtual filesystem traits in `src/filesystem/`.
- Keep SIFS persistence and command-record behavior in `src/store/` and `src/execution/`.
- Keep policy, event, and mediated HTTP contracts in `src/policy/`, `src/events/`, and `src/http/`.
