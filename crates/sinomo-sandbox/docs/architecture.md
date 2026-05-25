# Sandbox Architecture

`sinomo-sandbox` is layered so the sandbox can start with local development storage but eventually run on SIFS without changing the command contract.

```text
agent or CLI request
  -> sinomo sandbox command or daemon RPC
  -> cluster sandbox policy
  -> sandbox instance manifest
  -> pinned toolchain package
  -> WASIX or Preview1 runtime
  -> sandbox virtual filesystem
  -> local development backend or SIFS
```

## Storage Model

Durable sandbox state is rooted in SIFS:

```text
/services/sandbox
```

Instances use:

```text
/services/sandbox/instances/<sandbox_id>/manifest.sobj
/services/sandbox/instances/<sandbox_id>/root/
/services/sandbox/instances/<sandbox_id>/home/
/services/sandbox/instances/<sandbox_id>/tmp/
/services/sandbox/instances/<sandbox_id>/work/
/services/sandbox/instances/<sandbox_id>/checkpoints/
/services/sandbox/instances/<sandbox_id>/logs/
/services/sandbox/instances/<sandbox_id>/artifacts/
```

Toolchains use:

```text
/services/sandbox/toolchains/<toolchain_id>/manifest.sobj
/services/sandbox/toolchains/<toolchain_id>/bin/
/services/sandbox/toolchains/<toolchain_id>/lib/
/services/sandbox/toolchains/<toolchain_id>/share/
/services/sandbox/toolchains/<toolchain_id>/include/
/services/sandbox/toolchains/<toolchain_id>/etc/
```

The runtime sees a virtual Unix tree. Typical mounts include:

```text
/bin        pinned command modules
/lib        pinned support libraries
/share      pinned shared data
/include    pinned headers
/etc        generated and packaged config
/home       durable sandbox home
/tmp        instance-scoped temporary storage
/work       working directory
/artifacts  outputs intended for the caller
/logs       command records and raw streams
/dev        Sinomo-owned virtual devices
/proc       generated process view
```

`/tmp` belongs to the sandbox instance, not the agent session. Reboot clears it by default, but instance policy can retain it. Destroying the instance deletes the whole instance root.

## Virtual Filesystem Boundary

All sandbox filesystem operations should go through the virtual filesystem layer. The local host-backed filesystem is a development backend and staging implementation detail. Runtime code should not assume host paths are available or stable, because production state is expected to be SIFS-backed.

SIFS-backed WASIX execution mounts the sandbox workspace and declared SIFS mounts as virtual preopens backed directly by `SandboxFileSystem`; it must not copy SIFS state into host directories for the actual command run. Toolchain artifacts are provided through read-only virtual preopens built from the SIFS toolchain objects. WASI Preview1 remains a local-only compatibility runner and must not be used for cluster-backed SIFS execution.

The virtual filesystem layer owns:

- path normalization and escape checks,
- read-only and read-write mount behavior,
- symlink and hardlink semantics,
- file metadata and usage accounting,
- direct SIFS-backed WASIX mounts and local development backends,
- generated device and procfs entries.

## Runtime Model

WASIX is the default runtime and primary development target. It is required for real shell, Python, compiler, package manager, subprocess, pipe, and threading workloads. WASI Preview1 remains as a compatibility adapter for simple single-process modules.

Runtime target code is split under `src/runtime/`:

- `src/runtime/wasix/` owns Wasmer/WASIX setup, budget guards, process-tree signaling, virtual preopens, output streaming, memory checks, and metering.
- `src/runtime/preview1/` owns the Wasmtime/WASI Preview1 runner and bounded compatibility imports.
- `src/runtime/capabilities/` describes runtime capability reporting used by diagnostics.

WASIX execution denies ambient HTTP imports. Network access should go through Sinomo-mediated fetch APIs so policy, audit, and cluster routing stay visible.

## Process And Thread Model

The sandbox is intended to support normal POSIX-style coding workflows. That requires:

- subprocess execution through pinned sandbox commands,
- cwd and environment propagation,
- fd inheritance and redirection,
- pipes and wait semantics,
- process groups, sessions, TTY foreground ownership, and signals,
- pthread-style threading over shared imported memory,
- futex, mutex, condition variable, and TLS behavior.

The implementation currently supports part of this surface. The status doc tracks the exact remaining gaps.

## Policy And Audit

Sandbox policy is stored in cluster KV at:

```text
/config/sandbox/policy
```

Policy controls:

- allowed mount prefixes and local-owned mount placement,
- allowed toolchain IDs, hashes, provenance prefixes, and package-cache paths,
- allowed command capabilities,
- memory, wall-time, input, output, workspace, write, and file-count budgets,
- concurrent command and sandbox budgets,
- HTTP host and byte/timeout budgets.

Daemon-backed state-changing operations validate policy before mutating SIFS. Denied operations produce durable policy-denial events.

Command records and events capture lifecycle, output chunks, final status, resource exhaustion, mediated HTTP exchanges, toolchain updates, and sandbox configuration changes. Command logs persist under the instance `logs/` directory and can be replayed into the shared event stream.

## Source Layout

- `src/model.rs` defines IDs, manifests, mounts, resource limits, command records, and shared data structures.
- `src/runtime/` contains runtime adapters and capability reporting.
- `src/filesystem/` contains local and SIFS virtual filesystem backends plus path and usage helpers.
- `src/store/` contains instance stores, command-record persistence, and workspace sync helpers.
- `src/execution/` contains local and SIFS staged command execution plus structured process-plan execution.
- `src/toolchain/` contains local/SIFS toolchain stores, manifests, hash verification, package import helpers, and built-in package specs.
- `src/policy/`, `src/events/`, and `src/http/` own policy validation, event lowering, and mediated HTTP behavior.
- `toolchains/` contains maintained package recipes and verifiers.
- `vendor/` contains Sinomo-maintained vendored runtime crates needed for the WASIX path.
