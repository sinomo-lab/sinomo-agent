# Sandbox Architecture

`sinomo-sandbox` is layered so the sandbox can start with local development
storage but eventually run on SIFS without changing the command contract.

```text
agent or CLI request
  -> sinomo sandbox command or Sandbox component route
  -> cluster sandbox policy
  -> sandbox instance manifest
  -> execution backend (WASIX executor module, Docker/Podman, or simple-native)
  -> sandbox virtual filesystem
  -> local development backend or SIFS
```

## Component And Crate Ownership

The Sandbox component is materialized by `sinomo_sandbox::component_factory`
from generic component context. It owns local/clustered backend selection,
SIFS lifecycle leases, toolchain mutations, mediated HTTP, framed execution
streams, and the durable lifecycle recovery worker. Without cluster identity
it serves the local filesystem store at `<data-dir>/sandbox/runtime`; with a
cluster it serves the SIFS-over-DFS backend, except that lifecycle requests for
non-portable (node-local) instances use the local store. Lifecycle
routes (`create`, `status`, `update`, `destroy`) accept local or cluster-member
callers; execution, file, toolchain, and network routes are local commands. The
daemon only hosts the component through the product component catalog; see
the [system architecture](../../../docs/architecture.md).

Execution engines:

- WASIX is the external native-dylib module `module.sandbox.wasix`
  (`modules/sandbox/wasix`, crate `sinomo-sandbox-wasix-module`, built with its
  `wasix-runtime` feature). It implements the provider-neutral
  `sinomo.sandbox-executor/v1` interface, which this crate resolves by
  service requirement through `sinomo-module`'s active native component
  binding. The `wasix-runtime` feature on `sinomo-sandbox` itself is only a
  capability-report marker and links no engine code.
- Simple-native host execution lives in the `sinomo-sandbox-simple` crate.
- Docker/Podman orchestration is lightweight host-process code in
  `sinomo-sandbox`.

Current execution boundary: the clustered backend rejects dynamic
`/sandbox/run/*` toolchain and process-plan requests, so commands run in the
invoking process (the CLI's local WASIX, container, or simple-native path, or
the agent's simple-native path) rather than inside the daemon.

## Storage Model

Durable sandbox state is rooted in SIFS:

```text
/services/sandbox
```

Instances use a control root, immutable workspace generations, and a stable
workspace path:

```text
/services/sandbox/control/<sandbox_id>/manifest.sobj
/services/sandbox/control/<sandbox_id>/checkpoints/
/services/sandbox/generations/<sandbox_id>/<incarnation_id>/<transaction_id>/
/services/sandbox/instances/<sandbox_id>
/services/sandbox/locks/<sandbox_id>.lock
```

Each generation contains `root/`, `home/`, `tmp/`, `work/`, `var/`, `logs/`,
and `artifacts/`. The stable instance path is a symlink that an authoritative
lifecycle transaction replaces atomically to publish one complete generation.
The manifest, its reconciliation fence, and the checkpoint catalog stay in the
control root so publishing a generation cannot orphan them. Lock anchors live
outside the instance subtree so destroy and recreate cannot evade a live
lifecycle lease on the previous generation.

### Lifecycle Recovery

Recovery of an interrupted authoritative transaction is event-triggered; the
component never lists `/services/sandbox/control` or runs a recovery cadence.
Before writing a fence, a transaction records two obligations naming its node
and lifecycle lease session: a node-local wake marker under
`<data-dir>/daemon/sandbox-lifecycle-recovery/pending/` and a compact cluster-KV
index entry at `/sandbox/pending/<sandbox_id>`. Both are retired when the fence
clears, or when the transaction fails without leaving a fence. A fence is then
finished, always under the exclusive lifecycle lease, when:

- a lease-holding mutation opens the sandbox; a transaction finishes a
  different pending operation first and continues its own pending operation
  through its idempotent path;
- a status, list or read request reports the fence, which schedules one
  background recovery;
- the component host becomes ready, for the node's own wake markers;
- committed membership drops the node named by an index entry, for that
  node's entries; the live node that wins the lease finishes them.

Fences written without these obligations, for example by older binaries,
recover through the access paths. Lease contention defers recovery to the
holder with a bounded follow-up that spans one advisory-lock lease.

Toolchains use:

```text
/services/sandbox/toolchains/<toolchain_id>/manifest.sobj
/services/sandbox/toolchains/<toolchain_id>/bin/
/services/sandbox/toolchains/<toolchain_id>/lib/
/services/sandbox/toolchains/<toolchain_id>/share/
/services/sandbox/toolchains/<toolchain_id>/include/
/services/sandbox/toolchains/<toolchain_id>/etc/
/services/sandbox/toolchains/<toolchain_id>/bundles/
```

The runtime sees a virtual Unix tree. Typical mounts include:

```text
/bin        pinned command modules
/lib        pinned support libraries
/share      pinned shared data
/include    pinned headers
/etc        generated and packaged config
/root       durable root home
/home       durable sandbox home
/tmp        instance-scoped temporary storage
/work       working directory
/var        durable variable state
/artifacts  outputs intended for the caller
/logs       command records and raw streams
/dev        Sinomo-owned virtual devices
/proc       generated process view
/sys        generated sysfs view
```

`/tmp` belongs to the sandbox instance, not the agent session. Reboot clears it
by default, but instance policy can retain it. Destroying the instance deletes
its generations and control root, including `/tmp` and checkpoints.

## Virtual Filesystem Boundary

WASIX sandbox filesystem operations go through the virtual filesystem layer. The local
host-backed filesystem remains a development backend and must not be used as an implicit
copy of production SIFS state.

SIFS-backed WASIX execution mounts the sandbox workspace and declared SIFS
mounts as virtual preopens backed directly by `SandboxFileSystem`; it must not
copy SIFS state into host directories for the actual command run. Toolchain
artifacts are provided through read-only virtual preopens built from the SIFS
toolchain objects. This direct-SIFS runner lives in the WASIX module crate; the
`sinomo.sandbox-executor/v1` interface currently carries only host-path
preopens, so no production host invokes it yet.

Docker/Podman execution is a local host-container backend. It binds the local sandbox instance directories to `/root`, `/home`, `/tmp`, `/work`, `/var`, `/logs`, and `/artifacts` inside a selected image. It does not replace the SIFS-backed WASIX path for clustered execution; clustered execution still needs the policy-visible SIFS virtual filesystem and pinned toolchain model.

The virtual filesystem layer owns:

- path normalization and escape checks,
- read-only and read-write mount behavior,
- symlink and hardlink semantics,
- file metadata and usage accounting,
- direct SIFS-backed WASIX mounts and local development backends,
- generated device and procfs entries.

## Execution Backend Model

Sinomo supports multiple sandbox execution backends behind the same instance, command, output, and command-record contracts.

WASIX is the intended production SIFS backend. It is required for
policy-visible clustered execution, pinned toolchain packages, direct SIFS
mounts, and the long-term self-evolution path; see the current execution
boundary above for what is wired today. WASI Preview1 support has been removed
rather than kept as a compatibility path.

Docker/Podman is a local backend for development and compatibility workloads. It runs commands from a configured image with network disabled by default, a read-only image root, dropped Linux capabilities, and writable sandbox mounts for the standard sandbox directories. Container mode treats the requested command name as an executable inside the image; it does not use WASIX module artifacts from the sandbox toolchain store.

Simple-native is a host-command backend for workflows that need arbitrary native tools. A
command is still admitted through the sandbox manifest, cluster policy, resource limits,
lease, cancellation, and audit paths, but the admitted process sees the host filesystem and
normal host command semantics. Simple-native is a permission-boundary hint, not a security
sandbox intended to contain hostile code or prevent deliberate escape.

On Linux, simple-native can project policy-approved SIFS paths from an
already-mounted live DFS/FUSE root into the command's filesystem. A bubblewrap
(`bwrap`) mount namespace is used only to express those guest paths; it
deliberately keeps the host root visible and writable. Other platforms reject
manifests that declare mounts. There is no command-time copy-in or copy-back.
Native programs therefore use the same POSIX filesystem objects as every other
cluster client, and remote changes become visible according to the DFS
filesystem's normal consistency rules.

Container mode can be selected explicitly:

```text
sinomo sandbox run <sandbox-id> --docker <image> sh -- -c 'echo hello'
sinomo sandbox process-plan <sandbox-id> --podman <image> plan.json
```

Simple-native mode can be selected explicitly:

```text
sinomo sandbox run <sandbox-id> --simple-native sh -- -c 'echo hello'
sinomo sandbox process-plan <sandbox-id> --simple-native plan.json
```

Container mode can also be selected for local service routes and local CLI execution through:

```text
SINOMO_SANDBOX_BACKEND=docker|podman
SINOMO_SANDBOX_CONTAINER_IMAGE=<image>
```

Simple-native can also be selected for local CLI execution through:

```text
SINOMO_SANDBOX_BACKEND=simple-native
```

Simple-native host-path hints can be extended when a caller needs to describe additional
paths:

```text
SINOMO_SANDBOX_SIMPLE_READ_PATHS=/path/one:/path/two
SINOMO_SANDBOX_SIMPLE_WRITE_PATHS=/path/out
SINOMO_SANDBOX_SIMPLE_EXECUTE_PATHS=/opt/tool/bin
```

A live SIFS/FUSE root for declared mounts can be supplied with:

```text
SINOMO_SANDBOX_SIMPLE_HOST_SIFS_ROOT=/path/to/live/sifs/mount
```

This is a mount source, never a synchronization or staging directory.

WASIX runtime code is split between the contracts crate and the module crate:

- `modules/sandbox/wasix/src/runtime/wasix/` owns Wasmer/WASIX setup, budget
  guards, process-tree signaling, virtual preopens, output streaming, memory
  checks, and metering.
- `modules/sandbox/wasix/src/execution/` owns local and SIFS staged command and
  process-plan execution.
- `src/runtime/` in this crate holds the ABI selector and the capability
  reporting used by diagnostics.

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

The implementation currently supports part of this surface. The
[implementation notes](implementation-notes.md#remaining-gaps) track the exact
remaining gaps.

## Synchronization And Checkpoints

SIFS synchronization is continuous and independent of command or checkpoint state. WASIX
uses the SIFS VFS directly, while simple-native uses live DFS/FUSE mount projections. A
successful write is visible through that filesystem immediately; writes completed before a
failure, timeout, or cancellation are not rolled back merely because the command failed.

Checkpoints are explicit lifecycle artifacts for tracking and restoring work. They do not
commit, flush, synchronize, or publish a command, and command execution must never create,
resume, or discard one implicitly. Import/export helpers may copy trees only when explicitly
invoked as lifecycle or migration operations; they are not an execution transport.

## Policy And Audit

Sandbox policy is the `sandbox.policy` configuration definition. The cluster
value, which overrides the local default, is stored in cluster KV at:

```text
/config/sandbox/policy
```

Policy controls:

- allowed mount prefixes, read-write SIFS mount opt-in, and local-owned mount
  placement,
- allowed toolchain IDs, unsigned-toolchain opt-in, provenance prefixes, and
  package-cache paths,
- allowed command capabilities,
- memory, wall-time, input, output, workspace, write, and file-count budgets,
- concurrent command and sandbox budgets,
- HTTP host and byte/timeout budgets.

Clustered state-changing operations validate policy before mutating SIFS.
Denied operations produce durable policy-denial events.

Command records and events capture lifecycle, output chunks, final status, resource exhaustion, mediated HTTP exchanges, toolchain updates, and sandbox configuration changes. Command logs persist under the instance `logs/` directory and can be replayed into the shared event stream.

## Source Layout

- `src/model.rs` defines IDs, manifests, mounts, resource limits, command records, and shared data structures.
- `src/service.rs`, `src/module.rs`, and `src/storage.rs` define the Sandbox
  component, its routes, and its storage registration.
- `src/backend.rs`, `src/clustered_backend.rs`, `src/clustered_lifecycle.rs`,
  `src/clustered_toolchain.rs`, and `src/lifecycle_recovery.rs` own backend
  selection, SIFS lifecycle leases and transactions, toolchain mutations, and
  lifecycle recovery.
- `src/external_executor.rs` and `src/external_executor/` define the
  `sinomo.sandbox-executor/v1` protocol and its typed client and runner.
- `src/runtime/` contains the WASI ABI selector and capability reporting.
- `src/filesystem/` contains local and SIFS virtual filesystem backends plus path and usage helpers.
- `src/store/` contains instance stores, command-record persistence, explicit checkpoints,
  and opt-in import/export helpers.
- `src/execution/` contains execution contracts, audit and cancellation helpers,
  process-plan types, and the Docker/Podman backend.
- `src/toolchain/` contains local/SIFS toolchain stores, manifests, hash verification, package import helpers, and built-in package specs.
- `src/policy/`, `src/events/`, and `src/http/` own policy validation, event lowering, and mediated HTTP behavior.
- `toolchains/` contains maintained package recipes and verifiers.
- `modules/sandbox/wasix/` (outside this crate) contains the WASIX executor
  module; its `vendor/` holds the Sinomo-maintained Wasmer/WASIX crates and
  the WASIX libc source used by the toolchain sysroot.
