# sinomo-sandbox

`sinomo-sandbox` is Sinomo's Rust-native sandbox subsystem for agent tool
execution. It provides resumable command workspaces backed by SIFS, a WASI
command runner, pinned toolchain manifests, policy validation, command logs, and
operator diagnostics.

The goal is a minimal real POSIX command environment for coding tasks, not a
POSIX-like imitation. Commands are only exposed when Sinomo can run a genuine
implementation inside the sandbox contract. Missing commands are reported
explicitly instead of being replaced with host tools or fake subsets.

## Architecture

The sandbox is layered around SIFS persistence:

```text
agent or CLI request
  -> sinomo sandbox command
  -> daemon sandbox RPC for cluster-backed operation
  -> sandbox policy and toolchain validation
  -> SIFS-backed sandbox instance
  -> local staged WASI execution
  -> logs, command records, and changed files written back to SIFS
```

The crate owns the reusable contracts and implementations:

- `SandboxInstanceManifest` records the sandbox id, pinned toolchain, cwd,
  environment, mounts, resource limits, command history, and `/tmp` policy.
- `LocalSandboxInstanceStore` and `SifsSandboxInstanceStore` manage instance
  lifecycle, checkpoints, reboot, resume, and destroy.
- `LocalSandboxFileSystem` and `SifsSandboxFileSystem` expose the sandbox
  virtual paths with path and symlink escape protection.
- `LocalWasiCommandRunner` runs WASI preview1 modules with stdin/stdout/stderr,
  memory/input/output limits, timeout/cancellation, explicit directory preopens,
  and no ambient TCP, UDP, name lookup, or host filesystem access. Preview1
  extension helpers resolve paths below a root `/` preopen while preserving
  more specific nested preopen precedence, so read-only submounts cannot be
  bypassed through root. The preview1
  pinned subprocess bridge applies the same containment checks when redirecting
  a child command's stdout or stderr to a sandbox path, and validates child
  environment entries before building the child WASI context. A child
  environment entry must be `NAME=VALUE` with `NAME` matching the sandbox
  environment-name grammar. A child spawned with a null environment pointer inherits the parent command
  environment, and child processes share the parent command's remaining
  preview1 fuel budget. Explicit child cwd requests are rejected when the path
  is missing, is not a directory, or escapes through a symlink; redirected
  spawn-failure output is resolved relative to that same child cwd. Ambiguous
  stderr requests that ask for both `stderr_to_stdout` and a separate stderr
  path are rejected before the child starts. Redirected child stdout/stderr
  obeys the same output limit as top-level command output, and child
  file-backed stdin is checked for sandbox containment, symlink escape, and
  size before the host reads it into the child input pipe.
- The WASIX runner applies the same direct stdin input limit before constructing
  its guest stdin file. When preopened directories are present, it preflights
  the requested cwd against those mounts before starting the module and rejects
  unmapped paths, files, and symlink escapes. WASIX mapped preopen trees are
  also rejected when they already contain symlinks, so host-fs mappings do not
  inherit symlink escape state from staged workspace data. WASIX modules are
  preflighted for ambient HTTP imports; raw `wasix_http_client_v1` imports are
  rejected before execution because sandbox network access must use the
  mediated fetch path.
- `LocalStagedSandboxCommandRunner` and `SifsStagedSandboxCommandRunner` stage
  workspaces into the WASI runner, persist command output records, and sync
  changed workspace and read-write mount data back to the backing store.
- `SandboxProcessPlan` provides the lower-level process substrate for running
  multiple pinned toolchain commands with explicit stdout-to-stdin pipes. A
  step that receives a pipe from the previous step cannot also declare its own
  stdin payload, because that would make dataflow ambiguous. It is not a shell
  and does not provide shell syntax, job control, or process spawning semantics
  by itself. Each step must use an `argv[0]` that matches the pinned command
  name, so a multicall module cannot use a checked command name to invoke a
  different applet.
- `LocalSandboxToolchainStore` and `SifsSandboxToolchainStore` persist pinned
  command modules and support artifacts with BLAKE3 hash verification.

## Source Layout

Keep new sandbox implementation work in the module that owns the relevant
contract instead of growing the facade files:

- `src/runtime/preview1/` contains the Wasmtime/WASI Preview1 runner and the
  bounded Preview1 compatibility imports.
- `src/runtime/wasix/` contains the Wasmer/WASIX runner, process-tree signaling,
  streaming output file, static memory-budget checks, and WASIX preopen/cwd
  guards.
- `src/execution/local.rs` owns local staged single-command execution,
  checkpoint rollback, quota accounting, and command records.
- `src/execution/local_process_plan.rs` owns local structured process-plan
  execution, while `src/execution/local_fidelity.rs` owns local toolchain
  verifier runner helpers.
- `src/execution/sifs.rs` owns SIFS-backed staged single-command execution,
  placement metadata, read-write mount sync, and SIFS command history
  writeback.
- `src/execution/sifs_process_plan.rs` owns SIFS-backed structured
  process-plan execution.
- `src/execution/staging/` owns temporary runtime staging helpers for SIFS
  mounts and pinned toolchain runtime mounts such as `/bin`, `/lib`, `/share`,
  `/include`, and `/etc`; its verifier tests live beside that staging code.
- `src/filesystem/local.rs` and `src/filesystem/sifs.rs` own the local and SIFS
  virtual filesystem backends. Shared filesystem errors, SIFS path helpers, and
  symlink/path containment checks live in `src/filesystem/support.rs`.
- `src/toolchain/local.rs` and `src/toolchain/sifs.rs` own local and SIFS
  toolchain stores. Toolchain manifests stay in `src/toolchain/manifest.rs`;
  hash, bundle, validation, and error helpers stay in
  `src/toolchain/support.rs`; built-in BusyBox package registration stays in
  `src/toolchain/busybox.rs`.
- `src/policy/core.rs`, `src/events/envelope.rs`, and
  `src/http/protocol.rs` own policy validation, durable event lowering, and
  mediated HTTP/fetch behavior respectively. Their `mod.rs` files are thin
  facades.

## SIFS Layout

Durable sandbox state lives under:

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
/services/sandbox/toolchains/<toolchain_id>/bin/<command>.wasm
/services/sandbox/toolchains/<toolchain_id>/lib/
/services/sandbox/toolchains/<toolchain_id>/share/
/services/sandbox/toolchains/<toolchain_id>/include/
/services/sandbox/toolchains/<toolchain_id>/etc/
```

Daemon-backed SIFS writes use DFS content-service advertisements as replica
candidates and fall back to the local node when no cluster view is available.

## Command Surface

The maintained base userspace target is BusyBox WASIp1. The current profile
exposes real file-management, search, archive, and text
processing tools such as `awk`, `cat`, `chmod`, `cmp`, `cp`, `diff`, `find`,
`grep`, `gzip`, `head`, `ls`, `mkdir`, `mv`, `patch`, `rm`, `sed`, `sort`,
`tail`, `tar`, `test`, `touch`, `uniq`, and `wc`.

The current default BusyBox profile does not expose `sh`. BusyBox ash requires
descriptor and process behavior that the runtime does not yet provide, and the
recipe must not patch ash command execution, pipeline, here-doc, or redirection
semantics to hide that runtime gap.

BusyBox `awk`, `gzip`, and `chmod` are exposed only for the behavior that
passes the maintained fidelity suite. `awk` external command features remain
unsupported until Sinomo has child-process execution.

QuickJS is the first richer package target and can be imported into the coding
toolchain for JavaScript-based helper scripts.

## CLI Examples

Create a local sandbox:

```sh
sinomo sandbox create
```

Create a daemon-backed SIFS sandbox and retain `/tmp` across session reboots:

```sh
sinomo sandbox create --daemon --tmp retain
```

Run a pinned toolchain command:

```sh
sinomo sandbox run <sandbox-id> grep -- -R TODO /work
```

Run a structured process plan that pipes one pinned command into the next:

```sh
sinomo sandbox process-plan <sandbox-id> process-plan.json
sinomo sandbox process-plan <sandbox-id> --daemon process-plan.json
```

Each process-plan step selects a pinned command by name and can override the
step-local cwd and environment without mutating the sandbox manifest:

```json
{
  "steps": [
    {
      "command_name": "find",
      "argv": ["find", ".", "-type", "f"],
      "cwd": "/work",
      "env": { "LC_ALL": "C" },
      "pipe_stdout_to_next": true
    },
    {
      "command_name": "grep",
      "argv": ["grep", "TODO"],
      "stdin": null
    }
  ]
}
```

Environment variable names from process plans and toolchain package
`default_env` entries must use the same sandbox env-name grammar as
`sinomo sandbox config --env`: ASCII alphanumeric or `_`, not empty, not
starting with a digit, and no `=`.
The local and SIFS process-plan runners preflight every step cwd before running
the first command, so a statically invalid later step does not leave earlier
command history or staging artifacts behind.

Run through the local daemon and stream output before the final command record:

```sh
sinomo sandbox run <sandbox-id> --daemon find -- /work -type f
```

Daemon-backed process plans also stream each step's raw stdout/stderr chunks
before the final process-plan summary:

```sh
sinomo sandbox process-plan <sandbox-id> --daemon process-plan.json
```

Inspect command logs:

```sh
sinomo sandbox logs <sandbox-id> <command-id>
```

Checkpoint, resume, reboot, and destroy:

```sh
sinomo sandbox checkpoint <sandbox-id>
sinomo sandbox resume <sandbox-id> <checkpoint-id>
sinomo sandbox reboot <sandbox-id>
sinomo sandbox destroy <sandbox-id>
```

Use the mediated HTTP fetch path instead of ambient network access:

```sh
sinomo sandbox fetch <sandbox-id> https://example.com/data.json /work/data.json
sinomo sandbox fetch <sandbox-id> https://example.com/data.json /work/data.json --daemon
```

Fetch stores both the response body and a redacted HTTP exchange record in the
sandbox filesystem. Audit records and `sandbox.network.http` events retain only
scheme, host, and path from the URL; query strings and fragments are dropped
before persistence. On daemon/SIFS-backed paths those objects are written
through the same replica-candidate mechanism as other sandbox workspace data.

Show sandbox readiness and missing capability findings:

```sh
sinomo sandbox doctor
sinomo sandbox doctor <sandbox-id>
```

## Toolchain Maintenance

Real command packages live under `toolchains/`. Each package owns its build
recipe, import manifest generation, verifier, and any package-specific notes:

- BusyBox: `toolchains/busybox/`
- QuickJS: `toolchains/quickjs/`
- Bash: `toolchains/bash/`
- Python and pip: `toolchains/python/`

The BusyBox verifier is the gate for exposing base userspace applets:

```sh
crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh
```

`sinomo sandbox toolchain busybox-plan` prints the maintained applet list,
fidelity cases, and known MVP command gaps. A command listed as a gap is
intentionally absent from the default toolchain until a real substrate can pass
the same verifier standard.

The combined coding-toolchain verifier builds BusyBox and QuickJS, imports
QuickJS into the BusyBox toolchain, creates a disposable sandbox, and runs the
default coding fidelity checks. The workflow covers file creation,
copy/move/compare, permission changes, archive create/list, remove/rmdir
cleanup, stdin text processing, search, and a QuickJS-to-BusyBox cross-tool
file handoff:

```sh
crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh
```

The verifier removes its automatically-created disposable `SINOMO_DATA_DIR`
after the run. Set `SINOMO_VERIFY_DATA_DIR=<path>` to use an operator-owned
directory, or set `KEEP_SANDBOX=1` to retain generated state for debugging.
Set `SKIP_BUILD=1` to reuse existing package outputs from the verifier output
directories. Set `VERIFY_FULL=1` to run the heavier local gate: BusyBox,
QuickJS, and Python/pip are verified in one WASIp1 coding workspace, then
BusyBox WASIX, Bash, Make, and TCC are verified in a separate WASIX shell/build
workspace. Full mode uses `target/` for its automatically-created data
directories because the Python package is large. With `VERIFY_FULL=1
SKIP_BUILD=1`, the reused BusyBox WASIX, Bash, Python, Make, and TCC package
manifests must already exist in their configured output directories.

Bash is maintained as the real `bash` and `sh` route for shell workflows. It
is a WASIX package, not a patched BusyBox ash substitute. Local verification
builds BusyBox WASIX plus Bash WASIX, imports Bash into the disposable
toolchain, and runs shell cases that depend on external command execution:

```sh
sinomo sandbox toolchain bash-plan
crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
sinomo sandbox toolchain verify-bash <sandbox-id> [--daemon] [--allow-unsigned]
```

Daemon-backed Bash verification uses cluster policy and requires both
`child-process-exec` and the temporary `wasix.unpinned-child-exec`
acknowledgement to be allowlisted.

Python and pip are maintained as a real CPython package with standard-library
and support artifacts. The verifier covers interpreter probes, offline
pure-Python wheel/install flows, checkpoint/resume behavior, and the explicit
native-extension boundary:

```sh
sinomo sandbox toolchain python-plan
crates/sinomo-sandbox/toolchains/python/verify-python-wasi.sh
sinomo sandbox toolchain verify-python <sandbox-id> [--daemon] [--allow-unsigned]
```

Python commands require the `preview1.pinned-subprocess` capability. The local
daemon-backed mediated pip-index check also requires `network.http` and an
allowlist entry for the local package-index host, for example
`--allow-http-host 127.0.0.1`. Long package-index fetches must also fit within
the cluster policy's `--max-http-timeout-ms` limit.

For direct CLI verification, `--allow-unsigned` only applies to local toolchain
checks. Daemon-backed verification uses the cluster sandbox policy instead:

```sh
sinomo sandbox policy set --allow-unsigned-toolchains
sinomo sandbox toolchain verify-coding <sandbox-id> --daemon
```

No new command should be added to the default toolchain manifest until its real
implementation passes the relevant verifier.

## Policy And Audit

Sandbox policy is stored in cluster KV at:

```text
/config/sandbox/policy
```

Policy controls mount prefixes, local-owned mount locality, resource budgets,
concurrent execution limits, command capabilities, allowed toolchain hashes,
toolchain provenance prefixes, package-cache prefixes, HTTP hosts, and network
budgets.

Command resource budgets include memory, wall time, stdin bytes, stdout/stderr
bytes, workspace bytes, per-command written bytes, and file count. Toolchain
packages should declare both `max_input_bytes` and `max_output_bytes` in each
command's `default_limits`; older manifests without `max_input_bytes` load with
the default input budget for compatibility.

The daemon records durable events for lifecycle and config updates, mount
decisions, policy denials, resource exhaustion, toolchain updates, command
start/output/completion, and mediated HTTP exchanges. Config update events
include changed cwd status and environment variable names, but not environment
values. Command records under `/logs` persist the effective command
environment for operator inspection, including process-plan step overrides, but
command events only include environment variable names. Events are written
through `sinomo-events` to the shared JSONL event log.

Daemon-backed state-changing operations validate the current cluster sandbox
policy before mutating SIFS state. Denied create, config, write, checkpoint,
resume, reboot, run, and mediated HTTP operations emit `sandbox.policy.denied`;
mount-related denials also emit `sandbox.mount.denied`.
SIFS staged execution also rejects remote local-owned mounts before creating a
local staging workspace unless policy explicitly allows that placement.

Daemon-backed toolchain updates are preflighted against the same cluster policy
before package bytes are written to SIFS. This checks allowed toolchain ids,
toolchain provenance prefixes, package-cache paths, and default command
resource limits. Signature requirements remain an invocation-time policy check
so unsigned packages can be imported before they are signed or explicitly
allowlisted.

`sinomo sandbox doctor` reports recent sandbox-scoped policy denials for a
specific sandbox and includes service-scoped toolchain/package denials in the
global view.

Persisted command logs can be replayed into the event stream with
`append_sandbox_command_record_events`. Replay emits a reconstructed
`sandbox.command.started` event, raw stdout/stderr `sandbox.command.output`
chunk events, and then the final command status event, so downstream agents do
not have to rely on summaries.
`sinomo sandbox logs <sandbox-id> <command-id>` decodes the same durable command
record before printing stdout/stderr, including cwd, argv, and environment
variable names for the effective invocation.

## Current Limitations

- Runtime target implementations live under `src/runtime/`. The default
  preview1 path remains the stable Wasmtime runner; `target_abi = "wasix"`
  selects the Wasmer/WASIX runner only when the crate is built with the
  `wasix-runtime` feature. Toolchain package imports and
  `sinomo sandbox toolchain add-command --target-abi wasix` record that ABI in
  command metadata. The manifest-level `target_abi` is the default for older
  and single-ABI toolchains, while merged packages can carry per-command ABI
  overrides so one composite coding toolchain can contain Preview1 and WASIX
  commands. Both local and SIFS-backed staged toolchain execution honor the
  command's effective ABI instead of falling back to preview1.
  In generic `import-package` manifests, a command can set its own
  `target_abi`; if it is omitted, the package-level `target_abi` is used.
- Staged toolchain execution materializes the pinned command modules from the
  manifest under a read-only `/bin` mount before the command starts. WASIX child
  process resolution through `/bin/<command>` and PATH uses those pinned modules,
  not ambient host binaries. Strict child exec containment is still incomplete:
  Wasmer's WASIX resolver can load absolute guest WASM paths from writable
  mounts such as `/work`, so `sandbox doctor` reports
  `pinned-child-exec-containment` as an unsupported WASIX process capability.
  Support artifacts remain mounted read-only under `/lib`, `/share`,
  `/include`, and `/etc`. The test suite covers both direct `/bin/<command>`
  child execution and WASIX search-path resolution through the staged `/bin`.
  Runtime staging uses a content-addressed local `toolchain-cache` under the
  sandbox runtime root, so large support trees such as Python are expanded once
  per content hash instead of being deleted and recopied for every command.
- The current full coding verifier uses separate WASIp1 and WASIX workspaces
  instead of one merged command namespace. This avoids pretending that one
  command name can currently refer to both the WASIp1 coding implementation and
  the WASIX child-process implementation required by Bash and Make.
- WASIX host filesystem mappings are executed inside the Tokio runtime context
  required by Wasmer's host-fs layer. Read-only WASIX preopens are staged with
  host permissions made read-only for the duration of the command and restored
  after execution. When a WASIX command has mapped directories, its cwd is
  resolved through those mappings before execution; missing mappings,
  non-directory targets, and symlink escapes are rejected before the module
  starts. Mapped preopen trees are also scanned before execution and rejected
  if any existing entry is a symlink.
- WASIX modules are compiled with Sinomo's Wasmer tunables and metering. The
  tunables reject host/VM linear-memory creation above the command memory
  budget, and metering bounds non-yielding guest loops with a deterministic
  instruction budget. Yielding WASIX process trees are signaled with `SIGKILL`
  on timeout or cancellation, and `sinomo sandbox doctor` reports those
  process-tree guards separately from the remaining engine-layer gaps. Exact
  engine-level wall-clock interruption and immediate cancellation of
  non-yielding code remain separate runtime gaps. WASIX modules are also
  scanned before compile and rejected if they import raw WASIX HTTP client
  functions; mediated `sinomo sandbox fetch` is the only network path.
  Socket-related WASIX imports are not rejected statically because WASIX libc
  and shell/process-capable toolchains may import them even for local process
  plumbing, while this build does not enable a host virtual network backend.
- Staged WASI command access to `/dev/null`, bounded `/dev/zero`, and seeded
  `/dev/random` plus `/dev/urandom` is implemented through a per-run
  Sinomo-owned `/dev` preopen. The runtime does not map host device paths.
- `sinomo sandbox shell` only works when the pinned toolchain contains a real
  `sh`; the current default BusyBox WASIp1 profile does not expose `sh` until
  the runtime has descriptor and process support. Full shell parity still
  depends on the same real process-capable substrate as pipelines, command
  substitution, and arbitrary fork/exec. Command validation is ABI-aware:
  Preview1 still rejects `child-process-exec`, while WASIX permits it only when
  the binary is built with `wasix-runtime` and cluster policy allowlists both
  `child-process-exec` and the explicit `wasix.unpinned-child-exec` gap
  acknowledgement.
- Bash, Python, and pip are required for coding sandboxes, but they are not
  claimed as generally available until real packages and fidelity suites cover
  the expected production paths. Bash has a real WASIX package and passes the
  maintained local plus daemon-local SIFS verifier when `child-process-exec`
  and `wasix.unpinned-child-exec` are explicitly allowed by policy. Python has
  a real CPython WASIp1 package with standard-library/support artifacts, and
  the verifier covers offline
  pure-Python `pip wheel` plus `pip install --find-links`, with
  checkpoint/resume through SIFS.
  It keeps pip's wheel cache under `/work/pip-cache` and verifies that cache
  after resume. It also requires local C-extension package installation to
  fail, keeping the native-extension boundary explicit. The maintained
  daemon-local verifier also covers mediated HTTP acquisition of PEP 503-style
  simple-index pages and wheels into `/work/simple` and `/work/packages`, then
  installs from that fetched index with real pip. That mediated index fixture
  first verifies sandboxed pip cannot access the HTTP package source directly,
  even while the source is reachable to the daemon-mediated fetch path. It
  now includes multiple dependency versions and verifies pip selects the newest
  compatible wheel from the fetched index, resolves that dependency's own
  transitive requirement from the same fetched index, resolves a dependency
  whose declared package name requires PEP 503 normalization, respects an
  environment marker by ignoring an incompatible dependency, skips a newer
  wheel excluded by `data-requires-python`, skips a yanked release when a
  non-yanked compatible wheel exists, applies a constraints file that pins an
  older compatible dependency, installs with `--require-hashes` from a
  requirements file that pins all transitive wheels, verifies a mismatched hash
  is denied,
  and installs an extra that pulls an optional dependency from the same mediated index. Broader resolver edge cases
  and successful native-extension support remain planned. TinyCC has a real
  WASIX build recipe and a `verify-tcc` compiler suite for version output,
  preprocessing, object generation under `/work`, invalid-input diagnostics,
  and explicit unsupported `-run` execution. GNU Make has a real WASIX build
  recipe and a `verify-make` workflow suite for Makefile parsing, dependency
  ordering, failed recipe status, and recipe execution through pinned sandbox
  commands. The combined build-tools verifier covers both packages, so `make`
  and `tcc` are implemented richer toolchain targets subject to their
  documented WASIX limits and cluster policy.
