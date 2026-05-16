# SIFS-Backed WASM Sandbox Plan

## Purpose

Sinomo needs a resumable sandbox for agent actions, tool execution, lightweight
build/test work, and long-running task workspaces. The target is inspired by
WebVM's browser-hosted Linux-like command environment, but the Sinomo sandbox
should not boot a full Linux distribution or emulate a whole machine.

The first implementation should be a Rust-native WASM command runtime with a
minimal real POSIX userspace, explicit Sinomo policy enforcement, and persistent
workspace state stored in SIFS. Sinomo should own and maintain the sandbox
runtime, packaging recipes, policy contract, and command verification suite in
this repository instead of relying on agent-crate-specific code or ad hoc host
tool execution.

The primary initial workload is coding tasks. Assume language toolchains are
available as pinned sandbox toolchains; the base userspace must give the agent
enough real file management, text processing, and search tools to inspect,
modify, build, test, and package code without falling back to host execution.
Language toolchains can be larger optional packages, but the baseline coding
surface must include real commands for file management and search before the
sandbox is treated as useful for coding agents.

The sandbox should provide:

- a real command shell and a useful set of real Unix tools,
- Bash as a real shell command for coding tasks,
- Python and pip as real language/package-management commands for coding tasks,
- deterministic process and filesystem policy boundaries,
- durable task workspaces under SIFS,
- resume after daemon restart or node handoff,
- inspectable execution records for agents and operators,
- a path toward richer toolchains without trusting host binaries by default.

## Active Focus Gate

The current implementation focus is full POSIX/process support for the Python
coding workload. Pause all sandbox improvements that do not directly unblock
this gate unless they fix a serious bug that prevents the POSIX/process layer
from working.

This is the only active sandbox implementation goal until it is finished.
Do not spend implementation time on unrelated sandbox improvements, command
surface bookkeeping, UX polish, rollout automation, cluster features, broader
fidelity expansion, or documentation cleanup while this gate is open. A bug is
in scope only when it directly prevents the POSIX/process substrate from
running the Python workload below as a real sandbox workload.

The concrete readiness gate is:

- `python` runs as a real sandbox command.
- `pip` runs as a real sandbox command.
- `pip install` can install ordinary packages into the sandbox workspace.
- `numpy` imports and executes representative array operations inside the
  sandbox.
- `matplotlib` imports and can render a non-interactive figure to a file inside
  the sandbox.

The gate includes the process behavior those tools rely on: real command
execution, child-process spawning from sandbox-visible paths, inherited and
explicit working directories, environment propagation, stdin/stdout/stderr
handling, exit-status reporting, bounded execution, dynamic module loading, and
durable writes through the sandbox filesystem.

This gate is not satisfied by host execution, fake commands, Python-specific
shortcuts that bypass the sandbox process model, or patching application logic
to compensate for missing runtime behavior. Missing behavior must be fixed in
the sandbox POSIX/process substrate so normal Python packages can run as they
would in a minimal real POSIX environment.

The earlier WASIp1 CPython package is not sufficient for this gate. It can run
Python and pure-Python pip workflows, but it cannot provide the dynamic loading
needed by ordinary compiled Python wheels. The active implementation path is the
WASIX CPython package plus the WASIX runtime/linker work required for real
dynamic side modules, C/C++ extension initialization, callback tables, and
process semantics.

Current gate evidence:
`crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh` passes
in `VERIFY_BACKEND=daemon-local` mode against
`target/sinomo-python-wasix-native-out/package.json`. That verifier imports the
real BusyBox WASIX, Bash WASIX, and CPython WASIX packages through the daemon,
seeds a WASIX wheelhouse into the SIFS-backed sandbox workspace, stages the
Python WASM module into `/work` and executes it through `subprocess.run`, runs
`subprocess.run(..., shell=True)` through the pinned `/bin/sh`, runs real
`pip install` for `numpy` and `matplotlib`, verifies Python child-process
stdin, cwd, environment, stderr, exit-status handling, verifies
`subprocess.Popen(...).communicate(...)` stdin/stdout/stderr descriptor
behavior, verifies `subprocess.Popen(...).wait(timeout=...)` reports a still
running child as a timeout and `Popen.kill()` makes the child waitable,
verifies `subprocess.run(..., timeout=...)` raises `TimeoutExpired` for both
inherited and captured stdio paths, verifies runaway Python compute is reported
as a sandbox timeout, checkpoints and resumes the sandbox after installing
native packages, imports and executes NumPy from the resumed package tree,
renders a matplotlib Agg PNG under `/work`, and reads the rendered file back
through the daemon/SIFS path. The successful run rendered
`target/sinomo-sandbox-host-tmp/matplotlib-wasix-native-gate-01krqz00000000000000000001.png`.
The SIFS writeback path was fixed for this workload by making self-only
replicated-eventual writes local commits, indexing tombstones in memory, and
syncing only changed command paths plus logs after staged SIFS execution, so
Python package trees do not require per-file distributed intent bookkeeping,
full metadata scans, or full workspace writeback after every command.

The broader local coding gate also passes in `VERIFY_FULL=1` mode through
`crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` with prebuilt WASIX
BusyBox, Bash, Make, TinyCC, and WASIp1 Python packages. That gate verifies the
file/search BusyBox surface, QuickJS handoff, Python stdlib execution,
`python -m pip`, real `pip` command execution, local package wheel/install
flows, simple-index installs, Bash pipelines/redirection/error handling,
descriptor duplication, command substitution, `/work` shebang execution, `sh` execution through the
maintained Bash package, Make recipes with a sandbox shell, and TinyCC
preprocessing/object compilation diagnostics. The focused Bash WASIX verifier
also stages a WASM executable into `/work` with `sinomo sandbox write --file`
and executes it through Bash, proving the explicit sandbox-local child exec
mode. This gate is supporting evidence for the POSIX/process work; it does not
broaden the active scope beyond the Python/POSIX gate above.

## Implementation Status

Current repo status:

| Phase | Status | Evidence |
|---|---|---|
| Phase 0: Architecture And API Skeleton | Implemented | `sinomo-sandbox` defines sandbox IDs, manifests, resource limits, VFS traits, storage registration, path normalization, and error shapes. `sinomo-cli` exposes `sinomo sandbox` help. |
| Phase 1: Local WASI Command Runner | Partially implemented | `LocalWasiCommandRunner` executes WASI preview1 modules through Wasmtime, passes stdin, captures stdout/stderr, reports exit status, enforces timeout/cancellation, memory limits, output limits, no default host preopens, explicit read-write/read-only preopened temporary directories, and path-escape denial. `LocalStagedSandboxCommandRunner` wraps local staged command execution with checkpoint rollback, quota checks, changed-path reporting, stdin forwarding, stdout/stderr persistence, command records, and optional per-command effective resource limits. `SandboxToolchainManifest`, `LocalSandboxToolchainStore`, and `SifsSandboxToolchainStore` record command names, module paths, BLAKE3 hashes, support artifact paths/hashes, capabilities, default limits, provenance, local cache persistence, and SIFS persistence under `/services/sandbox/toolchains/<toolchain_id>`. `sinomo sandbox toolchain add-command` installs real WASI command modules into the local staged toolchain cache, `sinomo sandbox toolchain import-busybox` imports a real BusyBox WASI module as the default core applet toolchain, and named `sandbox run`/`sandbox shell` apply command default limits from the pinned toolchain manifest. `crates/sinomo-sandbox/toolchains/busybox` now includes a pinned BusyBox 1.36.1 plus wasi-sdk 24.0 WASIp1 build recipe with SHA-256 verified downloads, a Sinomo applet config fragment, and a containerized build entrypoint. |
| Phase 2: SIFS-Backed Workspace | Partially implemented | `LocalSandboxInstanceStore` creates the planned instance directory layout and persists `manifest.sobj` through `sinomo-object`; `LocalSandboxFileSystem` provides a tested local staging VFS for root/home/tmp/work/logs/artifacts paths with path-escape and symlink-escape checks, snapshot/diff changed-path reporting, workspace file-count/byte accounting, per-command written-byte accounting, local checkpoint/resume/reboot/destroy, and checkpoint cleanup; command stdout/stderr plus structured `.record.sobj` records are persisted under `/logs` through backend-agnostic helpers tested against both local and SIFS filesystems, command ids are appended to the sandbox manifest command history after completed, failed, cancelled, or resource-exhausted runs, persisted `cwd` and environment variables are configurable through `sinomo sandbox config`, `sandbox_command_record_event` lowers completed/failed/resource-exhausted records into cluster event envelopes, `sandbox_policy_denied_event` and `sandbox_scheduling_failed_event` provide typed denial/failure events, and `append_sandbox_command_record_events` reads persisted local/SIFS command records plus stdout/stderr logs and appends reconstructed `sandbox.command.started`, raw `sandbox.command.output` chunk events, and the final command-status event into an `EventStore`; `sinomo sandbox logs <sandbox-id> <command-id>` decodes the command record before printing raw streams; `SifsSandboxInstanceStore` creates/loads/saves instance manifests and standard directories under `/services/sandbox/instances/<sandbox_id>`, with SIFS checkpoint/resume/reboot/delete/destroy support for root/home/tmp/work/logs/artifacts and explicit replica-candidate writes for distributed persistence tests; `SifsSandboxFileSystem` adapts sandbox virtual paths to the existing SIFS POSIX API for stat/read/write/list/rename/unlink/create-dir, supports explicit replica candidates, and is tested with `NetworkedDfsService` including a cross-node resume/read regression; backend-agnostic workspace sync copies root/home/tmp/work/logs/artifacts between local staging and SIFS while preserving empty directories and removing stale destination entries; `SifsStagedSandboxCommandRunner` stages a SIFS workspace into a local WASI runtime, stages declared SIFS mounts into local WASI preopens, executes through the local staged runner, syncs read-write mounts back to SIFS, syncs command logs/workspace data back to SIFS, saves updated manifest command history back to SIFS, accepts explicit replica candidates for workspace/log/mount writeback, and is tested with command-write, process-plan output, and read-write mount resume/read regressions from a replica node; `SifsSandboxToolchainStore` writes and reads toolchain manifests/modules through SIFS with hash verification; `sinomo sandbox create`, `status`, `config`, named `run`, `run --wasm`, `logs`, `ls`, `cat`, `write`, `checkpoint`, `resume`, `reboot`, `destroy`, and local `toolchain add-command` are wired, `sinomo sandbox create --mount <sifs-path>:<mount-path>[:ro|rw]` persists mount declarations in the manifest, and local `sandbox run` now validates declared mounts, pinned command capabilities, effective command limits, and `argv[0]` before delegating the execution transaction to `sinomo-sandbox`. Local CLI commands still default to local staging, but `sinomo sandbox create --daemon`, `status --daemon`, `config --daemon`, `toolchain import-busybox --daemon`, `run --daemon`, `shell --daemon`, `logs --daemon`, `ls --daemon`, `cat --daemon`, `write --daemon`, `fetch --daemon`, `checkpoint --daemon`, `resume --daemon`, `reboot --daemon`, and `destroy --daemon` now use the daemon/SIFS path for SIFS-backed instances, SIFS toolchains, status reads, command execution, real pinned shell invocation when available, logs, file inspection, writes, daemon-backed HTTP fetch-to-file, checkpoint, resume, reboot, and destroy. |
| Phase 3+: BusyBox userspace, scheduling, network, richer toolchains | Partially implemented | `SandboxPolicy` provides reusable checks for allowed mount prefixes, local-owned mount locality, resource budgets, concurrent command/sandbox budgets, command capability allowlists, signed/allowlisted toolchains, toolchain provenance prefix allowlists, package-cache SIFS prefix allowlists, HTTP host allowlists, HTTP byte/timeout budgets, toolchain update preflight, and full invocation validation across instance manifest, pinned toolchain, selected command, and execution node. `SandboxClusterPolicyConfig` owns the typed cluster KV policy contract at `/config/sandbox/policy`, with snapshot decode and transaction helpers. `select_sandbox_execution_node` provides the tested node-locality scheduling primitive, prefers the owner node for `/sinomo/nodes/<node_id>/files` mounts, returns inspectable `unavailable-offline` and `policy-blocked` reasons, and SIFS staged execution records `last_placement` in the sandbox manifest. Local CLI named command execution plus local and SIFS staged runners reject denied manifests before execution or staging, and named pinned toolchain commands only run after validating toolchain signature/allowlist/provenance where applicable, package-cache paths, command capabilities, matching `argv[0]`, and effective command limits. Local and SIFS filesystem APIs expose virtual `/dev`, `/dev/null`, and bounded `/dev/zero`; staged WASI command execution now installs a per-run Sinomo-owned `/dev` preopen so commands can open `/dev/null` and bounded `/dev/zero` without mapping host devices. The daemon now exposes `/sandbox/create`, `/sandbox/status`, `/sandbox/checkpoint`, `/sandbox/resume`, `/sandbox/destroy`, `/sandbox/list`, `/sandbox/read`, `/sandbox/write`, `/sandbox/http/fetch`, `/sandbox/http/fetch-to-file`, `/sandbox/toolchain/import-busybox`, `/sandbox/toolchain/import-package`, `/sandbox/toolchain/add-command`, `/sandbox/toolchain/add-artifact`, and `/sandbox/run/toolchain`, `/sandbox/run/toolchain/stream`, `/sandbox/run/process-plan`, and `/sandbox/run/process-plan/stream`, loads the cluster sandbox policy, rejects denied create/config/write/checkpoint/resume/reboot mutations before changing SIFS state, preflights daemon toolchain imports/adds before writing SIFS package state, enforces active execution concurrency limits before daemon-backed runs, opens SIFS through the local DFS metadata/content stores, runs the pinned SIFS toolchain command through `SifsStagedSandboxCommandRunner`, derives replica candidates from DFS content-service advertisements, and persists placement/log/workspace changes back to SIFS. `sinomo sandbox create --daemon`, `status --daemon`, `toolchain import-busybox --daemon`, `toolchain import-package --daemon`, `toolchain add-command --daemon`, `toolchain add-artifact --daemon`, `run --daemon`, `shell --daemon`, `logs --daemon`, `ls --daemon`, `cat --range`, `write --daemon`, `fetch --daemon`, `checkpoint --daemon`, `resume --daemon`, `reboot --daemon`, and `destroy --daemon` select daemon-backed SIFS paths while direct `--wasm` execution remains local only. `sinomo sandbox process-plan <sandbox-id> <plan.json> [--daemon]` exposes the structured process-plan substrate for pinned toolchain commands and explicit stdout-to-stdin pipes without parsing shell syntax, using either the local staging backend, the daemon-backed single-response SIFS RPC route at `/sandbox/run/process-plan`, or the live-output stream route at `/sandbox/run/process-plan/stream`. `sinomo sandbox policy show/set` reads and writes the cluster-scoped policy through cluster KV, including allowed mount prefixes, package-cache prefixes, toolchain provenance prefixes, concurrency limits, and HTTP hosts. `sinomo sandbox doctor [<sandbox-id>]` reports local toolchain readiness, missing pinned toolchains, coding-toolchain command gaps for every BusyBox applet and QuickJS command required by `verify-coding`, missing commands from the planned minimal POSIX command environment, policy incompatibilities, placement metadata, recent policy-denial and resource-exhaustion events from the durable event log, and local CLI backend limitations. `SandboxBusyBoxToolchainSpec` declares the core BusyBox applet set and can install a real BusyBox WASI module into local or SIFS toolchain stores; `BUSYBOX_TOOLCHAIN_FIDELITY_CASES` declares the first host-side real-tool checks, `sinomo sandbox toolchain busybox-plan` prints the applet/fidelity plan, and `sinomo sandbox toolchain verify-busybox [--daemon]` runs the BusyBox fidelity suite against local or daemon-backed SIFS sandboxes. Generic WASI command modules, support artifacts, and declared packages can now be added to local or SIFS toolchain manifests with `sinomo sandbox toolchain add-command [--daemon]`, `sinomo sandbox toolchain add-artifact [--daemon]`, and `sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>]`, letting richer language/runtime packages be imported as standalone toolchains or merged into a BusyBox-based coding toolchain through the same SIFS replication path. `sinomo sandbox toolchain hash <file>` prints the exact BLAKE3 content hash expected by package manifests. `crates/sinomo-sandbox/toolchains/busybox/README.md` documents the current import contract and explicitly forbids fake BusyBox substitutes, and the same directory now includes a pinned containerized BusyBox WASIp1 build recipe. `crates/sinomo-sandbox/toolchains/quickjs/README.md` adds the first concrete optional language-runtime package target: a pinned QuickJS WASIp1 recipe that generates a generic `import-package` manifest for command `qjs` plus support artifacts; `sinomo sandbox toolchain quickjs-plan` prints the maintained QuickJS target, standalone and BusyBox-merged import paths, `verify-coding` command, and fidelity case set; `sinomo sandbox toolchain verify-quickjs [--daemon]` runs QuickJS fidelity checks for expression evaluation, runtime JSON/array behavior, sandbox file reads, ES module import from `/work`, runtime error stderr reporting, and process-spawn denial against local or daemon-backed SIFS sandboxes; and `sinomo sandbox toolchain verify-coding [--daemon]` verifies that one pinned coding toolchain provides both BusyBox file/search applets and QuickJS runtime behavior, then runs a cross-tool workflow where QuickJS writes a file under `/work` and BusyBox `grep` reads it back. `SandboxProcessPlan` adds a reusable lower-level process substrate that runs multiple pinned toolchain commands as separate WASI invocations and can pipe stdout from one step into stdin of the next through both local and SIFS-backed staged runners; it rejects ambiguous steps where an explicit stdin payload would conflict with a pipe from the previous step, and it is not exposed as `sh` and does not claim shell syntax, job control, or general POSIX process parity. `sinomo sandbox shell` requires a pinned real `sh` command and does not provide a fake fallback; the default BusyBox WASIp1 profile still does not expose ash, while the maintained Bash WASIX package can provide real `sh` when imported into the pinned toolchain and allowed by cluster policy. Mediated HTTP exchange records now persist under `/logs` as SOBJ files, round-trip through local and SIFS filesystems, lower into `sandbox.network.http` events containing redacted request metadata, and the recording fetch helper audits successful and failed post-policy transport attempts. `sandbox_http_fetch_to_file` stores successful response bodies into the sandbox filesystem, and `sinomo sandbox fetch` exposes a curl-like command surface backed by the sandbox HTTP transport abstraction; the production path now routes through local daemon object RPC at `/sandbox/http/fetch` and `/sandbox/http/fetch-to-file`, and the daemon applies cluster sandbox policy before outbound HTTP. Additional packaged language/build runtimes are still needed before the coding-task toolchain surface is production-ready. |

Recent fetch-to-file quota handling update: local and daemon-backed
`sinomo sandbox fetch` now emit `sandbox.resource.exhausted` when the mediated
response body would exceed the sandbox write/workspace quota, while retaining
the HTTP audit record for the completed network attempt.

The Bash target now has a checked-in Sinomo-owned WASIX recipe under
`crates/sinomo-sandbox/toolchains/bash/`. It pins GNU Bash, wasi-sdk,
Binaryen, and wasix-libc inputs, verifies downloads, builds real upstream Bash
with the WASIX sysroot, applies Binaryen asyncify plus function-pointer cast
emulation for Bash's setjmp/longjmp and generic callback paths, and emits a
generic `import-package` manifest only after a real module exists. The recipe
builds a real GNU Bash WASIX module and the generated package manifest passes
Sinomo package validation. The package exposes `bash` and `sh` command entries
backed by the same upstream Bash module, with `sh` relying on Bash's real
invocation-name behavior rather than a shim. Local and daemon-local SIFS-backed
`verify-bash` now pass after importing Bash into the BusyBox WASIX package:
Bash builtin `printf`, external-command pipeline with `grep`, redirection plus
external `cat`, descriptor duplication with `2>&1`, command substitution,
`set -e`, `/work` shebang execution, `sh -c` external-pipeline behavior, and an explicit sandbox-local WASM
executable staged into `/work` all run through the WASIX runtime. Bash remains
gated from ordinary agent use by sandbox policy and production readiness:
Bash/WASIX command manifests declare both `child-process-exec` and
`wasix.unpinned-child-exec`, and the default policy denies both capabilities.
Ordinary clusters must explicitly allow the Bash/WASIX toolchain plus both
capabilities before `bash` or `sh` is available to agents. No shim or BusyBox
shell substitute is allowed.

BusyBox recipe status: `crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh`
is now the required end-to-end gate for applet additions. It builds the pinned
BusyBox recipe, imports the generated module, creates a disposable sandbox, and
runs `verify-busybox`. The current recipe avoids wasi-sdk SJLJ/legacy-exception
output with local WASI compatibility headers, forces a direct static applet
entry path, patches BusyBox `pwd` on WASI to validate Sinomo's injected sandbox
`PWD` before falling back to wasi-libc `getcwd`, patches BusyBox `xmkstemp` on
WASI so `patch` can create temporary files through `open(O_CREAT|O_EXCL)`, and
passes the maintained local BusyBox fidelity suite. No new BusyBox applet
should be exposed unless that verifier passes for the applet.
The same recipe now also supports `BUSYBOX_TARGET_ABI=wasix`. The WASIX path
pins Binaryen and wasix-libc in addition to BusyBox and wasi-sdk, builds the
same real BusyBox applet profile against the WASIX sysroot, applies Binaryen
asyncify plus function-pointer cast emulation, emits a generic
`import-package` manifest, and has been locally validated with package
verification, direct `grep` execution, and Bash verifier external-command
cases. This gives Bash compatible `/bin` commands without relabeling the
WASIp1 module or changing shell semantics.
`crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh` is the combined local
Bash gate: it builds BusyBox WASIX and Bash WASIX, validates both package
manifests, imports Bash into the BusyBox WASIX toolchain in a disposable data
directory, creates a sandbox, and runs `sinomo sandbox toolchain verify-bash`.
Set `VERIFY_BACKEND=daemon-local` to run the same package/import/execution gate
through a disposable local daemon, cluster policy, and SIFS-backed sandbox
workspace.
`crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` is the combined
coding-toolchain verifier: it builds the pinned BusyBox and QuickJS recipes,
imports QuickJS into the BusyBox toolchain in a disposable local data
directory, creates a sandbox, and runs `sinomo sandbox toolchain verify-coding`.
`verify-coding` now includes the Phase 3 command workflow against `/work`
before the QuickJS/BusyBox cross-tool file workflow. The workflow covers
creation, copy/move/compare, permission changes, archive create/list,
remove/rmdir cleanup, stdin text processing, search, and cross-tool file
handoff, so the local gate covers the documented coding-task acceptance path
instead of only isolated applet checks.
`crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` accepts
`VERIFY_FULL=1` as the explicit heavier local gate. Full mode keeps the
BusyBox/QuickJS/Python coding workspace on the WASIp1 BusyBox base, then runs a
separate WASIX shell/build workspace with BusyBox WASIX plus Bash, Make, and
TCC. This split is intentional: the current package model can record per-command
ABIs, but a single command name such as `grep` cannot simultaneously be the
WASIp1 coding command and the WASIX child-process command needed by Bash and
Make. `VERIFY_FULL=1 SKIP_BUILD=1` intentionally reuses existing package outputs
and fails early if any required BusyBox WASIX, Bash, Python, Make, or TCC
package manifest is missing.
The QuickJS recipe also maps build and repository paths out of compiler-visible
file/debug paths so the generated module is byte-identical across independent
work directories.

The durable event path is now shared with the wider event subsystem:
`sinomo-events` registers `events/event-log.jsonl`, and `JsonlEventStore`
persists newline-delimited `EventEnvelope` records with replay filters and
duplicate id rejection. The daemon-backed sandbox path appends
`sandbox.instance.created`, `sandbox.instance.updated`,
`sandbox.instance.checkpointed`, `sandbox.instance.resumed`,
`sandbox.instance.rebooted`, `sandbox.instance.destroyed`,
`sandbox.command.started`, `sandbox.command.output`,
`sandbox.command.completed`, `sandbox.mount.added`, `sandbox.mount.denied`,
`sandbox.toolchain.pinned`, `sandbox.toolchain.updated`, and
`sandbox.network.http` records to that store after the corresponding SIFS state
has been written. Command output events currently persist captured raw
stdout/stderr chunks after command completion. The WASI and SIFS-staged runners
now expose an output sink that receives raw stdout/stderr chunks while the
command runs, and local `sinomo sandbox run`/`shell` uses that sink for
operator-visible streaming. Daemon-backed toolchain execution also has a
streaming RPC route that sends output chunk frames before the final command
outcome frame.

## WebVM Lessons To Reuse

WebVM demonstrates a useful product shape:

- a terminal-first virtual environment,
- a prebuilt root image containing common command-line tools,
- persistent storage,
- networking as an explicit controlled subsystem,
- browser/WASM isolation instead of direct host execution.

Sinomo should reuse the shape, not the implementation. WebVM uses CheerpX to run
unmodified x86 Linux binaries with a Linux syscall emulator and a virtual block
filesystem. Sinomo should instead run WASM modules directly in a Rust host and
provide the minimum WASI-like system interface needed by our command environment.

Key differences:

| Area | WebVM-like shape | Sinomo target |
|---|---|---|
| CPU/runtime | x86 virtualization in WebAssembly | Rust host running WASM components/modules |
| OS surface | Full Linux ABI compatibility | Minimal real POSIX command surface backed by WASI/host shims |
| Root filesystem | Disk image/block filesystem | SIFS-backed virtual filesystem |
| Persistence | VM-local persistent storage | Cluster-persisted SIFS workspace state |
| Networking | Browser-mediated networking | Sinomo-network mediated allowlisted egress |
| Security model | Browser sandbox plus VM boundary | Sinomo policy, capability checks, resource limits, and node-local enforcement |

## Design Position

Use a layered sandbox:

```text
agent/tool request
  -> sinomo-sandbox service
  -> policy and capability check
  -> sandbox instance
  -> wasm runtime
  -> command environment
  -> virtual filesystem adapter
  -> SIFS
```

The sandbox runtime should be a production Sinomo service, not an experiment in
the agent crate. It should live in `sinomo-sandbox` or a successor crate with
runtime exports through `sinomo-runtime`.

The initial runtime should prioritize correctness, observability, and resumable
state over breadth. The goal is not a "POSIX-like" imitation. It is a minimal
real POSIX system for the agent: if Sinomo exposes `grep`, `sed`, `awk`,
`tar`, or any other command, that command must behave as the genuine tool within
the documented sandbox limits. If Sinomo cannot provide a real implementation,
the command should be absent and the runtime should report that it is
unsupported.

## Filesystem Model

Every sandbox has one SIFS workspace root:

```text
/services/sandbox/instances/<sandbox_id>/
```

Recommended layout:

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

The runtime mounts these as a virtual Unix tree:

```text
/bin       immutable tool package mount
/etc       generated sandbox config
/home      SIFS-backed user home
/tmp       SIFS-backed durable temp for the sandbox instance
/work      SIFS-backed working directory
/artifacts SIFS-backed outputs
/dev/null  virtual device
/dev/zero  virtual device
/dev/random virtual device
/dev/urandom virtual device
```

The first version should not expose arbitrary host paths. Host access should
only appear through explicit SIFS mounts. Existing local-owned SIFS paths such
as `/sinomo/nodes/<node_id>/files` can be mounted read-only or read-write only
when policy grants that capability.

### Workspace Persistence

SIFS is the durability boundary. The sandbox should commit meaningful progress
to SIFS after each command or explicit checkpoint:

- file mutations,
- stdout/stderr log segments,
- exit status,
- resource usage,
- current working directory,
- environment variables,
- command history,
- active package/tool versions,
- checkpoint metadata.

The runtime does not need to snapshot raw WASM linear memory in the first
version. Resume means:

```text
reload workspace state
restore virtual filesystem view
restore shell/session metadata
continue with the next command
```

`/tmp` belongs to the sandbox instance. It is durable while that instance
exists, including across daemon restart, node handoff, and `sandbox resume`.
A new agent session is only a reboot of the sandbox instance. By default, reboot
clears `/tmp`. The behavior is an instance policy flag, so selected sandboxes
can retain `/tmp` across sessions when requested. Destroying the sandbox
instance deletes the whole instance root, including `/tmp`.

Long-running process suspension can come later as an explicit checkpointable
process model.

## Runtime Model

Use a Rust WASM runtime with WASI support as the execution core. Candidate
runtimes:

- Wasmtime for the first implementation because it has mature WASI support,
  resource limits, component-model direction, and strong Rust integration.
- Wasmer/WASIX for process-capable toolchains where plain WASI preview1 cannot
  provide real fork/exec/wait, pipes, TTY, or other POSIX process semantics.

Runtime target code should stay split by ABI under `sinomo-sandbox/src/runtime/`
so preview1 and WASIX behavior can evolve independently. Toolchain manifests
keep a default ABI in `target_abi`; individual commands can override that ABI
when a composite coding toolchain merges packages that require different
runtimes, such as Preview1 CPython plus WASIX Bash/Make/TCC. Preview1 remains
the default, while WASIX requires a crate build with the `wasix-runtime`
feature.

The current WASIX runner configures Wasmer with explicit Sinomo capabilities:
ambient insecure authority remains disabled, ambient HTTP hosts remain empty,
and runtime process/thread fanout is capped before a module starts. WASIX
execution now uses a lower-level Wasmer process handle instead of the
synchronous high-level `run_wasm` entrypoint, so Sinomo can signal the root
WASIX process tree with `SIGKILL` when the requested wall-clock budget expires
or the Sinomo cancellation token is tripped. `sinomo sandbox doctor` reports
those yielding-process guards as supported `process-tree-timeout` and
`process-tree-cancellation` budget capabilities. The host wait path now polls
the termination reason and returns cancellation promptly instead of waiting for
the original command timeout after the cancellation guard has signalled the
process tree. The runner compiles WASIX command modules with Sinomo's
multi-module-safe Wasmer metering, giving standalone, pinned-child, and
sandbox-local child-exec commands a deterministic instruction budget instead of
allowing non-yielding guest loops to run without a runtime-enforced bound.
When a module already imports the real WASIX `sched_yield` syscall, the
metering middleware also injects periodic signal checkpoints at metered
branches, so a non-yielding loop in normal WASIX libc-backed commands can
observe Sinomo cancellation without the guest source calling `sched_yield`
itself. The Python gate verifies a runaway Python compute loop is reported as a
sandbox timeout. If a signalled WASIX process tree does not stop within the
shutdown grace window, the host wait path keeps waiting for the runner to stop
instead of returning while a non-yielding task can keep consuming CPU in the
background. The runner still reports exact `wall-timeout` and fully general
immediate `cancellation` budget capabilities as incomplete because
engine-level wall-clock interruption of modules without a signal-checkpoint
import remains pending; that gap must be closed before long-running WASIX
shells or language runtimes are considered production-ready.
WASIX stdout and stderr are captured through a Sinomo virtual file that emits
bounded output chunks as guest writes happen, so operators and agents are not
blocked on process exit before seeing command output. The same capture path
checks the configured per-stream output limit before accepting a guest write,
so a long-running WASIX command cannot accumulate unbounded stdout or stderr
while waiting for process exit. `sinomo sandbox doctor` reports this as the
WASIX `output-limit` budget capability alongside the direct-stdin
`input-limit` guard, the process/thread
`task-limit` guard, the deterministic `instruction-budget` guard, and the
yielding-process `process-tree-timeout`/`process-tree-cancellation` guards,
while still reporting exact wall-timeout and immediate cancellation as
incomplete until non-yielding compute loops can be interrupted at the engine
layer. The same doctor path reports WASIX `memory-limit` as a linear-memory guard: the
runner rejects modules whose imported or defined memories are unbounded or
declare initial/maximum sizes above the sandbox memory budget, and it installs
Sinomo Wasmer tunables that reject host/VM memory creation above the same
budget. This is still linear-memory budget enforcement rather than full
process-level memory accounting for every host-side runtime allocation.
When preopened directories are present, the WASIX runner also preflights the
requested cwd through those mappings before starting the module. Unmapped cwd
paths, non-directory targets, and symlink escapes are rejected as invalid
requests instead of being left to runtime-dependent host-fs behavior.
Mapped WASIX preopen trees are also scanned before execution and rejected if
any existing entry is a symlink, keeping staged workspace or toolchain state
from inheriting host-fs symlink escapes.
The WASIX runner also preflights module imports and rejects raw
`wasix_http_client_v1` imports. Sandbox commands must use Sinomo-mediated
fetch/network APIs instead of ambient WASIX HTTP. The gate intentionally does
not reject socket or port symbols that WASIX libc imports for shell and process
support; this build does not enable a host virtual-network backend for those
imports.

Staged toolchain execution must expose only the pinned toolchain contents to the
guest. Support artifacts mount read-only under `/lib`, `/share`, `/include`,
and `/etc`; command modules from the same manifest mount read-only under `/bin`
so normal WASIX `/bin/<command>` and PATH child process lookup resolves to
pinned sandbox commands instead of ambient host binaries. The WASIX child-exec
resolver supports two explicit policy modes. Commands that declare
`pinned-child-exec-containment` run in pinned-only mode, where child processes
must resolve to absolute `/bin/<command>` paths, including PATH lookups and
script interpreters. Commands that declare `wasix.unpinned-child-exec` opt into
sandbox-local child exec so shells, Python, and build tools can execute WASM
modules and scripts staged under writable sandbox paths such as `/work`, while
still resolving through the sandbox virtual filesystem rather than host paths.

The runtime should provide:

- preopened virtual directories,
  including root (`/`) preopens for preview1 extension helpers; more specific
  nested preopens still take precedence over `/` so read-only submounts cannot
  be bypassed through a root mapping,
- process plans for multiple pinned command invocations with explicit
  stdout-to-stdin pipes plus per-step cwd and environment overrides; each
  process-plan step must use an `argv[0]` matching its pinned command name so
  BusyBox-style multicall modules cannot bypass the command policy by selecting
  a different applet at runtime; per-step environment overrides, stored
  sandbox environment entries, and package default environments must use the
  sandbox environment-name grammar and reject embedded NUL values before
  staging or execution starts; package argv prefixes are validated before
  import so persisted package data cannot later produce malformed runtime argv;
  package required-capability strings must be non-empty and NUL-free before
  policy can match them, and cluster policy allowed-capability entries use the
  same validation; cluster policy SIFS prefixes, provenance prefixes, and HTTP
  host allowlists are validated on both write and KV decode before they affect
  authorization; all process-plan step cwd values are preflighted
  before the first step runs,
- a bounded preview1 subprocess bridge for runtimes such as CPython that need
  synchronous child command execution before full POSIX fork/exec/wait parity
  exists; the bridge resolves child executables only through the read-only
  pinned `/bin` toolchain mount and rejects workspace or other mounted files as
  child executables; child executable reads are rejected when the selected
  `/bin` entry is a symlink or exceeds the command memory budget before being
  read into host memory. Child stdout/stderr redirection goes back through sandbox
  path containment and rejects symlink escapes plus non-regular existing
  targets before the child command starts, so an invalid output destination
  cannot be reported only after child side effects have run. Child
  environment variable names are validated with the same sandbox grammar before
  the child WASI context is built; malformed child environment entries without
  `=` are rejected; a null child env pointer inherits the parent command
  environment. Guest-provided child argv, environment, cwd, and stdio path
  control strings are bounded before host allocation or parsing.
  Timeout/cancellation flags raised while a child is
  running are reported as top-level command budget failures instead of being
  collapsed into an ordinary child exit. Child processes draw from the parent
  command's remaining preview1 fuel budget instead of receiving a fresh full
  budget per spawn. Explicit child cwd requests are validated before the child
  starts, including missing paths, non-directories, and symlink escapes.
  Redirected spawn-failure output uses the same effective child cwd as
  successful child stdout/stderr redirection, so relative Python subprocess
  pipe paths remain under the requested child working directory. Ambiguous
  stderr requests that ask for both `stderr_to_stdout` and a separate stderr
  path are rejected before the child starts.
  Redirected child stdout/stderr is captured with the same bounded-output
  contract as top-level command output and reports `OutputLimitExceeded` before
  writing an oversized redirected file. Child executable module reads from the
  pinned `/bin` mount are checked against the command memory budget before the
  host reads the module into memory.
  Commands must declare the
  `preview1.pinned-subprocess` capability before this bridge is enabled for the
  invocation,
- stdin/stdout/stderr streams,
  with bounded direct stdin in both preview1 and WASIX plus bounded file-backed
  child stdin in the preview1 subprocess bridge, so command startup cannot
  buffer unbounded input into host memory before execution. File-backed child
  stdin is checked for sandbox containment, direct symlink entries, symlink
  escape, regular-file type, and size before the host reads the file into the
  child input pipe,
- wall-clock and CPU-ish execution budgets,
- memory limits,
- fuel or epoch interruption where available,
- deterministic cancellation,
- structured exit status,
- no ambient host filesystem access,
- no ambient host network access.

The current WASI runner explicitly disables ambient TCP, UDP, and name lookup.
Network egress for sandbox commands must go through Sinomo-mediated HTTP/fetch
paths so policy, byte budgets, audit logs, and cluster routing remain
enforceable.

### Command Execution Contract

The service API should be command-oriented:

```rust
struct SandboxCommandRequest {
    sandbox_id: SandboxId,
    argv: Vec<String>,
    cwd: String,
    stdin: Option<Vec<u8>>,
    env: BTreeMap<String, String>,
    timeout_ms: u64,
}

struct SandboxCommandResponse {
    command_id: SandboxCommandId,
    exit_status: i32,
    stdout_ref: String,
    stderr_ref: String,
    changed_paths: Vec<String>,
    resource_usage: SandboxResourceUsage,
}
```

Stdout and stderr should be streamed to the caller and persisted under:

```text
/services/sandbox/instances/<sandbox_id>/logs/<command_id>.stdout
/services/sandbox/instances/<sandbox_id>/logs/<command_id>.stderr
```

Large outputs should be chunked and referenced by SIFS paths instead of returned
inline.

## Command Environment

The first root image should be a signed tool bundle, not a Linux distro. It
should contain WASM builds or native WASI ports of common tools plus a real
POSIX `sh`.

The command rule is strict:

```text
real tool or no tool
```

No compatibility theater is allowed. Do not substitute a partial custom parser,
toy shell, fake `sed`, fake `awk`, fake `git`, or mostly-compatible command and
present it as the real tool. A deliberately limited Sinomo-specific helper can
exist, but it must have its own name and documentation.

Initial commands:

```text
sh
cat
echo
printf
pwd
ls
mkdir
rmdir
rm
cp
mv
touch
head
tail
wc
sort
uniq
grep
find
sed
awk
cmp
diff
patch
chmod
ln
tar
gzip
sha256sum or b3sum
date
env
true
false
test
```

Nice-to-have after the base:

```text
jq
git, only if a real Git implementation can run inside the sandbox contract
python, pip, bash, or quickjs as larger command packages
curl-like client routed through Sinomo-network policy
```

Start with BusyBox as the first command environment target. BusyBox is useful
because it can provide a compact real userspace. Sinomo should maintain its own
BusyBox build path so the runtime, filesystem assumptions, patches, and
reproducible build inputs are controlled by the project. The current WASIp1
profile keeps BusyBox ash close to upstream: it does not patch
shell command execution, pipeline, here-doc, or redirection semantics to hide
missing runtime behavior. Job control, fd-backed concurrent pipelines,
exec/wait, and arbitrary POSIX
fork/exec remain pending until Sinomo provides the runtime fd/process substrate
they require.
`gzip` is now part of the profile after the BusyBox recipe switched descriptor
moves to WASI `fd_renumber` and enabled decompression support. `chmod` is also
part of the profile after Sinomo added WASI extensions that apply mode changes
to explicit read-write preopens and report real permission bits back to BusyBox
`stat`/`lstat` callers. BusyBox `awk` is now part of the profile for real
in-process awk programs; awk external command features fail with `ENOTSUP`
until Sinomo has a real child-process substrate. Sinomo should still expose
only commands that behave as real tools inside the sandbox contract. A smaller
set of real, tested tools is better than a broad but brittle surface.

### Tool Packaging

Tool bundles should be immutable SIFS artifacts:

```text
/services/sandbox/toolchains/<toolchain_id>/manifest.sobj
/services/sandbox/toolchains/<toolchain_id>/bin/<tool>.wasm
/services/sandbox/toolchains/<toolchain_id>/lib/...
```

The manifest records:

- toolchain id and version,
- default target WASI ABI plus optional per-command ABI overrides,
- hashes for every module,
- hashes and byte sizes for support artifacts under `lib/`, `share/`,
  `include/`, and `etc/`,
- command names,
- required capabilities,
- default resource limits, including memory, wall time, stdin bytes, stdout and
  stderr bytes, workspace bytes, per-command written bytes, and file count,
- provenance and signature.

Sandbox instances pin a toolchain version in their manifest. Resuming a sandbox
uses the same toolchain unless an explicit migration is recorded.

Toolchains, language runtimes, package archives, and large support files should
be replicated through SIFS. Local nodes may cache already-replicated content for
performance, but the source of truth is SIFS. SIFS is expected to host large
files and should be used for this rather than creating a separate local-only
toolchain distribution path.

## SIFS Integration

The sandbox should use `sinomo-dfs` through a virtual filesystem trait:

```rust
trait SandboxFileSystem {
    fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError>;
    fn read(&self, path: &SandboxPath, range: Option<ByteRange>) -> Result<Vec<u8>, SandboxFsError>;
    fn write(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError>;
    fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError>;
    fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError>;
    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError>;
    fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError>;
}
```

The first backend can adapt the existing SIFS path API. It should keep all
sandbox path normalization inside the sandbox crate so WASM tools never see real
host paths.

### Locality

Execution should be scheduled on a node that can efficiently serve the mounted
SIFS paths:

- If the workspace is under `/services`, any eligible compute node can run it.
- If the sandbox mounts `/sinomo/nodes/<node_id>/files`, prefer that owner node.
- If a mounted subtree is unavailable, the command should fail with an
  inspectable `unavailable-offline` or `policy-blocked` reason.

The scheduler should expose the selected node and path locality in the sandbox
manifest.

## Policy And Security

The sandbox is a policy boundary, not only a WASM runtime boundary.

Required policy checks:

- caller may create or resume the sandbox,
- caller may mount each SIFS path,
- command may access network if requested,
- command may write to the target subtree,
- command may read raw content from sensitive paths,
- resource limits are within caller and node policy,
- toolchain is signed and allowed,
- toolchain provenance matches the configured provenance policy,
- every toolchain command module and support artifact is inside an allowed SIFS
  package-cache prefix,
- execution node is allowed to see mounted raw content.

Default deny:

- host filesystem access,
- host process access,
- raw sockets,
- outbound network,
- privileged clocks or randomness beyond WASI-safe defaults,
- mounting local-owned node files from another node without explicit policy.

Resource controls:

- max memory,
- max wall time,
- max input bytes,
- max output bytes,
- max file count,
- max total workspace bytes,
- max written bytes per command,
- max concurrent commands per sandbox,
- max concurrent sandboxes per node.

The cluster policy exposes the two concurrency budgets as
`max_concurrent_commands_per_sandbox` and
`max_concurrent_sandboxes_per_node`. Daemon-backed sandbox command execution
tracks active runs in-process and denies new commands before staging when the
configured budget would be exceeded. Denials emit the normal
`sandbox.policy.denied` event path.

Every denial should emit structured events and be visible in `sandbox doctor`
or `cluster doctor`. Sandbox policy-denial and resource-exhaustion events are
already surfaced by `sinomo sandbox doctor`, both globally and for a specific
sandbox id.

## Network Model

Networking should not be a raw WASI socket pass-through in the first version.

Use explicit Sinomo-mediated APIs:

```text
sandbox http request -> sinomo-network policy check -> outbound request/proxy
```

This keeps all cluster network traffic and policy decisions in Sinomo. It also
lets the runtime support offline and replay-friendly modes.

Initial network capabilities:

- disabled by default,
- allowlisted HTTP(S) fetch,
- optional cluster-internal service calls,
- per-command byte and timeout budgets,
- request/response logs with redaction policy.

Raw TCP/UDP can be revisited later.

## Event And Audit Model

Initial event kinds:

```text
sandbox.instance.created
sandbox.instance.updated
sandbox.instance.resumed
sandbox.instance.rebooted
sandbox.instance.checkpointed
sandbox.command.started
sandbox.command.output
sandbox.command.completed
sandbox.command.cancelled
sandbox.command.failed
sandbox.policy.denied
sandbox.resource.exhausted
sandbox.scheduler.failed
sandbox.mount.added
sandbox.mount.denied
sandbox.toolchain.pinned
sandbox.toolchain.updated
sandbox.file.written
sandbox.network.http
```

Current persistence uses `JsonlEventStore` at:

```text
<sinomo-data-dir>/events/event-log.jsonl
```

Daemon-backed sandbox lifecycle operations append lifecycle events after the
manifest or workspace operation completes. Config update events include
non-secret change metadata such as whether cwd changed and which environment
variable names were set or unset; they do not include environment values.
Daemon-backed toolchain commands persist the effective command environment in
`/logs/<command_id>.record.sobj`, including process-plan step overrides, while
the derived `sandbox.command.started` and final command-status events include
only environment variable names. `sandbox.command.output` events contain
captured raw stdout/stderr chunks. `sinomo sandbox fetch` appends
`sandbox.network.http` after the daemon-mediated fetch stores the response body
and HTTP exchange audit record in the sandbox filesystem.
`sinomo sandbox logs <sandbox-id> <command-id>` decodes that durable command
record before printing raw stdout/stderr and shows cwd, argv, and environment
variable names for the effective invocation.
HTTP audit records and `sandbox.network.http` events store the request method,
host, sandbox workspace root, and a redacted URL containing only scheme, host,
and path; query strings and fragments are dropped before persistence. Failed
post-policy HTTP transport attempts are also lowered into
`sandbox.network.http` from the persisted audit record, with a null response
status and zero response bytes.
Fetch policy denials emit `sandbox.policy.denied` before returning and do not
call the mediated transport.
Daemon-backed sandbox creation appends `sandbox.mount.added` for accepted mount
declarations, and failed mount policy checks emit both `sandbox.policy.denied`
and `sandbox.mount.denied`.
BusyBox import appends `sandbox.toolchain.updated`, and sandbox creation
appends `sandbox.toolchain.pinned` for the manifest's pinned toolchain id.
Local and daemon-backed direct file writes through `sinomo sandbox write`
append `sandbox.file.written` after the sandbox write succeeds, and direct write
quota failures append `sandbox.resource.exhausted` before returning. Local and
daemon-backed direct writes to read-only sandbox paths append
`sandbox.policy.denied` and `sandbox.mount.denied` before returning. Local and
daemon-backed `sinomo sandbox fetch` also append `sandbox.resource.exhausted`
when the mediated response body would exceed the sandbox write/workspace quota,
while retaining the HTTP audit record for the completed network attempt.
Successful local and daemon-backed fetch-to-file writes also append
`sandbox.file.written` for the response body path, in addition to the
`sandbox.network.http` audit event. File-write events include the workspace
root; fetch-to-file file-write events also include the fetch phase plus the HTTP
audit command id and request id so operators can correlate the file mutation
with the network exchange record. Sandbox-scoped policy-denial, mount-denial,
and resource-exhaustion events also include the workspace root.
Daemon-backed command execution emits `sandbox.policy.denied` before returning
policy failures and `sandbox.scheduler.failed` before returning scheduling
failures.

Events should include:

- sandbox id,
- command id,
- caller/session id,
- executing node id,
- SIFS workspace path,
- mounted SIFS paths,
- toolchain id,
- command argv,
- exit status,
- resource usage,
- output refs,
- changed path summary.

Agents should receive raw stdout/stderr chunks as the command runs. Summaries
can be produced as secondary artifacts, but they must not replace raw output in
the agent-facing stream.

The runtime-level streaming hook is implemented through `SandboxOutputSink` on
`SandboxWasiRunOptions` and the staged SIFS runner, and local CLI execution
uses it to write stdout/stderr before printing the command summary. Daemon
toolchain execution streams live output through
`/sandbox/run/toolchain/stream`, and daemon process-plan execution streams live
output through `/sandbox/run/process-plan/stream`; the older
`/sandbox/run/toolchain` and `/sandbox/run/process-plan` routes remain
available for single-response callers. Stream de-duplication keys include the
command id, stream, and offset so process plans do not collapse same-offset
chunks from different steps.
`sinomo sandbox reboot [--daemon]` now provides the agent-session reboot
boundary. Reboot preserves durable workspace state and clears `/tmp` when the
instance uses the default `clear-on-session-start` policy; instances created
with `--tmp retain` or `--keep-tmp` retain `/tmp` across reboot. Daemon-backed
reboot emits `sandbox.instance.rebooted` with the applied temp-policy result.
The sandbox crate has focused local and SIFS tests for this contract: `/work`
survives reboot, `/tmp` is removed from SIFS and recreated behind a fresh epoch
by default, and retained instances keep `/tmp` across reboot.

## CLI Shape

The first CLI should support direct operator testing:

```text
sinomo sandbox create [--daemon] [--id <sandbox-id>] [--toolchain <toolchain-id>] [--memory-mb <n>] [--wall-time-ms <ms>] [--max-input-bytes <n>] [--max-output-bytes <n>] [--max-workspace-bytes <n>] [--max-written-bytes-per-command <n>] [--max-file-count <n>] [--tmp clear|retain] [--keep-tmp] [--mount <sifs-path>:<mount-path>[:ro|rw]]
sinomo sandbox run <sandbox-id> [--daemon] [--stdin] [--timeout-ms <ms>] [--allow-capability <name>] <command> -- [args...]
sinomo sandbox shell <sandbox-id> [--daemon] [--timeout-ms <ms>] [-- <script>]
sinomo sandbox status <sandbox-id> [--daemon]
sinomo sandbox config <sandbox-id> [--daemon] [--cwd <path>] [--env NAME=VALUE] [--unset-env NAME]
sinomo sandbox logs <sandbox-id> [<command-id>] [--daemon]
sinomo sandbox ls <sandbox-id> [path] [--daemon]
sinomo sandbox cat <sandbox-id> <path> [--range <start:end>] [--daemon]
sinomo sandbox write <sandbox-id> <path> [--file <host-path>|--stdin|<literal>] [--daemon]
sinomo sandbox fetch <sandbox-id> <url> <output-path> [--daemon] [--timeout-ms <ms>] [--max-response-bytes <n>] [--header <name:value>]
sinomo sandbox checkpoint <sandbox-id> [--daemon]
sinomo sandbox resume <sandbox-id> <checkpoint-id> [--daemon]
sinomo sandbox reboot <sandbox-id> [--daemon]
sinomo sandbox destroy <sandbox-id> [--daemon]
sinomo sandbox policy show
sinomo sandbox policy set [policy flags...]
sinomo sandbox toolchain add-command [--daemon] [--toolchain <toolchain-id>] [--version <version>] [--target-abi preview1|wasix] [--provenance <text>] [--capability <name>] <command> <module.wasm>
sinomo sandbox toolchain add-artifact [--daemon] [--toolchain <toolchain-id>] [--version <version>] [--provenance <text>] <lib|share|include|etc/path> <file>
sinomo sandbox toolchain import-busybox [--daemon] --version <version> [--provenance <text>] <busybox.wasm>
sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>] <package.json>
sinomo sandbox toolchain verify-package <package.json>
sinomo sandbox toolchain hash <file>
sinomo sandbox toolchain busybox-plan
sinomo sandbox toolchain bash-plan
sinomo sandbox toolchain build-tools-plan
sinomo sandbox toolchain quickjs-plan
sinomo sandbox toolchain python-plan
crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh
sinomo sandbox toolchain verify-bash <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-busybox <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-quickjs <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-python <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-coding <sandbox-id> [--daemon] [--allow-unsigned] [--full]
sinomo sandbox toolchain verify-make <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-tcc <sandbox-id> [--daemon] [--allow-unsigned]
```

`--allow-unsigned` is a local verifier escape hatch only. Daemon-backed
verification is cluster-scoped and must use the cluster sandbox policy, for
example `sinomo sandbox policy set --allow-unsigned-toolchains`.
For `sandbox run`, `--allow-capability` is also local-only and exists for
operator verification of unsigned/local toolchains; daemon-backed runs must use
`sinomo sandbox policy set --allow-capability <name>` so capability decisions
remain cluster-scoped.

Agent-facing tools can wrap the same API:

```text
sandbox.create
sandbox.run
sandbox.read_file
sandbox.write_file
sandbox.list
sandbox.checkpoint
sandbox.resume
```

## Crate Boundaries

Recommended crates/modules:

| Crate | Responsibility |
|---|---|
| `sinomo-sandbox` | Sandbox IDs, manifests, policy checks, command runtime, VFS traits, WASM host integration. |
| `sinomo-dfs` | SIFS path operations, object persistence, policy violation events for DFS paths. |
| `sinomo-network` | Optional mediated outbound network and cluster service calls. |
| `sinomo-cli` | Operator CLI and local testing commands. |
| `sinomo-daemon` | Sandbox service lifecycle, scheduling, command execution endpoint, event emission. |
| `sinomo-storage` | Register sandbox local caches, temporary runtime state, and toolchain cache paths. |

The agent crate should remain general. It should call sandbox tools; it should
not contain Minecraft-specific, shell-specific, or WASM-runtime-specific code.

Within `sinomo-sandbox`, implementation ownership is split so new WASIX and
toolchain work does not collect in facade modules:

- `src/runtime/preview1/` owns the Wasmtime/WASI Preview1 runner and bounded
  Preview1 compatibility imports.
- `src/runtime/wasix/` owns the Wasmer/WASIX runner, process-tree signaling,
  output capture, static memory-budget checks, and WASIX preopen/cwd handling.
- `src/execution/local.rs` owns local staged single-command execution;
  `src/execution/local_process_plan.rs` owns local structured process-plan
  execution; `src/execution/local_fidelity.rs` owns local toolchain verifier
  runner helpers; `src/execution/sifs.rs` owns SIFS-backed staged
  single-command execution, placement, and mount sync; and
  `src/execution/sifs_process_plan.rs` owns SIFS-backed structured
  process-plan execution.
- `src/execution/staging/` owns temporary local runtime staging for SIFS mounts,
  pinned toolchain runtime mounts, content-addressed toolchain cache staging,
  SIFS mount copyback, and its local cache verifier tests.
- `src/filesystem/local.rs` and `src/filesystem/sifs.rs` own the corresponding
  virtual filesystem backends, with shared path/error helpers in
  `src/filesystem/support.rs`.
- `src/toolchain/local.rs` and `src/toolchain/sifs.rs` own local and SIFS
  toolchain stores; manifests, built-in BusyBox registration, SOBJ serde, and
  shared hash/bundle/validation helpers stay in sibling toolchain modules.
- `src/policy/core.rs`, `src/events/envelope.rs`, `src/http/protocol.rs`, and
  `src/fidelity/cases.rs` keep policy, event, protocol, and verifier case-table
  ownership out of the crate facade modules.

## Storage Registration

`sinomo-sandbox` should register its own local storage paths:

```text
sandbox/cache/toolchains
sandbox/cache/modules
sandbox/runtime
sandbox/tmp
```

These are local caches and runtime state. Durable workspace contents belong in
SIFS under `/services/sandbox/...`.

`sinomo-events` registers the node-local durable event ledger:

```text
events/event-log.jsonl
```

Sandbox event helpers write cluster event envelopes through the generic
`EventStore` trait, so tests can use in-memory stores and daemon-backed
operation can use the durable JSONL store without changing the sandbox event
format.

Toolchain caches are only caches. The complete toolchain artifacts should be
addressable and replicated through SIFS.

Storage contract violations should be reported as node health findings, not as
startup blockers. The current CLI path implements this as install-time warnings
and `sinomo cluster doctor` warning findings through
`data_dir_storage_contract_findings`; unregistered sandbox-local paths therefore
degrade diagnostics without preventing the daemon or install path from starting.

## Implementation Phases

### Phase 0: Architecture And API Skeleton

- Add this design as the planning document.
- Define `SandboxId`, `SandboxCommandId`, manifest structs, resource limit
  structs, and error types.
- Add `sinomo-sandbox` storage registration.
- Add empty CLI command group and help text.
- Add unit tests for path normalization and policy denial shapes.

### Phase 1: Local WASI Command Runner

- Integrate Wasmtime.
- Run one WASM command with preopened in-memory or temporary directories.
- Capture stdout, stderr, exit status, timeout, and memory limit failures.
- Implement cancellation.
- Package one or two proof commands, such as real `echo` and real `cat`.
- Add tests for no host path escape, timeout, and output limits.

### Phase 2: SIFS-Backed Workspace

- Implement `SandboxFileSystem` on top of the existing SIFS path API.
  Local and SIFS filesystem backends now expose `/dev`, `/dev/null`, bounded
  reads from `/dev/zero`, and random reads from `/dev/random` and `/dev/urandom`
  as virtual paths without persisting device files into SIFS. Writes to virtual
  device sinks bypass workspace write quotas and do not emit durable file-write
  events. Staged WASI command execution now installs a per-run Sinomo-owned
  `/dev` preopen for `/dev/null`, bounded `/dev/zero`, and seeded random
  devices. A host symlink staging approach is explicitly not accepted: the WASI
  runner has a regression test that confirms symlink escape to host `/dev/null`
  does not succeed.
- Persist sandbox manifests under `/services/sandbox/instances/<sandbox_id>`.
- Persist command logs and changed files through SIFS.
- Persist `/tmp` as durable sandbox instance state through SIFS.
- Implement `sandbox create`, `sandbox run`, `sandbox status`, and
  `sandbox logs`.
- Add resume by reloading the manifest and workspace path.
- Add session reboot semantics that keep instance state durable while applying
  the `/tmp` policy.

Acceptance test:

```text
create sandbox
write file from command
restart daemon
resume sandbox
read file from another node
```

### Phase 3: Minimal Real POSIX Command Environment

- Add a Sinomo-maintained BusyBox build as the first userspace target, with its
  real applets first. BusyBox ash is not exposed in the default profile until descriptor and process support exist; runtime gaps must be fixed below ash rather than through shell-behavior patches.
- Add the core tools listed in the command environment section as each command
  builds and passes fidelity inside the sandbox contract.
  The current BusyBox WASIp1 profile includes `cat`, `cp`, `cmp`, `date`, `diff`,
  `awk`, `echo`, `env`, `false`, `find`, `grep`, `gzip`, `head`, `ln`, `ls`, `mkdir`,
  `mv`, `patch`, `printf`, `pwd`, `rm`, `rmdir`, `sed`, `sha256sum`, `sort`,
  `tail`, `tar`, `test`, `touch`, `true`, `uniq`, `wc` and `chmod`;
  the BusyBox fidelity declaration now requires every exposed applet to have at
  least one direct invocation case before it remains in the default manifest.
  BusyBox `awk`
  runs real awk programs, while pipe redirections and `system()` remain bounded
  by the sandbox's current incomplete fd/process substrate.
- Build or vendor WASI-compatible tool modules with reproducible hashes.
- Add host-side tool fidelity tests before exposing each command name.
- Store toolchain manifest in SIFS.
- Pin toolchain id in sandbox manifest.
- Keep CLI `sandbox shell` backed by the pinned real `sh` command; do not add a
  fake shell fallback.
- Add CLI `sandbox policy show/set` for the typed cluster KV policy.
- Use `crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` as the local
  end-to-end gate for the first composite coding-task toolchain. The gate now
  includes the command workflow from the acceptance test, extended with
  copy/move/compare, chmod, tar, rm/rmdir, sed, awk, sort, wc, head, and tail
  checks, before the QuickJS/BusyBox cross-tool workflow. With `VERIFY_FULL=1`
  the same script also verifies Python/pip on the WASIp1 coding workspace and
  verifies Bash, Make, and TCC on a separate WASIX shell/build workspace.

Acceptance test:

```text
mkdir /work/a
touch /work/a/file
printf hello
grep hell from stdin
find /work -type f
diff /work/a/file /work/a/file
resume on another node and inspect outputs
```

### Phase 4: Policy And Node Scheduling

- Add mount declarations and policy checks.
- Add read-only/read-write mount enforcement.
- Add node locality selection for mounted SIFS paths.
- Load resource budgets from cluster KV config in daemon-scheduled execution.
  The daemon `/sandbox/run/toolchain` route now loads the cluster sandbox policy
  and runs pinned SIFS toolchain commands through the SIFS staged runner.
- Emit sandbox event families.
  The durable JSONL event store and daemon-backed lifecycle, command completion,
  command start, captured output, cancellation/resource status mapping,
  active-execution concurrency denial, persisted config updates, and
  mount/toolchain/network HTTP event appends are implemented; daemon-backed
  session reboot emits
  `sandbox.instance.rebooted`; true live output streaming exists at the runtime
  sink layer, local CLI layer, and daemon streaming RPC layer for both single
  pinned commands and structured process plans.
- Add `sandbox doctor` or extend `cluster doctor`.
  `sandbox doctor` now uses a runtime capability report for planned virtual
  devices (`/dev/null`, `/dev/zero`, `/dev/random`, and `/dev/urandom`) and
  runtime budget guards including direct stdin input limits; those devices are
  implemented for staged WASI command execution through a per-run Sinomo-owned
  `/dev` preopen, so the doctor no longer reports them as missing. Missing
  planned MVP commands that are known substrate gaps are reported with explicit
  reasons; `sandbox doctor` also emits an error-level
  `mvp-command-surface-incomplete` finding while any required MVP command is
  missing. The runtime capability report still includes
  `child-process-exec` for each ABI; Preview1 reports it as unsupported, while
  WASIX reports it as available when the crate is built with `wasix-runtime`.
  `sandbox doctor` surfaces unsupported runtime capabilities as warnings so the
  remaining shell/process limits are explicit even when no sandbox command is
  being run. SIFS staged execution rejects remote
  local-owned mounts before creating a local staging workspace unless policy
  explicitly allows remote local-owned mounts. Recent policy-denial audit
  events are surfaced both for a specific sandbox and globally;
  toolchain/package policy denials that are not tied to one sandbox are reported
  as cluster sandbox service denials. Daemon-backed direct writes to read-only
  sandbox mounts are also rejected without mutating the SIFS source and append
  both `sandbox.policy.denied` and `sandbox.mount.denied` records with
  `mount-read-only-denied`.

Acceptance test:

```text
mount /services/sandbox/test as /work rw
mount /sinomo/nodes/<other>/files as /host ro
verify denied write to /host
verify policy event
```

### Phase 5: Network And Package Fetching

- Add allowlisted HTTP fetch through Sinomo-network. The sandbox crate now has
  the typed HTTP request/response contract, default-deny policy validation,
  host allowlists, byte/timeout budgets, redacted exchange records, durable
  SOBJ log persistence, recording fetch helper, response-body file writes
  tested against local and SIFS filesystems, replica-node readback coverage for
  fetched bodies and audit records, and `sandbox.network.http` event lowering.
  `sinomo sandbox fetch` sends HTTP requests to the local daemon for mediated
  egress, and `sinomo sandbox fetch --daemon` lets the daemon validate cluster
  policy, store the response body in SIFS, persist the HTTP audit record, and
  append the network event.
- Add `curl`-like command backed by mediated network API. `sinomo sandbox
  fetch` now parses URL/output/limits/header options and executes against the
  sandbox HTTP transport abstraction, with the production transport connected to
  daemon-mediated routing.
- Record network request refs with redaction policy. The durable record format
  is used by `sinomo sandbox fetch` after the daemon-mediated HTTP response is
  returned, so response bytes and audit metadata are stored with the sandbox.
  HTTP audit records and `sandbox.network.http` events retain only scheme,
  host, and path from the URL; query strings and fragments are dropped before
  persistence. The CLI also appends the corresponding `sandbox.network.http`
  event to `events/event-log.jsonl`.
- Keep raw sockets disabled.
  The local WASI runner now explicitly disables TCP, UDP, IP-name lookup, and
  socket address checks in the Wasmtime WASI context; sandbox networking remains
  available only through mediated `sinomo sandbox fetch`/daemon HTTP paths. A
  regression test runs a WASI module that calls `sock_send` directly and
  verifies that the syscall cannot succeed. The WASIX runner preflights module
  imports and rejects raw `wasix_http_client_v1` imports before execution so
  process-capable toolchains cannot bypass Sinomo-mediated HTTP policy, while
  still allowing socket/port imports that WASIX libc may include for local
  shell/process support.

Acceptance test:

```text
network disabled -> fetch denied
allowlist enabled -> fetch succeeds
response body stored in SIFS
audit event includes URL policy metadata
```

### Phase 6: Richer Toolchains

- Add required shell and language runtimes as toolchain packages.
  `sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>] <package.json>` now
  imports a declared package containing real command modules and support
  artifacts, or merges it into an existing pinned toolchain when `--into` is
  supplied. Each package entry must declare a BLAKE3 content hash, and import
  rejects hash mismatches plus absolute or escaping host paths before installing
  anything. A package-level `target_abi` is the default for its commands, and
  each command may declare its own `target_abi` when a merged coding toolchain
  needs to mix Preview1 and WASIX commands.
  `sinomo sandbox toolchain verify-package <package.json>` runs the same
  manifest, file path, duplicate-entry, and hash validation without
  importing, so package recipes can be checked before SIFS writes. This lets a
  coding-task toolchain combine the BusyBox file/search applets with QuickJS,
  Bash, Python, pip, tinycc, or selected build tools once their real sandbox
  builds are available. Bash, Python, and pip are required for coding-task
  readiness and `sandbox doctor` reports them as error-level missing commands
  until real packages and fidelity suites exist.
  `crates/sinomo-sandbox/toolchains/quickjs` now provides the first pinned
  package recipe, generating a QuickJS `qjs` WASIp1 command module package for
  that importer. `crates/sinomo-sandbox/toolchains/bash` now provides the
  pinned Bash WASIX recipe target. The recipe now produces a real module and
  the full Bash verifier passes locally and through a daemon-local SIFS-backed
  sandbox when cluster policy explicitly allows `child-process-exec` and
  `wasix.unpinned-child-exec`.
  `crates/sinomo-sandbox/toolchains/python`
  now contains pinned CPython recipes for the older Preview1 pure-Python
  regression path and the active WASIX native-extension path. The WASIX recipe
  builds real CPython with zlib, standard-library/support artifact staging,
  bundled pip/setuptools, WASIX process support, and dynamic side-module loading
  for compiled Python wheels. The `python` and `pip` WASIX package commands
  declare `child-process-exec` plus `wasix.unpinned-child-exec`; policy must
  allow those capabilities before the commands can run. The active
  native-extension verifier
  is `crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh`;
  it imports the BusyBox WASIX, Bash WASIX, and CPython WASIX packages, seeds a
  WASIX wheelhouse into `/work/wheelhouse`, verifies direct Python subprocesses
  plus `shell=True` through the pinned `/bin/sh`, installs `numpy` and
  `matplotlib` with real pip, verifies NumPy array execution, and verifies
  matplotlib Agg rendering to a PNG under `/work`. Set
  `VERIFY_BACKEND=daemon-local` to run that same native-extension gate through a
  disposable single-node daemon and SIFS-backed sandbox state.
  The daemon-local native-extension gate has passed end to end. The supporting
  SIFS path now has dedicated regressions for recursive-clear tombstone recovery
  and self-only eventual-write fast commits so large Python site-packages trees
  remain practical without weakening distributed writes that target remote
  replicas.
  The older local `verify-python` gate now
  passes for basic interpreter probes, `mmap` import, `pip --version`, offline
  wheel creation for the maintained pure-Python package fixture, offline
  install from that wheel cache into `/work/site-packages`, installation through
  a local PEP 503-style `file:///work/simple` package index into
  `/work/index-site-packages`, and importing those installed packages; the wheel
  cache lives under `/work/pip-cache`, and the post-resume verifier requires
  wheel files to remain present there. In
  daemon-local mode the same gate now starts a local package source, fetches a
  PEP 503-style project index pages plus top-level and dependency wheels
  through the mediated `sandbox fetch --daemon` HTTP path into `/work/simple`
  and `/work/packages`, installs the top-level package with real pip from
  `file:///work/simple` so pip resolves the dependency from that fetched index,
  first verifies sandboxed pip cannot reach the HTTP package source directly
  without the mediated fetch step, verifies pip selects the newest compatible
  dependency when the fetched simple index exposes multiple versions, verifies
  a dependency-of-dependency is resolved from the same fetched index, verifies
  a dependency whose declared package name requires PEP 503 normalization,
  verifies environment markers skip an incompatible dependency, verifies a
  `data-requires-python` link excludes an otherwise newer incompatible wheel,
  verifies yanked releases are not selected while a non-yanked compatible wheel
  exists, verifies a constraints file can pin an older compatible dependency,
  verifies hash-checked requirements pin every transitive wheel and deny a
  mismatched hash, verifies extras pull an optional dependency from the same
  mediated index, and imports the installed packages.
  The daemon-local policy fixture raises
  `max_http_timeout_ms` with `sinomo sandbox policy set --max-http-timeout-ms`
  so the verifier is exercising the same timeout enforcement as production
  mediated fetches. The same gate previously required local C-extension
  package failure so the native-extension
  boundary stayed explicit; the active POSIX/process gate replaces that
  boundary with successful WASIX native-extension loading for ordinary Python
  wheels. The dynamic linker now mirrors side-module table entries back into
  the main module table for callbacks crossing module boundaries and applies a
  generic Binaryen fpcast compatibility transform to dynamically loaded WASIX
  side modules so C function-pointer casts used by native extensions can run
  without package-specific patches. When the main module uses the fpcast
  universal ABI, loading a dynamic side module now fails early if the Binaryen
  `wasm-opt` transformer is unavailable instead of silently continuing into a
  later indirect-call trap. Python is still partial for broader remote
  package-index resolver edge cases, but the current gate requires native
  extension wheels to work.
  `crates/sinomo-sandbox/toolchains/python/verify-python-wasi.sh`
  remains the Preview1 pure-Python regression gate: it builds or reuses the
  pinned package, validates the
  manifest, runs `verify-python`, checkpoints and resumes the sandbox, and
  verifies the installed package still imports from `/work/site-packages`. The
  gate defaults to local staging and can run the same flow against daemon-backed
  SIFS state with `VERIFY_BACKEND=daemon`; `VERIFY_BACKEND=daemon-local` creates
  a disposable single-node daemon/SIFS root and now passes the full Python and
  pip flow, checkpoint/resume, and post-resume import check. Development
  clusters can opt in to policy updates for this unsigned package with
  `UPDATE_CLUSTER_POLICY=1`. The sandbox crate also
  includes
  `crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh`
  for the active POSIX/Python gate: it imports the CPython WASIX package into a
  disposable local sandbox together with BusyBox WASIX and Bash WASIX shell
support, seeds a WASIX wheelhouse into `/work/wheelhouse`, verifies Python
`subprocess.run` can execute a sandbox-local WASM module staged under `/work`,
verifies Python `Popen(...).communicate(...)` file descriptor behavior,
verifies `subprocess.run(..., timeout=...)` raises `TimeoutExpired`, verifies
Python `shell=True` runs through the pinned `/bin/sh`, runs real pip installs
for `numpy` and `matplotlib`, verifies NumPy array execution, and verifies
matplotlib Agg rendering to a PNG under `/work`. The
  same script can run against a disposable daemon/SIFS backend with
  `VERIFY_BACKEND=daemon-local`.
  The sandbox crate also
  maintains an explicit richer-toolchain target list: QuickJS is implemented;
  Bash and Python have maintained packages and verifier gates; pip has verified
  offline wheel build/install, local simple-index install, SIFS-backed
  wheel-cache resume, and daemon-local mediated simple-index dependency
  acquisition, direct ambient-HTTP denial, compatible-version selection,
  normalized package names, environment markers, constraint pinning,
  requires-python link filtering, yanked release filtering, hash-checked
  requirements with mismatch denial, extras, and one transitive dependency
  through the sandbox HTTP path. Native-extension support is now
  represented by the active NumPy/matplotlib gate instead of by an expected
  failure test, while broader remote package-index resolver edge cases remain
  pending.
  The active gate now requires successful `numpy` and `matplotlib`
  installation/import/execution rather than treating native-extension failure as
  an acceptable boundary;
  TinyCC has a real WASIX package recipe plus `verify-tcc` compiler fidelity
  for version output, preprocessing, object generation under `/work`,
  invalid-input diagnostics, and explicit unsupported `-run` execution; and
  GNU Make has a
  real WASIX package recipe plus `verify-make` workflow fidelity for Makefile
  parsing, variable expansion, dependency ordering, failed recipe status, and
  recipe command execution through pinned sandbox `sh`/BusyBox commands. The
  combined `verify-build-tools-wasix.sh` gate builds and verifies both
  build-tool packages. Make is subject to the current WASIX shell/process
  limits and cluster policy, while TinyCC currently compiles x86_64 object
  artifacts and does not execute compiled programs inside the sandbox.
  `sandbox doctor` reports implemented targets when missing from the pinned
  toolchain, reports required targets as errors, and reports planned targets as
  readiness information instead of pretending they are available.
- Consider Bash, Python, QuickJS, tinycc, or selected build tools.
  QuickJS is the first concrete target. Bash, Python, and pip are now required
  coding-sandbox targets. Bash now runs through the WASIX substrate for
  internal shell cases and external pinned BusyBox WASIX commands, with local
  and daemon-local SIFS-backed verifier coverage under explicit cluster policy.
  Python now has a CPython WASIX
  package recipe with full standard library/support artifact staging, zlib,
  dynamic side-module loading, POSIX process support through WASIX, and a pinned
  pip/setuptools environment. Pip now passes the native-extension gate for
  local wheelhouse installs: `numpy` imports and runs representative array
  operations, and `matplotlib` imports with the Agg backend and renders a figure
  file inside the sandbox. The older CPython WASIp1 gate still documents
  pure-Python package and daemon/SIFS checkpoint coverage, but native-extension
  readiness is no longer represented by an expected failure test.
  It now stores pip wheel cache state under `/work/pip-cache` and verifies that
  cache after checkpoint/resume. The daemon-local gate also verifies mediated
  HTTP acquisition of simple-index pages and wheels into `/work/simple` and
  `/work/packages` before pip installs from that fetched index, including a
  direct ambient-HTTP denial check, a multiple-version dependency case where
  pip must select the newest compatible wheel, an environment-marked dependency
  that must be ignored, a normalized package-name dependency, a
  requires-python link filtering case, a constraints file pinning an older
  compatible dependency, a yanked release filtering case, a hash-checked
  requirements install plus hash-mismatch denial, an extras install that pulls
  an optional dependency, and a transitive dependency fetched through the same index.
  Broader remote package-index resolver edge cases and successful
  native-extension behavior, including `numpy` and `matplotlib`, are the current
  production-readiness gate for pip and Python.
  TinyCC now has a WASIX build recipe and a `verify-tcc` compiler suite; GNU Make has a WASIX build recipe and a `verify-make` workflow suite. The combined build-tools gate imports and verifies both packages through `verify-build-tools-wasix.sh`.
- Add generic SIFS-backed toolchain command installation. Implemented through
  `/sandbox/toolchain/add-command` and `sinomo sandbox toolchain add-command
  --daemon`, which stores real WASI command modules in
  `/services/sandbox/toolchains/<toolchain_id>/bin/`, updates the manifest,
  and records per-command capability requirements plus default resource limits
  for policy validation.
- Add package support-file storage. `SandboxToolchainManifest` now records
  non-command support artifacts under `lib/`, `share/`, `include/`, and `etc/`,
  and both local and SIFS toolchain stores persist and hash-check those files.
  `/sandbox/toolchain/add-artifact` and `sinomo sandbox toolchain add-artifact
  --daemon` expose the SIFS-backed installation path so richer language
  runtimes can keep their replicated package contents in SIFS. Daemon package
  imports can also store a large support tree as a single SIFS artifact bundle
  while retaining per-file artifact metadata and hashes in the manifest; runtime
  staging expands the bundle into a content-addressed local execution cache
  under the sandbox runtime root. This keeps SIFS as the durability source
  without turning one language runtime import into thousands of DFS object
  writes or re-copying the expanded runtime tree for every command.
- Add package-cache policy and provenance checks.
  The cluster sandbox policy now supports toolchain provenance prefix
  allowlists, exposed through `sinomo sandbox policy set
  --allow-toolchain-provenance-prefix <prefix>`, and applies them during
  complete toolchain invocation validation. Package-cache prefixes are also
  allowlisted through `sinomo sandbox policy set --allow-package-cache-prefix
  <sifs-prefix>`, defaulting to `/services/sandbox/toolchains`; every command
  module and support artifact path in a toolchain manifest must stay under an
  allowed package-cache prefix.
- Replicate complete toolchain artifacts through SIFS.
  SIFS toolchain stores now accept explicit replica candidates and tests verify
  that a replicated command module plus support artifact can be read from a
  replica node after metadata reload. Daemon-backed sandbox writes, instance
  lifecycle updates, toolchain imports, and staged command writeback now derive
  replica candidates from DFS content-service advertisements in the cluster
  control plane, falling back to the local node when no cluster view is
  available.
- Evaluate whether any commands need a Linux syscall compatibility layer. Do
  not add one unless real workflows require it.

## Settled Decisions

| Decision | Resolution |
|---|---|
| Sandbox instance root | `/services/sandbox` |
| `/tmp` durability | Durable for the sandbox instance; cleared by default on agent session reboot; retention across sessions is an instance policy flag; deleted when the sandbox instance is destroyed |
| First userspace target | Sinomo-maintained BusyBox build |
| Shell model | Real shell support uses the WASIX Bash package for verified pipelines, redirection, descriptor duplication, command substitution, shebang execution, and sandbox-local child exec; full fork/exec shell parity, exec/wait, fd-backed concurrent pipelines, and job control still wait for the fd/process substrate; structured shell is out of scope for now |
| Command policy | Real, fully functional Linux/POSIX tools or no tool; no fake substitutes |
| Sandbox interior permissions | Agent has full permission inside the sandbox instance |
| Initial workload target | Coding tasks with language toolchains available through sandbox toolchain packages |
| Toolchain storage | Replicate complete toolchains through SIFS |
| Agent command stream | Raw stdout/stderr chunks |

## Remaining Open Questions

No product-level open questions remain for the MVP. The current BusyBox route
is a conservative WASI preview1 build with local compatibility headers and direct applets only. Remaining implementation work is to add a real
fd/process substrate before claiming job control, arbitrary POSIX fork/exec, or other process-oriented tools, and to
expose additional initial commands only
after their real implementations pass the verifier.

## Near-Term Recommendation

Start with a narrow but real MVP:

```text
Wasmtime + SIFS workspace + Sinomo-maintained BusyBox direct applets + real file/search tools + language toolchain packages + policy-controlled /tmp + raw output streaming
```

Avoid host binary execution and avoid exposing fake tools. A Linux syscall
compatibility layer or WASI preview2/process model is required before claiming
real shell/process semantics. The highest-risk integration is not WASM execution
itself; it is the SIFS-backed VFS semantics, instance lifecycle, raw output
streaming, and reliable resume across node restarts. Prove those first.

## References

- WebVM repository: https://github.com/leaningtech/webvm
- WebVM 2.0 overview: https://labs.leaningtech.com/blog/webvm-20.html
- SIFS architecture: `docs/sifs.md`
- DFS architecture: `docs/dfs-architecture.md`
