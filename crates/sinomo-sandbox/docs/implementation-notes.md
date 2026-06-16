# Sandbox Implementation Notes

This document is the detailed implementation record for `sinomo-sandbox`. Current implementation status, remaining gaps, and detailed design notes belong here.

## Reading Guide

- Start with the current implementation snapshot for the active runtime, filesystem, toolchain, and shell status.
- Use the remaining gaps section as the working backlog for POSIX, SIFS, operations, and verification work.
- Use the detailed design notes as the implementation record. They intentionally keep lower-level details out of the crate README.
- Use the appendices for settled decisions, open questions, near-term recommendations, and references.

## Current Implementation Snapshot

### Baseline

- WASIX is the maintained sandbox runtime. Preview1 support has been removed instead of retained as a compatibility path.
- Local and daemon-backed SIFS sandbox operations are wired through the CLI for create, status, config, run, shell, logs, file inspection, file writes, HTTP fetch, checkpoint, resume, reboot, destroy, toolchain import, and policy.
- Instance manifests, command records, stdout/stderr logs, checkpoints, toolchains, support artifacts, and package manifests are stored as structured sandbox state.
- Daemon-backed operation writes sandbox state through SIFS under `/services/sandbox`.
- The default policy lets a sandbox use available host memory and does not set wall-time or workspace-size limits unless cluster policy overrides them.
- Ambient WASIX HTTP is denied. Network fetches go through the mediated sandbox fetch path and produce audit records.

### Runtime

The WASIX runner currently provides:

- mapped virtual preopens for workspace, toolchain mounts, `/dev`, and `/proc`,
- stdin/stdout/stderr capture and streaming,
- process-tree timeout and cancellation signaling for yielding process trees,
- memory-budget checks through Wasmer tunables,
- instruction metering when a module shape supports it,
- raw WASIX HTTP import denial,
- generated `/dev/null`, `/dev/zero`, `/dev/random`, `/dev/urandom`, stdio aliases, `/dev/fd`, and `/proc/self/fd` style entries,
- generated procfs views for common self, task, mount, fdinfo, cwd/root/exe, argv, environment, and status probes,
- partial process spawn, exec, wait, signal, process group, session, TTY, cwd, environment, and PATH behavior,
- close-on-exec descriptor cleanup is applied before normal WASIX module start and before built-in WASIX command dispatch, and a runtime regression verifies a `CLOEXEC` descriptor is unavailable to a spawned child,
- root virtual path traversal resolves preopen aliases in both component form (`work`) and absolute-alias form (`/work`), so cwd and stat-style operations share the same virtual namespace as open-style operations,
- `proc_join` reports normal child exit status and reaps joined child processes so repeated joins on the same child fail,
- built-in WASIX commands that finish inside the runtime, such as the `/bin/wasmer` help path, now run normal process-exit cleanup before their join handle is reported finished, so child process records and inherited descriptors are not left live,
- failed WASIX spawn setup paths for child cwd, stdio setup, and `proc_spawn2` fd actions now run child environment cleanup before the forked process record is removed,
- `proc_spawn2` cwd fd actions now apply runtime-backed `Chdir` and `Fchdir` updates to the child environment before module launch, including process metadata synchronization for fd-based cwd changes,
- forked `proc_spawn` and `proc_spawn2` children that fail during executable loading, built-in dispatch, or module launch are now marked terminated before their process record is removed, so failed child creation does not leave a live thread status behind,
- non-vfork `proc_exec3` failures now restore the caller's argv, environment, executable-path metadata, FD table, preopen FD list, current directory, and union bookkeeping before returning the exec error, matching the POSIX expectation that a failed `execve` leaves the caller running with its original process metadata and descriptor state,
- `proc_fork_env` plus failed `proc_exec3` is covered by a runtime regression that verifies the vfork-style child can return from the failed exec, exit via `proc_exit2`, restore the parent environment, and be reaped by the parent,
- process-group signaling is covered by a runtime regression that moves a spawned child into its own group, sends `SIGKILL` to the negative process group id, and observes signal termination through `proc_join`,
- process-group stop reporting is covered by a runtime regression that sends `SIGSTOP` to a child group, observes `JoinStatusType::Stopped` through `proc_join` without reaping the child, then resumes and kills the same child,
- `proc_setpgid` now rejects session leaders, and a runtime regression covers the initial process/session leader case so shell job-control setup cannot silently accept an invalid process-group transition,
- continued child status is reported for resumed stopped children, can be observed without consuming it, and can then be consumed through the WASIX continued-status syscall; blocking wait-status paths re-check after subscription so stop/continue/exit events are not lost between the pre-check and wait registration,
- WASIX compatibility aliases for `process_spawn`, `pipe`, `pipe2`, `kill`, `raise`, `unshare`, `setns`, and `eventfd`; plain POSIX process/session names such as `getpid`, `getppid`, `getpgrp`, `getpgid`, `getsid`, `setpgid`, `setsid`, `tcgetpgrp`, `tcgetsid`, and `tcsetpgrp` now use direct POSIX-shaped return values while the `proc_*` names retain the WASIX pointer-return ABI,
- POSIX `kill` and `raise` wrappers now return `0` or negative errno while legacy `proc_signal` and `proc_raise` keep their WASIX errno-return shape,
- POSIX identity aliases for `getuid`, `geteuid`, `getgid`, `getegid`, `getresuid`, `getresgid`, `setuid`, `seteuid`, `setgid`, `setegid`, `setresuid`, `setresgid`, `getgroups`, and `setgroups`,
- POSIX exec vector wrappers for `execv`, `execve`, `execvp`, and `execvpe`; these read normal null-terminated argv/envp vectors and lower them into the WASIX exec path,
- POSIX `waitpid` wrapper that maps WASIX child wait results into conventional integer wait status values and supports `WNOHANG`, `WUNTRACED`, and `WCONTINUED`,
- POSIX `waitid` status-selection wrapper that returns child pid, signal, `CLD_*` code, and status scalar fields for libc `siginfo_t` construction,
- POSIX `fork` and `vfork` wrappers that return 0 in the child, the child pid in the parent, and negative errno values on failure; modules that lack the asyncify unwind/rewind hooks fail before any child process is created,
- Linux `clone` thread compatibility for `CLONE_THREAD | CLONE_VM | CLONE_SIGHAND` callers; the runtime creates a same-process WASIX thread, shares memory, writes parent/child TID slots when requested, and resumes the cloned thread from the asyncified syscall continuation with return value zero,
- POSIX/Linux process and thread compatibility exports for `_exit`, `gettid`, `set_tid_address`, `get_clear_child_tid`, `set_robust_list`, `get_robust_list`, `getrlimit`, `setrlimit`, `prlimit`, `getrusage`, `getpriority`, `setpriority`, `nice`, `sched_getscheduler`, `sched_getparam`, `sched_setscheduler`, `sched_setparam`, `sched_yield`, `sched_get_priority_max`, `sched_get_priority_min`, `sched_rr_get_interval`, `sched_getaffinity`, `sched_setaffinity`, `sched_getcpu`, `clock_gettime`, `clock_getres`, `sleep`, `usleep`, `nanosleep`, `clock_nanosleep`, `arch_prctl`, `membarrier`, `rseq`, `tkill`, and `tgkill`,
- Linux pidfd compatibility exports for `pidfd_open`, `pidfd_send_signal`, `pidfd_getfd`, `pidfd_status`, and `pidfd_pid`, with descriptor ownership tracked in WASIX runtime state rather than CPython-local tables,
- Linux timerfd compatibility exports for `timerfd_create`, `timerfd_settime`, `timerfd_gettime`, `timerfd_read`, `timerfd_ready`, `timerfd_remaining_ms`, and `timerfd_known`, with timer state tracked in WASIX runtime state rather than CPython-local tables,
- POSIX interval timer compatibility exports for `timer_create`, `timer_settime`, `timer_gettime`, `timer_getoverrun`, and `timer_delete`, with timer lifecycle and scheduling state tracked in WASIX runtime state rather than CPython-local tables; libc timer helper state now freezes the delivered overrun count, prevents duplicate delivery when a blocked timer signal is unmasked, and disarms process-owned timer helpers during normal exit cleanup,
- POSIX process interval timer compatibility exports for `getitimer`, `setitimer`, `alarm`, and `ualarm`, with process timer state tracked in WASIX runtime state rather than CPython-local tables; pending-operation checkpoints now convert due interval timers into normal deliverable process signals before popping the thread signal queue, so blocking futex-backed waits can be interrupted by `SIGALRM`; `alarm()` now lowers through the POSIX timer path so blocked `SIGALRM` can be consumed by `sigwait`,
- POSIX alternate signal stack compatibility exports for `sigaltstack`, with active stack state tracked in WASIX runtime state rather than CPython-local globals,
- POSIX signal mask compatibility exports for `sigprocmask` and `pthread_sigmask`, with each WASIX thread tracking its own active signal mask in runtime state rather than sharing a CPython-global mask; blocked pending signals stay pending for `sigwait`/`sigpending`, unblocked delivery no longer double-consumes runtime pending signals, and signal handlers temporarily apply the action mask plus the current signal unless `SA_NODEFER` is set,
- Linux `rt_sigreturn` compatibility returns successfully because the current signal delivery path invokes handlers synchronously instead of building a kernel signal frame,
- Linux `FUTEX_FD` compatibility creates a runtime-backed `eventfd` that is notified by futex wake paths for legacy pollable futex users,
- POSIX semaphore support covers unnamed local semaphores, pshared/runtime-backed semaphores, and named `sem_open`/`sem_close`/`sem_unlink` lifecycle state, including unlink-deferred destruction while references remain open,
- Linux signalfd compatibility exports for `signalfd`, `signalfd_known`, `signalfd_mask_low`, and `signalfd_mask_high`, with descriptor mask ownership tracked in WASIX runtime state while libc-specific signal record formatting remains in the CPython compatibility layer,
- WASIX TTY/job-control compatibility aliases for `tcgetpgrp`, `tcgetsid`, and `tcsetpgrp`,
- WASIX thread-local key create/destroy/set/get storage with process-global keys and per-thread values,
- thread-local destructor callbacks are invoked during normal WASIX thread exit, with bounded repeated passes for destructors that install new values,
- explicit `thread_exit` from a WASIX worker still runs normal thread cleanup, including thread-local destructor callbacks,
- spawned WASIX worker threads mark their task status finished when the `wasi_thread_start` callback returns, so `thread_join` wakes without relying on process timeout or termination,
- imported shared-memory WASIX modules can spawn and join worker threads through `thread_spawn_v2`/`thread_join`, matching the memory shape expected from threaded Clang/pthread outputs,
- imported shared-memory WASIX modules can block a worker in `futex_wait`, wake it from another thread through `futex_wake`, and move waiters between futex addresses through `futex_requeue`,
- imported shared-memory WASIX modules can wait on multiple futex addresses through `futex_wait_any` and report the index of the address that was woken,
- joined non-main WASIX threads are reaped from the process table so thread metadata and task-count guards do not linger for the rest of the process.
- cancellation wakes WASIX commands blocked in futex deep sleep, including the case where the main thread is waiting in `thread_join` while a worker is blocked in `futex_wait`.
- WASIX task-count accounting now rejects thread/process creation at the configured boundary instead of admitting one extra active task, and thread creation reports `EAGAIN` when that resource limit is reached.
- The effective WASIX task limit now uses the remaining host cgroup task budget directly, minus a small host reserve, instead of dividing the budget by an assumed large host-thread multiplier. The outer cgroup remains the hard resource guard while POSIX workloads can create ordinary pthread fanout under reasonable `TasksMax` caps.
- failed WASIX thread creation paths that discover a missing `wasi_thread_start` export now remove the pre-created thread handle, so repeated failed `thread_spawn_v2` calls do not consume the process task budget.
- threaded CPython workloads avoid the guest asyncify futex path and use shared-memory atomic waits/wakes for Python's libc futex calls.

### Threading

Basic threaded execution has been proven for WASIX modules that import shared memory. Threaded modules should be built with imported shared memory; modules that define and export their own shared memory can spawn smoke-test workers but do not provide the same shared-memory behavior expected by real pthread toolchains.

WASIX thread-local storage key syscalls are wired for process-global keys and per-thread values. Destructor function pointers are recorded with keys and invoked through the guest indirect function table during normal return and `proc_exit` cleanup paths.

The Clang WASIX build recipe has been adjusted to inject the expected default threading linker flags for generated programs:

```text
-pthread
-Wl,--shared-memory
-Wl,--import-memory
-Wl,--extra-features=atomics
-Wl,--extra-features=bulk-memory
-Wl,--extra-features=mutable-globals
```

The Clang package verifier now covers normal `clang` invocation for hello-world compilation, `open(O_CREAT)` filesystem behavior, and a Clang-built pthread program using `pthread_create`, mutex, condvar, and `pthread_join`. The verifier also checks that the pthread output imports shared `env.memory`, which is the shape the WASIX runner expects for real threaded programs.

BusyBox WASIX and CPython WASIX recipes now lower POSIX `fork()` and `vfork()` calls to the direct WASIX `fork`/`vfork` compatibility imports. CPython WASIX also lowers thread-shaped Linux `clone` calls to the runtime `clone_thread` import instead of keeping the shared-address-space case as a libc-side unsupported branch. BusyBox WASIX also lowers `getppid`, `getpgrp`, `getpgid`, `getsid`, `setpgid`, `setsid`, `tcgetpgrp`, `tcgetsid`, `tcsetpgrp`, `kill`, `raise`, `dup`, `dup2`, `pipe`, `chdir`, `execv`, `execve`, `execvp`, `fchdir`, `wait`, and `waitpid` to runtime process imports instead of returning compatibility stubs. The legacy pointer-return `proc_fork` ABI, length-based `proc_chdir` ABI, positive-errno `proc_fchdir` ABI, and WASIX errno-return `proc_signal`/`proc_raise` ABI remain exported for existing WASIX callers, but new POSIX-facing toolchain wrappers no longer need to encode child pids or paths through non-POSIX shapes. CPython WASIX lowers Linux clear-child-TID, robust-list, sleep, clock, `sched_yield`, scheduler priority bounds, scheduler interval, `waitid`, `arch_prctl`, `membarrier`, `rseq`, `tkill`, `tgkill`, `rt_sigreturn`, `FUTEX_FD`, pidfd, timerfd, POSIX timer, process interval timer, `sigaltstack`, signal masks, and signalfd syscall paths plus `pipe2`, `dup3`, `eventfd`, `unshare`, and `setns` descriptor/process operations into runtime process/thread imports instead of keeping those lifecycle, descriptor, event, wait-status, and namespace hooks entirely inside the compatibility layer.

### Filesystem

The sandbox has local and SIFS virtual filesystem backends, staging helpers, path normalization, symlink escape checks, quota accounting, workspace diffing, logs, checkpoints, and read-write mount sync. WASIX `fd_sync` and `fd_datasync` now follow POSIX-facing descriptor behavior for the current runtime: a valid regular-file descriptor can be synced even when the original WASI rights set did not include explicit sync capability bits, while invalid descriptors and unsyncable descriptors such as pipes still report the expected POSIX errors.

Generated virtual filesystem features include:

- per-run `/dev` without host device mappings,
- generated `/proc` entries for common Linux process probes,
- descriptor aliases such as `/dev/fd/<n>` and `/proc/self/fd/<n>`,
- package/toolchain mounts under `/bin`, `/lib`, `/share`, `/include`, and `/etc`.
- local raw WASIX module runs now stage the sandbox's pinned toolchain runtime before launch, so child `exec` from a test or user program can resolve standard commands such as `/bin/ls` through the same real toolchain files used by named sandbox commands.

### Toolchains

Maintained packages:

- BusyBox WASIX: base file, text, archive, and search tools.
- Bash WASIX: real `bash` and `sh`.
- CPython WASIX: Python, pip, subprocess, native-extension loading, and the current `numpy` plus `matplotlib` gate.
- Make WASIX: real GNU Make workflow target.
- TCC WASIX: lightweight C compiler target.
- Clang/LLVM WASIX: LLVM 21.1.6 package recipe for Clang, LLD, LLVM helpers, and shared sysroot/resource artifacts.

The CPython WASIX native gate currently passes locally with real `pip`: `pip install --find-links` installs `numpy` and `matplotlib` into the sandbox workspace, `numpy` imports and runs array operations, and `matplotlib` imports with the Agg backend and renders a PNG artifact inside `/work`. The Python main module is linked through the WASIX C++ linker so dynamically loaded native extension side modules can resolve the C++ runtime ABI from the main image.

### Process Plans And Shell

`SandboxProcessPlan` can run multiple pinned commands with explicit stdout-to-stdin pipes. It is a low-level execution substrate, not a shell syntax implementation.

`sinomo sandbox shell` requires a pinned real `sh`; the default route is the BusyBox ash WASIX applet in the maintained BusyBox package. Bash remains the richer shell package target. BusyBox ash should not be patched to fake missing process support. The runtime must support the process behavior real shells need.

## Remaining Gaps

The target is a real POSIX coding sandbox backed by SIFS. A developer or agent should be able to use real shell commands, Python with pip, build tools, and filesystem operations without host-command escape hatches. Missing features should be closed in the runtime, virtual filesystem, toolchain package, or cluster operation layer instead of patched around in individual tools.

### POSIX Process Gaps

- Complete fork, exec, spawn, wait, and wait-status behavior for shell and Python subprocess users.
- Support nested process trees with correct cwd, environment, argv, file descriptor inheritance, close-on-exec, redirection, and pipe behavior. Close-on-exec is now covered for normal spawned WASIX modules and built-in command dispatch; broader descriptor inheritance and redirection cases still need real-tool coverage.
- Implement robust signal delivery, exit propagation, cancellation, process groups, sessions, controlling TTY behavior, and foreground/background job control. Current coverage includes process-group `SIGKILL`, stopped-status reporting, and continued-status observation/consumption for a stopped child that is resumed with `SIGCONT`; `proc_setpgid` also rejects session leaders. Full shell job-control parity remains open.
- Implement realtime queued signals and CPU-time timers as real runtime features before advertising them. The libc/sysconf layer currently reports `_SC_REALTIME_SIGNALS`, `_SC_CPUTIME`, and `_SC_THREAD_CPUTIME` as unsupported so programs can feature-detect the gap instead of entering broken paths.
- Clean up every failed nested process path so leaked descriptors, process records, blocked futures, and task counters do not survive command exit. The in-process WASIX built-in completion path now runs normal process-exit cleanup, and failed spawn setup now cleans up child environments for cwd, stdio, and `proc_spawn2` fd-action failures. Forked `proc_spawn` and `proc_spawn2` executable-load/module-launch failures now mark the failed child terminated before removing its process record. Non-vfork `proc_exec3` failures restore the caller's argv, environment, executable-path metadata, FD table, preopen FD list, current directory, and union bookkeeping. The `proc_fork_env` failed-exec path now has runtime coverage for returning to the child, exiting through `proc_exit2`, restoring the parent, and reaping the child. Deeper process-tree cleanup and full rollback of filesystem mutations such as union mounts still need the same treatment.
- Keep child exec containment on every executable lookup path, including shell lookup, scripts, generated binaries, and package-manager helpers.

### Threading Gaps

- Broaden real pthread coverage from Clang-built modules: futex wait/wake/requeue, wait-any, mutexes, condition variables, cancellation, multiple-worker programs, and multi-threaded subprocess interactions.
- Verify TLS key behavior and destructor callbacks across normal return, explicit thread exit, process exit, failed initialization, unwind, and cancellation paths.
- Track blocked threads, runnable threads, task counts, peak memory, and CPU use as runtime resources instead of only enforcing simple task boundaries. Failed `thread_spawn_v2` attempts that cannot run because the module lacks `wasi_thread_start` now release their pre-created thread handle, but broader failed-initialization and cancellation accounting still needs real workload coverage.
- Ensure cancellation wakes blocked futex, join, pipe, and process waits without corrupting shared state.

### Filesystem And VFS Gaps

- Route all runtime filesystem behavior through the sandbox virtual filesystem; host-backed filesystems must remain a development backend only.
- Improve Linux-like metadata behavior: permissions, ownership, chmod, uid/gid, umask, timestamps, inode identity, link counts, hardlinks, symlink metadata, and stat variants.
- Close edge cases for rename, unlink, open-create, truncation, append, directory iteration, descriptor aliases, and concurrent access.
- Run useful `pjdfstest` subsets and move permission checks into the runtime or VFS layer where they belong.
- Keep generated `/dev`, `/proc`, `/etc`, package mounts, and workspace mounts consistent across local and SIFS-backed execution.

### Toolchain Gaps

- Stabilize Bash and BusyBox as real shell and base utility packages without semantic patches to shell behavior.
- Remove the legacy CPython compatibility shim by moving every required ABI fix into the WASIX runtime, virtual filesystem, compiler, sysroot, or system-library layer. Python must build from upstream sources without program-specific source or compatibility patches.
- Broaden CPython and pip coverage beyond the current NumPy and matplotlib native-extension gate to more common coding workflows and package families.
- Keep Clang/LLVM, LLD, Make, and generated binaries working with one maintained WASIX toolchain and Linux-style support paths.
- Keep package recipes reproducible, pinned, hash-checked, and documented.
- Move required support files to their corresponding Linux paths instead of accumulating sidecar files that callers must know about.

### SIFS Integration Gaps

- Make SIFS-backed execution the default production path for instances, toolchains, logs, checkpoints, package caches, generated binaries, and artifacts.
- Keep local development execution and SIFS-backed execution semantically aligned for build systems and package managers.
- Support large-file workflows, package caches, and compiler artifacts without assuming local host paths.
- Preserve durable stdout/stderr chunks, command records, and audit events even across daemon restart or node movement.

### Operations Gaps

- Improve `sinomo sandbox doctor` readiness reporting for shell, Python, threading, build tools, and SIFS placement.
- Add resource accounting for CPU, process count, thread count, blocked output, and cleanup after cancellation.
- Harden durable streaming for very large stdout/stderr streams.
- Make daemon recovery detect and clean up leaked runtime work after process crashes or node restarts.
- Add cluster-facing health signals for sandbox runtime failures, policy denials, storage issues, and toolchain package drift.
- Keep network access mediated through Sinomo policy and audit paths; ambient guest network imports should remain denied unless explicitly designed.

### Verification Gaps

- Run representative shell, Python, pip, Make, Clang/LLVM, and generated-binary workflows through the same command path users will invoke.
- Add workload-driven verification before conformance tightening; use real tools to expose missing runtime behavior.
- Track known verifier failures with the exact failing command, expected Linux behavior, current sandbox behavior, and owning layer for the fix.

## Detailed Design Notes

### Purpose And Scope

Sinomo needs a resumable sandbox for agent actions, tool execution, lightweight build/test work, and long-running task workspaces. The target is inspired by WebVM's browser-hosted Linux-like command environment, but the Sinomo sandbox should not boot a full Linux distribution or emulate a whole machine.

The first implementation should be a Rust-native WASM command runtime with a minimal real POSIX userspace, explicit Sinomo policy enforcement, and persistent workspace state stored in SIFS. Sinomo should own and maintain the sandbox runtime, packaging recipes, policy contract, and command verification suite in this repository instead of relying on agent-crate-specific code or ad hoc host tool execution.

The primary initial workload is coding tasks. Assume language toolchains are available as pinned sandbox toolchains; the base userspace must give the agent enough real file management, text processing, and search tools to inspect, modify, build, test, and package code without falling back to host execution. Language toolchains can be larger optional packages, but the baseline coding surface must include real commands for file management and search before the sandbox is treated as useful for coding agents.

The sandbox should provide:

- a real command shell and a useful set of real Unix tools,
- Bash as a real shell command for coding tasks,
- Python and pip as real language/package-management commands for coding tasks,
- deterministic process and filesystem policy boundaries,
- durable task workspaces under SIFS,
- resume after daemon restart or node handoff,
- inspectable execution records for agents and operators,
- a path toward richer toolchains without trusting host binaries by default.

### Phase Status

This status record is intentionally more detailed than the snapshot above. Use it when tracing why a subsystem exists or when updating evidence after implementation work.

| Phase | Status | Evidence |
|---|---|---|
| Phase 0: Architecture And API Skeleton | Implemented | `sinomo-sandbox` defines sandbox IDs, manifests, resource limits, VFS traits, storage registration, path normalization, and error shapes. `sinomo-cli` exposes `sinomo sandbox` help. |
| Phase 1: Local WASI Command Runner | Partially implemented | WASIX is now the only maintained sandbox runtime target. The WASIX runner handles buffered stdin, fd-backed stdin pipes, stdout/stderr capture, stdout pipe teeing, process-tree signaling, input/output limits, static memory-budget checks, instruction metering, mapped preopen/cwd validation, and raw WASIX HTTP import denial. `LocalStagedSandboxCommandRunner` wraps local staged command execution with checkpoint rollback, quota checks, changed-path reporting, stdin forwarding, stdout/stderr persistence, command records, and optional per-command effective resource limits. `SandboxToolchainManifest`, `LocalSandboxToolchainStore`, and `SifsSandboxToolchainStore` record command names, module paths, BLAKE3 hashes, support artifact paths/hashes, capabilities, default limits, provenance, local cache persistence, and SIFS persistence under `/services/sandbox/toolchains/<toolchain_id>`. `sinomo sandbox toolchain add-command` and generic package manifests target WASIX. BusyBox is imported through the generic `sinomo sandbox toolchain import-package` package path as the default core applet toolchain. |
| Phase 2: SIFS-Backed Workspace | Partially implemented | `LocalSandboxInstanceStore` creates the planned instance directory layout and persists `manifest.sobj` through `sinomo-object`; `LocalSandboxFileSystem` provides a tested local development VFS for root/home/tmp/work/logs/artifacts paths with path-escape and symlink-escape checks, snapshot/diff changed-path reporting, workspace file-count/byte accounting, per-command written-byte accounting, local checkpoint/resume/reboot/destroy, and checkpoint cleanup; command stdout/stderr plus structured `.record.sobj` records are persisted under `/logs` through backend-agnostic helpers tested against both local and SIFS filesystems, command ids are appended to the sandbox manifest command history after completed, failed, cancelled, or resource-exhausted runs, persisted `cwd` and environment variables are configurable through `sinomo sandbox config`, `sandbox_command_record_event` lowers completed/failed/resource-exhausted records into cluster event envelopes, `sandbox_policy_denied_event` and `sandbox_scheduling_failed_event` provide typed denial/failure events, and `append_sandbox_command_record_events` reads persisted local/SIFS command records plus stdout/stderr logs and appends reconstructed `sandbox.command.started`, raw `sandbox.command.output` chunk events, and the final command-status event into an `EventStore`; `sinomo sandbox logs <sandbox-id> <command-id>` decodes the command record before printing raw streams; `SifsSandboxInstanceStore` creates/loads/saves instance manifests and standard directories under `/services/sandbox/instances/<sandbox_id>`, with SIFS checkpoint/resume/reboot/delete/destroy support for root/home/tmp/work/logs/artifacts and explicit replica-candidate writes for distributed persistence tests; `SifsSandboxFileSystem` adapts sandbox virtual paths to the existing SIFS POSIX API for stat/read/write/list/rename/unlink/create-dir, supports explicit replica candidates, and is tested with `NetworkedDfsService` including a cross-node resume/read regression; backend-agnostic workspace sync remains for local development import/export and non-production staging helpers; `SifsStagedSandboxCommandRunner` now runs SIFS-backed WASIX commands against virtual preopens backed directly by `SandboxFileSystem`, runs only WASIX for SIFS-backed execution, exposes toolchain artifacts through read-only virtual preopens built from SIFS objects, persists command logs/workspace metadata back through the same SIFS VFS, saves updated manifest command history back to SIFS, accepts explicit replica candidates for workspace/log writes, and is tested with command-write, process-plan output, direct virtual preopen, and replica-node regressions; `SifsSandboxToolchainStore` writes and reads toolchain manifests/modules through SIFS with hash verification; `sinomo sandbox create`, `status`, `config`, named `run`, `run --wasm`, `logs`, `ls`, `cat`, `write`, `checkpoint`, `resume`, `reboot`, `destroy`, and local `toolchain add-command` are wired, `sinomo sandbox create --mount <sifs-path>:<mount-path>[:ro|rw]` persists mount declarations in the manifest, and local `sandbox run` now validates declared mounts, pinned command capabilities, effective command limits, and `argv[0]` before delegating the execution transaction to `sinomo-sandbox`. Local CLI commands still default to local staging, but `sinomo sandbox create --daemon`, `status --daemon`, `config --daemon`, `run --daemon`, `shell --daemon`, `logs --daemon`, `ls --daemon`, `cat --daemon`, `write --daemon`, `fetch --daemon`, `checkpoint --daemon`, `resume --daemon`, `reboot --daemon`, and `destroy --daemon` now use the daemon/SIFS path for SIFS-backed instances, SIFS toolchains, status reads, command execution, real pinned shell invocation when available, logs, file inspection, writes, daemon-backed HTTP fetch-to-file, checkpoint, resume, reboot, and destroy. |
| Phase 3+: BusyBox userspace, scheduling, network, richer toolchains | Partially implemented | `SandboxPolicy` provides reusable checks for allowed mount prefixes, local-owned mount locality, resource budgets, concurrent command/sandbox budgets, command capability allowlists, signed/allowlisted toolchains, toolchain provenance prefix allowlists, package-cache SIFS prefix allowlists, HTTP host allowlists, HTTP byte/timeout budgets, toolchain update preflight, and full invocation validation across instance manifest, pinned toolchain, selected command, and execution node. `SandboxClusterPolicyConfig` owns the typed cluster KV policy contract at `/config/sandbox/policy`, with snapshot decode and transaction helpers. `select_sandbox_execution_node` provides the tested node-locality scheduling primitive, prefers the owner node for `/sinomo/nodes/<node_id>/files` mounts, returns inspectable `unavailable-offline` and `policy-blocked` reasons, and SIFS staged execution records `last_placement` in the sandbox manifest. Local CLI named command execution plus local and SIFS staged runners reject denied manifests before execution or staging, and named pinned toolchain commands only run after validating toolchain signature/allowlist/provenance where applicable, package-cache paths, command capabilities, matching `argv[0]`, and effective command limits. Local and SIFS filesystem APIs expose virtual `/dev`, `/dev/null`, and bounded `/dev/zero`; staged WASI command execution now installs a per-run Sinomo-owned `/dev` preopen so commands can open `/dev/null` and bounded `/dev/zero` without mapping host devices. The daemon now exposes `/sandbox/create`, `/sandbox/status`, `/sandbox/checkpoint`, `/sandbox/resume`, `/sandbox/destroy`, `/sandbox/list`, `/sandbox/read`, `/sandbox/write`, `/sandbox/http/fetch`, `/sandbox/http/fetch-to-file`, `/sandbox/toolchain/import-package`, `/sandbox/toolchain/add-command`, `/sandbox/toolchain/add-artifact`, and `/sandbox/run/toolchain`, `/sandbox/run/toolchain/stream`, `/sandbox/run/process-plan`, and `/sandbox/run/process-plan/stream`, loads the cluster sandbox policy, rejects denied create/config/write/checkpoint/resume/reboot mutations before changing SIFS state, preflights daemon toolchain imports/adds before writing SIFS package state, enforces active execution concurrency limits before daemon-backed runs, opens SIFS through the local DFS metadata/content stores, runs the pinned SIFS toolchain command through `SifsStagedSandboxCommandRunner`, derives replica candidates from DFS content-service advertisements, and persists placement/log/workspace changes back to SIFS. `sinomo sandbox create --daemon`, `status --daemon`, `toolchain import-package --daemon`, `toolchain add-command --daemon`, `toolchain add-artifact --daemon`, `run --daemon`, `shell --daemon`, `logs --daemon`, `ls --daemon`, `cat --range`, `write --daemon`, `fetch --daemon`, `checkpoint --daemon`, `resume --daemon`, `reboot --daemon`, and `destroy --daemon` select daemon-backed SIFS paths while direct `--wasm` execution remains local only. `sinomo sandbox process-plan <sandbox-id> <plan.json> [--daemon]` exposes the structured process-plan substrate for pinned toolchain commands and explicit stdout-to-stdin pipes without parsing shell syntax, using either the local staging backend, the daemon-backed single-response SIFS RPC route at `/sandbox/run/process-plan`, or the live-output stream route at `/sandbox/run/process-plan/stream`; local and SIFS/daemon WASIX-only linear pipelines now run connected steps concurrently with fd-backed stdout-to-stdin pipes, using direct virtual preopens backed by the sandbox VFS. `sinomo sandbox policy show/set` reads and writes the cluster-scoped policy through cluster KV, including allowed mount prefixes, package-cache prefixes, toolchain provenance prefixes, concurrency limits, and HTTP hosts. `sinomo sandbox doctor [<sandbox-id>]` reports local toolchain readiness, missing pinned toolchains, coding-toolchain command gaps for every BusyBox applet required by `verify-coding`, missing commands from the planned minimal POSIX command environment, policy incompatibilities, placement metadata, recent policy-denial and resource-exhaustion events from the durable event log, and local CLI backend limitations. `SandboxBusyBoxToolchainSpec` declares the core BusyBox applet set and installs a real BusyBox WASIX module into local or SIFS toolchain stores by default; `BUSYBOX_TOOLCHAIN_FIDELITY_CASES` declares host-side real-tool checks, `sinomo sandbox toolchain busybox-plan` prints the applet/fidelity plan, and `sinomo sandbox toolchain verify-busybox [--daemon]` runs the BusyBox fidelity suite against local or daemon-backed SIFS sandboxes. Generic WASI command modules, support artifacts, and declared packages can now be added to local or SIFS toolchain manifests with `sinomo sandbox toolchain add-command [--daemon]`, `sinomo sandbox toolchain add-artifact [--daemon]`, and `sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>]`, letting richer language/runtime packages be imported as standalone toolchains or merged into a BusyBox-based coding toolchain through the same SIFS replication path. `sinomo sandbox toolchain hash <file>` prints the exact BLAKE3 content hash expected by package manifests. `crates/sinomo-sandbox/toolchains/busybox/README.md` documents the current import contract and explicitly forbids fake BusyBox substitutes, and the same directory now includes the pinned containerized BusyBox WASIX package recipe. The active coding target is WASIX: Bash provides real `bash` and `sh`, CPython WASIX provides `python` and `pip` with native-wheel support, and Make/TinyCC provide build-tool coverage. `SandboxProcessPlan` adds a reusable lower-level process substrate that runs multiple pinned toolchain commands as separate WASI invocations and can pipe stdout from one step into stdin of the next through both local and SIFS-backed staged runners; it rejects ambiguous steps where an explicit stdin payload would conflict with a pipe from the previous step, and it is not exposed as `sh` and does not claim shell syntax or general POSIX process parity. WASIX terminal process support now tracks process groups, sessions, foreground terminal ownership, background stdin `SIGTTIN`, background `tcsetpgrp` `SIGTTOU`, and POSIX `TOSTOP` background stdout/stderr `SIGTTOU` through the terminal fd path. `sinomo sandbox shell` requires a pinned real `sh` command and does not provide a fake fallback; the maintained BusyBox WASIX package provides default `sh` through upstream ash, while Bash remains the richer shell package target. Mediated HTTP exchange records now persist under `/logs` as SOBJ files, round-trip through local and SIFS filesystems, lower into `sandbox.network.http` events containing redacted request metadata, and the recording fetch helper audits successful and failed post-policy transport attempts. `sandbox_http_fetch_to_file` stores successful response bodies into the sandbox filesystem, and `sinomo sandbox fetch` exposes a curl-like command surface backed by the sandbox HTTP transport abstraction; the production path now routes through local daemon object RPC at `/sandbox/http/fetch` and `/sandbox/http/fetch-to-file`, and the daemon applies cluster sandbox policy before outbound HTTP. Remaining implementation work includes arbitrary POSIX fork/exec/wait parity, broader job control, and more production hardening around process-oriented tools. |

Recent fetch-to-file quota handling update: local and daemon-backed `sinomo sandbox fetch` now emit `sandbox.resource.exhausted` when the mediated response body would exceed the sandbox write/workspace quota, while retaining the HTTP audit record for the completed network attempt.

The Bash target now has a checked-in Sinomo-owned WASIX recipe under `crates/sinomo-sandbox/toolchains/bash/`. It pins GNU Bash, wasi-sdk, Binaryen, and wasix-libc inputs, verifies downloads, builds real upstream Bash with the WASIX sysroot, applies Binaryen asyncify plus function-pointer cast emulation for Bash's setjmp/longjmp and generic callback paths, and emits a generic `import-package` manifest only after a real module exists. The recipe builds a real GNU Bash WASIX module and the generated package manifest passes Sinomo package validation. The package exposes `bash` and `sh` command entries backed by the same upstream Bash module, with `sh` relying on Bash's real invocation-name behavior rather than a shim. Local and daemon-local SIFS-backed `verify-bash` now pass after importing Bash into the BusyBox WASIX package: Bash builtin `printf`, external-command pipeline with `grep`, redirection plus external `cat`, descriptor duplication with `2>&1`, command substitution, `set -e`, `/work` shebang execution, `sh -c` external-pipeline behavior, and an explicit sandbox-local WASM executable staged into `/work` all run through the WASIX runtime. Bash remains the richer shell package target and is gated from ordinary agent use by sandbox policy and production readiness: Bash/WASIX command manifests declare both `child-process-exec` and `wasix.unpinned-child-exec`, and the default policy denies both capabilities. Ordinary clusters must explicitly allow the Bash/WASIX toolchain plus both capabilities before `bash` is available to agents. No shim shell substitute is allowed.

BusyBox recipe status: `crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh` is now the required end-to-end gate for applet additions. It builds the pinned BusyBox WASIX package recipe, validates and imports the generated `package.json`, creates a disposable sandbox, and runs `verify-busybox`. The recipe builds against the real WASIX sysroot, applies the checked-in BusyBox patch, uses Binaryen asyncify plus function-pointer cast emulation, and emits a generic `import-package` manifest. The old `wasi-compat` header directory was removed with the legacy compatibility runtime and should not be moved into the shared WASIX sysroot. Generic POSIX support belongs in `crates/sinomo-sandbox/toolchains/build-tools/sysroot` and the runtime/system-library layer. No new BusyBox applet should be exposed unless the verifier passes for the applet. BusyBox `sleep` is now included in the maintained WASIX command set because upstream timer shell tests use `/bin/sleep`, and the rebuilt BusyBox package passed with hash `blake3:fa2a700ab3690e6ccd6f92d4c8d704f44fd10e9b7eb0a9af6e2a13fb999c3593` in the local package gate. `crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh` is the combined local Bash gate: it builds BusyBox WASIX and Bash WASIX, validates both package manifests, imports Bash into the BusyBox WASIX toolchain in a disposable data directory, creates a sandbox, and runs `sinomo sandbox toolchain verify-bash`. Set `VERIFY_BACKEND=daemon-local` to run the same package/import/execution gate through a disposable local daemon, cluster policy, and SIFS-backed sandbox workspace. `crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` is the combined coding-toolchain verifier: it builds the pinned BusyBox WASIX recipe, creates a disposable local sandbox, and runs `sinomo sandbox toolchain verify-coding`. `verify-coding` now includes the Phase 3 command workflow against `/work`. The workflow covers creation, copy/move/compare, permission changes, archive create/list, remove/rmdir cleanup, stdin text processing, search, and cross-tool file handoff, so the local gate covers the documented coding-task acceptance path instead of only isolated applet checks. `crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` accepts `VERIFY_FULL=1` as the explicit heavier local gate. Full mode runs the active WASIX shell/build workspace with BusyBox WASIX plus Bash, Python, Make, and TCC; `VERIFY_FULL=1 SKIP_BUILD=1` intentionally reuses existing package outputs and fails early if any required BusyBox WASIX, Bash, Python, Make, or TCC package manifest is missing.

The durable event path is now shared with the wider event subsystem: `sinomo-events` registers `events/event-log.jsonl`, and `JsonlEventStore` persists newline-delimited `EventEnvelope` records with replay filters and duplicate id rejection. The daemon-backed sandbox path appends `sandbox.instance.created`, `sandbox.instance.updated`, `sandbox.instance.checkpointed`, `sandbox.instance.resumed`, `sandbox.instance.rebooted`, `sandbox.instance.destroyed`, `sandbox.command.started`, `sandbox.command.output`, `sandbox.command.completed`, `sandbox.mount.added`, `sandbox.mount.denied`, `sandbox.toolchain.pinned`, `sandbox.toolchain.updated`, and `sandbox.network.http` records to that store after the corresponding SIFS state has been written. Command output events currently persist captured raw stdout/stderr chunks after command completion. The WASI and SIFS-staged runners now expose an output sink that receives raw stdout/stderr chunks while the command runs, and local `sinomo sandbox run`/`shell` uses that sink for operator-visible streaming. Daemon-backed toolchain execution also has a streaming RPC route that sends output chunk frames before the final command outcome frame.

### WebVM Lessons To Reuse

WebVM demonstrates a useful product shape:

- a terminal-first virtual environment,
- a prebuilt root image containing common command-line tools,
- persistent storage,
- networking as an explicit controlled subsystem,
- browser/WASM isolation instead of direct host execution.

Sinomo should reuse the shape, not the implementation. WebVM uses CheerpX to run unmodified x86 Linux binaries with a Linux syscall emulator and a virtual block filesystem. Sinomo should instead run WASM modules directly in a Rust host and provide the minimum WASI-like system interface needed by our command environment.

Key differences:

| Area | WebVM-like shape | Sinomo target |
|---|---|---|
| CPU/runtime | x86 virtualization in WebAssembly | Rust host running WASM components/modules |
| OS surface | Full Linux ABI compatibility | Minimal real POSIX command surface backed by WASI/host shims |
| Root filesystem | Disk image/block filesystem | SIFS-backed virtual filesystem |
| Persistence | VM-local persistent storage | Cluster-persisted SIFS workspace state |
| Networking | Browser-mediated networking | Sinomo-network mediated allowlisted egress |
| Security model | Browser sandbox plus VM boundary | Sinomo policy, capability checks, resource limits, and node-local enforcement |

### Design Position

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

The sandbox runtime should be a production Sinomo service, not an experiment in the agent crate. It should live in `sinomo-sandbox` or a successor crate with runtime exports through `sinomo-runtime`.

The initial runtime should prioritize correctness, observability, and resumable state over breadth. The goal is not a "POSIX-like" imitation. It is a minimal real POSIX system for the agent: if Sinomo exposes `grep`, `sed`, `awk`, `tar`, or any other command, that command must behave as the genuine tool within the documented sandbox limits. If Sinomo cannot provide a real implementation, the command should be absent and the runtime should report that it is unsupported.

### Filesystem Model

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

The first version should not expose arbitrary host paths. Host access should only appear through explicit SIFS mounts. Existing local-owned SIFS paths such as `/sinomo/nodes/<node_id>/files` can be mounted read-only or read-write only when policy grants that capability.

#### Workspace Persistence

SIFS is the durability boundary. The sandbox should commit meaningful progress to SIFS after each command or explicit checkpoint:

- file mutations,
- stdout/stderr log segments,
- exit status,
- resource usage,
- current working directory,
- environment variables,
- command history,
- active package/tool versions,
- checkpoint metadata.

The runtime does not need to snapshot raw WASM linear memory in the first version. Resume means:

```text
reload workspace state
restore virtual filesystem view
restore shell/session metadata
continue with the next command
```

`/tmp` belongs to the sandbox instance. It is durable while that instance exists, including across daemon restart, node handoff, and `sandbox resume`. A new agent session is only a reboot of the sandbox instance. By default, reboot clears `/tmp`. The behavior is an instance policy flag, so selected sandboxes can retain `/tmp` across sessions when requested. Destroying the sandbox instance deletes the whole instance root, including `/tmp`.

Long-running process suspension can come later as an explicit checkpointable process model.

### Runtime Model

Use a Rust WASM runtime with WASI support as the execution core. Candidate runtimes:

- Wasmtime for the first implementation because it has mature WASI support, resource limits, component-model direction, and strong Rust integration.
- Wasmer/WASIX for process-capable toolchains where plain WASI cannot provide real fork/exec/wait, pipes, TTY, or other POSIX process semantics.

Runtime target code should stay focused on WASIX under `sinomo-sandbox/src/runtime/` because the sandbox must support real process semantics for shell and Python workloads. Toolchain manifests keep a default ABI in `target_abi`, but supported commands should target WASIX.

The current WASIX runner configures Wasmer with explicit Sinomo capabilities: ambient insecure authority remains disabled, ambient HTTP hosts remain empty, and runtime process/thread fanout is capped before a module starts. WASIX execution now uses a lower-level Wasmer process handle instead of the synchronous high-level `run_wasm` entrypoint, so Sinomo can signal the root WASIX process tree with `SIGKILL` when the requested wall-clock budget expires or the Sinomo cancellation token is tripped. `sinomo sandbox doctor` reports those yielding-process guards as supported `process-tree-timeout` and `process-tree-cancellation` budget capabilities. The host wait path now polls the termination reason and returns cancellation promptly instead of waiting for the original command timeout after the cancellation guard has signalled the process tree. The runner compiles WASIX command modules with Sinomo's multi-module-safe Wasmer metering, giving standalone, pinned-child, and sandbox-local child-exec commands a deterministic instruction budget instead of allowing non-yielding guest loops to run without a runtime-enforced bound. When a module already imports the real WASIX `sched_yield` syscall, the metering middleware also injects periodic signal checkpoints at metered branches, so a non-yielding loop in normal WASIX libc-backed commands can observe Sinomo cancellation without the guest source calling `sched_yield` itself. The Python gate verifies a runaway Python compute loop is reported as a sandbox timeout. If a signalled WASIX process tree does not stop within the shutdown grace window, the host wait path keeps waiting for the runner to stop instead of returning while a non-yielding task can keep consuming CPU in the background. WASIX budget guard threads are also woken when command execution finishes, and the process runner marks the guard finished before propagating wait-path errors, so failed or short-lived process runs do not leave timeout or cancellation guard threads alive until the original command deadline. The host wall-time watchdog also handles unrepresentably large timeout values without panicking the daemon. The runner still reports exact `wall-timeout` and fully general immediate `cancellation` budget capabilities as incomplete because engine-level wall-clock interruption of modules without a signal-checkpoint import remains pending; that gap must be closed before long-running WASIX shells or language runtimes are considered production-ready. WASIX guest process signaling now has a regression where a parent process spawns a pinned child command, sends `SIGKILL` through `proc_signal`, and waits for it with `proc_join`, covering the basic signal/wait path needed by process-oriented tools. `proc_spawn` now accepts non-empty child preopen metadata and lets the child inherit the parent sandbox VFS/preopen set instead of returning `Notsup`, which expands compatibility with libc/tooling paths that pass preopen lists through spawn. The runner no longer preflights fork/exec imports out of the module before compilation; process-capable guests now enter the WASIX runtime and exercise the actual process substrate, so further work can expand runtime support instead of growing a separate list of rejected module shapes. `proc_spawn` also accepts the legacy `chroot` flag as already satisfied by the inherited Sinomo sandbox virtual root, so libc callers that request a chrooted child are not rejected before the child can run inside the existing sandbox filesystem boundary. Direct `chroot(2)` uses the same model: it validates the requested directory inside the sandbox namespace and records the call as satisfied by the existing sandbox root boundary. When WASIX `proc_spawn2` or `proc_exec3` supplies an explicit environment vector, the child environment now replaces the parent environment instead of merging into it, matching POSIX `envp` behavior while preserving inheritance when no environment vector is supplied. WASIX `proc_spawn2` and `proc_exec3` now propagate resolved spawn failures through the runtime's spawn-error-to-errno mapping instead of collapsing all child spawn failures to `Noexec`, so missing binaries, access denial, and invalid requests surface as more accurate syscall errors. `proc_spawn2` and `proc_exec3` also resolve all non-absolute executable names relative to the current sandbox directory when PATH search is not requested, instead of only handling names prefixed with `./`. WASIX PATH search now resolves relative PATH entries, including the empty entry, against the sandbox current directory instead of the virtual root, so shell and Python subprocess lookup follows POSIX cwd-relative PATH semantics. The older `proc_spawn` entrypoint now uses the same rule for non-absolute executable names, so all WASIX child-exec paths reach the sandbox filesystem with an absolute virtual path before binary lookup. WASIX `posix_spawn` file actions now handle `Fchdir` inside the sandbox namespace: root descriptors set cwd to `/`, and directory descriptors are resolved back to their loaded virtual path instead of copying the backing inode path into the child current directory. WASIX `chdir` now resolves the target through the sandbox virtual inode tree, including symlinked directories, and stores a normalized virtual cwd instead of probing the backing filesystem path directly. `path_open2` now resolves final symlink inodes by reopening the symlink target through the normal sandbox path-open path instead of trapping on `unimplemented!`, so tools that open sandbox symlinks see a syscall result rather than a runtime panic. Absolute symlink targets now resolve from the sandbox virtual root rather than being rejected, which lets real root layouts use links such as `/bin/sh` without escaping into the host filesystem. WASI `path_symlink` now follows POSIX symlink creation semantics by recording the target string exactly as supplied without resolving or requiring the target to exist first, so dangling symlinks and package-managed link layouts work. `path_readlink` now accepts a buffer exactly matching the link target length, matching readlink-style output that does not require a trailing NUL byte. Virtual symlink stat now reports a symbolic-link file type and target-string size directly from the sandbox inode, so links created inside the sandbox do not depend on host `symlink_metadata` to be inspectable. Remaining symlink descriptor edge paths in `fd_seek` and `sock_send_file` now return normal syscall errors instead of panicking the runtime when a guest operates on an already-open symlink descriptor. Dynamic-call and dynamic-linker edge cases now fail through syscall results instead of host panics: `call_dynamic` returns `Inval` for non-byte-representable function signatures or strict result-buffer mismatch, `dlopen` maps impossible error-buffer offset conversion into memory access failure, and `dlsym` reports a dynamic-linker error when a resolved symbol address does not fit the guest pointer width. Descriptor and unlink edge cases also return normal syscall errors rather than panicking: `fd_renumber` reports `Inval` if the target descriptor cannot be reused after close, and `path_unlink_file` reports `Inval` for non-file inode kinds such as buffers, sockets, pipes, event descriptors, and epoll handles. Additional WASIX edge paths now report syscall errors instead of host panics: `path_open2` returns `Inval` for buffer-backed inode opens, `closure_free` returns `Inval` when the closure slot cannot be freed, and `closure_prepare` returns `Noexec` if the generated in-memory closure module cannot be loaded. Closure trampoline generation now also rejects reference value types with `Inval` instead of panicking while calculating argument/result storage. Socket and descriptor error paths now use ordinary invalid-operation errors: `sock_open` and `sock_pair` return `Inval` for incompatible protocol/type combinations instead of `Notsup`, unsupported socket kinds return `Inval`, and `fd_read` on symlink or epoll descriptors returns `Inval`. The vendored WASIX rewind path now calls `asyncify_stop_rewind` when leaving rewind mode instead of incorrectly reusing the unwind stop hook. The runtime capability report exposes this boundary as `wasix.asyncified-fork`, distinct from general `child-process-exec`, so doctor output does not imply arbitrary POSIX fork/job-control parity. Command policy now allows `wasix.asyncified-fork` for WASIX toolchains when the runtime feature is enabled, while keeping it a separate explicit capability from ordinary child exec so only asyncify-built modules can claim the fork path. WASIX child process state now records parent PIDs, and `proc_parent` reports the stored parent process rather than echoing the queried process ID. Parent PID state is stored in the shared process record, so process-table lookups, procfs status generation, `proc_parent`, child `setpgid` validation, and `SIGCHLD` delivery observe the same parent metadata as the executing child handle. `proc_spawn` also resolves and validates a requested child working directory through the normal WASIX `chdir` path instead of assigning a raw current-directory string. The default WASIX runtime now installs Wasmer's in-memory TTY bridge, so `tty_get` and `tty_set` are available to non-interactive shell and language-runtime guests instead of returning `Notsup`; `PluggableRuntime::new` also installs this default bridge directly, so custom builder paths that use the standard runtime constructor inherit TTY state without requiring separate builder-side setup. `proc_join` now honors non-blocking waits for "any child" joins, returning a no-status result when children are still running instead of entering the blocking deep-sleep path. The process table also cleans stale child entries during all-children joins plus blocking and non-blocking any-child joins, avoiding a dead or missing child handle from poisoning future wait calls or panicking the blocking wait path with an empty live-child set. Specific-PID `proc_join` now reports a no-child syscall error when the target process is absent instead of returning a successful empty status. `proc_spawn` and `proc_spawn2` now add child handles to the parent only after the child process has actually been created, so failed spawn setup or missing executables do not leave phantom children for later wait calls. `proc_spawn` now implements `StdioMode::Null` with real sandbox-local null stdio handles instead of closing the child descriptor, so spawned children can read EOF from null stdin and discard output to null stdout/stderr without receiving bad-fd errors. `proc_spawn` now treats `StdioMode::Log` as inherited stdio, which routes child output through the existing Sinomo capture/logging descriptors instead of closing the descriptor and causing bad-fd failures. The Sinomo-owned WASIX `/dev` preopen now also exposes sandbox-visible `/dev/stdin`, `/dev/stdout`, `/dev/stderr`, `/dev/tty`, and `/dev/fd/{0,1,2}` entries. `path_open` maps the stdio entries back to cloned process stdio handles and maps `/dev/fd/<n>` to a clone of any currently open descriptor, so shells and language runtimes can use normal fd-device redirection without mapping host device nodes. The same descriptor-alias handling is available to path stat operations, and `/proc/self/fd/<n>` is accepted as a descriptor alias for open/stat even though Sinomo does not mount a broader `/proc` filesystem yet. WASIX command execution now mounts a Sinomo-owned minimal `/proc` tree with `/proc/self/fd`, and directory reads of that path enumerate the process's current fd table. `path_readlink` now handles `/dev/std*`, `/dev/fd/<n>`, and `/proc/self/fd/<n>` aliases, returning sandbox-internal descriptor labels without exposing host staging paths. `/proc/self/exe` is exposed as a sandbox executable alias. Readlink returns a sandbox path derived from argv/PATH, while open and stat resolve argv[0] through the current directory or PATH and never expose the host module staging path. `/proc/self/cwd` is also mounted as a live current-working-directory alias for open, stat, and readlink, so procfs cwd probes resolve to the sandbox virtual cwd instead of the host staging directory. `/proc/self/root` is mounted as a sandbox-root alias for open, stat, and readlink, resolving to `/` inside the sandbox rather than to the host staging root. Path traversal through `/proc/self/cwd/...` and `/proc/self/root/...` now resolves against the sandbox virtual cwd and root for open and stat operations, matching the way POSIX runtimes commonly use these procfs symlinks. `/proc/self/cmdline` is populated from the actual command argv as a NUL-separated procfs file in the per-run proc tree. `/proc/self/environ` is populated from the effective command environment as NUL-separated `KEY=value` entries. For WASIX this uses the same lowered environment passed to the runner, including `PATH`, `PWD`, and Sinomo WASIX control variables. `/proc/mounts` and `/proc/self/mounts` are populated from the actual sandbox preopen table plus the Sinomo-owned `/dev` and `/proc` mounts, using escaped mount fields and read-only/read-write options that match the configured sandbox mount modes. `/etc/mtab` is generated from the same mount table for older tools that still read the legacy mount-table path. The minimal procfs mount now also exposes generated read-only `/proc/self/comm`, `/proc/self/stat`, `/proc/self/status`, `/proc/self/statm`, `/proc/self/limits`, `/proc/self/io`, `/proc/self/cgroup`, `/proc/self/cpuset`, `/proc/self/{uid_map,gid_map,projid_map,setgroups,mountstats}`, `/proc/self/loginuid`, `/proc/self/sessionid`, `/proc/self/oom_score`, `/proc/self/oom_score_adj`, `/proc/self/personality`, `/proc/self/timers`, `/proc/self/timerslack_ns`, `/proc/self/schedstat`, `/proc/self/sched`, `/proc/self/autogroup`, `/proc/self/wchan`, `/proc/self/stack`, `/proc/self/attr/{current,exec,fscreate,keycreate,prev}`, `/proc/self/maps`, `/proc/self/smaps`, `/proc/self/numa_maps`, `/proc/self/auxv`, and `/proc/self/fdinfo/<n>` files. It also exposes `/proc/self/ns/{cgroup,ipc,mnt,net,pid,pid_for_children,time,uts}` as namespace symlink targets for stat and readlink probes. The generated files are backed by WASIX runtime process and descriptor state. `/proc/self/fdinfo` directory reads enumerate the live fd table. `/proc/self/task` is now present and directory reads inject the current WASIX thread id. Paths under `/proc/self/task/<tid>/...` and `/proc/thread-self/...` resolve back to the current process/thread view, so runtime probes that use the thread-oriented procfs layout can read the same generated `status`, `stat`, `fdinfo`, cwd/root, and executable aliases. Procfs readlink handling follows the same aliases for `cwd`, `root`, `exe`, and fd targets. Numeric current-process procfs paths are also treated as first-class process views: WASIX open/stat/readlink and directory reads alias `/proc/<pid>` and `/proc/<pid>/task/<tid>` for the current process back to the live `/proc/self` state. The task views include a Linux-style `children` file; WASIX populates it from the runtime child-process table so process-oriented tools can inspect currently spawned child PIDs. WASIX task directory reads now enumerate the live process thread table, and the generated `stat`, `status`, and `sched` files report the runtime thread count instead of a fixed single-thread placeholder. The `/proc` root now enumerates all live WASIX control-plane processes, and numeric process files such as `/proc/<pid>/status`, `/proc/<pid>/stat`, `/proc/<pid>/comm`, `/proc/<pid>/cmdline`, `/proc/<pid>/environ`, `/proc/<pid>/sched`, and `/proc/<pid>/task/<tid>/children` are generated from the selected runtime process where that process state is available. WASIX process records now retain guest argv, environment, and current working directory metadata across fork, spawn, exec, and chdir paths so procfs process introspection reflects the actual child command rather than a generic placeholder. Open, stat, and readlink paths for `/proc/<pid>/cwd` and `/proc/<pid>/cwd/...` resolve through the selected process's recorded sandbox cwd. Open, stat, and readlink paths for `/proc/<pid>/exe` resolve from the selected process executable metadata recorded at spawn/exec time, with argv, PATH, and cwd fallback for older process records. Generated `stat`, `status`, and `sched` process files now use live WASIX task status plus recorded process start time instead of hard-coded always-running placeholders. `status` also reports the selected process's recorded file-descriptor table size, inherited process group/session IDs, namespace PID fields, and the sandbox's fixed uid/gid group identity. Descriptor snapshots include fd numbers, current offsets, and open flags; they are refreshed from the real WASIX open, dup, close, pipe, eventfd, epoll, socket, socketpair, and accept syscall paths. WASIX process group and session state is stored in the shared process record, so updates are visible through all process handles, procfs views, signal selection, and wait selection. Sinomo's WASIX import surface now includes `proc_getpgid`, `proc_getpgrp`, `proc_getsid`, `proc_setpgid`, and `proc_setsid` for runtimes that can lower POSIX job-control calls directly, and `/proc/<pid>/sessionid` reports the selected process's recorded session rather than a fixed placeholder. TTY state now also carries a foreground process group, initializes that group from the first WASIX process group attached to the terminal, and Sinomo's WASIX import surface includes `proc_tcgetpgrp`, `proc_tcgetsid`, and `proc_tcsetpgrp` so runtimes can lower POSIX terminal job-control calls without patching shell behavior. The WASIX import surface also includes `path_chmod`, `path_mode`, `fd_chmod`, and `fd_mode` for host-backed sandbox paths and descriptors. BusyBox WASIX routes `chmod` and mode-bit `stat` through those imports, so executable-bit workflows operate on the real staged workspace metadata instead of relying on legacy compatibility extensions. WASIX processes now track an inherited POSIX file creation mask through `proc_umask`; default file creation applies `0666 & ~umask`, directory creation applies `0777 & ~umask`, and the CPython WASIX package routes `os.umask` through that runtime import instead of keeping Python-only process state. CPython WASIX also routes `os.chmod`, `os.fchmod`, and `os.chmod` `dir_fd` forms through the WASIX permission imports so Python package and build scripts can update executable bits through the sandbox runtime. The single-user sandbox identity is exposed through WASIX `proc_getuid`, `proc_getgid`, and `proc_getgroups`; CPython WASIX maps `os.getuid`, `os.geteuid`, `os.getgid`, `os.getegid`, and `os.getgroups` to that runtime identity. CPython WASIX maps path, fd, and `dir_fd` ownership changes through the same WASIX imports. Ownership changes validate that the target exists and then succeed as virtual ownership operations for any requested uid/gid. Sinomo does not map those ids to host ownership, but this lets tar, wheel, and package install flows that restore archived ownership proceed inside the sandbox. CPython WASIX also exposes the direct identity-setter calls (`setuid`, `seteuid`, `setgid`, and `setegid`) with the same virtual process-identity state used by the `setres*`, `setre*`, `setgroups`, and `initgroups` wrappers. These calls update the guest-visible real/effective/saved uid/gid and supplementary-group values for the current process without changing host credentials or SIFS ownership. The WASIX runtime process table now also carries guest-visible real/effective uid/gid and supplementary group state, inherited by forked and spawned child processes; `proc_getuid`, `proc_getgid`, and `proc_getgroups` read that process record instead of returning hardcoded root identity. The same runtime identity record now backs `proc_getresuid`, `proc_getresgid`, `proc_setresuid`, `proc_setresgid`, and `proc_setgroups`, so CPython identity changes update the shared WASIX process state rather than a Python-local shadow copy. Generated `/proc/<pid>/status` files also report the selected process's current umask, real/effective/saved uid/gid, and supplementary group list from that process record. BusyBox WASIX also routes `chown`, `lchown`, and `fchown` calls from its shared helper layer through the same runtime imports, so ownership probes and future ownership-managing applets follow the sandbox ABI instead of the sysroot mock. BusyBox WASIX `getgroups`, `getpgid`, `getpgrp`, `getsid`, `setpgid`, `setsid`, `tcgetpgrp`, `tcgetsid`, and `tcsetpgrp` now route through the WASIX process and terminal imports as well; CPython WASIX exposes the same process-group, session, and terminal process-group calls through `os` where CPython expects POSIX support. CPython WASIX also maps `os.getpid`, `os.getppid`, and `os.kill` to the runtime process ID, parent process, and signal imports, and maps `os.wait`/`os.waitpid` to WASIX `proc_join` with POSIX wait-status encoding for normal exits, signal exits, stopped children, nonblocking waits, and process-group wait selection. CPython WASIX also exposes `waitid` on the same runtime wait path for `P_ALL`, `P_PID`, and `P_PGID` child selection, returning Linux-style `siginfo_t` child status records for exited, signaled, and stopped children. The WASIX import surface also includes `proc_wait_status`, a non-consuming child-status observation path used by CPython `waitid(..., WNOWAIT)` so POSIX callers can inspect child status without reaping it. The CPython WASIX subprocess wrapper now lowers `shell=True` invocations to the pinned sandbox `sh -c` command path instead of falling back to CPython's ambient WASI subprocess behavior. The WASIX runtime process table now also carries guest-visible POSIX nice values, inherited by child processes. `proc_getpriority` and `proc_setpriority` apply POSIX process, process-group, and user selection against that process table; BusyBox WASIX and CPython WASIX route `getpriority`, `setpriority`, and `nice` through those imports, and generated procfs `stat`, `sched`, and `autogroup` files report the selected process's current nice state. WASIX processes now also carry inherited POSIX resource-limit state. `proc_getrlimit` and `proc_setrlimit` expose current-process and selected-pid resource limits through the runtime process table, with defaults derived from the sandbox memory and wall-time budgets. BusyBox WASIX routes `getrlimit` and `setrlimit` through those imports for shell `ulimit` workflows, and CPython WASIX routes `getrlimit`, `setrlimit`, and `prlimit` through the same runtime state instead of keeping a Python-local limit array. Generated `/proc/<pid>/limits` files report the selected process's runtime resource-limit state rather than fixed placeholders. BusyBox WASIX and CPython WASIX also route `getrusage` through the runtime process record, reporting sandbox-scoped elapsed user time and maxrss from the process resource-limit state. `proc_times` exposes the same runtime process clock in POSIX 100 Hz ticks, and BusyBox WASIX plus CPython WASIX route `times` through that import instead of computing independent bridge-local tick values. WASIX processes now carry guest-visible scheduler metadata as inherited process state. `proc_getscheduler` and `proc_setscheduler` expose that metadata for POSIX scheduler calls; BusyBox WASIX and CPython WASIX route `sched_getscheduler`, `sched_getparam`, `sched_setparam`, and `sched_setscheduler` through those imports, and generated procfs `sched` files report the selected process policy. The same scheduler bridge now carries inherited CPU affinity metadata and a guest-visible current CPU number. `proc_getaffinity`, `proc_setaffinity`, and `proc_getcpu` back BusyBox WASIX and CPython WASIX `sched_getaffinity`, `sched_setaffinity`, and `sched_getcpu`, so POSIX callers that inspect or save and restore CPU placement use shared runtime process state. BusyBox WASIX also routes guest-visible identity getters and setters (`getuid`, `geteuid`, `getgid`, `getegid`, `getresuid`, `getresgid`, `setuid`, `seteuid`, `setgid`, `setegid`, `setreuid`, `setregid`, `setresuid`, `setresgid`, and `setgroups`) through the same WASIX process identity imports used by CPython, so core userspace tools read and update the shared runtime process record without changing host credentials. The active CPython WASIX recipe also provides the missing libc `getgroups` symbol from Sinomo's `proc_getgroups` import, so CPython exposes `os.getgroups` through the same runtime-backed identity path as BusyBox. CPython WASIX now also exposes runtime-backed `gettid` through the WASIX thread table and a Linux-style `syscall` dispatcher for common process, identity, group/session, and signal syscalls, so native packages that reach below libc for those process basics still use Sinomo's process metadata instead of failing at link time. CPython WASIX direct identity syscalls now also include `setreuid`, `setregid`, `setfsuid`, `setfsgid`, `getgroups`, and `setgroups`, matching the runtime-backed libc identity wrappers. CPython WASIX also routes `get_phys_pages`, `get_avphys_pages`, `get_nprocs_conf`, and `get_nprocs` through the same runtime budget and scheduler view used by `sysconf` and `sysinfo`, so Python and native packages see one consistent sandbox CPU/memory topology. CPython WASIX `eventfd`, `eventfd_read`, and `eventfd_write` now lower to the WASIX eventfd descriptor import and ordinary 8-byte descriptor reads/writes, including the Linux syscall dispatcher paths for `eventfd` and `eventfd2`; this gives async runtimes and native extensions a real sandbox descriptor primitive instead of a missing symbol. CPython WASIX now also wraps libc `fork` and `vfork` directly to the WASIX `proc_fork` import and routes `syscall(SYS_fork)` plus `syscall(SYS_vfork)` through the same path, so Python and native extensions reach Sinomo's process substrate instead of relying on ambient libc behavior. Pidfds now participate in the same special descriptor readiness path as timerfd, signalfd, inotify, and fanotify descriptors: `poll`, `select`, and `epoll` observe a pidfd as readable once the corresponding process status is available through the runtime's non-consuming status query. The bridge also routes direct Linux-style syscall dispatch for `close`, `pipe`, `pipe2`, `dup`, `dup2`, and `dup3` through the same WASIX descriptor operations used by libc wrappers. That direct syscall dispatch also covers descriptor and file setup calls commonly used by native runtime code (`read`, `write`, `open`, `creat`, `openat`, `fcntl`, `fsync`, `fdatasync`, `sync`, `syncfs`, `ioctl`, `truncate`, `ftruncate`, `pread64`, `pwrite64`, `readv`, `writev`, `preadv`, `pwritev`, `close_range`, `stat`, `lstat`, `fstat`, `newfstatat`, `access`, `faccessat`, `faccessat2`, `mkdir`, `mkdirat`, `rmdir`, `unlink`, `unlinkat`, `rename`, `renameat`, `link`, `linkat`, `symlink`, `symlinkat`, `readlink`, `readlinkat`, `chmod`, `fchmod`, `fchmodat`, `chown`, `lchown`, `fchown`, `fchownat`, `umask`, `getcwd`, `chdir`, `fchdir`, `chroot`, `getrlimit`, `setrlimit`, `prlimit64`, `getrusage`, `time`, `times`, `getpgrp`, `getpriority`, `setpriority`, scheduler metadata and affinity syscalls, `nanosleep`, `clock_nanosleep`, `gettimeofday`, `clock_gettime`, `clock_getres`, I/O priority syscalls (`ioprio_get` and `ioprio_set`) backed by guest-visible process scheduling metadata, and memory-management calls backed by the active WASIX toolchain (`mmap`, `munmap`, `brk`, `msync`, `madvise`, `mprotect`, `mremap`, `mincore`, `mlock`, `munlock`, `mlockall`, `munlockall`, and `mlock2`). The direct futex syscall now covers the common Linux wait/wake family used by native runtimes (`FUTEX_WAIT`, `FUTEX_WAKE`, `FUTEX_WAIT_BITSET`, `FUTEX_WAKE_BITSET`, private futexes, realtime wait flags, and wake-side requeue, compare-and-requeue, and wake-op compatibility) on top of WASIX futex waits and wakes. WASIX thread-exit cleanup now also performs Linux-style `set_tid_address` clear-and-wake behavior and walks the current thread's robust futex list to mark owned futexes with `FUTEX_OWNER_DIED` before waking waiters, which gives pthread joins and robust mutex recovery a real runtime path instead of merely recording the metadata. TTY ioctls include Linux termios get/set operations and foreground process-group queries/updates, in addition to window sizing and nonblocking/readability controls, so shell startup paths can use either libc termios calls or direct `ioctl` requests. Direct socket syscalls and POSIX socket entrypoints (`socket`, `connect`, `accept`, `accept4`, `send`, `recv`, `sendto`, `recvfrom`, `sendmsg`, `recvmsg`, `sendmmsg`, `recvmmsg`, `shutdown`, `bind`, `listen`, `getsockname`, `getpeername`, `socketpair`, `setsockopt`, and `getsockopt`) now route through the same WASIX socket descriptor layer as ordinary POSIX calls, including scatter/gather data paths, batched message helpers, per-call `MSG_DONTWAIT`, and common socket flag, buffer-size, timeout, linger, type, protocol, domain, and TCP/IP options. Socket descriptors also carry sandbox-local metadata across `dup`, `dup2`, `dup3`, `accept`, and `socketpair`, so metadata queries do not depend on host socket state. `SO_PEERCRED` returns sandbox process identity instead of host credentials. Descriptor readiness syscalls (`poll`, `ppoll`, `select`, `pselect6`, `epoll_create`, `epoll_create1`, `epoll_ctl`, `epoll_wait`, `epoll_pwait`, and `epoll_pwait2`) also lower to the bridge's WASIX poll and epoll descriptor implementations. File allocation/advice syscalls (`fallocate`, `fadvise64`, and `readahead`) lower to the same WASI `fd_allocate` and `fd_advise` operations as their POSIX libc wrappers, while `sync_file_range` lowers range sync requests to the existing sandbox fd data-sync path. Additional direct syscalls for common native package probes and setup (`uname`, `ioctl`, `flock`, `sethostname`, `setdomainname`, `getrandom`, `memfd_create`, `sysinfo`, `getcpu`, `utime`, `utimes`, `futimesat`, and `utimensat`) now route to the same sandbox-backed libc wrappers and runtime metadata imports as normal POSIX calls. Process lifecycle syscalls used by native child-management code (`execve`, `exit`, `exit_group`, `wait4`, and `waitid`) now route to the same WASIX process exec, exit, and wait paths as the libc wrappers. Modern pidfd syscalls (`pidfd_open`, `pidfd_send_signal`, and `pidfd_getfd`) provide sandbox-local process descriptors for child tracking, signal delivery, same-process descriptor duplication, and `waitid(P_PIDFD, ...)`. `process_mrelease` accepts those sandbox pidfds, validates the target, and records the lifecycle operation inside the sandbox process model without host process memory reclamation. Linux `execveat` and `fexecve` now route through the same WASIX exec path as `execve`, including relative-to-dirfd execution and `AT_EMPTY_PATH` descriptor execution via the sandbox `/proc/self/fd` view when a launcher uses that entry point. Fork-style direct `clone` calls now route to the same WASIX process fork path, including parent and child TID stores for process-style native launchers, child-clear-TID state, child-side TLS-base state from `CLONE_SETTLS`, `CLONE_PARENT` parent inheritance through WASIX process-table reparenting, parent-side `CLONE_PIDFD` publication through the sandbox pidfd table, `CLONE_VFORK` launch through the WASIX vfork path for the common `CLONE_VM | CLONE_VFORK | SIGCHLD` process-creation pattern, child-side `CLONE_CLEAR_SIGHAND` reset for the CPython bridge's signal-handler table, and sandbox-local namespace creation flags; `clone3` now supports the same fork-style process creation path through Linux's newer structured argument form and keeps the 64-bit `clone3` flag field intact before validation, while treating requested TID arrays as sandbox-local launch metadata. `CLONE_SYSVSEM` and `CLONE_IO` are accepted as sandbox-local process metadata on fork-style clones instead of rejecting the clone, because the sandbox does not expose host System V semaphore adjustment or I/O scheduler contexts. Linux `prctl` process-control calls now maintain sandbox-local process state for process name, dumpability, no-new-privs, parent-death signal, seccomp mode/filter probes, child-subreaper status, keep-caps, securebits, capability bounding set, timer slack, process timing mode, clear-child-TID address, transparent-hugepage preference, I/O flusher status, ptracer selection, and anonymous VMA names. The Linux `seccomp` syscall now records sandbox-local strict/filter state, reports supported filter actions and notification structure sizes, and provides sandbox-local listener descriptors when native runtimes ask for user-notification filters, without applying host process filters. Modern Linux Security Module syscalls (`lsm_get_self_attr`, `lsm_set_self_attr`, and `lsm_list_modules`) now expose sandbox-local LSM attribute state and the sandbox Landlock module identity for runtimes and language packages that probe the post-6.8 LSM userspace ABI. Additional Linux process/runtime probe syscalls (`personality`, `set_tid_address`, `set_robust_list`, `get_robust_list`, `capget`, `capset`, `unshare`, and `setns`) now keep guest-visible sandbox-local state for compatibility with native runtime startup paths without changing host process personality, host thread bookkeeping, host namespaces, or host privileges; namespace joins validate the descriptor and remain sandbox-local metadata rather than entering host namespaces. Modern scheduler and thread-runtime probes (`sched_getattr`, `sched_setattr`, `membarrier`, `rseq`, and `arch_prctl`) now lower to the sandbox scheduler metadata, guest memory-ordering barrier, restartable-sequence registration state, and architecture TLS-base state used by native runtime startup code. WASI `poll_oneoff` clock subscriptions now accept the runtime's process and thread CPU clocks in addition to realtime and monotonic clocks, using the same platform clock source that backs `clock_time_get`, so language runtimes do not hit an unsupported-clock error when polling CPU-clock subscriptions. NUMA memory-policy syscalls (`set_mempolicy`, `get_mempolicy`, `mbind`, `migrate_pages`, `move_pages`, and `set_mempolicy_home_node`) now expose a coherent single-node sandbox policy model for allocators and numerical libraries that probe Linux NUMA behavior. Data movement and file offset syscalls (`lseek`, `sendfile`, `copy_file_range`, `splice`, `vmsplice`, `preadv2`, and `pwritev2`) now route through the same WASI fd seek, vector I/O, and bridge read/write copy paths as the libc wrappers. Same-process memory transfer syscalls (`process_vm_readv` and `process_vm_writev`) now copy between guest iovec ranges inside the sandbox memory model, giving native debuggers and runtime probes a real current-process path without exposing host process memory. Same-process `process_madvise` now applies the existing sandbox `madvise` behavior across guest iovec ranges for native runtimes that use the newer process-scoped advisory interface. Current-process `kcmp` now compares sandbox-visible process resources and file descriptor identities, so runtimes that check descriptor/process sharing use sandbox metadata instead of missing raw Linux syscall behavior. Direct `futex` wait/wake syscalls and the newer futex2 `futex_wait` and `futex_wake` syscalls now route to the WASIX futex substrate, giving native thread runtimes a real sandbox blocking primitive when they bypass libc. Direct directory enumeration syscalls (`getdents` and `getdents64`) now fill Linux dirent64 records from the sandbox directory descriptor via `fdopendir`/`readdir`. Filesystem status syscalls (`statfs` and `fstatfs`) now return Linux-style statfs records derived from the same sandbox-backed `statvfs` budget view. Modern metadata syscall `statx` now maps sandbox `fstatat` results into Linux statx records, including mode, ownership, size, block count, inode, timestamps, and device metadata, and uses `fstat` for `AT_EMPTY_PATH` fd-targeted metadata reads. `statx` also reports the Linux attribute mask understood by the sandbox, with unset attributes represented explicitly rather than omitted. Modern cache-introspection syscall `cachestat` now reports sandbox-visible file range page counts from sandbox file metadata, with dirty/writeback/eviction fields represented as zero because the WASIX sandbox does not expose host page cache state. Modern mount-introspection syscalls (`statmount` and `listmount`) now expose a concrete sandbox root SIFS mount id, filesystem type, mount point, mount namespace id, options, and uid/gid maps for newer libmount-style probes without requiring callers to parse procfs mount text. Modern path operation syscalls (`openat2` and `renameat2`) now route to the sandbox VFS. `openat2` accepts the common Linux resolve flags used by secure file-open code, rejects sandbox escapes for `RESOLVE_BENEATH` and `RESOLVE_IN_ROOT`, rejects sandbox magic-link prefixes for `RESOLVE_NO_MAGICLINKS` and `RESOLVE_NO_SYMLINKS`, and maps `RESOLVE_NO_SYMLINKS` onto no-follow open behavior for the final path component. `renameat2(RENAME_NOREPLACE)` enforces no-replace semantics before calling the regular rename path. Modern permission syscall `fchmodat2` now routes to the same sandbox chmod path as `fchmodat`, including `AT_EMPTY_PATH` handling for fd-targeted mode updates. Related fd-targeted `AT_EMPTY_PATH` handling now also covers direct `newfstatat`, `faccessat2`, and `fchownat` ownership updates through the existing sandbox `fstat`, access-check, and `fchown` paths. Linux handle-based file lookup (`name_to_handle_at` and `open_by_handle_at`) now has sandbox-local handle records keyed by sandbox file identity. Handles round-trip through the Linux `file_handle` ABI, preserve the sandbox boundary by reopening only previously resolved sandbox paths, and return `ESTALE` if the record no longer matches the target file identity. Extended attribute libc calls and direct syscalls (`setxattr`, `lsetxattr`, `fsetxattr`, `getxattr`, `lgetxattr`, `fgetxattr`, `listxattr`, `llistxattr`, `flistxattr`, `removexattr`, `lremovexattr`, and `fremovexattr`) now use sandbox-local xattr state keyed by sandbox file identity, so Python and native packages can store, enumerate, read, and remove guest-visible attributes without depending on host filesystem xattr support. Interval timer syscalls (`getitimer`, `setitimer`, and `alarm`) now route through the same sandbox process timer bridge as libc. POSIX timer libc calls and direct syscalls (`timer_create`, `timer_settime`, `timer_gettime`, `timer_getoverrun`, and `timer_delete`) now maintain sandbox-local timer IDs, clock bindings, interval/value state, remaining-time calculation, and overrun counts from sandbox clock reads. Linux timerfd calls (`timerfd_create`, `timerfd_settime`, and `timerfd_gettime`) now expose descriptor-backed timers with blocking and nonblocking expiration reads through the sandbox fd layer, so event-loop runtimes can use timer descriptors without host timerfd access. Linux inotify calls (`inotify_init`, `inotify_init1`, `inotify_add_watch`, and `inotify_rm_watch`) now provide sandbox-local watch descriptors and fd-readable inotify event records, including watch-removal notifications and queue overflow records. Sandbox file operations emit matching create, delete, move, open, truncate/modify, and attribute-change events for watched paths and parent directories, so file-watch/event-loop libraries can use the standard descriptor API inside the sandbox. Polling syscalls now understand sandbox-local timerfd, signalfd, and inotify readiness directly, and the epoll wrapper keeps shadow interest records for those descriptors so `poll`, `select`, and `epoll_wait` can drive normal Linux-style event loops. Fanotify syscalls (`fanotify_init` and `fanotify_mark`) now provide sandbox-local notification descriptors, mark tables, and Linux metadata records for the same sandbox path mutation stream. Fanotify events report sandbox metadata with `FAN_NOFD` rather than host file descriptors, preserving the sandbox filesystem boundary while giving file-aware runtime probes and package tools a real readiness/read path. Direct `rt_sigaction`, `rt_sigprocmask`, `rt_sigpending`, `rt_sigtimedwait`, `rt_sigqueueinfo`, `rt_tgsigqueueinfo`, `rt_sigsuspend`, `sigaltstack`, `signalfd`, `tkill`, `tgkill`, and `pause` syscalls also route through the same sandbox signal-action, mask, pending-signal, and process-signal wrappers used by libc; alternate signal stacks are tracked as guest-visible process state, and signalfd descriptors read pending sandbox signals through the normal fd path, so native child setup code that calls `syscall(...)` manipulates sandbox descriptors consistently. CPython WASIX also prevents `SIGKILL` and `SIGSTOP` from entering the blocked signal mask through `sigprocmask` or `sigsuspend`, matching POSIX non-blockable signal behavior while keeping the sandbox-local mask state. The same active recipe wraps libc `umask`, `chmod`, `fchmod`, `chown`, `lchown`, and `fchown` so Python package scripts use Sinomo's runtime-backed mode and ownership imports instead of the current WASIX sysroot stubs. The active recipe routes `fchmodat` and `fchownat` through the same imports as well, including `AT_FDCWD` and directory-fd-relative paths via WASIX `fchdir`. It also wraps libc `kill`, `killpg`, `wait`, `waitpid`, `wait3`, `wait4`, `getpgid`, `getpgrp`, `getsid`, `setpgid`, `setsid`, `tcgetpgrp`, and `tcsetpgrp` to the Sinomo WASIX process, wait, and terminal process-group imports so CPython's native `os` module observes the same sandbox process table as Bash and BusyBox. The CPython bridge also wraps `prctl` and routes raw `clone` syscalls for process-style cloning through the same WASIX process table, giving native package launchers and runtime probes a Linux-compatible process-control surface inside the sandbox. It also wraps libc user/group identity and process ID calls (`getuid`, `geteuid`, `getgid`, `getegid`, `setuid`, `seteuid`, `setfsuid`, `setgid`, `setegid`, `setfsgid`, `getpid`, `getppid`) to the sandbox identity/process imports, and exposes real `fchdir` through the WASIX descriptor cwd import. Related CPython identity calls (`getresuid`, `getresgid`, `setreuid`, `setregid`, `setresuid`, `setresgid`, `setgroups`, and `initgroups`) are backed by the same runtime process identity state, accepting guest-visible identity changes without changing host credentials or SIFS ownership. CPython WASIX also exposes `lockf` and `fcntl.flock` by lowering both to the same runtime-backed advisory file-lock path used by `fcntl`, so Python packages that coordinate work through POSIX or BSD-style advisory locks can use one consistent sandbox lock primitive. The CPython WASIX fcntl bridge now also exposes Linux OFD locks, memfd seals, `F_DUPFD_CLOEXEC`, and pipe-size get/set metadata through sandbox descriptor state, so native extensions and package tooling can use the common Linux descriptor-control ABI without falling through to host-specific behavior. Memfd seals are enforced on the sandbox write, vector-write, pwrite, pwritev, and truncate paths, including write/grow/shrink restrictions. WASIX command execution now also installs the effective sandbox memory, wall-time, and task budgets into process resource-limit metadata before startup. Child processes inherit those limits across fork, spawn, and exec even if the guest replaces its environment. The runner also passes memory, wall-time, output, workspace, and file-count budgets into the guest environment, and CPython WASIX wraps `uname`, `gethostname`, `getrlimit`, `setrlimit`, `prlimit`, `getpagesize`, and `getloadavg` so Python build scripts and native packages see sandbox-scoped platform and resource metadata instead of host metadata or missing libc stubs. That resource-discovery surface now also covers `sysinfo`, `get_nprocs`, `get_nprocs_conf`, `get_phys_pages`, and `get_avphys_pages`, with `sysinfo` reporting sandbox memory-budget data instead of host machine totals. CPython WASIX also exposes the WASI-backed `eventfd`, `eventfd_read`, and `eventfd_write` entrypoints for event notification descriptors used by native runtime libraries. CPython WASIX also routes `getrandom` and `getentropy` through WASI `random_get`, so Python randomness APIs and package code use the runtime randomness source instead of device-file fallbacks. Classic libc PRNG and environment/allocation helpers (`random`, `srandom`, `initstate`, `setstate`, the `drand48` family, `reallocarray`, `clearenv`, `secure_getenv`, `strtof_l`, `strtod_l`, `strtold_l`, `l64a`, and `a64l`) now have sandbox-local implementations for extension probes and runtime libraries. The floating-point environment surface from `<fenv.h>` is also present with the WASIX sysroot's fixed round-to-nearest behavior and no dynamic exception flags, covering numerical-library configure probes without depending on host FPU state. CPython WASIX now builds the `termios` module against a sandbox TTY surface: `tcgetattr`, `tcsetattr`, speed helpers, raw-mode conversion, window-size helpers, drain/flush/flow operations, `tcgetsid`, and the standard winsize and nonblocking `ioctl` requests are backed by the runtime's terminal state rather than host terminal state. The Bash and BusyBox WASIX recipes also link termios wrapper objects so shell and core-userspace terminal paths synchronize the POSIX `TOSTOP` bit with the same runtime terminal state. Time conversion and formatting now uses a deterministic UTC sandbox model: `gmtime_r`, `localtime_r`, `timegm`, `mktime`, `tzset`, `strftime_l`, `asctime_r`, and `ctime_r` are backed without consulting host timezone state. Memory-advice helpers (`madvise` and `posix_madvise`) now accept the standard POSIX advisory modes, and Linux-specific `madvise` modes such as free, fork-inheritance, huge-page, dump, cold/pageout, populate, and collapse hints are accepted as sandbox-local advisory metadata, matching their advisory contract while letting native extensions and allocators use normal runtime paths. Memory mapping control helpers now include `mprotect`, which records sandbox-visible VMA protection metadata, and `mremap`, which supports movable anonymous remaps on top of the existing sandbox mmap/munmap substrate. The `map_shadow_stack` syscall now maps sandbox-local anonymous memory through the same substrate and optionally writes the guest-visible restore token requested by modern runtime probes. The `mseal` syscall records sealed VMA ranges and the bridge enforces those seals against later `mprotect`, `munmap`, and `mremap` changes made through the sandbox compatibility layer. Memory residency helpers (`mlock`, `munlock`, `mlockall`, `munlockall`, `mlock2`, and `mincore`) now expose Linux-compatible sandbox-local behavior: guest memory is treated as resident from the sandbox perspective, with argument validation and direct syscall routing for native allocators and numerical libraries that probe page residency or memory-lock APIs. CPython WASIX now builds with the WASI emulated mman library enabled, exposing `sys/mman.h`, `mmap`, `munmap`, and `msync` for Python's `mmap` module and native packages that use memory-mapped sandbox files. The CPython bridge also backs POSIX shared-memory names with sandbox-local files under `/dev/shm` through `shm_open` and `shm_unlink`, so Python and native code can use the standard shared-memory API inside the sandbox filesystem boundary. Explicit memory clearing helpers (`explicit_bzero` and `explicit_memset`) now use volatile byte writes so CPython and native packages can request optimization-resistant buffer clearing through normal libc symbols. String and signal-description compatibility helpers (`strlcpy`, `strlcat`, `strnlen`, `stpcpy`, `stpncpy`, `strdup`, `strndup`, `strcasecmp`, `strncasecmp`, `strcasestr`, `strsep`, `strcoll_l`, `strxfrm_l`, `strerror_l`, `strverscmp`, `basename`, `dirname`, `fnmatch`, `strchrnul`, `memrchr`, `glob`, `globfree`, `mempcpy`, `memccpy`, `memmem`, `strsignal`, and BSD `err.h` diagnostics) now provide the expected libc behavior for CPython and native-extension configure probes. POSIX `search.h` hash-table, linear-search, queue, and tree helpers (`hcreate`, `hsearch`, `lfind`, `lsearch`, `insque`, `remque`, `tsearch`, `tfind`, `tdelete`, `twalk`, and `tdestroy`, including reentrant hash-table variants) are also available for package configure checks and small C runtimes. `stdio_ext.h` buffering and stream-introspection helpers are backed conservatively over normal `FILE *` streams. `syslog.h` logging calls write sandbox-local diagnostics to stderr while preserving log mask and identity state. `setlocale`, `localeconv`, locale handle helpers, and `nl_langinfo` now expose a stable sandbox `C`/`C.UTF-8` locale surface, wide-character display-width helpers (`wcwidth` and `wcswidth`), wide-string duplication/copy/length helpers, locale-stable case-insensitive wide-string comparison, and wide-character collation/transform helpers (`wcscoll` and `wcsxfrm`) use deterministic codepoint ordering. Stateless UTF-8 conversion helpers (`mbrtowc`, `mbrlen`, `wcrtomb`, `mbtowc`, `mbsinit`, `mbsrtowcs`, `mbsnrtowcs`, `wcsrtombs`, `wcsnrtombs`, `wcsftime`, `btowc`, and `wctob`) plus classic `mbstowcs`/`wcstombs` and wide numeric conversions cover the narrow/wide string, number parsing, and time-formatting paths used by Python locale initialization and package probes without depending on host locale state. Wide-character stdio (`fgetwc`, `fputwc`, `fgetws`, `fputws`, `ungetwc`, their standard aliases, and unlocked variants) now routes through the same UTF-8 conversion layer over normal `FILE *` streams. CPython WASIX also wraps `statvfs` and `fstatvfs` so `os.statvfs`, fd-based statvfs calls, and higher-level Python disk-usage helpers report the sandbox workspace capacity and file-count budget. Temporary-file helpers (`tmpfile`, `tmpnam`, `tmpnam_r`, `tempnam`, `mkstemp`, `mkostemp`, `mkstemps`, `mkostemps`, `mkdtemp`, and `mktemp`) now allocate names, atomically opened files, directories, or anonymous opened files under sandbox `/tmp`, so Python packages using libc temp-file fallbacks stay inside the sandbox filesystem. The same CPython bridge now wraps common directory-fd filesystem APIs (`access`, `faccessat`, `openat`, `mkdirat`, `unlinkat`, `readlinkat`, `symlinkat`, `linkat`, and `renameat`) through the WASIX `fchdir` path, giving Python `dir_fd` operations and package build scripts real sandbox filesystem behavior instead of falling back to missing libc stubs. The bridge also wraps `pathconf`, `fpathconf`, `sysconf`, and `confstr` so Python and native package probes for page size, CPU count, path limits, open-file limits, and default command search paths resolve to sandbox-scoped values. CPython WASIX also wraps terminal and login identity calls (`isatty`, `ttyname`, `ttyname_r`, `ctermid`, `getlogin`, `getlogin_r`, and `getpass`) so shells and Python packages see the sandbox's virtual `/dev/tty` and fixed sandbox user rather than host terminal metadata. `getpass` prompts through sandbox stderr, reads from `/dev/tty` when available, falls back to stdin, and uses the sandbox termios state to suppress echo while reading. The cross configuration also exposes `paths.h` and `sysexits.h`, so native packages can use the sysroot's standard path and exit-code constants for sandbox-local `/bin/sh`, `/dev/tty`, `/tmp`, and command status conventions. The CPython WASIX recipe now also exposes the `pwd` and `grp` modules through a sandbox-local passwd/group database containing the fixed `sinomo` user and group, with `getpwuid`, `getpwnam`, `getpwent`, `getgrgid`, `getgrnam`, `getgrent`, `getgrouplist`, and their reentrant forms backed by the runtime UID/GID imports. These libc entrypoints are linked through wrappers so CPython uses the sandbox database rather than sysroot placeholder account data. The same account bridge now provides `shadow.h` entrypoints for the fixed sandbox account (`getspent`, `getspnam`, `fgetspent`, `sgetspent`, `putspent`, lock helpers, and reentrant lookup), so Python packages and native configure scripts that inspect shadow account data see sandbox-local state consistent with the passwd/group database. CPython WASIX configure now also enables `utime`, `futimens`, `futimes`, `futimesat`, `lutimes`, `utimensat`, and `utimes`, routing Python `os.utime` and older POSIX timestamp callers, including fd, `dir_fd`, and symlink-sensitive forms, through the runtime's existing WASI timestamp syscalls for sandbox files. Direct `utimensat` now also supports fd-targeted `AT_EMPTY_PATH` timestamp updates through the sandbox descriptor timestamp path. The same bridge now exposes process priority and process-times entrypoints (`nice`, `getpriority`, `setpriority`, `times`, and `getrusage`) with priority tracked as sandbox-local process metadata and process usage reported from the sandbox process clock and resource-budget view. The CPython bridge also now exposes file sizing, allocation, sync, positional I/O, and vector I/O entrypoints (`truncate`, `ftruncate`, `posix_fallocate`, `posix_fadvise`, `fsync`, `fdatasync`, `pread`, `pwrite`, `preadv`, `pwritev`, `readv`, and `writev`) through WASI descriptor operations, exposes `sync` and `syncfs` on top of the sandbox sync path, plus `memfd_create` as an unlinked sandbox-local temporary file descriptor under `/tmp` with `MFD_CLOEXEC`, `memfd_secret` as the same sandbox-local anonymous descriptor class with close-on-exec and shrink/seal metadata, plus file-copy helpers (`copy_file_range` and `sendfile`) through sandbox-local descriptor reads and writes, so Python packages and native build helpers can use the normal POSIX file APIs against sandbox files. WASIX `fd_sync` and `fd_datasync` now also accept directory and root descriptors as successful metadata-sync barriers inside the sandbox namespace, matching installer and package-manager patterns after atomic renames, while still rejecting pipe, socket, event, epoll, and symlink descriptors as invalid sync targets. The common path APIs (`getcwd`, `get_current_dir_name`, `getwd`, `realpath`, `chdir`, `mkdir`, `rmdir`, `unlink`, `readlink`, `symlink`, `link`, and `rename`) now route through the same WASIX cwd and sandbox path bridges used by the directory-fd forms, so CPython can expose both direct path and `dir_fd` operations consistently. CPython WASIX also now uses WASIX-backed descriptor IPC and readiness primitives (`pipe`, `pipe2`, `socketpair`, `dup`, `dup3`, `close_range`, `closefrom`, `epoll_create`, `epoll_create1`, `epoll_ctl`, `epoll_wait`, `epoll_pwait`, `poll`, `ppoll`, `select`, and `pselect`), which improves Python subprocess, selector, and event-loop compatibility without routing through host process state. The CPython WASIX cross configuration also exposes the sysroot's real `spawn.h` and `regex.h` surfaces, including `posix_spawn`, `posix_spawnp`, spawn attributes/file actions, and POSIX regex compile/execute/error/free entrypoints, so Python and native package builds can choose those process and text-matching APIs through normal configure checks. It now also advertises WASIX libc's `fork` and `vfork` entrypoints, which lower to Sinomo's `proc_fork` and `proc_fork_env` runtime paths instead of host process creation. It also exposes the sysroot's maintained `nftw`, `getopt`/`getopt_long`, `iconv`, and gettext entrypoints for filesystem walking, option parsing, character-set conversion, and localization probes used by native packages. The recipe links the sysroot `libcrypt` and exposes `crypt.h`, `crypt`, `crypt_r`, `monetary.h`, `strfmon`, and `strfmon_l` for account/password and locale-sensitive formatting probes that expect the standard POSIX headers. CPython WASIX also exposes the scheduler entrypoints (`sched_yield`, `sched_get_priority_max`, `sched_get_priority_min`, `sched_getscheduler`, `sched_getparam`, `sched_setparam`, `sched_setscheduler`, and `sched_rr_get_interval`) against the sandbox's fixed cooperative scheduling model. Signal-facing CPython APIs now also have sandbox-local disposition and mask state (`sigaction`, `signal`, `sigemptyset`, `sigfillset`, `sigaddset`, `sigdelset`, `sigismember`, `sigprocmask`, `pthread_sigmask`, `sigpending`, `raise`, `sigwait`, `sigwaitinfo`, `sigtimedwait`, `sigsuspend`, `pause`, `sighold`, `sigrelse`, `sigignore`, `siginterrupt`, `sigpause`, `sigqueue`, and `pthread_kill`), with default signal delivery routed through WASI `proc_raise`. POSIX timer and sleep entrypoints (`getitimer`, `setitimer`, `alarm`, `ualarm`, `clock_nanosleep`, `sleep`, and `usleep`) now route through the sandbox timer state, WASIX interval signal delivery, and libc sleep behavior. Terminal-generated signals now route through the WASIX control plane and are delivered to every live process in the terminal process's recorded process group, instead of only the single process handle attached to the TTY. The TTY line discipline now maps Ctrl-C, Ctrl-\ , and Ctrl-Z to `SIGINT`, `SIGQUIT`, and `SIGTSTP` respectively through that same foreground group path. Background process groups that attempt to read from the terminal now receive `SIGTTIN` through the normal signal path before the read proceeds. If the process remains in the background after signal handling, the read returns an I/O error, matching the POSIX behavior shells rely on when `SIGTTIN` is ignored or blocked. Background callers of `tcsetpgrp` now receive `SIGTTOU` before foreground terminal ownership is changed, so job-control shells can use the same signal/disposition flow they expect on Linux. The task status model now records stopped processes for `SIGSTOP`, `SIGTSTP`, `SIGTTIN`, and `SIGTTOU`, returns them to running state on `SIGCONT`, reports stopped processes as `T` in procfs `stat`/`status`, and lets `proc_join` return `JoinStatusType::Stopped` when callers use `WAKE_STOPPED`. Stop-aware joins cover exact child waits, any-child waits, and process-group-filtered waits; a wait first reports an already-stopped matching child and, when blocking, wakes on either a matching child stop or a matching child exit. A stopped child is reported once per stop event, and `SIGCONT` clears that report state so a later stop can be reported again, matching the behavior shell job-control wait loops expect. WASIX syscall pending operation processing now also parks a stopped thread on its signal queue, so the guest stops executing at the next syscall/pending-operation boundary until `SIGCONT` or a terminating signal arrives. WASIX `proc_signal` now supports POSIX process-selection forms even though the ABI transports `pid` as an unsigned integer: `0` targets the caller's recorded process group, two's-complement negative values target `-pgid`, and `-1` targets all live WASIX processes. `Signone` performs the same existence probe without delivery. Group broadcasts signal each selected process directly rather than using the per-process child-forwarding path, avoiding duplicate delivery when a process is waiting on children in the same group. Child status changes now notify the recorded parent process with `SIGCHLD` when a child stops, continues, or terminates. The default signal path ignores `SIGCHLD`, while guests that install a WASIX signal handler can observe the same status-change notification that POSIX shells and job-control code expect. Default signal handling now treats the normal POSIX terminate-by-default signal set as process-ending signals in the process status, default no-handler path, and asyncify polling path, rather than only terminating for the smaller interrupt/kill subset. Signal exits are preserved in task status and surfaced through blocking and nonblocking `proc_join` as `JoinStatusType::ExitSignal` with the terminating signal instead of being flattened into a normal exit. Stored signal dispositions are also honored by the no-handler and asyncify polling paths, so `Disposition::Ignore` suppresses default signal termination for spawned or forked children that inherit an ignored signal action, and ignored signals are also suppressed before dispatch to a registered WASIX signal handler. Signal delivery no longer pre-applies ignorable default terminate/stop effects before the disposition check runs; only non-ignorable `SIGKILL` and `SIGSTOP` update task state immediately at delivery time. `SignalDisposition::Default` now clears an inherited signal override during spawn/fork preparation instead of persisting a spurious default override entry, and the initial `WasiEnvBuilder` state also filters default dispositions out of the stored override map. The registered-handler path now also treats `SIGKILL` and `SIGSTOP` as non-catchable default actions before invoking the guest signal handler. Parent `SIGCHLD` notification now tracks the point where child state actually changes: non-ignorable `SIGKILL`/`SIGSTOP` and `SIGCONT` notify at delivery, while ignorable stop signals notify only after disposition processing applies the stop. The CPython WASIX bridge now mirrors those POSIX signal-disposition boundaries: `sigaction` rejects attempts to catch or ignore `SIGKILL` and `SIGSTOP`, and the default local `raise` path ignores `SIGCHLD`, `SIGURG`, and `SIGWINCH` instead of forwarding them as terminating process signals. Local CPython WASIX handler delivery also honors `SA_RESETHAND` and `SA_NODEFER`: a handler can be reset to the default disposition on delivery, and the handler's `sa_mask` plus the delivered signal are temporarily added to the sandbox signal mask unless the handler explicitly requests nested delivery. `sigsuspend` now consumes an unblocked pending sandbox signal through that same handler/default-action delivery path before restoring the original mask and returning `EINTR`, instead of discarding the pending signal. Pending signals whose current disposition is ignored, including default-ignored `SIGCHLD`, `SIGURG`, and `SIGWINCH`, are consumed without waking `sigsuspend`. Pending signal state in the CPython WASIX bridge now also keeps sandbox-local `siginfo_t` data for blocked self-directed `raise`, `sigqueue`, `rt_sigqueueinfo`, and `rt_tgsigqueueinfo` delivery, so `sigtimedwait` and `signalfd` can observe queued sender/value metadata instead of only the signal number. Directory reads of `/proc/<pid>/fd` and `/proc/<pid>/fdinfo`, including matching task paths, enumerate the selected live process's recorded descriptor table, and `/proc/<pid>/fdinfo/<n>` is generated from that selected-process snapshot. Readlink and symlink stat for `/proc/<pid>/fd/<n>` also use the recorded selected-process descriptor target, so descriptor probes no longer alias every numeric process back to the inspecting process's fd table. Offset-changing `fd_read`, `fd_write`, and `fd_seek` paths refresh the same descriptor snapshot so fdinfo `pos` tracks normal descriptor movement. Descriptor snapshots preserve known sandbox path targets for file, directory, and symlink descriptors, falling back to synthetic Linux-style labels only for runtime-only descriptors such as pipes, sockets, eventfds, and epoll handles. Fdinfo `flags` now combines the recorded access mode, append/nonblocking/sync status flags, and close-on-exec state, and `fd_renumber`, `fd_fdstat_set_flags`, and `fd_fdflags_set` refresh the process descriptor snapshot after successful mutation. Numeric process task files such as `/proc/<pid>/task/<tid>/status`, `/proc/<pid>/task/<tid>/cmdline`, and `/proc/<pid>/task/<tid>/environ` now validate `<tid>` against the selected process's live thread table and expose the same selected-process procfs metadata. Directory reads of `/proc/<pid>/task` also enumerate the selected process's live threads for every live WASIX process, not only the caller's process. Task symlink paths such as `/proc/<pid>/task/<tid>/cwd` and `/proc/<pid>/task/<tid>/exe` also validate the selected thread before resolving through the selected process's cwd and executable metadata. Numeric process and task namespace symlinks under `/proc/<pid>/ns/*` and `/proc/<pid>/task/<tid>/ns/*` now support readlink and stat probes after validating the selected process and task. Numeric process and task attribute files under `/proc/<pid>/attr/*` and `/proc/<pid>/task/<tid>/attr/*` now expose the same selected-process unconfined/empty security attribute view as `/proc/self/attr/*`. Task-path aliases now validate the requested `<tid>` against the live WASIX thread table before resolving `/proc/self/task/<tid>/...` to the current process view, so stale or invented task IDs do not silently alias to the active thread. The same procfs mount now snapshots common read-only system probe files (`/proc/cpuinfo`, `/proc/meminfo`, `/proc/stat`, `/proc/uptime`, `/proc/loadavg`, `/proc/version`, `/proc/buddyinfo`, `/proc/crypto`, `/proc/devices`, `/proc/diskstats`, `/proc/filesystems`, `/proc/locks`, `/proc/modules`, `/proc/partitions`, `/proc/swaps`, `/proc/vmstat`, `/proc/zoneinfo`, `/proc/pressure/{cpu,io,memory}`, `/proc/mountinfo`, and `/proc/self/mountinfo`) into the per-run sandbox proc tree. It also provides common `/proc/sys` identity and VM probe files such as `kernel/{ostype,osrelease,version,hostname,domainname,pid_max,threads-max,ngroups_max,cap_last_cap}`, `kernel/{shmmax,shmall,shmmni,msgmax,msgmnb,msgmni,sem}`, `kernel/{core_pattern,core_uses_pid,hotplug,kptr_restrict,kstack_depth_to_print,modules_disabled,nmi_watchdog,panic,panic_on_oops,sysrq}`, `kernel/keys/{gc_delay,maxbytes,maxkeys,root_maxbytes,root_maxkeys}`, `kernel/{perf_event_paranoid,perf_event_max_sample_rate,sched_child_runs_first,sched_rr_timeslice_ms,sched_rt_period_us,sched_rt_runtime_us}`, `kernel/random/{boot_id,entropy_avail,poolsize,urandom_min_reseed_secs,write_wakeup_threshold,uuid}`, `kernel/yama/ptrace_scope`, `crypto/fips_enabled`, `fs/{file-max,file-nr,inode-nr,inode-state,aio-max-nr,aio-nr,dentry-state,nr_open,pipe-max-size,pipe-user-pages-hard,pipe-user-pages-soft}`, `fs/epoll/max_user_watches`, `fs/inotify/{max_user_watches,max_user_instances,max_queued_events}`, `fs/mqueue/{msg_default,msg_max,msgsize_default,msgsize_max}`, `net/core/{somaxconn,rmem_default,wmem_default,rmem_max,wmem_max,netdev_max_backlog,optmem_max}`, `net/ipv4/{ip_local_port_range,ip_forward,tcp_syncookies,tcp_fin_timeout,tcp_keepalive_time,tcp_keepalive_intvl,tcp_keepalive_probes,tcp_max_syn_backlog,tcp_retries1,tcp_retries2,tcp_rmem,tcp_wmem,tcp_window_scaling}`, `net/ipv6/conf/{all,default}/disable_ipv6`, `net/unix/max_dgram_qlen`, `user/{max_cgroup_namespaces,max_ipc_namespaces,max_mnt_namespaces,max_net_namespaces,max_pid_namespaces,max_time_namespaces,max_user_namespaces,max_uts_namespaces}`, and `vm/{overcommit_memory,overcommit_ratio,max_map_count,mmap_min_addr,drop_caches,min_free_kbytes,stat_interval,swappiness,vfs_cache_pressure,zone_reclaim_mode,dirty_ratio,dirty_background_ratio}`. On Linux hosts this uses the host procfs data available to the runtime where appropriate; on other hosts it falls back to a conservative virtual view so language runtimes and native libraries can still complete their startup probes. The generated procfs tree also includes a minimal `/proc/net` surface: `dev`, `route`, `if_inet6`, `tcp`, `tcp6`, `udp`, `udp6`, and `unix`. These files intentionally describe an empty sandbox network namespace while still letting resolvers, language runtimes, and native libraries complete ordinary network-stack discovery probes. WASIX runs also mount a Sinomo-owned read-only `/etc` when the sandbox has not explicitly mounted one. It provides standard files that real POSIX runtimes expect during startup and networking/user lookups: `passwd`, `group`, `nsswitch.conf`, `hosts`, `hostname`, `resolv.conf`, `services`, `protocols`, `gai.conf`, `issue`, `shells`, `profile`, `ld.so.conf`, `os-release`, `locale.conf`, `default/locale`, `machine-id`, `timezone`, `localtime`, `termcap`, and standard CA bundle paths under `/etc/ssl` and `/etc/pki/tls/certs` when a bundle is available on the execution node. The resolver file is copied from the local runtime host when available so DNS behavior follows the execution node; the identity files describe the sandbox's own root and `sinomo` users rather than host accounts. WASIX runs now also mount a Sinomo-owned read-only `/sys` when the sandbox has not explicitly mounted one. The initial sysfs surface covers CPU topology and memory-management probes used by Python native packages: `/sys/devices/system/cpu/{online,possible,present,kernel_max}`, `/sys/devices/system/node/{online,possible}`, and `/sys/kernel/mm/transparent_hugepage/{enabled,hpage_pmd_size}`. On Linux hosts these files mirror the execution node where available, otherwise they fall back to a conservative virtual topology. The sysfs mount also exposes a minimal cgroup v2 root under `/sys/fs/cgroup`, including `cgroup.controllers`, `cgroup.subtree_control`, `cgroup.procs`, `cgroup.type`, `cpu.max`, `cpu.weight`, `memory.current`, `memory.max`, `pids.current`, and `pids.max`, so language runtimes and native libraries can complete ordinary container/cgroup resource discovery without seeing the host cgroup tree. The sysfs mount also exposes `/sys/devices/system/cpu/cpuN` directories for the available CPU set, with common `online` and `topology/*` files used by CPU detection code. Host topology files are mirrored when present; otherwise each virtual CPU is reported as its own core in package 0. Each virtual CPU also has a `cache/index0` directory with common cache metadata files such as `coherency_line_size`, `level`, `type`, `size`, `shared_cpu_list`, and `shared_cpu_map`. The sysfs mount also exposes common `/sys/class` and `/sys/devices/virtual` entries for an empty loopback network device, console/tty metadata, and the standard memory character devices (`null`, `zero`, `full`, `random`, `urandom`). These entries let runtime probes discover the same device names that the Sinomo-owned `/dev` mount provides. The per-run `/dev` preopen also creates `/dev/shm` as a sandbox-local directory, giving Python and native-library workloads a tmpfs-style shared-memory path without persisting those entries into SIFS or mapping host `/dev/shm`. WASIX runs also provide writable sandbox-local `/run` and `/var` mounts when the sandbox has not explicitly mounted them. `/run` includes `/run/user/1000` and `/run/lock`; `/var` includes `tmp`, `log`, `cache`, and `lib` directories so language runtimes and POSIX tools can use standard runtime, cache, and temporary paths without touching the host filesystem. Direct WASIX runs also get a writable sandbox-local `/tmp` fallback when the sandbox has not explicitly mounted `/tmp`; staged instance runs still use the durable SIFS-backed `/tmp` preopen. They also provide writable sandbox-local `/home/sinomo` and `/root` directories when the sandbox has not explicitly mounted `/home` or `/root`, and default `HOME`, `USER`, `LOGNAME`, `SHELL`, `TMPDIR`, `XDG_CACHE_HOME`, `XDG_CONFIG_HOME`, and `XDG_DATA_HOME` environment values are injected only when the caller did not provide them. Locale defaults also use `LANG=C.UTF-8` and `LC_CTYPE=C.UTF-8` unless the caller supplies locale values, and terminal/timezone defaults use `TERM=dumb` and `TZ=UTC` unless the caller supplies them. Direct runs also get read-only `/usr` and `/opt` fallback mounts with standard `bin`, `lib`, `local`, and `share` directories unless the sandbox or toolchain provides those trees explicitly. The generated `/usr` tree includes `share/zoneinfo/{UTC,Etc/UTC}` and mirrors host compiled terminfo entries for `dumb`, `xterm`, and `xterm-256color` when they are available on the execution node. The device surface now includes `/dev/full`, `/dev/console`, `/dev/ptmx`, and `/dev/pts` in addition to the existing null/zero/random/stdio/fd/shm entries. Console opens are routed through the sandbox TTY/stdout descriptors, `/dev/pts` hosts sandbox-local PTY slave paths, and `/dev/full` reads as zeroes while storage-facing writes report a no-space condition. The WASIX Python compatibility layer now provides sandbox-local pseudo-terminal allocation through `posix_openpt`, `grantpt`, `unlockpt`, `ptsname`, `openpty`, `login_tty`, `forkpty`, `/dev/ptmx`, `/dev/pts/<n>`, and PTY ioctl handling. PTY master/slave descriptors are backed by WASIX duplex descriptors and share sandbox termios/window-size state so Python `pty`/terminal helpers and shell job-control setup paths can work against real descriptors. The `proc_exec3` vfork child-spawn path now propagates asyncify/spawn failures as syscall errors instead of unwrapping and panicking the host runtime. The WASIX fork child runner now reports `Noexec` when the forked module lacks the required `_start` or `wasi_thread_start` export, rather than panicking while re-entering the child process. The WASIX fork path now handles store-snapshot serialization failure as a runtime error path instead of unwrapping and panicking the host. Windows platform clock support now uses `GetProcessTimes` and `GetThreadTimes` for WASI process and thread CPU clocks, so guests using these clocks receive real syscall results instead of hitting host `unimplemented!` panics on Windows nodes. Non-Unix hosts now load sandbox-visible host entries with unknown file types as `Unknown` file inodes instead of panicking during path resolution, matching the Unix path's normal syscall-result behavior for special entries. WASI timestamp update helpers now propagate monotonic-clock failures as syscall errors instead of unwrapping and panicking the runtime while handling `*_SET_TIME_NOW` timestamp requests. Process signal timers, CPU backoff scheduling, and TTY mobile input de-duplication now handle monotonic-clock read failures without host panics, so runtime clock backend errors do not crash shell or subprocess execution paths. Joined children are removed from the WASIX control plane when they are collected through blocking joins, non-blocking joins, or any-child joins, and spawn setup failures remove their provisional process record before returning. This preserves zombie-like waitability until collection without accumulating already-collected processes indefinitely. `proc_signal` now follows the same missing-process behavior as the other process syscalls: unknown process IDs return `Badf`, while `Signone` is treated as a process-existence probe and does not deliver a signal to an existing process. `proc_join` now also understands the POSIX waitpid-style process selection that WASIX libc can encode through the unsigned `OptionPid`: `-1` waits for any child, `0` waits for a child in the caller's process group, and values below `-1` wait for a child in process group `-pid`. The same filtering applies to blocking and non-blocking joins, so shells and language runtimes can build on group-scoped waits instead of only exact child PIDs. When `WAKE_STOPPED` is set, those exact, any-child, and group-scoped blocking waits also wake for stopped children instead of requiring the child to exit first. `proc_fork` now treats failure to start the child WASM task as a real fork failure: it removes the provisional child from the parent child list and WASIX control plane, then rewinds the parent with a failure errno instead of reporting a successful child PID for a process that was never scheduled. Copy-memory `proc_fork` child re-entry now uses the main module handles for both static and dynamically linked WASIX modules, so dynamically linked children that were already admitted through the linker-instance-group path do not fail immediately as missing static module handles. The shared asyncify unwind/rewind helpers now also read asyncify exports from the main module handles instead of the static-only handle path, and `proc_fork` no longer rejects dynamically linked modules before attempting the vfork-style path. WASIX descriptor duplication now honors the syscall's minimum-result-fd semantics by allocating the first free descriptor at or above the requested minimum, and `posix_spawn` dup2 file actions now report descriptor installation failure instead of silently ignoring a failed target-fd insert. WASI `fd_renumber` and WASIX `posix_spawn` dup2 file actions now preserve the source descriptor's base rights instead of replacing them with inheriting rights, so child pipes and redirections keep their read/write capabilities. WASIX now exposes `fchdir` as a real descriptor-based current-directory change: directory descriptors are resolved through the sandbox inode tree, the process cwd metadata is synchronized after the change, and non-directory descriptors return `Notdir`. The maintained BusyBox WASIX recipe routes its compatibility `fchdir` shim to this runtime import instead of returning a fixed unsupported error. WASIX context creation now enforces the documented `() -> ()` entrypoint signature for both params and results, so context switching does not admit invalid guest entrypoints that would later fail after the context is scheduled. In-memory WASI buffer inodes now behave like seekable files for POSIX-style I/O: reads past the buffer end return EOF instead of slicing out of range, writes and pwrite honor the requested offset instead of appending unconditionally, append mode writes at the buffer end, sparse writes zero-fill the gap, `fd_seek(..., SEEK_END)` works against the buffer length, and `fd_sync`/`fd_datasync` succeed as in-memory flushes while updating the reported size. WASIX `path_open2` can also open existing buffer-backed inodes as regular file-like descriptors and honors truncate-on-open by clearing the buffer and reported size. Buffer-backed inodes now also behave like regular files during path handling: using one as an intermediate path component returns `Notdir` instead of panicking the host, and unlinking a buffer-backed file succeeds after removing the directory entry without requiring host-file cleanup. `path_rename` now always installs the renamed source inode at the target path, including when the target already existed, instead of leaving the old target entry in place after a successful rename. Renaming one directory entry for a multi-link regular file now moves only the directory entry and leaves the shared inode's backing path intact, so other hard links keep working after the rename. When `path_rename` replaces an existing target entry, the replaced inode's link count is decremented so stat metadata follows the directory-entry update. Directory rename now handles virtual-only directories by treating a missing backing filesystem source as an in-memory sandbox rename, updates the moved directory's parent pointer, and restores the source entry when a real backing rename fails. Renaming a single-link symlink now updates the stored symlink location metadata, so relative symlink targets continue to resolve from the link's new containing directory after `mv`. New virtual inodes now start with a baseline link count of one when their backing metadata does not provide one, giving hard-link, unlink, rename, and stat paths meaningful POSIX link metadata from creation. Virtual inode creation now also fills in a default file type when backing metadata does not provide one, so fd/path stat reports regular files, directories, symlinks, and known socket kinds consistently for in-memory sandbox objects. Path stat on buffer-backed inodes now reports a regular-file type and current buffer length instead of returning `Io`. Descriptor stat now refreshes file-backed and buffer-backed size/timestamp metadata from the live inode before returning `fd_filestat_get`, so stat reads do not depend on a prior sync/truncate path to observe ordinary writes. Path stat on the virtual root now reports a directory instead of falling through to a generic I/O error. `fd_readdir` now enforces `FD_READDIR`, includes all loaded virtual directory entries instead of only preopened entries, includes `.` and `..` for the virtual root with stable inode values, returns root entry names without a leading slash, and derives stable inode values for backing filesystem entries instead of reporting zero. Directory stat now falls back to virtual directory metadata when a sandbox directory has no backing filesystem metadata entry. `fd_readdir` now treats a missing backing directory for a loaded virtual directory as an empty backing set and still returns the directory's virtual entries. `fd_readdir` now validates directory cookies and entry-name lengths before host-size conversion, returning `Overflow` instead of truncating values. `path_unlink_file` now avoids link-count underflow if it encounters legacy or externally loaded zero-link metadata while still removing the directory entry. `path_remove_directory` now enforces the base descriptor's `PATH_REMOVE_DIRECTORY` right before mutating the directory tree, and it can remove empty virtual-only directories whose backing path has no host entry. `path_create_directory` now checks existence through the requested sandbox virtual path instead of feeding a backing path back through path resolution, so lazy-loaded existing files or directories are detected correctly, and it can create child directories below virtual-only parents by installing the sandbox directory inode even when no host parent exists. WASIX `path_open2` can now create regular in-memory sandbox files below virtual-only directories when the backing filesystem has no parent entry, so shell redirection and normal file writes can proceed through the virtual filesystem instead of failing immediately after virtual `mkdir`. `path_filestat_set_times` now updates the resolved target inode and backing file handle instead of accidentally mutating the base directory fd inode, so `touch`-style path timestamp updates affect the requested file. `path_link` now rejects directory/root sources and runtime-only descriptor sources instead of creating hard-link cycles or nonsensical aliases. Buffer-backed `fd_allocate` and `fd_filestat_set_size` now reject unrepresentable sizes with `Overflow` before resizing, keeping large POSIX file-size requests from wrapping through host `usize` conversion. `fd_read`, `fd_pread`, and `fd_write` now also validate file offsets before converting them to host `usize` values, returning `Overflow` rather than silently truncating large guest offsets. `fd_seek(..., SEEK_CUR)` now validates positive overflow and negative underflow before mutating the descriptor cursor, so a failed relative seek does not corrupt the open file offset. `fd_seek(..., SEEK_END)` now returns the computed end-relative offset for file and buffer descriptors instead of returning the stale pre-seek cursor value. WASIX stdout and stderr are captured through a Sinomo virtual file that emits bounded output chunks as guest writes happen, so operators and agents are not blocked on process exit before seeing command output. The same capture path checks the configured per-stream output limit before accepting a guest write, so a long-running WASIX command cannot accumulate unbounded stdout or stderr while waiting for process exit. WASIX stdout can also be teed to an fd-backed pipe while retaining the same capture and output-limit path, and the runtime has a regression that connects one running WASIX command's stdout pipe to another running WASIX command's stdin pipe. The local WASIX-only linear process-plan runner uses that endpoint inside one staged transaction boundary, and SIFS/daemon WASIX process plans reuse the same fd-backed path with direct virtual preopens backed by the sandbox VFS. `sinomo sandbox doctor` reports this as the WASIX `output-limit` budget capability alongside the buffered and fd-backed stdin `input-limit` guard, the process/thread `task-limit` guard, the deterministic `instruction-budget` guard, and the yielding-process `process-tree-timeout`/`process-tree-cancellation` guards, while still reporting exact wall-timeout and immediate cancellation as incomplete until non-yielding compute loops can be interrupted at the engine layer. The same doctor path reports WASIX `memory-limit` as a linear-memory guard: the runner rejects modules whose imported or defined memories are unbounded or declare initial/maximum sizes above the sandbox memory budget, and it installs Sinomo Wasmer tunables that reject host/VM memory creation above the same budget. This is still linear-memory budget enforcement rather than full process-level memory accounting for every host-side runtime allocation. The WASIX process resource-limit table is also wired into concrete runtime behavior for open-file and file-growth limits: `RLIMIT_NOFILE` bounds descriptor allocation through the process fd table and stays synchronized when the current process calls `setrlimit`; direct descriptor-moving paths such as WASI `fd_renumber` and POSIX-spawn `dup2` file actions now respect the same maximum target fd. `RLIMIT_FSIZE` caps regular-file writes at the process file-size limit and rejects oversized `ftruncate`/allocation growth with `SIGXFSZ`/`EFBIG`. `RLIMIT_NPROC` now gates WASIX `fork`, `proc_spawn`, and `proc_spawn2` before provisional child process creation, returning `EAGAIN` when the sandbox process table has reached the current process's process-count limit. Reaped child processes now contribute elapsed child user-time and maximum-resident-set proxy values to the parent, so `getrusage(RUSAGE_CHILDREN)` and the child fields returned by `times()` report collected child work instead of fixed zero values. Parent-death signal state now lives in the WASIX process table and is delivered to live children when their parent exits before reparenting, so `prctl(PR_SET_PDEATHSIG)` participates in the same process lifecycle as signals, wait, and reparenting instead of remaining guest-local bookkeeping. Process name state also lives in the WASIX process table, so `prctl(PR_SET_NAME)`, `/proc/self/comm`, `/proc/self/stat`, and `/proc/self/status` report the same sandbox process identity instead of diverging between guest-local wrapper state and runtime procfs metadata. Dumpable, no-new-privileges, and seccomp mode metadata now live in the same process table as well, so `prctl` getters and `/proc/self/status` report coherent sandbox process-control state for native runtime probes. Timer slack is also stored in the WASIX process table and exported through `/proc/self/timerslack_ns`, so `prctl(PR_SET_TIMERSLACK)` and procfs agree on the current process timer metadata. Linux personality metadata is now stored in the WASIX process table and exported through `/proc/self/personality`, so the direct `personality` syscall and procfs report the same process state. Clear-child-TID and robust futex-list addresses are now stored as process-table metadata as well, so `set_tid_address`, `PR_GET_TID_ADDRESS`, clone `CLONE_CHILD_CLEARTID`, and `get_robust_list` share the same process record instead of depending on copied guest-local bridge statics. Linux capability masks now live in the WASIX process table too: `capget`, `capset`, `PR_CAPBSET_READ`, `PR_CAPBSET_DROP`, and `/proc/self/status` capability fields use the same effective, permitted, inheritable, and bounding metadata. The remaining scalar Linux `prctl` process-control probes used by native runtimes, including keep-caps, securebits, timing mode, THP disable, I/O flusher, and ptracer selection, now store their guest-visible values in the WASIX process record instead of per-bridge globals. Namespace creation and join metadata from `clone`, `unshare`, and `setns` now updates WASIX process-table namespace ids, and `/proc/self/ns/*` plus numeric process namespace readlinks and symlink stat sizes report those sandbox namespace ids instead of fixed bridge-local values. Architecture TLS-base metadata from `arch_prctl`, clone `CLONE_SETTLS`, and restartable-sequence registration from `rseq` now live in the WASIX process record, giving native runtime startup paths a runtime-owned source of truth instead of copied bridge globals. NUMA memory-policy state and Linux I/O priority metadata now live in the WASIX process record as well, so `set_mempolicy`, `get_mempolicy`, `mbind`, `set_mempolicy_home_node`, `ioprio_set`, and `ioprio_get` share runtime metadata across libc wrappers and direct syscall paths. UTS hostname and domain name state now also live in the WASIX process record, so `uname`, `gethostname`, `sethostname`, `getdomainname`, and `setdomainname` share the same guest-visible identity across forked and spawned process paths instead of depending on bridge-local globals; `/proc/sys/kernel/hostname` and `/proc/sys/kernel/domainname` are generated from that same process metadata when opened through the WASIX procfs path. process metadata instead of bridge-local globals. Child-subreaper state also lives in the WASIX process table, and live orphaned children now reparent to the nearest live subreaper ancestor before falling back to sandbox init, matching the process-supervision behavior expected by Linux launchers. The CPython WASIX bridge now uses that cumulative child accounting to populate `wait3`/`wait4` rusage deltas for successfully reaped child processes instead of returning fabricated zero usage. CPython WASIX `subprocess` also preserves `close_fds=True` semantics on platforms where libc does not expose `POSIX_SPAWN_CLOSEFROM` by lowering the live `/proc/self/fd` descriptor set, or the current open-file limit fallback, into real POSIX spawn close actions. WASIX `proc_spawn2` treats close actions for already-closed descriptors as successful no-ops, matching the POSIX spawn file-action behavior expected by this lowering. The Bash and BusyBox WASIX bridges now route `wait`, `waitpid`, `wait3`, `wait4`, and `waitid` through the WASIX child status path, including rusage deltas for `wait3`/`wait4`, so shell and core userspace wait variants share the same process-table accounting. When preopened directories are present, the WASIX runner also preflights the requested cwd through those mappings before starting the module. Unmapped cwd paths, non-directory targets, and symlink escapes are rejected as invalid requests instead of being left to runtime-dependent host-fs behavior. Mapped WASIX preopen trees are also scanned before execution and rejected if any existing entry is a symlink, keeping staged workspace or toolchain state from inheriting host-fs symlink escapes. The WASIX runner also preflights module imports and rejects raw `wasix_http_client_v1` imports. Sandbox commands must use Sinomo-mediated fetch/network APIs instead of ambient WASIX HTTP. The gate intentionally does not reject socket or port symbols that WASIX libc imports for shell and process support; this build does not enable a host virtual-network backend for those imports. The WASIX Python compatibility layer now routes direct socket setup, introspection, data-path, shutdown, and option calls through WASIX socket descriptors instead of falling back to an ambient libc path, while preserving Sinomo's separate policy that externally reachable network I/O must go through mediated sandbox networking.

Staged toolchain execution must expose only the pinned toolchain contents to the guest. Support artifacts mount read-only under `/lib`, `/share`, `/include`, and `/etc`; command modules from the same manifest mount read-only under `/bin` so normal WASIX `/bin/<command>` and PATH child process lookup resolves to pinned sandbox commands instead of ambient host binaries. The WASIX child-exec resolver supports two explicit policy modes. Commands that declare `pinned-child-exec-containment` run in pinned-only mode, where child processes must resolve to absolute `/bin/<command>` paths, including PATH lookups and script interpreters. Commands that declare `wasix.unpinned-child-exec` opt into sandbox-local child exec so shells, Python, and build tools can execute WASM modules and scripts staged under writable sandbox paths such as `/work`, while still resolving through the sandbox virtual filesystem rather than host paths.

The runtime should provide:

- preopened virtual directories, including root (`/`) preopens; more specific nested preopens still take precedence over `/` so read-only submounts cannot be bypassed through a root mapping,
- process plans for multiple pinned command invocations with explicit stdout-to-stdin pipes plus per-step cwd and environment overrides; each process-plan step must use an `argv[0]` matching its pinned command name so BusyBox-style multicall modules cannot bypass the command policy by selecting a different applet at runtime; per-step environment overrides, stored sandbox environment entries, and package default environments must use the sandbox environment-name grammar and reject embedded NUL values before staging or execution starts; package argv prefixes are validated before import so persisted package data cannot later produce malformed runtime argv; package required-capability strings must be non-empty and NUL-free before policy can match them, and cluster policy allowed-capability entries use the same validation; cluster policy SIFS prefixes, provenance prefixes, and HTTP host allowlists are validated on both write and KV decode before they affect authorization; all process-plan step cwd values are preflighted before the first step runs,
- stdin/stdout/stderr streams, with bounded direct stdin, bounded fd-backed WASIX stdin pipes, stdout pipe teeing, and local WASIX-only concurrent stdout-to-stdin process-plan pipes, so command startup cannot buffer unbounded input into host memory before execution,
- wall-clock and CPU-ish execution budgets,
- memory limits,
- fuel or epoch interruption where available,
- deterministic cancellation,
- structured exit status,
- no ambient host filesystem access,
- no ambient host network access.

The current WASI runner explicitly disables ambient TCP, UDP, and name lookup. Network egress for sandbox commands must go through Sinomo-mediated HTTP/fetch paths so policy, byte budgets, audit logs, and cluster routing remain enforceable.

#### Command Execution Contract

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

Large outputs should be chunked and referenced by SIFS paths instead of returned inline.

### Command Environment

The first root image should be a signed tool bundle, not a Linux distro. It should contain WASM builds or native WASI ports of common tools plus a real POSIX `sh`.

The command rule is strict:

```text
real tool or no tool
```

No compatibility theater is allowed. Do not substitute a partial custom parser, toy shell, fake `sed`, fake `awk`, fake `git`, or mostly-compatible command and present it as the real tool. A deliberately limited Sinomo-specific helper can exist, but it must have its own name and documentation.

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
python, pip, or bash as larger command packages
curl-like client routed through Sinomo-network policy
```

Start with BusyBox as the first command environment target. BusyBox is useful because it can provide a compact real userspace. Sinomo should maintain its own BusyBox build path so the runtime, filesystem assumptions, patches, and reproducible build inputs are controlled by the project. The current WASIX package keeps BusyBox ash close to upstream: it does not patch shell command execution, pipeline, here-doc, or redirection semantics to hide missing runtime behavior. Job control, fd-backed concurrent pipelines, exec/wait, and arbitrary POSIX fork/exec remain pending until Sinomo provides the runtime fd/process substrate they require. `gzip` is now part of the profile after the BusyBox recipe switched descriptor moves to WASI `fd_renumber` and enabled decompression support. `chmod` is also part of the profile after Sinomo added WASI extensions that apply mode changes to explicit read-write preopens and report real permission bits back to BusyBox `stat`/`lstat` callers. BusyBox `awk` is now part of the profile for real in-process awk programs; awk external command features fail with `ENOTSUP` until Sinomo has a real child-process substrate. Sinomo should still expose only commands that behave as real tools inside the sandbox contract. A smaller set of real, tested tools is better than a broad but brittle surface.

#### Tool Packaging

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
- hashes and byte sizes for support artifacts under `lib/`, `share/`, `include/`, and `etc/`,
- command names,
- required capabilities,
- default resource limits, including memory, wall time, stdin bytes, stdout and stderr bytes, workspace bytes, per-command written bytes, and file count,
- provenance and signature.

Sandbox instances pin a toolchain version in their manifest. Resuming a sandbox uses the same toolchain unless an explicit migration is recorded.

Toolchains, language runtimes, package archives, and large support files should be replicated through SIFS. Local nodes may cache already-replicated content for performance, but the source of truth is SIFS. SIFS is expected to host large files and should be used for this rather than creating a separate local-only toolchain distribution path.

### SIFS Integration

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

The first backend can adapt the existing SIFS path API. It should keep all sandbox path normalization inside the sandbox crate so WASM tools never see real host paths.

#### Locality

Execution should be scheduled on a node that can efficiently serve the mounted SIFS paths:

- If the workspace is under `/services`, any eligible compute node can run it.
- If the sandbox mounts `/sinomo/nodes/<node_id>/files`, prefer that owner node.
- If a mounted subtree is unavailable, the command should fail with an inspectable `unavailable-offline` or `policy-blocked` reason.

The scheduler should expose the selected node and path locality in the sandbox manifest.

### Policy And Security

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
- every toolchain command module and support artifact is inside an allowed SIFS package-cache prefix,
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

The cluster policy exposes the two concurrency budgets as `max_concurrent_commands_per_sandbox` and `max_concurrent_sandboxes_per_node`. Daemon-backed sandbox command execution tracks active runs in-process and denies new commands before staging when the configured budget would be exceeded. Denials emit the normal `sandbox.policy.denied` event path.

Every denial should emit structured events and be visible in `sandbox doctor` or `cluster doctor`. Sandbox policy-denial and resource-exhaustion events are already surfaced by `sinomo sandbox doctor`, both globally and for a specific sandbox id.

### Network Model

Networking should not be a raw WASI socket pass-through in the first version.

Use explicit Sinomo-mediated APIs:

```text
sandbox http request -> sinomo-network policy check -> outbound request/proxy
```

This keeps all cluster network traffic and policy decisions in Sinomo. It also lets the runtime support offline and replay-friendly modes.

Initial network capabilities:

- disabled by default,
- allowlisted HTTP(S) fetch,
- optional cluster-internal service calls,
- per-command byte and timeout budgets,
- request/response logs with redaction policy.

Raw TCP/UDP can be revisited later.

### Event And Audit Model

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

Daemon-backed sandbox lifecycle operations append lifecycle events after the manifest or workspace operation completes. Config update events include non-secret change metadata such as whether cwd changed and which environment variable names were set or unset; they do not include environment values. Daemon-backed toolchain commands persist the effective command environment in `/logs/<command_id>.record.sobj`, including process-plan step overrides, while the derived `sandbox.command.started` and final command-status events include only environment variable names. `sandbox.command.output` events contain captured raw stdout/stderr chunks. `sinomo sandbox fetch` appends `sandbox.network.http` after the daemon-mediated fetch stores the response body and HTTP exchange audit record in the sandbox filesystem. `sinomo sandbox logs <sandbox-id> <command-id>` decodes that durable command record before printing raw stdout/stderr and shows cwd, argv, and environment variable names for the effective invocation. HTTP audit records and `sandbox.network.http` events store the request method, host, sandbox workspace root, and a redacted URL containing only scheme, host, and path; query strings and fragments are dropped before persistence. Failed post-policy HTTP transport attempts are also lowered into `sandbox.network.http` from the persisted audit record, with a null response status and zero response bytes. Fetch policy denials emit `sandbox.policy.denied` before returning and do not call the mediated transport. Daemon-backed sandbox creation appends `sandbox.mount.added` for accepted mount declarations, and failed mount policy checks emit both `sandbox.policy.denied` and `sandbox.mount.denied`. BusyBox import appends `sandbox.toolchain.updated`, and sandbox creation appends `sandbox.toolchain.pinned` for the manifest's pinned toolchain id. Local and daemon-backed direct file writes through `sinomo sandbox write` append `sandbox.file.written` after the sandbox write succeeds, and direct write quota failures append `sandbox.resource.exhausted` before returning. Local and daemon-backed direct writes to read-only sandbox paths append `sandbox.policy.denied` and `sandbox.mount.denied` before returning. Local and daemon-backed `sinomo sandbox fetch` also append `sandbox.resource.exhausted` when the mediated response body would exceed the sandbox write/workspace quota, while retaining the HTTP audit record for the completed network attempt. Successful local and daemon-backed fetch-to-file writes also append `sandbox.file.written` for the response body path, in addition to the `sandbox.network.http` audit event. File-write events include the workspace root; fetch-to-file file-write events also include the fetch phase plus the HTTP audit command id and request id so operators can correlate the file mutation with the network exchange record. Sandbox-scoped policy-denial, mount-denial, and resource-exhaustion events also include the workspace root. Daemon-backed command execution emits `sandbox.policy.denied` before returning policy failures and `sandbox.scheduler.failed` before returning scheduling failures.

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

Agents should receive raw stdout/stderr chunks as the command runs. Summaries can be produced as secondary artifacts, but they must not replace raw output in the agent-facing stream.

The runtime-level streaming hook is implemented through `SandboxOutputSink` on `SandboxWasiRunOptions` and the staged SIFS runner, and local CLI execution uses it to write stdout/stderr before printing the command summary. Daemon toolchain execution streams live output through `/sandbox/run/toolchain/stream`, and daemon process-plan execution streams live output through `/sandbox/run/process-plan/stream`; the older `/sandbox/run/toolchain` and `/sandbox/run/process-plan` routes remain available for single-response callers. Stream de-duplication keys include the command id, stream, and offset so process plans do not collapse same-offset chunks from different steps. `sinomo sandbox reboot [--daemon]` now provides the agent-session reboot boundary. Reboot preserves durable workspace state and clears `/tmp` when the instance uses the default `clear-on-session-start` policy; instances created with `--tmp retain` or `--keep-tmp` retain `/tmp` across reboot. Daemon-backed reboot emits `sandbox.instance.rebooted` with the applied temp-policy result. The sandbox crate has focused local and SIFS tests for this contract: `/work` survives reboot, `/tmp` is removed from SIFS and recreated behind a fresh epoch by default, and retained instances keep `/tmp` across reboot.

### CLI Shape

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
sinomo sandbox toolchain add-command [--daemon] [--toolchain <toolchain-id>] [--version <version>] [--target-abi wasix] [--provenance <text>] [--capability <name>] <command> <module.wasm>
sinomo sandbox toolchain add-artifact [--daemon] [--toolchain <toolchain-id>] [--version <version>] [--provenance <text>] <lib|share|include|etc/path> <file>
sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>] <package.json>
sinomo sandbox toolchain verify-package <package.json>
sinomo sandbox toolchain hash <file>
sinomo sandbox toolchain busybox-plan
sinomo sandbox toolchain bash-plan
sinomo sandbox toolchain build-tools-plan
sinomo sandbox toolchain python-plan
crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh
sinomo sandbox toolchain verify-bash <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-busybox <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-python <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-coding <sandbox-id> [--daemon] [--allow-unsigned] [--full]
sinomo sandbox toolchain verify-make <sandbox-id> [--daemon] [--allow-unsigned]
sinomo sandbox toolchain verify-tcc <sandbox-id> [--daemon] [--allow-unsigned]
```

`--allow-unsigned` is a local verifier escape hatch only. Daemon-backed verification is cluster-scoped and must use the cluster sandbox policy, for example `sinomo sandbox policy set --allow-unsigned-toolchains`. For `sandbox run`, `--allow-capability` is also local-only and exists for operator verification of unsigned/local toolchains; daemon-backed runs must use `sinomo sandbox policy set --allow-capability <name>` so capability decisions remain cluster-scoped.

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

### Crate Boundaries

Recommended crates/modules:

| Crate | Responsibility |
|---|---|
| `sinomo-sandbox` | Sandbox IDs, manifests, policy checks, command runtime, VFS traits, WASM host integration. |
| `sinomo-dfs` | SIFS path operations, object persistence, policy violation events for DFS paths. |
| `sinomo-network` | Optional mediated outbound network and cluster service calls. |
| `sinomo-cli` | Operator CLI and local testing commands. |
| `sinomo-daemon` | Sandbox service lifecycle, scheduling, command execution endpoint, event emission. |
| `sinomo-storage` | Register sandbox local caches, temporary runtime state, and toolchain cache paths. |

The agent crate should remain general. It should call sandbox tools; it should not contain Minecraft-specific, shell-specific, or WASM-runtime-specific code.

Within `sinomo-sandbox`, implementation ownership is split so new WASIX and toolchain work does not collect in facade modules:

- `src/runtime/wasix/` owns the Wasmer/WASIX runner, process-tree signaling, output capture, static memory-budget checks, and WASIX preopen/cwd handling.
- `src/execution/local.rs` owns local staged single-command execution; `src/execution/local_process_plan.rs` owns local structured process-plan execution; `src/execution/local_fidelity.rs` owns local toolchain verifier runner helpers; `src/execution/sifs.rs` owns SIFS-backed WASIX single-command execution, placement, direct virtual preopens, command persistence, and rollback; and `src/execution/sifs_process_plan.rs` owns SIFS-backed structured process-plan execution.
- `src/execution/staging/` owns local runtime staging for local execution and virtual toolchain preopen construction for SIFS/WASIX execution. SIFS workspace and SIFS mounts are no longer copied to host paths for the command run; the active SIFS path uses the sandbox VFS directly and only supports WASIX execution.
- `src/filesystem/local.rs` and `src/filesystem/sifs.rs` own the corresponding virtual filesystem backends, with shared path/error helpers in `src/filesystem/support.rs`.
- `src/toolchain/local.rs` and `src/toolchain/sifs.rs` own local and SIFS toolchain stores; manifests, built-in BusyBox registration, SOBJ serde, and shared hash/bundle/validation helpers stay in sibling toolchain modules.
- `src/policy/core.rs`, `src/events/envelope.rs`, `src/http/protocol.rs`, and `src/fidelity/cases.rs` keep policy, event, protocol, and verifier case-table ownership out of the crate facade modules.

### Storage Registration

`sinomo-sandbox` should register its own local storage paths:

```text
sandbox/cache/toolchains
sandbox/cache/modules
sandbox/runtime
sandbox/tmp
```

These are local caches and runtime state. Durable workspace contents belong in SIFS under `/services/sandbox/...`.

`sinomo-events` registers the node-local durable event ledger:

```text
events/event-log.jsonl
```

Sandbox event helpers write cluster event envelopes through the generic `EventStore` trait, so tests can use in-memory stores and daemon-backed operation can use the durable JSONL store without changing the sandbox event format.

Toolchain caches are only caches. The complete toolchain artifacts should be addressable and replicated through SIFS.

Storage contract violations should be reported as node health findings, not as startup blockers. The current CLI path implements this as install-time warnings and `sinomo cluster doctor` warning findings through `data_dir_storage_contract_findings`; unregistered sandbox-local paths therefore degrade diagnostics without preventing the daemon or install path from starting.

### Implementation Phases

#### Phase 0: Architecture And API Skeleton

- Add this design as the planning document.
- Define `SandboxId`, `SandboxCommandId`, manifest structs, resource limit structs, and error types.
- Add `sinomo-sandbox` storage registration.
- Add empty CLI command group and help text.
- Add unit tests for path normalization and policy denial shapes.

#### Phase 1: Local WASI Command Runner

- Integrate Wasmtime.
- Run one WASM command with preopened in-memory or temporary directories.
- Capture stdout, stderr, exit status, timeout, and memory limit failures.
- Implement cancellation.
- Package one or two proof commands, such as real `echo` and real `cat`.
- Add tests for no host path escape, timeout, and output limits.

#### Phase 2: SIFS-Backed Workspace

- Implement `SandboxFileSystem` on top of the existing SIFS path API. Local and SIFS filesystem backends now expose `/dev`, `/dev/null`, bounded reads from `/dev/zero`, and random reads from `/dev/random` and `/dev/urandom` as virtual paths without persisting device files into SIFS. Writes to virtual device sinks bypass workspace write quotas and do not emit durable file-write events. Staged WASI command execution now installs a per-run Sinomo-owned `/dev` preopen for `/dev/null`, bounded `/dev/zero`, seeded random devices, stdio aliases, `/dev/tty`, `/dev/fd/{0,1,2}`, and a per-run `/dev/shm` directory. A host symlink staging approach is explicitly not accepted: the WASI runner has a regression test that confirms symlink escape to host `/dev/null` does not succeed.
- Persist sandbox manifests under `/services/sandbox/instances/<sandbox_id>`.
- Persist command logs and changed files through SIFS.
- Persist `/tmp` as durable sandbox instance state through SIFS.
- Implement `sandbox create`, `sandbox run`, `sandbox status`, and `sandbox logs`.
- Add resume by reloading the manifest and workspace path.
- Add session reboot semantics that keep instance state durable while applying the `/tmp` policy.

Acceptance test:

```text
create sandbox
write file from command
restart daemon
resume sandbox
read file from another node
```

#### Phase 3: Minimal Real POSIX Command Environment

- Add a Sinomo-maintained BusyBox build as the first userspace target, with its real applets first. BusyBox ash is exposed as the default `sh` only through the upstream WASIX BusyBox applet; runtime gaps must be fixed below ash rather than through shell-behavior patches.
- Add the core tools listed in the command environment section as each command builds and passes fidelity inside the sandbox contract. The current BusyBox WASIX package includes `cat`, `cp`, `cmp`, `date`, `diff`, `awk`, `echo`, `env`, `false`, `find`, `grep`, `gzip`, `head`, `ln`, `ls`, `mkdir`, `mv`, `patch`, `printf`, `pwd`, `rm`, `rmdir`, `sed`, `sha256sum`, `sh`, `sort`, `tail`, `tar`, `test`, `touch`, `true`, `uniq`, `wc` and `chmod`; the BusyBox fidelity declaration now requires every exposed applet to have at least one direct invocation case before it remains in the default manifest. BusyBox `awk` runs real awk programs, and BusyBox `sh` is covered by a pipeline/current-directory shell case.
- Build or vendor WASI-compatible tool modules with reproducible hashes.
- Add host-side tool fidelity tests before exposing each command name.
- Store toolchain manifest in SIFS.
- Pin toolchain id in sandbox manifest.
- Keep CLI `sandbox shell` backed by the pinned real `sh` command; do not add a fake shell fallback.
- Add CLI `sandbox policy show/set` for the typed cluster KV policy.
- Use `crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh` as the local end-to-end gate for the first composite coding-task toolchain. The gate now includes the command workflow from the acceptance test, extended with copy/move/compare, chmod, tar, rm/rmdir, sed, awk, sort, wc, head, and tail checks. With `VERIFY_FULL=1` the same script verifies the maintained WASIX Python/pip, Bash, Make, and TCC packages.

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

#### Phase 4: Policy And Node Scheduling

- Add mount declarations and policy checks.
- Add read-only/read-write mount enforcement.
- Add node locality selection for mounted SIFS paths.
- Load resource budgets from cluster KV config in daemon-scheduled execution. The daemon `/sandbox/run/toolchain` route now loads the cluster sandbox policy and runs pinned SIFS toolchain commands through the SIFS staged runner.
- Emit sandbox event families. The durable JSONL event store and daemon-backed lifecycle, command completion, command start, captured output, cancellation/resource status mapping, active-execution concurrency denial, persisted config updates, and mount/toolchain/network HTTP event appends are implemented; daemon-backed session reboot emits `sandbox.instance.rebooted`; true live output streaming exists at the runtime sink layer, local CLI layer, and daemon streaming RPC layer for both single pinned commands and structured process plans.
- Add `sandbox doctor` or extend `cluster doctor`. `sandbox doctor` now uses a runtime capability report for planned virtual devices (`/dev/null`, `/dev/zero`, `/dev/random`, and `/dev/urandom`) and runtime budget guards including direct stdin input limits; those devices are implemented for staged WASI command execution through a per-run Sinomo-owned `/dev` preopen, so the doctor no longer reports them as missing. Missing planned MVP commands that are known substrate gaps are reported with explicit reasons; `sandbox doctor` also emits an error-level `mvp-command-surface-incomplete` finding while any required MVP command is missing. The WASIX runtime capability report includes `child-process-exec` when the module crate is built with `wasix-runtime`. `sandbox doctor` surfaces unsupported runtime capabilities as warnings so the remaining shell/process limits are explicit even when no sandbox command is being run. SIFS staged execution rejects remote local-owned mounts before creating a local staging workspace unless policy explicitly allows remote local-owned mounts. Recent policy-denial audit events are surfaced both for a specific sandbox and globally; toolchain/package policy denials that are not tied to one sandbox are reported as cluster sandbox service denials. Daemon-backed direct writes to read-only sandbox mounts are also rejected without mutating the SIFS source and append both `sandbox.policy.denied` and `sandbox.mount.denied` records with `mount-read-only-denied`.

Acceptance test:

```text
mount /services/sandbox/test as /work rw
mount /sinomo/nodes/<other>/files as /host ro
verify denied write to /host
verify policy event
```

#### Phase 5: Network And Package Fetching

- Add allowlisted HTTP fetch through Sinomo-network. The sandbox crate now has the typed HTTP request/response contract, default-deny policy validation, host allowlists, byte/timeout budgets, redacted exchange records, durable SOBJ log persistence, recording fetch helper, response-body file writes tested against local and SIFS filesystems, replica-node readback coverage for fetched bodies and audit records, and `sandbox.network.http` event lowering. `sinomo sandbox fetch` sends HTTP requests to the local daemon for mediated egress, and `sinomo sandbox fetch --daemon` lets the daemon validate cluster policy, store the response body in SIFS, persist the HTTP audit record, and append the network event.
- Add `curl`-like command backed by mediated network API. `sinomo sandbox fetch` now parses URL/output/limits/header options and executes against the sandbox HTTP transport abstraction, with the production transport connected to daemon-mediated routing.
- Record network request refs with redaction policy. The durable record format is used by `sinomo sandbox fetch` after the daemon-mediated HTTP response is returned, so response bytes and audit metadata are stored with the sandbox. HTTP audit records and `sandbox.network.http` events retain only scheme, host, and path from the URL; query strings and fragments are dropped before persistence. The CLI also appends the corresponding `sandbox.network.http` event to `events/event-log.jsonl`.
- Keep raw sockets disabled. The local WASI runner now explicitly disables TCP, UDP, IP-name lookup, and socket address checks in the Wasmtime WASI context; sandbox networking remains available only through mediated `sinomo sandbox fetch`/daemon HTTP paths. A regression test runs a WASI module that calls `sock_send` directly and verifies that the syscall cannot succeed. The WASIX runner preflights module imports and rejects raw `wasix_http_client_v1` imports before execution so process-capable toolchains cannot bypass Sinomo-mediated HTTP policy, while still allowing socket/port imports that WASIX libc may include for local shell/process support.

Acceptance test:

```text
network disabled -> fetch denied
allowlist enabled -> fetch succeeds
response body stored in SIFS
audit event includes URL policy metadata
```

#### Phase 6: Richer Toolchains

- Add required shell and language runtimes as toolchain packages.
  `sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>] <package.json>` now
  imports a declared package containing real command modules and support
  artifacts, or merges it into an existing pinned toolchain when `--into` is
  supplied. Each package entry must declare a BLAKE3 content hash, and import
  rejects hash mismatches plus absolute or escaping host paths before installing
  anything. A package-level `target_abi` is the default for its commands and
  now defaults to WASIX when omitted.
  `sinomo sandbox toolchain verify-package <package.json>` runs the same
  manifest, file path, duplicate-entry, and hash validation without
  importing, so package recipes can be checked before SIFS writes. This lets a
  coding-task toolchain combine the BusyBox file/search applets with Bash,
  Python, pip, tinycc, or selected build tools once their real sandbox
  builds are available. Bash, Python, and pip are required for coding-task
  readiness and `sandbox doctor` reports them as error-level missing commands
  until real packages and fidelity suites exist.
  `crates/sinomo-sandbox/toolchains/bash` now provides the
  pinned Bash WASIX recipe target. The recipe now produces a real module and
  the full Bash verifier passes locally and through a daemon-local SIFS-backed
  sandbox when cluster policy explicitly allows `child-process-exec` and
  `wasix.unpinned-child-exec`.
  `crates/sinomo-sandbox/toolchains/python`
  now contains the pinned CPython WASIX native-extension path. The WASIX recipe
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
  maintains an explicit richer-toolchain target list: Bash and Python have
  maintained packages and verifier gates; pip has verified
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
  build-tool packages. Clang/LLVM now has a pinned LLVM 21.1.6 WASIX package
  recipe that cross-builds Clang, LLD, `llvm-ar`, `llvm-ranlib`, and `llvm-nm`
  as real sandbox command modules and packages the same WASIX
  sysroot/resource artifacts used by host-side Sinomo module recipes; the full
  Clang verifier is opt-in with `VERIFY_CLANG=1` because building LLVM is
  materially larger than the default build-tools gate. Make is subject to the current WASIX
  shell/process limits and cluster policy, while TinyCC currently compiles
  x86_64 object artifacts and does not execute compiled programs inside the
  sandbox.
  `sandbox doctor` reports implemented targets when missing from the pinned
  toolchain, reports required targets as errors, and reports planned targets as
  readiness information instead of pretending they are available.
- Consider Bash, Python, tinycc, or selected build tools.
  Bash, Python, and pip are now required coding-sandbox targets. Bash now runs through the WASIX substrate for
  internal shell cases and external pinned BusyBox WASIX commands, with local
  and daemon-local SIFS-backed verifier coverage under explicit cluster policy.
  Python now has a CPython WASIX
  package recipe with full standard library/support artifact staging, zlib,
  dynamic side-module loading, POSIX process support through WASIX, and a pinned
  pip/setuptools environment. Pip now passes the native-extension gate for
  local wheelhouse installs: `numpy` imports and runs representative array
  operations, and `matplotlib` imports with the Agg backend and renders a figure
  file inside the sandbox. It now stores pip wheel cache state under `/work/pip-cache` and verifies that
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
- Expand WASIX process support directly before adding more unsupported-case
  checks. The vendored WASIX `proc_exec3` path now lets the `proc_fork_env`
  vfork-style child-exec route return success without requiring an asyncify
  context-switching environment; that requirement is only applied to the
  asyncify vfork restore path that actually needs it. WASIX thread signaling
  now reports a missing target thread as a syscall error instead of silently
  dropping the signal while returning success. WASIX `thread_join` now rejects
  self-joins and missing target threads with syscall errors instead of
  deadlocking or reporting a successful join that never happened. WASIX
  `thread_spawn` now removes the provisional guest thread from the process table
  if the host task cannot be scheduled, so failed spawns do not leave phantom
  threads that can poison future joins, signals, or thread-budget accounting.
  WASIX `thread_signal(Signone)` now mirrors `proc_signal(Signone)` as an
  existence probe and does not enqueue a wakeup signal.
  WASIX futex wait/wake now follows the documented wait condition and wake
  result semantics: a mismatched expected futex value returns an invalid wait
  instead of a false successful wake, and wake calls report whether they
  actually woke a waiting thread. Linux priority-inheritance futex lock,
  try-lock, unlock, `LOCK_PI2`, and PI requeue operations now maintain
  sandbox-local owner TID state and wake waiters through the same futex
  substrate, giving pthread and native-library PI mutex/condition-variable paths
  real process-local behavior without claiming host kernel priority scheduling.
  WASIX futex waiters now carry Linux futex bitsets through masked wait/wake
  runtime imports, so `FUTEX_WAIT_BITSET`, `FUTEX_WAKE_BITSET`, and the newer
  `futex_wait`/`futex_wake` mask arguments select only matching waiters instead
  of collapsing all waits on an address into a single unmasked queue. Linux
  futex requeue operations now move matching waiters between WASIX futex queues
  through a runtime `futex_requeue` import, so condition-variable style
  `FUTEX_REQUEUE` and `FUTEX_CMP_REQUEUE` paths can transfer sleepers to the
  mutex futex instead of only waking a prefix and leaving the rest stranded.
  CPython WASIX clone handling now applies `CLONE_CHILD_CLEARTID` and
  `CLONE_CHILD_SETTID` side effects only in the child path, so parent processes
  do not accidentally register child clear-TID addresses or write child TID
  values into parent memory.
The newer `futex_waitv` syscall now validates Linux wait-vector entries and
waits through a WASIX `futex_wait_any` runtime import that registers every
address in the wait vector with the futex substrate, so modern native libraries
can wake on whichever listed futex changes first instead of being approximated
as a wait on only the first address.
Linux native AIO syscalls (`io_setup`, `io_destroy`, `io_submit`,
`io_getevents`, and `io_cancel`) now provide sandbox-local completion queues
over the existing descriptor operations. Submitted pread, pwrite, vector I/O,
fsync, fdatasync, poll, and noop requests complete through the same sandbox fd
paths and optionally signal an eventfd, giving native libraries an in-sandbox
Linux AIO ABI without host kernel AIO access.
Modern Landlock syscalls (`landlock_create_ruleset`, `landlock_add_rule`, and
`landlock_restrict_self`) now keep sandbox-local ruleset descriptors and
restrict-self metadata for runtime probes and package runners that enable Linux
self-confinement. The calls validate ruleset/rule descriptors and record
path-beneath rules inside the Sinomo sandbox without applying host Landlock
policy or escaping the existing sandbox filesystem boundary.
The spawned-thread callback path now exits the guest thread with a syscall
error if `wasi_thread_start`, the thread id, or the start pointer cannot be
  passed to the guest callback, instead of unwrapping and panicking the host.
  WASIX `proc_spawn`, `proc_spawn2`, and `proc_exec3` also treat consumed child
  environments during built-in probing as `Noexec` spawn/exec failures and clean
  up provisional process records instead of panicking the host runtime.
  WASIX `proc_spawn2` and `proc_exec3` preserve specific spawn failure errno
  mappings instead of always returning `Noexec`.
  WASIX `proc_spawn` supports `StdioMode::Log` through inherited captured stdio.
  WASIX PATH search resolves relative entries against the sandbox cwd.
  Parent PID state is now shared through the process table so `proc_parent`,
  procfs status, child `setpgid` checks, and `SIGCHLD` delivery agree on parent
  metadata across cloned process handles.
  WASIX process-group/session support now includes shared mutable group/session
  records, `proc_getpgid`, `proc_getpgrp`, `proc_getsid`, `proc_setpgid`, and
  `proc_setsid` imports, POSIX negative-pid process selection for signal and
  join paths, and procfs session ID reporting from the selected process record. TTY foreground
  process group state plus `proc_tcgetpgrp`, `proc_tcgetsid`, and
  `proc_tcsetpgrp` imports provide the terminal-side state that shell job
  control expects. The terminal
  foreground group is initialized from the first attached WASIX process group,
  and terminal-generated signals are delivered to the recorded process group
  through the WASIX control plane. The terminal control-character path now
  covers Ctrl-C, Ctrl-\ , and Ctrl-Z as `SIGINT`, `SIGQUIT`, and `SIGTSTP`. Stop and continue signals now
  update process-visible task state, stopped threads suspend at syscall
  boundaries until continued or terminated, and `proc_join` supports
  `JoinFlags::WAKE_STOPPED` with `JoinStatusType::Stopped` for exact, any-child,
  and process-group-filtered waits. Stop-aware waits now report each child stop
  once until `SIGCONT` clears the report state, so repeated waits do not
  re-report the same stale stop. Continued child state is also tracked separately
  for POSIX `WCONTINUED`; the CPython, Bash, and BusyBox WASIX wait wrappers now report
  `WIFCONTINUED`/`CLD_CONTINUED` from `waitpid` and `waitid`. Bash and BusyBox
  also route ordinary `waitpid`/`waitid` through WASIX `proc_join` and
  non-consuming `waitid(..., WNOWAIT)` through `proc_wait_status`, so shell and
  applet wait calls see the same exit, signal, stopped, any-child, and
  process-group selection semantics as the runtime. CPython, Bash, and BusyBox
  `waitid` wrappers now also let `WNOHANG` be the only nonblocking switch and
  use non-consuming status observation whenever `WEXITED` is absent, so
  stopped/continued-only waits do not accidentally reap exited children. The
  non-consuming status path now also accepts explicit event-class selection
  through `proc_wait_status_select`, and those WASIX `waitid` wrappers use it so
  `WSTOPPED`-only observation no longer reports an exited child state that the
  caller did not request. Blocking any-child/group stop-only waits now also keep
  waiting across unrequested exits from other matching children instead of
  returning early while a still-live matching child can later produce the
  requested stopped status. The runtime also exposes a blocking continued-child
  wait import; CPython, Bash, and BusyBox use it for continued-only `waitid`
  calls, so real job-control wait paths can sleep until `SIGCONT` produces a
  reportable child status instead of observing only already-queued continued
  events. The child-status selection path now also has an explicit continued
  event class, so non-consuming `waitid` paths that request continued state
  together with stopped or exited state can block on whichever child status
  arrives first instead of waiting only for exit/stop after the first continued
  probe misses.
  Child stop, continue, and termination events now
  deliver `SIGCHLD` to the recorded parent process, giving shell
  job-control handlers a real status-change notification path. WASIX now applies
  the broader POSIX terminate-by-default signal set across process status,
  no-handler signal processing, and asyncify polling, and `proc_join` now
  reports signal termination through `JoinStatusType::ExitSignal` in both
  blocking and nonblocking wait paths. Stored `SignalDisposition::Ignore` actions now suppress default termination in the
  no-handler and asyncify polling paths, and ignorable terminate/stop effects
  are applied during signal processing rather than before disposition checks.
  Ignored signals are also suppressed before dispatch to a registered WASIX
  signal handler, and `SignalDisposition::Default` now resets inherited signal
  overrides in the shared spawn/fork preparation path and is filtered out of
  initial builder state. `SIGKILL` and `SIGSTOP` bypass registered guest signal
  handlers as non-catchable defaults. Parent `SIGCHLD` notification now follows
  actual child state transitions instead of firing for ignorable signals before
  disposition processing. Terminal reads from a background process group now
  raise `SIGTTIN` and either suspend through the normal stop path or return an
  I/O error if the process is still background after signal handling. A
  background `tcsetpgrp` caller now raises `SIGTTOU` before terminal foreground
  process-group mutation proceeds. Terminal state tracks POSIX `TOSTOP`, and
  background writes to stdout or stderr now raise `SIGTTOU` when that flag is
  set. When a process exits and leaves a stopped child process group orphaned in
  the same session, WASIX now sends `SIGHUP` followed by `SIGCONT` to that
  process group, matching the POSIX job-control wakeup path that real shells
  expect. Exiting processes now also reparent live children to the sandbox init
  process when it is still present, or to no parent when init itself exits, so
  child `proc_parent`, later `SIGCHLD`, and wait ownership do not keep pointing
  at a dead parent.
  Copy-memory `proc_fork` child re-entry now uses the main module handles for
  dynamically linked children admitted through the linker-instance-group path.
  Asyncify unwind/rewind now uses main module handles, and `proc_fork` attempts
  dynamically linked vfork-style execution instead of rejecting it up front.
  WASIX process-shared mappings use host in-memory backing for true
  cross-process sharing, with explicit publication and pull at fork, wait,
  sleep, process exit, and synchronization-object boundaries. Same-process
  pthreads keep ordinary `MAP_SHARED` data in native WASM shared memory and use
  runtime objects only for process-shared mutex, condition-variable, rwlock,
  semaphore, and futex coordination. This avoids stale backing snapshots
  overwriting fresh thread writes while preserving direct `fork` plus
  `MAP_SHARED` visibility at POSIX synchronization points. Direct host remapping
  of Wasmer linear-memory pages remains disabled because fixed mappings were
  nondeterministic under fork; closing that gap is still required for arbitrary
  cross-process shared-memory protocols that do not use POSIX synchronization
  calls. Linux uses anonymous `memfd` backing; Windows has an anonymous
  page-file mapping abstraction, with full Windows linear-memory remapping still pending. Asyncify deep-sleep
  waiters process pending signals and then continue polling ready work in the
  same wake, so `SIGCHLD` delivery no longer leaves a ready `waitpid(-1)` join
  path asleep. Focused verification now covers a pthread `MAP_SHARED`
  visibility loop, `pthread_cond_broadcast/1-2.c`, the full
  `pthread_cond_broadcast` C directory, the full `pthread_cond_signal` C
  directory, the full `pthread_mutex_lock` C directory, a direct `fork` plus
  `MAP_SHARED` probe, `pthread_cancel`, `pthread_barrier`, and the full
  `pthread_rwlock` C family with 43 compile/link/run passes, with the 14
  shell-wrapped cases recorded as unsupported when shell execution is disabled. The
  current `fork` interface run compiles and links all 19 C cases and runs 18 of
  19 cases successfully after child-side signal masks, pending self signals,
  interval timers, POSIX `timer_create`/`timer_settime` with `SIGEV_THREAD`,
  runtime-backed POSIX message queues, `times(2)`, process CPU clock offsets,
  `kill(pid, 0)` `ESRCH` reporting, per-process shared mapping remap tracking,
  stdio lock cleanup, and wait-side shared mapping refresh were fixed. The
  runtime now keeps upstream wasix-libc `fork` cleanup in place and registers
  Sinomo's child-side WASIX cleanup through an atfork hook instead of replacing
  libc `fork`. The sysroot now owns the POSIX `pthread_atfork` registry and
  fork-handler ordering so atfork registration can also reach runtime signal
  checkpoints; focused `pthread_atfork` coverage passes all 7 C cases after
  `kill(getpid(), sig)` was changed to route process-directed self-signals
  through runtime thread selection instead of storing them as caller-local
  pending signals when the sending thread blocks the signal. The focused
  `pthread_cond_timedwait/2-5.c` case now passes under a 256-task host cgroup
  after the WASIX task cap stopped dividing available cgroup task slots by 32,
  and the runtime shared-condvar wait path now returns POSIX `ETIMEDOUT` on
  timeout instead of reporting success to the guest. The full `pthread_cond`
  family still has timed-wait failures around lost wakeups after mixed
  shared-mapping scenarios and short-timeout cross-process relock fairness, so
  condition variables remain partial rather than fully passing. The remaining fork
  case, `fork/7-1.c`, is unresolved because of a missing upstream test artifact.
  POSIX message queues are now backed by runtime-owned queue objects and
  descriptors rather than guest-local storage: descriptors keep open queue
  objects alive across `mq_unlink`, enforce read-only/write-only access modes,
  share named queues across forked processes, validate POSIX name and attribute
  limits, expose `MQ_PRIO_MAX`, reject oversized messages with `EMSGSIZE`, and
  order messages by descending priority. Focused `mq_open` coverage now compiles
  and links all 37 C cases, with 27 passes, 9 upstream `UNTESTED` cases, and one
  remaining race-sensitive parent/child `O_CREAT | O_EXCL` case. Focused
  `mq_send` coverage now compiles and links all 19 C cases, with 18 passes and
  one upstream `UNTESTED` priority-scheduling case after runtime-backed blocking
  full-queue waits, sender wakeups, and handler-driven `EINTR` delivery were
  added. Focused `mq_receive` coverage now compiles, links, and runs all 10 C
  cases successfully after receive-side `EMSGSIZE`, blocking empty-queue waits,
  receiver wakeups, and handler-driven `EINTR` delivery were added; the two
  shell cases remain unsupported while shell execution is disabled. Timed queue
  operations now lower through deadline-aware WASIX imports instead of reusing
  the untimed calls: `mq_timedreceive` validates absolute `timespec` deadlines
  only when the queue would block, returns `ETIMEDOUT` for expired deadlines,
  and still wakes on sends or handled signals; `mq_timedsend` has symmetric
  full-queue deadline behavior. Focused `mq_timedreceive` coverage now compiles,
  links, and runs all 19 C cases successfully, with three shell cases unsupported
  while shell execution is disabled. Focused `mq_timedsend` coverage compiles
  and links all 27 C cases, with 25 passes and two upstream `UNTESTED` cases.
  `mq_getattr` and `mq_setattr` now persist descriptor-level `O_NONBLOCK`
  updates through runtime state, and `mq_notify` supports `SIGEV_SIGNAL`
  registration, one-shot delivery, unregister, `EBUSY` on duplicate
  registration, and suppression while a receiver is already blocked. Focused
  `mq_getattr`, `mq_setattr`, and `mq_notify` coverage now passes every
  supported C case, with shell cases still recorded as unsupported while shell
  execution is disabled. `mq_close` now removes the notification registration
  owned by the closed descriptor, and the runtime accepts signal-zero
  notification registrations as no-signal registrations for the POSIX lifecycle
  cases. Focused `mq_close` coverage passes the six supported C cases with one
  upstream `UNTESTED` undefined-behavior case, and focused `mq_unlink` coverage
  passes its five supported C cases with shell and upstream `UNTESTED` cases
  recorded separately.
  POSIX sleep delivery now uses an interruptible wait that reports `EINTR` for
  handled signals while preserving default `SIGSTOP`/`SIGCONT` behavior; the
  focused `nanosleep` C group compiles, links, and runs all 12 C cases
  successfully, with the one shell case still unsupported while shell execution
  is disabled.
  Runtime-backed semaphore waits now treat a processed guest signal as an
  interruptible wait and return `EINTR`; this fixes `sem_wait/7-1.c`, where a
  child blocked in `sem_wait` installs a signal handler and expects the wait to
  return after the parent sends `SIGABRT`.
  WASIX descriptor duplication now honors minimum-result-fd allocation, and
  `posix_spawn` dup2 file actions report failed descriptor installation.
  WASI `fd_renumber` and WASIX `posix_spawn` dup2 preserve source descriptor
  base rights.
  WASIX `fchdir` resolves an open directory descriptor through the sandbox inode
  tree and updates the process cwd without requiring a path string from the
  guest. The maintained BusyBox WASIX recipe now calls that import for `fchdir`
  instead of keeping an unsupported stub.
  WASIX context creation now enforces the documented `() -> ()` entrypoint
  signature for both params and results.
  Windows WASI process/thread CPU clocks use Windows CPU-time APIs instead of
  host panics.
  Non-Unix path resolution maps unknown host file types to `Unknown` inodes
  instead of host panics.
  WASI `*_SET_TIME_NOW` timestamp helpers propagate clock errors instead of
  host panics.
  Process signal timers, CPU backoff scheduling, and TTY input de-duplication
  tolerate monotonic-clock errors without host panics.
  In-memory WASI buffer inodes now support POSIX-style offset reads, writes,
  pwrite, append, sparse zero-fill, `SEEK_END`, and successful in-memory
  `fd_sync`/`fd_datasync`; `path_open2` now opens existing buffer-backed inodes as file-like
  descriptors and applies truncate-on-open.
  Buffer-backed path traversal returns `Notdir` instead of panicking, and
  buffer-backed file unlink succeeds after directory-entry removal.
  `path_rename` replaces an existing target entry with the renamed source inode
  after successful rename.
  `path_rename` preserves the shared backing path when renaming one entry of a
  hard-linked regular file.
  `path_rename` decrements the replaced target inode's link count.
  Directory rename supports virtual-only directories and restores source
  entries on backing rename failures.
  Single-link symlink rename updates relative-target resolution metadata.
  Virtual inode creation initializes zero link counts to one.
  Virtual inode creation initializes default file types for stat metadata.
  Path stat reports buffer-backed inodes as regular files with their current
  length.
  Path stat reports the virtual root as a directory.
  CPython WASIX `linkat(..., AT_SYMLINK_FOLLOW)` now resolves the old path
  inside the sandbox and creates the hard link to the referent instead of
  returning an unsupported-operation error.
  BusyBox WASIX now leaves `awk`'s real `system()` path enabled so awk scripts
  can launch sandbox child commands through the WASIX process substrate.
  TinyCC WASIX now routes `execvp` through WASIX `proc_exec3` with Sinomo's
  argv/env encoding and sandbox `PATH` lookup instead of returning `ENOSYS`, so
  compiler-driver child execution can use the same process substrate as the
  shell and Python packages.
  GNU Make WASIX now provides real `getlogin()` and `getlogin_r()`
  compatibility entrypoints from sandbox `LOGNAME`/`USER` environment metadata,
  with the standard Sinomo login fallback, instead of reporting no login
  identity to configure and build logic.
  BusyBox WASIX `mknod` now supports the POSIX regular-file case by creating a
  real sandbox file with the requested mode, while still denying device-node
  creation because the sandbox must not expose host devices.
  CPython WASIX now wraps `mknod`/`mknodat` and direct `SYS_mknod`/
  `SYS_mknodat` for the same regular-file case through sandbox `openat`, while
  still denying device-node creation.
  `fd_readdir` enforces read-directory rights and reports loaded virtual entries.
  `fd_readdir` reports stable inode values for `.`, `..`, virtual entries, and
  backing filesystem entries.
  Directory stat supports virtual-only directory metadata.
  `fd_readdir` supports virtual-only directory entries without backing metadata.
  `fd_readdir` validates cookie and name-length conversions.
  `path_unlink_file` does not underflow zero link-count metadata.
  `path_remove_directory` checks the required directory-removal right.
  `path_remove_directory` removes empty virtual-only directories without host
  backing entries.
  `path_create_directory` performs its existing-entry check on the virtual path.
  `path_create_directory` can create directories below virtual-only parents.
  WASIX `path_open2` creates buffer-backed files below virtual-only directories.
  `path_filestat_set_times` updates the resolved target path inode.
  `path_link` only hard-links file-like and symlink inodes, rejecting
  directories and runtime-only descriptors.
  Buffer-backed allocation and truncate reject unrepresentable sizes with
  `Overflow` before resizing.
  Read/write syscall entrypoints validate guest file offsets before host-size
  conversion.
  Absolute symlink targets resolve from the sandbox virtual root in the WASIX
  path loader and `path_open2`.
  `path_symlink` creates links without resolving the target path first, allowing
  dangling symlinks and preserving the exact readlink target.
  `path_readlink` accepts exact-size buffers for non-NUL-terminated link
  targets.
  Symlink stat uses virtual inode data for link type and link-target size.
  WASIX `chdir` uses sandbox virtual path resolution and normalizes `.`/`..`
  before storing the current directory.
  Relative `fd_seek` now validates overflow/underflow before mutating the file
  cursor, and `SEEK_END` returns the computed file/buffer offset instead of a
  stale cursor.
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
  while retaining per-file artifact metadata and hashes in the manifest. During
  SIFS-backed WASIX execution, toolchain support files are exposed through
  read-only virtual preopens built from those SIFS objects instead of a host
  execution cache.
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

## Appendices

### Settled Decisions

| Decision | Resolution |
|---|---|
| Sandbox instance root | `/services/sandbox` |
| `/tmp` durability | Durable for the sandbox instance; cleared by default on agent session reboot; retention across sessions is an instance policy flag; deleted when the sandbox instance is destroyed |
| First userspace target | Sinomo-maintained BusyBox build |
| Shell model | Default `sh` is the real upstream BusyBox ash applet from the maintained BusyBox WASIX package; Bash remains the richer shell package target; full fork/exec shell parity, exec/wait, and job control still wait for the fd/process substrate; structured shell is out of scope for now |
| Command policy | Real, fully functional Linux/POSIX tools or no tool; no fake substitutes |
| Sandbox interior permissions | Agent has full permission inside the sandbox instance |
| Initial workload target | Coding tasks with language toolchains available through sandbox toolchain packages |
| Toolchain storage | Replicate complete toolchains through SIFS |
| Agent command stream | Raw stdout/stderr chunks |

### Remaining Open Questions

No product-level open questions remain for the MVP. The active route is WASIX-only. Remaining implementation work is to close the fd/process substrate gaps before claiming job control, arbitrary POSIX fork/exec, or other process-oriented tools, and to expose additional initial commands only after their real implementations pass the verifier.

### Near-Term Recommendation

Continue with a narrow but real WASIX-first MVP:

```text
WASIX runtime + SIFS workspace + Sinomo-maintained BusyBox/Bash/Python toolchains + real file/search/build tools + policy-controlled /tmp + raw output streaming
```

Avoid host binary execution and avoid exposing fake tools. The active runtime target is WASIX because shell, Python, pip, native extensions, and build tools need real process behavior. The next runtime work should close the remaining fd/process gaps: arbitrary fork/exec/wait parity, job-control behavior, and stronger budget enforcement for non-yielding process trees. The highest-risk integration is now the boundary between that process substrate, SIFS-backed VFS semantics, raw output streaming, and reliable resume across node restarts.

### References

- WebVM repository: https://github.com/leaningtech/webvm
- WebVM 2.0 overview: https://labs.leaningtech.com/blog/webvm-20.html
- SIFS architecture: `docs/sifs.md`
- DFS architecture: `docs/dfs-architecture.md`
