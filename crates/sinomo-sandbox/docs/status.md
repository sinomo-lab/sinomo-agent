# Sandbox Implementation Status

This document tracks the current implementation state for `sinomo-sandbox`.
Keep the project roadmap in `docs/plans/sifs-wasm-sandbox-plan.md`, keep the
crate README focused on developer onboarding, and keep the detailed remaining
gap register in `docs/gaps.md`.

## Current Baseline

- WASIX is the default runtime. Preview1 is retained for compatibility with
  simple single-process modules.
- Local and daemon-backed SIFS sandbox operations are wired through the CLI for
  create, status, config, run, shell, logs, file inspection, file writes, HTTP
  fetch, checkpoint, resume, reboot, destroy, toolchain import, and policy.
- Instance manifests, command records, stdout/stderr logs, checkpoints,
  toolchains, support artifacts, and package manifests are stored as structured
  sandbox state.
- Daemon-backed operation writes sandbox state through SIFS under
  `/services/sandbox`.
- The default policy lets a sandbox use available host memory and does not set
  wall-time or workspace-size limits unless cluster policy overrides them.
- Ambient WASIX HTTP is denied. Network fetches go through the mediated sandbox
  fetch path and produce audit records.

## Runtime Status

The WASIX runner currently provides:

- mapped virtual preopens for workspace, toolchain mounts, `/dev`, and `/proc`,
- stdin/stdout/stderr capture and streaming,
- process-tree timeout and cancellation signaling for yielding process trees,
- memory-budget checks through Wasmer tunables,
- instruction metering when a module shape supports it,
- raw WASIX HTTP import denial,
- generated `/dev/null`, `/dev/zero`, `/dev/random`, `/dev/urandom`, stdio
  aliases, `/dev/fd`, and `/proc/self/fd` style entries,
- generated procfs views for common self, task, mount, fdinfo, cwd/root/exe,
  argv, environment, and status probes,
- partial process spawn, exec, wait, signal, process group, session, TTY, cwd,
  environment, and PATH behavior,
- close-on-exec descriptor cleanup is applied before normal WASIX module start
  and before built-in WASIX command dispatch, and a runtime regression verifies
  a `CLOEXEC` descriptor is unavailable to a spawned child,
- root virtual path traversal resolves preopen aliases in both component form
  (`work`) and absolute-alias form (`/work`), so cwd and stat-style operations
  share the same virtual namespace as open-style operations,
- `proc_join` reports normal child exit status and reaps joined child processes
  so repeated joins on the same child fail,
- built-in WASIX commands that finish inside the runtime, such as the
  `/bin/wasmer` help path, now run normal process-exit cleanup before their
  join handle is reported finished, so child process records and inherited
  descriptors are not left live,
- failed WASIX spawn setup paths for child cwd, stdio setup, and `proc_spawn2`
  fd actions now run child environment cleanup before the forked process record
  is removed,
- `proc_spawn2` cwd fd actions now apply runtime-backed `Chdir` and `Fchdir`
  updates to the child environment before module launch, including process
  metadata synchronization for fd-based cwd changes,
- forked `proc_spawn` and `proc_spawn2` children that fail during executable
  loading, built-in dispatch, or module launch are now marked terminated before
  their process record is removed, so failed child creation does not leave a
  live thread status behind,
- non-vfork `proc_exec3` failures now restore the caller's argv, environment,
  executable-path metadata, FD table, preopen FD list, current directory, and
  union bookkeeping before returning the exec error, matching the POSIX
  expectation that a failed `execve` leaves the caller running with its original
  process metadata and descriptor state,
- `proc_fork_env` plus failed `proc_exec3` is covered by a runtime regression
  that verifies the vfork-style child can return from the failed exec, exit via
  `proc_exit2`, restore the parent environment, and be reaped by the parent,
- process-group signaling is covered by a runtime regression that moves a
  spawned child into its own group, sends `SIGKILL` to the negative process
  group id, and observes signal termination through `proc_join`,
- process-group stop reporting is covered by a runtime regression that sends
  `SIGSTOP` to a child group, observes `JoinStatusType::Stopped` through
  `proc_join` without reaping the child, then resumes and kills the same child,
- `proc_setpgid` now rejects session leaders, and a runtime regression covers
  the initial process/session leader case so shell job-control setup cannot
  silently accept an invalid process-group transition,
- continued child status is reported for resumed stopped children, can be
  observed without consuming it, and can then be consumed through the WASIX
  continued-status syscall; blocking wait-status paths re-check after
  subscription so stop/continue/exit events are not lost between the pre-check
  and wait registration,
- WASIX compatibility aliases for `process_spawn`, `pipe`, `pipe2`, `kill`,
  `raise`, `unshare`, `setns`, and `eventfd`; plain POSIX process/session
  names such as `getpid`, `getppid`,
  `getpgrp`, `getpgid`, `getsid`, `setpgid`, `setsid`, `tcgetpgrp`,
  `tcgetsid`, and `tcsetpgrp` now use direct POSIX-shaped return values while
  the `proc_*` names retain the WASIX pointer-return ABI,
- POSIX `kill` and `raise` wrappers now return `0` or negative errno while
  legacy `proc_signal` and `proc_raise` keep their WASIX errno-return shape,
- POSIX identity aliases for `getuid`, `geteuid`, `getgid`, `getegid`,
  `getresuid`, `getresgid`, `setuid`, `seteuid`, `setgid`, `setegid`,
  `setresuid`, `setresgid`, `getgroups`, and `setgroups`,
- POSIX exec vector wrappers for `execv`, `execve`, `execvp`, and `execvpe`;
  these read normal null-terminated argv/envp vectors and lower them into the
  WASIX exec path,
- POSIX `waitpid` wrapper that maps WASIX child wait results into conventional
  integer wait status values and supports `WNOHANG`, `WUNTRACED`, and
  `WCONTINUED`,
- POSIX `waitid` status-selection wrapper that returns child pid, signal,
  `CLD_*` code, and status scalar fields for libc `siginfo_t` construction,
- POSIX `fork` and `vfork` wrappers that return 0 in the child, the child pid
  in the parent, and negative errno values on failure; modules that lack the
  asyncify unwind/rewind hooks fail before any child process is created,
- Linux `clone` thread compatibility for `CLONE_THREAD | CLONE_VM |
  CLONE_SIGHAND` callers; the runtime creates a same-process WASIX thread,
  shares memory, writes parent/child TID slots when requested, and resumes the
  cloned thread from the asyncified syscall continuation with return value zero,
- POSIX/Linux process and thread compatibility exports for `_exit`, `gettid`,
  `set_tid_address`, `get_clear_child_tid`, `set_robust_list`,
  `get_robust_list`, `getrlimit`, `setrlimit`, `prlimit`, `getrusage`,
  `getpriority`, `setpriority`, `nice`, `sched_getscheduler`,
  `sched_getparam`, `sched_setscheduler`, `sched_setparam`,
  `sched_yield`, `sched_get_priority_max`, `sched_get_priority_min`,
  `sched_rr_get_interval`, `sched_getaffinity`, `sched_setaffinity`,
  `sched_getcpu`, `clock_gettime`, `clock_getres`, `sleep`, `usleep`,
  `nanosleep`,
  `clock_nanosleep`, `arch_prctl`, `membarrier`, `rseq`, `tkill`, and
  `tgkill`,
- Linux pidfd compatibility exports for `pidfd_open`, `pidfd_send_signal`,
  `pidfd_getfd`, `pidfd_status`, and `pidfd_pid`, with descriptor ownership
  tracked in WASIX runtime state rather than CPython-local tables,
- Linux timerfd compatibility exports for `timerfd_create`,
  `timerfd_settime`, `timerfd_gettime`, `timerfd_read`, `timerfd_ready`,
  `timerfd_remaining_ms`, and `timerfd_known`, with timer state tracked in
  WASIX runtime state rather than CPython-local tables,
- POSIX interval timer compatibility exports for `timer_create`,
  `timer_settime`, `timer_gettime`, `timer_getoverrun`, and `timer_delete`,
  with timer lifecycle and scheduling state tracked in WASIX runtime state
  rather than CPython-local tables,
- POSIX process interval timer compatibility exports for `getitimer`,
  `setitimer`, `alarm`, and `ualarm`, with process timer state tracked in
  WASIX runtime state rather than CPython-local tables,
- POSIX alternate signal stack compatibility exports for `sigaltstack`, with
  active stack state tracked in WASIX runtime state rather than CPython-local
  globals,
- POSIX signal mask compatibility exports for `sigprocmask` and
  `pthread_sigmask`, with each WASIX thread tracking its own active signal mask
  in runtime state rather than sharing a CPython-global mask,
- Linux `rt_sigreturn` compatibility returns successfully because the current
  signal delivery path invokes handlers synchronously instead of building a
  kernel signal frame,
- Linux `FUTEX_FD` compatibility creates a runtime-backed `eventfd` that is
  notified by futex wake paths for legacy pollable futex users,
- Linux signalfd compatibility exports for `signalfd`, `signalfd_known`,
  `signalfd_mask_low`, and `signalfd_mask_high`, with descriptor mask
  ownership tracked in WASIX runtime state while libc-specific signal record
  formatting remains in the CPython compatibility layer,
- WASIX TTY/job-control compatibility aliases for `tcgetpgrp`, `tcgetsid`, and
  `tcsetpgrp`,
- WASIX thread-local key create/destroy/set/get storage with process-global
  keys and per-thread values,
- thread-local destructor callbacks are invoked during normal WASIX thread
  exit, with bounded repeated passes for destructors that install new values,
- explicit `thread_exit` from a WASIX worker still runs normal thread cleanup,
  including thread-local destructor callbacks,
- spawned WASIX worker threads mark their task status finished when the
  `wasi_thread_start` callback returns, so `thread_join` wakes without relying
  on process timeout or termination,
- imported shared-memory WASIX modules can spawn and join worker threads through
  `thread_spawn_v2`/`thread_join`, matching the memory shape expected from
  threaded Clang/pthread outputs,
- imported shared-memory WASIX modules can block a worker in `futex_wait`, wake
  it from another thread through `futex_wake`, and move waiters between futex
  addresses through `futex_requeue`,
- imported shared-memory WASIX modules can wait on multiple futex addresses
  through `futex_wait_any` and report the index of the address that was woken,
- joined non-main WASIX threads are reaped from the process table so thread
  metadata and task-count guards do not linger for the rest of the process.
- cancellation wakes WASIX commands blocked in futex deep sleep, including the
  case where the main thread is waiting in `thread_join` while a worker is
  blocked in `futex_wait`.
- WASIX task-count accounting now rejects thread/process creation at the
  configured boundary instead of admitting one extra active task, and thread
  creation reports `EAGAIN` when that resource limit is reached.
- failed WASIX thread creation paths that discover a missing
  `wasi_thread_start` export now remove the pre-created thread handle, so
  repeated failed `thread_spawn_v2` calls do not consume the process task
  budget.
- threaded CPython workloads avoid the guest asyncify futex path and use
  shared-memory atomic waits/wakes for Python's libc futex calls.

Runtime gaps are tracked in [gaps.md](gaps.md).

## Threading Status

Basic threaded execution has been proven for WASIX modules that import shared
memory. Threaded modules should be built with imported shared memory; modules
that define and export their own shared memory can spawn smoke-test workers but
do not provide the same shared-memory behavior expected by real pthread
toolchains.

WASIX thread-local storage key syscalls are wired for process-global keys and
per-thread values. Destructor function pointers are recorded with keys and
invoked through the guest indirect function table during normal return and
`proc_exit` cleanup paths.

The Clang WASIX build recipe has been adjusted to inject the expected default
threading linker flags for generated programs:

```text
-pthread
-Wl,--shared-memory
-Wl,--import-memory
-Wl,--extra-features=atomics
-Wl,--extra-features=bulk-memory
-Wl,--extra-features=mutable-globals
```

The Clang package verifier now covers normal `clang` invocation for hello-world
compilation, `open(O_CREAT)` filesystem behavior, and a Clang-built pthread
program using `pthread_create`, mutex, condvar, and `pthread_join`. The verifier
also checks that the pthread output imports shared `env.memory`, which is the
shape the WASIX runner expects for real threaded programs.

BusyBox WASIX and CPython WASIX recipes now lower POSIX `fork()` and `vfork()`
calls to the direct WASIX `fork`/`vfork` compatibility imports. CPython WASIX
also lowers thread-shaped Linux `clone` calls to the runtime `clone_thread`
import instead of keeping the shared-address-space case as a libc-side
unsupported branch. BusyBox WASIX
also lowers `getppid`, `getpgrp`, `getpgid`, `getsid`, `setpgid`, `setsid`,
`tcgetpgrp`, `tcgetsid`, `tcsetpgrp`, `kill`, `raise`, `dup`, `dup2`, `pipe`,
`chdir`, `execv`, `execve`, `execvp`, `fchdir`, `wait`, and `waitpid` to runtime
process imports instead of returning compatibility stubs. The legacy
pointer-return
`proc_fork` ABI, length-based `proc_chdir` ABI, positive-errno
`proc_fchdir` ABI, and WASIX errno-return `proc_signal`/`proc_raise` ABI remain exported for
existing WASIX callers, but new POSIX-facing toolchain wrappers no longer need
to encode child pids or paths through non-POSIX shapes.
CPython WASIX lowers Linux clear-child-TID, robust-list, sleep, clock,
`sched_yield`, scheduler priority bounds, scheduler interval, `waitid`,
`arch_prctl`, `membarrier`, `rseq`, `tkill`, `tgkill`, `rt_sigreturn`, `FUTEX_FD`, pidfd,
timerfd, POSIX timer, process interval timer, `sigaltstack`, signal masks, and signalfd syscall paths plus
`pipe2`, `dup3`, `eventfd`, `unshare`, and `setns` descriptor/process
operations into runtime process/thread imports instead of keeping those
lifecycle, descriptor, event, wait-status, and namespace hooks entirely inside
the compatibility layer.

Threading gaps are tracked in [gaps.md](gaps.md).

## Filesystem Status

The sandbox has local and SIFS virtual filesystem backends, staging helpers,
path normalization, symlink escape checks, quota accounting, workspace diffing,
logs, checkpoints, and read-write mount sync.

Generated virtual filesystem features include:

- per-run `/dev` without host device mappings,
- generated `/proc` entries for common Linux process probes,
- descriptor aliases such as `/dev/fd/<n>` and `/proc/self/fd/<n>`,
- package/toolchain mounts under `/bin`, `/lib`, `/share`, `/include`, and
  `/etc`.

Filesystem gaps are tracked in [gaps.md](gaps.md).

## Toolchain Status

Maintained packages:

- BusyBox WASIX: base file, text, archive, and search tools.
- Bash WASIX: real `bash` and `sh`.
- CPython WASIX: Python, pip, subprocess, native-extension loading, and the
  current `numpy` plus `matplotlib` gate.
- Make WASIX: real GNU Make workflow target.
- TCC WASIX: lightweight C compiler target.
- Clang/LLVM WASIX: LLVM 21.1.6 package recipe for Clang, LLD, LLVM helpers,
  and shared sysroot/resource artifacts.
- QuickJS Preview1: compatibility JavaScript helper package.

The CPython WASIX native gate currently passes locally with real `pip`:
`pip install --find-links` installs `numpy` and `matplotlib` into the sandbox
workspace, `numpy` imports and runs array operations, and `matplotlib` imports
with the Agg backend and renders a PNG artifact inside `/work`. The Python main
module is linked through the WASIX C++ linker so dynamically loaded native
extension side modules can resolve the C++ runtime ABI from the main image.

Toolchain gaps are tracked in [gaps.md](gaps.md).

## Process-Plan And Shell Status

`SandboxProcessPlan` can run multiple pinned commands with explicit
stdout-to-stdin pipes. It is a low-level execution substrate, not a shell syntax
implementation.

`sinomo sandbox shell` requires a pinned real `sh`; the maintained route is the
WASIX Bash package. BusyBox ash should not be patched to fake missing process
support. The runtime must support the process behavior real shells need.

Shell and process gaps are tracked in [gaps.md](gaps.md).

## Operational Gaps

- Improve `sinomo sandbox doctor` readiness reporting for shell, Python,
  threading, build tools, and SIFS placement.
- Add resource accounting for CPU, process count, thread count, blocked output,
  and cleanup after cancellation.
- Harden durable streaming for very large stdout/stderr streams.
- Make daemon recovery detect and clean up leaked runtime work after process
  crashes or node restarts.
- Add cluster-facing health signals for sandbox runtime failures, policy
  denials, storage issues, and toolchain package drift.
