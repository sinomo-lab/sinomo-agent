# Sandbox Gap Register

This document tracks the detailed remaining gaps for `sinomo-sandbox`. The
project roadmap belongs in `docs/plans/sifs-wasm-sandbox-plan.md`; the crate
README is only the developer entry point.

## Production Target

The target is a real POSIX coding sandbox backed by SIFS. A developer or agent
should be able to use real shell commands, Python with pip, build tools, and
filesystem operations without host-command escape hatches. Missing features
should be closed in the runtime, virtual filesystem, toolchain package, or
cluster operation layer instead of patched around in individual tools.

## POSIX Process Gaps

- Complete fork, exec, spawn, wait, and wait-status behavior for shell and
  Python subprocess users.
- Support nested process trees with correct cwd, environment, argv, file
  descriptor inheritance, close-on-exec, redirection, and pipe behavior.
  Close-on-exec is now covered for normal spawned WASIX modules and built-in
  command dispatch; broader descriptor inheritance and redirection cases still
  need real-tool coverage.
- Implement robust signal delivery, exit propagation, cancellation, process
  groups, sessions, controlling TTY behavior, and foreground/background job
  control. Current coverage includes process-group `SIGKILL`, stopped-status
  reporting, and continued-status observation/consumption for a stopped child
  that is resumed with `SIGCONT`; `proc_setpgid` also rejects session leaders.
  Full shell job-control parity remains open.
- Clean up every failed nested process path so leaked descriptors, process
  records, blocked futures, and task counters do not survive command exit.
  The in-process WASIX built-in completion path now runs normal process-exit
  cleanup, and failed spawn setup now cleans up child environments for cwd,
  stdio, and `proc_spawn2` fd-action failures. Forked `proc_spawn` and
  `proc_spawn2` executable-load/module-launch failures now mark the failed
  child terminated before removing its process record. Non-vfork `proc_exec3`
  failures restore the caller's argv, environment, executable-path metadata,
  FD table, preopen FD list, current directory, and union bookkeeping. The
  `proc_fork_env` failed-exec path now has runtime coverage for returning to
  the child, exiting through `proc_exit2`, restoring the parent, and reaping
  the child. Deeper process-tree cleanup and full rollback of filesystem
  mutations such as union mounts still need the same treatment.
- Keep child exec containment on every executable lookup path, including shell
  lookup, scripts, generated binaries, and package-manager helpers.

## Threading Gaps

- Broaden real pthread coverage from Clang-built modules: futex
  wait/wake/requeue, wait-any, mutexes, condition variables, cancellation,
  multiple-worker programs, and multi-threaded subprocess interactions.
- Verify TLS key behavior and destructor callbacks across normal return,
  explicit thread exit, process exit, failed initialization, unwind, and
  cancellation paths.
- Track blocked threads, runnable threads, task counts, peak memory, and CPU
  use as runtime resources instead of only enforcing simple task boundaries.
  Failed `thread_spawn_v2` attempts that cannot run because the module lacks
  `wasi_thread_start` now release their pre-created thread handle, but broader
  failed-initialization and cancellation accounting still needs real workload
  coverage.
- Ensure cancellation wakes blocked futex, join, pipe, and process waits
  without corrupting shared state.

## Filesystem And VFS Gaps

- Route all runtime filesystem behavior through the sandbox virtual filesystem;
  host-backed filesystems must remain a development backend only.
- Improve Linux-like metadata behavior: permissions, ownership, chmod, uid/gid,
  umask, timestamps, inode identity, link counts, hardlinks, symlink metadata,
  and stat variants.
- Close edge cases for rename, unlink, open-create, truncation, append,
  directory iteration, descriptor aliases, and concurrent access.
- Run useful `pjdfstest` subsets and move permission checks into the runtime or
  VFS layer where they belong.
- Keep generated `/dev`, `/proc`, `/etc`, package mounts, and workspace mounts
  consistent across local and SIFS-backed execution.

## Toolchain Gaps

- Stabilize Bash and BusyBox as real shell and base utility packages without
  semantic patches to shell behavior.
- Remove the legacy CPython compatibility shim by moving every required ABI
  fix into the WASIX runtime, virtual filesystem, compiler, sysroot, or
  system-library layer. Python must build from upstream sources without
  program-specific source or compatibility patches.
- Broaden CPython and pip coverage beyond the current NumPy and matplotlib
  native-extension gate to more common coding workflows and package families.
- Keep Clang/LLVM, LLD, Make, and generated binaries working with one
  maintained WASIX toolchain and Linux-style support paths.
- Keep package recipes reproducible, pinned, hash-checked, and documented.
- Move required support files to their corresponding Linux paths instead of
  accumulating sidecar files that callers must know about.

## SIFS Integration Gaps

- Make SIFS-backed execution the default production path for instances,
  toolchains, logs, checkpoints, package caches, generated binaries, and
  artifacts.
- Keep local development execution and SIFS-backed execution semantically
  aligned for build systems and package managers.
- Support large-file workflows, package caches, and compiler artifacts without
  assuming local host paths.
- Preserve durable stdout/stderr chunks, command records, and audit events even
  across daemon restart or node movement.

## Operations Gaps

- Harden long-running output streaming and backpressure for large logs.
- Make cancellation, timeout, and daemon restart recovery clean up process
  trees, threads, staged files, temporary mounts, and blocked output writers.
- Report sandbox health for missing toolchains, policy denials, package drift,
  storage drift, leaked runtime work, SIFS write failures, and runtime crashes.
- Add resource accounting for CPU, peak memory, process count, thread count,
  file count, output bytes, and SIFS usage.
- Keep network access mediated through Sinomo policy and audit paths; ambient
  guest network imports should remain denied unless explicitly designed.

## Verification Gaps

- Run representative shell, Python, pip, Make, Clang/LLVM, and generated-binary
  workflows through the same command path users will invoke.
- Add workload-driven verification before conformance tightening; use real
  tools to expose missing runtime behavior.
- Track known verifier failures with the exact failing command, expected Linux
  behavior, current sandbox behavior, and owning layer for the fix.
