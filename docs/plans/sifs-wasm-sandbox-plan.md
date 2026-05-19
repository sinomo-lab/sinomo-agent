# SIFS-Backed WASM Sandbox Plan

## Purpose

Sinomo needs a resumable sandbox for agent actions, tool execution, coding workflows, and long-running task workspaces. The sandbox is inspired by WebVM's terminal-first product shape, but Sinomo should not boot a full Linux distribution or emulate a whole machine. The target is a Rust-native WASM command runtime with a minimal real POSIX userspace, explicit Sinomo policy enforcement, and persistent workspace state stored in SIFS.

The primary workload is coding tasks. The foundation is still a real POSIX userspace: shell, process, descriptor, filesystem, signal, terminal, threading, and library behavior should be compatible with normal Unix programs within the documented sandbox limits. On top of that foundation, agents should bootstrap the tools they need through a real package manager inside the sandbox instead of Sinomo hard-coding Python, pip, compilers, or other language stacks as the baseline. If Sinomo cannot expose a genuine command with its real behavior, that command should be absent rather than replaced by a fake subset.

## Current Direction

- Runtime focus: WASIX first. WASI Preview1 remains a compatibility runner for simple single-process modules.
- Filesystem focus: all sandbox filesystem behavior goes through the sandbox virtual filesystem layer, with production state backed by SIFS under `/services/sandbox`.
- Userspace focus: a real POSIX userspace foundation plus `pacman` or a pacman-compatible package manager that installs user-needed tools from a Sinomo-maintained `wasm32-wasix` package repository.
- Policy focus: cluster policy controls mounts, toolchains, network fetch, resources, and command capabilities before state changes or command runs.
- Operations focus: command records, raw stdout/stderr chunks, logs, checkpoints, artifacts, and package/toolchain state are durable and inspectable.

Detailed implementation status and gaps are tracked in [`crates/sinomo-sandbox/docs/implementation-notes.md`](../../crates/sinomo-sandbox/docs/implementation-notes.md). Detailed architecture is tracked in [`crates/sinomo-sandbox/docs/architecture.md`](../../crates/sinomo-sandbox/docs/architecture.md). Package-build implementation guidance is tracked in [`crates/sinomo-sandbox/docs/sandbox-package-build-guide.md`](../../crates/sinomo-sandbox/docs/sandbox-package-build-guide.md).

## Roadmap

### Phase 0: Architecture And API Skeleton

Status: implemented.

Keep the sandbox model, IDs, manifests, resource limits, policy contracts, mount contracts, runtime boundaries, storage registration, and CLI command surface in `sinomo-sandbox` and `sinomo-cli`.

### Phase 1: Local Command Runtime

Status: partially implemented.

Maintain WASIX as the default command runtime and Preview1 as a compatibility path. Continue expanding real POSIX process, thread, descriptor, signal, TTY, filesystem, dynamic-linking, and Linux compatibility behavior in the runtime, VFS, compiler, sysroot, or system-library layer. Avoid program-specific runtime shims except as temporary compatibility debt documented in the crate status.

### Phase 2: SIFS-Backed Workspace

Status: partially implemented.

Make SIFS-backed execution the production default for sandbox instances, workspace files, `/tmp`, logs, checkpoints, artifacts, package caches, and toolchains. Local host-backed execution should remain a development backend with semantics kept aligned to SIFS-backed execution.

### Phase 3: POSIX Userspace Foundation

Status: partially implemented.

Provide the genuine POSIX userspace needed to run package managers, package scripts, and installed tools:

- subprocess, pipe, wait, signal, descriptor, filesystem metadata, and locking behavior for normal Unix programs;
- real `sh` and enough base utilities for package scripts and operator escape hatches;
- real file, text, archive, compression, signature, hash, and process tools needed by package installation and common build scripts;
- filesystem, `/dev`, `/proc`, `/tmp`, `/var`, `/usr`, dynamic-linking, terminal, and thread behavior compatible with ordinary packaged programs.

### Phase 4: Package-Manager Bootstrap

Status: implementation goal defined.

Use the POSIX foundation to make `pacman` or a pacman-compatible package manager work inside the sandbox:

- local package database under the sandbox root;
- repository sync, package download, dependency resolution, extraction, install/remove/upgrade/query, and script execution routed through Sinomo policy and the sandbox VFS;
- package state persisted to SIFS with command records and audit events;
- bootstrap installer limited to creating the initial package-manager-capable root, not replacing the in-sandbox package manager.

Python, pip, Clang/LLVM, Make, language runtimes, and other coding tools should be packages installed through this path based on the user's actual need. They should not be treated as mandatory baseline commands.

### Phase 5: Package Repository And Build System

Status: implementation goal defined.

Introduce the package repository and build system that feeds the sandbox package manager. The recommended direction is Arch-inspired, ALPM-style packaging for a Sinomo-maintained `wasm32-wasix` repository stored in SIFS. Official Arch Linux binary packages should not be installed directly because they target Linux/ELF host architectures, not WASIX/WASM. Existing Arch PKGBUILDs can be useful recipe templates, but automatic rebuild of arbitrary Arch packages is a later compatibility project.

### Phase 6: Production Operations

Status: partially implemented.

Harden daemon-backed execution, streaming, cancellation, timeout cleanup, resource accounting, node health reporting, package drift diagnostics, storage drift diagnostics, and recovery after daemon restart or node movement. All cluster network traffic must remain mediated by Sinomo networking and policy.

## Storage Contract

Production sandbox state is rooted at:

```text
/services/sandbox
```

Instance state uses:

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

Toolchain and package state use the sandbox service prefix as documented by the crate architecture docs.

`/tmp` belongs to the sandbox instance. By default, reboot clears `/tmp`; an instance policy flag may retain it. Destroying the sandbox instance deletes the whole instance root, including `/tmp`.

## Acceptance Criteria

The sandbox is production-ready for coding agents when:

- a daemon-backed SIFS sandbox can run a real shell and pacman-compatible package manager without host-command escape hatches;
- the underlying POSIX userspace is real enough for normal package scripts, subprocess trees, dynamic libraries, file metadata, and installed Unix tools;
- packages can be installed, removed, upgraded, and queried through the sandbox package manager, with package state persisted to SIFS;
- user-needed coding tools such as Python, pip, compilers, search tools, and archive tools are installable from the Sinomo `wasm32-wasix` repository;
- generated binaries and subprocess trees run through WASIX process semantics;
- package installation writes through the sandbox VFS and persists to SIFS;
- command output, exit status, resource use, and audit events are durable;
- cancellation, timeout, daemon restart, and node movement do not leak process trees, staged files, blocked writers, or inconsistent SIFS state;
- policy denials and missing capabilities are reported as clear sandbox health issues.

## Documentation Ownership

Keep this file as the roadmap. Do not use it as a detailed implementation log.

- Current architecture: `crates/sinomo-sandbox/docs/architecture.md`
- Current status, gaps, and implementation notes: `crates/sinomo-sandbox/docs/implementation-notes.md`
- Package build implementation guide: `crates/sinomo-sandbox/docs/sandbox-package-build-guide.md`
