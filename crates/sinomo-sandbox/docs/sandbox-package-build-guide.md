# Arch-Style Sandbox Package Build Guide

This guide describes the package build and install system for the Sinomo sandbox. The goal is an Arch-inspired package workflow for `wasm32-wasix`, with a real package manager inside the sandbox and automated source rebuilds for suitable official Arch packages.

Status: proposed. The `sinomo sandbox package` commands,
the `/services/sandbox/packages` repository layout, the bootstrap installer, and
the build worker below do not exist yet. Implemented pieces are the upstream
makepkg package recipe (`toolchains/build-tools/build-makepkg-wasix.sh`) and its
`sinomo sandbox toolchain verify-makepkg` smoke gate; toolchains are still
installed with `sinomo sandbox toolchain import-package`.

## Goals

The package system must:

- provide a `pacman` or pacman-compatible package manager inside the sandbox;
- store package repositories, package cache, source cache, and package database state in SIFS;
- build installable `wasm32-wasix` packages from source;
- automatically adapt and build suitable official Arch PKGBUILDs when the source is available, the package is not architecture-specific, and the build script stays within the sandbox build policy;
- let Python, pip, compilers, language runtimes, search tools, archive tools, and other coding tools be installed on demand as packages rather than treated as mandatory baseline commands;
- keep package installation, removal, upgrade, query, and script execution inside the sandbox contract;
- fix portability in the POSIX runtime, VFS, compiler, sysroot, or system library layers, not with package-specific runtime shims.

The system must not support official Arch Linux binary packages. Those packages are Linux ELF binaries for host architectures such as `x86_64`; the Sinomo sandbox needs WASIX/WASM modules and WASIX libraries. The Arch binary repos are useful as metadata and dependency references only, not as installable binary payloads.

## Target Package Model

Sinomo packages should be ALPM-compatible where practical:

- package archive with staged filesystem content;
- package metadata such as `.PKGINFO`, `.BUILDINFO`, and `.MTREE` or a compatible Sinomo extension;
- repository metadata containing package names, versions, dependencies, provides/conflicts/replaces, hashes, sizes, and file lists;
- local package database under the sandbox root.

The package target triple is `wasm32-wasix`. Package architecture should use a Sinomo package architecture name such as `wasm32-wasix`, not Arch's host `x86_64` or `any` binary package targets. Arch `arch=('any')` is useful as a hint that the package may be source-portable, but the output package is still a Sinomo WASIX package.

## SIFS Layout

Use SIFS as the durability boundary:

```text
/services/sandbox/packages/repos/wasm32-wasix/<repo>.db
/services/sandbox/packages/repos/wasm32-wasix/<repo>.files
/services/sandbox/packages/repos/wasm32-wasix/*.pkg.tar.zst
/services/sandbox/packages/sources/
/services/sandbox/packages/cache/
/services/sandbox/packages/builds/<build_id>/
/services/sandbox/packages/logs/<build_id>/
```

Sandbox instances keep package state in their root filesystem:

```text
/var/lib/pacman/
/var/cache/pacman/pkg/
/etc/pacman.conf
/etc/pacman.d/
```

If the implementation starts with a pacman-compatible command rather than upstream pacman, keep these paths compatible so upstream pacman can replace it later without migrating sandbox instances.

## Build Inputs

A package recipe can come from one of three sources:

1. Sinomo-maintained package recipes under the sandbox crate.
2. Official Arch PKGBUILDs fetched from Arch packaging repositories.
3. User or cluster-local package recipes approved by sandbox policy.

For official Arch PKGBUILDs, the automated builder should accept candidates when all of these are true:

- source is available and hashable;
- source URLs are fetchable through Sinomo-mediated fetch or are already cached in SIFS;
- the package is not inherently architecture-specific to Linux ELF, kernel modules, firmware, bootloaders, device drivers, or host hardware;
- the build script does not require privileged host operations, ambient network after source fetch, system service control, device access, user namespace mutation, host package-manager mutation, or undeclared host paths;
- declared dependencies can be resolved from the Sinomo `wasm32-wasix` repo or recursively built from accepted source recipes;
- the build system can target `wasm32-wasix` through the maintained compiler, sysroot, and runtime support.

Rejecting a package is normal. Rejections should be explicit and machine-readable so the next missing runtime, sysroot, dependency, or recipe feature is clear.

## Recipe Adaptation

Do not edit upstream source code semantically as part of generic package porting. The adaptation layer may:

- set target toolchain variables such as `CC`, `CXX`, `AR`, `RANLIB`, `LD`, `PKG_CONFIG`, `CFLAGS`, `CXXFLAGS`, and `LDFLAGS`;
- provide a `wasm32-wasix` sysroot, pkg-config path, CMake toolchain file, Meson cross file, Autoconf config site, and cargo/npm/python build metadata where appropriate;
- apply build-system patches needed to make the package target WASIX, when the patch does not fake runtime behavior;
- disable tests that require unsupported host hardware, host kernel features, or external services, while recording the disabled test reason;
- install files into Linux-style paths under the package root.

Package-specific compatibility shims are a last resort and should be tracked as debt. If many packages need the same shim, move the fix to the runtime, VFS, sysroot, compiler, or system library layer.

## Build Worker

The first build worker can run outside ordinary user sandboxes on a trusted Sinomo node, using the maintained WASIX compiler/sysroot. It should still use Sinomo policy, fetch, storage, and audit paths.

Expected command shape:

```text
sinomo sandbox package build <pkgname>
sinomo sandbox package build --from-arch <pkgbase>
sinomo sandbox package build --repo wasm32-wasix <pkgname>
sinomo sandbox package repo-add <package-file>
sinomo sandbox package sync
sinomo sandbox package install <sandbox-id> <pkgname>
```

Longer term, the same build workflow should run inside a builder sandbox once POSIX process, VFS, dynamic-linking, and package-manager behavior are strong enough.

The build worker should produce:

- package archive;
- package metadata;
- build log;
- source manifest and source hashes;
- dependency closure record;
- applied adaptation record;
- disabled test record;
- reproducibility metadata;
- pass/fail reason with owning layer for failures.

## Bootstrap Installer

Use a small Sinomo-native ALPM bootstrap installer only to create the initial package-manager-capable root:

1. parse repository and package metadata for the minimal package-manager root;
2. resolve the dependency closure for `pacman` or the compatible manager;
3. fetch package archives through Sinomo-mediated fetch/SIFS;
4. extract into the sandbox VFS;
5. create `/var/lib/pacman` or the compatible local package database;
6. write `/etc/pacman.conf` and repository configuration;
7. hand later install/remove/upgrade/query work to the in-sandbox package manager;
8. emit command records and audit events.

The bootstrap installer is not the long-term user-facing package manager. Once `pacman` or the compatible manager runs in the sandbox, normal package work should go through that command.

## In-Sandbox Package Manager

The in-sandbox package manager must:

- read repository metadata from the Sinomo `wasm32-wasix` repository;
- install, remove, upgrade, query, and verify packages;
- maintain package database locks and transaction records;
- run package install scripts through the sandbox shell and POSIX runtime;
- write only through the sandbox VFS;
- fetch only through Sinomo-mediated transport or SIFS package cache;
- emit command records and package audit events;
- preserve package state across daemon restart, node movement, and sandbox resume.

A pacman-compatible implementation is acceptable if it preserves the user-facing semantics, database paths, package metadata model, and transaction behavior we need. Upstream pacman remains the preferred compatibility target.

## Repository Policy

Repository operations should verify:

- package hash;
- metadata hash;
- allowed repository;
- allowed package provenance;
- dependency closure;
- package capabilities;
- file conflicts;
- install script policy;
- source provenance for packages built from Arch PKGBUILDs;
- build worker identity and build log availability.

Repository metadata should be signed or hash-rooted by cluster policy before it is trusted by sandbox instances.

## POSIX Requirements

The package goal depends on the real POSIX userspace goal. Required behavior includes:

- real `sh`/`bash` behavior for package scripts;
- tar, zstd/xz/gzip, coreutils, findutils, grep, sed, awk, patch, diff, and hash/signature tools;
- process spawning, pipes, wait, signals, cancellation, process groups, and descriptor inheritance;
- filesystem metadata, permissions, ownership, symlinks, hardlinks, mtree-like checks, timestamps, directory iteration, and file locking;
- dynamic linking or a compatible static package-manager build;
- `/tmp`, `/var`, `/usr`, `/etc`, `/dev`, and `/proc` behavior expected by build scripts and package install scripts;
- mediated network fetch and package cache access through Sinomo policy.

Missing behavior should be implemented in the runtime, VFS, compiler, sysroot, or system library layer.

## Makepkg Base Environment Gap

The first `makepkg` target should bring up source-package workflows before
trying to support every Arch packaging feature. Excluding compilers, the current
sandbox already has a meaningful base: real WASIX Bash/`sh`, BusyBox applets for
common file and text operations, GNU Make, Python/pip, a Clang/LLVM package
recipe, and an upstream pacman makepkg package recipe. The current source
declarations and package recipes expose BusyBox commands `awk cat chmod cp cmp
date diff dirname echo env false find grep gzip head ln ls mkdir mv patch printf
pwd rm rmdir sed sha256sum sh sleep sort tail tar test touch true tr uname uniq
wc`, Bash commands `bash sh`, GNU Make as `make`, upstream pacman `makepkg`,
Python as `python pip`, and Clang/LLVM commands `clang clang++ cc c++ wasm-ld
ld.lld llvm-ar llvm-ranlib llvm-nm`.

The makepkg package recipe renders upstream pacman 7.1.0 scripts and configuration into the sandbox's normal Linux paths:

```text
/usr/bin/makepkg
/usr/share/makepkg/
/etc/makepkg.conf
/etc/makepkg.conf.d/
/etc/pacman.conf
/etc/pacman.d/
```

The current verifier imports BusyBox, Bash, Make, makepkg, and TCC into one pinned WASIX toolchain and verifies `makepkg --version`. Full PKGBUILD execution is not claimed yet. A simple local-source `PKGBUILD` now reaches upstream libmakepkg startup, loads `/etc/makepkg.conf.d/*.conf` through Bash's real `compgen`, and then fails when upstream makepkg looks for a real `pacman` command. That should be fixed by bringing in real pacman/libalpm support, not by patching makepkg.

Upstream `makepkg.sh.in` declares its minimum external program set as `awk`, `bsdtar`/libarchive, `bzip2`, coreutils, `fakeroot`, `file`, findutils, gettext, `gpg`, `grep`, `gzip`, `sed`, `tput`/ncurses, and `xz`. The default `makepkg.conf` also assumes `curl`-based download agents, VCS client mappings, optional signing, optional stripping/debug helpers, multiple compression formats, and `INTEGRITY_CHECK=(ck)`. For Sinomo, those defaults should be narrowed in the first package until the corresponding real packages and policy paths exist.

The remaining base-system gaps for real `makepkg` are:

- `pacman`, `libalpm`, `repo-add`, `vercmp`, package database layout, package query, and dependency resolution under `/var/lib/pacman`;
- `bsdtar`/libarchive, because upstream `makepkg` uses archive type detection and extraction through libarchive rather than plain BusyBox `tar`;
- compression tools beyond the current BusyBox `gzip`: at minimum `bzip2`, `xz`, and `zstd`, with `lz4`, `lzip`, `lzop`, `lrzip`, and legacy `compress` either packaged or disabled through the sandbox makepkg config;
- checksum commands for configured integrity modes: `cksum`, `md5sum`, `sha1sum`, `sha224sum`, `sha256sum`, `sha384sum`, `sha512sum`, and `b2sum`; currently only `sha256sum` is documented as present in the BusyBox profile;
- `file`, because `makepkg` uses MIME/type detection during source extraction and package tidy steps;
- `gettext` runtime commands/libraries used by pacman scripts for messages;
- `fakeroot` or an equivalent in-sandbox ownership virtualization path; even though agents have full permission inside the sandbox, `makepkg` expects fakeroot semantics while creating root-owned package metadata;
- GnuPG/GPGME for source verification and optional signing, or a first milestone that runs with `--skippgpcheck` and `BUILDENV=(!sign)` while the signature stack is brought in;
- mediated download commands, primarily `curl`, plus policy integration for `DLAGENTS`; direct ambient network access must remain denied;
- VCS clients for VCS sources: `git` first, then `svn`, `hg`, `bzr`, and `fossil` as package coverage expands;
- `strip`, `objcopy`, `readelf`, and `debugedit` if default `OPTIONS=(strip debug ...)` behavior is enabled; an early config can disable `strip`, `debug`, `lto`, and `autodeps` until the binutils/debug toolchain exists;
- stronger base applet coverage than the current BusyBox profile: `install`, `readlink`, `realpath`, `basename`, `mktemp`, `du`, `stat`, `id`, `whoami`, `groups`, `getent`, `tee`, and `tput` are common enough that they should be packaged as real tools before broad PKGBUILD ingestion;
- VFS support for package-manager metadata: stable mode bits, ownership/group metadata, symlinks, hardlinks, timestamps, directory iteration, atomic rename, file locking, and mtree-like manifest generation over SIFS-backed state;
- process support for the shell scripts themselves: process substitution, pipelines, traps, background jobs, signal cleanup, child reaping, descriptor inheritance, and cancellation of subprocess trees.

Recommended preparation order:

1. Add the missing core utilities needed by makepkg scripts to the BusyBox WASIX package or to dedicated real-tool packages.
2. Keep the pacman makepkg package recipe aligned with upstream scripts and Linux paths.
3. Add pacman/libalpm local database operations and a tiny SIFS-backed repository containing the base packages.
4. Add `libarchive`/`bsdtar`, `file`, `gettext`, `cksum`, and the missing checksum/compression commands.
5. Implement an in-sandbox fakeroot-compatible ownership layer in the runtime/VFS instead of patching `makepkg` packaging logic.
6. Enable mediated `curl` downloads through `DLAGENTS`, then add GPG/GPGME and VCS clients.
7. Re-enable strip/debug/autodeps after binutils and metadata fidelity are strong enough.

## Implementation Sequence

1. Define the Sinomo package metadata extensions and SIFS repository layout.
2. Implement the native ALPM bootstrap installer that writes through the VFS.
3. Package `pacman` or a pacman-compatible manager for `wasm32-wasix` with its minimal dependencies.
4. Add package bootstrap/install/remove/upgrade/sync/query CLI and daemon RPC commands.
5. Convert existing BusyBox, Bash, Python, Clang/LLVM, and Make toolchain scripts into installable package recipes.
6. Implement the trusted package build worker and repository publication path.
7. Add official Arch PKGBUILD ingestion for source packages.
8. Add automated PKGBUILD classification: accepted, rejected, needs dependency, needs runtime feature, needs sysroot feature, needs build-system adapter, or unsafe script.
9. Recursively build accepted dependency closures into the Sinomo `wasm32-wasix` repository.
10. Run package install and package script verification inside SIFS-backed
    sandbox instances.
11. Move build execution into builder sandboxes when the POSIX foundation is
    strong enough.
12. Expand automated Arch package coverage from simple libraries and CLI tools
    toward richer language stacks.

## Non-Goals

- Installing official Arch Linux binary packages directly.
- Running package build scripts with ambient host filesystem or network access.
- Faking unsupported commands or package-manager behavior with subsets while claiming compatibility.
- Patching each package's source code to work around missing generic POSIX runtime behavior.

## Sources

- Arch manual: `makepkg(8)`, which describes source download, validation, build, package staging, metadata generation, and package output.
- Arch manual: `PKGBUILD(5)`, which defines mandatory fields and package functions.
- Arch manual: `alpm(7)`, `alpm-package(7)`, and `PKGINFO(5)`, which describe ALPM packages, metadata, and repository concepts.
- Arch Wiki: clean chroot builds with `devtools`, documenting the official expectation that packages build in a clean Arch environment.
