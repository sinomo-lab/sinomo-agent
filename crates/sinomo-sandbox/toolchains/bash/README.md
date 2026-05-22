# Sinomo Bash Sandbox Toolchain

This package builds GNU Bash for the WASIX sandbox from unmodified upstream Bash source. The recipe does not patch Bash files and does not provide shell behavior through a shim, BusyBox alias, host command, or reduced parser. Required compatibility work belongs in the shared WASIX sysroot, the compiler/link flags, or the sandbox runtime.

## Build Recipe

The pinned recipe is `build-bash-wasix.sh`. It downloads and verifies GNU Bash 5.2.37, wasi-sdk 24.0, Binaryen 124, and wasix-libc `v2026-05-12.1`, prepares the shared Sinomo WASIX sysroot, configures upstream Bash for `wasm32-wasi`, builds it, runs Binaryen asyncify and function-pointer cast emulation, and writes a Sinomo `import-package` manifest for `bash` and `sh`.

```text
crates/sinomo-sandbox/toolchains/bash/build-bash-wasix.sh
```

The output directory contains:

```text
bash-5.2.37-wasix.wasm
package.json
bash-5.2.37-build.manifest
```

Containerized builds are available for clean hosts:

```text
docker build -f crates/sinomo-sandbox/toolchains/bash/Containerfile -t sinomo-bash-wasix .
docker run --rm -v "$PWD/crates/sinomo-sandbox/toolchains/bash/out:/src/crates/sinomo-sandbox/toolchains/bash/out" sinomo-bash-wasix
```

## Sysroot Integration

Bash uses the shared prepared WASIX sysroot from `toolchains/build-tools/prepare-wasix-sysroot.sh`. Bash-specific source patches are intentionally absent. The current compile integration relies on shared sysroot additions for POSIX declarations, `sigsetjmp`/`siglongjmp`, environment archive linkage, and the argc/argv `main` entry bridge. These changes are not Bash behavior changes; they make the common libc/toolchain surface match what upstream programs expect.

## Verification

Use the local end-to-end gate after changing the recipe, sysroot preparation, command execution, or package import logic:

```text
crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
```

The verifier builds or imports BusyBox WASIX external commands, imports the Bash package into the same toolchain, creates a disposable sandbox, and verifies:

- `bash -c` builtin execution,
- external pipelines through BusyBox commands,
- command substitution,
- redirection and descriptor duplication,
- `set -e`,
- `/work` shebang execution through `/bin/sh`,
- `sh -c` compatibility,
- sandbox-local WASM exec from a Bash command.

Use the daemon-backed SIFS gate before treating the package as cluster-ready:

```text
VERIFY_BACKEND=daemon-local crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
```

## Current Status

The upstream-source build and local verifier pass. The generated package is still capability-gated: both `bash` and `sh` declare `child-process-exec` and `wasix.unpinned-child-exec`, so ordinary clusters must explicitly allow the toolchain and both capabilities before the commands are available.

`bash -l` login-shell startup is tracked separately from the coding-sandbox noninteractive shell path. The current verifier intentionally uses `bash -c`; login-shell support should be fixed in the runtime or shared userspace when it is needed, without patching Bash source.
