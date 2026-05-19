# Sinomo Bash Sandbox Toolchain

Bash is a required coding-sandbox command and also provides the maintained `sh` command by invoking the same upstream Bash WASIX module under the `sh` argv0. It is not currently exposed by the default sandbox toolchain because a real shell environment requires the sandbox runtime to support genuine process execution, command lookup, redirection, and pipelines. Sinomo must not provide a fake shell or a shell-like parser under the `bash` or `sh` command names.

## Reproducible WASIX Build Recipe

This directory now contains the first Sinomo-owned Bash WASIX package target:

```text
Containerfile
build-bash-wasix.sh
```

The recipe pins GNU Bash 5.2.37, wasi-sdk 24.0, Binaryen 124, and wasix-libc `v2026-05-12.1`, verifies all downloads by SHA-256, configures upstream Bash for `wasm32-wasi` with the WASIX sysroot, applies Binaryen asyncify and function-pointer cast emulation, and emits a generic `import-package` manifest when the build succeeds. Asyncify is required by the non-EH WASIX toolchain path for setjmp/longjmp-style control flow, and function-pointer cast emulation keeps Bash's generic callback/unwind-protect paths from trapping on strict wasm indirect-call signatures. The recipe intentionally stays close to upstream Bash and uses WASIX libc's real process-oriented symbols instead of replacing Bash shell behavior. If configure, compilation, linking, or runtime verification fails, that failure is a runtime/toolchain blocker to fix; it is not permission to expose a shim, BusyBox `sh`, or a reduced parser under the `bash` command name.

Build in a clean container:

```text
docker build -f crates/sinomo-sandbox/toolchains/bash/Containerfile -t sinomo-bash-wasix .
docker run --rm -v "$PWD/crates/sinomo-sandbox/toolchains/bash/out:/src/crates/sinomo-sandbox/toolchains/bash/out" sinomo-bash-wasix
```

Or run directly on a Linux host with curl, tar, make, autoconf-compatible build tools, and Rust/Cargo available:

```text
crates/sinomo-sandbox/toolchains/bash/build-bash-wasix.sh
```

The intended output directory contains:

```text
bash-5.2.37-wasix.wasm
package.json
bash-5.2.37-build.manifest
```

Import into the BusyBox-based coding toolchain only after the package builds and the Bash verifier passes:

```text
sinomo sandbox toolchain import-package --daemon --into toolchain_busybox crates/sinomo-sandbox/toolchains/bash/out/package.json
```

## Availability Gate

`bash` can be registered for ordinary agent use only after all of these are true:

- the runtime has a real child-process substrate for sandbox commands,
- the Bash package is built from real upstream Bash or an equivalent maintained source package,
- the package is imported through `sinomo sandbox toolchain import-package`,
- `sinomo sandbox shell` can execute the pinned command without fallback logic,
- the Bash verifier passes inside a SIFS-backed sandbox.

Minimum verifier cases:

```text
bash -lc 'printf "%s\n" hello'
bash -lc 'printf "%s\n" hello | grep hell'
bash -lc 'printf "%s\n" hello > /work/out && cat /work/out'
bash -lc 'mkdir -p /work/a && touch /work/a/file && test -f /work/a/file'
bash -lc 'set -e; false; printf unreachable'
sh -c 'printf "%s\n" hello | grep hell'
```

The verifier must fail if `bash` or `sh` is a shim, host command, BusyBox `sh`, or any other substitute.

Inspect the maintained target from the CLI with:

```text
sinomo sandbox toolchain bash-plan
```

## Current Status

A pinned Bash WASIX recipe is checked in, and the recipe now builds a real GNU Bash WASIX module plus a generic Sinomo package manifest for `bash` and `sh`. The combined WASIX toolchain path works locally and through a daemon-local SIFS-backed sandbox: import the BusyBox WASIX package first, import this Bash package into that same toolchain, create a sandbox pinned to it, and run `sinomo sandbox toolchain verify-bash <sandbox-id>`. The verifier passes Bash builtin `printf`, external `grep`, redirection plus external `cat`, descriptor duplication with `2>&1`, command substitution, `set -e`, `/work` shebang execution, and `sh -c` external-pipeline cases.

Use the end-to-end local gate for recipe changes:

```text
crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
```

It builds the BusyBox WASIX external-command package, builds the Bash WASIX package, verifies both package manifests, imports Bash into the BusyBox WASIX toolchain in a disposable local data directory, creates a sandbox, and runs the Bash fidelity suite.

Use the SIFS-backed daemon-local gate before treating Bash as production-ready:

```text
VERIFY_BACKEND=daemon-local crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh
```

When `SINOMO_BIN` is not provided, the verifier runs `cargo run` with `sinomo-sandbox/wasix-runtime` enabled. Set `SINOMO_CARGO_FEATURES=` only when intentionally verifying a preconfigured build surface. The verifier removes its automatically-created disposable Sinomo data directory after the run. Set `SINOMO_VERIFY_DATA_DIR=<path>` to use an operator-owned directory, or set `KEEP_SANDBOX=1` to retain generated state for debugging.

Bash is still not exposed by the default sandbox toolchain for ordinary agent use. The generated Bash and `sh` package manifests declare both `child-process-exec` and `wasix.unpinned-child-exec`; the default sandbox policy denies both, so ordinary clusters must explicitly allow the Bash/WASIX toolchain plus both capabilities before either command is available. The second capability records that Bash may execute sandbox-local WASM modules and scripts from writable mounts such as `/work`. BusyBox is now provided through the WASIX package manifest; external-command support should use that package rather than relabeling older Preview1 modules.
