# Sinomo Build Tools Sandbox Target

This target tracks real compiler and build-tool commands for coding sandboxes. The initial tracked commands are:

- `tcc`
- `make`
- `clang`

These commands are available only after Sinomo has real WASI or WASIX packages and verifier coverage. Do not expose host binaries, fake compilers, fake `make`, reduced parsers, or wrappers under these names. `make` and `tcc` currently have maintained package recipes and default verifier gates. Clang has a maintained WASIX recipe and an opt-in verifier because a full LLVM build is large enough that it should not run on every build-tools check by default.

`build-make-wasix.sh` is the real GNU Make package recipe. It builds upstream GNU Make 4.4.1 against the pinned WASIX sysroot, applies only the compile-time compatibility needed for wasix-libc's flexible `struct dirent` layout and WASIX startup, runs Binaryen asyncify/function-pointer cast emulation, and emits a generic `import-package` manifest for command `make`.

`build-tcc-wasix.sh` is the real TinyCC package recipe. It builds upstream TinyCC 0.9.27 as a WASIX command module, keeps TinyCC's compiler behavior intact, and only provides a WASIX portability stub for `execvp` so unsupported `-run` execution fails explicitly instead of escaping the sandbox. The current package targets TinyCC's x86_64 cross-compiler mode: it can preprocess and compile C sources from `/work` into sandbox output artifacts, while executing compiled native programs remains unsupported.

`build-clang-wasix.sh` is the real Clang/LLVM package recipe. It targets LLVM 21.1.6 by default and pins the `llvm-project-21.1.6.src.tar.xz` hash from the upstream release attestation. It builds native LLVM tablegen tools for the build host, then cross-builds LLVM's Clang driver, LLD, `llvm-ar`, `llvm-ranlib`, and `llvm-nm` as WASIX command modules. It packages those modules with the same pinned WASIX sysroot and Clang resource artifacts that the driver uses inside the sandbox. It also emits `host-env.sh`, which points host-side Sinomo module recipes at the same wasi-sdk and WASIX sysroot inputs, so outside-the-sandbox module builds and inside-the-sandbox compiles share one pinned compiler input set instead of two separate Clang toolchains. The sandbox still executes only the WASIX command modules; host compiler binaries are never exposed as sandbox commands.

`verify-build-tools-wasix.sh` builds BusyBox WASIX, Bash WASIX, GNU Make WASIX, and TinyCC WASIX into one pinned toolchain and runs `sinomo sandbox toolchain verify-make` plus `sinomo sandbox toolchain verify-tcc` against the result. That gate passes locally; set `VERIFY_BACKEND=daemon-local` to exercise daemon-backed SIFS import and execution. The availability claims are still scoped: `make` declares both `child-process-exec` and `wasix.unpinned-child-exec`, executes recipes through the sandbox `sh`/BusyBox commands, and can run sandbox-local WASM modules and scripts from writable mounts such as `/work` when policy allows that capability. `tcc -run` remains explicitly unsupported.

Set `VERIFY_CLANG=1` on `verify-build-tools-wasix.sh` to include the Clang package in the same pinned toolchain. For a narrower compiler-only check, run `verify-clang-wasix.sh`; it imports the package, writes a real C source into `/work`, verifies `clang --version`, compiles to an object file, and asks Clang to invoke sandbox `wasm-ld` to link a WASM output.

## Availability Rule

Build tools follow the same rule as the rest of the sandbox command environment:

```text
real tool or no tool
```

If a command cannot compile and run inside the sandbox contract, it must stay absent. A Sinomo-specific helper may exist under a distinct name, but it must not be presented as `tcc`, `cc`, `make`, or another standard tool.

## Required Package Gates

Before `tcc` can remain available:

- build a real TinyCC command module for WASI or WASIX,
- package it through `sinomo sandbox toolchain import-package`,
- verify C compilation from `/work` source into a sandbox output artifact,
- verify diagnostics for invalid C input,
- verify that produced artifacts remain under SIFS-backed workspace paths,
- document unsupported native/linker features explicitly.

Before `make` can remain available:

- build a real Make command module for WASI or WASIX,
- package it through `sinomo sandbox toolchain import-package`,
- verify Makefile parsing from `/work`,
- verify variable expansion, dependency ordering, and failed recipe status,
- verify recipe command execution only through pinned sandbox commands,
- document unsupported job control, shell, and process features explicitly.

Before `clang` can remain available:

- build real Clang/LLVM command modules for WASIX,
- package Clang, LLD, and LLVM helper modules through `import-package`,
- package the sysroot and Clang resource directory as read-only artifacts,
- verify `clang --version` inside the sandbox,
- verify C compilation from `/work` source into `/work` output artifacts,
- verify linker child-process execution through pinned sandbox `wasm-ld`,
- keep host-side module recipe builds pointed at the same pinned compiler inputs instead of maintaining an unrelated second Clang distribution.

Current recipe status: `build-make-wasix.sh` builds and packages a real GNU Make WASIX module. `build-tcc-wasix.sh` builds and packages a real TinyCC WASIX module. `build-clang-wasix.sh` builds and packages Clang/LLVM as a WASIX module set, but its full verifier is intentionally opt-in because LLVM is much larger than the other build tools. `verify-build-tools-wasix.sh` validates the BusyBox, Bash, Make, and TCC package manifests, imports them into one pinned toolchain, creates a sandbox, and runs the maintained `verify-make` and `verify-tcc` workflow cases. With `VERIFY_CLANG=1`, it also merges the Clang package before creating the sandbox. The same gate supports `VERIFY_BACKEND=daemon-local`, which exercises daemon-backed SIFS toolchain import and command execution.

## Expected Verifier Shape

The maintained gate is:

```text
crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
```

Run it locally:

```text
crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
```

Run the daemon/SIFS gate against the packages already built by the local run:

```text
SKIP_BUILD=1 VERIFY_BACKEND=daemon-local crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
```

Run the heavyweight Clang gate:

```text
VERIFY_CLANG=1 crates/sinomo-sandbox/toolchains/build-tools/verify-build-tools-wasix.sh
```

Run only the Clang package gate:

```text
crates/sinomo-sandbox/toolchains/build-tools/verify-clang-wasix.sh
```

`sinomo sandbox doctor` reports missing implemented build tools, including `make` and `tcc`, as warnings when the sandbox's pinned toolchain does not contain them.

The verifier creates a disposable Sinomo data directory for local and daemon-local runs and removes it on success or failure. Set `SINOMO_VERIFY_DATA_DIR=<path>` to use an operator-owned directory, or set `KEEP_SANDBOX=1` when the generated sandbox state should be retained for debugging.
