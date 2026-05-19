# Sinomo QuickJS Sandbox Toolchain

This directory is the Sinomo-owned home for the first optional language runtime package target: a real QuickJS WASI preview1 command module imported through the generic sandbox toolchain package path.

QuickJS is not a shell and does not replace the BusyBox userspace. It is a larger language-runtime package intended for coding tasks that need a compact JavaScript interpreter inside the sandbox. Sinomo must not expose a fake JavaScript command: `qjs` should be registered only from a real QuickJS WASI module built by this recipe or an equivalent reproducible recipe.

## Reproducible WASIp1 Build Recipe

The recipe pins upstream QuickJS 2025-09-13-2 and wasi-sdk 24.0, verifies both downloads by SHA-256, builds a `qjs` WASI preview1 module, and emits a package manifest compatible with:

```text
sinomo sandbox toolchain import-package [--daemon] [--into <toolchain-id>] <package.json>
```

Build in a clean container:

```text
docker build -f crates/sinomo-sandbox/toolchains/quickjs/Containerfile -t sinomo-quickjs-wasi .
docker run --rm -v "$PWD/crates/sinomo-sandbox/toolchains/quickjs/out:/src/crates/sinomo-sandbox/toolchains/quickjs/out" sinomo-quickjs-wasi
```

Or run directly on a Linux host with curl, tar, make, clang-compatible build tools, and Rust/Cargo available:

```text
crates/sinomo-sandbox/toolchains/quickjs/build-qjs-wasi.sh
```

The output directory contains:

```text
qjs-2025-09-13-2-wasip1.wasm
package.json
quickjs-2025-09-13-2-build.manifest
```

The package manifest declares:

- toolchain id `01k4z8q5c00000000000000000` by default, overrideable with `QUICKJS_TOOLCHAIN_ID`,
- command `qjs`,
- BLAKE3 content hashes generated with `sinomo sandbox toolchain hash`,
- QuickJS license and version support artifacts under `share/quickjs/`.

Inspect the maintained target from the CLI with:

```text
sinomo sandbox toolchain quickjs-plan
```

Import the generated package into SIFS with:

```text
sinomo sandbox toolchain import-package --daemon crates/sinomo-sandbox/toolchains/quickjs/out/package.json
```

Then create a sandbox pinned to the generated toolchain id or update a policy allowlist as needed before running `qjs`. To add `qjs` to an existing BusyBox-based coding toolchain instead, import the same package with an explicit target:

```text
sinomo sandbox toolchain import-package --daemon --into toolchain_busybox crates/sinomo-sandbox/toolchains/quickjs/out/package.json
```

Local smoke check after a build:

```text
SINOMO_DATA_DIR=/tmp/sinomo-quickjs-smoke sinomo sandbox toolchain import-package --into toolchain_busybox crates/sinomo-sandbox/toolchains/quickjs/out/package.json
SINOMO_DATA_DIR=/tmp/sinomo-quickjs-smoke sinomo sandbox create --toolchain toolchain_busybox
SINOMO_DATA_DIR=/tmp/sinomo-quickjs-smoke sinomo sandbox run <sandbox-id> qjs -- -e 'console.log(1 + 1)'
SINOMO_DATA_DIR=/tmp/sinomo-quickjs-smoke sinomo sandbox toolchain verify-coding <sandbox-id> --allow-unsigned
```

For recipe changes that affect the complete coding-task toolchain, use the combined verifier from the toolchains directory:

```text
crates/sinomo-sandbox/toolchains/verify-coding-wasi.sh
```

It builds the pinned BusyBox and QuickJS recipes, imports QuickJS into the BusyBox toolchain in a disposable local `SINOMO_DATA_DIR`, creates a sandbox, and runs `sinomo sandbox toolchain verify-coding`. The generated data directory is removed automatically unless `SINOMO_VERIFY_DATA_DIR=<path>` or `KEEP_SANDBOX=1` is set. Set `SKIP_BUILD=1` to reuse already-built BusyBox and QuickJS outputs; the script validates that the expected BusyBox module and QuickJS package manifest exist before importing them.

## Current Scope

This target is an interpreter package, not a full Node.js replacement. It should support ordinary QuickJS script/module execution inside the current WASI command contract. Raw sockets, host process spawning, worker threads, and unrestricted host filesystem access are outside the sandbox contract and must remain absent unless Sinomo explicitly provides real mediated equivalents.

Build status:

- The pinned recipe, container entrypoint, package manifest generation, and support-artifact layout are captured here.
- The recipe maps build and repository paths out of the compiler-visible file names/debug paths so independent work directories produce byte-identical `qjs` modules.
- Import uses the existing generic `sinomo sandbox toolchain import-package` path.
- A local end-to-end smoke path has been verified: build, import, create a sandbox pinned to this toolchain, and run `qjs -e 'console.log(1 + 1)'`.
- `sinomo sandbox toolchain verify-quickjs` runs the first host-side runtime fidelity cases against a sandbox pinned to this toolchain. It covers expression evaluation, JSON/array runtime behavior, sandbox file reads, and ES module import from `/work`, runtime error reporting on stderr, and confirms host process spawning is unsupported inside the sandbox.
- `sinomo sandbox toolchain verify-coding` runs the QuickJS suite together with the BusyBox applet suite, and should be used for a composite coding toolchain imported with `--into`. It also runs a cross-tool workflow where QuickJS writes a file under `/work` and BusyBox `grep` reads it back.
- Add `--daemon` to run the same fidelity cases against a daemon-backed SIFS sandbox instance.
- Broader runtime fidelity cases for additional sandbox-denied features should be added before treating this package as production-ready for agent coding tasks.
