# Sinomo BusyBox Sandbox Toolchain

This directory is the Sinomo-owned home for the first sandbox userspace target: a real BusyBox build for the WASIX sandbox userspace.

The sandbox runtime does not emulate shell commands and does not expose fake subsets. A BusyBox toolchain import must point at the generated WASIX package manifest. Sinomo then registers that module as the pinned applet implementation for the command names declared by the package.

Current import command:

```text
sinomo sandbox toolchain import-package <package.json>
```

## Reproducible Build Recipe

Sinomo maintains the first BusyBox recipe in this directory:

```text
Containerfile
build-busybox-wasi.sh
busybox-wasi.fragment
```

The recipe pins upstream BusyBox 1.36.1, wasi-sdk 24.0, Binaryen 124, and wasix-libc `v2026-05-12.1`, verifies downloads by SHA-256 before extraction, applies the Sinomo applet fragment on top of BusyBox `allnoconfig`, builds a WASIX module, and applies Binaryen asyncify plus function-pointer cast emulation so the applet module satisfies the current Wasmer/WASIX runner shape. The output directory contains:

```text
busybox-1.36.1-wasix.wasm
busybox-1.36.1-wasix.config
busybox-1.36.1-wasix.manifest
package.json
```

Run the build in a clean container:

```text
docker build -f crates/sinomo-sandbox/toolchains/busybox/Containerfile -t sinomo-busybox-wasi crates/sinomo-sandbox/toolchains/busybox
docker run --rm -v "$PWD/crates/sinomo-sandbox/toolchains/busybox/out:/src/sinomo-busybox-wasi/out" sinomo-busybox-wasi
```

Or run it directly on a Linux host with curl, make, gcc, and tar available:

```text
crates/sinomo-sandbox/toolchains/busybox/build-busybox-wasi.sh
```

Build the WASIX variant directly with:

```text
crates/sinomo-sandbox/toolchains/busybox/build-busybox-wasi.sh
```

The generated package manifest is imported with `sinomo sandbox toolchain import-package`. Operators should then create a sandbox, import the package, and run:

```text
sinomo sandbox toolchain verify-busybox <sandbox-id> --allow-unsigned
```

For recipe changes, use the end-to-end smoke verifier instead of trusting the build output alone:

```text
crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh
```

The verifier builds the pinned recipe, imports the generated module into a disposable local toolchain cache, creates a sandbox, and runs `verify-busybox`. A command must not be added to the default applet manifest unless this path passes.

The recipe deliberately uses upstream BusyBox source and the WASI SDK directly. It must not be replaced by a shell shim, command emulator, or BusyBox source patch that hides missing runtime behavior. If an applet does not compile or pass fidelity, treat that as a runtime, VFS, sysroot, or system-library gap before exposing that applet as supported.

The current recipes build a deliberately narrow profile. They register these core applets:

```text
awk cat chmod cp cmp date diff dirname echo env false find grep gzip head ln ls
mkdir mv patch printf pwd rm rmdir sed sha256sum sh sort tail tar test touch true
tr uname uniq wc
```

`sh` is the upstream BusyBox ash applet in the WASIX package. The recipe must not patch ash command execution, pipeline, here-doc, or redirection semantics to hide runtime gaps. The runtime must not substitute a fake shell or a mostly-compatible command.

The exposed command list remains conservative because some BusyBox applets depend on filesystem or process operations that still need runtime hardening. BusyBox build and runtime failures are verifier output for improving the POSIX runtime, VFS, sysroot, or system library. The removed `wasi-compat` headers belonged to the legacy compatibility runtime and should not be moved into the shared WASIX sysroot.

The importer stores one WASI module per applet path in the local cache or SIFS toolchain store. The source of truth for cluster use is still SIFS:

```text
/services/sandbox/toolchains/<toolchain_id>/manifest.sobj
/services/sandbox/toolchains/<toolchain_id>/bin/<tool>.wasm
```

The default applet and fidelity-case plan can be inspected with:

```text
sinomo sandbox toolchain busybox-plan
```

Fidelity cases are intentionally small but real: every applet exposed by the default Sinomo BusyBox manifest has at least one direct invocation case. The suite covers file creation, inspection, stdin filters, path mutations, checksum generation, exit statuses, and cleanup through the imported BusyBox module. The first case removes the verifier work directory, so the suite is intended to be repeatable against the same sandbox instance. An applet should not be treated as available to agents until the imported module passes the relevant fidelity case on the host that will execute it.

Run the current fidelity suite against a sandbox whose manifest pins the BusyBox toolchain with:

```text
sinomo sandbox toolchain verify-busybox <sandbox-id> [--daemon] [--allow-unsigned]
```

Use `--daemon` to run the same fidelity cases against a daemon-backed SIFS sandbox instance.

Cluster-wide sandbox policy is represented by `SandboxClusterPolicyConfig` at:

```text
/config/sandbox/policy
```

The policy object carries allowed mount prefixes, local-owned mount rules, allowed command capabilities, toolchain allowlists, signature requirements, and maximum resource budgets. Runtime entry points should load this object from cluster KV before validating a sandbox invocation.

Operators can inspect or update this cluster policy with:

```text
sinomo sandbox policy show
sinomo sandbox policy set --require-signed-toolchains --allow-mount-prefix /services/sandbox --max-memory-mb 1024
```

Build status:

- The Rust-side toolchain manifest, applet registration, local cache import, SIFS import path, fidelity-case declarations, and local fidelity runner are implemented.
- The exact BusyBox WASIX build inputs and commands are captured by `build-busybox-wasi.sh`, `busybox-wasi.fragment`, and `Containerfile`.
- The WASIX package is a verifier target for the current runtime/sysroot. Build or fidelity failures should be addressed below BusyBox, not by modifying BusyBox semantics.
- Deeper workflow fidelity coverage and additional language/toolchain packages are still pending.
