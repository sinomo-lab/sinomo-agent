# Sinomo BusyBox Sandbox Toolchain

This directory is the Sinomo-owned home for the first sandbox userspace target: a real BusyBox build for WASI preview1 and WASIX.

The sandbox runtime does not emulate shell commands and does not expose fake subsets. A BusyBox toolchain import must point at a real BusyBox WASI module. Sinomo then registers that module as the pinned applet implementation for the core command names declared by `BUSYBOX_TOOLCHAIN_COMMANDS`.

Current import command:

```text
sinomo sandbox toolchain import-busybox --version <version> [--provenance <text>] <busybox.wasm>
```

## Reproducible Build Recipe

Sinomo maintains the first BusyBox recipe in this directory:

```text
Containerfile
build-busybox-wasi.sh
busybox-wasi.fragment
```

The recipe pins upstream BusyBox 1.36.1 and wasi-sdk 24.0, verifies downloads by SHA-256 before extraction, applies the Sinomo applet fragment on top of BusyBox `allnoconfig`, and builds a WASI preview1 module with the pinned WASI SDK clang by default. Set `BUSYBOX_TARGET_ABI=wasix` to build the WASIX package variant; that path also pins Binaryen 124 and wasix-libc `v2026-05-12.1`, uses the WASIX sysroot, and applies Binaryen asyncify plus function-pointer cast emulation so the applet module satisfies the current Wasmer/WASIX runner shape. The output directory contains:

```text
busybox-1.36.1-wasip1.wasm
busybox-1.36.1-wasip1.config
busybox-1.36.1-wasip1.manifest
```

For WASIX builds the output uses the `wasix` suffix and also includes a generic toolchain package manifest:

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
BUSYBOX_TARGET_ABI=wasix crates/sinomo-sandbox/toolchains/busybox/build-busybox-wasi.sh
```

The generated WASIp1 manifest prints the exact `sinomo sandbox toolchain import-busybox` command with provenance including the BusyBox version, wasi-sdk version, and resulting module hash. The WASIX package manifest is imported with `sinomo sandbox toolchain import-package`. Operators should then create a sandbox, import the module/package, and run:

```text
sinomo sandbox toolchain verify-busybox <sandbox-id> --allow-unsigned
```

For recipe changes, use the end-to-end smoke verifier instead of trusting the build output alone:

```text
crates/sinomo-sandbox/toolchains/busybox/verify-busybox-wasi.sh
```

The verifier builds the pinned recipe, imports the generated module into a disposable local toolchain cache, creates a sandbox, and runs `verify-busybox`. A command must not be added to the default applet manifest unless this path passes.

The recipe deliberately uses upstream BusyBox source and the WASI SDK directly. It must not be replaced by a shell shim or a command emulator. If an applet does not compile or pass fidelity against WASI, remove that applet from the exposed fragment until a real BusyBox implementation works.

The current recipes build a deliberately narrow profile. They register these core applets:

```text
awk cat chmod cp cmp date diff dirname echo env false find grep gzip head ln ls
mkdir mv patch printf pwd rm rmdir sed sha256sum sort tail tar test touch true
tr uname uniq wc
```

`sh` is not exposed in the current default WASIp1 profile. BusyBox ash requires descriptor and process behavior that the runtime does not yet provide, and the recipe must not patch ash command execution, pipeline, here-doc, or redirection semantics to hide that runtime gap. The runtime must not substitute a fake shell or a mostly-compatible command.

The current recipe avoids wasi-sdk SJLJ/legacy-exception output with local WASI compatibility headers, applies WASI-specific `pwd` and `xmkstemp` portability patches so BusyBox validates Sinomo's injected sandbox `PWD` and can create temporary files for `patch`, patches descriptor moves to use WASI `fd_renumber` so file-mode `gzip` can open input/output paths without relying on unsupported `dup2`, wires BusyBox `chmod`/permission checks to Sinomo WASI extensions that apply and report real mode bits for explicit read-write preopens, and keeps unsupported external process features blocked on the fuller fd/process substrate. BusyBox `awk` is enabled for real in-process awk programs; its external command features fail with `ENOTSUP` until Sinomo has real POSIX process support. The recipe also forces a direct static applet entry path. The exposed command list remains conservative because some BusyBox applets depend on unsupported WASI preview1 filesystem or process operations under the current runtime.

The WASIX variant keeps the same exposed applet list and exists primarily so real Bash can launch compatible external commands such as `/bin/grep` and `/bin/cat` through the WASIX process substrate. It adds compatibility shims for libc surfaces that are absent from the current WASIX sysroot (`major`, `minor`, `mknod`, `chroot`, and `settimeofday`). `getgroups`, process-group/session calls, terminal foreground-process-group calls, `fchdir`, path/fd `chmod`, path/fd no-op ownership changes for the sandbox identity, and the path/fd mode-bit side of `stat` are routed to Sinomo's WASIX runtime imports so the WASIX applets apply and report real sandbox process and host-backed filesystem state. Unsupported operations return explicit unsupported or permission errors instead of pretending they succeeded.

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
- The exact BusyBox WASIp1 and WASIX build inputs and commands are captured by `build-busybox-wasi.sh`, `busybox-wasi.fragment`, and `Containerfile`.
- The WASIX package has been validated locally with `verify-package`, direct `grep` execution, and `crates/sinomo-sandbox/toolchains/verify-bash-wasix.sh` after importing Bash into the same WASIX BusyBox toolchain.
- Deeper workflow fidelity coverage and additional language/toolchain packages are still pending.
