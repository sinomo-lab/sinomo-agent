# Sinomo Python Sandbox Toolchain

Python and pip are required coding-sandbox commands. They must be real commands
inside the sandbox, not host fallbacks, wrappers, or reduced substitutes.

The target is a CPython-based package with:

- command `python`,
- command `pip`,
- a complete standard library artifact layout,
- durable site-packages stored through SIFS,
- package installation using real pip behavior,
- network access routed through Sinomo-mediated sandbox networking and policy,
- no ambient host filesystem or socket access.

## Availability Gate

The active CPython recipe for coding-sandbox readiness is:

```text
crates/sinomo-sandbox/toolchains/python/build-cpython-wasix.sh
```

It pins CPython, zlib, wasi-sdk/WASIX inputs, stages the standard library and
generated sysconfig data as package artifacts, unpacks the bundled pip wheel
into `Lib/site-packages`, and emits a generic `import-package` manifest with
command `python` plus command `pip` lowered to `python -m pip`.

The older `build-cpython-wasi.sh` recipe remains useful for Preview1 regression
coverage and pure-Python pip behavior, but it is not the production
native-extension path because WASIp1 cannot provide the dynamic loading needed by
ordinary compiled Python wheels.

Inspect the maintained target from the CLI with:

```text
sinomo sandbox toolchain python-plan
```

The Preview1 pure-Python regression gate is:

```text
crates/sinomo-sandbox/toolchains/python/verify-python-wasi.sh
```

It builds the pinned CPython WASIp1 package by default, validates the generated
package manifest, imports it into a disposable local data directory, runs
`sinomo sandbox toolchain verify-python`, checkpoints the sandbox, resumes it,
and verifies the installed package still imports from `/work/site-packages`.
The maintained pip workflow builds a wheel into `/work/pip-cache`, installs
from that cache, installs the same package through a local PEP 503-style
`file:///work/simple` simple index, and the post-resume check verifies that
wheel files are still present there.
In daemon-local mode, the verifier also starts a local HTTP package source,
fetches PEP 503-style simple-index pages and pure-Python wheels through
`sinomo sandbox fetch --daemon` under the cluster HTTP allowlist, installs with
real pip from `file:///work/simple`, verifies newest-compatible dependency
selection, PEP 503 package-name normalization, environment marker exclusion,
requires-python link filtering, constraints-file pinning, hash-checked
requirements with mismatch denial, yanked release filtering, extras, transitive
dependencies, and imports the installed packages. Set
`VERIFY_MEDIATED_PIP_INDEX=1` to force that mediated dependency-acquisition
check for another daemon-backed backend.
The active WASIX gate is successful installation/import/execution for common
scientific packages such as `numpy` and `matplotlib`. In the current manual
gate, `pip install --find-links` installs wheels into the sandbox workspace,
`numpy` imports and runs representative array operations, and `matplotlib`
imports with the Agg backend and renders a figure file inside the sandbox.
Run that gate with:

```text
crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh
```

The script imports BusyBox WASIX, Bash WASIX, and CPython WASIX packages into a
disposable local sandbox, seeds a WASIX wheelhouse into `/work/wheelhouse`,
verifies direct Python subprocess execution plus `shell=True` through the
pinned `/bin/sh`, installs `numpy` and `matplotlib` with real pip, imports both
packages from `/work/native-site`, runs a NumPy array operation, and verifies
that matplotlib renders a PNG under `/work`.
Set `VERIFY_BACKEND=daemon-local` to run the same native-extension gate through
a disposable single-node daemon and SIFS-backed sandbox state.
Set `SKIP_BUILD=1` with `WORKDIR` and `OUTDIR` when reusing an already-built
package during local iteration.

By default the gate uses local staging. Set `VERIFY_BACKEND=daemon` to run the
same flow against the current daemon-backed SIFS state, or
`VERIFY_BACKEND=daemon-local` to create a disposable single-node daemon and SIFS
root for an isolated end-to-end check:

```text
VERIFY_BACKEND=daemon crates/sinomo-sandbox/toolchains/python/verify-python-wasi.sh
VERIFY_BACKEND=daemon-local crates/sinomo-sandbox/toolchains/python/verify-python-wasi.sh
```

Daemon mode imports the package through `sinomo sandbox toolchain import-package
--daemon`, creates a SIFS-backed sandbox, runs `verify-python --daemon`,
checkpoints and resumes through the daemon, and verifies the installed package
from `/work/site-packages` after resume. It uses the cluster sandbox policy; set
`UPDATE_CLUSTER_POLICY=1` only in a development cluster where the verifier may
allow the unsigned Python package, the Python toolchain id, the package-cache
prefix, and the `preview1.pinned-subprocess` capability.

Daemon package import stores the large Python support tree as a SIFS artifact
bundle while retaining per-file artifact metadata and hashes in the toolchain
manifest. Runtime staging expands the bundle into the local execution cache, so
the verifier exercises SIFS persistence without creating thousands of DFS
objects for a single language runtime import.

Minimum `python` verifier cases:

```text
python -c 'print(1 + 1)'
python -c 'import json; print(json.dumps({"ok": True}, sort_keys=True))'
python /work/script.py
python -m compileall /work
```

Minimum `pip` verifier cases:

```text
python -m pip --version
pip --version
pip install --target /work/site-packages <pure-python-wheel>
pip install --index-url file:///work/simple --target /work/index-site-packages <pure-python-package>
PYTHONPATH=/work/site-packages python -c 'import <installed_package>'
```

For full pip support, the verifier must keep expanding resolver coverage
against allowlisted package indexes through the mediated sandbox network path.
The current daemon-local gate covers newest-compatible dependency selection,
PEP 503 package-name normalization, environment marker exclusion,
requires-python link filtering, constraints-file pinning, hash-checked
requirements with mismatch denial, yanked release filtering, extras, transitive
dependencies, wheel cache/state stored under SIFS, and repeated installation
after sandbox resume.
Native-extension package support is the current production-readiness gate. In
particular, `numpy` must import and run representative array operations, and
`matplotlib` must render a non-interactive figure to a file inside the sandbox.

## Current Status

`build-cpython-wasix.sh` produces a real CPython WASIX package. The interpreter
probe passes for basic Python execution, `zlib` import, standard-library
artifact staging, and the packaged pip entrypoint can report `pip --version`.
The package runs on the WASIX runtime so Python and pip can use the same process
and dynamic-loading substrate as the shell/build-tool path.

The `verify-python` gate now passes for the maintained offline pure-Python
package workflow, including `pip install --target /work/site-packages`
from a locally-built wheel cache, `pip install --index-url file:///work/simple`
from a local PEP 503-style simple index backed by that cache, and importing the
installed packages from `/work/site-packages` and `/work/index-site-packages`.
The pip wheel cache is stored under `/work/pip-cache` and verified after
checkpoint/resume. The Python package recipe sanitizes CPython build sysconfig
so setuptools asks for sandbox paths such as `/bin/clang` and
`/include/python3.13`, and the Sinomo subprocess hook now routes non-Python
child commands through the pinned sandbox child-process bridge instead of
falling back to host-path subprocess behavior. Native-extension package support
has moved to the WASIX runtime. The dynamic linker mirrors side-module callback
tables into the main module table and applies a generic Binaryen function-pointer
cast compatibility transform to dynamically loaded WASIX side modules, which
allows unmodified `numpy` and `matplotlib` wheels to import and execute inside
the sandbox. When the main module requires that fpcast-compatible ABI, the
runtime now fails side-module loading early if the bundled Binaryen `wasm-opt`
transformer is missing, instead of silently skipping the transform and trapping
later during native-extension execution. The daemon-local gate
additionally verifies mediated package-index acquisition by fetching
PEP 503-style project pages plus top-level and dependency wheels through the
Sinomo sandbox HTTP path into `/work/simple` and `/work/packages`, then
installing with real pip from `file:///work/simple` and importing it from
`/work/mediated-site-packages`. It covers newest-compatible dependency
selection, PEP 503 package-name normalization, environment marker exclusion,
requires-python link filtering, constraints-file pinning, `--require-hashes`
requirements including a hash-mismatch denial case, yanked release filtering,
extras, and transitive dependency resolution through the mediated index. The
script can run the core gate locally or through the
daemon/SIFS path, and both modes verify the installed package
after checkpoint/resume.

Full pip ecosystem support is still not complete. The current priority is to
keep hardening the WASIX POSIX/process substrate for Python workloads, with
`python`, `pip`, `numpy`, and `matplotlib` as the active acceptance gate, before
expanding broader resolver coverage.
