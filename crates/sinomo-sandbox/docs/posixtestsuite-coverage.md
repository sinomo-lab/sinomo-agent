# POSIX Test Suite Coverage

This document tracks Sinomo WASIX coverage derived from [`emscripten-core/posixtestsuite`](https://github.com/emscripten-core/posixtestsuite). The current reference commit is `85555325079ea362fa680bd2209c843cfe47e670`.

The current in-tree tests do not vendor the GPL test sources. They encode selected upstream assertions as WASIX WAT modules that call the same POSIX-shaped WASIX imports the C library lowers to. The source-level verifier fetches the pinned upstream commit into `target/`, compiles selected C tests with the maintained WASIX Clang/sysroot package, and runs the resulting modules through `sinomo sandbox run --wasm`.

## Harness Status

| Area | Status | Evidence |
| --- | --- | --- |
| Upstream source reference | Started | `emscripten-core/posixtestsuite@85555325079ea362fa680bd2209c843cfe47e670` is recorded in `src/tests/posixtestsuite_tests.rs`. |
| In-tree test module | Started | `crates/sinomo-sandbox/src/tests/posixtestsuite_tests.rs` runs selected WASIX WAT tests derived from upstream assertions. |
| Native C compile/run harness | Started | `crates/sinomo-sandbox/toolchains/build-tools/verify-posixtestsuite-wasix.sh` fetches the pinned suite, stages C and shell sources into a disposable sandbox, compiles C tests with sandboxed Clang, runs generated WASM outputs, and records all phases to a TSV results file. |
| Full-suite enumeration | Started | The pinned upstream tree currently contains 2,095 C tests and 188 shell tests under `conformance/`, `functional/`, and `stress/`; the verifier supports `POSIXTESTSUITE_SCOPE=all`, `POSIXTESTSUITE_SCOPE=path:<prefix>`, `POSIXTESTSUITE_LIMIT=<n>`, and `POSIXTESTSUITE_OFFSET=<n>` for chunked runs. |
| Coverage results | Partial | Targeted WAT tests pass locally with `cargo test -p sinomo-sandbox --features wasix-runtime posixtestsuite_ -- --nocapture`; the 10-test source-level smoke subset passes locally with `POSIXTESTSUITE_SCOPE=smoke crates/sinomo-sandbox/toolchains/build-tools/verify-posixtestsuite-wasix.sh`. A full-suite run has not completed yet and must complete before this goal can be considered done. |

## Source-Level C Results

These cases are compiled from upstream C sources by the WASIX Clang package and then run as generated WASM modules.

| Upstream path | Assertion represented | Harness | Result | Notes |
| --- | --- | --- | --- | --- |
| `conformance/interfaces/clock_gettime/1-1.c` | `clock_gettime(CLOCK_REALTIME, &tp)` succeeds and fills a non-empty `timespec`. | `verify-posixtestsuite-wasix.sh` | Pass | Exercises C headers, libc call path, linking, and runtime execution. |
| `conformance/interfaces/clock_gettime/7-1.c` | Invalid clock id returns `-1` and sets `errno` to `EINVAL`. | `verify-posixtestsuite-wasix.sh` | Pass | Confirms libc errno translation for this failure path. |
| `conformance/interfaces/clock_gettime/8-1.c` | Invalid clock id returns failure without relying on stale `errno`. | `verify-posixtestsuite-wasix.sh` | Pass | Covers source-level invalid clock handling. |
| `conformance/interfaces/clock_gettime/8-2.c` | Boundary and unassigned invalid clock ids fail with `EINVAL`. | `verify-posixtestsuite-wasix.sh` | Pass | Covers multiple invalid IDs through the C library. |
| `conformance/interfaces/clock_getres/1-1.c` | `clock_getres(CLOCK_REALTIME, &res)` succeeds and fills `res`. | `verify-posixtestsuite-wasix.sh` | Pass | Exercises source-level `struct timespec` layout. |
| `conformance/interfaces/clock_getres/5-1.c` | Invalid clock id returns `-1` and sets `errno` to `EINVAL`. | `verify-posixtestsuite-wasix.sh` | Pass | Confirms libc errno translation for `clock_getres`. |
| `conformance/interfaces/clock_getres/6-1.c` | Invalid clock id returns failure without relying on stale `errno`. | `verify-posixtestsuite-wasix.sh` | Pass | Covers source-level invalid clock handling. |
| `conformance/interfaces/clock_getres/6-2.c` | Boundary and unassigned invalid clock ids fail with `EINVAL`. | `verify-posixtestsuite-wasix.sh` | Pass | Covers multiple invalid IDs through the C library. |
| `conformance/interfaces/nanosleep/5-1.c` | Negative `tv_sec` fails with `EINVAL`. | `verify-posixtestsuite-wasix.sh` | Pass | Exercises libc `nanosleep` failure behavior. |
| `conformance/interfaces/nanosleep/6-1.c` | Invalid `tv_nsec` values fail with `EINVAL`. | `verify-posixtestsuite-wasix.sh` | Pass | Covers the upstream invalid nanosecond table. |

## Full-Suite Runner Status

The verifier now discovers all upstream C and shell tests instead of relying only on a hard-coded subset. Its current behavior is:

- C sources under `conformance/`, `functional/`, and `stress/` are staged into the sandbox.
- Sources with `main()` are compiled and linked by sandboxed Clang, copied out as WASM, and run through `sinomo sandbox run --wasm`.
- Compile-only sources without `main()` are wrapped with a generated `main()` so header/type/prototype coverage still exercises the sandboxed compiler and linker, then their run phase is recorded as `SKIP`.
- Shell tests are staged and run through sandboxed `sh` when BusyBox and Bash package manifests are available; otherwise they are recorded as `UNSUPPORTED`.
- POSIX test result codes are classified as `PASS`, `FAIL`, `UNRESOLVED`, `UNSUPPORTED`, `UNTESTED`, or `INTERRUPTED` instead of collapsing every nonzero exit into a generic failure.

Verified runner checks so far:

| Scope | Command | Result | Notes |
| --- | --- | --- | --- |
| 10-test smoke subset | `POSIXTESTSUITE_SCOPE=smoke crates/sinomo-sandbox/toolchains/build-tools/verify-posixtestsuite-wasix.sh` | Pass | 10 compile, 10 link, and 10 run phases passed. |
| `conformance/definitions` chunk | `POSIXTESTSUITE_SCOPE=path:conformance/definitions POSIXTESTSUITE_RUN_SHELL=0 POSIXTESTSUITE_LIMIT=60 crates/sinomo-sandbox/toolchains/build-tools/verify-posixtestsuite-wasix.sh` | Interrupted after harness validation | Confirmed compile-only wrapper tests are recorded with run `SKIP`; POSIX `PTS_UNTESTED` exits are recorded as `UNTESTED`. This was not a full-suite result. |

## WAT Runtime Results

| Upstream path | Assertion represented | Sinomo test | Result | Notes |
| --- | --- | --- | --- | --- |
| `conformance/interfaces/clock_gettime/1-1.c` | `clock_gettime(CLOCK_REALTIME, &tp)` succeeds and fills a non-empty `timespec`. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Direct WASIX import checks return code and `tv_sec != 0`; libc `errno` is not involved in this direct ABI test. |
| `conformance/interfaces/clock_gettime/3-1.c` | `CLOCK_MONOTONIC` is supported and non-decreasing. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Uses two immediate monotonic reads instead of the upstream long sleeps to keep unit tests fast. |
| `conformance/interfaces/clock_gettime/7-1.c` | Invalid clock id returns failure. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Direct ABI expects a negative errno-style return. |
| `conformance/interfaces/clock_gettime/8-2.c` | Boundary and unassigned invalid clock ids fail. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Covers `INT32_MIN`, `INT32_MAX`, `-1073743192`, `1073743192`, `-1`, and `17`. |
| `conformance/interfaces/clock_getres/1-1.c` | `clock_getres(CLOCK_REALTIME, &res)` succeeds and fills `res`. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Checks `tv_sec` changed from sentinel and `tv_nsec < 1_000_000_000`. |
| `conformance/interfaces/clock_getres/3-1.c` | `CLOCK_MONOTONIC` resolution is supported and filled. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Same direct ABI check as realtime. |
| `conformance/interfaces/clock_getres/5-1.c` | Invalid clock id returns failure. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Direct ABI expects negative errno-style return. |
| `conformance/interfaces/clock_getres/6-2.c` | Boundary and unassigned invalid clock ids fail. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Uses the same invalid-id set as the `clock_gettime` check. |
| `conformance/interfaces/clock_getres/7-1.c` | `CLOCK_PROCESS_CPUTIME_ID` resolution is supported and filled. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Covers the runtime import path; source-level `sysconf(_SC_CPUTIME)` behavior is not covered yet. |
| `conformance/interfaces/clock_getres/8-1.c` | `CLOCK_THREAD_CPUTIME_ID` resolution is supported and filled. | `posixtestsuite_clock_gettime_and_getres_subset` | Pass | Covers the runtime import path; source-level `sysconf(_SC_THREAD_CPUTIME)` behavior is not covered yet. |
| `conformance/interfaces/nanosleep/1-1.c` | `nanosleep()` succeeds for a short interval and elapsed time advances. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Uses a 3 ns request as upstream does. |
| `conformance/interfaces/nanosleep/2-1.c` | Multiple short `nanosleep()` intervals suspend for at least the requested interval. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Partial pass | Covers representative `3 ns` and `1 ms` intervals; the full upstream interval table is not run yet. |
| `conformance/interfaces/nanosleep/5-1.c` | Invalid negative nanoseconds fail. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Direct ABI expects negative errno-style return. |
| `conformance/interfaces/nanosleep/6-1.c` | Nanoseconds below zero or at least `1_000_000_000` fail with `EINVAL`. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Direct ABI covers failure shape for the invalid nanosecond values; libc `errno` storage is not covered yet. |
| `conformance/interfaces/clock_nanosleep/1-1.c` | Relative `clock_nanosleep()` sleeps for at least the requested interval. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Uses the upstream 3 ms request. |
| `conformance/interfaces/clock_nanosleep/3-1.c` | Absolute `clock_nanosleep()` in the past returns immediately and succeeds. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Checks success; bounded no-sleep timing should be added once timing flakiness controls exist. |
| `conformance/interfaces/clock_nanosleep/9-1.c` | Invalid clock id fails. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Direct ABI expects negative errno-style return. |
| `conformance/interfaces/clock_nanosleep/10-1.c` | Invalid flags fail. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Direct ABI expects negative errno-style return. |
| `conformance/interfaces/clock_nanosleep/11-1.c` | Invalid `tv_nsec` fails. | `posixtestsuite_nanosleep_and_clock_nanosleep_subset` | Pass | Direct ABI expects negative errno-style return. |

## Implementation Issues Found

| Issue | Fix |
| --- | --- |
| The WASIX import table exposed `chdir` through the legacy `proc_chdir(path, len)` ABI, so modules importing POSIX-shaped `chdir(path)` failed at instantiation. | `wasmer-wasix/src/lib.rs` now maps `chdir` to the existing POSIX-shaped `chdir` wrapper while keeping `proc_chdir` on the explicit-length ABI. |
| The existing WASIX compatibility import test declared `arch_prctl` with an `i64` address on the 32-bit WASIX import namespace, which did not match the runtime's pointer-sized `Memory32` ABI. | The test now imports and calls `arch_prctl` with an `i32` address for the 32-bit WASIX namespace. |
| The source-level verifier staged compiled outputs under `/work/posixtestsuite/out` without creating that sandbox directory first, so the linker could not open the output path. | The verifier now creates `/work/posixtestsuite/out` through the sandbox write path before compiling the selected C sources. |

## Not Covered Yet

The following upstream areas are intentionally not counted as covered yet:

- Broader source-level libc behavior beyond the selected clock and nanosleep C cases, including `sysconf`, feature macros outside these headers, and larger suites.
- Upstream tests that require `fork()`, `wait()`, signal handlers, process groups, shared mappings, pthreads, semaphores, message queues, and timers beyond the direct runtime imports represented above.
- Shell-script tests and the upstream `run_tests` harness.
- Stress and functional suites.
- Full source-level build of the entire upstream suite using the maintained WASIX Clang/sysroot.
- A completed `POSIXTESTSUITE_SCOPE=all` run over all 2,095 C tests and 188 shell tests.

## Next Coverage Targets

1. Complete a `POSIXTESTSUITE_SCOPE=all` run, or an equivalent set of chunked runs covering every discovered C and shell test exactly once.
2. Summarize full-suite results by top-level suite, phase, and POSIX result code in this document.
3. Expand implementation fixes from the first failing groups with actionable diagnostics, starting with process/thread/message-queue areas that now surface through the full-suite runner.
4. Grow toward the upstream `run_tests` harness only after individual source-level groups are stable enough to make failures actionable.
