use crate::internal::*;

use super::support::*;

const POSIXTESTSUITE_UPSTREAM: &str =
    "emscripten-core/posixtestsuite@85555325079ea362fa680bd2209c843cfe47e670";

#[cfg(feature = "wasix-runtime")]
fn run_posixtestsuite_wat(name: &str, module: &str) {
    let wasm = wat::parse_str(module).unwrap_or_else(|error| panic!("{name} WAT: {error}"));
    let outcome = runtime::run_wasix_module(
        &wasm,
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .unwrap_or_else(|error| panic!("{name} runtime: {error}"));

    assert_eq!(
        outcome.response.exit_status,
        0,
        "{name} derived from {POSIXTESTSUITE_UPSTREAM} failed with exit {}; stdout: {}; stderr: {}",
        outcome.response.exit_status,
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn posixtestsuite_clock_gettime_and_getres_subset() {
    // Derived from:
    // - conformance/interfaces/clock_gettime/{1-1,3-1,7-1,8-2}.c
    // - conformance/interfaces/clock_getres/{1-1,3-1,5-1,6-2,7-1,8-1}.c
    // The Rust test uses direct WASIX POSIX-shaped imports, so libc errno checks
    // are represented as negative errno-style return-value checks.
    run_posixtestsuite_wat(
        "clock_gettime/clock_getres subset",
        r#"
        (module
          (import "wasix_32v1" "clock_gettime"
            (func $clock_gettime (param i32 i32) (result i32)))
          (import "wasix_32v1" "clock_getres"
            (func $clock_getres (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 2)

          (func $exit_if_errno (param $rc i32) (param $code i32)
            local.get $rc
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)

          (func $timespec_ge (param $left i32) (param $right i32) (result i32)
            local.get $left
            i64.load
            local.get $right
            i64.load
            i64.gt_s
            if
              i32.const 1
              return
            end
            local.get $left
            i64.load
            local.get $right
            i64.load
            i64.lt_s
            if
              i32.const 0
              return
            end
            local.get $left
            i32.load offset=8
            local.get $right
            i32.load offset=8
            i32.ge_u)

          (func $expect_clock_getres_fills (param $clock i32) (param $ptr i32) (param $code i32)
            local.get $ptr
            i64.const 100000
            i64.store
            local.get $ptr
            i32.const 100000
            i32.store offset=8
            local.get $clock
            local.get $ptr
            call $clock_getres
            local.get $code
            call $exit_if_errno
            local.get $ptr
            i64.load
            i64.const 100000
            i64.eq
            if
              local.get $code
              i32.const 10
              i32.add
              call $proc_exit
            end
            local.get $ptr
            i32.load offset=8
            i32.const 1000000000
            i32.ge_u
            if
              local.get $code
              i32.const 20
              i32.add
              call $proc_exit
            end)

          (func $expect_invalid_clock (param $clock i32) (param $code i32)
            local.get $clock
            i32.const 0
            call $clock_gettime
            i32.const 0
            i32.ge_s
            if
              local.get $code
              call $proc_exit
            end
            local.get $clock
            i32.const 16
            call $clock_getres
            i32.const 0
            i32.ge_s
            if
              local.get $code
              i32.const 1
              i32.add
              call $proc_exit
            end)

          (func (export "_start")
            i32.const 0
            i32.const 32
            call $clock_gettime
            i32.const 10
            call $exit_if_errno
            i32.const 32
            i64.load
            i64.eqz
            if
              i32.const 11
              call $proc_exit
            end
            i32.const 32
            i32.load offset=8
            i32.const 1000000000
            i32.ge_u
            if
              i32.const 12
              call $proc_exit
            end

            i32.const 1
            i32.const 48
            call $clock_gettime
            i32.const 20
            call $exit_if_errno
            i32.const 1
            i32.const 64
            call $clock_gettime
            i32.const 21
            call $exit_if_errno
            i32.const 64
            i32.const 48
            call $timespec_ge
            i32.eqz
            if
              i32.const 22
              call $proc_exit
            end

            i32.const 0
            i32.const 80
            i32.const 30
            call $expect_clock_getres_fills
            i32.const 1
            i32.const 96
            i32.const 31
            call $expect_clock_getres_fills
            i32.const 2
            i32.const 112
            i32.const 32
            call $expect_clock_getres_fills
            i32.const 3
            i32.const 128
            i32.const 33
            call $expect_clock_getres_fills

            i32.const 9999
            i32.const 40
            call $expect_invalid_clock
            i32.const -2147483648
            i32.const 42
            call $expect_invalid_clock
            i32.const 2147483647
            i32.const 44
            call $expect_invalid_clock
            i32.const -1073743192
            i32.const 46
            call $expect_invalid_clock
            i32.const 1073743192
            i32.const 48
            call $expect_invalid_clock
            i32.const -1
            i32.const 50
            call $expect_invalid_clock
            i32.const 17
            i32.const 52
            call $expect_invalid_clock))
        "#,
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn posixtestsuite_nanosleep_and_clock_nanosleep_subset() {
    // Derived from:
    // - conformance/interfaces/nanosleep/{1-1,2-1,5-1,6-1}.c
    // - conformance/interfaces/clock_nanosleep/{1-1,3-1,9-1,10-1,11-1}.c
    run_posixtestsuite_wat(
        "nanosleep/clock_nanosleep subset",
        r#"
        (module
          (import "wasix_32v1" "clock_gettime"
            (func $clock_gettime (param i32 i32) (result i32)))
          (import "wasix_32v1" "nanosleep"
            (func $nanosleep (param i32 i32) (result i32)))
          (import "wasix_32v1" "clock_nanosleep"
            (func $clock_nanosleep (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 2)

          (func $exit_if_errno (param $rc i32) (param $code i32)
            local.get $rc
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)

          (func $set_timespec (param $ptr i32) (param $sec i64) (param $nsec i32)
            local.get $ptr
            local.get $sec
            i64.store
            local.get $ptr
            local.get $nsec
            i32.store offset=8
            local.get $ptr
            i32.const 0
            i32.store offset=12)

          (func $slept_more_than_nanos (param $before i32) (param $after i32) (param $requested i32) (result i32)
            (local $sec_delta i64)
            (local $nsec_delta i32)
            local.get $after
            i64.load
            local.get $before
            i64.load
            i64.sub
            local.set $sec_delta
            local.get $after
            i32.load offset=8
            local.get $before
            i32.load offset=8
            i32.sub
            local.set $nsec_delta
            local.get $nsec_delta
            i32.const 0
            i32.lt_s
            if
              local.get $nsec_delta
              i32.const 1000000000
              i32.add
              local.set $nsec_delta
              local.get $sec_delta
              i64.const 1
              i64.sub
              local.set $sec_delta
            end
            local.get $sec_delta
            i64.const 0
            i64.gt_s
            if
              i32.const 1
              return
            end
            local.get $nsec_delta
            local.get $requested
            i32.gt_s)

          (func $expect_sleep_long_enough (param $requested i32) (param $code i32)
            i32.const 0
            i32.const 32
            call $clock_gettime
            local.get $code
            call $exit_if_errno
            i32.const 64
            i64.const 0
            local.get $requested
            call $set_timespec
            i32.const 64
            i32.const 80
            call $nanosleep
            local.get $code
            i32.const 1
            i32.add
            call $exit_if_errno
            i32.const 0
            i32.const 48
            call $clock_gettime
            local.get $code
            i32.const 2
            i32.add
            call $exit_if_errno
            i32.const 32
            i32.const 48
            local.get $requested
            call $slept_more_than_nanos
            i32.eqz
            if
              local.get $code
              i32.const 3
              i32.add
              call $proc_exit
            end
            i32.const 80
            i64.load
            i64.const 0
            i64.ne
            if
              local.get $code
              i32.const 4
              i32.add
              call $proc_exit
            end
            i32.const 80
            i32.load offset=8
            i32.const 0
            i32.ne
            if
              local.get $code
              i32.const 5
              i32.add
              call $proc_exit
            end)

          (func $expect_nanosleep_invalid_nsec (param $nsec i32) (param $code i32)
            i32.const 96
            i64.const 0
            local.get $nsec
            call $set_timespec
            i32.const 96
            i32.const 0
            call $nanosleep
            i32.const 0
            i32.ge_s
            if
              local.get $code
              call $proc_exit
            end)

          (func (export "_start")
            i32.const 3
            i32.const 10
            call $expect_sleep_long_enough
            i32.const 1000000
            i32.const 20
            call $expect_sleep_long_enough

            i32.const -1
            i32.const 40
            call $expect_nanosleep_invalid_nsec
            i32.const -5
            i32.const 41
            call $expect_nanosleep_invalid_nsec
            i32.const -1000000000
            i32.const 42
            call $expect_nanosleep_invalid_nsec
            i32.const 1000000000
            i32.const 43
            call $expect_nanosleep_invalid_nsec
            i32.const 1000000001
            i32.const 44
            call $expect_nanosleep_invalid_nsec

            i32.const 0
            i32.const 112
            call $clock_gettime
            i32.const 50
            call $exit_if_errno
            i32.const 144
            i64.const 0
            i32.const 3000000
            call $set_timespec
            i32.const 0
            i32.const 0
            i32.const 144
            i32.const 0
            call $clock_nanosleep
            i32.const 51
            call $exit_if_errno
            i32.const 0
            i32.const 128
            call $clock_gettime
            i32.const 52
            call $exit_if_errno
            i32.const 112
            i32.const 128
            i32.const 3000000
            call $slept_more_than_nanos
            i32.eqz
            if
              i32.const 53
              call $proc_exit
            end

            i32.const 0
            i32.const 160
            call $clock_gettime
            i32.const 60
            call $exit_if_errno
            i32.const 176
            i32.const 160
            i64.load
            i64.const 1
            i64.sub
            i32.const 160
            i32.load offset=8
            call $set_timespec
            i32.const 0
            i32.const 1
            i32.const 176
            i32.const 0
            call $clock_nanosleep
            i32.const 61
            call $exit_if_errno

            i32.const 9999
            i32.const 0
            i32.const 144
            i32.const 0
            call $clock_nanosleep
            i32.const 0
            i32.ge_s
            if
              i32.const 70
              call $proc_exit
            end
            i32.const 0
            i32.const 2
            i32.const 144
            i32.const 0
            call $clock_nanosleep
            i32.const 0
            i32.ge_s
            if
              i32.const 71
              call $proc_exit
            end
            i32.const 144
            i64.const 0
            i32.const 1000000000
            call $set_timespec
            i32.const 0
            i32.const 0
            i32.const 144
            i32.const 0
            call $clock_nanosleep
            i32.const 0
            i32.ge_s
            if
              i32.const 72
              call $proc_exit
            end))
        "#,
    );
}
