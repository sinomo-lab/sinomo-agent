use crate::internal::*;
#[cfg(test)]
use crate::{
    SINOMO_WASI_ERRNO_ROFS, sandbox_runtime_disables_ambient_network,
    sandbox_wasi_extension_read_host_path,
};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::support::*;

#[test]
fn local_wasi_runner_captures_stdout_and_chunks() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_stdout_module(),
            request.clone(),
            SandboxWasiRunOptions::default(),
        )
        .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    assert!(outcome.stderr.is_empty());
    assert_eq!(outcome.response.resource_usage.stdout_bytes, 6);
    assert_eq!(
        outcome.response.stdout_ref,
        sandbox_command_stdout_path(&request.sandbox_id, &outcome.response.command_id)
    );
    assert_eq!(outcome.output_chunks.len(), 1);
    assert_eq!(outcome.output_chunks[0].stream, SandboxOutputStream::Stdout);
    assert_eq!(outcome.output_chunks[0].bytes, b"hello\n");
}

#[test]
fn local_wasi_subprocess_resolves_child_from_pinned_bin() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_spawn_echo_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_child_executable_obeys_memory_budget() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-large-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-large-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(bin_dir.join("echo"), vec![0u8; 64 * 1024 + 1]).expect("large child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_with_stderr_file_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                memory_limit_bytes: 64 * 1024,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    let stderr = fs::read_to_string(work_dir.join("err.txt")).expect("child stderr");
    assert!(stderr.contains("exceeded memory limit"));
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_inherits_parent_env_when_envp_is_null() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-inherit-env-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("envdump"),
        wat::parse_str(wasi_environ_to_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let mut request = test_wasi_request(1_000);
    request
        .env
        .insert("PARENT_ENV".to_string(), "inherited".to_string());

    let outcome = runner
        .run_module(
            wasi_spawn_envdump_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
    let stdout = String::from_utf8_lossy(&outcome.stdout);
    assert!(stdout.contains("PARENT_ENV=inherited"));
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_runner_rejects_invalid_request_env_name() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let mut request = test_wasi_request(1_000);
    request
        .env
        .insert("BAD-NAME".to_string(), "value".to_string());

    let error = runner
        .run_module(
            wasi_stdout_module(),
            request,
            SandboxWasiRunOptions::default(),
        )
        .expect_err("invalid env name should be rejected before execution");

    assert!(matches!(
        error,
        SandboxWasiError::InvalidRequest { ref detail }
            if detail.contains("invalid sandbox env name `BAD-NAME`")
    ));
}

#[test]
fn local_wasi_runner_rejects_malformed_request_argv_and_env() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let mut empty_argv0 = test_wasi_request(1_000);
    empty_argv0.argv[0].clear();

    let error = runner
        .run_module(
            wasi_stdout_module(),
            empty_argv0,
            SandboxWasiRunOptions::default(),
        )
        .expect_err("empty argv0 should be rejected before execution");
    assert!(matches!(
        error,
        SandboxWasiError::InvalidRequest { ref detail }
            if detail.contains("argv[0] must be non-empty")
    ));

    let mut nul_argv = test_wasi_request(1_000);
    nul_argv.argv.push("bad\0arg".to_string());
    let error = runner
        .run_module(
            wasi_stdout_module(),
            nul_argv,
            SandboxWasiRunOptions::default(),
        )
        .expect_err("argv NUL should be rejected before execution");
    assert!(matches!(
        error,
        SandboxWasiError::InvalidRequest { ref detail }
            if detail.contains("argv[1] contains an embedded NUL byte")
    ));

    let mut nul_env = test_wasi_request(1_000);
    nul_env
        .env
        .insert("GOOD_ENV".to_string(), "bad\0value".to_string());
    let error = runner
        .run_module(
            wasi_stdout_module(),
            nul_env,
            SandboxWasiRunOptions::default(),
        )
        .expect_err("env NUL should be rejected before execution");
    assert!(matches!(
        error,
        SandboxWasiError::InvalidRequest { ref detail }
            if detail.contains("environment variable `GOOD_ENV` contains an embedded NUL byte")
    ));
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_rejects_invalid_request_env_name() {
    let mut request = test_wasi_request(1_000);
    request
        .env
        .insert("BAD-NAME".to_string(), "value".to_string());

    let wasm = wat::parse_str(
        r#"
            (module
              (memory (export "memory") 1 2)
              (func (export "_start")))
            "#,
    )
    .expect("wasix noop");

    let error = runtime::run_wasix_module(&wasm, request, SandboxWasiRunOptions::default())
        .expect_err("invalid env name should be rejected before WASIX starts");

    assert!(matches!(
        error,
        SandboxWasiError::InvalidRequest { ref detail }
            if detail.contains("invalid sandbox env name `BAD-NAME`")
    ));
}

#[test]
fn local_wasi_subprocess_child_timeout_reports_timeout() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-timeout-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("loop"),
        wat::parse_str(looping_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(10);

    let error = runner
        .run_module(
            wasi_spawn_loop_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                fuel: u64::MAX,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("child timeout");

    assert!(matches!(error, SandboxWasiError::TimedOut { .. }));
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_child_cancellation_reports_cancelled() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-cancel-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("loop"),
        wat::parse_str(looping_module()).expect("child wasm"),
    )
    .expect("child command");
    let cancellation = SandboxCancellation::new();
    let cancellation_for_thread = cancellation.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        cancellation_for_thread.cancel();
    });

    let error = runner
        .run_module(
            wasi_spawn_loop_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                cancellation,
                fuel: u64::MAX,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("child cancellation");

    canceller.join().expect("canceller");
    assert!(matches!(error, SandboxWasiError::Cancelled));
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_children_share_fuel_budget() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-fuel-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("burn"),
        wat::parse_str(wasi_finite_burn_module(2_000)).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_burn_twice_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                fuel: 8_500,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(
        outcome.response.exit_status, 0,
        "child processes must draw from the parent command fuel budget instead of receiving a fresh full budget per spawn"
    );
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_requires_explicit_runtime_capability() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-capability-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_spawn_echo_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_invalid_child_env_name() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-env-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_spawn_echo_with_invalid_env_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_malformed_child_env_entry() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-malformed-env-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_spawn_echo_with_malformed_env_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_child_outside_pinned_bin() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-work");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        work_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_spawn_work_echo_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: work_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("work path"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_subprocess_rejects_child_executable_symlink() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::write(
        bin_dir.join("echo.real"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    std::os::unix::fs::symlink(bin_dir.join("echo.real"), bin_dir.join("echo"))
        .expect("child executable symlink");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_missing_explicit_child_cwd() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-missing-cwd-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-missing-cwd-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_at_cwd_preview1_module("/work/missing"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_oversized_explicit_child_cwd() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-large-cwd-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-large-cwd-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let oversized_cwd = format!("/work/{}", "a".repeat(4097));

    let outcome = runner
        .run_module(
            wasi_spawn_echo_at_cwd_preview1_module(&oversized_cwd),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_file_explicit_child_cwd() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-file-cwd-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-file-cwd-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(work_dir.join("not-a-dir"), b"file").expect("cwd file");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_at_cwd_preview1_module("/work/not-a-dir"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_subprocess_rejects_symlink_escape_explicit_child_cwd() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-cwd-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-cwd-work");
    let outside_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-cwd-outside");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::create_dir_all(&outside_dir).expect("outside dir");
    std::os::unix::fs::symlink(&outside_dir, work_dir.join("outside")).expect("cwd symlink");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_at_cwd_preview1_module("/work/outside"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
    fs::remove_dir_all(outside_dir).ok();
}

#[test]
fn local_wasi_subprocess_writes_spawn_error_relative_to_child_cwd() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-error-cwd-work");
    fs::create_dir_all(work_dir.join("subdir")).expect("work subdir");

    let outcome = runner
        .run_module(
            wasi_spawn_outside_bin_with_relative_child_stderr_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: work_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("work path"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(
        work_dir.join("subdir").join("err.txt").exists(),
        "spawn failure stderr should resolve relative to the requested child cwd"
    );
    assert!(
        !work_dir.join("err.txt").exists(),
        "spawn failure stderr must not resolve relative to the parent cwd"
    );
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_ambiguous_stderr_redirection() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-ambiguous-stderr-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-ambiguous-stderr-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_with_ambiguous_stderr_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(!work_dir.join("out.txt").exists());
    assert!(!work_dir.join("err.txt").exists());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_oversized_guest_control_string() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-control-string-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-control-string-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_with_oversized_stdout_path_len_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    assert!(!work_dir.join("out.txt").exists());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_oversized_argv_cstring() {
    let runner = LocalWasiCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module(
            wasi_spawn_oversized_argv0_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
}

#[test]
fn local_wasi_subprocess_redirected_stdout_obeys_output_limit() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdout-limit-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdout-limit-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let error = runner
        .run_module(
            wasi_spawn_echo_to_out_file_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                output_limit_bytes: 4,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("child redirected stdout output limit");

    assert!(matches!(
        error,
        SandboxWasiError::OutputLimitExceeded { .. }
    ));
    assert!(!work_dir.join("out.txt").exists());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_directory_stdout_redirection() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdout-directory-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdout-directory-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(work_dir.join("out.txt")).expect("stdout directory");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_echo_to_out_file_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    assert!(work_dir.join("out.txt").is_dir());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_preflights_stdout_redirection_before_child_side_effects() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdout-preflight-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdout-preflight-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(work_dir.join("out.txt")).expect("stdout directory");
    fs::write(
        bin_dir.join("writer"),
        wat::parse_str(wasi_write_file_module("side.txt")).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_writer_to_out_file_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(
        !work_dir.join("side.txt").exists(),
        "invalid child stdout redirection should be rejected before the child command starts"
    );
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_preflights_stderr_redirection_before_child_side_effects() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stderr-preflight-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stderr-preflight-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(work_dir.join("err.txt")).expect("stderr directory");
    fs::write(
        bin_dir.join("writer"),
        wat::parse_str(wasi_write_file_module("side.txt")).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_writer_to_err_file_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(
        !work_dir.join("side.txt").exists(),
        "invalid child stderr redirection should be rejected before the child command starts"
    );
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_reads_file_backed_stdin() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(work_dir.join("in.txt"), b"from file\n").expect("stdin file");
    fs::write(
        bin_dir.join("cat"),
        wat::parse_str(wasi_stdin_to_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_cat_with_file_stdin_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                input_limit_bytes: 64,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"from file\n");
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_file_stdin_obeys_input_limit() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-limit-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-limit-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(work_dir.join("in.txt"), b"too much stdin").expect("stdin file");
    fs::write(
        bin_dir.join("cat"),
        wat::parse_str(wasi_stdin_to_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let error = runner
        .run_module(
            wasi_spawn_cat_with_file_stdin_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                input_limit_bytes: 4,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("child file stdin input limit");

    assert!(matches!(error, SandboxWasiError::InputLimitExceeded { .. }));
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_subprocess_rejects_directory_file_stdin() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-directory-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-directory-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(work_dir.join("in.txt")).expect("stdin directory");
    fs::write(
        bin_dir.join("cat"),
        wat::parse_str(wasi_stdin_to_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_cat_with_file_stdin_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_subprocess_rejects_file_stdin_symlink_inside_preopen() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-inner-symlink-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-inner-symlink-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(work_dir.join("real.txt"), b"from symlink\n").expect("real stdin file");
    std::os::unix::fs::symlink(work_dir.join("real.txt"), work_dir.join("in.txt"))
        .expect("stdin symlink");
    fs::write(
        bin_dir.join("cat"),
        wat::parse_str(wasi_stdin_to_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_cat_with_file_stdin_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_subprocess_rejects_file_stdin_symlink_escape() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-symlink-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-symlink-work");
    let outside_dir = unique_test_dir("sinomo-sandbox-preview1-child-stdin-symlink-outside");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::create_dir_all(&outside_dir).expect("outside dir");
    fs::write(outside_dir.join("secret.txt"), b"secret").expect("outside file");
    std::os::unix::fs::symlink(outside_dir.join("secret.txt"), work_dir.join("in.txt"))
        .expect("stdin symlink");
    fs::write(
        bin_dir.join("cat"),
        wat::parse_str(wasi_stdin_to_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runner
        .run_module(
            wasi_spawn_cat_with_file_stdin_preview1_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
    fs::remove_dir_all(outside_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_subprocess_rejects_stdout_symlink_escape() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let bin_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-work");
    let outside_dir = unique_test_dir("sinomo-sandbox-preview1-child-symlink-outside");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::create_dir_all(&outside_dir).expect("outside dir");
    let outside_file = outside_dir.join("stdout.txt");
    fs::write(&outside_file, b"outside").expect("outside file");
    std::os::unix::fs::symlink(&outside_file, work_dir.join("link")).expect("stdout symlink");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");
    let request = test_wasi_request(1_000);

    let outcome = runner
        .run_module(
            wasi_spawn_echo_to_work_link_preview1_module(),
            request,
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: bin_dir.clone(),
                        guest_path: SandboxPath::new("/bin").expect("bin path"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work path"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                ],
                allow_preview1_pinned_subprocesses: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert_eq!(fs::read(&outside_file).expect("outside file"), b"outside");
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
    fs::remove_dir_all(outside_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_captures_stdout_and_chunks() {
    let request = test_wasi_request(1_000);
    let streamed_chunks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let streamed_chunks_for_sink = streamed_chunks.clone();
    let outcome = runtime::run_wasix_module(
        wasi_stdout_module().as_bytes(),
        request.clone(),
        SandboxWasiRunOptions::default().with_output_sink(Some(Arc::new(
            move |chunk: SandboxOutputChunk| {
                streamed_chunks_for_sink
                    .lock()
                    .expect("streamed chunks")
                    .push(chunk);
            },
        ))),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    assert!(outcome.stderr.is_empty());
    assert_eq!(
        outcome.response.stdout_ref,
        sandbox_command_stdout_path(&request.sandbox_id, &outcome.response.command_id)
    );
    assert_eq!(outcome.output_chunks.len(), 1);
    assert_eq!(outcome.output_chunks[0].stream, SandboxOutputStream::Stdout);
    assert_eq!(outcome.output_chunks[0].bytes, b"hello\n");
    assert_eq!(
        *streamed_chunks.lock().expect("streamed chunks"),
        outcome.output_chunks
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_allows_child_exec_from_pinned_bin() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-child-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-child-bin-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("child command");

    let outcome = runtime::run_wasix_module(
        wasix_spawn_absolute_module("/bin/echo").as_bytes(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            preopened_dirs: vec![
                SandboxWasiPreopen {
                    host_path: bin_dir.clone(),
                    guest_path: SandboxPath::new("/bin").expect("bin path"),
                    mode: SandboxMountMode::ReadOnly,
                },
                SandboxWasiPreopen {
                    host_path: work_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("work path"),
                    mode: SandboxMountMode::ReadWrite,
                },
            ],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_rejects_child_exec_from_writable_mount() {
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-child-work");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        work_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("work child command");

    let outcome = runtime::run_wasix_module(
        wasix_spawn_absolute_module("/work/echo").as_bytes(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: work_dir.clone(),
                guest_path: SandboxPath::new("/work").expect("work path"),
                mode: SandboxMountMode::ReadWrite,
            }],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run parent");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_allows_unpinned_child_exec_with_instruction_metering() {
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-child-work-metered");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        work_dir.join("echo"),
        wat::parse_str(wasi_stdout_module()).expect("child wasm"),
    )
    .expect("work child command");

    let outcome = runtime::run_wasix_module(
        wasix_spawn_absolute_module("/work/echo").as_bytes(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: work_dir.clone(),
                guest_path: SandboxPath::new("/work").expect("work path"),
                mode: SandboxMountMode::ReadWrite,
            }],
            wasix_pinned_child_exec_only: false,
            wasix_instruction_metering: true,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_enforces_input_limit() {
    let mut request = test_wasi_request(1_000);
    request.stdin = Some(b"too much stdin".to_vec());

    let error = runtime::run_wasix_module(
        wasi_stdin_to_stdout_module().as_bytes(),
        request,
        SandboxWasiRunOptions {
            input_limit_bytes: 4,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("WASIX input limit");

    assert!(matches!(error, SandboxWasiError::InputLimitExceeded { .. }));
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_enforces_output_limit() {
    let error = runtime::run_wasix_module(
        wasi_stdout_module().as_bytes(),
        test_wasi_request(1_000),
        SandboxWasiRunOptions {
            output_limit_bytes: 4,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("WASIX output limit");

    assert!(matches!(
        error,
        SandboxWasiError::OutputLimitExceeded { .. }
    ));
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_rejects_write_to_read_only_preopen() {
    let host_dir = unique_test_dir("sinomo-sandbox-wasix-readonly");
    fs::create_dir_all(&host_dir).expect("host dir");
    let request = test_wasi_request(1_000);

    let outcome = runtime::run_wasix_module(
        wasi_write_file_module("out.txt").as_bytes(),
        request,
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: host_dir.clone(),
                guest_path: SandboxPath::new("/work").expect("guest"),
                mode: SandboxMountMode::ReadOnly,
            }],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("readonly run");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(!host_dir.join("out.txt").exists());
    fs::remove_dir_all(host_dir).ok();
}

#[cfg(not(feature = "wasix-runtime"))]
#[test]
fn wasix_runner_reports_missing_feature_when_disabled() {
    let error = runtime::run_wasix_module(
        wasi_stdout_module().as_bytes(),
        test_wasi_request(1_000),
        SandboxWasiRunOptions::default(),
    )
    .expect_err("wasix feature missing");
    assert!(error.to_string().contains("wasix-runtime"));
}

#[test]
fn local_wasi_runner_streams_output_to_sink() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let request = test_wasi_request(1_000);
    let chunks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let chunks_for_sink = chunks.clone();

    let outcome = runner
        .run_module(
            wasi_stdout_module(),
            request.clone(),
            SandboxWasiRunOptions {
                output_sink: Some(Arc::new(move |chunk: SandboxOutputChunk| {
                    chunks_for_sink.lock().expect("chunks").push(chunk);
                })),
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run");

    let chunks = chunks.lock().expect("chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].sandbox_id, request.sandbox_id);
    assert_eq!(chunks[0].command_id, outcome.response.command_id);
    assert_eq!(chunks[0].stream, SandboxOutputStream::Stdout);
    assert_eq!(chunks[0].offset, 0);
    assert_eq!(chunks[0].bytes, b"hello\n");
}

#[test]
fn local_wasi_runner_passes_stdin_to_module() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let mut request = test_wasi_request(1_000);
    request.stdin = Some(b"from stdin\n".to_vec());

    let outcome = runner
        .run_module(
            wasi_stdin_to_stdout_module(),
            request,
            SandboxWasiRunOptions::default(),
        )
        .expect("run");

    assert_eq!(outcome.stdout, b"from stdin\n");
}

#[test]
fn local_wasi_runner_reports_proc_exit_status() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module(
            wasi_exit_module(7),
            test_wasi_request(1_000),
            SandboxWasiRunOptions::default(),
        )
        .expect("run");

    assert_eq!(outcome.response.exit_status, 7);
}

#[test]
fn local_wasi_runner_times_out_looping_module() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let error = runner
        .run_module(
            looping_module(),
            test_wasi_request(10),
            SandboxWasiRunOptions {
                fuel: u64::MAX,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("timeout");

    assert!(matches!(error, SandboxWasiError::TimedOut { .. }));
}

#[test]
fn local_wasi_runner_obeys_cancellation_token() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let cancellation = SandboxCancellation::new();
    cancellation.cancel();

    let error = runner
        .run_module(
            looping_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                cancellation,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("cancelled");

    assert!(matches!(error, SandboxWasiError::Cancelled));
}

#[test]
fn local_wasi_runner_enforces_output_limit() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let error = runner
        .run_module(
            wasi_stdout_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                output_limit_bytes: 4,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("output limit");

    assert!(matches!(
        error,
        SandboxWasiError::OutputLimitExceeded { .. }
    ));
}

#[test]
fn local_wasi_runner_enforces_input_limit() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let mut request = test_wasi_request(1_000);
    request.stdin = Some(b"too much stdin".to_vec());

    let error = runner
        .run_module(
            wasi_stdin_to_stdout_module(),
            request,
            SandboxWasiRunOptions {
                input_limit_bytes: 4,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("input limit");

    assert!(matches!(error, SandboxWasiError::InputLimitExceeded { .. }));
}

#[test]
fn local_wasi_runner_enforces_memory_limit() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let error = runner
        .run_module(
            memory_hungry_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                memory_limit_bytes: 64 * 1024,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("memory limit");

    assert!(matches!(
        error,
        SandboxWasiError::MemoryLimitExceeded { .. }
    ));
}

#[test]
fn local_wasi_runner_has_no_host_preopens_by_default() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module(
            fd3_prestat_exit_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions::default(),
        )
        .expect("run");

    assert_ne!(outcome.response.exit_status, 0);
}

#[test]
fn local_wasi_runner_writes_only_to_explicit_preopened_dir() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let host_dir = unique_test_dir("sinomo-sandbox-wasi-preopen");
    fs::create_dir_all(&host_dir).expect("host dir");
    let sibling_escape = host_dir
        .parent()
        .expect("parent")
        .join("sinomo-sandbox-escape.txt");
    let _ = fs::remove_file(&sibling_escape);

    let outcome = runner
        .run_module(
            wasi_write_file_module("out.txt"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: host_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("guest"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(
        fs::read(host_dir.join("out.txt")).expect("written"),
        b"created"
    );
    assert!(!sibling_escape.exists());
    fs::remove_dir_all(&host_dir).ok();
}

#[test]
fn local_wasi_runner_rejects_preopen_path_escape() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let host_dir = unique_test_dir("sinomo-sandbox-wasi-escape");
    fs::create_dir_all(&host_dir).expect("host dir");
    let sibling_escape = host_dir
        .parent()
        .expect("parent")
        .join("sinomo-sandbox-escape.txt");
    let _ = fs::remove_file(&sibling_escape);

    let outcome = runner
        .run_module(
            wasi_write_file_module("../sinomo-sandbox-escape.txt"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: host_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("guest"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(!sibling_escape.exists());
    fs::remove_dir_all(&host_dir).ok();
}

#[test]
fn local_wasi_runner_rejects_write_to_read_only_preopen() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let host_dir = unique_test_dir("sinomo-sandbox-wasi-readonly");
    fs::create_dir_all(&host_dir).expect("host dir");

    let outcome = runner
        .run_module(
            wasi_write_file_module("out.txt"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: host_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("guest"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run");

    assert_ne!(outcome.response.exit_status, 0);
    assert!(!host_dir.join("out.txt").exists());
    fs::remove_dir_all(&host_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_runner_chmods_only_explicit_read_write_preopen() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let host_dir = unique_test_dir("sinomo-sandbox-wasi-chmod");
    fs::create_dir_all(&host_dir).expect("host dir");
    let file = host_dir.join("script.sh");
    fs::write(&file, b"#!/bin/sh\n").expect("file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).expect("mode");

    let outcome = runner
        .run_module(
            wasi_chmod_module("/work/script.sh", 0o755),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: host_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("work"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("chmod");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(
        fs::metadata(&file).expect("metadata").permissions().mode() & 0o777,
        0o755
    );

    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).expect("reset mode");
    let outcome = runner
        .run_module(
            wasi_chmod_module("/work/script.sh", 0o755),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: host_dir.clone(),
                    guest_path: SandboxPath::new("/work").expect("work"),
                    mode: SandboxMountMode::ReadOnly,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("readonly chmod denial");

    assert_eq!(outcome.response.exit_status, SINOMO_WASI_ERRNO_ROFS);
    assert_eq!(
        fs::metadata(&file).expect("metadata").permissions().mode() & 0o777,
        0o644
    );
    fs::remove_dir_all(host_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_extension_resolves_paths_under_root_preopen() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let host_dir = unique_test_dir("sinomo-sandbox-wasi-root-preopen");
    fs::create_dir_all(host_dir.join("work")).expect("work dir");
    let file = host_dir.join("work").join("script.sh");
    fs::write(&file, b"#!/bin/sh\n").expect("file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).expect("mode");

    let outcome = runner
        .run_module(
            wasi_chmod_module("/work/script.sh", 0o755),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: host_dir.clone(),
                    guest_path: SandboxPath::new("/").expect("root"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("chmod through root preopen");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(
        fs::metadata(&file).expect("metadata").permissions().mode() & 0o777,
        0o755
    );
    fs::remove_dir_all(host_dir).ok();
}

#[cfg(unix)]
#[test]
fn local_wasi_extension_prefers_specific_preopen_over_root() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let root_dir = unique_test_dir("sinomo-sandbox-wasi-root-preopen-specific-root");
    let work_dir = unique_test_dir("sinomo-sandbox-wasi-root-preopen-specific-work");
    fs::create_dir_all(root_dir.join("work")).expect("root work dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    let file = work_dir.join("script.sh");
    fs::write(&file, b"#!/bin/sh\n").expect("file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).expect("mode");

    let outcome = runner
        .run_module(
            wasi_chmod_module("/work/script.sh", 0o755),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![
                    SandboxWasiPreopen {
                        host_path: root_dir.clone(),
                        guest_path: SandboxPath::new("/").expect("root"),
                        mode: SandboxMountMode::ReadWrite,
                    },
                    SandboxWasiPreopen {
                        host_path: work_dir.clone(),
                        guest_path: SandboxPath::new("/work").expect("work"),
                        mode: SandboxMountMode::ReadOnly,
                    },
                ],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("chmod through specific read-only preopen");

    assert_eq!(outcome.response.exit_status, SINOMO_WASI_ERRNO_ROFS);
    assert_eq!(
        fs::metadata(&file).expect("metadata").permissions().mode() & 0o777,
        0o644
    );
    fs::remove_dir_all(root_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[test]
fn local_wasi_runner_exposes_sandbox_virtual_devices_when_enabled() {
    let runner = LocalWasiCommandRunner::new().expect("runner");

    let null_outcome = runner
        .run_module(
            wasi_read_path_to_stdout_module(3, "null", 16),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                enable_virtual_devices: true,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("read null");
    assert_eq!(null_outcome.response.exit_status, 0);
    assert!(null_outcome.stdout.is_empty());

    let zero_outcome = runner
        .run_module(
            wasi_read_path_to_stdout_module(3, "zero", 8),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                enable_virtual_devices: true,
                zero_device_bytes: 64,
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("read zero");
    assert_eq!(zero_outcome.response.exit_status, 0);
    assert_eq!(zero_outcome.stdout, vec![0; 8]);
}

#[test]
fn local_wasi_runner_keeps_virtual_devices_disabled_for_raw_runs() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module(
            wasi_read_path_to_stdout_module(3, "zero", 8),
            test_wasi_request(1_000),
            SandboxWasiRunOptions::default(),
        )
        .expect("run");

    assert_ne!(outcome.response.exit_status, 0);
}

#[cfg(unix)]
#[test]
fn local_wasi_runner_rejects_symlinked_host_device_escape() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let dev_dir = unique_test_dir("sinomo-sandbox-wasi-device-symlink");
    fs::create_dir_all(&dev_dir).expect("dev dir");
    std::os::unix::fs::symlink("/dev/null", dev_dir.join("null")).expect("null symlink");

    let outcome = runner
        .run_module(
            wasi_write_file_module_on_fd(3, "null"),
            test_wasi_request(1_000),
            SandboxWasiRunOptions {
                preopened_dirs: vec![SandboxWasiPreopen {
                    host_path: dev_dir.clone(),
                    guest_path: SandboxPath::new("/dev").expect("guest"),
                    mode: SandboxMountMode::ReadWrite,
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect("run");

    assert_ne!(
        outcome.response.exit_status, 0,
        "WASI /dev support must not be implemented by symlink escape to host devices"
    );
    fs::remove_dir_all(&dev_dir).ok();
}

#[cfg(unix)]
#[test]
fn sandbox_wasi_extension_read_host_path_rejects_symlink_escape() {
    let root = unique_test_dir("sinomo-sandbox-wasi-read-symlink-root");
    let outside = unique_test_dir("sinomo-sandbox-wasi-read-symlink-outside");
    fs::create_dir_all(&root).expect("root");
    fs::create_dir_all(&outside).expect("outside");
    fs::write(outside.join("secret.txt"), b"secret").expect("outside file");
    std::os::unix::fs::symlink(outside.join("secret.txt"), root.join("link")).expect("symlink");

    let error = sandbox_wasi_extension_read_host_path(&root, &root.join("link"))
        .expect_err("symlink escape rejected");
    assert!(error.contains("escapes sandbox preopen"));

    fs::remove_dir_all(&root).ok();
    fs::remove_dir_all(&outside).ok();
}

#[test]
fn local_wasi_runner_denies_raw_socket_syscalls() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module(
            wasi_sock_send_exit_module(),
            test_wasi_request(1_000),
            SandboxWasiRunOptions::default(),
        )
        .expect("run");

    assert_ne!(
        outcome.response.exit_status, 0,
        "raw WASI socket calls must not succeed; sandbox networking must use mediated fetch"
    );
    assert!(sandbox_runtime_disables_ambient_network());
}
