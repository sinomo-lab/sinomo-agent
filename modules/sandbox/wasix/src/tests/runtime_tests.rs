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

#[cfg(feature = "wasix-runtime")]
#[test]
fn local_wasi_runner_rejects_virtual_preopens() {
    let runner = LocalWasiCommandRunner::new().expect("runner");
    let request = test_wasi_request(1_000);

    let error = runner
        .run_module(
            wasi_stdout_module(),
            request,
            SandboxWasiRunOptions {
                virtual_preopened_dirs: vec![SandboxWasiVirtualPreopen {
                    guest_path: SandboxPath::new("/work").expect("work"),
                    mode: SandboxMountMode::ReadWrite,
                    fs: Arc::new(virtual_fs::mem_fs::FileSystem::default()),
                }],
                ..SandboxWasiRunOptions::default()
            },
        )
        .expect_err("Preview1 must not silently ignore virtual preopens");

    assert!(matches!(error, SandboxWasiError::InvalidRequest { .. }));
    assert!(
        error.to_string().contains("use WASIX"),
        "unexpected error: {error}"
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn virtual_mem_fs_seek_beyond_end_then_write_creates_sparse_gap() {
    virtual_mio::block_on(async {
        use virtual_fs::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, FileSystem as _};

        let fs = virtual_fs::mem_fs::FileSystem::default();
        let mut file = fs
            .new_open_options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(std::path::Path::new("/sparse.txt"))
            .expect("create sparse file");

        file.write_all(b"hi").await.expect("write prefix");
        assert_eq!(
            file.seek(std::io::SeekFrom::Start(5))
                .await
                .expect("seek past end"),
            5
        );
        file.write_all(b"!").await.expect("write sparse suffix");
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .expect("rewind");
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.expect("read sparse");
        assert_eq!(bytes, b"hi\0\0\0!");
    });
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn virtual_mem_fs_read_after_seek_beyond_end_returns_eof() {
    virtual_mio::block_on(async {
        use virtual_fs::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, FileSystem as _};

        let fs = virtual_fs::mem_fs::FileSystem::default();
        let mut file = fs
            .new_open_options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(std::path::Path::new("/eof.txt"))
            .expect("create eof file");

        file.write_all(b"hi").await.expect("write prefix");
        assert_eq!(
            file.seek(std::io::SeekFrom::Start(5))
                .await
                .expect("seek past end"),
            5
        );
        let mut bytes = Vec::new();
        let read = file.read_to_end(&mut bytes).await.expect("read eof");
        assert_eq!(read, 0);
        assert!(bytes.is_empty());
    });
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
fn wasix_runner_exposes_core_process_and_thread_compat_imports() {
    let module = r#"
        (module
          (import "wasix_32v1" "getpid"
            (func $getpid (result i32)))
          (import "wasix_32v1" "getppid"
            (func $getppid (result i32)))
          (import "wasix_32v1" "getpgrp"
            (func $getpgrp (result i32)))
          (import "wasix_32v1" "getpgid"
            (func $getpgid (param i32) (result i32)))
          (import "wasix_32v1" "getsid"
            (func $getsid (param i32) (result i32)))
          (import "wasix_32v1" "tcgetpgrp"
            (func $tcgetpgrp (param i32) (result i32)))
          (import "wasix_32v1" "tcgetsid"
            (func $tcgetsid (param i32) (result i32)))
          (import "wasix_32v1" "tcsetpgrp"
            (func $tcsetpgrp (param i32 i32) (result i32)))
          (import "wasix_32v1" "kill"
            (func $kill (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_signal"
            (func $proc_signal (param i32 i32) (result i32)))
          (import "wasix_32v1" "raise"
            (func $raise (param i32) (result i32)))
          (import "wasix_32v1" "proc_raise"
            (func $proc_raise (param i32) (result i32)))
          (import "wasix_32v1" "sigaltstack_set"
            (func $sigaltstack_set (param i64 i32 i64 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "sigaltstack_get"
            (func $sigaltstack_get (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "signal_mask_set"
            (func $signal_mask_set (param i64 i64) (result i32)))
          (import "wasix_32v1" "signal_mask_get"
            (func $signal_mask_get (param i32 i32) (result i32)))
          (import "wasix_32v1" "unshare"
            (func $unshare (param i64) (result i32)))
          (import "wasix_32v1" "setns"
            (func $setns (param i32 i32) (result i32)))
          (import "wasix_32v1" "waitpid"
            (func $waitpid (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "waitid"
            (func $waitid (param i32 i32 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "fd_pipe"
            (func $fd_pipe (param i32 i32) (result i32)))
          (import "wasix_32v1" "pipe"
            (func $pipe (param i32) (result i32)))
          (import "wasix_32v1" "pipe2"
            (func $pipe2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "eventfd"
            (func $eventfd (param i32 i32) (result i32)))
          (import "wasix_32v1" "timerfd_create"
            (func $timerfd_create (param i32 i32) (result i32)))
          (import "wasix_32v1" "timerfd_settime"
            (func $timerfd_settime (param i32 i32 i64 i64 i64 i64 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "timerfd_gettime"
            (func $timerfd_gettime (param i32 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "timerfd_ready"
            (func $timerfd_ready (param i32) (result i32)))
          (import "wasix_32v1" "timerfd_read"
            (func $timerfd_read (param i32) (result i64)))
          (import "wasix_32v1" "timerfd_remaining_ms"
            (func $timerfd_remaining_ms (param i32) (result i32)))
          (import "wasix_32v1" "timerfd_known"
            (func $timerfd_known (param i32) (result i32)))
          (import "wasix_32v1" "posix_timer_create"
            (func $posix_timer_create (param i32) (result i32)))
          (import "wasix_32v1" "posix_timer_settime"
            (func $posix_timer_settime (param i32 i32 i64 i64 i64 i64 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "posix_timer_gettime"
            (func $posix_timer_gettime (param i32 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "posix_timer_getoverrun"
            (func $posix_timer_getoverrun (param i32) (result i32)))
          (import "wasix_32v1" "posix_timer_delete"
            (func $posix_timer_delete (param i32) (result i32)))
          (import "wasix_32v1" "itimer_settime"
            (func $itimer_settime (param i32 i64 i64 i64 i64 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "itimer_gettime"
            (func $itimer_gettime (param i32 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "signalfd"
            (func $signalfd (param i32 i64 i64 i32) (result i32)))
          (import "wasix_32v1" "signalfd_known"
            (func $signalfd_known (param i32) (result i32)))
          (import "wasix_32v1" "signalfd_mask_low"
            (func $signalfd_mask_low (param i32) (result i64)))
          (import "wasix_32v1" "pidfd_open"
            (func $pidfd_open (param i32 i32) (result i32)))
          (import "wasix_32v1" "pidfd_status"
            (func $pidfd_status (param i32) (result i32)))
          (import "wasix_32v1" "pidfd_pid"
            (func $pidfd_pid (param i32) (result i32)))
          (import "wasix_32v1" "pidfd_getfd"
            (func $pidfd_getfd (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "dup"
            (func $dup (param i32) (result i32)))
          (import "wasix_32v1" "dup2"
            (func $dup2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "dup3"
            (func $dup3 (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "chdir"
            (func $chdir (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_chdir"
            (func $proc_chdir (param i32 i32) (result i32)))
          (import "wasix_32v1" "fchdir"
            (func $fchdir (param i32) (result i32)))
          (import "wasix_32v1" "proc_fchdir"
            (func $proc_fchdir (param i32) (result i32)))
          (import "wasix_32v1" "fd_close"
            (func $fd_close (param i32) (result i32)))
          (import "wasix_32v1" "thread_local_create"
            (func $thread_local_create (param i64 i32) (result i32)))
          (import "wasix_32v1" "thread_local_set"
            (func $thread_local_set (param i32 i64) (result i32)))
          (import "wasix_32v1" "thread_local_get"
            (func $thread_local_get (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_local_destroy"
            (func $thread_local_destroy (param i32) (result i32)))
          (import "wasix_32v1" "thread_sleep"
            (func $thread_sleep (param i64) (result i32)))
          (import "wasix_32v1" "sleep"
            (func $sleep (param i32) (result i32)))
          (import "wasix_32v1" "usleep"
            (func $usleep (param i64) (result i32)))
          (import "wasix_32v1" "nanosleep"
            (func $nanosleep (param i32 i32) (result i32)))
          (import "wasix_32v1" "clock_nanosleep"
            (func $clock_nanosleep (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "clock_gettime"
            (func $clock_gettime (param i32 i32) (result i32)))
          (import "wasix_32v1" "clock_getres"
            (func $clock_getres (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_id"
            (func $thread_id (param i32) (result i32)))
          (import "wasix_32v1" "gettid"
            (func $gettid (result i32)))
          (import "wasix_32v1" "set_tid_address"
            (func $set_tid_address (param i64) (result i32)))
          (import "wasix_32v1" "get_clear_child_tid"
            (func $get_clear_child_tid (param i32) (result i32)))
          (import "wasix_32v1" "set_robust_list"
            (func $set_robust_list (param i64 i64) (result i32)))
          (import "wasix_32v1" "get_robust_list"
            (func $get_robust_list (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "getrlimit"
            (func $getrlimit (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "setrlimit"
            (func $setrlimit (param i32 i64 i64) (result i32)))
          (import "wasix_32v1" "prlimit"
            (func $prlimit (param i32 i32 i32 i64 i64 i32 i32) (result i32)))
          (import "wasix_32v1" "getrusage"
            (func $getrusage (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "getpriority"
            (func $getpriority (param i32 i32) (result i32)))
          (import "wasix_32v1" "setpriority"
            (func $setpriority (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "nice"
            (func $nice (param i32) (result i32)))
          (import "wasix_32v1" "sched_getscheduler"
            (func $sched_getscheduler (param i32) (result i32)))
          (import "wasix_32v1" "sched_getparam"
            (func $sched_getparam (param i32 i32) (result i32)))
          (import "wasix_32v1" "sched_setscheduler"
            (func $sched_setscheduler (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "sched_setparam"
            (func $sched_setparam (param i32 i32) (result i32)))
          (import "wasix_32v1" "sched_yield"
            (func $sched_yield (result i32)))
          (import "wasix_32v1" "sched_get_priority_max"
            (func $sched_get_priority_max (param i32) (result i32)))
          (import "wasix_32v1" "sched_get_priority_min"
            (func $sched_get_priority_min (param i32) (result i32)))
          (import "wasix_32v1" "sched_rr_get_interval"
            (func $sched_rr_get_interval (param i32 i32) (result i32)))
          (import "wasix_32v1" "sched_getaffinity"
            (func $sched_getaffinity (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "sched_setaffinity"
            (func $sched_setaffinity (param i32 i64 i64) (result i32)))
          (import "wasix_32v1" "sched_getcpu"
            (func $sched_getcpu (result i32)))
          (import "wasix_32v1" "tkill"
            (func $tkill (param i32 i32) (result i32)))
          (import "wasix_32v1" "tgkill"
            (func $tgkill (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "arch_prctl"
            (func $arch_prctl (param i32 i32) (result i32)))
          (import "wasix_32v1" "membarrier"
            (func $membarrier (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "rseq"
            (func $rseq (param i64 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 160) "/\00")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start") (local $dup_fd i32) (local $fd i32) (local $tmp i32)
            i32.const 32
            call $getpid
            i32.store
            i32.const 32
            i32.load
            i32.eqz
            if
              i32.const 91
              call $proc_exit
            end

            i32.const 36
            call $getppid
            i32.store

            call $getpgrp
            i32.const 32
            i32.load
            i32.ne
            if
              i32.const 93
              call $proc_exit
            end

            i32.const 0
            call $getpgid
            i32.const 32
            i32.load
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end

            i32.const 0
            call $getsid
            i32.const 32
            i32.load
            i32.ne
            if
              i32.const 95
              call $proc_exit
            end

            i32.const 1
            call $tcgetpgrp
            i32.const 32
            i32.load
            i32.ne
            if
              i32.const 97
              call $proc_exit
            end

            i32.const 1
            call $tcgetsid
            i32.const 32
            i32.load
            i32.ne
            if
              i32.const 98
              call $proc_exit
            end

            i32.const 1
            i32.const 32
            i32.load
            call $tcsetpgrp
            call $exit_if_errno

            i32.const 0
            i32.const 0
            call $kill
            call $exit_if_errno

            i32.const 0
            call $raise
            call $exit_if_errno

            i32.const 9999
            i32.const 0
            call $kill
            i32.const 0
            i32.ge_s
            if
              i32.const 106
              call $proc_exit
            end

            i32.const 9999
            i32.const 0
            call $proc_signal
            i32.eqz
            if
              i32.const 107
              call $proc_exit
            end

            i32.const 0
            call $proc_raise
            call $exit_if_errno

            i32.const 128
            i32.const 136
            i32.const 144
            call $sigaltstack_get
            call $exit_if_errno
            i32.const 136
            i32.load
            i32.const 2
            i32.ne
            if
              i32.const 108
              call $proc_exit
            end
            i64.const 1024
            i32.const 0
            i64.const 8192
            i32.const 128
            i32.const 136
            i32.const 144
            call $sigaltstack_set
            call $exit_if_errno
            i32.const 128
            i32.const 136
            i32.const 144
            call $sigaltstack_get
            call $exit_if_errno
            i32.const 128
            i64.load
            i64.const 1024
            i64.ne
            if
              i32.const 109
              call $proc_exit
            end
            i32.const 136
            i32.load
            i32.const 0
            i32.ne
            if
              i32.const 110
              call $proc_exit
            end
            i64.const 0
            i32.const 2
            i64.const 0
            i32.const 128
            i32.const 136
            i32.const 144
            call $sigaltstack_set
            call $exit_if_errno
            i64.const 32
            i64.const 0
            call $signal_mask_set
            call $exit_if_errno
            i32.const 128
            i32.const 136
            call $signal_mask_get
            call $exit_if_errno
            i32.const 128
            i64.load
            i64.const 32
            i64.ne
            if
              i32.const 116
              call $proc_exit
            end
            i64.const 0
            i64.const 0
            call $signal_mask_set
            call $exit_if_errno

            i32.const -1
            i32.const 76
            i32.const 1
            call $waitpid
            i32.const 0
            i32.ge_s
            if
              i32.const 99
              call $proc_exit
            end

            i32.const -1
            i32.const 5
            i32.const 72
            i32.const 76
            i32.const 80
            i32.const 84
            call $waitid
            i32.const 0
            i32.ge_s
            if
              i32.const 111
              call $proc_exit
            end

            call $getpid
            i32.const 0
            call $pidfd_open
            local.tee $fd
            i32.const 0
            i32.lt_s
            if
              i32.const 112
              call $proc_exit
            end
            local.get $fd
            call $pidfd_status
            i32.const 0
            i32.ne
            if
              i32.const 113
              call $proc_exit
            end
            local.get $fd
            call $pidfd_pid
            call $getpid
            i32.ne
            if
              i32.const 114
              call $proc_exit
            end
            local.get $fd
            i32.const 1
            i32.const 0
            call $pidfd_getfd
            local.tee $tmp
            i32.const 0
            i32.lt_s
            if
              i32.const 115
              call $proc_exit
            end
            local.get $tmp
            call $fd_close
            call $exit_if_errno
            local.get $fd
            call $fd_close
            call $exit_if_errno

            i32.const 1
            i32.const 0
            call $timerfd_create
            local.tee $fd
            i32.const 0
            i32.lt_s
            if
              i32.const 116
              call $proc_exit
            end
            local.get $fd
            call $timerfd_known
            i32.const 1
            i32.ne
            if
              i32.const 117
              call $proc_exit
            end
            local.get $fd
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $timerfd_gettime
            call $exit_if_errno
            local.get $fd
            i32.const 0
            i64.const 0
            i64.const 1000000
            i64.const 0
            i64.const 0
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $timerfd_settime
            call $exit_if_errno
            local.get $fd
            call $timerfd_remaining_ms
            i32.const 0
            i32.lt_s
            if
              i32.const 118
              call $proc_exit
            end
            local.get $fd
            call $timerfd_ready
            i32.const 0
            i32.lt_s
            if
              i32.const 119
              call $proc_exit
            end
            i64.const 2000000
            call $thread_sleep
            call $exit_if_errno
            local.get $fd
            call $timerfd_read
            i64.const 0
            i64.le_s
            if
              i32.const 120
              call $proc_exit
            end
            local.get $fd
            call $fd_close
            call $exit_if_errno

            i32.const 1
            call $posix_timer_create
            local.tee $fd
            i32.const 0
            i32.lt_s
            if
              i32.const 121
              call $proc_exit
            end
            local.get $fd
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $posix_timer_gettime
            call $exit_if_errno
            local.get $fd
            i32.const 0
            i64.const 0
            i64.const 1000000
            i64.const 0
            i64.const 1000000
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $posix_timer_settime
            call $exit_if_errno
            local.get $fd
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $posix_timer_gettime
            call $exit_if_errno
            i64.const 2000000
            call $thread_sleep
            call $exit_if_errno
            local.get $fd
            call $posix_timer_getoverrun
            i32.const 0
            i32.lt_s
            if
              i32.const 122
              call $proc_exit
            end
            local.get $fd
            call $posix_timer_delete
            call $exit_if_errno

            i32.const 0
            i64.const 10
            i64.const 0
            i64.const 0
            i64.const 0
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $itimer_settime
            call $exit_if_errno
            i32.const 0
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $itimer_gettime
            call $exit_if_errno
            i32.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i32.const 128
            i32.const 136
            i32.const 144
            i32.const 152
            call $itimer_settime
            call $exit_if_errno

            i32.const -1
            i64.const 4
            i64.const 0
            i32.const 0
            call $signalfd
            local.tee $fd
            i32.const 0
            i32.lt_s
            if
              i32.const 123
              call $proc_exit
            end
            local.get $fd
            call $signalfd_known
            i32.const 1
            i32.ne
            if
              i32.const 124
              call $proc_exit
            end
            local.get $fd
            call $signalfd_mask_low
            i64.const 4
            i64.ne
            if
              i32.const 125
              call $proc_exit
            end
            local.get $fd
            i64.const 8
            i64.const 0
            i32.const 0
            call $signalfd
            local.get $fd
            i32.ne
            if
              i32.const 126
              call $proc_exit
            end
            local.get $fd
            call $signalfd_mask_low
            i64.const 8
            i64.ne
            if
              i32.const 127
              call $proc_exit
            end
            local.get $fd
            call $fd_close
            call $exit_if_errno

            i32.const 124
            call $thread_id
            call $exit_if_errno
            call $gettid
            i32.const 124
            i32.load
            i32.ne
            if
              i32.const 100
              call $proc_exit
            end
            i64.const 180
            call $set_tid_address
            i32.const 124
            i32.load
            i32.ne
            if
              i32.const 110
              call $proc_exit
            end
            i32.const 176
            call $get_clear_child_tid
            call $exit_if_errno
            i32.const 176
            i32.load
            i32.const 180
            i32.ne
            if
              i32.const 111
              call $proc_exit
            end
            i64.const 192
            i64.const 24
            call $set_robust_list
            call $exit_if_errno
            i32.const 0
            i32.const 184
            i32.const 188
            call $get_robust_list
            call $exit_if_errno
            i32.const 184
            i32.load
            i32.const 192
            i32.ne
            if
              i32.const 112
              call $proc_exit
            end
            i32.const 188
            i32.load
            i32.const 24
            i32.ne
            if
              i32.const 113
              call $proc_exit
            end
            i32.const 9999
            i32.const 184
            i32.const 188
            call $get_robust_list
            i32.const 0
            i32.ge_s
            if
              i32.const 114
              call $proc_exit
            end
            i32.const 4098
            i32.const 305419896
            call $arch_prctl
            call $exit_if_errno
            i32.const 4099
            i32.const 368
            call $arch_prctl
            call $exit_if_errno
            i32.const 368
            i32.load
            i32.const 305419896
            i32.ne
            if
              i32.const 138
              call $proc_exit
            end
            i32.const 57005
            i32.const 0
            call $arch_prctl
            i32.const 0
            i32.ge_s
            if
              i32.const 139
              call $proc_exit
            end
            i32.const 0
            i32.const 0
            i32.const 0
            call $membarrier
            i32.const 0
            i32.le_s
            if
              i32.const 140
              call $proc_exit
            end
            i32.const 1
            i32.const 0
            i32.const 0
            call $membarrier
            call $exit_if_errno
            i32.const 1
            i32.const 1
            i32.const 0
            call $membarrier
            i32.const 0
            i32.ge_s
            if
              i32.const 141
              call $proc_exit
            end
            i64.const 384
            i32.const 32
            i32.const 0
            i32.const 1392848979
            call $rseq
            call $exit_if_errno
            i64.const 384
            i32.const 32
            i32.const 1
            i32.const 1392848979
            call $rseq
            call $exit_if_errno
            i64.const 0
            i32.const 32
            i32.const 0
            i32.const 0
            call $rseq
            i32.const 0
            i32.ge_s
            if
              i32.const 142
              call $proc_exit
            end
            i32.const 7
            i32.const 200
            i32.const 208
            call $getrlimit
            call $exit_if_errno
            i32.const 7
            i32.const 200
            i64.load
            i32.const 208
            i64.load
            call $setrlimit
            call $exit_if_errno
            i32.const 0
            i32.const 7
            i32.const 0
            i64.const 0
            i64.const 0
            i32.const 216
            i32.const 224
            call $prlimit
            call $exit_if_errno
            i32.const 216
            i64.load
            i32.const 200
            i64.load
            i64.ne
            if
              i32.const 115
              call $proc_exit
            end
            i32.const 9999
            i32.const 216
            i32.const 224
            call $getrlimit
            i32.const 0
            i32.ge_s
            if
              i32.const 116
              call $proc_exit
            end
            i32.const 0
            i32.const 232
            i32.const 240
            i32.const 248
            call $getrusage
            call $exit_if_errno
            i32.const 1234
            i32.const 232
            i32.const 240
            i32.const 248
            call $getrusage
            i32.const 0
            i32.ge_s
            if
              i32.const 117
              call $proc_exit
            end
            i32.const 0
            i32.const 0
            call $getpriority
            i32.const 0
            i32.ne
            if
              i32.const 123
              call $proc_exit
            end
            i32.const 0
            i32.const 0
            i32.const 5
            call $setpriority
            call $exit_if_errno
            i32.const 0
            i32.const 0
            call $getpriority
            i32.const 5
            i32.ne
            if
              i32.const 124
              call $proc_exit
            end
            i32.const 1
            call $nice
            i32.const 6
            i32.ne
            if
              i32.const 125
              call $proc_exit
            end
            i32.const 9999
            i32.const 0
            call $getpriority
            i32.const 0
            i32.ge_s
            if
              i32.const 126
              call $proc_exit
            end
            i32.const 0
            call $sched_getscheduler
            i32.const 0
            i32.ne
            if
              i32.const 118
              call $proc_exit
            end
            i32.const 0
            i32.const 256
            call $sched_getparam
            call $exit_if_errno
            i32.const 256
            i32.load
            i32.const 0
            i32.ne
            if
              i32.const 119
              call $proc_exit
            end
            i32.const 0
            i32.const 0
            i32.const 0
            call $sched_setscheduler
            call $exit_if_errno
            i32.const 0
            i32.const 0
            call $sched_setparam
            call $exit_if_errno
            i32.const 0
            i32.const 352
            call $sched_rr_get_interval
            call $exit_if_errno
            i32.const 352
            i64.load
            i64.const 0
            i64.ne
            if
              i32.const 135
              call $proc_exit
            end
            i32.const 360
            i32.load
            i32.const 10000000
            i32.ne
            if
              i32.const 136
              call $proc_exit
            end
            i32.const 9999
            i32.const 352
            call $sched_rr_get_interval
            i32.const 0
            i32.ge_s
            if
              i32.const 137
              call $proc_exit
            end
            i32.const 0
            i64.const 1
            i64.const 0
            call $sched_setaffinity
            call $exit_if_errno
            i32.const 0
            i32.const 264
            i32.const 272
            call $sched_getaffinity
            call $exit_if_errno
            i32.const 264
            i64.load
            i64.const 1
            i64.ne
            if
              i32.const 120
              call $proc_exit
            end
            call $sched_getcpu
            i32.const 0
            i32.lt_s
            if
              i32.const 121
              call $proc_exit
            end
            i32.const 9999
            call $sched_getscheduler
            i32.const 0
            i32.ge_s
            if
              i32.const 122
              call $proc_exit
            end
            i32.const 124
            i32.load
            i32.const 0
            call $tkill
            call $exit_if_errno
            i32.const 32
            i32.load
            i32.const 124
            i32.load
            i32.const 0
            call $tgkill
            call $exit_if_errno
            i32.const 9999
            i32.const 0
            call $tkill
            i32.const 0
            i32.ge_s
            if
              i32.const 108
              call $proc_exit
            end
            i32.const 9999
            i32.const 124
            i32.load
            i32.const 0
            call $tgkill
            i32.const 0
            i32.ge_s
            if
              i32.const 109
              call $proc_exit
            end
            i64.const 0
            call $thread_sleep
            call $exit_if_errno
            i32.const 0
            call $sleep
            i32.const 0
            i32.ne
            if
              i32.const 127
              call $proc_exit
            end
            i64.const 0
            call $usleep
            call $exit_if_errno
            i32.const 288
            i64.const 0
            i64.store
            i32.const 296
            i64.const 0
            i64.store
            i32.const 304
            i64.const 1
            i64.store
            i32.const 312
            i64.const 1
            i64.store
            i32.const 288
            i32.const 304
            call $nanosleep
            call $exit_if_errno
            i32.const 304
            i64.load
            i64.const 0
            i64.ne
            if
              i32.const 128
              call $proc_exit
            end
            i32.const 312
            i64.load
            i64.const 0
            i64.ne
            if
              i32.const 129
              call $proc_exit
            end
            i32.const 1
            i32.const 0
            i32.const 288
            i32.const 304
            call $clock_nanosleep
            call $exit_if_errno
            i32.const 296
            i64.const 1000000000
            i64.store
            i32.const 288
            i32.const 0
            call $nanosleep
            i32.const 0
            i32.ge_s
            if
              i32.const 130
              call $proc_exit
            end
            i32.const 9999
            i32.const 0
            i32.const 288
            i32.const 0
            call $clock_nanosleep
            i32.const 0
            i32.ge_s
            if
              i32.const 131
              call $proc_exit
            end
            i32.const 1
            i32.const 320
            call $clock_gettime
            call $exit_if_errno
            i32.const 328
            i32.load
            i32.const 1000000000
            i32.ge_u
            if
              i32.const 132
              call $proc_exit
            end
            i32.const 1
            i32.const 336
            call $clock_getres
            call $exit_if_errno
            i32.const 344
            i32.load
            i32.const 1000000000
            i32.ge_u
            if
              i32.const 133
              call $proc_exit
            end
            i32.const 9999
            i32.const 320
            call $clock_gettime
            i32.const 0
            i32.ge_s
            if
              i32.const 134
              call $proc_exit
            end

            i64.const 0
            i32.const 40
            call $thread_local_create
            call $exit_if_errno
            i32.const 40
            i32.load
            i64.const 305419896
            call $thread_local_set
            call $exit_if_errno
            i32.const 40
            i32.load
            i32.const 44
            call $thread_local_get
            call $exit_if_errno
            i32.const 44
            i32.load
            i32.const 305419896
            i32.ne
            if
              i32.const 96
              call $proc_exit
            end
            i32.const 40
            i32.load
            call $thread_local_destroy
            call $exit_if_errno

            i32.const 48
            i32.const 52
            call $fd_pipe
            call $exit_if_errno
            i32.const 48
            i32.load
            call $fd_close
            call $exit_if_errno
            i32.const 52
            i32.load
            call $fd_close
            call $exit_if_errno

            i32.const 128
            call $pipe
            call $exit_if_errno

            i32.const 136
            i32.const 0
            call $pipe2
            call $exit_if_errno
            i32.const 136
            i32.load
            call $fd_close
            call $exit_if_errno
            i32.const 140
            i32.load
            call $fd_close
            call $exit_if_errno

            i32.const 128
            i32.load
            i32.const 0
            call $setns
            call $exit_if_errno

            i64.const 0
            call $unshare
            call $exit_if_errno

            i32.const 9999
            i32.const 0
            call $setns
            i32.const 0
            i32.ge_s
            if
              i32.const 107
              call $proc_exit
            end

            i32.const 0
            i32.const 0
            call $eventfd
            local.tee $dup_fd
            i32.const 0
            i32.lt_s
            if
              i32.const 108
              call $proc_exit
            end
            local.get $dup_fd
            call $fd_close
            call $exit_if_errno

            call $sched_yield
            call $exit_if_errno

            i32.const 0
            call $sched_get_priority_max
            call $exit_if_errno

            i32.const 0
            call $sched_get_priority_min
            call $exit_if_errno

            i32.const 9999
            call $sched_get_priority_max
            i32.const 0
            i32.ge_s
            if
              i32.const 110
              call $proc_exit
            end

            i32.const 0
            i32.const 2147483647
            call $eventfd
            i32.const 0
            i32.ge_s
            if
              i32.const 109
              call $proc_exit
            end

            i32.const 128
            i32.load
            call $dup
            local.tee $dup_fd
            i32.const 0
            i32.lt_s
            if
              i32.const 101
              call $proc_exit
            end
            local.get $dup_fd
            call $fd_close
            call $exit_if_errno

            i32.const 128
            i32.load
            i32.const 80
            call $dup2
            i32.const 80
            i32.ne
            if
              i32.const 102
              call $proc_exit
            end
            i32.const 80
            call $fd_close
            call $exit_if_errno

            i32.const 128
            i32.load
            i32.const 84
            i32.const 0
            call $dup3
            i32.const 84
            i32.ne
            if
              i32.const 106
              call $proc_exit
            end
            i32.const 84
            call $fd_close
            call $exit_if_errno

            i32.const 9999
            i32.const 132
            i32.load
            call $dup2
            i32.const 0
            i32.ge_s
            if
              i32.const 103
              call $proc_exit
            end

            i32.const 128
            i32.load
            call $fd_close
            call $exit_if_errno
            i32.const 132
            i32.load
            call $fd_close
            call $exit_if_errno

            i32.const 160
            i32.const 1
            call $chdir
            call $exit_if_errno

            i32.const 160
            i32.const 1
            call $proc_chdir
            call $exit_if_errno

            i32.const 9999
            call $fchdir
            i32.const 0
            i32.ge_s
            if
              i32.const 104
              call $proc_exit
            end

            i32.const 9999
            call $proc_fchdir
            i32.eqz
            if
              i32.const 105
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(
        outcome.response.exit_status,
        0,
        "stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_exposes_posix_identity_compat_imports() {
    let module = r#"
        (module
          (import "wasix_32v1" "getuid"
            (func $getuid (result i32)))
          (import "wasix_32v1" "geteuid"
            (func $geteuid (result i32)))
          (import "wasix_32v1" "getgid"
            (func $getgid (result i32)))
          (import "wasix_32v1" "getegid"
            (func $getegid (result i32)))
          (import "wasix_32v1" "getresuid"
            (func $getresuid (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "getresgid"
            (func $getresgid (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "setuid"
            (func $setuid (param i32) (result i32)))
          (import "wasix_32v1" "seteuid"
            (func $seteuid (param i32) (result i32)))
          (import "wasix_32v1" "setgid"
            (func $setgid (param i32) (result i32)))
          (import "wasix_32v1" "setegid"
            (func $setegid (param i32) (result i32)))
          (import "wasix_32v1" "setgroups"
            (func $setgroups (param i32 i32) (result i32)))
          (import "wasix_32v1" "getgroups"
            (func $getgroups (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func $expect (param $actual i32) (param $expected i32) (param $code i32)
            local.get $actual
            local.get $expected
            i32.ne
            if
              local.get $code
              call $proc_exit
            end)
          (func (export "_start") (local $errno i32)
            call $getuid
            i32.const 0
            i32.const 101
            call $expect
            call $geteuid
            i32.const 0
            i32.const 102
            call $expect
            call $getgid
            i32.const 0
            i32.const 103
            call $expect
            call $getegid
            i32.const 0
            i32.const 104
            call $expect

            i32.const 1001
            call $setuid
            call $exit_if_errno
            i32.const 1002
            call $setgid
            call $exit_if_errno
            i32.const 1003
            call $seteuid
            call $exit_if_errno
            i32.const 1004
            call $setegid
            call $exit_if_errno

            call $getuid
            i32.const 1001
            i32.const 105
            call $expect
            call $geteuid
            i32.const 1003
            i32.const 106
            call $expect
            call $getgid
            i32.const 1002
            i32.const 107
            call $expect
            call $getegid
            i32.const 1004
            i32.const 108
            call $expect

            i32.const 80
            i32.const 84
            i32.const 88
            call $getresuid
            call $exit_if_errno
            i32.const 80
            i32.load
            i32.const 1001
            i32.const 109
            call $expect
            i32.const 84
            i32.load
            i32.const 1003
            i32.const 110
            call $expect
            i32.const 88
            i32.load
            i32.const 1001
            i32.const 111
            call $expect

            i32.const 92
            i32.const 96
            i32.const 100
            call $getresgid
            call $exit_if_errno
            i32.const 92
            i32.load
            i32.const 1002
            i32.const 112
            call $expect
            i32.const 96
            i32.load
            i32.const 1004
            i32.const 113
            call $expect
            i32.const 100
            i32.load
            i32.const 1002
            i32.const 114
            call $expect

            i32.const 104
            i32.const 2001
            i32.store
            i32.const 108
            i32.const 2002
            i32.store
            i32.const 104
            i32.const 2
            call $setgroups
            call $exit_if_errno
            i32.const 112
            i32.const 2
            i32.const 120
            call $getgroups
            call $exit_if_errno
            i32.const 120
            i32.load
            i32.const 2
            i32.const 115
            call $expect
            i32.const 112
            i32.load
            i32.const 2001
            i32.const 116
            call $expect
            i32.const 116
            i32.load
            i32.const 2002
            i32.const 117
            call $expect))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(
        outcome.response.exit_status,
        0,
        "stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_exposes_posix_exec_vector_compat_imports() {
    let module = r#"
        (module
          (import "wasix_32v1" "execv"
            (func $execv (param i32 i32) (result i32)))
          (import "wasix_32v1" "execve"
            (func $execve (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "execvp"
            (func $execvp (param i32 i32) (result i32)))
          (import "wasix_32v1" "execvpe"
            (func $execvpe (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 128) "/definitely-missing\00")
          (data (i32.const 160) "definitely-missing\00")
          (data (i32.const 192) "argv0\00")
          (data (i32.const 224) "PATH=/bin:/usr/bin\00")
          (func $expect_nonzero (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 192
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 80
            i32.const 224
            i32.store
            i32.const 84
            i32.const 0
            i32.store

            i32.const 128
            i32.const 64
            call $execv
            i32.const 101
            call $expect_nonzero

            i32.const 128
            i32.const 64
            i32.const 80
            call $execve
            i32.const 102
            call $expect_nonzero

            i32.const 160
            i32.const 64
            call $execvp
            i32.const 103
            call $expect_nonzero

            i32.const 160
            i32.const 64
            i32.const 80
            call $execvpe
            i32.const 104
            call $expect_nonzero))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_posix_fork_fails_cleanly_without_asyncify_exports() {
    let module = r#"
        (module
          (import "wasix_32v1" "fork"
            (func $fork (result i32)))
          (import "wasix_32v1" "vfork"
            (func $vfork (result i32)))
          (import "wasix_32v1" "clone_thread"
            (func $clone_thread (param i64 i32 i32 i64 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (func (export "_start") (local $pid i32)
            call $fork
            local.tee $pid
            i32.const -45
            i32.ne
            if
              i32.const 79
              call $proc_exit
            end

            call $vfork
            local.tee $pid
            i32.const -45
            i32.ne
            if
              i32.const 80
              call $proc_exit
            end

            i64.const 65536
            i32.const 64
            i32.const 68
            i64.const 0
            i32.const 1
            i32.const 1
            i32.const 1
            call $clone_thread
            local.tee $pid
            i32.const -45
            i32.eq
            if
              i32.const 0
              call $proc_exit
            end

            i32.const 81
            call $proc_exit))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            wasix_auto_asyncify: false,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(
        outcome.response.exit_status,
        0,
        "stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_invokes_thread_local_destructors_on_exit() {
    let module = r#"
        (module
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_local_create"
            (func $thread_local_create (param i64 i32) (result i32)))
          (import "wasix_32v1" "thread_local_set"
            (func $thread_local_set (param i32 i64) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (table (export "__indirect_function_table") 2 funcref)
          (elem (i32.const 1) $destructor)
          (data (i32.const 100) "D")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func $destructor (param $value i64)
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "_start")
            i64.const 1
            i32.const 40
            call $thread_local_create
            call $exit_if_errno
            i32.const 40
            i32.load
            i64.const 7
            call $thread_local_set
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"D");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_invokes_thread_local_destructors_after_proc_exit() {
    let module = r#"
        (module
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_local_create"
            (func $thread_local_create (param i64 i32) (result i32)))
          (import "wasix_32v1" "thread_local_set"
            (func $thread_local_set (param i32 i64) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (table (export "__indirect_function_table") 2 funcref)
          (elem (i32.const 1) $destructor)
          (data (i32.const 100) "E")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func $destructor (param $value i64)
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "_start")
            i64.const 1
            i32.const 40
            call $thread_local_create
            call $exit_if_errno
            i32.const 40
            i32.load
            i64.const 7
            call $thread_local_set
            call $exit_if_errno
            i32.const 0
            call $proc_exit))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"E");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_invokes_thread_local_destructors_for_joined_threads() {
    let module = r#"
        (module
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasix_32v1" "thread_local_create"
            (func $thread_local_create (param i64 i32) (result i32)))
          (import "wasix_32v1" "thread_local_set"
            (func $thread_local_set (param i32 i64) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (import "env" "memory" (memory 1 128 shared))
          (table (export "__indirect_function_table") 2 funcref)
          (elem (i32.const 1) $destructor)
          (data (i32.const 100) "T")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func $destructor (param $value i64)
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 180
            i32.const 1
            i32.store
            i32.const 40
            i32.load
            i64.const 11
            call $thread_local_set
            call $exit_if_errno)
          (func (export "_start")
            i64.const 1
            i32.const 40
            call $thread_local_create
            call $exit_if_errno

            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            call $exit_if_errno
            i32.const 160
            i32.load
            call $thread_join
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"T");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_invokes_thread_local_destructors_after_thread_exit() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasix_32v1" "thread_exit"
            (func $thread_exit (param i32)))
          (import "wasix_32v1" "thread_local_create"
            (func $thread_local_create (param i64 i32) (result i32)))
          (import "wasix_32v1" "thread_local_set"
            (func $thread_local_set (param i32 i64) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (table (export "__indirect_function_table") 2 funcref)
          (elem (i32.const 1) $destructor)
          (data (i32.const 100) "X")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func $destructor (param $value i64)
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 40
            i32.load
            i64.const 13
            call $thread_local_set
            call $exit_if_errno
            i32.const 0
            call $thread_exit
            unreachable)
          (func (export "_start")
            i64.const 1
            i32.const 40
            call $thread_local_create
            call $exit_if_errno

            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            call $exit_if_errno
            i32.const 160
            i32.load
            call $thread_join
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"X");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_spawns_and_joins_thread() {
    let module = r#"
        (module
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128 shared)
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32))
          (func (export "_start")
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            i32.const 91
            call $exit_if_errno
            i32.const 160
            i32.load
            call $thread_join
            i32.const 92
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert!(outcome.stdout.is_empty());
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_spawns_thread_with_imported_shared_memory() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (data (i32.const 100) "I")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "_start")
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            i32.const 91
            call $exit_if_errno
            i32.const 160
            i32.load
            call $thread_join
            i32.const 92
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"I");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_allows_more_than_legacy_default_thread_task_limit() {
    let module = r#"
        (module
          (import "env" "memory" (memory 8 128 shared))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32))
          (func $write_start (param $index i32)
            i32.const 64
            i32.const 65536
            local.get $index
            i32.const 4096
            i32.mul
            i32.add
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store)
          (func (export "_start")
            (local $index i32)
            (local $errno i32)
            loop $spawn_until_limit
              local.get $index
              call $write_start
              i32.const 64
              i32.const 160
              call $thread_spawn_v2
              local.set $errno

              local.get $index
              i32.const 64
              i32.lt_u
              if
                local.get $errno
                if
                  local.get $index
                  i32.const 1
                  i32.add
                  call $proc_exit
                end
                local.get $index
                i32.const 1
                i32.add
                local.set $index
                br $spawn_until_limit
              end

              local.get $errno
              if
                i32.const 92
                call $proc_exit
              end
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_reaps_failed_thread_spawn_without_start_export() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (func $write_start
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store)
          (func (export "_start")
            (local $index i32)
            (local $errno i32)
            call $write_start
            loop $spawn_failures
              i32.const 64
              i32.const 160
              call $thread_spawn_v2
              local.set $errno

              local.get $errno
              i32.eqz
              if
                i32.const 91
                call $proc_exit
              end

              local.get $index
              i32.eqz
              if
                i32.const 200
                local.get $errno
                i32.store
              else
                local.get $errno
                i32.const 200
                i32.load
                i32.ne
                if
                  i32.const 92
                  call $proc_exit
                end
              end

              local.get $index
              i32.const 80
              i32.lt_u
              if
                local.get $index
                i32.const 1
                i32.add
                local.set $index
                br $spawn_failures
              end
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_wakes_thread_waiting_on_futex() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "futex_wait"
            (func $futex_wait (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "futex_wake"
            (func $futex_wake (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (data (i32.const 100) "F")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func $write_f
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 204
            i32.const 1
            i32.store
            i32.const 200
            i32.const 0
            i32.const 216
            i32.const 208
            call $futex_wait
            i32.const 93
            call $exit_if_errno
            i32.const 208
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end
            call $write_f)
          (func (export "_start")
            (local $remaining i32)
            i32.const 200
            i32.const 0
            i32.store
            i32.const 204
            i32.const 0
            i32.store
            i32.const 208
            i32.const 0
            i32.store
            i32.const 216
            i32.const 0
            i32.store
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            i32.const 91
            call $exit_if_errno

            i32.const 50000000
            local.set $remaining
            loop $wait_ready
              i32.const 204
              i32.load
              i32.eqz
              if
                local.get $remaining
                i32.eqz
                if
                  i32.const 95
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $wait_ready
              end
            end

            i32.const 50000000
            local.set $remaining
            loop $wake_waiter
              i32.const 212
              i32.const 0
              i32.store
              i32.const 200
              i32.const 212
              call $futex_wake
              i32.const 96
              call $exit_if_errno
              i32.const 212
              i32.load8_u
              if
              else
                local.get $remaining
                i32.eqz
                if
                  i32.const 97
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $wake_waiter
              end
            end

            i32.const 160
            i32.load
            call $thread_join
            i32.const 92
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"F");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_requeues_thread_waiting_on_futex() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "futex_wait"
            (func $futex_wait (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "futex_wake"
            (func $futex_wake (param i32 i32) (result i32)))
          (import "wasix_32v1" "futex_requeue"
            (func $futex_requeue (param i32 i32 i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (data (i32.const 100) "Q")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func $write_q
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 204
            i32.const 1
            i32.store
            i32.const 200
            i32.const 0
            i32.const 216
            i32.const 208
            call $futex_wait
            i32.const 93
            call $exit_if_errno
            i32.const 208
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end
            call $write_q)
          (func (export "_start")
            (local $remaining i32)
            i32.const 200
            i32.const 0
            i32.store
            i32.const 204
            i32.const 0
            i32.store
            i32.const 208
            i32.const 0
            i32.store
            i32.const 216
            i32.const 0
            i32.store
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            i32.const 91
            call $exit_if_errno

            i32.const 50000000
            local.set $remaining
            loop $wait_ready
              i32.const 204
              i32.load
              i32.eqz
              if
                local.get $remaining
                i32.eqz
                if
                  i32.const 95
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $wait_ready
              end
            end

            i32.const 50000000
            local.set $remaining
            loop $requeue_waiter
              i32.const 220
              i32.const 0
              i32.store
              i32.const 200
              i32.const 224
              i32.const 0
              i32.const 1
              i32.const -1
              i32.const 220
              call $futex_requeue
              i32.const 96
              call $exit_if_errno
              i32.const 220
              i32.load
              i32.const 1
              i32.eq
              if
              else
                local.get $remaining
                i32.eqz
                if
                  i32.const 97
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $requeue_waiter
              end
            end

            i32.const 220
            i32.const 0
            i32.store
            i32.const 200
            i32.const 220
            call $futex_wake
            i32.const 98
            call $exit_if_errno
            i32.const 220
            i32.load8_u
            if
              i32.const 99
              call $proc_exit
            end

            i32.const 220
            i32.const 0
            i32.store
            i32.const 224
            i32.const 220
            call $futex_wake
            i32.const 100
            call $exit_if_errno
            i32.const 220
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 101
              call $proc_exit
            end

            i32.const 160
            i32.load
            call $thread_join
            i32.const 92
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"Q");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_wait_any_reports_woken_futex_index() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "futex_wait_any"
            (func $futex_wait_any (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "futex_wake"
            (func $futex_wake (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (data (i32.const 100) "W")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func $write_w
            i32.const 16
            i32.const 100
            i32.store
            i32.const 20
            i32.const 1
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            drop)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 300
            i64.const 0
            i64.store
            i32.const 308
            i64.const 200
            i64.store
            i32.const 316
            i64.const 0
            i64.store
            i32.const 324
            i64.const 0
            i64.store
            i32.const 332
            i64.const 224
            i64.store
            i32.const 340
            i64.const 0
            i64.store
            i32.const 280
            i32.const 0
            i32.store
            i32.const 204
            i32.const 1
            i32.store
            i32.const 300
            i32.const 2
            i32.const 280
            i32.const 260
            call $futex_wait_any
            i32.const 93
            call $exit_if_errno
            i32.const 260
            i32.load
            i32.const 1
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end
            call $write_w)
          (func (export "_start")
            (local $remaining i32)
            i32.const 200
            i32.const 0
            i32.store
            i32.const 204
            i32.const 0
            i32.store
            i32.const 224
            i32.const 0
            i32.store
            i32.const 260
            i32.const 0
            i32.store
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            i32.const 91
            call $exit_if_errno

            i32.const 50000000
            local.set $remaining
            loop $wait_ready
              i32.const 204
              i32.load
              i32.eqz
              if
                local.get $remaining
                i32.eqz
                if
                  i32.const 95
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $wait_ready
              end
            end

            i32.const 50000000
            local.set $remaining
            loop $wake_second
              i32.const 264
              i32.const 0
              i32.store
              i32.const 224
              i32.const 264
              call $futex_wake
              i32.const 96
              call $exit_if_errno
              i32.const 264
              i32.load8_u
              if
              else
                local.get $remaining
                i32.eqz
                if
                  i32.const 97
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $wake_second
              end
            end

            i32.const 160
            i32.load
            call $thread_join
            i32.const 92
            call $exit_if_errno))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"W");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_cancels_join_on_worker_blocked_in_futex_wait() {
    let module = r#"
        (module
          (import "env" "memory" (memory 1 128 shared))
          (import "wasix_32v1" "futex_wait"
            (func $futex_wait (param i32 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "thread_spawn_v2"
            (func $thread_spawn_v2 (param i32 i32) (result i32)))
          (import "wasix_32v1" "thread_join"
            (func $thread_join (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "wasi_thread_start") (param $tid i32) (param $start_ptr i32)
            i32.const 204
            i32.const 1
            i32.store
            i32.const 200
            i32.const 0
            i32.const 216
            i32.const 208
            call $futex_wait
            i32.const 93
            call $exit_if_errno)
          (func (export "_start")
            (local $remaining i32)
            i32.const 200
            i32.const 0
            i32.store
            i32.const 204
            i32.const 0
            i32.store
            i32.const 208
            i32.const 0
            i32.store
            i32.const 216
            i32.const 0
            i32.store
            i32.const 64
            i32.const 65536
            i32.store
            i32.const 68
            i32.const 0
            i32.store
            i32.const 120
            i32.const 4096
            i32.store
            i32.const 124
            i32.const 0
            i32.store
            i32.const 64
            i32.const 160
            call $thread_spawn_v2
            i32.const 91
            call $exit_if_errno

            i32.const 50000000
            local.set $remaining
            loop $wait_ready
              i32.const 204
              i32.load
              i32.eqz
              if
                local.get $remaining
                i32.eqz
                if
                  i32.const 95
                  call $proc_exit
                end
                local.get $remaining
                i32.const 1
                i32.sub
                local.set $remaining
                br $wait_ready
              end
            end

            i32.const 160
            i32.load
            call $thread_join
            i32.const 92
            call $exit_if_errno))
        "#;

    let cancellation = SandboxCancellation::new();
    let canceller = cancellation.clone();
    let join = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        canceller.cancel();
    });
    let started = std::time::Instant::now();

    let error = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(30_000),
        SandboxWasiRunOptions {
            cancellation,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("worker futex wait plus thread join should be cancelled");
    join.join().expect("canceller");

    assert!(matches!(error, SandboxWasiError::Cancelled));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "cancellation should wake both thread_join and the worker futex wait"
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
fn wasix_runner_reaps_builtin_command_that_finishes_without_exec() {
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-builtin-reap-work");
    fs::create_dir_all(&work_dir).expect("work dir");

    let outcome = runtime::run_wasix_module(
        wasix_spawn_absolute_module("/bin/wasmer").as_bytes(),
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

    assert_eq!(outcome.response.exit_status, 0);
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_closes_cloexec_descriptors_before_child_start() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-cloexec-child-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-cloexec-child-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");

    let child = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 16) "\80\00\00\00\01\00\00\00")
          (data (i32.const 128) "x")
          (func (export "_start")
            i32.const 32
            i32.const 16
            i32.const 1
            i32.const 24
            call $fd_write
            i32.const 8
            i32.eq
            if
              i32.const 0
              call $proc_exit
            end
            i32.const 91
            call $proc_exit))
        "#;
    fs::write(
        bin_dir.join("probe"),
        wat::parse_str(child).expect("child wasm"),
    )
    .expect("child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "path_open2"
            (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "fd_dup2"
            (func $fd_dup2 (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_close"
            (func $fd_close (param i32) (result i32)))
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/work/leak.txt")
          (data (i32.const 96) "/bin/probe")
          (data (i32.const 112) "/bin/probe\0a")
          (data (i32.const 144) ".")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 3
            i32.const 0
            i32.const 64
            i32.const 14
            i32.const 9
            i64.const 64
            i64.const 0
            i32.const 0
            i32.const 0
            i32.const 180
            call $path_open2
            i32.const 70
            call $exit_if_errno

            i32.const 180
            i32.load
            i32.const 32
            i32.const 1
            i32.const 184
            call $fd_dup2
            i32.const 71
            call $exit_if_errno

            i32.const 180
            i32.load
            call $fd_close
            i32.const 72
            call $exit_if_errno

            i32.const 96
            i32.const 10
            i32.const 0
            i32.const 112
            i32.const 11
            i32.const 160
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 144
            i32.const 1
            i32.const 188
            call $proc_spawn
            i32.const 73
            call $exit_if_errno

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 188
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            i32.const 74
            call $exit_if_errno

            i32.const 224
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end
            i32.const 226
            i32.load16_u
            if
              i32.const 93
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_proc_spawn2_chdir_action_updates_child_cwd() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-spawn2-chdir-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-spawn2-chdir-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(work_dir.join("child")).expect("child cwd");

    let child = r#"
        (module
          (import "wasix_32v1" "getcwd"
            (func $getcwd (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 16) "\80\00\00\00\00\00\00\00")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 128
            i32.store
            i32.const 128
            i32.const 64
            call $getcwd
            i32.const 90
            call $exit_if_errno
            i32.const 64
            i32.load
            i32.const 11
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end

            i32.const 20
            i32.const 64
            i32.load
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 72
            call $fd_write
            i32.const 91
            call $exit_if_errno))
        "#;
    fs::write(
        bin_dir.join("cwdprobe"),
        wat::parse_str(child).expect("child wasm"),
    )
    .expect("child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "proc_spawn2"
            (func $proc_spawn2
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/cwdprobe")
          (data (i32.const 96) "/bin/cwdprobe\0a")
          (data (i32.const 448) "/work/child")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start") (local $errno i32)
            ;; ProcSpawnFdOp { cmd: Chdir, name: 448, name_len: 11 }
            i32.const 256
            i32.const 3
            i32.store8
            i32.const 268
            i32.const 448
            i32.store
            i32.const 272
            i32.const 11
            i32.store

            i32.const 64
            i32.const 13
            i32.const 96
            i32.const 14
            i32.const 0
            i32.const 0
            i32.const 256
            i32.const 1
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 128
            call $proc_spawn2
            local.tee $errno
            if
              local.get $errno
              call $proc_exit
            end

            i32.const 160
            i32.const 1
            i32.store8
            i32.const 164
            i32.const 128
            i32.load
            i32.store
            i32.const 160
            i32.const 0
            i32.const 192
            call $proc_join
            i32.const 71
            call $exit_if_errno

            i32.const 192
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 72
              call $proc_exit
            end
            i32.const 196
            i32.load
            if
              i32.const 73
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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
    assert_eq!(outcome.stdout, b"/work/child");
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_proc_spawn2_fchdir_action_updates_child_cwd() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-spawn2-fchdir-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-spawn2-fchdir-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(work_dir.join("child")).expect("child cwd");

    let child = r#"
        (module
          (import "wasix_32v1" "getcwd"
            (func $getcwd (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 16) "\80\00\00\00\00\00\00\00")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 128
            i32.store
            i32.const 128
            i32.const 64
            call $getcwd
            i32.const 90
            call $exit_if_errno
            i32.const 64
            i32.load
            i32.const 11
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end
            i32.const 128
            i32.load8_u
            i32.const 47
            i32.ne
            if
              i32.const 93
              call $proc_exit
            end
            i32.const 129
            i32.load8_u
            i32.const 119
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end
            i32.const 138
            i32.load8_u
            i32.const 100
            i32.ne
            if
              i32.const 95
              call $proc_exit
            end

            i32.const 20
            i32.const 64
            i32.load
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 72
            call $fd_write
            i32.const 91
            call $exit_if_errno))
        "#;
    fs::write(
        bin_dir.join("cwdprobe"),
        wat::parse_str(child).expect("child wasm"),
    )
    .expect("child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "path_open2"
            (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
          (import "wasix_32v1" "proc_spawn2"
            (func $proc_spawn2
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/cwdprobe")
          (data (i32.const 96) "/bin/cwdprobe\0a")
          (data (i32.const 448) "/work/child")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 3
            i32.const 0
            i32.const 448
            i32.const 11
            i32.const 2
            i64.const 0
            i64.const 0
            i32.const 0
            i32.const 0
            i32.const 224
            call $path_open2
            i32.const 69
            call $exit_if_errno
            i32.const 224
            i32.load
            i32.const 1
            i32.eq
            if
              i32.const 68
              call $proc_exit
            end

            ;; ProcSpawnFdOp { cmd: Fchdir, fd: opened directory fd }
            i32.const 256
            i32.const 4
            i32.store8
            i32.const 260
            i32.const 224
            i32.load
            i32.store

            i32.const 64
            i32.const 13
            i32.const 96
            i32.const 14
            i32.const 0
            i32.const 0
            i32.const 256
            i32.const 1
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 128
            call $proc_spawn2
            i32.const 70
            call $exit_if_errno

            i32.const 160
            i32.const 1
            i32.store8
            i32.const 164
            i32.const 128
            i32.load
            i32.store
            i32.const 160
            i32.const 0
            i32.const 192
            call $proc_join
            i32.const 71
            call $exit_if_errno

            i32.const 192
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 72
              call $proc_exit
            end
            i32.const 196
            i32.load
            if
              i32.const 196
              i32.load
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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

    assert_eq!(
        outcome.response.exit_status,
        0,
        "stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_reports_and_reaps_child_process_exit_status() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-child-exit-status-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-child-exit-status-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("exit7"),
        wat::parse_str(wasi_exit_module(7)).expect("child wasm"),
    )
    .expect("child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/exit7")
          (data (i32.const 96) "/bin/exit7\0a")
          (data (i32.const 128) ".")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func $exit_with_code_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 10
            i32.const 0
            i32.const 96
            i32.const 11
            i32.const 144
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 128
            i32.const 1
            i32.const 160
            call $proc_spawn
            i32.const 70
            call $exit_with_code_if_errno

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            i32.const 71
            call $exit_with_code_if_errno

            i32.const 224
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 91
              call $proc_exit
            end
            i32.const 226
            i32.load16_u
            i32.const 7
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 1
            i32.const 224
            call $proc_join
            i32.eqz
            if
              i32.const 93
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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
fn wasix_runner_failed_exec_preserves_caller_argv() {
    let module = r#"
        (module
          (import "wasix_32v1" "proc_exec3"
            (func $proc_exec3
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "args_sizes_get"
            (func $args_sizes_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "args_get"
            (func $args_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "environ_sizes_get"
            (func $environ_sizes_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "environ_get"
            (func $environ_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/missing")
          (data (i32.const 96) "mutated\0a")
          (data (i32.const 128) "CHANGED=1\0a")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            (local $argv0 i32)
            (local $argv1 i32)
            (local $index i32)
            (local $found i32)
            (local $entry i32)
            i32.const 64
            i32.const 12
            i32.const 96
            i32.const 8
            i32.const 128
            i32.const 10
            i32.const 0
            i32.const 0
            i32.const 0
            call $proc_exec3
            i32.eqz
            if
              i32.const 91
              call $proc_exit
            end

            i32.const 200
            i32.const 204
            call $args_sizes_get
            i32.const 92
            call $exit_if_errno
            i32.const 200
            i32.load
            i32.const 2
            i32.ne
            if
              i32.const 93
              call $proc_exit
            end

            i32.const 300
            i32.const 400
            call $args_get
            i32.const 94
            call $exit_if_errno
            i32.const 300
            i32.load
            local.set $argv0
            i32.const 304
            i32.load
            local.set $argv1

            local.get $argv0
            i32.load8_u
            i32.const 112
            i32.ne
            if
              i32.const 95
              call $proc_exit
            end
            local.get $argv1
            i32.load8_u
            i32.const 107
            i32.ne
            if
              i32.const 96
              call $proc_exit
            end

            i32.const 208
            i32.const 212
            call $environ_sizes_get
            i32.const 97
            call $exit_if_errno
            i32.const 500
            i32.const 700
            call $environ_get
            i32.const 98
            call $exit_if_errno

            i32.const 0
            local.set $index
            i32.const 0
            local.set $found
            loop $scan_env
              local.get $index
              i32.const 208
              i32.load
              i32.lt_u
              if
                i32.const 500
                local.get $index
                i32.const 4
                i32.mul
                i32.add
                i32.load
                local.set $entry
                local.get $entry
                i32.load8_u
                i32.const 79
                i32.eq
                if
                  local.get $entry
                  i32.const 1
                  i32.add
                  i32.load8_u
                  i32.const 82
                  i32.eq
                  if
                    i32.const 1
                    local.set $found
                  end
                end
                local.get $index
                i32.const 1
                i32.add
                local.set $index
                br $scan_env
              end
            end
            local.get $found
            i32.eqz
            if
              i32.const 99
              call $proc_exit
            end))
        "#;

    let mut request = test_wasi_request(5_000);
    request.argv = vec!["parent.wasm".to_string(), "keep".to_string()];
    request
        .env
        .insert("ORIGINAL".to_string(), "keep".to_string());

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        request,
        SandboxWasiRunOptions::default(),
    )
    .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_failed_vfork_exec_restores_parent_and_reaps_child() {
    let module = r#"
        (module
          (import "wasix_32v1" "proc_fork_env"
            (func $proc_fork_env (param i32) (result i32)))
          (import "wasix_32v1" "proc_exec3"
            (func $proc_exec3
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_exit2"
            (func $proc_exit2 (param i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "args_get"
            (func $args_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/missing")
          (data (i32.const 96) "child-mutated\0a")
          (data (i32.const 128) "CHILD=1\0a")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            (local $argv0 i32)
            (local $argv1 i32)
            i32.const 160
            call $proc_fork_env
            i32.const 70
            call $exit_if_errno

            i32.const 64
            i32.const 12
            i32.const 96
            i32.const 14
            i32.const 128
            i32.const 8
            i32.const 0
            i32.const 0
            i32.const 0
            call $proc_exec3
            i32.eqz
            if
              i32.const 91
              call $proc_exit
            end

            i32.const 7
            call $proc_exit2

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            i32.const 71
            call $exit_if_errno
            i32.const 224
            i32.load8_u
            i32.const 1
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end
            i32.const 226
            i32.load16_u
            i32.const 7
            i32.ne
            if
              i32.const 93
              call $proc_exit
            end

            i32.const 300
            i32.const 400
            call $args_get
            i32.const 72
            call $exit_if_errno
            i32.const 300
            i32.load
            local.set $argv0
            i32.const 304
            i32.load
            local.set $argv1
            local.get $argv0
            i32.load8_u
            i32.const 112
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end
            local.get $argv1
            i32.load8_u
            i32.const 107
            i32.ne
            if
              i32.const 95
              call $proc_exit
            end))
        "#;

    let mut request = test_wasi_request(5_000);
    request.argv = vec!["parent.wasm".to_string(), "keep".to_string()];

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        request,
        SandboxWasiRunOptions::default(),
    )
    .expect("run parent");

    assert_eq!(outcome.response.exit_status, 0);
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
fn wasix_runner_allows_guest_to_signal_and_join_child_process() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-child-signal-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-child-signal-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("loop"),
        wat::parse_str(wasix_yielding_loop_module()).expect("child wasm"),
    )
    .expect("loop child command");

    let outcome = runtime::run_wasix_module(
        wasix_spawn_signal_kill_module().as_bytes(),
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
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_rejects_session_leader_setpgid() {
    let module = r#"
        (module
          (import "wasix_32v1" "setpgid"
            (func $setpgid (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (func (export "_start")
            i32.const 0
            i32.const 0
            call $setpgid
            i32.eqz
            if
              i32.const 91
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_signals_child_process_group() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-process-group-signal-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-process-group-signal-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("loop"),
        wat::parse_str(wasix_yielding_loop_module()).expect("child wasm"),
    )
    .expect("loop child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_setpgid"
            (func $proc_setpgid (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_signal"
            (func $proc_signal (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/loop")
          (data (i32.const 96) "/bin/loop\0a")
          (data (i32.const 128) ".")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 9
            i32.const 0
            i32.const 96
            i32.const 10
            i32.const 144
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 128
            i32.const 1
            i32.const 160
            call $proc_spawn
            i32.const 70
            call $exit_if_errno

            i32.const 160
            i32.load
            i32.const 160
            i32.load
            call $proc_setpgid
            i32.const 71
            call $exit_if_errno

            i32.const 0
            i32.const 160
            i32.load
            i32.sub
            i32.const 9
            call $proc_signal
            i32.const 72
            call $exit_if_errno

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            i32.const 73
            call $exit_if_errno

            i32.const 224
            i32.load8_u
            i32.const 2
            i32.ne
            if
              i32.const 91
              call $proc_exit
            end
            i32.const 228
            i32.load8_u
            i32.const 9
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_reports_process_group_stop_without_reaping_child() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-process-group-stop-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-process-group-stop-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("loop"),
        wat::parse_str(wasix_yielding_loop_module()).expect("child wasm"),
    )
    .expect("loop child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_setpgid"
            (func $proc_setpgid (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_signal"
            (func $proc_signal (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/loop")
          (data (i32.const 96) "/bin/loop\0a")
          (data (i32.const 128) ".")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func $signal_group (param $signal i32) (param $code i32)
            i32.const 0
            i32.const 160
            i32.load
            i32.sub
            local.get $signal
            call $proc_signal
            local.get $code
            call $exit_if_errno)
          (func (export "_start")
            i32.const 64
            i32.const 9
            i32.const 0
            i32.const 96
            i32.const 10
            i32.const 144
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 128
            i32.const 1
            i32.const 160
            call $proc_spawn
            i32.const 70
            call $exit_if_errno

            i32.const 160
            i32.load
            i32.const 160
            i32.load
            call $proc_setpgid
            i32.const 71
            call $exit_if_errno

            i32.const 19
            i32.const 72
            call $signal_group

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 2
            i32.const 224
            call $proc_join
            i32.const 73
            call $exit_if_errno

            i32.const 224
            i32.load8_u
            i32.const 3
            i32.ne
            if
              i32.const 91
              call $proc_exit
            end
            i32.const 226
            i32.load8_u
            i32.const 19
            i32.ne
            if
              i32.const 92
              call $proc_exit
            end

            i32.const 18
            i32.const 74
            call $signal_group
            i32.const 9
            i32.const 75
            call $signal_group

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            i32.const 76
            call $exit_if_errno

            i32.const 224
            i32.load8_u
            i32.const 2
            i32.ne
            if
              i32.const 93
              call $proc_exit
            end
            i32.const 228
            i32.load8_u
            i32.const 9
            i32.ne
            if
              i32.const 94
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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
    fs::remove_dir_all(bin_dir).ok();
    fs::remove_dir_all(work_dir).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_reports_and_consumes_child_continued_status() {
    let bin_dir = unique_test_dir("sinomo-sandbox-wasix-process-continued-bin");
    let work_dir = unique_test_dir("sinomo-sandbox-wasix-process-continued-work");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    fs::create_dir_all(&work_dir).expect("work dir");
    fs::write(
        bin_dir.join("loop"),
        wat::parse_str(wasix_yielding_loop_module()).expect("child wasm"),
    )
    .expect("loop child command");

    let parent = r#"
        (module
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_setpgid"
            (func $proc_setpgid (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_signal"
            (func $proc_signal (param i32 i32) (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasix_32v1" "proc_child_continued_status"
            (func $proc_child_continued_status (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "/bin/loop")
          (data (i32.const 96) "/bin/loop\0a")
          (data (i32.const 128) ".")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func $signal_group (param $signal i32) (param $code i32)
            i32.const 0
            i32.const 160
            i32.load
            i32.sub
            local.get $signal
            call $proc_signal
            local.get $code
            call $exit_if_errno)
          (func $expect_continued_pid (param $consume i32) (param $code i32)
            i32.const 160
            i32.load
            local.get $consume
            i32.const 240
            call $proc_child_continued_status
            local.get $code
            call $exit_if_errno
            i32.const 240
            i32.load
            i32.const 160
            i32.load
            i32.ne
            if
              local.get $code
              i32.const 20
              i32.add
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 9
            i32.const 0
            i32.const 96
            i32.const 10
            i32.const 144
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 128
            i32.const 1
            i32.const 160
            call $proc_spawn
            i32.const 70
            call $exit_if_errno

            i32.const 160
            i32.load
            i32.const 160
            i32.load
            call $proc_setpgid
            i32.const 71
            call $exit_if_errno

            i32.const 19
            i32.const 72
            call $signal_group

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 2
            i32.const 224
            call $proc_join
            i32.const 73
            call $exit_if_errno

            i32.const 18
            i32.const 74
            call $signal_group

            i32.const 0
            i32.const 75
            call $expect_continued_pid
            i32.const 0
            i32.const 76
            call $expect_continued_pid
            i32.const 1
            i32.const 77
            call $expect_continued_pid

            i32.const 160
            i32.load
            i32.const 1
            i32.const 240
            call $proc_child_continued_status
            i32.const 78
            call $exit_if_errno
            i32.const 240
            i32.load
            if
              i32.const 98
              call $proc_exit
            end

            i32.const 9
            i32.const 79
            call $signal_group

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 160
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            i32.const 80
            call $exit_if_errno

            i32.const 224
            i32.load8_u
            i32.const 2
            i32.ne
            if
              i32.const 99
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(parent).expect("parent wasm").as_slice(),
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
    fs::remove_dir_all(bin_dir).ok();
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
fn wasix_runner_passes_buffered_stdin_to_module() {
    let mut request = test_wasi_request(1_000);
    request.stdin = Some(b"from buffered stdin\n".to_vec());

    let outcome = runtime::run_wasix_module(
        wasi_stdin_to_stdout_module().as_bytes(),
        request,
        SandboxWasiRunOptions::default(),
    )
    .expect("run WASIX buffered stdin");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"from buffered stdin\n");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_reads_fd_backed_stdin_pipe() {
    let (mut stdin_writer, stdin_reader) = virtual_fs::Pipe::channel();
    std::io::Write::write_all(&mut stdin_writer, b"from fd pipe\n").expect("pipe write");
    let stdin_reader = virtual_fs::ArcFile::new(Box::new(stdin_reader));

    let outcome = runtime::run_wasix_module(
        wasi_stdin_to_stdout_module().as_bytes(),
        test_wasi_request(1_000),
        SandboxWasiRunOptions {
            wasix_stdin_pipe: Some(stdin_reader),
            input_limit_bytes: 64,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run WASIX stdin pipe");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"from fd pipe\n");
    stdin_writer.close();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_tees_stdout_to_fd_backed_pipe() {
    let (stdout_writer, mut stdout_reader) = virtual_fs::Pipe::channel();

    let outcome = runtime::run_wasix_module(
        wasi_stdout_module().as_bytes(),
        test_wasi_request(1_000),
        SandboxWasiRunOptions {
            wasix_stdout_pipe: Some(stdout_writer),
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run WASIX stdout pipe");

    let mut piped_stdout = Vec::new();
    std::io::Read::read_to_end(&mut stdout_reader, &mut piped_stdout).expect("pipe read");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    assert_eq!(piped_stdout, b"hello\n");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_connects_stdout_pipe_to_concurrent_stdin_pipe() {
    let (stdout_writer, stdin_reader) = virtual_fs::Pipe::channel();
    let consumer = std::thread::spawn(move || {
        runtime::run_wasix_module(
            wasi_stdin_to_stdout_module().as_bytes(),
            test_wasi_request(5_000),
            SandboxWasiRunOptions {
                wasix_stdin_pipe: Some(virtual_fs::ArcFile::new(Box::new(stdin_reader))),
                input_limit_bytes: 64,
                ..SandboxWasiRunOptions::default()
            },
        )
    });

    let producer = runtime::run_wasix_module(
        wasi_stdout_module().as_bytes(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            wasix_stdout_pipe: Some(stdout_writer),
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run WASIX stdout producer");

    let consumer = consumer
        .join()
        .expect("consumer thread")
        .expect("run WASIX stdin consumer");

    assert_eq!(producer.response.exit_status, 0);
    assert_eq!(producer.stdout, b"hello\n");
    assert_eq!(consumer.response.exit_status, 0);
    assert_eq!(consumer.stdout, b"hello\n");
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_enforces_fd_backed_stdin_pipe_input_limit() {
    let (mut stdin_writer, stdin_reader) = virtual_fs::Pipe::channel();
    std::io::Write::write_all(&mut stdin_writer, b"too much fd stdin").expect("pipe write");

    let error = runtime::run_wasix_module(
        wasi_stdin_to_stdout_module().as_bytes(),
        test_wasi_request(1_000),
        SandboxWasiRunOptions {
            wasix_stdin_pipe: Some(virtual_fs::ArcFile::new(Box::new(stdin_reader))),
            input_limit_bytes: 4,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("WASIX fd stdin input limit");

    assert!(matches!(
        error,
        SandboxWasiError::Runtime { .. } | SandboxWasiError::InputLimitExceeded { .. }
    ));
    stdin_writer.close();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_rejects_ambiguous_buffered_and_fd_stdin() {
    let (_stdin_writer, stdin_reader) = virtual_fs::Pipe::channel();
    let mut request = test_wasi_request(1_000);
    request.stdin = Some(b"buffered stdin\n".to_vec());

    let error = runtime::run_wasix_module(
        wasi_stdin_to_stdout_module().as_bytes(),
        request,
        SandboxWasiRunOptions {
            wasix_stdin_pipe: Some(virtual_fs::ArcFile::new(Box::new(stdin_reader))),
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("ambiguous WASIX stdin");

    assert!(matches!(error, SandboxWasiError::InvalidRequest { .. }));
    assert!(error.to_string().contains("fd-backed pipe"));
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

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_denies_create_when_parent_mode_disallows_mutation() {
    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_wasix_parent_create_permission");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let work_path = SandboxPath::new("/work").expect("work path");
    sandbox_fs
        .set_owner(&work_path, 0, 0)
        .expect("set work owner");
    sandbox_fs
        .set_mode(&work_path, 0o555)
        .expect("set work mode");
    let sandbox_fs = Arc::new(Mutex::new(sandbox_fs));

    let module = r#"
        (module
          (import "wasix_32v1" "setuid"
            (func $setuid (param i32) (result i32)))
          (import "wasix_32v1" "path_open2"
            (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_close"
            (func $fd_close (param i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 96) "/work/blocked.txt")
          (func $exit_if_errno (param $errno i32) (param $code i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $code
              call $proc_exit
            end)
          (func (export "_start") (local $errno i32)
            i32.const 1001
            call $setuid
            i32.const 80
            call $exit_if_errno

            i32.const 3
            i32.const 0
            i32.const 96
            i32.const 17
            i32.const 9
            i64.const 64
            i64.const 0
            i32.const 0
            i32.const 0
            i32.const 64
            call $path_open2
            local.set $errno

            local.get $errno
            i32.eqz
            if
              i32.const 64
              i32.load
              call $fd_close
              drop
              i32.const 90
              call $proc_exit
            end

            local.get $errno
            i32.const 2
            i32.ne
            if
              local.get $errno
              call $proc_exit
            end))
        "#;

    let outcome = runtime::run_wasix_module(
        wat::parse_str(module).expect("module").as_slice(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            virtual_preopened_dirs: runtime::wasix_sandbox_workspace_filesystem_preopens(
                Arc::clone(&sandbox_fs),
                SandboxMountMode::ReadWrite,
            ),
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(
        outcome.response.exit_status,
        0,
        "stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
    assert!(matches!(
        sandbox_fs
            .lock()
            .expect("fs")
            .read(
                &SandboxPath::new("/work/blocked.txt").expect("blocked path"),
                None
            )
            .expect_err("denied create should not persist"),
        SandboxFsError::NotFound { .. }
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn wasix_runner_uses_virtual_cwd_without_host_cwd_path() {
    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_wasix_virtual_cwd_no_host_path");
    let sandbox_fs = Arc::new(Mutex::new(LocalSandboxFileSystem::new(layout)));

    let outcome = runtime::run_wasix_module(
        wasi_stdout_module().as_bytes(),
        test_wasi_request(5_000),
        SandboxWasiRunOptions {
            virtual_preopened_dirs: runtime::wasix_sandbox_workspace_filesystem_preopens(
                Arc::clone(&sandbox_fs),
                SandboxMountMode::ReadWrite,
            ),
            working_directory_host_path: Some(runtime_root.join("not-a-mounted-cwd")),
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect("WASIX virtual cwd should not need a host cwd path");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    fs::remove_dir_all(runtime_root).ok();
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
