use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sinomo_module::{ModuleId, NativeDylibModuleLoaderState};
use sinomo_sandbox::{
    LocalSandboxBackend, LocalSandboxInstanceStore, LocalSandboxToolchainStore, SandboxBackend,
    SandboxCancellationToken, SandboxCommandExecutionError, SandboxId,
    SandboxLocalExecutionJournalPhase, SandboxPath, SandboxReadRequest, SandboxStatusRequest,
    SandboxStoreError, SandboxToolchainId, SandboxWasiError, read_sandbox_local_execution_journal,
    recover_sandbox_local_execution_journal,
};
use sinomo_sandbox_wasix_api::{
    SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, SINOMO_SANDBOX_WASIX_MODULE_ID,
    SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION,
};
use sinomo_sandbox_wasix_host::SandboxWasixModuleCommandRunner;
use time::OffsetDateTime;

#[test]
fn active_native_module_runs_preview1_module_when_env_set() {
    let Ok(module_path) = std::env::var("SINOMO_WASIX_NATIVE_MODULE_PATH") else {
        eprintln!("skipping: SINOMO_WASIX_NATIVE_MODULE_PATH is not set");
        return;
    };
    let module_path = PathBuf::from(module_path);
    let root = unique_temp("sinomo-wasix-host-loader");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create temp root");

    install_active_wasix_module(&root, &module_path);

    let runner =
        SandboxWasixModuleCommandRunner::from_active_module(&root).expect("load active module");
    let store = LocalSandboxInstanceStore::new(root.join("sandbox").join("runtime"));
    let toolchain_store = LocalSandboxToolchainStore::new(root.join("sandbox").join("cache"));
    let sandbox_id = SandboxId::generate();
    store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::generate(),
            OffsetDateTime::now_utc(),
        )
        .expect("create sandbox");
    let wasm = wat::parse_str(
        r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1 2)
          (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
          (func (export "_start")
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            drop))
        "#,
    )
    .expect("compile wat");

    let outcome = runner
        .run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
            &store,
            &toolchain_store,
            &sandbox_id,
            wasm,
            vec!["hello.wasm".to_string()],
            None,
            5_000,
            None,
            Vec::new(),
            None,
        )
        .expect("run wasm through active module");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn active_native_module_preserves_cancellation_as_typed_error_when_env_set() {
    let Ok(module_path) = std::env::var("SINOMO_WASIX_NATIVE_MODULE_PATH") else {
        eprintln!("skipping: SINOMO_WASIX_NATIVE_MODULE_PATH is not set");
        return;
    };
    let module_path = PathBuf::from(module_path);
    let root = unique_temp("sinomo-wasix-host-cancellation");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create temp root");

    install_active_wasix_module(&root, &module_path);

    let runner =
        SandboxWasixModuleCommandRunner::from_active_module(&root).expect("load active module");
    let store = LocalSandboxInstanceStore::new(root.join("sandbox").join("runtime"));
    let toolchain_store = LocalSandboxToolchainStore::new(root.join("sandbox").join("cache"));
    let sandbox_id = SandboxId::generate();
    store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::generate(),
            OffsetDateTime::now_utc(),
        )
        .expect("create sandbox");
    let layout = store.layout(&sandbox_id);
    for index in 0..512 {
        fs::write(
            layout.work.join(format!("large-tree-{index:04}.bin")),
            vec![index as u8; 4096],
        )
        .expect("seed workspace tree");
    }
    let wasm = wat::parse_str(
        r#"
        (module
          (import "wasix_32v1" "sched_yield"
            (func $sched_yield (result i32)))
          (memory (export "memory") 1 2)
          (func (export "_start")
            (loop $again
              call $sched_yield
              drop
              br $again)))
        "#,
    )
    .expect("compile yielding WASIX module");
    let cancellation = SandboxCancellationToken::new();
    let canceller = cancellation.clone();
    let cancellation_store = store.clone();
    let cancellation_sandbox_id = sandbox_id.clone();
    let cancel_thread = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if read_sandbox_local_execution_journal(&cancellation_store, &cancellation_sandbox_id)
                .expect("read cancellation marker")
                .is_some_and(|record| {
                    record.phase == SandboxLocalExecutionJournalPhase::GuestActive
                })
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        canceller.cancel();
    });
    let started = std::time::Instant::now();

    let error = runner
        .run_module_with_stdin_limits_preopens_output_sink_toolchain_and_cancellation(
            &store,
            &toolchain_store,
            &sandbox_id,
            wasm,
            vec!["yield-forever.wasm".to_string()],
            None,
            30_000,
            None,
            Vec::new(),
            None,
            cancellation,
        )
        .expect_err("native module execution should observe cancellation");
    cancel_thread.join().expect("canceller");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::CancelledCleanupFailed { .. })
    ));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "native module cancellation should not wait for the command timeout"
    );
    let marker = read_sandbox_local_execution_journal(&store, &sandbox_id)
        .expect("read durable cancellation marker")
        .expect("cancellation leaves cleanup marker");
    assert_eq!(
        marker.phase,
        SandboxLocalExecutionJournalPhase::OutcomeRecorded
    );
    recover_sandbox_local_execution_journal(&store, &sandbox_id, OffsetDateTime::now_utc())
        .expect("recover deferred cancellation cleanup");
    assert_eq!(
        read_sandbox_local_execution_journal(&store, &sandbox_id).expect("read marker"),
        None
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn live_native_owner_fences_observers_and_direct_runner_overlap_when_env_set() {
    let Ok(module_path) = std::env::var("SINOMO_WASIX_NATIVE_MODULE_PATH") else {
        eprintln!("skipping: SINOMO_WASIX_NATIVE_MODULE_PATH is not set");
        return;
    };
    let root = unique_temp("sinomo-wasix-host-live-overlap");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create temp root");
    install_active_wasix_module(&root, &PathBuf::from(module_path));

    let store = LocalSandboxInstanceStore::new(root.join("sandbox/runtime"));
    let toolchain_store = LocalSandboxToolchainStore::new(root.join("sandbox/cache"));
    let sandbox_id = SandboxId::generate();
    store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::generate(),
            OffsetDateTime::now_utc(),
        )
        .expect("create sandbox");
    let yielding_wasm = wat::parse_str(
        r#"
        (module
          (import "wasix_32v1" "sched_yield" (func $sched_yield (result i32)))
          (memory (export "memory") 1 2)
          (func (export "_start")
            (loop $again call $sched_yield drop br $again)))
        "#,
    )
    .expect("compile yielding module");
    let cancellation = SandboxCancellationToken::new();
    let run_root = root.clone();
    let run_store = store.clone();
    let run_toolchain_store = toolchain_store.clone();
    let run_sandbox_id = sandbox_id.clone();
    let run_wasm = yielding_wasm.clone();
    let run_cancellation = cancellation.clone();
    let live_run = std::thread::spawn(move || {
        SandboxWasixModuleCommandRunner::from_active_module(&run_root)
            .expect("load first runner")
            .run_module_with_stdin_limits_preopens_output_sink_toolchain_and_cancellation(
                &run_store,
                &run_toolchain_store,
                &run_sandbox_id,
                run_wasm,
                vec!["first-live-run.wasm".to_string()],
                None,
                30_000,
                None,
                Vec::new(),
                None,
                run_cancellation,
            )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let guest_active = loop {
        if read_sandbox_local_execution_journal(&store, &sandbox_id)
            .expect("read live marker")
            .is_some_and(|record| record.phase == SandboxLocalExecutionJournalPhase::GuestActive)
        {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    };
    if !guest_active {
        cancellation.cancel();
        let _ = live_run.join();
        panic!("native runner did not publish GuestActive before deadline");
    }

    let mut backend = LocalSandboxBackend::with_store(store.clone());
    for error in [
        backend
            .status(SandboxStatusRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect_err("status must report live owner"),
        backend
            .read(SandboxReadRequest {
                sandbox_id: sandbox_id.clone(),
                path: SandboxPath::new("/work/not-yet-created").expect("path"),
                range: None,
            })
            .expect_err("read must report live owner"),
    ] {
        assert!(error.to_string().contains("active local execution owner"));
    }

    let second_runner =
        SandboxWasixModuleCommandRunner::from_active_module(&root).expect("load second runner");
    let overlap_error = second_runner
        .run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
            &store,
            &toolchain_store,
            &sandbox_id,
            yielding_wasm,
            vec!["overlapping-run.wasm".to_string()],
            None,
            5_000,
            None,
            Vec::new(),
            None,
        )
        .expect_err("second direct runner must not overlap the live owner");
    assert!(matches!(
        overlap_error,
        SandboxCommandExecutionError::Store(SandboxStoreError::LocalExecutionBusy { .. })
    ));

    cancellation.cancel();
    let first_error = live_run
        .join()
        .expect("join first runner")
        .expect_err("first runner should be cancelled");
    assert!(matches!(
        first_error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::CancelledCleanupFailed { .. })
    ));
    recover_sandbox_local_execution_journal(&store, &sandbox_id, OffsetDateTime::now_utc())
        .expect("recover cancelled owner");

    let finite_wasm =
        wat::parse_str(r#"(module (memory (export "memory") 1 2) (func (export "_start")))"#)
            .expect("compile finite module");
    second_runner
        .run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
            &store,
            &toolchain_store,
            &sandbox_id,
            finite_wasm,
            vec!["after-overlap.wasm".to_string()],
            None,
            5_000,
            None,
            Vec::new(),
            None,
        )
        .expect("direct runner succeeds after owner cleanup");
    let _ = fs::remove_dir_all(root);
}

fn install_active_wasix_module(data_dir_root: &Path, module_path: &Path) {
    let bytes = fs::read(module_path).expect("read WASIX native module");
    let content_hash = sinomo_module::native_module_content_hash(&bytes);
    let library_name = module_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("module library file name")
        .to_string();
    let cache_key = "module/sandbox/wasix";
    let cache_path = sinomo_module::native_module_cache_path(
        data_dir_root,
        cache_key,
        &content_hash,
        &library_name,
    );
    fs::create_dir_all(cache_path.parent().expect("cache parent")).expect("create cache dir");
    fs::write(&cache_path, &bytes).expect("write cached module");

    let module = unsafe {
        sinomo_sandbox_wasix_host::SandboxWasixNativeModule::load_from_content_addressed_path(
            &cache_path,
            &content_hash,
        )
    }
    .expect("load cached module");
    let descriptor = module.descriptor().clone();
    drop(module);

    let module_id = ModuleId::from_legacy_named(SINOMO_SANDBOX_WASIX_MODULE_ID).expect("module id");
    sinomo_module::write_native_dylib_module_loader_state(
        data_dir_root,
        &NativeDylibModuleLoaderState {
            module_id,
            artifact_id: None,
            content_hash,
            cache_key: cache_key.to_string(),
            library_name,
            platform_triple: descriptor.platform_triple,
            native_abi_version: SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION,
            interface_version: SINOMO_SANDBOX_WASIX_INTERFACE_VERSION.to_string(),
            activated_at: OffsetDateTime::now_utc().to_string(),
            source: format!("env:{}", module_path.display()),
        },
    )
    .expect("write loader state");
}

fn unique_temp(prefix: &str) -> PathBuf {
    std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("test-tmp")
        .join(format!(
            "{prefix}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
}
