use super::memory::validate_wasix_module_memory_budget;
use super::output::WasixStreamingOutputCapture;
use super::sandbox_wasix_capabilities;
use super::setup::WasixReadOnlyFileSystem;
use crate::{
    LocalSandboxFileSystem, LocalSandboxInstanceLayout, SANDBOX_OUTPUT_CHUNK_BYTES,
    SANDBOX_WASIX_MAX_TASKS, SandboxCancellation, SandboxCommandId, SandboxCommandRequest,
    SandboxFileSystem, SandboxId, SandboxMountMode, SandboxOutputChunk, SandboxOutputStream,
    SandboxPath, SandboxWasiError, SandboxWasiPreopen, SandboxWasiRunOptions,
};
use std::collections::BTreeMap;
use std::{
    fs, io,
    sync::{Arc, Mutex},
    time::Duration,
};
use wasmer::{
    AsStoreRef, Extern, Function, FunctionType, Imports, Instance, Module, Store, Table, Type,
    Value,
    sys::{Cranelift, EngineBuilder, Features},
};

#[test]
fn wasix_capabilities_disable_ambient_authority_and_cap_tasks() {
    let capabilities = sandbox_wasix_capabilities();

    assert!(!capabilities.insecure_allow_all);
    assert!(!capabilities.http_client.allow_all);
    assert!(capabilities.http_client.allowed_hosts.is_empty());
    assert_eq!(
        capabilities.threading.max_threads,
        Some(SANDBOX_WASIX_MAX_TASKS)
    );
    assert!(!capabilities.threading.enable_asynchronous_threading);
    assert!(
        capabilities
            .threading
            .enable_exponential_cpu_backoff
            .is_none()
    );
}

#[test]
fn wasix_read_only_filesystem_rejects_mutating_operations() {
    let root = sinomo_testdir::TestDir::new("sinomo-wasix-read-only-fs");
    fs::write(root.join("file.txt"), b"hello").unwrap();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let host_fs =
        virtual_fs::host_fs::FileSystem::new(tokio::runtime::Handle::current(), &root).unwrap();
    let read_only = WasixReadOnlyFileSystem::new(Arc::new(host_fs));

    let mut read_options = virtual_fs::FileSystem::new_open_options(&read_only);
    let mut file = read_options.read(true).open("/file.txt").unwrap();
    let mut bytes = Vec::new();
    virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes)).unwrap();
    assert_eq!(bytes, b"hello");
    assert!(matches!(
        file.as_mut().set_len(0),
        Err(virtual_fs::FsError::PermissionDenied)
    ));
    assert!(matches!(
        file.as_mut().set_times(Some(1), Some(2)),
        Err(virtual_fs::FsError::PermissionDenied)
    ));
    assert!(matches!(
        file.as_mut().note_link_added(),
        Err(virtual_fs::FsError::PermissionDenied)
    ));
    assert!(matches!(
        file.as_mut().note_link_removed(),
        Err(virtual_fs::FsError::PermissionDenied)
    ));
    let write_error = virtual_mio::block_on(virtual_fs::AsyncWriteExt::write(&mut file, b"x"))
        .expect_err("read-only file handle must reject writes");
    assert_eq!(write_error.kind(), io::ErrorKind::PermissionDenied);

    let mut write_options = virtual_fs::FileSystem::new_open_options(&read_only);
    let error = write_options.write(true).open("/file.txt").unwrap_err();
    assert!(matches!(error, virtual_fs::FsError::PermissionDenied));
    assert!(matches!(
        virtual_fs::FileSystem::create_dir(&read_only, "/new".as_ref()),
        Err(virtual_fs::FsError::PermissionDenied)
    ));
    assert_eq!(fs::read(root.join("file.txt")).unwrap(), b"hello");
}

#[test]
fn wasix_virtual_fs_error_conversion_preserves_posix_directory_errors() {
    let not_directory: std::io::Error = virtual_fs::FsError::BaseNotDirectory.into();
    assert_eq!(not_directory.kind(), std::io::ErrorKind::NotADirectory);

    let not_file: std::io::Error = virtual_fs::FsError::NotAFile.into();
    assert_eq!(not_file.kind(), std::io::ErrorKind::InvalidInput);

    let directory_not_empty: std::io::Error = virtual_fs::FsError::DirectoryNotEmpty.into();
    assert_eq!(
        directory_not_empty.kind(),
        std::io::ErrorKind::DirectoryNotEmpty
    );
}

#[test]
fn wasix_streaming_output_capture_emits_chunks_at_write_time() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink_received = Arc::clone(&received);
    let sink: Arc<dyn crate::SandboxOutputSink> = Arc::new(move |chunk: SandboxOutputChunk| {
        sink_received.lock().unwrap().push(chunk);
    });
    let capture = WasixStreamingOutputCapture::new(
        crate::SandboxId::generate(),
        SandboxCommandId::generate(),
        SandboxOutputStream::Stdout,
        SANDBOX_OUTPUT_CHUNK_BYTES,
        Some(sink),
        None,
    );
    let mut file = capture.file();

    let written =
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write(&mut file, b"hello")).unwrap();

    assert_eq!(written, 5);
    assert_eq!(capture.bytes().unwrap(), b"hello");
    assert_eq!(capture.chunks().unwrap().len(), 1);
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].offset, 0);
    assert_eq!(received[0].bytes, b"hello");
}

#[test]
fn wasix_streaming_output_capture_rejects_writes_over_limit() {
    let capture = WasixStreamingOutputCapture::new(
        crate::SandboxId::generate(),
        SandboxCommandId::generate(),
        SandboxOutputStream::Stderr,
        4,
        None,
        None,
    );
    let mut file = capture.file();

    let error =
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write(&mut file, b"hello")).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(capture.output_limit_exceeded().unwrap());
    assert!(capture.bytes().unwrap().is_empty());
    assert!(capture.chunks().unwrap().is_empty());
}

#[test]
fn wasix_streaming_output_capture_rejects_resize_over_limit() {
    let capture = WasixStreamingOutputCapture::new(
        crate::SandboxId::generate(),
        SandboxCommandId::generate(),
        SandboxOutputStream::Stdout,
        4,
        None,
        None,
    );
    let mut file = capture.file();

    let error = virtual_fs::VirtualFile::set_len(&mut file, 5).unwrap_err();

    assert!(matches!(error, virtual_fs::FsError::UnknownError));
    assert!(capture.output_limit_exceeded().unwrap());
    assert!(capture.bytes().unwrap().is_empty());
}

#[test]
fn wasix_streaming_output_capture_allows_in_budget_overwrite() {
    let capture = WasixStreamingOutputCapture::new(
        crate::SandboxId::generate(),
        SandboxCommandId::generate(),
        SandboxOutputStream::Stdout,
        5,
        None,
        None,
    );
    let mut file = capture.file();

    virtual_mio::block_on(async {
        use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
        file.write_all(b"hello").await?;
        file.rewind().await?;
        file.write_all(b"j").await
    })
    .unwrap();

    assert_eq!(capture.bytes().unwrap(), b"jello");
    assert!(!capture.output_limit_exceeded().unwrap());
}

#[test]
fn wasix_streaming_output_capture_reads_past_eof_as_empty() {
    let capture = WasixStreamingOutputCapture::new(
        crate::SandboxId::generate(),
        SandboxCommandId::generate(),
        SandboxOutputStream::Stdout,
        8,
        None,
        None,
    );
    let mut file = capture.file();

    virtual_mio::block_on(async {
        use virtual_fs::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
        file.write_all(b"hello").await?;
        file.seek(io::SeekFrom::Start(100)).await?;
        let mut buf = [0_u8; 4];
        let read = file.read(&mut buf).await?;
        assert_eq!(read, 0);
        Ok::<(), io::Error>(())
    })
    .unwrap();
}

#[test]
fn wasix_streaming_output_capture_rejects_unrepresentable_resize() {
    let capture = WasixStreamingOutputCapture::new(
        crate::SandboxId::generate(),
        SandboxCommandId::generate(),
        SandboxOutputStream::Stdout,
        usize::MAX,
        None,
        None,
    );
    let mut file = capture.file();

    let error = virtual_fs::VirtualFile::set_len(&mut file, u64::MAX).unwrap_err();

    assert!(matches!(error, virtual_fs::FsError::UnknownError));
    assert!(capture.output_limit_exceeded().unwrap());
}

#[test]
fn wasix_fpcast_adapter_can_bridge_indirect_call_abis() {
    let compiler = Cranelift::default();
    let features = Features::all();
    let engine: wasmer::Engine = EngineBuilder::new(compiler)
        .set_features(Some(features))
        .into();
    let mut store = Store::new(engine);

    let target_module = Module::new(
        &store,
        r#"
        (module
          (func (export "target")
            (param i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64
                   i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64
                   i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64
                   i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64)
            (result i64)
            local.get 0
            local.get 15
            i64.add))
        "#,
    )
    .expect("target module");
    let target_instance =
        Instance::new(&mut store, &target_module, &Imports::new()).expect("target instance");
    let target = target_instance
        .exports
        .get_function("target")
        .expect("target export")
        .clone();

    let adapter = fpcast_adapter_for_test(
        &mut store,
        target,
        &FunctionType::new(vec![Type::I64; 16], vec![Type::I64]),
    );
    assert_eq!(
        adapter.ty(&store.as_store_ref()),
        FunctionType::new(vec![Type::I64; 16], vec![Type::I64])
    );

    let caller_module = Module::new(
        &store,
        r#"
        (module
          (type $fpcast16 (func
            (param i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64)
            (result i64)))
          (import "env" "table" (table 1 funcref))
          (func (export "run") (result i64)
            i64.const 2
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 0
            i64.const 40
            i32.const 0
            call_indirect (type $fpcast16)))
        "#,
    )
    .expect("table caller module");
    let table_type = caller_module
        .imports()
        .find_map(|import| match import.ty() {
            wasmer::ExternType::Table(table_type) => Some(table_type.clone()),
            _ => None,
        })
        .expect("table import");
    let table = Table::new(&mut store, table_type, Value::FuncRef(None)).expect("table");
    table
        .set(&mut store, 0, Value::FuncRef(Some(adapter)))
        .expect("set adapter");
    let mut imports = Imports::new();
    imports.define("env", "table", Extern::Table(table));
    let caller_instance =
        Instance::new(&mut store, &caller_module, &imports).expect("table caller instance");
    let run = caller_instance
        .exports
        .get_typed_function::<(), i64>(&store, "run")
        .expect("run export");

    assert_eq!(run.call(&mut store).expect("indirect fpcast call"), 42);
}

#[test]
fn wasix_runner_signals_yielding_process_tree_on_timeout() {
    let error = super::run_wasix_module(
        wasix_yield_forever_module().as_slice(),
        wasix_test_request(10),
        SandboxWasiRunOptions::default(),
    )
    .expect_err("yielding WASIX process should be timed out");

    assert!(matches!(
        error,
        SandboxWasiError::TimedOut { timeout_ms: 10 }
    ));
}

#[test]
fn wasix_runner_times_out_non_yielding_loop_with_instruction_budget() {
    let error = super::run_wasix_module(
        wasix_non_yielding_loop_module().as_slice(),
        wasix_test_request(10),
        SandboxWasiRunOptions::default(),
    )
    .expect_err("non-yielding WASIX process should exhaust the instruction budget");

    assert!(matches!(
        error,
        SandboxWasiError::TimedOut { timeout_ms: 10 }
    ));
}

#[test]
fn wasix_runner_keeps_guest_unreachable_as_runtime_error() {
    let outcome = super::run_wasix_module(
        wasix_unreachable_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("guest unreachable should be reported as a failed process");

    assert_ne!(outcome.response.exit_status, 0);
}

#[test]
fn wasix_runner_signals_yielding_process_tree_on_cancellation() {
    let cancellation = SandboxCancellation::new();
    let canceller = cancellation.clone();
    let join = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        canceller.cancel();
    });
    let started = std::time::Instant::now();

    let error = super::run_wasix_module(
        wasix_yield_forever_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions {
            cancellation,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("yielding WASIX process should be cancelled");
    join.join().expect("canceller");

    assert!(matches!(error, SandboxWasiError::Cancelled));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "WASIX cancellation should not wait for the original command timeout"
    );
}

#[test]
fn wasix_runner_cancels_non_yielding_process_after_metered_shutdown() {
    let cancellation = SandboxCancellation::new();
    let canceller = cancellation.clone();
    let join = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        canceller.cancel();
    });
    let started = std::time::Instant::now();

    let error = super::run_wasix_module(
        wasix_non_yielding_loop_with_sched_yield_import_module().as_slice(),
        wasix_test_request(30_000),
        SandboxWasiRunOptions {
            cancellation,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("non-yielding WASIX process should stop through metered cancellation");
    join.join().expect("canceller");

    assert!(matches!(error, SandboxWasiError::Cancelled));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "non-yielding WASIX cancellation should be observed through metered signal checkpoints"
    );
}

#[test]
fn wasix_runner_cancels_thread_blocked_in_futex_wait() {
    let cancellation = SandboxCancellation::new();
    let canceller = cancellation.clone();
    let join = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        canceller.cancel();
    });
    let started = std::time::Instant::now();

    let error = super::run_wasix_module(
        wasix_futex_wait_forever_module().as_slice(),
        wasix_test_request(30_000),
        SandboxWasiRunOptions {
            cancellation,
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("WASIX futex wait should be cancelled");
    join.join().expect("canceller");

    assert!(matches!(error, SandboxWasiError::Cancelled));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "blocked WASIX futex wait should be woken by cancellation"
    );
}

#[test]
fn wasix_runner_rejects_unmapped_cwd_when_preopens_exist() {
    let host_dir = wasix_unique_test_dir("sinomo-sandbox-wasix-cwd-unmapped");

    let error = super::run_wasix_module(
        wasix_noop_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: host_dir.to_path_buf(),
                guest_path: SandboxPath::new("/tmp").expect("guest"),
                mode: SandboxMountMode::ReadWrite,
            }],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("unmapped cwd should be rejected before WASIX starts");

    assert!(format!("{error}").contains("WASIX cwd `/work` is not mounted"));
}

#[test]
fn wasix_runner_rejects_file_cwd() {
    let host_dir = wasix_unique_test_dir("sinomo-sandbox-wasix-cwd-file");
    fs::write(host_dir.join("not-a-dir"), b"file").expect("cwd file");
    let mut request = wasix_test_request(1_000);
    request.cwd = SandboxPath::new("/work/not-a-dir").expect("cwd");

    let error = super::run_wasix_module(
        wasix_noop_module().as_slice(),
        request,
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: host_dir.to_path_buf(),
                guest_path: SandboxPath::new("/work").expect("guest"),
                mode: SandboxMountMode::ReadWrite,
            }],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("file cwd should be rejected before WASIX starts");

    let message = format!("{error}");
    assert!(
        message.contains("is not a directory"),
        "unexpected error: {message}"
    );
}

#[test]
fn wasix_runner_rejects_file_cwd_inside_virtual_preopen() {
    let runtime_root = wasix_unique_test_dir("sinomo-sandbox-wasix-virtual-cwd-file");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_wasix_virtual_cwd_file").expect("sandbox id");
    let layout = LocalSandboxInstanceLayout::new(&runtime_root, &sandbox_id);
    for dir in layout.required_dirs() {
        fs::create_dir_all(dir).expect("instance dir");
    }
    let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
    local_fs
        .write(&SandboxPath::new("/work/not-a-dir").expect("path"), b"file")
        .expect("cwd file");
    let local_fs = Arc::new(Mutex::new(local_fs));
    let mut request = wasix_test_request(1_000);
    request.cwd = SandboxPath::new("/work/not-a-dir").expect("cwd");

    let error = super::run_wasix_module(
        wasix_noop_module().as_slice(),
        request,
        SandboxWasiRunOptions {
            preopened_dirs: Vec::new(),
            virtual_preopened_dirs: vec![super::wasix_sandbox_filesystem_preopen(
                SandboxPath::new("/work").expect("guest"),
                Arc::clone(&local_fs),
                SandboxPath::new("/work").expect("root"),
                SandboxMountMode::ReadWrite,
            )],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("virtual file cwd should be rejected before WASIX starts");

    let message = format!("{error}");
    assert!(
        message.contains("is not a directory"),
        "unexpected error: {message}"
    );
}

#[test]
fn wasix_runner_rejects_missing_cwd_inside_virtual_preopen() {
    let runtime_root = wasix_unique_test_dir("sinomo-sandbox-wasix-virtual-cwd-missing");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_wasix_virtual_cwd_missing").expect("sandbox id");
    let layout = LocalSandboxInstanceLayout::new(&runtime_root, &sandbox_id);
    for dir in layout.required_dirs() {
        fs::create_dir_all(dir).expect("instance dir");
    }
    let local_fs = Arc::new(Mutex::new(LocalSandboxFileSystem::new(layout)));
    let mut request = wasix_test_request(1_000);
    request.cwd = SandboxPath::new("/work/missing").expect("cwd");

    let error = super::run_wasix_module(
        wasix_noop_module().as_slice(),
        request,
        SandboxWasiRunOptions {
            preopened_dirs: Vec::new(),
            virtual_preopened_dirs: vec![super::wasix_sandbox_filesystem_preopen(
                SandboxPath::new("/work").expect("guest"),
                Arc::clone(&local_fs),
                SandboxPath::new("/work").expect("root"),
                SandboxMountMode::ReadWrite,
            )],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("missing virtual cwd should be rejected before WASIX starts");

    assert!(format!("{error}").contains("is invalid"));
}

#[cfg(unix)]
#[test]
fn wasix_runner_rejects_symlink_escape_cwd() {
    let host_dir = wasix_unique_test_dir("sinomo-sandbox-wasix-cwd-symlink-root");
    let outside_dir = wasix_unique_test_dir("sinomo-sandbox-wasix-cwd-symlink-outside");
    std::os::unix::fs::symlink(&outside_dir, host_dir.join("outside")).expect("symlink");
    let mut request = wasix_test_request(1_000);
    request.cwd = SandboxPath::new("/work/outside").expect("cwd");

    let error = super::run_wasix_module(
        wasix_noop_module().as_slice(),
        request,
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: host_dir.to_path_buf(),
                guest_path: SandboxPath::new("/work").expect("guest"),
                mode: SandboxMountMode::ReadWrite,
            }],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("symlink escape cwd should be rejected before WASIX starts");

    assert!(format!("{error}").contains("WASIX preopen contains symlink"));
}

#[cfg(unix)]
#[test]
fn wasix_runner_rejects_symlink_inside_mapped_preopen() {
    let host_dir = wasix_unique_test_dir("sinomo-sandbox-wasix-preopen-symlink-root");
    let outside_dir = wasix_unique_test_dir("sinomo-sandbox-wasix-preopen-symlink-outside");
    std::os::unix::fs::symlink(outside_dir.join("secret.txt"), host_dir.join("link"))
        .expect("symlink");

    let error = super::run_wasix_module(
        wasix_noop_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions {
            preopened_dirs: vec![SandboxWasiPreopen {
                host_path: host_dir.to_path_buf(),
                guest_path: SandboxPath::new("/work").expect("guest"),
                mode: SandboxMountMode::ReadWrite,
            }],
            ..SandboxWasiRunOptions::default()
        },
    )
    .expect_err("symlink inside mapped preopen should be rejected before WASIX starts");

    assert!(format!("{error}").contains("WASIX preopen contains symlink"));
}

#[test]
fn wasix_runner_allows_socket_imports_for_wasix_libc_compatibility() {
    let outcome = super::run_wasix_module(
        wasix_sock_open_import_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("socket imports alone should not block process-capable toolchains");

    assert_eq!(outcome.response.exit_status, 0);
}

#[test]
fn wasix_runner_rejects_raw_http_imports() {
    let error = super::run_wasix_module(
        wasix_http_client_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions::default(),
    )
    .expect_err("raw WASIX HTTP imports should be rejected before execution");

    let message = format!("{error}");
    assert!(message.contains("wasix_http_client_v1"));
    assert!(message.contains("mediated sandbox fetch"));
}

#[test]
fn wasix_runner_allows_passive_port_imports_for_process_toolchains() {
    let outcome = super::run_wasix_module(
        wasix_passive_port_import_module().as_slice(),
        wasix_test_request(1_000),
        SandboxWasiRunOptions::default(),
    )
    .expect("passive port imports should not block process-capable toolchains");

    assert_eq!(outcome.response.exit_status, 0);
}

#[test]
fn wasix_memory_budget_accepts_bounded_module_memory() {
    let wasm = wat::parse_str(
        r#"
            (module
              (memory 1 2)
              (func (export "_start")))
            "#,
    )
    .expect("wat");

    validate_wasix_module_memory_budget(&wasm, 2 * 64 * 1024).expect("memory budget");
}

fn wasix_test_request(timeout_ms: u64) -> SandboxCommandRequest {
    SandboxCommandRequest {
        sandbox_id: crate::SandboxId::generate(),
        argv: vec!["yield.wasm".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        stdin: None,
        env: BTreeMap::new(),
        timeout_ms,
    }
}

fn wasix_unique_test_dir(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(name)
}

fn wasix_noop_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (memory (export "memory") 1 2)
              (func (export "_start")))
            "#,
    )
    .expect("noop wasix module")
}

fn wasix_sock_open_import_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (import "wasix_32v1" "sock_open"
                (func $sock_open (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1 2)
              (func (export "_start")))
            "#,
    )
    .expect("sock_open import wasix module")
}

fn wasix_http_client_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (import "wasix_http_client_v1" "request"
                (func $request (param i32 i32) (result i32)))
              (memory (export "memory") 1 2)
              (func (export "_start")
                i32.const 0
                i32.const 0
                call $request
                drop))
            "#,
    )
    .expect("http wasix module")
}

fn wasix_passive_port_import_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (import "wasix_32v1" "port_mac"
                (func $port_mac (param i32) (result i32)))
              (memory (export "memory") 1 2)
              (func (export "_start")))
            "#,
    )
    .expect("passive port import wasix module")
}

fn wasix_yield_forever_module() -> Vec<u8> {
    wat::parse_str(
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
    .expect("yielding wasix module")
}

fn wasix_non_yielding_loop_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (memory (export "memory") 1 2)
              (func (export "_start")
                (loop $again
                  br $again)))
            "#,
    )
    .expect("non-yielding wasix module")
}

fn wasix_non_yielding_loop_with_sched_yield_import_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (import "wasix_32v1" "sched_yield"
                (func $sched_yield (result i32)))
              (memory (export "memory") 1 2)
              (func (export "_start")
                (loop $again
                  br $again)))
            "#,
    )
    .expect("non-yielding wasix module with sched_yield import")
}

fn wasix_futex_wait_forever_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (import "wasix_32v1" "futex_wait"
                (func $futex_wait (param i32 i32 i32 i32) (result i32)))
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
              (func (export "_start")
                i32.const 200
                i32.const 0
                i32.store
                i32.const 208
                i32.const 0
                i32.store
                i32.const 200
                i32.const 0
                i32.const 208
                i32.const 204
                call $futex_wait
                call $exit_if_errno))
            "#,
    )
    .expect("futex wait wasix module")
}

fn wasix_unreachable_module() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (memory (export "memory") 1 2)
              (func (export "_start")
                unreachable))
            "#,
    )
    .expect("unreachable wasix module")
}

fn fpcast_adapter_for_test(
    store: &mut Store,
    func: Function,
    universal_type: &FunctionType,
) -> Function {
    let func_ty = func.ty(&store.as_store_ref());
    let adapter_wat = fpcast_adapter_wat_for_test(&func_ty, universal_type);
    let adapter_module =
        Module::new(&store.as_store_ref(), adapter_wat.as_bytes()).expect("fpcast adapter module");
    let mut imports = Imports::new();
    imports.define("env", "target", func);
    let adapter_instance =
        Instance::new(store, &adapter_module, &imports).expect("fpcast adapter instance");
    adapter_instance
        .exports
        .get_function("adapter")
        .expect("fpcast adapter export")
        .clone()
}

fn fpcast_adapter_wat_for_test(
    source_type: &FunctionType,
    universal_type: &FunctionType,
) -> String {
    let mut wat = String::new();
    wat.push_str("(module\n");
    wat.push_str("  (import \"env\" \"target\" (func $target");
    for ty in source_type.params() {
        wat.push_str(" (param ");
        wat.push_str(fpcast_type_to_wat_for_test(*ty));
        wat.push(')');
    }
    for ty in source_type.results() {
        wat.push_str(" (result ");
        wat.push_str(fpcast_type_to_wat_for_test(*ty));
        wat.push(')');
    }
    wat.push_str("))\n");

    wat.push_str("  (func (export \"adapter\")");
    for index in 0..universal_type.params().len() {
        wat.push_str(&format!(" (param $p{index} i64)"));
    }
    wat.push_str(" (result i64)\n");

    for (index, ty) in source_type.params().iter().enumerate() {
        if index < universal_type.params().len() {
            wat.push_str(&format!("    local.get $p{index}\n"));
        } else {
            wat.push_str("    i64.const 0\n");
        }
        match ty {
            Type::I32 => wat.push_str("    i32.wrap_i64\n"),
            Type::I64 => {}
            Type::F32 => wat.push_str("    i32.wrap_i64\n    f32.reinterpret_i32\n"),
            Type::F64 => wat.push_str("    f64.reinterpret_i64\n"),
            other => panic!("unsupported fpcast adapter argument type {other:?}"),
        }
    }

    wat.push_str("    call $target\n");
    match source_type.results().first().copied() {
        None => wat.push_str("    i64.const 0\n"),
        Some(Type::I32) => wat.push_str("    i64.extend_i32_u\n"),
        Some(Type::I64) => {}
        Some(Type::F32) => wat.push_str("    i32.reinterpret_f32\n    i64.extend_i32_u\n"),
        Some(Type::F64) => wat.push_str("    i64.reinterpret_f64\n"),
        Some(other) => panic!("unsupported fpcast adapter return type {other:?}"),
    }
    wat.push_str("  )\n");
    wat.push_str(")\n");
    wat
}

fn fpcast_type_to_wat_for_test(ty: Type) -> &'static str {
    match ty {
        Type::I32 => "i32",
        Type::I64 => "i64",
        Type::F32 => "f32",
        Type::F64 => "f64",
        other => panic!("unsupported fpcast adapter value type {other:?}"),
    }
}

#[test]
fn wasix_memory_budget_rejects_unbounded_module_memory() {
    let wasm = wat::parse_str(
        r#"
            (module
              (memory 1)
              (func (export "_start")))
            "#,
    )
    .expect("wat");

    let error = validate_wasix_module_memory_budget(&wasm, 2 * 64 * 1024)
        .expect_err("unbounded memory should be rejected");

    assert!(format!("{error}").contains("unbounded"));
}

#[test]
fn wasix_memory_budget_rejects_maximum_over_limit() {
    let wasm = wat::parse_str(
        r#"
            (module
              (memory 1 3)
              (func (export "_start")))
            "#,
    )
    .expect("wat");

    let error = validate_wasix_module_memory_budget(&wasm, 2 * 64 * 1024)
        .expect_err("oversized max memory should be rejected");

    assert!(matches!(
        error,
        SandboxWasiError::MemoryLimitExceeded { .. }
    ));
}
