use crate::{
    SandboxCommandRequest, SandboxLocalCommandOutcome, SandboxWasiError, SandboxWasiRunOptions,
};

#[cfg(feature = "wasix-runtime")]
use super::memory::validate_wasix_module_memory_budget;
#[cfg(feature = "wasix-runtime")]
use super::metering::{wasix_engine, wasix_module_supports_instruction_metering};
#[cfg(feature = "wasix-runtime")]
use super::network::validate_wasix_module_network_imports;
#[cfg(feature = "wasix-runtime")]
use super::output::WasixStreamingOutputCapture;
#[cfg(feature = "wasix-runtime")]
use super::preopen::{validate_wasix_current_dir, validate_wasix_preopen_tree};
#[cfg(feature = "wasix-runtime")]
use super::process::{
    run_wasix_process_tree, sandbox_wasix_capabilities, wasix_command_exceeded_requested_timeout,
    wasix_exit_status,
};
#[cfg(feature = "wasix-runtime")]
use super::setup::{configure_wasix_mapped_directories, configure_wasix_stdin, wasix_environment};
#[cfg(feature = "wasix-runtime")]
use super::tunables::SandboxWasixTunables;
#[cfg(feature = "wasix-runtime")]
use crate::{
    SandboxCommandId, SandboxOutputStream, SandboxResourceUsage, sandbox_command_stderr_path,
    sandbox_command_stdout_path, validate_wasi_request,
};
#[cfg(feature = "wasix-runtime")]
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};
#[cfg(feature = "wasix-runtime")]
use wasmer::sys::NativeEngineExt;
#[cfg(feature = "wasix-runtime")]
use wasmer_wasix::runners::wasi::RuntimeOrEngine;
#[cfg(feature = "wasix-runtime")]
use wasmer_wasix::runtime::Runtime;

#[cfg(feature = "wasix-runtime")]
const SINOMO_WASIX_STACK_SIZE: usize = 16 * 1024 * 1024;

#[cfg(feature = "wasix-runtime")]
fn bundled_wasm_opt_path() -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("toolchains")
        .join("python")
        .join("work-wasix")
        .join("binaryen-version_124")
        .join("bin")
        .join("wasm-opt");
    path.is_file().then_some(path)
}

#[cfg(feature = "wasix-runtime")]
fn looks_like_threaded_wasm(wasm: &[u8]) -> bool {
    wasm.starts_with(b"\0asm")
        && wasm
            .windows(b"wasi_thread_start".len())
            .any(|window| window == b"wasi_thread_start")
}

#[cfg(feature = "wasix-runtime")]
fn module_has_export(wasm: &[u8], export_name: &str) -> bool {
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        let Ok(payload) = payload else {
            return false;
        };
        if let wasmparser::Payload::ExportSection(exports) = payload {
            for export in exports {
                let Ok(export) = export else {
                    return false;
                };
                if export.name == export_name {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(feature = "wasix-runtime")]
fn module_imports_any(wasm: &[u8], module_name: &str, names: &[&str]) -> bool {
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        let Ok(payload) = payload else {
            return false;
        };
        if let wasmparser::Payload::ImportSection(imports) = payload {
            for import in imports.into_imports() {
                let Ok(import) = import else {
                    return false;
                };
                if import.module == module_name && names.contains(&import.name) {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(feature = "wasix-runtime")]
fn needs_asyncify_transform(wasm: &[u8]) -> bool {
    wasm.starts_with(b"\0asm")
        && !module_has_export(wasm, "asyncify_start_unwind")
        && module_imports_any(
            wasm,
            "wasix_32v1",
            &[
                "fork",
                "vfork",
                "proc_fork",
                "proc_fork_env",
                "clone_thread",
            ],
        )
}

#[cfg(feature = "wasix-runtime")]
fn transform_wasix_wasm(
    wasm: &[u8],
    auto_asyncify: bool,
    fpcast_emu: bool,
) -> Result<Option<Vec<u8>>, SandboxWasiError> {
    let apply_fpcast = fpcast_emu && looks_like_threaded_wasm(wasm);
    let apply_asyncify = auto_asyncify && needs_asyncify_transform(wasm);
    if !apply_fpcast && !apply_asyncify {
        return Ok(None);
    }
    let Some(wasm_opt) = bundled_wasm_opt_path() else {
        if apply_asyncify {
            return Err(SandboxWasiError::Runtime {
                detail: "WASIX module imports fork/clone APIs but is not asyncified, and bundled Binaryen wasm-opt is missing".to_string(),
            });
        }
        return Ok(None);
    };
    let temp_dir = std::env::temp_dir().join(format!(
        "sinomo-wasix-transform-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    std::fs::create_dir_all(&temp_dir).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create WASIX transform temp dir: {error}"),
    })?;
    let cleanup = |path: &Path| {
        let _ = std::fs::remove_dir_all(path);
    };
    let input_path = temp_dir.join("input.wasm");
    let output_path = temp_dir.join("output.wasm");
    if let Err(error) = std::fs::write(&input_path, wasm) {
        cleanup(&temp_dir);
        return Err(SandboxWasiError::Runtime {
            detail: format!("write WASIX transform input: {error}"),
        });
    }
    let mut command = Command::new(&wasm_opt);
    if apply_asyncify {
        command.arg("--asyncify");
    }
    if apply_fpcast {
        command.arg("--fpcast-emu");
    }
    let output = command
        .arg("--pass-arg=max-func-params@64")
        .arg("--no-validation")
        .arg("--enable-bulk-memory")
        .arg("--enable-mutable-globals")
        .arg("--enable-threads")
        .arg("--enable-exception-handling")
        .arg("--enable-simd")
        .arg("--enable-relaxed-simd")
        .arg("--enable-extended-const")
        .arg(&input_path)
        .arg("-o")
        .arg(&output_path)
        .output()
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!("run {}: {error}", wasm_opt.display()),
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        cleanup(&temp_dir);
        return Err(SandboxWasiError::Runtime {
            detail: format!("WASIX wasm-opt transform failed: {}", stderr.trim()),
        });
    }
    let transformed = std::fs::read(&output_path).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("read WASIX transform output: {error}"),
    })?;
    cleanup(&temp_dir);
    Ok(Some(transformed))
}

#[cfg(not(feature = "wasix-runtime"))]
pub(crate) fn run_wasix_module(
    _wasm: &[u8],
    _request: SandboxCommandRequest,
    _options: SandboxWasiRunOptions,
) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
    Err(SandboxWasiError::InvalidRequest {
        detail: "WASIX toolchain requested, but sinomo-sandbox was built without the `wasix-runtime` feature".to_string(),
    })
}

#[cfg(feature = "wasix-runtime")]
pub(crate) fn run_wasix_module(
    wasm: &[u8],
    request: SandboxCommandRequest,
    options: SandboxWasiRunOptions,
) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
    std::thread::scope(|scope| {
        let handle = std::thread::Builder::new()
            .name("sinomo-wasix-command".to_string())
            .stack_size(SINOMO_WASIX_STACK_SIZE)
            .spawn_scoped(scope, move || {
                run_wasix_module_on_worker_thread(wasm, request, options)
            })
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("spawn WASIX host thread: {error}"),
            })?;
        handle.join().map_err(|panic| {
            let detail = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            SandboxWasiError::Runtime {
                detail: format!("WASIX host thread panicked: {detail}"),
            }
        })?
    })
}

#[cfg(feature = "wasix-runtime")]
fn run_wasix_module_on_worker_thread(
    wasm: &[u8],
    request: SandboxCommandRequest,
    options: SandboxWasiRunOptions,
) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
    wasmer_vm::set_stack_size(SINOMO_WASIX_STACK_SIZE);
    validate_wasi_request(&request)?;
    if options.cancellation.is_cancelled() {
        return Err(SandboxWasiError::Cancelled);
    }
    let Some(program_name) = request.argv.first() else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "WASIX command argv is empty".to_string(),
        });
    };
    let command_id = SandboxCommandId::generate();
    let started = Instant::now();
    let transformed_wasm =
        transform_wasix_wasm(wasm, options.wasix_auto_asyncify, options.wasix_fpcast_emu)?;
    let wasm = transformed_wasm.as_deref().unwrap_or(wasm);
    validate_wasix_module_memory_budget(wasm, options.memory_limit_bytes)?;
    validate_wasix_module_network_imports(wasm)?;
    let instruction_metering =
        options.wasix_instruction_metering && wasix_module_supports_instruction_metering(wasm)?;
    let mut engine = wasix_engine(request.timeout_ms, instruction_metering);
    engine.set_tunables(SandboxWasixTunables::for_target(
        engine.as_sys().target(),
        options.memory_limit_bytes,
        SINOMO_WASIX_STACK_SIZE,
    ));
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(SINOMO_WASIX_STACK_SIZE)
        .enable_time()
        .build()
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create WASIX host-fs runtime: {error}"),
        })?;
    let tokio_enter = tokio_runtime.enter();
    let task_manager = std::sync::Arc::new(
        wasmer_wasix::runtime::task_manager::tokio::TokioTaskManager::new_with_wasm_thread_stack_size(
            tokio_runtime.handle().clone(),
            SINOMO_WASIX_STACK_SIZE,
        ),
    );
    let mut runtime = wasmer_wasix::runtime::PluggableRuntime::new(task_manager);
    runtime.set_engine(engine.clone());
    runtime.set_dynamic_linker_stack_size(SINOMO_WASIX_STACK_SIZE as u64);
    let runtime: std::sync::Arc<dyn Runtime + Send + Sync> = std::sync::Arc::new(runtime);
    let hashed_module = wasmer_wasix::runtime::module_cache::HashedModuleData::new(wasm.to_vec());
    let module_hash = *hashed_module.hash();
    let module = tokio_runtime
        .block_on(runtime.load_hashed_module(hashed_module, Some(&engine)))
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!("compile WASIX module: {error}"),
        })?;
    for preopen in &options.preopened_dirs {
        validate_wasix_preopen_tree(preopen)?;
    }
    validate_wasix_current_dir(&request.cwd, &options.preopened_dirs)?;
    let mut runner = wasmer_wasix::runners::wasi::WasiRunner::new();
    if let Some(metadata_host_path) = &options.metadata_host_path {
        runner.with_sinomo_metadata_root(metadata_host_path.clone());
    }
    runner.with_capabilities(sandbox_wasix_capabilities());
    runner.with_args(request.argv.iter().skip(1).cloned());
    let mut env = wasix_environment(
        &request,
        options.wasix_pinned_child_exec_only,
        &options.wasix_command_overrides,
    );
    env.push((
        "SINOMO_SANDBOX_MEMORY_LIMIT_BYTES".to_string(),
        options.memory_limit_bytes.to_string(),
    ));
    env.push((
        "SINOMO_SANDBOX_WALL_TIME_LIMIT_MS".to_string(),
        request.timeout_ms.to_string(),
    ));
    env.push((
        "SINOMO_SANDBOX_OUTPUT_LIMIT_BYTES".to_string(),
        options.output_limit_bytes.to_string(),
    ));
    env.push((
        "SINOMO_SANDBOX_WORKSPACE_LIMIT_BYTES".to_string(),
        options.workspace_limit_bytes.to_string(),
    ));
    env.push((
        "SINOMO_SANDBOX_FILE_COUNT_LIMIT".to_string(),
        options.file_count_limit.to_string(),
    ));
    runner.with_envs(env.clone());
    runner.with_current_dir(request.cwd.as_str());
    let runtime_mounts = configure_wasix_mapped_directories(
        &mut runner,
        &request.argv,
        &env,
        &options.preopened_dirs,
        options.wasix_pinned_child_exec_only,
        options.enable_virtual_devices,
        options.zero_device_bytes,
    )?;
    let stdin_input = configure_wasix_stdin(
        &mut runner,
        request.stdin.clone(),
        options.wasix_stdin_pipe.clone(),
        options.input_limit_bytes,
    )?;

    let stdout_capture = WasixStreamingOutputCapture::new(
        request.sandbox_id.clone(),
        command_id.clone(),
        SandboxOutputStream::Stdout,
        options.output_limit_bytes,
        options.output_sink.clone(),
        options.wasix_stdout_pipe.clone(),
    );
    let stderr_capture = WasixStreamingOutputCapture::new(
        request.sandbox_id.clone(),
        command_id.clone(),
        SandboxOutputStream::Stderr,
        options.output_limit_bytes,
        options.output_sink.clone(),
        None,
    );
    let stdout_file = virtual_fs::ArcFile::new(Box::new(stdout_capture.file()));
    let stderr_file = virtual_fs::ArcFile::new(Box::new(stderr_capture.file()));
    runner
        .with_stdout(Box::new(stdout_file.clone()))
        .with_stderr(Box::new(stderr_file.clone()));

    let run_result = run_wasix_process_tree(
        &runner,
        RuntimeOrEngine::Runtime(runtime),
        program_name,
        module,
        module_hash,
        options.memory_limit_bytes,
        request.timeout_ms,
        options.cancellation.clone(),
    );
    drop(tokio_enter);
    drop(runtime_mounts);
    let elapsed = started.elapsed();

    if stdout_capture.output_limit_exceeded()? || stderr_capture.output_limit_exceeded()? {
        return Err(SandboxWasiError::OutputLimitExceeded {
            limit: options.output_limit_bytes,
        });
    }
    if let Some(input) = &stdin_input {
        if let Some(limit) = input.input_limit_exceeded()? {
            return Err(SandboxWasiError::InputLimitExceeded { limit });
        }
    }
    if wasix_command_exceeded_requested_timeout(elapsed, request.timeout_ms) {
        return Err(SandboxWasiError::TimedOut {
            timeout_ms: request.timeout_ms,
        });
    }

    let exit_status = wasix_exit_status(run_result?, request.timeout_ms)?;

    let stdout = stdout_capture.bytes()?;
    let stderr = stderr_capture.bytes()?;
    if stdout.len() > options.output_limit_bytes || stderr.len() > options.output_limit_bytes {
        return Err(SandboxWasiError::OutputLimitExceeded {
            limit: options.output_limit_bytes,
        });
    }
    let stdout_ref = sandbox_command_stdout_path(&request.sandbox_id, &command_id);
    let stderr_ref = sandbox_command_stderr_path(&request.sandbox_id, &command_id);
    let mut output_chunks = stdout_capture.chunks()?;
    output_chunks.extend(stderr_capture.chunks()?);
    Ok(SandboxLocalCommandOutcome {
        response: crate::SandboxCommandResponse {
            command_id,
            exit_status,
            stdout_ref,
            stderr_ref,
            changed_paths: Vec::new(),
            resource_usage: SandboxResourceUsage {
                wall_time_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                peak_memory_bytes: 0,
                stdout_bytes: stdout.len() as u64,
                stderr_bytes: stderr.len() as u64,
                written_bytes: 0,
            },
        },
        stdout,
        stderr,
        output_chunks,
    })
}
