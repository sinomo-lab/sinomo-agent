use crate::{SANDBOX_WASIX_MAX_TASKS, SandboxWasiError};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use wasmer::{Global, Value};
use wasmer_wasix::{
    WasiRuntimeError,
    bin_factory::run_exec,
    runners::wasi::{PackageOrHash, RuntimeOrEngine, WasiRunner},
    runtime::task_manager::TaskWasm,
    runtime::task_manager::VirtualTaskManagerExt,
    wasmer_wasix_types::wasi::ExitCode,
};

use super::budget::{WasixProcessBudgetGuard, run_with_host_wall_timeout};
use super::metering::is_wasix_metering_exhaustion;

pub(super) fn run_wasix_process_tree(
    runner: &WasiRunner,
    runtime_or_engine: RuntimeOrEngine,
    program_name: &str,
    module: wasmer::Module,
    module_hash: wasmer_types::ModuleHash,
    memory_limit_bytes: usize,
    timeout_ms: u64,
    cancellation: crate::SandboxCancellation,
) -> Result<Result<ExitCode, Arc<WasiRuntimeError>>, SandboxWasiError> {
    let wasi = webc::metadata::annotations::Wasi::new(program_name);
    let builder = runner
        .prepare_webc_env(
            program_name,
            &wasi,
            PackageOrHash::Hash(module_hash),
            runtime_or_engine,
            None,
        )
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!("prepare WASIX environment: {error}"),
        })?;
    let env = builder.build().map_err(|error| SandboxWasiError::Runtime {
        detail: format!("build WASIX environment: {error}"),
    })?;
    let runtime = env.runtime.clone();
    let tasks = runtime.task_manager().clone();
    let process = env.process.clone();
    process.set_sandbox_resource_limits(
        memory_limit_bytes as u64,
        timeout_ms,
        SANDBOX_WASIX_MAX_TASKS as u64,
    );
    env.sync_process_resource_limits();
    let budget_guard = WasixProcessBudgetGuard::start(timeout_ms, cancellation, process);
    let timeout_process = env.process.clone();

    let mut task_handle = env.thread.join_handle();
    let metering_global = Arc::new(Mutex::new(None::<Global>));
    let metering_exhausted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let metering_global_pre_run = Arc::clone(&metering_global);
    let metering_global_recycle = Arc::clone(&metering_global);
    let metering_exhausted_recycle = Arc::clone(&metering_exhausted);
    tasks
        .task_wasm(
            TaskWasm::new(Box::new(run_exec), env, module, true, true)
                .with_pre_run(Box::new(move |ctx, store| {
                    Box::pin(async move {
                        if let Some(instance) = ctx.data(store).try_clone_instance() {
                            if let Ok(global) = instance
                                .exports
                                .get_global("wasmer_metering_points_exhausted")
                            {
                                *metering_global_pre_run.lock().expect("metering global") =
                                    Some(global.clone());
                            }
                        }
                    })
                }))
                .with_recycle(Box::new(move |mut props| {
                    if let Some(global) = metering_global_recycle
                        .lock()
                        .expect("metering global")
                        .clone()
                    {
                        metering_exhausted_recycle.store(
                            matches!(global.get(&mut props.store), Value::I32(1)),
                            std::sync::atomic::Ordering::SeqCst,
                        );
                    }
                })),
        )
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!("spawn WASIX module: {error}"),
        })?;
    let wait_result = run_with_host_wall_timeout(
        timeout_ms,
        budget_guard.done_handle(),
        budget_guard.termination_reason_handle(),
        timeout_process,
        move || {
            tasks
                .spawn_and_block_on(async move { task_handle.wait_finished().await })
                .map_err(|error| SandboxWasiError::Runtime {
                    detail: format!("wait for WASIX process tree: {error}"),
                })
        },
    );
    budget_guard.finish();
    let wait_result = wait_result??;
    if let Some(error) = budget_guard.termination_error(
        timeout_ms,
        metering_exhausted.load(std::sync::atomic::Ordering::SeqCst),
    ) {
        Err(error)
    } else {
        Ok(wait_result)
    }
}

pub(super) fn wasix_exit_status(
    result: Result<ExitCode, Arc<WasiRuntimeError>>,
    timeout_ms: u64,
) -> Result<i32, SandboxWasiError> {
    match result {
        Ok(code) => Ok(code.raw()),
        Err(error) => {
            if let Some(code) = error.as_exit_code() {
                Ok(code.raw())
            } else if is_wasix_metering_exhaustion(&error.to_string()) {
                Err(SandboxWasiError::TimedOut { timeout_ms })
            } else {
                Err(SandboxWasiError::Runtime {
                    detail: format!("WASIX runtime error: {error}"),
                })
            }
        }
    }
}

pub(crate) fn wasix_command_exceeded_requested_timeout(elapsed: Duration, timeout_ms: u64) -> bool {
    elapsed >= Duration::from_millis(timeout_ms)
}

pub(crate) fn sandbox_wasix_capabilities() -> wasmer_wasix::capabilities::Capabilities {
    let mut capabilities = wasmer_wasix::capabilities::Capabilities::new();
    capabilities.threading.max_threads = Some(SANDBOX_WASIX_MAX_TASKS);
    capabilities
}
