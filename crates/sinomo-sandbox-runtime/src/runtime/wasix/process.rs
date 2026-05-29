use crate::{SANDBOX_WASIX_MAX_TASKS, SandboxWasiError};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use wasmer::{Global, Value};
use wasmer_wasix::{
    WasiRuntimeError,
    bin_factory::run_exec,
    runners::wasi::{PackageOrHash, RuntimeOrEngine, WasiRunner},
    runtime::task_manager::TaskWasm,
    runtime::task_manager::VirtualTaskManagerExt,
    wasmer_wasix_types::wasi::{Disposition, ExitCode, Signal, SignalDisposition},
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
    task_limit: usize,
    cancellation: crate::SandboxCancellation,
) -> Result<Result<ExitCode, Arc<WasiRuntimeError>>, SandboxWasiError> {
    let wasi = webc::metadata::annotations::Wasi::new(program_name);
    let mut builder = runner
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
    configure_default_ignored_signals(&mut builder);
    let env = builder.build().map_err(|error| SandboxWasiError::Runtime {
        detail: format!("build WASIX environment: {error}"),
    })?;
    env.process
        .set_executable_path(if program_name.contains('/') {
            program_name.to_string()
        } else {
            format!("/bin/{program_name}")
        });
    let runtime = env.runtime.clone();
    let tasks = runtime.task_manager().clone();
    let process = env.process.clone();
    process.set_sandbox_resource_limits(memory_limit_bytes as u64, timeout_ms, task_limit as u64);
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
                let mut detail = format!("WASIX runtime error: {error}");
                if let WasiRuntimeError::Runtime(runtime_error) = error.as_ref() {
                    if let Some(trap) = runtime_error.clone().to_trap() {
                        detail.push_str(&format!("; trap: {trap:?}"));
                    }
                    let trace = runtime_error.trace();
                    if !trace.is_empty() {
                        detail.push_str("; wasm trace:");
                        for frame in trace.iter().take(24) {
                            detail.push_str(&format!(
                                " {}:{}",
                                frame.module_name(),
                                frame.function_name().unwrap_or("<unknown>")
                            ));
                        }
                        if trace.len() > 24 {
                            detail.push_str(&format!(" ... +{} frames", trace.len() - 24));
                        }
                    }
                }
                Err(SandboxWasiError::Runtime { detail })
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn sandbox_wasix_capabilities() -> wasmer_wasix::capabilities::Capabilities {
    sandbox_wasix_capabilities_with_task_limit(SANDBOX_WASIX_MAX_TASKS)
}

pub(crate) fn sandbox_wasix_capabilities_with_task_limit(
    task_limit: usize,
) -> wasmer_wasix::capabilities::Capabilities {
    let mut capabilities = wasmer_wasix::capabilities::Capabilities::new();
    capabilities.threading.max_threads = Some(task_limit);
    capabilities
}

pub(crate) fn effective_sandbox_wasix_task_limit() -> usize {
    SANDBOX_WASIX_MAX_TASKS.min(host_cgroup_task_capacity().unwrap_or(SANDBOX_WASIX_MAX_TASKS))
}

#[cfg(target_os = "linux")]
fn host_cgroup_task_capacity() -> Option<usize> {
    const HOST_TASK_RESERVE: usize = 16;
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroup.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("0"), Some(""), Some(path)) => Some(path.trim_start_matches('/')),
            _ => None,
        }
    })?;
    let mut path = PathBuf::from("/sys/fs/cgroup");
    if !relative.is_empty() {
        path.push(relative);
    }

    let mut best_available = None::<usize>;
    loop {
        let max_path = path.join("pids.max");
        let current_path = path.join("pids.current");
        if let (Some(max), Some(current)) = (
            read_cgroup_pids_max(&max_path),
            read_cgroup_pids_current(&current_path),
        ) {
            let available = max.saturating_sub(current);
            best_available = Some(match best_available {
                Some(best) => best.min(available),
                None => available,
            });
        }
        if path == PathBuf::from("/sys/fs/cgroup") {
            break;
        }
        if !path.pop() {
            break;
        }
    }

    best_available.map(|available| {
        let capacity = available.saturating_sub(HOST_TASK_RESERVE).max(3);
        capacity.min(available.max(1))
    })
}

#[cfg(not(target_os = "linux"))]
fn host_cgroup_task_capacity() -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
fn read_cgroup_pids_max(path: &std::path::Path) -> Option<usize> {
    let value = std::fs::read_to_string(path).ok()?;
    let value = value.trim();
    if value == "max" {
        None
    } else {
        value.parse().ok()
    }
}

#[cfg(target_os = "linux")]
fn read_cgroup_pids_current(path: &std::path::Path) -> Option<usize> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn configure_default_ignored_signals(builder: &mut wasmer_wasix::WasiEnvBuilder) {
    for sig in [Signal::Sigchld, Signal::Sigurg, Signal::Sigwinch] {
        builder.add_signal(SignalDisposition {
            sig,
            disp: Disposition::Ignore,
        });
    }
}
