use super::{
    guest::{
        read_guest_cstring_array, read_guest_env_array, read_guest_optional_string,
        read_guest_string,
    },
    paths::{
        sandbox_wasi_extension_guest_path, sandbox_wasi_extension_host_path,
        sandbox_wasi_extension_read_guest_file_with_limit,
        sandbox_wasi_extension_read_host_path_with_limit,
        sandbox_wasi_extension_validate_guest_file_write_target,
        sandbox_wasi_extension_write_guest_file,
    },
    runner::{guest_path_for_wasi, run_wasi_start},
    state::LocalWasiState,
};
use crate::internal::sandbox_wasi_error_from_wasmtime;
use crate::{SandboxMountMode, SandboxPath, SandboxWasiError, SandboxWasiPreopen};
use std::{collections::BTreeMap, fs, sync::atomic::Ordering};

const SINOMO_SANDBOX_WASI_MAX_CHILD_DEPTH: usize = 16;

pub(super) fn sinomo_sandbox_wasi_proc_spawn_wait(
    caller: &mut wasmtime::Caller<'_, LocalWasiState>,
    argv_ptr: i32,
    envp_ptr: i32,
) -> i32 {
    sinomo_sandbox_wasi_proc_spawn_wait_impl(
        caller,
        argv_ptr,
        envp_ptr,
        None,
        ChildProcessStdio::default(),
    )
}

pub(super) fn sinomo_sandbox_wasi_proc_spawn_wait_at(
    caller: &mut wasmtime::Caller<'_, LocalWasiState>,
    argv_ptr: i32,
    envp_ptr: i32,
    cwd_ptr: i32,
    cwd_len: i32,
) -> i32 {
    if cwd_ptr <= 0 || cwd_len < 0 {
        return 127 << 8;
    }
    let Some(memory) = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
    else {
        return 127 << 8;
    };
    let Ok(cwd) =
        read_guest_string(caller, &memory, cwd_ptr as usize, cwd_len as usize).and_then(|path| {
            SandboxPath::new(path).map_err(|error| SandboxWasiError::InvalidRequest {
                detail: format!("invalid child cwd: {error}"),
            })
        })
    else {
        return 127 << 8;
    };
    sinomo_sandbox_wasi_proc_spawn_wait_impl(
        caller,
        argv_ptr,
        envp_ptr,
        Some(cwd),
        ChildProcessStdio::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn sinomo_sandbox_wasi_proc_spawn_wait_at_io(
    caller: &mut wasmtime::Caller<'_, LocalWasiState>,
    argv_ptr: i32,
    envp_ptr: i32,
    cwd_ptr: i32,
    cwd_len: i32,
    stdin_path_ptr: i32,
    stdin_path_len: i32,
    stdout_path_ptr: i32,
    stdout_path_len: i32,
    stdout_append: i32,
    stderr_path_ptr: i32,
    stderr_path_len: i32,
    stderr_append: i32,
    stderr_to_stdout: i32,
) -> i32 {
    if cwd_ptr <= 0
        || cwd_len < 0
        || stdin_path_len < 0
        || stdout_path_len < 0
        || stderr_path_len < 0
    {
        return 127 << 8;
    }
    let Some(memory) = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
    else {
        return 127 << 8;
    };
    let Ok(cwd) =
        read_guest_string(caller, &memory, cwd_ptr as usize, cwd_len as usize).and_then(|path| {
            SandboxPath::new(path).map_err(|error| SandboxWasiError::InvalidRequest {
                detail: format!("invalid child cwd: {error}"),
            })
        })
    else {
        return 127 << 8;
    };
    let Ok(stdin_path) =
        read_guest_optional_string(caller, &memory, stdin_path_ptr, stdin_path_len)
    else {
        return 127 << 8;
    };
    let Ok(stdout_path) =
        read_guest_optional_string(caller, &memory, stdout_path_ptr, stdout_path_len)
    else {
        return 127 << 8;
    };
    let Ok(stderr_path) =
        read_guest_optional_string(caller, &memory, stderr_path_ptr, stderr_path_len)
    else {
        return 127 << 8;
    };
    let stdio = ChildProcessStdio {
        stdin_path,
        stdout_path,
        stdout_append: stdout_append != 0,
        stderr_path,
        stderr_append: stderr_append != 0,
        stderr_to_stdout: stderr_to_stdout != 0,
    };
    if stdio.stderr_to_stdout && stdio.stderr_path.is_some() {
        return 127 << 8;
    }
    sinomo_sandbox_wasi_proc_spawn_wait_impl(caller, argv_ptr, envp_ptr, Some(cwd), stdio)
}

fn sinomo_sandbox_wasi_proc_spawn_wait_impl(
    caller: &mut wasmtime::Caller<'_, LocalWasiState>,
    argv_ptr: i32,
    envp_ptr: i32,
    cwd: Option<SandboxPath>,
    stdio: ChildProcessStdio,
) -> i32 {
    if argv_ptr <= 0 || envp_ptr < 0 {
        return 127 << 8;
    }
    if !caller.data().allow_pinned_subprocesses {
        return 127 << 8;
    }
    if caller.data().child_depth >= SINOMO_SANDBOX_WASI_MAX_CHILD_DEPTH {
        return 127 << 8;
    }
    let Some(memory) = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
    else {
        return 127 << 8;
    };
    let Ok(argv) = read_guest_cstring_array(caller, &memory, argv_ptr as usize) else {
        return 127 << 8;
    };
    if argv.is_empty() {
        return 127 << 8;
    }
    let env = if envp_ptr == 0 {
        None
    } else {
        match read_guest_env_array(caller, &memory, envp_ptr as usize) {
            Ok(env) => Some(env),
            Err(_) => return 127 << 8,
        }
    };
    let parent_remaining_fuel = match caller.get_fuel() {
        Ok(remaining) => remaining,
        Err(_) => return 127 << 8,
    };
    caller
        .data()
        .fuel_remaining
        .fetch_min(parent_remaining_fuel, Ordering::SeqCst);
    let child_cwd = child_cwd_for_request(caller.data(), env.as_ref(), cwd.as_ref());
    match run_wasi_child_process(caller.data(), argv, env, cwd, stdio.clone()) {
        Ok(child) => {
            if caller.set_fuel(child.remaining_fuel).is_err() {
                return 127 << 8;
            }
            (child.exit_status & 0xff) << 8
        }
        Err(error) => {
            let shared_remaining = caller.data().fuel_remaining.load(Ordering::SeqCst);
            let _ = caller.set_fuel(shared_remaining);
            if matches!(error, SandboxWasiError::InputLimitExceeded { .. }) {
                caller
                    .data()
                    .child_input_limit_exceeded
                    .store(true, Ordering::SeqCst);
            }
            if matches!(error, SandboxWasiError::OutputLimitExceeded { .. }) {
                caller
                    .data()
                    .child_output_limit_exceeded
                    .store(true, Ordering::SeqCst);
            }
            write_child_spawn_error(caller.data(), &stdio, &child_cwd, &error);
            127 << 8
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ChildProcessStdio {
    stdin_path: Option<String>,
    stdout_path: Option<String>,
    stdout_append: bool,
    stderr_path: Option<String>,
    stderr_append: bool,
    stderr_to_stdout: bool,
}

struct ChildProcessOutcome {
    exit_status: i32,
    remaining_fuel: u64,
}

fn write_child_spawn_error(
    parent: &LocalWasiState,
    stdio: &ChildProcessStdio,
    cwd: &SandboxPath,
    error: &SandboxWasiError,
) {
    let error_bytes = format!("sinomo sandbox subprocess failed: {error}\n").into_bytes();
    if let Some(stderr_path) = &stdio.stderr_path {
        if let Ok(path) = sandbox_wasi_extension_guest_path(stderr_path, cwd) {
            let _ = sandbox_wasi_extension_write_guest_file(
                &path,
                &parent.preopened_dirs,
                &error_bytes,
                stdio.stderr_append,
            );
            return;
        }
    }
    if stdio.stderr_to_stdout
        && let Some(stdout_path) = &stdio.stdout_path
        && let Ok(path) = sandbox_wasi_extension_guest_path(stdout_path, cwd)
    {
        let _ = sandbox_wasi_extension_write_guest_file(
            &path,
            &parent.preopened_dirs,
            &error_bytes,
            stdio.stdout_append,
        );
    }
}

fn run_wasi_child_process(
    parent: &LocalWasiState,
    argv: Vec<String>,
    env: Option<BTreeMap<String, String>>,
    cwd: Option<SandboxPath>,
    stdio: ChildProcessStdio,
) -> Result<ChildProcessOutcome, SandboxWasiError> {
    let engine = parent.engine.clone();
    let explicit_child_cwd = cwd.is_some() || env.as_ref().and_then(|env| env.get("PWD")).is_some();
    let child_cwd = child_cwd_for_request(parent, env.as_ref(), cwd.as_ref());
    let child_module_wasm =
        resolve_wasi_child_process_module(parent, argv.first().map(String::as_str), &child_cwd)?;
    let module = wasmtime::Module::new(&engine, &child_module_wasm)
        .map_err(sandbox_wasi_error_from_wasmtime)?;
    let child_working_directory_host_path =
        sandbox_wasi_extension_host_path(&child_cwd, &parent.preopened_dirs)
            .map(|(_, host_path)| host_path)
            .or_else(|| {
                if child_cwd == parent.cwd {
                    parent.working_directory_host_path.clone()
                } else {
                    None
                }
            });
    if explicit_child_cwd {
        validate_child_working_directory(&parent.preopened_dirs, &child_cwd)?;
    }
    let mut wasi = wasmtime_wasi::WasiCtxBuilder::new();
    wasi.allow_blocking_current_thread(true);
    wasi.allow_ip_name_lookup(false);
    wasi.allow_tcp(false);
    wasi.allow_udp(false);
    wasi.socket_addr_check(|_, _| Box::pin(async { false }));
    wasi.args(&argv);
    let mut child_env = env.unwrap_or_else(|| parent.env.clone());
    child_env.insert("PWD".to_string(), child_cwd.as_str().to_string());
    for (key, value) in &child_env {
        if key == "PWD" {
            continue;
        }
        wasi.env(key, value);
    }
    wasi.env("PWD", child_cwd.as_str());
    let child_stdin = if let Some(stdin_path) = &stdio.stdin_path {
        let stdin_path =
            sandbox_wasi_extension_guest_path(stdin_path, &child_cwd).map_err(|error| {
                SandboxWasiError::InvalidRequest {
                    detail: format!("invalid child stdin path: {error}"),
                }
            })?;
        let stdin = sandbox_wasi_extension_read_guest_file_with_limit(
            &stdin_path,
            &parent.preopened_dirs,
            parent.input_limit_bytes,
        )?;
        Some(wasmtime_wasi::p2::pipe::MemoryInputPipe::new(stdin))
    } else if let Some(stdin) = parent.stdin.clone() {
        Some(stdin)
    } else {
        None
    };
    if let Some(stdin) = child_stdin.clone() {
        wasi.stdin(stdin);
    }
    if let Some(stdout_path) = &stdio.stdout_path {
        let stdout_path =
            sandbox_wasi_extension_guest_path(stdout_path, &child_cwd).map_err(|error| {
                SandboxWasiError::InvalidRequest {
                    detail: format!("invalid child stdout path: {error}"),
                }
            })?;
        sandbox_wasi_extension_validate_guest_file_write_target(
            &stdout_path,
            &parent.preopened_dirs,
        )?;
    }
    if let Some(stderr_path) = &stdio.stderr_path {
        let stderr_path =
            sandbox_wasi_extension_guest_path(stderr_path, &child_cwd).map_err(|error| {
                SandboxWasiError::InvalidRequest {
                    detail: format!("invalid child stderr path: {error}"),
                }
            })?;
        sandbox_wasi_extension_validate_guest_file_write_target(
            &stderr_path,
            &parent.preopened_dirs,
        )?;
    }
    let child_output_capacity = parent.output_limit_bytes.saturating_add(1);
    let child_stdout = if stdio.stdout_path.is_some() {
        wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(child_output_capacity)
    } else {
        parent.stdout.clone()
    };
    let child_stderr = if stdio.stderr_to_stdout {
        child_stdout.clone()
    } else if stdio.stderr_path.is_some() {
        wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(child_output_capacity)
    } else {
        parent.stderr.clone()
    };
    if parent.child_output_limit_exceeded.load(Ordering::SeqCst) {
        return Err(SandboxWasiError::OutputLimitExceeded {
            limit: parent.output_limit_bytes,
        });
    }
    wasi.stdout(child_stdout.clone());
    wasi.stderr(child_stderr.clone());
    if let Some(host_path) = &child_working_directory_host_path {
        wasi.preopened_dir(
            host_path,
            ".",
            wasmtime_wasi::DirPerms::all(),
            wasmtime_wasi::FilePerms::all(),
        )
        .map_err(sandbox_wasi_error_from_wasmtime)?;
    }
    if let Some(host_path) = &parent.virtual_device_host_path {
        wasi.preopened_dir(
            host_path,
            "dev",
            wasmtime_wasi::DirPerms::READ,
            wasmtime_wasi::FilePerms::READ | wasmtime_wasi::FilePerms::WRITE,
        )
        .map_err(sandbox_wasi_error_from_wasmtime)?;
    }
    for preopen in &parent.preopened_dirs {
        let (dir_perms, file_perms) = match preopen.mode {
            SandboxMountMode::ReadOnly => (
                wasmtime_wasi::DirPerms::READ,
                wasmtime_wasi::FilePerms::READ,
            ),
            SandboxMountMode::ReadWrite => (
                wasmtime_wasi::DirPerms::all(),
                wasmtime_wasi::FilePerms::all(),
            ),
        };
        wasi.preopened_dir(
            &preopen.host_path,
            guest_path_for_wasi(&preopen.guest_path),
            dir_perms,
            file_perms,
        )
        .map_err(sandbox_wasi_error_from_wasmtime)?;
    }
    let mut store = wasmtime::Store::new(
        &engine,
        LocalWasiState {
            wasi: wasi.build_p1(),
            limits: wasmtime::StoreLimitsBuilder::new()
                .memory_size(parent.memory_limit_bytes)
                .build(),
            stdin: child_stdin,
            working_directory_host_path: child_working_directory_host_path,
            preopened_dirs: parent.preopened_dirs.clone(),
            virtual_device_host_path: parent.virtual_device_host_path.clone(),
            cwd: child_cwd.clone(),
            env: child_env,
            engine: engine.clone(),
            stdout: child_stdout.clone(),
            stderr: child_stderr.clone(),
            input_limit_bytes: parent.input_limit_bytes,
            child_input_limit_exceeded: parent.child_input_limit_exceeded.clone(),
            output_limit_bytes: parent.output_limit_bytes,
            child_output_limit_exceeded: parent.child_output_limit_exceeded.clone(),
            memory_limit_bytes: parent.memory_limit_bytes,
            fuel: parent.fuel,
            fuel_remaining: parent.fuel_remaining.clone(),
            child_depth: parent.child_depth + 1,
            allow_pinned_subprocesses: parent.allow_pinned_subprocesses,
        },
    );
    store.limiter(|state| &mut state.limits);
    let child_fuel = parent
        .fuel_remaining
        .load(Ordering::SeqCst)
        .min(parent.fuel);
    store
        .set_fuel(child_fuel)
        .map_err(sandbox_wasi_error_from_wasmtime)?;
    store.set_epoch_deadline(1);
    let mut linker = wasmtime::Linker::new(&engine);
    wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut LocalWasiState| {
        &mut state.wasi
    })
    .map_err(sandbox_wasi_error_from_wasmtime)?;
    super::extensions::add_sinomo_sandbox_wasi_extensions(&mut linker)
        .map_err(sandbox_wasi_error_from_wasmtime)?;
    let exit_status = match run_wasi_start(&mut store, &linker, &module) {
        Ok(exit_status) => exit_status,
        Err(error) => {
            if let Ok(remaining_fuel) = store.get_fuel() {
                parent
                    .fuel_remaining
                    .fetch_min(remaining_fuel, Ordering::SeqCst);
            }
            if super::runner::output_pipe_limit_exceeded(&error) {
                return Err(SandboxWasiError::OutputLimitExceeded {
                    limit: parent.output_limit_bytes,
                });
            }
            return Err(sandbox_wasi_error_from_wasmtime(error));
        }
    };
    let remaining_fuel = store.get_fuel().map_err(sandbox_wasi_error_from_wasmtime)?;
    parent
        .fuel_remaining
        .fetch_min(remaining_fuel, Ordering::SeqCst);
    if let Some(stdout_path) = &stdio.stdout_path {
        let stdout_path =
            sandbox_wasi_extension_guest_path(stdout_path, &child_cwd).map_err(|error| {
                SandboxWasiError::InvalidRequest {
                    detail: format!("invalid child stdout path: {error}"),
                }
            })?;
        let bytes = child_stdout.contents();
        if bytes.len() > parent.output_limit_bytes {
            return Err(SandboxWasiError::OutputLimitExceeded {
                limit: parent.output_limit_bytes,
            });
        }
        sandbox_wasi_extension_write_guest_file(
            &stdout_path,
            &parent.preopened_dirs,
            bytes.as_ref(),
            stdio.stdout_append,
        )?;
    }
    if let Some(stderr_path) = &stdio.stderr_path {
        let stderr_path =
            sandbox_wasi_extension_guest_path(stderr_path, &child_cwd).map_err(|error| {
                SandboxWasiError::InvalidRequest {
                    detail: format!("invalid child stderr path: {error}"),
                }
            })?;
        let bytes = child_stderr.contents();
        if bytes.len() > parent.output_limit_bytes {
            return Err(SandboxWasiError::OutputLimitExceeded {
                limit: parent.output_limit_bytes,
            });
        }
        sandbox_wasi_extension_write_guest_file(
            &stderr_path,
            &parent.preopened_dirs,
            bytes.as_ref(),
            stdio.stderr_append,
        )?;
    }
    Ok(ChildProcessOutcome {
        exit_status,
        remaining_fuel,
    })
}

fn child_cwd_for_request(
    parent: &LocalWasiState,
    env: Option<&BTreeMap<String, String>>,
    cwd: Option<&SandboxPath>,
) -> SandboxPath {
    cwd.cloned()
        .or_else(|| {
            env.and_then(|env| env.get("PWD"))
                .or_else(|| parent.env.get("PWD"))
                .and_then(|path| SandboxPath::new(path).ok())
        })
        .unwrap_or_else(|| parent.cwd.clone())
}

fn validate_child_working_directory(
    preopened_dirs: &[SandboxWasiPreopen],
    path: &SandboxPath,
) -> Result<(), SandboxWasiError> {
    let Some((preopen, host_path)) = sandbox_wasi_extension_host_path(path, preopened_dirs) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child cwd is not mounted: {path}"),
        });
    };
    let root = fs::canonicalize(&preopen.host_path).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("resolve child cwd preopen {}: {error}", preopen.guest_path),
    })?;
    let target =
        fs::canonicalize(&host_path).map_err(|error| SandboxWasiError::InvalidRequest {
            detail: format!("child cwd `{path}` is invalid: {error}"),
        })?;
    if !target.starts_with(&root) {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child cwd `{path}` escapes its sandbox preopen"),
        });
    }
    let metadata = fs::metadata(&target).map_err(|error| SandboxWasiError::InvalidRequest {
        detail: format!("child cwd `{path}` is invalid: {error}"),
    })?;
    if !metadata.is_dir() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child cwd `{path}` is not a directory"),
        });
    }
    Ok(())
}

fn resolve_wasi_child_process_module(
    parent: &LocalWasiState,
    argv0: Option<&str>,
    cwd: &SandboxPath,
) -> Result<Vec<u8>, SandboxWasiError> {
    let Some(argv0) = argv0.filter(|value| !value.is_empty()) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "sandbox child process argv[0] is empty".to_string(),
        });
    };

    if argv0.contains('/') {
        let path = sandbox_wasi_extension_guest_path(argv0, cwd).map_err(|error| {
            SandboxWasiError::InvalidRequest {
                detail: format!("invalid sandbox child executable path: {error}"),
            }
        })?;
        return read_pinned_child_executable(parent, &path);
    }

    let candidate = SandboxPath::new(format!("/bin/{argv0}")).map_err(|error| {
        SandboxWasiError::InvalidRequest {
            detail: format!("invalid sandbox child executable path: {error}"),
        }
    })?;
    read_pinned_child_executable(parent, &candidate)
}

fn read_pinned_child_executable(
    parent: &LocalWasiState,
    path: &SandboxPath,
) -> Result<Vec<u8>, SandboxWasiError> {
    if path.as_str() != "/bin" && !path.as_str().starts_with("/bin/") {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!(
                "sandbox child executable {path} is outside the pinned /bin toolchain mount"
            ),
        });
    }
    let Some((preopen, host_path)) = sandbox_wasi_extension_host_path(path, &parent.preopened_dirs)
    else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("sandbox child executable {path} not found in pinned /bin"),
        });
    };
    if preopen.guest_path.as_str() != "/bin" || preopen.mode != SandboxMountMode::ReadOnly {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!(
                "sandbox child executable {path} must resolve through the read-only pinned /bin toolchain mount"
            ),
        });
    }
    sandbox_wasi_extension_read_host_path_with_limit(
        &preopen.host_path,
        &host_path,
        parent.memory_limit_bytes,
    )
    .map_err(|error| match error {
        SandboxWasiError::Runtime { detail } => SandboxWasiError::Runtime {
            detail: format!("read sandbox child executable {path}: {detail}"),
        },
        SandboxWasiError::InvalidRequest { detail } => SandboxWasiError::InvalidRequest {
            detail: format!("sandbox child executable {path} is invalid: {detail}"),
        },
        other => other,
    })
}
