use super::{
    guest::read_guest_string,
    paths::{
        SINOMO_WASI_ERRNO_INVAL, SINOMO_WASI_ERRNO_NOENT, SINOMO_WASI_ERRNO_ROFS,
        sandbox_wasi_extension_chmod_host_path, sandbox_wasi_extension_guest_path,
        sandbox_wasi_extension_host_path, sandbox_wasi_extension_host_path_mode,
    },
    process::{
        sinomo_sandbox_wasi_proc_spawn_wait, sinomo_sandbox_wasi_proc_spawn_wait_at,
        sinomo_sandbox_wasi_proc_spawn_wait_at_io,
    },
    state::LocalWasiState,
};
use crate::{
    SINOMO_SANDBOX_WASI_EXT_MODULE, SINOMO_SANDBOX_WASI_PATH_CHMOD, SINOMO_SANDBOX_WASI_PATH_MODE,
    SINOMO_SANDBOX_WASI_PROC_SPAWN_WAIT, SINOMO_SANDBOX_WASI_PROC_SPAWN_WAIT_AT,
    SINOMO_SANDBOX_WASI_PROC_SPAWN_WAIT_AT_IO, SandboxMountMode,
};

pub(super) fn add_sinomo_sandbox_wasi_extensions(
    linker: &mut wasmtime::Linker<LocalWasiState>,
) -> Result<(), wasmtime::Error> {
    linker.func_wrap(
        SINOMO_SANDBOX_WASI_EXT_MODULE,
        SINOMO_SANDBOX_WASI_PATH_CHMOD,
        |mut caller: wasmtime::Caller<'_, LocalWasiState>,
         path_ptr: i32,
         path_len: i32,
         mode: i32|
         -> i32 { sinomo_sandbox_wasi_path_chmod(&mut caller, path_ptr, path_len, mode) },
    )?;
    linker.func_wrap(
        SINOMO_SANDBOX_WASI_EXT_MODULE,
        SINOMO_SANDBOX_WASI_PATH_MODE,
        |mut caller: wasmtime::Caller<'_, LocalWasiState>,
         path_ptr: i32,
         path_len: i32,
         mode_ptr: i32|
         -> i32 {
            sinomo_sandbox_wasi_path_mode(&mut caller, path_ptr, path_len, mode_ptr)
        },
    )?;
    linker.func_wrap(
        SINOMO_SANDBOX_WASI_EXT_MODULE,
        SINOMO_SANDBOX_WASI_PROC_SPAWN_WAIT,
        |mut caller: wasmtime::Caller<'_, LocalWasiState>, argv_ptr: i32, envp_ptr: i32| -> i32 {
            sinomo_sandbox_wasi_proc_spawn_wait(&mut caller, argv_ptr, envp_ptr)
        },
    )?;
    linker.func_wrap(
        SINOMO_SANDBOX_WASI_EXT_MODULE,
        SINOMO_SANDBOX_WASI_PROC_SPAWN_WAIT_AT,
        |mut caller: wasmtime::Caller<'_, LocalWasiState>,
         argv_ptr: i32,
         envp_ptr: i32,
         cwd_ptr: i32,
         cwd_len: i32|
         -> i32 {
            sinomo_sandbox_wasi_proc_spawn_wait_at(
                &mut caller,
                argv_ptr,
                envp_ptr,
                cwd_ptr,
                cwd_len,
            )
        },
    )?;
    linker.func_wrap(
        SINOMO_SANDBOX_WASI_EXT_MODULE,
        SINOMO_SANDBOX_WASI_PROC_SPAWN_WAIT_AT_IO,
        |mut caller: wasmtime::Caller<'_, LocalWasiState>,
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
         stderr_to_stdout: i32|
         -> i32 {
            sinomo_sandbox_wasi_proc_spawn_wait_at_io(
                &mut caller,
                argv_ptr,
                envp_ptr,
                cwd_ptr,
                cwd_len,
                stdin_path_ptr,
                stdin_path_len,
                stdout_path_ptr,
                stdout_path_len,
                stdout_append,
                stderr_path_ptr,
                stderr_path_len,
                stderr_append,
                stderr_to_stdout,
            )
        },
    )?;
    Ok(())
}

fn sinomo_sandbox_wasi_path_chmod(
    caller: &mut wasmtime::Caller<'_, LocalWasiState>,
    path_ptr: i32,
    path_len: i32,
    mode: i32,
) -> i32 {
    if path_ptr < 0 || path_len < 0 || mode < 0 {
        return SINOMO_WASI_ERRNO_INVAL;
    }
    let Some(memory) = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
    else {
        return SINOMO_WASI_ERRNO_INVAL;
    };
    let Ok(path) = read_guest_string(caller, &memory, path_ptr as usize, path_len as usize) else {
        return SINOMO_WASI_ERRNO_INVAL;
    };
    let Ok(sandbox_path) = sandbox_wasi_extension_guest_path(&path, &caller.data().cwd) else {
        return SINOMO_WASI_ERRNO_INVAL;
    };
    let Some((preopen, host_path)) =
        sandbox_wasi_extension_host_path(&sandbox_path, &caller.data().preopened_dirs)
    else {
        return SINOMO_WASI_ERRNO_NOENT;
    };
    if preopen.mode != SandboxMountMode::ReadWrite {
        return SINOMO_WASI_ERRNO_ROFS;
    }
    sandbox_wasi_extension_chmod_host_path(&preopen.host_path, &host_path, mode as u32)
}

fn sinomo_sandbox_wasi_path_mode(
    caller: &mut wasmtime::Caller<'_, LocalWasiState>,
    path_ptr: i32,
    path_len: i32,
    mode_ptr: i32,
) -> i32 {
    if path_ptr < 0 || path_len < 0 || mode_ptr < 0 {
        return SINOMO_WASI_ERRNO_INVAL;
    }
    let Some(memory) = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
    else {
        return SINOMO_WASI_ERRNO_INVAL;
    };
    let Ok(path) = read_guest_string(caller, &memory, path_ptr as usize, path_len as usize) else {
        return SINOMO_WASI_ERRNO_INVAL;
    };
    let Ok(sandbox_path) = sandbox_wasi_extension_guest_path(&path, &caller.data().cwd) else {
        return SINOMO_WASI_ERRNO_INVAL;
    };
    let Some((preopen, host_path)) =
        sandbox_wasi_extension_host_path(&sandbox_path, &caller.data().preopened_dirs)
    else {
        return SINOMO_WASI_ERRNO_NOENT;
    };
    let mode = match sandbox_wasi_extension_host_path_mode(&preopen.host_path, &host_path) {
        Ok(mode) => mode,
        Err(errno) => return errno,
    };
    let bytes = mode.to_le_bytes();
    if memory
        .write(&mut *caller, mode_ptr as usize, &bytes)
        .is_err()
    {
        return SINOMO_WASI_ERRNO_INVAL;
    }
    super::paths::SINOMO_WASI_ERRNO_SUCCESS
}
