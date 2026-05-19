use wasmer::{FromToNativeWasmType, MemoryAccessError, MemoryView};

use super::*;
use crate::{
    VIRTUAL_ROOT_FD, WasiFs,
    fs::WasiFsProcessSnapshot,
    os::task::{OwnedTaskStatus, TaskStatus},
    syscalls::*,
};

/// Replaces the current process with a new process
///
/// ## Parameters
///
/// * `name` - Name of the process to be spawned
/// * `args` - List of the arguments to pass the process
///   (entries are separated by line feeds)
/// * `envs` - List of the environment variables to pass process
///
/// ## Return
///
/// If the execution fails, returns an error code. Does not return otherwise.
#[instrument(level = "trace", skip_all, fields(name = field::Empty, %args_len), ret)]
pub fn proc_exec3<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    name: WasmPtr<u8, M>,
    name_len: M::Offset,
    args: WasmPtr<u8, M>,
    args_len: M::Offset,
    envs: WasmPtr<u8, M>,
    envs_len: M::Offset,
    search_path: Bool,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    // If we were just restored the stack then we were woken after a deep sleep
    if let Some(exit_code) = unsafe { handle_rewind::<M, i32>(&mut ctx) } {
        // We should never get here as the process will be termined
        // in the `WasiEnv::do_pending_operations()` call
        let exit_code = ExitCode::from_native(exit_code);
        ctx.data().process.terminate(exit_code);
        return Err(WasiError::Exit(exit_code));
    }

    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let name = name.read_utf8_string(&memory, name_len).map_err(|err| {
        warn!("failed to execve as the name could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let args = args.read_utf8_string(&memory, args_len).map_err(|err| {
        warn!("failed to execve as the args could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let args = parse_wasix_line_encoded_strings(&args);
    let envs = if envs.is_null() {
        None
    } else {
        let envs = envs.read_utf8_string(&memory, envs_len).map_err(|err| {
            warn!("failed to execve as the envs could not be read - {}", err);
            WasiError::Exit(Errno::Inval.into())
        })?;
        Some(parse_wasix_line_encoded_strings(&envs))
    };
    let path = if path.is_null() {
        None
    } else {
        Some(path.read_utf8_string(&memory, path_len).map_err(|err| {
            warn!("failed to execve as the path could not be read - {}", err);
            WasiError::Exit(Errno::Inval.into())
        })?)
    };

    proc_exec3_prepared::<M>(ctx, name, args, envs, search_path, path)
}

/// ### `execv()`
/// POSIX-shaped exec wrapper that reads a null-terminated argv vector and
/// inherits the current sandbox environment.
#[instrument(level = "trace", skip_all, ret)]
pub fn execv<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    path: WasmPtr<u8, M>,
    argv: WasmPtr<WasmPtr<u8, M>, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    handle_exec_rewind::<M>(&mut ctx)?;

    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let path = read_guest_c_string::<M>(&memory, path).map_err(|err| {
        warn!("failed to execv as the path could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let args = read_guest_c_string_array::<M>(&memory, argv).map_err(|err| {
        warn!("failed to execv as argv could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;

    proc_exec3_prepared::<M>(ctx, path, args, None, Bool::False, None)
}

/// ### `execve()`
/// POSIX-shaped exec wrapper that reads null-terminated argv and envp vectors.
#[instrument(level = "trace", skip_all, ret)]
pub fn execve<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    path: WasmPtr<u8, M>,
    argv: WasmPtr<WasmPtr<u8, M>, M>,
    envp: WasmPtr<WasmPtr<u8, M>, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    handle_exec_rewind::<M>(&mut ctx)?;

    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let path = read_guest_c_string::<M>(&memory, path).map_err(|err| {
        warn!("failed to execve as the path could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let args = read_guest_c_string_array::<M>(&memory, argv).map_err(|err| {
        warn!("failed to execve as argv could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let envs = if envp.is_null() {
        None
    } else {
        Some(read_guest_c_string_array::<M>(&memory, envp).map_err(|err| {
            warn!("failed to execve as envp could not be read - {}", err);
            WasiError::Exit(Errno::Inval.into())
        })?)
    };

    proc_exec3_prepared::<M>(ctx, path, args, envs, Bool::False, None)
}

/// ### `execvp()`
/// POSIX-shaped exec wrapper that searches PATH and inherits the environment.
#[instrument(level = "trace", skip_all, ret)]
pub fn execvp<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    file: WasmPtr<u8, M>,
    argv: WasmPtr<WasmPtr<u8, M>, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    handle_exec_rewind::<M>(&mut ctx)?;

    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let file = read_guest_c_string::<M>(&memory, file).map_err(|err| {
        warn!("failed to execvp as the file could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let args = read_guest_c_string_array::<M>(&memory, argv).map_err(|err| {
        warn!("failed to execvp as argv could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let path = current_env_path(ctx.data());

    proc_exec3_prepared::<M>(ctx, file, args, None, Bool::True, path)
}

/// ### `execvpe()`
/// POSIX-shaped exec wrapper that searches PATH and uses an explicit envp.
#[instrument(level = "trace", skip_all, ret)]
pub fn execvpe<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    file: WasmPtr<u8, M>,
    argv: WasmPtr<WasmPtr<u8, M>, M>,
    envp: WasmPtr<WasmPtr<u8, M>, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    handle_exec_rewind::<M>(&mut ctx)?;

    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let file = read_guest_c_string::<M>(&memory, file).map_err(|err| {
        warn!("failed to execvpe as the file could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let args = read_guest_c_string_array::<M>(&memory, argv).map_err(|err| {
        warn!("failed to execvpe as argv could not be read - {}", err);
        WasiError::Exit(Errno::Inval.into())
    })?;
    let envs = if envp.is_null() {
        None
    } else {
        Some(read_guest_c_string_array::<M>(&memory, envp).map_err(|err| {
            warn!("failed to execvpe as envp could not be read - {}", err);
            WasiError::Exit(Errno::Inval.into())
        })?)
    };
    let path = envs
        .as_ref()
        .and_then(|envs| envs.iter().find_map(|env| env.strip_prefix("PATH=").map(str::to_string)))
        .or_else(|| current_env_path(ctx.data()));

    proc_exec3_prepared::<M>(ctx, file, args, envs, Bool::True, path)
}

fn handle_exec_rewind<M: MemorySize>(ctx: &mut FunctionEnvMut<'_, WasiEnv>) -> Result<(), WasiError> {
    if let Some(exit_code) = unsafe { handle_rewind::<M, i32>(ctx) } {
        let exit_code = ExitCode::from_native(exit_code);
        ctx.data().process.terminate(exit_code);
        return Err(WasiError::Exit(exit_code));
    }
    Ok(())
}

fn read_guest_c_string<M: MemorySize>(
    memory: &MemoryView,
    ptr: WasmPtr<u8, M>,
) -> Result<String, MemoryAccessError> {
    ptr.read_utf8_string_with_nul(memory)
}

fn read_guest_c_string_array<M: MemorySize>(
    memory: &MemoryView,
    ptrs: WasmPtr<WasmPtr<u8, M>, M>,
) -> Result<Vec<String>, MemoryAccessError> {
    if ptrs.is_null() {
        return Ok(Vec::new());
    }

    let mut values = Vec::new();
    for index in 0usize.. {
        let ptr = ptrs
            .add_offset(to_offset::<M>(index).map_err(|_| MemoryAccessError::Overflow)?)?
            .read(memory)?;
        if ptr.is_null() {
            break;
        }
        values.push(read_guest_c_string::<M>(memory, ptr)?);
    }
    Ok(values)
}

fn current_env_path(env: &WasiEnv) -> Option<String> {
    env.state
        .envs
        .lock()
        .unwrap()
        .iter()
        .find_map(|entry| {
            std::str::from_utf8(entry)
                .ok()
                .and_then(|entry| entry.strip_prefix("PATH=").map(str::to_string))
        })
}

fn proc_exec3_prepared<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    mut name: String,
    mut args: Vec<String>,
    envs: Option<Vec<String>>,
    search_path: Bool,
    path: Option<String>,
) -> Result<Errno, WasiError> {
    let reexec_current_executable = name.is_empty();
    if name.is_empty()
        && let Some(current_executable) = ctx.data().process.executable_path()
    {
        name = current_executable;
    }
    Span::current().record("name", name.as_str());
    if reexec_current_executable
        && args
            .first()
            .is_some_and(|arg| matches!(arg.as_str(), "-cc1" | "-cc1as"))
    {
        args.insert(0, name.clone());
    }

    let envs = match envs {
        Some(envs) => {
            let mut vec = vec![];
            for env in envs {
                let (key, value) = wasi_try_ok!(env.split_once('=').ok_or(Errno::Inval));

                vec.push((key.to_string(), value.to_string()));
            }
            Some(vec)
        }
        None => None,
    };

    // Convert relative paths into absolute paths
    if search_path == Bool::True && !name.contains('/') {
        let path = match path.as_deref() {
            Some(path) => path.split(':').collect(),
            None => vec!["/usr/local/bin", "/bin", "/usr/bin"],
        };
        let (_, state, inodes) =
            unsafe { ctx.data().get_memory_and_wasi_state_and_inodes(&ctx, 0) };
        match find_executable_in_path(&state.fs, inodes, path.iter().map(AsRef::as_ref), &name) {
            FindExecutableResult::Found(p) => name = p,
            FindExecutableResult::AccessError => return Ok(Errno::Access),
            FindExecutableResult::NotFound => return Ok(Errno::Noexec),
        }
    } else if !name.starts_with('/') {
        name = ctx.data().state.fs.relative_path_to_absolute(name);
    }

    trace!(name);

    // Convert the preopen directories
    let preopen = ctx.data().state.preopen.clone();

    // Get the current working directory
    let (_, cur_dir) = {
        let (memory, state, inodes) =
            unsafe { ctx.data().get_memory_and_wasi_state_and_inodes(&ctx, 0) };
        match state.fs.get_current_dir(inodes, crate::VIRTUAL_ROOT_FD) {
            Ok(a) => a,
            Err(err) => {
                warn!("failed to create subprocess for fork - {}", err);
                return Err(WasiError::Exit(err.into()));
            }
        }
    };

    let new_store = ctx.data().runtime.new_store();

    // If we are in a vfork we need to first spawn a subprocess of this type
    // with the forked WasiEnv, then do a longjmp back to the vfork point.
    if let Some(mut vfork) = ctx.data_mut().vfork.take() {
        // Needed in case an error happens and we need to get back into the child process
        let mut child_env = Box::new(ctx.data().clone());

        // We will need the child pid later
        let child_pid = ctx.data().process.pid();

        tracing::debug!(
            %child_pid,
            vfork_pid = %vfork.env.process.pid(),
            "proc_exec in presence of vfork"
        );

        // Restore the WasiEnv to the point when we vforked
        let mut vfork_env = vfork.env.clone();
        vfork_env.swap_inner(ctx.data_mut());
        std::mem::swap(vfork_env.as_mut(), ctx.data_mut());
        let mut wasi_env = *vfork_env;
        wasi_env.owned_handles.push(vfork.handle.clone());
        _prepare_wasi(&mut wasi_env, Some(args), envs, None);
        wasi_env.process.set_executable_path(name.clone());

        // Record the stack offsets before we give up ownership of the wasi_env
        let stack_lower = wasi_env.layout.stack_lower;
        let stack_upper = wasi_env.layout.stack_upper;

        // Spawn a new process with this current execution environment
        let mut err_exit_code: ExitCode = Errno::Success.into();

        let spawn_result = {
            let bin_factory = Box::new(ctx.data().bin_factory.clone());
            let tasks = wasi_env.tasks().clone();

            let mut config = Some(wasi_env);

            match bin_factory.try_built_in(name.clone(), Some(&ctx), &mut config) {
                Ok(a) => Ok(()),
                Err(err) => {
                    if !err.is_not_found() {
                        error!("builtin failed - {}", err);
                    }

                    match config.take() {
                        Some(env) => {
                            let name_inner = name.clone();
                            __asyncify_light(ctx.data(), None, async {
                                let ret = bin_factory.spawn(name_inner, env).await;
                                match ret {
                                    Ok(ret) => {
                                        trace!(%child_pid, "spawned sub-process");
                                        Ok(())
                                    }
                                    Err(err) => {
                                        let errno = conv_spawn_err_to_errno(&err);
                                        err_exit_code = errno.into();

                                        debug!(%child_pid, "process failed with (err={})", err_exit_code);

                                        Err(errno)
                                    }
                                }
                            })
                            .map_err(|err| {
                                warn!("failed to execve as child spawn asyncify failed - {}", err);
                                err
                            })?
                        }
                        None => {
                            warn!("failed to execve as builtin probing consumed child environment");
                            Err(Errno::Noexec)
                        }
                    }
                }
            }
        };

        match spawn_result {
            Err(e) => {
                // We failed to spawn a new process - put the child env back
                child_env.swap_inner(ctx.data_mut());
                std::mem::swap(child_env.as_mut(), ctx.data_mut());

                // Put back the vfork we previously took from here
                ctx.data_mut().vfork = Some(vfork);
                return Ok(e);
            }
            Ok(()) => {
                // We spawned a new process - put the parent env back
                ctx.data_mut().swap_inner(&mut vfork.env);
                std::mem::swap(ctx.data_mut(), &mut vfork.env);

                let Some(asyncify_info) = vfork.asyncify else {
                    // vfork without asyncify only forks the WasiEnv, which we have restored
                    // above. Restoring the control flow is done on the guest side.
                    // See `proc_fork_env()` for information about this.

                    return Ok(Errno::Success);
                };

                assert!(vfork.env.context_switching_environment.is_none());
                assert!(ctx.data().context_switching_environment.is_some());

                // Jump back to the vfork point and continue execution
                // note: fork does not return any values hence passing `()`
                let rewind_stack = asyncify_info.rewind_stack.freeze();
                let store_data = asyncify_info.store_data;
                unwind::<M, _>(ctx, move |mut ctx, _, _| {
                    // Rewind the stack
                    match rewind::<M, _>(
                        ctx,
                        None,
                        rewind_stack,
                        store_data,
                        ForkResult {
                            pid: child_pid.raw() as Pid,
                            ret: Errno::Success,
                        },
                    ) {
                        Errno::Success => OnCalledAction::InvokeAgain,
                        err => {
                            warn!("fork failed - could not rewind the stack - errno={}", err);
                            OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                        }
                    }
                })?;
                Ok(Errno::Success)
            }
        }
    }
    // Otherwise we need to unwind the stack to get out of the current executing
    // callstack, steal the memory/WasiEnv and switch it over to a new thread
    // on the new module
    else {
        let restore_on_exec_failure = ExecFailureRestore::capture(ctx.data());

        // Prepare the environment
        let mut wasi_env = ctx.data().clone();
        _prepare_wasi(&mut wasi_env, Some(args), envs, None);
        wasi_env.process.set_executable_path(name.clone());

        // Get a reference to the runtime
        let bin_factory = ctx.data().bin_factory.clone();
        let tasks = wasi_env.tasks().clone();

        // Create the process and drop the context
        let bin_factory = Box::new(ctx.data().bin_factory.clone());

        let mut builder = Some(wasi_env);

        let process = match bin_factory.try_built_in(name.clone(), Some(&ctx), &mut builder) {
            Ok(a) => Ok(a),
            Err(err) => {
                if !err.is_not_found() {
                    error!("builtin failed - {}", err);
                }

                let Some(env) = builder.take() else {
                    warn!("failed to execve as builtin probing consumed child environment");
                    restore_on_exec_failure.restore(ctx.data());
                    return Ok(Errno::Noexec);
                };

                // Spawn a new process with this current execution environment
                block_on(bin_factory.spawn(name, env))
            }
        };

        match process {
            Ok(mut process) => {
                // If we support deep sleeping then we switch to deep sleep mode
                let env = ctx.data();

                let thread = env.thread.clone();

                // The poller will wait for the process to actually finish
                let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
                    process
                        .wait_finished()
                        .await
                        .unwrap_or_else(|_| Errno::Child.into())
                        .to_native()
                })?;
                match res {
                    AsyncifyAction::Finish(mut ctx, result) => {
                        // When we arrive here the process should already be terminated
                        let exit_code = ExitCode::from_native(result);
                        ctx.data().process.terminate(exit_code);
                        WasiEnv::process_signals_and_exit(&mut ctx)?;
                        Err(WasiError::Exit(Errno::Unknown.into()))
                    }
                    AsyncifyAction::Unwind => Ok(Errno::Success),
                }
            }
            Err(err) => {
                warn!(
                    "failed to execve as the process could not be spawned (fork)[0] - {}",
                    err
                );
                restore_on_exec_failure.restore(ctx.data());
                Ok(conv_spawn_err_to_errno(&err))
            }
        }
    }
}

struct ExecFailureRestore {
    args: Vec<String>,
    envs: Vec<Vec<u8>>,
    executable_path: Option<String>,
    fs: WasiFsProcessSnapshot,
}

impl ExecFailureRestore {
    fn capture(env: &WasiEnv) -> Self {
        Self {
            args: env.state.args.lock().unwrap().clone(),
            envs: env.state.envs.lock().unwrap().clone(),
            executable_path: env.process.executable_path(),
            fs: env.state.fs.capture_process_snapshot(),
        }
    }

    fn restore(&self, env: &WasiEnv) {
        *env.state.args.lock().unwrap() = self.args.clone();
        *env.state.envs.lock().unwrap() = self.envs.clone();
        env.process
            .set_executable_path_option(self.executable_path.clone());
        env.state.fs.restore_process_snapshot(&self.fs);
        sync_process_metadata(env);
    }
}

pub(crate) enum FindExecutableResult {
    Found(String),
    AccessError,
    NotFound,
}

pub(crate) fn find_executable_in_path<'a>(
    fs: &WasiFs,
    inodes: &WasiInodes,
    path: impl IntoIterator<Item = &'a str>,
    file_name: &str,
) -> FindExecutableResult {
    let mut encountered_eaccess = false;
    for p in path {
        let full_path = if p.is_empty() {
            file_name.to_string()
        } else {
            format!("{}/{}", p.trim_end_matches('/'), file_name)
        };
        let full_path = fs.relative_path_to_absolute(full_path);
        match fs.get_inode_at_path(inodes, VIRTUAL_ROOT_FD, &full_path, true) {
            Ok(_) => return FindExecutableResult::Found(full_path),
            Err(Errno::Access) => encountered_eaccess = true,
            Err(_) => (),
        }
    }

    if encountered_eaccess {
        FindExecutableResult::AccessError
    } else {
        FindExecutableResult::NotFound
    }
}
