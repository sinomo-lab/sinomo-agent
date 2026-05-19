use super::*;
use crate::{
    WasiThreadHandle, WasiVForkAsyncify, capture_store_snapshot,
    os::task::{OwnedTaskStatus, thread::WasiMemoryLayout},
    runtime::task_manager::{TaskWasm, TaskWasmRunProperties},
    state::context_switching::ContextSwitchingEnvironment,
    syscalls::*,
};
use serde::{Deserialize, Serialize};
use wasmer::{FromToNativeWasmType, Memory};

#[derive(Serialize, Deserialize)]
pub(crate) struct ForkResult {
    pub pid: Pid,
    pub ret: Errno,
}

struct ForkOutcome {
    pid: Pid,
    ret: Errno,
}

#[derive(Serialize, Deserialize)]
struct CloneThreadResult {
    tid: Tid,
    ret: Errno,
}

/// ### `proc_fork()`
/// Forks the current process into a new subprocess. If the function
/// returns a zero then its the new subprocess. If it returns a positive
/// number then its the current process and the $pid represents the child.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn proc_fork<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    copy_memory: Bool,
    pid_ptr: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    let outcome = proc_fork_common(ctx, copy_memory, Some(pid_ptr))?;
    Ok(outcome.ret)
}

/// ### `fork()`
/// POSIX-shaped fork wrapper. Returns 0 in the child, the child pid in the
/// parent, or a negative errno value on failure.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn fork<M: MemorySize>(ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    let outcome = proc_fork_common::<M>(ctx, Bool::True, None)?;
    Ok(fork_outcome_to_posix(outcome))
}

/// ### `vfork()`
/// POSIX-shaped vfork wrapper over the existing WASIX lightweight fork path.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn vfork<M: MemorySize>(ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    let outcome = proc_fork_common::<M>(ctx, Bool::False, None)?;
    Ok(fork_outcome_to_posix(outcome))
}

/// ### `clone_thread()`
/// POSIX/Linux-shaped same-process clone for `CLONE_THREAD | CLONE_VM`
/// callers. This uses the runtime's asyncify rewind path: the parent receives
/// the new TID and the cloned thread resumes from the syscall with return
/// value zero.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn clone_thread<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    child_stack: u64,
    parent_tid_ptr: WasmPtr<i32, M>,
    child_tid_ptr: WasmPtr<i32, M>,
    tls: u64,
    set_parent_tid: Bool,
    set_child_tid: Bool,
    clear_child_tid: Bool,
) -> Result<i32, WasiError> {
    clone_thread_common::<M>(
        ctx,
        child_stack,
        parent_tid_ptr,
        child_tid_ptr,
        tls,
        set_parent_tid == Bool::True,
        set_child_tid == Bool::True,
        clear_child_tid == Bool::True,
    )
}

fn fork_outcome_to_posix(outcome: ForkOutcome) -> i32 {
    if outcome.ret == Errno::Success {
        outcome.pid as i32
    } else {
        -outcome.ret.to_native()
    }
}

fn clone_thread_result_to_posix(result: CloneThreadResult) -> i32 {
    if result.ret == Errno::Success {
        result.tid as i32
    } else {
        -result.ret.to_native()
    }
}

fn clone_thread_common<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    child_stack: u64,
    parent_tid_ptr: WasmPtr<i32, M>,
    child_tid_ptr: WasmPtr<i32, M>,
    tls: u64,
    set_parent_tid: bool,
    set_child_tid: bool,
    clear_child_tid: bool,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if let Some(context_switching_environment) = ctx.data().context_switching_environment.as_ref()
        && context_switching_environment.active_context_id()
            != context_switching_environment.main_context_id()
    {
        warn!(
            "thread cloning is only supported from the main context when using WASIX context-switching features"
        );
        return Ok(-Errno::Notsup.to_native());
    }

    if let Some(result) = unsafe { handle_rewind::<M, CloneThreadResult>(&mut ctx) } {
        return Ok(clone_thread_result_to_posix(result));
    }

    if !supports_asyncified_fork(ctx.data()) {
        warn!("thread cloning requires asyncify unwind and rewind exports");
        return Ok(-Errno::Noexec.to_native());
    }

    let current_layout = ctx.data().thread.memory_layout().clone();
    let stack_upper = if child_stack == 0 {
        current_layout.stack_upper
    } else {
        child_stack
    };
    let stack_size = current_layout.stack_size;
    let stack_lower = match stack_upper.checked_sub(stack_size) {
        Some(value) => value,
        None => return Ok(-Errno::Inval.to_native()),
    };
    let layout = WasiMemoryLayout {
        stack_upper,
        stack_lower,
        guard_size: current_layout.guard_size,
        stack_size,
        tls_base: if tls == 0 { current_layout.tls_base } else { Some(tls) },
    };

    let thread_start = ThreadStartType::ThreadSpawn { start_ptr: 0 };
    let thread_handle = match ctx.data().process.new_thread(layout.clone(), thread_start) {
        Ok(handle) => handle,
        Err(err) => {
            warn!(?err, "failed to create cloned thread");
            return Ok(-Errno::Again.to_native());
        }
    };
    let tid: Tid = thread_handle.id().into();
    if clear_child_tid {
        thread_handle.set_clear_child_tid(child_tid_ptr.offset().into());
    }
    let thread_id = thread_handle.id();

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if set_parent_tid
        && !parent_tid_ptr.is_null()
        && let Err(err) = parent_tid_ptr.write(&memory, tid as i32)
    {
        env.process.remove_thread(&thread_id);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    if set_child_tid
        && !child_tid_ptr.is_null()
        && let Err(err) = child_tid_ptr.write(&memory, tid as i32)
    {
        env.process.remove_thread(&thread_id);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }

    let state = env.state.clone();
    let snapshot = capture_store_snapshot(&mut ctx.as_store_mut());
    unwind::<M, _>(ctx, move |mut ctx, memory_stack, rewind_stack| {
        let env_inner = ctx.data().inner();
        let instance_handles = env_inner.main_module_instance_handles();
        let module = instance_handles.module_clone();
        let dynamic_linker = env_inner.linker().cloned();
        let mut thread_env = ctx.data().clone();
        thread_env.state = state;
        thread_env.thread = thread_handle.as_thread();
        thread_env.layout = layout;
        thread_env.enable_deep_sleep = false;
        let child_handle = thread_handle;
        let tasks = thread_env.tasks().clone();
        let spawn_type = if let Some(linker) = dynamic_linker {
            let func_env = ctx.as_ref();
            let store = ctx.as_store_mut();
            SpawnType::NewLinkerInstanceGroup(linker, func_env, store, false)
        } else {
            let memory = instance_handles.memory_clone();
            SpawnType::ShareMemory(memory, ctx.as_store_ref())
        };

        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();
        let store_data = match snapshot.serialize() {
            Ok(store_data) => Bytes::from(store_data),
            Err(err) => {
                warn!("failed to serialize clone-thread store snapshot - {}", err);
                ctx.data().process.remove_thread(&thread_id);
                return OnCalledAction::Trap(Box::new(WasiError::Exit(Errno::Perm.into())));
            }
        };

        let child_memory_stack = memory_stack.clone();
        let child_rewind_stack = rewind_stack.clone();
        let child_store_data = store_data.clone();
        let spawn_result = tasks.task_wasm(
            TaskWasm::new(
                Box::new(move |props: TaskWasmRunProperties| {
                    run_clone_thread::<M>(
                        props.ctx,
                        props.store,
                        child_handle,
                        child_memory_stack,
                        child_rewind_stack,
                        child_store_data.clone(),
                    );
                }),
                thread_env,
                module,
                false,
                false,
            )
            .with_globals(snapshot)
            .with_memory(spawn_type),
        );

        if let Err(err) = spawn_result {
            warn!("failed to spawn cloned thread - {}", err);
            ctx.data().process.remove_thread(&thread_id);
            match rewind::<M, _>(
                ctx,
                Some(memory_stack),
                rewind_stack,
                store_data,
                CloneThreadResult {
                    tid: 0,
                    ret: Errno::Again,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => OnCalledAction::Trap(Box::new(WasiError::Exit(err.into()))),
            }
        } else {
            match rewind::<M, _>(
                ctx,
                Some(memory_stack),
                rewind_stack,
                store_data,
                CloneThreadResult {
                    tid,
                    ret: Errno::Success,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => OnCalledAction::Trap(Box::new(WasiError::Exit(err.into()))),
            }
        }
    })
    .map(|err| -err.to_native())
}

fn clone_thread_rewind_result<M: MemorySize>(result: CloneThreadResult) -> RewindResultType {
    RewindResultType::RewindWithResult(
        bincode::serde::encode_to_vec(&result, config::legacy())
            .unwrap()
            .into(),
    )
}

fn run_clone_thread<M: MemorySize>(
    ctx: WasiFunctionEnv,
    store: Store,
    child_handle: WasiThreadHandle,
    memory_stack: Bytes,
    rewind_stack: Bytes,
    store_data: Bytes,
) {
    let layout = ctx.data(&store).layout.clone();
    let rewind_state = RewindState {
        memory_stack,
        rewind_stack,
        store_data,
        start: ThreadStartType::ThreadSpawn { start_ptr: 0 },
        layout,
        is_64bit: M::is_64bit(),
    };
    run_clone_thread_from_rewind::<M>(
        ctx,
        store,
        child_handle,
        Some((
            rewind_state,
            clone_thread_rewind_result::<M>(CloneThreadResult {
                tid: 0,
                ret: Errno::Success,
            }),
        )),
    );
}

fn run_clone_thread_from_rewind<M: MemorySize>(
    ctx: WasiFunctionEnv,
    mut store: Store,
    child_handle: WasiThreadHandle,
    rewind_state: Option<(RewindState, RewindResultType)>,
) {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();
    let pid = env.pid();
    let tid = env.tid();

    if let Some((rewind_state, rewind_result)) = rewind_state {
        let mut ctx_mut = ctx.env.clone().into_mut(&mut store);
        let res = rewind_ext::<M>(
            &mut ctx_mut,
            Some(rewind_state.memory_stack),
            rewind_state.rewind_stack,
            rewind_state.store_data,
            rewind_result,
        );
        if res != Errno::Success {
            child_handle.set_status_finished(Ok(res.into()));
            return;
        }
    }

    trace!(%pid, %tid, "re-invoking clone thread continuation");
    let env_inner = ctx.data(&store).inner();
    let module_handles = env_inner.main_module_instance_handles();
    let Some(start) = module_handles.start.clone() else {
        warn!(%pid, %tid, "cloned thread is missing _start export");
        child_handle.set_status_finished(Ok(Errno::Noexec.into()));
        return;
    };

    let mut ret: ExitCode = Errno::Success.into();
    let (mut store, err) =
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), vec![]);
    if let Err(err) = err {
        match err.downcast::<WasiError>() {
            Ok(WasiError::Exit(exit_code)) => {
                ret = exit_code;
            }
            Ok(WasiError::ThreadExit) => {
                ret = Errno::Success.into();
            }
            Ok(WasiError::DeepSleep(deep)) => {
                trace!(%pid, %tid, "cloned thread entered a deep sleep");
                let respawn = {
                    let tasks = tasks.clone();
                    let rewind_state = deep.rewind;
                    move |ctx, store, rewind_result| {
                        run_clone_thread_from_rewind::<M>(
                            ctx,
                            store,
                            child_handle,
                            Some((
                                rewind_state,
                                RewindResultType::RewindWithResult(rewind_result),
                            )),
                        );
                    }
                };
                unsafe {
                    tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
                };
                return;
            }
            _ => {
                ret = Errno::Unknown.into();
            }
        }
    }

    trace!(%pid, %tid, "cloned thread exited (code = {})", ret);
    ctx.on_exit(&mut store, Some(ret));
    child_handle.set_status_finished(Ok(ret));
    drop(child_handle);
}

fn supports_asyncified_fork(env: &WasiEnv) -> bool {
    let handles = env.inner().main_module_instance_handles();
    handles.asyncify_start_unwind.is_some()
        && handles.asyncify_stop_unwind.is_some()
        && handles.asyncify_start_rewind.is_some()
        && handles.asyncify_stop_rewind.is_some()
}

fn proc_fork_common<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    copy_memory: Bool,
    pid_ptr: Option<WasmPtr<Pid, M>>,
) -> Result<ForkOutcome, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if let Some(context_switching_environment) = ctx.data().context_switching_environment.as_ref()
        && context_switching_environment.active_context_id()
            != context_switching_environment.main_context_id()
    {
        warn!(
            "process forking is only supported from the main context when using WASIX context-switching features"
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Notsup,
        });
    }

    // If we were just restored then we need to return the value instead
    if let Some(result) = unsafe { handle_rewind::<M, ForkResult>(&mut ctx) } {
        if result.pid == 0 {
            trace!("handle_rewind - i am child (ret={})", result.ret);
        } else {
            trace!(
                "handle_rewind - i am parent (child={}, ret={})",
                result.pid, result.ret
            );
        }
        if let Some(pid_ptr) = pid_ptr {
            let memory = unsafe { ctx.data().memory_view(&ctx) };
            if let Err(err) = pid_ptr.write(&memory, result.pid) {
                return Ok(ForkOutcome {
                    pid: 0,
                    ret: crate::mem_error_to_wasi(err),
                });
            }
        }
        return Ok(ForkOutcome {
            pid: result.pid,
            ret: result.ret,
        });
    }
    trace!(%copy_memory, "capturing");

    if let Some(vfork) = ctx.data().vfork.as_ref() {
        warn!("process forking not supported in an active vfork");
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Notsup,
        });
    }
    if ctx.data().process.child_process_limit_reached() {
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Again,
        });
    }

    if !supports_asyncified_fork(ctx.data()) {
        warn!("process forking requires asyncify unwind and rewind exports");
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Noexec,
        });
    }

    // Fork the environment which will copy all the open file handlers
    // and associate a new context but otherwise shares things like the
    // file system interface. The handle to the forked process is stored
    // in the parent process context
    let (mut child_env, mut child_handle) = match ctx.data().fork() {
        Ok(p) => p,
        Err(err) => {
            debug!("could not fork process: {err}");
            // TODO: evaluate the appropriate error code, document it in the spec.
            return Ok(ForkOutcome {
                pid: 0,
                ret: Errno::Perm,
            });
        }
    };
    let child_pid = child_env.process.pid();
    let child_finished = child_env.process.finished.clone();

    // We write a zero to the PID before we capture the stack
    // so that this is what will be returned to the child
    {
        let mut inner = ctx.data().process.lock();
        inner.children.push(child_env.process.clone());
    }
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    // Setup some properties in the child environment
    if let Some(pid_ptr) = pid_ptr {
        if let Err(err) = pid_ptr.write(&memory, 0) {
            return Ok(ForkOutcome {
                pid: 0,
                ret: crate::mem_error_to_wasi(err),
            });
        }
    }
    let pid = child_env.pid();
    let tid = child_env.tid();

    // If we are not copying the memory then we act like a `vfork`
    // instead which will pretend to be the new process for a period
    // of time until `proc_exec` is called at which point the fork
    // actually occurs
    if copy_memory == Bool::False {
        // Perform the unwind action
        return unwind::<M, _>(ctx, move |mut ctx, mut memory_stack, rewind_stack| {
            // Grab all the globals and serialize them
            let store_data = match crate::utils::store::capture_store_snapshot(
                &mut ctx.as_store_mut(),
            )
            .serialize()
            {
                Ok(store_data) => store_data,
                Err(err) => {
                    warn!("failed to serialize vfork store snapshot - {}", err);
                    return OnCalledAction::Trap(Box::new(WasiError::Exit(Errno::Perm.into())));
                }
            };
            let store_data = Bytes::from(store_data);

            // We first fork the environment and replace the current environment
            // so that the process can continue to prepare for the real fork as
            // if it had actually forked
            child_env.swap_inner(ctx.data_mut());
            std::mem::swap(ctx.data_mut(), &mut child_env);
            let previous_vfork = ctx.data_mut().vfork.replace(WasiVFork {
                asyncify: Some(WasiVForkAsyncify {
                    rewind_stack: rewind_stack.clone(),
                    store_data: store_data.clone(),
                    is_64bit: M::is_64bit(),
                }),
                env: Box::new(child_env),
                handle: child_handle,
            });
            assert!(previous_vfork.is_none()); // Already checked above

            // Carry on as if the fork had taken place (which basically means
            // it prevents to be the new process with the old one suspended)
            // Rewind the stack and carry on
            match rewind::<M, _>(
                ctx,
                Some(memory_stack.freeze()),
                rewind_stack.freeze(),
                store_data,
                ForkResult {
                    pid: 0,
                    ret: Errno::Success,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => {
                    warn!("failed - could not rewind the stack - errno={}", err);
                    OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                }
            }
        })
        .map(|ret| ForkOutcome { pid: 0, ret });
    }

    // Create the thread that will back this forked process
    let state = env.state.clone();
    let bin_factory = env.bin_factory.clone();

    // Perform the unwind action
    let snapshot = capture_store_snapshot(&mut ctx.as_store_mut());
    unwind::<M, _>(ctx, move |mut ctx, mut memory_stack, rewind_stack| {
        let tasks = ctx.data().tasks().clone();
        let span = debug_span!(
            "unwind",
            memory_stack_len = memory_stack.len(),
            rewind_stack_len = rewind_stack.len()
        );
        let _span_guard = span.enter();
        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();

        // Grab all the globals and serialize them
        let store_data = match snapshot.serialize() {
            Ok(store_data) => store_data,
            Err(err) => {
                warn!("failed to serialize fork store snapshot - {}", err);
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    Bytes::new(),
                    ForkResult {
                        pid: 0,
                        ret: Errno::Perm,
                    },
                ) {
                    Errno::Success => return OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        return OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())));
                    }
                }
            }
        };
        let store_data = Bytes::from(store_data);

        // Now we use the environment and memory references
        let runtime = child_env.runtime.clone();
        let tasks = child_env.tasks().clone();
        let child_memory_stack = memory_stack.clone();
        let child_rewind_stack = rewind_stack.clone();

        let env_inner = ctx.data().inner();
        let instance_handles = env_inner.main_module_instance_handles();
        let module = instance_handles.module_clone();
        let dynamic_linker = env_inner.linker().cloned();
        let spawn_type = if let Some(linker) = dynamic_linker {
            let func_env = ctx.as_ref();
            let store = ctx.as_store_mut();
            SpawnType::NewLinkerInstanceGroup(linker, func_env, store, true)
        } else {
            let memory = instance_handles.memory_clone();
            SpawnType::CopyMemory(memory, ctx.as_store_ref())
        };

        // Spawn a new process with this current execution environment
        let signaler = Box::new(child_env.process.clone());
        {
            let runtime = runtime.clone();
            let tasks = tasks.clone();
            let tasks_outer = tasks.clone();
            let child_store_data = store_data.clone();

            let run = move |mut props: TaskWasmRunProperties| {
                let ctx = props.ctx;
                let mut store = props.store;

                // Rewind the stack and carry on
                {
                    trace!("rewinding child");
                    let mut ctx = ctx.env.clone().into_mut(&mut store);
                    let (data, mut store) = ctx.data_and_store_mut();
                    match rewind::<M, _>(
                        ctx,
                        Some(child_memory_stack),
                        child_rewind_stack,
                        child_store_data.clone(),
                        ForkResult {
                            pid: 0,
                            ret: Errno::Success,
                        },
                    ) {
                        Errno::Success => OnCalledAction::InvokeAgain,
                        err => {
                            warn!(
                                "wasm rewind failed - could not rewind the stack - errno={}",
                                err
                            );
                            return;
                        }
                    };
                }

                // Invoke the start function
                run::<M>(ctx, store, child_handle, None);
            };

            let spawn_result = tasks_outer
                .task_wasm(
                    TaskWasm::new(Box::new(run), child_env, module, false, false)
                        .with_globals(snapshot)
                        .with_memory(spawn_type),
                )
                .map_err(|err| {
                    warn!(
                        "failed to fork as the process could not be spawned - {}",
                        err
                    );
                    err
                });
            if spawn_result.is_err() {
                {
                    let mut inner = ctx.data().process.lock();
                    inner.children.retain(|child| child.pid != child_pid);
                }
                ctx.data().control_plane.remove_process(child_pid);
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    ForkResult {
                        pid: 0,
                        ret: Errno::Perm,
                    },
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                }
            } else {
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    ForkResult {
                        pid: child_pid.raw() as Pid,
                        ret: Errno::Success,
                    },
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                }
            }
        }
    })
    .map(|ret| ForkOutcome { pid: 0, ret })
}

fn run<M: MemorySize>(
    ctx: WasiFunctionEnv,
    mut store: Store,
    child_handle: WasiThreadHandle,
    rewind_state: Option<(RewindState, RewindResultType)>,
) -> ExitCode {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();
    let pid = env.pid();
    let tid = env.tid();

    // If we need to rewind then do so
    if let Some((rewind_state, rewind_result)) = rewind_state {
        let mut ctx = ctx.env.clone().into_mut(&mut store);
        let res = rewind_ext::<M>(
            &mut ctx,
            Some(rewind_state.memory_stack),
            rewind_state.rewind_stack,
            rewind_state.store_data,
            rewind_result,
        );
        if res != Errno::Success {
            return res.into();
        }
    }

    let mut ret: ExitCode = Errno::Success.into();
    let (mut store, err) = if ctx.data(&store).thread.is_main() {
        trace!(%pid, %tid, "re-invoking main");
        let env_inner = ctx.data(&store).inner();
        let module_handles = env_inner.main_module_instance_handles();
        let Some(start) = module_handles.start.clone() else {
            warn!(%pid, %tid, "forked process is missing _start export");
            return Errno::Noexec.into();
        };
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), vec![])
    } else {
        trace!(%pid, %tid, "re-invoking thread_spawn");
        let env_inner = ctx.data(&store).inner();
        let module_handles = env_inner.main_module_instance_handles();
        let Some(start) = module_handles.thread_spawn.clone() else {
            warn!(%pid, %tid, "forked thread is missing wasi_thread_start export");
            return Errno::Noexec.into();
        };
        let params = vec![0i32.into(), 0i32.into()];
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), params)
    };
    if let Err(err) = err {
        match err.downcast::<WasiError>() {
            Ok(WasiError::Exit(exit_code)) => {
                ret = exit_code;
            }
            Ok(WasiError::DeepSleep(deep)) => {
                trace!(%pid, %tid, "entered a deep sleep");

                // Create the respawn function
                let respawn = {
                    let tasks = tasks.clone();
                    let rewind_state = deep.rewind;
                    move |ctx, store, rewind_result| {
                        run::<M>(
                            ctx,
                            store,
                            child_handle,
                            Some((
                                rewind_state,
                                RewindResultType::RewindWithResult(rewind_result),
                            )),
                        );
                    }
                };

                /// Spawns the WASM process after a trigger
                unsafe {
                    tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
                };
                return Errno::Success.into();
            }
            _ => {}
        }
    }
    trace!(%pid, %tid, "child exited (code = {})", ret);

    // Clean up the environment and return the result
    ctx.on_exit((&mut store), Some(ret));

    // We drop the handle at the last moment which will close the thread
    drop(child_handle);
    ret
}
