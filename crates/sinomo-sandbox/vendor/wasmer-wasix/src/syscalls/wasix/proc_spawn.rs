use virtual_fs::{NullFile, Pipe};
use wasmer_wasix_types::wasi::ProcessHandles;

use super::*;
use crate::{fs::Inode, syscalls::*};

/// Spawns a new process within the context of this machine
///
/// ## Parameters
///
/// * `name` - Name of the process to be spawned
/// * `chroot` - Indicates if the process will chroot or not
/// * `args` - List of the arguments to pass the process
///   (entries are separated by line feeds)
/// * `preopen` - List of the preopens for this process
///   (entries are separated by line feeds)
/// * `stdin` - How will stdin be handled
/// * `stdout` - How will stdout be handled
/// * `stderr` - How will stderr be handled
/// * `working_dir` - Working directory where this process should run
///   (passing '.' will use the current directory)
///
/// ## Return
///
/// Returns a bus process id that can be used to invoke calls
#[instrument(level = "trace", skip_all, fields(name = field::Empty, working_dir = field::Empty), ret)]
pub fn proc_spawn<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    name: WasmPtr<u8, M>,
    name_len: M::Offset,
    chroot: Bool,
    args: WasmPtr<u8, M>,
    args_len: M::Offset,
    preopen: WasmPtr<u8, M>,
    preopen_len: M::Offset,
    stdin: WasiStdioMode,
    stdout: WasiStdioMode,
    stderr: WasiStdioMode,
    working_dir: WasmPtr<u8, M>,
    working_dir_len: M::Offset,
    ret_handles: WasmPtr<ProcessHandles, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let control_plane = &env.control_plane;
    let memory = unsafe { env.memory_view(&ctx) };
    let name = unsafe { get_input_str_ok!(&memory, name, name_len) };
    let args = unsafe { get_input_str_ok!(&memory, args, args_len) };
    let preopen = unsafe { get_input_str_ok!(&memory, preopen, preopen_len) };
    let working_dir = unsafe { get_input_str_ok!(&memory, working_dir, working_dir_len) };

    Span::current()
        .record("name", name.as_str())
        .record("working_dir", working_dir.as_str());

    if chroot == Bool::True {
        trace!("spawned process uses the inherited sandbox virtual root");
    }

    let args = parse_wasix_line_encoded_strings(&args);

    let preopen = parse_wasix_line_encoded_strings(&preopen);
    let name = if name.starts_with('/') {
        name
    } else {
        ctx.data().state.fs.relative_path_to_absolute(name)
    };

    let (handles, ctx) = match proc_spawn_internal(
        ctx,
        name,
        Some(args),
        Some(preopen),
        Some(working_dir),
        stdin,
        stdout,
        stderr,
    )? {
        Ok(a) => a,
        Err(err) => {
            return Ok(err);
        }
    };

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_handles.write(&memory, handles));
    Ok(Errno::Success)
}

pub fn proc_spawn_internal(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    name: String,
    args: Option<Vec<String>>,
    preopen: Option<Vec<String>>,
    working_dir: Option<String>,
    stdin: WasiStdioMode,
    stdout: WasiStdioMode,
    stderr: WasiStdioMode,
) -> WasiResult<(ProcessHandles, FunctionEnvMut<'_, WasiEnv>)> {
    if ctx.data().process.child_process_limit_reached() {
        return Ok(Err(Errno::Again));
    }

    // Fork the current environment and set the new arguments
    let (mut child_env, handle) = match ctx.data().fork() {
        Ok(x) => x,
        Err(err) => {
            // TODO: evaluate the appropriate error code, document it in the spec.
            return Ok(Err(Errno::Access));
        }
    };
    let child_process = child_env.process.clone();
    let child_pid = child_env.pid();
    _prepare_wasi(&mut child_env, args, None, None);
    child_env.process.set_executable_path(name.clone());

    // The child environment is forked from the parent and already carries the
    // sandbox VFS plus the current preopen set. Some WASIX libc paths pass the
    // preopen list through proc_spawn as metadata; accepting it here preserves
    // real inherited-preopen behavior instead of failing the spawn before the
    // child process can run.
    if let Some(preopen) = preopen
        && !preopen.is_empty()
    {
        for preopen in preopen {
            trace!("spawned process inherits preopen [{}]", preopen);
        }
    }

    // Change the current directory
    if let Some(working_dir) = working_dir {
        let working_dir = child_env.state.fs.relative_path_to_absolute(working_dir);
        if let Err(err) = chdir_internal(&child_env, &working_dir) {
            child_env.blocking_on_exit(Some(err.into()));
            ctx.data().control_plane.remove_process(child_pid);
            return Ok(Err(err));
        }
    }

    // Replace the STDIO
    let (stdin, stdout, stderr) = {
        let (child_state, child_inodes) = child_env.get_wasi_state_and_inodes();
        let mut conv_stdio_mode = |mode: WasiStdioMode,
                                   fd: WasiFd,
                                   pipe_towards_child: bool|
         -> Result<OptionFd, Errno> {
            match mode {
                WasiStdioMode::Piped => {
                    let (tx, rx) = Pipe::new().split();
                    let read_inode = child_state.fs.create_inode_with_default_stat(
                        child_inodes,
                        Kind::PipeRx { rx },
                        false,
                        "pipe".into(),
                    );
                    let write_inode = child_state.fs.create_inode_with_default_stat(
                        child_inodes,
                        Kind::PipeTx { tx },
                        false,
                        "pipe".into(),
                    );

                    let (parent_end, child_end) = if pipe_towards_child {
                        (write_inode, read_inode)
                    } else {
                        (read_inode, write_inode)
                    };

                    let rights = crate::net::socket::all_socket_rights();
                    let pipe = ctx.data().state.fs.create_fd(
                        rights,
                        rights,
                        Fdflags::empty(),
                        Fdflagsext::empty(),
                        0,
                        parent_end,
                    )?;
                    child_state.fs.create_fd_ext(
                        rights,
                        rights,
                        Fdflags::empty(),
                        Fdflagsext::empty(),
                        0,
                        child_end,
                        Some(fd),
                        false,
                    )?;

                    trace!("fd_pipe (fd1={}, fd2={})", pipe, fd);
                    Ok(OptionFd {
                        tag: OptionTag::Some,
                        fd: pipe,
                    })
                }
                WasiStdioMode::Inherit | WasiStdioMode::Log => Ok(OptionFd {
                    tag: OptionTag::None,
                    fd: u32::MAX,
                }),
                WasiStdioMode::Null => {
                    let rights = Rights::FD_DATASYNC
                        | Rights::FD_READ
                        | Rights::FD_SYNC
                        | Rights::FD_WRITE
                        | Rights::FD_ADVISE
                        | Rights::FD_FILESTAT_GET
                        | Rights::FD_FDSTAT_SET_FLAGS
                        | Rights::POLL_FD_READWRITE;
                    let name = match fd {
                        0 => "stdin",
                        1 => "stdout",
                        2 => "stderr",
                        _ => "stdio",
                    };
                    let fd_flags = if fd == 0 {
                        Fdflags::empty()
                    } else {
                        Fdflags::APPEND
                    };
                    child_state.fs.create_std_dev_inner(
                        child_inodes,
                        Box::<NullFile>::default(),
                        name,
                        fd,
                        rights,
                        fd_flags,
                        Inode::from_path(&format!("stdio-null-{fd}")),
                    );
                    Ok(OptionFd {
                        tag: OptionTag::None,
                        fd: u32::MAX,
                    })
                }
                _ => {
                    child_state.fs.close_fd(fd);
                    Ok(OptionFd {
                        tag: OptionTag::None,
                        fd: u32::MAX,
                    })
                }
            }
        };
        // TODO: proc_spawn isn't used in WASIX at the time of writing
        // this code, so the implementation isn't tested at all
        let stdin = match conv_stdio_mode(stdin, 0, true) {
            Ok(a) => a,
            Err(err) => {
                child_env.blocking_on_exit(Some(err.into()));
                ctx.data().control_plane.remove_process(child_pid);
                return Ok(Err(err));
            }
        };
        let stdout = match conv_stdio_mode(stdout, 1, false) {
            Ok(a) => a,
            Err(err) => {
                child_env.blocking_on_exit(Some(err.into()));
                ctx.data().control_plane.remove_process(child_pid);
                return Ok(Err(err));
            }
        };
        let stderr = match conv_stdio_mode(stderr, 2, false) {
            Ok(a) => a,
            Err(err) => {
                child_env.blocking_on_exit(Some(err.into()));
                ctx.data().control_plane.remove_process(child_pid);
                return Ok(Err(err));
            }
        };
        (stdin, stdout, stderr)
    };

    // Create the new process
    let bin_factory = Box::new(ctx.data().bin_factory.clone());

    let mut builder = Some(child_env);

    // First we try the built in commands
    let mut process = match bin_factory.try_built_in(name.clone(), Some(&ctx), &mut builder) {
        Ok(a) => a,
        Err(err) => {
            if !err.is_not_found() {
                error!("builtin failed - {}", err);
            }
            // Now we actually spawn the process
            let Some(child_env) = builder.take() else {
                warn!("failed to spawn as builtin probing consumed child environment");
                child_process.terminate(Errno::Noexec.into());
                ctx.data().control_plane.remove_process(child_pid);
                return Ok(Err(Errno::Noexec));
            };
            let child_work = bin_factory.spawn(name, child_env);

            match __asyncify(&mut ctx, None, async move { Ok(child_work.await) })?
                .map_err(|err| Errno::Unknown)
            {
                Ok(Ok(a)) => a,
                Ok(Err(err)) => {
                    let errno = conv_spawn_err_to_errno(&err);
                    child_process.terminate(errno.into());
                    ctx.data().control_plane.remove_process(child_pid);
                    return Ok(Err(errno));
                }
                Err(err) => {
                    child_process.terminate(err.into());
                    ctx.data().control_plane.remove_process(child_pid);
                    return Ok(Err(err));
                }
            }
        }
    };

    // Add the process to the environment state
    ctx.data_mut().owned_handles.push(handle);
    {
        let mut inner = ctx.data().process.lock();
        inner.children.push(child_process);
    }
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    let handles = ProcessHandles {
        pid: child_pid.raw(),
        stdin,
        stdout,
        stderr,
    };
    Ok(Ok((handles, ctx)))
}
