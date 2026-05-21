use std::sync::atomic::AtomicUsize;

use virtual_fs::Pipe;

use super::*;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;

const LINUX_O_CLOEXEC: i32 = 0o2000000;
const LINUX_O_NONBLOCK: i32 = 0o4000;

// Used to make pipe end names unique. This is necessary since we use
// a hash of the name to calculate inode numbers. The actual number
// has no other meaning.
static PIPE_NUMBER: AtomicUsize = AtomicUsize::new(0);

/// ### `fd_pipe()`
/// Creates ta pipe that feeds data between two file handles
/// Output:
/// - `Fd`
///     First file handle that represents the read end of the pipe
/// - `Fd`
///     Second file handle that represents the write end of the pipe
#[instrument(level = "trace", skip_all, fields(read_fd = field::Empty, write_fd = field::Empty), ret)]
pub fn fd_pipe<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ro_read_fd: WasmPtr<WasiFd, M>,
    ro_write_fd: WasmPtr<WasiFd, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let (read_fd, write_fd) = wasi_try_ok!(fd_pipe_internal(&mut ctx, None, None));
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_pipe(&mut ctx, read_fd, write_fd).map_err(|err| {
            tracing::error!("failed to save create pipe event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    let env = ctx.data();
    let (memory, state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    Span::current()
        .record("read_fd", read_fd)
        .record("write_fd", write_fd);

    wasi_try_mem_ok!(ro_read_fd.write(&memory, read_fd));
    wasi_try_mem_ok!(ro_write_fd.write(&memory, write_fd));

    Ok(Errno::Success)
}

/// ### `pipe()`
/// POSIX-shaped pipe wrapper. Writes read/write fds into `fds[0]` and `fds[1]`
/// and returns 0 on success or a negative errno value on failure.
#[instrument(level = "trace", skip_all, fields(read_fd = field::Empty, write_fd = field::Empty), ret)]
pub fn pipe<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fds: WasmPtr<WasiFd, M>,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let (read_fd, write_fd) = match fd_pipe_internal(&mut ctx, None, None) {
        Ok(fds) => fds,
        Err(err) => return Ok(-err.to_native()),
    };
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_pipe(&mut ctx, read_fd, write_fd).map_err(|err| {
            tracing::error!("failed to save create pipe event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    Span::current()
        .record("read_fd", read_fd)
        .record("write_fd", write_fd);

    if let Err(err) = fds.write(&memory, read_fd) {
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    let write_ptr = match fds.add_offset(match to_offset::<M>(1) {
        Ok(offset) => offset,
        Err(err) => return Ok(-err.to_native()),
    }) {
        Ok(ptr) => ptr,
        Err(err) => return Ok(-crate::mem_error_to_wasi(err).to_native()),
    };
    if let Err(err) = write_ptr.write(&memory, write_fd) {
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }

    Ok(0)
}

/// ### `pipe2()`
/// Linux-shaped pipe creation with descriptor/file flags.
#[instrument(level = "trace", skip_all, fields(read_fd = field::Empty, write_fd = field::Empty, %flags), ret)]
pub fn pipe2<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fds: WasmPtr<WasiFd, M>,
    flags: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if flags & !(LINUX_O_CLOEXEC | LINUX_O_NONBLOCK) != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    let (read_fd, write_fd) = match fd_pipe_internal(&mut ctx, None, None) {
        Ok(fds) => fds,
        Err(err) => return Ok(-err.to_native()),
    };
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_pipe(&mut ctx, read_fd, write_fd).map_err(|err| {
            tracing::error!("failed to save create pipe event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    if let Err(err) = apply_linux_pipe_flags(&mut ctx, read_fd, write_fd, flags) {
        let _ = ctx.data().state.fs.close_fd(read_fd);
        let _ = ctx.data().state.fs.close_fd(write_fd);
        return Ok(-err.to_native());
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    Span::current()
        .record("read_fd", read_fd)
        .record("write_fd", write_fd);

    if let Err(err) = fds.write(&memory, read_fd) {
        let _ = ctx.data().state.fs.close_fd(read_fd);
        let _ = ctx.data().state.fs.close_fd(write_fd);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    let write_ptr = match fds.add_offset(match to_offset::<M>(1) {
        Ok(offset) => offset,
        Err(err) => {
            let _ = ctx.data().state.fs.close_fd(read_fd);
            let _ = ctx.data().state.fs.close_fd(write_fd);
            return Ok(-err.to_native());
        }
    }) {
        Ok(ptr) => ptr,
        Err(err) => {
            let _ = ctx.data().state.fs.close_fd(read_fd);
            let _ = ctx.data().state.fs.close_fd(write_fd);
            return Ok(-crate::mem_error_to_wasi(err).to_native());
        }
    };
    if let Err(err) = write_ptr.write(&memory, write_fd) {
        let _ = ctx.data().state.fs.close_fd(read_fd);
        let _ = ctx.data().state.fs.close_fd(write_fd);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }

    Ok(0)
}

fn apply_linux_pipe_flags(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    read_fd: WasiFd,
    write_fd: WasiFd,
    flags: i32,
) -> Result<(), Errno> {
    if flags & LINUX_O_NONBLOCK != 0 {
        for fd in [read_fd, write_fd] {
            let ret = fd_fdstat_set_flags_internal(ctx, fd, Fdflags::NONBLOCK)
                .map_err(|_| Errno::Fault)?;
            if ret != Errno::Success {
                return Err(ret);
            }
        }
    }
    if flags & LINUX_O_CLOEXEC != 0 {
        for fd in [read_fd, write_fd] {
            let ret =
                fd_fdflags_set_internal(ctx, fd, Fdflagsext::CLOEXEC).map_err(|_| Errno::Fault)?;
            if ret != Errno::Success {
                return Err(ret);
            }
        }
    }
    Ok(())
}

pub fn fd_pipe_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    with_read_fd: Option<WasiFd>,
    with_write_fd: Option<WasiFd>,
) -> Result<(WasiFd, WasiFd), Errno> {
    let env = ctx.data();
    let (memory, state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };
    let (tx, rx) = Pipe::new().split();

    // FIXME: since a hash of the inode name is used to calculate the inode number, this may
    // or may not break journals that include pipes and are compacted.
    let pipe_no = PIPE_NUMBER.fetch_add(1, Ordering::SeqCst);

    let rx_inode = state.fs.create_inode_with_default_stat(
        inodes,
        Kind::PipeRx { rx },
        false,
        format!("pipe{pipe_no}-rx").into(),
    );
    let tx_inode = state.fs.create_inode_with_default_stat(
        inodes,
        Kind::PipeTx { tx },
        false,
        format!("pipe{pipe_no}-tx").into(),
    );

    let rights = Rights::FD_SYNC
        | Rights::FD_DATASYNC
        | Rights::POLL_FD_READWRITE
        | Rights::SOCK_SEND
        | Rights::FD_FDSTAT_SET_FLAGS
        | Rights::FD_FILESTAT_GET;

    let read_rights = rights | Rights::FD_READ;
    let write_rights = rights | Rights::FD_WRITE;

    let read_fd = if let Some(fd) = with_read_fd {
        state
            .fs
            .with_fd(
                read_rights,
                read_rights,
                Fdflags::empty(),
                Fdflagsext::empty(),
                0,
                rx_inode,
                fd,
            )
            .map(|()| fd)?
    } else {
        state.fs.create_fd(
            read_rights,
            read_rights,
            Fdflags::empty(),
            Fdflagsext::empty(),
            0,
            rx_inode,
        )?
    };

    let write_fd = if let Some(fd) = with_write_fd {
        state
            .fs
            .with_fd(
                write_rights,
                write_rights,
                Fdflags::empty(),
                Fdflagsext::empty(),
                0,
                tx_inode,
                fd,
            )
            .map(|()| fd)?
    } else {
        state.fs.create_fd(
            write_rights,
            write_rights,
            Fdflags::empty(),
            Fdflagsext::empty(),
            0,
            tx_inode,
        )?
    };

    sync_process_metadata(env);
    Ok((read_fd, write_fd))
}
