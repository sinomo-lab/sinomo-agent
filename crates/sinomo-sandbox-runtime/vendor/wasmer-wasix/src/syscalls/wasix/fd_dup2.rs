use super::*;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;

const LINUX_O_CLOEXEC: i32 = 0o2000000;

/// ### `fd_dup()`
/// Duplicates the file handle
/// Inputs:
/// - `Fd fd`
///   File handle to be cloned
/// Outputs:
/// - `Fd fd`
///   The new file handle that is a duplicate of the original
#[instrument(level = "trace", skip_all, fields(%fd, ret_fd = field::Empty), ret)]
pub fn fd_dup2<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    min_result_fd: WasiFd,
    cloexec: Bool,
    ret_fd: WasmPtr<WasiFd, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let copied_fd = wasi_try_ok!(fd_dup_internal(
        &mut ctx,
        fd,
        min_result_fd,
        cloexec == Bool::True
    ));
    let env = ctx.data();
    sync_process_metadata(env);

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_duplicate(&mut ctx, fd, copied_fd, cloexec == Bool::True)
            .map_err(|err| {
                tracing::error!("failed to save file descriptor duplicate event - {}", err);
                WasiError::Exit(ExitCode::from(Errno::Fault))
            })?;
    }

    Span::current().record("ret_fd", copied_fd);
    let env = ctx.data();
    let (memory, state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
    wasi_try_mem_ok!(ret_fd.write(&memory, copied_fd));

    Ok(Errno::Success)
}

/// ### `dup()`
/// POSIX-shaped descriptor duplication. Returns the new fd or a negative errno.
#[instrument(level = "trace", skip_all, fields(%fd, ret_fd = field::Empty), ret)]
pub fn dup(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let copied_fd = match fd_dup_internal(&mut ctx, fd, 0, false) {
        Ok(fd) => fd,
        Err(err) => return Ok(-err.to_native()),
    };
    let env = ctx.data();
    sync_process_metadata(env);

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_duplicate(&mut ctx, fd, copied_fd, false).map_err(|err| {
            tracing::error!("failed to save file descriptor duplicate event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Span::current().record("ret_fd", copied_fd);
    Ok(copied_fd as i32)
}

/// ### `dup2()`
/// POSIX-shaped descriptor duplication to a requested fd.
#[instrument(level = "trace", skip_all, fields(%fd, %new_fd), ret)]
pub fn dup2(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    new_fd: WasiFd,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if fd == new_fd {
        return match ctx.data().state.fs.get_fd(fd) {
            Ok(_) => Ok(new_fd as i32),
            Err(err) => Ok(-err.to_native()),
        };
    }

    {
        let env = ctx.data();
        let (_, state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
        if let Err(err) = state.fs.get_fd(fd) {
            return Ok(-err.to_native());
        }
        if state.fs.get_fd(new_fd).is_ok() {
            let _ = state.fs.close_fd(new_fd);
        }
    }

    let copied_fd = match fd_dup_internal(&mut ctx, fd, new_fd, false) {
        Ok(copied_fd) if copied_fd == new_fd => copied_fd,
        Ok(copied_fd) => {
            let _ = ctx.data().state.fs.close_fd(copied_fd);
            return Ok(-Errno::Mfile.to_native());
        }
        Err(err) => return Ok(-err.to_native()),
    };
    let env = ctx.data();
    sync_process_metadata(env);

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_duplicate(&mut ctx, fd, copied_fd, false).map_err(|err| {
            tracing::error!("failed to save file descriptor duplicate event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Ok(copied_fd as i32)
}

/// ### `dup3()`
/// Linux-shaped descriptor duplication to a requested fd with descriptor flags.
#[instrument(level = "trace", skip_all, fields(%fd, %new_fd, %flags), ret)]
pub fn dup3(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    new_fd: WasiFd,
    flags: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if fd == new_fd || flags & !LINUX_O_CLOEXEC != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    {
        let env = ctx.data();
        let (_, state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
        if let Err(err) = state.fs.get_fd(fd) {
            return Ok(-err.to_native());
        }
        if state.fs.get_fd(new_fd).is_ok() {
            let _ = state.fs.close_fd(new_fd);
        }
    }

    let cloexec = flags & LINUX_O_CLOEXEC != 0;
    let copied_fd = match fd_dup_internal(&mut ctx, fd, new_fd, cloexec) {
        Ok(copied_fd) if copied_fd == new_fd => copied_fd,
        Ok(copied_fd) => {
            let _ = ctx.data().state.fs.close_fd(copied_fd);
            return Ok(-Errno::Mfile.to_native());
        }
        Err(err) => return Ok(-err.to_native()),
    };
    let env = ctx.data();
    sync_process_metadata(env);

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_duplicate(&mut ctx, fd, copied_fd, cloexec).map_err(|err| {
            tracing::error!("failed to save file descriptor duplicate event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Ok(copied_fd as i32)
}

pub(crate) fn fd_dup_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    min_result_fd: WasiFd,
    cloexec: bool,
) -> Result<WasiFd, Errno> {
    let env = ctx.data();
    let (_, state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
    state.fs.clone_fd_ext(fd, min_result_fd, Some(cloexec))
}
