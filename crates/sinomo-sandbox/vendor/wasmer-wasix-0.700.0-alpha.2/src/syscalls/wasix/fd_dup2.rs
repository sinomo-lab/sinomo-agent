use super::*;
use crate::syscalls::*;

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

pub(crate) fn fd_dup_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    min_result_fd: WasiFd,
    cloexec: bool,
) -> Result<WasiFd, Errno> {
    let env = ctx.data();
    let (_, mut state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
    if min_result_fd > 0 {
        if fd == min_result_fd {
            return Ok(min_result_fd);
        }
        if state.fs.get_fd(min_result_fd).is_ok() {
            state.fs.close_fd(min_result_fd)?;
        }
        let mut fd_map = state.fs.fd_map.write().unwrap();
        let fd_entry = fd_map.get(fd).ok_or(Errno::Badf)?;
        let new_fd_entry = Fd {
            inner: FdInner {
                offset: fd_entry.inner.offset.clone(),
                rights: fd_entry.inner.rights_inheriting,
                fd_flags: {
                    let mut f = fd_entry.inner.fd_flags;
                    f.set(Fdflagsext::CLOEXEC, cloexec);
                    f
                },
                ..fd_entry.inner
            },
            inode: fd_entry.inode.clone(),
            ..*fd_entry
        };
        if !fd_map.insert(true, min_result_fd, new_fd_entry) {
            return Err(Errno::Inval);
        }
        return Ok(min_result_fd);
    }
    let fd = state.fs.clone_fd_ext(fd, min_result_fd, Some(cloexec))?;
    Ok(fd)
}
