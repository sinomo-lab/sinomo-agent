use super::*;
use crate::syscalls::*;

/// ### `fd_datasync()`
/// Synchronize the file data to disk
/// Inputs:
/// - `Fd fd`
///     The file descriptor to sync
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn fd_datasync(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let state = env.state.clone();
    let fd_entry = wasi_try_ok!(state.fs.get_fd(fd));
    if !fd_entry.inner.rights.contains(Rights::FD_DATASYNC) {
        return Ok(Errno::Access);
    }
    let inode = fd_entry.inode.clone();

    #[allow(clippy::await_holding_lock)]
    wasi_try_ok!(__asyncify(&mut ctx, None, async move {
        state.fs.flush(fd).await.map(|_| Errno::Success)
    })?);

    let size = {
        let guard = inode.read();
        match guard.deref() {
            Kind::File {
                handle: Some(handle),
                ..
            } => {
                let mut handle = handle.write().unwrap();
                if let Err(error) = handle.refresh_metadata() {
                    return Ok(fs_error_into_wasi_err(error));
                }
                Some(handle.size())
            }
            Kind::Buffer { buffer } => Some(buffer.len() as u64),
            _ => None,
        }
    };
    if let Some(size) = size {
        inode.stat.write().unwrap().st_size = size;
    }

    Ok(Errno::Success)
}
