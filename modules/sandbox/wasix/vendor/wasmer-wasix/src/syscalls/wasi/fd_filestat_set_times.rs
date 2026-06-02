use super::*;
use crate::syscalls::*;

/// ### `fd_filestat_set_times()`
/// Set timestamp metadata on a file
/// Inputs:
/// - `Timestamp st_atim`
///     Last accessed time
/// - `Timestamp st_mtim`
///     Last modified time
/// - `Fstflags fst_flags`
///     Bit-vector for controlling which times get set
#[instrument(level = "trace", skip_all, fields(%fd, %st_atim, %st_mtim), ret)]
pub fn fd_filestat_set_times(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    st_atim: Timestamp,
    st_mtim: Timestamp,
    fst_flags: Fstflags,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    wasi_try_ok!(fd_filestat_set_times_internal(
        &mut ctx, fd, st_atim, st_mtim, fst_flags
    ));
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_set_times(&mut ctx, fd, st_atim, st_mtim, fst_flags).map_err(
            |err| {
                tracing::error!("failed to save file set times event - {}", err);
                WasiError::Exit(ExitCode::from(Errno::Fault))
            },
        )?;
    }

    Ok(Errno::Success)
}

pub(crate) fn fd_filestat_set_times_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    st_atim: Timestamp,
    st_mtim: Timestamp,
    fst_flags: Fstflags,
) -> Result<(), Errno> {
    let env = ctx.data();
    let (_, mut state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
    let fd_entry = state.fs.get_fd(fd)?;

    if !fd_entry
        .inner
        .rights
        .contains(Rights::FD_FILESTAT_SET_TIMES)
    {
        return Err(Errno::Access);
    }

    if (fst_flags.contains(Fstflags::SET_ATIM) && fst_flags.contains(Fstflags::SET_ATIM_NOW))
        || (fst_flags.contains(Fstflags::SET_MTIM) && fst_flags.contains(Fstflags::SET_MTIM_NOW))
    {
        return Err(Errno::Inval);
    }

    let inode = fd_entry.inode;

    let sets_atime = fst_flags.contains(Fstflags::SET_ATIM)
        || fst_flags.contains(Fstflags::SET_ATIM_NOW);
    let sets_mtime = fst_flags.contains(Fstflags::SET_MTIM)
        || fst_flags.contains(Fstflags::SET_MTIM_NOW);
    if !sets_atime && !sets_mtime {
        return Ok(());
    }

    let mut atime = None;
    let mut mtime = None;

    if sets_atime {
        let time_to_set = if fst_flags.contains(Fstflags::SET_ATIM) {
            st_atim
        } else {
            get_current_time_in_nanos()?
        };
        atime = Some(time_to_set);
    }

    if sets_mtime {
        let time_to_set = if fst_flags.contains(Fstflags::SET_MTIM) {
            st_mtim
        } else {
            get_current_time_in_nanos()?
        };
        mtime = Some(time_to_set);
    }

    let (next_atime, next_mtime) = {
        let stat = inode.stat.read().unwrap();
        (atime.unwrap_or(stat.st_atim), mtime.unwrap_or(stat.st_mtim))
    };
    fd_set_times(env, fd, next_atime, next_mtime)?;

    {
        let mut stat = inode.stat.write().unwrap();
        stat.st_atim = next_atime;
        stat.st_mtim = next_mtime;
    }

    Ok(())
}
