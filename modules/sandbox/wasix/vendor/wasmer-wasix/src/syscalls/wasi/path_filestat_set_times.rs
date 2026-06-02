use super::*;
use crate::syscalls::*;

/// ### `path_filestat_set_times()`
/// Update time metadata on a file or directory
/// Inputs:
/// - `Fd fd`
///     The directory relative to which the path is resolved
/// - `LookupFlags flags`
///     Flags to control how the path is understood
/// - `const char *path`
///     String containing the file path
/// - `u32 path_len`
///     The length of the `path` string
/// - `Timestamp st_atim`
///     The timestamp that the last accessed time attribute is set to
/// -  `Timestamp st_mtim`
///     The timestamp that the last modified time attribute is set to
/// - `Fstflags fst_flags`
///     A bitmask controlling which attributes are set
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, %st_atim, %st_mtim), ret)]
pub fn path_filestat_set_times<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: LookupFlags,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    st_atim: Timestamp,
    st_mtim: Timestamp,
    fst_flags: Fstflags,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    let path_string = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path_string.as_str());

    wasi_try_ok!(path_filestat_set_times_internal(
        &mut ctx,
        fd,
        flags,
        &path_string,
        st_atim,
        st_mtim,
        fst_flags
    ));
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_path_set_times(
            &mut ctx,
            fd,
            flags,
            path_string,
            st_atim,
            st_mtim,
            fst_flags,
        )
        .map_err(|err| {
            tracing::error!("failed to save file set times event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Ok(Errno::Success)
}

pub(crate) fn path_filestat_set_times_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: LookupFlags,
    path: &str,
    st_atim: Timestamp,
    st_mtim: Timestamp,
    fst_flags: Fstflags,
) -> Result<(), Errno> {
    let env = ctx.data();
    let (_memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };
    let fd_entry = state.fs.get_fd(fd)?;
    if !fd_entry
        .inner
        .rights
        .contains(Rights::PATH_FILESTAT_SET_TIMES)
        && !matches!(
            fd_entry.inode.read().deref(),
            Kind::Dir { .. } | Kind::Root { .. }
        )
    {
        return Err(Errno::Access);
    }
    if (fst_flags.contains(Fstflags::SET_ATIM) && fst_flags.contains(Fstflags::SET_ATIM_NOW))
        || (fst_flags.contains(Fstflags::SET_MTIM) && fst_flags.contains(Fstflags::SET_MTIM_NOW))
    {
        return Err(Errno::Inval);
    }
    let mut trailing_directory_metadata = None;
    if path_requires_directory(path) {
        let Some(key) =
            crate::syscalls::wasix::key_for_path_at(env, fd, path).map(std::path::PathBuf::from)
        else {
            return Err(Errno::Noent);
        };
        match state.fs.root_fs.metadata(&key) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                trailing_directory_metadata = Some(metadata);
            }
            Ok(_) => return Err(Errno::Notdir),
            Err(error) => return Err(super::path_link::io_error_into_wasi_err(error.into())),
        }
    }

    let follows_symlink = path_requires_directory(path) || flags & __WASI_LOOKUP_SYMLINK_FOLLOW != 0;
    let file_inode = state.fs.get_inode_at_path(inodes, fd, path, follows_symlink);
    let file_inode = match file_inode {
        Ok(inode) => Some(inode),
        Err(err) if trailing_directory_metadata.is_some() => {
            tracing::trace!(
                ?err,
                path,
                "using VFS metadata for trailing-directory path outside inode cache"
            );
            None
        }
        Err(err) => return Err(err),
    };

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
        if let Some(file_inode) = &file_inode {
            let stat = file_inode.stat.read().unwrap();
            (atime.unwrap_or(stat.st_atim), mtime.unwrap_or(stat.st_mtim))
        } else {
            let metadata = trailing_directory_metadata
                .as_ref()
                .expect("metadata exists when inode lookup is skipped");
            (
                atime.unwrap_or(metadata.accessed()),
                mtime.unwrap_or(metadata.modified()),
            )
        }
    };
    path_set_times_at(env, fd, path, follows_symlink, next_atime, next_mtime)?;

    if let Some(file_inode) = file_inode {
        {
            let mut stat = file_inode.stat.write().unwrap();
            stat.st_atim = next_atime;
            stat.st_mtim = next_mtime;
        }

        if let Kind::File {
            handle: Some(handle),
            ..
        } = file_inode.kind.write().unwrap().deref()
        {
            let mut handle = handle.write().unwrap();
            handle.set_times(atime, mtime);
        }
    }

    Ok(())
}

fn path_requires_directory(path: &str) -> bool {
    path.ends_with('/') && path != "/"
}
