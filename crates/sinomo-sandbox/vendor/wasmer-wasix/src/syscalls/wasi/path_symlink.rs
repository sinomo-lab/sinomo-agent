use super::*;
use crate::syscalls::*;

/// ### `path_symlink()`
/// Create a symlink
/// Inputs:
/// - `const char *old_path`
///     Array of UTF-8 bytes representing the source path
/// - `u32 old_path_len`
///     The number of bytes to read from `old_path`
/// - `Fd fd`
///     The base directory from which the paths are understood
/// - `const char *new_path`
///     Array of UTF-8 bytes representing the target path
/// - `u32 new_path_len`
///     The number of bytes to read from `new_path`
#[instrument(level = "trace", skip_all, fields(%fd, old_path = field::Empty, new_path = field::Empty), ret)]
pub fn path_symlink<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    old_path: WasmPtr<u8, M>,
    old_path_len: M::Offset,
    fd: WasiFd,
    new_path: WasmPtr<u8, M>,
    new_path_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };
    let old_path_str = unsafe { get_input_str_ok!(&memory, old_path, old_path_len) };
    Span::current().record("old_path", old_path_str.as_str());
    let new_path_str = unsafe { get_input_str_ok!(&memory, new_path, new_path_len) };
    Span::current().record("new_path", new_path_str.as_str());

    wasi_try_ok!(path_symlink_internal(
        &mut ctx,
        &old_path_str,
        fd,
        &new_path_str
    ));
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_path_symlink(&mut ctx, old_path_str, fd, new_path_str).map_err(
            |err| {
                tracing::error!("failed to save path symbolic link event - {}", err);
                WasiError::Exit(ExitCode::from(Errno::Fault))
            },
        )?;
    }

    Ok(Errno::Success)
}

pub fn path_symlink_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    old_path: &str,
    fd: WasiFd,
    new_path: &str,
) -> Result<(), Errno> {
    let env = ctx.data();
    let (memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    let base_fd = state.fs.get_fd(fd)?;
    if !base_fd.inner.rights.contains(Rights::PATH_SYMLINK) {
        return Err(Errno::Access);
    }
    match path_parent_mutation_allowed_at(env, fd, new_path) {
        Ok(true) => {}
        Ok(false) => return Err(Errno::Access),
        Err(err) => return Err(err),
    }
    if path_requires_directory(new_path) {
        let Some(link_path) =
            crate::syscalls::wasix::key_for_path_at(env, fd, new_path).map(std::path::PathBuf::from)
        else {
            return Err(Errno::Noent);
        };
        match state.fs.root_fs.symlink_metadata(&link_path) {
            Ok(metadata) if metadata.file_type().is_dir() => return Err(Errno::Exist),
            Ok(_) => return Err(Errno::Notdir),
            Err(error) => {
                return Err(super::path_link::io_error_into_wasi_err(error.into()));
            }
        }
    }

    let new_path_path = std::path::Path::new(new_path);
    let (target_parent_inode, entry_name) =
        state
            .fs
            .get_parent_inode_at_path(inodes, fd, new_path_path, true)?;
    let host_link_path = {
        let guard = target_parent_inode.read();
        match guard.deref() {
            Kind::Dir { path, .. } => path.join(&entry_name),
            Kind::Root { .. } => return Err(Errno::Notcapable),
            Kind::Socket { .. }
            | Kind::PipeRx { .. }
            | Kind::PipeTx { .. }
            | Kind::DuplexPipe { .. }
            | Kind::EventNotifications { .. }
            | Kind::Epoll { .. } => return Err(Errno::Inval),
            Kind::File { .. } | Kind::Symlink { .. } | Kind::Buffer { .. } => {
                unreachable!("get_parent_inode_at_path returned something other than a Dir or Root")
            }
        }
    };

    // short circuit if anything is wrong, before we create an inode
    {
        let guard = target_parent_inode.read();
        match guard.deref() {
            Kind::Dir { entries, .. } => {
                if entries.contains_key(&entry_name) {
                    return Err(Errno::Exist);
                }
            }
            Kind::Root { .. } => return Err(Errno::Notcapable),
            Kind::Socket { .. }
            | Kind::PipeRx { .. }
            | Kind::PipeTx { .. }
            | Kind::DuplexPipe { .. }
            | Kind::EventNotifications { .. }
            | Kind::Epoll { .. } => return Err(Errno::Inval),
            Kind::File { .. } | Kind::Symlink { .. } | Kind::Buffer { .. } => {
                unreachable!("get_parent_inode_at_path returned something other than a Dir or Root")
            }
        }
    }

    state
        .fs
        .root_fs
        .symlink(
            std::path::Path::new(old_path),
            std::path::Path::new(&host_link_path),
        )
        .map_err(|error| super::path_link::io_error_into_wasi_err(error.into()))?;

    let kind = Kind::Symlink {
        base_po_dir: fd,
        path_to_symlink: std::path::PathBuf::from(new_path),
        relative_path: std::path::PathBuf::from(old_path),
    };
    let new_inode =
        state
            .fs
            .create_inode_with_default_stat(inodes, kind, false, entry_name.clone().into());

    {
        let mut guard = target_parent_inode.write();
        if let Kind::Dir { entries, .. } = guard.deref_mut() {
            entries.insert(entry_name, new_inode);
        }
    }

    Ok(())
}

fn path_requires_directory(path: &str) -> bool {
    path.ends_with('/') && path != "/"
}
