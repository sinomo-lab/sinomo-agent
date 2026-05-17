use super::*;
use crate::os::task::TaskStatus;
use crate::{VIRTUAL_ROOT_FD, WasiProcess};
use crate::syscalls::*;
use crate::syscalls::wasix::default_process_name;

/// ### `path_open()`
/// Open file located at the given path
/// Inputs:
/// - `Fd dirfd`
///     The fd corresponding to the directory that the file is in
/// - `LookupFlags dirflags`
///     Flags specifying how the path will be resolved
/// - `char *path`
///     The path of the file or directory to open
/// - `u32 path_len`
///     The length of the `path` string
/// - `Oflags o_flags`
///     How the file will be opened
/// - `Rights fs_rights_base`
///     The rights of the created file descriptor
/// - `Rights fs_rightsinheriting`
///     The rights of file descriptors derived from the created file descriptor
/// - `Fdflags fs_flags`
///     The flags of the file descriptor
/// Output:
/// - `Fd* fd`
///     The new file descriptor
/// Possible Errors:
/// - `Errno::Access`, `Errno::Badf`, `Errno::Fault`, `Errno::Fbig?`, `Errno::Inval`, `Errno::Io`, `Errno::Loop`, `Errno::Mfile`, `Errno::Nametoolong?`, `Errno::Nfile`, `Errno::Noent`, `Errno::Notdir`, `Errno::Rofs`, and `Errno::Notcapable`
#[instrument(level = "trace", skip_all, fields(%dirfd, path = field::Empty, follow_symlinks = field::Empty, ret_fd = field::Empty), ret)]
pub fn path_open2<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    dirfd: WasiFd,
    dirflags: LookupFlags,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    o_flags: Oflags,
    fs_rights_base: Rights,
    fs_rights_inheriting: Rights,
    fs_flags: Fdflags,
    fd_flags: Fdflagsext,
    fd: WasmPtr<WasiFd, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if dirflags & __WASI_LOOKUP_SYMLINK_FOLLOW != 0 {
        Span::current().record("follow_symlinks", true);
    }
    let env = ctx.data();
    let (memory, mut state, mut inodes) =
        unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };
    /* TODO: find actual upper bound on name size (also this is a path, not a name :think-fish:) */
    let path_len64: u64 = path_len.into();
    if path_len64 > 1024u64 * 1024u64 {
        return Ok(Errno::Nametoolong);
    }

    if path_len64 == 0 {
        return Ok(Errno::Noent);
    }

    // o_flags:
    // - __WASI_O_CREAT (create if it does not exist)
    // - __WASI_O_DIRECTORY (fail if not dir)
    // - __WASI_O_EXCL (fail if file exists)
    // - __WASI_O_TRUNC (truncate size to 0)

    let path_string = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path_string.as_str());

    let out_fd = wasi_try_ok!(path_open_internal(
        ctx.data(),
        dirfd,
        dirflags,
        &path_string,
        o_flags,
        fs_rights_base,
        fs_rights_inheriting,
        fs_flags,
        fd_flags,
        None,
    )?);
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_path_open(
            &mut ctx,
            out_fd,
            dirfd,
            dirflags,
            path_string,
            o_flags,
            fs_rights_base,
            fs_rights_inheriting,
            fs_flags,
            fd_flags,
        )
        .map_err(|err| {
            tracing::error!("failed to save unlink event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    let env = ctx.data();
    let (memory, mut state, mut inodes) =
        unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    Span::current().record("ret_fd", out_fd);

    let fd_ref = fd.deref(&memory);
    wasi_try_mem_ok!(fd_ref.write(out_fd));

    Ok(Errno::Success)
}

pub(crate) fn path_open_internal(
    env: &WasiEnv,
    dirfd: WasiFd,
    dirflags: LookupFlags,
    path: &str,
    o_flags: Oflags,
    fs_rights_base: Rights,
    fs_rights_inheriting: Rights,
    fs_flags: Fdflags,
    fd_flags: Fdflagsext,
    with_fd: Option<WasiFd>,
) -> Result<Result<WasiFd, Errno>, WasiError> {
    let state = env.state.deref();
    let inodes = &state.inodes;
    if let Some(bytes) = proc_generated_process_file(env, path) {
        if o_flags.intersects(Oflags::CREATE | Oflags::TRUNC)
            || fs_rights_base.contains(Rights::FD_WRITE)
            || fs_flags.contains(Fdflags::APPEND)
        {
            return Ok(Err(Errno::Access));
        }
        let inode = wasi_try_ok_ok!(state.fs.create_inode(
            inodes,
            Kind::Buffer { buffer: bytes },
            false,
            path.to_string(),
        ));
        let rights = fs_rights_base & proc_generated_file_rights();
        let inheriting = fs_rights_inheriting & proc_generated_file_rights();
        let fd = wasi_try_ok_ok!(state.fs.create_fd(
            rights,
            inheriting,
            fs_flags,
            fd_flags,
            Fd::READ,
            inode,
        ));
        sync_process_metadata(env);
        return Ok(Ok(fd));
    }
    let path = proc_pid_alias_target(env, state, inodes, path).unwrap_or_else(|| path.to_string());
    let path = path.as_str();

    if let Some(bytes) = proc_generated_file(env, path) {
        if o_flags.intersects(Oflags::CREATE | Oflags::TRUNC)
            || fs_rights_base.contains(Rights::FD_WRITE)
            || fs_flags.contains(Fdflags::APPEND)
        {
            return Ok(Err(Errno::Access));
        }
        let inode = wasi_try_ok_ok!(state.fs.create_inode(
            inodes,
            Kind::Buffer { buffer: bytes },
            false,
            path.to_string(),
        ));
        let rights = fs_rights_base & proc_generated_file_rights();
        let inheriting = fs_rights_inheriting & proc_generated_file_rights();
        let fd = wasi_try_ok_ok!(state.fs.create_fd(
            rights,
            inheriting,
            fs_flags,
            fd_flags,
            Fd::READ,
            inode,
        ));
        sync_process_metadata(env);
        return Ok(Ok(fd));
    }

    let process_tids = env
        .process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw())
        .collect::<Vec<_>>();
    if let Some(target) = proc_path_alias_target(state, inodes, path, &process_tids) {
        return path_open_internal(
            env,
            crate::VIRTUAL_ROOT_FD,
            dirflags,
            &target,
            o_flags,
            fs_rights_base,
            fs_rights_inheriting,
            fs_flags,
            fd_flags,
            with_fd,
        );
    }

    if let Some(special_fd) = special_device_fd(path, fs_rights_base) {
        let fd = wasi_try_ok_ok!(state.fs.clone_fd(special_fd));
        sync_process_metadata(env);
        return Ok(Ok(fd));
    }

    let path_arg = std::path::PathBuf::from(&path);
    let maybe_inode = state.fs.get_inode_at_path(
        inodes,
        dirfd,
        path,
        dirflags & __WASI_LOOKUP_SYMLINK_FOLLOW != 0,
    );

    let working_dir = wasi_try_ok_ok!(state.fs.get_fd(dirfd));
    let working_dir_rights_inheriting = working_dir.inner.rights_inheriting;

    // ASSUMPTION: open rights apply recursively
    if !working_dir.inner.rights.contains(Rights::PATH_OPEN) {
        return Ok(Err(Errno::Access));
    }

    let mut open_flags = 0;
    // TODO: traverse rights of dirs properly
    // COMMENTED OUT: WASI isn't giving appropriate rights here when opening
    //              TODO: look into this; file a bug report if this is a bug
    //
    // Maximum rights: should be the working dir rights
    // Minimum rights: whatever rights are provided
    let adjusted_rights = /*fs_rights_base &*/ working_dir_rights_inheriting;
    let mut open_options = state.fs_new_open_options();

    let target_rights = match maybe_inode {
        Ok(_) => {
            let write_permission = adjusted_rights.contains(Rights::FD_WRITE);

            // append, truncate, and create all require the permission to write
            let (append_permission, truncate_permission, create_permission) = if write_permission {
                (
                    fs_flags.contains(Fdflags::APPEND),
                    o_flags.contains(Oflags::TRUNC),
                    o_flags.contains(Oflags::CREATE),
                )
            } else {
                (false, false, false)
            };

            virtual_fs::OpenOptionsConfig {
                read: fs_rights_base.contains(Rights::FD_READ),
                write: write_permission,
                create_new: create_permission && o_flags.contains(Oflags::EXCL),
                create: create_permission,
                append: append_permission,
                truncate: truncate_permission,
            }
        }
        Err(_) => virtual_fs::OpenOptionsConfig {
            append: fs_flags.contains(Fdflags::APPEND),
            write: fs_rights_base.contains(Rights::FD_WRITE),
            read: fs_rights_base.contains(Rights::FD_READ),
            create_new: o_flags.contains(Oflags::CREATE) && o_flags.contains(Oflags::EXCL),
            create: o_flags.contains(Oflags::CREATE),
            truncate: o_flags.contains(Oflags::TRUNC),
        },
    };

    let parent_rights = virtual_fs::OpenOptionsConfig {
        read: working_dir.inner.rights.contains(Rights::FD_READ),
        write: working_dir.inner.rights.contains(Rights::FD_WRITE),
        // The parent is a directory, which is why these options
        // aren't inherited from the parent (append / truncate doesn't work on directories)
        create_new: true,
        create: true,
        append: true,
        truncate: true,
    };

    let minimum_rights = target_rights.minimum_rights(&parent_rights);

    open_options.options(minimum_rights.clone());

    let orig_path = path;

    let inode = if let Ok(inode) = maybe_inode {
        // Happy path, we found the file we're trying to open
        let processing_inode = inode.clone();
        let mut guard = processing_inode.write();

        let deref_mut = guard.deref_mut();

        if o_flags.contains(Oflags::EXCL) && o_flags.contains(Oflags::CREATE) {
            return Ok(Err(Errno::Exist));
        }

        match deref_mut {
            Kind::File {
                handle, path, fd, ..
            } => {
                if let Some(special_fd) = fd {
                    // short circuit if we're dealing with a special file
                    assert!(handle.is_some());
                    return Ok(Ok(*special_fd));
                }
                if o_flags.contains(Oflags::DIRECTORY) || orig_path.ends_with('/') {
                    return Ok(Err(Errno::Notdir));
                }

                let open_options = open_options
                    .write(minimum_rights.write)
                    .create(minimum_rights.create)
                    .append(false)
                    .truncate(minimum_rights.truncate);

                if minimum_rights.read {
                    open_flags |= Fd::READ;
                }
                if minimum_rights.write {
                    open_flags |= Fd::WRITE;
                }
                if minimum_rights.create {
                    open_flags |= Fd::CREATE;
                }
                if minimum_rights.truncate {
                    open_flags |= Fd::TRUNCATE;
                }
                // TODO: I strongly suspect that assigning the handle unconditionally
                // breaks opening the same file multiple times.
                *handle = Some(Arc::new(std::sync::RwLock::new(wasi_try_ok_ok!(
                    open_options.open(&path).map_err(fs_error_into_wasi_err)
                ))));

                if let Some(handle) = handle {
                    let handle = handle.read().unwrap();
                    if let Some(fd) = handle.get_special_fd() {
                        // We clone the file descriptor so that when its closed
                        // nothing bad happens
                        let dup_fd = wasi_try_ok_ok!(state.fs.clone_fd(fd));
                        trace!(
                            %dup_fd
                        );

                        // some special files will return a constant FD rather than
                        // actually open the file (/dev/stdin, /dev/stdout, /dev/stderr)
                        return Ok(Ok(dup_fd));
                    }
                }
            }
            Kind::Buffer { buffer } => {
                if o_flags.contains(Oflags::DIRECTORY) || orig_path.ends_with('/') {
                    return Ok(Err(Errno::Notdir));
                }
                if minimum_rights.truncate {
                    buffer.clear();
                    processing_inode.stat.write().unwrap().st_size = 0;
                }
            }
            Kind::Root { .. } => {
                if !o_flags.contains(Oflags::DIRECTORY) {
                    return Ok(Err(Errno::Isdir));
                }
            }
            Kind::Dir { .. } => {
                if fs_rights_base.contains(Rights::FD_WRITE) {
                    return Ok(Err(Errno::Isdir));
                }
            }
            Kind::Socket { .. }
            | Kind::PipeTx { .. }
            | Kind::PipeRx { .. }
            | Kind::DuplexPipe { .. }
            | Kind::EventNotifications { .. }
            | Kind::Epoll { .. } => {}
            Kind::Symlink {
                base_po_dir,
                path_to_symlink,
                relative_path,
            } => {
                let (base_po_dir, target_path) = if relative_path.is_absolute() {
                    (
                        VIRTUAL_ROOT_FD,
                        relative_path.to_string_lossy().to_string(),
                    )
                } else {
                    let mut target_path = path_to_symlink.clone();
                    target_path.pop();
                    target_path.push(relative_path);
                    (*base_po_dir, target_path.to_string_lossy().to_string())
                };
                drop(guard);
                return path_open_internal(
                    env,
                    base_po_dir,
                    dirflags | __WASI_LOOKUP_SYMLINK_FOLLOW,
                    &target_path,
                    o_flags,
                    fs_rights_base,
                    fs_rights_inheriting,
                    fs_flags,
                    fd_flags,
                    with_fd,
                );
            }
        }
        inode
    } else {
        // less-happy path, we have to try to create the file
        if o_flags.contains(Oflags::CREATE) {
            if o_flags.contains(Oflags::DIRECTORY) {
                return Ok(Err(Errno::Notdir));
            }

            // Trailing slash matters. But the underlying opener normalizes it away later.
            if path.ends_with('/') {
                return Ok(Err(Errno::Isdir));
            }

            // strip end file name

            let (parent_inode, new_entity_name) =
                wasi_try_ok_ok!(state.fs.get_parent_inode_at_path(
                    inodes,
                    dirfd,
                    &path_arg,
                    dirflags & __WASI_LOOKUP_SYMLINK_FOLLOW != 0
                ));
            let new_file_host_path = {
                let guard = parent_inode.read();
                match guard.deref() {
                    Kind::Dir { path, .. } => {
                        let mut new_path = path.clone();
                        new_path.push(&new_entity_name);
                        new_path
                    }
                    Kind::Root { .. } => {
                        let mut new_path = std::path::PathBuf::new();
                        new_path.push(&new_entity_name);
                        new_path
                    }
                    _ => return Ok(Err(Errno::Inval)),
                }
            };
            // once we got the data we need from the parent, we lookup the host file
            // todo: extra check that opening with write access is okay
            let (kind, virtual_only) = {
                // We set create_new because the path already didn't resolve to an existing file,
                // so it must be created.
                let open_options = open_options
                    .read(minimum_rights.read)
                    .append(minimum_rights.append)
                    .write(minimum_rights.write)
                    .create_new(true);

                if minimum_rights.read {
                    open_flags |= Fd::READ;
                }
                if minimum_rights.write {
                    open_flags |= Fd::WRITE;
                }
                if minimum_rights.create_new {
                    open_flags |= Fd::CREATE;
                }
                if minimum_rights.truncate {
                    open_flags |= Fd::TRUNCATE;
                }

                match open_options.open(&new_file_host_path) {
                    Ok(handle) => {
                        let mode = 0o666 & !env.process.umask();
                        match crate::syscalls::chmod_host_path(&new_file_host_path, mode) {
                            Errno::Success | Errno::Noent => {}
                            err => return Ok(Err(err)),
                        }
                        (
                            Kind::File {
                                handle: Some(Arc::new(std::sync::RwLock::new(handle))),
                                path: new_file_host_path,
                                fd: None,
                            },
                            false,
                        )
                    }
                    Err(err) => {
                        // Even though the file does not exist, it still failed to create with
                        // `AlreadyExists` error.  This can happen if the path resolves to a
                        // symlink that points outside the FS sandbox.
                        if err == FsError::AlreadyExists {
                            return Ok(Err(Errno::Perm));
                        }

                        if err == FsError::EntryNotFound {
                            (
                                Kind::Buffer {
                                    buffer: Vec::new(),
                                },
                                true,
                            )
                        } else {
                            return Ok(Err(fs_error_into_wasi_err(err)));
                        }
                    }
                }
            };

            let new_inode = {
                wasi_try_ok_ok!(
                    state
                        .fs
                        .create_inode(inodes, kind, false, new_entity_name.clone())
                )
            };

            {
                let mut guard = parent_inode.write();
                if let Kind::Dir { entries, .. } = guard.deref_mut() {
                    entries.insert(new_entity_name, new_inode.clone());
                }
            }

            if virtual_only {
                new_inode.stat.write().unwrap().st_size = 0;
            }

            new_inode
        } else {
            return Ok(Err(maybe_inode.unwrap_err()));
        }
    };

    // TODO: check and reduce these
    // TODO: ensure a mutable fd to root can never be opened
    let out_fd = wasi_try_ok_ok!(if let Some(fd) = with_fd {
        state
            .fs
            .with_fd(
                adjusted_rights,
                fs_rights_inheriting,
                fs_flags,
                fd_flags,
                open_flags,
                inode,
                fd,
            )
            .map(|_| fd)
    } else {
        state.fs.create_fd(
            adjusted_rights,
            fs_rights_inheriting,
            fs_flags,
            fd_flags,
            open_flags,
            inode,
        )
    });
    sync_process_metadata(env);

    Ok(Ok(out_fd))
}

fn proc_generated_file(env: &WasiEnv, path: &str) -> Option<Vec<u8>> {
    match path {
        "/proc/sys/kernel/hostname" => Some(format!("{}\n", env.process.hostname()).into_bytes()),
        "/proc/sys/kernel/domainname" => Some(format!("{}\n", env.process.domainname()).into_bytes()),
        "/proc/self/comm" => Some(format!("{}\n", proc_self_name(env)).into_bytes()),
        "/proc/self/cmdline" => Some(proc_process_cmdline(&env.process)),
        "/proc/self/environ" => Some(proc_process_environ(&env.process)),
        "/proc/self/stat" => Some(proc_self_stat(env).into_bytes()),
        "/proc/self/status" => Some(proc_self_status(env).into_bytes()),
        "/proc/self/statm" => Some("0 0 0 0 0 0 0\n".as_bytes().to_vec()),
        "/proc/self/limits" => Some(proc_self_limits(env).into_bytes()),
        "/proc/self/io" => Some(proc_self_io().into_bytes()),
        "/proc/self/cgroup" => Some("0::/\n".as_bytes().to_vec()),
        "/proc/self/cpuset" => Some("/\n".as_bytes().to_vec()),
        "/proc/self/uid_map" => Some(proc_self_id_map().into_bytes()),
        "/proc/self/gid_map" => Some(proc_self_id_map().into_bytes()),
        "/proc/self/projid_map" => Some(proc_self_id_map().into_bytes()),
        "/proc/self/setgroups" => Some("allow\n".as_bytes().to_vec()),
        "/proc/self/children" => Some(proc_self_children(env).into_bytes()),
        "/proc/self/mountstats" => Some(Vec::new()),
        "/proc/self/loginuid" => Some("4294967295\n".as_bytes().to_vec()),
        "/proc/self/sessionid" => Some(proc_process_sessionid(&env.process).into_bytes()),
        "/proc/self/oom_score" => Some("0\n".as_bytes().to_vec()),
        "/proc/self/oom_score_adj" => Some("0\n".as_bytes().to_vec()),
        "/proc/self/personality" => Some(proc_process_personality(&env.process).into_bytes()),
        "/proc/self/timers" => Some(Vec::new()),
        "/proc/self/timerslack_ns" => Some(proc_process_timerslack(&env.process).into_bytes()),
        "/proc/self/schedstat" => Some("0 0 0\n".as_bytes().to_vec()),
        "/proc/self/sched" => Some(proc_self_sched(env).into_bytes()),
        "/proc/self/autogroup" => Some(proc_process_autogroup(&env.process).into_bytes()),
        "/proc/self/wchan" => Some("0\n".as_bytes().to_vec()),
        "/proc/self/stack" => Some(Vec::new()),
        "/proc/self/attr/current" => Some("unconfined\n".as_bytes().to_vec()),
        "/proc/self/attr/exec" => Some(Vec::new()),
        "/proc/self/attr/fscreate" => Some(Vec::new()),
        "/proc/self/attr/keycreate" => Some(Vec::new()),
        "/proc/self/attr/prev" => Some("unconfined\n".as_bytes().to_vec()),
        "/proc/self/maps" => Some(proc_self_maps().into_bytes()),
        "/proc/self/smaps" => Some(proc_self_smaps().into_bytes()),
        "/proc/self/numa_maps" => Some(proc_self_numa_maps().into_bytes()),
        "/proc/self/auxv" => Some(proc_self_auxv()),
        _ => proc_task_children_path(path)
            .map(|_| proc_self_children(env).into_bytes())
            .or_else(|| {
                path.strip_prefix("/proc/self/fdinfo/")
                    .and_then(|fd| fd.parse::<WasiFd>().ok())
                    .and_then(|fd| proc_self_fdinfo(env, fd))
            }),
    }
}

fn proc_generated_process_file(env: &WasiEnv, path: &str) -> Option<Vec<u8>> {
    let (process, tail) = proc_process_path(env, path)?;
    let tail = proc_process_file_tail(&process, &tail)?;
    match tail.as_str() {
        "comm" => Some(format!("{}\n", proc_process_name(env, &process)).into_bytes()),
        "cmdline" => Some(proc_process_cmdline(&process)),
        "environ" => Some(proc_process_environ(&process)),
        "stat" => Some(proc_process_stat(env, &process).into_bytes()),
        "status" => Some(proc_process_status(env, &process).into_bytes()),
        "statm" => Some("0 0 0 0 0 0 0\n".as_bytes().to_vec()),
        "limits" => Some(proc_process_limits(&process).into_bytes()),
        "io" => Some(proc_self_io().into_bytes()),
        "cgroup" => Some("0::/\n".as_bytes().to_vec()),
        "cpuset" => Some("/\n".as_bytes().to_vec()),
        "children" => Some(proc_process_children(&process).into_bytes()),
        "mountstats" => Some(Vec::new()),
        "loginuid" => Some("4294967295\n".as_bytes().to_vec()),
        "sessionid" => Some(proc_process_sessionid(&process).into_bytes()),
        "oom_score" => Some("0\n".as_bytes().to_vec()),
        "oom_score_adj" => Some("0\n".as_bytes().to_vec()),
        "personality" => Some(proc_process_personality(&process).into_bytes()),
        "timers" => Some(Vec::new()),
        "timerslack_ns" => Some(proc_process_timerslack(&process).into_bytes()),
        "schedstat" => Some("0 0 0\n".as_bytes().to_vec()),
        "sched" => Some(proc_process_sched(env, &process).into_bytes()),
        "autogroup" => Some(proc_process_autogroup(&process).into_bytes()),
        "wchan" => Some("0\n".as_bytes().to_vec()),
        "stack" => Some(Vec::new()),
        "attr/current" => Some("unconfined\n".as_bytes().to_vec()),
        "attr/exec" => Some(Vec::new()),
        "attr/fscreate" => Some(Vec::new()),
        "attr/keycreate" => Some(Vec::new()),
        "attr/prev" => Some("unconfined\n".as_bytes().to_vec()),
        _ if tail.starts_with("fdinfo/") => tail
            .strip_prefix("fdinfo/")
            .and_then(|fd| fd.parse::<WasiFd>().ok())
            .and_then(|fd| proc_process_fdinfo(&process, fd)),
        "maps" => Some(proc_self_maps().into_bytes()),
        "smaps" => Some(proc_self_smaps().into_bytes()),
        "numa_maps" => Some(proc_self_numa_maps().into_bytes()),
        "auxv" => Some(proc_self_auxv()),
        _ => None,
    }
}

fn proc_process_file_tail(process: &WasiProcess, tail: &str) -> Option<String> {
    let Some(tail) = tail.strip_prefix("task/") else {
        return Some(tail.to_string());
    };
    let (tid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    let tid = tid.parse::<u32>().ok()?;
    let process_tids = process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw())
        .collect::<Vec<_>>();
    process_tids.contains(&tid).then(|| rest.to_string())
}

fn proc_generated_file_rights() -> Rights {
    Rights::FD_ADVISE
        | Rights::FD_READ
        | Rights::FD_SEEK
        | Rights::FD_TELL
        | Rights::FD_FILESTAT_GET
        | Rights::POLL_FD_READWRITE
}

fn proc_self_name(env: &WasiEnv) -> String {
    proc_process_name(env, &env.process)
}

fn proc_process_name(_env: &WasiEnv, process: &WasiProcess) -> String {
    process
        .process_name()
        .unwrap_or_else(|| default_process_name(process))
}

fn proc_process_cmdline(process: &WasiProcess) -> Vec<u8> {
    join_nul_terminated(process.args().iter().map(String::as_bytes))
}

fn proc_process_environ(process: &WasiProcess) -> Vec<u8> {
    join_nul_terminated(process.envs().iter().map(Vec::as_slice))
}

fn proc_process_timerslack(process: &WasiProcess) -> String {
    format!("{}\n", process.timer_slack_nanos())
}

fn proc_process_personality(process: &WasiProcess) -> String {
    format!("{:08x}\n", process.personality())
}

fn join_nul_terminated<'a>(parts: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(part);
        bytes.push(0);
    }
    bytes
}

fn proc_self_stat(env: &WasiEnv) -> String {
    proc_process_stat(env, &env.process)
}

fn proc_process_stat(env: &WasiEnv, process: &WasiProcess) -> String {
    let pid = process.pid().raw();
    let ppid = process.ppid().raw();
    let pgid = process.pgid().raw();
    let sid = process.session_id().raw();
    let comm = proc_process_name(env, process);
    let threads = process.active_threads().max(1);
    let state = proc_process_state_char(process);
    let start_time_ticks = process.start_time_nanos() / 10_000_000;
    let nice = process.nice();
    let priority = 20 + nice;
    format!(
        "{pid} ({comm}) {state} {ppid} {pgid} {sid} 0 -1 4194304 0 0 0 0 0 0 0 0 {priority} {nice} {threads} 0 {start_time_ticks} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
    )
}

fn proc_self_status(env: &WasiEnv) -> String {
    proc_process_status(env, &env.process)
}

fn proc_process_status(env: &WasiEnv, process: &WasiProcess) -> String {
    let pid = process.pid().raw();
    let ppid = process.ppid().raw();
    let pgid = process.pgid().raw();
    let sid = process.session_id().raw();
    let umask = process.umask();
    let identity = process.identity();
    let groups = if identity.groups.is_empty() {
        String::new()
    } else {
        identity
            .groups
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(" ")
    };
    let name = proc_process_name(env, process);
    let fd_size = if process.pid() == env.process.pid() {
        env.state.fs.fd_map.read().unwrap().keys().count()
    } else {
        process.fd_size()
    };
    let threads = process.active_threads().max(1);
    let state = proc_process_state_status(process);
    let no_new_privs = u32::from(process.no_new_privs());
    let seccomp = process.seccomp_mode();
    let (cap_effective, cap_permitted, cap_inheritable, cap_bounding) = process.capabilities();
    format!(
        "Name:\t{name}\nUmask:\t{umask:04o}\nState:\t{state}\nTgid:\t{pid}\nNgid:\t0\nPid:\t{pid}\nPPid:\t{ppid}\nTracerPid:\t0\nUid:\t{ruid}\t{euid}\t{suid}\t{euid}\nGid:\t{rgid}\t{egid}\t{sgid}\t{egid}\nFDSize:\t{fd_size}\nGroups:\t{groups}\nNStgid:\t{pid}\nNSpid:\t{pid}\nNSpgid:\t{pgid}\nNSsid:\t{sid}\nThreads:\t{threads}\nSigQ:\t0/0\nSigPnd:\t0000000000000000\nShdPnd:\t0000000000000000\nSigBlk:\t0000000000000000\nSigIgn:\t0000000000000000\nSigCgt:\t0000000000000000\nCapInh:\t{cap_inheritable:016x}\nCapPrm:\t{cap_permitted:016x}\nCapEff:\t{cap_effective:016x}\nCapBnd:\t{cap_bounding:016x}\nCapAmb:\t0000000000000000\nNoNewPrivs:\t{no_new_privs}\nSeccomp:\t{seccomp}\n",
        ruid = identity.real_uid,
        euid = identity.effective_uid,
        suid = identity.saved_uid,
        rgid = identity.real_gid,
        egid = identity.effective_gid,
        sgid = identity.saved_gid,
    )
}

fn proc_process_state_char(process: &WasiProcess) -> &'static str {
    match process.task_status() {
        TaskStatus::Pending => "S",
        TaskStatus::Running => "R",
        TaskStatus::Stopped(_) => "T",
        TaskStatus::Finished(_) | TaskStatus::Signaled { .. } => "Z",
    }
}

fn proc_process_state_status(process: &WasiProcess) -> &'static str {
    match process.task_status() {
        TaskStatus::Pending => "S (sleeping)",
        TaskStatus::Running => "R (running)",
        TaskStatus::Stopped(_) => "T (stopped)",
        TaskStatus::Finished(_) | TaskStatus::Signaled { .. } => "Z (zombie)",
    }
}

fn proc_self_io() -> String {
    "rchar: 0\nwchar: 0\nsyscr: 0\nsyscw: 0\nread_bytes: 0\nwrite_bytes: 0\ncancelled_write_bytes: 0\n"
        .to_string()
}

fn proc_self_children(env: &WasiEnv) -> String {
    proc_process_children(&env.process)
}

fn proc_process_sessionid(process: &WasiProcess) -> String {
    format!("{}\n", process.session_id().raw())
}

fn proc_process_children(process: &WasiProcess) -> String {
    let children = process.lock().children.clone();
    if children.is_empty() {
        return String::new();
    }

    let mut output = children
        .iter()
        .map(|child| child.pid.raw().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    output.push('\n');
    output
}

fn proc_task_children_path(path: &str) -> Option<()> {
    let tail = path.strip_prefix("/proc/self/task/")?;
    let (tid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    (rest == "children" && tid.parse::<u64>().is_ok()).then_some(())
}

fn proc_self_id_map() -> String {
    "         0          0 4294967295\n".to_string()
}

fn proc_self_sched(env: &WasiEnv) -> String {
    proc_process_sched(env, &env.process)
}

fn proc_process_sched(env: &WasiEnv, process: &WasiProcess) -> String {
    let pid = process.pid().raw();
    let name = proc_process_name(env, process);
    let threads = process.active_threads().max(1);
    let exec_start = process.start_time_nanos() as f64 / 1_000_000.0;
    let priority = 120 + process.nice();
    let (policy, _) = process.scheduler();
    format!(
        "{name} ({pid}, #threads: {threads})\n-------------------------------------------------------------------\nse.exec_start                                : {exec_start:20.6}\nse.vruntime                                  :             0.000000\nse.sum_exec_runtime                          :             0.000000\nnr_switches                                  :                    0\nnr_voluntary_switches                        :                    0\nnr_involuntary_switches                      :                    0\nse.load.weight                               :              1048576\npolicy                                       :                  {policy:3}\nprio                                         :                  {priority:3}\n"
    )
}

fn proc_process_autogroup(process: &WasiProcess) -> String {
    format!("/autogroup-0 nice {}\n", process.nice())
}

fn proc_self_maps() -> String {
    "00010000-00100000 r-xp 00000000 00:00 0 /proc/self/exe\n00100000-00200000 rw-p 00000000 00:00 0 [heap]\n7fff0000-80000000 rw-p 00000000 00:00 0 [stack]\n"
        .to_string()
}

fn proc_self_smaps() -> String {
    proc_self_maps()
        .lines()
        .map(|line| {
            format!(
                "{line}\nSize:                0 kB\nRss:                 0 kB\nPss:                 0 kB\nShared_Clean:        0 kB\nShared_Dirty:        0 kB\nPrivate_Clean:       0 kB\nPrivate_Dirty:       0 kB\nReferenced:          0 kB\nAnonymous:           0 kB\nVmFlags: rd wr mr mw me ac\n"
            )
        })
        .collect()
}

fn proc_self_numa_maps() -> String {
    "00010000 default file=/proc/self/exe\n00100000 default heap\n7fff0000 default stack\n"
        .to_string()
}

fn proc_self_auxv() -> Vec<u8> {
    let word_size = std::mem::size_of::<usize>();
    let mut bytes = Vec::with_capacity(word_size * 2);
    bytes.resize(word_size * 2, 0);
    bytes
}

fn proc_self_limits(env: &WasiEnv) -> String {
    proc_process_limits(&env.process)
}

fn proc_process_limits(process: &WasiProcess) -> String {
    const RLIMIT_CPU: i32 = 0;
    const RLIMIT_FSIZE: i32 = 1;
    const RLIMIT_DATA: i32 = 2;
    const RLIMIT_STACK: i32 = 3;
    const RLIMIT_NPROC: i32 = 6;
    const RLIMIT_NOFILE: i32 = 7;
    const RLIMIT_AS: i32 = 9;
    format!(
        "Limit                     Soft Limit           Hard Limit           Units\n\
Max cpu time              {cpu_cur:<20} {cpu_max:<20} seconds\n\
Max file size             {fsize_cur:<20} {fsize_max:<20} bytes\n\
Max data size             {data_cur:<20} {data_max:<20} bytes\n\
Max stack size            {stack_cur:<20} {stack_max:<20} bytes\n\
Max open files            {nofile_cur:<20} {nofile_max:<20} files\n\
Max processes             {nproc_cur:<20} {nproc_max:<20} processes\n\
Max address space         {as_cur:<20} {as_max:<20} bytes\n",
        cpu_cur = proc_limit_value(process.resource_limit(RLIMIT_CPU).current),
        cpu_max = proc_limit_value(process.resource_limit(RLIMIT_CPU).maximum),
        fsize_cur = proc_limit_value(process.resource_limit(RLIMIT_FSIZE).current),
        fsize_max = proc_limit_value(process.resource_limit(RLIMIT_FSIZE).maximum),
        data_cur = proc_limit_value(process.resource_limit(RLIMIT_DATA).current),
        data_max = proc_limit_value(process.resource_limit(RLIMIT_DATA).maximum),
        stack_cur = proc_limit_value(process.resource_limit(RLIMIT_STACK).current),
        stack_max = proc_limit_value(process.resource_limit(RLIMIT_STACK).maximum),
        nofile_cur = proc_limit_value(process.resource_limit(RLIMIT_NOFILE).current),
        nofile_max = proc_limit_value(process.resource_limit(RLIMIT_NOFILE).maximum),
        nproc_cur = proc_limit_value(process.resource_limit(RLIMIT_NPROC).current),
        nproc_max = proc_limit_value(process.resource_limit(RLIMIT_NPROC).maximum),
        as_cur = proc_limit_value(process.resource_limit(RLIMIT_AS).current),
        as_max = proc_limit_value(process.resource_limit(RLIMIT_AS).maximum),
    )
}

fn proc_limit_value(value: u64) -> String {
    if value == u64::MAX {
        "unlimited".to_string()
    } else {
        value.to_string()
    }
}

fn proc_self_fdinfo(env: &WasiEnv, fd: WasiFd) -> Option<Vec<u8>> {
    let info = env.process.fd_info(fd)?;
    Some(format!("pos:\t{}\nflags:\t{:o}\nmnt_id:\t1\n", info.pos, info.flags).into_bytes())
}

fn proc_process_fdinfo(process: &WasiProcess, fd: WasiFd) -> Option<Vec<u8>> {
    let info = process.fd_info(fd)?;
    Some(format!("pos:\t{}\nflags:\t{:o}\nmnt_id:\t1\n", info.pos, info.flags).into_bytes())
}

fn special_device_fd(path: &str, rights: Rights) -> Option<WasiFd> {
    match path {
        "/dev/stdin" => Some(__WASI_STDIN_FILENO),
        "/dev/stdout" => Some(__WASI_STDOUT_FILENO),
        "/dev/stderr" => Some(__WASI_STDERR_FILENO),
        "/dev/tty" | "/dev/console" | "/dev/ptmx" => {
            if rights.contains(Rights::FD_READ) && !rights.contains(Rights::FD_WRITE) {
                Some(__WASI_STDIN_FILENO)
            } else {
                Some(__WASI_STDOUT_FILENO)
            }
        }
        _ => path
            .strip_prefix("/dev/fd/")
            .or_else(|| path.strip_prefix("/proc/self/fd/"))
            .and_then(|fd| fd.parse::<WasiFd>().ok()),
    }
}

fn proc_pid_alias_target(
    env: &WasiEnv,
    state: &WasiState,
    inodes: &crate::WasiInodes,
    path: &str,
) -> Option<String> {
    let (process, tail) = proc_process_path(env, path)?;
    if process.pid() == env.pid() {
        Some(join_virtual_path("/proc/self", &tail))
    } else {
        let tail = proc_process_file_tail(&process, &tail)?;
        if tail == "cwd" {
            Some(process.cwd())
        } else if let Some(tail) = tail.strip_prefix("cwd/") {
            Some(join_virtual_path(&process.cwd(), tail))
        } else {
            match tail.as_str() {
                "" => Some("/proc/self".to_string()),
                "task" => Some("/proc/self/task".to_string()),
                "root" => Some("/proc/self/root".to_string()),
                "exe" => proc_process_exe_target(state, inodes, &process),
                _ => None,
            }
        }
    }
}

fn proc_process_path(env: &WasiEnv, path: &str) -> Option<(WasiProcess, String)> {
    let tail = path.strip_prefix("/proc/")?;
    let (pid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    if pid == "self" || pid == "thread-self" {
        return None;
    }
    let pid = pid.parse::<u32>().ok()?;
    let process = env.control_plane.get_process(pid.into())?;
    Some((process, rest.to_string()))
}

fn proc_path_alias_target(
    state: &WasiState,
    inodes: &crate::WasiInodes,
    path: &str,
    process_tids: &[u32],
) -> Option<String> {
    proc_thread_self_alias_target(path)
        .or_else(|| proc_task_alias_target(process_tids, path))
        .or_else(|| {
            proc_path_alias_tail(path, "/proc/self/cwd").map(|tail| {
                let cwd = state.fs.current_dir.lock().unwrap().clone();
                join_virtual_path(&cwd, tail)
            })
        })
        .or_else(|| {
            proc_path_alias_tail(path, "/proc/self/root").map(|tail| join_virtual_path("/", tail))
        })
        .or_else(|| {
            (path == "/proc/self/exe")
                .then(|| proc_self_exe_target(state, inodes))
                .flatten()
        })
}

fn proc_thread_self_alias_target(path: &str) -> Option<String> {
    proc_path_alias_tail(path, "/proc/thread-self").map(|tail| join_virtual_path("/proc/self", tail))
}

fn proc_task_alias_target(process_tids: &[u32], path: &str) -> Option<String> {
    let tail = path.strip_prefix("/proc/self/task/")?;
    let (tid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    let tid = tid.parse::<u32>().ok()?;
    if process_tids.contains(&tid) {
        Some(join_virtual_path("/proc/self", rest))
    } else {
        None
    }
}

fn proc_path_alias_tail<'a>(path: &'a str, alias: &str) -> Option<&'a str> {
    if path == alias {
        Some("")
    } else {
        path.strip_prefix(alias)
            .and_then(|tail| tail.strip_prefix('/'))
    }
}

fn join_virtual_path(base: &str, tail: &str) -> String {
    if tail.is_empty() {
        return base.to_string();
    }
    if base == "/" {
        format!("/{tail}")
    } else {
        format!("{}/{}", base.trim_end_matches('/'), tail)
    }
}

fn proc_self_exe_target(state: &WasiState, inodes: &crate::WasiInodes) -> Option<String> {
    let argv0 = state.args.lock().unwrap().first()?.clone();
    if argv0.contains('/') {
        let target = if argv0.starts_with('/') {
            argv0
        } else {
            state.fs.relative_path_to_absolute(argv0)
        };
        if state
            .fs
            .get_inode_at_path(inodes, crate::VIRTUAL_ROOT_FD, &target, true)
            .is_ok()
        {
            return Some(target);
        }
        return None;
    }
    let path_env = state
        .envs
        .lock()
        .unwrap()
        .iter()
        .find_map(|entry| {
            entry
                .strip_prefix(b"PATH=")
                .and_then(|value| std::str::from_utf8(value).ok())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "/usr/local/bin:/bin:/usr/bin".to_string());
    path_env.split(':').find_map(|entry| {
        let base = if entry.is_empty() {
            state.fs.current_dir.lock().unwrap().clone()
        } else if entry.starts_with('/') {
            entry.to_string()
        } else {
            state.fs.relative_path_to_absolute(entry.to_string())
        };
        let candidate = join_virtual_path(&base, &argv0);
        state
            .fs
            .get_inode_at_path(inodes, crate::VIRTUAL_ROOT_FD, &candidate, true)
            .is_ok()
            .then_some(candidate)
    })
}

fn proc_process_exe_target(
    state: &WasiState,
    inodes: &crate::WasiInodes,
    process: &WasiProcess,
) -> Option<String> {
    if let Some(target) = process.executable_path()
        && state
            .fs
            .get_inode_at_path(inodes, crate::VIRTUAL_ROOT_FD, &target, true)
            .is_ok()
    {
        return Some(target);
    }
    let argv0 = process.args().first()?.clone();
    if argv0.contains('/') {
        let target = if argv0.starts_with('/') {
            argv0
        } else {
            join_virtual_path(&process.cwd(), &argv0)
        };
        if state
            .fs
            .get_inode_at_path(inodes, crate::VIRTUAL_ROOT_FD, &target, true)
            .is_ok()
        {
            return Some(target);
        }
        return None;
    }
    let envs = process.envs();
    let path_env = envs
        .iter()
        .find_map(|entry| {
            entry
                .strip_prefix(b"PATH=")
                .and_then(|value| std::str::from_utf8(value).ok())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "/usr/local/bin:/bin:/usr/bin".to_string());
    path_env.split(':').find_map(|entry| {
        let base = if entry.is_empty() {
            process.cwd()
        } else if entry.starts_with('/') {
            entry.to_string()
        } else {
            join_virtual_path(&process.cwd(), entry)
        };
        let candidate = join_virtual_path(&base, &argv0);
        state
            .fs
            .get_inode_at_path(inodes, crate::VIRTUAL_ROOT_FD, &candidate, true)
            .is_ok()
            .then_some(candidate)
    })
}
