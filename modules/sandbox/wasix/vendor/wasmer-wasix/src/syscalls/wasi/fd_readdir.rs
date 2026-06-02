use super::*;
use crate::fs::Inode;
use crate::syscalls::*;
use std::collections::HashSet;

/// ### `fd_readdir()`
/// Read data from directory specified by file descriptor
/// Inputs:
/// - `Fd fd`
///     File descriptor from which directory data will be read
/// - `void *buf`
///     Buffer where directory entries are stored
/// - `u32 buf_len`
///     Length of data in `buf`
/// - `Dircookie cookie`
///     Where the directory reading should start from
/// Output:
/// - `u32 *bufused`
///     The Number of bytes stored in `buf`; if less than `buf_len` then entire
///     directory has been read
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn fd_readdir<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    buf: WasmPtr<u8, M>,
    buf_len: M::Offset,
    cookie: Dircookie,
    bufused: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (memory, mut state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
    // TODO: figure out how this is supposed to work;
    // is it supposed to pack the buffer full every time until it can't? or do one at a time?

    let buf_arr = wasi_try_mem_ok!(buf.slice(&memory, buf_len));
    let bufused_ref = bufused.deref(&memory);
    let working_dir = wasi_try_ok!(state.fs.get_fd(fd));
    if !working_dir.inner.rights.contains(Rights::FD_READDIR) {
        return Ok(Errno::Access);
    }
    let current_tid = env.tid().raw().to_string();
    let process_tids = env
        .process
        .all_threads()
        .into_iter()
        .map(|tid| tid.raw().to_string())
        .collect::<Vec<_>>();
    let current_pid = env.pid().raw().to_string();
    let processes = env.control_plane.processes();
    let process_pids = processes
        .iter()
        .map(|process| process.pid().raw().to_string())
        .collect::<Vec<_>>();
    let mut cur_cookie = cookie;
    let mut buf_idx = 0usize;

    let entries: Vec<(String, Filetype, u64)> = {
        let guard = working_dir.inode.read();
        match guard.deref() {
            Kind::Dir {
                path,
                entries,
                parent,
                ..
            } => {
                trace!("reading dir {:?}", path);
                let current_ino = working_dir.inode.stat.read().unwrap().st_ino;
                let parent_ino = path
                    .parent()
                    .and_then(|parent_path| {
                        let parent_path = if parent_path.as_os_str().is_empty() {
                            std::path::Path::new("/")
                        } else {
                            parent_path
                        };
                        state
                            .fs
                            .root_fs
                            .metadata(parent_path)
                            .ok()
                            .map(|metadata| metadata.ino())
                    })
                    .or_else(|| {
                        parent
                            .upgrade()
                            .map(|parent| parent.stat.read().unwrap().st_ino)
                    })
                    .unwrap_or(current_ino);
                // TODO: refactor this code
                // we need to support multiple calls,
                // simple and obviously correct implementation for now:
                // maintain consistent order via lexacographic sorting
                let fs_info = match state.fs_read_dir(path) {
                    Ok(read_dir) => wasi_try_ok!(
                        read_dir
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(fs_error_into_wasi_err)
                    ),
                    Err(Errno::Noent) => Vec::new(),
                    Err(err) => return Ok(err),
                };
	                let mut entry_vec = wasi_try_ok!(
	                    fs_info
	                        .into_iter()
	                        .map(|entry| {
	                            let filename = entry.file_name().to_string_lossy().to_string();
	                            trace!("getting file: {:?}", filename);
	                            let metadata = entry.metadata().map_err(fs_error_into_wasi_err)?;
	                            let filetype =
	                                virtual_file_type_to_wasi_file_type(metadata.file_type());
	                            let ino = metadata.ino();
	                            Ok((filename, filetype, ino))
	                        })
	                        .collect::<Result<Vec<(String, Filetype, u64)>, _>>()
                );
                let mut seen = entry_vec
                    .iter()
                    .map(|(name, _, _)| name.clone())
                    .collect::<HashSet<_>>();
                if is_proc_self_fd_dir(path) || is_proc_self_fdinfo_dir(path) {
                    let fds = state.fs.fd_map.read().unwrap().keys().collect::<Vec<_>>();
                    entry_vec.extend(fds.into_iter().filter_map(|fd| {
                        let name = fd.to_string();
                        if !seen.insert(name.clone()) {
                            return None;
                        }
                        let stat = state.fs.filestat_fd(fd).ok()?;
                        Some((name, stat.st_filetype, stat.st_ino))
                    }));
                }
                if is_proc_self_task_dir(path) {
                    for tid in &process_tids {
                        if seen.insert(tid.clone()) {
                            let ino = Inode::from_path(&format!("/proc/self/task/{tid}")).as_u64();
                            entry_vec.push((tid.clone(), Filetype::Directory, ino));
                        }
                    }
                    if seen.insert(current_tid.clone()) {
                        let ino =
                            Inode::from_path(&format!("/proc/self/task/{current_tid}")).as_u64();
                        entry_vec.push((current_tid.clone(), Filetype::Directory, ino));
                    }
                }
                if let Some((pid, tids)) = proc_pid_task_dir(path, &processes) {
                    for tid in &tids {
                        if seen.insert(tid.clone()) {
                            let ino = Inode::from_path(&format!("/proc/{pid}/task/{tid}")).as_u64();
                            entry_vec.push((tid.clone(), Filetype::Directory, ino));
                        }
                    }
                }
                if let Some((pid, dir_name, fds)) = proc_pid_fd_dir(path, &processes) {
                    for fd in fds {
                        let name = fd.to_string();
                        if seen.insert(name.clone()) {
                            let ino =
                                Inode::from_path(&format!("/proc/{pid}/{dir_name}/{fd}")).as_u64();
                            let filetype = if dir_name == "fd" {
                                Filetype::SymbolicLink
                            } else {
                                Filetype::RegularFile
                            };
                            entry_vec.push((name, filetype, ino));
                        }
                    }
                }
                if is_proc_root_dir(path) {
                    for pid in &process_pids {
                        if seen.insert(pid.clone()) {
                            let ino = Inode::from_path(&format!("/proc/{pid}")).as_u64();
                            entry_vec.push((pid.clone(), Filetype::Directory, ino));
                        }
                    }
                    if seen.insert(current_pid.clone()) {
                        let ino = Inode::from_path(&format!("/proc/{current_pid}")).as_u64();
                        entry_vec.push((current_pid.clone(), Filetype::Directory, ino));
                    }
                }
                entry_vec.extend(
                    entries
                        .iter()
                        .filter(|(name, _)| seen.insert((*name).clone()))
                        .filter_map(|(name, _inode)| {
                            let entry_path = path.join(name);
                            if let Ok(metadata) = state.fs.root_fs.metadata(&entry_path) {
                                let filetype =
                                    virtual_file_type_to_wasi_file_type(metadata.file_type());
                                return Some((name.clone(), filetype, metadata.ino()));
                            }
                            None
                        }),
                );
                // adding . and .. special folders
                entry_vec.push((".".to_string(), Filetype::Directory, current_ino));
                entry_vec.push(("..".to_string(), Filetype::Directory, parent_ino));
                entry_vec.sort_by(|a, b| a.0.cmp(&b.0));
                entry_vec
            }
            Kind::Root { entries } => {
                trace!("reading root");
                let current_ino = working_dir.inode.stat.read().unwrap().st_ino;
                let fs_info = match state.fs_read_dir(Path::new("/")) {
                    Ok(read_dir) => wasi_try_ok!(
                        read_dir
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(fs_error_into_wasi_err)
                    ),
                    Err(Errno::Noent) => Vec::new(),
                    Err(err) => return Ok(err),
                };
                let mut entry_vec = wasi_try_ok!(
                    fs_info
                        .into_iter()
                        .filter_map(|entry| {
                            let filename = entry.file_name().to_string_lossy().to_string();
                            if filename == "/" {
                                return None;
                            }
                            let metadata = match entry.metadata().map_err(fs_error_into_wasi_err) {
                                Ok(metadata) => metadata,
                                Err(error) => return Some(Err(error)),
                            };
                            let filetype =
                                virtual_file_type_to_wasi_file_type(metadata.file_type());
                            let ino = metadata.ino();
                            Some(Ok((filename, filetype, ino)))
                        })
                        .collect::<Result<Vec<(String, Filetype, u64)>, _>>()
                );
                let mut seen = entry_vec
                    .iter()
                    .map(|(name, _, _)| name.clone())
                    .collect::<HashSet<_>>();
                let sorted_entries = {
                    let mut entry_vec: Vec<(String, InodeGuard)> = entries
                        .iter()
                        .map(|(a, b)| (a.clone(), b.clone()))
                        .collect();
                    entry_vec.sort_by(|a, b| a.0.cmp(&b.0));
                    entry_vec
                };
                entry_vec.extend(sorted_entries.into_iter().filter_map(|(name, _inode)| {
                    if name == "/" {
                        return None;
                    }
                    if !seen.insert(name.clone()) {
                        return None;
                    }
                    let entry_path = Path::new("/").join(&name);
                    if let Ok(metadata) = state.fs.root_fs.metadata(&entry_path) {
                        let filetype =
                            virtual_file_type_to_wasi_file_type(metadata.file_type());
                        return Some((name, filetype, metadata.ino()));
                    }
                    None
                }));
                entry_vec.push((".".to_string(), Filetype::Directory, current_ino));
                entry_vec.push(("..".to_string(), Filetype::Directory, current_ino));
                entry_vec.sort_by(|a, b| a.0.cmp(&b.0));
                entry_vec
            }
            Kind::File { .. }
            | Kind::Symlink { .. }
            | Kind::Buffer { .. }
            | Kind::Socket { .. }
            | Kind::PipeRx { .. }
            | Kind::PipeTx { .. }
            | Kind::DuplexPipe { .. }
            | Kind::EventNotifications { .. }
            | Kind::Epoll { .. } => return Ok(Errno::Notdir),
        }
    };

    let start_entry: usize = wasi_try_ok!(cookie.try_into().map_err(|_| Errno::Overflow));
    for (entry_path_str, wasi_file_type, ino) in entries.iter().skip(start_entry) {
        cur_cookie += 1;
        let namlen = entry_path_str.len();
        let d_namlen = wasi_try_ok!(namlen.try_into().map_err(|_| Errno::Overflow));
        trace!("returning dirent for {}", entry_path_str);
        let dirent = Dirent {
            d_next: cur_cookie,
            d_ino: *ino,
            d_namlen,
            d_type: *wasi_file_type,
        };
        let dirent_bytes = dirent_to_le_bytes(&dirent);
        let buf_len: u64 = buf_len.into();
        let upper_limit = std::cmp::min(
            (buf_len - buf_idx as u64) as usize,
            std::mem::size_of::<Dirent>(),
        );
        for (i, b) in dirent_bytes.iter().enumerate().take(upper_limit) {
            wasi_try_mem_ok!(buf_arr.index((i + buf_idx) as u64).write(*b));
        }
        buf_idx += upper_limit;
        if upper_limit != std::mem::size_of::<Dirent>() {
            break;
        }
        let upper_limit = std::cmp::min((buf_len - buf_idx as u64) as usize, namlen);
        for (i, b) in entry_path_str.bytes().take(upper_limit).enumerate() {
            wasi_try_mem_ok!(buf_arr.index((i + buf_idx) as u64).write(b));
        }
        buf_idx += upper_limit;
        if upper_limit != namlen {
            break;
        }
    }

    let buf_idx: M::Offset = wasi_try_ok!(buf_idx.try_into().map_err(|_| Errno::Overflow));
    wasi_try_mem_ok!(bufused_ref.write(buf_idx));
    Ok(Errno::Success)
}

fn is_proc_self_fd_dir(path: &std::path::Path) -> bool {
    is_proc_self_named_dir(path, "fd")
}

fn is_proc_self_fdinfo_dir(path: &std::path::Path) -> bool {
    is_proc_self_named_dir(path, "fdinfo")
}

fn is_proc_self_task_dir(path: &std::path::Path) -> bool {
    is_proc_self_named_dir(path, "task")
}

fn proc_pid_task_dir(
    path: &std::path::Path,
    processes: &[crate::WasiProcess],
) -> Option<(String, Vec<String>)> {
    let mut components = path.components().rev().filter_map(|component| {
        component
            .as_os_str()
            .to_str()
            .filter(|value| !value.is_empty())
    });
    let (Some("task"), Some(pid), Some("proc")) =
        (components.next(), components.next(), components.next())
    else {
        return None;
    };
    let pid_raw = pid.parse::<u32>().ok()?;
    let process = processes
        .iter()
        .find(|process| process.pid().raw() == pid_raw)?;
    let tids = process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw().to_string())
        .collect::<Vec<_>>();
    Some((pid.to_string(), tids))
}

fn proc_pid_fd_dir(
    path: &std::path::Path,
    processes: &[crate::WasiProcess],
) -> Option<(String, String, Vec<wasmer_wasix_types::wasi::Fd>)> {
    let components = path
        .components()
        .filter_map(|component| {
            component
                .as_os_str()
                .to_str()
                .filter(|value| !value.is_empty())
        })
        .collect::<Vec<_>>();
    let (pid, dir_name) = match components.as_slice() {
        ["proc", pid, dir_name] if *dir_name == "fd" || *dir_name == "fdinfo" => (*pid, *dir_name),
        ["proc", pid, "task", tid, dir_name] if *dir_name == "fd" || *dir_name == "fdinfo" => {
            let tid = tid.parse::<u32>().ok()?;
            let process = find_process_by_pid(processes, pid)?;
            if !process
                .all_threads()
                .iter()
                .any(|thread| thread.raw() == tid)
            {
                return None;
            }
            (*pid, *dir_name)
        }
        _ => return None,
    };
    let process = find_process_by_pid(processes, pid)?;
    Some((pid.to_string(), dir_name.to_string(), process.fd_numbers()))
}

fn find_process_by_pid<'a>(
    processes: &'a [crate::WasiProcess],
    pid: &str,
) -> Option<&'a crate::WasiProcess> {
    let pid_raw = pid.parse::<u32>().ok()?;
    processes
        .iter()
        .find(|process| process.pid().raw() == pid_raw)
}

fn is_proc_root_dir(path: &std::path::Path) -> bool {
    path.components()
        .filter_map(|component| component.as_os_str().to_str())
        .filter(|component| !component.is_empty())
        .eq(["proc"])
}

fn is_proc_self_named_dir(path: &std::path::Path, name: &str) -> bool {
    let mut components = path.components().rev().filter_map(|component| {
        component
            .as_os_str()
            .to_str()
            .filter(|value| !value.is_empty())
    });
    matches!(
        (components.next(), components.next()),
        (Some(component), Some("self")) if component == name
    )
}
