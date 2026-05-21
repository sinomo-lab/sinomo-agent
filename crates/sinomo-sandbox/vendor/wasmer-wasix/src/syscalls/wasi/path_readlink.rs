use super::*;
use crate::WasiProcess;
use crate::syscalls::*;

/// ### `path_readlink()`
/// Read the value of a symlink
/// Inputs:
/// - `Fd dir_fd`
///     The base directory from which `path` is understood
/// - `const char *path`
///     Pointer to UTF-8 bytes that make up the path to the symlink
/// - `u32 path_len`
///     The number of bytes to read from `path`
/// - `u32 buf_len`
///     Space available pointed to by `buf`
/// Outputs:
/// - `char *buf`
///     Pointer to characters containing the path that the symlink points to
/// - `u32 buf_used`
///     The number of bytes written to `buf`
#[instrument(level = "trace", skip_all, fields(%dir_fd, path = field::Empty), ret)]
pub fn path_readlink<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    dir_fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    buf: WasmPtr<u8, M>,
    buf_len: M::Offset,
    buf_used: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    let base_dir = wasi_try_ok!(state.fs.get_fd(dir_fd));
    if !base_dir.inner.rights.contains(Rights::PATH_READLINK) {
        return Ok(Errno::Access);
    }
    let path_str = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path_str.as_str());

    let processes = env.control_plane.processes();
    let process_tids = env
        .process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw())
        .collect::<Vec<_>>();
    if let Some(link_target) = fd_alias_readlink_target(
        state,
        Some(&env.process),
        Some(env.pid().raw()),
        &process_tids,
        &processes,
        &path_str,
    ) {
        return write_readlink_target(&memory, buf, buf_len, buf_used, &link_target);
    }

    let inode = wasi_try_ok!(state.fs.get_inode_at_path(inodes, dir_fd, &path_str, false));

    {
        let guard = inode.read();
        if let Kind::Symlink { relative_path, .. } = guard.deref() {
            let rel_path_str = relative_path.to_string_lossy();
            return write_readlink_target(&memory, buf, buf_len, buf_used, &rel_path_str);
        } else {
            return Ok(Errno::Inval);
        }
    }
}

fn fd_alias_readlink_target(
    state: &WasiState,
    current_process: Option<&WasiProcess>,
    current_pid: Option<u32>,
    process_tids: &[u32],
    processes: &[WasiProcess],
    path: &str,
) -> Option<String> {
    if path == "/proc/thread-self" {
        return Some("/proc/self/task/self".to_string());
    }

    if let Some(target) = proc_process_readlink_target(current_pid, processes, path) {
        return Some(target);
    }

    let path = proc_thread_self_alias_target(path)
        .or_else(|| proc_task_alias_target(process_tids, path))
        .or_else(|| proc_pid_alias_target(current_pid, processes, path))
        .unwrap_or_else(|| path.to_string());

    let fd = match path.as_str() {
        "/proc/self/cwd" => return Some(state.fs.current_dir.lock().unwrap().clone()),
        "/proc/self/root" => return Some("/".to_string()),
        "/proc/self/exe" => return proc_self_exe_readlink_target(state),
        "/proc/self/ns/cgroup" => return namespace_target_for(current_process?, "cgroup"),
        "/proc/self/ns/ipc" => return namespace_target_for(current_process?, "ipc"),
        "/proc/self/ns/mnt" => return namespace_target_for(current_process?, "mnt"),
        "/proc/self/ns/net" => return namespace_target_for(current_process?, "net"),
        "/proc/self/ns/pid" => return namespace_target_for(current_process?, "pid"),
        "/proc/self/ns/pid_for_children" => {
            return namespace_target_for(current_process?, "pid_for_children");
        }
        "/proc/self/ns/time" => return namespace_target_for(current_process?, "time"),
        "/proc/self/ns/uts" => return namespace_target_for(current_process?, "uts"),
        "/dev/stdin" => return Some("/dev/fd/0".to_string()),
        "/dev/stdout" => return Some("/dev/fd/1".to_string()),
        "/dev/stderr" => return Some("/dev/fd/2".to_string()),
        "/dev/tty" => return Some("/dev/stdout".to_string()),
        "/dev/console" => return Some("/dev/tty".to_string()),
        "/dev/ptmx" => return Some("/dev/pts/ptmx".to_string()),
        _ => path
            .strip_prefix("/dev/fd/")
            .or_else(|| path.strip_prefix("/proc/self/fd/"))
            .and_then(|fd| fd.parse::<WasiFd>().ok())?,
    };
    let fd_entry = state.fs.get_fd(fd).ok()?;
    let target_kind = match fd_entry.inode.read().deref() {
        Kind::File { .. } | Kind::Buffer { .. } => "file",
        Kind::Dir { .. } | Kind::Root { .. } => "dir",
        Kind::Symlink { .. } => "symlink",
        Kind::Socket { .. } => "socket",
        Kind::PipeTx { .. } | Kind::PipeRx { .. } | Kind::DuplexPipe { .. } => "pipe",
        Kind::Epoll { .. } => "eventpoll",
        Kind::EventNotifications { .. } => "eventfd",
    };
    Some(format!("{target_kind}:[{fd}]"))
}

fn proc_thread_self_alias_target(path: &str) -> Option<String> {
    proc_path_alias_tail(path, "/proc/thread-self")
        .map(|tail| join_virtual_path("/proc/self", tail))
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

fn proc_pid_alias_target(
    current_pid: Option<u32>,
    processes: &[WasiProcess],
    path: &str,
) -> Option<String> {
    let tail = path.strip_prefix("/proc/")?;
    let (pid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    let pid = pid.parse::<u32>().ok()?;
    if !processes.iter().any(|process| process.pid().raw() == pid) {
        return None;
    }
    if Some(pid) == current_pid {
        Some(join_virtual_path("/proc/self", rest))
    } else {
        match rest {
            "cwd" => Some("/proc/self/cwd".to_string()),
            "root" => Some("/proc/self/root".to_string()),
            "exe" => Some("/proc/self/exe".to_string()),
            _ => None,
        }
    }
}

fn proc_process_readlink_target(
    current_pid: Option<u32>,
    processes: &[WasiProcess],
    path: &str,
) -> Option<String> {
    let tail = path.strip_prefix("/proc/")?;
    let (pid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    let pid = pid.parse::<u32>().ok()?;
    if Some(pid) == current_pid {
        return None;
    }
    let process = processes
        .iter()
        .find(|process| process.pid().raw() == pid)?;
    let rest = proc_process_link_tail(process, rest)?;
    match rest {
        "cwd" => Some(process.cwd()),
        "root" => Some("/".to_string()),
        "exe" => proc_process_exe_readlink_target(process),
        _ if rest.starts_with("fd/") => {
            let fd = rest.strip_prefix("fd/")?.parse::<WasiFd>().ok()?;
            process.fd_info(fd).map(|info| info.target)
        }
        _ => proc_namespace_readlink_target(process, rest),
    }
}

fn proc_namespace_readlink_target(process: &WasiProcess, path: &str) -> Option<String> {
    match path {
        "ns/cgroup" => namespace_target_for(process, "cgroup"),
        "ns/ipc" => namespace_target_for(process, "ipc"),
        "ns/mnt" => namespace_target_for(process, "mnt"),
        "ns/net" => namespace_target_for(process, "net"),
        "ns/pid" => namespace_target_for(process, "pid"),
        "ns/pid_for_children" => namespace_target_for(process, "pid_for_children"),
        "ns/time" => namespace_target_for(process, "time"),
        "ns/uts" => namespace_target_for(process, "uts"),
        _ => None,
    }
}

fn namespace_target_for(process: &WasiProcess, namespace: &str) -> Option<String> {
    let target_name = if namespace == "pid_for_children" {
        "pid"
    } else {
        namespace
    };
    Some(format!(
        "{}:[{}]",
        target_name,
        process.namespace_id(namespace)
    ))
}

fn proc_process_link_tail<'a>(process: &WasiProcess, rest: &'a str) -> Option<&'a str> {
    let Some(tail) = rest.strip_prefix("task/") else {
        return Some(rest);
    };
    let (tid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    let tid = tid.parse::<u32>().ok()?;
    let process_tids = process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw())
        .collect::<Vec<_>>();
    process_tids.contains(&tid).then_some(rest)
}

fn proc_path_alias_tail<'a>(path: &'a str, alias: &str) -> Option<&'a str> {
    if path == alias {
        Some("")
    } else {
        path.strip_prefix(alias)
            .and_then(|tail| tail.strip_prefix('/'))
    }
}

fn proc_self_exe_readlink_target(state: &WasiState) -> Option<String> {
    let argv0 = state.args.lock().unwrap().first()?.clone();
    if argv0.contains('/') {
        if argv0.starts_with('/') {
            Some(argv0)
        } else {
            Some(state.fs.relative_path_to_absolute(argv0))
        }
    } else {
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
        let base = path_env.split(':').next().unwrap_or("/bin");
        let base = if base.is_empty() {
            state.fs.current_dir.lock().unwrap().clone()
        } else if base.starts_with('/') {
            base.to_string()
        } else {
            state.fs.relative_path_to_absolute(base.to_string())
        };
        Some(join_virtual_path(&base, &argv0))
    }
}

fn proc_process_exe_readlink_target(process: &WasiProcess) -> Option<String> {
    if let Some(target) = process.executable_path() {
        return Some(target);
    }
    let argv0 = process.args().first()?.clone();
    if argv0.contains('/') {
        if argv0.starts_with('/') {
            Some(argv0)
        } else {
            Some(join_virtual_path(&process.cwd(), &argv0))
        }
    } else {
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
        let base = path_env.split(':').next().unwrap_or("/bin");
        let base = if base.is_empty() {
            process.cwd()
        } else if base.starts_with('/') {
            base.to_string()
        } else {
            join_virtual_path(&process.cwd(), base)
        };
        Some(join_virtual_path(&base, &argv0))
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

fn write_readlink_target<M: MemorySize>(
    memory: &MemoryView,
    buf: WasmPtr<u8, M>,
    buf_len: M::Offset,
    buf_used: WasmPtr<M::Offset, M>,
    target: &str,
) -> Result<Errno, WasiError> {
    let buf_len: u64 = buf_len.into();
    let bytes = target.as_bytes();
    if bytes.len() as u64 > buf_len {
        return Ok(Errno::Overflow);
    }
    let out = wasi_try_mem_ok!(buf.slice(memory, wasi_try_ok!(to_offset::<M>(bytes.len()))));
    wasi_try_mem_ok!(out.write_slice(bytes));
    let bytes_len: M::Offset = wasi_try_ok!(bytes.len().try_into().map_err(|_| Errno::Overflow));
    wasi_try_mem_ok!(buf_used.deref(memory).write(bytes_len));
    Ok(Errno::Success)
}
