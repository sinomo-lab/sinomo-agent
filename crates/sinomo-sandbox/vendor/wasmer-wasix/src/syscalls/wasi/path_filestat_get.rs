use super::*;
use crate::WasiProcess;
use crate::syscalls::*;
use crate::types::wasi::Snapshot0Filestat;

/// ### `path_filestat_get()`
/// Access metadata about a file or directory
/// Inputs:
/// - `Fd fd`
///     The directory that `path` is relative to
/// - `LookupFlags flags`
///     Flags to control how `path` is understood
/// - `const char *path`
///     String containing the file path
/// - `u32 path_len`
///     The length of the `path` string
/// Output:
/// - `__wasi_file_stat_t *buf`
///     The location where the metadata will be stored
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty), ret)]
pub fn path_filestat_get<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: LookupFlags,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    buf: WasmPtr<Filestat, M>,
) -> Errno {
    let env = ctx.data();
    let (memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    let mut path_string = unsafe { get_input_str!(&memory, path, path_len) };

    // Convert relative paths into absolute paths
    // let path_str = if path_string == "." {
    //     &"/"
    // } else if path_string.starts_with("./") {
    //     &path_string[1..]
    // } else {
    //     &path_string
    // };
    // if path_string.starts_with("./") || path_string == "." {
    //     path_string = ctx.data().state.fs.relative_path_to_absolute(path_string);
    // }
    tracing::trace!(path = path_string.as_str());
    let process_tids = env
        .process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw())
        .collect::<Vec<_>>();
    let processes = env.control_plane.processes();

    let stat = wasi_try!(path_filestat_get_internal(
        env,
        &memory,
        state,
        inodes,
        Some(env.pid().raw()),
        &process_tids,
        &processes,
        fd,
        flags,
        &path_string
    ));

    wasi_try_mem!(buf.deref(&memory).write(stat));

    Errno::Success
}

/// ### `path_filestat_get_internal()`
/// return a Filstat or Errno
pub(crate) fn path_filestat_get_internal(
    env: &WasiEnv,
    memory: &MemoryView,
    state: &WasiState,
    inodes: &crate::WasiInodes,
    current_pid: Option<u32>,
    process_tids: &[u32],
    processes: &[WasiProcess],
    fd: WasiFd,
    flags: LookupFlags,
    path_string: &str,
) -> Result<Filestat, Errno> {
    let root_dir = state.fs.get_fd(fd)?;

    if !root_dir.inner.rights.contains(Rights::PATH_FILESTAT_GET) {
        return Err(Errno::Access);
    }
    if let Some(stat) = proc_generated_process_filestat(processes, path_string) {
        return Ok(stat);
    }
    if let Some(target) = proc_path_alias_target(state, inodes, path_string, process_tids) {
        return path_filestat_get_internal(
            env,
            memory,
            state,
            inodes,
            current_pid,
            process_tids,
            processes,
            crate::VIRTUAL_ROOT_FD,
            flags,
            &target,
        );
    }
    if let Some(target) = proc_pid_alias_target(state, inodes, current_pid, processes, path_string)
    {
        return path_filestat_get_internal(
            env,
            memory,
            state,
            inodes,
            current_pid,
            process_tids,
            processes,
            crate::VIRTUAL_ROOT_FD,
            flags,
            &target,
        );
    }
    if let Some(stat) = proc_generated_filestat(state, current_pid, processes, path_string) {
        return Ok(stat);
    }
    if let Some(special_fd) = special_filestat_fd(path_string) {
        let fd_entry = state.fs.get_fd(special_fd)?;
        if !fd_entry.inner.rights.contains(Rights::FD_FILESTAT_GET) {
            return Err(Errno::Access);
        }
        return state.fs.filestat_fd(special_fd);
    }
    let file_inode = state.fs.get_inode_at_path(
        inodes,
        fd,
        path_string,
        flags & __WASI_LOOKUP_SYMLINK_FOLLOW != 0,
    )?;

    let mut stat = if file_inode.is_preopened {
        *file_inode.stat.read().unwrap().deref()
    } else {
        let guard = file_inode.read();
        state.fs.get_stat_for_kind(guard.deref())?
    };
    if let Some((hardlink_ino, hardlink_nlink)) = path_hardlink_stat_at(env, fd, path_string) {
        stat.st_ino = hardlink_ino;
        stat.st_nlink = hardlink_nlink;
    } else if stat.st_ino == 0 {
        stat.st_ino = file_inode.ino().as_u64();
    }
    Ok(stat)
}

fn proc_generated_filestat(
    state: &WasiState,
    current_pid: Option<u32>,
    processes: &[WasiProcess],
    path: &str,
) -> Option<Filestat> {
    if let Some(target) = proc_namespace_target(current_pid, processes, path) {
        return Some(Filestat {
            st_filetype: Filetype::SymbolicLink,
            st_size: target.len() as u64,
            ..Filestat::default()
        });
    }
    let size = match path {
        "/proc/sys/kernel/hostname" => current_pid
            .and_then(|pid| {
                processes
                    .iter()
                    .find(|process| process.pid().raw() == pid)
                    .map(|process| process.hostname().len() + 1)
            })
            .unwrap_or("sinomo-sandbox\n".len()),
        "/proc/sys/kernel/domainname" => current_pid
            .and_then(|pid| {
                processes
                    .iter()
                    .find(|process| process.pid().raw() == pid)
                    .map(|process| process.domainname().len() + 1)
            })
            .unwrap_or("localdomain\n".len()),
        "/proc/self/comm" => {
            let name = state
                .args
                .lock()
                .unwrap()
                .first()
                .map(|arg| {
                    std::path::Path::new(arg)
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or(arg)
                        .to_string()
                })
                .unwrap_or_else(|| "wasix".to_string());
            name.len() + 1
        }
        "/proc/self/cmdline" => state
            .args
            .lock()
            .unwrap()
            .iter()
            .map(|arg| arg.len() + 1)
            .sum(),
        "/proc/self/environ" => state
            .envs
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.len() + 1)
            .sum(),
        "/proc/self/stat" => 160,
        "/proc/self/status" => 768,
        "/proc/self/statm" => 14,
        "/proc/self/limits" => 512,
        "/proc/self/io" => 96,
        "/proc/self/cgroup" => 5,
        "/proc/self/cpuset" => 2,
        "/proc/self/uid_map" => 33,
        "/proc/self/gid_map" => 33,
        "/proc/self/projid_map" => 33,
        "/proc/self/setgroups" => 6,
        "/proc/self/children" => 0,
        "/proc/self/mountstats" => 0,
        "/proc/self/loginuid" => 11,
        "/proc/self/sessionid" => proc_current_sessionid_len(current_pid, processes),
        "/proc/self/oom_score" => 2,
        "/proc/self/oom_score_adj" => 2,
        "/proc/self/personality" => 9,
        "/proc/self/timers" => 0,
        "/proc/self/timerslack_ns" => 6,
        "/proc/self/schedstat" => 6,
        "/proc/self/sched" => 512,
        "/proc/self/autogroup" => 20,
        "/proc/self/wchan" => 2,
        "/proc/self/stack" => 0,
        "/proc/self/attr/current" => 11,
        "/proc/self/attr/exec" => 0,
        "/proc/self/attr/fscreate" => 0,
        "/proc/self/attr/keycreate" => 0,
        "/proc/self/attr/prev" => 11,
        "/proc/self/maps" => 160,
        "/proc/self/smaps" => 1_024,
        "/proc/self/numa_maps" => 96,
        "/proc/self/auxv" => std::mem::size_of::<usize>() * 2,
        _ if proc_task_children_path(path).is_some() => 0,
        _ => {
            let fd = path
                .strip_prefix("/proc/self/fdinfo/")
                .and_then(|fd| fd.parse::<WasiFd>().ok())?;
            state.fs.get_fd(fd).ok()?;
            48
        }
    };
    Some(Filestat {
        st_filetype: Filetype::RegularFile,
        st_size: size as u64,
        ..Filestat::default()
    })
}

fn proc_current_sessionid_len(current_pid: Option<u32>, processes: &[WasiProcess]) -> usize {
    let session_id = current_pid
        .and_then(|pid| {
            processes
                .iter()
                .find(|process| process.pid().raw() == pid)
                .map(|process| process.session_id().raw())
        })
        .unwrap_or(u32::MAX);
    format!("{session_id}\n").len()
}

fn proc_generated_process_filestat(processes: &[WasiProcess], path: &str) -> Option<Filestat> {
    let (process, tail) = proc_process_path(processes, path)?;
    let tail = proc_process_file_tail(&process, &tail)?;
    if let Some(target) = proc_process_namespace_target(&process, &tail) {
        return Some(Filestat {
            st_filetype: Filetype::SymbolicLink,
            st_size: target.len() as u64,
            ..Filestat::default()
        });
    }
    if let Some(target) = proc_process_fd_target(&process, &tail) {
        return Some(Filestat {
            st_filetype: Filetype::SymbolicLink,
            st_size: target.len() as u64,
            ..Filestat::default()
        });
    }
    let size = match tail.as_str() {
        "comm" => proc_process_name(&process).len() + 1,
        "cmdline" => process.args().iter().map(|arg| arg.len() + 1).sum(),
        "environ" => process.envs().iter().map(|entry| entry.len() + 1).sum(),
        "stat" => 160,
        "status" => 768,
        "statm" => 14,
        "limits" => 512,
        "io" => 96,
        "cgroup" => 5,
        "cpuset" => 2,
        "uid_map" | "gid_map" | "projid_map" => 33,
        "setgroups" => 6,
        "children" => 0,
        "mountstats" => 0,
        "loginuid" => 11,
        "sessionid" => format!("{}\n", process.session_id().raw()).len(),
        "oom_score" | "oom_score_adj" | "wchan" => 2,
        "personality" => 9,
        "timers" | "stack" => 0,
        "timerslack_ns" | "schedstat" => 6,
        "sched" => 512,
        "autogroup" => 20,
        "attr/current" | "attr/prev" => 11,
        "attr/exec" | "attr/fscreate" | "attr/keycreate" => 0,
        _ if tail.starts_with("fdinfo/") => {
            let fd = tail.strip_prefix("fdinfo/")?.parse::<WasiFd>().ok()?;
            let info = process.fd_info(fd)?;
            format!("pos:\t{}\nflags:\t{:o}\nmnt_id:\t1\n", info.pos, info.flags).len()
        }
        "maps" => 160,
        "smaps" => 1_024,
        "numa_maps" => 96,
        "auxv" => std::mem::size_of::<usize>() * 2,
        _ => return None,
    };
    Some(Filestat {
        st_filetype: Filetype::RegularFile,
        st_size: size as u64,
        ..Filestat::default()
    })
}

fn proc_process_fd_target(process: &WasiProcess, path: &str) -> Option<String> {
    let fd = path.strip_prefix("fd/")?.parse::<WasiFd>().ok()?;
    process.fd_info(fd).map(|info| info.target)
}

fn proc_process_namespace_target(process: &WasiProcess, path: &str) -> Option<String> {
    let namespace = path.strip_prefix("ns/")?;
    proc_namespace_link_target(process, namespace)
}

fn proc_process_path(processes: &[WasiProcess], path: &str) -> Option<(WasiProcess, String)> {
    let tail = path.strip_prefix("/proc/")?;
    let (pid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    if pid == "self" || pid == "thread-self" {
        return None;
    }
    let pid = pid.parse::<u32>().ok()?;
    let process = processes
        .iter()
        .find(|process| process.pid().raw() == pid)?;
    Some((process.clone(), rest.to_string()))
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

fn proc_process_name(process: &WasiProcess) -> String {
    process
        .args()
        .first()
        .map(|arg| {
            std::path::Path::new(arg)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(arg)
                .to_string()
        })
        .unwrap_or_else(|| "wasix".to_string())
}

fn proc_namespace_target(
    current_pid: Option<u32>,
    processes: &[WasiProcess],
    path: &str,
) -> Option<String> {
    let namespace = path.strip_prefix("/proc/self/ns/")?;
    let process = current_pid
        .and_then(|pid| {
            processes
                .iter()
                .find(|process| process.pid().raw() == pid)
                .cloned()
        })
        .or_else(|| processes.first().cloned())?;
    proc_namespace_link_target(&process, namespace)
}

fn proc_namespace_link_target(process: &WasiProcess, namespace: &str) -> Option<String> {
    let target_namespace = match namespace {
        "cgroup" | "ipc" | "mnt" | "net" | "pid" | "pid_for_children" | "time" | "uts" => namespace,
        _ => return None,
    };
    let display_namespace = if target_namespace == "pid_for_children" {
        "pid"
    } else {
        target_namespace
    };
    Some(format!(
        "{display_namespace}:[{}]",
        process.namespace_id(target_namespace)
    ))
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

fn proc_task_children_path(path: &str) -> Option<()> {
    let tail = path.strip_prefix("/proc/self/task/")?;
    let (tid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    (rest == "children" && tid.parse::<u64>().is_ok()).then_some(())
}

fn proc_pid_alias_target(
    state: &WasiState,
    inodes: &crate::WasiInodes,
    current_pid: Option<u32>,
    processes: &[WasiProcess],
    path: &str,
) -> Option<String> {
    let tail = path.strip_prefix("/proc/")?;
    let (pid, rest) = tail.split_once('/').unwrap_or((tail, ""));
    let pid = pid.parse::<u32>().ok()?;
    let process = processes
        .iter()
        .find(|process| process.pid().raw() == pid)?;
    if Some(pid) == current_pid {
        Some(join_virtual_path("/proc/self", rest))
    } else {
        let rest = proc_process_file_tail(process, rest)?;
        if rest == "cwd" {
            Some(process.cwd())
        } else if let Some(tail) = rest.strip_prefix("cwd/") {
            Some(join_virtual_path(&process.cwd(), tail))
        } else {
            match rest.as_str() {
                "" => Some("/proc/self".to_string()),
                "root" => Some("/proc/self/root".to_string()),
                "exe" => proc_process_exe_target(state, inodes, process),
                _ => Some(join_virtual_path("/proc/self", &rest)),
            }
        }
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

fn special_filestat_fd(path: &str) -> Option<WasiFd> {
    match path {
        "/dev/stdin" => Some(__WASI_STDIN_FILENO),
        "/dev/stdout" => Some(__WASI_STDOUT_FILENO),
        "/dev/stderr" => Some(__WASI_STDERR_FILENO),
        "/dev/tty" | "/dev/console" | "/dev/ptmx" => Some(__WASI_STDOUT_FILENO),
        _ => path
            .strip_prefix("/dev/fd/")
            .or_else(|| path.strip_prefix("/proc/self/fd/"))
            .and_then(|fd| fd.parse::<WasiFd>().ok()),
    }
}

/// ### `path_filestat_get_old()`
/// Access metadata about a file or directory
/// Inputs:
/// - `Fd fd`
///     The directory that `path` is relative to
/// - `LookupFlags flags`
///     Flags to control how `path` is understood
/// - `const char *path`
///     String containing the file path
/// - `u32 path_len`
///     The length of the `path` string
/// Output:
/// - `__wasi_file_stat_t *buf`
///     The location where the metadata will be stored
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty), ret)]
pub fn path_filestat_get_old<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: LookupFlags,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    buf: WasmPtr<Snapshot0Filestat, M>,
) -> Errno {
    let env = ctx.data();
    let (memory, mut state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    let path_string = unsafe { get_input_str!(&memory, path, path_len) };
    Span::current().record("path", path_string.as_str());
    let process_tids = env
        .process
        .all_threads()
        .into_iter()
        .map(|thread| thread.raw())
        .collect::<Vec<_>>();
    let processes = env.control_plane.processes();

    let stat = wasi_try!(path_filestat_get_internal(
        env,
        &memory,
        state,
        inodes,
        Some(env.pid().raw()),
        &process_tids,
        &processes,
        fd,
        flags,
        &path_string
    ));

    let old_stat = Snapshot0Filestat {
        st_dev: stat.st_dev,
        st_ino: stat.st_ino,
        st_filetype: stat.st_filetype,
        st_nlink: stat.st_nlink as u32,
        st_size: stat.st_size,
        st_atim: stat.st_atim,
        st_mtim: stat.st_mtim,
        st_ctim: stat.st_ctim,
    };
    wasi_try_mem!(buf.deref(&memory).write(old_stat));

    Errno::Success
}
