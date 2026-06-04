use crate::internal::*;

use crate::SandboxWasiError;
use virtual_fs::FileSystem as _;

#[derive(Clone)]
pub(crate) struct WasiVirtualSystemMounts {
    root_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    proc_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    etc_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    sys_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    run_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    var_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    tmp_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    home_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    root_home_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    usr_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    opt_fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
}

impl WasiVirtualSystemMounts {
    pub(crate) fn create(
        zero_device_bytes: usize,
        argv: &[String],
        env: &[(String, String)],
        mounts: &[(SandboxPath, SandboxMountMode)],
    ) -> Result<Self, SandboxWasiError> {
        let zero_device_bytes = zero_device_bytes.min(SANDBOX_WASI_ZERO_DEVICE_BYTES);
        let dev = virtual_fs::mem_fs::FileSystem::default();
        let proc = virtual_fs::mem_fs::FileSystem::default();
        let etc = virtual_fs::mem_fs::FileSystem::default();
        let sys = virtual_fs::mem_fs::FileSystem::default();
        let run = virtual_fs::mem_fs::FileSystem::default();
        let var = virtual_fs::mem_fs::FileSystem::default();
        let tmp = virtual_fs::mem_fs::FileSystem::default();
        let home = virtual_fs::mem_fs::FileSystem::default();
        let root_home = virtual_fs::mem_fs::FileSystem::default();
        let usr = virtual_fs::mem_fs::FileSystem::default();
        let opt = virtual_fs::mem_fs::FileSystem::default();

        populate_virtual_dev_mount(&dev, zero_device_bytes)?;
        populate_virtual_proc_mount(&proc, argv, env, mounts)?;
        populate_virtual_etc_mount(&etc, mounts)?;
        populate_virtual_sys_mount(&sys)?;
        for dir in ["user/1000", "lock"] {
            virtual_mount_create_dir_all(&run, dir, "/run")?;
        }
        for dir in ["tmp", "log", "cache", "lib"] {
            virtual_mount_create_dir_all(&var, dir, "/var")?;
        }
        for dir in [
            "sinomo",
            "sinomo/.cache",
            "sinomo/.config",
            "sinomo/.local/share",
        ] {
            virtual_mount_create_dir_all(&home, dir, "/home")?;
        }
        for dir in [".cache", ".config", ".local/share"] {
            virtual_mount_create_dir_all(&root_home, dir, "/root")?;
        }
        for dir in [
            "bin",
            "lib",
            "local/bin",
            "local/lib",
            "share",
            "share/terminfo/d",
            "share/terminfo/x",
            "share/zoneinfo/Etc",
        ] {
            virtual_mount_create_dir_all(&usr, dir, "/usr")?;
        }
        for (usr_file, content) in usr_system_files() {
            virtual_mount_write_file(&usr, &usr_file, content, "/usr")?;
        }
        virtual_mount_create_dir_all(&tmp, "/", "/tmp")?;
        virtual_mount_create_dir_all(&opt, "/", "/opt")?;

        Ok(Self {
            root_fs: Arc::new(dev),
            proc_fs: Arc::new(proc),
            etc_fs: Arc::new(etc),
            sys_fs: Arc::new(sys),
            run_fs: Arc::new(run),
            var_fs: Arc::new(var),
            tmp_fs: Arc::new(tmp),
            home_fs: Arc::new(home),
            root_home_fs: Arc::new(root_home),
            usr_fs: Arc::new(usr),
            opt_fs: Arc::new(opt),
        })
    }

    pub(crate) fn fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.root_fs)
    }

    pub(crate) fn proc_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.proc_fs)
    }

    pub(crate) fn etc_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.etc_fs)
    }

    pub(crate) fn sys_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.sys_fs)
    }

    pub(crate) fn run_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.run_fs)
    }

    pub(crate) fn var_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.var_fs)
    }

    pub(crate) fn tmp_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.tmp_fs)
    }

    pub(crate) fn home_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.home_fs)
    }

    pub(crate) fn root_home_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.root_home_fs)
    }

    pub(crate) fn usr_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.usr_fs)
    }

    pub(crate) fn opt_fs(&self) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
        Arc::clone(&self.opt_fs)
    }
}

fn virtual_mount_path(path: &str) -> PathBuf {
    if path == "/" || path.is_empty() {
        PathBuf::from("/")
    } else {
        PathBuf::from("/").join(path)
    }
}

fn virtual_mount_create_dir_all(
    fs: &virtual_fs::mem_fs::FileSystem,
    path: &str,
    guest_path: &str,
) -> Result<(), SandboxWasiError> {
    virtual_fs::create_dir_all(fs, &virtual_mount_path(path)).map_err(|error| {
        SandboxWasiError::Runtime {
            detail: format!("create generated {guest_path} virtual directory {path}: {error}"),
        }
    })
}

fn virtual_mount_write_file(
    fs: &virtual_fs::mem_fs::FileSystem,
    path: &str,
    bytes: impl AsRef<[u8]>,
    guest_path: &str,
) -> Result<(), SandboxWasiError> {
    let path_buf = virtual_mount_path(path);
    if let Some(parent) = path_buf.parent() {
        virtual_fs::create_dir_all(fs, parent).map_err(|error| SandboxWasiError::Runtime {
            detail: format!(
                "create generated {guest_path} virtual file parent {}: {error}",
                parent.display()
            ),
        })?;
    }
    let mut file = fs
        .new_open_options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path_buf)
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!(
                "open generated {guest_path} virtual file {}: {error}",
                path_buf.display()
            ),
        })?;
    virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(
        &mut file,
        bytes.as_ref(),
    ))
    .map_err(|error| SandboxWasiError::Runtime {
        detail: format!(
            "write generated {guest_path} virtual file {}: {error}",
            path_buf.display()
        ),
    })
}

fn populate_virtual_dev_mount(
    dev: &virtual_fs::mem_fs::FileSystem,
    zero_device_bytes: usize,
) -> Result<(), SandboxWasiError> {
    virtual_mount_write_file(dev, "null", [], "/dev")?;
    virtual_mount_write_file(dev, "zero", vec![0; zero_device_bytes], "/dev")?;
    let mut random = vec![0; 64 * 1024];
    getrandom::getrandom(&mut random).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("seed sandbox random devices: {error}"),
    })?;
    virtual_mount_write_file(dev, "random", &random, "/dev")?;
    virtual_mount_write_file(dev, "urandom", &random, "/dev")?;
    virtual_mount_write_file(dev, "full", vec![0; zero_device_bytes], "/dev")?;
    for device in ["stdin", "stdout", "stderr", "tty", "console", "ptmx"] {
        virtual_mount_write_file(dev, device, [], "/dev")?;
    }
    for dir in ["fd", "pts", "shm"] {
        virtual_mount_create_dir_all(dev, dir, "/dev")?;
    }
    for fd in ["0", "1", "2"] {
        virtual_mount_write_file(dev, &format!("fd/{fd}"), [], "/dev")?;
    }
    Ok(())
}

fn populate_virtual_proc_mount(
    proc: &virtual_fs::mem_fs::FileSystem,
    argv: &[String],
    env: &[(String, String)],
    mounts: &[(SandboxPath, SandboxMountMode)],
) -> Result<(), SandboxWasiError> {
    for dir in [
        "self/fd",
        "self/fdinfo",
        "self/net",
        "self/attr",
        "self/ns",
        "self/task",
        "thread-self",
        "1/fd",
        "1/fdinfo",
        "1/net",
        "1/attr",
        "1/ns",
        "1/task/1/fd",
        "1/task/1/fdinfo",
        "1/task/1/net",
        "1/task/1/attr",
        "1/task/1/ns",
        "self/task/1/fd",
        "self/task/1/fdinfo",
        "self/task/1/net",
        "self/task/1/attr",
        "self/task/1/ns",
        "net",
        "pressure",
        "sys/crypto",
        "sys/fs/epoll",
        "sys/fs/inotify",
        "sys/fs/mqueue",
        "sys/kernel/keys",
        "sys/kernel/random",
        "sys/kernel/yama",
        "sys/net/core",
        "sys/net/ipv4",
        "sys/net/ipv6/conf/all",
        "sys/net/ipv6/conf/default",
        "sys/net/unix",
        "sys/user",
        "sys/vm",
    ] {
        virtual_mount_create_dir_all(proc, dir, "/proc")?;
    }
    for fd in ["0", "1", "2"] {
        virtual_mount_write_file(proc, &format!("self/fd/{fd}"), [], "/proc")?;
        virtual_mount_write_file(
            proc,
            &format!("self/fdinfo/{fd}"),
            proc_self_fdinfo(fd),
            "/proc",
        )?;
    }
    for link in ["cwd", "root", "exe"] {
        virtual_mount_write_file(proc, &format!("self/{link}"), [], "/proc")?;
    }
    for (proc_file, content) in proc_self_files(argv) {
        virtual_mount_write_file(proc, &format!("self/{proc_file}"), content, "/proc")?;
    }
    virtual_mount_write_file(proc, "self/cmdline", proc_cmdline(argv), "/proc")?;
    virtual_mount_write_file(proc, "self/environ", proc_environ(env), "/proc")?;
    let mounts = proc_mounts(mounts);
    virtual_mount_write_file(proc, "mounts", &mounts, "/proc")?;
    virtual_mount_write_file(proc, "self/mounts", &mounts, "/proc")?;
    let mountinfo = proc_mountinfo(&mounts);
    virtual_mount_write_file(proc, "mountinfo", &mountinfo, "/proc")?;
    virtual_mount_write_file(proc, "self/mountinfo", &mountinfo, "/proc")?;
    for (proc_file, content) in proc_system_files() {
        virtual_mount_write_file(proc, &proc_file, content, "/proc")?;
    }
    for (proc_file, content) in proc_self_net_files() {
        virtual_mount_write_file(proc, &format!("self/net/{proc_file}"), content, "/proc")?;
    }
    for (proc_file, content) in proc_self_attr_files() {
        virtual_mount_write_file(proc, &format!("self/attr/{proc_file}"), content, "/proc")?;
    }
    for proc_file in proc_self_ns_files() {
        virtual_mount_write_file(proc, &format!("self/ns/{proc_file}"), [], "/proc")?;
    }
    populate_virtual_proc_pid_view(proc, "1", argv, env, &mounts, &mountinfo)?;
    populate_virtual_proc_task_view(proc, "self/task/1", argv, env, &mounts, &mountinfo)?;
    Ok(())
}

fn populate_virtual_proc_pid_view(
    proc: &virtual_fs::mem_fs::FileSystem,
    pid: &str,
    argv: &[String],
    env: &[(String, String)],
    mounts: &str,
    mountinfo: &str,
) -> Result<(), SandboxWasiError> {
    for fd in ["0", "1", "2"] {
        virtual_mount_write_file(proc, &format!("{pid}/fd/{fd}"), [], "/proc")?;
        virtual_mount_write_file(
            proc,
            &format!("{pid}/fdinfo/{fd}"),
            proc_self_fdinfo(fd),
            "/proc",
        )?;
    }
    for link in ["cwd", "root", "exe"] {
        virtual_mount_write_file(proc, &format!("{pid}/{link}"), [], "/proc")?;
    }
    for (proc_file, content) in proc_self_files(argv) {
        virtual_mount_write_file(proc, &format!("{pid}/{proc_file}"), content, "/proc")?;
    }
    virtual_mount_write_file(proc, &format!("{pid}/cmdline"), proc_cmdline(argv), "/proc")?;
    virtual_mount_write_file(proc, &format!("{pid}/environ"), proc_environ(env), "/proc")?;
    virtual_mount_write_file(proc, &format!("{pid}/mounts"), mounts, "/proc")?;
    virtual_mount_write_file(proc, &format!("{pid}/mountinfo"), mountinfo, "/proc")?;
    for (proc_file, content) in proc_self_net_files() {
        virtual_mount_write_file(proc, &format!("{pid}/net/{proc_file}"), content, "/proc")?;
    }
    for (proc_file, content) in proc_self_attr_files() {
        virtual_mount_write_file(proc, &format!("{pid}/attr/{proc_file}"), content, "/proc")?;
    }
    for proc_file in proc_self_ns_files() {
        virtual_mount_write_file(proc, &format!("{pid}/ns/{proc_file}"), [], "/proc")?;
    }
    populate_virtual_proc_task_view(proc, &format!("{pid}/task/1"), argv, env, mounts, mountinfo)
}

fn populate_virtual_proc_task_view(
    proc: &virtual_fs::mem_fs::FileSystem,
    task_path: &str,
    argv: &[String],
    env: &[(String, String)],
    mounts: &str,
    mountinfo: &str,
) -> Result<(), SandboxWasiError> {
    for fd in ["0", "1", "2"] {
        virtual_mount_write_file(proc, &format!("{task_path}/fd/{fd}"), [], "/proc")?;
        virtual_mount_write_file(
            proc,
            &format!("{task_path}/fdinfo/{fd}"),
            proc_self_fdinfo(fd),
            "/proc",
        )?;
    }
    for link in ["cwd", "root", "exe"] {
        virtual_mount_write_file(proc, &format!("{task_path}/{link}"), [], "/proc")?;
    }
    virtual_mount_write_file(proc, &format!("{task_path}/children"), [], "/proc")?;
    for (proc_file, content) in proc_self_files(argv) {
        virtual_mount_write_file(proc, &format!("{task_path}/{proc_file}"), content, "/proc")?;
    }
    virtual_mount_write_file(
        proc,
        &format!("{task_path}/cmdline"),
        proc_cmdline(argv),
        "/proc",
    )?;
    virtual_mount_write_file(
        proc,
        &format!("{task_path}/environ"),
        proc_environ(env),
        "/proc",
    )?;
    virtual_mount_write_file(proc, &format!("{task_path}/mounts"), mounts, "/proc")?;
    virtual_mount_write_file(proc, &format!("{task_path}/mountinfo"), mountinfo, "/proc")?;
    for (proc_file, content) in proc_self_net_files() {
        virtual_mount_write_file(
            proc,
            &format!("{task_path}/net/{proc_file}"),
            content,
            "/proc",
        )?;
    }
    for (proc_file, content) in proc_self_attr_files() {
        virtual_mount_write_file(
            proc,
            &format!("{task_path}/attr/{proc_file}"),
            content,
            "/proc",
        )?;
    }
    for proc_file in proc_self_ns_files() {
        virtual_mount_write_file(proc, &format!("{task_path}/ns/{proc_file}"), [], "/proc")?;
    }
    Ok(())
}

fn populate_virtual_etc_mount(
    etc: &virtual_fs::mem_fs::FileSystem,
    mounts: &[(SandboxPath, SandboxMountMode)],
) -> Result<(), SandboxWasiError> {
    for dir in ["ssl/certs", "pki/tls/certs", "default"] {
        virtual_mount_create_dir_all(etc, dir, "/etc")?;
    }
    let mounts = proc_mounts(mounts);
    virtual_mount_write_file(etc, "mtab", mounts, "/etc")?;
    for (etc_file, content) in etc_system_files() {
        virtual_mount_write_file(etc, &etc_file, content, "/etc")?;
    }
    Ok(())
}

fn populate_virtual_sys_mount(
    sys: &virtual_fs::mem_fs::FileSystem,
) -> Result<(), SandboxWasiError> {
    for dir in [
        "block",
        "class/block",
        "class/mem",
        "class/net/lo",
        "class/tty/console",
        "class/tty/tty",
        "devices/system/cpu",
        "devices/system/node",
        "fs/cgroup",
        "devices/virtual/mem/null",
        "devices/virtual/mem/zero",
        "devices/virtual/mem/full",
        "devices/virtual/mem/random",
        "devices/virtual/mem/urandom",
        "devices/virtual/net/lo",
        "devices/virtual/tty/console",
        "devices/virtual/tty/tty",
        "kernel/mm/transparent_hugepage",
    ] {
        virtual_mount_create_dir_all(sys, dir, "/sys")?;
    }
    for processor in 0..sandbox_cpu_count() {
        virtual_mount_create_dir_all(
            sys,
            &format!("devices/system/cpu/cpu{processor}/topology"),
            "/sys",
        )?;
        virtual_mount_create_dir_all(
            sys,
            &format!("devices/system/cpu/cpu{processor}/cache/index0"),
            "/sys",
        )?;
    }
    for (sys_file, content) in sys_system_files() {
        virtual_mount_write_file(sys, &sys_file, content, "/sys")?;
    }
    Ok(())
}

fn proc_cmdline(argv: &[String]) -> Vec<u8> {
    let mut cmdline = Vec::new();
    for arg in argv {
        cmdline.extend_from_slice(arg.as_bytes());
        cmdline.push(0);
    }
    cmdline
}

fn proc_environ(env: &[(String, String)]) -> Vec<u8> {
    let mut environ = Vec::new();
    for (key, value) in env {
        environ.extend_from_slice(key.as_bytes());
        environ.push(b'=');
        environ.extend_from_slice(value.as_bytes());
        environ.push(0);
    }
    environ
}

fn proc_mounts(mounts: &[(SandboxPath, SandboxMountMode)]) -> String {
    let mut output = String::new();
    for (guest_path, mode) in mounts {
        let options = match mode {
            SandboxMountMode::ReadOnly => "ro,nosuid,nodev,noexec,relatime",
            SandboxMountMode::ReadWrite => "rw,nosuid,nodev,noexec,relatime",
        };
        output.push_str(&format!(
            "sinomo-sandbox {} sifs {options} 0 0\n",
            proc_escape_mount_field(guest_path.as_str())
        ));
    }
    output.push_str("sinomo-sandbox /dev devtmpfs rw,nosuid,noexec,relatime 0 0\n");
    output.push_str("sinomo-sandbox /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n");
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/etc")
    {
        output.push_str("sinomo-sandbox /etc tmpfs ro,nosuid,nodev,noexec,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/sys")
    {
        output.push_str("sinomo-sandbox /sys sysfs ro,nosuid,nodev,noexec,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/run")
    {
        output.push_str("sinomo-sandbox /run tmpfs rw,nosuid,nodev,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/var")
    {
        output.push_str("sinomo-sandbox /var tmpfs rw,nosuid,nodev,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/tmp")
    {
        output.push_str("sinomo-sandbox /tmp tmpfs rw,nosuid,nodev,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/home")
    {
        output.push_str("sinomo-sandbox /home tmpfs rw,nosuid,nodev,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/root")
    {
        output.push_str("sinomo-sandbox /root tmpfs rw,nosuid,nodev,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/usr")
    {
        output.push_str("sinomo-sandbox /usr tmpfs ro,nosuid,nodev,relatime 0 0\n");
    }
    if !mounts
        .iter()
        .any(|(guest_path, _)| guest_path.as_str() == "/opt")
    {
        output.push_str("sinomo-sandbox /opt tmpfs ro,nosuid,nodev,relatime 0 0\n");
    }
    output
}

fn proc_mountinfo(mounts: &str) -> String {
    let mut output = String::new();
    for (index, line) in mounts.lines().enumerate() {
        let mut fields = line.split_whitespace();
        let Some(_source) = fields.next() else {
            continue;
        };
        let Some(mount_point) = fields.next() else {
            continue;
        };
        let Some(fs_type) = fields.next() else {
            continue;
        };
        let Some(options) = fields.next() else {
            continue;
        };
        let id = index + 1;
        output.push_str(&format!(
            "{id} 0 0:0 / {mount_point} {options} - {fs_type} sinomo-sandbox {options}\n"
        ));
    }
    output
}

fn proc_escape_mount_field(value: &str) -> String {
    value
        .replace('\\', "\\134")
        .replace(' ', "\\040")
        .replace('\t', "\\011")
        .replace('\n', "\\012")
}

fn proc_self_files(argv: &[String]) -> Vec<(String, Vec<u8>)> {
    let name = proc_self_name(argv);
    vec![
        ("comm".to_string(), format!("{name}\n").into_bytes()),
        ("stat".to_string(), proc_self_stat(&name).into_bytes()),
        ("status".to_string(), proc_self_status(&name).into_bytes()),
        ("statm".to_string(), b"0 0 0 0 0 0 0\n".to_vec()),
        ("limits".to_string(), proc_self_limits().into_bytes()),
        ("io".to_string(), proc_self_io().into_bytes()),
        ("cgroup".to_string(), b"0::/\n".to_vec()),
        ("cpuset".to_string(), b"/\n".to_vec()),
        ("uid_map".to_string(), proc_self_uid_map().into_bytes()),
        ("gid_map".to_string(), proc_self_gid_map().into_bytes()),
        (
            "projid_map".to_string(),
            proc_self_projid_map().into_bytes(),
        ),
        ("setgroups".to_string(), b"allow\n".to_vec()),
        ("mountstats".to_string(), Vec::new()),
        ("loginuid".to_string(), b"4294967295\n".to_vec()),
        ("sessionid".to_string(), b"4294967295\n".to_vec()),
        ("oom_score".to_string(), b"0\n".to_vec()),
        ("oom_score_adj".to_string(), b"0\n".to_vec()),
        ("personality".to_string(), b"00000000\n".to_vec()),
        ("timers".to_string(), Vec::new()),
        ("timerslack_ns".to_string(), b"50000\n".to_vec()),
        ("schedstat".to_string(), b"0 0 0\n".to_vec()),
        ("sched".to_string(), proc_self_sched(&name, 1).into_bytes()),
        ("autogroup".to_string(), b"/autogroup-0 nice 0\n".to_vec()),
        ("wchan".to_string(), b"0\n".to_vec()),
        ("stack".to_string(), Vec::new()),
        ("maps".to_string(), proc_self_maps().into_bytes()),
        ("smaps".to_string(), proc_self_smaps().into_bytes()),
        ("numa_maps".to_string(), proc_self_numa_maps().into_bytes()),
        (
            "auxv".to_string(),
            vec![0; std::mem::size_of::<usize>() * 2],
        ),
    ]
}

fn proc_self_name(argv: &[String]) -> String {
    argv.first()
        .map(|arg| {
            Path::new(arg)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(arg)
                .to_string()
        })
        .unwrap_or_else(|| "wasix".to_string())
}

fn proc_self_stat(name: &str) -> String {
    format!(
        "1 ({name}) R 0 1 1 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
    )
}

fn proc_self_status(name: &str) -> String {
    format!(
        "Name:\t{name}\nUmask:\t0022\nState:\tR (running)\nTgid:\t1\nNgid:\t0\nPid:\t1\nPPid:\t0\nTracerPid:\t0\nUid:\t1000\t1000\t1000\t1000\nGid:\t1000\t1000\t1000\t1000\nFDSize:\t3\nThreads:\t1\nSigQ:\t0/0\nSigPnd:\t0000000000000000\nShdPnd:\t0000000000000000\nSigBlk:\t0000000000000000\nSigIgn:\t0000000000000000\nSigCgt:\t0000000000000000\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\nNoNewPrivs:\t1\nSeccomp:\t0\n"
    )
}

fn proc_self_limits() -> String {
    "Limit                     Soft Limit           Hard Limit           Units\nMax cpu time              unlimited            unlimited            seconds\nMax file size             unlimited            unlimited            bytes\nMax data size             unlimited            unlimited            bytes\nMax stack size            unlimited            unlimited            bytes\nMax open files            1024                 1024                 files\nMax processes             unlimited            unlimited            processes\nMax address space         unlimited            unlimited            bytes\n".to_string()
}

fn proc_self_io() -> String {
    "rchar: 0\nwchar: 0\nsyscr: 0\nsyscw: 0\nread_bytes: 0\nwrite_bytes: 0\ncancelled_write_bytes: 0\n"
        .to_string()
}

fn proc_self_uid_map() -> String {
    "         0          0 4294967295\n".to_string()
}

fn proc_self_gid_map() -> String {
    "         0          0 4294967295\n".to_string()
}

fn proc_self_projid_map() -> String {
    "         0          0 4294967295\n".to_string()
}

fn proc_self_sched(name: &str, pid: u64) -> String {
    format!(
        "{name} ({pid}, #threads: 1)\n-------------------------------------------------------------------\nse.exec_start                                :             0.000000\nse.vruntime                                  :             0.000000\nse.sum_exec_runtime                          :             0.000000\nnr_switches                                  :                    0\nnr_voluntary_switches                        :                    0\nnr_involuntary_switches                      :                    0\nse.load.weight                               :              1048576\npolicy                                       :                    0\nprio                                         :                  120\n"
    )
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

fn proc_self_fdinfo(fd: &str) -> String {
    let flags = match fd {
        "0" => 0o0,
        "1" | "2" => 0o2001,
        _ => 0o0,
    };
    format!("pos:\t0\nflags:\t{flags:o}\nmnt_id:\t1\n")
}

fn proc_system_files() -> Vec<(&'static str, String)> {
    mod sandbox_policy {
        pub(super) fn configured_value(_path: &str) -> Option<String> {
            None
        }
    }
    vec![
        (
            "cpuinfo",
            sandbox_policy::configured_value("/proc/cpuinfo")
                .unwrap_or_else(|| proc_cpuinfo_fallback()),
        ),
        (
            "meminfo",
            sandbox_policy::configured_value("/proc/meminfo")
                .unwrap_or_else(|| proc_meminfo_fallback()),
        ),
        (
            "stat",
            sandbox_policy::configured_value("/proc/stat").unwrap_or_else(|| proc_stat_fallback()),
        ),
        (
            "uptime",
            sandbox_policy::configured_value("/proc/uptime")
                .unwrap_or_else(|| "0.00 0.00\n".to_string()),
        ),
        (
            "loadavg",
            sandbox_policy::configured_value("/proc/loadavg")
                .unwrap_or_else(|| "0.00 0.00 0.00 1/1 1\n".to_string()),
        ),
        (
            "version",
            sandbox_policy::configured_value("/proc/version")
                .unwrap_or_else(|| "Sinomo sandbox WASIX\n".to_string()),
        ),
        (
            "buddyinfo",
            sandbox_policy::configured_value("/proc/buddyinfo").unwrap_or_else(|| String::new()),
        ),
        (
            "crypto",
            sandbox_policy::configured_value("/proc/crypto").unwrap_or_else(|| proc_crypto()),
        ),
        (
            "devices",
            sandbox_policy::configured_value("/proc/devices").unwrap_or_else(|| proc_devices()),
        ),
        (
            "diskstats",
            sandbox_policy::configured_value("/proc/diskstats").unwrap_or_else(|| String::new()),
        ),
        ("filesystems", proc_filesystems()),
        (
            "locks",
            sandbox_policy::configured_value("/proc/locks").unwrap_or_else(|| String::new()),
        ),
        (
            "modules",
            sandbox_policy::configured_value("/proc/modules").unwrap_or_else(|| String::new()),
        ),
        (
            "partitions",
            sandbox_policy::configured_value("/proc/partitions")
                .unwrap_or_else(|| proc_partitions()),
        ),
        (
            "swaps",
            sandbox_policy::configured_value("/proc/swaps").unwrap_or_else(|| proc_swaps()),
        ),
        (
            "vmstat",
            sandbox_policy::configured_value("/proc/vmstat").unwrap_or_else(|| proc_vmstat()),
        ),
        (
            "zoneinfo",
            sandbox_policy::configured_value("/proc/zoneinfo").unwrap_or_else(|| proc_zoneinfo()),
        ),
        (
            "pressure/cpu",
            sandbox_policy::configured_value("/proc/pressure/cpu")
                .unwrap_or_else(|| proc_pressure()),
        ),
        (
            "pressure/io",
            sandbox_policy::configured_value("/proc/pressure/io")
                .unwrap_or_else(|| proc_pressure()),
        ),
        (
            "pressure/memory",
            sandbox_policy::configured_value("/proc/pressure/memory")
                .unwrap_or_else(|| proc_pressure()),
        ),
        ("net/dev", proc_net_dev()),
        ("net/route", proc_net_route()),
        ("net/if_inet6", String::new()),
        ("net/tcp", proc_net_socket_table()),
        ("net/tcp6", proc_net_socket_table()),
        ("net/udp", proc_net_socket_table()),
        ("net/udp6", proc_net_socket_table()),
        ("net/unix", proc_net_unix()),
        (
            "sys/kernel/ostype",
            sandbox_policy::configured_value("/proc/sys/kernel/ostype")
                .unwrap_or_else(|| "Linux\n".to_string()),
        ),
        (
            "sys/kernel/osrelease",
            sandbox_policy::configured_value("/proc/sys/kernel/osrelease")
                .unwrap_or_else(|| "6.0.0-sinomo-sandbox\n".to_string()),
        ),
        (
            "sys/kernel/version",
            sandbox_policy::configured_value("/proc/sys/kernel/version")
                .unwrap_or_else(|| "Sinomo sandbox WASIX\n".to_string()),
        ),
        (
            "sys/kernel/hostname",
            sandbox_policy::configured_value("/proc/sys/kernel/hostname")
                .unwrap_or_else(|| "sinomo-sandbox\n".to_string()),
        ),
        (
            "sys/kernel/domainname",
            sandbox_policy::configured_value("/proc/sys/kernel/domainname")
                .unwrap_or_else(|| "(none)\n".to_string()),
        ),
        (
            "sys/kernel/pid_max",
            sandbox_policy::configured_value("/proc/sys/kernel/pid_max")
                .unwrap_or_else(|| "4194304\n".to_string()),
        ),
        (
            "sys/kernel/threads-max",
            sandbox_policy::configured_value("/proc/sys/kernel/threads-max")
                .unwrap_or_else(|| "4194304\n".to_string()),
        ),
        (
            "sys/kernel/ngroups_max",
            sandbox_policy::configured_value("/proc/sys/kernel/ngroups_max")
                .unwrap_or_else(|| "65536\n".to_string()),
        ),
        (
            "sys/kernel/cap_last_cap",
            sandbox_policy::configured_value("/proc/sys/kernel/cap_last_cap")
                .unwrap_or_else(|| "40\n".to_string()),
        ),
        (
            "sys/kernel/shmmax",
            sandbox_policy::configured_value("/proc/sys/kernel/shmmax")
                .unwrap_or_else(|| "18446744073692774399\n".to_string()),
        ),
        (
            "sys/kernel/shmall",
            sandbox_policy::configured_value("/proc/sys/kernel/shmall")
                .unwrap_or_else(|| "18446744073692774399\n".to_string()),
        ),
        (
            "sys/kernel/shmmni",
            sandbox_policy::configured_value("/proc/sys/kernel/shmmni")
                .unwrap_or_else(|| "4096\n".to_string()),
        ),
        (
            "sys/kernel/msgmax",
            sandbox_policy::configured_value("/proc/sys/kernel/msgmax")
                .unwrap_or_else(|| "8192\n".to_string()),
        ),
        (
            "sys/kernel/msgmnb",
            sandbox_policy::configured_value("/proc/sys/kernel/msgmnb")
                .unwrap_or_else(|| "16384\n".to_string()),
        ),
        (
            "sys/kernel/msgmni",
            sandbox_policy::configured_value("/proc/sys/kernel/msgmni")
                .unwrap_or_else(|| "32000\n".to_string()),
        ),
        (
            "sys/kernel/sem",
            sandbox_policy::configured_value("/proc/sys/kernel/sem")
                .unwrap_or_else(|| "32000\t1024000000\t500\t32000\n".to_string()),
        ),
        (
            "sys/kernel/core_pattern",
            sandbox_policy::configured_value("/proc/sys/kernel/core_pattern")
                .unwrap_or_else(|| "core\n".to_string()),
        ),
        (
            "sys/kernel/core_uses_pid",
            sandbox_policy::configured_value("/proc/sys/kernel/core_uses_pid")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/hotplug",
            sandbox_policy::configured_value("/proc/sys/kernel/hotplug")
                .unwrap_or_else(|| "\n".to_string()),
        ),
        (
            "sys/kernel/kptr_restrict",
            sandbox_policy::configured_value("/proc/sys/kernel/kptr_restrict")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/kernel/keys/gc_delay",
            sandbox_policy::configured_value("/proc/sys/kernel/keys/gc_delay")
                .unwrap_or_else(|| "300\n".to_string()),
        ),
        (
            "sys/kernel/keys/maxbytes",
            sandbox_policy::configured_value("/proc/sys/kernel/keys/maxbytes")
                .unwrap_or_else(|| "20000\n".to_string()),
        ),
        (
            "sys/kernel/keys/maxkeys",
            sandbox_policy::configured_value("/proc/sys/kernel/keys/maxkeys")
                .unwrap_or_else(|| "200\n".to_string()),
        ),
        (
            "sys/kernel/keys/root_maxbytes",
            sandbox_policy::configured_value("/proc/sys/kernel/keys/root_maxbytes")
                .unwrap_or_else(|| "25000000\n".to_string()),
        ),
        (
            "sys/kernel/keys/root_maxkeys",
            sandbox_policy::configured_value("/proc/sys/kernel/keys/root_maxkeys")
                .unwrap_or_else(|| "1000000\n".to_string()),
        ),
        (
            "sys/kernel/kstack_depth_to_print",
            sandbox_policy::configured_value("/proc/sys/kernel/kstack_depth_to_print")
                .unwrap_or_else(|| "24\n".to_string()),
        ),
        (
            "sys/kernel/modules_disabled",
            sandbox_policy::configured_value("/proc/sys/kernel/modules_disabled")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/kernel/nmi_watchdog",
            sandbox_policy::configured_value("/proc/sys/kernel/nmi_watchdog")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/panic",
            sandbox_policy::configured_value("/proc/sys/kernel/panic")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/panic_on_oops",
            sandbox_policy::configured_value("/proc/sys/kernel/panic_on_oops")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/perf_event_paranoid",
            sandbox_policy::configured_value("/proc/sys/kernel/perf_event_paranoid")
                .unwrap_or_else(|| "3\n".to_string()),
        ),
        (
            "sys/kernel/perf_event_max_sample_rate",
            sandbox_policy::configured_value("/proc/sys/kernel/perf_event_max_sample_rate")
                .unwrap_or_else(|| "100000\n".to_string()),
        ),
        (
            "sys/kernel/sched_child_runs_first",
            sandbox_policy::configured_value("/proc/sys/kernel/sched_child_runs_first")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/sched_rr_timeslice_ms",
            sandbox_policy::configured_value("/proc/sys/kernel/sched_rr_timeslice_ms")
                .unwrap_or_else(|| "100\n".to_string()),
        ),
        (
            "sys/kernel/sched_rt_period_us",
            sandbox_policy::configured_value("/proc/sys/kernel/sched_rt_period_us")
                .unwrap_or_else(|| "1000000\n".to_string()),
        ),
        (
            "sys/kernel/sched_rt_runtime_us",
            sandbox_policy::configured_value("/proc/sys/kernel/sched_rt_runtime_us")
                .unwrap_or_else(|| "950000\n".to_string()),
        ),
        (
            "sys/kernel/sysrq",
            sandbox_policy::configured_value("/proc/sys/kernel/sysrq")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/yama/ptrace_scope",
            sandbox_policy::configured_value("/proc/sys/kernel/yama/ptrace_scope")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/fs/file-max",
            sandbox_policy::configured_value("/proc/sys/fs/file-max")
                .unwrap_or_else(|| "9223372036854775807\n".to_string()),
        ),
        (
            "sys/fs/file-nr",
            sandbox_policy::configured_value("/proc/sys/fs/file-nr")
                .unwrap_or_else(|| "0\t0\t9223372036854775807\n".to_string()),
        ),
        (
            "sys/fs/inode-nr",
            sandbox_policy::configured_value("/proc/sys/fs/inode-nr")
                .unwrap_or_else(|| "0\t0\n".to_string()),
        ),
        (
            "sys/fs/inode-state",
            sandbox_policy::configured_value("/proc/sys/fs/inode-state")
                .unwrap_or_else(|| "0\t0\t0\t0\t0\t0\t0\n".to_string()),
        ),
        (
            "sys/fs/aio-max-nr",
            sandbox_policy::configured_value("/proc/sys/fs/aio-max-nr")
                .unwrap_or_else(|| "65536\n".to_string()),
        ),
        (
            "sys/fs/aio-nr",
            sandbox_policy::configured_value("/proc/sys/fs/aio-nr")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/fs/dentry-state",
            sandbox_policy::configured_value("/proc/sys/fs/dentry-state")
                .unwrap_or_else(|| "0\t0\t45\t0\t0\t0\n".to_string()),
        ),
        (
            "sys/fs/epoll/max_user_watches",
            sandbox_policy::configured_value("/proc/sys/fs/epoll/max_user_watches")
                .unwrap_or_else(|| "1048576\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msg_default",
            sandbox_policy::configured_value("/proc/sys/fs/mqueue/msg_default")
                .unwrap_or_else(|| "10\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msg_max",
            sandbox_policy::configured_value("/proc/sys/fs/mqueue/msg_max")
                .unwrap_or_else(|| "10\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msgsize_default",
            sandbox_policy::configured_value("/proc/sys/fs/mqueue/msgsize_default")
                .unwrap_or_else(|| "8192\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msgsize_max",
            sandbox_policy::configured_value("/proc/sys/fs/mqueue/msgsize_max")
                .unwrap_or_else(|| "8192\n".to_string()),
        ),
        (
            "sys/fs/nr_open",
            sandbox_policy::configured_value("/proc/sys/fs/nr_open")
                .unwrap_or_else(|| "1048576\n".to_string()),
        ),
        (
            "sys/fs/pipe-max-size",
            sandbox_policy::configured_value("/proc/sys/fs/pipe-max-size")
                .unwrap_or_else(|| "1048576\n".to_string()),
        ),
        (
            "sys/fs/pipe-user-pages-hard",
            sandbox_policy::configured_value("/proc/sys/fs/pipe-user-pages-hard")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/fs/pipe-user-pages-soft",
            sandbox_policy::configured_value("/proc/sys/fs/pipe-user-pages-soft")
                .unwrap_or_else(|| "16384\n".to_string()),
        ),
        (
            "sys/fs/inotify/max_user_watches",
            sandbox_policy::configured_value("/proc/sys/fs/inotify/max_user_watches")
                .unwrap_or_else(|| "1048576\n".to_string()),
        ),
        (
            "sys/fs/inotify/max_user_instances",
            sandbox_policy::configured_value("/proc/sys/fs/inotify/max_user_instances")
                .unwrap_or_else(|| "1024\n".to_string()),
        ),
        (
            "sys/fs/inotify/max_queued_events",
            sandbox_policy::configured_value("/proc/sys/fs/inotify/max_queued_events")
                .unwrap_or_else(|| "16384\n".to_string()),
        ),
        (
            "sys/crypto/fips_enabled",
            sandbox_policy::configured_value("/proc/sys/crypto/fips_enabled")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/kernel/random/boot_id",
            sandbox_policy::configured_value("/proc/sys/kernel/random/boot_id")
                .unwrap_or_else(|| format!("{}\n", proc_uuid())),
        ),
        (
            "sys/kernel/random/entropy_avail",
            sandbox_policy::configured_value("/proc/sys/kernel/random/entropy_avail")
                .unwrap_or_else(|| "256\n".to_string()),
        ),
        (
            "sys/kernel/random/poolsize",
            sandbox_policy::configured_value("/proc/sys/kernel/random/poolsize")
                .unwrap_or_else(|| "256\n".to_string()),
        ),
        (
            "sys/kernel/random/urandom_min_reseed_secs",
            sandbox_policy::configured_value("/proc/sys/kernel/random/urandom_min_reseed_secs")
                .unwrap_or_else(|| "60\n".to_string()),
        ),
        (
            "sys/kernel/random/write_wakeup_threshold",
            sandbox_policy::configured_value("/proc/sys/kernel/random/write_wakeup_threshold")
                .unwrap_or_else(|| "256\n".to_string()),
        ),
        (
            "sys/kernel/random/uuid",
            sandbox_policy::configured_value("/proc/sys/kernel/random/uuid")
                .unwrap_or_else(|| format!("{}\n", proc_uuid())),
        ),
        (
            "sys/net/core/somaxconn",
            sandbox_policy::configured_value("/proc/sys/net/core/somaxconn")
                .unwrap_or_else(|| "4096\n".to_string()),
        ),
        (
            "sys/net/core/rmem_default",
            sandbox_policy::configured_value("/proc/sys/net/core/rmem_default")
                .unwrap_or_else(|| "212992\n".to_string()),
        ),
        (
            "sys/net/core/wmem_default",
            sandbox_policy::configured_value("/proc/sys/net/core/wmem_default")
                .unwrap_or_else(|| "212992\n".to_string()),
        ),
        (
            "sys/net/core/rmem_max",
            sandbox_policy::configured_value("/proc/sys/net/core/rmem_max")
                .unwrap_or_else(|| "212992\n".to_string()),
        ),
        (
            "sys/net/core/wmem_max",
            sandbox_policy::configured_value("/proc/sys/net/core/wmem_max")
                .unwrap_or_else(|| "212992\n".to_string()),
        ),
        (
            "sys/net/core/netdev_max_backlog",
            sandbox_policy::configured_value("/proc/sys/net/core/netdev_max_backlog")
                .unwrap_or_else(|| "1000\n".to_string()),
        ),
        (
            "sys/net/core/optmem_max",
            sandbox_policy::configured_value("/proc/sys/net/core/optmem_max")
                .unwrap_or_else(|| "20480\n".to_string()),
        ),
        (
            "sys/net/ipv4/ip_local_port_range",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/ip_local_port_range")
                .unwrap_or_else(|| "32768\t60999\n".to_string()),
        ),
        (
            "sys/net/ipv4/ip_forward",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/ip_forward")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_syncookies",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_syncookies")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_fin_timeout",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_fin_timeout")
                .unwrap_or_else(|| "60\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_keepalive_time",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_keepalive_time")
                .unwrap_or_else(|| "7200\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_keepalive_intvl",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_keepalive_intvl")
                .unwrap_or_else(|| "75\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_keepalive_probes",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_keepalive_probes")
                .unwrap_or_else(|| "9\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_max_syn_backlog",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_max_syn_backlog")
                .unwrap_or_else(|| "4096\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_retries1",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_retries1")
                .unwrap_or_else(|| "3\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_retries2",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_retries2")
                .unwrap_or_else(|| "15\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_rmem",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_rmem")
                .unwrap_or_else(|| "4096\t131072\t6291456\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_wmem",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_wmem")
                .unwrap_or_else(|| "4096\t16384\t4194304\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_window_scaling",
            sandbox_policy::configured_value("/proc/sys/net/ipv4/tcp_window_scaling")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/net/ipv6/conf/all/disable_ipv6",
            sandbox_policy::configured_value("/proc/sys/net/ipv6/conf/all/disable_ipv6")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/net/ipv6/conf/default/disable_ipv6",
            sandbox_policy::configured_value("/proc/sys/net/ipv6/conf/default/disable_ipv6")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/net/unix/max_dgram_qlen",
            sandbox_policy::configured_value("/proc/sys/net/unix/max_dgram_qlen")
                .unwrap_or_else(|| "512\n".to_string()),
        ),
        (
            "sys/user/max_cgroup_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_cgroup_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_ipc_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_ipc_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_mnt_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_mnt_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_net_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_net_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_pid_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_pid_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_time_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_time_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_user_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_user_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/user/max_uts_namespaces",
            sandbox_policy::configured_value("/proc/sys/user/max_uts_namespaces")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/vm/overcommit_memory",
            sandbox_policy::configured_value("/proc/sys/vm/overcommit_memory")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/vm/overcommit_ratio",
            sandbox_policy::configured_value("/proc/sys/vm/overcommit_ratio")
                .unwrap_or_else(|| "50\n".to_string()),
        ),
        (
            "sys/vm/max_map_count",
            sandbox_policy::configured_value("/proc/sys/vm/max_map_count")
                .unwrap_or_else(|| "1048576\n".to_string()),
        ),
        (
            "sys/vm/mmap_min_addr",
            sandbox_policy::configured_value("/proc/sys/vm/mmap_min_addr")
                .unwrap_or_else(|| "65536\n".to_string()),
        ),
        (
            "sys/vm/drop_caches",
            sandbox_policy::configured_value("/proc/sys/vm/drop_caches")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/vm/min_free_kbytes",
            sandbox_policy::configured_value("/proc/sys/vm/min_free_kbytes")
                .unwrap_or_else(|| "67584\n".to_string()),
        ),
        (
            "sys/vm/stat_interval",
            sandbox_policy::configured_value("/proc/sys/vm/stat_interval")
                .unwrap_or_else(|| "1\n".to_string()),
        ),
        (
            "sys/vm/swappiness",
            sandbox_policy::configured_value("/proc/sys/vm/swappiness")
                .unwrap_or_else(|| "60\n".to_string()),
        ),
        (
            "sys/vm/vfs_cache_pressure",
            sandbox_policy::configured_value("/proc/sys/vm/vfs_cache_pressure")
                .unwrap_or_else(|| "100\n".to_string()),
        ),
        (
            "sys/vm/zone_reclaim_mode",
            sandbox_policy::configured_value("/proc/sys/vm/zone_reclaim_mode")
                .unwrap_or_else(|| "0\n".to_string()),
        ),
        (
            "sys/vm/dirty_ratio",
            sandbox_policy::configured_value("/proc/sys/vm/dirty_ratio")
                .unwrap_or_else(|| "20\n".to_string()),
        ),
        (
            "sys/vm/dirty_background_ratio",
            sandbox_policy::configured_value("/proc/sys/vm/dirty_background_ratio")
                .unwrap_or_else(|| "10\n".to_string()),
        ),
    ]
}

fn proc_self_net_files() -> Vec<(&'static str, String)> {
    vec![
        ("dev", proc_net_dev()),
        ("route", proc_net_route()),
        ("if_inet6", String::new()),
        ("tcp", proc_net_socket_table()),
        ("tcp6", proc_net_socket_table()),
        ("udp", proc_net_socket_table()),
        ("udp6", proc_net_socket_table()),
        ("unix", proc_net_unix()),
    ]
}

fn proc_self_attr_files() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("current", b"unconfined\n"),
        ("exec", b""),
        ("fscreate", b""),
        ("keycreate", b""),
        ("prev", b"unconfined\n"),
    ]
}

fn proc_self_ns_files() -> [&'static str; 8] {
    [
        "cgroup",
        "ipc",
        "mnt",
        "net",
        "pid",
        "pid_for_children",
        "time",
        "uts",
    ]
}

fn proc_cpuinfo_fallback() -> String {
    let cpu_count = sandbox_cpu_count();
    let mut output = String::new();
    for processor in 0..cpu_count {
        output.push_str(&format!(
            "processor\t: {processor}\nvendor_id\t: Sinomo\nmodel name\t: WASIX virtual CPU\n\n"
        ));
    }
    output
}

fn proc_meminfo_fallback() -> String {
    "MemTotal:        1048576 kB\nMemFree:         1048576 kB\nMemAvailable:    1048576 kB\n"
        .to_string()
}

fn proc_stat_fallback() -> String {
    let cpu_count = sandbox_cpu_count();
    let mut output = "cpu  0 0 0 0 0 0 0 0 0 0\n".to_string();
    for processor in 0..cpu_count {
        output.push_str(&format!("cpu{processor} 0 0 0 0 0 0 0 0 0 0\n"));
    }
    output.push_str("intr 0\nctxt 0\nbtime 0\nprocesses 1\nprocs_running 1\nprocs_blocked 0\n");
    output
}

fn proc_filesystems() -> String {
    [
        "nodev\tproc",
        "nodev\tdevtmpfs",
        "nodev\ttmpfs",
        "nodev\tsifs",
    ]
    .join("\n")
        + "\n"
}

fn proc_crypto() -> String {
    [
        "name         : crc32c",
        "driver       : crc32c-sinomo",
        "module       : kernel",
        "priority     : 100",
        "refcnt       : 1",
        "selftest     : passed",
        "type         : shash",
        "blocksize    : 1",
        "digestsize   : 4",
    ]
    .join("\n")
        + "\n\n"
}

fn proc_devices() -> String {
    [
        "Character devices:",
        "  1 mem",
        "  5 /dev/tty",
        " 10 misc",
        "",
        "Block devices:",
        "",
    ]
    .join("\n")
        + "\n"
}

fn proc_partitions() -> String {
    "major minor  #blocks  name\n\n".to_string()
}

fn proc_swaps() -> String {
    "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n".to_string()
}

fn proc_vmstat() -> String {
    [
        "nr_free_pages 0",
        "nr_zone_inactive_anon 0",
        "nr_zone_active_anon 0",
        "nr_zone_inactive_file 0",
        "nr_zone_active_file 0",
        "nr_zone_unevictable 0",
        "nr_mlock 0",
        "nr_anon_pages 0",
        "nr_mapped 0",
        "nr_file_pages 0",
        "nr_dirty 0",
        "nr_writeback 0",
        "pgpgin 0",
        "pgpgout 0",
        "pswpin 0",
        "pswpout 0",
    ]
    .join("\n")
        + "\n"
}

fn proc_zoneinfo() -> String {
    [
        "Node 0, zone      DMA",
        "  pages free     0",
        "        min      0",
        "        low      0",
        "        high     0",
        "        spanned  0",
        "        present  0",
        "        managed  0",
        "  protection: (0)",
    ]
    .join("\n")
        + "\n"
}

fn proc_pressure() -> String {
    "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
        .to_string()
}

fn proc_net_dev() -> String {
    "Inter-|   Receive                                                |  Transmit\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    lo:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0\n"
        .to_string()
}

fn proc_net_route() -> String {
    "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n"
        .to_string()
}

fn proc_net_socket_table() -> String {
    "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
        .to_string()
}

fn proc_net_unix() -> String {
    "Num       RefCount Protocol Flags    Type St Inode Path\n".to_string()
}

fn proc_uuid() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::getrandom(&mut bytes).is_err() {
        return "00000000-0000-4000-8000-000000000000".to_string();
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

fn sys_system_files() -> Vec<(String, String)> {
    let cpu_range = sandbox_cpu_range();
    let mut files = vec![
        (
            "devices/system/cpu/online".to_string(),
            format!("{cpu_range}\n"),
        ),
        (
            "devices/system/cpu/possible".to_string(),
            format!("{cpu_range}\n"),
        ),
        (
            "devices/system/cpu/present".to_string(),
            format!("{cpu_range}\n"),
        ),
        (
            "devices/system/cpu/kernel_max".to_string(),
            format!("{}\n", sandbox_cpu_count().saturating_sub(1)),
        ),
        ("devices/system/node/online".to_string(), "0\n".to_string()),
        (
            "devices/system/node/possible".to_string(),
            "0\n".to_string(),
        ),
        (
            "fs/cgroup/cgroup.controllers".to_string(),
            "cpu io memory pids\n".to_string(),
        ),
        (
            "fs/cgroup/cgroup.subtree_control".to_string(),
            String::new(),
        ),
        ("fs/cgroup/cgroup.procs".to_string(), "1\n".to_string()),
        ("fs/cgroup/cgroup.type".to_string(), "domain\n".to_string()),
        ("fs/cgroup/cpu.max".to_string(), "max 100000\n".to_string()),
        ("fs/cgroup/cpu.weight".to_string(), "100\n".to_string()),
        ("fs/cgroup/memory.current".to_string(), "0\n".to_string()),
        ("fs/cgroup/memory.max".to_string(), "max\n".to_string()),
        ("fs/cgroup/pids.current".to_string(), "1\n".to_string()),
        ("fs/cgroup/pids.max".to_string(), "max\n".to_string()),
        (
            "kernel/mm/transparent_hugepage/enabled".to_string(),
            "always madvise [never]\n".to_string(),
        ),
        (
            "kernel/mm/transparent_hugepage/hpage_pmd_size".to_string(),
            "2097152\n".to_string(),
        ),
        (
            "class/net/lo/address".to_string(),
            "00:00:00:00:00:00\n".to_string(),
        ),
        ("class/net/lo/addr_len".to_string(), "6\n".to_string()),
        ("class/net/lo/flags".to_string(), "0x9\n".to_string()),
        ("class/net/lo/ifindex".to_string(), "1\n".to_string()),
        ("class/net/lo/iflink".to_string(), "1\n".to_string()),
        ("class/net/lo/mtu".to_string(), "65536\n".to_string()),
        (
            "class/net/lo/operstate".to_string(),
            "unknown\n".to_string(),
        ),
        ("class/net/lo/type".to_string(), "772\n".to_string()),
        ("class/tty/console/dev".to_string(), "5:1\n".to_string()),
        ("class/tty/tty/dev".to_string(), "5:0\n".to_string()),
        (
            "devices/virtual/mem/null/dev".to_string(),
            "1:3\n".to_string(),
        ),
        (
            "devices/virtual/mem/zero/dev".to_string(),
            "1:5\n".to_string(),
        ),
        (
            "devices/virtual/mem/full/dev".to_string(),
            "1:7\n".to_string(),
        ),
        (
            "devices/virtual/mem/random/dev".to_string(),
            "1:8\n".to_string(),
        ),
        (
            "devices/virtual/mem/urandom/dev".to_string(),
            "1:9\n".to_string(),
        ),
        (
            "devices/virtual/net/lo/address".to_string(),
            "00:00:00:00:00:00\n".to_string(),
        ),
        (
            "devices/virtual/net/lo/mtu".to_string(),
            "65536\n".to_string(),
        ),
        (
            "devices/virtual/net/lo/operstate".to_string(),
            "unknown\n".to_string(),
        ),
        (
            "devices/virtual/tty/console/dev".to_string(),
            "5:1\n".to_string(),
        ),
        (
            "devices/virtual/tty/tty/dev".to_string(),
            "5:0\n".to_string(),
        ),
    ];
    for processor in 0..sandbox_cpu_count() {
        let cpu = format!("devices/system/cpu/cpu{processor}");
        files.push((format!("{cpu}/online"), "1\n".to_string()));
        files.push((
            format!("{cpu}/topology/physical_package_id"),
            "0\n".to_string(),
        ));
        files.push((format!("{cpu}/topology/core_id"), format!("{processor}\n")));
        files.push((
            format!("{cpu}/topology/thread_siblings_list"),
            format!("{processor}\n"),
        ));
        files.push((
            format!("{cpu}/topology/core_siblings_list"),
            format!("{}\n", sandbox_cpu_range()),
        ));
        files.push((
            format!("{cpu}/cache/index0/coherency_line_size"),
            "64\n".to_string(),
        ));
        files.push((format!("{cpu}/cache/index0/level"), "1\n".to_string()));
        files.push((format!("{cpu}/cache/index0/type"), "Data\n".to_string()));
        files.push((format!("{cpu}/cache/index0/size"), "32K\n".to_string()));
        files.push((
            format!("{cpu}/cache/index0/shared_cpu_list"),
            format!("{processor}\n"),
        ));
        files.push((
            format!("{cpu}/cache/index0/shared_cpu_map"),
            format!("{:x}\n", 1usize << processor.min(usize::BITS as usize - 1)),
        ));
    }
    files
}

fn sandbox_cpu_count() -> usize {
    1
}

fn sandbox_cpu_range() -> String {
    match sandbox_cpu_count() {
        0 | 1 => "0".to_string(),
        count => format!("0-{}", count - 1),
    }
}

fn etc_system_files() -> Vec<(String, Vec<u8>)> {
    let ca_bundle = etc_ca_bundle();
    let mut files = vec![
        ("passwd".to_string(), etc_passwd().into_bytes()),
        ("group".to_string(), etc_group().into_bytes()),
        (
            "nsswitch.conf".to_string(),
            etc_nsswitch_conf().into_bytes(),
        ),
        ("hosts".to_string(), etc_hosts().into_bytes()),
        ("resolv.conf".to_string(), etc_resolv_conf().into_bytes()),
        ("services".to_string(), etc_services().into_bytes()),
        ("protocols".to_string(), etc_protocols().into_bytes()),
        (
            "gai.conf".to_string(),
            b"# Sinomo sandbox resolver defaults\n".to_vec(),
        ),
        ("hostname".to_string(), b"sinomo-sandbox\n".to_vec()),
        ("issue".to_string(), b"Sinomo Sandbox\n".to_vec()),
        ("shells".to_string(), b"/bin/sh\n/bin/bash\n".to_vec()),
        ("profile".to_string(), etc_profile().into_bytes()),
        ("ld.so.conf".to_string(), etc_ld_so_conf().into_bytes()),
        ("os-release".to_string(), etc_os_release().into_bytes()),
        ("locale.conf".to_string(), b"LANG=C.UTF-8\n".to_vec()),
        ("default/locale".to_string(), b"LANG=C.UTF-8\n".to_vec()),
        ("termcap".to_string(), etc_termcap().into_bytes()),
        (
            "machine-id".to_string(),
            format!("{}\n", proc_uuid().replace('-', "")).into_bytes(),
        ),
        ("timezone".to_string(), b"Etc/UTC\n".to_vec()),
        ("localtime".to_string(), etc_localtime()),
    ];
    if !ca_bundle.is_empty() {
        for path in [
            "ssl/certs/ca-certificates.crt",
            "ssl/cert.pem",
            "pki/tls/certs/ca-bundle.crt",
        ] {
            files.push((path.to_string(), ca_bundle.clone()));
        }
    }
    files
}

fn etc_passwd() -> String {
    [
        "root:x:0:0:root:/root:/bin/sh",
        "sinomo:x:1000:1000:Sinomo Sandbox:/home/sinomo:/bin/sh",
    ]
    .join("\n")
        + "\n"
}

fn etc_group() -> String {
    ["root:x:0:", "sinomo:x:1000:sinomo"].join("\n") + "\n"
}

fn etc_nsswitch_conf() -> String {
    ["passwd: files", "group: files", "hosts: files dns"].join("\n") + "\n"
}

fn etc_hosts() -> String {
    [
        "127.0.0.1 localhost",
        "::1 localhost ip6-localhost ip6-loopback",
        "127.0.1.1 sinomo-sandbox",
    ]
    .join("\n")
        + "\n"
}

fn etc_resolv_conf() -> String {
    "options edns0 trust-ad\n".to_string()
}

fn etc_services() -> String {
    [
        "http\t\t80/tcp",
        "https\t\t443/tcp",
        "domain\t\t53/tcp",
        "domain\t\t53/udp",
        "ssh\t\t22/tcp",
        "smtp\t\t25/tcp",
        "ntp\t\t123/udp",
    ]
    .join("\n")
        + "\n"
}

fn etc_protocols() -> String {
    [
        "ip\t0\tIP",
        "icmp\t1\tICMP",
        "tcp\t6\tTCP",
        "udp\t17\tUDP",
        "ipv6\t41\tIPv6",
        "icmpv6\t58\tIPv6-ICMP",
    ]
    .join("\n")
        + "\n"
}

fn etc_termcap() -> String {
    [
        "dumb|80-column dumb tty:\\",
        "\t:am:\\",
        "\t:co#80:\\",
        "\t:li#24:",
    ]
    .join("\n")
        + "\n"
}

fn etc_profile() -> String {
    [
        "export PATH=${PATH:-/bin:/usr/bin}",
        "export HOME=${HOME:-/home/sinomo}",
        "export USER=${USER:-sinomo}",
        "export LOGNAME=${LOGNAME:-sinomo}",
        "export SHELL=${SHELL:-/bin/sh}",
        "export TMPDIR=${TMPDIR:-/tmp}",
        "export LANG=${LANG:-C.UTF-8}",
        "export LC_CTYPE=${LC_CTYPE:-C.UTF-8}",
        "export TZ=${TZ:-UTC}",
        "export TERM=${TERM:-dumb}",
    ]
    .join("\n")
        + "\n"
}

fn etc_ld_so_conf() -> String {
    ["/lib", "/usr/lib", "/usr/local/lib"].join("\n") + "\n"
}

fn etc_os_release() -> String {
    [
        "NAME=\"Sinomo Sandbox\"",
        "ID=sinomo-sandbox",
        "PRETTY_NAME=\"Sinomo Sandbox WASIX\"",
    ]
    .join("\n")
        + "\n"
}

fn etc_ca_bundle() -> Vec<u8> {
    Vec::new()
}

fn etc_localtime() -> Vec<u8> {
    b"UTC\n".to_vec()
}

fn usr_system_files() -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    let utc = etc_localtime();
    files.push(("share/zoneinfo/UTC".to_string(), utc.clone()));
    files.push(("share/zoneinfo/Etc/UTC".to_string(), utc));

    files
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_device_proc_files_are_policy_generated() {
        let files = proc_system_files()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(files.get("cpuinfo").unwrap(), &proc_cpuinfo_fallback());
        assert_eq!(files.get("meminfo").unwrap(), &proc_meminfo_fallback());
        assert_eq!(
            files.get("sys/kernel/hostname").unwrap(),
            "sinomo-sandbox\n"
        );
        assert_eq!(files.get("sys/net/ipv4/ip_forward").unwrap(), "0\n");
    }

    #[test]
    fn virtual_device_etc_and_usr_do_not_copy_host_files() {
        let etc = etc_system_files()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let usr = usr_system_files()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(etc.get("resolv.conf").unwrap(), b"options edns0 trust-ad\n");
        assert!(!etc.contains_key("ssl/certs/ca-certificates.crt"));
        assert!(!etc.contains_key("ssl/cert.pem"));
        assert!(!etc.contains_key("pki/tls/certs/ca-bundle.crt"));
        assert_eq!(etc.get("localtime").unwrap(), b"UTC\n");
        assert!(
            std::str::from_utf8(etc.get("os-release").unwrap())
                .unwrap()
                .contains("Sinomo Sandbox")
        );
        assert_eq!(usr.get("share/zoneinfo/UTC").unwrap(), b"UTC\n");
        assert!(!usr.keys().any(|path| path.starts_with("share/terminfo/")));
    }

    #[test]
    fn virtual_device_sys_files_are_policy_generated() {
        let files = sys_system_files()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(files.get("devices/system/cpu/online").unwrap(), "0\n");
        assert_eq!(files.get("devices/system/cpu/kernel_max").unwrap(), "0\n");
        assert_eq!(files.get("fs/cgroup/memory.max").unwrap(), "max\n");
        assert_eq!(
            files.get("kernel/mm/transparent_hugepage/enabled").unwrap(),
            "always madvise [never]\n"
        );
    }

    #[test]
    fn wasix_virtual_system_mounts_are_built_without_host_staging() {
        let devices = WasiVirtualSystemMounts::create(
            64,
            &["test.wasm".to_string()],
            &[],
            &[(
                SandboxPath::new("/work").unwrap(),
                SandboxMountMode::ReadWrite,
            )],
        )
        .expect("devices");

        assert!(devices.proc_fs().metadata(Path::new("/cpuinfo")).is_ok());
        assert!(devices.etc_fs().metadata(Path::new("/os-release")).is_ok());
        assert!(
            devices
                .sys_fs()
                .metadata(Path::new("/devices/system/cpu/online"))
                .is_ok()
        );
        assert!(devices.fs().metadata(Path::new("/zero")).is_ok());
    }
}

pub(crate) fn validate_wasi_request(
    request: &SandboxCommandRequest,
) -> Result<(), SandboxWasiError> {
    if request.argv.is_empty() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "argv must include at least argv[0]".to_string(),
        });
    }
    if request.argv[0].is_empty() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "argv[0] must be non-empty".to_string(),
        });
    }
    for (index, arg) in request.argv.iter().enumerate() {
        if arg.contains('\0') {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!("argv[{index}] contains an embedded NUL byte"),
            });
        }
    }
    if request.timeout_ms == 0 {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "timeout_ms must be non-zero".to_string(),
        });
    }
    for (name, value) in &request.env {
        if !sandbox_env_name_is_valid(name) {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!("invalid sandbox env name `{name}`"),
            });
        }
        if value.contains('\0') {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!("environment variable `{name}` contains an embedded NUL byte"),
            });
        }
    }
    Ok(())
}
