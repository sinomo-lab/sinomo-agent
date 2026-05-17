use crate::internal::*;

use super::SandboxWasiError;

#[derive(Clone)]
pub(crate) struct LocalWasiVirtualDeviceDir {
    root: PathBuf,
    proc_root: PathBuf,
    etc_root: PathBuf,
    sys_root: PathBuf,
    run_root: PathBuf,
    var_root: PathBuf,
    tmp_root: PathBuf,
    home_root: PathBuf,
    root_home: PathBuf,
    usr_root: PathBuf,
    opt_root: PathBuf,
}

impl LocalWasiVirtualDeviceDir {
    pub(crate) fn create(
        zero_device_bytes: usize,
        argv: &[String],
        env: &[(String, String)],
        mounts: &[(SandboxPath, SandboxMountMode)],
    ) -> Result<Self, SandboxWasiError> {
        let zero_device_bytes = zero_device_bytes.min(SANDBOX_WASI_ZERO_DEVICE_BYTES);
        let root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-dev-{}",
            SandboxCommandId::generate()
        ));
        let proc_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-proc-{}",
            SandboxCommandId::generate()
        ));
        let etc_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-etc-{}",
            SandboxCommandId::generate()
        ));
        let sys_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-sys-{}",
            SandboxCommandId::generate()
        ));
        let run_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-run-{}",
            SandboxCommandId::generate()
        ));
        let var_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-var-{}",
            SandboxCommandId::generate()
        ));
        let tmp_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-tmp-{}",
            SandboxCommandId::generate()
        ));
        let home_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-home-{}",
            SandboxCommandId::generate()
        ));
        let root_home = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-root-home-{}",
            SandboxCommandId::generate()
        ));
        let usr_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-usr-{}",
            SandboxCommandId::generate()
        ));
        let opt_root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-opt-{}",
            SandboxCommandId::generate()
        ));
        fs::create_dir_all(&root).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox virtual device directory: {error}"),
        })?;
        fs::create_dir_all(&tmp_root).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox virtual tmp directory: {error}"),
        })?;
        fs::create_dir_all(&etc_root).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox virtual etc directory: {error}"),
        })?;
        for etc_dir in [
            etc_root.join("ssl").join("certs"),
            etc_root.join("pki").join("tls").join("certs"),
            etc_root.join("default"),
        ] {
            fs::create_dir_all(&etc_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual etc directory: {error}"),
            })?;
        }
        let mut sys_dirs = vec![
            sys_root.join("block"),
            sys_root.join("class").join("block"),
            sys_root.join("class").join("mem"),
            sys_root.join("class").join("net").join("lo"),
            sys_root.join("class").join("tty").join("console"),
            sys_root.join("class").join("tty").join("tty"),
            sys_root.join("devices").join("system").join("cpu"),
            sys_root.join("devices").join("system").join("node"),
            sys_root.join("fs").join("cgroup"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("mem")
                .join("null"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("mem")
                .join("zero"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("mem")
                .join("full"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("mem")
                .join("random"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("mem")
                .join("urandom"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("net")
                .join("lo"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("tty")
                .join("console"),
            sys_root
                .join("devices")
                .join("virtual")
                .join("tty")
                .join("tty"),
            sys_root
                .join("kernel")
                .join("mm")
                .join("transparent_hugepage"),
        ];
        for processor in 0..sandbox_cpu_count() {
            sys_dirs.push(
                sys_root
                    .join("devices")
                    .join("system")
                    .join("cpu")
                    .join(format!("cpu{processor}"))
                    .join("topology"),
            );
            sys_dirs.push(
                sys_root
                    .join("devices")
                    .join("system")
                    .join("cpu")
                    .join(format!("cpu{processor}"))
                    .join("cache"),
            );
            sys_dirs.push(
                sys_root
                    .join("devices")
                    .join("system")
                    .join("cpu")
                    .join(format!("cpu{processor}"))
                    .join("cache")
                    .join("index0"),
            );
        }
        for sys_dir in sys_dirs {
            fs::create_dir_all(&sys_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual sys directory: {error}"),
            })?;
        }
        for run_dir in [run_root.join("user").join("1000"), run_root.join("lock")] {
            fs::create_dir_all(run_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual run directory: {error}"),
            })?;
        }
        for var_dir in [
            var_root.join("tmp"),
            var_root.join("log"),
            var_root.join("cache"),
            var_root.join("lib"),
        ] {
            fs::create_dir_all(var_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual var directory: {error}"),
            })?;
        }
        for home_dir in [
            home_root.join("sinomo"),
            home_root.join("sinomo").join(".cache"),
            home_root.join("sinomo").join(".config"),
            home_root.join("sinomo").join(".local").join("share"),
            root_home.join(".cache"),
            root_home.join(".config"),
            root_home.join(".local").join("share"),
        ] {
            fs::create_dir_all(home_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual home directory: {error}"),
            })?;
        }
        for usr_dir in [
            usr_root.join("bin"),
            usr_root.join("lib"),
            usr_root.join("local").join("bin"),
            usr_root.join("local").join("lib"),
            usr_root.join("share"),
            usr_root.join("share").join("terminfo").join("d"),
            usr_root.join("share").join("terminfo").join("x"),
            usr_root.join("share").join("zoneinfo").join("Etc"),
            opt_root.clone(),
        ] {
            fs::create_dir_all(usr_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual usr directory: {error}"),
            })?;
        }
        fs::create_dir_all(proc_root.join("self").join("fd")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc fd directory: {error}"),
            }
        })?;
        fs::create_dir_all(proc_root.join("self").join("fdinfo")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc fdinfo directory: {error}"),
            }
        })?;
        fs::create_dir_all(proc_root.join("self").join("net")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc self net directory: {error}"),
            }
        })?;
        fs::create_dir_all(proc_root.join("self").join("attr")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc self attr directory: {error}"),
            }
        })?;
        fs::create_dir_all(proc_root.join("self").join("ns")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc self ns directory: {error}"),
            }
        })?;
        fs::create_dir_all(proc_root.join("self").join("task")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc task directory: {error}"),
            }
        })?;
        for task_dir in [
            proc_root.join("1").join("fd"),
            proc_root.join("1").join("fdinfo"),
            proc_root.join("1").join("net"),
            proc_root.join("1").join("attr"),
            proc_root.join("1").join("ns"),
            proc_root.join("1").join("task").join("1").join("fd"),
            proc_root.join("1").join("task").join("1").join("fdinfo"),
            proc_root.join("1").join("task").join("1").join("net"),
            proc_root.join("1").join("task").join("1").join("attr"),
            proc_root.join("1").join("task").join("1").join("ns"),
            proc_root.join("self").join("task").join("1").join("fd"),
            proc_root.join("self").join("task").join("1").join("fdinfo"),
            proc_root.join("self").join("task").join("1").join("net"),
            proc_root.join("self").join("task").join("1").join("attr"),
            proc_root.join("self").join("task").join("1").join("ns"),
        ] {
            fs::create_dir_all(task_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc task view directory: {error}"),
            })?;
        }
        fs::create_dir_all(proc_root.join("thread-self")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc thread-self directory: {error}"),
            }
        })?;
        for proc_sys_dir in [
            proc_root.join("net"),
            proc_root.join("pressure"),
            proc_root.join("sys").join("crypto"),
            proc_root.join("sys").join("fs").join("epoll"),
            proc_root.join("sys").join("fs").join("inotify"),
            proc_root.join("sys").join("fs").join("mqueue"),
            proc_root.join("sys").join("kernel").join("keys"),
            proc_root.join("sys").join("kernel").join("random"),
            proc_root.join("sys").join("kernel").join("yama"),
            proc_root.join("sys").join("net").join("core"),
            proc_root.join("sys").join("net").join("ipv4"),
            proc_root
                .join("sys")
                .join("net")
                .join("ipv6")
                .join("conf")
                .join("all"),
            proc_root
                .join("sys")
                .join("net")
                .join("ipv6")
                .join("conf")
                .join("default"),
            proc_root.join("sys").join("net").join("unix"),
            proc_root.join("sys").join("user"),
            proc_root.join("sys").join("vm"),
        ] {
            fs::create_dir_all(proc_sys_dir).map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox virtual proc sys directory: {error}"),
            })?;
        }
        let devices = Self {
            root,
            proc_root,
            etc_root,
            sys_root,
            run_root,
            var_root,
            tmp_root,
            home_root,
            root_home,
            usr_root,
            opt_root,
        };
        fs::write(devices.root.join("null"), []).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox /dev/null: {error}"),
        })?;
        let zero = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(devices.root.join("zero"))
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /dev/zero: {error}"),
            })?;
        zero.set_len(zero_device_bytes as u64)
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("size sandbox /dev/zero: {error}"),
            })?;
        let mut random = vec![0; 64 * 1024];
        getrandom::getrandom(&mut random).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("seed sandbox random devices: {error}"),
        })?;
        fs::write(devices.root.join("random"), &random).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /dev/random: {error}"),
            }
        })?;
        fs::write(devices.root.join("urandom"), &random).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /dev/urandom: {error}"),
            }
        })?;
        let full = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(devices.root.join("full"))
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /dev/full: {error}"),
            })?;
        full.set_len(zero_device_bytes as u64)
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("size sandbox /dev/full: {error}"),
            })?;
        for device in ["stdin", "stdout", "stderr", "tty", "console", "ptmx"] {
            fs::write(devices.root.join(device), []).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /dev/{device}: {error}"),
                }
            })?;
        }
        fs::create_dir_all(devices.root.join("fd")).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox /dev/fd: {error}"),
        })?;
        fs::create_dir_all(devices.root.join("pts")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /dev/pts: {error}"),
            }
        })?;
        for fd in ["0", "1", "2"] {
            fs::write(devices.root.join("fd").join(fd), []).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /dev/fd/{fd}: {error}"),
                }
            })?;
        }
        fs::create_dir_all(devices.root.join("shm")).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /dev/shm: {error}"),
            }
        })?;
        for fd in ["0", "1", "2"] {
            fs::write(devices.proc_root.join("self").join("fd").join(fd), []).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /proc/self/fd/{fd}: {error}"),
                }
            })?;
            fs::write(
                devices.proc_root.join("self").join("fdinfo").join(fd),
                proc_self_fdinfo(fd),
            )
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/fdinfo/{fd}: {error}"),
            })?;
        }
        fs::write(devices.proc_root.join("self").join("cwd"), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/cwd: {error}"),
            }
        })?;
        fs::write(devices.proc_root.join("self").join("root"), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/root: {error}"),
            }
        })?;
        fs::write(devices.proc_root.join("self").join("exe"), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/exe: {error}"),
            }
        })?;
        for (proc_file, content) in proc_self_files(argv) {
            fs::write(devices.proc_root.join("self").join(&proc_file), content).map_err(
                |error| SandboxWasiError::Runtime {
                    detail: format!("create sandbox /proc/self/{proc_file}: {error}"),
                },
            )?;
        }
        let mut cmdline = Vec::new();
        for arg in argv {
            cmdline.extend_from_slice(arg.as_bytes());
            cmdline.push(0);
        }
        fs::write(devices.proc_root.join("self").join("cmdline"), cmdline).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/cmdline: {error}"),
            }
        })?;
        let mut environ = Vec::new();
        for (key, value) in env {
            environ.extend_from_slice(key.as_bytes());
            environ.push(b'=');
            environ.extend_from_slice(value.as_bytes());
            environ.push(0);
        }
        fs::write(devices.proc_root.join("self").join("environ"), environ).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/environ: {error}"),
            }
        })?;
        let mounts = proc_mounts(mounts);
        fs::write(devices.proc_root.join("mounts"), &mounts).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/mounts: {error}"),
            }
        })?;
        fs::write(devices.proc_root.join("self").join("mounts"), &mounts).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/mounts: {error}"),
            }
        })?;
        fs::write(devices.etc_root.join("mtab"), &mounts).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /etc/mtab: {error}"),
            }
        })?;
        let mountinfo = proc_mountinfo(&mounts);
        fs::write(devices.proc_root.join("mountinfo"), &mountinfo).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/mountinfo: {error}"),
            }
        })?;
        fs::write(devices.proc_root.join("self").join("mountinfo"), &mountinfo).map_err(
            |error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/mountinfo: {error}"),
            },
        )?;
        for (proc_file, content) in proc_system_files() {
            fs::write(devices.proc_root.join(proc_file), content).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /proc/{proc_file}: {error}"),
                }
            })?;
        }
        for (proc_file, content) in proc_self_net_files() {
            fs::write(
                devices.proc_root.join("self").join("net").join(proc_file),
                content,
            )
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/net/{proc_file}: {error}"),
            })?;
        }
        for (proc_file, content) in proc_self_attr_files() {
            fs::write(
                devices.proc_root.join("self").join("attr").join(proc_file),
                content,
            )
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/attr/{proc_file}: {error}"),
            })?;
        }
        for proc_file in proc_self_ns_files() {
            fs::write(
                devices.proc_root.join("self").join("ns").join(proc_file),
                [],
            )
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/ns/{proc_file}: {error}"),
            })?;
        }
        write_proc_pid_view(&devices.proc_root, "1", argv, env, &mounts, &mountinfo)?;
        write_proc_task_view(&devices.proc_root, argv, env, &mounts, &mountinfo)?;
        for (etc_file, content) in etc_system_files() {
            fs::write(devices.etc_root.join(&etc_file), content).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /etc/{etc_file}: {error}"),
                }
            })?;
        }
        for (sys_file, content) in sys_system_files() {
            fs::write(devices.sys_root.join(&sys_file), content).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /sys/{sys_file}: {error}"),
                }
            })?;
        }
        for (usr_file, content) in usr_system_files() {
            fs::write(devices.usr_root.join(&usr_file), content).map_err(|error| {
                SandboxWasiError::Runtime {
                    detail: format!("create sandbox /usr/{usr_file}: {error}"),
                }
            })?;
        }
        Ok(devices)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.root
    }

    pub(crate) fn proc_path(&self) -> &Path {
        &self.proc_root
    }

    pub(crate) fn etc_path(&self) -> &Path {
        &self.etc_root
    }

    pub(crate) fn sys_path(&self) -> &Path {
        &self.sys_root
    }

    pub(crate) fn run_path(&self) -> &Path {
        &self.run_root
    }

    pub(crate) fn var_path(&self) -> &Path {
        &self.var_root
    }

    pub(crate) fn tmp_path(&self) -> &Path {
        &self.tmp_root
    }

    pub(crate) fn home_path(&self) -> &Path {
        &self.home_root
    }

    pub(crate) fn root_home_path(&self) -> &Path {
        &self.root_home
    }

    pub(crate) fn usr_path(&self) -> &Path {
        &self.usr_root
    }

    pub(crate) fn opt_path(&self) -> &Path {
        &self.opt_root
    }
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

fn write_proc_task_view(
    proc_root: &Path,
    argv: &[String],
    env: &[(String, String)],
    mounts: &str,
    mountinfo: &str,
) -> Result<(), SandboxWasiError> {
    let task_root = proc_root.join("self").join("task").join("1");
    for fd in ["0", "1", "2"] {
        fs::write(task_root.join("fd").join(fd), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/task/1/fd/{fd}: {error}"),
            }
        })?;
        fs::write(task_root.join("fdinfo").join(fd), proc_self_fdinfo(fd)).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/task/1/fdinfo/{fd}: {error}"),
            }
        })?;
    }
    for link in ["cwd", "root", "exe"] {
        fs::write(task_root.join(link), []).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/self/task/1/{link}: {error}"),
        })?;
    }
    fs::write(task_root.join("children"), []).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/self/task/1/children: {error}"),
    })?;
    for (proc_file, content) in proc_self_files(argv) {
        fs::write(task_root.join(&proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/task/1/{proc_file}: {error}"),
            }
        })?;
    }

    let mut cmdline = Vec::new();
    for arg in argv {
        cmdline.extend_from_slice(arg.as_bytes());
        cmdline.push(0);
    }
    fs::write(task_root.join("cmdline"), cmdline).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/self/task/1/cmdline: {error}"),
    })?;
    let mut environ = Vec::new();
    for (key, value) in env {
        environ.extend_from_slice(key.as_bytes());
        environ.push(b'=');
        environ.extend_from_slice(value.as_bytes());
        environ.push(0);
    }
    fs::write(task_root.join("environ"), environ).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/self/task/1/environ: {error}"),
    })?;
    fs::write(task_root.join("mounts"), mounts).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/self/task/1/mounts: {error}"),
    })?;
    fs::write(task_root.join("mountinfo"), mountinfo).map_err(|error| {
        SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/self/task/1/mountinfo: {error}"),
        }
    })?;
    for (proc_file, content) in proc_self_net_files() {
        fs::write(task_root.join("net").join(proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/task/1/net/{proc_file}: {error}"),
            }
        })?;
    }
    for (proc_file, content) in proc_self_attr_files() {
        fs::write(task_root.join("attr").join(proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/task/1/attr/{proc_file}: {error}"),
            }
        })?;
    }
    for proc_file in proc_self_ns_files() {
        fs::write(task_root.join("ns").join(proc_file), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/self/task/1/ns/{proc_file}: {error}"),
            }
        })?;
    }
    Ok(())
}

fn write_proc_pid_view(
    proc_root: &Path,
    pid: &str,
    argv: &[String],
    env: &[(String, String)],
    mounts: &str,
    mountinfo: &str,
) -> Result<(), SandboxWasiError> {
    let pid_root = proc_root.join(pid);
    for fd in ["0", "1", "2"] {
        fs::write(pid_root.join("fd").join(fd), []).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/{pid}/fd/{fd}: {error}"),
        })?;
        fs::write(pid_root.join("fdinfo").join(fd), proc_self_fdinfo(fd)).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/fdinfo/{fd}: {error}"),
            }
        })?;
    }
    for link in ["cwd", "root", "exe"] {
        fs::write(pid_root.join(link), []).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/{pid}/{link}: {error}"),
        })?;
    }
    for (proc_file, content) in proc_self_files(argv) {
        fs::write(pid_root.join(&proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/{proc_file}: {error}"),
            }
        })?;
    }

    let mut cmdline = Vec::new();
    for arg in argv {
        cmdline.extend_from_slice(arg.as_bytes());
        cmdline.push(0);
    }
    fs::write(pid_root.join("cmdline"), cmdline).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/cmdline: {error}"),
    })?;
    let mut environ = Vec::new();
    for (key, value) in env {
        environ.extend_from_slice(key.as_bytes());
        environ.push(b'=');
        environ.extend_from_slice(value.as_bytes());
        environ.push(0);
    }
    fs::write(pid_root.join("environ"), environ).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/environ: {error}"),
    })?;
    fs::write(pid_root.join("mounts"), mounts).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/mounts: {error}"),
    })?;
    fs::write(pid_root.join("mountinfo"), mountinfo).map_err(|error| {
        SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/{pid}/mountinfo: {error}"),
        }
    })?;
    for (proc_file, content) in proc_self_net_files() {
        fs::write(pid_root.join("net").join(proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/net/{proc_file}: {error}"),
            }
        })?;
    }
    for (proc_file, content) in proc_self_attr_files() {
        fs::write(pid_root.join("attr").join(proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/attr/{proc_file}: {error}"),
            }
        })?;
    }
    for proc_file in proc_self_ns_files() {
        fs::write(pid_root.join("ns").join(proc_file), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/ns/{proc_file}: {error}"),
            }
        })?;
    }
    write_proc_pid_task_view(proc_root, pid, argv, env, mounts, mountinfo)
}

fn write_proc_pid_task_view(
    proc_root: &Path,
    pid: &str,
    argv: &[String],
    env: &[(String, String)],
    mounts: &str,
    mountinfo: &str,
) -> Result<(), SandboxWasiError> {
    let task_root = proc_root.join(pid).join("task").join("1");
    for fd in ["0", "1", "2"] {
        fs::write(task_root.join("fd").join(fd), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/task/1/fd/{fd}: {error}"),
            }
        })?;
        fs::write(task_root.join("fdinfo").join(fd), proc_self_fdinfo(fd)).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/task/1/fdinfo/{fd}: {error}"),
            }
        })?;
    }
    for link in ["cwd", "root", "exe"] {
        fs::write(task_root.join(link), []).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/{pid}/task/1/{link}: {error}"),
        })?;
    }
    fs::write(task_root.join("children"), []).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/task/1/children: {error}"),
    })?;
    for (proc_file, content) in proc_self_files(argv) {
        fs::write(task_root.join(&proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/task/1/{proc_file}: {error}"),
            }
        })?;
    }

    let mut cmdline = Vec::new();
    for arg in argv {
        cmdline.extend_from_slice(arg.as_bytes());
        cmdline.push(0);
    }
    fs::write(task_root.join("cmdline"), cmdline).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/task/1/cmdline: {error}"),
    })?;
    let mut environ = Vec::new();
    for (key, value) in env {
        environ.extend_from_slice(key.as_bytes());
        environ.push(b'=');
        environ.extend_from_slice(value.as_bytes());
        environ.push(0);
    }
    fs::write(task_root.join("environ"), environ).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/task/1/environ: {error}"),
    })?;
    fs::write(task_root.join("mounts"), mounts).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("create sandbox /proc/{pid}/task/1/mounts: {error}"),
    })?;
    fs::write(task_root.join("mountinfo"), mountinfo).map_err(|error| {
        SandboxWasiError::Runtime {
            detail: format!("create sandbox /proc/{pid}/task/1/mountinfo: {error}"),
        }
    })?;
    for (proc_file, content) in proc_self_net_files() {
        fs::write(task_root.join("net").join(proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/task/1/net/{proc_file}: {error}"),
            }
        })?;
    }
    for (proc_file, content) in proc_self_attr_files() {
        fs::write(task_root.join("attr").join(proc_file), content).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/task/1/attr/{proc_file}: {error}"),
            }
        })?;
    }
    for proc_file in proc_self_ns_files() {
        fs::write(task_root.join("ns").join(proc_file), []).map_err(|error| {
            SandboxWasiError::Runtime {
                detail: format!("create sandbox /proc/{pid}/task/1/ns/{proc_file}: {error}"),
            }
        })?;
    }
    Ok(())
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
    vec![
        (
            "cpuinfo",
            fs::read_to_string("/proc/cpuinfo").unwrap_or_else(|_| proc_cpuinfo_fallback()),
        ),
        (
            "meminfo",
            fs::read_to_string("/proc/meminfo").unwrap_or_else(|_| proc_meminfo_fallback()),
        ),
        (
            "stat",
            fs::read_to_string("/proc/stat").unwrap_or_else(|_| proc_stat_fallback()),
        ),
        (
            "uptime",
            fs::read_to_string("/proc/uptime").unwrap_or_else(|_| "0.00 0.00\n".to_string()),
        ),
        (
            "loadavg",
            fs::read_to_string("/proc/loadavg")
                .unwrap_or_else(|_| "0.00 0.00 0.00 1/1 1\n".to_string()),
        ),
        (
            "version",
            fs::read_to_string("/proc/version")
                .unwrap_or_else(|_| "Sinomo sandbox WASIX\n".to_string()),
        ),
        (
            "buddyinfo",
            fs::read_to_string("/proc/buddyinfo").unwrap_or_else(|_| String::new()),
        ),
        (
            "crypto",
            fs::read_to_string("/proc/crypto").unwrap_or_else(|_| proc_crypto()),
        ),
        (
            "devices",
            fs::read_to_string("/proc/devices").unwrap_or_else(|_| proc_devices()),
        ),
        (
            "diskstats",
            fs::read_to_string("/proc/diskstats").unwrap_or_else(|_| String::new()),
        ),
        ("filesystems", proc_filesystems()),
        (
            "locks",
            fs::read_to_string("/proc/locks").unwrap_or_else(|_| String::new()),
        ),
        (
            "modules",
            fs::read_to_string("/proc/modules").unwrap_or_else(|_| String::new()),
        ),
        (
            "partitions",
            fs::read_to_string("/proc/partitions").unwrap_or_else(|_| proc_partitions()),
        ),
        (
            "swaps",
            fs::read_to_string("/proc/swaps").unwrap_or_else(|_| proc_swaps()),
        ),
        (
            "vmstat",
            fs::read_to_string("/proc/vmstat").unwrap_or_else(|_| proc_vmstat()),
        ),
        (
            "zoneinfo",
            fs::read_to_string("/proc/zoneinfo").unwrap_or_else(|_| proc_zoneinfo()),
        ),
        (
            "pressure/cpu",
            fs::read_to_string("/proc/pressure/cpu").unwrap_or_else(|_| proc_pressure()),
        ),
        (
            "pressure/io",
            fs::read_to_string("/proc/pressure/io").unwrap_or_else(|_| proc_pressure()),
        ),
        (
            "pressure/memory",
            fs::read_to_string("/proc/pressure/memory").unwrap_or_else(|_| proc_pressure()),
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
            fs::read_to_string("/proc/sys/kernel/ostype").unwrap_or_else(|_| "Linux\n".to_string()),
        ),
        (
            "sys/kernel/osrelease",
            fs::read_to_string("/proc/sys/kernel/osrelease")
                .unwrap_or_else(|_| "6.0.0-sinomo-sandbox\n".to_string()),
        ),
        (
            "sys/kernel/version",
            fs::read_to_string("/proc/sys/kernel/version")
                .unwrap_or_else(|_| "Sinomo sandbox WASIX\n".to_string()),
        ),
        (
            "sys/kernel/hostname",
            fs::read_to_string("/proc/sys/kernel/hostname")
                .unwrap_or_else(|_| "sinomo-sandbox\n".to_string()),
        ),
        (
            "sys/kernel/domainname",
            fs::read_to_string("/proc/sys/kernel/domainname")
                .unwrap_or_else(|_| "(none)\n".to_string()),
        ),
        (
            "sys/kernel/pid_max",
            fs::read_to_string("/proc/sys/kernel/pid_max")
                .unwrap_or_else(|_| "4194304\n".to_string()),
        ),
        (
            "sys/kernel/threads-max",
            fs::read_to_string("/proc/sys/kernel/threads-max")
                .unwrap_or_else(|_| "4194304\n".to_string()),
        ),
        (
            "sys/kernel/ngroups_max",
            fs::read_to_string("/proc/sys/kernel/ngroups_max")
                .unwrap_or_else(|_| "65536\n".to_string()),
        ),
        (
            "sys/kernel/cap_last_cap",
            fs::read_to_string("/proc/sys/kernel/cap_last_cap")
                .unwrap_or_else(|_| "40\n".to_string()),
        ),
        (
            "sys/kernel/shmmax",
            fs::read_to_string("/proc/sys/kernel/shmmax")
                .unwrap_or_else(|_| "18446744073692774399\n".to_string()),
        ),
        (
            "sys/kernel/shmall",
            fs::read_to_string("/proc/sys/kernel/shmall")
                .unwrap_or_else(|_| "18446744073692774399\n".to_string()),
        ),
        (
            "sys/kernel/shmmni",
            fs::read_to_string("/proc/sys/kernel/shmmni").unwrap_or_else(|_| "4096\n".to_string()),
        ),
        (
            "sys/kernel/msgmax",
            fs::read_to_string("/proc/sys/kernel/msgmax").unwrap_or_else(|_| "8192\n".to_string()),
        ),
        (
            "sys/kernel/msgmnb",
            fs::read_to_string("/proc/sys/kernel/msgmnb").unwrap_or_else(|_| "16384\n".to_string()),
        ),
        (
            "sys/kernel/msgmni",
            fs::read_to_string("/proc/sys/kernel/msgmni").unwrap_or_else(|_| "32000\n".to_string()),
        ),
        (
            "sys/kernel/sem",
            fs::read_to_string("/proc/sys/kernel/sem")
                .unwrap_or_else(|_| "32000\t1024000000\t500\t32000\n".to_string()),
        ),
        (
            "sys/kernel/core_pattern",
            fs::read_to_string("/proc/sys/kernel/core_pattern")
                .unwrap_or_else(|_| "core\n".to_string()),
        ),
        (
            "sys/kernel/core_uses_pid",
            fs::read_to_string("/proc/sys/kernel/core_uses_pid")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/hotplug",
            fs::read_to_string("/proc/sys/kernel/hotplug").unwrap_or_else(|_| "\n".to_string()),
        ),
        (
            "sys/kernel/kptr_restrict",
            fs::read_to_string("/proc/sys/kernel/kptr_restrict")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/kernel/keys/gc_delay",
            fs::read_to_string("/proc/sys/kernel/keys/gc_delay")
                .unwrap_or_else(|_| "300\n".to_string()),
        ),
        (
            "sys/kernel/keys/maxbytes",
            fs::read_to_string("/proc/sys/kernel/keys/maxbytes")
                .unwrap_or_else(|_| "20000\n".to_string()),
        ),
        (
            "sys/kernel/keys/maxkeys",
            fs::read_to_string("/proc/sys/kernel/keys/maxkeys")
                .unwrap_or_else(|_| "200\n".to_string()),
        ),
        (
            "sys/kernel/keys/root_maxbytes",
            fs::read_to_string("/proc/sys/kernel/keys/root_maxbytes")
                .unwrap_or_else(|_| "25000000\n".to_string()),
        ),
        (
            "sys/kernel/keys/root_maxkeys",
            fs::read_to_string("/proc/sys/kernel/keys/root_maxkeys")
                .unwrap_or_else(|_| "1000000\n".to_string()),
        ),
        (
            "sys/kernel/kstack_depth_to_print",
            fs::read_to_string("/proc/sys/kernel/kstack_depth_to_print")
                .unwrap_or_else(|_| "24\n".to_string()),
        ),
        (
            "sys/kernel/modules_disabled",
            fs::read_to_string("/proc/sys/kernel/modules_disabled")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/kernel/nmi_watchdog",
            fs::read_to_string("/proc/sys/kernel/nmi_watchdog")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/panic",
            fs::read_to_string("/proc/sys/kernel/panic").unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/panic_on_oops",
            fs::read_to_string("/proc/sys/kernel/panic_on_oops")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/perf_event_paranoid",
            fs::read_to_string("/proc/sys/kernel/perf_event_paranoid")
                .unwrap_or_else(|_| "3\n".to_string()),
        ),
        (
            "sys/kernel/perf_event_max_sample_rate",
            fs::read_to_string("/proc/sys/kernel/perf_event_max_sample_rate")
                .unwrap_or_else(|_| "100000\n".to_string()),
        ),
        (
            "sys/kernel/sched_child_runs_first",
            fs::read_to_string("/proc/sys/kernel/sched_child_runs_first")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/sched_rr_timeslice_ms",
            fs::read_to_string("/proc/sys/kernel/sched_rr_timeslice_ms")
                .unwrap_or_else(|_| "100\n".to_string()),
        ),
        (
            "sys/kernel/sched_rt_period_us",
            fs::read_to_string("/proc/sys/kernel/sched_rt_period_us")
                .unwrap_or_else(|_| "1000000\n".to_string()),
        ),
        (
            "sys/kernel/sched_rt_runtime_us",
            fs::read_to_string("/proc/sys/kernel/sched_rt_runtime_us")
                .unwrap_or_else(|_| "950000\n".to_string()),
        ),
        (
            "sys/kernel/sysrq",
            fs::read_to_string("/proc/sys/kernel/sysrq").unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/yama/ptrace_scope",
            fs::read_to_string("/proc/sys/kernel/yama/ptrace_scope")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/fs/file-max",
            fs::read_to_string("/proc/sys/fs/file-max")
                .unwrap_or_else(|_| "9223372036854775807\n".to_string()),
        ),
        (
            "sys/fs/file-nr",
            fs::read_to_string("/proc/sys/fs/file-nr")
                .unwrap_or_else(|_| "0\t0\t9223372036854775807\n".to_string()),
        ),
        (
            "sys/fs/inode-nr",
            fs::read_to_string("/proc/sys/fs/inode-nr").unwrap_or_else(|_| "0\t0\n".to_string()),
        ),
        (
            "sys/fs/inode-state",
            fs::read_to_string("/proc/sys/fs/inode-state")
                .unwrap_or_else(|_| "0\t0\t0\t0\t0\t0\t0\n".to_string()),
        ),
        (
            "sys/fs/aio-max-nr",
            fs::read_to_string("/proc/sys/fs/aio-max-nr").unwrap_or_else(|_| "65536\n".to_string()),
        ),
        (
            "sys/fs/aio-nr",
            fs::read_to_string("/proc/sys/fs/aio-nr").unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/fs/dentry-state",
            fs::read_to_string("/proc/sys/fs/dentry-state")
                .unwrap_or_else(|_| "0\t0\t45\t0\t0\t0\n".to_string()),
        ),
        (
            "sys/fs/epoll/max_user_watches",
            fs::read_to_string("/proc/sys/fs/epoll/max_user_watches")
                .unwrap_or_else(|_| "1048576\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msg_default",
            fs::read_to_string("/proc/sys/fs/mqueue/msg_default")
                .unwrap_or_else(|_| "10\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msg_max",
            fs::read_to_string("/proc/sys/fs/mqueue/msg_max")
                .unwrap_or_else(|_| "10\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msgsize_default",
            fs::read_to_string("/proc/sys/fs/mqueue/msgsize_default")
                .unwrap_or_else(|_| "8192\n".to_string()),
        ),
        (
            "sys/fs/mqueue/msgsize_max",
            fs::read_to_string("/proc/sys/fs/mqueue/msgsize_max")
                .unwrap_or_else(|_| "8192\n".to_string()),
        ),
        (
            "sys/fs/nr_open",
            fs::read_to_string("/proc/sys/fs/nr_open").unwrap_or_else(|_| "1048576\n".to_string()),
        ),
        (
            "sys/fs/pipe-max-size",
            fs::read_to_string("/proc/sys/fs/pipe-max-size")
                .unwrap_or_else(|_| "1048576\n".to_string()),
        ),
        (
            "sys/fs/pipe-user-pages-hard",
            fs::read_to_string("/proc/sys/fs/pipe-user-pages-hard")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/fs/pipe-user-pages-soft",
            fs::read_to_string("/proc/sys/fs/pipe-user-pages-soft")
                .unwrap_or_else(|_| "16384\n".to_string()),
        ),
        (
            "sys/fs/inotify/max_user_watches",
            fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
                .unwrap_or_else(|_| "1048576\n".to_string()),
        ),
        (
            "sys/fs/inotify/max_user_instances",
            fs::read_to_string("/proc/sys/fs/inotify/max_user_instances")
                .unwrap_or_else(|_| "1024\n".to_string()),
        ),
        (
            "sys/fs/inotify/max_queued_events",
            fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
                .unwrap_or_else(|_| "16384\n".to_string()),
        ),
        (
            "sys/crypto/fips_enabled",
            fs::read_to_string("/proc/sys/crypto/fips_enabled")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/kernel/random/boot_id",
            fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .unwrap_or_else(|_| format!("{}\n", proc_uuid())),
        ),
        (
            "sys/kernel/random/entropy_avail",
            fs::read_to_string("/proc/sys/kernel/random/entropy_avail")
                .unwrap_or_else(|_| "256\n".to_string()),
        ),
        (
            "sys/kernel/random/poolsize",
            fs::read_to_string("/proc/sys/kernel/random/poolsize")
                .unwrap_or_else(|_| "256\n".to_string()),
        ),
        (
            "sys/kernel/random/urandom_min_reseed_secs",
            fs::read_to_string("/proc/sys/kernel/random/urandom_min_reseed_secs")
                .unwrap_or_else(|_| "60\n".to_string()),
        ),
        (
            "sys/kernel/random/write_wakeup_threshold",
            fs::read_to_string("/proc/sys/kernel/random/write_wakeup_threshold")
                .unwrap_or_else(|_| "256\n".to_string()),
        ),
        (
            "sys/kernel/random/uuid",
            fs::read_to_string("/proc/sys/kernel/random/uuid")
                .unwrap_or_else(|_| format!("{}\n", proc_uuid())),
        ),
        (
            "sys/net/core/somaxconn",
            fs::read_to_string("/proc/sys/net/core/somaxconn")
                .unwrap_or_else(|_| "4096\n".to_string()),
        ),
        (
            "sys/net/core/rmem_default",
            fs::read_to_string("/proc/sys/net/core/rmem_default")
                .unwrap_or_else(|_| "212992\n".to_string()),
        ),
        (
            "sys/net/core/wmem_default",
            fs::read_to_string("/proc/sys/net/core/wmem_default")
                .unwrap_or_else(|_| "212992\n".to_string()),
        ),
        (
            "sys/net/core/rmem_max",
            fs::read_to_string("/proc/sys/net/core/rmem_max")
                .unwrap_or_else(|_| "212992\n".to_string()),
        ),
        (
            "sys/net/core/wmem_max",
            fs::read_to_string("/proc/sys/net/core/wmem_max")
                .unwrap_or_else(|_| "212992\n".to_string()),
        ),
        (
            "sys/net/core/netdev_max_backlog",
            fs::read_to_string("/proc/sys/net/core/netdev_max_backlog")
                .unwrap_or_else(|_| "1000\n".to_string()),
        ),
        (
            "sys/net/core/optmem_max",
            fs::read_to_string("/proc/sys/net/core/optmem_max")
                .unwrap_or_else(|_| "20480\n".to_string()),
        ),
        (
            "sys/net/ipv4/ip_local_port_range",
            fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
                .unwrap_or_else(|_| "32768\t60999\n".to_string()),
        ),
        (
            "sys/net/ipv4/ip_forward",
            fs::read_to_string("/proc/sys/net/ipv4/ip_forward")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_syncookies",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_syncookies")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_fin_timeout",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_fin_timeout")
                .unwrap_or_else(|_| "60\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_keepalive_time",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_keepalive_time")
                .unwrap_or_else(|_| "7200\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_keepalive_intvl",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_keepalive_intvl")
                .unwrap_or_else(|_| "75\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_keepalive_probes",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_keepalive_probes")
                .unwrap_or_else(|_| "9\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_max_syn_backlog",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_max_syn_backlog")
                .unwrap_or_else(|_| "4096\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_retries1",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_retries1")
                .unwrap_or_else(|_| "3\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_retries2",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_retries2")
                .unwrap_or_else(|_| "15\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_rmem",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_rmem")
                .unwrap_or_else(|_| "4096\t131072\t6291456\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_wmem",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_wmem")
                .unwrap_or_else(|_| "4096\t16384\t4194304\n".to_string()),
        ),
        (
            "sys/net/ipv4/tcp_window_scaling",
            fs::read_to_string("/proc/sys/net/ipv4/tcp_window_scaling")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/net/ipv6/conf/all/disable_ipv6",
            fs::read_to_string("/proc/sys/net/ipv6/conf/all/disable_ipv6")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/net/ipv6/conf/default/disable_ipv6",
            fs::read_to_string("/proc/sys/net/ipv6/conf/default/disable_ipv6")
                .unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/net/unix/max_dgram_qlen",
            fs::read_to_string("/proc/sys/net/unix/max_dgram_qlen")
                .unwrap_or_else(|_| "512\n".to_string()),
        ),
        (
            "sys/user/max_cgroup_namespaces",
            fs::read_to_string("/proc/sys/user/max_cgroup_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_ipc_namespaces",
            fs::read_to_string("/proc/sys/user/max_ipc_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_mnt_namespaces",
            fs::read_to_string("/proc/sys/user/max_mnt_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_net_namespaces",
            fs::read_to_string("/proc/sys/user/max_net_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_pid_namespaces",
            fs::read_to_string("/proc/sys/user/max_pid_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_time_namespaces",
            fs::read_to_string("/proc/sys/user/max_time_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_user_namespaces",
            fs::read_to_string("/proc/sys/user/max_user_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/user/max_uts_namespaces",
            fs::read_to_string("/proc/sys/user/max_uts_namespaces")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/vm/overcommit_memory",
            fs::read_to_string("/proc/sys/vm/overcommit_memory")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/vm/overcommit_ratio",
            fs::read_to_string("/proc/sys/vm/overcommit_ratio")
                .unwrap_or_else(|_| "50\n".to_string()),
        ),
        (
            "sys/vm/max_map_count",
            fs::read_to_string("/proc/sys/vm/max_map_count")
                .unwrap_or_else(|_| "1048576\n".to_string()),
        ),
        (
            "sys/vm/mmap_min_addr",
            fs::read_to_string("/proc/sys/vm/mmap_min_addr")
                .unwrap_or_else(|_| "65536\n".to_string()),
        ),
        (
            "sys/vm/drop_caches",
            fs::read_to_string("/proc/sys/vm/drop_caches").unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/vm/min_free_kbytes",
            fs::read_to_string("/proc/sys/vm/min_free_kbytes")
                .unwrap_or_else(|_| "67584\n".to_string()),
        ),
        (
            "sys/vm/stat_interval",
            fs::read_to_string("/proc/sys/vm/stat_interval").unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "sys/vm/swappiness",
            fs::read_to_string("/proc/sys/vm/swappiness").unwrap_or_else(|_| "60\n".to_string()),
        ),
        (
            "sys/vm/vfs_cache_pressure",
            fs::read_to_string("/proc/sys/vm/vfs_cache_pressure")
                .unwrap_or_else(|_| "100\n".to_string()),
        ),
        (
            "sys/vm/zone_reclaim_mode",
            fs::read_to_string("/proc/sys/vm/zone_reclaim_mode")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "sys/vm/dirty_ratio",
            fs::read_to_string("/proc/sys/vm/dirty_ratio").unwrap_or_else(|_| "20\n".to_string()),
        ),
        (
            "sys/vm/dirty_background_ratio",
            fs::read_to_string("/proc/sys/vm/dirty_background_ratio")
                .unwrap_or_else(|_| "10\n".to_string()),
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
            fs::read_to_string("/sys/devices/system/cpu/online")
                .unwrap_or_else(|_| format!("{cpu_range}\n")),
        ),
        (
            "devices/system/cpu/possible".to_string(),
            fs::read_to_string("/sys/devices/system/cpu/possible")
                .unwrap_or_else(|_| format!("{cpu_range}\n")),
        ),
        (
            "devices/system/cpu/present".to_string(),
            fs::read_to_string("/sys/devices/system/cpu/present")
                .unwrap_or_else(|_| format!("{cpu_range}\n")),
        ),
        (
            "devices/system/cpu/kernel_max".to_string(),
            fs::read_to_string("/sys/devices/system/cpu/kernel_max")
                .unwrap_or_else(|_| format!("{}\n", sandbox_cpu_count().saturating_sub(1))),
        ),
        (
            "devices/system/node/online".to_string(),
            fs::read_to_string("/sys/devices/system/node/online")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "devices/system/node/possible".to_string(),
            fs::read_to_string("/sys/devices/system/node/possible")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "fs/cgroup/cgroup.controllers".to_string(),
            fs::read_to_string("/sys/fs/cgroup/cgroup.controllers")
                .unwrap_or_else(|_| "cpu io memory pids\n".to_string()),
        ),
        (
            "fs/cgroup/cgroup.subtree_control".to_string(),
            fs::read_to_string("/sys/fs/cgroup/cgroup.subtree_control")
                .unwrap_or_else(|_| String::new()),
        ),
        (
            "fs/cgroup/cgroup.procs".to_string(),
            fs::read_to_string("/sys/fs/cgroup/cgroup.procs").unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "fs/cgroup/cgroup.type".to_string(),
            fs::read_to_string("/sys/fs/cgroup/cgroup.type")
                .unwrap_or_else(|_| "domain\n".to_string()),
        ),
        (
            "fs/cgroup/cpu.max".to_string(),
            fs::read_to_string("/sys/fs/cgroup/cpu.max")
                .unwrap_or_else(|_| "max 100000\n".to_string()),
        ),
        (
            "fs/cgroup/cpu.weight".to_string(),
            fs::read_to_string("/sys/fs/cgroup/cpu.weight").unwrap_or_else(|_| "100\n".to_string()),
        ),
        (
            "fs/cgroup/memory.current".to_string(),
            fs::read_to_string("/sys/fs/cgroup/memory.current")
                .unwrap_or_else(|_| "0\n".to_string()),
        ),
        (
            "fs/cgroup/memory.max".to_string(),
            fs::read_to_string("/sys/fs/cgroup/memory.max").unwrap_or_else(|_| "max\n".to_string()),
        ),
        (
            "fs/cgroup/pids.current".to_string(),
            fs::read_to_string("/sys/fs/cgroup/pids.current").unwrap_or_else(|_| "1\n".to_string()),
        ),
        (
            "fs/cgroup/pids.max".to_string(),
            fs::read_to_string("/sys/fs/cgroup/pids.max").unwrap_or_else(|_| "max\n".to_string()),
        ),
        (
            "kernel/mm/transparent_hugepage/enabled".to_string(),
            fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
                .unwrap_or_else(|_| "always madvise [never]\n".to_string()),
        ),
        (
            "kernel/mm/transparent_hugepage/hpage_pmd_size".to_string(),
            fs::read_to_string("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size")
                .unwrap_or_else(|_| "2097152\n".to_string()),
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
        files.push((
            format!("{cpu}/online"),
            read_sys_cpu_file(processor, "online").unwrap_or_else(|| "1\n".to_string()),
        ));
        files.push((
            format!("{cpu}/topology/physical_package_id"),
            read_sys_cpu_file(processor, "topology/physical_package_id")
                .unwrap_or_else(|| "0\n".to_string()),
        ));
        files.push((
            format!("{cpu}/topology/core_id"),
            read_sys_cpu_file(processor, "topology/core_id")
                .unwrap_or_else(|| format!("{processor}\n")),
        ));
        files.push((
            format!("{cpu}/topology/thread_siblings_list"),
            read_sys_cpu_file(processor, "topology/thread_siblings_list")
                .unwrap_or_else(|| format!("{processor}\n")),
        ));
        files.push((
            format!("{cpu}/topology/core_siblings_list"),
            read_sys_cpu_file(processor, "topology/core_siblings_list")
                .unwrap_or_else(|| format!("{}\n", sandbox_cpu_range())),
        ));
        files.push((
            format!("{cpu}/cache/index0/coherency_line_size"),
            read_sys_cpu_file(processor, "cache/index0/coherency_line_size")
                .unwrap_or_else(|| "64\n".to_string()),
        ));
        files.push((
            format!("{cpu}/cache/index0/level"),
            read_sys_cpu_file(processor, "cache/index0/level").unwrap_or_else(|| "1\n".to_string()),
        ));
        files.push((
            format!("{cpu}/cache/index0/type"),
            read_sys_cpu_file(processor, "cache/index0/type")
                .unwrap_or_else(|| "Data\n".to_string()),
        ));
        files.push((
            format!("{cpu}/cache/index0/size"),
            read_sys_cpu_file(processor, "cache/index0/size")
                .unwrap_or_else(|| "32K\n".to_string()),
        ));
        files.push((
            format!("{cpu}/cache/index0/shared_cpu_list"),
            read_sys_cpu_file(processor, "cache/index0/shared_cpu_list")
                .unwrap_or_else(|| format!("{processor}\n")),
        ));
        files.push((
            format!("{cpu}/cache/index0/shared_cpu_map"),
            read_sys_cpu_file(processor, "cache/index0/shared_cpu_map").unwrap_or_else(|| {
                format!("{:x}\n", 1usize << processor.min(usize::BITS as usize - 1))
            }),
        ));
    }
    files
}

fn read_sys_cpu_file(processor: usize, relative_path: &str) -> Option<String> {
    fs::read_to_string(format!(
        "/sys/devices/system/cpu/cpu{processor}/{relative_path}"
    ))
    .ok()
}

fn sandbox_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
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
    fs::read_to_string("/etc/resolv.conf")
        .ok()
        .filter(|content| !content.trim().is_empty())
        .unwrap_or_else(|| "options edns0 trust-ad\n".to_string())
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
    fs::read_to_string("/etc/os-release")
        .ok()
        .filter(|content| !content.trim().is_empty())
        .unwrap_or_else(|| {
            [
                "NAME=\"Sinomo Sandbox\"",
                "ID=sinomo-sandbox",
                "PRETTY_NAME=\"Sinomo Sandbox WASIX\"",
            ]
            .join("\n")
                + "\n"
        })
}

fn etc_ca_bundle() -> Vec<u8> {
    for path in [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/cert.pem",
    ] {
        if let Ok(content) = fs::read(path)
            && !content.is_empty()
        {
            return content;
        }
    }
    Vec::new()
}

fn etc_localtime() -> Vec<u8> {
    read_first_nonempty_binary(&[
        "/etc/localtime",
        "/usr/share/zoneinfo/UTC",
        "/usr/share/zoneinfo/Etc/UTC",
    ])
    .unwrap_or_else(|| b"UTC\n".to_vec())
}

fn usr_system_files() -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    let utc = etc_localtime();
    files.push(("share/zoneinfo/UTC".to_string(), utc.clone()));
    files.push(("share/zoneinfo/Etc/UTC".to_string(), utc));

    for (guest_path, host_candidates) in [
        (
            "share/terminfo/d/dumb",
            &[
                "/usr/share/terminfo/d/dumb",
                "/lib/terminfo/d/dumb",
                "/etc/terminfo/d/dumb",
            ][..],
        ),
        (
            "share/terminfo/x/xterm",
            &[
                "/usr/share/terminfo/x/xterm",
                "/lib/terminfo/x/xterm",
                "/etc/terminfo/x/xterm",
            ][..],
        ),
        (
            "share/terminfo/x/xterm-256color",
            &[
                "/usr/share/terminfo/x/xterm-256color",
                "/lib/terminfo/x/xterm-256color",
                "/etc/terminfo/x/xterm-256color",
            ][..],
        ),
    ] {
        if let Some(content) = read_first_nonempty_binary(host_candidates) {
            files.push((guest_path.to_string(), content));
        }
    }

    files
}

fn read_first_nonempty_binary(paths: &[&str]) -> Option<Vec<u8>> {
    paths
        .iter()
        .find_map(|path| fs::read(path).ok().filter(|content| !content.is_empty()))
}

impl Drop for LocalWasiVirtualDeviceDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).ok();
        fs::remove_dir_all(&self.proc_root).ok();
        fs::remove_dir_all(&self.etc_root).ok();
        fs::remove_dir_all(&self.sys_root).ok();
        fs::remove_dir_all(&self.run_root).ok();
        fs::remove_dir_all(&self.var_root).ok();
        fs::remove_dir_all(&self.tmp_root).ok();
        fs::remove_dir_all(&self.home_root).ok();
        fs::remove_dir_all(&self.root_home).ok();
        fs::remove_dir_all(&self.usr_root).ok();
        fs::remove_dir_all(&self.opt_root).ok();
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
