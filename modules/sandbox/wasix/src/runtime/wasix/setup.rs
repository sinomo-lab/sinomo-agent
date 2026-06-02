use crate::{
    SandboxCommandRequest, SandboxMountMode, SandboxWasiError, SandboxWasiPreopen,
    SandboxWasiVirtualPreopen, WasiVirtualSystemMounts,
};
use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use super::preopen::{validate_wasix_pinned_child_exec_preopens, validate_wasix_preopen_tree};

pub(super) struct WasixRuntimeMounts {
    _virtual_devices: Option<WasiVirtualSystemMounts>,
}

pub(super) fn wasix_environment(
    request: &SandboxCommandRequest,
    pinned_child_exec_only: bool,
    command_overrides: &[crate::SandboxWasixCommandOverride],
) -> Vec<(String, String)> {
    let mut env = request
        .env
        .iter()
        .filter(|(key, _)| key.as_str() != "PWD" && !key.starts_with("SINOMO_SANDBOX_"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    if !env.iter().any(|(key, _)| key == "PATH") {
        env.push(("PATH".to_string(), "/bin:/usr/bin".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "HOME") {
        env.push(("HOME".to_string(), "/home/sinomo".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "USER") {
        env.push(("USER".to_string(), "sinomo".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "LOGNAME") {
        env.push(("LOGNAME".to_string(), "sinomo".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "SHELL") {
        env.push(("SHELL".to_string(), "/bin/sh".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "TMPDIR") {
        env.push(("TMPDIR".to_string(), "/tmp".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "XDG_CACHE_HOME") {
        env.push((
            "XDG_CACHE_HOME".to_string(),
            "/home/sinomo/.cache".to_string(),
        ));
    }
    if !env.iter().any(|(key, _)| key == "XDG_CONFIG_HOME") {
        env.push((
            "XDG_CONFIG_HOME".to_string(),
            "/home/sinomo/.config".to_string(),
        ));
    }
    if !env.iter().any(|(key, _)| key == "XDG_DATA_HOME") {
        env.push((
            "XDG_DATA_HOME".to_string(),
            "/home/sinomo/.local/share".to_string(),
        ));
    }
    if !env.iter().any(|(key, _)| key == "LANG") {
        env.push(("LANG".to_string(), "C.UTF-8".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "LC_CTYPE") {
        env.push(("LC_CTYPE".to_string(), "C.UTF-8".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "TZ") {
        env.push(("TZ".to_string(), "UTC".to_string()));
    }
    if !env.iter().any(|(key, _)| key == "TERM") {
        env.push(("TERM".to_string(), "dumb".to_string()));
    }
    if pinned_child_exec_only {
        env.push((
            "SINOMO_WASIX_PINNED_CHILD_EXEC_ONLY".to_string(),
            "1".to_string(),
        ));
    }
    for command in command_overrides {
        if !command.argv_prefix.is_empty() {
            env.push((
                format!("SINOMO_WASIX_COMMAND_ARGV_PREFIX_{}", command.name),
                command.argv_prefix.join("\u{1f}"),
            ));
        }
        if let Some(exec_name) = &command.exec_name {
            env.push((
                format!("SINOMO_WASIX_COMMAND_EXEC_NAME_{}", command.name),
                exec_name.clone(),
            ));
        }
    }
    env.push(("PWD".to_string(), request.cwd.as_str().to_string()));
    env
}

pub(super) fn configure_wasix_mapped_directories(
    runner: &mut wasmer_wasix::runners::wasi::WasiRunner,
    argv: &[String],
    env: &[(String, String)],
    preopened_dirs: &[SandboxWasiPreopen],
    virtual_preopened_dirs: &[SandboxWasiVirtualPreopen],
    pinned_child_exec_only: bool,
    enable_virtual_devices: bool,
    zero_device_bytes: usize,
) -> Result<WasixRuntimeMounts, SandboxWasiError> {
    if pinned_child_exec_only {
        validate_wasix_pinned_child_exec_preopens(preopened_dirs)?;
    }
    for preopen in preopened_dirs {
        validate_wasix_preopen_tree(preopen)?;
    }
    let virtual_devices = if enable_virtual_devices {
        let mount_table = preopened_dirs
            .iter()
            .map(|preopen| (preopen.guest_path.clone(), preopen.mode.clone()))
            .chain(
                virtual_preopened_dirs
                    .iter()
                    .map(|preopen| (preopen.guest_path.clone(), preopen.mode.clone())),
            )
            .collect::<Vec<_>>();
        let devices = WasiVirtualSystemMounts::create(zero_device_bytes, argv, env, &mount_table)?;
        let root_workspace_preopen = root_preopen_exists(preopened_dirs, virtual_preopened_dirs);
        if !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/dev") {
            mount_wasix_virtual_directory(
                runner,
                "/dev",
                devices.fs(),
                SandboxMountMode::ReadWrite,
            );
        }
        mount_wasix_virtual_directory(
            runner,
            "/proc",
            devices.proc_fs(),
            SandboxMountMode::ReadOnly,
        );
        if !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/etc") {
            mount_wasix_virtual_directory(
                runner,
                "/etc",
                devices.etc_fs(),
                SandboxMountMode::ReadOnly,
            );
        }
        if !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/sys") {
            mount_wasix_virtual_directory(
                runner,
                "/sys",
                devices.sys_fs(),
                SandboxMountMode::ReadOnly,
            );
        }
        if !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/run") {
            mount_wasix_virtual_directory(
                runner,
                "/run",
                devices.run_fs(),
                SandboxMountMode::ReadWrite,
            );
        }
        if !root_workspace_preopen
            && !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/var")
        {
            mount_wasix_virtual_directory(
                runner,
                "/var",
                devices.var_fs(),
                SandboxMountMode::ReadWrite,
            );
        }
        if !root_workspace_preopen
            && !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/tmp")
        {
            mount_wasix_virtual_directory(
                runner,
                "/tmp",
                devices.tmp_fs(),
                SandboxMountMode::ReadWrite,
            );
        }
        if !root_workspace_preopen
            && !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/home")
        {
            mount_wasix_virtual_directory(
                runner,
                "/home",
                devices.home_fs(),
                SandboxMountMode::ReadWrite,
            );
        }
        if !root_workspace_preopen
            && !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/root")
        {
            mount_wasix_virtual_directory(
                runner,
                "/root",
                devices.root_home_fs(),
                SandboxMountMode::ReadWrite,
            );
        }
        if !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/usr") {
            mount_wasix_virtual_directory(
                runner,
                "/usr",
                devices.usr_fs(),
                system_parent_mount_mode(preopened_dirs, virtual_preopened_dirs, "/usr"),
            );
        }
        if !preopen_exists(preopened_dirs, virtual_preopened_dirs, "/opt") {
            mount_wasix_virtual_directory(
                runner,
                "/opt",
                devices.opt_fs(),
                system_parent_mount_mode(preopened_dirs, virtual_preopened_dirs, "/opt"),
            );
        }
        Some(devices)
    } else {
        None
    };
    for preopen in preopened_dirs {
        if let Some(devices) = virtual_devices.as_ref()
            && mount_nested_host_preopen_into_system_mount(devices, preopen)?
        {
            continue;
        }
        mount_wasix_host_directory(
            runner,
            preopen.guest_path.as_str(),
            &preopen.host_path,
            preopen.mode.clone(),
        )?;
    }
    for preopen in virtual_preopened_dirs {
        if let Some(devices) = virtual_devices.as_ref()
            && mount_nested_virtual_preopen_into_system_mount(devices, preopen)?
        {
            continue;
        }
        mount_wasix_virtual_directory(
            runner,
            preopen.guest_path.as_str(),
            Arc::clone(&preopen.fs),
            preopen.mode.clone(),
        );
    }
    Ok(WasixRuntimeMounts {
        _virtual_devices: virtual_devices,
    })
}

fn root_preopen_exists(
    host_preopens: &[SandboxWasiPreopen],
    virtual_preopens: &[SandboxWasiVirtualPreopen],
) -> bool {
    preopen_exists(host_preopens, virtual_preopens, "/")
}

fn preopen_exists(
    host_preopens: &[SandboxWasiPreopen],
    virtual_preopens: &[SandboxWasiVirtualPreopen],
    guest_path: &str,
) -> bool {
    host_preopens
        .iter()
        .any(|preopen| preopen_covers_system_mount(&preopen.guest_path, guest_path))
        || virtual_preopens
            .iter()
            .any(|preopen| preopen_covers_system_mount(&preopen.guest_path, guest_path))
}

fn preopen_covers_system_mount(preopen_path: &crate::SandboxPath, guest_path: &str) -> bool {
    preopen_path.as_str() == guest_path
}

fn system_parent_mount_mode(
    host_preopens: &[SandboxWasiPreopen],
    virtual_preopens: &[SandboxWasiVirtualPreopen],
    guest_path: &str,
) -> SandboxMountMode {
    let has_read_write_child = host_preopens
        .iter()
        .any(|preopen| nested_read_write_preopen(&preopen.guest_path, &preopen.mode, guest_path))
        || virtual_preopens.iter().any(|preopen| {
            nested_read_write_preopen(&preopen.guest_path, &preopen.mode, guest_path)
        });
    if has_read_write_child {
        SandboxMountMode::ReadWrite
    } else {
        SandboxMountMode::ReadOnly
    }
}

fn nested_read_write_preopen(
    preopen_path: &crate::SandboxPath,
    mode: &SandboxMountMode,
    guest_path: &str,
) -> bool {
    *mode == SandboxMountMode::ReadWrite
        && preopen_path
            .as_str()
            .strip_prefix(guest_path)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn mount_nested_host_preopen_into_system_mount(
    devices: &WasiVirtualSystemMounts,
    preopen: &SandboxWasiPreopen,
) -> Result<bool, SandboxWasiError> {
    let Some((parent, relative_mount_path)) = nested_system_mount_path(&preopen.guest_path) else {
        return Ok(false);
    };
    let fs: Arc<dyn virtual_fs::FileSystem + Send + Sync> = Arc::new(
        virtual_fs::host_fs::FileSystem::new(tokio::runtime::Handle::current(), &preopen.host_path)
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!(
                    "prepare WASIX nested mount {} from {}: {error}",
                    preopen.guest_path,
                    preopen.host_path.display()
                ),
            })?,
    );
    mount_nested_preopen_into_system_mount(
        devices,
        parent,
        &relative_mount_path,
        wrap_wasix_mount_fs(fs, preopen.mode.clone()),
    )?;
    Ok(true)
}

fn mount_nested_virtual_preopen_into_system_mount(
    devices: &WasiVirtualSystemMounts,
    preopen: &SandboxWasiVirtualPreopen,
) -> Result<bool, SandboxWasiError> {
    let Some((parent, relative_mount_path)) = nested_system_mount_path(&preopen.guest_path) else {
        return Ok(false);
    };
    mount_nested_preopen_into_system_mount(
        devices,
        parent,
        &relative_mount_path,
        wrap_wasix_mount_fs(Arc::clone(&preopen.fs), preopen.mode.clone()),
    )?;
    Ok(true)
}

fn nested_system_mount_path(path: &crate::SandboxPath) -> Option<(&'static str, PathBuf)> {
    for parent in [
        "/dev", "/etc", "/sys", "/run", "/var", "/tmp", "/home", "/root", "/usr", "/opt",
    ] {
        if let Some(tail) = path.as_str().strip_prefix(parent)
            && tail.starts_with('/')
        {
            return Some((
                parent,
                PathBuf::from("/").join(tail.trim_start_matches('/')),
            ));
        }
    }
    None
}

fn mount_nested_preopen_into_system_mount(
    devices: &WasiVirtualSystemMounts,
    parent: &str,
    relative_mount_path: &Path,
    fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
) -> Result<(), SandboxWasiError> {
    let parent_fs = system_mount_fs(devices, parent).ok_or_else(|| SandboxWasiError::Runtime {
        detail: format!("unknown WASIX system mount parent {parent}"),
    })?;
    parent_fs
        .mount(
            relative_mount_path.display().to_string(),
            relative_mount_path,
            Box::new(virtual_fs::ArcFileSystem::new(fs)),
        )
        .map_err(|error| SandboxWasiError::Runtime {
            detail: format!(
                "prepare WASIX nested mount {parent}{}: {error}",
                relative_mount_path.display()
            ),
        })
}

fn system_mount_fs(
    devices: &WasiVirtualSystemMounts,
    parent: &str,
) -> Option<Arc<dyn virtual_fs::FileSystem + Send + Sync>> {
    match parent {
        "/dev" => Some(devices.fs()),
        "/etc" => Some(devices.etc_fs()),
        "/sys" => Some(devices.sys_fs()),
        "/run" => Some(devices.run_fs()),
        "/var" => Some(devices.var_fs()),
        "/tmp" => Some(devices.tmp_fs()),
        "/home" => Some(devices.home_fs()),
        "/root" => Some(devices.root_home_fs()),
        "/usr" => Some(devices.usr_fs()),
        "/opt" => Some(devices.opt_fs()),
        _ => None,
    }
}

fn mount_wasix_host_directory(
    runner: &mut wasmer_wasix::runners::wasi::WasiRunner,
    guest: &str,
    host: &Path,
    mode: SandboxMountMode,
) -> Result<(), SandboxWasiError> {
    let fs: Arc<dyn virtual_fs::FileSystem + Send + Sync> = Arc::new(
        virtual_fs::host_fs::FileSystem::new(tokio::runtime::Handle::current(), host).map_err(
            |error| SandboxWasiError::Runtime {
                detail: format!(
                    "prepare WASIX mount {guest} from {}: {error}",
                    host.display()
                ),
            },
        )?,
    );
    let fs = match mode {
        SandboxMountMode::ReadOnly => wrap_wasix_mount_fs(fs, SandboxMountMode::ReadOnly),
        SandboxMountMode::ReadWrite => wrap_wasix_mount_fs(fs, SandboxMountMode::ReadWrite),
    };
    runner.with_mount(guest.to_string(), fs);
    Ok(())
}

fn mount_wasix_virtual_directory(
    runner: &mut wasmer_wasix::runners::wasi::WasiRunner,
    guest: &str,
    fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    mode: SandboxMountMode,
) {
    let fs = wrap_wasix_mount_fs(fs, mode);
    runner.with_mount(guest.to_string(), fs);
}

fn wrap_wasix_mount_fs(
    fs: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
    mode: SandboxMountMode,
) -> Arc<dyn virtual_fs::FileSystem + Send + Sync> {
    match mode {
        SandboxMountMode::ReadOnly => Arc::new(WasixReadOnlyFileSystem::new(fs)),
        SandboxMountMode::ReadWrite => fs,
    }
}

#[derive(Debug)]
pub(super) struct WasixReadOnlyFileSystem {
    inner: Arc<dyn virtual_fs::FileSystem + Send + Sync>,
}

impl WasixReadOnlyFileSystem {
    pub(super) fn new(inner: Arc<dyn virtual_fs::FileSystem + Send + Sync>) -> Self {
        Self { inner }
    }
}

impl virtual_fs::FileOpener for WasixReadOnlyFileSystem {
    fn open(
        &self,
        path: &Path,
        conf: &virtual_fs::OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn virtual_fs::VirtualFile + Send + Sync + 'static>> {
        if conf.would_mutate() {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        let file = self
            .inner
            .new_open_options()
            .options(conf.clone())
            .open(path)?;
        Ok(Box::new(WasixReadOnlyVirtualFile::new(file)))
    }
}

#[derive(Debug)]
struct WasixReadOnlyVirtualFile {
    inner: Box<dyn virtual_fs::VirtualFile + Send + Sync + 'static>,
}

impl WasixReadOnlyVirtualFile {
    fn new(inner: Box<dyn virtual_fs::VirtualFile + Send + Sync + 'static>) -> Self {
        Self { inner }
    }
}

impl virtual_fs::VirtualFile for WasixReadOnlyVirtualFile {
    fn last_accessed(&self) -> u64 {
        self.inner.last_accessed()
    }

    fn last_modified(&self) -> u64 {
        self.inner.last_modified()
    }

    fn created_time(&self) -> u64 {
        self.inner.created_time()
    }

    fn inode(&self) -> Option<u64> {
        self.inner.inode()
    }

    fn link_count(&self) -> Option<u64> {
        self.inner.link_count()
    }

    fn refresh_metadata(&mut self) -> virtual_fs::Result<()> {
        self.inner.refresh_metadata()
    }

    fn note_link_added(&mut self) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn note_link_removed(&mut self) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_mode(&mut self, _mode: u32) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_owner(&mut self, _uid: u32, _gid: u32) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_times(&mut self, _atime: Option<u64>, _mtime: Option<u64>) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn set_len(&mut self, _new_size: u64) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    fn get_special_fd(&self) -> Option<u32> {
        self.inner.get_special_fd()
    }

    fn write_from_mmap(&mut self, _offset: u64, _len: u64) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }

    fn poll_read_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_read_ready(cx)
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::PermissionDenied)))
    }
}

impl virtual_fs::AsyncSeek for WasixReadOnlyVirtualFile {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut *self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut *self.inner).poll_complete(cx)
    }
}

impl virtual_fs::AsyncRead for WasixReadOnlyVirtualFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(cx, buf)
    }
}

impl virtual_fs::AsyncWrite for WasixReadOnlyVirtualFile {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::PermissionDenied)))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl virtual_fs::FileSystem for WasixReadOnlyFileSystem {
    fn readlink(&self, path: &Path) -> virtual_fs::Result<std::path::PathBuf> {
        self.inner.readlink(path)
    }

    fn read_dir(&self, path: &Path) -> virtual_fs::Result<virtual_fs::ReadDir> {
        self.inner.read_dir(path)
    }

    fn create_dir(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn remove_dir(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn rename<'a>(
        &'a self,
        _from: &'a Path,
        _to: &'a Path,
    ) -> Pin<Box<dyn Future<Output = virtual_fs::Result<()>> + Send + 'a>> {
        Box::pin(async { Err(virtual_fs::FsError::PermissionDenied) })
    }

    fn metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.inner.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.inner.symlink_metadata(path)
    }

    fn remove_file(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn symlink(&self, _target: &Path, _link_path: &Path) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn hard_link(&self, _old_path: &Path, _new_path: &Path) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_mode(&self, _path: &Path, _mode: u32) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_owner(&self, _path: &Path, _uid: u32, _gid: u32) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_times(
        &self,
        _path: &Path,
        _atime: Option<u64>,
        _mtime: Option<u64>,
    ) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn set_symlink_times(
        &self,
        _path: &Path,
        _atime: Option<u64>,
        _mtime: Option<u64>,
    ) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }

    fn new_open_options(&self) -> virtual_fs::OpenOptions<'_> {
        virtual_fs::OpenOptions::new(self)
    }

    fn mount(
        &self,
        _name: String,
        _path: &Path,
        _fs: Box<dyn virtual_fs::FileSystem + Send + Sync>,
    ) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::PermissionDenied)
    }
}

pub(super) fn configure_wasix_stdin(
    runner: &mut wasmer_wasix::runners::wasi::WasiRunner,
    stdin: Option<Vec<u8>>,
    stdin_pipe: Option<virtual_fs::ArcFile<virtual_fs::Pipe>>,
    input_limit_bytes: usize,
) -> Result<Option<WasixBoundedInputHandle>, SandboxWasiError> {
    match (stdin, stdin_pipe) {
        (Some(_), Some(_)) => {
            return Err(SandboxWasiError::InvalidRequest {
                detail: "WASIX stdin cannot be both buffered bytes and an fd-backed pipe"
                    .to_string(),
            });
        }
        (Some(stdin), None) => {
            if stdin.len() > input_limit_bytes {
                return Err(SandboxWasiError::InputLimitExceeded {
                    limit: input_limit_bytes,
                });
            }
            let mut stdin_file = virtual_fs::ArcFile::new(Box::<virtual_fs::BufferFile>::default());
            virtual_mio::block_on(async {
                use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
                stdin_file.write_all(&stdin).await?;
                stdin_file.rewind().await
            })
            .map_err(|error| SandboxWasiError::Runtime {
                detail: format!("prepare WASIX stdin: {error}"),
            })?;
            runner.with_stdin(Box::new(stdin_file));
            Ok(None)
        }
        (None, Some(stdin_pipe)) => {
            let input = WasixBoundedInputHandle::new(input_limit_bytes);
            runner.with_stdin(Box::new(WasixBoundedInputFile::new(stdin_pipe, &input)));
            Ok(Some(input))
        }
        (None, None) => {
            runner.with_stdin(Box::<virtual_fs::NullFile>::default());
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct WasixBoundedInputHandle {
    state: Arc<Mutex<WasixBoundedInputState>>,
}

#[derive(Debug)]
struct WasixBoundedInputState {
    bytes_read: usize,
    limit: usize,
    limit_exceeded: bool,
}

impl WasixBoundedInputHandle {
    fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(WasixBoundedInputState {
                bytes_read: 0,
                limit,
                limit_exceeded: false,
            })),
        }
    }

    pub(super) fn input_limit_exceeded(&self) -> Result<Option<usize>, SandboxWasiError> {
        let state = self.state.lock().map_err(|_| SandboxWasiError::Runtime {
            detail: "WASIX input limit lock poisoned".to_string(),
        })?;
        Ok(state.limit_exceeded.then_some(state.limit))
    }

    fn reserve_read(&self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("WASIX input limit lock poisoned"))?;
        if state.limit_exceeded || state.bytes_read >= state.limit {
            state.limit_exceeded = true;
            return Err(io::Error::other(format!(
                "WASIX stdin exceeded {} byte limit",
                state.limit
            )));
        }
        Ok(())
    }

    fn record_read(&self, bytes_read: usize) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("WASIX input limit lock poisoned"))?;
        state.bytes_read = state.bytes_read.saturating_add(bytes_read);
        if state.bytes_read > state.limit {
            state.limit_exceeded = true;
            Err(io::Error::other(format!(
                "WASIX stdin exceeded {} byte limit",
                state.limit
            )))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
struct WasixBoundedInputFile {
    inner: virtual_fs::ArcFile<virtual_fs::Pipe>,
    input: WasixBoundedInputHandle,
}

impl WasixBoundedInputFile {
    fn new(inner: virtual_fs::ArcFile<virtual_fs::Pipe>, input: &WasixBoundedInputHandle) -> Self {
        Self {
            inner,
            input: input.clone(),
        }
    }
}

impl virtual_fs::VirtualFile for WasixBoundedInputFile {
    fn last_accessed(&self) -> u64 {
        self.inner.last_accessed()
    }

    fn last_modified(&self) -> u64 {
        self.inner.last_modified()
    }

    fn created_time(&self) -> u64 {
        self.inner.created_time()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn set_len(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        self.inner.set_len(new_size)
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        self.inner.unlink()
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    fn poll_read_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_read_ready(cx)
    }

    fn poll_write_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_ready(cx)
    }
}

impl virtual_fs::AsyncSeek for WasixBoundedInputFile {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.inner).poll_complete(cx)
    }
}

impl virtual_fs::AsyncRead for WasixBoundedInputFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.input.reserve_read() {
            return Poll::Ready(Err(error));
        }
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        match self
            .input
            .record_read(buf.filled().len().saturating_sub(before))
        {
            Ok(()) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl virtual_fs::AsyncWrite for WasixBoundedInputFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exact_preopen_covers_default_system_mount() {
        assert!(!preopen_covers_system_mount(
            &crate::SandboxPath::new("/opt/pkg").expect("preopen"),
            "/opt",
        ));
        assert!(!preopen_covers_system_mount(
            &crate::SandboxPath::new("/usr/local").expect("preopen"),
            "/usr",
        ));
        assert!(preopen_covers_system_mount(
            &crate::SandboxPath::new("/tmp").expect("preopen"),
            "/tmp",
        ));
        assert!(preopen_covers_system_mount(
            &crate::SandboxPath::new("/opt").expect("preopen"),
            "/opt",
        ));
        assert!(!preopen_covers_system_mount(
            &crate::SandboxPath::new("/optical").expect("preopen"),
            "/opt",
        ));
        assert!(!preopen_covers_system_mount(
            &crate::SandboxPath::new("/work/opt").expect("preopen"),
            "/opt",
        ));
    }

    #[test]
    fn nested_virtual_preopen_does_not_suppress_parent_system_mount() {
        let virtual_preopen = crate::SandboxWasiVirtualPreopen {
            guest_path: crate::SandboxPath::new("/opt/pkg").expect("preopen"),
            mode: SandboxMountMode::ReadOnly,
            fs: std::sync::Arc::new(virtual_fs::mem_fs::FileSystem::default()),
        };

        assert!(!preopen_exists(&[], &[virtual_preopen.clone()], "/opt"));
        assert!(preopen_exists(&[], &[virtual_preopen], "/opt/pkg"));
    }
}
