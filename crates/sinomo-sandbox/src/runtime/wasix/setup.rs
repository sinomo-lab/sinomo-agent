use crate::{
    LocalWasiVirtualDeviceDir, SandboxCommandRequest, SandboxMountMode, SandboxWasiError,
    SandboxWasiPreopen,
};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use super::preopen::{
    WasixReadOnlyPreopenGuard, validate_wasix_pinned_child_exec_preopens,
    validate_wasix_preopen_tree,
};

pub(super) struct WasixRuntimeMounts {
    _virtual_devices: Option<LocalWasiVirtualDeviceDir>,
    _read_only_preopens: WasixReadOnlyPreopenGuard,
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
    pinned_child_exec_only: bool,
    enable_virtual_devices: bool,
    zero_device_bytes: usize,
) -> Result<WasixRuntimeMounts, SandboxWasiError> {
    if pinned_child_exec_only {
        validate_wasix_pinned_child_exec_preopens(preopened_dirs)?;
    }
    let mut mapped_dirs =
        Vec::with_capacity(preopened_dirs.len() + usize::from(enable_virtual_devices));
    let mut read_only_preopens = WasixReadOnlyPreopenGuard::default();
    for preopen in preopened_dirs {
        validate_wasix_preopen_tree(preopen)?;
        if preopen.mode == SandboxMountMode::ReadOnly {
            read_only_preopens
                .add_tree(&preopen.host_path)
                .map_err(|error| SandboxWasiError::Runtime {
                    detail: format!(
                        "prepare WASIX read-only preopen {}: {error}",
                        preopen.guest_path
                    ),
                })?;
        }
        mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
            host: preopen.host_path.clone(),
            guest: preopen.guest_path.as_str().to_string(),
        });
    }
    let virtual_devices = if enable_virtual_devices {
        let mount_table = preopened_dirs
            .iter()
            .map(|preopen| (preopen.guest_path.clone(), preopen.mode.clone()))
            .collect::<Vec<_>>();
        let devices =
            LocalWasiVirtualDeviceDir::create(zero_device_bytes, argv, env, &mount_table)?;
        mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
            host: devices.proc_path().to_path_buf(),
            guest: "/proc".to_string(),
        });
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/etc")
        {
            read_only_preopens
                .add_tree(devices.etc_path())
                .map_err(|error| SandboxWasiError::Runtime {
                    detail: format!("prepare WASIX sandbox /etc: {error}"),
                })?;
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.etc_path().to_path_buf(),
                guest: "/etc".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/sys")
        {
            read_only_preopens
                .add_tree(devices.sys_path())
                .map_err(|error| SandboxWasiError::Runtime {
                    detail: format!("prepare WASIX sandbox /sys: {error}"),
                })?;
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.sys_path().to_path_buf(),
                guest: "/sys".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/run")
        {
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.run_path().to_path_buf(),
                guest: "/run".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/var")
        {
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.var_path().to_path_buf(),
                guest: "/var".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/tmp")
        {
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.tmp_path().to_path_buf(),
                guest: "/tmp".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/home")
        {
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.home_path().to_path_buf(),
                guest: "/home".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/root")
        {
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.root_home_path().to_path_buf(),
                guest: "/root".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/usr")
        {
            read_only_preopens
                .add_tree(devices.usr_path())
                .map_err(|error| SandboxWasiError::Runtime {
                    detail: format!("prepare WASIX sandbox /usr: {error}"),
                })?;
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.usr_path().to_path_buf(),
                guest: "/usr".to_string(),
            });
        }
        if !preopened_dirs
            .iter()
            .any(|preopen| preopen.guest_path.as_str() == "/opt")
        {
            read_only_preopens
                .add_tree(devices.opt_path())
                .map_err(|error| SandboxWasiError::Runtime {
                    detail: format!("prepare WASIX sandbox /opt: {error}"),
                })?;
            mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
                host: devices.opt_path().to_path_buf(),
                guest: "/opt".to_string(),
            });
        }
        Some(devices)
    } else {
        None
    };
    runner.with_mapped_directories(mapped_dirs);
    Ok(WasixRuntimeMounts {
        _virtual_devices: virtual_devices,
        _read_only_preopens: read_only_preopens,
    })
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
