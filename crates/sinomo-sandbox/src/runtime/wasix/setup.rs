use crate::{
    LocalWasiVirtualDeviceDir, SandboxCommandRequest, SandboxMountMode, SandboxWasiError,
    SandboxWasiPreopen,
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
        .filter(|(key, _)| key.as_str() != "PWD")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    if !env.iter().any(|(key, _)| key == "PATH") {
        env.push(("PATH".to_string(), "/bin:/usr/bin".to_string()));
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
        let devices = LocalWasiVirtualDeviceDir::create(zero_device_bytes)?;
        mapped_dirs.push(wasmer_wasix::runners::MappedDirectory {
            host: devices.path().to_path_buf(),
            guest: "/dev".to_string(),
        });
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
    input_limit_bytes: usize,
) -> Result<(), SandboxWasiError> {
    if let Some(stdin) = stdin {
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
    } else {
        runner.with_stdin(Box::<virtual_fs::NullFile>::default());
    }
    Ok(())
}
