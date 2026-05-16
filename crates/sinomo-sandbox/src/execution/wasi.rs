use crate::internal::*;

use super::SandboxWasiError;

#[derive(Clone)]
pub(crate) struct LocalWasiVirtualDeviceDir {
    root: PathBuf,
}

impl LocalWasiVirtualDeviceDir {
    pub(crate) fn create(zero_device_bytes: usize) -> Result<Self, SandboxWasiError> {
        let zero_device_bytes = zero_device_bytes.min(SANDBOX_WASI_ZERO_DEVICE_BYTES);
        let root = std::env::temp_dir().join(format!(
            "sinomo-sandbox-wasi-dev-{}",
            SandboxCommandId::generate()
        ));
        fs::create_dir_all(&root).map_err(|error| SandboxWasiError::Runtime {
            detail: format!("create sandbox virtual device directory: {error}"),
        })?;
        let devices = Self { root };
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
        Ok(devices)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for LocalWasiVirtualDeviceDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).ok();
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
