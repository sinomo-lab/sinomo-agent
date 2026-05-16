use crate::internal::*;

use super::{ByteRange, SandboxDirEntry, SandboxFsError, SandboxMetadata};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SandboxVirtualDevicePath {
    DevDirectory,
    Null,
    Zero,
    Random,
    Urandom,
}

pub(super) fn sandbox_virtual_device_path(path: &SandboxPath) -> Option<SandboxVirtualDevicePath> {
    match path.as_str() {
        "/dev" => Some(SandboxVirtualDevicePath::DevDirectory),
        "/dev/null" => Some(SandboxVirtualDevicePath::Null),
        "/dev/zero" => Some(SandboxVirtualDevicePath::Zero),
        "/dev/random" => Some(SandboxVirtualDevicePath::Random),
        "/dev/urandom" => Some(SandboxVirtualDevicePath::Urandom),
        _ => None,
    }
}

pub fn sandbox_path_is_virtual_device_sink(path: &SandboxPath) -> bool {
    matches!(
        sandbox_virtual_device_path(path),
        Some(
            SandboxVirtualDevicePath::Null
                | SandboxVirtualDevicePath::Zero
                | SandboxVirtualDevicePath::Random
                | SandboxVirtualDevicePath::Urandom
        )
    )
}

pub(super) fn sandbox_path_is_dev_tree(path: &SandboxPath) -> bool {
    path.as_str() == "/dev" || path.as_str().starts_with("/dev/")
}

pub(super) fn sandbox_virtual_device_metadata(path: &SandboxPath) -> Option<SandboxMetadata> {
    sandbox_virtual_device_path(path).map(|device| SandboxMetadata {
        path: path.clone(),
        is_dir: matches!(device, SandboxVirtualDevicePath::DevDirectory),
        len: 0,
    })
}

pub(super) fn sandbox_virtual_device_dir_entries() -> Vec<SandboxDirEntry> {
    SANDBOX_MVP_VIRTUAL_DEVICES
        .iter()
        .map(|path| {
            let path = SandboxPath::new(*path).expect("static virtual device path");
            SandboxDirEntry {
                metadata: sandbox_virtual_device_metadata(&path)
                    .expect("static virtual device metadata"),
                path,
            }
        })
        .collect()
}

pub(super) fn read_sandbox_virtual_device(
    path: &SandboxPath,
    range: Option<ByteRange>,
) -> Result<Vec<u8>, SandboxFsError> {
    match sandbox_virtual_device_path(path) {
        Some(SandboxVirtualDevicePath::Null) => Ok(Vec::new()),
        Some(SandboxVirtualDevicePath::Zero) => {
            let Some(range) = range else {
                return Err(SandboxFsError::Unsupported {
                    operation: "unbounded read from /dev/zero".to_string(),
                });
            };
            let Some(end) = range.end else {
                return Err(SandboxFsError::Unsupported {
                    operation: "open-ended read from /dev/zero".to_string(),
                });
            };
            if end <= range.start {
                return Ok(Vec::new());
            }
            let len =
                usize::try_from(end - range.start).map_err(|_| SandboxFsError::Unavailable {
                    reason: format!("range too large for {}", path),
                })?;
            Ok(vec![0; len])
        }
        Some(SandboxVirtualDevicePath::Random | SandboxVirtualDevicePath::Urandom) => {
            let Some(range) = range else {
                return Err(SandboxFsError::Unsupported {
                    operation: format!("unbounded read from {}", path),
                });
            };
            let Some(end) = range.end else {
                return Err(SandboxFsError::Unsupported {
                    operation: format!("open-ended read from {}", path),
                });
            };
            if end <= range.start {
                return Ok(Vec::new());
            }
            let len =
                usize::try_from(end - range.start).map_err(|_| SandboxFsError::Unavailable {
                    reason: format!("range too large for {}", path),
                })?;
            let mut bytes = vec![0; len];
            getrandom::getrandom(&mut bytes).map_err(|error| SandboxFsError::Unavailable {
                reason: format!("random device unavailable for {}: {error}", path),
            })?;
            Ok(bytes)
        }
        Some(SandboxVirtualDevicePath::DevDirectory) => {
            Err(SandboxFsError::ReadOnly { path: path.clone() })
        }
        None => Err(SandboxFsError::NotFound { path: path.clone() }),
    }
}

pub(super) fn write_sandbox_virtual_device(path: &SandboxPath) -> Result<(), SandboxFsError> {
    match sandbox_virtual_device_path(path) {
        Some(
            SandboxVirtualDevicePath::Null
            | SandboxVirtualDevicePath::Zero
            | SandboxVirtualDevicePath::Random
            | SandboxVirtualDevicePath::Urandom,
        ) => Ok(()),
        Some(SandboxVirtualDevicePath::DevDirectory) => {
            Err(SandboxFsError::ReadOnly { path: path.clone() })
        }
        None => Err(SandboxFsError::NotFound { path: path.clone() }),
    }
}
