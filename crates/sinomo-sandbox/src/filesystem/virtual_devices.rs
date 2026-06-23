use crate::internal::*;

use super::{ByteRange, SandboxDirEntry, SandboxFsError, SandboxMetadata};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SandboxVirtualDevicePath {
    DevDirectory,
    Null,
    Zero,
    Random,
    Urandom,
    Full,
    Stdin,
    Stdout,
    Stderr,
    Tty,
    Console,
    Ptmx,
    FdDirectory,
    Fd0,
    Fd1,
    Fd2,
    PtsDirectory,
    ShmDirectory,
}

pub(super) fn sandbox_virtual_device_path(path: &SandboxPath) -> Option<SandboxVirtualDevicePath> {
    match path.as_str() {
        "/dev" => Some(SandboxVirtualDevicePath::DevDirectory),
        "/dev/null" => Some(SandboxVirtualDevicePath::Null),
        "/dev/zero" => Some(SandboxVirtualDevicePath::Zero),
        "/dev/random" => Some(SandboxVirtualDevicePath::Random),
        "/dev/urandom" => Some(SandboxVirtualDevicePath::Urandom),
        "/dev/full" => Some(SandboxVirtualDevicePath::Full),
        "/dev/stdin" => Some(SandboxVirtualDevicePath::Stdin),
        "/dev/stdout" => Some(SandboxVirtualDevicePath::Stdout),
        "/dev/stderr" => Some(SandboxVirtualDevicePath::Stderr),
        "/dev/tty" => Some(SandboxVirtualDevicePath::Tty),
        "/dev/console" => Some(SandboxVirtualDevicePath::Console),
        "/dev/ptmx" => Some(SandboxVirtualDevicePath::Ptmx),
        "/dev/fd" => Some(SandboxVirtualDevicePath::FdDirectory),
        "/dev/fd/0" => Some(SandboxVirtualDevicePath::Fd0),
        "/dev/fd/1" => Some(SandboxVirtualDevicePath::Fd1),
        "/dev/fd/2" => Some(SandboxVirtualDevicePath::Fd2),
        "/dev/pts" => Some(SandboxVirtualDevicePath::PtsDirectory),
        "/dev/shm" => Some(SandboxVirtualDevicePath::ShmDirectory),
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
                | SandboxVirtualDevicePath::Full
                | SandboxVirtualDevicePath::Stdout
                | SandboxVirtualDevicePath::Stderr
                | SandboxVirtualDevicePath::Tty
                | SandboxVirtualDevicePath::Console
                | SandboxVirtualDevicePath::Ptmx
                | SandboxVirtualDevicePath::Fd1
                | SandboxVirtualDevicePath::Fd2
        )
    )
}

pub(super) fn sandbox_path_is_dev_tree(path: &SandboxPath) -> bool {
    path.as_str() == "/dev" || path.as_str().starts_with("/dev/")
}

pub(super) fn sandbox_virtual_device_metadata(path: &SandboxPath) -> Option<SandboxMetadata> {
    sandbox_virtual_device_path(path).map(|device| SandboxMetadata {
        path: path.clone(),
        is_dir: matches!(
            device,
            SandboxVirtualDevicePath::DevDirectory
                | SandboxVirtualDevicePath::FdDirectory
                | SandboxVirtualDevicePath::PtsDirectory
                | SandboxVirtualDevicePath::ShmDirectory
        ),
        is_symlink: false,
        len: 0,
        link_count: if matches!(
            device,
            SandboxVirtualDevicePath::DevDirectory
                | SandboxVirtualDevicePath::FdDirectory
                | SandboxVirtualDevicePath::PtsDirectory
                | SandboxVirtualDevicePath::ShmDirectory
        ) {
            2
        } else {
            1
        },
        mode: if matches!(
            device,
            SandboxVirtualDevicePath::DevDirectory
                | SandboxVirtualDevicePath::FdDirectory
                | SandboxVirtualDevicePath::PtsDirectory
                | SandboxVirtualDevicePath::ShmDirectory
        ) {
            0o755
        } else {
            0o666
        },
        uid: 0,
        gid: 0,
        inode: virtual_device_inode(path),
        object_identity: None,
        accessed_at_unix_nanos: 1_000_000_000,
        modified_at_unix_nanos: 1_000_000_000,
        created_at_unix_nanos: 1_000_000_000,
        changed_at_unix_nanos: 1_000_000_000,
    })
}

fn virtual_device_inode(path: &SandboxPath) -> u64 {
    let hash = blake3::hash(path.as_str().as_bytes());
    u64::from_le_bytes(hash.as_bytes()[0..8].try_into().expect("hash slice"))
}

pub(super) fn sandbox_virtual_device_dir_entries(path: &SandboxPath) -> Vec<SandboxDirEntry> {
    let prefix = match path.as_str() {
        "/dev/fd" => "/dev/fd/",
        "/dev/pts" => "/dev/pts/",
        "/dev/shm" => "/dev/shm/",
        _ => "/dev/",
    };
    SANDBOX_MVP_VIRTUAL_DEVICES
        .iter()
        .copied()
        .filter(|device| device.starts_with(prefix))
        .filter(|device| !device[prefix.len()..].contains('/'))
        .map(|device| {
            let path = SandboxPath::new(device).expect("static virtual device path");
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
        Some(SandboxVirtualDevicePath::Full) => {
            let Some(range) = range else {
                return Err(SandboxFsError::Unsupported {
                    operation: "unbounded read from /dev/full".to_string(),
                });
            };
            let Some(end) = range.end else {
                return Err(SandboxFsError::Unsupported {
                    operation: "open-ended read from /dev/full".to_string(),
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
            getrandom::fill(&mut bytes).map_err(|error| SandboxFsError::Unavailable {
                reason: format!("random device unavailable for {}: {error}", path),
            })?;
            Ok(bytes)
        }
        Some(SandboxVirtualDevicePath::DevDirectory) => {
            Err(SandboxFsError::ReadOnly { path: path.clone() })
        }
        Some(
            SandboxVirtualDevicePath::Stdin
            | SandboxVirtualDevicePath::Stdout
            | SandboxVirtualDevicePath::Stderr
            | SandboxVirtualDevicePath::Tty
            | SandboxVirtualDevicePath::Console
            | SandboxVirtualDevicePath::Ptmx
            | SandboxVirtualDevicePath::Fd0
            | SandboxVirtualDevicePath::Fd1
            | SandboxVirtualDevicePath::Fd2,
        ) => Ok(Vec::new()),
        Some(
            SandboxVirtualDevicePath::FdDirectory
            | SandboxVirtualDevicePath::PtsDirectory
            | SandboxVirtualDevicePath::ShmDirectory,
        ) => Err(SandboxFsError::ReadOnly { path: path.clone() }),
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
        Some(SandboxVirtualDevicePath::Full) => Err(SandboxFsError::Unavailable {
            reason: "/dev/full has no free space".to_string(),
        }),
        Some(
            SandboxVirtualDevicePath::Stdout
            | SandboxVirtualDevicePath::Stderr
            | SandboxVirtualDevicePath::Tty
            | SandboxVirtualDevicePath::Console
            | SandboxVirtualDevicePath::Ptmx
            | SandboxVirtualDevicePath::Fd1
            | SandboxVirtualDevicePath::Fd2,
        ) => Ok(()),
        Some(
            SandboxVirtualDevicePath::DevDirectory
            | SandboxVirtualDevicePath::FdDirectory
            | SandboxVirtualDevicePath::PtsDirectory
            | SandboxVirtualDevicePath::ShmDirectory
            | SandboxVirtualDevicePath::Stdin
            | SandboxVirtualDevicePath::Fd0,
        ) => Err(SandboxFsError::ReadOnly { path: path.clone() }),
        None => Err(SandboxFsError::NotFound { path: path.clone() }),
    }
}
