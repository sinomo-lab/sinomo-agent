use sinomo_core::network::{NetworkError, ObjectRpcIncomingBody};
use sinomo_core::service::ServiceBodyMode;
use sinomo_module_api::NativeModuleStagedBody;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const NATIVE_MODULE_BODY_BUFFER_BYTES: usize = 256 * 1024;

/// Cancellation seam used while the module owner drains a host-provided body.
///
/// Process-specific operation registries implement this hook; staging remains
/// independent from the daemon that happens to host the module invocation.
pub trait NativeModuleOperationCancellation {
    fn is_cancelled(&self) -> bool;
}

/// Private, invocation-scoped files used to cross the native module ABI.
///
/// The directory is removed when the guard is dropped. A response body can
/// retain this guard through the network body-source lifetime.
#[derive(Debug)]
pub struct NativeModuleSessionStaging {
    root: PathBuf,
    request_body_path: Option<PathBuf>,
    response_body_path: Option<PathBuf>,
}

impl NativeModuleSessionStaging {
    pub fn create(
        data_dir_root: &Path,
        request_body_mode: ServiceBodyMode,
        response_body_mode: ServiceBodyMode,
    ) -> Result<Self, NetworkError> {
        let runtime_root = data_dir_root.join("module").join("runtime");
        std::fs::create_dir_all(&runtime_root).map_err(|source| NetworkError::Io {
            operation: format!(
                "create native dylib module runtime root {}",
                runtime_root.display()
            ),
            detail: source.to_string(),
        })?;
        let parent = runtime_root.join("native-dylib-sessions");
        sinomo_core::storage::durable_file::ensure_private_directory(&parent).map_err(
            |source| NetworkError::Io {
                operation: format!(
                    "create private native dylib session staging root {}",
                    parent.display()
                ),
                detail: source.to_string(),
            },
        )?;

        let root = (0..8)
            .find_map(|_| {
                let candidate = parent.join(sinomo_core::Id::generate().as_str());
                match sinomo_core::storage::durable_file::create_private_directory(&candidate) {
                    Ok(()) => Some(Ok(candidate)),
                    Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => None,
                    Err(source) => Some(Err(NetworkError::Io {
                        operation: format!(
                            "create native dylib session staging {}",
                            candidate.display()
                        ),
                        detail: source.to_string(),
                    })),
                }
            })
            .transpose()?
            .ok_or_else(|| NetworkError::Io {
                operation: "create native dylib session staging".to_string(),
                detail: "could not allocate a unique private staging directory".to_string(),
            })?;

        let request_body_path =
            (request_body_mode != ServiceBodyMode::None).then(|| root.join("request.body"));
        let response_body_path =
            (response_body_mode != ServiceBodyMode::None).then(|| root.join("response.body"));
        // Install the cleanup guard before creating either file so partial
        // initialization cannot leave a private session behind.
        let staging = Self {
            root,
            request_body_path,
            response_body_path,
        };
        for path in staging
            .request_body_path
            .iter()
            .chain(staging.response_body_path.iter())
        {
            sinomo_core::storage::durable_file::create_private_staged_file(path).map_err(
                |source| NetworkError::Io {
                    operation: format!("create private native dylib body file {}", path.display()),
                    detail: source.to_string(),
                },
            )?;
        }
        Ok(staging)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn request_body_path(&self) -> Option<&Path> {
        self.request_body_path.as_deref()
    }

    pub fn response_body_path(&self) -> Option<&Path> {
        self.response_body_path.as_deref()
    }
}

impl Drop for NativeModuleSessionStaging {
    fn drop(&mut self) {
        if let Err(source) = std::fs::remove_dir_all(&self.root)
            && source.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.root.display(),
                error = %source,
                "failed to clean native dylib session staging"
            );
        }
    }
}

pub fn native_module_staged_body_path(path: &Path) -> Result<String, NetworkError> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!(
                "native dylib session staging path is not valid UTF-8: {}",
                path.display()
            ),
        })
}

pub async fn stage_native_module_request_body<C>(
    mut body: ObjectRpcIncomingBody,
    path: &Path,
    cancellation: &C,
) -> Result<NativeModuleStagedBody, NetworkError>
where
    C: NativeModuleOperationCancellation + ?Sized,
{
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|source| NetworkError::Io {
            operation: format!("open native dylib request body staging {}", path.display()),
            detail: source.to_string(),
        })?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; NATIVE_MODULE_BODY_BUFFER_BYTES];
    loop {
        if cancellation.is_cancelled() {
            return Err(NetworkError::Io {
                operation: format!("stage native dylib request body for {}", path.display()),
                detail: "native dylib operation was cancelled while receiving its request body"
                    .to_string(),
            });
        }
        let read = body
            .read(&mut buffer)
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("stream native dylib request body to {}", path.display()),
                detail: source.to_string(),
            })?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("write native dylib request body staging {}", path.display()),
                detail: source.to_string(),
            })?;
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| NetworkError::Serialization {
                detail: "native dylib request body length exceeds u64".to_string(),
            })?;
    }
    file.flush().await.map_err(|source| NetworkError::Io {
        operation: format!("flush native dylib request body staging {}", path.display()),
        detail: source.to_string(),
    })?;
    drop(file);
    Ok(NativeModuleStagedBody::new(
        native_module_staged_body_path(path)?,
        length,
        format!("blake3:{}", hasher.finalize().to_hex()),
    ))
}

pub fn inspect_native_module_staged_body(
    path: &Path,
) -> Result<NativeModuleStagedBody, NetworkError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| NetworkError::Io {
        operation: format!("inspect native dylib response body {}", path.display()),
        detail: source.to_string(),
    })?;
    if !metadata.file_type().is_file() {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body is not a regular file: {}",
                path.display()
            ),
        });
    }
    let mut file = std::fs::File::open(path).map_err(|source| NetworkError::Io {
        operation: format!("open native dylib response body {}", path.display()),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; NATIVE_MODULE_BODY_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|source| NetworkError::Io {
            operation: format!("hash native dylib response body {}", path.display()),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| NetworkError::Serialization {
                detail: "native dylib response body length exceeds u64".to_string(),
            })?;
    }
    Ok(NativeModuleStagedBody::new(
        native_module_staged_body_path(path)?,
        length,
        format!("blake3:{}", hasher.finalize().to_hex()),
    ))
}

pub fn validate_native_module_response_body(
    mode: ServiceBodyMode,
    expected_path: Option<&Path>,
    reported: Option<NativeModuleStagedBody>,
) -> Result<Option<PathBuf>, NetworkError> {
    let Some(reported) = reported else {
        return if mode == ServiceBodyMode::Required {
            Err(NetworkError::Serialization {
                detail: "native dylib route requires a non-empty response body".to_string(),
            })
        } else {
            Ok(None)
        };
    };
    if mode == ServiceBodyMode::None {
        return Err(NetworkError::Serialization {
            detail: "native dylib route does not declare a response body".to_string(),
        });
    }
    let expected_path = expected_path.ok_or_else(|| NetworkError::Serialization {
        detail: "native dylib response body has no host-assigned staging path".to_string(),
    })?;
    if Path::new(&reported.path) != expected_path {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body path `{}` does not match host-assigned path `{}`",
                reported.path,
                expected_path.display()
            ),
        });
    }
    let actual = inspect_native_module_staged_body(expected_path)?;
    if reported.length != actual.length {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body length mismatch: module reported {}, host measured {}",
                reported.length, actual.length
            ),
        });
    }
    if reported.blake3 != actual.blake3 {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib response body BLAKE3 mismatch: module reported {}, host measured {}",
                reported.blake3, actual.blake3
            ),
        });
    }
    if mode == ServiceBodyMode::Required && actual.length == 0 {
        return Err(NetworkError::Serialization {
            detail: "native dylib route requires a non-empty response body".to_string(),
        });
    }
    Ok(Some(expected_path.to_path_buf()))
}

pub fn validate_native_module_request_body_mode(
    route: &str,
    mode: ServiceBodyMode,
    has_body: bool,
) -> Result<(), NetworkError> {
    match mode {
        ServiceBodyMode::None if has_body => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} does not accept a request body"),
        }),
        ServiceBodyMode::Required if !has_body => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} requires a non-empty request body"),
        }),
        _ => Ok(()),
    }
}

pub fn validate_native_module_staged_request_body(
    route: &str,
    mode: ServiceBodyMode,
    body: Option<&NativeModuleStagedBody>,
) -> Result<(), NetworkError> {
    match (mode, body) {
        (ServiceBodyMode::None, Some(_)) => Err(NetworkError::Serialization {
            detail: format!("native dylib route {route} does not accept a request body"),
        }),
        (ServiceBodyMode::Required, None)
        | (ServiceBodyMode::Required, Some(NativeModuleStagedBody { length: 0, .. })) => {
            Err(NetworkError::Serialization {
                detail: format!("native dylib route {route} requires a non-empty request body"),
            })
        }
        _ => Ok(()),
    }
}
