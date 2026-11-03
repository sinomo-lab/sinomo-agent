use crate::{
    BUILD_ARTIFACT_ROUTE, BUILD_RPC_CATEGORY, BUILD_SUBMIT_ROUTE, BuildArtifactRequest,
    BuildArtifactResponse, BuildError, BuildSubmitRequest, Result, verify_artifact_file,
};
use sinomo_core::network::{ObjectRpcBodySource, ObjectRpcCall, ObjectRpcRequest, ObjectRpcResult};
use std::path::Path;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Builds a retry-safe submission call and binds its planned bundle length to
/// the file attachment opened by the client.
pub fn build_submit_rpc_call(
    request: &BuildSubmitRequest,
    source_bundle: Option<&Path>,
) -> Result<ObjectRpcCall> {
    let body = match source_bundle {
        Some(path) => {
            let body =
                ObjectRpcBodySource::file(path.to_path_buf()).map_err(|error| BuildError::Io {
                    path: path.display().to_string(),
                    detail: error.to_string(),
                })?;
            if body.total_bytes() != Some(request.plan.source.bundle_bytes) {
                return Err(BuildError::Io {
                    path: path.display().to_string(),
                    detail: format!(
                        "build source bundle size changed after planning: expected {}, found {}",
                        request.plan.source.bundle_bytes,
                        body.total_bytes().unwrap_or_default()
                    ),
                });
            }
            body
        }
        None => ObjectRpcBodySource::default(),
    };
    let head = ObjectRpcRequest::encode_body(BUILD_SUBMIT_ROUTE, BUILD_RPC_CATEGORY, request)
        .map_err(build_protocol_error)?
        .with_operation_id(format!("build-submit:{}", request.plan.key.path_key()))
        .with_response_retry_safe();
    Ok(ObjectRpcCall::new(head, body))
}

pub fn build_artifact_rpc_call(request: &BuildArtifactRequest) -> Result<ObjectRpcCall> {
    let head = ObjectRpcRequest::encode_body(BUILD_ARTIFACT_ROUTE, BUILD_RPC_CATEGORY, request)
        .map_err(build_protocol_error)?
        .with_operation_id(format!(
            "build-artifact:{}:{}",
            request.build_key.path_key(),
            request.artifact_id
        ))
        .with_response_retry_safe();
    Ok(ObjectRpcCall::new(head, ObjectRpcBodySource::default()))
}

/// Receives, verifies, and atomically installs one build artifact response.
///
/// Transport selection remains a runtime-host concern. Artifact identity,
/// provenance, length, hash, mode, and durable destination semantics remain
/// owned by the build domain regardless of which host transported the body.
pub async fn receive_build_artifact_rpc_result(
    mut result: ObjectRpcResult,
    request: &BuildArtifactRequest,
    destination_path: &Path,
) -> Result<BuildArtifactResponse> {
    let response: BuildArtifactResponse = result
        .response
        .decode_body()
        .map_err(build_protocol_error)?;
    if response.artifact.artifact_id != request.artifact_id {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "build artifact response id {} does not match requested {}",
                response.artifact.artifact_id, request.artifact_id
            ),
        });
    }
    if response.artifact.provenance.build_key != request.build_key {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "build artifact response key {} does not match requested {}",
                response.artifact.provenance.build_key, request.build_key
            ),
        });
    }
    let parent = destination_path.parent().ok_or_else(|| BuildError::Io {
        path: destination_path.display().to_string(),
        detail: "artifact destination has no parent directory".to_string(),
    })?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|source| BuildError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    let partial = destination_path.with_extension(format!(
        "partial-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let mut output = tokio::fs::File::create(&partial)
        .await
        .map_err(|source| BuildError::Io {
            path: partial.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut received = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = result
            .body
            .read(&mut buffer)
            .await
            .map_err(|source| BuildError::Io {
                path: partial.display().to_string(),
                detail: format!("receive build artifact body: {source}"),
            })?;
        if count == 0 {
            break;
        }
        received = received.saturating_add(count as u64);
        if received > response.artifact.size_bytes {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "build artifact {} exceeded declared size {}",
                    response.artifact.artifact_id, response.artifact.size_bytes
                ),
            });
        }
        if let Err(source) = output.write_all(&buffer[..count]).await {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(BuildError::Io {
                path: partial.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    if let Err(source) = output.flush().await {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(BuildError::Io {
            path: partial.display().to_string(),
            detail: source.to_string(),
        });
    }
    if let Err(source) = output.sync_all().await {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(BuildError::Io {
            path: partial.display().to_string(),
            detail: source.to_string(),
        });
    }
    drop(output);
    if let Err(error) = verify_artifact_file(&response.artifact, &partial) {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(error);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if let Err(source) = tokio::fs::set_permissions(
            &partial,
            std::fs::Permissions::from_mode(0o755),
        )
        .await
        {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(BuildError::Io {
                path: partial.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    if destination_path.exists() {
        tokio::fs::remove_file(destination_path)
            .await
            .map_err(|source| BuildError::Io {
                path: destination_path.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    tokio::fs::rename(&partial, destination_path)
        .await
        .map_err(|source| BuildError::Io {
            path: format!("{} -> {}", partial.display(), destination_path.display()),
            detail: source.to_string(),
        })?;
    Ok(response)
}

fn build_protocol_error(error: impl std::fmt::Display) -> BuildError {
    BuildError::InvalidValue {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_call_binds_identity_and_is_retry_safe() {
        let request = BuildArtifactRequest {
            build_key: crate::BuildKey::new(format!("blake3:{}", "a".repeat(64)))
                .expect("build key"),
            artifact_id: "artifact-client-protocol".to_string(),
        };

        let call = build_artifact_rpc_call(&request).expect("artifact call");
        let decoded: BuildArtifactRequest = call.request.decode_body().expect("request head");

        assert_eq!(decoded, request);
        assert_eq!(call.request.route, BUILD_ARTIFACT_ROUTE);
        assert_eq!(
            call.request.operation_id(),
            Some(format!(
                "build-artifact:{}:{}",
                request.build_key.path_key(),
                request.artifact_id
            ))
            .as_deref()
        );
        assert!(call.request.is_response_retry_safe());
        assert!(call.body.is_empty());
    }
}
