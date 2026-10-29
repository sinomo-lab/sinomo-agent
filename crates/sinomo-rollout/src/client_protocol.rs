use crate::{
    ClusterRolloutBinaryRequest, ClusterRolloutBuildRequest, ROLLOUT_BINARY_ROUTE,
    ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, RolloutError,
};
use sinomo_core::network::{ObjectRpcBodySource, ObjectRpcCall, ObjectRpcRequest};
use std::path::Path;

/// Builds one retry-safe whole-artifact rollout call after revalidating that
/// the prepared attachment still matches its manifest length.
pub fn rollout_binary_rpc_call(
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<ObjectRpcCall, RolloutError> {
    let artifact = request
        .manifest
        .artifact(&request.artifact_id)
        .ok_or_else(|| RolloutError::MissingArtifact {
            artifact: request.artifact_id.clone(),
        })?;
    let attachment = ObjectRpcBodySource::file_with_streaming_cache(attachment_path.to_path_buf())
        .map_err(|error| RolloutError::Io {
            path: attachment_path.display().to_string(),
            detail: error.to_string(),
        })?;
    if attachment.total_bytes() != Some(artifact.total_bytes) {
        return Err(RolloutError::Io {
            path: attachment_path.display().to_string(),
            detail: format!(
                "rollout artifact size changed after preparation: expected {}, found {}",
                artifact.total_bytes,
                attachment.total_bytes().unwrap_or_default()
            ),
        });
    }
    let head = ObjectRpcRequest::encode_body(ROLLOUT_BINARY_ROUTE, ROLLOUT_RPC_CATEGORY, request)
        .map_err(rollout_protocol_error)?
        .with_operation_id(format!(
            "{}:{}",
            request.manifest.upload_id, request.artifact_id
        ))
        .with_response_retry_safe();
    Ok(ObjectRpcCall::new(head, attachment))
}

/// Moves an inline source bundle into the stream-native request body while
/// preserving an explicitly present empty bundle in the typed request head.
pub fn rollout_build_rpc_call(
    mut request: ClusterRolloutBuildRequest,
) -> Result<ObjectRpcCall, RolloutError> {
    let operation_id = format!("rollout-build:{}", request.requested_at);
    let source_bundle = match request.source_bundle.take() {
        Some(source_bundle) if !source_bundle.is_empty() => source_bundle,
        Some(source_bundle) => {
            request.source_bundle = Some(source_bundle);
            Vec::new()
        }
        None => Vec::new(),
    };
    let head = ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, &request)
        .map_err(rollout_protocol_error)?
        .with_operation_id(operation_id);
    Ok(ObjectRpcCall::new(
        head,
        ObjectRpcBodySource::bytes(source_bundle),
    ))
}

fn rollout_protocol_error(error: impl std::fmt::Display) -> RolloutError {
    RolloutError::InvalidArtifact {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_request(source_bundle: Option<Vec<u8>>) -> ClusterRolloutBuildRequest {
        ClusterRolloutBuildRequest {
            source: "test".to_string(),
            remote_repo: None,
            source_bundle,
            source_head: None,
            force_source_overwrite: false,
            host: "127.0.0.1".to_string(),
            host_override: None,
            port: 21_353,
            shadow_port: 21_354,
            requested_at: "2026-08-20T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn build_call_moves_nonempty_bundle_out_of_the_typed_head() {
        let call =
            rollout_build_rpc_call(build_request(Some(vec![1, 2, 3]))).expect("rollout build call");
        let decoded: ClusterRolloutBuildRequest = call.request.decode_body().expect("request head");
        assert!(decoded.source_bundle.is_none());
        assert_eq!(call.body.total_bytes(), Some(3));
        assert_eq!(
            call.request.operation_id(),
            Some("rollout-build:2026-08-20T00:00:00Z")
        );
    }

    #[test]
    fn explicitly_empty_bundle_remains_distinguishable_from_absent() {
        let call =
            rollout_build_rpc_call(build_request(Some(Vec::new()))).expect("rollout build call");
        let decoded: ClusterRolloutBuildRequest = call.request.decode_body().expect("request head");
        assert_eq!(decoded.source_bundle, Some(Vec::new()));
        assert_eq!(call.body.total_bytes(), Some(0));
    }
}
