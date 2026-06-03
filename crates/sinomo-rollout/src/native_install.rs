use crate::{
    ClusterRolloutNativeDylibModuleArtifact, LocalRolloutNativeDylibModuleArtifact, RolloutError,
};
use std::path::Path;

pub fn install_local_rollout_native_dylib_modules(
    data_dir: &Path,
    artifacts: &[LocalRolloutNativeDylibModuleArtifact],
) -> Result<Vec<sinomo_module::NativeDylibModuleInstallResult>, RolloutError> {
    artifacts
        .iter()
        .map(|artifact| {
            sinomo_module::install_native_dylib_module_from_file(
                data_dir,
                &artifact.built_library,
                native_dylib_module_install_request(
                    &artifact.module_id,
                    &artifact.interface_version,
                    &artifact.cache_key,
                    &artifact.library_name,
                    &artifact.source,
                )?,
            )
            .map_err(Into::into)
        })
        .collect()
}

pub fn install_cluster_rollout_native_dylib_modules(
    data_dir: &Path,
    artifacts: &[ClusterRolloutNativeDylibModuleArtifact],
) -> Result<Vec<sinomo_module::NativeDylibModuleInstallResult>, RolloutError> {
    artifacts
        .iter()
        .map(|artifact| {
            sinomo_module::install_native_dylib_module_from_bytes(
                data_dir,
                &artifact.library,
                native_dylib_module_install_request(
                    &artifact.module_id,
                    &artifact.interface_version,
                    &artifact.cache_key,
                    &artifact.library_name,
                    &artifact.source,
                )?,
            )
            .map_err(Into::into)
        })
        .collect()
}

pub fn native_dylib_module_install_request(
    module_id: &str,
    interface_version: &str,
    cache_key: &str,
    library_name: &str,
    source: &str,
) -> Result<sinomo_module::NativeDylibModuleInstallRequest, RolloutError> {
    let module_id = sinomo_module::ModuleId::new(module_id.to_string())
        .or_else(|_| sinomo_module::ModuleId::from_legacy_named(module_id))
        .map_err(|error| RolloutError::InvalidArtifact {
            detail: format!("invalid native dylib module id `{module_id}`: {error}"),
        })?;
    Ok(sinomo_module::NativeDylibModuleInstallRequest {
        expected_module_id: Some(module_id),
        artifact_id: None,
        expected_interface_version: Some(interface_version.to_string()),
        cache_key: Some(cache_key.to_string()),
        library_name: Some(library_name.to_string()),
        source: source.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_dylib_module_install_request_accepts_legacy_module_ids() {
        let request = native_dylib_module_install_request(
            "module.sandbox.wasix",
            "sinomo-sandbox-wasix-v1",
            "sandbox-wasix",
            "libsinomo_sandbox_wasix_module.so",
            "modules/sandbox/wasix",
        )
        .expect("legacy module id should parse");

        assert_eq!(
            request
                .expected_module_id
                .as_ref()
                .expect("module id should be present"),
            &sinomo_module::ModuleId::from_legacy_named("module.sandbox.wasix")
                .expect("expected legacy module id should parse")
        );
        assert_eq!(
            request.expected_interface_version.as_deref(),
            Some("sinomo-sandbox-wasix-v1")
        );
        assert_eq!(request.cache_key.as_deref(), Some("sandbox-wasix"));
    }
}
