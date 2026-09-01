//! Process adapter that binds the Rollout component to a daemon executable.
//!
//! Rollout owns routes, policy, telemetry, and lifecycle. This crate contains
//! the narrow process mechanism needed by a daemon-hosted product, keeping the
//! daemon's service assembly independent of Rollout.

mod desired;
mod runtime;

pub use runtime::*;

#[derive(Debug, thiserror::Error)]
pub enum RolloutHostError {
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("network error at {address}: {detail}")]
    Network { address: String, detail: String },
    #[error("failed to parse rollout operation: {detail}")]
    Parse { detail: String },
    #[error("sinomo daemon is not running: {detail}")]
    NotRunning { detail: String },
    #[error("rollout host operation failed: {detail}")]
    External { detail: String },
}

impl From<sinomo_daemon::DaemonError> for RolloutHostError {
    fn from(error: sinomo_daemon::DaemonError) -> Self {
        Self::External {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_rollout::RolloutError> for RolloutHostError {
    fn from(error: sinomo_rollout::RolloutError) -> Self {
        Self::Io {
            path: "sinomo rollout".to_string(),
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_core::network::NetworkError> for RolloutHostError {
    fn from(error: sinomo_core::network::NetworkError) -> Self {
        Self::Network {
            address: "rollout transport".to_string(),
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_api::DaemonApiError> for RolloutHostError {
    fn from(error: sinomo_api::DaemonApiError) -> Self {
        Self::External {
            detail: error.to_string(),
        }
    }
}

use std::sync::Arc;

use sinomo_api::{DaemonPaths, SINOMO_APP_DAEMON_LAUNCH_ID_ENV};
use sinomo_module::{
    ComponentError, ComponentFactory, ComponentHostRole, ComponentScope, StaticComponentFactory,
};
use sinomo_rollout::{
    ClusterRolloutBinaryManifest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, RolloutHostFuture, RolloutServiceHost,
};

pub fn daemon_status_projection(
    data_dir_root: &std::path::Path,
) -> Result<Option<sinomo_module::ComponentStatusProjection>, String> {
    let paths = sinomo_rollout::SelfRolloutPaths::new(data_dir_root);
    let status = sinomo_rollout::read_rollout_status(&paths).map_err(|error| error.to_string())?;
    status
        .map(|status| {
            serde_json::to_value(status)
                .map(|value| sinomo_module::ComponentStatusProjection::new("sinomo.rollout", value))
                .map_err(|error| error.to_string())
        })
        .transpose()
}

pub fn recover_startup_state(data_dir_root: &std::path::Path) -> Result<(), String> {
    let data_dir = sinomo_core::local::SinomoDataDir::new(data_dir_root);
    runtime::recover_native_loader_intents_before_registry_load(&DaemonPaths::new(&data_dir))
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub fn component_factory(
    data_dir: &sinomo_core::local::SinomoDataDir,
    host_role: ComponentHostRole,
) -> Result<RolloutHostComponentFactory, sinomo_module::ComponentError> {
    let paths = match host_role {
        ComponentHostRole::Shadow => DaemonPaths::for_shadow(data_dir),
        ComponentHostRole::Main | ComponentHostRole::Embedded => DaemonPaths::new(data_dir),
    };
    let label = match host_role {
        ComponentHostRole::Shadow => "shadow-daemon",
        ComponentHostRole::Main => "daemon",
        ComponentHostRole::Embedded => "embedded-daemon",
    };
    let inner = sinomo_rollout::component_factory_with_telemetry(
        Arc::new(DaemonProcessRolloutHost { paths }),
        None,
        label,
    )?;
    Ok(RolloutHostComponentFactory { inner, host_role })
}

pub struct RolloutHostComponentFactory {
    inner: StaticComponentFactory,
    host_role: ComponentHostRole,
}

impl ComponentFactory for RolloutHostComponentFactory {
    fn descriptor(&self) -> &sinomo_module::ComponentDescriptor {
        self.inner.descriptor()
    }

    fn mount(
        &self,
        context: &sinomo_module::ComponentContext,
    ) -> Result<ComponentScope, ComponentError> {
        let mut scope = self.inner.mount(context)?;
        if matches!(
            self.host_role,
            ComponentHostRole::Main | ComponentHostRole::Embedded
        ) {
            let runtime = context
                .runtime()
                .ok_or_else(|| ComponentError::RuntimeUnavailable {
                    component_id: sinomo_rollout::SINOMO_ROLLOUT_MODULE_LEGACY_ID.to_string(),
                })?;
            let data_dir = context.data_dir_root();
            let shutdown = scope.shutdown_receiver();
            scope.own_task(
                "rollout shadow finalization",
                runtime.spawn(run_shadow_finalization_until_stopped(data_dir, shutdown)),
            );
        } else if self.host_role == ComponentHostRole::Shadow
            && sinomo_core::network::ClusterPaths::new(context.data_dir())
                .state
                .exists()
        {
            let runtime = context
                .runtime()
                .ok_or_else(|| ComponentError::RuntimeUnavailable {
                    component_id: sinomo_rollout::SINOMO_ROLLOUT_MODULE_LEGACY_ID.to_string(),
                })?;
            let data_dir = context.data_dir_root();
            let paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
            let cluster_paths = sinomo_core::network::ClusterPaths::new(context.data_dir());
            let shutdown = scope.shutdown_receiver();
            let task_runtime = runtime.clone();
            scope.own_task(
                "desired rollout reconciliation",
                runtime.spawn(desired::run_until_stopped(
                    paths,
                    cluster_paths,
                    task_runtime,
                    shutdown,
                )),
            );
        }
        Ok(scope)
    }

    fn register_storage_paths(
        &self,
        layout: &mut sinomo_core::storage::StorageLayout,
    ) -> Result<(), sinomo_core::storage::StorageContractError> {
        self.inner.register_storage_paths(layout)
    }

    fn register_config_definitions(
        &self,
        builder: sinomo_config::ConfigRegistryBuilder,
    ) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
        self.inner.register_config_definitions(builder)
    }
}

async fn run_shadow_finalization_until_stopped(
    data_dir_root: std::path::PathBuf,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let paths = DaemonPaths::with_root(data_dir_root.join("daemon"));
    loop {
        if *shutdown.borrow() {
            break;
        }
        let finalize_paths = paths.clone();
        match tokio::task::spawn_blocking(move || {
            runtime::handle_daemon_finalize_shadow_rollout(&finalize_paths)
        })
        .await
        {
            Ok(Ok(response)) if !response.starts_with("no pending shadow rollout") => {
                tracing::info!(response, "rollout shadow finalization advanced");
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "rollout shadow finalization failed"),
            Err(error) => tracing::warn!(%error, "rollout shadow finalization task failed"),
        }
        let delay = if maintenance_inhibited(&data_dir_root) {
            std::time::Duration::from_secs(15)
        } else {
            sinomo_core::network::CLUSTER_NETWORK_PUBLISH_INTERVAL
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

pub fn maintenance_inhibited(data_dir_root: &std::path::Path) -> bool {
    let paths = sinomo_rollout::SelfRolloutPaths::new(data_dir_root);
    sinomo_rollout::rollout_shadow_finalize_path(&paths).exists()
        || sinomo_rollout::rollout_main_activation_path(&paths).exists()
}

struct DaemonProcessRolloutHost {
    paths: DaemonPaths,
}

impl RolloutServiceHost for DaemonProcessRolloutHost {
    fn activate_binary<'a>(
        &'a self,
        manifest: ClusterRolloutBinaryManifest,
    ) -> RolloutHostFuture<'a, Result<ClusterRolloutBinaryResponse, String>> {
        let paths = self.paths.clone();
        Box::pin(async move {
            reject_persistent_rollout_from_app_owned_daemon()?;
            tokio::task::spawn_blocking(move || {
                runtime::activate_daemon_rollout_binary_manifest(&paths, manifest)
            })
            .await
            .map_err(|error| format!("rollout binary activation task failed: {error}"))?
            .map_err(|error| error.to_string())
        })
    }

    fn activate_native_dylib(
        &self,
        request: ClusterRolloutNativeDylibModuleRequest,
    ) -> Result<ClusterRolloutBinaryResponse, String> {
        runtime::handle_daemon_rollout_native_dylib_module_request(&self.paths, request)
            .map_err(|error| error.to_string())
    }

    fn build(
        &self,
        request: ClusterRolloutBuildRequest,
    ) -> Result<ClusterRolloutBinaryResponse, String> {
        reject_persistent_rollout_from_app_owned_daemon()?;
        runtime::handle_daemon_rollout_build_request(&self.paths, request)
            .map_err(|error| error.to_string())
    }

    fn finalize_shadow(&self) -> Result<String, String> {
        reject_persistent_rollout_from_app_owned_daemon()?;
        runtime::handle_daemon_finalize_shadow_rollout(&self.paths)
            .map_err(|error| error.to_string())
    }

    fn clear_status(&self) -> Result<String, String> {
        runtime::handle_daemon_clear_rollout_status(&self.paths).map_err(|error| error.to_string())
    }

    fn should_stop_after(&self, response: &ClusterRolloutBinaryResponse) -> bool {
        std::env::var_os(SINOMO_APP_DAEMON_LAUNCH_ID_ENV).is_none()
            && response.status == "accepted"
            && !runtime::daemon_paths_are_shadow(&self.paths)
    }
}

fn reject_persistent_rollout_from_app_owned_daemon() -> Result<(), String> {
    if std::env::var_os(SINOMO_APP_DAEMON_LAUNCH_ID_ENV).is_some() {
        return Err(
            "binary rollout is unavailable in an app-owned daemon because no detached shadow or recovery process may outlive the app"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod component_tests {
    use super::*;

    #[test]
    fn rollout_status_and_maintenance_state_are_owner_projections() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-host-status-projection");
        let paths = sinomo_rollout::SelfRolloutPaths::new(&root);
        sinomo_rollout::ensure_rollout_layout(&paths).expect("rollout layout");
        let status = sinomo_rollout::cluster_rollout_progress_status(
            sinomo_rollout::ClusterRolloutProgressUpdate::new(
                "accepted",
                "cluster-remote-build",
                None,
                "test",
                "2026-09-01T00:00:00Z",
                "shadow-finalized",
                sinomo_rollout::CLUSTER_ROLLOUT_PROGRESS_TOTAL,
                "accepted",
            ),
        );
        sinomo_rollout::write_rollout_status(&paths, &status).expect("rollout status");

        let projection = daemon_status_projection(root.path())
            .expect("status projection")
            .expect("present status projection");
        assert_eq!(projection.key, "sinomo.rollout");
        let projected: sinomo_core::ClusterNodeRolloutStatus =
            serde_json::from_value(projection.value).expect("decode projection");
        assert_eq!(projected.status, "accepted");
        assert!(!maintenance_inhibited(root.path()));

        std::fs::write(
            sinomo_rollout::rollout_main_activation_path(&paths),
            b"pending",
        )
        .expect("pending activation marker");
        assert!(maintenance_inhibited(root.path()));
    }
}
