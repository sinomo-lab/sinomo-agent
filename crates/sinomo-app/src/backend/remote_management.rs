//! Local remote-management permission and module state for Settings.
//!
//! Permission mutation is deliberately host-local. It must never be routed
//! through the daemon or configuration service because that would let a remote
//! caller grant the authority required to manage the receiving host.

use sinomo_runtime::module::ModuleRuntimeConfigSource;
use sinomo_runtime::node::{LocalNodePermissionStore, LocalNodePermissions};

use crate::backend::Backend;
use crate::backend::modules::{
    ModuleConfigSnapshot, ModuleObservedState, module_config_setting, module_ids_match,
};

pub const REMOTE_MANAGEMENT_ALLOW_COMMAND: &str = "sinomo node permissions allow remote-management";
pub const REMOTE_MANAGEMENT_MODULE_ID: &str = "module.remote-management";
pub const REMOTE_MANAGEMENT_DENY_COMMAND: &str = "sinomo node permissions deny remote-management";
pub const EXTERNAL_INTEGRATIONS_ALLOW_COMMAND: &str =
    "sinomo node permissions allow external-integrations";
pub const EXTERNAL_INTEGRATIONS_DENY_COMMAND: &str =
    "sinomo node permissions deny external-integrations";

pub struct RemoteManagementPermissionMutation {
    pub permissions: LocalNodePermissions,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteManagementSettingsSnapshot {
    pub permission_loading: bool,
    pub permission_enabled: bool,
    pub external_integrations_enabled: bool,
    pub permission_error: Option<String>,
    pub resource_exposure_count: usize,
    pub resource_operation_count: usize,
    pub module_loading: bool,
    pub module_node_id: Option<String>,
    pub module_node_label: String,
    pub module_override: Option<bool>,
    pub module_effective_enabled: bool,
    pub module_effective_source: ModuleRuntimeConfigSource,
    pub module_observed: ModuleObservedState,
    pub module_observed_detail: String,
    pub module_error: Option<String>,
}

impl Default for RemoteManagementSettingsSnapshot {
    fn default() -> Self {
        Self {
            permission_loading: true,
            permission_enabled: false,
            external_integrations_enabled: false,
            permission_error: None,
            resource_exposure_count: 0,
            resource_operation_count: 0,
            module_loading: true,
            module_node_id: None,
            module_node_label: "This node".to_string(),
            module_override: None,
            module_effective_enabled: false,
            module_effective_source: ModuleRuntimeConfigSource::DisabledDefault,
            module_observed: ModuleObservedState::Unavailable,
            module_observed_detail: "Module state is loading".to_string(),
            module_error: None,
        }
    }
}

impl RemoteManagementSettingsSnapshot {
    pub fn replace_permission_state(&mut self, source: &Self) {
        self.permission_loading = source.permission_loading;
        self.permission_enabled = source.permission_enabled;
        self.external_integrations_enabled = source.external_integrations_enabled;
        self.permission_error.clone_from(&source.permission_error);
        self.resource_exposure_count = source.resource_exposure_count;
        self.resource_operation_count = source.resource_operation_count;
    }

    pub fn replace_permission_document(&mut self, permissions: &LocalNodePermissions) {
        self.permission_loading = false;
        self.permission_enabled = permissions.remote_management_enabled();
        self.external_integrations_enabled = permissions.external_integrations_enabled();
        self.permission_error = None;
        self.resource_exposure_count = permissions.resource_exposures().len();
        self.resource_operation_count = permissions
            .resource_exposures()
            .values()
            .map(|exposure| exposure.operations().len())
            .sum();
    }

    pub fn replace_module_state(&mut self, source: &Self) {
        self.module_loading = source.module_loading;
        self.module_node_id.clone_from(&source.module_node_id);
        self.module_node_label.clone_from(&source.module_node_label);
        self.module_override = source.module_override;
        self.module_effective_enabled = source.module_effective_enabled;
        self.module_effective_source = source.module_effective_source;
        self.module_observed = source.module_observed;
        self.module_observed_detail
            .clone_from(&source.module_observed_detail);
        self.module_error.clone_from(&source.module_error);
    }
}

impl Backend {
    pub fn remote_management_settings_snapshot(&self) -> RemoteManagementSettingsSnapshot {
        let module_snapshot = self.module_config_snapshot(None);
        self.remote_management_settings_snapshot_from_modules(&module_snapshot)
    }

    pub fn remote_management_settings_snapshot_from_modules(
        &self,
        module_snapshot: &ModuleConfigSnapshot,
    ) -> RemoteManagementSettingsSnapshot {
        if self.settings_node_is_remote() {
            return self.remote_management_snapshot_from_parts(false, false,
                Some("Host-access consent must be reviewed and changed locally on the selected node.".into()),
                0, 0, module_snapshot);
        }
        let permission_store = LocalNodePermissionStore::new(self.data_dir.root());
        match permission_store.load() {
            Ok(permissions) => self.remote_management_settings_snapshot_from_permissions(
                &permissions,
                module_snapshot,
            ),
            Err(error) => self.remote_management_snapshot_from_parts(
                false,
                false,
                Some(error.to_string()),
                0,
                0,
                module_snapshot,
            ),
        }
    }

    pub fn remote_management_settings_snapshot_from_permissions(
        &self,
        permissions: &LocalNodePermissions,
        module_snapshot: &ModuleConfigSnapshot,
    ) -> RemoteManagementSettingsSnapshot {
        self.remote_management_snapshot_from_parts(
            permissions.remote_management_enabled(),
            permissions.external_integrations_enabled(),
            None,
            permissions.resource_exposures().len(),
            permissions
                .resource_exposures()
                .values()
                .map(|exposure| exposure.operations().len())
                .sum(),
            module_snapshot,
        )
    }

    pub fn set_local_remote_management_permission(
        &self,
        enabled: bool,
    ) -> Result<RemoteManagementPermissionMutation, String> {
        if self.settings_node_is_remote() {
            return Err("Host-access consent must be changed locally on the selected node.".into());
        }
        let store = LocalNodePermissionStore::new(self.data_dir.root());
        store
            .mutate(|permissions| Ok(permissions.set_remote_management_enabled(enabled)))
            .map(
                |(permissions, changed)| RemoteManagementPermissionMutation {
                    permissions,
                    changed,
                },
            )
            .map_err(|error| error.to_string())
    }

    pub fn set_local_external_integrations_permission(
        &self,
        enabled: bool,
    ) -> Result<RemoteManagementPermissionMutation, String> {
        if self.settings_node_is_remote() {
            return Err("Host-access consent must be changed locally on the selected node.".into());
        }
        let store = LocalNodePermissionStore::new(self.data_dir.root());
        store
            .mutate(|permissions| Ok(permissions.set_external_integrations_enabled(enabled)))
            .map(
                |(permissions, changed)| RemoteManagementPermissionMutation {
                    permissions,
                    changed,
                },
            )
            .map_err(|error| error.to_string())
    }

    pub fn set_local_remote_management_module_override(
        &self,
        enabled: Option<bool>,
    ) -> Result<(), String> {
        if let Some(node_id) = self.settings_node_id.as_deref() {
            self.set_node_module_override(node_id, REMOTE_MANAGEMENT_MODULE_ID, enabled, false)
        } else {
            self.set_local_module_override(REMOTE_MANAGEMENT_MODULE_ID, enabled)
        }
    }

    fn remote_management_snapshot_from_parts(
        &self,
        permission_enabled: bool,
        external_integrations_enabled: bool,
        permission_error: Option<String>,
        resource_exposure_count: usize,
        resource_operation_count: usize,
        module_snapshot: &ModuleConfigSnapshot,
    ) -> RemoteManagementSettingsSnapshot {
        let selected_node = module_snapshot.selected_node.as_ref();
        let module = selected_node.and_then(|_| {
            module_snapshot
                .modules
                .iter()
                .find(|module| module_ids_match(&module.module_id, REMOTE_MANAGEMENT_MODULE_ID))
        });
        let mut module_errors = module_snapshot.warnings.clone();
        if module_snapshot.selected_node.is_some() && selected_node.is_none() {
            module_errors
                .push("module configuration is unavailable on the selected node".to_string());
        }
        if let Some(error) = selected_node.and_then(|node| node.error.clone()) {
            module_errors.push(error);
        }
        if module.is_none() {
            module_errors.push(
                "remote-management module is missing from the selected node's catalog".to_string(),
            );
        }

        RemoteManagementSettingsSnapshot {
            permission_loading: false,
            permission_enabled,
            external_integrations_enabled,
            permission_error,
            resource_exposure_count,
            resource_operation_count,
            module_loading: false,
            module_node_id: selected_node.map(|node| node.summary.node_id.clone()),
            module_node_label: selected_node
                .map(|node| node.summary.display_name.clone())
                .unwrap_or_else(|| "This node".to_string()),
            module_override: selected_node.and_then(|node| {
                module_config_setting(&node.config, REMOTE_MANAGEMENT_MODULE_ID)
                    .map(|setting| setting.enabled)
            }),
            module_effective_enabled: module.is_some_and(|module| module.effective_enabled),
            module_effective_source: module
                .map(|module| module.effective_source)
                .unwrap_or(ModuleRuntimeConfigSource::DisabledDefault),
            module_observed: module
                .map(|module| module.observed)
                .unwrap_or(ModuleObservedState::Unavailable),
            module_observed_detail: module
                .map(|module| module.observed_detail.clone())
                .filter(|detail| !detail.trim().is_empty())
                .unwrap_or_else(|| "The selected node has not reported module state".to_string()),
            module_error: (!module_errors.is_empty()).then(|| module_errors.join("; ")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_permission_mutation_is_live_and_preserves_resource_exposures() {
        let backend = Backend::local_fallback();
        let store = LocalNodePermissionStore::new(backend.data_dir.root());
        store
            .mutate(|permissions| permissions.expose_resource_operation("camera:front", "observe"))
            .expect("seed resource exposure");

        let initial = backend.remote_management_settings_snapshot();
        assert!(!initial.permission_enabled);
        assert_eq!(initial.resource_exposure_count, 1);
        assert_eq!(initial.resource_operation_count, 1);

        let allowed_mutation = backend
            .set_local_remote_management_permission(true)
            .expect("allow remote management");
        assert!(allowed_mutation.changed);
        assert!(allowed_mutation.permissions.remote_management_enabled());
        let allowed = backend.remote_management_settings_snapshot();
        assert!(allowed.permission_enabled);
        assert_eq!(allowed.resource_operation_count, 1);

        assert!(
            backend
                .set_local_remote_management_permission(false)
                .expect("deny remote management")
                .changed
        );
        let denied = backend.remote_management_settings_snapshot();
        assert!(!denied.permission_enabled);
        assert_eq!(denied.resource_operation_count, 1);
    }

    #[test]
    fn external_integrations_permission_is_live_and_independent() {
        let backend = Backend::local_fallback();

        let initial = backend.remote_management_settings_snapshot();
        assert!(!initial.external_integrations_enabled);
        assert!(!initial.permission_enabled);

        let mutation = backend
            .set_local_external_integrations_permission(true)
            .expect("allow external integrations");
        assert!(mutation.changed);
        assert!(mutation.permissions.external_integrations_enabled());
        assert!(!mutation.permissions.remote_management_enabled());

        let allowed = backend.remote_management_settings_snapshot();
        assert!(allowed.external_integrations_enabled);
        assert!(!allowed.permission_enabled);

        assert!(
            backend
                .set_local_external_integrations_permission(false)
                .expect("deny external integrations")
                .changed
        );
        assert!(
            !backend
                .remote_management_settings_snapshot()
                .external_integrations_enabled
        );
    }

    #[test]
    fn local_module_override_targets_remote_management_only() {
        let backend = Backend::local_fallback();
        backend
            .set_local_remote_management_module_override(Some(true))
            .expect("set local override");

        let snapshot = backend.remote_management_settings_snapshot();
        assert_eq!(snapshot.module_override, Some(true));
        assert!(snapshot.module_effective_enabled);
        assert_eq!(
            snapshot.module_effective_source,
            ModuleRuntimeConfigSource::Node
        );
    }

    #[test]
    fn unreadable_permission_document_is_reported_as_unavailable() {
        let backend = Backend::local_fallback();
        let store = LocalNodePermissionStore::new(backend.data_dir.root());
        std::fs::create_dir_all(store.path().parent().expect("permission parent"))
            .expect("create permission parent");
        std::fs::write(store.path(), b"not-a-sinomo-object")
            .expect("write malformed permission document");

        let snapshot = backend.remote_management_settings_snapshot();
        assert!(!snapshot.permission_enabled);
        assert!(snapshot.permission_error.is_some());
    }

    #[test]
    fn unreadable_native_loader_state_is_not_presented_as_inactive() {
        let backend = Backend::local_fallback();
        let state_dir =
            sinomo_runtime::module::native_dylib_module_loader_state_dir(&backend.data_dir.root());
        std::fs::create_dir_all(&state_dir).expect("create native loader state directory");
        std::fs::write(state_dir.join("corrupt.sobj"), b"not-a-sinomo-object")
            .expect("write malformed loader state");

        let snapshot = backend.remote_management_settings_snapshot();
        assert_eq!(snapshot.module_observed, ModuleObservedState::Unavailable);
        assert!(
            snapshot
                .module_error
                .as_deref()
                .is_some_and(|error| error.contains("loader observations unavailable"))
        );
    }

    #[test]
    fn persisted_native_loader_record_is_not_claimed_as_running() {
        let backend = Backend::local_fallback();
        let module_id =
            sinomo_runtime::module::ModuleId::from_legacy_named(REMOTE_MANAGEMENT_MODULE_ID)
                .expect("remote-management module id");
        sinomo_runtime::module::write_native_dylib_module_loader_state(
            &backend.data_dir.root(),
            &sinomo_runtime::module::NativeDylibModuleLoaderState {
                module_id,
                artifact_id: None,
                content_hash: "00".repeat(32),
                replaces_content_hash: None,
                cache_key: "remote/management".to_string(),
                library_name: "remote-management.test".to_string(),
                platform_triple: format!(
                    "{}-{}-{}",
                    std::env::consts::ARCH,
                    std::env::consts::OS,
                    std::env::consts::FAMILY
                ),
                native_abi_version: sinomo_runtime::module::NATIVE_DYLIB_HOST_ABI_VERSION,
                interface_version: "sinomo.remote-management.test".to_string(),
                activated_at: "2026-07-18T00:00:00Z".to_string(),
                source: "test".to_string(),
            },
        )
        .expect("write loader record");

        let snapshot = backend.remote_management_settings_snapshot();
        assert_eq!(snapshot.module_observed, ModuleObservedState::NotReported);
        assert!(
            snapshot
                .module_observed_detail
                .contains("daemon registration is not independently reported")
        );
    }

    #[test]
    fn native_rust_state_with_same_id_does_not_override_native_dylib_backend() {
        let backend = Backend::local_fallback();
        let module_id =
            sinomo_runtime::module::ModuleId::from_legacy_named(REMOTE_MANAGEMENT_MODULE_ID)
                .expect("remote-management module id");
        sinomo_runtime::module::set_native_rust_module_enabled(
            &backend.data_dir.root(),
            &module_id,
            true,
            "test",
            "conflicting backend observation",
        )
        .expect("write conflicting native Rust state");

        let snapshot = backend.remote_management_settings_snapshot();
        assert_eq!(snapshot.module_observed, ModuleObservedState::NotReported);
        assert_eq!(
            snapshot.module_observed_detail,
            "The local loader has not published an observation"
        );
    }
}
