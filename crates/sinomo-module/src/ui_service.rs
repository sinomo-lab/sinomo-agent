//! Local application catalog for module-contributed UI extension surfaces.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::{ModuleBackendRegistration, ModuleRegistrySnapshot};

pub const MODULE_UI_CATALOG_ROUTE: &str = "/module/ui/catalog";
pub const MODULE_UI_RPC_CATEGORY: &str = "module.ui";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionCatalogRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_point_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionCatalogEntry {
    pub module_id: String,
    pub module_version: String,
    pub module_backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component_path: Option<String>,
    pub contribution: sinomo_module_api::UiExtensionContribution,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UiExtensionCatalogResponse {
    pub registry_generation: u64,
    pub entries: Vec<UiExtensionCatalogEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Clone)]
pub struct HostedUiExtensionCatalogService {
    snapshot: Arc<ModuleRegistrySnapshot>,
    data_dir_root: PathBuf,
}

impl HostedUiExtensionCatalogService {
    pub fn new(snapshot: Arc<ModuleRegistrySnapshot>, data_dir_root: PathBuf) -> Self {
        Self {
            snapshot,
            data_dir_root,
        }
    }
}

impl Service for HostedUiExtensionCatalogService {
    fn name(&self) -> &'static str {
        MODULE_UI_RPC_CATEGORY
    }

    fn routes(&self) -> &'static [&'static str] {
        &[MODULE_UI_CATALOG_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![ui_extension_catalog_route()]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let snapshot = Arc::clone(&self.snapshot);
        let data_dir_root = self.data_dir_root.clone();
        Box::pin(async move {
            if request.route != MODULE_UI_CATALOG_ROUTE {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            let payload: UiExtensionCatalogRequest = ctx.decode_request_body(&request).await?;
            let response =
                ui_extension_catalog_response(snapshot.as_ref(), &payload, &data_dir_root)
                    .map_err(ServiceError::Service)?;
            ctx.respond_value(MODULE_UI_CATALOG_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_ui_extension_catalog_service(
    builder: ServiceRegistryBuilder,
    snapshot: Arc<ModuleRegistrySnapshot>,
    data_dir_root: PathBuf,
) -> ServiceRegistryBuilder {
    builder.register(HostedUiExtensionCatalogService::new(
        snapshot,
        data_dir_root,
    ))
}

pub fn ui_extension_catalog_route() -> ServiceRoute {
    ServiceRoute::describe(
        MODULE_UI_CATALOG_ROUTE,
        MODULE_UI_RPC_CATEGORY,
        "list active module UI extension contributions",
        "sinomo_module::UiExtensionCatalogRequest",
        "sinomo_module::UiExtensionCatalogResponse",
    )
    .with_auth_scope("local-command")
    .observable("module.ui.catalog", "read")
}

pub fn ui_extension_catalog_response(
    snapshot: &ModuleRegistrySnapshot,
    request: &UiExtensionCatalogRequest,
    data_dir_root: &Path,
) -> Result<UiExtensionCatalogResponse, String> {
    if let Some(point) = request.extension_point_id.as_deref()
        && (point.is_empty()
            || point.len() > 256
            || !point.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':' | b'@')
            }))
    {
        return Err("UI extension point filter is invalid".to_string());
    }
    let mut by_point = BTreeMap::<String, Vec<UiExtensionCatalogEntry>>::new();
    let mut warnings = Vec::new();
    for registration in snapshot.registry().registrations() {
        for contribution in &registration.surfaces.ui_contributions {
            if request
                .extension_point_id
                .as_deref()
                .is_some_and(|point| point != contribution.extension_point_id)
            {
                continue;
            }
            let component_path =
                match resolve_ui_component_path(data_dir_root, &registration.backend, contribution)
                {
                    Ok(path) => path,
                    Err(detail) => {
                        warnings.push(format!(
                            "UI contribution `{}` from module `{}` is unavailable: {detail}",
                            contribution.contribution_id,
                            registration.module_id()
                        ));
                        None
                    }
                };
            by_point
                .entry(contribution.extension_point_id.clone())
                .or_default()
                .push(UiExtensionCatalogEntry {
                    module_id: registration.module_id().to_string(),
                    module_version: registration.manifest.version.clone(),
                    module_backend: module_backend_label(&registration.backend).to_string(),
                    component_path,
                    contribution: contribution.clone(),
                });
        }
    }

    let mut entries = Vec::new();
    for (point, mut point_entries) in by_point {
        let contributions = point_entries
            .iter()
            .map(|entry| entry.contribution.clone())
            .collect::<Vec<_>>();
        let order = match sinomo_module_api::order_ui_contributions(&contributions) {
            Ok(order) => order,
            Err(sinomo_module_api::UiExtensionContractError::CyclicOrdering { contributions }) => {
                warnings.push(format!(
                    "UI extension point `{point}` discarded cyclic relative ordering among {}",
                    contributions.join(", ")
                ));
                point_entries
                    .sort_by(|left, right| ui_entry_sort_key(left).cmp(&ui_entry_sort_key(right)));
                entries.extend(point_entries);
                continue;
            }
            Err(error) => return Err(error.to_string()),
        };
        entries.extend(order.into_iter().map(|index| point_entries[index].clone()));
    }
    Ok(UiExtensionCatalogResponse {
        registry_generation: snapshot.version(),
        entries,
        warnings,
    })
}

fn resolve_ui_component_path(
    data_dir_root: &Path,
    backend: &ModuleBackendRegistration,
    contribution: &sinomo_module_api::UiExtensionContribution,
) -> Result<Option<String>, String> {
    let sinomo_module_api::UiContributionPayload::CustomWidget { widget } = &contribution.payload
    else {
        return Ok(None);
    };
    let ModuleBackendRegistration::Declarative { payload_ref } = backend else {
        return Err(
            "custom UI artifacts currently require a declarative module payload".to_string(),
        );
    };
    let relative_payload = payload_ref
        .strip_prefix("local://")
        .ok_or_else(|| format!("unsupported declarative payload reference `{payload_ref}`"))?;
    validate_relative_path(relative_payload)?;
    validate_relative_path(&widget.component.resource_path)?;
    let payload_root = data_dir_root.join(relative_payload);
    let component_path = payload_root.join(&widget.component.resource_path);
    let canonical_payload_root = std::fs::canonicalize(&payload_root)
        .map_err(|source| format!("cannot resolve {}: {source}", payload_root.display()))?;
    let canonical_component = std::fs::canonicalize(&component_path)
        .map_err(|source| format!("cannot resolve {}: {source}", component_path.display()))?;
    if !canonical_component.starts_with(&canonical_payload_root) {
        return Err(format!(
            "UI component {} escapes its immutable module payload",
            component_path.display()
        ));
    }
    let metadata = std::fs::symlink_metadata(&canonical_component)
        .map_err(|source| format!("cannot inspect {}: {source}", canonical_component.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "UI component {} must be a regular non-symlink file",
            canonical_component.display()
        ));
    }
    if metadata.len() > sinomo_module_api::MAX_UI_COMPONENT_ARTIFACT_BYTES {
        return Err(format!(
            "UI component {} is {} bytes; limit is {}",
            canonical_component.display(),
            metadata.len(),
            sinomo_module_api::MAX_UI_COMPONENT_ARTIFACT_BYTES
        ));
    }
    let bytes = std::fs::read(&canonical_component)
        .map_err(|source| format!("cannot read {}: {source}", canonical_component.display()))?;
    let actual_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    if actual_hash != widget.component.content_hash {
        return Err(format!(
            "content hash mismatch: expected {}, actual {actual_hash}",
            widget.component.content_hash
        ));
    }
    Ok(Some(canonical_component.to_string_lossy().into_owned()))
}

fn validate_relative_path(value: &str) -> Result<(), String> {
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("path `{value}` is not a normalized relative path"));
    }
    Ok(())
}

fn ui_entry_sort_key(entry: &UiExtensionCatalogEntry) -> (&str, i32, &str, &str) {
    (
        entry.contribution.group.as_str(),
        entry.contribution.order,
        entry.module_id.as_str(),
        entry.contribution.contribution_id.as_str(),
    )
}

fn module_backend_label(backend: &ModuleBackendRegistration) -> &'static str {
    match backend {
        ModuleBackendRegistration::NativeRust { .. } => "native-rust",
        ModuleBackendRegistration::NativeDylib { .. } => "native-dylib",
        ModuleBackendRegistration::Declarative { .. } => "declarative",
        ModuleBackendRegistration::SandboxProcess { .. } => "sandbox-process",
        ModuleBackendRegistration::WasmComponent { .. } => "wasm-component",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModuleId, ModuleRegistration, ModuleRegistry};
    use sinomo_core::TrustRing;
    use sinomo_module_api::{
        FILE_PREVIEW_EXTENSION_POINT_ID, FILE_PREVIEW_EXTENSION_POINT_VERSION, UiComponentArtifact,
        UiContentSelector, UiCustomWidgetContribution, UiExtensionContribution,
    };

    fn contribution(id: &str, order: i32) -> UiExtensionContribution {
        contribution_with_hash(id, order, format!("blake3:{}", "1".repeat(64)))
    }

    fn contribution_with_hash(
        id: &str,
        order: i32,
        content_hash: String,
    ) -> UiExtensionContribution {
        let widget = UiCustomWidgetContribution::new(
            "Fixture preview",
            UiComponentArtifact::wasm(
                format!("artifact.{id}"),
                content_hash,
                "ui/fixture.wasm",
                "sinomo:ui-widget/widget",
            ),
        )
        .with_selector(UiContentSelector::extension(".fixture"));
        UiExtensionContribution::custom_widget(
            id,
            FILE_PREVIEW_EXTENSION_POINT_ID,
            FILE_PREVIEW_EXTENSION_POINT_VERSION,
            widget,
        )
        .with_order("preview", order)
    }

    fn registration(
        module: &str,
        contributions: Vec<UiExtensionContribution>,
    ) -> ModuleRegistration {
        let module_id = ModuleId::from_legacy_named(module).expect("module id");
        let mut registration = ModuleRegistration::native_rust(
            module_id,
            "1.0.0",
            module,
            module,
            "1.0.0",
            TrustRing::R1TrustedRuntime,
            module,
        );
        registration.surfaces.ui_contributions = contributions;
        registration
    }

    fn declarative_registration(
        module: &str,
        contribution: UiExtensionContribution,
        payload_ref: &str,
    ) -> ModuleRegistration {
        let module_id = ModuleId::from_legacy_named(module).expect("module id");
        ModuleRegistration {
            manifest: sinomo_core::ModuleManifest::declarative(
                module_id,
                "1.0.0",
                module,
                "1.0.0",
                TrustRing::R1TrustedRuntime,
            ),
            backend: ModuleBackendRegistration::Declarative {
                payload_ref: payload_ref.to_string(),
            },
            surfaces: crate::ModuleSurfaces::default().with_ui_contribution(contribution),
            component: sinomo_module_api::ComponentDescriptor::default(),
        }
    }

    #[test]
    fn catalog_is_generation_stamped_and_deterministically_ordered() {
        let registry = ModuleRegistry::builder()
            .register(registration(
                "module.second",
                vec![contribution("module.second.preview", 20)],
            ))
            .expect("second registration")
            .register(registration(
                "module.first",
                vec![contribution("module.first.preview", 10)],
            ))
            .expect("first registration")
            .try_build()
            .expect("registry");
        let store = crate::ModuleRegistrySnapshotStore::new(registry).expect("snapshot store");
        let snapshot = store.active_snapshot().expect("active snapshot");
        let root = sinomo_testdir::TestDir::new("sinomo-module-ui-catalog");
        let response = ui_extension_catalog_response(
            snapshot.as_ref(),
            &UiExtensionCatalogRequest {
                extension_point_id: Some(FILE_PREVIEW_EXTENSION_POINT_ID.to_string()),
            },
            root.path(),
        )
        .expect("catalog");

        assert_eq!(response.registry_generation, 1);
        assert_eq!(response.entries.len(), 2);
        assert_eq!(
            response.entries[0].contribution.contribution_id,
            "module.first.preview"
        );
        assert_eq!(
            response.entries[1].contribution.contribution_id,
            "module.second.preview"
        );
    }

    #[test]
    fn catalog_route_is_local_command_only() {
        let route = ui_extension_catalog_route();
        assert_eq!(route.auth_scope.as_deref(), Some("local-command"));
    }

    #[test]
    fn catalog_resolves_and_reverifies_declarative_ui_component() {
        let root = sinomo_testdir::TestDir::new("sinomo-module-ui-component-path");
        let bytes = b"fixture wasm component";
        let hash = format!("blake3:{}", blake3::hash(bytes).to_hex());
        let component_path = root.path().join("payload/ui/fixture.wasm");
        std::fs::create_dir_all(component_path.parent().expect("component parent"))
            .expect("create component parent");
        std::fs::write(&component_path, bytes).expect("write component");
        let registration = declarative_registration(
            "module.fixture",
            contribution_with_hash("module.fixture.preview", 0, hash),
            "local://payload",
        );
        let registry = ModuleRegistry::builder()
            .register(registration)
            .expect("registration")
            .try_build()
            .expect("registry");
        let store = crate::ModuleRegistrySnapshotStore::new(registry).expect("snapshot store");
        let snapshot = store.active_snapshot().expect("snapshot");

        let response = ui_extension_catalog_response(
            snapshot.as_ref(),
            &UiExtensionCatalogRequest::default(),
            root.path(),
        )
        .expect("catalog");

        assert!(response.warnings.is_empty());
        assert_eq!(response.entries.len(), 1);
        assert_eq!(
            response.entries[0].component_path.as_deref(),
            Some(component_path.to_string_lossy().as_ref())
        );
    }
}
