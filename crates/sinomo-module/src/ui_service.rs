//! Local application catalog for module-contributed UI extension surfaces.

use serde::{Deserialize, Serialize};
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use crate::{
    ExtensionRegistrySnapshot, ModuleBackendRegistration, ModuleRegistrySnapshot,
    ModuleRuntimeGenerationStore, compile_default_extension_registry,
};

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
    generations: ModuleRuntimeGenerationStore,
    data_dir_root: PathBuf,
}

impl HostedUiExtensionCatalogService {
    pub fn new(generations: ModuleRuntimeGenerationStore, data_dir_root: PathBuf) -> Self {
        Self {
            generations,
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
        let generations = self.generations.clone();
        let data_dir_root = self.data_dir_root.clone();
        Box::pin(async move {
            if request.route != MODULE_UI_CATALOG_ROUTE {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            let payload: UiExtensionCatalogRequest = ctx.decode_request_body(&request).await?;
            let generation = generations.active_snapshot();
            let response = ui_extension_catalog_response_with_registry(
                generation.module_registry.as_ref(),
                generation.extension_registry.as_ref(),
                &payload,
                &data_dir_root,
            )
            .map_err(ServiceError::Service)?;
            ctx.respond_value(MODULE_UI_CATALOG_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_ui_extension_catalog_service(
    builder: ServiceRegistryBuilder,
    generations: ModuleRuntimeGenerationStore,
    data_dir_root: PathBuf,
) -> ServiceRegistryBuilder {
    builder.register(HostedUiExtensionCatalogService::new(
        generations,
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
    let registry =
        compile_default_extension_registry(snapshot).map_err(|error| error.to_string())?;
    ui_extension_catalog_response_with_registry(snapshot, &registry, request, data_dir_root)
}

pub fn ui_extension_catalog_response_with_registry(
    snapshot: &ModuleRegistrySnapshot,
    registry: &ExtensionRegistrySnapshot,
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
    let mut ui_point_ids = registry
        .points
        .iter()
        .filter(|point| {
            point.definition.contribution_schema_id
                == sinomo_module_api::UI_EXTENSION_PAYLOAD_SCHEMA_ID
        })
        .map(|point| point.definition.point_id.as_str())
        .collect::<BTreeSet<_>>();
    for registration in snapshot.registry().registrations() {
        ui_point_ids.extend(
            registration
                .surfaces
                .ui_contributions
                .iter()
                .map(|contribution| contribution.extension_point_id.as_str()),
        );
    }
    let mut warnings = registry
        .diagnostics
        .iter()
        .filter(|diagnostic| ui_point_ids.contains(diagnostic.point_id.as_str()))
        .map(|diagnostic| {
            format!(
                "extension registry {:?} for `{}`{}: {}",
                diagnostic.code,
                diagnostic.point_id,
                diagnostic
                    .contribution_id
                    .as_deref()
                    .map(|id| format!(" contribution `{id}`"))
                    .unwrap_or_default(),
                diagnostic.detail
            )
        })
        .collect::<Vec<_>>();
    let mut entries = Vec::new();
    for point in &registry.points {
        if request
            .extension_point_id
            .as_deref()
            .is_some_and(|requested| requested != point.definition.point_id)
        {
            continue;
        }
        for resolved in &point.contributions {
            if resolved.contribution.payload_schema_id
                != sinomo_module_api::UI_EXTENSION_PAYLOAD_SCHEMA_ID
                || resolved.contribution.payload_sobj.is_empty()
            {
                continue;
            }
            let payload = match sinomo_module_api::decode_sobj::<
                sinomo_module_api::UiContributionPayload,
            >(&resolved.contribution.payload_sobj)
            {
                Ok(payload) => payload,
                Err(error) => {
                    warnings.push(format!(
                        "UI contribution `{}` payload is invalid: {error}",
                        resolved.contribution.contribution_id
                    ));
                    continue;
                }
            };
            let contribution = ui_contribution_from_generic(
                &resolved.contribution,
                &point.definition.point_version,
                payload,
            );
            let Some(registration) = snapshot
                .registry()
                .registrations()
                .find(|registration| registration.module_id().to_string() == resolved.module_id)
            else {
                warnings.push(format!(
                    "UI contribution `{}` lost its module registration",
                    contribution.contribution_id
                ));
                continue;
            };
            let component_path = match resolve_ui_component_path(
                data_dir_root,
                &registration.backend,
                &contribution,
            ) {
                Ok(path) => path,
                Err(detail) => {
                    warnings.push(format!(
                        "UI contribution `{}` from module `{}` is unavailable: {detail}",
                        contribution.contribution_id, resolved.module_id
                    ));
                    None
                }
            };
            entries.push(UiExtensionCatalogEntry {
                module_id: resolved.module_id.clone(),
                module_version: resolved.module_version.clone(),
                module_backend: resolved.module_backend.clone(),
                component_path,
                contribution,
            });
        }
    }
    Ok(UiExtensionCatalogResponse {
        registry_generation: registry.generation.module_registry_generation,
        entries,
        warnings,
    })
}

fn ui_contribution_from_generic(
    contribution: &sinomo_module_api::ExtensionContribution,
    point_version: &str,
    payload: sinomo_module_api::UiContributionPayload,
) -> sinomo_module_api::UiExtensionContribution {
    sinomo_module_api::UiExtensionContribution {
        contribution_id: contribution.contribution_id.clone(),
        extension_point_id: contribution.point_id.clone(),
        extension_point_version: point_version.to_string(),
        group: contribution.group.clone(),
        order: contribution.order,
        before: contribution.before.clone(),
        after: contribution.after.clone(),
        when: contribution
            .when
            .iter()
            .map(|condition| sinomo_module_api::UiContextCondition {
                key: condition.key.clone(),
                value: match &condition.value {
                    sinomo_module_api::ExtensionContextValue::Bool(value) => {
                        sinomo_module_api::UiContextValue::Bool(*value)
                    }
                    sinomo_module_api::ExtensionContextValue::String(value) => {
                        sinomo_module_api::UiContextValue::String(value.clone())
                    }
                    sinomo_module_api::ExtensionContextValue::Integer(value) => {
                        sinomo_module_api::UiContextValue::Integer(*value)
                    }
                },
            })
            .collect(),
        payload,
    }
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
    fn catalog_projects_generic_ui_contributions_from_the_shared_registry() {
        let ui = contribution("module.generic.preview", 0);
        let mut registration = registration("module.generic", Vec::new());
        registration.surfaces.extension_contributions = vec![
            ui.to_extension_contribution()
                .expect("generic UI contribution"),
        ];
        let registry = ModuleRegistry::builder()
            .register(registration)
            .expect("registration")
            .try_build()
            .expect("registry");
        let store = crate::ModuleRegistrySnapshotStore::new(registry).expect("snapshot store");
        let snapshot = store.active_snapshot().expect("snapshot");
        let root = sinomo_testdir::TestDir::new("sinomo-module-generic-ui-catalog");

        let response = ui_extension_catalog_response(
            snapshot.as_ref(),
            &UiExtensionCatalogRequest::default(),
            root.path(),
        )
        .expect("catalog");

        assert_eq!(response.entries.len(), 1);
        assert_eq!(
            response.entries[0].contribution.contribution_id,
            "module.generic.preview"
        );
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
