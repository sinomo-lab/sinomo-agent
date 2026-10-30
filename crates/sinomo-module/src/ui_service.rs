//! Local application catalog for module-contributed UI extension surfaces.

use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use crate::{
    ExtensionRegistrySnapshot, ModuleBackendRegistration, ModuleRegistrySnapshot,
    ModuleRuntimeGenerationStore, compile_default_extension_registry,
};

pub use sinomo_api::module::MODULE_UI_CATALOG_ROUTE;
pub use sinomo_api::module::MODULE_UI_RPC_CATEGORY;
pub use sinomo_api::module::{
    MODULE_OBSERVATIONS_ROUTE, ModuleObservationsRequest, ModuleObservationsResponse,
};

pub use sinomo_api::module::UiExtensionCatalogRequest;

pub use sinomo_api::module::UiExtensionCatalogEntry;

pub use sinomo_api::module::UiExtensionCatalogResponse;

/// Resident module metadata for host clients. Reading this catalog never loads
/// native libraries or reopens their persisted loader state.
pub const MODULE_CATALOG_ROUTE: &str = "/module/catalog";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ModuleCatalogRequest {}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ModuleCatalogResponse {
    pub registry_generation: u64,
    pub registrations: Vec<crate::ModuleRegistration>,
}

pub fn module_catalog_response(snapshot: &ModuleRegistrySnapshot) -> ModuleCatalogResponse {
    ModuleCatalogResponse {
        registry_generation: snapshot.version(),
        registrations: snapshot.registry().registrations().cloned().collect(),
    }
}

pub fn module_catalog_route() -> ServiceRoute {
    ServiceRoute::describe(
        MODULE_CATALOG_ROUTE,
        "module.catalog",
        "read resident module registrations",
        "sinomo_module::ModuleCatalogRequest",
        "sinomo_module::ModuleCatalogResponse",
    )
    .with_auth_scope("local-command")
    .observable("module.catalog", "read")
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
        &[
            MODULE_UI_CATALOG_ROUTE,
            MODULE_OBSERVATIONS_ROUTE,
            MODULE_CATALOG_ROUTE,
        ]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ui_extension_catalog_route(),
            module_observations_route(),
            module_catalog_route(),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let generations = self.generations.clone();
        let data_dir_root = self.data_dir_root.clone();
        Box::pin(async move {
            if request.route == MODULE_CATALOG_ROUTE {
                let _: ModuleCatalogRequest = ctx.decode_request_body(&request).await?;
                let generation = generations.active_snapshot();
                ctx.respond_value(
                    MODULE_CATALOG_ROUTE,
                    module_catalog_response(generation.module_registry.as_ref()),
                )?;
                return Ok(ServiceDisposition::Continue);
            }
            if request.route == MODULE_OBSERVATIONS_ROUTE {
                let _: ModuleObservationsRequest = ctx.decode_request_body(&request).await?;
                let generation = generations.active_snapshot();
                let response = tokio::task::spawn_blocking(move || {
                    module_observations_with_registry(
                        &data_dir_root,
                        generation.module_registry.registry(),
                    )
                })
                .await
                .map_err(|error| ServiceError::Service(error.to_string()))?;
                ctx.respond_value(MODULE_OBSERVATIONS_ROUTE, response)?;
                return Ok(ServiceDisposition::Continue);
            }
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

/// Loader recovery and cache inspection remain in the daemon's module host.
pub fn module_observations(data_dir_root: &Path) -> ModuleObservationsResponse {
    module_observations_with_display_names(data_dir_root, BTreeMap::new())
}

pub fn module_observations_with_registry(
    data_dir_root: &Path,
    registry: &crate::ModuleRegistry,
) -> ModuleObservationsResponse {
    let names = registry
        .registrations()
        .filter_map(|registration| {
            registration
                .component
                .display_name
                .as_ref()
                .map(|name| (registration.module_id().clone(), name.clone()))
        })
        .collect();
    module_observations_with_display_names(data_dir_root, names)
}

fn module_observations_with_display_names(
    data_dir_root: &Path,
    mut display_names: BTreeMap<crate::ModuleId, String>,
) -> ModuleObservationsResponse {
    let native_dylib = crate::read_native_dylib_module_loader_states(data_dir_root)
        .map_err(|error| error.to_string());
    // Disabled modules may have an activation record without a live registration.
    // Read their owner-provided descriptor from that exact installed artifact.
    if let Ok(states) = &native_dylib {
        let cache_root = crate::native_dylib_module_cache_dir(data_dir_root);
        for state in states {
            if display_names.contains_key(&state.module_id) {
                continue;
            }
            let path = crate::native_module_cache_path(
                data_dir_root,
                &state.cache_key,
                &state.content_hash,
                &state.library_name,
            );
            if let Ok(Some(entry)) =
                crate::native_dylib_module_cache_entry_from_path(&cache_root, &path)
                && entry.module_id == state.module_id
                && let Some(name) = entry.display_name
            {
                display_names.insert(entry.module_id, name);
            }
        }
    }
    let inactive_cache = native_dylib
        .as_ref()
        .map_err(Clone::clone)
        .and_then(|states| {
            let active_keys = states.iter().map(|state| state.cache_key.clone()).collect();
            crate::read_native_dylib_module_cache_entries_excluding_cache_keys(
                data_dir_root,
                &active_keys,
            )
            .map_err(|error| error.to_string())
        });
    if let Ok(entries) = &inactive_cache {
        for entry in entries {
            if let Some(name) = &entry.display_name {
                display_names
                    .entry(entry.module_id.clone())
                    .or_insert_with(|| name.clone());
            }
        }
    }
    if let Ok(registrations) = crate::read_declarative_module_registrations(data_dir_root) {
        for registration in registrations {
            if let Some(name) = registration.component.display_name {
                display_names
                    .entry(registration.manifest.module_id)
                    .or_insert(name);
            }
        }
    }
    ModuleObservationsResponse {
        display_names,
        native_dylib,
        inactive_cache,
        native_rust: crate::read_native_rust_module_runtime_states(data_dir_root)
            .map_err(|error| error.to_string()),
        declarative: crate::read_declarative_module_loader_states(data_dir_root)
            .map_err(|error| error.to_string()),
    }
}

pub fn module_observations_route() -> ServiceRoute {
    ServiceRoute::describe(
        MODULE_OBSERVATIONS_ROUTE,
        MODULE_UI_RPC_CATEGORY,
        "inspect installed module and loader observations",
        "sinomo_api::module::ModuleObservationsRequest",
        "sinomo_api::module::ModuleObservationsResponse",
    )
    .with_auth_scope("admin")
    .observable("module.observations.read", "read")
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

    #[test]
    fn resident_catalog_tracks_registration_withdrawal_without_artifacts() {
        let registration = ModuleRegistration::native_rust(
            ModuleId::from_legacy_named("module.catalog-fixture").unwrap(),
            "1.0.0",
            "fixture",
            "fixture",
            "fixture",
            sinomo_core::TrustRing::R1TrustedRuntime,
            "fixture",
        );
        let registry = ModuleRegistry::builder()
            .register(registration.clone())
            .unwrap()
            .try_build()
            .unwrap();
        let snapshots = crate::ModuleRegistrySnapshotStore::new(registry).unwrap();
        let first = module_catalog_response(&snapshots.active_snapshot().unwrap());
        assert_eq!(first.registrations, vec![registration]);
        let withdrawn = snapshots
            .replace_registry(ModuleRegistry::default())
            .unwrap();
        let next = module_catalog_response(&withdrawn);
        assert!(next.registry_generation > first.registry_generation);
        assert!(next.registrations.is_empty());
    }

    #[test]
    fn observations_publish_registered_names_and_keep_legacy_wire_compatible() {
        let registration = registration("module.fixture-owner", Vec::new())
            .with_display_name("Owner supplied name");
        let id = registration.module_id().clone();
        let registry = ModuleRegistry::builder()
            .register(registration)
            .unwrap()
            .try_build()
            .unwrap();
        let root = sinomo_testdir::TestDir::new("module-display-names");
        let response = module_observations_with_registry(root.path(), &registry);
        assert_eq!(
            response.display_names.get(&id).map(String::as_str),
            Some("Owner supplied name")
        );
        let mut wire = serde_json::to_value(&response).unwrap();
        let decoded: ModuleObservationsResponse = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(decoded.display_names, response.display_names);
        let encoded = sinomo_module_api::encode_sobj(&response).unwrap();
        let decoded: ModuleObservationsResponse = sinomo_module_api::decode_sobj(&encoded).unwrap();
        assert_eq!(decoded.display_names, response.display_names);
        wire.as_object_mut().unwrap().remove("display_names");
        let old: ModuleObservationsResponse = serde_json::from_value(wire).unwrap();
        assert!(old.display_names.is_empty());
    }

    #[test]
    fn observations_keep_names_for_inactive_declarative_modules() {
        let registration = ModuleRegistration {
            manifest: sinomo_core::ModuleManifest::declarative(
                ModuleId::from_legacy_named("module.inactive-provider").unwrap(),
                "1.0.0",
                "fixture",
                "1.0.0",
                TrustRing::R1TrustedRuntime,
            ),
            backend: ModuleBackendRegistration::Declarative {
                payload_ref: "sifs:/fixture".into(),
            },
            surfaces: Default::default(),
            component: sinomo_module_api::ComponentDescriptor::new(
                "module.inactive-provider",
                "1.0.0",
            )
            .with_display_name("Inactive Provider"),
        };
        let root = sinomo_testdir::TestDir::new("inactive-module-display-name");
        crate::write_declarative_module_registration(root.path(), &registration).unwrap();
        let response = module_observations(root.path());
        assert_eq!(
            response
                .display_names
                .get(registration.module_id())
                .map(String::as_str),
            Some("Inactive Provider")
        );
    }

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
        let canonical_component_path =
            std::fs::canonicalize(&component_path).expect("canonical component fixture path");
        assert_eq!(
            response.entries[0].component_path.as_deref(),
            Some(canonical_component_path.to_string_lossy().as_ref())
        );
    }
}
