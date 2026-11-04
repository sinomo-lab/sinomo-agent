//! Local catalog service for the compiled, generation-stamped extension registry.

use serde::{Deserialize, Serialize};
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::{
    ExtensionRegistryCompileError, ExtensionRegistryDiagnostic, ExtensionRegistrySnapshot,
    HostExtensionPointDefinition, ModuleRegistrySnapshot, ModuleRuntimeGenerationStore,
    ResolvedExtensionPoint, compile_extension_registry,
    module_diagnostics_extension_point_definition,
};

pub const MODULE_EXTENSION_CATALOG_ROUTE: &str = "/module/extensions/catalog";
pub const MODULE_EXTENSION_RPC_CATEGORY: &str = "module.extensions";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionCatalogRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionCatalogResponse {
    pub registry_generation: u64,
    pub generated_at: String,
    pub points: Vec<ResolvedExtensionPoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ExtensionRegistryDiagnostic>,
}

#[derive(Clone)]
pub struct HostedExtensionCatalogService {
    generations: ModuleRuntimeGenerationStore,
}

impl HostedExtensionCatalogService {
    pub fn new(generations: ModuleRuntimeGenerationStore) -> Self {
        Self { generations }
    }
}

impl Service for HostedExtensionCatalogService {
    fn name(&self) -> &'static str {
        MODULE_EXTENSION_RPC_CATEGORY
    }

    fn routes(&self) -> &'static [&'static str] {
        &[MODULE_EXTENSION_CATALOG_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![extension_catalog_route()]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let generations = self.generations.clone();
        Box::pin(async move {
            if request.route != MODULE_EXTENSION_CATALOG_ROUTE {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            let payload: ExtensionCatalogRequest = ctx.decode_request_body(&request).await?;
            let generation = generations.active_snapshot();
            let response =
                extension_catalog_response(generation.extension_registry.as_ref(), &payload)
                    .map_err(ServiceError::Service)?;
            ctx.respond_value(MODULE_EXTENSION_CATALOG_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_extension_catalog_service(
    builder: ServiceRegistryBuilder,
    generations: ModuleRuntimeGenerationStore,
) -> ServiceRegistryBuilder {
    builder.register(HostedExtensionCatalogService::new(generations))
}

pub fn extension_catalog_route() -> ServiceRoute {
    ServiceRoute::describe(
        MODULE_EXTENSION_CATALOG_ROUTE,
        MODULE_EXTENSION_RPC_CATEGORY,
        "list compiled module extension points, contributions, and pending diagnostics",
        "sinomo_module::ExtensionCatalogRequest",
        "sinomo_module::ExtensionCatalogResponse",
    )
    .with_auth_scope("local-command")
    .observable("module.extensions.catalog", "read")
}

/// Host-owned points that are always available in this process generation.
/// Domain crates should add their own definitions when compiling a narrower
/// registry rather than allowing contributors to invent owner behavior.
pub fn default_host_extension_point_definitions() -> Vec<HostExtensionPointDefinition> {
    vec![
        HostExtensionPointDefinition::new(
            "sinomo-app",
            env!("CARGO_PKG_VERSION"),
            sinomo_module_api::panels::extension_point(),
        ),
        HostExtensionPointDefinition::new(
            "sinomo-cli",
            env!("CARGO_PKG_VERSION"),
            sinomo_module_api::cli::cli_commands_extension_point(),
        ),
        HostExtensionPointDefinition::new(
            "sinomo-app",
            env!("CARGO_PKG_VERSION"),
            sinomo_module_api::file_preview_extension_point_definition(),
        ),
        HostExtensionPointDefinition::new(
            "sinomo-module",
            env!("CARGO_PKG_VERSION"),
            module_diagnostics_extension_point_definition(),
        ),
    ]
}

pub fn compile_default_extension_registry(
    snapshot: &ModuleRegistrySnapshot,
) -> Result<ExtensionRegistrySnapshot, ExtensionRegistryCompileError> {
    let registry =
        compile_extension_registry(snapshot, default_host_extension_point_definitions())?;
    crate::cli_catalog_entries(snapshot, &registry)
        .map_err(|detail| ExtensionRegistryCompileError::CliCommands { detail })?;
    Ok(registry)
}

pub fn extension_catalog_response(
    registry: &ExtensionRegistrySnapshot,
    request: &ExtensionCatalogRequest,
) -> Result<ExtensionCatalogResponse, String> {
    if request.point_version.is_some() && request.point_id.is_none() {
        return Err("an extension point version filter requires a point id".to_string());
    }
    if let Some(point_id) = request.point_id.as_deref() {
        validate_catalog_filter(point_id, "extension point id")?;
    }
    if let Some(point_version) = request.point_version.as_deref() {
        validate_catalog_filter(point_version, "extension point version")?;
    }
    let points = registry
        .points
        .iter()
        .filter(|point| {
            request
                .point_id
                .as_deref()
                .is_none_or(|point_id| point.definition.point_id == point_id)
                && request
                    .point_version
                    .as_deref()
                    .is_none_or(|version| point.definition.point_version == version)
        })
        .cloned()
        .collect();
    let diagnostics = registry
        .diagnostics
        .iter()
        .filter(|diagnostic| {
            request
                .point_id
                .as_deref()
                .is_none_or(|point_id| diagnostic.point_id == point_id)
        })
        .cloned()
        .collect();
    Ok(ExtensionCatalogResponse {
        registry_generation: registry.generation.module_registry_generation,
        generated_at: registry.generated_at.clone(),
        points,
        diagnostics,
    })
}

fn validate_catalog_filter(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 256
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':' | b'@' | b'*')
        })
    {
        return Err(format!("{label} filter is invalid"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModuleRegistry, ModuleRegistrySnapshotStore};

    #[test]
    fn default_catalog_has_generation_and_host_owned_points() {
        let registry = ModuleRegistry::builder().try_build().expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("snapshot store");
        let module_snapshot = store.active_snapshot().expect("module snapshot");
        let extensions =
            compile_default_extension_registry(module_snapshot.as_ref()).expect("extensions");
        let response = extension_catalog_response(
            &extensions,
            &ExtensionCatalogRequest {
                point_id: Some(crate::MODULE_DIAGNOSTICS_EXTENSION_POINT_ID.to_string()),
                point_version: None,
            },
        )
        .expect("catalog");

        assert_eq!(response.registry_generation, module_snapshot.version());
        assert_eq!(response.points.len(), 1);
        assert_eq!(
            response.points[0].definition.point_id,
            crate::MODULE_DIAGNOSTICS_EXTENSION_POINT_ID
        );
    }

    #[test]
    fn catalog_route_is_local_command_only() {
        assert_eq!(
            extension_catalog_route().auth_scope.as_deref(),
            Some("local-command")
        );
    }

    #[test]
    fn version_filter_requires_a_point_id() {
        let registry = ExtensionRegistrySnapshot {
            generation: crate::ExtensionRegistryGeneration {
                module_registry_generation: 1,
            },
            generated_at: "test".to_string(),
            points: Vec::new(),
            diagnostics: Vec::new(),
        };
        assert!(
            extension_catalog_response(
                &registry,
                &ExtensionCatalogRequest {
                    point_id: None,
                    point_version: Some("1.0.0".to_string()),
                }
            )
            .is_err()
        );
    }
}
