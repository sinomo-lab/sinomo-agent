//! Generation-bound catalog and invocation for module-owned command namespaces.
use crate::{
    ComponentDescriptor, ComponentServiceClient, ExtensionRegistrySnapshot, ModuleRegistrySnapshot,
    ModuleRuntimeHost, ServiceRequirement,
};
use sinomo_api::cli::*;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture, ServiceRegistry,
    ServiceRegistryBuilder, ServiceRegistrySnapshotStore, ServiceRoute,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Validation runs before a candidate registry can replace the active generation.
pub fn cli_catalog_entries(
    snapshot: &ModuleRegistrySnapshot,
    extensions: &ExtensionRegistrySnapshot,
) -> Result<Vec<CliCatalogEntry>, String> {
    let mut names = BTreeMap::<String, String>::new();
    let mut entries = Vec::new();
    if extensions
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.point_id == CLI_COMMANDS_POINT)
    {
        return Err("CLI extension contribution did not satisfy its declared contract".into());
    }
    for point in extensions
        .points
        .iter()
        .filter(|point| point.definition.point_id == CLI_COMMANDS_POINT)
    {
        for resolved in &point.contributions {
            if !resolved.contribution.when.is_empty() {
                return Err(
                    "CLI namespaces cannot change ownership through context predicates".into(),
                );
            }
            let command: CliCommandDescriptor =
                sinomo_module_api::decode_sobj(&resolved.contribution.payload_sobj)
                    .map_err(|error| error.to_string())?;
            command.validate()?;
            sinomo_cli_registry::module_command_autocomplete(&command)?;
            if command.command_id != resolved.contribution.contribution_id {
                return Err("CLI contribution identity does not match its command identity".into());
            }
            let registration = snapshot
                .registry()
                .registrations()
                .find(|registration| registration.module_id().as_str() == resolved.module_id)
                .ok_or_else(|| "CLI command owner is unavailable".to_string())?;
            let offer = registration
                .component
                .offers
                .iter()
                .find(|offer| {
                    offer.service_id == command.service_id
                        && offer.interface_id == CLI_INTERFACE_ID
                        && offer.interface_version == CLI_INTERFACE_VERSION
                })
                .ok_or_else(|| "CLI contribution has no matching service offer".to_string())?;
            let operation = offer
                .operations
                .iter()
                .find(|operation| operation.operation == CLI_INVOKE_OPERATION)
                .ok_or_else(|| "CLI service does not offer invocation".to_string())?;
            let route = registration
                .surfaces
                .object_rpc_routes
                .iter()
                .find(|route| operation.route.as_deref() == Some(route.route.as_str()))
                .ok_or_else(|| "CLI invocation has no owned object-RPC route".to_string())?;
            if route.auth != crate::ModuleRouteAuth::Component {
                return Err("module CLI execution routes must be component-only".into());
            }
            if route.operation != CLI_INVOKE_OPERATION
                || route.request_body_mode != sinomo_module_api::NativeModuleBodyMode::None
                || route.response_body_mode != sinomo_module_api::NativeModuleBodyMode::None
            {
                return Err(
                    "module CLI v1 requires the cli.invoke operation with buffered values".into(),
                );
            }
            for name in std::iter::once(&command.name).chain(&command.aliases) {
                if let Some(previous) = names.insert(name.clone(), command.command_id.clone()) {
                    return Err(format!(
                        "CLI namespace `{name}` conflicts with `{previous}`"
                    ));
                }
            }
            entries.push(CliCatalogEntry {
                module_id: resolved.module_id.clone(),
                module_version: resolved.module_version.clone(),
                command,
            });
        }
    }
    entries.sort_by(|a, b| a.command.name.cmp(&b.command.name));
    Ok(entries)
}

#[derive(Clone)]
pub struct HostedCliService {
    catalog_id: String,
    runtime: ModuleRuntimeHost,
    /// Immutable service view from the same materialization as the registrations.
    services: ServiceRegistry,
    module_generation: u64,
}

impl HostedCliService {
    pub fn new(
        runtime: ModuleRuntimeHost,
        services: ServiceRegistry,
        module_generation: u64,
    ) -> Self {
        Self {
            catalog_id: sinomo_core::Id::generate().to_string(),
            runtime,
            services,
            module_generation,
        }
    }
}

impl Service for HostedCliService {
    fn name(&self) -> &'static str {
        CLI_RPC_CATEGORY
    }
    fn routes(&self) -> &'static [&'static str] {
        &[CLI_CATALOG_ROUTE, CLI_INVOKE_ROUTE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(CLI_CATALOG_ROUTE, CLI_RPC_CATEGORY)
                .with_auth_scope("local-command")
                .observable("module.cli.catalog", "read"),
            ServiceRoute::new(CLI_INVOKE_ROUTE, CLI_RPC_CATEGORY)
                .with_auth_scope("local-command")
                .with_timeout(Duration::from_millis(CLI_MAX_DEADLINE_MS))
                .observable("module.cli.invoke", "execute"),
        ]
    }
    fn handle(&self, mut context: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let generation = self.runtime.generations.active_snapshot();
            let entries =
                cli_catalog_entries(&generation.module_registry, &generation.extension_registry)
                    .map_err(ServiceError::Service)?;
            if request.route == CLI_CATALOG_ROUTE {
                let _: CliCatalogRequest = context.decode_request_body(&request).await?;
                context.respond_value(
                    CLI_CATALOG_ROUTE,
                    CliCatalogResponse {
                        catalog_id: self.catalog_id.clone(),
                        generation: generation.generation,
                        commands: entries,
                    },
                )?;
                return Ok(ServiceDisposition::Continue);
            }
            if request.route != CLI_INVOKE_ROUTE {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            let invoke: CliInvokeRequest = context.decode_request_body(&request).await?;
            invoke.request.validate().map_err(ServiceError::Service)?;
            if request
                .operation_id()
                .is_some_and(|id| id != invoke.request.invocation_id)
            {
                return Err(ServiceError::Service(
                    "CLI invocation identity differs from RPC operation identity".into(),
                ));
            }
            if invoke.catalog_id != self.catalog_id
                || invoke.generation != generation.generation
                || self.module_generation != generation.module_registry.version()
            {
                context.respond_value(CLI_INVOKE_ROUTE, CliInvokeResponse::CatalogChanged)?;
                return Ok(ServiceDisposition::Continue);
            }
            let entry = entries
                .into_iter()
                .find(|entry| {
                    entry.module_id == invoke.module_id
                        && entry.command.command_id == invoke.request.command_id
                })
                .ok_or_else(|| {
                    ServiceError::Service("CLI command is no longer registered".into())
                })?;
            if !entry.command.accepts_stdin && !invoke.request.stdin.is_empty() {
                return Err(ServiceError::Service(
                    "this command does not accept stdin".into(),
                ));
            }
            if entry.command.requires_cluster_state && invoke.request.action == CliAction::Invoke {
                context.require_cluster(CLI_INVOKE_ROUTE)?;
            }
            let _lease = self
                .runtime
                .invocations
                .acquire(&entry.module_id)
                .map_err(|error| ServiceError::Service(error.to_string()))?;
            if self.runtime.generations.active_snapshot().generation != invoke.generation {
                context.respond_value(CLI_INVOKE_ROUTE, CliInvokeResponse::CatalogChanged)?;
                return Ok(ServiceDisposition::Continue);
            }
            let requirement = ServiceRequirement::new(CLI_INTERFACE_ID, CLI_INTERFACE_VERSION)
                .from_service(&entry.command.service_id)
                .with_operation(CLI_INVOKE_OPERATION)
                .with_caller_capability(CLI_INVOKE_OPERATION);
            let caller = ComponentDescriptor::new("sinomo-cli", env!("CARGO_PKG_VERSION"))
                .require(requirement.clone());
            let client = ComponentServiceClient::new(
                caller,
                Arc::clone(&context.data_dir),
                ServiceRegistrySnapshotStore::new(self.services.clone()),
            );
            let expected_hash = generation
                .module_registry
                .registry()
                .registrations()
                .find(|registration| registration.module_id().as_str() == entry.module_id)
                .and_then(|registration| match &registration.backend {
                    crate::ModuleBackendRegistration::NativeDylib { content_hash, .. } => {
                        Some(content_hash.as_str())
                    }
                    _ => None,
                });
            let result = client
                .call_local_on_provider::<_, CliModuleResponse>(
                    &entry.module_id,
                    &requirement,
                    CLI_INVOKE_OPERATION,
                    &invoke.request.invocation_id,
                    expected_hash,
                    &invoke.request,
                    Duration::from_millis(entry.command.max_deadline_ms),
                    CLI_MAX_OUTPUT_BYTES,
                )
                .await;
            let response = match result {
                Ok(response) => match response.validate() {
                    Ok(()) => CliInvokeResponse::Completed(response),
                    Err(detail) => CliInvokeResponse::ExecutionFailed { detail },
                },
                Err(detail) => CliInvokeResponse::ExecutionFailed { detail },
            };
            context.respond_value(CLI_INVOKE_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_cli_service(
    builder: ServiceRegistryBuilder,
    runtime: ModuleRuntimeHost,
    services: ServiceRegistry,
    generation: u64,
) -> ServiceRegistryBuilder {
    builder.register(HostedCliService::new(runtime, services, generation))
}
