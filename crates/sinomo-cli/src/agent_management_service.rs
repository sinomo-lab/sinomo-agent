//! Daemon-owned application service for agent definition management.

use std::fs;
use std::path::{Path, PathBuf};

use sinomo_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionSifsStore, AgentDefinitionStore,
    AgentPackageProvenance, AgentPackageSource, AgentRole, AgentToolDescriptor,
    default_agent_tool_descriptors, image_generation_descriptor, web_search_descriptor,
};
use sinomo_app_contract::agent as contract;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::app_api::AgentChatSifsStorage;

#[derive(Clone)]
pub struct HostedAgentManagementService {
    daemon_address: String,
}

impl HostedAgentManagementService {
    pub fn new(daemon_address: impl Into<String>) -> Self {
        Self {
            daemon_address: daemon_address.into(),
        }
    }
}

impl Service for HostedAgentManagementService {
    fn name(&self) -> &'static str {
        "agent-app"
    }

    fn routes(&self) -> &'static [&'static str] {
        contract::AGENT_MANAGEMENT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        contract::AGENT_MANAGEMENT_RPC_ROUTES
            .iter()
            .copied()
            .map(|route| {
                let capability = if matches!(
                    route,
                    contract::AGENT_SNAPSHOT_ROUTE
                        | contract::AGENT_VALIDATE_ROUTE
                        | contract::AGENT_TEST_ROUTE
                ) {
                    contract::AGENT_MANAGEMENT_READ_CAPABILITY
                } else {
                    contract::AGENT_MANAGEMENT_WRITE_CAPABILITY
                };
                ServiceRoute::from_route_metadata(
                    route,
                    contract::AGENT_MANAGEMENT_RPC_CATEGORY,
                    contract::AGENT_MANAGEMENT_ROUTE_AUTH_SCOPE,
                    [capability],
                )
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let service = self.clone();
        Box::pin(async move {
            if !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "agent app operations are available only to the local app".to_string(),
                ));
            }
            let data_dir = ctx.data_dir.as_ref().clone();
            let runtime = tokio::runtime::Handle::current();
            match request.route.as_str() {
                contract::AGENT_SNAPSHOT_ROUTE => {
                    let payload: contract::AgentSnapshotRequest =
                        ctx.decode_request_body(&request).await?;
                    let snapshot = tokio::task::spawn_blocking(move || {
                        agent_snapshot(
                            &data_dir,
                            &service.daemon_address,
                            &runtime,
                            payload.selected_agent_id.as_deref(),
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(
                        contract::AGENT_SNAPSHOT_ROUTE,
                        contract::AgentSnapshotResponse { snapshot },
                    )?;
                }
                contract::AGENT_SAVE_ROUTE => {
                    let payload: contract::AgentSaveRequest =
                        ctx.decode_request_body(&request).await?;
                    tokio::task::spawn_blocking(move || {
                        save_definition(&data_dir, payload.definition, payload.previous_id)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::AGENT_SAVE_ROUTE,
                        contract::AgentMutationResponse { accepted: true },
                    )?;
                }
                contract::AGENT_DELETE_ROUTE => {
                    let payload: contract::AgentDeleteRequest =
                        ctx.decode_request_body(&request).await?;
                    let deleted = tokio::task::spawn_blocking(move || {
                        delete_definition(&data_dir, &payload.id)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::AGENT_DELETE_ROUTE,
                        contract::AgentDeleteResponse { deleted },
                    )?;
                }
                contract::AGENT_IMPORT_ROUTE => {
                    let payload: contract::AgentImportRequest =
                        ctx.decode_request_body(&request).await?;
                    let id = tokio::task::spawn_blocking(move || {
                        import_definition(&data_dir, &payload.path)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::AGENT_IMPORT_ROUTE,
                        contract::AgentImportResponse { id },
                    )?;
                }
                contract::AGENT_EXPORT_ROUTE => {
                    let payload: contract::AgentExportRequest =
                        ctx.decode_request_body(&request).await?;
                    let path = tokio::task::spawn_blocking(move || {
                        export_definition(
                            &data_dir,
                            &service.daemon_address,
                            &runtime,
                            &payload.id,
                            &payload.output,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::AGENT_EXPORT_ROUTE,
                        contract::AgentExportResponse { path },
                    )?;
                }
                contract::AGENT_VALIDATE_ROUTE | contract::AGENT_TEST_ROUTE => {
                    let payload: contract::AgentInspectRequest =
                        ctx.decode_request_body(&request).await?;
                    let run_round_trip = request.route == contract::AGENT_TEST_ROUTE;
                    let report = tokio::task::spawn_blocking(move || {
                        inspect_definition(
                            &data_dir,
                            &service.daemon_address,
                            &runtime,
                            &payload.id,
                            run_round_trip,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(request.route, contract::AgentInspectResponse { report })?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn agent_snapshot(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    daemon_address: &str,
    runtime: &tokio::runtime::Handle,
    selected_agent_id: Option<&str>,
) -> contract::AgentConfigSnapshot {
    let catalog = AgentChatSifsStorage::new(data_dir.clone(), daemon_address, runtime)
        .open_agent_catalog_vfs()
        .and_then(|vfs| AgentCatalog::load_from_vfs(vfs).map_err(|error| error.to_string()));
    let (catalog, error) = match catalog {
        Ok(catalog) => (catalog, None),
        Err(error) => (AgentCatalog::built_ins(), Some(error)),
    };
    let mut definitions = catalog.list().cloned().collect::<Vec<_>>();
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    let selected = selected_agent_id
        .and_then(|id| definitions.iter().find(|definition| definition.key() == id))
        .or_else(|| definitions.iter().find(|definition| editable(definition)))
        .or_else(|| definitions.first())
        .and_then(|definition| contract_definition(definition).ok());
    let mut tools = default_agent_tool_descriptors();
    tools.extend([web_search_descriptor(), image_generation_descriptor()]);
    contract::AgentConfigSnapshot {
        agents: definitions.iter().map(summary).collect(),
        selected_agent: selected,
        available_tools: tools
            .iter()
            .filter_map(|descriptor| contract_tool_descriptor(descriptor).ok())
            .collect(),
        error,
    }
}

fn save_definition(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    definition: contract::AgentDefinition,
    previous_id: Option<String>,
) -> Result<(), String> {
    let definition = host_definition(definition)?;
    if !editable(&definition) {
        return Err("only custom agent definitions can be saved".to_string());
    }
    definition.validate().map_err(|error| error.to_string())?;
    let store =
        AgentDefinitionSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    store
        .save_definition(&definition)
        .map_err(|error| error.to_string())?;
    if let Some(previous_id) = previous_id
        && previous_id != definition.key()
    {
        store
            .delete_definition(&previous_id)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn delete_definition(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    id: &str,
) -> Result<bool, String> {
    if AgentDefinition::built_in(id).is_some() {
        return Err("built-in and module-provided agents cannot be deleted".to_string());
    }
    let store =
        AgentDefinitionSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let agent = store
        .load_definitions()
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|agent| agent.key() == id)
        .ok_or_else(|| format!("agent definition `{id}` was not found"))?;
    if !editable(&agent) {
        return Err("built-in and module-provided agents cannot be deleted".to_string());
    }
    store
        .delete_definition(id)
        .map_err(|error| error.to_string())
}

fn import_definition(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    path: &Path,
) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut definition: AgentDefinition =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    definition.package.provenance = AgentPackageProvenance::imported(
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .display()
            .to_string(),
    );
    definition.validate().map_err(|error| error.to_string())?;
    let id = definition.key().to_string();
    AgentDefinitionSifsStore::open(Some(data_dir.root()))
        .map_err(|error| error.to_string())?
        .save_definition(&definition)
        .map_err(|error| error.to_string())?;
    Ok(id)
}

fn export_definition(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    daemon_address: &str,
    runtime: &tokio::runtime::Handle,
    id: &str,
    output: &Path,
) -> Result<PathBuf, String> {
    let definition = resolve_definition(data_dir, daemon_address, runtime, id)?;
    let output = if output.is_dir() || output.extension().is_none() {
        output.join(format!("{}.json", definition.key()))
    } else {
        output.to_path_buf()
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&definition).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(output)
}

fn inspect_definition(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    daemon_address: &str,
    runtime: &tokio::runtime::Handle,
    id: &str,
    round_trip: bool,
) -> Result<contract::AgentValidationReport, String> {
    let definition = resolve_definition(data_dir, daemon_address, runtime, id)?;
    let validation = definition.validate();
    let mut checks = vec![contract::AgentValidationCheck {
        name: "Package validation".to_string(),
        passed: validation.is_ok(),
        detail: validation
            .map(|_| "Package schema and fields are valid".to_string())
            .unwrap_or_else(|error| error.to_string()),
    }];
    if round_trip {
        let passed = serde_json::to_vec(&definition)
            .and_then(|bytes| serde_json::from_slice::<AgentDefinition>(&bytes))
            .map(|decoded| decoded == definition)
            .unwrap_or(false);
        checks.push(contract::AgentValidationCheck {
            name: "Package round trip".to_string(),
            passed,
            detail: if passed {
                "Package exports and imports without losing data".to_string()
            } else {
                "Package changed during JSON export/import".to_string()
            },
        });
    }
    Ok(contract::AgentValidationReport {
        agent_id: id.to_string(),
        checks,
    })
}

fn resolve_definition(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    daemon_address: &str,
    runtime: &tokio::runtime::Handle,
    id: &str,
) -> Result<AgentDefinition, String> {
    if let Some(definition) = AgentDefinition::built_in(id) {
        return Ok(definition);
    }
    let vfs = AgentChatSifsStorage::new(data_dir.clone(), daemon_address, runtime)
        .open_agent_catalog_vfs()?;
    AgentCatalog::load_from_vfs(vfs)
        .map_err(|error| error.to_string())?
        .get(id)
        .cloned()
        .ok_or_else(|| format!("agent definition `{id}` was not found"))
}

fn summary(definition: &AgentDefinition) -> contract::AgentConfigSummary {
    contract::AgentConfigSummary {
        id: definition.key().to_string(),
        name: if definition.name.trim().is_empty() {
            definition.key().to_string()
        } else {
            definition.name.clone()
        },
        description: definition.description.clone(),
        role: role_label(&definition.role).to_string(),
        source: source_label(&definition.package.provenance.source).to_string(),
        package_version: definition.package.version.clone(),
        provenance: provenance_label(definition),
        readiness: if definition.validate().is_ok() {
            contract::AgentReadiness::Ready
        } else {
            contract::AgentReadiness::NeedsAttention
        },
        editable: editable(definition),
        tool_count: definition.tools.len(),
        mcp_server_count: definition.tool_runtime.mcp_servers.len(),
        skill_count: definition.skills.len(),
        default_model: match (
            definition.model_policy.provider.as_deref(),
            definition.model_policy.model.as_deref(),
        ) {
            (Some(provider), Some(model)) => format!("{provider} / {model}"),
            (Some(provider), None) => format!("{provider} default"),
            (None, Some(model)) => model.to_string(),
            (None, None) => "Cluster default".to_string(),
        },
        home_server_node_id: definition.placement.home_server_node_id.clone(),
        sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
    }
}

fn contract_definition(definition: &AgentDefinition) -> Result<contract::AgentDefinition, String> {
    serde_json::from_value(serde_json::to_value(definition).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn host_definition(definition: contract::AgentDefinition) -> Result<AgentDefinition, String> {
    serde_json::from_value(serde_json::to_value(definition).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn contract_tool_descriptor(
    descriptor: &AgentToolDescriptor,
) -> Result<contract::AgentToolDescriptor, String> {
    serde_json::from_value(serde_json::to_value(descriptor).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn editable(definition: &AgentDefinition) -> bool {
    matches!(
        definition.package.provenance.source,
        AgentPackageSource::Local | AgentPackageSource::Imported
    )
}

fn role_label(role: &AgentRole) -> &'static str {
    match role {
        AgentRole::Main => "main",
        AgentRole::Background => "background",
        AgentRole::Deliberation => "deliberation",
        AgentRole::Evolution => "evolution",
    }
}

fn source_label(source: &AgentPackageSource) -> &'static str {
    match source {
        AgentPackageSource::BuiltIn => "built-in",
        AgentPackageSource::Local => "custom",
        AgentPackageSource::Imported => "imported",
        AgentPackageSource::Module => "module",
        AgentPackageSource::Unknown => "unknown",
    }
}

fn provenance_label(definition: &AgentDefinition) -> String {
    match definition.package.provenance.source {
        AgentPackageSource::BuiltIn => "Bundled with Sinomo".to_string(),
        AgentPackageSource::Local => "Custom package stored in SIFS".to_string(),
        AgentPackageSource::Imported => definition
            .package
            .provenance
            .path
            .as_deref()
            .map(|path| format!("Imported from {path}"))
            .unwrap_or_else(|| "Imported package".to_string()),
        AgentPackageSource::Module => format!(
            "Provided by {} · {}",
            definition
                .package
                .provenance
                .module_id
                .as_deref()
                .unwrap_or("unknown module"),
            definition
                .package
                .provenance
                .module_version
                .as_deref()
                .unwrap_or(&definition.package.version)
        ),
        AgentPackageSource::Unknown => "Package provenance unavailable".to_string(),
    }
}

pub fn register_hosted_agent_management_service(
    builder: ServiceRegistryBuilder,
    daemon_address: impl Into<String>,
) -> ServiceRegistryBuilder {
    builder.register(HostedAgentManagementService::new(daemon_address))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_management_routes_are_local_and_capability_scoped() {
        let routes = HostedAgentManagementService::new("127.0.0.1:23572").route_categories();
        assert_eq!(routes.len(), contract::AGENT_MANAGEMENT_RPC_ROUTES.len());
        for route in routes {
            assert_eq!(
                route.auth_scope.as_deref(),
                Some(contract::AGENT_MANAGEMENT_ROUTE_AUTH_SCOPE)
            );
            assert_eq!(route.required_capabilities.len(), 1);
        }
    }
}
