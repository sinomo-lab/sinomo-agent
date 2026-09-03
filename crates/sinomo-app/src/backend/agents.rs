//! Thin app adapter for daemon-owned agent definition management.

use std::path::{Path, PathBuf};

#[cfg(not(test))]
use sinomo_client::AgentManagementClient;
#[cfg(not(test))]
use sinomo_client::agent_contract::AgentSaveRequest;
pub use sinomo_client::agent_contract::{
    AgentConfigSnapshot, AgentConfigSummary, AgentDefinition, AgentMcpProtocolEra,
    AgentMcpServerDefinition, AgentOutputFormat, AgentPackageDefinition, AgentPackageSource,
    AgentReadiness, AgentRole, AgentToolDescriptor, AgentToolInvocationMode, AgentToolSpec,
    AgentValidationCheck, AgentValidationReport, agent_definition_is_editable,
    agent_home_sifs_path_for_id, agent_role_label, agent_source_label,
    fallback_agent_tool_descriptors,
};

use crate::backend::Backend;

pub fn built_in_agent_config_snapshot(
    selected_agent_id: Option<&str>,
    error: Option<String>,
) -> AgentConfigSnapshot {
    let definition = AgentDefinition::main();
    let selected_agent = selected_agent_id
        .is_none_or(|id| id == definition.key())
        .then_some(definition.clone());
    AgentConfigSnapshot {
        agents: vec![AgentConfigSummary {
            id: definition.id.clone(),
            name: definition.name.clone(),
            description: definition.description.clone(),
            role: "main".to_string(),
            source: "built-in".to_string(),
            package_version: definition.package.version.clone(),
            provenance: "Bundled with Sinomo".to_string(),
            module_id: None,
            module_version: None,
            readiness: AgentReadiness::Ready,
            editable: false,
            tool_count: definition.tools.len(),
            mcp_server_count: 0,
            skill_count: 0,
            default_model: "Cluster default".to_string(),
            home_server_node_id: None,
            sandbox_node_id: None,
        }],
        selected_agent,
        available_tools: fallback_agent_tool_descriptors(),
        error,
    }
}

impl Backend {
    pub fn agent_config_snapshot(&self, selected_agent_id: Option<&str>) -> AgentConfigSnapshot {
        #[cfg(test)]
        {
            return test_agent_snapshot(self, selected_agent_id);
        }
        #[cfg(not(test))]
        match self.agent_management_client().and_then(|client| {
            client
                .snapshot(selected_agent_id.map(str::to_string))
                .map_err(|error| error.to_string())
        }) {
            Ok(snapshot) => snapshot,
            Err(error) => built_in_agent_config_snapshot(selected_agent_id, Some(error)),
        }
    }

    pub fn save_custom_agent_definition(
        &self,
        definition: &AgentDefinition,
        previous_id: Option<&str>,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            return test_save_definition(self, definition, previous_id);
        }
        #[cfg(not(test))]
        self.agent_management_client()?
            .save(AgentSaveRequest {
                definition: definition.clone(),
                previous_id: previous_id.map(str::to_string),
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn delete_custom_agent_definition(&self, id: &str) -> Result<bool, String> {
        #[cfg(test)]
        {
            return test_delete_definition(self, id);
        }
        #[cfg(not(test))]
        self.agent_management_client()?
            .delete(id)
            .map_err(|error| error.to_string())
    }

    pub fn import_agent_definition(&self, path: &Path) -> Result<String, String> {
        #[cfg(test)]
        {
            return test_import_definition(self, path);
        }
        #[cfg(not(test))]
        self.agent_management_client()?
            .import(path.to_path_buf())
            .map_err(|error| error.to_string())
    }

    pub fn export_agent_definition(&self, id: &str, output: &Path) -> Result<PathBuf, String> {
        #[cfg(test)]
        {
            return test_export_definition(self, id, output);
        }
        #[cfg(not(test))]
        self.agent_management_client()?
            .export(id, output.to_path_buf())
            .map_err(|error| error.to_string())
    }

    pub fn validate_agent_definition(&self, id: &str) -> Result<AgentValidationReport, String> {
        #[cfg(test)]
        {
            return test_inspect_definition(self, id, false);
        }
        #[cfg(not(test))]
        self.agent_management_client()?
            .validate(id)
            .map_err(|error| error.to_string())
    }

    pub fn test_agent_definition(&self, id: &str) -> Result<AgentValidationReport, String> {
        #[cfg(test)]
        {
            return test_inspect_definition(self, id, true);
        }
        #[cfg(not(test))]
        self.agent_management_client()?
            .test(id)
            .map_err(|error| error.to_string())
    }

    #[cfg(not(test))]
    fn agent_management_client(&self) -> Result<AgentManagementClient, String> {
        self.connection
            .daemon_address()
            .map(AgentManagementClient::at_address)
            .ok_or_else(|| "agent management requires a daemon connection".to_string())
    }
}

#[cfg(test)]
fn test_agent_snapshot(backend: &Backend, selected_agent_id: Option<&str>) -> AgentConfigSnapshot {
    let catalog = backend
        .catalog_read_vfs()
        .and_then(|vfs| sinomo_agent::AgentCatalog::load_from_vfs(vfs).map_err(|e| e.to_string()));
    let (catalog, error) = match catalog {
        Ok(catalog) => (catalog, None),
        Err(error) => (sinomo_agent::AgentCatalog::built_ins(), Some(error)),
    };
    let definitions = catalog.list().cloned().collect::<Vec<_>>();
    let selected_agent = selected_agent_id
        .and_then(|id| definitions.iter().find(|definition| definition.key() == id))
        .or_else(|| {
            definitions
                .iter()
                .find(|definition| test_host_editable(definition))
        })
        .or_else(|| definitions.first())
        .and_then(test_contract_definition);
    let mut descriptors = sinomo_agent::default_agent_tool_descriptors();
    descriptors.extend([
        sinomo_agent::web_search_descriptor(),
        sinomo_agent::image_generation_descriptor(),
    ]);
    AgentConfigSnapshot {
        agents: definitions.iter().map(test_summary).collect(),
        selected_agent,
        available_tools: descriptors
            .iter()
            .filter_map(|descriptor| {
                serde_json::to_value(descriptor)
                    .ok()
                    .and_then(|value| serde_json::from_value(value).ok())
            })
            .collect(),
        error,
    }
}

#[cfg(test)]
fn test_save_definition(
    backend: &Backend,
    definition: &AgentDefinition,
    previous_id: Option<&str>,
) -> Result<(), String> {
    use sinomo_agent::AgentDefinitionStore;
    let host = test_host_definition(definition)?;
    host.validate().map_err(|error| error.to_string())?;
    let store = sinomo_agent::AgentDefinitionSifsStore::open(Some(backend.data_dir.root()))
        .map_err(|error| error.to_string())?;
    store
        .save_definition(&host)
        .map_err(|error| error.to_string())?;
    if let Some(previous_id) = previous_id
        && previous_id != definition.key()
    {
        store
            .delete_definition(previous_id)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
fn test_delete_definition(backend: &Backend, id: &str) -> Result<bool, String> {
    use sinomo_agent::AgentDefinitionStore;
    if sinomo_agent::AgentDefinition::built_in(id).is_some() {
        return Err("built-in and module-provided agents cannot be deleted".to_string());
    }
    sinomo_agent::AgentDefinitionSifsStore::open(Some(backend.data_dir.root()))
        .map_err(|error| error.to_string())?
        .delete_definition(id)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
fn test_import_definition(backend: &Backend, path: &Path) -> Result<String, String> {
    use sinomo_agent::AgentDefinitionStore;
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let definition: AgentDefinition =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let id = definition.key().to_string();
    test_save_definition(backend, &definition, None)?;
    let store = sinomo_agent::AgentDefinitionSifsStore::open(Some(backend.data_dir.root()))
        .map_err(|error| error.to_string())?;
    let mut host = test_host_definition(&definition)?;
    host.package.provenance = sinomo_agent::AgentPackageProvenance::imported(
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .display()
            .to_string(),
    );
    store
        .save_definition(&host)
        .map_err(|error| error.to_string())?;
    Ok(id)
}

#[cfg(test)]
fn test_export_definition(backend: &Backend, id: &str, output: &Path) -> Result<PathBuf, String> {
    let definition = test_agent_snapshot(backend, Some(id))
        .selected_agent
        .filter(|definition| definition.key() == id)
        .ok_or_else(|| format!("agent definition `{id}` was not found"))?;
    let output = if output.is_dir() || output.extension().is_none() {
        output.join(format!("{id}.json"))
    } else {
        output.to_path_buf()
    };
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        &output,
        serde_json::to_vec_pretty(&definition).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(output)
}

#[cfg(test)]
fn test_inspect_definition(
    backend: &Backend,
    id: &str,
    round_trip: bool,
) -> Result<AgentValidationReport, String> {
    let definition = test_agent_snapshot(backend, Some(id))
        .selected_agent
        .filter(|definition| definition.key() == id)
        .ok_or_else(|| format!("agent definition `{id}` was not found"))?;
    let validation = definition.validate();
    let mut checks = vec![AgentValidationCheck {
        name: "Package validation".to_string(),
        passed: validation.is_ok(),
        detail: validation
            .map(|_| "Package schema and fields are valid".to_string())
            .unwrap_or_else(|error| error),
    }];
    checks.extend(
        [
            ("Provider binding", "Provider selection is valid"),
            ("Agent placement", "Agent placement is available"),
            ("Sandbox placement", "Sandbox placement is available"),
            (
                "Skill dependencies",
                "Packaged skill dependencies are available",
            ),
        ]
        .into_iter()
        .map(|(name, detail)| AgentValidationCheck {
            name: name.to_string(),
            passed: true,
            detail: detail.to_string(),
        }),
    );
    if round_trip {
        let passed = serde_json::to_vec(&definition)
            .and_then(|bytes| serde_json::from_slice::<AgentDefinition>(&bytes))
            .map(|decoded| decoded == definition)
            .unwrap_or(false);
        checks.push(AgentValidationCheck {
            name: "Package round trip".to_string(),
            passed,
            detail: if passed {
                "Package exports and imports without losing data".to_string()
            } else {
                "Package changed during JSON export/import".to_string()
            },
        });
    }
    Ok(AgentValidationReport {
        agent_id: id.to_string(),
        checks,
    })
}

#[cfg(test)]
fn test_contract_definition(definition: &sinomo_agent::AgentDefinition) -> Option<AgentDefinition> {
    serde_json::to_value(definition)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
}

#[cfg(test)]
fn test_host_definition(
    definition: &AgentDefinition,
) -> Result<sinomo_agent::AgentDefinition, String> {
    serde_json::to_value(definition)
        .map_err(|error| error.to_string())
        .and_then(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
}

#[cfg(test)]
fn test_host_editable(definition: &sinomo_agent::AgentDefinition) -> bool {
    matches!(
        definition.package.provenance.source,
        sinomo_agent::AgentPackageSource::Local | sinomo_agent::AgentPackageSource::Imported
    )
}

#[cfg(test)]
fn test_summary(definition: &sinomo_agent::AgentDefinition) -> AgentConfigSummary {
    AgentConfigSummary {
        id: definition.key().to_string(),
        name: definition.name.clone(),
        description: definition.description.clone(),
        role: format!("{:?}", definition.role).to_lowercase(),
        source: match definition.package.provenance.source {
            sinomo_agent::AgentPackageSource::BuiltIn => "built-in",
            sinomo_agent::AgentPackageSource::Local => "custom",
            sinomo_agent::AgentPackageSource::Imported => "imported",
            sinomo_agent::AgentPackageSource::Module => "module",
            sinomo_agent::AgentPackageSource::Unknown => "unknown",
        }
        .to_string(),
        package_version: definition.package.version.clone(),
        provenance: "test catalog".to_string(),
        module_id: definition.package.provenance.module_id.clone(),
        module_version: definition.package.provenance.module_version.clone(),
        readiness: if definition.validate().is_ok() {
            AgentReadiness::Ready
        } else {
            AgentReadiness::NeedsAttention
        },
        editable: test_host_editable(definition),
        tool_count: definition.tools.len(),
        mcp_server_count: definition.tool_runtime.mcp_servers.len(),
        skill_count: definition.skills.len(),
        default_model: definition
            .model_policy
            .model
            .clone()
            .unwrap_or_else(|| "Cluster default".to_string()),
        home_server_node_id: definition.placement.home_server_node_id.clone(),
        sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
    }
}
