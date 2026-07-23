//! Agent definition snapshots and mutations for the Settings UI.

use std::fs;
use std::path::{Path, PathBuf};

use sinomo_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionSifsStore, AgentDefinitionStore,
    AgentPackageProvenance, AgentPackageSource, AgentRole, AgentToolRegistry,
    register_mcp_server_tools,
};
use sinomo_runtime::node::LocalNodePermissionStore;

use crate::backend::Backend;

#[derive(Clone, Debug, Default)]
pub struct AgentConfigSnapshot {
    pub agents: Vec<AgentConfigSummary>,
    pub selected_agent: Option<AgentDefinition>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentConfigSummary {
    pub id: String,
    pub name: String,
    pub description: String,
    pub role: String,
    pub source: String,
    pub package_version: String,
    pub provenance: String,
    pub readiness: AgentReadiness,
    pub editable: bool,
    pub tool_count: usize,
    pub mcp_server_count: usize,
    pub skill_count: usize,
    pub default_model: String,
    pub home_server_node_id: Option<String>,
    pub sandbox_node_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentReadiness {
    Ready,
    NeedsAttention,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentValidationCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentValidationReport {
    pub agent_id: String,
    pub checks: Vec<AgentValidationCheck>,
}

impl AgentValidationReport {
    pub fn is_ready(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }

    pub fn summary(&self) -> String {
        let passed = self.checks.iter().filter(|check| check.passed).count();
        format!("{passed} of {} checks passed", self.checks.len())
    }
}

/// Snapshot the built-in agent catalog only — no data-dir or SIFS I/O. Used as the
/// degraded fallback when the SIFS-backed catalog cannot be read in time.
pub fn built_in_agent_config_snapshot(
    selected_agent_id: Option<&str>,
    error: Option<String>,
) -> AgentConfigSnapshot {
    agent_config_snapshot_from_catalog(AgentCatalog::built_ins(), selected_agent_id, error)
}

fn agent_config_snapshot_from_catalog(
    catalog: AgentCatalog,
    selected_agent_id: Option<&str>,
    error: Option<String>,
) -> AgentConfigSnapshot {
    let mut definitions = catalog.list().cloned().collect::<Vec<_>>();
    definitions.sort_by(|left, right| {
        agent_definition_sort_group(left)
            .cmp(&agent_definition_sort_group(right))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.key().cmp(right.key()))
    });

    let selected_agent = selected_agent_id
        .and_then(|id| definitions.iter().find(|definition| definition.key() == id))
        .or_else(|| {
            definitions
                .iter()
                .find(|definition| agent_definition_is_editable(definition))
        })
        .or_else(|| definitions.first())
        .cloned();
    let agents = definitions
        .iter()
        .map(AgentConfigSummary::from_definition)
        .collect();
    AgentConfigSnapshot {
        agents,
        selected_agent,
        error,
    }
}

impl Backend {
    pub fn agent_config_snapshot(&self, selected_agent_id: Option<&str>) -> AgentConfigSnapshot {
        let (catalog, error) = match AgentCatalog::load_from_data_dir(self.data_dir.as_ref()) {
            Ok(catalog) => (catalog, None),
            Err(source) => (AgentCatalog::built_ins(), Some(source.to_string())),
        };
        agent_config_snapshot_from_catalog(catalog, selected_agent_id, error)
    }

    pub fn save_custom_agent_definition(
        &self,
        definition: &AgentDefinition,
        previous_id: Option<&str>,
    ) -> Result<(), String> {
        if !agent_definition_is_editable(definition) {
            return Err("only custom agent definitions can be saved".to_string());
        }
        let store = AgentDefinitionSifsStore::open(Some(self.data_dir.root()))
            .map_err(|error| error.to_string())?;
        store
            .save_definition(definition)
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

    pub fn delete_custom_agent_definition(&self, id: &str) -> Result<bool, String> {
        let snapshot = self.agent_config_snapshot(Some(id));
        let Some(agent) = snapshot
            .selected_agent
            .as_ref()
            .filter(|agent| agent.key() == id)
        else {
            return Err(format!("agent definition `{id}` was not found"));
        };
        if !agent_definition_is_editable(agent) {
            return Err("built-in and module-provided agents cannot be deleted".to_string());
        }
        AgentDefinitionSifsStore::open(Some(self.data_dir.root()))
            .map_err(|error| error.to_string())?
            .delete_definition(id)
            .map_err(|error| error.to_string())
    }

    pub fn import_agent_definition(&self, path: &Path) -> Result<String, String> {
        let bytes = fs::read(path).map_err(|error| {
            format!(
                "failed to read agent package at {}: {error}",
                path.display()
            )
        })?;
        let mut definition: AgentDefinition = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "failed to parse agent package at {}: {error}",
                path.display()
            )
        })?;
        definition.package.provenance =
            AgentPackageProvenance::imported(agent_import_source_path(path));
        definition.validate().map_err(|error| error.to_string())?;
        let id = definition.key().to_string();
        AgentDefinitionSifsStore::open(Some(self.data_dir.root()))
            .map_err(|error| error.to_string())?
            .save_definition(&definition)
            .map_err(|error| error.to_string())?;
        Ok(id)
    }

    pub fn export_agent_definition(&self, id: &str, output: &Path) -> Result<PathBuf, String> {
        let definition = self.agent_definition(id)?;
        let output = agent_export_path(output, definition.key());
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create export directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let bytes = serde_json::to_vec_pretty(&definition).map_err(|error| error.to_string())?;
        fs::write(&output, bytes).map_err(|error| {
            format!(
                "failed to export agent package to {}: {error}",
                output.display()
            )
        })?;
        Ok(output)
    }

    pub fn validate_agent_definition(&self, id: &str) -> Result<AgentValidationReport, String> {
        let definition = self.agent_definition(id)?;
        Ok(agent_validation_report(&definition))
    }

    pub fn test_agent_definition(&self, id: &str) -> Result<AgentValidationReport, String> {
        let definition = self.agent_definition(id)?;
        let mut report = agent_validation_report(&definition);

        let round_trip = serde_json::to_vec(&definition)
            .and_then(|bytes| serde_json::from_slice::<AgentDefinition>(&bytes))
            .map(|decoded| decoded == definition)
            .unwrap_or(false);
        report.checks.push(AgentValidationCheck {
            name: "Package round trip".to_string(),
            passed: round_trip,
            detail: if round_trip {
                "Package exports and imports without losing data".to_string()
            } else {
                "Package changed during JSON export/import".to_string()
            },
        });

        let provider = definition.model_policy.provider.as_deref();
        let provider_ready = provider.is_none_or(|provider| {
            crate::backend::config::load_provider_config(self.config.as_ref())
                .unwrap_or_default()
                .accounts
                .iter()
                .any(|account| account.enabled && account.provider == provider)
        });
        report.checks.push(AgentValidationCheck {
            name: "Provider binding".to_string(),
            passed: provider_ready,
            detail: match provider {
                None => "Uses the cluster default provider".to_string(),
                Some(provider) if provider_ready => {
                    format!("Enabled provider `{provider}` is configured")
                }
                Some(provider) => format!("Provider `{provider}` is not configured and enabled"),
            },
        });

        let home_server = definition.placement.home_server_node_id.as_deref();
        let home_server_ready =
            home_server.is_none_or(|node_id| node_id == self.node.node_id.as_str());
        report.checks.push(AgentValidationCheck {
            name: "Agent placement".to_string(),
            passed: home_server_ready,
            detail: match home_server {
                None => "Uses the local agent runtime".to_string(),
                Some(node_id) if home_server_ready => {
                    format!("Home server `{node_id}` is local")
                }
                Some(node_id) => format!(
                    "Home server `{node_id}` is remote; remote agent routing is unavailable"
                ),
            },
        });

        let sandbox_node = definition.tool_runtime.sandbox_node_id.as_deref();
        let node_ready = sandbox_node.is_none_or(|node_id| node_id == self.node.node_id.as_str());
        report.checks.push(AgentValidationCheck {
            name: "Sandbox placement".to_string(),
            passed: node_ready,
            detail: match sandbox_node {
                None => "Uses the local runtime placement".to_string(),
                Some(node_id) if node_ready => format!("Sandbox node `{node_id}` is local"),
                Some(node_id) => format!(
                    "Sandbox node `{node_id}` is remote; automatic remote sandbox routing is unavailable"
                ),
            },
        });

        let (skills_ready, skills_detail) = if definition.skills.is_empty() {
            (true, "No packaged skills required".to_string())
        } else {
            match sinomo_cli::app_api::skill_summaries(self.data_dir.as_ref(), true) {
                Ok(skills) => {
                    let missing = definition
                        .skills
                        .iter()
                        .filter(|required| {
                            !skills.iter().any(|skill| skill.id == required.as_str())
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if missing.is_empty() {
                        (
                            true,
                            format!("{} packaged skills are available", definition.skills.len()),
                        )
                    } else {
                        (false, format!("Missing skills: {}", missing.join(", ")))
                    }
                }
                Err(error) => (false, format!("Skill catalog unavailable: {error}")),
            }
        };
        report.checks.push(AgentValidationCheck {
            name: "Skill dependencies".to_string(),
            passed: skills_ready,
            detail: skills_detail,
        });

        let enabled_mcp_servers = definition
            .tool_runtime
            .mcp_servers
            .iter()
            .filter(|server| server.enabled)
            .collect::<Vec<_>>();
        if !enabled_mcp_servers.is_empty() {
            let permission = LocalNodePermissionStore::new(self.data_dir.root())
                .load()
                .map_err(|error| error.to_string())?;
            let integrations_allowed = permission.external_integrations_enabled();
            report.checks.push(AgentValidationCheck {
                name: "External integrations permission".to_string(),
                passed: integrations_allowed,
                detail: if integrations_allowed {
                    "This node allows configured MCP integrations".to_string()
                } else {
                    "Allow external integrations on this node before MCP tools can run".to_string()
                },
            });
            if integrations_allowed {
                for server in enabled_mcp_servers {
                    let registry = AgentToolRegistry::new();
                    let result = register_mcp_server_tools(&registry, server);
                    let (passed, detail) = match result {
                        Ok(registration) => {
                            let tools = registration.tools;
                            let detail = if tools.is_empty() {
                                "Connected successfully; the configured allow-list exposes no tools"
                                    .to_string()
                            } else {
                                format!(
                                    "Connected and discovered {}: {}",
                                    if tools.len() == 1 {
                                        "1 tool".to_string()
                                    } else {
                                        format!("{} tools", tools.len())
                                    },
                                    tools.join(", ")
                                )
                            };
                            (true, detail)
                        }
                        Err(error) if !server.required => (
                            true,
                            format!("Optional server is currently unavailable: {error}"),
                        ),
                        Err(error) => (false, error.to_string()),
                    };
                    report.checks.push(AgentValidationCheck {
                        name: format!("MCP · {}", server.name),
                        passed,
                        detail,
                    });
                }
            }
        }
        Ok(report)
    }

    fn agent_definition(&self, id: &str) -> Result<AgentDefinition, String> {
        AgentCatalog::load_from_data_dir(self.data_dir.as_ref())
            .map_err(|error| error.to_string())?
            .get(id)
            .cloned()
            .ok_or_else(|| format!("agent definition `{id}` was not found"))
    }
}

impl AgentConfigSummary {
    fn from_definition(definition: &AgentDefinition) -> Self {
        Self {
            id: definition.key().to_string(),
            name: if definition.name.trim().is_empty() {
                definition.key().to_string()
            } else {
                definition.name.clone()
            },
            description: definition.description.clone(),
            role: agent_role_label(&definition.role).to_string(),
            source: agent_source_label(&definition.package.provenance.source).to_string(),
            package_version: definition.package.version.clone(),
            provenance: agent_provenance_label(definition),
            readiness: if definition.validate().is_ok() {
                AgentReadiness::Ready
            } else {
                AgentReadiness::NeedsAttention
            },
            editable: agent_definition_is_editable(definition),
            tool_count: definition.tools.len(),
            mcp_server_count: definition.tool_runtime.mcp_servers.len(),
            skill_count: definition.skills.len(),
            default_model: agent_default_model_label(definition),
            home_server_node_id: definition.placement.home_server_node_id.clone(),
            sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
        }
    }
}

fn agent_default_model_label(definition: &AgentDefinition) -> String {
    match (
        definition.model_policy.provider.as_deref(),
        definition.model_policy.model.as_deref(),
    ) {
        (Some(provider), Some(model)) => format!("{provider} / {model}"),
        (Some(provider), None) => format!("{provider} default"),
        (None, Some(model)) => model.to_string(),
        (None, None) => "Cluster default".to_string(),
    }
}

fn agent_validation_report(definition: &AgentDefinition) -> AgentValidationReport {
    let validation = definition.validate();
    AgentValidationReport {
        agent_id: definition.key().to_string(),
        checks: vec![AgentValidationCheck {
            name: "Package validation".to_string(),
            passed: validation.is_ok(),
            detail: validation
                .map(|_| "Package schema and fields are valid".to_string())
                .unwrap_or_else(|error| error.to_string()),
        }],
    }
}

fn agent_provenance_label(definition: &AgentDefinition) -> String {
    let provenance = &definition.package.provenance;
    match provenance.source {
        AgentPackageSource::BuiltIn => "Bundled with Sinomo".to_string(),
        AgentPackageSource::Local => "Custom package stored in SIFS".to_string(),
        AgentPackageSource::Imported => provenance
            .path
            .as_deref()
            .map(|path| format!("Imported from {path}"))
            .unwrap_or_else(|| "Imported package".to_string()),
        AgentPackageSource::Module => {
            let module = provenance.module_id.as_deref().unwrap_or("unknown module");
            let version = provenance
                .module_version
                .as_deref()
                .unwrap_or(&definition.package.version);
            format!("Provided by {module} · {version}")
        }
        AgentPackageSource::Unknown => "Package provenance unavailable".to_string(),
    }
}

fn agent_import_source_path(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn agent_export_path(output: &Path, id: &str) -> PathBuf {
    if output.is_dir() || output.extension().is_none() {
        output.join(format!("{id}.json"))
    } else {
        output.to_path_buf()
    }
}

pub fn agent_definition_is_editable(definition: &AgentDefinition) -> bool {
    matches!(
        &definition.package.provenance.source,
        AgentPackageSource::Local | AgentPackageSource::Imported
    )
}

pub fn agent_role_label(role: &AgentRole) -> &'static str {
    match role {
        AgentRole::Main => "main",
        AgentRole::Background => "background",
        AgentRole::Deliberation => "deliberation",
        AgentRole::Evolution => "evolution",
    }
}

pub fn agent_source_label(source: &AgentPackageSource) -> &'static str {
    match source {
        AgentPackageSource::BuiltIn => "built-in",
        AgentPackageSource::Local => "custom",
        AgentPackageSource::Imported => "imported",
        AgentPackageSource::Module => "module",
        AgentPackageSource::Unknown => "unknown",
    }
}

fn agent_definition_sort_group(definition: &AgentDefinition) -> u8 {
    match &definition.package.provenance.source {
        AgentPackageSource::Local | AgentPackageSource::Imported => 0,
        AgentPackageSource::BuiltIn => 1,
        AgentPackageSource::Module => 2,
        AgentPackageSource::Unknown => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{AgentDefinition, AgentMcpServerDefinition, AgentPackageDefinition};

    #[test]
    fn backend_saves_and_deletes_custom_agent_definitions() {
        let backend = Backend::local_fallback();
        let mut definition =
            AgentDefinition::custom("docs-agent", AgentRole::Deliberation, "Write docs.");
        definition.description = "Documentation specialist".to_string();

        backend
            .save_custom_agent_definition(&definition, None)
            .expect("save custom agent");
        let snapshot = backend.agent_config_snapshot(Some("docs-agent"));
        assert!(
            snapshot.agents.iter().any(|agent| {
                agent.id == "docs-agent"
                    && agent.editable
                    && agent.description == "Documentation specialist"
            }),
            "saved custom agent should appear in the settings snapshot"
        );

        assert!(
            backend
                .delete_custom_agent_definition("docs-agent")
                .expect("delete custom agent")
        );
        let snapshot = backend.agent_config_snapshot(Some("docs-agent"));
        assert!(
            !snapshot.agents.iter().any(|agent| agent.id == "docs-agent"),
            "deleted custom agent should disappear from the settings snapshot"
        );
    }

    #[test]
    fn backend_rejects_saving_read_only_agent_definitions() {
        let backend = Backend::local_fallback();
        let mut definition = AgentDefinition::main();
        definition.package = AgentPackageDefinition::built_in();

        let error = backend
            .save_custom_agent_definition(&definition, None)
            .expect_err("built-in save should fail");
        assert!(error.contains("custom agent definitions"));
    }

    #[test]
    fn backend_imports_validates_tests_and_exports_agent_packages() {
        let backend = Backend::local_fallback();
        let source = backend.data_dir.root().join("incoming-agent.json");
        let export_dir = backend.data_dir.root().join("exports");
        let mut definition =
            AgentDefinition::custom("package-agent", AgentRole::Background, "Handle packages.");
        definition.name = "Package Agent".to_string();
        definition.description = "Exercises package lifecycle actions".to_string();
        fs::write(
            &source,
            serde_json::to_vec_pretty(&definition).expect("serialize package"),
        )
        .expect("write import package");

        let id = backend
            .import_agent_definition(&source)
            .expect("import agent package");
        assert_eq!(id, "package-agent");
        let snapshot = backend.agent_config_snapshot(Some(&id));
        let summary = snapshot
            .agents
            .iter()
            .find(|agent| agent.id == id)
            .expect("imported summary");
        assert_eq!(summary.source, "imported");
        assert_eq!(summary.readiness, AgentReadiness::Ready);
        assert!(summary.provenance.contains("incoming-agent.json"));

        let validation = backend
            .validate_agent_definition(&id)
            .expect("validate imported package");
        assert!(validation.is_ready(), "{validation:?}");
        let test = backend
            .test_agent_definition(&id)
            .expect("test imported package");
        assert!(test.is_ready(), "{test:?}");
        assert!(
            test.checks
                .iter()
                .any(|check| check.name == "Package round trip")
        );

        let exported = backend
            .export_agent_definition(&id, &export_dir)
            .expect("export agent package");
        assert_eq!(exported, export_dir.join("package-agent.json"));
        let exported_definition: AgentDefinition =
            serde_json::from_slice(&fs::read(&exported).expect("read exported package"))
                .expect("parse exported package");
        assert_eq!(exported_definition.key(), id);
        assert!(matches!(
            exported_definition.package.provenance.source,
            AgentPackageSource::Imported
        ));
    }

    #[test]
    fn summary_exposes_version_provenance_and_readiness() {
        let mut definition = AgentDefinition::main();
        definition.package = AgentPackageDefinition::built_in();
        let mut mcp = AgentMcpServerDefinition::streamable_http(
            "workspace",
            "https://mcp.example.test/tools",
        );
        mcp.enabled = false;
        definition.tool_runtime.mcp_servers.push(mcp);
        let summary = AgentConfigSummary::from_definition(&definition);

        assert_eq!(summary.package_version, definition.package.version);
        assert_eq!(summary.provenance, "Bundled with Sinomo");
        assert_eq!(summary.readiness, AgentReadiness::Ready);
        assert_eq!(summary.mcp_server_count, 1);
    }
}
