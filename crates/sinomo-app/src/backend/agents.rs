//! Agent definition snapshots and mutations for the Settings UI.

use sinomo_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionSifsStore, AgentDefinitionStore,
    AgentPackageSource, AgentRole,
};

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
    pub editable: bool,
    pub tool_count: usize,
    pub skill_count: usize,
    pub sandbox_node_id: Option<String>,
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
            editable: agent_definition_is_editable(definition),
            tool_count: definition.tools.len(),
            skill_count: definition.skills.len(),
            sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
        }
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
    use sinomo_agent::{AgentDefinition, AgentPackageDefinition};

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
}
