use super::{
    AGENT_SKILL_EVOLUTION_EVIDENCE_KIND, AGENT_SKILL_USAGE_EVIDENCE_KIND, AgentTool,
    AgentToolCapability, AgentToolDescriptor, AgentToolEvidence, AgentToolRequest, AgentToolResult,
};
use crate::{
    AgentToolCall, AgentToolKind, AgentToolSpec, SkillActivationScope, SkillCatalogEntry,
    SkillCatalogError, SkillCatalogSifsStore, SkillCatalogSource, SkillCatalogStore,
    SkillCatalogWrite, TaskId,
};
use sinomo_runtime::local::SinomoDataDir;

#[derive(Clone)]
pub struct SkillAgentToolConfig {
    data_dir: SinomoDataDir,
    allow_writes: bool,
}

impl SkillAgentToolConfig {
    pub fn new(data_dir: SinomoDataDir) -> Self {
        Self {
            data_dir,
            allow_writes: true,
        }
    }

    pub fn allowing_writes(mut self, allow_writes: bool) -> Self {
        self.allow_writes = allow_writes;
        self
    }
}

#[derive(Clone)]
pub struct SkillUpdateTool {
    config: SkillAgentToolConfig,
}

impl SkillUpdateTool {
    pub fn new(config: SkillAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for SkillUpdateTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        skill_update_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if !self.config.allow_writes {
            return AgentToolResult::blocked(
                "skill_update is disabled for this agent runtime",
                serde_json::json!({ "error": "skill_writes_disabled" }),
            );
        }
        let Some(skill_id) = input_string(request.call, "skill") else {
            return AgentToolResult::failed(
                "skill_update requires a string skill",
                serde_json::json!({ "error": "missing_skill" }),
            );
        };
        let Some(markdown) =
            input_text(request.call, "markdown").or_else(|| input_text(request.call, "text"))
        else {
            return AgentToolResult::failed(
                "skill_update requires full replacement SKILL.md markdown",
                serde_json::json!({ "error": "missing_markdown" }),
            );
        };
        let version = input_string(request.call, "version")
            .unwrap_or_else(|| format!("evolved-{}", TaskId::generate()));
        let scope = input_string(request.call, "scope")
            .unwrap_or_else(|| SkillActivationScope::Cluster.as_str().to_string());
        let reason = input_string(request.call, "reason")
            .unwrap_or_else(|| "prompt-based skill update".to_string());
        let source_id = input_string(request.call, "source_id")
            .unwrap_or_else(|| request.session.session_id.to_string());
        let activate = input_bool(request.call, "activate").unwrap_or(true);

        let store = match SkillCatalogSifsStore::open(Some(self.config.data_dir.root())) {
            Ok(store) => store,
            Err(error) => return skill_error_result("open skill catalog", error),
        };
        let catalog = match store.load_catalog() {
            Ok(catalog) => catalog,
            Err(error) => return skill_error_result("load skill catalog", error),
        };
        let active_entry = catalog
            .active_entry_for_scope(&scope, &skill_id)
            .cloned()
            .or_else(|| {
                catalog
                    .entries
                    .iter()
                    .find(|entry| entry.skill_id == skill_id)
                    .cloned()
            });
        let previous_content_hash = active_entry
            .as_ref()
            .map(|entry| entry.content_hash.clone());

        let mut write = match SkillCatalogWrite::from_markdown_skill(
            Some(skill_id.clone()),
            version,
            SkillCatalogSource::evolution(source_id.clone()),
            markdown,
        ) {
            Ok(write) => write,
            Err(error) => return skill_error_result("prepare skill update", error),
        };
        if let Some(active) = active_entry.as_ref() {
            write.entry = inherit_skill_metadata(write.entry, active);
        }
        if activate {
            write = write.activate_for_scope(scope.clone(), "agent", reason.clone());
        }
        let entry = write.entry.clone();
        if let Err(error) = store.write_skill_version(write) {
            return skill_error_result("write skill update", error);
        }

        let output = serde_json::json!({
            "status": "updated",
            "skill": skill_entry_json(&entry),
            "activated": activate,
            "scope": scope,
            "source_id": source_id,
            "previous_content_hash": previous_content_hash,
        });
        let evidence = AgentToolEvidence::availability_only(
            true,
            "wrote prompt-based skill update through the Sinomo skill catalog",
        )
        .with_output_value(&output, false)
        .with_record_value(
            AGENT_SKILL_USAGE_EVIDENCE_KIND,
            serde_json::json!({
                "operation": "update",
                "skill": entry.skill_id,
                "version": entry.version,
                "activated": activate,
                "scope": scope,
                "source": "evolution",
            }),
        )
        .with_record_value(
            AGENT_SKILL_EVOLUTION_EVIDENCE_KIND,
            serde_json::json!({
                "operation": "prompt_update",
                "skill": entry.skill_id,
                "version": entry.version,
                "base_hash": entry.base_hash,
                "content_hash": entry.content_hash,
                "reason": reason,
            }),
        );
        AgentToolResult::succeeded(
            format!("updated skill {}@{}", entry.skill_id, entry.version),
            output,
        )
        .with_evidence(evidence)
    }
}

pub fn skill_tool_descriptors() -> Vec<AgentToolDescriptor> {
    vec![skill_update_descriptor()]
}

pub fn registry(config: SkillAgentToolConfig) -> super::AgentToolRegistry {
    super::AgentToolRegistry::new().with_tool(SkillUpdateTool::new(config))
}

pub fn register_tools(
    registry: &super::AgentToolRegistry,
    config: SkillAgentToolConfig,
) -> Vec<Option<AgentToolDescriptor>> {
    vec![registry.register(SkillUpdateTool::new(config))]
}

fn skill_update_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::SkillUpdate,
        "skill_update",
        "Persist a prompt-based improvement as a new catalog skill version from full SKILL.md markdown.",
        true,
    ))
    .with_capabilities([AgentToolCapability::ManageSkills])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["skill", "markdown"],
        "properties": {
            "skill": {
                "type": "string",
                "description": "Skill id to create or update."
            },
            "markdown": {
                "type": "string",
                "description": "Full replacement SKILL.md content. Include portable frontmatter when possible."
            },
            "version": {
                "type": "string",
                "description": "Optional new version. Defaults to an evolved-* version id."
            },
            "activate": {
                "type": "boolean",
                "description": "Whether to activate the new version after writing. Defaults to true."
            },
            "scope": {
                "type": "string",
                "description": "Activation scope. Defaults to cluster."
            },
            "reason": {
                "type": "string",
                "description": "Short reason or learning that justified the update."
            },
            "source_id": {
                "type": "string",
                "description": "Optional evolution source id. Defaults to the interaction session id."
            }
        }
    }))
}

fn inherit_skill_metadata(
    mut entry: SkillCatalogEntry,
    active: &SkillCatalogEntry,
) -> SkillCatalogEntry {
    entry.base_hash = Some(active.content_hash.clone());
    if entry.description.is_empty() {
        entry.description = active.description.clone();
    }
    if entry.target_agents.is_empty() {
        entry.target_agents = active.target_agents.clone();
    }
    if entry.model_families.is_empty() {
        entry.model_families = active.model_families.clone();
    }
    if entry.activation_tags.is_empty() {
        entry.activation_tags = active.activation_tags.clone();
    }
    if entry.required_capabilities.is_empty() {
        entry.required_capabilities = active.required_capabilities.clone();
    }
    entry
}

fn skill_entry_json(entry: &SkillCatalogEntry) -> serde_json::Value {
    serde_json::json!({
        "id": &entry.skill_id,
        "version": &entry.version,
        "description": &entry.description,
        "source": {
            "kind": entry.source.kind.as_str(),
            "id": &entry.source.source_id,
            "version": &entry.source.source_version,
        },
        "status": entry.status.as_str(),
        "path": &entry.sifs_path,
        "manifest_path": &entry.manifest_path,
        "content_hash": &entry.content_hash,
        "base_hash": &entry.base_hash,
        "target_agents": &entry.target_agents,
        "model_families": &entry.model_families,
        "activation_tags": &entry.activation_tags,
        "required_capabilities": &entry.required_capabilities,
    })
}

fn skill_error_result(context: &str, error: SkillCatalogError) -> AgentToolResult {
    AgentToolResult::failed(
        format!("{context}: {error}"),
        serde_json::json!({
            "error": "skill_catalog_error",
            "context": context,
            "detail": error.to_string(),
        }),
    )
}

fn input_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

fn input_text(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
}

fn input_bool(call: &AgentToolCall, key: &str) -> Option<bool> {
    call.input.get(key).and_then(serde_json::Value::as_bool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentToolStatus, InteractionSession, InteractionSurface, InteractionSurfaceKind};
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;

    #[test]
    fn skill_update_tool_writes_evolution_version_and_activates() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-skill-update-tool");
        let data_dir = SinomoDataDir::new(&root);
        let store = SkillCatalogSifsStore::open(Some(data_dir.root())).expect("store");
        let original_text = "---\nname: shortcut\ndescription: Use slow setup.\n---\n\n# Shortcut\nRun the slow setup.\n";
        let original_write = SkillCatalogWrite::from_markdown_skill(
            Some("shortcut".to_string()),
            "1.0.0",
            SkillCatalogSource::user("test"),
            original_text,
        )
        .expect("original write")
        .activate_for_scope(SkillActivationScope::Cluster.as_str(), "test", "seed");
        let original_entry = original_write.entry.clone();
        store
            .write_skill_version(original_write)
            .expect("write original");

        let registry = registry(SkillAgentToolConfig::new(data_dir.clone()));
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_skill_update_tool").expect("session"),
            NodeId::from_legacy_named("node_skill_update_tool").expect("node"),
            InteractionSurface::new("test", InteractionSurfaceKind::CommandLine),
        );
        let call = AgentToolCall {
            call_id: None,
            tool: "skill_update".to_string(),
            input: serde_json::json!({
                "skill": "shortcut",
                "version": "1.1.0",
                "source_id": "job-shortcut",
                "reason": "learned faster path",
                "markdown": "---\nname: shortcut\ndescription: Use fast setup.\n---\n\n# Shortcut\nRun the shortcut command.\n",
            }),
            r#async: false,
            summary: "record learned shortcut".to_string(),
        };

        let outcome = registry.execute(&session, 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert!(outcome.evidence.record_value("skill_usage").is_some());
        assert!(outcome.evidence.record_value("skill_evolution").is_some());
        let catalog = store.load_catalog().expect("catalog");
        let active = catalog
            .active_entry_for_scope(SkillActivationScope::Cluster.as_str(), "shortcut")
            .expect("active shortcut");
        assert_eq!(active.version, "1.1.0");
        assert_eq!(active.source.kind, crate::SkillCatalogSourceKind::Evolution);
        assert_eq!(active.source.source_id.as_deref(), Some("job-shortcut"));
        assert_eq!(
            active.base_hash.as_deref(),
            Some(original_entry.content_hash.as_str())
        );
        let updated_text = store
            .read_skill_text(active)
            .expect("read updated")
            .expect("updated text");
        assert!(updated_text.contains("Run the shortcut command."));
    }
}
