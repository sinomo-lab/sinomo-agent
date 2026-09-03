use sinomo_agent::{
    AgentDefinition, AgentPackageProvenance, AgentRole, AgentToolKind, AgentToolSpec,
    default_coding_agent_tools,
};
use sinomo_module_api::{
    NativeModuleAgentDefinition, NativeModuleAgentTool, NativeModuleDeclarativeResource,
};

use sinomo_app_contract::evolution as contract;

pub const EVOLUTION_AGENT_ID: &str = "evolution-agent";
pub(crate) const EVOLUTION_PLANNING_SKILL_ID: &str = "evolution-planning";
pub(crate) const EVOLUTION_REVIEW_SKILL_ID: &str = "evolution-review";

const EVOLUTION_AGENT_PROMPT: &str = r#"You are Sinomo's evolution agent model.

You operate inside Sinomo's supervised self-evolution loop. The runtime owns
tool execution, tool-surface availability, sandbox enforcement, event emission,
candidate evidence, and session state. Use tools when evidence or workspace
changes are needed, then continue after tool_result messages arrive.

Prefer small, auditable changes that satisfy the mutation packet and produce
clear verification evidence. When the next step is clear and no more tools are
needed, return an empty tool_calls array."#;

const EVOLUTION_PLANNING_SKILL_PATH: &str = "skills/evolution-planning/SKILL.md";
const EVOLUTION_REVIEW_SKILL_PATH: &str = "skills/evolution-review/SKILL.md";
const EVOLUTION_PLANNING_SKILL: &str = include_str!("../skills/evolution-planning/SKILL.md");
const EVOLUTION_REVIEW_SKILL: &str = include_str!("../skills/evolution-review/SKILL.md");
const EVOLUTION_DEFAULT_TOOLSETS: &[&str] = &[
    "core",
    "coding",
    "local",
    "interactive",
    "discovery",
    "version_control",
    contract::EVOLUTION_MODULE_OWNER,
];

pub fn evolution_agent_definition() -> AgentDefinition {
    let mut definition = AgentDefinition::custom(
        EVOLUTION_AGENT_ID,
        AgentRole::Evolution,
        EVOLUTION_AGENT_PROMPT,
    )
    .with_tools(evolution_agent_tools())
    .with_skill(EVOLUTION_PLANNING_SKILL_ID)
    .with_skill(EVOLUTION_REVIEW_SKILL_ID)
    .with_package_version(env!("CARGO_PKG_VERSION"))
    .with_package_provenance(AgentPackageProvenance::module(
        contract::EVOLUTION_MODULE_ID,
        env!("CARGO_PKG_VERSION"),
    ));
    definition.tool_surface.visible_toolsets = EVOLUTION_DEFAULT_TOOLSETS
        .iter()
        .map(|toolset| (*toolset).to_string())
        .collect();
    definition
}

pub(crate) fn evolution_agent_export() -> NativeModuleAgentDefinition {
    let mut export = NativeModuleAgentDefinition::new(
        EVOLUTION_AGENT_ID,
        EVOLUTION_AGENT_ID,
        "evolution",
        EVOLUTION_AGENT_PROMPT,
    )
    .with_description("supervised self-evolution mutation agent")
    .with_skill(EVOLUTION_PLANNING_SKILL_ID)
    .with_skill(EVOLUTION_REVIEW_SKILL_ID);
    for tool in evolution_agent_tools() {
        export = export.with_tool(NativeModuleAgentTool::new(
            agent_tool_kind_identifier(&tool.kind),
            tool.name,
            tool.description,
            tool.async_default,
        ));
    }
    for toolset in EVOLUTION_DEFAULT_TOOLSETS {
        export = export.with_visible_toolset(*toolset);
    }
    export
}

pub(crate) fn evolution_agent_resources() -> Vec<NativeModuleDeclarativeResource> {
    vec![
        skill_resource(
            EVOLUTION_PLANNING_SKILL_ID,
            EVOLUTION_PLANNING_SKILL_PATH,
            "Plan bounded evolution mutations from evidence and explicit acceptance gates.",
            &["evolution", "planning"],
            EVOLUTION_PLANNING_SKILL,
        ),
        skill_resource(
            EVOLUTION_REVIEW_SKILL_ID,
            EVOLUTION_REVIEW_SKILL_PATH,
            "Review evolution candidates for scope, evidence, and safe rollout readiness.",
            &["evolution", "review"],
            EVOLUTION_REVIEW_SKILL,
        ),
    ]
}

fn evolution_agent_tools() -> Vec<AgentToolSpec> {
    default_coding_agent_tools()
}

fn agent_tool_kind_identifier(kind: &AgentToolKind) -> String {
    serde_json::to_value(kind)
        .expect("agent tool kind serializes")
        .as_str()
        .expect("agent tool kind serializes as an identifier")
        .to_string()
}

fn skill_resource(
    id: &str,
    path: &str,
    description: &str,
    activation_tags: &[&str],
    embedded_text: &str,
) -> NativeModuleDeclarativeResource {
    NativeModuleDeclarativeResource {
        id: id.to_string(),
        kind: "skill".to_string(),
        path: path.to_string(),
        description: description.to_string(),
        target_agents: Vec::new(),
        model_families: Vec::new(),
        activation_tags: activation_tags
            .iter()
            .map(|tag| (*tag).to_string())
            .collect(),
        required_capabilities: Vec::new(),
        embedded_text: Some(embedded_text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolution_agent_package_owns_tools_skills_and_module_provenance() {
        let definition = evolution_agent_definition();
        assert_eq!(definition.key(), EVOLUTION_AGENT_ID);
        assert_eq!(definition.role, AgentRole::Evolution);
        assert_eq!(
            definition.package.provenance.module_id.as_deref(),
            Some(contract::EVOLUTION_MODULE_ID)
        );
        assert!(!definition.tools.is_empty());
        assert_eq!(
            definition.skills,
            [EVOLUTION_PLANNING_SKILL_ID, EVOLUTION_REVIEW_SKILL_ID]
        );
        assert!(
            definition
                .tool_surface
                .visible_toolsets
                .contains(&contract::EVOLUTION_MODULE_OWNER.to_string())
        );

        let resources = evolution_agent_resources();
        assert_eq!(resources.len(), definition.skills.len());
        assert!(resources.iter().all(|resource| {
            resource.kind == "skill"
                && resource.target_agents.is_empty()
                && resource.embedded_text.as_deref().is_some_and(|text| {
                    sinomo_agent::parse_skill_markdown(text)
                        .is_ok_and(|document| document.name() == resource.id)
                })
        }));
    }
}
