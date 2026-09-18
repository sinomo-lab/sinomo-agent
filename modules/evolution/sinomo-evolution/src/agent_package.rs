use sinomo_agent::{
    AgentDefinition, AgentPackageProvenance, AgentRole, AgentToolKind, AgentToolSpec,
    default_coding_agent_tools,
};
use sinomo_module_api::{
    NativeModuleAgentDefinition, NativeModuleAgentTool, NativeModuleDeclarativeResource,
};

use sinomo_runtime::api::app::evolution as contract;

pub const EVOLUTION_AGENT_ID: &str = "evolution-agent";
pub const EVOLUTION_PROPOSER_AGENT_ID: &str = "evolution-proposer";
pub const EVOLUTION_IMPLEMENTER_AGENT_ID: &str = "evolution-implementer";
pub const EVOLUTION_EVALUATOR_AGENT_ID: &str = "evolution-evaluator";
pub(crate) const EVOLUTION_PLANNING_SKILL_ID: &str = "evolution-planning";
pub(crate) const EVOLUTION_REVIEW_SKILL_ID: &str = "evolution-review";
pub(crate) const EVOLUTION_ORCHESTRATION_SKILL_ID: &str = "evolution-orchestration";

const EVOLUTION_AGENT_PROMPT: &str = r#"You are Sinomo's evolution governor.

You own one or more goal-directed evolution loops. Translate each user goal
into explicit metrics and guardrails, maintain its revision DAG, create bounded
subgoals and tasks, assign proposal, implementation, and evaluation work to
subagents, and choose the next experiment from immutable evidence. The runtime
owns tool execution, sandbox enforcement, event emission, evidence, and session
state. Candidate agents may propose or implement revisions, but they never
self-approve, select, promote, or roll out their own work.

Keep competing branches separate until their artifacts have been evaluated on
the same metric contract. Track tokens, compute, wall time, and monetary cost
with evidence and confidence. Ask for a user decision at configured checkpoints
or when goal interpretation would materially change."#;

const EVOLUTION_IMPLEMENTER_PROMPT: &str = r#"You are an evolution implementation subagent.

Implement only the assigned task in its isolated candidate workspace. Preserve
the parent revision identity and mutation scope, use the supplied tools, record
all evaluatable artifacts, and return verification evidence. You cannot approve,
select, promote, or roll out the revision."#;

const EVOLUTION_PROPOSER_PROMPT: &str = r#"You are an evolution proposal subagent.

Use the assigned goal, parent revisions, metric contract, evidence, and budget
to propose one bounded, testable improvement. State its expected metric effect,
required artifacts, risks, and implementation task. Do not edit target content,
self-approve, select, promote, or roll out a revision."#;

const EVOLUTION_EVALUATOR_PROMPT: &str = r#"You are an evolution evaluation subagent.

Evaluate candidate artifacts against the goal's declared metrics and absolute
guardrails. Keep observations attributable to exact artifact and revision
identities, report sample counts and uncertainty, and expose regressions. You
cannot modify the candidate or select it for promotion."#;

const EVOLUTION_PLANNING_SKILL_PATH: &str = "skills/evolution-planning/SKILL.md";
const EVOLUTION_REVIEW_SKILL_PATH: &str = "skills/evolution-review/SKILL.md";
const EVOLUTION_ORCHESTRATION_SKILL_PATH: &str = "skills/evolution-orchestration/SKILL.md";
const EVOLUTION_PLANNING_SKILL: &str = include_str!("../skills/evolution-planning/SKILL.md");
const EVOLUTION_REVIEW_SKILL: &str = include_str!("../skills/evolution-review/SKILL.md");
const EVOLUTION_ORCHESTRATION_SKILL: &str =
    include_str!("../skills/evolution-orchestration/SKILL.md");
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
    .with_tools(evolution_governor_tools())
    .with_skill(EVOLUTION_PLANNING_SKILL_ID)
    .with_skill(EVOLUTION_REVIEW_SKILL_ID)
    .with_skill(EVOLUTION_ORCHESTRATION_SKILL_ID)
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

pub fn evolution_implementer_agent_definition() -> AgentDefinition {
    AgentDefinition::custom(
        EVOLUTION_IMPLEMENTER_AGENT_ID,
        AgentRole::Deliberation,
        EVOLUTION_IMPLEMENTER_PROMPT,
    )
    .with_tools(evolution_agent_tools())
    .with_skill(EVOLUTION_PLANNING_SKILL_ID)
    .with_package_version(env!("CARGO_PKG_VERSION"))
    .with_package_provenance(AgentPackageProvenance::module(
        contract::EVOLUTION_MODULE_ID,
        env!("CARGO_PKG_VERSION"),
    ))
}

pub fn evolution_proposer_agent_definition() -> AgentDefinition {
    AgentDefinition::custom(
        EVOLUTION_PROPOSER_AGENT_ID,
        AgentRole::Deliberation,
        EVOLUTION_PROPOSER_PROMPT,
    )
    .with_tools(evolution_agent_tools())
    .with_skill(EVOLUTION_PLANNING_SKILL_ID)
    .with_package_version(env!("CARGO_PKG_VERSION"))
    .with_package_provenance(AgentPackageProvenance::module(
        contract::EVOLUTION_MODULE_ID,
        env!("CARGO_PKG_VERSION"),
    ))
}

pub fn evolution_evaluator_agent_definition() -> AgentDefinition {
    AgentDefinition::custom(
        EVOLUTION_EVALUATOR_AGENT_ID,
        AgentRole::Deliberation,
        EVOLUTION_EVALUATOR_PROMPT,
    )
    .with_tools(evolution_agent_tools())
    .with_skill(EVOLUTION_REVIEW_SKILL_ID)
    .with_package_version(env!("CARGO_PKG_VERSION"))
    .with_package_provenance(AgentPackageProvenance::module(
        contract::EVOLUTION_MODULE_ID,
        env!("CARGO_PKG_VERSION"),
    ))
}

pub(crate) fn evolution_agent_exports() -> Vec<NativeModuleAgentDefinition> {
    vec![
        agent_export(
            EVOLUTION_AGENT_ID,
            "evolution",
            EVOLUTION_AGENT_PROMPT,
            "goal-directed evolution governor and subagent supervisor",
            &[
                EVOLUTION_ORCHESTRATION_SKILL_ID,
                EVOLUTION_PLANNING_SKILL_ID,
                EVOLUTION_REVIEW_SKILL_ID,
            ],
            evolution_governor_tools(),
        ),
        agent_export(
            EVOLUTION_PROPOSER_AGENT_ID,
            "deliberation",
            EVOLUTION_PROPOSER_PROMPT,
            "bounded evidence-driven evolution proposal subagent",
            &[EVOLUTION_PLANNING_SKILL_ID],
            evolution_agent_tools(),
        ),
        agent_export(
            EVOLUTION_IMPLEMENTER_AGENT_ID,
            "deliberation",
            EVOLUTION_IMPLEMENTER_PROMPT,
            "isolated evolution implementation subagent",
            &[EVOLUTION_PLANNING_SKILL_ID],
            evolution_agent_tools(),
        ),
        agent_export(
            EVOLUTION_EVALUATOR_AGENT_ID,
            "deliberation",
            EVOLUTION_EVALUATOR_PROMPT,
            "artifact-bound evolution evaluation subagent",
            &[EVOLUTION_REVIEW_SKILL_ID],
            evolution_agent_tools(),
        ),
    ]
}

fn agent_export(
    id: &str,
    role: &str,
    prompt: &str,
    description: &str,
    skills: &[&str],
    tools: Vec<AgentToolSpec>,
) -> NativeModuleAgentDefinition {
    let mut export =
        NativeModuleAgentDefinition::new(id, id, role, prompt).with_description(description);
    for skill in skills {
        export = export.with_skill(*skill);
    }
    for tool in tools {
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
            EVOLUTION_ORCHESTRATION_SKILL_ID,
            EVOLUTION_ORCHESTRATION_SKILL_PATH,
            "Govern a metric-driven evolution DAG and assign bounded work to subagents.",
            &["evolution", "orchestration", "goal"],
            EVOLUTION_ORCHESTRATION_SKILL,
        ),
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

fn evolution_governor_tools() -> Vec<AgentToolSpec> {
    let mut tools = default_coding_agent_tools();
    tools.push(AgentToolSpec::new(
        AgentToolKind::ModuleObjectRpc,
        "module_object_rpc",
        "Call typed evolution goal, revision, dashboard, and workflow-control operations through the authenticated module service.",
        true,
    ));
    tools
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
            [
                EVOLUTION_PLANNING_SKILL_ID,
                EVOLUTION_REVIEW_SKILL_ID,
                EVOLUTION_ORCHESTRATION_SKILL_ID
            ]
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

        let exports = evolution_agent_exports();
        assert_eq!(exports.len(), 4);
        assert_eq!(exports[0].id, EVOLUTION_AGENT_ID);
        assert_eq!(exports[1].id, EVOLUTION_PROPOSER_AGENT_ID);
        assert_eq!(exports[2].id, EVOLUTION_IMPLEMENTER_AGENT_ID);
        assert_eq!(exports[3].id, EVOLUTION_EVALUATOR_AGENT_ID);
    }
}
