#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
//! Replays scripted agent sessions from `tests/scenarios/*.json` against the
//! real local workspace tools in a temporary workspace.
//!
//! A scenario seeds workspace files, sends user turns, and answers the
//! agent's model requests from its script. Each run is reduced to an outline
//! (items, tool statuses, and event kinds per turn) and compared with the
//! scenario's `.outline.txt`. Set `SINOMO_UPDATE_SCENARIOS=1` to rewrite the
//! outlines after an intended behavior change, and review the diff.
//!
//! Set `SINOMO_RERECORD_SCENARIOS=<provider>/<model>` to run the scenarios
//! against that configured provider (credentials from `SINOMO_DATA_DIR` or
//! `~/.sinomo`) instead of their scripts. Each run writes
//! `<name>.recorded.json`: the scenario with the recorded, redacted script.
//! Review it, copy over what is worth keeping, and tighten its expectations
//! by hand; recorded files are not replayed.

use serde::{Deserialize, Serialize};
use sinomo_agent::*;
use sinomo_lm::{
    LanguageModelCancellation, LanguageModelError, LanguageModelHostAccess,
    LanguageModelInvocation, LanguageModelProvider, LanguageModelProviderCapabilities,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelResponse,
    LanguageModelStreamEvent, ModelScript, RecordingProvider, ScriptRecorder, ScriptRedactor,
    ScriptedProvider,
};
use sinomo_runtime::{events::SessionId, local::SinomoDataDir, node::NodeId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const RERECORD_ENV: &str = "SINOMO_RERECORD_SCENARIOS";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    description: String,
    #[serde(default)]
    workspace: ScenarioWorkspace,
    turns: Vec<String>,
    script: ModelScript,
    #[serde(default)]
    expect: ScenarioExpectations,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScenarioWorkspace {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    files: BTreeMap<String, String>,
    #[serde(default)]
    allow_writes: bool,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScenarioExpectations {
    /// Text the last turn's answer contains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    answer_contains: Option<String>,
    /// Text each workspace file contains after the session.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    files: BTreeMap<String, String>,
    /// Workspace files that must not exist after the session.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    absent_files: Vec<String>,
}

fn scenarios_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("scenarios")
}

/// The model a scenario runs against: its script, or a configured provider
/// being recorded.
enum ScenarioModel {
    Scripted(ScriptedProvider),
    Recording(RecordingProvider<ConfiguredModel>),
}

impl ScenarioModel {
    fn provider(&self) -> &dyn LanguageModelProvider {
        match self {
            Self::Scripted(provider) => provider,
            Self::Recording(provider) => provider,
        }
    }
}

impl LanguageModelProvider for ScenarioModel {
    fn info(&self) -> LanguageModelProviderInfo {
        self.provider().info()
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.provider().complete(request)
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.provider().complete_streaming_events(request, on_event)
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.provider()
            .complete_streaming_events_with_cancellation(request, cancellation, on_event)
    }
}

/// A provider from the operator's model configuration, for re-recording.
struct ConfiguredModel {
    data_dir: SinomoDataDir,
    provider: String,
    model: String,
}

impl LanguageModelProvider for ConfiguredModel {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.provider.clone(),
            default_model: self.model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events(self, request, &mut |_| {})
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events_with_cancellation(
            self,
            request,
            &LanguageModelCancellation::default(),
            on_event,
        )
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        sinomo_lm::complete_configured_model_request_streaming_events_with_account_and_cancellation(
            &self.data_dir,
            &self.provider,
            &self.model,
            None,
            None,
            request,
            cancellation,
            on_event,
        )
    }
}

fn run_scenario(name: &str) {
    let path = scenarios_dir().join(format!("{name}.json"));
    let json = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let mut scenario: Scenario = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));

    let root = sinomo_testdir::TestDir::new(&format!("agent-scenario-{name}"));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    for (file, content) in &scenario.workspace.files {
        let file = workspace.join(file);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, content).unwrap();
    }

    let rerecord = std::env::var(RERECORD_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let (model, provider, model_name) = match &rerecord {
        None => {
            let script = std::mem::take(&mut scenario.script);
            let scripted = ScriptedProvider::new(script);
            (
                ScenarioModel::Scripted(scripted),
                "scripted".to_string(),
                "scripted".to_string(),
            )
        }
        Some(selection) => {
            let (provider, model_name) = selection.split_once('/').unwrap_or_else(|| {
                panic!("{RERECORD_ENV} must be <provider>/<model>, not {selection:?}")
            });
            let recorder = ScriptRecorder::new(
                root.join("recorded-script.json"),
                ScriptRedactor::for_current_process().with_path(&workspace, "<workspace>"),
            );
            let configured = ConfiguredModel {
                data_dir: SinomoDataDir::from_env_or_home().expect("operator data dir"),
                provider: provider.to_string(),
                model: model_name.to_string(),
            };
            (
                ScenarioModel::Recording(RecordingProvider::new(configured, recorder)),
                provider.to_string(),
                model_name.to_string(),
            )
        }
    };
    let scripted = match &model {
        ScenarioModel::Scripted(scripted) => Some(scripted.clone()),
        ScenarioModel::Recording(_) => None,
    };
    let recorder = match &model {
        ScenarioModel::Scripted(_) => None,
        ScenarioModel::Recording(recording) => Some(recording.recorder().clone()),
    };
    let tools = AgentToolRegistry::local_workspace_with_config(
        local_workspace::LocalWorkspaceAgentConfig::new(&workspace)
            .allowing_file_writes(scenario.workspace.allow_writes),
    );
    let data_dir = SinomoDataDir::new(root.join("data"));
    let agent = Agent::new(model, provider.clone(), model_name.clone())
        .with_config(AgentConfig::new(provider, model_name).with_max_steps(12))
        .with_tool_registry(tools)
        .with_session_store(FileAgentSessionStore::for_data_dir(&data_dir));
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let interaction = InteractionSession::new(
        session.session_id.clone(),
        NodeId::generate(),
        InteractionSurface::new("scenario", InteractionSurfaceKind::CommandLine),
    );

    let mut outline = Vec::new();
    let mut answer = String::new();
    for (index, turn) in scenario.turns.iter().enumerate() {
        let input = AgentLoopInput {
            session: interaction.clone(),
            latest_event: user_interaction_input_event(&interaction, turn),
            transient_system_context: None,
            async_update: None,
            context_attachments: Vec::new(),
            tool_specs: Vec::new(),
        };
        let run = agent
            .start_turn(&mut session, &input)
            .unwrap_or_else(|error| {
                let violations = scripted
                    .as_ref()
                    .map(ScriptedProvider::violations)
                    .unwrap_or_default();
                panic!("turn {} failed: {error}\nscript: {violations:?}", index + 1)
            });
        outline.push(format!("turn {}: {}", index + 1, one_line(turn)));
        outline.extend(outline_run(&run, &workspace));
        answer = run.final_decision.text.clone();
    }
    if let Some(scripted) = &scripted {
        scripted.assert_finished();
    }
    if let Some(recorder) = &recorder {
        scenario.script = ModelScript::from_json(&recorder.script_json()).unwrap();
        let recorded = scenarios_dir().join(format!("{name}.recorded.json"));
        std::fs::write(
            &recorded,
            serde_json::to_string_pretty(&scenario).unwrap() + "\n",
        )
        .unwrap();
        eprintln!("recorded {}", recorded.display());
    }

    if let Some(expected) = &scenario.expect.answer_contains {
        assert!(
            answer.contains(expected.as_str()),
            "answer {answer:?} does not contain {expected:?}"
        );
    }
    for (file, expected) in &scenario.expect.files {
        let content = std::fs::read_to_string(workspace.join(file))
            .unwrap_or_else(|error| panic!("read workspace file {file}: {error}"));
        assert!(
            content.contains(expected.as_str()),
            "workspace file {file} does not contain {expected:?}:\n{content}"
        );
    }
    for file in &scenario.expect.absent_files {
        assert!(
            !workspace.join(file).exists(),
            "workspace file {file} exists"
        );
    }
    if rerecord.is_none() {
        check_outline(name, &outline.join("\n"));
    }
}

/// The parts of a run a scenario pins: what the agent recorded and which
/// events it published, without ids, times, or temporary paths.
fn outline_run(run: &AgentRun, workspace: &Path) -> Vec<String> {
    let mut lines = Vec::new();
    for item in &run.items {
        let line = match item {
            // The rendered turn prompt; the turn line shows what the user said.
            AgentItem::UserMessage { .. } => "user".to_string(),
            AgentItem::AssistantText { content, .. } => {
                format!("assistant: {}", one_line(content))
            }
            AgentItem::AssistantReasoningSummary { .. } => "reasoning".to_string(),
            AgentItem::ToolCall { call, .. } => format!(
                "tool_call {} {}",
                call.tool,
                one_line(&call.input.to_string())
            ),
            AgentItem::ToolResult {
                tool_name, status, ..
            } => format!(
                "tool_result {} {}",
                tool_name.as_deref().unwrap_or("?"),
                enum_name(status)
            ),
            AgentItem::AsyncTaskUpdate { update, .. } => {
                format!("async_update {}", enum_name(&update.status))
            }
            AgentItem::ContextInjection { .. } => "context".to_string(),
            AgentItem::CompactionSummary { .. } => "compaction".to_string(),
            AgentItem::ModelUsage { .. } => "usage".to_string(),
            AgentItem::Error { detail, .. } => format!("error: {}", one_line(detail)),
            AgentItem::PatchSnapshot { changed_files, .. } => {
                format!("patch {}", changed_files.join(", "))
            }
            AgentItem::VerificationResult { command, .. } => format!("verification {command}"),
            AgentItem::CodingRunState { .. } => "coding_state".to_string(),
        };
        lines.push(format!("  {}", normalize_paths(&line, workspace)));
    }
    lines.push(format!("  stop: {}", enum_name(&run.stop_reason)));
    let mut kinds = Vec::<String>::new();
    for event in &run.events {
        let kind = match serde_json::to_value(&event.kind) {
            Ok(serde_json::Value::String(kind)) => kind,
            Ok(other) => other.to_string(),
            Err(error) => format!("<{error}>"),
        };
        if kinds.last() != Some(&kind) {
            kinds.push(kind);
        }
    }
    lines.push(format!("  events: {}", kinds.join(", ")));
    lines
}

fn check_outline(name: &str, actual: &str) {
    let path = scenarios_dir().join(format!("{name}.outline.txt"));
    let actual = format!("{actual}\n");
    if std::env::var_os("SINOMO_UPDATE_SCENARIOS").is_some() {
        std::fs::write(&path, &actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_default();
    if expected.replace("\r\n", "\n") != actual {
        panic!(
            "scenario `{name}` outline changed; rerun with SINOMO_UPDATE_SCENARIOS=1 if \
             intended.\n--- expected ({})\n{expected}\n--- actual\n{actual}",
            path.display()
        );
    }
}

fn enum_name(value: &impl serde::Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(name)) => name,
        Ok(other) => other.to_string(),
        Err(error) => format!("<{error}>"),
    }
}

fn one_line(text: &str) -> String {
    const LIMIT: usize = 120;
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match text.char_indices().nth(LIMIT) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text,
    }
}

fn normalize_paths(line: &str, workspace: &Path) -> String {
    let native = workspace.display().to_string();
    let escaped = native.replace('\\', "\\\\");
    let forward = native.replace('\\', "/");
    line.replace(&escaped, "<workspace>")
        .replace(&native, "<workspace>")
        .replace(&forward, "<workspace>")
}

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_scenario(stringify!($name));
            }
        )*

        #[test]
        fn every_scenario_file_has_a_test() {
            let listed = [$(stringify!($name)),*];
            for entry in std::fs::read_dir(scenarios_dir()).unwrap() {
                let file_name = entry.unwrap().file_name().to_string_lossy().into_owned();
                let replayed = file_name
                    .strip_suffix(".json")
                    .filter(|name| !name.ends_with(".recorded"));
                if let Some(name) = replayed {
                    assert!(
                        listed.contains(&name),
                        "add `{name}` to the scenarios! list in tests/scenarios.rs"
                    );
                }
            }
        }
    };
}

scenarios!(
    read_and_answer,
    edit_a_file,
    recover_from_a_failed_tool_call,
    repair_malformed_output,
);
