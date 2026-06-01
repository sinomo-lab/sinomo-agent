use crate::{CandidateWorkspace, EvolutionJobId, parse_git_status_paths, shell_command};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    Confidence, EvaluationGate, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingHint,
    RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_runtime::eval::{
    CommandControlAction, CommandControlRegistry, CommandRunResult, CommandRunner,
    LocalCommandRunner, NoopCommandRunObserver, VerificationCommand,
};
use sinomo_runtime::events::EventId;
use sinomo_runtime::node::NodeId;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionToolDescriptor {
    pub name: String,
    pub input_schema_json: serde_json::Value,
    pub output_schema_json: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effects: Vec<ToolEffect>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ToolEffect {
    ReadFile,
    WriteFile,
    RunCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionToolCall {
    pub tool: String,
    #[serde(default)]
    pub input: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionToolResult {
    pub tool: String,
    pub status: ToolCallStatus,
    pub output: serde_json::Value,
    pub evidence: ToolEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ToolCallStatus {
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolEvidence {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionToolEventContext {
    pub node_id: NodeId,
    pub job_id: EvolutionJobId,
}

impl EvolutionToolEventContext {
    pub fn new(node_id: NodeId, job_id: EvolutionJobId) -> Self {
        Self { node_id, job_id }
    }
}

pub trait EvolutionTool: Send + Sync {
    fn descriptor(&self) -> EvolutionToolDescriptor;
    fn execute(
        &self,
        workspace: &CandidateWorkspace,
        input: serde_json::Value,
    ) -> EvolutionToolResult;
}

pub struct EvolutionToolRegistry {
    tools: BTreeMap<String, Box<dyn EvolutionTool>>,
    command_control: Arc<CommandControlRegistry>,
}

impl Default for EvolutionToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EvolutionToolRegistry {
    pub fn new() -> Self {
        Self::with_command_control(CommandControlRegistry::new_shared())
    }

    pub fn with_command_control(command_control: Arc<CommandControlRegistry>) -> Self {
        Self {
            tools: BTreeMap::new(),
            command_control,
        }
    }

    pub fn bootstrap() -> Self {
        let mut registry = Self::new();
        let command_runner = LocalCommandRunner::with_observer(
            sinomo_runtime::eval::CommandInactivityPolicy::default(),
            NoopCommandRunObserver,
            registry.command_control.clone(),
        );
        registry.register(ReadFileTool);
        registry.register(SearchTool);
        registry.register(ApplyPatchTool);
        registry.register(RunCommandTool {
            runner: command_runner,
        });
        registry.register(CommandControlTool {
            name: "interrupt_command",
            action: CommandControlAction::Interrupt,
            control: registry.command_control.clone(),
        });
        registry.register(CommandControlTool {
            name: "kill_command",
            action: CommandControlAction::Kill,
            control: registry.command_control.clone(),
        });
        registry
    }

    pub fn active_command_run_ids(&self) -> Vec<String> {
        self.command_control.active_run_ids()
    }

    pub fn register<T>(&mut self, tool: T)
    where
        T: EvolutionTool + 'static,
    {
        let name = tool.descriptor().name;
        self.tools.insert(name, Box::new(tool));
    }

    pub fn descriptors(&self) -> Vec<EvolutionToolDescriptor> {
        self.tools.values().map(|tool| tool.descriptor()).collect()
    }

    pub fn execute(
        &self,
        workspace: &CandidateWorkspace,
        call: EvolutionToolCall,
    ) -> EvolutionToolResult {
        match self.tools.get(&call.tool) {
            Some(tool) => tool.execute(workspace, call.input),
            None => {
                let tool_name = call.tool;
                failed_tool_result(
                    tool_name.clone(),
                    format!("unknown evolution tool `{tool_name}`"),
                    Vec::new(),
                )
            }
        }
    }

    pub fn execute_with_event(
        &self,
        workspace: &CandidateWorkspace,
        context: &EvolutionToolEventContext,
        call: EvolutionToolCall,
    ) -> (EvolutionToolResult, EventEnvelope) {
        let tool_name = call.tool.clone();
        let input = call.input.clone();
        let result = self.execute(workspace, call);
        let changed_files = changed_files_from_git_status(&workspace.path);
        let event = tool_call_event(context, workspace, tool_name, input, &result, changed_files);
        (result, event)
    }
}

#[derive(Clone, Debug)]
struct ReadFileTool;

impl EvolutionTool for ReadFileTool {
    fn descriptor(&self) -> EvolutionToolDescriptor {
        descriptor(
            "read_file",
            object_schema(&[("path", "string")]),
            object_schema(&[("path", "string"), ("content", "string")]),
            vec![ToolEffect::ReadFile],
        )
    }

    fn execute(
        &self,
        workspace: &CandidateWorkspace,
        input: serde_json::Value,
    ) -> EvolutionToolResult {
        let Some(path) = string_field(&input, "path") else {
            return failed_tool_result("read_file", "missing string field `path`", Vec::new());
        };
        let file_path = workspace.path.join(path);
        match fs::read_to_string(&file_path) {
            Ok(content) => succeeded_tool_result(
                "read_file",
                serde_json::json!({ "path": path, "content": content }),
                format!("read {}", file_path.display()),
                vec![path.to_string()],
            ),
            Err(error) => failed_tool_result(
                "read_file",
                format!("failed to read {}: {error}", file_path.display()),
                vec![path.to_string()],
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct SearchTool;

impl EvolutionTool for SearchTool {
    fn descriptor(&self) -> EvolutionToolDescriptor {
        descriptor(
            "search",
            object_schema(&[("query", "string")]),
            object_schema(&[
                ("query", "string"),
                ("stdout", "string"),
                ("stderr", "string"),
            ]),
            vec![ToolEffect::ReadFile, ToolEffect::RunCommand],
        )
    }

    fn execute(
        &self,
        workspace: &CandidateWorkspace,
        input: serde_json::Value,
    ) -> EvolutionToolResult {
        let Some(query) = string_field(&input, "query") else {
            return failed_tool_result("search", "missing string field `query`", Vec::new());
        };
        command_tool_result(
            "search",
            format!("rg -- {}", shell_quote(query)),
            &workspace.path,
        )
    }
}

#[derive(Clone, Debug)]
struct ApplyPatchTool;

impl EvolutionTool for ApplyPatchTool {
    fn descriptor(&self) -> EvolutionToolDescriptor {
        descriptor(
            "apply_patch",
            object_schema(&[("patch", "string")]),
            object_schema(&[("stdout", "string"), ("stderr", "string")]),
            vec![ToolEffect::WriteFile, ToolEffect::RunCommand],
        )
    }

    fn execute(
        &self,
        workspace: &CandidateWorkspace,
        input: serde_json::Value,
    ) -> EvolutionToolResult {
        let Some(patch) = string_field(&input, "patch") else {
            return failed_tool_result("apply_patch", "missing string field `patch`", Vec::new());
        };
        let start = Instant::now();
        if patch.trim_start().starts_with("*** Begin Patch") {
            return apply_v4a_patch(workspace, patch, start);
        }
        let mut child = match shell_command("git apply --whitespace=nowarn -")
            .current_dir(&workspace.path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                return failed_command_result(
                    "apply_patch",
                    "git apply --whitespace=nowarn -",
                    -1,
                    duration_millis(start.elapsed()),
                    String::new(),
                    error.to_string(),
                );
            }
        };
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            if let Err(error) = stdin.write_all(patch.as_bytes()) {
                return failed_command_result(
                    "apply_patch",
                    "git apply --whitespace=nowarn -",
                    -1,
                    duration_millis(start.elapsed()),
                    String::new(),
                    error.to_string(),
                );
            }
        }
        let output = child.wait_with_output();
        command_output_result(
            "apply_patch",
            "git apply --whitespace=nowarn -",
            output,
            start.elapsed(),
        )
    }
}

fn apply_v4a_patch(
    workspace: &CandidateWorkspace,
    patch: &str,
    start: Instant,
) -> EvolutionToolResult {
    match apply_v4a_patch_to_workspace(&workspace.path, patch) {
        Ok(paths) => EvolutionToolResult {
            tool: "apply_patch".to_string(),
            status: ToolCallStatus::Succeeded,
            output: serde_json::json!({
                "stdout": "",
                "stderr": "",
                "exit_code": 0,
                "format": "v4a"
            }),
            evidence: ToolEvidence {
                summary: "applied V4A patch".to_string(),
                command: Some("apply V4A patch".to_string()),
                exit_code: Some(0),
                duration_ms: Some(duration_millis(start.elapsed())),
                paths,
            },
        },
        Err(error) => failed_command_result(
            "apply_patch",
            "apply V4A patch",
            -1,
            duration_millis(start.elapsed()),
            String::new(),
            error,
        ),
    }
}

fn apply_v4a_patch_to_workspace(root: &Path, patch: &str) -> Result<Vec<String>, String> {
    let mut changed_paths = Vec::new();
    let mut lines = patch.lines().peekable();
    while let Some(line) = lines.next() {
        let Some(path) = line.strip_prefix("*** Update File: ") else {
            continue;
        };
        let path = path.trim();
        if path.is_empty() || path.starts_with('/') || path.split('/').any(|part| part == "..") {
            return Err(format!("unsafe V4A patch path `{path}`"));
        }
        let mut old_text = String::new();
        let mut new_text = String::new();
        let mut in_hunk = false;
        while let Some(next_line) = lines.peek().copied() {
            if next_line.starts_with("*** Update File: ")
                || next_line.starts_with("*** Add File: ")
                || next_line.starts_with("*** Delete File: ")
                || next_line.starts_with("*** End Patch")
            {
                break;
            }
            let hunk_line = lines.next().unwrap_or_default();
            if hunk_line.starts_with("@@") {
                in_hunk = true;
                continue;
            }
            if !in_hunk {
                continue;
            }
            if let Some(rest) = hunk_line.strip_prefix('+') {
                new_text.push_str(rest);
                new_text.push('\n');
            } else if let Some(rest) = hunk_line.strip_prefix('-') {
                old_text.push_str(rest);
                old_text.push('\n');
            } else if let Some(rest) = hunk_line.strip_prefix(' ') {
                old_text.push_str(rest);
                old_text.push('\n');
                new_text.push_str(rest);
                new_text.push('\n');
            } else if hunk_line.is_empty() {
                old_text.push('\n');
                new_text.push('\n');
            }
        }
        if old_text.is_empty() {
            return Err(format!(
                "V4A patch for `{path}` did not contain removable context"
            ));
        }
        let file_path = root.join(path);
        let content = fs::read_to_string(&file_path)
            .map_err(|error| format!("failed to read {}: {error}", file_path.display()))?;
        let updated = if content.contains(&old_text) {
            content.replacen(&old_text, &new_text, 1)
        } else {
            let old_without_final_newline = old_text.trim_end_matches('\n');
            let new_without_final_newline = new_text.trim_end_matches('\n');
            if old_without_final_newline.is_empty() || !content.contains(old_without_final_newline)
            {
                return Err(format!("V4A patch context not found in `{path}`"));
            }
            content.replacen(old_without_final_newline, new_without_final_newline, 1)
        };
        fs::write(&file_path, updated)
            .map_err(|error| format!("failed to write {}: {error}", file_path.display()))?;
        changed_paths.push(path.to_string());
    }
    if changed_paths.is_empty() {
        Err("V4A patch did not contain any update file sections".to_string())
    } else {
        Ok(changed_paths)
    }
}

#[derive(Clone, Debug)]
struct RunCommandTool {
    runner: LocalCommandRunner,
}

impl EvolutionTool for RunCommandTool {
    fn descriptor(&self) -> EvolutionToolDescriptor {
        descriptor(
            "run_command",
            object_schema(&[("command", "string")]),
            object_schema(&[
                ("run_id", "string"),
                ("stdout", "string"),
                ("stderr", "string"),
                ("exit_code", "integer"),
            ]),
            vec![ToolEffect::RunCommand],
        )
    }

    fn execute(
        &self,
        workspace: &CandidateWorkspace,
        input: serde_json::Value,
    ) -> EvolutionToolResult {
        let Some(command) = string_field(&input, "command") else {
            return failed_tool_result("run_command", "missing string field `command`", Vec::new());
        };
        command_tool_result_with_runner(
            "run_command",
            command.to_string(),
            &workspace.path,
            &self.runner,
        )
    }
}

#[derive(Clone, Debug)]
struct CommandControlTool {
    name: &'static str,
    action: CommandControlAction,
    control: Arc<CommandControlRegistry>,
}

impl EvolutionTool for CommandControlTool {
    fn descriptor(&self) -> EvolutionToolDescriptor {
        descriptor(
            self.name,
            object_schema(&[("run_id", "string")]),
            object_schema(&[("run_id", "string"), ("action", "string")]),
            vec![ToolEffect::RunCommand],
        )
    }

    fn execute(
        &self,
        _workspace: &CandidateWorkspace,
        input: serde_json::Value,
    ) -> EvolutionToolResult {
        let Some(run_id) = string_field(&input, "run_id") else {
            return failed_tool_result(self.name, "missing string field `run_id`", Vec::new());
        };
        let result = match self.action {
            CommandControlAction::Interrupt => self.control.interrupt(run_id),
            CommandControlAction::Kill => self.control.kill(run_id),
        };
        match result {
            Ok(()) => succeeded_tool_result(
                self.name,
                serde_json::json!({
                    "run_id": run_id,
                    "action": command_control_action_name(self.action),
                }),
                format!(
                    "sent {} to `{run_id}`",
                    command_control_action_name(self.action)
                ),
                Vec::new(),
            ),
            Err(error) => failed_tool_result(
                self.name,
                format!(
                    "failed to {} `{run_id}`: {error}",
                    command_control_action_name(self.action)
                ),
                Vec::new(),
            ),
        }
    }
}

fn command_control_action_name(action: CommandControlAction) -> &'static str {
    match action {
        CommandControlAction::Interrupt => "interrupt",
        CommandControlAction::Kill => "kill",
    }
}

fn descriptor(
    name: &str,
    input_schema_json: serde_json::Value,
    output_schema_json: serde_json::Value,
    effects: Vec<ToolEffect>,
) -> EvolutionToolDescriptor {
    EvolutionToolDescriptor {
        name: name.to_string(),
        input_schema_json,
        output_schema_json,
        effects,
    }
}

fn object_schema(fields: &[(&str, &str)]) -> serde_json::Value {
    let properties = fields
        .iter()
        .map(|(name, kind)| ((*name).to_string(), serde_json::json!({ "type": kind })))
        .collect::<serde_json::Map<_, _>>();
    let required = fields
        .iter()
        .map(|(name, _)| serde_json::Value::String((*name).to_string()))
        .collect::<Vec<_>>();
    serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    })
}

fn string_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn succeeded_tool_result(
    tool: impl Into<String>,
    output: serde_json::Value,
    summary: impl Into<String>,
    paths: Vec<String>,
) -> EvolutionToolResult {
    EvolutionToolResult {
        tool: tool.into(),
        status: ToolCallStatus::Succeeded,
        output,
        evidence: ToolEvidence {
            summary: summary.into(),
            command: None,
            exit_code: None,
            duration_ms: None,
            paths,
        },
    }
}

fn failed_tool_result(
    tool: impl Into<String>,
    summary: impl Into<String>,
    paths: Vec<String>,
) -> EvolutionToolResult {
    EvolutionToolResult {
        tool: tool.into(),
        status: ToolCallStatus::Failed,
        output: serde_json::Value::Null,
        evidence: ToolEvidence {
            summary: summary.into(),
            command: None,
            exit_code: None,
            duration_ms: None,
            paths,
        },
    }
}

fn command_tool_result(tool: &str, command: String, cwd: &Path) -> EvolutionToolResult {
    let runner = LocalCommandRunner::new();
    command_tool_result_with_runner(tool, command, cwd, &runner)
}

fn command_tool_result_with_runner(
    tool: &str,
    command: String,
    cwd: &Path,
    runner: &LocalCommandRunner,
) -> EvolutionToolResult {
    let verification_command = VerificationCommand {
        name: tool.to_string(),
        command: command.clone(),
        expected_exit_code: 0,
        gate: EvaluationGate::StaticChecks,
    };
    match runner.run(&verification_command, cwd) {
        Ok(result) => command_run_result_to_tool_result(tool, result),
        Err(error) => {
            failed_command_result(tool, &command, -1, 0, String::new(), error.to_string())
        }
    }
}

fn command_run_result_to_tool_result(tool: &str, result: CommandRunResult) -> EvolutionToolResult {
    let status = if result.exit_code == 0 {
        ToolCallStatus::Succeeded
    } else {
        ToolCallStatus::Failed
    };
    EvolutionToolResult {
        tool: tool.to_string(),
        status,
        output: serde_json::json!({
            "run_id": result.run_id,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "exit_code": result.exit_code,
        }),
        evidence: ToolEvidence {
            summary: format!("`{}` exited {}", result.command, result.exit_code),
            command: Some(result.command),
            exit_code: Some(result.exit_code),
            duration_ms: Some(result.duration_ms),
            paths: Vec::new(),
        },
    }
}

fn command_output_result(
    tool: &str,
    command: &str,
    output: std::io::Result<std::process::Output>,
    duration: Duration,
) -> EvolutionToolResult {
    match output {
        Ok(output) => {
            let exit_code = output.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            let status = if output.status.success() {
                ToolCallStatus::Succeeded
            } else {
                ToolCallStatus::Failed
            };
            EvolutionToolResult {
                tool: tool.to_string(),
                status,
                output: serde_json::json!({
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code,
                }),
                evidence: ToolEvidence {
                    summary: format!("`{command}` exited {exit_code}"),
                    command: Some(command.to_string()),
                    exit_code: Some(exit_code),
                    duration_ms: Some(duration_millis(duration)),
                    paths: Vec::new(),
                },
            }
        }
        Err(error) => failed_command_result(
            tool,
            command,
            -1,
            duration_millis(duration),
            String::new(),
            error.to_string(),
        ),
    }
}

fn failed_command_result(
    tool: &str,
    command: &str,
    exit_code: i32,
    duration_ms: u64,
    stdout: String,
    stderr: String,
) -> EvolutionToolResult {
    EvolutionToolResult {
        tool: tool.to_string(),
        status: ToolCallStatus::Failed,
        output: serde_json::json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
        }),
        evidence: ToolEvidence {
            summary: format!("failed to run `{command}`"),
            command: Some(command.to_string()),
            exit_code: Some(exit_code),
            duration_ms: Some(duration_ms),
            paths: Vec::new(),
        },
    }
}

fn tool_call_event(
    context: &EvolutionToolEventContext,
    workspace: &CandidateWorkspace,
    tool: String,
    input: serde_json::Value,
    result: &EvolutionToolResult,
    changed_files: Vec<String>,
) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        context.node_id.clone(),
        Source::new("evolution.tool_registry").expect("valid source"),
        EventKind::new("evolution.tool.executed").expect("valid event kind"),
        Salience::new(0.72).expect("valid salience"),
        Confidence::new(0.95).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        )
        .with_hint(RoutingHint::new(format!("tool:{tool}")).expect("hint")),
    )
    .with_payload(serde_json::json!({
        "job_id": context.job_id.clone(),
        "tool": tool,
        "input": input,
        "result": result,
        "changed_files": changed_files,
        "candidate_workspace": {
            "job_id": workspace.job_id.clone(),
            "path": workspace.path.clone(),
            "branch_name": workspace.branch_name.clone(),
            "source_revision": workspace.source_revision.clone(),
        },
    }));

    for path in &result.evidence.paths {
        event.evidence.refs.push(sinomo_runtime::core::EvidenceRef {
            uri: format!("file://{}", workspace.path.join(path).display()),
            kind: sinomo_runtime::core::EvidenceKind::Other,
            content_hash: None,
        });
    }
    event
}

fn changed_files_from_git_status(cwd: &Path) -> Vec<String> {
    let Ok(output) = shell_command("git status --short")
        .current_dir(cwd)
        .output()
    else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    parse_git_status_paths(&String::from_utf8_lossy(&output.stdout)).unwrap_or_default()
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn shell_quote(value: &str) -> String {
    format!("{value:?}")
}
