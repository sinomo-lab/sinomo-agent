//! Autonomous self-evolution planning skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    CandidateBundleManifest, CommandLogRef, Confidence, EditScope, EvaluationGate,
    EvaluationOutcome, EvaluationScorecard, EventEnvelope, EventId, EventKind, EvolutionJob,
    EvolutionJobId, EvolutionJobStatus, EvolutionScope, FileExcerpt, LatencyClass, ModuleId,
    MutationOutputContract, MutationPacket, NodeId, PrivacyClass, RecoveryPlan, RollbackPolicy,
    RoutingHint, RoutingMetadata, RoutingScope, Salience, Source, TrustRing, VerificationCommand,
    VerificationPlan, WorkspaceId,
};
use sinomo_eval::{CommandRunResult, CommandRunner, ExecutingEvalHarness};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelProvider, LanguageModelRequest,
    LanguageModelResponseFormat, LanguageModelRole,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionOpportunity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_event_id: Option<EventId>,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_module_id: Option<ModuleId>,
    pub objective: String,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_paths: Vec<String>,
    pub priority: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateWorkspacePlan {
    pub job_id: EvolutionJobId,
    pub workspace_id: WorkspaceId,
    pub branch_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionRequest {
    pub objective: String,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intended_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<VerificationCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionReport {
    pub job: EvolutionJob,
    pub candidate_workspace: CandidateWorkspace,
    pub bundle: CandidateBundleManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    pub review_summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateWorkspace {
    pub job_id: EvolutionJobId,
    pub path: PathBuf,
    pub branch_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

pub trait WorkspaceAdapter {
    fn root(&self) -> &Path;
    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError>;
    fn changed_files(&self, workspace: &CandidateWorkspace) -> Result<Vec<String>, WorkspaceError>;
    fn diff(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError>;
    fn status(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError>;
}

#[derive(Clone, Debug)]
pub struct GitWorkspaceAdapter {
    source_root: PathBuf,
    worktrees_root: PathBuf,
}

impl GitWorkspaceAdapter {
    pub fn new(source_root: impl Into<PathBuf>, worktrees_root: impl Into<PathBuf>) -> Self {
        Self {
            source_root: source_root.into(),
            worktrees_root: worktrees_root.into(),
        }
    }

    fn run_git(&self, args: &[String], cwd: &Path) -> Result<String, WorkspaceError> {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .map_err(|source| WorkspaceError::Git {
                command: format!("git {}", args.join(" ")),
                detail: source.to_string(),
            })?;
        if !output.status.success() {
            return Err(WorkspaceError::Git {
                command: format!("git {}", args.join(" ")),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

impl WorkspaceAdapter for GitWorkspaceAdapter {
    fn root(&self) -> &Path {
        &self.source_root
    }

    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError> {
        fs::create_dir_all(&self.worktrees_root).map_err(|source| WorkspaceError::Io {
            path: self.worktrees_root.display().to_string(),
            detail: source.to_string(),
        })?;
        let source_revision = self.run_git(
            &["rev-parse".to_string(), "HEAD".to_string()],
            &self.source_root,
        )?;
        let branch_name = format!("codex/evolve/{}", job.job_id.as_str());
        let path = self
            .worktrees_root
            .join(job.job_id.as_str())
            .join("worktree");
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|source| WorkspaceError::Io {
                    path: parent.display().to_string(),
                    detail: source.to_string(),
                })?;
            }
            self.run_git(
                &[
                    "worktree".to_string(),
                    "add".to_string(),
                    "-B".to_string(),
                    branch_name.clone(),
                    path.display().to_string(),
                    source_revision.clone(),
                ],
                &self.source_root,
            )?;
        }

        Ok(CandidateWorkspace {
            job_id: job.job_id.clone(),
            path,
            branch_name,
            source_revision: Some(source_revision),
        })
    }

    fn changed_files(&self, workspace: &CandidateWorkspace) -> Result<Vec<String>, WorkspaceError> {
        parse_git_status_paths(&self.status(workspace)?).map_err(WorkspaceError::StatusParse)
    }

    fn diff(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        let tracked = self.run_git(
            &[
                "diff".to_string(),
                "--binary".to_string(),
                "HEAD".to_string(),
                "--".to_string(),
            ],
            &workspace.path,
        )?;
        let status = self.status(workspace)?;
        let mut diff = tracked;
        for path in parse_git_status_paths(&status).map_err(WorkspaceError::StatusParse)? {
            if is_untracked_status_path(&status, &path) {
                let file_path = workspace.path.join(&path);
                if file_path.is_file() {
                    let content =
                        fs::read_to_string(&file_path).map_err(|source| WorkspaceError::Io {
                            path: file_path.display().to_string(),
                            detail: source.to_string(),
                        })?;
                    if !diff.is_empty() {
                        diff.push_str("\n\n");
                    }
                    diff.push_str(&format!(
                        "diff --git a/{path} b/{path}\nnew file mode 100644\n--- /dev/null\n+++ b/{path}\n"
                    ));
                    for line in content.lines() {
                        diff.push('+');
                        diff.push_str(line);
                        diff.push('\n');
                    }
                }
            }
        }
        Ok(diff)
    }

    fn status(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        self.run_git(
            &["status".to_string(), "--porcelain".to_string()],
            &workspace.path,
        )
    }
}

pub fn parse_git_status_paths(status: &str) -> Result<Vec<String>, String> {
    let mut paths = Vec::new();
    for line in status.lines().filter(|line| !line.trim().is_empty()) {
        if line.len() < 4 {
            return Err(format!("invalid porcelain status line: {line}"));
        }
        let path = line[3..].trim();
        let path = path
            .rsplit_once(" -> ")
            .map(|(_, new_path)| new_path)
            .unwrap_or(path)
            .trim_matches('"')
            .to_string();
        if !path.is_empty() {
            paths.push(path);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn is_untracked_status_path(status: &str, path: &str) -> bool {
    status
        .lines()
        .any(|line| line.starts_with("?? ") && line.get(3..).map(str::trim) == Some(path))
}

pub trait CandidateBundleStore {
    fn create(&self, job: &EvolutionJob) -> Result<CandidateBundleManifest, BundleError>;
    fn write_json<T: Serialize>(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &T,
    ) -> Result<(), BundleError>;
    fn write_text(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &str,
    ) -> Result<(), BundleError>;
}

#[derive(Clone, Debug)]
pub struct LocalCandidateBundleStore {
    candidates_root: PathBuf,
}

impl LocalCandidateBundleStore {
    pub fn new(candidates_root: impl Into<PathBuf>) -> Self {
        Self {
            candidates_root: candidates_root.into(),
        }
    }

    pub fn path_for(&self, manifest: &CandidateBundleManifest, name: &str) -> PathBuf {
        PathBuf::from(&manifest.bundle_path).join(name)
    }

    pub fn write_manifest(&self, manifest: &CandidateBundleManifest) -> Result<(), BundleError> {
        self.write_json(manifest, "manifest.json", manifest)
    }

    pub fn write_command_log(
        &self,
        manifest: &CandidateBundleManifest,
        result: &CommandRunResult,
    ) -> Result<CommandLogRef, BundleError> {
        let safe_name = safe_file_stem(&result.name);
        let path = format!("command-logs/{safe_name}.txt");
        let text = format!(
            "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
            result.name,
            result.command,
            result.expected_exit_code,
            result.exit_code,
            result.duration_ms,
            result.stdout,
            result.stderr
        );
        self.write_text(manifest, &path, &text)?;
        Ok(CommandLogRef {
            name: result.name.clone(),
            command: result.command.clone(),
            log_path: self.path_for(manifest, &path).display().to_string(),
            exit_code: Some(result.exit_code),
        })
    }
}

impl CandidateBundleStore for LocalCandidateBundleStore {
    fn create(&self, job: &EvolutionJob) -> Result<CandidateBundleManifest, BundleError> {
        let bundle_path = self.candidates_root.join(job.job_id.as_str());
        fs::create_dir_all(bundle_path.join("command-logs")).map_err(|source| BundleError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
        let manifest = CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: bundle_path.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: bundle_path
                .join("mutation-packet.json")
                .display()
                .to_string(),
            job_path: Some(bundle_path.join("job.json").display().to_string()),
            model_output_path: None,
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            review_path: None,
            command_logs: Vec::new(),
        };
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    fn write_json<T: Serialize>(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &T,
    ) -> Result<(), BundleError> {
        let path = self.path_for(manifest, name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| BundleError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let content = serde_json::to_string_pretty(value)
            .map_err(|source| BundleError::Serialize(source.to_string()))?;
        fs::write(&path, content).map_err(|source| BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    }

    fn write_text(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &str,
    ) -> Result<(), BundleError> {
        let path = self.path_for(manifest, name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| BundleError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&path, value).map_err(|source| BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    }
}

pub trait MutationModel {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
    ) -> Result<ModelMutationResult, MutationModelError>;
}

mod tools;

pub use tools::{
    EvolutionTool, EvolutionToolCall, EvolutionToolDescriptor, EvolutionToolEventContext,
    EvolutionToolRegistry, EvolutionToolResult, ToolCallStatus, ToolEffect, ToolEvidence,
};

#[cfg(windows)]
fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("powershell");
    shell.args(["-NoProfile", "-NonInteractive", "-Command", command]);
    shell
}

#[cfg(not(windows))]
fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("sh");
    shell.args(["-c", command]);
    shell
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelMutationResult {
    pub provider: String,
    pub model: String,
    pub raw_output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parsed_patch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<StructuredMutationOutput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<EvolutionToolResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_turns: Vec<MutationModelTurn>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MutationModelTurn {
    pub turn: usize,
    pub raw_output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<StructuredMutationOutput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<EvolutionToolResult>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct StructuredMutationOutput {
    #[serde(default, alias = "changedFiles")]
    pub changed_files: Vec<String>,
    #[serde(default, alias = "toolCalls")]
    pub tool_calls: Vec<EvolutionToolCall>,
    #[serde(default, alias = "unified_diff")]
    pub patch: Option<String>,
    #[serde(default, alias = "patchSummary")]
    pub patch_summary: Option<String>,
    #[serde(default, alias = "verificationNotes")]
    pub verification_notes: Option<String>,
    #[serde(default, alias = "experimentNotes")]
    pub experiment_notes: Option<String>,
    #[serde(default, alias = "recoveryNotes")]
    pub recovery_notes: Option<String>,
}

pub struct LanguageModelMutationBridge<P> {
    provider: P,
    model: String,
    tool_registry: EvolutionToolRegistry,
    max_tool_rounds: usize,
}

impl<P> LanguageModelMutationBridge<P> {
    pub fn new(provider: P, model: impl Into<String>) -> Self {
        Self {
            provider,
            model: model.into(),
            tool_registry: EvolutionToolRegistry::bootstrap(),
            max_tool_rounds: 4,
        }
    }

    pub fn with_tool_registry(mut self, tool_registry: EvolutionToolRegistry) -> Self {
        self.tool_registry = tool_registry;
        self
    }

    pub fn with_max_tool_rounds(mut self, max_tool_rounds: usize) -> Self {
        self.max_tool_rounds = max_tool_rounds;
        self
    }
}

impl<P: LanguageModelProvider> MutationModel for LanguageModelMutationBridge<P> {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
    ) -> Result<ModelMutationResult, MutationModelError> {
        let info = self.provider.info();
        let response_format = if info.supports_json_object {
            LanguageModelResponseFormat::JsonObject
        } else {
            LanguageModelResponseFormat::Text
        };
        let direct_edit = info.supports_tool_permissions;
        let structured_response = !direct_edit;
        let descriptors = self.tool_registry.descriptors();
        let mut messages = vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: render_mutation_system_prompt(structured_response, direct_edit),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: render_mutation_bridge_prompt(
                    packet,
                    workspace,
                    &descriptors,
                    structured_response,
                    direct_edit,
                ),
            },
        ];
        let mut model_turns = Vec::new();
        let mut all_tool_results = Vec::new();
        let mut final_response = None;

        for turn in 1..=self.max_tool_rounds.max(1) {
            let request = LanguageModelRequest {
                provider: info.provider.clone(),
                model: self.model.clone(),
                messages: messages.clone(),
                temperature: Some(0.2),
                max_output_tokens: None,
                response_format: response_format.clone(),
                metadata: serde_json::json!({
                    "job_id": packet.job_id,
                    "candidate_workspace": workspace.path,
                    "direct_edit": direct_edit,
                    "target_ring_policy": "ignored-during-bootstrap",
                    "turn": turn,
                    "max_tool_rounds": self.max_tool_rounds,
                }),
            };
            let response = self.provider.complete(&request)?;
            let structured_output = parse_structured_mutation_output(&response.content);
            let tool_calls = if direct_edit {
                Vec::new()
            } else {
                structured_output
                    .as_ref()
                    .map(|output| output.tool_calls.clone())
                    .unwrap_or_default()
            };
            let mut turn_tool_results = Vec::new();
            for call in tool_calls {
                let result = self.tool_registry.execute(workspace, call);
                turn_tool_results.push(result.clone());
                all_tool_results.push(result);
            }
            model_turns.push(MutationModelTurn {
                turn,
                raw_output: response.content.clone(),
                structured_output: structured_output.clone(),
                tool_results: turn_tool_results.clone(),
            });

            let has_final_patch = structured_output
                .as_ref()
                .and_then(|output| output.patch.as_ref())
                .is_some_and(|patch| !patch.trim().is_empty());
            let has_text_patch = extract_patch_from_text(&response.content).is_some();
            if !direct_edit && !turn_tool_results.is_empty() && !has_final_patch && !has_text_patch
            {
                if turn >= self.max_tool_rounds.max(1) {
                    return Err(MutationModelError::ToolRoundLimitExceeded {
                        max_rounds: self.max_tool_rounds,
                    });
                }
                messages.push(LanguageModelMessage {
                    role: LanguageModelRole::Assistant,
                    content: response.content,
                });
                messages.push(LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: render_tool_results_followup(&turn_tool_results),
                });
                continue;
            } else {
                final_response = Some(response);
                break;
            }
        }

        let response = final_response.ok_or(MutationModelError::ToolRoundLimitExceeded {
            max_rounds: self.max_tool_rounds,
        })?;
        let structured_output = parse_structured_mutation_output(&response.content);
        let parsed_patch = structured_output
            .as_ref()
            .and_then(|output| output.patch.clone())
            .or_else(|| extract_patch_from_text(&response.content));
        if !direct_edit
            && let Some(patch) = parsed_patch
                .as_ref()
                .filter(|patch| !patch.trim().is_empty())
        {
            let result = self.tool_registry.execute(
                workspace,
                EvolutionToolCall {
                    tool: "apply_patch".to_string(),
                    input: serde_json::json!({ "patch": patch }),
                },
            );
            if result.status != ToolCallStatus::Succeeded {
                return Err(MutationModelError::ToolExecution {
                    tool: result.tool,
                    detail: result.evidence.summary,
                });
            }
            all_tool_results.push(result);
        }
        let summary = structured_output
            .as_ref()
            .and_then(|output| output.patch_summary.clone())
            .or_else(|| first_non_empty_line(&response.content));
        Ok(ModelMutationResult {
            provider: response.provider,
            model: response.model,
            raw_output: response.content,
            parsed_patch,
            summary,
            structured_output,
            tool_results: all_tool_results,
            model_turns,
        })
    }
}

pub fn render_mutation_prompt(packet: &MutationPacket) -> String {
    let mut prompt = String::new();
    prompt.push_str("# Sinomo Mutation Packet\n\n");
    prompt.push_str(&format!("job_id: {}\n", packet.job_id));
    prompt.push_str("target_ring: ignored during current bootstrap pass\n");
    prompt.push_str(&format!("objective: {}\n\n", packet.objective));
    prompt.push_str("## Intended Edit Scope\n");
    prompt.push_str(&format!(
        "workspace_root: {}\n",
        packet.edit_scope.workspace_root
    ));
    for path in &packet.edit_scope.intended_paths {
        prompt.push_str(&format!("- {path}\n"));
    }
    if !packet.edit_scope.notes.is_empty() {
        prompt.push_str(&format!("notes: {}\n", packet.edit_scope.notes));
    }
    prompt.push_str("\n## Verification Commands\n");
    for command in &packet.verification.commands {
        prompt.push_str(&format!(
            "- {}: `{}` expects exit {} ({:?})\n",
            command.name, command.command, command.expected_exit_code, command.gate
        ));
    }
    prompt.push_str("\n## Relevant Files\n");
    if packet.relevant_files.is_empty() {
        prompt.push_str("- no excerpts were selected; inspect the candidate worktree as needed\n");
    } else {
        for file in &packet.relevant_files {
            prompt.push_str(&format!(
                "\n### {}\n```text\n{}\n```\n",
                file.path, file.content
            ));
        }
    }
    prompt.push_str("\n## Output Contract\n");
    for section in &packet.output_contract.expected_sections {
        prompt.push_str(&format!("- {section}\n"));
    }
    prompt.push_str(&format!(
        "prefer_patch: {}\nnotes: {}\n",
        packet.output_contract.prefer_patch, packet.output_contract.notes
    ));
    prompt.push_str("\n## Recovery\n");
    prompt.push_str(&format!(
        "strategy: {:?}\nnote: {}\n",
        packet.recovery.strategy, packet.recovery.note
    ));
    prompt
}

pub fn render_mutation_bridge_prompt(
    packet: &MutationPacket,
    workspace: &CandidateWorkspace,
    tool_descriptors: &[EvolutionToolDescriptor],
    structured_json: bool,
    direct_edit: bool,
) -> String {
    let mut prompt = render_mutation_prompt(packet);
    prompt.push_str("\n## Candidate Workspace\n");
    prompt.push_str(&format!("path: {}\n", workspace.path.display()));
    prompt.push_str(&format!("branch: {}\n", workspace.branch_name));
    if let Some(revision) = &workspace.source_revision {
        prompt.push_str(&format!("source_revision: {revision}\n"));
    }
    prompt.push_str(
        "Treat this workspace as the only mutation target. The running Sinomo instance may be using the source workspace separately.\n",
    );

    prompt.push_str("\n## Tool Substrate\n");
    if tool_descriptors.is_empty() {
        prompt.push_str("- no registered tools were provided\n");
    } else {
        for descriptor in tool_descriptors {
            prompt.push_str(&format!(
                "- {} effects={:?} input_schema={}\n",
                descriptor.name, descriptor.effects, descriptor.input_schema_json
            ));
        }
    }

    prompt.push_str("\nLong-running command tools emit `run_id` values. If a command produces no further stdout/stderr, the runtime notifies the model after 1 minute, after 5 minutes, and then every 10 minutes of continued inactivity; use `interrupt_command` to send SIGINT or `kill_command` to terminate by run_id when appropriate.\n");

    prompt.push_str("\n## Mutation Mode\n");
    if direct_edit {
        prompt.push_str(
            "Edit files directly in the candidate workspace. Return concise review notes after the edits.\n",
        );
    } else {
        prompt.push_str(
            "Return a unified diff in the `patch` field. Sinomo will apply it to the candidate workspace through the tool registry.\n",
        );
    }

    if structured_json {
        prompt.push_str("\n## Required JSON Response\n");
        prompt.push_str(
            "Return only a JSON object with keys: tool_calls, changed_files, patch, patch_summary, verification_notes, experiment_notes, recovery_notes.\n",
        );
        prompt.push_str(
            "`tool_calls` is an array of objects shaped like `{ \"tool\": \"read_file\", \"input\": { ... } }`. Use it when you need more context or want Sinomo to run a command before finalizing. When ready, leave `tool_calls` empty and provide `patch` or final notes.\n",
        );
        if direct_edit {
            prompt.push_str("When files were edited directly, set patch to null unless you also have a useful review diff.\n");
        }
    }
    prompt
}

fn render_mutation_system_prompt(structured_json: bool, direct_edit: bool) -> String {
    let mut prompt = String::from(
        "You are Sinomo's supervised bootstrap mutation worker. Work only on the requested R0/R1 candidate and produce reviewable evidence.",
    );
    if direct_edit {
        prompt.push_str(
            " You may use the coding-agent harness to edit the candidate workspace directly.",
        );
    } else {
        prompt.push_str(
            " You cannot edit files directly; produce a reviewable patch that Sinomo can apply as a candidate.",
        );
    }
    if structured_json {
        prompt.push_str(" Return valid JSON only.");
    }
    prompt
}

pub fn parse_structured_mutation_output(raw_output: &str) -> Option<StructuredMutationOutput> {
    let json = extract_json_object(raw_output)?;
    serde_json::from_str::<StructuredMutationOutput>(json).ok()
}

fn extract_json_object(raw_output: &str) -> Option<&str> {
    let trimmed = raw_output.trim();
    for fence in ["```json", "```"] {
        if let Some(start) = trimmed.find(fence) {
            let body_start = start + fence.len();
            let body = trimmed[body_start..].trim_start_matches(['\r', '\n']);
            if let Some(end) = body.find("```") {
                let candidate = body[..end].trim();
                if candidate.starts_with('{') && candidate.ends_with('}') {
                    return Some(candidate);
                }
            }
        }
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Some(trimmed);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if start < end {
        Some(&trimmed[start..=end])
    } else {
        None
    }
}

fn extract_patch_from_text(raw_output: &str) -> Option<String> {
    let trimmed = raw_output.trim();
    for fence in ["```diff", "```patch", "```"] {
        if let Some(start) = trimmed.find(fence) {
            let body_start = start + fence.len();
            let body = trimmed[body_start..].trim_start_matches(['\r', '\n']);
            if let Some(end) = body.find("```") {
                let patch = body[..end].trim();
                if patch.contains("diff --git") || patch.contains("--- ") {
                    return Some(patch.to_string());
                }
            }
        }
    }
    trimmed
        .find("diff --git ")
        .map(|start| trimmed[start..].trim().to_string())
}

fn first_non_empty_line(raw_output: &str) -> Option<String> {
    raw_output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('{') && !line.starts_with('}'))
        .map(ToOwned::to_owned)
}

fn render_tool_results_followup(results: &[EvolutionToolResult]) -> String {
    let tool_results_json =
        serde_json::to_string_pretty(results).unwrap_or_else(|_| "[]".to_string());
    format!(
        "Tool results from the previous turn:\n```json\n{tool_results_json}\n```\nUse these results as evidence. Return the next JSON response. If more context is required, request more `tool_calls`; if ready, return an empty `tool_calls` array and provide the final patch or notes."
    )
}

pub struct EvolutionOrchestrator<W, R, B> {
    pub workspace: W,
    pub evaluator: ExecutingEvalHarness<R>,
    pub bundles: B,
}

impl<W, R, B> EvolutionOrchestrator<W, R, B> {
    pub fn new(workspace: W, evaluator: ExecutingEvalHarness<R>, bundles: B) -> Self {
        Self {
            workspace,
            evaluator,
            bundles,
        }
    }
}

impl<W, R, B> EvolutionOrchestrator<W, R, B>
where
    W: WorkspaceAdapter,
    R: CommandRunner,
    B: CandidateBundleStore,
{
    pub fn propose(
        &self,
        request: EvolutionRequest,
        model: Option<&dyn MutationModel>,
    ) -> Result<EvolutionReport, EvolutionError> {
        let job = create_bootstrap_job(&request, self.workspace.root());
        let mut bundle = self.bundles.create(&job)?;
        let candidate_workspace = self.workspace.prepare_candidate(&job)?;
        let packet = mutation_packet_for_job(&job);

        self.bundles.write_json(&bundle, "job.json", &job)?;
        self.bundles
            .write_json(&bundle, "mutation-packet.json", &packet)?;

        let mut model_result = None;
        if request.dry_run {
            // Dry-run proposals only prepare candidate artifacts and verification evidence.
        } else if let Some(model) = model {
            let result = model.mutate(&packet, &candidate_workspace)?;
            self.bundles
                .write_text(&bundle, "model-output.txt", &result.raw_output)?;
            self.bundles
                .write_json(&bundle, "model-result.json", &result)?;
            if let Some(patch) = &result.parsed_patch {
                self.bundles.write_text(&bundle, "model.patch", patch)?;
            }
            if !result.tool_results.is_empty() {
                self.bundles.write_json(
                    &bundle,
                    "model-tool-results.json",
                    &result.tool_results,
                )?;
            }
            if !result.model_turns.is_empty() {
                self.bundles
                    .write_json(&bundle, "model-turns.json", &result.model_turns)?;
            }
            bundle.model_output_path = Some(
                PathBuf::from(&bundle.bundle_path)
                    .join("model-output.txt")
                    .display()
                    .to_string(),
            );
            model_result = Some(result);
        }

        let changed_files = self.workspace.changed_files(&candidate_workspace)?;
        let diff = self.workspace.diff(&candidate_workspace)?;
        self.bundles
            .write_json(&bundle, "changed-files.json", &changed_files)?;
        self.bundles.write_text(&bundle, "diff.patch", &diff)?;
        bundle.changed_files_path = Some(
            PathBuf::from(&bundle.bundle_path)
                .join("changed-files.json")
                .display()
                .to_string(),
        );
        bundle.diff_path = Some(
            PathBuf::from(&bundle.bundle_path)
                .join("diff.patch")
                .display()
                .to_string(),
        );

        let (scorecard, command_results) = self
            .evaluator
            .run_verification_plan(job.verification.clone(), &candidate_workspace.path)?;
        for result in &command_results {
            let log_ref = write_command_log_via_store(&self.bundles, &bundle, result)?;
            bundle.command_logs.push(log_ref);
        }
        self.bundles
            .write_json(&bundle, "scorecard.json", &scorecard)?;
        bundle.scorecard_path = Some(
            PathBuf::from(&bundle.bundle_path)
                .join("scorecard.json")
                .display()
                .to_string(),
        );

        let review_summary = human_review_summary(
            &job,
            &candidate_workspace,
            &changed_files,
            &scorecard,
            model_result.as_ref(),
        );
        self.bundles
            .write_text(&bundle, "human-review.md", &review_summary)?;
        bundle.review_path = Some(
            PathBuf::from(&bundle.bundle_path)
                .join("human-review.md")
                .display()
                .to_string(),
        );
        self.bundles.write_json(&bundle, "manifest.json", &bundle)?;

        Ok(EvolutionReport {
            job: EvolutionJob {
                status: EvolutionJobStatus::HumanReviewRequested,
                ..job
            },
            candidate_workspace,
            bundle,
            scorecard: Some(scorecard),
            changed_files,
            review_summary,
        })
    }
}

fn create_bootstrap_job(request: &EvolutionRequest, source_root: &Path) -> EvolutionJob {
    let job_id = EvolutionJobId::generate();
    let commands = if request.verification_commands.is_empty() {
        vec![VerificationCommand {
            name: "cargo check".to_string(),
            command: "cargo check".to_string(),
            expected_exit_code: 0,
            gate: EvaluationGate::StaticChecks,
        }]
    } else {
        request.verification_commands.clone()
    };
    let required_gates = commands
        .iter()
        .map(|command| command.gate.clone())
        .collect();
    EvolutionJob {
        job_id,
        objective: request.objective.clone(),
        target_ring: request.target_ring.clone(),
        target_module_id: None,
        edit_scope: EditScope {
            workspace_root: source_root.display().to_string(),
            intended_paths: request.intended_paths.clone(),
            notes: "bootstrap supervised self-evolution iteration".to_string(),
        },
        verification: VerificationPlan { commands },
        recovery: RecoveryPlan::default(),
        scope: EvolutionScope {
            allowed_paths: request.intended_paths.clone(),
            forbidden_paths: Vec::new(),
            workspace_id: None,
        },
        status: EvolutionJobStatus::Created,
        required_gates,
        rollback_policy: RollbackPolicy::default(),
    }
}

fn mutation_packet_for_job(job: &EvolutionJob) -> MutationPacket {
    MutationPacket {
        job_id: job.job_id.clone(),
        objective: job.objective.clone(),
        target_ring: job.target_ring.clone(),
        edit_scope: job.edit_scope.clone(),
        relevant_files: select_relevant_files(
            Path::new(&job.edit_scope.workspace_root),
            &job.edit_scope.intended_paths,
        ),
        verification: job.verification.clone(),
        output_contract: MutationOutputContract::default(),
        recovery: job.recovery.clone(),
    }
}

fn select_relevant_files(workspace_root: &Path, intended_paths: &[String]) -> Vec<FileExcerpt> {
    const MAX_FILES: usize = 12;
    const MAX_BYTES_PER_FILE: usize = 16 * 1024;

    let mut candidates = Vec::new();
    for intended_path in intended_paths {
        let root = workspace_root.join(glob_base_path(intended_path));
        if root.is_file() {
            candidates.push(root);
        } else if root.is_dir() {
            collect_relevant_files(&root, &mut candidates, MAX_FILES);
        }
        if candidates.len() >= MAX_FILES {
            break;
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates.truncate(MAX_FILES);

    candidates
        .into_iter()
        .filter_map(|path| {
            let bytes = fs::read(&path).ok()?;
            let content =
                String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_BYTES_PER_FILE)]).into_owned();
            let relative = path
                .strip_prefix(workspace_root)
                .unwrap_or(&path)
                .display()
                .to_string()
                .replace('\\', "/");
            Some(FileExcerpt {
                path: relative,
                content,
            })
        })
        .collect()
}

fn glob_base_path(pattern: &str) -> &str {
    pattern
        .split(['*', '?', '['])
        .next()
        .unwrap_or(pattern)
        .trim_end_matches(['/', '\\'])
}

fn collect_relevant_files(dir: &Path, candidates: &mut Vec<PathBuf>, max_files: usize) {
    if candidates.len() >= max_files || is_ignored_dir(dir) {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if candidates.len() >= max_files {
            break;
        }
        let path = entry.path();
        if path.is_dir() {
            collect_relevant_files(&path, candidates, max_files);
        } else if is_relevant_source_file(&path) {
            candidates.push(path);
        }
    }
}

fn is_ignored_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| matches!(name, ".git" | "target" | "node_modules" | ".evolution"))
}

fn is_relevant_source_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if matches!(name, "Cargo.toml" | "README.md") {
        return true;
    }
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("rs" | "toml" | "md" | "json")
    )
}

fn human_review_summary(
    job: &EvolutionJob,
    workspace: &CandidateWorkspace,
    changed_files: &[String],
    scorecard: &EvaluationScorecard,
    model_result: Option<&ModelMutationResult>,
) -> String {
    let verification = match scorecard.outcome {
        EvaluationOutcome::Passed => "passed",
        EvaluationOutcome::Failed => "failed",
        EvaluationOutcome::Inconclusive => "inconclusive",
    };
    let mut summary = format!(
        "# Sinomo Evolution Candidate\n\njob: {}\nstatus: awaiting-human-review\nobjective: {}\nworktree: {}\nbranch: {}\nverification: {}\nchanged_files:\n{}\n\nrecovery: {}\n",
        job.job_id,
        job.objective,
        workspace.path.display(),
        workspace.branch_name,
        verification,
        changed_files
            .iter()
            .map(|path| format!("- {path}"))
            .collect::<Vec<_>>()
            .join("\n"),
        job.recovery.note
    );
    if let Some(model_result) = model_result {
        summary.push_str("\n## Model Result\n");
        summary.push_str(&format!(
            "provider: {}\nmodel: {}\n",
            model_result.provider, model_result.model
        ));
        if let Some(model_summary) = &model_result.summary {
            summary.push_str(&format!("summary: {model_summary}\n"));
        }
        if let Some(output) = &model_result.structured_output {
            if let Some(notes) = &output.verification_notes {
                summary.push_str(&format!("verification_notes: {notes}\n"));
            }
            if let Some(notes) = &output.recovery_notes {
                summary.push_str(&format!("recovery_notes: {notes}\n"));
            }
        }
        for tool_result in &model_result.tool_results {
            summary.push_str(&format!(
                "tool {}: {:?} ({})\n",
                tool_result.tool, tool_result.status, tool_result.evidence.summary
            ));
        }
    }
    summary
}

fn write_command_log_via_store<B: CandidateBundleStore>(
    store: &B,
    manifest: &CandidateBundleManifest,
    result: &CommandRunResult,
) -> Result<CommandLogRef, BundleError> {
    let safe_name = safe_file_stem(&result.name);
    let path = format!("command-logs/{safe_name}.txt");
    let text = format!(
        "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        result.name,
        result.command,
        result.expected_exit_code,
        result.exit_code,
        result.duration_ms,
        result.stdout,
        result.stderr
    );
    store.write_text(manifest, &path, &text)?;
    Ok(CommandLogRef {
        name: result.name.clone(),
        command: result.command.clone(),
        log_path: PathBuf::from(&manifest.bundle_path)
            .join(&path)
            .display()
            .to_string(),
        exit_code: Some(result.exit_code),
    })
}

fn safe_file_stem(value: &str) -> String {
    let stem: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    stem.trim_matches('-').to_string().if_empty("command")
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

#[derive(Debug, Error)]
pub enum EvolutionError {
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error(transparent)]
    Eval(#[from] sinomo_eval::EvalError),
    #[error(transparent)]
    MutationModel(#[from] MutationModelError),
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("git command `{command}` failed: {detail}")]
    Git { command: String, detail: String },
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to parse git status: {0}")]
    StatusParse(String),
}

#[derive(Debug, Error)]
pub enum BundleError {
    #[error("bundle filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to serialize bundle artifact: {0}")]
    Serialize(String),
}

#[derive(Debug, Error)]
pub enum MutationModelError {
    #[error(transparent)]
    LanguageModel(#[from] LanguageModelError),
    #[error("mutation tool `{tool}` failed: {detail}")]
    ToolExecution { tool: String, detail: String },
    #[error("mutation bridge exceeded max tool rounds ({max_rounds})")]
    ToolRoundLimitExceeded { max_rounds: usize },
}

#[derive(Clone, Debug)]
pub struct EvolutionPlanner {
    node_id: NodeId,
}

impl EvolutionPlanner {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn scope_job(
        &self,
        opportunity: EvolutionOpportunity,
    ) -> (EvolutionJob, CandidateWorkspacePlan) {
        let job_id = EvolutionJobId::generate();
        let required_gates = required_gates_for(&opportunity.target_ring);
        let job = EvolutionJob {
            job_id: job_id.clone(),
            objective: opportunity.objective,
            target_ring: opportunity.target_ring,
            target_module_id: opportunity.target_module_id,
            edit_scope: sinomo_core::EditScope {
                workspace_root: opportunity.workspace_id.to_string(),
                intended_paths: opportunity.allowed_paths.clone(),
                notes: "planner-scoped evolution opportunity".to_string(),
            },
            verification: sinomo_core::VerificationPlan {
                commands: required_gates
                    .iter()
                    .map(|gate| sinomo_core::VerificationCommand {
                        name: format!("{gate:?}"),
                        command: "cargo check".to_string(),
                        expected_exit_code: 0,
                        gate: gate.clone(),
                    })
                    .collect(),
            },
            recovery: sinomo_core::RecoveryPlan::default(),
            scope: EvolutionScope {
                allowed_paths: opportunity.allowed_paths.clone(),
                forbidden_paths: opportunity.forbidden_paths.clone(),
                workspace_id: Some(opportunity.workspace_id.clone()),
            },
            status: EvolutionJobStatus::Scoped,
            required_gates,
            rollback_policy: RollbackPolicy::default(),
        };
        let branch_name = format!("evolution/{}", job_id.as_str());
        let workspace = CandidateWorkspacePlan {
            job_id,
            workspace_id: opportunity.workspace_id,
            branch_name,
            allowed_paths: opportunity.allowed_paths,
            forbidden_paths: opportunity.forbidden_paths,
        };
        (job, workspace)
    }

    pub fn job_event(
        &self,
        job: &EvolutionJob,
        workspace: &CandidateWorkspacePlan,
    ) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("evolution.planner").expect("valid source"),
            EventKind::new("evolution.job.scoped").expect("valid event kind"),
            Salience::new(0.86).expect("valid salience"),
            Confidence::new(0.97).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            )
            .with_hint(
                RoutingHint::new(format!("target:ring:{}", trust_ring_hint(&job.target_ring)))
                    .expect("hint"),
            ),
        )
        .with_payload(serde_json::json!({
            "job": job,
            "candidate_workspace": workspace,
        }));
        if let Some(workspace_id) = &job.scope.workspace_id {
            event.evidence.refs.push(sinomo_core::EvidenceRef {
                uri: format!("dfs://{workspace_id}"),
                kind: sinomo_core::EvidenceKind::Other,
                content_hash: None,
            });
        }
        event
    }
}

pub fn required_gates_for(target_ring: &TrustRing) -> Vec<EvaluationGate> {
    if target_ring.requires_continuity_gates() {
        vec![
            EvaluationGate::ManifestValidation,
            EvaluationGate::StaticChecks,
            EvaluationGate::IntegrationTests,
            EvaluationGate::ReplayTests,
            EvaluationGate::ContinuityChecks,
        ]
    } else {
        vec![
            EvaluationGate::ManifestValidation,
            EvaluationGate::ReplayTests,
        ]
    }
}

fn trust_ring_hint(target_ring: &TrustRing) -> &'static str {
    match target_ring {
        TrustRing::R0ContinuityRoot => "r0",
        TrustRing::R1TrustedRuntime => "r1",
        TrustRing::R2TrustedService => "r2",
        TrustRing::R3SandboxedModule => "r3",
        TrustRing::R4DisposableWorkspace => "r4",
        TrustRing::R5ModelProvider => "r5",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_eval::LocalCommandRunner;
    use sinomo_lm::{
        LanguageModelProviderInfo, LanguageModelResponse, LanguageModelUsage,
        PROVIDER_DEFAULT_MODEL,
    };
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn r1_job_receives_continuity_gates_and_workspace_scope() {
        let planner = EvolutionPlanner::new(NodeId::new("node_desktop").expect("node"));
        let (job, workspace) = planner.scope_job(EvolutionOpportunity {
            observed_event_id: None,
            target_ring: TrustRing::R1TrustedRuntime,
            target_module_id: None,
            objective: "harden event store".to_string(),
            workspace_id: WorkspaceId::new("workspace_runtime").expect("workspace"),
            allowed_paths: vec!["crates/sinomo-events/**".to_string()],
            forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
            priority: 0.9,
        });

        assert!(
            job.required_gates
                .contains(&EvaluationGate::ContinuityChecks)
        );
        assert_eq!(job.scope.workspace_id, Some(workspace.workspace_id));
        assert!(workspace.branch_name.starts_with("evolution/evojob_"));
    }

    #[test]
    fn git_status_parser_handles_modified_untracked_and_renamed_paths() {
        let paths = parse_git_status_paths(" M crates/a.rs\n?? docs/new.md\nR  old.rs -> new.rs\n")
            .expect("status paths");

        assert_eq!(
            paths,
            vec![
                "crates/a.rs".to_string(),
                "docs/new.md".to_string(),
                "new.rs".to_string()
            ]
        );
    }

    #[test]
    fn mutation_prompt_contains_objective_verification_and_recovery() {
        let packet = MutationPacket {
            job_id: EvolutionJobId::new("evojob_prompt").expect("job"),
            objective: "improve bundle summaries".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: "candidate".to_string(),
                intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                notes: "keep it focused".to_string(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: vec![VerificationCommand {
                    name: "check".to_string(),
                    command: "cargo check".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let prompt = render_mutation_prompt(&packet);

        assert!(prompt.contains("improve bundle summaries"));
        assert!(prompt.contains("cargo check"));
        assert!(prompt.contains("recovery notes"));
        assert!(prompt.contains("restore the disposable VM snapshot"));
    }

    #[test]
    fn bridge_prompt_exposes_candidate_workspace_tools_and_json_contract() {
        let root = temp_dir("bridge-prompt");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "add mutation evidence".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                notes: "keep it focused".to_string(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let prompt = render_mutation_bridge_prompt(
            &packet,
            &workspace,
            &EvolutionToolRegistry::bootstrap().descriptors(),
            true,
            false,
        );

        assert!(prompt.contains("Candidate Workspace"));
        assert!(prompt.contains(&root.display().to_string()));
        assert!(prompt.contains("apply_patch"));
        assert!(prompt.contains("interrupt_command"));
        assert!(prompt.contains("kill_command"));
        assert!(prompt.contains("no further stdout/stderr"));
        assert!(prompt.contains("Required JSON Response"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn structured_mutation_output_parses_json_and_patch_fences() {
        let structured = parse_structured_mutation_output(
            r#"{"changed_files":["src/lib.rs"],"patch":"diff --git a/src/lib.rs b/src/lib.rs","patch_summary":"updated bridge"}"#,
        )
        .expect("structured output");

        assert_eq!(structured.changed_files, vec!["src/lib.rs".to_string()]);
        assert_eq!(structured.patch_summary.as_deref(), Some("updated bridge"));

        let patch = extract_patch_from_text(
            "notes\n```diff\ndiff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n```\n",
        )
        .expect("patch");
        assert!(patch.starts_with("diff --git"));

        let fenced = parse_structured_mutation_output(
            "```json\n{\"tool_calls\":[{\"tool\":\"read_file\",\"input\":{\"path\":\"Cargo.toml\"}}],\"changed_files\":[]}\n```",
        )
        .expect("fenced json");
        assert_eq!(fenced.tool_calls.len(), 1);
        assert_eq!(fenced.tool_calls[0].tool, "read_file");
    }

    #[test]
    fn mutation_packet_collects_relevant_source_excerpts_from_intended_paths() {
        let root = temp_dir("packet-excerpts");
        fs::create_dir_all(root.join("crates/example/src")).expect("dirs");
        fs::write(
            root.join("crates/example/Cargo.toml"),
            "[package]\nname = \"example\"\n",
        )
        .expect("manifest");
        fs::write(
            root.join("crates/example/src/lib.rs"),
            "pub fn example() {}\n",
        )
        .expect("lib");
        let job = create_bootstrap_job(
            &EvolutionRequest {
                objective: "collect context".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["crates/example/**".to_string()],
                verification_commands: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            &root,
        );

        let packet = mutation_packet_for_job(&job);

        assert!(
            packet
                .relevant_files
                .iter()
                .any(|file| file.path == "crates/example/Cargo.toml")
        );
        assert!(
            packet
                .relevant_files
                .iter()
                .any(|file| file.path == "crates/example/src/lib.rs")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn language_model_bridge_requests_json_and_applies_returned_patch() {
        let root = git_repo("bridge-applies-patch");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let provider = FakeLanguageModelProvider::new(
            LanguageModelProviderInfo {
                provider: "fake-json".to_string(),
                default_model: "fake-model".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::Http,
                supports_json_object: true,
                supports_tool_permissions: false,
            },
            serde_json::json!({
                "changed_files": ["patched.txt"],
                "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
                "patch_summary": "updated patched.txt",
                "verification_notes": "not run",
                "experiment_notes": "unit test fixture",
                "recovery_notes": "restore temp repo"
            })
            .to_string(),
        );
        let requests = provider.requests.clone();
        let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "apply patch from json provider".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["patched.txt".to_string()],
                notes: String::new(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let result = bridge.mutate(&packet, &workspace).expect("mutate");

        assert_eq!(
            requests.borrow()[0].response_format,
            LanguageModelResponseFormat::JsonObject
        );
        assert_eq!(result.summary.as_deref(), Some("updated patched.txt"));
        assert!(result.parsed_patch.as_deref().is_some());
        assert_eq!(result.tool_results[0].status, ToolCallStatus::Succeeded);
        let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
        assert_eq!(patched.replace("\r\n", "\n"), "new\n");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn language_model_bridge_runs_tool_rounds_before_final_patch() {
        let root = git_repo("bridge-tool-rounds");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let provider = FakeLanguageModelProvider::with_responses(
            LanguageModelProviderInfo {
                provider: "fake-tool-loop".to_string(),
                default_model: "fake-model".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::Http,
                supports_json_object: true,
                supports_tool_permissions: false,
            },
            vec![
                serde_json::json!({
                    "tool_calls": [
                        { "tool": "read_file", "input": { "path": "patched.txt" } }
                    ],
                    "changed_files": []
                })
                .to_string(),
                serde_json::json!({
                    "tool_calls": [],
                    "changed_files": ["patched.txt"],
                    "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
                    "patch_summary": "updated after reading file"
                })
                .to_string(),
            ],
        );
        let requests = provider.requests.clone();
        let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "use tools before patch".to_string(),
            target_ring: TrustRing::R3SandboxedModule,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["patched.txt".to_string()],
                notes: String::new(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let result = bridge.mutate(&packet, &workspace).expect("mutate");

        assert_eq!(requests.borrow().len(), 2);
        assert!(requests.borrow()[0].metadata["target_ring"].is_null());
        assert_eq!(
            requests.borrow()[0].metadata["target_ring_policy"],
            "ignored-during-bootstrap"
        );
        assert!(
            requests.borrow()[1]
                .messages
                .last()
                .unwrap()
                .content
                .contains("Tool results")
        );
        assert_eq!(result.model_turns.len(), 2);
        assert_eq!(result.tool_results[0].tool, "read_file");
        assert_eq!(result.tool_results[1].tool, "apply_patch");
        let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
        assert_eq!(patched.replace("\r\n", "\n"), "new\n");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn language_model_bridge_errors_when_final_patch_fails_to_apply() {
        let root = git_repo("bridge-bad-patch");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let provider = FakeLanguageModelProvider::new(
            LanguageModelProviderInfo {
                provider: "fake-json".to_string(),
                default_model: "fake-model".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::Http,
                supports_json_object: true,
                supports_tool_permissions: false,
            },
            serde_json::json!({
                "tool_calls": [],
                "changed_files": ["missing.txt"],
                "patch": "diff --git a/missing.txt b/missing.txt\n--- a/missing.txt\n+++ b/missing.txt\n@@ -1 +1 @@\n-old\n+new\n"
            })
            .to_string(),
        );
        let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "bad patch".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["missing.txt".to_string()],
                notes: String::new(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let error = bridge.mutate(&packet, &workspace).expect_err("bad patch");

        assert!(matches!(error, MutationModelError::ToolExecution { .. }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_bundle_store_writes_packet_and_manifest_paths() {
        let root = temp_dir("bundle-store");
        let store = LocalCandidateBundleStore::new(root.join("candidates"));
        let job = create_bootstrap_job(
            &EvolutionRequest {
                objective: "write bundle".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: Vec::new(),
                verification_commands: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            &root,
        );
        let manifest = store.create(&job).expect("bundle create");
        let packet = mutation_packet_for_job(&job);

        store
            .write_json(&manifest, "mutation-packet.json", &packet)
            .expect("packet write");

        assert!(PathBuf::from(&manifest.bundle_path).exists());
        assert!(PathBuf::from(&manifest.mutation_packet_path).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orchestrator_dry_run_writes_bundle_and_runs_verification() {
        let root = temp_dir("orchestrator");
        let candidate = root.join("candidate");
        fs::create_dir_all(&candidate).expect("candidate dir");
        let orchestrator = EvolutionOrchestrator::new(
            FakeWorkspace {
                root: root.clone(),
                candidate: candidate.clone(),
            },
            ExecutingEvalHarness::new(LocalCommandRunner::new()),
            LocalCandidateBundleStore::new(root.join("candidates")),
        );

        let report = orchestrator
            .propose(
                EvolutionRequest {
                    objective: "dry run".to_string(),
                    target_ring: TrustRing::R1TrustedRuntime,
                    intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                    verification_commands: vec![VerificationCommand {
                        name: "success".to_string(),
                        command: "exit 0".to_string(),
                        expected_exit_code: 0,
                        gate: EvaluationGate::StaticChecks,
                    }],
                    model_provider: None,
                    model: None,
                    dry_run: true,
                },
                None,
            )
            .expect("dry run propose");

        assert_eq!(report.job.status, EvolutionJobStatus::HumanReviewRequested);
        assert_eq!(
            report.scorecard.as_ref().expect("scorecard").outcome,
            EvaluationOutcome::Passed
        );
        assert!(PathBuf::from(&report.bundle.mutation_packet_path).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orchestrator_writes_model_turn_and_tool_evidence_artifacts() {
        let root = temp_dir("orchestrator-model-evidence");
        let candidate = root.join("candidate");
        fs::create_dir_all(&candidate).expect("candidate dir");
        let orchestrator = EvolutionOrchestrator::new(
            FakeWorkspace {
                root: root.clone(),
                candidate: candidate.clone(),
            },
            ExecutingEvalHarness::new(LocalCommandRunner::new()),
            LocalCandidateBundleStore::new(root.join("jobs")),
        );
        let model = FakeMutationModel;

        let report = orchestrator
            .propose(
                EvolutionRequest {
                    objective: "write model evidence".to_string(),
                    target_ring: TrustRing::R1TrustedRuntime,
                    intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                    verification_commands: vec![VerificationCommand {
                        name: "success".to_string(),
                        command: "exit 0".to_string(),
                        expected_exit_code: 0,
                        gate: EvaluationGate::StaticChecks,
                    }],
                    model_provider: None,
                    model: None,
                    dry_run: false,
                },
                Some(&model),
            )
            .expect("propose");
        let bundle = PathBuf::from(&report.bundle.bundle_path);

        assert!(bundle.join("model-output.txt").exists());
        assert!(bundle.join("model-result.json").exists());
        assert!(bundle.join("model-turns.json").exists());
        assert!(bundle.join("model-tool-results.json").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn git_workspace_adapter_places_worktree_inside_job_folder() {
        let source = git_repo("git-workspace-layout-source");
        fs::write(source.join("README.md"), "# source\n").expect("readme");
        run_in(&source, "git add README.md").expect("git add");
        run_in(&source, "git commit -m init").expect("git commit");
        let jobs_root = temp_dir("git-workspace-layout-jobs");
        let job = create_bootstrap_job(
            &EvolutionRequest {
                objective: "layout".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["README.md".to_string()],
                verification_commands: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            &source,
        );
        let adapter = GitWorkspaceAdapter::new(&source, &jobs_root);

        let workspace = adapter.prepare_candidate(&job).expect("candidate");

        assert_eq!(
            workspace.path,
            jobs_root.join(job.job_id.as_str()).join("worktree")
        );
        assert!(workspace.path.exists());
        let _ = run_in(
            &source,
            &format!("git worktree remove --force {}", workspace.path.display()),
        );
        let _ = fs::remove_dir_all(source);
        let _ = fs::remove_dir_all(jobs_root);
    }

    #[test]
    fn bootstrap_tool_registry_exposes_minimal_phase_three_tools() {
        let registry = EvolutionToolRegistry::bootstrap();
        let names = registry
            .descriptors()
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();

        for expected in [
            "read_file",
            "list_files",
            "search",
            "apply_patch",
            "run_command",
            "interrupt_command",
            "kill_command",
            "git_diff",
            "git_status",
            "format",
            "test",
        ] {
            assert!(names.iter().any(|name| name == expected), "{expected}");
        }
    }

    #[test]
    fn read_file_tool_returns_content_and_path_evidence() {
        let root = temp_dir("read-file-tool");
        fs::create_dir_all(&root).expect("root");
        fs::write(root.join("notes.txt"), "hello tools").expect("write note");
        let workspace = candidate_workspace(&root);
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "read_file".to_string(),
                input: serde_json::json!({ "path": "notes.txt" }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        assert_eq!(result.output["content"], "hello tools");
        assert_eq!(result.evidence.paths, vec!["notes.txt".to_string()]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_registry_can_emit_execution_event_with_result_and_changed_files() {
        let root = git_repo("tool-event");
        fs::write(root.join("tracked.txt"), "before\n").expect("write tracked");
        run_in(&root, "git add tracked.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        fs::write(root.join("tracked.txt"), "after\n").expect("modify tracked");
        let workspace = candidate_workspace(&root);
        let context = EvolutionToolEventContext::new(
            NodeId::new("node_tool_test").expect("node"),
            workspace.job_id.clone(),
        );

        let (result, event) = EvolutionToolRegistry::bootstrap().execute_with_event(
            &workspace,
            &context,
            EvolutionToolCall {
                tool: "read_file".to_string(),
                input: serde_json::json!({ "path": "tracked.txt" }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        assert_eq!(event.kind.as_str(), "evolution.tool.executed");
        assert_eq!(event.payload["tool"], "read_file");
        assert_eq!(event.payload["result"]["status"], "succeeded");
        assert_eq!(
            event.payload["changed_files"],
            serde_json::json!(["tracked.txt"])
        );
        assert_eq!(event.evidence.refs.len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_command_tool_captures_exit_code_stdout_and_stderr() {
        let root = temp_dir("run-command-tool");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": output_command() }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        assert_eq!(result.output["exit_code"], 0);
        assert!(result.output["run_id"].as_str().is_some());
        assert!(
            result.output["stdout"]
                .as_str()
                .unwrap()
                .contains("sinomo-stdout")
        );
        assert!(
            result.output["stderr"]
                .as_str()
                .unwrap()
                .contains("sinomo-stderr")
        );
        assert!(result.evidence.command.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn command_control_tool_reports_unknown_run_id() {
        let root = temp_dir("command-control-tool");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "interrupt_command".to_string(),
                input: serde_json::json!({ "run_id": "cmd_missing" }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Failed);
        assert!(
            result.evidence.summary.contains("cmd_missing"),
            "{}",
            result.evidence.summary
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn kill_command_tool_can_terminate_active_run_command() {
        use std::thread;

        let root = temp_dir("kill-command-tool");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let registry = std::sync::Arc::new(EvolutionToolRegistry::bootstrap());
        let running_registry = registry.clone();
        let running_workspace = workspace.clone();
        let handle = thread::spawn(move || {
            running_registry.execute(
                &running_workspace,
                EvolutionToolCall {
                    tool: "run_command".to_string(),
                    input: serde_json::json!({ "command": "sleep 5" }),
                },
            )
        });
        let run_id = wait_for_active_command(&registry);

        let control = registry.execute(
            &workspace,
            EvolutionToolCall {
                tool: "kill_command".to_string(),
                input: serde_json::json!({ "run_id": run_id }),
            },
        );
        let result = handle.join().expect("run command thread");

        assert_eq!(control.status, ToolCallStatus::Succeeded);
        assert_ne!(result.output["exit_code"], 0);
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    fn wait_for_active_command(registry: &EvolutionToolRegistry) -> String {
        for _ in 0..100 {
            if let Some(run_id) = registry.active_command_run_ids().into_iter().next() {
                return run_id;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("expected an active command run");
    }

    #[test]
    fn git_tools_report_status_and_diff_from_candidate_workspace() {
        let root = git_repo("git-tools");
        fs::write(root.join("tracked.txt"), "before\n").expect("write tracked");
        run_in(&root, "git add tracked.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        fs::write(root.join("tracked.txt"), "after\n").expect("modify tracked");
        let workspace = candidate_workspace(&root);
        let registry = EvolutionToolRegistry::bootstrap();

        let status = registry.execute(
            &workspace,
            EvolutionToolCall {
                tool: "git_status".to_string(),
                input: serde_json::json!({}),
            },
        );
        let diff = registry.execute(
            &workspace,
            EvolutionToolCall {
                tool: "git_diff".to_string(),
                input: serde_json::json!({}),
            },
        );

        assert_eq!(status.status, ToolCallStatus::Succeeded);
        assert!(
            status.output["stdout"]
                .as_str()
                .unwrap()
                .contains("tracked.txt")
        );
        assert_eq!(diff.status, ToolCallStatus::Succeeded);
        assert!(diff.output["stdout"].as_str().unwrap().contains("+after"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_patch_tool_applies_unified_diff_in_candidate_workspace() {
        let root = git_repo("apply-patch-tool");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let patch = "\
diff --git a/patched.txt b/patched.txt
index 3b18e51..6320cd2 100644
--- a/patched.txt
+++ b/patched.txt
@@ -1 +1 @@
-old
+new
";
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
        assert_eq!(patched.replace("\r\n", "\n"), "new\n");
        let _ = fs::remove_dir_all(root);
    }

    #[derive(Clone, Debug)]
    struct FakeLanguageModelProvider {
        info: LanguageModelProviderInfo,
        responses: Vec<String>,
        requests: Rc<RefCell<Vec<LanguageModelRequest>>>,
    }

    impl FakeLanguageModelProvider {
        fn new(info: LanguageModelProviderInfo, response: String) -> Self {
            Self::with_responses(info, vec![response])
        }

        fn with_responses(info: LanguageModelProviderInfo, responses: Vec<String>) -> Self {
            Self {
                info,
                responses,
                requests: Rc::new(RefCell::new(Vec::new())),
            }
        }
    }

    impl LanguageModelProvider for FakeLanguageModelProvider {
        fn info(&self) -> LanguageModelProviderInfo {
            self.info.clone()
        }

        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            let response_index = self.requests.borrow().len();
            self.requests.borrow_mut().push(request.clone());
            Ok(LanguageModelResponse {
                provider: self.info.provider.clone(),
                model: self.info.default_model.clone(),
                content: self
                    .responses
                    .get(response_index)
                    .or_else(|| self.responses.last())
                    .cloned()
                    .unwrap_or_default(),
                usage: Some(LanguageModelUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(20),
                }),
            })
        }
    }

    #[derive(Clone, Debug)]
    struct FakeMutationModel;

    impl MutationModel for FakeMutationModel {
        fn mutate(
            &self,
            _packet: &MutationPacket,
            _workspace: &CandidateWorkspace,
        ) -> Result<ModelMutationResult, MutationModelError> {
            let tool_result = EvolutionToolResult {
                tool: "read_file".to_string(),
                status: ToolCallStatus::Succeeded,
                output: serde_json::json!({ "content": "evidence" }),
                evidence: ToolEvidence {
                    summary: "read evidence".to_string(),
                    command: None,
                    exit_code: None,
                    duration_ms: None,
                    paths: vec!["evidence.txt".to_string()],
                },
            };
            Ok(ModelMutationResult {
                provider: "fake".to_string(),
                model: "fake-model".to_string(),
                raw_output: "{\"patch_summary\":\"fake mutation\"}".to_string(),
                parsed_patch: None,
                summary: Some("fake mutation".to_string()),
                structured_output: Some(StructuredMutationOutput {
                    patch_summary: Some("fake mutation".to_string()),
                    ..StructuredMutationOutput::default()
                }),
                tool_results: vec![tool_result.clone()],
                model_turns: vec![MutationModelTurn {
                    turn: 1,
                    raw_output: "{\"patch_summary\":\"fake mutation\"}".to_string(),
                    structured_output: Some(StructuredMutationOutput {
                        patch_summary: Some("fake mutation".to_string()),
                        ..StructuredMutationOutput::default()
                    }),
                    tool_results: vec![tool_result],
                }],
            })
        }
    }

    #[derive(Clone, Debug)]
    struct FakeWorkspace {
        root: PathBuf,
        candidate: PathBuf,
    }

    impl WorkspaceAdapter for FakeWorkspace {
        fn root(&self) -> &Path {
            &self.root
        }

        fn prepare_candidate(
            &self,
            job: &EvolutionJob,
        ) -> Result<CandidateWorkspace, WorkspaceError> {
            Ok(CandidateWorkspace {
                job_id: job.job_id.clone(),
                path: self.candidate.clone(),
                branch_name: "codex/evolve/test".to_string(),
                source_revision: Some("test".to_string()),
            })
        }

        fn changed_files(
            &self,
            _workspace: &CandidateWorkspace,
        ) -> Result<Vec<String>, WorkspaceError> {
            Ok(Vec::new())
        }

        fn diff(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
            Ok(String::new())
        }

        fn status(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
            Ok(String::new())
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-evolution-{name}-{stamp}"))
    }

    fn candidate_workspace(root: &Path) -> CandidateWorkspace {
        CandidateWorkspace {
            job_id: EvolutionJobId::new("evojob_tools").expect("job"),
            path: root.to_path_buf(),
            branch_name: "codex/evolve/tools".to_string(),
            source_revision: Some("test".to_string()),
        }
    }

    fn git_repo(name: &str) -> PathBuf {
        let root = temp_dir(name);
        fs::create_dir_all(&root).expect("root");
        run_in(&root, "git init").expect("git init");
        run_in(&root, "git config user.email sinomo@example.test").expect("git email");
        run_in(&root, "git config user.name Sinomo").expect("git name");
        root
    }

    fn run_in(cwd: &Path, command: &str) -> Result<(), String> {
        let output = super::shell_command(command)
            .current_dir(cwd)
            .output()
            .map_err(|error| error.to_string())?;
        if output.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }

    #[cfg(windows)]
    fn output_command() -> &'static str {
        "Write-Output sinomo-stdout; [Console]::Error.WriteLine('sinomo-stderr')"
    }

    #[cfg(not(windows))]
    fn output_command() -> &'static str {
        "printf sinomo-stdout; printf sinomo-stderr >&2"
    }
}
