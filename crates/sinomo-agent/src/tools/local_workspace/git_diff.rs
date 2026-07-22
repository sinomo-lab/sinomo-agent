use super::*;
use std::path::Component;

#[derive(Clone)]
pub(super) struct GitDiffTool {
    config: LocalWorkspaceAgentConfig,
}

impl GitDiffTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for GitDiffTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        let staged = input_bool(request.call, "staged").unwrap_or(false);
        let max_bytes = input_usize(request.call, "max_bytes")
            .unwrap_or(self.config.max_read_bytes)
            .min(self.config.max_read_bytes);
        let path = match input_string(request.call, "path")
            .map(|path| normalize_git_path_arg(&path))
            .transpose()
        {
            Ok(path) => path,
            Err(detail) => {
                return AgentToolResult::blocked(
                    detail.clone(),
                    serde_json::json!({ "error": "path_not_allowed", "detail": detail }),
                );
            }
        };
        let timeout_ms = input_timeout_ms(request.call);

        let diff_output = match run_git_diff_command(
            &self.config,
            request.session,
            staged,
            path.as_deref(),
            &[],
            timeout_ms,
            &request.cancellation_token,
            request.progress_sink.clone(),
        ) {
            Ok(output) => output,
            Err(result) => return result,
        };
        let stat_output = match run_git_diff_command(
            &self.config,
            request.session,
            staged,
            path.as_deref(),
            &["--stat"],
            timeout_ms,
            &request.cancellation_token,
            request.progress_sink.clone(),
        ) {
            Ok(output) => output,
            Err(result) => return result,
        };
        let names_output = match run_git_diff_command(
            &self.config,
            request.session,
            staged,
            path.as_deref(),
            &["--name-only"],
            timeout_ms,
            &request.cancellation_token,
            request.progress_sink.clone(),
        ) {
            Ok(output) => output,
            Err(result) => return result,
        };

        let diff = String::from_utf8_lossy(&diff_output.output.stdout).to_string();
        let stat = String::from_utf8_lossy(&stat_output.output.stdout).to_string();
        let changed_files = String::from_utf8_lossy(&names_output.output.stdout)
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let visible = truncate_utf8_lossy(diff.as_bytes(), max_bytes);
        let full_diff_ref = if !diff.is_empty() {
            write_local_evidence(&self.config, "git_diff", "diff", diff.as_bytes())
        } else {
            None
        };

        let result = serde_json::json!({
            "staged": staged,
            "path": &path,
            "changed_files": &changed_files,
            "stat": &stat,
            "diff": &visible.text,
            "diff_truncated": visible.truncated,
            "full_diff_ref": &full_diff_ref,
        });
        let mut evidence =
            AgentToolEvidence::availability_only(true, "inspected git diff for workspace changes")
                .with_output_value(&result, visible.truncated)
                .with_record(
                    AGENT_CODING_EVIDENCE_KIND,
                    &AgentCodingToolEvidence {
                        diff_ref: full_diff_ref.clone(),
                        diff_summary: if stat.trim().is_empty() {
                            None
                        } else {
                            Some(stat.clone())
                        },
                        changed_files: changed_files.clone(),
                        ..AgentCodingToolEvidence::default()
                    },
                );
        if let Some(full_diff_ref) = full_diff_ref {
            evidence = evidence.with_artifact(
                AgentToolArtifactRef::new("diff", full_diff_ref).with_media_type("text/x-diff"),
            );
        }
        AgentToolResult::succeeded(
            format!("diff includes {} changed file(s)", changed_files.len()),
            result,
        )
        .with_evidence(evidence)
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    descriptor_for_kind(AgentToolKind::GitDiff)
        .with_toolsets(["version_control"])
        .with_capabilities([
            AgentToolCapability::ReadWorkspace,
            AgentToolCapability::InspectVersionControl,
        ])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Optional workspace-relative path to diff."
                },
                "staged": {
                    "type": "boolean",
                    "description": "Diff staged changes instead of unstaged changes. Defaults to false."
                },
                "max_bytes": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum model-visible diff bytes."
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional timeout for each git diff subprocess."
                }
            }
        }))
        .requiring_local_permission(
            sinomo_runtime::node::LocalNodePermission::RemoteManagement.as_str(),
        )
}

fn run_git_diff_command(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    staged: bool,
    path: Option<&str>,
    extra_args: &[&str],
    timeout_ms: Option<u64>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> Result<LocalCommandExecution, AgentToolResult> {
    let mut args = vec!["diff".to_string(), "--no-ext-diff".to_string()];
    args.extend(extra_args.iter().map(|arg| (*arg).to_string()));
    if staged {
        args.push("--cached".to_string());
    }
    args.push("--".to_string());
    if let Some(path) = path {
        args.push(path.to_string());
    }
    let execution = execute_local_command(
        config,
        session,
        &config.git_program,
        &args,
        None,
        timeout_ms,
        None,
        cancellation_token,
        progress_sink,
    )?;
    if execution.output.success && !execution.cancelled && !execution.timed_out {
        return Ok(execution);
    }
    Err(local_command_result_from_output(
        config,
        execution.root,
        execution.cwd,
        &config.git_program,
        &args,
        &execution.output,
        execution.timed_out,
        execution.cancelled,
        execution.cleanup_pending,
        execution.duration_ms,
        timeout_ms,
        "git_diff",
        "inspected git diff for workspace changes",
        Vec::new(),
        Some("git diff".to_string()),
    ))
}

fn normalize_git_path_arg(path: &str) -> Result<String, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." {
        return Ok(".".to_string());
    }
    let requested = Path::new(trimmed);
    if requested.is_absolute() {
        return Err(format!("path {trimmed} must be workspace-relative"));
    }
    for component in requested.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            return Err(format!("path {trimmed} escapes workspace root"));
        }
    }
    Ok(trimmed.to_string())
}
