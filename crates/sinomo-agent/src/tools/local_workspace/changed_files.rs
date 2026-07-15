use super::*;
use serde::Serialize;

#[derive(Clone)]
pub(super) struct ChangedFilesTool {
    config: LocalWorkspaceAgentConfig,
}

impl ChangedFilesTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for ChangedFilesTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        let include_untracked = input_bool(request.call, "include_untracked").unwrap_or(true);
        let args = vec![
            "status".to_string(),
            "--porcelain=v1".to_string(),
            "-z".to_string(),
        ];
        let execution = match execute_local_command(
            &self.config,
            request.session,
            &self.config.git_program,
            &args,
            None,
            input_timeout_ms(request.call),
            None,
            &request.cancellation_token,
            request.progress_sink.clone(),
        ) {
            Ok(execution) => execution,
            Err(result) => return result,
        };
        if !execution.output.success || execution.cancelled || execution.timed_out {
            return local_command_result_from_output(
                &self.config,
                execution.root,
                execution.cwd,
                &self.config.git_program,
                &args,
                &execution.output,
                execution.timed_out,
                execution.cancelled,
                execution.cleanup_pending,
                execution.duration_ms,
                input_timeout_ms(request.call),
                "changed_files",
                "inspected git status for workspace changes",
                Vec::new(),
                Some("git status".to_string()),
            );
        }
        let changed = parse_git_status_porcelain_z(&execution.output.stdout, include_untracked);
        let paths = changed
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let result = serde_json::json!({
            "count": changed.len(),
            "include_untracked": include_untracked,
            "changed_files": &changed,
            "paths": &paths,
        });
        AgentToolResult::succeeded(
            format!("found {} changed file(s)", paths.len()),
            result.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(
                true,
                "inspected git status for workspace changes",
            )
            .with_output_value(&result, false)
            .with_record(
                AGENT_CODING_EVIDENCE_KIND,
                &AgentCodingToolEvidence {
                    changed_files: paths,
                    ..AgentCodingToolEvidence::default()
                },
            ),
        )
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::ChangedFiles))
        .with_toolsets(["version_control"])
        .with_capabilities([
            AgentToolCapability::ReadWorkspace,
            AgentToolCapability::InspectVersionControl,
        ])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "include_untracked": {
                    "type": "boolean",
                    "description": "Whether to include untracked files. Defaults to true."
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional timeout for the git status subprocess."
                }
            }
        }))
        .requiring_local_permission(
            sinomo_runtime::node::LocalNodePermission::RemoteManagement.as_str(),
        )
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
struct ChangedFileStatus {
    path: String,
    status_code: String,
    staged: String,
    unstaged: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_path: Option<String>,
}

fn parse_git_status_porcelain_z(bytes: &[u8], include_untracked: bool) -> Vec<ChangedFileStatus> {
    let mut files = Vec::new();
    let mut parts = bytes
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty());
    while let Some(entry) = parts.next() {
        if entry.len() < 4 {
            continue;
        }
        let staged_code = entry[0] as char;
        let unstaged_code = entry[1] as char;
        if !include_untracked && staged_code == '?' && unstaged_code == '?' {
            continue;
        }
        let path = String::from_utf8_lossy(&entry[3..]).to_string();
        let original_path =
            if matches!(staged_code, 'R' | 'C') || matches!(unstaged_code, 'R' | 'C') {
                parts
                    .next()
                    .map(|part| String::from_utf8_lossy(part).to_string())
            } else {
                None
            };
        files.push(ChangedFileStatus {
            path,
            status_code: format!("{staged_code}{unstaged_code}"),
            staged: git_status_label(staged_code).to_string(),
            unstaged: git_status_label(unstaged_code).to_string(),
            original_path,
        });
    }
    files
}

fn git_status_label(code: char) -> &'static str {
    match code {
        ' ' => "unchanged",
        'M' => "modified",
        'A' => "added",
        'D' => "deleted",
        'R' => "renamed",
        'C' => "copied",
        'U' => "unmerged",
        '?' => "untracked",
        '!' => "ignored",
        _ => "unknown",
    }
}
