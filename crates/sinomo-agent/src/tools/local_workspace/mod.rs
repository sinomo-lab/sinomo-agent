mod apply_patch;
mod command_session;
mod format;
mod git_diff;
mod git_status;
mod list_files;
mod read_file;
mod request_user_input;
mod run_command;
mod search;
mod test;

use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentTool, AgentToolArtifactRef,
    AgentToolCapability, AgentToolDescriptor, AgentToolEvidence, AgentToolProviderSpec,
    AgentToolRegistry, AgentToolResult, AgentToolStatus,
};
use crate::{
    AgentStoragePaths, AgentToolCall, AgentToolKind, AgentToolSpec, AgentVerificationState, TaskId,
    default_coding_agent_tools,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const DEFAULT_LOCAL_MAX_READ_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE: u64 = 512 * 1024;
const DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_PATCH_BYTES: usize = 512 * 1024;
const DEFAULT_LOCAL_MAX_LIST_ENTRIES: usize = 500;
const DEFAULT_LOCAL_MAX_SEARCH_MATCHES: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalWorkspaceAgentConfig {
    pub root: PathBuf,
    pub evidence_dir: Option<PathBuf>,
    pub allow_shell_commands: bool,
    pub allow_file_writes: bool,
    pub max_read_bytes: usize,
    pub max_search_bytes_per_file: u64,
    pub max_command_output_bytes: usize,
    pub max_patch_bytes: usize,
    pub max_list_entries: usize,
    pub max_search_matches: usize,
}

impl LocalWorkspaceAgentConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            evidence_dir: None,
            allow_shell_commands: false,
            allow_file_writes: false,
            max_read_bytes: DEFAULT_LOCAL_MAX_READ_BYTES,
            max_search_bytes_per_file: DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE,
            max_command_output_bytes: DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES,
            max_patch_bytes: DEFAULT_LOCAL_MAX_PATCH_BYTES,
            max_list_entries: DEFAULT_LOCAL_MAX_LIST_ENTRIES,
            max_search_matches: DEFAULT_LOCAL_MAX_SEARCH_MATCHES,
        }
    }

    pub fn allowing_shell_commands(mut self, allow_shell_commands: bool) -> Self {
        self.allow_shell_commands = allow_shell_commands;
        self
    }

    pub fn allowing_file_writes(mut self, allow_file_writes: bool) -> Self {
        self.allow_file_writes = allow_file_writes;
        self
    }

    pub fn with_evidence_dir(mut self, evidence_dir: impl Into<PathBuf>) -> Self {
        self.evidence_dir = Some(evidence_dir.into());
        self
    }

    pub fn with_agent_storage(mut self, paths: &AgentStoragePaths, session_id: &SessionId) -> Self {
        self.evidence_dir = Some(paths.evidence_dir_for_session(session_id));
        self
    }

    pub fn with_data_dir_evidence(self, data_dir: &SinomoDataDir, session_id: &SessionId) -> Self {
        self.with_agent_storage(&AgentStoragePaths::from_data_dir(data_dir), session_id)
    }
}

pub fn registry(config: LocalWorkspaceAgentConfig) -> AgentToolRegistry {
    let command_sessions = command_session::CommandSessionStore::new();
    AgentToolRegistry::new()
        .with_tool(read_file::ReadFileTool::new(config.clone()))
        .with_tool(list_files::ListFilesTool::new(config.clone()))
        .with_tool(search::SearchFilesTool::new(config.clone()))
        .with_tool(git_status::GitStatusTool::new(config.clone()))
        .with_tool(git_diff::GitDiffTool::new(config.clone()))
        .with_tool(apply_patch::ApplyPatchTool::new(config.clone()))
        .with_tool(run_command::RunCommandTool::new(config.clone()))
        .with_tool(format::FormatTool::new(config.clone()))
        .with_tool(test::TestTool::new(config.clone()))
        .with_tool(command_session::StartCommandSessionTool::new(
            config.clone(),
            command_sessions.clone(),
        ))
        .with_tool(command_session::InspectCommandSessionTool::new(
            config.clone(),
            command_sessions.clone(),
        ))
        .with_tool(command_session::WriteCommandStdinTool::new(
            config.clone(),
            command_sessions.clone(),
        ))
        .with_tool(command_session::CancelCommandSessionTool::new(
            config.clone(),
            command_sessions,
        ))
        .with_tool(request_user_input::RequestUserInputTool)
}

pub fn default_local_workspace_tool_descriptors() -> Vec<AgentToolDescriptor> {
    vec![
        read_file::descriptor(),
        list_files::descriptor(),
        search::descriptor(),
        git_status::descriptor(),
        git_diff::descriptor(),
        apply_patch::descriptor(),
        run_command::descriptor(),
        format::descriptor(),
        test::descriptor(),
        command_session::start_descriptor(),
        command_session::inspect_descriptor(),
        command_session::write_stdin_descriptor(),
        command_session::cancel_descriptor(),
        request_user_input::descriptor(),
    ]
}

fn spec_for_kind(kind: AgentToolKind) -> AgentToolSpec {
    default_coding_agent_tools()
        .into_iter()
        .find(|spec| spec.kind == kind)
        .expect("default coding tool spec exists")
}

fn local_git(config: &LocalWorkspaceAgentConfig, args: &[&str]) -> AgentToolResult {
    run_local_command(config, "git", args)
}

fn local_run_command_from_call(
    config: &LocalWorkspaceAgentConfig,
    call: &AgentToolCall,
) -> AgentToolResult {
    if !config.allow_shell_commands {
        return AgentToolResult::blocked(
            "run_command is disabled for this local workspace agent",
            serde_json::json!({
                "error": "command_execution_disabled",
            }),
        );
    }
    let Some(command) = input_string(call, "command") else {
        return AgentToolResult::failed(
            "run_command requires a string command",
            serde_json::json!({ "error": "missing_command" }),
        );
    };
    #[cfg(windows)]
    let result = run_local_command(config, "cmd", &["/C", &command]);
    #[cfg(not(windows))]
    let result = run_local_command(config, "sh", &["-lc", &command]);
    result
}

fn local_named_command(
    config: &LocalWorkspaceAgentConfig,
    call: &AgentToolCall,
    tool_name: &str,
    default_command: &str,
) -> AgentToolResult {
    if !config.allow_shell_commands {
        let result = AgentToolResult::blocked(
            format!("{tool_name} is disabled for this local workspace agent"),
            serde_json::json!({
                "error": "command_execution_disabled",
                "tool": tool_name,
            }),
        );
        return if tool_name == "test" {
            result.with_evidence(
                AgentToolEvidence::policy_only(
                    false,
                    "test command blocked by local workspace policy",
                )
                .with_record(
                    AGENT_CODING_EVIDENCE_KIND,
                    &AgentCodingToolEvidence {
                        verification_state: AgentVerificationState::Blocked,
                        ..AgentCodingToolEvidence::default()
                    },
                ),
            )
        } else {
            result
        };
    }
    let command = input_string(call, "command").unwrap_or_else(|| default_command.to_string());
    let call = AgentToolCall {
        tool: "run_command".to_string(),
        input: serde_json::json!({ "command": command }),
        r#async: call.r#async,
        summary: call.summary.clone(),
    };
    let result = local_run_command_from_call(config, &call);
    if tool_name == "test" {
        with_verification_evidence(result)
    } else {
        result
    }
}

fn with_verification_evidence(mut result: AgentToolResult) -> AgentToolResult {
    let status = match result.status {
        AgentToolStatus::Succeeded => AgentVerificationState::Passed,
        AgentToolStatus::Failed => AgentVerificationState::Failed,
        AgentToolStatus::Blocked | AgentToolStatus::Cancelled => AgentVerificationState::Blocked,
    };
    if let Some(evidence) = result.evidence.take() {
        result.evidence = Some(with_coding_evidence(evidence, |coding| {
            coding.verification_state = status;
        }));
    } else {
        result.evidence = Some(
            AgentToolEvidence::policy_only(
                result.status != AgentToolStatus::Blocked,
                "test command produced verification evidence",
            )
            .with_record(
                AGENT_CODING_EVIDENCE_KIND,
                &AgentCodingToolEvidence {
                    verification_state: status,
                    ..AgentCodingToolEvidence::default()
                },
            ),
        );
    }
    result
}

fn with_coding_evidence(
    evidence: AgentToolEvidence,
    update: impl FnOnce(&mut AgentCodingToolEvidence),
) -> AgentToolEvidence {
    let mut coding = coding_evidence(&evidence);
    update(&mut coding);
    evidence.with_record(AGENT_CODING_EVIDENCE_KIND, &coding)
}

fn coding_evidence(evidence: &AgentToolEvidence) -> AgentCodingToolEvidence {
    evidence
        .record(AGENT_CODING_EVIDENCE_KIND)
        .unwrap_or_default()
}

fn run_local_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[&str],
) -> AgentToolResult {
    let (root, output) = match execute_local_command(config, program, args, None) {
        Ok(output) => output,
        Err(result) => return result,
    };
    local_command_result_from_output(
        config,
        root,
        program,
        args,
        &output,
        "run_command",
        "command execution allowed by local workspace policy",
        Vec::new(),
        None,
    )
}

fn execute_local_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[&str],
    stdin: Option<&[u8]>,
) -> Result<(PathBuf, Output), AgentToolResult> {
    let root = match canonical_workspace_root(config) {
        Ok(root) => root,
        Err(detail) => {
            return Err(AgentToolResult::blocked(
                detail.clone(),
                serde_json::json!({ "error": "workspace_root_not_allowed", "detail": detail }),
            ));
        }
    };
    let mut command = Command::new(program);
    command.args(args).current_dir(&root);
    let output = if let Some(stdin) = stdin {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to run {program}: {source}"),
                    serde_json::json!({ "error": "command_spawn_failed", "detail": source.to_string() }),
                )
            })?;
        if let Some(mut child_stdin) = child.stdin.take() {
            child_stdin.write_all(stdin).map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to write stdin for {program}: {source}"),
                    serde_json::json!({ "error": "command_stdin_failed", "detail": source.to_string() }),
                )
            })?;
        }
        child.wait_with_output().map_err(|source| {
            AgentToolResult::failed(
                format!("failed to wait for {program}: {source}"),
                serde_json::json!({ "error": "command_wait_failed", "detail": source.to_string() }),
            )
        })?
    } else {
        command.output().map_err(|source| {
            AgentToolResult::failed(
                format!("failed to run {program}: {source}"),
                serde_json::json!({ "error": "command_spawn_failed", "detail": source.to_string() }),
            )
        })?
    };
    Ok((root, output))
}

fn local_command_result_from_output(
    config: &LocalWorkspaceAgentConfig,
    root: PathBuf,
    program: &str,
    args: &[&str],
    output: &Output,
    evidence_tool_name: &str,
    evidence_reason: &str,
    changed_files: Vec<String>,
    summary_override: Option<String>,
) -> AgentToolResult {
    let stdout = truncate_utf8_lossy(&output.stdout, config.max_command_output_bytes);
    let stderr = truncate_utf8_lossy(&output.stderr, config.max_command_output_bytes);
    let status = output.status.code();
    let succeeded = output.status.success();
    let summary = summary_override
        .unwrap_or_else(|| format!("{program} {} exited with {:?}", args.join(" "), status));
    let payload = serde_json::json!({
        "program": program,
        "args": args,
        "status": status,
        "success": succeeded,
        "stdout": stdout.text,
        "stderr": stderr.text,
        "stdout_truncated": stdout.truncated,
        "stderr_truncated": stderr.truncated,
        "changed_files": changed_files.clone(),
    });
    let full_output_ref = if stdout.truncated || stderr.truncated {
        let payload = serde_json::json!({
            "program": program,
            "args": args,
            "stdout": String::from_utf8_lossy(&output.stdout),
            "stderr": String::from_utf8_lossy(&output.stderr),
        });
        serde_json::to_vec_pretty(&payload)
            .ok()
            .and_then(|bytes| write_local_evidence(config, evidence_tool_name, "json", &bytes))
    } else {
        None
    };
    let mut evidence = AgentToolEvidence::policy_only(true, evidence_reason)
        .with_command(root.display().to_string(), status)
        .with_output_value(&payload, stdout.truncated || stderr.truncated);
    if !changed_files.is_empty() {
        evidence = evidence.with_record(
            AGENT_CODING_EVIDENCE_KIND,
            &AgentCodingToolEvidence {
                changed_files,
                ..AgentCodingToolEvidence::default()
            },
        );
    }
    if let Some(full_output_ref) = full_output_ref {
        evidence = evidence.with_artifact(
            AgentToolArtifactRef::new("full_output", full_output_ref)
                .with_media_type("application/json"),
        );
    }
    if succeeded {
        AgentToolResult::succeeded(summary, payload).with_evidence(evidence)
    } else {
        AgentToolResult::failed(summary, payload).with_evidence(evidence)
    }
}

fn write_local_evidence(
    config: &LocalWorkspaceAgentConfig,
    tool_name: &str,
    extension: &str,
    bytes: &[u8],
) -> Option<String> {
    let evidence_dir = config.evidence_dir.as_ref()?;
    fs::create_dir_all(evidence_dir).ok()?;
    let path = evidence_dir.join(format!(
        "{}-{}.{}",
        tool_name.replace('_', "-"),
        TaskId::generate(),
        extension
    ));
    fs::write(&path, bytes).ok()?;
    Some(path.display().to_string())
}

fn input_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

fn input_raw_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

fn input_bool(call: &AgentToolCall, key: &str) -> Option<bool> {
    call.input.get(key).and_then(serde_json::Value::as_bool)
}

fn input_usize(call: &AgentToolCall, key: &str) -> Option<usize> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn canonical_workspace_root(config: &LocalWorkspaceAgentConfig) -> Result<PathBuf, String> {
    fs::canonicalize(&config.root).map_err(|source| {
        format!(
            "workspace root {} is not accessible: {source}",
            config.root.display()
        )
    })
}

fn resolve_existing_workspace_path(
    config: &LocalWorkspaceAgentConfig,
    requested: &str,
) -> Result<PathBuf, String> {
    let root = canonical_workspace_root(config)?;
    let requested_path = Path::new(requested);
    let joined = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        root.join(requested_path)
    };
    let resolved = fs::canonicalize(&joined)
        .map_err(|source| format!("path {} is not accessible: {source}", joined.display()))?;
    if resolved.starts_with(&root) {
        Ok(resolved)
    } else {
        Err(format!(
            "path {} escapes workspace root {}",
            resolved.display(),
            root.display()
        ))
    }
}

fn display_workspace_path(config: &LocalWorkspaceAgentConfig, path: &Path) -> String {
    canonical_workspace_root(config)
        .ok()
        .and_then(|root| path.strip_prefix(root).ok().map(Path::to_path_buf))
        .unwrap_or_else(|| path.to_path_buf())
        .display()
        .to_string()
}

fn collect_workspace_entries(
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
    recursive: bool,
    max_entries: usize,
    entries: &mut Vec<serde_json::Value>,
) -> Result<bool, String> {
    if entries.len() >= max_entries {
        return Ok(true);
    }
    let mut dir_entries = fs::read_dir(path)
        .map_err(|source| format!("failed to list {}: {source}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| format!("failed to read entry in {}: {source}", path.display()))?;
    dir_entries.sort_by_key(|entry| entry.file_name());
    for entry in dir_entries {
        if entries.len() >= max_entries {
            return Ok(true);
        }
        let file_type = entry
            .file_type()
            .map_err(|source| format!("failed to read type for {:?}: {source}", entry.path()))?;
        let kind = if file_type.is_dir() {
            "directory"
        } else if file_type.is_file() {
            "file"
        } else if file_type.is_symlink() {
            "symlink"
        } else {
            "other"
        };
        let path = entry.path();
        entries.push(serde_json::json!({
            "path": display_workspace_path(config, &path),
            "kind": kind,
        }));
        if recursive && file_type.is_dir() {
            let truncated =
                collect_workspace_entries(config, &path, recursive, max_entries, entries)?;
            if truncated {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn collect_workspace_search_matches(
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
    query: &str,
    case_sensitive: bool,
    max_matches: usize,
    matches: &mut Vec<serde_json::Value>,
) -> Result<bool, String> {
    if matches.len() >= max_matches {
        return Ok(true);
    }
    let metadata = fs::metadata(path)
        .map_err(|source| format!("failed to stat {}: {source}", path.display()))?;
    if metadata.is_dir() {
        let mut dir_entries = fs::read_dir(path)
            .map_err(|source| format!("failed to list {}: {source}", path.display()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| format!("failed to read entry in {}: {source}", path.display()))?;
        dir_entries.sort_by_key(|entry| entry.file_name());
        for entry in dir_entries {
            let truncated = collect_workspace_search_matches(
                config,
                &entry.path(),
                query,
                case_sensitive,
                max_matches,
                matches,
            )?;
            if truncated {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if !metadata.is_file() || metadata.len() > config.max_search_bytes_per_file {
        return Ok(false);
    }
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(_) => return Ok(false),
    };
    let query_cmp = if case_sensitive {
        query.to_string()
    } else {
        query.to_ascii_lowercase()
    };
    for (line_index, line) in content.lines().enumerate() {
        let haystack = if case_sensitive {
            line.to_string()
        } else {
            line.to_ascii_lowercase()
        };
        if haystack.contains(&query_cmp) {
            matches.push(serde_json::json!({
                "path": display_workspace_path(config, path),
                "line": line_index + 1,
                "text": line,
            }));
            if matches.len() >= max_matches {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct TruncatedText {
    text: String,
    truncated: bool,
}

fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> TruncatedText {
    let truncated = bytes.len() > max_bytes;
    TruncatedText {
        text: String::from_utf8_lossy(&bytes[..bytes.len().min(max_bytes)]).to_string(),
        truncated,
    }
}
