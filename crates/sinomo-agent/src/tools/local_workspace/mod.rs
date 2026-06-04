mod apply_patch;
mod changed_files;
mod command_session;
mod git_diff;
mod read_file;
mod request_user_input;
mod run_command;
mod search;

use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentCommandOutput, AgentTool,
    AgentToolArtifactRef, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolProviderSpec, AgentToolRegistry, AgentToolResult, AgentToolStatus,
};
use crate::{
    AgentStoragePaths, AgentToolCall, AgentToolKind, AgentToolSpec, TaskId,
    default_coding_agent_tools,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const DEFAULT_LOCAL_MAX_READ_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE: u64 = 512 * 1024;
const DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_PATCH_BYTES: usize = 512 * 1024;
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
        .with_tool(search::SearchFilesTool::new(config.clone()))
        .with_tool(apply_patch::ApplyPatchTool::new(config.clone()))
        .with_tool(changed_files::ChangedFilesTool::new(config.clone()))
        .with_tool(git_diff::GitDiffTool::new(config.clone()))
        .with_tool(run_command::RunCommandTool::new(config.clone()))
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
        search::descriptor(),
        apply_patch::descriptor(),
        changed_files::descriptor(),
        git_diff::descriptor(),
        run_command::descriptor(),
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LocalCommandOptions {
    cwd: Option<String>,
    timeout_ms: Option<u64>,
    evidence_tool_name: String,
    evidence_reason: String,
    changed_files: Vec<String>,
    summary_override: Option<String>,
}

fn local_run_command_from_call(
    config: &LocalWorkspaceAgentConfig,
    call: &AgentToolCall,
) -> AgentToolResult {
    local_shell_command_from_call(
        config,
        call,
        "run_command",
        None,
        "command execution allowed by local workspace policy",
    )
}

fn local_shell_command_from_call(
    config: &LocalWorkspaceAgentConfig,
    call: &AgentToolCall,
    tool_name: &str,
    default_command: Option<&str>,
    evidence_reason: &str,
) -> AgentToolResult {
    if !config.allow_shell_commands {
        return AgentToolResult::blocked(
            format!("{tool_name} is disabled for this local workspace agent"),
            serde_json::json!({
                "error": "command_execution_disabled",
                "tool": tool_name,
            }),
        );
    }
    let command =
        match input_string(call, "command").or_else(|| default_command.map(str::to_string)) {
            Some(command) => command,
            None => {
                return AgentToolResult::failed(
                    format!("{tool_name} requires a string command"),
                    serde_json::json!({ "error": "missing_command", "tool": tool_name }),
                );
            }
        };
    #[cfg(windows)]
    let (program, args): (&str, Vec<String>) = ("cmd", vec!["/C".to_string(), command.clone()]);
    #[cfg(not(windows))]
    let (program, args): (&str, Vec<String>) = local_shell_program_and_args(command.clone());
    run_local_command_with_options(
        config,
        program,
        args,
        None,
        LocalCommandOptions {
            cwd: input_string(call, "cwd"),
            timeout_ms: input_timeout_ms(call),
            evidence_tool_name: tool_name.to_string(),
            evidence_reason: evidence_reason.to_string(),
            summary_override: Some(command),
            ..LocalCommandOptions::default()
        },
    )
}

#[cfg(not(windows))]
fn local_shell_program_and_args(command: String) -> (&'static str, Vec<String>) {
    if Path::new("/bin/bash").exists() || Path::new("/usr/bin/bash").exists() {
        (
            "bash",
            vec![
                "-o".to_string(),
                "pipefail".to_string(),
                "-lc".to_string(),
                command,
            ],
        )
    } else {
        ("sh", vec!["-lc".to_string(), command])
    }
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

fn run_local_command_with_options(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: Vec<String>,
    stdin: Option<&[u8]>,
    options: LocalCommandOptions,
) -> AgentToolResult {
    let execution = match execute_local_command(
        config,
        program,
        &args,
        stdin,
        options.timeout_ms,
        options.cwd.as_deref(),
    ) {
        Ok(output) => output,
        Err(result) => return result,
    };
    local_command_result_from_output(
        config,
        execution.root,
        execution.cwd,
        program,
        &args,
        &execution.output,
        execution.timed_out,
        execution.duration_ms,
        options.timeout_ms,
        &options.evidence_tool_name,
        &options.evidence_reason,
        options.changed_files,
        options.summary_override,
    )
}

struct LocalCommandExecution {
    root: PathBuf,
    cwd: PathBuf,
    output: Output,
    timed_out: bool,
    duration_ms: u64,
}

fn execute_local_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
) -> Result<LocalCommandExecution, AgentToolResult> {
    let (root, cwd) = match resolve_workspace_cwd(config, cwd) {
        Ok(paths) => paths,
        Err(detail) => {
            return Err(AgentToolResult::blocked(
                detail.clone(),
                serde_json::json!({ "error": "workspace_root_not_allowed", "detail": detail }),
            ));
        }
    };
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&cwd)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_command_process_group(&mut command);
    let mut child = command.spawn().map_err(|source| {
        AgentToolResult::failed(
            format!("failed to run {program}: {source}"),
            serde_json::json!({ "error": "command_spawn_failed", "detail": source.to_string() }),
        )
    })?;
    if let Some(stdin) = stdin {
        if let Some(mut child_stdin) = child.stdin.take() {
            child_stdin.write_all(stdin).map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to write stdin for {program}: {source}"),
                    serde_json::json!({ "error": "command_stdin_failed", "detail": source.to_string() }),
                )
            })?;
        }
    }
    let started = Instant::now();
    let timed_out = if let Some(timeout_ms) = timeout_ms {
        wait_for_child_or_timeout(&mut child, timeout_ms).map_err(|source| {
            AgentToolResult::failed(
                format!("failed to wait for {program}: {source}"),
                serde_json::json!({ "error": "command_wait_failed", "detail": source.to_string() }),
            )
        })?
    } else {
        false
    };
    let output = child.wait_with_output().map_err(|source| {
        AgentToolResult::failed(
            format!("failed to wait for {program}: {source}"),
            serde_json::json!({ "error": "command_wait_failed", "detail": source.to_string() }),
        )
    })?;
    Ok(LocalCommandExecution {
        root,
        cwd,
        output,
        timed_out,
        duration_ms: duration_ms_from_elapsed(started.elapsed()),
    })
}

fn wait_for_child_or_timeout(
    child: &mut std::process::Child,
    timeout_ms: u64,
) -> Result<bool, std::io::Error> {
    let timeout = Duration::from_millis(timeout_ms);
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(false);
        }
        if started.elapsed() >= timeout {
            let _ = terminate_child_process_tree(child);
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn duration_ms_from_elapsed(elapsed: Duration) -> u64 {
    elapsed.as_millis().min(u128::from(u64::MAX)) as u64
}

fn aggregate_command_output(stdout: &str, stderr: &str) -> String {
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout.to_string(),
        (true, false) => stderr.to_string(),
        (false, false) => format!("stdout:\n{stdout}\nstderr:\n{stderr}"),
    }
}

#[cfg(unix)]
fn configure_command_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if set_process_group(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_command_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_child_process_tree(child: &mut Child) -> bool {
    let target_process_group = -(child.id() as i32);
    if send_signal(target_process_group, 9) == 0 {
        true
    } else {
        child.kill().is_ok()
    }
}

#[cfg(windows)]
fn terminate_child_process_tree(child: &mut Child) -> bool {
    let pid = child.id().to_string();
    let killed_tree = Command::new("taskkill")
        .args(["/PID", pid.as_str(), "/T", "/F"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    killed_tree || child.kill().is_ok()
}

#[cfg(not(any(unix, windows)))]
fn terminate_child_process_tree(child: &mut Child) -> bool {
    child.kill().is_ok()
}

#[cfg(unix)]
fn send_signal(pid: i32, signal: i32) -> i32 {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe { kill(pid, signal) }
}

#[cfg(unix)]
fn set_process_group(pid: i32, pgid: i32) -> i32 {
    unsafe extern "C" {
        fn setpgid(pid: i32, pgid: i32) -> i32;
    }
    unsafe { setpgid(pid, pgid) }
}

fn local_command_result_from_output(
    config: &LocalWorkspaceAgentConfig,
    root: PathBuf,
    cwd: PathBuf,
    program: &str,
    args: &[String],
    output: &Output,
    timed_out: bool,
    duration_ms: u64,
    timeout_ms: Option<u64>,
    evidence_tool_name: &str,
    evidence_reason: &str,
    changed_files: Vec<String>,
    summary_override: Option<String>,
) -> AgentToolResult {
    let stdout = truncate_utf8_lossy(&output.stdout, config.max_command_output_bytes);
    let stderr = truncate_utf8_lossy(&output.stderr, config.max_command_output_bytes);
    let status = output.status.code();
    let succeeded = !timed_out && output.status.success();
    let cwd_display = display_workspace_path(config, &cwd);
    let stdout_ref = if stdout.truncated {
        write_local_evidence(config, evidence_tool_name, "stdout.txt", &output.stdout).map(|uri| {
            AgentToolArtifactRef::new("stdout", uri)
                .with_label("complete stdout")
                .with_media_type("text/plain; charset=utf-8")
        })
    } else {
        None
    };
    let stderr_ref = if stderr.truncated {
        write_local_evidence(config, evidence_tool_name, "stderr.txt", &output.stderr).map(|uri| {
            AgentToolArtifactRef::new("stderr", uri)
                .with_label("complete stderr")
                .with_media_type("text/plain; charset=utf-8")
        })
    } else {
        None
    };
    let aggregated_output = aggregate_command_output(&stdout.text, &stderr.text);
    let command_output = AgentCommandOutput {
        exit_code: status,
        stdout: stdout.text.clone(),
        stderr: stderr.text.clone(),
        stdout_ref: stdout_ref.clone(),
        stderr_ref: stderr_ref.clone(),
        aggregated_output: aggregated_output.clone(),
        duration_ms,
        timed_out,
        truncated: stdout.truncated || stderr.truncated,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    };
    let summary = if timed_out {
        format!(
            "{program} {} timed out after {} ms",
            args.join(" "),
            timeout_ms.unwrap_or_default()
        )
    } else {
        summary_override
            .map(|summary| {
                if succeeded {
                    summary
                } else {
                    format!("{summary} failed with exit status {:?}", status)
                }
            })
            .unwrap_or_else(|| format!("{program} {} exited with {:?}", args.join(" "), status))
    };
    let payload = serde_json::json!({
        "program": program,
        "args": args,
        "cwd": cwd_display,
        "status": status,
        "success": succeeded,
        "timed_out": timed_out,
        "duration_ms": duration_ms,
        "timeout_ms": timeout_ms,
        "stdout": stdout.text,
        "stderr": stderr.text,
        "stdout_ref": stdout_ref,
        "stderr_ref": stderr_ref,
        "aggregated_output": aggregated_output,
        "command_output": command_output,
        "stdout_truncated": stdout.truncated,
        "stderr_truncated": stderr.truncated,
        "changed_files": changed_files.clone(),
    });
    let full_output_ref = if stdout.truncated || stderr.truncated {
        let payload = serde_json::json!({
            "program": program,
            "args": args,
            "cwd": cwd_display,
            "duration_ms": duration_ms,
            "timed_out": timed_out,
            "timeout_ms": timeout_ms,
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
        .with_command(cwd.display().to_string(), status)
        .with_output_value(&payload, stdout.truncated || stderr.truncated)
        .with_metadata(serde_json::json!({
            "workspace_root": root.display().to_string(),
            "timed_out": timed_out,
            "timeout_ms": timeout_ms,
        }));
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
    if let Some(stdout_ref) = command_output.stdout_ref.clone() {
        evidence = evidence.with_artifact(stdout_ref);
    }
    if let Some(stderr_ref) = command_output.stderr_ref.clone() {
        evidence = evidence.with_artifact(stderr_ref);
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

fn input_timeout_ms(call: &AgentToolCall) -> Option<u64> {
    call.input
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
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

fn resolve_workspace_cwd(
    config: &LocalWorkspaceAgentConfig,
    requested: Option<&str>,
) -> Result<(PathBuf, PathBuf), String> {
    let root = canonical_workspace_root(config)?;
    let Some(requested) = requested else {
        return Ok((root.clone(), root));
    };
    let requested_path = Path::new(requested);
    let joined = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        root.join(requested_path)
    };
    let resolved = fs::canonicalize(&joined)
        .map_err(|source| format!("cwd {} is not accessible: {source}", joined.display()))?;
    if !resolved.starts_with(&root) {
        return Err(format!(
            "cwd {} escapes workspace root {}",
            resolved.display(),
            root.display()
        ));
    }
    if !resolved.is_dir() {
        return Err(format!("cwd {} is not a directory", resolved.display()));
    }
    Ok((root, resolved))
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
    if let Ok(root) = canonical_workspace_root(config) {
        if let Ok(relative) = path.strip_prefix(root) {
            if relative.as_os_str().is_empty() {
                return ".".to_string();
            }
            return relative.display().to_string();
        }
    }
    path.display().to_string()
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
    consumed_bytes: usize,
}

fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> TruncatedText {
    let truncated = bytes.len() > max_bytes;
    let consumed_bytes = bytes.len().min(max_bytes);
    TruncatedText {
        text: String::from_utf8_lossy(&bytes[..consumed_bytes]).to_string(),
        truncated,
        consumed_bytes,
    }
}
