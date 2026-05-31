use super::*;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_COMMAND_SESSION_TIMEOUT_MS: u64 = 5 * 60 * 1000;

#[derive(Clone, Default)]
pub(super) struct CommandSessionStore {
    sessions: Arc<Mutex<HashMap<String, Arc<CommandSessionState>>>>,
}

impl CommandSessionStore {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn insert(&self, session: Arc<CommandSessionState>) {
        self.sessions
            .lock()
            .expect("command session store lock poisoned")
            .insert(session.session_id.clone(), session);
    }

    fn get(&self, session_id: &str) -> Option<Arc<CommandSessionState>> {
        self.sessions
            .lock()
            .expect("command session store lock poisoned")
            .get(session_id)
            .cloned()
    }
}

#[derive(Debug)]
struct CommandSessionState {
    session_id: String,
    command: String,
    cwd: String,
    started: Instant,
    timeout_ms: u64,
    child: Mutex<Option<Child>>,
    stdin: Mutex<Option<ChildStdin>>,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    status: Mutex<CommandSessionStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommandSessionStatus {
    state: CommandSessionStateKind,
    exit_code: Option<i32>,
    timed_out: bool,
    cancel_requested: bool,
    error: Option<String>,
}

impl CommandSessionStatus {
    fn running() -> Self {
        Self {
            state: CommandSessionStateKind::Running,
            exit_code: None,
            timed_out: false,
            cancel_requested: false,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommandSessionStateKind {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl CommandSessionStateKind {
    fn as_str(&self) -> &'static str {
        match self {
            CommandSessionStateKind::Running => "running",
            CommandSessionStateKind::Completed => "completed",
            CommandSessionStateKind::Failed => "failed",
            CommandSessionStateKind::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone)]
pub(super) struct StartCommandSessionTool {
    config: LocalWorkspaceAgentConfig,
    sessions: CommandSessionStore,
}

impl StartCommandSessionTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig, sessions: CommandSessionStore) -> Self {
        Self { config, sessions }
    }
}

impl AgentTool for StartCommandSessionTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        start_descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        start_command_session(&self.config, &self.sessions, request.call)
    }
}

#[derive(Clone)]
pub(super) struct InspectCommandSessionTool {
    config: LocalWorkspaceAgentConfig,
    sessions: CommandSessionStore,
}

impl InspectCommandSessionTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig, sessions: CommandSessionStore) -> Self {
        Self { config, sessions }
    }
}

impl AgentTool for InspectCommandSessionTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        inspect_descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        inspect_command_session(&self.config, &self.sessions, request.call)
    }
}

#[derive(Clone)]
pub(super) struct WriteCommandStdinTool {
    config: LocalWorkspaceAgentConfig,
    sessions: CommandSessionStore,
}

impl WriteCommandStdinTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig, sessions: CommandSessionStore) -> Self {
        Self { config, sessions }
    }
}

impl AgentTool for WriteCommandStdinTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        write_stdin_descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        write_command_stdin(&self.config, &self.sessions, request.call)
    }
}

#[derive(Clone)]
pub(super) struct CancelCommandSessionTool {
    config: LocalWorkspaceAgentConfig,
    sessions: CommandSessionStore,
}

impl CancelCommandSessionTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig, sessions: CommandSessionStore) -> Self {
        Self { config, sessions }
    }
}

impl AgentTool for CancelCommandSessionTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        cancel_descriptor()
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        cancel_command_session(&self.config, &self.sessions, request.call)
    }
}

pub(super) fn start_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::StartCommandSession))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .supporting_parallel(true)
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": { "type": "string" },
                "timeout_ms": { "type": "integer", "minimum": 1 }
            }
        }))
}

pub(super) fn inspect_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::InspectCommandSession))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .supporting_parallel(true)
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["session_id"],
            "properties": {
                "session_id": { "type": "string" },
                "stdout_offset": { "type": "integer", "minimum": 0 },
                "stderr_offset": { "type": "integer", "minimum": 0 }
            }
        }))
}

pub(super) fn write_stdin_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::WriteCommandStdin))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["session_id"],
            "properties": {
                "session_id": { "type": "string" },
                "input": { "type": "string" },
                "close": { "type": "boolean" }
            }
        }))
}

pub(super) fn cancel_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(spec_for_kind(AgentToolKind::CancelCommandSession))
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["session_id"],
            "properties": {
                "session_id": { "type": "string" }
            }
        }))
}

fn start_command_session(
    config: &LocalWorkspaceAgentConfig,
    sessions: &CommandSessionStore,
    call: &AgentToolCall,
) -> AgentToolResult {
    if !config.allow_shell_commands {
        return AgentToolResult::blocked(
            "start_command_session is disabled for this local workspace agent",
            serde_json::json!({ "error": "command_execution_disabled" }),
        );
    }
    let Some(command_text) = input_string(call, "command") else {
        return AgentToolResult::failed(
            "start_command_session requires a string command",
            serde_json::json!({ "error": "missing_command" }),
        );
    };
    let root = match canonical_workspace_root(config) {
        Ok(root) => root,
        Err(detail) => {
            return AgentToolResult::blocked(
                detail.clone(),
                serde_json::json!({ "error": "workspace_root_not_allowed", "detail": detail }),
            );
        }
    };
    let timeout_ms = call
        .input
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_COMMAND_SESSION_TIMEOUT_MS);

    #[cfg(windows)]
    let (program, args): (&str, Vec<&str>) = ("cmd", vec!["/C", &command_text]);
    #[cfg(not(windows))]
    let (program, args): (&str, Vec<&str>) = ("sh", vec!["-lc", &command_text]);

    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(source) => {
            return AgentToolResult::failed(
                format!("failed to start command session: {source}"),
                serde_json::json!({
                    "error": "command_spawn_failed",
                    "detail": source.to_string(),
                }),
            );
        }
    };
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdin = child.stdin.take();
    let session_id = TaskId::generate().to_string();
    let session = Arc::new(CommandSessionState {
        session_id: session_id.clone(),
        command: command_text,
        cwd: root.display().to_string(),
        started: Instant::now(),
        timeout_ms,
        child: Mutex::new(Some(child)),
        stdin: Mutex::new(stdin),
        stdout: Arc::new(Mutex::new(Vec::new())),
        stderr: Arc::new(Mutex::new(Vec::new())),
        status: Mutex::new(CommandSessionStatus::running()),
    });

    spawn_reader(stdout, session.stdout.clone());
    spawn_reader(stderr, session.stderr.clone());
    spawn_waiter(session.clone());
    sessions.insert(session.clone());

    let payload = command_session_snapshot_payload(&session, 0, 0, config.max_command_output_bytes);
    AgentToolResult::succeeded(
        format!("started command session `{session_id}`"),
        payload.clone(),
    )
    .with_evidence(command_session_evidence(
        true,
        "command session started by local workspace tool",
        &session,
        &payload,
    ))
}

fn inspect_command_session(
    config: &LocalWorkspaceAgentConfig,
    sessions: &CommandSessionStore,
    call: &AgentToolCall,
) -> AgentToolResult {
    let Some(session_id) = input_string(call, "session_id") else {
        return AgentToolResult::failed(
            "inspect_command_session requires a string session_id",
            serde_json::json!({ "error": "missing_session_id" }),
        );
    };
    let Some(session) = sessions.get(&session_id) else {
        return AgentToolResult::failed(
            format!("unknown command session `{session_id}`"),
            serde_json::json!({ "error": "unknown_command_session", "session_id": session_id }),
        );
    };
    let stdout_offset = input_usize_or_zero(call, "stdout_offset");
    let stderr_offset = input_usize_or_zero(call, "stderr_offset");
    let payload = command_session_snapshot_payload(
        &session,
        stdout_offset,
        stderr_offset,
        config.max_command_output_bytes,
    );
    AgentToolResult::succeeded(
        format!("inspected command session `{}`", session.session_id),
        payload.clone(),
    )
    .with_evidence(command_session_evidence(
        true,
        "command session inspected by local workspace tool",
        &session,
        &payload,
    ))
}

fn write_command_stdin(
    _config: &LocalWorkspaceAgentConfig,
    sessions: &CommandSessionStore,
    call: &AgentToolCall,
) -> AgentToolResult {
    let Some(session_id) = input_string(call, "session_id") else {
        return AgentToolResult::failed(
            "write_command_stdin requires a string session_id",
            serde_json::json!({ "error": "missing_session_id" }),
        );
    };
    let Some(session) = sessions.get(&session_id) else {
        return AgentToolResult::failed(
            format!("unknown command session `{session_id}`"),
            serde_json::json!({ "error": "unknown_command_session", "session_id": session_id }),
        );
    };
    let input = input_raw_string(call, "input").unwrap_or_default();
    let close = input_bool(call, "close").unwrap_or(false);
    let mut stdin = session.stdin.lock().expect("command stdin lock poisoned");
    let mut written = 0usize;
    if !input.is_empty() {
        let Some(writer) = stdin.as_mut() else {
            return AgentToolResult::failed(
                format!("stdin is closed for command session `{session_id}`"),
                serde_json::json!({ "error": "stdin_closed", "session_id": session_id }),
            );
        };
        if let Err(source) = writer.write_all(input.as_bytes()) {
            return AgentToolResult::failed(
                format!("failed to write stdin for command session `{session_id}`: {source}"),
                serde_json::json!({
                    "error": "stdin_write_failed",
                    "session_id": session_id,
                    "detail": source.to_string(),
                }),
            );
        }
        written = input.len();
    }
    if close {
        stdin.take();
    }
    let payload = serde_json::json!({
        "session_id": session_id,
        "bytes_written": written,
        "stdin_closed": close,
    });
    AgentToolResult::succeeded(
        format!("wrote {written} byte(s) to command session `{session_id}`"),
        payload.clone(),
    )
    .with_evidence(command_session_evidence(
        true,
        "command session stdin updated by local workspace tool",
        &session,
        &payload,
    ))
}

fn cancel_command_session(
    _config: &LocalWorkspaceAgentConfig,
    sessions: &CommandSessionStore,
    call: &AgentToolCall,
) -> AgentToolResult {
    let Some(session_id) = input_string(call, "session_id") else {
        return AgentToolResult::failed(
            "cancel_command_session requires a string session_id",
            serde_json::json!({ "error": "missing_session_id" }),
        );
    };
    let Some(session) = sessions.get(&session_id) else {
        return AgentToolResult::failed(
            format!("unknown command session `{session_id}`"),
            serde_json::json!({ "error": "unknown_command_session", "session_id": session_id }),
        );
    };
    {
        let mut status = session.status.lock().expect("command status lock poisoned");
        status.cancel_requested = true;
        if status.state == CommandSessionStateKind::Running {
            status.state = CommandSessionStateKind::Cancelled;
        }
    }
    let killed = session
        .child
        .lock()
        .expect("command child lock poisoned")
        .as_mut()
        .map(|child| child.kill().is_ok())
        .unwrap_or(false);
    let payload = serde_json::json!({
        "session_id": session_id,
        "kill_signal_sent": killed,
    });
    AgentToolResult::succeeded(
        format!("cancel requested for command session `{session_id}`"),
        payload.clone(),
    )
    .with_evidence(command_session_evidence(
        true,
        "command session cancellation requested by local workspace tool",
        &session,
        &payload,
    ))
}

fn spawn_reader(pipe: Option<impl Read + Send + 'static>, buffer: Arc<Mutex<Vec<u8>>>) {
    let Some(mut pipe) = pipe else {
        return;
    };
    thread::spawn(move || {
        let mut chunk = [0_u8; 8192];
        loop {
            match pipe.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => {
                    buffer
                        .lock()
                        .expect("command output lock poisoned")
                        .extend_from_slice(&chunk[..count]);
                }
                Err(_) => break,
            }
        }
    });
}

fn spawn_waiter(session: Arc<CommandSessionState>) {
    thread::spawn(move || {
        loop {
            let exit = {
                let mut child = session.child.lock().expect("command child lock poisoned");
                let Some(child) = child.as_mut() else {
                    return;
                };
                match child.try_wait() {
                    Ok(Some(status)) => Some(Ok(status)),
                    Ok(None) => None,
                    Err(source) => Some(Err(source.to_string())),
                }
            };
            if let Some(exit) = exit {
                let mut child = session.child.lock().expect("command child lock poisoned");
                let _ = child.take();
                let mut status = session.status.lock().expect("command status lock poisoned");
                match exit {
                    Ok(exit_status) => {
                        status.exit_code = exit_status.code();
                        if status.cancel_requested {
                            status.state = CommandSessionStateKind::Cancelled;
                        } else if exit_status.success() {
                            status.state = CommandSessionStateKind::Completed;
                        } else {
                            status.state = CommandSessionStateKind::Failed;
                        }
                    }
                    Err(detail) => {
                        status.state = CommandSessionStateKind::Failed;
                        status.error = Some(detail);
                    }
                }
                return;
            }
            if session.timeout_ms > 0
                && session.started.elapsed() >= Duration::from_millis(session.timeout_ms)
            {
                let mut child = session.child.lock().expect("command child lock poisoned");
                if let Some(child) = child.as_mut() {
                    let _ = child.kill();
                }
                let mut status = session.status.lock().expect("command status lock poisoned");
                status.timed_out = true;
                status.state = CommandSessionStateKind::Failed;
                status.error = Some(format!("command timed out after {} ms", session.timeout_ms));
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
}

fn command_session_snapshot_payload(
    session: &CommandSessionState,
    stdout_offset: usize,
    stderr_offset: usize,
    max_output_bytes: usize,
) -> serde_json::Value {
    let status = session
        .status
        .lock()
        .expect("command status lock poisoned")
        .clone();
    let stdout = session.stdout.lock().expect("stdout lock poisoned").clone();
    let stderr = session.stderr.lock().expect("stderr lock poisoned").clone();
    let stdout_offset = stdout_offset.min(stdout.len());
    let stderr_offset = stderr_offset.min(stderr.len());
    let stdout_delta = truncate_utf8_lossy(&stdout[stdout_offset..], max_output_bytes);
    let stderr_delta = truncate_utf8_lossy(&stderr[stderr_offset..], max_output_bytes);
    serde_json::json!({
        "session_id": session.session_id,
        "command": session.command,
        "cwd": session.cwd,
        "status": status.state.as_str(),
        "exit_code": status.exit_code,
        "timed_out": status.timed_out,
        "cancel_requested": status.cancel_requested,
        "error": status.error,
        "elapsed_ms": session.started.elapsed().as_millis(),
        "timeout_ms": session.timeout_ms,
        "stdout": stdout_delta.text,
        "stderr": stderr_delta.text,
        "stdout_offset": stdout.len(),
        "stderr_offset": stderr.len(),
        "stdout_truncated": stdout_delta.truncated,
        "stderr_truncated": stderr_delta.truncated,
    })
}

fn command_session_evidence(
    allowed: bool,
    reason: impl Into<String>,
    session: &CommandSessionState,
    payload: &serde_json::Value,
) -> AgentToolEvidence {
    let status = session
        .status
        .lock()
        .expect("command status lock poisoned")
        .clone();
    AgentToolEvidence::policy_only(allowed, reason)
        .with_command(session.cwd.clone(), status.exit_code)
        .with_output_value(payload, command_session_payload_truncated(payload))
        .with_record_value(
            "command_session",
            serde_json::json!({
                "session_id": session.session_id,
                "command": session.command,
                "status": status.state.as_str(),
                "timed_out": status.timed_out,
                "cancel_requested": status.cancel_requested,
            }),
        )
}

fn command_session_payload_truncated(payload: &serde_json::Value) -> bool {
    payload
        .get("stdout_truncated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || payload
            .get("stderr_truncated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
}

fn input_usize_or_zero(call: &AgentToolCall, key: &str) -> usize {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default()
}
