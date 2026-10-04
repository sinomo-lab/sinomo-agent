mod output;
pub(crate) use output::AgentProcessOutput;

use crate::{AgentCancellationToken, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub struct AgentProcessManager {
    processes: Arc<Mutex<HashMap<String, Arc<AgentProcessState>>>>,
    output_directory: Option<PathBuf>,
}

impl AgentProcessManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_output_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.output_directory = Some(directory.into());
        self
    }

    pub(crate) fn captured_output(
        &self,
        process_id: &str,
    ) -> Result<AgentProcessOutput, AgentProcessError> {
        Ok(self.process(process_id)?.output.clone())
    }

    pub fn start(
        &self,
        spec: AgentProcessStart,
        max_output_bytes: usize,
    ) -> Result<AgentProcessSnapshot, AgentProcessError> {
        let process_id = TaskId::generate().to_string();
        let output_directory =
            match &self.output_directory {
                Some(directory) => directory.clone(),
                None => {
                    let data_dir = sinomo_runtime::local::SinomoDataDir::from_env_or_home()
                        .map_err(|error| AgentProcessError::Spawn {
                            command: spec.command.clone(),
                            detail: error.to_string(),
                        })?;
                    crate::AgentStoragePaths::from_data_dir(&data_dir)
                        .evidence_dir
                        .join("process-capture")
                }
            };
        let (output, stdout, stderr) = AgentProcessOutput::create(&output_directory, &process_id)
            .map_err(|error| AgentProcessError::Spawn {
            command: spec.command.clone(),
            detail: format!("create process output capture: {error}"),
        })?;
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .current_dir(&spec.cwd)
            .stdin(if spec.stdin_piped {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        configure_process_group(&mut command);
        let mut child = command.spawn().map_err(|source| AgentProcessError::Spawn {
            command: spec.command.clone(),
            detail: source.to_string(),
        })?;
        let stdin = child.stdin.take();
        let process = Arc::new(AgentProcessState {
            process_id: process_id.clone(),
            command: spec.command,
            program: spec.program,
            args: spec.args,
            cwd: spec.cwd,
            started: Instant::now(),
            timeout_ms: spec.timeout_ms,
            cancellation_token: spec.cancellation_token,
            child: Mutex::new(Some(child)),
            stdin: Mutex::new(stdin),
            output,
            status: Mutex::new(AgentProcessStatus::running()),
        });
        spawn_process_waiter(process.clone());
        self.processes
            .lock()
            .expect("agent process manager lock poisoned")
            .insert(process_id, process.clone());
        process.snapshot(0, 0, max_output_bytes)
    }

    pub fn snapshot(
        &self,
        process_id: &str,
        stdout_offset: usize,
        stderr_offset: usize,
        max_output_bytes: usize,
    ) -> Result<AgentProcessSnapshot, AgentProcessError> {
        let process = self.process(process_id)?;
        process.snapshot(stdout_offset, stderr_offset, max_output_bytes)
    }

    pub fn write_stdin(
        &self,
        process_id: &str,
        input: &[u8],
        close: bool,
    ) -> Result<AgentProcessStdinWrite, AgentProcessError> {
        let process = self.process(process_id)?;
        let mut stdin = process
            .stdin
            .lock()
            .expect("agent process stdin lock poisoned");
        let mut written = 0usize;
        if !input.is_empty() {
            let Some(writer) = stdin.as_mut() else {
                return Err(AgentProcessError::StdinClosed {
                    process_id: process_id.to_string(),
                });
            };
            writer
                .write_all(input)
                .map_err(|source| AgentProcessError::StdinWrite {
                    process_id: process_id.to_string(),
                    detail: source.to_string(),
                })?;
            written = input.len();
        }
        if close {
            stdin.take();
        }
        Ok(AgentProcessStdinWrite {
            process_id: process_id.to_string(),
            bytes_written: written,
            stdin_closed: close,
        })
    }

    pub fn cancel(&self, process_id: &str) -> Result<AgentProcessCancellation, AgentProcessError> {
        let process = self.process(process_id)?;
        process.cancellation_token.cancel();
        {
            let mut status = process
                .status
                .lock()
                .expect("agent process status lock poisoned");
            // The process stays `Running` until it has actually exited; the
            // waiter then records it as cancelled.
            status.cancel_requested = true;
        }
        let killed = process
            .child
            .lock()
            .expect("agent process child lock poisoned")
            .as_mut()
            .map(terminate_process_tree)
            .unwrap_or(false);
        Ok(AgentProcessCancellation {
            process_id: process_id.to_string(),
            kill_signal_sent: killed,
        })
    }

    fn process(&self, process_id: &str) -> Result<Arc<AgentProcessState>, AgentProcessError> {
        self.processes
            .lock()
            .expect("agent process manager lock poisoned")
            .get(process_id)
            .cloned()
            .ok_or_else(|| AgentProcessError::UnknownProcess {
                process_id: process_id.to_string(),
            })
    }
}

#[derive(Clone, Debug)]
pub struct AgentProcessStart {
    pub command: String,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub stdin_piped: bool,
    pub timeout_ms: Option<u64>,
    pub cancellation_token: AgentCancellationToken,
}

impl AgentProcessStart {
    pub fn new(
        command: impl Into<String>,
        program: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
        cwd: impl Into<PathBuf>,
    ) -> Self {
        Self {
            command: command.into(),
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            cwd: cwd.into(),
            stdin_piped: false,
            timeout_ms: None,
            cancellation_token: AgentCancellationToken::new(),
        }
    }

    pub fn with_stdin_piped(mut self, stdin_piped: bool) -> Self {
        self.stdin_piped = stdin_piped;
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_cancellation_token(mut self, cancellation_token: AgentCancellationToken) -> Self {
        self.cancellation_token = cancellation_token;
        self
    }
}

#[derive(Debug)]
struct AgentProcessState {
    process_id: String,
    command: String,
    program: String,
    args: Vec<String>,
    cwd: PathBuf,
    started: Instant,
    timeout_ms: Option<u64>,
    cancellation_token: AgentCancellationToken,
    child: Mutex<Option<Child>>,
    stdin: Mutex<Option<ChildStdin>>,
    output: AgentProcessOutput,
    status: Mutex<AgentProcessStatus>,
}

impl AgentProcessState {
    fn snapshot(
        &self,
        stdout_offset: usize,
        stderr_offset: usize,
        max_output_bytes: usize,
    ) -> Result<AgentProcessSnapshot, AgentProcessError> {
        let status = self
            .status
            .lock()
            .expect("agent process status lock poisoned")
            .clone();
        let read = |stderr, offset| {
            self.output
                .window(stderr, offset, max_output_bytes)
                .map_err(|error| AgentProcessError::Output {
                    process_id: self.process_id.clone(),
                    detail: error.to_string(),
                })
        };
        let stdout = read(false, stdout_offset)?;
        let stderr = read(true, stderr_offset)?;
        Ok(AgentProcessSnapshot {
            process_id: self.process_id.clone(),
            command: self.command.clone(),
            program: self.program.clone(),
            args: self.args.clone(),
            cwd: self.cwd.display().to_string(),
            status: status.lifecycle,
            exit_code: status.exit_code,
            timed_out: status.timed_out,
            cancel_requested: status.cancel_requested,
            error: status.error,
            elapsed_ms: self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            timeout_ms: self.timeout_ms,
            stdout: String::from_utf8_lossy(&stdout.bytes).into_owned(),
            stderr: String::from_utf8_lossy(&stderr.bytes).into_owned(),
            stdout_offset: stdout.offset,
            stderr_offset: stderr.offset,
            stdout_total_bytes: stdout.total_bytes,
            stderr_total_bytes: stderr.total_bytes,
            stdout_truncated: stdout.offset < stdout.total_bytes,
            stderr_truncated: stderr.offset < stderr.total_bytes,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentProcessSnapshot {
    pub process_id: String,
    pub command: String,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub status: AgentProcessLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub cancel_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub elapsed_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_offset: usize,
    pub stderr_offset: usize,
    pub stdout_total_bytes: usize,
    pub stderr_total_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentProcessLifecycleStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl AgentProcessLifecycleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgentProcessStatus {
    lifecycle: AgentProcessLifecycleStatus,
    exit_code: Option<i32>,
    timed_out: bool,
    cancel_requested: bool,
    error: Option<String>,
}

impl AgentProcessStatus {
    fn running() -> Self {
        Self {
            lifecycle: AgentProcessLifecycleStatus::Running,
            exit_code: None,
            timed_out: false,
            cancel_requested: false,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentProcessStdinWrite {
    pub process_id: String,
    pub bytes_written: usize,
    pub stdin_closed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentProcessCancellation {
    pub process_id: String,
    pub kill_signal_sent: bool,
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum AgentProcessError {
    #[error("failed to read output for agent process `{process_id}`: {detail}")]
    Output { process_id: String, detail: String },
    #[error("unknown agent process `{process_id}`")]
    UnknownProcess { process_id: String },
    #[error("failed to spawn `{command}`: {detail}")]
    Spawn { command: String, detail: String },
    #[error("stdin is closed for agent process `{process_id}`")]
    StdinClosed { process_id: String },
    #[error("failed to write stdin for agent process `{process_id}`: {detail}")]
    StdinWrite { process_id: String, detail: String },
}

/// How often a process that has been told to stop is told again while it
/// has not exited yet.
const PROCESS_TERMINATION_RETRY: Duration = Duration::from_millis(500);

fn spawn_process_waiter(process: Arc<AgentProcessState>) {
    thread::spawn(move || {
        let mut last_termination: Option<Instant> = None;
        let mut should_terminate = |now: Instant| {
            let due = last_termination
                .is_none_or(|last| now.duration_since(last) >= PROCESS_TERMINATION_RETRY);
            if due {
                last_termination = Some(now);
            }
            due
        };
        loop {
            let exit = {
                let mut child = process
                    .child
                    .lock()
                    .expect("agent process child lock poisoned");
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
                let mut child = process
                    .child
                    .lock()
                    .expect("agent process child lock poisoned");
                let _ = child.take();
                let mut status = process
                    .status
                    .lock()
                    .expect("agent process status lock poisoned");
                match exit {
                    Ok(exit_status) => {
                        status.exit_code = exit_status.code();
                        if status.cancel_requested {
                            status.lifecycle = AgentProcessLifecycleStatus::Cancelled;
                        } else if status.timed_out {
                            status.lifecycle = AgentProcessLifecycleStatus::Failed;
                        } else if exit_status.success() {
                            status.lifecycle = AgentProcessLifecycleStatus::Completed;
                        } else {
                            status.lifecycle = AgentProcessLifecycleStatus::Failed;
                        }
                    }
                    Err(detail) => {
                        status.lifecycle = AgentProcessLifecycleStatus::Failed;
                        status.error = Some(detail);
                    }
                }
                return;
            }
            if process.cancellation_token.is_cancelled() {
                // Stay `Running` until the exit is observed above, so callers
                // never see a cancelled process that is still alive.
                process
                    .status
                    .lock()
                    .expect("agent process status lock poisoned")
                    .cancel_requested = true;
                if should_terminate(Instant::now())
                    && let Some(child) = process
                        .child
                        .lock()
                        .expect("agent process child lock poisoned")
                        .as_mut()
                {
                    let _ = terminate_process_tree(child);
                }
            } else if process.timeout_ms.is_some_and(|timeout_ms| {
                process.started.elapsed() >= Duration::from_millis(timeout_ms)
            }) {
                {
                    let mut status = process
                        .status
                        .lock()
                        .expect("agent process status lock poisoned");
                    if !status.timed_out {
                        status.timed_out = true;
                        status.error = Some(format!(
                            "command timed out after {} ms",
                            process.timeout_ms.unwrap_or_default()
                        ));
                    }
                }
                if should_terminate(Instant::now())
                    && let Some(child) = process
                        .child
                        .lock()
                        .expect("agent process child lock poisoned")
                        .as_mut()
                {
                    let _ = terminate_process_tree(child);
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
}

fn configure_process_group(command: &mut Command) {
    configure_command_process_group(command);
}

pub(crate) fn configure_command_process_group(command: &mut Command) {
    configure_command_process_group_impl(command);
}

pub(crate) fn terminate_process_tree(child: &mut Child) -> bool {
    terminate_process_tree_impl(child)
}

#[cfg(unix)]
fn configure_command_process_group_impl(command: &mut Command) {
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
fn configure_command_process_group_impl(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_tree_impl(child: &mut Child) -> bool {
    let target_process_group = -(child.id() as i32);
    if send_signal(target_process_group, 9) == 0 {
        true
    } else {
        child.kill().is_ok()
    }
}

#[cfg(windows)]
fn terminate_process_tree_impl(child: &mut Child) -> bool {
    let pid = child.id().to_string();
    // Resolve taskkill from the system directory, not from PATH.
    let taskkill = std::env::var_os("SystemRoot")
        .map(|root| {
            std::path::PathBuf::from(root)
                .join("System32")
                .join("taskkill.exe")
        })
        .unwrap_or_else(|| std::path::PathBuf::from("taskkill.exe"));
    let killed_tree = Command::new(taskkill)
        .args(["/PID", pid.as_str(), "/T", "/F"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    killed_tree || child.kill().is_ok()
}

#[cfg(not(any(unix, windows)))]
fn terminate_process_tree_impl(child: &mut Child) -> bool {
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

#[cfg(all(test, not(windows)))]
mod tests {
    use super::*;

    #[test]
    fn process_manager_streams_stdin_output_and_cancels() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-process-manager");
        let manager = AgentProcessManager::new().with_output_directory(root.join("capture"));
        let initial = manager
            .start(
                AgentProcessStart::new(
                    "interactive process",
                    "sh",
                    [
                        "-lc",
                        "read line; echo stdout:$line; echo stderr:$line 1>&2; sleep 5",
                    ],
                    &root,
                )
                .with_stdin_piped(true)
                .with_timeout_ms(Some(10_000)),
                1024,
            )
            .expect("start process");

        let write = manager
            .write_stdin(&initial.process_id, b"hello\n", true)
            .expect("write stdin");
        assert_eq!(write.bytes_written, 6);

        let mut observed = None;
        for _ in 0..100 {
            let snapshot = manager
                .snapshot(&initial.process_id, 0, 0, 1024)
                .expect("snapshot");
            if snapshot.stdout.contains("stdout:hello") && snapshot.stderr.contains("stderr:hello")
            {
                observed = Some(snapshot);
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let observed = observed.expect("streamed output");
        assert_eq!(observed.status, AgentProcessLifecycleStatus::Running);

        let cancellation = manager.cancel(&initial.process_id).expect("cancel process");
        assert_eq!(cancellation.process_id, initial.process_id);

        let mut cancelled = None;
        for _ in 0..100 {
            let snapshot = manager
                .snapshot(&initial.process_id, 0, 0, 1024)
                .expect("snapshot");
            if snapshot.status == AgentProcessLifecycleStatus::Cancelled {
                cancelled = Some(snapshot);
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let cancelled = cancelled.expect("cancelled process");
        assert!(cancelled.cancel_requested);
        assert!(!cancelled.timed_out);
    }
}
