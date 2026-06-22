mod apply_patch;
mod changed_files;
mod git_diff;
mod list_files;
mod read_file;
mod request_user_input;
mod run_command;
mod search;

use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentCommandOutput, AgentTool,
    AgentToolArtifactRef, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolProgressEvent, AgentToolProgressSink, AgentToolProviderSpec, AgentToolRegistry,
    AgentToolResult, AgentToolStatus,
};
use crate::{
    AgentCancellationToken, AgentProcessLifecycleStatus, AgentProcessManager, AgentProcessSnapshot,
    AgentProcessStart, AgentStoragePaths, AgentToolCall, AgentToolKind, AgentToolSpec, TaskId,
    default_coding_agent_tools,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeId;
use sinomo_runtime::sandbox::{
    LocalSandboxInstanceLayout, LocalSandboxInstanceStore, LocalSandboxToolchainStore,
    SandboxCommandExecutionError, SandboxId, SandboxMount, SandboxMountMode, SandboxPath,
    SandboxPolicy, SandboxStoreError, SandboxToolchainId, SandboxWasiError,
};
use sinomo_runtime::sandbox_simple::{SandboxSimpleNativeCommandRunner, SandboxSimpleNativeConfig};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const DEFAULT_LOCAL_MAX_READ_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE: u64 = 512 * 1024;
const DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_PATCH_BYTES: usize = 512 * 1024;
const DEFAULT_LOCAL_MAX_SEARCH_MATCHES: usize = 100;
const DEFAULT_LOCAL_MAX_LIST_ENTRIES: usize = 200;
const AGENT_SANDBOX_TOOLCHAIN_ID: &str = "agent_tools";
const AGENT_HOME_SIFS_ROOT: &str = "/sinomo/agents";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalWorkspaceAgentConfig {
    pub root: PathBuf,
    pub evidence_dir: Option<PathBuf>,
    pub sifs_evidence: Option<LocalWorkspaceSifsEvidenceConfig>,
    pub command_runtime: LocalWorkspaceCommandRuntime,
    pub sandbox_runtime_root: PathBuf,
    pub agent_home_sifs_path: Option<String>,
    pub agent_home_local_root: Option<PathBuf>,
    pub sandbox_allow_unenforced: bool,
    pub allow_shell_commands: bool,
    pub allow_file_writes: bool,
    pub max_read_bytes: usize,
    pub max_search_bytes_per_file: u64,
    pub max_command_output_bytes: usize,
    pub max_patch_bytes: usize,
    pub max_search_matches: usize,
    pub max_list_entries: usize,
    pub git_program: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalWorkspaceCommandRuntime {
    SimpleSandbox,
    LocalProcess,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalWorkspaceSifsEvidenceConfig {
    pub mount_dir: PathBuf,
    pub sifs_root: String,
}

impl LocalWorkspaceSifsEvidenceConfig {
    pub fn new(mount_dir: impl Into<PathBuf>, sifs_root: impl Into<String>) -> Self {
        Self {
            mount_dir: mount_dir.into(),
            sifs_root: normalize_sifs_evidence_root(sifs_root.into()),
        }
    }

    fn sifs_uri_for_file(&self, file_name: &str) -> String {
        AgentToolArtifactRef::sifs(
            "evidence",
            format!(
                "{}/{}",
                self.sifs_root.trim_end_matches('/'),
                file_name.trim_start_matches('/')
            ),
        )
        .uri
    }
}

impl LocalWorkspaceAgentConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let agent_home_sifs_path = Some(default_agent_home_sifs_path_for_root(&root));
        Self {
            root,
            evidence_dir: None,
            sifs_evidence: None,
            command_runtime: LocalWorkspaceCommandRuntime::SimpleSandbox,
            sandbox_runtime_root: default_agent_sandbox_runtime_root(),
            agent_home_sifs_path,
            agent_home_local_root: None,
            sandbox_allow_unenforced: false,
            allow_shell_commands: false,
            allow_file_writes: false,
            max_read_bytes: DEFAULT_LOCAL_MAX_READ_BYTES,
            max_search_bytes_per_file: DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE,
            max_command_output_bytes: DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES,
            max_patch_bytes: DEFAULT_LOCAL_MAX_PATCH_BYTES,
            max_search_matches: DEFAULT_LOCAL_MAX_SEARCH_MATCHES,
            max_list_entries: DEFAULT_LOCAL_MAX_LIST_ENTRIES,
            git_program: "git".to_string(),
        }
    }

    pub fn with_command_runtime(mut self, command_runtime: LocalWorkspaceCommandRuntime) -> Self {
        self.command_runtime = command_runtime;
        self
    }

    pub fn using_simple_sandbox_commands(self) -> Self {
        self.with_command_runtime(LocalWorkspaceCommandRuntime::SimpleSandbox)
    }

    pub fn using_local_process_commands(self) -> Self {
        self.with_command_runtime(LocalWorkspaceCommandRuntime::LocalProcess)
    }

    pub fn with_sandbox_runtime_root(mut self, sandbox_runtime_root: impl Into<PathBuf>) -> Self {
        self.sandbox_runtime_root = sandbox_runtime_root.into();
        self
    }

    pub fn with_agent_home_sifs_path(mut self, agent_home_sifs_path: impl Into<String>) -> Self {
        self.agent_home_sifs_path =
            Some(normalize_agent_home_sifs_path(agent_home_sifs_path.into()));
        self
    }

    pub fn without_agent_home_sifs_path(mut self) -> Self {
        self.agent_home_sifs_path = None;
        self
    }

    pub fn with_agent_home_local_root(mut self, agent_home_local_root: impl Into<PathBuf>) -> Self {
        self.agent_home_local_root = Some(agent_home_local_root.into());
        self
    }

    pub fn without_agent_home_local_root(mut self) -> Self {
        self.agent_home_local_root = None;
        self
    }

    pub fn allowing_unenforced_simple_sandbox(mut self, allow_unenforced: bool) -> Self {
        self.sandbox_allow_unenforced = allow_unenforced;
        self
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

    pub fn with_sifs_evidence_mount(
        mut self,
        mount_dir: impl Into<PathBuf>,
        sifs_root: impl Into<String>,
    ) -> Self {
        self.sifs_evidence = Some(LocalWorkspaceSifsEvidenceConfig::new(mount_dir, sifs_root));
        self
    }

    pub fn with_git_program(mut self, git_program: impl Into<String>) -> Self {
        self.git_program = git_program.into();
        self
    }

    pub fn with_agent_storage(mut self, paths: &AgentStoragePaths, session_id: &SessionId) -> Self {
        self.evidence_dir = Some(paths.evidence_dir_for_session(session_id));
        self.agent_home_sifs_path = Some(default_agent_home_sifs_path_for_session(session_id));
        self
    }

    pub fn with_data_dir_evidence(
        mut self,
        data_dir: &SinomoDataDir,
        session_id: &SessionId,
    ) -> Self {
        self = self.with_agent_storage(&AgentStoragePaths::from_data_dir(data_dir), session_id);
        self.sandbox_runtime_root = data_dir.root().join("sandbox").join("agent-tools");
        self.agent_home_local_root = Some(data_dir.root().join("agents"));
        self
    }
}

pub fn registry(config: LocalWorkspaceAgentConfig) -> AgentToolRegistry {
    AgentToolRegistry::new()
        .with_tool(read_file::ReadFileTool::new(config.clone()))
        .with_tool(list_files::ListFilesTool::new(config.clone()))
        .with_tool(search::SearchFilesTool::new(config.clone()))
        .with_tool(apply_patch::ApplyPatchTool::new(config.clone()))
        .with_tool(changed_files::ChangedFilesTool::new(config.clone()))
        .with_tool(git_diff::GitDiffTool::new(config.clone()))
        .with_tool(run_command::RunCommandTool::new(config.clone()))
        .with_tool(request_user_input::RequestUserInputTool)
}

pub fn default_local_workspace_tool_descriptors() -> Vec<AgentToolDescriptor> {
    vec![
        read_file::descriptor(),
        list_files::descriptor(),
        search::descriptor(),
        apply_patch::descriptor(),
        changed_files::descriptor(),
        git_diff::descriptor(),
        run_command::descriptor(),
        request_user_input::descriptor(),
    ]
}

fn spec_for_kind(kind: AgentToolKind) -> AgentToolSpec {
    default_coding_agent_tools()
        .into_iter()
        .chain(hidden_local_workspace_tool_specs())
        .find(|spec| spec.kind == kind)
        .expect("default coding tool spec exists")
}

fn hidden_local_workspace_tool_specs() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ChangedFiles,
            "changed_files",
            "List workspace files changed relative to version control, including staged, unstaged, renamed, deleted, and untracked files.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::GitDiff,
            "git_diff",
            "Return a bounded version-control diff summary for the workspace and an evidence ref for the full diff when it is large.",
            false,
        ),
    ]
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
    cancellation_token: AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> AgentToolResult {
    local_shell_command_from_call(
        config,
        call,
        "run_command",
        None,
        "shell command execution enabled for this local workspace",
        cancellation_token,
        progress_sink,
    )
}

fn local_shell_command_from_call(
    config: &LocalWorkspaceAgentConfig,
    call: &AgentToolCall,
    tool_name: &str,
    default_command: Option<&str>,
    evidence_reason: &str,
    cancellation_token: AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
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
    let (program, args): (&str, Vec<String>) = ("cmd.exe", vec!["/C".to_string(), command.clone()]);
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
        cancellation_token,
        progress_sink,
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
    cancellation_token: AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> AgentToolResult {
    let execution = match execute_local_command(
        config,
        program,
        &args,
        stdin,
        options.timeout_ms,
        options.cwd.as_deref(),
        &cancellation_token,
        progress_sink,
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
        execution.cancelled,
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
    output: LocalCommandOutput,
    timed_out: bool,
    cancelled: bool,
    duration_ms: u64,
}

struct LocalCommandOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status_code: Option<i32>,
    success: bool,
}

fn execute_local_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> Result<LocalCommandExecution, AgentToolResult> {
    match config.command_runtime {
        LocalWorkspaceCommandRuntime::SimpleSandbox => execute_simple_sandbox_command(
            config,
            program,
            args,
            stdin,
            timeout_ms,
            cwd,
            cancellation_token,
            progress_sink,
        ),
        LocalWorkspaceCommandRuntime::LocalProcess => execute_local_process_command(
            config,
            program,
            args,
            stdin,
            timeout_ms,
            cwd,
            cancellation_token,
            progress_sink,
        ),
    }
}

fn execute_local_process_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
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
    let command_text = std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    let manager = AgentProcessManager::new();
    let process = manager
        .start(
            AgentProcessStart::new(command_text, program.to_string(), args.iter().cloned(), &cwd)
                .with_stdin_piped(stdin.is_some())
                .with_timeout_ms(timeout_ms)
                .with_cancellation_token(cancellation_token.clone()),
            config.max_command_output_bytes,
        )
        .map_err(|source| {
            AgentToolResult::failed(
                format!("failed to run {program}: {source}"),
                serde_json::json!({ "error": "command_spawn_failed", "detail": source.to_string() }),
            )
        })?;
    progress_sink.emit(AgentToolProgressEvent::ProcessStarted {
        process: process.clone(),
    });
    if let Some(stdin) = stdin {
        manager
            .write_stdin(&process.process_id, stdin, true)
            .map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to write stdin for {program}: {source}"),
                    serde_json::json!({ "error": "command_stdin_failed", "detail": source.to_string() }),
                )
            })?;
    }
    let started = Instant::now();
    let mut stdout_offset = 0usize;
    let mut stderr_offset = 0usize;
    let final_snapshot = loop {
        let snapshot = manager
            .snapshot(
                &process.process_id,
                stdout_offset,
                stderr_offset,
                config.max_command_output_bytes,
            )
            .map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to inspect {program}: {source}"),
                    serde_json::json!({ "error": "command_inspect_failed", "detail": source.to_string() }),
                )
            })?;
        if !snapshot.stdout.is_empty() || !snapshot.stderr.is_empty() {
            progress_sink.emit(AgentToolProgressEvent::ProcessOutput {
                process: snapshot.clone(),
            });
        }
        stdout_offset = snapshot.stdout_offset;
        stderr_offset = snapshot.stderr_offset;
        if snapshot.status != AgentProcessLifecycleStatus::Running {
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                process: snapshot.clone(),
            });
            break snapshot;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let full_snapshot = manager
        .snapshot(&process.process_id, 0, 0, usize::MAX)
        .unwrap_or(final_snapshot);
    let timed_out = full_snapshot.timed_out;
    let cancelled = full_snapshot.cancel_requested
        || full_snapshot.status == AgentProcessLifecycleStatus::Cancelled;
    let success = full_snapshot.status == AgentProcessLifecycleStatus::Completed;
    let status_code = full_snapshot.exit_code;
    let output = LocalCommandOutput {
        stdout: full_snapshot.stdout.into_bytes(),
        stderr: full_snapshot.stderr.into_bytes(),
        status_code,
        success,
    };
    Ok(LocalCommandExecution {
        root,
        cwd,
        output,
        timed_out,
        cancelled,
        duration_ms: duration_ms_from_elapsed(started.elapsed()),
    })
}

fn execute_simple_sandbox_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
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
    let prepared = prepare_sandbox_command(program, args, &cwd)?;
    let sandbox_cwd = sandbox_path_for_host_path(&cwd)?;
    let sandbox_id = agent_sandbox_id(&root);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named(AGENT_SANDBOX_TOOLCHAIN_ID).map_err(|source| {
            AgentToolResult::failed(
                format!("failed to prepare simple sandbox toolchain id: {source}"),
                serde_json::json!({ "error": "sandbox_toolchain_id_invalid", "detail": source.to_string() }),
            )
        })?;
    let store = LocalSandboxInstanceStore::new(&config.sandbox_runtime_root);
    let agent_home_local = agent_home_local_mirror(config);
    let (_manifest, layout) = prepare_agent_sandbox_manifest(
        config,
        &store,
        &sandbox_id,
        &toolchain_id,
        sandbox_cwd,
        &root,
        &cwd,
        &prepared.path_prefixes,
    )?;
    let runner = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
        allow_unenforced: config.sandbox_allow_unenforced,
        extra_read_paths: sandbox_extra_read_paths(
            &root,
            &cwd,
            &prepared.path_prefixes,
            agent_home_local.as_deref(),
        ),
        extra_write_paths: sandbox_extra_write_paths(&root, agent_home_local.as_deref()),
        extra_execute_paths: sandbox_extra_execute_paths(&prepared.path_prefixes),
    });
    let toolchain_store = LocalSandboxToolchainStore::new(layout.toolchain_runtime_cache_root());
    let node_id = NodeId::derived("sinomo-agent-simple-sandbox-node", sandbox_id.as_str());
    let policy = agent_sandbox_policy();
    let command_text = std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    let process_id = TaskId::generate().to_string();
    let started = Instant::now();
    progress_sink.emit(AgentToolProgressEvent::ProcessStarted {
        process: sandbox_process_snapshot(
            &process_id,
            &command_text,
            program,
            args,
            &cwd,
            AgentProcessLifecycleStatus::Running,
            None,
            false,
            cancellation_token.is_cancelled(),
            None,
            started.elapsed(),
            timeout_ms,
            &[],
            &[],
            config.max_command_output_bytes,
        ),
    });
    if cancellation_token.is_cancelled() {
        let output = LocalCommandOutput {
            stdout: Vec::new(),
            stderr: b"command cancelled before sandbox start".to_vec(),
            status_code: None,
            success: false,
        };
        progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
            process: sandbox_process_snapshot(
                &process_id,
                &command_text,
                program,
                args,
                &cwd,
                AgentProcessLifecycleStatus::Cancelled,
                None,
                false,
                true,
                Some("command cancelled before sandbox start".to_string()),
                started.elapsed(),
                timeout_ms,
                &output.stdout,
                &output.stderr,
                config.max_command_output_bytes,
            ),
        });
        return Ok(LocalCommandExecution {
            root,
            cwd,
            output,
            timed_out: false,
            cancelled: true,
            duration_ms: duration_ms_from_elapsed(started.elapsed()),
        });
    }

    let outcome = runner.run_toolchain_command_with_stdin_policy_and_output_sink(
        &store,
        &toolchain_store,
        &sandbox_id,
        &prepared.command_name,
        prepared.argv,
        stdin.map(|bytes| bytes.to_vec()),
        timeout_ms.unwrap_or(0),
        &node_id,
        &policy,
        None,
    );
    match outcome {
        Ok(outcome) => {
            let status_code = Some(outcome.response.exit_status);
            let success = outcome.response.exit_status == 0;
            let status = if success {
                AgentProcessLifecycleStatus::Completed
            } else {
                AgentProcessLifecycleStatus::Failed
            };
            if !outcome.stdout.is_empty() || !outcome.stderr.is_empty() {
                progress_sink.emit(AgentToolProgressEvent::ProcessOutput {
                    process: sandbox_process_snapshot(
                        &process_id,
                        &command_text,
                        program,
                        args,
                        &cwd,
                        status.clone(),
                        status_code,
                        false,
                        false,
                        None,
                        started.elapsed(),
                        timeout_ms,
                        &outcome.stdout,
                        &outcome.stderr,
                        config.max_command_output_bytes,
                    ),
                });
            }
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                process: sandbox_process_snapshot(
                    &process_id,
                    &command_text,
                    program,
                    args,
                    &cwd,
                    status,
                    status_code,
                    false,
                    false,
                    None,
                    started.elapsed(),
                    timeout_ms,
                    &outcome.stdout,
                    &outcome.stderr,
                    config.max_command_output_bytes,
                ),
            });
            Ok(LocalCommandExecution {
                root,
                cwd,
                output: LocalCommandOutput {
                    stdout: outcome.stdout,
                    stderr: outcome.stderr,
                    status_code,
                    success,
                },
                timed_out: false,
                cancelled: false,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            })
        }
        Err(SandboxCommandExecutionError::Wasi(SandboxWasiError::TimedOut { timeout_ms })) => {
            let stderr = format!("sandbox command timed out after {timeout_ms} ms").into_bytes();
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                process: sandbox_process_snapshot(
                    &process_id,
                    &command_text,
                    program,
                    args,
                    &cwd,
                    AgentProcessLifecycleStatus::Failed,
                    None,
                    true,
                    false,
                    Some(format!("command timed out after {timeout_ms} ms")),
                    started.elapsed(),
                    Some(timeout_ms),
                    &[],
                    &stderr,
                    config.max_command_output_bytes,
                ),
            });
            Ok(LocalCommandExecution {
                root,
                cwd,
                output: LocalCommandOutput {
                    stdout: Vec::new(),
                    stderr,
                    status_code: None,
                    success: false,
                },
                timed_out: true,
                cancelled: false,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            })
        }
        Err(SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)) => {
            let stderr = b"sandbox command was cancelled".to_vec();
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                process: sandbox_process_snapshot(
                    &process_id,
                    &command_text,
                    program,
                    args,
                    &cwd,
                    AgentProcessLifecycleStatus::Cancelled,
                    None,
                    false,
                    true,
                    Some("sandbox command was cancelled".to_string()),
                    started.elapsed(),
                    timeout_ms,
                    &[],
                    &stderr,
                    config.max_command_output_bytes,
                ),
            });
            Ok(LocalCommandExecution {
                root,
                cwd,
                output: LocalCommandOutput {
                    stdout: Vec::new(),
                    stderr,
                    status_code: None,
                    success: false,
                },
                timed_out: false,
                cancelled: true,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            })
        }
        Err(source) => Err(AgentToolResult::failed(
            format!("failed to run {program} in simple sandbox: {source}"),
            serde_json::json!({
                "error": "simple_sandbox_command_failed",
                "detail": source.to_string(),
                "runtime": "simple_sandbox",
            }),
        )),
    }
}

struct PreparedSandboxCommand {
    command_name: String,
    argv: Vec<String>,
    path_prefixes: Vec<PathBuf>,
}

fn prepare_sandbox_command(
    program: &str,
    args: &[String],
    cwd: &Path,
) -> Result<PreparedSandboxCommand, AgentToolResult> {
    let program_uses_path =
        program.contains('/') || program.contains('\\') || Path::new(program).is_absolute();
    if !program_uses_path {
        return Ok(PreparedSandboxCommand {
            command_name: program.to_string(),
            argv: std::iter::once(program.to_string())
                .chain(args.iter().cloned())
                .collect(),
            path_prefixes: Vec::new(),
        });
    }
    let program_path = Path::new(program);
    let command_name = program_path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            AgentToolResult::failed(
                format!("failed to prepare sandbox command for {program}"),
                serde_json::json!({
                    "error": "sandbox_command_name_invalid",
                    "detail": format!("program {program} has no valid file name"),
                }),
            )
        })?
        .to_string();
    let mut path_prefixes = Vec::new();
    if let Some(parent) = program_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let parent = if parent.is_absolute() {
            parent.to_path_buf()
        } else {
            cwd.join(parent)
        };
        path_prefixes.push(parent);
    }
    Ok(PreparedSandboxCommand {
        command_name: command_name.clone(),
        argv: std::iter::once(command_name)
            .chain(args.iter().cloned())
            .collect(),
        path_prefixes,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_agent_sandbox_manifest(
    config: &LocalWorkspaceAgentConfig,
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    toolchain_id: &SandboxToolchainId,
    sandbox_cwd: SandboxPath,
    root: &Path,
    cwd: &Path,
    path_prefixes: &[PathBuf],
) -> Result<
    (
        sinomo_runtime::sandbox::SandboxInstanceManifest,
        LocalSandboxInstanceLayout,
    ),
    AgentToolResult,
> {
    fs::create_dir_all(&config.sandbox_runtime_root).map_err(|source| {
        AgentToolResult::failed(
            format!(
                "failed to create sandbox runtime root {}: {source}",
                config.sandbox_runtime_root.display()
            ),
            serde_json::json!({
                "error": "sandbox_runtime_root_unavailable",
                "detail": source.to_string(),
                "path": config.sandbox_runtime_root,
            }),
        )
    })?;
    let now = time::OffsetDateTime::now_utc();
    let (mut manifest, _layout) = match store.load(sandbox_id) {
        Ok(loaded) => loaded,
        Err(SandboxStoreError::NotFound { .. }) => store
            .create(sandbox_id.clone(), toolchain_id.clone(), now)
            .map_err(sandbox_store_failure("create simple sandbox instance"))?,
        Err(source) => {
            return Err(sandbox_store_failure("load simple sandbox instance")(
                source,
            ));
        }
    };
    manifest.toolchain_id = toolchain_id.clone();
    manifest.cwd = sandbox_cwd;
    let agent_home_local = agent_home_local_mirror(config);
    if let Some(home) = agent_home_local.as_deref() {
        fs::create_dir_all(home).map_err(|source| {
            AgentToolResult::failed(
                format!(
                    "failed to create sandbox agent home mirror {}: {source}",
                    home.display()
                ),
                serde_json::json!({
                    "error": "sandbox_agent_home_mirror_unavailable",
                    "detail": source.to_string(),
                    "path": home,
                }),
            )
        })?;
    }
    manifest.env = sandbox_environment(
        config,
        root,
        cwd,
        path_prefixes,
        agent_home_local.as_deref(),
    );
    manifest.mounts = agent_home_mounts(config)?;
    manifest.updated_at = now;
    let layout = store
        .save_manifest(&manifest)
        .map_err(sandbox_store_failure("save simple sandbox manifest"))?;
    if let Some(home_sifs_path) = &config.agent_home_sifs_path {
        fs::create_dir_all(&layout.home).map_err(|source| {
            AgentToolResult::failed(
                format!(
                    "failed to create sandbox home {}: {source}",
                    layout.home.display()
                ),
                serde_json::json!({
                    "error": "sandbox_home_unavailable",
                    "detail": source.to_string(),
                    "path": layout.home,
                }),
            )
        })?;
        fs::create_dir_all(&layout.metadata).map_err(|source| {
            AgentToolResult::failed(
                format!(
                    "failed to create sandbox metadata {}: {source}",
                    layout.metadata.display()
                ),
                serde_json::json!({
                    "error": "sandbox_metadata_unavailable",
                    "detail": source.to_string(),
                    "path": layout.metadata,
                }),
            )
        })?;
        let _ = fs::write(
            layout.metadata.join("agent-home-sifs-path.txt"),
            normalize_agent_home_sifs_path(home_sifs_path.clone()),
        );
        if let Some(home) = agent_home_local.as_deref() {
            let _ = fs::write(
                layout.metadata.join("agent-home-local-path.txt"),
                home.display().to_string(),
            );
        }
    }
    Ok((manifest, layout))
}

fn sandbox_store_failure(
    action: &'static str,
) -> impl FnOnce(SandboxStoreError) -> AgentToolResult {
    move |source| {
        AgentToolResult::failed(
            format!("{action} failed: {source}"),
            serde_json::json!({
                "error": "simple_sandbox_store_failed",
                "action": action,
                "detail": source.to_string(),
            }),
        )
    }
}

fn agent_home_mounts(
    config: &LocalWorkspaceAgentConfig,
) -> Result<Vec<SandboxMount>, AgentToolResult> {
    let Some(source_sifs_path) = &config.agent_home_sifs_path else {
        return Ok(Vec::new());
    };
    Ok(vec![SandboxMount {
        source_sifs_path: normalize_agent_home_sifs_path(source_sifs_path.clone()),
        mount_path: SandboxPath::new("/home").map_err(|source| {
            AgentToolResult::failed(
                format!("failed to prepare sandbox home mount: {source}"),
                serde_json::json!({ "error": "sandbox_home_mount_invalid", "detail": source.to_string() }),
            )
        })?,
        mode: SandboxMountMode::ReadWrite,
    }])
}

fn agent_sandbox_policy() -> SandboxPolicy {
    let mut policy = SandboxPolicy::default();
    if !policy
        .allowed_mount_prefixes
        .iter()
        .any(|prefix| prefix == AGENT_HOME_SIFS_ROOT)
    {
        policy
            .allowed_mount_prefixes
            .push(AGENT_HOME_SIFS_ROOT.to_string());
    }
    policy
}

fn sandbox_environment(
    config: &LocalWorkspaceAgentConfig,
    root: &Path,
    cwd: &Path,
    path_prefixes: &[PathBuf],
    agent_home_local: Option<&Path>,
) -> BTreeMap<String, String> {
    let mut envs = BTreeMap::new();
    if let Some(path) = sandbox_path_env(path_prefixes) {
        envs.insert("PATH".to_string(), path);
    }
    envs.insert(
        "SINOMO_AGENT_TOOL_RUNTIME".to_string(),
        "simple_sandbox".to_string(),
    );
    envs.insert(
        "SINOMO_WORKSPACE_ROOT".to_string(),
        root.display().to_string(),
    );
    envs.insert(
        "SINOMO_WORKSPACE_CWD".to_string(),
        cwd.display().to_string(),
    );
    if let Some(home_sifs_path) = &config.agent_home_sifs_path {
        envs.insert(
            "SINOMO_AGENT_HOME_SIFS".to_string(),
            normalize_agent_home_sifs_path(home_sifs_path.clone()),
        );
    }
    if let Some(home) = agent_home_local {
        let home = home.display().to_string();
        envs.insert("HOME".to_string(), home.clone());
        envs.insert("USERPROFILE".to_string(), home.clone());
        envs.insert("SINOMO_AGENT_HOME_LOCAL".to_string(), home);
    }
    for name in [
        "CARGO_HOME",
        "RUSTUP_HOME",
        "RUSTC",
        "RUSTDOC",
        "RUSTC_WRAPPER",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    ] {
        if let Ok(value) = env::var(name)
            && !value.trim().is_empty()
        {
            envs.insert(name.to_string(), value);
        }
    }
    if !envs.contains_key("CARGO_HOME")
        && let Some(path) = user_home_path().map(|home| home.join(".cargo"))
        && path.exists()
    {
        envs.insert("CARGO_HOME".to_string(), path.display().to_string());
    }
    if !envs.contains_key("RUSTUP_HOME")
        && let Some(path) = user_home_path().map(|home| home.join(".rustup"))
        && path.exists()
    {
        envs.insert("RUSTUP_HOME".to_string(), path.display().to_string());
    }
    envs
}

fn sandbox_extra_read_paths(
    root: &Path,
    cwd: &Path,
    path_prefixes: &[PathBuf],
    agent_home_local: Option<&Path>,
) -> Vec<PathBuf> {
    let mut paths = vec![root.to_path_buf(), cwd.to_path_buf()];
    paths.extend(path_prefixes.iter().cloned());
    if let Some(home) = agent_home_local {
        paths.push(home.to_path_buf());
    }
    paths.extend(current_path_dirs());
    paths.extend(toolchain_home_paths());
    dedup_pathbufs(paths)
}

fn sandbox_extra_write_paths(root: &Path, agent_home_local: Option<&Path>) -> Vec<PathBuf> {
    let mut paths = vec![root.to_path_buf()];
    if let Some(home) = agent_home_local {
        paths.push(home.to_path_buf());
    }
    dedup_pathbufs(paths)
}

fn sandbox_extra_execute_paths(path_prefixes: &[PathBuf]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    paths.extend(path_prefixes.iter().cloned());
    paths.extend(current_path_dirs());
    dedup_pathbufs(paths)
}

fn sandbox_path_env(path_prefixes: &[PathBuf]) -> Option<String> {
    let mut entries = Vec::new();
    entries.extend(path_prefixes.iter().cloned());
    entries.extend(current_path_dirs());
    let entries = dedup_pathbufs(entries);
    env::join_paths(entries)
        .ok()
        .and_then(|value| value.into_string().ok())
}

fn current_path_dirs() -> Vec<PathBuf> {
    env::var_os("PATH")
        .map(|value| env::split_paths(&value).collect())
        .unwrap_or_default()
}

fn toolchain_home_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for name in ["CARGO_HOME", "RUSTUP_HOME"] {
        if let Some(path) = env::var_os(name).map(PathBuf::from)
            && path.exists()
        {
            paths.push(path);
        }
    }
    if let Some(home) = user_home_path() {
        for child in [".cargo", ".rustup"] {
            let path = home.join(child);
            if path.exists() {
                paths.push(path);
            }
        }
    }
    dedup_pathbufs(paths)
}

fn user_home_path() -> Option<PathBuf> {
    env::var_os("USERPROFILE")
        .or_else(|| env::var_os("HOME"))
        .map(PathBuf::from)
}

fn dedup_pathbufs(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut unique = Vec::new();
    for path in paths {
        if !unique.iter().any(|existing| existing == &path) {
            unique.push(path);
        }
    }
    unique
}

fn agent_sandbox_id(root: &Path) -> SandboxId {
    SandboxId::derived(
        "sinomo-agent-local-workspace-sandbox",
        root.to_string_lossy().as_bytes(),
    )
}

fn default_agent_sandbox_runtime_root() -> PathBuf {
    env::temp_dir().join("sinomo-agent").join("sandbox-runtime")
}

fn agent_home_local_mirror(config: &LocalWorkspaceAgentConfig) -> Option<PathBuf> {
    let home_sifs_path = config
        .agent_home_sifs_path
        .as_ref()
        .map(|path| normalize_agent_home_sifs_path(path.clone()))?;
    let agent_id = home_sifs_path
        .strip_prefix(&format!("{AGENT_HOME_SIFS_ROOT}/"))
        .and_then(|path| path.strip_suffix("/home"))
        .map(sanitize_agent_home_id)
        .unwrap_or_else(|| "custom".to_string());
    if let Some(root) = &config.agent_home_local_root {
        return Some(root.join(agent_id).join("home"));
    }
    let digest = blake3::hash(home_sifs_path.as_bytes()).to_hex().to_string();
    config
        .sandbox_runtime_root
        .join("agent-homes")
        .join(format!("{agent_id}-{}", &digest[..16]))
        .join("home")
        .into()
}

fn default_agent_home_sifs_path_for_root(root: &Path) -> String {
    let id = SandboxId::derived("sinomo-agent-home-root", root.to_string_lossy().as_bytes());
    agent_home_sifs_path_for_id(&id.to_string())
}

fn default_agent_home_sifs_path_for_session(session_id: &SessionId) -> String {
    agent_home_sifs_path_for_id(&session_id.to_string())
}

fn agent_home_sifs_path_for_id(agent_id: &str) -> String {
    let agent_id = sanitize_agent_home_id(agent_id);
    format!("{AGENT_HOME_SIFS_ROOT}/{agent_id}/home")
}

fn sanitize_agent_home_id(agent_id: &str) -> String {
    let sanitized: String = agent_id
        .trim()
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect();
    let sanitized = sanitized.trim_matches('-').trim_matches('.').to_string();
    if sanitized.is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}

fn normalize_agent_home_sifs_path(path: String) -> String {
    let path = path
        .trim()
        .trim_start_matches("sifs://")
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();
    if path.is_empty() {
        agent_home_sifs_path_for_id("default")
    } else {
        format!("/{path}")
    }
}

fn sandbox_path_for_host_path(path: &Path) -> Result<SandboxPath, AgentToolResult> {
    SandboxPath::new(sandbox_path_string_for_host_path(path)?).map_err(|source| {
        AgentToolResult::failed(
            format!(
                "failed to prepare sandbox cwd for {}: {source}",
                path.display()
            ),
            serde_json::json!({
                "error": "sandbox_cwd_invalid",
                "detail": source.to_string(),
                "path": path,
            }),
        )
    })
}

#[cfg(windows)]
fn sandbox_path_string_for_host_path(path: &Path) -> Result<String, AgentToolResult> {
    let raw = path.display().to_string();
    let raw = raw.strip_prefix(r"\\?\").unwrap_or(&raw);
    let normalized = raw.replace('\\', "/");
    if normalized.as_bytes().get(1) == Some(&b':')
        && normalized
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
    {
        Ok(format!("/{normalized}"))
    } else {
        Err(AgentToolResult::failed(
            format!(
                "sandbox cwd {} is not a supported Windows drive path",
                path.display()
            ),
            serde_json::json!({
                "error": "sandbox_cwd_unsupported",
                "detail": "simple sandbox cwd supports local Windows drive paths only",
                "path": path,
            }),
        ))
    }
}

#[cfg(not(windows))]
fn sandbox_path_string_for_host_path(path: &Path) -> Result<String, AgentToolResult> {
    Ok(path.display().to_string())
}

#[allow(clippy::too_many_arguments)]
fn sandbox_process_snapshot(
    process_id: &str,
    command: &str,
    program: &str,
    args: &[String],
    cwd: &Path,
    status: AgentProcessLifecycleStatus,
    exit_code: Option<i32>,
    timed_out: bool,
    cancel_requested: bool,
    error: Option<String>,
    elapsed: Duration,
    timeout_ms: Option<u64>,
    stdout: &[u8],
    stderr: &[u8],
    max_output_bytes: usize,
) -> AgentProcessSnapshot {
    let stdout_text = truncate_utf8_lossy(stdout, max_output_bytes);
    let stderr_text = truncate_utf8_lossy(stderr, max_output_bytes);
    AgentProcessSnapshot {
        process_id: process_id.to_string(),
        command: command.to_string(),
        program: program.to_string(),
        args: args.to_vec(),
        cwd: cwd.display().to_string(),
        status,
        exit_code,
        timed_out,
        cancel_requested,
        error,
        elapsed_ms: duration_ms_from_elapsed(elapsed),
        timeout_ms,
        stdout: stdout_text.text,
        stderr: stderr_text.text,
        stdout_offset: stdout.len().min(max_output_bytes),
        stderr_offset: stderr.len().min(max_output_bytes),
        stdout_total_bytes: stdout.len(),
        stderr_total_bytes: stderr.len(),
        stdout_truncated: stdout_text.truncated,
        stderr_truncated: stderr_text.truncated,
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

fn local_command_result_from_output(
    config: &LocalWorkspaceAgentConfig,
    root: PathBuf,
    cwd: PathBuf,
    program: &str,
    args: &[String],
    output: &LocalCommandOutput,
    timed_out: bool,
    cancelled: bool,
    duration_ms: u64,
    timeout_ms: Option<u64>,
    evidence_tool_name: &str,
    evidence_reason: &str,
    changed_files: Vec<String>,
    summary_override: Option<String>,
) -> AgentToolResult {
    let stdout = truncate_utf8_lossy(&output.stdout, config.max_command_output_bytes);
    let stderr = truncate_utf8_lossy(&output.stderr, config.max_command_output_bytes);
    let status = output.status_code;
    let succeeded = !timed_out && !cancelled && output.success;
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
    } else if cancelled {
        format!("{} {} cancelled", program, args.join(" "))
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
        "cancelled": cancelled,
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
            "cancelled": cancelled,
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
    let mut evidence = AgentToolEvidence::availability_only(true, evidence_reason)
        .with_command(cwd.display().to_string(), status)
        .with_output_value(&payload, stdout.truncated || stderr.truncated)
        .with_metadata(serde_json::json!({
            "workspace_root": root.display().to_string(),
            "timed_out": timed_out,
            "cancelled": cancelled,
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
    if cancelled {
        AgentToolResult::cancelled(summary, payload).with_evidence(evidence)
    } else if succeeded {
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
    let file_name = format!(
        "{}-{}.{}",
        sanitize_evidence_component(&tool_name.replace('_', "-")),
        TaskId::generate(),
        sanitize_evidence_component(extension.trim_start_matches('.'))
    );
    let local_ref = config.evidence_dir.as_ref().and_then(|evidence_dir| {
        fs::create_dir_all(evidence_dir).ok()?;
        let path = evidence_dir.join(&file_name);
        fs::write(&path, bytes).ok()?;
        Some(path.display().to_string())
    });
    let sifs_ref = config.sifs_evidence.as_ref().and_then(|sifs| {
        fs::create_dir_all(&sifs.mount_dir).ok()?;
        let path = sifs.mount_dir.join(&file_name);
        fs::write(&path, bytes).ok()?;
        Some(sifs.sifs_uri_for_file(&file_name))
    });
    sifs_ref.or(local_ref)
}

fn normalize_sifs_evidence_root(root: String) -> String {
    let root = root
        .trim()
        .trim_start_matches("sifs://")
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();
    if root.is_empty() {
        "/events/agent/evidence".to_string()
    } else {
        format!("/{root}")
    }
}

fn sanitize_evidence_component(component: &str) -> String {
    let sanitized: String = component
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect();
    let sanitized = sanitized.trim_matches('-').trim_matches('.').to_string();
    if sanitized.is_empty() {
        "evidence".to_string()
    } else {
        sanitized
    }
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

fn ensure_local_tool_not_cancelled(
    cancellation_token: &AgentCancellationToken,
    tool_name: &str,
) -> Result<(), String> {
    if cancellation_token.is_cancelled() {
        Err(format!("{tool_name} cancelled"))
    } else {
        Ok(())
    }
}

fn local_tool_cancelled_result(tool_name: &str, detail: impl Into<String>) -> AgentToolResult {
    let detail = detail.into();
    let payload = serde_json::json!({
        "error": "tool_cancelled",
        "tool": tool_name,
        "detail": detail,
    });
    AgentToolResult::cancelled(format!("{tool_name} cancelled"), payload.clone()).with_evidence(
        AgentToolEvidence::availability_only(false, "turn cancellation requested")
            .with_output_value(&payload, false),
    )
}

fn collect_workspace_search_matches(
    config: &LocalWorkspaceAgentConfig,
    path: &Path,
    query: &str,
    case_sensitive: bool,
    max_matches: usize,
    matches: &mut Vec<serde_json::Value>,
    cancellation_token: &AgentCancellationToken,
) -> Result<bool, String> {
    ensure_local_tool_not_cancelled(cancellation_token, "search")?;
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
            ensure_local_tool_not_cancelled(cancellation_token, "search")?;
            let truncated = collect_workspace_search_matches(
                config,
                &entry.path(),
                query,
                case_sensitive,
                max_matches,
                matches,
                cancellation_token,
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
        ensure_local_tool_not_cancelled(cancellation_token, "search")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_workspace_commands_default_to_simple_sandbox_with_agent_home() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-default");
        let config = LocalWorkspaceAgentConfig::new(&root);

        assert_eq!(
            config.command_runtime,
            LocalWorkspaceCommandRuntime::SimpleSandbox
        );
        assert!(
            config
                .agent_home_sifs_path
                .as_deref()
                .is_some_and(|path| path.starts_with("/sinomo/agents/") && path.ends_with("/home"))
        );
    }

    #[test]
    fn simple_sandbox_manifest_mounts_agent_home_at_home() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-home");
        let runtime_root = root.join("sandbox-runtime");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .with_agent_home_sifs_path("/sinomo/agents/test-agent/home");
        let canonical_workspace = fs::canonicalize(&workspace).expect("canonical workspace");
        let sandbox_id = agent_sandbox_id(&canonical_workspace);
        let toolchain_id =
            SandboxToolchainId::from_legacy_named(AGENT_SANDBOX_TOOLCHAIN_ID).expect("toolchain");
        let store = LocalSandboxInstanceStore::new(&runtime_root);
        let sandbox_cwd =
            sandbox_path_for_host_path(&canonical_workspace).expect("sandbox cwd path");

        let (_manifest, layout) = prepare_agent_sandbox_manifest(
            &config,
            &store,
            &sandbox_id,
            &toolchain_id,
            sandbox_cwd,
            &canonical_workspace,
            &canonical_workspace,
            &[],
        )
        .expect("manifest");
        let (manifest, _layout) = store.load(&sandbox_id).expect("load manifest");

        assert_eq!(manifest.mounts.len(), 1);
        assert_eq!(
            manifest.mounts[0].source_sifs_path,
            "/sinomo/agents/test-agent/home"
        );
        assert_eq!(manifest.mounts[0].mount_path.as_str(), "/home");
        assert_eq!(manifest.mounts[0].mode, SandboxMountMode::ReadWrite);
        assert!(layout.home.exists());
        let home_local = agent_home_local_mirror(&config).expect("local home mirror");
        assert!(home_local.exists());
        assert_eq!(
            manifest.env.get("HOME").map(String::as_str),
            Some(home_local.to_str().expect("utf8 home mirror"))
        );
        assert_eq!(
            manifest.env.get("USERPROFILE").map(String::as_str),
            Some(home_local.to_str().expect("utf8 home mirror"))
        );
        assert_eq!(
            manifest
                .env
                .get("SINOMO_AGENT_HOME_LOCAL")
                .map(String::as_str),
            Some(home_local.to_str().expect("utf8 home mirror"))
        );
        assert_eq!(
            fs::read_to_string(layout.metadata.join("agent-home-sifs-path.txt"))
                .expect("home metadata"),
            "/sinomo/agents/test-agent/home"
        );
        assert_eq!(
            fs::read_to_string(layout.metadata.join("agent-home-local-path.txt"))
                .expect("local home metadata"),
            home_local.display().to_string()
        );
    }

    #[test]
    fn agent_home_local_mirror_roams_by_sifs_home_path() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-home-roaming");
        let runtime_root = root.join("sandbox-runtime");
        let workspace_a = root.join("workspace-a");
        let workspace_b = root.join("workspace-b");
        fs::create_dir_all(&workspace_a).expect("workspace a");
        fs::create_dir_all(&workspace_b).expect("workspace b");
        let config_a = LocalWorkspaceAgentConfig::new(&workspace_a)
            .with_sandbox_runtime_root(&runtime_root)
            .with_agent_home_sifs_path("/sinomo/agents/test-agent/home");
        let config_b = LocalWorkspaceAgentConfig::new(&workspace_b)
            .with_sandbox_runtime_root(&runtime_root)
            .with_agent_home_sifs_path("/sinomo/agents/test-agent/home");

        assert_eq!(
            agent_home_local_mirror(&config_a),
            agent_home_local_mirror(&config_b)
        );
        assert!(
            agent_home_local_mirror(&config_a)
                .expect("local mirror")
                .starts_with(runtime_root.join("agent-homes"))
        );
    }

    #[test]
    fn agent_home_local_mirror_uses_data_dir_agents_home_root() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-home-data-dir");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let session_id = SessionId::from_legacy_named("sess-agent-home").expect("session");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_data_dir_evidence(&data_dir, &session_id);
        let agent_id = session_id.to_string();

        assert_eq!(
            config.agent_home_sifs_path.as_deref(),
            Some(agent_home_sifs_path_for_id(&agent_id).as_str())
        );
        assert_eq!(
            agent_home_local_mirror(&config),
            Some(data_dir.root().join("agents").join(agent_id).join("home"))
        );
    }

    #[test]
    fn simple_sandbox_policy_allows_roaming_agent_home_prefix() {
        let policy = agent_sandbox_policy();

        assert!(
            policy
                .allowed_mount_prefixes
                .iter()
                .any(|prefix| prefix == "/sinomo/agents")
        );
    }

    #[test]
    #[cfg(windows)]
    fn local_workspace_command_uses_simple_sandbox_on_windows() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-command");
        let runtime_root = root.join("sandbox-runtime");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .allowing_shell_commands(true);
        let args = vec![
            "/C".to_string(),
            "echo %SINOMO_AGENT_TOOL_RUNTIME% && echo %USERPROFILE% && echo roaming>%USERPROFILE%\\roaming.txt && type %USERPROFILE%\\roaming.txt".to_string(),
        ];

        let execution = execute_local_command(
            &config,
            "cmd.exe",
            &args,
            None,
            Some(5000),
            None,
            &AgentCancellationToken::new(),
            AgentToolProgressSink::default(),
        )
        .expect("sandbox command");

        assert!(
            execution.output.success,
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&execution.output.stdout),
            String::from_utf8_lossy(&execution.output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&execution.output.stdout)
                .to_ascii_lowercase()
                .contains("simple_sandbox")
        );
        assert!(
            String::from_utf8_lossy(&execution.output.stdout)
                .to_ascii_lowercase()
                .contains("roaming")
        );
    }
}
