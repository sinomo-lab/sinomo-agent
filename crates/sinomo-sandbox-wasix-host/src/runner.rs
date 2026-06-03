use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sinomo_dfs::NetworkedDfsService;
use sinomo_module::{
    ModuleId, NativeDylibModuleLoaderState, NativeModuleUnloadReport, native_module_cache_path,
    read_native_dylib_module_loader_state,
};
use sinomo_network::NetworkTransport;
use sinomo_sandbox::*;
use sinomo_sandbox_wasix_api::{
    SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, SINOMO_SANDBOX_WASIX_MODULE_ID,
    SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION, SandboxWasixCommandOverride, SandboxWasixHostPreopen,
    SandboxWasixNativeErrorResponse, SandboxWasixNativeRequest, SandboxWasixNativeResponse,
    SandboxWasixRunModuleOptions, SandboxWasixRunModuleRequest,
    SandboxWasixRunReplayFixtureRequest, SandboxWasixValidateComponentRequest,
};
use time::OffsetDateTime;

use crate::{SandboxWasixNativeModule, SandboxWasixNativeModuleError};

pub struct SandboxWasixModuleCommandRunner {
    module: SandboxWasixNativeModule,
}

impl SandboxWasixModuleCommandRunner {
    pub fn from_active_module(
        data_dir_root: impl AsRef<Path>,
    ) -> Result<Self, SandboxCommandExecutionError> {
        let module = load_active_sandbox_wasix_module(data_dir_root.as_ref())?;
        Ok(Self { module })
    }

    pub fn from_module(module: SandboxWasixNativeModule) -> Self {
        Self { module }
    }

    pub fn unload(self) -> NativeModuleUnloadReport {
        self.module.unload()
    }

    pub fn validate_sinomo_module_v1_component_contract(
        &self,
        wasm: impl AsRef<[u8]>,
    ) -> Result<(), SandboxCommandExecutionError> {
        let request =
            SandboxWasixNativeRequest::ValidateComponent(SandboxWasixValidateComponentRequest {
                wasm: wasm.as_ref().to_vec(),
            });
        match self
            .module
            .invoke(&request)
            .map_err(wasix_native_module_error_to_wasi)?
        {
            SandboxWasixNativeResponse::ValidateComponent(_) => Ok(()),
            SandboxWasixNativeResponse::Error(error) => {
                Err(wasix_native_error_to_wasi(error).into())
            }
            other => Err(SandboxWasiError::Runtime {
                detail: format!("WASIX module returned unexpected validation response {other:?}"),
            }
            .into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        mut extra_preopens: Vec<SandboxWasixHostPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let (manifest, layout) = store.load(sandbox_id)?;
        let (toolchain, _toolchain_layout) =
            match toolchain_store.load_manifest(&manifest.toolchain_id) {
                Ok(toolchain) => toolchain,
                Err(SandboxToolchainError::NotFound { .. }) => {
                    return self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
                        store,
                        sandbox_id,
                        wasm,
                        argv,
                        stdin,
                        timeout_ms,
                        limits,
                        extra_preopens,
                        output_sink,
                        default_sandbox_wasi_abi(),
                        BTreeMap::new(),
                        None,
                        BTreeMap::new(),
                        false,
                        true,
                        true,
                        true,
                        false,
                        Vec::new(),
                    );
                }
                Err(error) => return Err(error.into()),
            };
        let toolchain_artifacts = load_local_toolchain_artifacts(toolchain_store, &toolchain)?;
        let toolchain_commands = load_local_toolchain_commands(toolchain_store, &toolchain)?;
        let mut artifact_preopens =
            stage_toolchain_runtime_to_local(&layout, &toolchain_artifacts, &toolchain_commands)?;
        artifact_preopens.append(&mut extra_preopens);
        self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            limits,
            artifact_preopens,
            output_sink,
            default_sandbox_wasi_abi(),
            BTreeMap::new(),
            None,
            BTreeMap::new(),
            false,
            true,
            true,
            true,
            false,
            wasix_command_overrides_for_toolchain(&toolchain),
        )
    }

    pub fn run_toolchain_command_with_stdin_and_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_and_output_sink(
            store,
            toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
            store,
            toolchain_store,
            sandbox_id,
            command_name,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            None,
            BTreeMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        validate_sandbox_toolchain_argv(command_name, &argv).map_err(|detail| {
            SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { detail })
        })?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let (toolchain, _toolchain_layout) =
            toolchain_store.load_manifest(&manifest.toolchain_id)?;
        let wasm = toolchain_store.load_command_module(&toolchain, command_name)?;
        let toolchain_artifacts = load_local_toolchain_artifacts(toolchain_store, &toolchain)?;
        let toolchain_commands = load_local_toolchain_commands(toolchain_store, &toolchain)?;
        self.run_resolved_toolchain_command_with_policy(
            store,
            sandbox_id,
            &manifest,
            &layout,
            &toolchain,
            command_name,
            wasm,
            toolchain_artifacts,
            toolchain_commands,
            argv,
            stdin,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
            cwd_override,
            env_override,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_resolved_toolchain_command_with_policy(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        manifest: &SandboxInstanceManifest,
        layout: &LocalSandboxInstanceLayout,
        toolchain: &SandboxToolchainManifest,
        command_name: &str,
        wasm: Vec<u8>,
        toolchain_artifacts: Vec<(String, Vec<u8>)>,
        toolchain_commands: Vec<(String, Vec<u8>)>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let command = toolchain.command(command_name)?;
        policy
            .validate_invocation(manifest, toolchain, command, execution_node_id)
            .map_err(SandboxCommandExecutionError::PolicyDenied)?;
        let limits = effective_sandbox_command_limits(&manifest.limits, &command.default_limits);
        let timeout_ms = effective_sandbox_timeout_ms(timeout_ms, limits.wall_time_ms);
        let argv = command.effective_argv(argv);
        let allow_preview1_pinned_subprocesses = command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY);
        let wasix_pinned_child_exec_only = !command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY);
        let wasix_auto_asyncify = command
            .required_capabilities
            .iter()
            .any(|capability| capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY);
        let artifact_preopens =
            stage_toolchain_runtime_to_local(layout, &toolchain_artifacts, &toolchain_commands)?;
        self.run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
            store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            Some(limits),
            artifact_preopens,
            output_sink,
            command.effective_target_abi(toolchain),
            command.default_env.clone(),
            cwd_override,
            env_override,
            allow_preview1_pinned_subprocesses,
            wasix_pinned_child_exec_only,
            false,
            wasix_auto_asyncify,
            false,
            wasix_command_overrides_for_toolchain(toolchain),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_module_with_stdin_limits_preopens_output_sink_abi_and_env(
        &self,
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        wasm: impl AsRef<[u8]>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        limits: Option<SandboxResourceLimits>,
        extra_preopens: Vec<SandboxWasixHostPreopen>,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
        target_abi: SandboxWasiAbi,
        default_env: BTreeMap<String, String>,
        cwd_override: Option<SandboxPath>,
        env_override: BTreeMap<String, String>,
        allow_preview1_pinned_subprocesses: bool,
        wasix_pinned_child_exec_only: bool,
        wasix_instruction_metering: bool,
        wasix_auto_asyncify: bool,
        wasix_fpcast_emu: bool,
        wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        let (manifest, layout) = store.load(sandbox_id)?;
        let limits = limits.unwrap_or_else(|| manifest.limits.clone());
        let effective_cwd = cwd_override.unwrap_or_else(|| manifest.cwd.clone());
        let sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
        sandbox_fs.validate_resource_limits(&limits)?;
        validate_sandbox_working_directory(&sandbox_fs, &effective_cwd)?;
        let working_directory_host_path = if target_abi == SandboxWasiAbi::Preview1 {
            Some(sandbox_fs.host_path(&effective_cwd)?)
        } else {
            None
        };
        let before = sandbox_fs.snapshot()?;
        let rollback_checkpoint = store.create_checkpoint(sandbox_id, OffsetDateTime::now_utc())?;
        let started_at = OffsetDateTime::now_utc();
        let mut options = local_staged_sandbox_wasix_options(&layout, &limits, extra_preopens);
        options.working_directory_host_path = working_directory_host_path;
        options.allow_preview1_pinned_subprocesses = allow_preview1_pinned_subprocesses;
        options.wasix_pinned_child_exec_only = wasix_pinned_child_exec_only;
        options.wasix_instruction_metering = wasix_instruction_metering;
        options.wasix_auto_asyncify = target_abi == SandboxWasiAbi::Wasix && wasix_auto_asyncify;
        options.wasix_fpcast_emu = target_abi == SandboxWasiAbi::Wasix && wasix_fpcast_emu;
        options.wasix_command_overrides = wasix_command_overrides;

        let mut env = default_env;
        env.extend(manifest.env.clone());
        env.extend(env_override);
        env.insert("PWD".to_string(), effective_cwd.as_str().to_string());
        let request = SandboxCommandRequest {
            sandbox_id: manifest.sandbox_id.clone(),
            argv: argv.clone(),
            cwd: effective_cwd.clone(),
            stdin,
            env,
            timeout_ms,
        };
        let mut outcome = match self.invoke_run_module(
            target_abi,
            wasm.as_ref().to_vec(),
            request.clone(),
            options,
            output_sink,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                store.resume_checkpoint(
                    sandbox_id,
                    &rollback_checkpoint.checkpoint_id,
                    OffsetDateTime::now_utc(),
                )?;
                let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
                let command_id = SandboxCommandId::generate();
                let completed_at = OffsetDateTime::now_utc();
                let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
                persist_command_record(
                    &mut sandbox_fs,
                    &SandboxCommandRecord {
                        sandbox_id: manifest.sandbox_id.clone(),
                        command_id: command_id.clone(),
                        status: sandbox_command_record_status_for_wasi_error(&error),
                        workspace_root: manifest.workspace_root.clone(),
                        toolchain_id: manifest.toolchain_id.clone(),
                        argv,
                        cwd: effective_cwd.clone(),
                        env: request.env.clone(),
                        started_at,
                        completed_at: Some(completed_at),
                        response: None,
                        error: Some(error.to_string()),
                    },
                )?;
                record_local_command_history(store, sandbox_id, &command_id, completed_at)?;
                return Err(error.into());
            }
        };

        let after = sandbox_fs.snapshot()?;
        if let Err(error) = validate_workspace_usage(&after.workspace_usage(), &limits) {
            store.resume_checkpoint(
                sandbox_id,
                &rollback_checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )?;
            let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
            let completed_at = OffsetDateTime::now_utc();
            let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
            persist_command_record(
                &mut sandbox_fs,
                &SandboxCommandRecord {
                    sandbox_id: manifest.sandbox_id.clone(),
                    command_id: outcome.response.command_id.clone(),
                    status: SandboxCommandRecordStatus::ResourceExhausted,
                    workspace_root: manifest.workspace_root.clone(),
                    toolchain_id: manifest.toolchain_id.clone(),
                    argv,
                    cwd: effective_cwd.clone(),
                    env: request.env.clone(),
                    started_at,
                    completed_at: Some(completed_at),
                    response: Some(outcome.response.clone()),
                    error: Some(error.to_string()),
                },
            )?;
            record_local_command_history(
                store,
                sandbox_id,
                &outcome.response.command_id,
                completed_at,
            )?;
            return Err(error.into());
        }

        let written_bytes = after.written_bytes_since(&before);
        if let Err(error) = validate_written_bytes(written_bytes, &limits) {
            store.resume_checkpoint(
                sandbox_id,
                &rollback_checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )?;
            let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
            let mut response = outcome.response.clone();
            response.resource_usage.written_bytes = written_bytes;
            let completed_at = OffsetDateTime::now_utc();
            let mut sandbox_fs = LocalSandboxFileSystem::new(layout.clone());
            persist_command_record(
                &mut sandbox_fs,
                &SandboxCommandRecord {
                    sandbox_id: manifest.sandbox_id.clone(),
                    command_id: response.command_id.clone(),
                    status: SandboxCommandRecordStatus::ResourceExhausted,
                    workspace_root: manifest.workspace_root.clone(),
                    toolchain_id: manifest.toolchain_id.clone(),
                    argv,
                    cwd: effective_cwd.clone(),
                    env: request.env.clone(),
                    started_at,
                    completed_at: Some(completed_at),
                    response: Some(response),
                    error: Some(error.to_string()),
                },
            )?;
            record_local_command_history(
                store,
                sandbox_id,
                &outcome.response.command_id,
                completed_at,
            )?;
            return Err(error.into());
        }

        let _ = store.delete_checkpoint(sandbox_id, &rollback_checkpoint.checkpoint_id);
        outcome.response.changed_paths = sandbox_fs.changed_paths_since(&before)?;
        outcome.response.resource_usage.written_bytes = written_bytes;
        let completed_at = OffsetDateTime::now_utc();
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        persist_command_outputs(
            &mut sandbox_fs,
            &SandboxCommandRecord {
                sandbox_id: manifest.sandbox_id.clone(),
                command_id: outcome.response.command_id.clone(),
                status: SandboxCommandRecordStatus::Completed,
                workspace_root: manifest.workspace_root.clone(),
                toolchain_id: manifest.toolchain_id.clone(),
                argv,
                cwd: effective_cwd,
                env: request.env.clone(),
                started_at,
                completed_at: Some(completed_at),
                response: Some(outcome.response.clone()),
                error: None,
            },
            &outcome.stdout,
            &outcome.stderr,
        )?;
        record_local_command_history(
            store,
            sandbox_id,
            &outcome.response.command_id,
            completed_at,
        )?;
        Ok(outcome)
    }

    pub fn run_toolchain_process_plan_with_policy_and_output_sink(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError> {
        validate_sandbox_process_plan(plan)?;
        let (manifest, layout) = store.load(sandbox_id)?;
        let sandbox_fs = LocalSandboxFileSystem::new(layout);
        validate_sandbox_process_plan_working_directories(plan, &sandbox_fs, &manifest.cwd)?;
        let (toolchain, _toolchain_layout) =
            toolchain_store.load_manifest(&manifest.toolchain_id)?;
        validate_sandbox_process_plan_invocations(
            plan,
            &manifest,
            &toolchain,
            execution_node_id,
            policy,
        )?;

        let mut step_outcomes = Vec::with_capacity(plan.steps.len());
        let mut piped_stdin: Option<Vec<u8>> = None;
        for step in &plan.steps {
            let stdin = step.stdin.clone().or_else(|| piped_stdin.take());
            let outcome = self.run_toolchain_command_with_stdin_policy_output_sink_cwd_and_env(
                store,
                toolchain_store,
                sandbox_id,
                &step.command_name,
                step.argv.clone(),
                stdin,
                timeout_ms,
                execution_node_id,
                policy,
                output_sink.clone(),
                step.cwd.clone(),
                step.env.clone(),
            )?;
            if outcome.response.exit_status != 0 {
                let exit_status = outcome.response.exit_status;
                let stdout = outcome.stdout.clone();
                let stderr = outcome.stderr.clone();
                step_outcomes.push(outcome);
                return Ok(SandboxProcessPlanOutcome {
                    step_outcomes,
                    exit_status,
                    stdout,
                    stderr,
                });
            }
            if step.pipe_stdout_to_next {
                piped_stdin = Some(outcome.stdout.clone());
            }
            step_outcomes.push(outcome);
        }

        let last = step_outcomes
            .last()
            .expect("non-empty process plan should produce an outcome");
        Ok(SandboxProcessPlanOutcome {
            exit_status: last.response.exit_status,
            stdout: last.stdout.clone(),
            stderr: last.stderr.clone(),
            step_outcomes,
        })
    }

    pub fn verify_busybox_toolchain_fidelity(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
        cases: &[SandboxBusyBoxFidelityCase],
    ) -> Result<Vec<SandboxToolFidelityResult>, SandboxToolFidelityError> {
        let mut results = Vec::new();
        for case in cases {
            let outcome = self.run_toolchain_command_with_stdin_and_policy(
                store,
                toolchain_store,
                sandbox_id,
                case.command,
                case.argv_vec(),
                case.stdin_bytes(),
                SandboxResourceLimits::default().wall_time_ms,
                execution_node_id,
                policy,
            )?;
            let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
            let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
            let mut mismatch = None;
            if outcome.response.exit_status != case.expected_exit_status {
                mismatch = Some(format!(
                    "expected exit {}, got {}",
                    case.expected_exit_status, outcome.response.exit_status
                ));
            } else if stdout != case.expected_stdout {
                mismatch = Some(format!(
                    "expected stdout {:?}, got {:?}",
                    case.expected_stdout, stdout
                ));
            }
            results.push(SandboxToolFidelityResult {
                case_name: case.name.to_string(),
                command: case.command.to_string(),
                passed: mismatch.is_none(),
                exit_status: outcome.response.exit_status,
                stdout,
                stderr,
                mismatch,
            });
        }
        let failed = results.iter().filter(|result| !result.passed).count();
        if failed > 0 {
            return Err(SandboxToolFidelityError::Failed {
                failed,
                total: results.len(),
                results,
            });
        }
        Ok(results)
    }

    pub fn verify_quickjs_toolchain_fidelity(
        &self,
        store: &LocalSandboxInstanceStore,
        toolchain_store: &LocalSandboxToolchainStore,
        sandbox_id: &SandboxId,
        execution_node_id: &sinomo_node::NodeId,
        policy: &SandboxPolicy,
        cases: &[SandboxQuickJsFidelityCase],
    ) -> Result<Vec<SandboxToolFidelityResult>, SandboxToolFidelityError> {
        let mut results = Vec::new();
        for case in cases {
            if !case.setup_files.is_empty() {
                let (_manifest, layout) = store
                    .load(sandbox_id)
                    .map_err(SandboxCommandExecutionError::from)?;
                let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
                for (path, contents) in case.setup_files {
                    let path = SandboxPath::new((*path).to_string())
                        .map_err(SandboxFsError::from)
                        .map_err(SandboxCommandExecutionError::from)?;
                    sandbox_fs
                        .write(&path, contents.as_bytes())
                        .map_err(SandboxCommandExecutionError::from)?;
                }
            }
            let outcome = self.run_toolchain_command_with_stdin_and_policy(
                store,
                toolchain_store,
                sandbox_id,
                QUICKJS_TOOLCHAIN_COMMAND,
                case.argv_vec(),
                None,
                SandboxResourceLimits::default().wall_time_ms,
                execution_node_id,
                policy,
            )?;
            let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
            let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
            let mut mismatch = None;
            if outcome.response.exit_status != case.expected_exit_status {
                mismatch = Some(format!(
                    "expected exit {}, got {}",
                    case.expected_exit_status, outcome.response.exit_status
                ));
            } else if stdout != case.expected_stdout {
                mismatch = Some(format!(
                    "expected stdout {:?}, got {:?}",
                    case.expected_stdout, stdout
                ));
            } else if let Some(expected) = case.expected_stderr_contains
                && !stderr.contains(expected)
            {
                mismatch = Some(format!(
                    "expected stderr to contain {:?}, got {:?}",
                    expected, stderr
                ));
            }
            results.push(SandboxToolFidelityResult {
                case_name: case.name.to_string(),
                command: QUICKJS_TOOLCHAIN_COMMAND.to_string(),
                passed: mismatch.is_none(),
                exit_status: outcome.response.exit_status,
                stdout,
                stderr,
                mismatch,
            });
        }
        let failed = results.iter().filter(|result| !result.passed).count();
        if failed > 0 {
            return Err(SandboxToolFidelityError::Failed {
                failed,
                total: results.len(),
                results,
            });
        }
        Ok(results)
    }

    fn invoke_run_module(
        &self,
        target_abi: SandboxWasiAbi,
        wasm: Vec<u8>,
        command: SandboxCommandRequest,
        options: SandboxWasixRunModuleOptions,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
        let request = SandboxWasixNativeRequest::RunModule(SandboxWasixRunModuleRequest {
            target_abi,
            wasm,
            command,
            options,
        });
        match self
            .module
            .invoke_with_output_sink(&request, output_sink)
            .map_err(wasix_native_module_error_to_wasi)?
        {
            SandboxWasixNativeResponse::RunModule(response) => Ok(response.outcome),
            SandboxWasixNativeResponse::Error(error) => Err(wasix_native_error_to_wasi(error)),
            other => Err(SandboxWasiError::Runtime {
                detail: format!("WASIX module returned unexpected response {other:?}"),
            }),
        }
    }
}

pub struct SandboxWasixSifsCommandOutcome<T> {
    pub dfs: NetworkedDfsService<T>,
    pub command: Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError>,
}

pub struct SandboxWasixSifsProcessPlanOutcome<T> {
    pub dfs: NetworkedDfsService<T>,
    pub process: Result<SandboxProcessPlanOutcome, SandboxCommandExecutionError>,
}

pub struct SandboxWasixSifsCommandRunner {
    local_runner: SandboxWasixModuleCommandRunner,
    replica_candidates: Vec<sinomo_node::NodeId>,
}

impl SandboxWasixSifsCommandRunner {
    pub fn from_active_module(
        data_dir_root: impl AsRef<Path>,
    ) -> Result<Self, SandboxCommandExecutionError> {
        Ok(Self {
            local_runner: SandboxWasixModuleCommandRunner::from_active_module(data_dir_root)?,
            replica_candidates: Vec::new(),
        })
    }

    pub fn with_replica_candidates(mut self, replica_candidates: Vec<sinomo_node::NodeId>) -> Self {
        self.replica_candidates = replica_candidates;
        self.replica_candidates.sort();
        self.replica_candidates.dedup();
        self
    }

    fn sifs_instance_store<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
    ) -> SifsSandboxInstanceStore<T> {
        let store = SifsSandboxInstanceStore::new(dfs);
        if self.replica_candidates.is_empty() {
            store
        } else {
            store.with_replica_candidates(self.replica_candidates.clone())
        }
    }

    fn sifs_toolchain_store<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
    ) -> SifsSandboxToolchainStore<T> {
        let store = SifsSandboxToolchainStore::new(dfs);
        if self.replica_candidates.is_empty() {
            store
        } else {
            store.with_replica_candidates(self.replica_candidates.clone())
        }
    }

    fn sifs_filesystem<T: NetworkTransport>(
        &self,
        dfs: NetworkedDfsService<T>,
        manifest: &SandboxInstanceManifest,
    ) -> SifsSandboxFileSystem<T> {
        let fs = SifsSandboxFileSystem::new(dfs, manifest);
        if self.replica_candidates.is_empty() {
            fs
        } else {
            fs.with_replica_candidates(self.replica_candidates.clone())
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_toolchain_command_with_stdin_policy_and_output_sink<
        T: NetworkTransport + Send + 'static,
    >(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxWasixSifsCommandOutcome<T>, SandboxCommandExecutionError> {
        let (dfs, execution_node_id, _manifest, local_toolchain_store) =
            self.materialize_sifs_execution(dfs, local_store, sandbox_id)?;
        let command = self
            .local_runner
            .run_toolchain_command_with_stdin_policy_and_output_sink(
                local_store,
                &local_toolchain_store,
                sandbox_id,
                command_name,
                argv,
                stdin,
                timeout_ms,
                &execution_node_id,
                policy,
                output_sink,
            );
        let dfs = self.reconcile_sifs_execution(dfs, local_store, sandbox_id)?;
        Ok(SandboxWasixSifsCommandOutcome { dfs, command })
    }

    pub fn run_toolchain_process_plan_with_policy_and_output_sink<
        T: NetworkTransport + Send + 'static,
    >(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        plan: &SandboxProcessPlan,
        timeout_ms: u64,
        policy: &SandboxPolicy,
        output_sink: Option<Arc<dyn SandboxOutputSink>>,
    ) -> Result<SandboxWasixSifsProcessPlanOutcome<T>, SandboxCommandExecutionError> {
        let (dfs, execution_node_id, _manifest, local_toolchain_store) =
            self.materialize_sifs_execution(dfs, local_store, sandbox_id)?;
        let process = self
            .local_runner
            .run_toolchain_process_plan_with_policy_and_output_sink(
                local_store,
                &local_toolchain_store,
                sandbox_id,
                plan,
                timeout_ms,
                &execution_node_id,
                policy,
                output_sink,
            );
        let dfs = self.reconcile_sifs_execution(dfs, local_store, sandbox_id)?;
        Ok(SandboxWasixSifsProcessPlanOutcome { dfs, process })
    }

    fn materialize_sifs_execution<T: NetworkTransport + Send + 'static>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
    ) -> Result<
        (
            NetworkedDfsService<T>,
            sinomo_node::NodeId,
            SandboxInstanceManifest,
            LocalSandboxToolchainStore,
        ),
        SandboxCommandExecutionError,
    > {
        let mut sifs_store = self.sifs_instance_store(dfs);
        let execution_node_id = sifs_store.dfs.node_id().clone();
        let mut manifest = sifs_store.load(sandbox_id)?;
        manifest.last_placement = Some(sandbox_execution_placement_for_node(
            &manifest,
            &execution_node_id,
        )?);
        sifs_store.save_manifest(&manifest)?;
        let mut sifs_fs = self.sifs_filesystem(sifs_store.into_inner(), &manifest);
        sifs_fs.ensure_workspace()?;
        ensure_local_instance_from_manifest(local_store, &manifest)?;
        let (_local_manifest, local_layout) = local_store.load(sandbox_id)?;
        let mut local_fs = LocalSandboxFileSystem::new(local_layout);
        sync_sandbox_workspace_roots(&mut sifs_fs, &mut local_fs)?;
        let dfs = sifs_fs.into_inner();

        let mut sifs_toolchain_store = self.sifs_toolchain_store(dfs);
        let toolchain = sifs_toolchain_store.load_manifest(&manifest.toolchain_id)?;
        let toolchain_artifacts = sifs_toolchain_store.load_artifacts(&toolchain)?;
        let toolchain_commands = toolchain
            .commands
            .keys()
            .map(|command_name| {
                sifs_toolchain_store
                    .load_command_module(&toolchain, command_name)
                    .map(|bytes| (command_name.clone(), bytes))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let dfs = sifs_toolchain_store.into_inner();
        let local_toolchain_store = local_sifs_toolchain_store(local_store, sandbox_id);
        write_resolved_toolchain_to_local_store(
            &local_toolchain_store,
            &toolchain,
            &toolchain_artifacts,
            &toolchain_commands,
        )?;
        Ok((dfs, execution_node_id, manifest, local_toolchain_store))
    }

    fn reconcile_sifs_execution<T: NetworkTransport + Send + 'static>(
        &self,
        dfs: NetworkedDfsService<T>,
        local_store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
    ) -> Result<NetworkedDfsService<T>, SandboxCommandExecutionError> {
        let (local_manifest, local_layout) = local_store.load(sandbox_id)?;
        let mut local_fs = LocalSandboxFileSystem::new(local_layout);
        let mut sifs_fs = self.sifs_filesystem(dfs, &local_manifest);
        sifs_fs.ensure_workspace()?;
        sync_sandbox_workspace_roots(&mut local_fs, &mut sifs_fs)?;
        let mut sifs_store = self.sifs_instance_store(sifs_fs.into_inner());
        sifs_store.save_manifest(&local_manifest)?;
        Ok(sifs_store.into_inner())
    }
}

pub fn run_sandbox_wasm_module_replay_fixture_with_active_module(
    data_dir_root: impl AsRef<Path>,
    fixture: &sinomo_module::ModuleReplayFixture,
) -> Result<sinomo_module::ModuleReplayReport, SandboxCommandExecutionError> {
    let runner = SandboxWasixModuleCommandRunner::from_active_module(data_dir_root.as_ref())?;
    let response = runner
        .module
        .invoke(&SandboxWasixNativeRequest::RunReplayFixture(
            SandboxWasixRunReplayFixtureRequest {
                data_dir_root: data_dir_root.as_ref().to_path_buf(),
                fixture: fixture.clone(),
            },
        ))
        .map_err(wasix_native_module_error_to_wasi)?;
    match response {
        SandboxWasixNativeResponse::RunReplayFixture(response) => Ok(response.report),
        SandboxWasixNativeResponse::Error(error) => Err(wasix_native_error_to_wasi(error).into()),
        other => Err(SandboxWasiError::Runtime {
            detail: format!("WASIX module returned unexpected replay response {other:?}"),
        }
        .into()),
    }
}

fn load_active_sandbox_wasix_module(
    data_dir_root: &Path,
) -> Result<SandboxWasixNativeModule, SandboxCommandExecutionError> {
    let module_id =
        ModuleId::from_legacy_named(SINOMO_SANDBOX_WASIX_MODULE_ID).map_err(|source| {
            SandboxWasiError::Runtime {
                detail: format!("invalid WASIX module id: {source}"),
            }
        })?;
    let state = read_native_dylib_module_loader_state(data_dir_root, &module_id)
        .map_err(|source| SandboxWasiError::Runtime {
            detail: format!("read WASIX native module loader state: {source}"),
        })?
        .ok_or_else(|| SandboxWasiError::Runtime {
            detail: format!(
                "WASIX native module is not active; install and activate `{SINOMO_SANDBOX_WASIX_MODULE_ID}`"
            ),
        })?;
    validate_active_wasix_state(&state)?;
    let library_path = native_module_cache_path(
        data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    unsafe {
        SandboxWasixNativeModule::load_from_content_addressed_path(
            &library_path,
            &state.content_hash,
        )
    }
    .map_err(wasix_native_module_error_to_wasi)
    .map_err(SandboxCommandExecutionError::from)
}

fn validate_active_wasix_state(
    state: &NativeDylibModuleLoaderState,
) -> Result<(), SandboxCommandExecutionError> {
    let expected_module_id =
        ModuleId::from_legacy_named(SINOMO_SANDBOX_WASIX_MODULE_ID).map_err(|source| {
            SandboxWasiError::Runtime {
                detail: format!("invalid WASIX module id: {source}"),
            }
        })?;
    if state.module_id != expected_module_id {
        return Err(SandboxWasiError::Runtime {
            detail: format!(
                "WASIX native module loader state points to {}, expected {}",
                state.module_id, expected_module_id
            ),
        }
        .into());
    }
    if state.native_abi_version != SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION {
        return Err(SandboxWasiError::Runtime {
            detail: format!(
                "WASIX native module ABI mismatch: expected {}, got {}",
                SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION, state.native_abi_version
            ),
        }
        .into());
    }
    if state.interface_version != SINOMO_SANDBOX_WASIX_INTERFACE_VERSION {
        return Err(SandboxWasiError::Runtime {
            detail: format!(
                "WASIX native module interface mismatch: expected {}, got {}",
                SINOMO_SANDBOX_WASIX_INTERFACE_VERSION, state.interface_version
            ),
        }
        .into());
    }
    Ok(())
}

fn local_staged_sandbox_wasix_options(
    layout: &LocalSandboxInstanceLayout,
    limits: &SandboxResourceLimits,
    extra_preopens: Vec<SandboxWasixHostPreopen>,
) -> SandboxWasixRunModuleOptions {
    let mut preopened_dirs = vec![
        local_staged_sandbox_preopen(&layout.root_dir, "/root", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.home, "/home", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.tmp, "/tmp", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.work, "/work", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.var, "/var", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.logs, "/logs", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.artifacts, "/artifacts", SandboxMountMode::ReadWrite),
    ];
    preopened_dirs.extend(extra_preopens);
    SandboxWasixRunModuleOptions {
        preopened_dirs,
        enable_virtual_devices: true,
        zero_device_bytes: SANDBOX_WASI_ZERO_DEVICE_BYTES as u64,
        memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024),
        input_limit_bytes: limits.max_input_bytes,
        output_limit_bytes: limits.max_output_bytes,
        workspace_limit_bytes: limits.max_workspace_bytes,
        file_count_limit: limits.max_file_count,
        ..SandboxWasixRunModuleOptions::default()
    }
}

fn local_staged_sandbox_preopen(
    host_path: &Path,
    guest_path: &str,
    mode: SandboxMountMode,
) -> SandboxWasixHostPreopen {
    SandboxWasixHostPreopen {
        host_path: host_path.to_path_buf(),
        guest_path: SandboxPath::new(guest_path).expect("static sandbox path"),
        mode,
    }
}

fn load_local_toolchain_artifacts(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .artifacts
        .keys()
        .map(|relative_path| {
            store
                .load_artifact(manifest, relative_path)
                .map(|bytes| (relative_path.clone(), bytes))
        })
        .collect()
}

fn load_local_toolchain_commands(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .commands
        .keys()
        .map(|command_name| {
            store
                .load_command_module(manifest, command_name)
                .map(|bytes| (command_name.clone(), bytes))
        })
        .collect()
}

fn stage_toolchain_runtime_to_local(
    layout: &LocalSandboxInstanceLayout,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<Vec<SandboxWasixHostPreopen>, SandboxCommandExecutionError> {
    let toolchain_root = ensure_local_toolchain_runtime_cache(layout, artifacts, commands)?;
    let mut preopens = Vec::new();
    if toolchain_root.exists() {
        let mut entries = fs::read_dir(&toolchain_root)
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "list toolchain artifact stage {}: {source}",
                    toolchain_root.display()
                ),
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "read toolchain artifact stage {}: {source}",
                    toolchain_root.display()
                ),
            })?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let file_type = entry
                .file_type()
                .map_err(|source| SandboxFsError::Unavailable {
                    reason: format!(
                        "inspect toolchain artifact stage entry {}: {source}",
                        entry.path().display()
                    ),
                })?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            validate_toolchain_stage_top_level_name(&name)?;
            preopens.push(SandboxWasixHostPreopen {
                host_path: entry.path(),
                guest_path: SandboxPath::new(format!("/{name}")).map_err(|source| {
                    SandboxFsError::Unavailable {
                        reason: format!(
                            "invalid staged toolchain top-level path `{name}`: {source}"
                        ),
                    }
                })?,
                mode: SandboxMountMode::ReadOnly,
            });
        }
    }
    Ok(preopens)
}

fn ensure_local_toolchain_runtime_cache(
    layout: &LocalSandboxInstanceLayout,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<PathBuf, SandboxCommandExecutionError> {
    let cache_root = layout.toolchain_runtime_cache_root();
    let cache_key = local_toolchain_runtime_cache_key(artifacts, commands)?;
    let cache_manifest = local_toolchain_runtime_cache_manifest(&cache_key, artifacts, commands)?;
    let toolchain_root = cache_root.join(&cache_key);
    let complete_marker = toolchain_root.join(".complete");
    if complete_marker.is_file()
        && local_toolchain_runtime_cache_is_complete(
            &toolchain_root,
            &cache_manifest,
            artifacts,
            commands,
        )?
    {
        return Ok(toolchain_root);
    }

    fs::create_dir_all(&cache_root).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "create toolchain runtime cache {}: {source}",
            cache_root.display()
        ),
    })?;

    if toolchain_root.exists() {
        reject_symlink_chain(
            &toolchain_root,
            &SandboxPath::new("/toolchain").expect("static path"),
        )?;
        fs::remove_dir_all(&toolchain_root).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "clear incomplete toolchain runtime cache {}: {source}",
                toolchain_root.display()
            ),
        })?;
    }

    let temp_root = cache_root.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    if temp_root.exists() {
        fs::remove_dir_all(&temp_root).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "clear temporary toolchain runtime cache {}: {source}",
                temp_root.display()
            ),
        })?;
    }
    fs::create_dir_all(&temp_root).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "create temporary toolchain runtime cache {}: {source}",
            temp_root.display()
        ),
    })?;

    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        let host_path = temp_root.join("bin").join(command_name);
        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "create toolchain command cache parent {}: {source}",
                    parent.display()
                ),
            })?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain command cache {}: {source}",
                host_path.display()
            ),
        })?;
        set_staged_toolchain_command_executable(&host_path)?;
    }
    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        let host_path = temp_root.join(&relative_path);
        if let Some(parent) = host_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "create toolchain artifact cache parent {}: {source}",
                    parent.display()
                ),
            })?;
        }
        if host_path.exists() {
            fs::remove_file(&host_path).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "replace toolchain artifact cache {}: {source}",
                    host_path.display()
                ),
            })?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain artifact cache {}: {source}",
                host_path.display()
            ),
        })?;
        if relative_path.starts_with("bin/") {
            set_staged_toolchain_command_executable(&host_path)?;
        }
    }
    fs::write(temp_root.join(".complete"), &cache_manifest).map_err(|source| {
        SandboxFsError::Unavailable {
            reason: format!(
                "write toolchain runtime cache marker {}: {source}",
                temp_root.join(".complete").display()
            ),
        }
    })?;

    match fs::rename(&temp_root, &toolchain_root) {
        Ok(()) => Ok(toolchain_root),
        Err(source) if toolchain_root.join(".complete").is_file() => {
            let _ = fs::remove_dir_all(&temp_root);
            let _ = source;
            Ok(toolchain_root)
        }
        Err(source) => Err(SandboxFsError::Unavailable {
            reason: format!(
                "publish toolchain runtime cache {}: {source}",
                toolchain_root.display()
            ),
        }
        .into()),
    }
}

fn local_toolchain_runtime_cache_is_complete(
    toolchain_root: &Path,
    expected_manifest: &str,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<bool, SandboxCommandExecutionError> {
    match fs::read_to_string(toolchain_root.join(".complete")) {
        Ok(actual_manifest) if actual_manifest == expected_manifest => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "read toolchain runtime cache marker {}: {source}",
                    toolchain_root.join(".complete").display()
                ),
            }
            .into());
        }
    }

    let mut expected_files = BTreeMap::new();
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        expected_files.insert(format!("bin/{command_name}"), bytes.len() as u64);
        if !local_toolchain_runtime_cache_file_matches(
            &toolchain_root.join("bin").join(command_name),
            bytes.len() as u64,
        )? {
            return Ok(false);
        }
    }
    for (relative_path, bytes) in artifacts {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        expected_files.insert(relative_path.clone(), bytes.len() as u64);
        if !local_toolchain_runtime_cache_file_matches(
            &toolchain_root.join(&relative_path),
            bytes.len() as u64,
        )? {
            return Ok(false);
        }
    }

    let mut actual_files = BTreeSet::new();
    if !collect_local_toolchain_runtime_cache_files(
        toolchain_root,
        toolchain_root,
        &mut actual_files,
    )? {
        return Ok(false);
    }
    Ok(actual_files == expected_files.keys().cloned().collect::<BTreeSet<_>>())
}

fn local_toolchain_runtime_cache_manifest(
    cache_key: &str,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<String, SandboxCommandExecutionError> {
    let mut manifest = String::new();
    manifest.push_str("sinomo-toolchain-runtime-cache-v1\n");
    manifest.push_str("key=");
    manifest.push_str(cache_key);
    manifest.push('\n');

    let mut artifact_entries = Vec::with_capacity(artifacts.len());
    for (relative_path, bytes) in artifacts {
        artifact_entries.push((
            validate_toolchain_artifact_relative_path(relative_path)?,
            bytes.len(),
            sandbox_module_hash(bytes),
        ));
    }
    artifact_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, len, hash) in artifact_entries {
        manifest.push_str("artifact\t");
        manifest.push_str(&relative_path);
        manifest.push('\t');
        manifest.push_str(&len.to_string());
        manifest.push('\t');
        manifest.push_str(&hash);
        manifest.push('\n');
    }

    let mut command_entries = Vec::with_capacity(commands.len());
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        command_entries.push((
            command_name.clone(),
            bytes.len(),
            sandbox_module_hash(bytes),
        ));
    }
    command_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (command_name, len, hash) in command_entries {
        manifest.push_str("command\t");
        manifest.push_str(&command_name);
        manifest.push('\t');
        manifest.push_str(&len.to_string());
        manifest.push('\t');
        manifest.push_str(&hash);
        manifest.push('\n');
    }

    Ok(manifest)
}

fn collect_local_toolchain_runtime_cache_files(
    root: &Path,
    path: &Path,
    files: &mut BTreeSet<String>,
) -> Result<bool, SandboxCommandExecutionError> {
    let mut entry_count = 0usize;
    for entry in fs::read_dir(path).map_err(|source| SandboxFsError::Unavailable {
        reason: format!("list toolchain runtime cache {}: {source}", path.display()),
    })? {
        let entry = entry.map_err(|source| SandboxFsError::Unavailable {
            reason: format!("read toolchain runtime cache {}: {source}", path.display()),
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if path == root && name == ".complete" {
            continue;
        }
        entry_count += 1;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "inspect toolchain runtime cache entry {}: {source}",
                    entry.path().display()
                ),
            })?;
        if metadata.file_type().is_symlink() {
            return Err(SandboxFsError::Path(SandboxPathError::EscapesRoot {
                path: entry.path().display().to_string(),
            })
            .into());
        }
        if metadata.is_dir() {
            if !collect_local_toolchain_runtime_cache_files(root, &entry.path(), files)? {
                return Ok(false);
            }
            continue;
        }
        if !metadata.is_file() {
            return Err(SandboxFsError::Unsupported {
                operation: format!(
                    "inspect special toolchain runtime cache entry {}",
                    entry.path().display()
                ),
            }
            .into());
        }
        let relative_path = entry
            .path()
            .strip_prefix(root)
            .map_err(|source| SandboxFsError::Unavailable {
                reason: format!(
                    "derive toolchain runtime cache relative path {}: {source}",
                    entry.path().display()
                ),
            })?
            .to_string_lossy()
            .replace('\\', "/");
        files.insert(relative_path);
    }
    Ok(path == root || entry_count > 0)
}

fn local_toolchain_runtime_cache_file_matches(
    path: &Path,
    expected_len: u64,
) -> Result<bool, SandboxCommandExecutionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(SandboxFsError::Unavailable {
                reason: format!(
                    "inspect toolchain runtime cache file {}: {source}",
                    path.display()
                ),
            }
            .into());
        }
    };
    Ok(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() == expected_len)
}

fn local_toolchain_runtime_cache_key(
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<String, SandboxCommandExecutionError> {
    let mut hasher = SandboxModuleHasher::new();
    hasher.update(b"sinomo-toolchain-runtime-cache-v1\n");
    let mut artifact_entries = Vec::with_capacity(artifacts.len());
    for (relative_path, bytes) in artifacts {
        artifact_entries.push((
            validate_toolchain_artifact_relative_path(relative_path)?,
            bytes,
        ));
    }
    artifact_entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, bytes) in artifact_entries {
        hasher.update(b"artifact\0");
        hasher.update(relative_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes);
        hasher.update(b"\n");
    }

    let mut command_entries = Vec::with_capacity(commands.len());
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        command_entries.push((command_name, bytes));
    }
    command_entries.sort_by(|left, right| left.0.cmp(right.0));
    for (command_name, bytes) in command_entries {
        hasher.update(b"command\0");
        hasher.update(command_name.as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(bytes);
        hasher.update(b"\n");
    }
    Ok(hasher
        .finalize()
        .strip_prefix("blake3:")
        .unwrap_or("invalid")
        .to_string())
}

fn validate_toolchain_stage_top_level_name(name: &str) -> Result<(), SandboxCommandExecutionError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(SandboxFsError::Unavailable {
            reason: format!("invalid staged toolchain top-level path `{name}`"),
        }
        .into());
    }
    if matches!(
        name,
        "artifacts"
            | "dev"
            | "etc"
            | "home"
            | "logs"
            | "opt"
            | "proc"
            | "root"
            | "run"
            | "sys"
            | "tmp"
            | "var"
            | "work"
    ) {
        return Err(SandboxFsError::Unavailable {
            reason: format!(
                "staged toolchain top-level path `{name}` would shadow a sandbox filesystem mount"
            ),
        }
        .into());
    }
    Ok(())
}

#[cfg(unix)]
fn set_staged_toolchain_command_executable(
    host_path: &Path,
) -> Result<(), SandboxCommandExecutionError> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(host_path)
        .map_err(|source| SandboxFsError::Unavailable {
            reason: format!(
                "inspect staged toolchain command {}: {source}",
                host_path.display()
            ),
        })?
        .permissions();
    permissions.set_mode(0o555);
    fs::set_permissions(host_path, permissions).map_err(|source| SandboxFsError::Unavailable {
        reason: format!(
            "mark staged toolchain command executable {}: {source}",
            host_path.display()
        ),
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn set_staged_toolchain_command_executable(
    _host_path: &Path,
) -> Result<(), SandboxCommandExecutionError> {
    Ok(())
}

fn wasix_command_overrides_for_toolchain(
    toolchain: &SandboxToolchainManifest,
) -> Vec<SandboxWasixCommandOverride> {
    toolchain
        .commands
        .values()
        .filter(|command| {
            command.effective_target_abi(toolchain) == SandboxWasiAbi::Wasix
                && (!command.argv_prefix.is_empty() || command.exec_name.is_some())
        })
        .map(|command| SandboxWasixCommandOverride {
            name: command.name.clone(),
            argv_prefix: command.argv_prefix.clone(),
            exec_name: command.exec_name.clone(),
        })
        .collect()
}

fn ensure_local_instance_from_manifest(
    local_store: &LocalSandboxInstanceStore,
    manifest: &SandboxInstanceManifest,
) -> Result<LocalSandboxInstanceLayout, SandboxCommandExecutionError> {
    let layout = local_store.layout(&manifest.sandbox_id);
    for dir in layout.required_dirs() {
        fs::create_dir_all(dir).map_err(|source| SandboxStoreError::Io {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    local_store.save_manifest(manifest).map_err(Into::into)
}

fn local_sifs_toolchain_store(
    local_store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
) -> LocalSandboxToolchainStore {
    let layout = local_store.layout(sandbox_id);
    LocalSandboxToolchainStore::new(layout.runtime_root.join("sifs-toolchain-cache"))
}

fn write_resolved_toolchain_to_local_store(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
    artifacts: &[(String, Vec<u8>)],
    commands: &[(String, Vec<u8>)],
) -> Result<(), SandboxCommandExecutionError> {
    let layout = store.save_manifest(manifest)?;
    for (command_name, bytes) in commands {
        validate_toolchain_command_name(command_name)?;
        let path = layout.command_module(command_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxToolchainError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&path, bytes).map_err(|source| SandboxToolchainError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    for (relative_path, bytes) in artifacts {
        let path = layout.artifact(relative_path)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxToolchainError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&path, bytes).map_err(|source| SandboxToolchainError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

fn wasix_native_module_error_to_wasi(error: SandboxWasixNativeModuleError) -> SandboxWasiError {
    SandboxWasiError::Runtime {
        detail: error.to_string(),
    }
}

fn wasix_native_error_to_wasi(error: SandboxWasixNativeErrorResponse) -> SandboxWasiError {
    SandboxWasiError::Runtime {
        detail: format!("{}: {}", error.code, error.detail),
    }
}
