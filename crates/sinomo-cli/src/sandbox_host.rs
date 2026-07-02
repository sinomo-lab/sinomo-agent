use super::*;
#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;
use std::{path::Path, sync::Arc, time::Duration};

#[derive(Debug, Default)]
pub(crate) struct SinomoCliSandboxHost;

impl sinomo_runtime::sandbox::SandboxCliHost for SinomoCliSandboxHost {
    fn force_local_mode(&self) -> bool {
        cli_force_local_mode()
    }

    fn default_main_daemon_address(
        &self,
    ) -> Result<Option<String>, sinomo_runtime::sandbox::SandboxCliError> {
        default_main_daemon_status()
            .map(|status| status.map(|status| status.address))
            .map_err(sandbox_cli_error_from_cli)
    }

    fn send_daemon_rpc(
        &self,
        address: &str,
        request: &sinomo_runtime::network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<sinomo_runtime::network::ObjectRpcResponse, sinomo_runtime::sandbox::SandboxCliError>
    {
        sinomo_runtime::daemon::send_daemon_rpc(address, request, timeout)
            .map_err(sandbox_cli_error_from_daemon)
    }

    fn send_daemon_rpc_stream(
        &self,
        address: &str,
        request: &sinomo_runtime::network::ObjectRpcRequest,
        timeout: Duration,
        on_response: &mut dyn FnMut(
            sinomo_runtime::network::ObjectRpcResponse,
        )
            -> Result<bool, sinomo_runtime::sandbox::SandboxCliError>,
    ) -> Result<(), sinomo_runtime::sandbox::SandboxCliError> {
        sinomo_runtime::daemon::send_daemon_rpc_stream(address, request, timeout, |response| {
            on_response(response).map_err(daemon_error_from_sandbox_cli)
        })
        .map_err(sandbox_cli_error_from_daemon)
    }

    #[cfg(test)]
    fn read_cluster_kv_snapshot(
        &self,
        paths: &sinomo_runtime::network::ClusterPaths,
    ) -> Option<Result<sinomo_runtime::kv::KvSnapshot, sinomo_runtime::sandbox::SandboxCliError>>
    {
        Some(
            sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
                .map_err(sinomo_runtime::network::NetworkError::directory)
                .map_err(Into::into),
        )
    }

    #[cfg(test)]
    fn commit_cluster_kv_txn(
        &self,
        data_dir: &sinomo_runtime::local::SinomoDataDir,
        txn: &sinomo_runtime::kv::KvTxn,
    ) -> Option<Result<(), sinomo_runtime::sandbox::SandboxCliError>> {
        let paths = sinomo_runtime::network::ClusterPaths::new(data_dir);
        let result = (|| {
            let snapshot = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
                .unwrap_or_else(|_| {
                    sinomo_runtime::kv::InProcessKvStore::new()
                        .snapshot()
                        .expect("snapshot")
                });
            let mut store = sinomo_runtime::kv::InProcessKvStore::restore(snapshot)?;
            let _: sinomo_runtime::kv::KvTxnResponse = store.txn(txn.clone())?;
            sinomo_runtime::kv::write_cluster_kv_snapshot(
                &paths.kv_paths(),
                &store.snapshot().expect("snapshot"),
            )
            .map_err(sinomo_runtime::network::NetworkError::directory)
            .map_err(sinomo_runtime::network::NetworkError::directory)?;
            Ok(())
        })();
        Some(result)
    }

    #[cfg(feature = "sandbox")]
    fn run_wasix_module_with_stdin(
        &self,
        store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
        toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_runtime::sandbox::SandboxId,
        wasm: Vec<u8>,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        output_sink: Option<Arc<dyn sinomo_runtime::sandbox::SandboxOutputSink>>,
    ) -> Result<
        sinomo_runtime::sandbox::SandboxLocalCommandOutcome,
        sinomo_runtime::sandbox::SandboxCommandExecutionError,
    > {
        #[cfg(test)]
        {
            return sinomo_sandbox_wasix_module::LocalStagedSandboxCommandRunner::new()?
                .run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
                    store,
                    toolchain_store,
                    sandbox_id,
                    wasm,
                    argv,
                    stdin,
                    timeout_ms,
                    None,
                    Vec::new(),
                    output_sink,
                );
        }
        #[cfg(not(test))]
        active_wasix_runner()?.run_module_with_stdin_limits_preopens_output_sink_and_toolchain(
            store,
            toolchain_store,
            sandbox_id,
            wasm,
            argv,
            stdin,
            timeout_ms,
            None,
            Vec::new(),
            output_sink,
        )
    }

    #[cfg(feature = "sandbox")]
    fn run_wasix_toolchain_command(
        &self,
        store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
        toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_runtime::sandbox::SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &sinomo_runtime::sandbox::SandboxPolicy,
        output_sink: Option<Arc<dyn sinomo_runtime::sandbox::SandboxOutputSink>>,
    ) -> Result<
        sinomo_runtime::sandbox::SandboxLocalCommandOutcome,
        sinomo_runtime::sandbox::SandboxCommandExecutionError,
    > {
        #[cfg(test)]
        {
            return sinomo_sandbox_wasix_module::LocalStagedSandboxCommandRunner::new()?
                .run_toolchain_command_with_stdin_policy_and_output_sink(
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
                );
        }
        #[cfg(not(test))]
        active_wasix_runner()?.run_toolchain_command_with_stdin_policy_and_output_sink(
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
        )
    }

    #[cfg(feature = "sandbox")]
    fn run_wasix_process_plan(
        &self,
        store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
        toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_runtime::sandbox::SandboxId,
        plan: &sinomo_runtime::sandbox::SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &sinomo_runtime::sandbox::SandboxPolicy,
        output_sink: Option<Arc<dyn sinomo_runtime::sandbox::SandboxOutputSink>>,
    ) -> Result<
        sinomo_runtime::sandbox::SandboxProcessPlanOutcome,
        sinomo_runtime::sandbox::SandboxCommandExecutionError,
    > {
        #[cfg(test)]
        {
            return sinomo_sandbox_wasix_module::LocalStagedSandboxCommandRunner::new()?
                .run_toolchain_process_plan_with_policy_and_output_sink(
                    store,
                    toolchain_store,
                    sandbox_id,
                    plan,
                    timeout_ms,
                    execution_node_id,
                    policy,
                    output_sink,
                );
        }
        #[cfg(not(test))]
        active_wasix_runner()?.run_toolchain_process_plan_with_policy_and_output_sink(
            store,
            toolchain_store,
            sandbox_id,
            plan,
            timeout_ms,
            execution_node_id,
            policy,
            output_sink,
        )
    }

    fn run_simple_native_toolchain_command(
        &self,
        store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
        toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_runtime::sandbox::SandboxId,
        command_name: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &sinomo_runtime::sandbox::SandboxPolicy,
        output_sink: Option<Arc<dyn sinomo_runtime::sandbox::SandboxOutputSink>>,
    ) -> Result<
        sinomo_runtime::sandbox::SandboxLocalCommandOutcome,
        sinomo_runtime::sandbox::SandboxCommandExecutionError,
    > {
        sinomo_runtime::sandbox_simple::SandboxSimpleNativeCommandRunner::from_env()?
            .run_toolchain_command_with_stdin_policy_and_output_sink(
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
            )
    }

    fn run_simple_native_process_plan(
        &self,
        store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
        toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_runtime::sandbox::SandboxId,
        plan: &sinomo_runtime::sandbox::SandboxProcessPlan,
        timeout_ms: u64,
        execution_node_id: &NodeId,
        policy: &sinomo_runtime::sandbox::SandboxPolicy,
        output_sink: Option<Arc<dyn sinomo_runtime::sandbox::SandboxOutputSink>>,
    ) -> Result<
        sinomo_runtime::sandbox::SandboxProcessPlanOutcome,
        sinomo_runtime::sandbox::SandboxCommandExecutionError,
    > {
        sinomo_runtime::sandbox_simple::SandboxSimpleNativeCommandRunner::from_env()?
            .run_process_plan_with_policy_and_output_sink(
                store,
                toolchain_store,
                sandbox_id,
                plan,
                timeout_ms,
                execution_node_id,
                policy,
                output_sink,
            )
    }

    #[cfg(feature = "sandbox")]
    fn verify_busybox_toolchain_fidelity(
        &self,
        store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
        toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
        sandbox_id: &sinomo_runtime::sandbox::SandboxId,
        execution_node_id: &NodeId,
        policy: &sinomo_runtime::sandbox::SandboxPolicy,
        cases: &[sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase],
    ) -> Result<
        Vec<sinomo_runtime::sandbox::SandboxToolFidelityResult>,
        sinomo_runtime::sandbox::SandboxToolFidelityError,
    > {
        #[cfg(test)]
        {
            let runner = sinomo_sandbox_wasix_module::LocalStagedSandboxCommandRunner::new()
                .map_err(sinomo_runtime::sandbox::SandboxCommandExecutionError::from)?;
            return runner.verify_busybox_toolchain_fidelity(
                store,
                toolchain_store,
                sandbox_id,
                execution_node_id,
                policy,
                cases,
            );
        }
        #[cfg(not(test))]
        active_wasix_runner()?.verify_busybox_toolchain_fidelity(
            store,
            toolchain_store,
            sandbox_id,
            execution_node_id,
            policy,
            cases,
        )
    }

    #[cfg(feature = "sandbox")]
    fn validate_component_artifact_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<(), sinomo_runtime::sandbox::SandboxCliError> {
        #[cfg(test)]
        {
            return sinomo_sandbox_wasix_module::validate_sinomo_module_v1_component_contract(
                bytes,
            )
            .map_err(|source| sinomo_runtime::sandbox::SandboxCliError::Parse {
                detail: source.to_string(),
            });
        }
        #[cfg(not(test))]
        active_wasix_runner()?
            .validate_sinomo_module_v1_component_contract(bytes)
            .map_err(Into::into)
    }

    #[cfg(feature = "sandbox")]
    fn run_module_replay_fixture(
        &self,
        data_dir_root: &Path,
        fixture: &sinomo_runtime::module::ModuleReplayFixture,
    ) -> Result<sinomo_runtime::module::ModuleReplayReport, sinomo_runtime::sandbox::SandboxCliError>
    {
        #[cfg(test)]
        {
            return sinomo_sandbox_wasix_module::run_sandbox_wasm_module_replay_fixture(
                data_dir_root,
                fixture,
            )
            .map_err(|source| sinomo_runtime::sandbox::SandboxCliError::Parse {
                detail: format!("sandbox module replay failed: {source}"),
            });
        }
        #[cfg(not(test))]
        sinomo_runtime::sandbox_wasix_host::run_sandbox_wasm_module_replay_fixture_with_active_module(
            data_dir_root,
            fixture,
        )
        .map_err(|source| sinomo_runtime::sandbox::SandboxCliError::Parse {
            detail: format!("sandbox module replay failed: {source}"),
        })
    }
}

#[cfg(all(feature = "sandbox", not(test)))]
fn active_wasix_runner() -> Result<
    sinomo_runtime::sandbox_wasix_host::SandboxWasixModuleCommandRunner,
    sinomo_runtime::sandbox::SandboxCommandExecutionError,
> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(|error| {
        sinomo_runtime::sandbox::SandboxCommandExecutionError::Wasi(
            sinomo_runtime::sandbox::SandboxWasiError::Runtime {
                detail: error.to_string(),
            },
        )
    })?;
    data_dir.ensure_root_layout().map_err(|error| {
        sinomo_runtime::sandbox::SandboxCommandExecutionError::Wasi(
            sinomo_runtime::sandbox::SandboxWasiError::Runtime {
                detail: error.to_string(),
            },
        )
    })?;
    sinomo_runtime::sandbox_wasix_host::SandboxWasixModuleCommandRunner::from_active_module(
        data_dir.root(),
    )
    .map_err(|error| {
        sinomo_runtime::sandbox::SandboxCommandExecutionError::Wasi(
            sinomo_runtime::sandbox::SandboxWasiError::Runtime {
                detail: error.to_string(),
            },
        )
    })
}

fn sandbox_cli_error_from_cli(error: CliError) -> sinomo_runtime::sandbox::SandboxCliError {
    sinomo_runtime::sandbox::SandboxCliError::Parse {
        detail: error.to_string(),
    }
}

fn sandbox_cli_error_from_daemon(
    error: sinomo_runtime::daemon::DaemonError,
) -> sinomo_runtime::sandbox::SandboxCliError {
    match error {
        sinomo_runtime::daemon::DaemonError::AuthDenied => {
            sinomo_runtime::sandbox::SandboxCliError::AuthDenied
        }
        sinomo_runtime::daemon::DaemonError::AuthExpired => {
            sinomo_runtime::sandbox::SandboxCliError::AuthExpired
        }
        sinomo_runtime::daemon::DaemonError::Io { path, detail } => {
            sinomo_runtime::sandbox::SandboxCliError::Io { path, detail }
        }
        sinomo_runtime::daemon::DaemonError::Json { path, detail } => {
            sinomo_runtime::sandbox::SandboxCliError::Json { path, detail }
        }
        sinomo_runtime::daemon::DaemonError::Parse { detail } => {
            sinomo_runtime::sandbox::SandboxCliError::Parse { detail }
        }
        sinomo_runtime::daemon::DaemonError::Network { address, detail } => {
            sinomo_runtime::sandbox::SandboxCliError::Network { address, detail }
        }
        sinomo_runtime::daemon::DaemonError::AlreadyRunning { lock_file, detail } => {
            sinomo_runtime::sandbox::SandboxCliError::DaemonAlreadyRunning { lock_file, detail }
        }
        sinomo_runtime::daemon::DaemonError::NotRunning { detail } => {
            sinomo_runtime::sandbox::SandboxCliError::DaemonNotRunning { detail }
        }
        sinomo_runtime::daemon::DaemonError::ClusterOrphan => {
            sinomo_runtime::sandbox::SandboxCliError::ClusterOrphan
        }
        sinomo_runtime::daemon::DaemonError::ClusterReadOnly { detail } => {
            sinomo_runtime::sandbox::SandboxCliError::ClusterReadOnly { detail }
        }
    }
}

fn daemon_error_from_sandbox_cli(
    error: sinomo_runtime::sandbox::SandboxCliError,
) -> sinomo_runtime::daemon::DaemonError {
    sinomo_runtime::daemon::DaemonError::Parse {
        detail: error.to_string(),
    }
}
