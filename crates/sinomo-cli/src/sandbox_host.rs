use super::*;
#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;
#[cfg(feature = "sandbox")]
use std::path::Path;
use std::{sync::Arc, time::Duration};

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
    fn read_cluster_kv_local_snapshot_for_test(
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
    fn commit_cluster_kv_request_for_test(
        &self,
        data_dir: &sinomo_runtime::local::SinomoDataDir,
        request: &sinomo_runtime::kv::KvTxnRequest,
    ) -> Option<
        Result<
            sinomo_runtime::kv::ClusterKvTxnCommitResult,
            sinomo_runtime::sandbox::SandboxCliError,
        >,
    > {
        let paths = sinomo_runtime::network::ClusterPaths::new(data_dir);
        let result = (|| {
            let node_id = sinomo_runtime::cluster::read_cluster_state_without_kv(&paths)
                .map(|state| state.local_node_id)
                .unwrap_or_else(|_| {
                    sinomo_runtime::node::NodeId::from_legacy_named("sandbox_cli_test")
                        .expect("valid sandbox CLI test node id")
                });
            let kv_paths = paths.kv_paths();
            let replayed_committed_request =
                sinomo_runtime::kv::read_cluster_kv_store_for_node(&kv_paths, node_id.clone())
                    .ok()
                    .and_then(|store| store.committed_request(&request.request_id).cloned())
                    .is_some();
            let raft_store = kv_paths.raft_store();
            let commit = raft_store
                .append_request_with_response(&node_id, request.clone().into_command_request())?;
            raft_store.publish_commit_result(
                &commit,
                sinomo_runtime::kv::cluster_kv_key_can_defer_materialized_snapshot,
            )?;
            Ok(sinomo_runtime::kv::ClusterKvTxnCommitResult {
                request_id: request.request_id.clone(),
                command_digest: commit.committed_request.command_digest,
                snapshot: commit.snapshot,
                txn_response: commit.txn_response,
                authority: sinomo_runtime::kv::KvAuthorityProof {
                    term: commit.committed_request.term,
                    commit_index: commit.committed_request.log_index,
                    revision: commit.committed_request.revision,
                    basis: if replayed_committed_request {
                        sinomo_runtime::kv::KvAuthorityBasis::ExactCommittedRequest
                    } else {
                        sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        }
                    },
                },
                replayed_committed_request,
            })
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
        active_sandbox_executor_runner()?
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
        active_sandbox_executor_runner()?.run_toolchain_command_with_stdin_policy_and_output_sink(
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
        active_sandbox_executor_runner()?.run_toolchain_process_plan_with_policy_and_output_sink(
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
        active_sandbox_executor_runner()?.verify_busybox_toolchain_fidelity(
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
        active_sandbox_executor_runner()?
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
        sinomo_runtime::sandbox::run_sandbox_wasm_module_replay_fixture_with_active_module(
            data_dir_root,
            fixture,
        )
        .map_err(|source| sinomo_runtime::sandbox::SandboxCliError::Parse {
            detail: format!("sandbox module replay failed: {source}"),
        })
    }
}

#[cfg(feature = "sandbox")]
fn active_sandbox_executor_runner() -> Result<
    sinomo_runtime::sandbox::SandboxExecutorCommandRunner,
    sinomo_runtime::sandbox::SandboxCommandExecutionError,
> {
    #[cfg(test)]
    if let Some(module_path) = std::env::var_os("SINOMO_SANDBOX_EXECUTOR_NATIVE_MODULE_PATH") {
        // SAFETY: this opt-in test path is explicitly supplied as a trusted
        // native artifact by the test operator. Descriptor validation still
        // requires the generic sandbox-executor interface before invocation.
        return unsafe {
            sinomo_runtime::sandbox::SandboxExecutorCommandRunner::from_verified_module_path(
                module_path,
            )
        };
    }
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
    sinomo_runtime::sandbox::SandboxExecutorCommandRunner::from_active_module(data_dir.root())
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
        sinomo_runtime::daemon::DaemonError::NetworkDelivery {
            address, detail, ..
        } => sinomo_runtime::sandbox::SandboxCliError::Network { address, detail },
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::sandbox::SandboxCliHost;

    #[test]
    fn sandbox_kv_test_hook_preserves_request_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-sandbox-kv-request");
        let data_dir = SinomoDataDir::new(root.path());
        let paths = sinomo_runtime::network::ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let request = sinomo_runtime::kv::KvTxnRequest::generated(sinomo_runtime::kv::KvTxn {
            compares: Vec::new(),
            success: vec![sinomo_runtime::kv::KvOp::Put {
                key: "/config/sandbox/test".to_string(),
                value: b"one".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        });
        let host = SinomoCliSandboxHost;

        let first = host
            .commit_cluster_kv_request_for_test(&data_dir, &request)
            .expect("test hook")
            .expect("first commit");
        let replay = host
            .commit_cluster_kv_request_for_test(&data_dir, &request)
            .expect("test hook")
            .expect("exact retry");

        assert!(!first.replayed_committed_request);
        assert!(replay.replayed_committed_request);
        assert_eq!(first.snapshot.revision, replay.snapshot.revision);
        assert_eq!(
            replay.authority.basis,
            sinomo_runtime::kv::KvAuthorityBasis::ExactCommittedRequest
        );

        let conflicting = sinomo_runtime::kv::KvTxnRequest::new(
            request.request_id,
            sinomo_runtime::kv::KvTxn {
                compares: Vec::new(),
                success: vec![sinomo_runtime::kv::KvOp::Put {
                    key: "/config/sandbox/test".to_string(),
                    value: b"two".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        );
        assert!(
            host.commit_cluster_kv_request_for_test(&data_dir, &conflicting)
                .expect("test hook")
                .is_err()
        );
    }
}
