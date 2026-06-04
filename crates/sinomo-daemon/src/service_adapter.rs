//! Bridges the daemon's synchronous, thread-per-connection control loop to the async,
//! host-agnostic service seam (`sinomo-service`).
//!
//! Routes owned by the [`ServiceRegistry`] are served here; everything else falls
//! through to the daemon's own dispatch. Each client connection already runs on its own
//! thread, so concurrency across connections is preserved — this just `block_on`s the
//! dispatch future on a shared runtime and writes the buffered response frames to the
//! (blocking) control stream afterward.
//!
//! The registry mixes host-agnostic services from `sinomo-service` and native modules
//! such as `sinomo-rollout` with **daemon-foundational** services defined here
//! ([`DaemonControlService`], [`ClusterKvService`]) that own daemon state (their
//! [`DaemonPaths`], which encode whether this is the main or shadow daemon) and call
//! the daemon's own cluster/control handlers. Consuming services receive a real
//! [`DaemonClusterAccess`] via [`ServiceContext::cluster`] when the node is clustered.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

use sinomo_config::{
    ConfigRegistry, ConfigRegistryBuilder, register_services as register_config_services,
};
use sinomo_dfs::{
    DFS_PROTOCOL_FRAME_ROUTE, DFS_SMOKE_ROUTE, register_services as register_dfs_services,
};
use sinomo_kv::{KvEntry, KvTxn, KvTxnResponse};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    CLUSTER_KV_SNAPSHOT_ROUTE, CLUSTER_KV_TXN_ROUTE, CLUSTER_RAFT_APPEND_ROUTE,
    CLUSTER_RAFT_VOTE_ROUTE, ClusterPaths, ClusterRaftAppendRequest, ClusterRaftVoteRequest,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE, DaemonControlStream, ObjectRpcRequest,
    ObjectRpcResponse, SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE, SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
    read_cluster_kv_entry, read_cluster_kv_prefix_entries,
};
use sinomo_rollout::{
    ClusterRolloutBinaryResponse, ROLLOUT_BINARY_ROUTE, ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
    ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE, ROLLOUT_BUILD_ROUTE,
    register_services as register_rollout_services,
};
use sinomo_sandbox::{
    LocalSandboxBackend, SandboxAddToolchainArtifactRequest, SandboxAddToolchainArtifactResponse,
    SandboxAddToolchainCommandRequest, SandboxAddToolchainCommandResponse, SandboxBackend,
    SandboxBackendError, SandboxBackendFactory, SandboxCheckpointRequest,
    SandboxCheckpointResponse, SandboxCreateInstanceRequest, SandboxCreateInstanceResponse,
    SandboxDestroyRequest, SandboxDestroyResponse, SandboxHttpFetchFileResult,
    SandboxHttpFetchToFileRequest, SandboxHttpRequest, SandboxHttpResponse,
    SandboxImportBusyBoxToolchainRequest, SandboxImportBusyBoxToolchainResponse,
    SandboxImportToolchainPackageFromPathRequest, SandboxImportToolchainPackageRequest,
    SandboxImportToolchainPackageResponse, SandboxListRequest, SandboxListResponse,
    SandboxLocalCommandOutcome, SandboxProcessPlanOutcome, SandboxReadRequest, SandboxReadResponse,
    SandboxRebootRequest, SandboxRebootResponse, SandboxResumeRequest, SandboxResumeResponse,
    SandboxRunProcessPlanRequest, SandboxRunToolchainRequest, SandboxStatusRequest,
    SandboxStatusResponse, SandboxUpdateInstanceRequest, SandboxUpdateInstanceResponse,
    SandboxWriteRequest, SandboxWriteResponse,
};
use sinomo_service::{
    ClusterAccess, ResponseSink, Service, ServiceContext, ServiceDisposition, ServiceError,
    ServiceFuture, ServiceRegistry, ServiceRegistryBuilder, ServiceSigner,
};

use crate::auth::{AuthenticatedDaemonCommand, DaemonSignerAuthority};
use crate::cluster::{
    handle_daemon_cluster_kv_snapshot, handle_daemon_cluster_kv_txn_request,
    handle_daemon_cluster_raft_append_request, handle_daemon_cluster_raft_vote_request,
    handle_forwarded_daemon_cluster_kv_txn_request,
};
use crate::control::{
    ACCEPTED_ROLLOUT_RESPONSE_DRAIN_GRACE, accepted_rollout_should_stop_current_daemon,
    daemon_data_dir_path, daemon_status_response, handle_daemon_sandbox_checkpoint,
    handle_daemon_sandbox_create, handle_daemon_sandbox_destroy, handle_daemon_sandbox_http_fetch,
    handle_daemon_sandbox_http_fetch_to_file, handle_daemon_sandbox_import_busybox,
    handle_daemon_sandbox_list, handle_daemon_sandbox_read, handle_daemon_sandbox_reboot,
    handle_daemon_sandbox_resume, handle_daemon_sandbox_run_process_plan,
    handle_daemon_sandbox_run_process_plan_stream, handle_daemon_sandbox_run_toolchain,
    handle_daemon_sandbox_run_toolchain_stream, handle_daemon_sandbox_status,
    handle_daemon_sandbox_toolchain_add_artifact, handle_daemon_sandbox_toolchain_add_command,
    handle_daemon_sandbox_toolchain_import_package,
    handle_daemon_sandbox_toolchain_import_package_from_path, handle_daemon_sandbox_update,
    handle_daemon_sandbox_write, write_daemon_rpc_response_frame,
};
use crate::dfs::{handle_daemon_dfs_protocol_frame, run_daemon_dfs_smoke};
use crate::error::DaemonError;
use crate::paths::DaemonPaths;
use crate::rollout::{
    handle_daemon_rollout_binary_request, handle_daemon_rollout_binary_upload_chunk_request,
    handle_daemon_rollout_binary_upload_finalize_request, handle_daemon_rollout_build_request,
};

/// Shared executor for driving service futures from connection threads. A multi-thread
/// runtime so concurrent connection threads can `block_on` it. `enable_all` is required:
/// handlers may make synchronous daemon-to-daemon RPCs, which now drive their async I/O on
/// this ambient runtime (via `block_in_place`) instead of building a nested one, so the IO
/// and time drivers must be present.
static SERVICE_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build daemon service runtime")
});

fn daemon_error(error: DaemonError) -> ServiceError {
    ServiceError::Service(error.to_string())
}

/// Real cluster-KV access for services that consume it. Reads come from the local
/// cluster-KV snapshot; commits go through the daemon's quorum-replicated txn path
/// (forwarding to the leader when needed).
struct DaemonClusterAccess {
    paths: DaemonPaths,
    cluster_paths: ClusterPaths,
}

impl DaemonClusterAccess {
    fn new(paths: DaemonPaths, data_dir: &SinomoDataDir) -> Self {
        Self {
            cluster_paths: ClusterPaths::new(data_dir),
            paths,
        }
    }
}

impl ClusterAccess for DaemonClusterAccess {
    fn kv_get(&self, key: &str) -> Result<Option<KvEntry>, ServiceError> {
        read_cluster_kv_entry(&self.cluster_paths, key).map_err(ServiceError::from)
    }

    fn kv_range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, ServiceError> {
        read_cluster_kv_prefix_entries(&self.cluster_paths, prefix).map_err(ServiceError::from)
    }

    fn kv_commit(&self, txn: KvTxn) -> Result<KvTxnResponse, ServiceError> {
        handle_daemon_cluster_kv_txn_request(&self.paths, txn).map_err(daemon_error)
    }
}

/// Daemon-foundational service: daemon status + shutdown. Holds this daemon's paths so
/// status reflects the right (main/shadow) process; shutdown reports `Stop`.
struct DaemonControlService {
    paths: DaemonPaths,
}

impl Service for DaemonControlService {
    fn name(&self) -> &'static str {
        "daemon-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[DAEMON_STATUS_ROUTE, DAEMON_SHUTDOWN_ROUTE]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let (response, disposition) = match request.route.as_str() {
                DAEMON_STATUS_ROUTE => {
                    let _: () = request.decode_body()?;
                    let status = daemon_status_response(&paths).map_err(daemon_error)?;
                    (
                        ObjectRpcResponse::encode_body(DAEMON_STATUS_ROUTE, &status)?,
                        ServiceDisposition::Continue,
                    )
                }
                DAEMON_SHUTDOWN_ROUTE => {
                    let _: () = request.decode_body()?;
                    (
                        ObjectRpcResponse::encode_body(
                            DAEMON_SHUTDOWN_ROUTE,
                            &"sinomo daemon stopping".to_string(),
                        )?,
                        ServiceDisposition::Stop,
                    )
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(disposition)
        })
    }
}

/// Daemon-foundational service: the cluster KV control plane (snapshot + txn). A local
/// signer commits directly; a remote signer's txn is applied without re-forwarding,
/// matching the daemon's prior dispatch.
struct ClusterKvService {
    paths: DaemonPaths,
}

impl Service for ClusterKvService {
    fn name(&self) -> &'static str {
        "cluster-kv"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[CLUSTER_KV_SNAPSHOT_ROUTE, CLUSTER_KV_TXN_ROUTE]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let response = match request.route.as_str() {
                CLUSTER_KV_SNAPSHOT_ROUTE => {
                    let _: () = request.decode_body()?;
                    let snapshot =
                        handle_daemon_cluster_kv_snapshot(&paths).map_err(daemon_error)?;
                    ObjectRpcResponse::encode_body(CLUSTER_KV_SNAPSHOT_ROUTE, &snapshot)?
                }
                CLUSTER_KV_TXN_ROUTE => {
                    let txn: KvTxn = request.decode_body()?;
                    let result = if ctx.signer.is_local() {
                        handle_daemon_cluster_kv_txn_request(&paths, txn)
                    } else {
                        handle_forwarded_daemon_cluster_kv_txn_request(&paths, txn)
                    }
                    .map_err(daemon_error)?;
                    ObjectRpcResponse::encode_body(CLUSTER_KV_TXN_ROUTE, &result)?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Daemon-foundational service: cluster raft consensus (append + vote). These inbound
/// peer RPCs authorize the sender against cluster membership, so the service carries the
/// request's `AuthenticatedDaemonCommand` (captured when the per-request registry is
/// built) and hands it to the daemon's own consensus handlers.
struct ClusterRaftService {
    paths: DaemonPaths,
    authenticated: AuthenticatedDaemonCommand,
}

impl Service for ClusterRaftService {
    fn name(&self) -> &'static str {
        "cluster-raft"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_VOTE_ROUTE]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        let authenticated = self.authenticated.clone();
        Box::pin(async move {
            let response = match request.route.as_str() {
                CLUSTER_RAFT_APPEND_ROUTE => {
                    let append: ClusterRaftAppendRequest = request.decode_body()?;
                    let result =
                        handle_daemon_cluster_raft_append_request(&paths, append, &authenticated)
                            .map_err(daemon_error)?;
                    ObjectRpcResponse::encode_body(CLUSTER_RAFT_APPEND_ROUTE, &result)?
                }
                CLUSTER_RAFT_VOTE_ROUTE => {
                    let vote: ClusterRaftVoteRequest = request.decode_body()?;
                    let result =
                        handle_daemon_cluster_raft_vote_request(&paths, vote, &authenticated)
                            .map_err(daemon_error)?;
                    ObjectRpcResponse::encode_body(CLUSTER_RAFT_VOTE_ROUTE, &result)?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Daemon-foundational service: rollout binary staging + source build. An accepted
/// rollout on the main daemon must restart the process, reported via
/// `ServiceDisposition::Stop` (the adapter then drains and stops). A handler error is
/// reported as a `failed` response rather than propagated, matching the prior dispatch.
struct RolloutBinaryService {
    paths: DaemonPaths,
}

impl RolloutBinaryService {
    fn finish(
        paths: &DaemonPaths,
        route: &'static str,
        result: Result<ClusterRolloutBinaryResponse, DaemonError>,
    ) -> Result<(ObjectRpcResponse, ServiceDisposition), ServiceError> {
        let response = result.unwrap_or_else(|error| ClusterRolloutBinaryResponse {
            status: "failed".to_string(),
            detail: error.to_string(),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
        let disposition = if accepted_rollout_should_stop_current_daemon(paths, &response) {
            ServiceDisposition::Stop
        } else {
            ServiceDisposition::Continue
        };
        Ok((
            ObjectRpcResponse::encode_body(route, &response)?,
            disposition,
        ))
    }
}

impl Service for RolloutBinaryService {
    fn name(&self) -> &'static str {
        "rollout-binary"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            ROLLOUT_BINARY_ROUTE,
            ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
            ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
            ROLLOUT_BUILD_ROUTE,
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let (response, disposition) = match request.route.as_str() {
                ROLLOUT_BINARY_ROUTE => {
                    let payload = request.decode_body()?;
                    Self::finish(
                        &paths,
                        ROLLOUT_BINARY_ROUTE,
                        handle_daemon_rollout_binary_request(&paths, payload),
                    )?
                }
                ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE => {
                    let payload = request.decode_body()?;
                    let response =
                        handle_daemon_rollout_binary_upload_chunk_request(&paths, payload)
                            .map_err(daemon_error)?;
                    (
                        ObjectRpcResponse::encode_body(
                            ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
                            &response,
                        )?,
                        ServiceDisposition::Continue,
                    )
                }
                ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE => {
                    let payload = request.decode_body()?;
                    Self::finish(
                        &paths,
                        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
                        handle_daemon_rollout_binary_upload_finalize_request(&paths, payload),
                    )?
                }
                ROLLOUT_BUILD_ROUTE => {
                    let payload = request.decode_body()?;
                    Self::finish(
                        &paths,
                        ROLLOUT_BUILD_ROUTE,
                        handle_daemon_rollout_build_request(&paths, payload),
                    )?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(disposition)
        })
    }
}

/// Daemon-foundational service: the DFS data plane. `protocol-frame` processes an inbound
/// peer frame against this node's local DFS backend + cluster control plane; `smoke` runs
/// the cluster replication self-test, which reaches peers via the daemon's transport. Both
/// are inherently clustered, so they wrap the daemon's own data-plane handlers (which do
/// their own cluster access) rather than living in the host-agnostic `DfsService`.
struct DaemonDfsDataPlaneService {
    paths: DaemonPaths,
}

impl Service for DaemonDfsDataPlaneService {
    fn name(&self) -> &'static str {
        "dfs-data-plane"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[DFS_SMOKE_ROUTE, DFS_PROTOCOL_FRAME_ROUTE]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let paths = self.paths.clone();
        Box::pin(async move {
            let response = match request.route.as_str() {
                DFS_SMOKE_ROUTE => {
                    let _: () = request.decode_body()?;
                    let report = run_daemon_dfs_smoke(&paths).map_err(daemon_error)?;
                    ObjectRpcResponse::encode_body(DFS_SMOKE_ROUTE, &report)?
                }
                DFS_PROTOCOL_FRAME_ROUTE => {
                    let command = request.decode_body()?;
                    let frame_response =
                        handle_daemon_dfs_protocol_frame(&paths, command).map_err(daemon_error)?;
                    ObjectRpcResponse::encode_body(DFS_PROTOCOL_FRAME_ROUTE, &frame_response)?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Maps a daemon error from a wrapped sandbox handler into the host-agnostic backend
/// error (the message is preserved; the seam re-maps it to `DaemonError` on the way out).
fn daemon_sandbox_error(error: DaemonError) -> SandboxBackendError {
    SandboxBackendError::Other(error.to_string())
}

/// Clustered sandbox backend: serves the sandbox routes against the daemon's SIFS-over-DFS
/// store by delegating to the existing daemon handlers (which evaluate cluster policy, emit
/// events, and require cluster state).
struct DaemonSandboxBackend {
    paths: DaemonPaths,
}

impl SandboxBackend for DaemonSandboxBackend {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        handle_daemon_sandbox_create(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        handle_daemon_sandbox_status(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        handle_daemon_sandbox_update(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        handle_daemon_sandbox_checkpoint(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        handle_daemon_sandbox_resume(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        handle_daemon_sandbox_reboot(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        handle_daemon_sandbox_destroy(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        handle_daemon_sandbox_list(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        handle_daemon_sandbox_read(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        handle_daemon_sandbox_write(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        handle_daemon_sandbox_http_fetch(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        handle_daemon_sandbox_http_fetch_to_file(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        handle_daemon_sandbox_import_busybox(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_add_command(&self.paths, request)
            .map_err(daemon_sandbox_error)
    }

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_add_artifact(&self.paths, request)
            .map_err(daemon_sandbox_error)
    }

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_import_package(&self.paths, request)
            .map_err(daemon_sandbox_error)
    }

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        handle_daemon_sandbox_toolchain_import_package_from_path(&self.paths, request)
            .map_err(daemon_sandbox_error)
    }

    fn run_toolchain(
        &mut self,
        request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        handle_daemon_sandbox_run_toolchain(&self.paths, request).map_err(daemon_sandbox_error)
    }

    fn run_process_plan(
        &mut self,
        request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        handle_daemon_sandbox_run_process_plan(&self.paths, request).map_err(daemon_sandbox_error)
    }
}

/// Selects the sandbox backend by context: the daemon's clustered SIFS-over-DFS store when
/// a cluster is initialized, else the host-agnostic local filesystem store — so sandbox
/// works on this node even without a cluster.
struct DaemonSandboxBackendFactory {
    paths: DaemonPaths,
}

impl SandboxBackendFactory for DaemonSandboxBackendFactory {
    fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
        if ctx.cluster.is_some() {
            Ok(Box::new(DaemonSandboxBackend {
                paths: self.paths.clone(),
            }))
        } else {
            Ok(Box::new(LocalSandboxBackend::new(
                ctx.data_dir.root().join("sandbox").join("runtime"),
            )))
        }
    }
}

/// Build the registry of services the daemon serves through the seam, bound to this
/// daemon's `paths` (so the foundational services act for the right process) and the
/// request's `authenticated` principal (raft consensus authorizes the sender against it).
fn daemon_service_registry(
    paths: &DaemonPaths,
    authenticated: &AuthenticatedDaemonCommand,
) -> ServiceRegistry {
    let builder: ServiceRegistryBuilder = register_dfs_services(ServiceRegistry::builder());
    let config_registry = daemon_config_registry();
    let builder = register_config_services(builder, config_registry);
    let builder = register_rollout_services(builder)
        .register(DaemonControlService {
            paths: paths.clone(),
        })
        .register(ClusterKvService {
            paths: paths.clone(),
        })
        .register(ClusterRaftService {
            paths: paths.clone(),
            authenticated: authenticated.clone(),
        })
        .register(RolloutBinaryService {
            paths: paths.clone(),
        })
        .register(DaemonDfsDataPlaneService {
            paths: paths.clone(),
        });
    sinomo_sandbox::register_services_with_factory(
        builder,
        Arc::new(DaemonSandboxBackendFactory {
            paths: paths.clone(),
        }),
    )
    .build()
}

fn daemon_config_registry() -> ConfigRegistry {
    register_daemon_config_definitions(ConfigRegistry::builder())
        .expect("daemon config definitions must be valid")
        .build()
}

fn register_daemon_config_definitions(
    builder: ConfigRegistryBuilder,
) -> Result<ConfigRegistryBuilder, String> {
    let builder =
        sinomo_dfs::register_dfs_config_definitions(builder).map_err(|error| error.to_string())?;
    let builder =
        sinomo_rollout::register_config_definitions(builder).map_err(|error| error.to_string())?;
    sinomo_sandbox::register_config_definitions(builder).map_err(|error| error.to_string())
}

/// Collects a service's response frame(s) so the daemon can write them to the blocking
/// control stream after the async dispatch completes.
struct BufferSink {
    frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
}

impl ResponseSink for BufferSink {
    fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
        self.frames
            .lock()
            .map_err(|_| ServiceError::Service("daemon response sink poisoned".to_string()))?
            .push(response);
        Ok(())
    }
}

/// The streaming sandbox-execution routes write `SandboxRun*StreamFrame`s to the connection
/// in real time during long-running, `sandbox`-feature-gated execution. That stream-coupled
/// I/O model is owned by the daemon's own stream handlers (and cannot run daemonless), so we
/// serve them here directly rather than through the unary, host-agnostic service registry.
fn try_dispatch_sandbox_stream(
    request: &ObjectRpcRequest,
    stream: &mut DaemonControlStream,
    paths: &DaemonPaths,
) -> Option<Result<bool, DaemonError>> {
    let result = match request.route.as_str() {
        SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE => request
            .decode_body()
            .map_err(DaemonError::from)
            .and_then(|payload| handle_daemon_sandbox_run_toolchain_stream(paths, payload, stream)),
        SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE => request
            .decode_body()
            .map_err(DaemonError::from)
            .and_then(|payload| {
                handle_daemon_sandbox_run_process_plan_stream(paths, payload, stream)
            }),
        _ => return None,
    };
    Some(result.map(|()| false))
}

/// If the request's route is owned by the service registry, serve it and return the
/// outcome (`Ok(true)` ⇒ the daemon should stop). Returns `None` for routes the registry
/// does not own, so the caller falls through to the daemon's own dispatch.
pub(crate) fn try_dispatch_via_registry(
    authenticated: &AuthenticatedDaemonCommand,
    rpc_request: &Option<ObjectRpcRequest>,
    stream: &mut DaemonControlStream,
    paths: &DaemonPaths,
) -> Option<Result<bool, DaemonError>> {
    let request = rpc_request.as_ref()?;
    if let Some(result) = try_dispatch_sandbox_stream(request, stream, paths) {
        return Some(result);
    }
    let registry = daemon_service_registry(paths, authenticated);
    if !registry.owns(&request.route) {
        return None;
    }
    Some(dispatch_via_registry(
        &registry,
        authenticated,
        request.clone(),
        stream,
        paths,
    ))
}

fn dispatch_via_registry(
    registry: &ServiceRegistry,
    authenticated: &AuthenticatedDaemonCommand,
    request: ObjectRpcRequest,
    stream: &mut DaemonControlStream,
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    let data_dir = Arc::new(SinomoDataDir::new(daemon_data_dir_path(paths)?));
    let cluster: Option<Arc<dyn ClusterAccess>> = if ClusterPaths::new(&data_dir).state.exists() {
        Some(Arc::new(DaemonClusterAccess::new(paths.clone(), &data_dir)))
    } else {
        None
    };
    let signer = if matches!(authenticated.signer, DaemonSignerAuthority::Local) {
        ServiceSigner::Local
    } else {
        ServiceSigner::Remote {
            node_id: authenticated.signer_node_id.clone(),
            public_key: authenticated.signer_public_key.clone(),
        }
    };
    let frames = Arc::new(Mutex::new(Vec::new()));
    let ctx = ServiceContext {
        data_dir,
        cluster,
        signer,
        sink: Box::new(BufferSink {
            frames: Arc::clone(&frames),
        }),
    };

    let disposition = SERVICE_RUNTIME.block_on(registry.dispatch(ctx, request))?;

    let drained: Vec<ObjectRpcResponse> = std::mem::take(&mut *frames.lock().expect("sink frames"));
    for frame in drained {
        write_daemon_rpc_response_frame(stream, frame)?;
    }
    let stop = disposition == ServiceDisposition::Stop;
    if stop {
        // Flush and half-close so the client receives the response before this daemon stops
        // or is replaced by a staged rollout binary; the grace gives it time to read.
        stream.flush().map_err(|source| DaemonError::Network {
            address: "daemon client".to_string(),
            detail: source.to_string(),
        })?;
        stream.shutdown_write();
        std::thread::sleep(ACCEPTED_ROLLOUT_RESPONSE_DRAIN_GRACE);
    }
    Ok(stop)
}
