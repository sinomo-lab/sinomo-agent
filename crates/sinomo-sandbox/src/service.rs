//! Host-agnostic sandbox service.
//!
//! [`SandboxService`] dispatches the sandbox control routes to a [`SandboxBackend`] that a
//! host-provided [`SandboxBackendFactory`] opens per request. The factory selects the
//! backing store from the request context — a local filesystem store when there is no
//! cluster, the daemon's clustered SIFS-over-DFS store otherwise — so a single service
//! serves both daemonless and clustered sandboxes. (The clustered backend needs
//! host-specific networking, so it cannot be constructed inside this crate; the daemon
//! supplies it through the factory.)

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::{
    LocalSandboxBackend, SANDBOX_CHECKPOINT_ROUTE, SANDBOX_CREATE_ROUTE, SANDBOX_DESTROY_ROUTE,
    SANDBOX_HTTP_FETCH_ROUTE, SANDBOX_HTTP_FETCH_TO_FILE_ROUTE, SANDBOX_IMPORT_BUSYBOX_ROUTE,
    SANDBOX_LIST_ROUTE, SANDBOX_READ_ROUTE, SANDBOX_REBOOT_ROUTE, SANDBOX_RESUME_ROUTE,
    SANDBOX_RUN_CANCEL_ROUTE, SANDBOX_RUN_PROCESS_PLAN_ROUTE, SANDBOX_RUN_TOOLCHAIN_ROUTE,
    SANDBOX_STATUS_ROUTE, SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
    SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE, SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
    SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE, SANDBOX_UPDATE_ROUTE, SANDBOX_WRITE_ROUTE,
    SandboxBackend, SandboxBackendError, SandboxCancellationToken, SandboxId,
    SandboxRunCancelResponse,
};
use sinomo_core::network::{ObjectRpcCancelRequest, ObjectRpcRequest, ObjectRpcResponse};

use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

impl From<SandboxBackendError> for ServiceError {
    fn from(error: SandboxBackendError) -> Self {
        ServiceError::Service(error.to_string())
    }
}

/// Opens the [`SandboxBackend`] for a request. Hosts implement this to choose the backing
/// store from the [`ServiceContext`] (e.g. local filesystem when `ctx.cluster` is `None`,
/// clustered SIFS otherwise).
pub trait SandboxBackendFactory: Send + Sync {
    fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError>;
}

/// Default factory serving the sandbox routes from the **local filesystem store** at
/// `<data-dir>/sandbox/runtime`. Use this for daemonless / clusterless hosts (the CLI, or a
/// daemon with no cluster initialized).
pub struct LocalSandboxBackendFactory;

impl SandboxBackendFactory for LocalSandboxBackendFactory {
    fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
        let backend = LocalSandboxBackend::new(ctx.data_dir.root().join("sandbox").join("runtime"))
            .with_container_config_from_env()
            .map_err(|source| ServiceError::Service(source.to_string()))?;
        Ok(Box::new(backend))
    }
}

/// Serves the sandbox control routes by opening a backend per request and dispatching the
/// decoded request to it.
pub struct SandboxService {
    factory: Arc<dyn SandboxBackendFactory>,
    active_run_cancellations: Arc<Mutex<BTreeMap<String, SandboxCancellationToken>>>,
    active_sandbox_operations: Arc<Mutex<BTreeMap<SandboxId, SandboxOperationState>>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct SandboxOperationState {
    active_runs: u64,
    mutation_active: bool,
}

#[derive(Clone, Copy, Debug)]
enum SandboxOperationKind {
    Run,
    Mutation,
}

struct SandboxOperationGuard {
    active: Arc<Mutex<BTreeMap<SandboxId, SandboxOperationState>>>,
    sandbox_id: SandboxId,
    kind: SandboxOperationKind,
}

impl Drop for SandboxOperationGuard {
    fn drop(&mut self) {
        let Ok(mut active) = self.active.lock() else {
            return;
        };
        let Some(state) = active.get_mut(&self.sandbox_id) else {
            return;
        };
        match self.kind {
            SandboxOperationKind::Run => {
                state.active_runs = state.active_runs.saturating_sub(1);
            }
            SandboxOperationKind::Mutation => state.mutation_active = false,
        }
        if state.active_runs == 0 && !state.mutation_active {
            active.remove(&self.sandbox_id);
        }
    }
}

impl SandboxService {
    pub fn new(factory: Arc<dyn SandboxBackendFactory>) -> Self {
        Self {
            factory,
            active_run_cancellations: Arc::new(Mutex::new(BTreeMap::new())),
            active_sandbox_operations: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

fn begin_sandbox_operation(
    active: &Arc<Mutex<BTreeMap<SandboxId, SandboxOperationState>>>,
    sandbox_id: &SandboxId,
    kind: SandboxOperationKind,
) -> Result<SandboxOperationGuard, ServiceError> {
    let mut states = active
        .lock()
        .map_err(|_| ServiceError::Service("sandbox operation tracker lock failed".to_string()))?;
    let state = states.entry(sandbox_id.clone()).or_default();
    match kind {
        SandboxOperationKind::Run if state.mutation_active || state.active_runs > 0 => {
            return Err(ServiceError::Service(format!(
                "sandbox `{sandbox_id}` already has an active execution or lifecycle mutation"
            )));
        }
        SandboxOperationKind::Run => state.active_runs = state.active_runs.saturating_add(1),
        SandboxOperationKind::Mutation if state.mutation_active || state.active_runs > 0 => {
            return Err(ServiceError::Service(format!(
                "sandbox `{sandbox_id}` is active; retry the lifecycle mutation after execution completes"
            )));
        }
        SandboxOperationKind::Mutation => state.mutation_active = true,
    }
    Ok(SandboxOperationGuard {
        active: Arc::clone(active),
        sandbox_id: sandbox_id.clone(),
        kind,
    })
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    register_services_with_factory(builder, Arc::new(LocalSandboxBackendFactory))
}

pub fn register_services_with_factory(
    builder: ServiceRegistryBuilder,
    factory: Arc<dyn SandboxBackendFactory>,
) -> ServiceRegistryBuilder {
    builder.register(SandboxService::new(factory))
}

fn dispatch_sandbox_backend_request(
    backend: &mut dyn SandboxBackend,
    request: ObjectRpcRequest,
    active_run_cancellations: &Arc<Mutex<BTreeMap<String, SandboxCancellationToken>>>,
    active_sandbox_operations: &Arc<Mutex<BTreeMap<SandboxId, SandboxOperationState>>>,
) -> Result<ObjectRpcResponse, ServiceError> {
    let response = match request.route.as_str() {
        SANDBOX_CREATE_ROUTE => {
            let payload: crate::SandboxCreateInstanceRequest = request.decode_body()?;
            ObjectRpcResponse::encode_body(SANDBOX_CREATE_ROUTE, &backend.create(payload)?)?
        }
        SANDBOX_STATUS_ROUTE => {
            let payload: crate::SandboxStatusRequest = request.decode_body()?;
            ObjectRpcResponse::encode_body(SANDBOX_STATUS_ROUTE, &backend.status(payload)?)?
        }
        SANDBOX_UPDATE_ROUTE => {
            let payload: crate::SandboxUpdateInstanceRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(SANDBOX_UPDATE_ROUTE, &backend.update(payload)?)?
        }
        SANDBOX_CHECKPOINT_ROUTE => {
            let payload: crate::SandboxCheckpointRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(SANDBOX_CHECKPOINT_ROUTE, &backend.checkpoint(payload)?)?
        }
        SANDBOX_RESUME_ROUTE => {
            let payload: crate::SandboxResumeRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(SANDBOX_RESUME_ROUTE, &backend.resume(payload)?)?
        }
        SANDBOX_REBOOT_ROUTE => {
            let payload: crate::SandboxRebootRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(SANDBOX_REBOOT_ROUTE, &backend.reboot(payload)?)?
        }
        SANDBOX_DESTROY_ROUTE => {
            let payload: crate::SandboxDestroyRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(SANDBOX_DESTROY_ROUTE, &backend.destroy(payload)?)?
        }
        SANDBOX_LIST_ROUTE => {
            let payload: crate::SandboxListRequest = request.decode_body()?;
            ObjectRpcResponse::encode_body(SANDBOX_LIST_ROUTE, &backend.list(payload)?)?
        }
        SANDBOX_READ_ROUTE => {
            let payload: crate::SandboxReadRequest = request.decode_body()?;
            ObjectRpcResponse::encode_body(SANDBOX_READ_ROUTE, &backend.read(payload)?)?
        }
        SANDBOX_WRITE_ROUTE => {
            let payload: crate::SandboxWriteRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(SANDBOX_WRITE_ROUTE, &backend.write(payload)?)?
        }
        SANDBOX_HTTP_FETCH_ROUTE => {
            let payload = request.decode_body()?;
            ObjectRpcResponse::encode_body(SANDBOX_HTTP_FETCH_ROUTE, &backend.http_fetch(payload)?)?
        }
        SANDBOX_HTTP_FETCH_TO_FILE_ROUTE => {
            let payload: crate::SandboxHttpFetchToFileRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Mutation,
            )?;
            ObjectRpcResponse::encode_body(
                SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
                &backend.http_fetch_to_file(payload)?,
            )?
        }
        SANDBOX_IMPORT_BUSYBOX_ROUTE => {
            let payload = request.decode_body()?;
            ObjectRpcResponse::encode_body(
                SANDBOX_IMPORT_BUSYBOX_ROUTE,
                &backend.import_busybox(payload)?,
            )?
        }
        SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE => {
            let payload = request.decode_body()?;
            ObjectRpcResponse::encode_body(
                SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
                &backend.toolchain_add_command(payload)?,
            )?
        }
        SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE => {
            let payload = request.decode_body()?;
            ObjectRpcResponse::encode_body(
                SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
                &backend.toolchain_add_artifact(payload)?,
            )?
        }
        SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE => {
            let payload = request.decode_body()?;
            ObjectRpcResponse::encode_body(
                SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
                &backend.import_toolchain_package(payload)?,
            )?
        }
        SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE => {
            let payload = request.decode_body()?;
            ObjectRpcResponse::encode_body(
                SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
                &backend.import_toolchain_package_from_path(payload)?,
            )?
        }
        SANDBOX_RUN_TOOLCHAIN_ROUTE => {
            let operation_id = request.operation_id().map(str::to_string);
            let payload: crate::SandboxRunToolchainRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Run,
            )?;
            let cancellation_token = register_sandbox_run_cancellation(
                active_run_cancellations,
                operation_id.as_deref(),
            )?;
            let result =
                backend.run_toolchain_with_cancellation(payload, cancellation_token.clone());
            unregister_sandbox_run_cancellation(active_run_cancellations, operation_id.as_deref());
            ObjectRpcResponse::encode_body(SANDBOX_RUN_TOOLCHAIN_ROUTE, &result?)?
        }
        SANDBOX_RUN_PROCESS_PLAN_ROUTE => {
            let operation_id = request.operation_id().map(str::to_string);
            let payload: crate::SandboxRunProcessPlanRequest = request.decode_body()?;
            let _guard = begin_sandbox_operation(
                active_sandbox_operations,
                &payload.sandbox_id,
                SandboxOperationKind::Run,
            )?;
            let cancellation_token = register_sandbox_run_cancellation(
                active_run_cancellations,
                operation_id.as_deref(),
            )?;
            let result =
                backend.run_process_plan_with_cancellation(payload, cancellation_token.clone());
            unregister_sandbox_run_cancellation(active_run_cancellations, operation_id.as_deref());
            ObjectRpcResponse::encode_body(SANDBOX_RUN_PROCESS_PLAN_ROUTE, &result?)?
        }
        other => return Err(ServiceError::UnknownRoute(other.to_string())),
    };
    Ok(response)
}

fn register_sandbox_run_cancellation(
    active_run_cancellations: &Arc<Mutex<BTreeMap<String, SandboxCancellationToken>>>,
    operation_id: Option<&str>,
) -> Result<SandboxCancellationToken, ServiceError> {
    let token = SandboxCancellationToken::new();
    let Some(operation_id) = operation_id else {
        return Ok(token);
    };
    let mut active = active_run_cancellations.lock().map_err(|_| {
        ServiceError::Service("sandbox cancellation registry lock failed".to_string())
    })?;
    if active.contains_key(operation_id) {
        return Err(ServiceError::Service(format!(
            "sandbox run operation `{operation_id}` is already active"
        )));
    }
    active.insert(operation_id.to_string(), token.clone());
    Ok(token)
}

fn unregister_sandbox_run_cancellation(
    active_run_cancellations: &Arc<Mutex<BTreeMap<String, SandboxCancellationToken>>>,
    operation_id: Option<&str>,
) {
    let Some(operation_id) = operation_id else {
        return;
    };
    if let Ok(mut active) = active_run_cancellations.lock() {
        active.remove(operation_id);
    }
}

fn dispatch_sandbox_run_cancel_request(
    active_run_cancellations: &Arc<Mutex<BTreeMap<String, SandboxCancellationToken>>>,
    request: ObjectRpcRequest,
) -> Result<ObjectRpcResponse, ServiceError> {
    let payload: ObjectRpcCancelRequest = request.decode_body()?;
    let operation_id = payload.operation_id;
    let metadata_operation_id = request.cancels_operation_id().map(str::to_string);
    if metadata_operation_id
        .as_deref()
        .is_some_and(|metadata| metadata != operation_id)
    {
        return ObjectRpcResponse::encode_body(
            SANDBOX_RUN_CANCEL_ROUTE,
            &SandboxRunCancelResponse {
                operation_id,
                cancelled: false,
                detail: "cancel metadata operation id does not match request body".to_string(),
            },
        )
        .map_err(ServiceError::from);
    }
    let token = active_run_cancellations
        .lock()
        .map_err(|_| {
            ServiceError::Service("sandbox cancellation registry lock failed".to_string())
        })?
        .get(&operation_id)
        .cloned();
    let (cancelled, detail) = if let Some(token) = token {
        if token.cancel() {
            (true, payload.reason)
        } else {
            (
                false,
                "sandbox run operation is already completing or completed".to_string(),
            )
        }
    } else {
        (false, "sandbox run operation is not active".to_string())
    };
    ObjectRpcResponse::encode_body(
        SANDBOX_RUN_CANCEL_ROUTE,
        &SandboxRunCancelResponse {
            operation_id,
            cancelled,
            detail,
        },
    )
    .map_err(ServiceError::from)
}

impl Service for SandboxService {
    fn name(&self) -> &'static str {
        "sandbox"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[
            SANDBOX_CREATE_ROUTE,
            SANDBOX_STATUS_ROUTE,
            SANDBOX_UPDATE_ROUTE,
            SANDBOX_CHECKPOINT_ROUTE,
            SANDBOX_RESUME_ROUTE,
            SANDBOX_REBOOT_ROUTE,
            SANDBOX_DESTROY_ROUTE,
            SANDBOX_LIST_ROUTE,
            SANDBOX_READ_ROUTE,
            SANDBOX_WRITE_ROUTE,
            SANDBOX_HTTP_FETCH_ROUTE,
            SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
            SANDBOX_IMPORT_BUSYBOX_ROUTE,
            SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
            SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
            SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
            SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
            SANDBOX_RUN_TOOLCHAIN_ROUTE,
            SANDBOX_RUN_PROCESS_PLAN_ROUTE,
            SANDBOX_RUN_CANCEL_ROUTE,
        ]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| ServiceRoute::new(route, self.name()).with_concurrency_limit(8))
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let factory = Arc::clone(&self.factory);
        let active_run_cancellations = Arc::clone(&self.active_run_cancellations);
        let active_sandbox_operations = Arc::clone(&self.active_sandbox_operations);
        Box::pin(async move {
            if request.route == SANDBOX_RUN_CANCEL_ROUTE {
                let response =
                    dispatch_sandbox_run_cancel_request(&active_run_cancellations, request)?;
                ctx.respond(response)?;
                return Ok(ServiceDisposition::Continue);
            }
            let mut backend = factory.open(&ctx)?;
            let active_run_cancellations_for_task = Arc::clone(&active_run_cancellations);
            let active_sandbox_operations_for_task = Arc::clone(&active_sandbox_operations);
            let response = tokio::task::spawn_blocking(move || {
                dispatch_sandbox_backend_request(
                    backend.as_mut(),
                    request,
                    &active_run_cancellations_for_task,
                    &active_sandbox_operations_for_task,
                )
            })
            .await
            .map_err(|source| {
                ServiceError::Service(format!("sandbox backend task failed: {source}"))
            })??;
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SANDBOX_UNLIMITED_WALL_TIME_MS, SandboxContainerBackendConfig, SandboxContainerRuntime,
        SandboxCreateInstanceRequest, SandboxId, SandboxRunToolchainRequest, SandboxTempPolicy,
        SandboxToolchainId,
    };
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse};

    use sinomo_core::service::{ResponseSink, ServiceRegistry, ServiceSigner};

    #[derive(Clone, Default)]
    struct CollectingSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for CollectingSink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
            if !response.body.is_empty() {
                return Err(ServiceError::Service(
                    "sandbox test sink received an unexpected response body".to_string(),
                ));
            }
            self.frames
                .lock()
                .expect("sink lock")
                .push(response.response);
            Ok(())
        }
    }

    fn local_context(dir: &std::path::Path, sink: CollectingSink) -> ServiceContext {
        ServiceContext::new(
            Arc::new(SinomoDataDir::new(dir)),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        )
    }

    /// A factory that fails to open a backend (e.g. clustered route with no cluster).
    struct UnavailableFactory;
    impl SandboxBackendFactory for UnavailableFactory {
        fn open(&self, _ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
            Err(ServiceError::NotClustered(SANDBOX_STATUS_ROUTE.to_string()))
        }
    }

    struct FakeContainerFactory {
        config: SandboxContainerBackendConfig,
    }

    impl SandboxBackendFactory for FakeContainerFactory {
        fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
            Ok(Box::new(
                LocalSandboxBackend::new(ctx.data_dir.root().join("sandbox").join("runtime"))
                    .with_container_config(self.config.clone()),
            ))
        }
    }

    #[test]
    fn reports_its_routes() {
        let service = SandboxService::new(Arc::new(UnavailableFactory));
        assert_eq!(service.name(), "sandbox");
        assert!(service.routes().contains(&SANDBOX_STATUS_ROUTE));
        assert_eq!(service.routes().len(), 20);
        let routes = service.route_categories();
        assert_eq!(routes.len(), service.routes().len());
        assert!(
            routes
                .iter()
                .all(|route| route.concurrency_limit == Some(8))
        );
    }

    /// The service is registered, owns the route, opens a backend via the factory, and
    /// propagates the factory's error (the request body is never decoded in this path).
    #[tokio::test]
    async fn factory_error_propagates() {
        let registry = register_services_with_factory(
            ServiceRegistry::builder(),
            Arc::new(UnavailableFactory),
        )
        .build();
        assert!(registry.owns(SANDBOX_STATUS_ROUTE));

        let dir = sinomo_testdir::TestDir::new("sinomo-service-sandbox-test");
        let request =
            ObjectRpcRequest::encode_body(SANDBOX_STATUS_ROUTE, "sandbox", &()).expect("encode");
        let error = registry
            .dispatch(local_context(&dir, CollectingSink::default()), request)
            .await
            .expect_err("factory failure should surface");
        assert!(
            matches!(error, ServiceError::NotClustered(route) if route == SANDBOX_STATUS_ROUTE)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_cancel_route_cancels_active_container_execution() {
        use std::os::unix::fs::PermissionsExt;

        let dir = sinomo_testdir::TestDir::new("sinomo-sandbox-service-cancel");
        let fake_runtime = dir.join("fake-container-runtime.sh");
        let started_marker = dir.join("container-started");
        let cancel_marker = dir.join("container-cancelled");
        std::fs::write(
            &fake_runtime,
            format!(
                r#"#!/bin/sh
started='{}'
cancelled='{}'
if [ "$1" = "rm" ]; then
  touch "$cancelled"
  exit 0
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    fake-image) shift; break ;;
    --name|--network|--cap-drop|--security-opt|--memory|--stop-timeout|--volume|--workdir|--env) shift 2 ;;
    --rm|--pull=never|--read-only|-i) shift ;;
    *) shift ;;
  esac
done
touch "$started"
while [ ! -f "$cancelled" ]; do
  sleep 0.05
done
exit 0
"#,
                started_marker.display(),
                cancel_marker.display()
            ),
        )
        .expect("write fake runtime");
        let mut permissions = std::fs::metadata(&fake_runtime)
            .expect("metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_runtime, permissions).expect("permissions");

        let config = SandboxContainerBackendConfig::new(
            SandboxContainerRuntime::Command(fake_runtime),
            "fake-image",
        )
        .expect("container config");
        let registry = register_services_with_factory(
            ServiceRegistry::builder(),
            Arc::new(FakeContainerFactory { config }),
        )
        .build();
        let sandbox_id = SandboxId::from_legacy_named("sandbox_service_cancel").expect("sandbox");
        let create = SandboxCreateInstanceRequest {
            sandbox_id: Some(sandbox_id.clone()),
            toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_cancel")
                .expect("toolchain"),
            temp_policy: SandboxTempPolicy::RetainAcrossSessions,
            memory_mb: None,
            wall_time_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
            max_workspace_bytes: None,
            max_written_bytes_per_command: None,
            max_file_count: None,
            mounts: Vec::new(),
        };
        let create_request =
            ObjectRpcRequest::encode_body(SANDBOX_CREATE_ROUTE, "sandbox", &create)
                .expect("create request");
        registry
            .dispatch(
                local_context(&dir, CollectingSink::default()),
                create_request,
            )
            .await
            .expect("create sandbox");

        let operation_id = "sandbox_run_cancel_route_op";
        let run = SandboxRunToolchainRequest {
            sandbox_id,
            command_name: "sh".to_string(),
            argv: vec!["sh".to_string(), "-c".to_string(), "sleep 30".to_string()],
            stdin: None,
            timeout_ms: SANDBOX_UNLIMITED_WALL_TIME_MS,
        };
        let run_request =
            ObjectRpcRequest::encode_body(SANDBOX_RUN_TOOLCHAIN_ROUTE, "sandbox", &run)
                .expect("run request")
                .with_operation_id(operation_id);
        let run_registry = registry.clone();
        let run_dir = dir.to_path_buf();
        let run_handle = tokio::spawn(async move {
            run_registry
                .dispatch(
                    local_context(&run_dir, CollectingSink::default()),
                    run_request,
                )
                .await
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        while !started_marker.exists() {
            assert!(
                Instant::now() < deadline,
                "fake runtime did not start before deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let overlapping_run = SandboxRunToolchainRequest {
            sandbox_id: SandboxId::from_legacy_named("sandbox_service_cancel").expect("sandbox"),
            command_name: "sh".to_string(),
            argv: vec!["sh".to_string(), "-c".to_string(), "true".to_string()],
            stdin: None,
            timeout_ms: SANDBOX_UNLIMITED_WALL_TIME_MS,
        };
        let overlapping_request =
            ObjectRpcRequest::encode_body(SANDBOX_RUN_TOOLCHAIN_ROUTE, "sandbox", &overlapping_run)
                .expect("overlapping run request")
                .with_operation_id("sandbox_run_overlap");
        let overlap_error = registry
            .dispatch(
                local_context(&dir, CollectingSink::default()),
                overlapping_request,
            )
            .await
            .expect_err("a second run must not share the mutable local workspace");
        assert!(
            overlap_error.to_string().contains("active execution"),
            "unexpected overlap error: {overlap_error}"
        );

        let cancel_sink = CollectingSink::default();
        let cancel_frames = Arc::clone(&cancel_sink.frames);
        let cancel_request = ObjectRpcRequest::encode_body(
            SANDBOX_RUN_CANCEL_ROUTE,
            "sandbox",
            &ObjectRpcCancelRequest::new(operation_id, "readiness cancel"),
        )
        .expect("cancel request")
        .with_cancels_operation_id(operation_id);
        registry
            .dispatch(local_context(&dir, cancel_sink), cancel_request)
            .await
            .expect("cancel route");
        let cancel_response: SandboxRunCancelResponse = cancel_frames
            .lock()
            .expect("cancel frames")
            .first()
            .expect("cancel response")
            .decode_body()
            .expect("cancel response body");
        assert_eq!(cancel_response.operation_id, operation_id);
        assert!(cancel_response.cancelled);

        let run_error = run_handle
            .await
            .expect("run task")
            .expect_err("run should be cancelled");
        assert!(
            run_error.to_string().contains("cancelled"),
            "unexpected run error: {run_error}"
        );
        assert!(cancel_marker.exists());
    }

    #[test]
    fn lifecycle_mutation_is_fenced_while_sandbox_run_is_active() {
        let active = Arc::new(Mutex::new(BTreeMap::new()));
        let sandbox_id = SandboxId::from_legacy_named("service_lifecycle_fence").expect("id");
        let run = begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Run)
            .expect("begin run");

        let error = begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Mutation)
            .err()
            .expect("mutation must be fenced");
        assert!(error.to_string().contains("is active"));

        drop(run);
        let mutation =
            begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Mutation)
                .expect("mutation after run");
        assert!(begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Run).is_err());
        drop(mutation);
        assert!(active.lock().expect("tracker").is_empty());
    }

    #[test]
    fn concurrent_runs_are_fenced_until_the_first_run_releases_the_workspace() {
        let active = Arc::new(Mutex::new(BTreeMap::new()));
        let sandbox_id = SandboxId::from_legacy_named("service_run_fence").expect("id");
        let first = begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Run)
            .expect("begin first run");

        let error = begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Run)
            .err()
            .expect("overlapping run must be fenced");
        assert!(error.to_string().contains("active execution"));

        drop(first);
        let second = begin_sandbox_operation(&active, &sandbox_id, SandboxOperationKind::Run)
            .expect("run after first released workspace");
        drop(second);
        assert!(active.lock().expect("tracker").is_empty());
    }

    #[test]
    fn late_cancel_is_not_acknowledged_after_completion_wins_cas() {
        let active = Arc::new(Mutex::new(BTreeMap::new()));
        let operation_id = "sandbox_late_cancel";
        let token = SandboxCancellationToken::new();
        assert!(token.begin_completing());
        active
            .lock()
            .expect("active cancellations")
            .insert(operation_id.to_string(), token);
        let request = ObjectRpcRequest::encode_body(
            SANDBOX_RUN_CANCEL_ROUTE,
            "sandbox",
            &ObjectRpcCancelRequest::new(operation_id, "too late"),
        )
        .expect("cancel request")
        .with_cancels_operation_id(operation_id);

        let response =
            dispatch_sandbox_run_cancel_request(&active, request).expect("cancel response");
        let response: SandboxRunCancelResponse = response.decode_body().expect("response body");
        assert!(!response.cancelled);
        assert!(response.detail.contains("completing"));
    }
}
