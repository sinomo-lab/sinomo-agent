//! Host-agnostic sandbox service.
//!
//! [`SandboxService`] dispatches the sandbox control routes to a [`SandboxBackend`] that a
//! host-provided [`SandboxBackendFactory`] opens per request. The factory selects the
//! backing store from the request context — a local filesystem store when there is no
//! cluster, the daemon's clustered SIFS-over-DFS store otherwise — so a single service
//! serves both daemonless and clustered sandboxes. (The clustered backend needs
//! host-specific networking, so it cannot be constructed inside this crate; the daemon
//! supplies it through the factory.)

use std::sync::Arc;

use sinomo_network::{
    ObjectRpcRequest, ObjectRpcResponse, SANDBOX_CHECKPOINT_ROUTE, SANDBOX_CREATE_ROUTE,
    SANDBOX_DESTROY_ROUTE, SANDBOX_HTTP_FETCH_ROUTE, SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
    SANDBOX_IMPORT_BUSYBOX_ROUTE, SANDBOX_LIST_ROUTE, SANDBOX_READ_ROUTE, SANDBOX_REBOOT_ROUTE,
    SANDBOX_RESUME_ROUTE, SANDBOX_RUN_PROCESS_PLAN_ROUTE, SANDBOX_RUN_TOOLCHAIN_ROUTE,
    SANDBOX_STATUS_ROUTE, SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
    SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE, SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
    SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE, SANDBOX_UPDATE_ROUTE, SANDBOX_WRITE_ROUTE,
};
use sinomo_sandbox::{LocalSandboxBackend, SandboxBackend, SandboxBackendError};

use crate::{Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture};

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
        Ok(Box::new(LocalSandboxBackend::new(
            ctx.data_dir.root().join("sandbox").join("runtime"),
        )))
    }
}

/// Serves the sandbox control routes by opening a backend per request and dispatching the
/// decoded request to it.
pub struct SandboxService {
    factory: Arc<dyn SandboxBackendFactory>,
}

impl SandboxService {
    pub fn new(factory: Arc<dyn SandboxBackendFactory>) -> Self {
        Self { factory }
    }
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
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let factory = Arc::clone(&self.factory);
        Box::pin(async move {
            let mut backend = factory.open(&ctx)?;
            let response = match request.route.as_str() {
                SANDBOX_CREATE_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_CREATE_ROUTE, &backend.create(payload)?)?
                }
                SANDBOX_STATUS_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_STATUS_ROUTE, &backend.status(payload)?)?
                }
                SANDBOX_UPDATE_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_UPDATE_ROUTE, &backend.update(payload)?)?
                }
                SANDBOX_CHECKPOINT_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        SANDBOX_CHECKPOINT_ROUTE,
                        &backend.checkpoint(payload)?,
                    )?
                }
                SANDBOX_RESUME_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_RESUME_ROUTE, &backend.resume(payload)?)?
                }
                SANDBOX_REBOOT_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_REBOOT_ROUTE, &backend.reboot(payload)?)?
                }
                SANDBOX_DESTROY_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        SANDBOX_DESTROY_ROUTE,
                        &backend.destroy(payload)?,
                    )?
                }
                SANDBOX_LIST_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_LIST_ROUTE, &backend.list(payload)?)?
                }
                SANDBOX_READ_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_READ_ROUTE, &backend.read(payload)?)?
                }
                SANDBOX_WRITE_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(SANDBOX_WRITE_ROUTE, &backend.write(payload)?)?
                }
                SANDBOX_HTTP_FETCH_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        SANDBOX_HTTP_FETCH_ROUTE,
                        &backend.http_fetch(payload)?,
                    )?
                }
                SANDBOX_HTTP_FETCH_TO_FILE_ROUTE => {
                    let payload = request.decode_body()?;
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
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        SANDBOX_RUN_TOOLCHAIN_ROUTE,
                        &backend.run_toolchain(payload)?,
                    )?
                }
                SANDBOX_RUN_PROCESS_PLAN_ROUTE => {
                    let payload = request.decode_body()?;
                    ObjectRpcResponse::encode_body(
                        SANDBOX_RUN_PROCESS_PLAN_ROUTE,
                        &backend.run_process_plan(payload)?,
                    )?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use sinomo_local::SinomoDataDir;

    use crate::{ResponseSink, ServiceRegistry, ServiceSigner};

    #[derive(Clone, Default)]
    struct CollectingSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for CollectingSink {
        fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    fn local_context(sink: CollectingSink) -> ServiceContext {
        ServiceContext {
            data_dir: Arc::new(SinomoDataDir::new(
                std::env::temp_dir().join("sinomo-service-sandbox-test"),
            )),
            cluster: None,
            signer: ServiceSigner::Local,
            sink: Box::new(sink),
        }
    }

    /// A factory that fails to open a backend (e.g. clustered route with no cluster).
    struct UnavailableFactory;
    impl SandboxBackendFactory for UnavailableFactory {
        fn open(&self, _ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
            Err(ServiceError::NotClustered(SANDBOX_STATUS_ROUTE.to_string()))
        }
    }

    #[test]
    fn reports_its_routes() {
        let service = SandboxService::new(Arc::new(UnavailableFactory));
        assert_eq!(service.name(), "sandbox");
        assert!(service.routes().contains(&SANDBOX_STATUS_ROUTE));
        assert_eq!(service.routes().len(), 19);
    }

    /// The service is registered, owns the route, opens a backend via the factory, and
    /// propagates the factory's error (the request body is never decoded in this path).
    #[tokio::test]
    async fn factory_error_propagates() {
        let registry = ServiceRegistry::builder()
            .register(SandboxService::new(Arc::new(UnavailableFactory)))
            .build();
        assert!(registry.owns(SANDBOX_STATUS_ROUTE));

        let request =
            ObjectRpcRequest::encode_body(SANDBOX_STATUS_ROUTE, "sandbox", &()).expect("encode");
        let error = registry
            .dispatch(local_context(CollectingSink::default()), request)
            .await
            .expect_err("factory failure should surface");
        assert!(
            matches!(error, ServiceError::NotClustered(route) if route == SANDBOX_STATUS_ROUTE)
        );
    }
}
