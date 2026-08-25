//! Rollout object-RPC services.
//!
//! The diagnostics route is host-agnostic: it reports rollout state from the local
//! data directory and works with no daemon and no cluster. Binary/build activation
//! routes use [`RolloutServiceHost`], so rollout owns its wire and service semantics
//! while a daemon or embedded host supplies process-specific effects.

use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;

use crate::{
    ClusterRolloutBinaryManifest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, ROLLOUT_BINARY_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_FINALIZE_SHADOW_ROUTE,
    ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE, RolloutBinaryIngress, SelfRolloutPaths,
    complete_rollout_binary_activation, receive_rollout_binary_ingress, rollout_diagnostics_report,
    rollout_module_surfaces,
};

const ROLLOUT_ROUTES: &[&str] = &[ROLLOUT_DIAGNOSTICS_ROUTE];
const ROLLOUT_HOST_ROUTES: &[&str] = &[
    ROLLOUT_BINARY_ROUTE,
    ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
    ROLLOUT_BUILD_ROUTE,
    ROLLOUT_FINALIZE_SHADOW_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE,
];

pub type RolloutHostFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Process-specific effects required by rollout activation and recovery routes.
///
/// The rollout crate owns request decoding, streamed-body rules, durable upload
/// ingress, artifact verification, error response policy, and service
/// disposition. Hosts only activate a verified manifest, perform build/module
/// process effects, and decide whether an accepted response requires their
/// current process to stop.
pub trait RolloutServiceHost: Send + Sync {
    fn activate_binary<'a>(
        &'a self,
        manifest: ClusterRolloutBinaryManifest,
    ) -> RolloutHostFuture<'a, std::result::Result<ClusterRolloutBinaryResponse, String>>;

    fn activate_native_dylib(
        &self,
        request: ClusterRolloutNativeDylibModuleRequest,
    ) -> std::result::Result<ClusterRolloutBinaryResponse, String>;

    fn build(
        &self,
        request: ClusterRolloutBuildRequest,
    ) -> std::result::Result<ClusterRolloutBinaryResponse, String>;

    fn finalize_shadow(&self) -> std::result::Result<String, String>;

    fn clear_status(&self) -> std::result::Result<String, String>;

    fn should_stop_after(&self, response: &ClusterRolloutBinaryResponse) -> bool;
}

#[derive(Default)]
pub struct RolloutService;

pub struct HostedRolloutService {
    host: Arc<dyn RolloutServiceHost>,
}

impl RolloutService {
    pub fn new() -> Self {
        Self
    }
}

impl HostedRolloutService {
    pub fn new(host: Arc<dyn RolloutServiceHost>) -> Self {
        Self { host }
    }

    fn finish(
        &self,
        result: std::result::Result<ClusterRolloutBinaryResponse, String>,
    ) -> (ClusterRolloutBinaryResponse, ServiceDisposition) {
        let response = result.unwrap_or_else(|detail| ClusterRolloutBinaryResponse {
            status: "failed".to_string(),
            detail,
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        });
        let disposition = if self.host.should_stop_after(&response) {
            ServiceDisposition::Stop
        } else {
            ServiceDisposition::Continue
        };
        (response, disposition)
    }
}

impl Service for RolloutService {
    fn name(&self) -> &'static str {
        "rollout"
    }

    fn routes(&self) -> &'static [&'static str] {
        ROLLOUT_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![rollout_service_route(ROLLOUT_DIAGNOSTICS_ROUTE)]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            match request.route.as_str() {
                ROLLOUT_DIAGNOSTICS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let rollout_paths = SelfRolloutPaths::new(&ctx.data_dir.root());
                    let report = rollout_diagnostics_report(&rollout_paths)
                        .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(ROLLOUT_DIAGNOSTICS_ROUTE, report)?;
                    Ok(ServiceDisposition::Continue)
                }
                other => Err(ServiceError::UnknownRoute(other.to_string())),
            }
        })
    }
}

impl Service for HostedRolloutService {
    fn name(&self) -> &'static str {
        "rollout-host"
    }

    fn routes(&self) -> &'static [&'static str] {
        ROLLOUT_HOST_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                let service_route = rollout_host_service_route(route);
                match route {
                    ROLLOUT_BINARY_ROUTE => {
                        service_route.with_request_body_mode(ServiceBodyMode::Required)
                    }
                    ROLLOUT_BUILD_ROUTE => {
                        service_route.with_request_body_mode(ServiceBodyMode::Optional)
                    }
                    _ => service_route,
                }
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let route = request.route.clone();
            match route.as_str() {
                ROLLOUT_BINARY_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let mut body = ctx.take_request_body();
                    let ingress =
                        receive_rollout_binary_ingress(&ctx.data_dir.root(), payload, &mut body)
                            .await;
                    let result = match ingress {
                        Err(error) => Err(error.to_string()),
                        Ok(RolloutBinaryIngress::Complete(response)) => Ok(response),
                        Ok(RolloutBinaryIngress::Activate(prepared)) => {
                            match host.activate_binary(prepared.manifest.clone()).await {
                                Ok(response) => {
                                    complete_rollout_binary_activation(&prepared, &response)
                                        .map(|()| response)
                                        .map_err(|error| error.to_string())
                                }
                                Err(error) => Err(error),
                            }
                        }
                    };
                    let service = HostedRolloutService { host };
                    let (response, disposition) = service.finish(result);
                    ctx.respond_value(route, response)?;
                    Ok(disposition)
                }
                ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let task_host = Arc::clone(&host);
                    let result = run_blocking_rollout_host_task(
                        route.clone(),
                        "rollout native dylib module",
                        move || task_host.activate_native_dylib(payload),
                    )
                    .await?;
                    let service = HostedRolloutService { host };
                    let (response, disposition) = service.finish(result);
                    ctx.respond_value(route, response)?;
                    Ok(disposition)
                }
                ROLLOUT_BUILD_ROUTE => {
                    let payload = decode_rollout_build_session_request(&mut ctx, &request).await?;
                    let task_host = Arc::clone(&host);
                    let result =
                        run_blocking_rollout_host_task(route.clone(), "rollout build", move || {
                            task_host.build(payload)
                        })
                        .await?;
                    let service = HostedRolloutService { host };
                    let (response, disposition) = service.finish(result);
                    ctx.respond_value(route, response)?;
                    Ok(disposition)
                }
                ROLLOUT_FINALIZE_SHADOW_ROUTE | ROLLOUT_CLEAR_STATUS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let task_host = Arc::clone(&host);
                    let task_route = route.clone();
                    let response = run_blocking_rollout_host_task(
                        route.clone(),
                        "rollout control",
                        move || match task_route.as_str() {
                            ROLLOUT_FINALIZE_SHADOW_ROUTE => task_host.finalize_shadow(),
                            ROLLOUT_CLEAR_STATUS_ROUTE => task_host.clear_status(),
                            _ => unreachable!("matched rollout control route"),
                        },
                    )
                    .await?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(route, response)?;
                    Ok(ServiceDisposition::Continue)
                }
                other => Err(ServiceError::UnknownRoute(other.to_string())),
            }
        })
    }
}

async fn decode_rollout_build_session_request(
    ctx: &mut ServiceContext,
    request: &ObjectRpcRequest,
) -> std::result::Result<ClusterRolloutBuildRequest, ServiceError> {
    let mut payload: ClusterRolloutBuildRequest = ctx.decode_request_body(request).await?;
    let mut body = ctx.take_request_body();
    if body.is_empty() {
        return Ok(payload);
    }
    if payload.source_bundle.is_some() {
        return Err(ServiceError::Service(
            "rollout build source bundle must be carried either in the request head or the session body, not both"
                .to_string(),
        ));
    }
    let mut source_bundle = Vec::new();
    body.read_to_end(&mut source_bundle)
        .await
        .map_err(|source| {
            ServiceError::Service(format!(
                "failed to receive rollout build source bundle body: {source}"
            ))
        })?;
    payload.source_bundle = Some(source_bundle);
    Ok(payload)
}

async fn run_blocking_rollout_host_task<T>(
    route: String,
    label: &'static str,
    task: impl FnOnce() -> std::result::Result<T, String> + Send + 'static,
) -> std::result::Result<std::result::Result<T, String>, ServiceError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(task).await.map_err(|source| {
        ServiceError::Service(format!("{label} route {route} task failed: {source}"))
    })
}

fn rollout_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = rollout_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("rollout module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown rollout service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
    .with_concurrency_limit(8)
}

fn rollout_host_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = rollout_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("rollout module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown hosted rollout service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(RolloutService::new())
}

pub fn register_host_services(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn RolloutServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedRolloutService::new(host))
}

/// Component factory for rollout-owned generic and host-effect services.
pub fn component_factory(
    host: Arc<dyn RolloutServiceHost>,
) -> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    component_factory_with_telemetry(host, None, "daemon")
}

pub fn component_factory_with_telemetry(
    host: Arc<dyn RolloutServiceHost>,
    telemetry_host: Option<Arc<dyn crate::RolloutTelemetryHost>>,
    label: impl Into<String>,
) -> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor = sinomo_module::component_descriptor_from_registration(
        &crate::rollout_module_registration(),
    )?;
    let label = label.into();
    Ok(
        sinomo_module::StaticComponentFactory::new(descriptor, move |context, scope| {
            let builder = register_services(ServiceRegistryBuilder::default());
            scope.set_services(register_host_services(builder, Arc::clone(&host)).build());
            if let Some(telemetry_host) = telemetry_host.as_ref() {
                let runtime = context.runtime().ok_or_else(|| {
                    sinomo_module::ComponentError::RuntimeUnavailable {
                        component_id: crate::SINOMO_ROLLOUT_MODULE_LEGACY_ID.to_string(),
                    }
                })?;
                let reactor = crate::RolloutTelemetryReactor::new(context.data_dir_root())
                    .map_err(|error| {
                        sinomo_module::ComponentError::component(
                            crate::SINOMO_ROLLOUT_MODULE_LEGACY_ID,
                            error,
                        )
                    })?;
                let task = runtime.spawn(reactor.run_until_stopped_async(
                    Arc::clone(telemetry_host),
                    scope.shutdown_receiver(),
                    label.clone(),
                ));
                scope.own_task("rollout telemetry reconciliation", task);
            }
            Ok(())
        })?
        .with_storage_paths(crate::register_storage_paths)
        .with_config_definitions(register_component_config_definitions),
    )
}

fn register_component_config_definitions(
    builder: sinomo_config::ConfigRegistryBuilder,
) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
    crate::register_config_definitions(builder).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse};
    use sinomo_core::service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<ObjectRpcOutgoingResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    struct TestRolloutHost {
        stop: bool,
    }

    impl RolloutServiceHost for TestRolloutHost {
        fn activate_binary<'a>(
            &'a self,
            _manifest: ClusterRolloutBinaryManifest,
        ) -> RolloutHostFuture<'a, std::result::Result<ClusterRolloutBinaryResponse, String>>
        {
            Box::pin(async { Err("binary activation not expected".to_string()) })
        }

        fn activate_native_dylib(
            &self,
            _request: ClusterRolloutNativeDylibModuleRequest,
        ) -> std::result::Result<ClusterRolloutBinaryResponse, String> {
            Err("native activation not expected".to_string())
        }

        fn build(
            &self,
            _request: ClusterRolloutBuildRequest,
        ) -> std::result::Result<ClusterRolloutBinaryResponse, String> {
            Err("build not expected".to_string())
        }

        fn finalize_shadow(&self) -> std::result::Result<String, String> {
            Err("finalization not expected".to_string())
        }

        fn clear_status(&self) -> std::result::Result<String, String> {
            Err("clear not expected".to_string())
        }

        fn should_stop_after(&self, _response: &ClusterRolloutBinaryResponse) -> bool {
            self.stop
        }
    }

    #[test]
    fn rollout_service_route_metadata_matches_module_surfaces() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let service_route = registry
            .route_categories()
            .into_iter()
            .find(|route| route.route == ROLLOUT_DIAGNOSTICS_ROUTE)
            .expect("rollout diagnostics service route");
        let module_route = rollout_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("rollout module surfaces")
            .into_iter()
            .find(|route| route.route == ROLLOUT_DIAGNOSTICS_ROUTE)
            .expect("rollout diagnostics module route");

        assert_eq!(service_route.category, module_route.category);
        assert_eq!(
            service_route.auth_scope.as_deref(),
            Some(module_route.auth_scope.as_str())
        );
        assert_eq!(
            service_route.required_capabilities,
            module_route
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(service_route.concurrency_limit, Some(8));
    }

    #[test]
    fn hosted_rollout_routes_match_module_surfaces_and_stream_modes() {
        let registry = register_host_services(
            ServiceRegistry::builder(),
            Arc::new(TestRolloutHost { stop: false }),
        )
        .build();
        let module_routes = rollout_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("rollout module surfaces");
        for route in ROLLOUT_HOST_ROUTES {
            let service_route = registry
                .route(route)
                .unwrap_or_else(|| panic!("hosted rollout route {route}"));
            let module_route = module_routes
                .iter()
                .find(|descriptor| descriptor.route == *route)
                .unwrap_or_else(|| panic!("rollout module route {route}"));
            assert_eq!(service_route.category, module_route.category);
            assert_eq!(
                service_route.auth_scope.as_deref(),
                Some(module_route.auth_scope.as_str())
            );
            assert_eq!(
                service_route.request_body_mode,
                match *route {
                    ROLLOUT_BINARY_ROUTE => ServiceBodyMode::Required,
                    ROLLOUT_BUILD_ROUTE => ServiceBodyMode::Optional,
                    _ => ServiceBodyMode::None,
                }
            );
        }
    }

    #[test]
    fn hosted_rollout_errors_remain_typed_failed_responses() {
        let service = HostedRolloutService::new(Arc::new(TestRolloutHost { stop: false }));
        let (response, disposition) = service.finish(Err("activation failed".to_string()));

        assert_eq!(response.status, "failed");
        assert_eq!(response.detail, "activation failed");
        assert_eq!(disposition, ServiceDisposition::Continue);
    }

    #[tokio::test]
    async fn rollout_build_session_body_rehydrates_source_bundle() {
        let payload = ClusterRolloutBuildRequest {
            source: "test-source".to_string(),
            remote_repo: None,
            source_bundle: None,
            source_head: Some("abc123".to_string()),
            force_source_overwrite: false,
            host: "127.0.0.1".to_string(),
            host_override: None,
            port: 21_353,
            shadow_port: 21_354,
            requested_at: "2026-07-12T00:00:00Z".to_string(),
        };
        let request = ObjectRpcRequest::encode_body(
            ROLLOUT_BUILD_ROUTE,
            crate::ROLLOUT_RPC_CATEGORY,
            &payload,
        )
        .expect("request head");
        let expected_bundle = vec![9_u8; 48 * 1024];
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-build-session-body");
        let mut context = ServiceContext::new(
            Arc::new(SinomoDataDir::new(&root)),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::bytes(expected_bundle.clone()),
            Box::new(VecSink::default()),
        );

        let decoded = decode_rollout_build_session_request(&mut context, &request)
            .await
            .expect("session request");

        assert_eq!(decoded.source_bundle, Some(expected_bundle));
        assert_eq!(decoded.source_head, payload.source_head);
    }

    #[tokio::test]
    async fn rollout_diagnostics_serves_in_local_mode() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let dir = sinomo_testdir::TestDir::new("sinomo-rollout-service");
        let data_dir = Arc::new(SinomoDataDir::new(&dir));
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext::new(
            data_dir,
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        );
        let request = ObjectRpcRequest::encode_body(
            ROLLOUT_DIAGNOSTICS_ROUTE,
            crate::ROLLOUT_RPC_CATEGORY,
            &(),
        )
        .expect("encode");

        let disposition = registry.dispatch(ctx, request).await.expect("dispatch");
        assert_eq!(disposition, ServiceDisposition::Continue);
        let response = frames
            .lock()
            .expect("frames")
            .pop()
            .expect("a response frame")
            .into_materialized_value_response(usize::MAX)
            .await
            .expect("materialize diagnostics response");
        let report: String = response.decode_body().expect("decode report");
        assert!(report.contains("rollout root:"));
    }
}
