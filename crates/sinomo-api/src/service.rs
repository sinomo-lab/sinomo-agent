//! Hosted service adapters for daemon lifecycle and process-operation APIs.

use crate::{
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE, DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE, DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE, DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE, DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE, DAEMON_RPC_CATEGORY,
    DAEMON_SHUTDOWN_ROUTE, DAEMON_STATUS_ROUTE, DaemonClusterMaintenanceRefreshRequest,
    DaemonClusterMaintenanceRefreshResponse, DaemonLocalConnectivityReloadRequest,
    DaemonLocalConnectivityReloadResponse, DaemonNetworkChunkMetricsResponse,
    DaemonObjectRpcDestinationProcessAffinity, DaemonObjectRpcForwardRequest,
    DaemonObjectRpcForwardResponse, DaemonStatus,
};
use sinomo_core::network::{
    NetworkError, ObjectRpcBodySource, ObjectRpcIncomingBody, ObjectRpcRequest, ObjectRpcResponse,
};
use sinomo_core::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const MAIN_LIFECYCLE_ROUTES: &[&str] = &[DAEMON_STATUS_ROUTE, DAEMON_SHUTDOWN_ROUTE];
const SHADOW_LIFECYCLE_ROUTES: &[&str] = &[
    DAEMON_STATUS_ROUTE,
    DAEMON_SHUTDOWN_ROUTE,
    DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
];

pub type DaemonLifecycleFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonShutdownKind {
    Local,
    RemoteShadow,
}

/// Process effects required by the daemon lifecycle wire service.
pub trait DaemonLifecycleHost: Send + Sync + 'static {
    fn is_shadow(&self) -> bool;

    fn status(&self) -> DaemonLifecycleFuture<'_, Result<DaemonStatus, String>>;

    fn shutdown_requested(&self, kind: DaemonShutdownKind);
}

pub struct HostedDaemonLifecycleService {
    host: Arc<dyn DaemonLifecycleHost>,
}

impl HostedDaemonLifecycleService {
    pub fn new(host: Arc<dyn DaemonLifecycleHost>) -> Self {
        Self { host }
    }
}

impl Service for HostedDaemonLifecycleService {
    fn name(&self) -> &'static str {
        "daemon-lifecycle"
    }

    fn routes(&self) -> &'static [&'static str] {
        if self.host.is_shadow() {
            SHADOW_LIFECYCLE_ROUTES
        } else {
            MAIN_LIFECYCLE_ROUTES
        }
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                let mut descriptor = ServiceRoute::new(route, DAEMON_RPC_CATEGORY);
                if route == DAEMON_SHUTDOWN_ROUTE {
                    descriptor.auth_scope = Some("local-command".to_string());
                } else if route == DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE {
                    descriptor.auth_scope = Some("local-or-cluster-voter".to_string());
                }
                descriptor
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let _: () = ctx.decode_request_body(&request).await?;
            match request.route.as_str() {
                DAEMON_STATUS_ROUTE => {
                    let status = host.status().await.map_err(ServiceError::Service)?;
                    ctx.respond_value(DAEMON_STATUS_ROUTE, status)?;
                    Ok(ServiceDisposition::Continue)
                }
                DAEMON_SHUTDOWN_ROUTE => {
                    host.shutdown_requested(DaemonShutdownKind::Local);
                    ctx.respond_value(DAEMON_SHUTDOWN_ROUTE, "sinomo daemon stopping".to_string())?;
                    Ok(ServiceDisposition::Stop)
                }
                DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE if host.is_shadow() => {
                    host.shutdown_requested(DaemonShutdownKind::RemoteShadow);
                    ctx.respond_value(
                        DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
                        "sinomo shadow daemon stopping".to_string(),
                    )?;
                    Ok(ServiceDisposition::Stop)
                }
                other => Err(ServiceError::UnknownRoute(other.to_string())),
            }
        })
    }
}

pub fn register_hosted_daemon_lifecycle_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn DaemonLifecycleHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedDaemonLifecycleService::new(host))
}

const MAIN_OPERATIONS_ROUTES: &[&str] = &[
    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
];
const SHADOW_OPERATIONS_ROUTES: &[&str] = &[
    DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
    DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
];

pub type DaemonOperationsFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Process effects required by the daemon operations wire service.
///
/// Route ownership, decoding, safety checks, authorization metadata, response
/// framing, and main/shadow exposure are kept in this API crate. The host only
/// bridges those operations to the daemon's live network and DFS resources.
pub trait DaemonOperationsHost: Send + Sync + 'static {
    fn is_shadow(&self) -> bool;

    fn forward_object_rpc(
        &self,
        payload: DaemonObjectRpcForwardRequest,
        request_body: ObjectRpcIncomingBody,
    ) -> DaemonOperationsFuture<
        '_,
        Result<(DaemonObjectRpcForwardResponse, ObjectRpcBodySource), ServiceError>,
    >;

    fn cluster_maintenance_refresh_requested(&self, traceparent: Option<String>);

    fn reload_local_connectivity(&self) -> Result<(), ServiceError>;
}

pub struct HostedDaemonOperationsService {
    host: Arc<dyn DaemonOperationsHost>,
}

impl HostedDaemonOperationsService {
    pub fn new(host: Arc<dyn DaemonOperationsHost>) -> Self {
        Self { host }
    }
}

impl Service for HostedDaemonOperationsService {
    fn name(&self) -> &'static str {
        "daemon-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        if self.host.is_shadow() {
            SHADOW_OPERATIONS_ROUTES
        } else {
            MAIN_OPERATIONS_ROUTES
        }
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                let mut descriptor = ServiceRoute::new(route, DAEMON_RPC_CATEGORY);
                descriptor.auth_scope = Some("local-command".to_string());
                if is_daemon_object_rpc_forward_route(route) {
                    descriptor.request_body_mode = ServiceBodyMode::Optional;
                    descriptor.response_body_mode = ServiceBodyMode::Optional;
                }
                descriptor
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            if host.is_shadow()
                && matches!(
                    request.route.as_str(),
                    DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE
                )
            {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            if is_daemon_object_rpc_forward_route(request.route.as_str()) {
                let forward_route = request.route.clone();
                let mut payload: DaemonObjectRpcForwardRequest =
                    ctx.decode_request_body(&request).await?;
                enforce_daemon_object_rpc_forward_route_affinity(&forward_route, &mut payload)
                    .map_err(ServiceError::Network)?;
                let (response, response_body) = host
                    .forward_object_rpc(payload, ctx.take_request_body())
                    .await?;
                ctx.respond_with_body(
                    ObjectRpcResponse::encode_body(forward_route, &response)?,
                    response_body,
                )?;
                return Ok(ServiceDisposition::Continue);
            }
            match request.route.as_str() {
                DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE => {
                    let _: DaemonClusterMaintenanceRefreshRequest =
                        ctx.decode_request_body(&request).await?;
                    host.cluster_maintenance_refresh_requested(
                        request.traceparent().map(ToString::to_string),
                    );
                    ctx.respond_value(
                        DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
                        DaemonClusterMaintenanceRefreshResponse,
                    )?;
                }
                DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE => {
                    let _: DaemonLocalConnectivityReloadRequest =
                        ctx.decode_request_body(&request).await?;
                    host.reload_local_connectivity()?;
                    ctx.respond_value(
                        DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
                        DaemonLocalConnectivityReloadResponse,
                    )?;
                }
                DAEMON_NETWORK_CHUNK_METRICS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
                        DaemonNetworkChunkMetricsResponse {
                            chunk_metrics: sinomo_core::network::network_chunk_metrics(),
                        },
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_hosted_daemon_operations_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn DaemonOperationsHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedDaemonOperationsService::new(host))
}

fn is_daemon_object_rpc_forward_route(route: &str) -> bool {
    matches!(
        route,
        DAEMON_OBJECT_RPC_FORWARD_ROUTE
            | DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
            | DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
            | DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE
    )
}

pub fn enforce_daemon_object_rpc_forward_route_affinity(
    route: &str,
    payload: &mut DaemonObjectRpcForwardRequest,
) -> Result<(), NetworkError> {
    match route {
        DAEMON_OBJECT_RPC_FORWARD_ROUTE | DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE => {
            if payload.destination_process_affinity
                != DaemonObjectRpcDestinationProcessAffinity::Any
            {
                return Err(NetworkError::InvalidClusterCommand {
                    detail: format!(
                        "explicit main-daemon affinity requires a fail-closed main forwarding route, not legacy route {route}"
                    ),
                });
            }
        }
        DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE | DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE => {
            payload.destination_process_affinity =
                DaemonObjectRpcDestinationProcessAffinity::MainDaemon;
        }
        _ => {
            return Err(NetworkError::UnknownRoute {
                route: route.to_string(),
            });
        }
    }
    Ok(())
}

/// Builds the routed-session options carried by a daemon forward request.
/// Transport execution remains a daemon-host concern; timeout and destination
/// affinity semantics belong to this API contract.
pub fn daemon_object_rpc_forward_options(
    payload: &DaemonObjectRpcForwardRequest,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    let transport_timeouts = daemon_object_rpc_forward_transport_timeouts(payload);
    let mut options = sinomo_core::network::ClusterObjectRpcOptions::new(transport_timeouts);
    if let Some(overall_timeout_millis) = payload.overall_timeout_millis {
        options = options.with_overall_timeout(Duration::from_millis(overall_timeout_millis));
    }
    match payload.destination_process_affinity {
        DaemonObjectRpcDestinationProcessAffinity::Any => options,
        DaemonObjectRpcDestinationProcessAffinity::MainDaemon => {
            sinomo_core::cluster::main_daemon_cluster_rpc_options(options)
        }
    }
}

pub fn daemon_object_rpc_forward_transport_timeouts(
    payload: &DaemonObjectRpcForwardRequest,
) -> sinomo_core::network::DaemonRpcTimeouts {
    let mut timeouts = sinomo_core::network::DaemonRpcTimeouts::default()
        .with_connect_timeout(Some(Duration::from_millis(payload.connect_timeout_millis)));
    if let Some(idle_timeout_millis) = payload.idle_timeout_millis {
        timeouts = timeouts.with_idle_timeout(Some(Duration::from_millis(idle_timeout_millis)));
    }
    timeouts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forward_request() -> DaemonObjectRpcForwardRequest {
        DaemonObjectRpcForwardRequest {
            target_node_id: sinomo_core::node::NodeId::from_legacy_named(
                "node_daemon_forward_options",
            )
            .expect("node id"),
            request: ObjectRpcRequest::encode_body("/test/daemon-forward", "test", &())
                .expect("request"),
            streamed_value_body: false,
            connect_timeout_millis: 5_000,
            idle_timeout_millis: None,
            overall_timeout_millis: None,
            observe_connectivity: false,
            destination_process_affinity: DaemonObjectRpcDestinationProcessAffinity::Any,
        }
    }

    #[test]
    fn forward_options_preserve_distinct_transport_timeouts() {
        let mut payload = forward_request();
        payload.idle_timeout_millis = Some(310_000);

        let timeouts = daemon_object_rpc_forward_transport_timeouts(&payload);

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(5)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(310)));
    }

    #[test]
    fn forward_options_retain_default_idle_timeout_when_omitted() {
        let mut payload = forward_request();
        payload.connect_timeout_millis = 7_000;

        let timeouts = daemon_object_rpc_forward_transport_timeouts(&payload);

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(7)));
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn forward_options_preserve_explicit_process_affinity() {
        let mut payload = forward_request();
        payload.overall_timeout_millis = Some(15_000);
        assert_eq!(
            daemon_object_rpc_forward_options(&payload).destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::Any,
        );

        payload.destination_process_affinity =
            DaemonObjectRpcDestinationProcessAffinity::MainDaemon;
        assert_eq!(
            daemon_object_rpc_forward_options(&payload).destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::MainDaemon,
        );
    }

    struct TestHost {
        shadow: bool,
    }

    impl DaemonLifecycleHost for TestHost {
        fn is_shadow(&self) -> bool {
            self.shadow
        }

        fn status(&self) -> DaemonLifecycleFuture<'_, Result<DaemonStatus, String>> {
            Box::pin(async { Err("unused test status".to_string()) })
        }

        fn shutdown_requested(&self, _kind: DaemonShutdownKind) {}
    }

    struct TestOperationsHost {
        shadow: bool,
    }

    impl DaemonOperationsHost for TestOperationsHost {
        fn is_shadow(&self) -> bool {
            self.shadow
        }

        fn forward_object_rpc(
            &self,
            _payload: DaemonObjectRpcForwardRequest,
            _request_body: ObjectRpcIncomingBody,
        ) -> DaemonOperationsFuture<
            '_,
            Result<(DaemonObjectRpcForwardResponse, ObjectRpcBodySource), ServiceError>,
        > {
            Box::pin(async { Err(ServiceError::Service("unused test forward".to_string())) })
        }

        fn cluster_maintenance_refresh_requested(&self, _traceparent: Option<String>) {}

        fn reload_local_connectivity(&self) -> Result<(), ServiceError> {
            Ok(())
        }
    }

    #[test]
    fn lifecycle_routes_preserve_main_and_shadow_authority() {
        let main = HostedDaemonLifecycleService::new(Arc::new(TestHost { shadow: false }));
        assert_eq!(main.routes(), MAIN_LIFECYCLE_ROUTES);
        assert_eq!(
            main.route_categories()
                .into_iter()
                .find(|route| route.route == DAEMON_SHUTDOWN_ROUTE)
                .and_then(|route| route.auth_scope),
            Some("local-command".to_string())
        );

        let shadow = HostedDaemonLifecycleService::new(Arc::new(TestHost { shadow: true }));
        assert_eq!(shadow.routes(), SHADOW_LIFECYCLE_ROUTES);
        assert_eq!(
            shadow
                .route_categories()
                .into_iter()
                .find(|route| route.route == DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE)
                .and_then(|route| route.auth_scope),
            Some("local-or-cluster-voter".to_string())
        );
    }

    #[test]
    fn operations_routes_preserve_main_shadow_and_body_metadata() {
        let main =
            HostedDaemonOperationsService::new(Arc::new(TestOperationsHost { shadow: false }));
        assert_eq!(main.routes(), MAIN_OPERATIONS_ROUTES);
        let routes = main.route_categories();
        let forward = routes
            .iter()
            .find(|route| route.route == DAEMON_OBJECT_RPC_FORWARD_ROUTE)
            .expect("forward route");
        assert_eq!(forward.auth_scope.as_deref(), Some("local-command"));
        assert_eq!(forward.request_body_mode, ServiceBodyMode::Optional);
        assert_eq!(forward.response_body_mode, ServiceBodyMode::Optional);
        let shadow =
            HostedDaemonOperationsService::new(Arc::new(TestOperationsHost { shadow: true }));
        assert_eq!(shadow.routes(), SHADOW_OPERATIONS_ROUTES);
    }
}
