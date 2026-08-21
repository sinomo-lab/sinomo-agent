//! Hosted cluster-network service routes.

use crate::ClusterHealthRow;
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    CLUSTER_HEALTH_READ_CAPABILITY, CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY,
    CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY, CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_CAPABILITY, CLUSTER_NETWORK_PING_ROUTE, CLUSTER_NETWORK_RPC_CATEGORY,
    ClusterConnectivityRecord, ClusterPaths, NetworkError, ObjectRpcBodySource,
    ObjectRpcIncomingRequest, ObjectRpcOutgoingResponse, ObjectRpcRequest,
};
use sinomo_service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

const CLUSTER_NETWORK_MAIN_ROUTES: &[&str] = &[
    CLUSTER_HEALTH_ROUTE,
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE,
];
const CLUSTER_NETWORK_PASSIVE_ROUTES: &[&str] = &[
    CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
    CLUSTER_NETWORK_PING_ROUTE,
];

pub type ClusterNetworkServiceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Process effects required by the cluster-owned network service adapter.
pub trait ClusterNetworkServiceHost: Send + Sync + 'static {
    fn include_authority_health(&self) -> bool;

    fn handle_ping<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ObjectRpcIncomingRequest,
    ) -> ClusterNetworkServiceFuture<'a, Result<ObjectRpcOutgoingResponse, ServiceError>>;

    fn authority_health<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ObjectRpcRequest,
    ) -> ClusterNetworkServiceFuture<'a, Result<Vec<ClusterHealthRow>, ServiceError>>;
}

pub struct HostedClusterNetworkService {
    host: Arc<dyn ClusterNetworkServiceHost>,
}

impl HostedClusterNetworkService {
    pub fn new(host: Arc<dyn ClusterNetworkServiceHost>) -> Self {
        Self { host }
    }
}

fn cluster_network_service_route(route: &'static str) -> ServiceRoute {
    let (category, capability) = match route {
        CLUSTER_HEALTH_ROUTE => (CLUSTER_HEALTH_RPC_CATEGORY, CLUSTER_HEALTH_READ_CAPABILITY),
        CLUSTER_NETWORK_CONNECTIVITY_ROUTE => (
            CLUSTER_NETWORK_RPC_CATEGORY,
            CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY,
        ),
        CLUSTER_NETWORK_PING_ROUTE => (
            CLUSTER_NETWORK_RPC_CATEGORY,
            CLUSTER_NETWORK_PING_CAPABILITY,
        ),
        other => panic!("unknown cluster-network service route {other}"),
    };
    let descriptor =
        ServiceRoute::from_route_metadata(route, category, "local-or-cluster-member", [capability]);
    if route == CLUSTER_NETWORK_PING_ROUTE {
        descriptor
            .with_request_body_mode(ServiceBodyMode::Optional)
            .with_response_body_mode(ServiceBodyMode::Optional)
            .with_incremental_request_value()
    } else {
        descriptor
    }
}

fn read_bounded_cluster_connectivity_snapshot(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, ServiceError> {
    let records = sinomo_network::read_local_cluster_connectivity_records(paths)?;
    if records.len() > sinomo_network::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(
            sinomo_network::NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records: records.len(),
                max_records: sinomo_network::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            }
            .into(),
        );
    }
    Ok(records)
}

impl Service for HostedClusterNetworkService {
    fn name(&self) -> &'static str {
        "cluster-network"
    }

    fn routes(&self) -> &'static [&'static str] {
        if self.host.include_authority_health() {
            CLUSTER_NETWORK_MAIN_ROUTES
        } else {
            CLUSTER_NETWORK_PASSIVE_ROUTES
        }
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(cluster_network_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            match request.route.as_str() {
                CLUSTER_NETWORK_PING_ROUTE => {
                    let request_body = ctx.take_request_body();
                    let response = host
                        .handle_ping(
                            ctx.data_dir.as_ref(),
                            ObjectRpcIncomingRequest::new(request, request_body),
                        )
                        .await?;
                    ctx.respond_with_body(response.response, response.body)?;
                }
                CLUSTER_NETWORK_CONNECTIVITY_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
                    let response = tokio::task::spawn_blocking(move || {
                        read_bounded_cluster_connectivity_snapshot(&cluster_paths)
                    })
                    .await
                    .map_err(|source| {
                        ServiceError::Service(format!(
                            "cluster network connectivity task failed: {source}"
                        ))
                    })??;
                    ctx.respond_value(CLUSTER_NETWORK_CONNECTIVITY_ROUTE, response)?;
                }
                CLUSTER_HEALTH_ROUTE if host.include_authority_health() => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let response = host
                        .authority_health(ctx.data_dir.as_ref(), request)
                        .await?;
                    ctx.respond_value(CLUSTER_HEALTH_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_cluster_network_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn ClusterNetworkServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedClusterNetworkService::new(host))
}

/// Handles both typed-value and semantic-attachment network pings for one responder.
pub async fn handle_cluster_network_ping_service_request_for_node(
    responder_node_id: &sinomo_node::NodeId,
    request: ObjectRpcIncomingRequest,
    timeline: Option<sinomo_network::NetworkTimelineTraceContext>,
) -> Result<ObjectRpcOutgoingResponse, ServiceError> {
    if request.has_streamed_value() {
        let decode_started_ns = cluster_network_service_timeline_started_ns(timeline);
        let ping_request: Result<crate::ClusterNetworkPingRequest, _> = request.decode_body().await;
        record_cluster_network_service_timeline_stage(
            timeline,
            sinomo_network::network_timeline_stage::PING_REQUEST_DECODE,
            sinomo_network::network_timeline_flag::REQUEST,
            decode_started_ns,
            ping_request.is_ok(),
        );
        let ping_request = ping_request?;
        let build_started_ns = cluster_network_service_timeline_started_ns(timeline);
        let response = crate::cluster_network_ping_response_for_node(
            responder_node_id,
            ping_request,
            time::OffsetDateTime::now_utc(),
        );
        record_cluster_network_service_timeline_stage(
            timeline,
            sinomo_network::network_timeline_stage::PING_RESPONSE_BUILD,
            sinomo_network::network_timeline_flag::RESPONSE,
            build_started_ns,
            response.is_ok(),
        );
        let response = response?;
        let encode_started_ns = cluster_network_service_timeline_started_ns(timeline);
        let encoded = ObjectRpcOutgoingResponse::encode_value(CLUSTER_NETWORK_PING_ROUTE, response);
        record_cluster_network_service_timeline_stage(
            timeline,
            sinomo_network::network_timeline_stage::PING_RESPONSE_ENCODE,
            sinomo_network::network_timeline_flag::RESPONSE,
            encode_started_ns,
            encoded.is_ok(),
        );
        return encoded.map_err(ServiceError::from);
    }

    let ObjectRpcIncomingRequest {
        request: request_head,
        body: request_body,
    } = request;
    let decode_started_ns = cluster_network_service_timeline_started_ns(timeline);
    let ping_request = crate::decode_cluster_network_ping_rpc_request(&request_head);
    record_cluster_network_service_timeline_stage(
        timeline,
        sinomo_network::network_timeline_stage::PING_REQUEST_DECODE,
        sinomo_network::network_timeline_flag::REQUEST,
        decode_started_ns,
        ping_request.is_ok(),
    );
    let ping_request = ping_request?;

    match ping_request.payload_mode {
        crate::ClusterNetworkPingPayloadMode::EmbeddedSobj => {
            if !request_body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "embedded-SOBJ cluster network ping does not accept an attached body"
                        .to_string(),
                }
                .into());
            }
            let build_started_ns = cluster_network_service_timeline_started_ns(timeline);
            let response = crate::cluster_network_ping_response_for_node(
                responder_node_id,
                ping_request,
                time::OffsetDateTime::now_utc(),
            );
            record_cluster_network_service_timeline_stage(
                timeline,
                sinomo_network::network_timeline_stage::PING_RESPONSE_BUILD,
                sinomo_network::network_timeline_flag::RESPONSE,
                build_started_ns,
                response.is_ok(),
            );
            let response = response?;
            let encode_started_ns = cluster_network_service_timeline_started_ns(timeline);
            let encoded =
                ObjectRpcOutgoingResponse::encode_value(CLUSTER_NETWORK_PING_ROUTE, response);
            record_cluster_network_service_timeline_stage(
                timeline,
                sinomo_network::network_timeline_stage::PING_RESPONSE_ENCODE,
                sinomo_network::network_timeline_flag::RESPONSE,
                encode_started_ns,
                encoded.is_ok(),
            );
            encoded.map_err(ServiceError::from)
        }
        crate::ClusterNetworkPingPayloadMode::SemanticAttachment => {
            crate::validate_cluster_network_ping_attachment_request(&ping_request)?;
            let declared_body_bytes = request_body.total_bytes();
            if declared_body_bytes != Some(ping_request.request_attachment_bytes)
                && !(ping_request.request_attachment_bytes == 0 && request_body.is_empty())
            {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "cluster network ping request attachment descriptor declares {declared_body_bytes:?} bytes, head declares {}",
                        ping_request.request_attachment_bytes
                    ),
                }
                .into());
            }

            // Consume ownership-bearing chunks directly. This keeps the benchmark on the
            // canonical bounded network body path without an application AsyncRead copy loop.
            let actual_request_attachment_bytes = request_body
                .discard_owned_chunks()
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "drain cluster network ping request attachment".to_string(),
                    detail: source.to_string(),
                })?;
            if actual_request_attachment_bytes != ping_request.request_attachment_bytes {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "cluster network ping request attachment delivered {actual_request_attachment_bytes} bytes, head declares {}",
                        ping_request.request_attachment_bytes
                    ),
                }
                .into());
            }

            let response = crate::cluster_network_ping_attachment_rpc_response_for_node(
                responder_node_id,
                &ping_request,
                actual_request_attachment_bytes,
                time::OffsetDateTime::now_utc(),
            )?;
            let response_body = ObjectRpcBodySource::zeroes(ping_request.response_attachment_bytes);
            Ok(ObjectRpcOutgoingResponse::new(response, response_body))
        }
    }
}

#[inline]
fn cluster_network_service_timeline_started_ns(
    timeline: Option<sinomo_network::NetworkTimelineTraceContext>,
) -> u64 {
    timeline
        .map(|_| sinomo_network::network_timeline_monotonic_ns())
        .unwrap_or(0)
}

#[inline]
fn record_cluster_network_service_timeline_stage(
    timeline: Option<sinomo_network::NetworkTimelineTraceContext>,
    stage: u16,
    flags: u16,
    started_ns: u64,
    success: bool,
) {
    let Some(trace) = timeline else {
        return;
    };
    sinomo_network::record_network_timeline_event_now(sinomo_network::NetworkTimelineEvent::new(
        trace,
        stage,
        flags
            | if success {
                sinomo_network::network_timeline_flag::SUCCESS
            } else {
                0
            },
        0,
        0,
        0,
        0,
        0,
        sinomo_network::network_timeline_monotonic_ns().saturating_sub(started_ns),
    ));
}
#[cfg(test)]
mod tests {
    use super::*;

    struct TestHost {
        include_authority_health: bool,
    }

    impl ClusterNetworkServiceHost for TestHost {
        fn include_authority_health(&self) -> bool {
            self.include_authority_health
        }

        fn handle_ping<'a>(
            &'a self,
            _data_dir: &'a SinomoDataDir,
            _request: ObjectRpcIncomingRequest,
        ) -> ClusterNetworkServiceFuture<'a, Result<ObjectRpcOutgoingResponse, ServiceError>>
        {
            Box::pin(async { Err(ServiceError::Service("unused ping".to_string())) })
        }

        fn authority_health<'a>(
            &'a self,
            _data_dir: &'a SinomoDataDir,
            _request: ObjectRpcRequest,
        ) -> ClusterNetworkServiceFuture<'a, Result<Vec<ClusterHealthRow>, ServiceError>> {
            Box::pin(async { Err(ServiceError::Service("unused health".to_string())) })
        }
    }

    #[test]
    fn cluster_network_routes_preserve_authority_and_body_modes() {
        let main = HostedClusterNetworkService::new(Arc::new(TestHost {
            include_authority_health: true,
        }));
        assert_eq!(main.routes(), CLUSTER_NETWORK_MAIN_ROUTES);
        let ping = main
            .route_categories()
            .into_iter()
            .find(|route| route.route == CLUSTER_NETWORK_PING_ROUTE)
            .expect("ping route");
        assert_eq!(ping.auth_scope.as_deref(), Some("local-or-cluster-member"));
        assert_eq!(ping.request_body_mode, ServiceBodyMode::Optional);
        assert_eq!(ping.response_body_mode, ServiceBodyMode::Optional);
        assert_eq!(
            ping.required_capabilities,
            [CLUSTER_NETWORK_PING_CAPABILITY]
        );

        let passive = HostedClusterNetworkService::new(Arc::new(TestHost {
            include_authority_health: false,
        }));
        assert_eq!(passive.routes(), CLUSTER_NETWORK_PASSIVE_ROUTES);
    }
}
