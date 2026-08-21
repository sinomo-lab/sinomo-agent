//! Hosted Raft consensus object-RPC service.

use crate::{
    ClusterAuthenticatedPeer, handle_cluster_raft_append_request_async,
    handle_cluster_raft_vote_request_async,
};
use sinomo_kv::{
    CLUSTER_RAFT_APPEND_ROUTE, CLUSTER_RAFT_RPC_ROUTE_POLICIES, CLUSTER_RAFT_RPC_ROUTES,
    CLUSTER_RAFT_VOTE_ROUTE, ClusterRaftAppendRequest, ClusterRaftVoteRequest,
};
use sinomo_network::{ClusterPaths, ObjectRpcRequest};
use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use std::sync::Arc;

pub trait ClusterConsensusServiceHost: Send + Sync + 'static {
    fn raft_append_received(&self);
}

pub struct HostedClusterConsensusService {
    runtime: Option<tokio::runtime::Handle>,
    host: Arc<dyn ClusterConsensusServiceHost>,
}

impl HostedClusterConsensusService {
    pub fn new(
        runtime: Option<tokio::runtime::Handle>,
        host: Arc<dyn ClusterConsensusServiceHost>,
    ) -> Self {
        Self { runtime, host }
    }
}

fn authenticated_peer(
    signer: &ServiceSigner,
    route: &str,
) -> Result<ClusterAuthenticatedPeer, ServiceError> {
    let Some((node_id, public_key)) = signer.cluster_identity() else {
        return Err(ServiceError::Service(format!(
            "cluster route {route} requires an authenticated cluster signer identity"
        )));
    };
    Ok(ClusterAuthenticatedPeer {
        node_id: node_id.to_string(),
        public_key: public_key.to_string(),
    })
}

impl Service for HostedClusterConsensusService {
    fn name(&self) -> &'static str {
        "cluster-kv-consensus"
    }

    fn routes(&self) -> &'static [&'static str] {
        CLUSTER_RAFT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        CLUSTER_RAFT_RPC_ROUTE_POLICIES
            .iter()
            .map(|policy| {
                ServiceRoute::from_route_metadata(
                    policy.route,
                    policy.category,
                    policy.auth.as_scope(),
                    policy.required_capabilities.iter().copied(),
                )
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let route = request.route.clone();
            if route == CLUSTER_RAFT_APPEND_ROUTE {
                host.raft_append_received();
            }
            let runtime = runtime.as_ref().ok_or_else(|| {
                ServiceError::Service(
                    "cluster kv consensus rpc requires a service runtime".to_string(),
                )
            })?;
            let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
            let authenticated_peer = authenticated_peer(&ctx.signer, &route)?;
            match route.as_str() {
                CLUSTER_RAFT_APPEND_ROUTE => {
                    let payload: ClusterRaftAppendRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = handle_cluster_raft_append_request_async(
                        runtime,
                        &cluster_paths,
                        payload,
                        &authenticated_peer,
                    )
                    .await?;
                    ctx.respond_value(CLUSTER_RAFT_APPEND_ROUTE, response)?;
                }
                CLUSTER_RAFT_VOTE_ROUTE => {
                    let payload: ClusterRaftVoteRequest = ctx.decode_request_body(&request).await?;
                    let response = handle_cluster_raft_vote_request_async(
                        runtime,
                        &cluster_paths,
                        payload,
                        &authenticated_peer,
                    )
                    .await?;
                    ctx.respond_value(CLUSTER_RAFT_VOTE_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_cluster_consensus_service(
    builder: ServiceRegistryBuilder,
    runtime: Option<tokio::runtime::Handle>,
    host: Arc<dyn ClusterConsensusServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedClusterConsensusService::new(runtime, host))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHost;

    impl ClusterConsensusServiceHost for TestHost {
        fn raft_append_received(&self) {}
    }

    #[test]
    fn consensus_service_routes_follow_raft_policies() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let service =
            HostedClusterConsensusService::new(Some(runtime.handle().clone()), Arc::new(TestHost));
        assert_eq!(service.routes(), CLUSTER_RAFT_RPC_ROUTES);
        assert_eq!(
            service.route_categories(),
            CLUSTER_RAFT_RPC_ROUTE_POLICIES
                .iter()
                .map(|policy| ServiceRoute::from_route_metadata(
                    policy.route,
                    policy.category,
                    policy.auth.as_scope(),
                    policy.required_capabilities.iter().copied(),
                ))
                .collect::<Vec<_>>()
        );
    }
}
