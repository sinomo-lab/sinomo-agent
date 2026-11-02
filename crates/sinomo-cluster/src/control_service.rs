//! Hosted cluster-control object-RPC service.

use crate::{
    CLUSTER_CONTROL_ADMIN_CAPABILITY, CLUSTER_CONTROL_INIT_ROUTE, CLUSTER_CONTROL_INVITE_ROUTE,
    CLUSTER_CONTROL_JOIN_ROUTE, CLUSTER_CONTROL_LEAVE_ROUTE, CLUSTER_CONTROL_REMOVE_ROUTE,
    CLUSTER_CONTROL_RENAME_ROUTE, CLUSTER_CONTROL_ROLE_ROUTE, CLUSTER_CONTROL_RPC_ROUTE_POLICIES,
    CLUSTER_CONTROL_RPC_ROUTES, ClusterControlInitRequest, ClusterControlInitResponse,
    ClusterControlInviteRequest, ClusterControlInviteResponse, ClusterControlJoinRequest,
    ClusterControlJoinResponse, ClusterControlLeaveRequest, ClusterControlLeaveResponse,
    ClusterControlRemoveRequest, ClusterControlRemoveResponse, ClusterControlRenameRequest,
    ClusterControlRenameResponse, ClusterControlRoleRequest, ClusterControlRoleResponse,
    ClusterMemberRecord, RemoteClusterControlAuthorityFence, cluster_control_rpc_route_policy,
};
use sinomo_kv::{
    ClusterKvRaftConfigRecord, ClusterKvRouteAuth, KvAuthoritativeRead, KvAuthorityBasis,
    KvSnapshot, cluster_kv_raft_config_key,
};
use sinomo_network::{NetworkError, ObjectRpcRequest};
use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type ClusterControlServiceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

/// Process operations required by the cluster-owned control service.
pub trait ClusterControlServiceHost: Send + Sync + 'static {
    fn route_hint(
        &self,
        _request: crate::ClusterRouteHintRequest,
    ) -> ClusterControlServiceFuture<'_, crate::ClusterRouteHintResponse> {
        Box::pin(async {
            Err(ServiceError::UnknownRoute(
                crate::CLUSTER_CONTROL_ROUTE_HINT_ROUTE.into(),
            ))
        })
    }
    fn authoritative_snapshot(
        &self,
    ) -> ClusterControlServiceFuture<'_, KvAuthoritativeRead<KvSnapshot>>;

    fn init(
        &self,
        request: ClusterControlInitRequest,
    ) -> ClusterControlServiceFuture<'_, ClusterControlInitResponse>;

    fn join(
        &self,
        request: ClusterControlJoinRequest,
    ) -> ClusterControlServiceFuture<'_, ClusterControlJoinResponse>;

    fn invite(
        &self,
        request: ClusterControlInviteRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> ClusterControlServiceFuture<'_, ClusterControlInviteResponse>;

    fn rename(
        &self,
        request: ClusterControlRenameRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> ClusterControlServiceFuture<'_, ClusterControlRenameResponse>;

    fn role(
        &self,
        request: ClusterControlRoleRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> ClusterControlServiceFuture<'_, ClusterControlRoleResponse>;

    fn state_authority(
        &self,
        request: crate::ClusterControlStateAuthorityRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> ClusterControlServiceFuture<'_, crate::ClusterControlStateAuthorityResponse>;

    fn remove(
        &self,
        request: ClusterControlRemoveRequest,
        authority: Option<RemoteClusterControlAuthorityFence>,
    ) -> ClusterControlServiceFuture<'_, ClusterControlRemoveResponse>;

    fn leave(
        &self,
        request: ClusterControlLeaveRequest,
    ) -> ClusterControlServiceFuture<'_, ClusterControlLeaveResponse>;
}

pub struct HostedClusterControlService {
    host: Arc<dyn ClusterControlServiceHost>,
}

impl HostedClusterControlService {
    pub fn new(host: Arc<dyn ClusterControlServiceHost>) -> Self {
        Self { host }
    }
}

fn deny_remote_authority(reason: &str) -> ServiceError {
    tracing::warn!(
        target: "sinomo.cluster.control_auth",
        reason,
        "remote cluster-control mutation denied by authoritative membership policy"
    );
    ServiceError::Network(NetworkError::AuthDenied)
}

pub fn validate_remote_cluster_control_authority(
    snapshot: &KvSnapshot,
    signer: &ServiceSigner,
) -> Result<RemoteClusterControlAuthorityFence, ServiceError> {
    let ServiceSigner::Remote {
        node_id,
        public_key,
    } = signer
    else {
        return Err(deny_remote_authority(
            "fresh cluster-control authorization requires a remote signer",
        ));
    };
    let node_id = node_id
        .parse::<sinomo_node::NodeId>()
        .map_err(|_| deny_remote_authority("signer node id is invalid"))?;
    let member_key = sinomo_node::cluster_member_key(&node_id);
    let member_entry = snapshot
        .get(&member_key)
        .ok_or_else(|| deny_remote_authority("signer member is absent"))?;
    let member: ClusterMemberRecord = member_entry.decode()?;
    if member.node_id != node_id || member.node_public_key != *public_key || !member.has_raft_vote()
    {
        return Err(deny_remote_authority(
            "signer voter identity is stale, demoted, or revoked",
        ));
    }

    let config_entry = snapshot
        .get(&cluster_kv_raft_config_key())
        .ok_or_else(|| deny_remote_authority("committed Raft config is absent"))?;
    let config: ClusterKvRaftConfigRecord = config_entry.decode()?;
    if config.cluster_id != member.cluster_id || !config.voters.contains(&node_id) {
        return Err(deny_remote_authority(
            "signer is not a voter in the committed Raft config",
        ));
    }

    let policy_entry = snapshot
        .get(&sinomo_node::cluster_capability_policy_key(&node_id))
        .ok_or_else(|| deny_remote_authority("signer capability policy is absent"))?;
    let policy: sinomo_node::ClusterCapabilityPolicyRecord = policy_entry.decode()?;
    let advertises_required = policy
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == CLUSTER_CONTROL_ADMIN_CAPABILITY);
    let allows_required = policy
        .allowed_capabilities
        .iter()
        .any(|capability| capability.as_str() == CLUSTER_CONTROL_ADMIN_CAPABILITY);
    if policy.cluster_id != member.cluster_id
        || policy.node_id != node_id
        || !advertises_required
        || !allows_required
    {
        return Err(deny_remote_authority(
            "signer capability policy does not grant cluster.control.admin",
        ));
    }
    Ok(RemoteClusterControlAuthorityFence::new(
        node_id,
        public_key.clone(),
        member_entry.clone(),
        config_entry.clone(),
        policy_entry.clone(),
    ))
}

async fn authorize_request(
    host: &dyn ClusterControlServiceHost,
    route: &str,
    signer: &ServiceSigner,
) -> Result<Option<RemoteClusterControlAuthorityFence>, ServiceError> {
    let policy = cluster_control_rpc_route_policy(route)
        .ok_or_else(|| ServiceError::UnknownRoute(route.to_string()))?;
    if policy.auth == ClusterKvRouteAuth::LocalOnly {
        return signer
            .is_local()
            .then_some(None)
            .ok_or_else(|| deny_remote_authority("route is local-only"));
    }
    if signer.is_local() {
        return Ok(None);
    }
    if !matches!(signer, ServiceSigner::Remote { .. }) {
        return Err(deny_remote_authority(
            "non-local signer has no authenticated cluster identity",
        ));
    }

    let authoritative = host.authoritative_snapshot().await?;
    if authoritative.authority.revision != authoritative.value.revision
        || !matches!(
            authoritative.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum {
                voter_acks,
                required_quorum,
            } if required_quorum > 0 && voter_acks >= required_quorum
        )
    {
        return Err(deny_remote_authority(
            "fresh current-term quorum authority could not be established",
        ));
    }
    validate_remote_cluster_control_authority(&authoritative.value, signer).map(Some)
}

fn disposition(route: &str) -> ServiceDisposition {
    if matches!(
        route,
        CLUSTER_CONTROL_INIT_ROUTE | CLUSTER_CONTROL_JOIN_ROUTE | CLUSTER_CONTROL_LEAVE_ROUTE
    ) {
        ServiceDisposition::Stop
    } else {
        ServiceDisposition::Continue
    }
}

impl Service for HostedClusterControlService {
    fn name(&self) -> &'static str {
        "cluster-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        CLUSTER_CONTROL_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        CLUSTER_CONTROL_RPC_ROUTE_POLICIES
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
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let route = request.route.clone();
            let authority = authorize_request(host.as_ref(), &route, &ctx.signer).await?;
            match route.as_str() {
                crate::CLUSTER_CONTROL_ROUTE_HINT_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(&route, host.route_hint(payload).await?)?;
                }
                CLUSTER_CONTROL_INIT_ROUTE => {
                    let payload: ClusterControlInitRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(CLUSTER_CONTROL_INIT_ROUTE, host.init(payload).await?)?;
                }
                CLUSTER_CONTROL_JOIN_ROUTE => {
                    let payload: ClusterControlJoinRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(CLUSTER_CONTROL_JOIN_ROUTE, host.join(payload).await?)?;
                }
                CLUSTER_CONTROL_INVITE_ROUTE => {
                    let payload: ClusterControlInviteRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        CLUSTER_CONTROL_INVITE_ROUTE,
                        host.invite(payload, authority).await?,
                    )?;
                }
                CLUSTER_CONTROL_RENAME_ROUTE => {
                    let payload: ClusterControlRenameRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        CLUSTER_CONTROL_RENAME_ROUTE,
                        host.rename(payload, authority).await?,
                    )?;
                }
                CLUSTER_CONTROL_ROLE_ROUTE => {
                    let payload: ClusterControlRoleRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        CLUSTER_CONTROL_ROLE_ROUTE,
                        host.role(payload, authority).await?,
                    )?;
                }
                crate::CLUSTER_CONTROL_STATE_AUTHORITY_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        crate::CLUSTER_CONTROL_STATE_AUTHORITY_ROUTE,
                        host.state_authority(payload, authority).await?,
                    )?;
                }
                CLUSTER_CONTROL_REMOVE_ROUTE => {
                    let payload: ClusterControlRemoveRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(
                        CLUSTER_CONTROL_REMOVE_ROUTE,
                        host.remove(payload, authority).await?,
                    )?;
                }
                CLUSTER_CONTROL_LEAVE_ROUTE => {
                    let payload: ClusterControlLeaveRequest =
                        ctx.decode_request_body(&request).await?;
                    ctx.respond_value(CLUSTER_CONTROL_LEAVE_ROUTE, host.leave(payload).await?)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(disposition(&route))
        })
    }
}

pub fn register_cluster_control_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn ClusterControlServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedClusterControlService::new(host))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unused<'a, T>() -> ClusterControlServiceFuture<'a, T> {
        Box::pin(async { Err(ServiceError::Service("unused host method".to_string())) })
    }

    struct TestHost;

    impl ClusterControlServiceHost for TestHost {
        fn state_authority(
            &self,
            _request: crate::ClusterControlStateAuthorityRequest,
            _authority: Option<RemoteClusterControlAuthorityFence>,
        ) -> ClusterControlServiceFuture<'_, crate::ClusterControlStateAuthorityResponse> {
            unused()
        }
        fn authoritative_snapshot(
            &self,
        ) -> ClusterControlServiceFuture<'_, KvAuthoritativeRead<KvSnapshot>> {
            unused()
        }

        fn init(
            &self,
            _request: ClusterControlInitRequest,
        ) -> ClusterControlServiceFuture<'_, ClusterControlInitResponse> {
            unused()
        }

        fn join(
            &self,
            _request: ClusterControlJoinRequest,
        ) -> ClusterControlServiceFuture<'_, ClusterControlJoinResponse> {
            unused()
        }

        fn invite(
            &self,
            _request: ClusterControlInviteRequest,
            _authority: Option<RemoteClusterControlAuthorityFence>,
        ) -> ClusterControlServiceFuture<'_, ClusterControlInviteResponse> {
            unused()
        }

        fn rename(
            &self,
            _request: ClusterControlRenameRequest,
            _authority: Option<RemoteClusterControlAuthorityFence>,
        ) -> ClusterControlServiceFuture<'_, ClusterControlRenameResponse> {
            unused()
        }

        fn role(
            &self,
            _request: ClusterControlRoleRequest,
            _authority: Option<RemoteClusterControlAuthorityFence>,
        ) -> ClusterControlServiceFuture<'_, ClusterControlRoleResponse> {
            unused()
        }

        fn remove(
            &self,
            _request: ClusterControlRemoveRequest,
            _authority: Option<RemoteClusterControlAuthorityFence>,
        ) -> ClusterControlServiceFuture<'_, ClusterControlRemoveResponse> {
            unused()
        }

        fn leave(
            &self,
            _request: ClusterControlLeaveRequest,
        ) -> ClusterControlServiceFuture<'_, ClusterControlLeaveResponse> {
            unused()
        }
    }

    #[test]
    fn identity_changing_routes_stop_the_service_loop() {
        assert_eq!(
            disposition(CLUSTER_CONTROL_INIT_ROUTE),
            ServiceDisposition::Stop
        );
        assert_eq!(
            disposition(CLUSTER_CONTROL_JOIN_ROUTE),
            ServiceDisposition::Stop
        );
        assert_eq!(
            disposition(CLUSTER_CONTROL_LEAVE_ROUTE),
            ServiceDisposition::Stop
        );
        assert_eq!(
            disposition(CLUSTER_CONTROL_INVITE_ROUTE),
            ServiceDisposition::Continue
        );
    }

    #[test]
    fn local_only_control_routes_reject_remote_before_host_dispatch() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let host = TestHost;
        let remote = ServiceSigner::Remote {
            node_id: "remote-node".to_string(),
            public_key: "remote-key".to_string(),
        };
        for route in [
            crate::CLUSTER_CONTROL_ROUTE_HINT_ROUTE,
            CLUSTER_CONTROL_INIT_ROUTE,
            CLUSTER_CONTROL_JOIN_ROUTE,
            CLUSTER_CONTROL_LEAVE_ROUTE,
        ] {
            let error = runtime
                .block_on(authorize_request(&host, route, &remote))
                .expect_err("remote identity change must be denied");
            assert!(matches!(
                error,
                ServiceError::Network(NetworkError::AuthDenied)
            ));
            assert!(
                runtime
                    .block_on(authorize_request(&host, route, &ServiceSigner::Local))
                    .expect("local identity change remains authorized")
                    .is_none()
            );
        }
    }
}
