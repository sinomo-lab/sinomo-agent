//! Hosted cluster-membership object-RPC service.

use crate::{
    ClusterInviteChallengeProof, ClusterInviteChallengeRequest, ClusterInvitePublishRequest,
    ClusterInvitePublishResponse, ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse,
    ClusterMemberPublishRequest, ClusterMemberPublishResponse,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY, ObjectRpcRequest,
};
use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

const JOIN_CHALLENGE_BUDGET: Duration = Duration::from_secs(20);
const JOIN_REGISTRATION_BUDGET: Duration = Duration::from_secs(25);
const MEMBERSHIP_ROUTES: &[&str] = &[
    CLUSTER_JOIN_CHALLENGE_ROUTE,
    CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_INVITE_PUBLISH_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE,
];

pub type ClusterMembershipServiceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

/// Process effects required by the cluster-owned membership service.
pub trait ClusterMembershipServiceHost: Send + Sync + 'static {
    fn local_join_challenge<'a>(
        &'a self,
        request: ClusterInviteChallengeRequest,
    ) -> ClusterMembershipServiceFuture<'a, ClusterInviteChallengeProof>;

    fn forward_join_challenge<'a>(
        &'a self,
        request: ClusterInviteChallengeRequest,
    ) -> ClusterMembershipServiceFuture<'a, ClusterInviteChallengeProof>;

    fn register_join<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ClusterJoinRegistrationRequest,
    ) -> ClusterMembershipServiceFuture<'a, ClusterJoinRegistrationResponse>;

    fn publish_invite<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ClusterInvitePublishRequest,
        signer_node_id: String,
        signer_public_key: String,
    ) -> ClusterMembershipServiceFuture<'a, ClusterInvitePublishResponse>;

    fn publish_members<'a>(
        &'a self,
        data_dir: &'a SinomoDataDir,
        request: ClusterMemberPublishRequest,
        signer_node_id: String,
    ) -> ClusterMembershipServiceFuture<'a, ClusterMemberPublishResponse>;

    /// Called after the join handler has persisted the accepted projection.
    fn join_accepted(&self) {}

    /// Optional host-specific trace sink used by join diagnostics.
    fn trace_join(&self, _detail: &str) {}
}

pub struct HostedClusterMembershipService {
    host: Arc<dyn ClusterMembershipServiceHost>,
}

impl HostedClusterMembershipService {
    pub fn new(host: Arc<dyn ClusterMembershipServiceHost>) -> Self {
        Self { host }
    }
}

fn authenticated_cluster_identity(
    signer: &sinomo_service::ServiceSigner,
    route: &str,
) -> Result<(String, String), ServiceError> {
    let Some((node_id, public_key)) = signer.cluster_identity() else {
        return Err(ServiceError::Service(format!(
            "cluster route {route} requires an authenticated cluster signer identity"
        )));
    };
    Ok((node_id.to_string(), public_key.to_string()))
}

fn member_publish_outcome_name(accepted: bool, detail: &str) -> &'static str {
    if !accepted {
        "rejected"
    } else if detail == "member endpoints already current" {
        "already_current"
    } else if detail == "member endpoints published" {
        "published"
    } else {
        "accepted_other"
    }
}

fn observe_member_publish_response(
    signer_origin: &'static str,
    signer_node_id: &str,
    member_count: usize,
    response: &ClusterMemberPublishResponse,
    duration: Duration,
) {
    let outcome = member_publish_outcome_name(response.accepted, &response.detail);
    let duration_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
    metrics::counter!(
        "sinomo.daemon.cluster.member_publish.responses",
        "origin" => signer_origin,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.daemon.cluster.member_publish.us",
        "origin" => signer_origin,
        "outcome" => outcome
    )
    .record(duration_us as f64);
    tracing::trace!(
        target: "sinomo.cluster.member_publish",
        signer_origin,
        signer_node_id,
        member_count,
        outcome,
        accepted = response.accepted,
        snapshot_returned = response.kv_snapshot.is_some(),
        duration_us,
        detail = %response.detail,
        "cluster member publish completed"
    );
}

impl Service for HostedClusterMembershipService {
    fn name(&self) -> &'static str {
        "cluster-membership"
    }

    fn routes(&self) -> &'static [&'static str] {
        MEMBERSHIP_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| ServiceRoute::new(route, CLUSTER_MEMBERSHIP_RPC_CATEGORY))
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            if request.route == CLUSTER_JOIN_CHALLENGE_ROUTE {
                let challenge: ClusterInviteChallengeRequest =
                    ctx.decode_request_body(&request).await?;
                host.trace_join(&format!(
                    "challenge invite={} received",
                    challenge.invite_id
                ));
                let challenge_for_timeout = challenge.clone();
                let challenge_for_work = challenge.clone();
                let host_for_work = Arc::clone(&host);
                let proof = tokio::time::timeout(JOIN_CHALLENGE_BUDGET, async move {
                    let local_started = Instant::now();
                    match host_for_work
                        .local_join_challenge(challenge_for_work.clone())
                        .await
                    {
                        Ok(proof) => {
                            host_for_work.trace_join(&format!(
                                "challenge invite={} local proof ok in {:?}",
                                challenge_for_work.invite_id,
                                local_started.elapsed()
                            ));
                            Ok(proof)
                        }
                        Err(local_error) => {
                            host_for_work.trace_join(&format!(
                                "challenge invite={} local proof failed in {:?}: {local_error}",
                                challenge_for_work.invite_id,
                                local_started.elapsed()
                            ));
                            let forward_started = Instant::now();
                            match host_for_work
                                .forward_join_challenge(challenge_for_work.clone())
                                .await
                            {
                                Ok(proof) => {
                                    host_for_work.trace_join(&format!(
                                        "challenge invite={} forward proof ok in {:?}",
                                        challenge_for_work.invite_id,
                                        forward_started.elapsed()
                                    ));
                                    Ok(proof)
                                }
                                Err(forward_error) => {
                                    host_for_work.trace_join(&format!(
                                        "challenge invite={} forward proof failed in {:?}: {forward_error}",
                                        challenge_for_work.invite_id,
                                        forward_started.elapsed()
                                    ));
                                    tracing::warn!(
                                        "cluster join challenge for invite {} failed (local: {local_error}; forward-to-authority: {forward_error})",
                                        challenge_for_work.invite_id
                                    );
                                    Err(local_error)
                                }
                            }
                        }
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    host.trace_join(&format!(
                        "challenge invite={} timed out after {:?}",
                        challenge_for_timeout.invite_id, JOIN_CHALLENGE_BUDGET
                    ));
                    tracing::warn!(
                        "cluster join challenge for invite {} timed out after {}s",
                        challenge_for_timeout.invite_id,
                        JOIN_CHALLENGE_BUDGET.as_secs()
                    );
                    Err(ServiceError::Service(
                        "cluster join challenge timed out".to_string(),
                    ))
                })?;
                ctx.respond_value(CLUSTER_JOIN_CHALLENGE_ROUTE, proof)?;
                return Ok(ServiceDisposition::Continue);
            }

            if request.route == CLUSTER_JOIN_REGISTER_ROUTE {
                let payload: ClusterJoinRegistrationRequest =
                    ctx.decode_request_body(&request).await?;
                let started = Instant::now();
                let invite_id = payload.invite.invite_id.clone();
                host.trace_join(&format!(
                    "register invite={} received with {} member(s)",
                    invite_id,
                    payload.members.len()
                ));
                let response = tokio::time::timeout(
                    JOIN_REGISTRATION_BUDGET,
                    host.register_join(ctx.data_dir.as_ref(), payload),
                )
                .await
                .map_err(|_| {
                    host.trace_join(&format!(
                        "register invite={} timed out after {:?}",
                        invite_id, JOIN_REGISTRATION_BUDGET
                    ));
                    tracing::warn!(
                        "cluster join registration timed out after {}s",
                        JOIN_REGISTRATION_BUDGET.as_secs()
                    );
                    ServiceError::Service("cluster join registration timed out".to_string())
                })??;
                host.trace_join(&format!(
                    "register invite={} completed accepted={} in {:?}",
                    invite_id,
                    response.accepted,
                    started.elapsed()
                ));
                if response.accepted {
                    host.join_accepted();
                }
                ctx.respond_value(CLUSTER_JOIN_REGISTER_ROUTE, response)?;
                return Ok(ServiceDisposition::Continue);
            }

            match request.route.as_str() {
                CLUSTER_INVITE_PUBLISH_ROUTE => {
                    let payload: ClusterInvitePublishRequest =
                        ctx.decode_request_body(&request).await?;
                    let (signer_node_id, signer_public_key) =
                        authenticated_cluster_identity(&ctx.signer, CLUSTER_INVITE_PUBLISH_ROUTE)?;
                    let response = host
                        .publish_invite(
                            ctx.data_dir.as_ref(),
                            payload,
                            signer_node_id,
                            signer_public_key,
                        )
                        .await?;
                    ctx.respond_value(CLUSTER_INVITE_PUBLISH_ROUTE, response)?;
                }
                CLUSTER_MEMBER_PUBLISH_ROUTE => {
                    let payload: ClusterMemberPublishRequest =
                        ctx.decode_request_body(&request).await?;
                    let member_count = payload.members.len();
                    let signer_origin = if ctx.signer.is_local() {
                        "local"
                    } else {
                        "remote"
                    };
                    let (signer_node_id, _) =
                        authenticated_cluster_identity(&ctx.signer, CLUSTER_MEMBER_PUBLISH_ROUTE)?;
                    let started = Instant::now();
                    let response = host
                        .publish_members(ctx.data_dir.as_ref(), payload, signer_node_id.clone())
                        .await?;
                    observe_member_publish_response(
                        signer_origin,
                        &signer_node_id,
                        member_count,
                        &response,
                        started.elapsed(),
                    );
                    ctx.respond_value(CLUSTER_MEMBER_PUBLISH_ROUTE, response)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_cluster_membership_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn ClusterMembershipServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedClusterMembershipService::new(host))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_are_cluster_membership_routes() {
        assert_eq!(MEMBERSHIP_ROUTES.len(), 4);
        assert!(MEMBERSHIP_ROUTES.contains(&CLUSTER_JOIN_CHALLENGE_ROUTE));
        assert!(MEMBERSHIP_ROUTES.contains(&CLUSTER_JOIN_REGISTER_ROUTE));
        assert!(MEMBERSHIP_ROUTES.contains(&CLUSTER_INVITE_PUBLISH_ROUTE));
        assert!(MEMBERSHIP_ROUTES.contains(&CLUSTER_MEMBER_PUBLISH_ROUTE));
    }
}
