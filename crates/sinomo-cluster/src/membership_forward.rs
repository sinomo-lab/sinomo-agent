use crate::control_plane_options::DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state;
use sinomo_base::ClusterBootstrapState;
use sinomo_network::ClusterPaths;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use sinomo_network::{
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterSignedObjectRpcAsyncHost,
};
use sinomo_network::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use sinomo_network::{
    ObjectRpcRequest, ObjectRpcResponse, ObjectRpcSessionCall, ObjectRpcSessionResult,
};
use sinomo_node::NodeId;
use std::future::Future;
use std::time::{Duration, Instant as StdInstant};
use tokio::time::Instant as TokioInstant;

#[derive(Clone, Copy, Debug)]
struct MembershipForwardDeadline {
    expires_at: TokioInstant,
    timeout: Duration,
}

impl MembershipForwardDeadline {
    fn new(timeout: Duration) -> Self {
        Self {
            expires_at: TokioInstant::now() + timeout,
            timeout,
        }
    }

    fn expires_at(&self) -> TokioInstant {
        self.expires_at
    }

    fn remaining_at(&self, now: TokioInstant) -> Option<Duration> {
        self.expires_at
            .checked_duration_since(now)
            .filter(|remaining| !remaining.is_zero())
    }

    fn rpc_options(&self) -> Option<ClusterObjectRpcOptions> {
        self.rpc_options_at(TokioInstant::now())
    }

    fn rpc_options_at(&self, now: TokioInstant) -> Option<ClusterObjectRpcOptions> {
        self.remaining_at(now)
            .map(|remaining| ClusterObjectRpcOptions::default().with_overall_timeout(remaining))
    }

    fn timeout_error(
        &self,
        stage: impl std::fmt::Display,
        delivery_state: NetworkDeliveryState,
    ) -> NetworkError {
        NetworkError::timeout(
            format!("cluster membership forwarding {stage}"),
            NetworkTimeoutPhase::Overall,
            self.timeout,
            delivery_state,
        )
    }
}

#[derive(Debug)]
enum MembershipForwardAttemptOutcome<T> {
    Completed(Result<T, NetworkError>),
    // Keep semantic-deadline expiry out of the ordinary fallback path. The
    // outer deadline cannot observe whether the transport dispatched before
    // its future was dropped, so retrying here could replay a mutation.
    DeadlineExceeded(NetworkError),
}

async fn run_membership_forward_attempt_with_deadline<T>(
    deadline: MembershipForwardDeadline,
    stage: String,
    attempt: impl Future<Output = Result<T, NetworkError>>,
) -> MembershipForwardAttemptOutcome<T> {
    match tokio::time::timeout_at(deadline.expires_at(), attempt).await {
        Ok(result) => MembershipForwardAttemptOutcome::Completed(result),
        Err(_) => MembershipForwardAttemptOutcome::DeadlineExceeded(
            deadline.timeout_error(stage, NetworkDeliveryState::PossiblyApplied),
        ),
    }
}

fn membership_forward_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(
        target: "sinomo.cluster.formation",
        detail = %args,
        "cluster membership forward stage"
    );
    let enabled = std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"));
    if enabled {
        eprintln!("[sinomo cluster join] {args}");
    }
}

pub trait ClusterMembershipRpcForwardHost {
    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub fn forward_cluster_membership_rpc_to_state_authority_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut failures = Vec::new();
    for target_node_id in ClusterEndpointSelector::new(paths, state)
        .select_internal_unique_node_targets(ClusterEndpointInternalSelectionPurpose::VoterCommand)
        .into_iter()
        .map(|endpoint| endpoint.node_id)
    {
        match host.call_node_object_rpc(state, &target_node_id, request) {
            Ok(response) => return Ok(response),
            Err(error) => {
                failures.push(format!("node {target_node_id}: {error}"));
                if !membership_forward_error_allows_fallback(request, &error) {
                    return Err(error);
                }
            }
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "{failure_detail}{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub async fn forward_cluster_membership_rpc_to_state_authority_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut (impl ClusterSignedObjectRpcAsyncHost + Send),
) -> Result<ObjectRpcResponse, NetworkError> {
    forward_cluster_membership_rpc_to_state_authority_async_with_timeout(
        runtime,
        paths,
        state,
        request,
        failure_detail,
        host,
        DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
    )
    .await
}

async fn forward_cluster_membership_rpc_to_state_authority_async_with_timeout(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut (impl ClusterSignedObjectRpcAsyncHost + Send),
    timeout: Duration,
) -> Result<ObjectRpcResponse, NetworkError> {
    let deadline = MembershipForwardDeadline::new(timeout);
    let state_authority_targets = match tokio::time::timeout_at(
        deadline.expires_at(),
        ClusterEndpointSelector::select_internal_unique_node_targets_async(
            runtime,
            paths,
            state,
            ClusterEndpointInternalSelectionPurpose::VoterCommand,
        ),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            return Err(deadline.timeout_error(
                "while selecting state authorities",
                NetworkDeliveryState::DefinitelyNotSent,
            ));
        }
    };
    let mut failures = Vec::new();
    let mut operation_may_have_applied = false;
    let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
    for target in &state_authority_targets {
        let Some(options) = deadline.rpc_options() else {
            return Err(deadline.timeout_error(
                "before the next routed attempt",
                membership_forward_timeout_delivery_state(operation_may_have_applied),
            ));
        };
        let started = StdInstant::now();
        membership_forward_trace(format_args!(
            "membership forward routed node {} via {} ({:?}) started",
            target.node_id, target.address, target.transport
        ));
        match run_membership_forward_attempt_with_deadline(
            deadline,
            format!(
                "while routing to node {} via {} ({:?})",
                target.node_id, target.address, target.transport
            ),
            client.call_target(
                state,
                &target.node_id,
                ObjectRpcSessionCall::value(request.clone()),
                options,
            ),
        )
        .await
        {
            MembershipForwardAttemptOutcome::Completed(Ok(result)) => {
                let response = membership_forward_value_response(result)?;
                membership_forward_trace(format_args!(
                    "membership forward routed node {} via {} ({:?}) ok in {:?}",
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                return Ok(response);
            }
            MembershipForwardAttemptOutcome::Completed(Err(error)) => {
                membership_forward_trace(format_args!(
                    "membership forward routed node {} via {} ({:?}) failed in {:?}: {error}",
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                failures.push(format!(
                    "routed node {} via {} ({:?}): {error}",
                    target.node_id, target.address, target.transport
                ));
                membership_forward_record_delivery_uncertainty(
                    &mut operation_may_have_applied,
                    &error,
                );
                if !membership_forward_error_allows_fallback(request, &error) {
                    return Err(error);
                }
            }
            MembershipForwardAttemptOutcome::DeadlineExceeded(error) => {
                membership_forward_trace(format_args!(
                    "membership forward routed node {} via {} ({:?}) failed in {:?}: {error}",
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                return Err(error);
            }
        }
    }
    {
        let mut direct_client = ClusterObjectRpcClient::endpoint_only(host);
        for target in &state_authority_targets {
            let Some(options) = deadline.rpc_options() else {
                return Err(deadline.timeout_error(
                    "before the next direct attempt",
                    membership_forward_timeout_delivery_state(operation_may_have_applied),
                ));
            };
            let started = StdInstant::now();
            membership_forward_trace(format_args!(
                "membership forward direct node {} via {} ({:?}) started",
                target.node_id, target.address, target.transport
            ));
            match run_membership_forward_attempt_with_deadline(
                deadline,
                format!(
                    "while sending directly to node {} via {} ({:?})",
                    target.node_id, target.address, target.transport
                ),
                direct_client.call_endpoint(
                    state,
                    target,
                    ObjectRpcSessionCall::value(request.clone()),
                    options,
                ),
            )
            .await
            {
                MembershipForwardAttemptOutcome::Completed(Ok(result)) => {
                    let response = membership_forward_value_response(result)?;
                    membership_forward_trace(format_args!(
                        "membership forward direct node {} via {} ({:?}) ok in {:?}",
                        target.node_id,
                        target.address,
                        target.transport,
                        started.elapsed()
                    ));
                    return Ok(response);
                }
                MembershipForwardAttemptOutcome::Completed(Err(error)) => {
                    membership_forward_trace(format_args!(
                        "membership forward direct node {} via {} ({:?}) failed in {:?}: {error}",
                        target.node_id,
                        target.address,
                        target.transport,
                        started.elapsed()
                    ));
                    failures.push(format!(
                        "direct node {} via {} ({:?}): {error}",
                        target.node_id, target.address, target.transport
                    ));
                    membership_forward_record_delivery_uncertainty(
                        &mut operation_may_have_applied,
                        &error,
                    );
                    if !membership_forward_error_allows_fallback(request, &error) {
                        return Err(error);
                    }
                }
                MembershipForwardAttemptOutcome::DeadlineExceeded(error) => {
                    membership_forward_trace(format_args!(
                        "membership forward direct node {} via {} ({:?}) failed in {:?}: {error}",
                        target.node_id,
                        target.address,
                        target.transport,
                        started.elapsed()
                    ));
                    return Err(error);
                }
            }
        }
    }
    Err(membership_forward_failure(failure_detail, &failures))
}

fn membership_forward_value_response(
    result: ObjectRpcSessionResult,
) -> Result<ObjectRpcResponse, NetworkError> {
    let (response, body) = result.into_parts();
    if !body.is_empty() {
        return Err(NetworkError::Serialization {
            detail: format!(
                "cluster membership RPC route {} returned an unexpected attachment",
                response.route
            ),
        });
    }
    Ok(response)
}

fn membership_forward_failure(failure_detail: &str, failures: &[String]) -> NetworkError {
    NetworkError::ClusterReadOnly {
        detail: format!(
            "{failure_detail}{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    }
}

fn membership_forward_error_allows_fallback(
    request: &ObjectRpcRequest,
    error: &NetworkError,
) -> bool {
    match error.delivery_state() {
        Some(NetworkDeliveryState::DefinitelyNotSent) => true,
        Some(NetworkDeliveryState::PossiblyApplied) => request.is_response_retry_safe(),
        None => false,
    }
}

fn membership_forward_record_delivery_uncertainty(
    operation_may_have_applied: &mut bool,
    error: &NetworkError,
) {
    if error.delivery_state() != Some(NetworkDeliveryState::DefinitelyNotSent) {
        *operation_may_have_applied = true;
    }
}

fn membership_forward_timeout_delivery_state(
    operation_may_have_applied: bool,
) -> NetworkDeliveryState {
    if operation_may_have_applied {
        NetworkDeliveryState::PossiblyApplied
    } else {
        NetworkDeliveryState::DefinitelyNotSent
    }
}

/// A bridge introducer named in an invite minted before it joined is not itself a
/// proof-capable channel/issuer, so it cannot answer a join challenge. Relay the challenge to
/// a reachable cluster state authority (the invite issuer can produce an issuer-signed proof).
/// The routing-aware send routes the challenge across segments when no authority is directly
/// reachable.
#[cfg(any(test, feature = "test-support"))]
pub fn forward_cluster_join_challenge_rpc_to_state_authority_with_host(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    forward_cluster_membership_rpc_to_state_authority_with_host(
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any state authority",
        host,
    )
}

#[cfg(any(test, feature = "test-support"))]
pub async fn forward_cluster_join_challenge_rpc_to_state_authority_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut (impl ClusterSignedObjectRpcAsyncHost + Send),
) -> Result<ObjectRpcResponse, NetworkError> {
    let state_paths = paths.clone();
    let state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })??;
    forward_cluster_membership_rpc_to_state_authority_async_with_host(
        runtime,
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any state authority",
        host,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::ClusterMemberRecord;
    use sinomo_local::SinomoDataDir;
    use sinomo_node::ClusterId;
    use time::OffsetDateTime;

    struct FailingMembershipForwardHost {
        error: NetworkError,
        calls: Vec<NodeId>,
    }

    impl ClusterMembershipRpcForwardHost for FailingMembershipForwardHost {
        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            target_node_id: &NodeId,
            _request: &ObjectRpcRequest,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls.push(target_node_id.clone());
            Err(self.error.clone())
        }
    }

    fn membership_forward_test_state()
    -> (sinomo_testdir::TestDir, ClusterPaths, ClusterBootstrapState) {
        static INSTALL_DIRECTORY: std::sync::Once = std::sync::Once::new();
        INSTALL_DIRECTORY.call_once(crate::install_kv_cluster_directory);

        let root = sinomo_testdir::TestDir::new("sinomo-membership-forward-delivery-state");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_membership_forward_delivery_state")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_membership_forward_delivery_state_local")
            .expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "membership forward delivery state",
            local_node,
            "local",
            "pk-local",
            now,
        );
        for (suffix, address) in [("a", "voter-a.test:21353"), ("b", "voter-b.test:21353")] {
            let node_id = NodeId::from_legacy_named(format!(
                "node_membership_forward_delivery_state_{suffix}"
            ))
            .expect("voter node");
            let mut voter = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id.clone(),
                format!("voter {suffix}"),
                format!("pk-{suffix}"),
                now,
            );
            voter.capabilities = sinomo_node::main_daemon_capabilities(&voter.role);
            voter.allowed_capabilities = voter.capabilities.clone();
            voter.endpoints.push(sinomo_network::daemon_lan_endpoint(
                &node_id,
                address.to_string(),
                0,
            ));
            state.members.push(voter);
        }
        crate::local_state_store::write_cluster_state(&paths, &state).expect("write state");
        (root, paths, state)
    }

    #[test]
    fn membership_forward_attempts_share_one_absolute_deadline() {
        let started_at = TokioInstant::now();
        let timeout = Duration::from_secs(15);
        let deadline = MembershipForwardDeadline {
            expires_at: started_at + timeout,
            timeout,
        };

        assert_eq!(
            deadline.rpc_options_at(started_at),
            Some(ClusterObjectRpcOptions::default().with_overall_timeout(Duration::from_secs(15)))
        );
        assert_eq!(
            deadline.rpc_options_at(started_at + Duration::from_secs(6)),
            Some(ClusterObjectRpcOptions::default().with_overall_timeout(Duration::from_secs(9)))
        );
        assert_eq!(
            deadline.rpc_options_at(started_at + timeout),
            None,
            "an attempt at the operation deadline must not receive a fresh timeout"
        );
    }

    #[test]
    fn membership_forward_retries_only_typed_safe_failures() {
        let request = ObjectRpcRequest::from_encoded_body(
            "/cluster/membership/test",
            "cluster.membership",
            Vec::new(),
        );
        let ambiguous = NetworkError::outcome_unknown(
            "membership test",
            request.operation_id(),
            "response was not observed",
        );

        assert!(!membership_forward_error_allows_fallback(
            &request, &ambiguous
        ));
        assert!(membership_forward_error_allows_fallback(
            &request.clone().with_response_retry_safe(),
            &ambiguous
        ));
        assert!(membership_forward_error_allows_fallback(
            &request,
            &NetworkError::delivery_failed(
                "membership test",
                NetworkDeliveryState::DefinitelyNotSent,
                "connect failed",
            )
        ));
        let untyped = NetworkError::Io {
            operation: "legacy membership transport".to_string(),
            detail: "delivery state unavailable".to_string(),
        };
        assert!(!membership_forward_error_allows_fallback(
            &request, &untyped
        ));
        assert!(!membership_forward_error_allows_fallback(
            &request.clone().with_response_retry_safe(),
            &untyped,
        ));
        assert!(!membership_forward_error_allows_fallback(
            &request,
            &NetworkError::ObjectRpc {
                route: request.route.clone(),
                detail: "application rejected the request".to_string(),
            },
        ));
    }

    #[test]
    fn synchronous_membership_forward_stops_after_ambiguous_delivery() {
        let (_root, paths, state) = membership_forward_test_state();
        let request = ObjectRpcRequest::from_encoded_body(
            "/cluster/membership/test",
            "cluster.membership",
            Vec::new(),
        );
        let ambiguous = NetworkError::outcome_unknown(
            "membership test",
            request.operation_id(),
            "response was not observed",
        );
        let mut host = FailingMembershipForwardHost {
            error: ambiguous.clone(),
            calls: Vec::new(),
        };

        let error = forward_cluster_membership_rpc_to_state_authority_with_host(
            &paths,
            &state,
            &request,
            "membership forwarding failed",
            &mut host,
        )
        .expect_err("ambiguous mutation must be terminal");

        assert_eq!(error, ambiguous);
        assert_eq!(host.calls.len(), 1, "the mutation must not be replayed");
    }

    #[test]
    fn synchronous_membership_forward_retries_proven_not_sent_failure() {
        let (_root, paths, state) = membership_forward_test_state();
        let request = ObjectRpcRequest::from_encoded_body(
            "/cluster/membership/test",
            "cluster.membership",
            Vec::new(),
        );
        let mut host = FailingMembershipForwardHost {
            error: NetworkError::delivery_failed(
                "membership test",
                NetworkDeliveryState::DefinitelyNotSent,
                "connect failed",
            ),
            calls: Vec::new(),
        };

        let error = forward_cluster_membership_rpc_to_state_authority_with_host(
            &paths,
            &state,
            &request,
            "membership forwarding failed",
            &mut host,
        )
        .expect_err("all authorities fail");

        assert!(matches!(error, NetworkError::ClusterReadOnly { .. }));
        assert_eq!(host.calls.len(), 2, "both voters should be attempted");
    }

    #[tokio::test]
    async fn membership_forward_in_flight_deadline_is_typed_and_not_retried() {
        let deadline = MembershipForwardDeadline::new(Duration::ZERO);
        let outcome = run_membership_forward_attempt_with_deadline::<()>(
            deadline,
            "during a test attempt".to_string(),
            std::future::pending(),
        )
        .await;

        let MembershipForwardAttemptOutcome::DeadlineExceeded(error) = outcome else {
            panic!("an expired in-flight attempt must take the terminal deadline path");
        };
        assert!(matches!(
            error,
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Overall,
                timeout: Duration::ZERO,
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }
        ));
    }

    #[test]
    fn membership_forward_timeout_is_not_sent_only_when_every_failure_proves_it() {
        let mut operation_may_have_applied = false;
        membership_forward_record_delivery_uncertainty(
            &mut operation_may_have_applied,
            &NetworkError::delivery_failed(
                "membership test",
                NetworkDeliveryState::DefinitelyNotSent,
                "connect failed",
            ),
        );
        assert_eq!(
            membership_forward_timeout_delivery_state(operation_may_have_applied),
            NetworkDeliveryState::DefinitelyNotSent
        );

        membership_forward_record_delivery_uncertainty(
            &mut operation_may_have_applied,
            &NetworkError::outcome_unknown("membership test", None, "response was not observed"),
        );
        assert_eq!(
            membership_forward_timeout_delivery_state(operation_may_have_applied),
            NetworkDeliveryState::PossiblyApplied
        );
    }
}
