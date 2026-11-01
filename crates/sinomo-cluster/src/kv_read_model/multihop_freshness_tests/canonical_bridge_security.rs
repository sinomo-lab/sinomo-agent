//! Native authenticated source-owner bridge rejection controls. Invalid rows
//! and membership cuts are negative DATA only; actual B/C positive observations
//! still come from the real endpoint RPC callback/store producer.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_wrong_source() {
    multihop_source_scenario(SourceCase::BridgeWrongSource).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_wrong_cluster() {
    multihop_source_scenario(SourceCase::BridgeWrongCluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_unknown_member() {
    multihop_source_scenario(SourceCase::BridgeUnknownMember).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_unbound_endpoint() {
    multihop_source_scenario(SourceCase::BridgeEndpointMismatch).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_future_clock() {
    multihop_source_scenario(SourceCase::BridgeFutureClock).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_expired_clock() {
    multihop_source_scenario(SourceCase::BridgeExpiredClock).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_existing_canonical_failure() {
    multihop_source_scenario(SourceCase::BridgeCanonicalFailure).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_existing_canonical_quarantine() {
    multihop_source_scenario(SourceCase::BridgeCanonicalQuarantine).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_healthy_identity_mismatch_reason() {
    multihop_source_scenario(SourceCase::BridgeIdentityReason).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_healthy_auth_denied_reason() {
    multihop_source_scenario(SourceCase::BridgeAuthReason).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_pending_target_membership() {
    multihop_source_scenario(SourceCase::BridgePendingTarget).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_bridge_rejects_removed_target_membership() {
    multihop_source_scenario(SourceCase::BridgeRemovedTarget).await;
}
