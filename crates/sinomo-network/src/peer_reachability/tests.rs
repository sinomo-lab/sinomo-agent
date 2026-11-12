use super::*;
use crate::connectivity::{ClusterConnectivityRecord, ClusterConnectivityState};
use crate::connectivity_verification::{
    begin_peer_verification, register_peer_verification_owner, settle_peer_verification,
    take_pending_peer_verifications,
};
use sinomo_base::{ClusterEndpointRecord, ClusterMemberRecord};
use sinomo_local::SinomoDataDir;
use std::time::Duration;

struct Fixture {
    _root: sinomo_testdir::TestDir,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    local: NodeId,
    b: NodeId,
    c: NodeId,
    b_endpoint: ClusterEndpointRecord,
    c_endpoint: ClusterEndpointRecord,
}

fn fixture(name: &str) -> Fixture {
    let root = sinomo_testdir::TestDir::new(name);
    let data_dir = SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().expect("layout");
    let paths = ClusterPaths::new(&data_dir);
    paths.ensure().expect("cluster paths");
    let now = OffsetDateTime::now_utc();
    let cluster_id =
        ClusterId::from_legacy_named(format!("cluster_{name}")).expect("fixture cluster");
    let local = NodeId::from_legacy_named(format!("node_{name}_local")).expect("local node");
    let b = NodeId::from_legacy_named(format!("node_{name}_b")).expect("node b");
    let c = NodeId::from_legacy_named(format!("node_{name}_c")).expect("node c");
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id.clone(),
        "peer reachability fixture",
        local.clone(),
        "local",
        "pk-local",
        now,
    );
    let b_endpoint =
        crate::endpoint_advertisement::daemon_lan_endpoint(&b, "192.0.2.10:21353".to_string(), 10);
    let c_endpoint =
        crate::endpoint_advertisement::daemon_lan_endpoint(&c, "192.0.2.11:21353".to_string(), 10);
    for (node_id, endpoint) in [(&b, &b_endpoint), (&c, &c_endpoint)] {
        let mut member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            node_id.clone(),
            node_id.to_string(),
            "pk",
            now,
        );
        member.endpoints = vec![endpoint.clone()];
        state.members.push(member);
    }
    Fixture {
        _root: root,
        paths,
        state,
        local,
        b,
        c,
        b_endpoint,
        c_endpoint,
    }
}

fn edge(
    fixture: &Fixture,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    consecutive_failures: u16,
    latency_ms: Option<u64>,
) -> ClusterConnectivityRecord {
    crate::connectivity::cluster_connectivity_record(
        &fixture.state,
        endpoint,
        connectivity_state,
        latency_ms,
        consecutive_failures,
        None,
        None,
        OffsetDateTime::now_utc(),
        crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
    )
}

fn write(fixture: &Fixture, records: &[ClusterConnectivityRecord]) {
    crate::write_local_cluster_connectivity_records(&fixture.paths, &fixture.local, records)
        .expect("write local connectivity");
}

fn healthy_pair(fixture: &Fixture) {
    write(
        fixture,
        &[
            edge(
                fixture,
                &fixture.b_endpoint,
                ClusterConnectivityState::Healthy,
                0,
                Some(5),
            ),
            edge(
                fixture,
                &fixture.c_endpoint,
                ClusterConnectivityState::Healthy,
                0,
                Some(5),
            ),
        ],
    );
}

async fn no_change_within(subscription: &mut PeerReachabilitySubscription, wait: Duration) {
    if let Ok(changes) = tokio::time::timeout(wait, subscription.changed()).await {
        panic!("unexpected peer reachability change: {changes:?}");
    }
}

#[tokio::test]
async fn verdict_change_wakes_subscriber_naming_only_that_peer() {
    let fixture = fixture("peer_reachability_names_peer");
    healthy_pair(&fixture);
    let mut subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);
    let current = subscription.current();
    assert_eq!(current.observer_node_id, fixture.local);
    assert_eq!(current.verdict(&fixture.b), PeerPathVerdict::Path);
    assert_eq!(current.verdict(&fixture.c), PeerPathVerdict::Path);

    write(
        &fixture,
        &[edge(
            &fixture,
            &fixture.b_endpoint,
            ClusterConnectivityState::Unreachable,
            crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
        )],
    );
    let changes = tokio::time::timeout(Duration::from_secs(5), subscription.changed())
        .await
        .expect("verdict change wakes the subscriber");
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(changes[0].peer_node_id, fixture.b);
    assert_eq!(
        changes[0].previous.as_ref().map(|peer| peer.verdict),
        Some(PeerPathVerdict::Path)
    );
    assert_eq!(
        changes[0].current.as_ref().map(|peer| peer.verdict),
        Some(PeerPathVerdict::NoPath)
    );
    assert_eq!(
        subscription.current().verdict(&fixture.c),
        PeerPathVerdict::Path
    );
}

#[tokio::test]
async fn time_only_refresh_and_unrelated_wakes_report_nothing() {
    let fixture = fixture("peer_reachability_time_only");
    healthy_pair(&fixture);
    let mut subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);
    let computed = peer_reachability_computations_for_test(&fixture.paths);
    let inputs = current_inputs(&fixture.paths, &fixture.state).expect("inputs");

    // A newer timestamp and latency sample move no route input at all.
    crate::record_local_cluster_endpoint_command_connectivity(
        &fixture.paths,
        &fixture.state,
        &fixture.b_endpoint,
        ClusterConnectivityState::Healthy,
        Some(77),
        true,
    )
    .expect("timestamp-only success");
    // An unrelated committed-directory invalidation wakes, but the exact
    // input generations are unchanged, so nothing is recomputed.
    crate::invalidate_cluster_route_graph_cache(&fixture.paths);
    no_change_within(&mut subscription, Duration::from_millis(200)).await;
    let after = current_inputs(&fixture.paths, &fixture.state).expect("inputs");
    assert_eq!(
        (
            after.local_routing_generation,
            after.directory_routing_version
        ),
        (
            inputs.local_routing_generation,
            inputs.directory_routing_version
        ),
        "time-only evidence and an unrelated invalidation move no input of this root"
    );
    // The module registry is process-wide; another test may legitimately
    // move it. Only an unchanged input set proves the memo was reused.
    if after == inputs {
        assert_eq!(
            peer_reachability_computations_for_test(&fixture.paths),
            computed,
            "unchanged inputs must not recompute verdicts"
        );
    }

    // One failure inside the grace window is a real route-input change that
    // moves no verdict: recomputed in memory, nothing reported.
    crate::record_local_cluster_endpoint_command_connectivity(
        &fixture.paths,
        &fixture.state,
        &fixture.b_endpoint,
        ClusterConnectivityState::Degraded,
        None,
        true,
    )
    .expect("grace failure");
    no_change_within(&mut subscription, Duration::from_millis(200)).await;
    assert!(peer_reachability_computations_for_test(&fixture.paths) > computed);
    assert_eq!(
        subscription.current().verdict(&fixture.b),
        PeerPathVerdict::Path
    );
}

#[tokio::test]
async fn no_subscriber_means_no_computation() {
    let fixture = fixture("peer_reachability_no_subscriber");
    healthy_pair(&fixture);
    write(
        &fixture,
        &[edge(
            &fixture,
            &fixture.b_endpoint,
            ClusterConnectivityState::Unreachable,
            crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
        )],
    );
    crate::invalidate_cluster_route_graph_cache(&fixture.paths);
    assert_eq!(peer_reachability_computations_for_test(&fixture.paths), 0);

    let snapshot = peer_reachability_snapshot(&fixture.paths, &fixture.state).expect("snapshot");
    assert_eq!(snapshot.verdict(&fixture.b), PeerPathVerdict::NoPath);
    assert_eq!(snapshot.verdict(&fixture.c), PeerPathVerdict::Path);
    assert_eq!(peer_reachability_computations_for_test(&fixture.paths), 1);
    assert!(
        !PEER_REACHABILITY_VIEWS
            .lock()
            .expect("views")
            .keys()
            .any(|key| key.root == fixture.paths.root),
        "a one-shot snapshot does not retain a view"
    );
}

#[tokio::test]
async fn lagged_subscriber_resyncs_by_diff_and_a_flap_collapses() {
    let fixture = fixture("peer_reachability_lagged");
    healthy_pair(&fixture);
    let mut subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);
    // Each observation must be newer than the last one the store accepted.
    let unreachable = |fixture: &Fixture| {
        edge(
            fixture,
            &fixture.b_endpoint,
            ClusterConnectivityState::Unreachable,
            crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
        )
    };
    // Path -> NoPath -> Path between two waits is not reported.
    write(&fixture, &[unreachable(&fixture)]);
    healthy_pair(&fixture);
    no_change_within(&mut subscription, Duration::from_millis(200)).await;

    // Overrun the hint channel, then change B: the lag is one more wake.
    for _ in 0..300 {
        crate::invalidate_cluster_route_graph_cache(&fixture.paths);
    }
    write(&fixture, &[unreachable(&fixture)]);
    let changes = tokio::time::timeout(Duration::from_secs(5), subscription.changed())
        .await
        .expect("lagged subscriber still observes the change");
    assert_eq!(
        changes
            .iter()
            .map(|change| change.peer_node_id.clone())
            .collect::<Vec<_>>(),
        vec![fixture.b.clone()]
    );
}

#[tokio::test]
async fn running_verification_is_part_of_the_reported_entry() {
    let fixture = fixture("peer_reachability_verifying");
    healthy_pair(&fixture);
    let _owner = register_peer_verification_owner(&fixture.paths);
    let mut subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);

    begin_peer_verification(&fixture.paths, &fixture.b);
    let changes = tokio::time::timeout(Duration::from_secs(5), subscription.changed())
        .await
        .expect("verification start is reported");
    assert_eq!(changes.len(), 1);
    let current = changes[0].current.as_ref().expect("peer entry");
    assert_eq!(current.peer_node_id, fixture.b);
    assert!(current.verifying);
    assert_eq!(current.verdict, PeerPathVerdict::Path);

    settle_peer_verification(&fixture.paths, &fixture.b);
    let changes = tokio::time::timeout(Duration::from_secs(5), subscription.changed())
        .await
        .expect("a settled verification is reported even with an unchanged verdict");
    assert!(!changes[0].current.as_ref().expect("peer entry").verifying);
}

#[tokio::test]
async fn successful_delivery_against_no_path_requests_verification() {
    let fixture = fixture("peer_reachability_contradiction");
    write(
        &fixture,
        &[
            edge(
                &fixture,
                &fixture.b_endpoint,
                ClusterConnectivityState::Unreachable,
                crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
                None,
            ),
            edge(
                &fixture,
                &fixture.c_endpoint,
                ClusterConnectivityState::Healthy,
                0,
                Some(5),
            ),
        ],
    );
    let _owner = register_peer_verification_owner(&fixture.paths);
    let subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);
    assert_eq!(
        subscription.current().verdict(&fixture.b),
        PeerPathVerdict::NoPath
    );

    observe_peer_delivery_success(&fixture.paths, &fixture.local, &fixture.c);
    assert!(
        take_pending_peer_verifications(&fixture.paths).is_empty(),
        "a delivery consistent with the verdict requests nothing"
    );
    observe_peer_delivery_success(&fixture.paths, &fixture.local, &fixture.b);
    assert_eq!(
        take_pending_peer_verifications(&fixture.paths),
        vec![(fixture.b.clone(), PeerVerificationCause::DeliverySucceeded)]
    );

    drop(subscription);
    observe_peer_delivery_success(&fixture.paths, &fixture.local, &fixture.b);
    assert!(
        take_pending_peer_verifications(&fixture.paths).is_empty(),
        "without a subscribed view there is no verdict to contradict"
    );
}

#[tokio::test]
async fn failed_delivery_requests_verification_only_for_network_failures() {
    let fixture = fixture("peer_reachability_failure_request");
    let _owner = register_peer_verification_owner(&fixture.paths);
    observe_peer_delivery_failure(
        &fixture.paths,
        &fixture.local,
        &fixture.b,
        &NetworkError::timeout(
            "slow reply",
            crate::NetworkTimeoutPhase::Overall,
            Duration::from_secs(2),
            crate::NetworkDeliveryState::PossiblyApplied,
        ),
    );
    assert!(take_pending_peer_verifications(&fixture.paths).is_empty());
    observe_peer_delivery_failure(
        &fixture.paths,
        &fixture.local,
        &fixture.b,
        &NetworkError::delivery_failed(
            "rpc",
            crate::NetworkDeliveryState::PossiblyApplied,
            "connection reset by peer",
        ),
    );
    assert_eq!(
        take_pending_peer_verifications(&fixture.paths),
        vec![(fixture.b.clone(), PeerVerificationCause::DeliveryFailed)]
    );
}

#[tokio::test]
async fn expired_evidence_reads_like_a_fresh_classification() {
    let fixture = fixture("peer_reachability_expiry");
    // B's only evidence lasts one second; C's lasts the ordinary TTL.
    let short_lived = crate::connectivity::cluster_connectivity_record(
        &fixture.state,
        &fixture.b_endpoint,
        ClusterConnectivityState::Healthy,
        Some(5),
        0,
        None,
        None,
        OffsetDateTime::now_utc(),
        1,
    );
    write(
        &fixture,
        &[
            short_lived,
            edge(
                &fixture,
                &fixture.c_endpoint,
                ClusterConnectivityState::Healthy,
                0,
                Some(5),
            ),
        ],
    );
    let mut subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);
    assert_eq!(
        subscription.current().verdict(&fixture.b),
        PeerPathVerdict::Path
    );

    // Expiry is evidence ageing out, not an event: nothing wakes for it.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let fresh = compute_peers(&fixture.paths, &fixture.state)
        .expect("fresh classification")
        .peers;
    assert_eq!(
        fresh.get(&fixture.b).map(|peer| peer.verdict),
        Some(PeerPathVerdict::Unknown)
    );

    // The next hint finds unchanged input generations, yet the memo is past
    // its earliest expiry, so the refresh recomputes and reports B.
    crate::invalidate_cluster_route_graph_cache(&fixture.paths);
    let changes = tokio::time::timeout(Duration::from_secs(5), subscription.changed())
        .await
        .expect("a refresh after expiry reflects it");
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(changes[0].peer_node_id, fixture.b);
    assert_eq!(
        changes[0].current.as_ref().map(|peer| peer.verdict),
        Some(PeerPathVerdict::Unknown)
    );
    assert_eq!(subscription.current().peers, fresh);

    // A point read through the subscribed view agrees with a fresh read too.
    let point = peer_reachability_snapshot(&fixture.paths, &fixture.state).expect("point read");
    assert_eq!(point.peers, fresh);
    assert_eq!(
        point.peers,
        compute_peers(&fixture.paths, &fixture.state)
            .expect("fresh classification")
            .peers
    );
}

#[tokio::test]
async fn point_read_after_expiry_recomputes_without_a_hint() {
    let fixture = fixture("peer_reachability_expiry_point");
    let short_lived = crate::connectivity::cluster_connectivity_record(
        &fixture.state,
        &fixture.b_endpoint,
        ClusterConnectivityState::Healthy,
        Some(5),
        0,
        None,
        None,
        OffsetDateTime::now_utc(),
        1,
    );
    write(&fixture, &[short_lived]);
    let _subscription = subscribe_peer_reachability(&fixture.paths, &fixture.state);
    let before = peer_reachability_snapshot(&fixture.paths, &fixture.state).expect("read");
    assert_eq!(before.verdict(&fixture.b), PeerPathVerdict::Path);
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let after = peer_reachability_snapshot(&fixture.paths, &fixture.state).expect("read");
    assert_eq!(
        after.peers,
        compute_peers(&fixture.paths, &fixture.state)
            .expect("fresh classification")
            .peers,
        "the memo never outlives the evidence it was computed from"
    );
    assert_eq!(after.verdict(&fixture.b), PeerPathVerdict::Unknown);
}
