//! Real authenticated TCP carriers with an enforced A -> B -> C -> D adjacency.
//! The served connectivity payload is controlled; no Main or local Raft exists.
use super::*;
use crate::keyspace::{cluster_connectivity_key, cluster_endpoint_key, cluster_member_key};
use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_kv::InProcessKvStore;
use sinomo_network::{
    ClusterDirectory as _, ClusterObjectRpcAsyncHost, ClusterObjectRpcClient,
    ClusterObjectRpcOptions, ObjectRpcCall, RoutedConnector as _,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct ReadOnlyHost;
impl ClusterObjectRpcAsyncHost for ReadOnlyHost {}
#[derive(serde::Serialize, serde::Deserialize)]
struct OriginalOperation {
    id: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceCase {
    Success,
    PartiallyLiveMain,
    SaturatedHints,
    ColdSourceProcess,
    ColdRoutedSourceProcess,
    LiveFailureCountReset,
    SemanticChange,
    Deadline,
    DegradedShortcut,
    StalledLiveShortcut,
    StalledSourceHint,
    HealthyNoSources,
    HealthyUnrelatedSource,
    HealthyFailedSource,
    HealthySlowSource,
    CanonicalBridgeCalibrated,
    MissingCanonicalBridge,
    BridgeWrongSource,
    BridgeWrongCluster,
    BridgeUnknownMember,
    BridgeEndpointMismatch,
    BridgeFutureClock,
    BridgeExpiredClock,
    BridgeCanonicalFailure,
    BridgeCanonicalQuarantine,
    BridgeIdentityReason,
    BridgeAuthReason,
    BridgePendingTarget,
    BridgeRemovedTarget,
}
impl SourceCase {
    fn canonical_bridge(self) -> bool {
        matches!(self, Self::CanonicalBridgeCalibrated | Self::MissingCanonicalBridge
            | Self::BridgeWrongSource | Self::BridgeWrongCluster | Self::BridgeUnknownMember
            | Self::BridgeEndpointMismatch | Self::BridgeFutureClock | Self::BridgeExpiredClock
            | Self::BridgeCanonicalFailure | Self::BridgeCanonicalQuarantine
            | Self::BridgeIdentityReason | Self::BridgeAuthReason
            | Self::BridgePendingTarget | Self::BridgeRemovedTarget)
    }
    fn canonical_bridge_missing(self) -> bool {
        self.canonical_bridge() && !matches!(self, Self::CanonicalBridgeCalibrated
            | Self::BridgeCanonicalFailure | Self::BridgeCanonicalQuarantine)
    }
    fn canonical_bridge_succeeds(self) -> bool {
        matches!(self, Self::CanonicalBridgeCalibrated | Self::MissingCanonicalBridge)
    }
    fn healthy_guard(self) -> bool {
        matches!(self, Self::HealthyNoSources | Self::HealthyUnrelatedSource
            | Self::HealthyFailedSource | Self::HealthySlowSource)
    }
    fn calibrated(self) -> bool {
        matches!(self, Self::DegradedShortcut | Self::StalledLiveShortcut | Self::StalledSourceHint)
    }
    fn stalled(self) -> bool {
        matches!(self, Self::StalledLiveShortcut | Self::StalledSourceHint)
    }
}

// This test-only decorator preserves actual KV routing snapshots. An
// advisory failure supplies no route, membership, ACK or effect permission.
struct AdvisoryFailureDirectory {
    inner: Arc<dyn sinomo_network::ClusterDirectory>,
    started: Arc<AtomicBool>,
    failures: Arc<AtomicUsize>,
    release: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}
impl sinomo_network::ClusterDirectory for AdvisoryFailureDirectory {
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, sinomo_network::NetworkError> { self.inner.members() }
    fn endpoints(&self) -> Result<Vec<sinomo_base::ClusterEndpointRecord>, sinomo_network::NetworkError> { self.inner.endpoints() }
    fn node_statuses(&self) -> Result<std::collections::BTreeMap<NodeId, sinomo_base::ClusterNodeStatusRecord>, sinomo_network::NetworkError> { self.inner.node_statuses() }
    fn connectivity_records(&self) -> Result<Vec<sinomo_network::ClusterConnectivityRecord>, sinomo_network::NetworkError> { self.inner.connectivity_records() }
    fn connectivity_freshness_baseline(&self) -> Result<sinomo_network::DirectoryConnectivityBaseline, sinomo_network::NetworkError> {
        if self.started.load(Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            self.release.store(true, Ordering::SeqCst);
            self.notify.notify_one();
            return Err(sinomo_network::NetworkError::Directory { detail: "owned test advisory source unavailable".into() });
        }
        self.inner.connectivity_freshness_baseline()
    }
    fn version(&self) -> Result<sinomo_network::DirectoryVersion, sinomo_network::NetworkError> { self.inner.version() }
    fn routing_version(&self) -> Result<sinomo_network::DirectoryVersion, sinomo_network::NetworkError> { self.inner.routing_version() }
    fn records_snapshot(&self) -> Result<sinomo_network::DirectoryRecordsSnapshot, sinomo_network::NetworkError> { self.inner.records_snapshot() }
    fn routing_records_snapshot(&self) -> Result<sinomo_network::DirectoryRoutingRecordsSnapshot, sinomo_network::NetworkError> { self.inner.routing_records_snapshot() }
    fn snapshot(&self) -> Result<sinomo_network::DirectorySnapshot, sinomo_network::NetworkError> { self.inner.snapshot() }
}

// This host consumes ONLY the actual endpoint observation delivered by
// call_endpoint after its real authenticated one-hop RPC. It projects through
// the same cluster-owned coordinator/store boundary as the normal host.
struct NativeSourceObservationHost {
    owner: Option<(ClusterPaths, sinomo_base::ClusterBootstrapState)>,
    coordinator: Arc<crate::ClusterCommandObservationCoordinator>,
    emitted: Arc<std::sync::Mutex<Vec<sinomo_network::ClusterConnectivityRecord>>>,
}
impl ClusterObjectRpcAsyncHost for NativeSourceObservationHost {
    fn observe_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a sinomo_base::ClusterEndpointRecord,
        observation: sinomo_network::ClusterObjectRpcObservation,
    ) -> sinomo_network::ClusterObjectRpcFuture<'a, ()> {
        Box::pin(async move {
            let Some((paths, state)) = &self.owner else { return Ok(()); };
            let projected = crate::ClusterCommandConnectivityObservation::from_rpc(observation);
            let key = crate::cluster_command_observation_key(Some(paths), endpoint);
            let sequence = self.coordinator.next_sequence();
            let reservation = if observation.is_success() {
                self.coordinator.try_reserve_success(key, sequence)
            } else {
                self.coordinator.try_reserve_failure(key, sequence)
            };
            if let Some(reservation) = reservation {
                let mut actual = None;
                reservation.publish(|| { actual = Some(projected.record_daemon_owned(paths, state, endpoint)); });
                if let Some(actual) = actual {
                    self.emitted.lock().unwrap().push(actual?);
                }
            }
            Ok(())
        })
    }
}

async fn multihop_source_scenario(case: SourceCase) {
    let _registry = TEST_NETWORK_MODULE_REGISTRY_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let root = sinomo_testdir::TestDir::new("cold-live-two-bridges");
    let cluster = sinomo_node::ClusterId::from_legacy_named("cold-live-two-bridges").unwrap();
    let nodes = (0..4)
        .map(|i| NodeId::from_legacy_named(format!("cold-live-node-{i}")).unwrap())
        .collect::<Vec<_>>();
    let mut data = Vec::new();
    let mut keys = Vec::new();
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..4 {
        let dir = sinomo_local::SinomoDataDir::new(root.join(format!("node-{i}")));
        dir.ensure_root_layout().unwrap();
        keys.push(sinomo_node::load_or_create_main_node_identity(&dir.root()).unwrap());
        if i > 0 {
            let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            endpoints.push(sinomo_network::daemon_lan_endpoint(
                &nodes[i],
                listener.local_addr_string().unwrap(),
                0,
            ));
            listeners.push((i, listener, false));
        }
        data.push(dir);
    }
    let mut extra_endpoints = Vec::new();
    if matches!(
        case,
        SourceCase::ColdSourceProcess | SourceCase::ColdRoutedSourceProcess
    ) {
        // Two authenticated processes advertise the same source node. The
        // preferred endpoint has only expired in-memory source observations;
        // the other process holds the genuine live source renewal.
        let source_index = if case == SourceCase::ColdRoutedSourceProcess {
            2
        } else {
            1
        };
        endpoints[source_index - 1].priority = 10;
        let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        extra_endpoints.push(sinomo_network::daemon_lan_endpoint(
            &nodes[source_index],
            listener.local_addr_string().unwrap(),
            0,
        ));
        listeners.push((source_index, listener, true));
    }
    let now = OffsetDateTime::now_utc();
    let mut state = sinomo_base::ClusterBootstrapState::initialized_locally(
        cluster.clone(),
        "cold bridges",
        nodes[0].clone(),
        "caller",
        keys[0].public_key.clone(),
        now,
    );
    for i in 1..4 {
        let mut member = ClusterMemberRecord::local_non_voting(
            cluster.clone(),
            nodes[i].clone(),
            format!("node-{i}"),
            keys[i].public_key.clone(),
            now,
        );
        member.endpoints.push(endpoints[i - 1].clone());
        state.members.push(member);
    }
    for endpoint in &extra_endpoints {
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == endpoint.node_id)
            .unwrap()
            .endpoints
            .push(endpoint.clone());
    }
    let mut stalled_servers = tokio::task::JoinSet::new();
    let stalled_connections = Arc::new(AtomicUsize::new(0));
    if case == SourceCase::SaturatedHints {
        let mut selected = 0;
        for ordinal in 0u32..100_000 {
            let id = sinomo_network::ClusterEndpointId::derived(
                "cold-stalled-endpoint",
                &ordinal.to_le_bytes(),
            );
            if id >= endpoints[0].endpoint_id {
                continue;
            }
            let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let mut endpoint = sinomo_network::daemon_lan_endpoint(
                &nodes[1],
                listener.local_addr_string().unwrap(),
                100,
            );
            endpoint.endpoint_id = id;
            state.members[1].endpoints.push(endpoint);
            let accepted = stalled_connections.clone();
            stalled_servers.spawn(async move {
                let mut streams = Vec::new();
                loop {
                    streams.push(listener.accept().await.unwrap());
                    accepted.fetch_add(1, Ordering::SeqCst);
                }
            });
            selected += 1;
            if selected == 16 {
                break;
            }
        }
        assert_eq!(
            selected, 16,
            "sixteen earlier hints are deliberately stalled before the known adjacent endpoint"
        );
    }
    // Relays sign their own adjacent transport. Each real signer needs its
    // matching bootstrap identity, just as the caller does; a key alone is
    // insufficient for a legacy-named fixture node.
    for (i, dir) in data.iter().enumerate().skip(1) {
        let paths = ClusterPaths::new(dir);
        paths.ensure().unwrap();
        let mut own = state.clone();
        own.local_node_id = nodes[i].clone();
        crate::write_cluster_state(&paths, &own).unwrap();
    }
    let paths = ClusterPaths::new(&data[0]);
    paths.ensure().unwrap();
    crate::write_cluster_state(&paths, &state).unwrap();
    let mut live = Vec::new();
    let mut kv = InProcessKvStore::new();
    for member in &state.members {
        kv.put_serde(cluster_member_key(&member.node_id), member, None)
            .unwrap();
    }
    for (i, endpoint) in endpoints.iter().enumerate() {
        kv.put_serde(
            cluster_endpoint_key(&endpoint.node_id, endpoint),
            endpoint,
            None,
        )
        .unwrap();
        let mut source = state.clone();
        source.local_node_id = nodes[i].clone();
        let mut observation = sinomo_network::cluster_connectivity_record(
            &source,
            endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            None,
            0,
            None,
            None,
            now,
            900,
        );
        if case == SourceCase::ColdRoutedSourceProcess && i == 1 {
            // The fresh process remains reachable through a degraded edge;
            // the healthy alternate is deliberately the preferred cold one.
            // This makes process selection deterministic without a timing race.
            observation.state = sinomo_network::ClusterConnectivityState::Degraded;
            observation.consecutive_failures = 2;
        }
        let mut expired = observation.clone();
        expired.observed_at -= time::Duration::hours(1);
        expired.expires_at -= time::Duration::hours(1);
        if case == SourceCase::PartiallyLiveMain && i == 0 {
            expired = observation.clone();
        }
        if case == SourceCase::LiveFailureCountReset && i == 2 {
            expired.consecutive_failures = 1;
        }
        if i == 1 && matches!(case, SourceCase::BridgeCanonicalFailure | SourceCase::BridgeCanonicalQuarantine) {
            expired = observation.clone();
            expired.state = sinomo_network::ClusterConnectivityState::Unreachable;
            expired.consecutive_failures = 3;
            if case == SourceCase::BridgeCanonicalQuarantine {
                expired.failure_reason = Some(sinomo_network::ClusterConnectivityFailureReason::IdentityMismatch);
            }
        }
        // A negative canonical-publication cut: no success/edge/ACK is added.
        // B's later source row must come from its actual endpoint-RPC observer.
        if !(case.canonical_bridge_missing() && i == 1) {
            kv.put_serde(
                cluster_connectivity_key(
                    &expired.source_node_id,
                    &expired.target_node_id,
                    &expired.endpoint_id,
                ),
                &expired,
                None,
            ).unwrap();
        }
        live.push(observation);
    }
    if case.calibrated() {
        // Controlled routing DATA: the refusal/live-stall cases begin with a
        // live A->D degraded shortcut. The initial-source-hint case stores only
        // an expired B->D semantic baseline; B's real source later renews it.
        // The independently calibrated healthy A->B->C->D route is discoverable.
        let mut shortcut_owner = state.clone();
        if case == SourceCase::StalledSourceHint {
            shortcut_owner.local_node_id = nodes[1].clone();
        }
        let shortcut = sinomo_network::cluster_connectivity_record(
            &shortcut_owner,
            &endpoints[2],
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            2,
            None,
            None,
            now,
            sinomo_network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut stored_shortcut = shortcut.clone();
        if case == SourceCase::StalledSourceHint {
            // Only B's actual authenticated source can supply the renewal;
            // the initial current directory must not already claim A -> D.
            stored_shortcut.observed_at -= time::Duration::hours(1);
            stored_shortcut.expires_at -= time::Duration::hours(1);
        }
        kv.put_serde(
            cluster_connectivity_key(
                &stored_shortcut.source_node_id,
                &stored_shortcut.target_node_id,
                &stored_shortcut.endpoint_id,
            ),
            &stored_shortcut,
            None,
        )
        .unwrap();
        live.push(shortcut);
    }
    if case.healthy_guard() {
        let healthy = sinomo_network::cluster_connectivity_record(&state, &endpoints[2],
            sinomo_network::ClusterConnectivityState::Healthy, None, 0, None, None, now, 900);
        kv.put_serde(cluster_connectivity_key(&healthy.source_node_id, &healthy.target_node_id,
            &healthy.endpoint_id), &healthy, None).unwrap();
        live.push(healthy);
    }
    for endpoint in &extra_endpoints {
        kv.put_serde(
            cluster_endpoint_key(&endpoint.node_id, endpoint),
            endpoint,
            None,
        )
        .unwrap();
        let target_index = nodes
            .iter()
            .position(|node| node == &endpoint.node_id)
            .unwrap();
        let mut source = state.clone();
        source.local_node_id = nodes[target_index - 1].clone();
        let observation = sinomo_network::cluster_connectivity_record(
            &source,
            endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            None,
            0,
            None,
            None,
            now,
            900,
        );
        kv.put_serde(
            cluster_connectivity_key(
                &observation.source_node_id,
                &observation.target_node_id,
                &observation.endpoint_id,
            ),
            &observation,
            None,
        )
        .unwrap();
        live.push(observation);
    }
    crate::write_cluster_kv_snapshot(&paths, &kv.snapshot().unwrap()).unwrap();
    let original_snapshot = std::fs::read(&paths.kv_paths().snapshot).unwrap();
    let mut changed = live[2].clone();
    changed.state = sinomo_network::ClusterConnectivityState::Unreachable;
    changed.consecutive_failures = 3;
    kv.put_serde(
        cluster_connectivity_key(
            &changed.source_node_id,
            &changed.target_node_id,
            &changed.endpoint_id,
        ),
        &changed,
        None,
    )
    .unwrap();
    let replacement = kv.snapshot().unwrap();

    sinomo_kv::deny_process_local_raft_access_for_root(paths.kv_paths().raft.clone()).unwrap();
    let observed = Arc::new(KvClusterDirectory::new(paths.clone()));
    assert_eq!(
        observed.connectivity_records().unwrap().len(),
        usize::from(matches!(case, SourceCase::BridgeCanonicalFailure | SourceCase::BridgeCanonicalQuarantine))
        + usize::from(matches!(
            case,
            SourceCase::PartiallyLiveMain
                | SourceCase::ColdSourceProcess
                | SourceCase::ColdRoutedSourceProcess
                | SourceCase::DegradedShortcut
                | SourceCase::StalledLiveShortcut
                | SourceCase::HealthyNoSources | SourceCase::HealthyUnrelatedSource
                | SourceCase::HealthyFailedSource | SourceCase::HealthySlowSource
        ))
    );
    let baseline = observed.connectivity_freshness_baseline().unwrap();
    assert_eq!(
        baseline.records.len(),
        3 + extra_endpoints.len() + usize::from(case.calibrated() || case.healthy_guard())
            - usize::from(case.canonical_bridge_missing())
    );
    for record in &live {
        if case.canonical_bridge() && record.source_node_id == nodes[1]
            && record.target_node_id == nodes[2] {
            let canonical = baseline.records.iter().find(|old|
                old.source_node_id == record.source_node_id && old.target_node_id == record.target_node_id
                    && old.endpoint_id == record.endpoint_id);
            if case.canonical_bridge_missing() { assert!(canonical.is_none()); }
            else if matches!(case, SourceCase::BridgeCanonicalFailure | SourceCase::BridgeCanonicalQuarantine) {
                assert_eq!(canonical.unwrap().state, sinomo_network::ClusterConnectivityState::Unreachable);
            } else { assert!(sinomo_network::cluster_connectivity_stable_eq(canonical.unwrap(), record)); }
            continue;
        }
        assert!(
            baseline
                .records
                .iter()
                .any(|old| sinomo_network::cluster_connectivity_stable_eq(old, record)),
            "missing baseline: {record:?}"
        );
    }
    let directory = sinomo_network::DirectorySnapshot::try_from_records(
        sinomo_network::DirectoryVersion(1),
        state.members.clone(),
        endpoints.iter().chain(&extra_endpoints).cloned(),
    )
    .unwrap();
    let live_snapshot =
        sinomo_network::connectivity_snapshot_from_directory_records(&directory, live.clone(), now)
            .unwrap();
    assert!(
        !sinomo_network::plan_routes(
            &directory,
            &live_snapshot,
            &nodes[0],
            &nodes[3],
            Default::default()
        )
        .is_empty(),
        "controlled live graph has an executable two-bridge route"
    );
    if case == SourceCase::ColdRoutedSourceProcess {
        let preferred = sinomo_network::plan_routes(
            &directory,
            &live_snapshot,
            &nodes[0],
            &nodes[2],
            Default::default(),
        );
        assert_eq!(
            preferred[0].hops.last().unwrap().endpoint.endpoint_id,
            extra_endpoints[0].endpoint_id,
            "the before-control must actually choose the cold process first"
        );
    }
    let reads = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let effects = Arc::new(AtomicUsize::new(0));
    let rejected_direct = Arc::new(AtomicUsize::new(0));
    let shortcut_refusals = Arc::new(AtomicUsize::new(0));
    let stalled_handshakes = Arc::new(AtomicUsize::new(0));
    let drained_stalls = Arc::new(AtomicUsize::new(0));
    let stalled_read_errors = Arc::new(AtomicUsize::new(0));
    let stalled_handshake_bytes = Arc::new(AtomicUsize::new(0));
    let original_started = Arc::new(AtomicBool::new(false));
    let baseline_failures = Arc::new(AtomicUsize::new(0));
    let guard_release = Arc::new(AtomicBool::new(false));
    let guard_notify = Arc::new(tokio::sync::Notify::new());
    let guard_held = Arc::new(AtomicUsize::new(0));
    let first_binding = Arc::new(std::sync::Mutex::new(None::<[u8; 32]>));
    let original_binding = Arc::new(std::sync::Mutex::new(None::<[u8; 32]>));
    let hot_binding = Arc::new(std::sync::Mutex::new(None::<[u8; 32]>));
    let failed_source_calls = Arc::new(AtomicUsize::new(0));
    let slow_source_calls = Arc::new(AtomicUsize::new(0));
    let native_source_rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    let bridge_reports = Arc::new(std::sync::Mutex::new(Vec::<Vec<sinomo_network::ClusterConnectivityRecord>>::new()));
    let native_observation_coordinator = Arc::new(crate::ClusterCommandObservationCoordinator::production());
    let first_bridge_read_refusals = Arc::new(AtomicUsize::new(usize::MAX));
    let mut servers = tokio::task::JoinSet::new();
    for (i, listener, cold_source_process) in listeners {
        let node = nodes[i].clone();
        let adjacent = nodes[i - 1].clone();
        let caller = nodes[0].clone();
        let directory = directory.clone();
        let mut replies = live.clone();
        if cold_source_process {
            for reply in &mut replies {
                if reply.source_node_id == node {
                    reply.observed_at -= time::Duration::hours(1);
                    reply.expires_at -= time::Duration::hours(1);
                }
            }
        }
        let server_paths = if case.canonical_bridge() { ClusterPaths::new(&data[i]) } else { paths.clone() };
        let bridge_reports = bridge_reports.clone();
        let replacement = replacement.clone();

        let reads = reads.clone();
        let effects = effects.clone();
        let rejected_direct = rejected_direct.clone();
        let shortcut_refusals = shortcut_refusals.clone();
        let original_started = original_started.clone();
        let first_bridge_read_refusals = first_bridge_read_refusals.clone();
        let stalled_handshakes = stalled_handshakes.clone();
        let drained_stalls = drained_stalls.clone();
        let stalled_read_errors = stalled_read_errors.clone();
        let stalled_handshake_bytes = stalled_handshake_bytes.clone();
        let stalled_source = nodes[if case == SourceCase::StalledSourceHint { 1 } else { 0 }].clone();
        let guard_release = guard_release.clone();
        let guard_notify = guard_notify.clone();
        let guard_held = guard_held.clone();
        let first_binding = first_binding.clone();
        let original_binding = original_binding.clone();
        let hot_binding = hot_binding.clone();
        let failed_source_calls = failed_source_calls.clone();
        let slow_source_calls = slow_source_calls.clone();
        let identity = Arc::new(
            sinomo_network::DaemonRoutedSessionIdentity::new(node.clone(), data[i].clone())
                .unwrap(),
        );
        let provider = Arc::new(
            sinomo_network::ClusterLinkProvider::new(
                node.clone(),
                identity.clone(),
                sinomo_network::NetworkHealthPolicy::default(),
            )
            .unwrap(),
        );
        servers.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let mut stream = listener.accept().await.unwrap().into_inner();
                let node = node.clone();
                let adjacent = adjacent.clone();
                let caller = caller.clone();
                let directory = directory.clone();
                let identity = identity.clone();
                let provider = provider.clone();
                let replies = replies.clone();
                let server_paths = server_paths.clone();
                let bridge_reports = bridge_reports.clone();
                let replacement = replacement.clone();

                let reads = reads.clone();
                let effects = effects.clone();
                let rejected_direct = rejected_direct.clone();
                let shortcut_refusals = shortcut_refusals.clone();
                let original_started = original_started.clone();
                let first_bridge_read_refusals = first_bridge_read_refusals.clone();
                let stalled_handshakes = stalled_handshakes.clone();
                let drained_stalls = drained_stalls.clone();
                let stalled_read_errors = stalled_read_errors.clone();
                let stalled_handshake_bytes = stalled_handshake_bytes.clone();
                let stalled_source = stalled_source.clone();
                let guard_release = guard_release.clone();
                let guard_notify = guard_notify.clone();
                let guard_held = guard_held.clone();
                let first_binding = first_binding.clone();
                let original_binding = original_binding.clone();
                let hot_binding = hot_binding.clone();
                let failed_source_calls = failed_source_calls.clone();
                let slow_source_calls = slow_source_calls.clone();
                connections.spawn(async move {
                    let metadata =
                        sinomo_network::read_authenticated_transport_prelude(&mut stream).await?;
                    let source = sinomo_network::verify_builtin_authenticated_transport_metadata(
                        &metadata,
                        &node,
                        &directory,
                        identity.as_ref(),
                    )?;
                    let direct_binding = metadata.channel_binding;
                    if case.healthy_guard() && i == 3 && source == caller
                        && original_started.load(Ordering::SeqCst)
                        && guard_held.fetch_add(1, Ordering::SeqCst) == 0 {
                        // Hold the real authenticated physical socket before
                        // NORMAL routed acceptance. Keep its session bytes intact.
                        // The Original later must prove the SAME prelude binding.
                        *first_binding.lock().unwrap() = Some(direct_binding);
                        loop {
                            let notified = guard_notify.notified();
                            if guard_release.load(Ordering::SeqCst) { break; }
                            notified.await;
                        }
                    }
                    // Real authentication precedes adjacency rejection. C and D cannot
                    // accept the caller's direct link, only their previous bridge.
                    if source != adjacent
                        && !(case.healthy_guard() && i == 3 && source == caller) {
                        if case.stalled() && i == 3 && source == stalled_source
                            && original_started.load(Ordering::SeqCst) {
                            // Hold the ACTUAL authenticated adjacent TCP stream
                            // before routed acceptance. Drain real incoming bytes
                            // without inventing any successful ACK/frame/result.
                            // EOF/error is diagnostic only, not local cancellation proof.
                            stalled_handshakes.fetch_add(1, Ordering::SeqCst);
                            use tokio::io::AsyncReadExt as _;
                            let mut discard = [0u8; 1024];
                            loop {
                                match stream.read(&mut discard).await {
                                    Ok(0) => {
                                        drained_stalls.fetch_add(1, Ordering::SeqCst);
                                        break;
                                    }
                                    Err(_) => {
                                        stalled_read_errors.fetch_add(1, Ordering::SeqCst);
                                        break;
                                    }
                                    Ok(count) => { stalled_handshake_bytes.fetch_add(count, Ordering::SeqCst); }
                                }
                            }
                            return Ok::<(), sinomo_network::NetworkError>(());
                        }
                        if case == SourceCase::DegradedShortcut && i == 3 && source == caller {
                            // This count follows real native prelude verification.
                            // No successful transport/session/application reply is fabricated.
                            shortcut_refusals.fetch_add(1, Ordering::SeqCst);
                        }
                        rejected_direct.fetch_add(1, Ordering::SeqCst);
                        return Ok::<(), sinomo_network::NetworkError>(());
                    }
                    let direct = sinomo_network::AuthenticatedDirectStream::accepted(
                        Box::new(stream),
                        node.clone(),
                        source,
                        sinomo_base::Id::generate(),
                        metadata.channel_binding,
                        metadata.capabilities,
                    )?;
                    let disposition = sinomo_network::accept_or_relay_routed_session(
                        direct,
                        &directory,
                        identity.as_ref(),
                        provider.as_ref(),
                        None,
                    )
                    .await
                    .map_err(|error| {
                        eprintln!("fixture relay/accept node={node} error={error}");
                        error
                    })?;
                    if let sinomo_network::RoutedSessionDisposition::Destination(carrier) =
                        disposition
                    {
                        let mut acceptor = sinomo_network::accept_routed_stream_carrier(carrier);
                        while let Some(stream) = acceptor.accept().await? {
                            if (case.calibrated() || case.canonical_bridge()) && !original_started.load(Ordering::SeqCst) {
                                assert!(stream.peer_node_id == caller || stream.peer_node_id == adjacent,
                                    "calibration still requires an actual admitted caller or adjacent signer");
                            } else {
                                assert_eq!(stream.peer_node_id, caller);
                            }
                            let peer = sinomo_network::RpcPeerContext::authenticated(
                                stream.local_node_id.clone(),
                                stream.peer_node_id.clone(),
                                stream.session_id.clone(),
                                stream.channel_binding,
                            )?;
                            let replies = replies.clone();
                            let server_paths = server_paths.clone();
                            let bridge_reports = bridge_reports.clone();
                            let replacement = replacement.clone();

                            let reads = reads.clone();
                            let effects = effects.clone();
                            let shortcut_refusals = shortcut_refusals.clone();
                            let original_started = original_started.clone();
                            let first_bridge_read_refusals = first_bridge_read_refusals.clone();
                            let stalled_handshakes = stalled_handshakes.clone();
                            let guard_release = guard_release.clone();
                            let guard_notify = guard_notify.clone();
                            let original_binding = original_binding.clone();
                            let hot_binding = hot_binding.clone();
                            let failed_source_calls = failed_source_calls.clone();
                            let slow_source_calls = slow_source_calls.clone();
                            sinomo_network::serve_service_rpc_peer_stream(
                                stream,
                                &peer,
                                sinomo_network::ObjectFrameIoOptions::default(),
                                move |mut request, _| {
                                    let replies = replies.clone();
                                    let server_paths = server_paths.clone();
                                    let bridge_reports = bridge_reports.clone();
                                    let replacement = replacement.clone();

                                    let reads = reads.clone();
                                    let effects = effects.clone();
                                    let shortcut_refusals = shortcut_refusals.clone();
                                    let original_started = original_started.clone();
                                    let first_bridge_read_refusals = first_bridge_read_refusals.clone();
                                    let stalled_handshakes = stalled_handshakes.clone();
                                    let guard_release = guard_release.clone();
                                    let guard_notify = guard_notify.clone();
                                    let original_binding = original_binding.clone();
                                    let hot_binding = hot_binding.clone();
                                    let failed_source_calls = failed_source_calls.clone();
                                    let slow_source_calls = slow_source_calls.clone();
                                    async move {
                                        let route = request.request.route.clone();
                                        if route
                                            == sinomo_network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE
                                        {
                                            let guard_hot_read = request.request.metadata.get("guard-hot-read").map(String::as_str)
                                                == Some("healthy-first-carrier");
                                            let _: () = request.decode_body().await?;
                                            reads[i].fetch_add(1, Ordering::SeqCst);
                                            if guard_hot_read {
                                                assert_eq!(case, SourceCase::HealthyNoSources);
                                                assert_eq!(i, 3);
                                                *hot_binding.lock().unwrap() = Some(direct_binding);
                                            }
                                            if original_started.load(Ordering::SeqCst) {
                                                if case == SourceCase::HealthyFailedSource && i == 1 {
                                                    failed_source_calls.fetch_add(1, Ordering::SeqCst);
                                                }
                                                if case == SourceCase::HealthySlowSource && i == 1 {
                                                    slow_source_calls.fetch_add(1, Ordering::SeqCst);
                                                }
                                                if (case == SourceCase::HealthyUnrelatedSource && i == 2)
                                                    || (matches!(case, SourceCase::HealthyFailedSource | SourceCase::HealthySlowSource) && i == 1) {
                                                    // Actual C reachability over B requires accepted
                                                    // B source progress, while preferred A-D is unchanged.
                                                    guard_release.store(true, Ordering::SeqCst);
                                                    guard_notify.notify_one();
                                                }
                                                if case == SourceCase::HealthyFailedSource && i == 1 {
                                                    return Err(sinomo_network::NetworkError::Io {
                                                        operation: "owned test source response failure".into(),
                                                        detail: "actual request decoded before response failure".into(),
                                                    });
                                                }
                                                if case == SourceCase::HealthySlowSource && i == 1 {
                                                    std::future::pending::<()>().await;
                                                }
                                            }
                                            if case.calibrated() && i == 2
                                                && original_started.load(Ordering::SeqCst) {
                                                let _ = first_bridge_read_refusals.compare_exchange(
                                                    usize::MAX, if case.stalled() { stalled_handshakes.load(Ordering::SeqCst) }
                                                        else { shortcut_refusals.load(Ordering::SeqCst) },
                                                    Ordering::SeqCst, Ordering::SeqCst,
                                                );
                                            }
                                            if i == 2 && case == SourceCase::SemanticChange {
                                                crate::write_cluster_kv_snapshot(
                                                    &server_paths,
                                                    &replacement,
                                                )
                                                .unwrap();
                                            }
                                            if i == 2 && case == SourceCase::Deadline {
                                                std::future::pending::<()>().await;
                                            }

                                            // New bridge controls report the real source owner's local
                                            // store. Only negative cases alter an earned response row.
                                            let mut replies = if case.canonical_bridge() {
                                                sinomo_network::read_local_cluster_connectivity_records(&server_paths)?
                                            } else { replies };
                                            if case.canonical_bridge() && i == 1 && original_started.load(Ordering::SeqCst) {
                                                assert_eq!(replies.len(), 1, "actual B owner report prerequisite");
                                                let row = &mut replies[0];
                                                match case {
                                                    SourceCase::BridgeWrongSource => row.source_node_id = NodeId::from_legacy_named("untrusted-third-party-source").unwrap(),
                                                    SourceCase::BridgeWrongCluster => row.cluster_id = sinomo_node::ClusterId::from_legacy_named("unrelated-live-cluster").unwrap(),
                                                    SourceCase::BridgeUnknownMember => row.target_node_id = NodeId::from_legacy_named("unaccepted-bridge-target").unwrap(),
                                                    SourceCase::BridgeEndpointMismatch => row.endpoint_id = sinomo_network::ClusterEndpointId::derived("unaccepted-bridge-endpoint", b"negative"),
                                                    SourceCase::BridgeFutureClock => { row.observed_at = OffsetDateTime::now_utc() + time::Duration::hours(1); row.expires_at = row.observed_at + time::Duration::hours(1); }
                                                    SourceCase::BridgeExpiredClock => row.expires_at = OffsetDateTime::now_utc() - time::Duration::seconds(1),
                                                    SourceCase::BridgeIdentityReason => row.failure_reason = Some(sinomo_network::ClusterConnectivityFailureReason::IdentityMismatch),
                                                    SourceCase::BridgeAuthReason => row.failure_reason = Some(sinomo_network::ClusterConnectivityFailureReason::CommandAuthDenied),
                                                    _ => {},
                                                }
                                                let mut reported = bridge_reports.lock().unwrap();
                                                assert!(reported.len() < 32, "bounded native source report capture");
                                                reported.push(replies.clone());
                                            }
                                            // Other original cases retain their controlled third-party DATA.
                                            sinomo_network::ObjectRpcOutgoingResponse::encode_value(route, replies)
                                        } else {
                                            assert_eq!(i, 3);
                                            assert_eq!(route, "/test/cold-multihop-original");
                                            assert_eq!(
                                                request
                                                    .request
                                                    .metadata
                                                    .get("operation-id")
                                                    .map(String::as_str),
                                                Some("original-multihop")
                                            );
                                            let body: OriginalOperation =
                                                request.decode_body().await?;
                                            assert_eq!(body.id, "original-multihop");
                                            assert_eq!(body.bytes, vec![0x71; 65579]);
                                            if case.healthy_guard() {
                                                *original_binding.lock().unwrap() = Some(direct_binding);
                                            }
                                            assert_eq!(effects.fetch_add(1, Ordering::SeqCst), 0);
                                            sinomo_network::ObjectRpcOutgoingResponse::encode_value(
                                                route,
                                                "original response".to_string(),
                                            )
                                        }
                                    }
                                },
                            )
                            .await?;
                        }
                    }
                    Ok::<(), sinomo_network::NetworkError>(())
                });
                while connections.try_join_next().is_some() {}
            }
        });
    }
    // Calibration uses actual native carrier/auth/RPC owners on this SAME
    // fixture and an explicitly supplied complete routing DATA graph. Three
    // direct endpoint RPCs prove each adjacent source can serve real data;
    // one nonpooled full-chain connect proves the actual relay/accept path.
    // Direct-endpoint pool scopes differ from the Original fenced scope, and
    // the end-to-end connector never enters the object-RPC pool. No business
    // call, KV/DFS effect authority, or successful native ACK is fabricated.
    let calibration_started_at = std::time::Instant::now();
    let calibration = if case.calibrated() || case.canonical_bridge() {
        async {
            let mut facts = Vec::new();
            for index in 1usize..=3 {
                let one = async {
                    let actor = index - 1;
                    let actor_paths = ClusterPaths::new(&data[actor]);
                    let mut actor_state = state.clone();
                    actor_state.local_node_id = nodes[actor].clone();
                    let mut host = NativeSourceObservationHost {
                        owner: (case.canonical_bridge() && actor > 0).then(|| (actor_paths.clone(), actor_state.clone())),
                        coordinator: native_observation_coordinator.clone(),
                        emitted: native_source_rows.clone(),
                    };
                    let mut client = ClusterObjectRpcClient::routed(&mut host, &actor_paths);
                    let response = client.call_endpoint(&actor_state, &endpoints[index-1],
                        ObjectRpcCall::encode_value(
                            sinomo_network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                            sinomo_network::CLUSTER_NETWORK_RPC_CATEGORY, (),
                        ).map_err(|e|e.to_string())?,
                        ClusterObjectRpcOptions::default().with_overall_timeout(
                            sinomo_network::DEFAULT_NETWORK_TIMEOUT),
                    ).await.map_err(|e|e.to_string())?;
                    let payload = response.decode_body::<Vec<sinomo_network::ClusterConnectivityRecord>>()
                        .await.map_err(|e|e.to_string())?;
                    if !case.canonical_bridge() && index == 2 && !payload.iter().any(|r|
                        r.source_node_id == nodes[2] && r.target_node_id == nodes[3]
                            && r.state == sinomo_network::ClusterConnectivityState::Healthy
                            && r.expires_at > OffsetDateTime::now_utc()) {
                        return Err("C's actual authenticated source endpoint did not serve its controlled live edge".into());
                    }
                    Ok::<_,String>(serde_json::json!({"actual_direct_source":nodes[actor],
                        "actual_direct_target":nodes[index],"actual_endpoint":endpoints[index-1],
                        "actual_served_routing_data":payload}))
                };
                facts.push(tokio::time::timeout(sinomo_network::DEFAULT_NETWORK_TIMEOUT, one)
                    .await.map_err(|e|format!("calibration original5s action budget expired: {e}"))??);
            }
            if case.canonical_bridge() {
                let mut host = ReadOnlyHost;
                let mut probe = ClusterObjectRpcClient::routed(&mut host, &paths);
                let raw = probe.call_endpoint(&state, &endpoints[0],
                    ObjectRpcCall::encode_value(sinomo_network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                        sinomo_network::CLUSTER_NETWORK_RPC_CATEGORY, ()).map_err(|e|e.to_string())?,
                    ClusterObjectRpcOptions::default().with_overall_timeout(sinomo_network::DEFAULT_NETWORK_TIMEOUT),
                ).await.map_err(|e|e.to_string())?
                    .decode_body::<Vec<sinomo_network::ClusterConnectivityRecord>>()
                    .await.map_err(|e|e.to_string())?;
                let earned = sinomo_network::read_local_cluster_connectivity_records(&ClusterPaths::new(&data[1]))
                    .map_err(|e|e.to_string())?;
                if raw != earned || raw.len() != 1 || raw[0].source_node_id != nodes[1]
                    || raw[0].target_node_id != nodes[2]
                    || raw[0].endpoint_id != endpoints[1].endpoint_id
                    || raw[0].state != sinomo_network::ClusterConnectivityState::Healthy {
                    return Err(format!("actual authenticated B source did not serve its earned B-C row: {raw:?}"));
                }
                facts.push(serde_json::json!({"actual_authenticated_B_raw_report":raw,
                    "source_owner":"actual B direct-endpoint observation callback/store"}));
            }
            let full_chain = async {
                let identity = Arc::new(sinomo_network::DaemonRoutedSessionIdentity::new(
                    nodes[0].clone(), data[0].clone()).map_err(|e|e.to_string())?);
                let provider = Arc::new(sinomo_network::ClusterLinkProvider::new(
                    nodes[0].clone(), identity.clone(),
                    sinomo_network::NetworkHealthPolicy::default()).map_err(|e|e.to_string())?);
                let tracker = Arc::new(sinomo_network::ConnectivityTracker::new(live_snapshot.clone()));
                let connector = sinomo_network::MultiHopRoutedConnector::from_snapshot(
                    nodes[0].clone(), directory.clone(), tracker.clone(), tracker, provider, identity,
                ).map_err(|e|e.to_string())?;
                let stream = connector.connect(nodes[3].clone(),
                    sinomo_network::ConnectOptions {
                        timeout: sinomo_network::DEFAULT_NETWORK_TIMEOUT,
                        ..Default::default()
                    }).await.map_err(|e|e.to_string())?;
                let actual_path = stream.route_trace.nodes.clone();
                if actual_path != nodes {
                    return Err(format!("calibration did not use the real healthy bridge chain: {actual_path:?}"));
                }
                drop(stream);
                Ok::<_,String>(serde_json::json!({"actual_nonpooled_native_route":actual_path}))
            };
            facts.push(tokio::time::timeout(sinomo_network::DEFAULT_NETWORK_TIMEOUT, full_chain)
                .await.map_err(|e|format!("calibration original5s action budget expired: {e}"))??);
            Ok::<_,String>(facts)
        }.await
    } else {
        Ok(Vec::new())
    };
    let calibration_elapsed = calibration_started_at.elapsed();
    // Negative membership DATA applies only after genuine B/C production and
    // full-chain calibration. The Original's own fenced current membership
    // must exclude a Pending/Removed intermediate target, even while its
    // physical endpoint and positive historical observation still exist.
    if matches!(case, SourceCase::BridgePendingTarget | SourceCase::BridgeRemovedTarget) && calibration.is_ok() {
        state.members.iter_mut().find(|m| m.node_id == nodes[2]).unwrap().state =
            if case == SourceCase::BridgePendingTarget { sinomo_base::ClusterMemberState::Pending }
            else { sinomo_base::ClusterMemberState::Removed };
        let fenced = sinomo_network::DirectorySnapshot::try_from_members(
            sinomo_network::DirectoryVersion(1), state.members.clone()).unwrap();
        assert!(!fenced.is_authorized(&nodes[2]));
        assert!(fenced.node(&nodes[2]).is_none());
    }
    let reads_before_original = reads
        .iter()
        .map(|n| n.load(Ordering::SeqCst))
        .collect::<Vec<_>>();
    let refusals_before_original = shortcut_refusals.load(Ordering::SeqCst);
    let mut host = ReadOnlyHost;
    // Keep the existing immutable fenced routing fixture. A Dynamic factory
    // setter is process-global with no scoped restore; do not pretend that
    // the existing module registry mutex isolates every directory consumer.
    let observed: Arc<dyn sinomo_network::ClusterDirectory> = if case == SourceCase::HealthyNoSources {
        Arc::new(AdvisoryFailureDirectory {
            inner: observed, started: original_started.clone(), failures: baseline_failures.clone(),
            release: guard_release.clone(), notify: guard_notify.clone(),
        })
    } else { observed };
    let mut client = ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
        &mut host, &paths, &state, observed,
    )
    .unwrap();
    let mut call = ObjectRpcCall::encode_value(
        "/test/cold-multihop-original",
        "test",
        OriginalOperation {
            id: "original-multihop".into(),
            bytes: vec![0x71; 65579],
        },
    )
    .unwrap();
    call.request
        .metadata
        .insert("operation-id".into(), "original-multihop".into());
    let mut options =
        ClusterObjectRpcOptions::default().with_overall_timeout(std::time::Duration::from_secs(5));
    if matches!(
        case,
        SourceCase::PartiallyLiveMain | SourceCase::DegradedShortcut
            | SourceCase::StalledLiveShortcut | SourceCase::StalledSourceHint
            | SourceCase::HealthyNoSources | SourceCase::HealthyUnrelatedSource
            | SourceCase::HealthyFailedSource | SourceCase::HealthySlowSource
    ) {
        options = options.with_destination_process_affinity(
            sinomo_network::DestinationProcessAffinity::MainDaemon,
        );
    }
    if case.canonical_bridge() && calibration.is_ok() {
        let actual = native_source_rows.lock().unwrap();
        assert_eq!(actual.len(), 2, "B-C and C-D must come from actual endpoint callbacks");
        for (source, target, endpoint) in [(1usize, 2usize, &endpoints[1]), (2, 3, &endpoints[2])] {
            let row = actual.iter().find(|row| row.source_node_id == nodes[source]
                && row.target_node_id == nodes[target] && row.endpoint_id == endpoint.endpoint_id).unwrap();
            assert_eq!(row.cluster_id, cluster);
            assert_eq!(row.address, endpoint.address);
            assert_eq!(row.transport, endpoint.transport);
            assert_eq!(row.state, sinomo_network::ClusterConnectivityState::Healthy);
            assert!(row.observed_at <= OffsetDateTime::now_utc() && row.expires_at > OffsetDateTime::now_utc());
            let owned = sinomo_network::read_local_cluster_connectivity_records(&ClusterPaths::new(&data[source])).unwrap();
            assert_eq!(owned, vec![row.clone()], "served owner is the actual normal local observation store");
        }
    }
    let original_started_at = std::time::Instant::now();
    let response = if calibration.is_ok() {
        original_started.store(true, Ordering::SeqCst);
        Some(
            match client.call_target(&state, &nodes[3], call, options).await {
                Ok(response) => response.decode_body::<String>().await,
                Err(error) => Err(error),
            },
        )
    } else {
        None
    };
    let original_elapsed = original_started_at.elapsed();
    let reads_before_hot = reads.iter().map(|n| n.load(Ordering::SeqCst)).collect::<Vec<_>>();
    let baseline_before_hot = baseline_failures.load(Ordering::SeqCst);
    let hot_response = if case == SourceCase::HealthyNoSources
        && response.as_ref().is_some_and(|r| r.is_ok()) {
        let mut hot_call = ObjectRpcCall::encode_value(
            sinomo_network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
            sinomo_network::CLUSTER_NETWORK_RPC_CATEGORY, ()).unwrap();
        // Harmless test DATA marks only this separate read-only call, never
        // changes the Original body/ID or grants a carrier/effect permission.
        hot_call.request.metadata.insert("guard-hot-read".into(), "healthy-first-carrier".into());
        Some(match client.call_target(&state, &nodes[3], hot_call, options).await {
            Ok(response) => response.decode_body::<Vec<sinomo_network::ClusterConnectivityRecord>>().await,
            Err(error) => Err(error),
        })
    } else { None };
    if case.healthy_guard() {
        let facts = serde_json::json!({
            "qualification":"controlled DATA plus real TCP/auth/session/relay/RPC; no Dynamic Main/Raft/DFS proof",
            "case":format!("{case:?}"),"actual_original_response":format!("{response:#?}"),
            "original_elapsed_us":original_elapsed.as_micros(),"original_budget_ms":5000,
            "initial_physical_prelude_binding":*first_binding.lock().unwrap(),
            "actual_original_physical_prelude_binding":*original_binding.lock().unwrap(),
            "actual_hot_read_physical_prelude_binding":*hot_binding.lock().unwrap(),
            "binding_limit":"Direct source D can create extra A-D sockets. Original must equal first held socket; otherwise the prerequisite is unqualified.",
            "reads_before_original":reads_before_original,"reads_before_hot":reads_before_hot,
            "reads_after_hot":reads.iter().map(|n| n.load(Ordering::SeqCst)).collect::<Vec<_>>(),
            "baseline_fault_hits":baseline_failures.load(Ordering::SeqCst),"baseline_before_hot":baseline_before_hot,
            "hot_response":format!("{hot_response:#?}"),"failed_source_calls":failed_source_calls.load(Ordering::SeqCst),
            "slow_source_calls":slow_source_calls.load(Ordering::SeqCst),"single_original_effects":effects.load(Ordering::SeqCst),
            "candidate_cap":sinomo_network::MAX_ROUTE_CANDIDATES,
            "no_remote_eof_or_cancellation_inference":true,
        });
        std::fs::write(data[0].root().join("healthy-first-carrier-guard.json"),
            serde_json::to_vec_pretty(&facts).unwrap()).unwrap();
    }
    if case.stalled() {
        // Capture actual remote read outcomes only. A relay may retain its
        // downstream handshake until the original wire deadline, so remote
        // EOF is not a proof of local losing-probe cancellation.
        let facts = serde_json::json!({
            "qualification":"controlled routing DATA + real native TCP/auth/session/relay/RPC; FencedBootstrap shared scheduling only; no Dynamic Main/Raft/DFS publication or exactC18 RAM graph proof",
            "case":format!("{case:?}"),"route_identity":"FencedBootstrap",
            "calibration":calibration,"calibration_elapsed_us":calibration_elapsed.as_micros(),
            "reads_before_original":reads_before_original,
            "reads_after_original":reads.iter().map(|n|n.load(Ordering::SeqCst)).collect::<Vec<_>>(),
            "actual_authenticated_stalled_streams":stalled_handshakes.load(Ordering::SeqCst),
            "actual_stalled_stream_eof_count":drained_stalls.load(Ordering::SeqCst),
            "actual_stalled_stream_read_errors":stalled_read_errors.load(Ordering::SeqCst),
            "actual_stalled_session_input_bytes":stalled_handshake_bytes.load(Ordering::SeqCst),
            "remote_read_outcomes_are_not_local_cancellation_proof":true,
            "new_success_may_preempt_stall":"A newly discovered healthy route may win before a losing probe reaches remote authentication. That is valid, not proof that the old failed carrier never existed.",
            "first_C_read_stalled_stream_count":first_bridge_read_refusals.load(Ordering::SeqCst),
            "original_elapsed_us":original_elapsed.as_micros(),
            "remaining_original_budget_us":sinomo_network::DEFAULT_NETWORK_TIMEOUT.saturating_sub(original_elapsed).as_micros(),
            "actual_original_response":format!("{response:#?}"),"actual_single_service_effects":effects.load(Ordering::SeqCst),
            "candidate_cap":sinomo_network::MAX_ROUTE_CANDIDATES,
            "search_bound_qualification":"production routing.rs unmodified, existing4096 bound; not a measured search-state counter",
            "original_budget_ms":sinomo_network::DEFAULT_NETWORK_TIMEOUT.as_millis(),
            "native_semantic_baseline":baseline.records,
            "stall_origin_limit":"In live-shortcut case a concurrent direct source read can share A-to-D transport identity; old zero source deltas disambiguate the original target dial. Do not call every new stalled connection an original target probe.",
        });
        std::fs::write(data[0].root().join("stalled-first-carrier-before-teardown.json"),
            serde_json::to_vec_pretty(&facts).unwrap()).unwrap();
    }
    // Cleanup also runs on the expected failed-before result.
    for scope in
        sinomo_network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&state)
            .unwrap()
    {
        sinomo_network::retire_pooled_routed_streams(&scope).await;
    }
    for endpoint in endpoints.iter().chain(&extra_endpoints) {
        let i = nodes
            .iter()
            .position(|node| node == &endpoint.node_id)
            .unwrap();
        let mut hash = blake3::Hasher::new();
        for value in [
            endpoint.endpoint_id.as_str(),
            endpoint.address.as_str(),
            keys[i].public_key.as_str(),
        ] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        hash.update(format!("{:?}", endpoint.transport).as_bytes());
        sinomo_network::retire_pooled_routed_streams(&format!(
            "{}:direct-endpoint:{}",
            cluster,
            hash.finalize().to_hex()
        ))
        .await;
    }
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    stalled_servers.abort_all();
    while stalled_servers.join_next().await.is_some() {}
    assert!(stalled_connections.load(Ordering::SeqCst) <= 16);
    if case != SourceCase::SemanticChange {
        assert_eq!(
            std::fs::read(&paths.kv_paths().snapshot).unwrap(),
            original_snapshot
        );
    }
    assert!(!paths.connectivity_local.exists());
    assert!(!sinomo_kv::evict_resident_raft_node_for_test(
        &paths.kv_paths().raft_wal_paths(&nodes[0])
    ));
    assert!(sinomo_kv::clear_process_local_raft_access_denial_for_test(
        &paths.kv_paths().raft
    ));
    eprintln!(
        "source_reads={:?} original_effects={} blocked_direct={}",
        reads
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        effects.load(Ordering::SeqCst),
        rejected_direct.load(Ordering::SeqCst)
    );
    if case == SourceCase::DegradedShortcut {
        let facts = serde_json::json!({
            "qualification":"controlled routing DATA; genuine native TCP/prelude/session/relay/RPC. No Main/Raft/DFS publication proof; no attribution to C17a RAM",
            "calibration":calibration,"reads_before_original":reads_before_original,
            "reads_after_original":reads.iter().map(|n|n.load(Ordering::SeqCst)).collect::<Vec<_>>(),
            "shortcut_refusals_before_original":refusals_before_original,
            "shortcut_refusals_after_original":shortcut_refusals.load(Ordering::SeqCst),
            "first_poststart_C_source_read_refusal_count":first_bridge_read_refusals.load(Ordering::SeqCst),
            "original_elapsed_us":original_elapsed.as_micros(),
            "calibration_elapsed_us":calibration_elapsed.as_micros(),
            "remaining_original_budget_us":sinomo_network::DEFAULT_NETWORK_TIMEOUT.saturating_sub(original_elapsed).as_micros(),
            "actual_original_response":format!("{response:#?}"),
            "actual_test_service_effect_count":effects.load(Ordering::SeqCst),
            "native_semantic_baseline":baseline.records,
            "candidate_cap":sinomo_network::MAX_ROUTE_CANDIDATES,"original_budget_ms":sinomo_network::DEFAULT_NETWORK_TIMEOUT.as_millis(),
        });
        std::fs::write(
            data[0]
                .root()
                .join("degraded-shortcut-source-discovery.json"),
            serde_json::to_vec_pretty(&facts).unwrap(),
        )
        .unwrap();
        assert!(
            calibration.is_ok(),
            "SETUP UNREACHED: actual same-fixture healthy bridge/source calibration failed: {calibration:?}"
        );
        assert_eq!(
            refusals_before_original, 0,
            "calibration must not consume the shortcut refusal premise"
        );
        assert!(
            shortcut_refusals.load(Ordering::SeqCst) > refusals_before_original,
            "UNREACHED: the Original must genuinely attempt/refuse the degraded shortcut before source continuation"
        );
        assert!(
            original_elapsed < sinomo_network::DEFAULT_NETWORK_TIMEOUT,
            "UNREACHED: actual refusal exhausted the unchanged Original5s budget"
        );
        if response.as_ref().unwrap().is_err() {
            assert!(
                sinomo_network::DEFAULT_NETWORK_TIMEOUT.saturating_sub(original_elapsed)
                    > calibration_elapsed,
                "UNREACHED: refusal did not leave even the measured same-fixture calibration duration within the Original budget"
            );
            assert_eq!(
                reads[2].load(Ordering::SeqCst),
                reads_before_original[2],
                "UNREACHED: failure occurred after C was consulted; not the skipped-source gap"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        } else {
            assert!(reads[2].load(Ordering::SeqCst) > reads_before_original[2]);
            assert!(
                first_bridge_read_refusals.load(Ordering::SeqCst) > 0
                    && first_bridge_read_refusals.load(Ordering::SeqCst) != usize::MAX,
                "source continuation must follow the real refusal, not replace existing preference policy"
            );
        }
    }
    if case.stalled() {
        assert!(calibration.is_ok(), "SETUP UNREACHED: real same-fixture healthy-source/full-chain calibration failed: {calibration:?}");
        assert_eq!(refusals_before_original, 0, "calibration cannot consume a shortcut refusal");
        if response.as_ref().unwrap().is_err() {
            assert!(stalled_handshakes.load(Ordering::SeqCst) > 0,
                "UNREACHED: old failed call must actually attempt/hold authenticated shortcut");
            assert!(stalled_handshake_bytes.load(Ordering::SeqCst) > 0,
                "UNREACHED: actual routed-session input must reach the old held stream");
            assert_eq!(reads[2].load(Ordering::SeqCst), reads_before_original[2],
                "UNREACHED: failure already consulted C, not skipped-source scheduling");
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            // Candidate/relay wire deadlines may finish before the outer
            // timeout owner. Both are typed pre-service exhaustion; this is
            // a fixture oracle, not production source-refresh eligibility.
            let terminal_pre_service = matches!(response.as_ref().unwrap(),
                Err(sinomo_network::NetworkError::Timeout {
                    delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent, ..
                })) || matches!(response.as_ref().unwrap(),
                Err(sinomo_network::NetworkError::RetryExhausted {
                    attempts, delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent, ..
                }) if *attempts > 0);
            assert!(terminal_pre_service,
                "UNREACHED: not actual typed pre-service exhaustion: {response:?}");
            assert!(sinomo_network::DEFAULT_NETWORK_TIMEOUT.saturating_sub(original_elapsed)
                    <= calibration_elapsed,
                "UNREACHED: stalled first carrier left more budget than the measured healthy calibration");
            if case == SourceCase::StalledLiveShortcut {
                assert_eq!(reads[1].load(Ordering::SeqCst), reads_before_original[1],
                    "UNREACHED: the live-hint old control unexpectedly read B");
            } else {
                assert!(reads[1].load(Ordering::SeqCst) > reads_before_original[1],
                    "UNREACHED: the actual initial B source did not supply its route hint");
            }
        } else {
            assert!(reads[2].load(Ordering::SeqCst) > reads_before_original[2]);
            // Success may preempt a losing probe before remote authentication.
            // Remote EOF/error is diagnostic only; relay handshake lifetime is
            // separately bounded by the original wire deadline.
        }
    }
    let response = response.expect("SETUP UNREACHED: calibration failed before the Original call");
    if case.canonical_bridge() {
        let facts = serde_json::json!({
            "qualification":"real four-node TCP/prelude/session/relay/RPC with existing endpoint-observation coordinator/source store; no Main/Raft/DFS or C20 offered-row attribution",
            "case":format!("{case:?}"),"actual_calibration":calibration,
            "calibration_elapsed_us":calibration_elapsed.as_micros(),
            "native_source_observer_rows":*native_source_rows.lock().unwrap(),
            "actual_original_B_reports":bridge_reports.lock().unwrap().clone(),
            "caller_fenced_membership_data":state.members,
            "canonical_baseline":baseline.records,"missing_only":case.canonical_bridge_missing(),
            "actual_original_response":format!("{response:#?}"),"actual_original_elapsed_us":original_elapsed.as_micros(),
            "reads_before_original":reads_before_original,"reads_after_original":reads.iter().map(|x|x.load(Ordering::SeqCst)).collect::<Vec<_>>(),
            "single_original_effects":effects.load(Ordering::SeqCst),"original_budget_ms":5000,
            "candidate_cap":sinomo_network::MAX_ROUTE_CANDIDATES,"source_inflight_cap":16,"record_search_cap":4096,
            "search_cap_limit":"production unchanged; not measured search-state count",
        });
        let export = std::fs::write(data[0].root().join("missing-canonical-bridge.json"),
            serde_json::to_vec_pretty(&facts).unwrap());
        if let Err(error)=&export { eprintln!("bridge evidence export failed: {error}"); }
        assert!(calibration.is_ok(), "SETUP: same real source/full-chain calibration failed: {calibration:?}");
        assert!(reads[1].load(Ordering::SeqCst) > reads_before_original[1], "SETUP: genuine B source report not consulted");
        if response.is_err() {
            assert_eq!(effects.load(Ordering::SeqCst), 0, "failed Original cannot reach service effect");
            assert_eq!(reads[2].load(Ordering::SeqCst), reads_before_original[2],
                "SETUP: a failure after C was consulted is not the missing B bridge gap");
            let pre_service = matches!(&response,
                Err(sinomo_network::NetworkError::Timeout { delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent, .. }))
                || matches!(&response, Err(sinomo_network::NetworkError::RetryExhausted {
                    attempts, delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent, .. }) if *attempts > 0)
                // Missing executable paths can exhaust before any target
                // carrier attempt. Preserve the exact observed typed fate.
                || matches!(&response, Err(sinomo_network::NetworkError::RetryExhausted {
                    operation, attempts: 0,
                    delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent, detail,
                }) if operation == &format!("establish routed session to {}", nodes[3])
                    && detail == "no executable path in the current snapshots");
            assert!(pre_service, "SETUP: actual failed call must be typed pre-service exhaustion: {response:?}");
        }
        let reported = bridge_reports.lock().unwrap();
        assert!(!reported.is_empty(), "SETUP: Original received no actual B source report");
        for report in reported.iter() {
            assert_eq!(report.len(), 1);
            let row = &report[0];
            match case {
                SourceCase::BridgeWrongSource => assert_ne!(row.source_node_id, nodes[1]),
                SourceCase::BridgeWrongCluster => assert_ne!(row.cluster_id, cluster),
                SourceCase::BridgeUnknownMember => assert!(!state.members.iter().any(|m| m.node_id == row.target_node_id)),
                SourceCase::BridgeEndpointMismatch => assert_ne!(row.endpoint_id, endpoints[1].endpoint_id),
                SourceCase::BridgeFutureClock => assert!(row.observed_at > OffsetDateTime::now_utc()),
                SourceCase::BridgeExpiredClock => assert!(row.expires_at <= OffsetDateTime::now_utc()),
                SourceCase::BridgeIdentityReason => { assert_eq!(row.state, sinomo_network::ClusterConnectivityState::Healthy); assert_eq!(row.failure_reason, Some(sinomo_network::ClusterConnectivityFailureReason::IdentityMismatch)); }
                SourceCase::BridgeAuthReason => { assert_eq!(row.state, sinomo_network::ClusterConnectivityState::Healthy); assert_eq!(row.failure_reason, Some(sinomo_network::ClusterConnectivityFailureReason::CommandAuthDenied)); }
                _ => {},
            }
        }
        if case.canonical_bridge_succeeds() {
            assert_eq!(response.as_ref().unwrap(), "original response");
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert!(reads[2].load(Ordering::SeqCst) > reads_before_original[2], "C's own report required");
        } else {
            assert!(response.is_err(), "invalid source evidence cannot dispatch Original");
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
        export.expect("bridge evidence retained after actual semantics");
        return;
    }
    match case {
        SourceCase::Success
        | SourceCase::PartiallyLiveMain
        | SourceCase::SaturatedHints
        | SourceCase::ColdSourceProcess
        | SourceCase::ColdRoutedSourceProcess
        | SourceCase::LiveFailureCountReset
        | SourceCase::DegradedShortcut
        | SourceCase::StalledLiveShortcut
        | SourceCase::StalledSourceHint
        | SourceCase::HealthyNoSources | SourceCase::HealthyUnrelatedSource
        | SourceCase::HealthyFailedSource | SourceCase::HealthySlowSource => {
            assert_eq!(response.unwrap(), "original response");
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
        SourceCase::SemanticChange => {
            assert!(
                matches!(
                    response,
                    Err(sinomo_network::NetworkError::Directory { .. })
                ),
                "{response:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
        SourceCase::Deadline => {
            assert!(
                matches!(
                    response,
                    Err(sinomo_network::NetworkError::Timeout {
                        delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
                        ..
                    })
                ),
                "{response:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
        SourceCase::CanonicalBridgeCalibrated | SourceCase::MissingCanonicalBridge
        | SourceCase::BridgeWrongSource | SourceCase::BridgeWrongCluster | SourceCase::BridgeUnknownMember
        | SourceCase::BridgeEndpointMismatch | SourceCase::BridgeFutureClock | SourceCase::BridgeExpiredClock
        | SourceCase::BridgeCanonicalFailure | SourceCase::BridgeCanonicalQuarantine
        | SourceCase::BridgeIdentityReason | SourceCase::BridgeAuthReason
        | SourceCase::BridgePendingTarget | SourceCase::BridgeRemovedTarget => unreachable!("handled source-owner bridge controls"),
    }
    if case.healthy_guard() {
        assert!(original_elapsed < sinomo_network::DEFAULT_NETWORK_TIMEOUT);
        let held_binding = *first_binding.lock().unwrap();
        assert!(held_binding.is_some(), "UNREACHED: initial authenticated A-D socket was not held");
        assert_eq!(*original_binding.lock().unwrap(), held_binding,
            "Original must use SAME physical socket held before native routed acceptance");
        match case {
            SourceCase::HealthyNoSources => {
                assert!(baseline_before_hot > 0, "UNREACHED: actual advisory capture did not fail");
                assert_eq!(reads_before_hot[1], reads_before_original[1]);
                assert_eq!(reads_before_hot[2], reads_before_original[2]);
                assert!(hot_response.unwrap().is_ok(), "hot read-only target RPC must succeed");
                assert_eq!(*hot_binding.lock().unwrap(), held_binding,
                    "marked hot RPC must reuse SAME actual native physical carrier as Original");
                assert_eq!(baseline_failures.load(Ordering::SeqCst), baseline_before_hot,
                    "hot carrier must leave establishment future unpolled");
                assert_eq!(reads[1].load(Ordering::SeqCst), reads_before_hot[1]);
                assert_eq!(reads[2].load(Ordering::SeqCst), reads_before_hot[2]);
                assert_eq!(reads[3].load(Ordering::SeqCst), reads_before_hot[3] + 1);
            }
            SourceCase::HealthyUnrelatedSource => {
                assert!(reads[1].load(Ordering::SeqCst) > reads_before_original[1]);
                assert!(reads[2].load(Ordering::SeqCst) > reads_before_original[2],
                    "UNREACHED: C source over actual B relay did not precede target release");
            }
            SourceCase::HealthyFailedSource => assert!(failed_source_calls.load(Ordering::SeqCst) > 0),
            SourceCase::HealthySlowSource => assert!(slow_source_calls.load(Ordering::SeqCst) > 0),
            _ => unreachable!(),
        }
    } else if case.calibrated() {
        assert!(reads[2].load(Ordering::SeqCst) > reads_before_original[2]);
    } else if case == SourceCase::ColdRoutedSourceProcess {
        assert!(
            (1..=2).contains(&reads[2].load(Ordering::SeqCst)),
            "C's genuine fresh process must reply; each routed endpoint is read at most once"
        );
    } else {
        assert_eq!(
            reads[2].load(Ordering::SeqCst),
            1,
            "C's own authenticated reply is required; B cannot renew C's edge"
        );
    }
    if case != SourceCase::SaturatedHints && !case.healthy_guard() {
        assert!(rejected_direct.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_reads_cross_two_bridges_without_trusting_third_party_rows() {
    multihop_source_scenario(SourceCase::Success).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multihop_source_semantic_change_prevents_original_dispatch() {
    multihop_source_scenario(SourceCase::SemanticChange).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multihop_source_deadline_preserves_original_delivery_and_drains() {
    multihop_source_scenario(SourceCase::Deadline).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partially_live_graph_refreshes_remote_bridges_for_main_destination() {
    multihop_source_scenario(SourceCase::PartiallyLiveMain).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn earlier_stalled_hints_cannot_starve_known_adjacent_source_read() {
    multihop_source_scenario(SourceCase::SaturatedHints).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_process_does_not_hide_other_endpoint_live_observation() {
    multihop_source_scenario(SourceCase::ColdSourceProcess).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_routes_with_live_failure_count_reset_on_remote_bridge() {
    multihop_source_scenario(SourceCase::LiveFailureCountReset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_routed_source_process_does_not_hide_other_endpoint_live_observation() {
    multihop_source_scenario(SourceCase::ColdRoutedSourceProcess).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_degraded_shortcut_continues_to_the_available_authenticated_bridge_source() {
    multihop_source_scenario(SourceCase::DegradedShortcut).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_live_target_carrier_does_not_hide_available_authenticated_sources() {
    multihop_source_scenario(SourceCase::StalledLiveShortcut).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_target_after_initial_source_hint_does_not_hide_next_bridge_source() {
    multihop_source_scenario(SourceCase::StalledSourceHint).await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_first_carrier_survives_advisory_capture_error_and_hot_reuse_skips_sources() {
    multihop_source_scenario(SourceCase::HealthyNoSources).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_first_carrier_keeps_same_physical_socket_across_unrelated_source_progress() {
    multihop_source_scenario(SourceCase::HealthyUnrelatedSource).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_first_carrier_survives_actual_source_response_failure() {
    multihop_source_scenario(SourceCase::HealthyFailedSource).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_first_carrier_does_not_wait_for_actual_slow_source_response() {
    multihop_source_scenario(SourceCase::HealthySlowSource).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_bridge_present_control_uses_real_source_observer() {
    multihop_source_scenario(SourceCase::CanonicalBridgeCalibrated).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_canonical_bridge_uses_actual_authenticated_source_owner() {
    multihop_source_scenario(SourceCase::MissingCanonicalBridge).await;
}

#[path = "multihop_freshness_tests/canonical_bridge_security.rs"]
mod canonical_bridge_security;
