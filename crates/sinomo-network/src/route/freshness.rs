//! Demand-owned authenticated source reads during carrier establishment.
//! This module owns no timer, task registry, durable store or membership.

use super::*;
use crate::{ClusterConnectivityRecord, ClusterConnectivityState, DirectorySnapshot};
use futures_util::StreamExt;

const MAX_SOURCE_READS_IN_FLIGHT: usize = 16;

struct SourceReadHost;
impl ClusterObjectRpcAsyncHost for SourceReadHost {}

fn matching_source_records(
    directory: &DirectorySnapshot,
    state: &ClusterBootstrapState,
    source: &NodeId,
    baseline: &[ClusterConnectivityRecord],
    incoming: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    if incoming.len() > crate::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
            observed_records: incoming.len(),
            max_records: crate::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
        });
    }
    let mut records = Vec::new();
    for live in incoming {
        if &live.source_node_id != source
            || live.cluster_id != state.cluster_id
            || !crate::cluster_network_record_live(live.expires_at, now)
            || live.observed_at > now
        {
            continue;
        }
        let bound = directory.node(&live.target_node_id).is_some_and(|target| {
            target.endpoints.iter().any(|endpoint| {
                endpoint.endpoint_id == live.endpoint_id
                    && endpoint.transport == live.transport
                    && endpoint.address == live.address
            })
        });
        if !directory.is_authorized(source) || !bound {
            continue;
        }
        if baseline.iter().any(|canonical| {
            crate::cluster_connectivity_stable_eq(canonical, &live)
                && canonical.consecutive_failures == live.consecutive_failures
                && live.observed_at >= canonical.observed_at
        }) {
            records.push(live);
        }
    }
    crate::validate_cluster_connectivity_records_for_source(source, &records)?;
    Ok(records)
}

fn merge_observations(
    records: &mut Vec<ClusterConnectivityRecord>,
    incoming: Vec<ClusterConnectivityRecord>,
) {
    for live in incoming {
        let old = records.iter().position(|record| {
            record.source_node_id == live.source_node_id
                && record.target_node_id == live.target_node_id
                && record.endpoint_id == live.endpoint_id
        });
        match old {
            Some(index) if records[index].observed_at < live.observed_at => records[index] = live,
            None => records.push(live),
            _ => {}
        }
    }
}

enum SourceRead {
    Direct(ClusterEndpointRecord),
    Routed(NodeId, Arc<crate::MultiHopRoutedConnector>),
}
impl SourceRead {
    fn source(&self) -> &NodeId {
        match self {
            Self::Direct(endpoint) => &endpoint.node_id,
            Self::Routed(node, _) => node,
        }
    }
}

async fn read_source(
    origin: SourceRead,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterObjectRpcOptions,
    deadline: tokio::time::Instant,
    route_context: (RouteDirectoryCacheIdentity, u64, u64),
) -> (SourceRead, Option<Vec<ClusterConnectivityRecord>>) {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return (origin, None);
    }
    let read = async {
        let call = ObjectRpcCall::encode_value(
            crate::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
            crate::CLUSTER_NETWORK_RPC_CATEGORY,
            (),
        )?;
        let response = match &origin {
            SourceRead::Direct(endpoint) => {
                let mut host = SourceReadHost;
                ClusterObjectRpcClient::routed(&mut host, paths)
                    .call_endpoint(
                        state,
                        endpoint,
                        call,
                        options.with_overall_timeout(remaining),
                    )
                    .await?
            }
            SourceRead::Routed(source, connector) => {
                // Reuse the ordinary authenticated carrier and exact request
                // authority scope. Never recurse into freshness collection.
                let constraints = stream_native_service_route_constraints();
                let affinity = crate::DestinationProcessAffinity::Any;
                let (identity, module_generation, directory_generation) = route_context;
                let stream = crate::routed_stream_pool::open_pooled_routed_stream_with_protection_destination_affinity_and_route_compatibility(
                    identity.routed_stream_scope(&state.cluster_id), module_generation, directory_generation,
                    constraints, affinity, state.local_node_id.clone(), source.clone(), options.protection, remaining,
                    connector.connect_with_destination_process_affinity(source.clone(), crate::ConnectOptions {
                        constraints, timeout: remaining, protection: options.protection,
                    }, affinity),
                ).await?;
                crate::service_rpc_stream::call_service_rpc_peer_stream(
                    stream,
                    call,
                    crate::ObjectFrameIoOptions::idle_timeout(
                        options.call_policy().health.no_progress_timeout,
                    ),
                )
                .await?
            }
        };
        response.response.error_detail()?;
        response
            .decode_body::<Vec<ClusterConnectivityRecord>>()
            .await
    };
    let response = tokio::time::timeout_at(deadline, read)
        .await
        .ok()
        .and_then(Result::ok);
    (origin, response)
}

pub(super) async fn refresh_connector(
    paths: &ClusterPaths,
    directory: &Arc<dyn crate::ClusterDirectory>,
    state: &ClusterBootstrapState,
    target: &NodeId,
    options: ClusterObjectRpcOptions,
    deadline: tokio::time::Instant,
    route_context: (RouteDirectoryCacheIdentity, u64, u64),
) -> Result<Option<Arc<crate::MultiHopRoutedConnector>>, NetworkError> {
    let baseline = directory.connectivity_freshness_baseline()?;
    let (version, members, endpoints, ordinary_records) =
        directory.routing_records_snapshot()?.into_parts();
    let snapshot = DirectorySnapshot::try_from_records(version, members, endpoints)?;
    if !snapshot.is_authorized(&state.local_node_id) || !snapshot.is_authorized(target) {
        return Ok(None);
    }
    let mut records =
        crate::route_graph_read_model::read_cluster_connectivity_graph_for_routing_snapshot(
            paths,
            state,
            &snapshot,
            ordinary_records,
        )?;
    let mut candidates = Vec::new();
    for node in snapshot.nodes().values() {
        if node.node_id == state.local_node_id
            || !state.members.iter().any(|member| {
                member.node_id == node.node_id
                    && member.node_public_key == node.public_key
                    && member.state.is_authorized()
            })
        {
            continue;
        }
        for endpoint in &node.endpoints {
            // Preserve existing live identity-mismatch quarantine. An expired
            // row may be a hint, never an authority grant or synthetic edge.
            if records.iter().any(|record| {
                record.source_node_id == state.local_node_id
                    && record.target_node_id == node.node_id
                    && record.endpoint_id == endpoint.endpoint_id
                    && record.failure_reason
                        == Some(crate::ClusterConnectivityFailureReason::IdentityMismatch)
            }) {
                continue;
            }
            crate::object::try_reserve_vec(&mut candidates, 1, "collect live source endpoints")?;
            candidates.push(endpoint.clone());
        }
    }
    if candidates.len() > crate::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
            observed_records: candidates.len(),
            max_records: crate::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
        });
    }
    let mut direct = std::collections::VecDeque::from(candidates);
    let mut reads = futures_util::stream::FuturesUnordered::new();
    let mut attempted_sources = std::collections::BTreeSet::new();
    let mut refreshed = false;
    let mut authenticated = std::collections::BTreeSet::new();
    let mut connector = connector_from_observed_records(
        paths,
        state,
        snapshot.clone(),
        records.clone(),
        options.call_policy().health,
        None,
    )?;
    loop {
        if connector.has_live_observed_route(target, options.destination_process_affinity) {
            break;
        }
        // Prioritize newly reachable sources over more direct hints. A pending
        // unreachable endpoint cannot consume the entire original deadline
        // before a second bridge's actual source can be queried.
        for node in snapshot.nodes().values() {
            if reads.len() == MAX_SOURCE_READS_IN_FLIGHT {
                break;
            }
            if node.node_id == state.local_node_id
                || &node.node_id == target
                || attempted_sources.contains(&node.node_id)
                || !state.members.iter().any(|member| {
                    member.node_id == node.node_id
                        && member.node_public_key == node.public_key
                        && member.state.is_authorized()
                })
                || !connector
                    .has_live_observed_route(&node.node_id, crate::DestinationProcessAffinity::Any)
            {
                continue;
            }
            attempted_sources.insert(node.node_id.clone());
            reads.push(read_source(
                SourceRead::Routed(node.node_id.clone(), connector.clone()),
                paths,
                state,
                options,
                deadline,
                route_context,
            ));
        }
        while reads.len() < MAX_SOURCE_READS_IN_FLIGHT {
            let Some(endpoint) = direct.pop_front() else {
                break;
            };
            if attempted_sources.contains(&endpoint.node_id) {
                continue;
            }
            reads.push(read_source(
                SourceRead::Direct(endpoint),
                paths,
                state,
                options,
                deadline,
                route_context,
            ));
        }
        let Some((origin, response)) = reads.next().await else {
            break;
        };
        let Some(response) = response else {
            continue;
        };
        let now = OffsetDateTime::now_utc();
        let mut owned = matching_source_records(
            &snapshot,
            state,
            origin.source(),
            &baseline.records,
            response,
            now,
        )?;
        attempted_sources.insert(origin.source().clone());
        if let SourceRead::Direct(endpoint) = origin {
            // Only a successful direct RPC supplies a local adjacent edge.
            // A routed reply never fabricates local-to-source connectivity.
            owned.push(crate::connectivity::cluster_connectivity_record(
                state,
                &endpoint,
                ClusterConnectivityState::Healthy,
                None,
                0,
                None,
                None,
                now,
                crate::CLUSTER_CONNECTIVITY_TTL_SECONDS,
            ));
        }
        for record in &owned {
            authenticated.insert((
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            ));
        }
        merge_observations(&mut records, owned);
        refreshed = true;
        connector = connector_from_observed_records(
            paths,
            state,
            snapshot.clone(),
            records.clone(),
            options.call_policy().health,
            Some(&authenticated),
        )?;
    }
    // Drop outstanding read futures on success, cancellation or deadline. No
    // detached helper survives this request-owned establishment phase.
    drop(reads);
    if !refreshed {
        return Ok(None);
    }
    if crate::network_module_routing_generation() != route_context.1
        || directory.routing_version()? != version
        || directory.connectivity_freshness_baseline()? != baseline
    {
        return Err(NetworkError::Directory {
            detail: "connectivity baseline changed during authenticated source read".into(),
        });
    }
    let connector = connector_from_observed_records(
        paths,
        state,
        snapshot,
        records,
        options.call_policy().health,
        Some(&authenticated),
    )?;
    Ok(Some(connector))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{ClusterMemberRecord, ClusterTransportKind};

    fn fixture() -> (
        DirectorySnapshot,
        ClusterBootstrapState,
        NodeId,
        ClusterConnectivityRecord,
        OffsetDateTime,
    ) {
        let now = OffsetDateTime::now_utc();
        let cluster = ClusterId::from_legacy_named("source_freshness").unwrap();
        let local = NodeId::from_legacy_named("source_freshness_local").unwrap();
        let source = NodeId::from_legacy_named("source_freshness_peer").unwrap();
        let target = NodeId::from_legacy_named("source_freshness_target").unwrap();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster.clone(),
            "freshness",
            local,
            "local",
            "pk-local",
            now,
        );
        state.members.push(ClusterMemberRecord::local_non_voting(
            cluster.clone(),
            source.clone(),
            "source",
            "pk-source",
            now,
        ));
        let mut member = ClusterMemberRecord::local_non_voting(
            cluster,
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target,
            "127.0.0.1:21353".into(),
            0,
        );
        member.endpoints.push(endpoint.clone());
        state.members.push(member);
        let directory = DirectorySnapshot::try_from_records(
            crate::DirectoryVersion(7),
            state.members.clone(),
            vec![endpoint.clone()],
        )
        .unwrap();
        let mut baseline = crate::connectivity::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            None,
            0,
            None,
            None,
            now - time::Duration::hours(1),
            crate::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        baseline.source_node_id = source.clone();
        (directory, state, source, baseline, now)
    }

    #[test]
    fn expired_baseline_requires_new_authenticated_own_source_statement() {
        let (directory, state, source, baseline, now) = fixture();
        let mut live = baseline.clone();
        live.observed_at = now;
        live.expires_at = now + time::Duration::seconds(crate::CLUSTER_CONNECTIVITY_TTL_SECONDS);
        let accepted = matching_source_records(
            &directory,
            &state,
            &source,
            std::slice::from_ref(&baseline),
            vec![live.clone()],
            now,
        )
        .unwrap();
        assert_eq!(accepted, vec![live.clone()]);
        assert!(!crate::cluster_network_record_live(
            baseline.expires_at,
            now
        ));
        for change in 0..8 {
            let mut rejected = live.clone();
            match change {
                0 => rejected.source_node_id = state.local_node_id.clone(),
                1 => rejected.cluster_id = ClusterId::from_legacy_named("wrong_cluster").unwrap(),
                2 => rejected.state = ClusterConnectivityState::Degraded,
                3 => rejected.consecutive_failures = 1,
                4 => rejected.address = "127.0.0.1:39999".into(),
                5 => rejected.observed_at = baseline.observed_at - time::Duration::seconds(1),
                6 => rejected.expires_at = now,
                _ => rejected.transport = ClusterTransportKind::Loopback,
            }
            assert!(
                matching_source_records(
                    &directory,
                    &state,
                    &source,
                    std::slice::from_ref(&baseline),
                    vec![rejected],
                    now
                )
                .unwrap()
                .is_empty(),
                "case {change}"
            );
        }
        assert!(
            matching_source_records(&directory, &state, &source, &[], vec![live], now)
                .unwrap()
                .is_empty(),
            "live claims cannot introduce uncommitted remote semantics"
        );
    }

    #[test]
    fn source_payload_limit_is_rejection_not_partial_acceptance() {
        let (directory, state, source, baseline, now) = fixture();
        let incoming = vec![baseline.clone(); crate::MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS + 1];
        assert!(matches!(
            matching_source_records(&directory, &state, &source, &[baseline], incoming, now),
            Err(NetworkError::ClusterConnectivitySnapshotRecordLimit { .. })
        ));
    }
}
