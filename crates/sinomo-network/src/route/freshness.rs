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
                endpoint.privacy != sinomo_base::ClusterEndpointPrivacy::LocalOnly
                    && endpoint.endpoint_id == live.endpoint_id
                    && endpoint.transport == live.transport
                    && endpoint.address == live.address
            })
        });
        if !directory.is_authorized(source) || !bound {
            continue;
        }
        // The source's failure count can advance within the same confirmed
        // health state. Quorum publication coalesces those count-only changes;
        // use its semantic predicate while retaining the actual live count
        // for route ordering and the source's existing failure-budget policy.
        if baseline.iter().any(|canonical| {
            crate::cluster_connectivity_stable_eq(canonical, &live)
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
) -> bool {
    let mut changed = false;
    for live in incoming {
        let old = records.iter().position(|record| {
            record.source_node_id == live.source_node_id
                && record.target_node_id == live.target_node_id
                && record.endpoint_id == live.endpoint_id
        });
        match old {
            Some(index) if records[index].observed_at < live.observed_at => {
                records[index] = live;
                changed = true;
            }
            None => {
                records.push(live);
                changed = true;
            }
            _ => {}
        }
    }
    changed
}

enum SourceRead {
    Direct(ClusterEndpointRecord),
    Routed(ClusterEndpointRecord, Arc<crate::MultiHopRoutedConnector>),
}
impl SourceRead {
    fn endpoint(&self) -> &ClusterEndpointRecord {
        match self {
            Self::Direct(endpoint) | Self::Routed(endpoint, _) => endpoint,
        }
    }

    fn source(&self) -> &NodeId {
        &self.endpoint().node_id
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
            SourceRead::Routed(endpoint, connector) => {
                let source = &endpoint.node_id;
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
                    Some(endpoint.endpoint_id.clone()),
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

fn direct_source_endpoint_candidates(
    snapshot: &DirectorySnapshot,
    state: &ClusterBootstrapState,
    records: &[ClusterConnectivityRecord],
    baseline: &[ClusterConnectivityRecord],
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
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
            // Bootstrap views may retain the owner's local listener hints.
            // Dialing those for a remote source would hit this caller's own
            // machine and must never create a cross-node observation.
            if endpoint.privacy == sinomo_base::ClusterEndpointPrivacy::LocalOnly {
                continue;
            }
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
    // These rows only order authenticated dials. Even expired source-owned
    // evidence can identify the adjacent bridge worth trying before unrelated
    // hints; it never becomes a live edge until a new authenticated reply.
    // Prefer newer accepted local observations over the older canonical row.
    candidates.sort_by_cached_key(|endpoint| {
        let observed = records
            .iter()
            .chain(baseline)
            .filter(|record| {
                record.source_node_id == state.local_node_id
                    && record.target_node_id == endpoint.node_id
                    && record.endpoint_id == endpoint.endpoint_id
                    && record.transport == endpoint.transport
                    && record.address == endpoint.address
            })
            .max_by_key(|record| record.observed_at);
        let rank = match observed.map(|record| &record.state) {
            Some(ClusterConnectivityState::Healthy) => 0,
            Some(ClusterConnectivityState::Degraded) => 1,
            None => 2,
            Some(ClusterConnectivityState::Unreachable) => 3,
        };
        (
            rank,
            endpoint.priority,
            endpoint.node_id.clone(),
            endpoint.endpoint_id.clone(),
        )
    });
    Ok(candidates)
}

fn validate_target_context(
    directory: &Arc<dyn crate::ClusterDirectory>,
    route_context: (RouteDirectoryCacheIdentity, u64, u64),
    dispatcher: &Option<sinomo_base::Id>,
    local: &NodeId,
) -> Result<(), NetworkError> {
    if crate::network_module_routing_generation() != route_context.1
        || route_context
            .0
            .effective_generation(directory.routing_version()?.0)
            != route_context.2
        || crate::routed_stream_pool::registered_routed_stream_incoming_token(local).as_ref()
            != dispatcher.as_ref()
    {
        return Err(NetworkError::Directory {
            detail: "original route context changed during carrier establishment".into(),
        });
    }
    Ok(())
}

fn validate_refresh_inputs(
    directory: &Arc<dyn crate::ClusterDirectory>,
    version: crate::DirectoryVersion,
    baseline: &crate::DirectoryConnectivityBaseline,
    route_context: (RouteDirectoryCacheIdentity, u64, u64),
    dispatcher: &Option<sinomo_base::Id>,
    local: &NodeId,
) -> Result<(), NetworkError> {
    validate_target_context(directory, route_context, dispatcher, local)?;
    if directory.routing_version()? != version
        || directory.connectivity_freshness_baseline()? != *baseline
    {
        return Err(NetworkError::Directory {
            detail: "connectivity baseline changed during authenticated source read".into(),
        });
    }
    Ok(())
}

fn same_preferred_transport(
    left: &[crate::routing::CandidatePath],
    right: &[crate::routing::CandidatePath],
) -> bool {
    match (left.first(), right.first()) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.nodes == right.nodes
                && left.hops.len() == right.hops.len()
                && left.hops.iter().zip(&right.hops).all(|(left, right)| {
                    left.source_node_id == right.source_node_id
                        && left.target_node_id == right.target_node_id
                        && left.endpoint == right.endpoint
                        && left.capabilities == right.capabilities
                })
        }
        _ => false,
    }
}

type CarrierProbe = Pin<
    Box<
        dyn Future<Output = Result<crate::PeerStream, crate::routing::CarrierEstablishmentFailure>>
            + Send,
    >,
>;

fn carrier_probe(
    connector: Arc<crate::MultiHopRoutedConnector>,
    target: NodeId,
    options: ClusterObjectRpcOptions,
    remaining: Duration,
) -> CarrierProbe {
    Box::pin(async move {
        connector
            .connect_with_source_freshness_observation(
                target,
                crate::ConnectOptions {
                    constraints: stream_native_service_route_constraints(),
                    timeout: remaining,
                    protection: options.protection,
                },
                options.destination_process_affinity,
            )
            .await
    })
}

/// Establish one actual carrier while the existing read-only source owner
/// discovers eligible paths. No application request/body enters this function.
pub(super) async fn connect_with_sources(
    paths: &ClusterPaths,
    directory: &Arc<dyn crate::ClusterDirectory>,
    state: &ClusterBootstrapState,
    target: &NodeId,
    options: ClusterObjectRpcOptions,
    deadline: tokio::time::Instant,
    original_timeout: Duration,
    route_context: (RouteDirectoryCacheIdentity, u64, u64),
    dispatcher: &Option<sinomo_base::Id>,
    initial: Arc<crate::MultiHopRoutedConnector>,
) -> Result<crate::PeerStream, NetworkError> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::timeout(
            "establish routed service RPC stream",
            NetworkTimeoutPhase::Connect,
            original_timeout,
            NetworkDeliveryState::DefinitelyNotSent,
        ));
    }
    let mut target_plan = initial.candidate_routes(
        target,
        stream_native_service_route_constraints(),
        options.destination_process_affinity,
    );
    let mut probe = if target_plan.is_empty() {
        None
    } else {
        Some(carrier_probe(
            Arc::clone(&initial),
            target.clone(),
            options,
            remaining,
        ))
    };
    let mut first_error = None;
    // This happens BEFORE any advisory baseline/snapshot/candidate capture.
    // Even a synchronous advisory error cannot replace an actual target result.
    if let Some(pending) = probe.as_mut() {
        let immediate =
            std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx))).await;
        if let std::task::Poll::Ready(result) = immediate {
            drop(probe.take());
            match result {
                Ok(stream) => {
                    validate_target_context(
                        directory,
                        route_context,
                        dispatcher,
                        &state.local_node_id,
                    )?;
                    if deadline <= tokio::time::Instant::now() {
                        return Err(NetworkError::timeout(
                            "establish routed service RPC stream",
                            NetworkTimeoutPhase::Connect,
                            original_timeout,
                            NetworkDeliveryState::DefinitelyNotSent,
                        ));
                    }
                    return Ok(stream);
                }
                Err(failure) => {
                    if !failure.allows_source_read() {
                        return Err(failure.into_error());
                    }
                    first_error = Some(failure.into_error());
                }
            }
        }
    }
    let captured = (|| -> Result<_, NetworkError> {
        let baseline = directory.connectivity_freshness_baseline()?;
        let (version, members, endpoints, ordinary_records) =
            directory.routing_records_snapshot()?.into_parts();
        let snapshot = DirectorySnapshot::try_from_records(version, members, endpoints)?;
        validate_refresh_inputs(
            directory,
            version,
            &baseline,
            route_context,
            dispatcher,
            &state.local_node_id,
        )?;
        if !snapshot.is_authorized(&state.local_node_id) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: state.local_node_id.clone(),
            });
        }
        if !snapshot.is_authorized(target) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: target.clone(),
            });
        }
        let records =
            crate::route_graph_read_model::read_cluster_connectivity_graph_for_routing_snapshot(
                paths,
                state,
                &snapshot,
                ordinary_records,
            )?;
        let candidates =
            direct_source_endpoint_candidates(&snapshot, state, &records, &baseline.records)?;
        Ok((baseline, version, snapshot, records, candidates))
    })();
    let (baseline, version, snapshot, mut records, candidates) = match captured {
        Ok(captured) => captured,
        Err(advisory_error) => {
            if let Some(pending) = probe {
                return match pending.await {
                    Ok(stream) => {
                        validate_target_context(
                            directory,
                            route_context,
                            dispatcher,
                            &state.local_node_id,
                        )?;
                        if deadline <= tokio::time::Instant::now() {
                            return Err(NetworkError::timeout(
                                "establish routed service RPC stream",
                                NetworkTimeoutPhase::Connect,
                                original_timeout,
                                NetworkDeliveryState::DefinitelyNotSent,
                            ));
                        }
                        Ok(stream)
                    }
                    Err(failure) => Err(failure.into_error()),
                };
            }
            return Err(first_error.unwrap_or(advisory_error));
        }
    };
    let mut direct = std::collections::VecDeque::from(candidates);
    let mut reads = futures_util::stream::FuturesUnordered::new();
    let mut attempted_endpoints = std::collections::BTreeSet::new();
    let mut authenticated = std::collections::BTreeSet::new();
    let mut connector = Arc::clone(&initial);
    let mut probe_after_progress = false;
    enum Progress {
        Source(Option<(SourceRead, Option<Vec<ClusterConnectivityRecord>>)>),
        Target(Result<crate::PeerStream, crate::routing::CarrierEstablishmentFailure>),
    }
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        if probe.is_none() && probe_after_progress && !target_plan.is_empty() {
            probe_after_progress = false;
            probe = Some(carrier_probe(
                Arc::clone(&connector),
                target.clone(),
                options,
                remaining,
            ));
        }
        // Poll the captured/raw target BEFORE constructing or polling source
        // work. A healthy target never waits for a source response. Keep a
        // pending probe alive across failed/unchanged source replies.
        let immediate = if let Some(pending) = probe.as_mut() {
            std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx))).await
        } else {
            std::task::Poll::Pending
        };
        if immediate.is_pending() {
            // Prioritize newly reachable sources over more direct hints. A pending
            // unreachable endpoint cannot consume the entire original deadline
            // before a second bridge's actual source can be queried.
            for node in snapshot.nodes().values() {
                if reads.len() == MAX_SOURCE_READS_IN_FLIGHT {
                    break;
                }
                if node.node_id == state.local_node_id
                    || &node.node_id == target
                    || !state.members.iter().any(|member| {
                        member.node_id == node.node_id
                            && member.node_public_key == node.public_key
                            && member.state.is_authorized()
                    })
                    || !connector.has_live_observed_route(
                        &node.node_id,
                        crate::DestinationProcessAffinity::Any,
                    )
                {
                    continue;
                }
                for endpoint in &node.endpoints {
                    if reads.len() == MAX_SOURCE_READS_IN_FLIGHT {
                        break;
                    }
                    let key = (node.node_id.clone(), endpoint.endpoint_id.clone());
                    if attempted_endpoints.contains(&key)
                        || endpoint.privacy == sinomo_base::ClusterEndpointPrivacy::LocalOnly
                        || !records.iter().any(|record| {
                            record.target_node_id == node.node_id
                                && record.endpoint_id == endpoint.endpoint_id
                                && record.transport == endpoint.transport
                                && record.address == endpoint.address
                                && record.state != ClusterConnectivityState::Unreachable
                                && crate::cluster_network_record_live(
                                    record.expires_at,
                                    OffsetDateTime::now_utc(),
                                )
                        })
                    {
                        continue;
                    }
                    // A source node's processes can hold different live views.
                    // Narrow this read to one already-advertised endpoint without
                    // changing the authorized directory cut or inventing an edge.
                    let mut nodes = snapshot.nodes().values().cloned().collect::<Vec<_>>();
                    nodes
                        .iter_mut()
                        .find(|n| n.node_id == node.node_id)
                        .unwrap()
                        .endpoints
                        .retain(|candidate| candidate == endpoint);
                    let selected = DirectorySnapshot::try_new(snapshot.version(), nodes)?;
                    let selected_records = records
                        .iter()
                        .filter(|record| {
                            record.target_node_id != node.node_id
                                || record.endpoint_id == endpoint.endpoint_id
                        })
                        .cloned()
                        .collect();
                    let source_connector = connector_from_observed_records(
                        paths,
                        state,
                        selected,
                        selected_records,
                        options.call_policy().health,
                        Some(&authenticated),
                    )?;
                    if !source_connector.has_live_observed_route(
                        &node.node_id,
                        crate::DestinationProcessAffinity::Any,
                    ) {
                        continue;
                    }
                    attempted_endpoints.insert(key);
                    reads.push(read_source(
                        SourceRead::Routed(endpoint.clone(), source_connector),
                        paths,
                        state,
                        options,
                        deadline,
                        route_context,
                    ));
                }
            }
            while reads.len() < MAX_SOURCE_READS_IN_FLIGHT {
                let Some(endpoint) = direct.pop_front() else {
                    break;
                };
                // An authenticated reply describes this serving process's RAM;
                // another endpoint for the same node can hold a live renewal that
                // a cold process has not observed. Keep the finite endpoint queue
                // eligible until the target route is proved. The existing in-flight
                // bound, deadline, source checks and cancellation remain unchanged.
                reads.push(read_source(
                    SourceRead::Direct(endpoint),
                    paths,
                    state,
                    options,
                    deadline,
                    route_context,
                ));
            }
        }
        let progress = match immediate {
            std::task::Poll::Ready(result) => Progress::Target(result),
            std::task::Poll::Pending => {
                if let Some(pending) = probe.as_mut() {
                    if reads.is_empty() {
                        Progress::Target(pending.await)
                    } else {
                        tokio::select! {
                            biased;
                            result = pending => Progress::Target(result),
                            source = reads.next() => Progress::Source(source),
                        }
                    }
                } else {
                    Progress::Source(reads.next().await)
                }
            }
        };
        let next = match progress {
            Progress::Target(Ok(stream)) => {
                drop(probe.take());
                drop(reads);
                validate_refresh_inputs(
                    directory,
                    version,
                    &baseline,
                    route_context,
                    dispatcher,
                    &state.local_node_id,
                )?;
                if deadline <= tokio::time::Instant::now() {
                    return Err(NetworkError::timeout(
                        "establish routed service RPC stream",
                        NetworkTimeoutPhase::Connect,
                        original_timeout,
                        NetworkDeliveryState::DefinitelyNotSent,
                    ));
                }
                return Ok(stream);
            }
            Progress::Target(Err(failure)) => {
                drop(probe.take());
                if !failure.allows_source_read() {
                    return Err(failure.into_error());
                }
                first_error.get_or_insert_with(|| failure.error().clone());
                validate_refresh_inputs(
                    directory,
                    version,
                    &baseline,
                    route_context,
                    dispatcher,
                    &state.local_node_id,
                )?;
                continue;
            }
            Progress::Source(source) => {
                validate_refresh_inputs(
                    directory,
                    version,
                    &baseline,
                    route_context,
                    dispatcher,
                    &state.local_node_id,
                )?;
                source
            }
        };
        let Some((origin, response)) = next else {
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
        attempted_endpoints.insert((
            origin.source().clone(),
            origin.endpoint().endpoint_id.clone(),
        ));
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
        let relevant_progress = owned.iter().any(|record| {
            let key = (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            );
            let newer = !authenticated.contains(&key)
                || records
                    .iter()
                    .find(|old| {
                        old.source_node_id == record.source_node_id
                            && old.target_node_id == record.target_node_id
                            && old.endpoint_id == record.endpoint_id
                    })
                    .is_none_or(|old| old.observed_at < record.observed_at);
            newer
                && target_plan.iter().flat_map(|path| &path.hops).any(|hop| {
                    hop.source_node_id == record.source_node_id
                        && hop.target_node_id == record.target_node_id
                        && hop.endpoint.endpoint_id == record.endpoint_id
                        && hop.endpoint.transport == record.transport
                        && hop.endpoint.address == record.address
                })
        });
        let mut new_authenticated = false;
        for record in &owned {
            new_authenticated |= authenticated.insert((
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            ));
        }
        let changed = merge_observations(&mut records, owned);
        if changed || new_authenticated {
            let refreshed = connector_from_observed_records(
                paths,
                state,
                snapshot.clone(),
                records.clone(),
                options.call_policy().health,
                Some(&authenticated),
            )?;
            let next_plan = refreshed.candidate_routes(
                target,
                stream_native_service_route_constraints(),
                options.destination_process_affinity,
            );
            if !same_preferred_transport(&next_plan, &target_plan) {
                // Follow the EXISTING planner order only when the preferred
                // physical route changes. New secondary paths or changed RTT/
                // health on the same transport do not restart its handshake.
                drop(probe.take());
                probe_after_progress = true;
            } else if probe.is_none() && first_error.is_some() && relevant_progress {
                probe_after_progress = true;
            }
            target_plan = next_plan;
            // Keep source reachability current without touching a healthy
            // pending target for unrelated or repeated source observations.
            connector = refreshed;
        }
    }
    // Drop outstanding read futures on success, cancellation or deadline. No
    // detached helper survives this request-owned establishment phase.
    drop(probe);
    drop(reads);
    validate_refresh_inputs(
        directory,
        version,
        &baseline,
        route_context,
        dispatcher,
        &state.local_node_id,
    )?;
    if let Some(error) = first_error {
        return Err(error);
    }
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::timeout(
            "establish routed service RPC stream",
            NetworkTimeoutPhase::Connect,
            original_timeout,
            NetworkDeliveryState::DefinitelyNotSent,
        ));
    }
    // With no refreshed route, preserve the captured connector's own typed
    // no-path/auth/error result rather than fabricating source permission.
    let result = initial
        .connect_with_source_freshness_observation(
            target.clone(),
            crate::ConnectOptions {
                constraints: stream_native_service_route_constraints(),
                timeout: remaining,
                protection: options.protection,
            },
            options.destination_process_affinity,
        )
        .await
        .map_err(crate::routing::CarrierEstablishmentFailure::into_error);
    match result {
        Ok(stream) => {
            validate_refresh_inputs(
                directory,
                version,
                &baseline,
                route_context,
                dispatcher,
                &state.local_node_id,
            )?;
            Ok(stream)
        }
        Err(error) => Err(error),
    }
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
    fn source_read_hint_order_keeps_quarantine_and_newer_failure_evidence() {
        let (_, mut state, _, mut baseline, now) = fixture();
        baseline.source_node_id = state.local_node_id.clone();
        let member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == baseline.target_node_id)
            .unwrap();
        let known = member.endpoints[0].clone();
        let mut unknown = known.clone();
        unknown.endpoint_id = crate::ClusterEndpointId::derived("unknown-source-hint", b"other");
        unknown.address = "127.0.0.1:21355".into();
        member.endpoints.push(unknown.clone());
        let directory =
            DirectorySnapshot::try_from_members(crate::DirectoryVersion(7), state.members.clone())
                .unwrap();
        assert!(!crate::cluster_network_record_live(
            baseline.expires_at,
            now
        ));
        assert_eq!(
            direct_source_endpoint_candidates(&directory, &state, &[], &[baseline.clone()])
                .unwrap()[0],
            known
        );
        let mut current = baseline.clone();
        current.observed_at = now;
        current.expires_at = now + time::Duration::seconds(7200);
        current.state = ClusterConnectivityState::Unreachable;
        current.consecutive_failures = 3;
        assert_eq!(
            direct_source_endpoint_candidates(
                &directory,
                &state,
                &[current.clone()],
                &[baseline.clone()]
            )
            .unwrap()[0],
            unknown
        );
        current.failure_reason = Some(crate::ClusterConnectivityFailureReason::IdentityMismatch);
        assert_eq!(
            direct_source_endpoint_candidates(&directory, &state, &[current], &[baseline.clone()])
                .unwrap(),
            vec![unknown]
        );
        assert!(
            !crate::cluster_network_record_live(baseline.expires_at, now),
            "ordering never renews the stored hint"
        );
    }

    #[test]
    fn source_read_candidates_do_not_dial_other_nodes_local_only_hints() {
        let (_, mut state, _, baseline, _) = fixture();
        let member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == baseline.target_node_id)
            .unwrap();
        let public = member.endpoints[0].clone();
        let mut private = public.clone();
        private.endpoint_id = crate::ClusterEndpointId::derived("foreign-local-only", b"loopback");
        private.privacy = sinomo_base::ClusterEndpointPrivacy::LocalOnly;
        private.transport = ClusterTransportKind::Loopback;
        private.address = "127.0.0.1:21354".into();
        member.endpoints.push(private);
        let directory =
            DirectorySnapshot::try_from_members(crate::DirectoryVersion(7), state.members.clone())
                .unwrap();
        assert_eq!(
            direct_source_endpoint_candidates(&directory, &state, &[], &[]).unwrap(),
            vec![public],
            "a peer's bootstrap loopback hint names its local machine, not a source read endpoint on this caller"
        );
    }

    #[test]
    fn source_read_cannot_convert_foreign_local_only_rows_into_edges() {
        let (_, mut state, source, mut baseline, now) = fixture();
        let member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == baseline.target_node_id)
            .unwrap();
        member.endpoints[0].privacy = sinomo_base::ClusterEndpointPrivacy::LocalOnly;
        let directory =
            DirectorySnapshot::try_from_members(crate::DirectoryVersion(7), state.members.clone())
                .unwrap();
        baseline.observed_at = now;
        baseline.expires_at = now + time::Duration::seconds(900);
        assert!(
            matching_source_records(
                &directory,
                &state,
                &source,
                std::slice::from_ref(&baseline),
                vec![baseline.clone()],
                now
            )
            .unwrap()
            .is_empty(),
            "an authenticated source cannot make another node's local-only endpoint usable from this caller"
        );
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
                3 => {
                    rejected.failure_reason =
                        Some(crate::ClusterConnectivityFailureReason::IdentityMismatch)
                }
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
    fn confirmed_health_accepts_live_failure_budget_progress_and_reset() {
        for (health, before, after) in [
            (ClusterConnectivityState::Healthy, 0, 1),
            (ClusterConnectivityState::Healthy, 1, 0),
            (ClusterConnectivityState::Degraded, 1, 2),
            (ClusterConnectivityState::Degraded, 2, 1),
        ] {
            let (directory, state, source, mut baseline, now) = fixture();
            baseline.state = health;
            baseline.consecutive_failures = before;
            let mut live = baseline.clone();
            live.consecutive_failures = after;
            live.observed_at = now;
            live.expires_at =
                now + time::Duration::seconds(crate::CLUSTER_CONNECTIVITY_TTL_SECONDS);
            assert_eq!(
                matching_source_records(
                    &directory,
                    &state,
                    &source,
                    &[baseline.clone()],
                    vec![live.clone()],
                    now,
                )
                .unwrap(),
                vec![live],
                "same confirmed health: count {before}->{after}"
            );
            assert!(!crate::cluster_network_record_live(
                baseline.expires_at,
                now
            ));
        }
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
