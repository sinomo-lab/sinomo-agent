//! This node's per-peer routed-availability verdicts and their change stream.
//!
//! Routing owns routed availability. This module classifies every authorized
//! peer from the same directory and connectivity capture that dispatch plans
//! against, and tells subscribers which peers' verdicts changed. The verdict is
//! one observer's view, held in process memory only: it is never written to
//! connectivity, cluster KV, membership or node status, and it never grants or
//! revokes routing authority.
//!
//! Recomputation is reactive. Route-input owners send a hint when a
//! route-relevant input changes; a subscriber then compares the exact input
//! generations with the memoized ones and recomputes only when they moved. With
//! no subscriber nothing is computed, and time-only connectivity refreshes do not
//! move any input.

use crate::connectivity_verification::{
    PeerVerificationCause, peer_verification_state, request_peer_verification_for_node,
};
use crate::directory::DirectorySnapshot;
use crate::paths::ClusterPaths;
use crate::route_input_watch::{RouteInputChange, subscribe_route_input_changes};
use crate::routing::{PeerPathVerdict, peer_path_classifications};
use crate::{NetworkError, cluster_directory_for, cluster_directory_with_bootstrap_state};
use sinomo_base::ClusterBootstrapState;
use sinomo_node::{ClusterId, NodeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use time::OffsetDateTime;

static PEER_REACHABILITY_VIEWS: LazyLock<
    Mutex<HashMap<PeerReachabilityViewKey, PeerReachabilityView>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
/// Number of subscribed views holding at least one no-path verdict. Lets a
/// successful delivery skip the view lookup entirely in the common case.
static PEER_REACHABILITY_NO_PATH_VIEWS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PEER_REACHABILITY_COMPUTATIONS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One peer's verdict from this node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerReachability {
    pub peer_node_id: NodeId,
    pub verdict: PeerPathVerdict,
    /// Every path to the peer crosses at least one relay.
    pub relayed_only: bool,
    /// The connectivity probe owner is verifying this peer's direct edges.
    pub verifying: bool,
    /// Changes each time a verification episode for this peer settles,
    /// whatever ended it: a conclusion, a bound or a cancellation. Zero while
    /// none has. Equal values mean no episode settled in between. A settled
    /// episode need not have examined the peer: see `concluded_generation`.
    pub examined_generation: u64,
    /// The `examined_generation` of this peer's last episode whose own probe
    /// concluded: a direct edge answered, or every direct edge failed. Zero
    /// while none has. An episode that was cancelled, had nothing to dial,
    /// produced no observation or ran out of rounds, or that recorded
    /// evidence answered without a probe, leaves it unchanged. A subscriber
    /// that noted the value can therefore tell a verdict the network examined
    /// since from one that only stood.
    pub concluded_generation: u64,
}

/// Generations of every input a snapshot was computed from.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerReachabilityInputs {
    pub directory_routing_version: u64,
    pub local_routing_generation: u64,
    pub module_routing_generation: u64,
    pub verification_generation: u64,
}

/// Every authorized peer's verdict from one observer at one input generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerReachabilitySnapshot {
    pub cluster_id: ClusterId,
    pub observer_node_id: NodeId,
    /// Advances whenever any peer's entry changes.
    pub generation: u64,
    pub inputs: PeerReachabilityInputs,
    pub peers: BTreeMap<NodeId, PeerReachability>,
}

impl PeerReachabilitySnapshot {
    fn empty(state: &ClusterBootstrapState) -> Self {
        Self {
            cluster_id: state.cluster_id.clone(),
            observer_node_id: state.local_node_id.clone(),
            generation: 0,
            inputs: PeerReachabilityInputs::default(),
            peers: BTreeMap::new(),
        }
    }

    /// The verdict for `peer`, `Unknown` when it is not an authorized peer.
    pub fn verdict(&self, peer_node_id: &NodeId) -> PeerPathVerdict {
        self.peers
            .get(peer_node_id)
            .map_or(PeerPathVerdict::Unknown, |peer| peer.verdict)
    }
}

/// One peer whose entry differs from the subscriber's previous snapshot.
/// `None` means the peer is not (or no longer) an authorized peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerReachabilityChange {
    pub peer_node_id: NodeId,
    pub previous: Option<PeerReachability>,
    pub current: Option<PeerReachability>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PeerReachabilityViewKey {
    root: PathBuf,
    cluster_id: ClusterId,
    observer_node_id: NodeId,
}

impl PeerReachabilityViewKey {
    fn new(paths: &ClusterPaths, state: &ClusterBootstrapState) -> Self {
        Self {
            root: paths.root.clone(),
            cluster_id: state.cluster_id.clone(),
            observer_node_id: state.local_node_id.clone(),
        }
    }
}

struct PeerReachabilityView {
    inputs: Option<PeerReachabilityInputs>,
    /// Earliest expiry among the edges the snapshot used. Expired evidence
    /// leaves the classification, so the memo is valid only before it; the
    /// next refresh after it recomputes. Expiry alone schedules nothing.
    valid_until: Option<OffsetDateTime>,
    snapshot: Arc<PeerReachabilitySnapshot>,
    subscribers: usize,
    any_no_path: bool,
    /// Serializes recomputation of this view; holders do no async work.
    compute: Arc<Mutex<()>>,
}

fn current_inputs(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<PeerReachabilityInputs, NetworkError> {
    // Generation counters are read before the routing capture, so a change
    // racing the capture leaves a newer generation and a later wake.
    let module_routing_generation = crate::network_module_routing_generation();
    let local_routing_generation =
        crate::connectivity_store::local_cluster_connectivity_routing_generation(paths);
    let verification_generation = peer_verification_state(paths).generation;
    let directory = cluster_directory_with_bootstrap_state(cluster_directory_for(paths), state);
    Ok(PeerReachabilityInputs {
        directory_routing_version: directory.routing_version()?.0,
        local_routing_generation,
        module_routing_generation,
        verification_generation,
    })
}

/// Every authorized peer's entry and the earliest expiry among the edges
/// that produced them.
struct ComputedPeers {
    peers: BTreeMap<NodeId, PeerReachability>,
    valid_until: Option<OffsetDateTime>,
}

fn compute_peers(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<ComputedPeers, NetworkError> {
    #[cfg(test)]
    if let Ok(mut computations) = PEER_REACHABILITY_COMPUTATIONS.lock() {
        *computations.entry(paths.root.clone()).or_default() += 1;
    }
    let verification = peer_verification_state(paths);
    let directory = cluster_directory_with_bootstrap_state(cluster_directory_for(paths), state);
    let (version, members, endpoints, committed) =
        directory.routing_records_snapshot()?.into_parts();
    let directory = DirectorySnapshot::try_from_records(version, members, endpoints)?;
    let records =
        crate::route_graph_read_model::read_cluster_connectivity_graph_for_routing_snapshot(
            paths, state, &directory, committed,
        )?;
    let connectivity = crate::connectivity_snapshot_from_directory_records(
        &directory,
        records,
        OffsetDateTime::now_utc(),
    )?;
    let valid_until = connectivity
        .edges()
        .iter()
        .map(|edge| edge.expires_at)
        .min();
    let peers = peer_path_classifications(&directory, &connectivity, &state.local_node_id)
        .into_iter()
        .map(|(peer_node_id, classification)| {
            let entry = PeerReachability {
                verifying: verification.verifying.contains(&peer_node_id),
                examined_generation: verification
                    .examined
                    .get(&peer_node_id)
                    .copied()
                    .unwrap_or_default(),
                concluded_generation: verification
                    .concluded
                    .get(&peer_node_id)
                    .copied()
                    .unwrap_or_default(),
                peer_node_id: peer_node_id.clone(),
                verdict: classification.verdict,
                relayed_only: classification.relayed_only,
            };
            (peer_node_id, entry)
        })
        .collect();
    Ok(ComputedPeers { peers, valid_until })
}

fn view_is_current(view: &PeerReachabilityView, inputs: PeerReachabilityInputs) -> bool {
    view.inputs == Some(inputs)
        && view
            .valid_until
            .is_none_or(|valid_until| OffsetDateTime::now_utc() < valid_until)
}

fn set_any_no_path(view: &mut PeerReachabilityView, any_no_path: bool) {
    if view.any_no_path == any_no_path {
        return;
    }
    view.any_no_path = any_no_path;
    if any_no_path {
        PEER_REACHABILITY_NO_PATH_VIEWS.fetch_add(1, Ordering::AcqRel);
    } else {
        PEER_REACHABILITY_NO_PATH_VIEWS.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Returns the subscribed view's snapshot, recomputing it only when an input
/// generation moved.
fn refresh_view(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    key: &PeerReachabilityViewKey,
) -> Result<Arc<PeerReachabilitySnapshot>, NetworkError> {
    let compute = {
        let views = PEER_REACHABILITY_VIEWS
            .lock()
            .map_err(|_| view_lock_error())?;
        let view = views.get(key).ok_or_else(view_missing_error)?;
        Arc::clone(&view.compute)
    };
    let _compute = compute.lock().map_err(|_| view_lock_error())?;
    let inputs = current_inputs(paths, state)?;
    {
        let views = PEER_REACHABILITY_VIEWS
            .lock()
            .map_err(|_| view_lock_error())?;
        let view = views.get(key).ok_or_else(view_missing_error)?;
        if view_is_current(view, inputs) {
            return Ok(Arc::clone(&view.snapshot));
        }
    }
    let ComputedPeers { peers, valid_until } = compute_peers(paths, state)?;
    let mut views = PEER_REACHABILITY_VIEWS
        .lock()
        .map_err(|_| view_lock_error())?;
    let view = views.get_mut(key).ok_or_else(view_missing_error)?;
    let generation = if view.snapshot.peers == peers {
        view.snapshot.generation
    } else {
        view.snapshot.generation.wrapping_add(1)
    };
    let any_no_path = peers
        .values()
        .any(|peer| peer.verdict == PeerPathVerdict::NoPath);
    view.snapshot = Arc::new(PeerReachabilitySnapshot {
        cluster_id: state.cluster_id.clone(),
        observer_node_id: state.local_node_id.clone(),
        generation,
        inputs,
        peers,
    });
    view.inputs = Some(inputs);
    view.valid_until = valid_until;
    set_any_no_path(view, any_no_path);
    Ok(Arc::clone(&view.snapshot))
}

fn view_lock_error() -> NetworkError {
    NetworkError::Io {
        operation: "lock peer reachability views".to_string(),
        detail: "peer reachability view lock poisoned".to_string(),
    }
}

fn view_missing_error() -> NetworkError {
    NetworkError::Io {
        operation: "refresh peer reachability view".to_string(),
        detail: "peer reachability view is not subscribed".to_string(),
    }
}

/// Every authorized peer's current verdict from this node.
///
/// Uses a subscribed view's memoized snapshot when one exists; otherwise
/// computes a snapshot without retaining it.
pub fn peer_reachability_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Arc<PeerReachabilitySnapshot>, NetworkError> {
    let key = PeerReachabilityViewKey::new(paths, state);
    let subscribed = PEER_REACHABILITY_VIEWS
        .lock()
        .map_err(|_| view_lock_error())?
        .contains_key(&key);
    if subscribed {
        return refresh_view(paths, state, &key);
    }
    let inputs = current_inputs(paths, state)?;
    let peers = compute_peers(paths, state)?.peers;
    Ok(Arc::new(PeerReachabilitySnapshot {
        cluster_id: state.cluster_id.clone(),
        observer_node_id: state.local_node_id.clone(),
        generation: 0,
        inputs,
        peers,
    }))
}

/// Subscribes to changes of this node's per-peer verdicts.
///
/// The subscription is bound to the cluster identity and bootstrap state it
/// was created with. It is state-convergent: each wake compares the current
/// snapshot with the one this subscriber last returned, so a lagged hint loses
/// nothing and a verdict that flips back between two waits is not reported.
pub fn subscribe_peer_reachability(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> PeerReachabilitySubscription {
    // Register for hints before the first read so no change can fall between.
    let receiver = subscribe_route_input_changes();
    let key = PeerReachabilityViewKey::new(paths, state);
    if let Ok(mut views) = PEER_REACHABILITY_VIEWS.lock() {
        views
            .entry(key.clone())
            .or_insert_with(|| PeerReachabilityView {
                inputs: None,
                valid_until: None,
                snapshot: Arc::new(PeerReachabilitySnapshot::empty(state)),
                subscribers: 0,
                any_no_path: false,
                compute: Arc::new(Mutex::new(())),
            })
            .subscribers += 1;
    }
    // A failed first read is retried by the first `changed()`.
    let (last, dirty) = match refresh_view(paths, state, &key) {
        Ok(snapshot) => (snapshot, false),
        Err(error) => {
            tracing::debug!(%error, "peer reachability subscription starts without a snapshot");
            (Arc::new(PeerReachabilitySnapshot::empty(state)), true)
        }
    };
    PeerReachabilitySubscription {
        paths: paths.clone(),
        state: state.clone(),
        key,
        last,
        receiver,
        dirty,
    }
}

/// Lag-safe stream of per-peer verdict changes. See
/// [`subscribe_peer_reachability`].
pub struct PeerReachabilitySubscription {
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    key: PeerReachabilityViewKey,
    last: Arc<PeerReachabilitySnapshot>,
    receiver: tokio::sync::broadcast::Receiver<RouteInputChange>,
    dirty: bool,
}

impl PeerReachabilitySubscription {
    /// The snapshot this subscriber last returned changes against.
    pub fn current(&self) -> Arc<PeerReachabilitySnapshot> {
        Arc::clone(&self.last)
    }

    /// Waits until at least one peer's entry differs from [`Self::current`]
    /// and returns exactly those peers. Never returns an empty list.
    pub async fn changed(&mut self) -> Vec<PeerReachabilityChange> {
        self.wait_for_change(false).await
    }

    /// Also wake on verification activity, including a probe owner becoming
    /// available to examine a verdict that was already standing.
    pub async fn changed_or_verification_activity(&mut self) -> Vec<PeerReachabilityChange> {
        self.wait_for_change(true).await
    }

    async fn wait_for_change(&mut self, watch_verification: bool) -> Vec<PeerReachabilityChange> {
        loop {
            if self.dirty {
                self.dirty = false;
                match refresh_view(&self.paths, &self.state, &self.key) {
                    Ok(current) => {
                        let changes = peer_reachability_changes(&self.last, &current);
                        let verification_changed = self.last.inputs.verification_generation
                            != current.inputs.verification_generation;
                        self.last = current;
                        if !changes.is_empty() || (watch_verification && verification_changed) {
                            return changes;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%error, "peer reachability refresh failed; keeping the previous snapshot");
                    }
                }
            }
            match self.receiver.recv().await {
                Ok(change) => self.dirty |= change.affects(&self.key.root),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => self.dirty = true,
                // The sender is process-static.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

impl Drop for PeerReachabilitySubscription {
    fn drop(&mut self) {
        let Ok(mut views) = PEER_REACHABILITY_VIEWS.lock() else {
            return;
        };
        let Some(view) = views.get_mut(&self.key) else {
            return;
        };
        view.subscribers = view.subscribers.saturating_sub(1);
        if view.subscribers == 0 {
            set_any_no_path(view, false);
            views.remove(&self.key);
        }
    }
}

fn peer_reachability_changes(
    previous: &PeerReachabilitySnapshot,
    current: &PeerReachabilitySnapshot,
) -> Vec<PeerReachabilityChange> {
    let peers = previous
        .peers
        .keys()
        .chain(current.peers.keys())
        .collect::<BTreeSet<_>>();
    peers
        .into_iter()
        .filter_map(|peer_node_id| {
            let before = previous.peers.get(peer_node_id);
            let after = current.peers.get(peer_node_id);
            (before != after).then(|| PeerReachabilityChange {
                peer_node_id: peer_node_id.clone(),
                previous: before.cloned(),
                current: after.cloned(),
            })
        })
        .collect()
}

/// A delivery to `peer` succeeded. When this observer's subscribed verdict
/// says there is no path, the evidence is stale: ask for verification so the
/// verdict recovers without waiting for the next scheduled probe.
pub(crate) fn observe_peer_delivery_success(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
) {
    if PEER_REACHABILITY_NO_PATH_VIEWS.load(Ordering::Acquire) == 0 {
        return;
    }
    let no_path = PEER_REACHABILITY_VIEWS.lock().ok().is_some_and(|views| {
        views.iter().any(|(key, view)| {
            key.root == paths.root
                && key.observer_node_id == *local_node_id
                && view.snapshot.verdict(peer_node_id) == PeerPathVerdict::NoPath
        })
    });
    if no_path {
        request_peer_verification_for_node(
            paths,
            local_node_id,
            peer_node_id,
            PeerVerificationCause::DeliverySucceeded,
        );
    }
}

/// A delivery to `peer` failed with `error`; request verification when the
/// failure class warrants it.
pub(crate) fn observe_peer_delivery_failure(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
    error: &NetworkError,
) {
    if let Some(cause) = PeerVerificationCause::from_delivery_error(error) {
        request_peer_verification_for_node(paths, local_node_id, peer_node_id, cause);
    }
}

#[cfg(test)]
fn peer_reachability_computations_for_test(paths: &ClusterPaths) -> u64 {
    PEER_REACHABILITY_COMPUTATIONS
        .lock()
        .ok()
        .and_then(|computations| computations.get(&paths.root).copied())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
