//! Demand for fresh direct-edge evidence about one peer.
//!
//! A failed delivery, or a delivery that contradicts this observer's no-path
//! verdict, asks the process's connectivity probe owner to verify that peer's
//! direct edges at once instead of waiting for the next scheduled probe. A
//! request is not evidence: only the owner's probe observation changes
//! connectivity, and it judges the peer by connect failure, disconnect and the
//! no-progress limit only. A process without a registered owner, such as a CLI,
//! ignores requests, so nothing accumulates where nothing can verify.

use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use crate::paths::ClusterPaths;
use serde::{Deserialize, Serialize};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

/// Pending (not yet taken) verification requests retained per cluster root.
pub const MAX_PENDING_PEER_VERIFICATIONS: usize = 256;

static NEXT_PEER_VERIFICATION_OWNER_ID: AtomicU64 = AtomicU64::new(1);
static PEER_VERIFICATIONS: LazyLock<Mutex<HashMap<PathBuf, RootPeerVerifications>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Why a peer's direct edges should be verified now.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PeerVerificationCause {
    /// The caller's own deadline expired before any stream to the peer was
    /// established. Weak: the network did not reach a verdict of its own.
    DeliveryDeadline,
    /// A connect failure or refusal, a disconnect, or the no-progress limit.
    DeliveryFailed,
    /// A delivery succeeded while this observer's verdict for the peer was no
    /// path, so the recorded evidence is stale.
    DeliverySucceeded,
    /// A service asked for verification, for example before it reports
    /// whether this node still reaches the peer.
    Requested,
}

impl PeerVerificationCause {
    /// Weak causes yield to a recent verification that found a path.
    pub const fn is_weak(self) -> bool {
        matches!(self, Self::DeliveryDeadline)
    }

    /// Failure-driven causes are not repeated against a peer whose every
    /// local direct edge is already unreachable; the evidence already says so.
    pub const fn is_failure(self) -> bool {
        matches!(self, Self::DeliveryDeadline | Self::DeliveryFailed)
    }

    /// Classifies a terminal delivery error.
    ///
    /// A slow peer that was reached (`Overall` after the request may have been
    /// sent) and authorization or application errors request nothing.
    pub fn from_delivery_error(error: &NetworkError) -> Option<Self> {
        match error {
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Connect,
                ..
            }
            | NetworkError::RetryExhausted { .. }
            | NetworkError::DeliveryFailed { .. }
            | NetworkError::Io { .. } => Some(Self::DeliveryFailed),
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Overall,
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            } => Some(Self::DeliveryDeadline),
            _ => None,
        }
    }
}

#[derive(Default)]
struct RootPeerVerifications {
    owners: BTreeMap<u64, Arc<tokio::sync::Notify>>,
    pending: BTreeMap<NodeId, PeerVerificationCause>,
    in_progress: BTreeSet<NodeId>,
    generation: u64,
}

impl RootPeerVerifications {
    fn is_unused(&self) -> bool {
        self.owners.is_empty() && self.pending.is_empty() && self.in_progress.is_empty()
    }
}

/// Requests verification of `peer` from this process's probe owner.
///
/// Returns whether a verification is now pending or running for the peer.
/// Requests coalesce per peer; the strongest pending cause is kept.
pub(crate) fn request_peer_verification_for_node(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
    cause: PeerVerificationCause,
) -> bool {
    if peer_node_id == local_node_id {
        return false;
    }
    let Ok(mut roots) = PEER_VERIFICATIONS.lock() else {
        return false;
    };
    let Some(root) = roots.get_mut(&paths.root) else {
        return false;
    };
    if root.owners.is_empty() {
        return false;
    }
    if root.in_progress.contains(peer_node_id) {
        return true;
    }
    if let Some(existing) = root.pending.get_mut(peer_node_id) {
        *existing = (*existing).max(cause);
        return true;
    }
    if root.pending.len() >= MAX_PENDING_PEER_VERIFICATIONS {
        if cause.is_weak() {
            return false;
        }
        let Some(weak) = root
            .pending
            .iter()
            .find(|(_, pending)| pending.is_weak())
            .map(|(peer, _)| peer.clone())
        else {
            return false;
        };
        root.pending.remove(&weak);
    }
    root.pending.insert(peer_node_id.clone(), cause);
    for notify in root.owners.values() {
        notify.notify_one();
    }
    true
}

/// Requests verification of `peer` on behalf of a service, such as a DFS
/// member asked whether it still reaches a shard primary.
///
/// Returns false when this process has no connectivity probe owner.
pub fn request_peer_verification(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    peer_node_id: &NodeId,
) -> bool {
    request_peer_verification_for_node(
        paths,
        &state.local_node_id,
        peer_node_id,
        PeerVerificationCause::Requested,
    )
}

/// Requests verification after this node's own direct edge to `peer` failed
/// outside a call this crate classifies, such as a relay's outbound hop.
#[doc(hidden)]
pub fn request_peer_verification_after_edge_failure(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
) -> bool {
    request_peer_verification_for_node(
        paths,
        local_node_id,
        peer_node_id,
        PeerVerificationCause::DeliveryFailed,
    )
}

/// The single connectivity probe owner's registration for one cluster root.
///
/// Requests are accepted only while an owner is registered. Dropping the
/// owner unregisters it.
pub struct PeerVerificationOwner {
    root: PathBuf,
    id: u64,
    notify: Arc<tokio::sync::Notify>,
}

impl PeerVerificationOwner {
    /// Waits until at least one request may be pending.
    pub async fn requested(&self) {
        self.notify.notified().await;
    }
}

impl Drop for PeerVerificationOwner {
    fn drop(&mut self) {
        let Ok(mut roots) = PEER_VERIFICATIONS.lock() else {
            return;
        };
        if let Some(root) = roots.get_mut(&self.root) {
            root.owners.remove(&self.id);
            if root.owners.is_empty() {
                root.pending.clear();
                if !root.in_progress.is_empty() {
                    root.in_progress.clear();
                    root.generation = root.generation.wrapping_add(1);
                    crate::route_input_watch::notify_route_input_changed(&self.root);
                }
            }
            if root.is_unused() {
                roots.remove(&self.root);
            }
        }
    }
}

/// Registers this process's connectivity probe owner for `paths`.
#[doc(hidden)]
pub fn register_peer_verification_owner(paths: &ClusterPaths) -> PeerVerificationOwner {
    let id = NEXT_PEER_VERIFICATION_OWNER_ID.fetch_add(1, Ordering::Relaxed);
    let notify = Arc::new(tokio::sync::Notify::new());
    if let Ok(mut roots) = PEER_VERIFICATIONS.lock() {
        let root = roots.entry(paths.root.clone()).or_default();
        root.owners.insert(id, Arc::clone(&notify));
        if !root.pending.is_empty() {
            notify.notify_one();
        }
    }
    PeerVerificationOwner {
        root: paths.root.clone(),
        id,
        notify,
    }
}

/// Takes every pending request for `paths`.
#[doc(hidden)]
pub fn take_pending_peer_verifications(
    paths: &ClusterPaths,
) -> Vec<(NodeId, PeerVerificationCause)> {
    let Ok(mut roots) = PEER_VERIFICATIONS.lock() else {
        return Vec::new();
    };
    roots
        .get_mut(&paths.root)
        .map(|root| std::mem::take(&mut root.pending).into_iter().collect())
        .unwrap_or_default()
}

/// Marks a verification episode for `peer` as running.
#[doc(hidden)]
pub fn begin_peer_verification(paths: &ClusterPaths, peer_node_id: &NodeId) {
    update_in_progress(paths, |in_progress| {
        in_progress.insert(peer_node_id.clone())
    });
}

/// Marks the verification episode for `peer` as settled.
#[doc(hidden)]
pub fn settle_peer_verification(paths: &ClusterPaths, peer_node_id: &NodeId) {
    update_in_progress(paths, |in_progress| in_progress.remove(peer_node_id));
}

fn update_in_progress(paths: &ClusterPaths, update: impl FnOnce(&mut BTreeSet<NodeId>) -> bool) {
    let changed = {
        let Ok(mut roots) = PEER_VERIFICATIONS.lock() else {
            return;
        };
        let Some(root) = roots.get_mut(&paths.root) else {
            return;
        };
        let changed = update(&mut root.in_progress);
        if changed {
            root.generation = root.generation.wrapping_add(1);
        }
        changed
    };
    if changed {
        crate::route_input_watch::notify_route_input_changed(&paths.root);
    }
}

/// Generation of the running-verification set for `paths` plus that set.
pub(crate) fn peer_verification_state(paths: &ClusterPaths) -> (u64, BTreeSet<NodeId>) {
    PEER_VERIFICATIONS
        .lock()
        .ok()
        .and_then(|roots| {
            roots
                .get(&paths.root)
                .map(|root| (root.generation, root.in_progress.clone()))
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Registry state is process memory keyed by root; nothing touches disk.
    fn paths(name: &str) -> ClusterPaths {
        ClusterPaths::new(&sinomo_local::SinomoDataDir::new(format!(
            "/nonexistent/sinomo-peer-verification-tests/{name}"
        )))
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    #[test]
    fn delivery_errors_classify_by_network_verdict_not_caller_deadline() {
        let connect = NetworkError::timeout(
            "connect",
            NetworkTimeoutPhase::Connect,
            std::time::Duration::from_secs(1),
            NetworkDeliveryState::DefinitelyNotSent,
        );
        assert_eq!(
            PeerVerificationCause::from_delivery_error(&connect),
            Some(PeerVerificationCause::DeliveryFailed)
        );
        let reset = NetworkError::delivery_failed(
            "rpc",
            NetworkDeliveryState::PossiblyApplied,
            "connection reset",
        );
        assert_eq!(
            PeerVerificationCause::from_delivery_error(&reset),
            Some(PeerVerificationCause::DeliveryFailed)
        );
        let deadline_before_stream = NetworkError::timeout(
            "rpc",
            NetworkTimeoutPhase::Overall,
            std::time::Duration::from_secs(1),
            NetworkDeliveryState::DefinitelyNotSent,
        );
        assert_eq!(
            PeerVerificationCause::from_delivery_error(&deadline_before_stream),
            Some(PeerVerificationCause::DeliveryDeadline)
        );
        let slow_reply = NetworkError::timeout(
            "rpc",
            NetworkTimeoutPhase::Overall,
            std::time::Duration::from_secs(1),
            NetworkDeliveryState::PossiblyApplied,
        );
        assert_eq!(
            PeerVerificationCause::from_delivery_error(&slow_reply),
            None
        );
        assert_eq!(
            PeerVerificationCause::from_delivery_error(&NetworkError::AuthDenied),
            None
        );
        assert_eq!(
            PeerVerificationCause::from_delivery_error(&NetworkError::UnauthorizedNode {
                node_id: node("node_peer_verification_classify"),
            }),
            None
        );
    }

    #[test]
    fn requests_without_an_owner_are_ignored() {
        let paths = paths("no-owner");
        let local = node("node_peer_verification_no_owner_local");
        let peer = node("node_peer_verification_no_owner_peer");
        assert!(!request_peer_verification_for_node(
            &paths,
            &local,
            &peer,
            PeerVerificationCause::DeliveryFailed
        ));
        assert!(take_pending_peer_verifications(&paths).is_empty());
    }

    #[test]
    fn requests_coalesce_per_peer_and_keep_the_strongest_cause() {
        let paths = paths("coalesce");
        let local = node("node_peer_verification_coalesce_local");
        let peer = node("node_peer_verification_coalesce_peer");
        let owner = register_peer_verification_owner(&paths);
        assert!(!request_peer_verification_for_node(
            &paths,
            &local,
            &local,
            PeerVerificationCause::DeliveryFailed
        ));
        for cause in [
            PeerVerificationCause::DeliveryDeadline,
            PeerVerificationCause::DeliveryFailed,
            PeerVerificationCause::DeliveryDeadline,
        ] {
            assert!(request_peer_verification_for_node(
                &paths, &local, &peer, cause
            ));
        }
        assert_eq!(
            take_pending_peer_verifications(&paths),
            vec![(peer.clone(), PeerVerificationCause::DeliveryFailed)]
        );
        begin_peer_verification(&paths, &peer);
        assert!(request_peer_verification_for_node(
            &paths,
            &local,
            &peer,
            PeerVerificationCause::DeliveryFailed
        ));
        assert!(
            take_pending_peer_verifications(&paths).is_empty(),
            "a running episode absorbs further requests"
        );
        let (generation, running) = peer_verification_state(&paths);
        assert!(running.contains(&peer));
        settle_peer_verification(&paths, &peer);
        let (settled_generation, running) = peer_verification_state(&paths);
        assert!(running.is_empty());
        assert_ne!(generation, settled_generation);
        drop(owner);
        assert!(!request_peer_verification_for_node(
            &paths,
            &local,
            &peer,
            PeerVerificationCause::DeliveryFailed
        ));
    }

    #[test]
    fn pending_capacity_is_bounded_and_evicts_weak_requests_first() {
        let paths = paths("capacity");
        let local = node("node_peer_verification_capacity_local");
        let _owner = register_peer_verification_owner(&paths);
        let weak = node("node_peer_verification_capacity_weak");
        assert!(request_peer_verification_for_node(
            &paths,
            &local,
            &weak,
            PeerVerificationCause::DeliveryDeadline
        ));
        for index in 1..MAX_PENDING_PEER_VERIFICATIONS {
            assert!(request_peer_verification_for_node(
                &paths,
                &local,
                &node(&format!("node_peer_verification_capacity_{index}")),
                PeerVerificationCause::DeliveryFailed
            ));
        }
        assert!(!request_peer_verification_for_node(
            &paths,
            &local,
            &node("node_peer_verification_capacity_late_weak"),
            PeerVerificationCause::DeliveryDeadline
        ));
        assert!(request_peer_verification_for_node(
            &paths,
            &local,
            &node("node_peer_verification_capacity_late_strong"),
            PeerVerificationCause::DeliveryFailed
        ));
        let pending = take_pending_peer_verifications(&paths);
        assert_eq!(pending.len(), MAX_PENDING_PEER_VERIFICATIONS);
        assert!(!pending.iter().any(|(peer, _)| peer == &weak));
    }
}
