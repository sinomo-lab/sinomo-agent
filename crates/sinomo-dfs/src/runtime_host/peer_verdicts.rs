//! This node's per-peer network verdicts, as the write-set roles act on them.
//!
//! The network layer computes the verdicts once per process for every
//! subscriber; this source subscribes only while a role needs them. The
//! keeper reads it to keep and take over shards, and the takeover
//! corroboration service reads it to answer whether this node still reaches a
//! shard primary. Tests may supply the verdicts of one data root instead.

use super::*;
use crate::DfsWriteSetMemberReach;
use sinomo_core::network::PeerPathVerdict;
use std::collections::BTreeMap;

/// Every peer's verdict at one generation of this node's view.
#[derive(Clone, Debug, Default)]
pub(super) struct DfsPeerVerdictSnapshot {
    /// Advances whenever any peer's entry changes.
    pub(super) generation: u64,
    /// Advances whenever a verification episode starts or ends.
    pub(super) verification_generation: u64,
    pub(super) peers: BTreeMap<NodeId, (PeerPathVerdict, bool)>,
}

impl DfsPeerVerdictSnapshot {
    /// The peer's settled reach; a verdict being verified is unsettled.
    pub(super) fn reach(&self, node: &NodeId) -> DfsWriteSetMemberReach {
        match self.peers.get(node) {
            Some((PeerPathVerdict::Path, false)) => DfsWriteSetMemberReach::Path,
            Some((PeerPathVerdict::NoPath, false)) => DfsWriteSetMemberReach::NoPath,
            _ => DfsWriteSetMemberReach::Unsettled,
        }
    }

    /// The peer's verdict and whether it is being verified; `Unknown` for a
    /// node that is not an authorized peer.
    pub(super) fn entry(&self, node: &NodeId) -> (PeerPathVerdict, bool) {
        self.peers
            .get(node)
            .copied()
            .unwrap_or((PeerPathVerdict::Unknown, false))
    }
}

/// A subscription to this node's verdicts, held only while `retain(true)`.
pub(super) struct DfsPeerVerdicts {
    cluster_paths: ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    subscription: Option<sinomo_core::network::PeerReachabilitySubscription>,
    #[cfg(any(test, feature = "test-support"))]
    test: std::path::PathBuf,
    /// The test verdict generation this role last read or woke for.
    #[cfg(any(test, feature = "test-support"))]
    test_seen: std::sync::atomic::AtomicU64,
}

impl DfsPeerVerdicts {
    pub(super) fn new(
        _data_dir: &SinomoDataDir,
        cluster_paths: &ClusterPaths,
        state: &sinomo_core::ClusterBootstrapState,
    ) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            state: state.clone(),
            subscription: None,
            #[cfg(any(test, feature = "test-support"))]
            test: _data_dir.root(),
            #[cfg(any(test, feature = "test-support"))]
            test_seen: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(super) fn retain(&mut self, needed: bool) {
        #[cfg(any(test, feature = "test-support"))]
        if test_seam::installed(&self.test) {
            self.subscription = None;
            return;
        }
        if needed && self.subscription.is_none() {
            self.subscription = Some(sinomo_core::network::subscribe_peer_reachability(
                &self.cluster_paths,
                &self.state,
            ));
        } else if !needed {
            self.subscription = None;
        }
    }

    pub(super) fn current(&self) -> DfsPeerVerdictSnapshot {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(snapshot) = test_seam::snapshot(&self.test) {
            self.test_seen
                .store(snapshot.generation, std::sync::atomic::Ordering::Release);
            return snapshot;
        }
        self.subscription
            .as_ref()
            .map(|subscription| {
                let current = subscription.current();
                DfsPeerVerdictSnapshot {
                    generation: current.generation,
                    verification_generation: current.inputs.verification_generation,
                    peers: current
                        .peers
                        .iter()
                        .map(|(node, peer)| (node.clone(), (peer.verdict, peer.verifying)))
                        .collect(),
                }
            })
            .unwrap_or_default()
    }

    /// Wait until some peer's entry changes; forever while not retained.
    pub(super) async fn changed(&mut self) {
        #[cfg(any(test, feature = "test-support"))]
        if test_seam::installed(&self.test) {
            // A verdict set while this role was not waiting is not lost: as
            // with the network's watch, a generation it has not seen ends
            // the wait at once.
            let wake = test_seam::wake(&self.test);
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let generation = test_seam::generation(&self.test);
            if self
                .test_seen
                .swap(generation, std::sync::atomic::Ordering::AcqRel)
                != generation
            {
                return;
            }
            notified.await;
            return;
        }
        match self.subscription.as_mut() {
            Some(subscription) => {
                subscription.changed().await;
            }
            None => std::future::pending::<()>().await,
        }
    }

    /// Ask the network to verify `peer` now. False when this process has no
    /// connectivity probe owner, so no episode will start.
    pub(super) fn request_verification(&self, peer: &NodeId) -> bool {
        #[cfg(any(test, feature = "test-support"))]
        if test_seam::installed(&self.test) {
            test_seam::record_verification_request(&self.test, peer);
            return false;
        }
        sinomo_core::network::request_peer_verification(&self.cluster_paths, &self.state, peer)
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(super) mod test_seam {
    //! Test support: the verdicts of one data root, set by the test. With
    //! them installed nothing subscribes to the network, and a verification
    //! request is recorded instead of starting an episode.

    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct State {
        generation: u64,
        peers: BTreeMap<NodeId, (PeerPathVerdict, bool)>,
        verification_requests: Vec<NodeId>,
        wake: Arc<tokio::sync::Notify>,
    }

    static VERDICTS: LazyLock<Mutex<BTreeMap<PathBuf, State>>> = LazyLock::new(Default::default);

    fn verdicts() -> std::sync::MutexGuard<'static, BTreeMap<PathBuf, State>> {
        VERDICTS.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(in crate::runtime_host) fn installed(root: &Path) -> bool {
        verdicts().contains_key(root)
    }

    pub(super) fn snapshot(root: &Path) -> Option<DfsPeerVerdictSnapshot> {
        verdicts().get(root).map(|state| DfsPeerVerdictSnapshot {
            generation: state.generation,
            verification_generation: state.verification_requests.len() as u64,
            peers: state.peers.clone(),
        })
    }

    pub(super) fn generation(root: &Path) -> u64 {
        verdicts().get(root).map_or(0, |state| state.generation)
    }

    pub(super) fn wake(root: &Path) -> Arc<tokio::sync::Notify> {
        Arc::clone(&verdicts().entry(root.to_path_buf()).or_default().wake)
    }

    pub(super) fn record_verification_request(root: &Path, peer: &NodeId) {
        if let Some(state) = verdicts().get_mut(root) {
            state.verification_requests.push(peer.clone());
        }
    }

    /// Set the verdict every role of `root` sees for `peer`.
    pub(in crate::runtime_host) fn set_peer_verdict(
        root: PathBuf,
        peer: NodeId,
        verdict: PeerPathVerdict,
        verifying: bool,
    ) {
        let wake = {
            let mut verdicts = verdicts();
            let state = verdicts.entry(root).or_default();
            state.peers.insert(peer, (verdict, verifying));
            state.generation += 1;
            Arc::clone(&state.wake)
        };
        wake.notify_waiters();
    }

    pub(in crate::runtime_host) fn verification_requests(root: &Path) -> Vec<NodeId> {
        verdicts()
            .get(root)
            .map(|state| state.verification_requests.clone())
            .unwrap_or_default()
    }
}
