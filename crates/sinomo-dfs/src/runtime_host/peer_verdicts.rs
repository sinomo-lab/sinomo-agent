//! This node's per-peer network verdicts, as the write-set roles act on them.
//!
//! The network layer computes the verdicts once per process for every
//! subscriber; this source subscribes only while a role needs them. The
//! keeper reads it to keep and take over shards, and the takeover
//! corroboration service reads it to answer whether this node still reaches a
//! shard primary. Tests may supply the verdicts of one data root instead.

use super::write_set_takeover::ShardKey;
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
    /// Per peer, the network's examined generation: it changes each time a
    /// verification episode for the peer settles. Absent while none has.
    pub(super) examined: BTreeMap<NodeId, u64>,
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

    /// The peer's examined generation; zero while no episode for it settled.
    pub(super) fn examined_generation(&self, node: &NodeId) -> u64 {
        self.examined.get(node).copied().unwrap_or_default()
    }
}

/// Whether a peer's no-path verdict is evidence gathered under one epoch of
/// a shard's record.
enum Examination {
    /// The verdict was something else when this node first read the epoch, or
    /// the network examined the peer since: a no-path verdict counts.
    Bound,
    /// The verdict already stood when this node first read the epoch, with
    /// this examined generation. It counts once the network examined the
    /// peer again.
    Awaiting { examined_generation: u64 },
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
    /// Per shard and peer, the record epoch a verdict was last judged under.
    examinations: BTreeMap<(ShardKey, NodeId), (u64, Examination)>,
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
            examinations: BTreeMap::new(),
        }
    }

    /// Whether `peer`'s settled no-path verdict in `snapshot` may be acted on
    /// under `epoch` of `shard`'s record: to take the shard over from `peer`,
    /// or to remove `peer` from its write set.
    ///
    /// A verdict that became no-path while this node already read that epoch
    /// is a reachability change under the record and counts as it is. One
    /// that already stood when the epoch was first read was formed before the
    /// record gave this node its role, so only the record changed: the
    /// network is asked to verify the peer, and the verdict counts once an
    /// episode for it has settled since. The wait is for that settlement,
    /// including when the probe owner leaves and returns during the wait.
    ///
    /// Any other verdict is not this function's concern and returns true.
    pub(super) fn examined_under(
        &mut self,
        snapshot: &DfsPeerVerdictSnapshot,
        shard: &ShardKey,
        epoch: u64,
        peer: &NodeId,
    ) -> bool {
        let stands = snapshot.entry(peer).0 == PeerPathVerdict::NoPath;
        let examined_generation = snapshot.examined_generation(peer);
        let key = (shard.clone(), peer.clone());
        match self.examinations.get(&key) {
            Some((seen, Examination::Bound)) if *seen == epoch => return true,
            Some((
                seen,
                Examination::Awaiting {
                    examined_generation: noted,
                },
            )) if *seen == epoch => {
                if !stands || examined_generation > *noted {
                    self.examinations.insert(key, (epoch, Examination::Bound));
                    return true;
                }
            }
            _ if !stands => {
                self.examinations.insert(key, (epoch, Examination::Bound));
                return true;
            }
            _ => {
                self.examinations.insert(
                    key.clone(),
                    (
                        epoch,
                        Examination::Awaiting {
                            examined_generation,
                        },
                    ),
                );
            }
        }
        // Still standing and unexamined. Asking again is how a probe owner
        // that went away, or came back with nothing pending, is noticed; the
        // network coalesces requests for one peer.
        self.request_verification(peer);
        false
    }

    /// Forget the examinations of shards `keep` rejects.
    pub(super) fn retain_examinations(&mut self, keep: impl Fn(&ShardKey) -> bool) {
        self.examinations.retain(|(shard, _), _| keep(shard));
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
                    examined: current
                        .peers
                        .iter()
                        .filter(|(_, peer)| peer.examined_generation != 0)
                        .map(|(node, peer)| (node.clone(), peer.examined_generation))
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
                subscription.changed_or_verification_activity().await;
            }
            None => std::future::pending::<()>().await,
        }
    }

    /// Ask the network to verify `peer` now. False when this process has no
    /// connectivity probe owner, so no episode will start.
    pub(super) fn request_verification(&self, peer: &NodeId) -> bool {
        #[cfg(any(test, feature = "test-support"))]
        if test_seam::installed(&self.test) {
            return test_seam::record_verification_request(&self.test, peer);
        }
        sinomo_core::network::request_peer_verification(&self.cluster_paths, &self.state, peer)
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(super) mod test_seam {
    //! Test support: the verdicts of one data root, set by the test. With
    //! them installed nothing subscribes to the network, and a verification
    //! request is recorded instead of starting an episode. A root has no
    //! probe owner unless the test gives it one; with one, a request is
    //! accepted and the test settles the episode.

    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct State {
        generation: u64,
        peers: BTreeMap<NodeId, (PeerPathVerdict, bool)>,
        examined: BTreeMap<NodeId, u64>,
        // None uses the fixture's scripted link result for each requested
        // examination. Some(true) lets a test hold and complete each episode.
        probe_owner: Option<bool>,
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
            examined: state.examined.clone(),
        })
    }

    pub(super) fn generation(root: &Path) -> u64 {
        verdicts().get(root).map_or(0, |state| state.generation)
    }

    pub(super) fn wake(root: &Path) -> Arc<tokio::sync::Notify> {
        Arc::clone(&verdicts().entry(root.to_path_buf()).or_default().wake)
    }

    /// Record a request; whether the root has a probe owner to run it.
    pub(super) fn record_verification_request(root: &Path, peer: &NodeId) -> bool {
        verdicts().get_mut(root).is_some_and(|state| {
            state.verification_requests.push(peer.clone());
            if let Some(manual) = state.probe_owner {
                return manual;
            }
            if matches!(
                state.peers.get(peer),
                Some((PeerPathVerdict::Path | PeerPathVerdict::NoPath, false))
            ) {
                *state.examined.entry(peer.clone()).or_default() += 1;
                state.generation += 1;
                state.wake.notify_waiters();
                return true;
            }
            false
        })
    }

    /// Give `root` a probe owner, or take it away.
    pub(in crate::runtime_host) fn set_probe_owner(root: PathBuf, owner: bool) {
        let wake = {
            let mut verdicts = verdicts();
            let state = verdicts.entry(root).or_default();
            state.probe_owner = Some(owner);
            state.generation += 1;
            Arc::clone(&state.wake)
        };
        wake.notify_waiters();
    }

    /// Settle a verification episode of `peer` for `root` with `verdict`.
    pub(in crate::runtime_host) fn settle_peer_verification(
        root: PathBuf,
        peer: NodeId,
        verdict: PeerPathVerdict,
    ) {
        let wake = {
            let mut verdicts = verdicts();
            let state = verdicts.entry(root).or_default();
            state.peers.insert(peer.clone(), (verdict, false));
            *state.examined.entry(peer).or_default() += 1;
            state.generation += 1;
            Arc::clone(&state.wake)
        };
        wake.notify_waiters();
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

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        verdicts: DfsPeerVerdicts,
        root: std::path::PathBuf,
        shard: ShardKey,
        peer: NodeId,
        other: NodeId,
    }

    /// One node's verdicts, set by the test; nothing touches disk.
    fn fixture(name: &str, probe_owner: bool) -> Fixture {
        let data_dir = SinomoDataDir::new(format!("/nonexistent/sinomo-dfs-examination-{name}"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        let node = |name: &str| NodeId::from_legacy_named(name).unwrap();
        let now = time::OffsetDateTime::UNIX_EPOCH;
        let state = sinomo_core::ClusterBootstrapState {
            cluster_id: sinomo_core::Id::generate(),
            cluster_name: name.into(),
            local_node_id: node("local"),
            local_node_display_name: "local".into(),
            term: 1,
            kv_revision: 1,
            members: Vec::new(),
            initialized_at: now,
            updated_at: now,
        };
        let fixture = Fixture {
            verdicts: DfsPeerVerdicts::new(&data_dir, &cluster_paths, &state),
            root: data_dir.root(),
            shard: (
                crate::sifs_file_system_id(),
                crate::DfsMetadataShardId::new("services".to_string()),
            ),
            peer: node("peer"),
            other: node("other"),
        };
        test_seam::set_probe_owner(fixture.root.clone(), probe_owner);
        fixture
    }

    impl Fixture {
        fn set(&self, peer: &NodeId, verdict: PeerPathVerdict, verifying: bool) {
            test_seam::set_peer_verdict(self.root.clone(), peer.clone(), verdict, verifying);
        }

        fn examined(&mut self, epoch: u64) -> bool {
            let snapshot = self.verdicts.current();
            let (shard, peer) = (self.shard.clone(), self.peer.clone());
            self.verdicts
                .examined_under(&snapshot, &shard, epoch, &peer)
        }

        fn asked(&self) -> usize {
            test_seam::verification_requests(&self.root)
                .iter()
                .filter(|asked| **asked == self.peer)
                .count()
        }
    }

    #[test]
    fn a_verdict_that_turns_no_path_under_the_epoch_counts_as_it_is() {
        let mut fixture = fixture("turns", true);
        fixture.set(&fixture.peer, PeerPathVerdict::Path, false);
        assert!(fixture.examined(3), "nothing stands");
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(fixture.examined(3));
        assert_eq!(fixture.asked(), 0, "the network is asked nothing");
    }

    #[test]
    fn a_verdict_that_stood_when_the_epoch_was_first_read_waits_for_a_settled_episode() {
        let mut fixture = fixture("stood", true);
        fixture.set(&fixture.peer, PeerPathVerdict::Path, false);
        assert!(fixture.examined(3));
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        // The record moves to a new epoch while the verdict stands.
        assert!(!fixture.examined(4));
        assert_eq!(
            fixture.asked(),
            1,
            "the network is asked to verify the peer"
        );
        // A request settles nothing, however often the keeper evaluates.
        assert!(!fixture.examined(4));
        // Another peer's verdict and another peer's episode say nothing
        // about this one.
        fixture.set(&fixture.other, PeerPathVerdict::NoPath, false);
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.other.clone(),
            PeerPathVerdict::NoPath,
        );
        assert!(!fixture.examined(4));
        // The peer's own episode settles with no path: the loss is confirmed.
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.peer.clone(),
            PeerPathVerdict::NoPath,
        );
        assert!(fixture.examined(4));
        assert!(fixture.examined(4), "and stays so at this epoch");
        // The next epoch is read with the verdict standing again.
        assert!(!fixture.examined(5));
    }

    #[test]
    fn an_episode_that_finds_a_path_or_is_still_running_leaves_nothing_standing() {
        let mut fixture = fixture("path", true);
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(!fixture.examined(3));
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.peer.clone(),
            PeerPathVerdict::Path,
        );
        assert!(fixture.examined(3), "there is no no-path verdict to act on");
        assert_eq!(
            fixture.verdicts.current().reach(&fixture.peer),
            DfsWriteSetMemberReach::Path
        );

        // A running episode makes the verdict unsettled; whatever it settles
        // with is then a change under the epoch.
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(!fixture.examined(4));
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, true);
        assert!(!fixture.examined(4));
        assert_eq!(
            fixture.verdicts.current().reach(&fixture.peer),
            DfsWriteSetMemberReach::Unsettled
        );
    }

    #[test]
    fn losing_the_probe_owner_does_not_certify_a_standing_verdict() {
        let mut fixture = fixture("no-owner", false);
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(!fixture.examined(3));

        // A replacement owner must finish an examination of this peer.
        let mut fixture = self::fixture("owner-lost", true);
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(!fixture.examined(3));
        test_seam::set_probe_owner(fixture.root.clone(), false);
        assert!(!fixture.examined(3));
        test_seam::set_probe_owner(fixture.root.clone(), true);
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.peer.clone(),
            PeerPathVerdict::NoPath,
        );
        assert!(fixture.examined(3));
    }

    #[test]
    fn examinations_of_a_shard_this_node_left_are_forgotten() {
        let mut fixture = fixture("forgotten", true);
        fixture.set(&fixture.peer, PeerPathVerdict::Path, false);
        assert!(fixture.examined(3));
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(fixture.examined(3));
        fixture.verdicts.retain_examinations(|_| false);
        assert!(!fixture.examined(3), "the epoch is read anew");
    }
}
