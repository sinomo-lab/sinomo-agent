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
use std::time::{Duration, Instant};

/// The first re-send of a request that the network examine a peer, after an
/// episode for it settled without a conclusion; each later one doubles, up to
/// the network's own timeout. The timer only asks again: it never certifies.
pub(super) const EXAMINATION_RESEND_FIRST: Duration =
    super::write_set_takeover::TAKEOVER_RESEND_FIRST;

/// Every peer's verdict at one generation of this node's view.
#[derive(Clone, Debug, Default)]
pub(super) struct DfsPeerVerdictSnapshot {
    /// Advances whenever any peer's entry changes.
    pub(super) generation: u64,
    /// Advances whenever a verification episode starts or ends.
    pub(super) verification_generation: u64,
    pub(super) peers: BTreeMap<NodeId, (PeerPathVerdict, bool)>,
    /// Per peer, the network's examined generation: it changes each time a
    /// verification episode for the peer settles, whatever ended it. Absent
    /// while none has.
    pub(super) settled: BTreeMap<NodeId, u64>,
    /// Per peer, the network's concluded generation: it changes each time an
    /// episode's own probe concluded, with a direct edge that answered or
    /// with every one failed. Absent while none has.
    pub(super) concluded: BTreeMap<NodeId, u64>,
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

    /// The peer's settled generation; zero while no episode for it settled.
    /// It says an episode ended, not that it examined the peer.
    pub(super) fn settled_generation(&self, node: &NodeId) -> u64 {
        self.settled.get(node).copied().unwrap_or_default()
    }

    /// The peer's concluded generation; zero while no episode for it
    /// concluded. Only this one says the network examined the peer.
    pub(super) fn concluded_generation(&self, node: &NodeId) -> u64 {
        self.concluded.get(node).copied().unwrap_or_default()
    }
}

/// Whether a peer's no-path verdict is evidence gathered under one epoch of
/// a shard's record.
enum Examination {
    /// The verdict was something else when this node first read the epoch, or
    /// the network examined the peer since: a no-path verdict counts.
    Bound,
    /// The verdict already stood when this node first read the epoch, with
    /// this concluded generation. It counts once an episode for the peer has
    /// concluded since.
    Awaiting {
        concluded_generation: u64,
        request: ExaminationRequest,
        /// The delay before the next request once an episode settles without
        /// a conclusion.
        resend: Duration,
    },
}

/// What became of this node's request that the network examine a peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExaminationRequest {
    /// None is outstanding: none was made yet, or this process had no probe
    /// owner to take it. It is made on every evaluation; a probe owner's
    /// arrival is verification activity, which wakes the role.
    NotAccepted,
    /// Accepted with the peer at this settled generation, and no episode for
    /// the peer has settled since. It is made again on every evaluation: the
    /// network coalesces requests for one peer, and that is how a probe owner
    /// that went away and came back with nothing pending is noticed.
    Open { settled_generation: u64 },
    /// An episode for the peer settled without a conclusion. The network
    /// bounds an episode and starts none on its own, and nothing else may
    /// arrive to prompt this node, so it asks again at `at`.
    Resend { at: Instant },
}

/// How far a peer's verdict is from being evidence under a record's epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StandingVerdict {
    /// A no-path verdict counts; any other verdict is nothing to wait for.
    Counts,
    /// A no-path verdict that stood when the epoch was first read, with the
    /// examination this node asked for pending or running. Its settlement is
    /// an event.
    BeingExamined,
    /// A no-path verdict that stood when the epoch was first read, with
    /// nothing examining the peer: this process has no probe owner, or the
    /// last episode concluded nothing and the request waits to be re-sent.
    Unexamined,
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
    /// or to remove `peer` from its write set. See [`Self::standing_verdict`].
    ///
    /// Any other verdict is not this function's concern and returns true.
    pub(super) fn examined_under(
        &mut self,
        snapshot: &DfsPeerVerdictSnapshot,
        shard: &ShardKey,
        epoch: u64,
        peer: &NodeId,
    ) -> bool {
        self.standing_verdict(snapshot, shard, epoch, peer) == StandingVerdict::Counts
    }

    /// How far `peer`'s verdict in `snapshot` is from being evidence under
    /// `epoch` of `shard`'s record.
    ///
    /// A verdict that became no-path while this node already read that epoch
    /// is a reachability change under the record and counts as it is. One
    /// that already stood when the epoch was first read was formed before the
    /// record gave this node its role, so only the record changed: the
    /// network is asked to verify the peer, and the verdict counts once an
    /// episode for it has concluded since, from its own probe. An episode
    /// that settles any other way (cancelled, nothing to dial, no
    /// observation, out of rounds, or answered from recorded evidence)
    /// examined nothing and certifies nothing.
    ///
    /// The wait is for a conclusion. While an episode is pending or running
    /// its settlement is the event. After one that concluded nothing, no
    /// event is owed to this node, so the request is re-sent on a timer that
    /// doubles to the network timeout, without a limit on how often; the
    /// timer never certifies. Without a probe owner the request is made on
    /// every evaluation, and the owner's arrival wakes the role.
    pub(super) fn standing_verdict(
        &mut self,
        snapshot: &DfsPeerVerdictSnapshot,
        shard: &ShardKey,
        epoch: u64,
        peer: &NodeId,
    ) -> StandingVerdict {
        let stands = snapshot.entry(peer).0 == PeerPathVerdict::NoPath;
        let concluded_generation = snapshot.concluded_generation(peer);
        let key = (shard.clone(), peer.clone());
        let awaiting = match self.examinations.get(&key) {
            Some((seen, Examination::Bound)) if *seen == epoch => return StandingVerdict::Counts,
            Some((
                seen,
                Examination::Awaiting {
                    concluded_generation: noted,
                    request,
                    resend,
                },
            )) if *seen == epoch => Some((*noted, *request, *resend)),
            _ => None,
        };
        let examined = awaiting.is_some_and(|(noted, ..)| concluded_generation > noted);
        if !stands || examined {
            self.examinations.insert(key, (epoch, Examination::Bound));
            return StandingVerdict::Counts;
        }
        // On the first read of this epoch with the verdict standing, note
        // where the peer's examinations stand.
        let (noted, mut request, mut resend) = awaiting.unwrap_or((
            concluded_generation,
            ExaminationRequest::NotAccepted,
            EXAMINATION_RESEND_FIRST,
        ));
        // Still standing and unexamined.
        let settled_generation = snapshot.settled_generation(peer);
        let now = Instant::now();
        let ask = match request {
            ExaminationRequest::NotAccepted => true,
            ExaminationRequest::Open {
                settled_generation: asked,
            } if asked == settled_generation => true,
            ExaminationRequest::Open { .. } => {
                // An episode settled since, and concluded nothing.
                let at = now + resend;
                resend = resend
                    .saturating_mul(2)
                    .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
                request = ExaminationRequest::Resend { at };
                self.wake_at(at);
                false
            }
            ExaminationRequest::Resend { at } => now >= at,
        };
        if ask {
            request = if self.request_verification(peer) {
                ExaminationRequest::Open { settled_generation }
            } else {
                ExaminationRequest::NotAccepted
            };
        }
        self.examinations.insert(
            key,
            (
                epoch,
                Examination::Awaiting {
                    concluded_generation: noted,
                    request,
                    resend,
                },
            ),
        );
        match request {
            ExaminationRequest::Open { .. } => StandingVerdict::BeingExamined,
            ExaminationRequest::NotAccepted | ExaminationRequest::Resend { .. } => {
                StandingVerdict::Unexamined
            }
        }
    }

    /// Wake this node's keeper at `at`, so a request due then is re-sent. A
    /// role evaluated outside a runtime has no keeper to wake.
    fn wake_at(&self, at: Instant) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let local = self.state.local_node_id.clone();
        runtime.spawn(async move {
            tokio::time::sleep_until(at.into()).await;
            crate::write_set_keeping_signals::wake_write_set_keeper(&local);
        });
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
                    settled: current
                        .peers
                        .iter()
                        .filter(|(_, peer)| peer.examined_generation != 0)
                        .map(|(node, peer)| (node.clone(), peer.examined_generation))
                        .collect(),
                    concluded: current
                        .peers
                        .iter()
                        .filter(|(_, peer)| peer.concluded_generation != 0)
                        .map(|(node, peer)| (node.clone(), peer.concluded_generation))
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

/// Test support: the write-set roles' gate on a standing no-path verdict over
/// this process's real network verdicts. The owning test settles real
/// verification episodes and asks what the gate then says; the gate's own
/// requests reach the real verification registry.
#[cfg(any(test, feature = "test-support"))]
pub struct DfsStandingVerdictGate {
    verdicts: DfsPeerVerdicts,
    shard: ShardKey,
}

#[cfg(any(test, feature = "test-support"))]
impl DfsStandingVerdictGate {
    pub fn new(
        data_dir: &SinomoDataDir,
        cluster_paths: &ClusterPaths,
        state: &sinomo_core::ClusterBootstrapState,
    ) -> Self {
        Self {
            verdicts: DfsPeerVerdicts::new(data_dir, cluster_paths, state),
            shard: (
                crate::sifs_file_system_id(),
                crate::DfsMetadataShardId::new("standing-verdict-gate".to_string()),
            ),
        }
    }

    /// Whether `peer`'s verdict, read from the network now, may be acted on
    /// under `epoch` of a shard's record: the keeper's own judgement.
    pub fn counts_under(&mut self, epoch: u64, peer: &NodeId) -> bool {
        // A new subscription reads the network's current verdicts.
        self.verdicts.retain(false);
        self.verdicts.retain(true);
        let snapshot = self.verdicts.current();
        self.verdicts
            .examined_under(&snapshot, &self.shard, epoch, peer)
    }

    /// The first wait before the gate asks the network again after an
    /// episode that concluded nothing; each later one doubles.
    pub const RESEND_FIRST: Duration = EXAMINATION_RESEND_FIRST;
}

#[cfg(any(test, feature = "test-support"))]
pub(super) mod test_seam {
    //! Test support: the verdicts of one data root, set by the test. With
    //! them installed nothing subscribes to the network, and a verification
    //! request is recorded instead of starting an episode.
    //!
    //! An episode ends as the network's do: with a conclusion of its own
    //! probe ([`settle_peer_verification`], which also sets the verdict the
    //! probe found), or without one ([`end_peer_verification_unconcluded`],
    //! as a cancelled episode, one with nothing to dial, or one whose rounds
    //! produced no verdict). A root has no probe owner unless the test gives
    //! it one; with one, a request is accepted and stays open until the test
    //! ends the episode. Left unset, each request for a peer whose verdict
    //! the test has set is answered at once by a probe that concludes with
    //! that verdict.

    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct State {
        generation: u64,
        peers: BTreeMap<NodeId, (PeerPathVerdict, bool)>,
        /// The source of the two maps below: one increasing value per ended
        /// episode, as in the network's registry.
        episodes: u64,
        settled: BTreeMap<NodeId, u64>,
        concluded: BTreeMap<NodeId, u64>,
        // None uses the fixture's scripted link result for each requested
        // examination. Some(true) lets a test hold and complete each episode.
        probe_owner: Option<bool>,
        verification_requests: Vec<NodeId>,
        wake: Arc<tokio::sync::Notify>,
    }

    impl State {
        /// One episode of `peer` ends; `concluded` when its own probe did.
        fn end_episode(&mut self, peer: &NodeId, concluded: bool) {
            self.episodes += 1;
            self.settled.insert(peer.clone(), self.episodes);
            if concluded {
                self.concluded.insert(peer.clone(), self.episodes);
            }
            self.generation += 1;
        }
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
            settled: state.settled.clone(),
            concluded: state.concluded.clone(),
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
                // The scripted link answers the probe: it concludes with the
                // verdict the test set.
                state.end_episode(peer, true);
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

    /// Settle a verification episode of `peer` for `root` whose own probe
    /// concluded with `verdict`: an edge answered, or every one failed.
    pub(in crate::runtime_host) fn settle_peer_verification(
        root: PathBuf,
        peer: NodeId,
        verdict: PeerPathVerdict,
    ) {
        let wake = {
            let mut verdicts = verdicts();
            let state = verdicts.entry(root).or_default();
            state.peers.insert(peer.clone(), (verdict, false));
            state.end_episode(&peer, true);
            Arc::clone(&state.wake)
        };
        wake.notify_waiters();
    }

    /// End a verification episode of `peer` for `root` that concluded
    /// nothing, as a cancelled episode, one with nothing to dial, or one
    /// whose rounds produced no verdict does. The peer's verdict stays what
    /// it was and is no longer being verified.
    pub(in crate::runtime_host) fn end_peer_verification_unconcluded(root: PathBuf, peer: NodeId) {
        let wake = {
            let mut verdicts = verdicts();
            let state = verdicts.entry(root).or_default();
            if let Some((_, verifying)) = state.peers.get_mut(&peer) {
                *verifying = false;
            }
            state.end_episode(&peer, false);
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
            self.standing(epoch) == StandingVerdict::Counts
        }

        fn standing(&mut self, epoch: u64) -> StandingVerdict {
            let snapshot = self.verdicts.current();
            let (shard, peer) = (self.shard.clone(), self.peer.clone());
            self.verdicts
                .standing_verdict(&snapshot, &shard, epoch, &peer)
        }

        /// An episode of the peer ends as a cancelled or inconclusive one
        /// does, or one with nothing to dial.
        fn end_unconcluded(&self) {
            test_seam::end_peer_verification_unconcluded(self.root.clone(), self.peer.clone());
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
    fn a_verdict_that_stood_when_the_epoch_was_first_read_waits_for_a_concluded_episode() {
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
        // The peer's own episode concludes with no path: the loss is
        // confirmed.
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
    fn an_episode_that_concludes_nothing_certifies_nothing_and_is_asked_for_again() {
        let mut fixture = fixture("unconcluded", true);
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert_eq!(fixture.standing(3), StandingVerdict::BeingExamined);
        assert_eq!(fixture.asked(), 1);

        // The episode is cancelled, or had nothing to dial, or its rounds
        // produced no verdict: it settled, and examined nothing.
        fixture.end_unconcluded();
        assert_eq!(fixture.standing(3), StandingVerdict::Unexamined);
        assert_eq!(
            fixture.asked(),
            1,
            "the request waits for its re-send, however often the keeper evaluates"
        );
        assert_eq!(fixture.standing(3), StandingVerdict::Unexamined);
        assert_eq!(fixture.asked(), 1);

        // The re-send asks again; nothing but a conclusion ends the wait.
        let margin = Duration::from_millis(40);
        std::thread::sleep(EXAMINATION_RESEND_FIRST + margin);
        assert_eq!(fixture.standing(3), StandingVerdict::BeingExamined);
        assert_eq!(fixture.asked(), 2);

        // A second episode that concludes nothing waits twice as long.
        fixture.end_unconcluded();
        assert_eq!(fixture.standing(3), StandingVerdict::Unexamined);
        std::thread::sleep(EXAMINATION_RESEND_FIRST + margin);
        assert_eq!(fixture.standing(3), StandingVerdict::Unexamined);
        assert_eq!(fixture.asked(), 2);
        std::thread::sleep(EXAMINATION_RESEND_FIRST);
        assert_eq!(fixture.standing(3), StandingVerdict::BeingExamined);
        assert_eq!(fixture.asked(), 3);

        // An episode whose own probe finds no path certifies the verdict.
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.peer.clone(),
            PeerPathVerdict::NoPath,
        );
        assert_eq!(fixture.standing(3), StandingVerdict::Counts);
    }

    #[test]
    fn a_conclusion_reached_while_the_request_waits_to_be_re_sent_certifies() {
        let mut fixture = fixture("concluded-meanwhile", true);
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        assert!(!fixture.examined(3));
        fixture.end_unconcluded();
        assert_eq!(fixture.standing(3), StandingVerdict::Unexamined);
        // Another requester's episode for the peer concludes before this
        // node's re-send is due.
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.peer.clone(),
            PeerPathVerdict::NoPath,
        );
        assert!(fixture.examined(3));
        assert_eq!(fixture.asked(), 1, "nothing more is asked");
    }

    #[test]
    fn an_unconcluded_episode_before_the_epoch_was_read_is_not_waited_out() {
        let mut fixture = fixture("unconcluded-before", true);
        fixture.set(&fixture.peer, PeerPathVerdict::NoPath, false);
        // An episode that concluded nothing ended before this node read the
        // epoch: the first read asks at once.
        fixture.end_unconcluded();
        assert_eq!(fixture.standing(3), StandingVerdict::BeingExamined);
        assert_eq!(fixture.asked(), 1);
        test_seam::settle_peer_verification(
            fixture.root.clone(),
            fixture.peer.clone(),
            PeerPathVerdict::NoPath,
        );
        assert!(fixture.examined(3));
        // That conclusion came before the next epoch was read: it is not one
        // under it.
        assert!(!fixture.examined(4), "the next epoch is read anew");
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
