//! Promises of a converting takeover, keyed by the shard, its epoch and its
//! lost primary.
//!
//! A candidate asks each member of its quorum on `dfs-migration` object RPC
//! (and itself, locally) to promise it. A member answers from one exact-set
//! read of the shard's serving, write-set and storage records, its members'
//! records and its own verdicts:
//!
//! - it refuses while the records no longer name the request's epoch and
//!   lost primary, the storage is Active, or the candidate or this member is
//!   not one the shard's quorum admits;
//! - once a takeover attempt is recorded at the epoch it promises only that
//!   attempt's converter;
//! - it vetoes while it does not judge the primary lost: it still reaches it,
//!   or its verdict is not settled;
//! - otherwise it promises the best-ranked candidate it knows of that is not
//!   lost, itself included once it has ranked its own copy, and refuses every
//!   other, naming the one that outranks it. Until it has captured its own
//!   copy it refuses as unranked.
//!
//! What a member learns (candidates and their ranks, its own rank, the
//! candidate it promises) is persisted under
//! `dfs/migration-owner/shard-takeovers/<fs>/<shard>.json` before it
//! replies; a new epoch or lost primary starts a new record. When an answer
//! changes that record the member wakes its own `Metadata{shard}` unit, which
//! may itself be the best-ranked candidate; repeating a question wakes
//! nobody.
//!
//! **Exclusivity and release.** A member promises one candidate at a time.
//! Its promise moves only to a candidate ranked above the one it promised, or
//! away from one it judges lost; without a loss the moves only go up a total
//! order of finitely many candidates, so they end. A candidate that dies
//! before its takeover is recorded is released this way. A stale quorum can
//! still write: two candidates that both counted one member's promise both
//! submit, the storage record's exact compare commits one of them, and the
//! builder never lets a recorded takeover be replaced at its epoch. So at most
//! one takeover attempt is recorded per epoch, attempts do not replace each
//! other, and promising for the epoch ends there.
use super::*;
use sinomo_core::node::NodeId;
use std::cmp::Ordering;

pub(in crate::authority) const ROUTE: &str = "/dfs-migration/shard-takeover-promise-v1";
pub(in crate::authority) const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_REPLY_BYTES: usize = 64 * 1024;
const STATE_VERSION: u16 = 1;
const MAX_STATE_BYTES: u64 = 1024 * 1024;

/// This node's settled verdict of its path to a peer, as the write-set
/// takeover planner reads it: a verdict that is being verified is unsettled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::authority) enum PeerReach {
    Path,
    NoPath,
    Unsettled,
}

/// A candidate's rank: the journal sequence of the source it captured of its
/// own copy, the most advanced first, ties to the smaller node id.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct TakeoverRank {
    pub node_id: NodeId,
    pub source_journal_sequence: u64,
}

impl TakeoverRank {
    /// The rank order: greater is better.
    pub(in crate::authority) fn cmp_rank(&self, other: &Self) -> Ordering {
        self.source_journal_sequence
            .cmp(&other.source_journal_sequence)
            .then_with(|| other.node_id.cmp(&self.node_id))
    }
}

/// Order candidates best first.
#[cfg(test)]
pub(in crate::authority) fn rank_takeover_candidates(
    mut ranks: Vec<TakeoverRank>,
) -> Vec<TakeoverRank> {
    ranks.sort_by(|left, right| right.cmp_rank(left));
    ranks
}

/// Who may convert a closed shard in its lost primary's place, and whose
/// promises it needs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::authority) enum TakeoverQuorum {
    /// A switched shard: the write set without the lost primary, every one
    /// of whom promises.
    WriteSet { members: BTreeSet<NodeId> },
    /// An unswitched shard: the configured members without the lost
    /// primary; a majority of all configured members promises.
    Majority {
        primary: NodeId,
        configured: BTreeSet<NodeId>,
    },
}

impl TakeoverQuorum {
    pub(in crate::authority) fn for_shard(
        shard: &DfsMetadataShardConfig,
        write_set: Option<&KvEntry>,
    ) -> Result<Self> {
        Ok(
            match DfsMetadataShardConversionInstallRule::for_shard(shard, write_set)
                .map_err(error)?
            {
                DfsMetadataShardConversionInstallRule::WriteSet { mut required } => {
                    required.remove(&shard.primary_node_id);
                    Self::WriteSet { members: required }
                }
                DfsMetadataShardConversionInstallRule::PrimaryAndMajority { .. } => {
                    Self::Majority {
                        primary: shard.primary_node_id.clone(),
                        configured: configured_members(shard),
                    }
                }
            },
        )
    }

    /// The candidates, each of which is also asked to promise.
    pub(in crate::authority) fn members(&self) -> BTreeSet<NodeId> {
        match self {
            Self::WriteSet { members } => members.clone(),
            Self::Majority {
                primary,
                configured,
            } => configured
                .iter()
                .filter(|node| *node != primary)
                .cloned()
                .collect(),
        }
    }

    pub(in crate::authority) fn satisfied(&self, promised: &BTreeSet<NodeId>) -> bool {
        match self {
            Self::WriteSet { members } => !members.is_empty() && members.is_subset(promised),
            Self::Majority { configured, .. } => {
                self.members().intersection(promised).count() > configured.len() / 2
            }
        }
    }
}

/// The shard's primary and replicas.
fn configured_members(shard: &DfsMetadataShardConfig) -> BTreeSet<NodeId> {
    std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct PromiseRequest {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub epoch: u64,
    pub lost_primary: NodeId,
    pub candidate: TakeoverRank,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct PromiseResponse {
    pub member: NodeId,
    pub epoch: u64,
    pub candidate: NodeId,
    pub answer: PromiseAnswer,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "answer", rename_all = "kebab-case", deny_unknown_fields)]
pub(in crate::authority) enum PromiseAnswer {
    Promised,
    Refused { refusal: Refusal },
}

/// Why a member does not promise a candidate.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "refusal", rename_all = "kebab-case", deny_unknown_fields)]
pub(in crate::authority) enum Refusal {
    /// The member still reaches the primary: a veto.
    ReachesPrimary,
    /// The member's verdict of the primary is not settled.
    PrimaryUnsettled,
    /// The shard's records do not admit this takeover now.
    Stale { reason: String },
    /// A takeover attempt is recorded at this epoch; its converter is final.
    TakeoverRecorded { converter: NodeId },
    /// The member has not ranked its own copy yet.
    Unranked,
    /// A candidate that is not lost ranks above this one.
    Outranked { by: TakeoverRank },
}

impl Refusal {
    /// The node whose verdict can change this refusal, besides the primary.
    pub(in crate::authority) fn names(&self) -> Option<NodeId> {
        match self {
            Self::TakeoverRecorded { converter } => Some(converter.clone()),
            Self::Outranked { by } => Some(by.node_id.clone()),
            _ => None,
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReachesPrimary => write!(f, "reaches the shard's primary"),
            Self::PrimaryUnsettled => write!(f, "has no settled verdict of the shard's primary"),
            Self::Stale { reason } => write!(f, "does not admit the takeover: {reason}"),
            Self::TakeoverRecorded { converter } => {
                write!(f, "a converting takeover by {converter} is recorded")
            }
            Self::Unranked => write!(f, "has not ranked its own copy"),
            Self::Outranked { by } => write!(
                f,
                "knows {} at journal sequence {}, which ranks higher",
                by.node_id, by.source_journal_sequence
            ),
        }
    }
}

/// A member's answer as the candidate sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::authority) enum PromiseReply {
    Promised,
    Refused(Refusal),
    /// The member could not be asked or did not answer this request.
    Unavailable {
        reason: String,
    },
}

impl From<PromiseAnswer> for PromiseReply {
    fn from(answer: PromiseAnswer) -> Self {
        match answer {
            PromiseAnswer::Promised => Self::Promised,
            PromiseAnswer::Refused { refusal } => Self::Refused(refusal),
        }
    }
}

/// What one member knows of one takeover key.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct PromiseState {
    pub version: u16,
    pub epoch: u64,
    pub lost_primary: Option<NodeId>,
    /// This member's own rank, once it captured its own copy.
    pub own: Option<u64>,
    /// Every other candidate it heard from, with its rank.
    pub known: BTreeMap<NodeId, u64>,
    /// The candidate it last promised.
    pub promised: Option<NodeId>,
}

impl PromiseState {
    /// This state if it is for `epoch` and `lost_primary`, or a new one.
    pub(in crate::authority) fn for_key(self, epoch: u64, lost_primary: &NodeId) -> Self {
        if self.version == STATE_VERSION
            && self.epoch == epoch
            && self.lost_primary.as_ref() == Some(lost_primary)
        {
            return self;
        }
        Self {
            version: STATE_VERSION,
            epoch,
            lost_primary: Some(lost_primary.clone()),
            ..Self::default()
        }
    }
}

/// Decide a request on member `local` whose records admit it and which
/// judges the primary lost: promise the best-ranked candidate known that is
/// not `lost`, this member included once ranked, and refuse every other. The
/// requesting candidate is known from its own request and never judged lost.
pub(in crate::authority) fn decide_promise(
    state: &mut PromiseState,
    local: &NodeId,
    candidate: &TakeoverRank,
    lost: impl Fn(&NodeId) -> bool,
) -> std::result::Result<(), Refusal> {
    if candidate.node_id != *local {
        state
            .known
            .insert(candidate.node_id.clone(), candidate.source_journal_sequence);
    }
    let Some(own) = state.own else {
        return Err(Refusal::Unranked);
    };
    let best = state
        .known
        .iter()
        .filter(|(node, _)| *node == &candidate.node_id || !lost(node))
        .map(|(node, sequence)| TakeoverRank {
            node_id: node.clone(),
            source_journal_sequence: *sequence,
        })
        .chain(std::iter::once(TakeoverRank {
            node_id: local.clone(),
            source_journal_sequence: own,
        }))
        .max_by(TakeoverRank::cmp_rank)
        .expect("the member itself is ranked");
    if best.node_id != candidate.node_id {
        return Err(Refusal::Outranked { by: best });
    }
    state.promised = Some(candidate.node_id.clone());
    Ok(())
}

/// Serializes every member's read-modify-write of its promise records.
static PROMISES: Mutex<()> = Mutex::new(());

fn state_path(
    data_root: &Path,
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> Result<PathBuf> {
    let file_system = file_system_id.to_string();
    for segment in [file_system.as_str(), shard_id] {
        let path = Path::new(segment);
        if segment.is_empty()
            || segment.contains(['\\', ':', '\0'])
            || path.components().count() != 1
            || !matches!(
                path.components().next(),
                Some(std::path::Component::Normal(_))
            )
        {
            return Err(error("a takeover promise names an invalid path segment"));
        }
    }
    Ok(data_root
        .join("dfs/migration-owner/shard-takeovers")
        .join(file_system)
        .join(format!("{shard_id}.json")))
}

fn read_state(path: &Path) -> Result<PromiseState> {
    if !path.try_exists().map_err(error)? {
        return Ok(PromiseState::default());
    }
    let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(path, MAX_STATE_BYTES)
        .map_err(error)?;
    let state: PromiseState = serde_json::from_slice(&bytes).map_err(error)?;
    if state.version != STATE_VERSION {
        return Err(error("a takeover promise record has an unknown format"));
    }
    Ok(state)
}

fn write_state(path: &Path, state: &PromiseState) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| error("a takeover promise record has no parent"))?;
    if !parent.is_dir() {
        std::fs::create_dir_all(parent).map_err(error)?;
        if let Some(grandparent) = parent.parent() {
            sinomo_core::storage::durable_file::sync_directory(grandparent).map_err(error)?;
        }
    }
    sinomo_core::storage::durable_file::write_file_atomic(
        path,
        &serde_json::to_vec(state).map_err(error)?,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("takeover", "tmp")
            .sync_all(),
    )
    .map_err(error)
}

/// The member records of `nodes`, by node, for loss verdicts.
struct MemberRecords(BTreeMap<NodeId, Option<sinomo_core::ClusterMemberRecord>>);

impl MemberRecords {
    fn departed(&self, node: &NodeId) -> bool {
        self.0
            .get(node)
            .and_then(Option::as_ref)
            .is_none_or(|record| record.node_id != *node || !record.state.is_authorized())
    }
}

impl NativeImportAuthority {
    /// This node's settled verdict of its path to `node`, from the network's
    /// per-peer verdicts.
    pub(in crate::authority) fn takeover_reach(&self, node: &NodeId) -> PeerReach {
        #[cfg(test)]
        if let Some(reach) = verdicts_for_test::get(&self.data_root, node) {
            return reach;
        }
        let paths = sinomo_core::network::ClusterPaths::new(self.component_context.data_dir());
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) else {
            return PeerReach::Unsettled;
        };
        match sinomo_core::network::peer_reachability_snapshot(&paths, &state)
            .ok()
            .and_then(|snapshot| snapshot.peers.get(node).cloned())
        {
            Some(peer)
                if !peer.verifying
                    && peer.verdict == sinomo_core::network::PeerPathVerdict::Path =>
            {
                PeerReach::Path
            }
            Some(peer)
                if !peer.verifying
                    && peer.verdict == sinomo_core::network::PeerPathVerdict::NoPath =>
            {
                PeerReach::NoPath
            }
            _ => PeerReach::Unsettled,
        }
    }

    /// Ask the network to verify `node` now, so a verdict from edges that
    /// have not aged out cannot keep vetoing a primary that died.
    fn verify_takeover_peer(&self, node: &NodeId) {
        #[cfg(test)]
        if verdicts_for_test::get(&self.data_root, node).is_some() {
            return;
        }
        let paths = sinomo_core::network::ClusterPaths::new(self.component_context.data_dir());
        if let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) {
            sinomo_core::network::request_peer_verification(&paths, &state, node);
        }
    }

    /// Record this node's own rank under `request`'s key.
    pub(in crate::authority) fn record_takeover_rank(
        &self,
        request: &PromiseRequest,
    ) -> Result<()> {
        let path = state_path(&self.data_root, &request.file_system_id, &request.shard_id)?;
        let _guard = PROMISES.lock().map_err(error)?;
        let before = read_state(&path)?;
        let mut state = before.clone().for_key(request.epoch, &request.lost_primary);
        state.own = Some(request.candidate.source_journal_sequence);
        if state != before {
            write_state(&path, &state)?;
        }
        Ok(())
    }

    /// The promise records of `request`'s shard on this node, for tests and
    /// diagnostics.
    #[cfg(test)]
    pub(in crate::authority) fn takeover_promises(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<PromiseState>> {
        let path = state_path(&self.data_root, file_system_id, shard_id)?;
        path.try_exists()
            .map_err(error)?
            .then(|| read_state(&path))
            .transpose()
    }

    /// Answer `caller`'s promise request on member `local`.
    /// sinomo:kv-consistency: A member answers a converting-takeover promise from exact-set reads of its shard's serving, write-set and storage records and of its configured members' records.
    pub(in crate::authority) fn answer_takeover_promise(
        &self,
        local: &NodeId,
        caller: &NodeId,
        request: &PromiseRequest,
    ) -> Result<PromiseAnswer> {
        if *caller != request.candidate.node_id {
            return Err(error(
                "a takeover promise is asked only by its own candidate",
            ));
        }
        let fs = &request.file_system_id;
        let id = request.shard_id.as_str();
        let path = state_path(&self.data_root, fs, id)?;
        let refused = |refusal| Ok(PromiseAnswer::Refused { refusal });
        let stale = |reason: &str| {
            refused(Refusal::Stale {
                reason: reason.into(),
            })
        };
        let (entries, _) = self.read_exact(&[
            sinomo_dfs::metadata_shard_key(fs, id),
            sinomo_dfs::metadata_shard_write_set_key(fs, id),
            sinomo_dfs::metadata_shard_storage_key(fs, id),
        ])?;
        let Some(shard_entry) = entries[0].entry.as_ref() else {
            return stale("the shard has no serving record");
        };
        let shard: DfsMetadataShardConfig = shard_entry.decode().map_err(error)?;
        if shard.file_system_id != *fs || shard.shard_id != id {
            return Err(error("the serving record names another shard"));
        }
        let write_set = entries[1].entry.as_ref();
        let storage = entries[2]
            .entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardStorage>().map_err(error))
            .transpose()?;
        if let Some(record) = &storage {
            record.validate_against(&shard)?;
        }
        if shard.epoch != request.epoch || shard.primary_node_id != request.lost_primary {
            return stale("the shard's serving record moved");
        }
        if storage
            .as_ref()
            .is_some_and(DfsMetadataShardStorage::is_active)
        {
            return stale("the shard's storage is active");
        }
        let quorum = TakeoverQuorum::for_shard(&shard, write_set)?;
        let members = quorum.members();
        if !members.contains(&request.candidate.node_id) || !members.contains(local) {
            return stale("the candidate or this member is outside the shard's takeover quorum");
        }
        if let Some(recorded) = super::recorded_takeover(&shard, write_set, storage.as_ref()) {
            return if recorded.primary_node_id == request.candidate.node_id {
                Ok(PromiseAnswer::Promised)
            } else {
                refused(Refusal::TakeoverRecorded {
                    converter: recorded.primary_node_id.clone(),
                })
            };
        }
        let configured = configured_members(&shard).into_iter().collect::<Vec<_>>();
        let member_keys = configured
            .iter()
            .map(sinomo_core::node::cluster_member_key)
            .collect::<Vec<_>>();
        let (records, _) = self.read_exact(&member_keys)?;
        let records = MemberRecords(
            configured
                .into_iter()
                .zip(records)
                .map(|(node, row)| {
                    let record = row
                        .entry
                        .as_ref()
                        .map(|entry| {
                            entry
                                .decode::<sinomo_core::ClusterMemberRecord>()
                                .map_err(error)
                        })
                        .transpose()?;
                    Ok((node, record))
                })
                .collect::<Result<_>>()?,
        );
        // A candidate the shard no longer admits counts as lost; the
        // verdicts are read before the promise records are locked.
        let judged_lost = records
            .0
            .keys()
            .filter(|node| {
                *node != local
                    && (records.departed(node) || self.takeover_reach(node) == PeerReach::NoPath)
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let lost = |node: &NodeId| {
            node != local && (!members.contains(node) || judged_lost.contains(node))
        };
        let primary_reach = self.takeover_reach(&shard.primary_node_id);
        let primary_lost =
            records.departed(&shard.primary_node_id) || primary_reach == PeerReach::NoPath;
        if !primary_lost {
            self.verify_takeover_peer(&shard.primary_node_id);
        }
        let (changed, decision) = {
            let _guard = PROMISES.lock().map_err(error)?;
            let before = read_state(&path)?;
            let mut state = before.clone().for_key(request.epoch, &request.lost_primary);
            let decision = if primary_lost {
                decide_promise(&mut state, local, &request.candidate, lost)
            } else {
                if request.candidate.node_id != *local {
                    state.known.insert(
                        request.candidate.node_id.clone(),
                        request.candidate.source_journal_sequence,
                    );
                }
                Err(if primary_reach == PeerReach::Path {
                    Refusal::ReachesPrimary
                } else {
                    Refusal::PrimaryUnsettled
                })
            };
            let changed = state != before;
            if changed {
                write_state(&path, &state)?;
            }
            (changed, decision)
        };
        if changed && *caller != *local {
            // This member may itself be the candidate the ranking picks.
            self.wake_takeover_unit(id)?;
        }
        Ok(match decision {
            Ok(()) => PromiseAnswer::Promised,
            Err(refusal) => PromiseAnswer::Refused { refusal },
        })
    }

    /// The `dfs-migration` route's answer to `caller`.
    pub(in crate::authority) fn receive_takeover_promise(
        &self,
        caller: &NodeId,
        request: PromiseRequest,
    ) -> Result<PromiseResponse> {
        let local = self.local_node()?;
        let answer = self.answer_takeover_promise(&local, caller, &request)?;
        Ok(PromiseResponse {
            member: local,
            epoch: request.epoch,
            candidate: request.candidate.node_id,
            answer,
        })
    }

    /// Ask `member` to promise `request`'s candidate. A member that cannot be
    /// asked, fails, or answers another request is unavailable.
    pub(in crate::authority) fn ask_takeover_promise(
        &self,
        member: &NodeId,
        request: &PromiseRequest,
    ) -> PromiseReply {
        let asked = (|| -> Result<PromiseResponse> {
            let transport = self
                .transport
                .as_ref()
                .ok_or_else(|| error("native component object-RPC transport is unavailable"))?;
            let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                ROUTE,
                "dfs-migration",
                request,
            )
            .map_err(error)?;
            let result = self.read_peer(transport.call_async(
                member,
                call,
                source_effect_rpc_options(),
                true,
            ))?;
            let response =
                self.read_peer(result.into_materialized_value_response(MAX_REPLY_BYTES))?;
            response.decode_body().map_err(error)
        })();
        match asked {
            Ok(response)
                if response.member == *member
                    && response.epoch == request.epoch
                    && response.candidate == request.candidate.node_id =>
            {
                response.answer.into()
            }
            Ok(_) => PromiseReply::Unavailable {
                reason: "the member answered another request".into(),
            },
            Err(failure) => PromiseReply::Unavailable {
                reason: failure.to_string(),
            },
        }
    }

    /// Requeue this node's `Metadata{shard}` unit.
    fn wake_takeover_unit(&self, shard_id: &str) -> Result<()> {
        let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
            self.component_context.data_dir(),
            self.component_context.host_role(),
        );
        let Some(owner) = OWNERS
            .get()
            .and_then(|owners| owners.lock().ok()?.get(&paths.root).and_then(Weak::upgrade))
        else {
            return Ok(());
        };
        let unit = inventory::UnitKey {
            relative_root: Path::new("dfs/metadata").join(shard_id),
            role: inventory::UnitRole::Metadata {
                shard: sinomo_dfs::DfsMetadataShardId::new(shard_id),
            },
        };
        unit.validate()?;
        owner
            .work
            .scheduler
            .lock()
            .map_err(error)?
            .enqueue(unit, false)?;
        owner.work.wake.notify_one();
        Ok(())
    }
}

#[cfg(test)]
pub(in crate::authority) mod verdicts_for_test {
    //! Test support: the verdicts one data root judges its peers by. With
    //! any installed, nothing else is consulted for that root.
    use super::*;
    use std::sync::LazyLock;

    static VERDICTS: LazyLock<Mutex<BTreeMap<PathBuf, BTreeMap<NodeId, PeerReach>>>> =
        LazyLock::new(Default::default);

    /// Set the verdict `data_root` has of `peer`.
    pub(in crate::authority) fn set(data_root: &Path, peer: &NodeId, reach: PeerReach) {
        VERDICTS
            .lock()
            .unwrap_or_else(|failure| failure.into_inner())
            .entry(data_root.canonicalize().expect("a live data root"))
            .or_default()
            .insert(peer.clone(), reach);
    }

    pub(super) fn get(data_root: &Path, peer: &NodeId) -> Option<PeerReach> {
        VERDICTS
            .lock()
            .unwrap_or_else(|failure| failure.into_inner())
            .get(data_root)
            .map(|peers| peers.get(peer).copied().unwrap_or(PeerReach::Unsettled))
    }
}
