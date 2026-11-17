//! Per-shard conversion of one metadata shard, driven by the shard's current
//! primary from its own copy.
//!
//! An attempt is fixed by the shard's storage identity, the serving record it
//! runs under (epoch and primary) and the facts of the primary's captured
//! source. It is recorded Converting before any other shard's contributions
//! are read, so those contributions are frozen while they are read; it is
//! sealed with the built image and the digest of the accepted contributions,
//! installed on the members its install rule names, and activated with one
//! transaction on the shard's own keys. Nothing compares or writes the
//! filesystem configuration or any other shard's keys.
//!
//! Every step is bounded and resumes from the attempt's files. A step that
//! cannot advance parks the unit on the KV keys and nodes it waits for and
//! returns; nothing here owns a timer. Every transaction request is saved
//! before it is submitted and is resubmitted byte for byte, because its
//! request identity is derived from the attempt: a lost outcome is settled by
//! the same request, and a definite rejection re-reads the shard's keys and
//! plans again.
//!
//! A serving change that moves the epoch or primary supersedes the attempt:
//! its driver deletes the attempt's files, and the shard's new primary starts
//! a new attempt from its own copy, whose Converting record replaces the
//! stale one.
//!
//! A closed shard whose primary is lost is converted by a member instead
//! ([`takeover`]): the same steps from that member's own copy, under a
//! promise quorum, with an activation that also makes it primary.
use super::*;
use crate::authority::contribution_source::{
    ContributionSource, SourceContributionAnswer, SourceContributionRead,
};
use crate::authority::staged_service::conversion_install::{ConversionSourceCut, source_cut_digest};
use crate::metadata::canonical_generation::CapturedMetadataUnit;
use crate::namespace_counts::{
    NamespaceCountMaintenanceLimits, NamespaceCountMaintenanceSource, ShardCountContributions,
};
use sinomo_core::node::NodeId;
use sinomo_dfs::DfsMetadataConversionTakeover;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataConversionSource, DfsMetadataGenerationSeal,
    DfsMetadataShardConversion, DfsMetadataShardConversionInstallRule, DfsMetadataShardStorage,
    DfsMetadataShardStorageCut, DfsMetadataShardStorageMember, DfsNamespaceJobFrame,
    DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
};
use std::future::Future;

mod pairs;

pub(in crate::authority) mod takeover;

const ATTEMPT_VERSION: u16 = 1;
/// The captured source facts of the attempt, as members receive them.
const SOURCE_CUT: &str = "source-cut.json";
/// The shard's share of the original placement goals the attempt's capture
/// binds.
const PLACEMENT_EVIDENCE: &str = "placement-evidence.json";
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// One shard's capture and count inputs are held in memory while its image
/// is built.
const COUNT_LIMITS: NamespaceCountMaintenanceLimits = NamespaceCountMaintenanceLimits {
    max_objects: 1_000_000,
    max_encoded_bytes: 64 * 1024 * 1024,
    deadline: None,
};
/// Definite rejections re-planned within one step before it yields.
const REPLANS_PER_STEP: usize = 2;

/// The outcome of one bounded step of a shard's conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::authority) enum ConversionProgress {
    /// The shard's storage is Active for this node's attempt and the local
    /// copy is activated.
    Ready,
    /// The shard's storage is Active under an attempt this node does not
    /// hold, or under native genesis. The local copy is activated by its own
    /// installed image or repaired by snapshot; this driver has nothing left.
    Handover,
    /// The step advanced; run the unit again.
    Pending { processed_records: usize },
    /// The step cannot advance. The unit is parked on the keys and nodes the
    /// step registered; `evidence` says what it waits for.
    Parked { evidence: String },
}

/// The unit state one step leaves, as the owner records it: a parked
/// conversion waits on the keys and nodes it registered and owns no retry
/// timer; only an error from a step takes the owner's bounded retry.
#[cfg(test)]
pub(super) fn observed_state(progress: &ConversionProgress) -> inventory::ObservedState {
    use inventory::ObservedState;
    match progress {
        ConversionProgress::Ready => ObservedState::VerifiedCurrent,
        ConversionProgress::Pending { processed_records } if *processed_records > 0 => ObservedState::Runnable,
        ConversionProgress::Pending { .. } => ObservedState::Waiting {
            evidence: "shard conversion awaits its dependencies".into(),
        },
        ConversionProgress::Handover => ObservedState::Waiting {
            evidence: "the shard's storage is active; the local copy activates its own installed image or repairs by snapshot".into(),
        },
        ConversionProgress::Parked { evidence } => ObservedState::Waiting { evidence: evidence.clone() },
    }
}

/// A conversion that cannot continue until an operator repairs the shard or
/// a later phase supplies the missing settlement. It names the shard and the
/// cause; the shard keeps its retained source and stays closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::authority) enum ConversionStop {
    /// The captured source changed under a recorded attempt of this primary.
    SourceChangedUnderAttempt { attempt_id: String },
    /// The captured image still has original work no current family owns.
    UnresolvedOriginalWork { references: Vec<String> },
    /// A legacy cross-shard hardlink job of this shard whose alias another
    /// shard owns: the existing settlement reads the alias shard's rows.
    LegacyHardlinkNeedsAliasShard { job_id: String, alias_shard: String },
    /// A legacy cross-shard hardlink job whose recorded rows settle it in
    /// neither direction.
    LegacyHardlinkUnsettled { job_id: String, reason: String },
    /// A count-less legacy directory move: no settlement exists yet.
    CountlessDirectoryMove { job_id: String },
    /// Another non-Completed job coordinated by this shard.
    UnsettledNamespaceJob { job_id: String, kind: String },
    /// A live alias of this shard whose target no shard holds as a live
    /// canonical row, this shard included.
    DanglingAlias { alias_path: String, alias_object_id: String, target_object_id: String },
    /// A live row of this shard whose parent path another shard owns, and
    /// that shard holds no live directory there.
    ParentDirectoryAbsent { path: String, parent_path: String, parent_shard: String },
    /// A hardlink job of this shard whose only step left commits a target
    /// fence that a current shard still holds: releasing it needs a
    /// fence-release step.
    TargetFenceNeedsRelease { job_id: String, fence_job_id: String, fence_shard: String },
}

impl std::fmt::Display for ConversionStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metadata shard conversion stopped: ")?;
        match self {
            Self::SourceChangedUnderAttempt { attempt_id } => write!(
                f, "the primary's source changed under its recorded attempt {attempt_id}"
            ),
            Self::UnresolvedOriginalWork { references } => write!(
                f, "original work remains unresolved: {}", references.join(", ")
            ),
            Self::LegacyHardlinkNeedsAliasShard { job_id, alias_shard } => write!(
                f, "legacy hardlink job {job_id} settles from the rows of alias shard {alias_shard}"
            ),
            Self::LegacyHardlinkUnsettled { job_id, reason } => {
                write!(f, "legacy hardlink job {job_id} cannot be settled: {reason}")
            }
            Self::CountlessDirectoryMove { job_id } => {
                write!(f, "legacy directory move {job_id} has no counted-parent plan")
            }
            Self::UnsettledNamespaceJob { job_id, kind } => {
                write!(f, "namespace job {job_id} ({kind}) is not Completed")
            }
            Self::DanglingAlias { alias_path, alias_object_id, target_object_id } => write!(
                f, "dangling alias {alias_path} ({alias_object_id}): no shard holds its target {target_object_id}"
            ),
            Self::ParentDirectoryAbsent { path, parent_path, parent_shard } => write!(
                f, "{path} has no live parent directory {parent_path} in shard {parent_shard}"
            ),
            Self::TargetFenceNeedsRelease { job_id, fence_job_id, fence_shard } => write!(
                f, "hardlink job {job_id} commits fence {fence_job_id}, which current shard {fence_shard} still holds"
            ),
        }
    }
}

impl From<ConversionStop> for MetadataBackendError {
    fn from(stop: ConversionStop) -> Self {
        error(stop)
    }
}

/// A transaction outcome the KV layer decided. An outcome it could not
/// report is an error the owner retries with the same saved request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::authority) enum Submitted {
    Committed,
    /// A compare failed; the request's identity is spent.
    Rejected,
}

/// Progress of the primary's capture of its own copy.
pub(in crate::authority) enum CaptureProgress {
    Captured(Arc<CapturedMetadataUnit>),
    Pending { processed_records: usize },
}

/// One member's answer to an install request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::authority) enum InstallReply {
    /// The member durably holds the sealed image for this attempt.
    Installed,
    /// The member cannot take the image now; the driver waits for it to
    /// become reachable or for the install rule to change.
    Unavailable { reason: String },
}

/// What a shard's conversion driver needs from its node. The owner supplies
/// the native implementation; tests supply doubles.
pub(in crate::authority) trait ShardConversionEnv {
    type Source: ContributionSource;
    fn local_node(&self) -> Result<NodeId>;
    fn data_root(&self) -> &Path;
    /// One exact-key-set authoritative read; `None` is an absent key.
    fn read_exact(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>>;
    /// The filesystem's complete metadata-shard catalog.
    fn catalog(&self, file_system_id: &DfsFileSystemId) -> Result<Vec<DfsMetadataShardConfig>>;
    /// Submit `request` exactly as given.
    fn submit(&self, request: &KvTxnRequest) -> Result<Submitted>;
    /// Whether the local copy of `shard` is already current format: an Active
    /// current header, or a copy born native genesis. A shard without a
    /// storage record whose copy is current has nothing to convert.
    fn local_copy_current(&self, shard: &DfsMetadataShardConfig) -> Result<bool>;
    /// Capture the local copy of `shard` with the converter session, under
    /// its source fence, binding the shard's [`Self::placement_evidence`].
    fn capture(&self, shard: &DfsMetadataShardConfig) -> Result<CaptureProgress>;
    /// The shard's share of its filesystem's original placement goals, which
    /// its capture binds as the complete-filesystem capture binds its share;
    /// `None` while the goals are still being captured.
    fn placement_evidence(
        &self,
        _shard: &DfsMetadataShardConfig,
    ) -> Result<Option<Vec<crate::metadata::RetainedPlacementReservation>>> {
        Ok(Some(Vec::new()))
    }
    fn contribution_source(&self) -> &Self::Source;
    fn block_on<F: Future>(&self, future: F) -> Result<F::Output>;
    /// Send the sealed image of `attempt` to member `node`. `source` is the
    /// primary's captured source the attempt's source-cut digest binds; the
    /// member reconciles its retained floors against it.
    fn install(
        &self,
        node: &NodeId,
        shard: &DfsMetadataShardConfig,
        attempt: &DfsMetadataShardConversion,
        image: &Path,
        source: &ConversionSourceCut,
    ) -> Result<InstallReply>;
    /// Put the sealed image in place of the local copy and activate it under
    /// the Active `record`. Idempotent.
    fn activate_local(
        &self,
        shard: &DfsMetadataShardConfig,
        record: &DfsMetadataShardStorage,
        image: &Path,
    ) -> Result<()>;
    /// Wake the unit when any of `keys` changes.
    fn watch_keys(&self, keys: BTreeSet<String>) -> Result<()>;
    /// Wake the unit when any of `nodes` becomes reachable.
    fn watch_nodes(&self, nodes: BTreeSet<NodeId>) -> Result<()>;
    /// This node's converting-takeover facilities: its verdicts of its peers
    /// and its members' promises. Without them only the primary converts.
    fn takeover(&self) -> Option<&dyn takeover::TakeoverEnv> {
        None
    }
}

/// The attempt's immutable identity and the serving record it started under.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct AttemptRecord {
    version: u16,
    /// Unsealed: the seal is recorded separately once the image is built.
    attempt: DfsMetadataShardConversion,
    shard: KvEntry,
    write_set: Option<KvEntry>,
}

/// One complete round of every other shard's answers, keyed by shard id.
type ContributionRound = BTreeMap<String, SourceContributionAnswer>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct AcceptedContributions {
    /// The catalog the answers were attributed with.
    catalog: Vec<DfsMetadataShardConfig>,
    answers: ContributionRound,
    digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct SettledJobs {
    /// The shard's namespace jobs after settling its own legacy jobs.
    namespace_jobs: Vec<DfsNamespaceJobRecord>,
    /// Settled job ids, with their outcome.
    settled: Vec<(String, String)>,
    /// The settled jobs' outer namespace requests, terminal.
    #[serde(default)]
    requests: Vec<sinomo_dfs::DfsNamespaceRequestRecord>,
    /// Canonical targets whose fence commit the settlement applied.
    #[serde(default)]
    objects: Vec<sinomo_dfs::DfsObjectMetadata>,
    /// Jobs kept Running for the per-pair settlement of the rounds.
    #[serde(default)]
    pending_pairs: Vec<String>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    settled_at: Option<time::OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct BuiltImage {
    seal: DfsMetadataGenerationSeal,
    contributions_digest: String,
}

/// The local state of one attempt on its driver, under
/// `dfs/migration-owner/shards/<fs>/<shard>/<attempt>/`.
struct AttemptFiles {
    root: PathBuf,
}

impl AttemptFiles {
    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
    fn image(&self) -> PathBuf {
        self.root.join("image")
    }
    fn read<T: serde::de::DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        let path = self.path(name);
        if !path.try_exists().map_err(error)? {
            return Ok(None);
        }
        let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(&path, MAX_FILE_BYTES)
            .map_err(error)?;
        serde_json::from_slice(&bytes).map(Some).map_err(error)
    }
    fn write(&self, name: &str, value: &impl Serialize) -> Result<()> {
        if !self.root.is_dir() {
            std::fs::create_dir_all(&self.root).map_err(error)?;
            if let Some(parent) = self.root.parent() {
                sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
            }
        }
        let bytes = serde_json::to_vec(value).map_err(error)?;
        sinomo_core::storage::durable_file::write_file_atomic(
            &self.path(name),
            &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("conversion", "tmp")
                .sync_all(),
        )
        .map_err(error)
    }
    /// Write `value` once; a later write must carry the same value.
    fn write_once<T: Serialize + serde::de::DeserializeOwned + PartialEq>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<()> {
        match self.read::<T>(name)? {
            Some(existing) if existing == *value => Ok(()),
            Some(_) => Err(error(format!("immutable conversion record {name} changed"))),
            None => self.write(name, value),
        }
    }
    fn remove(&self, name: &str) -> Result<()> {
        sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(&self.path(name))
            .map(|_| ())
            .map_err(error)
    }
    fn rejected(&self) -> Result<BTreeSet<String>> {
        Ok(self.read("rejected-requests.json")?.unwrap_or_default())
    }
}

/// Every attempt of one shard on this node.
struct ShardFiles {
    root: PathBuf,
}

impl ShardFiles {
    fn new(data_root: &Path, file_system_id: &DfsFileSystemId, shard_id: &str) -> Result<Self> {
        let file_system = file_system_id.to_string();
        for segment in [file_system.as_str(), shard_id] {
            let path = Path::new(segment);
            if segment.is_empty()
                || path.components().count() != 1
                || !matches!(path.components().next(), Some(std::path::Component::Normal(_)))
            {
                return Err(error("shard conversion names an invalid path segment"));
            }
        }
        Ok(Self {
            root: data_root
                .join("dfs/migration-owner/shards")
                .join(file_system)
                .join(shard_id),
        })
    }
    fn attempt(&self, attempt_id: &str) -> Result<AttemptFiles> {
        if attempt_id.len() != 64 || !attempt_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(error("shard conversion attempt id is not a digest"));
        }
        Ok(AttemptFiles { root: self.root.join(attempt_id) })
    }
    fn attempts(&self) -> Result<Vec<(String, AttemptFiles)>> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(failure) => return Err(error(failure)),
        };
        let mut attempts = Vec::new();
        for entry in entries {
            let name = entry.map_err(error)?.file_name();
            let name = name.to_str().ok_or_else(|| error("attempt directory is not UTF-8"))?;
            attempts.push((name.to_owned(), self.attempt(name)?));
        }
        attempts.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(attempts)
    }
    /// Delete every attempt but `keep`.
    fn remove_except(&self, keep: Option<&str>) -> Result<()> {
        for (id, files) in self.attempts()? {
            if Some(id.as_str()) != keep {
                remove_attempt(&files)?;
            }
        }
        Ok(())
    }
}

fn remove_attempt(files: &AttemptFiles) -> Result<()> {
    match std::fs::remove_dir_all(&files.root) {
        Ok(()) => {}
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => {}
        Err(failure) => return Err(error(failure)),
    }
    if let Some(parent) = files.root.parent() {
        sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
    }
    Ok(())
}

/// The shard's own keys and its configured members' records, from one
/// exact-set read.
struct ShardKeys {
    shard_entry: KvEntry,
    shard: DfsMetadataShardConfig,
    write_set: Option<KvEntry>,
    handoff: Option<KvEntry>,
    storage: Option<KvEntry>,
    members: Vec<(NodeId, Option<KvEntry>, Option<KvEntry>)>,
}

fn shard_key_names(file_system_id: &DfsFileSystemId, shard_id: &str) -> [String; 4] {
    [
        sinomo_dfs::metadata_shard_key(file_system_id, shard_id),
        sinomo_dfs::metadata_shard_write_set_key(file_system_id, shard_id),
        sinomo_dfs::metadata_shard_membership_handoff_key(file_system_id, shard_id),
        sinomo_dfs::metadata_shard_storage_key(file_system_id, shard_id),
    ]
}

fn member_key_names(nodes: &BTreeSet<NodeId>) -> BTreeSet<String> {
    nodes
        .iter()
        .flat_map(|node| {
            [
                sinomo_core::node::cluster_member_key(node),
                sinomo_core::node::cluster_capability_policy_key(node),
            ]
        })
        .collect()
}

impl ShardKeys {
    fn configured(&self) -> BTreeSet<NodeId> {
        std::iter::once(self.shard.primary_node_id.clone())
            .chain(self.shard.replica_node_ids.iter().cloned())
            .collect()
    }
    /// The storage record, which must belong to this shard's storage
    /// identity. A record that does not is not authority for anything.
    fn record(&self) -> Result<Option<DfsMetadataShardStorage>> {
        self.storage
            .as_ref()
            .map(|entry| {
                let record: DfsMetadataShardStorage = entry.decode().map_err(error)?;
                record.validate_against(&self.shard)?;
                Ok(record)
            })
            .transpose()
    }
    fn storage_members(&self) -> Vec<DfsMetadataShardStorageMember<'_>> {
        self.members
            .iter()
            .map(|(node_id, member, policy)| DfsMetadataShardStorageMember {
                node_id: node_id.clone(),
                member: member.as_ref(),
                policy: policy.as_ref(),
            })
            .collect()
    }
    fn cut<'a>(
        &'a self,
        members: &'a [DfsMetadataShardStorageMember<'a>],
        member_compares: Vec<KvCompare>,
    ) -> DfsMetadataShardStorageCut<'a> {
        DfsMetadataShardStorageCut {
            shard: &self.shard_entry,
            write_set: self.write_set.as_ref(),
            handoff: self.handoff.as_ref(),
            watermark: None,
            storage: self.storage.as_ref(),
            member_compares,
            members,
        }
    }
}

/// Read the shard's keys and its members' records in one exact-set read.
/// The members come from the serving record, so the read is repeated if
/// that record changed between the reads that name them.
fn read_shard_keys<E: ShardConversionEnv>(
    env: &E,
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> Result<Option<ShardKeys>> {
    let names = shard_key_names(file_system_id, shard_id);
    let mut configured: Option<BTreeSet<NodeId>> = None;
    for _ in 0..3 {
        let members = configured.clone().unwrap_or_default();
        // Member and policy keys, in the order the records are read back.
        let member_keys = members
            .iter()
            .flat_map(|node| {
                [
                    sinomo_core::node::cluster_member_key(node),
                    sinomo_core::node::cluster_capability_policy_key(node),
                ]
            })
            .collect::<Vec<_>>();
        // Watch first, so a change after this read wakes the parked unit.
        env.watch_keys(names.iter().cloned().chain(member_key_names(&members)).collect())?;
        let keys = names.iter().cloned().chain(member_keys).collect::<Vec<_>>();
        let mut entries = env.read_exact(&keys)?.into_iter();
        let [Some(shard_entry), write_set, handoff, storage] =
            std::array::from_fn(|_| entries.next().flatten())
        else {
            return Ok(None);
        };
        let shard: DfsMetadataShardConfig = shard_entry.decode().map_err(error)?;
        if shard.file_system_id != *file_system_id || shard.shard_id != shard_id {
            return Err(error("shard record names another shard"));
        }
        let read = ShardKeys {
            shard_entry,
            shard,
            write_set,
            handoff,
            storage,
            members: members
                .iter()
                .map(|node| (node.clone(), entries.next().flatten(), entries.next().flatten()))
                .collect(),
        };
        if configured.as_ref() == Some(&read.configured()) {
            return Ok(Some(read));
        }
        configured = Some(read.configured());
    }
    Err(MetadataBackendError::StoragePreparationPending {
        root: env.data_root().to_owned(),
        processed_records: 0,
    })
}

/// The attempt's source: the facts the primary's captured copy carries,
/// plus digests of the captured cut and of the operations it carries across.
pub(in crate::authority) fn conversion_source(
    captured: &CapturedMetadataUnit,
) -> Result<DfsMetadataConversionSource> {
    captured.validate()?;
    let source = &captured.source;
    Ok(DfsMetadataConversionSource {
        source_identity: source.source_identity.clone(),
        source_format: source.source_format,
        source_journal_sequence: source.source_journal_sequence,
        source_state_fingerprint: source.source_state_fingerprint.clone(),
        source_cut_digest: source_cut_digest(source, captured.projection_fingerprint())?,
        pending_mapping_digest: digest(&(
            "dfs-shard-conversion-pending-mapping-v1",
            captured.recovery_preparations(),
            &captured.proven_v7_terminals,
        ))?,
    })
}

enum Step {
    Done(ConversionProgress),
    /// A definite rejection or a committed transaction: read and plan again.
    Replan,
}

/// Advance the conversion of `shard_id` by one bounded step.
pub(in crate::authority) fn advance_shard_conversion<E: ShardConversionEnv>(
    env: &E,
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> Result<ConversionProgress> {
    for _ in 0..=REPLANS_PER_STEP {
        if let Step::Done(progress) = step(env, file_system_id, shard_id)? {
            return Ok(progress);
        }
    }
    Ok(ConversionProgress::Pending { processed_records: 1 })
}

fn parked(evidence: impl Into<String>) -> Result<Step> {
    Ok(Step::Done(ConversionProgress::Parked { evidence: evidence.into() }))
}

fn step<E: ShardConversionEnv>(
    env: &E,
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> Result<Step> {
    let local = env.local_node()?;
    let shard_files = ShardFiles::new(env.data_root(), file_system_id, shard_id)?;
    // 1. The shard's keys, in one exact-set read.
    let Some(keys) = read_shard_keys(env, file_system_id, shard_id)? else {
        return parked("the shard has no serving record");
    };
    let record = keys.record()?;
    if record.is_none() && env.local_copy_current(&keys.shard)? {
        return Ok(Step::Done(ConversionProgress::Ready));
    }
    if let Some(active) = record.as_ref().filter(|record| record.is_active()) {
        return finish_active(env, &shard_files, &keys, active, &local);
    }
    // A takeover recorded at this epoch, or a member other than the primary.
    if let Some(step) = takeover::step(env, &shard_files, &keys, record.as_ref(), &local)? {
        return Ok(step);
    }
    let running = record
        .as_ref()
        .and_then(DfsMetadataShardStorage::converting_attempt)
        .filter(|attempt| attempt.epoch == keys.shard.epoch);
    // An attempt that no longer runs under the serving record is superseded.
    for (id, files) in shard_files.attempts()? {
        let attempt = files.read::<AttemptRecord>("attempt.json")?;
        let current = attempt.as_ref().is_some_and(|record| {
            record.attempt.epoch == keys.shard.epoch
                && record.attempt.primary_node_id == keys.shard.primary_node_id
                && record.attempt.primary_node_id == local
        });
        if !current || running.is_some_and(|running| running.attempt_id != id && running.primary_node_id == local) {
            remove_attempt(&files)?;
        }
    }
    if keys.shard.primary_node_id != local {
        return parked("only the shard's primary converts it");
    }
    if keys.handoff.is_some() {
        return parked("a membership handoff is pending on the shard");
    }
    let rule = DfsMetadataShardConversionInstallRule::for_shard(&keys.shard, keys.write_set.as_ref())
        .map_err(error)?;
    let install_set = rule.install_set();
    let members = keys.storage_members();
    let member_compares = match keys.cut(&members, Vec::new()).storage_capable_compares_for(&install_set) {
        Ok(compares) => compares,
        Err(reason) => return parked(format!("an install member lacks the storage capability: {reason}")),
    };
    let cut = keys.cut(&members, member_compares);

    // 2. The attempt, fixed from the primary's own captured source.
    let (attempt, files, captured) = match fix_attempt(env, &shard_files, &keys, running, &local, None)? {
        Fixed::Attempt { attempt, files, captured } => (attempt, files, captured),
        Fixed::Capturing { processed_records } => {
            return Ok(Step::Done(ConversionProgress::Pending { processed_records }));
        }
    };

    // 3. Converting: the shard is closed everywhere, and contributions to it
    // are frozen from here on.
    let recorded = running.filter(|running| running.attempt_id == attempt.attempt_id);
    if recorded.is_none() {
        let record = DfsMetadataShardStorage::converting(&keys.shard, attempt.clone());
        return submit_saved(env, &files, "converting-request.json", || {
            sinomo_dfs::build_metadata_shard_storage_converting(&cut, &record).map_err(error)
        });
    }
    let recorded = recorded.expect("checked above");

    // 4-7. The image, built from the bound capture, the settled jobs and the
    // accepted contributions.
    let built = match files.read::<BuiltImage>("built.json")? {
        Some(built) => built,
        None => {
            let Some(captured) = captured else {
                return Err(error("conversion capture was not bound before the image build"));
            };
            match build_image(env, &files, &keys.shard, &attempt, &captured)? {
                Some(built) => built,
                None => return Ok(Step::Done(round_progress(&files)?)),
            }
        }
    };

    let accepted: AcceptedContributions = files
        .read("contributions-accepted.json")?
        .ok_or_else(|| error("built attempt lost its accepted contributions"))?;
    validate_built_image(&files, &accepted, &built, &attempt, &keys.shard)?;

    // 8. Seal.
    match recorded.sealed() {
        Some((seal, digest)) if *seal == built.seal && digest == built.contributions_digest => {}
        Some(_) => return Err(error("the shard's attempt is sealed with another image")),
        None => {
            return submit_saved(env, &files, "seal-request.json", || {
                sinomo_dfs::build_metadata_shard_storage_seal(&cut, &built.seal, &built.contributions_digest)
                    .map(|(request, _)| request)
                    .map_err(error)
            });
        }
    }

    // 9. Install under the rule of the current serving record.
    let mut sealed_attempt = attempt.clone();
    sealed_attempt.seal = Some(built.seal.clone());
    sealed_attempt.contributions_digest = Some(built.contributions_digest.clone());
    let acknowledged = match install(env, &files, &keys.shard, &rule, &sealed_attempt, &built, &local)? {
        Ok(acknowledged) => acknowledged,
        Err(evidence) => return parked(evidence),
    };

    // 10. Activation. The image was checked above against the sealed
    // contributions and its own rows.
    let ack_compares = match keys.cut(&members, Vec::new()).storage_capable_compares_for(&acknowledged) {
        Ok(compares) => compares,
        Err(reason) => return parked(format!("an acknowledging member lacks the storage capability: {reason}")),
    };
    let activation_cut = keys.cut(&members, ack_compares);
    submit_saved(env, &files, "activation-request.json", || {
        sinomo_dfs::build_metadata_shard_storage_activate(&activation_cut)
            .map(|(request, _)| request)
            .map_err(error)
    })
}

/// Steps 11 and 12. Storage is Active: activate the local copy of this node's attempt
/// and remove what the attempt no longer needs, or hand over.
fn finish_active<E: ShardConversionEnv>(
    env: &E,
    shard_files: &ShardFiles,
    keys: &ShardKeys,
    active: &DfsMetadataShardStorage,
    local: &NodeId,
) -> Result<Step> {
    let ours = active
        .converted_activation()
        .map(|(attempt, _)| attempt)
        .filter(|attempt| attempt.primary_node_id == *local);
    let Some(attempt) = ours else {
        shard_files.remove_except(None)?;
        return Ok(Step::Done(ConversionProgress::Handover));
    };
    let files = shard_files.attempt(&attempt.attempt_id)?;
    if files.read::<AttemptRecord>("attempt.json")?.is_none() {
        shard_files.remove_except(None)?;
        return Ok(Step::Done(ConversionProgress::Handover));
    }
    env.activate_local(&keys.shard, active, &files.image())?;
    // The request records stay as evidence of what the attempt submitted.
    for name in ["contributions-round.json", "settled-jobs.json", "install-acks.json", SOURCE_CUT] {
        files.remove(name)?;
    }
    shard_files.remove_except(Some(&attempt.attempt_id))?;
    Ok(Step::Done(ConversionProgress::Ready))
}

enum Fixed {
    Attempt {
        attempt: DfsMetadataShardConversion,
        files: AttemptFiles,
        /// The capture bound to the attempt, while the image is not built.
        captured: Option<Arc<CapturedMetadataUnit>>,
    },
    Capturing { processed_records: usize },
}

/// Steps 2 and 4. Derive the attempt from the converter's captured source and
/// bind the capture to it. Once the image is built the capture is not needed.
/// `takeover` names the lost primary when this node converts in its place.
fn fix_attempt<E: ShardConversionEnv>(
    env: &E,
    shard_files: &ShardFiles,
    keys: &ShardKeys,
    running: Option<&DfsMetadataShardConversion>,
    local: &NodeId,
    takeover: Option<&DfsMetadataConversionTakeover>,
) -> Result<Fixed> {
    let ours = running.filter(|running| running.primary_node_id == *local && running.takeover.as_ref() == takeover);
    if let Some(running) = ours {
        let files = shard_files.attempt(&running.attempt_id)?;
        if files.read::<BuiltImage>("built.json")?.is_some() {
            let record = files
                .read::<AttemptRecord>("attempt.json")?
                .ok_or_else(|| error("built conversion attempt lost its identity"))?;
            return Ok(Fixed::Attempt { attempt: record.attempt, files, captured: None });
        }
    }
    let captured = match env.capture(&keys.shard)? {
        CaptureProgress::Captured(captured) => captured,
        CaptureProgress::Pending { processed_records } => {
            return Ok(Fixed::Capturing { processed_records: processed_records.max(1) });
        }
    };
    // The capture binds the shard's share of the placement goals.
    let Some(placement) = env.placement_evidence(&keys.shard)? else {
        return Ok(Fixed::Capturing { processed_records: 1 });
    };
    if captured.source.retained_placement_reservations != placement {
        return Err(error("the shard's capture omits or changes its share of the original placement goals"));
    }
    let source = conversion_source(&captured)?;
    let shard = &keys.shard;
    let attempt_id = sinomo_dfs::metadata_shard_conversion_attempt_id(
        &shard.file_system_id,
        &shard.shard_id,
        &shard.assignment,
        shard.epoch,
        local,
        &source,
        takeover,
    )?;
    if let Some(running) = ours
        && running.attempt_id != attempt_id
    {
        return Err(ConversionStop::SourceChangedUnderAttempt {
            attempt_id: running.attempt_id.clone(),
        }
        .into());
    }
    let attempt = DfsMetadataShardConversion {
        attempt_id: attempt_id.clone(),
        epoch: shard.epoch,
        primary_node_id: local.clone(),
        takeover: takeover.cloned(),
        source,
        seal: None,
        contributions_digest: None,
    };
    let files = shard_files.attempt(&attempt_id)?;
    match files.read::<AttemptRecord>("attempt.json")? {
        Some(existing) if existing.attempt == attempt => {}
        Some(_) => return Err(error("conversion attempt identity changed")),
        None => {
            shard_files.remove_except(None)?;
            files.write(
                "attempt.json",
                &AttemptRecord {
                    version: ATTEMPT_VERSION,
                    attempt: attempt.clone(),
                    shard: keys.shard_entry.clone(),
                    write_set: keys.write_set.clone(),
                },
            )?;
        }
    }
    files.write_once(PLACEMENT_EVIDENCE, &placement)?;
    // The source facts members reconcile their floors against at install.
    files.write_once(
        SOURCE_CUT,
        &ConversionSourceCut {
            source: captured.source.clone(),
            projection_fingerprint: captured.projection_fingerprint().to_owned(),
        },
    )?;
    Ok(Fixed::Attempt { attempt, files, captured: Some(captured) })
}

/// Submit the request saved under `name`, or build, save and submit a new
/// one. Either decided outcome re-plans from a fresh read: a committed
/// request stays saved as evidence (its stage is skipped once its effect is
/// read), and a rejected one is set aside so it is never resubmitted. An
/// outcome the KV layer could not report is an error, and the step that
/// reaches this stage again resubmits the same saved bytes.
fn submit_saved<E: ShardConversionEnv>(
    env: &E,
    files: &AttemptFiles,
    name: &str,
    build: impl FnOnce() -> Result<KvTxnRequest>,
) -> Result<Step> {
    let request = match files.read::<KvTxnRequest>(name)? {
        Some(saved) => saved,
        None => {
            let request = build()?;
            if files.rejected()?.contains(&request.request_id.to_string()) {
                // The request identity is derived from the attempt, and a
                // rejected request spent it. Only a serving change, which
                // starts a new attempt, can submit again.
                return parked(format!(
                    "{name} would reuse the request identity a rejected compare spent"
                ));
            }
            files.write(name, &request)?;
            request
        }
    };
    if env.submit(&request)? == Submitted::Rejected {
        let mut rejected = files.rejected()?;
        rejected.insert(request.request_id.to_string());
        files.write("rejected-requests.json", &rejected)?;
        files.remove(name)?;
    }
    Ok(Step::Replan)
}

fn round_progress(files: &AttemptFiles) -> Result<ConversionProgress> {
    Ok(match files.read::<RoundState>("round-state.json")? {
        Some(RoundState::Again) => ConversionProgress::Pending { processed_records: 1 },
        Some(RoundState::Parked { evidence }) => ConversionProgress::Parked { evidence },
        None => ConversionProgress::Pending { processed_records: 1 },
    })
}

/// How the last contribution round ended, when it did not accept.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
enum RoundState {
    /// The round differs from its predecessor; the next round runs at once.
    Again,
    Parked { evidence: String },
}

/// Steps 5 to 7. Settle the shard's own legacy jobs, read the other shards'
/// contributions until they are stable, and build the image. `None` when
/// the contributions are not accepted yet (see `round-state.json`).
fn build_image<E: ShardConversionEnv>(
    env: &E,
    files: &AttemptFiles,
    shard: &DfsMetadataShardConfig,
    attempt: &DfsMetadataShardConversion,
    captured: &Arc<CapturedMetadataUnit>,
) -> Result<Option<BuiltImage>> {
    if conversion_source(captured)? != attempt.source {
        return Err(ConversionStop::SourceChangedUnderAttempt {
            attempt_id: attempt.attempt_id.clone(),
        }
        .into());
    }
    let catalog = env.catalog(&shard.file_system_id)?;
    // 5. The shard's own legacy cross-shard jobs, settled forward.
    let settled = match files.read::<SettledJobs>("settled-jobs.json")? {
        Some(settled) => settled,
        None => {
            let settled = settle_own_jobs(&catalog, shard, captured)?;
            files.write("settled-jobs.json", &settled)?;
            settled
        }
    };
    // What the rounds ask the other shards, from the shard's settled rows.
    let own = pairs::settled_unit(captured, &settled)?;
    // 6. Contributions, after Converting.
    let accepted = match files.read::<AcceptedContributions>("contributions-accepted.json")? {
        Some(accepted) => accepted,
        None => {
            let questions = pairs::RoundQuestions::new(&catalog, shard, &own, &settled)?;
            match contribution_round(env, files, shard, &catalog, attempt, &questions)? {
                Some(accepted) => accepted,
                None => return Ok(None),
            }
        }
    };
    let questions = pairs::RoundQuestions::new(&accepted.catalog, shard, &own, &settled)?;
    let settled_unit = Arc::new(pairs::with_pair_settlement(captured, &settled, &questions, &accepted.answers)?);
    // 7. The image, with counts from the shard's own rows and the accepted
    // contributions.
    let target = NamespaceCountMaintenanceSource { captured: settled_unit };
    let contributions = accepted_contributions(&accepted);
    let plan = crate::namespace_counts::contributions::plan_shard_namespace_count_maintenance(
        &accepted.catalog,
        &target,
        &contributions,
        COUNT_LIMITS,
    )?;
    let counted = plan.counted_objects(&target, COUNT_LIMITS)?;
    let generation = sinomo_dfs::metadata_shard_converted_generation(
        &shard.file_system_id,
        &shard.shard_id,
        &attempt.attempt_id,
    );
    let built = crate::metadata::canonical_generation::build_canonical_generation(
        &target.captured,
        &generation,
        &counted,
        &files.image(),
    )?;
    let built = BuiltImage { seal: built.seal, contributions_digest: accepted.digest };
    files.write_once("built.json", &built)?;
    Ok(Some(built))
}

fn accepted_contributions(accepted: &AcceptedContributions) -> Vec<ShardCountContributions> {
    accepted
        .answers
        .values()
        .map(|answer| answer.contributions.clone())
        .collect()
}

/// Per-shard activation check: the built image holds exactly the shard's
/// settled rows with the counts its own rows and the sealed contributions
/// give. It replaces re-running a filesystem-wide census around activation.
fn validate_built_image(
    files: &AttemptFiles,
    accepted: &AcceptedContributions,
    built: &BuiltImage,
    attempt: &DfsMetadataShardConversion,
    shard: &DfsMetadataShardConfig,
) -> Result<()> {
    if contributions_digest(&attempt.attempt_id, &accepted.answers)? != accepted.digest
        || accepted.digest != built.contributions_digest
    {
        return Err(error("accepted contributions differ from the digest the attempt sealed"));
    }
    let image = sinomo_dfs::read_metadata_candidate(&files.image())?;
    crate::namespace_counts::contributions::validate_shard_image_counts(
        &accepted.catalog,
        &sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id),
        std::slice::from_ref(&shard.file_system_id),
        &image.tables,
        &accepted_contributions(accepted),
        COUNT_LIMITS,
    )
}

fn contributions_digest(attempt_id: &str, answers: &ContributionRound) -> Result<String> {
    digest(&("dfs-shard-conversion-contributions-v1", attempt_id, answers))
}

/// One full round over every other shard of the filesystem. Accepts when
/// this round equals the previous one for every source and no answer names
/// a job touching the shard.
fn contribution_round<E: ShardConversionEnv>(
    env: &E,
    files: &AttemptFiles,
    target: &DfsMetadataShardConfig,
    catalog: &[DfsMetadataShardConfig],
    attempt: &DfsMetadataShardConversion,
    questions: &pairs::RoundQuestions,
) -> Result<Option<AcceptedContributions>> {
    let sources = catalog
        .iter()
        .filter(|source| source.shard_id != target.shard_id)
        .collect::<Vec<_>>();
    // A source's serving, write-set or storage change can end its wait.
    env.watch_keys(
        sources
            .iter()
            .flat_map(|source| shard_key_names(&source.file_system_id, &source.shard_id))
            .collect(),
    )?;
    let mut round = ContributionRound::new();
    let (mut nodes, mut keys) = (BTreeSet::new(), BTreeSet::new());
    for source in &sources {
        let question = questions.for_source(&source.shard_id);
        match env.block_on(env.contribution_source().read_asking(target, source, &question))?? {
            SourceContributionRead::Answered(answer) => {
                round.insert(source.shard_id.clone(), answer);
            }
            SourceContributionRead::Waiting { nodes: more_nodes, keys: more_keys } => {
                nodes.extend(more_nodes);
                keys.extend(more_keys);
            }
        }
    }
    if round.len() != sources.len() {
        // No partial round is kept: a source that cannot answer is waited
        // for, never treated as empty.
        env.watch_keys(keys)?;
        env.watch_nodes(nodes)?;
        files.write(
            "round-state.json",
            &RoundState::Parked { evidence: "a contributing shard has no member that can answer".into() },
        )?;
        return Ok(None);
    }
    // A job parked on this shard itself moves none of its contributions
    // until it activates, and a job whose only effect here is a fence this
    // shard holds is settled per pair from its recorded phase; any other job
    // touching the shard holds the rounds.
    let touching = round
        .iter()
        .filter(|(_, answer)| {
            answer
                .running_jobs_touching_target
                .iter()
                .any(|job| !job.parked_on_target() && !questions.settles_per_pair(job))
        })
        .map(|(shard, _)| shard.clone())
        .collect::<Vec<_>>();
    let previous = files.read::<ContributionRound>("contributions-round.json")?;
    files.write("contributions-round.json", &round)?;
    if !touching.is_empty() {
        // Such a job keeps the rounds from agreeing until it parks on this
        // shard or ends. Its coordinator's `dfs.namespace.job.parked` and
        // `dfs.namespace.job.completed` events wake this unit
        // (`source_job_wakes`), as do the source's keys.
        files.write(
            "round-state.json",
            &RoundState::Parked {
                evidence: format!("jobs of shards {} touch this shard", touching.join(", ")),
            },
        )?;
        return Ok(None);
    }
    if previous.as_ref() != Some(&round) {
        files.write("round-state.json", &RoundState::Again)?;
        return Ok(None);
    }
    // Per-pair checks and settlements, decided on the stable round.
    if let pairs::PairDecision::Wait(evidence) = pairs::settle_pairs(questions, &round)? {
        files.write("round-state.json", &RoundState::Parked { evidence })?;
        return Ok(None);
    }
    let accepted = AcceptedContributions {
        catalog: catalog.to_vec(),
        digest: contributions_digest(&attempt.attempt_id, &round)?,
        answers: round,
    };
    files.write_once("contributions-accepted.json", &accepted)?;
    files.remove("round-state.json")?;
    Ok(Some(accepted))
}

/// Step 5: settle the shard's own legacy cross-shard jobs forward in its
/// image (decision Q-C2: count a live alias, otherwise abort). A job the existing
/// settlement cannot decide from this shard's rows stops the conversion.
fn settle_own_jobs(
    catalog: &[DfsMetadataShardConfig],
    shard: &DfsMetadataShardConfig,
    captured: &CapturedMetadataUnit,
) -> Result<SettledJobs> {
    use crate::legacy_wire::hardlink_settlement::{LegacyHardlinkSettlement, settle_legacy_hardlink_job};
    // The loop below decides each of the shard's own non-Completed jobs.
    if captured.has_unresolved_work_beside_recorded_jobs() {
        return Err(ConversionStop::UnresolvedOriginalWork {
            references: captured
                .unresolved
                .iter()
                .take(8)
                .map(|work| format!("{}:{} ({})", work.family, work.original_id, work.reason))
                .collect(),
        }
        .into());
    }
    let rows = &captured.projected_rows;
    let at = time::OffsetDateTime::now_utc();
    let mut namespace_jobs = Vec::with_capacity(rows.namespace_jobs.len());
    let mut settled = Vec::new();
    let mut own = pairs::OwnPairWork::new(catalog, shard, rows);
    for job in &rows.namespace_jobs {
        if job.status == DfsNamespaceJobStatus::Completed {
            namespace_jobs.push(job.clone());
            continue;
        }
        // Target fences and fence-commit-only hardlink jobs.
        if own.claim(job)? {
            continue;
        }
        let legacy_hardlink = job.kind == DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
            && matches!(
                job.pending_frames.first(),
                Some(DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. })
            );
        if legacy_hardlink {
            let alias_path = job.target_path.as_deref().unwrap_or_default();
            let owner = sinomo_dfs::select_metadata_shard_config_for_path(&job.file_system_id, catalog, alias_path)
                .map_err(error)?;
            match owner {
                Some(owner) if owner.shard_id == shard.shard_id => {}
                owner => {
                    return Err(ConversionStop::LegacyHardlinkNeedsAliasShard {
                        job_id: job.job_id.clone(),
                        alias_shard: owner.map_or_else(|| "none".into(), |owner| owner.shard_id),
                    }
                    .into());
                }
            }
            match settle_legacy_hardlink_job(job, rows, at) {
                Ok(LegacyHardlinkSettlement::Completed { job, .. }) => {
                    settled.push((job.job_id.clone(), "completed".into()));
                    namespace_jobs.push(job);
                }
                Ok(LegacyHardlinkSettlement::Aborted { job_id, .. }) => {
                    settled.push((job_id, "aborted".into()));
                }
                Err(failure) => {
                    return Err(ConversionStop::LegacyHardlinkUnsettled {
                        job_id: job.job_id.clone(),
                        reason: failure.to_string(),
                    }
                    .into());
                }
            }
            continue;
        }
        if job.pending_frames.iter().any(|frame| {
            matches!(frame, DfsNamespaceJobFrame::CrossShardDirFinish { count_plan: None, .. })
        }) {
            return Err(ConversionStop::CountlessDirectoryMove { job_id: job.job_id.clone() }.into());
        }
        return Err(ConversionStop::UnsettledNamespaceJob {
            job_id: job.job_id.clone(),
            kind: format!("{:?}", job.kind),
        }
        .into());
    }
    own.finish(namespace_jobs, settled, at)
}

/// Step 9: install the sealed image on the members the rule names. Returns
/// the acknowledging members once they satisfy the rule, or what the
/// install waits for.
fn install<E: ShardConversionEnv>(
    env: &E,
    files: &AttemptFiles,
    shard: &DfsMetadataShardConfig,
    rule: &DfsMetadataShardConversionInstallRule,
    attempt: &DfsMetadataShardConversion,
    built: &BuiltImage,
    local: &NodeId,
) -> Result<std::result::Result<BTreeSet<NodeId>, String>> {
    let mut acks: BTreeMap<NodeId, DfsMetadataGenerationSeal> =
        files.read("install-acks.json")?.unwrap_or_default();
    acks.retain(|_, seal| *seal == built.seal);
    let image = sinomo_dfs::inspect_metadata_candidate(&files.image())?;
    if image.prefix_proof != built.seal.prefix_proof
        || image.state_fingerprint != built.seal.state_fingerprint
        || image.file_fingerprint != built.seal.file_fingerprint
    {
        return Err(error("the attempt's image differs from its seal"));
    }
    acks.insert(local.clone(), built.seal.clone());
    let source: ConversionSourceCut = files
        .read(SOURCE_CUT)?
        .ok_or_else(|| error("a built conversion attempt lost its captured source"))?;
    if source_cut_digest(&source.source, &source.projection_fingerprint)? != attempt.source.source_cut_digest {
        return Err(error("the attempt's recorded source differs from its source-cut digest"));
    }
    let mut unavailable = BTreeMap::new();
    for node in rule.install_set() {
        if acks.contains_key(&node) {
            continue;
        }
        match env.install(&node, shard, attempt, &files.image(), &source)? {
            InstallReply::Installed => {
                acks.insert(node, built.seal.clone());
            }
            InstallReply::Unavailable { reason } => {
                unavailable.insert(node, reason);
            }
        }
    }
    files.write("install-acks.json", &acks)?;
    let acknowledged = acks
        .keys()
        .filter(|node| rule.install_set().contains(*node))
        .cloned()
        .collect::<BTreeSet<_>>();
    if rule.satisfied_by(&acknowledged) {
        return Ok(Ok(acknowledged));
    }
    env.watch_nodes(unavailable.keys().cloned().collect())?;
    Ok(Err(format!(
        "the install rule is not met; waiting for {}",
        unavailable
            .iter()
            .map(|(node, reason)| format!("{node} ({reason})"))
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// The native environment of one shard's conversion on this owner.
pub(in crate::authority) struct OwnerConversionEnv<'a, C> {
    owner: &'a MigrationOwner,
    unit: &'a inventory::UnitKey,
    source: C,
    /// Every key and node this step watches; the scheduler keeps one set
    /// per unit, so each registration carries the union.
    keys: std::cell::RefCell<BTreeSet<String>>,
    nodes: std::cell::RefCell<BTreeSet<NodeId>>,
}

impl<'a, C: ContributionSource> OwnerConversionEnv<'a, C> {
    pub(super) fn new(owner: &'a MigrationOwner, unit: &'a inventory::UnitKey, source: C) -> Self {
        Self { owner, unit, source, keys: Default::default(), nodes: Default::default() }
    }
}

impl<C: ContributionSource> ShardConversionEnv for OwnerConversionEnv<'_, C> {
    type Source = C;
    fn local_node(&self) -> Result<NodeId> {
        self.owner.authority.local_node()
    }
    fn data_root(&self) -> &Path {
        &self.owner.authority.data_root
    }
    /// sinomo:kv-consistency: A conversion step binds its shard's serving, write-set, handoff and storage records and its members' records in one exact-set cut.
    fn read_exact(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>> {
        let (entries, _) = self.owner.authority.read_exact(keys)?;
        Ok(entries.into_iter().map(|entry| entry.entry).collect())
    }
    /// sinomo:kv-consistency: Contribution rounds attribute rows with the filesystem's complete shard catalog.
    fn catalog(&self, file_system_id: &DfsFileSystemId) -> Result<Vec<DfsMetadataShardConfig>> {
        let prefix = sinomo_dfs::metadata_shards_prefix(file_system_id);
        let authority = &self.owner.authority;
        let read = authority.read_native(
            authority.cluster()?.kv_range_prefix_complete_authoritative_async(prefix.clone()),
        )?;
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(&prefix, &read)
            .map_err(error)?;
        let mut catalog = read
            .value
            .iter()
            .map(|entry| entry.decode::<DfsMetadataShardConfig>().map_err(error))
            .collect::<Result<Vec<_>>>()?;
        catalog.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        if catalog.iter().any(|shard| shard.file_system_id != *file_system_id) {
            return Err(error("shard catalog names another filesystem"));
        }
        Ok(catalog)
    }
    fn submit(&self, request: &KvTxnRequest) -> Result<Submitted> {
        let commit = match self.owner.authority.commit_evaluated(request.clone()) {
            Ok(commit) => commit,
            Err(NativeEvaluationError::Native(failure)) => {
                return Err(self.owner.authority.native_read_error(failure));
            }
            Err(NativeEvaluationError::Domain(failure)) => return Err(failure),
        };
        let succeeded = commit.result.txn_response.as_ref().is_some_and(|response| response.succeeded);
        if succeeded {
            NativeImportAuthority::require_committed(&commit)?;
            return Ok(Submitted::Committed);
        }
        // A failed compare is decided only under current authority.
        current_authority(&commit.result.authority)?;
        Ok(Submitted::Rejected)
    }
    fn local_copy_current(&self, shard: &DfsMetadataShardConfig) -> Result<bool> {
        let root = self.owner.metadata_root.join(&shard.shard_id);
        Ok(sinomo_dfs::inspect_metadata_storage_generation(&root).is_ok_and(|header| {
            header.identity.shard_id.0 == shard.shard_id
                && (matches!(header.state, sinomo_dfs::DfsMetadataGenerationState::Active { .. })
                    || matches!(
                        header.identity.origin,
                        sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesis
                            | sinomo_dfs::DfsMetadataGenerationOrigin::NativeGenesisInGeneration { .. }
                    ))
        }))
    }
    fn capture(&self, shard: &DfsMetadataShardConfig) -> Result<CaptureProgress> {
        let physical = sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id);
        let Some(placement) = self.placement_evidence(shard)? else {
            return Ok(CaptureProgress::Pending { processed_records: 1 });
        };
        let destination = self.owner.metadata_root.join(&shard.shard_id);
        // The shard's own copy, or the combined copy that holds its rows.
        let source = crate::metadata::combined_root::shard_conversion_source(&destination, &physical)?
            .ok_or_else(|| error("the shard has no copy to convert"))?;
        let (progress, captured) = self.owner.metadata.capture_shard_conversion_source(
            &destination,
            &source,
            &physical,
            &shard.file_system_id,
            &placement,
            sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET,
        )?;
        Ok(match (captured, progress) {
            (Some(captured), _) => CaptureProgress::Captured(captured),
            (None, sinomo_dfs::DfsStoragePreparation::Pending { processed_records }) => {
                CaptureProgress::Pending { processed_records }
            }
            (None, sinomo_dfs::DfsStoragePreparation::Ready) => {
                return Err(error("the shard's copy has no source to convert"));
            }
        })
    }
    /// Fixed by the first capture: the share recorded beside the capture,
    /// otherwise the share of the goals captured now.
    fn placement_evidence(
        &self,
        shard: &DfsMetadataShardConfig,
    ) -> Result<Option<Vec<crate::metadata::RetainedPlacementReservation>>> {
        let physical = sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id);
        let destination = self.owner.metadata_root.join(&shard.shard_id);
        if let Some(recorded) = self.owner.metadata.recorded_placement_evidence(&destination, &physical)? {
            return Ok(Some(recorded));
        }
        let Some(goals) = self
            .owner
            .authority
            .capture_original_placement_goals(sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET)?
        else {
            return Ok(None);
        };
        crate::authority::placement_work::shard_placement_share(&goals, &shard.file_system_id, &physical).map(Some)
    }
    fn contribution_source(&self) -> &C {
        &self.source
    }
    fn block_on<F: Future>(&self, future: F) -> Result<F::Output> {
        block_native(&self.owner.authority.runtime, async {
            Ok::<_, MetadataBackendError>(future.await)
        })
    }
    fn install(
        &self,
        node: &NodeId,
        shard: &DfsMetadataShardConfig,
        attempt: &DfsMetadataShardConversion,
        image: &Path,
        source: &ConversionSourceCut,
    ) -> Result<InstallReply> {
        if *node == self.local_node()? {
            return Ok(InstallReply::Installed);
        }
        self.owner.authority.install_conversion_image(node, shard, attempt, image, source)
    }
    fn activate_local(
        &self,
        shard: &DfsMetadataShardConfig,
        record: &DfsMetadataShardStorage,
        image: &Path,
    ) -> Result<()> {
        let physical = sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id);
        let destination = self.owner.metadata_root.join(&shard.shard_id);
        let (attempt, _) = record
            .converted_activation()
            .ok_or_else(|| error("local activation requires an activated conversion"))?;
        self.owner.metadata.finish_shard_conversion_capture(&destination, &physical)?;
        let retained = self
            .owner
            .metadata_root
            .join(".metadata-retained")
            .join(format!("shard-conversion-{}", attempt.attempt_id));
        install_activated_image(&destination, image, &retained, &record.generation)?;
        block_native(
            &self.owner.authority.runtime,
            sinomo_dfs::activate_metadata_shard_storage(
                &self.owner.authority.component_context,
                &destination,
                &record.generation,
            ),
        )
    }
    fn watch_keys(&self, keys: BTreeSet<String>) -> Result<()> {
        let mut watched = self.keys.borrow_mut();
        watched.extend(keys);
        self.owner.work.scheduler.lock().map_err(error)?.watch_native_keys(self.unit, watched.clone())
    }
    fn watch_nodes(&self, nodes: BTreeSet<NodeId>) -> Result<()> {
        let mut watched = self.nodes.borrow_mut();
        watched.extend(nodes);
        self.owner.work.watch_reachable_nodes(self.unit, watched.clone())
    }
    fn takeover(&self) -> Option<&dyn takeover::TakeoverEnv> {
        Some(self)
    }
}

/// Put an activated attempt's sealed image in place of the shard's local
/// copy, which is retained beside it, never deleted. Each rename is durable
/// before the next, and a repeat after any interruption finishes the same
/// exchange: the copy moves once, then the image moves into its place.
pub(super) fn install_activated_image(
    destination: &Path,
    image: &Path,
    retained: &Path,
    generation: &sinomo_dfs::DfsMetadataGenerationIdentity,
) -> Result<()> {
    if sinomo_dfs::inspect_metadata_storage_generation(destination)
        .is_ok_and(|header| header.identity == *generation)
    {
        return Ok(());
    }
    if !image.join("metadata-generation.sobj").is_file() {
        return Err(error("the activated attempt has no image to install"));
    }
    let parent = destination.parent().ok_or_else(|| error("metadata root has no parent"))?;
    let retained_parent = retained.parent().ok_or_else(|| error("retained copy has no parent"))?;
    if destination.try_exists().map_err(error)? {
        if retained.try_exists().map_err(error)? {
            return Err(error("the shard's retained copy already exists beside a live copy"));
        }
        sinomo_core::storage::durable_file::ensure_directory(retained_parent).map_err(error)?;
        std::fs::rename(destination, retained).map_err(error)?;
        sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
        sinomo_core::storage::durable_file::sync_directory(retained_parent).map_err(error)?;
    }
    std::fs::rename(image, destination).map_err(error)?;
    sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
    if let Some(image_parent) = image.parent() {
        sinomo_core::storage::durable_file::sync_directory(image_parent).map_err(error)?;
    }
    Ok(())
}

/// Which conversion a `Metadata{shard}` unit runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetadataUnitDispatch {
    /// The per-shard driver.
    PerShard,
    /// The complete-filesystem conversion, kept compiled until it is removed.
    CompleteFileSystem,
}

/// The conversion a shard's unit runs, from its storage record.
///
/// - A native-genesis record holds no old storage; its unit keeps the
///   complete-filesystem path, which leaves a native-genesis copy to the core.
/// - Every other shard runs the per-shard driver: a copy without a record,
///   legacy or not, a shard whose rows live only in the legacy combined
///   `root` copy (`crate::metadata::combined_root`), and every shard whose
///   conversion already wrote a record.
///   The per-shard stage reconciles a legacy member's acknowledgement floors,
///   including those only a local checkpoint capture anchors.
pub(super) fn metadata_unit_dispatch(record: Option<&DfsMetadataShardStorage>) -> MetadataUnitDispatch {
    match record {
        Some(DfsMetadataShardStorage {
            state:
                sinomo_dfs::DfsMetadataShardStorageState::Active {
                    activation: sinomo_dfs::DfsMetadataShardActivation::NativeGenesis,
                },
            ..
        }) => MetadataUnitDispatch::CompleteFileSystem,
        _ => MetadataUnitDispatch::PerShard,
    }
}

impl MigrationOwner {
    /// Advance the `Metadata{shard}` unit with the conversion
    /// [`metadata_unit_dispatch`] chooses.
    pub(super) fn advance_metadata_unit(
        &self,
        unit: &inventory::UnitKey,
        shard: &sinomo_dfs::DfsMetadataShardId,
    ) -> Result<sinomo_dfs::DfsStoragePreparation> {
        use sinomo_dfs::DfsStoragePreparation as State;
        let Some(file_system_id) = self.shard_file_system(shard)? else {
            return Ok(State::Pending { processed_records: 0 });
        };
        let key = sinomo_dfs::metadata_shard_storage_key(&file_system_id, &shard.0);
        let (entries, _) = self.authority.read_exact(std::slice::from_ref(&key))?;
        let record = entries[0]
            .entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardStorage>().map_err(error))
            .transpose()?;
        match metadata_unit_dispatch(record.as_ref()) {
            MetadataUnitDispatch::CompleteFileSystem => self.queue_filesystem_for_shard(shard),
            MetadataUnitDispatch::PerShard => self.advance_shard_unit(unit, shard, &file_system_id),
        }
    }

    /// Run one bounded step of the shard's conversion. The shard's current
    /// primary converts, or a member in the place of a lost primary
    /// ([`takeover`]); on any other holder the step parks, and the local copy
    /// is activated by its own installed image or repaired by snapshot.
    fn advance_shard_unit(
        &self,
        unit: &inventory::UnitKey,
        shard: &sinomo_dfs::DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
    ) -> Result<sinomo_dfs::DfsStoragePreparation> {
        use sinomo_dfs::DfsStoragePreparation as State;
        let root = self.metadata_root.join(&shard.0);
        if crate::metadata::combined_root::shard_conversion_source(&root, shard)?.is_none() {
            return Ok(State::Pending { processed_records: 0 });
        }
        let file_system_id = file_system_id.clone();
        let progress = self.drive_shard_conversion(unit, &file_system_id, &shard.0)?;
        Ok(match progress {
            ConversionProgress::Ready => State::Ready,
            ConversionProgress::Pending { processed_records } => State::Pending { processed_records },
            ConversionProgress::Handover | ConversionProgress::Parked { .. } => {
                State::Pending { processed_records: 0 }
            }
        })
    }

    fn drive_shard_conversion(
        &self,
        unit: &inventory::UnitKey,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<ConversionProgress> {
        let source = crate::authority::contribution_sources::NativeContributionSource::new(Arc::clone(&self.authority));
        advance_shard_conversion(&OwnerConversionEnv::new(self, unit, source), file_system_id, shard_id)
    }

    /// The filesystem whose catalog names `shard`, from one complete
    /// authoritative read of the filesystem records. A physical shard
    /// belongs to at most one filesystem.
    /// sinomo:kv-consistency: A physical metadata shard is attributed to its filesystem from the complete filesystem catalog.
    fn shard_file_system(&self, shard: &sinomo_dfs::DfsMetadataShardId) -> Result<Option<DfsFileSystemId>> {
        let prefix = "/dfs/filesystems/";
        let read = self.authority.read_native(
            self.authority.cluster()?.kv_range_prefix_complete_authoritative_async(prefix.into()),
        )?;
        sinomo_core::kv::validate_cluster_kv_authoritative_complete_range_prefix_read(prefix, &read)
            .map_err(error)?;
        let mut owners = Vec::new();
        for entry in &read.value {
            if entry.key.rsplit_once("/metadata-shards/").is_some_and(|(_, tail)| tail == shard.0) {
                let config: DfsMetadataShardConfig = entry.decode().map_err(error)?;
                if entry.key != sinomo_dfs::metadata_shard_key(&config.file_system_id, &config.shard_id) {
                    return Err(error("shard record key differs from its body"));
                }
                owners.push(config.file_system_id);
            }
        }
        match owners.as_slice() {
            [] => Ok(None),
            [file_system_id] => Ok(Some(file_system_id.clone())),
            _ => Err(error("a physical metadata shard is named by more than one filesystem")),
        }
    }
}
