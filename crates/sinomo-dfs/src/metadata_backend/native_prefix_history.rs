//! One cfg(test) immutable historical DATA slot. No disk representation can
//! reconstruct a private native return/round or grant current authority.
use super::*;
use crate::node_transport::OwnedNativeMetadataPrefixRound;
use crate::publication_guard::{DfsPublicationExecutionOwners, DfsPublicationExecutionProvider};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::time::Instant;

pub(crate) const NATIVE_PREFIX_HISTORY_FILE: &str = "native-prefix-history-v1.sobj";
const MAX_BYTES: u64 = 1024 * 1024;
const CLASSIFICATION: &str = "historical-native-prefix-round-data-only-v1";

// This whole owner is cfg(test). The cut belongs only to the invoking thread,
// and observes a real successful storage return; it cannot replace that return.
type AfterPublicationCut = Box<dyn FnOnce(&Path, Instant)>;
thread_local! {
    static AFTER_PUBLICATION_CUT: std::cell::RefCell<Option<AfterPublicationCut>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn with_after_publication_cut_for_test<T>(
    action: impl FnOnce(&Path, Instant) + 'static,
    run: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<AfterPublicationCut>);
    impl Drop for Restore {
        fn drop(&mut self) {
            AFTER_PUBLICATION_CUT.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(AFTER_PUBLICATION_CUT.with(|slot| {
        slot.borrow_mut().replace(Box::new(action))
    }));
    run()
}

fn after_real_publication_for_test(path: &Path, deadline: Instant) {
    let action = AFTER_PUBLICATION_CUT.with(|slot| slot.borrow_mut().take());
    if let Some(action) = action {
        action(path, deadline);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum NativePrefixHistoryDeadlineQualification {
    OriginalProcessLocalDeadlineEnforcedNotReconstructible,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativePrefixHistoryPhysicalOwner {
    pub(crate) data_root: PathBuf,
    pub(crate) data_identity: (u64, u64),
    pub(crate) owner_identity: (u64, u64),
    pub(crate) shard_root: PathBuf,
    pub(crate) shard_identity: (u64, u64),
}
impl From<&crate::runtime_host::NativeMetadataPrefixPhysicalOwner>
    for NativePrefixHistoryPhysicalOwner
{
    fn from(value: &crate::runtime_host::NativeMetadataPrefixPhysicalOwner) -> Self {
        Self {
            data_root: value.data_root.clone(),
            data_identity: value.data_identity,
            owner_identity: value.owner_identity,
            shard_root: value.shard_root.clone(),
            shard_identity: value.shard_identity,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativePrefixHistorySource {
    assignment: DfsMetadataShardConfig,
    prefix: DfsMetadataPrefixProof,
    physical: NativePrefixHistoryPhysicalOwner,
}
impl NativePrefixHistorySource {
    pub(crate) fn prefix(&self) -> &DfsMetadataPrefixProof {
        &self.prefix
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativePrefixHistoryPeer {
    peer: NodeId,
    command: crate::DfsMetadataJournalBatchCommand,
    attempt_id: String,
    response: crate::DfsMetadataJournalBatchResponse,
}
impl NativePrefixHistoryPeer {
    pub(crate) fn peer(&self) -> &NodeId {
        &self.peer
    }
    pub(crate) fn command(&self) -> &crate::DfsMetadataJournalBatchCommand {
        &self.command
    }
    pub(crate) fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
    pub(crate) fn response(&self) -> &crate::DfsMetadataJournalBatchResponse {
        &self.response
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativePrefixHistoryRecord {
    format_version: u32,
    classification: String,
    round_id: String,
    target: crate::DfsMetadataQuorumSyncTarget,
    original_deadline: NativePrefixHistoryDeadlineQualification,
    peers: Vec<NativePrefixHistoryPeer>,
    source: NativePrefixHistorySource,
}
impl NativePrefixHistoryRecord {
    pub(crate) fn round_id(&self) -> &str {
        &self.round_id
    }
    pub(crate) fn target(&self) -> &crate::DfsMetadataQuorumSyncTarget {
        &self.target
    }
    pub(crate) fn peers(&self) -> &[NativePrefixHistoryPeer] {
        &self.peers
    }
    pub(crate) fn source(&self) -> &NativePrefixHistorySource {
        &self.source
    }
    pub(crate) fn deadline_qualification(&self) -> &NativePrefixHistoryDeadlineQualification {
        &self.original_deadline
    }
    fn from_round(round: &OwnedNativeMetadataPrefixRound) -> Result<Self> {
        if !round.is_complete() {
            return Err(invalid(
                "only a complete private native round may enter historical intake",
            ));
        }
        let source = round
            .common_source_observation()
            .ok_or_else(|| invalid("complete native round has no common source"))?;
        let peers = round
            .peer_outcomes()
            .iter()
            .map(|slot| {
                let native = slot
                    .native_return()
                    .ok_or_else(|| invalid("complete round lacks an actual native peer return"))?;
                native
                    .validate_binding(
                        slot.peer(),
                        slot.command(),
                        &source.physical_owner().data_root,
                    )
                    .map_err(|e| invalid(&e.to_string()))?;
                Ok(NativePrefixHistoryPeer {
                    peer: slot.peer().clone(),
                    command: slot.command().clone(),
                    attempt_id: native.attempt_id().to_owned(),
                    response: native.response().clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let value=Self {format_version:1,classification:CLASSIFICATION.into(),round_id:round.round_id().to_owned(),target:round.target().clone(),
            original_deadline:NativePrefixHistoryDeadlineQualification::OriginalProcessLocalDeadlineEnforcedNotReconstructible,
            peers,source:NativePrefixHistorySource {assignment:source.assignment().clone(),prefix:source.prefix().clone(),physical:source.physical_owner().into()}};
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        let assignment = &self.source.assignment;
        let expected = assignment
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if self.format_version != 1
            || self.classification != CLASSIFICATION
            || self.round_id.is_empty()
            || self.target.barrier_id.is_empty()
            || self.target.state_fingerprint.is_empty()
            || assignment.epoch == 0
            || self.target.checkpoint_snapshot.is_some()
            || self.target.expected_shard.as_ref() != Some(assignment)
            || self.target.primary_node_id != assignment.primary_node_id
            || self.target.file_system_id != assignment.file_system_id
            || self.target.shard_id.0 != assignment.shard_id
            || self.target.authority_epoch != Some(assignment.epoch)
            || assignment.replica_node_ids.len() != 2
            || expected.len() != 2
            || expected.contains(&assignment.primary_node_id)
            || self.target.replica_node_ids.len() != 2
            || self
                .target
                .replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                != expected
            || self.peers.len() != 2
            || !self.source.prefix.is_well_formed()
            || self.source.prefix.journal_sequence != self.target.journal_sequence
            || self.source.prefix.digest != self.target.state_fingerprint
        {
            return Err(invalid(
                "historical prefix record has an unsupported or inconsistent scope",
            ));
        }
        let mut peers = BTreeSet::new();
        let mut attempts = BTreeSet::new();
        for (index, (peer, expected_peer)) in self.peers.iter().zip(expected.iter()).enumerate() {
            crate::node_transport::validate_ordinary_metadata_prefix_command(&peer.command)
                .map_err(|e| invalid(&e.to_string()))?;
            let probe = peer
                .command
                .prefix_probe
                .as_ref()
                .expect("validated prefix");
            if &peer.peer != expected_peer
                || !peers.insert(peer.peer.clone())
                || peer.attempt_id.is_empty()
                || !attempts.insert(&peer.attempt_id)
                || peer.command.request_id
                    != format!("dfs_native_prefix_round_{}_{index}", self.round_id)
                || peer.command.source_node_id.as_ref() != Some(&assignment.primary_node_id)
                || peer.command.file_system_id.as_ref() != Some(&assignment.file_system_id)
                || peer.command.authority_epoch != Some(assignment.epoch)
                || peer.command.batch.shard_id != self.target.shard_id
                || probe.barrier_id != format!("{}-{}", self.target.barrier_id, self.round_id)
                || probe.journal_sequence != self.target.journal_sequence
                || probe.state_fingerprint != self.target.state_fingerprint
                || peer.response.request_id != peer.command.request_id
                || peer.response.shard_id != self.target.shard_id
                || peer.response.next_after_sequence != self.target.journal_sequence
                || peer.response.applied_entries != 0
                || !peer.response.verified_prefix_only
                || peer.response.verified_authority_probe_id.is_some()
                || peer.response.result != crate::DfsMetadataJournalBatchResult::Applied
            {
                return Err(invalid(
                    "historical prefix peer DATA is malformed or foreign",
                ));
            }
        }
        Ok(())
    }
    fn semantic_matches(&self, other: &Self) -> bool {
        // Fresh transport correlation/deadline never overwrites the first
        // observation. This does not certify a current table generation.
        self.source == other.source
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativePrefixHistoryDisposition {
    WrittenAndSynced,
    ExistingHistoricalData,
}
#[derive(Debug)]
pub(crate) struct NativePrefixHistoryIntake {
    record: NativePrefixHistoryRecord,
    disposition: NativePrefixHistoryDisposition,
}
impl NativePrefixHistoryIntake {
    pub(crate) fn record(&self) -> &NativePrefixHistoryRecord {
        &self.record
    }
    pub(crate) fn disposition(&self) -> NativePrefixHistoryDisposition {
        self.disposition
    }
}
fn invalid(message: &str) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("native prefix historical DATA: {message}"))
}
fn remaining(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(invalid("intake original deadline expired"))
    } else {
        Ok(())
    }
}
fn directory_identity(path: &Path) -> Result<(u64, u64)> {
    let named = std::fs::symlink_metadata(path).map_err(|e| invalid(&e.to_string()))?;
    if !named.is_dir() || named.file_type().is_symlink() {
        return Err(invalid("actual physical root is not a directory"));
    }
    let file = std::fs::File::open(path).map_err(|e| invalid(&e.to_string()))?;
    let identity =
        crate::runtime_host::storage_file_identity(&file).map_err(|e| invalid(&e.to_string()))?;
    let after = std::fs::symlink_metadata(path).map_err(|e| invalid(&e.to_string()))?;
    if !after.is_dir() || after.file_type().is_symlink() {
        return Err(invalid("physical directory changed while opening"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (named.dev(), named.ino()) != identity || (after.dev(), after.ino()) != identity {
            return Err(invalid("physical directory changed while opening"));
        }
    }
    Ok(identity)
}
fn check_physical(root: &Path, record: &NativePrefixHistoryRecord) -> Result<()> {
    let canonical = std::fs::canonicalize(root).map_err(|e| invalid(&e.to_string()))?;
    let owner = crate::runtime_host::publication_runtime_owner(&canonical)
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("actual native DATA owner is unavailable"))?;
    let expected = &record.source.physical;
    if canonical != expected.shard_root
        || owner.physical_data_root() != expected.data_root
        || directory_identity(&canonical)? != expected.shard_identity
        || directory_identity(owner.physical_data_root())? != expected.data_identity
        || owner
            .storage_owner_file_identity()
            .map_err(|e| invalid(&e.to_string()))?
            != expected.owner_identity
    {
        return Err(invalid(
            "copied/foreign/replaced historical owner cannot be rebound",
        ));
    }
    let path = owner.physical_data_root().join("daemon/data-owner.lock");
    let meta = std::fs::symlink_metadata(&path).map_err(|e| invalid(&e.to_string()))?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(invalid("named native DATA owner is unavailable"));
    }
    let file = std::fs::File::open(path).map_err(|e| invalid(&e.to_string()))?;
    if crate::runtime_host::storage_file_identity(&file).map_err(|e| invalid(&e.to_string()))?
        != expected.owner_identity
    {
        return Err(invalid(
            "named historical owner differs from actual held owner",
        ));
    }
    Ok(())
}

impl LocalSobjMetadataBackend {
    pub(super) fn read_native_prefix_history_owned(
        &self,
    ) -> Result<Option<NativePrefixHistoryRecord>> {
        let path = self.root.join(NATIVE_PREFIX_HISTORY_FILE);
        let before = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(invalid(&e.to_string())),
        };
        if !before.is_file() || before.file_type().is_symlink() || before.len() > MAX_BYTES {
            return Err(invalid("historical slot is not a bounded regular file"));
        }
        let file = std::fs::File::open(&path).map_err(|e| invalid(&e.to_string()))?;
        if !crate::repair_queue_store::same_file_observation(
            &before,
            &file.metadata().map_err(|e| invalid(&e.to_string()))?,
        ) {
            return Err(invalid("historical slot changed while opening"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| invalid(&e.to_string()))?;
        let after = std::fs::symlink_metadata(&path).map_err(|e| invalid(&e.to_string()))?;
        if bytes.len() as u64 != before.len()
            || !crate::repair_queue_store::same_file_observation(&before, &after)
        {
            return Err(invalid("historical slot changed while reading"));
        }
        let record: NativePrefixHistoryRecord = sinomo_core::object::serde_ext::from_bytes(&bytes)
            .map_err(|e| invalid(&e.to_string()))?;
        record.validate()?;
        if record.target.shard_id != self.shard_id
            || crate::metadata_io::encode_sobj(&record)? != bytes
        {
            return Err(invalid("historical slot scope or canonical codec differs"));
        }
        check_physical(&self.root, &record)?;
        Ok(Some(record)) // DATA only: readable bytes do not renew fsync/ACK.
    }

    pub(super) fn retain_native_prefix_history_owned(
        &mut self,
        round: &OwnedNativeMetadataPrefixRound,
        provider: &DfsPublicationExecutionProvider,
        execution: &DfsPublicationExecutionOwners,
        local: &NodeId,
        deadline: Instant,
    ) -> Result<NativePrefixHistoryIntake> {
        remaining(deadline)?;
        if !self.stat_read_snapshots.is_empty() || self.consistent_read_snapshot_depth != 0 {
            return Err(invalid(
                "a retained metadata read scope grants no historical writer custody",
            ));
        }
        let candidate = NativePrefixHistoryRecord::from_round(round)?;
        let expected = &candidate.source.assignment;
        let scope = self.publication_scope_for_backend(
            &expected.file_system_id,
            &DfsMetadataShardId::new(&expected.shard_id),
        )?;
        provider.validate_prefix_history_writer(execution, &scope, expected, local, deadline)?;
        check_physical(&self.root, &candidate)?;
        let _writer = loop {
            remaining(deadline)?;
            if let Some(lock) = crate::metadata_lock::try_metadata_write_lock(
                self.root.clone(),
                self.journal_write_lock_path(),
            )? {
                break lock;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        self.validate_retained_metadata_writer()?;
        if self.instance_authority_epoch != Some(expected.epoch) {
            return Err(invalid(
                "current metadata instance is not bound to the expected epoch",
            ));
        }
        provider.validate_prefix_history_writer(execution, &scope, expected, local, deadline)?;
        check_physical(&self.root, &candidate)?;
        if let Some(existing) = self.read_native_prefix_history_owned()? {
            if !existing.semantic_matches(&candidate) {
                return Err(invalid(
                    "immutable historical slot conflicts with this semantic source/assignment/prefix/owner",
                ));
            }
            remaining(deadline)?;
            return Ok(NativePrefixHistoryIntake {
                record: existing,
                disposition: NativePrefixHistoryDisposition::ExistingHistoricalData,
            });
        }
        let bytes = crate::metadata_io::encode_sobj(&candidate)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("historical slot exceeds its encoded bound"));
        }
        remaining(deadline)?;
        let path = self.root.join(NATIVE_PREFIX_HISTORY_FILE);
        let published = (|| -> Result<()> {
            sinomo_core::storage::durable_file::write_file_create_new_atomic(
                &path,
                &bytes,
                sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("tmp", "tmp")
                    .sync_all(),
            )
            .map_err(crate::metadata_io::metadata_durable_file_error)?;
            after_real_publication_for_test(&path, deadline);
            provider
                .validate_prefix_history_writer(execution, &scope, expected, local, deadline)?;
            if self.read_native_prefix_history_owned()?.as_ref() != Some(&candidate) {
                return Err(invalid(
                    "published historical bytes differ from actual input",
                ));
            }
            remaining(deadline)
        })();
        if let Err(error) = published {
            return Err(invalid(&format!(
                "publication/durability uncertain; retained slot is not rolled back: {error}"
            )));
        }
        Ok(NativePrefixHistoryIntake {
            record: candidate,
            disposition: NativePrefixHistoryDisposition::WrittenAndSynced,
        })
    }
}
