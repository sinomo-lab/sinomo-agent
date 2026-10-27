//! Exact repair DATA carried by the existing frozen-prefix handoff.
//!
//! A page is not author permission. Its receiver must first validate the
//! authenticated retiring primary, pending role rotation and frozen prefix.
//! The ordinary joint-prefix certificate is acknowledged only after all pages
//! are durably staged. The successor installs them through its existing queue
//! during proof-bound rebind, before it can author the activated generation.

use crate::repair_queue_store::{DfsRepairQueueKey, LocalDfsRepairQueueStore};
use crate::{DfsError, DfsMetadataShardMembershipHandoff, DfsRepairAttemptRecord};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;

const VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsRepairHandoffCut {
    pub version: u32,
    pub handoff: DfsMetadataShardMembershipHandoff,
    pub queue_sequence: u64,
    pub page_count: u64,
    pub record_count: u64,
    pub digest: String,
}

impl DfsRepairHandoffCut {
    fn validate(&self) -> Result<(), DfsError> {
        if self.version != VERSION
            || self.page_count == 0
            || self.page_count > self.record_count.max(1)
            || self.record_count
                > self
                    .page_count
                    .saturating_mul(crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT as u64)
            || self.digest.len() != 64
            || !self.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.handoff.handoff_id.is_empty()
            || self.handoff.retiring_primary_fence.is_none()
            || self.handoff.retiring_shard.primary_node_id
                == self.handoff.desired_shard.primary_node_id
            || self.handoff.retiring_shard.file_system_id
                != self.handoff.desired_shard.file_system_id
            || self.handoff.retiring_shard.shard_id != self.handoff.desired_shard.shard_id
            || self.handoff.retiring_shard.epoch.checked_add(1)
                != Some(self.handoff.desired_shard.epoch)
        {
            return Err(invalid(
                "unsupported or incomplete original repair custody cut",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsRepairHandoffPage {
    pub cut: DfsRepairHandoffCut,
    pub ordinal: u64,
    pub previous_digest: String,
    pub records: Vec<DfsRepairAttemptRecord>,
}

impl DfsRepairHandoffPage {
    pub(crate) fn digest(&self) -> Result<String, DfsError> {
        // The final cut digest is excluded from the chain's own input.
        // SOBJ lists are homogeneous. Use a named object for these differently
        // typed fields rather than a heterogeneous tuple/list.
        #[derive(Serialize)]
        struct DigestInput<'a> {
            version: u32,
            handoff: &'a DfsMetadataShardMembershipHandoff,
            queue_sequence: u64,
            ordinal: u64,
            previous_digest: &'a str,
            records: &'a [DfsRepairAttemptRecord],
        }
        let bytes = crate::metadata_io::encode_sobj(&DigestInput {
            version: VERSION,
            handoff: &self.cut.handoff,
            queue_sequence: self.cut.queue_sequence,
            ordinal: self.ordinal,
            previous_digest: &self.previous_digest,
            records: &self.records,
        })?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    pub(crate) fn confirmation(&self) -> Result<String, DfsError> {
        Ok(format!(
            "{}:repair-v1:{}:{}",
            self.cut.handoff.handoff_id,
            self.ordinal,
            self.digest()?
        ))
    }

    pub(crate) fn validate(&self) -> Result<(), DfsError> {
        self.cut.validate()?;
        if self.ordinal >= self.cut.page_count
            || self.records.len() > crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
            || crate::metadata_io::encode_sobj(self)?.len()
                > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT
            || (self.ordinal == 0 && !self.previous_digest.is_empty())
            || (self.ordinal > 0 && self.previous_digest.len() != 64)
        {
            return Err(invalid(
                "unsupported or incomplete bounded repair handoff page",
            ));
        }
        if self.records.windows(2).any(|pair| {
            DfsRepairQueueKey::from_record(&pair[0]) >= DfsRepairQueueKey::from_record(&pair[1])
        }) {
            return Err(invalid("repair handoff page is not strictly ordered"));
        }
        if self.ordinal + 1 == self.cut.page_count && self.digest()? != self.cut.digest {
            return Err(invalid(
                "repair handoff terminal digest differs from its cut",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

fn directory(root: &Path, handoff: &str, role: &str) -> PathBuf {
    root.join("journal")
        .join("repair-handoffs-v1")
        .join(format!(
            "{}-{role}",
            blake3::hash(handoff.as_bytes()).to_hex()
        ))
}

fn ensure_directory(root: &Path, directory: &Path) -> Result<(), DfsError> {
    // The actual backend root exists already. Persist each new namespace
    // component with the same storage helper, without changing its policy.
    let mut parent = root.to_path_buf();
    for component in directory
        .strip_prefix(root)
        .map_err(|_| invalid("repair custody escaped its physical backend"))?
        .components()
    {
        parent.push(component);
        sinomo_core::storage::durable_file::ensure_directory(&parent)
            .map_err(|error| invalid(&format!("persist repair custody directory: {error}")))?;
    }
    Ok(())
}

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, DfsError> {
    match path.metadata() {
        Ok(metadata)
            if metadata.len() > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT as u64 =>
        {
            return Err(invalid(
                "retained repair custody exceeds the existing page byte limit",
            ));
        }
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(invalid(&format!("stat retained repair custody: {error}")));
        }
        _ => {}
    }
    crate::metadata_io::read_sobj_optional(path).map_err(Into::into)
}

fn write<T: Serialize>(path: &Path, value: &T) -> Result<(), DfsError> {
    crate::metadata_io::write_sobj_atomic(path, value).map_err(Into::into)
}

fn before(deadline: Instant) -> Result<(), DfsError> {
    if Instant::now() >= deadline {
        Err(invalid(
            "repair custody transfer exhausted the original handoff deadline",
        ))
    } else {
        Ok(())
    }
}

/// One immutable disk-backed source cut; pages are read individually.
pub(crate) struct RepairHandoffExport {
    pub(crate) cut: DfsRepairHandoffCut,
    directory: PathBuf,
}

impl RepairHandoffExport {
    pub(crate) fn page(&self, ordinal: u64) -> Result<DfsRepairHandoffPage, DfsError> {
        let mut page: DfsRepairHandoffPage = read(&self.directory.join(format!("{ordinal}.sobj")))?
            .ok_or_else(|| invalid("original repair handoff page is unavailable"))?;
        if page.cut.version != self.cut.version
            || page.cut.handoff != self.cut.handoff
            || page.cut.queue_sequence != self.cut.queue_sequence
            || page.ordinal != ordinal
        {
            return Err(invalid(
                "original repair page belongs to another source cut",
            ));
        }
        page.cut = self.cut.clone();
        page.validate()?;
        Ok(page)
    }

    /// The caller retains actual mutation admission and the shared carrier
    /// drain exclusion, after the original metadata freeze is durable.
    pub(crate) fn capture(
        root: &Path,
        handoff: &DfsMetadataShardMembershipHandoff,
        store: &LocalDfsRepairQueueStore,
        mut belongs: impl FnMut(&DfsRepairAttemptRecord) -> Result<bool, DfsError>,
        deadline: Instant,
    ) -> Result<Self, DfsError> {
        let directory = directory(root, &handoff.handoff_id, "source");
        if let Some(cut) = read::<DfsRepairHandoffCut>(&directory.join("cut.sobj"))? {
            cut.validate()?;
            if cut.version != VERSION || cut.handoff != *handoff {
                return Err(invalid(
                    "retained repair custody belongs to another handoff",
                ));
            }
            return Ok(Self { cut, directory });
        }
        ensure_directory(root, &directory)?;
        let mut cut = DfsRepairHandoffCut {
            version: VERSION,
            handoff: handoff.clone(),
            queue_sequence: 0,
            page_count: 0,
            record_count: 0,
            digest: String::new(),
        };
        let mut after = None;
        let mut previous_digest = String::new();
        let mut records = Vec::new();
        let mut encoded_record_bytes = 0usize;
        loop {
            before(deadline)?;
            let (sequence, rows, more) = store.handoff_page(after.as_ref())?;
            if after.is_none() {
                cut.queue_sequence = sequence;
            }
            if sequence != cut.queue_sequence {
                return Err(invalid(
                    "repair queue changed while capturing its frozen handoff",
                ));
            }
            for row in rows {
                after = Some(DfsRepairQueueKey::from_record(&row));
                if !belongs(&row)? {
                    continue;
                }
                // Keep the same entry/byte limits as the metadata carrier.
                let row_bytes = crate::metadata_io::encode_sobj(&row)?.len();
                if row_bytes > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT - 4096 {
                    return Err(invalid(
                        "one repair custody row exceeds the existing page budget",
                    ));
                }
                if records.len() == crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
                    || encoded_record_bytes + row_bytes
                        > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT - 4096
                {
                    if records.is_empty() {
                        return Err(invalid(
                            "one repair custody row exceeds the existing page budget",
                        ));
                    }
                    let page = DfsRepairHandoffPage {
                        cut: cut.clone(),
                        ordinal: cut.page_count,
                        previous_digest: previous_digest.clone(),
                        records: std::mem::take(&mut records),
                    };
                    previous_digest = page.digest()?;
                    cut.record_count += page.records.len() as u64;
                    write(&directory.join(format!("{}.sobj", cut.page_count)), &page)?;
                    cut.page_count += 1;
                    encoded_record_bytes = 0;
                }
                encoded_record_bytes += row_bytes;
                records.push(row);
            }
            if !more {
                break;
            }
        }
        before(deadline)?;
        let page = DfsRepairHandoffPage {
            cut: cut.clone(),
            ordinal: cut.page_count,
            previous_digest,
            records,
        };
        cut.digest = page.digest()?;
        cut.record_count += page.records.len() as u64;
        write(&directory.join(format!("{}.sobj", cut.page_count)), &page)?;
        cut.page_count += 1;
        cut.validate()?;
        // Publish the immutable original cut last. Incomplete captures grant
        // no authority and may be rebuilt while the original source is frozen.
        write(&directory.join("cut.sobj"), &cut)?;
        Ok(Self { cut, directory })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Received {
    cut: DfsRepairHandoffCut,
    next_page: u64,
    record_count: u64,
    digest: String,
    last_key: Option<DfsRepairQueueKey>,
    installed_pages: u64,
}

/// Called only after native authenticated prefix/freeze validation. Stage the
/// exact original records durably while the successor is still a replica.
/// They cannot enter its active queue until its normal proof-bound rebind.
pub(crate) fn receive(root: &Path, page: &DfsRepairHandoffPage) -> Result<String, DfsError> {
    page.validate()?;
    let directory = directory(root, &page.cut.handoff.handoff_id, "received");
    ensure_directory(root, &directory)?;
    let state_path = directory.join("received.sobj");
    let mut received = read::<Received>(&state_path)?.unwrap_or(Received {
        cut: page.cut.clone(),
        next_page: 0,
        record_count: 0,
        digest: String::new(),
        last_key: None,
        installed_pages: 0,
    });
    if received.cut != page.cut {
        return Err(invalid("repair handoff cut changed during replay"));
    }
    let page_path = directory.join(format!("{}.sobj", page.ordinal));
    if page.ordinal < received.next_page {
        if read::<DfsRepairHandoffPage>(&page_path)?.as_ref() != Some(page) {
            return Err(invalid(
                "repair handoff replay changed an acknowledged page",
            ));
        }
        return page.confirmation();
    }
    if page.ordinal != received.next_page
        || page.previous_digest != received.digest
        || page.records.first().is_some_and(|first| {
            received
                .last_key
                .as_ref()
                .is_some_and(|last| *last >= DfsRepairQueueKey::from_record(first))
        })
    {
        return Err(invalid(
            "repair handoff page is missing its exact predecessor",
        ));
    }
    let count = received
        .record_count
        .checked_add(page.records.len() as u64)
        .ok_or_else(|| invalid("repair handoff record count overflow"))?;
    if count > page.cut.record_count
        || (page.ordinal + 1 == page.cut.page_count && count != page.cut.record_count)
    {
        return Err(invalid(
            "repair handoff does not cover its complete record count",
        ));
    }
    if let Some(previous) = read::<DfsRepairHandoffPage>(&page_path)? {
        if previous != *page {
            return Err(invalid("repair handoff changed a partially installed page"));
        }
    } else {
        write(&page_path, page)?;
    }
    received.next_page += 1;
    received.record_count = count;
    received.digest = page.digest()?;
    if let Some(last) = page.records.last() {
        received.last_key = Some(DfsRepairQueueKey::from_record(last));
    }
    write(&state_path, &received)?;
    page.confirmation()
}

/// Install one bounded page while the actual successor's native metadata
/// writer is still frozen. Each cursor follows the normal queue fsync. Rebind
/// remains pending until the full cut is installed, including after restart.
pub(crate) fn install_next_page(root: &Path, cut: &DfsRepairHandoffCut) -> Result<bool, DfsError> {
    let owner = crate::runtime_host::publication_runtime_owner(root)
        .map_err(|error| invalid(&error.to_string()))?
        .ok_or_else(|| invalid("repair custody activation has no actual DATA owner"))?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(owner.physical_data_root());
    let state = sinomo_core::cluster::read_cluster_state_without_kv(
        &sinomo_core::network::ClusterPaths::new(&data_dir),
    )
    .map_err(|error| invalid(&error.to_string()))?;
    if state.local_node_id != cut.handoff.desired_shard.primary_node_id {
        return Err(invalid(
            "repair custody activation belongs to another native node",
        ));
    }
    let store =
        LocalDfsRepairQueueStore::new(crate::default_dfs_repair_attempt_snapshot_path(&data_dir));
    let (complete, changed) = install_staged_page(root, cut, &store)?;
    if changed {
        crate::runtime_host::request_daemon_dfs_maintenance_reconciliation(
            &crate::runtime_host::DfsRuntimePaths::new(&data_dir),
        )
        .map_err(|error| invalid(&error.to_string()))?;
    }
    Ok(complete)
}

fn install_staged_page(
    root: &Path,
    cut: &DfsRepairHandoffCut,
    store: &LocalDfsRepairQueueStore,
) -> Result<(bool, bool), DfsError> {
    let directory = directory(root, &cut.handoff.handoff_id, "received");
    let state_path = directory.join("received.sobj");
    let mut received: Received = read(&state_path)?
        .ok_or_else(|| invalid("original repair custody is unavailable at activation"))?;
    if received.cut != *cut
        || received.next_page != cut.page_count
        || received.record_count != cut.record_count
        || received.digest != cut.digest
        || received.installed_pages > cut.page_count
    {
        return Err(invalid(
            "original repair custody is incomplete or changed at activation",
        ));
    }
    if received.installed_pages == cut.page_count {
        return Ok((true, false));
    }
    let page: DfsRepairHandoffPage =
        read(&directory.join(format!("{}.sobj", received.installed_pages)))?.ok_or_else(|| {
            invalid("received original repair page disappeared before activation")
        })?;
    page.validate()?;
    if page.cut != *cut || page.ordinal != received.installed_pages {
        return Err(invalid(
            "received repair page differs from its activation cut",
        ));
    }
    if page.ordinal + 1 < cut.page_count {
        let next: DfsRepairHandoffPage =
            read(&directory.join(format!("{}.sobj", page.ordinal + 1)))?
                .ok_or_else(|| invalid("received original repair page chain is incomplete"))?;
        if next.cut != *cut
            || next.ordinal != page.ordinal + 1
            || next.previous_digest != page.digest()?
        {
            return Err(invalid("received repair page changed after acknowledgment"));
        }
    }
    store.install_handoff_records(&page.records)?;
    received.installed_pages += 1;
    write(&state_path, &received)?;
    Ok((received.installed_pages == cut.page_count, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repair_queue_store::DfsRepairQueueOperation;
    use crate::{
        DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardHandoffCause,
        DfsMetadataShardMembershipHandoffFence,
    };
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::network::NetworkDeliveryState;
    use sinomo_core::node::NodeId;
    use std::time::Duration;

    // Pure persistence DATA tests. These construct no transport, metadata
    // author, publication permit or acknowledged native handoff.
    fn handoff() -> DfsMetadataShardMembershipHandoff {
        let old = NodeId::from_legacy_named("repair-custody-data-old").unwrap();
        let new = NodeId::from_legacy_named("repair-custody-data-new").unwrap();
        let retiring = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: old.clone(),
            replica_node_ids: vec![new.clone()],
            epoch: 4,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: new,
            replica_node_ids: vec![old.clone()],
            epoch: 5,
            ..retiring.clone()
        };
        DfsMetadataShardMembershipHandoff {
            cause: DfsMetadataShardHandoffCause::Membership,
            handoff_id: "repair-custody-data-handoff".into(),
            retiring_shard: retiring,
            desired_shard: desired,
            required_prepared_sequence: 17,
            required_head_watermark: None,
            retiring_primary_fence: Some(DfsMetadataShardMembershipHandoffFence {
                handoff_id: "repair-custody-data-handoff".into(),
                primary_node_id: old,
                journal_sequence: 17,
                state_fingerprint: "data-only-fingerprint".into(),
            }),
        }
    }
    fn row(index: usize) -> DfsRepairAttemptRecord {
        DfsRepairAttemptRecord {
            intent_id: format!("original-{index:04}"),
            object_id: crate::DfsObjectId::from_legacy_named(format!("object-{index}")).unwrap(),
            version: crate::DfsVersion::from_legacy_named(format!("version-{index}")).unwrap(),
            content_hash: format!("blake3:original-{index}"),
            missing_replica_node_id: NodeId::from_legacy_named("repair-custody-data-target")
                .unwrap(),
            attempts: index as u32,
            in_flight: index % 4 == 1,
            last_attempt_unix_nanos: index as u64,
            next_retry_unix_nanos: 1000 + index as u64,
            transfer_request_id: (index % 4 == 1).then(|| format!("original-request-{index}")),
            last_error: Some("retained original diagnostic".into()),
            transfer_delivery_state: match index % 4 {
                0 => Some(NetworkDeliveryState::DefinitelyNotSent),
                1 => Some(NetworkDeliveryState::PossiblyApplied),
                _ => None,
            },
            completed_transfer_request_id: (index % 4 == 2)
                .then(|| format!("original-completion-{index}")),
            transfer_kind: if index % 4 == 3 {
                None
            } else {
                Some(DfsRepairTransferKind::Repair)
            },
        }
    }
    #[test]
    fn pages_preserve_exact_unknown_named_and_completed_custody_through_restart() {
        let root = sinomo_testdir::TestDir::new("repair-handoff-page-restart");
        let source = LocalDfsRepairQueueStore::new(root.join("source-queue.sobj"));
        let destination = LocalDfsRepairQueueStore::new(root.join("destination-queue.sobj"));
        std::fs::create_dir(root.join("source")).unwrap();
        std::fs::create_dir(root.join("target")).unwrap();
        let rows = (0..513).map(row).collect::<Vec<_>>();
        source.append_enqueues(rows.clone()).unwrap();
        let export = RepairHandoffExport::capture(
            &root.join("source"),
            &handoff(),
            &source,
            |_| Ok(true),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(export.cut.page_count, 2);
        assert_eq!(export.cut.record_count, 513);
        let first = export.page(0).unwrap();
        let last = export.page(1).unwrap();
        let target = root.join("target");
        assert!(receive(&target, &last).is_err());
        assert!(destination.view().unwrap().records.is_empty());
        receive(&target, &first).unwrap();
        assert!(destination.view().unwrap().records.is_empty());
        assert!(install_staged_page(&target, &export.cut, &destination).is_err());
        receive(&target, &last).unwrap();
        assert_eq!(
            install_staged_page(&target, &export.cut, &destination).unwrap(),
            (false, true)
        );
        destination.reload_from_disk_on_next_access();
        assert_eq!(destination.view().unwrap().records, rows[..512]);
        // Durable rows survived, but no page receipt yet: replay the exact
        // interrupted install, not a generated successful protocol response.
        let directory = directory(&target, &last.cut.handoff.handoff_id, "received");
        write(&directory.join("1.sobj"), &last).unwrap();
        destination.install_handoff_records(&last.records).unwrap();
        destination.reload_from_disk_on_next_access();
        assert_eq!(
            install_staged_page(&target, &export.cut, &destination).unwrap(),
            (true, true)
        );
        assert_eq!(destination.view().unwrap().records, rows);
        let receipt: Received = read(&directory.join("received.sobj")).unwrap().unwrap();
        assert_eq!(receipt.next_page, export.cut.page_count);
        assert_eq!(receipt.record_count, export.cut.record_count);
        assert_eq!(receipt.digest, export.cut.digest);
        let mut later = rows[0].clone();
        later.transfer_request_id = Some("later-actual-queue-DATA".into());
        later.transfer_delivery_state = None;
        destination
            .append_operations(vec![DfsRepairQueueOperation::Set {
                record: later.clone(),
            }])
            .unwrap();
        receive(&target, &first).unwrap();
        assert_eq!(
            install_staged_page(&target, &export.cut, &destination).unwrap(),
            (true, false)
        );
        assert_eq!(
            destination
                .records_for_targets(&later.intent_id, &[later.missing_replica_node_id.clone()])
                .unwrap(),
            vec![later]
        );
        assert_eq!(source.view().unwrap().records, rows);
    }
    #[test]
    fn conflicting_corrupt_unsupported_and_incomplete_custody_never_acknowledges() {
        let root = sinomo_testdir::TestDir::new("repair-handoff-negative-pages");
        let source = LocalDfsRepairQueueStore::new(root.join("source.sobj"));
        let destination = LocalDfsRepairQueueStore::new(root.join("destination.sobj"));
        std::fs::create_dir(root.join("source")).unwrap();
        std::fs::create_dir(root.join("target")).unwrap();
        std::fs::create_dir(root.join("cancelled")).unwrap();
        let original = row(1);
        source.append_enqueues([original.clone()]).unwrap();
        let export = RepairHandoffExport::capture(
            &root.join("source"),
            &handoff(),
            &source,
            |_| Ok(true),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let page = export.page(0).unwrap();
        let target = root.join("target");
        for kind in 0..4 {
            let mut changed = page.clone();
            match kind {
                0 => changed.cut.version += 1,
                1 => changed.records[0].transfer_request_id = Some("replacement".into()),
                2 => changed.cut.record_count += 1,
                _ => changed.ordinal += 1,
            }
            assert!(receive(&target, &changed).is_err());
            assert!(destination.view().unwrap().records.is_empty());
        }
        let mut conflict = original.clone();
        conflict.transfer_request_id = Some("different-existing-owner".into());
        destination.append_enqueues([conflict.clone()]).unwrap();
        receive(&target, &page).unwrap();
        assert!(install_staged_page(&target, &export.cut, &destination).is_err());
        assert_eq!(destination.view().unwrap().records, vec![conflict]);
        assert_eq!(source.view().unwrap().records, vec![original]);
        assert!(
            RepairHandoffExport::capture(
                &root.join("cancelled"),
                &handoff(),
                &source,
                |_| Ok(true),
                Instant::now()
            )
            .is_err()
        );
    }

    #[test]
    fn custody_marker_roundtrips_and_cannot_downgrade_to_metadata_only_freeze() {
        #[derive(Deserialize)]
        struct OlderMarker {
            handoff_id: String,
        }
        let root = sinomo_testdir::TestDir::new("repair-handoff-marker-compatibility");
        let source = LocalDfsRepairQueueStore::new(root.join("queue.sobj"));
        let export = RepairHandoffExport::capture(
            &root,
            &handoff(),
            &source,
            |_| Ok(true),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let mut marker = crate::DfsMetadataShardWriteFreeze {
            handoff_id: export.cut.handoff.handoff_id.clone(),
            retiring_epoch: 4,
            activated_epoch: None,
            journal_sequence: 0,
            state_fingerprint: "data-only".into(),
            rebind_prefix_proof: crate::metadata_records::dfs_metadata_prefix_proof_genesis(
                &crate::DfsMetadataShardId::new("root"),
            ),
            repair_custody: None,
        };
        let legacy = serde_json::to_value(&marker).unwrap();
        assert_eq!(
            serde_json::from_value::<OlderMarker>(legacy.clone())
                .unwrap()
                .handoff_id,
            marker.handoff_id
        );
        assert_eq!(
            serde_json::from_value::<crate::DfsMetadataShardWriteFreeze>(legacy).unwrap(),
            marker
        );
        marker.repair_custody = Some(export.cut.clone());
        let current = serde_json::to_value(&marker).unwrap();
        assert!(
            serde_json::from_value::<OlderMarker>(current.clone()).is_err(),
            "old readers must not silently ignore the original repair obligation"
        );
        assert_eq!(
            serde_json::from_value::<crate::DfsMetadataShardWriteFreeze>(current.clone()).unwrap(),
            marker
        );
        let bytes = crate::metadata_io::encode_sobj(&marker).unwrap();
        let reopened: crate::DfsMetadataShardWriteFreeze =
            sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(reopened, marker);
        let mut future = current;
        future["repair_custody_v1"]["repair_custody"]["version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<crate::DfsMetadataShardWriteFreeze>(future).is_err());
    }
}
