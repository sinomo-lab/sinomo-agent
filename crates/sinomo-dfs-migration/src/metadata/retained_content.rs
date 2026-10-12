//! Import grants follow actual source membership and fresh durable bytework.
//! The durable work record fixes receipt identity; it never substitutes for a
//! newly obtained authority/copy certificate after controller reconstruction.
use super::*;
use sinomo_dfs::{
    DfsContentDurability, DfsMetadataTableSnapshotTables, DfsObjectVersionRecord,
    DfsReplicaReceiptRecord, DfsReplicationPolicy,
};
use std::collections::BTreeSet;

type VersionKey = (
    DfsFileSystemId,
    sinomo_dfs::DfsObjectId,
    sinomo_dfs::DfsVersion,
);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkRecord {
    source_binding: String,
    policy: DfsReplicationPolicy,
    targets: Vec<sinomo_core::node::NodeId>,
    durability: DfsContentDurability,
    receipts: Vec<DfsReplicaReceiptRecord>,
}

fn digest(value: &impl Serialize) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinomo.dfs-migration.current-import-content.v1\0");
    serde_json::to_writer(&mut hash, value).map_err(error)?;
    Ok(hash.finalize().to_hex().to_string())
}

fn validate_receipts(
    version: &DfsObjectVersionRecord,
    durability: &DfsContentDurability,
    receipts: &[DfsReplicaReceiptRecord],
) -> Result<()> {
    sinomo_dfs::validate_replication_plan(&durability.desired_replica_node_ids, &durability.policy)
        .map_err(error)?;
    if durability.source_version != version.version
        || version.content_hash.is_none()
        || version.size_bytes.is_none()
    {
        return Err(error(
            "import durability changes the exact immutable source revision",
        ));
    }
    let mut copies = BTreeSet::new();
    for receipt in receipts {
        if receipt.receipt.file_system_id != version.file_system_id
            || receipt.receipt.object_id != version.object_id
            || receipt.receipt.committed_version != version.version
            || receipt.receipt.owner_node_id != durability.coordinator_node_id
            || receipt.content_hash != version.content_hash
            || !durability
                .desired_replica_node_ids
                .contains(&receipt.replica_node_id)
            || !copies.insert(receipt.replica_node_id.clone())
        {
            return Err(error(
                "import receipt changes revision, content, owner, or replica identity",
            ));
        }
    }
    if copies.len() < durability.policy.min_commit_replicas {
        return Err(error(
            "import durability lacks its actual distinct W receipts",
        ));
    }
    Ok(())
}

fn has_source_durability(
    version: &DfsObjectVersionRecord,
    tables: &DfsMetadataTableSnapshotTables,
) -> bool {
    let Some(durability) = version.content_durability.as_deref() else {
        return false;
    };
    // Metadata-only successors retain the recorded data generation and its
    // actual receipts. They do not claim a second original byte publication.
    let mut data_generation = version.clone();
    data_generation.version = durability.source_version.clone();
    let receipts = tables
        .receipts
        .iter()
        .filter(|receipt| {
            receipt.receipt.file_system_id == version.file_system_id
                && receipt.receipt.object_id == version.object_id
                && receipt.receipt.committed_version == durability.source_version
                && receipt.content_hash == version.content_hash
                && durability
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    validate_receipts(&data_generation, durability, &receipts).is_ok()
}

/// Returns false only while bounded real work remains. Nothing is mounted or
/// published here; the resulting rows enter the same sealed current candidate.
pub(super) fn fulfill(
    session: &mut Session,
    scope: &MetadataSourceContentWorkScope,
    authority: &dyn MetadataImportAuthority,
    tables: &mut DfsMetadataTableSnapshotTables,
    remaining: &mut usize,
) -> Result<bool> {
    for index in 0..tables.versions.len() {
        let version = tables.versions[index].clone();
        if version.content_hash.is_none() {
            continue;
        }
        let key: VersionKey = (
            version.file_system_id.clone(),
            version.object_id.clone(),
            version.version.clone(),
        );
        if has_source_durability(&version, tables) {
            let locator = source_content::source_locator(session, &key.0, &key.1, &key.2)?;
            let durability = version
                .content_durability
                .as_ref()
                .expect("recorded W checked");
            let data_key = locator
                .content_version
                .as_deref()
                .unwrap_or(&locator.version);
            if locator.object_id == version.object_id.as_str()
                && data_key == durability.source_version.as_str()
            {
                continue;
            }
            if !session.confirmed_source_content_bindings.contains(&key) {
                if *remaining == 0 {
                    return Ok(false);
                }
                let evidence = lookup_metadata_source_version(
                    &session.source_root,
                    &session.staged_root,
                    scope,
                    &key.0,
                    &key.1,
                    &key.2,
                )?
                .ok_or_else(|| {
                    error("source binding confirmation lacks actual version membership")
                })?;
                let confirmation = authority.fulfill_retained_content(
                    scope,
                    &evidence.object,
                    &version,
                    &locator,
                    durability.policy.clone(),
                    durability.desired_replica_node_ids.clone(),
                )?;
                validate_receipts(&version, &confirmation.durability, &confirmation.receipts)?;
                if confirmation.durability.policy != durability.policy
                    || confirmation.durability.desired_replica_node_ids
                        != durability.desired_replica_node_ids
                {
                    return Err(error(
                        "canonical binding confirmation changes recorded W/N or targets",
                    ));
                }
                // Confirmation establishes canonical byte bindings only. Keep
                // the actual original grant, data generation and receipt IDs.
                session.confirmed_source_content_bindings.insert(key);
                *remaining -= 1;
            }
            continue;
        }
        let record = if let Some(record) = session.retained_content_work.get(&key) {
            record.clone()
        } else {
            if *remaining == 0 {
                return Ok(false);
            }
            let evidence = lookup_metadata_source_version(
                &session.source_root,
                &session.staged_root,
                scope,
                &version.file_system_id,
                &version.object_id,
                &version.version,
            )?
            .ok_or_else(|| {
                error("content import lacks independently verified original source membership")
            })?;
            if evidence.version.content_hash != version.content_hash
                || evidence.version.size_bytes != version.size_bytes
            {
                return Err(error(
                    "mapped content revision differs from actual source bytes",
                ));
            }
            let locator = source_content::source_locator(session, &key.0, &key.1, &key.2)?;
            if locator != evidence.locator {
                return Err(error("raw source locator changed after membership capture"));
            }
            // A missing old grant establishes NEW durability under the actual
            // current policy. The complete original old policy stays archived.
            let (policy, targets) =
                authority.current_retained_content_placement(scope, &evidence.object, &version)?;
            let source_binding = digest(&(scope, &evidence, &policy, &targets))?;
            let path = session
                .staged_root
                .join("retained-content-work")
                .join(format!("{}.json", digest(&key)?));
            let retained = if path.is_file() {
                Some(read_json::<WorkRecord>(&path)?)
            } else {
                None
            };
            if retained.as_ref().is_some_and(|old| {
                old.source_binding != source_binding
                    || old.policy != policy
                    || old.targets != targets
            }) {
                return Err(error(
                    "retained import work changes its immutable source or current policy snapshot",
                ));
            }
            let fresh = authority.fulfill_retained_content(
                scope,
                &evidence.object,
                &version,
                &locator,
                policy.clone(),
                targets.clone(),
            )?;
            validate_receipts(&version, &fresh.durability, &fresh.receipts)?;
            if fresh.durability.policy != policy
                || fresh.durability.desired_replica_node_ids != targets
            {
                return Err(error(
                    "real import bytework changes its selected current policy or replicas",
                ));
            }
            let record = if let Some(old) = retained {
                validate_receipts(&version, &old.durability, &old.receipts)?;
                let old_copies = old
                    .receipts
                    .iter()
                    .map(|receipt| receipt.replica_node_id.clone())
                    .collect::<BTreeSet<_>>();
                let fresh_copies = fresh
                    .receipts
                    .iter()
                    .map(|receipt| receipt.replica_node_id.clone())
                    .collect::<BTreeSet<_>>();
                if old.durability != fresh.durability || old_copies != fresh_copies {
                    return Err(error(
                        "fresh import copy confirmation differs from the retained immutable work",
                    ));
                }
                old
            } else {
                let record = WorkRecord {
                    source_binding,
                    policy,
                    targets,
                    durability: fresh.durability,
                    receipts: fresh.receipts,
                };
                fs::create_dir_all(
                    path.parent()
                        .ok_or_else(|| error("retained import work has no parent"))?,
                )
                .map_err(error)?;
                write_json(&path, &record)?;
                record
            };
            session.retained_content_work.insert(key, record.clone());
            *remaining -= 1;
            record
        };
        tables.versions[index].content_durability = Some(Box::new(record.durability));
        for receipt in record.receipts {
            tables.receipts.retain(|old| {
                !(old.receipt.object_id == receipt.receipt.object_id
                    && old.receipt.committed_version == receipt.receipt.committed_version
                    && old.replica_node_id == receipt.replica_node_id)
            });
            tables.receipts.push(receipt);
        }
    }
    Ok(true)
}
