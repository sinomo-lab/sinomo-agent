//! Import grants follow actual source membership and fresh durable bytework.
//! The durable work record fixes receipt identity; it never substitutes for a
//! newly obtained authority/copy certificate after controller reconstruction.
use super::*;
use sinomo_dfs::{
    DfsContentDurability, DfsMetadataTableSnapshotTables, DfsObjectVersionRecord,
    DfsReplicaReceiptRecord, DfsReplicationPolicy,
};
use std::collections::{BTreeMap, BTreeSet};

type VersionKey = (
    DfsFileSystemId,
    sinomo_dfs::DfsObjectId,
    sinomo_dfs::DfsVersion,
);
type ReceiptIndex = BTreeMap<VersionKey, Vec<DfsReplicaReceiptRecord>>;

fn source_receipts(receipts: &[DfsReplicaReceiptRecord]) -> ReceiptIndex {
    let mut index = BTreeMap::<_, Vec<_>>::new();
    for receipt in receipts {
        index
            .entry((
                receipt.receipt.file_system_id.clone(),
                receipt.receipt.object_id.clone(),
                receipt.receipt.committed_version.clone(),
            ))
            .or_default()
            .push(receipt.clone());
    }
    index
}

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
    validate_data_receipts(version, &version.version, durability, receipts)
}

fn validate_data_receipts(
    version: &DfsObjectVersionRecord,
    data_version: &sinomo_dfs::DfsVersion,
    durability: &DfsContentDurability,
    receipts: &[DfsReplicaReceiptRecord],
) -> Result<()> {
    sinomo_dfs::validate_replication_plan(&durability.desired_replica_node_ids, &durability.policy)
        .map_err(error)?;
    if &durability.source_version != data_version
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
            || &receipt.receipt.committed_version != data_version
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

fn has_source_durability(version: &DfsObjectVersionRecord, index: &ReceiptIndex) -> bool {
    let Some(durability) = version.content_durability.as_deref() else {
        return false;
    };
    has_recorded_source_receipts(version, durability, index)
}

fn has_recorded_source_receipts(
    version: &DfsObjectVersionRecord,
    durability: &DfsContentDurability,
    index: &ReceiptIndex,
) -> bool {
    // Metadata-only successors retain the recorded data generation and its
    // actual receipts. They do not claim a second original byte publication.
    let key = (
        version.file_system_id.clone(),
        version.object_id.clone(),
        durability.source_version.clone(),
    );
    let receipts = index
        .get(&key)
        .into_iter()
        .flatten()
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
    validate_data_receipts(version, &durability.source_version, durability, &receipts).is_ok()
}

// Released managed placement (a5b262c4, 9c0287c6) owns repair of the
// existing byte version. Its complete reserved operation identity separates
// that obligation from publication; equal base/target alone also occurs on
// stable-write retries and is never enough to classify an operation.
fn released_current_placement(
    publication: &sinomo_dfs::DfsWritePublication,
    status: &sinomo_dfs::DfsWriteIntentStatus,
    intent_id: &str,
    object: &sinomo_dfs::DfsObjectId,
    base: Option<&sinomo_dfs::DfsVersion>,
    target: &sinomo_dfs::DfsVersion,
    has_write_claim: bool,
) -> bool {
    publication.is_current()
        && *status == sinomo_dfs::DfsWriteIntentStatus::Committed
        && base == Some(target)
        && !has_write_claim
        && intent_id
            .strip_prefix("placement:")
            .and_then(|suffix| suffix.split_once(':'))
            .is_some_and(|(job, _)| {
                !job.is_empty() && intent_id == format!("placement:{job}:{object}:{target}")
            })
}

/// Released versions omitted their data grant while the committed write plan
/// and its original W receipts remained in the same source cut. Restore only
/// that recorded grant; this creates no copy, receipt or publication authority.
pub(super) fn restore_recorded_source_durability(
    tables: &mut DfsMetadataTableSnapshotTables,
) -> Result<()> {
    let receipts = source_receipts(&tables.receipts);
    let versions = tables
        .versions
        .iter()
        .filter(|version| version.content_durability.is_none() && version.content_hash.is_some())
        .map(|version| {
            (
                (
                    version.file_system_id.clone(),
                    version.object_id.clone(),
                    version.version.clone(),
                ),
                version,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let plans = tables
        .intents
        .iter()
        .filter(|intent| intent.status == sinomo_dfs::DfsWriteIntentStatus::Committed)
        .map(|intent| {
            (
                (
                    intent.file_system_id.clone(),
                    intent.object_id.clone(),
                    intent.target_version.clone(),
                ),
                intent.content_hash.as_ref(),
                intent.intent_id.as_str(),
                "intent",
                released_current_placement(
                    &intent.publication,
                    &intent.status,
                    &intent.intent_id,
                    &intent.object_id,
                    intent.base_version.as_ref(),
                    &intent.target_version,
                    intent.operation_id.is_some()
                        || intent.operation_order_token.is_some()
                        || intent.touch_parent_on_create
                        || intent.create_predecessor_object_id.is_some(),
                ),
                DfsContentDurability {
                    source_version: intent.target_version.clone(),
                    coordinator_node_id: intent.coordinator_node_id.clone(),
                    owner_epoch: intent.owner_epoch,
                    desired_replica_node_ids: intent.desired_replica_node_ids.clone(),
                    policy: intent.policy.clone(),
                },
            )
        })
        .chain(
            tables
                .intent_receipts
                .iter()
                .filter(|receipt| receipt.status == sinomo_dfs::DfsWriteIntentStatus::Committed)
                .map(|receipt| {
                    (
                        (
                            receipt.file_system_id.clone(),
                            receipt.object_id.clone(),
                            receipt.target_version.clone(),
                        ),
                        receipt.content_hash.as_ref(),
                        receipt.intent_id.as_str(),
                        "intent receipt",
                        released_current_placement(
                            &receipt.publication,
                            &receipt.status,
                            &receipt.intent_id,
                            &receipt.object_id,
                            receipt.base_version.as_ref(),
                            &receipt.target_version,
                            receipt.operation_id.is_some()
                                || receipt.operation_order_token.is_some()
                                || receipt.touch_parent_on_create
                                || receipt.create_predecessor_object_id.is_some(),
                        ),
                        DfsContentDurability {
                            source_version: receipt.target_version.clone(),
                            coordinator_node_id: receipt.coordinator_node_id.clone(),
                            owner_epoch: receipt.owner_epoch,
                            desired_replica_node_ids: receipt.desired_replica_node_ids.clone(),
                            policy: receipt.policy.clone(),
                        },
                    )
                }),
        );
    let mut recorded = BTreeMap::<VersionKey, (DfsContentDurability, bool, &str, &str)>::new();
    for (key, hash, operation, family, placement, durability) in plans {
        let Some(version) = versions.get(&key) else {
            continue;
        };
        if hash.is_some() && hash != version.content_hash.as_ref() {
            return Err(error(format!(
                "recorded source write plan changes immutable content: filesystem {} object {} version {}; {family} {operation}",
                key.0, key.1, key.2,
            )));
        }
        sinomo_dfs::validate_replication_plan(
            &durability.desired_replica_node_ids,
            &durability.policy,
        )
        .map_err(|cause| error(format!(
            "invalid recorded source write plan: filesystem {} object {} version {}; {family} {operation}: {cause}",
            key.0, key.1, key.2,
        )))?;
        // Preserve the plan and its receipts for repair/retry. It does not
        // compete to define the original byte grant. Immutable content and
        // plan validity above still apply to this released operation class.
        if placement {
            continue;
        }
        if let Some((old, recorded_hash, old_operation, old_family)) = recorded.get_mut(&key) {
            if old != &durability {
                return Err(error(format!(
                    "committed source write plans disagree on original durability: filesystem {} object {} version {}; {old_family} {old_operation} versus {family} {operation}",
                    key.0, key.1, key.2,
                )));
            }
            *recorded_hash |= hash.is_some();
        } else {
            recorded.insert(key, (durability, hash.is_some(), operation, family));
        }
    }
    for version in &mut tables.versions {
        let key = (
            version.file_system_id.clone(),
            version.object_id.clone(),
            version.version.clone(),
        );
        if let Some((durability, recorded_hash, _, _)) = recorded.remove(&key) {
            if recorded_hash && has_recorded_source_receipts(version, &durability, &receipts) {
                version.content_durability = Some(Box::new(durability));
            }
            // No original W proof: retain the ordinary fresh native bytework.
        }
    }
    Ok(())
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
    let mut receipts = source_receipts(&tables.receipts);
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
        if has_source_durability(&version, &receipts) {
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
                !(old.receipt.file_system_id == receipt.receipt.file_system_id
                    && old.receipt.object_id == receipt.receipt.object_id
                    && old.receipt.committed_version == receipt.receipt.committed_version
                    && old.replica_node_id == receipt.replica_node_id)
            });
            let key = (
                receipt.receipt.file_system_id.clone(),
                receipt.receipt.object_id.clone(),
                receipt.receipt.committed_version.clone(),
            );
            let indexed = receipts.entry(key).or_default();
            indexed.retain(|old| old.replica_node_id != receipt.replica_node_id);
            indexed.push(receipt.clone());
            tables.receipts.push(receipt);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn original_content() -> DfsMetadataTableSnapshotTables {
        let decode = |bytes: &[u8]| {
            sinomo_core::object::serde_ext::from_bytes::<Value>(bytes).unwrap()["mutation"].clone()
        };
        let version = decode(include_bytes!(
            "../../tests/fixtures/original-oracle-users-content-seq132.sobj"
        ));
        let receipt = decode(include_bytes!(
            "../../tests/fixtures/original-oracle-users-content-seq134.sobj"
        ));
        let plan = decode(include_bytes!(
            "../../tests/fixtures/original-oracle-users-content-seq334.sobj"
        ));
        DfsMetadataTableSnapshotTables {
            versions: vec![
                crate::legacy_wire::normalize_version(version["version_record"].clone()).unwrap(),
            ],
            receipts: vec![serde_json::from_value(receipt["receipt"].clone()).unwrap()],
            intent_receipts: vec![
                crate::legacy_wire::normalize_intent_receipt(plan["receipt"].clone()).unwrap(),
            ],
            ..Default::default()
        }
    }

    fn original_publication_and_placement() -> DfsMetadataTableSnapshotTables {
        let decode = |bytes: &[u8]| {
            sinomo_core::object::serde_ext::from_bytes::<Value>(bytes).unwrap()["mutation"].clone()
        };
        let version = decode(include_bytes!(
            "../../tests/fixtures/original-oracle-users-placement-seq41.sobj"
        ));
        let original = decode(include_bytes!(
            "../../tests/fixtures/original-oracle-users-placement-seq317.sobj"
        ));
        let placement = decode(include_bytes!(
            "../../tests/fixtures/original-oracle-users-placement-seq824.sobj"
        ));
        DfsMetadataTableSnapshotTables {
            versions: vec![
                crate::legacy_wire::normalize_version(version["version_record"].clone()).unwrap(),
            ],
            intents: vec![
                crate::legacy_wire::normalize_intent(placement["intent"].clone()).unwrap(),
            ],
            intent_receipts: vec![
                crate::legacy_wire::normalize_intent_receipt(original["receipt"].clone()).unwrap(),
            ],
            receipts: [
                include_bytes!("../../tests/fixtures/original-oracle-users-placement-seq43.sobj")
                    .as_slice(),
                include_bytes!("../../tests/fixtures/original-oracle-users-placement-seq845.sobj")
                    .as_slice(),
            ]
            .into_iter()
            .map(|bytes| serde_json::from_value(decode(bytes)["receipt"].clone()).unwrap())
            .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn exact_released_placement_preserves_original_publication_and_receipts() {
        let mut tables = original_publication_and_placement();
        let original = tables.clone();
        assert_eq!(
            tables.intents[0].base_version.as_ref(),
            Some(&tables.intents[0].target_version)
        );
        assert_ne!(
            tables.intents[0].owner_epoch,
            tables.intent_receipts[0].owner_epoch
        );
        assert_ne!(tables.intents[0].policy, tables.intent_receipts[0].policy);
        restore_recorded_source_durability(&mut tables).unwrap();
        // The exact captured publisher/replica evidence does not satisfy the
        // original plan's W. Keep fresh native work; never forge a grant.
        assert_eq!(tables, original);
    }

    #[test]
    fn malformed_placement_identity_cannot_hide_a_publication_conflict() {
        for mutation in 0..9 {
            let mut tables = original_publication_and_placement();
            let row = &mut tables.intents[0];
            match mutation {
                0 => row.operation_id = Some("stable-write-retry".into()),
                1 => row.operation_order_token = Some("write-order".into()),
                2 => row.touch_parent_on_create = true,
                3 => row.create_predecessor_object_id = Some(sinomo_dfs::DfsObjectId::generate()),
                4 => row.base_version = None,
                5 => row.intent_id.push_str(":extra"),
                6 => row.intent_id = format!("placement::{}:{}", row.object_id, row.target_version),
                7 => {
                    row.intent_id = format!(
                        "placement:job:{}:{}",
                        sinomo_dfs::DfsObjectId::generate(),
                        row.target_version
                    )
                }
                _ => row.publication = sinomo_dfs::DfsWritePublication::RetainedRepair,
            }
            let original = tables.clone();
            let message = restore_recorded_source_durability(&mut tables)
                .unwrap_err()
                .to_string();
            assert!(message.contains("disagree on original durability"));
            for identity in [
                original.versions[0].file_system_id.as_str(),
                original.versions[0].object_id.as_str(),
                original.versions[0].version.as_str(),
                original.intents[0].intent_id.as_str(),
                original.intent_receipts[0].intent_id.as_str(),
            ] {
                assert!(
                    message.contains(identity),
                    "missing identity {identity}: {message}"
                );
            }
            assert_eq!(tables, original);
        }
    }

    #[test]
    fn base_equal_stable_write_retry_still_restores_and_checks_original_grant() {
        for operation in [None, Some("stable-write-retry".to_owned())] {
            let mut tables = original_content();
            let row = &mut tables.intent_receipts[0];
            row.base_version = Some(row.target_version.clone());
            row.operation_id = operation;
            restore_recorded_source_durability(&mut tables).unwrap();
            assert!(tables.versions[0].content_durability.is_some());
            tables.versions[0].content_durability = None;
            let mut conflicting = tables.intent_receipts[0].clone();
            conflicting.intent_id.push_str(":conflicting-publication");
            conflicting.policy.min_commit_replicas = 2;
            tables.intent_receipts.push(conflicting);
            let original = tables.clone();
            assert!(restore_recorded_source_durability(&mut tables).is_err());
            assert_eq!(tables, original);
        }
    }

    #[test]
    fn exact_placement_still_checks_immutable_content_and_replication_plan() {
        for mutation in 0..2 {
            let mut tables = original_publication_and_placement();
            if mutation == 0 {
                tables.intents[0].content_hash = Some("blake3:changed".into());
            } else {
                tables.intents[0].policy.min_commit_replicas = 3;
            }
            let original = tables.clone();
            assert!(restore_recorded_source_durability(&mut tables).is_err());
            assert_eq!(tables, original);
        }
    }

    #[test]
    fn exact_original_committed_plan_restores_durability_without_replacing_receipts() {
        let mut tables = original_content();
        let original = tables.clone();
        restore_recorded_source_durability(&mut tables).unwrap();
        let plan = &original.intent_receipts[0];
        let durability = tables.versions[0].content_durability.as_deref().unwrap();
        assert_eq!(durability.source_version, plan.target_version);
        assert_eq!(durability.coordinator_node_id, plan.coordinator_node_id);
        assert_eq!(durability.owner_epoch, 3);
        assert_eq!(durability.policy, plan.policy);
        assert_eq!(
            durability.desired_replica_node_ids,
            plan.desired_replica_node_ids
        );
        assert_eq!(durability.policy.min_commit_replicas, 1);
        assert_eq!(durability.policy.desired_replicas, 2);
        assert_eq!(tables.receipts, original.receipts);
        assert_eq!(tables.intent_receipts, original.intent_receipts);
        tables.versions[0].content_durability = None;
        assert_eq!(tables, original);
    }

    #[test]
    fn incomplete_duplicate_and_foreign_source_receipts_never_restore_original_w() {
        for mutation in 0..6 {
            let mut tables = original_content();
            match mutation {
                0 => tables.receipts.clear(),
                1 => tables.receipts.push(tables.receipts[0].clone()),
                2 => tables.receipts[0].receipt.file_system_id = DfsFileSystemId::generate(),
                3 => {
                    tables.receipts[0].receipt.owner_node_id = sinomo_core::node::NodeId::generate()
                }
                4 => {
                    tables.receipts[0].content_hash =
                        Some(format!("blake3:{}", blake3::hash(b"other").to_hex()))
                }
                _ => tables.intent_receipts[0].content_hash = None,
            }
            let original = tables.clone();
            restore_recorded_source_durability(&mut tables).unwrap();
            assert_eq!(tables, original, "incomplete source proof is not a grant");
        }
    }

    #[test]
    fn conflicting_committed_plan_cannot_hide_behind_insufficient_w() {
        for mutation in 0..5 {
            let mut tables = original_content();
            let mut other = tables.intent_receipts[0].clone();
            other.intent_id.push_str(":different-plan");
            match mutation {
                0 => other.policy.min_commit_replicas = 2,
                1 => other.owner_epoch += 1,
                2 => other.coordinator_node_id = sinomo_core::node::NodeId::generate(),
                3 => {
                    other.policy.desired_replicas = 3;
                    other
                        .desired_replica_node_ids
                        .push(sinomo_core::node::NodeId::generate());
                }
                _ => {
                    other.policy.min_commit_replicas = 2;
                    other.content_hash = None;
                }
            }
            tables.intent_receipts.push(other);
            let original = tables.clone();
            assert!(restore_recorded_source_durability(&mut tables).is_err());
            assert_eq!(tables, original);
        }
    }

    #[test]
    fn explicit_durability_and_foreign_plans_remain_unchanged() {
        let mut tables = original_content();
        restore_recorded_source_durability(&mut tables).unwrap();
        let original = tables.clone();
        tables.intent_receipts[0].policy.min_commit_replicas = 2;
        let expected = tables.clone();
        restore_recorded_source_durability(&mut tables).unwrap();
        assert_eq!(tables, expected);
        assert_eq!(tables.versions, original.versions);

        let mut tables = original_content();
        tables.intent_receipts[0].file_system_id = DfsFileSystemId::generate();
        let expected = tables.clone();
        restore_recorded_source_durability(&mut tables).unwrap();
        assert_eq!(tables, expected);
    }
}
