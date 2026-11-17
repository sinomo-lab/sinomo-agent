//! Settle legacy cross-shard hardlink jobs while converting their coordinator
//! (decision Q-C2): count a live alias, otherwise abort. Current DFS refuses
//! these frames, so a job that still holds one is settled here from recorded
//! rows and is never resumed.
//!
//! - Creation (`HardlinkAcrossShardPrimaries`). The legacy stack is
//!   `[CrossShardHardlinkComplete, CrossShardHardlinkAdopt]`; the last frame
//!   runs first. The coordinator owns the existing path (`root_path`) and the
//!   alias shard owns the link path (`target_path`). Complete only advanced
//!   the canonical target's ctime and version, so dropping it loses no count.
//! - Alias mutation (`HardlinkAliasMutationAcrossShardPrimaries`). The legacy
//!   stack is `[TargetFenceCommit, AliasCommit, TargetFenceInstall]`, where
//!   the alias-commit frame predates recorded read sets. The coordinator owns
//!   the alias path, so the alias rows are its own. The alias transaction is
//!   atomic and its two paths stay locked while the job runs, so the
//!   occupants of those paths show whether it applied. The canonical shard
//!   still holds the target fence, which its own settlement releases.
//!
//! A job whose rows show neither outcome fails closed.
#![cfg_attr(not(test), allow(dead_code, reason = "consumed by the per-shard conversion driver"))]
use super::*;
use time::OffsetDateTime;

const LEGACY_ALIAS_COMMIT: &str = "cross-shard-hardlink-alias-commit";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LegacyHardlinkSettlement {
    /// The alias effect is durable: the coordinator keeps this Completed job,
    /// whose legacy frames are removed.
    Completed {
        job: DfsNamespaceJobRecord,
        /// The live alias the counts include; None after an alias unlink.
        live_alias: Option<DfsObjectMetadata>,
        /// The canonical shard's fence for this job, which it must release.
        target_fence_job_id: Option<String>,
    },
    /// The alias effect never applied: the coordinator drops this job.
    Aborted {
        job_id: String,
        target_fence_job_id: Option<String>,
    },
}

/// A legacy cross-primary alias mutation, decoded only for settlement. Its
/// transaction never becomes a runnable current frame.
#[derive(Clone, Debug)]
pub(crate) struct LegacyHardlinkAliasCommitJob {
    /// The original job without its legacy frame; the other frames are current.
    pub job: DfsNamespaceJobRecord,
    /// The legacy frame's position in the original `pending_frames` stack.
    pub alias_commit_index: usize,
    pub transaction: DfsNamespaceTransaction,
}

pub(crate) fn decode_legacy_hardlink_alias_commit_job(
    mut value: Value,
) -> Result<LegacyHardlinkAliasCommitJob> {
    validate_original_source_job(&value)?;
    let frames = value
        .get_mut("pending_frames")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("legacy alias job has no pending frames"))?;
    let legacy = frames
        .iter()
        .enumerate()
        .filter(|(_, frame)| frame.get("type").and_then(Value::as_str) == Some(LEGACY_ALIAS_COMMIT))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let [alias_commit_index] = legacy[..] else {
        return Err(invalid(
            "legacy alias job must hold exactly one original alias-commit frame",
        ));
    };
    let mut frame = frames.remove(alias_commit_index);
    let fields = frame
        .as_object_mut()
        .ok_or_else(|| invalid("legacy alias-commit frame is not an object"))?;
    let transaction = fields
        .remove("transaction")
        .ok_or_else(|| invalid("legacy alias-commit frame has no transaction"))?;
    if fields.len() != 1 {
        return Err(invalid("legacy alias-commit frame has an unsupported field"));
    }
    Ok(LegacyHardlinkAliasCommitJob {
        transaction: normalize_namespace_transaction(transaction)?,
        job: normalize_namespace_job(value)?,
        alias_commit_index,
    })
}

fn live(object: &DfsObjectMetadata) -> bool {
    object.kind != DfsObjectKind::Tombstone && object.tombstone_provenance.is_none()
}

/// The live occupant of `path`, which must match its namespace entry.
fn live_occupant<'a>(
    rows: &'a DfsMetadataTableSnapshotTables,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Result<Option<&'a DfsObjectMetadata>> {
    let mut occupants = rows
        .objects
        .iter()
        .filter(|object| &object.file_system_id == file_system_id && object.path == path && live(object));
    let occupant = occupants.next();
    if occupants.next().is_some() {
        return Err(invalid("legacy hardlink settlement found two live occupants of one path"));
    }
    if let Some(object) = occupant
        && !rows.paths.iter().any(|entry| {
            &entry.file_system_id == file_system_id
                && entry.path.trim_matches('/') == path.trim_matches('/')
                && entry.object_id == object.object_id
                && entry.kind == object.kind
        })
    {
        return Err(invalid(
            "legacy hardlink settlement found a live object without its namespace entry",
        ));
    }
    Ok(occupant)
}

/// Rows carrying exactly this object generation, including compacted tombstones.
fn recorded<'a>(
    rows: &'a DfsMetadataTableSnapshotTables,
    object: &DfsObjectMetadata,
) -> impl Iterator<Item = &'a DfsObjectMetadata> {
    rows.objects
        .iter()
        .chain(&rows.compacted_tombstones)
        .filter(move |row| {
            row.file_system_id == object.file_system_id
                && row.object_id == object.object_id
                && row.path == object.path
                && row.kind == object.kind
                && row.version == object.version
        })
}

fn completed(job: &DfsNamespaceJobRecord, at: OffsetDateTime) -> DfsNamespaceJobRecord {
    let mut job = job.clone();
    job.pending_frames.clear();
    job.status = DfsNamespaceJobStatus::Completed;
    job.updated_at = at;
    job.completed_at = Some(at);
    job
}

/// Settle a legacy cross-shard hardlink creation from the alias shard's rows:
/// Completed when the adopted alias is live at its path, Aborted when nothing
/// in the alias shard carries the alias identity.
pub(crate) fn settle_legacy_hardlink_job(
    job: &DfsNamespaceJobRecord,
    destination_rows: &DfsMetadataTableSnapshotTables,
    at: OffsetDateTime,
) -> Result<LegacyHardlinkSettlement> {
    if job.kind != DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
        || job.status != DfsNamespaceJobStatus::Running
    {
        return Err(invalid("not a running legacy cross-shard hardlink job"));
    }
    let alias_path = job
        .target_path
        .as_deref()
        .ok_or_else(|| invalid("legacy hardlink job has no alias path"))?;
    let (target, adopt) = match job.pending_frames.as_slice() {
        [DfsNamespaceJobFrame::CrossShardHardlinkComplete { target_object_id }] => {
            (target_object_id, None)
        }
        [
            DfsNamespaceJobFrame::CrossShardHardlinkComplete { target_object_id },
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias,
                target_object_id: adopted,
                ..
            },
        ] if adopted == target_object_id => (target_object_id, Some(alias)),
        _ => {
            return Err(invalid(
                "legacy hardlink job has no settleable creation frame stack",
            ));
        }
    };
    let occupant = live_occupant(destination_rows, &job.file_system_id, alias_path)?;
    let names_target = |object: &DfsObjectMetadata| {
        object.kind == DfsObjectKind::Hardlink && object.link_target_object_id.as_ref() == Some(target)
    };
    let live_alias = match adopt {
        Some(alias) => {
            if alias.kind != DfsObjectKind::Hardlink
                || alias.path != alias_path
                || alias.file_system_id != job.file_system_id
                || alias.link_target_object_id.as_ref() != Some(target)
            {
                return Err(invalid("legacy hardlink adopt frame differs from its job"));
            }
            let carriers = destination_rows
                .objects
                .iter()
                .chain(&destination_rows.compacted_tombstones)
                .filter(|row| row.file_system_id == alias.file_system_id && row.object_id == alias.object_id)
                .count();
            match occupant {
                _ if carriers == 0 => {
                    return Ok(LegacyHardlinkSettlement::Aborted {
                        job_id: job.job_id.clone(),
                        target_fence_job_id: None,
                    });
                }
                Some(occupant)
                    if carriers == 1 && occupant.object_id == alias.object_id && names_target(occupant) =>
                {
                    occupant.clone()
                }
                _ => {
                    return Err(invalid(
                        "legacy hardlink alias identity differs from its adopted record",
                    ));
                }
            }
        }
        // The adopt frame was popped only after the alias shard accepted it.
        None => match occupant {
            Some(occupant) if names_target(occupant) => occupant.clone(),
            _ => {
                return Err(invalid(
                    "legacy hardlink job recorded its adoption but the alias is not live",
                ));
            }
        },
    };
    Ok(LegacyHardlinkSettlement::Completed {
        job: completed(job, at),
        live_alias: Some(live_alias),
        target_fence_job_id: None,
    })
}

/// Settle a legacy cross-primary alias mutation from its coordinator's own
/// rows: Completed when every object its transaction wrote at the job's
/// locked paths is recorded and those paths hold exactly its live objects,
/// Aborted when none is recorded and the original alias is still live at its
/// path.
pub(crate) fn settle_legacy_hardlink_alias_commit_job(
    legacy: &LegacyHardlinkAliasCommitJob,
    alias_rows: &DfsMetadataTableSnapshotTables,
    at: OffsetDateTime,
) -> Result<LegacyHardlinkSettlement> {
    let job = &legacy.job;
    if job.kind != DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
        || job.status != DfsNamespaceJobStatus::Running
    {
        return Err(invalid("not a running legacy cross-primary alias mutation job"));
    }
    let source_path = job.root_path.as_str();
    let result_path = job
        .target_path
        .as_deref()
        .ok_or_else(|| invalid("legacy alias job has no result path"))?;
    let unsettleable = || invalid("legacy alias job has no settleable mutation frame stack");
    let [
        DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
            fence_job_id,
            source_path: committed_from,
            expected_target,
            ..
        },
        installs @ ..,
    ] = job.pending_frames.as_slice()
    else {
        return Err(unsettleable());
    };
    let installs_match = match installs {
        [] => true,
        [DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { fence, source_path: installed_from }] => {
            &fence.job_id == fence_job_id && installed_from == source_path
        }
        _ => false,
    };
    if legacy.alias_commit_index != 1 || committed_from != source_path || !installs_match {
        return Err(unsettleable());
    }
    let transaction = &legacy.transaction;
    let locked = transaction
        .objects
        .iter()
        .filter(|object| object.path == source_path || object.path == result_path)
        .collect::<Vec<_>>();
    if transaction.file_system_id != job.file_system_id
        || locked.is_empty()
        || locked.iter().any(|object| object.file_system_id != job.file_system_id)
    {
        return Err(invalid("legacy alias transaction does not change its job's paths"));
    }
    let applied = locked
        .iter()
        .filter(|object| recorded(alias_rows, object).next().is_some())
        .count();
    let original = live_occupant(alias_rows, &job.file_system_id, source_path)?.filter(|object| {
        object.kind == DfsObjectKind::Hardlink
            && object.link_target_object_id.as_ref() == Some(&expected_target.object_id)
    });
    // Applied also means each locked path is occupied by exactly what the
    // transaction left there, so a still-live original alias is not hidden.
    let mut occupants_match = true;
    let mut live_alias = None;
    for path in std::collections::BTreeSet::from([source_path, result_path]) {
        let written = locked.iter().find(|object| object.path == path && live(object));
        match (written, live_occupant(alias_rows, &job.file_system_id, path)?) {
            (None, None) => {}
            (Some(written), Some(occupant))
                if written.object_id == occupant.object_id
                    && written.version == occupant.version
                    && written.kind == occupant.kind =>
            {
                if occupant.kind == DfsObjectKind::Hardlink
                    && live_alias.replace(occupant.clone()).is_some()
                {
                    return Err(invalid("legacy alias transaction writes two live aliases"));
                }
            }
            _ => occupants_match = false,
        }
    }
    let target_fence_job_id = Some(fence_job_id.clone());
    if applied == locked.len() && occupants_match {
        return Ok(LegacyHardlinkSettlement::Completed {
            job: completed(job, at),
            live_alias,
            target_fence_job_id,
        });
    }
    if applied == 0 && original.is_some() {
        return Ok(LegacyHardlinkSettlement::Aborted {
            job_id: job.job_id.clone(),
            target_fence_job_id,
        });
    }
    Err(invalid(
        "legacy alias transaction is partly visible in its alias shard",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{DfsObjectPolicy, node::NodeId};

    fn object(name: &str, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(name).unwrap(),
            file_system_id: sifs_file_system_id(),
            path: path.into(),
            kind,
            namespace_link_count: None,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("settle-owner").unwrap(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(format!("{name}-version")).unwrap(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }
    fn target() -> DfsObjectMetadata {
        object("settle-target", "/services/file", DfsObjectKind::File)
    }
    fn alias(path: &str) -> DfsObjectMetadata {
        let mut alias = object("settle-alias", path, DfsObjectKind::Hardlink);
        alias.link_target_object_id = Some(target().object_id);
        alias
    }
    fn rows(objects: &[DfsObjectMetadata]) -> DfsMetadataTableSnapshotTables {
        let mut state = DfsMetadataCandidateState::new(Default::default()).unwrap();
        for object in objects {
            state.apply(DfsPreparedMetadataEffect::PutObject { object: object.clone() }).unwrap();
        }
        state.snapshot()
    }
    fn job(kind: DfsNamespaceJobKind, root_path: &str, target_path: &str, pending_frames: Vec<DfsNamespaceJobFrame>) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: format!("legacy-{root_path}->{target_path}"),
            file_system_id: sifs_file_system_id(),
            origin: None,
            kind,
            status: DfsNamespaceJobStatus::Running,
            root_path: root_path.into(),
            target_path: Some(target_path.into()),
            pending_frames,
            processed_object_count: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        }
    }
    fn creation(adopt_pending: bool) -> DfsNamespaceJobRecord {
        let target_object_id = target().object_id;
        let mut frames = vec![DfsNamespaceJobFrame::CrossShardHardlinkComplete {
            target_object_id: target_object_id.clone(),
        }];
        if adopt_pending {
            frames.push(DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias: alias("/users/alias"),
                target_object_id,
                destination_primary_node_id: NodeId::from_legacy_named("settle-users").unwrap(),
            });
        }
        // Through the retained decoder, exactly as conversion reads it.
        let original = job(DfsNamespaceJobKind::HardlinkAcrossShardPrimaries, "/services/file", "/users/alias", frames);
        normalize_namespace_job(serde_json::to_value(&original).unwrap()).unwrap()
    }
    fn at() -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60)
    }

    #[test]
    fn legacy_hardlink_creation_settles_forward_on_a_live_alias_and_aborts_otherwise() {
        let users = rows(&[object("settle-users", "/users", DfsObjectKind::Directory), alias("/users/alias")]);
        for adopt_pending in [true, false] {
            let LegacyHardlinkSettlement::Completed { job, live_alias, target_fence_job_id } =
                settle_legacy_hardlink_job(&creation(adopt_pending), &users, at()).unwrap()
            else {
                panic!("a live adopted alias settles forward");
            };
            assert_eq!(live_alias, Some(alias("/users/alias")));
            assert_eq!(target_fence_job_id, None);
            assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
            assert!(job.pending_frames.is_empty(), "the refused legacy frame is removed");
            assert_eq!(job.completed_at, Some(at()));
            assert_eq!(job.job_id, creation(adopt_pending).job_id);
        }
        let never_adopted = rows(&[object("settle-users", "/users", DfsObjectKind::Directory)]);
        assert_eq!(
            settle_legacy_hardlink_job(&creation(true), &never_adopted, at()).unwrap(),
            LegacyHardlinkSettlement::Aborted { job_id: creation(true).job_id, target_fence_job_id: None },
        );
        assert!(settle_legacy_hardlink_job(&creation(false), &never_adopted, at()).is_err(),
            "a popped adopt frame with no live alias is not a never-adopted job");
    }

    #[test]
    fn legacy_hardlink_creation_with_inconsistent_rows_or_frames_fails_closed() {
        let mut elsewhere = alias("/users/alias");
        elsewhere.path = "/users/other".into();
        let mut retargeted = alias("/users/alias");
        retargeted.link_target_object_id = Some(DfsObjectId::from_legacy_named("other-target").unwrap());
        for (adopted, rows) in [(true, rows(&[elsewhere])), (true, rows(&[retargeted.clone()])), (false, rows(&[retargeted]))] {
            assert!(settle_legacy_hardlink_job(&creation(adopted), &rows, at()).is_err());
        }
        let users = rows(&[alias("/users/alias")]);
        let mut current = creation(true);
        current.pending_frames.reverse();
        assert!(settle_legacy_hardlink_job(&current, &users, at()).is_err());
        let mut finished = creation(true);
        finished.status = DfsNamespaceJobStatus::Completed;
        assert!(settle_legacy_hardlink_job(&finished, &users, at()).is_err());
    }

    /// The original native9 shape: current fence frames around the alias
    /// frame that kept only its transaction.
    fn alias_mutation(result_path: &str, objects: Vec<DfsObjectMetadata>) -> Value {
        let source_path = "/users/alias";
        let expected_target = DfsNamespaceObjectPrecondition {
            object_id: target().object_id,
            version: target().version,
        };
        let mut committed_target = target();
        committed_target.version = DfsVersion::from_legacy_named("settle-target-next").unwrap();
        let fence_job_id = "legacy-target-fence".to_string();
        let mut fence = job(DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary, "/services/file", "/", vec![
            DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: source_path.into(),
                expected_target: expected_target.clone(),
                committed_target: committed_target.clone(),
            },
        ]);
        fence.job_id = fence_job_id.clone();
        fence.target_path = None;
        let original = job(DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries, source_path, result_path, vec![
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                fence_job_id,
                source_path: source_path.into(),
                expected_target,
                committed_target,
            },
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                fence: Box::new(DfsHardlinkTargetFence::from_job(fence).unwrap()),
                source_path: source_path.into(),
            },
        ]);
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            transaction_id: format!("legacy-alias-{result_path}"),
            file_system_id: sifs_file_system_id(),
            objects,
            versions: Vec::new(),
            file_version_pins: None,
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut value = serde_json::to_value(&original).unwrap();
        value["pending_frames"].as_array_mut().unwrap().insert(1, serde_json::json!({
            "type": LEGACY_ALIAS_COMMIT,
            "transaction": transaction,
        }));
        value
    }
    fn tombstone(of: &DfsObjectMetadata, name: &str) -> DfsObjectMetadata {
        let mut tombstone = of.clone();
        tombstone.object_id = DfsObjectId::from_legacy_named(name).unwrap();
        tombstone.kind = DfsObjectKind::Tombstone;
        tombstone.link_target_object_id = None;
        tombstone.version = DfsVersion::from_legacy_named(format!("{name}-version")).unwrap();
        tombstone
    }

    #[test]
    fn legacy_alias_commit_is_refused_by_conversion_but_decodes_for_settlement() {
        let value = alias_mutation("/users/alias", vec![tombstone(&alias("/users/alias"), "settle-unlinked")]);
        assert!(matches!(normalize_namespace_job(value.clone()),
            Err(MetadataBackendError::MissingOriginalSourceCustody { .. })));
        let legacy = decode_legacy_hardlink_alias_commit_job(value.clone()).unwrap();
        assert_eq!(legacy.alias_commit_index, 1);
        assert_eq!(legacy.job.pending_frames.len(), 2);
        assert_eq!(legacy.transaction.objects.len(), 1);
        let mut twice = value;
        let frame = twice["pending_frames"][1].clone();
        twice["pending_frames"].as_array_mut().unwrap().push(frame);
        assert!(decode_legacy_hardlink_alias_commit_job(twice).is_err());
    }

    #[test]
    fn legacy_alias_unlink_settles_from_its_coordinator_rows() {
        let unlinked = tombstone(&alias("/users/alias"), "settle-unlinked");
        let legacy = decode_legacy_hardlink_alias_commit_job(
            alias_mutation("/users/alias", vec![unlinked.clone()])).unwrap();
        let fence = Some("legacy-target-fence".to_string());
        assert_eq!(
            settle_legacy_hardlink_alias_commit_job(&legacy, &rows(&[alias("/users/alias")]), at()).unwrap(),
            LegacyHardlinkSettlement::Aborted { job_id: legacy.job.job_id.clone(), target_fence_job_id: fence.clone() },
        );
        let LegacyHardlinkSettlement::Completed { job, live_alias, target_fence_job_id } =
            settle_legacy_hardlink_alias_commit_job(&legacy, &rows(&[unlinked]), at()).unwrap()
        else {
            panic!("an applied unlink settles forward");
        };
        assert_eq!((live_alias, target_fence_job_id), (None, fence));
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.pending_frames.is_empty());
        assert!(settle_legacy_hardlink_alias_commit_job(&legacy, &rows(&[]), at()).is_err(),
            "neither the original alias nor the unlink is recorded");
        let mut both = rows(&[alias("/users/alias")]);
        both.objects.push(tombstone(&alias("/users/alias"), "settle-unlinked"));
        assert!(settle_legacy_hardlink_alias_commit_job(&legacy, &both, at()).is_err(),
            "a recorded unlink cannot leave the original alias live");
    }

    #[test]
    fn legacy_alias_rename_counts_the_renamed_alias_or_fails_closed_when_partial() {
        let mut renamed = alias("/users/renamed");
        renamed.version = DfsVersion::from_legacy_named("settle-alias-renamed").unwrap();
        let old_path = tombstone(&alias("/users/alias"), "settle-old-path");
        let legacy = decode_legacy_hardlink_alias_commit_job(
            alias_mutation("/users/renamed", vec![old_path.clone(), renamed.clone()])).unwrap();
        let LegacyHardlinkSettlement::Completed { live_alias, .. } =
            settle_legacy_hardlink_alias_commit_job(&legacy, &rows(&[old_path.clone(), renamed.clone()]), at()).unwrap()
        else {
            panic!("an applied rename settles forward");
        };
        assert_eq!(live_alias, Some(renamed.clone()));
        assert!(matches!(
            settle_legacy_hardlink_alias_commit_job(&legacy, &rows(&[alias("/users/alias")]), at()).unwrap(),
            LegacyHardlinkSettlement::Aborted { .. }));
        let mut partial = rows(&[alias("/users/alias")]);
        partial.objects.push(old_path);
        let error = settle_legacy_hardlink_alias_commit_job(&legacy, &partial, at()).err().unwrap();
        assert!(error.to_string().contains("partly visible"), "{error}");
    }
}
