//! Durable cursor paths and IO for local DFS metadata shards.

use std::path::{Path, PathBuf};

use sinomo_core::node::NodeId;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{read_dir, read_sobj_optional, write_sobj_atomic};
use crate::metadata_journal::{DfsMetadataJournalHead, DfsMetadataReplayCursor};
use crate::metadata_keys::key_segment;
use crate::metadata_records::{
    DfsMetadataAuthorityAckFloor, DfsMetadataPrefixAckFloor, DfsMetadataReplayCursorStatus,
    DfsMetadataShardId,
};

const REPLAY_CURSOR_FLUSH_INTERVAL: u64 = 512;

pub(crate) fn records_root(root: &Path) -> PathBuf {
    root.join("records")
}

pub(crate) fn replay_cursor_path(root: &Path, shard_id: &DfsMetadataShardId) -> PathBuf {
    records_root(root)
        .join("replay-cursors")
        .join(format!("{}.sobj", key_segment(&shard_id.0)))
}

pub(crate) fn local_materialization_cursor_path(root: &Path) -> PathBuf {
    records_root(root)
        .join("local-materialization")
        .join("cursor.sobj")
}

pub(crate) fn metadata_replication_cursor_path(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> PathBuf {
    records_root(root)
        .join("metadata-replication-cursors")
        .join(key_segment(target_node_id))
        .join(format!("{}.sobj", key_segment(&shard_id.0)))
}

pub(crate) fn metadata_prefix_ack_floor_path(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> PathBuf {
    root.join("coordination")
        .join("metadata-prefix-ack-floors")
        .join(format!("{}.sobj", key_segment(&shard_id.0)))
}

pub(crate) fn metadata_authority_ack_floor_path(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> PathBuf {
    root.join("coordination")
        .join("metadata-authority-ack-floors")
        .join(format!("{}.sobj", key_segment(&shard_id.0)))
}

pub(crate) fn journal_head_path(root: &Path) -> PathBuf {
    root.join("journal").join("head.sobj")
}

pub(crate) fn journal_write_lock_path(root: &Path) -> PathBuf {
    root.join("journal").join("write.lock")
}

pub(crate) fn replay_cursor_statuses(root: &Path) -> Result<Vec<DfsMetadataReplayCursorStatus>> {
    let cursor_dir = records_root(root).join("replay-cursors");
    if !cursor_dir.exists() {
        return Ok(Vec::new());
    }
    let mut cursors = Vec::new();
    for entry in read_dir(&cursor_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
            continue;
        }
        if let Some(cursor) = read_sobj_optional::<DfsMetadataReplayCursor>(&path)? {
            cursors.push(DfsMetadataReplayCursorStatus {
                source_shard_id: cursor.shard_id,
                replayed_through_sequence: cursor.last_sequence,
            });
        }
    }
    cursors.sort_by(|left, right| left.source_shard_id.cmp(&right.source_shard_id));
    Ok(cursors)
}

pub(crate) fn read_replay_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> Result<DfsMetadataReplayCursor> {
    Ok(
        read_sobj_optional::<DfsMetadataReplayCursor>(&replay_cursor_path(root, shard_id))?
            .unwrap_or(DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 0,
            }),
    )
}

pub(crate) fn write_replay_cursor(root: &Path, cursor: &DfsMetadataReplayCursor) -> Result<()> {
    write_sobj_atomic(&replay_cursor_path(root, &cursor.shard_id), cursor)
}

pub(crate) fn checkpoint_replay_cursor_if_due(
    root: &Path,
    cursor: &DfsMetadataReplayCursor,
) -> Result<()> {
    let durable = read_replay_cursor(root, &cursor.shard_id)?;
    if cursor.last_sequence <= durable.last_sequence
        || cursor.last_sequence.saturating_sub(durable.last_sequence) < REPLAY_CURSOR_FLUSH_INTERVAL
    {
        return Ok(());
    }
    write_replay_cursor(root, cursor)
}

pub(crate) fn ensure_local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    has_materialized_state: bool,
) -> Result<()> {
    if local_materialization_cursor_path(root).is_file() {
        return Ok(());
    }
    let last_sequence = if has_materialized_state {
        next_sequence.saturating_sub(1)
    } else {
        0
    };
    write_local_materialization_cursor(
        root,
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence,
        },
    )
}

pub(crate) fn local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
) -> Result<DfsMetadataReplayCursor> {
    Ok(
        read_sobj_optional::<DfsMetadataReplayCursor>(&local_materialization_cursor_path(root))?
            .unwrap_or(DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: next_sequence.saturating_sub(1),
            }),
    )
}

pub(crate) fn advance_local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    sequence: u64,
) -> Result<()> {
    let mut cursor = local_materialization_cursor(root, shard_id, next_sequence)?;
    if sequence <= cursor.last_sequence {
        return Ok(());
    }
    cursor.last_sequence = sequence;
    write_local_materialization_cursor(root, &cursor)
}

pub(crate) fn reconcile_local_materialization_cursor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    materialized_sequence: u64,
) -> Result<()> {
    if materialized_sequence == 0 {
        return Ok(());
    }
    advance_local_materialization_cursor(
        root,
        shard_id,
        next_sequence,
        materialized_sequence.min(next_sequence.saturating_sub(1)),
    )
}

pub(crate) fn checkpoint_local_materialization_cursor_if_due(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    sequence: u64,
) -> Result<()> {
    let cursor = local_materialization_cursor(root, shard_id, next_sequence)?;
    checkpoint_local_materialization_cursor_from_if_due(root, cursor, sequence)
}

pub(crate) fn checkpoint_local_materialization_cursor_from_if_due(
    root: &Path,
    cursor: DfsMetadataReplayCursor,
    sequence: u64,
) -> Result<()> {
    if sequence <= cursor.last_sequence
        || sequence.saturating_sub(cursor.last_sequence) < REPLAY_CURSOR_FLUSH_INTERVAL
    {
        return Ok(());
    }
    write_local_materialization_cursor(
        root,
        &DfsMetadataReplayCursor {
            shard_id: cursor.shard_id,
            last_sequence: sequence,
        },
    )
}

pub(crate) fn write_local_materialization_cursor(
    root: &Path,
    cursor: &DfsMetadataReplayCursor,
) -> Result<()> {
    write_sobj_atomic(&local_materialization_cursor_path(root), cursor)
}

pub(crate) fn metadata_replication_cursor(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> Result<u64> {
    // A sender cursor only avoids resending already acknowledged history.
    // Losing its atomic, non-durable checkpoint must restart catch-up, never
    // manufacture acknowledgement evidence or prevent replica recovery.
    match crate::metadata_io::read_sobj::<DfsMetadataReplayCursor>(
        &metadata_replication_cursor_path(root, target_node_id, shard_id),
    ) {
        Ok(cursor) if cursor.shard_id == *shard_id => Ok(cursor.last_sequence),
        Ok(_) | Err(MetadataBackendError::Serde(_)) => Ok(0),
        Err(MetadataBackendError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(0)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn put_metadata_replication_cursor(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<()> {
    put_metadata_replication_cursor_and_observe(root, target_node_id, shard_id, next_after_sequence)
        .map(|_| ())
}

/// Return the resulting scheduling hint without rereading it after publication.
/// No authority or durability evidence is minted by this value.
pub(crate) fn put_metadata_replication_cursor_and_observe(
    root: &Path,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<u64> {
    let current_sequence = metadata_replication_cursor(root, target_node_id, shard_id)?;
    let next_after_sequence = next_after_sequence.max(current_sequence);
    if next_after_sequence == current_sequence {
        return Ok(next_after_sequence);
    }
    crate::metadata_io::write_sobj_hint_atomic(
        &metadata_replication_cursor_path(root, target_node_id, shard_id),
        &DfsMetadataReplayCursor {
            shard_id: shard_id.clone(),
            last_sequence: next_after_sequence,
        },
    )?;
    Ok(next_after_sequence)
}

pub(crate) fn metadata_prefix_ack_floor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataPrefixAckFloor>> {
    crate::metadata_ack_floor_log::read(&metadata_prefix_ack_floor_path(root, shard_id), shard_id)
}

pub(crate) fn put_metadata_prefix_ack_floor(
    root: &Path,
    floor: &DfsMetadataPrefixAckFloor,
) -> Result<()> {
    crate::metadata_ack_floor_log::put(
        &metadata_prefix_ack_floor_path(root, &floor.shard_id),
        floor,
    )
}

impl crate::metadata_ack_floor_log::AckFloor for DfsMetadataPrefixAckFloor {
    const KIND: &'static str = "prefix";
    fn shard(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }
    fn validate(&self) -> Result<()> {
        let floor = self;
        if floor.state_fingerprint.is_empty() && floor.durable_prefix_proof.is_none() {
            return Err(MetadataBackendError::Serde(format!(
                "metadata prefix acknowledgement for shard {} has no durable evidence",
                floor.shard_id.0
            )));
        }
        if let Some(proof) = floor.durable_prefix_proof.as_ref()
            && (!proof.is_well_formed() || proof.journal_sequence != floor.journal_sequence)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata prefix acknowledgement for shard {} has an invalid durable prefix at sequence {}",
                floor.shard_id.0, floor.journal_sequence
            )));
        }
        Ok(())
    }
    fn merge(current: &Self, floor: &Self) -> Result<Self> {
        if floor.journal_sequence < current.journal_sequence {
            return Ok(current.clone());
        }
        if floor.journal_sequence == current.journal_sequence {
            if !floor.state_fingerprint.is_empty()
                && !current.state_fingerprint.is_empty()
                && floor.state_fingerprint != current.state_fingerprint
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata prefix acknowledgement for shard {} sequence {} conflicts with the durable fingerprint",
                    floor.shard_id.0, floor.journal_sequence
                )));
            }
            if let (Some(current), Some(new)) = (
                current.durable_prefix_proof.as_ref(),
                floor.durable_prefix_proof.as_ref(),
            ) && current != new
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata prefix acknowledgement for shard {} sequence {} conflicts with the durable journal prefix",
                    floor.shard_id.0, floor.journal_sequence
                )));
            }
            let merged = DfsMetadataPrefixAckFloor {
                shard_id: floor.shard_id.clone(),
                journal_sequence: floor.journal_sequence,
                state_fingerprint: if floor.state_fingerprint.is_empty() {
                    current.state_fingerprint.clone()
                } else {
                    floor.state_fingerprint.clone()
                },
                durable_prefix_proof: floor
                    .durable_prefix_proof
                    .clone()
                    .or_else(|| current.durable_prefix_proof.clone()),
            };
            if &merged == current {
                return Ok(current.clone());
            }
            return Ok(merged);
        }
        Ok(floor.clone())
    }
}

pub(crate) fn metadata_authority_ack_floor(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
    crate::metadata_ack_floor_log::read(
        &metadata_authority_ack_floor_path(root, shard_id),
        shard_id,
    )
}

pub(crate) fn put_metadata_authority_ack_floor(
    root: &Path,
    floor: &DfsMetadataAuthorityAckFloor,
) -> Result<()> {
    crate::metadata_ack_floor_log::put(
        &metadata_authority_ack_floor_path(root, &floor.shard_id),
        floor,
    )
}

impl crate::metadata_ack_floor_log::AckFloor for DfsMetadataAuthorityAckFloor {
    const KIND: &'static str = "authority";
    fn shard(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }
    fn validate(&self) -> Result<()> {
        let floor = self;
        if floor.journal_sequence == 0 {
            return Err(MetadataBackendError::Serde(format!(
                "metadata authority acknowledgement for shard {} has sequence zero",
                floor.shard_id.0
            )));
        }
        if let Some(proof) = floor.durable_prefix_proof.as_ref()
            && (!proof.is_well_formed() || proof.journal_sequence != floor.journal_sequence)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata authority acknowledgement for shard {} has an invalid durable prefix at sequence {}",
                floor.shard_id.0, floor.journal_sequence
            )));
        }
        Ok(())
    }
    fn merge(current: &Self, floor: &Self) -> Result<Self> {
        if floor.journal_sequence < current.journal_sequence {
            return Ok(current.clone());
        }
        if floor.journal_sequence == current.journal_sequence {
            match (
                current.durable_prefix_proof.as_ref(),
                floor.durable_prefix_proof.as_ref(),
            ) {
                (_, None) => return Ok(current.clone()),
                (None, Some(_)) => {
                    // Upgrade a legacy sequence-only floor with the exact
                    // durable proof learned by a versioned retry.
                }
                (Some(current_proof), Some(new_proof)) if current_proof == new_proof => {
                    return Ok(current.clone());
                }
                (Some(_), Some(_)) => {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata authority acknowledgement for shard {} sequence {} conflicts with its durable prefix",
                        floor.shard_id.0, floor.journal_sequence
                    )));
                }
            }
        }
        Ok(floor.clone())
    }
}

pub(crate) fn write_journal_head(root: &Path, next_sequence: u64) -> Result<()> {
    write_sobj_atomic(
        &journal_head_path(root),
        &DfsMetadataJournalHead { next_sequence },
    )
}

/// Publish the derived head after synced journal frames without adding a second
/// file+directory fsync. Crash recovery verifies the actual durable tail.
pub(crate) fn write_journal_head_hint(root: &Path, next_sequence: u64) -> Result<()> {
    crate::metadata_io::write_sobj_hint_atomic(
        &journal_head_path(root),
        &DfsMetadataJournalHead { next_sequence },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsMetadataPrefixProof;
    use crate::metadata_io::read_sobj;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    #[test]
    fn local_cursor_defaults_to_latest_local_sequence_until_persisted() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-cursor");
        let shard_id = DfsMetadataShardId::new("root");

        let cursor = local_materialization_cursor(&root, &shard_id, 12).expect("cursor");
        assert_eq!(
            cursor,
            DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 11
            }
        );

        write_local_materialization_cursor(
            &root,
            &DfsMetadataReplayCursor {
                shard_id: shard_id.clone(),
                last_sequence: 4,
            },
        )
        .expect("write cursor");
        assert_eq!(
            local_materialization_cursor(&root, &shard_id, 12).expect("persisted cursor"),
            DfsMetadataReplayCursor {
                shard_id,
                last_sequence: 4
            }
        );

        cleanup(&root);
    }

    #[test]
    fn metadata_prefix_ack_floor_is_durable_monotonic_and_fingerprint_bound() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-prefix-ack-floor");
        let shard_id = DfsMetadataShardId::new("root");
        let first = DfsMetadataPrefixAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: 10,
            state_fingerprint: "blake3:first".to_string(),
            durable_prefix_proof: None,
        };
        put_metadata_prefix_ack_floor(&root, &first).expect("write first floor");
        put_metadata_prefix_ack_floor(
            &root,
            &DfsMetadataPrefixAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: 7,
                state_fingerprint: "blake3:stale".to_string(),
                durable_prefix_proof: None,
            },
        )
        .expect("stale floor is ignored");
        assert_eq!(
            metadata_prefix_ack_floor(&root, &shard_id).expect("read floor"),
            Some(first.clone())
        );
        assert!(
            put_metadata_prefix_ack_floor(
                &root,
                &DfsMetadataPrefixAckFloor {
                    shard_id: shard_id.clone(),
                    journal_sequence: 10,
                    state_fingerprint: "blake3:conflict".to_string(),
                    durable_prefix_proof: None,
                },
            )
            .is_err(),
            "equal sequence with different state must not replace an acknowledgement"
        );
        let exact_prefix = DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 10,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64)),
        };
        put_metadata_prefix_ack_floor(
            &root,
            &DfsMetadataPrefixAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: 10,
                state_fingerprint: String::new(),
                durable_prefix_proof: Some(exact_prefix.clone()),
            },
        )
        .expect("enrich semantic floor with exact journal prefix");
        assert_eq!(
            metadata_prefix_ack_floor(&root, &shard_id).expect("read enriched floor"),
            Some(DfsMetadataPrefixAckFloor {
                durable_prefix_proof: Some(exact_prefix),
                ..first
            })
        );
        let advanced = DfsMetadataPrefixAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: 11,
            state_fingerprint: "blake3:advanced".to_string(),
            durable_prefix_proof: None,
        };
        put_metadata_prefix_ack_floor(&root, &advanced).expect("advance floor");
        assert_eq!(
            metadata_prefix_ack_floor(&root, &shard_id).expect("read advanced floor"),
            Some(advanced)
        );
    }

    #[test]
    fn metadata_replication_cursor_is_monotonic() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-replication-cursor");
        let target = node("node_target");
        let shard_id = DfsMetadataShardId::new("root");

        put_metadata_replication_cursor(&root, &target, &shard_id, 42).expect("write cursor");
        put_metadata_replication_cursor(&root, &target, &shard_id, 7).expect("stale cursor");

        assert_eq!(
            metadata_replication_cursor(&root, &target, &shard_id).expect("read cursor"),
            42
        );
        let cursor = read_sobj::<DfsMetadataReplayCursor>(&metadata_replication_cursor_path(
            &root, &target, &shard_id,
        ))
        .expect("durable cursor");
        assert_eq!(cursor.last_sequence, 42);

        cleanup(&root);
    }

    #[test]
    fn metadata_replication_cursor_recovers_lost_and_invalid_hints() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-replication-hint-recovery");
        let target = node("node_hint_target");
        let shard_id = DfsMetadataShardId::new("root");
        let path = metadata_replication_cursor_path(&root, &target, &shard_id);
        assert_eq!(
            metadata_replication_cursor(&root, &target, &shard_id).unwrap(),
            0
        );
        put_metadata_replication_cursor(&root, &target, &shard_id, 42).unwrap();
        let valid = std::fs::read(&path).unwrap();
        let wrong_shard = crate::metadata_io::encode_sobj(&DfsMetadataReplayCursor {
            shard_id: DfsMetadataShardId::new("other"),
            last_sequence: 999,
        })
        .unwrap();
        for invalid in [
            Vec::new(),
            b"malformed hint".to_vec(),
            valid[..valid.len() / 2].to_vec(),
            wrong_shard,
        ] {
            std::fs::write(&path, invalid).unwrap();
            assert_eq!(
                metadata_replication_cursor(&root, &target, &shard_id).unwrap(),
                0
            );
            put_metadata_replication_cursor(&root, &target, &shard_id, 7).unwrap();
            assert_eq!(
                metadata_replication_cursor(&root, &target, &shard_id).unwrap(),
                7
            );
        }
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            metadata_replication_cursor(&root, &target, &shard_id).unwrap(),
            0
        );
        put_metadata_replication_cursor(&root, &target, &shard_id, 8).unwrap();
        assert_eq!(
            metadata_replication_cursor(&root, &target, &shard_id).unwrap(),
            8
        );
    }

    #[test]
    fn metadata_replication_hint_recovery_preserves_io_errors_and_ack_floors() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-replication-hint-fail-closed");
        let target = node("node_hint_target");
        let shard_id = DfsMetadataShardId::new("root");
        let path = metadata_replication_cursor_path(&root, &target, &shard_id);
        std::fs::create_dir_all(&path).unwrap();
        assert!(matches!(
            metadata_replication_cursor(&root, &target, &shard_id),
            Err(MetadataBackendError::Io { .. })
        ));
        assert!(put_metadata_replication_cursor(&root, &target, &shard_id, 7).is_err());
        for path in [
            metadata_prefix_ack_floor_path(&root, &shard_id),
            metadata_authority_ack_floor_path(&root, &shard_id),
        ] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"malformed authority floor").unwrap();
        }
        assert!(metadata_prefix_ack_floor(&root, &shard_id).is_err());
        assert!(metadata_authority_ack_floor(&root, &shard_id).is_err());
    }

    #[test]
    fn metadata_authority_ack_floor_is_durable_and_monotonic() {
        let root = sinomo_testdir::TestDir::new("sinomo-metadata-authority-ack-floor");
        let shard_id = DfsMetadataShardId::new("root");
        let first = DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: 10,
            durable_prefix_proof: None,
        };
        put_metadata_authority_ack_floor(&root, &first).expect("write first floor");
        put_metadata_authority_ack_floor(
            &root,
            &DfsMetadataAuthorityAckFloor {
                shard_id: shard_id.clone(),
                journal_sequence: 7,
                durable_prefix_proof: None,
            },
        )
        .expect("stale floor is ignored");
        assert_eq!(
            metadata_authority_ack_floor(&root, &shard_id).expect("read floor"),
            Some(first)
        );
        let advanced = DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: 11,
            durable_prefix_proof: None,
        };
        put_metadata_authority_ack_floor(&root, &advanced).expect("advance floor");
        assert_eq!(
            metadata_authority_ack_floor(&root, &shard_id).expect("read advanced floor"),
            Some(advanced.clone())
        );
        let exact = DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: 11,
            durable_prefix_proof: Some(DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 11,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64)),
            }),
        };
        put_metadata_authority_ack_floor(&root, &exact).expect("upgrade exact floor");
        put_metadata_authority_ack_floor(&root, &exact).expect("repeat exact floor");
        assert_eq!(
            metadata_authority_ack_floor(&root, &shard_id).expect("read exact floor"),
            Some(exact.clone())
        );
        let conflicting = DfsMetadataAuthorityAckFloor {
            durable_prefix_proof: Some(DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 11,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "b".repeat(64)),
            }),
            ..exact.clone()
        };
        assert!(put_metadata_authority_ack_floor(&root, &conflicting).is_err());
        let wrong_sequence = DfsMetadataAuthorityAckFloor {
            journal_sequence: 12,
            durable_prefix_proof: exact.durable_prefix_proof,
            ..advanced
        };
        assert!(put_metadata_authority_ack_floor(&root, &wrong_sequence).is_err());
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            std::fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
