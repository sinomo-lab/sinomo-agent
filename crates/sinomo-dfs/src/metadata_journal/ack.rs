//! Receiver-local header on the last immutable frame of a certified append.
//! Prefix hashing and replication always consume the unchanged inner frame.
use super::*;
use crate::metadata_ack_floor_log::AckFloor;
use crate::{DfsMetadataAuthorityAckFloor, DfsMetadataPrefixAckFloor};

const MAGIC: &[u8; 8] = b"DFSACK01";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum JournalAck {
    Authority(DfsMetadataAuthorityAckFloor),
    Prefix(DfsMetadataPrefixAckFloor),
}

/// The receiver's authenticated configuration at the append, independent
/// of the logical journal bytes that are sent on to another participant.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalWitness {
    pub(crate) ack: JournalAck,
    pub(crate) file_system_id: crate::DfsFileSystemId,
    pub(crate) primary: sinomo_core::node::NodeId,
    pub(crate) epoch: u64,
    pub(crate) write_set_generation: Option<u64>,
}

impl JournalWitness {
    pub(super) fn validate(&self, entry: &DfsMetadataJournalEntry) -> Result<()> {
        self.ack.validate(entry)?;
        let epoch = match &self.ack {
            JournalAck::Authority(floor) => floor.authority_epoch,
            JournalAck::Prefix(floor) => floor.authority_epoch,
        };
        if self.epoch == 0
            || self.write_set_generation == Some(0)
            || epoch.is_some_and(|epoch| epoch != self.epoch)
            || matches!(&entry.mutation, DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record.digest.file_system_id != self.file_system_id || (matches!(self.ack, JournalAck::Authority(_)) && record.digest.owner_epoch != self.epoch))
        {
            return Err(MetadataBackendError::replica_diverged(
                "journal ACK and original shard identity disagree".into(),
            ));
        }
        Ok(())
    }
}

impl JournalAck {
    pub(super) fn validate(&self, entry: &DfsMetadataJournalEntry) -> Result<()> {
        let (shard, sequence) = match self {
            Self::Authority(floor) => {
                floor.validate()?;
                let DfsMetadataMutation::PrepareOperationV7 { record, .. } = &entry.mutation else {
                    return Err(MetadataBackendError::replica_diverged(
                        "authority evidence without its prepared operation".into(),
                    ));
                };
                if record.digest.prepare_sequence != entry.sequence
                    || record.digest.shard_id != entry.shard_id
                    || floor
                        .authority_epoch
                        .is_some_and(|epoch| epoch != record.digest.owner_epoch)
                {
                    return Err(MetadataBackendError::replica_diverged(
                        "authority evidence names another prepared operation".into(),
                    ));
                }
                (&floor.shard_id, floor.journal_sequence)
            }
            Self::Prefix(floor) => {
                floor.validate()?;
                (&floor.shard_id, floor.journal_sequence)
            }
        };
        if shard != &entry.shard_id || sequence != entry.sequence {
            return Err(MetadataBackendError::replica_diverged(
                "ACK evidence names another frame".into(),
            ));
        }
        Ok(())
    }
}

impl JournalWitness {
    pub(super) fn encode(&self, entry: &DfsMetadataJournalEntry, bytes: &[u8]) -> Result<Vec<u8>> {
        self.validate(entry)?;
        let header = sinomo_object_serde::to_bytes(self, 1, LengthMode::U32)
            .map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
        let size =
            u32::try_from(header.len()).map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
        let mut buffer = Vec::with_capacity(12 + header.len() + bytes.len() + 32);
        buffer.extend_from_slice(MAGIC);
        buffer.extend_from_slice(&size.to_le_bytes());
        buffer.extend_from_slice(&header);
        buffer.extend_from_slice(bytes);
        let checksum = blake3::hash(&buffer);
        buffer.extend_from_slice(checksum.as_bytes());
        Ok(buffer)
    }
}

pub(crate) fn split(bytes: &[u8]) -> Result<(Option<JournalWitness>, &[u8])> {
    if !bytes.starts_with(MAGIC) {
        return Ok((None, bytes));
    }
    let invalid = || {
        MetadataBackendError::replica_diverged(
            "invalid checksummed replica journal ACK record".into(),
        )
    };
    let end = bytes
        .len()
        .checked_sub(32)
        .filter(|end| *end > 12)
        .ok_or_else(invalid)?;
    let (data, checksum) = bytes.split_at(end);
    if checksum != blake3::hash(data).as_bytes() {
        return Err(invalid());
    }
    let size = u32::from_le_bytes(data[8..12].try_into().expect("checked header")) as usize;
    let (header, frame) = data[12..].split_at_checked(size).ok_or_else(invalid)?;
    let header = sinomo_object_serde::from_bytes(header)
        .map_err(|e| MetadataBackendError::replica_diverged(e.to_string()))?;
    Ok((Some(header), frame))
}

/// Only offsets are cached. An ACK floor is always read and checked against
/// the actual local journal and its exact frame.
#[derive(Clone, Debug, Default)]
pub(super) struct AckOffsets {
    authority: Option<u64>,
    prefix: Option<u64>,
}
impl AckOffsets {
    pub(super) fn observe(&mut self, ack: Option<&JournalAck>, offset: u64) {
        match ack {
            Some(JournalAck::Authority(_)) => self.authority = Some(offset),
            Some(JournalAck::Prefix(_)) => self.prefix = Some(offset),
            None => {}
        }
    }
}

pub(crate) fn floor(root: &Path, authority: bool) -> Result<Option<JournalAck>> {
    let Some((_, path)) = highest_segmented_journal_path(root)? else {
        return Ok(None);
    };
    let mut segment = open_indexed_journal_segment(&path)?;
    let offsets = &segment.index.acks;
    let Some(offset) = (if authority {
        offsets.authority
    } else {
        offsets.prefix
    }) else {
        return Ok(None);
    };
    segment
        .file
        .seek(SeekFrom::Start(offset))
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    let ack = read_next_segmented_journal_frame(&mut segment.file, &path)?
        .and_then(|frame| frame.ack.map(|w| w.ack))
        .ok_or_else(|| {
            MetadataBackendError::replica_diverged(
                "indexed ACK is missing its journal frame".into(),
            )
        })?;
    if matches!(ack, JournalAck::Authority(_)) != authority {
        return Err(MetadataBackendError::replica_diverged(
            "indexed ACK changed kind".into(),
        ));
    }
    Ok(Some(ack))
}

/// Before the active segment can be removed or stop being the tail, carry
/// its two independent promises into the existing compact floor stores.
pub(crate) fn checkpoint(root: &Path) -> Result<()> {
    if let Some(JournalAck::Authority(f)) = floor(root, true)? {
        crate::metadata_cursor::put_metadata_authority_ack_floor(root, &f)?;
    }
    if let Some(JournalAck::Prefix(f)) = floor(root, false)? {
        crate::metadata_cursor::put_metadata_prefix_ack_floor(root, &f)?;
    }
    Ok(())
}

pub(super) fn before_new_segment(root: &Path, path: &Path) -> Result<()> {
    if !path
        .try_exists()
        .map_err(|source| MetadataBackendError::Io {
            path: path.into(),
            source,
        })?
    {
        checkpoint(root)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn one_flush_recovers_exact_frame_and_floor_across_torn_tail_rotation_and_pruning() {
        let dir = sinomo_testdir::TestDir::new("journal-ack-restart");
        let root = dir.path();
        let entry = super::super::tests::test_journal_entry(1);
        let frame = DfsMetadataJournalFrame::encode(&entry).unwrap();
        let genesis = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&entry.shard_id);
        let proof = frame.prefix_proof_after(&genesis).unwrap();
        let floor = DfsMetadataAuthorityAckFloor {
            shard_id: entry.shard_id.clone(),
            journal_sequence: 1,
            durable_prefix_proof: proof,
            authority_epoch: Some(1),
        };
        let DfsMetadataMutation::PrepareOperationV7 { file_system_id, .. } = &entry.mutation else {
            unreachable!()
        };
        let witness = JournalWitness {
            ack: JournalAck::Authority(floor.clone()),
            file_system_id: file_system_id.clone(),
            primary: sinomo_core::node::NodeId::generate(),
            epoch: 1,
            write_set_generation: Some(1),
        };
        reset_journal_append_sync_calls_for_test();
        append_segmented_journal_frames_with_ack(
            root,
            &[(&entry, frame.exact_bytes())],
            0,
            Some(&witness),
        )
        .unwrap();
        assert_eq!(journal_append_sync_calls_for_test(), 1);
        assert!(
            !crate::metadata_cursor::metadata_authority_ack_floor_path(root, &entry.shard_id)
                .exists()
        );
        let original = crate::DfsMetadataJournalFrameRecord {
            sequence: entry.sequence,
            exact_bytes: bytes::Bytes::copy_from_slice(frame.exact_bytes()),
        };
        assert_eq!(
            persist_replayed_journal_frames_with_ack(
                root,
                &[(&entry, &original)],
                entry.sequence,
                Some(&witness)
            )
            .unwrap(),
            (Some(1), true)
        );
        let mut changed = witness.clone();
        changed.write_set_generation = Some(2);
        assert_eq!(
            persist_replayed_journal_frames_with_ack(
                root,
                &[(&entry, &original)],
                entry.sequence,
                Some(&changed)
            )
            .unwrap(),
            (Some(1), false)
        );
        assert_eq!(journal_append_sync_calls_for_test(), 1);
        let segment = highest_segmented_journal_path(root).unwrap().unwrap().1;
        let cold = sinomo_testdir::TestDir::new("journal-ack-cold");
        let cold_segment = cold.path().join(segment.strip_prefix(root).unwrap());
        fs::create_dir_all(cold_segment.parent().unwrap()).unwrap();
        fs::copy(&segment, &cold_segment).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&cold_segment)
            .unwrap()
            .write_all(&[64, 0, 0, 0, 1, 2])
            .unwrap();
        let recovered = read_segmented_journal_frame_at_sequence(cold.path(), 1)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.exact_bytes(), frame.exact_bytes());
        assert_eq!(
            crate::metadata_cursor::metadata_authority_ack_floor(cold.path(), &entry.shard_id)
                .unwrap(),
            Some(floor.clone())
        );
        let next = super::super::tests::test_journal_entry(JOURNAL_SEGMENT_ENTRY_LIMIT + 1);
        append_segmented_journal_frame_with_record_bytes(
            root,
            &DfsMetadataJournalFrame::encode(&next).unwrap(),
        )
        .unwrap();
        prune_segmented_journal_before_checkpoint(root, next.sequence).unwrap();
        assert!(!segment.exists());
        assert_eq!(
            crate::metadata_cursor::metadata_authority_ack_floor(root, &entry.shard_id).unwrap(),
            Some(floor)
        );
        let mut body = fs::read(&cold_segment).unwrap();
        body[16] ^= 1;
        fs::write(cold_segment, body).unwrap();
        assert!(
            crate::metadata_cursor::metadata_authority_ack_floor(cold.path(), &entry.shard_id)
                .is_err()
        );
    }
}
