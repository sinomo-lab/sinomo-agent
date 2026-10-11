//! Bounded, read-only proof of ancestry in the ORIGINAL source journal.
//! Current converted frames and native target indices are never inputs.
use super::{MetadataSourceCut, Result, ack_floor::LegacyAcknowledgement, error, source_proof};
use serde::{Deserialize, Serialize};
use sinomo_core::object::serde_ext;
use sinomo_dfs::DfsMetadataPrefixProof;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_SOURCE_ANCESTRY_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceAncestryChunk {
    pub(crate) source_binding: String,
    pub(crate) sequence: u64,
    pub(crate) frame_length: u64,
    pub(crate) offset: u64,
    #[serde(with = "serde_bytes")]
    pub(crate) bytes: Vec<u8>,
    /// A captured source table/snapshot anchor, rather than a generated ACK.
    pub(crate) resulting_anchor: Option<DfsMetadataPrefixProof>,
}

fn binding(cut: &MetadataSourceCut) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        &cut.source_identity,
        &cut.source_snapshot_fingerprint,
        &cut.scope_shard,
        &cut.file_system_ids,
        cut.source_journal_sequence,
        &cut.source_state_fingerprint,
        &cut.source_exact_prefix,
        &cut.projection_assignments,
    ))
    .map_err(error)?;
    let bytes = if let Some(format) = cut.source_format_version {
        serde_json::to_vec(&("format-qualified-source-ancestry", format, bytes)).map_err(error)?
    } else {
        bytes
    };
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}
fn anchor_path(stage: &Path, sequence: u64) -> PathBuf {
    stage
        .join("source-prefix-evidence")
        .join(format!("{sequence:020}.json"))
}
pub(super) fn retained_anchor(
    stage: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataPrefixProof>> {
    let path = anchor_path(stage, sequence);
    if path.is_file() {
        Ok(Some(super::read_json(&path)?))
    } else {
        Ok(None)
    }
}
pub(super) fn store_anchor(stage: &Path, proof: &DfsMetadataPrefixProof) -> Result<()> {
    source_proof::validate(proof)?;
    let path = anchor_path(stage, proof.journal_sequence);
    if path.is_file() {
        let old: DfsMetadataPrefixProof = super::read_json(&path)?;
        if old != *proof {
            return Err(error(
                "retained source prefix anchors fork at the same sequence",
            ));
        }
    } else {
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| error("source anchor has no parent"))?,
        )
        .map_err(error)?;
        super::write_json(&path, proof)?;
    }
    Ok(())
}

/// Retains real source custody for the entire authenticated transfer. The
/// caller persists chunks with its ordinary staged-transfer cursor.
pub(crate) struct OwnedSourceAncestryReader {
    _source_fence: Arc<File>,
    stage: PathBuf,
    binding: String,
    next_sequence: u64,
    head: u64,
    frame: Option<(File, u64, u64)>,
    verifier: SourceAncestryVerifier,
    finished: bool,
}
pub(crate) fn source_ancestry_reader(
    cut: &MetadataSourceCut,
    actual_source_root: &Path,
    floors: &[LegacyAcknowledgement],
) -> Result<OwnedSourceAncestryReader> {
    let fence = super::retain_source_fence(actual_source_root)?;
    super::validate_retained_source_identity(
        actual_source_root,
        &cut.source_identity,
        &cut.source_snapshot_fingerprint,
    )?;
    let verifier = SourceAncestryVerifier::new(cut, floors)?;
    let next_sequence = verifier.next_sequence;
    Ok(OwnedSourceAncestryReader {
        _source_fence: fence,
        stage: cut
            .staged_root
            .parent()
            .ok_or_else(|| error("current image has no source evidence parent"))?
            .to_owned(),
        binding: binding(cut)?,
        next_sequence,
        head: cut.source_journal_sequence,
        frame: None,
        verifier,
        finished: false,
    })
}
impl OwnedSourceAncestryReader {
    pub(crate) fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<SourceAncestryChunk>> {
        if max_bytes == 0 || max_bytes > MAX_SOURCE_ANCESTRY_CHUNK_BYTES {
            return Err(error("source ancestry chunk budget is outside 1..=1MiB"));
        }
        if self.finished {
            return Ok(None);
        }
        if self.next_sequence > self.head {
            self.verifier.finish()?;
            self.finished = true;
            return Ok(None);
        }
        if self.frame.is_none() {
            let path = super::evidence_path(&self.stage, self.next_sequence);
            let file = File::open(&path).map_err(error)?;
            let length = file.metadata().map_err(error)?.len();
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(error("retained ancestry frame is empty or exceeds 64MiB"));
            }
            self.frame = Some((file, length, 0));
        }
        let (file, length, offset) = self.frame.as_mut().expect("opened original frame");
        let count = (*length - *offset).min(max_bytes as u64) as usize;
        let mut bytes = vec![0; count];
        file.read_exact(&mut bytes).map_err(error)?;
        let last = *offset + count as u64 == *length;
        let anchor = anchor_path(&self.stage, self.next_sequence);
        let chunk = SourceAncestryChunk {
            source_binding: self.binding.clone(),
            sequence: self.next_sequence,
            frame_length: *length,
            offset: *offset,
            bytes,
            resulting_anchor: if last && anchor.is_file() {
                Some(super::read_json(&anchor)?)
            } else {
                None
            },
        };
        self.verifier.apply(&chunk)?;
        *offset += count as u64;
        if last {
            self.frame = None;
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| error("source ancestry sequence overflow"))?;
        }
        Ok(Some(chunk))
    }
}

pub(crate) struct SourceAncestryVerifier {
    binding: String,
    floors: Vec<LegacyAcknowledgement>,
    proofs: Vec<Option<DfsMetadataPrefixProof>>,
    head: u64,
    head_proof: Option<DfsMetadataPrefixProof>,
    next_sequence: u64,
    frame: Vec<u8>,
    frame_length: u64,
    poisoned: bool,
}
impl SourceAncestryVerifier {
    pub(crate) fn new(cut: &MetadataSourceCut, floors: &[LegacyAcknowledgement]) -> Result<Self> {
        let mut proofs = Vec::with_capacity(floors.len());
        for floor in floors {
            if floor.journal_sequence > cut.source_journal_sequence {
                return Err(error(
                    "retained source ACK floor is ahead of the immutable source cut",
                ));
            }
            let Some(proof) = floor.durable_prefix_proof.clone() else {
                // A legacy authority height carries no prefix identity. Keep
                // that constraint, but cover it only with an actual anchored
                // ACK in the SAME encoded source clock. Each such anchor is
                // independently checked and extended below; no proof is copied
                // into the height-only row.
                if !floor.state_fingerprint.is_empty()
                    || !floors.iter().any(|anchor| {
                        anchor.shard_id == floor.shard_id
                            && anchor.journal_sequence >= floor.journal_sequence
                            && anchor.durable_prefix_proof.is_some()
                    })
                {
                    return Err(error(
                        "source ACK floor has no retained prefix anchor; ancestry is unavailable",
                    ));
                }
                proofs.push(None);
                continue;
            };
            source_proof::validate(&proof)?;
            if proof.journal_sequence != floor.journal_sequence {
                return Err(error(
                    "source ACK prefix anchor differs from its actual floor",
                ));
            }
            if floor.journal_sequence == cut.source_journal_sequence
                && ((!floor.state_fingerprint.is_empty()
                    && floor.state_fingerprint != cut.source_state_fingerprint)
                    || cut.source_exact_prefix.as_ref() != Some(&proof))
            {
                return Err(error(
                    "retained source ACK floor forks the immutable source cut",
                ));
            }
            proofs.push(Some(proof));
        }
        if !floors.is_empty() {
            let head = cut
                .source_exact_prefix
                .as_ref()
                .ok_or_else(|| error("source cut has no retained prefix proof"))?;
            source_proof::validate(head)?;
            if head.journal_sequence != cut.source_journal_sequence {
                return Err(error(
                    "immutable source prefix differs from its journal cut",
                ));
            }
        }
        let next_sequence = floors
            .iter()
            .filter(|floor| floor.durable_prefix_proof.is_some())
            .map(|floor| floor.journal_sequence)
            .min()
            .unwrap_or(cut.source_journal_sequence)
            .checked_add(1)
            .ok_or_else(|| error("source ancestry sequence overflow"))?;
        Ok(Self {
            binding: binding(cut)?,
            floors: floors.to_vec(),
            proofs,
            head: cut.source_journal_sequence,
            head_proof: cut.source_exact_prefix.clone(),
            next_sequence,
            frame: Vec::new(),
            frame_length: 0,
            poisoned: false,
        })
    }
    pub(crate) fn apply(&mut self, chunk: &SourceAncestryChunk) -> Result<()> {
        if self.poisoned {
            return Err(error("source ancestry verifier is poisoned"));
        }
        let result = self.apply_inner(chunk);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn apply_inner(&mut self, chunk: &SourceAncestryChunk) -> Result<()> {
        if chunk.source_binding != self.binding
            || chunk.sequence != self.next_sequence
            || chunk.sequence > self.head
            || chunk.frame_length == 0
            || chunk.frame_length > MAX_FRAME_BYTES
            || chunk.offset != self.frame.len() as u64
            || chunk.bytes.is_empty()
            || chunk.bytes.len() > MAX_SOURCE_ANCESTRY_CHUNK_BYTES
            || chunk
                .offset
                .checked_add(chunk.bytes.len() as u64)
                .is_none_or(|end| end > chunk.frame_length)
            || (!self.frame.is_empty() && self.frame_length != chunk.frame_length)
        {
            return Err(error(
                "source ancestry chunk has a foreign cut, gap, overlap, or invalid frame bound",
            ));
        }
        self.frame_length = chunk.frame_length;
        self.frame.extend_from_slice(&chunk.bytes);
        if self.frame.len() as u64 != chunk.frame_length {
            if chunk.resulting_anchor.is_some() {
                return Err(error(
                    "source anchor arrived before its complete exact frame",
                ));
            }
            return Ok(());
        }
        let entry: serde_json::Value = serde_ext::from_bytes(&self.frame).map_err(error)?;
        if entry.get("sequence").and_then(serde_json::Value::as_u64) != Some(chunk.sequence) {
            return Err(error("source ancestry exact frame changed its sequence"));
        }
        if let Some(anchor) = &chunk.resulting_anchor {
            source_proof::validate(anchor)?;
            if anchor.journal_sequence != chunk.sequence {
                return Err(error("source anchor has a different frame sequence"));
            }
        }
        for (floor, proof) in self.floors.iter().zip(&mut self.proofs) {
            let Some(proof) = proof else { continue };
            if floor.journal_sequence >= chunk.sequence {
                continue;
            }
            if entry.get("shard_id") != Some(&serde_json::to_value(&floor.shard_id).map_err(error)?)
            {
                return Err(error(
                    "source ancestry frame crosses the actual encoded ACK shard",
                ));
            }
            let version = chunk
                .resulting_anchor
                .as_ref()
                .map_or(proof.version, |anchor| anchor.version);
            *proof = source_proof::extend_as(proof, chunk.sequence, &self.frame, version)?;
            if chunk
                .resulting_anchor
                .as_ref()
                .is_some_and(|anchor| anchor != proof)
            {
                return Err(error(
                    "original source frames fork their captured prefix anchor",
                ));
            }
        }
        self.frame.clear();
        self.frame_length = 0;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| error("source ancestry sequence overflow"))?;
        Ok(())
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if self.poisoned
            || !self.frame.is_empty()
            || self.next_sequence <= self.head
            || self
                .proofs
                .iter()
                .flatten()
                .any(|proof| Some(proof) != self.head_proof.as_ref())
        {
            return Err(error(
                "source ancestry is incomplete or does not reach the exact immutable source prefix",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::object::LengthMode;
    use sinomo_dfs::{DfsFileSystemId, DfsMetadataShardId};

    fn fixture() -> (
        sinomo_testdir::TestDir,
        MetadataSourceCut,
        LegacyAcknowledgement,
        Vec<u8>,
    ) {
        let dir = sinomo_testdir::TestDir::new("migration-source-ancestry");
        let root = dir.path().join("retained");
        let stage = dir.path().join("stage");
        fs::create_dir_all(root.join("journal")).unwrap();
        fs::create_dir_all(stage.join("journal-evidence")).unwrap();
        let shard = DfsMetadataShardId::new("original-source");
        let floor = DfsMetadataPrefixProof {
            version: 1,
            journal_sequence: 100,
            digest: format!(
                "dfs-metadata-prefix-v1:blake3:{}",
                blake3::hash(b"retained-checkpoint").to_hex()
            ),
        };
        let fs_id = DfsFileSystemId::generate();
        let bytes = serde_ext::to_bytes(
            &serde_json::json!({"shard_id":shard,"sequence":101,
            "created_at":"1970-01-01T00:00:00Z","mutation":{"type":"remove-version","file_system_id":fs_id,"object_id":sinomo_dfs::DfsObjectId::generate(),"version":sinomo_dfs::DfsVersion::generate()}}),
            1,
            LengthMode::U32,
        )
        .unwrap();
        fs::write(root.join("journal/00000000000000000101.sobj"), &bytes).unwrap();
        fs::write(super::super::evidence_path(&stage, 101), &bytes).unwrap();
        let head = source_proof::extend_as(&floor, 101, &bytes, 2).unwrap();
        store_anchor(&stage, &head).unwrap();
        let inventory = super::super::source::discover(&root).unwrap();
        let cut = MetadataSourceCut {
            source_root: root,
            source_identity: inventory.fingerprint.clone(),
            scope_shard: DfsMetadataShardId::new("target"),
            file_system_ids: vec![fs_id],
            source_snapshot_fingerprint: inventory.fingerprint,
            source_format_version: None,
            source_exact_prefix: Some(head.clone()),
            converted_image_fingerprint: "converted".into(),
            current_image_fingerprint: "current".into(),
            target_exact_prefix: head,
            source_journal_sequence: 101,
            source_state_fingerprint: "actual-source-state".into(),
            projection_assignments: Vec::new(),
            staged_root: stage.join("current-image"),
            pending_operations: Vec::new(),
            original_handoff: None,
        };
        let ack = LegacyAcknowledgement {
            shard_id: shard,
            journal_sequence: 100,
            state_fingerprint: String::new(),
            durable_prefix_proof: Some(floor),
        };
        (dir, cut, ack, bytes)
    }
    #[test]
    fn original_frames_stream_in_fragments_and_cross_an_actual_v1_v2_anchor() {
        let (_dir, cut, ack, _) = fixture();
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &[ack.clone()]).unwrap();
        let mut verifier = SourceAncestryVerifier::new(&cut, &[ack]).unwrap();
        assert!(verifier.finish().is_err());
        let mut count = 0;
        while let Some(chunk) = reader.next_chunk(17).unwrap() {
            assert!(chunk.bytes.len() <= 17);
            verifier.apply(&chunk).unwrap();
            count += 1;
        }
        assert!(count > 1);
        verifier.finish().unwrap();
    }
    #[test]
    fn changed_frame_cut_or_fragment_order_never_confirms_ancestry() {
        let (_dir, cut, ack, bytes) = fixture();
        let chunk = SourceAncestryChunk {
            source_binding: binding(&cut).unwrap(),
            sequence: 101,
            frame_length: bytes.len() as u64,
            offset: 0,
            bytes,
            resulting_anchor: cut.source_exact_prefix.clone(),
        };
        for change in 0..3 {
            let mut bad = chunk.clone();
            match change {
                0 => bad.source_binding.push('x'),
                1 => bad.offset = 1,
                _ => bad.bytes[8] ^= 1,
            }
            let mut verifier = SourceAncestryVerifier::new(&cut, &[ack.clone()]).unwrap();
            assert!(verifier.apply(&bad).is_err());
            assert!(verifier.apply(&chunk).is_err());
            assert!(verifier.finish().is_err());
        }
    }
    #[test]
    fn partial_frame_never_confirms_source_101_over_retained_floor_100() {
        let (_dir, cut, ack, _) = fixture();
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &[ack.clone()]).unwrap();
        let first = reader.next_chunk(17).unwrap().unwrap();
        let mut verifier = SourceAncestryVerifier::new(&cut, &[ack]).unwrap();
        verifier.apply(&first).unwrap();
        assert!(verifier.finish().is_err());
    }
    #[test]
    fn restart_replays_the_exact_durable_source_chunks_before_confirming() {
        let (dir, cut, ack, _) = fixture();
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &[ack.clone()]).unwrap();
        let ledger = dir.path().join("receiver-ledger");
        fs::create_dir(&ledger).unwrap();
        let mut count = 0;
        while let Some(chunk) = reader.next_chunk(17).unwrap() {
            let bytes = serde_ext::to_bytes(&chunk, 1, LengthMode::U32).unwrap();
            fs::write(ledger.join(format!("{count:020}.sobj")), bytes).unwrap();
            count += 1;
        }
        let mut verifier = SourceAncestryVerifier::new(&cut, &[ack]).unwrap();
        for ordinal in 0..count {
            let chunk: SourceAncestryChunk = serde_ext::from_bytes(
                &fs::read(ledger.join(format!("{ordinal:020}.sobj"))).unwrap(),
            )
            .unwrap();
            verifier.apply(&chunk).unwrap();
        }
        verifier.finish().unwrap();
    }
    #[test]
    fn missing_original_frame_anchor_or_receiver_prefix_is_unavailable() {
        let (_dir, cut, mut ack, _) = fixture();
        let stage = cut.staged_root.parent().unwrap();
        fs::remove_file(anchor_path(stage, 101)).unwrap();
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &[ack.clone()]).unwrap();
        while reader.next_chunk(1024).is_ok_and(|chunk| chunk.is_some()) {}
        assert!(reader.next_chunk(1024).is_err());
        ack.durable_prefix_proof = None;
        assert!(SourceAncestryVerifier::new(&cut, &[ack]).is_err());
        fs::remove_file(super::super::evidence_path(stage, 101)).unwrap();
        let (_, _, ack, _) = fixture();
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &[ack]).unwrap();
        assert!(reader.next_chunk(1024).is_err());
    }
    #[test]
    fn height_only_authority_is_covered_by_actual_same_clock_ancestry() {
        let (_dir, cut, ack, _) = fixture();
        let authority = LegacyAcknowledgement {
            shard_id: ack.shard_id.clone(),
            journal_sequence: 99,
            state_fingerprint: String::new(),
            durable_prefix_proof: None,
        };
        let floors = vec![authority.clone(), ack.clone()];
        let original = serde_json::to_vec(&floors).unwrap();
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &floors).unwrap();
        let mut verifier = SourceAncestryVerifier::new(&cut, &floors).unwrap();
        assert_eq!(verifier.floors.len(), 2);
        assert_eq!(verifier.proofs[0], None);
        assert!(verifier.finish().is_err());
        while let Some(chunk) = reader.next_chunk(17).unwrap() {
            verifier.apply(&chunk).unwrap();
        }
        verifier.finish().unwrap();
        assert_eq!(serde_json::to_vec(&floors).unwrap(), original);
        assert_eq!(verifier.floors[0].durable_prefix_proof, None);

        // Even a covering anchor at the exact head is actual evidence, while
        // its lower authority row remains proofless and unchanged.
        let mut head = ack;
        head.journal_sequence = cut.source_journal_sequence;
        head.durable_prefix_proof = cut.source_exact_prefix.clone();
        SourceAncestryVerifier::new(&cut, &[authority, head])
            .unwrap()
            .finish()
            .unwrap();
    }
    #[test]
    fn authority_coverage_never_discards_a_distinct_anchored_constraint() {
        let (_dir, cut, ack, _) = fixture();
        let authority = LegacyAcknowledgement {
            shard_id: ack.shard_id.clone(),
            journal_sequence: 99,
            state_fingerprint: String::new(),
            durable_prefix_proof: None,
        };
        for change in 0..5 {
            let mut bad = authority.clone();
            match change {
                0 => bad.journal_sequence = cut.source_journal_sequence + 1,
                1 => bad.journal_sequence = ack.journal_sequence + 1,
                2 => bad.shard_id = DfsMetadataShardId::new("different-clock"),
                3 => bad.state_fingerprint = "unanchored-prefix-identity".into(),
                _ => {
                    assert!(SourceAncestryVerifier::new(&cut, &[bad]).is_err());
                    continue;
                }
            }
            assert!(SourceAncestryVerifier::new(&cut, &[bad, ack.clone()]).is_err());
        }
        let mut head = ack.clone();
        head.journal_sequence = cut.source_journal_sequence;
        head.durable_prefix_proof = cut.source_exact_prefix.clone();
        let mut fork = ack;
        fork.durable_prefix_proof.as_mut().unwrap().digest =
            format!("dfs-metadata-prefix-v1:blake3:{}", "0".repeat(64));
        let floors = [authority, fork, head];
        let mut reader = source_ancestry_reader(&cut, &cut.source_root, &floors).unwrap();
        assert!(
            reader.next_chunk(1024).is_err(),
            "lower fork cannot be hidden by matching head"
        );
    }
}
