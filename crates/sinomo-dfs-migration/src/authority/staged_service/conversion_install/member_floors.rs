//! The acknowledgement floors a member's own copy of a converting shard
//! retains, and what anchors each of them.
//!
//! A conversion image may replace a member's copy only if every prefix that
//! copy acknowledged is an ancestor of the attempt's source cut. The member
//! reads the floors under the source fence of each copy that holds them and
//! anchors every floor; the stage then proves every anchor an ancestor of
//! the cut by extending it with the primary's retained source journal frames
//! up to the cut's exact prefix.
//!
//! - A copy in a per-shard native format retains exactly its own shard's
//!   floors, each with a version-2 prefix proof, which is its anchor.
//! - A legacy copy retains floors with a prefix proof, floors with a released
//!   scalar prefix, and height-only rows. A height-only row is anchored by an
//!   anchored floor of the same shard at least as high or, failing that, by a
//!   local checkpoint capture of the member's copy: the prefix its snapshot
//!   header records at a height at least the row's. The member's copy at that
//!   prefix extends the prefix it acknowledged at the row's height, so proving
//!   the checkpoint an ancestor of the cut proves the row's prefix one too.
//! - A legacy combined `root` copy holds several shards' rows and floors.
//!   Only this shard's floors are reconciled, from the shard's own copy and
//!   from the combined copy, whether the combined copy is still live or was
//!   retained beside the image a per-shard conversion of `root` installed.
//!
//! A floor that cannot be anchored, or whose anchor the cut does not extend,
//! refuses the stage with a [`FloorRefusal`].
use crate::metadata::MetadataSourceCut;
use crate::metadata::SourceCheckpointCapture;
use crate::metadata::ack_floor::LegacyAcknowledgement;
use crate::metadata::source_ancestry::{SourceAncestryVerifier, SourceCheckpointAnchor};
use sinomo_dfs::{DfsFileSystemId, DfsMetadataShardId, MetadataBackendError};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Result<T> = std::result::Result<T, MetadataBackendError>;

fn error(value: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(value.to_string())
}

/// Why a member refuses to reconcile its copy's floors with a source cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::authority) enum FloorRefusal {
    /// The copy is the retained source of an earlier import, whose floors
    /// belong to that import.
    EarlierImport,
    /// A native copy retains a floor of another shard, or one without a
    /// version-2 prefix proof.
    UnanchoredNativeFloor { journal_sequence: u64 },
    /// A legacy floor has no anchor the cut can carry: it is ahead of the
    /// cut, or its prefix has a form the cut's format does not define.
    UnanchorableFloor {
        journal_sequence: u64,
        reason: String,
    },
    /// A height-only floor that no anchored floor and no local checkpoint
    /// capture of the copy covers.
    NoCoveringCheckpoint { journal_sequence: u64 },
    /// The floors are not proved ancestors of the source cut.
    NotAncestor { reason: String },
}

impl std::fmt::Display for FloorRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EarlierImport => {
                write!(
                    f,
                    "this member's copy retains the floors of an earlier import"
                )
            }
            Self::UnanchoredNativeFloor { journal_sequence } => write!(
                f,
                "this member's copy retains a foreign or unanchored floor at {journal_sequence}"
            ),
            Self::UnanchorableFloor {
                journal_sequence,
                reason,
            } => write!(
                f,
                "this member's legacy floor at {journal_sequence} has no anchor the source cut carries: {reason}"
            ),
            Self::NoCoveringCheckpoint { journal_sequence } => write!(
                f,
                "this member's height-only floor at {journal_sequence} has no anchored floor or local checkpoint covering it"
            ),
            Self::NotAncestor { reason } => write!(
                f,
                "this member's floors are not proved ancestors of the source cut: {reason}"
            ),
        }
    }
}

impl FloorRefusal {
    pub(in crate::authority) fn not_ancestor(failure: impl std::fmt::Display) -> Self {
        Self::NotAncestor {
            reason: failure.to_string(),
        }
    }
}

impl From<FloorRefusal> for MetadataBackendError {
    fn from(refusal: FloorRefusal) -> Self {
        error(format!("shard conversion stage refused: {refusal}"))
    }
}

/// A member's reconciled floors, held under the source fences of the copies
/// they were read from.
pub(in crate::authority) struct MemberFloors {
    _fences: Vec<Arc<std::fs::File>>,
    pub floors: Vec<LegacyAcknowledgement>,
    /// The local checkpoint captures that anchor height-only floors.
    pub checkpoints: Vec<SourceCheckpointCapture>,
}

impl MemberFloors {
    pub(in crate::authority) fn anchors(&self) -> Vec<SourceCheckpointAnchor> {
        self.checkpoints
            .iter()
            .map(|capture| capture.anchor.clone())
            .collect()
    }

    /// The verifier that proves these floors ancestors of `cut`.
    pub(in crate::authority) fn verifier(
        &self,
        cut: &MetadataSourceCut,
    ) -> Result<SourceAncestryVerifier> {
        SourceAncestryVerifier::new(cut, &self.floors, &self.anchors())
            .map_err(|failure| FloorRefusal::not_ancestor(failure).into())
    }
}

/// The file beside a challenge's recorded floors that keeps the checkpoint
/// captures anchoring them, written only when there are any.
pub(in crate::authority) const CHECKPOINTS_RECORD: &str = "checkpoints.json";

/// The checkpoint captures recorded with the challenge at `ancestry`.
pub(in crate::authority) fn recorded_checkpoint_captures(
    ancestry: &Path,
) -> Result<Vec<SourceCheckpointCapture>> {
    match std::fs::read(ancestry.join(CHECKPOINTS_RECORD)) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(error),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(failure) => Err(error(failure)),
    }
}

/// The floors the member's copies of `shard` under `metadata_root` retain,
/// each anchored for `cut`, or the refusal that stops the stage.
pub(in crate::authority) fn member_floors(
    metadata_root: &Path,
    fs: &DfsFileSystemId,
    shard: &DfsMetadataShardId,
    cut: &MetadataSourceCut,
) -> Result<MemberFloors> {
    if cut.scope_shard != *shard {
        return Err(error(
            "the attempt's source cut names another physical shard",
        ));
    }
    let own = metadata_root.join(&shard.0);
    let own_present = own.try_exists().map_err(error)?;
    let mut fences = Vec::new();
    let mut floors = Vec::new();
    if own_present {
        fences.push(crate::metadata::retain_source_fence(&own)?);
        if own.join("metadata-import-v7.json").is_file() {
            return Err(FloorRefusal::EarlierImport.into());
        }
        if let Some(format) = crate::metadata::original_native_root_format(&own)? {
            let floors = crate::metadata::ack_floor::acknowledged_source_floors_for_root(
                &own,
                fs,
                Some(format),
                shard,
            )?;
            if let Some(floor) = floors.iter().find(|floor| {
                floor.shard_id != *shard
                    || floor
                        .durable_prefix_proof
                        .as_ref()
                        .is_none_or(|proof| proof.version != 2)
            }) {
                return Err(FloorRefusal::UnanchoredNativeFloor {
                    journal_sequence: floor.journal_sequence,
                }
                .into());
            }
            return checked(
                MemberFloors {
                    _fences: fences,
                    floors,
                    checkpoints: Vec::new(),
                },
                cut,
            );
        }
        floors.extend(shard_floors(&own, fs, shard)?);
    }
    if let Some(combined) = legacy_combined_root(metadata_root, shard)? {
        fences.push(crate::metadata::retain_source_fence(&combined)?);
        floors.extend(shard_floors(&combined, fs, shard)?);
    }
    // One row per exact floor, in a stable order.
    let floors = floors
        .into_iter()
        .map(|floor| Ok((serde_json::to_vec(&floor).map_err(error)?, floor)))
        .collect::<Result<BTreeMap<_, _>>>()?
        .into_values()
        .collect::<Vec<_>>();
    // Every floor here is this shard's, so an anchored floor covers each
    // height-only floor at or below its height.
    let mut anchored_height = None::<u64>;
    let mut height_only = Vec::new();
    for floor in &floors {
        let anchor = crate::metadata::source_ancestry::source_ack_prefix_anchor(
            floor,
            cut.source_format_version,
            shard,
            cut.source_journal_sequence,
        )
        .map_err(|failure| FloorRefusal::UnanchorableFloor {
            journal_sequence: floor.journal_sequence,
            reason: failure.to_string(),
        })?;
        match anchor {
            Some(_) => anchored_height = anchored_height.max(Some(floor.journal_sequence)),
            None => height_only.push(floor.journal_sequence),
        }
    }
    let uncovered = height_only
        .into_iter()
        .filter(|height| anchored_height.is_none_or(|anchored| anchored < *height))
        .max();
    let mut checkpoints = Vec::new();
    if let Some(journal_sequence) = uncovered {
        // Only the shard's own copy carries its clock in a checkpoint header.
        let capture = if own_present {
            crate::metadata::capture_legacy_source_checkpoint(&own, shard)?
        } else {
            None
        };
        match capture {
            Some(capture) if capture.anchor.prefix_proof.journal_sequence >= journal_sequence => {
                checkpoints.push(capture);
            }
            _ => return Err(FloorRefusal::NoCoveringCheckpoint { journal_sequence }.into()),
        }
    }
    checked(
        MemberFloors {
            _fences: fences,
            floors,
            checkpoints,
        },
        cut,
    )
}

fn checked(floors: MemberFloors, cut: &MetadataSourceCut) -> Result<MemberFloors> {
    floors.verifier(cut)?;
    Ok(floors)
}

/// The floors of `shard` a legacy copy at `root` retains; another shard's
/// floors in a combined copy are that shard's to reconcile.
fn shard_floors(
    root: &Path,
    fs: &DfsFileSystemId,
    shard: &DfsMetadataShardId,
) -> Result<Vec<LegacyAcknowledgement>> {
    Ok(
        crate::metadata::ack_floor::legacy_acknowledged_source_floors_for_root(root, fs)?
            .into_iter()
            .filter(|floor| floor.shard_id == *shard)
            .collect(),
    )
}

/// The legacy combined `root` copy, which can hold floors of `shard`: the
/// live copy while it is legacy, or the legacy copy a per-shard conversion
/// of `root` retained when it installed its image in the copy's place.
fn legacy_combined_root(
    metadata_root: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<PathBuf>> {
    crate::metadata::combined_root::legacy_combined_root(metadata_root, shard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::object::{LengthMode, serde_ext};
    use sinomo_dfs::DfsMetadataPrefixProof;

    fn proof(sequence: u64, seed: &str) -> DfsMetadataPrefixProof {
        DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: sequence,
            digest: format!(
                "dfs-metadata-prefix-v2:blake3:{}",
                blake3::hash(seed.as_bytes()).to_hex()
            ),
        }
    }

    fn floor(
        shard: &str,
        sequence: u64,
        proof: Option<DfsMetadataPrefixProof>,
    ) -> LegacyAcknowledgement {
        LegacyAcknowledgement {
            shard_id: DfsMetadataShardId::new(shard),
            journal_sequence: sequence,
            state_fingerprint: String::new(),
            durable_prefix_proof: proof,
        }
    }

    /// Write `floor` as a released plain floor row of `kind` in `root`.
    fn write_floor(root: &Path, kind: &str, floor: &LegacyAcknowledgement) {
        let path = root.join(format!(
            "coordination/metadata-{kind}-ack-floors/{}.sobj",
            floor.shard_id.0
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_ext::to_bytes(floor, 1, LengthMode::U32).unwrap(),
        )
        .unwrap();
    }

    /// A released format-3 snapshot header whose checkpoint is `prefix`.
    fn write_format3_checkpoint(root: &Path, prefix: &DfsMetadataPrefixProof) {
        #[derive(serde::Serialize)]
        struct Released3Header<'a> {
            format_version: u16,
            materialized_through_sequence: Option<u64>,
            prefix_proof: Option<&'a DfsMetadataPrefixProof>,
            generation: String,
        }
        let path = root.join("records/tables/snapshot.sobj");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let generation = blake3::hash(
            &serde_json::to_vec(&(3u16, Some(prefix.journal_sequence), Some(prefix))).unwrap(),
        )
        .to_hex()
        .to_string();
        let header = Released3Header {
            format_version: 3,
            materialized_through_sequence: Some(prefix.journal_sequence),
            prefix_proof: Some(prefix),
            generation,
        };
        let mut writer = sinomo_core::storage::migration::stream::MigrationStreamWriter::resume(
            &path,
            &serde_ext::to_bytes(&header, 3, LengthMode::U32).unwrap(),
            &Default::default(),
        )
        .unwrap();
        writer.finish().unwrap();
    }

    /// The primary's cut of `users` at `head`, retaining no journal frames.
    fn cut(stage: &Path, head: &DfsMetadataPrefixProof) -> MetadataSourceCut {
        MetadataSourceCut {
            retained_placement_reservations: Vec::new(),
            namespace_count_cohort: None,
            source_root: stage.join("primary-source"),
            source_identity: "primary-source".into(),
            scope_shard: DfsMetadataShardId::new("users"),
            file_system_ids: Vec::new(),
            source_snapshot_fingerprint: "primary-snapshot".into(),
            source_format_version: Some(3),
            source_exact_prefix: Some(head.clone()),
            converted_image_fingerprint: "converted".into(),
            current_image_fingerprint: "current".into(),
            target_exact_prefix: head.clone(),
            source_journal_sequence: head.journal_sequence,
            source_state_fingerprint: "primary-state".into(),
            projection_assignments: Vec::new(),
            staged_root: stage.join("namespace-count-current-image"),
            pending_operations: Vec::new(),
            original_handoff: None,
        }
    }

    fn refusal(result: Result<MemberFloors>) -> String {
        match result {
            Ok(_) => panic!("the member's floors must be refused"),
            Err(failure) => failure.to_string(),
        }
    }

    /// A member whose legacy floor is not an ancestor of the primary's source
    /// cut is refused, whether the floor carries its own prefix or a local
    /// checkpoint anchors it; one that cannot be anchored is refused too.
    /// Only the shard's own floors count, and an ancestor floor is accepted.
    #[test]
    fn a_member_whose_legacy_floor_is_not_an_ancestor_of_the_source_cut_is_refused() {
        let dir = sinomo_testdir::TestDir::new("conversion-member-legacy-floors");
        let metadata = dir.path().join("metadata");
        let users = metadata.join("users");
        let fs = DfsFileSystemId::generate();
        let shard = DfsMetadataShardId::new("users");
        let head = proof(9, "primary-head");
        let cut = cut(&dir.path().join("stage"), &head);
        let floors = |cut: &MetadataSourceCut| member_floors(&metadata, &fs, &shard, cut);
        let refused = |text: String, refusal: FloorRefusal| {
            assert!(text.contains(&refusal.to_string()), "{text}");
        };

        // An acknowledged prefix at the cut's own height that differs from it.
        write_floor(
            &users,
            "prefix",
            &floor("users", 9, Some(proof(9, "member-fork"))),
        );
        assert!(refusal(floors(&cut)).contains("not proved ancestors of the source cut"));
        // An acknowledged prefix ahead of the cut.
        write_floor(
            &users,
            "prefix",
            &floor("users", 10, Some(proof(10, "member-ahead"))),
        );
        assert!(refusal(floors(&cut)).contains("has no anchor the source cut carries"));

        // A height-only floor is anchored only by a covering local checkpoint.
        std::fs::remove_dir_all(users.join("coordination")).unwrap();
        write_floor(&users, "authority", &floor("users", 7, None));
        refused(
            refusal(floors(&cut)),
            FloorRefusal::NoCoveringCheckpoint {
                journal_sequence: 7,
            },
        );
        write_format3_checkpoint(&users, &proof(6, "member-checkpoint"));
        refused(
            refusal(floors(&cut)),
            FloorRefusal::NoCoveringCheckpoint {
                journal_sequence: 7,
            },
        );
        // A checkpoint at the cut's height whose prefix forks the cut.
        std::fs::remove_dir_all(users.join("records")).unwrap();
        write_format3_checkpoint(&users, &proof(9, "member-checkpoint-fork"));
        assert!(refusal(floors(&cut)).contains("not proved ancestors of the source cut"));

        // The same checkpoint at the cut's exact prefix proves the floor.
        std::fs::remove_dir_all(users.join("records")).unwrap();
        write_format3_checkpoint(&users, &head);
        let accepted = floors(&cut).unwrap();
        assert_eq!(accepted.floors, vec![floor("users", 7, None)]);
        assert_eq!(
            accepted.anchors(),
            vec![SourceCheckpointAnchor {
                shard_id: shard.clone(),
                prefix_proof: head.clone()
            }]
        );
        drop(accepted);

        // A combined root contributes only this shard's floors: its own floor
        // is not reconciled here, but its floor of this shard is.
        let combined = metadata.join("root");
        std::fs::create_dir_all(combined.join("records")).unwrap();
        std::fs::write(
            combined.join("records/original.sobj"),
            b"combined original scope",
        )
        .unwrap();
        write_floor(
            &combined,
            "prefix",
            &floor("root", 40, Some(proof(40, "root-clock"))),
        );
        assert_eq!(floors(&cut).unwrap().floors, vec![floor("users", 7, None)]);
        write_floor(
            &combined,
            "authority",
            &floor("users", 9, Some(proof(9, "combined-fork"))),
        );
        let refused_combined = refusal(floors(&cut));
        assert!(
            refused_combined.contains("not proved ancestors of the source cut"),
            "{refused_combined}"
        );
    }
}
