//! Inert installation recovery. Recorded paths identify roles; this helper
//! performs no rename and returns no authority or Ready decision.
use super::{MetadataBackendError, MetadataImportProposal, Result};
use serde::Deserialize;
use sinomo_dfs::DfsMetadataCandidateIdentity;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Installation {
    format_version: u32,
    proposal: MetadataImportProposal,
    source_identity: String,
    source_snapshot_fingerprint: String,
    source_root: PathBuf,
    destination_root: PathBuf,
    candidate_root: PathBuf,
    retained_source_root: PathBuf,
    native_image: DfsMetadataCandidateIdentity,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InstallationPosition {
    SourcePresent,
    SourceArchived,
    CandidatePublished,
}
pub(super) struct InterruptedInstallation {
    pub(super) actual_source: PathBuf,
    pub(super) destination: PathBuf,
    pub(super) candidate: PathBuf,
    pub(super) retained: PathBuf,
    pub(super) source_identity: String,
    pub(super) source_snapshot_fingerprint: String,
    pub(super) image: DfsMetadataCandidateIdentity,
    pub(super) proposal: MetadataImportProposal,
    pub(super) position: InstallationPosition,
    /// Physical location of the exact recorded image after any final rename.
    pub(super) resolved_image_root: PathBuf,
    /// Native request/plan provenance moves with the image, not with proposal.
    pub(super) provenance_plan_root: PathBuf,
}
fn invalid(message: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(message.to_string())
}
fn exists(path: &Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(_) => Err(invalid("recorded installation root is not a directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}
/// `stage` is the actual conversion-stage directory holding installation.json.
/// The caller must reacquire source custody and native certification before
/// completing either unpublished position or accepting a published image.
pub(super) fn resolve_interrupted_installation(
    destination: &Path,
    stage: &Path,
) -> Result<Option<InterruptedInstallation>> {
    let path = stage.join("installation.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(MetadataBackendError::Io { path, source }),
    };
    let record: Installation = serde_json::from_slice(&bytes).map_err(invalid)?;
    if record.format_version != 7
        || record.destination_root != destination
        || record.candidate_root != record.proposal.staged_root
        || record.native_image.state_fingerprint != record.proposal.current_image_fingerprint
        || Some(&record.native_image.prefix_proof) != record.proposal.target_exact_prefix.as_ref()
        || record.candidate_root == record.destination_root
    {
        return Err(invalid(
            "installation record has inconsistent actual paths or image cut",
        ));
    }
    let destination_exists = exists(&record.destination_root)?;
    let retained_exists = exists(&record.retained_source_root)?;
    let source_is_destination = record.source_root == record.destination_root;
    let actual_source = if source_is_destination && retained_exists {
        record.retained_source_root.clone()
    } else if exists(&record.source_root)? {
        record.source_root.clone()
    } else {
        // A retained destination is not evidence of a separate source's role.
        return Err(invalid(
            "recorded installation lost its actual source and archive",
        ));
    };
    let candidate_exists = exists(&record.candidate_root)?;
    let position = if candidate_exists {
        if super::inspect_import_candidate(&record.candidate_root)? != record.native_image {
            return Err(invalid(
                "interrupted installation candidate differs from its recorded image",
            ));
        }
        if source_is_destination && retained_exists {
            if destination_exists {
                return Err(invalid(
                    "installation has both archived source and an unexpected destination before candidate rename",
                ));
            }
            InstallationPosition::SourceArchived
        } else {
            InstallationPosition::SourcePresent
        }
    } else {
        if !destination_exists
            || source_is_destination && !retained_exists
            || super::inspect_import_candidate(&record.destination_root)? != record.native_image
        {
            return Err(invalid(
                "interrupted installation lost its intact candidate or retained source",
            ));
        }
        InstallationPosition::CandidatePublished
    };
    let resolved_image_root = if position == InstallationPosition::CandidatePublished {
        record.destination_root.clone()
    } else {
        record.candidate_root.clone()
    };
    Ok(Some(InterruptedInstallation {
        actual_source,
        destination: record.destination_root,
        candidate: record.candidate_root,
        retained: record.retained_source_root,
        source_identity: record.source_identity,
        source_snapshot_fingerprint: record.source_snapshot_fingerprint,
        image: record.native_image,
        proposal: record.proposal,
        position,
        provenance_plan_root: resolved_image_root.clone(),
        resolved_image_root,
    }))
}
