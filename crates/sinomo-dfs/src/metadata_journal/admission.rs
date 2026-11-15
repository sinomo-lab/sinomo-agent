//! Owner-scoped certification for authored journal admission.
//!
//! The existing shard writer flock serializes all callers. A strong registry
//! keeps uncertainty alive across backend replacement in this process; a new
//! process has no certificate and must explicitly synchronize surviving bytes.
//! This is not a new independent multi-process sequence allocator.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileGeneration {
    path: PathBuf,
    snapshot: Option<JournalSegmentFileSnapshot>,
    #[cfg(unix)]
    changed: (i64, i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Generation {
    #[cfg(unix)]
    root_identity: (u64, u64),
    files: Vec<FileGeneration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Certificate {
    generation: Generation,
    shard_id: DfsMetadataShardId,
    next_sequence: u64,
    /// The exact materialized prefix, when the owning backend has restored it.
    prefix: Option<DfsMetadataPrefixProof>,
}

#[derive(Default)]
struct Admission {
    certificate: Option<Certificate>,
    uncertain: bool,
}

static ADMISSION: OnceLock<Mutex<BTreeMap<PathBuf, Admission>>> = OnceLock::new();

fn owner_key(root: &Path) -> Result<PathBuf> {
    fs::canonicalize(root).map_err(|source| MetadataBackendError::Io {
        path: root.to_path_buf(),
        source,
    })
}

fn generation(root: &Path) -> Result<Generation> {
    #[cfg(unix)]
    let root_identity = {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::metadata(root).map_err(|source| MetadataBackendError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        (metadata.dev(), metadata.ino())
    };
    let mut paths = segmented_journal_paths_descending(root)?
        .into_iter()
        .map(|(_, path)| path)
        .collect::<Vec<_>>();
    paths.push(crate::metadata_table_store::metadata_table_snapshot_path(
        root,
    ));
    // The stable writer-lock inode distinguishes an empty live owner from an
    // unrelated root recreated at the same pathname.
    paths.push(crate::metadata_cursor::journal_write_lock_path(root));
    let mut files = Vec::new();
    for path in paths {
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        };
        if !metadata.is_file()
            && path == crate::metadata_table_store::metadata_table_snapshot_path(root)
        {
            // A directory at an as-yet-unpublished snapshot destination is
            // an output failure, not a replacement authoritative source.
            // If a real snapshot disappeared, generation equality still
            // fails and cold certification reports the read error.
            continue;
        }
        let snapshot = if metadata.is_file() {
            let file = fs::File::open(&path).map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
            Some(
                JournalSegmentFileSnapshot::from_file(&file).map_err(|source| {
                    MetadataBackendError::Io {
                        path: path.clone(),
                        source,
                    }
                })?,
            )
        } else {
            // Preserve the changed pathname in the generation. Cold recovery
            // decides whether a checkpoint has superseded this segment; an
            // uncovered non-file source still fails its writable open.
            None
        };
        files.push(FileGeneration {
            path,
            snapshot,
            #[cfg(unix)]
            changed: {
                use std::os::unix::fs::MetadataExt;
                (metadata.ctime(), metadata.ctime_nsec())
            },
        });
    }
    Ok(Generation {
        #[cfg(unix)]
        root_identity,
        files,
    })
}

fn can_reuse(generation: &Generation) -> bool {
    // Even an empty journal has the stable writer-lock identity. Platforms
    // without stable identities retain explicit certification on each open.
    !generation.files.is_empty()
        && generation.files.iter().all(|file| {
            file.snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.identity.supports_append_extension())
        })
}

/// Observe an existing owner's fully materialized certificate while the
/// canonical writer fence is held. This never certifies, recovers, allocates
/// an admission entry, or clears uncertainty from an arbitrary disk read.
pub(crate) fn current_owned_materialized_prefix(
    root: &Path,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataPrefixProof>> {
    let Some(states) = ADMISSION.get() else {
        return Ok(None);
    };
    let key = owner_key(root)?;
    let current = generation(&key)?;
    let states = states.lock().unwrap_or_else(|error| error.into_inner());
    let Some(state) = states.get(&key) else {
        return Ok(None);
    };
    let Some(certificate) = &state.certificate else {
        return Ok(None);
    };
    if state.uncertain
        || certificate.shard_id != *shard_id
        || certificate.generation != current
        || !can_reuse(&current)
    {
        return Ok(None);
    }
    Ok(certificate
        .prefix
        .as_ref()
        .filter(|prefix| {
            prefix.is_well_formed()
                && prefix.journal_sequence.checked_add(1) == Some(certificate.next_sequence)
        })
        .cloned())
}

/// An existing certificate captured under the canonical writer immediately
/// before a background checkpoint installs its already-validated image. This
/// owns only a cache transition; it grants no author or replica-ingest permit.
#[derive(Debug)]
pub(crate) struct OwnedCheckpointAdmission {
    root: PathBuf,
    certificate: Certificate,
}

pub(crate) fn capture_owned_checkpoint_admission(
    root: &Path,
) -> Result<Option<OwnedCheckpointAdmission>> {
    let Some(states) = ADMISSION.get() else {
        return Ok(None);
    };
    let key = owner_key(root)?;
    let current = generation(&key)?;
    let states = states.lock().unwrap_or_else(|error| error.into_inner());
    let certificate = states
        .get(&key)
        .filter(|state| !state.uncertain)
        .and_then(|state| state.certificate.as_ref())
        .filter(|certificate| {
            certificate.generation == current
                && can_reuse(&current)
                && certificate.prefix.as_ref().is_none_or(|proof| {
                    proof.is_well_formed() && proof.journal_sequence < certificate.next_sequence
                })
        });
    Ok(certificate
        .cloned()
        .map(|certificate| OwnedCheckpointAdmission {
            root: key,
            certificate,
        }))
}

impl OwnedCheckpointAdmission {
    /// Call only after the SAME writer completed snapshot installation and
    /// journal pruning. A lost/changed owner cannot clear admission uncertainty.
    pub(crate) fn record_installed(self, checkpoint: &DfsMetadataPrefixProof) -> Result<()> {
        if !checkpoint.is_well_formed()
            || checkpoint.journal_sequence >= self.certificate.next_sequence
        {
            return Err(MetadataBackendError::replica_diverged(
                "owned checkpoint is ahead of its captured journal certificate".into(),
            ));
        }
        {
            let states = ADMISSION
                .get()
                .expect("captured admission registry")
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let same_owner = states.get(&self.root).is_some_and(|state| {
                !state.uncertain && state.certificate.as_ref() == Some(&self.certificate)
            });
            if !same_owner {
                return Err(MetadataBackendError::replica_diverged(
                    "owned checkpoint lost its captured journal certificate".into(),
                ));
            }
        }
        // Reuse the existing exact-source transition, including retained-file
        // identities, pruned-segment coverage, equal-height conflict rejection,
        // and preservation of a stronger certified deferred journal prefix.
        record_owned_materialization(
            &self.root,
            &self.certificate.shard_id,
            self.certificate.next_sequence,
            Some(checkpoint.clone()),
        )
    }
}

/// Record a successful table append/checkpoint owned by the current shard
/// writer. Unlike an arbitrary disk observation, this transition may change
/// the snapshot and remove whole journal segments covered by that checkpoint.
/// A deferred durable suffix may have a stronger proof than the table cursor;
/// retain it only while its exact retained journal sources remain unchanged.
pub(crate) fn record_owned_materialization(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    prefix: Option<DfsMetadataPrefixProof>,
) -> Result<()> {
    let key = owner_key(root)?;
    let current = generation(&key)?;
    let previous = {
        let states = ADMISSION
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        states
            .get(&key)
            .filter(|state| !state.uncertain)
            .and_then(|state| state.certificate.clone())
    };
    if let Some(previous) = previous
        && previous.shard_id == *shard_id
        && previous.next_sequence == next_sequence
        && owned_sources_unchanged(&key, &previous.generation, &current)?
    {
        if previous
            .prefix
            .as_ref()
            .zip(prefix.as_ref())
            .is_some_and(|(old, new)| old.journal_sequence == new.journal_sequence && old != new)
        {
            return Err(MetadataBackendError::replica_diverged(
                "owned materialization changed an already certified exact prefix".into(),
            ));
        }
        let prefix = match (previous.prefix, prefix) {
            (Some(previous), Some(current))
                if previous.journal_sequence > current.journal_sequence =>
            {
                Some(previous)
            }
            (Some(previous), None) => Some(previous),
            (_, current) => current,
        };
        return record_certified(&key, shard_id, next_sequence, prefix);
    }
    // Cold bootstrap and authorized snapshot restore have no reusable owner
    // certificate. Readability does not certify those bytes: resynchronize the
    // actual checkpoint and journal before clearing admission uncertainty.
    mark_uncertain(&key)?;
    let (certified_next, certified_prefix) = certify_journal_sources(&key, Some(shard_id))?;
    if certified_next != next_sequence {
        return Err(MetadataBackendError::replica_diverged(format!(
            "owned materialization sequence {next_sequence} differs from certified source sequence {certified_next}"
        )));
    }
    record_certified(&key, shard_id, certified_next, certified_prefix)
}

fn owned_sources_unchanged(
    root: &Path,
    previous: &Generation,
    current: &Generation,
) -> Result<bool> {
    #[cfg(unix)]
    if previous.root_identity != current.root_identity {
        return Ok(false);
    }
    if !can_reuse(current) {
        return Ok(false);
    }
    let snapshot_path = crate::metadata_table_store::metadata_table_snapshot_path(root);
    for file in current
        .files
        .iter()
        .filter(|file| file.path != snapshot_path)
    {
        if !previous.files.contains(file) {
            return Ok(false);
        }
    }
    let removed = previous
        .files
        .iter()
        .filter(|file| {
            file.path != snapshot_path && !current.files.iter().any(|now| now.path == file.path)
        })
        .collect::<Vec<_>>();
    if removed.is_empty() {
        return Ok(true);
    }
    let checkpoint =
        crate::metadata_table_store::read_metadata_table_snapshot_for_replica_repair(root)?;
    let Some(proof) = checkpoint
        .prefix_proof
        .filter(|proof| proof.is_well_formed())
    else {
        return Ok(false);
    };
    if checkpoint.materialized_through_sequence != Some(proof.journal_sequence) {
        return Ok(false);
    }
    Ok(removed.iter().all(|file| {
        file.path.parent() == Some(root.join("journal").join("segments").as_path())
            && file
                .path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u64>().ok())
                .is_some_and(|segment| {
                    segment.saturating_mul(JOURNAL_SEGMENT_ENTRY_LIMIT) <= proof.journal_sequence
                })
    }))
}

pub(crate) fn mark_uncertain(root: &Path) -> Result<()> {
    let key = owner_key(root)?;
    let mut states = ADMISSION
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    states.entry(key).or_default().uncertain = true;
    Ok(())
}

/// Forget certification before an authorized replacement/reset changes sources.
pub(crate) fn invalidate(root: &Path) -> Result<()> {
    let key = owner_key(root)?;
    let mut states = ADMISSION
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    states.insert(
        key,
        Admission {
            certificate: None,
            uncertain: true,
        },
    );
    Ok(())
}

pub(crate) fn record_certified(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: u64,
    prefix: Option<DfsMetadataPrefixProof>,
) -> Result<()> {
    let key = owner_key(root)?;
    let generation = generation(&key)?;
    if prefix
        .as_ref()
        .is_some_and(|proof| !proof.is_well_formed() || proof.journal_sequence >= next_sequence)
    {
        return Err(MetadataBackendError::Serde(
            "journal admission certificate has an invalid exact prefix".into(),
        ));
    }
    let mut states = ADMISSION
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    states.insert(
        key,
        Admission {
            certificate: Some(Certificate {
                generation,
                shard_id: shard_id.clone(),
                next_sequence,
                prefix,
            }),
            uncertain: false,
        },
    );
    Ok(())
}

/// Called with the canonical shard writer lock, before any mutation or no-op
/// result can consume a resident table view. Reads alone cannot clear a fence.
pub(crate) fn certify_for_write(root: &Path, shard_id: &DfsMetadataShardId) -> Result<u64> {
    let key = owner_key(root)?;
    let current = generation(&key)?;
    {
        let states = ADMISSION
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(state) = states.get(&key)
            && !state.uncertain
            && let Some(certificate) = &state.certificate
            && certificate.shard_id == *shard_id
            && certificate.generation == current
            && can_reuse(&current)
            && certificate.prefix.as_ref().is_none_or(|proof| {
                proof.is_well_formed() && proof.journal_sequence < certificate.next_sequence
            })
        {
            return Ok(certificate.next_sequence);
        }
    }
    mark_uncertain(&key)?;
    let (next_sequence, prefix) = certify_journal_sources(&key, Some(shard_id))?;
    record_certified(&key, shard_id, next_sequence, prefix)?;
    Ok(next_sequence)
}

pub(crate) fn validate_materialized_prefix(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    prefix: Option<&DfsMetadataPrefixProof>,
) -> Result<()> {
    let next_sequence = certify_for_write(root, shard_id)?;
    let key = owner_key(root)?;
    let states = ADMISSION
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let certificate = states
        .get(&key)
        .and_then(|state| state.certificate.as_ref())
        .ok_or_else(|| MetadataBackendError::Serde("missing journal admission proof".into()))?;
    if let Some(expected) = certificate.prefix.as_ref()
        && expected.journal_sequence == next_sequence.saturating_sub(1)
    {
        if prefix != Some(expected) {
            return Err(MetadataBackendError::replica_diverged(
                "materialized metadata prefix differs from the exact certified journal/checkpoint prefix".into(),
            ));
        }
    } else if prefix.is_some_and(|proof| proof.journal_sequence >= next_sequence) {
        return Err(MetadataBackendError::replica_diverged(
            "materialized metadata prefix is ahead of certified journal/checkpoint sources".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn forget_for_test(root: &Path) {
    if let Ok(key) = owner_key(root)
        && let Some(states) = ADMISSION.get()
    {
        states
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_owner(label: &str) -> sinomo_testdir::TestDir {
        let root = sinomo_testdir::TestDir::new(label);
        fs::create_dir_all(root.path().join("journal")).unwrap();
        fs::File::create(crate::metadata_cursor::journal_write_lock_path(root.path())).unwrap();
        root
    }

    #[test]
    fn empty_certified_owner_reuses_stable_write_lock_identity() {
        let root = empty_owner("journal-admission-empty-owner");
        let shard = DfsMetadataShardId::new("services");
        assert_eq!(certify_for_write(root.path(), &shard).unwrap(), 1);
        let before = metadata_journal_tail_recovery_calls();
        assert_eq!(certify_for_write(root.path(), &shard).unwrap(), 1);
        assert_eq!(metadata_journal_tail_recovery_calls(), before);
    }

    #[test]
    fn captured_checkpoint_cannot_clear_later_uncertainty() {
        let root = empty_owner("journal-checkpoint-lost-certificate");
        let shard = DfsMetadataShardId::new("services");
        assert_eq!(certify_for_write(root.path(), &shard).unwrap(), 1);
        let captured = capture_owned_checkpoint_admission(root.path())
            .unwrap()
            .unwrap();
        mark_uncertain(root.path()).unwrap();
        let genesis = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard);
        assert!(captured.record_installed(&genesis).is_err());
        assert!(
            capture_owned_checkpoint_admission(root.path())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn owned_checkpoint_keeps_stronger_certified_deferred_prefix() {
        let root = sinomo_testdir::TestDir::new("journal-admission-owned-deferred-prefix");
        let shard = DfsMetadataShardId::new("services");
        crate::metadata_candidate::stage_current_test_generation(root.path(), &shard.0);
        fs::create_dir_all(root.path().join("journal")).unwrap();
        fs::File::create(crate::metadata_cursor::journal_write_lock_path(root.path())).unwrap();
        let mut prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard);
        let mut proofs = Vec::new();
        for sequence in 1..=2 {
            let entry = super::super::tests::test_journal_entry(sequence);
            let frame = DfsMetadataJournalFrame::encode(&entry).unwrap();
            append_segmented_journal_frame_with_record_bytes(root.path(), &frame).unwrap();
            prefix = frame.prefix_proof_after(&prefix).unwrap();
            proofs.push(prefix.clone());
        }
        let [prefix_one, prefix_two] = proofs.as_slice() else {
            unreachable!()
        };
        record_certified(root.path(), &shard, 3, Some(prefix_two.clone())).unwrap();
        crate::metadata_table_store::persist_metadata_table_snapshot(
            root.path(),
            &crate::metadata_tables::DfsMetadataTables::default(),
            Some(1),
            Some(prefix_one),
        )
        .unwrap();
        record_owned_materialization(root.path(), &shard, 3, Some(prefix_one.clone())).unwrap();
        validate_materialized_prefix(root.path(), &shard, Some(prefix_two)).unwrap();
        assert!(validate_materialized_prefix(root.path(), &shard, Some(prefix_one)).is_err());
    }

    #[test]
    fn owned_materialization_cannot_change_equal_height_certified_prefix() {
        let root = empty_owner("journal-admission-owned-conflicting-prefix");
        let shard = DfsMetadataShardId::new("services");
        let genesis = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard);
        let entry = super::super::tests::test_journal_entry(1);
        let original = DfsMetadataJournalFrame::encode(&entry)
            .unwrap()
            .prefix_proof_after(&genesis)
            .unwrap();
        let mut alternate = entry.clone();
        alternate.created_at += time::Duration::seconds(1);
        let conflicting = DfsMetadataJournalFrame::encode(&alternate)
            .unwrap()
            .prefix_proof_after(&genesis)
            .unwrap();
        record_certified(root.path(), &shard, 2, Some(original.clone())).unwrap();
        assert!(record_owned_materialization(root.path(), &shard, 2, Some(conflicting)).is_err());
        validate_materialized_prefix(root.path(), &shard, Some(&original)).unwrap();
    }
}
