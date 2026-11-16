//! Complete local source coverage for the explicit standalone owner. This
//! proves namespace completeness, separately from the existing epoch-zero
//! principal certificate. Multiple distinct roots need routing provenance and
//! remain unresolved; identical physical aliases do not create another owner.
use super::*;
use crate::metadata::canonical_generation::CapturedMetadataSource;

const MAX_ROOTS: usize = 64;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Alias {
    path: PathBuf,
    link_target: Option<PathBuf>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Coverage {
    version: u16,
    expected_generation: sinomo_dfs::DfsMetadataGenerationIdentity,
    aliases: Vec<Alias>,
    original_directory: (u64, u64),
    original_fence: (u64, u64),
    owner_file: (u64, u64),
    source_identity: String,
    source_snapshot_fingerprint: String,
    physical_shard: sinomo_dfs::DfsMetadataShardId,
    file_system_ids: Vec<sinomo_dfs::DfsFileSystemId>,
}

fn read_checkpoint<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(path, 64 * 1024 * 1024)
        .map_err(error)?;
    serde_json::from_slice(&bytes).map_err(error)
}

fn held_directory(path: &Path) -> Result<std::fs::File> {
    let mut open = std::fs::OpenOptions::new();
    open.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        open.custom_flags(0x02000000); // FILE_FLAG_BACKUP_SEMANTICS: open directory identity.
    }
    let file = open.open(path).map_err(error)?;
    if !file.metadata().map_err(error)?.is_dir() {
        return Err(error("standalone active source is not a directory"));
    }
    Ok(file)
}

fn only_recorded_child(directory: &Path, expected: &Path) -> Result<()> {
    let mut remaining = MAX_ROOTS;
    for entry in std::fs::read_dir(directory).map_err(error)? {
        if remaining == 0 {
            return Err(error("standalone lineage inventory exceeds its bound"));
        }
        remaining -= 1;
        let entry = entry.map_err(error)?;
        if entry.path() != expected || !entry.file_type().map_err(error)?.is_dir() {
            return Err(error(
                "standalone inventory contains an unqualified staging/archive lineage",
            ));
        }
    }
    Ok(())
}

/// Enumerate the entire declared active directory, retaining actual directory
/// handles during comparison. Reserved paths are excluded only for this exact
/// existing capture/install lineage, never merely because their name is hidden.
fn validate_routing_directory(directory: &Path) -> Result<()> {
    for entry in std::fs::read_dir(directory).map_err(error)? {
        let entry = entry.map_err(error)?;
        if entry.file_name() != "routing-topologies.sobj"
            || !entry.file_type().map_err(error)?.is_file()
        {
            return Err(error(
                "standalone routing cache contains unqualified source data",
            ));
        }
        std::fs::File::open(entry.path()).map_err(error)?;
    }
    Ok(())
}

fn validate_capture_directory(
    evidence: &Path,
    generation: &sinomo_dfs::DfsMetadataGenerationIdentity,
    retained: Option<&Coverage>,
) -> Result<()> {
    let mut budget = 1_000_000usize;
    for entry in std::fs::read_dir(evidence).map_err(error)? {
        if budget == 0 {
            return Err(error(
                "standalone capture artifact inventory exceeds its bound",
            ));
        }
        budget -= 1;
        let entry = entry.map_err(error)?;
        let name = entry.file_name();
        let kind = entry.file_type().map_err(error)?;
        match name.to_str() {
            Some(
                "events.slog"
                | "progress.json"
                | "captured-source-cut.json"
                | "captured-unit.json"
                | "standalone-coverage.json"
                | "source-cut.json"
                | "import-proposal.json"
                | "installation.json",
            ) if kind.is_file() => {
                std::fs::File::open(entry.path()).map_err(error)?;
            }
            Some(
                "journal-evidence"
                | "source-prefix-evidence"
                | "source-evidence"
                | "source-version-evidence",
            ) if kind.is_dir() => {
                for child in std::fs::read_dir(entry.path()).map_err(error)? {
                    if budget == 0 {
                        return Err(error(
                            "standalone source evidence inventory exceeds its bound",
                        ));
                    }
                    budget -= 1;
                    let child = child.map_err(error)?;
                    let path = child.path();
                    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                    let valid = if name == "journal-evidence" || name == "source-prefix-evidence" {
                        stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit())
                    } else {
                        stem.len() == 64
                            && stem
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    };
                    let extension = if name == "journal-evidence" {
                        "sobj"
                    } else {
                        "json"
                    };
                    if !valid
                        || path.extension().and_then(|s| s.to_str()) != Some(extension)
                        || !child.file_type().map_err(error)?.is_file()
                    {
                        return Err(error(
                            "standalone source evidence contains an unknown entry/subtree",
                        ));
                    }
                    std::fs::File::open(path).map_err(error)?;
                }
            }
            Some("namespace-count-current-image") if kind.is_dir() => {
                let candidate = entry.path();
                if !candidate
                    .join("metadata-generation.sobj")
                    .try_exists()
                    .map_err(error)?
                {
                    if retained.is_none_or(|proof| proof.expected_generation != *generation) {
                        return Err(error(
                            "headerless candidate has no original coverage-owned plan",
                        ));
                    }
                    for child in std::fs::read_dir(&candidate).map_err(error)? {
                        let child = child.map_err(error)?;
                        if child.file_name() != "metadata-candidate.lock"
                            || !child.file_type().map_err(error)?.is_file()
                            || child.metadata().map_err(error)?.len() != 0
                        {
                            return Err(error(
                                "headerless candidate contains unqualified retained DATA",
                            ));
                        }
                    }
                    // Exact empty initialization artifacts, before the current
                    // generation header write, belong to this existing plan.
                    continue;
                }
                let header = sinomo_dfs::inspect_metadata_storage_generation(&candidate)?;
                if header.identity != *generation
                    || !matches!(
                        header.state,
                        sinomo_dfs::DfsMetadataGenerationState::Staged { .. }
                    )
                {
                    return Err(error(
                        "standalone candidate changed its original planned generation",
                    ));
                }
                let cut_path = evidence.join("source-cut.json");
                match std::fs::symlink_metadata(&cut_path) {
                    Ok(_) => {
                        let cut: MetadataSourceCut = read_checkpoint(&cut_path)?;
                        if cut.staged_root != entry.path() {
                            return Err(error(
                                "standalone candidate is outside its recorded source cut",
                            ));
                        }
                        let identity = sinomo_dfs::inspect_metadata_candidate(&entry.path())?;
                        if identity.prefix_proof != cut.target_exact_prefix
                            || identity.state_fingerprint != cut.current_image_fingerprint
                        {
                            return Err(error(
                                "standalone staged image differs from its recorded source cut",
                            ));
                        }
                    }
                    Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => {
                        if retained.is_none_or(|proof| proof.expected_generation != *generation) {
                            return Err(error(
                                "pre-cut candidate has no original coverage-owned generation plan",
                            ));
                        }
                        // The original coverage was durable before build. A
                        // completed or partial unactivated candidate resumes
                        // the same canonical builder; it is never a source.
                        if entry
                            .path()
                            .join("records/tables/snapshot.sobj")
                            .try_exists()
                            .map_err(error)?
                        {
                            let actual = sinomo_dfs::inspect_metadata_candidate(&entry.path())?;
                            if let sinomo_dfs::DfsMetadataGenerationState::Staged {
                                seal: Some(seal),
                            } = header.state
                            {
                                if actual.prefix_proof != seal.prefix_proof
                                    || actual.state_fingerprint != seal.state_fingerprint
                                    || actual.file_fingerprint != seal.file_fingerprint
                                {
                                    return Err(error(
                                        "pre-cut candidate differs from its original generation seal",
                                    ));
                                }
                            }
                        }
                    }
                    Err(cause) => return Err(error(cause)),
                }
            }
            _ => {
                return Err(error(
                    "standalone job contains an unknown artifact or nested source root",
                ));
            }
        }
    }
    Ok(())
}

fn active_aliases(
    metadata: &Path,
    logical_source: &Path,
    original_source: &Path,
    evidence: &Path,
    generation: &sinomo_dfs::DfsMetadataGenerationIdentity,
    retained: Option<&Coverage>,
) -> Result<Vec<Alias>> {
    let relocated =
        original_source != logical_source && !logical_source.try_exists().map_err(error)?;
    if relocated && retained.is_none() {
        return Err(error(
            "relocated standalone source has no original coverage checkpoint",
        ));
    }
    let expected_active = if relocated {
        original_source
    } else {
        logical_source
    }
    .canonicalize()
    .map_err(error)?;
    let expected = held_directory(&expected_active)?;
    let expected_id = file_identity(&expected)?;
    let conversion_parent = evidence
        .parent()
        .ok_or_else(|| error("coverage has no original job root"))?;
    if conversion_parent.parent() != Some(metadata.join(".metadata-conversion").as_path()) {
        return Err(error(
            "coverage checkpoint is outside its actual metadata job",
        ));
    }
    let mut aliases = Vec::new();
    let mut handles = Vec::new();
    let mut remaining = MAX_ROOTS;
    for entry in std::fs::read_dir(metadata).map_err(error)? {
        if remaining == 0 {
            return Err(error("standalone active-root inventory exceeds its bound"));
        }
        remaining -= 1;
        let entry = entry.map_err(error)?;
        let path = entry.path();
        let kind = entry.file_type().map_err(error)?;
        match entry.file_name().to_str() {
            Some(".metadata-conversion") if kind.is_dir() => {
                only_recorded_child(&path, conversion_parent)?;
                only_recorded_child(conversion_parent, evidence)?;
                validate_capture_directory(evidence, generation, retained)?;
                continue;
            }
            Some(".metadata-retained") if kind.is_dir() && original_source != logical_source => {
                only_recorded_child(&path, original_source)?;
                continue;
            }
            Some("shard-index")
                if kind.is_dir() && crate::authority::routing_cache::current(metadata)? =>
            {
                validate_routing_directory(&path)?;
                continue;
            }
            _ => {}
        }
        if !kind.is_dir() && !kind.is_symlink() {
            return Err(error(
                "standalone inventory contains an unknown active metadata entry",
            ));
        }
        let link_target = if kind.is_symlink() {
            Some(std::fs::read_link(&path).map_err(error)?)
        } else {
            None
        };
        let alias = Alias {
            path: path.strip_prefix(metadata).map_err(error)?.to_owned(),
            link_target,
        };
        let canonical = match path.canonicalize() {
            Ok(canonical) => canonical,
            Err(cause)
                if relocated
                    && cause.kind() == std::io::ErrorKind::NotFound
                    && retained.is_some_and(|proof| proof.aliases.contains(&alias)) =>
            {
                aliases.push(alias);
                continue;
            }
            Err(cause) => return Err(error(cause)),
        };
        if !canonical.starts_with(metadata) {
            return Err(error(
                "standalone metadata alias escapes its actual owner root",
            ));
        }
        let handle = held_directory(&canonical)?;
        if file_identity(&handle)? != expected_id {
            return Err(error(
                "distinct active standalone roots have no qualified unique canonical-owner provenance",
            ));
        }
        aliases.push(alias);
        handles.push(handle);
    }
    if relocated {
        let original = Alias {
            path: logical_source
                .strip_prefix(metadata)
                .map_err(error)?
                .to_owned(),
            link_target: None,
        };
        if !retained.is_some_and(|proof| proof.aliases.contains(&original)) {
            return Err(error(
                "relocated source was not an original active directory",
            ));
        }
        aliases.push(original);
    }
    aliases.sort_by(|a, b| a.path.cmp(&b.path));
    if aliases.is_empty()
        || !aliases
            .iter()
            .any(|alias| metadata.join(&alias.path) == logical_source)
    {
        return Err(error(
            "complete standalone inventory omits its actual source",
        ));
    }
    if file_identity(&held_directory(&expected_active)?)? != expected_id {
        return Err(error(
            "standalone active directory changed during inventory",
        ));
    }
    Ok(aliases)
}

impl ExplicitStandaloneSourceFactory {
    pub(super) fn coverage_generation_identity(
        &self,
        source: &CapturedMetadataSource,
        binding: String,
    ) -> Result<sinomo_dfs::DfsMetadataGenerationIdentity> {
        let fs = source
            .file_system_ids
            .first()
            .ok_or_else(|| error("standalone source has no filesystem"))?;
        Ok(sinomo_dfs::DfsMetadataGenerationIdentity {
            file_system_id: fs.clone(),
            shard_id: source.physical_shard.clone(),
            generation: format!(
                "standalone:{}",
                digest(&(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, &binding))?
            ),
            origin: sinomo_dfs::DfsMetadataGenerationOrigin::StandaloneOwned {
                owner_node_id: self.node_id.clone(),
                source_binding: binding,
            },
        })
    }
    pub(super) fn require_coverage_for_cut(&self, cut: &MetadataSourceCut) -> Result<()> {
        let checkpoint = cut
            .staged_root
            .parent()
            .ok_or_else(|| error("standalone source cut has no capture owner"))?
            .join("captured-unit.json");
        let capture: crate::metadata::canonical_generation::CapturedMetadataUnit =
            read_checkpoint(&checkpoint)?;
        capture.validate()?;
        if !capture.source.matches_source_cut(cut) {
            return Err(error(
                "standalone publication changed its original captured source",
            ));
        }
        self.require_complete_coverage(&capture.source)
    }
    pub(super) fn require_complete_coverage(&self, source: &CapturedMetadataSource) -> Result<()> {
        if !source.projection_assignments.is_empty() || source.original_handoff.is_some() {
            return Err(error(
                "standalone coverage cannot borrow native routing or handoff authority",
            ));
        }
        let metadata = self
            .data_root
            .join("dfs/metadata")
            .canonicalize()
            .map_err(error)?;
        let logical = source.actual_source_root.clone();
        if logical.parent() != Some(metadata.as_path()) || source.file_system_ids.is_empty() {
            return Err(error(
                "standalone census source is not one declared active physical root",
            ));
        }
        let actual = crate::metadata::resolve_captured_original_source(source)?;
        let source_handle = held_directory(&actual)?;
        let fence = crate::metadata::retain_source_fence(&actual)?;
        let evidence = source
            .file_system_ids
            .iter()
            .map(|fs| StandaloneSourceOwnerEvidence {
                file_system_id: fs.clone(),
                shard_id: source.physical_shard.clone(),
                owner_node_id: self.node_id.clone(),
                owner_epoch: 0,
            })
            .collect::<Vec<_>>();
        // Existing eligibility/actual held source principal checks stay intact.
        let binding = self.binding_for_facts(
            &source.file_system_ids,
            &source.physical_shard,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
            &evidence,
            &actual,
        )?;
        let generation = self.coverage_generation_identity(source, binding)?;
        let owner_path = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
            &self.runtime_data_dir,
            self.component_context.host_role(),
        )
        .data_owner_lock;
        if file_identity(&std::fs::File::open(owner_path).map_err(error)?)?
            != self.owner_file_identity
            || self
                .data_owner
                .storage_owner_file_identity()
                .map_err(error)?
                != self.owner_file_identity
        {
            return Err(error(
                "standalone coverage lost its actual data-owner lease",
            ));
        }
        let checkpoint = source.evidence_root.join("standalone-coverage.json");
        let retained: Option<Coverage> = match std::fs::symlink_metadata(&checkpoint) {
            Ok(_) => Some(read_checkpoint(&checkpoint)?),
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => None,
            Err(cause) => return Err(error(cause)),
        };
        let aliases = active_aliases(
            &metadata,
            &logical,
            &actual,
            &source.evidence_root,
            &generation,
            retained.as_ref(),
        )?;
        let proof = Coverage {
            version: 1,
            expected_generation: generation.clone(),
            aliases: aliases.clone(),
            original_directory: file_identity(&source_handle)?,
            original_fence: file_identity(&fence)?,
            owner_file: self.owner_file_identity,
            source_identity: source.source_identity.clone(),
            source_snapshot_fingerprint: source.source_snapshot_fingerprint.clone(),
            physical_shard: source.physical_shard.clone(),
            file_system_ids: source.file_system_ids.clone(),
        };
        crate::metadata::validate_retained_source_identity(
            &actual,
            &source.source_identity,
            &source.source_snapshot_fingerprint,
        )?;
        if active_aliases(
            &metadata,
            &logical,
            &actual,
            &source.evidence_root,
            &generation,
            retained.as_ref(),
        )? != aliases
        {
            return Err(error(
                "standalone active-root set changed during qualification",
            ));
        }
        crate::metadata::write_count_cohort_checkpoint(
            &source.evidence_root.join("standalone-coverage.json"),
            &proof,
        )
    }
}

#[cfg(test)]
#[path = "coverage_tests.rs"]
mod tests;
