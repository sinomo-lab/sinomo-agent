mod local;
mod overlay;
mod router;
mod types;

pub use local::LocalSourceConnector;
pub use types::*;

use router::{
    ALL_PARTITIONS_TERM, INDEXED_SCHEMA_VERSION, LexicalRouterManifest, RouterKey,
    RouterMutationBatch, RouterPosting, RouterScanWatermark, RouterTreeStore, router_shard_key,
};

use crate::{MemoryError, MemoryQuery, decode_sobj, encode_sobj};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::Id;
use sinomo_runtime::storage::durable_file::{
    DurableAtomicFileOptions, create_private_staged_file, install_staged_file, sync_directory,
    write_file_atomic, write_file_create_new_atomic,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use time::OffsetDateTime;

const SOURCE_SCAN_PAGE_SIZE: usize = 64;
const MAX_REPORTED_GENERATION_IDS: usize = 256;
const MAX_CATALOG_PAGE_SIZE: usize = 256;
const MAX_TRACE_PAGE_SIZE: usize = 256;
const MAX_TRACE_RECORD_BYTES: usize = 4 * 1024 * 1024;
const CONTINUATION_KEY_BYTES: usize = 32;
const MAX_PENDING_ROUTER_UPDATES: usize = 1_024;
const MAX_ROUTER_QUERY_INSPECTED: usize = 4_096;

struct RoutedPartitionsPage {
    items: Vec<(IndexPartitionId, IndexGenerationId)>,
    has_more_routed: bool,
    scan_resume: Option<RouterScanWatermark>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RankedEvidenceCandidate {
    partition_id: IndexPartitionId,
    generation_id: IndexGenerationId,
    anchor_index: usize,
    score: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RankedEvidenceCursor {
    partition_id: IndexPartitionId,
    generation_id: IndexGenerationId,
    anchor_index: usize,
    score: i32,
}

#[derive(Clone, Debug)]
pub struct IndexedFileMemoryStore {
    root: PathBuf,
    connector: LocalSourceConnector,
    write_lock: Arc<Mutex<()>>,
    trace_lock: Arc<Mutex<()>>,
}

struct IndexedWriteGuard<'a> {
    _process_guard: MutexGuard<'a, ()>,
    file: fs::File,
}

impl Drop for IndexedWriteGuard<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl IndexedFileMemoryStore {
    pub fn new(memory_root: impl Into<PathBuf>) -> Self {
        Self {
            root: memory_root.into(),
            connector: LocalSourceConnector,
            write_lock: Arc::new(Mutex::new(())),
            trace_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn from_data_dir(data_dir: &sinomo_runtime::local::SinomoDataDir) -> Self {
        Self::new(data_dir.root().join("memory"))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn has_registered_roots(&self) -> Result<bool, MemoryError> {
        match fs::read_dir(self.roots_dir()) {
            Ok(mut entries) => Ok(entries
                .next()
                .transpose()
                .map_err(|source| MemoryError::Io {
                    path: self.roots_dir().display().to_string(),
                    source,
                })?
                .is_some()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(MemoryError::Io {
                path: self.roots_dir().display().to_string(),
                source,
            }),
        }
    }

    pub fn register_local_root(
        &self,
        mut config: LocalIndexRootConfig,
    ) -> Result<IndexRoot, MemoryError> {
        let _guard = self.write_guard()?;
        let canonical = fs::canonicalize(&config.path).map_err(|source| MemoryError::Io {
            path: config.path.display().to_string(),
            source,
        })?;
        if !canonical.is_dir() {
            return Err(MemoryError::InvalidIndexRoot {
                path: canonical.display().to_string(),
                detail: "local index root is not a directory".to_string(),
            });
        }
        fs::create_dir_all(&self.root).map_err(|source| MemoryError::Io {
            path: self.root.display().to_string(),
            source,
        })?;
        let memory_root = fs::canonicalize(&self.root).map_err(|source| MemoryError::Io {
            path: self.root.display().to_string(),
            source,
        })?;
        if canonical.starts_with(&memory_root) || memory_root.starts_with(&canonical) {
            return Err(MemoryError::InvalidIndexRoot {
                path: canonical.display().to_string(),
                detail: "source root overlaps the derived memory store".to_string(),
            });
        }
        self.ensure_continuation_key()?;

        config.path = canonical;
        config.include_extensions = config
            .include_extensions
            .into_iter()
            .map(|extension| extension.trim_start_matches('.').to_ascii_lowercase())
            .filter(|extension| !extension.is_empty())
            .collect();
        config.include_extensions.sort();
        config.include_extensions.dedup();
        if config.include_extensions.is_empty() {
            return Err(MemoryError::InvalidIndexRoot {
                path: config.path.display().to_string(),
                detail: "at least one file extension must be enabled".to_string(),
            });
        }

        let root_id = Id::derived(
            "memory-index-root",
            config.path.to_string_lossy().as_bytes(),
        );
        let path = self.root_path(&root_id);
        let existing = self.read_optional::<IndexRoot>(&path)?;
        if let Some(existing) = &existing {
            self.fence_root_config_change(existing, &config)?;
        }
        let authorization_epoch = existing
            .as_ref()
            .filter(|existing| existing.config == config)
            .map(|existing| existing.authorization_epoch.clone())
            .unwrap_or_else(Id::generate);
        let registered_at = existing
            .as_ref()
            .map(|existing| existing.registered_at)
            .unwrap_or_else(OffsetDateTime::now_utc);
        let root = IndexRoot {
            root_id,
            connector_id: "sinomo.memory.local/v1".to_string(),
            config,
            authorization_epoch,
            retention: SourceRetentionCapability::CurrentOnly,
            registered_at,
        };
        self.write_sobj_atomic(&path, &root)?;
        Ok(root)
    }

    /// Remove any route whose previously published authorization or resource
    /// envelope is no longer valid before the new root config becomes visible.
    /// A later explicit index pass publishes generations under the new policy.
    fn fence_root_config_change(
        &self,
        existing: &IndexRoot,
        replacement: &LocalIndexRootConfig,
    ) -> Result<(), MemoryError> {
        if existing.config == *replacement {
            return Ok(());
        }
        let policy_changed = existing.config.policy != replacement.policy;
        let mut after = None;
        loop {
            let page =
                self.source_page(&existing.root_id, after.as_ref(), SOURCE_SCAN_PAGE_SIZE)?;
            for source in &page.items {
                if source.freshness == SourceFreshness::Deleted {
                    continue;
                }
                let extension_allowed = Path::new(&source.version.file_ref.path_hint)
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| {
                        replacement
                            .include_extensions
                            .iter()
                            .any(|candidate| candidate.eq_ignore_ascii_case(extension))
                    });
                if policy_changed
                    || !extension_allowed
                    || source.version.byte_length > replacement.max_source_bytes
                {
                    self.unroute_partition(&Id::derived(
                        "memory-index-partition",
                        source.version.file_ref.object_id.as_str(),
                    ))?;
                }
            }
            after = page.continuation;
            if after.is_none() {
                break;
            }
        }
        Ok(())
    }

    pub fn root_by_id(&self, root_id: &IndexRootId) -> Result<IndexRoot, MemoryError> {
        self.read_required(&self.root_path(root_id), "index root")
    }

    pub fn scan_local_root_page(
        &self,
        root_id: &IndexRootId,
        cursor: Option<&LocalScanCursor>,
        limit: usize,
    ) -> Result<SourcePage, MemoryError> {
        let root = self.root_by_id(root_id)?;
        self.connector.scan_page(&root, cursor, limit)
    }

    pub fn index_root(&self, root_id: &IndexRootId) -> Result<IndexMaintenanceReport, MemoryError> {
        let _guard = self.write_guard()?;
        let root = self.root_by_id(root_id)?;
        let mut report = IndexMaintenanceReport::default();
        let mut cursor = None;
        loop {
            let page = self
                .connector
                .scan_page(&root, cursor.as_ref(), SOURCE_SCAN_PAGE_SIZE)?;
            for source in page.sources {
                report.discovered_files += 1;
                let existing = self.read_optional::<SourceDescriptor>(
                    &self.source_path(&source.file_ref.root_id, &source.file_ref.object_id),
                )?;
                let metadata_unchanged = existing.as_ref().is_some_and(|existing| {
                    existing.freshness == SourceFreshness::Current
                        && existing.source_version_hint == source.source_version_hint
                        && existing.version.file_ref.path_hint == source.file_ref.path_hint
                        && existing.policy == root.config.policy
                        && existing.retention == root.retention
                });
                if metadata_unchanged {
                    let snapshot = self.connector.acquire_snapshot(&root, &source)?;
                    if existing.as_ref().is_some_and(|existing| {
                        existing.version.content_hash == snapshot.version.content_hash
                    }) {
                        report.unchanged_files += 1;
                        continue;
                    }
                    let generation_id =
                        self.publish_verified_snapshot_locked(&root, &source, snapshot)?;
                    report.published_files += 1;
                    record_published_generation(&mut report, generation_id);
                    continue;
                }
                let generation_id = self.publish_candidate_locked(&root, &source)?;
                report.published_files += 1;
                record_published_generation(&mut report, generation_id);
            }
            cursor = page.continuation;
            if cursor.is_none() {
                break;
            }
        }

        let mut after = None;
        loop {
            let page = self.source_page(&root.root_id, after.as_ref(), SOURCE_SCAN_PAGE_SIZE)?;
            for mut source in page.items {
                if source.freshness != SourceFreshness::Deleted
                    && !local::descriptor_identity_is_current(&root, &source)
                {
                    self.unroute_partition(&Id::derived(
                        "memory-index-partition",
                        source.version.file_ref.object_id.as_str(),
                    ))?;
                    source.freshness = SourceFreshness::Deleted;
                    source.observed_at = OffsetDateTime::now_utc();
                    self.write_sobj_atomic(
                        &self.source_path(&root.root_id, &source.version.file_ref.object_id),
                        &source,
                    )?;
                    report.tombstoned_files += 1;
                }
            }
            after = page.continuation;
            if after.is_none() {
                break;
            }
        }
        Ok(report)
    }

    pub fn publish_source_candidate(
        &self,
        source: &SourceCandidate,
    ) -> Result<IndexGenerationId, MemoryError> {
        let _guard = self.write_guard()?;
        let root = self.root_by_id(&source.file_ref.root_id)?;
        self.publish_candidate_locked(&root, source)
    }

    fn publish_candidate_locked(
        &self,
        root: &IndexRoot,
        source: &SourceCandidate,
    ) -> Result<IndexGenerationId, MemoryError> {
        let snapshot = self.connector.acquire_snapshot(root, source)?;
        self.publish_verified_snapshot_locked(root, source, snapshot)
    }

    fn publish_verified_snapshot_locked(
        &self,
        root: &IndexRoot,
        source: &SourceCandidate,
        snapshot: VerifiedSourceSnapshot,
    ) -> Result<IndexGenerationId, MemoryError> {
        let text = std::str::from_utf8(&snapshot.bytes).map_err(|source_error| {
            MemoryError::InvalidSourceContent {
                path: source.absolute_path.display().to_string(),
                detail: source_error.to_string(),
            }
        })?;
        let pipeline = PipelineFingerprint::builtin_text();
        let anchors = extract_text_anchors(&snapshot.version, &source.media_type, text)?;
        let partition_id =
            Id::derived("memory-index-partition", source.file_ref.object_id.as_str());
        let descriptor = SourceDescriptor {
            version: snapshot.version,
            media_type: source.media_type.clone(),
            policy: root.config.policy.clone(),
            retention: root.retention.clone(),
            freshness: SourceFreshness::Current,
            platform_identity: source.platform_identity.clone(),
            source_version_hint: source.source_version_hint.clone(),
            observed_at: OffsetDateTime::now_utc(),
        };
        let generation_fingerprint = serde_json::to_vec(&(
            INDEXED_SCHEMA_VERSION,
            &partition_id,
            &descriptor.version,
            &descriptor.media_type,
            &descriptor.policy,
            &descriptor.retention,
            &descriptor.platform_identity,
            &descriptor.source_version_hint,
            &root.authorization_epoch,
            &pipeline,
            &anchors,
        ))
        .map_err(|source_error| MemoryError::SobjSerialize {
            path: source.absolute_path.display().to_string(),
            detail: source_error.to_string(),
        })?;
        let generation_id = Id::derived("memory-index-generation", generation_fingerprint);
        let generation = LexicalPartitionGeneration {
            manifest: IndexGenerationManifest {
                generation_id: generation_id.clone(),
                partition_id: partition_id.clone(),
                source_version_id: descriptor.version.version_id.clone(),
                authorization_epoch: root.authorization_epoch.clone(),
                pipeline_id: pipeline.id(),
                anchor_count: anchors.len(),
                fact_count: 0,
                published_at: OffsetDateTime::now_utc(),
            },
            source: descriptor.clone(),
            anchors,
            facts: Vec::new(),
            embedded_content: None,
        };

        let old_generation = self.current_partition_generation(&partition_id)?;
        self.validate_existing_generation(&generation)?;
        self.write_sobj_immutable(
            &self.partition_generation_path(&partition_id, &generation_id),
            &generation,
        )?;
        let pending = PendingRouterUpdate::new(
            partition_id.clone(),
            old_generation
                .as_ref()
                .map(|generation| generation.manifest.generation_id.clone()),
            Some(generation_id.clone()),
        );
        self.write_sobj_atomic(&self.pending_router_update_path(&partition_id), &pending)?;
        self.commit_pending_router_update_locked(&pending)?;
        Ok(generation_id)
    }

    fn validate_existing_generation(
        &self,
        generation: &LexicalPartitionGeneration,
    ) -> Result<(), MemoryError> {
        let path = self.partition_generation_path(
            &generation.manifest.partition_id,
            &generation.manifest.generation_id,
        );
        let Some(existing) = self.read_optional::<LexicalPartitionGeneration>(&path)? else {
            return Ok(());
        };
        let same_payload = existing.manifest.generation_id == generation.manifest.generation_id
            && existing.manifest.partition_id == generation.manifest.partition_id
            && existing.manifest.source_version_id == generation.manifest.source_version_id
            && existing.manifest.authorization_epoch == generation.manifest.authorization_epoch
            && existing.manifest.pipeline_id == generation.manifest.pipeline_id
            && existing.manifest.anchor_count == generation.manifest.anchor_count
            && existing.manifest.fact_count == generation.manifest.fact_count
            && existing.source.version == generation.source.version
            && existing.source.media_type == generation.source.media_type
            && existing.source.policy == generation.source.policy
            && existing.source.retention == generation.source.retention
            && existing.source.freshness == generation.source.freshness
            && existing.source.platform_identity == generation.source.platform_identity
            && existing.source.source_version_hint == generation.source.source_version_hint
            && existing.anchors == generation.anchors
            && existing.facts == generation.facts
            && existing.embedded_content == generation.embedded_content;
        if same_payload {
            Ok(())
        } else {
            Err(MemoryError::ImmutableGenerationCollision {
                generation_id: generation.manifest.generation_id.clone(),
                path: path.display().to_string(),
            })
        }
    }

    fn current_partition_generation(
        &self,
        partition_id: &IndexPartitionId,
    ) -> Result<Option<LexicalPartitionGeneration>, MemoryError> {
        let Some(manifest) =
            self.read_optional::<PartitionManifest>(&self.partition_manifest_path(partition_id))?
        else {
            return Ok(None);
        };
        self.read_optional(
            &self.partition_generation_path(partition_id, &manifest.current_generation_id),
        )
    }

    fn unroute_partition(&self, partition_id: &IndexPartitionId) -> Result<(), MemoryError> {
        let old = self.current_partition_generation(partition_id)?;
        if old.is_some() {
            let pending = PendingRouterUpdate::new(
                partition_id.clone(),
                old.as_ref()
                    .map(|generation| generation.manifest.generation_id.clone()),
                None,
            );
            self.write_sobj_atomic(&self.pending_router_update_path(partition_id), &pending)?;
            self.commit_pending_router_update_locked(&pending)?;
        }
        Ok(())
    }

    fn commit_pending_router_update_locked(
        &self,
        pending: &PendingRouterUpdate,
    ) -> Result<(), MemoryError> {
        pending.validate(&self.pending_router_update_path(&pending.partition_id))?;
        let old = pending
            .old_generation_id
            .as_ref()
            .map(|generation_id| {
                self.read_required::<LexicalPartitionGeneration>(
                    &self.partition_generation_path(&pending.partition_id, generation_id),
                    "pending old partition generation",
                )
            })
            .transpose()?;
        let new = pending
            .new_generation_id
            .as_ref()
            .map(|generation_id| {
                self.read_required::<LexicalPartitionGeneration>(
                    &self.partition_generation_path(&pending.partition_id, generation_id),
                    "pending new partition generation",
                )
            })
            .transpose()?;
        for generation in old.iter().chain(new.iter()) {
            if generation.manifest.partition_id != pending.partition_id {
                return Err(MemoryError::SobjDeserialize {
                    path: self
                        .pending_router_update_path(&pending.partition_id)
                        .display()
                        .to_string(),
                    detail: "pending router update references a generation from another partition"
                        .to_string(),
                });
            }
        }

        // Publish every object needed to interpret and later revoke the route
        // before atomically advancing the top router pointer.
        if let Some(generation) = &new {
            self.write_sobj_atomic(
                &self.partition_manifest_path(&pending.partition_id),
                &PartitionManifest {
                    partition_id: pending.partition_id.clone(),
                    current_generation_id: generation.manifest.generation_id.clone(),
                    published_at: generation.manifest.published_at,
                },
            )?;
            self.write_sobj_atomic(
                &self.source_path(
                    &generation.source.version.file_ref.root_id,
                    &generation.source.version.file_ref.object_id,
                ),
                &generation.source,
            )?;
        }
        self.update_router(&pending.partition_id, old.as_ref(), new.as_ref())?;
        let path = self.pending_router_update_path(&pending.partition_id);
        match fs::remove_file(&path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    sync_directory(parent).map_err(|source| MemoryError::DurableFile {
                        path: parent.display().to_string(),
                        detail: source.to_string(),
                    })?;
                }
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(MemoryError::Io {
                    path: path.display().to_string(),
                    source,
                });
            }
        }
        Ok(())
    }

    fn recover_pending_router_updates_locked(&self) -> Result<(), MemoryError> {
        let (ids, has_more) = bounded_ids(
            &self.pending_router_updates_dir(),
            None,
            MAX_PENDING_ROUTER_UPDATES,
        )?;
        if has_more {
            return Err(MemoryError::DurableFile {
                path: self.pending_router_updates_dir().display().to_string(),
                detail: format!(
                    "more than {MAX_PENDING_ROUTER_UPDATES} pending router updates require recovery"
                ),
            });
        }
        for partition_id in ids {
            let path = self.pending_router_update_path(&partition_id);
            let pending: PendingRouterUpdate =
                self.read_required(&path, "pending router update")?;
            if pending.partition_id != partition_id {
                return Err(MemoryError::SobjDeserialize {
                    path: path.display().to_string(),
                    detail: format!(
                        "pending update filename partition {partition_id} does not match payload {}",
                        pending.partition_id
                    ),
                });
            }
            self.commit_pending_router_update_locked(&pending)?;
        }
        Ok(())
    }

    fn recover_pending_router_updates_if_needed(&self) -> Result<(), MemoryError> {
        let pending = match fs::read_dir(self.pending_router_updates_dir()) {
            Ok(mut entries) => entries
                .next()
                .transpose()
                .map_err(|source| MemoryError::Io {
                    path: self.pending_router_updates_dir().display().to_string(),
                    source,
                })?,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(MemoryError::Io {
                    path: self.pending_router_updates_dir().display().to_string(),
                    source,
                });
            }
        };
        if pending.is_some() {
            let _guard = self.write_guard()?;
        }
        Ok(())
    }

    fn update_router(
        &self,
        partition_id: &IndexPartitionId,
        old: Option<&LexicalPartitionGeneration>,
        new: Option<&LexicalPartitionGeneration>,
    ) -> Result<(), MemoryError> {
        let mut manifest = self.load_router_manifest()?;
        let old_terms = generation_terms(old);
        let new_terms = generation_terms(new);
        let mut batches = BTreeMap::<String, RouterMutationBatch>::new();
        for term in &old_terms {
            batches
                .entry(router_shard_key(term))
                .or_default()
                .insert(RouterKey::new(term, partition_id.clone()), None);
        }
        if let Some(generation) = new {
            let posting = RouterPosting {
                partition_id: partition_id.clone(),
                generation_id: generation.manifest.generation_id.clone(),
                root_id: generation.source.version.file_ref.root_id.clone(),
                authorization_epoch: generation.manifest.authorization_epoch.clone(),
                policy: generation.source.policy.clone(),
            };
            for term in &new_terms {
                batches.entry(router_shard_key(term)).or_default().insert(
                    RouterKey::new(term, partition_id.clone()),
                    Some(posting.clone()),
                );
            }
        }

        let tree = self.router_tree_store();
        for (shard_key, mutations) in batches {
            let updated = tree.apply(&shard_key, manifest.shards.get(&shard_key), &mutations)?;
            match updated {
                Some(root) => {
                    manifest.shards.insert(shard_key, root);
                }
                None => {
                    manifest.shards.remove(&shard_key);
                }
            }
        }

        manifest.refresh_id();
        manifest.published_at = OffsetDateTime::now_utc();
        self.write_sobj_immutable(
            &self.router_manifest_generation_path(&manifest.manifest_id),
            &manifest,
        )?;
        self.write_sobj_atomic(&self.router_manifest_path(), &manifest)
    }

    fn load_router_manifest(&self) -> Result<LexicalRouterManifest, MemoryError> {
        let path = self.router_manifest_path();
        match self.read_optional(&path)? {
            Some(manifest) => LexicalRouterManifest::validate(manifest, &path),
            None => Ok(LexicalRouterManifest::empty()),
        }
    }

    fn load_router_manifest_for_query(
        &self,
        continuation: Option<&MemoryQueryContinuation>,
    ) -> Result<LexicalRouterManifest, MemoryError> {
        match continuation {
            Some(continuation) => {
                let path = self.router_manifest_generation_path(&continuation.router_manifest_id);
                let manifest = self.read_required(&path, "pinned router manifest generation")?;
                LexicalRouterManifest::validate(manifest, &path)
            }
            None => self.load_router_manifest(),
        }
    }

    fn route_partitions(
        &self,
        manifest: &LexicalRouterManifest,
        access: &MemoryAccessContext,
        terms: &[String],
        start_partition_id: Option<&IndexPartitionId>,
        start_exclusive: bool,
        limit: usize,
    ) -> Result<RoutedPartitionsPage, MemoryError> {
        let retained_limit = limit.min(MAX_MEMORY_RETRIEVAL_PARTITIONS).saturating_add(1);
        let mut routed = BTreeMap::<IndexPartitionId, IndexGenerationId>::new();
        let mut root_authorizations = BTreeMap::<IndexRootId, Option<(Id, IndexRootPolicy)>>::new();
        let mut discarded_larger_partition = false;
        let mut scan_resume = None::<RouterScanWatermark>;
        let tree = self.router_tree_store();
        let active_term_count = terms
            .iter()
            .filter(|term| manifest.shards.contains_key(&router_shard_key(term)))
            .count();
        let per_term_work = MAX_ROUTER_QUERY_INSPECTED / active_term_count.max(1);
        let extra_work = MAX_ROUTER_QUERY_INSPECTED % active_term_count.max(1);
        let mut active_term_index = 0usize;
        for term in terms {
            let shard_key = router_shard_key(term);
            let Some(root) = manifest.shards.get(&shard_key) else {
                continue;
            };
            let max_inspected = per_term_work
                .saturating_add(usize::from(active_term_index < extra_work))
                .max(1);
            active_term_index = active_term_index.saturating_add(1);
            let scan = tree.scan_term(
                &shard_key,
                root,
                term,
                start_partition_id,
                start_exclusive,
                retained_limit,
                max_inspected,
                |posting| {
                    if !root_authorizations.contains_key(&posting.root_id) {
                        let current = self
                            .read_optional::<IndexRoot>(&self.root_path(&posting.root_id))?
                            .map(|root| (root.authorization_epoch, root.config.policy));
                        root_authorizations.insert(posting.root_id.clone(), current);
                    }
                    let Some((current_epoch, current_policy)) = root_authorizations
                        .get(&posting.root_id)
                        .and_then(Option::as_ref)
                    else {
                        return Ok(false);
                    };
                    Ok(current_epoch == &posting.authorization_epoch
                        && current_policy == &posting.policy
                        && access.permits(current_policy))
                },
            )?;
            if !scan.exhausted {
                let watermark = scan.last_inspected.ok_or_else(|| {
                    MemoryError::InvalidMemoryContinuation {
                        detail: format!(
                            "bounded router scan for term {term:?} stopped without a resume watermark"
                        ),
                    }
                })?;
                if scan_resume
                    .as_ref()
                    .is_none_or(|current| watermark.partition_id < current.partition_id)
                {
                    scan_resume = Some(watermark);
                }
            }
            for posting in scan.eligible_postings {
                if routed
                    .get(&posting.partition_id)
                    .is_some_and(|generation_id| generation_id != &posting.generation_id)
                {
                    return Err(MemoryError::SobjDeserialize {
                        path: self.router_manifest_path().display().to_string(),
                        detail: format!(
                            "pinned router manifest maps partition {} to inconsistent generations",
                            posting.partition_id
                        ),
                    });
                }
                routed.insert(posting.partition_id, posting.generation_id);
                if routed.len() > retained_limit {
                    routed.pop_last();
                    discarded_larger_partition = true;
                }
            }
        }
        // A multi-term query may advance different term scans by different
        // amounts. Only expose candidates through the minimum fully inspected
        // partition frontier; later candidates are deterministically rediscovered
        // after the authenticated exclusive watermark.
        if let Some(watermark) = &scan_resume {
            routed.retain(|partition_id, _| partition_id <= &watermark.partition_id);
        }
        Ok(RoutedPartitionsPage {
            has_more_routed: discarded_larger_partition || routed.len() > limit,
            items: routed.into_iter().collect(),
            scan_resume,
        })
    }

    pub fn retrieve(
        &self,
        access: &MemoryAccessContext,
        query: &MemoryQuery,
    ) -> Result<MemoryEvidenceBundle, MemoryError> {
        self.recover_pending_router_updates_if_needed()?;
        if let Some(continuation) = &query.continuation {
            self.validate_query_continuation(continuation)?;
        }
        let router = self.load_router_manifest_for_query(query.continuation.as_ref())?;
        let (terms, query_terms_truncated) =
            tokenize_query(query.task_hint.as_deref().unwrap_or_default());
        let query_fingerprint = query_fingerprint(access, query)?;
        let (start_partition_id, start_exclusive) = match &query.continuation {
            Some(continuation) => {
                if continuation.router_manifest_id != router.manifest_id
                    || continuation.query_fingerprint != query_fingerprint
                {
                    return Err(MemoryError::InvalidMemoryContinuation {
                        detail: "continuation does not match the pinned router snapshot or query"
                            .to_string(),
                    });
                }
                if continuation.ranked_evidence {
                    // Evidence continuations identify the next item in the
                    // ranked page. Rebuild the same bounded routing window so
                    // relevance ordering remains global within that window.
                    (
                        continuation.window_start_partition_id.clone(),
                        continuation.window_start_exclusive,
                    )
                } else {
                    (
                        Some(continuation.next_partition_id.clone()),
                        continuation.resume_after_partition,
                    )
                }
            }
            None => (None, false),
        };

        let query_snapshot = MemoryQuerySnapshot {
            router_manifest_id: router.manifest_id.clone(),
            partition_generations: BTreeMap::new(),
            pinned_at: OffsetDateTime::now_utc(),
        };
        let plan = MemoryRetrievalPlan::from_query(
            query_fingerprint.clone(),
            &query_snapshot,
            query,
            terms.clone(),
        );
        debug_assert!(plan.validate().is_ok());
        let (routed_terms, planned_terms) = match &plan.operators[0] {
            MemoryRetrievalOperator::AuthorizedSourceEnumeration => {
                (vec![ALL_PARTITIONS_TERM.to_string()], Vec::new())
            }
            MemoryRetrievalOperator::LexicalExact { terms } => (terms.clone(), terms.clone()),
        };
        let ranked_resume = match &query.continuation {
            Some(continuation) if continuation.ranked_evidence => {
                let generation: LexicalPartitionGeneration = self.read_required(
                    &self.partition_generation_path(
                        &continuation.next_partition_id,
                        &continuation.next_generation_id,
                    ),
                    "ranked evidence continuation generation",
                )?;
                let lexical_anchor = generation
                    .anchors
                    .get(continuation.next_anchor)
                    .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
                        detail:
                            "ranked evidence continuation anchor is outside its pinned generation"
                                .to_string(),
                    })?;
                Some(RankedEvidenceCursor {
                    partition_id: continuation.next_partition_id.clone(),
                    generation_id: continuation.next_generation_id.clone(),
                    anchor_index: continuation.next_anchor,
                    score: lexical_score(&lexical_anchor.terms, &planned_terms),
                })
            }
            _ => None,
        };
        let effective_partition_limit = plan.limits.max_partitions;
        let routed_page = self.route_partitions(
            &router,
            access,
            &routed_terms,
            start_partition_id.as_ref(),
            start_exclusive,
            effective_partition_limit,
        )?;
        let routed = routed_page.items;
        let mut bundle = MemoryEvidenceBundle {
            plan: Some(plan.clone()),
            snapshot: Some(query_snapshot),
            files: Vec::new(),
            evidence: Vec::new(),
            relations: Vec::new(),
            coverage: EvidenceCoverage::Complete,
            continuation: None,
            trace: QueryFeatureTrace {
                plan_id: Some(plan.plan_id.clone()),
                terms: planned_terms.clone(),
                routed_partitions: routed.len().min(effective_partition_limit),
                searched_partitions: 0,
                stale_partitions: 0,
                trace_persisted: false,
            },
        };
        let query_approximation_reason = query_terms_truncated.then(|| {
            format!("query contains more than {MAX_MEMORY_RETRIEVAL_TERMS} unique lexical terms")
        });
        if let Some(reason) = &query_approximation_reason {
            bundle.coverage = EvidenceCoverage::Approximate {
                reason: reason.clone(),
            };
        }
        let partition_end = effective_partition_limit.min(routed.len());
        let mut ranked_candidates = Vec::new();
        for (partition_id, generation_id) in routed.iter().take(partition_end) {
            let generation: LexicalPartitionGeneration = self.read_required(
                &self.partition_generation_path(partition_id, generation_id),
                "partition generation",
            )?;
            if !self.current_root_authorizes_generation(access, &generation)? {
                continue;
            }
            bundle.trace.searched_partitions += 1;
            if let Some(snapshot) = bundle.snapshot.as_mut() {
                snapshot
                    .partition_generations
                    .insert(partition_id.clone(), generation_id.clone());
            }
            match self.materialize_generation_text(&generation) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    bundle.trace.stale_partitions += 1;
                    continue;
                }
                Err(error) => return Err(error),
            }
            if !self.current_root_authorizes_generation(access, &generation)? {
                bundle.trace.stale_partitions += 1;
                continue;
            }
            for (anchor_index, lexical_anchor) in generation.anchors.iter().enumerate() {
                let score = lexical_score(&lexical_anchor.terms, &planned_terms);
                if !planned_terms.is_empty() && score == 0 {
                    continue;
                }
                let candidate = RankedEvidenceCandidate {
                    partition_id: partition_id.clone(),
                    generation_id: generation_id.clone(),
                    anchor_index,
                    score,
                };
                if ranked_resume.as_ref().is_some_and(|cursor| {
                    compare_ranked_evidence_to_cursor(&candidate, cursor) == Ordering::Less
                }) {
                    continue;
                }
                retain_ranked_evidence_candidate(&mut ranked_candidates, candidate);
            }
        }

        let mut stopped_for_budget = false;
        for candidate in ranked_candidates {
            if bundle.evidence.len() >= plan.limits.max_evidence {
                bundle.coverage = EvidenceCoverage::BudgetTruncated {
                    reason: combine_coverage_reasons(
                        "evidence record limit reached".to_string(),
                        query_approximation_reason.as_deref(),
                    ),
                };
                bundle.continuation = Some(self.sign_query_continuation(
                    router.manifest_id.clone(),
                    query_fingerprint.clone(),
                    start_partition_id.clone(),
                    start_exclusive,
                    true,
                    candidate.partition_id,
                    candidate.generation_id,
                    false,
                    candidate.anchor_index,
                )?);
                stopped_for_budget = true;
                break;
            }
            let generation: LexicalPartitionGeneration = self.read_required(
                &self.partition_generation_path(&candidate.partition_id, &candidate.generation_id),
                "ranked evidence generation",
            )?;
            if !self.current_root_authorizes_generation(access, &generation)? {
                continue;
            }
            let text = match self.materialize_generation_text(&generation)? {
                Some(text) => text,
                None => {
                    bundle.trace.stale_partitions += 1;
                    continue;
                }
            };
            if !self.current_root_authorizes_generation(access, &generation)? {
                bundle.trace.stale_partitions += 1;
                continue;
            }
            let lexical_anchor =
                generation
                    .anchors
                    .get(candidate.anchor_index)
                    .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
                        detail: "ranked evidence anchor is outside its pinned generation"
                            .to_string(),
                    })?;
            let Some(content) = materialize_text_anchor(&text, &lexical_anchor.anchor.selector)
            else {
                bundle.trace.stale_partitions += 1;
                continue;
            };
            let evidence = MemoryEvidence {
                anchor: lexical_anchor.anchor.clone(),
                content: content.to_string(),
                score: candidate.score,
                reason: if planned_terms.is_empty() {
                    "bounded source enumeration".to_string()
                } else {
                    "exact lexical anchor match".to_string()
                },
            };
            if !try_push_evidence(
                &mut bundle,
                &generation.source,
                evidence,
                plan.limits.max_evidence_bytes,
            ) {
                bundle.coverage = EvidenceCoverage::BudgetTruncated {
                    reason: combine_coverage_reasons(
                        "serialized evidence byte limit reached".to_string(),
                        query_approximation_reason.as_deref(),
                    ),
                };
                bundle.continuation = Some(self.sign_query_continuation(
                    router.manifest_id.clone(),
                    query_fingerprint.clone(),
                    start_partition_id.clone(),
                    start_exclusive,
                    true,
                    candidate.partition_id,
                    candidate.generation_id,
                    false,
                    candidate.anchor_index,
                )?);
                stopped_for_budget = true;
                break;
            }
        }

        if !stopped_for_budget && (partition_end < routed.len() || routed_page.has_more_routed) {
            let (next_partition_id, next_generation_id) = routed
                .get(partition_end)
                .cloned()
                .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
                    detail: "bounded router omitted the next partition key".to_string(),
                })?;
            bundle.coverage = EvidenceCoverage::BudgetTruncated {
                reason: combine_coverage_reasons(
                    "partition search limit reached".to_string(),
                    query_approximation_reason.as_deref(),
                ),
            };
            bundle.continuation = Some(self.sign_query_continuation(
                router.manifest_id.clone(),
                query_fingerprint.clone(),
                None,
                false,
                false,
                next_partition_id,
                next_generation_id,
                false,
                0,
            )?);
        } else if !stopped_for_budget && let Some(scan_resume) = routed_page.scan_resume {
            bundle.coverage = EvidenceCoverage::BudgetTruncated {
                reason: combine_coverage_reasons(
                    "router posting work limit reached".to_string(),
                    query_approximation_reason.as_deref(),
                ),
            };
            bundle.continuation = Some(self.sign_query_continuation(
                router.manifest_id.clone(),
                query_fingerprint.clone(),
                None,
                false,
                false,
                scan_resume.partition_id,
                scan_resume.generation_id,
                true,
                0,
            )?);
        } else if !stopped_for_budget && bundle.trace.stale_partitions > 0 {
            bundle.coverage = EvidenceCoverage::Approximate {
                reason: combine_coverage_reasons(
                    "one or more current-only sources changed or became unavailable".to_string(),
                    query_approximation_reason.as_deref(),
                ),
            };
        }
        // Measure the finalized envelope in its larger, trace-failure state.
        // A successful trace append flips `false` to the one-byte-shorter `true`.
        bundle.trace.trace_persisted = false;
        let serialized_bundle_bytes = serde_json::to_vec(&bundle)
            .map_err(|source| MemoryError::SobjSerialize {
                path: "memory evidence bundle".to_string(),
                detail: source.to_string(),
            })?
            .len();
        if serialized_bundle_bytes > plan.limits.max_evidence_bytes {
            return Err(MemoryError::MemoryEvidenceBundleTooLarge {
                required: serialized_bundle_bytes,
                limit: plan.limits.max_evidence_bytes,
            });
        }
        let mut persisted_bundle = bundle.clone();
        persisted_bundle.trace.trace_persisted = true;
        if self
            .append_retrieval_trace(access, &query_fingerprint, &persisted_bundle)
            .is_ok()
        {
            bundle.trace.trace_persisted = true;
        }
        Ok(bundle)
    }

    fn materialize_generation_text(
        &self,
        generation: &LexicalPartitionGeneration,
    ) -> Result<Option<String>, MemoryError> {
        if let Some(content) = &generation.embedded_content {
            let content_hash = blake3::hash(content.as_bytes()).to_hex().to_string();
            return Ok(
                (content_hash == generation.source.version.content_hash).then(|| content.clone())
            );
        }
        let root = self.root_by_id(&generation.source.version.file_ref.root_id)?;
        let candidate = source_candidate_from_descriptor(&root, &generation.source)?;
        let snapshot = match self.connector.acquire_snapshot(&root, &candidate) {
            Ok(snapshot)
                if snapshot.version.version_id == generation.source.version.version_id
                    && snapshot.version.content_hash == generation.source.version.content_hash =>
            {
                snapshot
            }
            Ok(_) | Err(MemoryError::SourceChanged { .. }) | Err(MemoryError::Io { .. }) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        String::from_utf8(snapshot.bytes)
            .map(Some)
            .map_err(|source_error| MemoryError::InvalidSourceContent {
                path: candidate.absolute_path.display().to_string(),
                detail: source_error.to_string(),
            })
    }

    fn current_root_authorizes_generation(
        &self,
        access: &MemoryAccessContext,
        generation: &LexicalPartitionGeneration,
    ) -> Result<bool, MemoryError> {
        let root = self.root_by_id(&generation.source.version.file_ref.root_id)?;
        let extension_allowed = Path::new(&generation.source.version.file_ref.path_hint)
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| {
                root.config
                    .include_extensions
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(extension))
            });
        Ok(
            generation.manifest.authorization_epoch == root.authorization_epoch
                && generation.source.policy == root.config.policy
                && generation.source.retention == root.retention
                && generation.source.version.byte_length <= root.config.max_source_bytes
                && extension_allowed
                && access.permits(&root.config.policy),
        )
    }

    fn append_retrieval_trace(
        &self,
        access: &MemoryAccessContext,
        query_fingerprint: &Id,
        bundle: &MemoryEvidenceBundle,
    ) -> Result<(), MemoryError> {
        let _guard = self.trace_guard()?;
        let path = self.trace_path();
        let trace = MemoryRetrievalTrace {
            query_fingerprint: query_fingerprint.clone(),
            access_fingerprint: Id::derived(
                "memory-access-context",
                serde_json::to_vec(access).unwrap_or_default(),
            ),
            router_manifest_id: bundle
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.router_manifest_id.clone())
                .unwrap_or_else(|| Id::derived("memory-lexical-router-manifest", [])),
            coverage: bundle.coverage.clone(),
            selected_anchor_ids: bundle
                .evidence
                .iter()
                .map(|evidence| evidence.anchor.anchor_id.clone())
                .collect(),
            feature_trace: bundle.trace.clone(),
            retrieved_at: OffsetDateTime::now_utc(),
        };
        let bytes = encode_sobj(&path, &trace)?;
        if bytes.len() > MAX_TRACE_RECORD_BYTES {
            return Err(MemoryError::SobjSerialize {
                path: path.display().to_string(),
                detail: format!(
                    "retrieval trace is {} bytes, exceeding the {} byte record bound",
                    bytes.len(),
                    MAX_TRACE_RECORD_BYTES
                ),
            });
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| MemoryError::Io {
                path: parent.display().to_string(),
                source,
            })?;
        }
        let manifest_path = self.trace_manifest_path();
        let manifest = self
            .read_optional::<RetrievalTraceManifest>(&manifest_path)?
            .unwrap_or_else(RetrievalTraceManifest::empty);
        manifest.validate(&manifest_path)?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let file_len = file
            .metadata()
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?
            .len();
        if file_len < manifest.valid_bytes {
            return Err(MemoryError::SobjDeserialize {
                path: path.display().to_string(),
                detail: format!(
                    "retrieval trace is {file_len} bytes but its manifest commits {} bytes",
                    manifest.valid_bytes
                ),
            });
        }
        if file_len != manifest.valid_bytes {
            file.set_len(manifest.valid_bytes)
                .map_err(|source| MemoryError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
        }
        file.seek(SeekFrom::Start(manifest.valid_bytes))
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?;
        file.write_all(&(bytes.len() as u64).to_le_bytes())
            .and_then(|()| file.write_all(&bytes))
            .and_then(|()| file.sync_data())
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?;
        let valid_bytes = manifest
            .valid_bytes
            .checked_add(8)
            .and_then(|value| value.checked_add(bytes.len() as u64))
            .ok_or(MemoryError::SequenceOverflow)?;
        self.write_sobj_atomic(
            &manifest_path,
            &RetrievalTraceManifest {
                schema_version: INDEXED_SCHEMA_VERSION,
                valid_bytes,
            },
        )
    }

    fn write_guard(&self) -> Result<IndexedWriteGuard<'_>, MemoryError> {
        let process_guard = self
            .write_lock
            .lock()
            .map_err(|error| MemoryError::LockPoisoned {
                detail: error.to_string(),
            })?;
        fs::create_dir_all(&self.root).map_err(|source| MemoryError::Io {
            path: self.root.display().to_string(),
            source,
        })?;
        let lock_path = self.root.join("indexed.write.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| MemoryError::Io {
                path: lock_path.display().to_string(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| MemoryError::Io {
            path: lock_path.display().to_string(),
            source,
        })?;
        let guard = IndexedWriteGuard {
            _process_guard: process_guard,
            file,
        };
        self.recover_pending_router_updates_locked()?;
        Ok(guard)
    }

    fn trace_guard(&self) -> Result<MutexGuard<'_, ()>, MemoryError> {
        self.trace_lock
            .lock()
            .map_err(|error| MemoryError::LockPoisoned {
                detail: error.to_string(),
            })
    }

    fn ensure_continuation_key(&self) -> Result<[u8; CONTINUATION_KEY_BYTES], MemoryError> {
        let path = self.continuation_key_path();
        if path.is_file() {
            return read_continuation_key(&path);
        }
        let parent = path.parent().expect("continuation key has a parent");
        fs::create_dir_all(parent).map_err(|source| MemoryError::Io {
            path: parent.display().to_string(),
            source,
        })?;
        let mut key = [0_u8; CONTINUATION_KEY_BYTES];
        getrandom::fill(&mut key).map_err(|source| MemoryError::DurableFile {
            path: path.display().to_string(),
            detail: format!("failed to generate continuation key: {source}"),
        })?;
        let staged = parent.join(format!(".continuation-key.{}.stage", Id::generate()));
        let write_result = (|| {
            let mut file =
                create_private_staged_file(&staged).map_err(|source| MemoryError::DurableFile {
                    path: staged.display().to_string(),
                    detail: source.to_string(),
                })?;
            file.write_all(&key)
                .and_then(|()| file.sync_all())
                .map_err(|source| MemoryError::Io {
                    path: staged.display().to_string(),
                    source,
                })
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&staged);
            return Err(error);
        }
        match install_staged_file(&staged, &path, false) {
            Ok(()) => Ok(key),
            Err(_source) if path.is_file() => {
                let _ = fs::remove_file(&staged);
                read_continuation_key(&path)
            }
            Err(source) => {
                let _ = fs::remove_file(&staged);
                Err(MemoryError::DurableFile {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                })
            }
        }
    }

    fn sign_query_continuation(
        &self,
        router_manifest_id: RouterManifestId,
        query_fingerprint: Id,
        window_start_partition_id: Option<IndexPartitionId>,
        window_start_exclusive: bool,
        ranked_evidence: bool,
        next_partition_id: IndexPartitionId,
        next_generation_id: IndexGenerationId,
        resume_after_partition: bool,
        next_anchor: usize,
    ) -> Result<MemoryQueryContinuation, MemoryError> {
        let key = read_continuation_key(&self.continuation_key_path())?;
        let authenticator = continuation_authenticator(
            &key,
            &router_manifest_id,
            &query_fingerprint,
            window_start_partition_id.as_ref(),
            window_start_exclusive,
            ranked_evidence,
            &next_partition_id,
            &next_generation_id,
            resume_after_partition,
            next_anchor,
        )?;
        Ok(MemoryQueryContinuation {
            router_manifest_id,
            query_fingerprint,
            window_start_partition_id,
            window_start_exclusive,
            ranked_evidence,
            next_partition_id,
            next_generation_id,
            resume_after_partition,
            next_anchor,
            authenticator,
        })
    }

    fn validate_query_continuation(
        &self,
        continuation: &MemoryQueryContinuation,
    ) -> Result<(), MemoryError> {
        let key = read_continuation_key(&self.continuation_key_path())?;
        let expected = continuation_authenticator(
            &key,
            &continuation.router_manifest_id,
            &continuation.query_fingerprint,
            continuation.window_start_partition_id.as_ref(),
            continuation.window_start_exclusive,
            continuation.ranked_evidence,
            &continuation.next_partition_id,
            &continuation.next_generation_id,
            continuation.resume_after_partition,
            continuation.next_anchor,
        )?;
        if continuation.authenticator != expected {
            return Err(MemoryError::InvalidMemoryContinuation {
                detail: "continuation authenticator is invalid".to_string(),
            });
        }
        let generation: LexicalPartitionGeneration = self.read_required(
            &self.partition_generation_path(
                &continuation.next_partition_id,
                &continuation.next_generation_id,
            ),
            "continuation partition generation",
        )?;
        if generation.manifest.partition_id != continuation.next_partition_id
            || generation.manifest.generation_id != continuation.next_generation_id
            || (!continuation.ranked_evidence && continuation.next_anchor != 0)
            || (continuation.ranked_evidence && continuation.resume_after_partition)
            || (!continuation.ranked_evidence
                && (continuation.window_start_partition_id.is_some()
                    || continuation.window_start_exclusive))
            || (continuation.window_start_exclusive
                && continuation.window_start_partition_id.is_none())
            || (continuation.ranked_evidence
                && continuation.next_anchor >= generation.anchors.len())
        {
            return Err(MemoryError::InvalidMemoryContinuation {
                detail: "continuation position is outside its pinned partition generation"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn write_sobj_atomic<T: Serialize + ?Sized>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), MemoryError> {
        let bytes = encode_sobj(path, value)?;
        write_file_atomic(
            path,
            &bytes,
            DurableAtomicFileOptions::new("memory-index.sobj", "memory-index"),
        )
        .map_err(|source| MemoryError::DurableFile {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    }

    fn write_sobj_immutable<T: Serialize + ?Sized>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), MemoryError> {
        if path.is_file() {
            return Ok(());
        }
        let bytes = encode_sobj(path, value)?;
        match write_file_create_new_atomic(
            path,
            &bytes,
            DurableAtomicFileOptions::new("memory-generation.sobj", "memory-generation"),
        ) {
            Ok(()) => Ok(()),
            Err(_source) if path.is_file() => Ok(()),
            Err(source) => Err(MemoryError::DurableFile {
                path: path.display().to_string(),
                detail: source.to_string(),
            }),
        }
    }

    fn read_optional<T: for<'de> Deserialize<'de>>(
        &self,
        path: &Path,
    ) -> Result<Option<T>, MemoryError> {
        match fs::read(path) {
            Ok(bytes) => decode_sobj(path, &bytes).map(Some),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(MemoryError::Io {
                path: path.display().to_string(),
                source,
            }),
        }
    }

    fn read_required<T: for<'de> Deserialize<'de>>(
        &self,
        path: &Path,
        kind: &'static str,
    ) -> Result<T, MemoryError> {
        self.read_optional(path)?
            .ok_or_else(|| MemoryError::MissingIndexedMemoryObject {
                kind,
                path: path.display().to_string(),
            })
    }

    fn roots_dir(&self) -> PathBuf {
        self.root.join("catalog").join("roots")
    }

    fn root_path(&self, root_id: &IndexRootId) -> PathBuf {
        self.roots_dir().join(format!("{root_id}.sobj"))
    }

    fn sources_dir(&self, root_id: &IndexRootId) -> PathBuf {
        self.root
            .join("catalog")
            .join("sources")
            .join(root_id.as_str())
    }

    fn source_path(&self, root_id: &IndexRootId, file_id: &MemoryFileId) -> PathBuf {
        self.sources_dir(root_id).join(format!("{file_id}.sobj"))
    }

    fn partition_dir(&self, partition_id: &IndexPartitionId) -> PathBuf {
        self.root.join("partitions").join(partition_id.as_str())
    }

    fn partition_manifest_path(&self, partition_id: &IndexPartitionId) -> PathBuf {
        self.partition_dir(partition_id).join("manifest.sobj")
    }

    fn partition_generation_path(
        &self,
        partition_id: &IndexPartitionId,
        generation_id: &IndexGenerationId,
    ) -> PathBuf {
        self.partition_dir(partition_id)
            .join("generations")
            .join(format!("{generation_id}.sobj"))
    }

    fn pending_router_updates_dir(&self) -> PathBuf {
        self.root.join("transactions").join("router")
    }

    fn pending_router_update_path(&self, partition_id: &IndexPartitionId) -> PathBuf {
        self.pending_router_updates_dir()
            .join(format!("{partition_id}.sobj"))
    }

    fn router_manifest_path(&self) -> PathBuf {
        self.root
            .join("routing")
            .join("lexical")
            .join("manifest.sobj")
    }

    fn router_manifest_generation_path(&self, manifest_id: &RouterManifestId) -> PathBuf {
        self.root
            .join("routing")
            .join("lexical")
            .join("manifests")
            .join(format!("{manifest_id}.sobj"))
    }

    fn router_tree_store(&self) -> RouterTreeStore {
        RouterTreeStore::new(self.root.join("routing").join("lexical").join("shards"))
    }

    fn trace_path(&self) -> PathBuf {
        self.root.join("traces").join("retrieval.sobjlog")
    }

    fn trace_manifest_path(&self) -> PathBuf {
        self.root.join("traces").join("retrieval-manifest.sobj")
    }

    fn continuation_key_path(&self) -> PathBuf {
        self.root.join("private").join("continuation.key")
    }
}

impl MemorySourceCatalog for IndexedFileMemoryStore {
    fn register_root(&self, config: LocalIndexRootConfig) -> Result<IndexRoot, MemoryError> {
        self.register_local_root(config)
    }

    fn root_page(
        &self,
        after: Option<&IndexRootId>,
        limit: usize,
    ) -> Result<Page<IndexRoot, IndexRootId>, MemoryError> {
        validate_page_limit("index root catalog", limit, MAX_CATALOG_PAGE_SIZE)?;
        let (ids, has_more) = bounded_ids(&self.roots_dir(), after, limit)?;
        let mut items = Vec::with_capacity(ids.len());
        for id in &ids {
            items.push(self.root_by_id(id)?);
        }
        Ok(Page {
            continuation: has_more.then(|| ids.last().cloned()).flatten(),
            items,
        })
    }

    fn source_page(
        &self,
        root_id: &IndexRootId,
        after: Option<&MemoryFileId>,
        limit: usize,
    ) -> Result<Page<SourceDescriptor, MemoryFileId>, MemoryError> {
        validate_page_limit("memory source catalog", limit, MAX_CATALOG_PAGE_SIZE)?;
        let (ids, has_more) = bounded_ids(&self.sources_dir(root_id), after, limit)?;
        let mut items = Vec::with_capacity(ids.len());
        for id in &ids {
            items.push(self.read_required(&self.source_path(root_id, id), "source descriptor")?);
        }
        Ok(Page {
            continuation: has_more.then(|| ids.last().cloned()).flatten(),
            items,
        })
    }
}

impl MemoryIndexStore for IndexedFileMemoryStore {
    fn publish_source(&self, source: &SourceCandidate) -> Result<IndexGenerationId, MemoryError> {
        self.publish_source_candidate(source)
    }

    fn pin_query_snapshot(&self) -> Result<MemoryQuerySnapshot, MemoryError> {
        self.recover_pending_router_updates_if_needed()?;
        let router = self.load_router_manifest()?;
        Ok(MemoryQuerySnapshot {
            router_manifest_id: router.manifest_id,
            partition_generations: BTreeMap::new(),
            pinned_at: OffsetDateTime::now_utc(),
        })
    }
}

impl MemoryTraceStore for IndexedFileMemoryStore {
    fn trace_page(
        &self,
        cursor: Option<RetrievalTraceCursor>,
        limit: usize,
    ) -> Result<Page<MemoryRetrievalTrace, RetrievalTraceCursor>, MemoryError> {
        validate_page_limit("memory retrieval trace", limit, MAX_TRACE_PAGE_SIZE)?;
        let _guard = self.trace_guard()?;
        let path = self.trace_path();
        let manifest_path = self.trace_manifest_path();
        let Some(manifest) = self.read_optional::<RetrievalTraceManifest>(&manifest_path)? else {
            return Ok(Page {
                items: Vec::new(),
                continuation: None,
            });
        };
        manifest.validate(&manifest_path)?;
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Page {
                    items: Vec::new(),
                    continuation: None,
                });
            }
            Err(source) => {
                return Err(MemoryError::Io {
                    path: path.display().to_string(),
                    source,
                });
            }
        };
        file.lock_shared().map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let physical_file_len = file
            .metadata()
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?
            .len();
        if physical_file_len < manifest.valid_bytes {
            return Err(MemoryError::SobjDeserialize {
                path: path.display().to_string(),
                detail: format!(
                    "retrieval trace is {physical_file_len} bytes but its manifest commits {} bytes",
                    manifest.valid_bytes
                ),
            });
        }
        let file_len = manifest.valid_bytes;
        let mut offset = cursor.unwrap_or_default().byte_offset;
        if offset > file_len {
            return Err(MemoryError::InvalidMemoryContinuation {
                detail: format!("trace offset {offset} exceeds log length {file_len}"),
            });
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?;
        let mut items = Vec::with_capacity(limit.min(256));
        while items.len() < limit && offset < file_len {
            if file_len.saturating_sub(offset) < 8 {
                return Err(MemoryError::SobjDeserialize {
                    path: path.display().to_string(),
                    detail: "truncated retrieval trace length prefix".to_string(),
                });
            }
            let mut length_bytes = [0_u8; 8];
            file.read_exact(&mut length_bytes)
                .map_err(|source| MemoryError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
            let length = u64::from_le_bytes(length_bytes);
            let length_usize =
                usize::try_from(length).map_err(|_| MemoryError::SobjDeserialize {
                    path: path.display().to_string(),
                    detail: format!("retrieval trace record length {length} is not addressable"),
                })?;
            if length_usize > MAX_TRACE_RECORD_BYTES
                || offset.saturating_add(8).saturating_add(length) > file_len
            {
                return Err(MemoryError::SobjDeserialize {
                    path: path.display().to_string(),
                    detail: format!("invalid retrieval trace record length {length}"),
                });
            }
            let mut bytes = vec![0_u8; length_usize];
            file.read_exact(&mut bytes)
                .map_err(|source| MemoryError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
            items.push(decode_sobj(&path, &bytes)?);
            offset = offset.saturating_add(8).saturating_add(length);
        }
        Ok(Page {
            items,
            continuation: (offset < file_len).then_some(RetrievalTraceCursor {
                byte_offset: offset,
            }),
        })
    }
}

impl MemoryRetriever for IndexedFileMemoryStore {
    fn retrieve_evidence(
        &self,
        access: &MemoryAccessContext,
        query: &MemoryQuery,
    ) -> Result<MemoryEvidenceBundle, MemoryError> {
        self.retrieve(access, query)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct PendingRouterUpdate {
    schema_version: u32,
    transaction_id: Id,
    partition_id: IndexPartitionId,
    old_generation_id: Option<IndexGenerationId>,
    new_generation_id: Option<IndexGenerationId>,
}

impl PendingRouterUpdate {
    fn new(
        partition_id: IndexPartitionId,
        old_generation_id: Option<IndexGenerationId>,
        new_generation_id: Option<IndexGenerationId>,
    ) -> Self {
        let transaction_id = Self::canonical_id(
            &partition_id,
            old_generation_id.as_ref(),
            new_generation_id.as_ref(),
        );
        Self {
            schema_version: INDEXED_SCHEMA_VERSION,
            transaction_id,
            partition_id,
            old_generation_id,
            new_generation_id,
        }
    }

    fn canonical_id(
        partition_id: &IndexPartitionId,
        old_generation_id: Option<&IndexGenerationId>,
        new_generation_id: Option<&IndexGenerationId>,
    ) -> Id {
        Id::derived(
            "memory-pending-router-update",
            serde_json::to_vec(&(
                INDEXED_SCHEMA_VERSION,
                partition_id,
                old_generation_id,
                new_generation_id,
            ))
            .expect("pending router update is serializable"),
        )
    }

    fn validate(&self, path: &Path) -> Result<(), MemoryError> {
        if self.schema_version != INDEXED_SCHEMA_VERSION {
            return Err(MemoryError::UnsupportedSchema {
                path: path.display().to_string(),
                schema_version: self.schema_version,
            });
        }
        if self.old_generation_id.is_none() && self.new_generation_id.is_none() {
            return Err(MemoryError::SobjDeserialize {
                path: path.display().to_string(),
                detail: "pending router update has neither an old nor new generation".to_string(),
            });
        }
        let expected = Self::canonical_id(
            &self.partition_id,
            self.old_generation_id.as_ref(),
            self.new_generation_id.as_ref(),
        );
        if self.transaction_id != expected {
            return Err(MemoryError::SobjDeserialize {
                path: path.display().to_string(),
                detail: format!(
                    "pending router transaction id is not canonical: expected {expected}, got {}",
                    self.transaction_id
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RetrievalTraceManifest {
    schema_version: u32,
    valid_bytes: u64,
}

impl RetrievalTraceManifest {
    fn empty() -> Self {
        Self {
            schema_version: INDEXED_SCHEMA_VERSION,
            valid_bytes: 0,
        }
    }

    fn validate(&self, path: &Path) -> Result<(), MemoryError> {
        if self.schema_version != INDEXED_SCHEMA_VERSION {
            return Err(MemoryError::UnsupportedSchema {
                path: path.display().to_string(),
                schema_version: self.schema_version,
            });
        }
        Ok(())
    }
}

fn extract_text_anchors(
    version: &MemoryFileVersionRef,
    media_type: &str,
    text: &str,
) -> Result<Vec<LexicalAnchor>, MemoryError> {
    let mut anchors = Vec::new();
    let mut start = 0usize;
    let mut line = 1u64;
    for segment in text.split_inclusive('\n') {
        let mut content = segment.strip_suffix('\n').unwrap_or(segment);
        if let Some(without_cr) = content.strip_suffix('\r') {
            content = without_cr;
        }
        let end = start.saturating_add(content.len());
        if !content.trim().is_empty() {
            anchors.push(LexicalAnchor {
                anchor: MemoryAnchor::canonical(
                    version.clone(),
                    MemoryAnchorSelector::TextRange {
                        start_byte: start as u64,
                        end_byte: end as u64,
                        start_line: line,
                        end_line: line,
                    },
                    media_type,
                )?,
                terms: tokenize(content),
            });
        }
        start = start.saturating_add(segment.len());
        line = line.saturating_add(1);
    }
    if text.is_empty() {
        return Ok(anchors);
    }
    if !text.ends_with('\n') && start < text.len() {
        let content = &text[start..];
        if !content.trim().is_empty() {
            anchors.push(LexicalAnchor {
                anchor: MemoryAnchor::canonical(
                    version.clone(),
                    MemoryAnchorSelector::TextRange {
                        start_byte: start as u64,
                        end_byte: text.len() as u64,
                        start_line: line,
                        end_line: line,
                    },
                    media_type,
                )?,
                terms: tokenize(content),
            });
        }
    }
    Ok(anchors)
}

fn tokenize(text: &str) -> Vec<String> {
    tokenize_with_limit(text, None).0
}

fn tokenize_query(text: &str) -> (Vec<String>, bool) {
    tokenize_with_limit(text, Some(MAX_MEMORY_RETRIEVAL_TERMS))
}

fn tokenize_with_limit(text: &str, limit: Option<usize>) -> (Vec<String>, bool) {
    let mut terms = BTreeSet::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    let mut current_hasher = blake3::Hasher::new();
    let mut truncated = false;
    for character in text.chars() {
        if character.is_alphanumeric() || character == '_' {
            for normalized in character.to_lowercase() {
                let mut encoded = [0_u8; 4];
                current_hasher.update(normalized.encode_utf8(&mut encoded).as_bytes());
                if current_chars <= MAX_MEMORY_RETRIEVAL_TERM_CHARS {
                    current.push(normalized);
                }
                current_chars = current_chars.saturating_add(1);
            }
        } else if current_chars > 0 {
            finish_token(
                &mut terms,
                &mut current,
                &mut current_chars,
                &mut current_hasher,
                limit,
                &mut truncated,
            );
        }
    }
    if current_chars > 0 {
        finish_token(
            &mut terms,
            &mut current,
            &mut current_chars,
            &mut current_hasher,
            limit,
            &mut truncated,
        );
    }
    (terms.into_iter().collect(), truncated)
}

fn finish_token(
    terms: &mut BTreeSet<String>,
    current: &mut String,
    current_chars: &mut usize,
    current_hasher: &mut blake3::Hasher,
    limit: Option<usize>,
    truncated: &mut bool,
) {
    let token = if *current_chars <= MAX_MEMORY_RETRIEVAL_TERM_CHARS {
        std::mem::take(current)
    } else {
        // Preserve exact equality for long terms without retaining an unbounded
        // token: a readable bounded prefix plus the full normalized-token hash.
        let prefix = current
            .chars()
            .take(60.min(MAX_MEMORY_RETRIEVAL_TERM_CHARS))
            .collect::<String>();
        current.clear();
        format!("{prefix}~{}", current_hasher.finalize().to_hex())
    };
    terms.insert(token);
    if limit.is_some_and(|limit| terms.len() > limit) {
        terms.pop_last();
        *truncated = true;
    }
    *current_chars = 0;
    *current_hasher = blake3::Hasher::new();
}

fn record_published_generation(
    report: &mut IndexMaintenanceReport,
    generation_id: IndexGenerationId,
) {
    report.published_generation_count = report.published_generation_count.saturating_add(1);
    if report.published_generations.len() < MAX_REPORTED_GENERATION_IDS {
        report.published_generations.push(generation_id);
    } else {
        report.published_generations_truncated = true;
    }
}

fn generation_terms(generation: Option<&LexicalPartitionGeneration>) -> BTreeSet<String> {
    let mut terms = BTreeSet::from([ALL_PARTITIONS_TERM.to_string()]);
    if let Some(generation) = generation {
        for anchor in &generation.anchors {
            terms.extend(anchor.terms.iter().cloned());
        }
    } else {
        terms.clear();
    }
    terms
}

fn read_continuation_key(path: &Path) -> Result<[u8; CONTINUATION_KEY_BYTES], MemoryError> {
    let bytes = fs::read(path).map_err(|source| MemoryError::Io {
        path: path.display().to_string(),
        source,
    })?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| MemoryError::DurableFile {
            path: path.display().to_string(),
            detail: format!(
                "continuation key is {} bytes; expected {CONTINUATION_KEY_BYTES}",
                bytes.len()
            ),
        })
}

fn continuation_authenticator(
    key: &[u8; CONTINUATION_KEY_BYTES],
    router_manifest_id: &RouterManifestId,
    query_fingerprint: &Id,
    window_start_partition_id: Option<&IndexPartitionId>,
    window_start_exclusive: bool,
    ranked_evidence: bool,
    next_partition_id: &IndexPartitionId,
    next_generation_id: &IndexGenerationId,
    resume_after_partition: bool,
    next_anchor: usize,
) -> Result<String, MemoryError> {
    let payload = serde_json::to_vec(&(
        INDEXED_SCHEMA_VERSION,
        "memory-query-continuation",
        router_manifest_id,
        query_fingerprint,
        window_start_partition_id,
        window_start_exclusive,
        ranked_evidence,
        next_partition_id,
        next_generation_id,
        resume_after_partition,
        next_anchor,
    ))
    .map_err(|source| MemoryError::SobjSerialize {
        path: "memory query continuation".to_string(),
        detail: source.to_string(),
    })?;
    Ok(blake3::keyed_hash(key, &payload).to_hex().to_string())
}

fn source_candidate_from_descriptor(
    root: &IndexRoot,
    source: &SourceDescriptor,
) -> Result<SourceCandidate, MemoryError> {
    let path_hint = Path::new(&source.version.file_ref.path_hint);
    let mut absolute_path = root.config.path.clone();
    for component in path_hint.components() {
        match component {
            std::path::Component::Normal(segment) => absolute_path.push(segment),
            _ => {
                return Err(MemoryError::InvalidSourceContent {
                    path: source.version.file_ref.path_hint.clone(),
                    detail: "persisted source path is not a safe relative path".to_string(),
                });
            }
        }
    }
    Ok(SourceCandidate {
        file_ref: source.version.file_ref.clone(),
        absolute_path,
        platform_identity: source.platform_identity.clone(),
        source_version_hint: source.source_version_hint.clone(),
        byte_length: source.version.byte_length,
        media_type: source.media_type.clone(),
    })
}

fn lexical_score(anchor_terms: &[String], query_terms: &[String]) -> i32 {
    query_terms
        .iter()
        .filter(|term| anchor_terms.binary_search(term).is_ok())
        .count()
        .saturating_mul(100) as i32
}

fn compare_ranked_evidence(
    left: &RankedEvidenceCandidate,
    right: &RankedEvidenceCandidate,
) -> Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
        .then_with(|| left.generation_id.cmp(&right.generation_id))
        .then_with(|| left.anchor_index.cmp(&right.anchor_index))
}

fn compare_ranked_evidence_to_cursor(
    candidate: &RankedEvidenceCandidate,
    cursor: &RankedEvidenceCursor,
) -> Ordering {
    cursor
        .score
        .cmp(&candidate.score)
        .then_with(|| candidate.partition_id.cmp(&cursor.partition_id))
        .then_with(|| candidate.generation_id.cmp(&cursor.generation_id))
        .then_with(|| candidate.anchor_index.cmp(&cursor.anchor_index))
}

fn retain_ranked_evidence_candidate(
    candidates: &mut Vec<RankedEvidenceCandidate>,
    candidate: RankedEvidenceCandidate,
) {
    let index = candidates
        .binary_search_by(|current| compare_ranked_evidence(current, &candidate))
        .unwrap_or_else(|index| index);
    candidates.insert(index, candidate);
    if candidates.len() > MAX_MEMORY_RETRIEVAL_EVIDENCE.saturating_add(1) {
        candidates.pop();
    }
}

fn materialize_text_anchor<'a>(text: &'a str, selector: &MemoryAnchorSelector) -> Option<&'a str> {
    match selector {
        MemoryAnchorSelector::WholeFile => Some(text),
        MemoryAnchorSelector::TextRange {
            start_byte,
            end_byte,
            ..
        }
        | MemoryAnchorSelector::ByteRange {
            start_byte,
            end_byte,
        } => {
            let start = usize::try_from(*start_byte).ok()?;
            let end = usize::try_from(*end_byte).ok()?;
            text.get(start..end)
        }
        _ => None,
    }
}

fn try_push_evidence(
    bundle: &mut MemoryEvidenceBundle,
    source: &SourceDescriptor,
    evidence: MemoryEvidence,
    max_bytes: usize,
) -> bool {
    let mut candidate = bundle.clone();
    if let Some(file) = candidate
        .files
        .iter_mut()
        .find(|file| file.source.version.file_ref.object_id == source.version.file_ref.object_id)
    {
        file.evidence_anchor_ids
            .push(evidence.anchor.anchor_id.clone());
    } else {
        candidate.files.push(MemoryEvidenceFile {
            source: source.clone(),
            evidence_anchor_ids: vec![evidence.anchor.anchor_id.clone()],
        });
    }
    candidate.evidence.push(evidence);
    let fits = serde_json::to_vec(&candidate)
        .map(|bytes| bytes.len() <= max_bytes)
        .unwrap_or(false);
    if fits {
        *bundle = candidate;
    }
    fits
}

fn query_fingerprint(access: &MemoryAccessContext, query: &MemoryQuery) -> Result<Id, MemoryError> {
    let mut query = query.clone();
    query.continuation = None;
    // Page and byte budgets control delivery, not query identity. Excluding
    // them lets a caller resume a truncated snapshot with a larger budget
    // instead of producing a permanently non-progressing continuation.
    query.max_records = 0;
    query.max_bytes = 0;
    query.max_partitions = 0;
    serde_json::to_vec(&(access, query))
        .map(|bytes| Id::derived("memory-evidence-query", bytes))
        .map_err(|source| MemoryError::SobjSerialize {
            path: "memory evidence query".to_string(),
            detail: source.to_string(),
        })
}

fn combine_coverage_reasons(primary: String, additional: Option<&str>) -> String {
    match additional {
        Some(additional) if additional != primary => format!("{primary}; {additional}"),
        _ => primary,
    }
}

fn bounded_ids(
    directory: &Path,
    after: Option<&Id>,
    limit: usize,
) -> Result<(Vec<Id>, bool), MemoryError> {
    if limit == 0 {
        return Ok((Vec::new(), directory.is_dir()));
    }
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), false));
        }
        Err(source) => {
            return Err(MemoryError::Io {
                path: directory.display().to_string(),
                source,
            });
        }
    };
    let capacity = limit.saturating_add(1);
    let mut ids = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|source| MemoryError::Io {
            path: directory.display().to_string(),
            source,
        })?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("sobj") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(id) = Id::new(stem) else {
            continue;
        };
        if after.is_some_and(|after| &id <= after) {
            continue;
        }
        ids.insert(id);
        if ids.len() > capacity {
            ids.pop_last();
        }
    }
    let has_more = ids.len() > limit;
    if has_more {
        ids.pop_last();
    }
    Ok((ids.into_iter().collect(), has_more))
}

fn validate_page_limit(surface: &'static str, limit: usize, max: usize) -> Result<(), MemoryError> {
    if limit == 0 || limit > max {
        Err(MemoryError::InvalidMemoryPageLimit {
            surface,
            limit,
            max,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestMemoryDirectory(PathBuf);

    impl TestMemoryDirectory {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("sinomo-memory-indexed-{name}-{}", Id::generate()));
            fs::create_dir_all(&path).expect("create indexed-memory test directory");
            Self(path)
        }
    }

    impl Drop for TestMemoryDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn text_extraction_uses_exact_non_empty_line_ranges() {
        let root_id = Id::derived("test", b"root");
        let file_id = Id::derived("test", b"file");
        let version = MemoryFileVersionRef {
            file_ref: MemoryFileRef {
                connector_id: "test".to_string(),
                root_id,
                object_id: file_id,
                path_hint: "notes.md".to_string(),
            },
            version_id: Id::derived("test", b"version"),
            content_hash: "hash".to_string(),
            byte_length: 15,
        };
        let text = "alpha\n\nbeta γ\n";
        let anchors = extract_text_anchors(&version, "text/markdown", text).expect("anchors");
        assert_eq!(anchors.len(), 2);
        for anchor in anchors {
            let materialized = materialize_text_anchor(text, &anchor.anchor.selector).unwrap();
            assert!(!materialized.trim().is_empty());
        }
    }

    #[test]
    fn pending_router_publication_recovers_catalog_before_route_visibility() {
        let fixture = TestMemoryDirectory::new("publication-recovery");
        let source_root = fixture.0.join("source");
        let memory_root = fixture.0.join("memory");
        fs::create_dir_all(&source_root).expect("create source root");
        fs::write(source_root.join("note.md"), "recoverable evidence\n").expect("write source");
        let store = IndexedFileMemoryStore::new(&memory_root);
        let root = store
            .register_local_root(LocalIndexRootConfig::new(
                &source_root,
                IndexRootPolicy::cluster_local(),
            ))
            .expect("register root");
        store.index_root(&root.root_id).expect("index source");
        let source = store
            .source_page(&root.root_id, None, 1)
            .expect("source page")
            .items
            .into_iter()
            .next()
            .expect("source descriptor");
        let partition_id = Id::derived(
            "memory-index-partition",
            source.version.file_ref.object_id.as_str(),
        );
        let generation = store
            .current_partition_generation(&partition_id)
            .expect("load generation")
            .expect("current generation");

        {
            let _guard = store.write_guard().expect("lock indexed store");
            store
                .unroute_partition(&partition_id)
                .expect("make interrupted publication invisible");
            fs::remove_file(store.partition_manifest_path(&partition_id))
                .expect("remove partition manifest");
            fs::remove_file(store.source_path(&root.root_id, &source.version.file_ref.object_id))
                .expect("remove source descriptor");
            let pending = PendingRouterUpdate::new(
                partition_id.clone(),
                None,
                Some(generation.manifest.generation_id.clone()),
            );
            store
                .write_sobj_atomic(&store.pending_router_update_path(&partition_id), &pending)
                .expect("persist interrupted transaction");
        }

        let restarted = IndexedFileMemoryStore::new(&memory_root);
        let bundle = restarted
            .retrieve(
                &MemoryAccessContext::cluster_local(),
                &MemoryQuery {
                    task_hint: Some("recoverable".to_string()),
                    max_bytes: 128 * 1024,
                    ..MemoryQuery::default()
                },
            )
            .expect("recover before query");
        assert_eq!(bundle.evidence.len(), 1);
        assert_eq!(bundle.evidence[0].content, "recoverable evidence");
        assert!(!restarted.pending_router_update_path(&partition_id).exists());
        assert!(restarted.partition_manifest_path(&partition_id).is_file());
        assert!(
            restarted
                .source_path(&root.root_id, &source.version.file_ref.object_id)
                .is_file()
        );
    }
}
