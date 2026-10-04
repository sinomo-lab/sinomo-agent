//! Bounded per-source attachment records stored through ordinary DFS commits.
//! Small values share one indexed document; large values are immutable DFS
//! objects. Catalog CAS is the publication point and fences upload leases.

use crate::posix_model::Result;
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Seek};
use std::sync::{Arc, Mutex, OnceLock};

const FORMAT: u32 = 2;
const MAX_RECEIPTS: usize = 16;
const MAX_GARBAGE: usize = 64;
const CLAIM_NANOS: u64 = 5 * 60 * 1_000_000_000;
const MAX_CAS_ATTEMPTS: usize = 16;

fn invalid(message: impl ToString) -> DfsPosixError {
    DfsPosixError::InvalidPath {
        path: format!("attachment catalog: {}", message.to_string()),
    }
}
fn conflict(source: &DfsAttachmentSource, message: &str) -> DfsPosixError {
    DfsPosixError::Conflict {
        path: source.path.clone(),
        reason: message.into(),
    }
}
fn key(key: &DfsAttachmentKey) -> String {
    format!("{}~{}", key.kind, key.name)
}
fn path(source: &DfsAttachmentSource) -> String {
    format!("{}/catalog.sobj", dfs_attachment_catalog_directory(source))
}
fn codec() -> sinomo_core::network::SinomoObjectCodec {
    sinomo_core::network::SinomoObjectCodec::network_frame()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachmentBlob {
    pub path: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub size: u64,
}

impl AttachmentBlob {
    fn from_stat(stat: &DfsStat) -> Result<Self> {
        Ok(Self {
            path: stat.path.clone(),
            object_id: stat.object_id.clone(),
            version: stat.version.clone(),
            content_hash: stat
                .content_hash
                .clone()
                .ok_or_else(|| invalid("payload has no hash"))?,
            size: stat
                .size_bytes
                .ok_or_else(|| invalid("payload has no length"))?,
        })
    }
    fn owned_by(&self, source: &DfsAttachmentSource) -> bool {
        let normalized = self.path.trim_start_matches('/');
        let current = format!(
            "{}/payload-",
            dfs_attachment_catalog_directory(source).trim_start_matches('/')
        );
        if let Some(token) = normalized.strip_prefix(&current) {
            return DfsObjectId::new(token).is_ok();
        }
        let legacy = format!(
            "{}/",
            dfs_attachment_directory(source).trim_start_matches('/')
        );
        normalized
            .strip_prefix(&legacy)
            .and_then(|name| name.strip_suffix(".satt"))
            .and_then(|name| name.split_once('~'))
            .is_some_and(|(kind, name)| {
                DfsAttachmentKey {
                    kind: kind.into(),
                    name: name.into(),
                }
                .validate()
                .is_ok()
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Payload {
    Inline(Vec<u8>),
    Object(AttachmentBlob),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    descriptor: DfsAttachmentDescriptor,
    owner: String,
    fingerprint: String,
    /// Tombstones suppress legacy fallback and retain the latest receipt.
    payload: Option<Payload>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pending {
    request: DfsAttachmentUploadRequest,
    actor: String,
    owner: String,
    fingerprint: String,
    payload_hash: String,
    revision: DfsAttachmentRevision,
    token: String,
    expires_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Generation {
    request: DfsAttachmentUploadRequest,
    actor: String,
    owner: String,
    expires_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Receipt {
    operation_id: String,
    fingerprint: String,
    result: DfsAttachmentResponse,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Catalog {
    format: u32,
    source: DfsAttachmentSource,
    entries: BTreeMap<String, Entry>,
    pending: BTreeMap<String, Pending>,
    generations: BTreeMap<String, Generation>,
    garbage: Vec<AttachmentBlob>,
    receipts: VecDeque<Receipt>,
}

pub(crate) struct AttachmentCatalogView(Arc<Catalog>);
impl AttachmentCatalogView {
    pub(crate) fn contains(&self, key: &DfsAttachmentKey) -> bool {
        self.0
            .entries
            .contains_key(&super::attachment_catalog::key(key))
    }
    pub(crate) fn page(
        &self,
        source: &DfsStat,
        after: Option<&str>,
        limit: usize,
    ) -> (Vec<DfsAttachmentDescriptor>, Option<String>) {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut rows = self
            .0
            .entries
            .range::<str, _>((after.map(Excluded).unwrap_or(Unbounded), Unbounded));
        let mut output = Vec::new();
        let mut last = None;
        for _ in 0..limit {
            let Some((name, entry)) = rows.next() else {
                break;
            };
            last = Some(name.clone());
            if entry.payload.is_some() {
                output.push(current_descriptor(&entry.descriptor, source));
            }
        }
        (output, if rows.next().is_some() { last } else { None })
    }
}

impl Catalog {
    fn new(source: DfsAttachmentSource) -> Self {
        Self {
            format: FORMAT,
            source,
            entries: BTreeMap::new(),
            pending: BTreeMap::new(),
            generations: BTreeMap::new(),
            garbage: Vec::new(),
            receipts: VecDeque::new(),
        }
    }
    fn validate(&self, source: &DfsAttachmentSource) -> Result<()> {
        if self.format != FORMAT
            || self.source.file_system_id != source.file_system_id
            || self.source.object_id != source.object_id
            || self.entries.len() > DFS_ATTACHMENT_MAX_PER_SOURCE
            || self.pending.len() > 64
            || self.generations.len() > 64
            || self.receipts.len() > MAX_RECEIPTS
            || self.garbage.len() > MAX_GARBAGE
        {
            return Err(invalid("invalid catalog identity or bounds"));
        }
        for (name, entry) in &self.entries {
            entry.descriptor.metadata.validate().map_err(invalid)?;
            if *name != key(&entry.descriptor.metadata.key) || entry.owner.len() > 512 {
                return Err(invalid("invalid catalog record"));
            }
            match &entry.payload {
                Some(Payload::Inline(bytes))
                    if bytes.len() > DFS_ATTACHMENT_INLINE_BYTES
                        || bytes.len() as u64 != entry.descriptor.payload_bytes =>
                {
                    return Err(invalid("invalid inline attachment size"));
                }
                Some(Payload::Object(blob))
                    if !blob.owned_by(source) || blob.size != entry.descriptor.payload_bytes =>
                {
                    return Err(invalid("invalid attachment payload reference"));
                }
                _ => {}
            }
        }
        for (name, pending) in &self.pending {
            pending.request.metadata.validate().map_err(invalid)?;
            if *name != key(&pending.request.metadata.key)
                || pending.request.source.object_id != source.object_id
                || pending.request.source.file_system_id != source.file_system_id
                || pending.actor.len() > 512
                || pending.owner.len() > 512
                || DfsObjectId::new(&pending.token).is_err()
                || pending.request.payload_bytes > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64
            {
                return Err(invalid("invalid upload claim"));
            }
        }
        for (name, generation) in &self.generations {
            generation.request.validate().map_err(invalid)?;
            if *name != key(&generation.request.metadata.key)
                || !generation.request.generation
                || generation.actor.len() > 512
                || generation.owner.len() > 512
                || generation.request.source.file_system_id != source.file_system_id
                || generation.request.source.object_id != source.object_id
            {
                return Err(invalid("invalid generation claim"));
            }
        }
        if self.garbage.iter().any(|blob| !blob.owned_by(source)) {
            return Err(invalid("foreign attachment garbage reference"));
        }
        Ok(())
    }
    fn quota(&self, owner: &str, limits: &DfsAttachmentProducerPolicy) -> Result<()> {
        let mut live = BTreeMap::new();
        for (name, entry) in &self.entries {
            if entry.owner == owner {
                live.insert(
                    name,
                    if entry.payload.is_some() {
                        entry.descriptor.payload_bytes
                    } else {
                        0
                    },
                );
            }
        }
        for (name, pending) in &self.pending {
            if pending.owner == owner {
                live.entry(name)
                    .and_modify(|n| *n = (*n).max(pending.request.payload_bytes))
                    .or_insert(pending.request.payload_bytes);
            }
        }
        for (name, generation) in &self.generations {
            if generation.owner == owner {
                live.entry(name)
                    .and_modify(|n| *n = (*n).max(generation.request.payload_bytes))
                    .or_insert(generation.request.payload_bytes);
            }
        }
        if live.len() > limits.max_attachments_per_source
            || live
                .values()
                .try_fold(0u64, |a, n| a.checked_add(*n))
                .is_none_or(|n| n > limits.max_bytes_per_source)
        {
            return Err(invalid("producer storage quota exceeded for this source"));
        }
        Ok(())
    }
    fn receipt(&self, operation: &str, fingerprint: &str) -> Result<Option<DfsAttachmentResponse>> {
        if let Some(receipt) = self.receipts.iter().find(|r| r.operation_id == operation) {
            if receipt.fingerprint != fingerprint {
                return Err(invalid(
                    "operation id was reused with different attachment input",
                ));
            }
            return Ok(Some(receipt.result.clone()));
        }
        Ok(None)
    }
    fn record_receipt(
        &mut self,
        operation_id: String,
        fingerprint: String,
        result: DfsAttachmentResponse,
    ) {
        self.receipts.push_back(Receipt {
            operation_id,
            fingerprint,
            result,
        });
        while self.receipts.len() > MAX_RECEIPTS {
            self.receipts.pop_front();
        }
    }
}

#[derive(Default)]
struct Cache {
    entries: BTreeMap<(DfsObjectId, DfsVersion, String), (Arc<Catalog>, usize)>,
    order: VecDeque<(DfsObjectId, DfsVersion, String)>,
    bytes: usize,
}
fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Cache::default()))
}

fn remember_catalog(stat: &DfsStat, catalog: Arc<Catalog>, encoded_bytes: usize) {
    let Some(hash) = &stat.content_hash else {
        return;
    };
    let key = (stat.object_id.clone(), stat.version.clone(), hash.clone());
    let cost = encoded_bytes.saturating_add(
        (catalog.entries.len()
            + catalog.pending.len()
            + catalog.generations.len()
            + catalog.receipts.len())
            * 1024,
    );
    let mut cache = cache().lock().unwrap();
    // Older snapshots remain valid for active Arc holders, but must not fill
    // the cache with dozens of generations of a single busy source.
    let obsolete = cache
        .entries
        .keys()
        .filter(|k| k.0 == stat.object_id)
        .cloned()
        .collect::<Vec<_>>();
    for old in obsolete {
        if let Some((_, bytes)) = cache.entries.remove(&old) {
            cache.bytes -= bytes;
        }
    }
    cache.order.retain(|k| k.0 != stat.object_id);
    cache.bytes += cost;
    cache.order.push_back(key.clone());
    cache.entries.insert(key, (catalog, cost));
    while cache.bytes > 16 * 1024 * 1024 || cache.entries.len() > 64 {
        let old = cache.order.pop_front().unwrap();
        if let Some((_, bytes)) = cache.entries.remove(&old) {
            cache.bytes -= bytes;
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Inspect every current authority before accepting deletion evidence. A
    /// transport error or absent metadata is not permission to erase data.
    pub(crate) fn attachment_source_lifecycle(
        &mut self,
        source: &DfsAttachmentSource,
    ) -> Result<(Option<DfsObjectMetadata>, bool)> {
        let authorities = self.file_history_authorities(&source.file_system_id)?;
        let mut deleted = None;
        for shard in &authorities {
            let result = self.query_file_history_authority(
                shard,
                DfsFileHistorySelection::CurrentObject {
                    object_id: source.object_id.clone(),
                },
            )?;
            if let DfsFileHistoryShardResult::CurrentObject {
                object: Some(object),
            } = result
            {
                if object.object_id != source.object_id
                    || object.file_system_id != source.file_system_id
                    || object.tombstone_provenance.is_some()
                {
                    return Err(invalid("foreign cleanup evidence"));
                }
                if object.kind != DfsObjectKind::Tombstone {
                    return Ok((Some(*object), false));
                }
                deleted = Some(*object);
            }
        }
        if deleted.is_some() {
            for shard in &authorities {
                let result = self.query_file_history_authority(
                    shard,
                    DfsFileHistorySelection::List {
                        path: None,
                        object_id: Some(source.object_id.clone()),
                        after: None,
                        limit: 1,
                    },
                )?;
                if let DfsFileHistoryShardResult::List { entries, .. } = result {
                    if !entries.is_empty() {
                        return Ok((deleted, false));
                    }
                }
            }
        }
        Ok((deleted.clone(), deleted.is_some()))
    }

    /// Fence expired or superseded uploads before collecting payloads. New
    /// claims use fresh payload names, so an old upload cannot be resurrected
    /// by a retry racing this garbage pass.
    pub(crate) fn maintain_attachment_source(
        &mut self,
        source: &DfsAttachmentSource,
        after: Option<&DfsDirectoryPageCursor>,
        limit: usize,
    ) -> Result<(Option<DfsDirectoryPageCursor>, Option<u64>, usize)> {
        self.maintain_attachment_source_with_owner(source, after, limit, false)
    }

    pub(crate) fn maintain_attachment_source_in_background(
        &mut self,
        source: &DfsAttachmentSource,
        after: Option<&DfsDirectoryPageCursor>,
        limit: usize,
    ) -> Result<(Option<DfsDirectoryPageCursor>, Option<u64>, usize)> {
        self.maintain_attachment_source_with_owner(source, after, limit, true)
    }

    fn maintain_attachment_source_with_owner(
        &mut self,
        source: &DfsAttachmentSource,
        after: Option<&DfsDirectoryPageCursor>,
        limit: usize,
        background: bool,
    ) -> Result<(Option<DfsDirectoryPageCursor>, Option<u64>, usize)> {
        let managed = [
            dfs_attachment_catalog_directory(source),
            dfs_attachment_directory(source),
        ];
        let mut present = false;
        for directory in &managed {
            match self.posix_open_beneath_no_links_distributed("/", directory) {
                Ok(_) => {
                    present = true;
                    break;
                }
                Err(DfsPosixError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        if !present {
            return Ok((None, None, 0));
        }
        let (object, deleted) = self.attachment_source_lifecycle(source)?;
        let directory = dfs_attachment_catalog_directory(source);
        if deleted {
            return self.remove_deleted_attachment_directory(source, &directory, after, limit);
        }
        // Only the catalog authority maintains a live catalog. Source-feed
        // consumers and cross-shard reconciliation may discover the same
        // source, but must not race identical expiration/GC writes from
        // different requester identities. Any worker may still reconcile a
        // source whose authoritative deletion was proved above.
        if background {
            let authorities = self.file_history_authorities(&source.file_system_id)?;
            if let Some(shard) = crate::control_plane::select_metadata_shard_config_for_path(
                &source.file_system_id,
                &authorities,
                &path(source),
            )
            .map_err(DfsError::from)?
                && shard.primary_node_id != *self.node_id()
            {
                return Ok((None, None, 0));
            }
        }
        let Some(object) = object else {
            return Ok((
                None,
                Some(crate::dfs_now_unix_nanos().saturating_add(60 * 60 * 1_000_000_000)),
                0,
            ));
        };
        let (_, candidates, policy) = self.managed_replication_policy_for_path(
            &object.path,
            object.policy.clone(),
            object.current_locations.len().max(1),
        )?;
        let options = DfsWriteOptions {
            policy,
            replica_candidates: if candidates.is_empty() {
                vec![self.node_id().clone()]
            } else {
                candidates
                    .into_iter()
                    .filter(|c| c.accepts_content)
                    .map(|c| c.node_id)
                    .collect()
            },
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        let (base, current) = self.read_attachment_catalog(source)?;
        let mut catalog = (*current).clone();
        let now = crate::dfs_now_unix_nanos();
        catalog.generations.retain(|_, g| {
            object.kind != DfsObjectKind::Tombstone
                && g.expires_at > now
                && g.request
                    .metadata
                    .source_version
                    .as_ref()
                    .is_none_or(|v| v == &object.version)
        });
        catalog.pending.retain(|_, p| {
            object.kind != DfsObjectKind::Tombstone
                && p.expires_at > now
                && p.request
                    .metadata
                    .source_version
                    .as_ref()
                    .is_none_or(|v| v == &object.version)
        });
        if catalog.pending.len() != current.pending.len()
            || catalog.generations.len() != current.generations.len()
        {
            self.write_attachment_catalog(
                source,
                base.as_ref(),
                &catalog,
                options.clone(),
                "expire-uploads",
            )?;
        }
        let _ = self.collect_attachment_garbage(source, options, limit)?;
        let page = match self
            .posix_list_dir_beneath_no_links_distributed_page("/", &directory, after, limit)
        {
            Ok((_, page)) => page,
            Err(DfsPosixError::NotFound { .. }) => return Ok((None, None, 0)),
            Err(error) => return Err(error),
        };
        // A payload may have been created after the initial catalog read.
        // Read publication/claim state AFTER listing; otherwise a concurrent
        // upload could be mistaken for garbage.
        let (_, catalog) = self.read_attachment_catalog(source)?;
        let live: std::collections::BTreeSet<_> = catalog
            .entries
            .values()
            .filter_map(|entry| match &entry.payload {
                Some(Payload::Object(blob)) => Some(blob.path.trim_start_matches('/').to_string()),
                _ => None,
            })
            .chain(
                catalog
                    .pending
                    .values()
                    .map(|p| format!("{}/payload-{}", directory.trim_start_matches('/'), p.token)),
            )
            .collect();
        let mut reclaimed = 0;
        for entry in page.entries {
            let Some(token) = entry.name.strip_prefix("payload-") else {
                continue;
            };
            if DfsObjectId::new(token).is_err() || live.contains(entry.path.trim_start_matches('/'))
            {
                continue;
            }
            self.posix_unlink_if_generation(
                &entry.path,
                format!("attachment-orphan:{}:{}", entry.object_id, entry.version),
                DfsNamespaceObjectPrecondition {
                    object_id: entry.object_id,
                    version: entry.version,
                },
            )?;
            reclaimed += 1;
        }
        Ok((
            page.next_cursor,
            catalog
                .pending
                .values()
                .map(|p| p.expires_at)
                .chain(catalog.generations.values().map(|g| g.expires_at))
                .chain((!catalog.garbage.is_empty()).then_some(now))
                .min(),
            reclaimed,
        ))
    }

    pub(crate) fn remove_deleted_attachment_directory(
        &mut self,
        source: &DfsAttachmentSource,
        directory: &str,
        _after: Option<&DfsDirectoryPageCursor>,
        limit: usize,
    ) -> Result<(Option<DfsDirectoryPageCursor>, Option<u64>, usize)> {
        let page = match self
            .posix_list_dir_beneath_no_links_distributed_page("/", directory, None, limit)
        {
            Ok((_, page)) => page,
            Err(DfsPosixError::NotFound { .. }) => {
                if directory != dfs_attachment_directory(source) {
                    return self.remove_deleted_attachment_directory(
                        source,
                        &dfs_attachment_directory(source),
                        None,
                        limit,
                    );
                }
                return Ok((None, None, 0));
            }
            Err(e) => return Err(e),
        };
        let mut reclaimed = 0;
        for entry in page.entries {
            let owned = entry.name == "catalog.sobj"
                || entry
                    .name
                    .strip_prefix("payload-")
                    .is_some_and(|token| DfsObjectId::new(token).is_ok())
                || entry
                    .name
                    .strip_suffix(".satt")
                    .and_then(|s| s.split_once('~'))
                    .is_some_and(|(kind, name)| {
                        DfsAttachmentKey {
                            kind: kind.into(),
                            name: name.into(),
                        }
                        .validate()
                        .is_ok()
                    });
            if !owned || entry.kind != DfsObjectKind::File {
                return Err(invalid("unrecognized managed attachment entry"));
            }
            self.posix_unlink_if_generation(
                &entry.path,
                format!("attachment-source-gc:{}:{}", entry.object_id, entry.version),
                DfsNamespaceObjectPrecondition {
                    object_id: entry.object_id,
                    version: entry.version,
                },
            )?;
            reclaimed += 1;
        }
        if page.next_cursor.is_none() {
            match self.posix_rmdir(directory) {
                Ok(_) | Err(DfsPosixError::NotFound { .. }) => {}
                Err(DfsPosixError::DirectoryNotEmpty { .. }) => {
                    return Ok((None, Some(crate::dfs_now_unix_nanos()), reclaimed));
                }
                Err(e) => return Err(e),
            }
            // Legacy objects share the same source identity. Delete them only
            // after the v2 directory has been fenced and drained.
            if directory != dfs_attachment_directory(source) {
                let (cursor, retry, count) = self.remove_deleted_attachment_directory(
                    source,
                    &dfs_attachment_directory(source),
                    None,
                    limit.saturating_sub(reclaimed).max(1),
                )?;
                return Ok((
                    None,
                    if cursor.is_some() || retry.is_some() {
                        Some(crate::dfs_now_unix_nanos())
                    } else {
                        None
                    },
                    reclaimed + count,
                ));
            }
        }
        Ok((
            None,
            page.next_cursor.map(|_| crate::dfs_now_unix_nanos()),
            reclaimed,
        ))
    }

    fn read_attachment_catalog(
        &mut self,
        source: &DfsAttachmentSource,
    ) -> Result<(Option<DfsStat>, Arc<Catalog>)> {
        let path = path(source);
        let stat = match self.posix_open_beneath_no_links_distributed("/", &path) {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. }) => {
                return Ok((None, Arc::new(Catalog::new(source.clone()))));
            }
            Err(e) => return Err(e),
        };
        let size = stat
            .size_bytes
            .ok_or_else(|| invalid("catalog length unavailable"))?;
        if size > DFS_ATTACHMENT_CATALOG_MAX_BYTES as u64 || stat.kind != DfsObjectKind::File {
            return Err(invalid("catalog exceeds bounds"));
        }
        let cache_key = (
            stat.object_id.clone(),
            stat.version.clone(),
            stat.content_hash
                .clone()
                .ok_or_else(|| invalid("catalog hash is unavailable"))?,
        );
        if let Some((catalog, _)) = cache().lock().unwrap().entries.get(&cache_key) {
            if catalog.source.file_system_id != source.file_system_id
                || catalog.source.object_id != source.object_id
            {
                return Err(invalid("catalog cache identity mismatch"));
            }
            return Ok((Some(stat), Arc::clone(catalog)));
        }
        let bytes = self.posix_read_file_range_for_opened_stat(&path, &stat, 0, size as usize)?;
        if bytes.len() as u64 != size {
            return Err(invalid("short catalog read"));
        }
        let catalog: Catalog = codec().decode(&bytes).map_err(invalid)?;
        catalog.validate(source)?;
        let catalog = Arc::new(catalog);
        remember_catalog(&stat, Arc::clone(&catalog), bytes.len());
        Ok((Some(stat), catalog))
    }

    fn write_attachment_catalog(
        &mut self,
        source: &DfsAttachmentSource,
        base: Option<&DfsStat>,
        catalog: &Catalog,
        options: DfsWriteOptions,
        operation: &str,
    ) -> Result<()> {
        let mut timing = crate::service_trace::DfsPhaseTimer::new(operation);
        catalog.validate(source)?;
        let bytes = codec().encode(catalog).map_err(invalid)?;
        if bytes.len() > DFS_ATTACHMENT_CATALOG_MAX_BYTES {
            return Err(invalid("source attachment catalog capacity exhausted"));
        }
        let path = path(source);
        // Every retry against another page generation has a distinct physical
        // intent, while the logical receipt remains the caller's operation ID.
        let physical = format!(
            "attachment-catalog:{}",
            blake3::hash(
                format!(
                    "{operation}:{}:{}",
                    base.map(|b| b.version.as_str()).unwrap_or("absent"),
                    blake3::hash(&bytes)
                )
                .as_bytes()
            )
            .to_hex()
        );
        timing.mark("catalog_encode");
        let encoded_bytes = bytes.len();
        if base.is_none() {
            let directory = dfs_attachment_catalog_directory(source);
            self.posix_mkdir_all(&directory)?;
            self.posix_open_beneath_no_links_distributed("/", &directory)?;
        }
        let deadline = self
            .transport
            .operation_deadline()
            .unwrap_or_else(|| std::time::Instant::now() + std::time::Duration::from_secs(30));
        let committed = loop {
            match self.posix_write_internal_file_from_reader(
                &path,
                std::io::Cursor::new(bytes.as_slice()),
                options.clone(),
                base.map(|s| (&s.object_id, &s.version)),
                &physical,
            ) {
                Ok(committed) => break committed,
                Err(error)
                    if crate::posix::posix_error_is_retryable_write_serialization(&error) =>
                {
                    // Another admitted catalog writer has not settled yet.
                    // This is a known rejection, not an ambiguous delivery.
                    // Preserve this exact physical operation and expected
                    // base while waiting; a changed base returns CAS conflict
                    // to the outer catalog merge/revision check.
                    self.check_cancelled("attachment catalog serialization")?;
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Err(error);
                    }
                    std::thread::sleep(
                        std::time::Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(now)),
                    );
                }
                Err(error) => return Err(error),
            }
        };
        // The successful CAS supplies the immutable version we just encoded.
        // Keep it available to subsequent reads without downloading our own
        // newly committed catalog from another replica.
        remember_catalog(&committed, Arc::new(catalog.clone()), encoded_bytes);
        timing.mark("catalog_commit");
        Ok(())
    }

    pub(crate) fn catalog_attachment_get(
        &mut self,
        source: &DfsAttachmentSource,
        source_stat: &DfsStat,
        keys: &[DfsAttachmentKey],
    ) -> Result<Vec<Option<DfsAttachmentDescriptor>>> {
        let (_, catalog) = self.read_attachment_catalog(source)?;
        keys.iter()
            .map(|wanted| {
                wanted.validate().map_err(invalid)?;
                if let Some(entry) = catalog.entries.get(&key(wanted)) {
                    return Ok(entry
                        .payload
                        .as_ref()
                        .map(|_| current_descriptor(&entry.descriptor, source_stat)));
                }
                Ok(self.attachment_open(source, wanted)?.map(|(stat, header)| {
                    crate::attachments::descriptor(header, &stat, source_stat)
                }))
            })
            .collect()
    }

    pub(crate) fn catalog_attachment_read(
        &mut self,
        source: &DfsAttachmentSource,
        source_stat: &DfsStat,
        wanted: &DfsAttachmentKey,
        expected: &DfsAttachmentRevision,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<DfsAttachmentResponse>> {
        let (_, catalog) = self.read_attachment_catalog(source)?;
        let Some(entry) = catalog.entries.get(&key(wanted)) else {
            return Ok(None);
        };
        let Some(payload) = entry.payload.as_ref() else {
            return Err(invalid("attachment has been deleted"));
        };
        if &entry.descriptor.revision != expected
            || !current_descriptor(&entry.descriptor, source_stat).current
        {
            return Err(conflict(source, "attachment or source revision changed"));
        }
        if offset > entry.descriptor.payload_bytes {
            return Err(invalid("attachment offset exceeds length"));
        }
        let len = max_bytes.min((entry.descriptor.payload_bytes - offset) as usize);
        let bytes = match payload {
            Payload::Inline(bytes) => bytes[offset as usize..offset as usize + len].to_vec(),
            Payload::Object(blob) => {
                let stat = self.posix_open_beneath_no_links_distributed("/", &blob.path)?;
                if stat.object_id != blob.object_id
                    || stat.version != blob.version
                    || stat.content_hash.as_ref() != Some(&blob.content_hash)
                    || stat.size_bytes != Some(blob.size)
                {
                    return Err(conflict(source, "immutable attachment payload changed"));
                }
                self.posix_read_file_range_for_opened_stat(&blob.path, &stat, offset, len)?
            }
        };
        if bytes.len() != len {
            return Err(invalid("short attachment range"));
        }
        Ok(Some(DfsAttachmentResponse::Data {
            eof: offset + bytes.len() as u64 == entry.descriptor.payload_bytes,
            bytes,
        }))
    }

    pub(crate) fn catalog_attachment_put<R: Read + Seek>(
        &mut self,
        actor: &str,
        request: DfsAttachmentUploadRequest,
        mut reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsAttachmentResponse> {
        let mut timing = crate::service_trace::DfsPhaseTimer::new(&request.operation_id);
        request.validate().map_err(invalid)?;
        let policy = match self.control_plane.as_mut() {
            Some(client) => read_attachment_policy(Some(client.as_mut()))?,
            None => DfsAttachmentPolicy::default(),
        };
        timing.mark("storage_policy");
        let hash = crate::attachments::validate_attachment_reader(
            &request.metadata,
            &mut reader,
            request.payload_bytes,
        )?;
        let fingerprint = blake3::hash(
            &serde_json::to_vec(&(
                actor,
                &request.metadata,
                &request.expected,
                &request.operation_id,
                &hash,
                request.payload_bytes,
                &request.source.object_id,
                &request.source.file_system_id,
            ))
            .map_err(invalid)?,
        )
        .to_hex()
        .to_string();
        let mut inline = None;
        if request.payload_bytes <= DFS_ATTACHMENT_INLINE_BYTES as u64 {
            let mut bytes = Vec::with_capacity(request.payload_bytes as usize);
            reader
                .by_ref()
                .take(request.payload_bytes + 1)
                .read_to_end(&mut bytes)
                .map_err(invalid)?;
            if bytes.len() as u64 != request.payload_bytes
                || format!("blake3:{}", blake3::hash(&bytes).to_hex()) != hash
            {
                return Err(invalid("attachment body changed during upload"));
            }
            inline = Some(bytes);
        }
        timing.mark("payload_validation");
        let source = &request.source;
        let name = key(&request.metadata.key);
        let mut pending = None;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let source_stat = self.attachment_source(source)?;
            timing.mark("source_open");
            let (base, current) = self.read_attachment_catalog(source)?;
            timing.mark("catalog_open");
            if let Some(result) = current.receipt(&request.operation_id, &fingerprint)? {
                return Ok(refresh_result(result, &source_stat));
            }
            if let Some(entry) = current.entries.get(&name) {
                if entry.descriptor.operation_id == request.operation_id {
                    if entry.fingerprint != fingerprint {
                        return Err(invalid("operation id reused with different input"));
                    }
                    return Ok(DfsAttachmentResponse::Stored {
                        attachment: current_descriptor(&entry.descriptor, &source_stat),
                    });
                }
            }
            validate_generation(&current, actor, &request)?;
            if request
                .metadata
                .source_version
                .as_ref()
                .is_some_and(|v| v != &source_stat.version)
            {
                return Err(conflict(
                    source,
                    "source version changed before publication",
                ));
            }
            if let Some(claim) = current.pending.get(&name) {
                if claim.request.operation_id == request.operation_id
                    && claim.fingerprint == fingerprint
                    && claim.actor == actor
                {
                    pending = Some(claim.clone());
                    break;
                }
                return Err(conflict(
                    source,
                    "attachment generation already in progress",
                ));
            }
            let mut next = (*current).clone();
            let previous = next.entries.get(&name).filter(|e| e.payload.is_some());
            let legacy = if !next.entries.contains_key(&name) {
                self.attachment_open(source, &request.metadata.key)?
            } else {
                None
            };
            let observed = previous.map(|e| e.descriptor.revision.clone()).or_else(|| {
                legacy.as_ref().map(|(s, _)| DfsAttachmentRevision {
                    object_id: s.object_id.clone(),
                    version: s.version.clone(),
                })
            });
            if observed != request.expected {
                return Err(conflict(
                    source,
                    "attachment revision changed; reread before retrying",
                ));
            }
            let owner = previous
                .map(|e| e.owner.clone())
                .unwrap_or_else(|| actor.to_string());
            require_owner(actor, &owner)?;
            let revision = DfsAttachmentRevision {
                object_id: observed
                    .as_ref()
                    .map(|r| r.object_id.clone())
                    .unwrap_or_else(|| {
                        DfsObjectId::derived(
                            "attachment",
                            format!("{}:{}:{name}", source.file_system_id, source.object_id),
                        )
                    }),
                version: DfsVersion::derived("attachment-version", &fingerprint),
            };
            let descriptor = DfsAttachmentDescriptor {
                metadata: request.metadata.clone(),
                revision: revision.clone(),
                operation_id: request.operation_id.clone(),
                payload_bytes: request.payload_bytes,
                current: true,
            };
            let other_inline_bytes = current
                .entries
                .iter()
                .filter(|(k, _)| *k != &name)
                .filter_map(|(_, entry)| match &entry.payload {
                    Some(Payload::Inline(bytes)) => Some(bytes.len()),
                    _ => None,
                })
                .sum::<usize>();
            if let Some(bytes) = inline.as_ref().filter(|bytes| {
                other_inline_bytes.saturating_add(bytes.len()) <= DFS_ATTACHMENT_INLINE_BUDGET_BYTES
            }) {
                if let Some(entry) = previous {
                    if let Some(Payload::Object(blob)) = &entry.payload {
                        next.garbage.push(blob.clone());
                    }
                }
                if let Some((stat, _)) = legacy {
                    next.garbage.push(AttachmentBlob::from_stat(&stat)?);
                }
                next.generations.remove(&name);
                next.entries.insert(
                    name.clone(),
                    Entry {
                        descriptor: descriptor.clone(),
                        owner: owner.clone(),
                        fingerprint: fingerprint.clone(),
                        payload: Some(Payload::Inline(bytes.clone())),
                    },
                );
                next.quota(&owner, policy.producer(&owner))?;
                let result = DfsAttachmentResponse::Stored {
                    attachment: descriptor,
                };
                next.record_receipt(
                    request.operation_id.clone(),
                    fingerprint.clone(),
                    result.clone(),
                );
                match self.write_attachment_catalog(
                    source,
                    base.as_ref(),
                    &next,
                    options.clone(),
                    &request.operation_id,
                ) {
                    Ok(()) => {
                        return Ok(result);
                    }
                    Err(DfsPosixError::Conflict { .. } | DfsPosixError::AlreadyExists { .. }) => {
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                let claim = Pending {
                    request: request.clone(),
                    actor: actor.into(),
                    owner: owner.clone(),
                    fingerprint: fingerprint.clone(),
                    payload_hash: hash.clone(),
                    revision,
                    token: sinomo_core::Id::generate().to_string(),
                    expires_at: current
                        .generations
                        .get(&name)
                        .map(|g| g.expires_at)
                        .unwrap_or_else(|| crate::dfs_now_unix_nanos().saturating_add(CLAIM_NANOS)),
                };
                next.pending.insert(name.clone(), claim.clone());
                next.quota(&owner, policy.producer(&owner))?;
                match self.write_attachment_catalog(
                    source,
                    base.as_ref(),
                    &next,
                    options.clone(),
                    &format!("claim:{}", request.operation_id),
                ) {
                    Ok(()) => {
                        pending = Some(claim);
                        break;
                    }
                    Err(DfsPosixError::Conflict { .. } | DfsPosixError::AlreadyExists { .. }) => {
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        let claim = pending.ok_or_else(|| conflict(source, "catalog remained contended"))?;
        if claim.expires_at <= crate::dfs_now_unix_nanos() {
            return Err(conflict(source, "upload lease expired"));
        }
        let payload_path = format!(
            "{}/payload-{}",
            dfs_attachment_catalog_directory(source),
            claim.token
        );
        reader.rewind().map_err(invalid)?;
        let stat = self.posix_create_file_from_reader_with_operation_id(
            &payload_path,
            reader.take(request.payload_bytes + 1),
            options.clone(),
            &format!("attachment-payload:{}", claim.token),
        )?;
        let blob = AttachmentBlob::from_stat(&stat)?;
        if blob.size != request.payload_bytes || blob.content_hash != hash {
            return Err(invalid("attachment body changed during upload"));
        }
        for _ in 0..MAX_CAS_ATTEMPTS {
            let source_stat = self.attachment_source(source)?;
            let (base, current) = self.read_attachment_catalog(source)?;
            if let Some(result) = current.receipt(&request.operation_id, &fingerprint)? {
                return Ok(refresh_result(result, &source_stat));
            }
            validate_generation(&current, actor, &request)?;
            if claim.expires_at <= crate::dfs_now_unix_nanos() {
                return Err(conflict(source, "upload lease expired"));
            }
            if current
                .pending
                .get(&name)
                .is_none_or(|p| p.token != claim.token)
            {
                return Err(conflict(source, "upload lease was superseded"));
            }
            let mut next = (*current).clone();
            let old = next.entries.get(&name);
            if let Some(Payload::Object(old)) = old.and_then(|e| e.payload.as_ref()) {
                next.garbage.push(old.clone());
            }
            if old.is_none() {
                if let Some((stat, _)) = self.attachment_open(source, &request.metadata.key)? {
                    next.garbage.push(AttachmentBlob::from_stat(&stat)?);
                }
            }
            let descriptor = DfsAttachmentDescriptor {
                metadata: request.metadata.clone(),
                revision: claim.revision.clone(),
                operation_id: request.operation_id.clone(),
                payload_bytes: request.payload_bytes,
                current: request
                    .metadata
                    .source_version
                    .as_ref()
                    .is_none_or(|v| v == &source_stat.version),
            };
            next.pending.remove(&name);
            next.generations.remove(&name);
            next.entries.insert(
                name.clone(),
                Entry {
                    descriptor: descriptor.clone(),
                    owner: claim.owner.clone(),
                    fingerprint: fingerprint.clone(),
                    payload: Some(Payload::Object(blob.clone())),
                },
            );
            next.quota(&claim.owner, policy.producer(&claim.owner))?;
            let result = DfsAttachmentResponse::Stored {
                attachment: descriptor,
            };
            next.record_receipt(
                request.operation_id.clone(),
                fingerprint.clone(),
                result.clone(),
            );
            match self.write_attachment_catalog(
                source,
                base.as_ref(),
                &next,
                options.clone(),
                &request.operation_id,
            ) {
                Ok(()) => {
                    return Ok(result);
                }
                Err(DfsPosixError::Conflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict(
            source,
            "catalog remained contended during publication",
        ))
    }

    pub(crate) fn catalog_attachment_delete(
        &mut self,
        actor: &str,
        source: &DfsAttachmentSource,
        wanted: &DfsAttachmentKey,
        expected: &DfsAttachmentRevision,
        operation_id: &str,
        options: DfsWriteOptions,
    ) -> Result<DfsAttachmentResponse> {
        wanted.validate().map_err(invalid)?;
        validate_attachment_operation_id(operation_id).map_err(invalid)?;
        let fingerprint = blake3::hash(
            &serde_json::to_vec(&("delete", actor, wanted, expected, operation_id))
                .map_err(invalid)?,
        )
        .to_hex()
        .to_string();
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (base, current) = self.read_attachment_catalog(source)?;
            if let Some(result) = current.receipt(operation_id, &fingerprint)? {
                return Ok(result);
            }
            let name = key(wanted);
            if current.pending.contains_key(&name)
                || current
                    .generations
                    .get(&name)
                    .is_some_and(|g| g.expires_at > crate::dfs_now_unix_nanos())
            {
                return Err(conflict(source, "attachment upload is in progress"));
            }
            let mut next = (*current).clone();
            if let Some(entry) = next.entries.get_mut(&name) {
                if entry.payload.is_none() || &entry.descriptor.revision != expected {
                    return Err(conflict(source, "attachment revision changed"));
                }
                require_owner(actor, &entry.owner)?;
                if let Some(Payload::Object(blob)) = entry.payload.take() {
                    next.garbage.push(blob);
                }
            } else {
                let (stat, header) = self
                    .attachment_open(source, wanted)?
                    .ok_or_else(|| invalid("attachment does not exist"))?;
                if stat.object_id != expected.object_id || stat.version != expected.version {
                    return Err(conflict(source, "attachment revision changed"));
                }
                next.garbage.push(AttachmentBlob::from_stat(&stat)?);
                next.entries.insert(
                    name,
                    Entry {
                        descriptor: crate::attachments::descriptor(
                            header,
                            &stat,
                            &self.attachment_source(source)?,
                        ),
                        owner: actor.into(),
                        fingerprint: fingerprint.clone(),
                        payload: None,
                    },
                );
            }
            next.record_receipt(
                operation_id.into(),
                fingerprint.clone(),
                DfsAttachmentResponse::Deleted,
            );
            match self.write_attachment_catalog(
                source,
                base.as_ref(),
                &next,
                options.clone(),
                operation_id,
            ) {
                Ok(()) => {
                    return Ok(DfsAttachmentResponse::Deleted);
                }
                Err(DfsPosixError::Conflict { .. } | DfsPosixError::AlreadyExists { .. }) => {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(conflict(source, "catalog remained contended"))
    }

    pub(crate) fn collect_attachment_garbage(
        &mut self,
        source: &DfsAttachmentSource,
        options: DfsWriteOptions,
        limit: usize,
    ) -> Result<usize> {
        let (base, current) = self.read_attachment_catalog(source)?;
        let Some(base) = base else {
            return Ok(0);
        };
        let mut next = (*current).clone();
        let mut removed = Vec::new();
        for blob in current.garbage.iter().take(limit) {
            match self.posix_open_beneath_no_links_distributed("/", &blob.path) {
                Ok(stat) if stat.object_id == blob.object_id && stat.version == blob.version => {
                    self.posix_unlink_if_generation(
                        &blob.path,
                        format!("attachment-gc:{}:{}", blob.object_id, blob.version),
                        DfsNamespaceObjectPrecondition {
                            object_id: blob.object_id.clone(),
                            version: blob.version.clone(),
                        },
                    )?;
                }
                Ok(_) | Err(DfsPosixError::NotFound { .. }) => {}
                Err(e) => return Err(e),
            }
            removed.push(blob.clone());
        }
        if removed.is_empty() {
            return Ok(0);
        }
        next.garbage.retain(|blob| !removed.contains(blob));
        self.write_attachment_catalog(source, Some(&base), &next, options, "gc")?;
        Ok(removed.len())
    }

    pub(crate) fn catalog_attachment_view(
        &mut self,
        source: &DfsAttachmentSource,
    ) -> Result<AttachmentCatalogView> {
        let (_, catalog) = self.read_attachment_catalog(source)?;
        Ok(AttachmentCatalogView(catalog))
    }
}

fn current_descriptor(
    descriptor: &DfsAttachmentDescriptor,
    source: &DfsStat,
) -> DfsAttachmentDescriptor {
    let mut descriptor = descriptor.clone();
    descriptor.current = descriptor
        .metadata
        .source_version
        .as_ref()
        .is_none_or(|v| v == &source.version);
    descriptor
}
fn refresh_result(result: DfsAttachmentResponse, source: &DfsStat) -> DfsAttachmentResponse {
    match result {
        DfsAttachmentResponse::Stored { attachment } => DfsAttachmentResponse::Stored {
            attachment: current_descriptor(&attachment, source),
        },
        result => result,
    }
}
fn require_owner(actor: &str, owner: &str) -> Result<()> {
    if actor.starts_with("component:") && actor != owner {
        return Err(invalid(
            "a component cannot replace another producer's attachment",
        ));
    }
    Ok(())
}

/// sinomo:kv-consistency: Authorize attachment resource grants against the current global quota policy before admission.
pub(crate) fn read_attachment_policy(
    client: Option<&mut dyn DfsControlPlaneKvClient>,
) -> Result<DfsAttachmentPolicy> {
    let Some(client) = client else {
        return Ok(DfsAttachmentPolicy::default());
    };
    let definition = dfs_attachment_policy_definition();
    let values = client
        .get_authoritative_consistent(&[definition.cluster_key()])
        .map_err(invalid)?;
    let policy = match values.into_iter().next().flatten() {
        Some(entry) => serde_json::from_value(
            sinomo_config::decode_cluster_config_entry(&entry, &definition).map_err(invalid)?,
        )
        .map_err(invalid)?,
        None => DfsAttachmentPolicy::default(),
    };
    policy.validate().map_err(invalid)?;
    Ok(policy)
}

fn validate_generation(
    catalog: &Catalog,
    actor: &str,
    request: &DfsAttachmentUploadRequest,
) -> Result<()> {
    let active = catalog
        .generations
        .get(&key(&request.metadata.key))
        .filter(|g| g.expires_at > crate::dfs_now_unix_nanos());
    if !request.generation {
        if active.is_some() {
            return Err(conflict(
                &request.source,
                "attachment generation is reserved",
            ));
        }
        return Ok(());
    }
    let Some(generation) = active else {
        return Err(conflict(
            &request.source,
            "generation lease expired or was superseded",
        ));
    };
    if generation.actor != actor
        || generation.request.operation_id != request.operation_id
        || generation.request.metadata != request.metadata
        || generation.request.expected != request.expected
        || request.payload_bytes > generation.request.payload_bytes
    {
        return Err(conflict(
            &request.source,
            "publication does not match its generation lease",
        ));
    }
    Ok(())
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn claim_attachment_generation(
        &mut self,
        actor: &str,
        request: DfsAttachmentUploadRequest,
        options: DfsWriteOptions,
    ) -> Result<DfsAttachmentResponse> {
        request.validate().map_err(invalid)?;
        if !request.generation || request.metadata.source_version.is_none() {
            return Err(invalid(
                "generation claims require version-bound data and generation=true",
            ));
        }
        let policy = match self.control_plane.as_mut() {
            Some(client) => read_attachment_policy(Some(client.as_mut()))?,
            None => DfsAttachmentPolicy::default(),
        };
        let source = &request.source;
        let name = key(&request.metadata.key);
        for _ in 0..MAX_CAS_ATTEMPTS {
            let stat = self.attachment_source(source)?;
            if request.metadata.source_version.as_ref() != Some(&stat.version) {
                return Err(conflict(source, "source changed before generation"));
            }
            let (base, current) = self.read_attachment_catalog(source)?;
            let previous = current.entries.get(&name).filter(|e| e.payload.is_some());
            if let Some(entry) = previous {
                if entry.descriptor.metadata == request.metadata
                    && current_descriptor(&entry.descriptor, &stat).current
                    && request.expected.as_ref() != Some(&entry.descriptor.revision)
                {
                    return Ok(DfsAttachmentResponse::Descriptor {
                        attachment: Some(current_descriptor(&entry.descriptor, &stat)),
                    });
                }
                require_owner(actor, &entry.owner)?;
            }
            if let Some(generation) = current
                .generations
                .get(&name)
                .filter(|g| g.expires_at > crate::dfs_now_unix_nanos())
            {
                let same = generation.actor == actor
                    && generation.request.operation_id == request.operation_id
                    && generation.request.metadata == request.metadata
                    && generation.request.expected == request.expected
                    && generation.request.payload_bytes == request.payload_bytes;
                return Ok(DfsAttachmentResponse::Generation {
                    granted: same,
                    expires_at_unix_nanos: generation.expires_at,
                });
            }
            if let Some(pending) = current.pending.get(&name) {
                return Ok(DfsAttachmentResponse::Generation {
                    granted: false,
                    expires_at_unix_nanos: pending.expires_at,
                });
            }
            let legacy = if !current.entries.contains_key(&name) {
                self.attachment_open(source, &request.metadata.key)?
            } else {
                None
            };
            let observed = previous.map(|e| e.descriptor.revision.clone()).or_else(|| {
                legacy.map(|(s, _)| DfsAttachmentRevision {
                    object_id: s.object_id,
                    version: s.version,
                })
            });
            if observed != request.expected {
                return Err(conflict(
                    source,
                    "attachment revision changed before generation",
                ));
            }
            // A trusted node/operator may refresh a slot from another node,
            // while the original owner retains its storage accounting.
            let owner = previous
                .map(|e| e.owner.clone())
                .unwrap_or_else(|| actor.into());
            let mut next = (*current).clone();
            let expires_at = crate::dfs_now_unix_nanos().saturating_add(CLAIM_NANOS);
            next.generations.insert(
                name.clone(),
                Generation {
                    request: request.clone(),
                    actor: actor.into(),
                    owner: owner.clone(),
                    expires_at,
                },
            );
            next.quota(&owner, policy.producer(&owner))?;
            match self.write_attachment_catalog(
                source,
                base.as_ref(),
                &next,
                options.clone(),
                &format!("generation:{}", request.operation_id),
            ) {
                Ok(()) => {
                    return Ok(DfsAttachmentResponse::Generation {
                        granted: true,
                        expires_at_unix_nanos: expires_at,
                    });
                }
                Err(DfsPosixError::Conflict { .. } | DfsPosixError::AlreadyExists { .. }) => {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(conflict(
            source,
            "catalog remained contended during generation claim",
        ))
    }
}
