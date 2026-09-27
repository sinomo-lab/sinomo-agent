//! Durable change-feed checkpoints and bounded reconciliation for attachment
//! cleanup. This is DFS-owned maintenance; browser reads never drive it.
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const ROOTS: [&str; 4] = [
    "/.attachments/v2",
    "/services/.attachments/v2",
    "/users/.attachments/v2",
    "/.attachments/v1",
];
const MAX_TASKS: usize = 1024;
const RETRY_NANOS: u64 = 10 * 1_000_000_000;
const FEED_POLL_NANOS: u64 = 10 * 1_000_000_000;
const AUTHORITY_RECHECK_NANOS: u64 = 60 * 1_000_000_000;
const RESCAN_NANOS: u64 = 60 * 60 * 1_000_000_000;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct State {
    feeds: BTreeMap<String, Feed>,
    tasks: VecDeque<Task>,
    scan_root: usize,
    frames: Vec<Frame>,
    known_catalogs: bool,
    next_due: Option<u64>,
    #[serde(default)]
    next_scan: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Feed {
    cursor: DfsNamespaceChangeCursor,
    partial: Option<(u64, u64)>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Frame {
    path: String,
    after: Option<DfsDirectoryPageCursor>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Task {
    source: DfsAttachmentSource,
    after: Option<DfsDirectoryPageCursor>,
    due: u64,
}

struct Cached {
    state: Mutex<State>,
    hints: Mutex<Vec<DfsAttachmentSource>>,
    wake: AtomicBool,
    running: AtomicBool,
    notify: Arc<tokio::sync::Notify>,
}

fn state_path(data_dir: &sinomo_core::local::SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("attachment-maintenance.sobj")
}
fn cached(path: &Path) -> Result<Arc<Cached>, DfsError> {
    static CACHE: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Cached>>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    if let Some(value) = cache.get(path) {
        return Ok(Arc::clone(value));
    }
    let loaded = if std::fs::metadata(path).is_ok_and(|m| m.len() > 16 * 1024 * 1024) {
        Err(crate::MetadataBackendError::Serde(
            "oversized attachment maintenance checkpoint".into(),
        ))
    } else {
        crate::metadata_io::read_sobj_optional::<State>(path)
    };
    let state = match loaded {
        Ok(state) => state.unwrap_or_default(),
        Err(crate::MetadataBackendError::Serde(_)) => {
            let archive = path
                .parent()
                .unwrap()
                .join("attachment-maintenance-quarantine");
            std::fs::create_dir_all(&archive).map_err(|e| DfsError::Metadata {
                message: e.to_string(),
            })?;
            std::fs::rename(
                path,
                archive.join(format!("{}.sobj", sinomo_core::Id::generate())),
            )
            .map_err(|e| DfsError::Metadata {
                message: e.to_string(),
            })?;
            State::default()
        }
        Err(error) => return Err(error.into()),
    };
    if state.tasks.len() > MAX_TASKS || state.frames.len() > 12 || state.feeds.len() > 4096 {
        return Err(DfsError::Metadata {
            message: "attachment maintenance checkpoint exceeds bounds".into(),
        });
    }
    let value = Arc::new(Cached {
        state: Mutex::new(state),
        hints: Mutex::new(Vec::new()),
        wake: AtomicBool::new(true),
        running: AtomicBool::new(false),
        notify: Arc::new(tokio::sync::Notify::new()),
    });
    if cache.len() >= 64 {
        let old = cache
            .iter()
            .find(|(_, value)| {
                Arc::strong_count(value) == 1 && !value.running.load(Ordering::Acquire)
            })
            .map(|(key, _)| key.clone());
        if let Some(old) = old {
            cache.remove(&old);
        }
    }
    cache.insert(path.to_path_buf(), Arc::clone(&value));
    Ok(value)
}

pub(crate) fn wake(data_dir: &sinomo_core::local::SinomoDataDir, source: &DfsAttachmentSource) {
    if let Ok(cache) = cached(&state_path(data_dir)) {
        let mut hints = cache.hints.lock().unwrap();
        if hints.len() < MAX_TASKS
            && !hints.iter().any(|s| {
                s.object_id == source.object_id && s.file_system_id == source.file_system_id
            })
        {
            hints.push(source.clone());
        }
        cache.wake.store(true, Ordering::Release);
        cache.notify.notify_one();
    }
}

pub(crate) fn notify(data_dir: &sinomo_core::local::SinomoDataDir) {
    if let Ok(cache) = cached(&state_path(data_dir)) {
        cache.wake.store(true, Ordering::Release);
        cache.notify.notify_one();
    }
}

pub(crate) fn notifier(
    data_dir: &sinomo_core::local::SinomoDataDir,
) -> Result<Arc<tokio::sync::Notify>, DfsError> {
    Ok(Arc::clone(&cached(&state_path(data_dir))?.notify))
}

pub(crate) fn checkpoint_path(data_dir: &sinomo_core::local::SinomoDataDir) -> PathBuf {
    state_path(data_dir)
}

pub(crate) fn defer_without_authority(data_dir: &sinomo_core::local::SinomoDataDir) {
    if let Ok(cache) = cached(&state_path(data_dir)) {
        cache.wake.store(false, Ordering::Release);
        cache.hints.lock().unwrap().clear();
        cache.state.lock().unwrap().next_due =
            Some(crate::dfs_now_unix_nanos().saturating_add(AUTHORITY_RECHECK_NANOS));
    }
}

pub(crate) fn defer_after_error(data_dir: &sinomo_core::local::SinomoDataDir) {
    if let Ok(cache) = cached(&state_path(data_dir)) {
        cache.state.lock().unwrap().next_due =
            Some(crate::dfs_now_unix_nanos().saturating_add(RETRY_NANOS));
    }
}

pub(crate) fn deadline(
    data_dir: &sinomo_core::local::SinomoDataDir,
) -> Result<Option<u64>, DfsError> {
    let cache = cached(&state_path(data_dir))?;
    if cache.wake.load(Ordering::Acquire) {
        return Ok(Some(0));
    }
    let due = cache.state.lock().unwrap().next_due;
    Ok(due)
}

fn managed_source(path: &str, file_system_id: &DfsFileSystemId) -> Option<DfsAttachmentSource> {
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    let marker = parts.iter().position(|p| *p == ".attachments")?;
    let identity = parts.get(marker + 4)?;
    let (fs, object) = identity.split_once('-')?;
    let source = DfsAttachmentSource {
        path: String::new(),
        file_system_id: DfsFileSystemId::new(fs).ok()?,
        object_id: DfsObjectId::new(object).ok()?,
    };
    if &source.file_system_id != file_system_id {
        return None;
    }
    let directory = parts[..=marker + 4].join("/");
    if directory != dfs_attachment_catalog_directory(&source).trim_start_matches('/')
        && directory != dfs_attachment_directory(&source).trim_start_matches('/')
    {
        return None;
    }
    Some(source)
}

fn enqueue(state: &mut State, source: DfsAttachmentSource, now: u64) -> bool {
    if let Some(task) = state.tasks.iter_mut().find(|t| {
        t.source.object_id == source.object_id && t.source.file_system_id == source.file_system_id
    }) {
        task.due = task.due.min(now);
        return true;
    }
    if state.tasks.len() >= MAX_TASKS {
        return false;
    }
    state.tasks.push_back(Task {
        source,
        after: None,
        due: now,
    });
    true
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DfsAttachmentMaintenanceReport {
    pub sources_processed: usize,
    pub reclaimed_payloads: usize,
    pub pending_sources: usize,
    pub errors: Vec<String>,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn maintain_file_attachments_bounded(
        &mut self,
        checkpoint: &Path,
        shards: &[DfsMetadataShardConfig],
        max_sources: usize,
    ) -> Result<DfsAttachmentMaintenanceReport, DfsError> {
        self.maintain_file_attachments_bounded_at(
            checkpoint,
            shards,
            max_sources,
            crate::dfs_now_unix_nanos(),
        )
    }

    pub(crate) fn maintain_file_attachments_bounded_at(
        &mut self,
        checkpoint: &Path,
        shards: &[DfsMetadataShardConfig],
        max_sources: usize,
        now: u64,
    ) -> Result<DfsAttachmentMaintenanceReport, DfsError> {
        let cache = cached(checkpoint)?;
        if cache.running.swap(true, Ordering::AcqRel) {
            return Ok(Default::default());
        }
        struct Running(Arc<Cached>);
        impl Drop for Running {
            fn drop(&mut self) {
                self.0.running.store(false, Ordering::Release);
            }
        }
        let _running = Running(Arc::clone(&cache));
        let forced = cache.wake.swap(false, Ordering::AcqRel);
        let mut state = cache.state.lock().unwrap().clone();
        if !forced && state.next_due.is_none_or(|due| due > now) {
            return Ok(Default::default());
        }
        let original = state.clone();
        for source in cache.hints.lock().unwrap().drain(..) {
            if !enqueue(&mut state, source, now) {
                state.scan_root = 0;
                state.frames.clear();
            }
            state.known_catalogs = true;
        }
        let local_shards = if shards.is_empty() {
            vec![DfsMetadataShardConfig {
                file_system_id: sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: self.node_id().clone(),
                replica_node_ids: vec![],
                epoch: 0,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            }]
        } else {
            shards
                .iter()
                .filter(|s| s.primary_node_id == *self.node_id())
                .cloned()
                .collect()
        };
        let mut report = DfsAttachmentMaintenanceReport::default();
        let mut feed_more = false;
        for shard in local_shards.iter().take(64) {
            let mut feed = state.feeds.get(&shard.shard_id).cloned().unwrap_or(Feed {
                cursor: DfsNamespaceChangeCursor::from_shard_config(shard, 0),
                partial: None,
            });
            let batch = match self.metadata_backend_mut() {
                Some(backend) => {
                    // The mounted worker owns a long-lived handle, while
                    // message lanes commit through sibling handles. Refresh
                    // this exact shard's durable view before reading its
                    // cached prefix proof and projecting new journal entries.
                    backend.refresh_metadata_change_feed_view(&DfsMetadataShardId::new(
                        &shard.shard_id,
                    ))?;
                    crate::poll_dfs_namespace_changes(backend, &feed.cursor, shard, "/")
                }
                None => continue,
            }?;
            if batch.rescan_required() {
                state.scan_root = 0;
                state.frames.clear();
                feed.cursor =
                    DfsNamespaceChangeCursor::from_shard_config(shard, batch.journal_head_sequence);
                feed.partial = None;
            } else {
                let mut full = true;
                for event in batch.events {
                    let (source, provenance) = match event {
                        DfsNamespaceChangeEvent::Remove {
                            path,
                            object_id,
                            provenance,
                            ..
                        } => {
                            let source =
                                managed_source(&path, &shard.file_system_id).or_else(|| {
                                    (!path.split('/').any(|p| p == ".attachments")).then_some(
                                        DfsAttachmentSource {
                                            path,
                                            file_system_id: shard.file_system_id.clone(),
                                            object_id,
                                        },
                                    )
                                });
                            (source, provenance)
                        }
                        DfsNamespaceChangeEvent::Upsert {
                            path, provenance, ..
                        } => (managed_source(&path, &shard.file_system_id), provenance),
                        _ => continue,
                    };
                    if feed.partial.is_some_and(|p| {
                        (provenance.journal_sequence, provenance.journal_ordinal) <= p
                    }) {
                        continue;
                    }
                    if let Some(source) = source {
                        if !enqueue(&mut state, source, now) {
                            full = false;
                            feed_more = true;
                            break;
                        }
                    }
                    feed.partial = Some((provenance.journal_sequence, provenance.journal_ordinal));
                }
                if full {
                    feed.cursor = batch.next_cursor;
                    feed.partial = None;
                    feed_more |= batch.has_more;
                }
            }
            state.feeds.insert(shard.shard_id.clone(), feed);
        }
        // Reconciliation covers missed hints, compaction gaps and late orphan
        // uploads. It scans only managed directories, never the user namespace.
        if state.scan_root >= ROOTS.len() && state.next_scan <= now && state.tasks.is_empty() {
            state.scan_root = 0;
        }
        let was_scanning = state.scan_root < ROOTS.len() || !state.frames.is_empty();
        for _ in 0..16 {
            if state.tasks.len() >= MAX_TASKS {
                break;
            }
            if state.frames.is_empty() {
                if state.scan_root >= ROOTS.len() {
                    break;
                }
                let root = ROOTS[state.scan_root];
                state.scan_root += 1;
                // Catalog placement is independent of source placement. A
                // gap in this primary's source feed can therefore require
                // reconciliation in any managed attachment namespace. Keep
                // the scan paged and bounded, but do not omit foreign roots.
                state.frames.push(Frame {
                    path: root.into(),
                    after: None,
                });
            }
            let mut frame = state.frames.pop().unwrap();
            let page = match self.posix_list_dir_beneath_no_links_distributed_page(
                "/",
                &frame.path,
                frame.after.as_ref(),
                1,
            ) {
                Ok((_, page)) => page,
                Err(DfsPosixError::NotFound { .. }) => continue,
                Err(e) => {
                    report.errors.push(e.to_string());
                    state.frames.push(frame);
                    break;
                }
            };
            if let Some(after) = page.next_cursor {
                frame.after = Some(after);
                state.frames.push(frame);
            }
            for entry in page.entries {
                if entry.kind != DfsObjectKind::Directory {
                    continue;
                }
                if let Some(source) = managed_source(&entry.path, &sifs_file_system_id()) {
                    state.known_catalogs = true;
                    enqueue(&mut state, source, now);
                } else if entry.path.split('/').count() <= 8 {
                    state.frames.push(Frame {
                        path: entry.path,
                        after: None,
                    });
                }
            }
        }
        let tasks = state.tasks.len().min(max_sources.min(32));
        for _ in 0..tasks {
            let mut task = state.tasks.pop_front().unwrap();
            if task.due > now {
                state.tasks.push_back(task);
                continue;
            }
            match self.maintain_attachment_source_in_background(
                &task.source,
                task.after.as_ref(),
                8,
            ) {
                Ok((after, due, reclaimed)) => {
                    report.sources_processed += 1;
                    report.reclaimed_payloads += reclaimed;
                    task.after = after;
                    if task.after.is_some() || due.is_some() {
                        task.due = if task.after.is_some() {
                            now
                        } else {
                            due.unwrap()
                        };
                        state.tasks.push_back(task);
                    }
                }
                Err(error) => {
                    report.errors.push(error.to_string());
                    task.due = now.saturating_add(RETRY_NANOS);
                    state.tasks.push_back(task);
                }
            }
        }
        report.pending_sources = state.tasks.len();
        if was_scanning && state.scan_root >= ROOTS.len() && state.frames.is_empty() {
            state.next_scan = now.saturating_add(RESCAN_NANOS);
        }
        state.next_due = if feed_more || state.scan_root < ROOTS.len() || !state.frames.is_empty() {
            Some(now.saturating_add(1_000_000_000))
        } else {
            state
                .tasks
                .iter()
                .map(|t| t.due.max(now.saturating_add(1_000_000_000)))
                .chain(
                    state
                        .known_catalogs
                        .then_some(state.next_scan.max(now.saturating_add(1_000_000_000))),
                )
                // Notifications are hints, not a durable delivery guarantee.
                // Poll only this primary's bounded journal feed; the managed
                // directory reconciliation retains its much slower cadence.
                .chain((!local_shards.is_empty()).then_some(now.saturating_add(FEED_POLL_NANOS)))
                .min()
        };
        let mut progress = state.clone();
        progress.next_due = original.next_due;
        // A timer advancing is not durable progress. Avoid fsync and dirty
        // pages on every idle feed poll. Startup always forces a fresh pass.
        if progress != original || !checkpoint.exists() {
            crate::metadata_io::write_sobj_atomic(checkpoint, &state)?;
        }
        *cache.state.lock().unwrap() = state;
        Ok(report)
    }
}
