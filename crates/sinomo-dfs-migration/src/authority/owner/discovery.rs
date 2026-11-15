//! Bounded managed-root traversal. A persisted directory offset is never
//! trusted: after restart, unfinished directories are re-enumerated and the
//! exact durable unit references deduplicate them.
use super::inventory::{UnitKey, UnitReference, UnitRole};
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DirectoryStamp { modified_nanos: Option<u64>, len: u64 }
fn directory_stamp(path: &Path) -> Result<DirectoryStamp> {
    let metadata = std::fs::metadata(path).map_err(error)?;
    Ok(DirectoryStamp { modified_nanos: metadata.modified().ok().and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok()).and_then(|at| u64::try_from(at.as_nanos()).ok()), len: metadata.len() })
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryReceipt { epoch: u64, relative: PathBuf, stamp: DirectoryStamp }
#[derive(Clone, Serialize, Deserialize)]
struct Cursor {
    version: u16,
    epoch: u64,
    pending: VecDeque<PathBuf>,
    /// Missing/offline paths are evidence, never completion of their units.
    unavailable: BTreeMap<PathBuf, String>,
}
pub(super) struct Discovery {
    data_root: PathBuf,
    checkpoint: PathBuf,
    cursor: Cursor,
    current: Option<std::fs::ReadDir>,
    current_stamp: Option<DirectoryStamp>,
    completed: Vec<(PathBuf, DirectoryStamp)>,
    seeds: VecDeque<UnitKey>,
    retry_at: std::time::Instant,
}
impl Discovery {
    pub fn open(data_root: &Path, inventory_root: &Path) -> Result<Self> {
        let checkpoint = inventory_root.join("discovery.sobj");
        let cursor = if checkpoint.exists() {
            let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(&checkpoint, 128 * 1024).map_err(error)?;
            let cursor: Cursor = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
            if cursor.version != 1 { return Err(error("unknown migration discovery cursor")); }
            for path in cursor.pending.iter().chain(cursor.unavailable.keys()) { validate_directory(path)?; }
            Cursor { version: 1, epoch: cursor.epoch.checked_add(1).ok_or_else(|| error("migration enumeration epoch exhausted"))?,
                pending: managed_directories().into(), unavailable: cursor.unavailable }
        } else {
            Cursor { version: 1, epoch: 1, pending: managed_directories().into(), unavailable: BTreeMap::new() }
        };
        let seeds = [
            ("dfs", UnitRole::LocalState),
            ("dfs/metadata", UnitRole::MetadataRouting),
            ("dfs/content", UnitRole::ContentRoot),
            ("dfs/content", UnitRole::ContentInventory),
        ].into_iter().map(|(root, role)| UnitKey { relative_root: root.into(), role }).collect();
        let mut discovery = Self { data_root: data_root.to_owned(), checkpoint, cursor, current: None, current_stamp: None, completed: Vec::new(), seeds, retry_at: std::time::Instant::now() + std::time::Duration::from_secs(5) };
        discovery.commit()?;
        Ok(discovery)
    }
    pub fn step(&mut self, max_entries: usize) -> Result<Vec<UnitReference>> {
        if self.cursor.pending.is_empty() && std::time::Instant::now() >= self.retry_at {
            for missing in self.cursor.unavailable.keys().take(256) {
                let directory = if missing.extension().is_some() { missing.parent().unwrap_or(missing) } else { missing.as_path() };
                if !self.cursor.pending.iter().any(|path| path == directory) { self.cursor.pending.push_back(directory.to_owned()); }
            }
            self.retry_at = std::time::Instant::now() + std::time::Duration::from_secs(5);
        }
        let mut discovered = Vec::new();
        let mut processed = 0;
        while processed < max_entries {
            if let Some(seed) = self.seeds.pop_front() {
                discovered.push(UnitReference::discovered(seed)); processed += 1; continue;
            }
            let Some(relative) = self.cursor.pending.front().cloned() else { break; };
            if self.current.is_none() {
                if self.completed_unchanged(&relative)? { self.cursor.pending.pop_front(); processed += 1; continue; }
                match std::fs::read_dir(self.data_root.join(&relative)) {
                    Ok(reader) => { self.current_stamp = Some(directory_stamp(&self.data_root.join(&relative))?); self.current = Some(reader); self.cursor.unavailable.remove(&relative); }
                    Err(failure) => {
                        self.cursor.unavailable.insert(relative.clone(), failure.to_string());
                        self.cursor.pending.pop_front(); processed += 1; continue;
                    }
                }
            }
            let next = self.current.as_mut().expect("directory reader").next();
            let Some(entry) = next else {
                processed += 1;
                self.current = None;
                self.cursor.pending.pop_front();
                let after = directory_stamp(&self.data_root.join(&relative))?;
                if self.current_stamp.take().as_ref() == Some(&after) { self.completed.push((relative, after)); }
                else { self.cursor.pending.push_back(relative); }
                continue;
            };
            processed += 1;
            let entry = match entry { Ok(entry) => entry, Err(failure) => {
                self.cursor.unavailable.insert(relative.clone(), failure.to_string());
                self.current = None; self.cursor.pending.pop_front(); continue;
            }};
            let kind = match entry.file_type() { Ok(kind) => kind, Err(failure) => {
                self.cursor.unavailable.insert(relative.join(entry.file_name()), failure.to_string()); continue;
            }};
            if relative.starts_with("dfs/migration-owner/units") && kind.is_file() && entry.path().extension().is_some_and(|ext| ext == "sobj") {
                let restored = (|| -> Result<UnitReference> {
                    let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(&entry.path(), 64 * 1024).map_err(error)?;
                    let reference: UnitReference = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
                    reference.validate()?;
                    if entry.file_name() != std::ffi::OsString::from(format!("{}.sobj", reference.unit.id()?)) {
                        return Err(error("retained inventory file changed its exact unit key"));
                    }
                    Ok(reference)
                })();
                match restored { Ok(reference) => { self.cursor.unavailable.remove(&relative.join(entry.file_name())); discovered.push(reference); }, Err(failure) => {
                    self.cursor.unavailable.insert(relative.join(entry.file_name()), failure.to_string());
                }};
                continue;
            }
            if !kind.is_dir() {
                if kind.is_symlink() { self.cursor.unavailable.insert(relative.join(entry.file_name()), "managed source is a symbolic path requiring explicit owner resolution".into()); }
                continue;
            }
            let child = relative.join(entry.file_name());
            validate_directory(&child)?;
            if relative == Path::new("dfs/metadata") && !entry.file_name().to_string_lossy().starts_with('.') {
                // The derived routing index is managed by its own root unit,
                // not an independently owned physical metadata shard.
                if entry.file_name() == "shard-index" { continue; }
                let shard = entry.file_name().into_string().map_err(|_| error("metadata physical scope is not a valid identifier"))?;
                discovered.push(UnitReference::discovered(UnitKey { relative_root: child, role: UnitRole::Metadata { shard: sinomo_dfs::DfsMetadataShardId::new(shard) } }));
                continue;
            }
            if child == Path::new("dfs/metadata/.metadata-retained") || child == Path::new("dfs/metadata/.metadata-conversion") { continue; }
            let physical = self.data_root.join(&child);
            let job_root = !child.starts_with("dfs/migration-owner/units") && ["progress.json", "progress.sobj", "native-import-plan.json", "source-cut.json", "metadata-import-v7.json", "records/tables/snapshot.sobj", "content-reservations/00/batch-format-v1.sobj"]
                .iter().any(|name| physical.join(name).is_file());
            if job_root {
                discovered.push(UnitReference::discovered(UnitKey { relative_root: child, role: UnitRole::RetainedArtifact }));
            } else {
                if self.cursor.pending.contains(&child) || self.completed_unchanged(&child)? { continue; }
                if self.cursor.pending.len() >= 256 {
                    // Do not lose a child due to a bounded frontier. Re-enumerate
                    // this directory next turn; durable unit refs deduplicate.
                    self.current = None;
                    self.current_stamp = None;
                    self.cursor.pending.pop_front();
                    self.cursor.pending.push_back(relative);
                    break;
                }
                self.cursor.pending.push_back(child);
            }
        }
        // Unit references are persisted by the owner before this cursor is
        // committed, so a crash cannot skip a discovered cold unit.
        Ok(discovered)
    }
    fn completed_path(&self, relative: &Path) -> Result<PathBuf> {
        let bytes = sinomo_core::object::serde_ext::to_bytes(&relative.to_owned(), 1, sinomo_core::object::LengthMode::U32).map_err(|failure| error(format!("directory receipt key: {failure}")))?;
        Ok(self.checkpoint.parent().expect("inventory directory").join("directories").join(format!("{}.sobj", blake3::hash(&bytes).to_hex())))
    }
    fn completed_unchanged(&self, relative: &Path) -> Result<bool> {
        if self.cursor.unavailable.keys().any(|unavailable| unavailable.starts_with(relative)) { return Ok(false); }
        let path = self.completed_path(relative)?;
        if !path.exists() || !self.data_root.join(relative).exists() { return Ok(false); }
        let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(&path, 4096).map_err(error)?;
        let receipt: DirectoryReceipt = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
        if receipt.relative != relative { return Err(error("discovery completion changed its directory identity")); }
        Ok(receipt.epoch == self.cursor.epoch && receipt.stamp == directory_stamp(&self.data_root.join(relative))?)
    }
    fn record_completed(&self, relative: &Path, stamp: &DirectoryStamp) -> Result<()> {
        let path = self.completed_path(relative)?;
        sinomo_core::storage::durable_file::ensure_directory(path.parent().expect("directory receipt parent")).map_err(error)?;
        let bytes = sinomo_core::object::serde_ext::to_bytes(&DirectoryReceipt { epoch: self.cursor.epoch, relative: relative.to_owned(), stamp: stamp.clone() }, 1, sinomo_core::object::LengthMode::U32).map_err(|failure| error(format!("directory receipt body: {failure}")))?;
        sinomo_core::storage::durable_file::write_file_atomic(&path, &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("migration-directory", "tmp").sync_all()).map_err(error)
    }
    pub fn commit(&mut self) -> Result<()> {
        for (path, stamp) in &self.completed { self.record_completed(path, stamp)?; }
        let bytes = sinomo_core::object::serde_ext::to_bytes(&self.cursor, 1, sinomo_core::object::LengthMode::U32).map_err(|failure| error(format!("directory frontier checkpoint: {failure}")))?;
        if bytes.len() > 128 * 1024 { return Err(error("migration discovery frontier exceeds its bounded checkpoint")); }
        sinomo_core::storage::durable_file::write_file_atomic(&self.checkpoint, &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("migration-discovery", "tmp").sync_all()).map_err(error)?;
        self.completed.clear();
        Ok(())
    }
    pub fn restart_uncommitted(&mut self) -> Result<()> {
        let root = self.checkpoint.parent().expect("inventory directory").to_owned();
        *self = Self::open(&self.data_root, &root)?;
        Ok(())
    }
    pub fn has_work(&self) -> bool { !self.seeds.is_empty() || !self.cursor.pending.is_empty() || (!self.cursor.unavailable.is_empty() && std::time::Instant::now() >= self.retry_at) }
    pub fn next_retry(&self) -> Option<std::time::Instant> { (!self.cursor.unavailable.is_empty()).then_some(self.retry_at) }
}
fn managed_directories() -> Vec<PathBuf> {
    ["dfs/migration-owner/units", "dfs/metadata", "dfs/metadata/.metadata-conversion", "dfs/metadata/.metadata-retained", "dfs/.content-retained", "dfs/.content-conversion-v2", "dfs/content/.content-conversion-v2", "daemon/dfs-migration/receivers"]
        .into_iter().map(PathBuf::from).collect()
}
fn validate_directory(path: &Path) -> Result<()> {
    if path.is_absolute() || path.components().count() > 64 || path.components().any(|part| !matches!(part, std::path::Component::Normal(_)))
        || !managed_directories().iter().any(|root| path.starts_with(root)) {
        return Err(error("migration discovery left its declared managed roots"));
    }
    Ok(())
}
