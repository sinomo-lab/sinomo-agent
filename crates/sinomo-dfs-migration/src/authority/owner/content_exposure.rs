//! Replay-safe publication of a clean partial current content root. Original
//! source bytes remain in the retained tree; the source driver owns its cursor.
use super::*;
use crate::content::{OriginalV3SourceIdentity, probe_original_v3};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::storage::durable_file;

const HEADER_BYTES: u64 = 8 * 1024 * 1024;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DirectoryIdentity {
    #[cfg(unix)] device: u64,
    #[cfg(unix)] inode: u64,
    created: Option<std::time::SystemTime>,
}
fn directory_identity(path: &Path) -> Result<DirectoryIdentity> {
    let metadata = std::fs::symlink_metadata(path).map_err(error)?;
    if !metadata.is_dir() { return Err(error("content publication requires the actual physical directory")); }
    #[cfg(unix)] use std::os::unix::fs::MetadataExt;
    Ok(DirectoryIdentity { #[cfg(unix)] device: metadata.dev(), #[cfg(unix)] inode: metadata.ino(), created: metadata.created().ok() })
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Phase { Prepared, SourceRetained, TargetPublished, Covered }
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Publication {
    version: u16,
    pub inventory_id: String,
    source: OriginalV3SourceIdentity,
    source_directory: DirectoryIdentity,
    target_directory: DirectoryIdentity,
    pub target_incarnation: String,
    pub retained: PathBuf,
    staged: PathBuf,
    live: PathBuf,
    phase: Phase,
}
fn checkpoint(data_root: &Path) -> PathBuf { data_root.join("dfs/.content-conversion-v2/publication.sobj") }
fn write(data_root: &Path, publication: &Publication) -> Result<()> {
    let bytes = serde_ext::to_bytes(publication, 1, LengthMode::U32).map_err(error)?;
    durable_file::write_file_atomic(&checkpoint(data_root), &bytes,
        durable_file::DurableAtomicFileOptions::new("content-publication", "tmp").sync_all()).map_err(error)
}
pub(super) fn retained_publication(data_root: &Path) -> Result<Option<Publication>> {
    let path = checkpoint(data_root);
    if !path.exists() { return Ok(None); }
    let bytes = durable_file::read_object_file_bytes(&path, 64 * 1024).map_err(error)?;
    let publication: Publication = serde_ext::from_bytes(&bytes).map_err(error)?;
    let id = format!("content-source-v3:{}", blake3::hash(&serde_ext::to_bytes(&publication.source, 1, LengthMode::U32).map_err(error)?).to_hex());
    if publication.version != 1 || publication.inventory_id != id
        || publication.live != Path::new("dfs/content")
        || publication.retained != Path::new("dfs/.content-retained").join(id.trim_start_matches("content-source-v3:"))
        || publication.staged != Path::new("dfs/.content-conversion-v2/targets").join(id.trim_start_matches("content-source-v3:")) {
        return Err(error("content publication changed its original source/target identity"));
    }
    Ok(Some(publication))
}
fn source_matches(data_root: &Path, publication: &Publication, relative: &Path) -> Result<bool> {
    let root = data_root.join(relative);
    if !root.exists() { return Ok(false); }
    Ok(directory_identity(&root)? == publication.source_directory
        && probe_original_v3(&root, HEADER_BYTES)? == publication.source)
}
fn target_matches(data_root: &Path, publication: &Publication, relative: &Path) -> Result<bool> {
    let root = data_root.join(relative);
    if !root.exists() { return Ok(false); }
    if directory_identity(&root)? != publication.target_directory { return Ok(false); }
    let store = sinomo_dfs::LocalContentStore::open_for_inspection(&root);
    if store.store_incarnation().map_err(error)? != publication.target_incarnation { return Ok(false); }
    let pending = store.inventory_import_pending().map_err(error)?;
    Ok(pending.as_deref() == Some(publication.inventory_id.as_str())
        || (pending.is_none() && store.inventory_import_completed(&publication.inventory_id).map_err(error)?))
}
struct HeldSource(std::fs::File);
impl Drop for HeldSource { fn drop(&mut self) { let _ = FileExt::unlock(&self.0); } }

// A Windows directory cannot move while a file opened through it is held.
// Open the original byte lock through a hard link in the publication directory.
// Both generations of writers still contend on the same underlying file.
#[cfg(windows)]
fn open_publication_lock(source: &Path, alias: &Path) -> Result<std::fs::File> {
    match std::fs::hard_link(source, alias) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(error(e)),
    }
    if !std::fs::symlink_metadata(alias).map_err(error)?.is_file() {
        return Err(error("publication lock is not a regular file"));
    }
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(alias)
        .map_err(error)?;
    let opened = same_file::Handle::from_file(file.try_clone().map_err(error)?).map_err(error)?;
    if opened != same_file::Handle::from_path(source).map_err(error)? {
        return Err(error("publication lock does not identify the original content owner file"));
    }
    Ok(file)
}

/// Caller is the sole actual component data owner. This function only exposes
/// a Pending target; it cannot claim content inventory completeness.
pub(super) fn prepare(data_root: &Path, mut before_effect: impl FnMut(&Publication) -> Result<()>) -> Result<Publication> {
    let live = data_root.join("dfs/content");
    let work = data_root.join("dfs/.content-conversion-v2");
    let retained_publication = retained_publication(data_root)?;
    for path in [data_root.join("dfs"), work.clone(), work.join("targets"), data_root.join("dfs/.content-retained")] {
        if retained_publication.is_some() {
            // The original durable publication was written only after these
            // barriers. Replay revalidates physical directories instead of
            // repeatedly spending the worker quantum on unchanged fsyncs.
            directory_identity(&path)?;
        } else {
            durable_file::ensure_directory(&path).map_err(error)?;
        }
    }
    let mut publication = if let Some(publication) = retained_publication { publication }
    else {
        let source_lock_path = live.join("content-owner.lock");
        let source_lock = std::fs::OpenOptions::new().read(true).write(true).open(&source_lock_path).map_err(error)?;
        source_lock.try_lock_exclusive().map_err(error)?;
        let _held = HeldSource(source_lock);
        let source = probe_original_v3(&live, HEADER_BYTES)?;
        let hash = blake3::hash(&serde_ext::to_bytes(&source, 1, LengthMode::U32).map_err(error)?).to_hex().to_string();
        let inventory_id = format!("content-source-v3:{hash}");
        let staged = Path::new("dfs/.content-conversion-v2/targets").join(&hash);
        let target = data_root.join(&staged);
        // The normal current candidate initializer owns all current format
        // bytes; migration never invents a current sentinel.
        let writer = sinomo_dfs::ContentStoreStagingWriter::open_candidate(&target).map_err(error)?;
        writer.begin_inventory_import(&inventory_id).map_err(error)?;
        let target_incarnation = writer.store_incarnation().map_err(error)?;
        drop(writer);
        let publication = Publication {
            version: 1, inventory_id, source, source_directory: directory_identity(&live)?,
            target_directory: directory_identity(&target)?, target_incarnation,
            retained: Path::new("dfs/.content-retained").join(&hash), staged,
            live: "dfs/content".into(), phase: Phase::Prepared,
        };
        write(data_root, &publication)?;
        publication
    };
    before_effect(&publication)?;
    let retained = data_root.join(&publication.retained);
    let source_path = if source_matches(data_root, &publication, &publication.retained)? { retained.clone() } else { live.clone() };
    let lock_path = source_path.join("content-owner.lock");
    #[cfg(windows)]
    let source_lock = open_publication_lock(&lock_path, &work.join("source-owner.lock"))?;
    #[cfg(not(windows))]
    let source_lock = std::fs::File::options().read(true).write(true).open(&lock_path).map_err(error)?;
    source_lock.try_lock_exclusive().map_err(error)?;
    let _held = HeldSource(source_lock);
    if publication.phase == Phase::Prepared {
        if source_matches(data_root, &publication, &publication.live)? {
            if retained.exists() { return Err(error("content retained destination already belongs to another source")); }
            std::fs::rename(&live, &retained).map_err(error)?;
            durable_file::sync_directory(live.parent().expect("DFS root")).map_err(error)?;
            durable_file::sync_directory(retained.parent().expect("retained root parent")).map_err(error)?;
        }
        if !source_matches(data_root, &publication, &publication.retained)? {
            return Err(error("content publication lost its exact retained source"));
        }
        publication.phase = Phase::SourceRetained;
        write(data_root, &publication)?;
    }
    if publication.phase == Phase::SourceRetained {
        #[cfg(test)] observe(data_root, "source-retained")?;
        let staged = data_root.join(&publication.staged);
        if !live.exists() {
            if !target_matches(data_root, &publication, &publication.staged)? {
                return Err(error("content staged target changed before publication"));
            }
            std::fs::rename(&staged, &live).map_err(error)?;
            durable_file::sync_directory(live.parent().expect("DFS root")).map_err(error)?;
            durable_file::sync_directory(staged.parent().expect("staging parent")).map_err(error)?;
        }
        if !target_matches(data_root, &publication, &publication.live)? {
            return Err(error("content live target does not match the exact Pending publication"));
        }
        publication.phase = Phase::TargetPublished;
        write(data_root, &publication)?;
        #[cfg(test)] observe(data_root, "target-published")?;
    }
    if !source_matches(data_root, &publication, &publication.retained)? || !target_matches(data_root, &publication, &publication.live)? {
        return Err(error("content publication source/target changed after exposure"));
    }
    Ok(publication)
}

pub(super) fn mark_covered(data_root: &Path, publication: &Publication) -> Result<()> {
    let mut actual = retained_publication(data_root)?.ok_or_else(|| error("content publication checkpoint disappeared"))?;
    if actual.inventory_id != publication.inventory_id || actual.target_incarnation != publication.target_incarnation {
        return Err(error("content coverage belongs to a different original publication"));
    }
    if !source_matches(data_root, &actual, &actual.retained)? { return Err(error("content coverage source identity changed")); }
    let store = sinomo_dfs::LocalContentStore::open_for_inspection(data_root.join(&actual.live));
    if store.store_incarnation().map_err(error)? != actual.target_incarnation { return Err(error("content coverage target identity changed")); }
    if !store.inventory_import_completed(&actual.inventory_id).map_err(error)? {
        let writer = sinomo_dfs::ContentStoreStagingWriter::open_import_target(&data_root.join(&actual.live), &actual.inventory_id).map_err(error)?;
        writer.complete_inventory_import(&actual.inventory_id).map_err(error)?;
    }
    actual.phase = Phase::Covered;
    write(data_root, &actual)
}

/// Bounded frontend binding check. A valid fresh current root cannot replace
/// an in-progress publication and bypass the missing-unit readiness owner.
pub(super) fn frontend_target(data_root: &Path) -> Result<Option<bool>> {
    let Some(publication) = retained_publication(data_root)? else { return Ok(None); };
    let live = data_root.join(&publication.live);
    if !live.exists() { return Ok(Some(false)); }
    if target_matches(data_root, &publication, &publication.live)? { return Ok(Some(true)); }
    if publication.phase == Phase::Prepared && directory_identity(&live)? == publication.source_directory {
        return Ok(Some(false));
    }
    Err(error("current content root replaced the exact retained publication target"))
}

pub(super) fn checkpoint_relative() -> PathBuf { "dfs/.content-conversion-v2/publication.sobj".into() }

#[cfg(test)]
type Hook = Arc<dyn Fn(&str) -> Result<()> + Send + Sync>;
#[cfg(test)]
static HOOKS: OnceLock<Mutex<BTreeMap<PathBuf, Hook>>> = OnceLock::new();
#[cfg(test)]
pub(super) struct ExposureHook(PathBuf);
#[cfg(test)]
impl Drop for ExposureHook { fn drop(&mut self) { if let Some(hooks) = HOOKS.get() { hooks.lock().unwrap().remove(&self.0); } } }
#[cfg(test)]
pub(super) fn install_hook(root: &Path, hook: Hook) -> ExposureHook {
    HOOKS.get_or_init(Default::default).lock().unwrap().insert(root.to_owned(), hook);
    ExposureHook(root.to_owned())
}
#[cfg(test)]
pub(super) fn observe(root: &Path, point: &str) -> Result<()> {
    let hook = HOOKS.get().and_then(|hooks| hooks.lock().unwrap().get(root).cloned());
    if let Some(hook) = hook { hook(point)?; }
    Ok(())
}

/// Resolve only this publication's original physical source; paths in inventory
/// remain the original logical checkpoint locators across the owned rename.
pub(super) fn retained_source_root(data_root: &Path, publication: &Publication) -> Result<PathBuf> {
    for relative in [&publication.retained, &publication.live] {
        if source_matches(data_root, publication, relative)? { return Ok(data_root.join(relative)); }
    }
    Err(error("content publication cannot resolve its original retained source"))
}
pub(super) fn matches_source_identity(publication: &Publication, source: &OriginalV3SourceIdentity) -> bool {
    &publication.source == source
}

/// Absence must be observed, not inferred from an inaccessible retained tree.
pub(super) fn has_retained_sources(data_root: &Path) -> Result<bool> {
    let retained = data_root.join("dfs/.content-retained");
    match std::fs::symlink_metadata(&retained) {
        Ok(metadata) if metadata.file_type().is_dir() => {},
        Ok(_) => return Err(error("retained content source path is not a physical directory")),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(failure) => return Err(error(failure)),
    }
    match std::fs::read_dir(&retained) {
        Ok(mut entries) => Ok(entries.next().transpose().map_err(error)?.is_some()),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(failure) => Err(error(failure)),
    }
}

pub(super) fn live_root_absent(root: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(false),
        Ok(_) => Err(error("managed content root is not a physical directory")),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(failure) => Err(error(failure)),
    }
}

/// A completed archive is original evidence, never a current serving root.
/// This observation grants neither deletion nor content-root GC authority.
pub(super) fn completed_original_job(data_root: &Path, relative: &Path) -> Result<Option<inventory::JobReference>> {
    let Some(publication) = retained_publication(data_root)? else { return Ok(None); };
    if relative != publication.retained && relative != publication.staged
        && relative != checkpoint_relative().parent().expect("publication parent") { return Ok(None); }
    if publication.phase != Phase::Covered || !target_matches(data_root, &publication, &publication.live)?
        || !source_matches(data_root, &publication, &publication.retained)? { return Ok(None); }
    let target = data_root.join(&publication.live);
    if !sinomo_dfs::LocalContentStore::open_for_inspection(&target).inventory_import_completed(&publication.inventory_id).map_err(error)? {
        return Ok(None);
    }
    if !crate::content::OriginalV3Converter::completed_original_observation(&data_root.join(&publication.retained), &target, &publication.inventory_id)? {
        return Ok(None);
    }
    Ok(Some(inventory::JobReference { checkpoint: checkpoint_relative(), identity: publication.inventory_id }))
}
