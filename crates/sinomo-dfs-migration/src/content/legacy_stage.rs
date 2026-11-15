//! Representation transitions subordinate to the original formats1/2 cursor.
//! There is no worker here: the same parent converter drives the source3 unit
//! transformer, and only references its existing checkpoint.
use super::original_v3::{
    OriginalV3Budget, OriginalV3Converter, OriginalV3Selection, OriginalV3SourceIdentity,
    probe_original_v3,
};
use super::*;
use sinomo_dfs::ContentStoreStagingWriter;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

const HEADER_BUDGET: u64 = 8 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct LegacyContentJobReference {
    pub identity: String,
    /// Relative to the original source root, not an independent inventory cursor.
    pub checkpoint: PathBuf,
    pub original_target_incarnation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct LegacyPublishedV3 {
    pub predecessor: LegacyContentJobReference,
    pub checkpoint_digest: String,
    pub source: OriginalV3SourceIdentity,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
enum Phase {
    Prepared,
    Retained,
    Covered,
    Published,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Upgrade {
    version: u16,
    pub job: LegacyContentJobReference,
    parent_checkpoint_digest: String,
    source: OriginalV3SourceIdentity,
    target_incarnation: String,
    inventory_id: String,
    phase: Phase,
}
struct Held(File);
impl Drop for Held {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}
fn work(root: &Path) -> PathBuf {
    root.join(CONVERSION).join("target-upgrade-v3")
}
fn record_path(root: &Path) -> PathBuf {
    work(root).join("transition.sobj")
}
fn parent_digest(root: &Path) -> Result<String> {
    let bytes = durable_file::read_object_file_bytes(
        &root.join(CONVERSION).join("progress.sobj"),
        1024 * 1024,
    )
    .map_err(error)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
pub(super) fn job(incarnation: String) -> LegacyContentJobReference {
    LegacyContentJobReference {
        identity: format!("legacy-content-target:{incarnation}"),
        checkpoint: Path::new(CONVERSION).join("progress.sobj"),
        original_target_incarnation: incarnation,
    }
}
pub(super) fn target_incarnation(root: &Path) -> Result<String> {
    match source_format(root)? {
        Some(3) => super::original_v3::original_marker_incarnation(root),
        Some(version) if version == LocalContentStore::FORMAT_VERSION => {
            LocalContentStore::open_for_inspection(root)
                .store_incarnation()
                .map_err(error)
        }
        _ => Err(error(
            "legacy checkpoint target has no supported original incarnation",
        )),
    }
}
pub(super) fn reference(root: &Path) -> Result<LegacyContentJobReference> {
    if let Some(record) = load(root)? {
        return Ok(record.job);
    }
    let target = root.join(CONVERSION).join("store");
    if target.join(MARKER).exists() {
        return Ok(job(target_incarnation(&target)?));
    }
    if current(root)? {
        return Ok(job(LocalContentStore::open_for_inspection(root)
            .store_incarnation()
            .map_err(error)?));
    }
    Err(error(
        "legacy job has neither its original staged target nor current hint-only root",
    ))
}
fn transition_id(
    job: &LegacyContentJobReference,
    parent_checkpoint_digest: &str,
    source: &OriginalV3SourceIdentity,
) -> Result<String> {
    #[derive(Serialize)]
    struct Binding<'a> {
        job: &'a LegacyContentJobReference,
        parent_checkpoint_digest: &'a str,
        source: &'a OriginalV3SourceIdentity,
    }
    let bytes = serde_ext::to_bytes(
        &Binding {
            job,
            parent_checkpoint_digest,
            source,
        },
        1,
        LengthMode::U32,
    )
    .map_err(error)?;
    Ok(format!("legacy-stage-v3:{}", blake3::hash(&bytes).to_hex()))
}
fn load(root: &Path) -> Result<Option<Upgrade>> {
    let path = record_path(root);
    if !path.exists() {
        return Ok(None);
    }
    let record: Upgrade = read(&path)?;
    let expected = transition_id(
        &record.job,
        &record.parent_checkpoint_digest,
        &record.source,
    )?;
    if record.version != 1
        || record.job != job(record.source.store_incarnation.clone())
        || record.inventory_id != expected
    {
        return Err(error(
            "legacy staged transition changed its original parent/source identity",
        ));
    }
    Ok(Some(record))
}
impl Upgrade {
    pub(super) fn prepare(root: &Path) -> Result<Option<Self>> {
        if let Some(record) = load(root)? {
            return Ok(Some(record));
        }
        let old = root.join(CONVERSION).join("store");
        if source_format(&old)? != Some(3) {
            return Ok(None);
        }
        let _source = Held(lock(&old.join("content-owner.lock"))?);
        let source = probe_original_v3(&old, HEADER_BUDGET)?;
        let job = job(source.store_incarnation.clone());
        let parent_checkpoint_digest = parent_digest(root)?;
        let inventory_id = transition_id(&job, &parent_checkpoint_digest, &source)?;
        let work = work(root);
        durable_file::ensure_directory(&work).map_err(error)?;
        let target = work.join("current");
        let writer = ContentStoreStagingWriter::open_candidate(&target).map_err(error)?;
        writer
            .begin_inventory_import(&inventory_id)
            .map_err(error)?;
        let target_incarnation = writer.store_incarnation().map_err(error)?;
        drop(writer);
        let record = Self {
            version: 1,
            job,
            parent_checkpoint_digest,
            source,
            target_incarnation,
            inventory_id,
            phase: Phase::Prepared,
        };
        write(&record_path(root), &record)?;
        Ok(Some(record))
    }
    fn persist_phase(&mut self, root: &Path, phase: Phase) -> Result<()> {
        let mut next = self.clone();
        next.phase = phase;
        write(&record_path(root), &next)?;
        *self = next;
        Ok(())
    }
    fn verify_parent(&self, root: &Path) -> Result<()> {
        if self.phase != Phase::Published && parent_digest(root)? != self.parent_checkpoint_digest {
            return Err(error(
                "legacy parent cursor changed before its staged successor was qualified",
            ));
        }
        Ok(())
    }
    fn current_matches(&self, path: &Path) -> Result<bool> {
        if !path.exists() {
            return Ok(false);
        }
        let store = LocalContentStore::open_for_inspection(path);
        Ok(
            store.store_incarnation().map_err(error)? == self.target_incarnation
                && (store.inventory_import_pending().map_err(error)?.as_deref()
                    == Some(self.inventory_id.as_str())
                    || store
                        .inventory_import_completed(&self.inventory_id)
                        .map_err(error)?),
        )
    }
    pub(super) fn advance(
        &mut self,
        root: &Path,
        driver: &mut OriginalV3Converter,
        budget: usize,
        max_bytes: u64,
        deadline: Instant,
    ) -> Result<DfsStoragePreparation> {
        self.verify_parent(root)?;
        let work = work(root);
        let old = root.join(CONVERSION).join("store");
        let retained = work.join("original");
        let target = work.join("current");
        if self.phase == Phase::Prepared {
            let path = if retained.exists() { &retained } else { &old };
            let _source = Held(lock(&path.join("content-owner.lock"))?);
            if probe_original_v3(path, HEADER_BUDGET)? != self.source {
                return Err(error(
                    "original legacy staged source changed before relocation",
                ));
            }
            if !retained.exists() {
                fs::rename(&old, &retained).map_err(|e| io(&old, e))?;
                durable_file::sync_directory(&root.join(CONVERSION)).map_err(error)?;
                durable_file::sync_directory(&work).map_err(error)?;
            }
            self.persist_phase(root, Phase::Retained)?;
        }
        if self.phase == Phase::Retained {
            if !self.current_matches(&target)? {
                return Err(error(
                    "legacy staged current target changed its original identity",
                ));
            }
            let cancelled = AtomicBool::new(false);
            let step = driver.step(
                &retained,
                &target,
                &self.inventory_id,
                &OriginalV3Selection::All,
                &OriginalV3Budget {
                    max_records: budget.max(1),
                    max_bytes,
                    deadline,
                    cancelled: &cancelled,
                },
            )?;
            if !step.coverage_complete {
                return Ok(DfsStoragePreparation::Pending {
                    processed_records: step.processed_records,
                });
            }
            let observed = LocalContentStore::open_for_inspection(&target);
            if !observed
                .inventory_import_completed(&self.inventory_id)
                .map_err(error)?
            {
                let writer =
                    ContentStoreStagingWriter::open_import_target(&target, &self.inventory_id)
                        .map_err(error)?;
                writer
                    .complete_inventory_import(&self.inventory_id)
                    .map_err(error)?;
            }
            self.persist_phase(root, Phase::Covered)?;
        }
        if self.phase == Phase::Covered {
            self.verify_parent(root)?;
            if target.exists() {
                if old.exists() || !self.current_matches(&target)? {
                    return Err(error("legacy staged publication destination changed"));
                }
                fs::rename(&target, &old).map_err(|e| io(&target, e))?;
                durable_file::sync_directory(&work).map_err(error)?;
                durable_file::sync_directory(&root.join(CONVERSION)).map_err(error)?;
            } else if !self.current_matches(&old)? {
                return Err(error("legacy staged publication lost both exact names"));
            }
            self.persist_phase(root, Phase::Published)?;
        }
        if !self.current_matches(&old)? {
            return Err(error(
                "legacy parent no longer owns its exact upgraded target",
            ));
        }
        Ok(DfsStoragePreparation::Ready)
    }
    pub(super) fn published(&self) -> bool {
        self.phase == Phase::Published
    }
}

fn derive_published_v3(root: &Path) -> Result<Option<LegacyPublishedV3>> {
    if !super::incremental::published_three_checkpoint(root)? {
        return Ok(None);
    }
    let predecessor = reference(root)?;
    let source = probe_original_v3(root, HEADER_BUDGET)?;
    if source.store_incarnation != predecessor.original_target_incarnation {
        return Err(error(
            "published legacy successor changed the parent target incarnation",
        ));
    }
    Ok(Some(LegacyPublishedV3 {
        predecessor,
        checkpoint_digest: parent_digest(root)?,
        source,
    }))
}
pub(super) fn record_published_v3(root: &Path) -> Result<LegacyPublishedV3> {
    let record = derive_published_v3(root)?
        .ok_or_else(|| error("legacy parent has no qualified published3 prefix"))?;
    let path = root.join(CONVERSION).join("published-v3-successor.sobj");
    if path.exists() {
        let old: LegacyPublishedV3 = read(&path)?;
        if old != record {
            return Err(error("legacy successor proof changed after publication"));
        }
    } else {
        write(&path, &record)?;
    }
    Ok(record)
}
pub(super) fn published_v3(root: &Path) -> Result<Option<LegacyPublishedV3>> {
    let record = derive_published_v3(root)?;
    let path = root.join(CONVERSION).join("published-v3-successor.sobj");
    if path.exists() {
        let retained: LegacyPublishedV3 = read(&path)?;
        if record.as_ref() != Some(&retained) {
            return Err(error(
                "legacy successor changed its original checkpoint/source",
            ));
        }
    }
    Ok(record)
}
