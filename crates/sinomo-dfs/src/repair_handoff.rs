//! Exact repair DATA carried by the existing frozen-prefix handoff.
//!
//! A page is not author permission. Its receiver must first validate the
//! authenticated retiring primary, pending role rotation and frozen prefix.
//! The ordinary joint-prefix certificate is acknowledged only after all pages
//! are durably staged. The successor installs them through its existing queue
//! during proof-bound rebind, before it can author the activated generation.

use crate::repair_queue_store::{DfsRepairQueueKey, LocalDfsRepairQueueStore, RepairOriginalBirth, RepairOriginalImportMaterial, SelectedOriginalFixtureIdentity};
use crate::{DfsError, DfsMetadataShardMembershipHandoff, DfsRepairAttemptRecord};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;

const VERSION: u32 = 1;
const SELECTED_ORIGINAL_DATA_VERSION: u32 = 2;

/// Selected material only, never a complete-custody or serving protocol floor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectedRepairOriginalDataCut {
    pub(crate) source_queue_incarnation: String,
    pub(crate) producer: sinomo_core::node::NodeId,
    pub(crate) request_id: String,
    pub(crate) original_digest: String,
    pub(crate) captured_birth_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsRepairHandoffCut {
    pub version: u32,
    pub handoff: DfsMetadataShardMembershipHandoff,
    pub queue_sequence: u64,
    pub page_count: u64,
    pub record_count: u64,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) selected_original_data: Option<SelectedRepairOriginalDataCut>,
}

impl DfsRepairHandoffCut {
    pub(crate) fn validate(&self) -> Result<(), DfsError> {
        self.validate_representation(false)
    }

    pub(crate) fn validate_selected_original_data(&self) -> Result<(), DfsError> {
        if self.version != SELECTED_ORIGINAL_DATA_VERSION || self.selected_original_data.is_none() {
            return Err(invalid("selected reader requires explicit selected-original DATA coverage"));
        }
        self.validate_representation(true)
    }

    fn validate_representation(&self, selected_reader: bool) -> Result<(), DfsError> {
        let supported = match (&self.selected_original_data, self.version) {
            (None, VERSION) => true,
            (Some(selected), SELECTED_ORIGINAL_DATA_VERSION) => {
                selected_reader && !selected.source_queue_incarnation.is_empty()
                    && !selected.request_id.is_empty()
                    && selected.producer == self.handoff.retiring_shard.primary_node_id
                    && selected.original_digest.len() == 64
                    && selected.original_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                    && selected.captured_birth_digest.len() == 64
                    && selected.captured_birth_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            }
            _ => false,
        };
        if !supported
            || self.page_count == 0
            || self.page_count > self.record_count.saturating_add(if self.selected_original_data.is_some() { 1 } else { 0 }).max(1)
            || self.record_count
                > self
                    .page_count
                    .saturating_mul(crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT as u64)
            || self.digest.len() != 64
            || !self.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.handoff.handoff_id.is_empty()
            || self.handoff.retiring_primary_fence.is_none()
            || self.handoff.retiring_shard.primary_node_id
                == self.handoff.desired_shard.primary_node_id
            || self.handoff.retiring_shard.file_system_id
                != self.handoff.desired_shard.file_system_id
            || self.handoff.retiring_shard.shard_id != self.handoff.desired_shard.shard_id
            || self.handoff.retiring_shard.epoch.checked_add(1)
                != Some(self.handoff.desired_shard.epoch)
        {
            return Err(invalid(
                "unsupported or incomplete original repair custody cut",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DfsRepairHandoffPage {
    pub cut: DfsRepairHandoffCut,
    pub ordinal: u64,
    pub previous_digest: String,
    pub records: Vec<DfsRepairAttemptRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) selected_original_data: Option<Box<crate::repair_queue_store::RepairOriginalBirth>>,
}

impl DfsRepairHandoffPage {
    pub(crate) fn digest(&self) -> Result<String, DfsError> {
        if self.cut.version == SELECTED_ORIGINAL_DATA_VERSION {
            #[derive(Serialize)]
            struct SelectedDigestInput<'a> {
                domain: &'static str,
                handoff: &'a DfsMetadataShardMembershipHandoff,
                queue_sequence: u64,
                selected: &'a Option<SelectedRepairOriginalDataCut>,
                ordinal: u64,
                previous_digest: &'a str,
                records: &'a [DfsRepairAttemptRecord],
                original: &'a Option<Box<crate::repair_queue_store::RepairOriginalBirth>>,
            }
            return Ok(blake3::hash(&crate::metadata_io::encode_sobj(&SelectedDigestInput {
                domain: "sinomo.dfs.selected-original-data.v2",
                handoff: &self.cut.handoff,
                queue_sequence: self.cut.queue_sequence,
                selected: &self.cut.selected_original_data,
                ordinal: self.ordinal,
                previous_digest: &self.previous_digest,
                records: &self.records,
                original: &self.selected_original_data,
            })?).to_hex().to_string());
        }
        // The final cut digest is excluded from the chain's own input.
        // SOBJ lists are homogeneous. Use a named object for these differently
        // typed fields rather than a heterogeneous tuple/list.
        #[derive(Serialize)]
        struct DigestInput<'a> {
            version: u32,
            handoff: &'a DfsMetadataShardMembershipHandoff,
            queue_sequence: u64,
            ordinal: u64,
            previous_digest: &'a str,
            records: &'a [DfsRepairAttemptRecord],
        }
        let bytes = crate::metadata_io::encode_sobj(&DigestInput {
            version: VERSION,
            handoff: &self.cut.handoff,
            queue_sequence: self.cut.queue_sequence,
            ordinal: self.ordinal,
            previous_digest: &self.previous_digest,
            records: &self.records,
        })?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    pub(crate) fn confirmation(&self) -> Result<String, DfsError> {
        if self.cut.version == SELECTED_ORIGINAL_DATA_VERSION {
            return Ok(format!("{}:selected-original-data-staged-v2:{}:{}",
                self.cut.handoff.handoff_id, self.ordinal, self.digest()?));
        }
        Ok(format!(
            "{}:repair-v1:{}:{}",
            self.cut.handoff.handoff_id,
            self.ordinal,
            self.digest()?
        ))
    }

    pub(crate) fn validate(&self) -> Result<(), DfsError> {
        self.cut.validate()?;
        self.validate_contents()
    }

    pub(crate) fn validate_selected_original_data(&self) -> Result<(), DfsError> {
        self.cut.validate_selected_original_data()?;
        self.validate_contents()
    }

    fn validate_contents(&self) -> Result<(), DfsError> {
        match (&self.cut.selected_original_data, &self.selected_original_data, self.ordinal) {
            (None, None, _) => {}
            (Some(selected), Some(original), 0) => {
                original.validate(&selected.source_queue_incarnation)?;
                if original.original.source_node_id != selected.producer
                    || original.root.subject.request_id != selected.request_id
                    || original.root.original_command_digest != selected.original_digest
                    || original.captured_data_digest()? != selected.captured_birth_digest
                    || original.closed_at_sequence.is_some()
                    || original.attempts.iter().any(|attempt| attempt.sequence == 0 || attempt.sequence > self.cut.queue_sequence)
                {
                    return Err(invalid("selected original DATA differs from its immutable cut"));
                }
                RepairOriginalImportMaterial { cut: self.cut.clone(), birth: original.as_ref().clone() }.validate()?;
            }
            (Some(_), None, ordinal) if ordinal > 0 => {}
            _ => return Err(invalid("selected original DATA coverage is incomplete or duplicated")),
        }
        if self.ordinal >= self.cut.page_count
            || self.records.len() > crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
            || crate::metadata_io::encode_sobj(self)?.len()
                > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT
            || (self.ordinal == 0 && !self.previous_digest.is_empty())
            || (self.ordinal > 0 && self.previous_digest.len() != 64)
        {
            return Err(invalid(
                "unsupported or incomplete bounded repair handoff page",
            ));
        }
        if self.records.windows(2).any(|pair| {
            DfsRepairQueueKey::from_record(&pair[0]) >= DfsRepairQueueKey::from_record(&pair[1])
        }) {
            return Err(invalid("repair handoff page is not strictly ordered"));
        }
        if self.ordinal + 1 == self.cut.page_count && self.digest()? != self.cut.digest {
            return Err(invalid(
                "repair handoff terminal digest differs from its cut",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

fn directory(root: &Path, handoff: &str, role: &str) -> PathBuf {
    root.join("journal")
        .join("repair-handoffs-v1")
        .join(format!(
            "{}-{role}",
            blake3::hash(handoff.as_bytes()).to_hex()
        ))
}

fn ensure_directory(root: &Path, directory: &Path) -> Result<(), DfsError> {
    ensure_directory_before(root, directory, None)
}

fn ensure_directory_before(root: &Path, directory: &Path, deadline: Option<Instant>) -> Result<(), DfsError> {
    // The actual backend root exists already. Persist each new namespace
    // component with the same storage helper, without changing its policy.
    let mut parent = root.to_path_buf();
    for component in directory
        .strip_prefix(root)
        .map_err(|_| invalid("repair custody escaped its physical backend"))?
        .components()
    {
        if let Some(deadline) = deadline { before(deadline)?; }
        parent.push(component);
        sinomo_core::storage::durable_file::ensure_directory(&parent)
            .map_err(|error| invalid(&format!("persist repair custody directory: {error}")))?;
    }
    Ok(())
}

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, DfsError> {
    match path.metadata() {
        Ok(metadata)
            if metadata.len() > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT as u64 =>
        {
            return Err(invalid(
                "retained repair custody exceeds the existing page byte limit",
            ));
        }
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(invalid(&format!("stat retained repair custody: {error}")));
        }
        _ => {}
    }
    crate::metadata_io::read_sobj_optional(path).map_err(Into::into)
}

fn write<T: Serialize>(path: &Path, value: &T) -> Result<(), DfsError> {
    crate::metadata_io::write_sobj_atomic(path, value).map_err(Into::into)
}

fn write_representation<T: Serialize>(path: &Path, value: &T, selected: bool) -> Result<(), DfsError> {
    if selected {
        crate::metadata_io::write_file_atomic(path, &crate::metadata_io::encode_sobj(value)?)?;
        Ok(())
    } else { write(path, value) }
}

/// Synchronize the exact current descriptor after atomic publication. A
/// replacement between publication and descriptor capture must itself be
/// checked and synchronized, never adopted from a metadata/readback match.
pub(crate) fn publish_and_certify_selected<T: Serialize>(path: &Path, value: &T) -> Result<std::fs::Metadata, DfsError> {
    use std::io::Read as _;
    let expected = crate::metadata_io::encode_sobj(value)?;
    crate::metadata_io::write_file_atomic(path, &expected)?;
    #[cfg(test)]
    if let Some(hook) = SELECTED_AFTER_PUBLISH_HOOK.take() { hook(); }
    let before = std::fs::symlink_metadata(path).map_err(|error| invalid(&error.to_string()))?;
    if !before.is_file() || before.file_type().is_symlink() || before.len() != expected.len() as u64 {
        return Err(invalid("selected published file is not the exact expected regular file"));
    }
    let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path).map_err(|error| invalid(&error.to_string()))?;
    if !crate::repair_queue_store::same_file_observation(&before, &file.metadata().map_err(|error| invalid(&error.to_string()))?) {
        return Err(invalid("selected published file changed before descriptor validation"));
    }
    let mut actual = vec![0u8; expected.len()];
    file.read_exact(&mut actual).map_err(|error| invalid(&error.to_string()))?;
    let mut extra = [0u8; 1];
    if actual != expected || file.read(&mut extra).map_err(|error| invalid(&error.to_string()))? != 0 {
        return Err(invalid("selected descriptor contains different bytes"));
    }
    #[cfg(test)]
    if FAIL_SELECTED_DESCRIPTOR_SYNC.replace(false) { return Err(invalid("injected selected descriptor sync failure")); }
    sinomo_core::storage::durable_file::sync_open_file(&mut file, path, sinomo_core::storage::durable_file::DurableFileSync::All)
        .map_err(|error| invalid(&error.to_string()))?;
    let after = std::fs::symlink_metadata(path).map_err(|error| invalid(&error.to_string()))?;
    if after.file_type().is_symlink()
        || !crate::repair_queue_store::same_file_observation(&before, &after)
        || !crate::repair_queue_store::same_file_observation(&after, &file.metadata().map_err(|error| invalid(&error.to_string()))?)
    { return Err(invalid("selected descriptor/path changed during synchronization")); }
    Ok(after)
}

pub(crate) fn sync_selected_namespace(fixture: &SelectedOriginalFixtureIdentity, path: &Path) -> Result<(), DfsError> {
    fixture.check()?;
    if !path.starts_with(fixture.root()) { return Err(invalid("selected staging escaped its admitted root")); }
    let mut directories = Vec::new();
    let mut current = path;
    loop {
        let metadata = std::fs::symlink_metadata(current).map_err(|error| invalid(&error.to_string()))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() { return Err(invalid("selected staging namespace is not a physical directory")); }
        directories.push((current.to_owned(), metadata));
        if current == fixture.root() {
            if let Some(parent) = current.parent() {
                let metadata = std::fs::symlink_metadata(parent).map_err(|error| invalid(&error.to_string()))?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() { return Err(invalid("selected root has no physical parent")); }
                directories.push((parent.to_owned(), metadata));
            }
            break;
        }
        current = current.parent().filter(|parent| parent.starts_with(fixture.root()))
            .ok_or_else(|| invalid("selected staging escaped its admitted namespace"))?;
    }
    for (directory, _) in &directories {
        #[cfg(test)]
        if FAIL_SELECTED_NAMESPACE.replace(false) { return Err(invalid("injected selected staging namespace barrier failure")); }
        crate::metadata_io::sync_dir(directory)?;
    }
    for (directory, before) in &directories {
        let after = std::fs::symlink_metadata(directory).map_err(|error| invalid(&error.to_string()))?;
        if !after.is_dir() || after.file_type().is_symlink() { return Err(invalid("selected staging namespace changed")); }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            if (before.dev(), before.ino()) != (after.dev(), after.ino()) { return Err(invalid("selected staging namespace was replaced")); }
        }
        #[cfg(not(unix))]
        if before.created().ok().is_none() || before.created().ok() != after.created().ok() { return Err(invalid("selected staging namespace identity unavailable or changed")); }
    }
    fixture.check()
}

#[cfg(test)]
thread_local! {
    static FAIL_SELECTED_NAMESPACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_SELECTED_AFTER_QUEUE_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_SELECTED_AFTER_PAGE_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static SELECTED_AFTER_PUBLISH_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static FAIL_SELECTED_DESCRIPTOR_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn before(deadline: Instant) -> Result<(), DfsError> {
    if Instant::now() >= deadline {
        Err(invalid(
            "repair custody transfer exhausted the original handoff deadline",
        ))
    } else {
        Ok(())
    }
}

/// One immutable disk-backed source cut; pages are read individually.
pub(crate) struct RepairHandoffExport {
    pub(crate) cut: DfsRepairHandoffCut,
    directory: PathBuf,
    selected_fixture: Option<SelectedOriginalFixtureIdentity>,
}

impl RepairHandoffExport {
    fn recertify_selected_export(&self, deadline: Instant) -> Result<(), DfsError> {
        let fixture = self.selected_fixture.as_ref().ok_or_else(|| invalid("source has no selected reader"))?;
        sync_selected_namespace(fixture, &self.directory)?;
        let mut generations = Vec::new();
        let mut predecessor = String::new();
        let mut count = 0u64;
        let mut last_key = None;
        for ordinal in 0..self.cut.page_count {
            before(deadline)?;
            let page = self.page(ordinal)?;
            if page.previous_digest != predecessor || page.records.first().is_some_and(|first| last_key.as_ref().is_some_and(|last| *last >= DfsRepairQueueKey::from_record(first))) {
                return Err(invalid("selected source lost its exact page/record chain"));
            }
            predecessor = page.digest()?;
            count = count.checked_add(page.records.len() as u64).ok_or_else(|| invalid("selected source count overflow"))?;
            if let Some(last) = page.records.last() { last_key = Some(DfsRepairQueueKey::from_record(last)); }
            let path = self.directory.join(format!("{ordinal}.sobj"));
            before(deadline)?;
            let generation = publish_and_certify_selected(&path, &page)?;
            generations.push((path, generation));
        }
        if count != self.cut.record_count || predecessor != self.cut.digest { return Err(invalid("selected source cut coverage changed")); }
        let path = self.directory.join("cut.sobj");
        before(deadline)?;
        let generation = publish_and_certify_selected(&path, &self.cut)?;
        generations.push((path, generation));
        sync_selected_namespace(fixture, &self.directory)?;
        for (path, metadata) in generations {
            let current = std::fs::symlink_metadata(&path).map_err(|error| invalid(&error.to_string()))?;
            if current.file_type().is_symlink() || !crate::repair_queue_store::same_file_observation(&metadata, &current) {
                return Err(invalid("selected source files changed across durability barriers"));
            }
        }
        before(deadline)
    }
    pub(crate) fn page(&self, ordinal: u64) -> Result<DfsRepairHandoffPage, DfsError> {
        let mut page: DfsRepairHandoffPage = read(&self.directory.join(format!("{ordinal}.sobj")))?
            .ok_or_else(|| invalid("original repair handoff page is unavailable"))?;
        if page.cut.version != self.cut.version
            || page.cut.handoff != self.cut.handoff
            || page.cut.queue_sequence != self.cut.queue_sequence
            || page.cut.selected_original_data != self.cut.selected_original_data
            || page.ordinal != ordinal
        {
            return Err(invalid(
                "original repair page belongs to another source cut",
            ));
        }
        page.cut = self.cut.clone();
        if let Some(identity) = &self.selected_fixture {
            identity.check()?;
            page.validate_selected_original_data()?;
        } else { page.validate()?; }
        Ok(page)
    }

    /// The caller retains actual mutation admission and the shared carrier
    /// drain exclusion, after the original metadata freeze is durable.
    pub(crate) fn capture(
        root: &Path,
        handoff: &DfsMetadataShardMembershipHandoff,
        store: &LocalDfsRepairQueueStore,
        belongs: impl FnMut(&DfsRepairAttemptRecord) -> Result<bool, DfsError>,
        deadline: Instant,
    ) -> Result<Self, DfsError> {
        SelectedOriginalFixtureIdentity::reject_default_root(root)?;
        Self::capture_inner(root, handoff, store, belongs, deadline, None)
    }

    /// Representation admission is not transfer authority. The caller still
    /// retains authenticated retiring-primary/native DATA/freeze/drain scope.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn capture_selected_original(
        fixture: &SelectedOriginalFixtureIdentity,
        handoff: &DfsMetadataShardMembershipHandoff,
        store: &LocalDfsRepairQueueStore,
        request_id: &str,
        belongs: impl FnMut(&DfsRepairAttemptRecord) -> Result<bool, DfsError>,
        deadline: Instant,
    ) -> Result<Self, DfsError> {
        store.validate_selected_original_fixture(fixture)?;
        let directory = directory(fixture.root(), &handoff.handoff_id, "source");
        if let Some(cut) = read::<DfsRepairHandoffCut>(&directory.join("cut.sobj"))? {
            cut.validate_selected_original_data()?;
            if cut.handoff != *handoff || cut.selected_original_data.as_ref().is_none_or(|selected| selected.request_id != request_id) {
                return Err(invalid("selected source cut changed across reopen"));
            }
            let export = Self { cut, directory, selected_fixture: Some(fixture.clone()) };
            export.recertify_selected_export(deadline)?;
            return Ok(export);
        }
        let (sequence, birth) = store.selected_original_handoff_data(&handoff.retiring_shard.primary_node_id, request_id)?;
        Self::capture_inner(fixture.root(), handoff, store, belongs, deadline, Some((fixture.clone(), sequence, birth)))
    }

    pub(crate) fn capture_native_layout(
        root: &Path,
        layout: &crate::DfsSelectedOriginalDataLayout,
        handoff: &DfsMetadataShardMembershipHandoff,
        belongs: impl FnMut(&DfsRepairAttemptRecord) -> Result<bool, DfsError>,
        deadline: Instant,
    ) -> Result<Self, DfsError> {
        let shard = crate::DfsMetadataShardId::new(&handoff.retiring_shard.shard_id);
        let stage = layout.stage(&handoff.retiring_shard.file_system_id, &shard, root)?;
        let store = layout.queue()?;
        if let Some(request) = layout.selected_request(handoff)? {
            let directory = directory(root, &handoff.handoff_id, "source");
            if let Some(cut) = read::<DfsRepairHandoffCut>(&directory.join("cut.sobj"))? {
                cut.validate_selected_original_data()?;
                if cut.handoff != *handoff || cut.selected_original_data.as_ref()
                    .is_none_or(|selected| selected.request_id != request)
                { return Err(invalid("native selected source changed its exact cut")); }
                let export = Self { cut, directory, selected_fixture: Some(stage) };
                export.recertify_selected_export(deadline)?;
                return Ok(export);
            }
            let (sequence, birth) = store.selected_original_handoff_data(
                &handoff.retiring_shard.primary_node_id, &request,
            )?;
            Self::capture_inner(root, handoff, &store, belongs, deadline, Some((stage, sequence, birth)))
        } else {
            // An admitted fixture may perform its earlier ordinary V1 setup.
            // Only a fixed genuine origin selects cut2, never the layout bit.
            Self::capture_inner(root, handoff, &store, belongs, deadline, None)
        }
    }

    fn capture_inner(
        root: &Path,
        handoff: &DfsMetadataShardMembershipHandoff,
        store: &LocalDfsRepairQueueStore,
        mut belongs: impl FnMut(&DfsRepairAttemptRecord) -> Result<bool, DfsError>,
        deadline: Instant,
        selected: Option<(SelectedOriginalFixtureIdentity, u64, RepairOriginalBirth)>,
    ) -> Result<Self, DfsError> {
        let selected_fixture = selected.as_ref().map(|(identity, _, _)| identity.clone());
        let selected_birth = selected.as_ref().map(|(_, _, birth)| birth.clone());
        let selected_cut = selected_birth.as_ref().map(|birth| -> Result<SelectedRepairOriginalDataCut, DfsError> { Ok(SelectedRepairOriginalDataCut {
            source_queue_incarnation: birth.root.subject.queue_incarnation.clone(),
            producer: birth.original.source_node_id.clone(),
            request_id: birth.root.subject.request_id.clone(),
            original_digest: birth.root.original_command_digest.clone(),
            captured_birth_digest: birth.captured_data_digest()?,
        }) }).transpose()?;
        let original_bytes = selected_birth.as_ref().map(crate::metadata_io::encode_sobj).transpose()?.map_or(0, |bytes| bytes.len());
        if original_bytes > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT - 4096 {
            return Err(invalid("selected original exceeds the existing bounded page budget"));
        }
        let directory = directory(root, &handoff.handoff_id, "source");
        if let Some(cut) = read::<DfsRepairHandoffCut>(&directory.join("cut.sobj"))? {
            cut.validate()?;
            if cut.version != VERSION || cut.handoff != *handoff {
                return Err(invalid(
                    "retained repair custody belongs to another handoff",
                ));
            }
            return Ok(Self { cut, directory, selected_fixture: None });
        }
        ensure_directory(root, &directory)?;
        if let Some(fixture) = &selected_fixture { sync_selected_namespace(fixture, &directory)?; }
        let mut cut = DfsRepairHandoffCut {
            version: if selected.is_some() { SELECTED_ORIGINAL_DATA_VERSION } else { VERSION },
            handoff: handoff.clone(),
            queue_sequence: 0,
            page_count: 0,
            record_count: 0,
            digest: String::new(),
            selected_original_data: selected_cut,
        };
        let mut after = None;
        let mut previous_digest = String::new();
        let mut records = Vec::new();
        let mut encoded_record_bytes = original_bytes;
        loop {
            before(deadline)?;
            let (sequence, rows, more) = store.handoff_page(after.as_ref())?;
            if selected.as_ref().is_some_and(|(_, expected, _)| *expected != sequence) {
                return Err(invalid("selected original and retry rows belong to different source cuts"));
            }
            if after.is_none() { cut.queue_sequence = sequence; }
            if sequence != cut.queue_sequence {
                return Err(invalid(
                    "repair queue changed while capturing its frozen handoff",
                ));
            }
            for row in rows {
                after = Some(DfsRepairQueueKey::from_record(&row));
                if !belongs(&row)? {
                    continue;
                }
                // Keep the same entry/byte limits as the metadata carrier.
                let row_bytes = crate::metadata_io::encode_sobj(&row)?.len();
                if row_bytes > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT - 4096 {
                    return Err(invalid(
                        "one repair custody row exceeds the existing page budget",
                    ));
                }
                if records.len() + (cut.page_count == 0 && selected_birth.is_some()) as usize == crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
                    || encoded_record_bytes + row_bytes
                        > crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT - 4096
                {
                    if records.is_empty() && !(cut.page_count == 0 && selected_birth.is_some()) {
                        return Err(invalid(
                            "one repair custody row exceeds the existing page budget",
                        ));
                    }
                    let page = DfsRepairHandoffPage {
                        cut: cut.clone(),
                        ordinal: cut.page_count,
                        previous_digest: previous_digest.clone(),
                        records: std::mem::take(&mut records),
                        selected_original_data: (cut.page_count == 0).then(|| selected_birth.clone().map(Box::new)).flatten(),
                    };
                    previous_digest = page.digest()?;
                    cut.record_count += page.records.len() as u64;
                    write_representation(&directory.join(format!("{}.sobj", cut.page_count)), &page, selected_fixture.is_some())?;
                    cut.page_count += 1;
                    encoded_record_bytes = 0;
                }
                encoded_record_bytes += row_bytes;
                records.push(row);
            }
            if !more {
                break;
            }
        }
        before(deadline)?;
        let page = DfsRepairHandoffPage {
            cut: cut.clone(),
            ordinal: cut.page_count,
            previous_digest,
            records,
            selected_original_data: (cut.page_count == 0).then(|| selected_birth.clone().map(Box::new)).flatten(),
        };
        cut.digest = page.digest()?;
        cut.record_count += page.records.len() as u64;
        write_representation(&directory.join(format!("{}.sobj", cut.page_count)), &page, selected_fixture.is_some())?;
        cut.page_count += 1;
        if selected_fixture.is_some() { cut.validate_selected_original_data()?; } else { cut.validate()?; }
        // Publish the immutable original cut last. Incomplete captures grant
        // no authority and may be rebuilt while the original source is frozen.
        let export = Self { cut, directory, selected_fixture };
        if export.selected_fixture.is_some() {
            // Validate every selected page before publishing its immutable cut.
            export.recertify_selected_export(deadline)?;
        } else { write(&export.directory.join("cut.sobj"), &export.cut)?; }
        Ok(export)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Received {
    cut: DfsRepairHandoffCut,
    next_page: u64,
    record_count: u64,
    digest: String,
    last_key: Option<DfsRepairQueueKey>,
    installed_pages: u64,
}

fn recertify_selected_received(
    fixture: &SelectedOriginalFixtureIdentity,
    directory: &Path,
    received: &Received,
    deadline: Instant,
) -> Result<(), DfsError> {
    fixture.check()?;
    sync_selected_namespace(fixture, directory)?;
    received.cut.validate_selected_original_data()?;
    if received.next_page > received.cut.page_count || received.installed_pages > received.next_page {
        return Err(invalid("selected staging cursor exceeds its exact cut"));
    }
    let mut predecessor = String::new();
    let mut count = 0u64;
    let mut last_key = None;
    let mut generations = Vec::new();
    for ordinal in 0..received.next_page {
        before(deadline)?;
        let path = directory.join(format!("{ordinal}.sobj"));
        let page: DfsRepairHandoffPage = read(&path)?.ok_or_else(|| invalid("acknowledged selected page is unavailable"))?;
        page.validate_selected_original_data()?;
        if page.cut != received.cut || page.ordinal != ordinal || page.previous_digest != predecessor
            || page.records.first().is_some_and(|first| last_key.as_ref().is_some_and(|last| *last >= DfsRepairQueueKey::from_record(first)))
        { return Err(invalid("selected replay lost its exact prefix/record order")); }
        count = count.checked_add(page.records.len() as u64).ok_or_else(|| invalid("selected row count overflow"))?;
        if let Some(last) = page.records.last() { last_key = Some(DfsRepairQueueKey::from_record(last)); }
        predecessor = page.digest()?;
        before(deadline)?;
        let generation = publish_and_certify_selected(&path, &page)?;
        generations.push((path, generation));
    }
    if count != received.record_count || predecessor != received.digest || last_key != received.last_key
        || (received.next_page == received.cut.page_count && (count != received.cut.record_count || predecessor != received.cut.digest))
    { return Err(invalid("selected checkpoint does not certify its exact known frontier")); }
    let path = directory.join("received.sobj");
    before(deadline)?;
    let generation = publish_and_certify_selected(&path, received)?;
    generations.push((path, generation));
    sync_selected_namespace(fixture, directory)?;
    for (path, before) in generations {
        let after = std::fs::symlink_metadata(&path).map_err(|error| invalid(&error.to_string()))?;
        if after.file_type().is_symlink() || !crate::repair_queue_store::same_file_observation(&before, &after) {
            return Err(invalid("selected staged file was replaced across durability barriers"));
        }
    }
    before(deadline)?;
    fixture.check()
}

/// Called only after native authenticated prefix/freeze validation. Stage the
/// exact original records durably while the successor is still a replica.
/// They cannot enter its active queue until its normal proof-bound rebind.
pub(crate) fn receive(root: &Path, page: &DfsRepairHandoffPage) -> Result<String, DfsError> {
    SelectedOriginalFixtureIdentity::reject_default_root(root)?;
    receive_inner(root, page, None)
}

/// Caller must already hold the existing authenticated native handoff scope.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn receive_selected_original_data(
    fixture: &SelectedOriginalFixtureIdentity,
    page: &DfsRepairHandoffPage,
    deadline: Instant,
) -> Result<String, DfsError> {
    fixture.check()?;
    before(deadline)?;
    receive_inner(fixture.root(), page, Some((fixture, deadline)))
}

fn receive_inner(root: &Path, page: &DfsRepairHandoffPage, selected: Option<(&SelectedOriginalFixtureIdentity, Instant)>) -> Result<String, DfsError> {
    if selected.is_some() { page.validate_selected_original_data()?; } else { page.validate()?; }
    if let Some((_, deadline)) = selected {
        native_selected_page_origin(root, page, deadline)?;
    }
    let directory = directory(root, &page.cut.handoff.handoff_id, "received");
    ensure_directory_before(root, &directory, selected.map(|(_, deadline)| deadline))?;
    if let Some((fixture, _)) = selected { sync_selected_namespace(fixture, &directory)?; }
    if let Some((_, deadline)) = selected { before(deadline)?; }
    let state_path = directory.join("received.sobj");
    let mut received = read::<Received>(&state_path)?.unwrap_or(Received {
        cut: page.cut.clone(),
        next_page: 0,
        record_count: 0,
        digest: String::new(),
        last_key: None,
        installed_pages: 0,
    });
    if received.cut != page.cut {
        return Err(invalid("repair handoff cut changed during replay"));
    }
    let page_path = directory.join(format!("{}.sobj", page.ordinal));
    if page.ordinal < received.next_page {
        if read::<DfsRepairHandoffPage>(&page_path)?.as_ref() != Some(page) {
            return Err(invalid(
                "repair handoff replay changed an acknowledged page",
            ));
        }
        if let Some((fixture, deadline)) = selected { recertify_selected_received(fixture, &directory, &received, deadline)?; }
        return page.confirmation();
    }
    if page.ordinal != received.next_page
        || page.previous_digest != received.digest
        || page.records.first().is_some_and(|first| {
            received
                .last_key
                .as_ref()
                .is_some_and(|last| *last >= DfsRepairQueueKey::from_record(first))
        })
    {
        return Err(invalid(
            "repair handoff page is missing its exact predecessor",
        ));
    }
    let count = received
        .record_count
        .checked_add(page.records.len() as u64)
        .ok_or_else(|| invalid("repair handoff record count overflow"))?;
    if count > page.cut.record_count
        || (page.ordinal + 1 == page.cut.page_count && count != page.cut.record_count)
    {
        return Err(invalid(
            "repair handoff does not cover its complete record count",
        ));
    }
    if let Some(previous) = read::<DfsRepairHandoffPage>(&page_path)? {
        if previous != *page {
            return Err(invalid("repair handoff changed a partially installed page"));
        }
    } else {
        if let Some((_, deadline)) = selected { before(deadline)?; }
        write_representation(&page_path, page, selected.is_some())?;
        #[cfg(test)]
        if selected.is_some() && FAIL_SELECTED_AFTER_PAGE_WRITE.replace(false) {
            return Err(invalid("injected crash after selected page publication before Received"));
        }
    }
    if let Some((_, deadline)) = selected { before(deadline)?; }
    received.next_page += 1;
    received.record_count = count;
    received.digest = page.digest()?;
    if let Some(last) = page.records.last() {
        received.last_key = Some(DfsRepairQueueKey::from_record(last));
    }
    if let Some((_, deadline)) = selected { before(deadline)?; }
    write_representation(&state_path, &received, selected.is_some())?;
    if let Some((fixture, deadline)) = selected { recertify_selected_received(fixture, &directory, &received, deadline)?; }
    page.confirmation()
}

/// Structural staged-prefix inspection before selected receiver mutations.
/// This supplies origin DATA for the backend's frozen-shard check, not an ACK.
pub(crate) fn native_selected_page_origin(
    root: &Path,
    page: &DfsRepairHandoffPage,
    deadline: Instant,
) -> Result<RepairOriginalBirth, DfsError> {
    before(deadline)?;
    page.validate_selected_original_data()?;
    let directory = directory(root, &page.cut.handoff.handoff_id, "received");
    let received = read::<Received>(&directory.join("received.sobj"))?.unwrap_or(Received {
        cut: page.cut.clone(), next_page: 0, record_count: 0,
        digest: String::new(), last_key: None, installed_pages: 0,
    });
    if received.cut != page.cut || received.next_page > page.cut.page_count
        || received.installed_pages > received.next_page || page.ordinal > received.next_page
    {
        return Err(invalid("selected page differs from or skips its staged prefix"));
    }
    // Readable files only constrain the proposed input. This pure inspection
    // cannot renew an ACK or certify the durability of a crash-replayed file.
    let mut predecessor = String::new();
    let mut count = 0u64;
    let mut last_key = None;
    let mut first = None;
    for ordinal in 0..received.next_page {
        before(deadline)?;
        let prior = read::<DfsRepairHandoffPage>(&directory.join(format!("{ordinal}.sobj")))?
            .ok_or_else(|| invalid("selected prefix lost a staged page"))?;
        prior.validate_selected_original_data()?;
        if prior.cut != page.cut || prior.ordinal != ordinal || prior.previous_digest != predecessor
            || prior.records.first().is_some_and(|record| last_key.as_ref()
                .is_some_and(|last| *last >= DfsRepairQueueKey::from_record(record)))
        { return Err(invalid("selected staged prefix changed its exact chain/order")); }
        if ordinal == page.ordinal && prior != *page {
            return Err(invalid("selected duplicate changed staged material"));
        }
        count = count.checked_add(prior.records.len() as u64)
            .ok_or_else(|| invalid("selected staged prefix count overflow"))?;
        predecessor = prior.digest()?;
        if let Some(last) = prior.records.last() { last_key = Some(DfsRepairQueueKey::from_record(last)); }
        if ordinal == 0 { first = Some(prior); }
    }
    if count != received.record_count || predecessor != received.digest || last_key != received.last_key
        || (received.next_page == page.cut.page_count
            && (count != page.cut.record_count || predecessor != page.cut.digest))
    { return Err(invalid("selected staged cursor differs from its exact prefix")); }
    if page.ordinal == received.next_page {
        if page.previous_digest != predecessor || page.records.first().is_some_and(|record|
            last_key.as_ref().is_some_and(|last| *last >= DfsRepairQueueKey::from_record(record)))
        { return Err(invalid("selected incoming page has no exact predecessor/order")); }
        let count = count.checked_add(page.records.len() as u64)
            .ok_or_else(|| invalid("selected incoming page count overflow"))?;
        if count > page.cut.record_count
            || (page.ordinal + 1 == page.cut.page_count && count != page.cut.record_count)
        { return Err(invalid("selected incoming page changes total coverage")); }
        if read::<DfsRepairHandoffPage>(&directory.join(format!("{}.sobj", page.ordinal)))?
            .is_some_and(|pending| pending != *page)
        { return Err(invalid("selected incoming page conflicts with a pending crash-replayed page")); }
    }
    let first = if page.ordinal == 0 { page } else {
        first.as_ref().ok_or_else(|| invalid("selected page lost its original material"))?
    };
    before(deadline)?;
    first.selected_original_data.as_ref().map(|birth| birth.as_ref().clone())
        .ok_or_else(|| invalid("selected page has no genuine original material"))
}

/// Authenticated ingress supplies the layout and the once-captured receiver
/// budget. Neither the page version nor this helper grants transfer authority.
pub(crate) fn receive_native_layout(
    root: &Path,
    layout: &crate::DfsSelectedOriginalDataLayout,
    page: &DfsRepairHandoffPage,
    receiver_deadline: Option<Instant>,
) -> Result<String, DfsError> {
    let shard = crate::DfsMetadataShardId::new(&page.cut.handoff.retiring_shard.shard_id);
    let stage = layout.stage(&page.cut.handoff.retiring_shard.file_system_id, &shard, root)?;
    if page.cut.version == SELECTED_ORIGINAL_DATA_VERSION {
        let deadline = receiver_deadline.ok_or_else(|| invalid("selected DATA has no explicit receiver processing budget"))?;
        if Instant::now() >= deadline {
            return Err(invalid("selected receiver processing budget expired; DATA may already exist"));
        }
        let result = receive_inner(root, page, Some((&stage, deadline)));
        if Instant::now() >= deadline {
            return Err(invalid("selected receiver processing budget expired; DATA/uncertainty retained without ACK"));
        }
        result
    } else {
        page.validate()?;
        let result = receive_inner(root, page, None);
        stage.check()?;
        result
    }
}

pub(crate) fn install_native_layout(
    root: &Path,
    layout: &crate::DfsSelectedOriginalDataLayout,
    cut: &DfsRepairHandoffCut,
    processing_deadline: Option<Instant>,
) -> Result<bool, DfsError> {
    let owner = crate::runtime_host::publication_runtime_owner(root)
        .map_err(|error| invalid(&error.to_string()))?
        .ok_or_else(|| invalid("selected installation has no actual native DATA owner"))?;
    layout.check_owner(&owner)?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(owner.physical_data_root());
    let state = sinomo_core::cluster::read_cluster_state_without_kv(
        &sinomo_core::network::ClusterPaths::new(&data_dir),
    ).map_err(|error| invalid(&error.to_string()))?;
    if state.local_node_id != cut.handoff.desired_shard.primary_node_id {
        return Err(invalid("selected installation belongs to another actual native node"));
    }
    let shard = crate::DfsMetadataShardId::new(&cut.handoff.retiring_shard.shard_id);
    let stage = layout.stage(&cut.handoff.retiring_shard.file_system_id, &shard, root)?;
    let store = layout.queue()?;
    let (complete, changed) = if cut.version == SELECTED_ORIGINAL_DATA_VERSION {
        cut.validate_selected_original_data()?;
        let processing_deadline = processing_deadline.ok_or_else(|| invalid("selected install requires an explicit owner processing budget"))?;
        if Instant::now() >= processing_deadline { return Err(invalid("selected install processing budget expired")); }
        let outcome = install_staged_page_inner(root, cut, &store,
            Some((&stage, layout.queue_layout(), processing_deadline)));
        if Instant::now() >= processing_deadline {
            return Err(invalid("selected install processing budget expired; DATA retained, activation pending"));
        }
        outcome?
    } else {
        cut.validate()?;
        install_staged_page_inner(root, cut, &store, None)?
    };
    layout.check_owner(&owner)?;
    stage.check()?;
    if changed {
        crate::runtime_host::request_daemon_dfs_maintenance_reconciliation(
            &crate::runtime_host::DfsRuntimePaths::new(&data_dir),
        ).map_err(|error| invalid(&error.to_string()))?;
    }
    Ok(complete)
}

/// Install one bounded page while the actual successor's native metadata
/// writer is still frozen. Each cursor follows the normal queue fsync. Rebind
/// remains pending until the full cut is installed, including after restart.
pub(crate) fn install_next_page(root: &Path, cut: &DfsRepairHandoffCut) -> Result<bool, DfsError> {
    SelectedOriginalFixtureIdentity::reject_default_root(root)?;
    cut.validate()?;
    let owner = crate::runtime_host::publication_runtime_owner(root)
        .map_err(|error| invalid(&error.to_string()))?
        .ok_or_else(|| invalid("repair custody activation has no actual DATA owner"))?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(owner.physical_data_root());
    let state = sinomo_core::cluster::read_cluster_state_without_kv(
        &sinomo_core::network::ClusterPaths::new(&data_dir),
    )
    .map_err(|error| invalid(&error.to_string()))?;
    if state.local_node_id != cut.handoff.desired_shard.primary_node_id {
        return Err(invalid(
            "repair custody activation belongs to another native node",
        ));
    }
    let store =
        LocalDfsRepairQueueStore::new(crate::default_dfs_repair_attempt_snapshot_path(&data_dir));
    let (complete, changed) = install_staged_page(root, cut, &store)?;
    if changed {
        crate::runtime_host::request_daemon_dfs_maintenance_reconciliation(
            &crate::runtime_host::DfsRuntimePaths::new(&data_dir),
        )
        .map_err(|error| invalid(&error.to_string()))?;
    }
    Ok(complete)
}

fn install_staged_page(
    root: &Path,
    cut: &DfsRepairHandoffCut,
    store: &LocalDfsRepairQueueStore,
) -> Result<(bool, bool), DfsError> {
    SelectedOriginalFixtureIdentity::reject_default_root(root)?;
    cut.validate()?;
    install_staged_page_inner(root, cut, store, None)
}

/// Called only AFTER proof verification by the native handoff owner; the
/// fixture token admits the representation and supplies no transfer authority.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn install_selected_original_data(
    fixture: &SelectedOriginalFixtureIdentity,
    cut: &DfsRepairHandoffCut,
    store: &LocalDfsRepairQueueStore,
    deadline: Instant,
) -> Result<bool, DfsError> {
    store.validate_selected_original_fixture(fixture)?;
    cut.validate_selected_original_data()?;
    before(deadline)?;
    Ok(install_staged_page_inner(fixture.root(), cut, store, Some((fixture, fixture, deadline)))?.0)
}

fn install_staged_page_inner(
    root: &Path,
    cut: &DfsRepairHandoffCut,
    store: &LocalDfsRepairQueueStore,
    selected: Option<(&SelectedOriginalFixtureIdentity, &SelectedOriginalFixtureIdentity, Instant)>,
) -> Result<(bool, bool), DfsError> {
    let directory = directory(root, &cut.handoff.handoff_id, "received");
    let state_path = directory.join("received.sobj");
    let mut received: Received = read(&state_path)?
        .ok_or_else(|| invalid("original repair custody is unavailable at activation"))?;
    if received.cut != *cut
        || received.next_page != cut.page_count
        || received.record_count != cut.record_count
        || received.digest != cut.digest
        || received.installed_pages > cut.page_count
    {
        return Err(invalid(
            "original repair custody is incomplete or changed at activation",
        ));
    }
    if let Some((fixture, queue_layout, deadline)) = selected {
        recertify_selected_received(fixture, &directory, &received, deadline)?;
        // Page zero covers the one selected original even when its retry row
        // is absent. Repeated certification never allocates a new install ID.
        let first: DfsRepairHandoffPage = read(&directory.join("0.sobj"))?
            .ok_or_else(|| invalid("selected original page disappeared"))?;
        first.validate_selected_original_data()?;
        let material = RepairOriginalImportMaterial {
            cut: cut.clone(),
            birth: *first.selected_original_data.clone().ok_or_else(|| invalid("selected birth is missing"))?,
        };
        if received.installed_pages == cut.page_count {
            before(deadline)?;
            store.install_original_handoff_data(&material, queue_layout.root())?;
            before(deadline)?;
            return Ok((true, false));
        }
    } else if received.installed_pages == cut.page_count { return Ok((true, false)); }
    let page: DfsRepairHandoffPage =
        read(&directory.join(format!("{}.sobj", received.installed_pages)))?.ok_or_else(|| {
            invalid("received original repair page disappeared before activation")
        })?;
    if selected.is_some() { page.validate_selected_original_data()?; } else { page.validate()?; }
    if page.cut != *cut || page.ordinal != received.installed_pages {
        return Err(invalid(
            "received repair page differs from its activation cut",
        ));
    }
    if page.ordinal + 1 < cut.page_count {
        let next: DfsRepairHandoffPage =
            read(&directory.join(format!("{}.sobj", page.ordinal + 1)))?
                .ok_or_else(|| invalid("received original repair page chain is incomplete"))?;
        if next.cut != *cut
            || next.ordinal != page.ordinal + 1
            || next.previous_digest != page.digest()?
        {
            return Err(invalid("received repair page changed after acknowledgment"));
        }
    }
    if let Some((fixture, queue_layout, deadline)) = selected {
        before(deadline)?;
        let first: DfsRepairHandoffPage = read(&directory.join("0.sobj"))?.ok_or_else(|| invalid("selected original page disappeared"))?;
        first.validate_selected_original_data()?;
        store.install_original_handoff_page(&RepairOriginalImportMaterial {
            cut: cut.clone(),
            birth: *first.selected_original_data.ok_or_else(|| invalid("selected original is missing"))?,
        }, &page.records, queue_layout.root())?;
        #[cfg(test)]
        if FAIL_SELECTED_AFTER_QUEUE_SYNC.replace(false) { return Err(invalid("injected crash after queue sync before install cursor")); }
        before(deadline)?;
    } else { store.install_handoff_records(&page.records)?; }
    received.installed_pages += 1;
    write_representation(&state_path, &received, selected.is_some())?;
    if let Some((fixture, queue_layout, deadline)) = selected { recertify_selected_received(fixture, &directory, &received, deadline)?; }
    Ok((received.installed_pages == cut.page_count, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repair_queue_store::DfsRepairQueueOperation;
    use crate::{
        DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardHandoffCause,
        DfsMetadataShardMembershipHandoffFence,
    };
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::network::NetworkDeliveryState;
    use sinomo_core::node::NodeId;
    use std::time::Duration;

    // Pure persistence DATA tests. These construct no transport, metadata
    // author, publication permit or acknowledged native handoff.
    fn handoff() -> DfsMetadataShardMembershipHandoff {
        let old = NodeId::from_legacy_named("repair-custody-data-old").unwrap();
        let new = NodeId::from_legacy_named("repair-custody-data-new").unwrap();
        let retiring = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: old.clone(),
            replica_node_ids: vec![new.clone()],
            epoch: 4,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: new,
            replica_node_ids: vec![old.clone()],
            epoch: 5,
            ..retiring.clone()
        };
        DfsMetadataShardMembershipHandoff {
            cause: DfsMetadataShardHandoffCause::Membership,
            handoff_id: "repair-custody-data-handoff".into(),
            retiring_shard: retiring,
            desired_shard: desired,
            required_prepared_sequence: 17,
            required_head_watermark: None,
            retiring_primary_fence: Some(DfsMetadataShardMembershipHandoffFence {
                handoff_id: "repair-custody-data-handoff".into(),
                primary_node_id: old,
                journal_sequence: 17,
                state_fingerprint: "data-only-fingerprint".into(),
            }),
        }
    }
    fn row(index: usize) -> DfsRepairAttemptRecord {
        DfsRepairAttemptRecord {
            intent_id: format!("original-{index:04}"),
            object_id: crate::DfsObjectId::from_legacy_named(format!("object-{index}")).unwrap(),
            version: crate::DfsVersion::from_legacy_named(format!("version-{index}")).unwrap(),
            content_hash: format!("blake3:original-{index}"),
            missing_replica_node_id: NodeId::from_legacy_named("repair-custody-data-target")
                .unwrap(),
            attempts: index as u32,
            in_flight: index % 4 == 1,
            last_attempt_unix_nanos: index as u64,
            next_retry_unix_nanos: 1000 + index as u64,
            transfer_request_id: (index % 4 == 1).then(|| format!("original-request-{index}")),
            last_error: Some("retained original diagnostic".into()),
            transfer_delivery_state: match index % 4 {
                0 => Some(NetworkDeliveryState::DefinitelyNotSent),
                1 => Some(NetworkDeliveryState::PossiblyApplied),
                _ => None,
            },
            completed_transfer_request_id: (index % 4 == 2)
                .then(|| format!("original-completion-{index}")),
            transfer_kind: if index % 4 == 3 {
                None
            } else {
                Some(DfsRepairTransferKind::Repair)
            },
        }
    }
    #[test]
    fn pages_preserve_exact_unknown_named_and_completed_custody_through_restart() {
        let root = sinomo_testdir::TestDir::new("repair-handoff-page-restart");
        let source = LocalDfsRepairQueueStore::new(root.join("source-queue.sobj"));
        let destination = LocalDfsRepairQueueStore::new(root.join("destination-queue.sobj"));
        std::fs::create_dir(root.join("source")).unwrap();
        std::fs::create_dir(root.join("target")).unwrap();
        let rows = (0..513).map(row).collect::<Vec<_>>();
        source.append_enqueues(rows.clone()).unwrap();
        let export = RepairHandoffExport::capture(
            &root.join("source"),
            &handoff(),
            &source,
            |_| Ok(true),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(export.cut.page_count, 2);
        assert_eq!(export.cut.record_count, 513);
        let first = export.page(0).unwrap();
        let last = export.page(1).unwrap();
        let target = root.join("target");
        assert!(receive(&target, &last).is_err());
        assert!(destination.view().unwrap().records.is_empty());
        receive(&target, &first).unwrap();
        assert!(destination.view().unwrap().records.is_empty());
        assert!(install_staged_page(&target, &export.cut, &destination).is_err());
        receive(&target, &last).unwrap();
        assert_eq!(
            install_staged_page(&target, &export.cut, &destination).unwrap(),
            (false, true)
        );
        destination.reload_from_disk_on_next_access();
        assert_eq!(destination.view().unwrap().records, rows[..512]);
        // Durable rows survived, but no page receipt yet: replay the exact
        // interrupted install, not a generated successful protocol response.
        let directory = directory(&target, &last.cut.handoff.handoff_id, "received");
        write(&directory.join("1.sobj"), &last).unwrap();
        destination.install_handoff_records(&last.records).unwrap();
        destination.reload_from_disk_on_next_access();
        assert_eq!(
            install_staged_page(&target, &export.cut, &destination).unwrap(),
            (true, true)
        );
        assert_eq!(destination.view().unwrap().records, rows);
        let receipt: Received = read(&directory.join("received.sobj")).unwrap().unwrap();
        assert_eq!(receipt.next_page, export.cut.page_count);
        assert_eq!(receipt.record_count, export.cut.record_count);
        assert_eq!(receipt.digest, export.cut.digest);
        let mut later = rows[0].clone();
        later.transfer_request_id = Some("later-actual-queue-DATA".into());
        later.transfer_delivery_state = None;
        destination
            .append_operations(vec![DfsRepairQueueOperation::Set {
                record: later.clone(),
            }])
            .unwrap();
        receive(&target, &first).unwrap();
        assert_eq!(
            install_staged_page(&target, &export.cut, &destination).unwrap(),
            (true, false)
        );
        assert_eq!(
            destination
                .records_for_targets(&later.intent_id, &[later.missing_replica_node_id.clone()])
                .unwrap(),
            vec![later]
        );
        assert_eq!(source.view().unwrap().records, rows);
    }
    #[test]
    fn conflicting_corrupt_unsupported_and_incomplete_custody_never_acknowledges() {
        let root = sinomo_testdir::TestDir::new("repair-handoff-negative-pages");
        let source = LocalDfsRepairQueueStore::new(root.join("source.sobj"));
        let destination = LocalDfsRepairQueueStore::new(root.join("destination.sobj"));
        std::fs::create_dir(root.join("source")).unwrap();
        std::fs::create_dir(root.join("target")).unwrap();
        std::fs::create_dir(root.join("cancelled")).unwrap();
        let original = row(1);
        source.append_enqueues([original.clone()]).unwrap();
        let export = RepairHandoffExport::capture(
            &root.join("source"),
            &handoff(),
            &source,
            |_| Ok(true),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let page = export.page(0).unwrap();
        let target = root.join("target");
        for kind in 0..4 {
            let mut changed = page.clone();
            match kind {
                0 => changed.cut.version += 1,
                1 => changed.records[0].transfer_request_id = Some("replacement".into()),
                2 => changed.cut.record_count += 1,
                _ => changed.ordinal += 1,
            }
            assert!(receive(&target, &changed).is_err());
            assert!(destination.view().unwrap().records.is_empty());
        }
        let mut conflict = original.clone();
        conflict.transfer_request_id = Some("different-existing-owner".into());
        destination.append_enqueues([conflict.clone()]).unwrap();
        receive(&target, &page).unwrap();
        assert!(install_staged_page(&target, &export.cut, &destination).is_err());
        assert_eq!(destination.view().unwrap().records, vec![conflict]);
        assert_eq!(source.view().unwrap().records, vec![original]);
        assert!(
            RepairHandoffExport::capture(
                &root.join("cancelled"),
                &handoff(),
                &source,
                |_| Ok(true),
                Instant::now()
            )
            .is_err()
        );
    }

    #[test]
    fn custody_marker_roundtrips_and_cannot_downgrade_to_metadata_only_freeze() {
        #[derive(Deserialize)]
        struct OlderMarker {
            handoff_id: String,
        }
        let root = sinomo_testdir::TestDir::new("repair-handoff-marker-compatibility");
        let source = LocalDfsRepairQueueStore::new(root.join("queue.sobj"));
        let export = RepairHandoffExport::capture(
            &root,
            &handoff(),
            &source,
            |_| Ok(true),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let mut marker = crate::DfsMetadataShardWriteFreeze {
            handoff_id: export.cut.handoff.handoff_id.clone(),
            retiring_epoch: 4,
            activated_epoch: None,
            journal_sequence: 0,
            state_fingerprint: "data-only".into(),
            rebind_prefix_proof: crate::metadata_records::dfs_metadata_prefix_proof_genesis(
                &crate::DfsMetadataShardId::new("root"),
            ),
            repair_custody: None,
        };
        let legacy = serde_json::to_value(&marker).unwrap();
        assert_eq!(
            serde_json::from_value::<OlderMarker>(legacy.clone())
                .unwrap()
                .handoff_id,
            marker.handoff_id
        );
        assert_eq!(
            serde_json::from_value::<crate::DfsMetadataShardWriteFreeze>(legacy).unwrap(),
            marker
        );
        marker.repair_custody = Some(export.cut.clone());
        let current = serde_json::to_value(&marker).unwrap();
        assert!(
            serde_json::from_value::<OlderMarker>(current.clone()).is_err(),
            "old readers must not silently ignore the original repair obligation"
        );
        assert_eq!(
            serde_json::from_value::<crate::DfsMetadataShardWriteFreeze>(current.clone()).unwrap(),
            marker
        );
        let bytes = crate::metadata_io::encode_sobj(&marker).unwrap();
        let reopened: crate::DfsMetadataShardWriteFreeze =
            sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(reopened, marker);
        let mut future = current;
        future["repair_custody_v1"]["repair_custody"]["version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<crate::DfsMetadataShardWriteFreeze>(future).is_err());
    }

    // These exercise real local files, the queue writer and cold replay. The
    // static handoff fixture supplies NO native authority or process-outage proof.
    struct SelectedDataFixture {
        _root: sinomo_testdir::TestDir,
        source: LocalDfsRepairQueueStore,
        destination: LocalDfsRepairQueueStore,
        source_identity: SelectedOriginalFixtureIdentity,
        destination_identity: SelectedOriginalFixtureIdentity,
        export: RepairHandoffExport,
        original: crate::DfsRoutedMessage,
        _content: crate::LocalContentStore,
    }

    fn selected_data_fixture(label: &str, legacy_rows: usize) -> SelectedDataFixture {
        let root = sinomo_testdir::TestDir::new(label);
        let (source, content, original, attachment, payload, source_identity) =
            crate::repair_queue_store::tests::selected_original_fixture(&root.join("source"));
        source.enroll_original_repair(&original, &attachment, &payload).unwrap();
        source.record_original_repair_attempt(&original).unwrap();
        source.record_original_repair_delivery(&original, Some(NetworkDeliveryState::PossiblyApplied)).unwrap();
        let mut pending = source.view().unwrap().records[0].clone();
        let crate::DfsNetworkMessage::RepairReplica(command) = &original.message else { panic!("Repair fixture"); };
        pending.transfer_request_id = Some(command.request_id.clone());
        pending.transfer_delivery_state = Some(NetworkDeliveryState::PossiblyApplied);
        pending.attempts = 1;
        source.append_operations(vec![DfsRepairQueueOperation::Set { record: pending }]).unwrap();
        source.append_enqueues((0..legacy_rows).map(row)).unwrap();
        let mut transfer = handoff();
        transfer.retiring_shard = command.authority_shard.clone().unwrap();
        transfer.desired_shard = DfsMetadataShardConfig {
            primary_node_id: transfer.desired_shard.primary_node_id.clone(),
            replica_node_ids: vec![original.source_node_id.clone()],
            epoch: transfer.retiring_shard.epoch + 1,
            ..transfer.retiring_shard.clone()
        };
        transfer.retiring_primary_fence.as_mut().unwrap().primary_node_id = original.source_node_id.clone();
        let export = RepairHandoffExport::capture_selected_original(
            &source_identity, &transfer, &source, &command.request_id, |_| Ok(true), Instant::now() + Duration::from_secs(5),
        ).unwrap();
        let (destination, destination_identity) = LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("destination")).unwrap();
        SelectedDataFixture { _root: root, source, destination, source_identity, destination_identity, export, original, _content: content }
    }

    fn stage_selected(fixture: &SelectedDataFixture) {
        for ordinal in 0..fixture.export.cut.page_count {
            let acknowledgement = receive_selected_original_data(&fixture.destination_identity, &fixture.export.page(ordinal).unwrap(), Instant::now() + Duration::from_secs(5)).unwrap();
            assert!(acknowledgement.contains(":selected-original-data-staged-v2:"));
        }
    }

    fn install_selected(fixture: &SelectedDataFixture) {
        while !install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).unwrap() {}
    }

    fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        fn visit(root: &Path, directory: &Path, result: &mut Vec<(PathBuf, Vec<u8>)>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() { visit(root, &path, result); }
                else { result.push((path.strip_prefix(root).unwrap().to_owned(), std::fs::read(&path).unwrap())); }
            }
        }
        let mut result = Vec::new(); visit(root, root, &mut result); result.sort_by(|a, b| a.0.cmp(&b.0)); result
    }

    #[test]
    fn selected_original_data_preserves_all_rows_history_and_nonexecution_after_compaction_restart() {
        let mut fixture = selected_data_fixture("selected-original-data-restart", 513);
        assert_eq!(fixture.export.cut.record_count, 514);
        assert_eq!(fixture.export.cut.page_count, 2);
        let source_rows = fixture.source.view().unwrap().records;
        stage_selected(&fixture);
        install_selected(&fixture);
        assert_eq!(fixture.destination.view().unwrap().records, source_rows);
        let selected = fixture.export.cut.selected_original_data.as_ref().unwrap();
        let original = fixture.destination.installed_original_handoff_data(&selected.producer, &selected.request_id).unwrap().unwrap();
        assert_eq!(original.material.birth.original, fixture.original);
        assert_eq!(original.material.birth.captured_data_digest().unwrap(), selected.captured_birth_digest);
        let crate::DfsNetworkMessage::RepairReplica(command) = &fixture.original.message else { panic!("Repair fixture"); };
        assert!(fixture.destination.original_repair_for_slot(&command.intent_id, &fixture.original.target_node_id).unwrap().is_none());
        assert!(fixture.destination.validate_original_repair_dispatch(&fixture.original).is_err());
        assert!(fixture.destination.record_original_repair_attempt(&fixture.original).is_err());
        assert!(fixture.destination.closed_repair_birth(&original.material.birth.root.subject).unwrap().is_none());
        let head = fixture.destination.view().unwrap().head_sequence;
        assert!(install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).unwrap());
        assert_eq!(fixture.destination.view().unwrap().head_sequence, head, "duplicate creates no logical import/sequence");
        fixture.destination.compact_if_needed(crate::DfsRepairQueueCompactionOptions { force: true, ..Default::default() }).unwrap();
        let path = fixture.destination_identity.root().join("repair.sobj");
        drop(fixture.destination);
        LocalDfsRepairQueueStore::evict_for_test(&path);
        fixture.destination = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&fixture.destination_identity).unwrap();
        assert_eq!(fixture.destination.installed_original_handoff_data(&selected.producer, &selected.request_id).unwrap(), Some(original));
        assert_eq!(fixture.destination.view().unwrap().records, source_rows);
        assert!(install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).unwrap());
    }

    #[test]
    fn selected_original_data_rejects_changed_history_on_first_import_and_changed_cut_on_duplicate() {
        let fixture = selected_data_fixture("selected-original-data-history-conflict", 0);
        let page = fixture.export.page(0).unwrap();
        let material = RepairOriginalImportMaterial { cut: fixture.export.cut.clone(), birth: *page.selected_original_data.clone().unwrap() };
        let mut changed = material.clone();
        changed.birth.attempts[0].sequence = 1;
        let before = files(fixture.destination_identity.root());
        assert!(fixture.destination.install_original_handoff_data(&changed, fixture.destination_identity.root()).is_err());
        assert_eq!(files(fixture.destination_identity.root()), before);
        stage_selected(&fixture); install_selected(&fixture);
        let mut changed = material;
        changed.cut.digest = "0".repeat(64);
        let before = files(fixture.destination_identity.root());
        assert!(fixture.destination.install_original_handoff_data(&changed, fixture.destination_identity.root()).is_err());
        assert_eq!(files(fixture.destination_identity.root()), before);
    }

    #[test]
    fn selected_original_data_recovers_queue_sync_before_cursor_without_reallocating_import() {
        let mut fixture = selected_data_fixture("selected-original-data-cursor-crash", 0);
        stage_selected(&fixture);
        FAIL_SELECTED_AFTER_QUEUE_SYNC.set(true);
        assert!(install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).is_err());
        assert!(!FAIL_SELECTED_AFTER_QUEUE_SYNC.replace(false));
        let state: Received = read(&directory(fixture.destination_identity.root(), &fixture.export.cut.handoff.handoff_id, "received").join("received.sobj")).unwrap().unwrap();
        assert_eq!(state.installed_pages, 0);
        let selected = fixture.export.cut.selected_original_data.as_ref().unwrap();
        let imported = fixture.destination.installed_original_handoff_data(&selected.producer, &selected.request_id).unwrap().unwrap();
        let path = fixture.destination_identity.root().join("repair.sobj");
        drop(fixture.destination); LocalDfsRepairQueueStore::evict_for_test(&path);
        fixture.destination = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&fixture.destination_identity).unwrap();
        install_selected(&fixture);
        assert_eq!(fixture.destination.installed_original_handoff_data(&selected.producer, &selected.request_id).unwrap(), Some(imported));
    }

    #[test]
    fn selected_original_data_incomplete_pages_never_install_an_origin() {
        let fixture = selected_data_fixture("selected-original-data-partial", 513);
        receive_selected_original_data(&fixture.destination_identity, &fixture.export.page(0).unwrap(), Instant::now() + Duration::from_secs(5)).unwrap();
        assert!(install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).is_err());
        let selected = fixture.export.cut.selected_original_data.as_ref().unwrap();
        assert!(fixture.destination.installed_original_handoff_data(&selected.producer, &selected.request_id).unwrap().is_none());
        assert!(fixture.destination.view().unwrap().records.is_empty());
    }

    #[test]
    fn selected_original_data_default_readers_refuse_without_mutation() {
        let fixture = selected_data_fixture("selected-original-data-default-reader", 0);
        let before_source = files(fixture.source_identity.root());
        let before_destination = files(fixture.destination_identity.root());
        let page = fixture.export.page(0).unwrap();
        assert!(receive(fixture.destination_identity.root(), &page).is_err());
        assert!(install_staged_page(fixture.destination_identity.root(), &fixture.export.cut, &fixture.destination).is_err());
        assert!(RepairHandoffExport::capture(fixture.source_identity.root(), &fixture.export.cut.handoff, &fixture.source, |_| Ok(true), Instant::now() + Duration::from_secs(5)).is_err());
        let default = LocalDfsRepairQueueStore::new(fixture.destination_identity.root().join("repair.sobj"));
        assert!(default.view().is_err());
        assert_eq!(files(fixture.source_identity.root()), before_source);
        assert_eq!(files(fixture.destination_identity.root()), before_destination);
    }

    #[test]
    fn selected_original_data_namespace_failure_cannot_renew_staged_ack() {
        let fixture = selected_data_fixture("selected-original-data-ack-barrier", 0);
        let page = fixture.export.page(0).unwrap();
        receive_selected_original_data(&fixture.destination_identity, &page, Instant::now() + Duration::from_secs(5)).unwrap();
        FAIL_SELECTED_NAMESPACE.set(true);
        assert!(receive_selected_original_data(&fixture.destination_identity, &page, Instant::now() + Duration::from_secs(5)).is_err());
        assert!(!FAIL_SELECTED_NAMESPACE.replace(false));
        assert!(fixture.destination.view().unwrap().records.is_empty());
    }

    #[test]
    fn selected_original_data_queue_namespace_failure_preserves_import_and_pending_cursor() {
        let fixture = selected_data_fixture("selected-original-data-queue-barrier", 0);
        stage_selected(&fixture);
        crate::repair_queue_store::REPAIR_QUEUE_FAIL_SELECTED_NAMESPACE.set(true);
        assert!(install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).is_err());
        assert!(!crate::repair_queue_store::REPAIR_QUEUE_FAIL_SELECTED_NAMESPACE.replace(false));
        let state: Received = read(&directory(fixture.destination_identity.root(), &fixture.export.cut.handoff.handoff_id, "received").join("received.sobj")).unwrap().unwrap();
        assert_eq!(state.installed_pages, 0);
        install_selected(&fixture);
    }

    #[test]
    fn selected_original_data_identical_file_replacement_between_barriers_is_rejected() {
        let fixture = selected_data_fixture("selected-original-data-physical-barrier", 0);
        stage_selected(&fixture); install_selected(&fixture);
        fixture.destination.compact_if_needed(crate::DfsRepairQueueCompactionOptions { force: true, ..Default::default() }).unwrap();
        let path = fixture.destination_identity.root().join("repair.sobj");
        let replacement = path.with_extension("unsynchronized-replacement");
        let bytes = std::fs::read(&path).unwrap();
        crate::repair_queue_store::REPAIR_QUEUE_SELECTED_IMPORT_BARRIER_HOOK.set(Some(Box::new(move || {
            std::fs::write(&replacement, &bytes).unwrap();
            std::fs::rename(&replacement, &path).unwrap();
        })));
        assert!(install_selected_original_data(&fixture.destination_identity, &fixture.export.cut, &fixture.destination, Instant::now() + Duration::from_secs(5)).is_err());
        assert!(crate::repair_queue_store::REPAIR_QUEUE_SELECTED_IMPORT_BARRIER_HOOK.take().is_none());
    }

    #[test]
    fn selected_original_data_fixture_rejects_existing_and_replaced_roots() {
        let fixture = selected_data_fixture("selected-original-data-layout", 0);
        let before = files(fixture.destination_identity.root());
        assert!(LocalDfsRepairQueueStore::new_selected_original_fixture(fixture.destination_identity.root()).is_err());
        assert_eq!(files(fixture.destination_identity.root()), before);
        let saved = fixture.destination_identity.root().with_extension("original-directory");
        std::fs::rename(fixture.destination_identity.root(), &saved).unwrap();
        std::fs::create_dir(fixture.destination_identity.root()).unwrap();
        let marker = "selected-original-data-fixture-v2.sobj";
        std::fs::copy(saved.join(marker), fixture.destination_identity.root().join(marker)).unwrap();
        assert!(LocalDfsRepairQueueStore::reopen_selected_original_fixture(&fixture.destination_identity).is_err());
        assert!(fixture.destination.view().is_err());
    }
    #[test]
    fn selected_original_data_cannot_be_reenrolled_as_a_native_local_birth() {
        let fixture = selected_data_fixture("selected-original-data-reenroll", 0);
        stage_selected(&fixture); install_selected(&fixture);
        let crate::DfsNetworkMessage::RepairReplica(command) = &fixture.original.message else { panic!("Repair fixture"); };
        let content = crate::LocalContentStore::new(fixture.destination_identity.root().join("content")).unwrap();
        let manifest = content.put_object_version(command.object_id.as_str(), command.version.as_str(), b"complete original attachment").unwrap();
        assert_eq!(manifest.content_hash, command.content_hash);
        let descriptor = fixture.export.page(0).unwrap().selected_original_data.unwrap().attachment;
        let payload = content.capture_repair_payload(crate::content_store::RepairPayloadSubject {
            queue_incarnation: fixture.destination.custody_incarnation().unwrap(),
            producer: fixture.original.source_node_id.to_string(),
            target: fixture.original.target_node_id.to_string(),
            intent_id: command.intent_id.clone(),
            request_id: command.request_id.clone(),
        }, crate::repair_queue_store::original_repair_digest(&fixture.original, &descriptor).unwrap(),
            command.object_id.as_str(), command.version.as_str(), &command.content_hash, descriptor.total_size_bytes).unwrap();
        let before = fixture.destination.view().unwrap();
        assert!(fixture.destination.enroll_original_repair(&fixture.original, &descriptor, &payload).is_err());
        assert_eq!(fixture.destination.view().unwrap(), before);
    }

    #[test]
    fn selected_original_data_mismatched_fixture_store_pairs_fail_before_any_mutation() {
        let first = selected_data_fixture("selected-original-owner-one", 0);
        let second = selected_data_fixture("selected-original-owner-two", 0);
        stage_selected(&first);
        let before_first = files(first.destination_identity.root());
        let before_second = files(second.destination_identity.root());
        let before_source = files(first.source_identity.root());
        let before_other_source = files(second.source_identity.root());
        let request = &first.export.cut.selected_original_data.as_ref().unwrap().request_id;
        assert!(RepairHandoffExport::capture_selected_original(&first.source_identity, &first.export.cut.handoff, &second.source, request, |_| Ok(true), Instant::now() + Duration::from_secs(5)).is_err());
        assert!(install_selected_original_data(&first.destination_identity, &first.export.cut, &second.destination, Instant::now() + Duration::from_secs(5)).is_err());
        assert_eq!(files(first.source_identity.root()), before_source);
        assert_eq!(files(second.source_identity.root()), before_other_source);
        assert_eq!(files(first.destination_identity.root()), before_first);
        assert_eq!(files(second.destination_identity.root()), before_second);
        let ordinary_root = first._root.join("ordinary");
        std::fs::create_dir(&ordinary_root).unwrap();
        let ordinary = LocalDfsRepairQueueStore::new(ordinary_root.join("repair.sobj"));
        assert!(install_selected_original_data(&first.destination_identity, &first.export.cut, &ordinary, Instant::now() + Duration::from_secs(5)).is_err());
        let material = RepairOriginalImportMaterial { cut: first.export.cut.clone(), birth: *first.export.page(0).unwrap().selected_original_data.unwrap() };
        let error = ordinary.install_original_handoff_data(&material, &ordinary_root).unwrap_err();
        assert!(error.to_string().contains("ordinary queue"));
        assert!(files(&ordinary_root).is_empty(), "no writer lock or initializer is created");
        assert_eq!(files(first.destination_identity.root()), before_first);
    }

    #[test]
    fn selected_original_data_conflicting_import_cannot_append_accompanying_rows() {
        let fixture = selected_data_fixture("selected-original-atomic-conflict", 0);
        stage_selected(&fixture); install_selected(&fixture);
        let mut material = RepairOriginalImportMaterial { cut: fixture.export.cut.clone(), birth: *fixture.export.page(0).unwrap().selected_original_data.unwrap() };
        material.cut.digest = "0".repeat(64);
        let before = files(fixture.destination_identity.root());
        let head = fixture.destination.view().unwrap();
        assert!(fixture.destination.install_original_handoff_page(&material, &[row(9000)], fixture.destination_identity.root()).is_err());
        assert_eq!(fixture.destination.view().unwrap(), head);
        assert_eq!(files(fixture.destination_identity.root()), before);
    }

    #[test]
    fn selected_original_data_publication_replacement_requires_descriptor_sync_before_ack() {
        let fixture = selected_data_fixture("selected-original-publish-replacement", 0);
        let page = fixture.export.page(0).unwrap();
        receive_selected_original_data(&fixture.destination_identity, &page, Instant::now() + Duration::from_secs(5)).unwrap();
        let path = directory(fixture.destination_identity.root(), &fixture.export.cut.handoff.handoff_id, "received").join("0.sobj");
        let replacement = path.with_extension("uncertified-replacement");
        let bytes = std::fs::read(&path).unwrap();
        SELECTED_AFTER_PUBLISH_HOOK.set(Some(Box::new(move || {
            std::fs::write(&replacement, &bytes).unwrap();
            std::fs::rename(&replacement, &path).unwrap();
        })));
        FAIL_SELECTED_DESCRIPTOR_SYNC.set(true);
        assert!(receive_selected_original_data(&fixture.destination_identity, &page, Instant::now() + Duration::from_secs(5)).is_err());
        assert!(SELECTED_AFTER_PUBLISH_HOOK.take().is_none());
        assert!(!FAIL_SELECTED_DESCRIPTOR_SYNC.replace(false));
        assert!(fixture.destination.view().unwrap().records.is_empty());
        assert!(receive_selected_original_data(&fixture.destination_identity, &page, Instant::now() + Duration::from_secs(5)).unwrap().contains("selected-original-data-staged-v2"));
    }

    #[test]
    fn selected_pending_page_conflict_is_rejected_by_pure_preflight_without_mutation() {
        let fixture = selected_data_fixture("selected-pending-page-conflict", 513);
        let page = fixture.export.page(0).unwrap();
        assert_eq!(page.cut.page_count, 2);
        FAIL_SELECTED_AFTER_PAGE_WRITE.set(true);
        let error = receive_selected_original_data(&fixture.destination_identity, &page,
            Instant::now() + Duration::from_secs(5)).unwrap_err();
        assert!(error.to_string().contains("after selected page publication"));
        let received = directory(fixture.destination_identity.root(), &page.cut.handoff.handoff_id, "received");
        assert!(received.join("0.sobj").is_file());
        assert!(!received.join("received.sobj").exists());
        let mut changed = page.clone();
        changed.records[0].attempts += 1;
        changed.validate_selected_original_data().unwrap();
        let before_files = files(fixture.destination_identity.root());
        let error = native_selected_page_origin(fixture.destination_identity.root(), &changed,
            Instant::now() + Duration::from_secs(5)).unwrap_err();
        assert!(error.to_string().contains("pending crash-replayed page"));
        assert_eq!(files(fixture.destination_identity.root()), before_files);
        let original = native_selected_page_origin(fixture.destination_identity.root(), &page,
            Instant::now() + Duration::from_secs(5)).unwrap();
        assert_eq!(original.original, fixture.original);
        // Pure readability above did not create Received or an ACK. The actual
        // writer must synchronize the unchanged page before confirming it.
        assert_eq!(files(fixture.destination_identity.root()), before_files);
        receive_selected_original_data(&fixture.destination_identity, &page,
            Instant::now() + Duration::from_secs(5)).unwrap();
        assert!(received.join("received.sobj").is_file());
    }

    #[test]
    fn selected_recertification_expiry_does_not_publish_another_cursor() {
        let fixture = selected_data_fixture("selected-expiry-before-cursor", 0);
        stage_selected(&fixture);
        let dir = directory(fixture.destination_identity.root(), &fixture.export.cut.handoff.handoff_id, "received");
        let path = dir.join("received.sobj");
        let state: Received = read(&path).unwrap().unwrap();
        let before_cursor = std::fs::symlink_metadata(&path).unwrap();
        struct ClearPublishHook;
        impl Drop for ClearPublishHook {
            fn drop(&mut self) { SELECTED_AFTER_PUBLISH_HOOK.take(); }
        }
        let _clear = ClearPublishHook;
        let reached = std::rc::Rc::new(std::cell::Cell::new(false));
        let observed = std::rc::Rc::clone(&reached);
        let deadline = Instant::now() + Duration::from_millis(50);
        SELECTED_AFTER_PUBLISH_HOOK.set(Some(Box::new(move || {
            observed.set(true);
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
        })));
        let result = recertify_selected_received(&fixture.destination_identity, &dir, &state, deadline);
        assert!(reached.get(), "the exact post-publication fault cut must execute");
        assert!(result.is_err());
        assert!(crate::repair_queue_store::same_file_observation(
            &before_cursor, &std::fs::symlink_metadata(&path).unwrap()));
        assert_eq!(read::<Received>(&path).unwrap(), Some(state));
        assert!(!fixture.destination.installed_original_handoff_data(&fixture.original.source_node_id,
            fixture.export.cut.selected_original_data.as_ref().unwrap().request_id.as_str()).unwrap().is_some());
    }

}
