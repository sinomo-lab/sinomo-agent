//! Conversion of node-local pins, repair queues, and retired maintenance cursors.
//! The normal component data owner retains custody during every call.
mod mounted;
mod repair;

use serde_json::Value;
use sinomo_core::object::{LengthMode, serde_ext as sobj};
use sinomo_core::storage::durable_file::{
    DurableAtomicFileOptions, sync_directory, write_file_atomic,
};
use sinomo_dfs::{
    DfsAntiEntropyCursorRecord, DfsPinRecord, DfsStoragePreparation, MetadataBackendError,
};
use std::collections::BTreeMap;
use std::fs::{self, ReadDir};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, MetadataBackendError>;
const MARKER: &str = "local-state-converted-v8";
const MARKER_BYTES: &[u8] = b"dfs-local-state-converted-v8\n";
fn error(value: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("local DFS storage conversion: {value}"))
}
#[derive(Clone, Copy)]
enum Kind {
    Pins,
    AntiEntropy,
    Replication,
    MountedRoot,
    MountedSession,
    MountedOperation,
}
struct Session {
    directories: Vec<(Kind, ReadDir)>,
    repair: Option<repair::Conversion>,
    pending_mounted: Option<(mounted::Role, PathBuf)>,
}
#[derive(Default)]
pub(crate) struct LocalStateConverter {
    sessions: Mutex<BTreeMap<PathBuf, Arc<Mutex<Option<Session>>>>>,
}
impl LocalStateConverter {
    pub(crate) fn prepare(&self, root: &Path, budget: usize) -> Result<DfsStoragePreparation> {
        if !root.exists() {
            return Ok(DfsStoragePreparation::Ready);
        }
        let root = root.canonicalize().map_err(error)?;
        let marker = root.join(MARKER);
        if marker.exists() {
            if bounded_read(&marker)? != MARKER_BYTES {
                return Err(error("local storage conversion marker is malformed"));
            }
            // This completed root contract also covers later native queue
            // appends. Their current owner validates them without conversion.
            return Ok(DfsStoragePreparation::Ready);
        }
        let slot = {
            let mut sessions = self.sessions.lock().map_err(error)?;
            Arc::clone(
                sessions
                    .entry(root.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(None))),
            )
        };
        let mut owned_session = slot.lock().map_err(error)?;
        if owned_session.is_none() {
            let mut directories = Vec::new();
            for (kind, name) in [
                (Kind::Pins, "pins"),
                (Kind::AntiEntropy, "anti-entropy-cursors"),
                (Kind::Replication, "metadata-replication-cursors"),
                (Kind::MountedRoot, "mounted-mutation-outbox"),
            ] {
                let path = root.join(name);
                if path.exists() {
                    if !fs::symlink_metadata(&path).map_err(error)?.is_dir() {
                        return Err(error("conversion directory is not a physical directory"));
                    }
                    directories.push((kind, fs::read_dir(&path).map_err(error)?));
                }
            }
            *owned_session = Some(Session {
                directories,
                repair: repair::Conversion::open(&root)?,
                pending_mounted: None,
            });
        }
        let result = (|| {
            let session = owned_session
                .as_mut()
                .expect("owned local conversion session");
            let mut processed = 0;
            if let Some((role, path)) = &session.pending_mounted {
                if !mounted::convert(&root, path, *role)? {
                    return Ok(DfsStoragePreparation::Pending {
                        processed_records: 0,
                    });
                }
                session.pending_mounted = None;
                processed += 1;
            }
            if let Some(repair) = session.repair.as_mut() {
                let (ready, count) = repair.step(budget.max(1))?;
                processed += count;
                if !ready {
                    return Ok(DfsStoragePreparation::Pending {
                        processed_records: processed,
                    });
                }
                session.repair = None;
            }
            while processed < budget.max(1) {
                let Some((kind, entries)) = session.directories.last_mut() else {
                    write_file_atomic(
                        &marker,
                        MARKER_BYTES,
                        DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
                    )
                    .map_err(error)?;
                    return Ok(DfsStoragePreparation::Ready);
                };
                let kind = *kind;
                let Some(entry) = entries.next() else {
                    session.directories.pop();
                    continue;
                };
                let entry = entry.map_err(error)?;
                processed += 1;
                let path = entry.path();
                let ty = entry.file_type().map_err(error)?;
                if ty.is_symlink() {
                    return Err(error("source side-file symlink is forbidden"));
                }
                if ty.is_dir() {
                    let next = match kind {
                        Kind::Replication => Some(kind),
                        Kind::MountedRoot => {
                            match path.file_name().and_then(|name| name.to_str()) {
                                Some("sessions" | "preparing-sessions" | "retired-sessions") => {
                                    Some(Kind::MountedSession)
                                }
                                Some(
                                    "operations" | "preparing-operations" | "retired-operations",
                                ) => Some(Kind::MountedOperation),
                                _ => None,
                            }
                        }
                        Kind::MountedSession | Kind::MountedOperation => Some(kind),
                        _ => None,
                    };
                    if let Some(next) = next {
                        session
                            .directories
                            .push((next, fs::read_dir(path).map_err(error)?));
                    }
                    continue;
                }
                if !ty.is_file() {
                    return Err(error("source side file is not regular"));
                }
                let mounted_role = match (kind, path.file_name().and_then(|name| name.to_str())) {
                    (Kind::MountedRoot, Some("daemon-committed-work-latch.sobj")) => {
                        Some(mounted::Role::Latch)
                    }
                    (Kind::MountedSession, Some("state.sobj")) => Some(mounted::Role::Session),
                    (Kind::MountedOperation, Some("state.sobj")) => Some(mounted::Role::Operation),
                    (Kind::MountedOperation, Some("cleanup-authorized.sobj")) => {
                        Some(mounted::Role::CleanupAuthorization)
                    }
                    _ => None,
                };
                if let Some(role) = mounted_role {
                    if !mounted::convert(&root, &path, role)? {
                        session.pending_mounted = Some((role, path));
                        return Ok(DfsStoragePreparation::Pending {
                            processed_records: processed,
                        });
                    }
                    continue;
                }
                match (kind, path.extension().and_then(|x| x.to_str())) {
                    (Kind::Pins, Some("sobj")) => convert_pin(&root, &path)?,
                    (Kind::AntiEntropy, Some("json")) => convert_anti_entropy(&root, &path)?,
                    (Kind::Replication, Some("json")) => {
                        let value: Value =
                            serde_json::from_slice(&bounded_read(&path)?).map_err(error)?;
                        if value
                            .get("target_node_id")
                            .and_then(Value::as_str)
                            .is_none()
                            || value.get("shard_id").and_then(Value::as_str).is_none()
                            || value
                                .get("next_after_sequence")
                                .and_then(Value::as_u64)
                                .is_none()
                        {
                            return Err(error("malformed retired replication cursor"));
                        }
                        // Its sequence names the SOURCE chain, not the converted native
                        // chain. Preserve it as evidence; native replication resumes
                        // from its own certified prefix and current cursor.
                        archive(&root, &path)?;
                    }
                    _ => {}
                }
            }
            Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            })
        })();
        if result.is_err() || matches!(result, Ok(DfsStoragePreparation::Ready)) {
            owned_session.take();
        }
        result
    }
}
fn bounded_read(path: &Path) -> Result<Vec<u8>> {
    if fs::metadata(path).map_err(error)?.len() > 16 * 1024 * 1024 {
        return Err(error("source local record exceeds 16 MiB"));
    }
    fs::read(path).map_err(error)
}
fn source_id(value: &Value, field: &str) -> Result<sinomo_core::Id> {
    let source = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| error(format!("source omitted {field}")))?;
    sinomo_core::Id::new(source)
        .or_else(|_| sinomo_core::Id::from_legacy_named(source))
        .map_err(error)
}
fn segment(value: &impl ToString) -> String {
    value
        .to_string()
        .trim_matches('/')
        .replace(['/', '\\', ':'], "_")
}
fn pin_path(root: &Path, record: &DfsPinRecord) -> PathBuf {
    root.join("pins").join(format!(
        "{}-{}-{}-{}.sobj",
        segment(&record.file_system_id),
        blake3::hash(record.path.trim_matches('/').as_bytes()).to_hex(),
        segment(&record.pinned_by_node_id),
        segment(&record.mode)
    ))
}
fn convert_pin(root: &Path, path: &Path) -> Result<()> {
    let bytes = bounded_read(path)?;
    let mut value: Value = sobj::from_bytes(&bytes).map_err(error)?;
    for field in ["file_system_id", "pinned_by_node_id"] {
        value[field] = serde_json::to_value(source_id(&value, field)?).map_err(error)?;
    }
    if value.get("object_id").is_some_and(|id| !id.is_null()) {
        value["object_id"] =
            serde_json::to_value(source_id(&value, "object_id")?).map_err(error)?;
    }
    let record: DfsPinRecord = serde_json::from_value(value).map_err(error)?;
    let current = pin_path(root, &record);
    if current == path {
        return Ok(());
    }
    let encoded = sobj::to_bytes(&record, 1, LengthMode::U32).map_err(error)?;
    if current.exists() {
        let existing: DfsPinRecord = sobj::from_bytes(&bounded_read(&current)?).map_err(error)?;
        if existing != record {
            return Err(error("retained pin conflicts with an existing current pin"));
        }
    } else {
        write_file_atomic(
            &current,
            &encoded,
            DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
        )
        .map_err(error)?;
    }
    archive(root, path)
}
fn convert_anti_entropy(root: &Path, path: &Path) -> Result<()> {
    let value: Value = serde_json::from_slice(&bounded_read(path)?).map_err(error)?;
    let file_system_id = source_id(&value, "file_system_id")?;
    let current = root
        .join("anti-entropy-cursors")
        .join(format!("{}.sobj", segment(&file_system_id)));
    if current.exists() {
        let existing: DfsAntiEntropyCursorRecord =
            sobj::from_bytes(&bounded_read(&current)?).map_err(error)?;
        if existing.file_system_id != file_system_id {
            return Err(error(
                "current anti-entropy cursor changed filesystem identity",
            ));
        }
    } else {
        // The old walk cursor refers to the retired table layout. Restart the
        // current bounded walk instead of skipping unvisited native rows.
        let record = DfsAntiEntropyCursorRecord {
            file_system_id,
            next_cursor: None,
        };
        write_file_atomic(
            &current,
            &sobj::to_bytes(&record, 1, LengthMode::U32).map_err(error)?,
            DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
        )
        .map_err(error)?;
    }
    archive(root, path)
}
fn archive(root: &Path, path: &Path) -> Result<()> {
    let relative = path.strip_prefix(root).map_err(error)?;
    let bytes = bounded_read(path)?;
    let destination = root
        .join("migration-retained/local-state")
        .join(blake3::hash(&bytes).to_hex().as_str())
        .join(relative);
    fs::create_dir_all(
        destination
            .parent()
            .ok_or_else(|| error("archive has no parent"))?,
    )
    .map_err(error)?;
    if destination.exists() {
        if bounded_read(&destination)? != bytes {
            return Err(error("retained local source evidence changed"));
        }
        sync_archive_parents(root, &destination)?;
        fs::remove_file(path).map_err(error)?;
    } else {
        fs::rename(path, &destination).map_err(error)?;
        sync_archive_parents(root, &destination)?;
    }
    sync_directory(path.parent().ok_or_else(|| error("source has no parent"))?).map_err(error)?;
    Ok(())
}

fn sync_archive_parents(root: &Path, destination: &Path) -> Result<()> {
    let mut parent = destination.parent();
    while let Some(directory) = parent {
        sync_directory(directory).map_err(error)?;
        if directory == root {
            return Ok(());
        }
        parent = directory.parent();
    }
    Err(error("source archive escaped its owned root"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_dfs::{DfsPinMode, LocalDfsPinStore};
    use time::OffsetDateTime;

    fn old_pin(root: &Path) -> (PathBuf, DfsPinRecord) {
        let record = DfsPinRecord {
            file_system_id: sinomo_core::Id::from_legacy_named("local-state-fs").unwrap(),
            path: "/users/pinned".into(),
            object_id: None,
            pinned_by_node_id: sinomo_core::Id::from_legacy_named("local-state-node").unwrap(),
            mode: DfsPinMode::Content,
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let path = root.join("pins").join(format!(
            "{}-{}-{}.sobj",
            segment(&record.file_system_id),
            blake3::hash(record.path.trim_matches('/').as_bytes()).to_hex(),
            segment(&record.pinned_by_node_id)
        ));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, sobj::to_bytes(&record, 1, LengthMode::U32).unwrap()).unwrap();
        (path, record)
    }
    #[test]
    fn partial_conversion_resumes_without_exposing_retired_pins_or_cursor_positions() {
        let root = sinomo_testdir::TestDir::new("local-state-conversion-resume");
        let (old, record) = old_pin(&root);
        assert!(
            LocalDfsPinStore::new(root.join("pins"))
                .list_pins(None)
                .is_err()
        );
        let cursor = root.join("metadata-replication-cursors/peer/root.json");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        fs::write(&cursor,serde_json::to_vec(&serde_json::json!({"target_node_id":record.pinned_by_node_id,"shard_id":"root","next_after_sequence":91})).unwrap()).unwrap();
        let converter = LocalStateConverter::default();
        assert!(matches!(
            converter.prepare(&root, 1).unwrap(),
            DfsStoragePreparation::Pending {
                processed_records: 1
            }
        ));
        drop(converter);
        let converter = LocalStateConverter::default();
        for _ in 0..20 {
            if converter.prepare(&root, 1).unwrap() == DfsStoragePreparation::Ready {
                break;
            }
        }
        assert_eq!(
            converter.prepare(&root, 1).unwrap(),
            DfsStoragePreparation::Ready
        );
        assert!(!old.exists());
        assert!(!cursor.exists());
        assert_eq!(
            LocalDfsPinStore::new(root.join("pins"))
                .list_pins(None)
                .unwrap(),
            vec![record]
        );
        assert!(root.join("migration-retained/local-state").is_dir());
    }
    #[test]
    fn conversion_preserves_both_inputs_when_current_pin_disagrees() {
        let root = sinomo_testdir::TestDir::new("local-state-pin-collision");
        let (old, mut record) = old_pin(&root);
        record.created_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
        LocalDfsPinStore::new(root.join("pins"))
            .put_pin(&record)
            .unwrap();
        assert!(LocalStateConverter::default().prepare(&root, 16).is_err());
        assert!(old.exists());
        assert!(pin_path(&root, &record).exists());
        assert!(!root.join(MARKER).exists());
    }
}
