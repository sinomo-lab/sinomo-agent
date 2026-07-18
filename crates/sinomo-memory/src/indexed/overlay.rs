use super::{
    CONTINUATION_KEY_BYTES, IndexedFileMemoryStore, MemoryOverlayStore, OverlayOperation,
    OverlayOperationId, OverlayPageCursor, OverlaySnapshotId, Page,
};
use crate::{MemoryError, decode_sobj, encode_sobj};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::Id;
use sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent;
use std::fs::File;
use std::io::{Read, Take};
use std::path::{Path, PathBuf};

/// Page and object bounds keep replay memory proportional to the caller's
/// request instead of the lifetime size of an overlay stream.
const MAX_OVERLAY_PAGE_SIZE: usize = 64;
const MAX_OVERLAY_OPERATION_BYTES: u64 = 1024 * 1024;
const MAX_OVERLAY_METADATA_BYTES: u64 = 1024 * 1024;
const MAX_OVERLAY_PENDING_BYTES: u64 = MAX_OVERLAY_OPERATION_BYTES + MAX_OVERLAY_METADATA_BYTES;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct OverlayStreamHead {
    stream_id: Id,
    last_sequence: u64,
    snapshot_id: OverlaySnapshotId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct OverlaySnapshotManifest {
    snapshot_id: OverlaySnapshotId,
    stream_id: Id,
    watermark_sequence: u64,
    previous_snapshot_id: Option<OverlaySnapshotId>,
    last_operation_id: OverlayOperationId,
}

impl OverlaySnapshotManifest {
    fn canonical(
        stream_id: Id,
        watermark_sequence: u64,
        previous_snapshot_id: Option<OverlaySnapshotId>,
        last_operation_id: OverlayOperationId,
    ) -> Self {
        let snapshot_id = Self::canonical_id(
            &stream_id,
            watermark_sequence,
            previous_snapshot_id.as_ref(),
            &last_operation_id,
        );
        Self {
            snapshot_id,
            stream_id,
            watermark_sequence,
            previous_snapshot_id,
            last_operation_id,
        }
    }

    fn expected_snapshot_id(&self) -> OverlaySnapshotId {
        Self::canonical_id(
            &self.stream_id,
            self.watermark_sequence,
            self.previous_snapshot_id.as_ref(),
            &self.last_operation_id,
        )
    }

    fn canonical_id(
        stream_id: &Id,
        watermark_sequence: u64,
        previous_snapshot_id: Option<&OverlaySnapshotId>,
        last_operation_id: &OverlayOperationId,
    ) -> OverlaySnapshotId {
        Id::derived(
            "memory-overlay-snapshot",
            serde_json::to_vec(&(
                stream_id,
                watermark_sequence,
                previous_snapshot_id,
                last_operation_id,
            ))
            .expect("overlay snapshot fingerprint is serializable"),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct OverlaySequenceEntry {
    sequence: u64,
    operation_id: OverlayOperationId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct OverlayOperationIndex {
    operation_id: OverlayOperationId,
    sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct PendingOverlayAppend {
    stream_id: Id,
    sequence: u64,
    previous_snapshot_id: Option<OverlaySnapshotId>,
    operation: OverlayOperation,
}

impl MemoryOverlayStore for IndexedFileMemoryStore {
    fn append_overlay(&self, operation: OverlayOperation) -> Result<(), MemoryError> {
        self.append_overlay_operation(operation)
    }

    fn overlay_page(
        &self,
        cursor: Option<&OverlayPageCursor>,
        limit: usize,
    ) -> Result<Page<OverlayOperation, OverlayPageCursor>, MemoryError> {
        self.overlay_operation_page(cursor, limit)
    }
}

impl IndexedFileMemoryStore {
    /// Appends one immutable overlay object. Identical retries are idempotent;
    /// the same canonical id can never be rebound to different content.
    pub fn append_overlay_operation(&self, operation: OverlayOperation) -> Result<(), MemoryError> {
        let expected = operation.expected_operation_id();
        if operation.operation_id != expected {
            return Err(MemoryError::NonCanonicalOverlayOperationId {
                expected,
                actual: operation.operation_id,
            });
        }

        let path = self.overlay_operation_path(&operation.operation_id);
        let encoded = encode_sobj(&path, &operation)?;
        ensure_overlay_size(&path, encoded.len() as u64)?;

        // The process mutex and store-wide advisory lock serialize sequence
        // allocation and recover a single interrupted append before admitting
        // another writer.
        let _guard = self.write_guard()?;
        self.recover_pending_overlay_append()?;

        if let Some(existing) = read_overlay_operation_optional(&path)? {
            if existing != operation {
                return Err(MemoryError::ImmutableOverlayOperationCollision {
                    operation_id: operation.operation_id,
                    path: path.display().to_string(),
                });
            }
            let index = self
                .read_overlay_operation_index(&operation.operation_id)?
                .ok_or_else(|| MemoryError::InvalidOverlayObject {
                    path: path.display().to_string(),
                    detail: "operation exists without its append-sequence index".to_string(),
                })?;
            self.validate_overlay_sequence_entry(index.sequence, &operation.operation_id)?;
            return Ok(());
        }

        let head = self.load_overlay_stream_head()?;
        let sequence = head.as_ref().map_or(Ok(1), |head| {
            head.last_sequence
                .checked_add(1)
                .ok_or(MemoryError::SequenceOverflow)
        })?;
        let pending = PendingOverlayAppend {
            stream_id: head
                .as_ref()
                .map(|head| head.stream_id.clone())
                .unwrap_or_else(Id::generate),
            sequence,
            previous_snapshot_id: head.map(|head| head.snapshot_id),
            operation,
        };
        let pending_path = self.overlay_pending_path();
        let pending_bytes = encode_sobj(&pending_path, &pending)?;
        ensure_overlay_sobj_size(
            &pending_path,
            pending_bytes.len() as u64,
            MAX_OVERLAY_PENDING_BYTES,
        )?;
        self.write_sobj_atomic(&pending_path, &pending)?;
        self.finish_pending_overlay_append(&pending)
    }

    /// Replays a bounded append-sequence range from an immutable snapshot.
    /// Every page performs O(limit) direct reads regardless of stream age.
    pub fn overlay_operation_page(
        &self,
        cursor: Option<&OverlayPageCursor>,
        limit: usize,
    ) -> Result<Page<OverlayOperation, OverlayPageCursor>, MemoryError> {
        if limit == 0 || limit > MAX_OVERLAY_PAGE_SIZE {
            return Err(MemoryError::InvalidOverlayPageLimit {
                limit,
                max: MAX_OVERLAY_PAGE_SIZE,
            });
        }

        let (snapshot, next_sequence) = match cursor {
            Some(cursor) => {
                self.validate_overlay_cursor(cursor)?;
                let snapshot = self.load_overlay_snapshot(&cursor.snapshot_id)?;
                if cursor.next_sequence > snapshot.watermark_sequence {
                    return Err(MemoryError::InvalidMemoryContinuation {
                        detail: format!(
                            "overlay cursor sequence {} exceeds snapshot {} watermark {}",
                            cursor.next_sequence, cursor.snapshot_id, snapshot.watermark_sequence
                        ),
                    });
                }
                (snapshot, cursor.next_sequence)
            }
            None => {
                let Some(head) = self.load_overlay_stream_head()? else {
                    return Ok(Page {
                        items: Vec::new(),
                        continuation: None,
                    });
                };
                let snapshot = self.load_overlay_snapshot(&head.snapshot_id)?;
                if snapshot.stream_id != head.stream_id
                    || snapshot.watermark_sequence != head.last_sequence
                {
                    return Err(MemoryError::InvalidOverlayObject {
                        path: self.overlay_head_path().display().to_string(),
                        detail: "stream head does not match its immutable snapshot".to_string(),
                    });
                }
                (snapshot, 1)
            }
        };

        let limit_u64 = u64::try_from(limit).expect("overlay page limit fits u64");
        let last_sequence = next_sequence
            .saturating_add(limit_u64.saturating_sub(1))
            .min(snapshot.watermark_sequence);
        let item_count = last_sequence
            .checked_sub(next_sequence)
            .and_then(|distance| distance.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
                detail: "overlay cursor range is not addressable".to_string(),
            })?;
        let mut items = Vec::with_capacity(item_count);
        for sequence in next_sequence..=last_sequence {
            let entry = self.read_overlay_sequence_entry(sequence)?;
            let path = self.overlay_operation_path(&entry.operation_id);
            let operation = read_overlay_operation_required(&path)?;
            validate_overlay_operation_at_path(&path, &entry.operation_id, &operation)?;
            items.push(operation);
        }

        let following_sequence = last_sequence
            .checked_add(1)
            .ok_or(MemoryError::SequenceOverflow)?;
        let continuation = if following_sequence <= snapshot.watermark_sequence {
            Some(self.sign_overlay_cursor(snapshot.snapshot_id.clone(), following_sequence)?)
        } else {
            None
        };
        Ok(Page {
            items,
            continuation,
        })
    }

    fn recover_pending_overlay_append(&self) -> Result<(), MemoryError> {
        let Some(pending) = read_overlay_sobj_optional_with_limit::<PendingOverlayAppend>(
            &self.overlay_pending_path(),
            MAX_OVERLAY_PENDING_BYTES,
        )?
        else {
            return Ok(());
        };
        self.finish_pending_overlay_append(&pending)
    }

    fn sign_overlay_cursor(
        &self,
        snapshot_id: OverlaySnapshotId,
        next_sequence: u64,
    ) -> Result<OverlayPageCursor, MemoryError> {
        let key = self.ensure_continuation_key()?;
        let authenticator = overlay_cursor_authenticator(&key, &snapshot_id, next_sequence)?;
        Ok(OverlayPageCursor {
            snapshot_id,
            next_sequence,
            authenticator,
        })
    }

    fn validate_overlay_cursor(&self, cursor: &OverlayPageCursor) -> Result<(), MemoryError> {
        if cursor.next_sequence == 0 {
            return Err(MemoryError::InvalidMemoryContinuation {
                detail: "overlay cursor sequence must be one-based".to_string(),
            });
        }
        let key = self.ensure_continuation_key()?;
        let expected =
            overlay_cursor_authenticator(&key, &cursor.snapshot_id, cursor.next_sequence)?;
        if cursor.authenticator != expected {
            return Err(MemoryError::InvalidMemoryContinuation {
                detail: "overlay cursor authenticator is invalid".to_string(),
            });
        }
        Ok(())
    }

    fn finish_pending_overlay_append(
        &self,
        pending: &PendingOverlayAppend,
    ) -> Result<(), MemoryError> {
        let expected = pending.operation.expected_operation_id();
        if pending.operation.operation_id != expected {
            return Err(MemoryError::NonCanonicalOverlayOperationId {
                expected,
                actual: pending.operation.operation_id.clone(),
            });
        }
        let head = self.load_overlay_stream_head()?;
        if let Some(head) = &head {
            if pending.stream_id != head.stream_id {
                return Err(MemoryError::InvalidOverlayObject {
                    path: self.overlay_pending_path().display().to_string(),
                    detail: "pending append belongs to a different overlay stream".to_string(),
                });
            }
            if head.last_sequence >= pending.sequence {
                self.validate_completed_pending_append(head, pending)?;
                remove_file_if_exists(&self.overlay_pending_path())?;
                return Ok(());
            }
            let expected_sequence = head
                .last_sequence
                .checked_add(1)
                .ok_or(MemoryError::SequenceOverflow)?;
            if pending.sequence != expected_sequence
                || pending.previous_snapshot_id.as_ref() != Some(&head.snapshot_id)
            {
                return Err(MemoryError::InvalidOverlayObject {
                    path: self.overlay_pending_path().display().to_string(),
                    detail: "pending append is not the next operation after the stream head"
                        .to_string(),
                });
            }
        } else if pending.sequence != 1 || pending.previous_snapshot_id.is_some() {
            return Err(MemoryError::InvalidOverlayObject {
                path: self.overlay_pending_path().display().to_string(),
                detail: "initial pending append must use sequence one without a predecessor"
                    .to_string(),
            });
        }

        let operation_path = self.overlay_operation_path(&pending.operation.operation_id);
        self.write_sobj_immutable(&operation_path, &pending.operation)?;
        let persisted = read_overlay_operation_required(&operation_path)?;
        if persisted != pending.operation {
            return Err(MemoryError::ImmutableOverlayOperationCollision {
                operation_id: pending.operation.operation_id.clone(),
                path: operation_path.display().to_string(),
            });
        }

        let sequence_entry = OverlaySequenceEntry {
            sequence: pending.sequence,
            operation_id: pending.operation.operation_id.clone(),
        };
        self.write_overlay_immutable_checked(
            &self.overlay_sequence_path(pending.sequence),
            &sequence_entry,
            "overlay sequence entry",
        )?;
        let operation_index = OverlayOperationIndex {
            operation_id: pending.operation.operation_id.clone(),
            sequence: pending.sequence,
        };
        self.write_overlay_immutable_checked(
            &self.overlay_operation_index_path(&pending.operation.operation_id),
            &operation_index,
            "overlay operation index",
        )?;

        let snapshot = OverlaySnapshotManifest::canonical(
            pending.stream_id.clone(),
            pending.sequence,
            pending.previous_snapshot_id.clone(),
            pending.operation.operation_id.clone(),
        );
        self.write_overlay_immutable_checked(
            &self.overlay_snapshot_path(&snapshot.snapshot_id),
            &snapshot,
            "overlay snapshot manifest",
        )?;
        self.write_sobj_atomic(
            &self.overlay_head_path(),
            &OverlayStreamHead {
                stream_id: pending.stream_id.clone(),
                last_sequence: pending.sequence,
                snapshot_id: snapshot.snapshot_id,
            },
        )?;
        remove_file_if_exists(&self.overlay_pending_path())
    }

    fn validate_completed_pending_append(
        &self,
        head: &OverlayStreamHead,
        pending: &PendingOverlayAppend,
    ) -> Result<(), MemoryError> {
        if head.last_sequence != pending.sequence {
            return Err(MemoryError::InvalidOverlayObject {
                path: self.overlay_pending_path().display().to_string(),
                detail: "stale pending append falls behind the published stream head".to_string(),
            });
        }
        let snapshot = self.load_overlay_snapshot(&head.snapshot_id)?;
        let expected_snapshot = OverlaySnapshotManifest::canonical(
            pending.stream_id.clone(),
            pending.sequence,
            pending.previous_snapshot_id.clone(),
            pending.operation.operation_id.clone(),
        );
        if snapshot != expected_snapshot {
            return Err(MemoryError::InvalidOverlayObject {
                path: self
                    .overlay_snapshot_path(&head.snapshot_id)
                    .display()
                    .to_string(),
                detail: "published snapshot does not match the pending append".to_string(),
            });
        }
        self.validate_overlay_sequence_entry(pending.sequence, &pending.operation.operation_id)?;
        let index = self
            .read_overlay_operation_index(&pending.operation.operation_id)?
            .ok_or_else(|| MemoryError::InvalidOverlayObject {
                path: self
                    .overlay_operation_index_path(&pending.operation.operation_id)
                    .display()
                    .to_string(),
                detail: "completed append is missing its operation index".to_string(),
            })?;
        if index.sequence != pending.sequence {
            return Err(MemoryError::InvalidOverlayObject {
                path: self
                    .overlay_operation_index_path(&pending.operation.operation_id)
                    .display()
                    .to_string(),
                detail: "operation index points at a different append sequence".to_string(),
            });
        }
        let path = self.overlay_operation_path(&pending.operation.operation_id);
        if read_overlay_operation_required(&path)? != pending.operation {
            return Err(MemoryError::ImmutableOverlayOperationCollision {
                operation_id: pending.operation.operation_id.clone(),
                path: path.display().to_string(),
            });
        }
        Ok(())
    }

    fn load_overlay_stream_head(&self) -> Result<Option<OverlayStreamHead>, MemoryError> {
        let Some(head) =
            read_overlay_metadata_optional::<OverlayStreamHead>(&self.overlay_head_path())?
        else {
            return Ok(None);
        };
        if head.last_sequence == 0 {
            return Err(MemoryError::InvalidOverlayObject {
                path: self.overlay_head_path().display().to_string(),
                detail: "stream head sequence must be one-based".to_string(),
            });
        }
        let snapshot = self.load_overlay_snapshot(&head.snapshot_id)?;
        if snapshot.stream_id != head.stream_id || snapshot.watermark_sequence != head.last_sequence
        {
            return Err(MemoryError::InvalidOverlayObject {
                path: self.overlay_head_path().display().to_string(),
                detail: "stream head does not match its immutable snapshot".to_string(),
            });
        }
        Ok(Some(head))
    }

    fn load_overlay_snapshot(
        &self,
        snapshot_id: &OverlaySnapshotId,
    ) -> Result<OverlaySnapshotManifest, MemoryError> {
        let path = self.overlay_snapshot_path(snapshot_id);
        let snapshot = read_overlay_metadata_optional::<OverlaySnapshotManifest>(&path)?
            .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
                detail: format!("overlay snapshot {snapshot_id} is unavailable"),
            })?;
        if snapshot.snapshot_id != *snapshot_id
            || snapshot.snapshot_id != snapshot.expected_snapshot_id()
            || snapshot.watermark_sequence == 0
            || (snapshot.watermark_sequence == 1) != snapshot.previous_snapshot_id.is_none()
        {
            return Err(MemoryError::InvalidOverlayObject {
                path: path.display().to_string(),
                detail: "overlay snapshot manifest is not canonical".to_string(),
            });
        }
        self.validate_overlay_sequence_entry(
            snapshot.watermark_sequence,
            &snapshot.last_operation_id,
        )?;
        Ok(snapshot)
    }

    fn read_overlay_sequence_entry(
        &self,
        sequence: u64,
    ) -> Result<OverlaySequenceEntry, MemoryError> {
        let path = self.overlay_sequence_path(sequence);
        let entry = read_overlay_metadata_required::<OverlaySequenceEntry>(
            &path,
            "overlay sequence entry",
        )?;
        if entry.sequence != sequence {
            return Err(MemoryError::InvalidOverlayObject {
                path: path.display().to_string(),
                detail: format!(
                    "sequence file identifies {sequence}, payload identifies {}",
                    entry.sequence
                ),
            });
        }
        Ok(entry)
    }

    fn validate_overlay_sequence_entry(
        &self,
        sequence: u64,
        operation_id: &OverlayOperationId,
    ) -> Result<(), MemoryError> {
        let entry = self.read_overlay_sequence_entry(sequence)?;
        if entry.operation_id != *operation_id {
            return Err(MemoryError::InvalidOverlayObject {
                path: self.overlay_sequence_path(sequence).display().to_string(),
                detail: format!(
                    "sequence {sequence} points at {}, expected {operation_id}",
                    entry.operation_id
                ),
            });
        }
        Ok(())
    }

    fn read_overlay_operation_index(
        &self,
        operation_id: &OverlayOperationId,
    ) -> Result<Option<OverlayOperationIndex>, MemoryError> {
        let path = self.overlay_operation_index_path(operation_id);
        let index = read_overlay_metadata_optional::<OverlayOperationIndex>(&path)?;
        if let Some(index) = &index
            && index.operation_id != *operation_id
        {
            return Err(MemoryError::InvalidOverlayObject {
                path: path.display().to_string(),
                detail: format!(
                    "operation index file identifies {operation_id}, payload identifies {}",
                    index.operation_id
                ),
            });
        }
        Ok(index)
    }

    fn write_overlay_immutable_checked<T>(
        &self,
        path: &Path,
        value: &T,
        kind: &'static str,
    ) -> Result<(), MemoryError>
    where
        T: Serialize + for<'de> Deserialize<'de> + Eq,
    {
        let encoded = encode_sobj(path, value)?;
        ensure_overlay_metadata_size(path, encoded.len() as u64)?;
        self.write_sobj_immutable(path, value)?;
        let persisted = read_overlay_metadata_required::<T>(path, kind)?;
        if persisted != *value {
            return Err(MemoryError::InvalidOverlayObject {
                path: path.display().to_string(),
                detail: format!("immutable {kind} collides with different content"),
            });
        }
        Ok(())
    }

    fn overlay_root_dir(&self) -> PathBuf {
        self.root.join("overlays")
    }

    fn overlay_operations_dir(&self) -> PathBuf {
        self.overlay_root_dir().join("operations")
    }

    fn overlay_operation_path(&self, operation_id: &OverlayOperationId) -> PathBuf {
        let id = operation_id.as_str();
        let shard = id
            .get(..2)
            .expect("canonical ids always contain at least two ASCII characters");
        self.overlay_operations_dir()
            .join(shard)
            .join(format!("{operation_id}.sobj"))
    }

    fn overlay_sequence_path(&self, sequence: u64) -> PathBuf {
        self.overlay_root_dir()
            .join("sequence")
            .join(format!("{sequence:020}.sobj"))
    }

    fn overlay_operation_index_path(&self, operation_id: &OverlayOperationId) -> PathBuf {
        let id = operation_id.as_str();
        let shard = id
            .get(..2)
            .expect("canonical ids always contain at least two ASCII characters");
        self.overlay_root_dir()
            .join("by-operation")
            .join(shard)
            .join(format!("{operation_id}.sobj"))
    }

    fn overlay_snapshot_path(&self, snapshot_id: &OverlaySnapshotId) -> PathBuf {
        let id = snapshot_id.as_str();
        let shard = id
            .get(..2)
            .expect("canonical ids always contain at least two ASCII characters");
        self.overlay_root_dir()
            .join("snapshots")
            .join(shard)
            .join(format!("{snapshot_id}.sobj"))
    }

    fn overlay_head_path(&self) -> PathBuf {
        self.overlay_root_dir().join("head.sobj")
    }

    fn overlay_pending_path(&self) -> PathBuf {
        self.overlay_root_dir().join("pending.sobj")
    }
}

fn validate_overlay_operation_at_path(
    path: &Path,
    operation_id: &OverlayOperationId,
    operation: &OverlayOperation,
) -> Result<(), MemoryError> {
    if operation.operation_id != *operation_id {
        return Err(MemoryError::InvalidOverlayObject {
            path: path.display().to_string(),
            detail: format!(
                "index identifies {operation_id}, payload identifies {}",
                operation.operation_id
            ),
        });
    }
    let expected = operation.expected_operation_id();
    if operation.operation_id != expected {
        return Err(MemoryError::NonCanonicalOverlayOperationId {
            expected,
            actual: operation.operation_id.clone(),
        });
    }
    Ok(())
}

fn overlay_cursor_authenticator(
    key: &[u8; CONTINUATION_KEY_BYTES],
    snapshot_id: &OverlaySnapshotId,
    next_sequence: u64,
) -> Result<String, MemoryError> {
    let payload = serde_json::to_vec(&("memory-overlay-page-cursor", snapshot_id, next_sequence))
        .map_err(|source| MemoryError::SobjSerialize {
        path: "memory overlay page cursor".to_string(),
        detail: source.to_string(),
    })?;
    Ok(blake3::keyed_hash(key, &payload).to_hex().to_string())
}

fn ensure_overlay_size(path: &Path, bytes: u64) -> Result<(), MemoryError> {
    if bytes > MAX_OVERLAY_OPERATION_BYTES {
        return Err(MemoryError::OverlayOperationTooLarge {
            path: path.display().to_string(),
            bytes,
            limit: MAX_OVERLAY_OPERATION_BYTES,
        });
    }
    Ok(())
}

fn ensure_overlay_metadata_size(path: &Path, bytes: u64) -> Result<(), MemoryError> {
    ensure_overlay_sobj_size(path, bytes, MAX_OVERLAY_METADATA_BYTES)
}

fn ensure_overlay_sobj_size(path: &Path, bytes: u64, limit: u64) -> Result<(), MemoryError> {
    if bytes > limit {
        return Err(MemoryError::InvalidOverlayObject {
            path: path.display().to_string(),
            detail: format!("overlay metadata is {bytes} bytes, exceeding the {limit} byte bound"),
        });
    }
    Ok(())
}

fn read_overlay_metadata_optional<T>(path: &Path) -> Result<Option<T>, MemoryError>
where
    T: for<'de> Deserialize<'de>,
{
    read_overlay_sobj_optional_with_limit(path, MAX_OVERLAY_METADATA_BYTES)
}

fn read_overlay_sobj_optional_with_limit<T>(
    path: &Path,
    max_bytes: u64,
) -> Result<Option<T>, MemoryError>
where
    T: for<'de> Deserialize<'de>,
{
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(MemoryError::Io {
                path: path.display().to_string(),
                source,
            });
        }
    };
    let declared_bytes = file
        .metadata()
        .map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?
        .len();
    ensure_overlay_sobj_size(path, declared_bytes, max_bytes)?;
    let mut bytes = Vec::with_capacity(declared_bytes as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
    ensure_overlay_sobj_size(path, bytes.len() as u64, max_bytes)?;
    decode_sobj(path, &bytes).map(Some)
}

fn read_overlay_metadata_required<T>(path: &Path, kind: &'static str) -> Result<T, MemoryError>
where
    T: for<'de> Deserialize<'de>,
{
    read_overlay_metadata_optional(path)?.ok_or_else(|| MemoryError::MissingIndexedMemoryObject {
        kind,
        path: path.display().to_string(),
    })
}

fn remove_file_if_exists(path: &Path) -> Result<(), MemoryError> {
    remove_file_if_exists_and_sync_parent(path)
        .map(|_| ())
        .map_err(|source| MemoryError::DurableFile {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

fn read_overlay_operation_optional(path: &Path) -> Result<Option<OverlayOperation>, MemoryError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(MemoryError::Io {
                path: path.display().to_string(),
                source,
            });
        }
    };
    read_overlay_operation(path, file).map(Some)
}

fn read_overlay_operation_required(path: &Path) -> Result<OverlayOperation, MemoryError> {
    read_overlay_operation_optional(path)?.ok_or_else(|| MemoryError::MissingIndexedMemoryObject {
        kind: "overlay operation",
        path: path.display().to_string(),
    })
}

fn read_overlay_operation(path: &Path, file: File) -> Result<OverlayOperation, MemoryError> {
    let declared_bytes = file
        .metadata()
        .map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?
        .len();
    ensure_overlay_size(path, declared_bytes)?;

    let mut bytes = Vec::with_capacity(declared_bytes as usize);
    let mut reader: Take<File> = file.take(MAX_OVERLAY_OPERATION_BYTES + 1);
    reader
        .read_to_end(&mut bytes)
        .map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
    ensure_overlay_size(path, bytes.len() as u64)?;
    decode_sobj(path, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryNodeRef, OverlayOperationKind, OverlayValidationState};
    use sinomo_runtime::core::MemoryScope;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "sinomo-memory-overlay-{name}-{}-{}",
                std::process::id(),
                Id::generate()
            ));
            fs::create_dir_all(&path).expect("create overlay test root");
            Self(path)
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn operation(index: usize) -> OverlayOperation {
        OverlayOperation::canonical(
            MemoryScope::Workspace {
                workspace_id: "workspace-a".to_string(),
            },
            OverlayValidationState::Unverified,
            OverlayOperationKind::Annotate {
                node: MemoryNodeRef::External(format!("document-{index}")),
                key: "topic".to_string(),
                value: format!("overlay-{index}"),
            },
            time::OffsetDateTime::from_unix_timestamp(1_752_840_000 + index as i64)
                .expect("valid test timestamp"),
        )
    }

    #[test]
    fn canonical_overlay_ids_are_stable_and_payload_sensitive() {
        let first = operation(1);
        let repeated = operation(1);
        let different = operation(2);

        assert_eq!(first.operation_id, repeated.operation_id);
        assert_ne!(first.operation_id, different.operation_id);
        assert_eq!(first.operation_id, first.expected_operation_id());
    }

    #[test]
    fn overlay_append_is_immutable_idempotent_and_collision_checked() {
        let root = TempRoot::new("immutable");
        let store = IndexedFileMemoryStore::new(&root.0);
        let operation = operation(1);

        store
            .append_overlay_operation(operation.clone())
            .expect("append operation");
        store
            .append_overlay_operation(operation.clone())
            .expect("idempotent retry");

        let path = store.overlay_operation_path(&operation.operation_id);
        let mut collided = operation.clone();
        if let OverlayOperationKind::Annotate { value, .. } = &mut collided.kind {
            *value = "tampered".to_string();
        }
        store
            .write_sobj_atomic(&path, &collided)
            .expect("simulate non-cooperating writer");
        let error = store
            .append_overlay_operation(operation)
            .expect_err("different immutable payload must collide");
        assert!(matches!(
            error,
            MemoryError::ImmutableOverlayOperationCollision { .. }
        ));
    }

    #[test]
    fn overlay_replay_is_paged_ordered_and_restart_stable() {
        let root = TempRoot::new("replay");
        let store = IndexedFileMemoryStore::new(&root.0);
        let expected: Vec<_> = (0..9).map(operation).collect();
        for operation in &expected {
            store
                .append_overlay_operation(operation.clone())
                .expect("append operation");
        }

        let mut replayed = Vec::new();
        let mut continuation = None;
        loop {
            let page = IndexedFileMemoryStore::new(&root.0)
                .overlay_operation_page(continuation.as_ref(), 3)
                .expect("replay page after restart");
            replayed.extend(page.items);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }

        assert_eq!(replayed, expected);
    }

    #[test]
    fn separate_store_instances_serialize_concurrent_idempotent_appends() {
        let root = TempRoot::new("concurrent");
        let root_path = root.0.clone();
        let operation = operation(7);
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let root_path = root_path.clone();
            let operation = operation.clone();
            workers.push(thread::spawn(move || {
                let store = IndexedFileMemoryStore::new(root_path);
                barrier.wait();
                store.append_overlay_operation(operation)
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().expect("join writer").expect("append overlay");
        }

        let page = IndexedFileMemoryStore::new(&root.0)
            .overlay_operation_page(None, 4)
            .expect("replay operations");
        assert_eq!(page.items, vec![operation]);
        assert_eq!(page.continuation, None);
    }

    #[test]
    fn overlay_append_rejects_noncanonical_ids_and_unbounded_pages() {
        let root = TempRoot::new("validation");
        let store = IndexedFileMemoryStore::new(&root.0);
        let mut invalid = operation(3);
        invalid.operation_id = Id::generate();

        assert!(matches!(
            store.append_overlay_operation(invalid),
            Err(MemoryError::NonCanonicalOverlayOperationId { .. })
        ));
        assert!(matches!(
            store.overlay_operation_page(None, MAX_OVERLAY_PAGE_SIZE + 1),
            Err(MemoryError::InvalidOverlayPageLimit { .. })
        ));
    }

    #[test]
    fn overlay_cursor_pins_snapshot_across_a_lexically_earlier_append() {
        let root = TempRoot::new("snapshot-concurrent-append");
        let store = IndexedFileMemoryStore::new(&root.0);
        let mut candidates: Vec<_> = (100..120).map(operation).collect();
        candidates.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
        let late_lexical_predecessor = candidates.remove(0);
        let initial: Vec<_> = candidates.into_iter().rev().take(5).collect();
        for operation in &initial {
            store
                .append_overlay_operation(operation.clone())
                .expect("append initial operation");
        }

        let first = store
            .overlay_operation_page(None, 2)
            .expect("first snapshot page");
        assert!(
            late_lexical_predecessor.operation_id < first.items[1].operation_id,
            "fixture must sort before the live lexical cursor that used to lose it"
        );
        let mut pinned_replay = first.items;
        let mut cursor = first.continuation;

        store
            .append_overlay_operation(late_lexical_predecessor.clone())
            .expect("append after snapshot pin");
        while let Some(next) = cursor {
            let page = store
                .overlay_operation_page(Some(&next), 2)
                .expect("continue pinned replay");
            pinned_replay.extend(page.items);
            cursor = page.continuation;
        }
        assert_eq!(pinned_replay, initial);

        let fresh = store
            .overlay_operation_page(None, MAX_OVERLAY_PAGE_SIZE)
            .expect("fresh snapshot");
        let mut expected = initial;
        expected.push(late_lexical_predecessor);
        assert_eq!(fresh.items, expected);
        assert!(fresh.continuation.is_none());
    }

    #[test]
    fn overlay_cursor_rejects_tampering_and_unknown_snapshots() {
        let root = TempRoot::new("cursor-validation");
        let store = IndexedFileMemoryStore::new(&root.0);
        for index in 0..4 {
            store
                .append_overlay_operation(operation(index))
                .expect("append operation");
        }
        let first = store.overlay_operation_page(None, 2).expect("first page");
        let cursor = first.continuation.expect("continuation");

        let mut tampered = cursor.clone();
        tampered.next_sequence += 1;
        assert!(matches!(
            store.overlay_operation_page(Some(&tampered), 2),
            Err(MemoryError::InvalidMemoryContinuation { .. })
        ));

        let unknown = store
            .sign_overlay_cursor(Id::generate(), 1)
            .expect("sign unknown snapshot fixture");
        assert!(matches!(
            store.overlay_operation_page(Some(&unknown), 2),
            Err(MemoryError::InvalidMemoryContinuation { .. })
        ));

        let beyond_watermark = store
            .sign_overlay_cursor(cursor.snapshot_id, 99)
            .expect("sign out-of-range fixture");
        assert!(matches!(
            store.overlay_operation_page(Some(&beyond_watermark), 2),
            Err(MemoryError::InvalidMemoryContinuation { .. })
        ));
    }

    #[test]
    fn overlay_append_recovers_pending_sequence_before_next_writer() {
        let root = TempRoot::new("pending-recovery");
        let store = IndexedFileMemoryStore::new(&root.0);
        let first = operation(1);
        let interrupted = operation(2);
        let following = operation(3);
        store
            .append_overlay_operation(first.clone())
            .expect("append first");
        let head = store
            .load_overlay_stream_head()
            .expect("load head")
            .expect("head");
        store
            .write_sobj_atomic(
                &store.overlay_pending_path(),
                &PendingOverlayAppend {
                    stream_id: head.stream_id,
                    sequence: 2,
                    previous_snapshot_id: Some(head.snapshot_id),
                    operation: interrupted.clone(),
                },
            )
            .expect("persist interrupted append");

        IndexedFileMemoryStore::new(&root.0)
            .append_overlay_operation(following.clone())
            .expect("recover then append next operation");
        let replay = store
            .overlay_operation_page(None, MAX_OVERLAY_PAGE_SIZE)
            .expect("replay recovered stream");
        assert_eq!(replay.items, vec![first, interrupted, following]);
        assert!(replay.continuation.is_none());
        assert!(!store.overlay_pending_path().exists());
    }

    #[test]
    fn overlay_page_does_not_scan_unrelated_lifetime_operation_files() {
        let root = TempRoot::new("bounded-direct-page");
        let store = IndexedFileMemoryStore::new(&root.0);
        for index in 0..5 {
            store
                .append_overlay_operation(operation(index))
                .expect("append operation");
        }
        let unrelated = store.overlay_operations_dir().join("zz/not-an-id.sobj");
        fs::create_dir_all(unrelated.parent().expect("unrelated parent"))
            .expect("create unrelated shard");
        fs::write(&unrelated, b"not an overlay object").expect("write unrelated object");

        let first = store
            .overlay_operation_page(None, 1)
            .expect("direct first page");
        let second = store
            .overlay_operation_page(first.continuation.as_ref(), 1)
            .expect("direct second page");
        assert_eq!(first.items, vec![operation(0)]);
        assert_eq!(second.items, vec![operation(1)]);
    }
}
