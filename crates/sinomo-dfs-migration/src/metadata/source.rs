//! Read-only retained metadata source traversal. The caller retains the actual
//! source writer lock; this reader neither acquires it nor repairs source bytes.
use super::{MetadataBackendError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinomo_core::object::serde_ext;
use sinomo_core::storage::migration::stream::{MigrationStreamReader, StreamCursor};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const MAX_RECORD: usize = 64 * 1024 * 1024;
const SNAPSHOT: &str = "records/tables/snapshot.sobj";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum Kind {
    SnapshotPlain,
    SnapshotStream,
    TableLog,
    JournalSegment,
    JournalLoose,
    LooseRow(String),
    EvidenceDocument,
    EvidenceLog,
    EvidenceRaw,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct SourceFile {
    pub relative: PathBuf,
    pub len: u64,
    pub blake3: String,
    pub modified_unix_nanos: Option<u128>,
    pub identity: Option<(u64, u64)>,
    kind: Kind,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct SourceInventory {
    pub files: Vec<SourceFile>,
    pub root_identity: Option<(u64, u64)>,
    pub fingerprint: String,
    pub authoritative_snapshot_present: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct SourceCursor {
    pub inventory: Option<SourceInventory>,
    pub file_index: usize,
    pub offset: u64,
    pub last_record_relative: PathBuf,
    pub last_record_offset: u64,
    pub last_record_end: u64,
    plain: Option<super::sobj_cursor::Cursor>,
    stream: Option<StreamCursor>,
    header_delivered: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum SourceRecord {
    SnapshotHeader(Value),
    SnapshotRow {
        table: String,
        row: Value,
        raw_document: super::source_document::SourceRawDocument,
    },
    TableBatch {
        batch: Value,
        raw_document: super::source_document::SourceRawDocument,
    },
    JournalFrame {
        entry: Value,
        #[serde(with = "serde_bytes")]
        exact_bytes: Vec<u8>,
    },
    LooseRow {
        family: String,
        row: Value,
        raw_document: super::source_document::SourceRawDocument,
    },
    Evidence {
        relative: PathBuf,
        value: Value,
    },
}
impl SourceRecord {
    pub(crate) fn raw_document_context(
        &self,
    ) -> Result<Option<super::source_document::SourceDocumentContext<'_>>> {
        use SourceRecord::*;
        match self {
            SnapshotRow { raw_document, .. }
            | LooseRow { raw_document, .. }
            | TableBatch { raw_document, .. } => raw_document.context().map(Some),
            JournalFrame { exact_bytes, .. } => {
                super::source_document::SourceDocumentContext::new(exact_bytes, "").map(Some)
            }
            SnapshotHeader(_) | Evidence { .. } => Ok(None),
        }
    }
}
fn invalid(message: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(message.to_string())
}
fn io(path: &Path, source: std::io::Error) -> MetadataBackendError {
    MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    }
}
fn identity(metadata: &fs::Metadata) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}
fn modified(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|time| time.as_nanos())
}
fn kind(relative: &Path, snapshot_present: bool, prefix: &[u8]) -> Kind {
    if relative == Path::new(SNAPSHOT) {
        return if prefix.starts_with(b"SMIGLOG1") {
            Kind::SnapshotStream
        } else {
            Kind::SnapshotPlain
        };
    }
    if relative == Path::new("records/tables/materialized.checkpoint.slog")
        || relative == Path::new("records/tables/materialized.slog")
    {
        return Kind::TableLog;
    }
    if relative.parent() == Some(Path::new("journal/segments"))
        && relative
            .extension()
            .is_some_and(|extension| extension == "slog")
    {
        return Kind::JournalSegment;
    }
    if relative.parent() == Some(Path::new("journal"))
        && relative
            .extension()
            .is_some_and(|extension| extension == "sobj")
        && relative
            .file_stem()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.parse::<u64>().is_ok())
    {
        return Kind::JournalLoose;
    }
    if !snapshot_present
        && relative.starts_with("records")
        && relative
            .extension()
            .is_some_and(|extension| extension == "sobj")
    {
        if let Some(family) = relative
            .components()
            .nth(1)
            .and_then(|component| component.as_os_str().to_str())
        {
            if family != "tables" {
                return Kind::LooseRow(family.to_owned());
            }
        }
    }
    if prefix.starts_with(b"SOBJ") {
        Kind::EvidenceDocument
    } else if relative
        .extension()
        .is_some_and(|extension| extension == "slog")
    {
        Kind::EvidenceLog
    } else {
        Kind::EvidenceRaw
    }
}
fn priority(file: &SourceFile) -> (u8, u64, &Path) {
    let sequence = file
        .relative
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse().ok())
        .unwrap_or(0);
    let phase = match &file.kind {
        Kind::SnapshotPlain | Kind::SnapshotStream => 2,
        Kind::LooseRow(_) => 3,
        Kind::JournalSegment => 0,
        Kind::JournalLoose => 1,
        Kind::TableLog if file.relative.ends_with("materialized.checkpoint.slog") => 4,
        Kind::TableLog => 5,
        _ => 6,
    };
    (phase, sequence, &file.relative)
}
/// Explicit O(number of files + source bytes) discovery/cut phase. Each file
/// hash is streamed in 64KiB blocks; no snapshot or logical table is collected.
/// The caller schedules this phase separately from the per-row conversion budget.
pub(super) fn discover(root: &Path) -> Result<SourceInventory> {
    let root_metadata = fs::metadata(root).map_err(|error| io(root, error))?;
    let snapshot_present = root.join(SNAPSHOT).is_file();
    let mut inventory = SourceInventory {
        root_identity: identity(&root_metadata),
        authoritative_snapshot_present: snapshot_present,
        ..Default::default()
    };
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(|error| io(&directory, error))? {
            let entry = entry.map_err(|error| io(&directory, error))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| io(&path, error))?;
            let relative = path.strip_prefix(root).map_err(invalid)?.to_owned();
            if relative == Path::new("journal/write.lock") {
                continue;
            }
            // Old repair archives are historical retained sources, never part
            // of the current authoritative image. Preserve them externally.
            if relative
                .components()
                .next()
                .is_some_and(|part| part.as_os_str() == "replica-repair-archives")
            {
                continue;
            }
            if metadata.file_type().is_symlink() {
                return Err(invalid(format!(
                    "source cut contains unsupported symlink {}",
                    relative.display()
                )));
            }
            if metadata.is_dir() {
                directories.push(path);
                continue;
            }
            if !metadata.is_file() {
                return Err(invalid(format!(
                    "source cut contains non-file {}",
                    relative.display()
                )));
            }
            let mut input = File::open(&path).map_err(|error| io(&path, error))?;
            let mut hash = blake3::Hasher::new();
            let mut block = [0u8; 64 * 1024];
            let mut prefix = Vec::new();
            loop {
                let count = input.read(&mut block).map_err(|error| io(&path, error))?;
                if count == 0 {
                    break;
                }
                if prefix.is_empty() {
                    prefix.extend_from_slice(&block[..count.min(8)]);
                }
                hash.update(&block[..count]);
            }
            let after = input.metadata().map_err(|error| io(&path, error))?;
            if after.len() != metadata.len()
                || modified(&after) != modified(&metadata)
                || identity(&after) != identity(&metadata)
            {
                return Err(invalid("retained source changed during cut discovery"));
            }
            inventory.files.push(SourceFile {
                kind: kind(&relative, snapshot_present, &prefix),
                relative,
                len: metadata.len(),
                blake3: hash.finalize().to_hex().to_string(),
                modified_unix_nanos: modified(&metadata),
                identity: identity(&metadata),
            });
        }
    }
    inventory
        .files
        .sort_by(|left, right| priority(left).cmp(&priority(right)));
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinomo.dfs-migration.source-cut.v1\0");
    serde_json::to_writer(&mut hash, &inventory).map_err(invalid)?;
    inventory.fingerprint = hash.finalize().to_hex().to_string();
    Ok(inventory)
}
/// Current open can create an empty cursor before its first native frame.
/// That bookkeeping is not an original metadata source. All real payload and
/// acknowledgement evidence, unknown rows and nonzero cursors remain sources.
pub(super) fn has_migration_source_data(root: &Path) -> Result<bool> {
    if !root.exists() {
        return Ok(false);
    }
    let inventory = discover(root)?;
    for file in &inventory.files {
        if file.relative == Path::new("records/local-materialization/cursor.sobj") {
            let bytes = bounded_document(&root.join(&file.relative), file.len)?;
            let row = super::source_document::decode_original_document(&bytes)?;
            if row.as_object().is_some_and(|fields| {
                fields.len() == 2
                    && fields.get("last_sequence").and_then(Value::as_u64) == Some(0)
                    && fields.get("shard_id").and_then(Value::as_str)
                        == root.file_name().and_then(|name| name.to_str())
            }) {
                continue;
            }
        }
        return Ok(true);
    }
    Ok(false)
}

impl SourceCursor {
    pub(super) fn new(inventory: SourceInventory) -> Self {
        Self {
            inventory: Some(inventory),
            ..Default::default()
        }
    }
    /// Persist only decoder position; source inventory is independently rebuilt
    /// from the held source when a membership witness is checked.
    pub(super) fn point(&self) -> Self {
        Self {
            inventory: None,
            file_index: self.file_index,
            offset: self.offset,
            last_record_relative: self.last_record_relative.clone(),
            last_record_offset: self.last_record_offset,
            last_record_end: self.last_record_end,
            plain: self.plain.clone(),
            stream: self.stream.clone(),
            header_delivered: self.header_delivered,
        }
    }
    fn advance(&mut self) {
        self.file_index += 1;
        self.offset = 0;
        self.plain = None;
        self.stream = None;
        self.header_delivered = false;
    }
}
/// A decoder boundary captured by an actual sequential source read. This is
/// never serialized into unsigned stage evidence; its owning membership keeps
/// the same source writer and immutable inventory alive.
#[derive(Clone, Debug)]
pub(super) struct VerifiedRecordPoint {
    point: SourceCursor,
}
impl VerifiedRecordPoint {
    pub(super) fn key(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.point).map_err(invalid)
    }
    pub(super) fn read(&self, root: &Path, inventory: &SourceInventory) -> Result<SourceRecord> {
        // Retain the complete actual source mutation check. Only reconstruction
        // of preceding decoder boundaries is shared with the verified replay.
        inventory.verify(root)?;
        let mut cursor = self.point.clone();
        cursor.inventory = Some(inventory.clone());
        next(root, &mut cursor)?
            .ok_or_else(|| invalid("verified source boundary is beyond its immutable cut"))
    }
}
pub(super) fn next_verified(
    root: &Path,
    cursor: &mut SourceCursor,
) -> Result<Option<(VerifiedRecordPoint, SourceRecord)>> {
    let point = cursor.point();
    Ok(next(root, cursor)?.map(|record| (VerifiedRecordPoint { point }, record)))
}

pub(super) fn read_at_point(root: &Path, point: &SourceCursor) -> Result<SourceRecord> {
    let inventory = discover(root)?;
    if point.file_index >= inventory.files.len() {
        return Err(invalid("source membership point is beyond its cut"));
    }
    // A serialized decoder position is a hint, not a structural certificate.
    // In particular its table label/row kind must not relabel opaque bytes as
    // a version row. Reconstruct that state from the actual file boundary.
    let mut cursor = SourceCursor::new(inventory);
    cursor.file_index = point.file_index;
    loop {
        if cursor.file_index != point.file_index {
            return Err(invalid(
                "source membership point is not an actual record boundary",
            ));
        }
        if cursor.offset == point.offset
            && cursor.header_delivered == point.header_delivered
            && serde_json::to_value((&cursor.plain, &cursor.stream)).map_err(invalid)?
                == serde_json::to_value((&point.plain, &point.stream)).map_err(invalid)?
        {
            return next(root, &mut cursor)?
                .ok_or_else(|| invalid("source membership point is beyond its cut"));
        }
        if next(root, &mut cursor)?.is_none() {
            return Err(invalid(
                "source membership point is not an actual record boundary",
            ));
        }
    }
}
impl SourceInventory {
    pub(super) fn snapshot_fingerprint(&self) -> Option<&str> {
        self.files
            .iter()
            .find(|file| file.relative == Path::new(SNAPSHOT))
            .map(|file| file.blake3.as_str())
    }

    /// Must be consumed before install/CAS; detects additions, removals and
    /// byte changes as well as pathname replacement of the same-length source.
    pub(super) fn verify(&self, root: &Path) -> Result<()> {
        if discover(root)? != *self {
            return Err(invalid("retained source cut changed before install"));
        }
        Ok(())
    }
}
fn bounded_document(path: &Path, len: u64) -> Result<Vec<u8>> {
    if len > MAX_RECORD as u64 {
        return Err(invalid(format!(
            "source record {} exceeds64MiB",
            path.display()
        )));
    }
    fs::read(path).map_err(|error| io(path, error))
}
fn decode(bytes: &[u8]) -> Result<Value> {
    super::source_document::decode_original_document(bytes)
}
fn frame(path: &Path, cursor: &mut SourceCursor, file_len: u64) -> Result<Option<Vec<u8>>> {
    if cursor.offset == file_len {
        return Ok(None);
    }
    let mut file = File::open(path).map_err(|error| io(path, error))?;
    file.seek(SeekFrom::Start(cursor.offset))
        .map_err(|error| io(path, error))?;
    if file_len - cursor.offset < 4 {
        return Err(invalid("torn-source-tail"));
    }
    let mut length = [0; 4];
    file.read_exact(&mut length)
        .map_err(|error| io(path, error))?;
    let count = u32::from_le_bytes(length) as usize;
    if count > MAX_RECORD {
        return Err(invalid("source frame exceeds64MiB"));
    }
    if file_len - cursor.offset - 4 < count as u64 {
        return Err(invalid("torn-source-tail"));
    }
    let mut bytes = vec![0; count];
    file.read_exact(&mut bytes)
        .map_err(|error| io(path, error))?;
    // Advance only after successful decode in next().
    Ok(Some(bytes))
}
/// Return one bounded record. Durable cursor publication belongs to the
/// converter after it persists the corresponding staged projection.
pub(super) fn next(root: &Path, cursor: &mut SourceCursor) -> Result<Option<SourceRecord>> {
    // A rejected complete record must not disappear from a retried cursor.
    // Copy only the bounded position state, never the full file manifest.
    let saved = (
        cursor.file_index,
        cursor.offset,
        cursor.plain.clone(),
        cursor.stream.clone(),
        cursor.header_delivered,
        cursor.last_record_relative.clone(),
        cursor.last_record_offset,
        cursor.last_record_end,
    );
    let result = next_inner(root, cursor);
    if result.is_err() {
        (
            cursor.file_index,
            cursor.offset,
            cursor.plain,
            cursor.stream,
            cursor.header_delivered,
            cursor.last_record_relative,
            cursor.last_record_offset,
            cursor.last_record_end,
        ) = saved;
    }
    result
}
fn next_inner(root: &Path, cursor: &mut SourceCursor) -> Result<Option<SourceRecord>> {
    loop {
        let inventory = cursor
            .inventory
            .as_ref()
            .ok_or_else(|| invalid("source cursor lacks a certified discovery cut"))?;
        let Some(source) = inventory.files.get(cursor.file_index).cloned() else {
            return Ok(None);
        };
        let path = root.join(&source.relative);
        let metadata = fs::metadata(&path).map_err(|error| io(&path, error))?;
        if metadata.len() != source.len
            || identity(&metadata) != source.identity
            || modified(&metadata) != source.modified_unix_nanos
        {
            return Err(invalid("source generation changed while resuming cursor"));
        }
        if cursor.offset > source.len {
            return Err(invalid("source cursor exceeds retained file cut"));
        }
        cursor.last_record_relative = source.relative.clone();
        cursor.last_record_offset = cursor.offset;
        let record = match &source.kind {
            Kind::SnapshotPlain => {
                if cursor.plain.is_none() {
                    cursor.plain = Some(super::sobj_cursor::Cursor::open(&path)?);
                }
                cursor.last_record_offset = cursor.plain.as_ref().expect("plain cursor").position();
                match cursor.plain.as_mut().expect("plain cursor").next(&path)? {
                    Some(super::sobj_cursor::Item::Header(key, value)) => {
                        SourceRecord::SnapshotHeader(Value::Object(
                            [(key, value)].into_iter().collect(),
                        ))
                    }
                    Some(super::sobj_cursor::Item::Row(table, row, raw_document)) => {
                        SourceRecord::SnapshotRow {
                            table,
                            row,
                            raw_document,
                        }
                    }
                    Some(super::sobj_cursor::Item::Op(row, raw_document)) => {
                        SourceRecord::SnapshotRow {
                            table: "$ops".into(),
                            row,
                            raw_document,
                        }
                    }
                    None => {
                        cursor.advance();
                        continue;
                    }
                }
            }
            Kind::SnapshotStream => {
                let mut reader = if let Some(stream) = &cursor.stream {
                    MigrationStreamReader::resume(&path, stream)
                } else {
                    MigrationStreamReader::open(&path)
                }
                .map_err(|error| io(&path, error))?;
                if !cursor.header_delivered {
                    let value = decode(&reader.header)?;
                    cursor.stream = Some(reader.cursor().clone());
                    cursor.header_delivered = true;
                    SourceRecord::SnapshotHeader(value)
                } else {
                    match reader
                        .next_record(MAX_RECORD)
                        .map_err(|error| io(&path, error))?
                    {
                        Some(bytes) => {
                            let value = decode(&bytes)?;
                            cursor.stream = Some(reader.cursor().clone());
                            let object = value.as_object().ok_or_else(|| {
                                invalid("retained snapshot frame is not an object")
                            })?;
                            if let Some(table) =
                                object.get("Table").and_then(|value| value.get("op"))
                            {
                                SourceRecord::SnapshotRow {
                                    table: "$ops".into(),
                                    row: table.clone(),
                                    raw_document: super::source_document::SourceRawDocument {
                                        bytes,
                                        row_pointer: "/Table/op".into(),
                                    },
                                }
                            } else if let Some(transfer) = object
                                .get("Transfer")
                                .and_then(|value| value.get("transfer"))
                            {
                                SourceRecord::SnapshotRow {
                                    table: "cross_shard_pin_transfers".into(),
                                    row: transfer.clone(),
                                    raw_document: super::source_document::SourceRawDocument {
                                        bytes,
                                        row_pointer: "/Transfer/transfer".into(),
                                    },
                                }
                            } else if let Some(state) = object
                                .get("IntentRetention")
                                .and_then(|value| value.get("state"))
                            {
                                SourceRecord::SnapshotRow {
                                    table: "intent_receipt_retention".into(),
                                    row: state.clone(),
                                    raw_document: super::source_document::SourceRawDocument {
                                        bytes,
                                        row_pointer: "/IntentRetention/state".into(),
                                    },
                                }
                            } else if let Some(state) = object
                                .get("NamespaceRetention")
                                .and_then(|value| value.get("state"))
                            {
                                SourceRecord::SnapshotRow {
                                    table: "namespace_receipt_retention".into(),
                                    row: state.clone(),
                                    raw_document: super::source_document::SourceRawDocument {
                                        bytes,
                                        row_pointer: "/NamespaceRetention/state".into(),
                                    },
                                }
                            } else if let Some(record) =
                                object.get("Prepared").and_then(|value| value.get("record"))
                            {
                                let current: sinomo_dfs::DfsPreparedOperationRecord =
                                    serde_json::from_value(record.clone()).map_err(invalid)?;
                                current.validate()?;
                                if object.len() != 1
                                    || object["Prepared"]
                                        .as_object()
                                        .is_none_or(|fields| fields.len() != 1)
                                    || serde_json::to_value(&current).map_err(invalid)? != *record
                                {
                                    return Err(invalid(
                                        "current prepared snapshot record changed its source shape",
                                    ));
                                }
                                SourceRecord::SnapshotRow {
                                    table: "prepared_operations_v7".into(),
                                    row: record.clone(),
                                    raw_document: super::source_document::SourceRawDocument {
                                        bytes,
                                        row_pointer: "/Prepared/record".into(),
                                    },
                                }
                            } else if let Some(record) =
                                object.get("Eviction").and_then(|value| value.get("record"))
                            {
                                let current: sinomo_dfs::DfsPreparedOperationEviction =
                                    serde_json::from_value(record.clone()).map_err(invalid)?;
                                current.validate()?;
                                if object.len() != 1
                                    || object["Eviction"]
                                        .as_object()
                                        .is_none_or(|fields| fields.len() != 1)
                                    || serde_json::to_value(&current).map_err(invalid)? != *record
                                {
                                    return Err(invalid(
                                        "current eviction snapshot record changed its source shape",
                                    ));
                                }
                                SourceRecord::SnapshotRow {
                                    table: "prepared_operation_evictions".into(),
                                    row: record.clone(),
                                    raw_document: super::source_document::SourceRawDocument {
                                        bytes,
                                        row_pointer: "/Eviction/record".into(),
                                    },
                                }
                            } else {
                                return Err(invalid("unknown retained snapshot frame family"));
                            }
                        }
                        None => {
                            cursor.advance();
                            continue;
                        }
                    }
                }
            }
            Kind::TableLog | Kind::JournalSegment | Kind::EvidenceLog => {
                let bytes = match frame(&path, cursor, source.len) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => {
                        cursor.advance();
                        continue;
                    }
                    Err(MetadataBackendError::Serde(message)) if message == "torn-source-tail" => {
                        let record = SourceRecord::Evidence {
                            relative: source.relative.clone(),
                            value: json!({"kind":"torn-tail","complete_through_offset":cursor.offset,"file_len":source.len,"source_blake3":source.blake3}),
                        };
                        cursor.last_record_end = cursor.offset;
                        cursor.advance();
                        return Ok(Some(record));
                    }
                    Err(error) => return Err(error),
                };
                let value = decode(&bytes)?;
                cursor.offset += 4 + bytes.len() as u64;
                match &source.kind {
                    Kind::TableLog => SourceRecord::TableBatch {
                        batch: value,
                        raw_document: super::source_document::SourceRawDocument::row(bytes),
                    },
                    Kind::JournalSegment => SourceRecord::JournalFrame {
                        entry: value,
                        exact_bytes: bytes,
                    },
                    _ => SourceRecord::Evidence {
                        relative: source.relative.clone(),
                        value,
                    },
                }
            }
            Kind::JournalLoose | Kind::LooseRow(_) | Kind::EvidenceDocument | Kind::EvidenceRaw => {
                if cursor.header_delivered {
                    cursor.advance();
                    continue;
                }
                let bytes = bounded_document(&path, source.len)?;
                let record = match &source.kind {
                    Kind::JournalLoose => SourceRecord::JournalFrame {
                        entry: decode(&bytes)?,
                        exact_bytes: bytes,
                    },
                    Kind::LooseRow(family) => SourceRecord::LooseRow {
                        family: family.clone(),
                        row: decode(&bytes)?,
                        raw_document: super::source_document::SourceRawDocument::row(bytes),
                    },
                    Kind::EvidenceDocument => SourceRecord::Evidence {
                        relative: source.relative.clone(),
                        value: decode(&bytes)?,
                    },
                    _ => SourceRecord::Evidence {
                        relative: source.relative.clone(),
                        value: json!({"encoding":"raw","exact_bytes":bytes}),
                    },
                };
                cursor.offset = source.len;
                cursor.header_delivered = true;
                record
            }
        };
        cursor.last_record_end = cursor
            .stream
            .as_ref()
            .map_or(cursor.offset, |stream| stream.offset);
        if let Some(plain) = &cursor.plain {
            cursor.offset = plain.position();
            cursor.last_record_end = cursor.offset;
        }
        return Ok(Some(record));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::object::LengthMode;
    use sinomo_core::storage::migration::stream::MigrationStreamWriter;
    use std::collections::BTreeMap;

    #[test]
    fn actual_normal_current_v7_source_preserves_scope_digest_slot_and_commit() {
        use sinomo_dfs::{
            DfsMetadataJournalEntry, DfsPreparedOperationCommitReference,
            DfsPreparedOperationRecord, DfsPreparedOperationStatus,
        };
        let (mut native, nodes) = sinomo_dfs::test_support::DfsTestRuntime::start_cluster(
            "migration-current-v7-source-validation",
            1,
        )
        .unwrap();
        let assignment = native.root_shard().unwrap();
        let root = native
            .data_dir(&nodes[0])
            .dfs_dir()
            .join("metadata")
            .join(&assignment.shard_id);
        // Read the actual normal producer's files only after its owner stops.
        native.stop_node(&nodes[0]).unwrap();
        assert!(crate::metadata::current_root(&root).unwrap());
        let inventory = discover(&root).unwrap();
        let mut cursor = SourceCursor::new(inventory.clone());
        let mut records = Vec::new();
        let mut commits = BTreeMap::new();
        let mut capture_op = |op: &Value| match op["type"].as_str() {
            Some("put-prepared-operation-v7") => records.push(op["record"].clone()),
            Some("commit-prepared-operation-v7") => {
                let reference: DfsPreparedOperationCommitReference =
                    serde_json::from_value(op["committed_entry"].clone()).unwrap();
                commits.insert(reference.digest.operation_id.clone(), reference);
            }
            _ => {}
        };
        let mut source_rows = Vec::new();
        let mut journal_commits = Vec::new();
        while let Some(record) = next(&root, &mut cursor).unwrap() {
            match record {
                SourceRecord::SnapshotRow { table, row, .. }
                | SourceRecord::LooseRow {
                    family: table, row, ..
                } => {
                    if table == "prepared_operations_v7" {
                        source_rows.push(row);
                    } else if table == "$ops" {
                        capture_op(&row);
                    }
                }
                SourceRecord::TableBatch { batch, .. } => {
                    for op in batch["ops"].as_array().unwrap() {
                        capture_op(op);
                    }
                }
                SourceRecord::JournalFrame { entry, .. } => {
                    match entry["mutation"]["type"].as_str() {
                        Some("prepare-operation-v7") => {
                            source_rows.push(entry["mutation"]["record"].clone())
                        }
                        Some("commit-prepared-operation-v7") => {
                            let entry: DfsMetadataJournalEntry =
                                serde_json::from_value(entry).unwrap();
                            let reference =
                                DfsPreparedOperationCommitReference::from_journal_entry(&entry)
                                    .unwrap();
                            journal_commits.push(reference);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        drop(capture_op);
        records.extend(source_rows);
        for reference in journal_commits {
            commits.insert(reference.digest.operation_id.clone(), reference);
        }
        assert!(
            !records.is_empty(),
            "normal native startup must retain actual V7 producer records"
        );
        let mut exact_commits = 0;
        for row in records {
            let record: DfsPreparedOperationRecord = serde_json::from_value(row.clone()).unwrap();
            assert_eq!(serde_json::to_value(&record).unwrap(), row);
            crate::legacy_wire::validate_original_v7_record(&row).unwrap();
            for (pointer, replacement) in [
                (
                    "/digest/owner_epoch",
                    serde_json::json!(record.digest.owner_epoch + 1),
                ),
                (
                    "/payload/transaction/transaction_id",
                    serde_json::json!("changed-original-transaction"),
                ),
                ("/digest/prepare_sequence", serde_json::json!(0)),
            ] {
                let mut changed = row.clone();
                *changed.pointer_mut(pointer).unwrap() = replacement;
                assert!(
                    crate::legacy_wire::validate_original_v7_record(&changed).is_err(),
                    "retained original changed at {pointer}"
                );
            }
            let mut unknown = row.clone();
            unknown["unrecognized_source_effect"] = serde_json::json!(true);
            assert!(crate::legacy_wire::validate_original_v7_record(&unknown).is_err());
            let mut unknown_body = row.clone();
            unknown_body["payload"]["unrecognized_source_effect"] = serde_json::json!(true);
            assert!(crate::legacy_wire::validate_original_v7_record(&unknown_body).is_err());
            if let Some(reference) = record
                .committed_entry
                .as_deref()
                .or_else(|| commits.get(&record.digest.operation_id))
                .filter(|reference| reference.digest == record.digest)
            {
                // This compact terminal row uses the SAME actual native marker,
                // never a fixture-provided guard, ACK, quorum or commit outcome.
                let mut committed = record.clone();
                committed.status = DfsPreparedOperationStatus::Committed;
                committed.committed_entry = Some(Box::new(reference.clone()));
                let terminal = serde_json::to_value(&committed).unwrap();
                crate::legacy_wire::validate_original_v7_record(&terminal).unwrap();
                exact_commits += 1;
                let mut wrong_slot = terminal.clone();
                wrong_slot["committed_entry"]["sequence"] =
                    serde_json::json!(record.digest.prepare_sequence);
                assert!(crate::legacy_wire::validate_original_v7_record(&wrong_slot).is_err());
                let mut wrong_digest = terminal.clone();
                wrong_digest["committed_entry"]["digest"]["owner_epoch"] =
                    serde_json::json!(record.digest.owner_epoch + 1);
                assert!(crate::legacy_wire::validate_original_v7_record(&wrong_digest).is_err());
                let mut no_commit = terminal.clone();
                no_commit.as_object_mut().unwrap().remove("committed_entry");
                assert!(crate::legacy_wire::validate_original_v7_record(&no_commit).is_err());
                let mut wrong_status = terminal;
                wrong_status["status"] = serde_json::json!("prepared");
                assert!(crate::legacy_wire::validate_original_v7_record(&wrong_status).is_err());
            }
        }
        assert!(
            exact_commits > 0,
            "actual native source must contain matching terminal markers"
        );
        inventory.verify(&root).unwrap();
        native.close().unwrap();
    }

    fn document(value: &Value) -> Vec<u8> {
        serde_ext::to_bytes(value, 1, LengthMode::U32).unwrap()
    }
    fn write(root: &Path, relative: &str, bytes: &[u8]) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    fn record_frame(value: &Value) -> Vec<u8> {
        let bytes = document(value);
        let mut frame = (bytes.len() as u32).to_le_bytes().to_vec();
        frame.extend(bytes);
        frame
    }
    #[test]
    fn plain_snapshot_keeps_original_payload_type_after_cursor_restart() {
        #[derive(Serialize)]
        struct Metadata<'a> {
            schema_version: u32,
            #[serde(with = "serde_bytes")]
            payload: &'a [u8],
        }
        #[derive(Serialize)]
        struct Row<'a> {
            module_metadata: BTreeMap<&'a str, Metadata<'a>>,
        }
        #[derive(Serialize)]
        struct Snapshot<'a> {
            format_version: u32,
            tables: BTreeMap<&'a str, Vec<Row<'a>>>,
        }
        let root = sinomo_testdir::TestDir::new("metadata-source-original-payload-kind");
        let bytes = serde_ext::to_bytes(
            &Snapshot {
                format_version: 2,
                tables: BTreeMap::from([(
                    "versions",
                    vec![Row {
                        module_metadata: BTreeMap::from([(
                            "module",
                            Metadata {
                                schema_version: 1,
                                payload: b"null",
                            },
                        )]),
                    }],
                )]),
            },
            1,
            LengthMode::U32,
        )
        .unwrap();
        write(root.path(), SNAPSHOT, &bytes);
        let mut cursor = SourceCursor::new(discover(root.path()).unwrap());
        while let Some(record) = next(root.path(), &mut cursor).unwrap() {
            if let SourceRecord::SnapshotRow {
                row, raw_document, ..
            } = &record
            {
                let decoded: Value = raw_document.decoded_value().unwrap();
                assert_eq!(&decoded, row);
                assert!(matches!(record.raw_document_context().unwrap().unwrap()
                    .raw_payload_wire("/module_metadata/module/payload").unwrap(),
                    super::super::source_document::SourcePayloadWire::CanonicalBytes(value) if value==b"null"));
            }
            cursor = serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
        }
        assert_eq!(fs::read(root.join(SNAPSHOT)).unwrap(), bytes);
    }
    #[test]
    fn plain_snapshot_cursor_restarts_and_obsolete_loose_rows_stay_evidence() {
        let root = sinomo_testdir::TestDir::new("metadata-source-plain-cursor");
        write(
            root.path(),
            SNAPSHOT,
            &document(&json!({
                "format_version":2,"tables":{"objects":[{"object_id":"first"},{"object_id":"second"}]}
            })),
        );
        write(
            root.path(),
            "records/objects/stale.sobj",
            &document(&json!({"object_id":"stale"})),
        );
        let cut = discover(root.path()).unwrap();
        assert!(cut.authoritative_snapshot_present);
        let mut cursor = SourceCursor::new(cut.clone());
        let mut rows = Vec::new();
        while let Some(record) = next(root.path(), &mut cursor).unwrap() {
            if let SourceRecord::SnapshotRow { table, row, .. } = record {
                assert_eq!(table, "objects");
                rows.push(row["object_id"].as_str().unwrap().to_owned());
            } else {
                assert!(!matches!(record, SourceRecord::LooseRow { .. }));
            }
            cursor = serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
        }
        assert_eq!(rows, vec!["first", "second"]);
        cut.verify(root.path()).unwrap();
        write(
            root.path(),
            "records/objects/stale.sobj",
            &document(&json!({"object_id":"changed"})),
        );
        assert!(cut.verify(root.path()).is_err());
    }
    #[test]
    fn framed_checkpoint_resumes_native_stream_cursor_and_validates_footer() {
        let root = sinomo_testdir::TestDir::new("metadata-source-framed-cursor");
        let path = root.join(SNAPSHOT);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let header = document(&json!({"format_version":3,"materialized_through_sequence":2}));
        let mut writer =
            MigrationStreamWriter::resume(&path, &header, &StreamCursor::default()).unwrap();
        writer
            .append(&document(
                &json!({"Table":{"op":{"type":"put-object","object":{"object_id":"one"}}}}),
            ))
            .unwrap();
        writer.append(&document(&json!({"IntentRetention":{"state":{"opaque_intent_history_evicted_object_ids":[]}}}))).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let mut cursor = SourceCursor::new(discover(root.path()).unwrap());
        assert!(matches!(
            next(root.path(), &mut cursor).unwrap(),
            Some(SourceRecord::SnapshotHeader(_))
        ));
        assert!(
            matches!(next(root.path(),&mut cursor).unwrap(),Some(SourceRecord::SnapshotRow {table,..}) if table=="$ops")
        );
        cursor = serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
        assert!(
            matches!(next(root.path(),&mut cursor).unwrap(),Some(SourceRecord::SnapshotRow {table,..}) if table=="intent_receipt_retention")
        );
        assert!(next(root.path(), &mut cursor).unwrap().is_none());
    }
    #[test]
    fn journal_exact_bytes_and_torn_tail_survive_resume_without_source_mutation() {
        let root = sinomo_testdir::TestDir::new("metadata-source-journal-tail");
        let entry = json!({"shard_id":"root","sequence":1,"mutation":{"type":"put-file-system","file_system_id":"fs"}});
        let exact = document(&entry);
        let mut bytes = record_frame(&entry);
        let complete = bytes.len();
        bytes.extend([7, 0]);
        write(
            root.path(),
            "journal/segments/00000000000000000001.slog",
            &bytes,
        );
        let mut cursor = SourceCursor::new(discover(root.path()).unwrap());
        let Some(SourceRecord::JournalFrame {
            entry: observed,
            exact_bytes,
        }) = next(root.path(), &mut cursor).unwrap()
        else {
            panic!("journal frame")
        };
        assert_eq!(observed, entry);
        assert_eq!(exact_bytes, exact);
        cursor = serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
        let Some(SourceRecord::Evidence { value, .. }) = next(root.path(), &mut cursor).unwrap()
        else {
            panic!("torn-tail evidence")
        };
        assert_eq!(value["complete_through_offset"], json!(complete));
        assert!(next(root.path(), &mut cursor).unwrap().is_none());
        assert_eq!(
            fs::read(root.join("journal/segments/00000000000000000001.slog")).unwrap(),
            bytes
        );
    }
    #[test]
    fn complete_invalid_frame_is_rejected_without_cursor_advance() {
        let root = sinomo_testdir::TestDir::new("metadata-source-invalid-complete-frame");
        write(
            root.path(),
            "records/tables/materialized.slog",
            &[3, 0, 0, 0, 1, 2, 3],
        );
        let mut cursor = SourceCursor::new(discover(root.path()).unwrap());
        let before = serde_json::to_vec(&cursor).unwrap();
        assert!(next(root.path(), &mut cursor).is_err());
        assert_eq!(serde_json::to_vec(&cursor).unwrap(), before);
        assert!(next(root.path(), &mut cursor).is_err());
    }
    #[test]
    fn discovery_keeps_journal_before_checkpoint_and_active_table_suffixes() {
        let root = sinomo_testdir::TestDir::new("metadata-source-order");
        let value = json!({"sequence":1,"ops":[]});
        for path in [
            "records/tables/materialized.slog",
            "records/tables/materialized.checkpoint.slog",
            "journal/segments/00000000000000000001.slog",
        ] {
            write(root.path(), path, &record_frame(&value));
        }
        write(
            root.path(),
            "journal/2.sobj",
            &document(&json!({"sequence":2})),
        );
        write(
            root.path(),
            "coordination/floor.sobj",
            &document(&json!({"journal_sequence":2})),
        );
        write(root.path(), "journal/write.lock", b"");
        let inventory = discover(root.path()).unwrap();
        assert_eq!(
            inventory
                .files
                .iter()
                .map(|file| file.relative.to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            [
                "journal/segments/00000000000000000001.slog",
                "journal/2.sobj",
                "records/tables/materialized.checkpoint.slog",
                "records/tables/materialized.slog",
                "coordination/floor.sobj"
            ]
        );
    }
}
