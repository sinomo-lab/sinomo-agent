//! Source-only repair checkpoint conversion. The component data owner fences
//! these inputs; the normal current queue owns all new append/floor semantics.
use super::*;
use crate::metadata::sobj_cursor::{Cursor, Item};
use serde::{Deserialize, Serialize};
use sinomo_core::storage::durable_file::{self, DurableLengthEndian, DurableRecordReadOptions};
use sinomo_dfs::{
    DfsRepairQueueBoundedStatus, DfsRepairQueueCompactionOptions, DfsRepairQueueOperation,
    LocalDfsRepairQueueStore,
};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

const SNAPSHOT: &str = "repair-attempts.sobj";
const LOG: &str = "repair-attempts.active.slog";
const MAX_FRAME: usize = 32 * 1024 * 1024;

fn finish_original_repair_conversion(root: &Path) -> Result<()> {
    let original = root.join("migration-retained/local-state/repair-v7");
    let directory = root.join("maintenance");
    let installation = original.join("installation.json");
    if installation.exists() {
        if root
            .join("migration-retained/local-state/repair-v8/ready")
            .exists()
        {
            return Err(error(
                "original repair installation remains pending beside a completed newer schema",
            ));
        }
        let install: Install =
            serde_json::from_slice(&bounded_read(&installation)?).map_err(error)?;
        finish_install_version(&directory, &original, &install, true)?;
    }
    if original.join("ready").exists() {
        if bounded_read(&original.join("ready"))? != b"dfs-repair-queue-current-v7\n" {
            return Err(error("original repair conversion marker is malformed"));
        }
        return Ok(());
    }
    if original.join("progress.json").exists() {
        // No installation was published: its candidate remains an archived
        // rebuildable DATA cursor. Require the same canonical source bytes
        // before starting the current schema's independent conversion.
        let mut value: Value =
            serde_json::from_slice(&bounded_read(&original.join("progress.json"))?)
                .map_err(error)?;
        if let Some(operations) = value
            .pointer_mut("/pending/operations")
            .and_then(Value::as_array_mut)
        {
            for operation in operations {
                for tag in ["set", "enqueue-if-missing"] {
                    if let Some(record) = operation
                        .get_mut(tag)
                        .and_then(|body| body.get_mut("record"))
                    {
                        normalize_record(record)?;
                    }
                }
            }
        }
        let progress: Progress = serde_json::from_value(value).map_err(error)?;
        if identity(&directory.join(SNAPSHOT))? != progress.snapshot
            || identity(&directory.join(LOG))? != progress.log
        {
            return Err(error(
                "original repair cursor source changed before the schema upgrade",
            ));
        }
    }
    Ok(())
}

pub(super) fn current_or_absent(root: &Path) -> Result<bool> {
    if root
        .join("migration-retained/local-state/repair-v7/installation.json")
        .exists()
    {
        return Ok(false);
    }
    let stage = root.join("migration-retained/local-state/repair-v8");
    if !stage.join("ready").exists()
        && root
            .join("migration-retained/local-state/repair-v7/progress.json")
            .exists()
    {
        return Ok(false);
    }
    if stage.join("installation.json").exists() {
        return Ok(false);
    }
    if stage.join("ready").exists() {
        if bounded_read(&stage.join("ready"))? != b"dfs-repair-queue-current-v8\n" {
            return Err(error("repair conversion marker is malformed"));
        }
        return Ok(true);
    }
    let directory = root.join("maintenance");
    if !directory.join(SNAPSHOT).exists() && !directory.join(LOG).exists() {
        return Ok(true);
    }
    // The converter's completion marker names a verified conversion. An
    // unmarked queue is inspected incrementally, including every retry field.
    Ok(false)
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    bytes: u64,
    digest: String,
}
fn identity(path: &Path) -> Result<Option<Identity>> {
    if !path.exists() {
        return Ok(None);
    }
    if !fs::symlink_metadata(path).map_err(error)?.is_file() {
        return Err(error("repair input is not a physical regular file"));
    }
    let mut file = File::open(path).map_err(error)?;
    let before = file.metadata().map_err(error)?;
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    let mut bytes = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(error)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        bytes += count as u64;
    }
    let after = file.metadata().map_err(error)?;
    if bytes != before.len()
        || after.len() != before.len()
        || before.modified().map_err(error)? != after.modified().map_err(error)?
    {
        return Err(error("repair source changed during capture"));
    }
    Ok(Some(Identity {
        bytes,
        digest: hash.finalize().to_hex().to_string(),
    }))
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    cursor: Option<Cursor>,
    snapshot_done: bool,
    records_seen: bool,
    floor: Option<u64>,
    log_offset: u64,
    log_sequence: Option<u64>,
    source_head: u64,
    stage_head: u64,
    stage_log_offset: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    operations: Vec<DfsRepairQueueOperation>,
    after: Position,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    snapshot: Option<Identity>,
    log: Option<Identity>,
    position: Position,
    pending: Option<Pending>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Batch {
    sequence: u64,
    operations: Vec<DfsRepairQueueOperation>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Install {
    source_snapshot: Option<Identity>,
    source_log: Option<Identity>,
    current_snapshot: Option<Identity>,
    current_head: u64,
}
fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    write_file_atomic(
        path,
        &serde_json::to_vec(value).map_err(error)?,
        DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
    )
    .map_err(error)
}
fn source_batch(file: &mut File, path: &Path) -> Result<Option<Batch>> {
    let Some(frame) = durable_file::read_length_prefixed_record(
        file,
        path,
        DurableRecordReadOptions::new(DurableLengthEndian::Little).max_record_bytes(MAX_FRAME),
    )
    .map_err(error)?
    else {
        return Ok(None);
    };
    let mut value: Value = sobj::from_bytes(&frame.bytes).map_err(error)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| error("repair batch is not an object"))?;
    if object.len() != 2 || !object.contains_key("sequence") || !object.contains_key("operations") {
        return Err(error("repair batch has unknown or missing fields"));
    }
    let operations = value
        .get_mut("operations")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| error("repair batch operations are not a list"))?;
    for operation in operations {
        let variants = operation
            .as_object_mut()
            .ok_or_else(|| error("repair operation is not tagged"))?;
        if variants.len() != 1 {
            return Err(error("repair operation has multiple tags"));
        }
        let (tag, body) = variants.iter_mut().next().expect("one tag");
        if body.as_object().is_none_or(|fields| fields.len() != 1) {
            return Err(error("repair operation has unknown or missing fields"));
        }
        match tag.as_str() {
            "set" | "enqueue-if-missing" => normalize_record(
                body.get_mut("record")
                    .ok_or_else(|| error("repair operation omitted its record"))?,
            )?,
            "remove" => {
                let key = body
                    .get_mut("key")
                    .ok_or_else(|| error("repair removal omitted its key"))?;
                if key.as_object().is_none_or(|fields| {
                    fields.len() != 2
                        || !fields.contains_key("intent_id")
                        || !fields.contains_key("missing_replica_node_id")
                }) {
                    return Err(error("repair removal key has unknown or missing fields"));
                }
                key["missing_replica_node_id"] =
                    serde_json::to_value(source_id(key, "missing_replica_node_id")?)
                        .map_err(error)?;
            }
            _ => return Err(error("unknown source repair operation")),
        }
    }
    serde_json::from_value(value).map(Some).map_err(error)
}
fn normalize_record(value: &mut Value) -> Result<()> {
    const FIELDS: &[&str] = &[
        "intent_id",
        "object_id",
        "version",
        "content_hash",
        "missing_replica_node_id",
        "attempts",
        "in_flight",
        "last_attempt_unix_nanos",
        "next_retry_unix_nanos",
        "transfer_request_id",
        "last_error",
        "next_chunk_index",
        "transfer_delivery_state",
        "completed_transfer_request_id",
        "transfer_kind",
    ];
    let fields = value
        .as_object()
        .ok_or_else(|| error("repair record is not an object"))?;
    if fields.keys().any(|field| !FIELDS.contains(&field.as_str())) {
        return Err(error(
            "unknown source retry-state field cannot be discarded",
        ));
    }
    for field in ["object_id", "version", "missing_replica_node_id"] {
        value[field] = serde_json::to_value(source_id(value, field)?).map_err(error)?;
    }
    // Before e519f07e, the real writer classified stale replica base as lost
    // authority and emitted MAX. Its exact source bytes are already archived;
    // current retry DATA must not retain that retired classification forever.
    let last_error = value
        .get("last_error")
        .and_then(Value::as_str)
        .unwrap_or("");
    let lost_authority = last_error.contains("source is not current metadata shard primary")
        || last_error.contains("source is not authorized to fetch DFS blob");
    let old_stale_max = !lost_authority
        && last_error.contains("stale DFS repair base")
        && value.get("next_retry_unix_nanos").and_then(Value::as_u64) == Some(u64::MAX);
    let old_chunk = match value.get("next_chunk_index") {
        None | Some(Value::Null) => false,
        Some(index)
            if index
                .as_u64()
                .is_some_and(|index| u32::try_from(index).is_ok()) =>
        {
            true
        }
        Some(_) => {
            return Err(error(
                "original repair chunk index is not representable as u32",
            ));
        }
    };
    value.as_object_mut().unwrap().remove("next_chunk_index");
    if old_stale_max || old_chunk {
        // Retire the old chunk/request session, without executing its protocol
        // or granting repair permission. The current owner revalidates its real
        // job/head and creates a fresh transfer. Ordinary finite backoff stays.
        value["in_flight"] = Value::Bool(false);
        value["transfer_request_id"] = Value::Null;
    }
    if old_stale_max {
        // Zero is NEW immediately-eligible retry DATA, never an old timestamp.
        value["next_retry_unix_nanos"] = Value::from(0);
    }
    // Only MIG imports missing original observations. Null is current UNKNOWN,
    // never a claim that an old request was not sent or that its reply returned.
    let fields = value.as_object_mut().expect("validated source record");
    fields
        .entry("transfer_delivery_state")
        .or_insert(Value::Null);
    fields
        .entry("completed_transfer_request_id")
        .or_insert(Value::Null);
    fields.entry("transfer_kind").or_insert(Value::Null);
    // Source counters, last-attempt clock, finite backoff and exact diagnostic
    // retain their meaning. Original retired session/MAX data stays in archive.
    let _: sinomo_dfs::DfsRepairAttemptRecord =
        serde_json::from_value(value.clone()).map_err(error)?;
    Ok(())
}
fn current_frame(file: &mut File, path: &Path) -> Result<Option<Batch>> {
    durable_file::read_length_prefixed_record(
        file,
        path,
        DurableRecordReadOptions::new(DurableLengthEndian::Little).max_record_bytes(MAX_FRAME),
    )
    .map_err(error)?
    .map(|frame| sobj::from_bytes(&frame.bytes).map_err(error))
    .transpose()
}

pub(super) struct Conversion {
    directory: PathBuf,
    stage: PathBuf,
    progress: Progress,
    queue: LocalDfsRepairQueueStore,
    source_stats: [Option<(u64, std::time::SystemTime)>; 2],
}
impl Conversion {
    pub(super) fn open(root: &Path) -> Result<Option<Self>> {
        finish_original_repair_conversion(root)?;
        let directory = root.join("maintenance");
        let stage = root.join("migration-retained/local-state/repair-v8");
        let install_path = stage.join("installation.json");
        if install_path.exists() {
            let install: Install =
                serde_json::from_slice(&bounded_read(&install_path)?).map_err(error)?;
            finish_install(&directory, &stage, &install)?;
            return Ok(None);
        }
        if stage.join("ready").exists() {
            if bounded_read(&stage.join("ready"))? != b"dfs-repair-queue-current-v8\n" {
                return Err(error("repair conversion marker is malformed"));
            }
            return Ok(None);
        }
        let snapshot = identity(&directory.join(SNAPSHOT))?;
        let log = identity(&directory.join(LOG))?;
        if snapshot.is_none() && log.is_none() {
            if root
                .join("migration-retained/local-state/repair-v7/ready")
                .exists()
            {
                fs::create_dir_all(&stage).map_err(error)?;
                write_file_atomic(
                    &stage.join("ready"),
                    b"dfs-repair-queue-current-v8\n",
                    DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
                )
                .map_err(error)?;
            }
            return Ok(None);
        }
        if !stage.join("progress.json").exists() && current_or_absent(root)? {
            return Ok(None);
        }
        fs::create_dir_all(&stage).map_err(error)?;
        let progress_path = stage.join("progress.json");
        let progress = if progress_path.exists() {
            let progress: Progress =
                serde_json::from_slice(&fs::read(&progress_path).map_err(error)?).map_err(error)?;
            if progress.snapshot != snapshot || progress.log != log {
                return Err(error(
                    "repair source differs from the durable conversion cut",
                ));
            }
            progress
        } else {
            if stage.join(SNAPSHOT).exists() || stage.join(LOG).exists() {
                return Err(error("repair candidate has no durable source cursor"));
            }
            let progress = Progress {
                snapshot,
                log,
                pending: None,
                position: Position {
                    cursor: if directory.join(SNAPSHOT).exists() {
                        Some(Cursor::open_sequence(&directory.join(SNAPSHOT), "records")?)
                    } else {
                        None
                    },
                    snapshot_done: !directory.join(SNAPSHOT).exists(),
                    records_seen: false,
                    floor: None,
                    log_offset: 0,
                    log_sequence: None,
                    source_head: 0,
                    stage_head: 0,
                    stage_log_offset: 0,
                },
            };
            save(&progress_path, &progress)?;
            progress
        };
        let source_stats = [
            stats(&directory.join(SNAPSHOT))?,
            stats(&directory.join(LOG))?,
        ];
        let queue = LocalDfsRepairQueueStore::new(stage.join(SNAPSHOT));
        let mut conversion = Self {
            directory,
            stage,
            progress,
            queue,
            source_stats,
        };
        conversion.finish_pending()?;
        // Reopen through the normal current owner. This also validates every
        // candidate batch after an interrupted process.
        if native_head(&conversion.queue)? != conversion.progress.position.stage_head {
            return Err(error("repair candidate and durable cursor disagree"));
        }
        Ok(Some(conversion))
    }
    fn finish_pending(&mut self) -> Result<()> {
        let Some(pending) = &self.progress.pending else {
            return Ok(());
        };
        let path = self.stage.join(LOG);
        let expected = self
            .progress
            .position
            .stage_head
            .checked_add(1)
            .ok_or_else(|| error("new repair queue sequence overflow"))?;
        let recovered = if path.exists() {
            let mut file = File::open(&path).map_err(error)?;
            file.seek(SeekFrom::Start(self.progress.position.stage_log_offset))
                .map_err(error)?;
            match current_frame(&mut file, &path)? {
                Some(batch) => {
                    if batch.sequence != expected || batch.operations != pending.operations {
                        return Err(error(
                            "uncertain repair append differs from its durable pending input",
                        ));
                    }
                    let end = file.stream_position().map_err(error)?;
                    if end != file.metadata().map_err(error)?.len() {
                        return Err(error("unowned suffix follows an uncertain repair append"));
                    }
                    file.sync_all().map_err(error)?;
                    sync_directory(&self.stage).map_err(error)?;
                    Some(end)
                }
                None => None,
            }
        } else {
            None
        };
        let actual_head = native_head(&self.queue)?;
        let required_head = if recovered.is_some() {
            expected
        } else {
            self.progress.position.stage_head
        };
        if actual_head != required_head {
            return Err(error(
                "repair append cursor and actual current queue generation disagree",
            ));
        }
        let offset = match recovered {
            Some(offset) => offset,
            None => {
                self.queue
                    .append_operations(pending.operations.clone())
                    .map_err(error)?;
                if native_head(&self.queue)? != expected {
                    return Err(error(
                        "normal repair append advanced an unexpected generation",
                    ));
                }
                fs::metadata(&path).map_err(error)?.len()
            }
        };
        let mut after = pending.after.clone();
        after.stage_head = expected;
        after.stage_log_offset = offset;
        self.progress.position = after;
        self.progress.pending = None;
        save(&self.stage.join("progress.json"), &self.progress)
    }
    fn advance(
        &mut self,
        mut after: Position,
        operations: Vec<DfsRepairQueueOperation>,
    ) -> Result<()> {
        if operations.is_empty() {
            after.stage_head = self.progress.position.stage_head;
            after.stage_log_offset = self.progress.position.stage_log_offset;
            self.progress.position = after;
            save(&self.stage.join("progress.json"), &self.progress)
        } else {
            self.progress.pending = Some(Pending { operations, after });
            save(&self.stage.join("progress.json"), &self.progress)?;
            self.finish_pending()
        }
    }
    fn snapshot_key(&self, record: &sinomo_dfs::DfsRepairAttemptRecord) -> Result<()> {
        let key = sinomo_dfs::DfsRepairQueueKey::from_record(record);
        let key_bytes = serde_json::to_vec(&key).map_err(error)?;
        let input = serde_json::to_vec(&(
            &key,
            self.progress
                .position
                .cursor
                .as_ref()
                .ok_or_else(|| error("repair snapshot key has no source cursor"))?
                .position(),
            blake3::hash(&serde_json::to_vec(record).map_err(error)?)
                .to_hex()
                .to_string(),
        ))
        .map_err(error)?;
        let path = self
            .stage
            .join("snapshot-keys")
            .join(blake3::hash(&key_bytes).to_hex().to_string());
        if path.exists() {
            if bounded_read(&path)? != input {
                return Err(error(
                    "repair snapshot contains a duplicate normalized queue key",
                ));
            }
        } else {
            // Publish before the pending append. A crash retry may repeat this
            // exact source position; a later row or named-ID alias may not.
            write_file_atomic(
                &path,
                &input,
                DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
            )
            .map_err(error)?;
        }
        Ok(())
    }
    pub(super) fn step(&mut self, budget: usize) -> Result<(bool, usize)> {
        if [
            stats(&self.directory.join(SNAPSHOT))?,
            stats(&self.directory.join(LOG))?,
        ] != self.source_stats
        {
            return Err(error("repair source changed while fenced for conversion"));
        }
        self.finish_pending()?;
        for processed in 0..budget {
            let mut after = self.progress.position.clone();
            if !after.snapshot_done {
                match after
                    .cursor
                    .as_mut()
                    .expect("snapshot cursor")
                    .next(&self.directory.join(SNAPSHOT))?
                {
                    Some(Item::Header(key, value)) if key == "records" && value.is_null() => {
                        if after.records_seen {
                            return Err(error("repair snapshot repeats its record list"));
                        }
                        after.records_seen = true;
                    }
                    Some(Item::Header(key, value)) if key == "applied_through_sequence" => {
                        if after.floor.is_some() {
                            return Err(error("repair snapshot repeats its floor"));
                        }
                        after.floor = Some(
                            value
                                .as_u64()
                                .ok_or_else(|| error("repair snapshot floor is not u64"))?,
                        );
                    }
                    Some(Item::Row(key, mut value, _)) if key == "records" => {
                        normalize_record(&mut value)?;
                        let record = serde_json::from_value(value).map_err(error)?;
                        self.snapshot_key(&record)?;
                        self.advance(
                            after,
                            vec![DfsRepairQueueOperation::EnqueueIfMissing { record }],
                        )?;
                        continue;
                    }
                    None => {
                        if !after.records_seen {
                            return Err(error("repair snapshot omitted its records"));
                        }
                        // The historical records-only checkpoint had an explicit
                        // zero-floor contract. It does not borrow metadata height.
                        after.source_head = after.floor.unwrap_or(0);
                        after.snapshot_done = true;
                        after.cursor = None;
                    }
                    _ => return Err(error("unknown source repair snapshot field")),
                }
                self.advance(after, Vec::new())?;
                continue;
            }
            if self.progress.log.is_some() {
                let path = self.directory.join(LOG);
                let mut file = File::open(&path).map_err(error)?;
                file.seek(SeekFrom::Start(after.log_offset))
                    .map_err(error)?;
                if let Some(batch) = source_batch(&mut file, &path)? {
                    if after
                        .log_sequence
                        .is_some_and(|previous| batch.sequence <= previous)
                    {
                        return Err(error("source repair log forks or regresses"));
                    }
                    after.log_sequence = Some(batch.sequence);
                    after.log_offset = file.stream_position().map_err(error)?;
                    let operations = if batch.sequence <= after.floor.unwrap_or(0) {
                        Vec::new()
                    } else {
                        if batch.sequence
                            != after
                                .source_head
                                .checked_add(1)
                                .ok_or_else(|| error("source repair sequence overflow"))?
                        {
                            return Err(error("source repair log has a sequence gap"));
                        }
                        after.source_head = batch.sequence;
                        batch.operations
                    };
                    self.advance(after, operations)?;
                    continue;
                }
                // The shared reader rewinds a genuinely incomplete EOF frame;
                // its physical bytes remain untouched in the retained source.
            }
            if identity(&self.directory.join(SNAPSHOT))? != self.progress.snapshot
                || identity(&self.directory.join(LOG))? != self.progress.log
            {
                return Err(error("repair source bytes changed before publication"));
            }
            self.queue
                .compact_if_needed(DfsRepairQueueCompactionOptions::force())
                .map_err(error)?;
            let install = Install {
                source_snapshot: self.progress.snapshot.clone(),
                source_log: self.progress.log.clone(),
                current_snapshot: identity(&self.stage.join(SNAPSHOT))?,
                current_head: self.progress.position.stage_head,
            };
            save(&self.stage.join("installation.json"), &install)?;
            finish_install(&self.directory, &self.stage, &install)?;
            return Ok((true, processed + 1));
        }
        Ok((false, budget))
    }
}
fn native_head(queue: &LocalDfsRepairQueueStore) -> Result<u64> {
    // Empty operations request the existing owner's normal recovery without
    // adding a batch. Its bounded status then avoids cloning all queue rows.
    queue.append_operations(Vec::new()).map_err(error)?;
    match queue.bounded_status_view(0).map_err(error)? {
        DfsRepairQueueBoundedStatus::Hydrated(status) => Ok(status.head_sequence),
        DfsRepairQueueBoundedStatus::NotHydrated => Err(error("current queue did not materialize")),
    }
}
fn stats(path: &Path) -> Result<Option<(u64, std::time::SystemTime)>> {
    if !path.exists() {
        return Ok(None);
    }
    let stat = fs::symlink_metadata(path).map_err(error)?;
    if !stat.is_file() {
        return Err(error("repair source is not a regular file"));
    }
    Ok(Some((stat.len(), stat.modified().map_err(error)?)))
}
fn retain(source: &Path, destination: &Path, expected: &Option<Identity>) -> Result<()> {
    match expected {
        None => {
            if source.exists() || destination.exists() {
                return Err(error("absent repair input appeared during install"));
            }
        }
        Some(expected) => {
            if destination.exists() {
                if identity(destination)?.as_ref() != Some(expected) {
                    return Err(error("retained repair input changed"));
                }
                if source.exists() {
                    return Err(error("repair input exists at two installation locations"));
                }
            } else {
                if identity(source)?.as_ref() != Some(expected) {
                    return Err(error("repair input changed before retention"));
                }
                fs::rename(source, destination).map_err(error)?;
                sync_directory(source.parent().expect("source parent")).map_err(error)?;
                sync_directory(destination.parent().expect("retained parent")).map_err(error)?;
            }
        }
    }
    Ok(())
}
fn finish_install(directory: &Path, stage: &Path, install: &Install) -> Result<()> {
    finish_install_version(directory, stage, install, false)
}
fn validate_install_snapshot(path: &Path, head: u64, original_v7: bool) -> Result<()> {
    let mut value: Value = sobj::from_bytes(&bounded_read(path)?).map_err(error)?;
    let fields = value
        .as_object()
        .ok_or_else(|| error("installed repair snapshot is not an object"))?;
    if fields.len() != 2
        || !fields.contains_key("records")
        || fields
            .get("applied_through_sequence")
            .and_then(Value::as_u64)
            != Some(head)
    {
        return Err(error(
            "repair installation changes its complete floor contract",
        ));
    }
    let mut keys = std::collections::BTreeSet::new();
    for record in value
        .get_mut("records")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| error("installed repair records are not an array"))?
    {
        if original_v7 {
            normalize_record(record)?;
        }
        let record: sinomo_dfs::DfsRepairAttemptRecord =
            serde_json::from_value(record.clone()).map_err(error)?;
        if !keys.insert((record.intent_id, record.missing_replica_node_id)) {
            return Err(error("repair installation repeats a queue key"));
        }
    }
    Ok(())
}
fn validate_retention_cut(
    source: &Path,
    destination: &Path,
    expected: &Option<Identity>,
) -> Result<()> {
    let canonical = identity(source)?;
    let retained = identity(destination)?;
    match expected {
        Some(expected)
            if (canonical.as_ref() == Some(expected) && retained.is_none())
                || (retained.as_ref() == Some(expected) && canonical.is_none()) =>
        {
            Ok(())
        }
        None if canonical.is_none() && retained.is_none() => Ok(()),
        _ => Err(error(
            "repair installation original differs from its exact physical cut",
        )),
    }
}
fn finish_install_version(
    directory: &Path,
    stage: &Path,
    install: &Install,
    original_v7: bool,
) -> Result<()> {
    let retained = stage.join("source");
    let snapshot = directory.join(SNAPSHOT);
    let candidate = stage.join(SNAPSHOT);
    let originals_retained = identity(&retained.join(SNAPSHOT))? == install.source_snapshot
        && identity(&retained.join(LOG))? == install.source_log;
    let installed = originals_retained && identity(&snapshot)? == install.current_snapshot;
    // Validate the exact original saved plan and full target schema BEFORE any
    // archive rename. The original target/floor are never rewritten for v8.
    match &install.current_snapshot {
        Some(expected) => {
            let target = if installed { &snapshot } else { &candidate };
            if identity(target)?.as_ref() != Some(expected) {
                return Err(error("current repair candidate changed before install"));
            }
            validate_install_snapshot(target, install.current_head, original_v7)?;
        }
        None => {
            if install.current_head != 0 || candidate.exists() {
                return Err(error(
                    "snapshot-absent repair installation violates its original empty target contract",
                ));
            }
        }
    }
    if !installed {
        validate_retention_cut(
            &directory.join(LOG),
            &retained.join(LOG),
            &install.source_log,
        )?;
        validate_retention_cut(
            &snapshot,
            &retained.join(SNAPSHOT),
            &install.source_snapshot,
        )?;
    } else if directory.join(LOG).exists() {
        return Err(error(
            "old repair log remains beside the installed checkpoint",
        ));
    }
    fs::create_dir_all(directory).map_err(error)?;
    fs::create_dir_all(&retained).map_err(error)?;
    sync_archive_parents(
        stage.parent().expect("stage parent"),
        &retained.join(SNAPSHOT),
    )?;
    if !installed {
        retain(
            &directory.join(LOG),
            &retained.join(LOG),
            &install.source_log,
        )?;
        retain(
            &snapshot,
            &retained.join(SNAPSHOT),
            &install.source_snapshot,
        )?;
        if install.current_snapshot.is_some() {
            fs::rename(&candidate, &snapshot).map_err(error)?;
            sync_directory(directory).map_err(error)?;
            sync_directory(stage).map_err(error)?;
        }
    }
    sync_directory(directory).map_err(error)?;
    sync_directory(stage).map_err(error)?;
    if directory.join(LOG).exists() {
        return Err(error("old repair log remains beside the new checkpoint"));
    }
    if !original_v7
        && install.current_snapshot.is_some()
        && native_head(&LocalDfsRepairQueueStore::new(&snapshot))? != install.current_head
    {
        return Err(error(
            "installed current repair floor differs from actual queue appends",
        ));
    }
    write_file_atomic(
        &stage.join("ready"),
        if original_v7 {
            b"dfs-repair-queue-current-v7\n"
        } else {
            b"dfs-repair-queue-current-v8\n"
        },
        DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
    )
    .map_err(error)?;
    fs::remove_file(stage.join("installation.json")).map_err(error)?;
    sync_directory(stage).map_err(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn record() -> Value {
        json!({"intent_id":"old-transfer","object_id":"object","version":"version",
            "content_hash":"recorded-hash","missing_replica_node_id":"receiver",
            "attempts":7,"in_flight":true,"last_attempt_unix_nanos":1234,
            "next_retry_unix_nanos":5678,"transfer_request_id":"actual-request","last_error":"busy"})
    }
    fn source(root: &Path, snapshot: Value, batches: &[Value], torn: bool) {
        let directory = root.join("maintenance");
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join(SNAPSHOT),
            sobj::to_bytes(&snapshot, 1, LengthMode::U32).unwrap(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        for batch in batches {
            let frame = sobj::to_bytes(batch, 1, LengthMode::U32).unwrap();
            bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes());
            bytes.extend(frame);
        }
        if torn {
            bytes.extend_from_slice(&100u32.to_le_bytes());
            bytes.extend_from_slice(b"partial");
        }
        fs::write(directory.join(LOG), bytes).unwrap();
    }
    fn complete(root: &Path) {
        let converter = LocalStateConverter::default();
        for _ in 0..100 {
            if matches!(
                converter.prepare(root, 1).unwrap(),
                DfsStoragePreparation::Ready
            ) {
                return;
            }
        }
        panic!("bounded conversion did not finish");
    }
    #[test]
    fn original_missing_transfer_observations_import_only_as_current_unknown() {
        let mut original = record();
        assert!(
            serde_json::from_value::<sinomo_dfs::DfsRepairAttemptRecord>(original.clone(),)
                .is_err()
        );
        normalize_record(&mut original).unwrap();
        assert!(original["transfer_delivery_state"].is_null());
        assert!(original["completed_transfer_request_id"].is_null());
        assert!(original["transfer_kind"].is_null());
        let current: sinomo_dfs::DfsRepairAttemptRecord =
            serde_json::from_value(original.clone()).unwrap();
        assert_eq!(
            current.transfer_request_id.as_deref(),
            Some("actual-request")
        );
        assert!(current.in_flight);
        let mut invalid = original.clone();
        invalid["transfer_delivery_state"] = json!("made-up-delivery");
        assert!(normalize_record(&mut invalid).is_err());
    }

    #[test]
    fn native_queue_preserves_retry_state_and_uses_its_own_floor() {
        let root = sinomo_testdir::TestDir::new("migration-repair-retry");
        let mut updated = record();
        updated["attempts"] = json!(8);
        updated["next_retry_unix_nanos"] = json!(9999);
        source(
            &root,
            json!({"records":[record()],"applied_through_sequence":40}),
            &[json!({"sequence":41,"operations":[{"set":{"record":updated.clone()}}]})],
            true,
        );
        let converter = LocalStateConverter::default();
        assert!(matches!(
            converter.prepare(&root, 1).unwrap(),
            DfsStoragePreparation::Pending { .. }
        ));
        drop(converter);
        complete(&root);
        normalize_record(&mut updated).unwrap();
        let current = LocalDfsRepairQueueStore::new(root.join("maintenance").join(SNAPSHOT))
            .view()
            .unwrap();
        assert_eq!(current.head_sequence, 2);
        assert_eq!(
            current.records,
            vec![serde_json::from_value(updated).unwrap()]
        );
        assert!(
            root.join("migration-retained/local-state/repair-v8/source")
                .join(LOG)
                .exists()
        );
        assert!(!root.join("maintenance").join(LOG).exists());
    }
    #[test]
    fn actual_old_stale_max_and_chunk_cursor_retire_only_retry_data() {
        let root = sinomo_testdir::TestDir::new("migration-pre-e519-repair");
        let mut stale = record();
        stale["next_retry_unix_nanos"] = json!(u64::MAX);
        stale["next_chunk_index"] = json!(3);
        stale["last_error"] =
            json!("stale DFS repair base for object: expected old, current prior");
        let mut lost = record();
        lost["intent_id"] = json!("original-lost-authority");
        lost["next_retry_unix_nanos"] = json!(u64::MAX);
        lost["last_error"] = json!("source is not current metadata shard primary");
        let snapshot = json!({"records":[stale.clone(),lost.clone()]});
        source(&root, snapshot, &[], false);
        let original = fs::read(root.join("maintenance").join(SNAPSHOT)).unwrap();
        complete(&root);
        assert_eq!(
            fs::read(
                root.join("migration-retained/local-state/repair-v8/source")
                    .join(SNAPSHOT)
            )
            .unwrap(),
            original
        );
        let rows = LocalDfsRepairQueueStore::new(root.join("maintenance").join(SNAPSHOT))
            .view()
            .unwrap()
            .records;
        let converted = rows
            .iter()
            .find(|row| row.intent_id == "old-transfer")
            .unwrap();
        assert_eq!(converted.attempts, 7);
        assert_eq!(converted.last_attempt_unix_nanos, 1234);
        assert_eq!(
            converted.last_error.as_deref(),
            stale["last_error"].as_str()
        );
        assert_eq!(converted.next_retry_unix_nanos, 0);
        assert!(!converted.in_flight);
        assert!(converted.transfer_request_id.is_none());
        let retained = rows
            .iter()
            .find(|row| row.intent_id == "original-lost-authority")
            .unwrap();
        assert_eq!(
            retained.next_retry_unix_nanos,
            u64::MAX,
            "genuine lost-authority suppression remains DATA"
        );
        assert_eq!(retained.last_attempt_unix_nanos, 1234);

        let mut malformed = record();
        malformed["next_chunk_index"] = json!("invented-index");
        assert!(normalize_record(&mut malformed).is_err());
        malformed["next_chunk_index"] = json!(u64::from(u32::MAX) + 1);
        assert!(normalize_record(&mut malformed).is_err());
        malformed["next_chunk_index"] = json!(u32::MAX);
        assert!(normalize_record(&mut malformed).is_ok());
        let mut finite = record();
        finite["next_chunk_index"] = json!(1);
        normalize_record(&mut finite).unwrap();
        assert_eq!(
            finite["next_retry_unix_nanos"],
            json!(5678),
            "real finite backoff is retained"
        );
        assert!(!finite["in_flight"].as_bool().unwrap());
    }

    #[test]
    fn a_source_gap_never_publishes_or_mutates_originals() {
        let root = sinomo_testdir::TestDir::new("migration-repair-gap");
        source(
            &root,
            json!({"records":[record()]}),
            &[json!({"sequence":2,"operations":[]})],
            false,
        );
        let snapshot = fs::read(root.join("maintenance").join(SNAPSHOT)).unwrap();
        let log = fs::read(root.join("maintenance").join(LOG)).unwrap();
        let mut conversion = Conversion::open(&root).unwrap().unwrap();
        assert!(conversion.step(20).is_err());
        assert_eq!(
            fs::read(root.join("maintenance").join(SNAPSHOT)).unwrap(),
            snapshot
        );
        assert_eq!(fs::read(root.join("maintenance").join(LOG)).unwrap(), log);
        assert!(!conversion.stage.join("installation.json").exists());
    }
    #[test]
    fn duplicate_and_named_alias_keys_do_not_discard_retry_state() {
        for named_alias in [false, true] {
            let root = sinomo_testdir::TestDir::new("migration-repair-duplicate-key");
            let mut duplicate = record();
            duplicate["attempts"] = json!(9);
            if named_alias {
                duplicate["missing_replica_node_id"] =
                    serde_json::to_value(source_id(&duplicate, "missing_replica_node_id").unwrap())
                        .unwrap();
            }
            source(&root, json!({"records":[record(),duplicate]}), &[], false);
            let original = fs::read(root.join("maintenance").join(SNAPSHOT)).unwrap();
            let mut conversion = Conversion::open(&root).unwrap().unwrap();
            assert!(!conversion.step(2).unwrap().0);
            drop(conversion);
            let mut resumed = Conversion::open(&root).unwrap().unwrap();
            assert!(resumed.step(20).is_err());
            assert_eq!(
                fs::read(root.join("maintenance").join(SNAPSHOT)).unwrap(),
                original
            );
            assert!(!resumed.stage.join("installation.json").exists());
        }
    }
    #[test]
    fn recovery_after_log_retention_installs_exact_current_checkpoint() {
        let root = sinomo_testdir::TestDir::new("migration-repair-install-recovery");
        source(&root, json!({"records":[record()]}), &[], false);
        let directory = root.join("maintenance");
        let stage = root.join("migration-retained/local-state/repair-v8");
        fs::create_dir_all(stage.join("source")).unwrap();
        let queue = LocalDfsRepairQueueStore::new(stage.join(SNAPSHOT));
        let mut normalized = record();
        normalize_record(&mut normalized).unwrap();
        queue
            .append_enqueues([serde_json::from_value(normalized).unwrap()])
            .unwrap();
        queue
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let install = Install {
            source_snapshot: identity(&directory.join(SNAPSHOT)).unwrap(),
            source_log: identity(&directory.join(LOG)).unwrap(),
            current_snapshot: identity(&stage.join(SNAPSHOT)).unwrap(),
            current_head: 1,
        };
        save(&stage.join("installation.json"), &install).unwrap();
        fs::rename(directory.join(LOG), stage.join("source").join(LOG)).unwrap();
        assert!(Conversion::open(&root).unwrap().is_none());
        assert_eq!(
            LocalDfsRepairQueueStore::new(directory.join(SNAPSHOT))
                .view()
                .unwrap()
                .head_sequence,
            1
        );
        assert!(stage.join("source").join(SNAPSHOT).exists());
        assert!(!stage.join("installation.json").exists());
    }
    #[test]
    fn uncertain_append_is_recovered_only_from_its_exact_native_frame() {
        let root = sinomo_testdir::TestDir::new("migration-repair-uncertain-append");
        source(&root, json!({"records":[record()]}), &[], false);
        let mut conversion = Conversion::open(&root).unwrap().unwrap();
        let mut normalized = record();
        normalize_record(&mut normalized).unwrap();
        let operations = vec![DfsRepairQueueOperation::EnqueueIfMissing {
            record: serde_json::from_value(normalized).unwrap(),
        }];
        let after = conversion.progress.position.clone();
        conversion.progress.pending = Some(Pending {
            operations: operations.clone(),
            after,
        });
        save(
            &conversion.stage.join("progress.json"),
            &conversion.progress,
        )
        .unwrap();
        conversion.queue.append_operations(operations).unwrap();
        drop(conversion);
        let recovered = Conversion::open(&root).unwrap().unwrap();
        assert!(recovered.progress.pending.is_none());
        assert_eq!(recovered.progress.position.stage_head, 1);
        assert_eq!(recovered.queue.view().unwrap().head_sequence, 1);
    }
    fn original_install(
        root: &Path,
        cut: usize,
        candidate_value: Option<Value>,
        head: u64,
    ) -> Install {
        source(root, json!({"records":[record()]}), &[], false);
        let directory = root.join("maintenance");
        let stage = root.join("migration-retained/local-state/repair-v7");
        fs::create_dir_all(stage.join("source")).unwrap();
        if let Some(value) = candidate_value {
            fs::write(
                stage.join(SNAPSHOT),
                sobj::to_bytes(&value, 1, LengthMode::U32).unwrap(),
            )
            .unwrap();
        }
        let install = Install {
            source_snapshot: identity(&directory.join(SNAPSHOT)).unwrap(),
            source_log: identity(&directory.join(LOG)).unwrap(),
            current_snapshot: identity(&stage.join(SNAPSHOT)).unwrap(),
            current_head: head,
        };
        save(&stage.join("installation.json"), &install).unwrap();
        if cut >= 1 {
            fs::rename(directory.join(LOG), stage.join("source").join(LOG)).unwrap();
        }
        if cut >= 2 {
            fs::rename(
                directory.join(SNAPSHOT),
                stage.join("source").join(SNAPSHOT),
            )
            .unwrap();
        }
        if cut >= 3 && install.current_snapshot.is_some() {
            fs::rename(stage.join(SNAPSHOT), directory.join(SNAPSHOT)).unwrap();
        }
        install
    }
    #[test]
    fn original_v7_pending_install_recovers_every_physical_cut_before_strict_v8() {
        for cut in 0..=3 {
            let root = sinomo_testdir::TestDir::new("migration-repair-original-install-cuts");
            let original = json!({"records":[record()],"applied_through_sequence":17});
            let install = original_install(&root, cut, Some(original), 17);
            let stage = root.join("migration-retained/local-state/repair-v7");
            let plan = fs::read(stage.join("installation.json")).unwrap();
            finish_original_repair_conversion(&root).unwrap();
            assert_eq!(
                identity(&root.join("maintenance").join(SNAPSHOT)).unwrap(),
                install.current_snapshot
            );
            assert_eq!(
                identity(&stage.join("source").join(SNAPSHOT)).unwrap(),
                install.source_snapshot
            );
            assert_eq!(
                identity(&stage.join("source").join(LOG)).unwrap(),
                install.source_log
            );
            // Read the archived original plan bytes, never mutate or recast it.
            let decoded: Install = serde_json::from_slice(&plan).unwrap();
            assert_eq!(decoded.current_head, 17);
            complete(&root);
            let view = LocalDfsRepairQueueStore::new(root.join("maintenance").join(SNAPSHOT))
                .view()
                .unwrap();
            assert_eq!(view.records.len(), 1);
            assert_eq!(
                view.records[0].transfer_request_id.as_deref(),
                Some("actual-request")
            );
            assert!(view.records[0].transfer_delivery_state.is_none());
            assert!(view.records[0].completed_transfer_request_id.is_none());
            assert!(view.records[0].transfer_kind.is_none());
            assert!(
                root.join("migration-retained/local-state/repair-v8/ready")
                    .exists()
            );
        }
    }
    #[test]
    fn original_v7_install_rejects_bad_schema_or_floor_before_any_rename() {
        for current in [
            json!({"records":[record()],"applied_through_sequence":18}),
            json!({"records":[record()],"applied_through_sequence":17,"unexpected":true}),
            json!({"records":"invalid","applied_through_sequence":17}),
        ] {
            let root = sinomo_testdir::TestDir::new("migration-repair-original-reject-before-cut");
            let install = original_install(&root, 0, Some(current), 17);
            let stage = root.join("migration-retained/local-state/repair-v7");
            let plan = fs::read(stage.join("installation.json")).unwrap();
            assert!(finish_original_repair_conversion(&root).is_err());
            assert_eq!(
                identity(&root.join("maintenance").join(SNAPSHOT)).unwrap(),
                install.source_snapshot
            );
            assert_eq!(
                identity(&root.join("maintenance").join(LOG)).unwrap(),
                install.source_log
            );
            assert!(!stage.join("source").join(SNAPSHOT).exists());
            assert!(!stage.join("source").join(LOG).exists());
            assert_eq!(fs::read(stage.join("installation.json")).unwrap(), plan);
        }
    }
    #[test]
    fn original_v7_empty_target_preserves_its_saved_absence_contract() {
        for cut in 0..=2 {
            let root = sinomo_testdir::TestDir::new("migration-repair-original-empty-cut");
            let install = original_install(&root, cut, None, 0);
            finish_original_repair_conversion(&root).unwrap();
            assert!(!root.join("maintenance").join(SNAPSHOT).exists());
            assert!(!root.join("maintenance").join(LOG).exists());
            assert_eq!(
                identity(
                    &root
                        .join("migration-retained/local-state/repair-v7/source")
                        .join(SNAPSHOT)
                )
                .unwrap(),
                install.source_snapshot
            );
            assert!(Conversion::open(&root).unwrap().is_none());
            assert!(
                root.join("migration-retained/local-state/repair-v8/ready")
                    .exists()
            );
        }
        let root = sinomo_testdir::TestDir::new("migration-repair-original-empty-nonzero");
        let install = original_install(&root, 0, None, 4);
        assert!(finish_original_repair_conversion(&root).is_err());
        assert_eq!(
            identity(&root.join("maintenance").join(SNAPSHOT)).unwrap(),
            install.source_snapshot
        );
    }
    #[test]
    fn strict_current_install_rejects_old_missing_facts_before_rename() {
        let root = sinomo_testdir::TestDir::new("migration-repair-current-strict-install");
        let _ = original_install(
            &root,
            0,
            Some(json!({"records":[record()],"applied_through_sequence":17})),
            17,
        );
        let stage = root.join("migration-retained/local-state/repair-v7");
        let install: Install =
            serde_json::from_slice(&fs::read(stage.join("installation.json")).unwrap()).unwrap();
        assert!(
            finish_install_version(&root.join("maintenance"), &stage, &install, false).is_err()
        );
        assert_eq!(
            identity(&root.join("maintenance").join(SNAPSHOT)).unwrap(),
            install.source_snapshot
        );
        assert!(stage.join("installation.json").exists());
    }

    #[test]
    fn original_v7_cursor_requires_unchanged_source_before_independent_v8_conversion() {
        for changed in [false, true] {
            let root = sinomo_testdir::TestDir::new("migration-repair-original-cursor-cut");
            source(&root, json!({"records":[record()]}), &[], false);
            let directory = root.join("maintenance");
            let original = root.join("migration-retained/local-state/repair-v7");
            fs::create_dir_all(&original).unwrap();
            let progress = Progress {
                snapshot: identity(&directory.join(SNAPSHOT)).unwrap(),
                log: identity(&directory.join(LOG)).unwrap(),
                pending: None,
                position: Position {
                    cursor: Some(
                        Cursor::open_sequence(&directory.join(SNAPSHOT), "records").unwrap(),
                    ),
                    snapshot_done: false,
                    records_seen: false,
                    floor: None,
                    log_offset: 0,
                    log_sequence: None,
                    source_head: 0,
                    stage_head: 0,
                    stage_log_offset: 0,
                },
            };
            save(&original.join("progress.json"), &progress).unwrap();
            let before = fs::read(original.join("progress.json")).unwrap();
            if changed {
                fs::write(directory.join(LOG), b"changed original source").unwrap();
                assert!(Conversion::open(&root).is_err());
                assert!(
                    !root
                        .join("migration-retained/local-state/repair-v8/progress.json")
                        .exists()
                );
            } else {
                complete(&root);
            }
            assert_eq!(fs::read(original.join("progress.json")).unwrap(), before);
        }
    }
}
