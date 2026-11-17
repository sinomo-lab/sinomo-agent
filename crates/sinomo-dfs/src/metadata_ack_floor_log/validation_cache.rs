//! Reuse decoded DATA only after rechecking the currently locked physical bytes.
//! This never certifies durability; every put still performs its normal barriers.
use super::*;
use std::any::Any;
use std::cell::RefCell;
use std::io::{Read, Seek};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileGeneration {
    device: u64,
    inode: u64,
    len: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl FileGeneration {
    fn from(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            len: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Source {
    path: PathBuf,
    shard: DfsMetadataShardId,
    kind: &'static str,
    parents: Vec<(u64, u64)>,
    lock: FileGeneration,
    pointer: FileGeneration,
    pointer_hash: blake3::Hash,
    log: FileGeneration,
}

pub(super) struct Validated {
    source: Source,
    bytes: blake3::Hasher,
    loaded: Box<dyn Any>,
}
thread_local! {
    // One last physical source per existing floor kind. No root-indexed cache
    // or retained descriptor/lock can accumulate across owners or requests.
    static LAST: RefCell<[Option<Validated>; 2]> = const { RefCell::new([None, None]) };
}
fn slot<T: AckFloor>() -> Option<usize> {
    match T::KIND {
        "authority" => Some(0),
        "prefix" => Some(1),
        _ => None,
    }
}

pub(super) fn source<T: AckFloor>(
    path: &Path,
    shard: &DfsMetadataShardId,
    held_lock: Option<&File>,
    log: &File,
    pointer_bytes: Option<&[u8]>,
) -> Result<Option<Source>> {
    let (Some(held_lock), Some(pointer_bytes)) = (held_lock, pointer_bytes) else {
        return Ok(None);
    };
    let stat = |path: &Path| fs::metadata(path).map_err(|error| io_error(path, error));
    let opened = |file: &File, path: &Path| file.metadata().map_err(|error| io_error(path, error));
    let lock_path = path.with_extension("lock");
    let log_path = path.with_extension("slog");
    let lock = FileGeneration::from(&opened(held_lock, &lock_path)?);
    let log = FileGeneration::from(&opened(log, &log_path)?);
    if lock != FileGeneration::from(&stat(&lock_path)?)
        || log != FileGeneration::from(&stat(&log_path)?)
    {
        return Ok(None);
    }
    let pointer = FileGeneration::from(&stat(path)?);
    let mut parents = Vec::new();
    // kind directory, coordination directory, physical metadata root.
    for parent in path.ancestors().skip(1).take(3) {
        let metadata = stat(parent)?;
        parents.push((metadata.dev(), metadata.ino()));
    }
    Ok(Some(Source {
        path: fs::canonicalize(path).map_err(|error| io_error(path, error))?,
        shard: shard.clone(),
        kind: T::KIND,
        parents,
        lock,
        pointer,
        pointer_hash: blake3::hash(pointer_bytes),
        log,
    }))
}

pub(super) fn reuse<T: AckFloor>(source: &Source, log: &mut File) -> Result<Option<Loaded<T>>> {
    let Some(index) = slot::<T>() else {
        return Ok(None);
    };
    let previous = LAST.with(|last| {
        let last = last.borrow();
        let entry = last[index]
            .as_ref()
            .filter(|entry| entry.source == *source)?;
        Some((
            entry.bytes.finalize(),
            entry.loaded.downcast_ref::<Loaded<T>>()?.clone(),
        ))
    });
    let Some((expected, loaded)) = previous else {
        return Ok(None);
    };
    // Identity/length/time are insufficient: equal-length edits with restored
    // timestamps must still be decoded and rejected when corrupt. Read the SAME
    // currently opened, floor-locked file; include this observation I/O in cost.
    let span = tracing::trace_span!(target: "sinomo.perf", "dfs.metadata.ack_floor.compare",
        stage = "dfs.metadata.ack_floor.compare", kind = T::KIND,
        io_kind = "disk.read", io_bytes = 0u64, matched = tracing::field::Empty, outcome = "error");
    let _entered = span.enter();
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut remaining = source.log.len;
    let mut read_bytes = 0u64;
    while remaining != 0 {
        let count = (remaining as usize).min(buffer.len());
        let read = log
            .read(&mut buffer[..count])
            .map_err(|error| io_error(&source.path.with_extension("slog"), error))?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        remaining -= read as u64;
        read_bytes += read as u64;
        span.record("io_bytes", read_bytes);
    }
    let matches = remaining == 0 && hash.finalize() == expected;
    log.rewind()
        .map_err(|error| io_error(&source.path.with_extension("slog"), error))?;
    span.record("matched", matches);
    span.record("outcome", "ok");
    Ok(matches.then_some(loaded))
}

pub(super) fn remember<T: AckFloor>(
    before: Option<Source>,
    after: Option<Source>,
    bytes: blake3::Hasher,
    loaded: &Loaded<T>,
) {
    let Some(index) = slot::<T>() else { return };
    let Some(source) = before.filter(|before| Some(before) == after.as_ref()) else {
        return;
    };
    if !loaded.initialized || loaded.floor.is_none() || loaded.complete_len != loaded.file_len {
        return;
    }
    LAST.with(|last| {
        last.borrow_mut()[index] = Some(Validated {
            source,
            bytes,
            loaded: Box::new(loaded.clone()),
        })
    });
}

/// Remove validation before any persistence can fail. The returned DATA is
/// private to this put and cannot be reused until all its barriers succeeded.
pub(super) fn take_for_append<T: AckFloor>() -> Option<Validated> {
    let index = slot::<T>()?;
    LAST.with(|last| last.borrow_mut()[index].take())
}

pub(super) fn prepare_append<T: AckFloor>(
    previous: Option<Validated>,
    path: &Path,
    held_lock: Option<&File>,
    log: &File,
    loaded: &Loaded<T>,
    floor: &T,
    record: Option<&[u8]>,
) -> Option<Validated> {
    let mut previous = previous?;
    if !loaded.initialized
        || loaded.complete_len != loaded.file_len
        || previous.loaded.downcast_ref::<Loaded<T>>()? != loaded
    {
        return None;
    }
    let pointer = read_optional(path).ok()?;
    let current = source::<T>(path, floor.shard(), held_lock, log, pointer.as_deref()).ok()??;
    if current != previous.source {
        return None;
    }
    let mut updated = loaded.clone();
    if let Some(record) = record {
        let length = (record.len() as u32).to_le_bytes();
        previous.bytes.update(&length);
        previous.bytes.update(record);
        updated.records += 1;
        updated.complete_len += length.len() as u64 + record.len() as u64;
        updated.file_len = updated.complete_len;
    }
    updated.floor = Some(floor.clone());
    previous.loaded = Box::new(updated);
    Some(previous)
}

/// Seed the existing bounded cache only for a completed append to the same
/// locked file. Cache observation failure leaves it empty; it cannot change a
/// completed persistence outcome. The next use still hashes the actual bytes.
pub(super) fn remember_appended<T: AckFloor>(
    path: &Path,
    held_lock: Option<&File>,
    log: &File,
    validated: Validated,
) {
    let reseed = || -> Option<()> {
        let loaded = validated.loaded.downcast_ref::<Loaded<T>>()?;
        let before = &validated.source;
        let pointer = read_optional(path).ok()?;
        let after = source::<T>(path, &before.shard, held_lock, log, pointer.as_deref()).ok()??;
        if before.path != after.path
            || before.shard != after.shard
            || before.kind != after.kind
            || before.parents != after.parents
            || before.lock != after.lock
            || before.pointer != after.pointer
            || before.pointer_hash != after.pointer_hash
            || before.log.device != after.log.device
            || before.log.inode != after.log.inode
            || after.log.len != loaded.complete_len
        {
            return None;
        }
        remember::<T>(
            Some(after.clone()),
            Some(after),
            validated.bytes.clone(),
            loaded,
        );
        Some(())
    };
    let _ = reseed();
}
