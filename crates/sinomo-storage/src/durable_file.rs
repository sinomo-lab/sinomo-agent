use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use thiserror::Error;

/// Default object-file reads are bounded by address space and fallible
/// allocation, not by a fixed byte ceiling. Callers with a protocol-level cap can
/// still pass a smaller `max_read_bytes` explicitly.
pub const DEFAULT_OBJECT_FILE_MAX_READ_BYTES: u64 = u64::MAX;
pub const DEFAULT_OBJECT_FILE_CACHE_CAPACITY: usize = 4096;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DurableFileError {
    #[error("storage I/O error at {path}: {detail}")]
    Io {
        path: String,
        kind: io::ErrorKind,
        detail: String,
    },
    #[error(
        "storage length-prefixed record at {path} is {record_bytes} bytes; max is {max_record_bytes}"
    )]
    RecordTooLarge {
        path: String,
        record_bytes: usize,
        max_record_bytes: usize,
        detail: String,
    },
    #[error(
        "storage resource exhausted at {path} during {operation}: requested {requested_bytes} bytes"
    )]
    ResourceExhausted {
        path: String,
        operation: String,
        requested_bytes: usize,
    },
}

impl DurableFileError {
    pub fn path(&self) -> &str {
        match self {
            DurableFileError::Io { path, .. }
            | DurableFileError::RecordTooLarge { path, .. }
            | DurableFileError::ResourceExhausted { path, .. } => path,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            DurableFileError::Io { detail, .. }
            | DurableFileError::RecordTooLarge { detail, .. } => detail,
            DurableFileError::ResourceExhausted { operation, .. } => operation,
        }
    }

    pub fn kind(&self) -> io::ErrorKind {
        match self {
            DurableFileError::Io { kind, .. } => *kind,
            DurableFileError::RecordTooLarge { .. } => io::ErrorKind::InvalidData,
            DurableFileError::ResourceExhausted { .. } => io::ErrorKind::OutOfMemory,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableObjectFileWriteResult {
    pub changed: bool,
    pub compare_read_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableObjectFileOptions<'a> {
    pub max_read_bytes: u64,
    pub fallback_file_name: &'a str,
    pub temp_label: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableFileSync {
    Data,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableReadChunk {
    Complete,
    CleanEof,
    Truncated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableLengthEndian {
    Big,
    Little,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRecordReadOptions {
    pub length_endian: DurableLengthEndian,
    pub max_record_bytes: Option<usize>,
}

impl DurableRecordReadOptions {
    pub const fn new(length_endian: DurableLengthEndian) -> Self {
        Self {
            length_endian,
            max_record_bytes: None,
        }
    }

    pub const fn max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = Some(max_record_bytes);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRecordAppendOptions {
    pub length_endian: DurableLengthEndian,
    pub sync: Option<DurableFileSync>,
}

impl DurableRecordAppendOptions {
    pub const fn new(length_endian: DurableLengthEndian) -> Self {
        Self {
            length_endian,
            sync: None,
        }
    }

    pub const fn sync(mut self, sync: DurableFileSync) -> Self {
        self.sync = Some(sync);
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DurableRecord {
    pub bytes: Vec<u8>,
    pub record_start: u64,
    pub record_end: u64,
    pub file_len: u64,
}

#[derive(Debug)]
pub struct DurableAppendFile {
    pub file: File,
    pub created: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableAppendFileOptions {
    pub sync_parent_on_create: bool,
}

impl DurableAppendFileOptions {
    pub const fn new() -> Self {
        Self {
            sync_parent_on_create: true,
        }
    }

    pub const fn without_parent_sync(mut self) -> Self {
        self.sync_parent_on_create = false;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableAtomicFileOptions<'a> {
    pub fallback_file_name: &'a str,
    pub temp_label: &'a str,
    pub sync: DurableFileSync,
}

impl<'a> DurableAtomicFileOptions<'a> {
    pub const fn new(fallback_file_name: &'a str, temp_label: &'a str) -> Self {
        Self {
            fallback_file_name,
            temp_label,
            sync: DurableFileSync::Data,
        }
    }

    pub const fn sync_all(mut self) -> Self {
        self.sync = DurableFileSync::All;
        self
    }
}

impl<'a> DurableObjectFileOptions<'a> {
    pub const fn new(fallback_file_name: &'a str, temp_label: &'a str) -> Self {
        Self {
            max_read_bytes: DEFAULT_OBJECT_FILE_MAX_READ_BYTES,
            fallback_file_name,
            temp_label,
        }
    }
}

pub fn read_full_chunk<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<DurableReadChunk, DurableFileError> {
    read_full_chunk_with_observer("stream", reader, buf, |_| {})
}

fn durable_allocation_error(
    path: impl std::fmt::Display,
    operation: &str,
    requested_bytes: usize,
) -> DurableFileError {
    DurableFileError::ResourceExhausted {
        path: path.to_string(),
        operation: operation.to_string(),
        requested_bytes,
    }
}

fn try_reserve_vec<T>(
    path: impl std::fmt::Display,
    vec: &mut Vec<T>,
    additional: usize,
    operation: &str,
) -> Result<(), DurableFileError> {
    let requested_items =
        vec.len()
            .checked_add(additional)
            .ok_or_else(|| DurableFileError::Io {
                path: path.to_string(),
                kind: io::ErrorKind::InvalidData,
                detail: "allocation size overflow".to_string(),
            })?;
    let requested_bytes = requested_items
        .checked_mul(std::mem::size_of::<T>().max(1))
        .ok_or_else(|| DurableFileError::Io {
            path: path.to_string(),
            kind: io::ErrorKind::InvalidData,
            detail: "allocation size overflow".to_string(),
        })?;
    vec.try_reserve(additional)
        .map_err(|_| durable_allocation_error(path, operation, requested_bytes))
}

fn try_zeroed_vec(
    path: impl std::fmt::Display,
    len: usize,
    operation: &str,
) -> Result<Vec<u8>, DurableFileError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| durable_allocation_error(path, operation, len))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

pub fn read_full_chunk_with_observer<R, F>(
    path: impl std::fmt::Display,
    reader: &mut R,
    buf: &mut [u8],
    mut on_read: F,
) -> Result<DurableReadChunk, DurableFileError>
where
    R: Read,
    F: FnMut(u64),
{
    let mut offset = 0;
    while offset < buf.len() {
        match reader.read(&mut buf[offset..]) {
            Ok(0) if offset == 0 => return Ok(DurableReadChunk::CleanEof),
            Ok(0) => return Ok(DurableReadChunk::Truncated),
            Ok(read) => {
                offset += read;
                on_read(read as u64);
            }
            Err(source) => return Err(io_error(&path, source)),
        }
    }
    Ok(DurableReadChunk::Complete)
}

pub fn read_length_prefixed_record(
    file: &mut File,
    path: &Path,
    options: DurableRecordReadOptions,
) -> Result<Option<DurableRecord>, DurableFileError> {
    read_length_prefixed_record_with_observer(file, path, options, |_| {})
}

pub fn read_length_prefixed_record_with_observer<F>(
    file: &mut File,
    path: &Path,
    options: DurableRecordReadOptions,
    mut on_read: F,
) -> Result<Option<DurableRecord>, DurableFileError>
where
    F: FnMut(u64),
{
    let record_start = file
        .stream_position()
        .map_err(|source| io_error(path.display(), source))?;
    let file_len = file
        .metadata()
        .map_err(|source| io_error(path.display(), source))?
        .len();
    let mut length_bytes = [0_u8; 4];
    match read_full_chunk_with_observer(path.display(), file, &mut length_bytes, &mut on_read)? {
        DurableReadChunk::Complete => {}
        DurableReadChunk::CleanEof | DurableReadChunk::Truncated => {
            seek_to_record_start(file, path, record_start)?;
            return Ok(None);
        }
    }
    let length_u32 = match options.length_endian {
        DurableLengthEndian::Big => u32::from_be_bytes(length_bytes),
        DurableLengthEndian::Little => u32::from_le_bytes(length_bytes),
    };
    let length = length_u32 as usize;
    if let Some(max_record_bytes) = options.max_record_bytes
        && length > max_record_bytes
    {
        return Err(DurableFileError::RecordTooLarge {
            path: path.display().to_string(),
            record_bytes: length,
            max_record_bytes,
            detail: format!("length-prefixed record is {length} bytes; max is {max_record_bytes}"),
        });
    }
    let record_end = record_start
        .saturating_add(4)
        .saturating_add(u64::from(length_u32));
    if record_end > file_len {
        seek_to_record_start(file, path, record_start)?;
        return Ok(None);
    }
    let mut bytes = try_zeroed_vec(path.display(), length, "read length-prefixed record")?;
    match read_full_chunk_with_observer(path.display(), file, &mut bytes, &mut on_read)? {
        DurableReadChunk::Complete => {}
        DurableReadChunk::CleanEof | DurableReadChunk::Truncated => {
            seek_to_record_start(file, path, record_start)?;
            return Ok(None);
        }
    }
    Ok(Some(DurableRecord {
        bytes,
        record_start,
        record_end,
        file_len,
    }))
}

pub fn append_length_prefixed_records<I, B>(
    file: &mut File,
    path: &Path,
    records: I,
    options: DurableRecordAppendOptions,
) -> Result<u64, DurableFileError>
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let mut written = 0_u64;
    for record in records {
        let record = record.as_ref();
        let len = u32::try_from(record.len()).map_err(|_| DurableFileError::Io {
            path: path.display().to_string(),
            kind: io::ErrorKind::InvalidData,
            detail: format!(
                "length-prefixed record exceeds u32 length: {}",
                record.len()
            ),
        })?;
        let length_bytes = match options.length_endian {
            DurableLengthEndian::Big => len.to_be_bytes(),
            DurableLengthEndian::Little => len.to_le_bytes(),
        };
        file.write_all(&length_bytes)
            .and_then(|()| file.write_all(record))
            .map_err(|source| io_error(path.display(), source))?;
        written = written.saturating_add(4).saturating_add(u64::from(len));
    }
    if let Some(sync) = options.sync {
        sync_file(file, path, sync)?;
    }
    Ok(written)
}

pub fn open_file_for_append(path: &Path) -> Result<DurableAppendFile, DurableFileError> {
    open_file_for_append_with_options(path, DurableAppendFileOptions::new())
}

pub fn open_file_for_append_with_options(
    path: &Path,
    options: DurableAppendFileOptions,
) -> Result<DurableAppendFile, DurableFileError> {
    create_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    match OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(path)
    {
        Ok(file) => {
            if options.sync_parent_on_create {
                sync_directory(parent)?;
            }
            Ok(DurableAppendFile {
                file,
                created: true,
            })
        }
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map(|file| DurableAppendFile {
                file,
                created: false,
            })
            .map_err(|source| io_error(path.display(), source)),
        Err(source) => Err(io_error(path.display(), source)),
    }
}

pub fn truncate_open_file_and_sync(
    file: &mut File,
    path: &Path,
    offset: u64,
    sync: DurableFileSync,
) -> Result<(), DurableFileError> {
    file.set_len(offset)
        .and_then(|()| file.seek(SeekFrom::Start(offset)).map(|_| ()))
        .map_err(|source| io_error(path.display(), source))?;
    sync_file(file, path, sync)
}

pub fn truncate_file_and_sync(
    path: &Path,
    offset: u64,
    sync: DurableFileSync,
) -> Result<bool, DurableFileError> {
    let len = fs::metadata(path)
        .map_err(|source| io_error(path.display(), source))?
        .len();
    if offset >= len {
        return Ok(false);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| io_error(path.display(), source))?;
    truncate_open_file_and_sync(&mut file, path, offset, sync)?;
    Ok(true)
}

pub fn remove_file_if_exists_and_sync_parent(path: &Path) -> Result<bool, DurableFileError> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
            Ok(true)
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_error(path.display(), source)),
    }
}

pub fn remove_empty_dir_if_exists_and_sync_parent(path: &Path) -> Result<bool, DurableFileError> {
    match fs::remove_dir(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
            Ok(true)
        }
        Err(source)
            if source.kind() == io::ErrorKind::NotFound
                || source.kind() == io::ErrorKind::DirectoryNotEmpty =>
        {
            Ok(false)
        }
        Err(source) => Err(io_error(path.display(), source)),
    }
}

pub fn file_blake3_hash_and_size(path: &Path) -> Result<(blake3::Hash, u64), DurableFileError> {
    let mut file = File::open(path).map_err(|source| io_error(path.display(), source))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut size_bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| io_error(path.display(), source))?;
        if read == 0 {
            break;
        }
        size_bytes = size_bytes
            .checked_add(read as u64)
            .ok_or_else(|| DurableFileError::Io {
                path: path.display().to_string(),
                kind: io::ErrorKind::InvalidData,
                detail: "content size overflow".to_string(),
            })?;
        hasher.update(&buffer[..read]);
    }
    Ok((hasher.finalize(), size_bytes))
}

pub fn write_file_create_new_atomic(
    path: &Path,
    content: &[u8],
    options: DurableAtomicFileOptions<'_>,
) -> Result<(), DurableFileError> {
    create_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temp_path(parent, path, options.fallback_file_name, options.temp_label);
    let write_temp_result = write_temp_file(&temp_path, content, false, options.sync);
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    if let Err(source) = fs::hard_link(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(io_error(path.display(), source));
    }
    fs::remove_file(&temp_path).map_err(|source| io_error(temp_path.display(), source))?;
    sync_directory(parent)
}

pub fn write_files_create_new_relaxed<I, P, B>(
    files: I,
    options: DurableAtomicFileOptions<'_>,
) -> Result<(), DurableFileError>
where
    I: IntoIterator<Item = (P, B)>,
    P: AsRef<Path>,
    B: AsRef<[u8]>,
{
    for (path, content) in files {
        let path = path.as_ref();
        let content = content.as_ref();
        if path.is_file() {
            continue;
        }
        create_parent_dir(path)?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let temp_path = temp_path(parent, path, options.fallback_file_name, options.temp_label);
        let write_temp_result = write_temp_file_without_sync(&temp_path, content, false);
        if let Err(error) = write_temp_result {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
        match fs::hard_link(&temp_path, path) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                let _ = fs::remove_file(&temp_path);
                return Err(io_error(path.display(), source));
            }
        }
        fs::remove_file(&temp_path).map_err(|source| io_error(temp_path.display(), source))?;
    }
    Ok(())
}

#[derive(Clone)]
struct ObjectFileWriteMark {
    hash: blake3::Hash,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
    last_used: u64,
}

pub struct ObjectFileWriteMarkCache {
    capacity: usize,
    clock: AtomicU64,
    entries: Mutex<HashMap<PathBuf, ObjectFileWriteMark>>,
}

impl ObjectFileWriteMarkCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            clock: AtomicU64::new(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn contains(&self, path: &Path) -> bool {
        self.entries
            .lock()
            .map(|entries| entries.contains_key(path))
            .unwrap_or(false)
    }

    pub fn refresh(&self, path: &Path) -> bool {
        self.cached_mark(path).is_some()
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }

    pub fn reset_clock(&self) {
        self.clock.store(1, Ordering::Relaxed);
    }

    pub fn retain_not_under_root(&self, root: &Path) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|path, _| !path.starts_with(root));
        }
    }

    pub fn record_mark(
        &self,
        path: &Path,
        hash: blake3::Hash,
        len: u64,
        modified: Option<SystemTime>,
        ino: u64,
    ) {
        if let Ok(mut entries) = self.entries.lock() {
            self.evict_if_needed(&mut entries, path);
            entries.insert(
                path.to_path_buf(),
                ObjectFileWriteMark {
                    hash,
                    len,
                    modified,
                    ino,
                    last_used: self.tick(),
                },
            );
        }
    }

    fn record_current_file(&self, path: &Path, hash: blake3::Hash, len: u64) {
        let (modified, ino) = file_modified_and_ino(path);
        self.record_mark(path, hash, len, modified, ino);
    }

    fn matches_current_file(&self, path: &Path, hash: blake3::Hash, len: u64) -> bool {
        let Some(mark) = self.cached_mark(path) else {
            return false;
        };
        if mark.hash != hash {
            return false;
        }
        fs::metadata(path).is_ok_and(|metadata| {
            mark.len == len
                && metadata.len() == mark.len
                && metadata.modified().ok() == mark.modified
                && file_ino(&metadata) == mark.ino
        })
    }

    fn cached_mark(&self, path: &Path) -> Option<ObjectFileWriteMark> {
        let mut entries = self.entries.lock().ok()?;
        let mark = entries.get_mut(path)?;
        mark.last_used = self.tick();
        Some(mark.clone())
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn evict_if_needed(
        &self,
        entries: &mut HashMap<PathBuf, ObjectFileWriteMark>,
        protected: &Path,
    ) {
        if entries.contains_key(protected) || entries.len() < self.capacity {
            return;
        }
        let eviction_candidate = entries
            .iter()
            .filter(|(path, _)| path.as_path() != protected)
            .min_by_key(|(_, mark)| mark.last_used)
            .map(|(path, _)| path.clone());
        if let Some(path) = eviction_candidate {
            entries.remove(&path);
        }
    }
}

pub fn file_ino(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        std::os::unix::fs::MetadataExt::ino(metadata)
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

pub fn read_optional_object_file_bytes(
    path: &Path,
    max_read_bytes: u64,
) -> Result<Option<Vec<u8>>, DurableFileError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error(path.display(), source)),
    };
    read_object_file_bytes_from_open_file(path, file, max_read_bytes).map(Some)
}

pub fn read_object_file_bytes(
    path: &Path,
    max_read_bytes: u64,
) -> Result<Vec<u8>, DurableFileError> {
    let file = File::open(path).map_err(|source| io_error(path.display(), source))?;
    read_object_file_bytes_from_open_file(path, file, max_read_bytes)
}

pub fn read_object_file_bytes_from_open_file(
    path: &Path,
    file: File,
    max_read_bytes: u64,
) -> Result<Vec<u8>, DurableFileError> {
    let len = file
        .metadata()
        .map_err(|source| io_error(path.display(), source))?
        .len();
    if len > max_read_bytes {
        return Err(DurableFileError::Io {
            path: path.display().to_string(),
            kind: io::ErrorKind::InvalidData,
            detail: format!("object file is {len} bytes; max is {max_read_bytes}"),
        });
    }

    let mut bytes = Vec::new();
    let initial_capacity = usize::try_from(len).map_err(|_| DurableFileError::Io {
        path: path.display().to_string(),
        kind: io::ErrorKind::InvalidData,
        detail: format!("object file length {len} does not fit in usize"),
    })?;
    try_reserve_vec(
        path.display(),
        &mut bytes,
        initial_capacity,
        "read object file",
    )?;
    let mut limited = file.take(max_read_bytes.saturating_add(1));
    let mut chunk = [0_u8; 64 * 1024];
    loop {
        let read = limited
            .read(&mut chunk)
            .map_err(|source| io_error(path.display(), source))?;
        if read == 0 {
            break;
        }
        if (bytes.len() as u64).saturating_add(read as u64) > max_read_bytes {
            return Err(DurableFileError::Io {
                path: path.display().to_string(),
                kind: io::ErrorKind::InvalidData,
                detail: format!("object file read exceeded max of {max_read_bytes} bytes"),
            });
        }
        try_reserve_vec(path.display(), &mut bytes, read, "read object file")?;
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(bytes)
}

pub fn write_object_file_bytes_if_changed(
    path: &Path,
    content: &[u8],
    cache: &ObjectFileWriteMarkCache,
    options: DurableObjectFileOptions<'_>,
) -> Result<DurableObjectFileWriteResult, DurableFileError> {
    let content_hash = blake3::hash(content);
    if cache.matches_current_file(path, content_hash, content.len() as u64) {
        return Ok(DurableObjectFileWriteResult {
            changed: false,
            compare_read_bytes: None,
        });
    }

    let existing = read_existing_object_file_for_compare(path, content.len() as u64);
    let compare_read_bytes = existing.as_ref().map(|bytes| bytes.len() as u64);
    if existing.as_deref() == Some(content) {
        cache.record_current_file(path, content_hash, content.len() as u64);
        return Ok(DurableObjectFileWriteResult {
            changed: false,
            compare_read_bytes,
        });
    }

    create_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temp_path(parent, path, options.fallback_file_name, options.temp_label);
    let write_temp_result = write_temp_file(&temp_path, content, false, DurableFileSync::Data);
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    if let Err(error) = replace_file(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    sync_directory(parent)?;
    cache.record_current_file(path, content_hash, content.len() as u64);
    Ok(DurableObjectFileWriteResult {
        changed: true,
        compare_read_bytes,
    })
}

pub fn write_object_file_bytes_create_new_serialized<F>(
    path: &Path,
    content: &[u8],
    cache: &ObjectFileWriteMarkCache,
    options: DurableObjectFileOptions<'_>,
    create_link: F,
) -> Result<DurableObjectFileWriteResult, DurableFileError>
where
    F: FnOnce(&Path, &Path) -> io::Result<()>,
{
    create_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temp_path(parent, path, options.fallback_file_name, options.temp_label);
    let write_temp_result = write_temp_file(&temp_path, content, true, DurableFileSync::Data);
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }

    match create_link(&temp_path, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temp_path);
        }
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temp_path);
            return Ok(DurableObjectFileWriteResult {
                changed: false,
                compare_read_bytes: None,
            });
        }
        Err(source) if hard_link_unsupported(&source) => {
            match fs::metadata(path) {
                Ok(_) => {
                    let _ = fs::remove_file(&temp_path);
                    return Ok(DurableObjectFileWriteResult {
                        changed: false,
                        compare_read_bytes: None,
                    });
                }
                Err(metadata_source) if metadata_source.kind() == io::ErrorKind::NotFound => {}
                Err(metadata_source) => {
                    let _ = fs::remove_file(&temp_path);
                    return Err(io_error(path.display(), metadata_source));
                }
            }
            if let Err(rename_source) = fs::rename(&temp_path, path) {
                let existed = path.exists();
                let _ = fs::remove_file(&temp_path);
                if existed
                    && matches!(
                        rename_source.kind(),
                        io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied
                    )
                {
                    return Ok(DurableObjectFileWriteResult {
                        changed: false,
                        compare_read_bytes: None,
                    });
                }
                return Err(io_error(
                    format!("{} -> {}", temp_path.display(), path.display()),
                    rename_source,
                ));
            }
        }
        Err(source) => {
            let _ = fs::remove_file(&temp_path);
            return Err(io_error(
                format!("{} -> {}", temp_path.display(), path.display()),
                source,
            ));
        }
    }

    sync_directory(parent)?;
    cache.record_current_file(path, blake3::hash(content), content.len() as u64);
    Ok(DurableObjectFileWriteResult {
        changed: true,
        compare_read_bytes: None,
    })
}

pub fn write_file_atomic(
    path: &Path,
    content: &[u8],
    options: DurableAtomicFileOptions<'_>,
) -> Result<(), DurableFileError> {
    create_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temp_path(parent, path, options.fallback_file_name, options.temp_label);
    let write_temp_result = write_temp_file(&temp_path, content, false, options.sync);
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    if let Err(error) = replace_file(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    sync_directory(parent)
}

/// Creates a new staging file that is accessible only to its owner (and, on
/// Windows, the local system account).
///
/// Callers should use this helper before streaming sensitive content into a
/// staged transfer. `install_staged_file` reapplies the final permissions before
/// publication, but it cannot retroactively protect a broadly accessible file
/// while that file is being populated.
pub fn create_private_staged_file(path: &Path) -> Result<File, DurableFileError> {
    create_private_staged_file_platform(path).map_err(|source| io_error(path.display(), source))
}

/// Atomically creates one new private directory whose parent already exists.
///
/// Unlike `ensure_private_directory`, this function never accepts an existing
/// directory. It is suitable for unique per-session directories where reuse
/// would cross a security boundary.
pub fn create_private_directory(path: &Path) -> Result<(), DurableFileError> {
    let parent = validate_private_directory_parent(path)?;
    create_private_directory_platform(path).map_err(|source| io_error(path.display(), source))?;
    set_private_directory_permissions(path).map_err(|source| io_error(path.display(), source))?;
    sync_directory(path)?;
    sync_directory(parent)
}

/// Creates or tightens one private directory whose parent already exists.
///
/// Creation applies owner-only permissions atomically: mode `0700` on Unix and
/// a protected owner/LocalSystem DACL on Windows. An existing real directory is
/// tightened to the same policy. Symlinks and other non-directory entries are
/// rejected. The helper deliberately does not recurse, so it cannot change the
/// permissions of shared ancestors.
pub fn ensure_private_directory(path: &Path) -> Result<(), DurableFileError> {
    validate_private_directory_parent(path)?;

    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_directory_metadata(path, &metadata)?,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            match create_private_directory(path) {
                Ok(()) => return Ok(()),
                Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(path)
                        .map_err(|source| io_error(path.display(), source))?;
                    validate_private_directory_metadata(path, &metadata)?;
                }
                Err(source) => return Err(source),
            }
        }
        Err(source) => return Err(io_error(path.display(), source)),
    }

    set_private_directory_permissions(path).map_err(|source| io_error(path.display(), source))?;
    sync_directory(path)
}

fn validate_private_directory_parent(path: &Path) -> Result<&Path, DurableFileError> {
    if path.file_name().is_none() {
        return Err(DurableFileError::Io {
            path: path.display().to_string(),
            kind: io::ErrorKind::InvalidInput,
            detail: "private directory path does not have a directory name".to_string(),
        });
    }
    let parent = install_parent(path);
    match fs::symlink_metadata(parent) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(DurableFileError::Io {
                path: parent.display().to_string(),
                kind: io::ErrorKind::InvalidInput,
                detail: "private directory parent is not a directory".to_string(),
            });
        }
        Err(source) => return Err(io_error(parent.display(), source)),
    }
    Ok(parent)
}

fn validate_private_directory_metadata(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), DurableFileError> {
    if metadata.file_type().is_dir() {
        return Ok(());
    }
    Err(DurableFileError::Io {
        path: path.display().to_string(),
        kind: io::ErrorKind::InvalidInput,
        detail: "private directory path is not a real directory".to_string(),
    })
}

#[cfg(unix)]
fn create_private_directory_platform(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path)
}

#[cfg(windows)]
fn create_private_directory_platform(path: &Path) -> io::Result<()> {
    windows_file::create_private_directory(path)
}

#[cfg(not(any(unix, windows)))]
fn create_private_directory_platform(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private directories are unsupported on this platform",
    ))
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(windows)]
fn set_private_directory_permissions(path: &Path) -> io::Result<()> {
    windows_file::set_private_permissions(path)
}

#[cfg(not(any(unix, windows)))]
fn set_private_directory_permissions(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private directories are unsupported on this platform",
    ))
}

#[cfg(unix)]
fn create_private_staged_file_platform(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(windows)]
fn create_private_staged_file_platform(path: &Path) -> io::Result<File> {
    windows_file::create_private_file(path)
}

#[cfg(not(any(unix, windows)))]
fn create_private_staged_file_platform(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

/// Atomically publishes an already-synced staged file from the destination's
/// directory and then makes the destination directory entry durable.
///
/// The staged path and destination must name distinct entries in the same
/// directory. With `overwrite` disabled, destination creation is a hard-link
/// operation, so an existing destination wins atomically without a preflight
/// existence check. With `overwrite` enabled, the platform's atomic replace
/// operation is used; the destination is never deleted as a fallback.
///
/// A failed publish leaves the staged path in place for the caller's cleanup
/// guard. Once the atomic namespace operation succeeds, a subsequent directory
/// sync error can only report an indeterminate durability outcome: the staged
/// name has already been consumed for replacement, or is still linked to the
/// installed file for a no-clobber publish.
pub fn install_staged_file(
    staged_path: &Path,
    destination_path: &Path,
    overwrite: bool,
) -> Result<(), DurableFileError> {
    let parent = validate_staged_install_paths(staged_path, destination_path)?;
    prepare_staged_install_metadata(staged_path, destination_path, overwrite)?;

    if overwrite {
        replace_file(staged_path, destination_path)?;
        return sync_directory(&parent);
    }

    fs::hard_link(staged_path, destination_path).map_err(|source| {
        io_error(
            format!(
                "{} -> {}",
                staged_path.display(),
                destination_path.display()
            ),
            source,
        )
    })?;

    sync_directory(&parent)?;

    // The destination link is durable before the staging name is removed. A
    // crash during this cleanup can therefore only leave a harmless extra link.
    fs::remove_file(staged_path).map_err(|source| io_error(staged_path.display(), source))
}

fn validate_staged_install_paths(
    staged_path: &Path,
    destination_path: &Path,
) -> Result<PathBuf, DurableFileError> {
    let staged_name = staged_path
        .file_name()
        .ok_or_else(|| DurableFileError::Io {
            path: staged_path.display().to_string(),
            kind: io::ErrorKind::InvalidInput,
            detail: "staged file path does not have a file name".to_string(),
        })?;
    let destination_name = destination_path
        .file_name()
        .ok_or_else(|| DurableFileError::Io {
            path: destination_path.display().to_string(),
            kind: io::ErrorKind::InvalidInput,
            detail: "destination file path does not have a file name".to_string(),
        })?;

    let staged_parent = install_parent(staged_path);
    let destination_parent = install_parent(destination_path);
    let staged_parent = fs::canonicalize(staged_parent)
        .map_err(|source| io_error(staged_parent.display(), source))?;
    let destination_parent = fs::canonicalize(destination_parent)
        .map_err(|source| io_error(destination_parent.display(), source))?;
    if staged_parent != destination_parent {
        return Err(DurableFileError::Io {
            path: format!(
                "{} -> {}",
                staged_path.display(),
                destination_path.display()
            ),
            kind: io::ErrorKind::InvalidInput,
            detail: "staged file and destination must be in the same directory".to_string(),
        });
    }
    if staged_name == destination_name {
        return Err(DurableFileError::Io {
            path: staged_path.display().to_string(),
            kind: io::ErrorKind::InvalidInput,
            detail: "staged file and destination must have distinct names".to_string(),
        });
    }

    let metadata = fs::symlink_metadata(staged_path)
        .map_err(|source| io_error(staged_path.display(), source))?;
    if !metadata.file_type().is_file() {
        return Err(DurableFileError::Io {
            path: staged_path.display().to_string(),
            kind: io::ErrorKind::InvalidInput,
            detail: "staged path is not a regular file".to_string(),
        });
    }

    Ok(staged_parent)
}

fn prepare_staged_install_metadata(
    staged_path: &Path,
    destination_path: &Path,
    overwrite: bool,
) -> Result<(), DurableFileError> {
    // Retain a writable handle before adopting destination permissions. This
    // permits durable replacement of read-only files (including mode 000) while
    // still syncing the staged inode after its metadata changes.
    let staged_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(staged_path)
        .map_err(|source| io_error(staged_path.display(), source))?;

    let destination_metadata = if overwrite {
        match fs::symlink_metadata(destination_path) {
            Ok(metadata) if metadata.file_type().is_file() => Some(metadata),
            Ok(_) => {
                return Err(DurableFileError::Io {
                    path: destination_path.display().to_string(),
                    kind: io::ErrorKind::InvalidInput,
                    detail: "destination is not a regular file".to_string(),
                });
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => None,
            Err(source) => return Err(io_error(destination_path.display(), source)),
        }
    } else {
        None
    };

    #[cfg(unix)]
    {
        let permissions = destination_metadata
            .map(|metadata| metadata.permissions())
            .unwrap_or_else(|| fs::Permissions::from_mode(0o600));
        fs::set_permissions(staged_path, permissions)
            .map_err(|source| io_error(staged_path.display(), source))?;
    }

    #[cfg(windows)]
    {
        let _ = destination_metadata;
        windows_file::set_private_permissions(staged_path)
            .map_err(|source| io_error(staged_path.display(), source))?;
    }

    #[cfg(not(any(unix, windows)))]
    let _ = destination_metadata;

    // The content is already synced by contract. Sync all metadata after the
    // final mode/DACL update so the published inode cannot regress to broader
    // staging permissions after a crash.
    staged_file
        .sync_all()
        .map_err(|source| io_error(staged_path.display(), source))
}

fn install_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

pub fn replace_file(temp_path: &Path, path: &Path) -> Result<(), DurableFileError> {
    replace_file_platform(temp_path, path).map_err(|source| {
        io_error(
            format!("{} -> {}", temp_path.display(), path.display()),
            source,
        )
    })
}

#[cfg(not(windows))]
fn replace_file_platform(temp_path: &Path, path: &Path) -> io::Result<()> {
    fs::rename(temp_path, path)
}

#[cfg(windows)]
fn replace_file_platform(temp_path: &Path, path: &Path) -> io::Result<()> {
    windows_file::replace_file(temp_path, path)
}

#[cfg(windows)]
mod windows_file {
    use super::*;
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::FromRawHandle as _;
    use std::ptr;
    use std::sync::{Arc, LazyLock, Weak};
    use std::time::{Duration, Instant};

    const CREATE_NEW: u32 = 1;
    const DACL_SECURITY_INFORMATION: u32 = 0x0000_0004;
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_FILE_EXISTS: i32 = 80;
    const ERROR_FILE_NOT_FOUND: i32 = 2;
    const ERROR_ALREADY_EXISTS: i32 = 183;
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    const ERROR_UNABLE_TO_REMOVE_REPLACED: i32 = 1175;
    const FILE_ATTRIBUTE_NORMAL: u32 = 0x0000_0080;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const GENERIC_READ: u32 = 0x8000_0000;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    const REPLACE_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(2);
    const REPLACE_RETRY_MAX_DELAY: Duration = Duration::from_millis(40);
    const REPLACE_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
    const SDDL_REVISION_1: u32 = 1;
    const WINDOWS_REPLACE_LOCK_CAPACITY: usize = 4096;

    static WINDOWS_REPLACE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    #[repr(C)]
    struct SecurityAttributes {
        length: u32,
        security_descriptor: *mut c_void,
        inherit_handle: i32,
    }

    struct LocalSecurityDescriptor(*mut c_void);

    impl Drop for LocalSecurityDescriptor {
        fn drop(&mut self) {
            // SAFETY: the descriptor was allocated by
            // ConvertStringSecurityDescriptorToSecurityDescriptorW and has not
            // otherwise been freed.
            unsafe {
                LocalFree(self.0);
            }
        }
    }

    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            string_security_descriptor: *const u16,
            string_sd_revision: u32,
            security_descriptor: *mut *mut c_void,
            security_descriptor_size: *mut u32,
        ) -> i32;
        fn SetFileSecurityW(
            file_name: *const u16,
            security_information: u32,
            security_descriptor: *const c_void,
        ) -> i32;
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateFileW(
            file_name: *const u16,
            desired_access: u32,
            share_mode: u32,
            security_attributes: *const SecurityAttributes,
            creation_disposition: u32,
            flags_and_attributes: u32,
            template_file: *mut c_void,
        ) -> *mut c_void;
        fn CreateDirectoryW(
            path_name: *const u16,
            security_attributes: *const SecurityAttributes,
        ) -> i32;
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
        fn ReplaceFileW(
            replaced_file_name: *const u16,
            replacement_file_name: *const u16,
            backup_file_name: *const u16,
            replace_flags: u32,
            exclude: *mut c_void,
            reserved: *mut c_void,
        ) -> i32;
    }

    pub(super) fn create_private_file(path: &Path) -> io::Result<File> {
        let path = wide_path(path)?;
        let descriptor = private_security_descriptor()?;
        let attributes = security_attributes(&descriptor);
        // SAFETY: the path and protected descriptor remain valid throughout
        // the call. Ownership of a successful handle is transferred once.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                &attributes,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                ptr::null_mut(),
            )
        };
        if handle == invalid_handle_value() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateFileW returned one valid owned handle.
        Ok(unsafe { File::from_raw_handle(handle) })
    }

    pub(super) fn create_private_directory(path: &Path) -> io::Result<()> {
        let path = wide_path(path)?;
        let descriptor = private_security_descriptor()?;
        let attributes = security_attributes(&descriptor);
        // SAFETY: path and the protected security descriptor remain valid for
        // the duration of CreateDirectoryW.
        let created = unsafe { CreateDirectoryW(path.as_ptr(), &attributes) };
        if created == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn set_private_permissions(path: &Path) -> io::Result<()> {
        let path = wide_path(path)?;
        let descriptor = private_security_descriptor()?;
        // SAFETY: both pointers remain valid for the duration of the call. The
        // protected DACL grants full access only to the owner and LocalSystem.
        let result =
            unsafe { SetFileSecurityW(path.as_ptr(), DACL_SECURITY_INFORMATION, descriptor.0) };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn replace_file(temp_path: &Path, path: &Path) -> io::Result<()> {
        // ReplaceFileW is not safe against simultaneous calls for one target:
        // competing replacements can partially rename the target and return
        // ERROR_UNABLE_TO_{REMOVE,MOVE}_REPLACEMENT. Serialize same-process
        // calls per path; cross-process storage owners retain their sidecar lock.
        let replace_lock = windows_replace_lock(path);
        let _replace_guard = replace_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp_path_wide = wide_path(temp_path)?;
        let path_wide = wide_path(path)?;
        let started = Instant::now();
        let mut delay = REPLACE_RETRY_INITIAL_DELAY;

        loop {
            match replace_file_once(&temp_path_wide, &path_wide, path) {
                Ok(()) => return Ok(()),
                Err(source) if replace_should_retry(&source) => {
                    let Some(remaining) = REPLACE_RETRY_TIMEOUT.checked_sub(started.elapsed())
                    else {
                        return Err(source);
                    };
                    if remaining.is_zero() {
                        return Err(source);
                    }
                    // Antivirus, indexers, older processes, and simultaneous
                    // ReplaceFileW calls can briefly prevent removal of the old
                    // target. These errors leave both names unchanged, so retry
                    // without weakening the atomic replacement contract.
                    std::thread::sleep(delay.min(remaining));
                    delay = (delay * 2).min(REPLACE_RETRY_MAX_DELAY);
                }
                Err(source) => return Err(source),
            }
        }
    }

    fn windows_replace_lock(path: &Path) -> Arc<Mutex<()>> {
        let mut locks = WINDOWS_REPLACE_LOCKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
            return lock;
        }
        if locks.len() >= WINDOWS_REPLACE_LOCK_CAPACITY {
            locks.retain(|_, lock| lock.strong_count() != 0);
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
        lock
    }

    fn replace_file_once(temp_path_wide: &[u16], path_wide: &[u16], path: &Path) -> io::Result<()> {
        match replace_existing(temp_path_wide, path_wide) {
            Ok(()) => return Ok(()),
            Err(source) if source.raw_os_error() == Some(ERROR_FILE_NOT_FOUND) => {}
            Err(source) => return Err(source),
        }

        // ReplaceFileW requires an existing destination. MoveFileExW without
        // REPLACE_EXISTING is the atomic absent-destination path. If a creator
        // wins the race between these calls, retry ReplaceFileW so that the
        // newly-existing destination's ACL and attributes are retained.
        // SAFETY: both pointers are valid NUL-terminated UTF-16 strings.
        let moved = unsafe {
            MoveFileExW(
                temp_path_wide.as_ptr(),
                path_wide.as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved != 0 {
            return Ok(());
        }
        let source = io::Error::last_os_error();
        let destination_now_exists = fs::symlink_metadata(path).is_ok();
        if matches!(
            source.raw_os_error(),
            Some(ERROR_FILE_EXISTS) | Some(ERROR_ALREADY_EXISTS)
        ) || (source.raw_os_error() == Some(ERROR_ACCESS_DENIED) && destination_now_exists)
        {
            return replace_existing(temp_path_wide, path_wide);
        }
        Err(source)
    }

    fn replace_should_retry(source: &io::Error) -> bool {
        matches!(
            source.raw_os_error(),
            Some(ERROR_SHARING_VIOLATION)
                | Some(ERROR_LOCK_VIOLATION)
                | Some(ERROR_UNABLE_TO_REMOVE_REPLACED)
        )
    }

    fn replace_existing(temp_path: &[u16], path: &[u16]) -> io::Result<()> {
        // ReplaceFileW retains the replaced file's ACL and attributes while
        // atomically installing the replacement content.
        // SAFETY: both slices are NUL-terminated UTF-16 buffers that outlive the
        // call; the optional pointers are intentionally null.
        let replaced = unsafe {
            ReplaceFileW(
                path.as_ptr(),
                temp_path.as_ptr(),
                ptr::null(),
                0,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if replaced == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn private_security_descriptor() -> io::Result<LocalSecurityDescriptor> {
        // Protected DACL: full access for the object owner and LocalSystem only.
        let sddl = "D:P(A;;FA;;;OW)(A;;FA;;;SY)\0"
            .encode_utf16()
            .collect::<Vec<_>>();
        let mut descriptor = ptr::null_mut();
        // SAFETY: sddl is NUL-terminated and descriptor points to writable
        // storage for the LocalAlloc-owned result.
        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        };
        if converted == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(LocalSecurityDescriptor(descriptor))
        }
    }

    fn security_attributes(descriptor: &LocalSecurityDescriptor) -> SecurityAttributes {
        SecurityAttributes {
            length: std::mem::size_of::<SecurityAttributes>() as u32,
            security_descriptor: descriptor.0,
            inherit_handle: 0,
        }
    }

    fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
        let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if wide.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows path contains a NUL code unit",
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    fn invalid_handle_value() -> *mut c_void {
        -1_isize as *mut c_void
    }
}

pub fn sync_directory(path: &Path) -> Result<(), DurableFileError> {
    #[cfg(windows)]
    {
        match open_dir_for_sync(path).and_then(|dir| dir.sync_all()) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::PermissionDenied => Ok(()),
            Err(source) => Err(io_error(path.display(), source)),
        }
    }
    #[cfg(not(windows))]
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|source| io_error(path.display(), source))
}

#[cfg(windows)]
fn open_dir_for_sync(path: &Path) -> io::Result<File> {
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

pub fn hard_link_unsupported(source: &io::Error) -> bool {
    source.kind() == io::ErrorKind::Unsupported
        || matches!(
            source.raw_os_error(),
            Some(
                1     // EPERM on Unix filesystems that disallow hard links.
                    | 18   // EXDEV, kept defensive even though temp and target share a directory.
                    | 38   // ENOSYS
                    | 45   // EOPNOTSUPP on macOS/BSD
                    | 50   // ERROR_NOT_SUPPORTED on Windows
                    | 95   // EOPNOTSUPP on Linux
                    | 1314 // ERROR_PRIVILEGE_NOT_HELD on Windows
            )
        )
}

fn read_existing_object_file_for_compare(path: &Path, expected_len: u64) -> Option<Vec<u8>> {
    let file = File::open(path).ok()?;
    if file.metadata().ok()?.len() != expected_len {
        return None;
    }
    let expected_len_usize = usize::try_from(expected_len).ok()?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(expected_len_usize).ok()?;
    let mut limited = file.take(expected_len.saturating_add(1));
    limited.read_to_end(&mut bytes).ok()?;
    (bytes.len() == expected_len_usize).then_some(bytes)
}

fn create_parent_dir(path: &Path) -> Result<(), DurableFileError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| io_error(parent.display(), source))?;
    }
    Ok(())
}

fn write_temp_file(
    temp_path: &Path,
    content: &[u8],
    create_new: bool,
    sync: DurableFileSync,
) -> Result<(), DurableFileError> {
    let mut file = write_temp_file_opened(temp_path, content, create_new)?;
    sync_file(&mut file, temp_path, sync)
}

fn write_temp_file_without_sync(
    temp_path: &Path,
    content: &[u8],
    create_new: bool,
) -> Result<(), DurableFileError> {
    write_temp_file_opened(temp_path, content, create_new).map(|_| ())
}

fn write_temp_file_opened(
    temp_path: &Path,
    content: &[u8],
    create_new: bool,
) -> Result<File, DurableFileError> {
    let mut options = OpenOptions::new();
    options.write(true);
    if create_new {
        options.create_new(true);
    } else {
        options.create(true).truncate(true);
    }
    let mut file = options
        .open(temp_path)
        .map_err(|source| io_error(temp_path.display(), source))?;
    file.write_all(content)
        .map_err(|source| io_error(temp_path.display(), source))?;
    Ok(file)
}

fn sync_file(file: &mut File, path: &Path, sync: DurableFileSync) -> Result<(), DurableFileError> {
    match sync {
        DurableFileSync::Data => file.sync_data(),
        DurableFileSync::All => file.sync_all(),
    }
    .map_err(|source| io_error(path.display(), source))
}

fn seek_to_record_start(
    file: &mut File,
    path: &Path,
    record_start: u64,
) -> Result<(), DurableFileError> {
    file.seek(SeekFrom::Start(record_start))
        .map(|_| ())
        .map_err(|source| io_error(path.display(), source))
}

fn temp_path(parent: &Path, path: &Path, fallback_file_name: &str, temp_label: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_file_name);
    parent.join(format!(
        ".{file_name}.{temp_label}-{}-{}",
        std::process::id(),
        TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn file_modified_and_ino(path: &Path) -> (Option<SystemTime>, u64) {
    match fs::metadata(path) {
        Ok(metadata) => (metadata.modified().ok(), file_ino(&metadata)),
        Err(_) => (None, 0),
    }
}

fn io_error(path: impl std::fmt::Display, source: io::Error) -> DurableFileError {
    DurableFileError::Io {
        path: path.to_string(),
        kind: source.kind(),
        detail: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_private_stage(path: &Path, content: &[u8]) {
        let mut file = create_private_staged_file(path).expect("create private stage");
        file.write_all(content).expect("write stage");
        file.sync_all().expect("sync stage");
    }

    #[test]
    fn staged_install_atomically_replaces_content_and_preserves_permissions() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-replace");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        fs::write(&destination, b"old content").expect("write destination");
        write_private_stage(&staged, b"new content");

        #[cfg(unix)]
        {
            fs::set_permissions(&destination, fs::Permissions::from_mode(0o600))
                .expect("restrict destination");
            fs::set_permissions(&staged, fs::Permissions::from_mode(0o644))
                .expect("widen stage before install");
        }

        install_staged_file(&staged, &destination, true).expect("replace destination");

        assert_eq!(
            fs::read(&destination).expect("read destination"),
            b"new content"
        );
        assert!(!staged.exists(), "successful replace consumes staged name");
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&destination)
                .expect("destination metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "replacement must not widen destination permissions"
        );
    }

    #[cfg(windows)]
    #[test]
    fn replace_file_retries_a_transient_destination_sharing_violation() {
        use std::os::windows::fs::OpenOptionsExt as _;

        const FILE_SHARE_READ: u32 = 0x0000_0001;
        const FILE_SHARE_WRITE: u32 = 0x0000_0002;

        let root = sinomo_testdir::TestDir::new("storage-replace-sharing-violation");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        fs::write(&destination, b"old content").expect("write destination");
        fs::write(&staged, b"new content").expect("write stage");

        // Deliberately omit FILE_SHARE_DELETE to reproduce ERROR_SHARING_VIOLATION
        // from ReplaceFileW while an external reader briefly holds the target.
        let destination_handle = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(&destination)
            .expect("open destination without delete sharing");
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(destination_handle);
        });

        let result = replace_file(&staged, &destination);
        release.join().expect("release destination handle");
        result.expect("replace after transient sharing violation");

        assert_eq!(
            fs::read(&destination).expect("read replaced destination"),
            b"new content"
        );
        assert!(!staged.exists(), "successful replace consumes staged name");
    }

    #[cfg(unix)]
    #[test]
    fn staged_install_replaces_read_only_destination_without_widening_mode() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-read-only");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        fs::write(&destination, b"old content").expect("write destination");
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o400))
            .expect("make destination read-only");
        write_private_stage(&staged, b"new content");

        install_staged_file(&staged, &destination, true).expect("replace read-only destination");

        assert_eq!(
            fs::read(&destination).expect("read destination"),
            b"new content"
        );
        assert_eq!(
            fs::metadata(&destination)
                .expect("destination metadata")
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
    }

    #[test]
    fn staged_install_overwrite_creates_private_destination_when_absent() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-create");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        write_private_stage(&staged, b"new content");

        install_staged_file(&staged, &destination, true).expect("install absent destination");

        assert_eq!(
            fs::read(&destination).expect("read destination"),
            b"new content"
        );
        assert!(!staged.exists(), "successful install consumes staged name");
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&destination)
                .expect("destination metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn staged_install_no_clobber_publishes_once() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-no-clobber");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        write_private_stage(&staged, b"content");

        install_staged_file(&staged, &destination, false).expect("publish destination");

        assert_eq!(
            fs::read(&destination).expect("read destination"),
            b"content"
        );
        assert!(!staged.exists(), "successful publish removes staged link");
    }

    #[test]
    fn staged_install_no_clobber_failure_preserves_stage_for_cleanup() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-preserve-failure");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        fs::write(&destination, b"winner").expect("write destination");
        write_private_stage(&staged, b"loser");

        let error = install_staged_file(&staged, &destination, false)
            .expect_err("existing destination must win");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&destination).expect("read winner"), b"winner");
        assert_eq!(fs::read(&staged).expect("read preserved stage"), b"loser");
        fs::remove_file(&staged).expect("cleanup preserved stage");
        assert!(!staged.exists());
    }

    #[cfg(unix)]
    #[test]
    fn staged_install_rejects_overwriting_symlink_and_preserves_stage() {
        use std::os::unix::fs::symlink;

        let root = sinomo_testdir::TestDir::new("storage-staged-install-symlink");
        let target = root.join("target.bin");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        fs::write(&target, b"target content").expect("write target");
        symlink(&target, &destination).expect("create destination symlink");
        write_private_stage(&staged, b"new content");

        let error = install_staged_file(&staged, &destination, true)
            .expect_err("overwrite must reject a symlink destination");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            fs::symlink_metadata(&destination)
                .expect("symlink metadata")
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&target).expect("read target"), b"target content");
        assert_eq!(
            fs::read(&staged).expect("read preserved stage"),
            b"new content"
        );
        fs::remove_file(&staged).expect("cleanup preserved stage");
    }

    #[test]
    fn private_directory_create_and_ensure_enforce_owner_only_mode() {
        let root = sinomo_testdir::TestDir::new("storage-private-directory");
        let created = root.join("created");
        ensure_private_directory(&created).expect("create private directory");
        assert!(fs::metadata(&created).expect("created metadata").is_dir());
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&created)
                .expect("created metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );

        let existing = root.join("existing");
        fs::create_dir(&existing).expect("create existing directory");
        #[cfg(unix)]
        fs::set_permissions(&existing, fs::Permissions::from_mode(0o777))
            .expect("widen existing directory");
        ensure_private_directory(&existing).expect("tighten existing directory");
        ensure_private_directory(&existing).expect("idempotent ensure");
        assert!(fs::metadata(&existing).expect("existing metadata").is_dir());
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&existing)
                .expect("existing metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[test]
    fn private_directory_create_new_rejects_session_directory_reuse() {
        let root = sinomo_testdir::TestDir::new("storage-private-directory-create-new");
        let path = root.join("session");
        create_private_directory(&path).expect("create private session directory");
        fs::write(path.join("owner"), b"first session").expect("write session marker");

        let error = create_private_directory(&path).expect_err("session reuse must fail");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::read(path.join("owner")).expect("existing session remains"),
            b"first session"
        );
    }

    #[test]
    fn private_directory_rejects_non_directory_entry() {
        let root = sinomo_testdir::TestDir::new("storage-private-directory-file");
        let path = root.join("not-a-directory");
        fs::write(&path, b"content").expect("write file");

        let error = ensure_private_directory(&path).expect_err("file must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(fs::read(&path).expect("file remains"), b"content");
    }

    #[cfg(unix)]
    #[test]
    fn private_directory_rejects_symlink_without_changing_target() {
        use std::os::unix::fs::symlink;

        let root = sinomo_testdir::TestDir::new("storage-private-directory-symlink");
        let target = root.join("target");
        let path = root.join("link");
        fs::create_dir(&target).expect("create target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).expect("set target mode");
        symlink(&target, &path).expect("create symlink");

        let error = ensure_private_directory(&path).expect_err("symlink must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            fs::metadata(&target)
                .expect("target metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[cfg(unix)]
    #[test]
    fn private_stage_is_created_with_owner_only_permissions() {
        let root = sinomo_testdir::TestDir::new("storage-private-staged-file");
        let staged = root.join("stage.bin");
        let file = create_private_staged_file(&staged).expect("create private stage");
        assert_eq!(
            file.metadata()
                .expect("stage metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn length_prefixed_records_stop_at_torn_tail_and_rewind() {
        let root = sinomo_testdir::TestDir::new("storage-length-prefixed-torn-tail");
        let path = root.join("records.slog");
        let mut append = open_file_for_append(&path).expect("open append");
        append_length_prefixed_records(
            &mut append.file,
            &path,
            [b"alpha".as_slice(), b"beta".as_slice()],
            DurableRecordAppendOptions::new(DurableLengthEndian::Little)
                .sync(DurableFileSync::Data),
        )
        .expect("append records");
        append
            .file
            .write_all(&10_u32.to_le_bytes())
            .expect("length");
        append.file.write_all(b"short").expect("torn body");
        append.file.sync_all().expect("sync");
        drop(append);

        let mut file = File::open(&path).expect("open read");
        let first = read_length_prefixed_record(
            &mut file,
            &path,
            DurableRecordReadOptions::new(DurableLengthEndian::Little),
        )
        .expect("read first")
        .expect("first record");
        assert_eq!(first.bytes, b"alpha");
        let second = read_length_prefixed_record(
            &mut file,
            &path,
            DurableRecordReadOptions::new(DurableLengthEndian::Little),
        )
        .expect("read second")
        .expect("second record");
        assert_eq!(second.bytes, b"beta");
        let torn_start = file.stream_position().expect("position before torn");
        assert!(
            read_length_prefixed_record(
                &mut file,
                &path,
                DurableRecordReadOptions::new(DurableLengthEndian::Little),
            )
            .expect("read torn")
            .is_none()
        );
        assert_eq!(
            file.stream_position().expect("position after torn"),
            torn_start
        );
    }

    #[test]
    fn length_prefixed_record_limit_has_typed_error() {
        let root = sinomo_testdir::TestDir::new("storage-length-prefixed-record-limit");
        let path = root.join("records.slog");
        fs::write(&path, 9_u32.to_be_bytes()).expect("write oversized length");

        let mut file = File::open(&path).expect("open read");
        let error = read_length_prefixed_record(
            &mut file,
            &path,
            DurableRecordReadOptions::new(DurableLengthEndian::Big).max_record_bytes(8),
        )
        .expect_err("record length must exceed configured limit");

        assert!(matches!(
            error,
            DurableFileError::RecordTooLarge {
                record_bytes: 9,
                max_record_bytes: 8,
                ..
            }
        ));
    }

    #[test]
    fn create_new_atomic_rejects_existing_and_relaxed_batch_skips_existing() {
        let root = sinomo_testdir::TestDir::new("storage-create-new-helpers");
        let path = root.join("nested/object.bin");
        write_file_create_new_atomic(
            &path,
            b"first",
            DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
        )
        .expect("create first");
        let error = write_file_create_new_atomic(
            &path,
            b"second",
            DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
        )
        .expect_err("existing target should fail");
        assert!(error.detail().contains("File exists") || error.detail().contains("exists"));
        assert_eq!(fs::read(&path).expect("read existing"), b"first");

        let new_path = root.join("batch/new.bin");
        write_files_create_new_relaxed(
            [
                (path.as_path(), b"ignored".as_slice()),
                (new_path.as_path(), b"created".as_slice()),
            ],
            DurableAtomicFileOptions::new("tmp", "tmp"),
        )
        .expect("relaxed batch");
        assert_eq!(fs::read(&path).expect("read skipped"), b"first");
        assert_eq!(fs::read(&new_path).expect("read created"), b"created");
    }

    #[test]
    fn explicit_object_file_read_cap_rejects_before_reading_body() {
        let root = sinomo_testdir::TestDir::new("storage-object-file-custom-read-cap");
        let path = root.join("object.sobj");
        fs::write(&path, b"this body is intentionally longer than eight bytes").expect("write");

        let error = read_object_file_bytes(&path, 8).expect_err("custom cap");
        assert!(
            matches!(
                error,
                DurableFileError::Io {
                    kind: io::ErrorKind::InvalidData,
                    ..
                }
            ),
            "unexpected custom cap error: {error:?}"
        );
    }

    #[test]
    fn remove_helpers_report_whether_anything_was_removed() {
        let root = sinomo_testdir::TestDir::new("storage-remove-sync-helpers");
        let file_path = root.join("dir/file.bin");
        fs::create_dir_all(file_path.parent().expect("parent")).expect("mkdir");
        fs::write(&file_path, b"content").expect("write file");
        assert!(remove_file_if_exists_and_sync_parent(&file_path).expect("remove file"));
        assert!(!remove_file_if_exists_and_sync_parent(&file_path).expect("missing file"));

        let empty_dir = root.join("empty");
        fs::create_dir_all(&empty_dir).expect("mkdir empty");
        assert!(remove_empty_dir_if_exists_and_sync_parent(&empty_dir).expect("remove empty"));
        assert!(!remove_empty_dir_if_exists_and_sync_parent(&empty_dir).expect("missing dir"));

        let non_empty = root.join("non-empty");
        fs::create_dir_all(&non_empty).expect("mkdir non-empty");
        fs::write(non_empty.join("child"), b"content").expect("write child");
        assert!(!remove_empty_dir_if_exists_and_sync_parent(&non_empty).expect("non-empty dir"));
    }

    #[test]
    fn file_blake3_hash_and_size_streams_file_content() {
        let root = sinomo_testdir::TestDir::new("storage-blake3-file-hash");
        let path = root.join("content.bin");
        fs::write(&path, b"abcdef").expect("write");
        let (hash, size) = file_blake3_hash_and_size(&path).expect("hash");
        assert_eq!(hash, blake3::hash(b"abcdef"));
        assert_eq!(size, 6);
    }
}
