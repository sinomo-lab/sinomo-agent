use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
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
            | DurableFileError::ResourceExhausted { path, .. } => path,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            DurableFileError::Io { detail, .. } => detail,
            DurableFileError::ResourceExhausted { operation, .. } => operation,
        }
    }

    pub fn kind(&self) -> io::ErrorKind {
        match self {
            DurableFileError::Io { kind, .. } => *kind,
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
        return Err(DurableFileError::Io {
            path: path.display().to_string(),
            kind: io::ErrorKind::InvalidData,
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

pub fn replace_file(temp_path: &Path, path: &Path) -> Result<(), DurableFileError> {
    match fs::rename(temp_path, path) {
        Ok(()) => Ok(()),
        Err(source)
            if path.exists()
                && matches!(
                    source.kind(),
                    io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied
                ) =>
        {
            fs::remove_file(path).map_err(|remove_source| DurableFileError::Io {
                path: path.display().to_string(),
                kind: remove_source.kind(),
                detail: format!(
                    "replace failed with {}; additionally failed to remove existing file: {}",
                    source, remove_source
                ),
            })?;
            fs::rename(temp_path, path).map_err(|rename_source| DurableFileError::Io {
                path: format!("{} -> {}", temp_path.display(), path.display()),
                kind: rename_source.kind(),
                detail: format!(
                    "replace after removing existing file failed: {}",
                    rename_source
                ),
            })
        }
        Err(source) => Err(io_error(
            format!("{} -> {}", temp_path.display(), path.display()),
            source,
        )),
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
