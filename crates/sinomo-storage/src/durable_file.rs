use crate::io_trace;
use std::collections::HashMap;
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
#[cfg(test)]
use std::io::Write;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::os::unix::ffi::OsStrExt as _;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
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

/// Sequential reader for a caller-fenced, immutable view of an append log.
/// Opens no persistent cache: each reader samples the file length and reads all
/// requested records anew. Read-ahead batches system calls, and record offsets
/// are advanced from validated frame lengths rather than seeking per record.
#[derive(Debug)]
pub struct DurableRecordReader {
    reader: BufReader<File>,
    path: PathBuf,
    options: DurableRecordReadOptions,
    file_len: u64,
    position: u64,
}

impl DurableRecordReader {
    pub fn new(
        mut file: File,
        path: &Path,
        options: DurableRecordReadOptions,
    ) -> Result<Self, DurableFileError> {
        let position = file
            .stream_position()
            .map_err(|source| io_error(path.display(), source))?;
        let file_len = file
            .metadata()
            .map_err(|source| io_error(path.display(), source))?
            .len();
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, file),
            path: path.to_path_buf(),
            options,
            file_len,
            position,
        })
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    pub fn next_record(&mut self) -> Result<Option<DurableRecord>, DurableFileError> {
        let record = read_length_prefixed_record_at(
            &mut self.reader,
            &self.path,
            self.options,
            self.position,
            self.file_len,
            |_| {},
        )?;
        if let Some(record) = &record {
            self.position = record.record_end;
        }
        Ok(record)
    }
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

impl Default for DurableAppendFileOptions {
    fn default() -> Self {
        Self::new()
    }
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
    on_read: F,
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
    read_length_prefixed_record_at(file, path, options, record_start, file_len, on_read)
}

fn read_length_prefixed_record_at<R: Read + Seek, F: FnMut(u64)>(
    file: &mut R,
    path: &Path,
    options: DurableRecordReadOptions,
    record_start: u64,
    file_len: u64,
    mut on_read: F,
) -> Result<Option<DurableRecord>, DurableFileError> {
    let io_span = tracing::trace_span!(target: "sinomo.perf", "storage.read_record", stage = "storage.read_record",
        io_kind = "disk.read", io_bytes = 0u64, outcome = "error");
    let _entered = io_span.enter();
    let mut read_bytes = 0u64;
    let mut on_read = |bytes| {
        read_bytes += bytes;
        io_span.record("io_bytes", read_bytes);
        on_read(bytes);
    };
    let mut length_bytes = [0_u8; 4];
    match read_full_chunk_with_observer(path.display(), file, &mut length_bytes, &mut on_read)? {
        DurableReadChunk::Complete => {}
        DurableReadChunk::CleanEof | DurableReadChunk::Truncated => {
            seek_to_record_start(file, path, record_start)?;
            io_span.record("outcome", "ok");
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
        io_span.record("outcome", "ok");
        return Ok(None);
    }
    let mut bytes = try_zeroed_vec(path.display(), length, "read length-prefixed record")?;
    match read_full_chunk_with_observer(path.display(), file, &mut bytes, &mut on_read)? {
        DurableReadChunk::Complete => {}
        DurableReadChunk::CleanEof | DurableReadChunk::Truncated => {
            seek_to_record_start(file, path, record_start)?;
            io_span.record("outcome", "ok");
            return Ok(None);
        }
    }
    io_span.record("outcome", "ok");
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
        io_trace::write_all(file, &length_bytes)
            .and_then(|()| io_trace::write_all(file, record))
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
    match file.set_len(offset) {
        Ok(()) => {
            file.seek(SeekFrom::Start(offset))
                .map_err(|source| io_error(path.display(), source))?;
            sync_file(file, path, sync)
        }
        #[cfg(windows)]
        Err(source) if source.kind() == io::ErrorKind::PermissionDenied => {
            // A Windows append handle may carry FILE_APPEND_DATA without the
            // FILE_WRITE_DATA right required by SetEndOfFile, even when the
            // Rust OpenOptions also requested write access. Reopen the same
            // shared file for truncation, durably sync that handle, then
            // reposition the long-lived append/read handle.
            let mut truncator = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|source| io_error(path.display(), source))?;
            truncator
                .set_len(offset)
                .map_err(|source| io_error(path.display(), source))?;
            truncator
                .seek(SeekFrom::Start(offset))
                .map_err(|source| io_error(path.display(), source))?;
            sync_file(&mut truncator, path, sync)?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|source| io_error(path.display(), source))?;
            Ok(())
        }
        Err(source) => Err(io_error(path.display(), source)),
    }
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
    let publication = match publish_staged_no_replace(&temp_path, path) {
        Ok(publication) => publication,
        Err(source) => {
            let _ = fs::remove_file(&temp_path);
            return Err(io_error(path.display(), source));
        }
    };
    if publication == NoReplacePublication::Linked {
        fs::remove_file(&temp_path).map_err(|source| io_error(temp_path.display(), source))?;
    }
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
        match publish_staged_no_replace(&temp_path, path) {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Ok(NoReplacePublication::Renamed) => continue,
            Ok(NoReplacePublication::Linked) => {}
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NoReplacePublication {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    Renamed,
    Linked,
}

fn publish_staged_no_replace(
    staged_path: &Path,
    destination_path: &Path,
) -> io::Result<NoReplacePublication> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    match rename_noreplace(staged_path, destination_path) {
        Ok(()) => return Ok(NoReplacePublication::Renamed),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => return Err(source),
        Err(source) if !rename_noreplace_can_fallback(&source) => return Err(source),
        Err(_) => {}
    }

    fs::hard_link(staged_path, destination_path)?;
    Ok(NoReplacePublication::Linked)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_noreplace(staged_path: &Path, destination_path: &Path) -> io::Result<()> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    let staged_path = CString::new(staged_path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "staged path contains an interior NUL byte",
        )
    })?;
    let destination_path = CString::new(destination_path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination path contains an interior NUL byte",
        )
    })?;
    // SAFETY: both C strings remain alive for the syscall and point at
    // NUL-terminated path bytes. RENAME_NOREPLACE performs one atomic
    // same-filesystem namespace operation without replacing an existing name.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            staged_path.as_ptr(),
            libc::AT_FDCWD,
            destination_path.as_ptr(),
            RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_noreplace_can_fallback(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
    )
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
    let file = match open_object_file_for_read(path) {
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
    let file =
        open_object_file_for_read(path).map_err(|source| io_error(path.display(), source))?;
    read_object_file_bytes_from_open_file(path, file, max_read_bytes)
}

fn open_object_file_for_read(path: &Path) -> io::Result<File> {
    #[cfg(windows)]
    {
        open_with_windows_sharing_retry(|| File::open(path))
    }
    #[cfg(not(windows))]
    {
        File::open(path)
    }
}

#[cfg(any(windows, test))]
fn open_with_windows_sharing_retry<T>(mut open: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    // ReplaceFileW and external scanners can briefly deny an otherwise valid
    // open. Retrying admission preserves the single-handle read below, so a
    // replacement cannot mix generations. Missing files, permissions, content
    // errors and persistent contention still fail under the original contract.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(40);
    let mut delay = std::time::Duration::from_millis(2);
    loop {
        match open() {
            Err(error) if matches!(error.raw_os_error(), Some(32 | 33)) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                std::thread::sleep(delay.min(remaining));
                delay = delay
                    .saturating_mul(2)
                    .min(std::time::Duration::from_millis(10));
            }
            result => return result,
        }
    }
}

pub fn read_object_file_bytes_from_open_file(
    path: &Path,
    file: File,
    max_read_bytes: u64,
) -> Result<Vec<u8>, DurableFileError> {
    let io_span = tracing::trace_span!(target: "sinomo.perf", "storage.read_object", stage = "storage.read_object",
        io_kind = "disk.read", io_bytes = 0u64, outcome = "error");
    let _entered = io_span.enter();
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
        io_span.record("io_bytes", bytes.len() as u64 + read as u64);
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
    io_span.record("outcome", "ok");
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

/// Publish a complete replacement atomically without a durability barrier.
///
/// Use only for derived hints that can be rebuilt from an independently synced
/// source of truth. A crash may lose or corrupt this file; readers must tolerate
/// that. Authoritative data must continue to use `write_file_atomic`.
pub fn write_file_atomic_hint(
    path: &Path,
    content: &[u8],
    options: DurableAtomicFileOptions<'_>,
) -> Result<(), DurableFileError> {
    create_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temp_path(parent, path, options.fallback_file_name, options.temp_label);
    if let Err(error) = write_temp_file_without_sync(&temp_path, content, false) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    if let Err(error) = replace_file(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    Ok(())
}

/// Exclusively create a stage using the host filesystem defaults.
pub fn create_private_staged_file(path: &Path) -> Result<File, DurableFileError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| io_error(path.display(), source))
}

/// A private stage whose bytes have been synchronized.
///
/// The writable file is consumed by `sync_private_staged_file`. Unix retains it
/// privately for inode validation; other platforms close it before using the
/// generic installer. This capability exposes no writable handle. Its caller
/// continues to own cleanup of the staged pathname on failure.
#[derive(Debug)]
pub struct SyncedPrivateStagedFile {
    path: PathBuf,
    #[cfg(unix)]
    file: File,
    #[cfg(unix)]
    identity: SyncedPrivateStageIdentity,
}

impl SyncedPrivateStagedFile {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
#[derive(Debug, Eq, PartialEq)]
struct SyncedPrivateStageIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanos: i64,
}

#[cfg(unix)]
impl SyncedPrivateStageIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanos: metadata.mtime_nsec(),
        }
    }
}

/// Finish a private stage after its producer has validated its contents.
///
/// This is the required file durability barrier, not a caller assertion that a
/// sync happened elsewhere. The returned capability admits only no-clobber
/// publication; ordinary replacements retain `install_staged_file` semantics.
/// The producer must own the only writer from finalization through publication:
/// no cloned handle or pathname alias may mutate the stage during that interval.
/// On Unix, installation checks inode, length, and modification time to reject a stage whose exclusive-ownership contract was violated.
pub fn sync_private_staged_file(
    path: &Path,
    file: File,
) -> Result<SyncedPrivateStagedFile, DurableFileError> {
    sync_private_staged_file_with(path, file, io_trace::sync_all)
}

fn sync_private_staged_file_with(
    path: &Path,
    file: File,
    sync: impl FnOnce(&File) -> io::Result<()>,
) -> Result<SyncedPrivateStagedFile, DurableFileError> {
    let metadata = file
        .metadata()
        .map_err(|source| io_error(path.display(), source))?;
    let path_metadata =
        fs::symlink_metadata(path).map_err(|source| io_error(path.display(), source))?;
    if !metadata.is_file() || !path_metadata.file_type().is_file() {
        return Err(io_error(
            path.display(),
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "private stage must name a regular file",
            ),
        ));
    }
    #[cfg(unix)]
    {
        if metadata.dev() != path_metadata.dev() || metadata.ino() != path_metadata.ino() {
            return Err(io_error(
                path.display(),
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private stage path does not name its open file",
                ),
            ));
        }
    }
    #[cfg(unix)]
    let identity_before_sync = SyncedPrivateStageIdentity::from_metadata(&metadata);
    sync(&file).map_err(|source| io_error(path.display(), source))?;
    #[cfg(unix)]
    let identity = SyncedPrivateStageIdentity::from_metadata(
        &file
            .metadata()
            .map_err(|source| io_error(path.display(), source))?,
    );
    #[cfg(unix)]
    if identity != identity_before_sync {
        return Err(io_error(
            path.display(),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "private stage changed while it was being synchronized",
            ),
        ));
    }
    #[cfg(not(unix))]
    drop(file);
    Ok(SyncedPrivateStagedFile {
        path: path.to_path_buf(),
        #[cfg(unix)]
        file,
        #[cfg(unix)]
        identity,
    })
}

/// Publish a finalized private stage without rewriting its unchanged metadata.
///
/// On Unix, the retained file handle and current pathname must still identify
/// the exact synchronized inode, size and modification time. Any
/// mismatch fails before publication. Both publication paths synchronize
/// their directory entries.
pub fn install_synced_private_staged_file(
    staged: &SyncedPrivateStagedFile,
    destination_path: &Path,
) -> Result<(), DurableFileError> {
    #[cfg(not(unix))]
    {
        install_staged_file(&staged.path, destination_path, false)
    }
    #[cfg(unix)]
    {
        let (staged_parent, destination_parent) =
            validate_staged_install_paths(&staged.path, destination_path)?;
        validate_synced_private_stage(staged)?;
        let publication =
            publish_staged_no_replace(&staged.path, destination_path).map_err(|source| {
                io_error(
                    format!(
                        "{} -> {}",
                        staged.path.display(),
                        destination_path.display()
                    ),
                    source,
                )
            })?;
        sync_directory(&destination_parent)?;
        if publication == NoReplacePublication::Linked {
            fs::remove_file(&staged.path)
                .map_err(|source| io_error(staged.path.display(), source))?;
        }
        if staged_parent != destination_parent {
            sync_directory(&staged_parent)?;
        }
        Ok(())
    }
}

/// Publish an owned batch of already-synced private files without clobbering.
/// Every file retains the single-file installer's identity checks. On Unix,
/// directory barriers cover the whole batch before success; an error may leave
/// a prefix of individually synced files visible, never a successful batch.
/// Callers must not acknowledge their checkpoint until this function succeeds.
/// Other platforms retain the single-file installer's durability policy.
pub fn install_synced_private_staged_files(
    files: &[(SyncedPrivateStagedFile, PathBuf)],
) -> Result<(), DurableFileError> {
    #[cfg(not(unix))]
    {
        for (staged, destination) in files {
            install_synced_private_staged_file(staged, destination)?;
        }
        Ok(())
    }
    #[cfg(unix)]
    {
        install_synced_private_staged_files_with(files, sync_directory)
    }
}

#[cfg(unix)]
fn install_synced_private_staged_files_with(
    files: &[(SyncedPrivateStagedFile, PathBuf)],
    mut sync: impl FnMut(&Path) -> Result<(), DurableFileError>,
) -> Result<(), DurableFileError> {
    let mut parents = std::collections::BTreeSet::new();
    // Reject a changed stage before any batch effect, then recheck it at its
    // own publication boundary as the single-file installer does.
    for (staged, destination) in files {
        let (source_parent, destination_parent) =
            validate_staged_install_paths(&staged.path, destination)?;
        validate_synced_private_stage(staged)?;
        parents.insert(source_parent);
        parents.insert(destination_parent);
    }
    for (staged, destination) in files {
        validate_synced_private_stage(staged)?;
        let publication = publish_staged_no_replace(&staged.path, destination)
            .map_err(|source| io_error(destination.display(), source))?;
        if publication == NoReplacePublication::Linked {
            fs::remove_file(&staged.path)
                .map_err(|source| io_error(staged.path.display(), source))?;
        }
    }
    for parent in parents {
        sync(&parent)?;
    }
    Ok(())
}

#[cfg(unix)]
fn validate_synced_private_stage(staged: &SyncedPrivateStagedFile) -> Result<(), DurableFileError> {
    let metadata = staged
        .file
        .metadata()
        .map_err(|source| io_error(staged.path.display(), source))?;
    let path_metadata = fs::symlink_metadata(&staged.path)
        .map_err(|source| io_error(staged.path.display(), source))?;
    if SyncedPrivateStageIdentity::from_metadata(&metadata) != staged.identity
        || SyncedPrivateStageIdentity::from_metadata(&path_metadata) != staged.identity
    {
        return Err(io_error(
            staged.path.display(),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "private stage identity or metadata changed after synchronization",
            ),
        ));
    }
    Ok(())
}

/// Read a bounded private stage while preserving its opaque synchronization
/// capability. This grants no new publication authority; a caller copying these
/// bytes to another durable representation must synchronize that representation.
/// Unix checks the exact synchronized inode/path identity before and after reading.
/// Other platforms return `None` and retain the existing installer contract.
pub fn read_synced_private_staged_file_bounded(
    staged: &SyncedPrivateStagedFile,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, DurableFileError> {
    #[cfg(not(unix))]
    {
        let _ = (staged, max_bytes);
        Ok(None)
    }
    #[cfg(unix)]
    {
        read_synced_private_staged_file_bounded_with(
            staged,
            max_bytes,
            read_object_file_bytes_from_open_file,
        )
    }
}

#[cfg(unix)]
fn read_synced_private_staged_file_bounded_with(
    staged: &SyncedPrivateStagedFile,
    max_bytes: usize,
    read: impl FnOnce(&Path, File, u64) -> Result<Vec<u8>, DurableFileError>,
) -> Result<Option<Vec<u8>>, DurableFileError> {
    validate_synced_private_stage(staged)?;
    if staged.identity.length > max_bytes as u64 {
        return Ok(None);
    }
    let file =
        File::open(&staged.path).map_err(|source| io_error(staged.path.display(), source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_error(staged.path.display(), source))?;
    if SyncedPrivateStageIdentity::from_metadata(&metadata) != staged.identity {
        return Err(io_error(
            staged.path.display(),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "private stage changed while opening bounded reader",
            ),
        ));
    }
    let bytes = read(&staged.path, file, max_bytes as u64)?;
    validate_synced_private_stage(staged)?;
    if bytes.len() as u64 != staged.identity.length {
        return Err(io_error(
            staged.path.display(),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "private stage changed length during bounded read",
            ),
        ));
    }
    Ok(Some(bytes))
}

/// Creates one new directory with the deployment's normal umask and inherited
/// ACLs, then durably records it. An existing entry is never reused.
pub fn create_directory(path: &Path) -> Result<(), DurableFileError> {
    let parent = validate_directory_parent(path)?;
    fs::create_dir(path).map_err(|source| io_error(path.display(), source))?;
    sync_directory(path)?;
    sync_directory(parent)
}

/// Ensures one real directory without changing its permissions or ACLs.
/// New directories inherit the deployment's filesystem policy. Existing
/// directories retain their policy, including group sharing and special bits.
/// Symlinks and other non-directory entries are rejected.
pub fn ensure_directory(path: &Path) -> Result<(), DurableFileError> {
    validate_directory_parent(path)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_directory_metadata(path, &metadata)?,
        Err(source) if source.kind() == io::ErrorKind::NotFound => match create_directory(path) {
            Ok(()) => return Ok(()),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path)
                    .map_err(|source| io_error(path.display(), source))?;
                validate_directory_metadata(path, &metadata)?;
            }
            Err(source) => return Err(source),
        },
        Err(source) => return Err(io_error(path.display(), source)),
    }
    sync_directory(path)
}

pub use create_directory as create_private_directory;
pub use ensure_directory as ensure_private_directory;
fn validate_directory_parent(path: &Path) -> Result<&Path, DurableFileError> {
    if path.file_name().is_none() {
        return Err(DurableFileError::Io {
            path: path.display().to_string(),
            kind: io::ErrorKind::InvalidInput,
            detail: "directory path does not have a directory name".to_string(),
        });
    }
    let parent = install_parent(path);
    match fs::symlink_metadata(parent) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(DurableFileError::Io {
                path: parent.display().to_string(),
                kind: io::ErrorKind::InvalidInput,
                detail: "directory parent is not a directory".to_string(),
            });
        }
        Err(source) => return Err(io_error(parent.display(), source)),
    }
    Ok(parent)
}

fn validate_directory_metadata(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), DurableFileError> {
    if metadata.file_type().is_dir() {
        return Ok(());
    }
    Err(DurableFileError::Io {
        path: path.display().to_string(),
        kind: io::ErrorKind::InvalidInput,
        detail: "directory path is not a real directory".to_string(),
    })
}

/// Atomically publishes an already-synced staged file on the destination's
/// filesystem and then makes both namespace updates durable.
///
/// The staged path and destination must name distinct entries on the same
/// filesystem, but may live in different directories. With `overwrite`
/// disabled, destination creation uses an atomic
/// no-replace rename where supported and otherwise a hard-link publication, so
/// an existing destination wins without a preflight existence check. With
/// `overwrite` enabled, the platform's atomic replace operation is used; the
/// destination is never deleted as a fallback.
///
/// A failed publish leaves the staged path in place for the caller's cleanup
/// guard. Once the atomic namespace operation succeeds, a subsequent directory
/// sync error can only report an indeterminate durability outcome: the staged
/// name has already been consumed by a rename, or is still linked to the
/// installed file on a hard-link fallback.
pub fn install_staged_file(
    staged_path: &Path,
    destination_path: &Path,
    overwrite: bool,
) -> Result<(), DurableFileError> {
    let (staged_parent, destination_parent) =
        validate_staged_install_paths(staged_path, destination_path)?;

    if overwrite {
        match fs::symlink_metadata(destination_path) {
            Ok(metadata) if !metadata.is_file() => {
                return Err(io_error(
                    destination_path.display(),
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "destination is not a regular file",
                    ),
                ));
            }
            Err(error) if error.kind() != io::ErrorKind::NotFound => {
                return Err(io_error(destination_path.display(), error));
            }
            _ => {}
        }
        replace_file(staged_path, destination_path)?;
        sync_install_directories(&staged_parent, &destination_parent)?;
        return Ok(());
    }

    let publication =
        publish_staged_no_replace(staged_path, destination_path).map_err(|source| {
            io_error(
                format!(
                    "{} -> {}",
                    staged_path.display(),
                    destination_path.display()
                ),
                source,
            )
        })?;

    sync_directory(&destination_parent)?;

    if publication == NoReplacePublication::Linked {
        // The destination link is durable before the staging name is removed.
        // A crash during cleanup can therefore only leave a harmless extra link.
        fs::remove_file(staged_path).map_err(|source| io_error(staged_path.display(), source))?;
    }
    if staged_parent != destination_parent {
        sync_directory(&staged_parent)?;
    }
    Ok(())
}

fn sync_install_directories(
    staged_parent: &Path,
    destination_parent: &Path,
) -> Result<(), DurableFileError> {
    sync_directory(destination_parent)?;
    if staged_parent != destination_parent {
        sync_directory(staged_parent)?;
    }
    Ok(())
}

fn validate_staged_install_paths(
    staged_path: &Path,
    destination_path: &Path,
) -> Result<(PathBuf, PathBuf), DurableFileError> {
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
    if staged_parent == destination_parent && staged_name == destination_name {
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

    Ok((staged_parent, destination_parent))
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
    use std::ptr;
    use std::sync::{Arc, LazyLock, Weak};
    use std::time::{Duration, Instant};

    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_FILE_EXISTS: i32 = 80;
    const ERROR_FILE_NOT_FOUND: i32 = 2;
    const ERROR_ALREADY_EXISTS: i32 = 183;
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    const ERROR_UNABLE_TO_REMOVE_REPLACED: i32 = 1175;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    const REPLACE_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(2);
    const REPLACE_RETRY_MAX_DELAY: Duration = Duration::from_millis(40);
    const REPLACE_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
    const WINDOWS_REPLACE_LOCK_CAPACITY: usize = 4096;

    static WINDOWS_REPLACE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    #[link(name = "kernel32")]
    unsafe extern "system" {
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

    fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
        let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if wide.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows path contains a NUL code unit",
            ));
        }
        const BACKSLASH: u16 = b'\\' as u16;
        const COLON: u16 = b':' as u16;
        const QUESTION: u16 = b'?' as u16;
        const DOT: u16 = b'.' as u16;
        let already_device_path = wide.starts_with(&[BACKSLASH, BACKSLASH, QUESTION, BACKSLASH])
            || wide.starts_with(&[BACKSLASH, BACKSLASH, DOT, BACKSLASH]);
        if !already_device_path {
            // Ordinary Win32 paths accept either separator. Adding the
            // verbatim prefix disables that normalization, so perform it
            // before promoting a drive or UNC path to extended length.
            for unit in &mut wide {
                if *unit == b'/' as u16 {
                    *unit = BACKSLASH;
                }
            }
        }
        if !already_device_path && wide.len() >= 3 && wide[1] == COLON && wide[2] == BACKSLASH {
            wide.splice(0..0, [BACKSLASH, BACKSLASH, QUESTION, BACKSLASH]);
        } else if !already_device_path && wide.starts_with(&[BACKSLASH, BACKSLASH]) {
            wide.splice(
                0..2,
                [
                    BACKSLASH,
                    BACKSLASH,
                    QUESTION,
                    BACKSLASH,
                    b'U' as u16,
                    b'N' as u16,
                    b'C' as u16,
                    BACKSLASH,
                ],
            );
        }
        wide.push(0);
        Ok(wide)
    }
}

pub fn sync_directory(path: &Path) -> Result<(), DurableFileError> {
    #[cfg(windows)]
    {
        match open_dir_for_sync(path).and_then(|dir| io_trace::sync_directory(&dir)) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::PermissionDenied => Ok(()),
            Err(source) => Err(io_error(path.display(), source)),
        }
    }
    #[cfg(not(windows))]
    File::open(path)
        .and_then(|dir| io_trace::sync_directory(&dir))
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
    io_trace::write_all(&mut file, content)
        .map_err(|source| io_error(temp_path.display(), source))?;
    Ok(file)
}

/// Synchronize an already-open file using the shared portable durability policy.
pub fn sync_open_file(
    file: &mut File,
    path: &Path,
    sync: DurableFileSync,
) -> Result<(), DurableFileError> {
    sync_file(file, path, sync)
}

fn sync_file(file: &mut File, path: &Path, sync: DurableFileSync) -> Result<(), DurableFileError> {
    match sync {
        DurableFileSync::Data => io_trace::sync_data(file),
        DurableFileSync::All => io_trace::sync_all(file),
    }
    .map_err(|source| io_error(path.display(), source))
}

fn seek_to_record_start<R: Seek>(
    file: &mut R,
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
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn object_read_open_recovers_only_transient_windows_sharing_errors() {
        for code in [32, 33] {
            let mut attempts = 0;
            let value = open_with_windows_sharing_retry(|| {
                attempts += 1;
                if attempts < 3 {
                    Err(io::Error::from_raw_os_error(code))
                } else {
                    Ok(17)
                }
            })
            .expect("transient sharing failure recovers");
            assert_eq!(value, 17);
            assert_eq!(attempts, 3);
        }
        for code in [2, 5] {
            let mut attempts = 0;
            let result = open_with_windows_sharing_retry::<()>(|| {
                attempts += 1;
                Err(io::Error::from_raw_os_error(code))
            });
            assert_eq!(
                result.expect_err("non-sharing failure").raw_os_error(),
                Some(code)
            );
            assert_eq!(
                attempts, 1,
                "missing files and access denial are not retried"
            );
        }
    }

    #[test]
    fn object_read_open_bounds_persistent_windows_sharing_failure() {
        let started = std::time::Instant::now();
        let error = open_with_windows_sharing_retry::<()>(|| Err(io::Error::from_raw_os_error(32)))
            .expect_err("persistent sharing failure remains an error");
        assert_eq!(error.raw_os_error(), Some(32));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[cfg(windows)]
    #[test]
    fn object_file_reads_survive_a_transient_windows_sharing_violation() {
        let root = sinomo_testdir::TestDir::new("object-read-sharing-violation");
        let path = root.join("snapshot.sobj");
        fs::write(&path, b"one complete generation").expect("snapshot");
        for optional in [false, true] {
            let blocker = OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(&path)
                .expect("exclusive reader");
            let release = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                drop(blocker);
            });
            let result = if optional {
                read_optional_object_file_bytes(&path, 64)
                    .map(|value| value.expect("existing file"))
            } else {
                read_object_file_bytes(&path, 64)
            };
            release.join().expect("release sharing denial");
            assert_eq!(
                result.expect("read after sharing denial"),
                b"one complete generation"
            );
        }
        assert!(
            read_object_file_bytes(&path, 1).is_err(),
            "read bound remains enforced"
        );
    }

    fn write_private_stage(path: &Path, content: &[u8]) {
        let mut file = create_private_staged_file(path).expect("create private stage");
        file.write_all(content).expect("write stage");
        file.sync_all().expect("sync stage");
    }

    fn synced_private_stage(path: &Path, content: &[u8]) -> SyncedPrivateStagedFile {
        let mut file = create_private_staged_file(path).expect("create private stage");
        file.write_all(content).expect("write stage");
        sync_private_staged_file(path, file).expect("finalize private stage")
    }

    #[cfg(unix)]
    #[test]
    fn operator_mode_change_does_not_invalidate_synced_stage() {
        let root = sinomo_testdir::TestDir::new("stage-mode-change");
        let stage = synced_private_stage(&root.join("stage"), b"bytes");
        fs::set_permissions(stage.path(), fs::Permissions::from_mode(0o640)).unwrap();
        let destination = root.join("published");
        install_synced_private_staged_file(&stage, &destination).unwrap();
        assert_eq!(fs::metadata(destination).unwrap().mode() & 0o777, 0o640);
    }

    #[test]
    fn bounded_private_stage_read_preserves_install_capability() {
        let root = sinomo_testdir::TestDir::new("storage-bounded-private-stage-read");
        let path = root.join("stage");
        let content = b"one exact private stage";
        let stage = synced_private_stage(&path, content);
        let read = read_synced_private_staged_file_bounded(&stage, content.len())
            .expect("bounded stage read");
        #[cfg(unix)]
        assert_eq!(read.as_deref(), Some(content.as_slice()));
        #[cfg(not(unix))]
        assert_eq!(
            read, None,
            "unsupported platforms retain installer fallback"
        );
        assert_eq!(fs::read(&path).unwrap(), content);
        let destination = root.join("installed");
        install_synced_private_staged_file(&stage, &destination)
            .expect("read must not consume or weaken the opaque capability");
        assert_eq!(fs::read(destination).unwrap(), content);
    }

    #[cfg(unix)]
    #[test]
    fn bounded_private_stage_limit_falls_back_without_reading_payload() {
        let root = sinomo_testdir::TestDir::new("storage-bounded-private-stage-limit");
        let path = root.join("stage");
        let content = b"larger than the selected inline limit";
        let stage = synced_private_stage(&path, content);
        let called = std::cell::Cell::new(false);
        let result =
            read_synced_private_staged_file_bounded_with(&stage, content.len() - 1, |_, _, _| {
                called.set(true);
                panic!("an over-limit stage must retain the original install path")
            })
            .expect("explicit over-limit fallback");
        assert_eq!(result, None);
        assert!(!called.get());
        let destination = root.join("installed");
        install_synced_private_staged_file(&stage, &destination).unwrap();
        assert_eq!(fs::read(destination).unwrap(), content);

        let empty = synced_private_stage(&root.join("empty"), b"");
        assert_eq!(
            read_synced_private_staged_file_bounded(&empty, 0).unwrap(),
            Some(Vec::new()),
            "zero-length content is within a zero-byte bound"
        );
    }

    #[cfg(unix)]
    #[test]
    fn bounded_private_stage_rejects_replacement_mode_and_same_size_mutation() {
        for mutation in ["replacement", "same-size-content"] {
            let root = sinomo_testdir::TestDir::new("storage-bounded-private-stage-mutation");
            let path = root.join("stage");
            let stage = synced_private_stage(&path, b"initial bytes");
            match mutation {
                "replacement" => {
                    fs::rename(&path, root.join("original-inode")).unwrap();
                    write_private_stage(&path, b"initial bytes");
                }
                "same-size-content" => {
                    let mut alias = OpenOptions::new().write(true).open(&path).unwrap();
                    alias.write_all(b"changed bytes").unwrap();
                    // Avoid sleeps and filesystem timestamp-resolution races.
                    alias.set_modified(SystemTime::UNIX_EPOCH).unwrap();
                }
                _ => unreachable!(),
            }
            let error = read_synced_private_staged_file_bounded(&stage, 64)
                .expect_err("modified stage must not reuse synchronization authority");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{mutation}");
            assert_eq!(
                read_synced_private_staged_file_bounded(&stage, 0)
                    .expect_err("limit fallback cannot hide an invalid capability")
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn bounded_private_stage_rejects_path_replacement_during_read() {
        let root = sinomo_testdir::TestDir::new("storage-bounded-private-stage-open-race");
        let path = root.join("stage");
        let stage = synced_private_stage(&path, b"initial bytes");
        let error =
            read_synced_private_staged_file_bounded_with(&stage, 64, |path, opened, limit| {
                // The reader already holds the validated original inode. A
                // same-size replacement pathname must not certify its bytes.
                fs::rename(path, root.join("retained-original")).unwrap();
                write_private_stage(path, b"changed bytes");
                let bytes = read_object_file_bytes_from_open_file(path, opened, limit)?;
                assert_eq!(bytes, b"initial bytes");
                Ok(bytes)
            })
            .expect_err("after-read validation must reject replacement");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(path).unwrap(), b"changed bytes");
    }

    #[cfg(unix)]
    #[test]
    fn bounded_private_stage_rejects_mutations_after_open() {
        for mutation in ["same-size-content", "shorter", "longer"] {
            let root = sinomo_testdir::TestDir::new("storage-bounded-private-stage-read-mutation");
            let path = root.join("stage");
            let stage = synced_private_stage(&path, b"initial bytes");
            let error =
                read_synced_private_staged_file_bounded_with(&stage, 64, |path, opened, limit| {
                    let mut alias = OpenOptions::new().write(true).open(path).unwrap();
                    match mutation {
                        "same-size-content" => {
                            alias.write_all(b"changed bytes").unwrap();
                            alias.set_modified(SystemTime::UNIX_EPOCH).unwrap();
                        }
                        "shorter" => alias.set_len(3).unwrap(),
                        "longer" => {
                            alias.seek(SeekFrom::End(0)).unwrap();
                            alias.write_all(b" extra bytes").unwrap();
                        }
                        _ => unreachable!(),
                    }
                    read_object_file_bytes_from_open_file(path, opened, limit)
                })
                .expect_err("a mutation after opening cannot produce accepted bytes");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{mutation}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn bounded_private_stage_rejects_short_read_without_metadata_change() {
        let root = sinomo_testdir::TestDir::new("storage-bounded-private-stage-short-read");
        let path = root.join("stage");
        let stage = synced_private_stage(&path, b"initial bytes");
        let error =
            read_synced_private_staged_file_bounded_with(&stage, 64, |path, mut opened, limit| {
                // Inject a short read through the real bounded reader while
                // leaving both the inode and pathname metadata unchanged.
                opened.seek(SeekFrom::Start(1)).unwrap();
                read_object_file_bytes_from_open_file(path, opened, limit)
            })
            .expect_err("unchanged metadata alone does not certify short bytes");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("changed length during bounded read")
        );
        assert_eq!(
            read_synced_private_staged_file_bounded(&stage, 64).unwrap(),
            Some(b"initial bytes".to_vec()),
            "the failed reader does not mutate the still-valid opaque stage"
        );
    }

    #[test]
    fn synced_private_stage_installs_across_directories_without_clobbering() {
        let root = sinomo_testdir::TestDir::new("storage-synced-private-stage");
        let staging = root.join("staging");
        let target = root.join("target");
        fs::create_dir(&staging).expect("staging directory");
        fs::create_dir(&target).expect("target directory");
        let destination = target.join("blob");
        let staged = synced_private_stage(&staging.join("first"), b"durable content");
        install_synced_private_staged_file(&staged, &destination).expect("install content");
        assert_eq!(
            fs::read(&destination).expect("reopen installed content"),
            b"durable content"
        );
        assert!(!staged.path().exists());
        let losing = synced_private_stage(&staging.join("second"), b"different content");
        let error = install_synced_private_staged_file(&losing, &destination)
            .expect_err("existing destination wins");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&destination).unwrap(), b"durable content");
        assert_eq!(fs::read(losing.path()).unwrap(), b"different content");
    }

    #[test]
    fn synced_private_stage_cannot_be_created_after_failed_sync() {
        let root = sinomo_testdir::TestDir::new("storage-synced-private-failed-sync");
        let path = root.join("staged");
        let mut file = create_private_staged_file(&path).expect("create stage");
        file.write_all(b"not certified durable").unwrap();
        let error = sync_private_staged_file_with(&path, file, |_| {
            Err(io::Error::other("injected file sync failure"))
        })
        .expect_err("failed file sync cannot create publication capability");
        assert!(error.to_string().contains("injected file sync failure"));
        assert_eq!(fs::read(&path).unwrap(), b"not certified durable");
    }

    #[cfg(unix)]
    #[test]
    fn synced_private_batch_requires_all_directory_barriers_and_preserves_conflicts() {
        let root = sinomo_testdir::TestDir::new("storage-synced-private-batch");
        let staging = root.join("staging");
        let target = root.join("target");
        fs::create_dir(&staging).unwrap();
        fs::create_dir(&target).unwrap();
        let files = [
            (
                synced_private_stage(&staging.join("a"), b"first"),
                target.join("a"),
            ),
            (
                synced_private_stage(&staging.join("b"), b"second"),
                target.join("b"),
            ),
        ];
        let mut synced = Vec::new();
        let error = install_synced_private_staged_files_with(&files, |parent| {
            synced.push(parent.to_owned());
            // Publication happened, but a failed final directory barrier is
            // still failure. Recovery owns these exact individually synced bytes.
            if parent == target {
                Err(io_error(
                    parent.display(),
                    io::Error::other("injected directory sync"),
                ))
            } else {
                sync_directory(parent)
            }
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected directory sync"));
        assert_eq!(synced, vec![staging.clone(), target.clone()]);
        assert_eq!(fs::read(target.join("a")).unwrap(), b"first");
        assert_eq!(fs::read(target.join("b")).unwrap(), b"second");
        let conflict = [(
            synced_private_stage(&staging.join("conflict"), b"replacement"),
            target.join("a"),
        )];
        assert_eq!(
            install_synced_private_staged_files(&conflict)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(target.join("a")).unwrap(), b"first");
        assert_eq!(fs::read(conflict[0].0.path()).unwrap(), b"replacement");
        let retry = [(
            synced_private_stage(&staging.join("c"), b"third"),
            target.join("c"),
        )];
        let mut synced = Vec::new();
        install_synced_private_staged_files_with(&retry, |parent| {
            synced.push(parent.to_owned());
            sync_directory(parent)
        })
        .unwrap();
        assert_eq!(synced, vec![staging, target.clone()]);
        assert_eq!(fs::read(target.join("c")).unwrap(), b"third");
    }

    #[cfg(unix)]
    #[test]
    fn synced_private_batch_rejects_changed_later_inode_before_publication() {
        let root = sinomo_testdir::TestDir::new("storage-synced-private-batch-mutation");
        let files = [
            (
                synced_private_stage(&root.join("first-stage"), b"first"),
                root.join("first"),
            ),
            (
                synced_private_stage(&root.join("second-stage"), b"second"),
                root.join("second"),
            ),
        ];
        fs::rename(files[1].0.path(), root.join("retained-original")).unwrap();
        write_private_stage(files[1].0.path(), b"second");
        assert_eq!(
            install_synced_private_staged_files(&files)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(!files[0].1.exists());
        assert!(!files[1].1.exists());
        assert_eq!(fs::read(files[0].0.path()).unwrap(), b"first");
    }

    #[cfg(unix)]
    #[test]
    fn synced_private_stage_rejects_identity_mode_and_content_mutation() {
        for mutation in ["replacement", "length", "same-size-content"] {
            let root = sinomo_testdir::TestDir::new("storage-synced-private-mutation");
            let path = root.join("staged");
            let stage = synced_private_stage(&path, b"initial bytes");
            match mutation {
                "replacement" => {
                    fs::rename(&path, root.join("original")).unwrap();
                    write_private_stage(&path, b"initial bytes");
                }
                "length" => {
                    OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .unwrap()
                        .write_all(b"changed length")
                        .unwrap();
                }
                "same-size-content" => {
                    let mut alias = OpenOptions::new().write(true).open(&path).unwrap();
                    alias.write_all(b"changed bytes").unwrap();
                    alias.set_modified(SystemTime::UNIX_EPOCH).unwrap();
                }
                _ => unreachable!(),
            }
            let destination = root.join("destination");
            let error = install_synced_private_staged_file(&stage, &destination)
                .expect_err("changed stage cannot reuse its old sync capability");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{mutation}");
            assert!(
                !destination.exists(),
                "{mutation} must fail before publication"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn synced_private_stage_rejects_a_mismatched_path_and_changes_during_sync() {
        let root = sinomo_testdir::TestDir::new("storage-synced-private-finalization-fence");
        let first = root.join("first");
        let second = root.join("second");
        let file = create_private_staged_file(&first).unwrap();
        write_private_stage(&second, b"other inode");
        assert_eq!(
            sync_private_staged_file(&second, file).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&first)
            .unwrap();
        let error = sync_private_staged_file_with(&first, file, |file| {
            file.sync_all()?;
            file.set_len(1)
        })
        .expect_err("a post-sync mutation cannot be captured as synchronized content");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn staged_install_publishes_content_and_stage_mode() {
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
            0o644
        );
    }

    #[cfg(windows)]
    #[test]
    fn replace_file_retries_a_transient_destination_sharing_violation() {
        use std::os::windows::fs::OpenOptionsExt as _;
        const FILE_SHARE_READ: u32 = 1;
        const FILE_SHARE_WRITE: u32 = 2;

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
    fn staged_install_publishes_stage_with_its_existing_mode() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-read-only");
        let destination = root.join("destination.bin");
        let staged = root.join(".destination.staged");
        fs::write(&destination, b"old content").expect("write destination");
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o400)).unwrap();
        write_private_stage(&staged, b"new content");
        fs::set_permissions(&staged, fs::Permissions::from_mode(0o600)).unwrap();
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
            0o600
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
    fn staged_install_no_clobber_moves_across_directories_on_one_filesystem() {
        let root = sinomo_testdir::TestDir::new("storage-staged-install-cross-directory");
        let staging = root.join("staging");
        let destination_dir = root.join("destination");
        fs::create_dir_all(&staging).expect("staging directory");
        fs::create_dir_all(&destination_dir).expect("destination directory");
        let staged = staging.join("payload.bin");
        let destination = destination_dir.join("payload.bin");
        write_private_stage(&staged, b"cross-directory content");

        install_staged_file(&staged, &destination, false).expect("publish across directories");

        assert_eq!(
            fs::read(&destination).expect("read destination"),
            b"cross-directory content"
        );
        assert!(!staged.exists(), "successful rename consumes staged name");
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

    #[cfg(unix)]
    #[test]
    fn ordinary_directory_preserves_deployment_permissions_and_inheritance() {
        use std::os::unix::fs::MetadataExt;

        let root = sinomo_testdir::TestDir::new("storage-directory-deployment-policy");
        let shared = root.join("shared");
        fs::create_dir(&shared).unwrap();
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o2770)).unwrap();
        let before = fs::metadata(&shared).unwrap();
        ensure_directory(&shared).expect("ensure shared directory");
        let after = fs::metadata(&shared).unwrap();
        assert_eq!(after.mode() & 0o7777, 0o2770);
        assert_eq!(
            (after.ctime(), after.ctime_nsec()),
            (before.ctime(), before.ctime_nsec())
        );

        // Compare normal OS creation without changing the process-wide umask.
        let control = shared.join("control");
        fs::create_dir(&control).unwrap();
        let control = fs::metadata(&control).unwrap();
        let created = shared.join("created");
        create_directory(&created).expect("create using deployment policy");
        let created_metadata = fs::metadata(&created).unwrap();
        assert_eq!(created_metadata.mode(), control.mode());
        assert_eq!(created_metadata.gid(), control.gid());
        assert_eq!(
            create_directory(&created).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        let ensured = shared.join("ensured");
        ensure_directory(&ensured).expect("ensure missing directory");
        assert_eq!(fs::metadata(&ensured).unwrap().mode(), control.mode());
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

    #[cfg(unix)]
    #[test]
    fn private_directory_ensure_does_not_dirty_an_already_private_inode() {
        use std::os::unix::fs::MetadataExt;

        let root = sinomo_testdir::TestDir::new("storage-private-directory-idempotent");
        let path = root.join("private");
        ensure_private_directory(&path).expect("create private directory");
        let before = fs::symlink_metadata(&path).expect("initial metadata");
        ensure_private_directory(&path).expect("ensure existing directory");
        let after = fs::symlink_metadata(&path).expect("unchanged metadata");
        assert_eq!(
            (after.ctime(), after.ctime_nsec()),
            (before.ctime(), before.ctime_nsec()),
            "an idempotent permission check must not manufacture dirty metadata"
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o1700))
            .expect("set operator-selected special mode bit");
        ensure_private_directory(&path).expect("retain selected mode");
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o7777, 0o1700);
    }

    #[test]
    fn private_directory_rejects_non_directory_entry() {
        let root = sinomo_testdir::TestDir::new("storage-private-directory-file");
        let path = root.join("not-a-directory");
        fs::write(&path, b"content").expect("write file");

        let error = ensure_private_directory(&path).expect_err("file must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            ensure_directory(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
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
            ensure_directory(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
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
    fn stage_inherits_creation_defaults() {
        let root = sinomo_testdir::TestDir::new("storage-private-staged-file");
        let staged = root.join("stage.bin");
        let file = create_private_staged_file(&staged).expect("create private stage");
        assert_eq!(
            file.metadata()
                .expect("stage metadata")
                .permissions()
                .mode()
                & 0o777,
            {
                let control = root.join("control");
                fs::write(&control, []).unwrap();
                fs::metadata(control).unwrap().mode() & 0o777
            }
        );
    }

    #[test]
    fn append_handle_can_be_durably_truncated() {
        let root = sinomo_testdir::TestDir::new("storage-append-truncate");
        let path = root.join("records.slog");
        let mut append = open_file_for_append(&path).expect("open append");
        append.file.write_all(b"first-second").expect("append");
        truncate_open_file_and_sync(&mut append.file, &path, 5, DurableFileSync::All)
            .expect("truncate append handle");
        append
            .file
            .write_all(b"-third")
            .expect("append after truncate");
        append.file.sync_all().expect("sync appended bytes");
        drop(append);
        assert_eq!(
            fs::read(&path).expect("read truncated file"),
            b"first-third"
        );
    }

    #[cfg(windows)]
    #[test]
    fn atomic_replace_normalizes_regular_windows_path_separators() {
        let root = sinomo_testdir::TestDir::new("storage-windows-mixed-separators");
        let path = root.join("nested/metadata/record.sobj");
        let options = DurableAtomicFileOptions::new("record.sobj", "tmp").sync_all();
        write_file_atomic(&path, b"first", options).expect("create through mixed separators");
        write_file_atomic(&path, b"second", options).expect("replace through mixed separators");
        assert_eq!(fs::read(path).expect("read replaced file"), b"second");
    }

    #[cfg(windows)]
    #[test]
    fn atomic_replace_supports_extended_length_temporary_paths() {
        let root = sinomo_testdir::TestDir::new("storage-windows-long-atomic");
        let parent = root.join("x".repeat(150));
        fs::create_dir_all(&parent).expect("create bounded parent");
        let path = parent.join("materialization-audit-intents.sobj");
        write_file_atomic(
            &path,
            b"durable",
            DurableAtomicFileOptions::new(
                "materialization-audit-intents.sobj",
                "cloud-files-materialization-audits",
            )
            .sync_all(),
        )
        .expect("replace through extended-length temporary path");
        assert_eq!(fs::read(path).expect("read installed file"), b"durable");
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
    fn sequential_record_reader_preserves_framing_limits_and_torn_tail() {
        let root = sinomo_testdir::TestDir::new("storage-record-batch");
        let path = root.join("records.slog");
        for endian in [DurableLengthEndian::Little, DurableLengthEndian::Big] {
            let payloads = [vec![1u8; 17], vec![2; 70_000], vec![3; 1]];
            let mut file = File::create(&path).unwrap();
            append_length_prefixed_records(
                &mut file,
                &path,
                &payloads,
                DurableRecordAppendOptions::new(endian),
            )
            .unwrap();
            let length = match endian {
                DurableLengthEndian::Little => 10u32.to_le_bytes(),
                DurableLengthEndian::Big => 10u32.to_be_bytes(),
            };
            file.write_all(&length).unwrap();
            file.write_all(b"torn").unwrap();
            drop(file);
            let options = DurableRecordReadOptions::new(endian).max_record_bytes(70_000);
            let mut serial = File::open(&path).unwrap();
            let mut batched =
                DurableRecordReader::new(File::open(&path).unwrap(), &path, options).unwrap();
            for expected in payloads {
                let record = batched.next_record().unwrap().unwrap();
                assert_eq!(record.bytes, expected);
                assert_eq!(
                    Some(record),
                    read_length_prefixed_record(&mut serial, &path, options).unwrap()
                );
            }
            for _ in 0..2 {
                assert!(batched.next_record().unwrap().is_none());
            }
            fs::write(&path, length).unwrap();
            let mut limited = DurableRecordReader::new(
                File::open(&path).unwrap(),
                &path,
                DurableRecordReadOptions::new(endian).max_record_bytes(9),
            )
            .unwrap();
            assert!(matches!(
                limited.next_record(),
                Err(DurableFileError::RecordTooLarge {
                    record_bytes: 10,
                    max_record_bytes: 9,
                    ..
                })
            ));
        }
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
