use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
#[cfg(windows)]
use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};
use thiserror::Error;

pub const DEFAULT_OBJECT_FILE_MAX_READ_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_OBJECT_FILE_CACHE_CAPACITY: usize = 4096;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DurableFileError {
    #[error("storage I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
}

impl DurableFileError {
    pub fn path(&self) -> &str {
        match self {
            DurableFileError::Io { path, .. } => path,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            DurableFileError::Io { detail, .. } => detail,
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
            detail: format!("object file is {len} bytes; max is {max_read_bytes}"),
        });
    }

    let mut bytes = Vec::with_capacity(len as usize);
    let mut limited = file.take(max_read_bytes.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(path.display(), source))?;
    if bytes.len() as u64 > max_read_bytes {
        return Err(DurableFileError::Io {
            path: path.display().to_string(),
            detail: format!("object file read exceeded max of {max_read_bytes} bytes"),
        });
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
                detail: format!(
                    "replace failed with {}; additionally failed to remove existing file: {}",
                    source, remove_source
                ),
            })?;
            fs::rename(temp_path, path).map_err(|rename_source| DurableFileError::Io {
                path: format!("{} -> {}", temp_path.display(), path.display()),
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
    let mut bytes = Vec::with_capacity(expected_len as usize);
    let mut limited = file.take(expected_len.saturating_add(1));
    limited.read_to_end(&mut bytes).ok()?;
    (bytes.len() as u64 == expected_len).then_some(bytes)
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
    match sync {
        DurableFileSync::Data => file.sync_data(),
        DurableFileSync::All => file.sync_all(),
    }
    .map_err(|source| io_error(temp_path.display(), source))?;
    Ok(())
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

fn io_error(path: impl std::fmt::Display, source: impl ToString) -> DurableFileError {
    DurableFileError::Io {
        path: path.to_string(),
        detail: source.to_string(),
    }
}
