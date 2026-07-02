use super::{ContentStoreError, Result};
use sinomo_core::storage::durable_file;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

pub(super) fn create_dir_all(path: PathBuf) -> Result<()> {
    fs::create_dir_all(&path).map_err(|source| ContentStoreError::Io { path, source })
}

pub(super) fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

pub(super) fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

pub(super) fn recursive_file_count_and_bytes(path: &Path) -> Result<(u64, u64)> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let mut file_count = 0_u64;
    let mut total_bytes = 0_u64;
    for entry in read_dir(path)? {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            let (child_count, child_bytes) = recursive_file_count_and_bytes(&entry_path)?;
            file_count = file_count.saturating_add(child_count);
            total_bytes = total_bytes.saturating_add(child_bytes);
            continue;
        }
        if entry_path.is_file() {
            let len = entry_path
                .metadata()
                .map_err(|source| ContentStoreError::Io {
                    path: entry_path.clone(),
                    source,
                })?
                .len();
            file_count = file_count.saturating_add(1);
            total_bytes = total_bytes.saturating_add(len);
        }
    }
    Ok((file_count, total_bytes))
}

pub(super) fn projected_file_growth(path: &Path, new_len: u64) -> Result<u64> {
    let existing_len = match path.metadata() {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: path.to_owned(),
                source,
            });
        }
    };
    Ok(new_len.saturating_sub(existing_len))
}

pub(super) fn file_len_if_exists(path: &Path) -> Result<Option<u64>> {
    match path.metadata() {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
        Ok(_) => Ok(None),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

pub(super) fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

pub(super) fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

pub(super) fn remove_file_if_exists_and_sync_parent(path: &Path) -> Result<bool> {
    durable_file::remove_file_if_exists_and_sync_parent(path).map_err(content_durable_file_error)
}

pub(super) fn remove_empty_dir_if_exists_and_sync_parent(path: &Path) -> Result<bool> {
    durable_file::remove_empty_dir_if_exists_and_sync_parent(path)
        .map_err(content_durable_file_error)
}

pub(super) fn file_age_nanos(path: &Path, now: SystemTime) -> Result<u64> {
    let modified = path
        .metadata()
        .and_then(|metadata| metadata.modified())
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    Ok(now
        .duration_since(modified)
        .map(duration_nanos_saturating)
        .unwrap_or(0))
}

pub(super) fn duration_nanos_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn system_time_nanos_since_epoch_saturating(time: SystemTime) -> u64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(duration_nanos_saturating)
        .unwrap_or(0)
}

pub(super) fn write_reader_to_file<R: Read>(path: &Path, reader: &mut R) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let mut file = fs::File::create(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| ContentStoreError::Io {
                path: path.to_owned(),
                source,
            })?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])
            .map_err(|source| ContentStoreError::Io {
                path: path.to_owned(),
                source,
            })?;
    }
    file.sync_all().map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

pub(super) fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    durable_file::write_file_atomic(
        path,
        bytes,
        durable_file::DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
    )
    .map_err(content_durable_file_error)
}

pub(super) fn write_file_create_new_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    durable_file::write_file_create_new_atomic(
        path,
        bytes,
        durable_file::DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
    )
    .map_err(content_durable_file_error)
}

pub(super) fn write_files_create_new_relaxed<'a, I>(files: I) -> Result<()>
where
    I: IntoIterator<Item = (PathBuf, &'a [u8])>,
{
    durable_file::write_files_create_new_relaxed(
        files,
        durable_file::DurableAtomicFileOptions::new("tmp", "tmp"),
    )
    .map_err(content_durable_file_error)
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    durable_file::sync_directory(path).map_err(content_durable_file_error)
}

pub(super) fn content_durable_file_error(
    error: durable_file::DurableFileError,
) -> ContentStoreError {
    ContentStoreError::Io {
        path: PathBuf::from(error.path()),
        source: io::Error::new(error.kind(), error.detail().to_string()),
    }
}

pub(super) fn temporary_path_for(path: &Path) -> PathBuf {
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(1);
    let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tmp");
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        counter
    ))
}
