//! Metadata write locking for local SOBJ backends.
//!
//! The lock combines an in-process root guard with the durable journal
//! `write.lock` file, so multiple handles in one process and multiple
//! processes serialize writes through the same path.

use crate::metadata_backend::{MetadataBackendError, Result};
use crate::metadata_io::create_dir_all;
use fs2::FileExt;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::{Condvar, Mutex, OnceLock};

static METADATA_WRITE_PATH_LOCKS: OnceLock<(Mutex<BTreeSet<PathBuf>>, Condvar)> = OnceLock::new();

#[derive(Debug)]
pub(crate) struct MetadataWriteLock {
    _path_guard: MetadataPathWriteGuard,
    _file: fs::File,
}

pub(crate) fn metadata_write_lock(root: PathBuf, lock_path: PathBuf) -> Result<MetadataWriteLock> {
    let path_guard = MetadataPathWriteGuard::lock(root);
    if let Some(parent) = lock_path.parent() {
        create_dir_all(parent.to_owned())?;
    };
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| MetadataBackendError::Io {
            path: lock_path.clone(),
            source,
        })?;
    file.lock_exclusive()
        .map_err(|source| MetadataBackendError::Io {
            path: lock_path.clone(),
            source,
        })?;
    Ok(MetadataWriteLock {
        _path_guard: path_guard,
        _file: file,
    })
}

/// In-process mutual exclusion for metadata writes, keyed by backend root. Two
/// `LocalSobjMetadataBackend` handles on the same `root` serialize here;
/// handles on different roots never contend. Cross-process exclusion is layered
/// on top through the journal `write.lock` flock.
#[derive(Debug)]
struct MetadataPathWriteGuard {
    path: PathBuf,
}

impl MetadataPathWriteGuard {
    fn lock(path: PathBuf) -> Self {
        let (mutex, condvar) =
            METADATA_WRITE_PATH_LOCKS.get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
        let mut locked = mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !locked.insert(path.clone()) {
            locked = condvar
                .wait(locked)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        Self { path }
    }
}

impl Drop for MetadataPathWriteGuard {
    fn drop(&mut self) {
        let Some((mutex, condvar)) = METADATA_WRITE_PATH_LOCKS.get() else {
            return;
        };
        let mut locked = mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locked.remove(&self.path);
        condvar.notify_all();
    }
}
