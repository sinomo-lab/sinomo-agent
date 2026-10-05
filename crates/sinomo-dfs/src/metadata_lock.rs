//! Metadata write locking for local SOBJ backends.
//!
//! The lock combines an in-process root guard with the durable journal
//! `write.lock` file, so multiple handles in one process and multiple
//! processes serialize writes through the same path.

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::create_dir_all;
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Condvar, Mutex, OnceLock};

static METADATA_WRITE_PATH_LOCKS: OnceLock<(Mutex<BTreeSet<PathBuf>>, Condvar)> = OnceLock::new();
static METADATA_WRITE_ACTIVITY: OnceLock<Mutex<BTreeMap<PathBuf, MetadataWriteActivity>>> =
    OnceLock::new();

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MetadataWriteActivity {
    pub(crate) epoch: u64,
    pub(crate) pending_writers: usize,
}

#[derive(Debug)]
pub(crate) struct MetadataWriteLock {
    _path_guard: MetadataPathWriteGuard,
    _file: fs::File,
}

/// Pin an existing writer generation for observational reads only. A busy or
/// absent owner is unavailable: inspection must not create storage or wait
/// outside the caller's admission/deadline contract.
pub(crate) fn try_metadata_inspection_lock(
    root: PathBuf,
    lock_path: PathBuf,
) -> Result<Option<MetadataWriteLock>> {
    let file = match fs::OpenOptions::new().read(true).open(&lock_path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: lock_path,
                source,
            });
        }
    };
    let Some(path_guard) = MetadataPathWriteGuard::try_lock(root) else {
        return Ok(None);
    };
    match FileExt::try_lock_shared(&file) {
        Ok(()) => {
            // A replaced lock inode cannot fence the current writer pathname.
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                let opened = file.metadata().map_err(|source| MetadataBackendError::Io {
                    path: lock_path.clone(),
                    source,
                })?;
                let named =
                    fs::metadata(&lock_path).map_err(|source| MetadataBackendError::Io {
                        path: lock_path.clone(),
                        source,
                    })?;
                if (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
                    return Ok(None);
                }
            }
            Ok(Some(MetadataWriteLock {
                _path_guard: path_guard,
                _file: file,
            }))
        }
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(source) => Err(MetadataBackendError::Io {
            path: lock_path,
            source,
        }),
    }
}

pub(crate) fn metadata_write_lock(root: PathBuf, lock_path: PathBuf) -> Result<MetadataWriteLock> {
    let mut activity = MetadataWriteActivityRegistration::begin(root.clone());
    let path_guard = MetadataPathWriteGuard::lock(root);
    activity.acquired();
    metadata_write_lock_after_path_guard(path_guard, lock_path)
}

/// Acquire the metadata writer lock for rebuildable checkpoint work.
///
/// Checkpoint admission observes ordinary writer waiters before rotating a
/// generation. The checkpoint itself must not register as foreground
/// activity or it would perpetually preempt its own soft-pressure request.
pub(crate) fn metadata_checkpoint_write_lock(
    root: PathBuf,
    lock_path: PathBuf,
) -> Result<MetadataWriteLock> {
    let path_guard = MetadataPathWriteGuard::lock(root);
    metadata_write_lock_after_path_guard(path_guard, lock_path)
}

fn metadata_write_lock_after_path_guard(
    path_guard: MetadataPathWriteGuard,
    lock_path: PathBuf,
) -> Result<MetadataWriteLock> {
    if let Some(parent) = lock_path.parent() {
        create_dir_all(parent.to_owned())?;
    };
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| MetadataBackendError::Io {
            path: lock_path.clone(),
            source,
        })?;
    FileExt::lock_exclusive(&file).map_err(|source| MetadataBackendError::Io {
        path: lock_path.clone(),
        source,
    })?;
    Ok(MetadataWriteLock {
        _path_guard: path_guard,
        _file: file,
    })
}

pub(crate) fn metadata_write_activity(root: &std::path::Path) -> MetadataWriteActivity {
    METADATA_WRITE_ACTIVITY
        .get()
        .and_then(|activity| {
            activity
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(root)
                .copied()
        })
        .unwrap_or_default()
}

struct MetadataWriteActivityRegistration {
    root: PathBuf,
    pending: bool,
}

impl MetadataWriteActivityRegistration {
    fn begin(root: PathBuf) -> Self {
        let activity = METADATA_WRITE_ACTIVITY.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut activity = activity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = activity.entry(root.clone()).or_default();
        entry.epoch = entry.epoch.saturating_add(1);
        entry.pending_writers = entry.pending_writers.saturating_add(1);
        Self {
            root,
            pending: true,
        }
    }

    fn acquired(&mut self) {
        if !self.pending {
            return;
        }
        self.pending = false;
        let Some(activity) = METADATA_WRITE_ACTIVITY.get() else {
            return;
        };
        let mut activity = activity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = activity.get_mut(&self.root) {
            entry.pending_writers = entry.pending_writers.saturating_sub(1);
        }
    }
}

impl Drop for MetadataWriteActivityRegistration {
    fn drop(&mut self) {
        self.acquired();
    }
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
    fn try_lock(path: PathBuf) -> Option<Self> {
        let (mutex, _) =
            METADATA_WRITE_PATH_LOCKS.get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
        let mut locked = mutex.try_lock().ok()?;
        if !locked.insert(path.clone()) {
            return None;
        }
        Some(Self { path })
    }

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
