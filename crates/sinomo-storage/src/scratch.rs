//! Cleanup for paths exclusively owned by an operation, rather than durable state.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Removes an operation's scratch path on errors, cancellation, and normal return.
/// The caller must own the entire path, including any directory contents.
#[derive(Debug)]
pub struct ScratchPath {
    path: PathBuf,
    armed: bool,
}

impl ScratchPath {
    pub fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ScratchPath {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = remove_owned_scratch_path(&self.path)
        {
            tracing::warn!(path = %self.path.display(), %error, "could not remove operation scratch path");
        }
    }
}

/// A unique scratch directory with a process-held lease. Cleanup after a crash
/// can take the same lease; concurrent operations never share writable paths.
#[derive(Debug)]
pub struct ScratchDirectory {
    _lease: File,
    cleanup: ScratchPath,
}

impl ScratchDirectory {
    pub fn create(parent: &Path, label: &str) -> io::Result<Self> {
        if label.is_empty()
            || !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid scratch label",
            ));
        }
        static NEXT: AtomicU64 = AtomicU64::new(0);
        fs::create_dir_all(parent)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = parent.join(format!(
            "{label}-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        let cleanup = ScratchPath::new(path.clone());
        let lease = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path.join(".lease.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lease)?;
        Ok(Self {
            _lease: lease,
            cleanup,
        })
    }

    pub fn path(&self) -> &Path {
        &self.cleanup.path
    }
}

/// Only for an owner-identified scratch directory. Returns false while its
/// operation still holds the lease, and never follows a root symlink.
pub fn remove_inactive_scratch_directory(path: &Path) -> io::Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() || is_reparse_or_symlink(&metadata) {
        return Ok(false);
    }
    let lock_path = path.join(".lease.lock");
    let lease = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
    {
        Ok(lease) => {
            if fs2::FileExt::try_lock_exclusive(&lease).is_err() {
                return Ok(false);
            }
            Some(lease)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    remove_owned_scratch_path(path)?;
    drop(lease);
    Ok(true)
}

/// Remove owner-selected scratch only. Windows Git packs may be read-only;
/// clear that attribute on files being discarded, never on retained paths.
pub fn remove_owned_scratch_path(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.is_dir() && !is_reparse_or_symlink(&metadata) {
        #[cfg(windows)]
        clear_readonly_files(path)?;
        fs::remove_dir_all(path)
    } else {
        #[cfg(windows)]
        if metadata.is_dir() {
            return fs::remove_dir(path);
        }
        #[cfg(windows)]
        if metadata.is_file()
            && !is_reparse_or_symlink(&metadata)
            && metadata.permissions().readonly()
        {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(path, permissions)?;
        }
        fs::remove_file(path)
    }
}

fn is_reparse_or_symlink(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

#[cfg(windows)]
fn clear_readonly_files(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        // Junctions and other reparse points are never traversed or chmodded.
        if metadata.file_attributes() & 0x400 != 0 {
            continue;
        }
        if metadata.is_dir() {
            clear_readonly_files(&entry.path())?;
        } else if metadata.permissions().readonly() {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(entry.path(), permissions)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_scratch_directories_are_isolated_and_leased() {
        let root = sinomo_testdir::TestDir::new("scratch-leases");
        let first = ScratchDirectory::create(&root, "download").unwrap();
        let second = ScratchDirectory::create(&root, "download").unwrap();
        assert_ne!(first.path(), second.path());
        assert!(!remove_inactive_scratch_directory(first.path()).unwrap());
        let path = first.path().to_path_buf();
        fs::write(path.join("partial"), b"unfinished").unwrap();
        drop(first);
        assert!(!path.exists());
        assert!(second.path().exists());
    }

    #[test]
    fn abandoned_scratch_directory_can_be_reaped_after_its_lease_is_released() {
        let root = sinomo_testdir::TestDir::new("scratch-restart-recovery");
        let mut scratch = ScratchDirectory::create(&root, "download").unwrap();
        let path = scratch.path().to_path_buf();
        fs::write(path.join("partial"), b"abandoned").unwrap();
        scratch.cleanup.disarm();
        drop(scratch);
        assert!(remove_inactive_scratch_directory(&path).unwrap());
        assert!(!path.exists());
    }

    #[test]
    fn scratch_cleanup_preserves_unrelated_files_and_renamed_output() {
        let root = sinomo_testdir::TestDir::new("scratch-output");
        let path = root.join("partial");
        fs::write(&path, b"artifact").unwrap();
        let guard = ScratchPath::new(path.clone());
        let output = root.join("durable");
        fs::rename(path, &output).unwrap();
        drop(guard);
        assert_eq!(fs::read(output).unwrap(), b"artifact");
    }

    #[cfg(windows)]
    #[test]
    fn scratch_cleanup_removes_readonly_git_objects() {
        let root = sinomo_testdir::TestDir::new("scratch-readonly");
        let scratch = ScratchDirectory::create(&root, "build").unwrap();
        let path = scratch.path().to_path_buf();
        let file = path.join("pack.idx");
        fs::write(&file, b"pack").unwrap();
        let mut permissions = fs::metadata(&file).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(file, permissions).unwrap();
        drop(scratch);
        assert!(!path.exists());
    }
}
