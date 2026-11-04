//! Host-directory backend whose every operation is confined to one directory.
//!
//! The backend holds an open capability for its root directory and resolves
//! each path relative to it with `cap-std`. Symbolic links are followed only
//! while they stay beneath the root; a link that leads outside, an absolute
//! link, or a `..` walk above the root fails instead of reaching the host
//! filesystem. Because the root is an open directory handle, moving or
//! replacing the root path after the backend opens it cannot redirect I/O.

use super::{
    Result, VfsAppendOptions, VfsBackend, VfsCreateMode, VfsDirEntry, VfsError, VfsFileRead,
    VfsMetadata, VfsObjectKind, VfsRemoveOptions, VfsWriteOptions, join_vfs_path,
};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct ConfinedLocalVfsBackend {
    host_root: PathBuf,
    canonical_root: PathBuf,
    dir: Dir,
}

impl ConfinedLocalVfsBackend {
    /// Open `host_root` as the confinement boundary. This is the one ambient
    /// filesystem access the backend performs.
    pub fn open(host_root: impl Into<PathBuf>) -> Result<Self> {
        let host_root = host_root.into();
        let dir = Dir::open_ambient_dir(&host_root, ambient_authority()).map_err(|source| {
            VfsError::Io {
                path: host_root.display().to_string(),
                source,
            }
        })?;
        let canonical_root =
            std::fs::canonicalize(&host_root).unwrap_or_else(|_| host_root.clone());
        Ok(Self {
            host_root,
            canonical_root,
            dir,
        })
    }

    /// Host directory this backend was opened at. Diagnostic only: I/O always
    /// goes through the opened directory capability.
    pub fn host_root(&self) -> &Path {
        &self.host_root
    }

    /// Canonical form of [`Self::host_root`], resolved once when the backend
    /// opened. Callers use it to name the root, never to perform I/O.
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    fn scope(&self, root: &str) -> Result<Dir> {
        let relative = relative(root);
        let opened = if relative == Path::new(".") {
            self.dir.try_clone()
        } else {
            self.dir.open_dir(relative)
        };
        opened.map_err(|source| map_io(root, source))
    }

    fn beneath<'a>(root: &str, path: &'a str) -> Result<&'a str> {
        if root == "/" {
            return Ok(path);
        }
        let root = root.trim_end_matches('/');
        if path == root {
            return Ok("/");
        }
        path.strip_prefix(root)
            .filter(|tail| tail.starts_with('/'))
            .ok_or_else(|| VfsError::OutsideScope {
                root: root.to_string(),
                path: path.to_string(),
            })
    }

    fn open_parent<'a>(&self, root: &str, path: &'a str) -> Result<(Dir, &'a str)> {
        let tail = Self::beneath(root, path)?;
        let (parent, name) = split_parent(tail).ok_or_else(|| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "the scoped root itself is not a file".to_string(),
        })?;
        let scope = self.scope(root)?;
        let parent_dir = if parent == "/" {
            scope
        } else {
            scope
                .open_dir(relative(parent))
                .map_err(|source| map_io(path, source))?
        };
        Ok((parent_dir, name))
    }
}

impl VfsBackend for ConfinedLocalVfsBackend {
    fn backend_name(&self) -> &'static str {
        "local-confined"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        self.stat_confined("/", path)
    }

    fn lstat(&self, path: &str) -> Result<VfsMetadata> {
        let metadata = if path == "/" {
            self.dir.dir_metadata()
        } else {
            self.dir.symlink_metadata(relative(path))
        }
        .map_err(|source| map_io(path, source))?;
        Ok(metadata_for(path, &metadata))
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        self.list_confined("/", path)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let mut file = self
            .dir
            .open(relative(path))
            .map_err(|source| map_io(path, source))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| map_io(path, source))?;
        Ok(bytes)
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        self.read_range_confined("/", path, 0, max_bytes)
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        self.write_confined("/", path, bytes, options)
    }

    fn append(&self, path: &str, bytes: &[u8], options: &VfsAppendOptions) -> Result<()> {
        let mut open = OpenOptions::new();
        open.append(true).create(options.create);
        let mut file = self
            .dir
            .open_with(relative(path), &open)
            .map_err(|source| map_io(path, source))?;
        file.write_all(bytes)
            .map_err(|source| map_io(path, source))?;
        file.sync_all().map_err(|source| map_io(path, source))
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        self.create_dir_all_confined("/", path)
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.rename_confined("/", from, to)
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        self.remove_confined("/", path, options)
    }

    fn stat_confined(&self, root: &str, path: &str) -> Result<VfsMetadata> {
        let tail = Self::beneath(root, path)?;
        let scope = self.scope(root)?;
        let metadata = if tail == "/" {
            scope.dir_metadata()
        } else {
            scope.metadata(relative(tail))
        }
        .map_err(|source| map_io(path, source))?;
        Ok(metadata_for(path, &metadata))
    }

    fn list_confined(&self, root: &str, path: &str) -> Result<Vec<VfsDirEntry>> {
        let tail = Self::beneath(root, path)?;
        let scope = self.scope(root)?;
        let directory = if tail == "/" {
            scope
        } else {
            scope
                .open_dir(relative(tail))
                .map_err(|source| map_io(path, source))?
        };
        let mut entries = Vec::new();
        for entry in directory.entries().map_err(|source| map_io(path, source))? {
            let entry = entry.map_err(|source| map_io(path, source))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let child_path = join_vfs_path(path, &name);
            let file_type = entry
                .file_type()
                .map_err(|source| map_io(&child_path, source))?;
            let kind = if file_type.is_symlink() {
                VfsObjectKind::Symlink
            } else if file_type.is_dir() {
                VfsObjectKind::Directory
            } else if file_type.is_file() {
                VfsObjectKind::File
            } else {
                VfsObjectKind::Other
            };
            let size_bytes = if kind == VfsObjectKind::File {
                entry.metadata().ok().map(|metadata| metadata.len())
            } else {
                None
            };
            entries.push(VfsDirEntry {
                name,
                path: child_path,
                kind,
                size_bytes,
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    fn read_range_confined(
        &self,
        root: &str,
        path: &str,
        offset: u64,
        max_bytes: u64,
    ) -> Result<VfsFileRead> {
        let tail = Self::beneath(root, path)?;
        let scope = self.scope(root)?;
        let mut file = scope
            .open(relative(tail))
            .map_err(|source| map_io(path, source))?;
        let metadata = file.metadata().map_err(|source| map_io(path, source))?;
        if metadata.is_dir() {
            return Err(VfsError::IsDirectory {
                path: path.to_string(),
            });
        }
        let len = metadata.len();
        if offset > len {
            return Err(VfsError::InvalidPath {
                path: path.to_string(),
                detail: "read offset is beyond end of file".to_string(),
            });
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|source| map_io(path, source))?;
        let mut bytes = Vec::new();
        file.take(max_bytes)
            .read_to_end(&mut bytes)
            .map_err(|source| map_io(path, source))?;
        let end = offset.saturating_add(bytes.len() as u64);
        Ok(VfsFileRead {
            bytes,
            truncated: end < len,
        })
    }

    fn write_confined(
        &self,
        root: &str,
        path: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<()> {
        let (parent, name) = self.open_parent(root, path)?;
        let existing = match parent.symlink_metadata(name) {
            Ok(metadata) => Some(metadata),
            Err(source) if source.kind() == io::ErrorKind::NotFound => None,
            Err(source) => return Err(map_io(path, source)),
        };
        match (&options.create_mode, existing.as_ref()) {
            (VfsCreateMode::CreateNew, Some(_)) => {
                return Err(VfsError::AlreadyExists {
                    path: path.to_string(),
                });
            }
            (VfsCreateMode::ReplaceExisting, None) => {
                return Err(VfsError::NotFound {
                    path: path.to_string(),
                });
            }
            (_, Some(metadata)) if metadata.is_dir() => {
                return Err(VfsError::IsDirectory {
                    path: path.to_string(),
                });
            }
            _ => {}
        }
        if existing
            .as_ref()
            .is_some_and(|metadata| metadata.file_type().is_symlink())
        {
            // Write through a link that stays beneath the root so the link
            // itself survives. The open fails if the link leads outside.
            let mut open = OpenOptions::new();
            open.write(true).truncate(true);
            let mut file = parent
                .open_with(name, &open)
                .map_err(|source| map_io(path, source))?;
            file.write_all(bytes)
                .map_err(|source| map_io(path, source))?;
            return file.sync_all().map_err(|source| map_io(path, source));
        }
        if existing.is_none() && options.create_mode == VfsCreateMode::CreateNew {
            // Exclusive creation must fail if the name appears concurrently,
            // which a rename would not detect.
            let mut open = OpenOptions::new();
            open.write(true).create_new(true);
            let mut file = parent
                .open_with(name, &open)
                .map_err(|source| map_io(path, source))?;
            file.write_all(bytes)
                .map_err(|source| map_io(path, source))?;
            return file.sync_all().map_err(|source| map_io(path, source));
        }
        // Write atomically: a sibling temp file is synced, then is
        // renamed onto the target, so a
        // crash leaves either the previous content or the complete new one.
        let temp = format!(
            ".sinomo-vfs-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let result = (|| -> Result<()> {
            let mut open = OpenOptions::new();
            open.write(true).create_new(true);
            let mut file = parent
                .open_with(&temp, &open)
                .map_err(|source| map_io(path, source))?;
            file.write_all(bytes)
                .map_err(|source| map_io(path, source))?;
            file.sync_all().map_err(|source| map_io(path, source))?;
            drop(file);
            parent
                .rename(&temp, &parent, name)
                .map_err(|source| map_io(path, source))?;
            // Persist the directory entry change itself (best effort; not
            // all platforms support syncing a directory handle).
            #[cfg(unix)]
            if let Ok(directory) = parent.try_clone() {
                let _ = directory.into_std_file().sync_all();
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = parent.remove_file(&temp);
        }
        result
    }

    fn create_dir_all_confined(&self, root: &str, path: &str) -> Result<()> {
        let tail = Self::beneath(root, path)?;
        if tail == "/" {
            return Ok(());
        }
        self.scope(root)?
            .create_dir_all(relative(tail))
            .map_err(|source| map_io(path, source))
    }

    fn remove_confined(&self, root: &str, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let (parent, name) = self.open_parent(root, path)?;
        let result = if options.directory {
            parent.remove_dir(name)
        } else {
            match parent.symlink_metadata(name) {
                Ok(metadata) if metadata.is_dir() => {
                    return Err(VfsError::IsDirectory {
                        path: path.to_string(),
                    });
                }
                _ => parent.remove_file(name),
            }
        };
        match result {
            Ok(()) => Ok(()),
            Err(source) if options.missing_ok && source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(map_io(path, source)),
        }
    }

    fn rename_confined(&self, root: &str, from: &str, to: &str) -> Result<()> {
        let (from_parent, from_name) = self.open_parent(root, from)?;
        let (to_parent, to_name) = self.open_parent(root, to)?;
        if let Ok(metadata) = to_parent.symlink_metadata(to_name)
            && metadata.is_dir()
        {
            return Err(VfsError::IsDirectory {
                path: to.to_string(),
            });
        }
        from_parent
            .rename(from_name, &to_parent, to_name)
            .map_err(|source| map_io(from, source))
    }
}

fn relative(path: &str) -> &Path {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        Path::new(".")
    } else {
        Path::new(trimmed)
    }
}

fn split_parent(path: &str) -> Option<(&str, &str)> {
    let trimmed = path.trim_end_matches('/');
    let index = trimmed.rfind('/')?;
    let name = &trimmed[index + 1..];
    if name.is_empty() {
        return None;
    }
    let parent = if index == 0 { "/" } else { &trimmed[..index] };
    Some((parent, name))
}

fn metadata_for(path: &str, metadata: &Metadata) -> VfsMetadata {
    let kind = if metadata.file_type().is_symlink() {
        VfsObjectKind::Symlink
    } else if metadata.is_dir() {
        VfsObjectKind::Directory
    } else if metadata.is_file() {
        VfsObjectKind::File
    } else {
        VfsObjectKind::Other
    };
    VfsMetadata {
        path: path.to_string(),
        size_bytes: (kind == VfsObjectKind::File).then_some(metadata.len()),
        kind,
        object_id: None,
        content_hash: None,
        version: None,
    }
}

/// `cap-std` reports a resolution that would leave the root as a permission
/// error with this message; surface it as a scope violation.
const CAP_STD_ESCAPE_MESSAGE: &str = "outside of the filesystem";

fn map_io(path: &str, source: io::Error) -> VfsError {
    match source.kind() {
        io::ErrorKind::NotFound => VfsError::NotFound {
            path: path.to_string(),
        },
        io::ErrorKind::AlreadyExists => VfsError::AlreadyExists {
            path: path.to_string(),
        },
        io::ErrorKind::NotADirectory => VfsError::NotDirectory {
            path: path.to_string(),
        },
        io::ErrorKind::IsADirectory => VfsError::IsDirectory {
            path: path.to_string(),
        },
        io::ErrorKind::DirectoryNotEmpty => VfsError::DirectoryNotEmpty {
            path: path.to_string(),
        },
        io::ErrorKind::PermissionDenied if source.to_string().contains(CAP_STD_ESCAPE_MESSAGE) => {
            VfsError::OutsideScope {
                root: "/".to_string(),
                path: path.to_string(),
            }
        }
        _ => VfsError::Io {
            path: path.to_string(),
            source,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VfsHandle;
    use std::sync::Arc;

    fn handle(root: &Path) -> VfsHandle {
        VfsHandle::new(
            Arc::new(ConfinedLocalVfsBackend::open(root).expect("open confined root")),
            "/",
        )
        .expect("handle")
    }

    #[test]
    fn confined_backend_reads_writes_lists_renames_and_removes() {
        let root = sinomo_testdir::TestDir::new("sinomo-vfs-confined-basic");
        let vfs = handle(root.path());

        vfs.create_dir_all_confined("src/nested").expect("mkdir");
        vfs.write_confined(
            "src/nested/a.txt",
            b"hello",
            VfsWriteOptions::local_replace(),
        )
        .expect("create");
        let error = vfs
            .write_confined(
                "src/nested/a.txt",
                b"again",
                VfsWriteOptions::local_replace().create_new(),
            )
            .expect_err("create_new must not replace");
        assert!(matches!(error, VfsError::AlreadyExists { .. }));
        vfs.write_confined(
            "src/nested/a.txt",
            b"hello world",
            VfsWriteOptions::local_replace(),
        )
        .expect("replace");

        let read = vfs
            .read_range_confined("src/nested/a.txt", 6, 3)
            .expect("range");
        assert_eq!(read.bytes, b"wor");
        assert!(read.truncated);
        assert_eq!(
            vfs.stat_confined("src/nested/a.txt")
                .expect("stat")
                .size_bytes,
            Some(11)
        );
        let names = vfs
            .list_confined("src/nested")
            .expect("list")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a.txt"]);

        vfs.rename_confined("src/nested/a.txt", "src/b.txt")
            .expect("rename");
        assert_eq!(
            std::fs::read(root.join("src/b.txt")).expect("renamed on host"),
            b"hello world"
        );
        vfs.remove_confined("src/b.txt", VfsRemoveOptions::file())
            .expect("remove");
        assert!(!root.join("src/b.txt").exists());
        vfs.remove_confined("src/b.txt", VfsRemoveOptions::file().missing_ok())
            .expect("missing ok");
    }

    #[test]
    fn confined_backend_rejects_parent_traversal() {
        let parent = sinomo_testdir::TestDir::new("sinomo-vfs-confined-dotdot");
        std::fs::create_dir_all(parent.join("root")).expect("root");
        std::fs::write(parent.join("secret.txt"), b"secret").expect("secret");
        let vfs = handle(&parent.join("root"));

        assert!(vfs.read_range_confined("../secret.txt", 0, 64).is_err());
        assert!(
            vfs.write_confined("../secret.txt", b"x", VfsWriteOptions::local_replace())
                .is_err()
        );
        assert_eq!(
            std::fs::read(parent.join("secret.txt")).expect("secret intact"),
            b"secret"
        );
    }

    #[cfg(unix)]
    #[test]
    fn confined_backend_follows_inner_links_and_refuses_escaping_links() {
        use std::os::unix::fs::symlink;

        let parent = sinomo_testdir::TestDir::new("sinomo-vfs-confined-links");
        let root = parent.join("root");
        std::fs::create_dir_all(root.join("real")).expect("real dir");
        std::fs::write(root.join("real/inner.txt"), b"inner").expect("inner");
        std::fs::write(parent.join("secret.txt"), b"secret").expect("secret");
        symlink("real", root.join("alias")).expect("inner dir link");
        symlink("real/inner.txt", root.join("inner-link.txt")).expect("inner file link");
        symlink(parent.join("secret.txt"), root.join("escape-abs")).expect("absolute link");
        symlink("../secret.txt", root.join("escape-rel")).expect("relative escape");
        let vfs = handle(&root);

        assert_eq!(
            vfs.read_range_confined("alias/inner.txt", 0, 64)
                .expect("inner dir link")
                .bytes,
            b"inner"
        );
        vfs.write_confined(
            "inner-link.txt",
            b"updated",
            VfsWriteOptions::local_replace(),
        )
        .expect("write through inner link");
        assert_eq!(
            std::fs::read(root.join("real/inner.txt")).expect("target"),
            b"updated"
        );
        assert!(
            std::fs::symlink_metadata(root.join("inner-link.txt"))
                .expect("link kept")
                .file_type()
                .is_symlink()
        );

        for escape in ["escape-abs", "escape-rel"] {
            assert!(vfs.read_range_confined(escape, 0, 64).is_err(), "{escape}");
            assert!(vfs.stat_confined(escape).is_err(), "{escape}");
            assert!(
                vfs.write_confined(escape, b"pwned", VfsWriteOptions::local_replace())
                    .is_err(),
                "{escape}"
            );
        }
        symlink(&parent, root.join("escape-dir")).expect("dir escape");
        assert!(
            vfs.write_confined(
                "escape-dir/planted.txt",
                b"pwned",
                VfsWriteOptions::local_replace()
            )
            .is_err()
        );
        assert!(!parent.join("planted.txt").exists());
        assert_eq!(
            std::fs::read(parent.join("secret.txt")).expect("secret intact"),
            b"secret"
        );
    }

    #[cfg(unix)]
    #[test]
    fn confined_replace_uses_creation_defaults() {
        use std::os::unix::fs::PermissionsExt;

        let root = sinomo_testdir::TestDir::new("sinomo-vfs-confined-mode");
        std::fs::write(root.join("run.sh"), b"#!/bin/sh\n").expect("script");
        std::fs::set_permissions(root.join("run.sh"), std::fs::Permissions::from_mode(0o755))
            .expect("chmod");
        let vfs = handle(root.path());

        vfs.write_confined(
            "run.sh",
            b"#!/bin/sh\necho hi\n",
            VfsWriteOptions::local_replace(),
        )
        .expect("replace");

        let mode = std::fs::metadata(root.join("run.sh"))
            .expect("metadata")
            .permissions()
            .mode();
        std::fs::write(root.join("control"), []).unwrap();
        assert_eq!(mode, std::fs::metadata(root.join("control")).unwrap().permissions().mode());
    }

    #[cfg(windows)]
    #[test]
    fn confined_backend_refuses_escaping_junctions() {
        let parent = sinomo_testdir::TestDir::new("sinomo-vfs-confined-junction");
        let root = parent.join("root");
        let outside = parent.join("outside");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&outside).expect("outside");
        std::fs::write(outside.join("secret.txt"), b"secret").expect("secret");
        let created = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(root.join("escape"))
            .arg(&outside)
            .output()
            .expect("run mklink");
        assert!(created.status.success(), "mklink /J failed: {created:?}");
        let vfs = handle(&root);

        assert!(vfs.read_range_confined("escape/secret.txt", 0, 64).is_err());
        assert!(
            vfs.write_confined(
                "escape/planted.txt",
                b"pwned",
                VfsWriteOptions::local_replace()
            )
            .is_err()
        );
        assert!(!outside.join("planted.txt").exists());
        assert_eq!(
            std::fs::read(outside.join("secret.txt")).expect("secret intact"),
            b"secret"
        );
    }

    #[test]
    fn scoped_confined_handle_cannot_reach_sibling_scopes() {
        let root = sinomo_testdir::TestDir::new("sinomo-vfs-confined-scoped");
        std::fs::create_dir_all(root.join("a")).expect("a");
        std::fs::create_dir_all(root.join("b")).expect("b");
        std::fs::write(root.join("b/secret.txt"), b"secret").expect("secret");
        let scoped = handle(root.path()).scoped("/a").expect("scope");

        assert!(scoped.read_range_confined("/b/secret.txt", 0, 64).is_err());
        scoped
            .write_confined("note.txt", b"ok", VfsWriteOptions::local_replace())
            .expect("write in scope");
        assert_eq!(std::fs::read(root.join("a/note.txt")).expect("note"), b"ok");
    }
}
