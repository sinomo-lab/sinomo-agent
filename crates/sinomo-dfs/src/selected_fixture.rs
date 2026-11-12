//! Physical identity of a selected native fixture layout: a fresh directory
//! carrying a marker, which default readers refuse. The selected layout's
//! routing and shard stages and the test-only local recovery fence publish
//! their files through it.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::DfsError;

const SELECTED_FIXTURE_MARKER: &str = "selected-original-data-fixture-v2.sobj";

fn fixture_error(message: &str) -> DfsError {
    DfsError::Metadata {
        message: format!("selected fixture layout: {message}"),
    }
}

fn io_error(path: &Path, error: std::io::Error) -> DfsError {
    fixture_error(&format!("{}: {error}", path.display()))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedFixtureMarker {
    version: u32,
    #[serde(rename = "queue_incarnation")]
    incarnation: String,
}

/// Representation admission for one fresh physical fixture directory. Not a
/// handoff, executor, fsync acknowledgement or serving capability.
#[derive(Clone, Debug)]
pub(crate) struct SelectedOriginalFixtureIdentity {
    root: PathBuf,
    marker: SelectedFixtureMarker,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    created: std::time::SystemTime,
}

impl SelectedOriginalFixtureIdentity {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Create `root` as a fresh physical directory with a new incarnation.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn create(root: PathBuf) -> Result<Self, DfsError> {
        let marker = SelectedFixtureMarker {
            version: 2,
            incarnation: crate::DfsVersion::generate().to_string(),
        };
        Self::admit(root, marker)
    }

    /// A separately admitted staging directory of the same fixture.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn admit_staging_root(&self, root: PathBuf) -> Result<Self, DfsError> {
        self.check()?;
        Self::admit(root, self.marker.clone())
    }

    #[cfg(any(test, feature = "test-support"))]
    fn admit(root: PathBuf, marker: SelectedFixtureMarker) -> Result<Self, DfsError> {
        fs::create_dir(&root).map_err(|error| io_error(&root, error))?;
        let metadata = fs::symlink_metadata(&root).map_err(|error| io_error(&root, error))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(fixture_error(
                "staging root is not a fresh physical directory",
            ));
        }
        crate::metadata_io::write_file_atomic(
            &root.join(SELECTED_FIXTURE_MARKER),
            &crate::metadata_io::encode_sobj(&marker)?,
        )?;
        if let Some(parent) = root.parent() {
            crate::metadata_io::sync_dir(parent)?;
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;
        let identity = Self {
            root,
            marker,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(not(unix))]
            created: metadata
                .created()
                .map_err(|error| fixture_error(&error.to_string()))?,
        };
        identity.check()?;
        Ok(identity)
    }

    /// Default readers refuse a root that carries a selected fixture marker.
    pub(crate) fn reject_default_root(root: &Path) -> Result<(), DfsError> {
        match fs::symlink_metadata(root.join(SELECTED_FIXTURE_MARKER)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error(root, error)),
            Ok(_) => Err(fixture_error(
                "default reader rejects selected original physical layout",
            )),
        }
    }

    pub(crate) fn check(&self) -> Result<(), DfsError> {
        let metadata =
            fs::symlink_metadata(&self.root).map_err(|error| io_error(&self.root, error))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(fixture_error("fixture is not its admitted directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            if (metadata.dev(), metadata.ino()) != (self.device, self.inode) {
                return Err(fixture_error("fixture was physically replaced"));
            }
        }
        #[cfg(not(unix))]
        if metadata
            .created()
            .map_err(|error| io_error(&self.root, error))?
            != self.created
        {
            return Err(fixture_error("fixture was physically replaced"));
        }
        let path = self.root.join(SELECTED_FIXTURE_MARKER);
        let before = fs::symlink_metadata(&path).map_err(|error| io_error(&path, error))?;
        if !before.is_file() || before.file_type().is_symlink() || before.len() > 4096 {
            return Err(fixture_error(
                "fixture marker is unavailable or unsupported",
            ));
        }
        let marker: SelectedFixtureMarker = crate::metadata_io::read_sobj(&path)?;
        let after = fs::symlink_metadata(&path).map_err(|error| io_error(&path, error))?;
        if marker != self.marker
            || after.file_type().is_symlink()
            || !same_file_observation(&before, &after)
        {
            return Err(fixture_error("fixture layout or incarnation changed"));
        }
        Ok(())
    }
}

/// Two observations of one regular file show the same physical file.
pub(crate) fn same_file_observation(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    if !left.is_file()
        || !right.is_file()
        || left.len() != right.len()
        || left.modified().ok() != right.modified().ok()
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if (left.dev(), left.ino(), left.ctime(), left.ctime_nsec())
            != (right.dev(), right.ino(), right.ctime(), right.ctime_nsec())
        {
            return false;
        }
    }
    true
}
