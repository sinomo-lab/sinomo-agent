//! DATA custody of the sole current native SnapshotRecord image.
//! A captured/received image carries no writer, configuration or author permit.
use crate::metadata_snapshot::{StagedSnapshot, decode_snapshot_image, stage_snapshot};
use crate::metadata_tables::{DfsMetadataTableState, DfsMetadataTables};
use crate::{DfsMetadataPrefixProof, DfsMetadataSnapshotImageDescriptor, MetadataBackendError};
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, MetadataBackendError>;
#[derive(Clone)]
pub struct DfsMetadataSnapshotImage(Arc<Image>);
struct Image {
    staged: Arc<StagedSnapshot>,
    descriptor: DfsMetadataSnapshotImageDescriptor,
    state: Mutex<Option<Arc<DfsMetadataTableState>>>,
}
impl std::fmt::Debug for DfsMetadataSnapshotImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfsMetadataSnapshotImage")
            .field("descriptor", &self.0.descriptor)
            .finish()
    }
}
impl PartialEq for DfsMetadataSnapshotImage {
    fn eq(&self, other: &Self) -> bool {
        self.descriptor() == other.descriptor()
    }
}
impl Eq for DfsMetadataSnapshotImage {}
impl Deref for DfsMetadataSnapshotImage {
    type Target = DfsMetadataSnapshotImageDescriptor;
    fn deref(&self) -> &Self::Target {
        self.descriptor()
    }
}
impl DfsMetadataSnapshotImage {
    pub fn descriptor(&self) -> &DfsMetadataSnapshotImageDescriptor {
        &self.0.descriptor
    }
    pub(crate) fn capture(
        root: &Path,
        tables: &DfsMetadataTables,
        sequence: u64,
        proof: &DfsMetadataPrefixProof,
    ) -> Result<Self> {
        let staged = Arc::new(stage_snapshot(root, tables, sequence, proof)?);
        let descriptor = staged.image_descriptor();
        Ok(Self(Arc::new(Image {
            staged,
            descriptor,
            state: Mutex::new(None),
        })))
    }
    pub(crate) fn capture_existing(root: &Path, source: File) -> Result<Self> {
        let (staged, state) = crate::metadata_snapshot::copy_verified_snapshot(root, source)?;
        let descriptor = staged.image_descriptor();
        Ok(Self(Arc::new(Image {
            staged: Arc::new(staged),
            descriptor,
            state: Mutex::new(Some(Arc::new(state))),
        })))
    }
    /// The caller holds the ordinary exclusive candidate writer. Keep every
    /// validated native record and its exact framing during hidden installation.
    pub(crate) fn install_candidate_copy(&self, root: &Path) -> Result<()> {
        self.stage_candidate_copy(root)?.install(root)
    }
    /// Keep the validated copy outside records/journal while their old state
    /// is archived. The caller still owns the native installation permit.
    pub(crate) fn stage_candidate_copy(&self, root: &Path) -> Result<StagedSnapshot> {
        self.validate()?;
        let destination = crate::metadata_table_store::metadata_table_snapshot_path(root);
        let parent = destination.parent().ok_or_else(|| {
            MetadataBackendError::Serde("snapshot candidate has no parent".into())
        })?;
        std::fs::create_dir_all(parent).map_err(|source| MetadataBackendError::Io {
            path: parent.to_owned(),
            source,
        })?;
        let source =
            File::open(self.0.staged.path()).map_err(|source| MetadataBackendError::Io {
                path: self.0.staged.path().to_owned(),
                source,
            })?;
        crate::metadata_snapshot::copy_snapshot_stream(root, source, self.descriptor())
    }
    pub fn attachment_source(
        &self,
    ) -> std::result::Result<
        sinomo_core::network::ObjectRpcBodySource,
        sinomo_core::network::NetworkError,
    > {
        self.0.staged.attachment_source()
    }
    pub(crate) fn state(&self) -> Result<Arc<DfsMetadataTableState>> {
        let mut state = self.0.state.lock().map_err(|e| {
            MetadataBackendError::Serde(format!("snapshot image state lock failed: {e}"))
        })?;
        if let Some(actual) = state.as_ref() {
            return Ok(Arc::clone(actual));
        }
        let path = self.0.staged.path();
        let file = File::open(path).map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?;
        let actual = Arc::new(decode_snapshot_image(file, Some(self.descriptor()))?);
        *state = Some(Arc::clone(&actual));
        Ok(actual)
    }
    pub fn validate(&self) -> Result<()> {
        self.state().map(|_| ())
    }
    #[cfg(any(test, feature = "storage-conversion"))]
    pub fn validate_file_system(&self, fs: &crate::DfsFileSystemId) -> Result<()> {
        let state = self.state()?;
        crate::metadata_backend::snapshot_validation::require_replica_snapshot_tables_file_system(
            &state.tables,
            fs,
        )?;
        crate::metadata_backend::snapshot_validation::require_version_pin_table_invariants(
            &state.tables,
        )
    }
    pub(crate) fn begin_received(
        root: &Path,
        descriptor: DfsMetadataSnapshotImageDescriptor,
    ) -> Result<SnapshotImageIngest> {
        descriptor.validate()?;
        let (staged, file) = StagedSnapshot::begin_received(root, &descriptor)?;
        Ok(SnapshotImageIngest {
            staged,
            file,
            descriptor,
            written: 0,
        })
    }
}
pub(crate) struct SnapshotImageIngest {
    // Close the writer before StagedSnapshot removes its path (Windows too).
    file: File,
    staged: StagedSnapshot,
    descriptor: DfsMetadataSnapshotImageDescriptor,
    written: u64,
}
impl SnapshotImageIngest {
    pub(crate) fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.written = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| {
                MetadataBackendError::Serde("snapshot attachment size overflow".into())
            })?;
        if self.written > self.descriptor.total_size_bytes {
            return Err(MetadataBackendError::Serde(
                "snapshot attachment exceeds declared image size".into(),
            ));
        }
        self.file
            .write_all(bytes)
            .map_err(|source| MetadataBackendError::Io {
                path: self.staged.path().to_owned(),
                source,
            })
    }
    pub(crate) fn finish(self) -> Result<DfsMetadataSnapshotImage> {
        if self.written != self.descriptor.total_size_bytes {
            return Err(MetadataBackendError::Serde(
                "snapshot attachment was incomplete".into(),
            ));
        }
        self.file
            .sync_all()
            .map_err(|source| MetadataBackendError::Io {
                path: self.staged.path().to_owned(),
                source,
            })?;
        drop(self.file);
        let image = DfsMetadataSnapshotImage(Arc::new(Image {
            staged: Arc::new(self.staged),
            descriptor: self.descriptor,
            state: Mutex::new(None),
        }));
        image.validate()?;
        Ok(image)
    }
}
