use super::{ContentExtent, LocalContentStore, ValidatedContentManifest};
use std::fs;
use std::io::{self, Read};
use std::sync::Arc;

/// Streaming reader for a complete content-addressed blob.
///
/// The reader bounds each `Read` call to the known blob size so callers can
/// stream large DFS payloads without buffering the full blob in memory.
#[derive(Debug)]
pub struct BlobReader {
    file: fs::File,
    size_bytes: u64,
    remaining_bytes: u64,
}

impl BlobReader {
    pub(super) fn new(file: fs::File, size_bytes: u64) -> Self {
        Self {
            file,
            size_bytes,
            remaining_bytes: size_bytes,
        }
    }

    pub(super) fn with_remaining(file: fs::File, size_bytes: u64, remaining_bytes: u64) -> Self {
        Self {
            file,
            size_bytes,
            remaining_bytes,
        }
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl Read for BlobReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining_bytes == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let read = self.file.read(&mut buffer[..limit])?;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(read as u64);
        Ok(read)
    }
}

/// Streaming reader for a bounded range inside a content-addressed blob.
///
/// The reported `size_bytes` is the full blob size; `remaining_bytes` tracks
/// the bounded slice that is still available from this reader.
#[derive(Debug)]
pub struct BlobRangeReader {
    file: fs::File,
    offset: u64,
    size_bytes: u64,
    remaining_bytes: u64,
}

impl BlobRangeReader {
    pub(super) fn new(file: fs::File, offset: u64, size_bytes: u64, remaining_bytes: u64) -> Self {
        Self {
            file,
            offset,
            size_bytes,
            remaining_bytes,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.remaining_bytes
    }
}

impl Read for BlobRangeReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining_bytes == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let read = self.file.read(&mut buffer[..limit])?;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(read as u64);
        Ok(read)
    }
}

/// Streaming reader for an object version, whether stored as one blob or as
/// logical extents over multiple blobs.
#[derive(Debug)]
pub struct ObjectVersionReader {
    inner: ObjectVersionReaderInner,
    size_bytes: u64,
}

#[derive(Debug)]
enum ObjectVersionReaderInner {
    Blob(BlobReader),
    Range(BlobRangeReader),
    Extents(ExtentReader),
    ManifestRange(ManifestExtentRangeReader),
}

impl ObjectVersionReader {
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn from_blob(reader: BlobReader) -> Self {
        Self {
            size_bytes: reader.size_bytes(),
            inner: ObjectVersionReaderInner::Blob(reader),
        }
    }

    pub fn from_blob_range(reader: BlobRangeReader) -> Self {
        let size_bytes = reader.remaining_bytes();
        Self {
            size_bytes,
            inner: ObjectVersionReaderInner::Range(reader),
        }
    }

    pub(super) fn from_extents(
        store: LocalContentStore,
        size_bytes: u64,
        extents: Vec<ContentExtent>,
    ) -> Self {
        Self {
            size_bytes,
            inner: ObjectVersionReaderInner::Extents(ExtentReader {
                store,
                extents: extents.into_iter(),
                current: None,
            }),
        }
    }

    pub(super) fn from_manifest_range(
        store: LocalContentStore,
        manifest: Arc<ValidatedContentManifest>,
        size_bytes: u64,
        next_extent_index: usize,
        end_extent_index: usize,
        range_start: u64,
        range_end: u64,
    ) -> Self {
        Self {
            size_bytes,
            inner: ObjectVersionReaderInner::ManifestRange(ManifestExtentRangeReader {
                store,
                manifest,
                next_extent_index,
                end_extent_index,
                range_start,
                range_end,
                current: None,
            }),
        }
    }
}

impl Read for ObjectVersionReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            ObjectVersionReaderInner::Blob(reader) => reader.read(buffer),
            ObjectVersionReaderInner::Range(reader) => reader.read(buffer),
            ObjectVersionReaderInner::Extents(reader) => reader.read(buffer),
            ObjectVersionReaderInner::ManifestRange(reader) => reader.read(buffer),
        }
    }
}

#[derive(Debug)]
struct ExtentReader {
    store: LocalContentStore,
    extents: std::vec::IntoIter<ContentExtent>,
    current: Option<BlobRangeReader>,
}

impl Read for ExtentReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let mut total_read = 0;
        while total_read < buffer.len() {
            if self.current.is_none() {
                let Some(extent) = self.extents.next() else {
                    break;
                };
                let max_bytes = usize::try_from(extent.size_bytes).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("extent is too large to read: {} bytes", extent.size_bytes),
                    )
                })?;
                self.current = Some(
                    self.store
                        .open_blob_range_stream(&extent.content_hash, extent.blob_offset, max_bytes)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
                );
            }
            let Some(reader) = self.current.as_mut() else {
                break;
            };
            let read = reader.read(&mut buffer[total_read..])?;
            if read == 0 {
                self.current = None;
                continue;
            }
            total_read += read;
            if reader.remaining_bytes() == 0 {
                self.current = None;
            }
        }
        Ok(total_read)
    }
}

#[derive(Debug)]
struct ManifestExtentRangeReader {
    store: LocalContentStore,
    manifest: Arc<ValidatedContentManifest>,
    next_extent_index: usize,
    end_extent_index: usize,
    range_start: u64,
    range_end: u64,
    current: Option<BlobRangeReader>,
}

impl ManifestExtentRangeReader {
    fn open_next_extent(&mut self) -> io::Result<Option<BlobRangeReader>> {
        if self.next_extent_index >= self.end_extent_index {
            return Ok(None);
        }
        let extent = self
            .manifest
            .manifest()
            .extents
            .get(self.next_extent_index)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "validated extent range exceeds the manifest",
                )
            })?;
        self.next_extent_index += 1;
        let extent_end = extent
            .logical_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "extent logical range overflow")
            })?;
        let logical_start = self.range_start.max(extent.logical_offset);
        let logical_end = self.range_end.min(extent_end);
        let extent_delta = logical_start
            .checked_sub(extent.logical_offset)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "extent range starts before extent",
                )
            })?;
        let blob_offset = extent
            .blob_offset
            .checked_add(extent_delta)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "extent blob offset overflow")
            })?;
        let size_bytes = logical_end.saturating_sub(logical_start);
        let max_bytes = usize::try_from(size_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("extent is too large to read: {size_bytes} bytes"),
            )
        })?;
        let reader = self
            .store
            .open_blob_range_stream(&extent.content_hash, blob_offset, max_bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if reader.remaining_bytes() != size_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "extent source is shorter than manifest: hash={}, expected={size_bytes}, actual={}",
                    extent.content_hash,
                    reader.remaining_bytes()
                ),
            ));
        }
        Ok(Some(reader))
    }
}

impl Read for ManifestExtentRangeReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let mut total_read = 0;
        while total_read < buffer.len() {
            if self.current.is_none() {
                self.current = self.open_next_extent()?;
                if self.current.is_none() {
                    break;
                }
            }
            let Some(reader) = self.current.as_mut() else {
                break;
            };
            let read = reader.read(&mut buffer[total_read..])?;
            if read == 0 {
                self.current = None;
                continue;
            }
            total_read += read;
            if reader.remaining_bytes() == 0 {
                self.current = None;
            }
        }
        Ok(total_read)
    }
}
