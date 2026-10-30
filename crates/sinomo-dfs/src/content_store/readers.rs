use super::inline_bindings::{ContentReadSnapshot, InlineReaderLease, InlineStoreLease};
use super::{ContentExtent, ValidatedContentManifest};
use sinomo_core::storage::streaming_file::StreamingFileReader;
use std::fs;
use std::io::{self, Read};
use std::sync::Arc;

/// Streaming reader for a complete content-addressed blob.
///
/// The reader bounds each `Read` call to the known blob size so callers can
/// stream large DFS payloads without buffering the full blob in memory.
#[derive(Debug)]
pub struct BlobReader {
    file: Option<BlobFile>,
    size_bytes: u64,
    remaining_bytes: u64,
    store_lease: Option<InlineStoreLease>,
    inline_generation: Option<InlineReaderLease>,
}

#[derive(Debug)]
enum BlobFile {
    Plain(fs::File),
    Streaming(StreamingFileReader),
}

impl Read for BlobFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(file) => file.read(buffer),
            Self::Streaming(file) => file.read(buffer),
        }
    }
}

impl BlobReader {
    pub(super) fn new(file: fs::File, size_bytes: u64) -> Self {
        // Only the complete loose-blob path opts into consumed-page advice.
        // Inline/shared segment and bounded-range readers stay plain below.
        Self {
            file: Some(BlobFile::Streaming(StreamingFileReader::new(file))),
            size_bytes,
            remaining_bytes: size_bytes,
            store_lease: None,
            inline_generation: None,
        }
    }

    pub(super) fn with_remaining(file: fs::File, size_bytes: u64, remaining_bytes: u64) -> Self {
        Self {
            file: Some(BlobFile::Plain(file)),
            size_bytes,
            remaining_bytes,
            store_lease: None,
            inline_generation: None,
        }
    }

    pub(super) fn empty() -> Self {
        Self {
            file: None,
            size_bytes: 0,
            remaining_bytes: 0,
            store_lease: None,
            inline_generation: None,
        }
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(super) fn retain_store(mut self, lease: InlineStoreLease) -> Self {
        self.store_lease = Some(lease);
        self
    }

    pub(super) fn retain_inline_generation(mut self, lease: InlineReaderLease) -> Self {
        self.inline_generation = Some(lease);
        self
    }

    fn read_bounded(&mut self, buffer: &mut [u8], missing_file: &'static str) -> io::Result<usize> {
        if self.remaining_bytes == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other(missing_file))?;
        let read = sinomo_core::storage::io_trace::read(file, &mut buffer[..limit])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "retained content range ended before its declared size",
            ));
        }
        self.remaining_bytes = self.remaining_bytes.saturating_sub(read as u64);
        Ok(read)
    }
}

impl Read for BlobReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.read_bounded(buffer, "non-empty blob reader has no backing file")
    }
}

/// Streaming reader for a bounded range inside a content-addressed blob.
///
/// The reported `size_bytes` is the full blob size; `remaining_bytes` tracks
/// the bounded slice that is still available from this reader.
#[derive(Debug)]
pub struct BlobRangeReader {
    reader: BlobReader,
    offset: u64,
    snapshot: Option<Arc<ContentReadSnapshot>>,
}

impl BlobRangeReader {
    pub(super) fn new(file: fs::File, offset: u64, size_bytes: u64, remaining_bytes: u64) -> Self {
        Self {
            reader: BlobReader::with_remaining(file, size_bytes, remaining_bytes),
            offset,
            snapshot: None,
        }
    }

    pub(super) fn empty() -> Self {
        Self {
            reader: BlobReader::empty(),
            offset: 0,
            snapshot: None,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.reader.size_bytes()
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.reader.remaining_bytes
    }

    pub(super) fn retain_store(mut self, lease: InlineStoreLease) -> Self {
        self.reader = self.reader.retain_store(lease);
        self
    }

    pub(super) fn retain_inline_generation(mut self, lease: InlineReaderLease) -> Self {
        self.reader = self.reader.retain_inline_generation(lease);
        self
    }

    pub(super) fn retain_snapshot(mut self, snapshot: Arc<ContentReadSnapshot>) -> Self {
        self.snapshot = Some(snapshot);
        self
    }
}

impl Read for BlobRangeReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader
            .read_bounded(buffer, "non-empty blob range reader has no backing file")
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
        snapshot: Arc<ContentReadSnapshot>,
        size_bytes: u64,
        extents: Vec<ContentExtent>,
    ) -> Self {
        Self {
            size_bytes,
            inner: ObjectVersionReaderInner::Extents(ExtentReader {
                snapshot,
                extents: extents.into_iter(),
                current: None,
            }),
        }
    }

    pub(super) fn from_manifest_range(
        snapshot: Arc<ContentReadSnapshot>,
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
                snapshot,
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
    snapshot: Arc<ContentReadSnapshot>,
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
                    self.snapshot
                        .open_range(&extent.content_hash, extent.blob_offset, max_bytes)
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
    snapshot: Arc<ContentReadSnapshot>,
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
            .snapshot
            .open_range(&extent.content_hash, blob_offset, max_bytes)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};

    #[test]
    fn full_loose_reader_preserves_bytes_bounds_and_empty_reads() {
        let root = sinomo_testdir::TestDir::new("full-loose-reader-bounds");
        let path = root.join("blob");
        fs::write(&path, b"abcdef").unwrap();
        let mut reader = BlobReader::new(fs::File::open(&path).unwrap(), 4);
        assert!(matches!(&reader.file, Some(BlobFile::Streaming(_))));
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        let mut bytes = Vec::new();
        assert_eq!(reader.read_to_end(&mut bytes).unwrap(), 4);
        assert_eq!(bytes, b"abcd");
        assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
        assert_eq!(fs::read(path).unwrap(), b"abcdef");
    }

    #[test]
    fn positioned_inline_and_range_readers_remain_plain_and_bounded() {
        let root = sinomo_testdir::TestDir::new("plain-range-reader-bounds");
        let path = root.join("segment");
        fs::write(&path, b"prefixPAYLOADsuffix").unwrap();
        let mut inline_file = fs::File::open(&path).unwrap();
        inline_file.seek(SeekFrom::Start(6)).unwrap();
        let mut inline = BlobReader::with_remaining(inline_file, 7, 7);
        assert!(matches!(&inline.file, Some(BlobFile::Plain(_))));
        let mut bytes = Vec::new();
        inline.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"PAYLOAD");

        let mut range_file = fs::File::open(&path).unwrap();
        range_file.seek(SeekFrom::Start(8)).unwrap();
        let mut range = BlobRangeReader::new(range_file, 2, 7, 3);
        assert!(matches!(&range.reader.file, Some(BlobFile::Plain(_))));
        bytes.clear();
        range.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"YLO");
        assert_eq!(range.offset(), 2);
        assert_eq!(range.remaining_bytes(), 0);
        assert_eq!(fs::read(path).unwrap(), b"prefixPAYLOADsuffix");
    }

    #[test]
    fn both_reader_backings_reject_premature_eof() {
        let root = sinomo_testdir::TestDir::new("blob-reader-premature-eof");
        let path = root.join("blob");
        fs::write(&path, b"short").unwrap();
        for mut reader in [
            BlobReader::new(fs::File::open(&path).unwrap(), 6),
            BlobReader::with_remaining(fs::File::open(&path).unwrap(), 6, 6),
        ] {
            let mut bytes = Vec::new();
            let error = reader.read_to_end(&mut bytes).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(bytes, b"short");
            assert_eq!(reader.remaining_bytes, 1);
        }
    }
}
