//! Version-pinned, seekable input shared by archive and analysis readers.
use super::*;
use parquet::file::reader::{ChunkReader, Length};
use std::io::{self, BufReader, Read, Seek, SeekFrom};

#[derive(Clone)]
pub(crate) struct Backing {
    inner: Arc<Mutex<paged::Source>>,
    pub(super) closed: Arc<AtomicBool>,
    base: u64,
    size: u64,
}
impl Backing {
    pub(super) fn new(source: paged::Source) -> Self {
        let size = source.len();
        Self {
            inner: Arc::new(Mutex::new(source)),
            closed: Default::default(),
            base: 0,
            size,
        }
    }
    pub(super) fn memory(bytes: Vec<u8>) -> Self {
        Self::new(paged::Source::Memory(bytes))
    }
    pub(super) fn size(&self) -> u64 {
        self.size
    }
    fn read_at(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::other("Artifact view closed"));
        }
        let count = (self.size.saturating_sub(offset))
            .min(length as u64)
            .min(65536) as usize;
        if count == 0 {
            return Ok(vec![]);
        }
        self.inner
            .lock()
            .map_err(|_| io::Error::other("Artifact reader unavailable"))?
            .read(self.base + offset, count)
            .map_err(io::Error::other)
    }
    pub(super) fn slice(&self, offset: u64, size: u64) -> Result<Self, String> {
        if offset.checked_add(size).is_none_or(|end| end > self.size) {
            return Err("Archive member is outside file bounds".into());
        }
        Ok(Self {
            inner: self.inner.clone(),
            closed: self.closed.clone(),
            base: self.base + offset,
            size,
        })
    }
    pub(super) fn bytes(&self, limit: u64) -> Result<Vec<u8>, String> {
        if self.size > limit {
            return Err(format!(
                "Member exceeds the {} MiB inline preview limit",
                limit / 1024 / 1024
            ));
        }
        let mut bytes = vec![0; self.size as usize];
        self.reader().read_exact(&mut bytes).map_err(err)?;
        Ok(bytes)
    }
    pub(super) fn reader(&self) -> BufReader<RangeFile> {
        BufReader::with_capacity(
            65536,
            RangeFile {
                source: self.clone(),
                pos: 0,
            },
        )
    }
}
pub(crate) struct RangeFile {
    source: Backing,
    pos: u64,
}
impl Read for RangeFile {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let data = self.source.read_at(self.pos, out.len())?;
        out[..data.len()].copy_from_slice(&data);
        self.pos += data.len() as u64;
        Ok(data.len())
    }
}
impl Seek for RangeFile {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        self.pos = match to {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::End(n) => self.source.size.checked_add_signed(n),
            SeekFrom::Current(n) => self.pos.checked_add_signed(n),
        }
        .ok_or_else(|| io::Error::other("Invalid artifact offset"))?;
        Ok(self.pos)
    }
}
impl Length for Backing {
    fn len(&self) -> u64 {
        self.size
    }
}
impl ChunkReader for Backing {
    type T = BufReader<RangeFile>;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut r = self.reader();
        r.seek(SeekFrom::Start(start))?;
        Ok(r)
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if length > 128 * 1024 * 1024
            || start
                .checked_add(length as u64)
                .is_none_or(|n| n > self.size)
        {
            return Err(parquet::errors::ParquetError::General(
                "Parquet chunk exceeds the preview limit or file bounds".into(),
            ));
        }
        let mut data = vec![0; length];
        self.get_read(start)?.read_exact(&mut data)?;
        Ok(data.into())
    }
}
