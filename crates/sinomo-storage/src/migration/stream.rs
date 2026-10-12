//! Bounded, checksummed staging stream for copy-on-write migrations.
//!
//! A durable job stores `StreamCursor` only after `checkpoint`. Reopen truncates
//! uncheckpointed output and resumes at that boundary. Completed streams are
//! independently verified before their owner atomically installs the file.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::durable_file::{create_private_staged_file, sync_directory};

const MAGIC: &[u8; 8] = b"SMIGLOG1";
const MAX_HEADER_BYTES: u64 = 1024 * 1024;
pub const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamCursor {
    pub offset: u64,
    pub records: u64,
    pub digest: String,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn digest(previous: &str, payload: &[u8]) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(previous.as_bytes());
    hash.update(&(payload.len() as u64).to_le_bytes());
    hash.update(payload);
    hash.finalize().to_hex().to_string()
}

fn digest_bytes(value: &str) -> io::Result<[u8; 32]> {
    blake3::Hash::from_hex(value)
        .map(|hash| *hash.as_bytes())
        .map_err(|error| invalid(error.to_string()))
}

fn read_u64(file: &mut File) -> io::Result<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn header(file: &mut File) -> io::Result<(Vec<u8>, StreamCursor)> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid("not a migration record stream"));
    }
    let len = read_u64(file)?;
    if len == 0 || len > MAX_HEADER_BYTES {
        return Err(invalid("invalid stream header length"));
    }
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes)?;
    let expected = digest("sinomo.storage.migration.stream.v1", &bytes);
    let mut checksum = [0; 32];
    file.read_exact(&mut checksum)?;
    if checksum != digest_bytes(&expected)? {
        return Err(invalid("stream header checksum mismatch"));
    }
    Ok((
        bytes,
        StreamCursor {
            offset: file.stream_position()?,
            records: 0,
            digest: expected,
        },
    ))
}

pub fn is_stream(path: &Path) -> io::Result<bool> {
    is_stream_file(&mut File::open(path)?)
}

/// Probe the same open generation that will be decoded, avoiding a pathname
/// race with atomic publication. Restores the caller's stream position.
pub fn is_stream_file(file: &mut File) -> io::Result<bool> {
    let position = file.stream_position()?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; 8];
    let result = match file.read_exact(&mut bytes) {
        Ok(()) => Ok(&bytes == MAGIC),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    };
    file.seek(SeekFrom::Start(position))?;
    result
}

pub fn read_header(path: &Path) -> io::Result<Vec<u8>> {
    header(&mut File::open(path)?).map(|(bytes, _)| bytes)
}

fn seek_checkpoint(
    file: &mut File,
    initial: &StreamCursor,
    cursor: &StreamCursor,
) -> io::Result<()> {
    if cursor.offset < initial.offset || file.metadata()?.len() < cursor.offset {
        return Err(invalid(
            "migration output is shorter than its durable cursor",
        ));
    }
    if cursor.offset == initial.offset && cursor != initial {
        return Err(invalid("invalid initial stream cursor"));
    }
    file.seek(SeekFrom::Start(cursor.offset - 32))?;
    let mut checksum = [0; 32];
    file.read_exact(&mut checksum)?;
    if checksum != digest_bytes(&cursor.digest)? {
        return Err(invalid(
            "migration output no longer matches its durable cursor",
        ));
    }
    Ok(())
}

pub struct MigrationStreamWriter {
    file: File,
    cursor: StreamCursor,
    finished: bool,
}

impl MigrationStreamWriter {
    pub fn resume(path: &Path, expected_header: &[u8], cursor: &StreamCursor) -> io::Result<Self> {
        if expected_header.is_empty() || expected_header.len() as u64 > MAX_HEADER_BYTES {
            return Err(invalid("invalid stream header length"));
        }
        let mut file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound && cursor.offset == 0 => {
                create_private_staged_file(path).map_err(io::Error::other)?
            }
            Err(error) => return Err(error),
        };
        let cursor = if cursor.offset == 0 {
            if cursor.records != 0 || !cursor.digest.is_empty() {
                return Err(invalid("invalid empty stream cursor"));
            }
            // No output has been checkpointed: a torn first header is safe to
            // replace. The caller holds the durable job's executor lock.
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(MAGIC)?;
            file.write_all(&(expected_header.len() as u64).to_le_bytes())?;
            file.write_all(expected_header)?;
            let checksum = digest("sinomo.storage.migration.stream.v1", expected_header);
            file.write_all(&digest_bytes(&checksum)?)?;
            file.sync_all()?;
            if let Some(parent) = path.parent() {
                sync_directory(parent).map_err(io::Error::other)?;
            }
            StreamCursor {
                offset: file.stream_position()?,
                records: 0,
                digest: checksum,
            }
        } else {
            let (actual_header, initial) = header(&mut file)?;
            if actual_header != expected_header {
                return Err(invalid("migration source header changed"));
            }
            seek_checkpoint(&mut file, &initial, cursor)?;
            file.set_len(cursor.offset)?;
            cursor.clone()
        };
        Ok(Self {
            file,
            cursor,
            finished: false,
        })
    }

    /// The actual already-computed framed stream identity; no read or sync.
    pub fn cursor(&self) -> &StreamCursor {
        &self.cursor
    }

    pub fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.finished || bytes.is_empty() || bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("invalid migration stream record"));
        }
        let records = self
            .cursor
            .records
            .checked_add(1)
            .ok_or_else(|| invalid("record count overflow"))?;
        let checksum = digest(&self.cursor.digest, bytes);
        self.file.write_all(&(bytes.len() as u64).to_le_bytes())?;
        self.file.write_all(bytes)?;
        self.file.write_all(&digest_bytes(&checksum)?)?;
        self.cursor = StreamCursor {
            offset: self.file.stream_position()?,
            records,
            digest: checksum,
        };
        Ok(())
    }

    pub fn checkpoint(&mut self) -> io::Result<StreamCursor> {
        self.file.sync_all()?;
        Ok(self.cursor.clone())
    }

    pub fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Err(invalid("stream already finished"));
        }
        self.file.write_all(&0_u64.to_le_bytes())?;
        self.file.write_all(&digest_bytes(&self.cursor.digest)?)?;
        self.file.sync_all()?;
        self.finished = true;
        Ok(())
    }
}

pub struct MigrationStreamReader {
    file: File,
    pub header: Vec<u8>,
    cursor: StreamCursor,
    finished: bool,
}

impl MigrationStreamReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::from_file(File::open(path)?)
    }

    pub fn from_file(mut file: File) -> io::Result<Self> {
        let (header, cursor) = header(&mut file)?;
        Ok(Self {
            file,
            header,
            cursor,
            finished: false,
        })
    }

    /// The owner must obtain this cursor from its durable verification job.
    pub fn resume(path: &Path, cursor: &StreamCursor) -> io::Result<Self> {
        let mut reader = Self::open(path)?;
        if cursor.offset != 0 {
            seek_checkpoint(&mut reader.file, &reader.cursor, cursor)?;
            reader.cursor = cursor.clone();
        }
        Ok(reader)
    }

    pub fn cursor(&self) -> &StreamCursor {
        &self.cursor
    }

    pub fn next_record_len(&mut self) -> io::Result<u64> {
        if self.finished {
            return Ok(0);
        }
        let position = self.file.stream_position()?;
        let len = read_u64(&mut self.file)?;
        self.file.seek(SeekFrom::Start(position))?;
        if len > MAX_RECORD_BYTES {
            return Err(invalid("migration stream record exceeds size limit"));
        }
        Ok(len)
    }

    pub fn next_record(&mut self, max_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        if self.finished {
            return Ok(None);
        }
        let len = read_u64(&mut self.file)?;
        if len > MAX_RECORD_BYTES || len > max_bytes as u64 {
            return Err(invalid("migration stream record exceeds read budget"));
        }
        let mut bytes = vec![0; len as usize];
        self.file.read_exact(&mut bytes)?;
        let expected = if len == 0 {
            self.cursor.digest.clone()
        } else {
            digest(&self.cursor.digest, &bytes)
        };
        let mut checksum = [0; 32];
        self.file.read_exact(&mut checksum)?;
        if checksum != digest_bytes(&expected)? {
            return Err(invalid("migration stream checksum mismatch"));
        }
        if len == 0 {
            let mut extra = [0; 1];
            if self.file.read(&mut extra)? != 0 {
                return Err(invalid("trailing bytes after migration stream footer"));
            }
            self.finished = true;
            return Ok(None);
        }
        self.cursor = StreamCursor {
            offset: self.file.stream_position()?,
            records: self
                .cursor
                .records
                .checked_add(1)
                .ok_or_else(|| invalid("record count overflow"))?,
            digest: expected,
        };
        Ok(Some(bytes))
    }
}
