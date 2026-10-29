//! Best-effort cache advice for explicitly one-pass file readers.
//!
//! Advice never changes file bytes or durability. Linux may retain advised
//! pages, particularly mapped or unwritten dirty pages; this is not a memory
//! limit or a promise that any page was reclaimed.

use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::Seek;
use std::io::{self, Read};

#[cfg(target_os = "linux")]
const RELEASE_BYTES: u64 = 8 * 1024 * 1024;

/// A sequential file reader that advises away completed aligned 8 MiB ranges.
///
/// Small files and incomplete leading/trailing ranges receive no advice. The
/// caller owns this file cursor; do not seek through a duplicated descriptor
/// while using the reader. On other platforms this is an ordinary file reader.
pub struct StreamingFileReader {
    file: File,
    #[cfg(target_os = "linux")]
    // Current cursor and first not-yet-advised aligned range. An unavailable
    // cursor or read error disables advice without changing Read behavior.
    progress: Option<(u64, u64)>,
}

impl StreamingFileReader {
    pub fn new(file: File) -> Self {
        #[cfg(target_os = "linux")]
        {
            let mut file = file;
            let progress = file.stream_position().ok().and_then(|position| {
                position
                    .checked_add(RELEASE_BYTES - 1)
                    .map(|rounded| (position, rounded / RELEASE_BYTES * RELEASE_BYTES))
            });
            Self { file, progress }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self { file }
        }
    }

    #[cfg(target_os = "linux")]
    fn consumed(&mut self, bytes: usize) {
        let Some((position, first_range)) = self.progress else {
            return;
        };
        let Some(end) = u64::try_from(bytes)
            .ok()
            .and_then(|bytes| position.checked_add(bytes))
        else {
            self.progress = None;
            return;
        };
        let completed_end = end / RELEASE_BYTES * RELEASE_BYTES;
        if completed_end > first_range {
            release_file_pages(&self.file, first_range, completed_end - first_range);
            self.progress = Some((end, completed_end));
        } else {
            self.progress = Some((end, first_range));
        }
    }
}

impl Read for StreamingFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let result = self.file.read(buffer);
        #[cfg(target_os = "linux")]
        match &result {
            Ok(bytes) => self.consumed(*bytes),
            Err(_) => self.progress = None,
        }
        result
    }
}

/// Advise that a file range is no longer needed in cache, without changing it.
///
/// Callers writing the file must perform their required output synchronization
/// first. This helper never flushes or syncs. Advice and its errors are ignored;
/// unwritten dirty pages are not guaranteed to be freed. Use aligned ranges for
/// effective advice. Zero length is skipped because Linux interprets it as the
/// entire remainder of the file. Non-Linux platforms perform no operation.
pub fn release_file_pages(file: &File, offset: u64, length: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let Some((offset, length)) = advice_range(offset, length) else {
            return;
        };
        // SAFETY: the borrowed File keeps its descriptor live; both offsets
        // were checked for the platform off_t representation. This is advice
        // only and supplies no pointer or file-content mutation.
        let _ = unsafe {
            libc::posix_fadvise(file.as_raw_fd(), offset, length, libc::POSIX_FADV_DONTNEED)
        };
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, offset, length);
}

#[cfg(target_os = "linux")]
fn advice_range(offset: u64, length: u64) -> Option<(libc::off_t, libc::off_t)> {
    if length == 0 {
        return None;
    }
    let end = offset.checked_add(length)?;
    let _ = libc::off_t::try_from(end).ok()?;
    Some((
        libc::off_t::try_from(offset).ok()?,
        libc::off_t::try_from(length).ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn reader_preserves_bytes_length_and_input_file() {
        let root = sinomo_testdir::TestDir::new("streaming-file-read");
        let path = root.join("input");
        let block = [0x5a; 64 * 1024];
        let mut output = File::create(&path).unwrap();
        for _ in 0..257 {
            output.write_all(&block).unwrap();
        }
        output.write_all(b"tail").unwrap();
        output.sync_all().unwrap();
        drop(output);
        let before = std::fs::metadata(&path).unwrap();
        let mut reader = StreamingFileReader::new(File::open(&path).unwrap());
        let mut buffer = [0_u8; 64 * 1024];
        let mut total = 0_u64;
        let mut actual = blake3::Hasher::new();
        loop {
            let count = reader.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            total += count as u64;
            actual.update(&buffer[..count]);
        }
        let mut expected = blake3::Hasher::new();
        for _ in 0..257 {
            expected.update(&block);
        }
        expected.update(b"tail");
        assert_eq!(actual.finalize(), expected.finalize());
        assert_eq!(total, before.len());
        assert_eq!(reader.read(&mut buffer).unwrap(), 0);
        let after = std::fs::metadata(&path).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(after.modified().unwrap(), before.modified().unwrap());
        let mut source = File::open(&path).unwrap();
        source.seek(SeekFrom::End(-4)).unwrap();
        let mut tail = [0; 4];
        source.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"tail");
        #[cfg(target_os = "linux")]
        assert_eq!(reader.progress, Some((total, 16 * 1024 * 1024)));
    }

    #[test]
    fn small_file_and_empty_read_preserve_normal_read_behavior() {
        let root = sinomo_testdir::TestDir::new("streaming-file-small");
        let path = root.join("input");
        std::fs::write(&path, b"small").unwrap();
        let mut reader = StreamingFileReader::new(File::open(&path).unwrap());
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        let mut bytes = Vec::new();
        assert_eq!(reader.read_to_end(&mut bytes).unwrap(), 5);
        assert_eq!(&bytes, b"small");
        #[cfg(target_os = "linux")]
        assert_eq!(reader.progress, Some((5, 0)));
    }

    #[test]
    fn read_error_is_not_hidden_as_eof_or_advice_failure() {
        let root = sinomo_testdir::TestDir::new("streaming-file-error");
        let path = root.join("input");
        std::fs::write(&path, b"unchanged").unwrap();
        let mut plain = File::options().write(true).open(&path).unwrap();
        let mut reader = StreamingFileReader::new(File::options().write(true).open(&path).unwrap());
        let mut byte = [0];
        let expected = plain.read(&mut byte).unwrap_err();
        let actual = reader.read(&mut byte).unwrap_err();
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.raw_os_error(), expected.raw_os_error());
        assert_eq!(std::fs::read(&path).unwrap(), b"unchanged");
        #[cfg(target_os = "linux")]
        assert!(reader.progress.is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn only_completed_aligned_ranges_are_advised() {
        let root = sinomo_testdir::TestDir::new("streaming-file-offset");
        let path = root.join("input");
        let file = File::create(&path).unwrap();
        file.set_len(3 * RELEASE_BYTES).unwrap();
        drop(file);
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(1)).unwrap();
        let mut reader = StreamingFileReader::new(file);
        assert_eq!(reader.progress, Some((1, RELEASE_BYTES)));
        let mut buffer = [0; 64 * 1024];
        for _ in 0..128 {
            reader.read_exact(&mut buffer).unwrap();
        }
        assert_eq!(reader.progress, Some((RELEASE_BYTES + 1, RELEASE_BYTES)));
        for _ in 0..128 {
            reader.read_exact(&mut buffer).unwrap();
        }
        assert_eq!(
            reader.progress,
            Some((2 * RELEASE_BYTES + 1, 2 * RELEASE_BYTES))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn explicit_advice_skips_zero_and_unrepresentable_ranges() {
        assert!(advice_range(0, 0).is_none());
        assert!(advice_range(u64::MAX, 1).is_none());
        assert!(advice_range(0, u64::MAX).is_none());
        assert!(advice_range(libc::off_t::MAX as u64, 1).is_none());
        assert_eq!(
            advice_range(RELEASE_BYTES, RELEASE_BYTES),
            Some((RELEASE_BYTES as libc::off_t, RELEASE_BYTES as libc::off_t))
        );
        let root = sinomo_testdir::TestDir::new("streaming-file-advice");
        let path = root.join("input");
        std::fs::write(&path, b"kept").unwrap();
        let file = File::open(&path).unwrap();
        release_file_pages(&file, 0, 0);
        release_file_pages(&file, u64::MAX, 1);
        assert_eq!(std::fs::read(path).unwrap(), b"kept");
    }
}
