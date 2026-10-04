//! Opt-in logical file I/O spans consumed by Observe. No paths or payloads are
//! recorded. Byte counts are successful transfers (including partial failures),
//! not physical device bytes; operation counts count these API calls.
use std::fs::File;
use std::io::{self, Read, Write};

fn span(kind: &'static str) -> tracing::Span {
    tracing::trace_span!(target: "sinomo.perf", "storage.io", stage = "storage.io",
        io_kind = kind, io_bytes = tracing::field::Empty, outcome = "error")
}

pub fn read(file: &mut File, bytes: &mut [u8]) -> io::Result<usize> {
    let span = span("disk.read");
    let _entered = span.enter();
    let result = file.read(bytes);
    span.record("io_bytes", result.as_ref().copied().unwrap_or(0) as u64);
    if result.is_ok() {
        span.record("outcome", "ok");
    }
    result
}

pub fn write_all(file: &mut File, mut bytes: &[u8]) -> io::Result<()> {
    let span = span("disk.write");
    // Preserve the uninstrumented standard-library path when disabled.
    if span.is_disabled() {
        return file.write_all(bytes);
    }
    let _entered = span.enter();
    let mut written = 0u64;
    let result = (|| {
        while !bytes.is_empty() {
            match file.write(bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    written += n as u64;
                    bytes = &bytes[n..];
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    })();
    span.record("io_bytes", written);
    if result.is_ok() {
        span.record("outcome", "ok");
    }
    result
}

pub fn sync_all(file: &File) -> io::Result<()> {
    sync("disk.sync_all", || file.sync_all())
}

pub fn sync_data(file: &File) -> io::Result<()> {
    sync("disk.sync_data", || file.sync_data())
}

pub(crate) fn sync_directory(file: &File) -> io::Result<()> {
    sync("disk.sync_directory", || file.sync_all())
}

fn sync(kind: &'static str, operation: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    let span = span(kind);
    let _entered = span.enter();
    let result = operation();
    span.record("io_bytes", 0u64);
    if result.is_ok() {
        span.record("outcome", "ok");
    }
    result
}
