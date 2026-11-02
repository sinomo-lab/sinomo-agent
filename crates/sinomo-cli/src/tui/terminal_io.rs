//! Session-owned capture for output that bypasses the structured logging API.
//! The renderer keeps a separate terminal handle; captured output is never printed here.
use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

const RECORDS: usize = 256;
const LINE_BYTES: usize = 4096;
static OUTPUT_OWNER: AtomicBool = AtomicBool::new(false);
struct OutputLease;
impl Drop for OutputLease {
    fn drop(&mut self) {
        OUTPUT_OWNER.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct OutputLine {
    pub stream: &'static str,
    pub text: String,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct OutputSnapshot {
    pub lines: VecDeque<OutputLine>,
    pub evicted: u64,
}

impl OutputSnapshot {
    fn push(&mut self, stream: &'static str, bytes: &[u8], truncated: bool) {
        if self.lines.len() == RECORDS {
            self.lines.pop_front();
            self.evicted = self.evicted.saturating_add(1);
        }
        self.lines.push_back(OutputLine {
            stream,
            text: String::from_utf8_lossy(&bytes[..bytes.len().min(LINE_BYTES)]).into_owned(),
            truncated: truncated || bytes.len() > LINE_BYTES,
        });
    }
}

pub(super) struct StdioRestore {
    stdout: platform::Descriptor,
    stderr: platform::Descriptor,
    restored: AtomicBool,
}

impl StdioRestore {
    pub fn restore(&self) {
        if !self.restored.swap(true, Ordering::AcqRel) {
            let _ = io::stdout().flush();
            let _ = io::stderr().flush();
            let _ = self.stdout.redirect(1);
            let _ = self.stderr.redirect(2);
        }
    }
}

pub(super) struct TerminalOutput {
    _lease: OutputLease,
    restore: Arc<StdioRestore>,
    stop: Arc<AtomicBool>,
    records: Arc<Mutex<OutputSnapshot>>,
    version: Arc<AtomicU64>,
    readers: Vec<JoinHandle<()>>,
}

impl TerminalOutput {
    pub fn start() -> io::Result<(Self, File)> {
        if OUTPUT_OWNER
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "terminal output already owned",
            ));
        }
        let lease = OutputLease;
        io::stdout().flush()?;
        io::stderr().flush()?;
        let renderer = platform::Descriptor::duplicate(1)?.into_file()?;
        let restore = Arc::new(StdioRestore {
            stdout: platform::Descriptor::duplicate(1)?,
            stderr: platform::Descriptor::duplicate(2)?,
            restored: AtomicBool::new(false),
        });
        let mut output = Self {
            _lease: lease,
            restore,
            stop: Arc::new(AtomicBool::new(false)),
            records: Arc::new(Mutex::new(OutputSnapshot::default())),
            version: Arc::new(AtomicU64::new(0)),
            readers: Vec::new(),
        };
        // Allocate both pipes before redirecting either stream. The guard restores
        // partial setup if a redirect or thread spawn fails.
        let (stdout, stdout_writer) = platform::pipe()?;
        let (stderr, stderr_writer) = platform::pipe()?;
        stdout_writer.redirect(1)?;
        stderr_writer.redirect(2)?;
        for (reader, stream) in [(stdout, "stdout"), (stderr, "stderr")] {
            let stop = Arc::clone(&output.stop);
            let records = Arc::clone(&output.records);
            let version = Arc::clone(&output.version);
            output.readers.push(
                thread::Builder::new()
                    .name(format!("tui-{stream}"))
                    .spawn(move || read_output(reader, stream, &stop, &records, &version))?,
            );
        }
        Ok((output, renderer))
    }

    pub fn restorer(&self) -> Arc<StdioRestore> {
        Arc::clone(&self.restore)
    }

    pub fn snapshot(&self, version: &mut u64) -> Option<OutputSnapshot> {
        let current = self.version.load(Ordering::Acquire);
        if current == *version {
            return None;
        }
        let records = self
            .records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        *version = current;
        Some(records)
    }
}

impl Drop for TerminalOutput {
    fn drop(&mut self) {
        self.restore.restore();
        self.stop.store(true, Ordering::Release);
        for reader in self.readers.drain(..) {
            reader.thread().unpark();
            let _ = reader.join();
        }
    }
}

fn read_output(
    mut file: File,
    stream: &'static str,
    stop: &AtomicBool,
    records: &Mutex<OutputSnapshot>,
    version: &AtomicU64,
) {
    let mut pending = Vec::with_capacity(LINE_BYTES);
    let mut truncated = false;
    let mut bytes = [0u8; 4096];
    // A descendant may retain a pipe, or keep writing after the TUI exits. Never
    // wait for that process or for EOF during teardown; drain only a bounded tail.
    let mut final_budget = 64 * 1024;
    let push = |line: &[u8], truncated| {
        if !line.is_empty() || truncated {
            records
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(stream, line, truncated);
            version.fetch_add(1, Ordering::Release);
        }
    };
    loop {
        let stopping = stop.load(Ordering::Acquire);
        match platform::read_ready(&mut file, &mut bytes) {
            Ok(Some(0)) => break,
            Ok(Some(n)) => {
                for b in &bytes[..n] {
                    if *b == b'\n' {
                        push(&pending, truncated);
                        pending.clear();
                        truncated = false;
                    } else if pending.len() < LINE_BYTES {
                        pending.push(*b);
                    } else {
                        truncated = true;
                    }
                }
                if stopping {
                    if n >= final_budget {
                        push(b"capture tail omitted during shutdown", true);
                        break;
                    }
                    final_budget -= n;
                }
            }
            Ok(None) if stopping => break,
            Ok(None) => {
                // Surface unterminated warnings too; bound each fragment as above.
                push(&pending, truncated);
                pending.clear();
                truncated = false;
                thread::park_timeout(Duration::from_millis(100));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                push(format!("output capture failed: {error}").as_bytes(), false);
                break;
            }
        }
    }
    push(&pending, truncated);
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    pub struct Descriptor(OwnedFd);
    impl Descriptor {
        pub fn duplicate(fd: i32) -> io::Result<Self> {
            let fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self(unsafe { OwnedFd::from_raw_fd(fd) }))
        }
        pub fn redirect(&self, target: i32) -> io::Result<()> {
            if unsafe { libc::dup2(self.0.as_raw_fd(), target) } < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
        pub fn into_file(self) -> io::Result<File> {
            Ok(File::from(self.0))
        }
    }
    pub fn pipe() -> io::Result<(File, Descriptor)> {
        let mut fds = [-1; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        for fd in [&reader, &writer] {
            if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        if unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((File::from(reader), Descriptor(writer)))
    }
    pub fn read_ready(file: &mut File, bytes: &mut [u8]) -> io::Result<Option<usize>> {
        match file.read(bytes) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            other => other.map(Some),
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::{
        ffi::c_void,
        os::windows::io::{AsRawHandle, FromRawHandle},
    };
    unsafe extern "C" {
        fn _dup(fd: i32) -> i32;
        fn _dup2(from: i32, to: i32) -> i32;
        fn _close(fd: i32) -> i32;
        fn _pipe(fds: *mut i32, size: u32, mode: i32) -> i32;
        fn _get_osfhandle(fd: i32) -> isize;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetStdHandle(kind: u32, handle: *mut c_void) -> i32;
        fn SetHandleInformation(handle: *mut c_void, mask: u32, flags: u32) -> i32;
        fn GetCurrentProcess() -> *mut c_void;
        fn DuplicateHandle(
            process: *mut c_void,
            source: *mut c_void,
            target_process: *mut c_void,
            target: *mut *mut c_void,
            access: u32,
            inherit: i32,
            options: u32,
        ) -> i32;
        fn PeekNamedPipe(
            pipe: *mut c_void,
            buffer: *mut c_void,
            size: u32,
            read: *mut u32,
            available: *mut u32,
            remaining: *mut u32,
        ) -> i32;
    }
    pub struct Descriptor(i32);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            unsafe {
                _close(self.0);
            }
        }
    }
    impl Descriptor {
        pub fn duplicate(fd: i32) -> io::Result<Self> {
            let fd = unsafe { _dup(fd) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let descriptor = Self(fd);
            // Saved terminal descriptors are private to the UI, including for
            // children spawned by native code using the CRT inheritance table.
            if unsafe { SetHandleInformation(_get_osfhandle(fd) as *mut c_void, 1, 0) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(descriptor)
        }
        pub fn redirect(&self, target: i32) -> io::Result<()> {
            if unsafe { _dup2(self.0, target) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let kind = if target == 1 { -11i32 } else { -12i32 } as u32;
            if unsafe { SetStdHandle(kind, _get_osfhandle(target) as *mut c_void) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
        pub fn into_file(self) -> io::Result<File> {
            let process = unsafe { GetCurrentProcess() };
            let mut target = std::ptr::null_mut();
            if unsafe {
                DuplicateHandle(
                    process,
                    _get_osfhandle(self.0) as *mut c_void,
                    process,
                    &mut target,
                    0,
                    0,
                    2,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { File::from_raw_handle(target) })
        }
    }
    pub fn pipe() -> io::Result<(File, Descriptor)> {
        let mut fds = [-1; 2];
        // Binary, non-inheritable originals. Child standard streams are duplicated
        // by Command; saved terminal handles never leak into children.
        if unsafe { _pipe(fds.as_mut_ptr(), 4096, 0x8000 | 0x80) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let reader = Descriptor(fds[0]);
        let writer = Descriptor(fds[1]);
        Ok((reader.into_file()?, writer))
    }
    pub fn read_ready(file: &mut File, bytes: &mut [u8]) -> io::Result<Option<usize>> {
        let mut available = 0;
        if unsafe {
            PeekNamedPipe(
                file.as_raw_handle(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                &mut available,
                std::ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(109) {
                Ok(Some(0))
            } else {
                Err(error)
            };
        }
        if available == 0 {
            return Ok(None);
        }
        let n = bytes.len().min(available as usize);
        file.read(&mut bytes[..n]).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raw_diagnostics_have_fixed_memory_bounds_and_report_omissions() {
        let mut buffer = OutputSnapshot::default();
        let bytes = vec![0xff; 2 * LINE_BYTES];
        for _ in 0..(RECORDS + 20) {
            buffer.push("stderr", &bytes, false);
        }
        assert_eq!(buffer.lines.len(), RECORDS);
        assert_eq!(buffer.evicted, 20);
        assert!(
            buffer
                .lines
                .iter()
                .all(|l| l.truncated && l.text.len() <= 3 * LINE_BYTES)
        );
    }
}
