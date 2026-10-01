use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Native process pipes are captured in private agent-owned files. Polls read
/// only their requested window; the complete stream remains available for
/// durable evidence without keeping it in the agent's heap.
#[derive(Clone, Debug)]
pub(crate) struct AgentProcessOutput(Arc<Capture>);

#[derive(Debug)]
struct Capture {
    stdout: Mutex<File>,
    stderr: Mutex<File>,
    // Fields drop in order: close readers before removing their directory.
    _directory: CaptureDirectory,
}

#[derive(Debug)]
struct CaptureDirectory(PathBuf);

impl Drop for CaptureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0.join("stdout"));
        let _ = fs::remove_file(self.0.join("stderr"));
        let _ = fs::remove_dir(&self.0);
    }
}

pub(crate) struct OutputWindow {
    pub bytes: Vec<u8>,
    pub offset: usize,
    pub total_bytes: usize,
}

impl AgentProcessOutput {
    pub(crate) fn create(root: &Path, process_id: &str) -> io::Result<(Self, File, File)> {
        fs::create_dir_all(root)?;
        let directory = root.join(process_id);
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        let directory = CaptureDirectory(directory);
        let stdout_path = directory.0.join("stdout");
        let stderr_path = directory.0.join("stderr");
        let stdout_writer = create_output_file(&stdout_path)?;
        let stderr_writer = create_output_file(&stderr_path)?;
        // Independent reader handles are essential: seeking a cloned handle
        // would also change the child's stdout/stderr file offset on Unix.
        let stdout = File::open(stdout_path)?;
        let stderr = File::open(stderr_path)?;
        Ok((
            Self(Arc::new(Capture {
                stdout: Mutex::new(stdout),
                stderr: Mutex::new(stderr),
                _directory: directory,
            })),
            stdout_writer,
            stderr_writer,
        ))
    }

    fn reader(&self, stderr: bool) -> io::Result<std::sync::MutexGuard<'_, File>> {
        let reader = if stderr {
            &self.0.stderr
        } else {
            &self.0.stdout
        };
        reader
            .lock()
            .map_err(|_| io::Error::other("process output lock poisoned"))
    }

    pub(crate) fn window(
        &self,
        stderr: bool,
        offset: usize,
        max_bytes: usize,
    ) -> io::Result<OutputWindow> {
        let mut reader = self.reader(stderr)?;
        let total_bytes = usize::try_from(reader.metadata()?.len())
            .map_err(|_| io::Error::other("process output exceeds addressable length"))?;
        let start = offset.min(total_bytes);
        let count = (total_bytes - start).min(max_bytes);
        let mut bytes = vec![0; count];
        if count != 0 {
            reader.seek(SeekFrom::Start(start as u64))?;
            reader.read_exact(&mut bytes)?;
        }
        Ok(OutputWindow {
            bytes,
            offset: start + count,
            total_bytes,
        })
    }
}

fn create_output_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.append(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}
