//! Bounded request-body spooling in the DFS-owned staging directory. The
//! blocking receiver owns its admission permit until transfer and commit finish.
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ObjectRpcIncomingBody;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::sync::OnceLock;

pub struct AttachmentUploadSpool {
    file: Option<File>,
    path: PathBuf,
}

impl AttachmentUploadSpool {
    pub fn receive(
        data_dir: &SinomoDataDir,
        mut body: ObjectRpcIncomingBody,
        expected: u64,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        if expected > crate::DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64
            || body.total_bytes().is_some_and(|n| n != expected)
        {
            return Err("attachment body length exceeds or differs from its declaration".into());
        }
        static BOOT: OnceLock<String> = OnceLock::new();
        let boot = BOOT.get_or_init(|| sinomo_core::Id::generate().to_string());
        let directory = data_dir.dfs_dir().join("content/staging/attachments");
        fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        // The daemon's DFS owner manages this directory. Previous process
        // uploads cannot resume without a client retry and are safe to reclaim.
        // Bound each pass; active uploads use the current process incarnation.
        for entry in fs::read_dir(&directory)
            .map_err(|e| e.to_string())?
            .take(128)
        {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("upload-")
                && !name.starts_with(&format!("upload-{boot}-"))
                && entry.file_type().map_err(|e| e.to_string())?.is_file()
            {
                fs::remove_file(entry.path()).map_err(|e| e.to_string())?;
            }
        }
        let path = directory.join(format!("upload-{boot}-{}", sinomo_core::Id::generate()));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        let file = options.open(&path).map_err(|e| e.to_string())?;
        let mut spool = Self {
            file: Some(file),
            path,
        };
        let mut received = 0u64;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        while let Some(chunk) = runtime
            .block_on(async { tokio::time::timeout_at(deadline, body.next_chunk()).await })
            .map_err(|_| "attachment upload deadline elapsed".to_string())?
            .map_err(|e| e.to_string())?
        {
            received = received
                .checked_add(chunk.as_ref().len() as u64)
                .ok_or("attachment length overflow")?;
            if received > expected {
                return Err("attachment body exceeds its declared length".into());
            }
            spool
                .file()
                .write_all(chunk.as_ref())
                .map_err(|e| e.to_string())?;
        }
        if received != expected {
            return Err("attachment body ended before its declared length".into());
        }
        spool.file().rewind().map_err(|e| e.to_string())?;
        Ok(spool)
    }

    pub fn file(&mut self) -> &mut File {
        self.file.as_mut().expect("live upload")
    }
}

impl Drop for AttachmentUploadSpool {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
    }
}
