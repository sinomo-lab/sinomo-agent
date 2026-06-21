use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

use sinomo_storage::durable_file::file_ino;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClusterFileMark {
    pub(crate) exists: bool,
    pub(crate) len: u64,
    pub(crate) modified: Option<SystemTime>,
    pub(crate) ino: u64,
}

pub(crate) fn cluster_file_mark(path: &PathBuf) -> Option<ClusterFileMark> {
    match fs::metadata(path) {
        Ok(metadata) => Some(ClusterFileMark {
            exists: true,
            len: metadata.len(),
            modified: metadata.modified().ok(),
            ino: file_ino(&metadata),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Some(ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        }),
        Err(_) => None,
    }
}
