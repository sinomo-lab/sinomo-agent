use crate::{BuildError, BuildJobId, BuildJobState, BuildJobStatus, BuildPaths, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
};

pub const BUILD_LOG_ROUTE: &str = "/build/log";
pub const BUILD_LOG_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildLogRequest {
    pub job_id: BuildJobId,
    pub offset: u64,
    pub max_bytes: usize,
    pub generation: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildLogResponse {
    pub job_id: BuildJobId,
    pub state: BuildJobState,
    pub generation: String,
    pub offset: u64,
    pub next_offset: u64,
    pub bytes: Vec<u8>,
    pub complete: bool,
}

pub(crate) fn read_build_job_log(
    paths: &BuildPaths,
    request: &BuildLogRequest,
    status: &BuildJobStatus,
) -> Result<BuildLogResponse> {
    if request.max_bytes == 0
        || request.max_bytes > BUILD_LOG_MAX_BYTES
        || status.job_id != request.job_id
    {
        return Err(BuildError::InvalidValue {
            detail: "invalid build log read identity or byte limit".into(),
        });
    }
    let generation = format!("{}:{:?}", status.job_id, status.started_at);
    let path = paths.job_log(&request.job_id);
    let io_error = |error: std::io::Error| BuildError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    };
    let mut file = match fs::File::open(&path) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(io_error(error)),
    };
    let length = file
        .as_ref()
        .map(|file| file.metadata().map(|meta| meta.len()))
        .transpose()
        .map_err(io_error)?
        .unwrap_or(0);
    let offset = if request
        .generation
        .as_ref()
        .is_some_and(|previous| previous != &generation)
        || request.offset > length
    {
        0
    } else {
        request.offset
    };
    let mut bytes = Vec::new();
    if let Some(file) = file.as_mut() {
        file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        file.take(request.max_bytes as u64)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
    }
    let next_offset = offset + bytes.len() as u64;
    Ok(BuildLogResponse {
        job_id: request.job_id.clone(),
        state: status.state,
        generation,
        offset,
        next_offset,
        bytes,
        complete: status.state.is_terminal() && next_offset >= length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn running_log_reads_are_bounded_and_reset_after_worker_restart() {
        let root = sinomo_testdir::TestDir::new("build-log-cursor");
        let paths = BuildPaths::new(&root);
        let job_id = BuildJobId::generate();
        let mut status = BuildJobStatus {
            job_id: job_id.clone(),
            key: crate::BuildKey::new(format!("blake3:{}", "a".repeat(64))).unwrap(),
            state: BuildJobState::Running,
            queue_position: None,
            detail: String::new(),
            artifact_set: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            started_at: Some(time::OffsetDateTime::UNIX_EPOCH),
            finished_at: None,
        };
        let path = paths.job_log(&job_id);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"first\nsecond\n").unwrap();
        let mut request = BuildLogRequest {
            job_id,
            offset: 0,
            max_bytes: 6,
            generation: None,
        };
        let first = read_build_job_log(&paths, &request, &status).unwrap();
        assert_eq!(first.bytes, b"first\n");
        assert!(!first.complete);
        request.offset = first.next_offset;
        request.generation = Some(first.generation);
        assert_eq!(
            read_build_job_log(&paths, &request, &status).unwrap().bytes,
            b"second"
        );
        status.started_at = Some(time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
        status.state = BuildJobState::Failed;
        fs::write(&path, b"error\n").unwrap();
        let restarted = read_build_job_log(&paths, &request, &status).unwrap();
        assert_eq!(restarted.offset, 0);
        assert_eq!(restarted.bytes, b"error\n");
        assert!(restarted.complete);
        request.max_bytes = BUILD_LOG_MAX_BYTES + 1;
        assert!(read_build_job_log(&paths, &request, &status).is_err());
    }
}
