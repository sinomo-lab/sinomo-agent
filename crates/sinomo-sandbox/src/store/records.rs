use crate::internal::*;

use super::{
    LocalSandboxInstanceStore, StoredSandboxCommandRecord, StoredSandboxHttpExchangeRecord,
};

pub fn encode_command_record(record: &SandboxCommandRecord) -> Result<Vec<u8>, SandboxStoreError> {
    let stored = StoredSandboxCommandRecord::try_from(record)?;
    sinomo_object_serde::to_bytes(&stored, SANDBOX_MANIFEST_OBJECT_VERSION, LengthMode::U32)
        .map_err(|source| SandboxStoreError::Object {
            path: "command.record.sobj".to_string(),
            detail: source.to_string(),
        })
}

pub fn decode_command_record(bytes: &[u8]) -> Result<SandboxCommandRecord, SandboxStoreError> {
    let stored: StoredSandboxCommandRecord =
        sinomo_object_serde::from_bytes(bytes).map_err(|source| SandboxStoreError::Object {
            path: "command.record.sobj".to_string(),
            detail: source.to_string(),
        })?;
    SandboxCommandRecord::try_from(stored)
}

pub fn persist_command_record<F: SandboxFileSystem>(
    sandbox_fs: &mut F,
    record: &SandboxCommandRecord,
) -> Result<(), SandboxStoreError> {
    let path = sandbox_command_record_log_path(&record.command_id)?;
    let bytes = encode_command_record(record)?;
    sandbox_fs
        .write(&path, &bytes)
        .map_err(SandboxStoreError::from)
}

pub fn persist_command_outputs<F: SandboxFileSystem>(
    sandbox_fs: &mut F,
    record: &SandboxCommandRecord,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<(), SandboxStoreError> {
    let stdout_path =
        sandbox_command_stdout_log_path(&record.command_id).map_err(SandboxFsError::from)?;
    let stderr_path =
        sandbox_command_stderr_log_path(&record.command_id).map_err(SandboxFsError::from)?;
    sandbox_fs
        .write(&stdout_path, stdout)
        .map_err(SandboxStoreError::from)?;
    sandbox_fs
        .write(&stderr_path, stderr)
        .map_err(SandboxStoreError::from)?;
    persist_command_record(sandbox_fs, record)
}

pub fn record_local_command_history(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
    now: OffsetDateTime,
) -> Result<(), SandboxStoreError> {
    let (mut manifest, _) = store.load(sandbox_id)?;
    if !manifest.command_history.iter().any(|id| id == command_id) {
        manifest.command_history.push(command_id.clone());
    }
    manifest.updated_at = now;
    store.save_manifest(&manifest).map(|_| ())
}

pub fn load_command_record<F: SandboxFileSystem>(
    sandbox_fs: &mut F,
    command_id: &SandboxCommandId,
) -> Result<SandboxCommandRecord, SandboxStoreError> {
    let path = sandbox_command_record_log_path(command_id)?;
    let bytes = sandbox_fs
        .read_unchecked_mut(&path, None)
        .map_err(SandboxStoreError::from)?;
    decode_command_record(&bytes)
}

pub fn encode_http_exchange_record(
    record: &SandboxHttpExchangeRecord,
) -> Result<Vec<u8>, SandboxStoreError> {
    let stored = StoredSandboxHttpExchangeRecord::try_from(record)?;
    sinomo_object_serde::to_bytes(
        &stored,
        SANDBOX_HTTP_EXCHANGE_OBJECT_VERSION,
        LengthMode::U32,
    )
    .map_err(|source| SandboxStoreError::Object {
        path: "http.exchange.sobj".to_string(),
        detail: source.to_string(),
    })
}

pub fn decode_http_exchange_record(
    bytes: &[u8],
) -> Result<SandboxHttpExchangeRecord, SandboxStoreError> {
    let stored: StoredSandboxHttpExchangeRecord =
        sinomo_object_serde::from_bytes(bytes).map_err(|source| SandboxStoreError::Object {
            path: "http.exchange.sobj".to_string(),
            detail: source.to_string(),
        })?;
    SandboxHttpExchangeRecord::try_from(stored)
}

pub fn persist_http_exchange_record<F: SandboxFileSystem>(
    sandbox_fs: &mut F,
    record: &SandboxHttpExchangeRecord,
) -> Result<(), SandboxStoreError> {
    let path = sandbox_http_exchange_record_log_path(&record.command_id, &record.request_id)?;
    let bytes = encode_http_exchange_record(record)?;
    sandbox_fs
        .write(&path, &bytes)
        .map_err(SandboxStoreError::from)
}

pub fn load_http_exchange_record<F: SandboxFileSystem>(
    sandbox_fs: &mut F,
    command_id: &SandboxCommandId,
    request_id: &SandboxNetworkRequestId,
) -> Result<SandboxHttpExchangeRecord, SandboxStoreError> {
    let path = sandbox_http_exchange_record_log_path(command_id, request_id)?;
    let bytes = sandbox_fs
        .read_unchecked_mut(&path, None)
        .map_err(SandboxStoreError::from)?;
    decode_http_exchange_record(&bytes)
}
