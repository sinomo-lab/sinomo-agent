use crate::internal::*;

#[derive(Debug, Error)]
pub enum SandboxEventEmissionError {
    #[error(transparent)]
    Store(#[from] SandboxStoreError),
    #[error(transparent)]
    Fs(#[from] SandboxFsError),
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
}

pub fn append_sandbox_command_record_events<F, S>(
    sandbox_fs: &mut F,
    origin_node_id: NodeId,
    event_store: &mut S,
) -> Result<Vec<AppendReceipt>, SandboxEventEmissionError>
where
    F: SandboxFileSystem,
    S: EventStore,
{
    let logs_path = SandboxPath::new("/logs").expect("static sandbox path");
    let mut receipts = Vec::new();
    let mut entries = sandbox_fs.list_unchecked_mut(&logs_path)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    for entry in entries {
        let path = entry.path;
        if !path.as_str().ends_with(".record.sobj") {
            continue;
        }
        let bytes = sandbox_fs.read_unchecked_mut(&path, None)?;
        let record = decode_command_record(&bytes)?;
        receipts.push(
            event_store.append(sandbox_command_started_event_from_record(
                origin_node_id.clone(),
                &record,
            ))?,
        );
        receipts.extend(append_sandbox_command_output_events_for_record(
            sandbox_fs,
            origin_node_id.clone(),
            &record,
            event_store,
        )?);
        let event = sandbox_command_record_event(origin_node_id.clone(), &record);
        receipts.push(event_store.append(event)?);
    }
    Ok(receipts)
}

pub fn append_sandbox_command_record_event<F, S>(
    sandbox_fs: &mut F,
    origin_node_id: NodeId,
    command_id: &SandboxCommandId,
    event_store: &mut S,
) -> Result<AppendReceipt, SandboxEventEmissionError>
where
    F: SandboxFileSystem,
    S: EventStore,
{
    let record_path = SandboxPath::new(format!("/logs/{command_id}.record.sobj"))
        .map_err(SandboxFsError::from)?;
    let bytes = sandbox_fs.read_unchecked_mut(&record_path, None)?;
    let record = decode_command_record(&bytes)?;
    event_store.append(sandbox_command_started_event_from_record(
        origin_node_id.clone(),
        &record,
    ))?;
    append_sandbox_command_output_events_for_record(
        sandbox_fs,
        origin_node_id.clone(),
        &record,
        event_store,
    )?;
    let event = sandbox_command_record_event(origin_node_id, &record);
    Ok(event_store.append(event)?)
}

fn append_sandbox_command_output_events_for_record<F, S>(
    sandbox_fs: &mut F,
    origin_node_id: NodeId,
    record: &SandboxCommandRecord,
    event_store: &mut S,
) -> Result<Vec<AppendReceipt>, SandboxEventEmissionError>
where
    F: SandboxFileSystem,
    S: EventStore,
{
    let mut receipts = Vec::new();
    let stdout_path =
        sandbox_command_stdout_log_path(&record.command_id).map_err(SandboxFsError::from)?;
    if let Ok(stdout) = sandbox_fs.read_unchecked_mut(&stdout_path, None) {
        for chunk in sandbox_output_chunks(
            &record.sandbox_id,
            &record.command_id,
            SandboxOutputStream::Stdout,
            &stdout,
        ) {
            receipts.push(
                event_store.append(sandbox_command_output_event(origin_node_id.clone(), &chunk))?,
            );
        }
    }
    let stderr_path =
        sandbox_command_stderr_log_path(&record.command_id).map_err(SandboxFsError::from)?;
    if let Ok(stderr) = sandbox_fs.read_unchecked_mut(&stderr_path, None) {
        for chunk in sandbox_output_chunks(
            &record.sandbox_id,
            &record.command_id,
            SandboxOutputStream::Stderr,
            &stderr,
        ) {
            receipts.push(
                event_store.append(sandbox_command_output_event(origin_node_id.clone(), &chunk))?,
            );
        }
    }
    Ok(receipts)
}

pub(crate) fn sandbox_output_chunks(
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
    stream: SandboxOutputStream,
    bytes: &[u8],
) -> Vec<SandboxOutputChunk> {
    bytes
        .chunks(SANDBOX_OUTPUT_CHUNK_BYTES)
        .enumerate()
        .map(|(index, chunk)| SandboxOutputChunk {
            sandbox_id: sandbox_id.clone(),
            command_id: command_id.clone(),
            stream: stream.clone(),
            offset: (index * SANDBOX_OUTPUT_CHUNK_BYTES) as u64,
            bytes: chunk.to_vec(),
        })
        .collect()
}

pub fn append_sandbox_http_exchange_events<F, S>(
    sandbox_fs: &mut F,
    origin_node_id: NodeId,
    event_store: &mut S,
) -> Result<Vec<AppendReceipt>, SandboxEventEmissionError>
where
    F: SandboxFileSystem,
    S: EventStore,
{
    let logs_path = SandboxPath::new("/logs").expect("static sandbox path");
    let mut receipts = Vec::new();
    let mut entries = sandbox_fs.list_unchecked_mut(&logs_path)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    for entry in entries {
        let path = entry.path;
        if !path.as_str().ends_with(".http.sobj") {
            continue;
        }
        let bytes = sandbox_fs.read_unchecked_mut(&path, None)?;
        let record = decode_http_exchange_record(&bytes)?;
        let event = sandbox_http_exchange_event(origin_node_id.clone(), &record);
        receipts.push(event_store.append(event)?);
    }
    Ok(receipts)
}
