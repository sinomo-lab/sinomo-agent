use crate::{
    SANDBOX_OUTPUT_CHUNK_BYTES, SandboxCommandId, SandboxId, SandboxOutputChunk,
    SandboxOutputStream,
};

pub fn sandbox_output_chunks_from_bytes(
    command_id: &SandboxCommandId,
    sandbox_id: &SandboxId,
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

pub fn sandbox_output_chunk_stream_key(chunk: &SandboxOutputChunk) -> (String, &'static str, u64) {
    let stream = match chunk.stream {
        SandboxOutputStream::Stdout => "stdout",
        SandboxOutputStream::Stderr => "stderr",
    };
    (chunk.command_id.to_string(), stream, chunk.offset)
}
