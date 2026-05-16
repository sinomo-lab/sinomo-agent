use crate::{
    SANDBOX_OUTPUT_CHUNK_BYTES, SandboxCommandId, SandboxId, SandboxOutputChunk, SandboxOutputSink,
    SandboxOutputStream,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

pub(super) fn spawn_output_watcher(
    sandbox_id: SandboxId,
    command_id: SandboxCommandId,
    stdout: wasmtime_wasi::p2::pipe::MemoryOutputPipe,
    stderr: wasmtime_wasi::p2::pipe::MemoryOutputPipe,
    done: Arc<AtomicBool>,
    sink: Arc<dyn SandboxOutputSink>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut stdout_offset = 0usize;
        let mut stderr_offset = 0usize;
        while !done.load(Ordering::SeqCst) {
            stdout_offset = emit_available_output_chunks(
                &sink,
                &sandbox_id,
                &command_id,
                SandboxOutputStream::Stdout,
                stdout_offset,
                &stdout.contents(),
            );
            stderr_offset = emit_available_output_chunks(
                &sink,
                &sandbox_id,
                &command_id,
                SandboxOutputStream::Stderr,
                stderr_offset,
                &stderr.contents(),
            );
            thread::sleep(Duration::from_millis(10));
        }
        let stdout_bytes = stdout.contents();
        let stderr_bytes = stderr.contents();
        let _ = emit_available_output_chunks(
            &sink,
            &sandbox_id,
            &command_id,
            SandboxOutputStream::Stdout,
            stdout_offset,
            &stdout_bytes,
        );
        let _ = emit_available_output_chunks(
            &sink,
            &sandbox_id,
            &command_id,
            SandboxOutputStream::Stderr,
            stderr_offset,
            &stderr_bytes,
        );
    })
}

fn emit_available_output_chunks(
    sink: &Arc<dyn SandboxOutputSink>,
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
    stream: SandboxOutputStream,
    offset: usize,
    bytes: &[u8],
) -> usize {
    if bytes.len() <= offset {
        return offset;
    }
    for (index, chunk) in bytes[offset..]
        .chunks(SANDBOX_OUTPUT_CHUNK_BYTES)
        .enumerate()
    {
        sink.output_chunk(SandboxOutputChunk {
            sandbox_id: sandbox_id.clone(),
            command_id: command_id.clone(),
            stream: stream.clone(),
            offset: (offset + index * SANDBOX_OUTPUT_CHUNK_BYTES) as u64,
            bytes: chunk.to_vec(),
        });
    }
    bytes.len()
}
