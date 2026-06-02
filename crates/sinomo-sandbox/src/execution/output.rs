use crate::internal::*;

/// Live sink for sandbox command output chunks (stdout/stderr) as they are
/// produced during execution.
///
/// This is a *contract*: the native WASIX module and compatibility backends
/// drive it while a command runs, and daemon streaming RPC handlers consume the
/// chunks. The blanket impl lets any matching closure act as a sink.
pub trait SandboxOutputSink: Send + Sync {
    fn output_chunk(&self, chunk: SandboxOutputChunk);
}

impl<F> SandboxOutputSink for F
where
    F: Fn(SandboxOutputChunk) + Send + Sync,
{
    fn output_chunk(&self, chunk: SandboxOutputChunk) {
        self(chunk)
    }
}
