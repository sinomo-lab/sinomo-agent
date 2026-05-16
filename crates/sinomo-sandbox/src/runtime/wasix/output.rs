use crate::{
    SANDBOX_OUTPUT_CHUNK_BYTES, SandboxCommandId, SandboxOutputChunk, SandboxOutputStream,
    SandboxWasiError,
};
use std::{
    io::{self, Cursor, Seek, SeekFrom, Write},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Clone)]
pub(super) struct WasixStreamingOutputCapture {
    inner: Arc<Mutex<WasixStreamingOutputState>>,
    sandbox_id: crate::SandboxId,
    command_id: SandboxCommandId,
    stream: SandboxOutputStream,
    output_limit_bytes: usize,
    output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
}
impl WasixStreamingOutputCapture {
    pub(super) fn new(
        sandbox_id: crate::SandboxId,
        command_id: SandboxCommandId,
        stream: SandboxOutputStream,
        output_limit_bytes: usize,
        output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WasixStreamingOutputState::default())),
            sandbox_id,
            command_id,
            stream,
            output_limit_bytes,
            output_sink,
        }
    }

    pub(super) fn file(&self) -> WasixStreamingOutputFile {
        WasixStreamingOutputFile {
            capture: self.clone(),
        }
    }

    pub(super) fn bytes(&self) -> Result<Vec<u8>, SandboxWasiError> {
        let inner = self.inner.lock().map_err(|_| SandboxWasiError::Runtime {
            detail: "WASIX output capture lock poisoned".to_string(),
        })?;
        Ok(inner.data.get_ref().clone())
    }

    pub(super) fn chunks(&self) -> Result<Vec<SandboxOutputChunk>, SandboxWasiError> {
        let inner = self.inner.lock().map_err(|_| SandboxWasiError::Runtime {
            detail: "WASIX output chunk lock poisoned".to_string(),
        })?;
        Ok(inner.chunks.clone())
    }

    pub(super) fn output_limit_exceeded(&self) -> Result<bool, SandboxWasiError> {
        let inner = self.inner.lock().map_err(|_| SandboxWasiError::Runtime {
            detail: "WASIX output limit lock poisoned".to_string(),
        })?;
        Ok(inner.output_limit_exceeded)
    }

    fn record_write_at(&self, mut offset: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let chunks = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| io::Error::other("WASIX output capture lock poisoned"))?;
            if inner.output_limit_exceeded {
                return Err(io::Error::other(format!(
                    "WASIX {:?} output exceeded {} byte limit",
                    self.stream, self.output_limit_bytes
                )));
            }
            if inner.data.get_ref().len() > self.output_limit_bytes {
                inner.output_limit_exceeded = true;
                return Err(io::Error::other(format!(
                    "WASIX {:?} output exceeded {} byte limit",
                    self.stream, self.output_limit_bytes
                )));
            }
            let mut chunks = Vec::new();
            for chunk in bytes.chunks(SANDBOX_OUTPUT_CHUNK_BYTES) {
                chunks.push(SandboxOutputChunk {
                    sandbox_id: self.sandbox_id.clone(),
                    command_id: self.command_id.clone(),
                    stream: self.stream.clone(),
                    offset,
                    bytes: chunk.to_vec(),
                });
                offset = offset.saturating_add(chunk.len() as u64);
            }
            inner.chunks.extend(chunks.iter().cloned());
            chunks
        };
        if let Some(sink) = &self.output_sink {
            for chunk in chunks {
                sink.output_chunk(chunk);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "wasix-runtime")]
#[derive(Default)]
struct WasixStreamingOutputState {
    data: Cursor<Vec<u8>>,
    chunks: Vec<SandboxOutputChunk>,
    output_limit_exceeded: bool,
}

#[cfg(feature = "wasix-runtime")]
#[derive(Clone)]
pub(super) struct WasixStreamingOutputFile {
    capture: WasixStreamingOutputCapture,
}

#[cfg(feature = "wasix-runtime")]
impl std::fmt::Debug for WasixStreamingOutputFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WasixStreamingOutputFile")
            .field("stream", &self.capture.stream)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "wasix-runtime")]
impl virtual_fs::VirtualFile for WasixStreamingOutputFile {
    fn last_accessed(&self) -> u64 {
        1_000_000_000
    }

    fn last_modified(&self) -> u64 {
        1_000_000_000
    }

    fn created_time(&self) -> u64 {
        1_000_000_000
    }

    fn size(&self) -> u64 {
        self.capture
            .inner
            .lock()
            .map(|inner| inner.data.get_ref().len() as u64)
            .unwrap_or(0)
    }

    fn set_len(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        let mut inner = self
            .capture
            .inner
            .lock()
            .map_err(|_| virtual_fs::FsError::Lock)?;
        let new_size_usize = usize::try_from(new_size).map_err(|_| {
            inner.output_limit_exceeded = true;
            virtual_fs::FsError::UnknownError
        })?;
        if new_size_usize > self.capture.output_limit_bytes || new_size_usize > isize::MAX as usize
        {
            inner.output_limit_exceeded = true;
            return Err(virtual_fs::FsError::UnknownError);
        }
        inner.data.get_mut().resize(new_size_usize, 0);
        if inner.data.position() > new_size {
            inner.data.set_position(new_size);
        }
        Ok(())
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        Ok(())
    }

    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let ready = self
            .capture
            .inner
            .lock()
            .map(|mut inner| {
                let cur = inner.data.stream_position().unwrap_or_default();
                let len = inner.data.seek(SeekFrom::End(0)).unwrap_or_default();
                let _ = inner.data.seek(SeekFrom::Start(cur));
                len.saturating_sub(cur) as usize
            })
            .map_err(|_| io::Error::other("WASIX output capture lock poisoned"));
        Poll::Ready(ready)
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(SANDBOX_OUTPUT_CHUNK_BYTES))
    }
}

#[cfg(feature = "wasix-runtime")]
impl virtual_fs::AsyncSeek for WasixStreamingOutputFile {
    fn start_seek(self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let mut inner = self
            .capture
            .inner
            .lock()
            .map_err(|_| io::Error::other("WASIX output capture lock poisoned"))?;
        inner.data.seek(position)?;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let position = self
            .capture
            .inner
            .lock()
            .map(|mut inner| inner.data.stream_position())
            .map_err(|_| io::Error::other("WASIX output capture lock poisoned"))
            .and_then(|result| result);
        Poll::Ready(position)
    }
}

#[cfg(feature = "wasix-runtime")]
impl virtual_fs::AsyncWrite for WasixStreamingOutputFile {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let (offset, written) = {
            let mut inner = match self.capture.inner.lock() {
                Ok(inner) => inner,
                Err(_) => {
                    return Poll::Ready(Err(io::Error::other(
                        "WASIX output capture lock poisoned",
                    )));
                }
            };
            let new_len = (inner.data.position() as usize)
                .saturating_add(buf.len())
                .max(inner.data.get_ref().len());
            if new_len > self.capture.output_limit_bytes {
                inner.output_limit_exceeded = true;
                return Poll::Ready(Err(io::Error::other(format!(
                    "WASIX {:?} output exceeded {} byte limit",
                    self.capture.stream, self.capture.output_limit_bytes
                ))));
            }
            let offset = inner.data.position();
            match inner.data.write(buf) {
                Ok(written) => (offset, written),
                Err(error) => return Poll::Ready(Err(error)),
            }
        };
        if let Err(error) = self.capture.record_write_at(offset, &buf[..written]) {
            return Poll::Ready(Err(error));
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "wasix-runtime")]
impl virtual_fs::AsyncRead for WasixStreamingOutputFile {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut virtual_fs::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut inner = match self.capture.inner.lock() {
            Ok(inner) => inner,
            Err(_) => {
                return Poll::Ready(Err(io::Error::other("WASIX output capture lock poisoned")));
            }
        };
        let Ok(position) = usize::try_from(inner.data.position()) else {
            return Poll::Ready(Ok(()));
        };
        let data = inner.data.get_ref();
        if position >= data.len() {
            return Poll::Ready(Ok(()));
        }
        let available = &data[position..];
        let len = available.len().min(buf.remaining());
        buf.put_slice(&available[..len]);
        let position = inner.data.position().saturating_add(len as u64);
        inner.data.set_position(position);
        Poll::Ready(Ok(()))
    }
}
