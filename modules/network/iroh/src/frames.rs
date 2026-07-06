//! In-flight byte accounting and length-prefixed object-frame I/O for the
//! iroh proxy and dial paths.
//!
//! Moved out of `crates/sinomo-network/src/roaming.rs` in stage C of
//! docs/plans/r0-r1-crate-boundary-refactor.md. The frame layout (u32
//! big-endian length prefix with the u64 extended-length sentinel) is the
//! object-frame contract owned by `sinomo-network`; this module only reads
//! and writes whole frames as opaque bytes.

use crate::IROH_DAEMON_COMMAND_MAX_BYTES;
use sinomo_core::network::{NetworkError, OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL, try_zeroed_vec};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Ceiling on bytes concurrently buffered by proxy command streams.
pub const IROH_DAEMON_PROXY_COMMAND_BUDGET_BYTES: usize =
    IROH_DAEMON_COMMAND_MAX_BYTES.saturating_add(12);

/// Tracks bytes currently buffered for in-flight proxy frames so a flood of
/// concurrent command streams cannot exhaust memory.
#[derive(Clone, Debug)]
pub struct IrohProxyByteBudget {
    inner: Arc<Mutex<IrohProxyByteBudgetState>>,
    max_bytes: usize,
}

#[derive(Debug, Default)]
struct IrohProxyByteBudgetState {
    in_flight_bytes: usize,
}

/// Releases its byte reservation on drop.
#[derive(Debug)]
pub struct IrohProxyByteBudgetPermit {
    budget: IrohProxyByteBudget,
    bytes: usize,
}

impl IrohProxyByteBudget {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(IrohProxyByteBudgetState::default())),
            max_bytes,
        }
    }

    pub fn try_acquire(
        &self,
        frame_bytes: usize,
    ) -> Result<IrohProxyByteBudgetPermit, NetworkError> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let would_use = match state.in_flight_bytes.checked_add(frame_bytes) {
            Some(would_use) => would_use,
            None => {
                return Err(NetworkError::ResourceExhausted {
                    operation: format!(
                        "reserve iroh proxy frame: {frame_bytes} bytes requested with {} bytes in flight and {} max",
                        state.in_flight_bytes, self.max_bytes
                    ),
                    requested_bytes: frame_bytes,
                });
            }
        };
        if frame_bytes > self.max_bytes || would_use > self.max_bytes {
            return Err(NetworkError::ResourceExhausted {
                operation: format!(
                    "reserve iroh proxy frame: {frame_bytes} bytes requested with {} bytes in flight and {} max",
                    state.in_flight_bytes, self.max_bytes
                ),
                requested_bytes: frame_bytes,
            });
        }
        state.in_flight_bytes = would_use;
        Ok(IrohProxyByteBudgetPermit {
            budget: self.clone(),
            bytes: frame_bytes,
        })
    }

    #[cfg(test)]
    pub fn in_flight_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .in_flight_bytes
    }
}

impl Drop for IrohProxyByteBudgetPermit {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.in_flight_bytes = state.in_flight_bytes.saturating_sub(self.bytes);
    }
}

fn io_error(operation: impl Into<String>, detail: impl Into<String>) -> NetworkError {
    NetworkError::Io {
        operation: operation.into(),
        detail: detail.into(),
    }
}

fn frame_eof_detail(part: &str, read: usize, expected: usize) -> String {
    if read == 0 {
        format!("peer closed before object frame {part} ({expected} bytes expected)")
    } else {
        format!("peer closed during object frame {part}: read {read} of {expected} bytes")
    }
}

/// Body length from the frame prefix (normal u32 or sentinel + u64 extended).
fn frame_body_len(
    len_bytes: [u8; 4],
    extended_len_bytes: Option<[u8; 8]>,
    max_bytes: usize,
) -> Result<usize, NetworkError> {
    let body_bytes = match extended_len_bytes {
        Some(extended) => usize::try_from(u64::from_be_bytes(extended)).map_err(|_| {
            NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes,
            }
        })?,
        None => u32::from_be_bytes(len_bytes) as usize,
    };
    if body_bytes > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: body_bytes,
            max_bytes,
        });
    }
    Ok(body_bytes)
}

async fn read_exact_frame_async<R>(
    reader: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
    operation: String,
    part: &'static str,
) -> Result<(), NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        let progress = match idle_timeout {
            Some(idle_timeout) => {
                match tokio::time::timeout(idle_timeout, reader.read(&mut buf[read..])).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(io_error(
                            operation,
                            format!(
                                "timed out after {}ms without {part} read progress",
                                idle_timeout.as_millis()
                            ),
                        ));
                    }
                }
            }
            None => reader.read(&mut buf[read..]).await,
        };
        match progress {
            Ok(0) => return Err(io_error(operation, frame_eof_detail(part, read, expected))),
            Ok(n) => read = read.saturating_add(n),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => return Err(io_error(operation, source.to_string())),
        }
    }
    Ok(())
}

/// Reads one whole length-prefixed object frame (prefix included in the
/// returned bytes), accounting the allocation against `budget`.
pub async fn read_proxy_frame_async<R>(
    reader: &mut R,
    budget: &IrohProxyByteBudget,
    scope: &str,
    idle_timeout: Option<Duration>,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut len_bytes = [0_u8; 4];
    read_exact_frame_async(
        reader,
        &mut len_bytes,
        idle_timeout,
        format!("read iroh daemon {scope} frame length"),
        "length",
    )
    .await?;
    let extended_len_bytes =
        if u32::from_be_bytes(len_bytes) == OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL {
            let mut extended_len_bytes = [0_u8; 8];
            read_exact_frame_async(
                reader,
                &mut extended_len_bytes,
                idle_timeout,
                format!("read iroh daemon {scope} frame extended length"),
                "extended length",
            )
            .await?;
            Some(extended_len_bytes)
        } else {
            None
        };
    let body_bytes = frame_body_len(len_bytes, extended_len_bytes, IROH_DAEMON_COMMAND_MAX_BYTES)?;
    let prefix_bytes = 4_usize + if extended_len_bytes.is_some() { 8 } else { 0 };
    let frame_bytes = prefix_bytes
        .checked_add(body_bytes)
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: IROH_DAEMON_COMMAND_MAX_BYTES,
        })?;
    let permit = budget.try_acquire(frame_bytes)?;
    let mut frame = try_zeroed_vec(frame_bytes, "reserve iroh daemon proxy frame")?;
    frame[..4].copy_from_slice(&len_bytes);
    if let Some(extended) = extended_len_bytes {
        frame[4..12].copy_from_slice(&extended);
    }
    read_exact_frame_async(
        reader,
        &mut frame[prefix_bytes..],
        idle_timeout,
        format!("read iroh daemon {scope} frame body"),
        "body",
    )
    .await?;
    Ok((frame, permit))
}

/// Writes `bytes` fully, enforcing per-write idle progress when a timeout is
/// given, then flushes.
pub async fn write_all_frame_async<W>(
    writer: &mut W,
    bytes: &[u8],
    operation: &str,
    idle_timeout: Option<Duration>,
) -> Result<(), NetworkError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let expected = bytes.len();
    let mut written = 0_usize;
    while written < expected {
        let progress = match idle_timeout {
            Some(idle_timeout) => {
                match tokio::time::timeout(idle_timeout, writer.write(&bytes[written..])).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(io_error(
                            operation,
                            format!(
                                "timed out after {}ms without write progress",
                                idle_timeout.as_millis()
                            ),
                        ));
                    }
                }
            }
            None => writer.write(&bytes[written..]).await,
        };
        match progress {
            Ok(0) => {
                return Err(io_error(
                    operation,
                    format!("peer stopped accepting frame: wrote {written} of {expected} bytes"),
                ));
            }
            Ok(n) => written = written.saturating_add(n),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => return Err(io_error(operation, source.to_string())),
        }
    }
    let flush = async { writer.flush().await };
    match idle_timeout {
        Some(idle_timeout) => match tokio::time::timeout(idle_timeout, flush).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(source)) => Err(io_error(operation, source.to_string())),
            Err(_) => Err(io_error(
                operation,
                format!(
                    "timed out after {}ms without flush progress",
                    idle_timeout.as_millis()
                ),
            )),
        },
        None => flush
            .await
            .map_err(|source| io_error(operation, source.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn byte_budget_releases_permits() {
        let budget = IrohProxyByteBudget::new(10);
        let first = budget.try_acquire(6).expect("first frame budget");
        let second = budget.try_acquire(4).expect("second frame budget");

        assert_eq!(budget.in_flight_bytes(), 10);
        let error = budget.try_acquire(1).expect_err("budget rejection");
        assert!(matches!(
            error,
            NetworkError::ResourceExhausted {
                ref operation,
                requested_bytes: 1,
            } if operation.contains("10 bytes in flight and 10 max")
        ));

        drop(first);
        assert_eq!(budget.in_flight_bytes(), 4);
        let replacement = budget.try_acquire(6).expect("replacement budget");
        drop(second);
        drop(replacement);

        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[tokio::test]
    async fn frame_reader_charges_budget_before_body_read() {
        use tokio::io::AsyncWriteExt;

        let budget = IrohProxyByteBudget::new(8);
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&4_u32.to_be_bytes())
            .await
            .expect("write length");
        writer.write_all(&[1, 2, 3, 4]).await.expect("write body");

        let (frame, permit) = read_proxy_frame_async(&mut reader, &budget, "test", None)
            .await
            .expect("read frame");
        assert_eq!(frame, [0, 0, 0, 4, 1, 2, 3, 4]);
        assert_eq!(budget.in_flight_bytes(), 8);
        drop(permit);
        assert_eq!(budget.in_flight_bytes(), 0);

        let budget = IrohProxyByteBudget::new(16);
        let (mut writer, mut reader) = tokio::io::duplex(32);
        let mut extended_frame = OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL.to_be_bytes().to_vec();
        extended_frame.extend_from_slice(&4_u64.to_be_bytes());
        extended_frame.extend_from_slice(&[1, 2, 3, 4]);
        writer
            .write_all(&extended_frame)
            .await
            .expect("write extended frame");

        let (frame, permit) = read_proxy_frame_async(&mut reader, &budget, "test", None)
            .await
            .expect("read extended proxy frame");
        assert_eq!(frame, extended_frame);
        assert_eq!(budget.in_flight_bytes(), 16);
        drop(permit);
        assert_eq!(budget.in_flight_bytes(), 0);

        let budget = IrohProxyByteBudget::new(7);
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&4_u32.to_be_bytes())
            .await
            .expect("write length only");
        let error = tokio::time::timeout(
            Duration::from_millis(100),
            read_proxy_frame_async(&mut reader, &budget, "test", None),
        )
        .await
        .expect("budget rejection should not wait for body")
        .expect_err("budget error");
        assert!(matches!(
            error,
            NetworkError::ResourceExhausted {
                ref operation,
                requested_bytes: 8,
            } if operation.contains("0 bytes in flight and 7 max")
        ));
        assert_eq!(budget.in_flight_bytes(), 0);

        let budget = IrohProxyByteBudget::new(16);
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&4_u32.to_be_bytes())
            .await
            .expect("write length");
        writer.write_all(&[1, 2]).await.expect("write partial body");
        drop(writer);
        let error = read_proxy_frame_async(&mut reader, &budget, "test", None)
            .await
            .expect_err("partial body");
        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read iroh daemon test frame body".to_string(),
                detail: "peer closed during object frame body: read 2 of 4 bytes".to_string(),
            }
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "aborted frame reads must release the reserved proxy budget"
        );

        let budget = IrohProxyByteBudget::new(16);
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&4_u32.to_be_bytes())
            .await
            .expect("write length for timeout");
        let error = read_proxy_frame_async(
            &mut reader,
            &budget,
            "test",
            Some(Duration::from_millis(10)),
        )
        .await
        .expect_err("slow body should time out");
        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read iroh daemon test frame body".to_string(),
                detail: "timed out after 10ms without body read progress".to_string(),
            }
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "timed-out frame reads must release the reserved proxy budget"
        );

        let budget = IrohProxyByteBudget::new(16);
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(&4_u32.to_be_bytes())
                .await
                .expect("write length for slow body");
            for byte in [1_u8, 2, 3, 4] {
                tokio::time::sleep(Duration::from_millis(10)).await;
                writer.write_all(&[byte]).await.expect("write body byte");
            }
        });
        let (frame, permit) = read_proxy_frame_async(
            &mut reader,
            &budget,
            "test",
            Some(Duration::from_millis(20)),
        )
        .await
        .expect("body read should continue while bytes make progress");
        assert_eq!(frame, [0, 0, 0, 4, 1, 2, 3, 4]);
        assert_eq!(budget.in_flight_bytes(), 8);
        drop(permit);
        writer_task.await.expect("slow body writer");
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[tokio::test]
    async fn frame_writer_idle_timeout_applies_to_final_flush() {
        struct StuckFlushWriter;

        impl tokio::io::AsyncWrite for StuckFlushWriter {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let mut writer = StuckFlushWriter;
        let error = write_all_frame_async(
            &mut writer,
            b"request",
            "write iroh daemon command",
            Some(Duration::from_millis(5)),
        )
        .await
        .expect_err("flush should be bounded by idle timeout");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "write iroh daemon command".to_string(),
                detail: "timed out after 5ms without flush progress".to_string(),
            }
        );
    }
}
