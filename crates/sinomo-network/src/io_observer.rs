use std::sync::{Arc, Mutex};

/// Receives notifications about disk and network I/O performed by the network
/// layer: object-file and KV-snapshot writes (op counts), the byte volume of
/// those reads/writes, and the byte volume of framed RPC traffic. Used by
/// sinomo-daemon to feed idle-stats counters. All methods default to no-ops so
/// implementors opt into only the signals they care about.
pub trait IoObserver: Send + Sync {
    /// Bytes read from disk (object-file reads, including the read-compare in
    /// `write_object_file`).
    fn on_disk_read(&self, _bytes: u64) {}
    /// Bytes written to disk (object-file and KV-snapshot writes).
    fn on_disk_write(&self, _bytes: u64) {}
    fn on_object_file_actual_write(&self) {}
    fn on_object_file_skipped_write(&self) {}
    fn on_kv_snapshot_actual_write(&self) {}
    fn on_kv_snapshot_skipped_write(&self) {}
    /// Bytes sent over a framed RPC stream (SOF2 prefix + body).
    fn on_net_sent(&self, _bytes: u64) {}
    /// Bytes received over a framed RPC stream (SOF2 prefix + body).
    fn on_net_recv(&self, _bytes: u64) {}
}

static IO_OBSERVER: Mutex<Option<Arc<dyn IoObserver>>> = Mutex::new(None);

/// Install a process-wide I/O observer. The daemon installs exactly one
/// observer at startup; tests may swap in their own. The previous observer is
/// returned so callers can restore it after a scoped change.
pub fn set_io_observer(observer: Option<Arc<dyn IoObserver>>) -> Option<Arc<dyn IoObserver>> {
    let mut guard = IO_OBSERVER.lock().ok()?;
    std::mem::replace(&mut *guard, observer)
}

pub fn io_observer() -> Option<Arc<dyn IoObserver>> {
    IO_OBSERVER.lock().ok().and_then(|guard| guard.clone())
}
