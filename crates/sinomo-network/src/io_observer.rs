use arc_swap::ArcSwap;
use std::sync::{Arc, LazyLock};

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

type SharedIoObserver = Arc<dyn IoObserver>;

/// Lock-free read-mostly slot for the process observer.
///
/// Framed network I/O consults this slot for every frame, while installation
/// happens only during daemon startup or a scoped test. The outer `Arc` is the
/// immutable snapshot managed by `ArcSwap`; the inner `Arc` is the trait
/// object returned to callers.
struct IoObserverSlot {
    observer: ArcSwap<Option<SharedIoObserver>>,
}

impl IoObserverSlot {
    fn empty() -> Self {
        Self {
            observer: ArcSwap::from_pointee(None),
        }
    }

    fn swap(&self, observer: Option<SharedIoObserver>) -> Option<SharedIoObserver> {
        self.observer.swap(Arc::new(observer)).as_ref().clone()
    }

    fn load(&self) -> Option<SharedIoObserver> {
        self.observer.load().as_ref().clone()
    }
}

static IO_OBSERVER: LazyLock<IoObserverSlot> = LazyLock::new(IoObserverSlot::empty);

/// Install a process-wide I/O observer. The daemon installs exactly one
/// observer at startup; tests may swap in their own. The previous observer is
/// returned so callers can restore it after a scoped change.
pub fn set_io_observer(observer: Option<Arc<dyn IoObserver>>) -> Option<Arc<dyn IoObserver>> {
    IO_OBSERVER.swap(observer)
}

pub fn io_observer() -> Option<Arc<dyn IoObserver>> {
    IO_OBSERVER.load()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct CountingObserver {
        sent_bytes: AtomicU64,
    }

    impl IoObserver for CountingObserver {
        fn on_net_sent(&self, bytes: u64) {
            self.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    #[test]
    fn observer_slot_loads_and_returns_replaced_observers() {
        // Exercise the same slot implementation as the process-global APIs
        // without mutating global state in parallel with other unit tests.
        let slot = IoObserverSlot::empty();
        let first_impl = Arc::new(CountingObserver::default());
        let first: SharedIoObserver = first_impl.clone();
        let second_impl = Arc::new(CountingObserver::default());
        let second: SharedIoObserver = second_impl.clone();

        assert!(slot.load().is_none());
        assert!(slot.swap(Some(first.clone())).is_none());
        let loaded = slot.load().expect("first observer");
        assert!(Arc::ptr_eq(&loaded, &first));
        loaded.on_net_sent(7);
        assert_eq!(first_impl.sent_bytes.load(Ordering::Relaxed), 7);

        let replaced = slot.swap(Some(second.clone())).expect("replaced observer");
        assert!(Arc::ptr_eq(&replaced, &first));
        assert!(Arc::ptr_eq(&slot.load().expect("second observer"), &second));

        let removed = slot.swap(None).expect("removed observer");
        assert!(Arc::ptr_eq(&removed, &second));
        assert!(slot.load().is_none());
    }
}
