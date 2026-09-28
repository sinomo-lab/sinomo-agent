//! Generic performance capture vocabulary and process-window I/O evidence.
//!
//! Timed stages are `tracing` spans with target `sinomo.perf` and a static
//! `stage` field. Observe records their lifetime only inside an explicit capture.
//! Use `Instrument` for futures, and explicitly carry spans into blocking jobs.
//! Never hold an entered span guard across an await. Disk/network fields on a
//! stage describe direct I/O only; inclusive attribution is computed from ancestry.
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::time::Instant;

pub(crate) fn monotonic_ns() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

/// Cumulative counters for the daemon process, not an individual operation.
/// `None` means unavailable. Kernel bytes differ from logical instrumented I/O:
/// page cache hits do not increment `read_bytes`, and writeback may be deferred.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ObserveProcessIo {
    pub source: String,
    pub read_chars: Option<u64>,
    pub write_chars: Option<u64>,
    pub read_syscalls: Option<u64>,
    pub write_syscalls: Option<u64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    pub cancelled_write_bytes: Option<u64>,
    /// Existing framed-RPC observer; excludes uninstrumented transports/bodies.
    pub framed_net_sent_bytes: Option<u64>,
    pub framed_net_received_bytes: Option<u64>,
    /// Linux non-loopback interfaces in this process's network namespace.
    /// Includes other processes and forwarding; not per-operation wire bytes.
    pub namespace_net_received_bytes: Option<u64>,
    pub namespace_net_sent_bytes: Option<u64>,
}

impl ObserveProcessIo {
    pub fn sample() -> Self {
        #[cfg(target_os = "linux")]
        let mut sample = std::fs::read_to_string("/proc/self/io")
            .map(|text| Self::from_proc_io(&text))
            .unwrap_or_default();
        #[cfg(not(target_os = "linux"))]
        let mut sample = Self::default();
        #[cfg(target_os = "linux")]
        if let Ok(text) = std::fs::read_to_string("/proc/self/net/dev")
            && let Some((received, sent)) = network_namespace_bytes(&text)
        {
            sample.namespace_net_received_bytes = Some(received);
            sample.namespace_net_sent_bytes = Some(sent);
        }
        if sample.source.is_empty() {
            sample.source = "unavailable".into();
        }
        if let Some(stats) = crate::process_idle_stats() {
            let stats = stats.snapshot();
            sample.framed_net_sent_bytes = Some(stats.net_bytes_sent);
            sample.framed_net_received_bytes = Some(stats.net_bytes_recv);
        }
        sample
    }

    #[cfg(any(target_os = "linux", test))]
    fn from_proc_io(text: &str) -> Self {
        let value = |key: &str| {
            text.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                (name == key).then(|| value.trim().parse().ok()).flatten()
            })
        };
        Self {
            source: "linux-proc-self-io".into(),
            read_chars: value("rchar"),
            write_chars: value("wchar"),
            read_syscalls: value("syscr"),
            write_syscalls: value("syscw"),
            read_bytes: value("read_bytes"),
            write_bytes: value("write_bytes"),
            cancelled_write_bytes: value("cancelled_write_bytes"),
            ..Self::default()
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn network_namespace_bytes(text: &str) -> Option<(u64, u64)> {
    let mut total = (0u64, 0u64);
    let mut observed = false;
    for line in text.lines() {
        let Some((interface, values)) = line.split_once(':') else {
            continue;
        };
        if interface.trim() == "lo" {
            continue;
        }
        let values: Vec<_> = values.split_whitespace().collect();
        let received: u64 = values.first()?.parse().ok()?;
        let sent: u64 = values.get(8)?.parse().ok()?;
        total.0 = total.0.checked_add(received)?;
        total.1 = total.1.checked_add(sent)?;
        observed = true;
    }
    observed.then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_network_sums_interfaces_without_loopback() {
        assert_eq!(
            network_namespace_bytes(
                "eth0: 100 0 0 0 0 0 0 0 200 0
lo: 800 0 0 0 0 0 0 0 800 0
eth1: 50 0 0 0 0 0 0 0 60 0"
            ),
            Some((150, 260))
        );
        assert_eq!(network_namespace_bytes("eth0: invalid"), None);
    }

    #[test]
    fn process_io_preserves_missing_invalid_and_zero_counters() {
        let sample = ObserveProcessIo::from_proc_io(
            "rchar: 123\nread_bytes: 0\nwrite_bytes: invalid\nsyscr: 4\n",
        );
        assert_eq!(sample.read_chars, Some(123));
        assert_eq!(sample.read_bytes, Some(0));
        assert_eq!(sample.write_bytes, None);
        assert_eq!(sample.read_syscalls, Some(4));
        assert_eq!(sample.write_syscalls, None);
    }
}
