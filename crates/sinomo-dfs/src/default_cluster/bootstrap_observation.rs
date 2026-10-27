//! Opt-in DATA from actual bootstrap-owner calls. No queries or effect authority.
use super::*;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_EVENTS: usize = 96;
const MAX_BYTES: usize = 256 * 1024;
const MAX_EVENT_BYTES: usize = 16 * 1024;
static CAPTURES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Capture>>>> = OnceLock::new();
static REGISTRY_LOSSES: AtomicU64 = AtomicU64::new(0);
struct Buffer {
    events: VecDeque<(usize, Value)>,
    bytes: usize,
    evicted: u64,
}
struct Capture {
    root: PathBuf,
    node: NodeId,
    pid: u32,
    sequence: AtomicU64,
    losses: AtomicU64,
    oversized: AtomicU64,
    registry_start: u64,
    buffer: Mutex<Buffer>,
}
pub(crate) struct CaptureGuard(Arc<Capture>);
impl CaptureGuard {
    pub(crate) fn start(root: PathBuf, node: NodeId) -> Self {
        let capture = Arc::new(Capture {
            root: root.clone(),
            node,
            pid: std::process::id(),
            sequence: AtomicU64::new(0),
            losses: AtomicU64::new(0),
            oversized: AtomicU64::new(0),
            registry_start: REGISTRY_LOSSES.load(Ordering::Relaxed),
            buffer: Mutex::new(Buffer {
                events: VecDeque::new(),
                bytes: 0,
                evicted: 0,
            }),
        });
        let mut map = CAPTURES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap();
        assert!(
            map.get(&root).and_then(Weak::upgrade).is_none(),
            "one capture owner per native root"
        );
        map.insert(root, Arc::downgrade(&capture));
        Self(capture)
    }
    pub(crate) fn snapshot(&self) -> Value {
        let data = self.0.buffer.lock().unwrap();
        json!({"classification":"actual bootstrap-call observations, DATA only",
            "data_root":self.0.root,"node":self.0.node,"pid":self.0.pid,
            "attempted_records":self.0.sequence.load(Ordering::Relaxed),
            "evicted_records":data.evicted,"contention_losses":self.0.losses.load(Ordering::Relaxed),
            "oversized_records":self.0.oversized.load(Ordering::Relaxed),
            "registry_losses":REGISTRY_LOSSES.load(Ordering::Relaxed).saturating_sub(self.0.registry_start),
            "retained_bytes":data.bytes,"limits":{"events":MAX_EVENTS,"bytes":MAX_BYTES,"event_bytes":MAX_EVENT_BYTES},
            "events":data.events.iter().map(|(_,value)|value).collect::<Vec<_>>()})
    }
}
impl Drop for CaptureGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = CAPTURES.get().unwrap().lock() {
            if map
                .get(&self.0.root)
                .is_some_and(|w| w.ptr_eq(&Arc::downgrade(&self.0)))
            {
                map.remove(&self.0.root);
            }
        }
    }
}
pub(crate) fn record(
    root: &Path,
    node: &NodeId,
    stage: &'static str,
    value: impl FnOnce() -> Value,
) {
    let Some(registry) = CAPTURES.get() else {
        return;
    };
    let Ok(map) = registry.try_lock() else {
        REGISTRY_LOSSES.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let capture = map.get(root).and_then(Weak::upgrade);
    drop(map);
    let Some(capture) = capture else { return };
    let ordinal = capture.sequence.fetch_add(1, Ordering::Relaxed);
    if &capture.node != node {
        capture.losses.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let entry = json!({"ordinal":ordinal,"stage":stage,"value":value()});
    let Ok(encoded) = serde_json::to_vec(&entry) else {
        capture.losses.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if encoded.len() > MAX_EVENT_BYTES {
        capture.oversized.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let Ok(mut buffer) = capture.buffer.try_lock() else {
        capture.losses.fetch_add(1, Ordering::Relaxed);
        return;
    };
    while buffer.events.len() >= MAX_EVENTS || buffer.bytes + encoded.len() > MAX_BYTES {
        let Some((bytes, _)) = buffer.events.pop_front() else {
            break;
        };
        buffer.bytes -= bytes;
        buffer.evicted += 1;
    }
    buffer.bytes += encoded.len();
    buffer.events.push_back((encoded.len(), entry));
}
