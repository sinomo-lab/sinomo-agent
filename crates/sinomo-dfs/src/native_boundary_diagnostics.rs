//! Test-only observations of actual failing publication/recovery boundaries.
//! No capability, transport result, budget or persistent DFS record is changed.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[derive(Default)]
struct Events {
    rows: Vec<serde_json::Value>,
    bytes: usize,
    dropped: usize,
}
struct State {
    root: PathBuf,
    all_operations: bool,
    events: Mutex<Events>,
}
static CAPTURES: OnceLock<Mutex<Vec<Weak<State>>>> = OnceLock::new();
pub(crate) struct Capture(Arc<State>);

pub(crate) fn selected_operation(root: &Path, id: &str) -> bool {
    id == "v7-host-bind"
        || id.starts_with("filesystem:")
        || CAPTURES.get().is_some_and(|captures| {
            captures
                .lock()
                .unwrap()
                .iter()
                .filter_map(Weak::upgrade)
                .any(|capture| capture.all_operations && root.starts_with(&capture.root))
        })
}

pub(crate) fn capture(root: PathBuf) -> Capture {
    capture_operations(root, false)
}

pub(crate) fn capture_all_operations(root: PathBuf) -> Capture {
    capture_operations(root, true)
}

fn capture_operations(root: PathBuf, all_operations: bool) -> Capture {
    let state = Arc::new(State {
        root,
        all_operations,
        events: Mutex::new(Events::default()),
    });
    let mut captures = CAPTURES.get_or_init(Default::default).lock().unwrap();
    captures.retain(|c| c.strong_count() != 0);
    captures.push(Arc::downgrade(&state));
    Capture(state)
}

pub(crate) fn enabled(root: &Path) -> bool {
    CAPTURES.get().is_some_and(|captures| {
        captures
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .any(|capture| root.starts_with(&capture.root))
    })
}

pub(crate) fn record(
    root: &Path,
    kind: &str,
    facts: impl FnOnce() -> serde_json::Value,
    stack: bool,
) {
    let Some(captures) = CAPTURES.get() else {
        return;
    };
    let capture = captures
        .lock()
        .unwrap()
        .iter()
        .filter_map(Weak::upgrade)
        .find(|capture| root.starts_with(&capture.root));
    let Some(capture) = capture else { return };
    let row = serde_json::json!({
        "kind": kind, "physical_root": root,
        "thread": format!("{:?}", std::thread::current()),
        "unix_nanos": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().to_string(),
        "facts": facts(),
        "stack": stack.then(|| std::backtrace::Backtrace::force_capture().to_string()),
    });
    let bytes = serde_json::to_vec(&row).unwrap().len();
    let mut state = capture.events.lock().unwrap();
    if state.rows.len() >= crate::native_shard_trace::MAX_PACKETS
        || state.bytes.saturating_add(bytes) > crate::native_shard_trace::MAX_BYTES
    {
        state.dropped += 1;
        return;
    }
    state.bytes += bytes;
    state.rows.push(row);
}

impl Capture {
    pub(crate) fn dump(&self, label: &str, extra: serde_json::Value) {
        let state = self.0.events.lock().unwrap();
        self.write(label, extra, &state);
    }

    pub(crate) fn checkpoint(&self, label: &str) {
        let mut state = self.0.events.lock().unwrap();
        if self.write(label, serde_json::Value::Null, &state) {
            state.rows.clear();
            state.bytes = 0;
            // Keep lifetime dropped accounting visible across every window.
        }
    }

    fn write(&self, label: &str, extra: serde_json::Value, state: &Events) -> bool {
        let path = self
            .0
            .root
            .join(format!("boundary-diagnostic-{label}.json"));
        let value = serde_json::json!({"root": self.0.root, "events": state.rows,
            "bytes": state.bytes, "dropped": state.dropped, "extra": extra,
            "qualification": "Actual observations only; no authority supplied. A nonzero dropped count invalidates complete-capture claims."});
        if let Err(error) = std::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()) {
            eprintln!(
                "boundary diagnostic unavailable at {}: {error}",
                path.display()
            );
            false
        } else {
            eprintln!("boundary diagnostic: {}", path.display());
            true
        }
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        self.dump("drop", serde_json::Value::Null);
    }
}
