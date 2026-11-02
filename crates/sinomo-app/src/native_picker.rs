//! Typed, non-blocking access to the host's native file and folder pickers.
//!
//! Native dialogs are modal at the operating-system level.  They therefore run
//! on a short-lived worker thread and return through this request-ID lane rather
//! than blocking a SUI event callback.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativePickerKind {
    OneFile,
    MultipleFiles,
    OneFolder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(any(feature = "desktop", target_os = "android")), allow(dead_code))]
pub(crate) enum NativePickerOutcome {
    Selected(Vec<PathBuf>),
    Cancelled,
    Failed(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativePickerResult {
    pub request_id: u64,
    /// Caller-owned generation captured when the dialog was opened.
    pub context_generation: u64,
    pub outcome: NativePickerOutcome,
}

pub(crate) struct NativePickerLane {
    next_request_id: u64,
    pending_request_id: Option<u64>,
    sender: Sender<NativePickerResult>,
    receiver: Receiver<NativePickerResult>,
}

impl Default for NativePickerLane {
    fn default() -> Self {
        Self::new()
    }
}

impl NativePickerLane {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            next_request_id: 0,
            pending_request_id: None,
            sender,
            receiver,
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.pending_request_id.is_some()
    }

    pub(crate) fn request(
        &mut self,
        kind: NativePickerKind,
        title: impl Into<String>,
        context_generation: u64,
        waker: Option<sui::UiHandle>,
    ) -> Result<u64, String> {
        if self.pending_request_id.is_some() {
            return Err("a file picker is already open".to_string());
        }
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        let request_id = self.next_request_id;
        self.pending_request_id = Some(request_id);
        let sender = self.sender.clone();
        let title = title.into();

        let spawn = std::thread::Builder::new()
            .name(format!("sinomo-native-picker-{request_id}"))
            .spawn(move || {
                // A platform dialog backend is outside Sinomo's control.  Do
                // not leave the typed lane permanently pending if one of
                // those backends panics while opening or closing the dialog.
                let outcome =
                    std::panic::catch_unwind(|| run_picker(kind, &title)).unwrap_or_else(|panic| {
                        let detail = panic
                            .downcast_ref::<&str>()
                            .map(|detail| (*detail).to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown platform dialog failure".to_string());
                        NativePickerOutcome::Failed(format!(
                            "the native picker closed unexpectedly: {detail}"
                        ))
                    });
                let _ = sender.send(NativePickerResult {
                    request_id,
                    context_generation,
                    outcome,
                });
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        if let Err(error) = spawn {
            self.pending_request_id = None;
            return Err(format!("could not open the native picker: {error}"));
        }
        Ok(request_id)
    }

    /// Drain the newest completion for the currently pending request.
    ///
    /// Results from superseded requests are intentionally discarded.  The
    /// caller still validates `context_generation` before applying a result.
    pub(crate) fn poll(&mut self) -> Option<NativePickerResult> {
        loop {
            match self.receiver.try_recv() {
                Ok(result) => {
                    if self.pending_request_id != Some(result.request_id) {
                        continue;
                    }
                    self.pending_request_id = None;
                    return Some(result);
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return None,
            }
        }
    }
}

#[cfg(feature = "desktop")]
fn run_picker(kind: NativePickerKind, title: &str) -> NativePickerOutcome {
    let dialog = rfd::FileDialog::new().set_title(title);
    let selected = match kind {
        NativePickerKind::OneFile => dialog.pick_file().map(|path| vec![path]),
        NativePickerKind::MultipleFiles => dialog.pick_files(),
        NativePickerKind::OneFolder => dialog.pick_folder().map(|path| vec![path]),
    };
    match selected {
        Some(paths) if !paths.is_empty() => NativePickerOutcome::Selected(paths),
        _ => NativePickerOutcome::Cancelled,
    }
}

#[cfg(all(not(feature = "desktop"), not(target_os = "android")))]
fn run_picker(_kind: NativePickerKind, _title: &str) -> NativePickerOutcome {
    NativePickerOutcome::Failed("native file pickers are unavailable on this platform".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_picker_results_are_ignored() {
        let mut lane = NativePickerLane::new();
        lane.pending_request_id = Some(7);
        lane.sender
            .send(NativePickerResult {
                request_id: 6,
                context_generation: 1,
                outcome: NativePickerOutcome::Cancelled,
            })
            .expect("send stale result");
        lane.sender
            .send(NativePickerResult {
                request_id: 7,
                context_generation: 2,
                outcome: NativePickerOutcome::Selected(vec![PathBuf::from("chosen.txt")]),
            })
            .expect("send current result");

        let result = lane.poll().expect("current result");
        assert_eq!(result.request_id, 7);
        assert_eq!(result.context_generation, 2);
        assert!(!lane.is_pending());
    }
}

#[cfg(all(target_os = "android", not(feature = "desktop")))]
fn run_picker(kind: NativePickerKind, _title: &str) -> NativePickerOutcome {
    let operation = match kind {
        NativePickerKind::OneFile => "file",
        NativePickerKind::MultipleFiles => "files",
        NativePickerKind::OneFolder => {
            return NativePickerOutcome::Failed(
                "Android document folders cannot be used as filesystem workspace paths".into(),
            );
        }
    };
    match crate::android_media::request(operation) {
        Ok(result) if result == "cancelled" => NativePickerOutcome::Cancelled,
        Ok(result) => match serde_json::from_str::<Vec<PathBuf>>(&result) {
            Ok(paths) if !paths.is_empty() => NativePickerOutcome::Selected(paths),
            Ok(_) => NativePickerOutcome::Cancelled,
            Err(error) => {
                NativePickerOutcome::Failed(format!("Invalid document picker result: {error}"))
            }
        },
        Err(error) => NativePickerOutcome::Failed(error),
    }
}
