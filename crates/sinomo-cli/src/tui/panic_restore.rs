//! Delegate panic reporting only after a fatal terminal owner has restored its screen.
use std::sync::{Arc, Mutex, Once, Weak};
use std::thread::{self, ThreadId};

pub(super) trait PanicRestore: Send + Sync {
    fn restore_terminal(&self);
}

type Owner = (ThreadId, Weak<dyn PanicRestore>);
static ACTIVE: Mutex<Option<Owner>> = Mutex::new(None);

pub(super) struct PanicRestoreGuard(Weak<dyn PanicRestore>);

pub(super) fn install(restore: Arc<dyn PanicRestore>) -> PanicRestoreGuard {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let restore = ACTIVE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .filter(|(owner, _)| cfg!(panic = "abort") || *owner == thread::current().id())
                .and_then(|(_, restore)| restore.upgrade());
            // With unwind, background workers can fail without ending the UI.
            // With abort, every panic is fatal, regardless of its originating thread.
            if let Some(restore) = restore {
                restore.restore_terminal();
            }
            previous(info);
        }));
    });
    let weak = Arc::downgrade(&restore);
    *ACTIVE.lock().unwrap_or_else(|e| e.into_inner()) =
        Some((thread::current().id(), weak.clone()));
    PanicRestoreGuard(weak)
}

impl Drop for PanicRestoreGuard {
    fn drop(&mut self) {
        let mut active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
        if active
            .as_ref()
            .is_some_and(|(_, restore)| restore.ptr_eq(&self.0))
        {
            *active = None;
        }
    }
}
