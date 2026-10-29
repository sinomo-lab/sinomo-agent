//! Optional platform API. Absence is a capability result, not a module-load error.
use crate::bindings::OptionalPipeWireApi;
use std::sync::Mutex;
static API: Mutex<Option<OptionalPipeWireApi>> = Mutex::new(None);

pub fn ensure_available() -> Result<(), String> {
    let mut api = API.lock().unwrap_or_else(|e| e.into_inner());
    if api.is_none() {
        *api = Some(unsafe {
            OptionalPipeWireApi::load(std::ffi::OsStr::new("libpipewire-0.3.so.0"))
        }?);
    }
    Ok(())
}

pub(crate) fn symbol<T: Copy>(select: impl FnOnce(&OptionalPipeWireApi) -> T) -> T {
    ensure_available().expect("initialize the optional PipeWire runtime before using its API");
    let api = API.lock().unwrap_or_else(|e| e.into_inner());
    select(
        api.as_ref()
            .expect("PipeWire runtime was unloaded during use"),
    )
}

/// Release the library after all PipeWire resources, callbacks and threads drain.
///
/// # Safety
/// No C call, object, function/global pointer or PipeWire initialization may
/// survive this call. It must not run concurrently with any API user.
pub unsafe fn unload() {
    let api = API.lock().unwrap_or_else(|e| e.into_inner()).take();
    drop(api);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn absent_optional_library_is_an_error_without_initialization() {
        let result = unsafe {
            OptionalPipeWireApi::load(std::ffi::OsStr::new("/sinomo-missing-pipewire-library.so"))
        };
        assert!(result.is_err());
    }

    #[test]
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn incompatible_library_reports_the_required_symbol() {
        let error = unsafe { OptionalPipeWireApi::load(std::ffi::OsStr::new("libc.so.6")) }
            .err()
            .expect("libc is not a PipeWire implementation");
        assert!(error.contains("required PipeWire symbol"), "{error}");
        assert!(API.lock().unwrap().is_none());
    }
}
