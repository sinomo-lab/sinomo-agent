#![cfg(target_os = "linux")]

use libloading::Library;
use sinomo_device::desktop::{DesktopError, protocol::*};
use sinomo_module_api::*;
use std::{ffi::c_void, path::Path, process::Command};

unsafe extern "C" fn bound_store(user: *mut c_void, permission: SinomoModuleByteSlice) -> u8 {
    let expected = unsafe { &*(user as *const String) };
    u8::from(unsafe { permission.as_slice() } == expected.as_bytes())
}

unsafe fn capabilities(
    invoke: SinomoModuleInvokeFn,
    free: SinomoModuleFreeBufferFn,
    store: &str,
) -> DesktopProviderResponse {
    let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
        operation: APP_USE_OS_OPERATION.into(),
        payload_sobj: encode_sobj(&DesktopProviderRequest {
            permission_store: store.into(),
            command: DesktopProviderCommand::Capabilities,
        })
        .unwrap(),
        host_data_dir_root: None,
        cancellation_wait_handle: None,
        request_body: None,
        response_body_path: None,
    });
    let bytes = encode_sobj(&request).unwrap();
    let mut output = SinomoModuleByteBuffer::null();
    assert_eq!(
        unsafe { invoke(SinomoModuleByteSlice::from_slice(&bytes), &mut output) },
        SINOMO_MODULE_STATUS_OK
    );
    let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) }.to_vec();
    unsafe { free(output) };
    let NativeModuleResponse::Invocation(response) = decode_sobj(&bytes).unwrap() else {
        panic!("native invocation response expected");
    };
    decode_sobj(&response.payload_sobj).unwrap()
}

fn exercise(path: &Path) {
    let dynamic = Command::new("readelf")
        .arg("-d")
        .arg(path)
        .output()
        .unwrap();
    assert!(dynamic.status.success());
    let needed = String::from_utf8(dynamic.stdout).unwrap();
    for forbidden in ["libpipewire", "libspa", "libX11", "libwayland", "libdbus"] {
        assert!(
            !needed.contains(forbidden),
            "mandatory GUI linkage: {needed}"
        );
    }
    let store =
        std::env::temp_dir().join(format!("sinomo-headless-{}", sinomo_core::Id::generate()));
    let store = store.to_string_lossy().into_owned();
    let mut permission = permission_store_capability(&store);
    for _ in 0..2 {
        let library = unsafe { Library::new(path) }.expect("headless native module load");
        unsafe {
            let version = *library
                .get::<SinomoModuleApiVersionFn>(b"sinomo_module_api_version")
                .unwrap();
            let init = *library
                .get::<SinomoModuleInitFn>(b"sinomo_module_init")
                .unwrap();
            let invoke = *library
                .get::<SinomoModuleInvokeFn>(b"sinomo_module_invoke")
                .unwrap();
            let shutdown = *library
                .get::<SinomoModuleShutdownFn>(b"sinomo_module_shutdown")
                .unwrap();
            let free = *library
                .get::<SinomoModuleFreeBufferFn>(b"sinomo_module_free_buffer")
                .unwrap();
            assert_eq!(version(), SINOMO_NATIVE_MODULE_ABI_VERSION);
            let mut host = SinomoModuleHostApiV2::empty();
            host.abi_version += 1;
            assert_eq!(init(host), SINOMO_MODULE_STATUS_UNSUPPORTED_ABI);
            host.abi_version = SINOMO_NATIVE_MODULE_ABI_VERSION;
            host.user_data = (&mut permission as *mut String).cast();
            host.local_permission_granted = Some(bound_store);
            assert_eq!(init(host), SINOMO_MODULE_STATUS_OK);
            // Rebinding the same idle host is supported by the module lifecycle.
            assert_eq!(init(host), SINOMO_MODULE_STATUS_OK);
            assert!(matches!(
                capabilities(invoke, free, "foreign-store"),
                DesktopProviderResponse::Failed {
                    error: DesktopError::PermissionDenied(_)
                }
            ));
            let DesktopProviderResponse::Capabilities { capabilities: caps } =
                capabilities(invoke, free, &store)
            else {
                panic!("headless capability result expected");
            };
            assert!(!caps.capture && !caps.keyboard_mouse && !caps.background_window_input);
            assert!(!caps.limitations.is_empty());
            assert_eq!(shutdown(), SINOMO_MODULE_STATUS_OK);
        }
        drop(library);
    }
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
    assert!(
        !maps.contains("libpipewire-0.3.so"),
        "headless discovery loaded a GUI client"
    );
}

#[test]
fn native_module_loads_and_reloads_without_desktop_services() {
    if std::env::var_os("SINOMO_HEADLESS_MODULE_CHILD").is_some() {
        let path = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join("libsinomo_app_use_module.so");
        exercise(&path);
        return;
    }
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "native_module_loads_and_reloads_without_desktop_services",
            "--nocapture",
        ])
        .env("SINOMO_HEADLESS_MODULE_CHILD", "1")
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY")
        .env_remove("XDG_SESSION_TYPE")
        .env_remove("DBUS_SESSION_BUS_ADDRESS")
        .status()
        .unwrap();
    assert!(status.success());
}
