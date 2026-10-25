//! Native ABI probe for an explicitly authorized Android/Linux test session.
//! OS effects always cross the actual independently loaded provider dylib.
#[cfg(not(any(target_os = "android", target_os = "linux")))]
fn main() {
    eprintln!("build this example for Linux or Android");
}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    android::run()
}

#[cfg(any(target_os = "android", target_os = "linux"))]
mod android {
    use serde_json::{Value, json};
    use sinomo_core::node::LocalNodePermissionStore;
    use sinomo_device::desktop::protocol::*;
    use sinomo_module_api::*;
    use std::ffi::{CString, c_void};
    use std::io::{BufRead, Write};
    use std::path::PathBuf;

    struct Host {
        permission: String,
        frame: Option<Value>,
        output: PathBuf,
        save: bool,
    }
    unsafe extern "C" fn permission(user: *mut c_void, name: SinomoModuleByteSlice) -> u8 {
        let host = unsafe { &*(user as *const Host) };
        u8::from(unsafe { name.as_slice() } == host.permission.as_bytes())
    }
    unsafe extern "C" fn output(user: *mut c_void, payload: SinomoModuleByteSlice) -> i32 {
        let host = unsafe { &mut *(user as *mut Host) };
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let delivery: DesktopFrameDelivery = decode_sobj(unsafe { payload.as_slice() })?;
            let f = delivery.frame;
            let sample = f.clone().into_sample()?;
            sample.validate()?;
            let checksum = f.pixels.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            });
            if host.save {
                std::fs::write(host.output.join("frame.bgra"), &f.pixels)?;
            }
            let metadata = json!({"capture_id":delivery.capture_id,"sequence":f.sequence,
                "width":f.width,"height":f.height,"stride":f.stride,
                "timestamp_ns":f.timestamp_ns,"bytes":f.pixels.len(),"checksum":format!("{checksum:016x}")});
            if host.save {
                std::fs::write(
                    host.output.join("frame.json"),
                    serde_json::to_vec(&metadata)?,
                )?;
            }
            host.frame = Some(metadata);
            Ok(())
        })();
        if let Err(error) = result {
            eprintln!("frame callback: {error}");
            return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
        }
        SINOMO_MODULE_STATUS_OK
    }
    struct Provider {
        library: *mut c_void,
        invoke: SinomoModuleInvokeFn,
        free: SinomoModuleFreeBufferFn,
        shutdown: SinomoModuleShutdownFn,
    }
    impl Provider {
        unsafe fn open(path: &str, host: &mut Host) -> Result<Self, Box<dyn std::error::Error>> {
            let name = CString::new(path)?;
            let library = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
            if library.is_null() {
                let detail = unsafe { std::ffi::CStr::from_ptr(libc::dlerror()) };
                return Err(detail.to_string_lossy().into_owned().into());
            }
            unsafe fn symbol<T: Copy>(library: *mut c_void, name: &std::ffi::CStr) -> T {
                let address = unsafe { libc::dlsym(library, name.as_ptr()) };
                assert!(!address.is_null(), "missing native ABI symbol {name:?}");
                assert_eq!(std::mem::size_of::<T>(), std::mem::size_of_val(&address));
                unsafe { std::mem::transmute_copy(&address) }
            }
            let version: SinomoModuleApiVersionFn =
                unsafe { symbol(library, c"sinomo_module_api_version") };
            assert_eq!(unsafe { version() }, SINOMO_NATIVE_MODULE_ABI_VERSION);
            let init: SinomoModuleInitFn = unsafe { symbol(library, c"sinomo_module_init") };
            let mut api = SinomoModuleHostApiV2::empty();
            api.user_data = (host as *mut Host).cast();
            api.local_permission_granted = Some(permission);
            api.output_chunk = Some(output);
            assert_eq!(unsafe { init(api) }, SINOMO_MODULE_STATUS_OK);
            Ok(Self {
                library,
                invoke: unsafe { symbol(library, c"sinomo_module_invoke") },
                free: unsafe { symbol(library, c"sinomo_module_free_buffer") },
                shutdown: unsafe { symbol(library, c"sinomo_module_shutdown") },
            })
        }
        fn invoke(
            &self,
            request: &DesktopProviderRequest,
        ) -> Result<DesktopProviderResponse, Box<dyn std::error::Error>> {
            let bytes = encode_sobj(&NativeModuleRequest::Invoke(
                NativeModuleInvocationRequest {
                    operation: APP_USE_OS_OPERATION.into(),
                    payload_sobj: encode_sobj(request)?,
                    host_data_dir_root: None,
                    cancellation_wait_handle: None,
                    request_body: None,
                    response_body_path: None,
                },
            ))?;
            let mut buffer = SinomoModuleByteBuffer::null();
            let status =
                unsafe { (self.invoke)(SinomoModuleByteSlice::from_slice(&bytes), &mut buffer) };
            if status != SINOMO_MODULE_STATUS_OK {
                return Err(format!("ABI status {status}").into());
            }
            let response = decode_sobj::<NativeModuleResponse>(unsafe {
                std::slice::from_raw_parts(buffer.ptr, buffer.len)
            });
            unsafe { (self.free)(buffer) };
            match response? {
                NativeModuleResponse::Invocation(response) => {
                    Ok(decode_sobj(&response.payload_sobj)?)
                }
                other => Err(format!("unexpected native response: {other:?}").into()),
            }
        }
    }
    impl Drop for Provider {
        fn drop(&mut self) {
            let status = unsafe { (self.shutdown)() };
            assert_eq!(
                status, SINOMO_MODULE_STATUS_OK,
                "native shutdown must drain OS workers"
            );
            assert_eq!(unsafe { libc::dlclose(self.library) }, 0);
        }
    }
    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 4 {
            return Err(
                "usage: android_native_probe PROVIDER_SO PERMISSION_STORE FRAME_DIR".into(),
            );
        }
        let store = LocalNodePermissionStore::from_path(&args[2]);
        std::fs::create_dir_all(&args[3])?;
        let mut host = Box::new(Host {
            permission: permission_store_capability(&args[2]),
            frame: None,
            output: args[3].clone().into(),
            save: false,
        });
        let mut provider = Some(unsafe { Provider::open(&args[1], &mut host)? });
        println!("{}", json!({"probe":"ready","uid":unsafe {libc::getuid()}}));
        for line in std::io::stdin().lock().lines() {
            let line = line?;
            let outcome = (|| -> Result<Value, Box<dyn std::error::Error>> {
                if line.len() > 256 * 1024 {
                    return Err("probe request exceeds limit".into());
                }
                let value: Value = serde_json::from_str(&line)?;
                match value["probe"].as_str() {
                    Some("unload") => {
                        drop(provider.take());
                        return Ok(json!({"unloaded":true}));
                    }
                    Some("reload") => {
                        drop(provider.take());
                        provider = Some(unsafe { Provider::open(&args[1], &mut host)? });
                        return Ok(json!({"reloaded":true}));
                    }
                    Some("permissions") => {
                        let p = store.load()?;
                        return Ok(
                            json!({"remote_management":p.remote_management_enabled(),"resources":p.resource_exposures()}),
                        );
                    }
                    // Local test controls use the real permission-store mutation
                    // API. They are not registered in the shipped app or provider.
                    Some("remote_management") => {
                        let enabled = value["enabled"].as_bool().ok_or("missing enabled")?;
                        store.mutate(|p| Ok(p.set_remote_management_enabled(enabled)))?;
                        return Ok(json!({"remote_management":enabled}));
                    }
                    Some("revoke") => {
                        let resource = value["resource"].as_str().ok_or("missing resource")?;
                        let operation = value["operation"].as_str().ok_or("missing operation")?;
                        store.mutate(|p| p.hide_resource_operation(resource, operation))?;
                        return Ok(json!({"revoked":true}));
                    }
                    Some("expose") => {
                        let resource = value["resource"].as_str().ok_or("missing resource")?;
                        let operation = value["operation"].as_str().ok_or("missing operation")?;
                        store.mutate(|p| p.expose_resource_operation(resource, operation))?;
                        return Ok(json!({"exposed":true}));
                    }
                    Some(other) => return Err(format!("unknown probe control {other}").into()),
                    None => {}
                }
                host.frame = None;
                host.save = value["save_frame"].as_bool().unwrap_or(false);
                let command = serde_json::from_value(value["command"].clone())?;
                let response = provider.as_ref().ok_or("provider unloaded")?.invoke(
                    &DesktopProviderRequest {
                        permission_store: args[2].clone(),
                        command,
                    },
                )?;
                Ok(json!({"response":response,"frame":host.frame.take()}))
            })();
            println!(
                "{}",
                outcome.unwrap_or_else(|error| json!({"probe_error":error.to_string()}))
            );
            std::io::stdout().flush()?;
        }
        Ok(())
    }
}
