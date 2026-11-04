//! Invocation-scoped host services. The optional entrypoint extends ABI v2
//! without changing its layout; providers must not retain this borrowed handle.
use crate::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const INVOCATION_HOST_VERSION: u32 = 1;
pub const MAX_HOST_CALL_BYTES: usize = 4 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostServiceRequest {
    pub interface_id: String,
    pub interface_version: String,
    pub operation: String,
    pub payload_sobj: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HostServiceResponse {
    Value(Vec<u8>),
    Error(String),
}
pub type HostServiceCallFn = unsafe extern "C" fn(
    *mut c_void,
    SinomoModuleByteSlice,
    *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct InvocationHost {
    pub version: u32,
    pub user_data: *mut c_void,
    pub call: HostServiceCallFn,
    pub free: SinomoModuleFreeBufferFn,
    pub is_cancelled: unsafe extern "C" fn(*mut c_void) -> u8,
}
pub type InvokeWithHostFn = unsafe extern "C" fn(
    SinomoModuleByteSlice,
    InvocationHost,
    *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode;
impl InvocationHost {
    /// # Safety
    /// The borrowed invocation handle must still be live.
    pub unsafe fn cancelled(&self) -> bool {
        self.version != INVOCATION_HOST_VERSION
            || unsafe { (self.is_cancelled)(self.user_data) != 0 }
    }

    /// # Safety
    /// The handle must come from the host of this invocation and must remain
    /// live until this synchronous call returns. Never store it or cross threads.
    pub unsafe fn call_sobj<Q: Serialize, R: DeserializeOwned>(
        &self,
        interface: &str,
        version: &str,
        operation: &str,
        request: &Q,
    ) -> Result<R, NativeModuleApiError> {
        if self.version != INVOCATION_HOST_VERSION {
            return Err(NativeModuleApiError::Runtime(
                "unsupported invocation host".into(),
            ));
        }
        let bytes = encode_sobj(&HostServiceRequest {
            interface_id: interface.into(),
            interface_version: version.into(),
            operation: operation.into(),
            payload_sobj: encode_sobj(request)?,
        })?;
        if bytes.len() > MAX_HOST_CALL_BYTES {
            return Err(NativeModuleApiError::Runtime(
                "host request exceeds limit".into(),
            ));
        }
        let mut out = SinomoModuleByteBuffer::null();
        let status = unsafe {
            (self.call)(
                self.user_data,
                SinomoModuleByteSlice::from_slice(&bytes),
                &mut out,
            )
        };
        let result = if out.len > MAX_HOST_CALL_BYTES {
            Err(NativeModuleApiError::Runtime(
                "host response exceeds limit".into(),
            ))
        } else if status != SINOMO_MODULE_STATUS_OK {
            Err(NativeModuleApiError::Runtime(
                "host service unavailable".into(),
            ))
        } else {
            decode_sobj::<HostServiceResponse>(unsafe {
                SinomoModuleByteSlice {
                    ptr: out.ptr,
                    len: out.len,
                }
                .as_slice()
            })
        };
        unsafe {
            (self.free)(out);
        }
        match result? {
            HostServiceResponse::Value(bytes) => decode_sobj(&bytes),
            HostServiceResponse::Error(error) => Err(NativeModuleApiError::Runtime(error)),
        }
    }
}
