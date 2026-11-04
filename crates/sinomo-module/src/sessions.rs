//! Invocation-scoped session services. Domain hosts implement the effect owner;
//! native modules receive only the checked host-call bridge.
use sinomo_module_api::{decode_sobj, encode_sobj, host::HostServiceRequest, sessions::*};
use std::sync::Arc;

pub trait SessionAccess: Send + Sync {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn request(&self, request: SessionRequest) -> Result<serde_json::Value, String>;
}
pub struct SessionHostCallbacks {
    pub sessions: Arc<dyn SessionAccess>,
}
impl crate::NativeModuleHostCallbacks for SessionHostCallbacks {
    fn is_cancelled(&self) -> bool {
        self.sessions.is_cancelled()
    }
    fn host_service(&self, request: &HostServiceRequest) -> Result<Vec<u8>, String> {
        if request.interface_id != SESSION_INTERFACE
            || request.interface_version != SESSION_VERSION
            || request.operation != SESSION_ACCESS
        {
            return Err("undeclared session service".into());
        }
        let request = decode_sobj(&request.payload_sobj).map_err(|e| e.to_string())?;
        encode_sobj(&self.sessions.request(request)?).map_err(|e| e.to_string())
    }
}
