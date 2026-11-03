use crate::NativeModuleOperationCancellation;
use sinomo_core::network::{NetworkError, ObjectRpcCancelRequest, ObjectRpcCancelResponse};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Per-invocation cancellation signal shared with native host callbacks and
/// streamed request-body staging.
#[derive(Clone, Debug, Default)]
pub struct NativeModuleOperationCancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl NativeModuleOperationCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn signal(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl NativeModuleOperationCancellation for NativeModuleOperationCancellationToken {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
}

/// Process-resident registry for cancel-addressable native module operations.
///
/// Hosts clone this lightweight handle into dispatch and cancel routes. The
/// registry owns uniqueness and cancellation semantics; hosts only decide how
/// an authenticated cancel request reaches it.
#[derive(Clone, Debug, Default)]
pub struct NativeModuleOperationCancellationRegistry {
    active: Arc<Mutex<BTreeMap<String, ActiveNativeModuleOperation>>>,
    next_unnamed_id: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
struct ActiveNativeModuleOperation {
    module_id: String,
    token: NativeModuleOperationCancellationToken,
}

impl NativeModuleOperationCancellationRegistry {
    pub fn register(
        &self,
        module_id: impl Into<String>,
        operation_id: Option<String>,
    ) -> Result<NativeModuleOperationCancellationRegistration, NetworkError> {
        let module_id = module_id.into();
        let token = NativeModuleOperationCancellationToken::new();
        let registry_key = operation_id.clone().unwrap_or_else(|| {
            format!(
                "unnamed:{module_id}:{}",
                self.next_unnamed_id.fetch_add(1, Ordering::Relaxed)
            )
        });
        let mut active = self.active.lock().map_err(|_| NetworkError::Io {
            operation: "register native dylib module cancellation".to_string(),
            detail: "native dylib operation cancellation registry lock poisoned".to_string(),
        })?;
        if operation_id.is_some() && active.contains_key(&registry_key) {
            return Err(NetworkError::Serialization {
                detail: format!("native dylib module operation `{registry_key}` is already active"),
            });
        }
        active.insert(
            registry_key.clone(),
            ActiveNativeModuleOperation {
                module_id,
                token: token.clone(),
            },
        );
        Ok(NativeModuleOperationCancellationRegistration {
            registry: self.clone(),
            registry_key,
            token,
        })
    }

    pub fn cancel_request(
        &self,
        metadata_operation_id: Option<String>,
        payload: ObjectRpcCancelRequest,
    ) -> Result<ObjectRpcCancelResponse, NetworkError> {
        let operation_id = payload.operation_id;
        if metadata_operation_id
            .as_deref()
            .is_some_and(|metadata| metadata != operation_id)
        {
            return Ok(ObjectRpcCancelResponse {
                operation_id,
                cancelled: false,
                detail: "cancel metadata operation id does not match request body".to_string(),
            });
        }
        let token = self
            .active
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "cancel native dylib module operation".to_string(),
                detail: "native dylib operation cancellation registry lock poisoned".to_string(),
            })?
            .get(&operation_id)
            .map(|active| active.token.clone());
        let (cancelled, detail) = if let Some(token) = token {
            token.cancel();
            (true, payload.reason)
        } else {
            (
                false,
                "native dylib module operation is not active".to_string(),
            )
        };
        Ok(ObjectRpcCancelResponse {
            operation_id,
            cancelled,
            detail,
        })
    }

    pub fn is_active(&self, operation_id: &str) -> bool {
        self.active
            .lock()
            .map(|active| active.contains_key(operation_id))
            .unwrap_or(false)
    }

    pub fn active_len(&self) -> usize {
        self.active.lock().map(|active| active.len()).unwrap_or(0)
    }

    pub fn active_for_module(&self, module_id: &str) -> usize {
        self.active
            .lock()
            .map(|active| {
                active
                    .values()
                    .filter(|operation| operation.module_id == module_id)
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn cancel_module(&self, module_id: &str) -> usize {
        let tokens = self
            .active
            .lock()
            .map(|active| {
                active
                    .values()
                    .filter(|operation| operation.module_id == module_id)
                    .map(|operation| operation.token.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for token in &tokens {
            token.cancel();
        }
        tokens.len()
    }

    fn unregister(&self, registry_key: &str) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(registry_key);
        }
    }
}

/// RAII owner for one registered invocation.
pub struct NativeModuleOperationCancellationRegistration {
    registry: NativeModuleOperationCancellationRegistry,
    registry_key: String,
    token: NativeModuleOperationCancellationToken,
}

impl NativeModuleOperationCancellationRegistration {
    pub fn token(&self) -> NativeModuleOperationCancellationToken {
        self.token.clone()
    }
}

impl Drop for NativeModuleOperationCancellationRegistration {
    fn drop(&mut self) {
        // Dropping the async dispatch future can detach a blocking invocation.
        // Signal callbacks before removing the addressable registry entry so
        // synchronous work and staged-file lifetimes can stop promptly.
        self.token.cancel();
        self.registry.unregister(&self.registry_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_is_unique_and_drop_cancels_then_unregisters() {
        let registry = NativeModuleOperationCancellationRegistry::default();
        let registration = registry
            .register("module.one", Some("operation.one".to_string()))
            .expect("register operation");
        let token = registration.token();
        assert!(registry.is_active("operation.one"));
        assert!(!token.is_cancelled());
        assert!(
            registry
                .register("module.one", Some("operation.one".to_string()))
                .is_err()
        );

        drop(registration);

        assert!(token.is_cancelled());
        assert!(!registry.is_active("operation.one"));
    }

    #[test]
    fn cancel_request_fences_metadata_and_signals_registered_operation() {
        let registry = NativeModuleOperationCancellationRegistry::default();
        let registration = registry
            .register("module.one", Some("operation.one".to_string()))
            .expect("register operation");
        let token = registration.token();
        let mismatch = registry
            .cancel_request(
                Some("operation.other".to_string()),
                ObjectRpcCancelRequest {
                    operation_id: "operation.one".to_string(),
                    reason: "stop".to_string(),
                },
            )
            .expect("mismatch response");
        assert!(!mismatch.cancelled);
        assert!(!token.is_cancelled());

        let cancelled = registry
            .cancel_request(
                Some("operation.one".to_string()),
                ObjectRpcCancelRequest {
                    operation_id: "operation.one".to_string(),
                    reason: "stop".to_string(),
                },
            )
            .expect("cancel response");
        assert!(cancelled.cancelled);
        assert!(token.is_cancelled());
    }

    #[test]
    fn module_drain_cancels_named_and_unnamed_native_operations() {
        let registry = NativeModuleOperationCancellationRegistry::default();
        let first = registry
            .register("module.one", Some("operation.one".to_string()))
            .expect("named operation");
        let second = registry
            .register("module.one", None)
            .expect("unnamed operation");
        let other = registry
            .register("module.two", Some("operation.two".to_string()))
            .expect("other operation");
        let first_token = first.token();
        let second_token = second.token();
        let other_token = other.token();

        assert_eq!(registry.active_for_module("module.one"), 2);
        assert_eq!(registry.cancel_module("module.one"), 2);
        assert!(first_token.is_cancelled());
        assert!(second_token.is_cancelled());
        assert!(!other_token.is_cancelled());
    }
}
