//! Generic binding from an active native component offer to the module ABI.
//!
//! Domain crates own their typed request and response protocols. This module
//! owns the common provider-neutral work: select exactly one active component,
//! verify its content-addressed descriptor, load it, and invoke one SOBJ
//! operation through the native-module ABI.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use serde::{Serialize, de::DeserializeOwned};
use sinomo_module_api::{
    NativeModuleDescriptor, NativeModuleErrorResponse, NativeModuleInvocationRequest,
    NativeModuleRequest, NativeModuleResponse, ServiceOffer, ServiceRequirement, decode_sobj,
    encode_sobj,
};
use thiserror::Error;

use crate::{
    ModuleLoaderStoreError, NativeModule, NativeModuleHostCallbacks, NativeModuleHostError,
    NativeModuleUnloadReport, native_module_cache_path, read_native_dylib_module_loader_states,
};

#[derive(Debug, Error)]
pub enum ActiveNativeComponentError {
    #[error(transparent)]
    Store(#[from] ModuleLoaderStoreError),
    #[error(transparent)]
    Host(#[from] NativeModuleHostError),
    #[error("invalid native component requirement: {0}")]
    Requirement(String),
    #[error("active native component loader state does not match its descriptor: {0}")]
    StateDescriptor(String),
    #[error(
        "no active component satisfies service `{service_id}` interface `{interface_id}` version `{interface_version}`"
    )]
    NotFound {
        service_id: String,
        interface_id: String,
        interface_version: String,
    },
    #[error(
        "{count} active components satisfy service `{service_id}` interface `{interface_id}` version `{interface_version}`; provider selection is ambiguous"
    )]
    Ambiguous {
        service_id: String,
        interface_id: String,
        interface_version: String,
        count: usize,
    },
    #[error("component `{component_id}` does not support native operation `{operation}`")]
    UnsupportedOperation {
        component_id: String,
        operation: String,
    },
    #[error("encode native component operation `{operation}`: {detail}")]
    Encode { operation: String, detail: String },
    #[error("decode native component operation `{operation}`: {detail}")]
    Decode { operation: String, detail: String },
    #[error("native component operation `{operation}` returned an unexpected response: {detail}")]
    UnexpectedResponse { operation: String, detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActiveNativeComponentResponse<T> {
    Value(T),
    Error(NativeModuleErrorResponse),
}

/// One loaded native module bound to the exact offer that satisfied a caller's
/// requirement.
#[derive(Clone)]
pub struct ActiveNativeComponent {
    module: Arc<NativeModule>,
    offer: ServiceOffer,
    _invocation_lease: Option<crate::ModuleInvocationLease>,
    cancellation_registry: Option<crate::NativeModuleOperationCancellationRegistry>,
}

impl ActiveNativeComponent {
    /// Resolve and load the single active component satisfying `requirement`.
    ///
    /// # Safety
    ///
    /// Native modules share the host process address space. Loader state must
    /// contain only artifacts that already passed policy and provenance checks.
    pub unsafe fn resolve(
        data_dir_root: impl AsRef<Path>,
        requirement: &ServiceRequirement,
    ) -> Result<Self, ActiveNativeComponentError> {
        let candidate = resolve_active_candidate(data_dir_root.as_ref(), requirement)?;
        let module = unsafe {
            NativeModule::load_from_content_addressed_path(
                &candidate.library_path,
                &candidate.content_hash,
            )
        }?;
        Ok(Self {
            module: Arc::new(module),
            offer: candidate.offer,
            _invocation_lease: candidate.lease,
            cancellation_registry: candidate.cancellation_registry,
        })
    }

    /// Revalidate activation, then lease an invocation-scoped provider shared
    /// with concurrent callers. Weak caching releases the image after the last
    /// call, without pinning libraries for the lifetime of agent sessions.
    ///
    /// # Safety
    /// The same trust requirements as `resolve` apply. The provider must use
    /// the scoped invocation host for all per-invocation host calls.
    pub unsafe fn resolve_shared(
        data_dir_root: impl AsRef<Path>,
        requirement: &ServiceRequirement,
    ) -> Result<Self, ActiveNativeComponentError> {
        static MODULES: OnceLock<Mutex<std::collections::BTreeMap<PathBuf, Weak<NativeModule>>>> =
            OnceLock::new();
        let candidate = resolve_active_candidate(data_dir_root.as_ref(), requirement)?;
        let key = std::fs::canonicalize(&candidate.library_path)
            .map_err(|e| ActiveNativeComponentError::StateDescriptor(e.to_string()))?;
        let mut modules = MODULES.get_or_init(Default::default).lock().map_err(|_| {
            ActiveNativeComponentError::StateDescriptor("shared native bindings poisoned".into())
        })?;
        modules.retain(|_, module| module.strong_count() > 0);
        let module = if let Some(module) = modules.get(&key).and_then(Weak::upgrade) {
            module
        } else {
            let module = Arc::new(unsafe {
                NativeModule::load_from_content_addressed_path(
                    &candidate.library_path,
                    &candidate.content_hash,
                )
            }?);
            if !module.supports_invocation_host() {
                return Err(ActiveNativeComponentError::StateDescriptor(
                    "shared calls require the scoped invocation entrypoint".into(),
                ));
            }
            modules.insert(key, Arc::downgrade(&module));
            module
        };
        Ok(Self {
            module,
            offer: candidate.offer,
            _invocation_lease: candidate.lease,
            cancellation_registry: candidate.cancellation_registry,
        })
    }

    /// Resolve and load one active component while retaining its Windows image
    /// mapping until process exit. This is reserved for providers whose runtime
    /// callbacks may outlive a completed invocation or shutdown hook.
    ///
    /// # Safety
    ///
    /// The same trust requirements as [`Self::resolve`] apply. The mapping
    /// lifetime is intentionally irreversible for this process.
    #[cfg(windows)]
    pub unsafe fn resolve_process_resident(
        data_dir_root: impl AsRef<Path>,
        requirement: &ServiceRequirement,
    ) -> Result<Self, ActiveNativeComponentError> {
        let candidate = resolve_active_candidate(data_dir_root.as_ref(), requirement)?;
        let module = unsafe {
            NativeModule::load_process_resident_from_content_addressed_path(
                &candidate.library_path,
                &candidate.content_hash,
            )
        }?;
        Ok(Self {
            module: Arc::new(module),
            offer: candidate.offer,
            _invocation_lease: candidate.lease,
            cancellation_registry: candidate.cancellation_registry,
        })
    }

    /// Bind an already-loaded native module to a generic service requirement.
    pub fn from_native_module(
        module: NativeModule,
        requirement: &ServiceRequirement,
    ) -> Result<Self, ActiveNativeComponentError> {
        requirement
            .validate()
            .map_err(|error| ActiveNativeComponentError::Requirement(error.to_string()))?;
        let offer = matching_offer(module.descriptor(), requirement).ok_or_else(|| {
            ActiveNativeComponentError::NotFound {
                service_id: requirement
                    .service_id
                    .clone()
                    .unwrap_or_else(|| "*".to_string()),
                interface_id: requirement.interface_id.clone(),
                interface_version: requirement.interface_version.clone(),
            }
        })?;
        Ok(Self {
            module: Arc::new(module),
            offer,
            _invocation_lease: None,
            cancellation_registry: None,
        })
    }

    /// Load a trusted provider artifact whose bytes were verified by the
    /// caller, then bind it to `requirement`.
    ///
    /// # Safety
    ///
    /// Native modules share the host process address space. The caller must
    /// establish artifact trust and provenance before invoking this function.
    pub unsafe fn load_verified(
        library_path: impl AsRef<Path>,
        requirement: &ServiceRequirement,
    ) -> Result<Self, ActiveNativeComponentError> {
        let module = unsafe { NativeModule::load_verified(library_path) }?;
        Self::from_native_module(module, requirement)
    }

    /// Load a trusted provider from a content-addressed path, verify its hash,
    /// and bind it to `requirement`.
    ///
    /// # Safety
    ///
    /// Native modules share the host process address space. The caller must
    /// establish artifact trust and provenance before invoking this function.
    pub unsafe fn load_from_content_addressed_path(
        library_path: impl AsRef<Path>,
        expected_content_hash: &str,
        requirement: &ServiceRequirement,
    ) -> Result<Self, ActiveNativeComponentError> {
        let module = unsafe {
            NativeModule::load_from_content_addressed_path(library_path, expected_content_hash)
        }?;
        Self::from_native_module(module, requirement)
    }

    pub fn descriptor(&self) -> &NativeModuleDescriptor {
        self.module.descriptor()
    }

    pub fn offer(&self) -> &ServiceOffer {
        &self.offer
    }

    pub fn invoke_sobj<Request, Response>(
        &self,
        operation: &str,
        request: &Request,
        callbacks: Option<Arc<dyn NativeModuleHostCallbacks>>,
    ) -> Result<ActiveNativeComponentResponse<Response>, ActiveNativeComponentError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        if !self
            .module
            .descriptor()
            .supported_operations
            .iter()
            .any(|candidate| candidate == operation)
        {
            return Err(ActiveNativeComponentError::UnsupportedOperation {
                component_id: self.module.descriptor().component.component_id.clone(),
                operation: operation.to_string(),
            });
        }
        let payload_sobj =
            encode_sobj(request).map_err(|error| ActiveNativeComponentError::Encode {
                operation: operation.to_string(),
                detail: error.to_string(),
            })?;
        let cancellation_registration = self
            .cancellation_registry
            .as_ref()
            .map(|registry| {
                let id = crate::parse_native_dylib_module_id(&self.module.descriptor().module_id)
                    .map_err(|e| ActiveNativeComponentError::StateDescriptor(e.to_string()))?;
                registry
                    .register(id.to_string(), None)
                    .map_err(|e| ActiveNativeComponentError::StateDescriptor(e.to_string()))
            })
            .transpose()?;
        // Register cancellation before checking admission so drain cannot
        // miss a call between its state fence and cancellation sweep. The
        // binding lease already protects physical execution; do not count it twice.
        if let Some(lease) = &self._invocation_lease {
            lease
                .ensure_active()
                .map_err(|e| ActiveNativeComponentError::StateDescriptor(e.to_string()))?;
        }
        let callbacks = if let Some(registration) = &cancellation_registration {
            Some(Arc::new(InvocationCancellationCallbacks {
                parent: callbacks,
                token: registration.token(),
            }) as Arc<dyn NativeModuleHostCallbacks>)
        } else {
            callbacks
        };
        let cancellation_wait_handle = callbacks
            .as_ref()
            .and_then(|callbacks| callbacks.cancellation_wait_handle());
        let native_request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: operation.to_string(),
            payload_sobj,
            host_data_dir_root: None,
            cancellation_wait_handle,
            request_body: None,
            response_body_path: None,
        });
        match self
            .module
            .invoke_with_callbacks(&native_request, callbacks)?
        {
            NativeModuleResponse::Invocation(response) if response.operation == operation => {
                let response = decode_sobj(&response.payload_sobj).map_err(|error| {
                    ActiveNativeComponentError::Decode {
                        operation: operation.to_string(),
                        detail: error.to_string(),
                    }
                })?;
                Ok(ActiveNativeComponentResponse::Value(response))
            }
            NativeModuleResponse::Error(error) => Ok(ActiveNativeComponentResponse::Error(error)),
            response => Err(ActiveNativeComponentError::UnexpectedResponse {
                operation: operation.to_string(),
                detail: format!("{response:?}"),
            }),
        }
    }

    pub fn unload(self) -> NativeModuleUnloadReport {
        match Arc::try_unwrap(self.module) {
            Ok(module) => module.unload(),
            Err(module) => NativeModuleUnloadReport {
                module_id: module.descriptor().module_id.clone(),
                interface_version: module.descriptor().interface_version.clone(),
                shutdown_called: false,
                shutdown_ok: false,
                shutdown_status: None,
                detail: "native provider still has active binding leases".into(),
                library_handle_released: false,
            },
        }
    }
}

struct ActiveNativeComponentCandidate {
    lease: Option<crate::ModuleInvocationLease>,
    cancellation_registry: Option<crate::NativeModuleOperationCancellationRegistry>,
    content_hash: String,
    library_path: PathBuf,
    offer: ServiceOffer,
}

fn resolve_active_candidate(
    data_dir_root: &Path,
    requirement: &ServiceRequirement,
) -> Result<ActiveNativeComponentCandidate, ActiveNativeComponentError> {
    requirement
        .validate()
        .map_err(|error| ActiveNativeComponentError::Requirement(error.to_string()))?;
    let states = read_native_dylib_module_loader_states(data_dir_root)?;
    if let Some(host) = crate::process_module_host(data_dir_root)
        .map_err(ActiveNativeComponentError::StateDescriptor)?
    {
        let generation = host.generations.active_snapshot();
        let candidates: Vec<_> = generation
            .module_registry
            .registry()
            .registrations()
            .filter_map(|registration| {
                let crate::ModuleBackendRegistration::NativeDylib { content_hash, .. } =
                    &registration.backend
                else {
                    return None;
                };
                registration
                    .component
                    .offers
                    .iter()
                    .find(|offer| requirement.is_satisfied_by(offer))
                    .map(|offer| (registration, content_hash, offer.clone()))
            })
            .collect();
        if candidates.len() > 1 {
            return Err(ActiveNativeComponentError::Ambiguous {
                service_id: requirement.service_id.clone().unwrap_or_else(|| "*".into()),
                interface_id: requirement.interface_id.clone(),
                interface_version: requirement.interface_version.clone(),
                count: candidates.len(),
            });
        }
        let Some((registration, hash, offer)) = candidates.into_iter().next() else {
            return Err(ActiveNativeComponentError::NotFound {
                service_id: requirement.service_id.clone().unwrap_or_else(|| "*".into()),
                interface_id: requirement.interface_id.clone(),
                interface_version: requirement.interface_version.clone(),
            });
        };
        let lease = host
            .invocations
            .acquire(registration.module_id().as_str())
            .map_err(|e| ActiveNativeComponentError::StateDescriptor(e.to_string()))?;
        let state = states
            .iter()
            .find(|state| {
                &state.module_id == registration.module_id() && &state.content_hash == hash
            })
            .ok_or_else(|| {
                ActiveNativeComponentError::StateDescriptor(
                    "resident native artifact is not the applied loader artifact".into(),
                )
            })?;
        return Ok(ActiveNativeComponentCandidate {
            lease: Some(lease),
            cancellation_registry: Some(host.native_operations.clone()),
            content_hash: state.content_hash.clone(),
            library_path: native_module_cache_path(
                data_dir_root,
                &state.cache_key,
                &state.content_hash,
                &state.library_name,
            ),
            offer,
        });
    }
    let mut candidates = Vec::new();
    for state in states {
        let library_path = native_module_cache_path(
            data_dir_root,
            &state.cache_key,
            &state.content_hash,
            &state.library_name,
        );
        let descriptor = unsafe {
            NativeModule::descriptor_from_content_addressed_path(&library_path, &state.content_hash)
        }?;
        crate::validate_native_dylib_descriptor_matches_state(&state, &descriptor)
            .map_err(|error| ActiveNativeComponentError::StateDescriptor(error.to_string()))?;
        if let Some(offer) = matching_offer(&descriptor, requirement) {
            candidates.push(ActiveNativeComponentCandidate {
                lease: None,
                cancellation_registry: None,
                content_hash: state.content_hash,
                library_path,
                offer,
            });
        }
    }
    match candidates.len() {
        0 => Err(ActiveNativeComponentError::NotFound {
            service_id: requirement
                .service_id
                .clone()
                .unwrap_or_else(|| "*".to_string()),
            interface_id: requirement.interface_id.clone(),
            interface_version: requirement.interface_version.clone(),
        }),
        1 => Ok(candidates.pop().expect("one candidate")),
        count => Err(ActiveNativeComponentError::Ambiguous {
            service_id: requirement
                .service_id
                .clone()
                .unwrap_or_else(|| "*".to_string()),
            interface_id: requirement.interface_id.clone(),
            interface_version: requirement.interface_version.clone(),
            count,
        }),
    }
}

fn matching_offer(
    descriptor: &NativeModuleDescriptor,
    requirement: &ServiceRequirement,
) -> Option<ServiceOffer> {
    descriptor
        .component
        .offers
        .iter()
        .find(|offer| requirement.is_satisfied_by(offer))
        .cloned()
}

struct InvocationCancellationCallbacks {
    parent: Option<Arc<dyn NativeModuleHostCallbacks>>,
    token: crate::NativeModuleOperationCancellationToken,
}
impl NativeModuleHostCallbacks for InvocationCancellationCallbacks {
    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.is_cancelled())
    }
    fn host_service(
        &self,
        request: &sinomo_module_api::host::HostServiceRequest,
    ) -> Result<Vec<u8>, String> {
        if self.is_cancelled() {
            return Err("native invocation cancelled".into());
        }
        self.parent
            .as_ref()
            .ok_or("host service unavailable")?
            .host_service(request)
    }
    fn output_payload(&self, payload: &[u8]) {
        if let Some(parent) = &self.parent {
            parent.output_payload(payload);
        }
    }
    fn output_payload_status(&self, payload: &[u8]) -> bool {
        !self.is_cancelled()
            && self
                .parent
                .as_ref()
                .is_none_or(|parent| parent.output_payload_status(payload))
    }
    fn log(&self, message: &[u8]) {
        if let Some(parent) = &self.parent {
            parent.log(message);
        }
    }
    fn cancellation_wait_handle(&self) -> Option<u64> {
        self.parent
            .as_ref()
            .and_then(|parent| parent.cancellation_wait_handle())
    }
    fn local_permission_granted(&self, permission: &str) -> bool {
        !self.is_cancelled()
            && self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.local_permission_granted(permission))
    }
}
