//! Generic binding from an active native component offer to the module ABI.
//!
//! Domain crates own their typed request and response protocols. This module
//! owns the common provider-neutral work: select exactly one active component,
//! verify its content-addressed descriptor, load it, and invoke one SOBJ
//! operation through the native-module ABI.

use std::path::{Path, PathBuf};
use std::sync::Arc;

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
pub struct ActiveNativeComponent {
    module: NativeModule,
    offer: ServiceOffer,
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
            module,
            offer: candidate.offer,
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
            module,
            offer: candidate.offer,
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
        Ok(Self { module, offer })
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
        let native_request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: operation.to_string(),
            payload_sobj,
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
        self.module.unload()
    }
}

struct ActiveNativeComponentCandidate {
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
