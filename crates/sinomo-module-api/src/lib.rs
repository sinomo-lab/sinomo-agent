//! Stable native-dylib ABI and generic SOBJ envelopes for Sinomo modules.
//!
//! This crate is deliberately small. Host binaries and trusted native modules
//! can depend on it without inheriting any dynamic loader, sandbox, Wasm, or
//! service implementation dependency.

mod component;
mod extension;
mod ui;

pub use component::*;
pub use extension::*;
pub use ui::*;

use std::collections::BTreeSet;
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use thiserror::Error;

pub const SINOMO_NATIVE_MODULE_ABI_VERSION: u32 = 2;
pub const SINOMO_MODULE_OBJECT_VERSION: u16 = 1;
/// Interface descriptors are catalog metadata, not an attachment transport.
/// Keep them independently bounded so a malformed module cannot make registry
/// discovery allocate an unbounded payload.
pub const MAX_MODULE_INTERFACE_DESCRIPTOR_BYTES: usize = 256 * 1024;

/// Module-local admission fence complementing the host's invocation leases.
/// A dylib uses one static instance to reject new ABI calls once shutdown
/// begins and to make shutdown fail rather than invalidate callbacks still in
/// use by an active invocation.
pub struct NativeModuleLifecycle {
    accepting_invocations: AtomicBool,
    active_invocations: AtomicUsize,
}

impl NativeModuleLifecycle {
    pub const fn new() -> Self {
        Self {
            accepting_invocations: AtomicBool::new(false),
            active_invocations: AtomicUsize::new(0),
        }
    }

    pub fn initialize(&self) -> bool {
        if self.active_invocations.load(Ordering::Acquire) != 0 {
            return false;
        }
        self.accepting_invocations.store(true, Ordering::Release);
        true
    }

    pub fn begin_invocation(&self) -> Option<NativeModuleInvocationGuard<'_>> {
        if !self.accepting_invocations.load(Ordering::Acquire) {
            return None;
        }
        self.active_invocations.fetch_add(1, Ordering::AcqRel);
        if !self.accepting_invocations.load(Ordering::Acquire) {
            self.active_invocations.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(NativeModuleInvocationGuard { lifecycle: self })
    }

    pub fn begin_shutdown(&self) -> bool {
        self.accepting_invocations.store(false, Ordering::Release);
        if self.active_invocations.load(Ordering::Acquire) == 0 {
            true
        } else {
            self.accepting_invocations.store(true, Ordering::Release);
            false
        }
    }

    pub fn active_invocations(&self) -> usize {
        self.active_invocations.load(Ordering::Acquire)
    }
}

impl Default for NativeModuleLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

pub struct NativeModuleInvocationGuard<'a> {
    lifecycle: &'a NativeModuleLifecycle,
}

impl Drop for NativeModuleInvocationGuard<'_> {
    fn drop(&mut self) {
        self.lifecycle
            .active_invocations
            .fetch_sub(1, Ordering::AcqRel);
    }
}

pub type SinomoModuleStatusCode = i32;

pub const SINOMO_MODULE_STATUS_OK: SinomoModuleStatusCode = 0;
pub const SINOMO_MODULE_STATUS_INVALID_ARGUMENT: SinomoModuleStatusCode = 1;
pub const SINOMO_MODULE_STATUS_UNSUPPORTED_ABI: SinomoModuleStatusCode = 2;
pub const SINOMO_MODULE_STATUS_RUNTIME_ERROR: SinomoModuleStatusCode = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoModuleByteSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl SinomoModuleByteSlice {
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }

    /// # Safety
    ///
    /// The caller must guarantee that `ptr` is valid for `len` bytes for the
    /// duration of the returned borrow.
    pub unsafe fn as_slice<'a>(self) -> &'a [u8] {
        if self.ptr.is_null() || self.len == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SinomoModuleByteBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl SinomoModuleByteBuffer {
    pub fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }

    pub fn from_vec(mut bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::null();
        }
        let buffer = Self {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
            cap: bytes.capacity(),
        };
        std::mem::forget(bytes);
        buffer
    }

    /// # Safety
    ///
    /// `buffer` must have been produced by [`SinomoModuleByteBuffer::from_vec`]
    /// in the same dynamic library that will reclaim it.
    pub unsafe fn into_vec(buffer: Self) -> Vec<u8> {
        if buffer.ptr.is_null() {
            return Vec::new();
        }
        unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.cap) }
    }
}

pub type SinomoModuleHostOutputFn = unsafe extern "C" fn(
    user_data: *mut c_void,
    payload_sobj: SinomoModuleByteSlice,
) -> SinomoModuleStatusCode;

pub type SinomoModuleHostLogFn = unsafe extern "C" fn(
    user_data: *mut c_void,
    message: SinomoModuleByteSlice,
) -> SinomoModuleStatusCode;

pub type SinomoModuleHostIsCancelledFn = unsafe extern "C" fn(user_data: *mut c_void) -> u8;

pub type SinomoModuleHostLocalPermissionGrantedFn =
    unsafe extern "C" fn(user_data: *mut c_void, permission: SinomoModuleByteSlice) -> u8;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinomoModuleHostApiV2 {
    pub abi_version: u32,
    pub user_data: *mut c_void,
    pub output_chunk: Option<SinomoModuleHostOutputFn>,
    pub log: Option<SinomoModuleHostLogFn>,
    pub is_cancelled: Option<SinomoModuleHostIsCancelledFn>,
    /// Fail-closed live check of a node-local permission at the final host
    /// effect boundary. Unknown names and unavailable state return false.
    pub local_permission_granted: Option<SinomoModuleHostLocalPermissionGrantedFn>,
}

impl SinomoModuleHostApiV2 {
    pub fn empty() -> Self {
        Self {
            abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            user_data: ptr::null_mut(),
            output_chunk: None,
            log: None,
            is_cancelled: None,
            local_permission_granted: None,
        }
    }
}

pub type SinomoModuleApiVersionFn = unsafe extern "C" fn() -> u32;
pub type SinomoModuleDescriptorFn =
    unsafe extern "C" fn(out: *mut SinomoModuleByteBuffer) -> SinomoModuleStatusCode;
pub type SinomoModuleInitFn =
    unsafe extern "C" fn(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode;
pub type SinomoModuleInvokeFn = unsafe extern "C" fn(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode;
pub type SinomoModuleShutdownFn = unsafe extern "C" fn() -> SinomoModuleStatusCode;
pub type SinomoModuleFreeBufferFn = unsafe extern "C" fn(buffer: SinomoModuleByteBuffer);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleDescriptor {
    pub module_id: String,
    pub module_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    pub native_abi_version: u32,
    pub backend: NativeModuleBackend,
    pub interface_version: String,
    pub platform_triple: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_operations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "NativeModuleSurfaces::is_empty")]
    pub surfaces: NativeModuleSurfaces,
    /// Backend-neutral lifecycle and service dependency contract. Legacy
    /// descriptors omit it and are adapted from their existing surfaces by the
    /// host until they are rebuilt.
    #[serde(default, skip_serializing_if = "ComponentDescriptor::is_empty")]
    pub component: ComponentDescriptor,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeModuleBackend {
    NativeDylib,
}

/// One versioned typed interface exported by a module.
///
/// The descriptor is deliberately generic: hosts discover memory, media, or
/// future extension protocols without teaching the module registry a
/// service-specific callback. `descriptor_sobj` is interpreted only by the
/// owner of `interface_id`.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleInterfaceExport {
    pub export_id: String,
    pub interface_id: String,
    pub interface_version: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<ModuleInterfaceOperationExport>,
    pub descriptor_sobj: Vec<u8>,
}

impl ModuleInterfaceExport {
    pub fn new(
        export_id: impl Into<String>,
        interface_id: impl Into<String>,
        interface_version: impl Into<String>,
        descriptor_sobj: Vec<u8>,
    ) -> Self {
        Self {
            export_id: export_id.into(),
            interface_id: interface_id.into(),
            interface_version: interface_version.into(),
            operations: Vec::new(),
            descriptor_sobj,
        }
    }

    pub fn with_operation(mut self, operation: ModuleInterfaceOperationExport) -> Self {
        self.operations.push(operation);
        self
    }

    pub fn validate(&self) -> Result<(), ModuleInterfaceContractError> {
        validate_interface_token(&self.export_id, "interface export id")
            .map_err(ModuleInterfaceContractError::InvalidText)?;
        validate_interface_token(&self.interface_id, "interface id")
            .map_err(ModuleInterfaceContractError::InvalidText)?;
        validate_interface_token(&self.interface_version, "interface version")
            .map_err(ModuleInterfaceContractError::InvalidText)?;
        if self.operations.is_empty() {
            return Err(ModuleInterfaceContractError::InvalidExport {
                export_id: self.export_id.clone(),
                detail: "at least one operation is required".to_string(),
            });
        }
        if self.descriptor_sobj.is_empty() {
            return Err(ModuleInterfaceContractError::InvalidExport {
                export_id: self.export_id.clone(),
                detail: "descriptor_sobj must be non-empty".to_string(),
            });
        }
        if self.descriptor_sobj.len() > MAX_MODULE_INTERFACE_DESCRIPTOR_BYTES {
            return Err(ModuleInterfaceContractError::InvalidExport {
                export_id: self.export_id.clone(),
                detail: format!(
                    "descriptor_sobj is {} bytes; the limit is {MAX_MODULE_INTERFACE_DESCRIPTOR_BYTES}",
                    self.descriptor_sobj.len()
                ),
            });
        }

        let mut logical_operations = BTreeSet::new();
        for operation in &self.operations {
            operation.validate().map_err(|error| {
                ModuleInterfaceContractError::InvalidOperation {
                    export_id: self.export_id.clone(),
                    logical_operation: operation.logical_operation.clone(),
                    detail: error.to_string(),
                }
            })?;
            if !logical_operations.insert(operation.logical_operation.as_str()) {
                return Err(ModuleInterfaceContractError::DuplicateLogicalOperation {
                    export_id: self.export_id.clone(),
                    logical_operation: operation.logical_operation.clone(),
                });
            }
        }
        Ok(())
    }
}

/// A bounded logical operation within a typed module interface.
///
/// Limits are part of the ABI contract. They are intentionally `u64` so the
/// interface itself does not impose a transport-sized ceiling; the declared
/// values are enforced by the service invoking the operation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleInterfaceOperationExport {
    pub logical_operation: String,
    pub target: ModuleInterfaceOperationTarget,
    pub request_schema_id: String,
    pub response_schema_id: String,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_facts_per_batch: u64,
    pub max_deadline_ms: u64,
    pub supports_cancellation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_schema_id: Option<String>,
}

impl ModuleInterfaceOperationExport {
    pub fn validate(&self) -> Result<(), ModuleInterfaceOperationError> {
        validate_interface_token(&self.logical_operation, "logical operation")
            .map_err(ModuleInterfaceOperationError::InvalidText)?;
        validate_interface_token(&self.request_schema_id, "request schema id")
            .map_err(ModuleInterfaceOperationError::InvalidText)?;
        validate_interface_token(&self.response_schema_id, "response schema id")
            .map_err(ModuleInterfaceOperationError::InvalidText)?;
        if let Some(schema_id) = &self.continuation_schema_id {
            validate_interface_token(schema_id, "continuation schema id")
                .map_err(ModuleInterfaceOperationError::InvalidText)?;
        }
        self.target.validate()?;
        if self.max_request_bytes == 0 {
            return Err(ModuleInterfaceOperationError::MissingLimit {
                field: "max_request_bytes",
            });
        }
        if self.max_response_bytes == 0 {
            return Err(ModuleInterfaceOperationError::MissingLimit {
                field: "max_response_bytes",
            });
        }
        if self.max_facts_per_batch == 0 {
            return Err(ModuleInterfaceOperationError::MissingLimit {
                field: "max_facts_per_batch",
            });
        }
        if self.max_deadline_ms == 0 {
            return Err(ModuleInterfaceOperationError::MissingLimit {
                field: "max_deadline_ms",
            });
        }
        Ok(())
    }
}

/// Runtime target for one logical typed-interface operation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ModuleInterfaceOperationTarget {
    /// Invoke `NativeModuleRequest::Invoke` with the named operation.
    NativeInvoke { operation: String },
    /// Dispatch through an exported module object-RPC route and operation.
    ObjectRpc { route: String, operation: String },
    /// Invoke a named entry in a sandbox process or Wasm component artifact.
    SandboxEntry { entry: String },
}

impl ModuleInterfaceOperationTarget {
    pub fn validate(&self) -> Result<(), ModuleInterfaceOperationError> {
        match self {
            Self::NativeInvoke { operation } => {
                validate_interface_token(operation, "native invoke operation")
                    .map_err(ModuleInterfaceOperationError::InvalidText)
            }
            Self::ObjectRpc { route, operation } => {
                if !route.starts_with('/')
                    || route != route.trim()
                    || route
                        .chars()
                        .any(|character| character.is_control() || character.is_whitespace())
                {
                    return Err(ModuleInterfaceOperationError::InvalidObjectRpcRoute {
                        route: route.clone(),
                    });
                }
                validate_interface_token(operation, "object RPC operation")
                    .map_err(ModuleInterfaceOperationError::InvalidText)
            }
            Self::SandboxEntry { entry } => validate_interface_token(entry, "sandbox entry")
                .map_err(ModuleInterfaceOperationError::InvalidText),
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ModuleInterfaceContractError {
    #[error("invalid module interface text: {0}")]
    InvalidText(String),
    #[error("module interface export `{export_id}` is invalid: {detail}")]
    InvalidExport { export_id: String, detail: String },
    #[error(
        "module interface export `{export_id}` operation `{logical_operation}` is invalid: {detail}"
    )]
    InvalidOperation {
        export_id: String,
        logical_operation: String,
        detail: String,
    },
    #[error(
        "module interface export `{export_id}` repeats logical operation `{logical_operation}`"
    )]
    DuplicateLogicalOperation {
        export_id: String,
        logical_operation: String,
    },
    #[error("duplicate module interface export id `{export_id}`")]
    DuplicateExportId { export_id: String },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ModuleInterfaceOperationError {
    #[error("{0}")]
    InvalidText(String),
    #[error("object RPC route `{route}` must be absolute and contain no control characters")]
    InvalidObjectRpcRoute { route: String },
    #[error("{field} must be greater than zero")]
    MissingLimit { field: &'static str },
}

pub fn validate_module_interface_exports(
    exports: &[ModuleInterfaceExport],
) -> Result<(), ModuleInterfaceContractError> {
    let mut export_ids = BTreeSet::new();
    for export in exports {
        export.validate()?;
        if !export_ids.insert(export.export_id.as_str()) {
            return Err(ModuleInterfaceContractError::DuplicateExportId {
                export_id: export.export_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_interface_token(value: &str, context: &str) -> Result<(), String> {
    if value.is_empty() || value != value.trim() {
        return Err(format!(
            "{context} must be non-empty without surrounding whitespace"
        ));
    }
    if !value.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | ':' | '/' | '@')
    }) {
        return Err(format!(
            "{context} must contain only ASCII letters, digits, '.', '-', '_', ':', '/', or '@'"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleSurfaces {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub object_rpc_routes: Vec<NativeModuleObjectRpcRoute>,
    /// Agent packages contributed by this module. Hosts validate role, tool,
    /// skill, and package semantics before merging them into the live catalog.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_definitions: Vec<NativeModuleAgentDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declarative_resources: Vec<NativeModuleDeclarativeResource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_owners: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_imports: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_exports: Vec<String>,
    /// SOBJ-encoded `sinomo_config::ConfigDefinition` records contributed by
    /// this module. Keeping the stable ABI payload SOBJ-native avoids making
    /// JSON or JSON Schema part of the module settings contract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settings: Vec<ModuleSettingsExport>,
    /// Cluster transport kinds this dylib serves as a runtime network module
    /// (e.g. `"iroh"`). Non-empty surfaces tell the host to resolve the
    /// dylib's network-module entry symbol in addition to the standard module
    /// entry points. Name-keyed SOBJ default keeps older descriptors
    /// compatible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_transports: Vec<String>,
    /// Generic typed interfaces exported by this module. The default is
    /// load-bearing for descriptors produced before interface discovery.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interface_exports: Vec<ModuleInterfaceExport>,
    /// Application UI contributions. Custom widgets reference immutable UI
    /// component artifacts and never carry native widget callbacks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ui_contributions: Vec<UiExtensionContribution>,
    /// Typed behavior or presentation extension points owned by this module.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extension_points: Vec<ExtensionPointDefinition>,
    /// Generic contributions targeting host- or module-owned extension points.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extension_contributions: Vec<ExtensionContribution>,
}

impl NativeModuleSurfaces {
    pub fn is_empty(&self) -> bool {
        self.object_rpc_routes.is_empty()
            && self.agent_definitions.is_empty()
            && self.declarative_resources.is_empty()
            && self.storage_owners.is_empty()
            && self.capability_imports.is_empty()
            && self.capability_exports.is_empty()
            && self.settings.is_empty()
            && self.network_transports.is_empty()
            && self.interface_exports.is_empty()
            && self.ui_contributions.is_empty()
            && self.extension_points.is_empty()
            && self.extension_contributions.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleSettingsExport {
    /// Duplicated outside the encoded definition for cheap catalog inspection
    /// and deterministic duplicate detection before decoding the payload.
    pub key: String,
    pub definition_sobj: Vec<u8>,
}

impl ModuleSettingsExport {
    pub fn new(key: impl Into<String>, definition_sobj: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            definition_sobj,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleObjectRpcRoute {
    pub route: String,
    pub category: String,
    pub operation: String,
    pub auth: String,
    pub request_body_mode: NativeModuleBodyMode,
    pub response_body_mode: NativeModuleBodyMode,
    /// Node-local permissions that must be granted before this route may run.
    ///
    /// The native-module ABI only carries these permission names. The owning
    /// node's policy layer decides what they mean and whether they are granted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_local_permissions: Vec<String>,
    /// Require the signed node id and public key to match one current member
    /// record exactly. Use for authority-sensitive data-plane operations where
    /// accepting a same-key legacy alias would weaken source binding.
    #[serde(default)]
    pub exact_member_identity: bool,
    /// Optional route-specific concurrency limit. Hosts apply their bounded
    /// native-module default when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_limit: Option<u32>,
    /// Optional route-specific wall-clock timeout. Long-running module
    /// operations must declare this explicitly instead of changing the host's
    /// default for every native module.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl NativeModuleObjectRpcRoute {
    pub fn new(
        route: impl Into<String>,
        category: impl Into<String>,
        operation: impl Into<String>,
        auth: impl Into<String>,
    ) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            operation: operation.into(),
            auth: auth.into(),
            request_body_mode: NativeModuleBodyMode::None,
            response_body_mode: NativeModuleBodyMode::None,
            required_local_permissions: Vec::new(),
            exact_member_identity: false,
            concurrency_limit: None,
            timeout_ms: None,
        }
    }

    pub fn with_request_body_mode(mut self, mode: NativeModuleBodyMode) -> Self {
        self.request_body_mode = mode;
        self
    }

    pub fn with_response_body_mode(mut self, mode: NativeModuleBodyMode) -> Self {
        self.response_body_mode = mode;
        self
    }

    pub fn require_local_permission(mut self, permission: impl Into<String>) -> Self {
        self.required_local_permissions.push(permission.into());
        self
    }

    pub fn require_local_permissions<I, S>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_local_permissions
            .extend(permissions.into_iter().map(Into::into));
        self
    }

    pub fn require_exact_member_identity(mut self) -> Self {
        self.exact_member_identity = true;
        self
    }

    pub fn with_concurrency_limit(mut self, concurrency_limit: u32) -> Self {
        self.concurrency_limit = Some(concurrency_limit);
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleAgentDefinition {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub role: String,
    pub system_prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<NativeModuleAgentTool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_pipeline_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_strategy_id: Option<String>,
}

impl NativeModuleAgentDefinition {
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        role: impl Into<String>,
        system_prompt: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            role: role.into(),
            system_prompt: system_prompt.into(),
            tools: Vec::new(),
            skills: Vec::new(),
            visible_toolsets: Vec::new(),
            context_pipeline_id: None,
            compaction_strategy_id: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_tool(mut self, tool: NativeModuleAgentTool) -> Self {
        self.tools.push(tool);
        self
    }

    pub fn with_skill(mut self, skill: impl Into<String>) -> Self {
        self.skills.push(skill.into());
        self
    }

    pub fn with_visible_toolset(mut self, toolset: impl Into<String>) -> Self {
        self.visible_toolsets.push(toolset.into());
        self
    }

    pub fn with_context_pipeline(mut self, pipeline_id: impl Into<String>) -> Self {
        self.context_pipeline_id = Some(pipeline_id.into());
        self
    }

    pub fn with_compaction_strategy(mut self, strategy_id: impl Into<String>) -> Self {
        self.compaction_strategy_id = Some(strategy_id.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleAgentTool {
    pub kind: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub async_default: bool,
}

impl NativeModuleAgentTool {
    pub fn new(
        kind: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        async_default: bool,
    ) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            description: description.into(),
            async_default,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeModuleBodyMode {
    None,
    Optional,
    Required,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleDeclarativeResource {
    pub id: String,
    pub kind: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Optional legacy discovery metadata. Hosts must not use this list to
    /// bind a skill to an agent or grant activation authority.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum NativeModuleRequest {
    ProbeCapabilities(NativeModuleProbeCapabilitiesRequest),
    Invoke(NativeModuleInvocationRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct NativeModuleProbeCapabilitiesRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeModuleInvocationRequest {
    pub operation: String,
    pub payload_sobj: Vec<u8>,
    /// Trusted host data-directory root for this invocation. This path is
    /// process-local context, not caller input, and modules must not return or
    /// persist it as remote authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_data_dir_root: Option<String>,
    /// Optional process-local wait handle which becomes signaled when the
    /// host cancels this invocation. This is an opaque native value and must
    /// never be persisted, returned to a remote caller, or used by a module
    /// running outside the host process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_wait_handle: Option<u64>,
    /// Host-private request attachment staged for this invocation.
    ///
    /// The path is local to the receiving host and must never be returned to a
    /// remote caller or persisted as module state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body: Option<NativeModuleStagedBody>,
    /// Host-assigned private path at which the module may stage a response
    /// attachment. The module must return the corresponding verified
    /// [`NativeModuleStagedBody`] in its invocation response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
// This is a serialized ABI envelope, not a long-lived in-memory sum type.
// Boxing the descriptor variant would add an allocation to every capability
// probe without changing the stable SOBJ representation.
#[allow(clippy::large_enum_variant)]
pub enum NativeModuleResponse {
    Capabilities(NativeModuleCapabilitiesResponse),
    Invocation(NativeModuleInvocationResponse),
    Error(NativeModuleErrorResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleCapabilitiesResponse {
    pub descriptor: NativeModuleDescriptor,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeModuleInvocationResponse {
    pub operation: String,
    pub payload_sobj: Vec<u8>,
    /// Description of a response attachment written to the exact
    /// host-assigned response staging path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body: Option<NativeModuleStagedBody>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleStagedBody {
    pub path: String,
    pub length: u64,
    /// BLAKE3 digest formatted as `blake3:<lowercase hex>`.
    pub blake3: String,
}

impl NativeModuleStagedBody {
    pub fn new(path: impl Into<String>, length: u64, blake3: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            length,
            blake3: blake3.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeModuleErrorResponse {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum NativeModuleApiError {
    #[error("SOBJ error: {0}")]
    Object(#[from] sinomo_object::Error),
    #[error("invalid native module ABI version {actual}; expected {expected}")]
    InvalidAbiVersion { expected: u32, actual: u32 },
    #[error("{0}")]
    Runtime(String),
}

pub fn encode_sobj<T: Serialize>(value: &T) -> Result<Vec<u8>, NativeModuleApiError> {
    Ok(sinomo_object_serde::to_bytes(
        value,
        SINOMO_MODULE_OBJECT_VERSION,
        LengthMode::U32,
    )?)
}

pub fn decode_sobj<T>(bytes: &[u8]) -> Result<T, NativeModuleApiError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(sinomo_object_serde::from_bytes(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_module_lifecycle_rejects_shutdown_until_invocations_drain() {
        let lifecycle = NativeModuleLifecycle::new();
        assert!(lifecycle.initialize());
        let first = lifecycle.begin_invocation().expect("first invocation");
        assert_eq!(lifecycle.active_invocations(), 1);
        assert!(!lifecycle.begin_shutdown());
        let second = lifecycle
            .begin_invocation()
            .expect("failed shutdown restores admission");
        drop(first);
        drop(second);
        assert!(lifecycle.begin_shutdown());
        assert!(lifecycle.begin_invocation().is_none());
        assert!(lifecycle.begin_shutdown(), "shutdown is idempotent");
        assert!(lifecycle.initialize(), "a later load can initialize again");
    }

    #[derive(Serialize)]
    struct LegacyNativeModuleObjectRpcRoute {
        route: String,
        category: String,
        operation: String,
        auth: String,
    }

    #[derive(Clone, Serialize)]
    struct LegacyNativeModuleSurfaces {
        object_rpc_routes: Vec<NativeModuleObjectRpcRoute>,
        declarative_resources: Vec<NativeModuleDeclarativeResource>,
        storage_owners: Vec<String>,
        capability_imports: Vec<String>,
        capability_exports: Vec<String>,
        settings: Vec<ModuleSettingsExport>,
        network_transports: Vec<String>,
    }

    #[derive(Serialize)]
    struct LegacyNativeModuleDescriptor {
        module_id: String,
        module_version: String,
        owner: String,
        native_abi_version: u32,
        backend: NativeModuleBackend,
        interface_version: String,
        platform_triple: String,
        supported_operations: Vec<String>,
        capabilities: Vec<String>,
        surfaces: LegacyNativeModuleSurfaces,
    }

    fn test_interface_export() -> ModuleInterfaceExport {
        ModuleInterfaceExport::new(
            "memory.content-extractor.markdown",
            "sinomo.memory.content-extractor/v1",
            "1.0.0",
            vec![0x53, 0x4f, 0x42, 0x4a],
        )
        .with_operation(ModuleInterfaceOperationExport {
            logical_operation: "extract".to_string(),
            target: ModuleInterfaceOperationTarget::NativeInvoke {
                operation: "memory.extract.markdown".to_string(),
            },
            request_schema_id: "sinomo.memory.extraction-request/v1".to_string(),
            response_schema_id: "sinomo.memory.derived-fact-batch/v1".to_string(),
            max_request_bytes: u64::MAX,
            max_response_bytes: u64::MAX,
            max_facts_per_batch: 1_024,
            max_deadline_ms: 30_000,
            supports_cancellation: true,
            continuation_schema_id: Some("sinomo.memory.extraction-continuation/v1".to_string()),
        })
    }

    #[test]
    fn typed_interface_export_roundtrips_and_validates() {
        let export = test_interface_export();
        export.validate().expect("valid interface export");

        let encoded = encode_sobj(&export).expect("encode interface export");
        let decoded: ModuleInterfaceExport =
            decode_sobj(&encoded).expect("decode interface export");

        assert_eq!(decoded, export);
    }

    #[test]
    fn typed_interface_export_requires_explicit_resource_contracts() {
        let mut export = test_interface_export();
        export.operations[0].max_deadline_ms = 0;

        let error = export.validate().expect_err("zero deadline must fail");
        assert!(error.to_string().contains("max_deadline_ms"));

        let duplicates = vec![test_interface_export(), test_interface_export()];
        assert!(matches!(
            validate_module_interface_exports(&duplicates),
            Err(ModuleInterfaceContractError::DuplicateExportId { .. })
        ));
    }

    #[test]
    fn legacy_native_descriptor_defaults_interface_exports() {
        let legacy = LegacyNativeModuleDescriptor {
            module_id: "module.legacy".to_string(),
            module_version: "0.1.0".to_string(),
            owner: "legacy".to_string(),
            native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
            backend: NativeModuleBackend::NativeDylib,
            interface_version: "sinomo.legacy.v1".to_string(),
            platform_triple: "test-platform".to_string(),
            supported_operations: Vec::new(),
            capabilities: Vec::new(),
            surfaces: LegacyNativeModuleSurfaces {
                object_rpc_routes: Vec::new(),
                declarative_resources: Vec::new(),
                storage_owners: vec!["legacy".to_string()],
                capability_imports: Vec::new(),
                capability_exports: Vec::new(),
                settings: Vec::new(),
                network_transports: Vec::new(),
            },
        };

        let encoded = encode_sobj(&legacy).expect("encode legacy descriptor");
        let decoded: NativeModuleDescriptor =
            decode_sobj(&encoded).expect("decode current descriptor");

        assert_eq!(decoded.module_id, "module.legacy");
        assert_eq!(decoded.surfaces.storage_owners, vec!["legacy"]);
        assert!(decoded.surfaces.agent_definitions.is_empty());
        assert!(decoded.surfaces.interface_exports.is_empty());
        assert!(decoded.surfaces.ui_contributions.is_empty());
        assert!(decoded.surfaces.extension_points.is_empty());
        assert!(decoded.surfaces.extension_contributions.is_empty());
    }

    #[test]
    fn native_module_route_local_permissions_roundtrip_through_sobj() {
        let route = NativeModuleObjectRpcRoute::new(
            "/remote/shell",
            "remote.management",
            "remote.management.shell.run",
            "admin",
        )
        .require_local_permissions(["remote-management", "device:display"]);

        let encoded = encode_sobj(&route).expect("encode route");
        let decoded: NativeModuleObjectRpcRoute = decode_sobj(&encoded).expect("decode route");

        assert_eq!(decoded, route);
    }

    #[test]
    fn native_module_route_v1_without_body_modes_is_rejected() {
        let legacy = LegacyNativeModuleObjectRpcRoute {
            route: "/legacy/inspect".to_string(),
            category: "legacy".to_string(),
            operation: "legacy.inspect".to_string(),
            auth: "admin".to_string(),
        };

        let encoded = encode_sobj(&legacy).expect("encode legacy route");
        let error = decode_sobj::<NativeModuleObjectRpcRoute>(&encoded)
            .expect_err("ABI v1 route must not decode as ABI v2");
        assert!(error.to_string().contains("request_body_mode"));
    }

    #[test]
    fn native_module_session_staging_roundtrips_through_sobj() {
        let staged = NativeModuleStagedBody::new(
            "/private/session/request.body",
            u64::MAX,
            "blake3:0123456789abcdef",
        );
        let request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
            operation: "files.write".to_string(),
            payload_sobj: vec![1, 2, 3],
            host_data_dir_root: Some("/private/sinomo-data".to_string()),
            cancellation_wait_handle: Some(u64::MAX),
            request_body: Some(staged.clone()),
            response_body_path: Some("/private/session/response.body".to_string()),
        });

        let encoded = encode_sobj(&request).expect("encode request");
        let decoded: NativeModuleRequest = decode_sobj(&encoded).expect("decode request");

        assert_eq!(decoded, request);
        assert_eq!(staged.length, u64::MAX);
    }
}
