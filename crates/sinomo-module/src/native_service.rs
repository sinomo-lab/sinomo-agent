//! Service materialization for active native-dylib component routes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sinomo_core::network::{
    NetworkError, ObjectRpcBodySource, ObjectRpcCancelRequest, ObjectRpcIncomingRequest,
    ObjectRpcOutgoingResponse, ObjectRpcRequest, ObjectRpcResponse,
};
use sinomo_core::service::{ServiceBodyMode, ServiceDisposition, ServiceRegistry, ServiceRoute};

use crate::{
    MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY, ModuleServiceSnapshot,
    NativeDylibModuleRouteTarget, NativeModule, NativeModuleHostCallbacks,
    NativeModuleOperationCancellationRegistry, NativeModuleOperationCancellationToken,
    NativeModuleSessionStaging, native_module_cache_path, native_module_staged_body_path,
    parse_native_dylib_module_id, read_native_dylib_module_loader_state,
    stage_native_module_request_body, validate_module_service_route_contract,
    validate_native_module_request_body_mode, validate_native_module_response_body,
    validate_native_module_staged_request_body,
};

pub trait NativeModuleRequestPreparer: Send + Sync {
    fn id(&self) -> &'static str;

    fn prepare(
        &self,
        data_dir_root: &Path,
        target: &NativeDylibModuleRouteTarget,
        request: &mut ObjectRpcRequest,
    ) -> Result<(), NetworkError>;
}

#[derive(Clone, Debug, Default)]
pub struct PassthroughNativeModuleRequestPreparer;

impl NativeModuleRequestPreparer for PassthroughNativeModuleRequestPreparer {
    fn id(&self) -> &'static str {
        "passthrough"
    }

    fn prepare(
        &self,
        _data_dir_root: &Path,
        _target: &NativeDylibModuleRouteTarget,
        _request: &mut ObjectRpcRequest,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct NativeModuleServiceMaterialization {
    pub host_observability_namespace: String,
    pub native_route_concurrency: usize,
    pub native_route_timeout: Duration,
}

impl NativeModuleServiceMaterialization {
    pub fn new(host_observability_namespace: impl Into<String>) -> Self {
        Self {
            host_observability_namespace: host_observability_namespace.into(),
            native_route_concurrency: 4,
            native_route_timeout: Duration::from_secs(310),
        }
    }
}

#[derive(Clone)]
pub struct MaterializedModuleServiceSnapshot {
    pub registry: ServiceRegistry,
    pub extension_registry: Arc<crate::ExtensionRegistrySnapshot>,
    pub runtime: crate::ModuleRuntimeHost,
}

pub fn materialize_module_service_snapshot(
    registry: ServiceRegistry,
    module_snapshot: ModuleServiceSnapshot,
    data_dir_root: PathBuf,
    request_preparer: Arc<dyn NativeModuleRequestPreparer>,
    options: NativeModuleServiceMaterialization,
) -> Result<ServiceRegistry, NetworkError> {
    materialize_module_service_snapshot_with_extensions(
        registry,
        module_snapshot,
        data_dir_root,
        request_preparer,
        options,
    )
    .map(|materialized| materialized.registry)
}

pub fn materialize_module_service_snapshot_with_extensions(
    registry: ServiceRegistry,
    module_snapshot: ModuleServiceSnapshot,
    data_dir_root: PathBuf,
    request_preparer: Arc<dyn NativeModuleRequestPreparer>,
    options: NativeModuleServiceMaterialization,
) -> Result<MaterializedModuleServiceSnapshot, NetworkError> {
    let extension_registry = Arc::new(
        crate::compile_default_extension_registry(module_snapshot.snapshot.as_ref()).map_err(
            |error| NetworkError::Serialization {
                detail: format!("compile module extension registry: {error}"),
            },
        )?,
    );
    let runtime = crate::ModuleRuntimeHost::new(
        Arc::clone(&module_snapshot.snapshot),
        Arc::clone(&extension_registry),
        0,
    )
    .map_err(module_error)?;
    materialize_module_service_snapshot_with_runtime(
        registry,
        module_snapshot,
        data_dir_root,
        request_preparer,
        options,
        runtime,
        extension_registry,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_module_service_snapshot_for_runtime(
    registry: ServiceRegistry,
    module_snapshot: ModuleServiceSnapshot,
    data_dir_root: PathBuf,
    request_preparer: Arc<dyn NativeModuleRequestPreparer>,
    options: NativeModuleServiceMaterialization,
    runtime: crate::ModuleRuntimeHost,
) -> Result<MaterializedModuleServiceSnapshot, NetworkError> {
    let extension_registry = Arc::new(
        crate::compile_default_extension_registry(module_snapshot.snapshot.as_ref()).map_err(
            |error| NetworkError::Serialization {
                detail: format!("compile module extension registry: {error}"),
            },
        )?,
    );
    materialize_module_service_snapshot_with_runtime(
        registry,
        module_snapshot,
        data_dir_root,
        request_preparer,
        options,
        runtime,
        extension_registry,
    )
}

#[allow(clippy::too_many_arguments)]
fn materialize_module_service_snapshot_with_runtime(
    registry: ServiceRegistry,
    module_snapshot: ModuleServiceSnapshot,
    data_dir_root: PathBuf,
    request_preparer: Arc<dyn NativeModuleRequestPreparer>,
    options: NativeModuleServiceMaterialization,
    runtime: crate::ModuleRuntimeHost,
    extension_registry: Arc<crate::ExtensionRegistrySnapshot>,
) -> Result<MaterializedModuleServiceSnapshot, NetworkError> {
    let invocation_gate: Arc<dyn sinomo_core::service::ServiceInvocationGate> =
        Arc::new(runtime.invocations.clone());
    let mut builder = registry.to_builder().with_invocation_gate(invocation_gate);
    builder = crate::register_extension_catalog_service(builder, runtime.generations.clone());
    builder = crate::register_ui_extension_catalog_service(
        builder,
        runtime.generations.clone(),
        data_dir_root.clone(),
    );
    let active_operations = runtime.native_operations.clone();
    let module_snapshot_version = module_snapshot.snapshot.version().to_string();
    let service_registry_key = format!("{}.service_registry", options.host_observability_namespace);
    let native_route_key = format!(
        "{}.native_dylib_module_route",
        options.host_observability_namespace
    );
    let module_route_key = format!("{}.module_route", options.host_observability_namespace);
    let cancel_route_key = format!(
        "{}.native_dylib_module_cancel_route",
        options.host_observability_namespace
    );

    for route in registry.route_categories() {
        builder = builder.replace_route_metadata(
            route
                .observable(&service_registry_key, "true")
                .observable(format!("{service_registry_key}.snapshot_version"), "1"),
        );
    }

    for mut descriptor in module_snapshot.routes {
        descriptor = descriptor
            .observable(&service_registry_key, "true")
            .observable(&module_route_key, "true");
        let native_target = module_snapshot
            .native_dylib_routes
            .get(&descriptor.route)
            .cloned();
        if let Some(native_target) = native_target {
            if registry.owns(&descriptor.route) {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "native dylib module route {} collides with a host service route",
                        descriptor.route
                    ),
                });
            }
            descriptor.concurrency_limit = Some(options.native_route_concurrency);
            descriptor.timeout_ms = Some(
                options
                    .native_route_timeout
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            );
            descriptor = descriptor
                .with_buffered_request_value()
                .observable(&native_route_key, "true")
                .observable(
                    "module.registry.snapshot_version",
                    module_snapshot_version.clone(),
                );
            let route_data_dir = data_dir_root.clone();
            let route_preparer = Arc::clone(&request_preparer);
            let route_operations = active_operations.clone();
            builder = builder.register_dynamic(
                "native-dylib",
                descriptor,
                move |mut context, request| {
                    let data_dir_root = route_data_dir.clone();
                    let target = native_target.clone();
                    let request_preparer = Arc::clone(&route_preparer);
                    let active_operations = route_operations.clone();
                    async move {
                        let request =
                            ObjectRpcIncomingRequest::new(request, context.take_request_body());
                        let response = dispatch_native_module_service_request(
                            data_dir_root,
                            target,
                            active_operations,
                            request_preparer,
                            request,
                        )
                        .await?;
                        context.respond_with_body(response.response, response.body)?;
                        Ok(ServiceDisposition::Continue)
                    }
                },
            );
        } else if let Some(mut service_route) = registry.route(&descriptor.route) {
            validate_module_service_route_contract(&descriptor, &service_route)
                .map_err(module_error)?;
            service_route.observability.extend(descriptor.observability);
            service_route.discovery = descriptor.discovery.clone();
            service_route = service_route
                .observable(&service_registry_key, "true")
                .observable(format!("{service_registry_key}.snapshot_version"), "1")
                .observable(
                    "module.registry.snapshot_version",
                    module_snapshot_version.clone(),
                );
            builder = builder.replace_route_metadata(service_route);
        }
    }

    let cancel_operations = active_operations.clone();
    builder = builder.register_dynamic(
        "native-dylib",
        native_module_cancel_route_descriptor(cancel_route_key)
            .observable("module.registry.snapshot_version", module_snapshot_version),
        move |mut context, request| {
            let active_operations = cancel_operations.clone();
            async move {
                let metadata_operation_id = request.cancels_operation_id().map(str::to_string);
                let payload: ObjectRpcCancelRequest = context.decode_request_body(&request).await?;
                let response = active_operations.cancel_request(metadata_operation_id, payload)?;
                context.respond_value(MODULE_NATIVE_DYLIB_CANCEL_ROUTE, response)?;
                Ok(ServiceDisposition::Continue)
            }
        },
    );
    Ok(MaterializedModuleServiceSnapshot {
        registry: builder.build(),
        extension_registry,
        runtime,
    })
}

async fn dispatch_native_module_service_request(
    data_dir_root: PathBuf,
    target: NativeDylibModuleRouteTarget,
    active_operations: NativeModuleOperationCancellationRegistry,
    request_preparer: Arc<dyn NativeModuleRequestPreparer>,
    request: ObjectRpcIncomingRequest,
) -> Result<ObjectRpcOutgoingResponse, NetworkError> {
    let route_for_error = request.request.route.clone();
    let operation_id = request.request.operation_id().map(str::to_string);
    let has_request_body = !request.body.is_empty();
    validate_native_module_request_body_mode(
        &route_for_error,
        target.request_body_mode,
        has_request_body,
    )?;
    let cancellation_registration =
        active_operations.register(target.module_id.to_string(), operation_id)?;
    let cancellation_token = cancellation_registration.token();
    let staging = (target.request_body_mode != ServiceBodyMode::None
        || target.response_body_mode != ServiceBodyMode::None)
        .then(|| {
            NativeModuleSessionStaging::create(
                &data_dir_root,
                target.request_body_mode,
                target.response_body_mode,
            )
        })
        .transpose()?;
    let staged_request_body = if has_request_body {
        let request_path = staging
            .as_ref()
            .and_then(NativeModuleSessionStaging::request_body_path)
            .ok_or_else(|| NetworkError::Serialization {
                detail: format!("native dylib route {route_for_error} has no request staging path"),
            })?;
        Some(
            stage_native_module_request_body(request.body, request_path, &cancellation_token)
                .await?,
        )
    } else {
        None
    };
    validate_native_module_staged_request_body(
        &route_for_error,
        target.request_body_mode,
        staged_request_body.as_ref(),
    )?;
    let response_body_path = staging
        .as_ref()
        .and_then(NativeModuleSessionStaging::response_body_path)
        .map(native_module_staged_body_path)
        .transpose()?;
    let mut request = request.request;
    request_preparer.prepare(&data_dir_root, &target, &mut request)?;
    let (response, response_body_path, staging) = tokio::task::spawn_blocking(move || {
        invoke_native_module_service_request(
            data_dir_root,
            target,
            cancellation_token,
            request,
            staged_request_body,
            response_body_path,
            staging,
        )
    })
    .await
    .map_err(|source| NetworkError::Io {
        operation: format!("dispatch native dylib module route {route_for_error}"),
        detail: format!("native dylib module route task failed: {source}"),
    })??;
    let body = if let Some(path) = response_body_path {
        let staging = staging.ok_or_else(|| NetworkError::Serialization {
            detail: "native dylib response body lost its staging lifetime".to_string(),
        })?;
        ObjectRpcBodySource::file(path)?.with_lifetime_guard(staging)?
    } else {
        drop(staging);
        ObjectRpcBodySource::empty()
    };
    Ok(ObjectRpcOutgoingResponse::new(response, body))
}

#[allow(clippy::too_many_arguments)]
fn invoke_native_module_service_request(
    data_dir_root: PathBuf,
    target: NativeDylibModuleRouteTarget,
    cancellation_token: NativeModuleOperationCancellationToken,
    request: ObjectRpcRequest,
    staged_request_body: Option<sinomo_module_api::NativeModuleStagedBody>,
    response_body_path: Option<String>,
    staging: Option<NativeModuleSessionStaging>,
) -> Result<
    (
        ObjectRpcResponse,
        Option<PathBuf>,
        Option<NativeModuleSessionStaging>,
    ),
    NetworkError,
> {
    let state = read_native_dylib_module_loader_state(&data_dir_root, &target.module_id)
        .map_err(module_error)?
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!(
                "native dylib module {} is not active for route {}",
                target.module_id, request.route
            ),
        })?;
    let library_path = native_module_cache_path(
        &data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    let module = unsafe {
        NativeModule::load_from_content_addressed_path(&library_path, &state.content_hash)
    }
    .map_err(module_error)?;
    let descriptor = module.descriptor();
    let descriptor_module_id =
        parse_native_dylib_module_id(&descriptor.module_id).map_err(module_error)?;
    if descriptor_module_id != target.module_id {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib route {} resolved module {}, but descriptor reports {}",
                request.route, target.module_id, descriptor.module_id
            ),
        });
    }
    if !descriptor
        .supported_operations
        .iter()
        .any(|operation| operation == &target.operation)
    {
        return Err(NetworkError::Serialization {
            detail: format!(
                "native dylib module {} does not support operation {}",
                target.module_id, target.operation
            ),
        });
    }
    let callbacks: Arc<dyn NativeModuleHostCallbacks> = Arc::new(
        DefaultNativeModuleHostCallbacks::new(cancellation_token, data_dir_root),
    );
    let native_response = module
        .invoke_with_callbacks(
            &sinomo_module_api::NativeModuleRequest::Invoke(
                sinomo_module_api::NativeModuleInvocationRequest {
                    operation: target.operation.clone(),
                    payload_sobj: request.body,
                    cancellation_wait_handle: callbacks.cancellation_wait_handle(),
                    request_body: staged_request_body,
                    response_body_path,
                },
            ),
            Some(callbacks),
        )
        .map_err(module_error)?;
    match native_response {
        sinomo_module_api::NativeModuleResponse::Invocation(response) => {
            if response.operation != target.operation {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "native dylib route {} expected operation {}, got {}",
                        request.route, target.operation, response.operation
                    ),
                });
            }
            let response_body_path = validate_native_module_response_body(
                target.response_body_mode,
                staging
                    .as_ref()
                    .and_then(NativeModuleSessionStaging::response_body_path),
                response.response_body,
            )?;
            Ok((
                ObjectRpcResponse::from_encoded_body(request.route, response.payload_sobj),
                response_body_path,
                staging,
            ))
        }
        sinomo_module_api::NativeModuleResponse::Error(error) => Err(NetworkError::Serialization {
            detail: format!(
                "native dylib module {} returned error {}: {}",
                target.module_id, error.code, error.detail
            ),
        }),
        sinomo_module_api::NativeModuleResponse::Capabilities(_) => {
            Err(NetworkError::Serialization {
                detail: format!(
                    "native dylib module {} returned capabilities for route invocation",
                    target.module_id
                ),
            })
        }
    }
}

pub struct DefaultNativeModuleHostCallbacks {
    cancellation_token: NativeModuleOperationCancellationToken,
    data_dir_root: PathBuf,
}

impl DefaultNativeModuleHostCallbacks {
    pub fn new(
        cancellation_token: NativeModuleOperationCancellationToken,
        data_dir_root: PathBuf,
    ) -> Self {
        Self {
            cancellation_token,
            data_dir_root,
        }
    }
}

impl NativeModuleHostCallbacks for DefaultNativeModuleHostCallbacks {
    fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    fn local_permission_granted(&self, permission: &str) -> bool {
        let Ok(permission) = sinomo_core::node::LocalNodePermission::try_from(permission) else {
            return false;
        };
        sinomo_core::node::LocalNodePermissionStore::new(&self.data_dir_root)
            .permission_granted(permission)
    }
}

fn native_module_cancel_route_descriptor(observability_key: String) -> ServiceRoute {
    ServiceRoute::describe(
        MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
        MODULE_RPC_CATEGORY,
        "cancel an active native-dylib module object-RPC operation by id",
        "sinomo_core::network::ObjectRpcCancelRequest",
        "sinomo_core::network::ObjectRpcCancelResponse",
    )
    .with_auth_scope("local-or-cluster-member")
    .require_capability("module.native-dylib.cancel")
    .observable(observability_key, "true")
}

fn module_error(error: impl std::fmt::Display) -> NetworkError {
    NetworkError::Serialization {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialization_publishes_one_extension_generation_to_both_catalogs() {
        let module_snapshots = crate::ModuleRegistrySnapshotStore::default();
        let module_snapshot =
            crate::module_service_snapshot(&module_snapshots).expect("module service snapshot");
        let expected_generation = module_snapshot.snapshot.version();
        let root = sinomo_testdir::TestDir::new("module-extension-materialization");
        let materialized = materialize_module_service_snapshot_with_extensions(
            ServiceRegistry::default(),
            module_snapshot,
            root.path().to_path_buf(),
            Arc::new(PassthroughNativeModuleRequestPreparer),
            NativeModuleServiceMaterialization::new("fixture"),
        )
        .expect("materialized services");

        assert!(
            materialized
                .registry
                .owns(crate::MODULE_EXTENSION_CATALOG_ROUTE)
        );
        assert!(materialized.registry.owns(crate::MODULE_UI_CATALOG_ROUTE));
        assert_eq!(
            materialized
                .extension_registry
                .generation
                .module_registry_generation,
            expected_generation
        );
    }
}
