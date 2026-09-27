//! Capability-checked, registry-backed calls between components.

use std::sync::{Arc, Mutex};

use serde::{Serialize, de::DeserializeOwned};
use sinomo_core::network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse, ObjectRpcRequest};
use sinomo_core::service::{
    ResponseSink, ServiceContext, ServiceDiscoveryHealth, ServiceDiscoveryIdentity,
    ServiceDiscoveryLocality, ServiceDisposition, ServiceError, ServiceRegistrySnapshotStore,
    ServiceSigner,
};
use thiserror::Error;

use crate::{ComponentDescriptor, ServiceRequirement, ServiceRequirementLocality};

pub const COMPONENT_CALLER_ID_METADATA_KEY: &str = "sinomo.component.caller.id";
pub const COMPONENT_CALLER_VERSION_METADATA_KEY: &str = "sinomo.component.caller.version";
pub const COMPONENT_CALL_INTERFACE_METADATA_KEY: &str = "sinomo.component.call.interface";
pub const COMPONENT_CALL_OPERATION_METADATA_KEY: &str = "sinomo.component.call.operation";

/// One successful response together with host-trusted provider provenance.
#[derive(Clone, Debug)]
pub struct ComponentServiceResponse<T> {
    pub provider: ServiceDiscoveryIdentity,
    pub duration_us: u64,
    pub value: T,
}

#[derive(Clone, Debug, Error)]
#[error("component service provider `{}` route `{}` failed: {detail}", provider.component_id, route)]
pub struct ComponentServiceCallError {
    pub provider: ServiceDiscoveryIdentity,
    pub route: String,
    pub duration_us: u64,
    pub detail: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ComponentServiceError {
    #[error(
        "component `{component_id}` did not declare service requirement `{interface_id}` {interface_version}"
    )]
    UndeclaredRequirement {
        component_id: String,
        interface_id: String,
        interface_version: String,
    },
    #[error(
        "component `{component_id}` did not declare operation `{operation}` for `{interface_id}`"
    )]
    UndeclaredOperation {
        component_id: String,
        interface_id: String,
        operation: String,
    },
    #[error("encode component service request: {detail}")]
    Encode { detail: String },
}

/// A component's bounded view of the live local service registry.
///
/// The client resolves interfaces, not concrete crates or providers. Every call
/// is checked against the caller's descriptor and the provider's discovery
/// identity before the ordinary service handler is entered.
#[derive(Clone)]
pub struct ComponentServiceClient {
    caller: ComponentDescriptor,
    data_dir: Arc<sinomo_core::local::SinomoDataDir>,
    registry: ServiceRegistrySnapshotStore,
}

impl std::fmt::Debug for ComponentServiceClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ComponentServiceClient")
            .field("caller", &self.caller.component_id)
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl ComponentServiceClient {
    pub(crate) fn new(
        caller: ComponentDescriptor,
        data_dir: Arc<sinomo_core::local::SinomoDataDir>,
        registry: ServiceRegistrySnapshotStore,
    ) -> Self {
        Self {
            caller,
            data_dir,
            registry,
        }
    }

    /// Invoke exactly one selected provider. Resolution is checked before any effect;
    /// it never fans a selected command out to other implementations of the interface.
    #[allow(clippy::too_many_arguments)]
    pub async fn call_local_on_provider<Request, Response>(
        &self,
        provider_id: &str,
        requirement: &ServiceRequirement,
        operation: &str,
        operation_id: &str,
        expected_native_hash: Option<&str>,
        request: &Request,
        deadline: std::time::Duration,
        max_response_bytes: usize,
    ) -> Result<Response, String>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned + Send + 'static,
    {
        self.authorize_requirement(requirement, operation)
            .map_err(|error| error.to_string())?;
        let routes = self
            .resolve_local_routes(requirement, operation)
            .into_iter()
            .filter(|(_, identity)| identity.component_id == provider_id)
            .collect::<Vec<_>>();
        if routes.len() != 1 {
            return Err("selected component CLI provider is unavailable or ambiguous".into());
        }
        let (route, _) = &routes[0];
        let permissions = sinomo_core::node::LocalNodePermissionStore::new(self.data_dir.root());
        for name in &route.required_local_permissions {
            let permission = sinomo_core::node::LocalNodePermission::try_from(name.as_str())
                .map_err(|error| error.to_string())?;
            if !permissions
                .try_permission_granted(permission)
                .map_err(|error| error.to_string())?
            {
                return Err(format!("node-local permission `{name}` is required"));
            }
        }
        let mut request = ObjectRpcRequest::encode_body(&route.route, &route.category, request)
            .map_err(|error| error.to_string())?
            .with_operation_id(operation_id)
            .with_metadata(
                COMPONENT_CALLER_ID_METADATA_KEY,
                self.caller.component_id.clone(),
            )
            .with_metadata(
                COMPONENT_CALLER_VERSION_METADATA_KEY,
                self.caller.component_version.clone(),
            )
            .with_metadata(
                COMPONENT_CALL_INTERFACE_METADATA_KEY,
                requirement.interface_id.clone(),
            )
            .with_metadata(COMPONENT_CALL_OPERATION_METADATA_KEY, operation);
        if let Some(hash) = expected_native_hash {
            request = request.with_metadata("module.cli.expected-content-hash", hash);
        }
        // Await directly: cancellation drops the dispatch future and its native-operation guard.
        tokio::time::timeout(
            deadline,
            dispatch_component_call::<Response>(
                self.registry.active_snapshot().registry_arc(),
                Arc::clone(&self.data_dir),
                self.caller.component_id.clone(),
                request,
                max_response_bytes,
            ),
        )
        .await
        .map_err(|_| "component CLI deadline elapsed".to_string())?
    }

    /// Resolve and invoke every local provider of an interface operation.
    /// Provider failures remain independent so fan-out consumers can return a
    /// useful partial response.
    pub async fn call_local_all<Request, Response>(
        &self,
        requirement: &ServiceRequirement,
        operation: &str,
        request: &Request,
        deadline: std::time::Duration,
        max_response_bytes: usize,
    ) -> Result<
        Vec<Result<ComponentServiceResponse<Response>, ComponentServiceCallError>>,
        ComponentServiceError,
    >
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned + Send + 'static,
    {
        self.authorize_requirement(requirement, operation)?;
        let routes = self.resolve_local_routes(requirement, operation);
        let registry = self.registry.active_snapshot().registry_arc();
        let mut pending = Vec::with_capacity(routes.len());
        for (route, provider) in routes {
            let started = std::time::Instant::now();
            let request = ObjectRpcRequest::encode_body(&route.route, &route.category, request)
                .map_err(|error| ComponentServiceError::Encode {
                    detail: error.to_string(),
                })?
                .with_metadata(
                    COMPONENT_CALLER_ID_METADATA_KEY,
                    self.caller.component_id.clone(),
                )
                .with_metadata(
                    COMPONENT_CALLER_VERSION_METADATA_KEY,
                    self.caller.component_version.clone(),
                )
                .with_metadata(
                    COMPONENT_CALL_INTERFACE_METADATA_KEY,
                    requirement.interface_id.clone(),
                )
                .with_metadata(COMPONENT_CALL_OPERATION_METADATA_KEY, operation);
            let registry = Arc::clone(&registry);
            let data_dir = Arc::clone(&self.data_dir);
            let caller_id = self.caller.component_id.clone();
            let task = tokio::spawn(async move {
                let result = tokio::time::timeout(deadline, async move {
                    dispatch_component_call::<Response>(
                        registry,
                        data_dir,
                        caller_id,
                        request,
                        max_response_bytes,
                    )
                    .await
                })
                .await
                .map_err(|_| format!("component service call timed out after {deadline:?}"))
                .and_then(|result| result);
                (duration_us(started.elapsed()), result)
            });
            pending.push((route, provider, started, task));
        }

        let mut calls = Vec::with_capacity(pending.len());
        for (route, provider, started, task) in pending {
            let (call_duration_us, result) = match task.await {
                Ok(result) => result,
                Err(error) => (
                    duration_us(started.elapsed()),
                    Err(format!("component service task failed: {error}")),
                ),
            };
            calls.push(match result {
                Ok(value) => Ok(ComponentServiceResponse {
                    provider: provider.clone(),
                    duration_us: call_duration_us,
                    value,
                }),
                Err(detail) => Err(ComponentServiceCallError {
                    provider: provider.clone(),
                    route: route.route.clone(),
                    duration_us: call_duration_us,
                    detail,
                }),
            });
        }
        Ok(calls)
    }

    fn authorize_requirement(
        &self,
        requirement: &ServiceRequirement,
        operation: &str,
    ) -> Result<(), ComponentServiceError> {
        let declared = self
            .caller
            .requirements
            .iter()
            .find(|declared| *declared == requirement)
            .ok_or_else(|| ComponentServiceError::UndeclaredRequirement {
                component_id: self.caller.component_id.clone(),
                interface_id: requirement.interface_id.clone(),
                interface_version: requirement.interface_version.clone(),
            })?;
        if !declared
            .operations
            .iter()
            .any(|declared| declared == operation)
        {
            return Err(ComponentServiceError::UndeclaredOperation {
                component_id: self.caller.component_id.clone(),
                interface_id: requirement.interface_id.clone(),
                operation: operation.to_string(),
            });
        }
        Ok(())
    }

    fn resolve_local_routes(
        &self,
        requirement: &ServiceRequirement,
        operation: &str,
    ) -> Vec<(sinomo_core::service::ServiceRoute, ServiceDiscoveryIdentity)> {
        let snapshot = self.registry.active_snapshot();
        let mut routes = snapshot
            .registry()
            .route_categories()
            .into_iter()
            .filter_map(|route| {
                let identity = route.discovery.clone()?;
                let locality_matches = match requirement.locality {
                    ServiceRequirementLocality::Local | ServiceRequirementLocality::Any => true,
                    ServiceRequirementLocality::Cluster => {
                        identity.locality == ServiceDiscoveryLocality::Cluster
                    }
                };
                let version_matches = requirement.interface_version == "*"
                    || requirement.interface_version == identity.interface_version;
                let service_matches = requirement
                    .service_id
                    .as_ref()
                    .is_none_or(|service_id| service_id == &identity.service_id);
                let caller_authorized = identity
                    .caller_requirements
                    .iter()
                    .chain(route.required_capabilities.iter())
                    .all(|capability| requirement.caller_capabilities.contains(capability));
                (identity.interface_id == requirement.interface_id
                    && version_matches
                    && service_matches
                    && locality_matches
                    && identity.health == ServiceDiscoveryHealth::Ready
                    && identity
                        .provider_capabilities
                        .iter()
                        .any(|capability| capability == operation)
                    && route
                        .required_capabilities
                        .iter()
                        .any(|capability| capability == operation)
                    && caller_authorized)
                    .then_some((route, identity))
            })
            .collect::<Vec<_>>();
        routes.sort_by(|(left_route, left), (right_route, right)| {
            left.component_id
                .cmp(&right.component_id)
                .then_with(|| left.service_id.cmp(&right.service_id))
                .then_with(|| left_route.route.cmp(&right_route.route))
        });
        routes
    }
}

async fn dispatch_component_call<Response>(
    registry: Arc<sinomo_core::service::ServiceRegistry>,
    data_dir: Arc<sinomo_core::local::SinomoDataDir>,
    caller_id: String,
    request: ObjectRpcRequest,
    max_response_bytes: usize,
) -> Result<Response, String>
where
    Response: DeserializeOwned + Send + 'static,
{
    let response = Arc::new(Mutex::new(None));
    let context = ServiceContext::new(
        data_dir,
        None,
        ServiceSigner::Local,
        ObjectRpcIncomingBody::empty(),
        Box::new(ComponentResponseSink {
            response: Arc::clone(&response),
        }),
    )
    .with_component_caller(caller_id);
    let outgoing = match registry.dispatch(context, request).await {
        Ok(ServiceDisposition::Continue) => take_response(response),
        Ok(ServiceDisposition::Stop) => Err(
            "component service requested host shutdown, which component calls cannot propagate"
                .to_string(),
        ),
        Err(error) => Err(error.to_string()),
    }?;
    async_decode_response(outgoing, max_response_bytes).await
}

fn duration_us(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn take_response(
    response: Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>,
) -> Result<ObjectRpcOutgoingResponse, String> {
    response
        .lock()
        .map_err(|_| "component response sink lock poisoned".to_string())?
        .take()
        .ok_or_else(|| "component service returned without a response".to_string())
}

async fn async_decode_response<Response>(
    response: ObjectRpcOutgoingResponse,
    max_response_bytes: usize,
) -> Result<Response, String>
where
    Response: DeserializeOwned + Send + 'static,
{
    response
        .into_materialized_value_response(max_response_bytes)
        .await
        .map_err(|error| error.to_string())?
        .decode_body()
        .map_err(|error| error.to_string())
}

struct ComponentResponseSink {
    response: Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>,
}

impl ResponseSink for ComponentResponseSink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        let mut slot = self.response.lock().map_err(|_| {
            ServiceError::Service("component response sink lock poisoned".to_string())
        })?;
        if slot.is_some() {
            return Err(ServiceError::Service(
                "component service sent more than one response".to_string(),
            ));
        }
        *slot = Some(response);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::service::{
        ServiceDiscoveryHealth, ServiceDiscoveryLocality, ServiceRegistry, ServiceRoute,
    };

    const INTERFACE: &str = "sinomo.fixture.math";
    const OPERATION: &str = "fixture.math.double";

    fn selected_route(provider: &str, route: &str) -> ServiceRoute {
        ServiceRoute::new(route, "fixture.math")
            .with_auth_scope("component")
            .require_capability(OPERATION)
            .with_discovery_identity(ServiceDiscoveryIdentity {
                service_id: "fixture-math".into(),
                interface_id: INTERFACE.into(),
                interface_version: "1.0.0".into(),
                provider_capabilities: vec![OPERATION.into()],
                caller_requirements: vec![OPERATION.into()],
                component_id: provider.into(),
                component_version: "1.0.0".into(),
                artifact_id: None,
                component_generation: 1,
                health: ServiceDiscoveryHealth::Ready,
                locality: ServiceDiscoveryLocality::Local,
            })
    }

    fn selected_client(
        registry: ServiceRegistry,
    ) -> (
        ComponentServiceClient,
        ServiceRequirement,
        sinomo_testdir::TestDir,
    ) {
        let requirement = ServiceRequirement::new(INTERFACE, "1.0.0")
            .from_service("fixture-math")
            .with_operation(OPERATION)
            .with_caller_capability(OPERATION);
        let root = sinomo_testdir::TestDir::new("selected-component-call");
        let client = ComponentServiceClient::new(
            ComponentDescriptor::new("fixture.consumer", "1.0.0").require(requirement.clone()),
            Arc::new(sinomo_core::local::SinomoDataDir::new(root.path())),
            ServiceRegistrySnapshotStore::new(registry),
        );
        (client, requirement, root)
    }

    #[tokio::test]
    async fn selected_provider_does_not_fan_out_or_fall_back() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut builder = ServiceRegistry::builder();
        for provider in ["fixture.first", "fixture.second"] {
            let calls = Arc::clone(&calls);
            builder = builder.register_dynamic(
                "math",
                selected_route(provider, &format!("/{provider}")),
                move |mut context, request| {
                    let calls = Arc::clone(&calls);
                    async move {
                        assert_eq!(context.component_caller(), Some("fixture.consumer"));
                        calls.lock().unwrap().push(provider);
                        context.respond_value(request.route, 42_u64)?;
                        Ok(ServiceDisposition::Continue)
                    }
                },
            );
        }
        let (client, requirement, _root) = selected_client(builder.build());
        let result: u64 = client
            .call_local_on_provider(
                "fixture.second",
                &requirement,
                OPERATION,
                "selected",
                None,
                &21_u64,
                std::time::Duration::from_secs(1),
                1024,
            )
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(*calls.lock().unwrap(), ["fixture.second"]);
        assert!(
            client
                .call_local_on_provider::<_, u64>(
                    "fixture.missing",
                    &requirement,
                    OPERATION,
                    "missing",
                    None,
                    &21_u64,
                    std::time::Duration::from_secs(1),
                    1024
                )
                .await
                .is_err()
        );
        assert_eq!(*calls.lock().unwrap(), ["fixture.second"]);
    }

    #[tokio::test]
    async fn selected_provider_checks_local_consent_before_effects() {
        let registry = ServiceRegistry::builder()
            .register_dynamic(
                "denied",
                selected_route("fixture.denied", "/fixture/denied")
                    .require_local_permission("remote-management"),
                |_, _| async { panic!("denied provider must never execute") },
            )
            .build();
        let (client, requirement, _root) = selected_client(registry);
        let error = client
            .call_local_on_provider::<_, u64>(
                "fixture.denied",
                &requirement,
                OPERATION,
                "denied",
                None,
                &21_u64,
                std::time::Duration::from_secs(1),
                1024,
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("node-local permission `remote-management`"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn selected_provider_deadline_drops_the_running_dispatch() {
        struct DropSignal(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = Arc::clone(&dropped);
        let registry = ServiceRegistry::builder()
            .register_dynamic(
                "pending",
                selected_route("fixture.pending", "/fixture/pending"),
                move |_, _| {
                    let signal = Arc::clone(&signal);
                    async move {
                        let _drop_signal = DropSignal(signal);
                        std::future::pending::<Result<ServiceDisposition, ServiceError>>().await
                    }
                },
            )
            .build();
        let (client, requirement, _root) = selected_client(registry);
        let error = client
            .call_local_on_provider::<_, u64>(
                "fixture.pending",
                &requirement,
                OPERATION,
                "timeout",
                None,
                &21_u64,
                std::time::Duration::from_millis(20),
                1024,
            )
            .await
            .unwrap_err();
        assert!(error.contains("deadline"), "{error}");
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn calls_all_matching_providers_through_the_live_registry() {
        let route = ServiceRoute::new("/fixture/math/double", "fixture.math")
            .with_auth_scope("component")
            .require_capability(OPERATION)
            .with_discovery_identity(ServiceDiscoveryIdentity {
                service_id: "fixture-math".to_string(),
                interface_id: INTERFACE.to_string(),
                interface_version: "1.0.0".to_string(),
                provider_capabilities: vec![OPERATION.to_string()],
                caller_requirements: vec![OPERATION.to_string()],
                component_id: "fixture.math.provider".to_string(),
                component_version: "1.2.3".to_string(),
                artifact_id: Some("fixture-math-artifact".to_string()),
                component_generation: 4,
                health: ServiceDiscoveryHealth::Ready,
                locality: ServiceDiscoveryLocality::Local,
            });
        let registry = ServiceRegistry::builder()
            .register_dynamic("fixture-math", route, |mut context, request| async move {
                let value: u64 = context.decode_request_body(&request).await?;
                context.respond_value(request.route, value * 2)?;
                Ok(ServiceDisposition::Continue)
            })
            .build();
        let requirement = ServiceRequirement::new(INTERFACE, "1.0.0")
            .from_service("fixture-math")
            .with_operation(OPERATION)
            .with_caller_capability(OPERATION);
        let caller =
            ComponentDescriptor::new("fixture.math.consumer", "1.0.0").require(requirement.clone());
        let data_dir = Arc::new(sinomo_core::local::SinomoDataDir::new(
            sinomo_testdir::TestDir::new("component-service-client").path(),
        ));
        let client = ComponentServiceClient::new(
            caller,
            data_dir,
            ServiceRegistrySnapshotStore::new(registry),
        );

        let calls = client
            .call_local_all::<_, u64>(
                &requirement,
                OPERATION,
                &21_u64,
                std::time::Duration::from_secs(1),
                1024,
            )
            .await
            .expect("authorize service requirement");
        assert_eq!(calls.len(), 1);
        let response = calls[0].as_ref().expect("provider response");
        assert_eq!(response.value, 42);
        assert_eq!(response.provider.component_id, "fixture.math.provider");
        assert_eq!(response.provider.component_generation, 4);
    }

    #[tokio::test]
    async fn undeclared_operation_fails_before_dispatch() {
        let requirement = ServiceRequirement::new(INTERFACE, "1.0.0");
        let caller =
            ComponentDescriptor::new("fixture.math.consumer", "1.0.0").require(requirement.clone());
        let data_dir = Arc::new(sinomo_core::local::SinomoDataDir::new(
            sinomo_testdir::TestDir::new("component-service-client-denied").path(),
        ));
        let client =
            ComponentServiceClient::new(caller, data_dir, ServiceRegistrySnapshotStore::default());
        let error = client
            .call_local_all::<_, u64>(
                &requirement,
                OPERATION,
                &21_u64,
                std::time::Duration::from_secs(1),
                1024,
            )
            .await
            .expect_err("undeclared operation must fail closed");
        assert!(matches!(
            error,
            ComponentServiceError::UndeclaredOperation { .. }
        ));
    }

    #[tokio::test]
    async fn provider_deadline_is_enforced_per_call() {
        let route = ServiceRoute::new("/fixture/math/slow", "fixture.math")
            .with_auth_scope("component")
            .require_capability(OPERATION)
            .with_discovery_identity(ServiceDiscoveryIdentity {
                service_id: "fixture-math".to_string(),
                interface_id: INTERFACE.to_string(),
                interface_version: "1.0.0".to_string(),
                provider_capabilities: vec![OPERATION.to_string()],
                caller_requirements: vec![OPERATION.to_string()],
                component_id: "fixture.math.slow".to_string(),
                component_version: "1.0.0".to_string(),
                artifact_id: None,
                component_generation: 1,
                health: ServiceDiscoveryHealth::Ready,
                locality: ServiceDiscoveryLocality::Local,
            });
        let registry = ServiceRegistry::builder()
            .register_dynamic("fixture-math", route, |mut context, request| async move {
                let value: u64 = context.decode_request_body(&request).await?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                context.respond_value(request.route, value)?;
                Ok(ServiceDisposition::Continue)
            })
            .build();
        let requirement = ServiceRequirement::new(INTERFACE, "1.0.0")
            .from_service("fixture-math")
            .with_operation(OPERATION)
            .with_caller_capability(OPERATION);
        let caller =
            ComponentDescriptor::new("fixture.math.consumer", "1.0.0").require(requirement.clone());
        let client = ComponentServiceClient::new(
            caller,
            Arc::new(sinomo_core::local::SinomoDataDir::new(
                sinomo_testdir::TestDir::new("component-service-client-timeout").path(),
            )),
            ServiceRegistrySnapshotStore::new(registry),
        );
        let calls = client
            .call_local_all::<_, u64>(
                &requirement,
                OPERATION,
                &1_u64,
                std::time::Duration::from_millis(1),
                1024,
            )
            .await
            .expect("authorized calls");
        let error = calls[0].as_ref().expect_err("provider should time out");
        assert!(error.detail.contains("timed out"));
    }
}
