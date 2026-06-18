use crate::error::NetworkError;
use crate::object::{ObjectFrameIoOptions, SinomoObjectCodec};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use sinomo_core::CapabilityName;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncWrite};
use tower_layer::Layer;
use tower_service::Service;

pub type ObjectRpcFuture =
    Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static>>;

type ObjectRouteHandler = Arc<dyn Fn(ObjectRpcRequest) -> ObjectRpcFuture + Send + Sync + 'static>;
pub type ObjectRpcAuthorizationFuture =
    Pin<Box<dyn Future<Output = Result<(), NetworkError>> + Send + 'static>>;
type ObjectRpcAuthorizer = Arc<
    dyn Fn(ObjectRouteDescriptor, ObjectRpcAuthorizationContext) -> ObjectRpcAuthorizationFuture
        + Send
        + Sync
        + 'static,
>;

pub const OBJECT_RPC_ERROR_METADATA_KEY: &str = "sinomo-error";
pub const OBJECT_RPC_ERROR_DETAIL_METADATA_KEY: &str = "sinomo-error-detail";
pub const OBJECT_RPC_OPERATION_ID_METADATA_KEY: &str = "sinomo-operation-id";
pub const OBJECT_RPC_CANCELS_OPERATION_ID_METADATA_KEY: &str = "sinomo-cancels-operation-id";
pub const OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY: &str = "sinomo-response-retry-safe";

pub const CLUSTER_HEALTH_RPC_CATEGORY: &str = "cluster.health";
pub const CLUSTER_HEALTH_ROUTE: &str = "/cluster/health";
pub const CLUSTER_HEALTH_SCOPE_METADATA_KEY: &str = "sinomo-health-scope";
pub const CLUSTER_HEALTH_SCOPE_LOCAL: &str = "local";
pub const CLUSTER_NETWORK_RPC_CATEGORY: &str = "cluster.network";
pub const CLUSTER_NETWORK_PING_ROUTE: &str = "/cluster/network/ping";
pub const CLUSTER_MEMBERSHIP_RPC_CATEGORY: &str = "cluster.membership";
pub const CLUSTER_MEMBER_PUBLISH_ROUTE: &str = "/cluster/membership/member-publish";
pub const CLUSTER_INVITE_PUBLISH_ROUTE: &str = "/cluster/membership/invite-publish";
pub const CLUSTER_JOIN_CHALLENGE_ROUTE: &str = "/cluster/join/challenge";
pub const CLUSTER_JOIN_REGISTER_ROUTE: &str = "/cluster/join/register";
pub const CLUSTER_HEALTH_RPC_ROUTE_METADATA: &[(&str, &str)] =
    &[(CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY)];
pub const CLUSTER_NETWORK_RPC_ROUTE_METADATA: &[(&str, &str)] =
    &[(CLUSTER_NETWORK_PING_ROUTE, CLUSTER_NETWORK_RPC_CATEGORY)];
pub const CLUSTER_MEMBERSHIP_RPC_ROUTE_METADATA: &[(&str, &str)] = &[
    (
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    ),
    (CLUSTER_JOIN_REGISTER_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY),
    (
        CLUSTER_INVITE_PUBLISH_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    ),
    (
        CLUSTER_MEMBER_PUBLISH_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    ),
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcRequest {
    pub route: String,
    pub category: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub auth: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

impl ObjectRpcRequest {
    pub fn from_encoded_body(
        route: impl Into<String>,
        category: impl Into<String>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            metadata: BTreeMap::new(),
            auth: None,
            body,
        }
    }

    pub fn encode_body<T: Serialize + ?Sized>(
        route: impl Into<String>,
        category: impl Into<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            route: route.into(),
            category: category.into(),
            metadata: BTreeMap::new(),
            auth: None,
            body: SinomoObjectCodec::network_frame().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        SinomoObjectCodec::network_frame().decode(&self.body)
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_auth(mut self, auth: Vec<u8>) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_operation_id(self, operation_id: impl Into<String>) -> Self {
        self.with_metadata(OBJECT_RPC_OPERATION_ID_METADATA_KEY, operation_id)
    }

    pub fn with_cancels_operation_id(self, operation_id: impl Into<String>) -> Self {
        self.with_metadata(OBJECT_RPC_CANCELS_OPERATION_ID_METADATA_KEY, operation_id)
    }

    pub fn with_response_retry_safe(self) -> Self {
        self.with_metadata(OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY, "true")
    }

    pub fn operation_id(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_OPERATION_ID_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn cancels_operation_id(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_CANCELS_OPERATION_ID_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn is_response_retry_safe(&self) -> bool {
        self.metadata
            .get(OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY)
            .is_some_and(|value| value == "true")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcCancelRequest {
    pub operation_id: String,
    pub reason: String,
}

impl ObjectRpcCancelRequest {
    pub fn new(operation_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            operation_id: operation_id.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcCancelResponse {
    pub operation_id: String,
    pub cancelled: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRpcAuthorizationContext {
    pub route: String,
    pub category: String,
    pub metadata: BTreeMap<String, String>,
}

impl ObjectRpcAuthorizationContext {
    pub fn from_request(request: &ObjectRpcRequest) -> Self {
        Self {
            route: request.route.clone(),
            category: request.category.clone(),
            metadata: request.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcResponse {
    pub route: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

impl ObjectRpcResponse {
    pub fn from_encoded_body(route: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            route: route.into(),
            metadata: BTreeMap::new(),
            body,
        }
    }

    pub fn encode_body<T: Serialize + ?Sized>(
        route: impl Into<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            route: route.into(),
            metadata: BTreeMap::new(),
            body: SinomoObjectCodec::network_frame().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        self.error_detail()?;
        SinomoObjectCodec::network_frame().decode(&self.body)
    }

    pub fn encode_error(
        route: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<Self, NetworkError> {
        let detail = detail.into();
        let mut metadata = BTreeMap::new();
        metadata.insert(
            OBJECT_RPC_ERROR_METADATA_KEY.to_string(),
            "true".to_string(),
        );
        metadata.insert(
            OBJECT_RPC_ERROR_DETAIL_METADATA_KEY.to_string(),
            detail.clone(),
        );
        Ok(Self {
            route: route.into(),
            metadata,
            body: SinomoObjectCodec::network_frame().encode(&detail)?,
        })
    }

    pub fn error_detail(&self) -> Result<(), NetworkError> {
        if self
            .metadata
            .get(OBJECT_RPC_ERROR_METADATA_KEY)
            .is_some_and(|value| value == "true")
        {
            let detail = self
                .metadata
                .get(OBJECT_RPC_ERROR_DETAIL_METADATA_KEY)
                .cloned()
                .unwrap_or_else(|| "daemon returned an RPC error".to_string());
            if let Some(detail) = detail.strip_prefix("cluster is read-only: ") {
                return Err(NetworkError::ClusterReadOnly {
                    detail: detail.to_string(),
                });
            }
            if matches!(
                detail.as_str(),
                "device authorization was denied" | "network authorization was denied"
            ) {
                return Err(NetworkError::AuthDenied);
            }
            return Err(NetworkError::Serialization { detail });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceCategory {
    pub name: String,
    pub summary: String,
}

impl ServiceCategory {
    pub fn new(name: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            summary: summary.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRouteDescriptor {
    pub route: String,
    pub category: String,
    pub summary: String,
    pub input_type: String,
    pub output_type: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub observability: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl ObjectRouteDescriptor {
    pub fn new(
        route: impl Into<String>,
        category: impl Into<String>,
        summary: impl Into<String>,
        input_type: impl Into<String>,
        output_type: impl Into<String>,
    ) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            summary: summary.into(),
            input_type: input_type.into(),
            output_type: output_type.into(),
            observability: BTreeMap::new(),
            auth_scope: None,
            required_capabilities: Vec::new(),
            concurrency_limit: None,
            timeout_ms: None,
        }
    }

    pub fn observable(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.observability.insert(key.into(), value.into());
        self
    }

    pub fn with_auth_scope(mut self, scope: impl Into<String>) -> Self {
        self.auth_scope = Some(scope.into());
        self
    }

    pub fn require_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }

    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = Some(limit);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_ms = Some(timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        self
    }

    pub fn for_types<Input, Output>(
        route: impl Into<String>,
        category: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self::new(
            route,
            category,
            summary,
            std::any::type_name::<Input>(),
            std::any::type_name::<Output>(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRpcRouteReadiness {
    pub route: String,
    pub category: String,
    pub concurrency_limit: Option<usize>,
    pub available_concurrency: Option<usize>,
    pub timeout_ms: Option<u64>,
}

impl ObjectRpcRouteReadiness {
    pub fn is_ready(&self) -> bool {
        self.available_concurrency
            .is_none_or(|available| available > 0)
    }

    pub fn is_saturated(&self) -> bool {
        !self.is_ready()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRpcCategoryDefinition {
    pub category: ServiceCategory,
    pub routes: Vec<ObjectRouteDescriptor>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceCatalog {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub categories: BTreeMap<String, ServiceCategory>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routes: BTreeMap<String, ObjectRouteDescriptor>,
}

impl ServiceCatalog {
    pub fn register_category(&mut self, category: ServiceCategory) {
        self.categories.insert(category.name.clone(), category);
    }

    pub fn register_route(&mut self, descriptor: ObjectRouteDescriptor) {
        self.routes.insert(descriptor.route.clone(), descriptor);
    }

    pub fn register_category_definition(&mut self, definition: ObjectRpcCategoryDefinition) {
        self.register_category(definition.category);
        for route in definition.routes {
            self.register_route(route);
        }
    }

    pub fn validate(&self) -> Result<(), NetworkError> {
        for (category_name, category) in &self.categories {
            validate_rpc_token(&category.name, "object rpc category")?;
            validate_non_empty_rpc_text(&category.summary, "object rpc category summary")?;
            if category_name != &category.name {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "object rpc category key {category_name} does not match category name {}",
                        category.name
                    ),
                });
            }
        }
        for (route, descriptor) in &self.routes {
            validate_rpc_route(&descriptor.route)?;
            validate_rpc_token(&descriptor.category, "object rpc route category")?;
            validate_non_empty_rpc_text(&descriptor.summary, "object rpc route summary")?;
            validate_non_empty_rpc_text(&descriptor.input_type, "object rpc route input type")?;
            validate_non_empty_rpc_text(&descriptor.output_type, "object rpc route output type")?;
            if route != &descriptor.route {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "object rpc route key {route} does not match descriptor route {}",
                        descriptor.route
                    ),
                });
            }
            if let Some(auth_scope) = &descriptor.auth_scope {
                validate_rpc_token(auth_scope, &format!("object rpc route {route} auth scope"))?;
            }
            let mut capabilities = BTreeSet::new();
            for capability in &descriptor.required_capabilities {
                if !capabilities.insert(capability) {
                    return Err(NetworkError::Serialization {
                        detail: format!(
                            "object rpc route {route} declares duplicate capability {capability}"
                        ),
                    });
                }
                CapabilityName::new(capability.clone()).map_err(|source| {
                    NetworkError::Serialization {
                        detail: format!(
                            "object rpc route {route} capability {capability} is invalid: {source}"
                        ),
                    }
                })?;
            }
            for (key, value) in &descriptor.observability {
                validate_rpc_metadata(key, &format!("object rpc route {route} observability key"))?;
                validate_rpc_metadata(
                    value,
                    &format!("object rpc route {route} observability value"),
                )?;
            }
            if matches!(descriptor.concurrency_limit, Some(0)) {
                return Err(NetworkError::Serialization {
                    detail: format!("object rpc route {route} has zero concurrency limit"),
                });
            }
            if matches!(descriptor.timeout_ms, Some(0)) {
                return Err(NetworkError::Serialization {
                    detail: format!("object rpc route {route} has zero timeout"),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceCatalogSnapshot {
    pub generated_at: OffsetDateTime,
    pub version: u64,
    pub catalog: ServiceCatalog,
}

impl ServiceCatalogSnapshot {
    pub fn now(catalog: ServiceCatalog) -> Self {
        Self {
            generated_at: OffsetDateTime::now_utc(),
            version: 0,
            catalog,
        }
    }
}

#[derive(Clone)]
struct ObjectRouteEntry {
    category: String,
    handler: ObjectRouteHandler,
}

impl ObjectRouteEntry {
    fn new(descriptor: &ObjectRouteDescriptor, handler: ObjectRouteHandler) -> Self {
        Self {
            category: descriptor.category.clone(),
            handler,
        }
    }

    fn call(&self, request: ObjectRpcRequest) -> ObjectRpcFuture {
        let route = request.route.clone();
        let expected_category = self.category.clone();
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            if request.category != expected_category {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "object rpc route {route} expected category {expected_category}, got {}",
                        request.category
                    ),
                });
            }
            handler(request).await
        })
    }
}

#[derive(Clone)]
pub struct ObjectRpcRouteRegistry {
    version: u64,
    generated_at: OffsetDateTime,
    catalog: ServiceCatalog,
    routes: BTreeMap<String, ObjectRouteEntry>,
}

impl Default for ObjectRpcRouteRegistry {
    fn default() -> Self {
        Self {
            version: 0,
            generated_at: OffsetDateTime::now_utc(),
            catalog: ServiceCatalog::default(),
            routes: BTreeMap::new(),
        }
    }
}

impl ObjectRpcRouteRegistry {
    pub fn builder() -> ObjectRpcRouteRegistryBuilder {
        let mut registry = Self::default();
        registry.version = 1;
        ObjectRpcRouteRegistryBuilder { registry }
    }

    pub fn to_builder(&self) -> ObjectRpcRouteRegistryBuilder {
        let mut registry = self.clone();
        registry.version = self.next_version();
        ObjectRpcRouteRegistryBuilder { registry }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn generated_at(&self) -> OffsetDateTime {
        self.generated_at
    }

    pub fn catalog(&self) -> &ServiceCatalog {
        &self.catalog
    }

    pub fn validate(&self) -> Result<(), NetworkError> {
        self.catalog.validate()?;
        for route in self.routes.keys() {
            if !self.catalog.routes.contains_key(route) {
                return Err(NetworkError::Serialization {
                    detail: format!("object rpc route handler {route} has no catalog descriptor"),
                });
            }
        }
        Ok(())
    }

    fn next_version(&self) -> u64 {
        self.version.saturating_add(1)
    }

    fn route(&self, route: &str) -> Option<ObjectRouteEntry> {
        self.routes.get(route).cloned()
    }

    pub fn route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        let descriptor = self.catalog.routes.get(route)?;
        self.routes.get(route)?;
        Some(ObjectRpcRouteReadiness {
            route: descriptor.route.clone(),
            category: descriptor.category.clone(),
            concurrency_limit: descriptor.concurrency_limit,
            available_concurrency: None,
            timeout_ms: descriptor.timeout_ms,
        })
    }
}

pub struct ObjectRpcRouteRegistryBuilder {
    registry: ObjectRpcRouteRegistry,
}

impl ObjectRpcRouteRegistryBuilder {
    pub fn register_category(mut self, category: ServiceCategory) -> Self {
        self.registry.catalog.register_category(category);
        self
    }

    pub fn register_category_definition(mut self, definition: ObjectRpcCategoryDefinition) -> Self {
        self.registry
            .catalog
            .register_category_definition(definition);
        self
    }

    pub fn register_route<F, Fut>(self, descriptor: ObjectRouteDescriptor, handler: F) -> Self
    where
        F: Fn(ObjectRpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
    {
        self.try_register_route(descriptor, handler)
            .expect("valid object rpc route registration")
    }

    pub fn try_register_route<F, Fut>(
        mut self,
        descriptor: ObjectRouteDescriptor,
        handler: F,
    ) -> Result<Self, NetworkError>
    where
        F: Fn(ObjectRpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
    {
        let handler = Arc::new(move |request| Box::pin(handler(request)) as ObjectRpcFuture);
        let route = descriptor.route.clone();
        if self.registry.catalog.routes.contains_key(&route)
            || self.registry.routes.contains_key(&route)
        {
            return Err(NetworkError::Serialization {
                detail: format!("duplicate object rpc route {route}"),
            });
        }
        self.registry.catalog.register_route(descriptor.clone());
        self.registry
            .routes
            .insert(route, ObjectRouteEntry::new(&descriptor, handler));
        Ok(self)
    }

    pub fn retain_routes<F>(mut self, mut retain: F) -> Self
    where
        F: FnMut(&ObjectRouteDescriptor) -> bool,
    {
        let removed_routes: Vec<String> = self
            .registry
            .catalog
            .routes
            .iter()
            .filter_map(|(route, descriptor)| (!retain(descriptor)).then(|| route.clone()))
            .collect();
        for route in removed_routes {
            self.registry.catalog.routes.remove(&route);
            self.registry.routes.remove(&route);
        }
        self
    }

    pub fn replace_route<F, Fut>(self, descriptor: ObjectRouteDescriptor, handler: F) -> Self
    where
        F: Fn(ObjectRpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
    {
        self.try_replace_route(descriptor, handler)
            .expect("valid object rpc route replacement")
    }

    pub fn try_replace_route<F, Fut>(
        mut self,
        descriptor: ObjectRouteDescriptor,
        handler: F,
    ) -> Result<Self, NetworkError>
    where
        F: Fn(ObjectRpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
    {
        let handler = Arc::new(move |request| Box::pin(handler(request)) as ObjectRpcFuture);
        let route = descriptor.route.clone();
        if !self.registry.catalog.routes.contains_key(&route)
            || !self.registry.routes.contains_key(&route)
        {
            return Err(NetworkError::UnknownRoute { route });
        }
        self.registry.catalog.register_route(descriptor.clone());
        self.registry
            .routes
            .insert(route, ObjectRouteEntry::new(&descriptor, handler));
        Ok(self)
    }

    pub fn try_replace_route_set<I, F, Fut, R>(
        self,
        mut remove_existing: R,
        descriptors: I,
        handler: F,
    ) -> Result<Self, NetworkError>
    where
        I: IntoIterator<Item = ObjectRouteDescriptor>,
        F: Fn(ObjectRpcRequest) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
        R: FnMut(&ObjectRouteDescriptor) -> bool,
    {
        let mut builder = self.retain_routes(|descriptor| !remove_existing(descriptor));
        for descriptor in descriptors {
            let route_handler = handler.clone();
            builder =
                builder.try_register_route(descriptor, move |request| route_handler(request))?;
        }
        Ok(builder)
    }

    pub fn catalog(&self) -> &ServiceCatalog {
        &self.registry.catalog
    }

    pub fn try_build(mut self) -> Result<Arc<ObjectRpcRouteRegistry>, NetworkError> {
        self.registry.generated_at = OffsetDateTime::now_utc();
        self.registry.validate()?;
        Ok(Arc::new(self.registry))
    }

    pub fn build(self) -> Arc<ObjectRpcRouteRegistry> {
        self.try_build().expect("valid object rpc route registry")
    }
}

#[derive(Clone)]
pub struct ObjectRpcRouter {
    active: Arc<ArcSwap<ObjectRpcRouteRegistry>>,
}

impl Default for ObjectRpcRouter {
    fn default() -> Self {
        Self {
            active: Arc::new(ArcSwap::from_pointee(ObjectRpcRouteRegistry::default())),
        }
    }
}

impl ObjectRpcRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_route_registry(
        registry: Arc<ObjectRpcRouteRegistry>,
    ) -> Result<Self, NetworkError> {
        registry.validate()?;
        Ok(Self {
            active: Arc::new(ArcSwap::from(registry)),
        })
    }

    pub fn catalog(&self) -> ServiceCatalog {
        self.route_registry_snapshot().catalog().clone()
    }

    pub fn catalog_snapshot(&self) -> ServiceCatalogSnapshot {
        let registry = self.route_registry_snapshot();
        ServiceCatalogSnapshot {
            generated_at: registry.generated_at(),
            version: registry.version(),
            catalog: registry.catalog().clone(),
        }
    }

    pub fn route_registry_snapshot(&self) -> Arc<ObjectRpcRouteRegistry> {
        self.active.load_full()
    }

    pub fn route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        self.route_registry_snapshot().route_readiness(route)
    }

    pub fn replace_route_registry(
        &self,
        registry: Arc<ObjectRpcRouteRegistry>,
    ) -> Result<(), NetworkError> {
        registry.validate()?;
        loop {
            let active = self.active.load_full();
            let active_version = active.version();
            if registry.version() <= active_version {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "object rpc route registry snapshot version {} is not newer than active version {active_version}",
                        registry.version()
                    ),
                });
            }
            let previous = self.active.compare_and_swap(&active, Arc::clone(&registry));
            if Arc::ptr_eq(&active, &previous) {
                return Ok(());
            }
        }
    }

    pub fn update_route_registry<F>(
        &self,
        update: F,
    ) -> Result<Arc<ObjectRpcRouteRegistry>, NetworkError>
    where
        F: FnOnce(
            ObjectRpcRouteRegistryBuilder,
        ) -> Result<ObjectRpcRouteRegistryBuilder, NetworkError>,
    {
        let active = self.route_registry_snapshot();
        let registry = update(active.to_builder())?.try_build()?;
        self.replace_route_registry(Arc::clone(&registry))?;
        Ok(registry)
    }

    pub fn replace_route_set<I, F, Fut, R>(
        &self,
        remove_existing: R,
        descriptors: I,
        handler: F,
    ) -> Result<Arc<ObjectRpcRouteRegistry>, NetworkError>
    where
        I: IntoIterator<Item = ObjectRouteDescriptor>,
        F: Fn(ObjectRpcRequest) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
        R: FnMut(&ObjectRouteDescriptor) -> bool,
    {
        self.update_route_registry(|builder| {
            builder.try_replace_route_set(remove_existing, descriptors, handler)
        })
    }

    fn call_route(&self, request: ObjectRpcRequest) -> ObjectRpcFuture {
        let registry = self.route_registry_snapshot();
        let Some(route) = registry.route(&request.route) else {
            let route = request.route.clone();
            return Box::pin(async move { Err(NetworkError::UnknownRoute { route }) });
        };
        Box::pin(async move { route.call(request).await })
    }
}

pub trait ObjectRpcRouteCatalog {
    fn object_rpc_route_descriptor(&self, route: &str) -> Option<ObjectRouteDescriptor>;

    fn object_rpc_route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        self.object_rpc_route_descriptor(route)
            .map(|descriptor| ObjectRpcRouteReadiness {
                route: descriptor.route,
                category: descriptor.category,
                concurrency_limit: descriptor.concurrency_limit,
                available_concurrency: None,
                timeout_ms: descriptor.timeout_ms,
            })
    }
}

impl ObjectRpcRouteCatalog for ObjectRpcRouter {
    fn object_rpc_route_descriptor(&self, route: &str) -> Option<ObjectRouteDescriptor> {
        self.route_registry_snapshot()
            .catalog()
            .routes
            .get(route)
            .cloned()
    }

    fn object_rpc_route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        self.route_readiness(route)
    }
}

#[derive(Clone)]
pub struct ObjectRpcAuthorizationLayer {
    authorizer: ObjectRpcAuthorizer,
}

impl ObjectRpcAuthorizationLayer {
    pub fn new<F, Fut>(authorizer: F) -> Self
    where
        F: Fn(ObjectRouteDescriptor, ObjectRpcAuthorizationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), NetworkError>> + Send + 'static,
    {
        Self {
            authorizer: Arc::new(move |descriptor, context| {
                Box::pin(authorizer(descriptor, context)) as ObjectRpcAuthorizationFuture
            }),
        }
    }
}

#[derive(Clone)]
pub struct ObjectRpcAuthorizationService<S> {
    inner: S,
    authorizer: ObjectRpcAuthorizer,
}

impl<S> ObjectRpcAuthorizationService<S> {
    fn new(inner: S, authorizer: ObjectRpcAuthorizer) -> Self {
        Self { inner, authorizer }
    }

    fn take_poll_ready_inner_for_call(&mut self) -> S
    where
        S: Clone,
    {
        // Tower readiness is tied to the service instance. Move the instance
        // checked by `poll_ready` into the call future and keep a clone in the
        // wrapper for the next readiness cycle.
        let replacement = self.inner.clone();
        std::mem::replace(&mut self.inner, replacement)
    }
}

#[derive(Clone)]
pub struct ObjectRpcRoutePolicyLayer {
    catalog: Arc<ServiceCatalog>,
    routes: Arc<BTreeMap<String, ObjectRpcRoutePolicyEntry>>,
}

#[derive(Clone)]
struct ObjectRpcRoutePolicyEntry {
    category: String,
    concurrency_limit: Option<usize>,
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
    timeout: Option<Duration>,
}

impl ObjectRpcRoutePolicyEntry {
    fn new(descriptor: &ObjectRouteDescriptor) -> Self {
        Self {
            category: descriptor.category.clone(),
            concurrency_limit: descriptor.concurrency_limit,
            semaphore: descriptor
                .concurrency_limit
                .map(|limit| Arc::new(tokio::sync::Semaphore::new(limit))),
            timeout: descriptor.timeout_ms.map(Duration::from_millis),
        }
    }

    fn available_concurrency(&self) -> Option<usize> {
        self.semaphore
            .as_ref()
            .map(|semaphore| semaphore.available_permits())
    }
}

impl ObjectRpcRoutePolicyLayer {
    pub fn new(catalog: ServiceCatalog) -> Result<Self, NetworkError> {
        catalog.validate()?;
        let routes = catalog
            .routes
            .iter()
            .map(|(route, descriptor)| (route.clone(), ObjectRpcRoutePolicyEntry::new(descriptor)))
            .collect::<BTreeMap<_, _>>();
        Ok(Self {
            catalog: Arc::new(catalog),
            routes: Arc::new(routes),
        })
    }

    pub fn catalog(&self) -> &ServiceCatalog {
        &self.catalog
    }
}

#[derive(Clone)]
pub struct ObjectRpcRoutePolicyService<S> {
    inner: S,
    layer: ObjectRpcRoutePolicyLayer,
}

impl<S> ObjectRpcRoutePolicyService<S> {
    fn new(inner: S, layer: ObjectRpcRoutePolicyLayer) -> Self {
        Self { inner, layer }
    }

    fn take_poll_ready_inner_for_call(&mut self) -> S
    where
        S: Clone,
    {
        let replacement = self.inner.clone();
        std::mem::replace(&mut self.inner, replacement)
    }
}

impl<S> Layer<S> for ObjectRpcRoutePolicyLayer {
    type Service = ObjectRpcRoutePolicyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ObjectRpcRoutePolicyService::new(inner, self.clone())
    }
}

impl<S> ObjectRpcRouteCatalog for ObjectRpcRoutePolicyService<S>
where
    S: ObjectRpcRouteCatalog,
{
    fn object_rpc_route_descriptor(&self, route: &str) -> Option<ObjectRouteDescriptor> {
        self.layer.catalog.routes.get(route).cloned()
    }

    fn object_rpc_route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        let descriptor = self.object_rpc_route_descriptor(route)?;
        let entry = self.layer.routes.get(route)?;
        Some(ObjectRpcRouteReadiness {
            route: descriptor.route,
            category: descriptor.category,
            concurrency_limit: descriptor.concurrency_limit,
            available_concurrency: entry.available_concurrency(),
            timeout_ms: descriptor.timeout_ms,
        })
    }
}

impl<S> ObjectRpcRouteCatalog for ObjectRpcAuthorizationService<S>
where
    S: ObjectRpcRouteCatalog,
{
    fn object_rpc_route_descriptor(&self, route: &str) -> Option<ObjectRouteDescriptor> {
        self.inner.object_rpc_route_descriptor(route)
    }

    fn object_rpc_route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        self.inner.object_rpc_route_readiness(route)
    }
}

impl<S> Layer<S> for ObjectRpcAuthorizationLayer {
    type Service = ObjectRpcAuthorizationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ObjectRpcAuthorizationService::new(inner, Arc::clone(&self.authorizer))
    }
}

#[macro_export]
macro_rules! object_rpc_category {
    (
        $name:expr,
        $summary:expr,
        [
            $(
                route(
                    $route:expr,
                    $route_summary:expr,
                    $input:ty => $output:ty
                    $(, observe($observe_key:expr => $observe_value:expr))*
                )
            ),* $(,)?
        ]
    ) => {{
        let category_name = $name.to_string();
        $crate::ObjectRpcCategoryDefinition {
            category: $crate::ServiceCategory::new(category_name.clone(), $summary),
            routes: vec![
                $(
                    $crate::ObjectRouteDescriptor::for_types::<$input, $output>(
                        $route,
                        category_name.clone(),
                        $route_summary,
                    )
                    $(
                        .observable($observe_key, $observe_value)
                    )*
                ),*
            ],
        }
    }};
}

impl Service<ObjectRpcRequest> for ObjectRpcRouter {
    type Response = ObjectRpcResponse;
    type Error = NetworkError;
    type Future = ObjectRpcFuture;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Tower readiness has no request, so it cannot account for route-specific
        // concurrency limits. Hosts that need scheduler backpressure should ask
        // `route_readiness` for the selected route before dispatching.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ObjectRpcRequest) -> Self::Future {
        self.call_route(request)
    }
}

impl<S> Service<ObjectRpcRequest> for ObjectRpcAuthorizationService<S>
where
    S: Service<ObjectRpcRequest, Response = ObjectRpcResponse, Error = NetworkError>
        + ObjectRpcRouteCatalog
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = ObjectRpcResponse;
    type Error = NetworkError;
    type Future = ObjectRpcFuture;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: ObjectRpcRequest) -> Self::Future {
        let descriptor = self.inner.object_rpc_route_descriptor(&request.route);
        let authorizer = Arc::clone(&self.authorizer);
        let context = ObjectRpcAuthorizationContext::from_request(&request);
        let future = match descriptor {
            Some(descriptor) => authorizer(descriptor, context),
            None => {
                let route = request.route.clone();
                return Box::pin(async move { Err(NetworkError::UnknownRoute { route }) });
            }
        };
        let mut inner = self.take_poll_ready_inner_for_call();
        Box::pin(async move {
            future.await?;
            inner.call(request).await
        })
    }
}

impl<S> Service<ObjectRpcRequest> for ObjectRpcRoutePolicyService<S>
where
    S: Service<ObjectRpcRequest, Response = ObjectRpcResponse, Error = NetworkError>
        + ObjectRpcRouteCatalog
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = ObjectRpcResponse;
    type Error = NetworkError;
    type Future = ObjectRpcFuture;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: ObjectRpcRequest) -> Self::Future {
        let route = request.route.clone();
        let Some(entry) = self.layer.routes.get(&route).cloned() else {
            return Box::pin(async move { Err(NetworkError::UnknownRoute { route }) });
        };
        if request.category != entry.category {
            let expected_category = entry.category;
            return Box::pin(async move {
                Err(NetworkError::Serialization {
                    detail: format!(
                        "object rpc route {route} expected category {expected_category}, got {}",
                        request.category
                    ),
                })
            });
        }

        let mut inner = self.take_poll_ready_inner_for_call();
        Box::pin(async move {
            let timeout_route = route.clone();
            let run = async move {
                let _permit = if let Some(semaphore) = entry.semaphore {
                    match semaphore.try_acquire_owned() {
                        Ok(permit) => Some(permit),
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            return Err(NetworkError::ObjectRpcRouteSaturated {
                                route,
                                concurrency_limit: entry.concurrency_limit.unwrap_or(0),
                            });
                        }
                        Err(tokio::sync::TryAcquireError::Closed) => {
                            return Err(NetworkError::Io {
                                operation: format!("object rpc route {route}"),
                                detail: "route limiter closed".to_string(),
                            });
                        }
                    }
                } else {
                    None
                };
                inner.call(request).await
            };
            if let Some(timeout) = entry.timeout {
                tokio::time::timeout(timeout, run)
                    .await
                    .map_err(|_| NetworkError::Io {
                        operation: format!("object rpc route {timeout_route}"),
                        detail: format!("timed out after {}ms", timeout.as_millis()),
                    })?
            } else {
                run.await
            }
        })
    }
}

pub async fn call_object_rpc_frame_async<IO>(
    stream: &mut IO,
    request: &ObjectRpcRequest,
    max_response_bytes: usize,
    frame_options: ObjectFrameIoOptions,
) -> Result<ObjectRpcResponse, NetworkError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    crate::write_object_frame_async(stream, request, frame_options).await?;
    crate::read_object_frame_async(stream, max_response_bytes, frame_options).await
}

pub async fn call_object_rpc_service_async<S>(
    service: &mut S,
    request: ObjectRpcRequest,
) -> Result<ObjectRpcResponse, NetworkError>
where
    S: Service<ObjectRpcRequest, Response = ObjectRpcResponse, Error = NetworkError>,
{
    std::future::poll_fn(|context| service.poll_ready(context)).await?;
    service.call(request).await
}

pub async fn serve_object_rpc_frame_async<IO, S>(
    stream: &mut IO,
    service: &mut S,
    max_request_bytes: usize,
    frame_options: ObjectFrameIoOptions,
) -> Result<(), NetworkError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
    S: Service<ObjectRpcRequest, Response = ObjectRpcResponse, Error = NetworkError>,
{
    let request: ObjectRpcRequest =
        crate::read_object_frame_async(stream, max_request_bytes, frame_options).await?;
    let response = call_object_rpc_service_async(service, request).await?;
    crate::write_object_frame_async(stream, &response, frame_options).await
}

fn validate_rpc_route(route: &str) -> Result<(), NetworkError> {
    if route.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: "object rpc route descriptor has an empty route".to_string(),
        });
    }
    if !route.starts_with('/') {
        return Err(NetworkError::Serialization {
            detail: format!("object rpc route {route} must start with /"),
        });
    }
    validate_no_control_chars(route, &format!("object rpc route {route}"))
}

fn validate_rpc_token(value: &str, context: &str) -> Result<(), NetworkError> {
    if value.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: format!("{context} must be non-empty"),
        });
    }
    if value != value.trim() {
        return Err(NetworkError::Serialization {
            detail: format!("{context} must not have leading or trailing whitespace"),
        });
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
    {
        return Err(NetworkError::Serialization {
            detail: format!(
                "{context} must contain only ASCII letters, digits, '.', '-', '_', or ':'"
            ),
        });
    }
    Ok(())
}

fn validate_non_empty_rpc_text(value: &str, context: &str) -> Result<(), NetworkError> {
    if value.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: format!("{context} must be non-empty"),
        });
    }
    validate_no_control_chars(value, context)
}

fn validate_rpc_metadata(value: &str, context: &str) -> Result<(), NetworkError> {
    if value.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: format!("{context} must be non-empty"),
        });
    }
    validate_no_control_chars(value, context)
}

fn validate_no_control_chars(value: &str, context: &str) -> Result<(), NetworkError> {
    if value.chars().any(char::is_control) {
        return Err(NetworkError::Serialization {
            detail: format!("{context} must not contain control characters"),
        });
    }
    Ok(())
}
