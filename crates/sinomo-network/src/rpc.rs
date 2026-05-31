use crate::error::NetworkError;
use crate::object::SinomoObjectCodec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncWrite};
use tower_layer::Layer;
use tower_service::Service;

pub type ObjectRpcFuture =
    Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static>>;

type ObjectRouteHandler = Box<dyn Fn(ObjectRpcRequest) -> ObjectRpcFuture + Send + Sync + 'static>;

pub const OBJECT_RPC_ERROR_METADATA_KEY: &str = "sinomo-error";
pub const OBJECT_RPC_ERROR_DETAIL_METADATA_KEY: &str = "sinomo-error-detail";

pub const DAEMON_RPC_CATEGORY: &str = "daemon";
pub const DAEMON_STATUS_ROUTE: &str = "/daemon/status";
pub const DAEMON_SHUTDOWN_ROUTE: &str = "/daemon/shutdown";
pub const CLUSTER_KV_RPC_CATEGORY: &str = "cluster.kv";
pub const CLUSTER_KV_SNAPSHOT_ROUTE: &str = "/cluster/kv/snapshot";
pub const CLUSTER_KV_TXN_ROUTE: &str = "/cluster/kv/txn";
pub const CLUSTER_KV_RANGE_PREFIX_ROUTE: &str = "/cluster/kv/range-prefix";
pub const CLUSTER_KV_WATCH_PREFIX_ROUTE: &str = "/cluster/kv/watch-prefix";
pub const CLUSTER_KV_WATCH_DRAIN_ROUTE: &str = "/cluster/kv/watch-drain";
pub const CLUSTER_KV_WATCH_EVENTS_ROUTE: &str = "/cluster/kv/watch-events";
pub const CLUSTER_RAFT_RPC_CATEGORY: &str = "cluster.raft";
pub const CLUSTER_RAFT_APPEND_ROUTE: &str = "/cluster/raft/append";
pub const CLUSTER_RAFT_VOTE_ROUTE: &str = "/cluster/raft/vote";
pub const CLUSTER_MEMBERSHIP_RPC_CATEGORY: &str = "cluster.membership";
pub const CLUSTER_MEMBER_PUBLISH_ROUTE: &str = "/cluster/membership/member-publish";
pub const CLUSTER_INVITE_PUBLISH_ROUTE: &str = "/cluster/membership/invite-publish";
pub const CLUSTER_JOIN_CHALLENGE_ROUTE: &str = "/cluster/join/challenge";
pub const CLUSTER_JOIN_REGISTER_ROUTE: &str = "/cluster/join/register";
pub const DFS_RPC_CATEGORY: &str = "dfs";
pub const DFS_STATUS_ROUTE: &str = "/dfs/status";
pub const DFS_INSPECTION_ROUTE: &str = "/dfs/inspection";
pub const DFS_SMOKE_ROUTE: &str = "/dfs/smoke";
pub const DFS_EXPOSE_CONFIG_ROUTE: &str = "/dfs/expose/config";
pub const DFS_METADATA_JOURNAL_BATCH_ROUTE: &str = "/dfs/metadata/journal-batch";
pub const DFS_METADATA_SNAPSHOT_INSTALL_ROUTE: &str = "/dfs/metadata/snapshot-install";
pub const DFS_PROTOCOL_FRAME_ROUTE: &str = "/dfs/protocol/frame";
pub const SANDBOX_RPC_CATEGORY: &str = "sandbox";
pub const SANDBOX_CREATE_ROUTE: &str = "/sandbox/create";
pub const SANDBOX_STATUS_ROUTE: &str = "/sandbox/status";
pub const SANDBOX_UPDATE_ROUTE: &str = "/sandbox/update";
pub const SANDBOX_CHECKPOINT_ROUTE: &str = "/sandbox/checkpoint";
pub const SANDBOX_RESUME_ROUTE: &str = "/sandbox/resume";
pub const SANDBOX_REBOOT_ROUTE: &str = "/sandbox/reboot";
pub const SANDBOX_DESTROY_ROUTE: &str = "/sandbox/destroy";
pub const SANDBOX_LIST_ROUTE: &str = "/sandbox/list";
pub const SANDBOX_READ_ROUTE: &str = "/sandbox/read";
pub const SANDBOX_WRITE_ROUTE: &str = "/sandbox/write";
pub const SANDBOX_IMPORT_BUSYBOX_ROUTE: &str = "/sandbox/toolchain/import-busybox";
pub const SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE: &str = "/sandbox/toolchain/import-package";
pub const SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE: &str =
    "/sandbox/toolchain/import-package-from-path";
pub const SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE: &str = "/sandbox/toolchain/add-command";
pub const SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE: &str = "/sandbox/toolchain/add-artifact";
pub const SANDBOX_HTTP_FETCH_ROUTE: &str = "/sandbox/http/fetch";
pub const SANDBOX_HTTP_FETCH_TO_FILE_ROUTE: &str = "/sandbox/http/fetch-to-file";
pub const SANDBOX_RUN_TOOLCHAIN_ROUTE: &str = "/sandbox/run/toolchain";
pub const SANDBOX_RUN_PROCESS_PLAN_ROUTE: &str = "/sandbox/run/process-plan";
pub const SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE: &str = "/sandbox/run/toolchain/stream";
pub const SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE: &str = "/sandbox/run/process-plan/stream";
pub const ROLLOUT_RPC_CATEGORY: &str = "rollout";
pub const ROLLOUT_DIAGNOSTICS_ROUTE: &str = "/rollout/diagnostics";
pub const ROLLOUT_BINARY_ROUTE: &str = "/rollout/binary";
pub const ROLLOUT_BUILD_ROUTE: &str = "/rollout/build";
pub const ROLLOUT_FINALIZE_SHADOW_ROUTE: &str = "/rollout/finalize-shadow";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcRequest {
    pub route: String,
    pub category: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

impl ObjectRpcRequest {
    pub fn encode_body<T: Serialize + ?Sized>(
        route: impl Into<String>,
        category: impl Into<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            route: route.into(),
            category: category.into(),
            metadata: BTreeMap::new(),
            body: SinomoObjectCodec::default().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        SinomoObjectCodec::default().decode(&self.body)
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
    pub fn encode_body<T: Serialize + ?Sized>(
        route: impl Into<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            route: route.into(),
            metadata: BTreeMap::new(),
            body: SinomoObjectCodec::default().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        self.error_detail()?;
        SinomoObjectCodec::default().decode(&self.body)
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
            body: SinomoObjectCodec::default().encode(&detail)?,
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
        }
    }

    pub fn observable(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.observability.insert(key.into(), value.into());
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceCatalogSnapshot {
    pub generated_at: OffsetDateTime,
    pub catalog: ServiceCatalog,
}

impl ServiceCatalogSnapshot {
    pub fn now(catalog: ServiceCatalog) -> Self {
        Self {
            generated_at: OffsetDateTime::now_utc(),
            catalog,
        }
    }
}

#[derive(Default)]
pub struct ObjectRpcRouter {
    catalog: ServiceCatalog,
    routes: BTreeMap<String, ObjectRouteHandler>,
}

impl ObjectRpcRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn catalog(&self) -> &ServiceCatalog {
        &self.catalog
    }

    pub fn register_category(mut self, category: ServiceCategory) -> Self {
        self.catalog.register_category(category);
        self
    }

    pub fn register_category_definition(mut self, definition: ObjectRpcCategoryDefinition) -> Self {
        self.catalog
            .register_category_definition(ObjectRpcCategoryDefinition {
                category: definition.category,
                routes: definition.routes,
            });
        self
    }

    pub fn register_route<F, Fut>(mut self, descriptor: ObjectRouteDescriptor, handler: F) -> Self
    where
        F: Fn(ObjectRpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'static,
    {
        let route = descriptor.route.clone();
        self.catalog.register_route(descriptor);
        self.routes
            .insert(route, Box::new(move |request| Box::pin(handler(request))));
        self
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
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ObjectRpcRequest) -> Self::Future {
        let Some(handler) = self.routes.get(&request.route) else {
            let route = request.route.clone();
            return Box::pin(async move { Err(NetworkError::UnknownRoute { route }) });
        };
        handler(request)
    }
}

pub async fn call_object_rpc_frame_async<IO>(
    stream: &mut IO,
    request: &ObjectRpcRequest,
    max_response_bytes: usize,
) -> Result<ObjectRpcResponse, NetworkError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    crate::write_object_frame_async(stream, request).await?;
    crate::read_object_frame_async(stream, max_response_bytes).await
}

pub async fn serve_object_rpc_frame_async<IO>(
    stream: &mut IO,
    router: &mut ObjectRpcRouter,
    max_request_bytes: usize,
) -> Result<(), NetworkError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let request: ObjectRpcRequest =
        crate::read_object_frame_async(stream, max_request_bytes).await?;
    let response = router.call(request).await?;
    crate::write_object_frame_async(stream, &response).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCategoryLayer {
    pub category: ServiceCategory,
}

impl ServiceCategoryLayer {
    pub fn new(name: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            category: ServiceCategory::new(name, summary),
        }
    }
}

impl<S> Layer<S> for ServiceCategoryLayer {
    type Service = CategorizedService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CategorizedService {
            category: self.category.clone(),
            inner,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CategorizedService<S> {
    category: ServiceCategory,
    inner: S,
}

impl<S> CategorizedService<S> {
    pub fn category(&self) -> &ServiceCategory {
        &self.category
    }
}

impl<S> Service<ObjectRpcRequest> for CategorizedService<S>
where
    S: Service<ObjectRpcRequest>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: ObjectRpcRequest) -> Self::Future {
        self.inner.call(request)
    }
}
