//! Rollout object-RPC services.
//!
//! The diagnostics route is host-agnostic: it reports rollout state from the local
//! data directory and works with no daemon and no cluster. Binary/build activation
//! routes still need daemon-local process ownership and remain daemon adapters.

use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::{
    ROLLOUT_DIAGNOSTICS_ROUTE, SelfRolloutPaths, rollout_diagnostics_report,
    rollout_module_surfaces,
};

const ROLLOUT_ROUTES: &[&str] = &[ROLLOUT_DIAGNOSTICS_ROUTE];

#[derive(Default)]
pub struct RolloutService;

impl RolloutService {
    pub fn new() -> Self {
        Self
    }
}

impl Service for RolloutService {
    fn name(&self) -> &'static str {
        "rollout"
    }

    fn routes(&self) -> &'static [&'static str] {
        ROLLOUT_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![rollout_service_route(ROLLOUT_DIAGNOSTICS_ROUTE)]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            match request.route.as_str() {
                ROLLOUT_DIAGNOSTICS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let rollout_paths = SelfRolloutPaths::new(&ctx.data_dir.root());
                    let report = rollout_diagnostics_report(&rollout_paths)
                        .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(ROLLOUT_DIAGNOSTICS_ROUTE, report)?;
                    Ok(ServiceDisposition::Continue)
                }
                other => Err(ServiceError::UnknownRoute(other.to_string())),
            }
        })
    }
}

fn rollout_service_route(route: &'static str) -> ServiceRoute {
    let descriptor = rollout_module_surfaces()
        .object_rpc_route_descriptor(route)
        .expect("rollout module surfaces should be valid")
        .unwrap_or_else(|| panic!("unknown rollout service route {route}"));
    ServiceRoute::from_route_metadata(
        descriptor.route,
        descriptor.category,
        descriptor.auth_scope,
        descriptor
            .required_capabilities
            .into_iter()
            .map(|capability| capability.to_string()),
    )
    .with_concurrency_limit(8)
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(RolloutService::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse};
    use sinomo_core::service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
            if !response.body.is_empty() {
                return Err(ServiceError::Service(
                    "rollout test sink received an unexpected response body".to_string(),
                ));
            }
            self.frames
                .lock()
                .expect("sink lock")
                .push(response.response);
            Ok(())
        }
    }

    #[test]
    fn rollout_service_route_metadata_matches_module_surfaces() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let service_route = registry
            .route_categories()
            .into_iter()
            .find(|route| route.route == ROLLOUT_DIAGNOSTICS_ROUTE)
            .expect("rollout diagnostics service route");
        let module_route = rollout_module_surfaces()
            .object_rpc_route_descriptors()
            .expect("rollout module surfaces")
            .into_iter()
            .find(|route| route.route == ROLLOUT_DIAGNOSTICS_ROUTE)
            .expect("rollout diagnostics module route");

        assert_eq!(service_route.category, module_route.category);
        assert_eq!(
            service_route.auth_scope.as_deref(),
            Some(module_route.auth_scope.as_str())
        );
        assert_eq!(
            service_route.required_capabilities,
            module_route
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(service_route.concurrency_limit, Some(8));
    }

    #[tokio::test]
    async fn rollout_diagnostics_serves_in_local_mode() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let dir = sinomo_testdir::TestDir::new("sinomo-rollout-service");
        let data_dir = Arc::new(SinomoDataDir::new(&dir));
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext::new(
            data_dir,
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink),
        );
        let request = ObjectRpcRequest::encode_body(
            ROLLOUT_DIAGNOSTICS_ROUTE,
            crate::ROLLOUT_RPC_CATEGORY,
            &(),
        )
        .expect("encode");

        let disposition = registry.dispatch(ctx, request).await.expect("dispatch");
        assert_eq!(disposition, ServiceDisposition::Continue);
        let report: String = frames
            .lock()
            .expect("frames")
            .first()
            .expect("a response frame")
            .decode_body()
            .expect("decode report");
        assert!(report.contains("rollout root:"));
    }
}
