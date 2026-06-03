//! Rollout object-RPC services.
//!
//! The diagnostics route is host-agnostic: it reports rollout state from the local
//! data directory and works with no daemon and no cluster. Binary/build activation
//! routes still need daemon-local process ownership and remain daemon adapters.

use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder,
};

use crate::{ROLLOUT_DIAGNOSTICS_ROUTE, SelfRolloutPaths, rollout_diagnostics_report};

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

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            match request.route.as_str() {
                ROLLOUT_DIAGNOSTICS_ROUTE => {
                    let _: () = request.decode_body()?;
                    let rollout_paths = SelfRolloutPaths::new(&ctx.data_dir.root());
                    let report = rollout_diagnostics_report(&rollout_paths)
                        .map_err(|error| ServiceError::Service(error.to_string()))?;
                    let response =
                        ObjectRpcResponse::encode_body(ROLLOUT_DIAGNOSTICS_ROUTE, &report)?;
                    ctx.respond(response)?;
                    Ok(ServiceDisposition::Continue)
                }
                other => Err(ServiceError::UnknownRoute(other.to_string())),
            }
        })
    }
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(RolloutService::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_local::SinomoDataDir;
    use sinomo_service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct VecSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for VecSink {
        fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    #[tokio::test]
    async fn rollout_diagnostics_serves_in_local_mode() {
        let registry = register_services(ServiceRegistry::builder()).build();
        let dir = std::env::temp_dir().join(format!(
            "sinomo-rollout-service-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let data_dir = Arc::new(SinomoDataDir::new(&dir));
        let sink = VecSink::default();
        let frames = Arc::clone(&sink.frames);
        let ctx = ServiceContext {
            data_dir,
            cluster: None,
            signer: ServiceSigner::Local,
            sink: Box::new(sink),
        };
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

        std::fs::remove_dir_all(&dir).ok();
    }
}
