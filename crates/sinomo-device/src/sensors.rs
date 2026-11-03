//! Bounded sensor observations. Hosts install hardware adapters; this owner
//! checks node-local exposure for every remote read.
use serde::{Deserialize, Serialize};
use sinomo_core::{
    network::ObjectRpcRequest,
    node::LocalNodePermissionStore,
    service::{
        Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
        ServiceRoute,
    },
};
use std::sync::OnceLock;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SensorReading {
    pub sensor: String,
    pub sensor_type: i32,
    pub timestamp_ns: u64,
    pub accuracy: i32,
    pub values: Vec<f32>,
}
pub type SensorReader = fn() -> Result<Vec<SensorReading>, String>;
static READER: OnceLock<SensorReader> = OnceLock::new();
pub fn install_sensor_reader(reader: SensorReader) {
    let _ = READER.set(reader);
}
pub fn read_local_sensors() -> Result<Vec<SensorReading>, String> {
    READER
        .get()
        .ok_or("Sensor capture is unavailable on this platform")?()
}
pub const SENSORS_READ: &str = "/media/sensors/read";
pub struct SensorService;
impl Service for SensorService {
    fn name(&self) -> &'static str {
        "media-sensors"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[SENSORS_READ]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(SENSORS_READ, "media")
                .with_auth_scope("local-or-cluster-member")
                .require_capability("media.sensors.capture")
                .with_concurrency_limit(2)
                .with_request_body_mode(ServiceBodyMode::None)
                .with_response_body_mode(ServiceBodyMode::None),
        ]
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            if request.route != SENSORS_READ {
                return Err(ServiceError::UnknownRoute(request.route));
            }
            let store = LocalNodePermissionStore::new(ctx.data_dir.root());
            require_exposure(&store)?;
            let readings = tokio::task::spawn_blocking(read_local_sensors)
                .await
                .map_err(|e| ServiceError::Service(e.to_string()))?
                .map_err(ServiceError::Service)?;
            require_exposure(&store)?;
            ctx.respond_value(SENSORS_READ, readings)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}
fn require_exposure(store: &LocalNodePermissionStore) -> Result<(), ServiceError> {
    if store.resource_operation_exposed("motion.default", "capture") {
        Ok(())
    } else {
        Err(ServiceError::Service(
            "Sensor sharing is not enabled on this node".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn remote_sensor_requests_do_not_touch_hardware_without_live_consent() {
        use sinomo_core::{
            local::SinomoDataDir,
            network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse},
            service::{ResponseSink, ServiceSigner},
        };
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        static READS: AtomicUsize = AtomicUsize::new(0);
        fn read() -> Result<Vec<SensorReading>, String> {
            READS.fetch_add(1, Ordering::SeqCst);
            Ok(vec![SensorReading {
                sensor: "fixture".into(),
                sensor_type: 1,
                timestamp_ns: 7,
                accuracy: 3,
                values: vec![1.0, 2.0, 3.0],
            }])
        }
        struct Sink(Arc<std::sync::Mutex<Option<ObjectRpcOutgoingResponse>>>);
        impl ResponseSink for Sink {
            fn send(
                self: Box<Self>,
                response: ObjectRpcOutgoingResponse,
            ) -> Result<(), ServiceError> {
                *self.0.lock().unwrap() = Some(response);
                Ok(())
            }
        }
        install_sensor_reader(read);
        let root = sinomo_testdir::TestDir::new("sensor-remote-owner");
        let store = LocalNodePermissionStore::new(root.path());
        let response = Arc::new(std::sync::Mutex::new(None));
        let context = || {
            ServiceContext::new(
                Arc::new(SinomoDataDir::new(root.path())),
                None,
                ServiceSigner::Remote {
                    node_id: "peer".into(),
                    public_key: "peer-key".into(),
                },
                ObjectRpcIncomingBody::default(),
                Box::new(Sink(response.clone())),
            )
        };
        let request = || ObjectRpcRequest::encode_body(SENSORS_READ, "media", &()).unwrap();
        assert!(SensorService.handle(context(), request()).await.is_err());
        assert_eq!(READS.load(Ordering::SeqCst), 0);
        store
            .mutate(|p| p.expose_resource_operation("motion.default", "capture"))
            .unwrap();
        SensorService.handle(context(), request()).await.unwrap();
        assert_eq!(READS.load(Ordering::SeqCst), 1);
        let reply = response.lock().unwrap().take().unwrap();
        let samples: Vec<SensorReading> = reply
            .into_materialized_value_response(4096)
            .await
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(samples[0].values, vec![1.0, 2.0, 3.0]);
        store
            .mutate(|p| p.hide_resource_operation("motion.default", "capture"))
            .unwrap();
        assert!(SensorService.handle(context(), request()).await.is_err());
        assert_eq!(READS.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sensor_read_requires_its_own_live_exposure() {
        let root = sinomo_testdir::TestDir::new("sensor-exposure");
        let store = LocalNodePermissionStore::new(root.path());
        assert!(require_exposure(&store).is_err());
        store
            .mutate(|p| p.expose_resource_operation("microphone.default", "capture"))
            .unwrap();
        assert!(require_exposure(&store).is_err());
        store
            .mutate(|p| p.expose_resource_operation("motion.default", "capture"))
            .unwrap();
        assert!(require_exposure(&store).is_ok());
        store
            .mutate(|p| p.hide_resource_operation("motion.default", "capture"))
            .unwrap();
        assert!(require_exposure(&store).is_err());
    }
}
