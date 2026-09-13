//! Host-agnostic service implementation for the cluster build service.
//!
//! Building is deliberately isolated from rollout activation: this service
//! receives immutable source, runs one queued compiler worker, and publishes a
//! verified content-addressed artifact set. It never stages or installs a
//! rollout binary and never mutates the native-module loader state.

use crate::{
    BUILD_ARTIFACT_ROUTE, BUILD_CAPABILITIES_ROUTE, BUILD_OBJECT_RPC_ROUTES, BUILD_PROTOCOL,
    BUILD_STATUS_ROUTE, BUILD_SUBMIT_ROUTE, BuildArtifactKind, BuildArtifactRequest,
    BuildArtifactResponse, BuildArtifactSetManifest, BuildCapabilitiesRequest,
    BuildCapabilitiesResponse, BuildJobId, BuildJobState, BuildJobStatus, BuildKey,
    BuildPlacementRequirements, BuildPlan, BuildServiceCapabilities, BuildStatusRequest,
    BuildStatusResponse, BuildSubmitRequest, BuildSubmitResponse, BuildTargetCapability,
    CargoBuildExecutor,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ObjectRpcBodySource, ObjectRpcIncomingBody, ObjectRpcRequest, ObjectRpcResponse,
    read_object_file, write_object_file,
};
use sinomo_core::node::NodeId;
use sinomo_core::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRoute,
};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const BUILD_READ_CONCURRENCY: usize = 16;
const BUILD_TRANSFER_CONCURRENCY: usize = 4;
const BUILD_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const BUILD_READ_TIMEOUT: Duration = Duration::from_secs(30);

pub struct BuildService {
    hosts: Mutex<BTreeMap<PathBuf, Arc<BuildQueueHost>>>,
}

impl BuildService {
    pub fn new() -> Self {
        Self {
            hosts: Mutex::new(BTreeMap::new()),
        }
    }

    /// Creates a build service and eagerly recovers the queue for one host.
    /// Daemon-style long-lived hosts use this constructor so interrupted jobs
    /// resume at startup rather than waiting for the first request.
    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        let data_dir_root = data_dir.as_ref().to_path_buf();
        let mut hosts = BTreeMap::new();
        hosts.insert(data_dir_root.clone(), BuildQueueHost::new(data_dir_root));
        Self {
            hosts: Mutex::new(hosts),
        }
    }

    fn host_for(&self, data_dir: &SinomoDataDir) -> Arc<BuildQueueHost> {
        let data_dir_root = data_dir.root().to_path_buf();
        let mut hosts = self
            .hosts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            hosts
                .entry(data_dir_root.clone())
                .or_insert_with(|| BuildQueueHost::new(data_dir_root)),
        )
    }
}

impl Default for BuildService {
    fn default() -> Self {
        Self::new()
    }
}

/// Registers the build module's runtime service with an arbitrary service host.
///
/// Keeping this composition edge in `sinomo-build` lets daemon, embedded, and
/// test hosts use the same implementation without teaching the build module
/// about daemon paths or process state.
pub fn register_services(
    builder: sinomo_core::service::ServiceRegistryBuilder,
) -> sinomo_core::service::ServiceRegistryBuilder {
    builder.register(BuildService::new())
}

/// Registers a build service that eagerly recovers one long-lived host's queue.
pub fn register_services_for_data_dir(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    data_dir: &Path,
) -> sinomo_core::service::ServiceRegistryBuilder {
    builder.register(BuildService::for_data_dir(data_dir))
}

/// Built-in component factory for the build service. The component owns its
/// recovered queue and registry fragment; the daemon supplies only the generic
/// data-directory context.
pub fn component_factory()
-> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor =
        sinomo_module::component_descriptor_from_registration(&crate::build_module_registration())?;
    Ok(
        sinomo_module::StaticComponentFactory::new(descriptor, |context, scope| {
            let data_dir_root = context.data_dir_root();
            scope.set_services(
                register_services_for_data_dir(
                    sinomo_core::service::ServiceRegistry::builder(),
                    &data_dir_root,
                )
                .build(),
            );
            Ok(())
        })?
        .with_storage_paths(crate::register_storage_paths),
    )
}

impl Service for BuildService {
    fn name(&self) -> &'static str {
        "build"
    }

    fn routes(&self) -> &'static [&'static str] {
        BUILD_OBJECT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        let surfaces = crate::build_module_surfaces();
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                let descriptor = surfaces
                    .object_rpc_route_descriptor(route)
                    .expect("build module surfaces should be valid")
                    .unwrap_or_else(|| panic!("unknown build service route {route}"));
                let service_route = ServiceRoute::from_route_metadata(
                    descriptor.route,
                    descriptor.category,
                    descriptor.auth_scope,
                    descriptor
                        .required_capabilities
                        .into_iter()
                        .map(|capability| capability.to_string()),
                );
                match route {
                    BUILD_SUBMIT_ROUTE => service_route
                        .with_request_body_mode(ServiceBodyMode::Optional)
                        .with_concurrency_limit(BUILD_TRANSFER_CONCURRENCY)
                        .with_timeout(BUILD_TRANSFER_TIMEOUT),
                    BUILD_ARTIFACT_ROUTE => service_route
                        .with_response_body_mode(ServiceBodyMode::Required)
                        .with_concurrency_limit(BUILD_TRANSFER_CONCURRENCY)
                        .with_timeout(BUILD_TRANSFER_TIMEOUT),
                    _ => service_route
                        .with_concurrency_limit(BUILD_READ_CONCURRENCY)
                        .with_timeout(BUILD_READ_TIMEOUT),
                }
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = self.host_for(&ctx.data_dir);
        Box::pin(async move {
            match request.route.as_str() {
                BUILD_CAPABILITIES_ROUTE => {
                    let payload: BuildCapabilitiesRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = host.capabilities(payload)?;
                    ctx.respond_value(BUILD_CAPABILITIES_ROUTE, response)?;
                }
                BUILD_SUBMIT_ROUTE => {
                    let payload: BuildSubmitRequest = ctx.decode_request_body(&request).await?;
                    let mut body = ctx.take_request_body();
                    let response = host.submit(payload, &mut body).await?;
                    ctx.respond_value(BUILD_SUBMIT_ROUTE, response)?;
                }
                BUILD_STATUS_ROUTE => {
                    let payload: BuildStatusRequest = ctx.decode_request_body(&request).await?;
                    let response = host.status(&payload)?;
                    ctx.respond_value(BUILD_STATUS_ROUTE, response)?;
                }
                crate::BUILD_LOG_ROUTE => {
                    let payload: crate::BuildLogRequest = ctx.decode_request_body(&request).await?;
                    let status = host.read_status(&payload.job_id)?;
                    let response = crate::log::read_build_job_log(&host.paths, &payload, &status)
                        .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(crate::BUILD_LOG_ROUTE, response)?;
                }
                BUILD_ARTIFACT_ROUTE => {
                    let payload: BuildArtifactRequest = ctx.decode_request_body(&request).await?;
                    let (response, body) = host.artifact(&payload)?;
                    ctx.respond_with_body(
                        ObjectRpcResponse::encode_body(BUILD_ARTIFACT_ROUTE, &response)?,
                        body,
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

struct QueuedBuildJob {
    job_id: BuildJobId,
    plan: BuildPlan,
}

struct BuildQueueHost {
    paths: crate::BuildPaths,
    node_id: NodeId,
    target_capabilities: Vec<BuildTargetCapability>,
    cargo_program: PathBuf,
    executor: Option<CargoBuildExecutor>,
    sender: mpsc::Sender<QueuedBuildJob>,
    inflight: Mutex<BTreeMap<BuildKey, BuildJobId>>,
    queued: Mutex<VecDeque<BuildJobId>>,
    queue_depth: AtomicU32,
    worker_active: AtomicBool,
    initialization_error: Option<String>,
}

impl BuildQueueHost {
    fn new(data_dir_root: PathBuf) -> Arc<Self> {
        let paths = crate::BuildPaths::new(&data_dir_root);
        let layout_error = paths.ensure_layout().err().map(|error| error.to_string());
        let cargo_program = std::env::var_os("CARGO")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("cargo"));
        let executor = configured_build_executor(&cargo_program, &paths);
        let executor_error = executor.as_ref().err().cloned();
        let executor = executor.ok();
        let initialization_error = layout_error.or(executor_error);
        let node_id = local_build_node_id(&data_dir_root);
        let target_capabilities = executor
            .as_ref()
            .map(|executor| executor.supported_targets().to_vec())
            .unwrap_or_default();
        let (sender, receiver) = mpsc::channel();
        let host = Arc::new(Self {
            paths,
            node_id,
            target_capabilities,
            cargo_program,
            executor,
            sender,
            inflight: Mutex::new(BTreeMap::new()),
            queued: Mutex::new(VecDeque::new()),
            queue_depth: AtomicU32::new(0),
            worker_active: AtomicBool::new(false),
            initialization_error,
        });
        if host.initialization_error.is_none() {
            host.recover_jobs();
            spawn_build_worker(Arc::clone(&host), receiver);
        }
        host
    }

    fn ensure_initialized(&self) -> Result<(), ServiceError> {
        match &self.initialization_error {
            Some(detail) => Err(ServiceError::Service(format!(
                "build service storage is unavailable: {detail}"
            ))),
            None => Ok(()),
        }
    }

    fn capabilities(
        &self,
        request: BuildCapabilitiesRequest,
    ) -> Result<BuildCapabilitiesResponse, ServiceError> {
        self.ensure_initialized()?;
        let resources = crate::sample_build_resources(
            &self.paths,
            u16::from(self.worker_active.load(Ordering::Relaxed)),
            1,
            self.queue_depth.load(Ordering::Relaxed),
        );
        let capabilities = BuildServiceCapabilities {
            node_id: self.node_id.clone(),
            protocol: BUILD_PROTOCOL.to_string(),
            targets: self.target_capabilities.clone(),
            resources,
        };
        let cache_hit = request.build_key.as_ref().map(|key| {
            self.cached_manifest(key, false)
                .is_ok_and(|manifest| manifest.is_some())
        });
        let supports_requested_target = request.target.as_ref().map(|target| {
            capabilities.supports(target, &request.artifact_kinds)
                && (cache_hit == Some(true)
                    || BuildPlacementRequirements::for_source_bytes(0)
                        .is_satisfied_by(&capabilities.resources))
        });
        Ok(BuildCapabilitiesResponse {
            capabilities,
            supports_requested_target,
            cache_hit,
        })
    }

    async fn submit(
        &self,
        request: BuildSubmitRequest,
        body: &mut ObjectRpcIncomingBody,
    ) -> Result<BuildSubmitResponse, ServiceError> {
        self.ensure_initialized()?;
        request.plan.verify_key().map_err(build_service_error)?;
        if request.plan.source.build_key_source_hash() != request.plan.input.source_hash {
            return Err(ServiceError::Service(
                "build source identity does not match the source hash in the build key input"
                    .to_string(),
            ));
        }
        if request.plan.input.target.rust_target.is_none() {
            return Err(ServiceError::Service(
                "build plan target must include a concrete Rust target triple".to_string(),
            ));
        }
        if request.plan.input.outputs.is_empty() {
            return Err(ServiceError::Service(
                "build plan must request at least one output".to_string(),
            ));
        }
        self.validate_plan_support(&request.plan)?;

        if let Some(manifest) = self.cached_manifest(&request.plan.key, true)? {
            let job_id = BuildJobId::generate();
            let now = OffsetDateTime::now_utc();
            let status = BuildJobStatus {
                job_id: job_id.clone(),
                key: request.plan.key.clone(),
                state: BuildJobState::Succeeded,
                queue_position: None,
                detail: "exact artifact-set cache hit".to_string(),
                artifact_set: Some(manifest),
                created_at: now,
                started_at: Some(now),
                finished_at: Some(now),
            };
            self.persist_plan(&job_id, &request.plan)?;
            self.persist_status(&status)?;
            return Ok(BuildSubmitResponse {
                job_id,
                state: BuildJobState::Succeeded,
                cache_hit: true,
            });
        }
        crate::BuildExecutionPolicy::for_source_bytes(
            &crate::sample_build_resources(
                &self.paths,
                u16::from(self.worker_active.load(Ordering::Relaxed)),
                1,
                self.queue_depth.load(Ordering::Relaxed),
            ),
            request.plan.source.bundle_bytes,
        )
        .map_err(build_service_error)?;

        let job_id = BuildJobId::generate();
        let now = OffsetDateTime::now_utc();
        let queue_position = self.queue_depth.load(Ordering::Relaxed).saturating_add(1);
        let mut status = BuildJobStatus {
            job_id: job_id.clone(),
            key: request.plan.key.clone(),
            state: BuildJobState::Queued,
            queue_position: Some(queue_position),
            detail: "receiving verified source bundle".to_string(),
            artifact_set: None,
            created_at: now,
            started_at: None,
            finished_at: None,
        };
        {
            // Reservation, durable plan, and initial status are serialized by
            // the same key map lock. A concurrent submit can therefore only
            // observe a job after its pollable record exists.
            let mut inflight = self.inflight.lock().map_err(|_| {
                ServiceError::Service("build inflight lock is poisoned".to_string())
            })?;
            if let Some(existing_job_id) = inflight.get(&request.plan.key).cloned() {
                let existing = self.read_status(&existing_job_id)?;
                return Ok(BuildSubmitResponse {
                    job_id: existing_job_id,
                    state: existing.state,
                    cache_hit: false,
                });
            }
            self.persist_plan(&job_id, &request.plan)?;
            self.persist_status(&status)?;
            inflight.insert(request.plan.key.clone(), job_id.clone());
        }
        let mut reservation = InflightReservation::new(self, request.plan.key.clone());

        if body.is_empty() {
            status.state = BuildJobState::Failed;
            status.queue_position = None;
            status.detail = "artifact cache miss requires a source bundle body".to_string();
            status.finished_at = Some(OffsetDateTime::now_utc());
            self.persist_status(&status)?;
            self.clear_inflight(&request.plan.key);
            return Err(ServiceError::Service(status.detail));
        }
        if let Err(error) = self
            .receive_source_bundle(&job_id, &request.plan, body)
            .await
        {
            status.state = BuildJobState::Failed;
            status.queue_position = None;
            status.detail = error.to_string();
            status.finished_at = Some(OffsetDateTime::now_utc());
            let _ = self.persist_status(&status);
            self.clear_inflight(&request.plan.key);
            return Err(error);
        }

        status.detail = "queued for the build worker".to_string();
        self.persist_status(&status)?;
        {
            let mut queued = self
                .queued
                .lock()
                .map_err(|_| ServiceError::Service("build queue lock is poisoned".to_string()))?;
            queued.push_back(job_id.clone());
        }
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        if self
            .sender
            .send(QueuedBuildJob {
                job_id: job_id.clone(),
                plan: request.plan,
            })
            .is_err()
        {
            self.queue_depth.fetch_sub(1, Ordering::Relaxed);
            self.remove_queued(&job_id);
            status.state = BuildJobState::Failed;
            status.queue_position = None;
            status.detail = "build worker is unavailable".to_string();
            status.finished_at = Some(OffsetDateTime::now_utc());
            self.persist_status(&status)?;
            self.clear_inflight(&status.key);
            return Err(ServiceError::Service(status.detail));
        }
        reservation.disarm();
        Ok(BuildSubmitResponse {
            job_id,
            state: BuildJobState::Queued,
            cache_hit: false,
        })
    }

    fn status(&self, request: &BuildStatusRequest) -> Result<BuildStatusResponse, ServiceError> {
        self.ensure_initialized()?;
        let mut status = self.read_status(&request.job_id)?;
        if status.state == BuildJobState::Queued {
            status.queue_position = self.queue_position(&request.job_id);
        }
        Ok(BuildStatusResponse { status })
    }

    fn artifact(
        &self,
        request: &BuildArtifactRequest,
    ) -> Result<(BuildArtifactResponse, ObjectRpcBodySource), ServiceError> {
        self.ensure_initialized()?;
        let manifest = self
            .cached_manifest(&request.build_key, false)?
            .ok_or_else(|| {
                ServiceError::Service(format!(
                    "build artifact set {} is not cached",
                    request.build_key
                ))
            })?;
        let artifact = manifest
            .artifact(&request.artifact_id)
            .cloned()
            .ok_or_else(|| {
                ServiceError::Service(format!(
                    "build artifact set {} has no artifact {}",
                    request.build_key, request.artifact_id
                ))
            })?;
        let path = self
            .paths
            .cache_artifacts_dir(&request.build_key)
            .join(&artifact.relative_path);
        crate::verify_artifact_file(&artifact, &path).map_err(build_service_error)?;
        let body = ObjectRpcBodySource::file(path).map_err(ServiceError::from)?;
        Ok((BuildArtifactResponse { artifact }, body))
    }

    fn validate_plan_support(&self, plan: &BuildPlan) -> Result<(), ServiceError> {
        let kinds = plan.artifact_kinds();
        if self.target_capabilities.iter().any(|capability| {
            capability.target == plan.input.target
                && capability.toolchain_fingerprint == plan.input.toolchain_fingerprint
                && capability.supports(&plan.input.target, &kinds)
        }) {
            return Ok(());
        }
        Err(ServiceError::Service(format!(
            "build target {}/{} ({}) is not supported by this server",
            plan.input.target.os,
            plan.input.target.arch,
            plan.input.target.path_key()
        )))
    }

    fn cached_manifest(
        &self,
        key: &BuildKey,
        verify_content: bool,
    ) -> Result<Option<BuildArtifactSetManifest>, ServiceError> {
        crate::manager::cached_build_manifest(&self.paths, key, verify_content)
            .map_err(build_service_error)
    }

    fn clear_inflight(&self, key: &BuildKey) {
        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.remove(key);
        }
    }

    fn queue_position(&self, job_id: &BuildJobId) -> Option<u32> {
        self.queued.lock().ok().and_then(|queued| {
            queued
                .iter()
                .position(|candidate| candidate == job_id)
                .map(|index| u32::try_from(index + 1).unwrap_or(u32::MAX))
        })
    }

    fn remove_queued(&self, job_id: &BuildJobId) {
        if let Ok(mut queued) = self.queued.lock()
            && let Some(index) = queued.iter().position(|candidate| candidate == job_id)
        {
            queued.remove(index);
        }
    }

    fn persist_plan(&self, job_id: &BuildJobId, plan: &BuildPlan) -> Result<(), ServiceError> {
        let path = self.paths.job_plan(job_id);
        write_object_file(&path, plan).map_err(ServiceError::from)
    }

    fn read_plan(&self, job_id: &BuildJobId) -> Result<BuildPlan, ServiceError> {
        read_object_file(&self.paths.job_plan(job_id)).map_err(ServiceError::from)
    }

    fn persist_status(&self, status: &BuildJobStatus) -> Result<(), ServiceError> {
        write_object_file(&self.paths.job_status(&status.job_id), status)
            .map_err(ServiceError::from)
    }

    fn read_status(&self, job_id: &BuildJobId) -> Result<BuildJobStatus, ServiceError> {
        read_object_file(&self.paths.job_status(job_id)).map_err(ServiceError::from)
    }

    async fn receive_source_bundle(
        &self,
        job_id: &BuildJobId,
        plan: &BuildPlan,
        body: &mut ObjectRpcIncomingBody,
    ) -> Result<(), ServiceError> {
        let destination = self.paths.job_source_bundle(job_id);
        let parent = destination.parent().ok_or_else(|| {
            ServiceError::Service(format!(
                "build source path {} has no parent",
                destination.display()
            ))
        })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| build_io_error(parent, source))?;
        let partial = destination.with_extension("partial");
        let mut output = tokio::fs::File::create(&partial)
            .await
            .map_err(|source| build_io_error(&partial, source))?;
        let mut hasher = blake3::Hasher::new();
        let mut received = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let count = body.read(&mut buffer).await.map_err(|source| {
                ServiceError::Service(format!("failed to receive build source bundle: {source}"))
            })?;
            if count == 0 {
                break;
            }
            received = received.saturating_add(count as u64);
            if received > plan.source.bundle_bytes {
                let _ = tokio::fs::remove_file(&partial).await;
                return Err(ServiceError::Service(format!(
                    "build source bundle exceeded declared size {}",
                    plan.source.bundle_bytes
                )));
            }
            hasher.update(&buffer[..count]);
            output
                .write_all(&buffer[..count])
                .await
                .map_err(|source| build_io_error(&partial, source))?;
        }
        if received != plan.source.bundle_bytes {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(ServiceError::Service(format!(
                "build source bundle size mismatch: expected {}, received {received}",
                plan.source.bundle_bytes
            )));
        }
        let actual_hash = format!("blake3:{}", hasher.finalize().to_hex());
        if actual_hash != plan.source.bundle_hash {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(ServiceError::Service(format!(
                "build source bundle hash mismatch: expected {}, got {actual_hash}",
                plan.source.bundle_hash
            )));
        }
        output
            .flush()
            .await
            .map_err(|source| build_io_error(&partial, source))?;
        output
            .sync_all()
            .await
            .map_err(|source| build_io_error(&partial, source))?;
        drop(output);
        tokio::fs::rename(&partial, &destination)
            .await
            .map_err(|source| build_io_error(&destination, source))?;
        Ok(())
    }

    fn recover_jobs(self: &Arc<Self>) {
        let Ok(entries) = fs::read_dir(&self.paths.jobs) else {
            return;
        };
        let mut recovered = Vec::new();
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let Some(job_id) = entry
                .file_name()
                .to_str()
                .and_then(|value| BuildJobId::new(value.to_string()).ok())
            else {
                continue;
            };
            let Ok(mut status) = self.read_status(&job_id) else {
                continue;
            };
            if !matches!(status.state, BuildJobState::Queued | BuildJobState::Running) {
                continue;
            }
            let Ok(plan) = self.read_plan(&job_id) else {
                status.state = BuildJobState::Failed;
                status.queue_position = None;
                status.detail =
                    "daemon restarted but the durable build plan is missing".to_string();
                status.finished_at = Some(OffsetDateTime::now_utc());
                let _ = self.persist_status(&status);
                continue;
            };
            if !self.paths.job_source_bundle(&job_id).is_file() {
                status.state = BuildJobState::Failed;
                status.queue_position = None;
                status.detail =
                    "daemon restarted but the durable source bundle is missing".to_string();
                status.finished_at = Some(OffsetDateTime::now_utc());
                let _ = self.persist_status(&status);
                continue;
            }
            status.state = BuildJobState::Queued;
            status.started_at = None;
            status.finished_at = None;
            status.detail = "recovered after daemon restart".to_string();
            let _ = self.persist_status(&status);
            recovered.push(QueuedBuildJob { job_id, plan });
        }
        for job in recovered {
            let reserved = self
                .inflight
                .lock()
                .ok()
                .map(|mut inflight| {
                    if inflight.contains_key(&job.plan.key) {
                        false
                    } else {
                        inflight.insert(job.plan.key.clone(), job.job_id.clone());
                        true
                    }
                })
                .unwrap_or(false);
            if !reserved {
                if let Ok(mut status) = self.read_status(&job.job_id) {
                    status.state = BuildJobState::Failed;
                    status.queue_position = None;
                    status.detail =
                        "duplicate same-key job discarded during daemon recovery".to_string();
                    status.finished_at = Some(OffsetDateTime::now_utc());
                    let _ = self.persist_status(&status);
                }
                continue;
            }
            if let Ok(mut queued) = self.queued.lock() {
                queued.push_back(job.job_id.clone());
            }
            self.queue_depth.fetch_add(1, Ordering::Relaxed);
            let _ = self.sender.send(job);
        }
    }
}

struct InflightReservation<'a> {
    host: &'a BuildQueueHost,
    key: BuildKey,
    armed: bool,
}

impl<'a> InflightReservation<'a> {
    fn new(host: &'a BuildQueueHost, key: BuildKey) -> Self {
        Self {
            host,
            key,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InflightReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.host.clear_inflight(&self.key);
        }
    }
}

fn spawn_build_worker(host: Arc<BuildQueueHost>, receiver: mpsc::Receiver<QueuedBuildJob>) {
    let host = Arc::downgrade(&host);
    let _ = std::thread::Builder::new()
        .name("sinomo-build-worker".to_string())
        .spawn(move || {
            while let Ok(job) = receiver.recv() {
                let Some(host) = host.upgrade() else {
                    break;
                };
                host.queue_depth.fetch_sub(1, Ordering::Relaxed);
                host.remove_queued(&job.job_id);
                host.worker_active.store(true, Ordering::Relaxed);
                run_queued_build(&host, job);
                host.worker_active.store(false, Ordering::Relaxed);
            }
        });
}

fn run_queued_build(host: &BuildQueueHost, job: QueuedBuildJob) {
    let mut status = match host.read_status(&job.job_id) {
        Ok(status) => status,
        Err(error) => {
            tracing::error!(job_id = %job.job_id, "could not load queued build status: {error}");
            host.clear_inflight(&job.plan.key);
            return;
        }
    };
    status.state = BuildJobState::Running;
    status.queue_position = None;
    status.detail = "compiler worker running".to_string();
    status.started_at = Some(OffsetDateTime::now_utc());
    status.finished_at = None;
    if let Err(error) = host.persist_status(&status) {
        tracing::error!(job_id = %job.job_id, "could not persist running build status: {error}");
    }

    let Some(executor) = host.executor.as_ref() else {
        status.state = BuildJobState::Failed;
        status.detail = "build executor is unavailable".to_string();
        status.finished_at = Some(OffsetDateTime::now_utc());
        let _ = host.persist_status(&status);
        host.clear_inflight(&job.plan.key);
        return;
    };
    match crate::manager::execute_and_cache_build(
        &host.paths,
        &job.job_id,
        &job.plan,
        executor,
        &host.cargo_program,
    ) {
        Ok(manifest) => {
            status.state = BuildJobState::Succeeded;
            status.detail = "verified artifact set cached".to_string();
            status.artifact_set = Some(manifest);
            status.finished_at = Some(OffsetDateTime::now_utc());
            let _ = host.persist_status(&status);
            let _ = fs::remove_dir_all(host.paths.job_work_dir(&job.job_id));
        }
        Err(error) => {
            status.state = BuildJobState::Failed;
            status.detail = error.to_string();
            status.artifact_set = None;
            status.finished_at = Some(OffsetDateTime::now_utc());
            let _ = host.persist_status(&status);
        }
    }
    host.clear_inflight(&job.plan.key);
}

pub(crate) fn configured_build_executor(
    cargo_program: &Path,
    paths: &crate::BuildPaths,
) -> Result<CargoBuildExecutor, String> {
    let rustc_program = std::env::var_os("RUSTC")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rustc"));
    let configured_targets = std::env::var_os("SINOMO_BUILD_TARGETS");
    build_executor_for_targets(
        cargo_program,
        &rustc_program,
        &paths.aarch64_gnu_cross_bin(),
        configured_targets
            .as_deref()
            .map(|value| value.to_string_lossy()),
    )
}

const AARCH64_LINUX_GNU_TARGET: &str = "aarch64-unknown-linux-gnu";
const AARCH64_GNU_CROSS_BIN_ENV: &str = "SINOMO_BUILD_AARCH64_GNU_BIN";

fn command_version(program: &Path) -> Result<String, String> {
    let output = crate::process::noninteractive_command(program)
        .arg("--version")
        .output()
        .map_err(|error| format!("run {} --version: {error}", program.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} --version failed: {}",
            program.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn aarch64_gnu_cross_bin(default_bin: &Path) -> PathBuf {
    if let Some(configured) = std::env::var_os(AARCH64_GNU_CROSS_BIN_ENV) {
        return PathBuf::from(configured);
    }
    default_bin.to_path_buf()
}

fn discover_aarch64_gnu_cross_environment(
    host_target: &str,
    base_toolchain_fingerprint: &str,
    default_bin: &Path,
) -> Result<Option<(String, Vec<(OsString, OsString)>)>, String> {
    if host_target != "x86_64-unknown-linux-gnu" {
        return Ok(None);
    }
    let bin = aarch64_gnu_cross_bin(default_bin);
    let gcc = bin.join("aarch64-linux-gnu-gcc");
    let gxx = bin.join("aarch64-linux-gnu-g++");
    let ar = bin.join("aarch64-linux-gnu-ar");
    let ranlib = bin.join("aarch64-linux-gnu-ranlib");
    if ![&gcc, &gxx, &ar, &ranlib]
        .into_iter()
        .all(|program| program.is_file())
    {
        return Ok(None);
    }
    let gcc_version = command_version(&gcc)?;
    let gxx_version = command_version(&gxx)?;
    let ar_version = command_version(&ar)?;
    let ranlib_version = command_version(&ranlib)?;
    let sysroot_output = crate::process::noninteractive_command(&gcc)
        .arg("-print-sysroot")
        .output()
        .map_err(|error| format!("inspect {} sysroot: {error}", gcc.display()))?;
    if !sysroot_output.status.success() {
        return Err(format!("{} could not report its sysroot", gcc.display()));
    }
    let sysroot = PathBuf::from(String::from_utf8_lossy(&sysroot_output.stdout).trim());
    let crt1 = sysroot.join("lib/crt1.o");
    let libc_linker_script = sysroot.join("lib/libc.so");
    let libc = sysroot.join("lib/libc.so.6");
    for required in [&crt1, &libc_linker_script, &libc] {
        if !required.is_file() {
            return Err(format!(
                "AArch64 GNU cross sysroot is incomplete: {} is missing",
                required.display()
            ));
        }
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(base_toolchain_fingerprint.as_bytes());
    hasher.update(AARCH64_LINUX_GNU_TARGET.as_bytes());
    hasher.update(gcc_version.as_bytes());
    hasher.update(gxx_version.as_bytes());
    hasher.update(ar_version.as_bytes());
    hasher.update(ranlib_version.as_bytes());
    for required in [&crt1, &libc_linker_script, &libc] {
        let bytes = fs::read(required)
            .map_err(|error| format!("fingerprint {}: {error}", required.display()))?;
        hasher.update(&bytes);
    }
    let fingerprint = format!(
        "cargo-cross-toolchain-blake3:{}",
        hasher.finalize().to_hex()
    );
    let environment = vec![
        (
            OsString::from("CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER"),
            gcc.as_os_str().to_os_string(),
        ),
        (
            OsString::from("CC_aarch64_unknown_linux_gnu"),
            gcc.as_os_str().to_os_string(),
        ),
        (
            OsString::from("CXX_aarch64_unknown_linux_gnu"),
            gxx.as_os_str().to_os_string(),
        ),
        (
            OsString::from("AR_aarch64_unknown_linux_gnu"),
            ar.as_os_str().to_os_string(),
        ),
        (
            OsString::from("RANLIB_aarch64_unknown_linux_gnu"),
            ranlib.as_os_str().to_os_string(),
        ),
    ];
    Ok(Some((fingerprint, environment)))
}

fn build_executor_for_targets(
    cargo_program: &Path,
    rustc_program: &Path,
    aarch64_gnu_cross_bin: &Path,
    configured_targets: Option<std::borrow::Cow<'_, str>>,
) -> Result<CargoBuildExecutor, String> {
    let toolchain = crate::inspect_cargo_toolchain(rustc_program, cargo_program)
        .map_err(|error| format!("inspect build toolchain: {error}"))?;
    let mut triples = vec![toolchain.host_target.clone()];
    if let Some(configured) = configured_targets {
        triples.extend(
            crate::parse_rust_target_list(&configured)
                .map_err(|error| format!("parse SINOMO_BUILD_TARGETS: {error}"))?,
        );
    }
    let mut aarch64_cross = discover_aarch64_gnu_cross_environment(
        &toolchain.host_target,
        &toolchain.fingerprint,
        aarch64_gnu_cross_bin,
    )
    .unwrap_or_else(|error| {
        tracing::warn!(%error, "ignoring unusable optional AArch64 GNU cross toolchain");
        None
    });
    if aarch64_cross.is_some() {
        match crate::inspect_rust_target(rustc_program, AARCH64_LINUX_GNU_TARGET) {
            Ok(_) => triples.push(AARCH64_LINUX_GNU_TARGET.to_string()),
            Err(error) => {
                tracing::warn!(%error, "ignoring AArch64 GNU cross toolchain without Rust target std");
                aarch64_cross = None;
            }
        }
    }
    triples.sort();
    triples.dedup();
    let capabilities = triples
        .into_iter()
        .map(|triple| {
            let target = crate::inspect_rust_target(rustc_program, &triple)
                .map_err(|error| format!("inspect configured build target {triple}: {error}"))?;
            let toolchain_fingerprint = if triple == AARCH64_LINUX_GNU_TARGET {
                aarch64_cross
                    .as_ref()
                    .map(|(fingerprint, _)| fingerprint.clone())
                    .unwrap_or_else(|| toolchain.fingerprint.clone())
            } else {
                toolchain.fingerprint.clone()
            };
            Ok(BuildTargetCapability {
                target,
                artifact_kinds: vec![
                    BuildArtifactKind::Binary,
                    BuildArtifactKind::NativeDylibModule,
                ],
                toolchain_fingerprint,
                supports_source_identity_hash: true,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut executor = CargoBuildExecutor::new(cargo_program, capabilities)
        .map_err(|error| format!("configure Cargo build executor: {error}"))?;
    if let Some((fingerprint, environment)) = aarch64_cross
        && let Some(capability) = executor
            .supported_targets()
            .iter()
            .find(|capability| {
                capability.target.rust_target.as_deref() == Some(AARCH64_LINUX_GNU_TARGET)
            })
            .cloned()
    {
        executor = executor
            .with_target_environment(capability.target, fingerprint, environment)
            .map_err(|error| format!("configure AArch64 GNU cross environment: {error}"))?;
    }
    Ok(executor)
}

fn local_build_node_id(data_dir_root: &Path) -> NodeId {
    let data_dir = SinomoDataDir::new(data_dir_root);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map(|state| state.local_node_id)
        .unwrap_or_else(|_| {
            NodeId::derived(
                "sinomo-build-node",
                data_dir_root.as_os_str().as_encoded_bytes(),
            )
        })
}

fn build_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}

fn build_io_error(path: &Path, source: std::io::Error) -> ServiceError {
    ServiceError::Service(format!("build I/O error at {}: {source}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_command_succeeded(output: std::process::Output, operation: &str) {
        assert!(
            output.status.success(),
            "{operation} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn build_service_declares_stream_modes_and_worker_safe_concurrency() {
        let service = BuildService::new();
        let routes = service
            .route_categories()
            .into_iter()
            .map(|route| (route.route.clone(), route))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            routes[BUILD_SUBMIT_ROUTE].request_body_mode,
            ServiceBodyMode::Optional
        );
        assert_eq!(
            routes[BUILD_ARTIFACT_ROUTE].response_body_mode,
            ServiceBodyMode::Required
        );
        assert_eq!(
            routes[BUILD_SUBMIT_ROUTE].concurrency_limit,
            Some(BUILD_TRANSFER_CONCURRENCY)
        );
    }

    #[test]
    fn build_service_executor_advertises_concrete_host_target() {
        let root = sinomo_testdir::TestDir::new("sinomo-build-capability-paths");
        let paths = crate::BuildPaths::new(&root);
        let executor = build_executor_for_targets(
            Path::new("cargo"),
            Path::new("rustc"),
            &paths.aarch64_gnu_cross_bin(),
            None,
        )
        .expect("executor");
        assert!(!executor.supported_targets().is_empty());
        assert!(
            executor
                .supported_targets()
                .iter()
                .all(|capability| capability.target.rust_target.is_some())
        );
        assert!(
            executor
                .supported_targets()
                .iter()
                .all(|capability| capability.supports_source_identity_hash)
        );
        let toolchain = crate::inspect_cargo_toolchain("rustc", "cargo").expect("toolchain");
        let discovered = discover_aarch64_gnu_cross_environment(
            &toolchain.host_target,
            &toolchain.fingerprint,
            &paths.aarch64_gnu_cross_bin(),
        )
        .expect("cross-toolchain discovery");
        let advertised = executor.supported_targets().iter().find(|capability| {
            capability.target.rust_target.as_deref() == Some(AARCH64_LINUX_GNU_TARGET)
        });
        assert_eq!(advertised.is_some(), discovered.is_some());
        if let (Some(advertised), Some((fingerprint, _))) = (advertised, discovered) {
            assert_eq!(advertised.toolchain_fingerprint, fingerprint);
        }
    }

    #[test]
    fn inflight_reservation_clears_key_when_post_reservation_work_fails() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-build-inflight-reservation");
        let host = BuildQueueHost::new(root.to_path_buf());
        let key = BuildKey::new(format!("blake3:{}", "a".repeat(64))).expect("build key");
        host.inflight
            .lock()
            .expect("inflight lock")
            .insert(key.clone(), BuildJobId::generate());

        {
            let _reservation = InflightReservation::new(host.as_ref(), key.clone());
            // Returning an error from any operation after reservation drops
            // this guard before the service future completes.
        }

        assert!(
            !host
                .inflight
                .lock()
                .expect("inflight lock")
                .contains_key(&key)
        );
    }

    #[test]
    fn build_source_bundle_is_materialized_without_rollout_state_or_remotes() {
        let root = sinomo_testdir::TestDir::new("sinomo-build-source-bundle");
        let source = root.join("source-repo");
        let bundle = root.join("source.bundle");
        fs::create_dir_all(&source).expect("source root");
        assert_command_succeeded(
            crate::process::noninteractive_command("git")
                .arg("init")
                .arg(&source)
                .output()
                .expect("git init"),
            "git init",
        );
        for (key, value) in [
            ("user.name", "Sinomo Build Test"),
            ("user.email", "build-test@sinomo.invalid"),
        ] {
            assert_command_succeeded(
                crate::process::noninteractive_command("git")
                    .arg("-C")
                    .arg(&source)
                    .args(["config", key, value])
                    .output()
                    .expect("git config"),
                "git config",
            );
        }
        fs::write(
            source.join("Cargo.toml"),
            b"[workspace]\nresolver = \"3\"\n",
        )
        .expect("write source manifest");
        assert_command_succeeded(
            crate::process::noninteractive_command("git")
                .arg("-C")
                .arg(&source)
                .args(["add", "Cargo.toml"])
                .output()
                .expect("git add"),
            "git add",
        );
        assert_command_succeeded(
            crate::process::noninteractive_command("git")
                .arg("-C")
                .arg(&source)
                .args(["commit", "-m", "build source"])
                .output()
                .expect("git commit"),
            "git commit",
        );
        assert_command_succeeded(
            crate::process::noninteractive_command("git")
                .arg("-C")
                .arg(&source)
                .args(["bundle", "create"])
                .arg(&bundle)
                .arg("HEAD")
                .output()
                .expect("git bundle"),
            "git bundle",
        );

        let materialized =
            crate::manager::materialize_build_source_bundle(&root.join("work"), &bundle)
                .expect("materialize source bundle");
        assert!(materialized.join("Cargo.toml").is_file());
        assert!(
            !crate::build_source_head(&materialized)
                .expect("materialized head")
                .is_empty()
        );
        let remotes = crate::process::noninteractive_command("git")
            .arg("-C")
            .arg(&materialized)
            .arg("remote")
            .output()
            .expect("git remote");
        assert!(
            remotes.status.success(),
            "git remote failed: {}",
            String::from_utf8_lossy(&remotes.stderr)
        );
        assert!(String::from_utf8_lossy(&remotes.stdout).trim().is_empty());
        assert!(!root.join("work").join("rollout").exists());
    }
}
