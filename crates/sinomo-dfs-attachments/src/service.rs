//! Module-owned filesystem feature routes and lifecycle.
use crate::*;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use sinomo_dfs::{
    DfsBrowserBackend, DfsBrowserServiceHost, authorize_dfs_core_component_access,
    dfs_browser_component_host, dfs_core_component_requirement, run_blocking_browser_task,
    run_blocking_service_task,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
const DAEMON_RPC_CATEGORY: &str = "daemon";
fn dfs_browser_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}
struct FeatureBackend {
    core: Arc<DfsBrowserBackend>,
    data_dir: SinomoDataDir,
    host: Arc<dyn DfsBrowserServiceHost>,
}
impl FeatureBackend {
    fn for_data_dir(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
        _host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Result<Self, ServiceError> {
        let host = dfs_browser_component_host(&data_dir)?;
        let core = DfsBrowserBackend::for_component_data_dir(Arc::clone(&data_dir), runtime)?;
        Ok(Self {
            core,
            data_dir: (*data_dir).clone(),
            host,
        })
    }
    fn event_origin_node_id(&self) -> Result<sinomo_core::Id, ServiceError> {
        self.core.event_origin_node_id()
    }
}

impl FeatureBackend {
    fn file_attachment(
        &self,
        actor: &str,
        request: DfsAttachmentRequest,
    ) -> Result<DfsAttachmentResponse, ServiceError> {
        let mutation = request.is_mutation();
        let changed_source = request.source().clone();
        let result = self.core.with_mutation_service(|service| {
            let options = if matches!(
                &request,
                DfsAttachmentRequest::ClaimGeneration { .. }
                    | DfsAttachmentRequest::Put { .. }
                    | DfsAttachmentRequest::Delete { .. }
            ) {
                service
                    .posix_stat_distributed(&request.source().path)
                    .map_err(dfs_browser_service_error)?;
                Some(
                    self.core
                        .write_options_for_path(service, &request.source().path)?,
                )
            } else {
                None
            };
            service
                .file_attachment_as(actor, request, options)
                .map_err(dfs_browser_service_error)
        });
        if mutation {
            crate::attachment_maintenance::wake(&self.data_dir, &changed_source);
            self.core.invalidate_stat_cache();
            self.host.notify_maintenance();
        }
        result
    }
    fn upload_attachment(
        &self,
        actor: &str,
        request: DfsAttachmentUploadRequest,
        reader: &mut std::fs::File,
    ) -> Result<DfsAttachmentResponse, ServiceError> {
        let changed_source = request.source.clone();
        let result = self.core.with_mutation_service(|service| {
            service
                .posix_stat_distributed(&request.source.path)
                .map_err(dfs_browser_service_error)?;
            let options = self
                .core
                .write_options_for_path(service, &request.source.path)?;
            service
                .file_attachment_from_reader_as(actor, request, reader, options)
                .map_err(dfs_browser_service_error)
        });
        crate::attachment_maintenance::wake(&self.data_dir, &changed_source);
        self.host.notify_maintenance();
        result
    }
}
#[derive(Default)]
struct AttachmentRuntime {
    admission: Arc<crate::attachment_admission::AttachmentAdmission>,
}
fn attachment_actor(ctx: &ServiceContext, component_only: bool) -> Result<String, ServiceError> {
    if let Some(component) = ctx.component_caller() {
        return Ok(format!("component:{component}"));
    }
    if component_only {
        return Err(dfs_browser_service_error(
            "file attachments require a checked component caller",
        ));
    }
    Ok(match &ctx.signer {
        ServiceSigner::Local => "local-operator".into(),
        ServiceSigner::LocalNode { node_id, .. } | ServiceSigner::Remote { node_id, .. } => {
            format!("node:{node_id}")
        }
    })
}

async fn attachment_work_limits(
    data_dir: Arc<SinomoDataDir>,
    runtime: tokio::runtime::Handle,
    host: Arc<dyn DfsBrowserServiceHost>,
    actor: String,
) -> Result<crate::attachment_admission::AttachmentWorkLimits, ServiceError> {
    run_blocking_service_task("attachment-policy", "attachment policy read", move || {
        let mut client = host.control_plane_client(&ClusterPaths::new(&data_dir), &runtime);
        let policy = match client.as_mut() {
            Some(client) => {
                crate::attachment_catalog::read_attachment_policy(Some(client.as_mut()))
            }
            None => Ok(DfsAttachmentPolicy::default()),
        }
        .map_err(dfs_browser_service_error)?;
        Ok(crate::attachment_admission::AttachmentWorkLimits::for_actor(&policy, &actor))
    })
    .await
}

pub const FEATURE_ROUTES: &[&str] = &[
    DFS_ATTACHMENT_ROUTE,
    DFS_ATTACHMENT_COMPONENT_ROUTE,
    DFS_ATTACHMENT_UPLOAD_ROUTE,
];

struct HostedFeatureService {
    runtime: tokio::runtime::Handle,
    host: Arc<dyn DfsBrowserServiceHost>,
    core_client: sinomo_module::ComponentServiceClient,
    feature_runtime: Arc<AttachmentRuntime>,
}
impl Service for HostedFeatureService {
    fn name(&self) -> &'static str {
        "dfs-attachments"
    }
    fn routes(&self) -> &'static [&'static str] {
        FEATURE_ROUTES
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        FEATURE_ROUTES.iter().copied().map(feature_route).collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let backend_host = Arc::clone(&self.host);
        let effect_host = Arc::clone(&self.host);
        let core_client = self.core_client.clone();
        let feature_runtime = Arc::clone(&self.feature_runtime);
        Box::pin(async move {
            authorize_dfs_core_component_access(&core_client).await?;
            let _owner = backend_host
                .operation_guard()?
                .ok_or_else(|| dfs_browser_service_error("DFS component owner is unavailable"))?;
            let route = request.route.clone();
            let request_traceparent = request.traceparent().map(ToString::to_string);
            let data_dir = Arc::clone(&ctx.data_dir);
            match route.as_str() {
                DFS_ATTACHMENT_UPLOAD_ROUTE => {
                    let operation_owner = backend_host.operation_guard()?;
                    let actor = attachment_actor(&ctx, false)?;
                    let payload: DfsAttachmentUploadRequest =
                        ctx.decode_request_body_bounded(&request, 16 * 1024).await?;
                    payload.validate().map_err(dfs_browser_service_error)?;
                    if payload.payload_bytes > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64 {
                        return Err(dfs_browser_service_error(
                            "attachment upload exceeds 64 MiB",
                        ));
                    }
                    let mut timing =
                        crate::service_trace::DfsPhaseTimer::new(&payload.operation_id);
                    let limits = attachment_work_limits(
                        Arc::clone(&data_dir),
                        runtime.clone(),
                        Arc::clone(&backend_host),
                        actor.clone(),
                    )
                    .await?;
                    timing.mark("admission_policy");
                    let permit = feature_runtime
                        .admission
                        .acquire(
                            actor.clone(),
                            payload.operation_id.clone(),
                            payload.payload_bytes,
                            &limits,
                        )
                        .await
                        .map_err(dfs_browser_service_error)?;
                    timing.mark("admission_queue");
                    let body = ctx.take_request_body();
                    let changed_path = payload.source.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "streamed DFS attachment",
                        move || {
                            let _operation_owner = operation_owner;
                            let _permit = permit;
                            let mut spool =
                                crate::attachment_upload::AttachmentUploadSpool::receive(
                                    &backend_data_dir,
                                    body,
                                    payload.payload_bytes,
                                    &backend_runtime,
                                )
                                .map_err(dfs_browser_service_error)?;
                            timing.mark("body_spool");
                            let backend = FeatureBackend::for_data_dir(
                                backend_data_dir,
                                &backend_runtime,
                                Arc::clone(&backend_host),
                            )?;
                            timing.mark("backend_open");
                            let result =
                                backend.upload_attachment(&actor, payload, spool.file())?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    effect_host.schedule_metadata_publish(
                        data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    effect_host.publish_namespace_change(
                        result.1,
                        "file-attachment",
                        vec![changed_path],
                    );
                    ctx.respond_value(route, result.0)?;
                }
                DFS_ATTACHMENT_ROUTE | DFS_ATTACHMENT_COMPONENT_ROUTE => {
                    let operation_owner = backend_host.operation_guard()?;
                    let actor = attachment_actor(&ctx, route == DFS_ATTACHMENT_COMPONENT_ROUTE)?;
                    let payload: DfsAttachmentRequest =
                        ctx.decode_request_body_bounded(&request, 80 * 1024).await?;
                    let changed_path = payload.source().path.clone();
                    let mutation = payload.is_mutation();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let result = if mutation {
                        let (operation_id, bytes) = match &payload {
                            DfsAttachmentRequest::ClaimGeneration { upload } => {
                                (upload.operation_id.clone(), 0)
                            }
                            DfsAttachmentRequest::Put {
                                operation_id,
                                bytes,
                                ..
                            } => (operation_id.clone(), bytes.len() as u64),
                            DfsAttachmentRequest::Delete { operation_id, .. } => {
                                (operation_id.clone(), 0)
                            }
                            DfsAttachmentRequest::Maintain { source, .. } => {
                                (format!("maintenance-{}", source.object_id), 0)
                            }
                            _ => unreachable!(),
                        };
                        let limits = attachment_work_limits(
                            Arc::clone(&data_dir),
                            runtime.clone(),
                            Arc::clone(&backend_host),
                            actor.clone(),
                        )
                        .await?;
                        let permit = feature_runtime
                            .admission
                            .acquire(actor.clone(), operation_id, bytes, &limits)
                            .await
                            .map_err(dfs_browser_service_error)?;
                        run_blocking_service_task(
                            route.clone(),
                            "DFS attachment mutation",
                            move || {
                                let _operation_owner = operation_owner;
                                let _permit = permit;
                                let backend = FeatureBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.file_attachment(&actor, payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            },
                        )
                        .await?
                    } else {
                        run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS attachment read",
                            move || {
                                let _operation_owner = operation_owner;
                                let backend = FeatureBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.file_attachment(&actor, payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            },
                        )
                        .await?
                    };
                    if mutation {
                        effect_host.schedule_metadata_publish(
                            data_dir.as_ref(),
                            &runtime,
                            request_traceparent.as_deref(),
                        );
                        effect_host.publish_namespace_change(
                            result.1,
                            "file-attachment",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(route, result.0)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_owned())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}
fn feature_route(route: &'static str) -> ServiceRoute {
    if route == DFS_ATTACHMENT_COMPONENT_ROUTE {
        return ServiceRoute::new(route, DAEMON_RPC_CATEGORY)
            .with_auth_scope("component")
            .require_capability(DFS_ATTACHMENT_COMPONENT_CAPABILITY)
            .with_concurrency_limit(16)
            .with_timeout(Duration::from_secs(60));
    }
    let descriptor = ServiceRoute::new(route, DAEMON_RPC_CATEGORY)
        .with_auth_scope("local-or-cluster-member")
        .require_capability("daemon.dfs.browser")
        .require_capability(sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY)
        .with_concurrency_limit(4)
        .with_timeout(Duration::from_secs(60));
    if route == DFS_ATTACHMENT_UPLOAD_ROUTE {
        descriptor.with_request_body_mode(sinomo_core::service::ServiceBodyMode::Optional)
    } else {
        descriptor
    }
}
pub fn module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named("module.dfs.attachments")
        .expect("valid filesystem feature module id")
}
pub fn module_enabled(root: impl AsRef<Path>) -> Result<bool, sinomo_dfs::DfsError> {
    sinomo_module::native_rust_module_enabled(root.as_ref(), &module_id(), true).map_err(|error| {
        sinomo_dfs::DfsError::Metadata {
            message: error.to_string(),
        }
    })
}
pub fn module_registration() -> sinomo_module::ModuleRegistration {
    let mut registration = sinomo_module::ModuleRegistration::native_rust(
        module_id(),
        env!("CARGO_PKG_VERSION"),
        "sinomo-dfs-attachments",
        env!("CARGO_PKG_NAME"),
        "sinomo.dfs.attachments",
        sinomo_core::TrustRing::R1TrustedRuntime,
        "sinomo-dfs-attachments",
    )
    .with_display_name("File Attachments");
    let mut surfaces =
        sinomo_module::ModuleSurfaces::default().with_storage_owner("sinomo-dfs-attachments");
    for route in FEATURE_ROUTES.iter().copied() {
        let descriptor = feature_route(route);
        let auth = if descriptor.auth_scope.as_deref() == Some("component") {
            sinomo_module::ModuleRouteAuth::Component
        } else if descriptor.auth_scope.as_deref() == Some("local-command") {
            sinomo_module::ModuleRouteAuth::LocalCommand
        } else {
            sinomo_module::ModuleRouteAuth::LocalOrClusterMember
        };
        let mut surface = sinomo_module::ModuleObjectRpcRoute::new(
            route,
            DAEMON_RPC_CATEGORY,
            if descriptor.auth_scope.as_deref() == Some("component") {
                "dfs.attachments.manage"
            } else {
                "daemon.dfs.browser"
            },
            auth,
        )
        .with_concurrency_limit(if descriptor.auth_scope.as_deref() == Some("component") {
            16
        } else {
            4
        })
        .with_timeout_ms(60_000);
        if route == DFS_ATTACHMENT_UPLOAD_ROUTE {
            surface =
                surface.with_request_body_mode(sinomo_module_api::NativeModuleBodyMode::Optional);
        }
        surfaces = surfaces.with_object_rpc_route(surface);
    }
    surfaces = surfaces
        .with_config_definition(dfs_attachment_policy_definition())
        .expect("valid attachment settings surface");
    let capabilities = sinomo_core::ModuleCapabilities {
        imports: [
            "dfs.semantic.access",
            "storage.local.read",
            "storage.local.write",
            "network.object_rpc",
            "cluster.kv.read",
        ]
        .into_iter()
        .map(|name| sinomo_core::CapabilityName::new(name).expect("valid feature import"))
        .collect(),
        exports: [
            "daemon.dfs.browser",
            "sifs.path.access",
            "dfs.attachments.manage",
        ]
        .into_iter()
        .map(|name| sinomo_core::CapabilityName::new(name).expect("valid feature export"))
        .collect(),
    };
    surfaces.capabilities = capabilities.clone();
    registration.manifest.capabilities = capabilities;
    registration.manifest.limits = sinomo_core::ModuleResourceLimits {
        network: sinomo_core::NetworkAccess::Allowlisted,
        filesystem: sinomo_core::FileSystemAccess::Scoped,
        ..Default::default()
    };
    registration.surfaces = surfaces;
    let mut browser_offer = sinomo_module::ServiceOffer::new(
        "dfs-attachments-browser",
        "sinomo.dfs.attachments.browser",
        "1.0.0",
    )
    .cluster_visible()
    .with_provider_capability("daemon.dfs.browser")
    .with_provider_capability(sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY);
    for route in FEATURE_ROUTES.iter().copied() {
        if feature_route(route).auth_scope.as_deref() != Some("component") {
            browser_offer = browser_offer.with_route(route);
        }
    }
    let mut descriptor =
        sinomo_module::ComponentDescriptor::new(module_id().as_str(), env!("CARGO_PKG_VERSION"))
            .with_display_name("File Attachments")
            .with_offer(browser_offer)
            .require(dfs_core_component_requirement());
    descriptor = descriptor
        .with_offer(
            sinomo_module::ServiceOffer::new(
                DFS_ATTACHMENT_COMPONENT_CAPABILITY,
                "sinomo.dfs.attachments",
                "2.0.0",
            )
            .with_operation(
                sinomo_module::ServiceOperationOffer::new(DFS_ATTACHMENT_COMPONENT_CAPABILITY)
                    .at_route(DFS_ATTACHMENT_COMPONENT_ROUTE)
                    .require_caller_capability(DFS_ATTACHMENT_COMPONENT_CAPABILITY),
            ),
        )
        .subscribe_event(namespace_subscription())
        .subscribe_event(cluster_control_subscription());
    registration.component = descriptor;
    registration
}
pub fn component_factory()
-> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let descriptor = sinomo_module::component_descriptor_from_registration(&module_registration())?;
    let caller = descriptor.clone();
    sinomo_module::StaticComponentFactory::new(descriptor, move |context, scope| {
        let runtime = context.runtime().cloned().ok_or_else(|| {
            sinomo_module::ComponentError::component(
                &caller.component_id,
                "filesystem feature requires the core runtime",
            )
        })?;
        let host = dfs_browser_component_host(context.data_dir()).map_err(|error| {
            sinomo_module::ComponentError::component(&caller.component_id, error)
        })?;
        let core_client = context.service_client(&caller)?;
        let feature_runtime = Arc::new(AttachmentRuntime::default());
        let event_client = context.event_client(&caller)?;
        let namespace_events =
            event_client
                .subscribe(&namespace_subscription())
                .map_err(|error| {
                    sinomo_module::ComponentError::component(&caller.component_id, error)
                })?;
        let cluster_events = event_client
            .subscribe(&cluster_control_subscription())
            .map_err(|error| {
                sinomo_module::ComponentError::component(&caller.component_id, error)
            })?;
        mount_attachment_maintenance(
            scope,
            Arc::new(context.data_dir().clone()),
            runtime.clone(),
            Arc::clone(&host),
            core_client.clone(),
            Arc::clone(&feature_runtime),
            namespace_events,
            cluster_events,
        );
        let service = HostedFeatureService {
            runtime,
            host,
            core_client,
            feature_runtime,
        };
        scope.set_services(ServiceRegistryBuilder::default().register(service).build());
        Ok(())
    })
}
pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    for (path, reason) in [
        (
            "dfs/attachment-maintenance.sobj",
            "bounded attachment cleanup and durable namespace feed checkpoint",
        ),
        (
            "dfs/attachment-maintenance-quarantine",
            "invalid reconstructible attachment cleanup checkpoint",
        ),
        (
            "dfs/content/staging/attachments",
            "bounded temporary attachment upload bodies",
        ),
    ] {
        layout.register_escape_path(
            "sinomo-dfs-attachments",
            sinomo_core::storage::StorageRing::R1,
            path,
            reason,
        )?;
    }
    Ok(())
}
fn cluster_control_subscription() -> sinomo_module::ComponentEventSubscription {
    sinomo_module::ComponentEventSubscription::new(
        sinomo_module::COMPONENT_HOST_CLUSTER_CONTROL_CHANGED_EVENT_KIND,
        sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_ID,
        sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_VERSION,
    )
}
fn namespace_subscription() -> sinomo_module::ComponentEventSubscription {
    sinomo_module::ComponentEventSubscription::new(
        DFS_NAMESPACE_CHANGED_EVENT_KIND,
        DFS_EVENT_SCHEMA_ID,
        DFS_EVENT_SCHEMA_VERSION,
    )
}
// Both foreground feature routes and this background borrower use the same
// resident core browser. Keep the feature and component custody in the work.
async fn run_attachment_cleanup_pass(
    data: Arc<SinomoDataDir>,
    runtime: tokio::runtime::Handle,
    operation_owner: Arc<dyn Send + Sync>,
    permit: crate::attachment_admission::AttachmentPermit,
) -> Result<DfsAttachmentMaintenanceReport, ServiceError> {
    run_blocking_browser_task(
        Arc::clone(&data),
        "attachment-cleanup",
        "attachment cleanup",
        move || {
            let _owner = operation_owner;
            let _permit = permit;
            let backend = DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &runtime)?;
            backend.with_mutation_service(|service| {
                #[cfg(test)]
                cleanup_admission_tests::pause_if_registered(&data);
                let shards = service
                    .metadata_authorities(&sifs_file_system_id())
                    .map_err(dfs_browser_service_error)?;
                if service.is_distributed()
                    && !shards
                        .iter()
                        .any(|shard| &shard.primary_node_id == service.node_id())
                {
                    crate::attachment_maintenance::defer_without_authority(&data);
                    return Ok(DfsAttachmentMaintenanceReport::default());
                }
                service
                    .maintain_file_attachments_bounded(
                        &crate::attachment_maintenance::checkpoint_path(&data),
                        &shards,
                        8,
                    )
                    .map_err(dfs_browser_service_error)
            })
        },
    )
    .await
}

fn mount_attachment_maintenance(
    scope: &mut sinomo_module::ComponentScope,
    data_dir: Arc<SinomoDataDir>,
    runtime: tokio::runtime::Handle,
    host: Arc<dyn DfsBrowserServiceHost>,
    core_client: sinomo_module::ComponentServiceClient,
    feature_runtime: Arc<AttachmentRuntime>,
    mut namespace_events: sinomo_module::ComponentEventReceiver,
    mut cluster_events: sinomo_module::ComponentEventReceiver,
) {
    let mut shutdown = scope.shutdown_receiver();
    let owner_runtime = runtime.clone();
    let task = runtime.spawn(async move {
        crate::attachment_maintenance::request_reconciliation(&data_dir);
        let Ok(notify) = crate::attachment_maintenance::notifier(&data_dir) else { return; };
        loop {
            if *shutdown.borrow() { break; }
            let now = crate::dfs_now_unix_nanos();
            let due = crate::attachment_maintenance::deadline(&data_dir).ok().flatten();
            if due.is_some_and(|due| due <= now) {
                crate::attachment_maintenance::begin_pass(&data_dir);
                let data = Arc::clone(&data_dir);
                let host = Arc::clone(&host);
                let runtime = owner_runtime.clone();
                let feature_runtime = Arc::clone(&feature_runtime);
                let core_client = core_client.clone();
                let work = async {
                    authorize_dfs_core_component_access(&core_client).await?;
                    let operation_owner = host.operation_guard()?.ok_or_else(|| dfs_browser_service_error("DFS owner unavailable"))?;
                    let limits = attachment_work_limits(Arc::clone(&data), runtime.clone(), Arc::clone(&host), "maintenance".into()).await?;
                    let permit = feature_runtime.admission.acquire("maintenance".into(), "attachment-cleanup".into(), 0, &limits).await.map_err(dfs_browser_service_error)?;
                    run_attachment_cleanup_pass(data, runtime, operation_owner, permit).await
                };
                tokio::select! {
                    _ = shutdown.changed() => break,
                    result = work => {
                        match result {
                            Err(error) => {
                                crate::attachment_maintenance::defer_after_error(&data_dir);
                                tracing::debug!(%error, "attachment cleanup deferred");
                            }
                            Ok(report) => for error in report.errors { tracing::debug!(%error, "attachment source cleanup deferred"); },
                        }
                    }
                }
                // Continue productive pages immediately; timers represent only
                // real retry or payload grace deadlines.
                tokio::task::yield_now().await;
                continue;
            }
            let timer = async {
                match due {
                    Some(deadline) => tokio::time::sleep(Duration::from_nanos(deadline.saturating_sub(now))).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = notify.notified() => {},
                _ = timer => {},
                event = cluster_events.recv() => {
                    match event {
                        Err(sinomo_module::ComponentEventReceiveError::Closed) => break,
                        _ => crate::attachment_maintenance::request_reconciliation(&data_dir),
                    }
                }
                event = namespace_events.recv() => {
                    match event {
                        Err(sinomo_module::ComponentEventReceiveError::Closed) => break,
                        Err(sinomo_module::ComponentEventReceiveError::Lagged { .. }) => crate::attachment_maintenance::request_reconciliation(&data_dir),
                        Ok(_) => crate::attachment_maintenance::notify(&data_dir),
                    }
                }
            }
        }
    });
    scope.own_task("dfs-attachment-maintenance", task);
}

#[cfg(test)]
mod attachment_identity_tests {
    use super::*;
    struct Sink;
    impl sinomo_core::service::ResponseSink for Sink {
        fn send(
            self: Box<Self>,
            _: sinomo_core::network::ObjectRpcOutgoingResponse,
        ) -> Result<(), ServiceError> {
            Ok(())
        }
    }
    #[test]
    fn attachment_component_identity_requires_checked_host_context() {
        let root = sinomo_testdir::TestDir::new("attachment-identity");
        let context = ServiceContext::new(
            Arc::new(SinomoDataDir::new(root.path())),
            None,
            ServiceSigner::Local,
            sinomo_core::network::ObjectRpcIncomingBody::empty(),
            Box::new(Sink),
        );
        // Wire caller labels cannot populate the host-owned context identity.
        assert!(attachment_actor(&context, true).is_err());
        assert_eq!(attachment_actor(&context, false).unwrap(), "local-operator");
        let context = context.with_component_caller("example.extractor".into());
        assert_eq!(
            attachment_actor(&context, true).unwrap(),
            "component:example.extractor"
        );
    }
}

#[cfg(test)]
mod cleanup_admission_tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock, mpsc};

    struct Pause {
        entered: tokio::sync::oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    static PAUSES: OnceLock<Mutex<HashMap<PathBuf, Pause>>> = OnceLock::new();

    pub(super) fn pause_if_registered(data: &SinomoDataDir) {
        let pause = PAUSES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .remove(&data.root());
        if let Some(pause) = pause {
            // This barrier only delays the actual native borrower. It supplies
            // no data, authority, readiness or successful operation result.
            pause.entered.send(()).unwrap();
            pause.release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    #[test]
    fn native_cleanup_shares_browser_admission_through_caller_cancellation() {
        let root = sinomo_testdir::TestDir::new("attachment-cleanup-browser-admission");
        let data = Arc::new(SinomoDataDir::new(root.path()));
        let (native, node) =
            sinomo_dfs::test_support::DfsTestRuntime::start_single_node_in_data_dir(
                "attachment-cleanup-browser-admission",
                (*data).clone(),
            )
            .unwrap();
        let runtime = native.runtime_handle();
        let host = dfs_browser_component_host(&data).unwrap();
        let backend = DfsBrowserBackend::for_component_data_dir(data.clone(), &runtime).unwrap();
        backend
            .with_mutation_service(|service| {
                service
                    .posix_write_file(
                        "/cleanup-admission.txt",
                        b"original committed bytes",
                        DfsWriteOptions::replicated_eventual(vec![node.clone()]),
                    )
                    .map_err(dfs_browser_service_error)
            })
            .unwrap();
        let admission = Arc::new(crate::attachment_admission::AttachmentAdmission::default());
        let results = runtime.block_on(async {
            let mut results = Vec::new();
            for cancel_waiter in [false, true] {
                let (entered, seen) = tokio::sync::oneshot::channel();
                let (release, released) = mpsc::channel();
                assert!(
                    PAUSES
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap()
                        .insert(
                            data.root(),
                            Pause {
                                entered,
                                release: released
                            }
                        )
                        .is_none()
                );
                let owner = host.operation_guard().unwrap().unwrap();
                let permit = admission
                    .acquire(
                        "maintenance".into(),
                        "attachment-cleanup".into(),
                        0,
                        &crate::attachment_admission::AttachmentWorkLimits::default(),
                    )
                    .await
                    .unwrap();
                let cleanup = tokio::spawn(run_attachment_cleanup_pass(
                    data.clone(),
                    runtime.clone(),
                    owner,
                    permit,
                ));
                tokio::time::timeout(Duration::from_secs(5), seen)
                    .await
                    .unwrap()
                    .unwrap();
                if cancel_waiter {
                    cleanup.abort();
                }
                // Direct callers keep the existing 100-ms fail-closed bound.
                let direct = backend.clone();
                let rejected =
                    tokio::task::spawn_blocking(move || direct.with_read_service(|_| Ok(())))
                        .await
                        .unwrap()
                        .unwrap_err();
                assert!(
                    rejected.to_string().contains("busy after waiting 100ms"),
                    "{rejected}"
                );
                let reader = backend.clone();
                let mut queued = tokio::spawn(run_blocking_browser_task(
                    data.clone(),
                    "test-hosted-read",
                    "native queued read",
                    move || {
                        reader.with_read_service(|service| {
                            service
                                .posix_read_file_distributed("/cleanup-admission.txt")
                                .map_err(dfs_browser_service_error)
                        })
                    },
                ));
                let observed = tokio::time::timeout(Duration::from_millis(150), &mut queued).await;
                let was_queued = observed.is_err();
                release.send(()).unwrap();
                let bytes = match observed {
                    Ok(result) => result.unwrap(),
                    Err(_) => tokio::time::timeout(Duration::from_secs(5), queued)
                        .await
                        .unwrap()
                        .unwrap(),
                };
                let finished = cleanup.await;
                if cancel_waiter {
                    assert!(finished.unwrap_err().is_cancelled());
                } else {
                    finished.unwrap().unwrap();
                }
                // Reacquiring the same producer identity proves its original
                // permit drained even when the async waiter was cancelled.
                let drained = tokio::time::timeout(
                    Duration::from_secs(5),
                    admission.acquire(
                        "maintenance".into(),
                        "attachment-cleanup".into(),
                        0,
                        &crate::attachment_admission::AttachmentWorkLimits::default(),
                    ),
                )
                .await
                .unwrap();
                drop(drained.unwrap());
                results.push((cancel_waiter, was_queued, bytes));
                // Drain the first failed control before reporting it; a later
                // cancellation cannot clarify an already rejected read.
                if !was_queued {
                    break;
                }
            }
            results
        });
        drop(backend);
        drop(host);
        native.close().unwrap();
        for (cancelled, was_queued, bytes) in results {
            assert!(
                was_queued,
                "hosted read bypassed cleanup custody (cancelled={cancelled}): {bytes:?}"
            );
            assert_eq!(bytes.unwrap(), b"original committed bytes");
        }
    }
}
