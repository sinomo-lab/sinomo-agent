use super::*;
use notify::Watcher;
use sinomo_core::{
    network::ObjectRpcRequest,
    service::{
        Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
        ServiceRoute,
    },
};
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct DesktopUpdateService;

pub(crate) fn update_publication() -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(APP_UPDATE_EVENT, "sinomo.app.update", "1.0.0")
}

pub(crate) fn observe_updates(
    context: &sinomo_module::ComponentContext,
    scope: &mut sinomo_module::ComponentScope,
    caller: &sinomo_module::ComponentDescriptor,
) -> std::result::Result<(), sinomo_module::ComponentError> {
    if !cfg!(any(
        target_os = "linux",
        target_os = "windows",
        target_os = "macos"
    )) || context.host_role() == sinomo_module::ComponentHostRole::Shadow
    {
        return Ok(());
    }
    let (Some(runtime), Ok(events)) = (context.runtime(), context.event_client(caller)) else {
        return Ok(());
    };
    let Some(origin) = events.origin_node_id().cloned() else {
        return Ok(());
    };
    let releases = DesktopReleases::new(context.data_dir_root());
    fn component_error(error: impl std::fmt::Display) -> sinomo_module::ComponentError {
        sinomo_module::ComponentError::component(crate::SINOMO_ROLLOUT_MODULE_LEGACY_ID, error)
    }
    fs::create_dir_all(&releases.root).map_err(component_error)?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.as_ref().is_ok_and(is_state_change) {
            let _ = tx.try_send(());
        }
    })
    .map_err(component_error)?;
    watcher
        .watch(&releases.root, notify::RecursiveMode::NonRecursive)
        .map_err(component_error)?;
    let mut shutdown = scope.shutdown_receiver();
    let task = runtime.spawn(async move {
        let _watcher = watcher;
        let mut previous_revision = releases.status().map(|s| s.revision).unwrap_or_default();
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                change = rx.recv() => if change.is_none() { break; },
            }
            let Ok(status) = releases.status() else {
                continue;
            };
            if status.revision == previous_revision {
                continue;
            }
            let envelope = sinomo_core::EventEnvelope::new(
                sinomo_core::Id::derived(
                    "app.update.changed",
                    format!("{}:{}", origin, status.revision),
                ),
                time::OffsetDateTime::now_utc(),
                origin.clone(),
                sinomo_core::Source::new("component/sinomo-rollout").expect("static source"),
                sinomo_core::EventKind::new(APP_UPDATE_EVENT).expect("static event"),
                sinomo_core::Salience::new(0.5).expect("static salience"),
                sinomo_core::Confidence::new(1.0).expect("static confidence"),
                sinomo_core::RoutingMetadata::new(
                    sinomo_core::RoutingScope::Local,
                    sinomo_core::LatencyClass::Reactive,
                    sinomo_core::PrivacyClass::Derived,
                ),
            )
            .with_payload(
                serde_json::json!({"previous_revision": previous_revision, "status": status}),
            );
            previous_revision = status.revision;
            if let Err(error) = events.publish(&update_publication(), envelope).await {
                tracing::warn!(%error, "desktop update event publication failed");
            }
        }
    });
    scope.own_task("desktop update notifications", task);
    Ok(())
}

fn is_state_change(event: &notify::Event) -> bool {
    matches!(
        event.kind,
        notify::EventKind::Create(_) | notify::EventKind::Modify(_) | notify::EventKind::Remove(_)
    ) && event
        .paths
        .iter()
        .any(|p| p.file_name().is_some_and(|n| n == "state.json"))
}

impl Service for DesktopUpdateService {
    fn name(&self) -> &'static str {
        "desktop-rollout"
    }
    fn routes(&self) -> &'static [&'static str] {
        APP_UPDATE_ROUTES
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        APP_UPDATE_ROUTES
            .iter()
            .map(|route| {
                if *route == APP_UPDATE_UPLOAD_ROUTE {
                    return ServiceRoute::from_route_metadata(
                        *route,
                        "rollout",
                        "admin",
                        ["rollout.app.stage"],
                    )
                    .require_local_permission("remote-management")
                    .with_request_body_mode(ServiceBodyMode::Required)
                    .with_concurrency_limit(2);
                }
                ServiceRoute::from_route_metadata(
                    *route,
                    "rollout",
                    "local-command",
                    [if *route == APP_UPDATE_STATUS_ROUTE {
                        "rollout.app.read"
                    } else {
                        "rollout.app.update"
                    }],
                )
                .with_concurrency_limit(8)
            })
            .collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            if request.route == APP_UPDATE_UPLOAD_ROUTE {
                if !ctx.signer.is_local() {
                    let permissions =
                        sinomo_core::node::LocalNodePermissionStore::new(ctx.data_dir.root());
                    if !permissions
                        .try_permission_granted(
                            sinomo_core::node::LocalNodePermission::RemoteManagement,
                        )
                        .map_err(|e| ServiceError::Service(e.to_string()))?
                    {
                        return Err(ServiceError::Service(
                            "remote desktop staging requires node-local remote-management consent"
                                .into(),
                        ));
                    }
                }
                let payload = ctx.decode_request_body(&request).await?;
                let body = ctx.take_request_body();
                let response = super::receive_artifact(ctx.data_dir.root(), payload, body)
                    .await
                    .map_err(ServiceError::Service)?;
                ctx.respond_value(APP_UPDATE_UPLOAD_ROUTE, response)?;
                return Ok(ServiceDisposition::Continue);
            }
            if !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "desktop updates require local user authority".into(),
                ));
            }
            let releases = DesktopReleases::new(ctx.data_dir.root());
            let route = request.route.clone();
            let operation: Box<dyn FnOnce() -> Result<AppUpdateStatus> + Send> = match route
                .as_str()
            {
                APP_UPDATE_STATUS_ROUTE => {
                    let value: AppUpdateStatusRequest = ctx.decode_request_body(&request).await?;
                    Box::new(move || wait_status(&releases, value))
                }
                APP_UPDATE_STAGE_ROUTE => {
                    let value: AppUpdateStageRequest = ctx.decode_request_body(&request).await?;
                    Box::new(move || releases.stage(&value.bundle_directory))
                }
                APP_UPDATE_RESTART_ROUTE => {
                    let value = ctx.decode_request_body(&request).await?;
                    Box::new(move || releases.request_restart(value))
                }
                APP_UPDATE_READY_ROUTE => {
                    let value = ctx.decode_request_body(&request).await?;
                    Box::new(move || releases.acknowledge_ready(value))
                }
                _ => return Err(ServiceError::UnknownRoute(route)),
            };
            let result = tokio::task::spawn_blocking(operation)
                .await
                .map_err(|e| ServiceError::Service(e.to_string()))?
                .map_err(ServiceError::Service)?;
            ctx.respond_value(route, result)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn wait_status(
    releases: &DesktopReleases,
    request: AppUpdateStatusRequest,
) -> Result<AppUpdateStatus> {
    let initial = releases.status()?;
    if request.after_revision != Some(initial.revision) || request.wait_ms == 0 {
        return Ok(initial);
    }
    fs::create_dir_all(&releases.root).map_err(err)?;
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.as_ref().is_ok_and(is_state_change) {
            let _ = tx.try_send(());
        }
    })
    .map_err(err)?;
    watcher
        .watch(&releases.root, notify::RecursiveMode::NonRecursive)
        .map_err(err)?;
    let deadline = Instant::now() + Duration::from_millis(request.wait_ms.min(25_000));
    loop {
        // Install the watch before checking again so publication cannot fall into a read/watch gap.
        let status = releases.status()?;
        if request.after_revision != Some(status.revision) {
            return Ok(status);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || rx.recv_timeout(remaining).is_err() {
            return releases.status();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        local::SinomoDataDir,
        network::{ObjectRpcIncomingBody, ObjectRpcOutgoingResponse},
        service::{ResponseSink, ServiceSigner},
    };
    use std::sync::Arc;

    struct Sink;
    impl ResponseSink for Sink {
        fn send(
            self: Box<Self>,
            _: ObjectRpcOutgoingResponse,
        ) -> std::result::Result<(), ServiceError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn remote_staging_needs_consent_and_remote_restart_remains_forbidden() {
        let root = sinomo_testdir::TestDir::new("desktop-authority");
        let service = DesktopUpdateService;
        for route in [
            APP_UPDATE_UPLOAD_ROUTE,
            APP_UPDATE_RESTART_ROUTE,
            APP_UPDATE_READY_ROUTE,
            APP_UPDATE_STAGE_ROUTE,
        ] {
            let context = ServiceContext::new(
                Arc::new(SinomoDataDir::new(&root)),
                None,
                ServiceSigner::Remote {
                    node_id: "remote".into(),
                    public_key: "key".into(),
                },
                ObjectRpcIncomingBody::empty(),
                Box::new(Sink),
            );
            let error = service
                .handle(
                    context,
                    ObjectRpcRequest::encode_body(route, "rollout", &()).unwrap(),
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(if route == APP_UPDATE_UPLOAD_ROUTE {
                        "consent"
                    } else {
                        "local user authority"
                    }),
                "{route}: {error}"
            );
        }
        let routes = service.route_categories();
        let upload = routes
            .iter()
            .find(|r| r.route == APP_UPDATE_UPLOAD_ROUTE)
            .unwrap();
        assert_eq!(upload.auth_scope.as_deref(), Some("admin"));
        assert_eq!(upload.required_local_permissions, ["remote-management"]);
        assert_eq!(upload.request_body_mode, ServiceBodyMode::Required);
        for route in routes.iter().filter(|r| r.route != APP_UPDATE_UPLOAD_ROUTE) {
            assert_eq!(route.auth_scope.as_deref(), Some("local-command"));
        }
    }

    #[test]
    fn status_watch_wakes_after_atomic_publication() {
        let root = sinomo_testdir::TestDir::new("desktop-update-watch");
        let releases = DesktopReleases::new(&root);
        let worker = releases.clone();
        let thread = std::thread::spawn(move || {
            wait_status(
                &worker,
                AppUpdateStatusRequest {
                    after_revision: Some(0),
                    wait_ms: 3000,
                },
            )
        });
        releases.begin_launch(false).unwrap();
        assert!(thread.join().unwrap().unwrap().revision > 0);
    }
}
