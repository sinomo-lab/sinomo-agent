//! Conversation-scoped session service and module panel invocation owner.
use crate::session_access::AgentSessionAccess;
use sinomo_runtime::{
    local::SinomoDataDir,
    module::{
        ActiveNativeComponent, ActiveNativeComponentResponse, UiExtensionCatalogRequest,
        sessions::{SessionAccess, SessionHostCallbacks},
        ui_extension_catalog_for_host,
    },
    module_api::{panels::*, *},
    network::ObjectRpcRequest,
    service::{
        Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture, ServiceRoute,
    },
};
use std::sync::Arc;

pub fn render_panel(
    data_dir: &SinomoDataDir,
    request: SessionPanelRequest,
) -> Result<PanelRenderResponse, String> {
    request
        .render
        .viewport
        .validate()
        .map_err(|e| e.to_string())?;
    if serde_json::to_vec(&request.render.state)
        .map_err(|e| e.to_string())?
        .len()
        > 64 * 1024
    {
        return Err("panel state exceeds limit".into());
    }
    let sessions = Arc::new(AgentSessionAccess::for_conversation(
        data_dir,
        &request.conversation_id,
    )?);
    let module_id = sinomo_runtime::module::parse_native_dylib_module_id(&request.module_id)
        .map_err(|e| e.to_string())?;
    let catalog = ui_extension_catalog_for_host(
        &data_dir.root(),
        &UiExtensionCatalogRequest {
            extension_point_id: Some(CONVERSATION_PANELS.into()),
        },
    )?;
    if request
        .expected_generation
        .is_some_and(|generation| generation != catalog.registry_generation)
    {
        return Err("panel catalog changed; refresh the selected panel".into());
    }
    let entry = catalog
        .entries
        .iter()
        .find(|entry| {
            entry.module_id == module_id.as_str()
                && entry.contribution.contribution_id == request.contribution_id
        })
        .ok_or("panel contribution is not active in the compiled catalog")?;
    let contribution = &entry.contribution;
    let UiContributionPayload::Declarative {
        schema_id,
        payload_sobj,
    } = &contribution.payload
    else {
        return Err("not a declarative session panel".into());
    };
    if schema_id != PANEL_SCHEMA {
        return Err("unsupported panel schema".into());
    }
    let definition: SessionPanelDefinition =
        decode_sobj(payload_sobj).map_err(|e| e.to_string())?;
    let requirement = ServiceRequirement::new(PANEL_INTERFACE, "1.0.0")
        .from_service(&definition.service_id)
        .with_operation(PANEL_RENDER);
    let provider = unsafe { ActiveNativeComponent::resolve_shared(data_dir.root(), &requirement) }
        .map_err(|e| e.to_string())?;
    if sinomo_runtime::module::parse_native_dylib_module_id(&provider.descriptor().module_id)
        .map_err(|e| e.to_string())?
        != module_id
    {
        return Err("panel provider identity changed".into());
    }
    if provider.descriptor().module_version != entry.module_version
        || !provider
            .descriptor()
            .surfaces
            .ui_contributions
            .contains(contribution)
    {
        return Err("panel contribution changed; refresh the catalog".into());
    }
    let response = provider
        .invoke_sobj(
            PANEL_RENDER,
            &request.render,
            Some(Arc::new(SessionHostCallbacks { sessions })),
        )
        .map_err(|e| e.to_string())?;
    let ActiveNativeComponentResponse::Value(response) = response else {
        let ActiveNativeComponentResponse::Error(error) = response else {
            unreachable!()
        };
        return Err(error.detail);
    };
    validate_response(&response)?;
    Ok(response)
}

pub struct SessionModuleService;
impl Service for SessionModuleService {
    fn name(&self) -> &'static str {
        "session-modules"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[PANEL_ROUTE, SESSION_ROUTE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .map(|route| {
                ServiceRoute::describe(
                    *route,
                    "session-modules",
                    "scoped session module access",
                    "session-request",
                    "session-response",
                )
                .with_auth_scope("local-command")
                .with_concurrency_limit(4)
                .with_timeout(std::time::Duration::from_secs(5))
            })
            .collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let data_dir = ctx.data_dir.clone();
            match request.route.as_str() {
                PANEL_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result =
                        tokio::task::spawn_blocking(move || render_panel(&data_dir, payload))
                            .await
                            .map_err(|e| ServiceError::Service(e.to_string()))?
                            .map_err(ServiceError::Service)?;
                    ctx.respond_value(PANEL_ROUTE, result)?;
                }
                SESSION_ROUTE => {
                    let payload: SessionAccessRequest = ctx.decode_request_body(&request).await?;
                    let result = tokio::task::spawn_blocking(move || {
                        AgentSessionAccess::for_conversation(&data_dir, &payload.conversation_id)?
                            .request(payload.request)
                    })
                    .await
                    .map_err(|e| ServiceError::Service(e.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(SESSION_ROUTE, result)?;
                }
                _ => return Err(ServiceError::UnknownRoute(request.route)),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}
