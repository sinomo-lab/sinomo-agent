//! Daemon-owned application service for skill catalog management.

use sinomo_runtime::api::app::skill as contract;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::app_api;

#[derive(Clone)]
pub struct HostedSkillService {
    daemon_address: String,
}

impl HostedSkillService {
    pub fn new(daemon_address: impl Into<String>) -> Self {
        Self {
            daemon_address: daemon_address.into(),
        }
    }
}

impl Service for HostedSkillService {
    fn name(&self) -> &'static str {
        "skill-app"
    }

    fn routes(&self) -> &'static [&'static str] {
        contract::SKILL_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        contract::SKILL_RPC_ROUTES
            .iter()
            .copied()
            .map(|route| {
                let capability = if matches!(
                    route,
                    contract::SKILL_LIST_ROUTE | contract::SKILL_DETAIL_ROUTE
                ) {
                    contract::SKILL_READ_CAPABILITY
                } else {
                    contract::SKILL_WRITE_CAPABILITY
                };
                ServiceRoute::from_route_metadata(
                    route,
                    contract::SKILL_RPC_CATEGORY,
                    contract::SKILL_ROUTE_AUTH_SCOPE,
                    [capability],
                )
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let service = self.clone();
        Box::pin(async move {
            if !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "skill app operations are available only to the local app".to_string(),
                ));
            }
            let data_dir = ctx.data_dir.as_ref().clone();
            let runtime = tokio::runtime::Handle::current();
            match request.route.as_str() {
                contract::SKILL_LIST_ROUTE => {
                    let payload: contract::SkillListRequest =
                        ctx.decode_request_body(&request).await?;
                    let skills = tokio::task::spawn_blocking(move || {
                        let vfs = open_catalog_vfs(&service.daemon_address, &data_dir, &runtime)?;
                        app_api::skill_summaries(&vfs, payload.include_all)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::SKILL_LIST_ROUTE,
                        contract::SkillListResponse { skills },
                    )?;
                }
                contract::SKILL_DETAIL_ROUTE => {
                    let payload: contract::SkillDetailRequest =
                        ctx.decode_request_body(&request).await?;
                    let detail = tokio::task::spawn_blocking(move || {
                        let vfs = open_catalog_vfs(&service.daemon_address, &data_dir, &runtime)?;
                        app_api::skill_detail(&vfs, &payload.skill_id, payload.version.as_deref())
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::SKILL_DETAIL_ROUTE,
                        contract::SkillDetailResponse { detail },
                    )?;
                }
                contract::SKILL_IMPORT_ROUTE => {
                    let payload: contract::SkillImportRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = tokio::task::spawn_blocking(move || {
                        app_api::import_skill(
                            &data_dir,
                            &payload.path,
                            payload.skill_id,
                            payload.version,
                            payload.activate,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::SKILL_IMPORT_ROUTE,
                        contract::SkillMutationResponse { result },
                    )?;
                }
                contract::SKILL_EXPORT_ROUTE => {
                    let payload: contract::SkillExportRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = tokio::task::spawn_blocking(move || {
                        let vfs = open_catalog_vfs(&service.daemon_address, &data_dir, &runtime)?;
                        app_api::export_skill(
                            &vfs,
                            &payload.skill_id,
                            payload.version.as_deref(),
                            payload.output.as_deref(),
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::SKILL_EXPORT_ROUTE,
                        contract::SkillMutationResponse { result },
                    )?;
                }
                contract::SKILL_ACTIVATE_ROUTE => {
                    let payload: contract::SkillActivateRequest =
                        ctx.decode_request_body(&request).await?;
                    let result = tokio::task::spawn_blocking(move || {
                        app_api::activate_skill(
                            &data_dir,
                            &payload.skill_id,
                            payload.version.as_deref(),
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        contract::SKILL_ACTIVATE_ROUTE,
                        contract::SkillMutationResponse { result },
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn open_catalog_vfs(
    daemon_address: &str,
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_runtime::vfs::VfsHandle, String> {
    app_api::AgentChatSifsStorage::new(data_dir.clone(), daemon_address, runtime)
        .open_agent_catalog_vfs()
}

pub fn register_hosted_skill_service(
    builder: ServiceRegistryBuilder,
    daemon_address: impl Into<String>,
) -> ServiceRegistryBuilder {
    builder.register(HostedSkillService::new(daemon_address))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_routes_are_local_and_capability_scoped() {
        let routes = HostedSkillService::new("127.0.0.1:23572").route_categories();
        assert_eq!(routes.len(), contract::SKILL_RPC_ROUTES.len());
        for route in routes {
            assert_eq!(
                route.auth_scope.as_deref(),
                Some(contract::SKILL_ROUTE_AUTH_SCOPE)
            );
            assert_eq!(route.required_capabilities.len(), 1);
        }
    }
}
