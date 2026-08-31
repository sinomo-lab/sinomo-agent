//! Daemon-owned application service for self-evolution execution and projections.

use sinomo_app_contract::evolution as contract;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

#[derive(Clone, Default)]
pub struct HostedEvolutionService;

impl Service for HostedEvolutionService {
    fn name(&self) -> &'static str {
        "evolution-app"
    }

    fn routes(&self) -> &'static [&'static str] {
        contract::EVOLUTION_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        contract::EVOLUTION_RPC_ROUTES
            .iter()
            .copied()
            .map(|route| {
                let capability = if route == contract::EVOLUTION_DASHBOARD_ROUTE {
                    contract::EVOLUTION_READ_CAPABILITY
                } else {
                    contract::EVOLUTION_WRITE_CAPABILITY
                };
                ServiceRoute::from_route_metadata(
                    route,
                    contract::EVOLUTION_RPC_CATEGORY,
                    contract::EVOLUTION_ROUTE_AUTH_SCOPE,
                    [capability],
                )
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            if !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "evolution app operations are available only to the local app".to_string(),
                ));
            }
            match request.route.as_str() {
                contract::EVOLUTION_DASHBOARD_ROUTE => {
                    let _: contract::EvolutionDashboardRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let dashboard = tokio::task::spawn_blocking(move || {
                        sinomo_evolution::app_evolution_dashboard(&data_dir)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(
                        contract::EVOLUTION_DASHBOARD_ROUTE,
                        contract::EvolutionDashboardResponse { dashboard },
                    )?;
                }
                contract::EVOLUTION_START_ROUTE => {
                    let payload: contract::EvolutionStartRequest =
                        ctx.decode_request_body(&request).await?;
                    tokio::task::spawn_blocking(move || {
                        sinomo_evolution::run_app_evolution_job(payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(
                        contract::EVOLUTION_START_ROUTE,
                        contract::EvolutionMutationResponse { accepted: true },
                    )?;
                }
                contract::EVOLUTION_DECISION_ROUTE => {
                    let payload: contract::EvolutionDecisionRequest =
                        ctx.decode_request_body(&request).await?;
                    tokio::task::spawn_blocking(move || {
                        sinomo_evolution::run_app_evolution_decision(payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(
                        contract::EVOLUTION_DECISION_ROUTE,
                        contract::EvolutionMutationResponse { accepted: true },
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_hosted_evolution_service(
    builder: ServiceRegistryBuilder,
) -> ServiceRegistryBuilder {
    builder.register(HostedEvolutionService)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolution_routes_are_local_and_capability_scoped() {
        let routes = HostedEvolutionService.route_categories();
        assert_eq!(routes.len(), contract::EVOLUTION_RPC_ROUTES.len());
        for route in routes {
            assert_eq!(
                route.auth_scope.as_deref(),
                Some(contract::EVOLUTION_ROUTE_AUTH_SCOPE)
            );
            assert_eq!(route.required_capabilities.len(), 1);
        }
    }
}
