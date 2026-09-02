//! Typed execution policies over a compiled extension registry generation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{
    ComponentDescriptor, ComponentServiceCallError, ComponentServiceClient,
    ExtensionRegistrySnapshot, ExtensionResolveError, ResolvedExtensionContribution,
    ResolvedExtensionOperation, ResolvedExtensionPoint, ServiceRequirement,
    ServiceRequirementLocality,
};
use sinomo_module_api::{
    ExtensionCompositionPolicy, ExtensionContext, ExtensionFailurePolicy, ExtensionLocality,
    ExtensionSelection, ExtensionVeto,
};

pub const MODULE_DIAGNOSTICS_EXTENSION_POINT_ID: &str = "sinomo.module.diagnostics.enrich";
pub const MODULE_DIAGNOSTICS_EXTENSION_POINT_VERSION: &str = "1.0.0";
pub const MODULE_DIAGNOSTICS_INPUT_SCHEMA_ID: &str = "sinomo.module.diagnostics.input/v1";
pub const MODULE_DIAGNOSTIC_CONTRIBUTION_SCHEMA_ID: &str =
    "sinomo.module.diagnostics.contribution/v1";
pub const MODULE_DIAGNOSTICS_REPORT_SCHEMA_ID: &str = "sinomo.module.diagnostics.report/v1";

#[derive(Clone)]
pub struct ExtensionExecutor {
    caller: ComponentDescriptor,
    registry: Arc<ExtensionRegistrySnapshot>,
    data_dir: Arc<sinomo_core::local::SinomoDataDir>,
    services: sinomo_core::service::ServiceRegistrySnapshotStore,
    invocation_leases: Option<crate::ModuleInvocationLeaseRegistry>,
}

impl ExtensionExecutor {
    pub fn new(
        caller: ComponentDescriptor,
        registry: Arc<ExtensionRegistrySnapshot>,
        data_dir: Arc<sinomo_core::local::SinomoDataDir>,
        services: sinomo_core::service::ServiceRegistrySnapshotStore,
    ) -> Self {
        Self {
            caller,
            registry,
            data_dir,
            services,
            invocation_leases: None,
        }
    }

    pub fn with_invocation_leases(
        mut self,
        invocation_leases: crate::ModuleInvocationLeaseRegistry,
    ) -> Self {
        self.invocation_leases = Some(invocation_leases);
        self
    }

    pub fn registry(&self) -> &ExtensionRegistrySnapshot {
        self.registry.as_ref()
    }

    pub async fn collect<Request, Contribution>(
        &self,
        point: &ResolvedExtensionPoint,
        request: &Request,
        context: &ExtensionContext,
        invocation: &ExtensionInvocationContext,
    ) -> Result<ExtensionCollection<Contribution>, ExtensionExecutionError>
    where
        Request: Serialize,
        Contribution: DeserializeOwned + Send + 'static,
    {
        self.require_policy(point, ExtensionCompositionPolicy::Collect)?;
        self.collect_for_policies(
            point,
            request,
            context,
            invocation,
            &[
                ExtensionCompositionPolicy::Collect,
                ExtensionCompositionPolicy::Reduce,
            ],
        )
        .await
    }

    pub async fn select_one<Request, Output>(
        &self,
        point: &ResolvedExtensionPoint,
        request: &Request,
        context: &ExtensionContext,
        selection: &ExtensionSelection,
        invocation: &ExtensionInvocationContext,
    ) -> Result<AttributedExtensionValue<Output>, ExtensionExecutionError>
    where
        Request: Serialize,
        Output: Serialize + DeserializeOwned + Send + 'static,
    {
        self.require_owner(point)?;
        let contribution = self
            .registry
            .select_one(point, context, Some(selection))
            .map_err(ExtensionExecutionError::Resolve)?;
        let child = invocation.enter(point, contribution)?;
        let value = self
            .invoke_contribution(point, contribution, request, &child)
            .await
            .map_err(|failure| ExtensionExecutionError::PointFailed {
                point_id: point.definition.point_id.clone(),
                failures: vec![failure],
            })?;
        self.ensure_response_bound(point, &value.value)?;
        Ok(value)
    }

    pub async fn reduce<Request, Contribution, Output, Reducer>(
        &self,
        point: &ResolvedExtensionPoint,
        request: &Request,
        context: &ExtensionContext,
        invocation: &ExtensionInvocationContext,
        mut output: Output,
        mut reducer: Reducer,
    ) -> Result<ExtensionReduction<Output>, ExtensionExecutionError>
    where
        Request: Serialize,
        Contribution: DeserializeOwned + Send + 'static,
        Output: Serialize,
        Reducer: FnMut(&mut Output, &AttributedExtensionValue<Contribution>) -> Result<(), String>,
    {
        self.require_policy(point, ExtensionCompositionPolicy::Reduce)?;
        let collection = self
            .collect_for_policies::<Request, Contribution>(
                point,
                request,
                context,
                invocation,
                &[ExtensionCompositionPolicy::Reduce],
            )
            .await?;
        let mut applied = Vec::new();
        for value in &collection.values {
            reducer(&mut output, value).map_err(|detail| {
                ExtensionExecutionError::ReducerRejected {
                    point_id: point.definition.point_id.clone(),
                    contribution_id: value.contribution_id.clone(),
                    detail,
                }
            })?;
            self.ensure_response_bound(point, &output)?;
            applied.push(value.contribution_id.clone());
        }
        self.ensure_response_bound(point, &output)?;
        Ok(ExtensionReduction {
            output,
            applied_contributions: applied,
            failures: collection.failures,
        })
    }

    pub async fn pipeline<State, Delta, Apply>(
        &self,
        point: &ResolvedExtensionPoint,
        context: &ExtensionContext,
        invocation: &ExtensionInvocationContext,
        mut state: State,
        mut apply_delta: Apply,
    ) -> Result<ExtensionPipelineResult<State>, ExtensionExecutionError>
    where
        State: Serialize,
        Delta: DeserializeOwned + Send + 'static,
        Apply: FnMut(&mut State, AttributedExtensionValue<Delta>) -> Result<(), String>,
    {
        self.require_owner(point)?;
        self.require_policy(point, ExtensionCompositionPolicy::Pipeline)?;
        self.ensure_request_bound(point, &state)?;
        let mut applied = Vec::new();
        let mut failures = Vec::new();
        for contribution in self.registry.eligible(point, context) {
            let child = invocation.enter(point, contribution)?;
            match self
                .invoke_contribution::<State, Delta>(point, contribution, &state, &child)
                .await
            {
                Ok(delta) => {
                    let contribution_id = delta.contribution_id.clone();
                    apply_delta(&mut state, delta).map_err(|detail| {
                        ExtensionExecutionError::DeltaRejected {
                            point_id: point.definition.point_id.clone(),
                            contribution_id: contribution_id.clone(),
                            detail,
                        }
                    })?;
                    self.ensure_request_bound(point, &state)?;
                    self.ensure_response_bound(point, &state)?;
                    applied.push(contribution_id);
                }
                Err(failure) => {
                    if fail_point_on_provider_error(point.definition.failure_policy) {
                        return Err(ExtensionExecutionError::PointFailed {
                            point_id: point.definition.point_id.clone(),
                            failures: vec![failure],
                        });
                    }
                    failures.push(failure);
                }
            }
        }
        self.ensure_response_bound(point, &state)?;
        Ok(ExtensionPipelineResult {
            state,
            applied_contributions: applied,
            failures,
        })
    }

    pub async fn veto<Request>(
        &self,
        point: &ResolvedExtensionPoint,
        request: &Request,
        context: &ExtensionContext,
        invocation: &ExtensionInvocationContext,
    ) -> Result<ExtensionVetoOutcome, ExtensionExecutionError>
    where
        Request: Serialize,
    {
        self.require_owner(point)?;
        self.require_policy(point, ExtensionCompositionPolicy::Veto)?;
        if point.definition.failure_policy != ExtensionFailurePolicy::FailClosed {
            return Err(ExtensionExecutionError::UnsafeVetoPoint {
                point_id: point.definition.point_id.clone(),
            });
        }
        let mut denials = Vec::new();
        let mut failures = Vec::new();
        let mut decision_leases = Vec::new();
        for contribution in self.registry.eligible(point, context) {
            let child = match invocation.enter(point, contribution) {
                Ok(child) => child,
                Err(error) => {
                    let failure = ExtensionInvocationFailure {
                        module_id: contribution.module_id.clone(),
                        contribution_id: contribution.contribution.contribution_id.clone(),
                        provider_component_id: None,
                        duration_us: 0,
                        detail: error.to_string(),
                    };
                    denials.push(fail_closed_denial(&failure));
                    failures.push(failure);
                    continue;
                }
            };
            match self
                .invoke_contribution::<Request, ExtensionVeto>(point, contribution, request, &child)
                .await
            {
                Ok(value) => {
                    if let Err(error) = value.value.validate() {
                        let failure = ExtensionInvocationFailure {
                            module_id: value.module_id.clone(),
                            contribution_id: value.contribution_id.clone(),
                            provider_component_id: value.provider_component_id.clone(),
                            duration_us: value.duration_us,
                            detail: format!("invalid veto result: {error}"),
                        };
                        denials.push(fail_closed_denial(&failure));
                        failures.push(failure);
                        continue;
                    }
                    if value.value.denied {
                        denials.push(value);
                    } else {
                        decision_leases.extend(value.invocation_leases);
                    }
                }
                Err(failure) => {
                    denials.push(fail_closed_denial(&failure));
                    failures.push(failure);
                }
            }
        }
        Ok(ExtensionVetoOutcome {
            denied: !denials.is_empty(),
            denials,
            failures,
            _invocation_leases: decision_leases,
        })
    }

    pub async fn collect_module_diagnostics(
        &self,
        input: &ModuleDiagnosticsInput,
        context: &ExtensionContext,
        invocation: &ExtensionInvocationContext,
    ) -> Result<ModuleDiagnosticsReport, ExtensionExecutionError> {
        let point = self
            .registry
            .latest_point(MODULE_DIAGNOSTICS_EXTENSION_POINT_ID)
            .ok_or_else(|| ExtensionExecutionError::PointUnavailable {
                point_id: MODULE_DIAGNOSTICS_EXTENSION_POINT_ID.to_string(),
            })?;
        let collection = self
            .collect::<_, ModuleDiagnosticContribution>(point, input, context, invocation)
            .await?;
        Ok(ModuleDiagnosticsReport {
            generation: self.registry.generation.module_registry_generation,
            diagnostics: collection.values,
            failures: collection.failures,
        })
    }

    async fn collect_for_policies<Request, Contribution>(
        &self,
        point: &ResolvedExtensionPoint,
        request: &Request,
        context: &ExtensionContext,
        invocation: &ExtensionInvocationContext,
        allowed_policies: &[ExtensionCompositionPolicy],
    ) -> Result<ExtensionCollection<Contribution>, ExtensionExecutionError>
    where
        Request: Serialize,
        Contribution: DeserializeOwned + Send + 'static,
    {
        self.require_owner(point)?;
        if !allowed_policies.contains(&point.definition.composition) {
            return Err(ExtensionExecutionError::WrongPolicy {
                point_id: point.definition.point_id.clone(),
                expected: allowed_policies.to_vec(),
                actual: point.definition.composition,
            });
        }
        self.ensure_request_bound(point, request)?;
        let mut values = Vec::new();
        let mut failures = Vec::new();
        for contribution in self.registry.eligible(point, context) {
            let child = invocation.enter(point, contribution)?;
            match self
                .invoke_contribution(point, contribution, request, &child)
                .await
            {
                Ok(value) => values.push(value),
                Err(failure) => {
                    if fail_point_on_provider_error(point.definition.failure_policy) {
                        return Err(ExtensionExecutionError::PointFailed {
                            point_id: point.definition.point_id.clone(),
                            failures: vec![failure],
                        });
                    }
                    failures.push(failure);
                }
            }
        }
        Ok(ExtensionCollection { values, failures })
    }

    async fn invoke_contribution<Request, Output>(
        &self,
        point: &ResolvedExtensionPoint,
        contribution: &ResolvedExtensionContribution,
        request: &Request,
        invocation: &ExtensionInvocationContext,
    ) -> Result<AttributedExtensionValue<Output>, ExtensionInvocationFailure>
    where
        Request: Serialize,
        Output: DeserializeOwned + Send + 'static,
    {
        let mut invocation_leases = Vec::new();
        if let Some(leases) = &self.invocation_leases {
            let mut identities = vec![point.owner_id.as_str(), contribution.module_id.as_str()];
            identities.dedup();
            for identity in identities {
                if let Some(lease) = leases.acquire_if_tracked(identity).map_err(|error| {
                    ExtensionInvocationFailure {
                        module_id: contribution.module_id.clone(),
                        contribution_id: contribution.contribution.contribution_id.clone(),
                        provider_component_id: None,
                        duration_us: 0,
                        detail: error.to_string(),
                    }
                })? {
                    invocation_leases.push(lease);
                }
            }
        }
        if let Some(operation) = &contribution.operation {
            let mut value = self
                .invoke_operation(point, contribution, operation, request, invocation)
                .await?;
            value.invocation_leases = invocation_leases;
            return Ok(value);
        }
        let started = Instant::now();
        let value =
            sinomo_module_api::decode_sobj::<Output>(&contribution.contribution.payload_sobj)
                .map_err(|error| ExtensionInvocationFailure {
                    module_id: contribution.module_id.clone(),
                    contribution_id: contribution.contribution.contribution_id.clone(),
                    provider_component_id: None,
                    duration_us: duration_us(started.elapsed()),
                    detail: format!("decode declarative contribution: {error}"),
                })?;
        Ok(AttributedExtensionValue {
            module_id: contribution.module_id.clone(),
            contribution_id: contribution.contribution.contribution_id.clone(),
            provider_component_id: None,
            duration_us: duration_us(started.elapsed()),
            value,
            invocation_leases,
        })
    }

    async fn invoke_operation<Request, Output>(
        &self,
        point: &ResolvedExtensionPoint,
        contribution: &ResolvedExtensionContribution,
        operation: &ResolvedExtensionOperation,
        request: &Request,
        invocation: &ExtensionInvocationContext,
    ) -> Result<AttributedExtensionValue<Output>, ExtensionInvocationFailure>
    where
        Request: Serialize,
        Output: DeserializeOwned + Send + 'static,
    {
        let requirement = self
            .owner_requirement(point, operation)
            .ok_or_else(|| ExtensionInvocationFailure {
                module_id: contribution.module_id.clone(),
                contribution_id: contribution.contribution.contribution_id.clone(),
                provider_component_id: None,
                duration_us: 0,
                detail: format!(
                    "extension point owner `{}` did not declare authority for `{}` {} operation `{}`",
                    self.caller.component_id,
                    operation.interface_id,
                    operation.interface_version,
                    operation.logical_operation,
                ),
            })?
            .clone();
        let client = ComponentServiceClient::new(
            self.caller.clone(),
            Arc::clone(&self.data_dir),
            self.services.clone(),
        );
        let remaining = invocation
            .remaining()
            .ok_or_else(|| ExtensionInvocationFailure {
                module_id: contribution.module_id.clone(),
                contribution_id: contribution.contribution.contribution_id.clone(),
                provider_component_id: None,
                duration_us: 0,
                detail: "extension invocation deadline elapsed".to_string(),
            })?;
        let deadline = remaining
            .min(Duration::from_millis(point.definition.max_deadline_ms))
            .min(Duration::from_millis(operation.max_deadline_ms));
        let response_limit = point
            .definition
            .max_response_bytes
            .min(operation.max_response_bytes)
            .min(usize::MAX as u64) as usize;
        let calls = client
            .call_local_all::<_, Output>(
                &requirement,
                &operation.logical_operation,
                request,
                deadline,
                response_limit,
            )
            .await
            .map_err(|error| ExtensionInvocationFailure {
                module_id: contribution.module_id.clone(),
                contribution_id: contribution.contribution.contribution_id.clone(),
                provider_component_id: None,
                duration_us: 0,
                detail: error.to_string(),
            })?;
        let mut matching = calls.into_iter().filter_map(|call| match call {
            Ok(response) if response.provider.component_id == contribution.module_id => {
                Some(Ok(response))
            }
            Err(error) if error.provider.component_id == contribution.module_id => Some(Err(error)),
            _ => None,
        });
        let first = matching.next().ok_or_else(|| ExtensionInvocationFailure {
            module_id: contribution.module_id.clone(),
            contribution_id: contribution.contribution.contribution_id.clone(),
            provider_component_id: None,
            duration_us: 0,
            detail: "the selected contribution provider is not active".to_string(),
        })?;
        if matching.next().is_some() {
            return Err(ExtensionInvocationFailure {
                module_id: contribution.module_id.clone(),
                contribution_id: contribution.contribution.contribution_id.clone(),
                provider_component_id: Some(contribution.module_id.clone()),
                duration_us: 0,
                detail: "the contribution resolves to more than one active provider".to_string(),
            });
        }
        match first {
            Ok(response) => Ok(AttributedExtensionValue {
                module_id: contribution.module_id.clone(),
                contribution_id: contribution.contribution.contribution_id.clone(),
                provider_component_id: Some(response.provider.component_id),
                duration_us: response.duration_us,
                value: response.value,
                invocation_leases: Vec::new(),
            }),
            Err(error) => Err(call_failure(contribution, error)),
        }
    }

    fn require_policy(
        &self,
        point: &ResolvedExtensionPoint,
        expected: ExtensionCompositionPolicy,
    ) -> Result<(), ExtensionExecutionError> {
        if point.definition.composition == expected {
            Ok(())
        } else {
            Err(ExtensionExecutionError::WrongPolicy {
                point_id: point.definition.point_id.clone(),
                expected: vec![expected],
                actual: point.definition.composition,
            })
        }
    }

    fn require_owner(&self, point: &ResolvedExtensionPoint) -> Result<(), ExtensionExecutionError> {
        if self.caller.component_id == point.owner_id
            && self.caller.component_version == point.owner_version
        {
            Ok(())
        } else {
            Err(ExtensionExecutionError::OwnerMismatch {
                point_id: point.definition.point_id.clone(),
                expected_owner: format!("{}@{}", point.owner_id, point.owner_version),
                actual_owner: format!(
                    "{}@{}",
                    self.caller.component_id, self.caller.component_version
                ),
            })
        }
    }

    fn owner_requirement<'a>(
        &'a self,
        point: &ResolvedExtensionPoint,
        operation: &ResolvedExtensionOperation,
    ) -> Option<&'a ServiceRequirement> {
        if self.caller.component_id != point.owner_id
            || self.caller.component_version != point.owner_version
        {
            return None;
        }
        self.caller.requirements.iter().find(|requirement| {
            let locality_matches = match point.definition.locality {
                ExtensionLocality::Local => matches!(
                    requirement.locality,
                    ServiceRequirementLocality::Local | ServiceRequirementLocality::Any
                ),
                ExtensionLocality::Cluster => matches!(
                    requirement.locality,
                    ServiceRequirementLocality::Cluster | ServiceRequirementLocality::Any
                ),
                ExtensionLocality::Any => true,
            };
            requirement.interface_id == operation.interface_id
                && (requirement.interface_version == "*"
                    || requirement.interface_version == operation.interface_version)
                && requirement
                    .service_id
                    .as_ref()
                    .is_none_or(|service_id| service_id == &operation.service_id)
                && requirement
                    .operations
                    .iter()
                    .any(|declared| declared == &operation.logical_operation)
                && requirement
                    .caller_capabilities
                    .iter()
                    .any(|capability| capability == &operation.logical_operation)
                && point
                    .definition
                    .apply_capabilities
                    .iter()
                    .all(|capability| requirement.caller_capabilities.contains(capability))
                && locality_matches
        })
    }

    fn ensure_request_bound<Request: Serialize>(
        &self,
        point: &ResolvedExtensionPoint,
        request: &Request,
    ) -> Result<(), ExtensionExecutionError> {
        let bytes = sinomo_module_api::encode_sobj(request).map_err(|error| {
            ExtensionExecutionError::RequestEncode {
                point_id: point.definition.point_id.clone(),
                detail: error.to_string(),
            }
        })?;
        if bytes.len() as u64 > point.definition.max_request_bytes {
            return Err(ExtensionExecutionError::RequestTooLarge {
                point_id: point.definition.point_id.clone(),
                actual: bytes.len() as u64,
                limit: point.definition.max_request_bytes,
            });
        }
        Ok(())
    }

    fn ensure_response_bound<Response: Serialize>(
        &self,
        point: &ResolvedExtensionPoint,
        response: &Response,
    ) -> Result<(), ExtensionExecutionError> {
        let bytes = sinomo_module_api::encode_sobj(response).map_err(|error| {
            ExtensionExecutionError::ResponseEncode {
                point_id: point.definition.point_id.clone(),
                detail: error.to_string(),
            }
        })?;
        if bytes.len() as u64 > point.definition.max_response_bytes {
            return Err(ExtensionExecutionError::ResponseTooLarge {
                point_id: point.definition.point_id.clone(),
                actual: bytes.len() as u64,
                limit: point.definition.max_response_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ExtensionInvocationContext {
    deadline: Instant,
    max_depth: usize,
    stack: Vec<(String, String)>,
}

impl ExtensionInvocationContext {
    pub fn new(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            max_depth: 16,
            stack: Vec::new(),
        }
    }

    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth.max(1);
        self
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }

    fn enter(
        &self,
        point: &ResolvedExtensionPoint,
        contribution: &ResolvedExtensionContribution,
    ) -> Result<Self, ExtensionExecutionError> {
        let frame = (
            point.definition.point_id.clone(),
            contribution.contribution.contribution_id.clone(),
        );
        if self.stack.contains(&frame) {
            return Err(ExtensionExecutionError::InvocationCycle {
                point_id: frame.0,
                contribution_id: frame.1,
            });
        }
        if self.stack.len() >= self.max_depth {
            return Err(ExtensionExecutionError::InvocationDepthExceeded {
                max_depth: self.max_depth,
            });
        }
        if self.remaining().is_none() {
            return Err(ExtensionExecutionError::DeadlineElapsed {
                point_id: point.definition.point_id.clone(),
            });
        }
        let mut child = self.clone();
        child.stack.push(frame);
        Ok(child)
    }
}

#[derive(Clone, Debug)]
pub struct AttributedExtensionValue<T> {
    pub module_id: String,
    pub contribution_id: String,
    pub provider_component_id: Option<String>,
    pub duration_us: u64,
    pub value: T,
    invocation_leases: Vec<crate::ModuleInvocationLease>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionInvocationFailure {
    pub module_id: String,
    pub contribution_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_component_id: Option<String>,
    pub duration_us: u64,
    pub detail: String,
}

#[derive(Clone, Debug)]
pub struct ExtensionCollection<T> {
    pub values: Vec<AttributedExtensionValue<T>>,
    pub failures: Vec<ExtensionInvocationFailure>,
}

#[derive(Clone, Debug)]
pub struct ExtensionReduction<T> {
    pub output: T,
    pub applied_contributions: Vec<String>,
    pub failures: Vec<ExtensionInvocationFailure>,
}

#[derive(Clone, Debug)]
pub struct ExtensionPipelineResult<T> {
    pub state: T,
    pub applied_contributions: Vec<String>,
    pub failures: Vec<ExtensionInvocationFailure>,
}

#[derive(Clone, Debug)]
pub struct ExtensionVetoOutcome {
    pub denied: bool,
    pub denials: Vec<AttributedExtensionValue<ExtensionVeto>>,
    pub failures: Vec<ExtensionInvocationFailure>,
    _invocation_leases: Vec<crate::ModuleInvocationLease>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleDiagnosticsInput {
    pub subject: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facts: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleDiagnosticContribution {
    pub code: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ModuleDiagnosticsReport {
    pub generation: u64,
    pub diagnostics: Vec<AttributedExtensionValue<ModuleDiagnosticContribution>>,
    pub failures: Vec<ExtensionInvocationFailure>,
}

pub fn module_diagnostics_extension_point_definition() -> sinomo_module_api::ExtensionPointDefinition
{
    sinomo_module_api::ExtensionPointDefinition::new(
        MODULE_DIAGNOSTICS_EXTENSION_POINT_ID,
        MODULE_DIAGNOSTICS_EXTENSION_POINT_VERSION,
        MODULE_DIAGNOSTICS_INPUT_SCHEMA_ID,
        MODULE_DIAGNOSTIC_CONTRIBUTION_SCHEMA_ID,
        MODULE_DIAGNOSTICS_REPORT_SCHEMA_ID,
        ExtensionCompositionPolicy::Collect,
    )
    .operation_backed()
    .with_operation_contract(
        "sinomo.module.diagnostics-enricher",
        "1.0.0",
        "sinomo.module.diagnostics.enrich",
    )
    .with_failure_policy(ExtensionFailurePolicy::Partial)
    .with_limits(32, 256 * 1024, 256 * 1024, 1_000)
    .require_cancellation()
}

#[derive(Clone, Debug, Error)]
pub enum ExtensionExecutionError {
    #[error("extension point `{point_id}` is unavailable")]
    PointUnavailable { point_id: String },
    #[error("extension point `{point_id}` is owned by `{expected_owner}`, not `{actual_owner}`")]
    OwnerMismatch {
        point_id: String,
        expected_owner: String,
        actual_owner: String,
    },
    #[error("extension point `{point_id}` uses {actual:?}; expected one of {expected:?}")]
    WrongPolicy {
        point_id: String,
        expected: Vec<ExtensionCompositionPolicy>,
        actual: ExtensionCompositionPolicy,
    },
    #[error(transparent)]
    Resolve(#[from] ExtensionResolveError),
    #[error("extension point `{point_id}` request encoding failed: {detail}")]
    RequestEncode { point_id: String, detail: String },
    #[error("extension point `{point_id}` request is {actual} bytes; limit is {limit}")]
    RequestTooLarge {
        point_id: String,
        actual: u64,
        limit: u64,
    },
    #[error("extension point `{point_id}` response encoding failed: {detail}")]
    ResponseEncode { point_id: String, detail: String },
    #[error("extension point `{point_id}` response is {actual} bytes; limit is {limit}")]
    ResponseTooLarge {
        point_id: String,
        actual: u64,
        limit: u64,
    },
    #[error("extension point `{point_id}` failed")]
    PointFailed {
        point_id: String,
        failures: Vec<ExtensionInvocationFailure>,
    },
    #[error("trusted reducer for `{point_id}` rejected `{contribution_id}`: {detail}")]
    ReducerRejected {
        point_id: String,
        contribution_id: String,
        detail: String,
    },
    #[error("trusted pipeline validator for `{point_id}` rejected `{contribution_id}`: {detail}")]
    DeltaRejected {
        point_id: String,
        contribution_id: String,
        detail: String,
    },
    #[error("veto point `{point_id}` is not fail-closed")]
    UnsafeVetoPoint { point_id: String },
    #[error("extension invocation cycle at `{point_id}` / `{contribution_id}`")]
    InvocationCycle {
        point_id: String,
        contribution_id: String,
    },
    #[error("extension invocation depth exceeds {max_depth}")]
    InvocationDepthExceeded { max_depth: usize },
    #[error("extension point `{point_id}` deadline elapsed")]
    DeadlineElapsed { point_id: String },
}

fn call_failure(
    contribution: &ResolvedExtensionContribution,
    error: ComponentServiceCallError,
) -> ExtensionInvocationFailure {
    ExtensionInvocationFailure {
        module_id: contribution.module_id.clone(),
        contribution_id: contribution.contribution.contribution_id.clone(),
        provider_component_id: Some(error.provider.component_id),
        duration_us: error.duration_us,
        detail: error.detail,
    }
}

fn fail_closed_denial(
    failure: &ExtensionInvocationFailure,
) -> AttributedExtensionValue<ExtensionVeto> {
    AttributedExtensionValue {
        module_id: failure.module_id.clone(),
        contribution_id: failure.contribution_id.clone(),
        provider_component_id: failure.provider_component_id.clone(),
        duration_us: failure.duration_us,
        value: ExtensionVeto::deny(
            "extension-provider-failed",
            format!("fail-closed extension provider failure: {}", failure.detail),
        ),
        invocation_leases: Vec::new(),
    }
}

fn fail_point_on_provider_error(policy: ExtensionFailurePolicy) -> bool {
    matches!(
        policy,
        ExtensionFailurePolicy::FailPoint | ExtensionFailurePolicy::FailClosed
    )
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExtensionRegistryGeneration, ResolvedExtensionOperation, ResolvedExtensionPoint};
    use sinomo_core::service::{
        ServiceDiscoveryHealth, ServiceDiscoveryIdentity, ServiceDiscoveryLocality,
        ServiceDisposition, ServiceRegistry, ServiceRoute,
    };
    use sinomo_module_api::{
        ExtensionContribution, ExtensionContributionKind, ExtensionOperationReference,
        ExtensionPointDefinition, ExtensionSelectionPolicy, ExtensionVersionRequirement,
    };

    fn resolved_point(
        policy: ExtensionCompositionPolicy,
        failure_policy: ExtensionFailurePolicy,
        contributions: Vec<ResolvedExtensionContribution>,
    ) -> ResolvedExtensionPoint {
        let mut definition = ExtensionPointDefinition::new(
            "fixture.point",
            "1.0.0",
            "fixture.request/v1",
            if policy == ExtensionCompositionPolicy::Veto {
                sinomo_module_api::EXTENSION_VETO_SCHEMA_ID
            } else {
                "fixture.contribution/v1"
            },
            "fixture.output/v1",
            policy,
        )
        .allow_kind(ExtensionContributionKind::Operation)
        .with_operation_contract("fixture.provider", "1.0.0", "fixture.contribute")
        .with_failure_policy(failure_policy)
        .with_limits(8, 1024, 1024, 500);
        if policy == ExtensionCompositionPolicy::SelectOne {
            definition.selection_policy = ExtensionSelectionPolicy::Explicit;
        }
        ResolvedExtensionPoint {
            owner_id: "fixture.owner".to_string(),
            owner_version: "1.0.0".to_string(),
            definition,
            contributions,
        }
    }

    fn operation_contribution(module: &str) -> ResolvedExtensionContribution {
        ResolvedExtensionContribution {
            module_id: module.to_string(),
            module_version: "1.0.0".to_string(),
            module_backend: "native-rust".to_string(),
            artifact_id: None,
            contribution: ExtensionContribution::operation(
                format!("{module}.contribution"),
                "fixture.point",
                ExtensionVersionRequirement::exact("1.0.0"),
                ExtensionOperationReference::new(format!("{module}.service"), "fixture.contribute"),
            ),
            operation: Some(ResolvedExtensionOperation {
                service_id: format!("{module}.service"),
                interface_id: "fixture.provider".to_string(),
                interface_version: "1.0.0".to_string(),
                logical_operation: "fixture.contribute".to_string(),
                request_schema_id: "fixture.request/v1".to_string(),
                response_schema_id: "fixture.contribution/v1".to_string(),
                max_request_bytes: 1024,
                max_response_bytes: 1024,
                max_deadline_ms: 500,
                supports_cancellation: true,
            }),
            legacy_ui: false,
        }
    }

    fn registry_with_point(point: ResolvedExtensionPoint) -> Arc<ExtensionRegistrySnapshot> {
        Arc::new(ExtensionRegistrySnapshot {
            generation: ExtensionRegistryGeneration {
                module_registry_generation: 7,
            },
            generated_at: "test".to_string(),
            points: vec![point],
            diagnostics: Vec::new(),
        })
    }

    fn service_registry<Response>(
        module: &str,
        response: Response,
    ) -> sinomo_core::service::ServiceRegistrySnapshotStore
    where
        Response: Serialize + Clone + Send + Sync + 'static,
    {
        let service_id = format!("{module}.service");
        let component_id = module.to_string();
        let route = ServiceRoute::new(format!("/{module}/contribute"), "fixture")
            .with_auth_scope("component")
            .require_capability("fixture.contribute")
            .with_discovery_identity(ServiceDiscoveryIdentity {
                service_id,
                interface_id: "fixture.provider".to_string(),
                interface_version: "1.0.0".to_string(),
                provider_capabilities: vec!["fixture.contribute".to_string()],
                caller_requirements: vec!["fixture.contribute".to_string()],
                component_id,
                component_version: "1.0.0".to_string(),
                artifact_id: None,
                component_generation: 1,
                health: ServiceDiscoveryHealth::Ready,
                locality: ServiceDiscoveryLocality::Local,
            });
        let registry = ServiceRegistry::builder()
            .register_dynamic("fixture", route, move |mut context, request| {
                let response = response.clone();
                async move {
                    let _: serde_json::Value = context.decode_request_body(&request).await?;
                    context.respond_value(request.route, response)?;
                    Ok(ServiceDisposition::Continue)
                }
            })
            .build();
        sinomo_core::service::ServiceRegistrySnapshotStore::new(registry)
    }

    fn executor<Response>(point: ResolvedExtensionPoint, response: Response) -> ExtensionExecutor
    where
        Response: Serialize + Clone + Send + Sync + 'static,
    {
        let module = point.contributions[0].module_id.clone();
        ExtensionExecutor::new(
            owner_descriptor(&point),
            registry_with_point(point),
            Arc::new(sinomo_core::local::SinomoDataDir::new(
                sinomo_testdir::TestDir::new("extension-executor").path(),
            )),
            service_registry(&module, response),
        )
    }

    fn owner_descriptor(point: &ResolvedExtensionPoint) -> ComponentDescriptor {
        let operation = point.contributions[0]
            .operation
            .as_ref()
            .expect("operation contribution");
        let mut requirement = ServiceRequirement::new(
            operation.interface_id.clone(),
            operation.interface_version.clone(),
        )
        .with_operation(operation.logical_operation.clone())
        .with_caller_capability(operation.logical_operation.clone());
        requirement.locality = match point.definition.locality {
            ExtensionLocality::Local => ServiceRequirementLocality::Local,
            ExtensionLocality::Cluster => ServiceRequirementLocality::Cluster,
            ExtensionLocality::Any => ServiceRequirementLocality::Any,
        };
        for capability in &point.definition.apply_capabilities {
            requirement = requirement.with_caller_capability(capability.clone());
        }
        ComponentDescriptor::new(&point.owner_id, &point.owner_version).require(requirement)
    }

    #[tokio::test]
    async fn collect_returns_attributed_provider_results() {
        let contribution = operation_contribution("fixture.provider");
        let point = resolved_point(
            ExtensionCompositionPolicy::Collect,
            ExtensionFailurePolicy::Partial,
            vec![contribution],
        );
        let executor = executor(point.clone(), 42_u64);
        let result = executor
            .collect::<_, u64>(
                &point,
                &serde_json::json!({"subject": "test"}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("collect");
        assert_eq!(result.values.len(), 1);
        assert_eq!(result.values[0].value, 42);
        assert_eq!(
            result.values[0].provider_component_id.as_deref(),
            Some("fixture.provider")
        );
    }

    #[tokio::test]
    async fn operation_execution_cannot_synthesize_owner_authority() {
        let contribution = operation_contribution("fixture.provider");
        let point = resolved_point(
            ExtensionCompositionPolicy::Collect,
            ExtensionFailurePolicy::Partial,
            vec![contribution],
        );
        let wrong_owner = ExtensionExecutor::new(
            ComponentDescriptor::new("fixture.other-owner", "1.0.0"),
            registry_with_point(point.clone()),
            Arc::new(sinomo_core::local::SinomoDataDir::new(
                sinomo_testdir::TestDir::new("extension-wrong-owner").path(),
            )),
            service_registry("fixture.provider", 42_u64),
        );
        assert!(matches!(
            wrong_owner
                .collect::<_, u64>(
                    &point,
                    &serde_json::json!({}),
                    &ExtensionContext::default(),
                    &ExtensionInvocationContext::new(Duration::from_secs(1)),
                )
                .await,
            Err(ExtensionExecutionError::OwnerMismatch { .. })
        ));

        let unauthorized = ExtensionExecutor::new(
            ComponentDescriptor::new(&point.owner_id, &point.owner_version),
            registry_with_point(point.clone()),
            Arc::new(sinomo_core::local::SinomoDataDir::new(
                sinomo_testdir::TestDir::new("extension-unauthorized-owner").path(),
            )),
            service_registry("fixture.provider", 42_u64),
        );
        let result = unauthorized
            .collect::<_, u64>(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("partial collection");
        assert!(result.values.is_empty());
        assert_eq!(result.failures.len(), 1);
        assert!(
            result.failures[0]
                .detail
                .contains("did not declare authority")
        );
    }

    #[tokio::test]
    async fn select_one_requires_and_honors_explicit_selection() {
        let contribution = operation_contribution("fixture.provider");
        let point = resolved_point(
            ExtensionCompositionPolicy::SelectOne,
            ExtensionFailurePolicy::FailPoint,
            vec![contribution],
        );
        let executor = executor(point.clone(), "selected".to_string());
        let selection = ExtensionSelection {
            point_id: point.definition.point_id.clone(),
            point_version: point.definition.point_version.clone(),
            contribution_id: "fixture.provider.contribution".to_string(),
        };
        let selected = executor
            .select_one::<_, String>(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &selection,
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("selected provider");
        assert_eq!(selected.value, "selected");
    }

    #[tokio::test]
    async fn trusted_reducer_and_pipeline_validate_each_contribution() {
        let contribution = operation_contribution("fixture.provider");
        let reduce_point = resolved_point(
            ExtensionCompositionPolicy::Reduce,
            ExtensionFailurePolicy::FailPoint,
            vec![contribution.clone()],
        );
        let reducer = executor(reduce_point.clone(), 2_i64);
        let reduced = reducer
            .reduce::<_, i64, i64, _>(
                &reduce_point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
                10,
                |state, value| {
                    *state += value.value;
                    Ok(())
                },
            )
            .await
            .expect("reduce");
        assert_eq!(reduced.output, 12);

        let pipeline_point = resolved_point(
            ExtensionCompositionPolicy::Pipeline,
            ExtensionFailurePolicy::FailPoint,
            vec![contribution],
        );
        let pipeline = executor(pipeline_point.clone(), 3_i64);
        let result = pipeline
            .pipeline::<i64, i64, _>(
                &pipeline_point,
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
                4,
                |state, delta| {
                    *state += delta.value;
                    Ok(())
                },
            )
            .await
            .expect("pipeline");
        assert_eq!(result.state, 7);
    }

    #[tokio::test]
    async fn veto_denies_on_provider_denial_and_provider_failure() {
        let contribution = operation_contribution("fixture.provider");
        let point = resolved_point(
            ExtensionCompositionPolicy::Veto,
            ExtensionFailurePolicy::FailClosed,
            vec![contribution.clone()],
        );
        let veto_executor = executor(
            point.clone(),
            ExtensionVeto::deny("fixture-denied", "fixture policy denied the request"),
        );
        let outcome = veto_executor
            .veto(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("veto");
        assert!(outcome.denied);
        assert_eq!(outcome.denials[0].value.code, "fixture-denied");

        let missing_services = ExtensionExecutor::new(
            owner_descriptor(&point),
            registry_with_point(point.clone()),
            Arc::new(sinomo_core::local::SinomoDataDir::new(
                sinomo_testdir::TestDir::new("extension-veto-missing").path(),
            )),
            sinomo_core::service::ServiceRegistrySnapshotStore::default(),
        );
        let failed = missing_services
            .veto(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("fail-closed outcome");
        assert!(failed.denied);
        assert_eq!(failed.denials[0].value.code, "extension-provider-failed");

        let invalid = executor(
            point.clone(),
            serde_json::json!({
                "denied": false,
                "code": "invalid-allow",
                "detail": "allow results cannot carry denial data"
            }),
        )
        .veto(
            &point,
            &serde_json::json!({}),
            &ExtensionContext::default(),
            &ExtensionInvocationContext::new(Duration::from_secs(1)),
        )
        .await
        .expect("malformed veto fails closed");
        assert!(invalid.denied);
        assert_eq!(invalid.failures.len(), 1);

        let services = service_registry("fixture.provider", ExtensionVeto::allow());
        let lifecycle = ExtensionExecutor::new(
            owner_descriptor(&point),
            registry_with_point(point.clone()),
            Arc::new(sinomo_core::local::SinomoDataDir::new(
                sinomo_testdir::TestDir::new("extension-veto-lifecycle").path(),
            )),
            services.clone(),
        );
        services
            .replace_registry(ServiceRegistry::default())
            .expect("provider deactivation");
        let disappeared = lifecycle
            .veto(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("provider disappearance fails closed");
        assert!(disappeared.denied);
        assert_eq!(
            disappeared.denials[0].value.code,
            "extension-provider-failed"
        );

        let expired = lifecycle
            .veto(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::ZERO),
            )
            .await
            .expect("expired veto invocation fails closed");
        assert!(expired.denied);
        assert!(expired.failures[0].detail.contains("deadline elapsed"));
    }

    #[tokio::test]
    async fn draining_provider_fences_stale_extension_snapshots_and_vetoes_fail_closed() {
        let contribution = operation_contribution("fixture.provider");
        let collect_point = resolved_point(
            ExtensionCompositionPolicy::Collect,
            ExtensionFailurePolicy::Partial,
            vec![contribution.clone()],
        );
        let leases = crate::ModuleInvocationLeaseRegistry::default();
        leases
            .activate_module("fixture.provider", "fixture.provider")
            .expect("activate provider");
        leases.begin_drain("fixture.provider").expect("drain");
        let collect =
            executor(collect_point.clone(), 42_u64).with_invocation_leases(leases.clone());
        let collection = collect
            .collect::<_, u64>(
                &collect_point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("partial collection");
        assert!(collection.values.is_empty());
        assert_eq!(collection.failures.len(), 1);

        let veto_point = resolved_point(
            ExtensionCompositionPolicy::Veto,
            ExtensionFailurePolicy::FailClosed,
            vec![contribution],
        );
        let veto =
            executor(veto_point.clone(), ExtensionVeto::allow()).with_invocation_leases(leases);
        let outcome = veto
            .veto(
                &veto_point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("fail-closed veto");
        assert!(outcome.denied);
        assert_eq!(outcome.denials[0].value.code, "extension-provider-failed");
    }

    #[tokio::test]
    async fn attributed_results_retain_provider_lease_until_owner_finishes_using_them() {
        let contribution = operation_contribution("fixture.provider");
        let point = resolved_point(
            ExtensionCompositionPolicy::Collect,
            ExtensionFailurePolicy::Partial,
            vec![contribution],
        );
        let leases = crate::ModuleInvocationLeaseRegistry::default();
        leases
            .activate_module("fixture.provider", "fixture.provider")
            .expect("activate provider");
        let collection = executor(point.clone(), 42_u64)
            .with_invocation_leases(leases.clone())
            .collect::<_, u64>(
                &point,
                &serde_json::json!({}),
                &ExtensionContext::default(),
                &ExtensionInvocationContext::new(Duration::from_secs(1)),
            )
            .await
            .expect("collection");
        assert_eq!(collection.values[0].value, 42);
        assert_eq!(leases.begin_drain("fixture.provider").expect("drain"), 1);
        drop(collection);
        assert!(
            leases
                .wait_for_drain("fixture.provider", Duration::from_secs(1))
                .await
                .expect("drain outcome")
                .drained
        );
    }

    #[test]
    fn invocation_context_rejects_reentry_and_depth() {
        let contribution = operation_contribution("fixture.provider");
        let point = resolved_point(
            ExtensionCompositionPolicy::Collect,
            ExtensionFailurePolicy::Partial,
            vec![contribution.clone()],
        );
        let context = ExtensionInvocationContext::new(Duration::from_secs(1)).with_max_depth(1);
        let child = context.enter(&point, &contribution).expect("first entry");
        assert!(matches!(
            child.enter(&point, &contribution),
            Err(ExtensionExecutionError::InvocationCycle { .. })
        ));
        let other = ResolvedExtensionContribution {
            contribution: ExtensionContribution::operation(
                "fixture.other",
                "fixture.point",
                ExtensionVersionRequirement::exact("1.0.0"),
                ExtensionOperationReference::new("fixture.other", "fixture.other"),
            ),
            ..contribution
        };
        assert!(matches!(
            child.enter(&point, &other),
            Err(ExtensionExecutionError::InvocationDepthExceeded { .. })
        ));
    }
}
