//! Deterministic compilation of module extension points and contributions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::format_description::well_known::Rfc3339;

use crate::{ModuleBackendRegistration, ModuleRegistration, ModuleRegistrySnapshot};
use sinomo_module_api::{
    ExtensionCompositionPolicy, ExtensionContext, ExtensionContribution,
    ExtensionOperationReference, ExtensionPointDefinition, ExtensionSelection,
    ModuleInterfaceExport, ModuleInterfaceOperationExport,
};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostExtensionPointDefinition {
    pub owner_id: String,
    pub owner_version: String,
    pub definition: ExtensionPointDefinition,
}

impl HostExtensionPointDefinition {
    pub fn new(
        owner_id: impl Into<String>,
        owner_version: impl Into<String>,
        definition: ExtensionPointDefinition,
    ) -> Self {
        Self {
            owner_id: owner_id.into(),
            owner_version: owner_version.into(),
            definition,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionRegistryGeneration {
    pub module_registry_generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionRegistrySnapshot {
    pub generation: ExtensionRegistryGeneration,
    pub generated_at: String,
    pub points: Vec<ResolvedExtensionPoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ExtensionRegistryDiagnostic>,
}

impl ExtensionRegistrySnapshot {
    pub fn point(&self, point_id: &str, point_version: &str) -> Option<&ResolvedExtensionPoint> {
        self.points.iter().find(|point| {
            point.definition.point_id == point_id && point.definition.point_version == point_version
        })
    }

    pub fn latest_point(&self, point_id: &str) -> Option<&ResolvedExtensionPoint> {
        self.points
            .iter()
            .filter(|point| point.definition.point_id == point_id)
            .max_by_key(|point| parse_version_key(&point.definition.point_version))
    }

    pub fn eligible<'a>(
        &'a self,
        point: &'a ResolvedExtensionPoint,
        context: &ExtensionContext,
    ) -> Vec<&'a ResolvedExtensionContribution> {
        point
            .contributions
            .iter()
            .filter(|contribution| context.matches(&contribution.contribution.when))
            .collect()
    }

    pub fn select_one<'a>(
        &'a self,
        point: &'a ResolvedExtensionPoint,
        context: &ExtensionContext,
        selection: Option<&ExtensionSelection>,
    ) -> Result<&'a ResolvedExtensionContribution, ExtensionResolveError> {
        if point.definition.composition != ExtensionCompositionPolicy::SelectOne {
            return Err(ExtensionResolveError::WrongPolicy {
                point_id: point.definition.point_id.clone(),
                expected: ExtensionCompositionPolicy::SelectOne,
                actual: point.definition.composition,
            });
        }
        let selection = selection.ok_or_else(|| ExtensionResolveError::SelectionRequired {
            point_id: point.definition.point_id.clone(),
            point_version: point.definition.point_version.clone(),
        })?;
        selection
            .validate()
            .map_err(|error| ExtensionResolveError::InvalidSelection {
                detail: error.to_string(),
            })?;
        if selection.point_id != point.definition.point_id
            || selection.point_version != point.definition.point_version
        {
            return Err(ExtensionResolveError::InvalidSelection {
                detail: "selection targets a different extension point generation".to_string(),
            });
        }
        self.eligible(point, context)
            .into_iter()
            .find(|contribution| {
                contribution.contribution.contribution_id == selection.contribution_id
            })
            .ok_or_else(|| ExtensionResolveError::SelectedContributionUnavailable {
                point_id: point.definition.point_id.clone(),
                contribution_id: selection.contribution_id.clone(),
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResolvedExtensionPoint {
    pub owner_id: String,
    pub owner_version: String,
    pub definition: ExtensionPointDefinition,
    pub contributions: Vec<ResolvedExtensionContribution>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResolvedExtensionContribution {
    pub module_id: String,
    pub module_version: String,
    pub module_backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    pub contribution: ExtensionContribution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<ResolvedExtensionOperation>,
    #[serde(default)]
    pub legacy_ui: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResolvedExtensionOperation {
    pub service_id: String,
    pub interface_id: String,
    pub interface_version: String,
    pub logical_operation: String,
    pub request_schema_id: String,
    pub response_schema_id: String,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_deadline_ms: u64,
    pub supports_cancellation: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ExtensionDiagnosticCode {
    MissingPoint,
    VersionMismatch,
    ContextKeyNotAllowed,
    ContributionKindNotAllowed,
    PayloadSchemaMismatch,
    PayloadTooLarge,
    OperationMissing,
    OperationSchemaMismatch,
    CapabilityDenied,
    ContributionLimitExceeded,
    OrderingCycle,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtensionRegistryDiagnostic {
    pub code: ExtensionDiagnosticCode,
    pub point_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution_id: Option<String>,
    pub detail: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtensionRegistryCompileError {
    #[error("invalid host extension point `{owner_id}`: {detail}")]
    InvalidHostPoint { owner_id: String, detail: String },
    #[error(
        "extension point `{point_id}` version `{point_version}` is defined by both `{first_owner}` and `{second_owner}`"
    )]
    DuplicatePoint {
        point_id: String,
        point_version: String,
        first_owner: String,
        second_owner: String,
    },
    #[error(
        "extension point `{point_id}` versions have conflicting owners `{first_owner}` and `{second_owner}`"
    )]
    PointOwnerConflict {
        point_id: String,
        first_owner: String,
        second_owner: String,
    },
    #[error(
        "extension contribution `{contribution_id}` is declared by both `{first_module}` and `{second_module}`"
    )]
    DuplicateContribution {
        contribution_id: String,
        first_module: String,
        second_module: String,
    },
    #[error("legacy UI contribution `{contribution_id}` could not be adapted: {detail}")]
    LegacyUiContribution {
        contribution_id: String,
        detail: String,
    },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtensionResolveError {
    #[error("extension point `{point_id}` uses {actual:?}; expected {expected:?}")]
    WrongPolicy {
        point_id: String,
        expected: ExtensionCompositionPolicy,
        actual: ExtensionCompositionPolicy,
    },
    #[error("extension point `{point_id}` version `{point_version}` requires explicit selection")]
    SelectionRequired {
        point_id: String,
        point_version: String,
    },
    #[error("invalid extension selection: {detail}")]
    InvalidSelection { detail: String },
    #[error(
        "selected contribution `{contribution_id}` is unavailable for extension point `{point_id}`"
    )]
    SelectedContributionUnavailable {
        point_id: String,
        contribution_id: String,
    },
}

#[derive(Clone)]
pub struct ExtensionRegistrySnapshotStore {
    active: Arc<ArcSwap<ExtensionRegistrySnapshot>>,
    update_lock: Arc<Mutex<()>>,
}

impl ExtensionRegistrySnapshotStore {
    pub fn new(snapshot: ExtensionRegistrySnapshot) -> Self {
        Self {
            active: Arc::new(ArcSwap::from_pointee(snapshot)),
            update_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn active_snapshot(&self) -> Arc<ExtensionRegistrySnapshot> {
        self.active.load_full()
    }

    pub fn replace(&self, snapshot: ExtensionRegistrySnapshot) -> Result<(), String> {
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| "extension registry update lock poisoned".to_string())?;
        if snapshot.generation.module_registry_generation
            < self.active.load().generation.module_registry_generation
        {
            return Err("extension registry generation cannot move backwards".to_string());
        }
        self.active.store(Arc::new(snapshot));
        Ok(())
    }
}

pub fn compile_extension_registry(
    module_snapshot: &ModuleRegistrySnapshot,
    host_definitions: impl IntoIterator<Item = HostExtensionPointDefinition>,
) -> Result<ExtensionRegistrySnapshot, ExtensionRegistryCompileError> {
    let mut definitions = BTreeMap::<(String, String), OwnedPoint>::new();
    let mut point_owners = BTreeMap::<String, String>::new();
    for host in host_definitions {
        host.definition.validate().map_err(|error| {
            ExtensionRegistryCompileError::InvalidHostPoint {
                owner_id: host.owner_id.clone(),
                detail: error.to_string(),
            }
        })?;
        insert_definition(
            &mut definitions,
            &mut point_owners,
            host.owner_id,
            host.owner_version,
            host.definition,
        )?;
    }
    for registration in module_snapshot.registry().registrations() {
        for definition in &registration.surfaces.extension_points {
            insert_definition(
                &mut definitions,
                &mut point_owners,
                registration.module_id().to_string(),
                registration.manifest.version.clone(),
                definition.clone(),
            )?;
        }
    }

    let mut declared = Vec::<DeclaredContribution>::new();
    let mut contribution_owners = BTreeMap::<String, String>::new();
    for registration in module_snapshot.registry().registrations() {
        for contribution in &registration.surfaces.extension_contributions {
            insert_contribution(
                &mut declared,
                &mut contribution_owners,
                registration,
                contribution.clone(),
                false,
            )?;
        }
        for ui in &registration.surfaces.ui_contributions {
            let contribution = ui.to_extension_contribution().map_err(|error| {
                ExtensionRegistryCompileError::LegacyUiContribution {
                    contribution_id: ui.contribution_id.clone(),
                    detail: error.to_string(),
                }
            })?;
            insert_contribution(
                &mut declared,
                &mut contribution_owners,
                registration,
                contribution,
                true,
            )?;
        }
    }

    let mut diagnostics = Vec::new();
    let mut by_point = BTreeMap::<(String, String), Vec<ResolvedExtensionContribution>>::new();
    for declared in declared {
        let matching = definitions
            .values()
            .filter(|point| {
                point.definition.point_id == declared.contribution.point_id
                    && declared
                        .contribution
                        .point_version
                        .matches(&point.definition.point_version)
            })
            .max_by_key(|point| parse_version_key(&point.definition.point_version));
        let Some(point) = matching else {
            let point_exists = definitions
                .values()
                .any(|point| point.definition.point_id == declared.contribution.point_id);
            diagnostics.push(ExtensionRegistryDiagnostic {
                code: if point_exists {
                    ExtensionDiagnosticCode::VersionMismatch
                } else {
                    ExtensionDiagnosticCode::MissingPoint
                },
                point_id: declared.contribution.point_id.clone(),
                contribution_id: Some(declared.contribution.contribution_id.clone()),
                detail: if point_exists {
                    "no active extension point version satisfies the contribution requirement"
                        .to_string()
                } else {
                    "the owning extension point is not active".to_string()
                },
            });
            continue;
        };
        match compile_contribution(point, declared, &mut diagnostics) {
            Some(contribution) => by_point
                .entry((
                    point.definition.point_id.clone(),
                    point.definition.point_version.clone(),
                ))
                .or_default()
                .push(contribution),
            None => continue,
        }
    }

    let mut points = Vec::with_capacity(definitions.len());
    for ((point_id, point_version), point) in definitions {
        let mut contributions = by_point
            .remove(&(point_id.clone(), point_version.clone()))
            .unwrap_or_default();
        order_contributions(&point.definition, &mut contributions, &mut diagnostics);
        if contributions.len() > point.definition.max_contributions as usize {
            for contribution in contributions
                .iter()
                .skip(point.definition.max_contributions as usize)
            {
                diagnostics.push(ExtensionRegistryDiagnostic {
                    code: ExtensionDiagnosticCode::ContributionLimitExceeded,
                    point_id: point_id.clone(),
                    contribution_id: Some(contribution.contribution.contribution_id.clone()),
                    detail: format!(
                        "point accepts at most {} contributions",
                        point.definition.max_contributions
                    ),
                });
            }
            contributions.truncate(point.definition.max_contributions as usize);
        }
        points.push(ResolvedExtensionPoint {
            owner_id: point.owner_id,
            owner_version: point.owner_version,
            definition: point.definition,
            contributions,
        });
    }
    points.sort_by(|left, right| {
        left.definition
            .point_id
            .cmp(&right.definition.point_id)
            .then_with(|| {
                parse_version_key(&left.definition.point_version)
                    .cmp(&parse_version_key(&right.definition.point_version))
            })
    });
    Ok(ExtensionRegistrySnapshot {
        generation: ExtensionRegistryGeneration {
            module_registry_generation: module_snapshot.version(),
        },
        generated_at: module_snapshot
            .generated_at()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string()),
        points,
        diagnostics,
    })
}

#[derive(Clone)]
struct OwnedPoint {
    owner_id: String,
    owner_version: String,
    definition: ExtensionPointDefinition,
}

#[derive(Clone)]
struct DeclaredContribution {
    module_id: String,
    module_version: String,
    module_backend: String,
    artifact_id: Option<String>,
    contribution: ExtensionContribution,
    interface_exports: Vec<ModuleInterfaceExport>,
    exported_capabilities: Vec<String>,
    legacy_ui: bool,
}

fn insert_definition(
    definitions: &mut BTreeMap<(String, String), OwnedPoint>,
    point_owners: &mut BTreeMap<String, String>,
    owner_id: String,
    owner_version: String,
    definition: ExtensionPointDefinition,
) -> Result<(), ExtensionRegistryCompileError> {
    if let Some(first_owner) = point_owners.get(&definition.point_id)
        && first_owner != &owner_id
    {
        return Err(ExtensionRegistryCompileError::PointOwnerConflict {
            point_id: definition.point_id,
            first_owner: first_owner.clone(),
            second_owner: owner_id,
        });
    }
    point_owners.insert(definition.point_id.clone(), owner_id.clone());
    let key = (
        definition.point_id.clone(),
        definition.point_version.clone(),
    );
    if let Some(previous) = definitions.insert(
        key,
        OwnedPoint {
            owner_id: owner_id.clone(),
            owner_version,
            definition: definition.clone(),
        },
    ) {
        return Err(ExtensionRegistryCompileError::DuplicatePoint {
            point_id: definition.point_id,
            point_version: definition.point_version,
            first_owner: previous.owner_id,
            second_owner: owner_id,
        });
    }
    Ok(())
}

fn insert_contribution(
    declared: &mut Vec<DeclaredContribution>,
    owners: &mut BTreeMap<String, String>,
    registration: &ModuleRegistration,
    contribution: ExtensionContribution,
    legacy_ui: bool,
) -> Result<(), ExtensionRegistryCompileError> {
    let module_id = registration.module_id().to_string();
    if let Some(first_module) =
        owners.insert(contribution.contribution_id.clone(), module_id.clone())
    {
        return Err(ExtensionRegistryCompileError::DuplicateContribution {
            contribution_id: contribution.contribution_id,
            first_module,
            second_module: module_id,
        });
    }
    declared.push(DeclaredContribution {
        module_id,
        module_version: registration.manifest.version.clone(),
        module_backend: module_backend_label(&registration.backend).to_string(),
        artifact_id: registration.component.artifact_id.clone(),
        contribution,
        interface_exports: registration.surfaces.interface_exports.clone(),
        exported_capabilities: registration
            .surfaces
            .capabilities
            .exports
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect(),
        legacy_ui,
    });
    Ok(())
}

fn compile_contribution(
    point: &OwnedPoint,
    declared: DeclaredContribution,
    diagnostics: &mut Vec<ExtensionRegistryDiagnostic>,
) -> Option<ResolvedExtensionContribution> {
    let contribution_id = declared.contribution.contribution_id.clone();
    let point_id = point.definition.point_id.clone();
    let diagnostic = |code, detail| ExtensionRegistryDiagnostic {
        code,
        point_id: point_id.clone(),
        contribution_id: Some(contribution_id.clone()),
        detail,
    };
    let kind = declared.contribution.kind()?;
    if !point.definition.allowed_contribution_kinds.contains(&kind) {
        diagnostics.push(diagnostic(
            ExtensionDiagnosticCode::ContributionKindNotAllowed,
            format!("point does not accept {kind:?} contributions"),
        ));
        return None;
    }
    if let Some(key) = declared
        .contribution
        .when
        .iter()
        .map(|condition| condition.key.as_str())
        .find(|key| {
            !point
                .definition
                .allowed_context_keys
                .iter()
                .any(|allowed| allowed == key)
        })
    {
        diagnostics.push(diagnostic(
            ExtensionDiagnosticCode::ContextKeyNotAllowed,
            format!("context key `{key}` is not owned by this point"),
        ));
        return None;
    }
    if !declared.contribution.payload_sobj.is_empty()
        && declared.contribution.payload_schema_id != point.definition.contribution_schema_id
    {
        diagnostics.push(diagnostic(
            ExtensionDiagnosticCode::PayloadSchemaMismatch,
            format!(
                "payload schema `{}` does not match `{}`",
                declared.contribution.payload_schema_id, point.definition.contribution_schema_id
            ),
        ));
        return None;
    }
    if declared.contribution.payload_sobj.len() as u64 > point.definition.max_response_bytes {
        diagnostics.push(diagnostic(
            ExtensionDiagnosticCode::PayloadTooLarge,
            format!(
                "declarative payload is {} bytes; point limit is {}",
                declared.contribution.payload_sobj.len(),
                point.definition.max_response_bytes,
            ),
        ));
        return None;
    }
    if point
        .definition
        .contributor_capabilities
        .iter()
        .any(|required| !declared.exported_capabilities.contains(required))
        || declared
            .contribution
            .required_capabilities
            .iter()
            .any(|required| !point.definition.apply_capabilities.contains(required))
    {
        diagnostics.push(diagnostic(
            ExtensionDiagnosticCode::CapabilityDenied,
            "contributor or apply capabilities do not satisfy the point contract".to_string(),
        ));
        return None;
    }
    let operation = match declared.contribution.operation.as_ref() {
        Some(reference) => {
            match resolve_operation(reference, &declared.interface_exports, &point.definition) {
                Ok(operation) => Some(operation),
                Err((code, detail)) => {
                    diagnostics.push(diagnostic(code, detail));
                    return None;
                }
            }
        }
        None => None,
    };
    Some(ResolvedExtensionContribution {
        module_id: declared.module_id,
        module_version: declared.module_version,
        module_backend: declared.module_backend,
        artifact_id: declared.artifact_id,
        contribution: declared.contribution,
        operation,
        legacy_ui: declared.legacy_ui,
    })
}

fn resolve_operation(
    reference: &ExtensionOperationReference,
    exports: &[ModuleInterfaceExport],
    point: &ExtensionPointDefinition,
) -> Result<ResolvedExtensionOperation, (ExtensionDiagnosticCode, String)> {
    let export = exports
        .iter()
        .find(|export| export.export_id == reference.export_id)
        .ok_or_else(|| {
            (
                ExtensionDiagnosticCode::OperationMissing,
                format!("interface export `{}` is not declared", reference.export_id),
            )
        })?;
    let operation = export
        .operations
        .iter()
        .find(|operation| operation.logical_operation == reference.logical_operation)
        .ok_or_else(|| {
            (
                ExtensionDiagnosticCode::OperationMissing,
                format!(
                    "logical operation `{}` is not declared by `{}`",
                    reference.logical_operation, reference.export_id
                ),
            )
        })?;
    validate_operation_schemas(export, operation, point)?;
    Ok(ResolvedExtensionOperation {
        service_id: export.export_id.clone(),
        interface_id: export.interface_id.clone(),
        interface_version: export.interface_version.clone(),
        logical_operation: operation.logical_operation.clone(),
        request_schema_id: operation.request_schema_id.clone(),
        response_schema_id: operation.response_schema_id.clone(),
        max_request_bytes: operation.max_request_bytes,
        max_response_bytes: operation.max_response_bytes,
        max_deadline_ms: operation.max_deadline_ms,
        supports_cancellation: operation.supports_cancellation,
    })
}

fn validate_operation_schemas(
    export: &ModuleInterfaceExport,
    operation: &ModuleInterfaceOperationExport,
    point: &ExtensionPointDefinition,
) -> Result<(), (ExtensionDiagnosticCode, String)> {
    let contract = point.operation_contract.as_ref().ok_or_else(|| {
        (
            ExtensionDiagnosticCode::OperationSchemaMismatch,
            "operation-backed point has no operation interface contract".to_string(),
        )
    })?;
    if export.interface_id != contract.interface_id
        || export.interface_version != contract.interface_version
        || operation.logical_operation != contract.logical_operation
    {
        return Err((
            ExtensionDiagnosticCode::OperationSchemaMismatch,
            format!(
                "operation `{}` exposes `{}` {} / `{}`; point requires `{}` {} / `{}`",
                export.export_id,
                export.interface_id,
                export.interface_version,
                operation.logical_operation,
                contract.interface_id,
                contract.interface_version,
                contract.logical_operation,
            ),
        ));
    }
    if operation.request_schema_id != point.input_schema_id
        || operation.response_schema_id != point.contribution_schema_id
    {
        return Err((
            ExtensionDiagnosticCode::OperationSchemaMismatch,
            format!(
                "operation `{}` schemas `{}` -> `{}` do not match point schemas `{}` -> `{}`",
                operation.logical_operation,
                operation.request_schema_id,
                operation.response_schema_id,
                point.input_schema_id,
                point.contribution_schema_id
            ),
        ));
    }
    if operation.max_request_bytes < point.max_request_bytes
        || operation.max_response_bytes < point.max_response_bytes
        || operation.max_deadline_ms < point.max_deadline_ms
        || (point.requires_cancellation && !operation.supports_cancellation)
    {
        return Err((
            ExtensionDiagnosticCode::OperationSchemaMismatch,
            format!(
                "operation `{}` limits or cancellation support do not satisfy the extension point",
                export.export_id,
            ),
        ));
    }
    Ok(())
}

fn order_contributions(
    point: &ExtensionPointDefinition,
    contributions: &mut Vec<ResolvedExtensionContribution>,
    diagnostics: &mut Vec<ExtensionRegistryDiagnostic>,
) {
    let by_id = contributions
        .iter()
        .enumerate()
        .map(|(index, contribution)| (contribution.contribution.contribution_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = vec![BTreeSet::<usize>::new(); contributions.len()];
    let mut indegree = vec![0usize; contributions.len()];
    for (index, contribution) in contributions.iter().enumerate() {
        for before in &contribution.contribution.before {
            if let Some(&target) = by_id.get(before.as_str())
                && outgoing[index].insert(target)
            {
                indegree[target] += 1;
            }
        }
        for after in &contribution.contribution.after {
            if let Some(&source) = by_id.get(after.as_str())
                && outgoing[source].insert(index)
            {
                indegree[index] += 1;
            }
        }
    }
    let mut ready = (0..contributions.len())
        .filter(|index| indegree[*index] == 0)
        .collect::<Vec<_>>();
    let mut ordered = Vec::with_capacity(contributions.len());
    while !ready.is_empty() {
        ready.sort_by(|left, right| {
            contribution_sort_key(&contributions[*right])
                .cmp(&contribution_sort_key(&contributions[*left]))
        });
        let index = ready.pop().expect("non-empty extension ordering queue");
        ordered.push(index);
        for target in outgoing[index].iter().copied() {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.push(target);
            }
        }
    }
    if ordered.len() == contributions.len() {
        let original = contributions.clone();
        *contributions = ordered
            .into_iter()
            .map(|index| original[index].clone())
            .collect();
        return;
    }
    let cyclic = indegree
        .iter()
        .enumerate()
        .filter(|(_, degree)| **degree > 0)
        .map(|(index, _)| contributions[index].contribution.contribution_id.clone())
        .collect::<Vec<_>>();
    diagnostics.push(ExtensionRegistryDiagnostic {
        code: ExtensionDiagnosticCode::OrderingCycle,
        point_id: point.point_id.clone(),
        contribution_id: None,
        detail: format!(
            "discarded cyclic relative ordering among {}",
            cyclic.join(", ")
        ),
    });
    contributions
        .sort_by(|left, right| contribution_sort_key(left).cmp(&contribution_sort_key(right)));
}

fn contribution_sort_key(contribution: &ResolvedExtensionContribution) -> (&str, i32, &str, &str) {
    (
        contribution.contribution.group.as_str(),
        contribution.contribution.order,
        contribution.module_id.as_str(),
        contribution.contribution.contribution_id.as_str(),
    )
}

fn module_backend_label(backend: &ModuleBackendRegistration) -> &'static str {
    match backend {
        ModuleBackendRegistration::NativeRust { .. } => "native-rust",
        ModuleBackendRegistration::NativeDylib { .. } => "native-dylib",
        ModuleBackendRegistration::Declarative { .. } => "declarative",
        ModuleBackendRegistration::SandboxProcess { .. } => "sandbox-process",
        ModuleBackendRegistration::WasmComponent { .. } => "wasm-component",
    }
}

fn parse_version_key(value: &str) -> (u64, u64, u64) {
    let mut parts = value.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(major), Some(minor), Some(patch), None) => (
            major.parse().unwrap_or(0),
            minor.parse().unwrap_or(0),
            patch.parse().unwrap_or(0),
        ),
        _ => (0, 0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModuleId, ModuleRegistration, ModuleRegistry, ModuleRegistrySnapshotStore};
    use sinomo_core::TrustRing;
    use sinomo_module_api::{
        ExtensionContributionKind, ExtensionFailurePolicy, ExtensionOperationReference,
        ExtensionVersionRequirement, ModuleInterfaceOperationExport,
        ModuleInterfaceOperationTarget,
    };

    fn point(
        id: &str,
        version: &str,
        policy: ExtensionCompositionPolicy,
    ) -> ExtensionPointDefinition {
        let mut point = ExtensionPointDefinition::new(
            id,
            version,
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
        .with_operation_contract(
            "fixture.extension-provider",
            "1.0.0",
            "fixture.contribute",
        );
        if policy == ExtensionCompositionPolicy::SelectOne {
            point = point.require_explicit_selection();
        }
        if policy == ExtensionCompositionPolicy::Veto {
            point = point.with_failure_policy(ExtensionFailurePolicy::FailClosed);
        }
        point
    }

    fn registration(
        module: &str,
        points: Vec<ExtensionPointDefinition>,
        contributions: Vec<ExtensionContribution>,
    ) -> ModuleRegistration {
        let module_id = ModuleId::from_legacy_named(module).expect("module id");
        let mut registration = ModuleRegistration::native_rust(
            module_id,
            "1.0.0",
            module,
            module,
            "1.0.0",
            TrustRing::R1TrustedRuntime,
            module,
        );
        registration.surfaces.extension_points = points;
        registration.surfaces.extension_contributions = contributions;
        registration
    }

    fn operation_contribution(
        module: &str,
        point_id: &str,
        version: &str,
    ) -> ExtensionContribution {
        ExtensionContribution::operation(
            format!("{module}.contribution"),
            point_id,
            ExtensionVersionRequirement::exact(version),
            ExtensionOperationReference::new(format!("{module}.service"), "fixture.contribute"),
        )
    }

    fn add_operation_export(registration: &mut ModuleRegistration, module: &str) {
        registration.surfaces.interface_exports.push(
            ModuleInterfaceExport::new(
                format!("{module}.service"),
                "fixture.extension-provider",
                "1.0.0",
                vec![1],
            )
            .with_operation(ModuleInterfaceOperationExport {
                logical_operation: "fixture.contribute".to_string(),
                target: ModuleInterfaceOperationTarget::ObjectRpc {
                    route: format!("/{module}/contribute"),
                    operation: "fixture.contribute".to_string(),
                },
                request_schema_id: "fixture.request/v1".to_string(),
                response_schema_id: "fixture.contribution/v1".to_string(),
                max_request_bytes: 1024 * 1024,
                max_response_bytes: 1024 * 1024,
                max_facts_per_batch: 1,
                max_deadline_ms: 5_000,
                supports_cancellation: true,
                continuation_schema_id: None,
            }),
        );
        registration
            .surfaces
            .object_rpc_routes
            .push(crate::ModuleObjectRpcRoute::new(
                format!("/{module}/contribute"),
                module,
                "fixture.contribute",
                crate::ModuleRouteAuth::Component,
            ));
        registration
            .surfaces
            .capabilities
            .exports
            .push(sinomo_core::CapabilityName::new("fixture.contribute").expect("capability"));
    }

    #[test]
    fn compilation_is_generation_stamped_and_resolves_operation_contracts() {
        let point_id = "fixture.diagnostics";
        let owner = registration(
            "module.owner",
            vec![point(
                point_id,
                "1.0.0",
                ExtensionCompositionPolicy::Collect,
            )],
            Vec::new(),
        );
        let mut provider = registration(
            "module.provider",
            Vec::new(),
            vec![operation_contribution("module.provider", point_id, "1.0.0")],
        );
        add_operation_export(&mut provider, "module.provider");
        let registry = ModuleRegistry::builder()
            .register(owner)
            .expect("owner")
            .register(provider)
            .expect("provider")
            .try_build()
            .expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("store");
        let module_snapshot = store.active_snapshot().expect("snapshot");
        let snapshot = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("compile registry");
        let repeated = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("repeat deterministic compilation");
        assert_eq!(snapshot, repeated);
        assert_eq!(snapshot.generation.module_registry_generation, 1);
        let point = snapshot.point(point_id, "1.0.0").expect("point");
        assert_eq!(point.contributions.len(), 1);
        assert_eq!(
            point.contributions[0]
                .operation
                .as_ref()
                .expect("operation")
                .service_id,
            "module.provider.service"
        );
    }

    #[test]
    fn missing_and_version_mismatched_points_remain_pending_diagnostics() {
        let provider = registration(
            "module.provider",
            Vec::new(),
            vec![
                ExtensionContribution::declarative(
                    "module.provider.missing",
                    "fixture.missing",
                    ExtensionVersionRequirement::Any,
                    "fixture.contribution/v1",
                    vec![1],
                ),
                ExtensionContribution::declarative(
                    "module.provider.version",
                    "fixture.present",
                    ExtensionVersionRequirement::exact("2.0.0"),
                    "fixture.contribution/v1",
                    vec![1],
                ),
            ],
        );
        let owner = registration(
            "module.owner",
            vec![point(
                "fixture.present",
                "1.0.0",
                ExtensionCompositionPolicy::Collect,
            )],
            Vec::new(),
        );
        let registry = ModuleRegistry::builder()
            .register(owner)
            .expect("owner")
            .register(provider)
            .expect("provider")
            .try_build()
            .expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("store");
        let module_snapshot = store.active_snapshot().expect("snapshot");
        let snapshot = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("compile registry");
        assert!(
            snapshot
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == ExtensionDiagnosticCode::MissingPoint })
        );
        assert!(
            snapshot
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == ExtensionDiagnosticCode::VersionMismatch })
        );
    }

    #[test]
    fn compatible_major_contributions_bind_to_the_latest_matching_point() {
        let point_id = "fixture.versioned";
        let owner = registration(
            "module.owner",
            vec![
                point(point_id, "1.1.0", ExtensionCompositionPolicy::ManyOrdered),
                point(point_id, "1.4.0", ExtensionCompositionPolicy::ManyOrdered),
                point(point_id, "2.0.0", ExtensionCompositionPolicy::ManyOrdered),
            ],
            Vec::new(),
        );
        let provider = registration(
            "module.provider",
            Vec::new(),
            vec![ExtensionContribution::declarative(
                "module.provider.compatible",
                point_id,
                ExtensionVersionRequirement::CompatibleMajor {
                    major: 1,
                    minimum_minor: 2,
                    minimum_patch: 0,
                },
                "fixture.contribution/v1",
                vec![1],
            )],
        );
        let registry = ModuleRegistry::builder()
            .register(owner)
            .expect("owner")
            .register(provider)
            .expect("provider")
            .try_build()
            .expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("store");
        let module_snapshot = store.active_snapshot().expect("snapshot");
        let snapshot = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("compile registry");

        assert!(
            snapshot
                .point(point_id, "1.1.0")
                .expect("old point")
                .contributions
                .is_empty()
        );
        assert_eq!(
            snapshot
                .point(point_id, "1.4.0")
                .expect("latest compatible point")
                .contributions
                .len(),
            1
        );
        assert!(
            snapshot
                .point(point_id, "2.0.0")
                .expect("next major point")
                .contributions
                .is_empty()
        );
    }

    #[test]
    fn operation_contract_must_satisfy_required_cancellation() {
        let point_id = "fixture.cancellable";
        let owner = registration(
            "module.owner",
            vec![
                point(point_id, "1.0.0", ExtensionCompositionPolicy::Collect)
                    .require_cancellation(),
            ],
            Vec::new(),
        );
        let mut provider = registration(
            "module.provider",
            Vec::new(),
            vec![operation_contribution("module.provider", point_id, "1.0.0")],
        );
        add_operation_export(&mut provider, "module.provider");
        provider.surfaces.interface_exports[0].operations[0].supports_cancellation = false;
        let registry = ModuleRegistry::builder()
            .register(owner)
            .expect("owner")
            .register(provider)
            .expect("provider")
            .try_build()
            .expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("store");
        let module_snapshot = store.active_snapshot().expect("snapshot");
        let snapshot = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("compile registry");

        assert!(snapshot.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ExtensionDiagnosticCode::OperationSchemaMismatch
                && diagnostic.contribution_id.as_deref() == Some("module.provider.contribution")
        }));
        assert!(
            snapshot
                .point(point_id, "1.0.0")
                .expect("point")
                .contributions
                .is_empty()
        );
    }

    #[test]
    fn cyclic_ordering_is_diagnosed_and_falls_back_deterministically() {
        let point_id = "fixture.ordered";
        let owner = registration(
            "module.owner",
            vec![point(
                point_id,
                "1.0.0",
                ExtensionCompositionPolicy::ManyOrdered,
            )],
            Vec::new(),
        );
        let mut first = ExtensionContribution::declarative(
            "module.first",
            point_id,
            ExtensionVersionRequirement::exact("1.0.0"),
            "fixture.contribution/v1",
            vec![1],
        );
        let mut second = ExtensionContribution::declarative(
            "module.second",
            point_id,
            ExtensionVersionRequirement::exact("1.0.0"),
            "fixture.contribution/v1",
            vec![2],
        );
        first.before.push(second.contribution_id.clone());
        second.before.push(first.contribution_id.clone());
        let provider = registration("module.provider", Vec::new(), vec![second, first]);
        let registry = ModuleRegistry::builder()
            .register(owner)
            .expect("owner")
            .register(provider)
            .expect("provider")
            .try_build()
            .expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("store");
        let module_snapshot = store.active_snapshot().expect("snapshot");
        let snapshot = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("compile registry");
        assert!(
            snapshot
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == ExtensionDiagnosticCode::OrderingCycle })
        );
        let ids = snapshot
            .point(point_id, "1.0.0")
            .expect("point")
            .contributions
            .iter()
            .map(|contribution| contribution.contribution.contribution_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["module.first", "module.second"]);
    }

    #[test]
    fn select_one_never_uses_activation_order_as_selection() {
        let point_id = "fixture.selected";
        let owner = registration(
            "module.owner",
            vec![point(
                point_id,
                "1.0.0",
                ExtensionCompositionPolicy::SelectOne,
            )],
            Vec::new(),
        );
        let provider = registration(
            "module.provider",
            Vec::new(),
            vec![ExtensionContribution::declarative(
                "module.provider.choice",
                point_id,
                ExtensionVersionRequirement::exact("1.0.0"),
                "fixture.contribution/v1",
                vec![1],
            )],
        );
        let registry = ModuleRegistry::builder()
            .register(owner)
            .expect("owner")
            .register(provider)
            .expect("provider")
            .try_build()
            .expect("registry");
        let store = ModuleRegistrySnapshotStore::new(registry).expect("store");
        let module_snapshot = store.active_snapshot().expect("snapshot");
        let snapshot = compile_extension_registry(module_snapshot.as_ref(), Vec::new())
            .expect("compile registry");
        let point = snapshot.point(point_id, "1.0.0").expect("point");
        assert!(matches!(
            snapshot.select_one(point, &ExtensionContext::default(), None),
            Err(ExtensionResolveError::SelectionRequired { .. })
        ));
        let selection = ExtensionSelection {
            point_id: point_id.to_string(),
            point_version: "1.0.0".to_string(),
            contribution_id: "module.provider.choice".to_string(),
        };
        assert_eq!(
            snapshot
                .select_one(point, &ExtensionContext::default(), Some(&selection))
                .expect("selected")
                .contribution
                .contribution_id,
            "module.provider.choice"
        );
    }

    #[test]
    fn snapshot_store_never_moves_the_module_generation_backwards() {
        let snapshot = |generation| ExtensionRegistrySnapshot {
            generation: ExtensionRegistryGeneration {
                module_registry_generation: generation,
            },
            generated_at: "test".to_string(),
            points: Vec::new(),
            diagnostics: Vec::new(),
        };
        let store = ExtensionRegistrySnapshotStore::new(snapshot(2));
        assert!(store.replace(snapshot(1)).is_err());
        assert_eq!(
            store
                .active_snapshot()
                .generation
                .module_registry_generation,
            2
        );
        store.replace(snapshot(3)).expect("forward generation");
        assert_eq!(
            store
                .active_snapshot()
                .generation
                .module_registry_generation,
            3
        );
    }
}
