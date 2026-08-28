use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Where a provided service may be resolved.
///
/// Cluster-visible services are also callable on their providing node. The
/// distinction controls publication and remote dependency resolution; it does
/// not create a second local handler registry.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceLocality {
    #[default]
    Local,
    Cluster,
}

/// Locality constraint declared by a component service dependency.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceRequirementLocality {
    #[default]
    Local,
    Cluster,
    Any,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ComponentEventLocality {
    #[default]
    Local,
    Cluster,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ComponentEventDurability {
    #[default]
    Transient,
    Durable,
}

/// One typed fact a component is authorized to publish.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComponentEventPublication {
    pub kind: String,
    pub schema_id: String,
    pub schema_version: String,
    #[serde(default)]
    pub locality: ComponentEventLocality,
    #[serde(default)]
    pub durability: ComponentEventDurability,
}

impl ComponentEventPublication {
    pub fn new(
        kind: impl Into<String>,
        schema_id: impl Into<String>,
        schema_version: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            schema_id: schema_id.into(),
            schema_version: schema_version.into(),
            locality: ComponentEventLocality::Local,
            durability: ComponentEventDurability::Transient,
        }
    }

    pub fn cluster_visible(mut self) -> Self {
        self.locality = ComponentEventLocality::Cluster;
        self
    }

    pub fn durable(mut self) -> Self {
        self.durability = ComponentEventDurability::Durable;
        self
    }

    pub fn validate(&self) -> Result<(), ComponentContractError> {
        validate_dotted_event_name(&self.kind, "event publication kind")?;
        validate_component_token(&self.schema_id, "event publication schema id")?;
        validate_component_token(&self.schema_version, "event publication schema version")
    }
}

/// One typed fact stream a component is authorized to consume. A trailing
/// `.*` matches a dotted namespace; every other pattern is exact.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComponentEventSubscription {
    pub kind_pattern: String,
    pub schema_id: String,
    pub schema_version: String,
    #[serde(default)]
    pub locality: ComponentEventLocality,
}

impl ComponentEventSubscription {
    pub fn new(
        kind_pattern: impl Into<String>,
        schema_id: impl Into<String>,
        schema_version: impl Into<String>,
    ) -> Self {
        Self {
            kind_pattern: kind_pattern.into(),
            schema_id: schema_id.into(),
            schema_version: schema_version.into(),
            locality: ComponentEventLocality::Local,
        }
    }

    pub fn from_cluster(mut self) -> Self {
        self.locality = ComponentEventLocality::Cluster;
        self
    }

    pub fn matches_kind(&self, kind: &str) -> bool {
        if self.kind_pattern == "*" {
            return true;
        }
        self.kind_pattern
            .strip_suffix(".*")
            .map_or(self.kind_pattern == kind, |prefix| {
                kind.strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('.'))
            })
    }

    pub fn validate(&self) -> Result<(), ComponentContractError> {
        if self.kind_pattern == "*" {
            validate_component_token(&self.schema_id, "event subscription schema id")?;
            return validate_component_token(
                &self.schema_version,
                "event subscription schema version",
            );
        }
        let kind = self
            .kind_pattern
            .strip_suffix(".*")
            .unwrap_or(self.kind_pattern.as_str());
        validate_dotted_event_name(kind, "event subscription kind pattern")?;
        validate_component_token(&self.schema_id, "event subscription schema id")?;
        validate_component_token(&self.schema_version, "event subscription schema version")
    }
}

/// One versioned service interface provided by a component.
///
/// Provider capabilities describe what the implementation can do. Caller
/// capabilities describe authority required to invoke it. Keeping the two
/// collections separate prevents service discovery from treating an
/// authorization requirement as a provider feature.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceOffer {
    pub service_id: String,
    pub interface_id: String,
    pub interface_version: String,
    #[serde(default)]
    pub locality: ServiceLocality,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caller_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<String>,
    /// Exact operation-to-route and operation-to-authority mappings. The
    /// flattened fields above remain rolling-compatible catalog summaries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<ServiceOperationOffer>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceOperationOffer {
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caller_capabilities: Vec<String>,
}

impl ServiceOperationOffer {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            route: None,
            caller_capabilities: Vec::new(),
        }
    }

    pub fn at_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    pub fn require_caller_capability(mut self, capability: impl Into<String>) -> Self {
        self.caller_capabilities.push(capability.into());
        self
    }
}

impl ServiceOffer {
    pub fn new(
        service_id: impl Into<String>,
        interface_id: impl Into<String>,
        interface_version: impl Into<String>,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            interface_id: interface_id.into(),
            interface_version: interface_version.into(),
            locality: ServiceLocality::Local,
            provider_capabilities: Vec::new(),
            caller_capabilities: Vec::new(),
            routes: Vec::new(),
            operations: Vec::new(),
        }
    }

    pub fn cluster_visible(mut self) -> Self {
        self.locality = ServiceLocality::Cluster;
        self
    }

    pub fn with_provider_capability(mut self, capability: impl Into<String>) -> Self {
        self.provider_capabilities.push(capability.into());
        self
    }

    pub fn require_caller_capability(mut self, capability: impl Into<String>) -> Self {
        self.caller_capabilities.push(capability.into());
        self
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.routes.push(route.into());
        self
    }

    pub fn with_operation(mut self, operation: ServiceOperationOffer) -> Self {
        if !self.provider_capabilities.contains(&operation.operation) {
            self.provider_capabilities.push(operation.operation.clone());
        }
        if let Some(route) = &operation.route
            && !self.routes.contains(route)
        {
            self.routes.push(route.clone());
        }
        self.operations.push(operation);
        self
    }

    pub fn validate(&self) -> Result<(), ComponentContractError> {
        validate_component_token(&self.service_id, "service id")?;
        validate_component_token(&self.interface_id, "service interface id")?;
        validate_component_token(&self.interface_version, "service interface version")?;
        validate_unique_tokens(
            &self.provider_capabilities,
            "provider capability",
            &self.service_id,
        )?;
        validate_unique_tokens(
            &self.caller_capabilities,
            "caller capability",
            &self.service_id,
        )?;
        let mut routes = BTreeSet::new();
        for route in &self.routes {
            if !route.starts_with('/')
                || route != route.trim()
                || route
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
            {
                return Err(ComponentContractError::InvalidRoute {
                    service_id: self.service_id.clone(),
                    route: route.clone(),
                });
            }
            if !routes.insert(route.as_str()) {
                return Err(ComponentContractError::DuplicateRoute {
                    service_id: self.service_id.clone(),
                    route: route.clone(),
                });
            }
        }
        let mut operations = BTreeSet::new();
        for operation in &self.operations {
            validate_component_token(&operation.operation, "service operation")?;
            validate_unique_tokens(
                &operation.caller_capabilities,
                "operation caller capability",
                &self.service_id,
            )?;
            if let Some(route) = &operation.route
                && !self.routes.contains(route)
            {
                return Err(ComponentContractError::InvalidRoute {
                    service_id: self.service_id.clone(),
                    route: route.clone(),
                });
            }
            if !operations.insert(operation.operation.as_str()) {
                return Err(ComponentContractError::DuplicateToken {
                    service_id: self.service_id.clone(),
                    kind: "service operation",
                    value: operation.operation.clone(),
                });
            }
        }
        Ok(())
    }
}

/// One service dependency that controls component activation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceRequirement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<String>,
    pub interface_id: String,
    /// Exact version or `*` while the interface negotiation vocabulary remains
    /// deliberately small. Compatibility ranges can be added to the ABI later
    /// without coupling the host ABI to a package manager.
    pub interface_version: String,
    #[serde(default)]
    pub locality: ServiceRequirementLocality,
    /// Capabilities the component is authorized to exercise on a matching
    /// provider. Providers remain authoritative: every capability they require
    /// must be declared here before the host dispatches an in-process call.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caller_capabilities: Vec<String>,
    /// Operations this component intends to invoke. Empty preserves legacy
    /// dependency-only requirements, but the component service client always
    /// requires the concrete operation to be listed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<String>,
    #[serde(default)]
    pub optional: bool,
}

impl ServiceRequirement {
    pub fn new(interface_id: impl Into<String>, interface_version: impl Into<String>) -> Self {
        Self {
            service_id: None,
            interface_id: interface_id.into(),
            interface_version: interface_version.into(),
            locality: ServiceRequirementLocality::Local,
            caller_capabilities: Vec::new(),
            operations: Vec::new(),
            optional: false,
        }
    }

    pub fn from_service(mut self, service_id: impl Into<String>) -> Self {
        self.service_id = Some(service_id.into());
        self
    }

    pub fn from_cluster(mut self) -> Self {
        self.locality = ServiceRequirementLocality::Cluster;
        self
    }

    pub fn from_anywhere(mut self) -> Self {
        self.locality = ServiceRequirementLocality::Any;
        self
    }

    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    pub fn with_caller_capability(mut self, capability: impl Into<String>) -> Self {
        self.caller_capabilities.push(capability.into());
        self
    }

    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operations.push(operation.into());
        self
    }

    pub fn is_satisfied_by(&self, offer: &ServiceOffer) -> bool {
        self.interface_id == offer.interface_id
            && (self.interface_version == "*" || self.interface_version == offer.interface_version)
            && self
                .service_id
                .as_ref()
                .is_none_or(|service_id| service_id == &offer.service_id)
            && match self.locality {
                ServiceRequirementLocality::Local => true,
                ServiceRequirementLocality::Cluster => offer.locality == ServiceLocality::Cluster,
                ServiceRequirementLocality::Any => true,
            }
            && offer
                .caller_capabilities
                .iter()
                .all(|capability| self.caller_capabilities.contains(capability))
            && self.operations.iter().all(|required| {
                offer
                    .operations
                    .iter()
                    .find(|operation| operation.operation == *required)
                    .map_or_else(
                        || offer.provider_capabilities.contains(required),
                        |operation| {
                            operation
                                .caller_capabilities
                                .iter()
                                .all(|capability| self.caller_capabilities.contains(capability))
                        },
                    )
            })
    }

    pub fn validate(&self) -> Result<(), ComponentContractError> {
        if let Some(service_id) = &self.service_id {
            validate_component_token(service_id, "required service id")?;
        }
        validate_component_token(&self.interface_id, "required service interface id")?;
        if self.interface_version != "*" {
            validate_component_token(
                &self.interface_version,
                "required service interface version",
            )?;
        }
        validate_unique_tokens(
            &self.caller_capabilities,
            "caller capability",
            self.service_id
                .as_deref()
                .unwrap_or(self.interface_id.as_str()),
        )?;
        validate_unique_tokens(
            &self.operations,
            "required operation",
            self.service_id
                .as_deref()
                .unwrap_or(self.interface_id.as_str()),
        )?;
        Ok(())
    }
}

/// Backend-neutral component identity and dependency contract.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComponentDescriptor {
    pub component_id: String,
    pub component_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub artifact_generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub offers: Vec<ServiceOffer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requirements: Vec<ServiceRequirement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_publications: Vec<ComponentEventPublication>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_subscriptions: Vec<ComponentEventSubscription>,
}

impl ComponentDescriptor {
    pub fn new(component_id: impl Into<String>, component_version: impl Into<String>) -> Self {
        Self {
            component_id: component_id.into(),
            component_version: component_version.into(),
            ..Self::default()
        }
    }

    pub fn with_artifact(
        mut self,
        artifact_id: impl Into<String>,
        artifact_generation: u64,
    ) -> Self {
        self.artifact_id = Some(artifact_id.into());
        self.artifact_generation = artifact_generation;
        self
    }

    pub fn with_offer(mut self, offer: ServiceOffer) -> Self {
        self.offers.push(offer);
        self
    }

    pub fn require(mut self, requirement: ServiceRequirement) -> Self {
        self.requirements.push(requirement);
        self
    }

    pub fn publish_event(mut self, publication: ComponentEventPublication) -> Self {
        self.event_publications.push(publication);
        self
    }

    pub fn subscribe_event(mut self, subscription: ComponentEventSubscription) -> Self {
        self.event_subscriptions.push(subscription);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.component_id.is_empty()
            && self.component_version.is_empty()
            && self.artifact_id.is_none()
            && self.artifact_generation == 0
            && self.offers.is_empty()
            && self.requirements.is_empty()
            && self.event_publications.is_empty()
            && self.event_subscriptions.is_empty()
    }

    pub fn validate(&self) -> Result<(), ComponentContractError> {
        validate_component_token(&self.component_id, "component id")?;
        validate_component_token(&self.component_version, "component version")?;
        if let Some(artifact_id) = &self.artifact_id {
            validate_component_token(artifact_id, "component artifact id")?;
        } else if self.artifact_generation != 0 {
            return Err(ComponentContractError::GenerationWithoutArtifact {
                component_id: self.component_id.clone(),
            });
        }

        let mut offers = BTreeSet::new();
        for offer in &self.offers {
            offer.validate()?;
            let key = (
                offer.service_id.as_str(),
                offer.interface_id.as_str(),
                offer.interface_version.as_str(),
            );
            if !offers.insert(key) {
                return Err(ComponentContractError::DuplicateOffer {
                    component_id: self.component_id.clone(),
                    service_id: offer.service_id.clone(),
                    interface_id: offer.interface_id.clone(),
                    interface_version: offer.interface_version.clone(),
                });
            }
        }
        for requirement in &self.requirements {
            requirement.validate()?;
        }
        let mut publications = BTreeSet::new();
        for publication in &self.event_publications {
            publication.validate()?;
            if !publications.insert((
                publication.kind.as_str(),
                publication.schema_id.as_str(),
                publication.schema_version.as_str(),
            )) {
                return Err(ComponentContractError::DuplicateEventContract {
                    component_id: self.component_id.clone(),
                    direction: "publication",
                    kind: publication.kind.clone(),
                });
            }
        }
        let mut subscriptions = BTreeSet::new();
        for subscription in &self.event_subscriptions {
            subscription.validate()?;
            if !subscriptions.insert((
                subscription.kind_pattern.as_str(),
                subscription.schema_id.as_str(),
                subscription.schema_version.as_str(),
            )) {
                return Err(ComponentContractError::DuplicateEventContract {
                    component_id: self.component_id.clone(),
                    direction: "subscription",
                    kind: subscription.kind_pattern.clone(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ComponentContractError {
    #[error("invalid component contract: {0}")]
    InvalidText(String),
    #[error("service `{service_id}` declares invalid route `{route}`")]
    InvalidRoute { service_id: String, route: String },
    #[error("service `{service_id}` repeats route `{route}`")]
    DuplicateRoute { service_id: String, route: String },
    #[error("service `{service_id}` repeats {kind} `{value}`")]
    DuplicateToken {
        service_id: String,
        kind: &'static str,
        value: String,
    },
    #[error(
        "component `{component_id}` repeats service offer `{service_id}` ({interface_id} {interface_version})"
    )]
    DuplicateOffer {
        component_id: String,
        service_id: String,
        interface_id: String,
        interface_version: String,
    },
    #[error("component `{component_id}` declares an artifact generation without an artifact id")]
    GenerationWithoutArtifact { component_id: String },
    #[error("component `{component_id}` repeats event {direction} contract `{kind}`")]
    DuplicateEventContract {
        component_id: String,
        direction: &'static str,
        kind: String,
    },
}

fn validate_dotted_event_name(value: &str, context: &str) -> Result<(), ComponentContractError> {
    validate_component_token(value, context)?;
    if value.split('.').count() < 2 || value.split('.').any(str::is_empty) {
        return Err(ComponentContractError::InvalidText(format!(
            "{context} must contain at least two non-empty dot-separated segments"
        )));
    }
    Ok(())
}

fn validate_component_token(value: &str, context: &str) -> Result<(), ComponentContractError> {
    if value.is_empty() || value != value.trim() {
        return Err(ComponentContractError::InvalidText(format!(
            "{context} must be non-empty without surrounding whitespace"
        )));
    }
    if !value.chars().all(|character| {
        character.is_ascii_alphanumeric()
            || matches!(character, '.' | '-' | '_' | ':' | '/' | '@' | '*')
    }) {
        return Err(ComponentContractError::InvalidText(format!(
            "{context} must contain only ASCII letters, digits, '.', '-', '_', ':', '/', '@', or '*'"
        )));
    }
    Ok(())
}

fn validate_unique_tokens(
    values: &[String],
    kind: &'static str,
    service_id: &str,
) -> Result<(), ComponentContractError> {
    let mut unique = BTreeSet::new();
    for value in values {
        validate_component_token(value, kind)?;
        if !unique.insert(value.as_str()) {
            return Err(ComponentContractError::DuplicateToken {
                service_id: service_id.to_string(),
                kind,
                value: value.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requirements_match_interface_version_identity_and_locality() {
        let offer = ServiceOffer::new("echo", "sinomo.fixture.echo", "1.0.0")
            .cluster_visible()
            .with_route("/fixture/echo");
        assert!(
            ServiceRequirement::new("sinomo.fixture.echo", "1.0.0")
                .from_service("echo")
                .is_satisfied_by(&offer)
        );
        assert!(
            ServiceRequirement::new("sinomo.fixture.echo", "*")
                .from_cluster()
                .is_satisfied_by(&offer)
        );
        assert!(!ServiceRequirement::new("sinomo.fixture.echo", "2.0.0").is_satisfied_by(&offer));
    }

    #[test]
    fn provider_and_caller_capabilities_remain_distinct() {
        let descriptor = ComponentDescriptor::new("fixture.echo", "1.0.0").with_offer(
            ServiceOffer::new("echo", "sinomo.fixture.echo", "1.0.0")
                .with_provider_capability("echo.streaming")
                .require_caller_capability("fixture.echo.invoke")
                .with_route("/fixture/echo"),
        );
        descriptor.validate().expect("component descriptor");
        assert_eq!(
            descriptor.offers[0].provider_capabilities,
            ["echo.streaming"]
        );
        assert_eq!(
            descriptor.offers[0].caller_capabilities,
            ["fixture.echo.invoke"]
        );
    }

    #[test]
    fn caller_authority_and_event_contracts_are_explicit() {
        let offer = ServiceOffer::new("echo", "sinomo.fixture.echo", "1.0.0")
            .require_caller_capability("fixture.echo.invoke");
        assert!(!ServiceRequirement::new("sinomo.fixture.echo", "1.0.0").is_satisfied_by(&offer));
        assert!(
            ServiceRequirement::new("sinomo.fixture.echo", "1.0.0")
                .with_caller_capability("fixture.echo.invoke")
                .is_satisfied_by(&offer)
        );

        let descriptor = ComponentDescriptor::new("fixture.events", "1.0.0")
            .publish_event(ComponentEventPublication::new(
                "fixture.job.completed",
                "sinomo.fixture.job",
                "1.0.0",
            ))
            .subscribe_event(ComponentEventSubscription::new(
                "fixture.job.*",
                "sinomo.fixture.job",
                "1.0.0",
            ));
        descriptor.validate().expect("event contracts");
        assert!(descriptor.event_subscriptions[0].matches_kind("fixture.job.completed"));
        assert!(!descriptor.event_subscriptions[0].matches_kind("fixture.jobs.completed"));
    }

    #[test]
    fn operation_authority_is_scoped_to_its_route() {
        let offer = ServiceOffer::new("jobs", "sinomo.fixture.jobs", "1.0.0")
            .with_operation(
                ServiceOperationOffer::new("fixture.jobs.read")
                    .at_route("/fixture/jobs/read")
                    .require_caller_capability("fixture.jobs.reader"),
            )
            .with_operation(
                ServiceOperationOffer::new("fixture.jobs.delete")
                    .at_route("/fixture/jobs/delete")
                    .require_caller_capability("fixture.jobs.admin"),
            );
        let reader = ServiceRequirement::new("sinomo.fixture.jobs", "1.0.0")
            .with_operation("fixture.jobs.read")
            .with_caller_capability("fixture.jobs.reader");
        assert!(reader.is_satisfied_by(&offer));
        assert!(
            !ServiceRequirement::new("sinomo.fixture.jobs", "1.0.0")
                .with_operation("fixture.jobs.delete")
                .with_caller_capability("fixture.jobs.reader")
                .is_satisfied_by(&offer)
        );
    }
}
