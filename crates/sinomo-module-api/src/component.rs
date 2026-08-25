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

    pub fn is_empty(&self) -> bool {
        self.component_id.is_empty()
            && self.component_version.is_empty()
            && self.artifact_id.is_none()
            && self.artifact_generation == 0
            && self.offers.is_empty()
            && self.requirements.is_empty()
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
}
