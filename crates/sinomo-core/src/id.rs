use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum IdError {
    #[error("{type_name} must start with {prefix}")]
    InvalidPrefix {
        type_name: &'static str,
        prefix: &'static str,
    },
    #[error("{type_name} value cannot be empty after {prefix}")]
    EmptyValue {
        type_name: &'static str,
        prefix: &'static str,
    },
    #[error("{type_name} contains an invalid character: {character}")]
    InvalidCharacter {
        type_name: &'static str,
        character: char,
    },
}

fn validate_prefixed_id(
    type_name: &'static str,
    prefix: &'static str,
    value: &str,
) -> Result<(), IdError> {
    if !value.starts_with(prefix) {
        return Err(IdError::InvalidPrefix { type_name, prefix });
    }

    let body = &value[prefix.len()..];
    if body.is_empty() {
        return Err(IdError::EmptyValue { type_name, prefix });
    }

    for character in body.chars() {
        if !(character.is_ascii_alphanumeric()
            || character == '_'
            || character == '-'
            || character == '.')
        {
            return Err(IdError::InvalidCharacter {
                type_name,
                character,
            });
        }
    }

    Ok(())
}

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub const PREFIX: &'static str = $prefix;

            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                validate_prefixed_id(stringify!($name), Self::PREFIX, &value)?;
                Ok(Self(value))
            }

            pub fn generate() -> Self {
                let generated = Ulid::new().to_string().to_ascii_lowercase();
                Self(format!("{}{}", Self::PREFIX, generated))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

macro_rules! define_open_id {
    ($name:ident, $generated_prefix:literal) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub const GENERATED_PREFIX: &'static str = $generated_prefix;

            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                validate_open_id(stringify!($name), &value)?;
                Ok(Self(value))
            }

            pub fn generate() -> Self {
                let generated = Ulid::new().to_string().to_ascii_lowercase();
                Self(format!("{}{}", Self::GENERATED_PREFIX, generated))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

fn validate_open_id(type_name: &'static str, value: &str) -> Result<(), IdError> {
    if value.trim().is_empty() {
        return Err(IdError::EmptyValue {
            type_name,
            prefix: "",
        });
    }

    for character in value.chars() {
        if !(character.is_ascii_alphanumeric()
            || character == '_'
            || character == '-'
            || character == '.'
            || character == ':')
        {
            return Err(IdError::InvalidCharacter {
                type_name,
                character,
            });
        }
    }

    Ok(())
}

define_id!(EventId, "evt_");
define_id!(ClusterId, "cluster_");
define_id!(ClusterEndpointId, "endpoint_");
define_id!(ClusterInviteId, "invite_");
define_id!(ClusterCertificateId, "cert_");
define_id!(NodeId, "node_");
define_id!(DeviceId, "device_");
define_id!(SessionId, "sess_");
define_id!(EntityId, "entity_");
define_id!(TaskId, "task_");
define_id!(WorkspaceId, "workspace_");
define_open_id!(DfsFileSystemId, "fs_");
define_id!(DfsObjectId, "obj_");
define_id!(DfsVersion, "version_");
define_id!(ArtifactId, "artifact_");
define_id!(EvolutionJobId, "evojob_");
define_id!(ScorecardId, "score_");
define_id!(RolloutPlanId, "rollout_");
define_id!(PolicyDecisionId, "policy_");

define_open_id!(ModuleId, "module_");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_as_strings() {
        let id = EventId::new("evt_example-01").expect("valid event id");
        let json = serde_json::to_string(&id).expect("serialize id");
        assert_eq!(json, "\"evt_example-01\"");

        let parsed: EventId = serde_json::from_str(&json).expect("deserialize id");
        assert_eq!(parsed, id);
    }

    #[test]
    fn ids_reject_wrong_prefix() {
        let error = EventId::new("node_example").expect_err("wrong prefix");
        assert!(matches!(error, IdError::InvalidPrefix { .. }));
    }

    #[test]
    fn generated_ids_have_prefix() {
        assert!(
            SessionId::generate()
                .as_str()
                .starts_with(SessionId::PREFIX)
        );
    }

    #[test]
    fn module_ids_allow_capsule_style_names() {
        let id = ModuleId::new("module.trigger.silence_policy").expect("module id");
        assert_eq!(id.as_str(), "module.trigger.silence_policy");
        assert!(ModuleId::generate().as_str().starts_with("module_"));
    }
}
