use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum IdError {
    #[error("{type_name} cannot be empty")]
    Empty { type_name: &'static str },
    #[error("{type_name} must be a raw lowercase ULID")]
    InvalidRawUlid { type_name: &'static str },
    #[error("{type_name} contains an invalid character: {character}")]
    InvalidCharacter {
        type_name: &'static str,
        character: char,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RawUlid(String);

impl RawUlid {
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        validate_raw_ulid("RawUlid", &value)?;
        Ok(Self(value))
    }

    pub fn from_legacy_named(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        validate_legacy_named_id("RawUlid", &value)?;
        Ok(Self::derived("legacy-named-id", value.as_bytes()))
    }

    pub fn generate() -> Self {
        Self(Ulid::new().to_string().to_ascii_lowercase())
    }

    pub fn derived(namespace: &str, value: impl AsRef<[u8]>) -> Self {
        let mut hash = 0x6a09_e667_f3bc_c909_bb67_ae85_84ca_a73b_u128;
        for byte in namespace
            .as_bytes()
            .iter()
            .chain(std::iter::once(&0))
            .chain(value.as_ref())
        {
            hash ^= u128::from(*byte);
            hash = hash.wrapping_mul(0x0000_0000_0000_0001_0000_0000_0000_013b_u128);
            hash = hash.rotate_left(17);
        }
        Self(Ulid::from(hash).to_string().to_ascii_lowercase())
    }

    pub fn deserialize_migrating_legacy(value: impl Into<String>) -> Result<Self, IdError> {
        Self::new(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RawUlid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RawUlid {
    type Err = IdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for RawUlid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RawUlid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::deserialize_migrating_legacy(value).map_err(serde::de::Error::custom)
    }
}

fn validate_id(type_name: &'static str, value: &str) -> Result<(), IdError> {
    if value.trim().is_empty() {
        return Err(IdError::Empty { type_name });
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

fn validate_raw_ulid(type_name: &'static str, value: &str) -> Result<(), IdError> {
    if value.trim().is_empty() {
        return Err(IdError::Empty { type_name });
    }
    if value.len() != 26 || value != value.to_ascii_lowercase() {
        return Err(IdError::InvalidRawUlid { type_name });
    }
    Ulid::from_string(value).map_err(|_| IdError::InvalidRawUlid { type_name })?;
    Ok(())
}

fn validate_legacy_named_id(type_name: &'static str, value: &str) -> Result<(), IdError> {
    validate_id(type_name, value)
}

pub type EventId = RawUlid;
pub type ClusterId = RawUlid;
pub type ClusterEndpointId = RawUlid;
pub type ClusterInviteId = RawUlid;
pub type ClusterCertificateId = RawUlid;
pub type NodeId = RawUlid;
pub type DeviceId = RawUlid;
pub type SessionId = RawUlid;
pub type EntityId = RawUlid;
pub type TaskId = RawUlid;
pub type WorkspaceId = RawUlid;
pub type DfsFileSystemId = RawUlid;
pub type DfsObjectId = RawUlid;
pub type DfsVersion = RawUlid;
pub type ArtifactId = RawUlid;
pub type EvolutionJobId = RawUlid;
pub type ScorecardId = RawUlid;
pub type RolloutPlanId = RawUlid;
pub type PolicyDecisionId = RawUlid;
pub type ModuleId = RawUlid;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_as_strings() {
        let id = EventId::new("01hxy6qa9k0t9vr4g9n8d3q5bf").expect("valid event id");
        let json = serde_json::to_string(&id).expect("serialize id");
        assert_eq!(json, "\"01hxy6qa9k0t9vr4g9n8d3q5bf\"");

        let parsed: EventId = serde_json::from_str(&json).expect("deserialize id");
        assert_eq!(parsed, id);
    }

    #[test]
    fn ids_reject_empty_values() {
        let error = EventId::new(" ").expect_err("empty id");
        assert!(matches!(error, IdError::Empty { .. }));
    }

    #[test]
    fn ids_reject_named_values_on_primary_constructor() {
        let error = ModuleId::new("module.trigger.silence_policy").expect_err("named id");
        assert!(matches!(error, IdError::InvalidRawUlid { .. }));
    }

    #[test]
    fn generated_ids_are_raw_ulids_without_prefixes() {
        let id = SessionId::generate();

        assert_eq!(id.as_str().len(), 26);
        assert!(Ulid::from_string(id.as_str()).is_ok());
        assert!(!id.as_str().contains('_'));
    }

    #[test]
    fn derived_ids_are_stable_raw_ulids_without_prefixes() {
        let id = ClusterEndpointId::derived("test.endpoint", b"node-a");

        assert_eq!(id, ClusterEndpointId::derived("test.endpoint", b"node-a"));
        assert_eq!(id.as_str().len(), 26);
        assert!(Ulid::from_string(id.as_str()).is_ok());
        assert!(!id.as_str().contains('_'));
    }

    #[test]
    fn legacy_named_fixture_ids_are_derived_raw_ulids() {
        let id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module id");
        assert_eq!(
            id,
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module id")
        );
        assert_eq!(id.as_str().len(), 26);
        assert!(Ulid::from_string(id.as_str()).is_ok());
        assert!(!id.as_str().contains('.'));
    }
}
