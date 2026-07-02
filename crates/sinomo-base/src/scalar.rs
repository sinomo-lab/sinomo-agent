use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum ScalarError {
    #[error("{type_name} cannot be empty")]
    Empty { type_name: &'static str },
    #[error("{type_name} contains an invalid character: {character}")]
    InvalidCharacter {
        type_name: &'static str,
        character: char,
    },
    #[error("{type_name} must contain at least two dot-separated segments")]
    InvalidDottedName { type_name: &'static str },
    #[error("{type_name} must be finite and between 0.0 and 1.0")]
    InvalidScore { type_name: &'static str },
}

fn validate_non_empty(type_name: &'static str, value: &str) -> Result<(), ScalarError> {
    if value.trim().is_empty() {
        return Err(ScalarError::Empty { type_name });
    }
    Ok(())
}

fn validate_token_chars(type_name: &'static str, value: &str) -> Result<(), ScalarError> {
    validate_non_empty(type_name, value)?;
    for character in value.chars() {
        if !(character.is_ascii_alphanumeric()
            || character == '_'
            || character == '-'
            || character == '.'
            || character == ':'
            || character == '/')
        {
            return Err(ScalarError::InvalidCharacter {
                type_name,
                character,
            });
        }
    }
    Ok(())
}

fn validate_dotted_name(type_name: &'static str, value: &str) -> Result<(), ScalarError> {
    validate_token_chars(type_name, value)?;
    let mut segments = value.split('.');
    if segments.clone().count() < 2 || segments.any(str::is_empty) {
        return Err(ScalarError::InvalidDottedName { type_name });
    }
    Ok(())
}

macro_rules! define_string_scalar {
    ($name:ident, $validator:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ScalarError> {
                let value = value.into();
                $validator(stringify!($name), &value)?;
                Ok(Self(value))
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
            type Err = ScalarError;

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

macro_rules! define_score {
    ($name:ident) => {
        #[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
        pub struct $name(f32);

        impl $name {
            pub fn new(value: f32) -> Result<Self, ScalarError> {
                if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                    return Err(ScalarError::InvalidScore {
                        type_name: stringify!($name),
                    });
                }
                Ok(Self(value))
            }

            pub fn get(self) -> f32 {
                self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_f32(self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = f32::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

define_string_scalar!(Source, validate_token_chars);
define_string_scalar!(EventKind, validate_dotted_name);
define_string_scalar!(CapabilityName, validate_dotted_name);
define_string_scalar!(TrustDomain, validate_token_chars);
define_string_scalar!(EntityRef, validate_token_chars);
define_string_scalar!(RoutingHint, validate_token_chars);

define_score!(Confidence);
define_score!(Salience);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_kind_requires_dotted_namespace() {
        assert!(EventKind::new("speech.transcript.final").is_ok());
        assert!(EventKind::new("speech").is_err());
        assert!(EventKind::new("speech..final").is_err());
    }

    #[test]
    fn bounded_scores_reject_out_of_range_values() {
        assert!(Confidence::new(0.0).is_ok());
        assert!(Confidence::new(1.0).is_ok());
        assert!(Confidence::new(1.1).is_err());
        assert!(Salience::new(f32::NAN).is_err());
    }
}
