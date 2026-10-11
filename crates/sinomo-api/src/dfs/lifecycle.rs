//! Generic immutable revision references and conditional content-copy values.

use super::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize};

/// Stable identity of one immutable content revision, independent of modules.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRevisionReference {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
}

/// Copy a granted immutable revision through canonical destination authority.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsContentCopyRequest {
    pub source: DfsRevisionReference,
    pub destination_path: String,
    /// None exclusively creates a destination. Replacement requires its exact
    /// current object and revision; a stale destination is never overwritten.
    pub expected_destination: Option<DfsRevisionReference>,
    pub operation_id: String,
}

/// Durable copy outcome; the historical source field name remains wire-stable.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentCopyResponse {
    pub restored_from: DfsRevisionReference,
    pub committed: DfsRevisionReference,
    pub path: String,
}

/// Publish immutable bytes and an owned reference without advancing a head.
/// Provenance remains opaque; the feature module interprets its metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(try_from = "RetainedContentPublicationWire")]
pub struct DfsRetainedContentPublication {
    pub namespace: String,
    pub reference_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<super::DfsModuleMetadataValue>,
}

impl DfsRetainedContentPublication {
    pub fn validate(&self) -> Result<(), String> {
        super::validate_metadata_namespace(&self.namespace)?;
        if self.reference_id.trim().is_empty()
            || self.reference_id.len() > 256
            || self.reference_id.contains('\0')
        {
            return Err("retained content requires a bounded nonempty reference identity".into());
        }
        if let Some(metadata) = &self.metadata {
            let bytes = serde_json::to_vec(&metadata.payload).map_err(|error| error.to_string())?;
            if metadata.schema_version == 0 || bytes.len() > 64 * 1024 {
                return Err(
                    "retained content metadata requires a version and at most 64 KiB".into(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedContentPublicationWire {
    namespace: String,
    reference_id: String,
    #[serde(default)]
    metadata: Option<super::DfsModuleMetadataValue>,
}

impl TryFrom<RetainedContentPublicationWire> for DfsRetainedContentPublication {
    type Error = String;

    fn try_from(wire: RetainedContentPublicationWire) -> Result<Self, Self::Error> {
        let publication = Self {
            namespace: wire.namespace,
            reference_id: wire.reference_id,
            metadata: wire.metadata,
        };
        publication.validate()?;
        Ok(publication)
    }
}

/// Explicit caller-owned immutable bytes with an opaque, namespaced reference.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreservedContentRequest {
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub publication: DfsRetainedContentPublication,
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsPreservedContentOutcome {
    Retained {
        identity: DfsRevisionReference,
    },
    Pending {
        identity: DfsRevisionReference,
        intent_id: String,
    },
}
