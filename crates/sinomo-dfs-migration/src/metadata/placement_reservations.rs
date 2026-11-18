//! Non-executable original placement evidence: a shard's share of the old
//! placement reconciliation goals, bound into its captured copy.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetainedPlacementReservation {
    capsule: Value,
}

impl RetainedPlacementReservation {
    pub(crate) fn from_capsule(capsule: Value) -> Result<Self> {
        let evidence = Self { capsule };
        evidence.reference()?;
        Ok(evidence)
    }

    pub(crate) fn reference(&self) -> Result<MetadataDataReservationRef> {
        const FIELDS: &[&str] = &[
            "capsule_kind",
            "capsule_digest",
            "source_entry_digest",
            "source_row",
            "original_source_scope",
            "disposition",
            "required_native_work",
        ];
        if self.capsule.as_object().is_none_or(|fields| {
            fields.len() != FIELDS.len() || fields.keys().any(|key| !FIELDS.contains(&key.as_str()))
        }) {
            return Err(error(
                "retained placement capsule changes its exact evidence fields",
            ));
        }
        crate::authority::placement_job_source::OriginalPlacementJob::verify_capsule(&self.capsule)
    }
}

pub(crate) fn validate_placement_reservations(
    reservations: &[RetainedPlacementReservation],
    file_system_ids: &[DfsFileSystemId],
    shard: &DfsMetadataShardId,
) -> Result<()> {
    let mut previous = None;
    for evidence in reservations {
        let reference = evidence.reference()?;
        if &reference.original_shard_id != shard
            || file_system_ids != [reference.file_system_id.clone()]
            || previous.as_ref().is_some_and(|prior| prior >= &reference)
        {
            return Err(error(
                "retained placement evidence is foreign, repeated, or not canonical",
            ));
        }
        previous = Some(reference);
    }
    Ok(())
}
