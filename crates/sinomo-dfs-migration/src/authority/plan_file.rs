//! One retained, decoded blueprint within the actual migration component.
//! This is immutable DATA validation, never a cached native authority decision.
use super::{MetadataImportPlan, NativeImportAuthority, Result, error};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(super) struct RetainedPlanFile {
    // One entry per component owner, shared by its existing bounded calls.
    // No path inventory, mtime shortcut, persisted capability or background job.
    checkpoint: Mutex<Option<Checkpoint>>,
    challenge: Mutex<Option<ChallengeCheckpoint>>,
    candidate: Mutex<
        Option<(
            std::path::PathBuf,
            Arc<sinomo_dfs::DfsMetadataCandidateInspection>,
        )>,
    >,
}
struct ChallengeCheckpoint {
    bytes: Vec<u8>,
    value: Arc<super::staged_service::SourceFloorChallenge>,
}
struct Checkpoint {
    bytes: Vec<u8>,
    plan: Arc<MetadataImportPlan>,
}
impl RetainedPlanFile {
    /// One physical candidate's full decoder result. This is DATA only: every
    /// call verifies current framing, EOF and raw bytes at the caller's root.
    pub(super) fn inspect_candidate(
        &self,
        root: &Path,
    ) -> Result<sinomo_dfs::DfsMetadataCandidateIdentity> {
        let retained = self
            .candidate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|(path, _)| path == root)
            .map(|(_, value)| Arc::clone(value));
        if let Some(retained) = retained {
            return retained.verify(root);
        }
        self.retain_candidate(
            root,
            sinomo_dfs::DfsMetadataCandidateInspection::capture(root)?,
        )
    }

    pub(super) fn retain_candidate(
        &self,
        root: &Path,
        inspection: sinomo_dfs::DfsMetadataCandidateInspection,
    ) -> Result<sinomo_dfs::DfsMetadataCandidateIdentity> {
        let actual = inspection.verify(root)?;
        *self.candidate.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((root.to_owned(), Arc::new(inspection)));
        Ok(actual)
    }

    /// One associated immutable ancestry challenge. Every use still reads all
    /// actual file bytes; only an identical document reuses its decoding. The
    /// caller must freshly validate source custody, floors, scope and authority.
    pub(super) fn read_challenge(
        &self,
        path: &Path,
    ) -> Result<Arc<super::staged_service::SourceFloorChallenge>> {
        let bytes = std::fs::read(path).map_err(error)?;
        {
            let saved = self.challenge.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(saved) = saved.as_ref().filter(|saved| saved.bytes == bytes) {
                return Ok(Arc::clone(&saved.value));
            }
        }
        let value = Arc::new(serde_json::from_slice(&bytes).map_err(error)?);
        *self.challenge.lock().unwrap_or_else(|e| e.into_inner()) = Some(ChallengeCheckpoint {
            bytes,
            value: Arc::clone(&value),
        });
        Ok(value)
    }

    /// Reuse only the pure validation of the complete, unchanged typed DATA.
    /// The existing single file entry supplies this result; native admission,
    /// receipts, fences and byte reads remain at their original call sites.
    pub(super) fn validate(&self, plan: &MetadataImportPlan) -> Result<()> {
        let retained = self
            .checkpoint
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|saved| Arc::clone(&saved.plan));
        if retained
            .as_ref()
            .is_some_and(|saved| same_plan(saved, plan))
        {
            return Ok(());
        }
        NativeImportAuthority::validate_plan(plan)
    }

    pub(super) fn read(&self, path: &Path) -> Result<Arc<MetadataImportPlan>> {
        // Every call reads the entire actual file. Replacement, same-size edits,
        // truncation and missing files cannot reuse the previous parsed value.
        let bytes = std::fs::read(path).map_err(error)?;
        {
            let saved = self.checkpoint.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(saved) = saved.as_ref().filter(|saved| saved.bytes == bytes) {
                return Ok(Arc::clone(&saved.plan));
            }
        }
        // Do not hold this data-cache mutex across parsing or validation. A
        // concurrent caller can complete its own exact read independently.
        let plan = serde_json::from_slice(&bytes).map_err(error)?;
        NativeImportAuthority::validate_plan(&plan)?;
        let plan = Arc::new(plan);
        *self.checkpoint.lock().unwrap_or_else(|e| e.into_inner()) = Some(Checkpoint {
            bytes,
            plan: Arc::clone(&plan),
        });
        Ok(plan)
    }
}

/// Typed equality covers every field, including captured native evidence.
/// Value's numeric equality alone is insufficient: -0.0 and 0.0 serialize
/// differently. Preserve that distinction in every opaque JSON field too.
pub(super) fn same_plan(left: &MetadataImportPlan, right: &MetadataImportPlan) -> bool {
    left == right
        && same_rows(
            &left.pending_operation_mapping,
            &right.pending_operation_mapping,
        )
        && same_rows(
            &left.combined_layout_source_fences,
            &right.combined_layout_source_fences,
        )
        && same_source_cut(&left.source_cut, &right.source_cut)
}

pub(super) fn same_source_cut(
    left: &super::MetadataSourceCut,
    right: &super::MetadataSourceCut,
) -> bool {
    left == right
        // Path equality normalizes separators and `.` components. Saved plan
        // identity used the serialized path, so retain its original spelling.
        && left.source_root.as_os_str() == right.source_root.as_os_str()
        && left.staged_root.as_os_str() == right.staged_root.as_os_str()
        && same_rows(&left.pending_operations, &right.pending_operations)
}

fn same_rows(left: &[serde_json::Value], right: &[serde_json::Value]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| same_json(a, b))
}

fn same_json(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (left, right) {
        (Value::Number(a), Value::Number(b)) if a.is_f64() || b.is_f64() => {
            a.to_string() == b.to_string()
        }
        (Value::Array(a), Value::Array(b)) => same_rows(a, b),
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().zip(b).all(|((key, value), (other_key, other))| {
                    key == other_key && same_json(value, other)
                })
        }
        _ => left == right,
    }
}

#[cfg(test)]
pub(super) fn assert_exact_native_plan_retention(plan: &MetadataImportPlan) {
    // The caller supplies the actual plan produced by its normal native owner.
    // Mutate only separate fixture copies, never that owner's blueprint/source.
    let directory = sinomo_testdir::TestDir::new("native-retained-plan-file");
    let first = directory.join("first.json");
    let second = directory.join("second.json");
    let bytes = serde_json::to_vec(plan).unwrap();
    std::fs::write(&first, &bytes).unwrap();
    std::fs::write(&second, &bytes).unwrap();
    let retained = RetainedPlanFile::default();
    let challenge_path = directory.join("challenge.json");
    let challenge = super::staged_service::SourceFloorChallenge {
        request_id: "saved-native-data-challenge".into(),
        plan: plan.clone(),
    };
    let challenge_bytes = serde_json::to_vec(&challenge).unwrap();
    std::fs::write(&challenge_path, &challenge_bytes).unwrap();
    let saved = retained.read_challenge(&challenge_path).unwrap();
    assert!(same_plan(&saved.plan, plan));
    assert!(Arc::ptr_eq(
        &saved,
        &retained.read_challenge(&challenge_path).unwrap()
    ));
    let mut changed = challenge.clone();
    changed.request_id.replace_range(..1, "x");
    let changed_bytes = serde_json::to_vec(&changed).unwrap();
    assert_eq!(changed_bytes.len(), challenge_bytes.len());
    std::fs::write(&challenge_path, changed_bytes).unwrap();
    let decoded = retained.read_challenge(&challenge_path).unwrap();
    assert!(!Arc::ptr_eq(&saved, &decoded));
    assert_eq!(decoded.request_id, changed.request_id);
    std::fs::write(
        &challenge_path,
        &challenge_bytes[..challenge_bytes.len() - 1],
    )
    .unwrap();
    assert!(retained.read_challenge(&challenge_path).is_err());
    std::fs::remove_file(&challenge_path).unwrap();
    assert!(retained.read_challenge(&challenge_path).is_err());
    std::fs::write(&challenge_path, &challenge_bytes).unwrap();
    let restarted = RetainedPlanFile::default();
    assert!(same_plan(
        &restarted.read_challenge(&challenge_path).unwrap().plan,
        plan
    ));
    let original = retained.read(&first).unwrap();
    retained.validate(plan).unwrap();
    assert!(Arc::ptr_eq(&original, &retained.read(&first).unwrap()));
    assert!(Arc::ptr_eq(&original, &retained.read(&second).unwrap()));
    assert_eq!(serde_json::to_vec(original.as_ref()).unwrap(), bytes);

    let mut changed = plan.clone();
    let fingerprint = &mut changed.facts.current_image_fingerprint;
    let last = fingerprint.pop().expect("actual nonempty candidate digest");
    fingerprint.push(if last == '0' { '1' } else { '0' });
    assert!(!same_plan(plan, &changed));
    assert!(
        retained.validate(&changed).is_err(),
        "changed typed data must earn validation again"
    );
    let changed = serde_json::to_vec(&changed).unwrap();
    assert_eq!(changed.len(), bytes.len());
    std::fs::write(&first, &changed).unwrap();
    assert!(
        retained.read(&first).is_err(),
        "same-size changed plan is revalidated"
    );
    std::fs::write(&first, &bytes[..bytes.len() - 1]).unwrap();
    assert!(
        retained.read(&first).is_err(),
        "truncation cannot reuse saved DATA"
    );
    std::fs::remove_file(&first).unwrap();
    assert!(
        retained.read(&first).is_err(),
        "missing file is not a retained receipt"
    );
    std::fs::write(&first, &bytes).unwrap();
    assert!(Arc::ptr_eq(&original, &retained.read(&first).unwrap()));
    let restarted = RetainedPlanFile::default();
    let reopened = restarted.read(&first).unwrap();
    assert!(!Arc::ptr_eq(&original, &reopened));
    assert_eq!(serde_json::to_vec(reopened.as_ref()).unwrap(), bytes);
    let mut equivalent = bytes.clone();
    equivalent.push(b'\n');
    std::fs::write(&first, equivalent).unwrap();
    let replaced = retained.read(&first).unwrap();
    assert!(
        !Arc::ptr_eq(&original, &replaced),
        "different bytes are decoded again"
    );
    assert_eq!(serde_json::to_vec(replaced.as_ref()).unwrap(), bytes);
    let revisited = retained.read(&second).unwrap();
    assert!(
        !Arc::ptr_eq(&original, &revisited),
        "only one parsed entry is retained"
    );
    assert_eq!(std::fs::read(&second).unwrap(), bytes);

    // Opaque payload values retain canonical byte distinctions, including
    // floats that serde_json's ordinary numeric equality equates.
    let mut signed_zero = plan.clone();
    signed_zero
        .facts
        .source_cut
        .pending_operations
        .push(serde_json::json!({"nested": [-0.0]}));
    let mut positive_zero = signed_zero.clone();
    *positive_zero
        .facts
        .source_cut
        .pending_operations
        .last_mut()
        .unwrap() = serde_json::json!({"nested": [0.0]});
    assert_eq!(signed_zero, positive_zero);
    assert!(!same_plan(&signed_zero, &positive_zero));
    assert_ne!(
        super::digest(&signed_zero).unwrap(),
        super::digest(&positive_zero).unwrap()
    );
    assert!(same_plan(plan, &plan.clone()));
    let mut changed_path = plan.clone();
    changed_path.facts.source_cut.source_root.push(".");
    assert_eq!(
        changed_path.facts.source_cut.source_root,
        plan.source_cut.source_root
    );
    assert!(!same_plan(plan, &changed_path));
    assert_ne!(
        super::digest(plan).unwrap(),
        super::digest(&changed_path).unwrap()
    );
    let mut changed_capture = plan.clone();
    changed_capture
        .captured_entries
        .pop()
        .expect("actual captured native entries");
    assert!(!same_plan(plan, &changed_capture));
}
