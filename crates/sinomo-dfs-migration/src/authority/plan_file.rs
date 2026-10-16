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
}
struct Checkpoint {
    bytes: Vec<u8>,
    plan: Arc<MetadataImportPlan>,
}
impl RetainedPlanFile {
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
    let original = retained.read(&first).unwrap();
    assert!(Arc::ptr_eq(&original, &retained.read(&first).unwrap()));
    assert!(Arc::ptr_eq(&original, &retained.read(&second).unwrap()));
    assert_eq!(serde_json::to_vec(original.as_ref()).unwrap(), bytes);

    let mut changed = plan.clone();
    let fingerprint = &mut changed.facts.current_image_fingerprint;
    let last = fingerprint.pop().expect("actual nonempty candidate digest");
    fingerprint.push(if last == '0' { '1' } else { '0' });
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
}
