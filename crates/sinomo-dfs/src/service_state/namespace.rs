//! Active namespace execution and its durability evidence have one lifetime.

use super::{DfsNamespaceOperationContext, DfsNamespaceRequestRecord};
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectMetadata, DfsVersion};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Observations made while constructing a namespace mutation, before any
/// configuration/authority work can refresh the backend. These are comparison
/// predicates, not authority. The existing prepare certificate still authorizes
/// publication and the backend still validates them atomically.
#[derive(Clone, Debug, Default)]
pub(crate) struct NamespaceReadSet {
    pub(crate) objects: BTreeMap<(DfsFileSystemId, DfsObjectId), Option<DfsVersion>>,
    pub(crate) paths: BTreeMap<(DfsFileSystemId, String), Option<DfsObjectId>>,
}

#[derive(Debug, Default)]
struct NamespacePlanningState {
    depth: usize,
    reads: NamespaceReadSet,
}

impl NamespaceReadSet {
    pub(crate) fn bind_expectations(
        &self,
        file_system_id: &DfsFileSystemId,
        objects: &mut [crate::DfsNamespaceObjectVersionExpectation],
        paths: &mut [crate::DfsNamespacePathOccupantExpectation],
    ) {
        for expected in objects {
            expected.expected_version = self
                .objects
                .get(&(file_system_id.clone(), expected.object_id.clone()))
                .cloned()
                .unwrap_or(None);
        }
        for expected in paths {
            expected.expected_object_id = self
                .paths
                .get(&(
                    file_system_id.clone(),
                    crate::namespace::metadata_path(&expected.path),
                ))
                .cloned()
                .unwrap_or(None);
        }
    }
}

pub(crate) struct NamespacePlanningGuard {
    state: Arc<Mutex<NamespacePlanningState>>,
    suspended: Option<NamespacePlanningState>,
}

impl Drop for NamespacePlanningGuard {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(suspended) = self.suspended.take() {
            *state = suspended;
            return;
        }
        state.depth = state.depth.saturating_sub(1);
        if state.depth == 0 {
            state.reads = NamespaceReadSet::default();
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NamespaceDurability {
    pub(crate) prepare_recorded: bool,
    pub(crate) job_recorded: bool,
}

#[derive(Debug, Default)]
pub(crate) struct DfsNamespaceExecution {
    context: Option<DfsNamespaceOperationContext>,
    request: Option<DfsNamespaceRequestRecord>,
    durability: NamespaceDurability,
    planning: Arc<Mutex<NamespacePlanningState>>,
}

impl DfsNamespaceExecution {
    pub(crate) fn begin_planning(&self) -> NamespacePlanningGuard {
        let mut state = self
            .planning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.depth == 0 {
            state.reads = NamespaceReadSet::default();
        }
        state.depth += 1;
        NamespacePlanningGuard {
            state: Arc::clone(&self.planning),
            suspended: None,
        }
    }

    /// A separately planned publication (or a request served while waiting for
    /// another request) must collect its own inputs. Its caller's prepared
    /// records retain their original comparisons when this phase completes.
    pub(crate) fn begin_independent_planning(&self) -> NamespacePlanningGuard {
        let mut state = self
            .planning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let suspended = std::mem::replace(
            &mut *state,
            NamespacePlanningState {
                depth: 1,
                reads: NamespaceReadSet::default(),
            },
        );
        NamespacePlanningGuard {
            state: Arc::clone(&self.planning),
            suspended: Some(suspended),
        }
    }

    pub(crate) fn captured_reads(&self) -> Option<NamespaceReadSet> {
        let state = self
            .planning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.depth > 0).then(|| state.reads.clone())
    }

    pub(crate) fn observe_object(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        object: Option<&DfsObjectMetadata>,
    ) {
        let mut state = self
            .planning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.depth == 0 {
            return;
        }
        let version = object.map(|object| object.version.clone());
        let key = (file_system_id.clone(), object_id.clone());
        // Keep the original predicate even if a later lookup sees a successor.
        // Only predicates in a publication's footprint are bound and checked;
        // other phases of a multi-step operation may legitimately advance.
        state.reads.objects.entry(key).or_insert(version);
    }

    pub(crate) fn observe_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        object: Option<&DfsObjectMetadata>,
    ) {
        if let Some(object) = object {
            self.observe_object(file_system_id, &object.object_id, Some(object));
        }
        let mut state = self
            .planning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.depth == 0 {
            return;
        }
        let key = (
            file_system_id.clone(),
            crate::namespace::metadata_path(path),
        );
        let occupant = object.map(|object| object.object_id.clone());
        state.reads.paths.entry(key).or_insert(occupant);
    }

    pub(crate) fn publication_completed(&self, objects: &[DfsObjectMetadata]) {
        let written = objects
            .iter()
            .map(|o| (o.file_system_id.clone(), o.object_id.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        let paths = objects
            .iter()
            .map(|o| {
                (
                    o.file_system_id.clone(),
                    crate::namespace::metadata_path(&o.path),
                )
            })
            .collect::<std::collections::BTreeSet<_>>();
        let mut state = self
            .planning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reads.objects.retain(|key, _| !written.contains(key));
        state.reads.paths.retain(|key, occupant| {
            !paths.contains(key)
                && !occupant
                    .as_ref()
                    .is_some_and(|id| written.contains(&(key.0.clone(), id.clone())))
        });
    }

    pub(crate) fn begin_request(&mut self, request: Option<DfsNamespaceRequestRecord>) {
        self.request = request;
        self.durability = NamespaceDurability::default();
    }

    /// The caller decides terminal versus pending from the returned evidence.
    /// Clearing the request can never leave stale durability flags behind.
    pub(crate) fn finish_request(&mut self) -> NamespaceDurability {
        self.request = None;
        std::mem::take(&mut self.durability)
    }

    pub(crate) fn request(&self) -> Option<&DfsNamespaceRequestRecord> {
        self.request.as_ref()
    }

    pub(crate) fn take_request(&mut self) -> Option<DfsNamespaceRequestRecord> {
        self.request.take()
    }

    pub(crate) fn restore_request(&mut self, request: Option<DfsNamespaceRequestRecord>) {
        self.request = request;
    }

    pub(crate) fn record_prepare(&mut self) {
        self.durability.prepare_recorded = true;
    }
    pub(crate) fn record_job(&mut self) {
        self.durability.job_recorded = true;
    }

    /// Intermediate cross-shard publications retain feed provenance without
    /// completing the outer request. Job evidence is deliberately retained.
    pub(crate) fn suspend_request_prepare(&mut self) -> (Option<DfsNamespaceRequestRecord>, bool) {
        (
            self.request.take(),
            std::mem::take(&mut self.durability.prepare_recorded),
        )
    }

    pub(crate) fn restore_request_prepare(
        &mut self,
        saved: (Option<DfsNamespaceRequestRecord>, bool),
    ) {
        self.request = saved.0;
        self.durability.prepare_recorded = saved.1;
    }

    pub(crate) fn context(&self) -> Option<&DfsNamespaceOperationContext> {
        self.context.as_ref()
    }
    pub(crate) fn context_mut(&mut self) -> Option<&mut DfsNamespaceOperationContext> {
        self.context.as_mut()
    }

    pub(crate) fn replace_context(
        &mut self,
        context: Option<DfsNamespaceOperationContext>,
    ) -> Option<DfsNamespaceOperationContext> {
        std::mem::replace(&mut self.context, context)
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.context.is_none()
            && self.request.is_none()
            && self.durability == NamespaceDurability::default()
            && self
                .planning
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .depth
                == 0
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finishing_request_returns_evidence_and_clears_its_lifetime() {
        let mut execution = DfsNamespaceExecution::default();
        execution.begin_request(None);
        execution.record_prepare();
        execution.record_job();
        assert!(!execution.is_quiescent());
        assert_eq!(
            execution.finish_request(),
            NamespaceDurability {
                prepare_recorded: true,
                job_recorded: true
            }
        );
        assert!(execution.is_quiescent());
        execution.begin_request(None);
        assert_eq!(execution.finish_request(), NamespaceDurability::default());
    }

    #[test]
    fn intermediate_publication_preserves_outer_prepare_and_job_evidence() {
        let mut execution = DfsNamespaceExecution::default();
        execution.record_prepare();
        execution.record_job();
        let saved = execution.suspend_request_prepare();
        assert_eq!(
            execution.durability,
            NamespaceDurability {
                prepare_recorded: false,
                job_recorded: true
            }
        );
        execution.restore_request_prepare(saved);
        assert_eq!(
            execution.finish_request(),
            NamespaceDurability {
                prepare_recorded: true,
                job_recorded: true
            }
        );
    }
}
