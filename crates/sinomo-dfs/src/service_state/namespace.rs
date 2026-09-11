//! Active namespace execution and its durability evidence have one lifetime.

use super::{DfsNamespaceOperationContext, DfsNamespaceRequestRecord};

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
}

impl DfsNamespaceExecution {
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
