//! Immutable migration plan DATA belongs to the existing native data owner.
//! This memo carries no authority, content proof or reusable receipt.
use super::*;

type Scope = (
    sinomo_core::node::NodeId,
    sinomo_dfs::DfsFileSystemId,
    sinomo_dfs::DfsMetadataShardId,
);

#[derive(Clone, Debug)]
pub(in crate::authority) struct ValidatedPlanData {
    pub(super) plan: MetadataImportPlan,
    pub(super) digest: String,
}
impl ValidatedPlanData {
    pub(super) fn new(plan: MetadataImportPlan) -> Result<Arc<Self>> {
        NativeImportAuthority::validate_plan(&plan)?;
        let digest = super::digest(&plan)?;
        Ok(Arc::new(Self { plan, digest }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(in crate::authority) struct PlanAcknowledgement {
    pub(super) plan_digest: String,
    pub(super) receiver_context: String,
}

struct ReceiverPlan {
    data: Arc<ValidatedPlanData>,
}
struct SourcePlan {
    stamp: Vec<u64>,
    source_writer: Weak<std::fs::File>,
    data: Arc<ValidatedPlanData>,
}
pub(in crate::authority) struct PlanContexts {
    receiver_context: String,
    receivers: BTreeMap<Scope, ReceiverPlan>,
    acknowledged: BTreeMap<Scope, PlanAcknowledgement>,
    sources: BTreeMap<PathBuf, SourcePlan>,
}
impl Default for PlanContexts {
    fn default() -> Self {
        Self {
            receiver_context: sinomo_core::Id::generate().to_string(),
            receivers: BTreeMap::new(),
            acknowledged: BTreeMap::new(),
            sources: BTreeMap::new(),
        }
    }
}
impl PlanContexts {
    pub(super) fn receiver(
        &self,
        scope: &Scope,
        ack: &PlanAcknowledgement,
    ) -> Option<Arc<ValidatedPlanData>> {
        if ack.receiver_context != self.receiver_context {
            return None;
        }
        self.receivers
            .get(scope)
            .filter(|row| row.data.digest == ack.plan_digest)
            .map(|row| Arc::clone(&row.data))
    }
    pub(super) fn publish(
        &mut self,
        scope: Scope,
        data: Arc<ValidatedPlanData>,
    ) -> PlanAcknowledgement {
        let ack = PlanAcknowledgement {
            plan_digest: data.digest.clone(),
            receiver_context: self.receiver_context.clone(),
        };
        self.receivers.insert(scope, ReceiverPlan { data });
        ack
    }
    pub(super) fn acknowledged(&self, scope: &Scope, digest: &str) -> Option<PlanAcknowledgement> {
        self.acknowledged
            .get(scope)
            .filter(|ack| ack.plan_digest == digest)
            .cloned()
    }
    pub(super) fn acknowledge(&mut self, scope: Scope, ack: PlanAcknowledgement) {
        self.acknowledged.insert(scope, ack);
    }
    pub(super) fn forget(&mut self, scope: &Scope, expected: &PlanAcknowledgement) {
        if self.acknowledged.get(scope) == Some(expected) {
            self.acknowledged.remove(scope);
        }
    }
    pub(super) fn source(
        &self,
        path: &Path,
        stamp: &[u64],
        writer: &Arc<std::fs::File>,
    ) -> Option<Arc<ValidatedPlanData>> {
        self.sources
            .get(path)
            .filter(|row| {
                row.stamp == stamp
                    && row
                        .source_writer
                        .upgrade()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, writer))
            })
            .map(|row| Arc::clone(&row.data))
    }
    pub(super) fn retain_source(
        &mut self,
        path: PathBuf,
        stamp: Vec<u64>,
        writer: &Arc<std::fs::File>,
        data: Arc<ValidatedPlanData>,
    ) {
        // Weak custody cannot keep the source writer alive after its actual
        // converter session drains. Changed atomic file identities invalidate.
        self.sources
            .retain(|_, row| row.source_writer.strong_count() > 0);
        self.sources.insert(
            path,
            SourcePlan {
                stamp,
                source_writer: Arc::downgrade(writer),
                data,
            },
        );
    }
}
pub(super) fn scope(
    node: &sinomo_core::node::NodeId,
    request: &ContentVerificationRequest,
) -> Scope {
    (
        node.clone(),
        request.version.file_system_id.clone(),
        request.source.scope_shard.clone(),
    )
}
pub(super) fn wire_scope(
    node: &sinomo_core::node::NodeId,
    request: &ContentVerificationWire,
) -> Scope {
    (
        node.clone(),
        request.version.file_system_id.clone(),
        request.source.scope_shard.clone(),
    )
}
pub(super) fn plan_stamp(path: &Path) -> Result<Option<Vec<u64>>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::metadata(path).map_err(error)?;
        return Ok(Some(vec![
            m.dev(),
            m.ino(),
            m.len(),
            m.mtime() as u64,
            m.mtime_nsec() as u64,
            m.ctime() as u64,
            m.ctime_nsec() as u64,
        ]));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}
