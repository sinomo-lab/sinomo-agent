//! How a converting shard reads what the other shards contribute to its
//! namespace counts.
//!
//! The conversion driver of target shard T asks every other shard S of the
//! filesystem, once per round, for S's contributions toward T. A source answers
//! from its current tables when it is open, or from its old image on its
//! primary when it is still closed. The driver accepts the contributions only
//! when two consecutive full rounds give identical answers and no answer names
//! a non-Completed job touching T, except a job parked on T itself (see
//! [`SourceJobTouchingTarget::parked_on_target`]).
//!
//! In the same read T asks S a question about what S holds: which object ids
//! are live canonical rows S owns, which paths S owns hold a live directory,
//! and which job ids are Running jobs of S. S answers it from the same state
//! as its contributions, and the answer takes part in the comparison of two
//! rounds.
use super::Result;
use crate::namespace_counts::ShardCountContributions;
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_dfs::{DfsCountContributionHoldings, DfsCountContributionQuestion, DfsMetadataShardConfig};
use std::collections::BTreeSet;
use std::future::Future;

/// One source shard's answer toward one target shard in one round.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceContributionAnswer {
    /// The source's live aliases and the child directories whose parent the
    /// target owns, under the census rules.
    pub(crate) contributions: ShardCountContributions,
    /// The source's non-Completed namespace jobs whose effects touch the
    /// target: paths or objects the target owns, or fences and parent
    /// participants held on it. Empty when the source is settled toward the
    /// target.
    pub(crate) running_jobs_touching_target: Vec<SourceJobTouchingTarget>,
    /// What the answer was read from: the source's serving epoch, its
    /// storage record state, and its prefix proof (open source) or source
    /// identity and fingerprint (old image). Two rounds agree only when their
    /// answers are equal, this basis included.
    pub(crate) basis: String,
    /// The entries of the round's question the source holds, read from the
    /// same state as the contributions.
    #[serde(default)]
    pub(crate) held: SourceHoldings,
}

/// What the target asks one source in a round beside its contributions.
pub(crate) type SourceQuestion = DfsCountContributionQuestion;

/// The entries of a [`SourceQuestion`] a source holds.
pub(crate) type SourceHoldings = DfsCountContributionHoldings;

/// One non-Completed job of the source that touches the target, with the
/// phase its coordinator recorded. The target settles its own side of some
/// such jobs from its rows and this phase (DESIGN-BD C.4); a job parked on
/// the target itself does not hold the rounds; every other one keeps them
/// from agreeing.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceJobTouchingTarget {
    pub(crate) job_id: String,
    /// The job family, as its record names it.
    pub(crate) family: String,
    /// The coordinator's recorded phase or step.
    pub(crate) phase: String,
}

/// The phase part a source appends when the job's coordinator recorded that
/// its next step waits for the target's storage to be Active.
pub(crate) const PARKED_ON_TARGET_PHASE: &str = "parked-on-target";

impl SourceJobTouchingTarget {
    /// Whether the job's next step waits for the target's storage. Such a
    /// job runs no step, so it moves none of the target's contributions,
    /// until the target activates; the objects that step carries are still
    /// live rows at their old paths, counted where they are.
    pub(crate) fn parked_on_target(&self) -> bool {
        self.phase.split(';').any(|part| part == PARKED_ON_TARGET_PHASE)
    }
}

/// The outcome of asking one source in one round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SourceContributionRead {
    Answered(SourceContributionAnswer),
    /// No member of the source can answer now. The driver parks on these
    /// nodes' reachability and these KV keys; it never treats the source as
    /// empty.
    Waiting {
        nodes: BTreeSet<NodeId>,
        keys: BTreeSet<String>,
    },
}

/// Reads one source's contributions toward one target.
pub(crate) trait ContributionSource: Send + Sync {
    fn read(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
    ) -> impl Future<Output = Result<SourceContributionRead>> + Send;

    /// [`Self::read`], also answering `question` in the answer's `held`. A
    /// source that answers no questions refuses a non-empty one, so an
    /// unasked source is never taken to hold nothing.
    fn read_asking(
        &self,
        target: &DfsMetadataShardConfig,
        source: &DfsMetadataShardConfig,
        question: &SourceQuestion,
    ) -> impl Future<Output = Result<SourceContributionRead>> + Send {
        let asked = !question.is_empty();
        let read = self.read(target, source);
        async move {
            if asked {
                return Err(super::error("this contribution source answers no question"));
            }
            read.await
        }
    }
}
