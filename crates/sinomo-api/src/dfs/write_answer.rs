//! Wire contract for an answer to a routed write that no owner claimed.
//!
//! A requester of this node waits for the answer to a routed write its
//! primary holds. The answer reaches the node's Main, which hands it to the
//! requester's registered response owner. When no attached owner takes it,
//! Main announces the answer instead, as a typed local event that names the
//! request: a requester outside Main registers no owner, and Main keeps
//! nothing for its answer; a registered owner that is not attached has the
//! answer kept for it until it registers again. A requester that holds that
//! request runs the call again: it reads a kept answer that its registration
//! hands over, or sends the same write again under the same id and is
//! answered from the write's row inline. Every other subscriber ignores the
//! event.

use crate::dfs::DfsObjectId;
use serde::{Deserialize, Serialize};

/// Emitted by the local DFS component when an answer to a routed write sent
/// from this node arrives and no attached response owner takes it. Its
/// payload is a [`DfsWriteAnswerUnclaimed`].
pub const DFS_WRITE_ANSWER_UNCLAIMED_EVENT_KIND: &str = "dfs.write.answer_unclaimed";
pub const DFS_WRITE_ANSWER_EVENT_SCHEMA_ID: &str = "sinomo.dfs.write-answer";
pub const DFS_WRITE_ANSWER_EVENT_SCHEMA_VERSION: &str = "1.0.0";

/// The request an unclaimed answer is for.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsWriteAnswerUnclaimed {
    /// The routed write's request id: its caller's operation id, when it
    /// has one.
    pub request_id: String,
    pub object_id: DfsObjectId,
}
