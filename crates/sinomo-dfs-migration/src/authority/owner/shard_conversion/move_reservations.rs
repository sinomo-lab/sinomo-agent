//! Destination reservations a converting shard holds for cross-shard
//! directory moves into it.
//!
//! A move adopts its root into the destination shard T together with a
//! Running reservation row, which makes creates and writes at or under the
//! target root Busy on T. Only the move's coordinator releases the row, in
//! its finish frame once the whole subtree has moved or in its abort frame,
//! and both go through T's primary, so neither runs while T is closed. T's
//! conversion therefore carries the row into its image unchanged and Running.
//! The coordinator rebuilds the row from its own job and releases only an
//! identical row, so the carried row is the one it releases after T
//! activates.
//!
//! The row is kept whatever the coordinator reports:
//!
//! - A move parked on T runs no step until T activates; the activation wakes
//!   it (`maintenance::storage_waits` in sinomo-dfs), it adopts the rest of
//!   its subtree and its finish frame releases the row.
//! - A move Running otherwise keeps the contribution rounds from agreeing
//!   until it parks on T or ends.
//! - A coordinator that is closed, unreachable or reports no phase decides
//!   nothing here. Keeping the row only keeps the target subtree Busy, which
//!   is what the move needs until it finishes, and the move releases it once
//!   it can run again. Dropping it instead would let a create under the
//!   target root be replaced by the move's later adoption.
//!
//! What the move has not adopted yet is still the source's; T counts it from
//! the source's contributions (`namespace_counts::contributions::moving`).
use super::*;

/// Whether `job` is a destination reservation the shard keeps through its
/// conversion, as an unsettled current-format row.
pub(super) fn keeps(job: &DfsNamespaceJobRecord) -> bool {
    crate::namespace_counts::contributions::is_kept_destination_reservation(job)
}
