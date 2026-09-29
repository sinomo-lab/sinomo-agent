/// Target hardening: a requester observes replica progress written by another
/// local handle. Notification is a hint; authority and exact durable history
/// are checked before atomically replacing cursor and namespace guards.
/// Disk failure, payload replay and concurrent threads remain Rust-test duties.
module dfs_replica_projection_refresh {
  type State = {
    durable: int, cursor: int, guards: int, observed: int,
    notice: int, epoch: int, noticeEpoch: int, exact: bool,
    refreshed: bool, rejected: bool, dispatched: bool,
  }
  var projection: State
  action init = projection' = {
    durable: 0, cursor: 0, guards: 0, observed: 0,
    notice: 0, epoch: 1, noticeEpoch: 1, exact: true,
    refreshed: false, rejected: false, dispatched: false,
  }
  action externalIngest = all {
    projection.durable < 2,
    projection' = projection
      .with("durable", projection.durable + 1)
      .with("notice", projection.notice + 1)
      .with("noticeEpoch", projection.epoch)
      .with("exact", true)
      .with("dispatched", false),
  }
  action staleAuthority = all {
    projection.epoch == 1,
    projection' = projection.with("epoch", 2).with("dispatched", false),
  }
  action unverifiableNotice = all {
    projection.exact,
    projection' = projection.with("exact", false).with("dispatched", false),
  }
  action refresh = all {
    projection.notice > projection.observed,
    projection.noticeEpoch == projection.epoch,
    projection.exact,
    projection' = projection
      .with("cursor", projection.durable)
      .with("guards", projection.durable)
      .with("observed", projection.notice)
      .with("refreshed", true),
  }
  action rejectRefresh = all {
    projection.notice > projection.observed,
    projection.noticeEpoch != projection.epoch or not(projection.exact),
    projection' = projection.with("rejected", true),
  }
  action dispatch = all {
    projection.observed == projection.notice,
    projection.noticeEpoch == projection.epoch,
    projection.exact,
    projection' = projection.with("dispatched", true),
  }
  action step = any {
    externalIngest, staleAuthority, unverifiableNotice, refresh, rejectRefresh,
    dispatch, projection' = projection,
  }
}
