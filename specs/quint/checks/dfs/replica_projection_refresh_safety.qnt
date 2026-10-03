module dfs_replica_projection_refresh_safety {
  import dfs_replica_projection_refresh.* from "../../models/dfs/replica_projection_refresh"
  def safety = all {
    projection.cursor == projection.guards,
    projection.cursor <= projection.durable,
    projection.observed <= projection.notice,
    projection.dispatched implies all {
      projection.cursor == projection.durable,
      projection.observed == projection.notice,
      projection.noticeEpoch == projection.epoch,
      projection.exact,
    },
  }
  def witnessExternalAdvanceRefreshesBeforeDispatch =
    projection.dispatched and projection.refreshed and projection.durable > 0
  def witnessRepeatedAdvanceRefreshesBothProjections =
    projection.dispatched and projection.cursor == 2 and projection.guards == 2
  def witnessStaleAuthorityCannotRefresh =
    projection.rejected and projection.noticeEpoch != projection.epoch
  def witnessUnverifiableNoticeCannotRefresh =
    projection.rejected and not(projection.exact)
}
