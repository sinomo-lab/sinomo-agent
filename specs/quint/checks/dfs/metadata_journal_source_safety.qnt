/// Invariant checks for `dfs_metadata_journal_source`.
module dfs_metadata_journal_source_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_journal_source.* from "../../models/dfs/metadata_journal_source"

  def acceptedMatchesImplementationPredicate: bool =
    accepted.forall(case => canAccept(case))


  def rejectedMatchesImplementationPredicate: bool =
    rejected.forall(case => not(canAccept(case)))


  def serviceTransportClaimMismatchesRejected: bool =
    accepted.forall(case =>
      case.surface == ServiceJournal or case.surface == ServiceSnapshot
        implies claimedSourceMatchesTransport(case)
    )


  def replicationJournalWithContextUsesPrimarySource: bool =
    accepted.forall(case =>
      case.surface == ReplicationJournal and case.hasContext
        implies claimedSourceIsPrimary(case)
    )


  def replicationSnapshotUsesPrimarySource: bool =
    accepted.forall(case =>
      case.surface == ReplicationSnapshot implies claimedSourceIsPrimary(case)
    )


  def serviceJournalWithConfiguredShardUsesPrimaryTransport: bool =
    accepted.forall(case =>
      case.surface == ServiceJournal
        and case.hasControlPlane
        and case.batchHasFileSystem
        and case.shardConfigured
        implies transportSourceIsPrimary(case)
    )


  def serviceSnapshotWithConfiguredShardUsesPrimaryTransport: bool =
    accepted.forall(case =>
      case.surface == ServiceSnapshot
        and case.hasControlPlane
        and case.shardConfigured
        implies transportSourceIsPrimary(case)
    )


  def configuredReplicaProcessorRequiresLocalReplica: bool =
    accepted.forall(case =>
      any {
        case.surface == ReplicationJournal and case.hasContext,
        case.surface == ReplicationSnapshot,
      } implies case.localConfiguredReplica
    )


  def primaryDoesNotApplyReplicaFeedWhenConfigKnown: bool =
    accepted.forall(case =>
      any {
        case.surface == ReplicationJournal and case.hasContext,
        case.surface == ReplicationSnapshot,
        case.surface == ServiceJournal
          and case.hasControlPlane
          and case.batchHasFileSystem
          and case.shardConfigured,
        case.surface == ServiceSnapshot and case.hasControlPlane and case.shardConfigured,
      } implies case.localNode != case.primary
    )


  def missingSourceRejectedWhenStandaloneContextRequiresIt: bool =
    accepted.forall(case =>
      not(
        any {
          case.surface == ReplicationJournal and case.hasContext,
          case.surface == ReplicationSnapshot,
          case.surface == ServiceSnapshot,
        }
          and case.claimedSource == None
      )
    )


  def acceptedAndRejectedAreDisjoint: bool =
    accepted.intersect(rejected).size() == 0


  def safety: bool = all {
    acceptedMatchesImplementationPredicate,
    rejectedMatchesImplementationPredicate,
    serviceTransportClaimMismatchesRejected,
    replicationJournalWithContextUsesPrimarySource,
    replicationSnapshotUsesPrimarySource,
    serviceJournalWithConfiguredShardUsesPrimaryTransport,
    serviceSnapshotWithConfiguredShardUsesPrimaryTransport,
    configuredReplicaProcessorRequiresLocalReplica,
    primaryDoesNotApplyReplicaFeedWhenConfigKnown,
    missingSourceRejectedWhenStandaloneContextRequiresIt,
    acceptedAndRejectedAreDisjoint,
  }
}
