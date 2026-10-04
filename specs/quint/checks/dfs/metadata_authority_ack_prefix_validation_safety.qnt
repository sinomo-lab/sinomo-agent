/// Safety and bounded repair reachability for exact-prefix authority ACKs.
module dfs_metadata_authority_ack_prefix_validation_safety {
  import dfs_metadata_authority_ack_prefix_validation.* from "../../models/dfs/metadata_authority_ack_prefix_validation"

  def operationApplicationReachesPreparedSequence: bool =
    ackPrefix.namedOperationAppliedAndVerified implies
      ackPrefix.receiverSequence == PrimaryPreparedSequence

  /// Sequence and named-operation equality do not substitute for exact prefix
  /// equality: the divergent predecessor reaches the same sequence but not the
  /// primary's prepared digest.
  def divergentHistoryRemainsObservableAtPreparedSequence: bool =
    ackPrefix.receiverPrefix == PreparedPrefixDivergent implies all {
      ackPrefix.priorPrefix == DivergentPredecessor,
      ackPrefix.receiverSequence == PrimaryPreparedSequence,
      ackPrefix.namedOperationAppliedAndVerified,
      not(resultingPrefixEqualsPrimaryPrepared(ackPrefix)),
    }

  def authorityFloorRequiresExactResultingPrefix: bool =
    ackPrefix.authorityFloorPersisted implies all {
      ackPrefix.authorityAckAccepted,
      resultingPrefixEqualsPrimaryPrepared(ackPrefix),
      ackPrefix.authorityFloorSequence == PrimaryPreparedSequence,
    }

  def acceptedAuthorityAckRequiresExactResultingPrefix: bool =
    ackPrefix.authorityAckAccepted implies all {
      ackPrefix.namedOperationAppliedAndVerified,
      resultingPrefixEqualsPrimaryPrepared(ackPrefix),
      ackPrefix.authorityFloorPersisted,
    }

  def divergentAuthorityAckRejectsWithoutFloor: bool =
    ackPrefix.lastTransition == DivergentAuthorityAckRejected implies all {
      ackPrefix.divergentAckRejected,
      ackPrefix.receiverPrefix == PreparedPrefixDivergent,
      not(ackPrefix.authorityAckAccepted),
      not(ackPrefix.authorityFloorPersisted),
      ackPrefix.authorityFloorSequence == 0,
      authoritativeSnapshotRepairAllowed(ackPrefix),
      not(snapshotRepairAllowedWith(true, false)),
    }

  def authoritativeRepairConsumesOnlyUnfencedDivergence: bool =
    ackPrefix.lastTransition == AuthoritativeSnapshotInstalled implies all {
      ackPrefix.divergentAckRejected,
      ackPrefix.authoritativeSnapshotInstalled,
      not(ackPrefix.authorityAckAccepted),
      not(ackPrefix.authorityFloorPersisted),
      ackPrefix.authorityFloorSequence == 0,
      resultingPrefixEqualsPrimaryPrepared(ackPrefix),
    }

  def repairedAckPersistsOnlyTheExactSnapshotPrefix: bool =
    ackPrefix.lastTransition == RepairedAuthorityAckAdmitted implies all {
      ackPrefix.divergentAckRejected,
      ackPrefix.authoritativeSnapshotInstalled,
      ackPrefix.authorityAckAccepted,
      ackPrefix.authorityFloorPersisted,
      resultingPrefixEqualsPrimaryPrepared(ackPrefix),
    }

  def witnessHealthyExactPrefixAuthorityAck: bool = all {
    ackPrefix.lastTransition == HealthyAuthorityAckAdmitted,
    ackPrefix.priorPrefix == ExactPredecessor,
    ackPrefix.namedOperationAppliedAndVerified,
    resultingPrefixEqualsPrimaryPrepared(ackPrefix),
    ackPrefix.authorityAckAccepted,
    ackPrefix.authorityFloorPersisted,
  }

  def witnessDivergentNamedOperationRejectsWithoutFloor: bool = all {
    ackPrefix.lastTransition == DivergentAuthorityAckRejected,
    ackPrefix.priorPrefix == DivergentPredecessor,
    ackPrefix.namedOperationAppliedAndVerified,
    ackPrefix.receiverSequence == PrimaryPreparedSequence,
    ackPrefix.receiverPrefix == PreparedPrefixDivergent,
    divergentAuthorityAckRejectsWithoutFloor,
  }

  def witnessRejectedDivergenceAllowsAuthoritativeSnapshotRepair: bool = all {
    ackPrefix.lastTransition == AuthoritativeSnapshotInstalled,
    ackPrefix.priorPrefix == DivergentPredecessor,
    ackPrefix.divergentAckRejected,
    ackPrefix.authoritativeSnapshotInstalled,
    authoritativeRepairConsumesOnlyUnfencedDivergence,
  }

  def witnessRepairedExactPrefixCanAcknowledge: bool = all {
    ackPrefix.lastTransition == RepairedAuthorityAckAdmitted,
    ackPrefix.priorPrefix == DivergentPredecessor,
    repairedAckPersistsOnlyTheExactSnapshotPrefix,
  }

  def staleCatchupRecoveryPreservesAcknowledgedPrefix: bool =
    ackPrefix.staleCatchupRejected implies all {
      ackPrefix.authorityFloorPersisted,
      ackPrefix.authorityFloorSequence == PrimaryPreparedSequence,
      resultingPrefixEqualsPrimaryPrepared(ackPrefix),
    }

  def witnessStaleCatchupRequestsSnapshot: bool =
    ackPrefix.lastTransition == StaleCatchupRejected
      and staleCatchupRecoveryPreservesAcknowledgedPrefix

  def witnessStaleCatchupSnapshotPreservesFloor: bool =
    ackPrefix.lastTransition == StaleCatchupSnapshotRecovered
      and ackPrefix.staleCatchupSnapshotRecovered
      and staleCatchupRecoveryPreservesAcknowledgedPrefix

  def safety: bool = all {
    operationApplicationReachesPreparedSequence,
    divergentHistoryRemainsObservableAtPreparedSequence,
    authorityFloorRequiresExactResultingPrefix,
    acceptedAuthorityAckRequiresExactResultingPrefix,
    divergentAuthorityAckRejectsWithoutFloor,
    authoritativeRepairConsumesOnlyUnfencedDivergence,
    repairedAckPersistsOnlyTheExactSnapshotPrefix,
    staleCatchupRecoveryPreservesAcknowledgedPrefix,
  }
}
