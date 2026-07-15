/// Safety checks and counterexample/fix witnesses for mkdir transaction ids.
module dfs_namespace_create_transaction_identity_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_create_transaction_identity.* from "../../models/dfs/namespace_create_transaction_identity"

  def generationsKeepStableObjectAndPathButAdvanceVersion: bool = {
    val original = payloadFor(OriginalGeneration)
    val resurrected = payloadFor(ResurrectedGeneration)
    all {
      original.object == resurrected.object,
      original.path == resurrected.path,
      original.version < resurrected.version,
      original != resurrected,
    }
  }


  def legacyIdentityCollidesAcrossDifferentPayloads: bool = all {
    transactionIdFor(LegacyPathIdentity, OriginalGeneration) ==
      transactionIdFor(LegacyPathIdentity, ResurrectedGeneration),
    payloadFor(OriginalGeneration) != payloadFor(ResurrectedGeneration),
  }


  def versionBoundIdentitySeparatesGenerations: bool = all {
    transactionIdFor(VersionBoundIdentity, OriginalGeneration) !=
      transactionIdFor(VersionBoundIdentity, ResurrectedGeneration),
    transactionIdFor(VersionBoundIdentity, ResurrectedGeneration) ==
      transactionIdFor(VersionBoundIdentity, ResurrectedGeneration),
  }


  def prepareAuditMatchesRetainedPayloadRule: bool =
    match lastPrepare {
      | Some(audit) => all {
          audit.accepted == prepareAccepts(
            audit.retainedPayloadBefore,
            audit.proposedPayload,
          ),
          audit.replayed ==
            (audit.retainedPayloadBefore == Some(audit.proposedPayload)),
          if (audit.accepted and not(audit.replayed)) {
            audit.mutationCountAfter == audit.mutationCountBefore + 1
          } else {
            audit.mutationCountAfter == audit.mutationCountBefore
          },
        }
      | None => true
    }


  def retainedOriginalCreateSurvivesMove: bool =
    any {
      phase == MoveLeftSelectedTombstone,
      phase == LegacyRecreateRejected,
      phase == VersionBoundRecreateApplied,
      phase == VersionBoundExactRetryReplayed,
    } implies all {
      retainedTransactions.get(
        transactionIdFor(identityScheme, OriginalGeneration)
      ) == Some(payloadFor(OriginalGeneration)),
      committedMutations >= 2,
    }


  def movedPathRemainsSelectedTombstoneUntilAcceptedRecreate: bool =
    any {
      phase == MoveLeftSelectedTombstone,
      phase == LegacyRecreateRejected,
    } implies pathState == SelectedTombstone


  def legacyCollisionRejectsWithoutMutation: bool =
    phase == LegacyRecreateRejected implies {
      val audit = unwrapOr(lastPrepare, {
        proposedId: LegacyMkdirCreateId,
        proposedPayload: payloadFor(ResurrectedGeneration),
        retainedPayloadBefore: None,
        accepted: true,
        replayed: false,
        mutationCountBefore: 0,
        mutationCountAfter: 1,
      })
      all {
        identityScheme == LegacyPathIdentity,
        audit.proposedId == transactionIdFor(
          LegacyPathIdentity,
          OriginalGeneration,
        ),
        audit.proposedId == transactionIdFor(
          LegacyPathIdentity,
          ResurrectedGeneration,
        ),
        audit.retainedPayloadBefore == Some(payloadFor(OriginalGeneration)),
        audit.proposedPayload == payloadFor(ResurrectedGeneration),
        not(audit.accepted),
        not(audit.replayed),
        audit.mutationCountAfter == audit.mutationCountBefore,
        committedMutations == 2,
        pathState == SelectedTombstone,
      }
    }


  def versionBoundRecreateUsesFreshRetainedIdentity: bool =
    any {
      phase == VersionBoundRecreateApplied,
      phase == VersionBoundExactRetryReplayed,
    } implies all {
      identityScheme == VersionBoundIdentity,
      retainedTransactions.get(
        transactionIdFor(VersionBoundIdentity, OriginalGeneration)
      ) == Some(payloadFor(OriginalGeneration)),
      retainedTransactions.get(
        transactionIdFor(VersionBoundIdentity, ResurrectedGeneration)
      ) == Some(payloadFor(ResurrectedGeneration)),
      pathState == PathLive,
      committedMutations == 3,
    }


  def exactRetryKeepsIdentityAndAddsNoMutation: bool =
    phase == VersionBoundExactRetryReplayed implies {
      val audit = unwrapOr(lastPrepare, {
        proposedId: LegacyMkdirCreateId,
        proposedPayload: payloadFor(OriginalGeneration),
        retainedPayloadBefore: None,
        accepted: false,
        replayed: false,
        mutationCountBefore: 0,
        mutationCountAfter: 1,
      })
      all {
        audit.proposedId == transactionIdFor(
          VersionBoundIdentity,
          ResurrectedGeneration,
        ),
        audit.retainedPayloadBefore == Some(audit.proposedPayload),
        audit.accepted,
        audit.replayed,
        audit.mutationCountAfter == audit.mutationCountBefore,
        committedMutations == 3,
      }
    }


  def witnessSelectedTombstoneRetainsOriginalMkdir: bool = all {
    phase == MoveLeftSelectedTombstone,
    pathState == SelectedTombstone,
    retainedOriginalCreateSurvivesMove,
  }


  def witnessLegacyRecreateRejectedByRetainedPayloadMismatch: bool = all {
    phase == LegacyRecreateRejected,
    legacyIdentityCollidesAcrossDifferentPayloads,
    legacyCollisionRejectsWithoutMutation,
  }


  def witnessVersionBoundRecreateAcceptedWithDistinctIdentity: bool = all {
    phase == VersionBoundRecreateApplied,
    versionBoundIdentitySeparatesGenerations,
    versionBoundRecreateUsesFreshRetainedIdentity,
  }


  def witnessVersionBoundExactRetryReplaysWithoutMutation: bool = all {
    phase == VersionBoundExactRetryReplayed,
    exactRetryKeepsIdentityAndAddsNoMutation,
  }


  def safety: bool = all {
    generationsKeepStableObjectAndPathButAdvanceVersion,
    legacyIdentityCollidesAcrossDifferentPayloads,
    versionBoundIdentitySeparatesGenerations,
    prepareAuditMatchesRetainedPayloadRule,
    retainedOriginalCreateSurvivesMove,
    movedPathRemainsSelectedTombstoneUntilAcceptedRecreate,
    legacyCollisionRejectsWithoutMutation,
    versionBoundRecreateUsesFreshRetainedIdentity,
    exactRetryKeepsIdentityAndAddsNoMutation,
  }
}
