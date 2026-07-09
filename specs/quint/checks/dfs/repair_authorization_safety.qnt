/// Invariant checks for `dfs_repair_authorization`.
module dfs_repair_authorization_safety {
  import common.* from "../../lib/common"
  import dfs_repair_authorization.* from "../../models/dfs/repair_authorization"

  def observedDecisionMatchesPredicate: bool =
    match lastCase {
      | Some(case) =>
          if (lastDecision == Authorized) {
            repairCommandAuthorized(case)
          } else {
            not(repairCommandAuthorized(case))
          }
      | None => true
    }

  /// The regression case: after primary movement, a locally known replicated
  /// object can still accept repair from a current replica even when the
  /// requester is not the metadata shard primary.

  def currentReplicaAfterPrimaryMoveCanRepair: bool =
    match lastCase {
      | Some(case) =>
          all {
            case.hasLocalObject,
            case.suppliedObjectMetadata,
            case.metadataShardKnown,
            not(requesterIsMetadataPrimary(case)),
            not(case.hasStoredIntent),
            not(case.objectOwnerIsRequester),
            case.objectPolicyIsReplicated,
            case.currentLocationsContainRequester,
          } implies lastDecision == Authorized
      | None => true
    }

  /// The paired regression: an unknown object supplied by a non-primary source
  /// must not become authorized merely because it carried object metadata.

  def unknownObjectFromNonPrimaryMetadataSourceRejected: bool =
    match lastCase {
      | Some(case) =>
          all {
            not(case.hasLocalObject),
            case.suppliedObjectMetadata,
            case.metadataShardKnown,
            not(requesterIsMetadataPrimary(case)),
            not(case.hasStoredIntent),
          } implies lastDecision == Rejected
      | None => true
    }


  def storedIntentAuthorizationRequiresLocalDesiredReplica: bool =
    match lastCase {
      | Some(case) =>
          case.hasStoredIntent and lastDecision == Authorized
            implies case.desiredReplicasContainLocal
      | None => true
    }


  def storedIntentAuthorizationRequiresCoordinatorOrPrimary: bool =
    match lastCase {
      | Some(case) =>
          case.hasStoredIntent and lastDecision == Authorized
            implies any {
              case.intentCoordinatorIsRequester,
              requesterIsMetadataPrimary(case) and case.suppliedObjectMetadata,
            }
      | None => true
    }


  def metadataPrimaryCanAuthorizeCommandObjectWithoutStoredIntent: bool =
    match lastCase {
      | Some(case) =>
          all {
            not(case.hasLocalObject),
            case.suppliedObjectMetadata,
            case.metadataShardKnown,
            requesterIsMetadataPrimary(case),
            not(case.hasStoredIntent),
          } implies lastDecision == Authorized
      | None => true
    }


  def localOwnerFallbackCanAuthorizeKnownObject: bool =
    match lastCase {
      | Some(case) =>
          all {
            case.hasLocalObject,
            not(case.hasStoredIntent),
            case.objectOwnerIsRequester,
          } implies lastDecision == Authorized
      | None => true
    }


  def authorityShardProofCanAuthorizeStaleReceiver: bool =
    match lastCase {
      | Some(case) =>
          all {
            case.suppliedObjectMetadata,
            not(case.hasStoredIntent),
            case.authorityShardProvesPrimary,
          } implies lastDecision == Authorized
      | None => true
    }


  def missingObjectAndMetadataRejected: bool =
    match lastCase {
      | Some(case) =>
          not(case.hasLocalObject) and not(case.suppliedObjectMetadata)
            implies lastDecision == Rejected
      | None => true
    }


  def safety: bool = all {
    observedDecisionMatchesPredicate,
    currentReplicaAfterPrimaryMoveCanRepair,
    unknownObjectFromNonPrimaryMetadataSourceRejected,
    storedIntentAuthorizationRequiresLocalDesiredReplica,
    storedIntentAuthorizationRequiresCoordinatorOrPrimary,
    metadataPrimaryCanAuthorizeCommandObjectWithoutStoredIntent,
    localOwnerFallbackCanAuthorizeKnownObject,
    authorityShardProofCanAuthorizeStaleReceiver,
    missingObjectAndMetadataRejected,
  }
}
