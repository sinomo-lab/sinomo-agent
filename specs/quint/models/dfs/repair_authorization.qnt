/// DFS repair command authorization after metadata-primary movement.
///
/// This module models `ensure_replica_command_authorized_with_metadata`, the
/// authorization gate shared by repair requests regardless of body size. It focuses
/// on the branch ordering that matters after metadata primary handoff: durable
/// intent authority and locally known current-replica evidence must be checked
/// before rejecting a requester merely because it is not the current metadata
/// shard primary.
module dfs_repair_authorization {
  import common.* from "../../lib/common"

  /// Boolean abstraction of the implementation inputs:
  /// - whether local object metadata exists or arrived on the command,
  /// - whether metadata-shard state proves the requester is primary,
  /// - whether a durable write intent exists and names the local receiver,
  /// - whether locally known owner/current-replica fallback evidence exists.
  type AuthCase = {
    hasLocalObject: bool,
    suppliedObjectMetadata: bool,
    metadataShardKnown: bool,
    requesterIsShardPrimary: bool,
    authorityShardProvesPrimary: bool,
    hasStoredIntent: bool,
    intentCoordinatorIsRequester: bool,
    desiredReplicasContainLocal: bool,
    objectOwnerIsRequester: bool,
    objectPolicyIsReplicated: bool,
    currentLocationsContainRequester: bool,
  }

  type Decision = Authorized | Rejected

  pure val Bools: Set[bool] = Set(true, false)

  var lastCase: Option[AuthCase]
  var lastDecision: Decision

  pure def requesterIsMetadataPrimary(case: AuthCase): bool =
    (case.metadataShardKnown and case.requesterIsShardPrimary)
      or case.authorityShardProvesPrimary

  pure def objectAvailable(case: AuthCase): bool =
    case.hasLocalObject or case.suppliedObjectMetadata

  pure def objectFromCommand(case: AuthCase): bool =
    not(case.hasLocalObject) and case.suppliedObjectMetadata

  pure def storedIntentAuthorizes(case: AuthCase): bool =
    all {
      case.hasStoredIntent,
      case.desiredReplicasContainLocal,
      any {
        case.intentCoordinatorIsRequester,
        requesterIsMetadataPrimary(case) and case.suppliedObjectMetadata,
      },
    }

  pure def metadataPrimaryCommandAuthorizes(case: AuthCase): bool =
    all {
      not(case.hasStoredIntent),
      requesterIsMetadataPrimary(case),
      case.suppliedObjectMetadata,
    }

  pure def localOwnerOrReplicaFallbackAuthorizes(case: AuthCase): bool =
    all {
      not(case.hasStoredIntent),
      any {
        not(objectFromCommand(case)),
        not(case.metadataShardKnown),
      },
      any {
        case.objectOwnerIsRequester,
        case.objectPolicyIsReplicated and case.currentLocationsContainRequester,
      },
    }

  pure def repairCommandAuthorized(case: AuthCase): bool =
    all {
      objectAvailable(case),
      any {
        storedIntentAuthorizes(case),
        metadataPrimaryCommandAuthorizes(case),
        localOwnerOrReplicaFallbackAuthorizes(case),
      },
    }

  pure def unwrapCase(value: Option[AuthCase]): AuthCase =
    match value {
      | Some(inner) => inner
      | None => {
          hasLocalObject: false,
          suppliedObjectMetadata: false,
          metadataShardKnown: false,
          requesterIsShardPrimary: false,
          authorityShardProvesPrimary: false,
          hasStoredIntent: false,
          intentCoordinatorIsRequester: false,
          desiredReplicasContainLocal: false,
          objectOwnerIsRequester: false,
          objectPolicyIsReplicated: false,
          currentLocationsContainRequester: false,
        }
    }

  action init = all {
    lastCase' = None,
    lastDecision' = Rejected,
  }

  action observeAuthorized(case: AuthCase): bool = all {
    repairCommandAuthorized(case),
    lastCase' = Some(case),
    lastDecision' = Authorized,
  }

  action observeRejected(case: AuthCase): bool = all {
    not(repairCommandAuthorized(case)),
    lastCase' = Some(case),
    lastDecision' = Rejected,
  }

  action step = {
    nondet hasLocalObject = oneOf(Bools)
    nondet suppliedObjectMetadata = oneOf(Bools)
    nondet metadataShardKnown = oneOf(Bools)
    nondet requesterIsShardPrimary = oneOf(Bools)
    nondet authorityShardProvesPrimary = oneOf(Bools)
    nondet hasStoredIntent = oneOf(Bools)
    nondet intentCoordinatorIsRequester = oneOf(Bools)
    nondet desiredReplicasContainLocal = oneOf(Bools)
    nondet objectOwnerIsRequester = oneOf(Bools)
    nondet objectPolicyIsReplicated = oneOf(Bools)
    nondet currentLocationsContainRequester = oneOf(Bools)
    val case = {
      hasLocalObject: hasLocalObject,
      suppliedObjectMetadata: suppliedObjectMetadata,
      metadataShardKnown: metadataShardKnown,
      requesterIsShardPrimary: requesterIsShardPrimary,
      authorityShardProvesPrimary: authorityShardProvesPrimary,
      hasStoredIntent: hasStoredIntent,
      intentCoordinatorIsRequester: intentCoordinatorIsRequester,
      desiredReplicasContainLocal: desiredReplicasContainLocal,
      objectOwnerIsRequester: objectOwnerIsRequester,
      objectPolicyIsReplicated: objectPolicyIsReplicated,
      currentLocationsContainRequester: currentLocationsContainRequester,
    }
    any {
      observeAuthorized(case),
      observeRejected(case),
    }
  }
}
