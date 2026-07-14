/// Ordinary canonical DFS write eligibility across partitions and shard moves.
///
/// Evidence class: current-behavior refinement. DFS metadata remains in the
/// DFS metadata journal; cluster KV contributes only the committed shard
/// assignment/epoch and, on the partition fallback path, an operation-bound
/// quorum authority permit. The ordinary path does not run a KV quorum round:
/// it gathers fresh acknowledgements from every replica in the exact current
/// metadata-shard assignment. A metadata acknowledgement abstracts the
/// production receiver durably applying and verifying the operation-bound
/// hidden DFS prepare; it is not a reachability-only response.
///
/// The split modeled here is intentionally asymmetric. N1 is the old primary.
/// Once N1 is isolated, the N2/N3 voter quorum may commit epoch 2 and either
/// promote N2 or keep N1 as primary while changing the replica assignment.
/// Committing those KV configuration facts does not immediately activate the
/// new DFS authority. The retiring shard can contain every old replica and can
/// still publish without KV, so activation must carry and fence its prepared
/// operations first. Only then may the new assignment authorize publications.
/// An operation begun under the retiring assignment remains in the state
/// alongside an operation begun under the activated assignment, so both
/// publication attempts and conflict rejection are observable.
///
/// A visible write requires W durable content receipts, current
/// primary/epoch/base authority, and one of these disjoint authority paths:
/// - fresh acknowledgements for this operation from every configured metadata
///   replica; or
/// - a fresh operation-bound permit from a quorum of the committed cluster
///   voter configuration when the complete metadata shard is not reachable.
///
/// This model deliberately excludes local-first work, POSIX fsync, and locks.
module dfs_canonical_write_eligibility {
  import common.* from "../../lib/common"

  type CommitAuthorization = FullShardAcknowledgements | ClusterQuorumPermit
  type IntentPreparation =
    | UnpreparedIntent
    | FullShardPreparedIntent
    | QuorumPreparedIntent
  type NetworkPartition = Connected | OldPrimaryIsolated | ThirdReplicaIsolated
  type ReconfigurationKind =
    | NoReconfiguration
    | PrimaryPromotion
    | SamePrimaryMembershipChange
  type WriteDecision =
    | Initialized
    | NetworkPartitionChanged
    | IntentStarted
    | ContentReceiptRecorded
    | MetadataAcknowledgementsRecorded
    | QuorumPermitIssued
    | PublicationRejected
    | StaleAuthorityRejected
    | VisibleVersionPublished
    | ControlPlaneReconfigured
    | RetiringPrimaryFenceAcknowledged
    | AuthorityHandoffCompleted
    | DirectPrimaryChangeRejected
    | ReplicaPromotedWithTransferProof
    | TerminalStutter

  type WriteIntent = {
    operation: int,
    coordinator: Node,
    epoch: int,
    assignmentRevision: int,
    clusterConfigRevision: int,
    baseVersion: int,
    targetVersion: int,
  }

  type MetadataAcknowledgement = {
    operation: int,
    coordinator: Node,
    epoch: int,
    assignmentRevision: int,
    baseVersion: int,
    targetVersion: int,
  }

  type AuthorityPermit = {
    operation: int,
    coordinator: Node,
    epoch: int,
    assignmentRevision: int,
    clusterConfigRevision: int,
    baseVersion: int,
    targetVersion: int,
    voters: Set[Node],
  }

  type CommitRecord = {
    operation: int,
    coordinator: Node,
    epoch: int,
    assignmentRevision: int,
    clusterConfigRevision: int,
    baseVersion: int,
    targetVersion: int,
    contentReceipts: Set[Node],
    authorization: CommitAuthorization,
    metadataAssignment: Set[Node],
    metadataAcknowledgers: Set[Node],
    clusterVoters: Set[Node],
    permitVoters: Set[Node],
  }

  pure val MaxVersion: int = 2
  pure val Versions: Set[int] = Set(1, 2)
  pure val MaxOperation: int = 3
  pure val Operations: Set[int] = Set(1, 2, 3)
  pure val MaxEpoch: int = 3
  pure val MaxRevision: int = 3
  /// The production FUSE benchmark uses replicated-eventual W=1. Keeping W
  /// below cluster quorum is what exposes the unsafe minority-side case.
  pure val MinContentReceipts: int = 1
  pure val DesiredContentReplicas: Set[Node] = Nodes

  var networkPartition: NetworkPartition
  var currentPrimary: Node
  var currentEpoch: int
  var assignmentRevision: int
  var clusterConfigRevision: int
  var metadataReplicas: Set[Node]
  var clusterVoters: Set[Node]
  /// Committed control-plane facts may lead the active DFS authority during a
  /// shard handoff. Per-write checks use these authority fields, not bare KV.
  var authorityPrimary: Node
  var authorityEpoch: int
  var authorityAssignmentRevision: int
  var authorityClusterConfigRevision: int
  var authorityMetadataReplicas: Set[Node]
  var authorityHandoffPending: bool
  /// A committed pending handoff may be invisible to an isolated retiring
  /// primary. This becomes true only when that primary observes the pending
  /// record under its publication guard and captures its exact durable head.
  var retiringPrimaryFenceAcknowledged: bool
  var retiringFenceOperationFloor: int
  var reconfigurationKind: ReconfigurationKind
  var retiringPreparedOperations: Set[int]
  var carriedRetiringOperations: Set[int]
  var requiredHandoffOperationFloor: int
  var exactStateWatermarkByReplica: Node -> int
  var directPrimaryChangeRejected: bool
  var primaryByEpoch: int -> Option[Node]
  var assignmentByRevision: int -> Set[Node]
  var votersByRevision: int -> Set[Node]
  var nextOperation: int
  var visibleVersion: int
  var intentByOperation: int -> Option[WriteIntent]
  var contentReceiptsByOperation: int -> Set[Node]
  var metadataAcksByOperation: int -> (Node -> Option[MetadataAcknowledgement])
  var permitByOperation: int -> Option[AuthorityPermit]
  /// Publications are indexed by operation rather than only by version. This
  /// permits the safety checker to detect two distinct operations publishing
  /// the same base/target pair instead of making conflicts unrepresentable.
  var publicationByOperation: int -> Option[CommitRecord]
  var publishedOperationByVersion: int -> Option[int]
  var publicationAttempts: Set[int]
  var rejectedWithoutAuthority: Set[int]
  var staleRejectedOperations: Set[int]
  var conflictRejectedOperations: Set[int]
  var lastOperation: Option[int]
  var lastDecision: WriteDecision

  pure val emptyPrimaryByEpoch: int -> Option[Node] =
    Set(1, 2, 3).mapBy(_ => None)

  pure val emptyAssignmentByRevision: int -> Set[Node] =
    Set(1, 2, 3).mapBy(_ => Set())

  pure val emptyVotersByRevision: int -> Set[Node] =
    Set(1, 2, 3).mapBy(_ => Set())

  pure val emptyExactStateWatermarks: Node -> int =
    Nodes.mapBy(_ => 0)

  pure val emptyIntents: int -> Option[WriteIntent] =
    Operations.mapBy(_ => None)

  pure val emptyContentReceiptsByOperation: int -> Set[Node] =
    Operations.mapBy(_ => Set())

  pure val emptyMetadataAcknowledgements: Node -> Option[MetadataAcknowledgement] =
    Nodes.mapBy(_ => None)

  pure val emptyMetadataAcksByOperation:
    int -> (Node -> Option[MetadataAcknowledgement]) =
      Operations.mapBy(_ => emptyMetadataAcknowledgements)

  pure val emptyPermits: int -> Option[AuthorityPermit] =
    Operations.mapBy(_ => None)

  pure val emptyPublications: int -> Option[CommitRecord] =
    Operations.mapBy(_ => None)

  pure val emptyPublishedVersions: int -> Option[int] =
    Versions.mapBy(_ => None)

  pure def unwrapIntent(value: Option[WriteIntent]): WriteIntent =
    match value {
      | Some(inner) => inner
      | None => {
          operation: 0,
          coordinator: N1,
          epoch: 0,
          assignmentRevision: 0,
          clusterConfigRevision: 0,
          baseVersion: 0,
          targetVersion: 0,
        }
    }

  pure def unwrapPermit(value: Option[AuthorityPermit]): AuthorityPermit =
    match value {
      | Some(inner) => inner
      | None => {
          operation: 0,
          coordinator: N1,
          epoch: 0,
          assignmentRevision: 0,
          clusterConfigRevision: 0,
          baseVersion: 0,
          targetVersion: 0,
          voters: Set(),
        }
    }

  pure def reachableFrom(node: Node): Set[Node] =
    match networkPartition {
      | Connected => Nodes
      | OldPrimaryIsolated =>
          if (node == N1) { Set(N1) } else { Set(N2, N3) }
      | ThirdReplicaIsolated =>
          if (node == N3) { Set(N3) } else { Set(N1, N2) }
    }

  pure def metadataAckMatches(
    acknowledgement: MetadataAcknowledgement,
    pending: WriteIntent
  ): bool = all {
    acknowledgement.operation == pending.operation,
    acknowledgement.coordinator == pending.coordinator,
    acknowledgement.epoch == pending.epoch,
    acknowledgement.assignmentRevision == pending.assignmentRevision,
    acknowledgement.baseVersion == pending.baseVersion,
    acknowledgement.targetVersion == pending.targetVersion,
  }

  pure def permitMatches(
    permit: AuthorityPermit,
    pending: WriteIntent,
    voters: Set[Node]
  ): bool = all {
    permit.operation == pending.operation,
    permit.coordinator == pending.coordinator,
    permit.epoch == pending.epoch,
    permit.assignmentRevision == pending.assignmentRevision,
    permit.clusterConfigRevision == pending.clusterConfigRevision,
    permit.baseVersion == pending.baseVersion,
    permit.targetVersion == pending.targetVersion,
    permit.voters.subseteq(voters),
    permit.voters.size() >= quorumSize(voters),
  }

  def intent(operation: int): WriteIntent =
    unwrapIntent(intentByOperation.get(operation))

  def intentHasCurrentAuthority(operation: int): bool = {
    val pending = intent(operation)
    all {
      isSome(intentByOperation.get(operation)),
      pending.coordinator == authorityPrimary,
      pending.epoch == authorityEpoch,
      pending.assignmentRevision == authorityAssignmentRevision,
      pending.clusterConfigRevision == authorityClusterConfigRevision,
      pending.baseVersion == visibleVersion,
      pending.targetVersion == visibleVersion + 1,
      pending.targetVersion <= MaxVersion,
      not(authorityHandoffPending and retiringPrimaryFenceAcknowledged),
    }
  }

  def contentThresholdMet(operation: int): bool =
    contentReceiptsByOperation.get(operation).size() >= MinContentReceipts

  def freshMetadataAcknowledgers(operation: int): Set[Node] = {
    val pending = intent(operation)
    val acknowledgements = metadataAcksByOperation.get(operation)
    authorityMetadataReplicas.filter(replica =>
      match acknowledgements.get(replica) {
        | Some(acknowledgement) => metadataAckMatches(acknowledgement, pending)
        | None => false
      }
    )
  }

  def historicalMetadataAcknowledgers(operation: int): Set[Node] = {
    val pending = intent(operation)
    val historicalAssignment =
      assignmentByRevision.get(pending.assignmentRevision)
    val acknowledgements = metadataAcksByOperation.get(operation)
    historicalAssignment.filter(replica =>
      match acknowledgements.get(replica) {
        | Some(acknowledgement) => metadataAckMatches(acknowledgement, pending)
        | None => false
      }
    )
  }

  def fullFreshShardAcknowledgements(operation: int): bool = all {
    intentHasCurrentAuthority(operation),
    freshMetadataAcknowledgers(operation) == authorityMetadataReplicas,
  }

  def fullHistoricalShardAcknowledgements(operation: int): bool = {
    val pending = intent(operation)
    val historicalAssignment =
      assignmentByRevision.get(pending.assignmentRevision)
    all {
      isSome(intentByOperation.get(operation)),
      historicalAssignment.size() > 0,
      historicalMetadataAcknowledgers(operation) == historicalAssignment,
    }
  }

  def matchingQuorumPermit(operation: int): bool = {
    val pending = intent(operation)
    match permitByOperation.get(operation) {
      | Some(permit) => all {
          intentHasCurrentAuthority(operation),
          permitMatches(permit, pending, clusterVoters),
        }
      | None => false
    }
  }

  def matchingHistoricalQuorumPermit(operation: int): bool = {
    val pending = intent(operation)
    val historicalVoters =
      votersByRevision.get(pending.clusterConfigRevision)
    match permitByOperation.get(operation) {
      | Some(permit) => all {
          isSome(intentByOperation.get(operation)),
          historicalVoters.size() > 0,
          permitMatches(permit, pending, historicalVoters),
        }
      | None => false
    }
  }

  def hasHistoricalAuthorityEvidence(operation: int): bool = any {
    fullHistoricalShardAcknowledgements(operation),
    matchingHistoricalQuorumPermit(operation),
  }

  def allConfiguredMetadataReplicasReachable(operation: int): bool =
    authorityMetadataReplicas.subseteq(
      reachableFrom(intent(operation).coordinator)
    )

  def hasStaleMetadataAcknowledgement(operation: int): bool = {
    val pending = intent(operation)
    val acknowledgements = metadataAcksByOperation.get(operation)
    Nodes.exists(replica =>
      match acknowledgements.get(replica) {
        | Some(acknowledgement) => not(metadataAckMatches(acknowledgement, pending))
        | None => false
      }
    ) or all {
      fullHistoricalShardAcknowledgements(operation),
      not(intentHasCurrentAuthority(operation)),
    }
  }

  def publishedOperationsForVersion(version: int): Set[int] =
    Operations.filter(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => record.targetVersion == version
        | None => false
      }
    )

  def canonicalConflictExists: bool =
    Versions.exists(version => publishedOperationsForVersion(version).size() > 1)

  def fullyPreparedOperations: Set[int] =
    Operations.filter(operation => all {
      isSome(intentByOperation.get(operation)),
      contentThresholdMet(operation),
      fullHistoricalShardAcknowledgements(operation),
    })

  def publicationsUnderActiveAuthority: Set[int] =
    Operations.filter(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.epoch == authorityEpoch,
            record.assignmentRevision == authorityAssignmentRevision,
          }
        | None => false
      }
    )

  /// Every hidden prepare belongs to the retiring state whether or not its
  /// content receipt threshold has arrived yet. The backend freeze carries
  /// metadata state, not only operations already eligible to publish.
  def retiringShardOperations: Set[int] =
    Operations.filter(operation => all {
      isSome(intentByOperation.get(operation)),
      fullHistoricalShardAcknowledgements(operation),
    }).union(publicationsUnderActiveAuthority)

  def currentQuorumPermittedOperations: Set[int] =
    Operations.filter(operation => all {
      isSome(intentByOperation.get(operation)),
      matchingQuorumPermit(operation),
    })

  action init = all {
    networkPartition' = Connected,
    currentPrimary' = N1,
    currentEpoch' = 1,
    assignmentRevision' = 1,
    clusterConfigRevision' = 1,
    metadataReplicas' = Set(N1),
    clusterVoters' = Nodes,
    authorityPrimary' = N1,
    authorityEpoch' = 1,
    authorityAssignmentRevision' = 1,
    authorityClusterConfigRevision' = 1,
    authorityMetadataReplicas' = Set(N1),
    authorityHandoffPending' = false,
    retiringPrimaryFenceAcknowledged' = false,
    retiringFenceOperationFloor' = 0,
    reconfigurationKind' = NoReconfiguration,
    retiringPreparedOperations' = Set(),
    carriedRetiringOperations' = Set(),
    requiredHandoffOperationFloor' = 0,
    exactStateWatermarkByReplica' = emptyExactStateWatermarks,
    directPrimaryChangeRejected' = false,
    primaryByEpoch' = emptyPrimaryByEpoch.set(1, Some(N1)),
    assignmentByRevision' = emptyAssignmentByRevision.set(1, Set(N1)),
    votersByRevision' = emptyVotersByRevision.set(1, Nodes),
    nextOperation' = 1,
    visibleVersion' = 0,
    intentByOperation' = emptyIntents,
    contentReceiptsByOperation' = emptyContentReceiptsByOperation,
    metadataAcksByOperation' = emptyMetadataAcksByOperation,
    permitByOperation' = emptyPermits,
    publicationByOperation' = emptyPublications,
    publishedOperationByVersion' = emptyPublishedVersions,
    publicationAttempts' = Set(),
    rejectedWithoutAuthority' = Set(),
    staleRejectedOperations' = Set(),
    conflictRejectedOperations' = Set(),
    lastOperation' = None,
    lastDecision' = Initialized,
  }

  /// Change the communication graph without deleting any already durable DFS
  /// prepare or authority evidence. Old evidence must be checked against the
  /// control-plane facts current at publication time.
  action changeNetworkPartition(nextPartition: NetworkPartition): bool = all {
    nextPartition != networkPartition,
    networkPartition' = nextPartition,
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    assignmentRevision' = assignmentRevision,
    clusterConfigRevision' = clusterConfigRevision,
    metadataReplicas' = metadataReplicas,
    clusterVoters' = clusterVoters,
    authorityPrimary' = authorityPrimary,
    authorityEpoch' = authorityEpoch,
    authorityAssignmentRevision' = authorityAssignmentRevision,
    authorityClusterConfigRevision' = authorityClusterConfigRevision,
    authorityMetadataReplicas' = authorityMetadataReplicas,
    authorityHandoffPending' = authorityHandoffPending,
    retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
    retiringFenceOperationFloor' = retiringFenceOperationFloor,
    reconfigurationKind' = reconfigurationKind,
    retiringPreparedOperations' = retiringPreparedOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    directPrimaryChangeRejected' = directPrimaryChangeRejected,
    primaryByEpoch' = primaryByEpoch,
    assignmentByRevision' = assignmentByRevision,
    votersByRevision' = votersByRevision,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    contentReceiptsByOperation' = contentReceiptsByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    publicationAttempts' = publicationAttempts,
    rejectedWithoutAuthority' = rejectedWithoutAuthority,
    staleRejectedOperations' = staleRejectedOperations,
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = None,
    lastDecision' = NetworkPartitionChanged,
  }

  /// Starting and durably preparing an intent may be one network-bounded DFS
  /// fanout. Keeping the unprepared branch preserves the W-only rejection
  /// cases; the prepared branch is a stuttering refinement of begin + receipt
  /// + metadata acknowledgement and keeps the long split witnesses reachable.
  action beginIntent(preparation: IntentPreparation): bool = {
    val prepareFullShard = preparation == FullShardPreparedIntent
    val prepareWithQuorum = preparation == QuorumPreparedIntent
    val prepare = prepareFullShard or prepareWithQuorum
    val startedIntent = {
      operation: nextOperation,
      coordinator: authorityPrimary,
      epoch: authorityEpoch,
      assignmentRevision: authorityAssignmentRevision,
      clusterConfigRevision: authorityClusterConfigRevision,
      baseVersion: visibleVersion,
      targetVersion: visibleVersion + 1,
    }
    val reachableAssignment = authorityMetadataReplicas.intersect(
      reachableFrom(authorityPrimary)
    )
    val reachableVoters = reachableFrom(authorityPrimary).intersect(
      clusterVoters
    )
    val preparedAcknowledgements = Nodes.mapBy(replica =>
      if (any {
        prepareFullShard and authorityMetadataReplicas.contains(replica),
        prepareWithQuorum and reachableAssignment.contains(replica),
      }) {
        Some({
          operation: startedIntent.operation,
          coordinator: startedIntent.coordinator,
          epoch: startedIntent.epoch,
          assignmentRevision: startedIntent.assignmentRevision,
          baseVersion: startedIntent.baseVersion,
          targetVersion: startedIntent.targetVersion,
        })
      } else {
        None
      }
    )
    val preparedWatermarks = Nodes.mapBy(replica =>
      if (all {
        prepare,
        reachableAssignment.contains(replica),
        nextOperation > exactStateWatermarkByReplica.get(replica),
      }) {
        nextOperation
      } else {
        exactStateWatermarkByReplica.get(replica)
      }
    )
    all {
      nextOperation <= MaxOperation,
      visibleVersion < MaxVersion,
      not(authorityHandoffPending and retiringPrimaryFenceAcknowledged),
      intentByOperation.get(nextOperation) == None,
      publicationByOperation.get(nextOperation) == None,
      prepareFullShard implies authorityMetadataReplicas.subseteq(
        reachableFrom(authorityPrimary)
      ),
      prepareWithQuorum implies all {
        not(authorityMetadataReplicas.subseteq(
          reachableFrom(authorityPrimary)
        )),
        reachableVoters.size() >= quorumSize(clusterVoters),
      },
      intentByOperation' =
        intentByOperation.set(nextOperation, Some(startedIntent)),
      contentReceiptsByOperation' =
        if (prepare) {
          contentReceiptsByOperation.set(
            nextOperation,
            Set(authorityPrimary)
          )
        } else {
          contentReceiptsByOperation
        },
      metadataAcksByOperation' =
        if (prepare) {
          metadataAcksByOperation.set(
            nextOperation,
            preparedAcknowledgements
          )
        } else {
          metadataAcksByOperation
        },
      exactStateWatermarkByReplica' = preparedWatermarks,
      nextOperation' = nextOperation + 1,
      networkPartition' = networkPartition,
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      assignmentRevision' = assignmentRevision,
      clusterConfigRevision' = clusterConfigRevision,
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      authorityPrimary' = authorityPrimary,
      authorityEpoch' = authorityEpoch,
      authorityAssignmentRevision' = authorityAssignmentRevision,
      authorityClusterConfigRevision' = authorityClusterConfigRevision,
      authorityMetadataReplicas' = authorityMetadataReplicas,
      authorityHandoffPending' = authorityHandoffPending,
      retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
      retiringFenceOperationFloor' = retiringFenceOperationFloor,
      reconfigurationKind' = reconfigurationKind,
      retiringPreparedOperations' = retiringPreparedOperations,
      carriedRetiringOperations' = carriedRetiringOperations,
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      primaryByEpoch' = primaryByEpoch,
      assignmentByRevision' = assignmentByRevision,
      votersByRevision' = votersByRevision,
      visibleVersion' = visibleVersion,
      permitByOperation' =
        if (prepareWithQuorum) {
          permitByOperation.set(nextOperation, Some({
            operation: startedIntent.operation,
            coordinator: startedIntent.coordinator,
            epoch: startedIntent.epoch,
            assignmentRevision: startedIntent.assignmentRevision,
            clusterConfigRevision: startedIntent.clusterConfigRevision,
            baseVersion: startedIntent.baseVersion,
            targetVersion: startedIntent.targetVersion,
            voters: reachableVoters,
          }))
        } else {
          permitByOperation
        },
      publicationByOperation' = publicationByOperation,
      publishedOperationByVersion' = publishedOperationByVersion,
      publicationAttempts' = publicationAttempts,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      staleRejectedOperations' = staleRejectedOperations,
      conflictRejectedOperations' = conflictRejectedOperations,
      lastOperation' = Some(nextOperation),
      lastDecision' = IntentStarted,
    }
  }

  action recordContentReceipt(operation: int, replica: Node): bool = all {
    Operations.contains(operation),
    isSome(intentByOperation.get(operation)),
    DesiredContentReplicas.contains(replica),
    reachableFrom(intent(operation).coordinator).contains(replica),
    contentReceiptsByOperation' = contentReceiptsByOperation.set(
      operation,
      contentReceiptsByOperation.get(operation).union(Set(replica))
    ),
    networkPartition' = networkPartition,
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    assignmentRevision' = assignmentRevision,
    clusterConfigRevision' = clusterConfigRevision,
    metadataReplicas' = metadataReplicas,
    clusterVoters' = clusterVoters,
    authorityPrimary' = authorityPrimary,
    authorityEpoch' = authorityEpoch,
    authorityAssignmentRevision' = authorityAssignmentRevision,
    authorityClusterConfigRevision' = authorityClusterConfigRevision,
    authorityMetadataReplicas' = authorityMetadataReplicas,
    authorityHandoffPending' = authorityHandoffPending,
    retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
    retiringFenceOperationFloor' = retiringFenceOperationFloor,
    reconfigurationKind' = reconfigurationKind,
    retiringPreparedOperations' = retiringPreparedOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    directPrimaryChangeRejected' = directPrimaryChangeRejected,
    primaryByEpoch' = primaryByEpoch,
    assignmentByRevision' = assignmentByRevision,
    votersByRevision' = votersByRevision,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    publicationAttempts' = publicationAttempts,
    rejectedWithoutAuthority' = rejectedWithoutAuthority,
    staleRejectedOperations' = staleRejectedOperations,
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = Some(operation),
    lastDecision' = ContentReceiptRecorded,
  }

  /// Fan out the hidden DFS prepare to every historically assigned replica
  /// reachable from this intent's coordinator. Each stored acknowledgement is
  /// operation-, epoch-, assignment-, base-, and target-bound.
  action recordMetadataAcknowledgements(operation: int): bool = {
    val pending = intent(operation)
    val historicalAssignment =
      assignmentByRevision.get(pending.assignmentRevision)
    val reachableAssignment =
      historicalAssignment.intersect(reachableFrom(pending.coordinator))
    val oldAcknowledgements = metadataAcksByOperation.get(operation)
    val newAcknowledgements = Nodes.mapBy(replica =>
      if (reachableAssignment.contains(replica)) {
        Some({
          operation: pending.operation,
          coordinator: pending.coordinator,
          epoch: pending.epoch,
          assignmentRevision: pending.assignmentRevision,
          baseVersion: pending.baseVersion,
          targetVersion: pending.targetVersion,
        })
      } else {
        oldAcknowledgements.get(replica)
      }
    )
    val newExactStateWatermarks = Nodes.mapBy(replica =>
      if (all {
        reachableAssignment.contains(replica),
        operation > exactStateWatermarkByReplica.get(replica),
      }) {
        operation
      } else {
        exactStateWatermarkByReplica.get(replica)
      }
    )
    all {
      Operations.contains(operation),
      isSome(intentByOperation.get(operation)),
      not(authorityHandoffPending and retiringPrimaryFenceAcknowledged),
      reachableAssignment.size() > 0,
      metadataAcksByOperation' =
        metadataAcksByOperation.set(operation, newAcknowledgements),
      networkPartition' = networkPartition,
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      assignmentRevision' = assignmentRevision,
      clusterConfigRevision' = clusterConfigRevision,
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      authorityPrimary' = authorityPrimary,
      authorityEpoch' = authorityEpoch,
      authorityAssignmentRevision' = authorityAssignmentRevision,
      authorityClusterConfigRevision' = authorityClusterConfigRevision,
      authorityMetadataReplicas' = authorityMetadataReplicas,
      authorityHandoffPending' = authorityHandoffPending,
      retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
      retiringFenceOperationFloor' = retiringFenceOperationFloor,
      reconfigurationKind' = reconfigurationKind,
      retiringPreparedOperations' = retiringPreparedOperations,
      carriedRetiringOperations' = carriedRetiringOperations,
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      exactStateWatermarkByReplica' = newExactStateWatermarks,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      primaryByEpoch' = primaryByEpoch,
      assignmentByRevision' = assignmentByRevision,
      votersByRevision' = votersByRevision,
      nextOperation' = nextOperation,
      visibleVersion' = visibleVersion,
      intentByOperation' = intentByOperation,
      contentReceiptsByOperation' = contentReceiptsByOperation,
      permitByOperation' = permitByOperation,
      publicationByOperation' = publicationByOperation,
      publishedOperationByVersion' = publishedOperationByVersion,
      publicationAttempts' = publicationAttempts,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      staleRejectedOperations' = staleRejectedOperations,
      conflictRejectedOperations' = conflictRejectedOperations,
      lastOperation' = Some(operation),
      lastDecision' = MetadataAcknowledgementsRecorded,
    }
  }

  /// The quorum path is exceptional. It is available only when the complete
  /// metadata-shard assignment is not reachable, and the permit is frozen to
  /// this operation and committed control-plane revisions.
  action issueQuorumPermit(operation: int): bool = {
    val pending = intent(operation)
    val reachableVoters =
      reachableFrom(pending.coordinator).intersect(clusterVoters)
    all {
      Operations.contains(operation),
      intentHasCurrentAuthority(operation),
      not(authorityHandoffPending and retiringPrimaryFenceAcknowledged),
      not(allConfiguredMetadataReplicasReachable(operation)),
      reachableVoters.size() >= quorumSize(clusterVoters),
      permitByOperation' = permitByOperation.set(operation, Some({
        operation: pending.operation,
        coordinator: pending.coordinator,
        epoch: pending.epoch,
        assignmentRevision: pending.assignmentRevision,
        clusterConfigRevision: pending.clusterConfigRevision,
        baseVersion: pending.baseVersion,
        targetVersion: pending.targetVersion,
        voters: reachableVoters,
      })),
      networkPartition' = networkPartition,
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      assignmentRevision' = assignmentRevision,
      clusterConfigRevision' = clusterConfigRevision,
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      authorityPrimary' = authorityPrimary,
      authorityEpoch' = authorityEpoch,
      authorityAssignmentRevision' = authorityAssignmentRevision,
      authorityClusterConfigRevision' = authorityClusterConfigRevision,
      authorityMetadataReplicas' = authorityMetadataReplicas,
      authorityHandoffPending' = authorityHandoffPending,
      retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
      retiringFenceOperationFloor' = retiringFenceOperationFloor,
      reconfigurationKind' = reconfigurationKind,
      retiringPreparedOperations' = retiringPreparedOperations,
      carriedRetiringOperations' = carriedRetiringOperations,
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      primaryByEpoch' = primaryByEpoch,
      assignmentByRevision' = assignmentByRevision,
      votersByRevision' = votersByRevision,
      nextOperation' = nextOperation,
      visibleVersion' = visibleVersion,
      intentByOperation' = intentByOperation,
      contentReceiptsByOperation' = contentReceiptsByOperation,
      metadataAcksByOperation' = metadataAcksByOperation,
      publicationByOperation' = publicationByOperation,
      publishedOperationByVersion' = publishedOperationByVersion,
      publicationAttempts' = publicationAttempts,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      staleRejectedOperations' = staleRejectedOperations,
      conflictRejectedOperations' = conflictRejectedOperations,
      lastOperation' = Some(operation),
      lastDecision' = QuorumPermitIssued,
    }
  }

  action publishVisibleVersion(
    operation: int,
    authorization: CommitAuthorization
  ): bool = {
    val pending = intent(operation)
    val fullShardPath = authorization == FullShardAcknowledgements
    val quorumPath = authorization == ClusterQuorumPermit
    val permit = unwrapPermit(permitByOperation.get(operation))
    val displacedStaleOperations = Operations.filter(candidate => all {
      candidate != operation,
      isSome(intentByOperation.get(candidate)),
      intent(candidate).baseVersion == pending.baseVersion,
      intent(candidate).targetVersion == pending.targetVersion,
      not(intentHasCurrentAuthority(candidate)),
      hasHistoricalAuthorityEvidence(candidate),
    })
    all {
      Operations.contains(operation),
      intentHasCurrentAuthority(operation),
      contentThresholdMet(operation),
      publishedOperationByVersion.get(pending.targetVersion) == None,
      publishedOperationsForVersion(pending.targetVersion).size() == 0,
      any {
        fullShardPath and fullFreshShardAcknowledgements(operation),
        quorumPath and matchingQuorumPermit(operation),
      },
      publicationByOperation' = publicationByOperation.set(operation, Some({
        operation: pending.operation,
        coordinator: pending.coordinator,
        epoch: pending.epoch,
        assignmentRevision: pending.assignmentRevision,
        clusterConfigRevision: pending.clusterConfigRevision,
        baseVersion: pending.baseVersion,
        targetVersion: pending.targetVersion,
        contentReceipts: contentReceiptsByOperation.get(operation),
        authorization: authorization,
        metadataAssignment: authorityMetadataReplicas,
        metadataAcknowledgers:
          if (fullShardPath) { freshMetadataAcknowledgers(operation) } else { Set() },
        clusterVoters: clusterVoters,
        permitVoters: if (quorumPath) { permit.voters } else { Set() },
      })),
      publishedOperationByVersion' =
        publishedOperationByVersion.set(pending.targetVersion, Some(operation)),
      visibleVersion' = pending.targetVersion,
      intentByOperation' = intentByOperation.set(operation, None),
      publicationAttempts' = publicationAttempts
        .union(Set(operation))
        .union(displacedStaleOperations),
      networkPartition' = networkPartition,
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      assignmentRevision' = assignmentRevision,
      clusterConfigRevision' = clusterConfigRevision,
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      authorityPrimary' = authorityPrimary,
      authorityEpoch' = authorityEpoch,
      authorityAssignmentRevision' = authorityAssignmentRevision,
      authorityClusterConfigRevision' = authorityClusterConfigRevision,
      authorityMetadataReplicas' = authorityMetadataReplicas,
      authorityHandoffPending' = authorityHandoffPending,
      retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
      retiringFenceOperationFloor' = retiringFenceOperationFloor,
      reconfigurationKind' = reconfigurationKind,
      retiringPreparedOperations' = retiringPreparedOperations,
      carriedRetiringOperations' = carriedRetiringOperations,
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      primaryByEpoch' = primaryByEpoch,
      assignmentByRevision' = assignmentByRevision,
      votersByRevision' = votersByRevision,
      nextOperation' = nextOperation,
      contentReceiptsByOperation' = contentReceiptsByOperation,
      metadataAcksByOperation' = metadataAcksByOperation,
      permitByOperation' = permitByOperation,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      staleRejectedOperations' =
        staleRejectedOperations.union(displacedStaleOperations),
      conflictRejectedOperations' =
        conflictRejectedOperations.union(displacedStaleOperations),
      lastOperation' = Some(operation),
      lastDecision' = VisibleVersionPublished,
    }
  }

  /// A component that has W content receipts but neither complete fresh shard
  /// acknowledgements nor a matching quorum permit must fail closed.
  action rejectPublicationWithoutAuthority(operation: int): bool = all {
    Operations.contains(operation),
    intentHasCurrentAuthority(operation),
    contentThresholdMet(operation),
    not(fullFreshShardAcknowledgements(operation)),
    not(matchingQuorumPermit(operation)),
    publicationAttempts' = publicationAttempts.union(Set(operation)),
    rejectedWithoutAuthority' = rejectedWithoutAuthority.union(Set(operation)),
    networkPartition' = networkPartition,
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    assignmentRevision' = assignmentRevision,
    clusterConfigRevision' = clusterConfigRevision,
    metadataReplicas' = metadataReplicas,
    clusterVoters' = clusterVoters,
    authorityPrimary' = authorityPrimary,
    authorityEpoch' = authorityEpoch,
    authorityAssignmentRevision' = authorityAssignmentRevision,
    authorityClusterConfigRevision' = authorityClusterConfigRevision,
    authorityMetadataReplicas' = authorityMetadataReplicas,
    authorityHandoffPending' = authorityHandoffPending,
    retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
    retiringFenceOperationFloor' = retiringFenceOperationFloor,
    reconfigurationKind' = reconfigurationKind,
    retiringPreparedOperations' = retiringPreparedOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    directPrimaryChangeRejected' = directPrimaryChangeRejected,
    primaryByEpoch' = primaryByEpoch,
    assignmentByRevision' = assignmentByRevision,
    votersByRevision' = votersByRevision,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    contentReceiptsByOperation' = contentReceiptsByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    staleRejectedOperations' = staleRejectedOperations,
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = Some(operation),
    lastDecision' = PublicationRejected,
  }

  /// Reject an operation whose coordinator/epoch/assignment/base is no longer
  /// canonical. If a different operation already owns its target version, the
  /// same attempt is also recorded as a rejected competing publication.
  action rejectStaleAuthority(operation: int): bool = {
    val pending = intent(operation)
    val targetClaim = publishedOperationByVersion.get(pending.targetVersion)
    val competing = match targetClaim {
      | Some(publishedOperation) => publishedOperation != operation
      | None => false
    }
    all {
      Operations.contains(operation),
      isSome(intentByOperation.get(operation)),
      not(intentHasCurrentAuthority(operation)),
      contentThresholdMet(operation),
      hasHistoricalAuthorityEvidence(operation),
      publicationAttempts' = publicationAttempts.union(Set(operation)),
      staleRejectedOperations' = staleRejectedOperations.union(Set(operation)),
      conflictRejectedOperations' =
        if (competing) {
          conflictRejectedOperations.union(Set(operation))
        } else {
          conflictRejectedOperations
        },
      networkPartition' = networkPartition,
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      assignmentRevision' = assignmentRevision,
      clusterConfigRevision' = clusterConfigRevision,
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      authorityPrimary' = authorityPrimary,
      authorityEpoch' = authorityEpoch,
      authorityAssignmentRevision' = authorityAssignmentRevision,
      authorityClusterConfigRevision' = authorityClusterConfigRevision,
      authorityMetadataReplicas' = authorityMetadataReplicas,
      authorityHandoffPending' = authorityHandoffPending,
      retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
      retiringFenceOperationFloor' = retiringFenceOperationFloor,
      reconfigurationKind' = reconfigurationKind,
      retiringPreparedOperations' = retiringPreparedOperations,
      carriedRetiringOperations' = carriedRetiringOperations,
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      primaryByEpoch' = primaryByEpoch,
      assignmentByRevision' = assignmentByRevision,
      votersByRevision' = votersByRevision,
      nextOperation' = nextOperation,
      visibleVersion' = visibleVersion,
      intentByOperation' = intentByOperation,
      contentReceiptsByOperation' = contentReceiptsByOperation,
      metadataAcksByOperation' = metadataAcksByOperation,
      permitByOperation' = permitByOperation,
      publicationByOperation' = publicationByOperation,
      publishedOperationByVersion' = publishedOperationByVersion,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      lastOperation' = Some(operation),
      lastDecision' = StaleAuthorityRejected,
    }
  }

  /// Generic membership CAS: the voter quorum may add N2 to the replica set
  /// while N1 is isolated, but it must retain the exact primary and bump the
  /// epoch. The committed config is deliberately not active DFS authority yet.
  action commitSamePrimaryMembershipReconfiguration: bool = all {
    networkPartition == OldPrimaryIsolated,
    currentPrimary == N1,
    currentEpoch == 1,
    assignmentRevision == 1,
    clusterConfigRevision == 1,
    fullyPreparedOperations.size() > 0,
    reachableFrom(N2).intersect(clusterVoters).size()
      >= quorumSize(clusterVoters),
    currentPrimary' = N1,
    currentEpoch' = 2,
    assignmentRevision' = 2,
    clusterConfigRevision' = 2,
    metadataReplicas' = Nodes,
    primaryByEpoch' = primaryByEpoch.set(2, Some(N1)),
    assignmentByRevision' = assignmentByRevision.set(2, Nodes),
    votersByRevision' = votersByRevision.set(2, clusterVoters),
    authorityPrimary' = authorityPrimary,
    authorityEpoch' = authorityEpoch,
    authorityAssignmentRevision' = authorityAssignmentRevision,
    authorityClusterConfigRevision' = authorityClusterConfigRevision,
    authorityMetadataReplicas' = authorityMetadataReplicas,
    authorityHandoffPending' = true,
    retiringPrimaryFenceAcknowledged' = false,
    retiringFenceOperationFloor' = 0,
    reconfigurationKind' = SamePrimaryMembershipChange,
    retiringPreparedOperations' = retiringShardOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = nextOperation - 1,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    directPrimaryChangeRejected' = directPrimaryChangeRejected,
    networkPartition' = networkPartition,
    clusterVoters' = clusterVoters,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    contentReceiptsByOperation' = contentReceiptsByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    publicationAttempts' = publicationAttempts,
    rejectedWithoutAuthority' = rejectedWithoutAuthority,
    staleRejectedOperations' = staleRejectedOperations,
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = None,
    lastDecision' = ControlPlaneReconfigured,
  }

  /// A bare primary mutation is not a legal generic membership CAS. This
  /// transition records the rejected attempt without changing either the
  /// committed configuration or active DFS authority.
  action rejectDirectPrimaryChange: bool = all {
    networkPartition == OldPrimaryIsolated,
    currentPrimary == N1,
    currentEpoch == 1,
    reachableFrom(N2).intersect(clusterVoters).size()
      >= quorumSize(clusterVoters),
    directPrimaryChangeRejected' = true,
    networkPartition' = networkPartition,
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    assignmentRevision' = assignmentRevision,
    clusterConfigRevision' = clusterConfigRevision,
    metadataReplicas' = metadataReplicas,
    clusterVoters' = clusterVoters,
    authorityPrimary' = authorityPrimary,
    authorityEpoch' = authorityEpoch,
    authorityAssignmentRevision' = authorityAssignmentRevision,
    authorityClusterConfigRevision' = authorityClusterConfigRevision,
    authorityMetadataReplicas' = authorityMetadataReplicas,
    authorityHandoffPending' = authorityHandoffPending,
    retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
    retiringFenceOperationFloor' = retiringFenceOperationFloor,
    reconfigurationKind' = reconfigurationKind,
    retiringPreparedOperations' = retiringPreparedOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    primaryByEpoch' = primaryByEpoch,
    assignmentByRevision' = assignmentByRevision,
    votersByRevision' = votersByRevision,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    contentReceiptsByOperation' = contentReceiptsByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    publicationAttempts' = publicationAttempts,
    rejectedWithoutAuthority' = rejectedWithoutAuthority,
    staleRejectedOperations' = staleRejectedOperations,
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = None,
    lastDecision' = DirectPrimaryChangeRejected,
  }

  /// The quorum-side pending decision does not atomically stop an isolated
  /// retiring primary. Once that primary reconnects, it observes the pending
  /// handoff under the same guard as canonical publication and captures the
  /// exact state after every earlier publication. Only this transition is the
  /// retiring-primary fence represented by production's fence acknowledgement.
  action acknowledgeRetiringPrimaryFence: bool = {
    val preparedAtFence =
      retiringPreparedOperations.union(retiringShardOperations)
    all {
      authorityHandoffPending,
      not(retiringPrimaryFenceAcknowledged),
      currentEpoch == 2,
      currentPrimary == N1,
      authorityEpoch == 1,
      reachableFrom(N2).contains(authorityPrimary),
      authorityPrimary' = authorityPrimary,
      authorityEpoch' = authorityEpoch,
      authorityAssignmentRevision' = authorityAssignmentRevision,
      authorityClusterConfigRevision' = authorityClusterConfigRevision,
      authorityMetadataReplicas' = authorityMetadataReplicas,
      authorityHandoffPending' = authorityHandoffPending,
      retiringPrimaryFenceAcknowledged' = true,
      retiringFenceOperationFloor' = nextOperation - 1,
      reconfigurationKind' = reconfigurationKind,
      retiringPreparedOperations' = preparedAtFence,
      carriedRetiringOperations' = carriedRetiringOperations,
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      networkPartition' = networkPartition,
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      assignmentRevision' = assignmentRevision,
      clusterConfigRevision' = clusterConfigRevision,
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      primaryByEpoch' = primaryByEpoch,
      assignmentByRevision' = assignmentByRevision,
      votersByRevision' = votersByRevision,
      nextOperation' = nextOperation,
      visibleVersion' = visibleVersion,
      intentByOperation' = intentByOperation,
      contentReceiptsByOperation' = contentReceiptsByOperation,
      metadataAcksByOperation' = metadataAcksByOperation,
      permitByOperation' = permitByOperation,
      publicationByOperation' = publicationByOperation,
      publishedOperationByVersion' = publishedOperationByVersion,
      publicationAttempts' = publicationAttempts,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      staleRejectedOperations' = staleRejectedOperations,
      conflictRejectedOperations' = conflictRejectedOperations,
      lastOperation' = None,
      lastDecision' = RetiringPrimaryFenceAcknowledged,
    }
  }

  /// Activation requires reconnecting to the retiring shard and transferring
  /// an exact state whose watermark covers both the quorum CAS operation floor
  /// and every full-shard prepare observed before the fence. Until this action,
  /// epoch 1 remains the only active DFS write authority.
  action completeAuthorityHandoff(promoteTransferredReplica: bool): bool = {
    val transferWatermark = retiringFenceOperationFloor
    val preparedAtFence = retiringPreparedOperations
    val newWatermarks = Nodes.mapBy(replica =>
      if (metadataReplicas.contains(replica)) {
        transferWatermark
      } else {
        exactStateWatermarkByReplica.get(replica)
      }
    )
    all {
      authorityHandoffPending,
      retiringPrimaryFenceAcknowledged,
      currentEpoch == 2,
      currentPrimary == N1,
      authorityEpoch == 1,
      reachableFrom(N2).contains(authorityPrimary),
      authorityMetadataReplicas.subseteq(reachableFrom(N2)),
      metadataReplicas.subseteq(reachableFrom(N2)),
      transferWatermark >= requiredHandoffOperationFloor,
      promoteTransferredReplica implies metadataReplicas.contains(N2),
      authorityPrimary' =
        if (promoteTransferredReplica) { N2 } else { currentPrimary },
      authorityEpoch' =
        if (promoteTransferredReplica) { 3 } else { currentEpoch },
      authorityAssignmentRevision' =
        if (promoteTransferredReplica) { 3 } else { assignmentRevision },
      authorityClusterConfigRevision' =
        if (promoteTransferredReplica) { 3 } else { clusterConfigRevision },
      authorityMetadataReplicas' = metadataReplicas,
      authorityHandoffPending' = false,
      retiringPrimaryFenceAcknowledged' = false,
      retiringFenceOperationFloor' = retiringFenceOperationFloor,
      retiringPreparedOperations' = preparedAtFence,
      carriedRetiringOperations' =
        carriedRetiringOperations.union(preparedAtFence),
      exactStateWatermarkByReplica' = newWatermarks,
      networkPartition' = networkPartition,
      currentPrimary' =
        if (promoteTransferredReplica) { N2 } else { currentPrimary },
      currentEpoch' =
        if (promoteTransferredReplica) { 3 } else { currentEpoch },
      assignmentRevision' =
        if (promoteTransferredReplica) { 3 } else { assignmentRevision },
      clusterConfigRevision' =
        if (promoteTransferredReplica) { 3 } else { clusterConfigRevision },
      metadataReplicas' = metadataReplicas,
      clusterVoters' = clusterVoters,
      reconfigurationKind' =
        if (promoteTransferredReplica) {
          PrimaryPromotion
        } else {
          reconfigurationKind
        },
      requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
      directPrimaryChangeRejected' = directPrimaryChangeRejected,
      primaryByEpoch' =
        if (promoteTransferredReplica) {
          primaryByEpoch.set(3, Some(N2))
        } else {
          primaryByEpoch
        },
      assignmentByRevision' =
        if (promoteTransferredReplica) {
          assignmentByRevision.set(3, metadataReplicas)
        } else {
          assignmentByRevision
        },
      votersByRevision' =
        if (promoteTransferredReplica) {
          votersByRevision.set(3, clusterVoters)
        } else {
          votersByRevision
        },
      nextOperation' = nextOperation,
      visibleVersion' = visibleVersion,
      intentByOperation' = intentByOperation,
      contentReceiptsByOperation' = contentReceiptsByOperation,
      metadataAcksByOperation' = metadataAcksByOperation,
      permitByOperation' = permitByOperation,
      publicationByOperation' = publicationByOperation,
      publishedOperationByVersion' = publishedOperationByVersion,
      publicationAttempts' = publicationAttempts,
      rejectedWithoutAuthority' = rejectedWithoutAuthority,
      staleRejectedOperations' = staleRejectedOperations,
      conflictRejectedOperations' = conflictRejectedOperations,
      lastOperation' = None,
      lastDecision' =
        if (promoteTransferredReplica) {
          ReplicaPromotedWithTransferProof
        } else {
          AuthorityHandoffCompleted
        },
    }
  }

  /// Primary transfer is proof-carrying rather than a generic membership CAS.
  /// N2 must already be a current replica with an exact new-epoch watermark at
  /// or above both the CAS floor and every operation allocated before transfer.
  action promoteReplicaWithTransferProof: bool = all {
    currentEpoch == 2,
    currentPrimary == N1,
    not(authorityHandoffPending),
    authorityEpoch == currentEpoch,
    metadataReplicas.contains(N2),
    exactStateWatermarkByReplica.get(N2) >= requiredHandoffOperationFloor,
    exactStateWatermarkByReplica.get(N2) >= retiringFenceOperationFloor,
    exactStateWatermarkByReplica.get(N2) >= nextOperation - 1,
    currentPrimary' = N2,
    currentEpoch' = 3,
    assignmentRevision' = 3,
    clusterConfigRevision' = 3,
    metadataReplicas' = metadataReplicas,
    primaryByEpoch' = primaryByEpoch.set(3, Some(N2)),
    assignmentByRevision' = assignmentByRevision.set(3, metadataReplicas),
    votersByRevision' = votersByRevision.set(3, clusterVoters),
    authorityPrimary' = N2,
    authorityEpoch' = 3,
    authorityAssignmentRevision' = 3,
    authorityClusterConfigRevision' = 3,
    authorityMetadataReplicas' = metadataReplicas,
    authorityHandoffPending' = false,
    retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
    retiringFenceOperationFloor' = retiringFenceOperationFloor,
    reconfigurationKind' = PrimaryPromotion,
    retiringPreparedOperations' = retiringPreparedOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    directPrimaryChangeRejected' = directPrimaryChangeRejected,
    networkPartition' = networkPartition,
    clusterVoters' = clusterVoters,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    contentReceiptsByOperation' = contentReceiptsByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    publicationAttempts' =
      publicationAttempts.union(currentQuorumPermittedOperations),
    rejectedWithoutAuthority' = rejectedWithoutAuthority,
    staleRejectedOperations' =
      staleRejectedOperations.union(currentQuorumPermittedOperations),
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = None,
    lastDecision' = ReplicaPromotedWithTransferProof,
  }

  action stutter: bool = all {
    networkPartition' = networkPartition,
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    assignmentRevision' = assignmentRevision,
    clusterConfigRevision' = clusterConfigRevision,
    metadataReplicas' = metadataReplicas,
    clusterVoters' = clusterVoters,
    authorityPrimary' = authorityPrimary,
    authorityEpoch' = authorityEpoch,
    authorityAssignmentRevision' = authorityAssignmentRevision,
    authorityClusterConfigRevision' = authorityClusterConfigRevision,
    authorityMetadataReplicas' = authorityMetadataReplicas,
    authorityHandoffPending' = authorityHandoffPending,
    retiringPrimaryFenceAcknowledged' = retiringPrimaryFenceAcknowledged,
    retiringFenceOperationFloor' = retiringFenceOperationFloor,
    reconfigurationKind' = reconfigurationKind,
    retiringPreparedOperations' = retiringPreparedOperations,
    carriedRetiringOperations' = carriedRetiringOperations,
    requiredHandoffOperationFloor' = requiredHandoffOperationFloor,
    exactStateWatermarkByReplica' = exactStateWatermarkByReplica,
    directPrimaryChangeRejected' = directPrimaryChangeRejected,
    primaryByEpoch' = primaryByEpoch,
    assignmentByRevision' = assignmentByRevision,
    votersByRevision' = votersByRevision,
    nextOperation' = nextOperation,
    visibleVersion' = visibleVersion,
    intentByOperation' = intentByOperation,
    contentReceiptsByOperation' = contentReceiptsByOperation,
    metadataAcksByOperation' = metadataAcksByOperation,
    permitByOperation' = permitByOperation,
    publicationByOperation' = publicationByOperation,
    publishedOperationByVersion' = publishedOperationByVersion,
    publicationAttempts' = publicationAttempts,
    rejectedWithoutAuthority' = rejectedWithoutAuthority,
    staleRejectedOperations' = staleRejectedOperations,
    conflictRejectedOperations' = conflictRejectedOperations,
    lastOperation' = lastOperation,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet nextPartition = oneOf(
        Set(Connected, OldPrimaryIsolated, ThirdReplicaIsolated)
      )
      changeNetworkPartition(nextPartition)
    },
    {
      nondet preparation = oneOf(Set(
        UnpreparedIntent,
        FullShardPreparedIntent,
        QuorumPreparedIntent
      ))
      beginIntent(preparation)
    },
    {
      nondet operation = oneOf(Operations)
      nondet replica = oneOf(Nodes)
      recordContentReceipt(operation, replica)
    },
    {
      nondet operation = oneOf(Operations)
      recordMetadataAcknowledgements(operation)
    },
    {
      nondet operation = oneOf(Operations)
      issueQuorumPermit(operation)
    },
    {
      nondet operation = oneOf(Operations)
      publishVisibleVersion(operation, FullShardAcknowledgements)
    },
    {
      nondet operation = oneOf(Operations)
      publishVisibleVersion(operation, ClusterQuorumPermit)
    },
    {
      nondet operation = oneOf(Operations)
      rejectPublicationWithoutAuthority(operation)
    },
    {
      nondet operation = oneOf(Operations)
      rejectStaleAuthority(operation)
    },
    commitSamePrimaryMembershipReconfiguration,
    rejectDirectPrimaryChange,
    acknowledgeRetiringPrimaryFence,
    {
      nondet promoteTransferredReplica = oneOf(Set(true, false))
      completeAuthorityHandoff(promoteTransferredReplica)
    },
    promoteReplicaWithTransferProof,
    stutter,
  }
}
