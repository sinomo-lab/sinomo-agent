/// Safety properties and focused witnesses for ordinary canonical DFS writes.
module dfs_canonical_write_eligibility_safety {
  import common.* from "../../lib/common"
  import dfs_canonical_write_eligibility.* from "../../models/dfs/canonical_write_eligibility"

  def currentControlPlaneFactsAreRecorded: bool = all {
    primaryByEpoch.get(currentEpoch) == Some(currentPrimary),
    assignmentByRevision.get(assignmentRevision) == metadataReplicas,
    votersByRevision.get(clusterConfigRevision) == clusterVoters,
  }

  def publicationIndexMatchesOperationRecords: bool =
    Versions.forall(version =>
      match publishedOperationByVersion.get(version) {
        | Some(operation) => all {
            publishedOperationsForVersion(version) == Set(operation),
            match publicationByOperation.get(operation) {
              | Some(record) => record.targetVersion == version
              | None => false
            },
          }
        | None => publishedOperationsForVersion(version) == Set()
      }
    )

  def visibleHistoryIsDenseAndUnique: bool =
    Versions.forall(version =>
      if (version <= visibleVersion) {
        publishedOperationsForVersion(version).size() == 1
      } else {
        publishedOperationsForVersion(version).size() == 0
      }
    )

  def committedVersionsAdvanceExactlyOnceFromTheirBase: bool =
    Operations.forall(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.operation == operation,
            record.targetVersion >= 1,
            record.targetVersion <= MaxVersion,
            record.baseVersion + 1 == record.targetVersion,
          }
        | None => true
      }
    )

  def everyCommitHasWExactContentReceipts: bool =
    Operations.forall(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.contentReceipts.subseteq(DesiredContentReplicas),
            record.contentReceipts.size() >= MinContentReceipts,
          }
        | None => true
      }
    )

  def everyCommitUsesARecordedPrimaryEpochAndAssignment: bool =
    Operations.forall(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            primaryByEpoch.get(record.epoch) == Some(record.coordinator),
            assignmentByRevision.get(record.assignmentRevision)
              == record.metadataAssignment,
            votersByRevision.get(record.clusterConfigRevision)
              == record.clusterVoters,
          }
        | None => true
      }
    )

  def everyCommitHasExactlyOneValidAuthorityPath: bool =
    Operations.forall(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) =>
            match record.authorization {
              | FullShardAcknowledgements => all {
                  record.metadataAcknowledgers == record.metadataAssignment,
                  record.permitVoters == Set(),
                }
              | ClusterQuorumPermit => all {
                  record.metadataAcknowledgers == Set(),
                  record.permitVoters.subseteq(record.clusterVoters),
                  record.permitVoters.size() >= quorumSize(record.clusterVoters),
                }
            }
        | None => true
      }
    )

  def minorityWithOnlyWRemainsUnpublished: bool =
    lastDecision == PublicationRejected implies
      match lastOperation {
        | Some(operation) => all {
            rejectedWithoutAuthority.contains(operation),
            contentThresholdMet(operation),
            not(fullFreshShardAcknowledgements(operation)),
            not(matchingQuorumPermit(operation)),
            publicationByOperation.get(operation) == None,
          }
        | None => false
      }

  def staleAuthorityEvidenceNeverPublishes: bool =
    staleRejectedOperations.forall(operation => all {
      publicationAttempts.contains(operation),
      publicationByOperation.get(operation) == None,
    })

  /// During a committed-but-unactivated membership change, epoch 1 remains
  /// the only DFS authority. After exact-state handoff, old evidence is fenced.
  def oldPrimaryEvidenceIsFencedAfterQuorumReconfiguration: bool =
    authorityEpoch >= 2 implies
      Operations.forall(operation => {
        val pending = intent(operation)
        all {
          isSome(intentByOperation.get(operation)),
          pending.coordinator == N1,
          pending.epoch == 1,
          hasHistoricalAuthorityEvidence(operation),
        } implies publicationByOperation.get(operation) == None
      })

  def pendingStaleAcknowledgementsDoNotBecomeFresh: bool =
    Operations.forall(operation => all {
      isSome(intentByOperation.get(operation)),
      hasStaleMetadataAcknowledgement(operation),
      freshMetadataAcknowledgers(operation) != authorityMetadataReplicas,
    } implies not(fullFreshShardAcknowledgements(operation)))

  def ordinaryFullShardPathDoesNotCarryAQuorumPermit: bool =
    Operations.forall(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) =>
            record.authorization != FullShardAcknowledgements
              or record.permitVoters == Set()
        | None => true
      }
    )

  /// This is derived from independently indexed publication records. Two
  /// operations with the same target version are representable, so weakening
  /// the publication fence makes this invariant fail.
  def noCanonicalConflicts: bool = not(canonicalConflictExists)

  def competingPublicationIsRejectedInsteadOfRecorded: bool =
    conflictRejectedOperations.forall(operation => all {
      publicationAttempts.contains(operation),
      staleRejectedOperations.contains(operation),
      publicationByOperation.get(operation) == None,
    })

  /// Generic membership CAS retains N1 and only commits configuration facts.
  /// The active DFS assignment stays on epoch 1 until exact state is carried.
  def committedAndActivatedAuthorityStaySeparatedDuringHandoff: bool =
    if (authorityHandoffPending) {
      all {
        currentEpoch == 2,
        currentPrimary == N1,
        metadataReplicas == Nodes,
        authorityEpoch == 1,
        authorityPrimary == N1,
        authorityMetadataReplicas == Set(N1),
        reconfigurationKind == SamePrimaryMembershipChange,
      }
    } else {
      all {
        authorityPrimary == currentPrimary,
        authorityEpoch == currentEpoch,
        authorityAssignmentRevision == assignmentRevision,
        authorityClusterConfigRevision == clusterConfigRevision,
        authorityMetadataReplicas == metadataReplicas,
      }
    }

  def genericMembershipChangeRetainsPrimaryAndBumpsEpoch: bool =
    currentEpoch == 2 implies all {
      currentPrimary == N1,
      metadataReplicas == Nodes,
      primaryByEpoch.get(2) == Some(N1),
      assignmentByRevision.get(2) == Nodes,
    }

  /// A quorum-side pending decision is distinct from the later local backend
  /// freeze. Once the retiring primary acknowledges that freeze, its exact
  /// captured floor covers the begin-time quorum floor and every retiring
  /// hidden prepare, and no old-authority publication can start afterward.
  def retiringPrimaryFenceAcknowledgesExactPostCommitState: bool = all {
    retiringPrimaryFenceAcknowledged implies all {
      authorityHandoffPending,
      retiringFenceOperationFloor >= requiredHandoffOperationFloor,
      retiringShardOperations.subseteq(retiringPreparedOperations),
    },
    not(authorityHandoffPending) implies
      not(retiringPrimaryFenceAcknowledged),
  }

  def proofCarryingPromotionRequiresCurrentExactReplica: bool =
    currentEpoch == 3 implies all {
      currentPrimary == N2,
      metadataReplicas.contains(N2),
      primaryByEpoch.get(3) == Some(N2),
      assignmentByRevision.get(3) == metadataReplicas,
      not(authorityHandoffPending),
      exactStateWatermarkByReplica.get(N2)
        >= requiredHandoffOperationFloor,
      exactStateWatermarkByReplica.get(N2)
        >= retiringFenceOperationFloor,
      reconfigurationKind == PrimaryPromotion,
    }

  /// A new-epoch publication is impossible while the committed membership is
  /// waiting for the retiring shard. Once it exists, the exact-state carry set
  /// covers every operation snapshotted at the activation fence.
  def newAssignmentCannotPublishBeforeRetiringStateIsCarried: bool =
    Operations.forall(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) =>
            record.epoch >= 2 implies all {
              not(authorityHandoffPending),
              retiringPreparedOperations.subseteq(carriedRetiringOperations),
              exactStateWatermarkByReplica.get(record.coordinator)
                >= requiredHandoffOperationFloor,
              exactStateWatermarkByReplica.get(record.coordinator)
                >= retiringFenceOperationFloor,
            }
        | None => true
      }
    )

  def retiringShardStateIsCarriedAtActivation: bool =
    all {
      currentEpoch >= 2,
      not(authorityHandoffPending),
    } implies all {
      retiringPreparedOperations.subseteq(carriedRetiringOperations),
      metadataReplicas.forall(replica =>
        exactStateWatermarkByReplica.get(replica)
          >= requiredHandoffOperationFloor
          and exactStateWatermarkByReplica.get(replica)
            >= retiringFenceOperationFloor
      ),
    }

  def stateRemainsBounded: bool = all {
    currentEpoch >= 1,
    currentEpoch <= MaxEpoch,
    assignmentRevision >= 1,
    assignmentRevision <= MaxRevision,
    clusterConfigRevision >= 1,
    clusterConfigRevision <= MaxRevision,
    visibleVersion >= 0,
    visibleVersion <= MaxVersion,
    nextOperation >= 1,
    nextOperation <= MaxOperation + 1,
    retiringFenceOperationFloor >= 0,
    retiringFenceOperationFloor <= MaxOperation,
  }

  def witnessConnectedWriteUsesFullShardAcksWithoutQuorumPermit: bool =
    Operations.exists(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.authorization == FullShardAcknowledgements,
            record.metadataAcknowledgers == record.metadataAssignment,
            record.permitVoters == Set(),
          }
        | None => false
      }
    )

  def witnessMinorityWithWCannotPublish: bool =
    Operations.exists(operation => all {
      rejectedWithoutAuthority.contains(operation),
      networkPartition == OldPrimaryIsolated,
      intent(operation).coordinator == N1,
      contentThresholdMet(operation),
      not(fullFreshShardAcknowledgements(operation)),
      not(matchingQuorumPermit(operation)),
      publicationByOperation.get(operation) == None,
    })

  def witnessQuorumPermitPublishesWithoutFullShardAcks: bool =
    Operations.exists(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.authorization == ClusterQuorumPermit,
            record.metadataAcknowledgers == Set(),
            record.permitVoters.size() >= quorumSize(record.clusterVoters),
            record.metadataAssignment != record.permitVoters,
          }
        | None => false
      }
    )

  /// Historical full-shard evidence for epoch 1 is carried across activation,
  /// but the retiring view cannot publish after the fence.
  def witnessStaleAssignmentAcknowledgementsCannotPublish: bool =
    Operations.exists(operation => all {
      authorityEpoch >= 2,
      staleRejectedOperations.contains(operation),
      intent(operation).coordinator == N1,
      intent(operation).epoch == 1,
      fullHistoricalShardAcknowledgements(operation),
      carriedRetiringOperations.contains(operation),
      not(intentHasCurrentAuthority(operation)),
      publicationByOperation.get(operation) == None,
    })

  def witnessStaleEpochOrPermitCannotPublish: bool =
    Operations.exists(operation => all {
      authorityEpoch == 3,
      staleRejectedOperations.contains(operation),
      intent(operation).coordinator == N1,
      intent(operation).epoch == 2,
      matchingHistoricalQuorumPermit(operation),
      not(intentHasCurrentAuthority(operation)),
      not(matchingQuorumPermit(operation)),
      publicationByOperation.get(operation) == None,
    })

  def witnessTwoVisibleVersionsHaveOneDenseHistory: bool = all {
    visibleVersion == MaxVersion,
    Versions.forall(version =>
      publishedOperationsForVersion(version).size() == 1
    ),
    not(canonicalConflictExists),
  }

  def witnessOldPrimaryEvidenceRejectedAfterQuorumReconfiguration: bool =
    witnessStaleAssignmentAcknowledgementsCannotPublish

  def witnessQuorumSideSuccessorPublishesAfterReconfiguration: bool =
    Operations.exists(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.coordinator == N2,
            record.epoch == 3,
            record.assignmentRevision == 3,
            record.metadataAssignment == Nodes,
            record.authorization == FullShardAcknowledgements,
            retiringPreparedOperations.subseteq(carriedRetiringOperations),
          }
        | None => false
      }
    )

  /// Both components attempted the same base/target publication. The current
  /// N2 operation is visible; the N1 operation is recorded as a stale conflict
  /// rejection even though it retained valid evidence for the old assignment.
  def witnessCompetingOldAndNewAttemptsChooseOnlyNewAuthority: bool =
    Operations.exists(oldOperation =>
      Operations.exists(newOperation => all {
        oldOperation != newOperation,
        staleRejectedOperations.contains(oldOperation),
        conflictRejectedOperations.contains(oldOperation),
        publicationAttempts.contains(oldOperation),
        publicationAttempts.contains(newOperation),
        fullHistoricalShardAcknowledgements(oldOperation),
        publicationByOperation.get(oldOperation) == None,
        match publicationByOperation.get(newOperation) {
          | Some(record) => all {
              record.coordinator == N2,
              record.epoch == 3,
              record.assignmentRevision == 3,
              record.baseVersion == intent(oldOperation).baseVersion,
              record.targetVersion == intent(oldOperation).targetVersion,
              record.authorization == FullShardAcknowledgements,
            }
          | None => false
        },
        not(canonicalConflictExists),
      }))

  /// The voter quorum committed a larger replica set, but epoch 1 remains the
  /// active DFS authority until N1 reconnects and transfers exact state.
  def witnessSamePrimaryMembershipCommitDoesNotActivateNewAuthority: bool =
    Operations.exists(operation => all {
      authorityHandoffPending,
      not(retiringPrimaryFenceAcknowledged),
      currentEpoch == 2,
      currentPrimary == N1,
      metadataReplicas == Nodes,
      authorityEpoch == 1,
      authorityMetadataReplicas == Set(N1),
      retiringPreparedOperations.contains(operation),
      fullHistoricalShardAcknowledgements(operation),
      publicationByOperation.get(operation) == None,
    })

  /// Because activation is still pending, every retiring replica is reachable
  /// from N1 and the old full-shard operation may publish without a KV round.
  def witnessRetiringShardPublishesBeforeMembershipActivation: bool =
    Operations.exists(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            authorityHandoffPending,
            not(retiringPrimaryFenceAcknowledged),
            currentEpoch == 2,
            record.epoch == 1,
            record.assignmentRevision == 1,
            record.metadataAssignment == Set(N1),
            record.authorization == FullShardAcknowledgements,
            retiringPreparedOperations.contains(operation),
          }
        | None => false
      }
    )

  def witnessMembershipActivationCarriesRetiringFullShardWrite: bool =
    all {
      currentEpoch >= 2,
      not(authorityHandoffPending),
      retiringPreparedOperations.size() > 0,
      retiringPreparedOperations.subseteq(carriedRetiringOperations),
      exactStateWatermarkByReplica.get(N2)
        >= requiredHandoffOperationFloor,
      exactStateWatermarkByReplica.get(N2)
        >= retiringFenceOperationFloor,
    }

  /// N1 may append another exact hidden prepare after the voter quorum commits
  /// pending epoch 2 but before N1 observes it. The retiring-primary fence must
  /// therefore advance beyond the begin-time floor and activation must carry
  /// that specific late operation to the desired assignment.
  def witnessPostCommitRetiringWriteIsCapturedAndCarried: bool = all {
    currentEpoch >= 2,
    not(authorityHandoffPending),
    retiringFenceOperationFloor > requiredHandoffOperationFloor,
    Operations.exists(operation => all {
      operation > requiredHandoffOperationFloor,
      operation <= retiringFenceOperationFloor,
      retiringPreparedOperations.contains(operation),
      carriedRetiringOperations.contains(operation),
    }),
    metadataReplicas.forall(replica =>
      exactStateWatermarkByReplica.get(replica)
        >= retiringFenceOperationFloor
    ),
  }

  def witnessDirectPrimaryChangeIsRejected: bool =
    directPrimaryChangeRejected

  def witnessProofCarryingReplicaPromotion: bool = all {
    currentEpoch == 3,
    currentPrimary == N2,
    metadataReplicas.contains(N2),
    exactStateWatermarkByReplica.get(N2)
      >= requiredHandoffOperationFloor,
    exactStateWatermarkByReplica.get(N2)
      >= retiringFenceOperationFloor,
    retiringPreparedOperations.subseteq(carriedRetiringOperations),
  }

  def witnessNewAssignmentPublicationWaitsForRetiringStateCarry: bool =
    Operations.exists(operation =>
      match publicationByOperation.get(operation) {
        | Some(record) => all {
            record.epoch >= 2,
            retiringPreparedOperations.size() > 0,
            retiringPreparedOperations.subseteq(carriedRetiringOperations),
            exactStateWatermarkByReplica.get(record.coordinator)
              >= requiredHandoffOperationFloor,
            exactStateWatermarkByReplica.get(record.coordinator)
              >= retiringFenceOperationFloor,
          }
        | None => false
      }
    )

  def safety: bool = all {
    currentControlPlaneFactsAreRecorded,
    publicationIndexMatchesOperationRecords,
    visibleHistoryIsDenseAndUnique,
    committedVersionsAdvanceExactlyOnceFromTheirBase,
    everyCommitHasWExactContentReceipts,
    everyCommitUsesARecordedPrimaryEpochAndAssignment,
    everyCommitHasExactlyOneValidAuthorityPath,
    minorityWithOnlyWRemainsUnpublished,
    staleAuthorityEvidenceNeverPublishes,
    oldPrimaryEvidenceIsFencedAfterQuorumReconfiguration,
    pendingStaleAcknowledgementsDoNotBecomeFresh,
    ordinaryFullShardPathDoesNotCarryAQuorumPermit,
    noCanonicalConflicts,
    competingPublicationIsRejectedInsteadOfRecorded,
    committedAndActivatedAuthorityStaySeparatedDuringHandoff,
    genericMembershipChangeRetainsPrimaryAndBumpsEpoch,
    retiringPrimaryFenceAcknowledgesExactPostCommitState,
    proofCarryingPromotionRequiresCurrentExactReplica,
    newAssignmentCannotPublishBeforeRetiringStateIsCarried,
    retiringShardStateIsCarriedAtActivation,
    stateRemainsBounded,
  }
}
