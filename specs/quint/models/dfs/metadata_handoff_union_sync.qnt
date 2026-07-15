/// Exact-union metadata membership-handoff synchronization.
///
/// Evidence class: current-behavior bug counterexample plus target hardening.
/// A membership handoff freezes the retiring primary and must carry the exact
/// prefix to the union of the retiring and desired shard assignments. In this
/// bounded instance N1 is the retained primary, N2 is the retiring replica,
/// and N3 is the desired replacement replica.
///
/// The old path reused the ordinary POSIX quorum fallback. If N3 answered
/// first, `{N1, N3}` was already a quorum, but publishing that set as a
/// retiring-epoch watermark rejected N3 as unconfigured. Returning from the
/// attempt cleared its fresh acknowledgement set, so a later N2 response could
/// not complete that same exact-union proof. The hardened path never invokes
/// the ordinary fallback for handoff synchronization: a partial union remains
/// pending and retry-enabled, retains fresh acknowledgements for the bounded
/// attempt, and activates only after all of `RequiredUnion` acknowledge.
///
/// A removal-only handoff exposes why membership equality is not a sufficient
/// fallback guard. Its retiring assignment is `{N1,N2,N3}` and its desired
/// assignment is `{N1,N2}`, so the required union equals the active retiring
/// assignment even though the sync still has membership-handoff purpose. At an
/// ordinary `{N1,N2}` quorum, ordinary metadata sync may use fallback, while
/// membership-handoff sync must retain the partial proof and wait for N3's
/// exact-prefix acknowledgement before activation.
///
/// This slice abstracts prefix transfer and exact fingerprint validation into
/// each accepted fresh acknowledgement. It does not weaken the existing
/// retiring-primary write fence and does not claim unbounded transport
/// liveness; the final-ack transition is an explicit reachability witness.
module dfs_metadata_handoff_union_sync {
  import common.* from "../../lib/common"

  type UnionSyncTransition =
    | Initialized
    | FreshAcknowledgementRecorded
    | OldFallbackRejectedUnconfiguredNode
    | OldFallbackReturnedPartialCertificate
    | OldExactUnionActivated
    | FixedExactUnionActivated
    | RemovalFreshAcknowledgementRecorded
    | RemovalFallbackPurposeSeparated
    | RemovalExactUnionActivated
    | TerminalStutter

  type SyncPurpose = OrdinaryMetadataSync | MembershipHandoffSync

  /// Retiring assignment: retained primary N1 plus old replica N2.
  pure val RetiringAssignment: Set[Node] = Set(N1, N2)
  /// Desired assignment: retained primary N1 plus new replica N3.
  pure val DesiredAssignment: Set[Node] = Set(N1, N3)
  pure val RequiredUnion: Set[Node] =
    RetiringAssignment.union(DesiredAssignment)
  pure val LocalPrimaryAcknowledgement: Set[Node] = Set(N1)
  pure val OrdinaryQuorumSize: int = 2
  pure val ActiveAssignment: Set[Node] = RetiringAssignment
  pure val HandoffTargetMembership: Set[Node] = RequiredUnion

  /// Removal-only handoff: N3 is leaving, so the desired assignment is a strict
  /// subset and its union with the retiring assignment equals the active set.
  pure val RemovalRetiringAssignment: Set[Node] = Set(N1, N2, N3)
  pure val RemovalDesiredAssignment: Set[Node] = Set(N1, N2)
  pure val RemovalRequiredUnion: Set[Node] =
    RemovalRetiringAssignment.union(RemovalDesiredAssignment)
  pure val RemovalActiveAssignment: Set[Node] = RemovalRetiringAssignment
  pure val RemovalTargetMembership: Set[Node] = RemovalRequiredUnion

  var oldAcknowledged: Set[Node]
  var fixedAcknowledged: Set[Node]
  var previousOldAcknowledged: Set[Node]
  var previousFixedAcknowledged: Set[Node]
  var oldHandoffPending: bool
  var fixedHandoffPending: bool
  var oldRetryEnabled: bool
  var fixedRetryEnabled: bool
  var oldActivated: bool
  var fixedActivated: bool
  /// Shared fallback-evaluation transitions derive these decisions from both
  /// sync purpose and member sets. The old path did not distinguish handoff
  /// sync from ordinary metadata sync; the fixed path requires ordinary purpose.
  var oldOrdinaryFallbackUsed: bool
  var fixedOrdinaryFallbackUsed: bool
  var fallbackEvaluationObserved: bool
  var removalAcknowledged: Set[Node]
  var previousRemovalAcknowledged: Set[Node]
  var removalFallbackAcknowledged: Set[Node]
  var removalHandoffPending: bool
  var removalRetryEnabled: bool
  var removalActivated: bool
  var removalHandoffFallbackUsed: bool
  var removalOrdinaryFallbackWouldBeUsed: bool
  var removalFallbackEvaluationObserved: bool
  var removalFullShardCertificateObserved: bool
  var lastAcknowledgingNode: Node
  var lastTransition: UnionSyncTransition

  pure def isExactUnion(acknowledged: Set[Node]): bool =
    acknowledged == RequiredUnion

  pure def isPartialOrdinaryQuorum(acknowledged: Set[Node]): bool = all {
    acknowledged.size() >= OrdinaryQuorumSize,
    not(isExactUnion(acknowledged)),
  }

  pure def isExactRemovalUnion(acknowledged: Set[Node]): bool =
    acknowledged == RemovalRequiredUnion

  pure def isPartialRemovalOrdinaryQuorum(
    acknowledged: Set[Node],
  ): bool = all {
    acknowledged.size() >= OrdinaryQuorumSize,
    not(isExactRemovalUnion(acknowledged)),
  }

  pure def retiringWatermarkAccepts(acknowledged: Set[Node]): bool = all {
    acknowledged.contains(N1),
    acknowledged.subseteq(RetiringAssignment),
    acknowledged.size() >= OrdinaryQuorumSize,
  }

  pure def oldOrdinaryFallbackEligible(
    acknowledged: Set[Node],
  ): bool = isPartialOrdinaryQuorum(acknowledged)

  pure def ordinaryFallbackEligibleForPurpose(
    purpose: SyncPurpose,
    targetMembership: Set[Node],
    activeAssignment: Set[Node],
    requiredUnion: Set[Node],
    acknowledged: Set[Node],
  ): bool = all {
    purpose == OrdinaryMetadataSync,
    targetMembership == activeAssignment,
    acknowledged.size() >= OrdinaryQuorumSize,
    acknowledged != requiredUnion,
  }

  action preserveRemovalState = all {
    previousRemovalAcknowledged' = removalAcknowledged,
    removalAcknowledged' = removalAcknowledged,
    removalFallbackAcknowledged' = removalFallbackAcknowledged,
    removalHandoffPending' = removalHandoffPending,
    removalRetryEnabled' = removalRetryEnabled,
    removalActivated' = removalActivated,
    removalHandoffFallbackUsed' = removalHandoffFallbackUsed,
    removalOrdinaryFallbackWouldBeUsed' =
      removalOrdinaryFallbackWouldBeUsed,
    removalFallbackEvaluationObserved' = removalFallbackEvaluationObserved,
    removalFullShardCertificateObserved' =
      removalFullShardCertificateObserved,
  }

  action preserveReplacementState = all {
    previousOldAcknowledged' = oldAcknowledged,
    previousFixedAcknowledged' = fixedAcknowledged,
    oldAcknowledged' = oldAcknowledged,
    fixedAcknowledged' = fixedAcknowledged,
    oldHandoffPending' = oldHandoffPending,
    fixedHandoffPending' = fixedHandoffPending,
    oldRetryEnabled' = oldRetryEnabled,
    fixedRetryEnabled' = fixedRetryEnabled,
    oldActivated' = oldActivated,
    fixedActivated' = fixedActivated,
    oldOrdinaryFallbackUsed' = oldOrdinaryFallbackUsed,
    fixedOrdinaryFallbackUsed' = fixedOrdinaryFallbackUsed,
    fallbackEvaluationObserved' = fallbackEvaluationObserved,
  }

  action init = all {
    oldAcknowledged' = LocalPrimaryAcknowledgement,
    fixedAcknowledged' = LocalPrimaryAcknowledgement,
    previousOldAcknowledged' = LocalPrimaryAcknowledgement,
    previousFixedAcknowledged' = LocalPrimaryAcknowledgement,
    oldHandoffPending' = true,
    fixedHandoffPending' = true,
    oldRetryEnabled' = true,
    fixedRetryEnabled' = true,
    oldActivated' = false,
    fixedActivated' = false,
    oldOrdinaryFallbackUsed' = false,
    fixedOrdinaryFallbackUsed' = false,
    fallbackEvaluationObserved' = false,
    removalAcknowledged' = LocalPrimaryAcknowledgement,
    previousRemovalAcknowledged' = LocalPrimaryAcknowledgement,
    removalFallbackAcknowledged' = LocalPrimaryAcknowledgement,
    removalHandoffPending' = true,
    removalRetryEnabled' = true,
    removalActivated' = false,
    removalHandoffFallbackUsed' = false,
    removalOrdinaryFallbackWouldBeUsed' = false,
    removalFallbackEvaluationObserved' = false,
    removalFullShardCertificateObserved' = false,
    lastAcknowledgingNode' = N1,
    lastTransition' = Initialized,
  }

  /// Deliver one fresh exact-prefix acknowledgement to both the old and fixed
  /// algorithms. A node is delivered once in this bounded transport trace;
  /// only the old attempt may subsequently discard that evidence.
  action recordFreshAcknowledgement(node: Node): bool = all {
    RequiredUnion.contains(node),
    not(fixedAcknowledged.contains(node)),
    fixedHandoffPending,
    previousOldAcknowledged' = oldAcknowledged,
    previousFixedAcknowledged' = fixedAcknowledged,
    oldAcknowledged' =
      if (oldHandoffPending) oldAcknowledged.union(Set(node)) else oldAcknowledged,
    fixedAcknowledged' = fixedAcknowledged.union(Set(node)),
    oldHandoffPending' = oldHandoffPending,
    fixedHandoffPending' = fixedHandoffPending,
    oldRetryEnabled' = oldRetryEnabled,
    fixedRetryEnabled' = fixedRetryEnabled,
    oldActivated' = oldActivated,
    fixedActivated' = fixedActivated,
    oldOrdinaryFallbackUsed' = oldOrdinaryFallbackUsed,
    fixedOrdinaryFallbackUsed' = fixedOrdinaryFallbackUsed,
    fallbackEvaluationObserved' = fallbackEvaluationObserved,
    preserveRemovalState,
    lastAcknowledgingNode' = node,
    lastTransition' = FreshAcknowledgementRecorded,
  }

  /// Counterfactual production bug: the ordinary quorum grace expires before
  /// the exact handoff union responds. The fallback either rejects a desired-
  /// only node under the retiring assignment or returns a certificate that is
  /// still too small for handoff activation. In both cases the invocation
  /// ends and its fresh ACK set resets to the local primary.
  action evaluateOrdinaryFallbackAtPartialUnion = all {
    oldHandoffPending,
    oldRetryEnabled,
    isPartialOrdinaryQuorum(oldAcknowledged),
    previousOldAcknowledged' = oldAcknowledged,
    previousFixedAcknowledged' = fixedAcknowledged,
    oldAcknowledged' = LocalPrimaryAcknowledgement,
    fixedAcknowledged' = fixedAcknowledged,
    oldHandoffPending' = true,
    fixedHandoffPending' = fixedHandoffPending,
    oldRetryEnabled' = true,
    fixedRetryEnabled' = fixedRetryEnabled,
    oldActivated' = false,
    fixedActivated' = fixedActivated,
    oldOrdinaryFallbackUsed' = oldOrdinaryFallbackEligible(oldAcknowledged),
    fixedOrdinaryFallbackUsed' = ordinaryFallbackEligibleForPurpose(
      MembershipHandoffSync,
      HandoffTargetMembership,
      ActiveAssignment,
      RequiredUnion,
      fixedAcknowledged
    ),
    fallbackEvaluationObserved' = true,
    preserveRemovalState,
    lastAcknowledgingNode' = lastAcknowledgingNode,
    lastTransition' =
      if (retiringWatermarkAccepts(oldAcknowledged)) {
        OldFallbackReturnedPartialCertificate
      } else {
        OldFallbackRejectedUnconfiguredNode
      },
  }

  action activateOldExactUnion = all {
    oldHandoffPending,
    oldRetryEnabled,
    isExactUnion(oldAcknowledged),
    previousOldAcknowledged' = oldAcknowledged,
    previousFixedAcknowledged' = fixedAcknowledged,
    oldAcknowledged' = oldAcknowledged,
    fixedAcknowledged' = fixedAcknowledged,
    oldHandoffPending' = false,
    fixedHandoffPending' = fixedHandoffPending,
    oldRetryEnabled' = false,
    fixedRetryEnabled' = fixedRetryEnabled,
    oldActivated' = true,
    fixedActivated' = fixedActivated,
    oldOrdinaryFallbackUsed' = oldOrdinaryFallbackUsed,
    fixedOrdinaryFallbackUsed' = fixedOrdinaryFallbackUsed,
    fallbackEvaluationObserved' = fallbackEvaluationObserved,
    preserveRemovalState,
    lastAcknowledgingNode' = lastAcknowledgingNode,
    lastTransition' = OldExactUnionActivated,
  }

  action activateFixedExactUnion = all {
    fixedHandoffPending,
    fixedRetryEnabled,
    isExactUnion(fixedAcknowledged),
    previousOldAcknowledged' = oldAcknowledged,
    previousFixedAcknowledged' = fixedAcknowledged,
    oldAcknowledged' = oldAcknowledged,
    fixedAcknowledged' = fixedAcknowledged,
    oldHandoffPending' = oldHandoffPending,
    fixedHandoffPending' = false,
    oldRetryEnabled' = oldRetryEnabled,
    fixedRetryEnabled' = false,
    oldActivated' = oldActivated,
    fixedActivated' = true,
    oldOrdinaryFallbackUsed' = oldOrdinaryFallbackUsed,
    fixedOrdinaryFallbackUsed' = fixedOrdinaryFallbackUsed,
    fallbackEvaluationObserved' = fallbackEvaluationObserved,
    preserveRemovalState,
    lastAcknowledgingNode' = lastAcknowledgingNode,
    lastTransition' = FixedExactUnionActivated,
  }

  /// Record one exact-prefix ACK for the removal-only handoff. The desired set
  /// is smaller than the retiring set, but the full retiring union remains the
  /// activation certificate.
  action recordRemovalFreshAcknowledgement(node: Node): bool = all {
    RemovalRequiredUnion.contains(node),
    not(removalAcknowledged.contains(node)),
    removalHandoffPending,
    preserveReplacementState,
    previousRemovalAcknowledged' = removalAcknowledged,
    removalAcknowledged' = removalAcknowledged.union(Set(node)),
    removalFallbackAcknowledged' = removalFallbackAcknowledged,
    removalHandoffPending' = removalHandoffPending,
    removalRetryEnabled' = removalRetryEnabled,
    removalActivated' = removalActivated,
    removalHandoffFallbackUsed' = removalHandoffFallbackUsed,
    removalOrdinaryFallbackWouldBeUsed' =
      removalOrdinaryFallbackWouldBeUsed,
    removalFallbackEvaluationObserved' = removalFallbackEvaluationObserved,
    removalFullShardCertificateObserved' =
      removalFullShardCertificateObserved,
    lastAcknowledgingNode' = node,
    lastTransition' = RemovalFreshAcknowledgementRecorded,
  }

  /// At a two-node quorum the member sets are equal: target union and active
  /// retiring assignment are both `{N1,N2,N3}`. Purpose is therefore the only
  /// discriminator. Ordinary metadata sync would use fallback, while a
  /// membership handoff retains the partial ACK set and keeps retrying.
  action evaluateRemovalFallbackByPurpose = all {
    removalHandoffPending,
    removalRetryEnabled,
    not(removalFallbackEvaluationObserved),
    isPartialRemovalOrdinaryQuorum(removalAcknowledged),
    preserveReplacementState,
    previousRemovalAcknowledged' = removalAcknowledged,
    removalAcknowledged' = removalAcknowledged,
    removalFallbackAcknowledged' = removalAcknowledged,
    removalHandoffPending' = true,
    removalRetryEnabled' = true,
    removalActivated' = false,
    removalHandoffFallbackUsed' = ordinaryFallbackEligibleForPurpose(
      MembershipHandoffSync,
      RemovalTargetMembership,
      RemovalActiveAssignment,
      RemovalRequiredUnion,
      removalAcknowledged
    ),
    removalOrdinaryFallbackWouldBeUsed' = ordinaryFallbackEligibleForPurpose(
      OrdinaryMetadataSync,
      RemovalTargetMembership,
      RemovalActiveAssignment,
      RemovalRequiredUnion,
      removalAcknowledged
    ),
    removalFallbackEvaluationObserved' = true,
    removalFullShardCertificateObserved' = false,
    lastAcknowledgingNode' = lastAcknowledgingNode,
    lastTransition' = RemovalFallbackPurposeSeparated,
  }

  action activateRemovalExactUnion = all {
    removalHandoffPending,
    removalRetryEnabled,
    isExactRemovalUnion(removalAcknowledged),
    preserveReplacementState,
    previousRemovalAcknowledged' = removalAcknowledged,
    removalAcknowledged' = removalAcknowledged,
    removalFallbackAcknowledged' = removalFallbackAcknowledged,
    removalHandoffPending' = false,
    removalRetryEnabled' = false,
    removalActivated' = true,
    removalHandoffFallbackUsed' = removalHandoffFallbackUsed,
    removalOrdinaryFallbackWouldBeUsed' =
      removalOrdinaryFallbackWouldBeUsed,
    removalFallbackEvaluationObserved' = removalFallbackEvaluationObserved,
    removalFullShardCertificateObserved' = true,
    lastAcknowledgingNode' = lastAcknowledgingNode,
    lastTransition' = RemovalExactUnionActivated,
  }

  action terminalStutter = all {
    any { oldActivated, fixedActivated, removalActivated },
    previousOldAcknowledged' = oldAcknowledged,
    previousFixedAcknowledged' = fixedAcknowledged,
    oldAcknowledged' = oldAcknowledged,
    fixedAcknowledged' = fixedAcknowledged,
    oldHandoffPending' = oldHandoffPending,
    fixedHandoffPending' = fixedHandoffPending,
    oldRetryEnabled' = oldRetryEnabled,
    fixedRetryEnabled' = fixedRetryEnabled,
    oldActivated' = oldActivated,
    fixedActivated' = fixedActivated,
    oldOrdinaryFallbackUsed' = oldOrdinaryFallbackUsed,
    fixedOrdinaryFallbackUsed' = fixedOrdinaryFallbackUsed,
    fallbackEvaluationObserved' = fallbackEvaluationObserved,
    previousRemovalAcknowledged' = removalAcknowledged,
    removalAcknowledged' = removalAcknowledged,
    removalFallbackAcknowledged' = removalFallbackAcknowledged,
    removalHandoffPending' = removalHandoffPending,
    removalRetryEnabled' = removalRetryEnabled,
    removalActivated' = removalActivated,
    removalHandoffFallbackUsed' = removalHandoffFallbackUsed,
    removalOrdinaryFallbackWouldBeUsed' =
      removalOrdinaryFallbackWouldBeUsed,
    removalFallbackEvaluationObserved' = removalFallbackEvaluationObserved,
    removalFullShardCertificateObserved' =
      removalFullShardCertificateObserved,
    lastAcknowledgingNode' = lastAcknowledgingNode,
    lastTransition' = TerminalStutter,
  }

  action step = any {
    {
      nondet node = oneOf(RequiredUnion)
      recordFreshAcknowledgement(node)
    },
    evaluateOrdinaryFallbackAtPartialUnion,
    activateOldExactUnion,
    activateFixedExactUnion,
    {
      nondet node = oneOf(RemovalRequiredUnion)
      recordRemovalFreshAcknowledgement(node)
    },
    evaluateRemovalFallbackByPurpose,
    activateRemovalExactUnion,
    terminalStutter,
  }
}
