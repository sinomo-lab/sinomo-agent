/// Safety and bounded reachability for one-context DFS operation execution.
module dfs_operation_authority_snapshot_reuse_safety {
  import dfs_operation_authority_snapshot_reuse.* from "../../models/dfs/operation_authority_snapshot_reuse"

  def countersAreBoundedAndConsistent: bool = all {
    operationSnapshotReuse.attempts >= 0,
    operationSnapshotReuse.attempts <= 2,
    operationSnapshotReuse.authoritySnapshotLoads
      == operationSnapshotReuse.attempts,
    operationSnapshotReuse.daemonDiscoveryProbes
      == operationSnapshotReuse.attempts,
    operationSnapshotReuse.snapshotUses
      == operationSnapshotReuse.routeUses
        + operationSnapshotReuse.prepareUses
        + operationSnapshotReuse.publishUses,
    operationSnapshotReuse.routeUses >= operationSnapshotReuse.acceptedRoutes,
    operationSnapshotReuse.routeUses <= operationSnapshotReuse.attempts,
    operationSnapshotReuse.prepareUses >= operationSnapshotReuse.acceptedPrepares,
    operationSnapshotReuse.prepareUses <= operationSnapshotReuse.acceptedRoutes,
    operationSnapshotReuse.publishUses
      <= operationSnapshotReuse.acceptedPrepares,
    operationSnapshotReuse.daemonPeerSends
      == operationSnapshotReuse.acceptedRoutes
        + operationSnapshotReuse.acceptedPrepares
        + operationSnapshotReuse.successfulPublications,
    operationSnapshotReuse.authorityChanges >= 0,
    operationSnapshotReuse.authorityChanges <= 2,
    operationSnapshotReuse.daemonRestarts >= 0,
    operationSnapshotReuse.daemonRestarts <= 1,
    operationSnapshotReuse.unrelatedKvChanges >= 0,
    operationSnapshotReuse.unrelatedKvChanges <= 1,
    operationSnapshotReuse.authorityFenceCount >= 0,
    operationSnapshotReuse.daemonFenceCount >= 0,
    operationSnapshotReuse.authorityFenceCount
      + operationSnapshotReuse.daemonFenceCount
      <= operationSnapshotReuse.attempts,
    operationSnapshotReuse.visibleCanonicalVersions >= 0,
    operationSnapshotReuse.visibleCanonicalVersions <= 1,
    operationSnapshotReuse.successfulPublications
      == operationSnapshotReuse.visibleCanonicalVersions,
  }

  /// The performance contract is structural: the only loads/probes are the
  /// attempt acquisitions. Route, prepare, and publish can consume the same
  /// immutable context three times without increasing either counter.
  def oneAuthorityLoadAndDiscoveryPerAttempt: bool = all {
    operationSnapshotReuse.authoritySnapshotLoads
      == operationSnapshotReuse.attempts,
    operationSnapshotReuse.daemonDiscoveryProbes
      == operationSnapshotReuse.attempts,
    operationSnapshotReuse.snapshotUses
      <= 3 * operationSnapshotReuse.attempts,
  }

  def routedAndPreparedEvidenceComesFromCapturedContext: bool = all {
    operationSnapshotReuse.routedOnAttempt implies
      operationSnapshotReuse.routedAuthority
        == operationSnapshotReuse.capturedAuthority,
    operationSnapshotReuse.preparedOnAttempt implies all {
      operationSnapshotReuse.routedOnAttempt,
      operationSnapshotReuse.preparedAuthority
        == operationSnapshotReuse.capturedAuthority,
    },
    operationSnapshotReuse.phase == OperationRouted implies
      operationSnapshotReuse.routedOnAttempt,
    operationSnapshotReuse.phase == MetadataPrepared implies all {
      operationSnapshotReuse.routedOnAttempt,
      operationSnapshotReuse.preparedOnAttempt,
    },
  }

  def acceptedStageUsedCurrentExactContext: bool = all {
    operationSnapshotReuse.lastTransition == RoutedUsingContext implies all {
      operationSnapshotReuse.routedOnAttempt,
      capturedDaemonIsCurrent,
    },
    operationSnapshotReuse.lastTransition == PreparedUsingContext implies all {
      operationSnapshotReuse.preparedOnAttempt,
      capturedContextIsCurrent,
      operationSnapshotReuse.preparedAuthority
        == operationSnapshotReuse.liveAuthority,
    },
  }

  def staleAuthorityOrDaemonFailsClosed: bool = all {
    operationSnapshotReuse.lastTransition == StaleAuthorityContextFenced
      implies all {
        operationSnapshotReuse.phase == AttemptFenced,
        not(capturedAuthorityIsCurrent),
        capturedDaemonIsCurrent,
        operationSnapshotReuse.fenceReason == AuthorityFence,
        operationSnapshotReuse.visibleCanonicalVersions == 0,
      },
    operationSnapshotReuse.lastTransition == StaleDaemonContextFenced
      implies all {
        operationSnapshotReuse.phase == AttemptFenced,
        not(capturedDaemonIsCurrent),
        operationSnapshotReuse.fenceReason == DaemonFence,
        operationSnapshotReuse.visibleCanonicalVersions == 0,
      },
    operationSnapshotReuse.phase == AttemptFenced implies all {
      operationSnapshotReuse.fenceReason != NoFence,
      operationSnapshotReuse.visibleCanonicalVersions == 0,
    },
  }

  /// No local-first state exists: the single canonical version becomes visible
  /// only after exact route and prepare evidence is revalidated at publication.
  def canonicalVisibilityRequiresCurrentExactContext: bool =
    if (operationSnapshotReuse.phase == CanonicalPublished) {
      all {
        operationSnapshotReuse.visibleCanonicalVersions == 1,
        operationSnapshotReuse.successfulPublications == 1,
        operationSnapshotReuse.routedOnAttempt,
        operationSnapshotReuse.preparedOnAttempt,
        operationSnapshotReuse.routedAuthority
          == operationSnapshotReuse.capturedAuthority,
        operationSnapshotReuse.preparedAuthority
          == operationSnapshotReuse.capturedAuthority,
        operationSnapshotReuse.publishedAuthority
          == operationSnapshotReuse.capturedAuthority,
        operationSnapshotReuse.publishedAuthority
          == operationSnapshotReuse.liveAuthority,
        capturedContextIsCurrent,
        operationSnapshotReuse.publishedAttempt
          == operationSnapshotReuse.attempts,
      }
    } else {
      all {
        operationSnapshotReuse.visibleCanonicalVersions == 0,
        operationSnapshotReuse.successfulPublications == 0,
      }
    }

  def primaryNameAbaDoesNotAuthorizeOldRevision: bool =
    operationSnapshotReuse.abaAuthorityFenceObserved implies all {
      operationSnapshotReuse.authorityFenceCount > 0,
      operationSnapshotReuse.authorityChanges == 2,
    }

  def witnessSingleSnapshotSuccessfulPublish: bool = all {
    operationSnapshotReuse.phase == CanonicalPublished,
    operationSnapshotReuse.attempts == 1,
    operationSnapshotReuse.authoritySnapshotLoads == 1,
    operationSnapshotReuse.daemonDiscoveryProbes == 1,
    operationSnapshotReuse.snapshotUses == 3,
    operationSnapshotReuse.routeUses == 1,
    operationSnapshotReuse.prepareUses == 1,
    operationSnapshotReuse.publishUses == 1,
  }

  def witnessCachedDaemonReusedAcrossAllPeerSends: bool = all {
    operationSnapshotReuse.phase == CanonicalPublished,
    operationSnapshotReuse.attempts == 1,
    operationSnapshotReuse.daemonDiscoveryProbes == 1,
    operationSnapshotReuse.daemonPeerSends == 3,
  }

  def witnessUnrelatedKvCommitDoesNotRefreshContext: bool = all {
    operationSnapshotReuse.phase == CanonicalPublished,
    operationSnapshotReuse.unrelatedKvChanges == 1,
    operationSnapshotReuse.authorityChanges == 0,
    operationSnapshotReuse.attempts == 1,
    operationSnapshotReuse.authoritySnapshotLoads == 1,
    operationSnapshotReuse.daemonDiscoveryProbes == 1,
  }

  def witnessAuthorityChangeBeforePrepareFenced: bool = all {
    operationSnapshotReuse.phase == AttemptFenced,
    operationSnapshotReuse.fenceReason == AuthorityFence,
    operationSnapshotReuse.authorityFenceCount == 1,
    operationSnapshotReuse.acceptedPrepares == 0,
    operationSnapshotReuse.publishUses == 0,
    operationSnapshotReuse.visibleCanonicalVersions == 0,
  }

  def witnessAuthorityChangeAfterPrepareFenced: bool = all {
    operationSnapshotReuse.phase == AttemptFenced,
    operationSnapshotReuse.fenceReason == AuthorityFence,
    operationSnapshotReuse.authorityFenceCount == 1,
    operationSnapshotReuse.acceptedPrepares == 1,
    operationSnapshotReuse.publishUses == 1,
    operationSnapshotReuse.visibleCanonicalVersions == 0,
  }

  def witnessAuthorityFenceRefreshesOnceAndPublishes: bool = all {
    operationSnapshotReuse.phase == CanonicalPublished,
    operationSnapshotReuse.authorityFenceCount == 1,
    operationSnapshotReuse.attempts == 2,
    operationSnapshotReuse.authoritySnapshotLoads == 2,
    operationSnapshotReuse.daemonDiscoveryProbes == 2,
    operationSnapshotReuse.publishedAuthority
      == operationSnapshotReuse.liveAuthority,
  }

  def witnessDaemonRestartFencesCachedHandle: bool = all {
    operationSnapshotReuse.phase == AttemptFenced,
    operationSnapshotReuse.fenceReason == DaemonFence,
    operationSnapshotReuse.daemonRestarts == 1,
    operationSnapshotReuse.daemonFenceCount == 1,
    operationSnapshotReuse.acceptedRoutes == 0,
    operationSnapshotReuse.visibleCanonicalVersions == 0,
  }

  def witnessDaemonFenceRefreshesOnceAndPublishes: bool = all {
    operationSnapshotReuse.phase == CanonicalPublished,
    operationSnapshotReuse.daemonFenceCount == 1,
    operationSnapshotReuse.attempts == 2,
    operationSnapshotReuse.authoritySnapshotLoads == 2,
    operationSnapshotReuse.daemonDiscoveryProbes == 2,
    operationSnapshotReuse.capturedDaemon == operationSnapshotReuse.liveDaemon,
  }

  def witnessPrimaryNameAbaStillFencedByRevisionAndEpoch: bool = all {
    operationSnapshotReuse.phase == AttemptFenced,
    operationSnapshotReuse.fenceReason == AuthorityFence,
    operationSnapshotReuse.authorityChanges == 2,
    operationSnapshotReuse.capturedAuthority.primary
      == operationSnapshotReuse.liveAuthority.primary,
    operationSnapshotReuse.capturedAuthority.authorityRevision
      < operationSnapshotReuse.liveAuthority.authorityRevision,
    operationSnapshotReuse.capturedAuthority.authorityEpoch
      < operationSnapshotReuse.liveAuthority.authorityEpoch,
    operationSnapshotReuse.abaAuthorityFenceObserved,
  }

  def safety: bool = all {
    countersAreBoundedAndConsistent,
    oneAuthorityLoadAndDiscoveryPerAttempt,
    routedAndPreparedEvidenceComesFromCapturedContext,
    acceptedStageUsedCurrentExactContext,
    staleAuthorityOrDaemonFailsClosed,
    canonicalVisibilityRequiresCurrentExactContext,
    primaryNameAbaDoesNotAuthorizeOldRevision,
  }
}
