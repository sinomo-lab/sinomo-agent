/// DFS canonical-operation reuse of one immutable authority/transport view.
///
/// Evidence class: target hardening for removing repeated full cluster-KV
/// snapshot loads and repeated local-daemon discovery from one DFS operation.
/// Each bounded attempt acquires exactly one committed DFS-authority projection
/// and one authenticated local-daemon handle. Routing, metadata prepare, and
/// publication reuse that immutable context without another discovery or
/// control-plane load. A receiver-side exact revision/epoch check fences a
/// stale authority view; a changed daemon generation fences a stale transport
/// handle. Only a fenced attempt may acquire one fresh context and retry the
/// same canonical operation.
///
/// `authorityRevision` is the generation of the exact DFS authority projection
/// (metadata-shard assignments plus active handoff/proof state), not the global
/// cluster-KV revision. An unrelated KV commit therefore does not invalidate a
/// valid operation context. The bounded model permits two primary rotations,
/// exposing primary-name ABA while revision and epoch still fence the old
/// context. It also permits one local-daemon restart and one retry.
///
/// This is deliberately the canonical strong path. There is no local overlay,
/// degraded mutation, or local-first publication in this model. Payload/blob
/// durability, replica quorum evidence, byte-level wire authentication, and
/// multi-shard atomicity remain in their existing focused slices.
module dfs_operation_authority_snapshot_reuse {
  type PrimaryNode = PrimaryOne | PrimaryTwo
  type LocalDaemonIdentity = DaemonOne | DaemonTwo
  type OperationPhase =
    | OperationIdle
    | ContextCaptured
    | OperationRouted
    | MetadataPrepared
    | CanonicalPublished
    | AttemptFenced
  type FenceReason = NoFence | AuthorityFence | DaemonFence
  type OperationTransition =
    | OperationInitialized
    | InitialContextAcquired
    | RetryContextAcquired
    | UnrelatedKvCommitted
    | AuthorityAdvanced
    | LocalDaemonRestarted
    | RoutedUsingContext
    | PreparedUsingContext
    | PublishedUsingContext
    | StaleAuthorityContextFenced
    | StaleDaemonContextFenced
    | OperationStuttered

  type AuthorityView = {
    authorityRevision: int,
    primary: PrimaryNode,
    authorityEpoch: int,
  }

  type DaemonView = {
    generation: int,
    identity: LocalDaemonIdentity,
  }

  type OperationSnapshotReuseState = {
    phase: OperationPhase,
    liveAuthority: AuthorityView,
    liveDaemon: DaemonView,
    unrelatedKvRevision: int,
    capturedAuthority: AuthorityView,
    capturedDaemon: DaemonView,
    routedAuthority: AuthorityView,
    preparedAuthority: AuthorityView,
    publishedAuthority: AuthorityView,
    routedOnAttempt: bool,
    preparedOnAttempt: bool,
    attempts: int,
    authoritySnapshotLoads: int,
    daemonDiscoveryProbes: int,
    snapshotUses: int,
    routeUses: int,
    prepareUses: int,
    publishUses: int,
    acceptedRoutes: int,
    acceptedPrepares: int,
    daemonPeerSends: int,
    visibleCanonicalVersions: int,
    successfulPublications: int,
    authorityChanges: int,
    daemonRestarts: int,
    unrelatedKvChanges: int,
    authorityFenceCount: int,
    daemonFenceCount: int,
    abaAuthorityFenceObserved: bool,
    fenceReason: FenceReason,
    publishedAttempt: int,
    lastTransition: OperationTransition,
  }

  pure val initialAuthority: AuthorityView = {
    authorityRevision: 1,
    primary: PrimaryOne,
    authorityEpoch: 1,
  }

  pure val initialDaemon: DaemonView = {
    generation: 1,
    identity: DaemonOne,
  }

  pure val initialState: OperationSnapshotReuseState = {
    phase: OperationIdle,
    liveAuthority: initialAuthority,
    liveDaemon: initialDaemon,
    unrelatedKvRevision: 1,
    capturedAuthority: initialAuthority,
    capturedDaemon: initialDaemon,
    routedAuthority: initialAuthority,
    preparedAuthority: initialAuthority,
    publishedAuthority: initialAuthority,
    routedOnAttempt: false,
    preparedOnAttempt: false,
    attempts: 0,
    authoritySnapshotLoads: 0,
    daemonDiscoveryProbes: 0,
    snapshotUses: 0,
    routeUses: 0,
    prepareUses: 0,
    publishUses: 0,
    acceptedRoutes: 0,
    acceptedPrepares: 0,
    daemonPeerSends: 0,
    visibleCanonicalVersions: 0,
    successfulPublications: 0,
    authorityChanges: 0,
    daemonRestarts: 0,
    unrelatedKvChanges: 0,
    authorityFenceCount: 0,
    daemonFenceCount: 0,
    abaAuthorityFenceObserved: false,
    fenceReason: NoFence,
    publishedAttempt: 0,
    lastTransition: OperationInitialized,
  }

  var operationSnapshotReuse: OperationSnapshotReuseState

  pure def nextPrimary(primary: PrimaryNode): PrimaryNode =
    match primary {
      | PrimaryOne => PrimaryTwo
      | PrimaryTwo => PrimaryOne
    }

  pure def nextDaemon(identity: LocalDaemonIdentity): LocalDaemonIdentity =
    match identity {
      | DaemonOne => DaemonTwo
      | DaemonTwo => DaemonOne
    }

  pure def capturedAuthorityIsCurrent: bool =
    operationSnapshotReuse.capturedAuthority
      == operationSnapshotReuse.liveAuthority

  pure def capturedDaemonIsCurrent: bool =
    operationSnapshotReuse.capturedDaemon == operationSnapshotReuse.liveDaemon

  pure def capturedContextIsCurrent: bool = all {
    capturedAuthorityIsCurrent,
    capturedDaemonIsCurrent,
  }

  action init = operationSnapshotReuse' = initialState

  /// One attempt acquires one exact committed DFS projection and discovers the
  /// authenticated local daemon once. A retry is available only after a typed
  /// fence and retains the same logical operation/payload by construction.
  action acquireImmutableOperationContext = all {
    any {
      operationSnapshotReuse.phase == OperationIdle,
      operationSnapshotReuse.phase == AttemptFenced,
    },
    operationSnapshotReuse.attempts < 2,
    operationSnapshotReuse.visibleCanonicalVersions == 0,
    operationSnapshotReuse' = operationSnapshotReuse
      .with("phase", ContextCaptured)
      .with("capturedAuthority", operationSnapshotReuse.liveAuthority)
      .with("capturedDaemon", operationSnapshotReuse.liveDaemon)
      .with("routedAuthority", operationSnapshotReuse.liveAuthority)
      .with("preparedAuthority", operationSnapshotReuse.liveAuthority)
      .with("routedOnAttempt", false)
      .with("preparedOnAttempt", false)
      .with("attempts", operationSnapshotReuse.attempts + 1)
      .with(
        "authoritySnapshotLoads",
        operationSnapshotReuse.authoritySnapshotLoads + 1
      )
      .with(
        "daemonDiscoveryProbes",
        operationSnapshotReuse.daemonDiscoveryProbes + 1
      )
      .with("fenceReason", NoFence)
      .with(
        "lastTransition",
        if (operationSnapshotReuse.phase == OperationIdle) {
          InitialContextAcquired
        } else {
          RetryContextAcquired
        }
      ),
  }

  /// A non-DFS KV commit advances the global revision but not the exact DFS
  /// authority projection generation. It must not force an operation refresh.
  action commitUnrelatedClusterKv = all {
    operationSnapshotReuse.phase != CanonicalPublished,
    operationSnapshotReuse.unrelatedKvChanges == 0,
    operationSnapshotReuse' = operationSnapshotReuse
      .with(
        "unrelatedKvRevision",
        operationSnapshotReuse.unrelatedKvRevision + 1
      )
      .with("unrelatedKvChanges", 1)
      .with("lastTransition", UnrelatedKvCommitted),
  }

  /// The second rotation returns to PrimaryOne, deliberately creating an ABA
  /// name while preserving a strictly newer projection revision and epoch.
  action advanceCommittedAuthority = all {
    operationSnapshotReuse.phase != CanonicalPublished,
    operationSnapshotReuse.authorityChanges < 2,
    operationSnapshotReuse' = operationSnapshotReuse
      .with("liveAuthority", {
        authorityRevision:
          operationSnapshotReuse.liveAuthority.authorityRevision + 1,
        primary: nextPrimary(operationSnapshotReuse.liveAuthority.primary),
        authorityEpoch: operationSnapshotReuse.liveAuthority.authorityEpoch + 1,
      })
      .with(
        "authorityChanges",
        operationSnapshotReuse.authorityChanges + 1
      )
      .with("lastTransition", AuthorityAdvanced),
  }

  action restartLocalDaemon = all {
    operationSnapshotReuse.phase != CanonicalPublished,
    operationSnapshotReuse.daemonRestarts == 0,
    operationSnapshotReuse' = operationSnapshotReuse
      .with("liveDaemon", {
        generation: operationSnapshotReuse.liveDaemon.generation + 1,
        identity: nextDaemon(operationSnapshotReuse.liveDaemon.identity),
      })
      .with("daemonRestarts", 1)
      .with("lastTransition", LocalDaemonRestarted),
  }

  /// Routing uses the captured primary and cached daemon. A stale daemon is
  /// fenced locally before a peer send; stale authority is rejected by the
  /// exact authority check at metadata prepare.
  action routeUsingCapturedContext = all {
    operationSnapshotReuse.phase == ContextCaptured,
    if (capturedDaemonIsCurrent) {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", OperationRouted)
        .with("routedAuthority", operationSnapshotReuse.capturedAuthority)
        .with("routedOnAttempt", true)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("routeUses", operationSnapshotReuse.routeUses + 1)
        .with("acceptedRoutes", operationSnapshotReuse.acceptedRoutes + 1)
        .with("daemonPeerSends", operationSnapshotReuse.daemonPeerSends + 1)
        .with("lastTransition", RoutedUsingContext)
    } else {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", AttemptFenced)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("routeUses", operationSnapshotReuse.routeUses + 1)
        .with("daemonFenceCount", operationSnapshotReuse.daemonFenceCount + 1)
        .with("fenceReason", DaemonFence)
        .with("lastTransition", StaleDaemonContextFenced)
    },
  }

  /// The authority checks revision, primary, and epoch as one exact record.
  /// There is no full snapshot refresh between route and prepare.
  action prepareUsingCapturedContext = all {
    operationSnapshotReuse.phase == OperationRouted,
    operationSnapshotReuse.routedOnAttempt,
    if (not(capturedDaemonIsCurrent)) {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", AttemptFenced)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("prepareUses", operationSnapshotReuse.prepareUses + 1)
        .with("daemonFenceCount", operationSnapshotReuse.daemonFenceCount + 1)
        .with("fenceReason", DaemonFence)
        .with("lastTransition", StaleDaemonContextFenced)
    } else if (not(capturedAuthorityIsCurrent)) {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", AttemptFenced)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("prepareUses", operationSnapshotReuse.prepareUses + 1)
        .with(
          "authorityFenceCount",
          operationSnapshotReuse.authorityFenceCount + 1
        )
        .with(
          "abaAuthorityFenceObserved",
          operationSnapshotReuse.abaAuthorityFenceObserved
            or operationSnapshotReuse.capturedAuthority.primary
              == operationSnapshotReuse.liveAuthority.primary
        )
        .with("fenceReason", AuthorityFence)
        .with("lastTransition", StaleAuthorityContextFenced)
    } else {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", MetadataPrepared)
        .with("preparedAuthority", operationSnapshotReuse.capturedAuthority)
        .with("preparedOnAttempt", true)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("prepareUses", operationSnapshotReuse.prepareUses + 1)
        .with("acceptedPrepares", operationSnapshotReuse.acceptedPrepares + 1)
        .with("daemonPeerSends", operationSnapshotReuse.daemonPeerSends + 1)
        .with("lastTransition", PreparedUsingContext)
    },
  }

  /// Publication rechecks the same exact context. Authority or daemon changes
  /// after prepare cannot turn stale evidence into a visible canonical write.
  action publishUsingCapturedContext = all {
    operationSnapshotReuse.phase == MetadataPrepared,
    operationSnapshotReuse.routedOnAttempt,
    operationSnapshotReuse.preparedOnAttempt,
    if (not(capturedDaemonIsCurrent)) {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", AttemptFenced)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("publishUses", operationSnapshotReuse.publishUses + 1)
        .with("daemonFenceCount", operationSnapshotReuse.daemonFenceCount + 1)
        .with("fenceReason", DaemonFence)
        .with("lastTransition", StaleDaemonContextFenced)
    } else if (not(capturedAuthorityIsCurrent)) {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", AttemptFenced)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("publishUses", operationSnapshotReuse.publishUses + 1)
        .with(
          "authorityFenceCount",
          operationSnapshotReuse.authorityFenceCount + 1
        )
        .with(
          "abaAuthorityFenceObserved",
          operationSnapshotReuse.abaAuthorityFenceObserved
            or operationSnapshotReuse.capturedAuthority.primary
              == operationSnapshotReuse.liveAuthority.primary
        )
        .with("fenceReason", AuthorityFence)
        .with("lastTransition", StaleAuthorityContextFenced)
    } else {
      operationSnapshotReuse' = operationSnapshotReuse
        .with("phase", CanonicalPublished)
        .with("publishedAuthority", operationSnapshotReuse.capturedAuthority)
        .with("snapshotUses", operationSnapshotReuse.snapshotUses + 1)
        .with("publishUses", operationSnapshotReuse.publishUses + 1)
        .with("daemonPeerSends", operationSnapshotReuse.daemonPeerSends + 1)
        .with("visibleCanonicalVersions", 1)
        .with("successfulPublications", 1)
        .with("publishedAttempt", operationSnapshotReuse.attempts)
        .with("lastTransition", PublishedUsingContext)
    },
  }

  action stutter = operationSnapshotReuse' = operationSnapshotReuse
    .with("lastTransition", OperationStuttered)

  action step = any {
    acquireImmutableOperationContext,
    commitUnrelatedClusterKv,
    advanceCommittedAuthority,
    restartLocalDaemon,
    routeUsingCapturedContext,
    prepareUsingCapturedContext,
    publishUsingCapturedContext,
    stutter,
  }

  /// Coverage controllers invoke only ordinary protocol actions. Symbolic
  /// verification continues to explore the unrestricted `step` relation.
  action successfulReuseCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else if (operationSnapshotReuse.phase == MetadataPrepared) {
      publishUsingCapturedContext
    } else {
      stutter
    }

  action unrelatedKvCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured
      and operationSnapshotReuse.unrelatedKvChanges == 0) {
      commitUnrelatedClusterKv
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else if (operationSnapshotReuse.phase == MetadataPrepared) {
      publishUsingCapturedContext
    } else {
      stutter
    }

  action authorityBeforePrepareCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == OperationRouted
      and operationSnapshotReuse.authorityChanges == 0) {
      advanceCommittedAuthority
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else {
      stutter
    }

  action authorityAfterPrepareCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else if (operationSnapshotReuse.phase == MetadataPrepared
      and operationSnapshotReuse.authorityChanges == 0) {
      advanceCommittedAuthority
    } else if (operationSnapshotReuse.phase == MetadataPrepared) {
      publishUsingCapturedContext
    } else {
      stutter
    }

  action authorityRetryCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == OperationRouted
      and operationSnapshotReuse.attempts == 1
      and operationSnapshotReuse.authorityFenceCount == 0
      and operationSnapshotReuse.authorityChanges == 0) {
      advanceCommittedAuthority
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else if (operationSnapshotReuse.phase == AttemptFenced
      and operationSnapshotReuse.attempts < 2) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == MetadataPrepared) {
      publishUsingCapturedContext
    } else {
      stutter
    }

  action daemonFenceCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured
      and operationSnapshotReuse.daemonRestarts == 0) {
      restartLocalDaemon
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else {
      stutter
    }

  action daemonRetryCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured
      and operationSnapshotReuse.attempts == 1
      and operationSnapshotReuse.daemonFenceCount == 0
      and operationSnapshotReuse.daemonRestarts == 0) {
      restartLocalDaemon
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == AttemptFenced
      and operationSnapshotReuse.attempts < 2) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else if (operationSnapshotReuse.phase == MetadataPrepared) {
      publishUsingCapturedContext
    } else {
      stutter
    }

  action primaryAbaCoverageStep =
    if (operationSnapshotReuse.phase == OperationIdle) {
      acquireImmutableOperationContext
    } else if (operationSnapshotReuse.phase == ContextCaptured
      and operationSnapshotReuse.authorityChanges < 2) {
      advanceCommittedAuthority
    } else if (operationSnapshotReuse.phase == ContextCaptured) {
      routeUsingCapturedContext
    } else if (operationSnapshotReuse.phase == OperationRouted) {
      prepareUsingCapturedContext
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    successfulReuseCoverageStep,
    unrelatedKvCoverageStep,
    authorityBeforePrepareCoverageStep,
    authorityAfterPrepareCoverageStep,
    authorityRetryCoverageStep,
    daemonFenceCoverageStep,
    daemonRetryCoverageStep,
    primaryAbaCoverageStep,
  }
}
