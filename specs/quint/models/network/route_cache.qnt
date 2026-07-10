/// Cluster delivery routing-table cache validity.
///
/// This models `ClusterDeliveryRoutingTableCacheMark` and the lookup/build
/// protocol in `crates/sinomo-network/src/route.rs`. A cache mark covers the
/// cluster state file, committed directory version, local-connectivity file
/// and content generation, and the sorted enabled module-dialer snapshot.
/// Table identity and the earliest connectivity expiry are checked separately.
///
/// Mark, identity, and expiry changes are deliberately lazy: the Rust cache
/// may retain an invalid entry until the next lookup, but it must never return
/// a plan from that entry. Endpoint identity mismatch is the exceptional eager
/// path and removes the cache immediately.
///
/// Evidence class: current-behavior refinement of the cache validity and
/// publication predicates. Cache hit rate and eventual rebuilding are omitted.
module network_route_cache {
  type ClusterIdentity = ClusterA | ClusterB
  type LocalIdentity = LocalA | LocalB
  type ModuleDialerSnapshot = BuiltinDialers | BuiltinAndModuleDialer

  type CacheMark = {
    stateFileMark: int,
    directoryVersion: int,
    localConnectivityFileMark: int,
    localConnectivityContentGeneration: int,
    moduleDialerSnapshot: ModuleDialerSnapshot,
  }

  type LiveInputs = {
    mark: CacheMark,
    directoryKnown: bool,
    clusterIdentity: ClusterIdentity,
    localIdentity: LocalIdentity,
    targetMember: bool,
    targetIdentityHealthy: bool,
  }

  type CacheEntry = {
    mark: CacheMark,
    clusterIdentity: ClusterIdentity,
    localIdentity: LocalIdentity,
    containsTargetPlan: bool,
    expiresAt: int,
  }

  type BuildSnapshot = {
    markKnown: bool,
    mark: CacheMark,
    clusterIdentity: ClusterIdentity,
    localIdentity: LocalIdentity,
    containsTargetPlan: bool,
    expiresAt: int,
  }

  type CacheState = NoCache | Cached(CacheEntry)
  type BuildState = NoBuild | Building(BuildSnapshot)

  type InvalidationCause =
    | NoInvalidation
    | StateFileChanged
    | DirectoryChanged
    | DirectoryUnknown
    | LocalConnectivityFileChanged
    | LocalConnectivityContentChanged
    | ModuleDialersChanged
    | ConnectivityExpired
    | TargetMembershipRemoved
    | ClusterIdentityChanged
    | LocalIdentityChanged
    | EndpointIdentityMismatch

  type CacheDecision =
    | NoCacheDecision
    | StablePlanCached
    | PlanBuildStarted
    | UnstableBuildDiscarded
    | CachedPlanReused
    | CacheMissed
    | CacheDisabledByUnknownDirectory
    | StaleMarkEvicted
    | StaleTableIdentityEvicted
    | ExpiredEntryEvicted
    | ValidityInputChanged
    | MembershipRemoved
    | IdentityMismatchInvalidated
    | ClockAdvancedToExpiry
    | CacheStuttered

  pure val CacheLifetime: int = 2

  pure val initialMark: CacheMark = {
    stateFileMark: 0,
    directoryVersion: 0,
    localConnectivityFileMark: 0,
    localConnectivityContentGeneration: 0,
    moduleDialerSnapshot: BuiltinDialers,
  }

  pure val initialInputs: LiveInputs = {
    mark: initialMark,
    directoryKnown: true,
    clusterIdentity: ClusterA,
    localIdentity: LocalA,
    targetMember: true,
    targetIdentityHealthy: true,
  }

  var now: int
  var live: LiveInputs
  var cache: CacheState
  var build: BuildState
  var lastInvalidation: InvalidationCause
  var observedRejections: Set[InvalidationCause]
  var cachedPlanReuseCount: int
  var discardedBuildCount: int
  var lastDecision: CacheDecision

  pure def cacheEntryFor(
    inputs: LiveInputs,
    expiresAt: int
  ): CacheEntry = {
    mark: inputs.mark,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: inputs.localIdentity,
    containsTargetPlan:
      inputs.targetMember and inputs.targetIdentityHealthy,
    expiresAt: expiresAt,
  }

  pure def buildSnapshotFor(
    inputs: LiveInputs,
    expiresAt: int
  ): BuildSnapshot = {
    markKnown: inputs.directoryKnown,
    mark: inputs.mark,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: inputs.localIdentity,
    containsTargetPlan:
      inputs.targetMember and inputs.targetIdentityHealthy,
    expiresAt: expiresAt,
  }

  pure def cacheEntryMarkMatches(
    entry: CacheEntry,
    inputs: LiveInputs
  ): bool =
    inputs.directoryKnown and entry.mark == inputs.mark

  pure def cacheEntryIdentityMatches(
    entry: CacheEntry,
    inputs: LiveInputs
  ): bool = all {
    entry.clusterIdentity == inputs.clusterIdentity,
    entry.localIdentity == inputs.localIdentity,
  }

  pure def cacheEntryExpired(entry: CacheEntry, currentTime: int): bool =
    entry.expiresAt <= currentTime

  /// Mirrors the complete stale predicate in
  /// `cluster_delivery_route_plan_from_cached_routing_table`.
  pure def cacheEntryReusable(
    entry: CacheEntry,
    inputs: LiveInputs,
    currentTime: int
  ): bool = all {
    cacheEntryMarkMatches(entry, inputs),
    cacheEntryIdentityMatches(entry, inputs),
    not(cacheEntryExpired(entry, currentTime)),
  }

  def liveCacheReusable: bool =
    match cache {
      | NoCache => false
      | Cached(entry) => cacheEntryReusable(entry, live, now)
    }

  pure def rememberRejection(
    causes: Set[InvalidationCause],
    cause: InvalidationCause
  ): Set[InvalidationCause] =
    if (cause == NoInvalidation) {
      causes
    } else {
      causes.union(Set(cause))
    }

  pure def toggleModuleSnapshot(
    snapshot: ModuleDialerSnapshot
  ): ModuleDialerSnapshot =
    match snapshot {
      | BuiltinDialers => BuiltinAndModuleDialer
      | BuiltinAndModuleDialer => BuiltinDialers
    }

  pure def toggleClusterIdentity(identity: ClusterIdentity): ClusterIdentity =
    match identity {
      | ClusterA => ClusterB
      | ClusterB => ClusterA
    }

  pure def toggleLocalIdentity(identity: LocalIdentity): LocalIdentity =
    match identity {
      | LocalA => LocalB
      | LocalB => LocalA
    }

  pure def withMark(inputs: LiveInputs, mark: CacheMark): LiveInputs = {
    mark: mark,
    directoryKnown: inputs.directoryKnown,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: inputs.localIdentity,
    targetMember: inputs.targetMember,
    targetIdentityHealthy: inputs.targetIdentityHealthy,
  }

  pure def stateFileChangedInputs(inputs: LiveInputs): LiveInputs =
    withMark(inputs, {
      stateFileMark: inputs.mark.stateFileMark + 1,
      directoryVersion: inputs.mark.directoryVersion,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration,
      moduleDialerSnapshot: inputs.mark.moduleDialerSnapshot,
    })

  pure def directoryChangedInputs(inputs: LiveInputs): LiveInputs =
    withMark(inputs, {
      stateFileMark: inputs.mark.stateFileMark,
      directoryVersion: inputs.mark.directoryVersion + 1,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration,
      moduleDialerSnapshot: inputs.mark.moduleDialerSnapshot,
    })

  pure def connectivityFileChangedInputs(inputs: LiveInputs): LiveInputs =
    withMark(inputs, {
      stateFileMark: inputs.mark.stateFileMark,
      directoryVersion: inputs.mark.directoryVersion,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark + 1,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration,
      moduleDialerSnapshot: inputs.mark.moduleDialerSnapshot,
    })

  pure def connectivityContentChangedInputs(inputs: LiveInputs): LiveInputs =
    withMark(inputs, {
      stateFileMark: inputs.mark.stateFileMark,
      directoryVersion: inputs.mark.directoryVersion,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration + 1,
      moduleDialerSnapshot: inputs.mark.moduleDialerSnapshot,
    })

  pure def moduleDialersChangedInputs(inputs: LiveInputs): LiveInputs =
    withMark(inputs, {
      stateFileMark: inputs.mark.stateFileMark,
      directoryVersion: inputs.mark.directoryVersion,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration,
      moduleDialerSnapshot:
        toggleModuleSnapshot(inputs.mark.moduleDialerSnapshot),
    })

  pure def directoryUnknownInputs(inputs: LiveInputs): LiveInputs = {
    mark: inputs.mark,
    directoryKnown: false,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: inputs.localIdentity,
    targetMember: inputs.targetMember,
    targetIdentityHealthy: inputs.targetIdentityHealthy,
  }

  pure def targetRemovedInputs(inputs: LiveInputs): LiveInputs = {
    mark: {
      stateFileMark: inputs.mark.stateFileMark,
      directoryVersion: inputs.mark.directoryVersion + 1,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration,
      moduleDialerSnapshot: inputs.mark.moduleDialerSnapshot,
    },
    directoryKnown: inputs.directoryKnown,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: inputs.localIdentity,
    targetMember: false,
    targetIdentityHealthy: inputs.targetIdentityHealthy,
  }

  pure def clusterIdentityChangedInputs(inputs: LiveInputs): LiveInputs = {
    mark: inputs.mark,
    directoryKnown: inputs.directoryKnown,
    clusterIdentity: toggleClusterIdentity(inputs.clusterIdentity),
    localIdentity: inputs.localIdentity,
    targetMember: inputs.targetMember,
    targetIdentityHealthy: inputs.targetIdentityHealthy,
  }

  pure def localIdentityChangedInputs(inputs: LiveInputs): LiveInputs = {
    mark: inputs.mark,
    directoryKnown: inputs.directoryKnown,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: toggleLocalIdentity(inputs.localIdentity),
    targetMember: inputs.targetMember,
    targetIdentityHealthy: inputs.targetIdentityHealthy,
  }

  pure def endpointIdentityMismatchInputs(inputs: LiveInputs): LiveInputs = {
    mark: {
      stateFileMark: inputs.mark.stateFileMark,
      directoryVersion: inputs.mark.directoryVersion,
      localConnectivityFileMark: inputs.mark.localConnectivityFileMark,
      localConnectivityContentGeneration:
        inputs.mark.localConnectivityContentGeneration + 1,
      moduleDialerSnapshot: inputs.mark.moduleDialerSnapshot,
    },
    directoryKnown: inputs.directoryKnown,
    clusterIdentity: inputs.clusterIdentity,
    localIdentity: inputs.localIdentity,
    targetMember: inputs.targetMember,
    targetIdentityHealthy: false,
  }

  action init = all {
    now' = 0,
    live' = initialInputs,
    cache' = NoCache,
    build' = NoBuild,
    lastInvalidation' = NoInvalidation,
    observedRejections' = Set(),
    cachedPlanReuseCount' = 0,
    discardedBuildCount' = 0,
    lastDecision' = NoCacheDecision,
  }

  /// The stable case abstracts one complete build whose before/after marks are
  /// equal. The split start/finish actions below exercise the concurrent-input
  /// change case explicitly.
  action cacheStablePlan: bool = all {
    cache == NoCache,
    build == NoBuild,
    live.directoryKnown,
    live.targetMember,
    live.targetIdentityHealthy,
    now' = now,
    live' = live,
    cache' = Cached(cacheEntryFor(live, now + CacheLifetime)),
    build' = build,
    lastInvalidation' = NoInvalidation,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = StablePlanCached,
  }

  action startPlanBuild: bool = all {
    cache == NoCache,
    build == NoBuild,
    live.targetMember,
    live.targetIdentityHealthy,
    now' = now,
    live' = live,
    cache' = cache,
    build' = Building(buildSnapshotFor(live, now + CacheLifetime)),
    lastInvalidation' = NoInvalidation,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = PlanBuildStarted,
  }

  action finishPlanBuild: bool =
    match build {
      | NoBuild => all {
          false,
          now' = now,
          live' = live,
          cache' = cache,
          build' = build,
          lastInvalidation' = lastInvalidation,
          observedRejections' = observedRejections,
          cachedPlanReuseCount' = cachedPlanReuseCount,
          discardedBuildCount' = discardedBuildCount,
          lastDecision' = lastDecision,
        }
      | Building(snapshot) => {
          val markStayedStable = all {
            snapshot.markKnown,
            live.directoryKnown,
            snapshot.mark == live.mark,
          }
          val nextCache =
            if (markStayedStable) {
              Cached({
                mark: snapshot.mark,
                clusterIdentity: snapshot.clusterIdentity,
                localIdentity: snapshot.localIdentity,
                containsTargetPlan: snapshot.containsTargetPlan,
                expiresAt: snapshot.expiresAt,
              })
            } else {
              cache
            }
          val rejectionCause =
            if (not(snapshot.markKnown) or not(live.directoryKnown)) {
              DirectoryUnknown
            } else {
              lastInvalidation
            }
          all {
            now' = now,
            live' = live,
            cache' = nextCache,
            build' = NoBuild,
            lastInvalidation' =
              if (markStayedStable) { NoInvalidation } else { rejectionCause },
            observedRejections' =
              if (markStayedStable) {
                observedRejections
              } else {
                rememberRejection(observedRejections, rejectionCause)
              },
            cachedPlanReuseCount' = cachedPlanReuseCount,
            discardedBuildCount' =
              if (markStayedStable) {
                discardedBuildCount
              } else {
                discardedBuildCount + 1
              },
            lastDecision' =
              if (markStayedStable) {
                StablePlanCached
              } else {
                UnstableBuildDiscarded
              },
          }
        }
    }

  /// Models the lookup order in Rust: unknown mark, mark equality, table
  /// identity, then earliest expiry.
  action lookupCachedPlan: bool =
    match cache {
      | NoCache => all {
          now' = now,
          live' = live,
          cache' = cache,
          build' = build,
          lastInvalidation' = lastInvalidation,
          observedRejections' = observedRejections,
          cachedPlanReuseCount' = cachedPlanReuseCount,
          discardedBuildCount' = discardedBuildCount,
          lastDecision' = CacheMissed,
        }
      | Cached(entry) =>
          if (not(live.directoryKnown)) {
            all {
              now' = now,
              live' = live,
              cache' = cache,
              build' = build,
              lastInvalidation' = DirectoryUnknown,
              observedRejections' =
                rememberRejection(observedRejections, DirectoryUnknown),
              cachedPlanReuseCount' = cachedPlanReuseCount,
              discardedBuildCount' = discardedBuildCount,
              lastDecision' = CacheDisabledByUnknownDirectory,
            }
          } else if (entry.mark != live.mark) {
            all {
              now' = now,
              live' = live,
              cache' = NoCache,
              build' = build,
              lastInvalidation' = lastInvalidation,
              observedRejections' =
                rememberRejection(observedRejections, lastInvalidation),
              cachedPlanReuseCount' = cachedPlanReuseCount,
              discardedBuildCount' = discardedBuildCount,
              lastDecision' = StaleMarkEvicted,
            }
          } else if (not(cacheEntryIdentityMatches(entry, live))) {
            all {
              now' = now,
              live' = live,
              cache' = NoCache,
              build' = build,
              lastInvalidation' = lastInvalidation,
              observedRejections' =
                rememberRejection(observedRejections, lastInvalidation),
              cachedPlanReuseCount' = cachedPlanReuseCount,
              discardedBuildCount' = discardedBuildCount,
              lastDecision' = StaleTableIdentityEvicted,
            }
          } else if (cacheEntryExpired(entry, now)) {
            all {
              now' = now,
              live' = live,
              cache' = NoCache,
              build' = build,
              lastInvalidation' = ConnectivityExpired,
              observedRejections' =
                rememberRejection(observedRejections, ConnectivityExpired),
              cachedPlanReuseCount' = cachedPlanReuseCount,
              discardedBuildCount' = discardedBuildCount,
              lastDecision' = ExpiredEntryEvicted,
            }
          } else {
            all {
              now' = now,
              live' = live,
              cache' = cache,
              build' = build,
              lastInvalidation' = NoInvalidation,
              observedRejections' = observedRejections,
              cachedPlanReuseCount' = cachedPlanReuseCount + 1,
              discardedBuildCount' = discardedBuildCount,
              lastDecision' = CachedPlanReused,
            }
          }
    }

  action changeStateFile: bool = all {
    now' = now,
    live' = stateFileChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = StateFileChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action changeDirectory: bool = all {
    now' = now,
    live' = directoryChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = DirectoryChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action makeDirectoryUnknown: bool = all {
    live.directoryKnown,
    now' = now,
    live' = directoryUnknownInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = DirectoryUnknown,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action changeConnectivityFile: bool = all {
    now' = now,
    live' = connectivityFileChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = LocalConnectivityFileChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action changeConnectivityContent: bool = all {
    now' = now,
    live' = connectivityContentChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = LocalConnectivityContentChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action changeModuleDialers: bool = all {
    now' = now,
    live' = moduleDialersChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = ModuleDialersChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action removeTargetMember: bool = all {
    live.targetMember,
    now' = now,
    live' = targetRemovedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = TargetMembershipRemoved,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = MembershipRemoved,
  }

  action changeClusterIdentity: bool = all {
    now' = now,
    live' = clusterIdentityChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = ClusterIdentityChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  action changeLocalIdentity: bool = all {
    now' = now,
    live' = localIdentityChangedInputs(live),
    cache' = cache,
    build' = build,
    lastInvalidation' = LocalIdentityChanged,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = ValidityInputChanged,
  }

  /// Mirrors `record_cluster_delivery_endpoint_identity_mismatch`: update the
  /// local-connectivity generation and eagerly remove this root's table.
  action recordEndpointIdentityMismatch: bool = all {
    live.targetIdentityHealthy,
    now' = now,
    live' = endpointIdentityMismatchInputs(live),
    cache' = NoCache,
    build' = build,
    lastInvalidation' = EndpointIdentityMismatch,
    observedRejections' =
      rememberRejection(observedRejections, EndpointIdentityMismatch),
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = IdentityMismatchInvalidated,
  }

  action advanceToCachedExpiry: bool =
    match cache {
      | NoCache => all {
          false,
          now' = now,
          live' = live,
          cache' = cache,
          build' = build,
          lastInvalidation' = lastInvalidation,
          observedRejections' = observedRejections,
          cachedPlanReuseCount' = cachedPlanReuseCount,
          discardedBuildCount' = discardedBuildCount,
          lastDecision' = lastDecision,
        }
      | Cached(entry) => all {
          now < entry.expiresAt,
          now' = entry.expiresAt,
          live' = live,
          cache' = cache,
          build' = build,
          lastInvalidation' = ConnectivityExpired,
          observedRejections' = observedRejections,
          cachedPlanReuseCount' = cachedPlanReuseCount,
          discardedBuildCount' = discardedBuildCount,
          lastDecision' = ClockAdvancedToExpiry,
        }
    }

  action stutter: bool = all {
    now' = now,
    live' = live,
    cache' = cache,
    build' = build,
    lastInvalidation' = lastInvalidation,
    observedRejections' = observedRejections,
    cachedPlanReuseCount' = cachedPlanReuseCount,
    discardedBuildCount' = discardedBuildCount,
    lastDecision' = CacheStuttered,
  }

  action step = any {
    cacheStablePlan,
    startPlanBuild,
    finishPlanBuild,
    lookupCachedPlan,
    changeStateFile,
    changeDirectory,
    makeDirectoryUnknown,
    changeConnectivityFile,
    changeConnectivityContent,
    changeModuleDialers,
    removeTargetMember,
    changeClusterIdentity,
    changeLocalIdentity,
    recordEndpointIdentityMismatch,
    advanceToCachedExpiry,
    stutter,
  }
}
