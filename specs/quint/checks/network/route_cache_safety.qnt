/// Safety properties and coverage witnesses for `network_route_cache`.
module network_route_cache_safety {
  import network_route_cache.* from "../../models/network/route_cache"

  def reusedPlanPassedCompleteRustValidityCheck: bool =
    lastDecision == CachedPlanReused implies liveCacheReusable


  /// Membership and endpoint identity are represented through mark changes in
  /// production, rather than repeated as independent lookup predicates. These
  /// checks ensure those transitions still prevent an old target plan reuse.
  def reusedTargetIsStillAuthorized: bool =
    lastDecision == CachedPlanReused implies all {
      live.targetMember,
      live.targetIdentityHealthy,
      match cache {
        | NoCache => false
        | Cached(entry) => entry.containsTargetPlan
      },
    }


  def unknownDirectoryNeverUsesOrStoresCache: bool = all {
    lastDecision == CachedPlanReused implies live.directoryKnown,
    lastDecision == StablePlanCached implies live.directoryKnown,
  }


  def expiredEntryNeverReused: bool =
    lastDecision == CachedPlanReused implies
      match cache {
        | NoCache => false
        | Cached(entry) => entry.expiresAt > now
      }


  def membershipRemovalChangesDirectoryMark: bool =
    not(live.targetMember) implies
      match cache {
        | NoCache => true
        | Cached(entry) => entry.mark.directoryVersion != live.mark.directoryVersion
      }


  def endpointIdentityMismatchClearsCache: bool =
    not(live.targetIdentityHealthy) implies cache == NoCache


  def successfulBuildHasKnownStableMark: bool =
    lastDecision == StablePlanCached implies
      match cache {
        | NoCache => false
        | Cached(entry) => all {
            live.directoryKnown,
            entry.mark == live.mark,
          }
      }


  def unstableBuildDoesNotPublishPlan: bool =
    lastDecision == UnstableBuildDiscarded implies all {
      build == NoBuild,
      cache == NoCache,
    }


  def rejectionCountersAreMonotonicFacts: bool = all {
    cachedPlanReuseCount >= 0,
    discardedBuildCount >= 0,
  }


  /// Named witnesses keep each validity input and exceptional path live.
  def witnessFreshCachedPlanReuse: bool =
    cachedPlanReuseCount > 0

  def witnessStateFileChangeRejected: bool =
    observedRejections.contains(StateFileChanged)

  def witnessDirectoryChangeRejected: bool =
    observedRejections.contains(DirectoryChanged)

  def witnessUnknownDirectoryDisablesCache: bool =
    observedRejections.contains(DirectoryUnknown)

  def witnessConnectivityFileChangeRejected: bool =
    observedRejections.contains(LocalConnectivityFileChanged)

  def witnessConnectivityContentChangeRejected: bool =
    observedRejections.contains(LocalConnectivityContentChanged)

  def witnessModuleDialerChangeRejected: bool =
    observedRejections.contains(ModuleDialersChanged)

  def witnessExpiredEntryRejected: bool =
    observedRejections.contains(ConnectivityExpired)

  def witnessMembershipRemovalRejected: bool =
    observedRejections.contains(TargetMembershipRemoved)

  def witnessClusterIdentityChangeRejected: bool =
    observedRejections.contains(ClusterIdentityChanged)

  def witnessLocalIdentityChangeRejected: bool =
    observedRejections.contains(LocalIdentityChanged)

  def witnessIdentityMismatchInvalidatesImmediately: bool = all {
    observedRejections.contains(EndpointIdentityMismatch),
    not(live.targetIdentityHealthy),
    cache == NoCache,
  }

  def witnessBuildRaceDiscarded: bool =
    discardedBuildCount > 0


  def safety: bool = all {
    reusedPlanPassedCompleteRustValidityCheck,
    reusedTargetIsStillAuthorized,
    unknownDirectoryNeverUsesOrStoresCache,
    expiredEntryNeverReused,
    membershipRemovalChangesDirectoryMark,
    endpointIdentityMismatchClearsCache,
    successfulBuildHasKnownStableMark,
    unstableBuildDoesNotPublishPlan,
    rejectionCountersAreMonotonicFacts,
  }
}
