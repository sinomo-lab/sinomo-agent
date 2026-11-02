/// DFS metadata journal and snapshot source validation.
///
/// This module models source validation before a node applies metadata journal
/// batches or compact snapshot installs, through the standalone replica
/// processor or the service. Only a shard's configured replicas ingest its
/// metadata; `localConfiguredReplica` includes a replica that a pending
/// membership handoff is adding.
module dfs_metadata_journal_source {
  import common.* from "../../lib/common"

  type SourceSurface =
    | ReplicationJournal
    | ReplicationSnapshot
    | ServiceJournal
    | ServiceSnapshot

  type ValidationCase = {
    surface: SourceSurface,
    localNode: Node,
    transportSource: Node,
    claimedSource: Option[Node],
    primary: Node,
    hasContext: bool,
    hasControlPlane: bool,
    batchHasFileSystem: bool,
    shardConfigured: bool,
    shardMatches: bool,
    localConfiguredReplica: bool,
  }

  pure val SourceSurfaces: Set[SourceSurface] =
    Set(ReplicationJournal, ReplicationSnapshot, ServiceJournal, ServiceSnapshot)

  pure val Bools: Set[bool] = Set(true, false)

  pure val SourceClaims: Set[Option[Node]] =
    Set(None, Some(N1), Some(N2), Some(N3))

  var accepted: Set[ValidationCase]
  var rejected: Set[ValidationCase]

  pure def claimedSourceMatchesTransport(case: ValidationCase): bool =
    match case.claimedSource {
      | Some(source) => source == case.transportSource
      | None => true
    }

  pure def claimedSourceIsPrimary(case: ValidationCase): bool =
    case.claimedSource == Some(case.primary)

  pure def transportSourceIsPrimary(case: ValidationCase): bool =
    case.transportSource == case.primary

  /// `validate_dfs_metadata_journal_batch_source`: no context means this helper
  /// cannot validate topology and returns no rejection. With context, missing
  /// source id, wrong shard, non-primary source, primary self-apply, and
  /// non-replica local nodes are rejected.
  pure def canAcceptReplicationJournal(case: ValidationCase): bool =
    not(case.hasContext)
      or all {
        case.shardMatches,
        claimedSourceIsPrimary(case),
        case.localNode != case.primary,
        case.localConfiguredReplica,
      }

  /// `validate_dfs_metadata_snapshot_install_source`: snapshot install has no
  /// no-context escape. It requires current primary source and configured local
  /// replica status.
  pure def canAcceptReplicationSnapshot(case: ValidationCase): bool =
    all {
      case.hasContext,
      case.shardMatches,
      claimedSourceIsPrimary(case),
      case.localNode != case.primary,
      case.localConfiguredReplica,
    }

  /// `NetworkedDfsService::validate_metadata_journal_batch_source`: a claimed
  /// source id, when present, must match the transport source. With control
  /// plane and filesystem evidence, the configured shard primary must be the
  /// transport source, the primary must not apply its own replica batch, and
  /// the local node must be a configured replica.
  pure def canAcceptServiceJournal(case: ValidationCase): bool =
    all {
      claimedSourceMatchesTransport(case),
      any {
        not(case.hasControlPlane),
        not(case.batchHasFileSystem),
        all {
          case.shardConfigured,
          transportSourceIsPrimary(case),
          case.localNode != case.primary,
          case.localConfiguredReplica,
        },
      },
    }

  /// `NetworkedDfsService::handle_metadata_snapshot_install`: snapshot install
  /// always carries a payload source, which must match transport source. With a
  /// control-plane shard config, that source must be the current primary,
  /// primary self-install is rejected, and the local node must be a configured
  /// replica.
  pure def canAcceptServiceSnapshot(case: ValidationCase): bool =
    all {
      claimedSourceMatchesTransport(case),
      case.claimedSource != None,
      any {
        not(case.hasControlPlane),
        all {
          case.shardConfigured,
          transportSourceIsPrimary(case),
          case.localNode != case.primary,
          case.localConfiguredReplica,
        },
      },
    }

  pure def canAccept(case: ValidationCase): bool =
    match case.surface {
      | ReplicationJournal => canAcceptReplicationJournal(case)
      | ReplicationSnapshot => canAcceptReplicationSnapshot(case)
      | ServiceJournal => canAcceptServiceJournal(case)
      | ServiceSnapshot => canAcceptServiceSnapshot(case)
    }

  action init = all {
    accepted' = Set(),
    rejected' = Set(),
  }

  action observeAccepted(case: ValidationCase): bool = all {
    canAccept(case),
    accepted' = accepted.union(Set(case)),
    rejected' = rejected,
  }

  action observeRejected(case: ValidationCase): bool = all {
    not(canAccept(case)),
    accepted' = accepted,
    rejected' = rejected.union(Set(case)),
  }

  action step = {
    nondet surface = oneOf(SourceSurfaces)
    nondet localNode = oneOf(Nodes)
    nondet transportSource = oneOf(Nodes)
    nondet claimedSource = oneOf(SourceClaims)
    nondet primary = oneOf(Nodes)
    nondet hasContext = oneOf(Bools)
    nondet hasControlPlane = oneOf(Bools)
    nondet batchHasFileSystem = oneOf(Bools)
    nondet shardConfigured = oneOf(Bools)
    nondet shardMatches = oneOf(Bools)
    nondet localConfiguredReplica = oneOf(Bools)
    val case = {
      surface: surface,
      localNode: localNode,
      transportSource: transportSource,
      claimedSource: claimedSource,
      primary: primary,
      hasContext: hasContext,
      hasControlPlane: hasControlPlane,
      batchHasFileSystem: batchHasFileSystem,
      shardConfigured: shardConfigured,
      shardMatches: shardMatches,
      localConfiguredReplica: localConfiguredReplica,
    }
    any {
      observeAccepted(case),
      observeRejected(case),
    }
  }
}
