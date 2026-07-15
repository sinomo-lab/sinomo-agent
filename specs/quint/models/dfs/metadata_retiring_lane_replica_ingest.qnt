/// Replica ingest through an activated, author-fenced retiring metadata lane.
///
/// Evidence class: target hardening for the post-primary-handoff repair path.
/// N1's durable retiring marker was activated for N2 at epoch 3. After a
/// subsequent committed rotation makes N3 primary at epoch 4, N1 remains a
/// configured replica but must remain unable to originate authoritative
/// writes. That author fence must not also block replica maintenance: N1 may
/// advance its durable prefix from an exact-N3/epoch-4 contiguous journal
/// batch, or replace it with an exact authoritative snapshot from that source.
///
/// Permit filesystem, source node, source epoch, payload filesystem, contiguity,
/// current replica membership, activated-marker evidence, and snapshot equality
/// are all load-bearing. The payload filesystem is a set
/// so the bounded model includes a mixed-filesystem batch or snapshot rather
/// than only a wholly foreign payload. This model separates the author gate
/// from the replica ingest gate, admits at most two successor appends, and keeps
/// arbitrary transport failure, persistence failure, and a second handoff out
/// of scope.
module dfs_metadata_retiring_lane_replica_ingest {
  import common.* from "../../lib/common"

  type FileSystem = TargetFileSystem | ForeignFileSystem
  type Shard = TargetShard | ForeignShard
  type AuthorWriteOutcome = AuthorWriteNotAttempted | AuthorWriteRejected
  type ReplicaIngestKind = NoReplicaIngest | JournalBatchIngest | SnapshotIngest
  type ReplicaIngestOutcome = IngestNotAttempted | IngestRejected | IngestAccepted
  type MarkerEvidence = MarkerNotObserved | MarkerPending | MarkerActivated
  type ReplicaMembershipEvidence = MembershipNotObserved | ReplicaRemoved | ReplicaConfigured
  type SnapshotEvidence = SnapshotNotSupplied | SnapshotExactAuthoritative | SnapshotArbitrary
  type RetiringLaneTransition =
    | RetiringLaneInitialized
    | ActiveSuccessorAppendCommitted
    | RetiringAuthorWriteAttemptRejected
    | ReplicaBatchRejected
    | ReplicaBatchAccepted
    | AuthoritativeSnapshotRejected
    | AuthoritativeSnapshotAccepted
    | RetiringLaneStuttered

  type RetiringLaneState = {
    handoffActivated: bool,
    retiringFileSystem: FileSystem,
    retiringShard: Shard,
    retiringNode: Node,
    retiringEpoch: int,
    markerSuccessor: Node,
    markerEpoch: int,
    activeSuccessor: Node,
    activeEpoch: int,
    activeEpochCommitted: bool,
    retiringConfiguredReplica: bool,
    retiringAuthorBound: bool,
    retiringAuthorFenced: bool,
    retiringSequence: int,
    successorSequence: int,
    successorAppendCommitted: bool,
    authorWriteOutcome: AuthorWriteOutcome,
    authorWriteAccepted: bool,
    authorSequenceBeforeAttempt: int,
    lastIngestKind: ReplicaIngestKind,
    lastIngestOutcome: ReplicaIngestOutcome,
    lastPermitFileSystem: FileSystem,
    lastPermitShard: Shard,
    lastPayloadFileSystems: Set[FileSystem],
    lastMarkerEvidence: MarkerEvidence,
    lastReplicaMembershipEvidence: ReplicaMembershipEvidence,
    lastSource: Node,
    lastSourceEpoch: int,
    replicaSequenceBeforeIngest: int,
    lastBatchBegin: int,
    lastBatchEnd: int,
    lastSnapshotSequence: int,
    lastSnapshotEvidence: SnapshotEvidence,
    lastTransition: RetiringLaneTransition,
  }

  pure val RetiringNode: Node = N1
  pure val MarkerSuccessor: Node = N2
  pure val ActiveSuccessor: Node = N3
  pure val ForgedSource: Node = N1
  pure val RetiringEpoch: int = 1
  pure val MarkerEpoch: int = 3
  pure val ActiveEpoch: int = 4
  pure val FutureUncommittedEpoch: int = 5
  pure val MaxSuccessorSequence: int = 3
  pure val TargetPayloadFileSystems: Set[FileSystem] = Set(TargetFileSystem)
  pure val ForeignPayloadFileSystems: Set[FileSystem] = Set(ForeignFileSystem)
  pure val MixedPayloadFileSystems: Set[FileSystem] =
    Set(TargetFileSystem, ForeignFileSystem)

  pure def permitIsExactActiveSuccessor(
    state: RetiringLaneState,
    permitFileSystem: FileSystem,
    permitShard: Shard,
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
  ): bool = all {
    permitFileSystem == state.retiringFileSystem,
    permitShard == state.retiringShard,
    state.markerEpoch <= state.activeEpoch,
    state.activeEpochCommitted,
    state.retiringConfiguredReplica,
    markerEvidence == MarkerActivated,
    membershipEvidence == ReplicaConfigured,
    source == state.activeSuccessor,
    sourceEpoch == state.activeEpoch,
  }

  pure def payloadBelongsOnlyToPermitFileSystem(
    permitFileSystem: FileSystem,
    payloadFileSystems: Set[FileSystem],
  ): bool = payloadFileSystems == Set(permitFileSystem)

  pure def batchIsContiguousAndAuthoritative(
    state: RetiringLaneState,
    beginSequence: int,
    endSequence: int,
  ): bool = all {
    beginSequence == state.retiringSequence + 1,
    endSequence >= beginSequence,
    endSequence <= state.successorSequence,
  }

  pure def replicaBatchIsAdmissible(
    state: RetiringLaneState,
    permitFileSystem: FileSystem,
    permitShard: Shard,
    payloadFileSystems: Set[FileSystem],
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
    beginSequence: int,
    endSequence: int,
  ): bool = all {
    state.handoffActivated,
    state.retiringAuthorFenced,
    not(state.retiringAuthorBound),
    permitIsExactActiveSuccessor(
      state,
      permitFileSystem,
      permitShard,
      markerEvidence,
      membershipEvidence,
      source,
      sourceEpoch
    ),
    payloadBelongsOnlyToPermitFileSystem(permitFileSystem, payloadFileSystems),
    batchIsContiguousAndAuthoritative(state, beginSequence, endSequence),
  }

  pure def authoritativeSnapshotIsAdmissible(
    state: RetiringLaneState,
    permitFileSystem: FileSystem,
    permitShard: Shard,
    payloadFileSystems: Set[FileSystem],
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
    snapshotSequence: int,
    evidence: SnapshotEvidence,
  ): bool = all {
    state.handoffActivated,
    state.retiringAuthorFenced,
    not(state.retiringAuthorBound),
    permitIsExactActiveSuccessor(
      state,
      permitFileSystem,
      permitShard,
      markerEvidence,
      membershipEvidence,
      source,
      sourceEpoch
    ),
    payloadBelongsOnlyToPermitFileSystem(permitFileSystem, payloadFileSystems),
    evidence == SnapshotExactAuthoritative,
    snapshotSequence == state.successorSequence,
  }

  pure val initialState: RetiringLaneState = {
    handoffActivated: true,
    retiringFileSystem: TargetFileSystem,
    retiringShard: TargetShard,
    retiringNode: RetiringNode,
    retiringEpoch: RetiringEpoch,
    markerSuccessor: MarkerSuccessor,
    markerEpoch: MarkerEpoch,
    activeSuccessor: ActiveSuccessor,
    activeEpoch: ActiveEpoch,
    activeEpochCommitted: true,
    retiringConfiguredReplica: true,
    retiringAuthorBound: false,
    retiringAuthorFenced: true,
    retiringSequence: 1,
    successorSequence: 1,
    successorAppendCommitted: false,
    authorWriteOutcome: AuthorWriteNotAttempted,
    authorWriteAccepted: false,
    authorSequenceBeforeAttempt: 1,
    lastIngestKind: NoReplicaIngest,
    lastIngestOutcome: IngestNotAttempted,
    lastPermitFileSystem: TargetFileSystem,
    lastPermitShard: TargetShard,
    lastPayloadFileSystems: Set(),
    lastMarkerEvidence: MarkerNotObserved,
    lastReplicaMembershipEvidence: MembershipNotObserved,
    lastSource: RetiringNode,
    lastSourceEpoch: RetiringEpoch,
    replicaSequenceBeforeIngest: 1,
    lastBatchBegin: 0,
    lastBatchEnd: 0,
    lastSnapshotSequence: 0,
    lastSnapshotEvidence: SnapshotNotSupplied,
    lastTransition: RetiringLaneInitialized,
  }

  var retiringLane: RetiringLaneState

  action init = retiringLane' = initialState

  action appendOnActiveSuccessor = all {
    retiringLane.successorSequence < MaxSuccessorSequence,
    retiringLane' = retiringLane
      .with("successorSequence", retiringLane.successorSequence + 1)
      .with("successorAppendCommitted", true)
      .with("lastTransition", ActiveSuccessorAppendCommitted),
  }

  /// The retiring lane's author API remains frozen even though its replica
  /// maintenance APIs below are allowed to advance the same durable backend.
  action attemptRetiringAuthorWrite =
    retiringLane' = retiringLane
      .with("authorWriteOutcome", AuthorWriteRejected)
      .with("authorWriteAccepted", false)
      .with("authorSequenceBeforeAttempt", retiringLane.retiringSequence)
      .with("lastTransition", RetiringAuthorWriteAttemptRejected)

  action ingestReplicaBatch(
    permitFileSystem: FileSystem,
    permitShard: Shard,
    payloadFileSystems: Set[FileSystem],
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
    beginSequence: int,
    endSequence: int,
  ): bool =
    retiringLane' = retiringLane
      .with("lastIngestKind", JournalBatchIngest)
      .with(
        "lastIngestOutcome",
        if (replicaBatchIsAdmissible(
          retiringLane,
          permitFileSystem,
          permitShard,
          payloadFileSystems,
          markerEvidence,
          membershipEvidence,
          source,
          sourceEpoch,
          beginSequence,
          endSequence
        )) IngestAccepted else IngestRejected
      )
      .with("lastPermitFileSystem", permitFileSystem)
      .with("lastPermitShard", permitShard)
      .with("lastPayloadFileSystems", payloadFileSystems)
      .with("lastMarkerEvidence", markerEvidence)
      .with("lastReplicaMembershipEvidence", membershipEvidence)
      .with("lastSource", source)
      .with("lastSourceEpoch", sourceEpoch)
      .with("replicaSequenceBeforeIngest", retiringLane.retiringSequence)
      .with("lastBatchBegin", beginSequence)
      .with("lastBatchEnd", endSequence)
      .with("lastSnapshotSequence", 0)
      .with("lastSnapshotEvidence", SnapshotNotSupplied)
      .with(
        "retiringSequence",
        if (replicaBatchIsAdmissible(
          retiringLane,
          permitFileSystem,
          permitShard,
          payloadFileSystems,
          markerEvidence,
          membershipEvidence,
          source,
          sourceEpoch,
          beginSequence,
          endSequence
        )) endSequence else retiringLane.retiringSequence
      )
      .with(
        "lastTransition",
        if (replicaBatchIsAdmissible(
          retiringLane,
          permitFileSystem,
          permitShard,
          payloadFileSystems,
          markerEvidence,
          membershipEvidence,
          source,
          sourceEpoch,
          beginSequence,
          endSequence
        )) ReplicaBatchAccepted else ReplicaBatchRejected
      )

  action installAuthoritativeSnapshot(
    permitFileSystem: FileSystem,
    permitShard: Shard,
    payloadFileSystems: Set[FileSystem],
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
    snapshotSequence: int,
    evidence: SnapshotEvidence,
  ): bool =
    retiringLane' = retiringLane
      .with("lastIngestKind", SnapshotIngest)
      .with(
        "lastIngestOutcome",
        if (authoritativeSnapshotIsAdmissible(
          retiringLane,
          permitFileSystem,
          permitShard,
          payloadFileSystems,
          markerEvidence,
          membershipEvidence,
          source,
          sourceEpoch,
          snapshotSequence,
          evidence
        )) IngestAccepted else IngestRejected
      )
      .with("lastPermitFileSystem", permitFileSystem)
      .with("lastPermitShard", permitShard)
      .with("lastPayloadFileSystems", payloadFileSystems)
      .with("lastMarkerEvidence", markerEvidence)
      .with("lastReplicaMembershipEvidence", membershipEvidence)
      .with("lastSource", source)
      .with("lastSourceEpoch", sourceEpoch)
      .with("replicaSequenceBeforeIngest", retiringLane.retiringSequence)
      .with("lastBatchBegin", 0)
      .with("lastBatchEnd", 0)
      .with("lastSnapshotSequence", snapshotSequence)
      .with("lastSnapshotEvidence", evidence)
      .with(
        "retiringSequence",
        if (authoritativeSnapshotIsAdmissible(
          retiringLane,
          permitFileSystem,
          permitShard,
          payloadFileSystems,
          markerEvidence,
          membershipEvidence,
          source,
          sourceEpoch,
          snapshotSequence,
          evidence
        )) snapshotSequence else retiringLane.retiringSequence
      )
      .with(
        "lastTransition",
        if (authoritativeSnapshotIsAdmissible(
          retiringLane,
          permitFileSystem,
          permitShard,
          payloadFileSystems,
          markerEvidence,
          membershipEvidence,
          source,
          sourceEpoch,
          snapshotSequence,
          evidence
        )) AuthoritativeSnapshotAccepted else AuthoritativeSnapshotRejected
      )

  action stutter =
    retiringLane' = retiringLane.with("lastTransition", RetiringLaneStuttered)

  action step = any {
    appendOnActiveSuccessor,
    attemptRetiringAuthorWrite,
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ForgedSource, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, RetiringEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, MarkerEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, FutureUncommittedEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerPending, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaRemoved, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, ForeignShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 2, retiringLane.successorSequence),
    ingestReplicaBatch(TargetFileSystem, TargetShard, MixedPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    ingestReplicaBatch(ForeignFileSystem, TargetShard, ForeignPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.retiringSequence + 1, retiringLane.successorSequence),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ForgedSource, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, RetiringEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, MarkerEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, FutureUncommittedEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerPending, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaRemoved, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, ForeignShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotArbitrary),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(TargetFileSystem, TargetShard, MixedPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    installAuthoritativeSnapshot(ForeignFileSystem, TargetShard, ForeignPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, retiringLane.successorSequence, SnapshotExactAuthoritative),
    stutter,
  }

  /// Coverage-only schedulers call the same admission actions but first create
  /// one committed successor append, so every rejection is otherwise capable
  /// of advancing the retiring prefix and both accepted ingest kinds converge.
  action batchCoverageStep(
    permitFileSystem: FileSystem,
    permitShard: Shard,
    payloadFileSystems: Set[FileSystem],
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
    beginOffset: int,
  ): bool =
    if (retiringLane.retiringSequence == retiringLane.successorSequence) {
      appendOnActiveSuccessor
    } else {
      ingestReplicaBatch(
        permitFileSystem,
        permitShard,
        payloadFileSystems,
        markerEvidence,
        membershipEvidence,
        source,
        sourceEpoch,
        retiringLane.retiringSequence + beginOffset,
        retiringLane.successorSequence
      )
    }

  action snapshotCoverageStep(
    permitFileSystem: FileSystem,
    permitShard: Shard,
    payloadFileSystems: Set[FileSystem],
    markerEvidence: MarkerEvidence,
    membershipEvidence: ReplicaMembershipEvidence,
    source: Node,
    sourceEpoch: int,
    evidence: SnapshotEvidence,
  ): bool =
    if (retiringLane.retiringSequence == retiringLane.successorSequence) {
      appendOnActiveSuccessor
    } else {
      installAuthoritativeSnapshot(
        permitFileSystem,
        permitShard,
        payloadFileSystems,
        markerEvidence,
        membershipEvidence,
        source,
        sourceEpoch,
        retiringLane.successorSequence,
        evidence
      )
    }

  action coverageStep = any {
    step,
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerPending, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaRemoved, ActiveSuccessor, ActiveEpoch, 1),
    batchCoverageStep(TargetFileSystem, ForeignShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ForgedSource, ActiveEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, RetiringEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, MarkerEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, FutureUncommittedEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, 2),
    batchCoverageStep(TargetFileSystem, TargetShard, MixedPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, 1),
    batchCoverageStep(ForeignFileSystem, TargetShard, ForeignPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, 1),
    batchCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, 1),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerPending, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaRemoved, ActiveSuccessor, ActiveEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, ForeignShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ForgedSource, ActiveEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, RetiringEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, MarkerEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, FutureUncommittedEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, SnapshotArbitrary),
    snapshotCoverageStep(TargetFileSystem, TargetShard, MixedPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(ForeignFileSystem, TargetShard, ForeignPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, SnapshotExactAuthoritative),
    snapshotCoverageStep(TargetFileSystem, TargetShard, TargetPayloadFileSystems, MarkerActivated, ReplicaConfigured, ActiveSuccessor, ActiveEpoch, SnapshotExactAuthoritative),
  }
}
