/// Safety properties and counterexample coverage for `raft_kv`.
module raft_kv_safety {
  import common.* from "../../lib/common"
  import raft_kv.* from "../../models/kv/raft_kv"

  /// Committed indices form one canonical prefix, and any node that claims a
  /// committed index agrees with that prefix.
  def committedPrefixAgreement: bool =
    Nodes.forall(n => logsMatchCommittedPrefix(n))

  def committedSlotsAreFilled: bool =
    LogIndexes
      .filter(i => i <= globalCommitIndex)
      .forall(i => match committed.get(i) { | Filled(_) => true | Empty => false })

  def oneLeaderPerTerm: bool =
    Set(1, 2, 3, 4).forall(t => leadersByTerm.get(t).size() <= 1)

  def kvMatchesCommittedLog: bool =
    kvCell == replayCommittedAt(globalCommitIndex)

  def committedIndexInBounds: bool = all {
    globalCommitIndex >= 0,
    globalCommitIndex <= MaxLogIndex,
    nextLogIndex == globalCommitIndex + 1,
  }

  /// Local availability is independent of quorum. The returned value is still
  /// constrained to the selected node's committed prefix, which may be older
  /// than the canonical prefix.
  def localReadReturnsSelectedCommittedPrefix: bool =
    lastOperation == LocalReadReturned implies all {
      lastRead.kind == LocalRead,
      lastRead.succeeded,
      lastRead.revision == commitIndex.get(lastRead.node),
      lastRead.revision <= globalCommitIndex,
      lastRead.prefix == committedPrefixAt(lastRead.revision),
      lastRead.cell == replayCommittedAt(lastRead.revision),
      not(lastRead.hadCurrentTermQuorumBarrier),
    }

  /// Authoritative success is tied to a current-term quorum barrier and returns
  /// the exact canonical committed prefix rather than a leader-local snapshot.
  def authoritativeReadSuccessHasCurrentTermCanonicalBarrier: bool =
    all {
      lastOperation == AuthoritativeReadReturned,
      lastRead.succeeded,
    } implies all {
      lastRead.kind == AuthoritativeRead,
      currentTermQuorumBarrierAvailable,
      lastRead.hadCurrentTermQuorumBarrier,
      lastRead.barrierTerm == currentTerm,
      lastRead.revision == globalCommitIndex,
      lastRead.prefix == committed,
      lastRead.cell == kvCell,
    }

  def authoritativeReadFailureNeverFallsBackToLocal: bool =
    all {
      lastOperation == AuthoritativeReadReturned,
      not(lastRead.succeeded),
    } implies all {
      lastRead.kind == AuthoritativeRead,
      not(currentTermQuorumBarrierAvailable),
      not(lastRead.hadCurrentTermQuorumBarrier),
      lastRead.barrierTerm == 0,
      lastRead.revision == -1,
      lastRead.prefix == emptyLog,
    }

  def lastOperationIsRefinedWrite: bool = any {
    lastOperation == QuorumWriteReturned,
    lastOperation == SemanticNoEffectWriteReturned,
    lastOperation == ExactCommittedRetryReturned,
  }

  def exactDurableEvidenceMatchesLastWrite: bool = all {
    lastWrite.evidence == ExactDurableCommittedRequestEvidence,
    hasExactDurableCommittedRequest(lastWrite.request, lastWrite.command),
    lastWrite.revision == committedRequestRevisions.get(lastWrite.request),
    lastWrite.evidenceTerm == committedRequestTerms.get(lastWrite.request),
  }

  /// Every refined successful write result is justified by the exact evidence
  /// class carried in that result.
  def writeSuccessHasQuorumOrExactDurableCommitEvidence: bool =
    lastOperationIsRefinedWrite implies all {
      lastWrite.succeeded,
      any {
        all {
          lastOperation == QuorumWriteReturned,
          lastWrite.kind == QuorumWrite,
          lastWrite.evidence == QuorumCommitEvidence,
          currentTermQuorumBarrierAvailable,
          lastWrite.evidenceTerm == currentTerm,
          lastWrite.revision == globalCommitIndex,
          hasExactDurableCommittedRequest(lastWrite.request, lastWrite.command),
          committedRequestRevisions.get(lastWrite.request) == lastWrite.revision,
          not(lastWrite.semanticNoEffect),
        },
        all {
          lastOperation == SemanticNoEffectWriteReturned,
          lastWrite.kind == SemanticNoEffectWrite,
          lastWrite.evidence == CurrentTermQuorumBarrierEvidence,
          currentTermQuorumBarrierAvailable,
          lastWrite.evidenceTerm == currentTerm,
          lastWrite.revision == globalCommitIndex,
          lastWrite.semanticNoEffect,
          isSemanticNoEffect(lastWrite.command),
        },
        all {
          lastOperation == ExactCommittedRetryReturned,
          lastWrite.kind == ExactCommittedRetry,
          exactDurableEvidenceMatchesLastWrite,
        },
      },
    }

  /// In particular, local equality alone is never enough for a successful
  /// semantic no-effect result in the refined transition relation.
  def semanticNoEffectSuccessHasBarrierOrExactCommittedRequest: bool =
    all {
      lastOperationIsRefinedWrite,
      lastWrite.succeeded,
      lastWrite.semanticNoEffect,
    } implies any {
      all {
        lastWrite.evidence == CurrentTermQuorumBarrierEvidence,
        currentTermQuorumBarrierAvailable,
        lastWrite.evidenceTerm == currentTerm,
      },
      exactDurableEvidenceMatchesLastWrite,
    }

  /// Executable legacy counterexample: the broken transition reports success
  /// for a semantic no-effect request without either accepted evidence class.
  /// It is intentionally a witness, not part of `safety`, because `coverageStep`
  /// executes it while symbolic verification explores only refined `step`.
  def brokenNoQuorumNoEffectViolatesWriteContract: bool = all {
    brokenNoEffectResult.attempted,
    brokenNoEffectResult.succeeded,
    brokenNoEffectResult.semanticNoEffect,
    not(brokenNoEffectResult.hadCurrentTermQuorumBarrier),
    not(brokenNoEffectResult.hadExactDurableCommittedRequest),
  }

  def witnessBrokenNoQuorumNoEffectCounterexample: bool =
    brokenNoQuorumNoEffectViolatesWriteContract

  def safety: bool = all {
    committedPrefixAgreement,
    committedSlotsAreFilled,
    oneLeaderPerTerm,
    kvMatchesCommittedLog,
    committedIndexInBounds,
    localReadReturnsSelectedCommittedPrefix,
    authoritativeReadSuccessHasCurrentTermCanonicalBarrier,
    authoritativeReadFailureNeverFallsBackToLocal,
    writeSuccessHasQuorumOrExactDurableCommitEvidence,
    semanticNoEffectSuccessHasBarrierOrExactCommittedRequest,
  }
}
