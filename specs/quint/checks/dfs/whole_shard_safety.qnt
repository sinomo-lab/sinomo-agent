/// Safety and witnesses for the bounded whole-shard target contract.
module dfs_whole_shard_safety {
  import dfs_whole_shard.* from "../../models/dfs/whole_shard"
  import dfs_whole_shard_authority.* from "../../contracts/dfs/whole_shard_authority"
  def commitSafety: bool = Operations.forall(op => {
    val c = s.commits.get(op)
    val p = s.prepares.get(op)
    not(c.present) or all {
      p.present, c.digest == digest(c.op, c.body), c.digest == p.digest,
      c.seq == p.seq, c.target == c.base + 1, c.receipts == p.receipts,
      contentReceiptsValid(op, c.receipts),
      c.epoch == p.epoch, c.revision == p.revision, c.primary == p.primary,
      c.epoch == c.admittedEpoch, c.revision == c.admittedRevision,
      c.primary == c.admittedPrimary, not(c.admittedFrozen),
      (s.profile.standalone and c.epoch == 0 and c.revision == 0 and c.members == Set(c.primary))
        or (c.epoch == 1 and c.primary == 1 and c.members == s.profile.initialMembers)
        or (c.epoch == 2 and c.primary == s.desiredPrimary and c.members == s.desiredMembers),
      c.path == (if (s.profile.standalone) 0 else 1),
      publicationProofAllowed(s.profile.standalone, c.primary, c.members, c.acks),
    }
  })
  def denseUniqueHistory: bool = all {
    s.visible == Operations.filter(op => s.commits.get(op).present).size(),
    Operations.forall(version =>
      Operations.filter(op => all { s.commits.get(op).present, s.commits.get(op).target == version }).size()
        == (if (version <= s.visible) 1 else 0)),
  }
  def activationSafety: bool = s.activated implies all {
    s.frozen, jointProof, s.epoch == 2,
    s.primary == s.desiredPrimary, s.members == s.desiredMembers,
    s.frozenHead >= s.capturedHead, samePrefix(s.frozenPrefix, s.capturedPrefix, s.capturedHead),
    s.frozenHead <= s.head, samePrefix(s.journal, s.frozenPrefix, s.frozenHead),
    s.primary == 1 or s.promotionMarker,
  }
  def payloadHiddenUntilCommit: bool = Operations.forall(op =>
    s.visibleBodies.get(op) == (if (s.commits.get(op).present) s.commits.get(op).body else 0))
  def journalCommitsExactlyOnce: bool = Operations.forall(op =>
    Sequences.filter(i => all { i <= s.head, s.journal.get(i).kind == 2, s.journal.get(i).op == op }).size()
      == (if (s.commits.get(op).present) 1 else 0))
  def journalCommitDigestMatchesPayload: bool = Sequences.forall(i =>
    i <= s.head and s.journal.get(i).kind == 2 implies all {
      s.commits.get(s.journal.get(i).op).present,
      s.journal.get(i).digest == s.commits.get(s.journal.get(i).op).digest,
      s.journal.get(i).epoch == s.commits.get(s.journal.get(i).op).epoch,
    })
  def replicaReadSafety: bool = s.profile.nodes.forall(node => all {
    s.replicaRead.get(node) >= 0, s.replicaIngestRead.get(node) >= 0,
    s.replicaIngestRead.get(node) <= s.replicaRead.get(node),
    s.replicaFreeze.get(node) >= 0, s.replicaFreeze.get(node) <= 3,
    (s.replicaRead.get(node) == 0) == (s.replicaFreeze.get(node) == 0),
  })
  def safety: bool = all {
    profileValid(s.profile), s.members.subseteq(s.profile.nodes), s.online.subseteq(s.profile.nodes),
    commitSafety, denseUniqueHistory, activationSafety, payloadHiddenUntilCommit, journalCommitsExactlyOnce, journalCommitDigestMatchesPayload, replicaReadSafety,
    s.head >= 0, s.head <= 6, s.visible >= 0, s.visible <= 3,
    s.capturedHead <= s.head, s.guardOp == 0 or Operations.contains(s.guardOp),
    s.frozen and not(s.activated) implies s.guardOp == 0,
    s.frozen and not(s.activated) implies not(writable),
  }
  def witnessHealthy: bool = Operations.exists(op => s.commits.get(op).present and s.commits.get(op).path == 1)
  def witnessStandalone: bool = s.profile.standalone and Operations.exists(op => s.commits.get(op).present and s.commits.get(op).path == 0)
  def witnessPartialRejected: bool = s.partialRejected and Operations.exists(op =>
    s.prepares.get(op).present and not(s.commits.get(op).present)
      and s.acks.get(op).size() >= quorum(s.members) and s.acks.get(op) != s.members)
  def witnessMinorityRejected: bool = s.minorityRejected
  def witnessChangedRetryRejected: bool = s.changedRetryRejected
  def witnessUnknownReplay: bool = s.lostReply.intersect(s.replayed).size() > 0
  def witnessJointOfflineActivation: bool = s.activated and s.handoffAcks != s.profile.initialMembers.union(s.desiredMembers)
  def witnessPromotion: bool = s.activated and s.primary == 2 and s.bound.get(2) == 2
  def witnessStaleEpochRejected: bool = s.staleEpochRejected
  def witnessFrozenWriterRejected: bool = s.frozenWriterRejected
  def witnessWrongDigestRejected: bool = s.wrongDigestRejected
  def witnessDivergentPrefixRejected: bool = s.divergentPrefixRejected
  def witnessLateAdmittedWriteCaptured: bool = s.activated and s.lateWriteCaptured
  def witnessCapturedPrefixCarried: bool = s.activated and s.capturedHead > 0
    and samePrefix(s.frozenPrefix, s.capturedPrefix, s.capturedHead)
  def witnessPartialActivationRejected: bool = s.partialActivationRejected
}
