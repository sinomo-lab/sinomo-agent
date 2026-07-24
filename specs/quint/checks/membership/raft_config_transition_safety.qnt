/// Safety checks and focused branch witnesses for `raft_config_transition`.
module raft_config_transition_safety {
  import common.* from "../../lib/common"
  import raft_config_transition.* from "../../models/membership/raft_config_transition"

  def scenarioRemainsFixed: bool = all {
    baseConfig == baseConfigFor(transitionCase),
    proposedConfig == proposedConfigFor(transitionCase),
  }

  def allConfigurationsHaveDisjointRoles: bool = all {
    configRolesAreDisjoint(baseConfig),
    configRolesAreDisjoint(proposedConfig),
    configRolesAreDisjoint(committedConfig),
  }

  /// Only a validated proposal may replace the committed base configuration.
  def committedConfigChangesOnlyAfterValidation: bool =
    if (phase == ProposalCommitted) {
      all {
        committedConfig == proposedConfig,
        transitionIsValid(baseConfig, proposedConfig),
        configRevision == 2,
      }
    } else {
      all {
        committedConfig == baseConfig,
        configRevision == 1,
      }
    }

  def invalidProposalNeverCommits: bool =
    not(transitionIsValid(baseConfig, proposedConfig)) implies
      phase != ProposalCommitted

  /// A committed voter addition is limited to one node and comes from the
  /// learner/non-voting roles in the prior committed configuration.
  def committedVoterAdditionsHavePriorBase: bool = {
    val added = addedVoters(baseConfig, committedConfig)
    phase == ProposalCommitted implies all {
      added.size() <= 1,
      added.subseteq(baseConfig.learners.union(baseConfig.nonVoting)),
    }
  }

  /// A committed voter removal retains both quorum thresholds and guarantees
  /// that every old quorum intersects every new quorum.
  def committedVoterRemovalHasUniversalQuorumOverlap: bool = {
    val removed = removedVoters(baseConfig, committedConfig)
    val retained = retainedVoters(baseConfig, committedConfig)
    all {
      phase == ProposalCommitted,
      removed.size() > 0,
    } implies all {
      retained.size() >= quorumSize(baseConfig.voters),
      retained.size() >= quorumSize(committedConfig.voters),
      everyOldAndNewQuorumOverlaps(baseConfig, committedConfig),
    }
  }

  /// Commit evidence is frozen from acknowledgements by voters in the current
  /// (pre-transition) configuration, never from proposed voters or health.
  def commitUsesCurrentConfigQuorum: bool = all {
    acknowledgers.subseteq(baseConfig.voters),
    commitAcknowledgers.subseteq(baseConfig.voters),
    phase == ProposalCommitted implies all {
      commitAcknowledgers == acknowledgers,
      acknowledgersHaveCurrentQuorum(baseConfig, commitAcknowledgers),
    },
  }

  /// Health observations are advisory. Recording one cannot mutate or commit
  /// the proposal, and health never appears in the transition/quorum formulas.
  def healthObservationIsNotCommitAuthority: bool =
    lastEvent == HealthObserved implies all {
      phase == ProposalReady,
      committedConfig == baseConfig,
      configRevision == 1,
      commitAcknowledgers == Set(),
    }

  def rejectionPreservesCommittedConfig: bool =
    phase == ProposalRejected implies all {
      committedConfig == baseConfig,
      not(transitionIsValid(baseConfig, proposedConfig)),
      configRevision == 1,
      commitAcknowledgers == Set(),
    }

  def revisionAndPhaseAreConsistent: bool = all {
    configRevision >= 1,
    configRevision <= 2,
    (configRevision == 2) iff (phase == ProposalCommitted),
  }

  /// Witness: a committed learner is promoted from one voter to two after the
  /// sole current voter acknowledges the change.
  def witnessOneToTwoLearnerPromotionCommitted: bool = all {
    transitionCase == PromoteLearnerOneToTwo,
    phase == ProposalCommitted,
    baseConfig.voters == Set(N1),
    baseConfig.learners.contains(N2),
    committedConfig.voters == Set(N1, N2),
    addedVoters(baseConfig, committedConfig) == Set(N2),
    commitAcknowledgers == Set(N1),
  }

  def witnessOneToTwoNonVotingPromotionCommitted: bool = all {
    transitionCase == PromoteNonVotingOneToTwo,
    phase == ProposalCommitted,
    baseConfig.voters == Set(N1),
    baseConfig.nonVoting.contains(N2),
    committedConfig.voters == Set(N1, N2),
    addedVoters(baseConfig, committedConfig) == Set(N2),
    commitAcknowledgers == Set(N1),
  }

  /// Witness: shrinking three voters to two retains both old and new quorum.
  def witnessThreeToTwoShrinkCommitted: bool = all {
    transitionCase == ShrinkThreeToTwo,
    phase == ProposalCommitted,
    baseConfig.voters == Set(N1, N2, N3),
    committedConfig.voters == Set(N1, N2),
    retainedVoters(baseConfig, committedConfig).size() == 2,
    commitAcknowledgers.size() >= quorumSize(baseConfig.voters),
  }

  /// Witness: shrinking two voters to one is rejected because the retained
  /// voter does not satisfy the old two-voter quorum.
  def witnessTwoToOneShrinkRejected: bool = all {
    transitionCase == RejectShrinkTwoToOne,
    phase == ProposalRejected,
    baseConfig.voters == Set(N1, N2),
    proposedConfig.voters == Set(N1),
    retainedVoters(baseConfig, proposedConfig).size()
      < quorumSize(baseConfig.voters),
  }

  def witnessDirectVoterAdditionRejected: bool = all {
    transitionCase == RejectDirectVoterAddition,
    phase == ProposalRejected,
    addedVoters(baseConfig, proposedConfig) == Set(N2),
    not(baseConfig.learners.union(baseConfig.nonVoting).contains(N2)),
  }

  def witnessTwoVoterAdditionsRejected: bool = all {
    transitionCase == RejectTwoVoterAdditions,
    phase == ProposalRejected,
    addedVoters(baseConfig, proposedConfig).size() == 2,
  }

  def witnessHealthObservationWithoutConfigMutation: bool = all {
    phase == ProposalReady,
    lastEvent == HealthObserved,
    committedConfig == baseConfig,
    configRevision == 1,
  }

  def witnessPartialAcknowledgementDoesNotCommit: bool = all {
    transitionCase == ShrinkThreeToTwo,
    phase == ProposalReady,
    acknowledgers.size() == 1,
    not(acknowledgersHaveCurrentQuorum(baseConfig, acknowledgers)),
    committedConfig == baseConfig,
  }

  def safety: bool = all {
    scenarioRemainsFixed,
    allConfigurationsHaveDisjointRoles,
    committedConfigChangesOnlyAfterValidation,
    invalidProposalNeverCommits,
    committedVoterAdditionsHavePriorBase,
    committedVoterRemovalHasUniversalQuorumOverlap,
    commitUsesCurrentConfigQuorum,
    healthObservationIsNotCommitAuthority,
    rejectionPreservesCommittedConfig,
    revisionAndPhaseAreConsistent,
  }
}
