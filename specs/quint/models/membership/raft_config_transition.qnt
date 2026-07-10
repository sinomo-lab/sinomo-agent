/// Focused model of committed Raft membership configuration changes.
///
/// This slice mirrors `validate_cluster_kv_raft_config_transition` rather than
/// re-modeling leader election or log replication. A proposal is evaluated
/// against one immutable committed base configuration. Committing it requires
/// acknowledgements from a quorum of that base configuration; health is only
/// an observation and is deliberately kept out of both validation and quorum
/// authority.
///
/// Evidence class: current-behavior refinement of configuration validation and
/// current-config quorum acknowledgement. It does not model Raft RPC transport.
module raft_config_transition {
  import common.* from "../../lib/common"

  type RaftConfig = {
    voters: Set[Node],
    learners: Set[Node],
    nonVoting: Set[Node],
  }

  type TransitionCase =
    | PromoteLearnerOneToTwo
    | PromoteNonVotingOneToTwo
    | RejectDirectVoterAddition
    | RejectTwoVoterAdditions
    | ShrinkThreeToTwo
    | RejectShrinkTwoToOne

  type TransitionPhase = ProposalReady | ProposalCommitted | ProposalRejected
  type HealthObservation = HealthUnknown | ObservedHealthy | ObservedUnhealthy
  type TransitionEvent =
    | Initialized
    | HealthObserved
    | AcknowledgementRecorded
    | ValidTransitionCommitted
    | InvalidTransitionRejected
    | TerminalStutter

  pure val TransitionCases: Set[TransitionCase] = Set(
    PromoteLearnerOneToTwo,
    PromoteNonVotingOneToTwo,
    RejectDirectVoterAddition,
    RejectTwoVoterAdditions,
    ShrinkThreeToTwo,
    RejectShrinkTwoToOne
  )

  pure val HealthObservations: Set[HealthObservation] =
    Set(ObservedHealthy, ObservedUnhealthy)

  var transitionCase: TransitionCase
  var baseConfig: RaftConfig
  var proposedConfig: RaftConfig
  var committedConfig: RaftConfig
  var acknowledgers: Set[Node]
  var commitAcknowledgers: Set[Node]
  var healthObservation: Node -> HealthObservation
  var phase: TransitionPhase
  var configRevision: int
  var lastEvent: TransitionEvent

  pure def baseConfigFor(selected: TransitionCase): RaftConfig =
    match selected {
      | PromoteLearnerOneToTwo => {
          voters: Set(N1),
          learners: Set(N2),
          nonVoting: Set(N3),
        }
      | PromoteNonVotingOneToTwo => {
          voters: Set(N1),
          learners: Set(N3),
          nonVoting: Set(N2),
        }
      | RejectDirectVoterAddition => {
          voters: Set(N1),
          learners: Set(),
          nonVoting: Set(N3),
        }
      | RejectTwoVoterAdditions => {
          voters: Set(N1),
          learners: Set(N2),
          nonVoting: Set(N3),
        }
      | ShrinkThreeToTwo => {
          voters: Set(N1, N2, N3),
          learners: Set(),
          nonVoting: Set(),
        }
      | RejectShrinkTwoToOne => {
          voters: Set(N1, N2),
          learners: Set(),
          nonVoting: Set(N3),
        }
    }

  pure def proposedConfigFor(selected: TransitionCase): RaftConfig =
    match selected {
      | PromoteLearnerOneToTwo => {
          voters: Set(N1, N2),
          learners: Set(),
          nonVoting: Set(N3),
        }
      | PromoteNonVotingOneToTwo => {
          voters: Set(N1, N2),
          learners: Set(N3),
          nonVoting: Set(),
        }
      | RejectDirectVoterAddition => {
          voters: Set(N1, N2),
          learners: Set(),
          nonVoting: Set(N3),
        }
      | RejectTwoVoterAdditions => {
          voters: Set(N1, N2, N3),
          learners: Set(),
          nonVoting: Set(),
        }
      | ShrinkThreeToTwo => {
          voters: Set(N1, N2),
          learners: Set(N3),
          nonVoting: Set(),
        }
      | RejectShrinkTwoToOne => {
          voters: Set(N1),
          learners: Set(N2),
          nonVoting: Set(N3),
        }
    }

  pure def addedVoters(current: RaftConfig, candidate: RaftConfig): Set[Node] =
    candidate.voters.exclude(current.voters)

  pure def removedVoters(current: RaftConfig, candidate: RaftConfig): Set[Node] =
    current.voters.exclude(candidate.voters)

  pure def retainedVoters(current: RaftConfig, candidate: RaftConfig): Set[Node] =
    current.voters.intersect(candidate.voters)

  /// Exact voter-addition rules from the Rust transition validator: a single
  /// new voter at most, already present in the committed learner/non-voting
  /// base configuration.
  pure def voterAdditionIsValid(current: RaftConfig, candidate: RaftConfig): bool = {
    val added = addedVoters(current, candidate)
    all {
      added.size() <= 1,
      added.subseteq(current.learners.union(current.nonVoting)),
    }
  }

  /// Removing any voter requires the retained set to satisfy both the old and
  /// new quorum thresholds. This is the overlapping-quorum check implemented
  /// by `validate_cluster_kv_raft_config_transition`.
  pure def voterRemovalIsValid(current: RaftConfig, candidate: RaftConfig): bool = {
    val removed = removedVoters(current, candidate)
    val retained = retainedVoters(current, candidate)
    removed.size() == 0 or all {
      retained.size() >= quorumSize(current.voters),
      retained.size() >= quorumSize(candidate.voters),
    }
  }

  pure def transitionIsValid(current: RaftConfig, candidate: RaftConfig): bool = all {
    voterAdditionIsValid(current, candidate),
    voterRemovalIsValid(current, candidate),
  }

  pure def acknowledgersHaveCurrentQuorum(
    current: RaftConfig,
    acked: Set[Node]
  ): bool =
    acked.intersect(current.voters).size() >= quorumSize(current.voters)

  pure def configRolesAreDisjoint(config: RaftConfig): bool = all {
    config.voters.intersect(config.learners).size() == 0,
    config.voters.intersect(config.nonVoting).size() == 0,
    config.learners.intersect(config.nonVoting).size() == 0,
  }

  action initialize(selected: TransitionCase): bool = {
    val current = baseConfigFor(selected)
    val candidate = proposedConfigFor(selected)
    all {
      transitionCase' = selected,
      baseConfig' = current,
      proposedConfig' = candidate,
      committedConfig' = current,
      acknowledgers' = Set(),
      commitAcknowledgers' = Set(),
      healthObservation' = Nodes.mapBy(_ => HealthUnknown),
      phase' = ProposalReady,
      configRevision' = 1,
      lastEvent' = Initialized,
    }
  }

  /// Focused initializers keep the important transition cases reachable with
  /// only one to three model steps.
  action initPromoteOneToTwo = initialize(PromoteLearnerOneToTwo)
  action initPromoteNonVotingOneToTwo = initialize(PromoteNonVotingOneToTwo)
  action initRejectDirectAddition = initialize(RejectDirectVoterAddition)
  action initRejectTwoAdditions = initialize(RejectTwoVoterAdditions)
  action initShrinkThreeToTwo = initialize(ShrinkThreeToTwo)
  action initRejectShrinkTwoToOne = initialize(RejectShrinkTwoToOne)

  action init = {
    nondet selected = oneOf(TransitionCases)
    initialize(selected)
  }

  action observeHealth(node: Node, observation: HealthObservation): bool = all {
    phase == ProposalReady,
    observation != HealthUnknown,
    healthObservation' = healthObservation.set(node, observation),
    transitionCase' = transitionCase,
    baseConfig' = baseConfig,
    proposedConfig' = proposedConfig,
    committedConfig' = committedConfig,
    acknowledgers' = acknowledgers,
    commitAcknowledgers' = commitAcknowledgers,
    phase' = phase,
    configRevision' = configRevision,
    lastEvent' = HealthObserved,
  }

  action recordAcknowledgement(node: Node): bool = all {
    phase == ProposalReady,
    baseConfig.voters.contains(node),
    not(acknowledgers.contains(node)),
    acknowledgers' = acknowledgers.union(Set(node)),
    transitionCase' = transitionCase,
    baseConfig' = baseConfig,
    proposedConfig' = proposedConfig,
    committedConfig' = committedConfig,
    commitAcknowledgers' = commitAcknowledgers,
    healthObservation' = healthObservation,
    phase' = phase,
    configRevision' = configRevision,
    lastEvent' = AcknowledgementRecorded,
  }

  action commitValidProposal: bool = all {
    phase == ProposalReady,
    transitionIsValid(baseConfig, proposedConfig),
    acknowledgersHaveCurrentQuorum(baseConfig, acknowledgers),
    committedConfig' = proposedConfig,
    commitAcknowledgers' = acknowledgers,
    phase' = ProposalCommitted,
    configRevision' = configRevision + 1,
    lastEvent' = ValidTransitionCommitted,
    transitionCase' = transitionCase,
    baseConfig' = baseConfig,
    proposedConfig' = proposedConfig,
    acknowledgers' = acknowledgers,
    healthObservation' = healthObservation,
  }

  /// Invalid proposals fail local transition validation and never reach the
  /// committed configuration. Rejection itself does not require Raft quorum.
  action rejectInvalidProposal: bool = all {
    phase == ProposalReady,
    not(transitionIsValid(baseConfig, proposedConfig)),
    committedConfig' = committedConfig,
    phase' = ProposalRejected,
    lastEvent' = InvalidTransitionRejected,
    transitionCase' = transitionCase,
    baseConfig' = baseConfig,
    proposedConfig' = proposedConfig,
    acknowledgers' = acknowledgers,
    commitAcknowledgers' = commitAcknowledgers,
    healthObservation' = healthObservation,
    configRevision' = configRevision,
  }

  action stutterTerminal: bool = all {
    phase != ProposalReady,
    transitionCase' = transitionCase,
    baseConfig' = baseConfig,
    proposedConfig' = proposedConfig,
    committedConfig' = committedConfig,
    acknowledgers' = acknowledgers,
    commitAcknowledgers' = commitAcknowledgers,
    healthObservation' = healthObservation,
    phase' = phase,
    configRevision' = configRevision,
    lastEvent' = TerminalStutter,
  }

  action step = any {
    {
      nondet node = oneOf(Nodes)
      nondet observation = oneOf(HealthObservations)
      observeHealth(node, observation)
    },
    {
      nondet node = oneOf(Nodes)
      recordAcknowledgement(node)
    },
    commitValidProposal,
    rejectInvalidProposal,
    stutterTerminal,
  }
}
