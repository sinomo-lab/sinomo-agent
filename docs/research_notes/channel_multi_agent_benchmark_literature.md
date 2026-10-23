# Channel and multi-agent discussion benchmark literature

## Focus

Literature review for benchmarks relevant to multi-agent discussion systems:
shared visible timelines, multiple agent roles, conversation-mediated
coordination, social interaction, and task outcomes after deliberation.

This note focuses on the research landscape rather than implementation design.
It asks which existing benchmarks can inform evaluation of a channel-style
multi-agent system, what each benchmark actually measures, and where the
literature leaves gaps.


## High-level findings

1. **MultiAgentBench / MARBLE is the closest direct fit.** It explicitly
   evaluates LLM-based multi-agent systems in collaborative and competitive
   interactive settings, so it is the best anchor for channel-level discussion
   research.
2. **Dialogue-game benchmarks such as clembench are useful microbenchmarks,**
   but they usually test constrained communication games rather than autonomous
   multi-agent coordination.
3. **SOTOPIA and AgentSense are the strongest social-interaction references.**
   They are important because channel systems must evaluate when agents should
   speak, negotiate, withhold irrelevant content, or pursue private goals.
4. **GAIA, AgentBench, and TheAgentCompany are not primarily discussion
   benchmarks.** Their value is external validity: they test whether discussion
   improves realistic assistant or workplace tasks compared with single-agent
   execution.
5. **Collaborative Gym / Co-Gym bridges task work and interaction quality.** It
   is especially relevant to shared-workspace collaboration, where agents and
   users communicate while acting.
6. **The literature underspecifies channel-specific behavior.** Existing
   benchmarks rarely score silence, redundant replies, runaway discussion,
   cross-agent visibility, or whether a participant speaks only when useful.

## Literature map

| Benchmark | Primary object of study | Multi-agent? | Communication central? | Most relevant signal |
|---|---|---:|---:|---|
| MultiAgentBench / MARBLE | LLM multi-agent collaboration and competition | Yes | Yes | Collective task success and interaction behavior |
| SOTOPIA | Social intelligence in open-ended role-play | Yes | Yes | Goal completion, social reasoning, negotiation |
| AgentSense | Multi-turn social interaction and implicit reasoning | Yes | Yes | Social scenario success and role consistency |
| clembench | Rule-governed dialogue games | Often two-party | Yes | Protocol adherence, game success, invalid moves |
| Collaborative Gym / Co-Gym | Human-agent collaboration in shared workspaces | Human-agent, extensible | Yes | Task progress plus collaboration quality |
| GAIA | General assistant tasks with verifiable answers | No | No | Realistic task success and exact-answer accuracy |
| AgentBench | Interactive agent ability across environments | Mostly single-agent | No | Tool/environment success across domains |
| TheAgentCompany | Workplace-style digital tasks | Mostly single-agent baseline | Some | Long-horizon workplace task completion |

## Annotated sources

### MultiAgentBench / MARBLE

- Paper: https://aclanthology.org/2025.acl-long.421/
- Code: https://github.com/ulab-uiuc/MARBLE
- arXiv: https://arxiv.org/abs/2503.01935

MultiAgentBench, implemented through MARBLE, is the most directly relevant
benchmark family for channel-style agent discussion. Its central object is not
just whether an individual model can solve a task, but whether multiple
LLM-based agents can coordinate, collaborate, or compete in interactive
settings.

The benchmark is valuable because it treats multi-agent organization as part of
the evaluated system. That matches the research question behind channels:
whether visible discussion among independent agents produces better outcomes
than a single model or a rigid sequential pipeline.

Its main limitation for channel research is that benchmark scores still tend to
collapse rich communication into task outcomes. Additional transcript-level
analysis is needed to study redundant speech, silence, runaway loops, and
whether agents respond only to shared visible context.

### clembench

- Site: https://clembench.github.io/
- Paper: https://aclanthology.org/2023.emnlp-main.689/
- Code: https://github.com/clembench/clembench

clembench evaluates language models through communicative games. It emphasizes
language use as an interactive process: models must follow game rules, exchange
messages, and reach task-specific goals. This makes it useful as a controlled
communication benchmark.

For multi-agent discussion research, clembench contributes a clean methodology:
short games, explicit protocols, automatic scoring, and clear failure modes
such as invalid moves. These properties make it easier to analyze communication
quality than in broad open-ended agent tasks.

Its limitation is ecological validity. Communicative games are intentionally
small and constrained. They are good for isolating dialogue mechanics, but they
do not cover long-horizon tool work, open-ended coordination, or persistent
agent identities.

### GAIA

- Paper: https://arxiv.org/abs/2311.12983
- Leaderboard: https://huggingface.co/gaia-benchmark
- Inspect eval page:
  https://ukgovernmentbeis.github.io/inspect_evals/evals/assistants/gaia/

GAIA is a benchmark for general AI assistants on realistic tasks with
verifiable answers. It is not a multi-agent benchmark by design, but it is a
useful reference for testing whether multi-agent discussion creates practical
accuracy gains on tasks that resemble real assistant work.

GAIA's value is its exact-answer orientation and human-realistic task framing.
It can ground multi-agent discussion research in a simple question: does adding
specialized discussion improve the final answer, or does it merely increase
cost and delay?

Its limitation is that it does not evaluate the interaction process. If several
agents discuss a GAIA task, the benchmark can score the final answer, but it
does not itself judge whether the discussion was efficient, necessary, or well
coordinated.

### SOTOPIA

- Paper: https://arxiv.org/abs/2310.11667
- Code: https://github.com/sotopia-lab/sotopia

SOTOPIA evaluates social intelligence in language agents through role-play
scenarios. Agents operate with goals, social context, and interaction dynamics
that require negotiation, cooperation, competition, persuasion, and sensitivity
to context.

This is important for channel research because many failures in multi-agent
discussion are social rather than purely factual: agents over-speak, reveal the
wrong context, ignore other participants, pursue the wrong conversational goal,
or fail to coordinate politely and effectively.

SOTOPIA is less focused on tool-mediated task execution than on social
interaction quality. It is therefore a strong complement to task benchmarks,
not a replacement for them.

### AgentSense

- Paper: https://aclanthology.org/2025.naacl-long.257/
- Code: https://github.com/ljcleo/agent_sense

AgentSense studies social intelligence in multi-turn agent interactions,
including implicit reasoning about social situations. Like SOTOPIA, it is
relevant because multi-agent channels are not only task routers; they are
conversation spaces where participants must infer when and how to contribute.

Its contribution is the emphasis on social context, goal pursuit, and role
consistency across interactions. These are useful lenses for evaluating whether
agents in a shared discussion remain coherent participants instead of
stateless responders.

The main limitation is that social benchmark success does not necessarily imply
competence in tool use, factual research, or workspace collaboration.

### Collaborative Gym / Co-Gym

- Paper: https://arxiv.org/abs/2412.15701
- OpenReview: https://openreview.net/forum?id=GDYueXtKXT
- Code: https://github.com/SALT-NLP/collaborative-gym

Collaborative Gym, or Co-Gym, evaluates collaborative agents in environments
where communication and actions both matter. It is framed around collaboration
with humans or simulated users in shared workspaces, making it relevant to
systems where discussion is connected to artifacts, state changes, and task
progress.

For channel research, Co-Gym is valuable because it treats collaboration as more
than producing a final text answer. Communication quality, action timing,
shared-state awareness, and user interaction are all part of the evaluation
problem.

Its limitation is complexity. Compared with dialogue games or exact-answer
assistant tasks, shared-workspace benchmarks are heavier and harder to make
fully reproducible.

### AgentBench

- Paper: https://arxiv.org/abs/2308.03688
- Code: https://github.com/THUDM/AgentBench

AgentBench evaluates LLMs as agents across interactive environments. It is
useful as a broad reference for agentic ability: planning, acting, observing,
and recovering in environments rather than answering static prompts.

For multi-agent discussion research, AgentBench is mostly a baseline and
stress-test source. It can show whether adding discussion helps in interactive
settings, but its benchmark design does not primarily study communication among
multiple agents.

Its limitation is therefore scope: it measures agent-environment interaction
more directly than agent-agent interaction.

### TheAgentCompany

- Paper: https://arxiv.org/abs/2412.14161
- Site: https://the-agent-company.com/
- Code: https://github.com/TheAgentCompany/TheAgentCompany

TheAgentCompany evaluates agents on workplace-style tasks involving software,
web tools, documents, and coworker-like interactions. It is relevant because it
pushes evaluation toward long-horizon digital labor rather than toy tasks.

For channel research, its best contribution is external validity. If
multi-agent discussion is useful, workplace tasks are a realistic setting where
specialization, review, handoff, and collaborative problem solving should show
up in outcomes.

Its limitation is that it is expensive and broad. It is not designed primarily
to isolate discussion mechanics, so a poor result may reflect tool-use,
environment, browsing, coding, or long-horizon planning failures rather than
conversation failures.

## Evaluation dimensions in the literature

The benchmark literature repeatedly uses these evaluation dimensions:

- **Final task success:** exact answer, game score, scenario outcome, or
  completed workplace task.
- **Protocol adherence:** whether the model follows the rules of a dialogue
  game, environment, or role.
- **Interaction efficiency:** number of turns, invalid messages, repeated
  actions, wall time, and token cost.
- **Social-goal completion:** whether agents satisfy private and public goals
  while respecting the scenario.
- **Role consistency:** whether agents preserve persona, role, and private
  information across turns.
- **Collaboration quality:** whether communication helps shared task progress
  rather than merely adding chatter.
- **Robustness across domains:** whether an agent architecture works outside a
  single game or environment.
- **Human compatibility:** whether collaboration remains useful when a human or
  simulated user participates.

Channel systems need all of these, but none of the surveyed benchmarks covers
them completely.

## Gaps for channel-style multi-agent systems

The surveyed literature leaves several open evaluation gaps:

- **Silence is under-evaluated.** Most benchmarks reward successful messages or
  final outcomes, but few explicitly reward an agent for not speaking when it
  has nothing useful to add.
- **Shared visibility is rarely isolated.** Many systems assume a coordinator,
  hidden scratchpad, or framework-specific memory. Benchmarks seldom ask
  whether an agent used only the messages visible to other channel
  participants.
- **Runaway conversation is not a standard metric.** Multi-agent systems can
  self-trigger unproductive discussion loops, but benchmark scores often hide
  this behind timeouts or failed tasks.
- **Redundant contribution is weakly measured.** A final answer can be correct
  even if several agents repeated the same claim or duplicated work.
- **Private context discipline is uneven.** Social benchmarks cover private
  goals, but general agent benchmarks rarely test leakage or inappropriate
  sharing across participants.
- **Single-agent comparisons are inconsistent.** Some benchmarks support clear
  single-agent baselines; others make it hard to separate the value of
  multi-agent discussion from extra calls, extra tokens, or extra wall time.
- **Transcript quality is not enough.** Human-rated conversation quality can
  look good while task performance is poor, and task success can hide a brittle
  or inefficient discussion process.

## Literature takeaways

MultiAgentBench / MARBLE should be treated as the primary research anchor for
multi-agent discussion. SOTOPIA and AgentSense add the social lens that MARBLE
alone does not fully cover. clembench provides small, controlled communication
tasks that are easier to analyze. GAIA, AgentBench, Co-Gym, and
TheAgentCompany provide increasing levels of external validity for assistant,
interactive, collaborative, and workplace tasks.

The main research conclusion is that no single benchmark is sufficient. A good
evaluation story for channel-style agents needs both outcome benchmarks and
process-sensitive analysis of the discussion itself, especially around
turn-taking, useful silence, redundant speech, shared visibility, and bounded
conversation dynamics.

## Sources consulted

- MultiAgentBench / MARBLE: https://aclanthology.org/2025.acl-long.421/
- MARBLE code: https://github.com/ulab-uiuc/MARBLE
- MultiAgentBench arXiv: https://arxiv.org/abs/2503.01935
- clembench site: https://clembench.github.io/
- clembench paper: https://aclanthology.org/2023.emnlp-main.689/
- clembench code: https://github.com/clembench/clembench
- GAIA paper: https://arxiv.org/abs/2311.12983
- GAIA leaderboard: https://huggingface.co/gaia-benchmark
- GAIA Inspect eval page: https://ukgovernmentbeis.github.io/inspect_evals/evals/assistants/gaia/
- SOTOPIA paper: https://arxiv.org/abs/2310.11667
- SOTOPIA code: https://github.com/sotopia-lab/sotopia
- AgentSense paper: https://aclanthology.org/2025.naacl-long.257/
- AgentSense code: https://github.com/ljcleo/agent_sense
- Collaborative Gym / Co-Gym paper: https://arxiv.org/abs/2412.15701
- Collaborative Gym OpenReview: https://openreview.net/forum?id=GDYueXtKXT
- Collaborative Gym code: https://github.com/SALT-NLP/collaborative-gym
- AgentBench paper: https://arxiv.org/abs/2308.03688
- AgentBench code: https://github.com/THUDM/AgentBench
- TheAgentCompany paper: https://arxiv.org/abs/2412.14161
- TheAgentCompany site: https://the-agent-company.com/
- TheAgentCompany code: https://github.com/TheAgentCompany/TheAgentCompany
