# Sinomo System Architecture Proposal

This proposal captures the high-level design intent for Sinomo. The detailed, current architecture lives in [architecture.md](architecture.md). When this proposal and the architecture differ, the architecture is the source of truth.

## Vision

Sinomo should be built as an individual AI entity runtime, not as a single monolithic model loop or a thin chat surface. The runtime should support continuous perception, durable memory, situated action, and slower reflection without letting any one path block the others.

The core design idea is a three-speed, event-centric multimodal system:

- keep live ingestion and perception always on,
- convert raw signals into timestamped structured events,
- let a fast reactive loop handle timing-sensitive decisions,
- let an interactive loop handle ordinary conversation and small actions,
- escalate only selected work into a slower deliberative loop,
- store memory through explicit promotion policies rather than dumping whole streams into context.

## Runtime Shape

Sinomo should run first on a serious local workstation or small server, with the same architecture able to expand across nearby trusted nodes. A single-machine deployment should be a special case of the distributed node model, not a separate design.

The core runtime should be realistic for self-hosted hardware:

- continuous audio, video, screen, text, telemetry, and event ingestion,
- local hot state and low-latency policy decisions,
- durable event and memory storage,
- node-local sandbox and action enforcement,
- optional use of local or remote model backends behind stable interfaces.

Model providers can change over time. The event contract, memory policy, node/device provenance, and runtime supervision model should remain stable.

## Design Principles

1. **Perception is always on.** Ingestion and perception continue while Sinomo thinks, speaks, runs tools, or executes long jobs.
2. **Events are the canonical substrate.** Services communicate through structured events with time, provenance, confidence, salience, evidence, and routing metadata.
3. **Timing and content are separate decisions.** The system decides when to speak, interrupt, alert, wait, or escalate separately from what to say or do.
4. **Reasoning runs at multiple speeds.** Fast reactive behavior, ordinary interaction, and slow deliberation should be separate cooperating loops.
5. **Memory is hierarchical.** Hot, warm, cold, and procedural memory are managed explicitly, with selective promotion from events.
6. **Distribution is native.** Nodes, devices, sessions, workspaces, events, and memory should carry identity and locality from the beginning.
7. **Safety is enforced by the runtime.** Sandboxes, policy checks, resource limits, and incident events protect action paths without putting human approval on the hot path.
8. **Implementation stays local-first.** Early versions should avoid unnecessary infrastructure and prove the contracts before adding heavier distributed services.

## Conceptual Planes

```text
Inputs
  -> Distributed Foundation Plane
  -> Ingestion Plane
  -> Perception Plane
  -> Event / World-State Plane
  -> Agent Control Plane
  -> Action / Generation Plane
  -> Outputs
```

The event/world-state plane is the architectural center. Perception produces events; memory, triggers, agents, deliberation, actions, and supervision consume and emit events. This keeps live behavior, replay, debugging, and long-horizon memory grounded in the same substrate.

## Control Loops

| Loop | Target | Responsibility |
|---|---:|---|
| Fast reactive | 50-150 ms | interruption, wake/no-wake, safety triggers, urgent acknowledgement, alert suppression |
| Interactive | 200 ms-2 s | dialogue, bounded retrieval, short tool calls, compact responses, task state |
| Deliberative | 2 s+ | planning, reflection, branch comparison, memory consolidation, long-running generation |

The fast path should stay small and deterministic. The interactive path should be bounded and user-facing. The deliberative path should run as asynchronous jobs that can be cancelled or superseded when new events change the situation.

## Memory Direction

Sinomo should not rely on one giant context window. The event log provides the replayable timeline, while memory tiers provide different working sets:

- **hot memory** for immediate live context,
- **warm episodic memory** for session continuity,
- **cold semantic memory** for long-term knowledge and summaries,
- **procedural memory** for reusable skills, workflows, policies, and recovery patterns.

Memory writes should be selective. Promote events when they are salient, repeated, unresolved, user-important, linked to task outcomes, or needed for future planning.

## Safety Direction

Sinomo should act freely inside explicit runtime envelopes. The node or device affected by an action is responsible for final local enforcement.

The runtime should record both successful and blocked actions as structured events. Supervisors can review incidents after the fact and adjust policy, but ordinary low-latency behavior should not depend on inline approval prompts.

## MVP Direction

The first useful MVP should prove the event-centric, three-speed runtime on local-first infrastructure:

- shared contracts for IDs, events, routing, node/device provenance, and capabilities,
- event validation, persistence interfaces, and replay,
- basic local node/device metadata,
- simulated perception events,
- a simple trigger path,
- a bounded interactive loop,
- one asynchronous deliberation worker,
- enough observability to replay decisions and inspect traces.

Detailed crate responsibilities, storage choices, distributed synchronization, DFS policy, runtime flows, and implementation phases are maintained in [architecture.md](architecture.md).
