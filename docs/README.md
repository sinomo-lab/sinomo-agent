# Sinomo Documentation

Use this page to choose a document by task and audience. Architecture documents
explain stable boundaries and invariants. User guides explain supported
workflows. Plans track unfinished work and are not descriptions of current
behavior.

## Users and Operators

| Task | Document |
|---|---|
| Create, join, inspect, or leave a cluster | [Cluster operations](user/cluster-operations.md) |
| Install updates or roll them out to nodes | [Rollout operations](user/rollout-operations.md) |
| Understand the shared filesystem and its policies | [SIFS](sifs.md) |

The CLI is the source of truth for command syntax. Run
`sinomo <command> help` when a guide and the installed binary differ.

## Developers

Start with the [architecture overview](architecture.md), then open the
subsystem document relevant to the change:

| Area | Document |
|---|---|
| Agent sessions, context, tools, and cancellation | [Agent system](agent-system-architecture.md) |
| Cluster identity, membership, consensus, and failure | [Clustering](clustering-architecture.md) |
| Routing, endpoint selection, and transports | [Network](network-architecture.md) |
| Distributed metadata, content, and consistency | [DFS](dfs-architecture.md) |
| Long-horizon records, retrieval, and governance | [Memory](memory-architecture.md) |
| Module identity, activation, and replacement | [Module lifecycle](module-lifecycle-architecture.md) |
| Evaluation, mutation, promotion, and rollback | [Self-evolution](self-evolution-architecture.md) |
| Logs, metrics, traces, and observation routes | [Observability](observability-architecture.md) |

The sandbox has its own [architecture document](../crates/sinomo-sandbox/docs/architecture.md).

## Specifications and Reference

- [Sinomo Object format](sinomo-object-format.md) is the normative binary
  encoding specification.
- [Storage architecture](storage-architecture.md) defines ownership and
  verification of local data-directory paths.
- [SIFS](sifs.md) defines the default cluster filesystem layout and policies.
- [Cluster formal verification](formal/cluster-verification.md) explains the
  executable Quint models and how to run them.

Exact wire layouts, schemas, and executable specifications are intentionally
detailed. General architecture documents should link to those references rather
than repeat them.

## Plans and Research

- [Plans](plans/README.md) contain active roadmaps and retained historical
  proposals.
- [Research notes](research_notes/README.md) contain literature reviews and
  design evidence. They are inputs to architecture, not normative contracts.

## Documentation Rules

- User guides are task-oriented and use current public commands.
- Architecture documents explain intent, ownership, invariants, and major
  flows. They do not catalogue every type, route, file, or test.
- Reference documents may include exact formats and protocol details when that
  precision is their purpose.
- Plans contain future work, status, and sequencing. Completed implementation
  history belongs in Git, release notes, or issue tracking.
- Research notes separate sourced findings from project decisions.
- A document should link to the owning specialist page instead of duplicating
  its content.
