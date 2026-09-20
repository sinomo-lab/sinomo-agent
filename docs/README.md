# Sinomo Documentation

This is the entry point for understanding, using, operating, developing, and
studying Sinomo. Choose an audience path below instead of reading the docs tree
in filename order.

If Sinomo is new to you, begin with the [system overview](system-overview.md)
and keep the [glossary](glossary.md) nearby. Architecture pages are normative
system boundaries; plans and research notes are not descriptions of current
behavior by themselves.

## Users and Operators

Start with the [user and operator guide](user/README.md).

| Task | Document |
|---|---|
| Build, install, initialize, configure a provider, and run a first interaction | [Getting started](user/getting-started.md) |
| Use the TUI, app, agents, models, memory, skills, SIFS, and voice | [Everyday use](user/everyday-use.md) |
| Manage processes, health, observation, backups, permissions, and incidents | [Operating Sinomo](user/operations.md) |
| Create, join, inspect, migrate, leave, or recover a cluster | [Cluster operations](user/cluster-operations.md) |
| Build, stage, activate, diagnose, or recover an update | [Rollout operations](user/rollout-operations.md) |
| Create, inspect, and control metric-driven evolution goals | [Evolution goals](user/evolution.md) |
| Diagnose common local, cluster, provider, DFS, rollout, and sandbox failures | [Troubleshooting](user/troubleshooting.md) |
| Understand the shared filesystem and placement policies | [SIFS reference](sifs.md) |
| Capture, retain, restore, or recover file content | [File history](user/file-history.md) |

The installed CLI is authoritative for command syntax:

```sh
sinomo help
COMMAND=cluster
sinomo "$COMMAND" help
```

## Developers and Contributors

Start with the [developer guide](developer/README.md) or the root
[contribution guide](../CONTRIBUTING.md).

| Task | Document |
|---|---|
| Prepare Rust, build applications, isolate state, and use Docker/Android | [Development setup](developer/getting-started.md) |
| Understand crate ownership, facades, and dependency direction | [Workspace and trust rings](developer/workspace-and-trust-rings.md) |
| Add a command, service, tool, provider, module, transport, backend, or test | [Extension points](developer/extension-points.md) |
| Choose Rust, formal, cluster, sandbox, and benchmark evidence | [Testing and validation](developer/testing.md) |
| Integrate explicit file retention, content preservation, and restore | [DFS file history](developer/dfs-file-history.md) |
| Understand research surfaces, evidence classes, and claim boundaries | [Research and evaluation](developer/research-and-evaluation.md) |
| Record reproducible formal, cluster, benchmark, model, or agent experiments | [Experimental reproducibility](developer/experimental-reproducibility.md) |
| Prepare a focused reviewable change | [Contributing](../CONTRIBUTING.md) |

Read the [architecture overview](architecture.md) before non-trivial work, then
open the subsystem document that owns the change.

Research and evaluation are developer workflows rather than a separate
documentation audience. Supporting evidence includes:

- [Formal verification](formal/cluster-verification.md) explains the Quint
  model hierarchy, evidence classes, checks, and known omissions.
- [Research notes](research_notes/README.md) map relevant literature and design
  options; they are not normative contracts.
- [Plans](plans/README.md) separate active/future work and retained
  implementation history from the current architecture.
- Benchmark-specific protocols live with their harnesses under
  `modules/benchmark/sinomo-benchmarks/`.

## Architecture by Subsystem

The [architecture overview](architecture.md) defines the stable system goals,
trust model, state ownership, execution model, and invariants.

| Area | Document |
|---|---|
| Agent sessions, context, tools, evidence, compaction, and cancellation | [Agent system](agent-system-architecture.md) |
| Cluster build scheduling, immutable artifacts, caches, and rollout handoff | [Build service](build-service-architecture.md) |
| Cluster identity, membership, KV/Raft authority, and failure | [Clustering](clustering-architecture.md) |
| Direct links, connectivity, routed streams, relays, and object RPC | [Network](network-architecture.md) |
| DFS metadata, content, placement, replication, repair, and POSIX semantics | [DFS](dfs-architecture.md) |
| Direct SIFS NFSv3 presentation and Windows drive mapping | [NFSv3](nfs-architecture.md) |
| Windows SIFS projection, hydration, writeback, reconciliation, and recovery | [Windows Cloud Files](cloud-files-architecture.md) |
| Indexed-file memory, retrieval, overlays, maintenance, and scope | [Memory](memory-architecture.md) |
| Module identity, compatibility, activation, observation, and replacement | [Module lifecycle](module-lifecycle-architecture.md) |
| Goal revision DAGs, agents, evaluation, promotion, rollback, and risk rings | [Evolution framework](self-evolution-architecture.md) |
| Logs, metrics, traces, profiling, and observation routes | [Observability](observability-architecture.md) |
| Local data-directory ownership and format policy | [Storage](storage-architecture.md) |

The sandbox has its own [architecture document](../crates/sinomo-sandbox/docs/architecture.md).

## Specifications and Reference

- [Sinomo Object format](sinomo-object-format.md) is the normative binary
  encoding specification.
- [SIFS](sifs.md) defines the default cluster filesystem layout and policies.
- [Storage architecture](storage-architecture.md) defines ownership and
  inspection of local data-directory paths.
- [Network data-path reference](developer/network-data-path.md) records the
  current migration-state object-RPC carrier formats and flow-control bounds.
- [Network layer separation](plans/network-layer-separation.md) tracks the
  replacement of the combined carrier with layered contracts inside the single
  `sinomo-network` crate.
- [Cluster formal verification](formal/cluster-verification.md) documents the
  executable Quint specifications and their evidence boundaries.
- Crate-specific operational or protocol references live with the owning crate,
  including `crates/sinomo-dfs/README.md`,
  `modules/dfs/cloud-files/README.md`, `crates/sinomo-daemon/README.md`,
  `crates/sinomo-object/README.md`, and sandbox/toolchain READMEs.

Exact formats, schemas, model bounds, and benchmark protocols may be detailed.
General architecture pages should link to those references instead of copying
their inventories.

## Plans and Research Notes

- [Plans](plans/README.md) contain active roadmaps and selected completed or
  historical records that retain useful reasoning.
- [Research notes](research_notes/README.md) contain literature reviews and
  design evidence.

A plan becomes current behavior only through implementation, tests, and an
updated architecture/user contract. A research recommendation becomes a
project decision only when adopted by architecture or a reviewed implementation.

## Document Status Rules

| Document class | Purpose |
|---|---|
| User guide | A task supported by the current public interface |
| Developer guide | Setup, navigation, extension, validation, evaluation, and reproducibility workflows |
| Architecture | Stable intent, ownership, invariants, and major flows |
| Reference/specification | Exact format, schema, protocol, model, or harness behavior |
| Plan | Future work, sequencing, status, or retained implementation rationale |
| Research note | Literature and design evidence that has not necessarily been adopted |

When updating documentation:

- use current public commands and verify their help;
- label targets and partial implementations instead of presenting them as
  guarantees;
- keep implementation logs out of architecture pages;
- update plan status when implementation lands;
- link to the owner rather than duplicating a contract;
- preserve relative links and run local link/anchor validation;
- never include credentials, invites, private prompts, or private cluster data.
