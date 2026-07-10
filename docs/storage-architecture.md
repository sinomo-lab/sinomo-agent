# Sinomo Storage Architecture

This document defines how Sinomo records ownership and format expectations for
files in the local data directory. It describes the storage contract, not a
catalogue of every current path.

## Purpose

Every durable local file must have an owning subsystem. The storage registry
makes that ownership inspectable so diagnostics and agents can answer:

- which subsystem owns a path;
- which trust ring may read or write it;
- whether the path is an exact contract or an intentionally open subtree;
- which encoding and object schema apply;
- whether an unrecognized file is expected or requires investigation.

`sinomo-storage` owns this vocabulary and the registry mechanics. It does not
own the complete data-directory layout.

## Ownership

The crate that creates or interprets durable state registers its own paths.
Lower-ring layout is composed first, then trusted runtime and application
layers add their contracts through the appropriate facade. This preserves the
trust-ring dependency direction while producing one inspectable layout.

Registration stays next to the owning subsystem. A central crate must not need
to understand every daemon, DFS, agent, provider, or evolution file in order to
validate the layout.

An exact file contract records a relative path, owner, encoding, and schema when
applicable. An escape path declares an open subtree whose children cannot be
enumerated in advance, such as a user workspace or content-addressed store.
Every escape requires a reason and an owner; it is not a blanket exemption from
inspection.

## Format Policy

Internal runtime state is binary-first. SOBJ documents and packed binary logs
provide schema-aware, bounded formats for state that people are not expected to
edit directly.

Human-readable formats are appropriate for:

- user-managed configuration;
- CLI or service output;
- logs, reports, and benchmark artifacts intended for review;
- external tools that require a specific text format;
- workspace content owned by users or agents.

The choice between binary and text follows the file's audience and contract,
not convenience in one call site. Secrets remain node-local and private even
when their transport or provider requires JSON.

## Layout Categories

The local data directory contains several kinds of state with different
ownership rules:

| Category | Contract |
|---|---|
| Runtime identity and daemon state | Exact files or tightly owned daemon subtrees. |
| Human-managed configuration | Explicit configuration documents under the shared config hierarchy. |
| Credentials and tokens | Node-local private files owned by the provider or authentication subsystem. |
| DFS metadata and content | Owned dynamic subtrees registered by DFS. |
| Workspaces and exposed files | Reasoned escape paths because contents are intentionally arbitrary. |
| Evolution jobs and review artifacts | Higher-ring subtrees intended for operator and agent inspection. |
| Installed binaries and managed source | Install and rollout-owned paths with explicit lifecycle rules. |

Cluster-wide configuration belongs in cluster KV or a shared configuration
document according to the owning subsystem. A local file must not quietly become
an alternate source of authority for distributed settings.

## Verification

Verification can check an explicit set of paths or walk a data directory. A
path is covered only when it matches an exact file contract or falls under a
registered escape path. The report separates covered and unregistered files so
operators can investigate drift without losing access to an otherwise usable
node.

Install and cluster diagnostics use the composed runtime layout. Unregistered
paths are health warnings rather than automatic deletion targets or startup
blockers. The owning subsystem decides whether a legacy path may be migrated;
the registry only reports coverage and schema expectations.

Storage verification does not validate domain semantics. A covered Raft log,
DFS manifest, or provider credential can still be corrupt or unauthorized; the
owning subsystem performs those checks.

## Invariants

1. Each durable path has one clear owner.
2. Higher-ring registration does not create lower-ring dependencies.
3. Escape paths are narrow, documented, and owned.
4. Internal binary documents declare their schema or encoding contract.
5. Human-editable configuration is distinguishable from generated runtime
   state.
6. Unregistered files are reported, never silently adopted or deleted.
7. Local storage cannot override cluster authority without an explicit
   subsystem rule.

The [architecture overview](architecture.md#state-and-data) explains how local
storage relates to cluster KV, events, memory, and DFS. The
[SIFS document](sifs.md) describes the cluster filesystem rather than the local
data-directory registry.
