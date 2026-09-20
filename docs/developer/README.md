# Developer Guide

This section is the entry point for building and changing Sinomo. It connects
the repository layout to the stable architecture without duplicating the
subsystem specifications.

## Start Here

| Task | Document |
|---|---|
| Prepare a toolchain, build the CLI or app, and run an isolated node | [Development setup](getting-started.md) |
| Understand crate ownership, facades, and dependency rules | [Workspace and trust rings](workspace-and-trust-rings.md) |
| Add a command, service, tool, provider, module, transport, backend, or test | [Extension points](extension-points.md) |
| Understand direct-link, connectivity, routed-stream, and object-RPC contracts | [Network architecture](../network-architecture.md) |
| Understand the current migration-state object-RPC carriers and protocol v1 | [Network data-path reference](network-data-path.md) |
| Integrate explicit file retention, content preservation, and restore | [DFS file history](dfs-file-history.md) |
| Choose unit, integration, formal, cluster, sandbox, and benchmark checks | [Testing and validation](testing.md) |
| Understand research surfaces, evidence classes, and claim boundaries | [Research and evaluation](research-and-evaluation.md) |
| Record a reproducible formal, cluster, benchmark, model, or agent experiment | [Experimental reproducibility](experimental-reproducibility.md) |
| Prepare a reviewable change | [Contributing](../../CONTRIBUTING.md) |

Read the [system overview](../system-overview.md) first if the runtime concepts
are unfamiliar. Read the normative [architecture overview](../architecture.md)
before a change that crosses crates or changes state ownership.

## Repository Map

| Path | Responsibility |
|---|---|
| `crates/sinomo` | Thin binary wrapper; no CLI behavior belongs here. |
| `crates/sinomo-cli` | Command parsing, handlers, daemon operations, and TUI. |
| `crates/sinomo-app` | Graphical application using the same runtime and state. |
| `crates/sinomo-core` | Facade over the complete R0 cluster foundation. |
| `crates/sinomo-runtime` | Facade over R0 and trusted R1 runtime services. |
| `crates/sinomo-*` | Foundation, trusted runtime, cognitive services, and applications. |
| `modules/` | Leaf native modules and opt-in benchmark adapters. |
| `specs/quint/` | Executable formal models, contracts, checks, and scenarios. |
| `docker/` | Multi-node development and validation environments. |
| `scripts/` | CI, formal, debugging, and Android helpers. |
| `docs/` | User guides, architecture, developer material, references, plans, evaluation guidance, and research notes. |

The workspace currently has 68 members. Sixty-one default members build with
plain `cargo build`; platform-specific applications, native module artifacts,
and benchmark packages are opt-in.

## How to Read the Code

When `.codegraph/` exists, begin with a question that names the public command,
type, route, or behavior:

```sh
codegraph explore "How does cluster join reach the daemon and commit membership?"
codegraph explore "AgentTool execute registration and cancellation path"
```

CodeGraph returns current source and call paths, including dynamic-dispatch
hops that text search cannot show. Use `rg` afterward for exact literals,
schemas, tests, or exhaustive references.

For a subsystem change, follow this order:

1. owning architecture document;
2. public contracts and facade exports;
3. production implementation and daemon/application adapters;
4. integration tests and executable specs;
5. plans only for unfinished or historical context.

Plans and research notes are evidence, not authority for current behavior.

## Change Ownership

The usual ownership rule is:

```text
domain crate: types, state machine, validation, invariants
       |
       v
daemon/runtime adapter: process lifecycle, RPC auth, scheduling, persistence host
       |
       v
CLI/app adapter: user intent, display, progress, and errors
```

Do not put a domain transition in the CLI merely because the command first
needs it. Do not put a provider or filesystem policy in the daemon merely
because the daemon hosts it. Stable contracts should remain testable without
starting the complete application.

## Documentation as Part of the Interface

A completed feature normally updates more than Rust:

- public command changes update user guides and help tests;
- ownership or invariant changes update architecture;
- exact encoding changes update the relevant reference and fixtures;
- formal refinements update model classification and Rust conformance evidence;
- unfinished work stays in a plan with an honest status;
- experiments record inputs, configuration, commit, and limitations.

The [documentation index](../README.md) defines the navigation paths. Avoid
turning architecture pages into implementation journals; link to specialist
evidence instead.
