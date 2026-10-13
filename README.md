# Sinomo

Sinomo is a local-first, distributed runtime for continuously operating AI
systems. It brings agents, models, durable memory, tools, shared files,
perception, devices, and controlled software updates into one explicit trust
and authority model.

A single machine is a complete one-node Sinomo cluster. Additional trusted
devices extend the same runtime while keeping data placement, device ownership,
privacy, availability, and final effect authority explicit.

Sinomo is an active pre-1.0 system. Its low-level cluster, network, KV/Raft,
storage, DFS/SIFS, daemon, module, sandbox, and rollout foundations are
implemented and the overall architecture is stable enough to document as a
system. Higher-level agent, memory, multimodal, placement, evaluation, and
self-evolution behavior continues to evolve.

## What Sinomo Provides

- a CLI, terminal interface, and graphical app over the same local runtime;
- durable model-backed agents with tools, skills, sessions, cancellation,
  steering, context assembly, and compaction;
- explicit local versus authoritative cluster state backed by reactive Raft;
- routed typed object RPC, service discovery, and direct or relayed transports;
- SIFS, a shared versioned filesystem with per-object placement policy;
- indexed-file memory with inspectable source evidence and rebuildable indexes;
- node-local policy and permission boundaries for host, device, and remote
  effects;
- sandbox, module, build, evaluation, rollout, activation, and recovery paths;
- structured operational events, logs, metrics, traces, profiling, integration
  harnesses, and executable Quint specifications.

Sinomo is the runtime around model providers; it is not itself a hosted model
service or a general-purpose distributed database.

## Quick Start from Source

The workspace requires Rust 1.95 or newer and Git.

Sinomo stores local state under `~/.sinomo` by default. To create a separate
persistent installation, choose and export `SINOMO_DATA_DIR` before running
`install`:

```sh
export SINOMO_DATA_DIR="$HOME/.sinomo-dev"
```

From the repository root, build and install:

`install` is a deployment transaction, not a read-only setup probe. On an
existing installation it may stop daemons, replace installed binaries, advance
managed source from the current checkout, build and install native modules,
rewrite boot entries, and restart processes. Verify the selected data
directory, repository, commit, and expected outage before running it.

```sh
cargo build -p sinomo
cargo run -p sinomo -- install
cargo run -p sinomo -- cluster init --name workstation
cargo run -p sinomo -- cluster health
```

Run `cluster init` only for a new data directory. An existing installation
should start with `sinomo cluster status` instead.

Authenticate a model provider, choose one of its available models, then open
the terminal interface:

```sh
PROVIDER=your-provider-id
MODEL=your-model-id
sinomo providers auth "$PROVIDER"
sinomo providers test "$PROVIDER"
sinomo models list --provider "$PROVIDER"
sinomo models default --provider "$PROVIDER" --model "$MODEL"
sinomo
```

The complete [getting-started guide](docs/user/getting-started.md) explains
daemon lifecycle, provider choices, the graphical app, health checks, and safe
next steps.

## System Shape

```text
inputs and devices
       |
       v
typed events and object requests
       |
       v
reactive / interactive / deliberative control
       |
       +---- agents, models, memory, tools, actions
       |
       v
versioned files, durable evidence, and authorized effects

shared foundation:
identity + cluster KV + routing + services + DFS/SIFS + policy + rollout
```

Discovery never grants trust, model output never grants tool authority, and
building an artifact never grants activation authority. The node that owns a
device, secret, file, or host capability performs the final check.

Read the [system overview](docs/system-overview.md) for the complete mental
model and [architecture](docs/architecture.md) for normative boundaries.

## Documentation

| Audience | Start here |
|---|---|
| Users and operators | [User guide](docs/user/README.md) |
| Developers | [Developer guide](docs/developer/README.md), including research and evaluation workflows |
| Potential contributors | [Contributing](CONTRIBUTING.md) |
| Everyone | [Documentation index](docs/README.md) and [glossary](docs/glossary.md) |

Subsystem architecture, exact formats, formal verification, active plans, and
literature notes are indexed in [docs/README.md](docs/README.md).

## Repository Layout

| Path | Purpose |
|---|---|
| `crates/` | R0 foundation, R1 trusted runtime, R2 services, and applications |
| `modules/` | Native extension modules and opt-in benchmark adapters |
| `docs/` | Audience guides, architecture, references, plans, evaluation guidance, and research notes |
| `specs/quint/` | Executable protocol models, contracts, checks, and scenarios |
| `docker/` | Container images and multi-node validation harnesses |
| `lints/` | Dylint architecture lints, including the DFS/KV access boundary |
| `distribution/` | Distribution profile that enumerates the default external modules |
| `scripts/` | CI, formal, debugging, and platform helpers |
| `skills/`, `.agents/skills/` | Contributor workflow skills for performance analysis, components, protocols, Docker verification, and rollout |

Every crate declares a trust ring in its manifest. The workspace boundary test
enforces dependency direction and the separation between networking, KV, and
the cluster bridge. See [workspace and trust rings](docs/developer/workspace-and-trust-rings.md).

## Contributing

Start with [CONTRIBUTING.md](CONTRIBUTING.md). Read the owning architecture
document before a cross-crate change, use CodeGraph first when the checkout is
indexed, add evidence at the right layer, and keep generated artifacts and
credentials out of commits.

## License

Workspace metadata declares dual licensing under MIT or Apache-2.0.
