# Contributing to Sinomo

Sinomo welcomes focused fixes, documentation, tests, formal models,
benchmarks, new adapters, and carefully scoped subsystem improvements. The
system crosses distributed storage, networking, policy, agents, media, and
host effects, so a small change can cross an important authority boundary.

This page is the contribution checklist. Use the
[developer guide](docs/developer/README.md) for setup and system navigation.

## Before You Change Code

1. Read [the architecture overview](docs/architecture.md) and the owning
   subsystem document.
2. Check `git status --short --branch` and preserve unrelated work.
3. If `.codegraph/` exists, use `codegraph explore` before text search when
   locating symbols or tracing cross-crate behavior.
4. Identify the owning crate and trust ring before adding a dependency.
5. Decide whether the change affects authority, persistent state, a wire/schema
   contract, cancellation, permissions, placement, or recovery.

The [workspace and trust-ring guide](docs/developer/workspace-and-trust-rings.md)
explains dependency direction. The [extension guide](docs/developer/extension-points.md)
maps common changes to their supported seams.

## Architectural Expectations

- R0 crates depend only on R0. R1 code crosses R0 through `sinomo-core`; R2
  crosses R0/R1 through `sinomo-runtime`.
- `sinomo-network` and `sinomo-kv` remain independent. `sinomo-cluster` is the
  only production bridge between networking and KV/Raft-backed cluster state.
- Domain crates own request meaning and state transitions. Daemon and CLI code
  adapt those contracts to processes, authentication, event loops, and users.
- Cross-service events are typed and attributable. Include provenance,
  evidence, causality, scope, and routing metadata where the domain requires
  them.
- Distributed settings belong in cluster KV. Credentials, remote-management
  consent, and device exposure remain node-local and fail-closed.
- Persistent paths are rooted through `SinomoDataDir` and registered with the
  storage layout. Sandbox filesystems remain VFS/SIFS-backed.
- Discovery, membership, model output, and artifact production never grant
  effect or activation authority by themselves.

Sinomo is pre-1.0. A deliberate breaking change can be appropriate, but it
must update all in-tree callers, tests, examples, schemas, and documentation in
the same change. Do not leave compatibility shims unless the migration itself
is an accepted requirement.

## Implement a Focused Change

- Prefer the smallest owning crate over a new cross-cutting helper.
- Use workspace dependencies instead of crate-local version pins.
- Add unit tests for a local algorithm and integration tests for behavior that
  crosses networking, KV, cluster, DFS, daemon, sandbox, or persistence
  boundaries.
- Preserve stable request/operation identity across retries and cancellation.
- Make failure and restart behavior explicit for durable or distributed work.
- Update the correct documentation class:
  - user guide for a supported task;
  - architecture for a stable boundary or invariant;
  - reference for an exact format or protocol;
  - plan for unfinished delivery work;
  - research note for external evidence or an unadopted design.

Avoid committing generated output from `target/`, `dist/`, toolchain `work/` or
`out/` directories, `_apalache-out/`, or `benchmark-results/`.

## Validate in Proportion to Risk

Start with the smallest relevant check, then broaden. A typical Rust change
uses the commands below; set the package and filter to the affected crate and
test:

```sh
CRATE=sinomo-cluster
TEST_FILTER=your_test_name
cargo test -p "$CRATE" "$TEST_FILTER"
cargo check -p "$CRATE"
cargo test -p sinomo-core --test workspace_ring_boundaries
cargo check --workspace
cargo build -p sinomo
```

Format touched Rust code with `cargo fmt --all`. Run Clippy, formal checks,
real-cluster harnesses, WASIX verifiers, or benchmarks when the affected
boundary requires them. The [testing guide](docs/developer/testing.md) maps
changes to those gates.

Current GitLab CI builds locked cross-platform CLI release artifacts on `main`.
It does not replace local tests, formatting, linting, formal verification, or
real-cluster validation.

Finish every contribution with:

```sh
python3 scripts/docs/check_links.py
git diff --check
git status --short --branch
```

If a broad check fails in unrelated existing code, report that separately and
show that the touched surface passes its focused check. Do not hide a relevant
failure behind unrelated baseline drift.

## Submission Notes

A reviewable change explains:

- the user or system problem and why the owning boundary is correct;
- architecture and trust-ring impact;
- authority, privacy, storage, cancellation, and recovery impact;
- wire, schema, file-format, and migration impact;
- tests and formal checks run, with exact commands and outcomes;
- cluster or benchmark environment when performance or distributed behavior is
  part of the claim;
- remaining limitations and intentionally deferred work.

Keep commits focused enough that reviewers can distinguish behavior, evidence,
and mechanical migration. The repository does not prescribe a branch name or
commit-message convention; prefer a concise message that states the completed
outcome.
