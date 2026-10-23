# Repository Instructions

## Before changing code

- Read `docs/architecture.md` before non-trivial work; its boundaries are load-bearing.
- Check `git status` first. Preserve unrelated changes and never commit generated artifacts (`target/`, toolchain `work/` or `out/`, or `benchmark-results/`).

## Change scope

- Keep requirements distinct from the implementation plan. Classify edits as
  requested work, necessary support, additional improvements or external work.
  Preserve valid wording/behavior unless there is a stated reason to change it.
- Additional improvements can be useful. Make their value and impact explicit
  and independently reviewable; do not hide them in a larger required change.
  Surface changes to intent or contracts before depending on them, honoring
  decisions already made instead of repeatedly asking for routine approval.
- For extended autonomous work, refactors and cross-boundary fixes, use
  [sinomo-change-scope](.agents/skills/sinomo-change-scope/SKILL.md): capture the
  starting state and requirements, then check the actual diff before handoff.
  Missing or stale scope accounting is not a completed review. The check enforces
  disclosure, not semantic correctness or authorization.

## Architecture invariants

- Every crate declares a trust ring in `Cargo.toml`; enforcement lives in `crates/sinomo-core/tests/workspace_ring_boundaries.rs`.
- R0 crates depend only on R0. R1 code crosses R0 through `sinomo_core::...`. R2 and application code crosses R0/R1 through `sinomo_runtime::...`. ABI exceptions are documented in the boundary test.
- Keep `sinomo-network` and `sinomo-kv` independent; only `sinomo-cluster` bridges them.
- Normal DFS operations use existing configuration/network projections. Dylint
  rejects implicit KV access; genuine global consistency and configuration reads
  use narrow, documented boundary functions. Do not duplicate consensus or
  connectivity logic to avoid the lint. See `lints/sinomo-architecture/README.md`.
- Events are the cross-service contract. Use typed, dotted event names and include provenance, evidence, causality, and routing metadata as applicable.
- Do not bypass Sinomo networking, storage, policy, or authority paths. Distributed settings belong in cluster KV; node-local consent stays local and fail-closed. Treat sandbox filesystems as VFS/SIFS-backed.

## Workspace conventions

- Crates live under `crates/`. `crates/sinomo` is only the binary wrapper; CLI behavior belongs in `crates/sinomo-cli/src/lib.rs`. `crates/sinomo-app` is the GUI.
- Use workspace dependencies (`dependency.workspace = true`) instead of crate-local version pins.
- State paths are owned by `sinomo_local::SinomoDataDir` (`SINOMO_DATA_DIR`, default `~/.sinomo`). Without `--repo`, `sinomo evolve propose` targets its managed source checkout, not the operator checkout.
- Prefer integration tests for cross-crate networking, KV, DFS, and sandbox behavior. Use real verifier scripts for WASIX/toolchain behavior.

## Documentation boundaries

- No development logs in documentation, regardless of relevance or importance.
  This includes undated change narration, implementation milestones and run results.
  Docs explain the system, requirements, reasoning and reusable procedures directly.
- Development-history dates, dated report filenames, concrete Git commit
  references and agent conversation/session histories are prohibited indicators
  of that broader problem; removing those markers alone does not fix a dev log.
  State the relevant requirement, mechanism or procedure directly. These rules
  also apply to plans, research notes, examples and skill documentation.
  Preserve meaningful facts such as conference/publication years and protocol
  version identifiers; they are not development-log provenance.
- Architecture docs state stable intent, ownership, invariants, design constraints
  and major flows. Most bug fixes do not change them.
- Do not append ACK sequences, lock ordering, state-field updates, retry branches,
  schema versions, implementation inventories, commit IDs or validation results
  to architecture pages. Exact behavior belongs beside its source/tests or in an
  existing dedicated protocol/reference document when readers need that contract.
- Developer/user guides explain reusable tasks, not the current debugging session.
  Keep run output and handoffs in task artifacts; preserve history through Git.
- A current implementation restriction does not supersede a stated requirement.
  Keep implementation gaps/status outside the architectural contract.
- When editing docs, apply
  [sinomo-documentation](.agents/skills/sinomo-documentation/SKILL.md).
  Consolidate the owning section instead of adding a paragraph for each fix.
- Run `python3 scripts/docs/check_content.py` and the link checker for doc changes.
  The content check rejects recognizable development-log markers, commit citations
  and agent-session provenance; review narration the mechanical check cannot infer.

## Common checks

```bash
cargo build -p sinomo
cargo run -p sinomo -- <args>
cargo test -p <crate> <test-name>
cargo test -p sinomo-core --test workspace_ring_boundaries
bash scripts/ci/check-dfs-kv-boundary.sh
cargo check -p sinomo-sandbox-wasix-module --features wasix-runtime
cargo fmt --all
cargo clippy --workspace --all-targets
```

Run the smallest relevant checks while iterating, then broaden validation in proportion to the change.

## Performance analysis and reporting

For DFS or cluster performance work, read and apply
[sinomo-performance-analysis](skills/sinomo-performance-analysis/SKILL.md).
Use its standard acceptance, per-node stage/I/O, node-window and coverage tables
for every DFS performance report. Docker gates run with debugging, tracing and
profiling enabled at unchanged thresholds. Use measured timings directly without
subtracting observation overhead, and report unavailable measurements explicitly.
