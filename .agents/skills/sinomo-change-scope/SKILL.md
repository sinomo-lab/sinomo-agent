---
name: sinomo-change-scope
description: Make Sinomo changes explicit during extended autonomous work, refactors and cross-boundary fixes. Capture requirements and existing work, account for the actual diff, surface additional improvements and contract changes, and reject stale or incomplete scope reviews.
---

# Make changes explicit before they accumulate

Use for extended autonomous work, multi-part changes and changes crossing an
architecture boundary. A directly requested one-line correction can use the same
reasoning in its ordinary diff summary without a separate artifact workflow.
This skill supports useful improvements; it does not require reverting every
change or asking permission for each implementation step.

## Keep intent stable and decisions visible

Capture the requested result and the constraints that must remain true. Separate
these from your current plan for implementing them. A failure in the chosen
implementation does not silently change the requirements.

Distinguish four bases for an edit:

| Basis | Meaning |
|---|---|
| `requested` | Directly implements a stated requirement. |
| `necessary` | A supporting change needed for that requirement, such as an affected caller, test or link. Explain the dependency. |
| `additional` | A useful improvement beyond the request. State its value and impact separately. Related subject matter alone is not justification. |
| `external` | A concurrent change owned by someone else. Identify the owner/context and preserve it. This does not make it part of your submission. |

Also state whether each edit preserves, restores or changes the contract. A fix
restoring W/N availability is different from deciding every participant must be
online. A wording substitution can preserve meaning while still being additional
work with too little value to justify including it.

Make material additional work and new design decisions visible before depending
on them during autonomous work. Keep them independently reviewable. Existing
authorization remains valid; a genuinely unsettled change to the user's intent
needs an explicit decision, while routine implementation does not need repeated
approval. Omit marginal incidental edits instead of creating a permission question
for them. Beneficial scope changes are allowed to be deliberate, not hidden.

## Capture a baseline before substantive edits

The [scope tool](../../../scripts/review/check_scope.py) records the task
requirements, planned paths and the existing working state. It compares changes
against that state, so initial dirty work is not presented as your new work.
Artifacts must be ignored or outside the checkout; the tool does not stage or
commit anything.

Example for a documentation task; replace the requirements and paths with the
actual task, not a generic permission to improve the repository:

```bash
SCOPE_DIR=target/change-scope/documentation-task
python3 scripts/review/check_scope.py capture \
  --path docs/dfs-architecture.md \
  --requirement cleanup='Remove development logs and implementation detail from architecture' \
  --requirement preserve='Preserve valid architecture wording, requirements and constraints' \
  --output "$SCOPE_DIR/baseline.json"
```

Include intended new files/directories in planned paths. Tracked files outside
those paths remain observable, and unexpected new paths are surfaced. Existing
untracked files outside the planned paths are fingerprinted without archiving
their contents; capture them in scope before editing if they become your work.
Generated/ignored runtime data is not a source-code review target. Submodules need
their own review scope.

Do not recapture after editing to make earlier changes disappear. If no starting
snapshot exists, disclose that limit and review the available diff directly.
A baseline for a later phase only covers that phase. Preserve the original
requirements; record new user direction explicitly rather than rewriting them
after the fact to match the patch.

## Account for the actual diff

```bash
python3 scripts/review/check_scope.py prepare \
  --snapshot "$SCOPE_DIR/baseline.json" \
  --output "$SCOPE_DIR/review.json"
```

The generated JSON includes each changed block, its path, old/new text, source
identity and whether the path was planned. It uses contiguous edit blocks rather
than whole files, so a small nearby replacement is not silently covered by a
large file-level summary. A completely rewritten block still requires reviewing
its individual semantic changes; the tool cannot infer them for you.

Fill only the declaration fields:

- `basis`: one of the four categories above.
- `requirement`: the captured requirement ID for requested/necessary work.
- `reason`: what problem this particular change addresses. “Cleanup” or the
  filename is not an explanation.
- `intent_effect`: `preserves`, `restores`, `changes`, or `external`.
- `decision`: required for additional work or restored/changed contracts. State
  the concrete before/after behavior, value and decision basis; distinguish an
  existing instruction from a proposal.
- `scope_reason`: required for paths outside the plan, including concurrent work.

Do not alter the generated diff, IDs or source identities. Do not classify
optional rewording as necessary merely because it shares a hunk with a real fix.
If one block contains both required work and an incidental edit, remove the
incidental edit or make the mixed decision explicit; do not hide it under the
dominant task. Never invent authorization in the declaration.

## Check at milestones and before handoff

```bash
python3 scripts/review/check_scope.py check \
  --snapshot "$SCOPE_DIR/baseline.json" \
  --review "$SCOPE_DIR/review.json" \
  --report "$SCOPE_DIR/REVIEW.md"
```

The command fails on missing classifications, unaccounted blocks, altered diff
copies, unexplained path expansion and stale reviews after source changes. It
also requires decision text for additional work and contract changes. The report
places additional work separately and shows the actual source changes, including
unclassified blocks. Binary/mode changes remain visible by identity.

After further edits, generate a fresh review/report file. Reuse explanations only
after checking their actual changed blocks; do not suppress newly appeared changes.
Before committing or handing off, provide the report and separately identify
additional improvements, contract changes and external work. A passing test suite
does not replace this scope accounting.

The tool enforces coverage and freshness of declarations. It cannot prove that a
classification is truthful, that an algorithm preserves intent, or that the user
approved a proposal. A passing scope check is **not** semantic approval. The
skill's requirement/decision review and the visible actual diff supply that
judgment; do not turn the check into a ceremonial justification of every edit.
