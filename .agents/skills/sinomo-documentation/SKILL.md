---
name: sinomo-documentation
description: Write, review or simplify Sinomo architecture, developer and user documentation. Keep design constraints separate from implementation mechanics and run history; choose the owning source, reference or artifact instead of appending each fix to a guide.
---

# Keep design documents useful

Use this when changing Sinomo documentation, including when a code fix seems to
need an architecture update. The document classes are defined in
[docs/README.md](../../../docs/README.md#document-status-rules). A valid outcome
is no architecture edit. This skill does not require permission for ordinary
documentation work or add a review gate to every code change.

## Keep the edit tied to the request

For each changed passage, identify the specific problem it fixes. If the original
already meets the requested intent, leave it unchanged. Equivalent wording, a
preferred style or broader terminology is not an improvement required by the task.

For a development-log cleanup, remove the log and repair only the resulting gap.
Do not rewrite the introduction, rename headings, reflow unrelated paragraphs or
replace good definitions while doing so. For example, keep “stable intent,
ownership, invariants, and major flows”; adding a requested design constraint
does not justify replacing “intent” with “purpose” or dropping “major flows.”

Review the final diff passage by passage: requested correction, necessary link or
coherence repair, or incidental edit. Undo the incidental edits. A shorter page
or consistent new vocabulary does not justify losing original design language.
If broader restructuring is the actual request, preserve valid text within it
rather than treating the request as permission to rewrite everything.

Useful additional improvements are not automatically forbidden. State their
value and effect separately and keep them independently reviewable. For extended
autonomous work, use [sinomo-change-scope](../sinomo-change-scope/SKILL.md) to bind
that explanation to the actual changed blocks and detect later unreviewed edits.

## Make every document standalone

No development logs in docs, even when the work is important and directly relevant.
This includes undated patch summaries, implementation milestones, investigation
chronology and particular run results. Apply the rule to plans, research notes,
examples and skills as well as architecture.

Development-history dates, dated report filenames, specific Git commits and
agent conversation/session references are signs to reject. Removing these signs
without removing the change narration does not make a document standalone.
Explain the required
behavior, failure case or procedure directly; do not make the reader reconstruct
it from a commit, transcript or previous discussion.

Keep raw provenance in task artifacts outside docs. Replace a dated retrospective
with reusable cases and causal reasoning. Replace fixed commit commands with a
procedure that obtains the required source identity from the owning build/fixture
configuration. Preserve conference/publication years and protocol version values:
they explain the subject and are not development-log provenance. Use placeholders
for incidental sample timestamps and run-specific values.
Do not remove actual application concepts such as session persistence or
transactional commits; those are product behavior, not development citations.

For example, replace a report that a patch added a retry owner with the actual
contract: which component owns an unfinished operation, what survives cancellation,
and how the caller resolves its outcome. Omit the patch story entirely. For a
reusable failure case, state its preconditions, cause and correction directly,
without narrating the investigation that discovered it.

## Decide what the reader needs before choosing a file

An architecture page answers: why does this subsystem exist, what does it own,
where are its trust/consistency boundaries, what behavior must it provide under
normal operation and failure, and what decisions constrain implementations?

Do not use it to answer: which function runs next, which lock is held, how an ACK
updates a cursor, which state field is persisted, how a retry branch now works,
or which tests passed on a particular revision. Removing dates and writing
“must” does not turn implementation narration into architecture.

Before editing architecture, state the design decision that changed. If the
answer is only “we fixed the implementation to satisfy the existing decision,”
keep the architecture unchanged. Put a non-obvious invariant beside the relevant
code and test its behavior. Do not infer a new design decision from a new field,
guard, helper or successful test.

## Route concrete material to its owner

| Material | Destination | Sinomo example |
|---|---|---|
| Purpose, ownership and guarantees | Owning architecture section | KV stores compact configuration; DFS owns file metadata. W is commit durability and N is desired placement. |
| Subtle implementation invariant | Source/Rustdoc and regression test | Replica ingress must not join an unrelated writer rebuild; exact cursor refresh accompanies snapshot installation. |
| Exact interoperability/persistence contract | Existing format/protocol reference or crate documentation | SOBJ encoding; native module ABI; storage conversion contract. |
| Repeatable developer task | Existing developer guide or harness README | How to run a selected native test, preserve fixtures or implement an extension. |
| Implementation-specific debugging knowledge | Relevant skill reference with source pointers | How to identify an admission cycle or distinguish a receipt query from transaction resubmission. |
| Known gap or future work | Issue or scoped plan | The implemented all-participant path does not meet the original outage requirement. |
| One execution's evidence | Gitignored task artifact and concise handoff | Source SHA, failed operation IDs, test counts, profiles, retained DATA and rollout status. |
| Lessons from an investigation | Standalone case explanation; raw history stays in task artifacts | Preconditions, failure mechanism, correction and verification without dates, commit citations or conversation references. |

A dedicated reference is justified by a reader needing an exact contract across
an interface or a reusable task. Do not create one for each fix, copy an entire
implementation inventory into a developer guide, or create a second source of
truth for every private state transition. The source and Git history may be the
right destination for information removed from architecture.

## Rewrite at the right level

Examples for choosing the right abstraction:

- **Keep in architecture:** “Authority transfer preserves acknowledged history
  and prevents the previous owner publishing after its successor takes over.”
  **Keep in source/reference:** the freeze marker filename, writer lock sequence,
  exact ACK fields, prefix comparison and backend-rebind branches.
- **Keep in architecture:** “Optional feature disablement does not revoke durable
  storage promises or block ordinary filesystem operations.” **Keep in the
  feature guide/source:** the module command, service version, retention-field
  schema and cleanup cursor.
- **Keep in architecture:** “Required completion work retains an owner through
  cancellation.” **Keep in source/tests:** which worker flag is cleared, which
  response inbox is used and when the admission guard is released.
- **Keep in architecture:** “Replicated-eventual writes commit at W and repair
  toward N under safe authority.” **Keep outside architecture:** a dated account
  of the current all-participant defect, its commit IDs and the latest test result.

Numbers are not inherently implementation detail: W/N is a user-visible policy
distinction. A captured batch size, current schema number or measured test timing
is normally an implementation/reference or evidence detail. Judge its role, not
just whether it contains a number or symbol.

Two different implementations should be able to satisfy the same architecture
statement. If the paragraph has to change whenever locks, helper functions or
record layouts change, it probably belongs closer to implementation.

## Consolidate without losing the actual constraints

Read the section and its neighbors before appending. For cleanup, outline the
decisions to preserve: ownership, forbidden dependencies, authority, durability,
availability, user-visible semantics and extension boundaries. Compare the result
against that outline; shorter text is not success if W/N or a trust boundary
disappears.

When the requirement changes, replace the owning explanation and its examples.
Do not leave old and new rules side by side with chronological qualifications.
If code conflicts with the requirement, retain the requirement and record the
implementation gap separately. Architecture must not endorse a regression simply
because tests and code now agree on it.

Use an older well-written revision to recover structure and intent, not to restore
obsolete technical claims wholesale. Preserve useful high-level section anchors
and repair links to removed detail. Retain historical evidence in Git/task
artifacts instead of moving all removed paragraphs into another permanent page.

## Verify the result

Read the finished document as a design or task guide, not as a diff. Check that
each section has one clear purpose, examples support a decision, links lead to
the actual owner, and no paragraph merely narrates the latest fix.

Run the existing link checker for public docs and explicitly include any skills
changed, because its default roots exclude `.agents`:

```sh
python3 scripts/docs/check_links.py
python3 scripts/docs/check_links.py .agents/skills
python3 scripts/docs/check_content.py
git diff --check
```

Broken links and prohibited references need correction. Passing the checks does
not establish good abstraction, standalone clarity or preservation of requirements.
Do not impose a mechanical line limit as a substitute for that review.
