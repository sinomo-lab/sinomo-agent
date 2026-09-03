---
name: evolution-review
description: Review evolution candidates for scope, evidence, and safe rollout readiness.
---

# Evolution Review

Use this skill before declaring an evolution candidate ready for supervised rollout.

- Compare the final diff with the mutation packet and reject unrelated scope.
- Verify the narrow checks first, then the architecture and trust-ring checks affected by the change.
- Inspect failures as evidence; do not hide, weaken, or relabel a failing gate.
- Confirm generated artifacts and runtime state were not committed as source.
- Summarize changed behavior, verification results, residual risks, and rollback boundaries.
