#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

run_steps="${QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS:-160}"
run_samples="${QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES:-5000}"

quint typecheck specs/quint/system_base.qnt
quint typecheck specs/quint/system_guarantees.qnt
quint typecheck specs/quint/system_smoke.qnt

args=(
  quint run specs/quint/system_smoke.qnt
  --main=system_smoke
  --invariant=safety
  --max-steps="$run_steps"
  --max-samples="$run_samples"
  --verbosity=1
)

if [[ -n "${QUINT_SYSTEM_SMOKE_SEED:-}" ]]; then
  args+=(--seed "$QUINT_SYSTEM_SMOKE_SEED")
fi

"${args[@]}"
