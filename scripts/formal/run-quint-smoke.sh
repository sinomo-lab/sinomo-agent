#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

expected_quint_version="${QUINT_EXPECTED_VERSION:-0.32.0}"
actual_quint_version="$(quint --version)"
if [[ "$actual_quint_version" != "$expected_quint_version" && "${QUINT_ALLOW_VERSION_MISMATCH:-0}" != "1" ]]; then
  echo "error: quint $expected_quint_version is required, found $actual_quint_version" >&2
  exit 1
fi

run_steps="${QUINT_SYSTEM_SMOKE_RUN_MAX_STEPS:-160}"
run_samples="${QUINT_SYSTEM_SMOKE_RUN_MAX_SAMPLES:-5000}"

quint typecheck specs/quint/models/system/base.qnt
quint typecheck specs/quint/contracts/system/guarantees.qnt
quint typecheck specs/quint/scenarios/system/smoke.qnt

args=(
  quint run specs/quint/scenarios/system/smoke.qnt
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
