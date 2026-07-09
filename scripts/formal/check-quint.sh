#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

verify_steps="${QUINT_VERIFY_MAX_STEPS:-5}"
run_steps="${QUINT_RUN_MAX_STEPS:-20}"
run_samples="${QUINT_RUN_MAX_SAMPLES:-50}"

quint typecheck specs/quint/raft_kv.qnt
quint run specs/quint/raft_kv.qnt \
  --main=raft_kv \
  --invariant=safety \
  --max-steps="$run_steps" \
  --max-samples="$run_samples" \
  --verbosity=1
quint verify specs/quint/raft_kv.qnt \
  --main=raft_kv \
  --invariant=safety \
  --max-steps="$verify_steps" \
  --verbosity=1
