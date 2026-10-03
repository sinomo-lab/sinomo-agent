#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
lint_toolchain=nightly-2026-08-20
lint_version=6.1.0
lint_mode="${1:-all}"
case "$lint_mode" in all|ui|workspace) ;; *) echo "usage: $0 [all|ui|workspace]" >&2; exit 2 ;; esac
if [[ -d "$repo_root/target/dylint-tools/bin" ]]; then
  export PATH="$repo_root/target/dylint-tools/bin:$PATH"
fi
if [[ "$(cargo dylint --version)" != "cargo-dylint $lint_version" ]]; then
  echo "Install cargo-dylint and dylint-link $lint_version; see lints/sinomo-architecture/README.md" >&2
  exit 2
fi
unset CARGO_BUILD_JOBS
export DYLINT_DRIVER_PATH="$repo_root/target/dylint-drivers"
mkdir -p "$DYLINT_DRIVER_PATH" "$repo_root/target/dfs-kv-boundary"
(
  cd lints/sinomo-architecture
  CARGO_TARGET_DIR="$repo_root/target/dylint-libraries" cargo +"$lint_toolchain" build --locked
)
lint_host="$(rustc +"$lint_toolchain" -vV | sed -n 's/^host: //p')"
lint_library="$repo_root/target/dylint-libraries/debug/libsinomo_architecture@$lint_toolchain-$lint_host.so"
if [[ "$lint_mode" != workspace ]]; then
  CARGO_TARGET_DIR="$repo_root/target/dylint-ui" python3 scripts/ci/test-dfs-kv-lint.py \
    --library "$lint_library" --artifacts "$repo_root/target/dfs-kv-boundary/ui"
fi
if [[ "$lint_mode" != ui ]]; then
  CARGO_TARGET_DIR="$repo_root/target/dylint-workspace" cargo dylint \
    --no-metadata --no-deps -p sinomo-dfs --lib-path "$lint_library" -- --lib --locked
fi
