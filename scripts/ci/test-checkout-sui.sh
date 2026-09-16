#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT

source_dir="${test_root}/source"
checkout_dir="${test_root}/checkout"
git init -q --initial-branch=main "${source_dir}"
git -C "${source_dir}" config user.name "CI Test"
git -C "${source_dir}" config user.email "ci@example.invalid"
git -C "${source_dir}" config commit.gpgsign false
mkdir -p "${source_dir}/crates/sui"
printf '[package]\nname = "sui"\n' > "${source_dir}/crates/sui/Cargo.toml"
git -C "${source_dir}" add .
git -C "${source_dir}" commit -qm "Old package name"
old_revision="$(git -C "${source_dir}" rev-parse HEAD)"

checkout_sui() {
  CI=true SUI_REPOSITORY_URL="file://${source_dir}" SUI_REF="${1:-main}" \
    SUI_CHECKOUT_DIR="${checkout_dir}" bash "${script_dir}/checkout-sui.sh"
}

assert_revision() {
  if [[ "$(git -C "${checkout_dir}" rev-parse HEAD)" != "$1" ]]; then
    echo "SUI checkout did not resolve the requested revision: $1" >&2
    exit 1
  fi
}

checkout_sui
assert_revision "${old_revision}"

printf '[package]\nname = "sinomo-ui"\n' > "${source_dir}/crates/sui/Cargo.toml"
git -C "${source_dir}" commit -qam "Rename package"
new_revision="$(git -C "${source_dir}" rev-parse HEAD)"
# A runner may also retain a remote URL with credentials from an earlier job.
git -C "${checkout_dir}" remote set-url origin "file://${test_root}/expired"
checkout_sui
assert_revision "${new_revision}"
echo "PASS: repeated CI checkout refreshes a stale package"

checkout_sui "${old_revision}"
assert_revision "${old_revision}"
echo "PASS: CI checkout honors an explicit revision"

if checkout_sui missing-ref; then
  echo "CI checkout unexpectedly accepted an unavailable ref" >&2
  exit 1
fi
assert_revision "${old_revision}"
echo "PASS: failed fetch does not silently reuse an old checkout"

printf 'local edits\n' >> "${checkout_dir}/crates/sui/Cargo.toml"
CI=false SUI_REPOSITORY_URL="file://${test_root}/missing" SUI_REF=main \
  SUI_CHECKOUT_DIR="${checkout_dir}" bash "${script_dir}/checkout-sui.sh"
assert_revision "${old_revision}"
if git -C "${checkout_dir}" diff --exit-code --quiet; then
  echo "Local SUI edits were unexpectedly discarded" >&2
  exit 1
fi
echo "PASS: local checkout and edits are preserved outside CI"
