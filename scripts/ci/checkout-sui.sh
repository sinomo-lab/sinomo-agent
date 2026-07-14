#!/usr/bin/env bash
set -euo pipefail

workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sui_checkout_dir="${SUI_CHECKOUT_DIR:-$(dirname "${workspace_root}")/sui}"
sui_ref="${SUI_REF:-main}"

if [[ -f "${sui_checkout_dir}/crates/sui/Cargo.toml" ]]; then
  echo "Using existing SUI checkout at ${sui_checkout_dir}"
  exit 0
fi

if [[ -n "${SUI_REPOSITORY_URL:-}" ]]; then
  repository_url="${SUI_REPOSITORY_URL}"
elif [[ -n "${CI_SERVER_URL:-}" && -n "${CI_PROJECT_NAMESPACE:-}" && -n "${CI_JOB_TOKEN:-}" ]]; then
  case "${CI_SERVER_URL}" in
    https://*)
      repository_url="https://gitlab-ci-token:${CI_JOB_TOKEN}@${CI_SERVER_URL#https://}/${CI_PROJECT_NAMESPACE}/sui.git"
      ;;
    http://*)
      repository_url="http://gitlab-ci-token:${CI_JOB_TOKEN}@${CI_SERVER_URL#http://}/${CI_PROJECT_NAMESPACE}/sui.git"
      ;;
    *)
      echo "Unsupported CI_SERVER_URL: ${CI_SERVER_URL}" >&2
      exit 1
      ;;
  esac
else
  echo "SUI is missing at ${sui_checkout_dir}. Set SUI_REPOSITORY_URL or run this script in GitLab CI." >&2
  exit 1
fi

mkdir -p "$(dirname "${sui_checkout_dir}")"
git init -q "${sui_checkout_dir}"
git -C "${sui_checkout_dir}" remote add origin "${repository_url}"

if ! git -C "${sui_checkout_dir}" fetch --depth 1 origin "${sui_ref}"; then
  echo "Unable to fetch SUI ref '${sui_ref}'. Ensure the SUI project permits this project to use CI_JOB_TOKEN, or set SUI_REPOSITORY_URL." >&2
  exit 1
fi

git -C "${sui_checkout_dir}" -c advice.detachedHead=false checkout -q --detach FETCH_HEAD
echo "Checked out SUI ${sui_ref} at $(git -C "${sui_checkout_dir}" rev-parse --short HEAD)"
