#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

usage() {
  echo "usage: $0 [--allow-dirty] <version>" >&2
  exit 64
}

allow_dirty=0
if [[ "${1:-}" == "--allow-dirty" ]]; then
  allow_dirty=1
  shift
fi
[[ $# -eq 1 ]] || usage
readonly expected_version="$1"

readonly -a published_packages=(
  stateknot-core
  stateknot-store-postgres
  stateknot-runtime
  stateknot-integrations
  stateknot-artifact-store
  stateknot-testkit
  stateknot
)
readonly -a bootstrap_packages=(stateknot-core stateknot-testkit)
for command in cargo git jq tar; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "missing required command: $command" >&2
    exit 69
  }
done

if [[ "$allow_dirty" -eq 0 ]]; then
  git diff --quiet --ignore-submodules -- || {
    echo "release verification requires a clean worktree" >&2
    exit 65
  }
  git diff --cached --quiet --ignore-submodules -- || {
    echo "release verification requires a clean index" >&2
    exit 65
  }
fi

readonly staging_root="$(mktemp -d)"
cleanup() {
  rm -rf -- "$staging_root"
}
trap cleanup EXIT

readonly metadata="$staging_root/metadata.json"
cargo metadata --format-version 1 --locked --no-deps >"$metadata"

readonly workspace_version="$(
  jq -er '.packages[] | select(.name == "stateknot-core") | .version' "$metadata"
)"
if [[ "$workspace_version" != "$expected_version" ]]; then
  echo "expected version $expected_version, found $workspace_version" >&2
  exit 65
fi

for package in "${published_packages[@]}"; do
  jq -e --arg package "$package" --arg version "$workspace_version" '
    any(.packages[];
      .name == $package and
      .version == $version and
      .publish == ["crates-io"]
    )
  ' "$metadata" >/dev/null || {
    echo "$package is not an exact crates.io release member at $workspace_version" >&2
    exit 65
  }
done

jq -e --arg version "$workspace_version" '
  [
    .packages[].dependencies[]
    | select(.path != null)
    | .req
  ]
  | all(. == ("=" + $version))
' "$metadata" >/dev/null || {
  echo "every workspace path dependency must pin the exact release version" >&2
  exit 65
}

package_args=(--locked --list)
if [[ "$allow_dirty" -eq 1 ]]; then
  package_args+=(--allow-dirty)
fi

for package in "${published_packages[@]}"; do
  listing="$staging_root/${package}.files"
  cargo package --package "$package" "${package_args[@]}" >"$listing"
  for required in LICENSE NOTICE README.md; do
    grep -Fxq "$required" "$listing" || {
      echo "$package does not include $required" >&2
      exit 65
    }
  done
  grep -Fxq "src/lib.rs" "$listing" || {
    echo "$package does not include src/lib.rs" >&2
    exit 65
  }

  crate_dir="$(jq -er --arg package "$package" '
    .packages[]
    | select(.name == $package)
    | .manifest_path
    | sub("/Cargo.toml$"; "")
  ' "$metadata")"
  source_bytes=0
  while IFS= read -r path; do
    case "$path" in
      .cargo_vcs_info.json|Cargo.lock|Cargo.toml|Cargo.toml.orig) continue ;;
    esac
    source_path="$crate_dir/$path"
    if [[ ! -f "$source_path" ]]; then
      source_path="$PWD/$path"
    fi
    [[ -f "$source_path" ]] || {
      echo "$package lists missing source file: $path" >&2
      exit 66
    }
    bytes="$(wc -c <"$source_path" | tr -d '[:space:]')"
    source_bytes=$((source_bytes + bytes))
  done <"$listing"
  if ((source_bytes > 10000000)); then
    echo "$package exceeds 10 MB before compression" >&2
    exit 65
  fi
done

cargo check --workspace --lib --bins --examples --locked
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps --locked

bootstrap_package_args=(--locked)
if [[ "$allow_dirty" -eq 1 ]]; then
  bootstrap_package_args+=(--allow-dirty)
fi
for package in "${bootstrap_packages[@]}"; do
  cargo package --package "$package" "${bootstrap_package_args[@]}"
  archive="target/package/${package}-${workspace_version}.crate"
  [[ -f "$archive" ]] || {
    echo "missing package archive: $archive" >&2
    exit 66
  }
  archive_bytes="$(wc -c <"$archive" | tr -d '[:space:]')"
  if ((archive_bytes > 10000000)); then
    echo "$archive exceeds the crates.io 10 MB package limit" >&2
    exit 65
  fi
  tar -xzf "$archive" -C "$staging_root"
  RUSTDOCFLAGS=-Dwarnings cargo doc \
    --manifest-path "$staging_root/${package}-${workspace_version}/Cargo.toml" \
    --no-deps --locked
done

echo "verified StateKnot release sources and bootstrap archives at $workspace_version"
