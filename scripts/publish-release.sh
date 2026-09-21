#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <version>" >&2
  exit 64
fi

readonly version="$1"
readonly expected_tag="v$version"
readonly -a packages=(
  stateknot-core
  stateknot-store-postgres
  stateknot-runtime
  stateknot-integrations
  stateknot-artifact-store
  stateknot-testkit
  stateknot
)
readonly user_agent='StateKnot release publisher (https://github.com/StateKnot/StateKnot)'

for command in cargo curl git shasum tar; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "missing required command: $command" >&2
    exit 69
  }
done

git diff --quiet --ignore-submodules --
git diff --cached --quiet --ignore-submodules --
if [[ "$(git describe --exact-match --tags HEAD 2>/dev/null || true)" != "$expected_tag" ]]; then
  echo "HEAD must be the exact immutable tag $expected_tag" >&2
  exit 65
fi
git merge-base --is-ancestor HEAD origin/main || {
  echo "$expected_tag is not reachable from origin/main" >&2
  exit 65
}

"$(dirname "$0")/verify-release.sh" "$version"

readonly download_root="$(mktemp -d)"
cleanup() {
  rm -rf -- "$download_root"
}
trap cleanup EXIT

digest() {
  shasum -a 256 "$1" | awk '{print $1}'
}

download_published() {
  local package="$1"
  local destination="$2"
  curl --fail --silent --show-error --location \
    --connect-timeout 10 --max-time 120 \
    --retry 5 --retry-all-errors --retry-delay 2 \
    --user-agent "$user_agent" \
    "https://crates.io/api/v1/crates/$package/$version/download" \
    --output "$destination"
}

validate_archive() {
  local archive="$1"
  local listing="$2"
  local archive_bytes
  archive_bytes="$(wc -c <"$archive" | tr -d '[:space:]')"
  if ((archive_bytes > 10000000)); then
    echo "$archive exceeds the crates.io 10 MB package limit" >&2
    exit 65
  fi
  tar -tzf "$archive" >"$listing"
  for required in LICENSE NOTICE README.md src/lib.rs; do
    grep -Eq "^[^/]+/$required$" "$listing" || {
      echo "$archive does not contain $required" >&2
      exit 65
    }
  done
}

for package in "${packages[@]}"; do
  # Packaging happens immediately before each upload. This is intentional: on
  # a lockstep multi-crate release, Cargo can resolve the current package only
  # after its StateKnot dependencies earlier in this list reach crates.io.
  cargo package --package "$package" --locked
  local_archive="target/package/${package}-${version}.crate"
  [[ -f "$local_archive" ]] || {
    echo "missing package archive: $local_archive" >&2
    exit 66
  }
  validate_archive "$local_archive" "$download_root/${package}.files"
  remote_archive="$download_root/${package}-${version}.crate"
  if download_published "$package" "$remote_archive" 2>/dev/null; then
    if [[ "$(digest "$local_archive")" != "$(digest "$remote_archive")" ]]; then
      echo "published $package $version does not match this tag" >&2
      exit 65
    fi
    echo "$package $version is already published with identical bytes"
    continue
  fi

  cargo publish --package "$package" --locked
  published=0
  for _ in {1..30}; do
    if download_published "$package" "$remote_archive" 2>/dev/null; then
      published=1
      break
    fi
    sleep 10
  done
  if [[ "$published" -ne 1 ]]; then
    echo "$package $version did not become downloadable within five minutes" >&2
    exit 75
  fi
  if [[ "$(digest "$local_archive")" != "$(digest "$remote_archive")" ]]; then
    echo "downloaded $package $version does not match the uploaded archive" >&2
    exit 65
  fi
  echo "published and verified $package $version"
done

consumer_root="$download_root/external-consumer"
mkdir -p "$consumer_root/src"
printf '%s\n' \
  '[package]' \
  'name = "stateknot-release-consumer"' \
  'version = "0.0.0"' \
  'edition = "2024"' \
  'publish = false' \
  '' \
  '[dependencies]' \
  "stateknot = \"=$version\"" \
  >"$consumer_root/Cargo.toml"
printf '%s\n' 'fn main() {}' >"$consumer_root/src/main.rs"
cargo check --manifest-path "$consumer_root/Cargo.toml"

echo "resolved and compiled stateknot $version from a registry-only consumer"
