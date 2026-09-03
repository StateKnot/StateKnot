#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/../.." && pwd)"
tck_commit="263b9cfaf16a554bdfb166a7ba5b67716e946349"
tck_archive_sha256="694c798e93fff30f650d44bdb3db0e1768b865a4f3ddbed64ec158db209bf5db"
tck_archive_url="https://github.com/a2aproject/a2a-tck/archive/${tck_commit}.tar.gz"
uv_version="0.11.25"
uv_binary="${STATEKNOT_A2A_UV_BIN:-uv}"
server_binary="${STATEKNOT_A2A_CONFORMANCE_BIN:-${repository_root}/target/debug/examples/a2a_conformance_server}"
results_root="${STATEKNOT_A2A_CONFORMANCE_RESULTS:-${script_dir}/results}"
server_port="${STATEKNOT_A2A_CONFORMANCE_PORT:-3400}"
uv_cache="${STATEKNOT_A2A_UV_CACHE:-${TMPDIR:-/tmp}/stateknot-a2a-uv-cache}"

for dependency in cargo curl patch python3 tar "${uv_binary}"; do
  if ! command -v "${dependency}" >/dev/null 2>&1; then
    echo "required command is missing: ${dependency}" >&2
    exit 1
  fi
done

if [[ ! "${server_port}" =~ ^[0-9]+$ ]] || ((server_port < 1 || server_port > 65535)); then
  echo "STATEKNOT_A2A_CONFORMANCE_PORT must be an integer from 1 through 65535" >&2
  exit 1
fi

observed_uv_version="$("${uv_binary}" --version | awk '{print $2}')"
if [[ "${observed_uv_version}" != "${uv_version}" ]]; then
  echo "uv ${uv_version} is required; observed ${observed_uv_version}" >&2
  exit 1
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/stateknot-a2a-tck.XXXXXX")"
tck_dir="${work_dir}/source"
archive="${work_dir}/a2a-tck.tar.gz"
server_pid=""

cleanup() {
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}" 2>/dev/null || true
  fi
  if [[ -n "${server_pid}" ]]; then
    wait "${server_pid}" 2>/dev/null || true
  fi
  case "${work_dir}" in
    "${TMPDIR:-/tmp}"/stateknot-a2a-tck.*) rm -rf -- "${work_dir}" ;;
    *) echo "refusing to remove unexpected work directory: ${work_dir}" >&2 ;;
  esac
}
trap cleanup EXIT INT TERM

if [[ -n "${STATEKNOT_A2A_TCK_ARCHIVE:-}" ]]; then
  cp -- "${STATEKNOT_A2A_TCK_ARCHIVE}" "${archive}"
else
  curl \
    --connect-timeout 15 \
    --fail \
    --location \
    --max-time 90 \
    --proto '=https' \
    --retry 3 \
    --retry-connrefused \
    --show-error \
    --silent \
    --tlsv1.2 \
    --output "${archive}" \
    "${tck_archive_url}"
fi

if command -v sha256sum >/dev/null 2>&1; then
  observed_archive_sha256="$(sha256sum "${archive}" | awk '{print $1}')"
else
  observed_archive_sha256="$(shasum -a 256 "${archive}" | awk '{print $1}')"
fi
if [[ "${observed_archive_sha256}" != "${tck_archive_sha256}" ]]; then
  echo "A2A TCK archive checksum mismatch" >&2
  echo "expected: ${tck_archive_sha256}" >&2
  echo "observed: ${observed_archive_sha256}" >&2
  exit 1
fi

mkdir -p "${tck_dir}" "${uv_cache}"
tar -xzf "${archive}" -C "${tck_dir}" --strip-components=1
patch --directory="${tck_dir}" --strip=1 --forward --batch \
  < "${script_dir}/tck-compat.patch"

cargo build \
  --manifest-path "${repository_root}/Cargo.toml" \
  --package stateknot-integrations \
  --example a2a_conformance_server \
  --locked

run_id="run-$(date -u +%Y%m%dT%H%M%SZ)-${PPID}"
run_dir="${results_root}/${run_id}"
mkdir -p "${run_dir}"
server_log="${run_dir}/server.log"
server_url="http://127.0.0.1:${server_port}"

STATEKNOT_A2A_CONFORMANCE_PORT="${server_port}" \
  "${server_binary}" >"${server_log}" 2>&1 &
server_pid=$!

ready=0
for _attempt in {1..100}; do
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    echo "A2A conformance server exited before readiness" >&2
    sed -n '1,200p' "${server_log}" >&2
    exit 1
  fi
  if curl \
    --fail \
    --silent \
    --show-error \
    --output /dev/null \
    --max-time 1 \
    "${server_url}/.well-known/agent-card.json"; then
    ready=1
    break
  fi
  sleep 0.1
done

if [[ "${ready}" != "1" ]]; then
  echo "A2A conformance server did not become ready" >&2
  sed -n '1,200p' "${server_log}" >&2
  exit 1
fi

set +e
(
  cd "${tck_dir}"
  UV_CACHE_DIR="${uv_cache}" "${uv_binary}" run --frozen ./run_tck.py \
    --sut-host "${server_url}" \
    --transport jsonrpc,http_json \
    -- \
    --webhook-host=127.0.0.1 \
    -rxX
)
tck_status=$?
set -e

if [[ -d "${tck_dir}/reports" ]]; then
  cp -R "${tck_dir}/reports/." "${run_dir}/"
fi
if [[ "${tck_status}" != "0" ]]; then
  echo "official A2A TCK failed with status ${tck_status}" >&2
  echo "A2A conformance evidence: ${run_dir}" >&2
  exit "${tck_status}"
fi

python3 "${script_dir}/verify-results.py" \
  "${run_dir}/junitreport.xml" \
  "${run_dir}/summary.json"
echo "A2A conformance evidence: ${run_dir}"
