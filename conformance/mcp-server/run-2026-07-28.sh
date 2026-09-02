#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/../.." && pwd)"
runner="${repository_root}/conformance/mcp-client/node_modules/.bin/conformance"
server_binary="${STATEKNOT_MCP_SERVER_CONFORMANCE_BIN:-${repository_root}/target/debug/examples/mcp_conformance_server}"
results_root="${STATEKNOT_MCP_SERVER_CONFORMANCE_RESULTS:-${script_dir}/results}"
server_port="${STATEKNOT_MCP_SERVER_CONFORMANCE_PORT:-8011}"
protocol_version="2026-07-28"

if [[ ! -x "${runner}" ]]; then
  echo "pinned MCP conformance runner is missing; run npm ci in conformance/mcp-client" >&2
  exit 1
fi

cargo build \
  --manifest-path "${repository_root}/Cargo.toml" \
  --package stateknot-integrations \
  --example mcp_conformance_server \
  --locked

run_id="run-$(date -u +%Y%m%dT%H%M%SZ)-${PPID}"
run_dir="${results_root}/${run_id}"
mkdir -p "${run_dir}"
server_log="${run_dir}/server.log"
server_url="http://127.0.0.1:${server_port}/mcp"

STATEKNOT_MCP_SERVER_PORT="${server_port}" "${server_binary}" >"${server_log}" 2>&1 &
server_pid=$!

cleanup() {
  if kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}" 2>/dev/null || true
  fi
  wait "${server_pid}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

ready=0
for _attempt in {1..100}; do
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    echo "MCP conformance server exited before readiness" >&2
    sed -n '1,200p' "${server_log}" >&2
    exit 1
  fi
  if curl --silent --show-error --output /dev/null --max-time 1 "${server_url}"; then
    ready=1
    break
  fi
  sleep 0.1
done

if [[ "${ready}" != "1" ]]; then
  echo "MCP conformance server did not become ready" >&2
  sed -n '1,200p' "${server_log}" >&2
  exit 1
fi

"${runner}" server \
  --url "${server_url}" \
  --requirements "${protocol_version}" \
  --output-dir "${run_dir}"

node "${script_dir}/verify-results.mjs" "${run_dir}"
echo "MCP server conformance evidence: ${run_dir}"
