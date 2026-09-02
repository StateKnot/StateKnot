#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/../.." && pwd)"
runner="${script_dir}/node_modules/.bin/conformance"
client_binary="${STATEKNOT_MCP_CONFORMANCE_BIN:-${repository_root}/target/debug/examples/mcp_conformance_client}"
results_root="${STATEKNOT_MCP_CONFORMANCE_RESULTS:-${script_dir}/results}"
protocol_version="2026-07-28"

if [[ ! -x "${runner}" ]]; then
  echo "pinned MCP conformance runner is missing; run npm ci in ${script_dir}" >&2
  exit 1
fi

cargo build \
  --manifest-path "${repository_root}/Cargo.toml" \
  --package stateknot-integrations \
  --example mcp_conformance_client \
  --locked

run_id="run-$(date -u +%Y%m%dT%H%M%SZ)-${PPID}"
run_dir="${results_root}/${run_id}"
mkdir -p "${run_dir}"

scenarios=(
  tools_call
  request-metadata
  http-standard-headers
  http-custom-headers
  http-invalid-tool-headers
  json-schema-ref-no-deref
  sep-2322-client-request-state
)

for scenario in "${scenarios[@]}"; do
  "${runner}" client \
    --command "${client_binary}" \
    --scenario "${scenario}" \
    --spec-version "${protocol_version}" \
    --timeout 30000 \
    --output-dir "${run_dir}/${scenario}"
done

node "${script_dir}/verify-results.mjs" "${run_dir}"
echo "MCP conformance evidence: ${run_dir}"
