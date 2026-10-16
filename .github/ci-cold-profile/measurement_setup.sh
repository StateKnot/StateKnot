#!/usr/bin/env bash
set -euo pipefail
if [[ "${GITHUB_EVENT_NAME:-}" != workflow_dispatch || "${GITHUB_REF:-}" != refs/heads/codex/* ]]; then
  printf '%s\n' 'Cold measurement requires an isolated codex branch manual dispatch.' >&2
  exit 78
fi
location=.github/ci-cold-profile
bash "$location/locale_guard.sh" check python3 "$location/runtime_controls.py"
bash "$location/locale_guard.sh" check python3 "$location/cold_probe.py" setup
