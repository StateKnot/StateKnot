#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
# Capture happens in Bash before Python. No locale variable is assigned/exported.
set -euo pipefail

fail() { printf '%s\n' "cold locale guard: $*" >&2; exit 64; }

capture() {
  local name
  for name in LANG LC_ALL LC_CTYPE; do
    if [[ "${!name+x}" == x ]]; then
      printf '%s\000%s\000%s\000' "$name" 1 "${!name}"
    else
      printf '%s\000%s\000%s\000' "$name" 0 ''
    fi
  done
}

verify_utf8() {
  local effective listing installed=0 entry charmap
  effective="${LC_ALL:-${LC_CTYPE:-${LANG:-}}}"
  case "$effective" in C.UTF-8|C.utf8) ;; *) fail 'effective LC_CTYPE must already be C.UTF-8 or C.utf8' ;; esac
  listing="$(locale -a)" || fail 'cannot enumerate installed locales'
  while IFS= read -r entry; do
    case "$entry" in C.UTF-8|C.utf8) installed=1 ;; esac
  done <<< "$listing"
  [[ "$installed" == 1 ]] || fail 'C UTF-8 locale is not installed'
  charmap="$(locale charmap)" || fail 'cannot inspect actual locale charmap'
  case "$charmap" in UTF-8|UTF8) ;; *) fail 'actual locale charmap is not UTF-8' ;; esac
  verified_charmap="$charmap"
}

[[ $# -ge 2 ]] || fail 'usage: locale_guard.sh init|check COMMAND [ARG ...]'
mode="$1"
shift
case "$mode" in
  init)
    [[ -z "${STATEKNOT_COLD_LOCALE_PROOF:-}" ]] || fail 'locale proof already initialized'
    [[ -n "${RUNNER_TEMP:-}" && -d "$RUNNER_TEMP" ]] || fail 'missing actual runner temporary directory'
    proof="$(mktemp -d "$RUNNER_TEMP/stateknot-cold-locale.XXXXXXXX")"
    chmod 700 "$proof"
    case "$proof" in *$'\n'*|*$'\r'*) fail 'unsafe proof path' ;; esac
    capture > "$proof/baseline.nul"
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
      printf 'evidence=%s\n' "$proof" >> "$GITHUB_OUTPUT"
    fi
    verify_utf8
    printf '%s\n' "$verified_charmap" > "$proof/charmap.txt"
    export STATEKNOT_COLD_LOCALE_PROOF="$proof"
    [[ -n "${GITHUB_ENV:-}" ]] || fail 'missing GitHub environment output path'
    printf 'STATEKNOT_COLD_LOCALE_PROOF=%s\n' "$proof" >> "$GITHUB_ENV"
    ;;
  check)
    proof="${STATEKNOT_COLD_LOCALE_PROOF:-}"
    [[ -n "$proof" && -d "$proof" && ! -L "$proof" ]] || fail 'missing or linked locale proof'
    [[ -f "$proof/baseline.nul" && ! -L "$proof/baseline.nul" ]] || fail 'missing or linked baseline'
    cmp -s "$proof/baseline.nul" <(capture) || fail 'original locale variables changed'
    verify_utf8
    cmp -s "$proof/charmap.txt" <(printf '%s\n' "$verified_charmap") || fail 'actual locale charmap changed'
    ;;
  *) fail 'unknown mode' ;;
esac
exec "$@"
