#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
set +m
work=$1
control=$2
expected_parent=$3
shift 3
if [[ $# == 4 && $1 == python3 && $3 == run && ( $4 == store || $4 == runtime ) ]]; then
  : # Original fixed single-full command policy.
elif [[ $# == 4 && $1 == python3 && $3 == run-runtime-shard && ( $4 == 0 || $4 == 1 ) ]]; then
  : # Explicit two-shard successor; no arbitrary executable or filter arguments.
else
  exit 94
fi

# The parent's inherited O_RDWR bootstrap descriptor prevents an open/EOF race.
# Open a read-only endpoint, then close every inherited writer before any child.
exec 3< "$control"
exec 7>&- 8>&- 9>&-
[[ $PPID == "$expected_parent" ]]
IFS= read -r stat < "/proc/$$/stat"
read -r -a fields <<< "${stat##*) }"
[[ ${fields[2]} == "$$" && ${fields[3]} == "$$" ]]
start=${fields[19]}
trap ':' TERM INT HUP
printf '%s %s\n' "$$" "$start" > "$work/leader-identity.tmp"
mv "$work/leader-identity.tmp" "$work/leader-identity"

(
  # Caught signals are reset on exec in the real suite child. The helper stays
  # alive long enough to wait for its true exit status when the group gets TERM.
  trap ':' TERM INT HUP
  if "$@"; then code=0; else code=$?; fi
  printf '%s\n' "$code" > "$work/suite-status.tmp"
  mv "$work/suite-status.tmp" "$work/suite-status"
  exit "$code"
) 3<&- &
helper=$!
printf '%s\n' "$helper" > "$work/helper-pid"

other_group_members() {
  local proc line tail state parent group rest
  for proc in /proc/[0-9]*/stat; do
    [[ "$proc" == "/proc/$$/stat" ]] && continue
    if { IFS= read -r line < "$proc"; } 2>/dev/null; then
      tail=${line##*) }
      read -r state parent group rest <<< "$tail"
      [[ "$group" == "$$" ]] && return 0
    fi
  done
  return 1
}

# No signal handler enters this path: self-TERM cannot recursively clean up.
# EOF also requests cleanup if the monitor dies. Only this still-live leader
# ever signals its own group; the parent never signals a numeric PID/PGID.
if IFS= read -r action <&3; then
  [[ "$action" == cleanup ]] || exit 95
  printf 'parent-cleanup\n' > "$work/cleanup-reason"
else
  printf 'parent-eof-or-signal\n' > "$work/cleanup-reason"
fi
kill -TERM -- "-$$"
deadline=$((SECONDS + 5))
helper_reaped=0
while (( SECONDS < deadline )); do
  # Bash reaps completed async children; wait records only the helper result,
  # never fabricates a missing suite status if it was externally interrupted.
  if [[ $helper_reaped == 0 ]] && ! kill -0 "$helper" 2>/dev/null; then
    if wait "$helper"; then helper_code=0; else helper_code=$?; fi
    printf '%s\n' "$helper_code" > "$work/helper-wait-status"
    helper_reaped=1
  fi
  if [[ $helper_reaped == 1 ]] && ! other_group_members; then
    printf '%s %s\n' "$$" "$start" > "$work/cleanup-complete.tmp"
    mv "$work/cleanup-complete.tmp" "$work/cleanup-complete"
    exit 0
  fi
  sleep 0.05
done
printf 'own-group-KILL; cleanup is not qualified\n' > "$work/forced-kill"
# The sender is still the actual session/group leader during this syscall.
# No code after it may signal this now-retired number again.
kill -KILL -- "-$$"
exit 96
