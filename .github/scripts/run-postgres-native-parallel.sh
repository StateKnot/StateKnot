#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
set +m
(( BASH_VERSINFO[0] > 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) ))
out=postgres-native-parallel
[[ -d "$out" && ! -L "$out" ]]
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
leader=$script_dir/postgres-native-leader.sh
binary_helper=$script_dir/postgres-native-binaries.py
declare -A worker_pid worker_start worker_state worker_fd suite_code
worker_fd[store]=8
worker_fd[runtime]=9
worker_state[store]=unstarted
worker_state[runtime]=unstarted

identity_alive() {
  local role=$1 stat
  local -a fields
  [[ ${worker_pid[$role]:-} && ${worker_start[$role]:-} ]] || return 1
  if ! IFS= read -r stat 2>/dev/null < "/proc/${worker_pid[$role]}/stat"; then return 1; fi
  read -r -a fields <<< "${stat##*) }"
  [[ ${fields[0]} != Z && ${fields[0]} != X && ${fields[2]} == "${worker_pid[$role]}" && ${fields[3]} == "${worker_pid[$role]}" && ${fields[19]} == "${worker_start[$role]}" ]]
}

bind_identity() {
  local role=$1 deadline actual_pid actual_start
  deadline=$((SECONDS + 8))
  until [[ -f "$out/$role-owner/leader-identity" ]]; do
    (( SECONDS < deadline )) || return 1
    kill -0 "${worker_pid[$role]}" 2>/dev/null || return 1
    sleep 0.05
  done
  read -r actual_pid actual_start < "$out/$role-owner/leader-identity" || return 1
  [[ "$actual_pid" == "${worker_pid[$role]}" && "$actual_start" =~ ^[0-9]+$ ]] || return 1
  worker_start[$role]=$actual_start
  identity_alive "$role"
}

group_present_readonly() {
  local wanted=$1 proc stat state parent group rest
  for proc in /proc/[0-9]*/stat; do
    if IFS= read -r stat 2>/dev/null < "$proc"; then
      read -r state parent group rest <<< "${stat##*) }"
      [[ "$group" == "$wanted" ]] && return 0
    fi
  done
  return 1
}

record_status() {
  local role=$1 code
  [[ -f "$out/$role-owner/suite-status" ]] || return 1
  IFS= read -r code < "$out/$role-owner/suite-status" || return 1
  [[ "$code" =~ ^[0-9]{1,3}$ ]] && (( 10#$code <= 255 )) || return 1
  if [[ -z ${suite_code[$role]:-} ]]; then
    suite_code[$role]=$code
    printf '%s\t%s\n' "$role" "$code" >> "$out/suite-exits.tsv"
  fi
}

retire() {
  local role=$1 reason=$2 deadline result=0 code
  [[ ${worker_state[$role]} == active ]] || return 0
  if [[ -z ${worker_start[$role]:-} ]]; then bind_identity "$role" || true; fi
  if ! identity_alive "$role"; then
    printf '%s\tleader-lost; no numeric PID/PGID signal\n' "$role" >> "$out/ownership-errors.tsv"
    worker_state[$role]=lost
    return 1
  fi
  printf '%s\t%s\n' "$role" "$reason" >> "$out/retire-requests.tsv" || return 1
  printf 'cleanup\n' >&"${worker_fd[$role]}" || return 1
  deadline=$((SECONDS + 12))
  while identity_alive "$role"; do
    if (( SECONDS >= deadline )); then
      printf '%s\tleader-cleanup-timeout; supervisor required\n' "$role" >> "$out/ownership-errors.tsv"
      worker_state[$role]=lost
      return 1
    fi
    sleep 0.05
  done
  # wait concerns this known child, never a signal to a potentially reused ID.
  if wait "${worker_pid[$role]}"; then code=0; else code=$?; fi
  printf '%s\t%s\n' "$role" "$code" >> "$out/leader-exits.tsv"
  record_status "$role" || true
  if [[ $code != 0 || ! -f "$out/$role-owner/cleanup-complete" || -f "$out/$role-owner/forced-kill" ]]; then result=1; fi
  if [[ -f "$out/$role-owner/cleanup-complete" ]]; then
    [[ $(cat "$out/$role-owner/cleanup-complete") == "${worker_pid[$role]} ${worker_start[$role]}" ]] || result=1
  fi
  # A successful leader wait alone is not descendant/group-empty evidence.
  if group_present_readonly "${worker_pid[$role]}"; then result=1; fi
  printf '%s\t%s\t%s\n' "$role" "${worker_pid[$role]}" "$result" >> "$out/retired.tsv"
  worker_state[$role]=retired
  case ${worker_fd[$role]} in 8) exec 8>&- ;; 9) exec 9>&- ;; esac
  if [[ -p "$out/$role-owner/control" ]]; then rm -- "$out/$role-owner/control" || result=1; fi
  unset "worker_pid[$role]" "worker_start[$role]"
  return "$result"
}

finish() {
  local status=$? role
  trap - EXIT
  trap '' INT TERM
  for role in store runtime; do
    if ! retire "$role" monitor-exit; then
      if [[ $status == 0 ]]; then status=1; fi
    fi
  done
  # Closing every writer also handles parent death via the leaders' read EOF.
  exec 8>&- 9>&-
  for role in store runtime; do
    if [[ -p "$out/$role-owner/control" ]]; then rm -- "$out/$role-owner/control" || status=1; fi
  done
  printf 'monitor_exit\t%s\n' "$status" >> "$out/cleanup.tsv"
  exit "$status"
}
trap finish EXIT
trap 'printf "INT\n" >> "$out/cleanup.tsv"; exit 130' INT
trap 'printf "TERM\n" >> "$out/cleanup.tsv"; exit 143' TERM

[[ $(uname -s) == Linux ]]
[[ ${STATEKNOT_REQUIRE_POSTGRES_TESTS:-} == 1 ]]
container=${STATEKNOT_PARALLEL_POSTGRES_CONTAINER:?}
major=${STATEKNOT_PARALLEL_POSTGRES_MAJOR:?}
[[ "$container" =~ ^[0-9a-f]{64}$ && "$major" =~ ^(16|17)$ ]]
base=${STATEKNOT_TEST_DATABASE_URL:?}
[[ "$base" == postgres://postgres:*@127.0.0.1:5432/stateknot_test ]]
keys=(CARGO_INCREMENTAL CARGO_PROFILE_DEV_DEBUG CARGO_PROFILE_TEST_DEBUG CARGO_PROFILE_TEST_OPT_LEVEL CARGO_PROFILE_TEST_DEBUG_ASSERTIONS CARGO_PROFILE_TEST_OVERFLOW_CHECKS)
values=(0 0 0 1 true true)
for i in "${!keys[@]}"; do
  key=${keys[$i]}
  [[ ${!key} == "${values[$i]}" ]]
  printf '%s\t%s\n' "$key" "${!key}"
done > "$out/profile.tsv"
printf 'STATEKNOT_REQUIRE_POSTGRES_TESTS\t1\n' >> "$out/profile.tsv"
for key in CARGO_TARGET_DIR RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTFLAGS RUST_MIN_STACK RUSTC RUSTC_BOOTSTRAP RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER; do
  value=${!key-}
  present=0
  if [[ ${!key+x} == x ]]; then present=1; fi
  printf '%s\t%s\t%s\n' "$key" "$present" "${#value}" >> "$out/override-presence.tsv"
  [[ -z "$value" ]]
done
[[ $(cat "$out/cold-target.txt") == imported-same-run ]]
python3 "$script_dir/postgres-native-artifacts.py" verify before
docker inspect --format '{{.Id}} {{.State.Running}}' "$container" > "$out/service.txt"
[[ $(cat "$out/service.txt") == "$container true" ]]

psql_in_service() {
  docker exec "$container" psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -At -c "$1"
}
settings="SELECT current_setting('server_version_num'),current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes');"
psql_in_service "$settings" > "$out/settings-before.tsv"
IFS='|' read -r version fsync synchronous_commit full_page_writes < "$out/settings-before.tsv"
[[ $((version / 10000)) == "$major" && "$fsync" == on && "$synchronous_commit" == on && "$full_page_writes" == on ]]
[[ $(psql_in_service "SELECT count(*) FROM pg_database WHERE datname IN ('stateknot_store_native','stateknot_runtime_native');") == 0 ]]
for database in stateknot_store_native stateknot_runtime_native; do
  docker exec "$container" createdb -U postgres --template=template0 --encoding=UTF8 "$database"
  printf '%s\n' "$database" >> "$out/created-databases.txt"
done

date -u +%Y-%m-%dT%H:%M:%SZ > "$out/pair-started.txt"
for role in store runtime; do
  mkdir "$out/$role-owner"
  mkfifo "$out/$role-owner/control"
done
exec 8<> "$out/store-owner/control"
exec 9<> "$out/runtime-owner/control"
printf '%s\n' 'python3 .github/scripts/postgres-native-binaries.py run store' > "$out/store-command.txt"
printf '%s\n' 'python3 .github/scripts/postgres-native-binaries.py run runtime' > "$out/runtime-command.txt"
STATEKNOT_TEST_DATABASE_URL="${base%/stateknot_test}/stateknot_store_native" \
  setsid bash "$leader" "$out/store-owner" "$out/store-owner/control" "$$" \
  python3 "$binary_helper" run store \
  9>&- > "$out/store.stdout" 2> "$out/store.stderr" &
worker_pid[store]=$!
worker_state[store]=active
STATEKNOT_TEST_DATABASE_URL="${base%/stateknot_test}/stateknot_runtime_native" \
  setsid bash "$leader" "$out/runtime-owner" "$out/runtime-owner/control" "$$" \
  python3 "$binary_helper" run runtime \
  8>&- > "$out/runtime.stdout" 2> "$out/runtime.stderr" &
worker_pid[runtime]=$!
worker_state[runtime]=active
printf 'store\t%s\nruntime\t%s\n' "${worker_pid[store]}" "${worker_pid[runtime]}" > "$out/pids.tsv"
for role in store runtime; do
  bind_identity "$role"
done
python3 "$binary_helper" overlap
printf 'Both original leader identities bound.\n' > "$out/monitor-ready"
printf 'Owned leaders started; complete native-binary outputs are retained separately.\n'
while [[ ${worker_state[store]} == active || ${worker_state[runtime]} == active ]]; do
  for role in store runtime; do
    [[ ${worker_state[$role]} == active ]] || continue
    identity_alive "$role"
    if record_status "$role"; then
      retire "$role" suite-complete
      (( 10#${suite_code[$role]} == 0 )) || exit "${suite_code[$role]}"
    fi
  done
  if [[ ${worker_state[store]} == active || ${worker_state[runtime]} == active ]]; then sleep 0.05; fi
done
[[ ${suite_code[store]} == 0 && ${suite_code[runtime]} == 0 ]]
# Full original test harnesses must actually finish; suite exit alone is insufficient.
[[ $(grep -Fxc 'running 276 tests' "$out/store.stdout") == 1 ]]
[[ $(grep -Ec '^test result: ok[.] 276 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in [0-9.]+s$' "$out/store.stdout") == 1 ]]
[[ $(grep -Fxc 'running 229 tests' "$out/runtime.stdout") == 1 ]]
[[ $(grep -Ec '^test result: ok[.] 229 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in [0-9.]+s$' "$out/runtime.stdout") == 1 ]]
psql_in_service "$settings" > "$out/settings-after.tsv"
cmp "$out/settings-before.tsv" "$out/settings-after.tsv"
python3 "$binary_helper" verify
python3 "$script_dir/postgres-native-artifacts.py" verify after
sha256sum "$out/store.stdout" "$out/store.stderr" "$out/runtime.stdout" "$out/runtime.stderr" > "$out/log-sha256.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$out/pair-completed.txt"
printf 'Both complete native suites passed their full harness guards.\n'
