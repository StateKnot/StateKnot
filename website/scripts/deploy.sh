#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <ssh-destination> [release-id]" >&2
  exit 64
fi

readonly destination="$1"
readonly release_id="${2:-$(git rev-parse --short=12 HEAD)}"
readonly remote_root="/var/www/stateknot"
readonly release_dir="${remote_root}/releases/${release_id}"
readonly staging_dir="/tmp/stateknot-${release_id}-$$"
readonly ssh_identity="${STATEKNOT_SSH_IDENTITY:-}"

if [[ ! "$release_id" =~ ^[A-Za-z0-9._-]{1,64}$ ]]; then
  echo "release id must match [A-Za-z0-9._-] and be at most 64 characters" >&2
  exit 64
fi

for command in ssh rsync scp curl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 69
  fi
done

if [[ ! -f dist/index.html || ! -f dist/404.html || ! -f dist/healthz.txt ]]; then
  echo "website/dist is incomplete; run npm run build first" >&2
  exit 66
fi

ssh_options=(-o BatchMode=yes)
if [[ -n "$ssh_identity" ]]; then
  if [[ ! -f "$ssh_identity" ]]; then
    echo "STATEKNOT_SSH_IDENTITY does not name a readable file" >&2
    exit 66
  fi
  ssh_options+=(-o IdentitiesOnly=yes -i "$ssh_identity")
fi
readonly -a ssh_options

printf -v rsync_shell '%q ' ssh "${ssh_options[@]}"
readonly rsync_shell="${rsync_shell% }"

cleanup_staging() {
  ssh "${ssh_options[@]}" "$destination" "rm -rf -- '$staging_dir'" \
    >/dev/null 2>&1 || true
}
trap cleanup_staging EXIT

ssh "${ssh_options[@]}" "$destination" \
  "install -d -m 0700 '$staging_dir/site'"
rsync -a --checksum -e "$rsync_shell" dist/ "$destination:$staging_dir/site/"
scp "${ssh_options[@]}" deploy/stateknot.conf \
  "$destination:$staging_dir/stateknot.conf"

ssh "${ssh_options[@]}" "$destination" bash -s -- \
  "$staging_dir" "$remote_root" "$release_dir" <<'REMOTE'
set -Eeuo pipefail

readonly staging_dir="$1"
readonly remote_root="$2"
readonly release_dir="$3"
readonly config_path="/etc/nginx/sites-available/stateknot.conf"
readonly config_link="/etc/nginx/sites-enabled/stateknot.conf"
readonly config_backup="$staging_dir/stateknot.conf.previous"
readonly next_link="$remote_root/.current-next"

case "$release_dir" in
  "$remote_root"/releases/*) ;;
  *)
    echo "release directory escaped the deployment root" >&2
    exit 64
    ;;
esac

cleanup() {
  rm -rf -- "$staging_dir"
}
trap cleanup EXIT

if sudo -n test -e "$release_dir"; then
  echo "release already exists: $release_dir" >&2
  exit 73
fi

sudo -n install -d -m 0755 "$remote_root/releases"
sudo -n install -d -m 0755 "$release_dir"
sudo -n cp -a "$staging_dir/site/." "$release_dir/"
sudo -n chown -R root:root "$release_dir"

had_config=0
if sudo -n test -f "$config_path"; then
  sudo -n cp -a "$config_path" "$config_backup"
  had_config=1
fi

restore_config() {
  if [[ "$had_config" -eq 1 ]]; then
    sudo -n install -m 0644 "$config_backup" "$config_path"
    sudo -n ln -sfn "$config_path" "$config_link"
  else
    sudo -n rm -f -- "$config_link" "$config_path"
  fi
}

sudo -n install -m 0644 "$staging_dir/stateknot.conf" "$config_path"
sudo -n ln -sfn "$config_path" "$config_link"
if ! sudo -n nginx -t; then
  restore_config
  exit 78
fi

previous_release=""
if sudo -n test -L "$remote_root/current"; then
  previous_candidate="$(
    sudo -n readlink -f "$remote_root/current" 2>/dev/null || true
  )"
  case "$previous_candidate" in
    "$remote_root"/releases/*)
      if sudo -n test -d "$previous_candidate"; then
        previous_release="$previous_candidate"
      fi
      ;;
  esac
fi

rollback_release() {
  if [[ -n "$previous_release" ]] && sudo -n test -d "$previous_release"; then
    sudo -n ln -sfn "$previous_release" "$next_link"
    sudo -n mv -Tf "$next_link" "$remote_root/current"
  else
    sudo -n rm -f -- "$remote_root/current" "$next_link"
  fi
}

sudo -n ln -sfn "$release_dir" "$next_link"
sudo -n mv -Tf "$next_link" "$remote_root/current"

if ! sudo -n systemctl reload nginx; then
  rollback_release
  restore_config
  sudo -n nginx -t
  sudo -n systemctl reload nginx
  exit 69
fi

if ! curl --fail --silent --show-error --max-time 5 \
  --header 'Host: 49.232.33.76' \
  http://127.0.0.1/healthz | grep -Fx 'stateknot-ok'; then
  rollback_release
  restore_config
  sudo -n nginx -t
  sudo -n systemctl reload nginx
  exit 70
fi
REMOTE

curl --fail --silent --show-error --max-time 10 \
  "http://49.232.33.76/healthz" | grep -Fx 'stateknot-ok'

trap - EXIT
echo "deployed release ${release_id} to http://49.232.33.76/"
