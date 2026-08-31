#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <ssh-destination>" >&2
  exit 64
fi

readonly destination="$1"
readonly staging_dir="/tmp/stateknot-tls-bootstrap-$$"
readonly ssh_identity="${STATEKNOT_SSH_IDENTITY:-}"

for command in ssh scp curl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 69
  fi
done

ssh_options=(-o BatchMode=yes)
if [[ -n "$ssh_identity" ]]; then
  if [[ ! -f "$ssh_identity" ]]; then
    echo "STATEKNOT_SSH_IDENTITY does not name a readable file" >&2
    exit 66
  fi
  ssh_options+=(-o IdentitiesOnly=yes -i "$ssh_identity")
fi
readonly -a ssh_options

cleanup_staging() {
  ssh "${ssh_options[@]}" "$destination" "rm -rf -- '$staging_dir'" \
    >/dev/null 2>&1 || true
}
trap cleanup_staging EXIT

ssh "${ssh_options[@]}" "$destination" "install -d -m 0700 '$staging_dir'"
scp "${ssh_options[@]}" deploy/Caddyfile \
  "$destination:$staging_dir/Caddyfile"

ssh "${ssh_options[@]}" "$destination" bash -s -- "$staging_dir" <<'REMOTE'
set -Eeuo pipefail

readonly staging_dir="$1"
readonly config_path="/etc/caddy/Caddyfile"
readonly config_backup="$staging_dir/Caddyfile.previous"

if ! sudo -n true; then
  echo "passwordless sudo is required" >&2
  exit 77
fi

nginx_was_active=0
nginx_was_enabled=0
caddy_was_active=0
caddy_was_enabled=0
had_config=0
committed=0

if sudo -n systemctl is-active --quiet nginx; then
  nginx_was_active=1
fi
if sudo -n systemctl is-enabled --quiet nginx 2>/dev/null; then
  nginx_was_enabled=1
fi
if sudo -n systemctl is-active --quiet caddy; then
  caddy_was_active=1
fi
if sudo -n systemctl is-enabled --quiet caddy 2>/dev/null; then
  caddy_was_enabled=1
fi

if ! command -v caddy >/dev/null 2>&1; then
  sudo -n systemctl mask caddy.service
  if ! sudo -n apt-get update ||
    ! sudo -n env DEBIAN_FRONTEND=noninteractive \
      apt-get install -y --no-install-recommends caddy; then
    sudo -n systemctl unmask caddy.service || true
    exit 69
  fi
  sudo -n systemctl unmask caddy.service
fi

if sudo -n test -f "$config_path"; then
  sudo -n cp -a "$config_path" "$config_backup"
  had_config=1
fi

restore_config() {
  if [[ "$had_config" -eq 1 ]]; then
    sudo -n install -o root -g root -m 0644 "$config_backup" "$config_path"
  else
    sudo -n rm -f -- "$config_path"
  fi
}

restore_services() {
  restore_config

  if [[ "$caddy_was_active" -eq 1 ]]; then
    if [[ "$caddy_was_enabled" -eq 1 ]]; then
      sudo -n systemctl enable caddy >/dev/null
    else
      sudo -n systemctl disable caddy >/dev/null 2>&1 || true
    fi
    sudo -n systemctl restart caddy
  else
    sudo -n systemctl disable --now caddy >/dev/null 2>&1 || true
  fi

  if [[ "$nginx_was_enabled" -eq 1 ]]; then
    sudo -n systemctl enable nginx >/dev/null
  else
    sudo -n systemctl disable nginx >/dev/null 2>&1 || true
  fi
  if [[ "$nginx_was_active" -eq 1 ]]; then
    sudo -n systemctl start nginx
  fi
}

finish() {
  status=$?
  trap - EXIT
  if [[ "$status" -ne 0 || "$committed" -ne 1 ]]; then
    restore_services
  fi
  rm -rf -- "$staging_dir"
  exit "$status"
}
trap finish EXIT

sudo -n caddy validate \
  --config "$staging_dir/Caddyfile" \
  --adapter caddyfile
sudo -n install -o root -g root -m 0644 \
  "$staging_dir/Caddyfile" "$config_path"

if [[ "$caddy_was_active" -eq 1 ]]; then
  sudo -n systemctl reload caddy
else
  sudo -n systemctl stop nginx
  sudo -n systemctl enable --now caddy
fi

tls_ready=0
for _ in $(seq 1 30); do
  if curl --silent --show-error --output /dev/null --max-time 5 \
    --resolve 'stknot.com:443:127.0.0.1' \
    https://stknot.com/ &&
    curl --silent --show-error --output /dev/null --max-time 5 \
      --resolve 'www.stknot.com:443:127.0.0.1' \
      https://www.stknot.com/; then
    tls_ready=1
    break
  fi
  sleep 2
done

if [[ "$tls_ready" -ne 1 ]]; then
  sudo -n journalctl --unit caddy --no-pager --lines 80 >&2 || true
  echo "Caddy did not obtain trusted certificates within the deadline" >&2
  exit 70
fi

sudo -n systemctl enable caddy >/dev/null
sudo -n systemctl disable --now nginx >/dev/null 2>&1 || true
sudo -n systemctl disable --now certbot.timer >/dev/null 2>&1 || true
sudo -n systemctl is-active --quiet caddy

committed=1
caddy version
REMOTE

curl --fail --silent --show-error --output /dev/null --max-time 10 \
  --retry 5 --retry-all-errors --retry-delay 1 \
  https://stknot.com/

trap - EXIT
echo "bootstrapped automatic TLS for https://stknot.com/"
