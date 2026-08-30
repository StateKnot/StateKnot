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

for command in ssh scp; do
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
scp "${ssh_options[@]}" deploy/stateknot-bootstrap.conf \
  "$destination:$staging_dir/stateknot.conf"
scp "${ssh_options[@]}" deploy/reload-nginx.sh \
  "$destination:$staging_dir/reload-nginx.sh"

ssh "${ssh_options[@]}" "$destination" bash -s -- "$staging_dir" <<'REMOTE'
set -Eeuo pipefail

readonly staging_dir="$1"
readonly config_path="/etc/nginx/sites-available/stateknot.conf"
readonly config_link="/etc/nginx/sites-enabled/stateknot.conf"
readonly config_backup="$staging_dir/stateknot.conf.previous"
readonly webroot="/var/www/letsencrypt"
readonly deploy_hook="/etc/letsencrypt/renewal-hooks/deploy/stateknot-reload-nginx"

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

finish() {
  status=$?
  trap - EXIT
  if [[ "$status" -ne 0 ]]; then
    restore_config
    sudo -n nginx -t
    sudo -n systemctl reload nginx
  fi
  rm -rf -- "$staging_dir"
  exit "$status"
}
trap finish EXIT

if ! command -v certbot >/dev/null 2>&1; then
  sudo -n apt-get update
  sudo -n env DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends certbot
fi

sudo -n install -d -m 0755 "$webroot/.well-known/acme-challenge"
sudo -n install -m 0644 "$staging_dir/stateknot.conf" "$config_path"
sudo -n ln -sfn "$config_path" "$config_link"
sudo -n nginx -t
sudo -n systemctl reload nginx

sudo -n certbot certonly \
  --webroot \
  --webroot-path "$webroot" \
  --cert-name stknot.com \
  --domains stknot.com \
  --domains www.stknot.com \
  --non-interactive \
  --agree-tos \
  --register-unsafely-without-email \
  --keep-until-expiring

sudo -n install -d -m 0755 /etc/letsencrypt/renewal-hooks/deploy
sudo -n install -m 0755 "$staging_dir/reload-nginx.sh" "$deploy_hook"
sudo -n systemctl enable --now certbot.timer
sudo -n certbot certificates --cert-name stknot.com
REMOTE

trap - EXIT
echo "bootstrapped TLS certificate for stknot.com"
