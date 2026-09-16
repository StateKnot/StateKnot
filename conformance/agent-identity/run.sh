#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
# Isolated disposable qualification only, NOT an identity-server deployment recipe.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/../.." && pwd)"
: "${STATEKNOT_TEST_DATABASE_URL:?real PostgreSQL qualification URL required}"
image='quay.io/keycloak/keycloak@sha256:29be7252db0a106f1cd2ac17b9a56ff2668073da645638a38b9fc67deeb2d6c4'
fixture_dir="$(mktemp -d "${TMPDIR:-/tmp}/stateknot-identity.XXXXXX")"
fixture_name="stateknot-identity-$(basename "${fixture_dir}" | tr '[:upper:]' '[:lower:]')"
created=0
cleanup() {
  if [[ "${created}" = 1 ]]; then docker rm --force --volumes "${fixture_name}" >/dev/null; fi
  # Only generated key material in this exact mktemp directory is removed.
  rm -f -- "${fixture_dir}/tls.key" "${fixture_dir}/tls.crt" "${fixture_dir}/tls.csr" \
    "${fixture_dir}/ca.key" "${fixture_dir}/ca.crt" "${fixture_dir}/ca.srl"
  rmdir -- "${fixture_dir}"
}
trap cleanup EXIT
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1 \
  -keyout "${fixture_dir}/ca.key" -out "${fixture_dir}/ca.crt" \
  -subj '/CN=StateKnot disposable qualification CA' -addext 'basicConstraints=critical,CA:TRUE' \
  >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "${fixture_dir}/tls.key" -out "${fixture_dir}/tls.csr" \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'basicConstraints=critical,CA:FALSE' -addext 'extendedKeyUsage=serverAuth' \
  >/dev/null 2>&1
openssl x509 -req -days 1 -sha256 -in "${fixture_dir}/tls.csr" \
  -CA "${fixture_dir}/ca.crt" -CAkey "${fixture_dir}/ca.key" -CAcreateserial \
  -copy_extensions copy -out "${fixture_dir}/tls.crt" >/dev/null 2>&1
# Docker's unprivileged Keycloak process must read disposable test credentials.
chmod 755 "${fixture_dir}"
chmod 644 "${fixture_dir}/tls.key" "${fixture_dir}/tls.crt"
docker create --name "${fixture_name}" --label stateknot.qualification=agent-identity \
  --memory=1536m --cpus=2 --pids-limit=256 --publish 127.0.0.1::8443 \
  --mount "type=bind,source=${fixture_dir},target=/fixture,readonly" \
  --mount "type=bind,source=${script_dir}/realm.json,target=/opt/keycloak/data/import/realm.json,readonly" \
  "${image}" start-dev --import-realm --http-enabled=false \
  --hostname-strict=false --https-certificate-file=/fixture/tls.crt \
  --https-certificate-key-file=/fixture/tls.key >/dev/null
created=1
docker start "${fixture_name}" >/dev/null
port="$(docker port "${fixture_name}" 8443/tcp | sed -n 's/^127\.0\.0\.1://p')"
[[ "${port}" =~ ^[0-9]+$ ]]
export STATEKNOT_TEST_IDENTITY_ISSUER="https://localhost:${port}/realms/stateknot-qualification"
export STATEKNOT_TEST_IDENTITY_CA="${fixture_dir}/ca.crt"
export STATEKNOT_REQUIRE_IDENTITY_TESTS=1
export STATEKNOT_REQUIRE_POSTGRES_TESTS=1
ready=0
for _ in $(seq 1 90); do
  if curl --noproxy '*' --fail --silent --max-time 2 --cacert "${STATEKNOT_TEST_IDENTITY_CA}" \
    "${STATEKNOT_TEST_IDENTITY_ISSUER}/.well-known/openid-configuration" >/dev/null; then ready=1; break; fi
  sleep 1
done
if [[ "${ready}" != 1 ]]; then docker logs --tail 100 "${fixture_name}"; exit 1; fi
cd "${repository_root}"
printf 'STATEKNOT_IDENTITY_IMAGE=%s\n' "${image}"
git rev-parse HEAD 'HEAD^{tree}'
cargo test -p stateknot --test agent_http --locked -- \
  --exact identity::keycloak_tls_authentication_rotation_revocation_and_owned_ingress \
  --nocapture --test-threads=1
