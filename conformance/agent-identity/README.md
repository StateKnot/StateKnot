<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Real Agent HTTP identity qualification

Requires Docker, OpenSSL 3, curl, the pinned Rust toolchain, and a **dedicated**
PostgreSQL 16/17 test database. Never point this at production: the suite creates
and migrates test data. Run from the repository root:

```sh
STATEKNOT_TEST_DATABASE_URL=postgres://postgres:stateknot_test_password@127.0.0.1:5432/stateknot_test \
  bash conformance/agent-identity/run.sh
```

The script owns a unique loopback-only container and temporary certificate
directory. Its exit trap removes that container/anonymous volume and generated
certificates/keys; database rows remain in the caller-provided test database.
Keycloak 26.7.3 is pinned to multiarch OCI index
`sha256:29be7252db0a106f1cd2ac17b9a56ff2668073da645638a38b9fc67deeb2d6c4`.
The embedded realm's fixed service-account secrets are disposable test fixtures,
not deployable secrets or example production defaults. No password grant,
insecure TLS flag, external user account or cloud IdP is used.

CI runs the profile against both PostgreSQL versions and requires one exact
`STATEKNOT_IDENTITY_EVIDENCE` marker so missing environment or a zero-test filter
cannot silently pass qualification. Environment artifacts pin Git source/tree,
Cargo.lock and the PostgreSQL image; this script records the IdP image and tree.
Local dirty-tree output is development feedback, not immutable release evidence.

For behavior, security boundaries and rollout see
[the operator guide](../../docs/agent-identity.md) and
[中文指南](../../docs/agent-identity.zh-CN.md).
