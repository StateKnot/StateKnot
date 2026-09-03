<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# A2A 1.0 conformance status

This page records the exact evidence for StateKnot's implemented A2A 1.0
HTTP+JSON and JSON-RPC server profile. It is not evidence for the separately
implemented client, gRPC, a stable API, or complete-framework certification.

## Frozen evaluation input

The reproducible gate freezes:

- official TCK repository `a2aproject/a2a-tck` at commit
  `263b9cfaf16a554bdfb166a7ba5b67716e946349`;
- downloaded archive SHA-256
  `694c798e93fff30f650d44bdb3db0e1768b865a4f3ddbed64ec158db209bf5db`;
- `uv` `0.11.25` and the TCK's committed `uv.lock` under `--frozen`;
- Rust `1.88.0`; and
- transports `jsonrpc,http_json`, with no expected-failures file.

The gate runs the official suite against the real `A2aServer` router through a
deterministic fixture. It does not call contract methods directly or bypass the
Host, body, authentication, authorization, admission, version, extension, or
stream boundaries.

## Result

| Pytest/TCK outcome | Count |
| --- | ---: |
| Collected | 265 |
| Passed | 177 |
| Declared skipped | 88 |
| Failed | 0 |
| Errors | 0 |
| Expected failures | 0 |

The TCK's per-surface report records Agent Card `10/10`, JSON-RPC `94/101`
with seven declared skips, and HTTP+JSON `91/96` with five declared skips. The
remaining skips are primarily the unconfigured gRPC transport plus mutually
exclusive capability/error preconditions. A skip is not a pass and StateKnot
does not claim gRPC.

The TCK also prints an aggregate `78.8%` value across its full requirement and
transport inventory. That denominator includes unconfigured gRPC and
non-applicable branches, so StateKnot publishes both the aggregate output and
the exact pytest counts instead of presenting it as 100% certification.

The repository verifier rejects count drift, any failure/error/xfail, duplicate
test identities, and a key test that becomes skipped. Required executed tests
cover Agent Card caching, ignored unknown fields, REST and JSON-RPC streaming,
multi-subscriber ordering, authenticated push delivery, authenticated extended
cards, HTTP 415 mapping, and JSON-RPC SSE envelopes.

## Audited TCK compatibility patch

The pinned TCK needs two minimal harness corrections:

1. `CORE-SEND-003` references `ContentTypeNotSupportedError` in its behavior
   text but omits the already-defined `expected_error` metadata. The patch adds
   only that metadata. This is tracked by
   [upstream issue #202](https://github.com/a2aproject/a2a-tck/issues/202).
2. The TCK JSON-RPC client emits Python snake-case parameters for task history,
   listing, and push-config operations. The A2A 1.0 schemas use camel-case wire
   fields such as `historyLength`, `contextId`, and `taskId`. The patch corrects
   request serialization only.

The patch does not change a server assertion, downgrade a requirement, add an
expected failure, or remove a test. It applies with `patch --forward` only after
the source archive checksum is verified. A changed upstream archive or hunk
fails before the server is built.

## Reproduce

```console
bash conformance/a2a-server/run-1.0.sh
```

The script verifies the supply-chain inputs, builds the actual Rust fixture,
waits for Agent Card readiness, runs both declared bindings, copies all HTML,
JSON, XML, and server-log evidence to an ignored timestamped directory, and
runs the independent standard-library-only result verifier. CI executes the
same script and uploads the evidence even on failure.

See [`conformance/a2a-server/README.md`](../conformance/a2a-server/README.md)
for the runner contract and [A2A 1.0 server profile](a2a-server.md) for the
production application obligations.

## Claim boundary

Passing this gate supports only the implemented server wire/application
boundary. Production qualification still requires a durable application
`A2aTaskService`, cross-replica policy and admission, transactional push outbox,
security/failure tests, stable API review, release artifacts, and operations
evidence. The separately implemented [A2A Client profile](a2a-client.md) has local
HTTP+JSON/JSON-RPC operation-matrix and PostgreSQL durability evidence, but no
official Client TCK claim. gRPC remains unimplemented.
