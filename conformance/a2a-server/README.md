<!-- Copyright 2026 StateKnot contributors -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# A2A 1.0 server conformance gate

This directory contains the reproducible compatibility gate for StateKnot's
A2A 1.0 HTTP+JSON and JSON-RPC server profile. It is a release gate, not a
claim about the intentionally excluded gRPC transport.

The runner freezes all executable inputs that are not part of this repository:

- upstream TCK commit `263b9cfaf16a554bdfb166a7ba5b67716e946349`;
- archive SHA-256
  `694c798e93fff30f650d44bdb3db0e1768b865a4f3ddbed64ec158db209bf5db`;
- `uv` `0.11.25`; and
- the TCK's committed `uv.lock`, consumed with `uv run --frozen`.

`tck-compat.patch` carries two audited upstream test-harness corrections:

1. `CORE-SEND-003` declares its already-defined expected
   `ContentTypeNotSupportedError`. This is tracked upstream as
   [a2a-tck issue #202](https://github.com/a2aproject/a2a-tck/issues/202).
2. The JSON-RPC client emits the A2A 1.0 camel-case wire names for task-list,
   history, and push-configuration parameters. The unpatched client emits
   Python snake-case identifiers even though the specification and schemas use
   names such as `historyLength`, `contextId`, and `taskId`.

The patch changes only TCK metadata and request serialization. It does not
relax a server assertion. The archive checksum is verified before extraction,
and `patch --forward` fails if the pinned source no longer matches the audited
hunks.

## Run locally

Install Rust `1.88.0`, `uv` `0.11.25`, `curl`, `patch`, and Python 3.11 or
newer, then run from the repository root:

```console
bash conformance/a2a-server/run-1.0.sh
```

For an offline rerun with an already downloaded, checksum-matching archive:

```console
STATEKNOT_A2A_TCK_ARCHIVE=/absolute/path/to/a2a-tck.tar.gz \
  bash conformance/a2a-server/run-1.0.sh
```

Results are written below `conformance/a2a-server/results/` and ignored by
Git. CI uploads the full report set even when the TCK fails.

The post-run verifier requires exactly 265 collected cases, 177 passes, 88
declared skips, zero failures, zero errors, and zero expected failures. It also
requires representative Agent Card caching, unknown-field, streaming,
multi-subscriber, push-authentication, extended-card, HTTP error, and JSON-RPC
SSE tests to execute rather than skip.

The fixture uses an in-memory backend solely to drive deterministic protocol
scenarios. Passing this gate qualifies the transport and application boundary;
production task durability, idempotency, authorization policy, and push outbox
delivery remain responsibilities of the `A2aTaskService` implementation.
