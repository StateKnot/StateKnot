<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC process

RFCs record decisions that create long-lived compatibility, correctness,
security, or operational commitments.

## When an RFC is required

Use an RFC for:

- public API or serialized-domain changes;
- graph, retry, cancellation, interrupt, or side-effect semantics;
- persistence schemas, migration, retention, or recovery guarantees;
- protocol versions and mappings;
- authentication, authorization, tenancy, secret, or sandbox boundaries;
- a new workspace crate or mandatory dependency; and
- release, MSRV, or compatibility policy.

Small bug fixes, internal refactors with unchanged observable behavior, tests,
and documentation corrections normally do not require an RFC.

## Lifecycle

1. Copy `0000-template.md` to the next four-digit number and descriptive slug.
2. Open a draft pull request and link the motivating issue and prototype.
3. Collect design, security, operations, and compatibility review.
4. Mark the RFC `Accepted`, `Rejected`, or `Withdrawn` in the same pull request.
5. Merge accepted text before relying on it as a supported contract.
6. Supersede an accepted RFC only with a new accepted RFC that links both ways.

An accepted RFC authorizes implementation but does not prove completion. The
implementation, tests, migration material, and release evidence remain separate
deliverables.
