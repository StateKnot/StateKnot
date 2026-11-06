<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental trusted Root disposition evidence — 2026-10-12

The ordinary Root V2/schema-35 implementation and complete owning Future heap
boundaries have passed the static gates and the stated focused native scopes.
Complete native qualification is **open**. RFC-0022 remains Draft and R1 remains
open. See the [adoption contract](../trusted-root-dispositions.md) and
[machine-readable evidence](experimental-trusted-root-dispositions-2026-10-12.json).

## Exact source and execution

The production chain is DCO-signed `3c15c595f2395e48e21c943d08784cfdfb11a7ec`
followed by `630e4c68fbe167234c9957aa5580ca230e510e09`, tree
`dac011e6dec01984b68db5a083d10456cc1397ec`. Its 875 tracked file hashes exactly
match the immutable source at
`/private/tmp/stateknot-ordinary35-private-future-source-v7`; manifest SHA256 is
`6f7dfde47feeb762a0b2dc8c18e6e749eb4eaedd70b66734e34c46c77bae733b`.

The stable cache was empty before final-source compilation. Cargo logs show
Core, Store and Runtime compiling from that exact source path. The focused
Runtime native binary SHA256 is
`83499cae7beeed61c9ee657acbad5a0ab001a79e165fc4d3d7d0e3ac2a5b6f30`.
Source before/after and the binary are bound in the receipts. PostgreSQL 16.15
and 17.11 use the pinned task-owned images, identities and labels. Every focused
group starts with a separate template0 database containing zero user tables and
no migration metadata. Ordinary flags disable incremental compilation and
debug symbols; default thread stacks, original leases, deadlines and assertions
remain unchanged.

This report and the adoption documents are a later documentation commit. The
code results bind the source above; they do not qualify a later composed tree.

## Passed scopes on that source

| Check | Actual result |
|---|---|
| Pinned nightly `2026-10-07`, complete workspace/all-targets/all-features check | Passed |
| Format and complete workspace strict Clippy | Passed; no warning allowance added |
| Complete workspace/all-targets/all-features/locked tests | 2,014 passed; 6 ignored |
| Separate workspace doctests | 45 passed |
| Strict workspace Rustdoc | Passed |
| Ordinary Root native group, PostgreSQL 16 and 17 | 9 passed on each |
| Complete original trusted role fixture at schema 35, PostgreSQL 16 and 17 | 1 substantive fixture passed on each |

The workspace test run has no database environment; its PostgreSQL entry
points return early and do not establish native qualification. Its original
sandbox loopback-permission failures remain recorded alongside the subsequent
same-source authorized loopback pass.

The nine ordinary cases include separate Start/INITIAL positive history and
component-substitution rollback; legacy creation rejection before any executor
call; exact recovery-session/journal/fence binding without writes; complete
Terminal ACK recovery; maximum input/state/output with an actual committed
durable priced model invocation; original known/unpriced-cost error ordering;
fresh source substitution rollback; and final database-clock expiry rollback.
The role fixture includes its original ACL drift, framework/provider/Join,
scoped operations, populated upgrades and 24-race checks. A single role fixture
is not a 10,000-race or capacity result.

## Retained native failures

The previous immutable `3c15c595` source ran **all** Store and Runtime native
tests on both PostgreSQL versions: 803 passed and 3 failed of 806 actual tests.
Those receipts remain distinct from the final heap-boundary source.

| Source / suite | Original result and remaining failure |
|---|---|
| `3c15c595`, PostgreSQL 16 Store | 251 passed |
| `3c15c595`, PostgreSQL 17 Store | 249 passed, 2 failed: generic Root 8/16/32 construction exceeded the original lease before application replay; an absolute scoped Wait elapsed before registration |
| `3c15c595`, PostgreSQL 16 Runtime | 151 passed, 1 failed: orphan parallel siblings exceeded the original five-second Driver timeout |
| `3c15c595`, PostgreSQL 17 Runtime | 152 passed |
| Final source focused deadline group, PostgreSQL 16 | 11 selected tests passed |
| Final source focused deadline group, PostgreSQL 17 | 10 passed, 1 failed: original two-second deadline reached before Waiting committed |

The two deadline groups contain four input-gated process helper entry points
in total; these are recorded as selected tests, not independent process-loss
fixtures. Final focused groups selected 42 tests in total: 41 passed, 1 failed.

The failed PostgreSQL 17 deadline database shows Agent admission to Start took
1.034662 seconds, then Start to Success took 0.831139 seconds. Success was
recorded only 22.706 milliseconds before the original deadline. The Run remained
Active at J3 with no Waiting commit and no quarantine; cleanup released its
lease. This is an actual deadline failure, not evidence that the original
30-second lease was exhausted.

One justified diagnostic on 2026-10-12 used the **same immutable binary**, a new
template0 database and the unchanged original deadline/lease/stack. It passed
1/1 and completed real Waiting, deadline cancellation, abandonment and closure.
Its Start/Success/Waiting intervals were 0.243568/0.130497/0.215859 seconds.
The actual container restart time and newly inspected loopback port are
recorded. This diagnostic does not replace the failed group or prove the
production stage-cost target.

Complete final composed native gates, the scoped ChildJoin and Elapsed36
combination, original stage-cost/capacity targets and real published N-1/N-2
upgrade qualification remain required. No limits, whole owning verification,
corruption rules or expected assertions were relaxed to obtain these results.
