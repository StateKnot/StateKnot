<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0027: Attribute Provider Native Tools charges across cold recovery

- Status: Draft
- Created: 2026-11-27
- Tracking: [experimental graph driver, #161](https://github.com/StateKnot/StateKnot/pull/161)

## Motivation

A Tools node makes two writes. Its first physical node attempt records the
charge for A and fails while B's external outcome is unknown. A second attempt
reconciles B and journals its SDK terminal, then loses its lease before node
completion. The third attempt can reuse both results. In schema 38 it observes
the first failed completion and suppresses both charges. The final lifecycle
counts both writes but this node omits B. PostgreSQL 16 and 17 reproduced this
sequence.

## Decision

Allow a failed node attempt to persist an optional, complete itemization of the
Tools charged to that attempt. Each entry contains the exact terminal Tool
binding and its observed DIRECT usage. A present, empty receipt proves zero
Tools charged. Absence means the writer supplied only aggregate usage. Keep the
existing aggregate-only interface for custom executors.

One physical failure can record up to 256 entries in at most 1 MiB of canonical
JSON. Entries are ordered by logical invocation ID and physical attempt ID.
The Store checks each Tool's activation, terminal revision, risk, input,
output and artifact sizes against its ledger. The runtime supplies the
graph-priced cost. The Store checks the sum of all counters and currency
amounts against the node completion. The journal projection binds the completion and its
receipt. Recording a failure and its receipt uses a single fenced transaction.
An acknowledgement retry must match the original itemization. A second attempt
cannot claim a Tool already charged by a receipt for this activation. Receipts
contain identifiers, digests, counters and prices. The Tool ledger retains the
original arguments and results.

Before dispatch the Driver reads the activation's verified history through the
current Start (at most 64 attempts). The executor skips pricing for each
terminal proven paid, including in a history that also has legacy rows. When
the entire history identifies the charged Tools, a fully priced terminal missing
from that set contributes its observed usage to the recovering node. Pricing
uses the graph's bound accounting implementation. Reuse of an exact terminal
requires no new external dispatch. This proposal leaves cumulative Run
accounting, unknown-effect reconciliation and Model accounting under their
existing contracts.

## Alternatives considered

The former replay rule suppresses all previously journaled terminals once any
attempt completes. That loses B in the motivating example. Charging every
reused terminal would charge A twice. Timestamps order the ledger events, but
aggregate usage supplied by a custom executor does not identify which terminal
it included. Persisting the itemization at the time of charge resolves this
ambiguity for new writes.

## Compatibility and operations

Schema 39 adds optional receipt data to failed completions. Migration preserves
the bytes and digests of every aggregate-only record. Counts and costs in those
records cannot identify which Tool contributed each component. For unmatched
terminals in a mixed or wholly legacy history the executor therefore keeps the
existing aggregate replay behavior. Operators should use the durable Run
account for historical totals.

The migration creates a unique journal index and a stored generated digest
column on node completions. Size the maintenance window and disk/WAL budget
using a restored copy with representative table sizes. Drain workers and back
up the database before migrating. Apply the migration
and the matching trusted-role profile, audit grants, and resume workers with
the schema 39 binary. To roll back, restore the pre-upgrade database and its
matching binary. Removing receipts from live history would discard billing
evidence.

The [PostgreSQL configuration guide](../postgresql-configuration.md#recovering-an-interrupted-schema-38-to-39-upgrade)
covers a lost migration acknowledgement and the rollback window.

## Acceptance

On both supported PostgreSQL majors, replay the failure, reconciliation and
cold takeover described above. The recovered node owes one call, one write,
26 input bytes, 23 output bytes and 11 micro-USD. Run accounting must retain
both writes and both model attempts, totaling 36 micro-USD. Check actual
business dispatch counts independently. Exercise receipt-preserving retries,
altered receipts, duplicate attribution and legacy history through the Store.
The public compatibility commitment remains subject to RFC review.

On PostgreSQL 16.15 and 17.11, the schema 38 SDK at `efcbf47` read a
restored backup of a nested Run before upgrade. The schema 39 migrator and
SDK at `d9c8c57` upgraded the restored database and read the same journal
head (sequence 13) and Tool result (revision 2). The old SDK rejected the
upgraded database with `IncompatibleSchema` and read the original digests
again from a separate restore of the pre-upgrade backup. The Store suite
also interrupts the deployment migrator on both sides of the schema 39 commit
and retries through the public deployment entry point.
