<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Trusted Root graph dispositions

This is the experimental implementation contract of
[RFC-0022](rfcs/0022-namespaced-graph-frames.md). Its qualification scope is
recorded in [the current evidence report](qualification/experimental-trusted-root-dispositions-2026-10-12.md).
The RFC and R1 remain open. The complete native gates still contain failures.

## Creating a Root Run

There are two authenticated creation sources. Agent V1 retains its actual Agent
admission, request, budget and deadline. Ordinary trusted graph V2 originates
from `PostgresStore::admit_run`, a real Pending-to-Active Start and the original
INITIAL Root checkpoint. Start and checkpoint may be one existing compound
commit or two existing separate commits. Flattened shared-state graph
composition uses this same ordinary Root path.

The Store derives a positive creation profile from complete owning history. It
binds the original Run provenance and admission time, first event, exact Start
revision and time, INITIAL checkpoint and that checkpoint's owning event. An
absent Agent row does not establish ordinary creation. Agent, submission,
deadline, frame or child-ownership evidence cannot be substituted for this
profile.

Before fresh Root work, `ClaimedRunRecovery::validate_graph_creating_source`
validates the original recovery session's exact Root checkpoint and journal
observation in a read-only, repeatable-read transaction. It authenticates the
canonical Run, complete owning history and creation source, then checks the
original fence and database clock. It returns unit and creates no portable
authority or business record. The Driver performs this check after its
cancellation handoff and before starting a node or entering a frame. Subsequent
mutations and dispatches retain their original fresh guards.

An authenticated legacy history whose original creation inputs cannot be
proved returns `GraphReplayHistoricalDispositionUnavailable` before dispatch.
This is an operational incompatibility; it does not quarantine the Run. Actual
corrupt owning records remain fail closed and retain the existing fenced
quarantine behavior.

## Continue, Wait and Terminal

The V2 whole barrier binds its creation source, base checkpoint, ordered
results, original complete GraphPlan, journal event, successor checkpoint and
lifecycle projection. Continue preserves quantum/yield and later fresh-claim
recovery. Future Wait preserves the original registration, partial resolution,
resume and abandonment contracts. An elapsed absolute Wait is a separate open
increment and is not qualified by schema 35.

Ordinary Terminal retains the complete original `AgentResult` and
`AgentTerminalValidationInputs` obtained from the real descriptor, complete
request and resolved budget. `AgentResult::validate_for_terminal_inputs`
executes the same predicate and error order as `AgentResult::validate_for`.
Known and unpriced costs, finite budget dimensions, schema identity,
artifacts and completion time remain required. The snapshot is not an Agent
admission or an executable descriptor.

The fresh path compares the whole plan using pinned schema/reducer callbacks
outside SQL locks, writes all components atomically and runs deferred guards
and the final database-clock/fence check. ACK recovery authenticates the
already committed whole record and exact original validation inputs before
fresh callbacks. An exact retry returns the original committed result and
does not repeat provider work or evidence callbacks.

The input and state ceilings remain 2 MiB. The maximum-component native fixture
also commits a terminal output 16 KiB below the original 2 MiB output ceiling,
with one actual durable model response, token usage, pricing and invocation
binding. The state remains complete in its separately anchored checkpoint;
the compact bundle contains the complete request and terminal output. The V2
bundle ceiling is 14,241,792 bytes; unchanged V1 retains 9,474,048 bytes. These
are complete-record reader ceilings, not permission to raise component limits.

## Upgrading schema 34 to 35

1. Drain old workers and close schema-34 runtime pools.
2. Apply the normal SQLx migration 35 with the migration owner. Earlier migration
   checksums and existing V1 facts remain unchanged; migration 35 does not invent
   or backfill trusted creation origins.
3. Install the matching trusted runtime/owner/retention role profile and verify
   the exact schema-35 catalog and ACLs.
4. Start the matching reader/writer binary, then enable ordinary V2 writes.

An already connected V1-only reader cannot interpret V2 history. This is not a
mixed-version rolling upgrade. New connections with a mismatched schema fail
operationally. A downgrade must keep V2 history away from a V1-only reader.
The nonempty native upgrade fixture preserves all 54 existing business and
migration table snapshots, migrations 1–34 checksums and three original V1
Continue/Wait/Terminal ACKs.

Default-stack heap boundaries enclose complete owning futures, including the
Root ancestor verifier, live-lease observation and deadline-cancellation
transaction. They preserve each SQL operation, proof and ACK; they do not
substitute metadata validation for complete authentication.

The qualified static and focused native results apply to their exact source
manifest. Full PostgreSQL 16/17 gates, complete composed Driver/frame/Wait fault
qualification, historical release upgrades and production capacity remain
required before adoption as Supported.
