<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Durable failure close

[中文](failure-close.zh-CN.md)

The PostgreSQL/runtime profile can now seal an original failure, stop parent
dispatch, drain owned children, and finally commit that same failure with exact
accounting. This is **not cancellation** and not immediate terminal failure.
The complete durable-child production profile remains gated by operational,
process-termination, role-isolation and capacity qualification.

## Supported boundary

Registration requires an Active Run, its exact current checkpoint and live
worker fence, no unfinished physical node in that fence, and no unsettled direct
model/tool invocation at that checkpoint. The trusted evidence provider must
supply **complete, fully priced, cumulative DIRECT usage**, excluding children.
It must include recovered effects from earlier attempts/checkpoints. Store
checks enforce monotonicity against already observed direct accounting; they do
not invent prices or establish completeness of arbitrary external effects.

Unpriced or unavailable direct evidence rejects registration before releasing
the recovery lease. The host must keep using normal fenced effect/usage recovery;
do not discard that work, substitute zero, or continually retry a bad snapshot.
Waiting Runs and arbitrary uncertain parent-side effects are not forced into
this profile. Child usage may remain unknown: that blocks settlement and final
closure, not the already sealed failure decision. Known child overruns remain
recorded rather than being clipped to the reservation.

## Execution and host integration

Before freezing a child-enabled executable registry, register
`register_standard_run_failure_close_event_schema`. `DurableGraphLifecycle::new`
checks the exact offline schema at startup. On a terminally blocked child-enabled
graph, the lifecycle coordinator freezes failure evidence and returns
`GraphBarrierLifecycleOutcome::FailureClosing`; `DurableAgentLoop` exposes
`AgentLoopOutcome::FailureClosing`. Stop dispatching on the released fence.
Ordinary graphs retain their existing direct terminal-failure path.

Run `DurableRunFailureCloser` alongside `DurableChildReconciler`, independently
leased child execution/cleanup, and deadline maintenance. The closer needs the
store, frozen schema registry, and finite `DurableGraphLifecycleOptions`; it
does not call the original provider again. Its Rustdoc includes a compiled host
configuration/tick example. No constructor starts a background task.

Schedule `tick(authorized_tenant, cursor, shutdown)` fairly even when the tenant
has no runnable parent. Each tick examines at most 16 candidates with 1–10
bounded mutation attempts per item. Defaults are three attempts, 25 ms initial
exponential delay capped at one second. Event identity is stable across retries.
Inspect every item result; one blocked/quarantined item does not stop the page.
Retain `RunFailureCloseSweepCursor` after errors; short pages restart the sweep.
After process loss begin at `None`, not an ever-advancing durable watermark.
Tenant mismatch is rejected. Shutdown preserves pending durable work, including
ambiguous commits that the next sweep recovers.

The business lifecycle remains Active while a close is pending. Runnable/deadline
discovery excludes it, and direct lease claims fail. Use
`load_run_failure_close` for explicit pending/completed status; existing core
lifecycle and Agent snapshot wire formats are unchanged. Hosts exposing a public
status API must translate this distinction deliberately rather than reporting
the parent as executing or already terminal.

## Atomicity, races and accounting

Migration 24 adds an immutable canonical intent with checksum, checkpoint,
Active lifecycle witness, original full Failure, frozen direct usage and exact
worker-journal anchor. Registration serializes on the parent Run row with spawn
and lifecycle writers. The event, decision, all immediate unsettled-child
cancellation work and parent lease release commit in **one transaction**; a
fresh database-clock lease check at the final write can roll everything back.
No provider call, child execution or ancestor lock is held in this transaction.

Once registered, no new parent lease, child slot, checkpoint or competing
cancellation/success outcome is allowed. The first decision wins. A logical
retry returns `Existing` with original evidence before fresh lease/head checks;
this does not assert that a later candidate failure was equivalent. Quarantine
remains available and blocks fresh finalization. Earlier cancellation or terminal
state rejects fresh failure registration. Deadline supervision preserves a close
and returns `FailureClosing` rather than replacing its cause.

Child cancellation uses the existing durable queue, with a verified Active
failure-close witness and public-safe `child.parent_failed` reason. Parent
diagnostic text is not copied to the child. Delivery is not cleanup acknowledgement.
A child with its own sealed failure keeps its original decision: cancellation
delivery stays pending until that child becomes terminal. The next reconciliation
pass records its genuine terminal receipt and once-only settlement.

Finalization adds fully verified, settled child subtree usage to frozen direct
usage **once**, then commits Failed and close completion together. The centralized
terminal-accounting path rejects changed direct totals, including generic
control-plane terminal writes. Database guards reject replacement of the full
original Failure, not just its ID. Source evidence survives audit head changes,
connection/coordinator reconstruction, final completion and retries.

## Deployment and evidence

Migrations 1–23 and published schemas are unchanged. Migration 24 synthesizes no
failure decisions for existing Runs. Startup verifies exact migration checksums,
columns, constraints, valid indexes, enabled triggers and function bodies.
Already-connected older writers are fenced out of failure-closing Runs by the
transaction capability marker. New child reconcilers are required to understand
the Active source witness. These markers are compatibility guards for a trusted
server-side pool, **not SQL-role authentication**. Upgrade in a controlled window;
do not downgrade while retained close evidence exists.

Real PostgreSQL tests cover source/final rollback, expiry after queue capture,
original-cause preservation, unknown usage, changed terminal accounting,
reconstructed worker/handoff recovery, a failure-closing child, 17-item sweeps,
tenant/shutdown/quarantine handling, and populated v23 SQL upgrade/catalog drift.
The separate [process-kill qualification](process-kill-qualification.md) executes
real owned-process termination at six committed drain/replay boundaries, with
exact journal, lifecycle and accounting verification. This is not pre-/in-commit
or live-provider qualification. A separate [COMMIT-loss/fencing profile](commit-loss-qualification.md)
adds source-registration request/response cuts and an expired old worker rejected
after higher-epoch takeover. The whole child execution profile still requires combined process-kill and
failover/restore drills, provider effect/usage recovery qualification, SQL role
isolation, and measured full-sweep latency/capacity under retained history.

Operators must alert on oldest pending close, full-sweep lag, unknown effects or
cost, child cancellation/settlement lag, quarantine and exhausted retries. Never
manually mark children settled or rewrite a failure to clear an alert. There is
no hard closure-latency SLA in this release.
