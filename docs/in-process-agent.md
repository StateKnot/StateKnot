<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Run a typed Agent in process

Status: implementation-backed pre-release API. This path is for a single
application process or a developer deployment that still needs real PostgreSQL
durability. It is not an in-memory executor and does not bypass authorization,
admission, the Graph Driver, fenced invocation attempts, or maintenance.

[Simplified Chinese](in-process-agent.zh-CN.md)

## What the convenience layer owns

`InProcessAgentRuntime` removes the HTTP listener, not the durable runtime. It
owns and supervises exactly two roles:

- one tenant-scoped `AgentWorker` for durable scheduling and execution;
- one `AgentMaintenance` role for deadlines, child cancellation/settlement,
  Join publication, and failure close.

Maintenance starts first. The Worker starts only after both actual readiness
checks pass and pauses when maintenance is unavailable. An unexpected role exit
causes fail-stop drain of its sibling. `shutdown().await` drains the Worker
first, then maintenance, and returns both joined reports. Dropping the owner
initiates cleanup but cannot prove it completed.

The binding requires the same qualified `PostgresStore`, frozen
`ExecutableGraphRegistry`, `AgentServiceRegistry`, authorizer, lifecycle
evidence provider, tenant, and finite role options used by the production host.
It never creates a test principal, installs an allow-all policy, runs a schema
migration, or falls back to process memory.

## Submit one recoverable typed request

Build and bind `TypedAgent<I, O>` as described in the
[typed Agent guide](typed-agent.md), then bind its authenticated caller:

```rust,no_run
# use std::time::Duration;
# use stateknot::{core::{AgentSubmissionKey, BudgetLimits}, in_process_agent::*, runtime::{AgentServiceCaller, TypedAgent}};
# use serde::{Serialize, de::DeserializeOwned};
async fn execute<I, O>(
    runtime: &InProcessAgentRuntime,
    codec: TypedAgent<I, O>,
    caller: AgentServiceCaller,
    key: AgentSubmissionKey,
    input: I,
) -> Result<InProcessAgentRun<O>, InProcessAgentRunError>
where
    I: Serialize,
    O: DeserializeOwned,
{
    runtime
        .agent(codec, caller)
        .expect("caller tenant was qualified for this runtime")
        .with_options(InProcessAgentRunOptions::new(
            Duration::from_millis(50),
            Duration::from_secs(30),
        ).expect("static bounds are valid"))
        .run(InProcessAgentRequest::new(key, input, BudgetLimits::empty()))
        .await
}
```

The application owns the `AgentSubmissionKey` and must persist it beside the
logical request. Reusing the same key and byte-equivalent content returns the
original run; reusing it for different content fails with a conflict. StateKnot
does not generate a hidden key because a caller could not recover a lost
acknowledgement without it.

The in-process runner performs a fresh authorized lookup by that exact
submission key after admission and on every poll. A concrete resource policy
can therefore grant `RunPermission::Read` for
`RunAccessTarget::Submission(key.digest_for(&tenant))` without granting
`TenantRuns` or Run-ID-wide reads. The returned Run ID is checked against the
admission result; submission authorization alone never implies read access.

`run` returns the full durable state:

| Outcome | Meaning | Caller action |
| --- | --- | --- |
| `Succeeded` | Provenance, complete budget accounting, pinned output schema, and typed decoding passed | Use `output`; retain the snapshot for audit if needed |
| `Failed` / `Cancelled` | A terminal public-safe failure committed | Inspect the snapshot outcome; never retry by inventing a new key blindly |
| `Pending(WaitTimeout)` | The caller wait expired; the run was not cancelled | Retry the same key/content or poll through the service/HTTP API |
| `Pending(RuntimeUnavailable)` | Local roles are draining or stopped | Start a replacement runtime and retry the same key/content |
| `Quarantined` | Integrity or operator policy removed the run from execution | Escalate to an operator; do not dispatch it elsewhere |

Cancelling or dropping the `run` future changes no durable lifecycle state.
Explicit user cancellation still goes through `AgentServiceV1` so authorization
and the two-phase cancellation record remain authoritative.

The complete checked example is
[`crates/stateknot/examples/in_process_agent.rs`](../crates/stateknot/examples/in_process_agent.rs):

```bash
cargo check -p stateknot --example in_process_agent --locked
```

## Move to the production service without rewriting the Agent

The in-process path and `AgentHost` use the same descriptors, schema digests,
graphs, authorizer, PostgreSQL rows, submission keys, Worker, and maintenance
state machines. Migration is therefore an ownership change, not a data or Agent
rewrite:

1. keep the exact executable and service registry revisions available;
2. deploy the separately qualified `AgentHost` behind loopback TLS termination;
3. stop accepting local submissions, then join the in-process runtime;
4. send the same authenticated caller identity, request, and retained
   submission key to Agent HTTP v1;
5. let the production Worker recover any nonterminal run from PostgreSQL.

Do not run two tenant schedulers accidentally. Multiple replicas require the
documented database fencing and capacity qualification; the convenience layer
does not claim multi-process rollout, public ingress, telemetry export, backup,
or release SLOs. See [owned Agent host](agent-host.md),
[PostgreSQL configuration](postgresql-configuration.md), and
[host qualification](host-qualification.md) for those production boundaries.
