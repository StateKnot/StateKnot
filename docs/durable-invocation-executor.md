<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable model and tool invocation execution

`stateknot-runtime` now contains the provider-neutral execution boundary between
durable model/tool ledgers and external adapters. It is pre-alpha and
unpublished. This document is the integration and recovery contract already
enforced by code. OpenAI Responses, Anthropic Messages, and one strict MCP
2026-07-28 client-side Remote Tool profile now bind to this contract; that does
not imply broader MCP conformance or live-provider qualification.

A [Simplified Chinese edition](durable-invocation-executor.zh-CN.md) is
maintained alongside this document.

## Implemented boundary

The runtime currently provides:

- startup-only immutable `ModelProviderRegistry` and `ToolProviderRegistry`
  snapshots keyed by exact owner/name/version capability identity;
- full descriptor equality checks against the durable invocation, the startup
  snapshot, and the live object-safe provider before dispatch;
- a trusted `InvocationBudgetProvider` boundary that resolves finite remaining
  capacity from durable run provenance instead of accepting caller-authored
  remaining-budget values;
- paired wall/monotonic clock observations for durable accounting decisions and
  active deadlines;
- durable-before-dispatch model and tool `StartAttempt` commits with stable
  start and terminal event identities;
- unary model execution, semantic streaming validation and accumulation, and a
  required durable stream-event sink;
- tool execution with explicit cancellation/deadline ambiguity for write
  effects;
- bounded identical PostgreSQL mutation retries with exact lost-acknowledgement
  convergence;
- retained terminal handoffs that retry persistence without repeating provider
  or tool I/O; and
- a closed public-safe journal schema that excludes prompts, arguments,
  responses, errors, endpoint identifiers, and credentials.

No database transaction remains open while model or tool code runs.

## Freeze exact providers at startup

Register adapters only after their descriptors and schemas have been validated.
Build a new immutable registry snapshot for a deployment; never mutate provider
selection beneath an active worker.

```rust,ignore
let mut model_bindings = ModelProviderRegistryBuilder::new();
model_bindings.register(model_adapter)?;

let mut tool_bindings = ToolProviderRegistryBuilder::new();
tool_bindings.register(tool_adapter)?;

let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_invocation_execution_event_schema(&mut schemas)?;
register_application_schemas(&mut schemas)?;

let executor = DurableInvocationExecutor::new(
    store,
    schemas.build()?,
    model_bindings.build(),
    tool_bindings.build(),
    budget_provider,
    DurableInvocationExecutorOptions::default(),
)?;
```

Aliases, model-family names, mutable endpoint routing, and fallback selection
must be resolved before an invocation intent is persisted. Recovery uses the
exact durable descriptor; a missing or changed binding fails closed before
external I/O.

## Execute one physical attempt

The caller retains one `ModelAttemptHandoff` or `ToolAttemptHandoff` containing:

- the exact live `RunFence`;
- the prepared or explicitly retryable durable invocation revision;
- one fresh run-wide physical `AttemptId`;
- two distinct, stable `EventId` values for start and terminal facts;
- a cooperative cancellation signal; and
- the required model stream sink or optional tool progress sink.

Call `execute_model` or `execute_tool` once with that handoff. The executor then:

1. validates tenant/run scope and startable ledger state;
2. checks whether the physical attempt has already advanced the durable ledger;
3. resolves the exact provider and trusted remaining budget;
4. commits `StartAttempt` and its journal event;
5. dispatches only after a fresh `Committed` result;
6. validates the provider's complete response, stream, result, or error; and
7. commits the terminal fact against the exact invocation head.

An idempotently observed start returns `Recovered` and never calls the external
adapter. It may represent a lost database acknowledgement or a concurrent
executor, so it is not fresh dispatch authority.

## Streaming contract

A streaming request must include an `Arc<dyn ModelEventSink>`. The runtime
validates and accumulates every semantic `ModelEvent` in sequence, then waits
for the sink to accept that exact event before polling the next one. Sink
implementations must durably deduplicate by `(attempt_id, sequence)` before
exposing an event externally.

The accumulated `ModelResponse` becomes authoritative only after its separate
terminal ledger commit. A missing terminal stream event, sequence violation,
invalid provider error, sink failure, cancellation, or deadline produces a
public-safe model error and no successful response.

## Tool ambiguity is never converted into a blind retry

Read-only tools can record a known cancellation or deadline failure. For
idempotent and non-idempotent writes, cancellation, timeout, or an invalid
failure contract may occur after the external system changed state. The
executor therefore records:

- `FailureCategory::AmbiguousExternalOutcome`;
- `ToolExternalEffect::Unknown`; and
- `RetryAdvice::ReconcileFirst`.

The durable tool ledger remains `Unknown` until application-owned
reconciliation establishes the external outcome. The executor never calls the
tool again merely because its local future was dropped.

`ToolReconciliationHandoff::result` and `::error` now provide the trusted
runtime commit boundary. They accept only evidence bound to the exact unknown
invocation and attempt. Result evidence is additionally checked against the
frozen local output schema before any database mutation. Then
`commit_tool_reconciliation` appends a distinct audit event and advances the
ledger atomically under the live worker fence, without provider lookup or Tool
I/O. Exact event retries converge idempotently; a same-run successor fence can
be attached with `rebind_fence`.

Known effect evidence resolves the invocation to `Failed`, successful evidence
resolves it to `Committed`, and still-unknown effect evidence deliberately
retains `Unknown`. A network service must put authorization and evidence-source
policy in front of this trusted worker/operations API.

Provider SDK retries must also be disabled unless the adapter can prove they
reuse the exact provider request identity and satisfy the durable descriptor's
semantics. StateKnot itself does not hide an external retry.

## Recover a terminal commit without dispatch

If provider I/O finishes but the terminal database commit cannot be confirmed,
`execute_model` or `execute_tool` returns a terminal error that owns the exact
payload-redacted recovery handoff.

```rust,ignore
match executor.execute_model(handoff).await {
    Ok(outcome) => consume(outcome),
    Err(ModelAttemptExecutionError::Terminal(error)) => {
        let recovery = error.into_recovery();
        persist_for_immediate_retry(recovery);
    }
    Err(error) => handle_pre_dispatch_failure(error),
}
```

Retry the retained value only through `commit_model_terminal` or
`commit_tool_terminal`; those methods perform no provider I/O. If the original
lease expired while the external call was in flight, first obtain the current
live fence for the same tenant/run and call `rebind_fence`. The store still
performs the authoritative live-fence check.

Terminal recovery payloads contain application data and are intentionally not
serializable or printable. Keep them in trusted process memory and retry within
the bounded lease-recovery workflow. A process crash after external completion
but before terminal persistence is recovered from the durable executing ledger:
write tools require reconciliation, while model retry policy remains an
application decision based on the persisted model failure/retry contract.

## Budget and deadline ownership

`InvocationBudgetProvider::remaining` must reload the admitted run's immutable
budget and cumulative durable usage, apply policy for the exact invocation and
attempt, and return a finite `BudgetRemaining` at the supplied trusted time.
The executor checks model attempt/turn/token/byte capacity and tool/write-call
capacity before the durable start.

An already-started attempt is recovered before provider lookup, clock access,
or budget evaluation. This ordering is required: a lost acknowledgement must
remain recoverable even if the deployment changed or the remaining budget was
consumed after the original start.

## Public journal schema

Install the schema with
`register_standard_invocation_execution_event_schema`. Its immutable identity
is:

```text
https://stknot.com/schemas/runtime/invocation-execution-event/1.0.0
```

The eight operations cover model/tool preparation, start, and terminal
response/result/error
facts. Each event exposes only binding kind, logical invocation ID, physical
attempt ID, and intent digest. Application payloads remain in their dedicated
bounded ledgers. Reconciliation reuses the immutable Tool result/error operation
shape while its distinct journal event kind makes the recovery action auditable.

## Operational requirements

Instrument at least:

- attempts admitted, durably started, recovered without dispatch, and
  terminally committed by boundary kind;
- provider duration, deadline/cancellation result, and contract violations;
- stream events accepted and durable-sink failures;
- ambiguous tool outcomes awaiting reconciliation;
- mutation retries and retained terminal handoffs; and
- exact-provider lookup and budget-provider failures.

Alert on retained terminal handoffs that cannot commit before lease recovery,
on any contract-violation error, and on growing `Unknown` tool invocations.
Never log descriptor secrets, request/input bytes, responses/results, model
errors, or tool errors from a recovery handoff.

## Qualification evidence and remaining blockers

Real PostgreSQL integration coverage proves:

- a model call whose original fence is superseded retains terminal evidence,
  rebinds to the new live fence, commits once, and does not re-evaluate budget
  or redispatch on retry;
- a seven-event semantic model stream reaches its durable sink in order,
  accumulates into the committed response, and emits nothing on duplicate
  recovery; and
- a timed-out idempotent-write tool records an ambiguous reconcile-first
  outcome, rejects schema-invalid reconciliation without mutation, commits
  authoritative result or known-effect error evidence, converges exact retries,
  and never calls the Tool again; and
- the strict MCP adapter passes the same durable-before-dispatch and
  reconciliation proof over a real loopback MCP exchange and PostgreSQL on
  versions 16 and 17.

The boundary remains pre-alpha. First-party OpenAI Responses and Anthropic
Messages adapters, the typed Agent contract, and durable transcript assembly in
the prebuilt provider-native graph are implemented. An application-persisted
model stream sink, an authorization-first network reconciliation service,
deployment-specific price tables and artifact evidence, telemetry, and live-provider
qualification are still required before production support can be claimed.
