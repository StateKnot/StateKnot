<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot

[![CI](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml)
[![Supply chain](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Durable agent orchestration for Rust.**

StateKnot is an open-source Rust framework under development for building typed,
durable, observable, and protocol-native agent systems. It is designed as a
Rust-native runtime rather than a line-by-line port of a Python agent framework.

> [!IMPORTANT]
> StateKnot is currently **pre-alpha**. The repository contains the reviewed
> architecture baseline and project infrastructure, but no production release
> or stable public API yet. Do not use it in production at this stage.

## Direction

StateKnot is being designed around five commitments:

- **Typed by default:** explicit state, tool schemas, structured output, and
  capability negotiation instead of untyped maps flowing through the runtime.
- **Durable execution:** journaled events, checkpoints, pause and resume,
  lease/fencing, transactional outbox, and realistic external-side-effect
  guarantees.
- **Graph and agent ergonomics:** a direct agent loop for common cases plus
  deterministic typed graphs for branching, parallelism, joins, loops, and
  human approval.
- **Protocol-native interoperability:** first-class MCP and A2A adapters without
  leaking their wire types into the stable core domain model.
- **Production governance:** tenant isolation, policy enforcement, budgets,
  auditability, OpenTelemetry, evaluation, failure injection, and compatibility
  testing are design requirements rather than optional add-ons.

The v1 scope baseline targets PostgreSQL-backed execution, OpenAI-compatible and
Anthropic model adapters, MCP client/server support, and A2A REST/JSON-RPC
client/server support. Its supported surface and explicit exclusions are
recorded in the [v1 scope baseline](docs/v1-scope.md).

## Current milestone

The project is in the **architecture-contract and vertical-validation phase**.
The unpublished core crate validates model, tool, agent admission/result,
durable run-lifecycle, canonical journal-envelope, graph-checkpoint,
tool- and model-invocation state machines, immutable pending node results,
physical node-attempt recovery, fixed-fence at-least-once outbox contracts, and
integrity-bound interrupt request/resolution plus timer registration/firing
records with exact authorization and journal causality.
The PostgreSQL 16/17 durability slice now implements
run admission, canonical journal append/read,
locked lifecycle transitions, projection-bound idempotency, immutable superstep
checkpoints, exact checkpoint parenting, bounded reverse-lineage verification,
immutable hash-linked tool- and model-invocation ledgers, exact journal
anchoring, a run-wide node/tool/model/outbox physical-attempt registry, durable node
starts and append-only completions, database-time retry gates, higher-fence
crash takeover, checkpoint advancement guards for unsettled invocations,
attempt-owned immutable pending node results with exact committed invocation
bindings and semantic idempotency, bounded verified attempt/result paging,
an indexed tenant-scoped runnable projection, database-time fixed keyset pages
whose decoded memory is hard-bounded, lease-expiry-aware discovery without
per-run polling, a transactional outbox with immutable destination snapshots,
atomic event enqueue, durable-before-dispatch fixed attempts, explicit
at-least-once retry/dead-letter/expiry recovery, schema verification, and
database-enforced lease fencing. Integrity-bound interrupt requests and durable
timers now persist with database-clock resolution/firing, indexed due/expiry
discovery, exact audit loads, and explicit cancellation/failure abandonment.
Recovery and trusted control-plane code can also commit a structured run
quarantine outside a potentially corrupt journal: the request binds a stable
ID, closed cause, bounded non-secret component, evidence checksum, and exact
journal observation, then atomically clears execution ownership and removes the
run from scheduler discovery with lost-ack convergence.
Read-only recovery validations can use `with_corruption_quarantine` to pass
successes and ordinary errors through while mapping only payload-redacted
`CorruptData` into that transaction; a stale journal observation fails instead
of quarantining newer durable state. Claimed workers can instead enter the
fence-bound `ClaimedRunRecovery` surface: creation and final revalidation check
the exact live lease and journal observation, all exposed bounded recovery
pages inherit one stable quarantine intent, and migration 11 records the
detecting attempt/epoch in a v2 audit digest. The quarantine transaction repeats
that unexpired fence predicate, so a superseded worker cannot stop its successor
even when no journal event separated their leases.
`ClaimedRunRecovery::plan_ready_nodes` now pins that verified checkpoint and
journal observation, derives canonical root-node activations, streams immutable
results and complete attempt histories through bounded verifiers, and emits a
stable `NodeId`-ordered classification of completed, dispatchable, deferred,
same-fence in-flight, terminally failed, or hard-limit-exhausted work at a
database-observed time. Per activation, 64 physical attempts is the closed
safety ceiling enforced by both planning and the start transaction.
Completed siblings become exact barrier inputs; contradictory activation,
result, attempt, fence, journal, or clock evidence fails closed through the
same fenced quarantine path. `start_recovered_node_attempt` is the production
handoff for a dispatchable decision: it binds the plan to an exact worker
append and atomically commits/revalidates the durable physical start before
node code. Only a newly `Committed` start grants that caller launch authority;
an `Idempotent` result is treated as already in flight and can be orphan-recovered
only under a higher fence. Lost acknowledgements and 24-way identical-start
races converge on one physical row on PostgreSQL 16/17.
Complete ready-set barriers now atomically bind and consume the exact immutable
result set while committing their event, successor checkpoint, lifecycle
projection, and run heads; the specialized wait-barrier commits that same unit
with a complete interrupt/timer batch. Raw successor checkpoint, generic wait
projection, and pending-result writes that bypass their durable evidence are
rejected. Protocol-specific outbox dispatch adapters, cross-tenant fairness,
pinned graph-registry revalidation, route/reducer/successor evaluation, durable
delayed-retry wakeup, the complete recovery/barrier dispatch loop, and a
runnable agent loop have not shipped yet.

The current milestone is to:

1. validate the three frozen production scenarios and their load/failure models;
2. accept the core domain, graph, durability, and protocol/security RFCs;
3. prove one end-to-end durable execution path with crash recovery;
4. publish compatibility and performance evidence before claiming support.

See the [qualification scenarios](docs/scenarios/README.md), the
[roadmap](docs/roadmap.md), the full
[research and implementation plan](docs/research-and-implementation-plan.md),
the [PostgreSQL provider operations guide](docs/postgresql-provider.md), and the
[completeness audit](docs/plan-completeness-audit.md).

## Repository layout

```text
crates/stateknot/        Unpublished facade crate used to validate the workspace
crates/stateknot-core/   Validated domain, run, journal, checkpoint, invocation, and ownership contracts
crates/stateknot-store-postgres/  PostgreSQL journal/checkpoint/invocation/lease/outbox durability slice
docs/                    Architecture contracts, qualification scenarios, and roadmap
website/                 Public Astro site, browser tests, and Caddy deployment
.github/                 Contribution templates and automated quality gates
```

Additional crates will be created only after their dependency or semantic
boundaries are proven. Empty provider, protocol, and runtime crates are
deliberately avoided.

## Development

The repository pins Rust 1.85.0, the initial minimum supported Rust version.
With `rustup` installed, the toolchain is selected automatically.

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

The public site has its own locked Node.js toolchain and verification workflow;
see the [website guide](website/README.md).

Before proposing an implementation, read [CONTRIBUTING.md](CONTRIBUTING.md).
Changes to public APIs, durable semantics, protocols, persistence, or security
boundaries require an RFC.

## Community and security

- Use GitHub issues for reproducible bugs and scoped feature proposals.
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
- Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md).
- Project decision-making is documented in [GOVERNANCE.md](GOVERNANCE.md).

## License

StateKnot is licensed under the [Apache License 2.0](LICENSE). Contributions are
accepted under the same license and require a Developer Certificate of Origin
sign-off. The license does not grant rights to project names or logos.
