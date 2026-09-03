<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot

[![CI](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml)
[![Supply chain](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Durable agent orchestration for Rust.**

[Website](https://stknot.com) · [English documentation](https://stknot.com/docs/) ·
[中文文档](https://stknot.com/zh/docs/)

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

The project is in the **architecture-contract and durable-runtime vertical-validation phase**.
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
`schedule_delayed_retry_wakeup` now projects a deferred-only recovery plan into
migration 12's independent `scheduler_not_before` gate. The same atomic
transaction revalidates checkpoint, journal, live fence, lifecycle, and
database time, preserves the run's queue age, and releases ownership. Ordinary
claims cannot bypass the gate; the tenant scheduler index exposes the run at
the inclusive due instant without a per-run timer update. Lost scheduling
acknowledgements converge exactly, while a delay that becomes due during the
transaction keeps its lease for immediate replanning. Upgrade, corruption,
direct-claim, due-race, and indexed-visibility behavior pass on PostgreSQL
16/17.
Complete ready-set barriers now atomically bind and consume the exact immutable
result set while committing their event, successor checkpoint, lifecycle
projection, and run heads; the specialized wait-barrier commits that same unit
with a complete interrupt/timer batch. Raw successor checkpoint, generic wait
projection, and pending-result writes that bypass their durable evidence are
rejected.
The core now compiles a bounded declarative graph into canonical JSON and an
exact SHA-256 identity, rejects invalid topology before admission, and derives
one deterministic barrier intent from a complete result set. Planning validates
the pinned checkpoint, schemas and reducer reference, applies updates in stable
`NodeId` order, and resolves continue, route, wait, or terminal control without
opening a storage transaction. Migration 13 adds an immutable tenant-scoped
compiled-graph registry. Registration is idempotent only for identical bytes;
claimed recovery reloads and recompiles the exact checkpoint-pinned definition,
checks its redundant identity/digest projections, and quarantines missing or
contradictory evidence under the live fence.
The new unpublished `stateknot-runtime` crate now freezes an offline,
digest-pinned JSON Schema 2020-12 registry, exact graph/reducer/node executable
bindings, independent bounded replay of every committed noninitial checkpoint,
and a fenced durable Graph Driver. The Driver durably starts a physical node
attempt before spawning node code, never launches from an idempotently observed
start, refreshes near-expiry ownership before launch, renews leases beneath a
database-time-derived monotonic expiry watchdog, propagates cooperative cancellation,
commits success/failure against the latest exact journal head, automatically
advances Continue barriers, and returns typed lease-bound handoffs for
Wait/Terminal or blocked failure supervision. Root-to-terminal continuation,
same-fence duplicate suppression, near-expiry launch protection, higher-fence
crash takeover, and execution beyond the original lease pass on PostgreSQL 16
and 17.
The same runtime now includes a fenced lifecycle coordinator, a bounded durable
Agent Loop, and a tenant-scoped scheduler worker. Wait barriers materialize
their complete interrupt/timer batch with database time; successful Terminal
and supervised failure transitions validate trusted admission/accounting
evidence before one atomic PostgreSQL commit; stable lifecycle event identities
make lost acknowledgements exactly retryable. The scheduler scans one tenant's
fixed-cutoff runnable pages, reuses a stable claim attempt identity, claims at
most one run per tick, and exposes bounded contention and retry counters.
The runtime now also freezes exact model and tool provider registries and
executes their durable attempts through trusted budget/deadline admission,
durable-before-dispatch starts, validated unary or durably-sunk streaming model
results, reconciliation-safe tool failures, one bounded original-attempt
provider probe with durable `Pending` retries, atomic reconciliation evidence,
and no-dispatch terminal recovery.
Migration 14 and `DurableFairScheduler` add an immutable shard-scoped smooth
weighted policy, globally ordered lost-ACK-safe reservations, exact cycle
shares, explicit reservation-count starvation bounds, and bounded
database-time retention. Twenty-seven runtime scenarios and 102 provider cases are
mandatory on both PostgreSQL 16 and 17.
The repository now also includes a schema-pinned typed Agent contract plus
first-party OpenAI Responses and Anthropic Messages unary/SSE adapters with
bounded transport controls and lossless provider-native unary tool continuation.
Migration 15 and `DurableAgentAdmission` now validate and atomically commit an
immutable authenticated Agent intent, database-clock admission, Active
lifecycle, sequence-one audit event, superstep-zero checkpoint, and scheduler
projections. Exact retries recover the original verified commit; conflicting
input, policy, graph, state, or identities fail closed.
Migration 16 and `DurableAgentRuns` add tenant-scoped durable ingress keys,
fresh-ID lost-ACK convergence, changed-content conflicts, fully revalidated
public run snapshots, and terminal success/failure/cancellation results.
The prebuilt `ProviderNativeAgentGraph` now composes sequential multi-turn
model/tool execution, provider-native transcript reconstruction, digest-pinned
local policy, exact deterministic accounting, no-redispatch recovery, known
failed-Tool continuation, and two-phase cancellation confirmation over those
durable layers. Migration 17 binds pending Tool results to their exact
`committed` or `failed` terminal revision instead of fabricating success.
`AgentServiceV1` now adds an exact-version, authorization-first embedding
boundary for tenant-scoped submission recovery, verified run/key reads, and
caller-retained two-phase cancellation identities. Its control event records
only public-safe admission/policy/decision digests and a stable failure ID.
`McpRemoteTool` now implements the first strict protocol adapter: MCP
2026-07-28 modern stateless discovery, complete JSON responses, exact local
schema and server-identity pins, attempt-scoped authorization, bounded
transport, and reconciliation-first ambiguous writes. It is a client-side
Remote Tool profile, not a complete MCP client/server conformance claim.
The separate `McpClient` now implements the general stateless MCP 2026-07-28
Tool surface: bounded discovery and pagination, JSON/request-scoped SSE,
standard and nested custom headers, per-request authorization, invalid-Tool
isolation, no network schema dereference, and exact multi-round request-state
handling. `McpOAuthAuthorization` adds challenge-driven protected-resource and
authorization-server discovery, pre-registration/CIMD/DCR, PKCE, issuer and
callback validation, scope upgrade, refresh, bounded replay, and caller-owned
durable stores. The pinned official runner gate covers all 32 scored client
scenarios, including all 25 OAuth scenarios: 373 scored assertions succeed,
zero fail, and 11 capability/method checks outside the advertised Tool surface
are explicitly skipped. Seven explicitly not-scored extensions remain reported
and unclaimed.
The new StateKnot-owned MCP Server application layer composes immutable,
bounded Tools, Resources, Resource Templates, Prompts, and optional Completion
behind the production stateless HTTP boundary. It enforces authentication,
admission, authorization-before-disclosure, exact scopes, offline JSON Schema
2020-12 validation, principal-bound pagination, output validation, progress,
cancellation, and integrity-bound multi-round request state without exposing
the official SDK's domain types. The pinned official Server gate covers all 37
scored scenarios: 114 assertions succeed, five capability checks skip, one SSE
check is informational, and zero fail or warn. Three unscored schema/header
gates add 32 successes. The conformance fixture validates the production
transport; StateKnot's registry and policy layer has separate real-HTTP tests.
MCP Tasks, broader client extensions, stable API/SDK-tier claims, and complete
framework production qualification remain unimplemented.
The new A2A 1.0 Server profile keeps official SDK wire types private behind
bounded StateKnot-owned Agent Card, message, task, artifact, stream, and push
contracts. Its HTTP+JSON and JSON-RPC/SSE boundary enforces exact Host/Origin/
route/version/extension policy, authentication before body parsing,
authorization before task/config lookup, caller-owned replica admission,
bounded unary/stream responses, and graceful shutdown. The checksum-pinned
official TCK gate collects 265 cases: 177 pass, 88 are declared skips, and zero
fail, error, or xfail. Critical streaming, multi-subscriber, authenticated push,
extended-card, caching, error-mapping, and unknown-field cases must execute.
The separate strict A2A 1.0 Client implements all eleven HTTP+JSON and JSON-RPC
operations plus both SSE surfaces. Discovery freezes the bounded Agent Card,
server-preferred interface, exact egress pin, extensions, tenant, and security
alternative. The current authenticated profile accepts complete single-scheme
HTTP Bearer, OAuth 2.0, or OpenID Connect requirements; API-key, Basic, mTLS,
and multi-scheme profiles remain explicit exclusions. `A2aRemoteAgent` binds
one advertised skill and local input/output schemas to the existing durable
Tool execution contract with explicit
AtMostOnce or operator-attested message-ID deduplication semantics. Real
loopback tests execute the complete operation matrix over both bindings; a
real PostgreSQL test proves durable state before A2A send, lost-response
`Unknown`, and duplicate no-redispatch. Optional operator-attested recovery can
now query bounded context/task history without resend or replay the exact
message ID only under durable deduplication evidence. The provider-native Agent
turn converts `Pending` into a durable delayed retry and commits authoritative
evidence without repeating the business call. Official Client conformance,
live-peer recovery qualification, gRPC, and a production durable server-side
task/push backend remain separate gates.
The strict MCP adapter is also composed with the durable invocation executor
in a real PostgreSQL + loopback test: durable start is observed before request
I/O, lost write responses remain unknown, duplicate execution never
redispatches, and authoritative result reconciliation is schema-validated,
fenced, atomic, and exactly idempotent on PostgreSQL 16 and 17. The runtime
PostgreSQL suite separately proves the authoritative error branch.
Stable network Agent/cancellation transport, production protocol-specific
outbox dispatch adapters, A2A Client live-peer/conformance and reconciliation
attestation qualification, A2A gRPC, artifact retrieval, parallel
siblings/Tools, output repair, loops/subgraphs, role isolation, general
retention, failover, restore, and the final stale-race gates have not shipped
yet.

The current milestone is to:

1. preserve the completed PostgreSQL-backed strict MCP recovery proof as a
   mandatory PostgreSQL 16/17 gate;
2. preserve the separate MCP Client and Server profiles and their exact pinned
   32-client/37-server official gates while completing stable API review and
   keeping Tasks and other extensions behind independent claims;
3. preserve the A2A 1.0 HTTP+JSON/JSON-RPC Server gate and implemented durable
   Client/outbound/reconciliation boundary while adding official/live-peer
   qualification of the exact deployment attestations;
4. validate the three frozen production scenarios and accept the core domain,
   graph, durability, and protocol/security RFCs;
5. qualify role isolation, failover/restore, and the final stale-race gates; and
6. publish compatibility and performance evidence before claiming support.

See the [qualification scenarios](docs/scenarios/README.md), the
[roadmap](docs/roadmap.md), the full
[research and implementation plan](docs/research-and-implementation-plan.md),
the [PostgreSQL provider operations guide](docs/postgresql-provider.md), and the
[typed Agent and first-party adapters](docs/typed-agent.md),
[durable Agent admission](docs/durable-agent-admission.md),
[durable Agent runs and results](docs/durable-agent-runs.md),
[provider-native Agent graph](docs/provider-native-agent.md),
[AgentService v1](docs/agent-service.md),
[strict MCP Remote Tool profile](docs/mcp-remote-tool.md),
[general stateless MCP Tool client](docs/mcp-client.md),
[MCP OAuth client authorization](docs/mcp-oauth.md),
[MCP Server profile](docs/mcp-server.md),
[MCP conformance status](docs/mcp-conformance.md),
[A2A 1.0 Client and durable remote-agent profile](docs/a2a-client.md),
[A2A 1.0 Server profile](docs/a2a-server.md),
[A2A 1.0 conformance status](docs/a2a-conformance.md),
[durable invocation](docs/durable-invocation-executor.md),
[fair scheduling](docs/cross-tenant-fair-scheduler.md), and
[completeness audit](docs/plan-completeness-audit.md) guides.

## Repository layout

```text
crates/stateknot/        Unpublished facade crate used to validate the workspace
crates/stateknot-core/   Validated domain, run, journal, checkpoint, invocation, and ownership contracts
crates/stateknot-integrations/  OpenAI/Anthropic adapters plus bounded MCP and A2A protocol profiles
crates/stateknot-runtime/  AgentService v1, executable/provider registries, durable Driver, invocation executor, Agent Loop, and fair scheduler
crates/stateknot-store-postgres/  PostgreSQL journal/checkpoint/invocation/lease/outbox durability slice
docs/                    Architecture contracts, qualification scenarios, and roadmap
website/                 Bilingual Astro docs, browser tests, and Caddy deployment
.github/                 Contribution templates and automated quality gates
```

Additional crates will be created only after their dependency or semantic
boundaries are proven. Empty provider and protocol crates are deliberately
avoided.

## Development

The repository pins Rust 1.88.0, the minimum supported Rust version required by
the official MCP Rust SDK 3.x protocol adapter.
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
