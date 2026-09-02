<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot documentation

This directory contains the normative design inputs for StateKnot. Claims in
the project README remain aspirational until backed by implementation,
conformance output, and the release gates in these documents.

## Start here

1. [v1 scope baseline](v1-scope.md) — the capabilities, guarantees, supported
   environment, and explicit exclusions that control implementation work.
2. [Qualification scenarios](scenarios/README.md) — the three production-shaped
   workloads, failure models, and measurable release criteria.
3. [Research and implementation plan](research-and-implementation-plan.md) —
   ecosystem research, product boundaries, architecture, execution guarantees,
   protocols, security, operations, and release gates.
4. [Completeness audit](plan-completeness-audit.md) — the initial scope audit,
   its resolved items, and the remaining decisions that block the public API.
5. [Roadmap](roadmap.md) — ordered milestones and exit criteria.
6. [RFC process](rfcs/README.md) — how durable project decisions are proposed,
   reviewed, accepted, and superseded.
7. [PostgreSQL provider operations](postgresql-provider.md) — the implemented
   durability slice, deployment boundary, validation, and explicit blockers.
8. [Durable Graph runtime](durable-graph-runtime.md) — production integration
   contract for executable registration, noninitial replay, fenced driving,
   lifecycle handoffs, and crash recovery. A [Simplified Chinese edition](durable-graph-runtime.zh-CN.md)
   is maintained alongside it.
9. [Durable Agent Loop and tenant scheduler](durable-agent-loop.md) — production
   integration contract for trusted lifecycle evidence, atomic Wait/Terminal/
   failure commits, lost-ack recovery, and tenant-scoped scheduling. A
   [Simplified Chinese edition](durable-agent-loop.zh-CN.md) is maintained
   alongside it.
10. [Durable model and tool invocation execution](durable-invocation-executor.md)
    — exact provider registration, trusted budget admission,
    durable-before-dispatch calls, streaming, ambiguity, and terminal recovery.
    A [Simplified Chinese edition](durable-invocation-executor.zh-CN.md) is
    maintained alongside it.
11. [Typed Agent and first-party model adapters](typed-agent.md) — generated
    digest-pinned schemas, bounded typed codecs, OpenAI Responses and Anthropic
    Messages unary/SSE bindings, compiled examples, and the explicit durable
    execution boundary. A [Simplified Chinese edition](typed-agent.zh-CN.md) is
    maintained alongside it.
12. [Durable Agent admission](durable-agent-admission.md) — immutable
    authenticated intent, database-clock commit, atomic run/event/checkpoint
    initialization, exact retry, migration, and sensitive-data operations. A
    [Simplified Chinese edition](durable-agent-admission.zh-CN.md) is maintained
    alongside it.
13. [Cross-tenant durable fair scheduling](cross-tenant-fair-scheduler.md) —
    immutable weighted policy, replica-safe global reservations, explicit
    starvation bounds, retention, rollout, and operations. A
    [Simplified Chinese edition](cross-tenant-fair-scheduler.zh-CN.md) is
    maintained alongside it.
14. [Provider-native Agent graph](provider-native-agent.md) — digest-pinned
    model/tool composition, sequential transcript recovery, local policy,
    exact accounting, two-phase cancellation, operations, and PostgreSQL
    evidence. A [Simplified Chinese edition](provider-native-agent.zh-CN.md) is
    maintained alongside it.
15. [General stateless MCP Tool client](mcp-client.md) — bounded dynamic Tool
    discovery/calls, JSON and request-scoped SSE, custom headers, MRTR, security
    boundaries, and pinned official non-OAuth conformance evidence. A
    [Simplified Chinese edition](mcp-client.zh-CN.md) is maintained alongside
    it.
16. [MCP conformance status](mcp-conformance.md) — exact frozen runner identity,
    passed and skipped checks, CI reproduction, and the explicit boundary that
    excludes OAuth, server, and SDK-tier claims. A
    [Simplified Chinese edition](mcp-conformance.zh-CN.md) is maintained
    alongside it.

Current drafts include the [core domain contract](rfcs/0001-core-domain-and-capability-model.md),
the [deterministic graph and scheduler contract](rfcs/0002-deterministic-graph-and-scheduler.md),
and the [PostgreSQL durability contract](rfcs/0003-postgresql-durability-recovery-and-migration.md).

## Normative language

RFCs marked `Accepted` define project contracts. Research documents explain
intent and trade-offs but do not override accepted RFCs or released API
documentation. Terms such as MUST, SHOULD, and MAY are interpreted as described
by RFC 2119 only when an accepted RFC explicitly says so.
