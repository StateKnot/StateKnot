<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot documentation

Latest execution guide: [Owned scheduling Worker](agent-worker.md)
([中文](agent-worker.zh-CN.md)) — fixed concurrency, actual readiness, joined
shutdown and fresh-process recovery, independent of HTTP ingress.

Service boundary guide: [Agent resource authorization](agent-resource-policy.md)
([中文](agent-resource-policy.zh-CN.md)) — concrete default-deny policy,
retained configuration, exact resource grants, freshness and recovery semantics.

This directory contains the normative design inputs for StateKnot. Claims in
the project README remain aspirational until backed by implementation,
conformance output, and the release gates in these documents.

## Start here

For database deployment, use the [trusted PostgreSQL role profile](postgresql-roles.md)
([简体中文](postgresql-roles.zh-CN.md)): executable migration/runtime/retention
privilege separation, audit, rollout and exact qualification boundaries.

The implemented durable-child integration boundaries are documented in
[Durable child runs](durable-child-runs.md) and its
[Chinese edition](durable-child-runs.zh-CN.md): ownership/accounting, cancellation,
opt-in Join execution and publication, [deadlines](agent-deadlines.md), and
[failure close](failure-close.md) ([中文](failure-close.zh-CN.md)). The complete
production profile remains gated by combined recovery, role-isolation and
measured capacity qualification; individual implemented slices do not remove
those gates. [Committed-boundary process-kill evidence](process-kill-qualification.md)
([中文](process-kill-qualification.zh-CN.md)) defines the executable six-point
failure-close recovery profile and its explicit exclusions.
The companion [COMMIT-loss/fencing profile](commit-loss-qualification.md)
([中文](commit-loss-qualification.zh-CN.md)) covers source-registration request and
response loss plus a retained old worker across real lease expiry and takeover.
The [successful Child Join process-loss profile](child-join-process-qualification.md)
([中文](child-join-process-qualification.zh-CN.md)) verifies eight committed
admission/Join/terminal/settlement/publication/takeover/resume/replay boundaries,
including a retained old worker rejected before higher-epoch recovery continues.
The [deadline cancel-and-join process-loss profile](deadline-join-process-qualification.md)
([中文](deadline-join-process-qualification.zh-CN.md)) adds nine committed
deadline/cancellation/settlement/cleanup boundaries and preserves the child's
first deadline reason when parent propagation arrives later.
The [Join/deadline COMMIT-loss profile](join-deadline-commit-loss-qualification.md)
([中文](join-deadline-commit-loss-qualification.zh-CN.md)) adds a six-cell
client-fault matrix for Join registration, Join publication and deadline
cancellation, covering both an unforwarded COMMIT and a committed response that
never reaches the worker.
The [child-admission COMMIT-loss profile](child-admission-commit-loss-qualification.md)
([中文](child-admission-commit-loss-qualification.zh-CN.md)) separately proves
all-or-nothing rollback and original-identity recovery for atomic child creation,
ownership, initial checkpoint, parent audit and cumulative budget reservation.
The [child-cancellation delivery COMMIT-loss profile](child-cancellation-commit-loss-qualification.md)
([中文](child-cancellation-commit-loss-qualification.zh-CN.md)) adds exact rollback
and original-receipt recovery for cancellation, real Wait abandonment, scheduler
re-entry and the durable delivery queue.
The [child-settlement COMMIT-loss profile](child-settlement-commit-loss-qualification.md)
([中文](child-settlement-commit-loss-qualification.zh-CN.md)) proves atomic
terminal accounting, notification removal, original-event recovery and
subsequent parent closure after both ambiguous COMMIT cuts.
The [parent-finalization COMMIT-loss profile](parent-finalization-commit-loss-qualification.md)
([中文](parent-finalization-commit-loss-qualification.zh-CN.md)) separately proves
atomic terminal failure, exact direct-plus-child accounting, original-event
recovery and once-only close completion across both ambiguous COMMIT cuts.
The [child-Join consumption COMMIT-loss profile](child-join-consumption-commit-loss-qualification.md)
([中文](child-join-consumption-commit-loss-qualification.zh-CN.md)) proves atomic
Join consumption with the physical node result and completion, original-event
recovery, and non-initial graph replay after both ambiguous COMMIT cuts.
The four [public core contract examples](core-contract-examples.md)
([中文](core-contract-examples.zh-CN.md)) compile the first Agent, typed Tool,
Model stream and explicit protocol mapping on MSRV while locking the reviewed
runtime-neutral direct dependency boundary. They close only RFC-0001 validation
item 1 and do not claim API stability or runtime execution. The same guide
documents the closed 38-file compatibility fixture catalog, whose exact content
digests and RFC 8785 root make current evidence drift fail CI. The corpus now
includes complete Tool authorization receipts and Skill approval, open-request,
acting-window, and revocation evidence without claiming that RFC-0001 validation
item 2 already has exhaustive type coverage.

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
   The [startup configuration guide](postgresql-configuration.md)
   ([简体中文](postgresql-configuration.zh-CN.md)) defines the production-safe
   environment contract, role-separated auto-migration, builder API and explicit
   local-development profile.
8. [Durable Graph runtime](durable-graph-runtime.md) — production integration
   contract for executable registration, noninitial replay, fenced driving,
   canonical bounded sibling batches, lifecycle handoffs, and crash recovery.
   A [Simplified Chinese edition](durable-graph-runtime.zh-CN.md) is maintained
   alongside it.
9. [Durable Agent Loop and tenant scheduler](durable-agent-loop.md) — production
   integration contract for trusted lifecycle evidence, atomic Wait/Terminal/
   failure commits, lost-ack recovery, and tenant-scoped scheduling. A
   [Simplified Chinese edition](durable-agent-loop.zh-CN.md) is maintained
   alongside it.
10. [Durable model and tool invocation execution](durable-invocation-executor.md)
    — exact provider registration, trusted budget admission,
    durable-before-dispatch calls, staged ordered Tool coordination, streaming,
    ambiguity, and terminal recovery.
    A [Simplified Chinese edition](durable-invocation-executor.zh-CN.md) is
    maintained alongside it.
11. [Typed Agent and first-party model adapters](typed-agent.md) — generated
    digest-pinned schemas, bounded typed codecs, OpenAI Responses and Anthropic
    Messages unary/SSE bindings, compiled examples, and the explicit durable
    execution boundary. A [Simplified Chinese edition](typed-agent.zh-CN.md) is
    maintained alongside it.
12. [In-process typed Agent execution](in-process-agent.md) — HTTP-free ownership
    of the real durable Worker and maintenance roles, caller-retained submission
    keys, bounded waits, exact terminal decoding, recovery, and the migration
    path to `AgentHost`. A [Simplified Chinese edition](in-process-agent.zh-CN.md)
    is maintained alongside it.
13. [Durable Agent admission](durable-agent-admission.md) — immutable
    authenticated intent, database-clock commit, atomic run/event/checkpoint
    initialization, exact retry, migration, and sensitive-data operations. A
    [Simplified Chinese edition](durable-agent-admission.zh-CN.md) is maintained
    alongside it.
14. [Cross-tenant durable fair scheduling](cross-tenant-fair-scheduler.md) —
    immutable weighted policy, replica-safe global reservations, explicit
    starvation bounds, retention, rollout, and operations. A
    [Simplified Chinese edition](cross-tenant-fair-scheduler.zh-CN.md) is
    maintained alongside it.
15. [Provider-native Agent graph](provider-native-agent.md) — digest-pinned
    model/tool composition, bounded parallel read-only waves with serialized
    writes, ordered transcript recovery, local policy, exact accounting,
    two-phase cancellation, operations, and PostgreSQL evidence. A [Simplified
    Chinese edition](provider-native-agent.zh-CN.md) is maintained alongside it.
16. [General stateless MCP Tool client](mcp-client.md) — bounded dynamic Tool
    discovery/calls, JSON and request-scoped SSE, custom headers, MRTR, security
    boundaries, OAuth challenge integration, and pinned official conformance
    evidence. A
    [Simplified Chinese edition](mcp-client.zh-CN.md) is maintained alongside
    it.
17. [MCP OAuth client authorization](mcp-oauth.md) — challenge-driven metadata
    discovery, registration, PKCE, issuer/callback validation, bounded replay,
    durable store requirements, operations, and all 25 scored OAuth scenarios.
    A [Simplified Chinese edition](mcp-oauth.zh-CN.md) is maintained alongside
    it.
18. [MCP Server profile](mcp-server.md) — strict stateless HTTP, immutable
    Tools/Resources/Prompts catalogs, authorization-first dispatch, bounded
    Completion and MRTR, operations, and exact Server evidence. A
    [Simplified Chinese edition](mcp-server.zh-CN.md) is maintained alongside
    it.
19. [MCP Skills server profile](mcp-skills-server.md) — Final SEP-2640
    negotiation, immutable complete manifests, exact content digests,
    authorization-first discovery and resource reads, with an explicit
    server claim boundary. A [Simplified Chinese edition](mcp-skills-server.zh-CN.md)
    is maintained alongside it.
20. [MCP Skills client and Host profile](mcp-skills-host.md) — explicit extension
    opt-in, strict static-manifest validation, host-assigned origin, approval
    before lazy verified reads, isolated memory caching, fresh nested consent,
    per-call execution permits, and exact-version guarded Tool-runtime adapters
    for execution and recovery. A
    [Simplified Chinese edition](mcp-skills-host.zh-CN.md) is maintained
    alongside it.
21. [MCP conformance status](mcp-conformance.md) — exact frozen runner identity,
    all 32 scored Client and 37 scored Server scenarios, CI reproduction,
    explicitly unscored extensions, and the stable-API/Tasks claim boundary. A
    [Simplified Chinese edition](mcp-conformance.zh-CN.md) is maintained
    alongside it.
22. [A2A 1.0 Client and durable remote-agent profile](a2a-client.md) — strict
    discovery, all HTTP+JSON/JSON-RPC/SSE operations, attempt-scoped
    authorization, exact delivery semantics, PostgreSQL-backed ambiguous-write
    recovery, operator-attested context/history or deduplicated replay,
    provider-native durable polling, and production deployment gates. A
    [Simplified Chinese edition](a2a-client.zh-CN.md) is maintained alongside it.
23. [A2A 1.0 Server profile](a2a-server.md) — bounded StateKnot-owned contracts,
    strict HTTP+JSON/JSON-RPC/SSE boundary, authorization-first dispatch,
    durable backend obligations, and production deployment gates. A
    [Simplified Chinese edition](a2a-server.zh-CN.md) is maintained alongside it.
24. [A2A 1.0 conformance status](a2a-conformance.md) — exact official TCK
    commit/archive identity, audited harness patch, 177 passing cases, explicit
    skips, CI reproduction, and the server-only claim boundary. A
    [Simplified Chinese edition](a2a-conformance.zh-CN.md) is maintained
    alongside it.
25. [Durable artifact storage and A2A task completion](artifact-storage.md) —
    direct no-resend task polling, migration 18's immutable registry, private
    conditional object publication, authorization-first resolution, complete
    integrity verification, and production operations. A
    [Simplified Chinese edition](artifact-storage.zh-CN.md) is maintained
    alongside it.

26. [Shared-state subgraphs and bounded loops](graph-composition.md) — scoped
    static composition, explicit loop exhaustion, pre-dispatch step limits,
    executable registration, PostgreSQL recovery, and upgrade obligations. A
    [Simplified Chinese edition](graph-composition.zh-CN.md) is maintained
    alongside it.
27. [Register local Rust Tools](local-tools.md) — production schema generation,
    typed adapters, exact executable registration, durable invocation and mixed
    local/MCP/A2A provider rules. A
    [Simplified Chinese edition](local-tools.zh-CN.md) is maintained alongside it.
28. [Skill composition and lifecycle](skill-composition.md) — capability
    bundles, prompt provenance, dependency resolution, state boundaries,
    enforceable budgets and immutable activation. A
    [Simplified Chinese edition](skill-composition.zh-CN.md) is maintained
    alongside it.
29. [Versioning and release policy](versioning-and-releases.md) — published
    package boundaries, prerelease/SemVer/MSRV promises, durable-data upgrade
    rules, crates.io gates, and Trusted Publishing. A
    [Simplified Chinese edition](versioning-and-releases.zh-CN.md) is maintained
    alongside it.
30. [Durable child Join process-loss qualification](child-join-process-qualification.md) —
    eight real-process committed boundaries, PostgreSQL-only reconstruction,
    retained stale-worker fencing and explicit remaining gates. A
    [Simplified Chinese edition](child-join-process-qualification.zh-CN.md) is
    maintained alongside it.
31. [Deadline cancel-and-join process-loss qualification](deadline-join-process-qualification.md) —
    nine fresh-process committed boundaries, inherited-deadline convergence,
    idempotent delivery, child-inclusive cancellation accounting and exact stale
    fencing. A [Simplified Chinese edition](deadline-join-process-qualification.zh-CN.md)
    is maintained alongside it.

Current drafts include the [core domain contract](rfcs/0001-core-domain-and-capability-model.md),
the [deterministic graph and scheduler contract](rfcs/0002-deterministic-graph-and-scheduler.md),
and the [PostgreSQL durability contract](rfcs/0003-postgresql-durability-recovery-and-migration.md).

## Normative language

RFCs marked `Accepted` define project contracts. Research documents explain
intent and trade-offs but do not override accepted RFCs or released API
documentation. Terms such as MUST, SHOULD, and MAY are interpreted as described
by RFC 2119 only when an accepted RFC explicitly says so.
