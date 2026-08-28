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

## Normative language

RFCs marked `Accepted` define project contracts. Research documents explain
intent and trade-offs but do not override accepted RFCs or released API
documentation. Terms such as MUST, SHOULD, and MAY are interpreted as described
by RFC 2119 only when an accepted RFC explicitly says so.
