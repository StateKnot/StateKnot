<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Skill composition and lifecycle

[简体中文](skill-composition.zh-CN.md)

StateKnot does not introduce a second execution engine named “Skill.” A Skill
is an application-owned capability bundle composed from existing versioned
instructions, Tools, graphs, policies and configuration. This keeps durable
state, authorization, budgets and recovery on the same audited paths.

Use this three-layer model:

1. **Capability bundle** for instructions plus a set of local, MCP or A2A Tool
   descriptors. Most Skills should stop here.
2. **Shared-state subgraph** when the capability requires a deterministic,
   durable, multi-step control flow inside the current run.
3. **Durable child run** when work needs an independent lifecycle, state,
   deadline, budget, cancellation or ownership boundary.

MCP Skills is a distribution and guarded-activation profile for remotely
supplied static content. It does not replace any of these execution choices and
never turns Skill text or `allowed-tools` into authority.

## Keep the module declaration in the application

An application can model composition without adding a framework-level mutable
registry:

```rust,ignore
struct SkillModule {
    identity: SkillIdentity,                 // exact application version
    dependencies: Vec<SkillRequirement>,    // exact versions
    instructions: Vec<Instruction>,         // stable ordered slots
    tools: Vec<ToolDescriptor>,              // exact executable revisions
    graph: Option<CompiledGraph>,             // only for real control flow
    policy_profile: SkillPolicyProfile,
}
```

This is an application pattern, not a promised StateKnot public API. Compile
the selected modules into one immutable `AgentDescriptor`, executable graph
registry and provider registry at deployment startup. Persist only the ordinary
exact identities already understood by the runtime. In-flight runs therefore
remain replayable without a separate “Skill checkpoint” or mutable plugin
lookup.

## Resolve dependencies before admission

Require exact Skill versions in production. At startup:

1. select the deployment's explicit root modules;
2. resolve all exact dependencies;
3. reject missing revisions, dependency cycles and duplicate Skill identities;
4. reject conflicting instruction identities, Tool identities, model-visible
   Tool names and graph identities;
5. topologically order modules with a documented stable tie-breaker; and
6. build and qualify one immutable deployment snapshot.

Do not resolve “latest,” semver ranges or marketplace priority while recovering
a run. Upgrades produce a new Agent/deployment version. Keep the previous
snapshot installed until its runs drain.

A remote nested MCP Skill is not an implicit dependency. It receives a separate
activation, approval and acting window within the same run scope. Revoking one
activation cannot be simulated by removing a Tool from a live registry.

## Compose prompts as typed, provenance-preserving slots

The application owns prompt order. Give each instruction a unique
`InstructionIdentity`, origin, trust label, security label and immutable
content. A useful order is:

1. platform and application policy;
2. Agent role and output contract;
3. selected local Skill instructions in topological order;
4. verified remote Skill content marked untrusted; and
5. run input and retrieved evidence.

Reject duplicate identities and undeclared ordering conflicts at startup. Do
not concatenate remote `SKILL.md` bytes into a privileged system instruction.
Preserve `McpSkillFileIdentity` beside every model-visible remote file so policy
and audit code can retain origin, manifest digest and content digest.

Instructions can suggest Tool use but cannot grant it. The Agent's exact
`AgentTools`, resource policy, Tool policy, caller grant and—where applicable—
MCP Skill acting-window permit remain the authority chain.

## Choose the state boundary deliberately

| Requirement | Composition primitive | State and recovery behavior |
| --- | --- | --- |
| Prompt + independent Tools | Capability bundle | No Skill-owned runtime state; Tool attempts use the current run |
| Fixed multi-step flow sharing Agent state | `SharedStateSubgraph` | Compiles into the parent graph; each node commits its own barrier |
| Bounded repeat with explicit exhaustion | `GraphSubgraphCall::bounded_loop` | Static expansion; global step budget still applies |
| Isolated long-running work | Durable child run | Independent run, state, budget, deadline and cancellation propagation |
| Remote static Skill package | MCP Skill Host activation | Untrusted verified files plus a durable approval/acting window; execution still uses ordinary Tools/graphs |

Use a subgraph only when control flow itself must be durable and deterministic.
A Tool collection does not need a graph. Conversely, do not hide a multi-step
business transaction inside one Tool call merely to make composition appear
simpler; that loses per-step attempt evidence, cancellation points and recovery.

Shared-state subgraphs expose each committed barrier to the parent and share its
schemas and cancellation. They are not atomic child calls. Use a durable child
run when the child must outlive a parent worker, carry independent budgets,
enforce ownership, or finish asynchronously. See
[Shared-state subgraphs and bounded loops](graph-composition.md) and
[Durable child runs](durable-child-runs.md).

## Enforce budgets at existing boundaries

Do not invent an unenforced `skill_budget` field. Compile module limits into
existing enforceable controls:

- Agent `BudgetLimits` for model turns, Tool calls, bytes, cost, fan-out and
  deadline;
- `ToolExecutionLimits` and resource policy for each Tool;
- graph maximum supersteps, parallelism and bounded-loop expansion;
- child-run budgets for isolated work; and
- MCP catalog, file, acting-window and per-operation authorization limits.

When several modules contribute limits, the deployment compiler should take the
most restrictive compatible value or reject an ambiguous conflict. It must not
silently widen a limit.

## Activate by replacing immutable deployments

Local Skill enablement is deployment configuration. Build a new exact Agent
version and registry snapshot, qualify it, then route new admissions to it.
Never add or remove bindings beneath an active Worker. Feature flags may select
between already-qualified exact deployments before admission; their evaluated
result must be reflected in the admitted Agent identity.

Remote MCP Skills use the separate Host lifecycle: discovery, explicit policy
approval, verified lazy reads, durable acting window, guarded Tool registration
and immutable revocation. Catalog discovery alone never modifies an Agent.
Dynamic manifests, disk installation and automatic discovery-to-Agent
composition remain outside the implemented profile.

## Example: JiaClaw composition

Suppose JiaClaw offers file management, web research and code assistance:

- **Files** is a capability bundle of local Rust read/write Tools. Write Tools
  declare non-idempotent effects and require path/resource policy.
- **Web research** is an MCP Tool bundle with a strict allowlist, response byte
  limit and provenance-preserving evidence. It needs no subgraph if each search
  is independent.
- **Code assistance** contributes application-controlled instructions and a
  bounded review subgraph. A long test or build operation becomes a durable
  child run rather than blocking one Tool attempt.
- A remotely installed review checklist is an MCP Skill activation. Its files
  remain untrusted context; its requested Tool names are presented to policy,
  not automatically enabled.

At startup JiaClaw resolves exact module versions, rejects name collisions,
builds one canonical instruction order, freezes all providers, compiles the
review graph and publishes a new Agent version. Existing conversations retain
their old snapshot; new runs use the new one.

## Trade-offs and non-goals

This design favors deterministic recovery and reviewable authority over dynamic
in-process plugin mutation. It creates more deployment versions and requires old
artifacts to remain available while runs drain. In return, every durable record
resolves to exact executable code, schemas, prompts and policy evidence.

StateKnot does not currently provide a Skill marketplace, hot-loaded native
code, semver dependency solver, cross-Skill hidden memory, automatic prompt
conflict resolution, or automatic Tool grants. Applications may build catalogs
and deployment tooling above this contract, but must preserve the immutable
admission and default-deny execution boundaries.
