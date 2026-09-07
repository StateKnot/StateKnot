<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable shared-state subgraphs and bounded loops

[简体中文](graph-composition.zh-CN.md)

`SharedStateSubgraph`, `GraphSubgraphCall`, and `GraphComposition` implement
static shared-state composition. They compile into an ordinary `CompiledGraph`
and execute through `DurableGraphDriver` / `DurableAgentLoop` with PostgreSQL.
This is an implemented, unpublished pre-alpha contract, not a claim that all
child-workflow or framework production-qualification gates are complete.

## Run the checked-in example

```console
cargo run -p stateknot-runtime --example durable_graph_composition --locked
```

The [complete example](../crates/stateknot-runtime/examples/durable_graph_composition.rs)
builds schemas, a reducer, a two-node review body, a three-iteration bound,
explicit success/exhaustion continuations, and a closed executable registry.
It compiles eight executable nodes and performs no database or provider I/O.
Use initial state `{"count":0}` when admitting this graph. Its decision node
accepts at count two. To execute, bind the registry and a trusted lifecycle
evidence provider to the [durable Agent Loop](durable-agent-loop.md); the
[PostgreSQL tests](../crates/stateknot-runtime/tests/postgres/graph_composition.rs)
exercise real execution and recovery, not an in-memory fallback.

## Define the return contract

A template is a finite acyclic compiled graph. Its pure terminal nodes are
**symbolic return ports**, never executable nodes. They cannot also declare
continue, route, or wait controls, and cannot appear in the entry set. All
other nodes are executable and cannot return a terminal output. Communicate
results through explicit state updates instead; no child output is discarded.

The parent contains a call-site node exposing only routes. By default each
template port has a same-named parent route. The template's `done` port might
map to the parent's `done -> finish` route; `again` might map to
`again -> exhausted`.

```rust,ignore
let subgraph = SharedStateSubgraph::new(body)?;
let call = GraphSubgraphCall::bounded_loop(
    NodeId::new("review")?,
    subgraph,
    NodeId::new("again")?,
    3,
)?;
let composition = GraphComposition::compile(parent, [call])?;
```

This fragment assumes the `body` and `parent` declarations in the complete
example; it is not a standalone program. `GraphSubgraphCall::once` instantiates
the body once. For multiple call-sites, graph route IDs must remain globally
unique. Bind ports explicitly with `with_return_routes`, for example
`done -> left.done` and `again -> left.again`. Mappings must be complete and
bijective; duplicate, missing, and extra ports/routes fail before admission.

## Loop and state semantics

- The loop is do/while-style: it executes at most the declared number of body
  iterations per entry into that call-site. Selecting `again` enters the next
  iteration. Selecting another port returns immediately.
- On the last iteration, `again` takes its explicit parent continuation. Choose
  a real failure executor or a deliberate fallback there; exhaustion never
  silently becomes success. An enclosing cycle can re-enter the call-site,
  so the global graph step budget remains mandatory.
- Every body node has its own durable start, result, and barrier. State updates
  become visible at each barrier, not atomically at the end of the subgraph.
  Parent and child share the exact input/state/update/output schema references
  and reducer revision. There is no private child state or implicit coercion.
- Parallel work retains ordinary bulk-synchronous semantics: stable ordered
  reduction and deduplicated next-ready sets, not a new asynchronous join
  accumulator. Source-node ordering is preserved within a call/iteration;
  ordering across scopes follows the expanded graph's canonical node IDs.
- A body wait suspends the containing run and retains its rewritten successor
  set. Resolving the existing durable wait resumes that set without repeating
  the completed child. Cancellation applies to the containing run.
- An already composed acyclic graph can be another template. Static nesting
  is flattened at startup and uses no recursive runtime call stack.

## Bind executable identities correctly

Register **only** `composition.graph()` as the deployable graph, not the
authoring parent or symbolic port nodes. For every generated node,
`composition.node_source(node_id)` supplies the exact template reference,
original node ID, call-site, and zero-based iteration. Construct the ordinary
`GraphNodeExecutor` using the composed graph reference and generated node ID.

Route IDs are also scoped. Resolve local decisions through
`source.route_id(&local_route_id)` and return that generated `RouteId`.
Returning the original route ID fails closed as an undeclared route.
Reducers receive composed node IDs too: use a role-independent reducer or
bind the immutable source map when a reducer needs source-role information.

Pass the actual `GraphNodeContext` to durable invocation code unchanged.
Never fabricate a child checkpoint, replace the activation with a template
identity, or reset the root superstep. Invocation bindings continue to name
the real tenant/run/checkpoint/activation, including across lease takeover.

## Resource, recovery, and upgrade guarantees

The complete expansion has at most 1,024 executable nodes, 256 routes per node,
and a two-MiB canonical descriptor. Every loop copy counts before allocation;
there is no runtime fallback if expansion is too large. Use an ordinary
globally step-bounded cyclic graph for large data-dependent iteration counts.
Template cycles are rejected; the longest body path must fit its own step
ceiling. Parent parallelism must not exceed any template ceiling, including
other concurrently ready parent nodes. This conservative rule is enforceable
without a second scheduler.

Generated node IDs use `skc-<full SHA-256 scope digest>-<four-digit ordinal>`.
The scope preimage is RFC 8785 JSON with domain
`stateknot.graph-composition.v1`, the complete parent and template references,
call-site, iteration, maximum iterations, repeat port, and return-route map.
The ordinal comes from the template's canonical node order, including ports.
Generated `skr-<full SHA-256>` route IDs bind the generated node and original
route using domain `stateknot.graph-composition.route.v1`.
The expanded graph digest therefore pins source revisions, call-sites, loop
bounds, and return bindings. Preserve this compiler version, all source
descriptors, the generated source map, and exact executable artifacts for
in-flight runs. Template or composition changes require a new graph version;
keep the old registry revision until its runs drain.

No checkpoint wire or PostgreSQL migration is needed. Empty composition keeps
the original graph bytes unchanged. Recovery replays the same ordinary
checkpoint lineage and consumes exact existing pending results. Starts left
without completion require the existing higher-fence takeover rules.

The driver now checks `maximum_supersteps` **before** starting another node,
including after restart at the limit. `GraphDriveBlockers::superstep_limit_reached`
enters lifecycle failure supervision. A trusted `failure_evidence` provider
must recover exact cumulative usage, even though there may be no failed node
at that checkpoint. Missing evidence leaves the run unfinished, not failed
with invented zero usage. The lifecycle records
`runtime.graph.superstep_limit_reached` with no retry advice; existing node
blocker counts remain node counts in the unchanged event schema. A retained
failure handoff can be retried after a lost acknowledgement without re-reading
evidence or changing the original terminal failure.

## Qualification and remaining boundaries

Core tests cover deterministic identities, a frozen digest, source-order
preservation, static nesting, return forwarding, port/route/schema/reducer
rejection, and resource ceilings. Real PostgreSQL tests cover pending-result
recovery across executable-registry recreation, early exit, explicit
exhaustion, orphan takeover, changed-composition refusal before dispatch,
wait/resume, and pre-dispatch global-limit failure including unavailable
evidence and lost acknowledgement. Both PostgreSQL 16 and 17 run in CI.

Isolated child state, independently scheduled child runs, dynamic recursion,
independent child cancellation, and asynchronous cross-step joins are outside
this API. Those require a separate namespaced checkpoint/lifecycle contract;
static composition must not be advertised as implementing them.

The scope distinction is informed by [LangGraph subgraph communication](https://docs.langchain.com/oss/python/langgraph/use-subgraphs)
and [Temporal child workflow lifecycles](https://docs.temporal.io/child-workflows).
StateKnot's lowering and persistence semantics above are its own contract.
