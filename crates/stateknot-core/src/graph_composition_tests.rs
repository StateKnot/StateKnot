// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    CapabilityIdentity, CapabilityName, CapabilityReference, GraphExecutionLimits,
    GraphReducerReference, PrincipalIdentity, RouteId, SchemaReference, Superstep, Version,
};

fn id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}
fn ids(values: &[&str]) -> ReadyNodes {
    ReadyNodes::try_new(values.iter().map(|v| id(v))).unwrap()
}
fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://example.com".parse().unwrap(),
            "composition".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}
fn schema(name: &str) -> SchemaReference {
    SchemaReference::new(
        format!("https://example.com/schemas/{name}")
            .parse()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(name),
    )
}
fn route(name: &str, target: &[&str]) -> GraphRoute {
    GraphRoute::new(RouteId::new(name).unwrap(), ids(target)).unwrap()
}
fn node(
    name: &str,
    targets: Option<&[&str]>,
    routes: Vec<GraphRoute>,
    terminal: bool,
) -> GraphNode {
    GraphNode::new(
        id(name),
        targets.map(ids),
        GraphRoutes::try_new(routes).unwrap(),
        None,
        terminal,
    )
    .unwrap()
}
fn compile(name: &str, entry: &[&str], nodes: Vec<GraphNode>) -> CompiledGraph {
    CompiledGraph::compile(
        identity(name),
        schema("input"),
        schema("state"),
        schema("update"),
        schema("output"),
        GraphReducerReference::new(identity("reducer"), Digest::sha256(b"reducer")),
        ids(entry),
        nodes,
        GraphExecutionLimits::new(Superstep::new(64).unwrap(), 4).unwrap(),
    )
    .unwrap()
}
fn terminal(name: &str) -> GraphNode {
    node(name, None, vec![], true)
}
fn body() -> SharedStateSubgraph {
    SharedStateSubgraph::new(compile(
        "body",
        &["work"],
        vec![
            node("work", Some(&["decide"]), vec![], false),
            node(
                "decide",
                None,
                vec![route("retry", &["again"]), route("accept", &["done"])],
                false,
            ),
            terminal("again"),
            terminal("done"),
        ],
    ))
    .unwrap()
}
fn parent() -> CompiledGraph {
    compile(
        "parent",
        &["review"],
        vec![
            node(
                "review",
                None,
                vec![route("again", &["exhausted"]), route("done", &["finish"])],
                false,
            ),
            terminal("exhausted"),
            terminal("finish"),
        ],
    )
}
fn bounded(iterations: u16) -> GraphComposition {
    GraphComposition::compile(
        parent(),
        [GraphSubgraphCall::bounded_loop(id("review"), body(), id("again"), iterations).unwrap()],
    )
    .unwrap()
}

#[test]
fn expansion_cannot_discard_parent_child_declarations() {
    let template = body();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-agent-v1.json")).unwrap();
    let mut descriptor = fixture["descriptors"]["valid"][0].clone();
    descriptor["input_schema"] = serde_json::to_value(template.graph().input_schema()).unwrap();
    descriptor["output_schema"] = serde_json::to_value(template.graph().output_schema()).unwrap();
    let descriptor: crate::AgentDescriptor = serde_json::from_value(descriptor).unwrap();
    let declaration = crate::ChildRunDeclaration::new(
        id("review"),
        crate::ChildRunSlot::new("analysis").unwrap(),
        &descriptor,
        template.graph(),
    )
    .unwrap();
    let declared = parent()
        .with_child_runs(
            crate::GraphChildRunPolicy::new(
                crate::ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
                [declaration],
            )
            .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        GraphComposition::compile(declared, [GraphSubgraphCall::once(id("review"), template)]),
        Err(GraphCompositionError::ChildDelegationUnsupported)
    ));
}
fn generated(composition: &GraphComposition, slot: &str, iteration: u16, name: &str) -> NodeId {
    composition
        .node_sources()
        .find(|(_, source)| {
            source.call_site() == &id(slot)
                && source.iteration() == iteration
                && source.node_id() == &id(name)
        })
        .unwrap()
        .0
        .clone()
}

fn named_call(slot: &str) -> GraphSubgraphCall {
    GraphSubgraphCall::once(id(slot), body())
        .with_return_routes([
            (id("again"), RouteId::new(format!("{slot}.again")).unwrap()),
            (id("done"), RouteId::new(format!("{slot}.done")).unwrap()),
        ])
        .unwrap()
}

#[test]
fn bounded_loop_has_distinct_durable_iterations_and_explicit_exhaustion() {
    let composition = bounded(3);
    assert_eq!(composition.graph.nodes().len(), 8);
    assert_eq!(
        composition.graph.entry_nodes(),
        &ready([generated(&composition, "review", 0, "work")]).unwrap()
    );
    for iteration in 0..3 {
        let work = generated(&composition, "review", iteration, "work");
        let decide = generated(&composition, "review", iteration, "decide");
        assert_eq!(
            composition
                .graph
                .node(&work)
                .unwrap()
                .continue_to()
                .unwrap(),
            &ready([decide.clone()]).unwrap()
        );
        let routes = composition.graph.node(&decide).unwrap().routes();
        assert_eq!(
            routes
                .get(
                    composition
                        .node_source(&decide)
                        .unwrap()
                        .route_id(&RouteId::new("accept").unwrap())
                        .unwrap()
                )
                .unwrap()
                .successors(),
            &ids(&["finish"])
        );
        let expected = if iteration == 2 {
            id("exhausted")
        } else {
            generated(&composition, "review", iteration + 1, "work")
        };
        assert_eq!(
            routes
                .get(
                    composition
                        .node_source(&decide)
                        .unwrap()
                        .route_id(&RouteId::new("retry").unwrap())
                        .unwrap()
                )
                .unwrap()
                .successors(),
            &ready([expected]).unwrap()
        );
    }
    assert!(composition.graph.node(&id("review")).is_none());
    assert!(composition.graph.node(&id("done")).is_none());
    assert_eq!(composition.node_sources().len(), 6);
}

#[test]
fn composition_is_deterministic_roundtrippable_and_revision_pinned() {
    let original = bounded(3);
    assert_eq!(original.graph, bounded(3).graph);
    let bytes = serde_json::to_vec(&original.graph).unwrap();
    assert_eq!(
        original.graph,
        serde_json::from_slice::<CompiledGraph>(&bytes).unwrap()
    );
    assert_ne!(original.graph.reference(), bounded(2).graph.reference());
    let child = body();
    let mut changed = serde_json::to_value(child.graph()).unwrap();
    // Recompilation, not tampering with a supplied checksum, changes a revision.
    changed["definition_digest"] = serde_json::json!(Digest::sha256(b"wrong"));
    assert!(serde_json::from_value::<CompiledGraph>(changed).is_err());
    let revised = CompiledGraph::compile(
        identity("body-v2"),
        schema("input"),
        schema("state"),
        schema("update"),
        schema("output"),
        child.graph().reducer().clone(),
        child.graph().entry_nodes().clone(),
        child.graph().nodes().to_vec(),
        child.graph().limits(),
    )
    .unwrap();
    let revised = GraphComposition::compile(
        parent(),
        [GraphSubgraphCall::bounded_loop(
            id("review"),
            SharedStateSubgraph::new(revised).unwrap(),
            id("again"),
            3,
        )
        .unwrap()],
    )
    .unwrap();
    assert_ne!(original.graph.reference(), revised.graph.reference());
    assert_ne!(original.graph.entry_nodes(), revised.graph.entry_nodes());
}

#[test]
fn empty_composition_preserves_existing_wire_bytes() {
    let parent = parent();
    let original = parent.canonical_definition_bytes().unwrap();
    let composition = GraphComposition::compile(parent, []).unwrap();
    assert_eq!(
        original,
        composition.graph.canonical_definition_bytes().unwrap()
    );
    assert_eq!(composition.node_sources().len(), 0);
}

#[test]
fn separate_calls_are_isolated_and_input_order_does_not_change_bytes() {
    let parent = compile(
        "two-calls",
        &["left", "right"],
        vec![
            node(
                "left",
                None,
                vec![route("left.again", &["end"]), route("left.done", &["end"])],
                false,
            ),
            node(
                "right",
                None,
                vec![
                    route("right.again", &["end"]),
                    route("right.done", &["end"]),
                ],
                false,
            ),
            terminal("end"),
        ],
    );
    let left = named_call("left");
    let right = named_call("right");
    let a = GraphComposition::compile(parent.clone(), [left.clone(), right.clone()]).unwrap();
    let b = GraphComposition::compile(parent, [right, left]).unwrap();
    assert_eq!(a.graph, b.graph);
    assert_ne!(
        generated(&a, "left", 0, "work"),
        generated(&a, "right", 0, "work")
    );
    assert_eq!(a.graph.entry_nodes().len(), 2);
}

#[test]
fn static_nesting_preserves_body_controls_and_has_no_runtime_call_nodes() {
    let inner = bounded(2).into_graph();
    let outer = compile(
        "outer",
        &["nested"],
        vec![
            node(
                "nested",
                None,
                vec![route("exhausted", &["out"]), route("finish", &["out"])],
                false,
            ),
            terminal("out"),
        ],
    );
    let nested = GraphComposition::compile(
        outer,
        [GraphSubgraphCall::once(
            id("nested"),
            SharedStateSubgraph::new(inner).unwrap(),
        )],
    )
    .unwrap();
    assert_eq!(nested.graph.nodes().len(), 5);
    assert_eq!(nested.node_sources().len(), 4);
    assert!(
        nested
            .graph
            .nodes()
            .iter()
            .all(|node| node.node_id() == &id("out") || !node.allows_terminal())
    );
}

#[test]
fn return_forwarding_to_another_call_is_resolved_without_empty_executors() {
    let parent = compile(
        "chain",
        &["left"],
        vec![
            node(
                "left",
                None,
                vec![
                    route("left.again", &["right"]),
                    route("left.done", &["right"]),
                ],
                false,
            ),
            node(
                "right",
                None,
                vec![
                    route("right.again", &["end"]),
                    route("right.done", &["end"]),
                ],
                false,
            ),
            terminal("end"),
        ],
    );
    let composition =
        GraphComposition::compile(parent, [named_call("left"), named_call("right")]).unwrap();
    let decide = generated(&composition, "left", 0, "decide");
    let expected = ready([generated(&composition, "right", 0, "work")]).unwrap();
    for route in composition.graph.node(&decide).unwrap().routes().iter() {
        assert_eq!(route.successors(), &expected);
    }
}

#[test]
fn wait_successors_are_mapped_to_parent_continuations() {
    let wait = GraphNode::new(
        id("pause"),
        None,
        GraphRoutes::empty(),
        Some(ids(&["done"])),
        false,
    )
    .unwrap();
    let child = SharedStateSubgraph::new(compile(
        "wait-body",
        &["pause"],
        vec![wait, terminal("done")],
    ))
    .unwrap();
    let parent = compile(
        "wait-parent",
        &["call"],
        vec![
            node("call", None, vec![route("done", &["finish"])], false),
            terminal("finish"),
        ],
    );
    let composition =
        GraphComposition::compile(parent, [GraphSubgraphCall::once(id("call"), child)]).unwrap();
    let pause = generated(&composition, "call", 0, "pause");
    assert_eq!(
        composition.graph.node(&pause).unwrap().wait_to().unwrap(),
        &ids(&["finish"])
    );
}

#[test]
fn ambiguous_returns_cycles_and_empty_templates_are_rejected() {
    let mixed = compile(
        "mixed",
        &["work"],
        vec![node("work", Some(&["end"]), vec![], true), terminal("end")],
    );
    assert!(matches!(
        SharedStateSubgraph::new(mixed),
        Err(GraphCompositionError::MixedReturnPort { .. })
    ));
    let cycle = compile(
        "cycle",
        &["work"],
        vec![
            node(
                "work",
                Some(&["work"]),
                vec![route("done", &["end"])],
                false,
            ),
            terminal("end"),
        ],
    );
    assert!(matches!(
        SharedStateSubgraph::new(cycle),
        Err(GraphCompositionError::CyclicTemplate)
    ));
    let empty = compile("empty", &["end"], vec![terminal("end")]);
    assert!(matches!(
        SharedStateSubgraph::new(empty),
        Err(GraphCompositionError::EntryIsReturnPort)
    ));
    let no_return = compile(
        "no-return",
        &["work"],
        vec![
            GraphNode::new(
                id("work"),
                None,
                GraphRoutes::empty(),
                Some(ids(&["work"])),
                false,
            )
            .unwrap(),
        ],
    );
    assert!(matches!(
        SharedStateSubgraph::new(no_return),
        Err(GraphCompositionError::MissingReturnPorts)
    ));
}

#[test]
fn loops_reject_zero_unknown_ports_and_expansion_above_the_hard_ceiling() {
    assert!(matches!(
        GraphSubgraphCall::bounded_loop(id("review"), body(), id("again"), 0),
        Err(GraphCompositionError::InvalidIterationLimit)
    ));
    assert!(matches!(
        GraphSubgraphCall::bounded_loop(id("review"), body(), id("again"), 1025),
        Err(GraphCompositionError::InvalidIterationLimit)
    ));
    assert!(matches!(
        GraphSubgraphCall::bounded_loop(id("review"), body(), id("typo"), 2),
        Err(GraphCompositionError::UnknownRepeatPort { .. })
    ));
    assert_eq!(bounded(511).graph.nodes().len(), CompiledGraph::MAX_NODES);
    let oversized =
        GraphSubgraphCall::bounded_loop(id("review"), body(), id("again"), 512).unwrap();
    assert!(matches!(
        GraphComposition::compile(parent(), [oversized]),
        Err(GraphCompositionError::ExpansionTooLarge)
    ));
}

#[test]
fn duplicate_absent_and_incomplete_call_sites_fail_before_admission() {
    let call = GraphSubgraphCall::once(id("review"), body());
    assert!(matches!(
        GraphComposition::compile(parent(), [call.clone(), call]),
        Err(GraphCompositionError::DuplicateCallSite { .. })
    ));
    assert!(matches!(
        GraphComposition::compile(parent(), [GraphSubgraphCall::once(id("missing"), body())]),
        Err(GraphCompositionError::UnknownCallSite { .. })
    ));
    let parent = compile(
        "bad-return",
        &["review"],
        vec![
            node("review", None, vec![route("done", &["end"])], false),
            terminal("end"),
        ],
    );
    assert!(matches!(
        GraphComposition::compile(parent, [GraphSubgraphCall::once(id("review"), body())]),
        Err(GraphCompositionError::ReturnRoutesMismatch { .. })
    ));
}

#[test]
fn every_shared_schema_and_reducer_pin_is_enforced() {
    let child = body();
    for field in 0..5 {
        let graph = child.graph();
        let changed = schema("changed");
        let graph = CompiledGraph::compile(
            graph.identity().clone(),
            if field == 0 {
                changed.clone()
            } else {
                graph.input_schema().clone()
            },
            if field == 1 {
                changed.clone()
            } else {
                graph.state_schema().clone()
            },
            if field == 2 {
                changed.clone()
            } else {
                graph.update_schema().clone()
            },
            if field == 3 {
                changed.clone()
            } else {
                graph.output_schema().clone()
            },
            if field == 4 {
                GraphReducerReference::new(identity("other"), Digest::sha256(b"other"))
            } else {
                graph.reducer().clone()
            },
            graph.entry_nodes().clone(),
            graph.nodes().to_vec(),
            graph.limits(),
        )
        .unwrap();
        assert!(matches!(
            GraphComposition::compile(
                parent(),
                [GraphSubgraphCall::once(
                    id("review"),
                    SharedStateSubgraph::new(graph).unwrap()
                )]
            ),
            Err(GraphCompositionError::SharedStateContractMismatch { .. })
        ));
    }
}

#[test]
fn template_step_and_parallelism_limits_are_not_silently_discarded() {
    let child = body();
    let graph = child.graph();
    let rebuild = |steps, parallelism| {
        CompiledGraph::compile(
            graph.identity().clone(),
            graph.input_schema().clone(),
            graph.state_schema().clone(),
            graph.update_schema().clone(),
            graph.output_schema().clone(),
            graph.reducer().clone(),
            graph.entry_nodes().clone(),
            graph.nodes().to_vec(),
            GraphExecutionLimits::new(Superstep::new(steps).unwrap(), parallelism).unwrap(),
        )
        .unwrap()
    };
    assert!(matches!(
        SharedStateSubgraph::new(rebuild(1, 4)),
        Err(GraphCompositionError::TemplateStepLimit)
    ));
    assert!(matches!(
        GraphComposition::compile(
            parent(),
            [GraphSubgraphCall::once(
                id("review"),
                SharedStateSubgraph::new(rebuild(2, 1)).unwrap()
            )]
        ),
        Err(GraphCompositionError::TemplateParallelismLimit { .. })
    ));
}

#[test]
fn parallel_body_preserves_canonical_source_order_and_deduplicates_joins() {
    let body = SharedStateSubgraph::new(compile(
        "ordered-body",
        &["alpha", "beta"],
        vec![
            node("alpha", Some(&["join"]), vec![], false),
            node("beta", Some(&["join"]), vec![], false),
            node("join", Some(&["done"]), vec![], false),
            terminal("done"),
        ],
    ))
    .unwrap();
    let parent = compile(
        "ordered-parent",
        &["call"],
        vec![
            node("call", None, vec![route("done", &["end"])], false),
            terminal("end"),
        ],
    );
    let composition =
        GraphComposition::compile(parent, [GraphSubgraphCall::once(id("call"), body)]).unwrap();
    let names: Vec<_> = composition
        .graph
        .entry_nodes()
        .iter()
        .map(|node| composition.node_source(node).unwrap().node_id().as_str())
        .collect();
    assert_eq!(names, vec!["alpha", "beta"]);
    let mut successors = BTreeSet::new();
    for node in composition.graph.entry_nodes() {
        successors.extend(
            composition
                .graph
                .node(node)
                .unwrap()
                .continue_to()
                .unwrap()
                .iter()
                .cloned(),
        );
    }
    assert_eq!(successors.len(), 1);
}

#[test]
fn return_bindings_are_complete_bijective_and_digest_bound() {
    let call = GraphSubgraphCall::once(id("review"), body());
    assert!(
        call.clone()
            .with_return_routes([(id("done"), RouteId::new("done").unwrap())])
            .is_err()
    );
    assert!(
        call.clone()
            .with_return_routes([
                (id("done"), RouteId::new("done").unwrap()),
                (id("again"), RouteId::new("done").unwrap())
            ])
            .is_err()
    );
    assert!(
        call.clone()
            .with_return_routes([(id("typo"), RouteId::new("done").unwrap())])
            .is_err()
    );
    let swapped = call
        .clone()
        .with_return_routes([
            (id("done"), RouteId::new("again").unwrap()),
            (id("again"), RouteId::new("done").unwrap()),
        ])
        .unwrap();
    assert_ne!(
        GraphComposition::compile(parent(), [call])
            .unwrap()
            .graph
            .reference(),
        GraphComposition::compile(parent(), [swapped])
            .unwrap()
            .graph
            .reference()
    );
}

#[test]
fn generated_identity_contract_is_frozen() {
    assert_eq!(
        bounded(3).graph.definition_digest().to_string(),
        "sha256:25eefd8354f87f670e2534fb8672069cd4d3bcf5b91ad605e6f2dd3d8c905e17"
    );
}
