// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version fixture for canonical compiled graph definitions.

use schemars::schema_for;
use serde::Deserialize;
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    BoundedJson, CanonicalJson, CapabilityIdentity, CapabilityName, CapabilityReference,
    CompiledGraph, Digest, GraphExecutionLimits, GraphNode, GraphReducerReference, GraphRoute,
    GraphRoutes, IssuerId, JsonLimits, NodeId, PrincipalIdentity, ReadyNodes, RouteId, SchemaId,
    SchemaReference, SubjectId, Superstep, Version,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-graph/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    expected: ExpectedDigests,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    definition: Digest,
    canonical_wire: Digest,
}

fn fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-graph-v1.json")).unwrap()
}

fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/stateknot"
                .parse::<IssuerId>()
                .unwrap(),
            "canonical-graph-registry".parse::<SubjectId>().unwrap(),
        ),
        CapabilityReference::new(
            name.parse::<CapabilityName>().unwrap(),
            Version::new(1, 2, 3),
        ),
    )
}

fn schema(name: &str) -> SchemaReference {
    SchemaReference::new(
        format!("https://schemas.example.com/graph/{name}")
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(2, 1, 0),
        Digest::sha256(format!("schema:{name}")),
    )
}

fn ready(names: &[&str]) -> ReadyNodes {
    ReadyNodes::try_new(names.iter().map(|name| NodeId::new(*name).unwrap())).unwrap()
}

fn node(
    name: &str,
    continue_to: Option<&[&str]>,
    routes: Vec<GraphRoute>,
    terminal: bool,
) -> GraphNode {
    GraphNode::new(
        NodeId::new(name).unwrap(),
        continue_to.map(ready),
        GraphRoutes::try_new(routes).unwrap(),
        None,
        terminal,
    )
    .unwrap()
}

fn graph() -> CompiledGraph {
    CompiledGraph::compile(
        identity("orders.workflow"),
        schema("input"),
        schema("state"),
        schema("update"),
        schema("output"),
        GraphReducerReference::new(
            identity("orders.reducer"),
            Digest::sha256(b"canonical-orders-reducer-v1"),
        ),
        ready(&["authorize", "reserve"]),
        [
            node("complete", None, Vec::new(), true),
            node("authorize", Some(&["complete"]), Vec::new(), false),
            node(
                "reserve",
                None,
                vec![
                    GraphRoute::new(
                        RouteId::new("reserve.failed").unwrap(),
                        ready(&["complete"]),
                    )
                    .unwrap(),
                    GraphRoute::new(RouteId::new("reserve.ok").unwrap(), ready(&["complete"]))
                        .unwrap(),
                ],
                false,
            ),
        ],
        GraphExecutionLimits::new(Superstep::new(128).unwrap(), 8).unwrap(),
    )
    .unwrap()
}

#[test]
fn canonical_graph_fixture_freezes_definition_and_wire_digests() {
    let fixture = fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    let graph = graph();
    let wire = to_value(&graph).unwrap();
    let bounded =
        BoundedJson::try_from_value_with_limits(wire.clone(), JsonLimits::MAXIMUM).unwrap();
    let canonical = CanonicalJson::new(&bounded).unwrap();

    assert_eq!(graph.definition_digest(), fixture.expected.definition);
    assert_eq!(canonical.digest(), fixture.expected.canonical_wire);
    assert_eq!(from_value::<CompiledGraph>(wire).unwrap(), graph);
}

#[test]
fn canonical_graph_fixture_fails_closed_after_tampering() {
    let mut changed = to_value(graph()).unwrap();
    changed["entry_nodes"] = json!(["complete"]);
    assert!(from_value::<CompiledGraph>(changed).is_err());

    let mut changed = to_value(graph()).unwrap();
    changed["nodes"][0]["continue_to"] = json!(["missing"]);
    assert!(from_value::<CompiledGraph>(changed).is_err());

    let mut changed = to_value(graph()).unwrap();
    changed["definition_digest"] = json!(Digest::sha256(b"substituted"));
    assert!(from_value::<CompiledGraph>(changed).is_err());

    let mut changed = to_value(graph()).unwrap();
    changed["unknown"] = json!(true);
    assert!(from_value::<CompiledGraph>(changed).is_err());
}

#[test]
fn canonical_graph_schema_is_closed() {
    let schema = to_value(schema_for!(CompiledGraph)).unwrap();
    let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
    assert_eq!(
        schema.get("additionalProperties"),
        Some(&Value::Bool(false))
    );
}
