// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Independent state, routing and checkpoint checksum models for root barriers.

#[path = "support/canonical_reference.rs"]
mod canonical_model;
use canonical_model::canonical_reference;

use proptest::{collection, prelude::*};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stateknot_core::{
    BoundedJson, CanonicalJson, CapabilityIdentity, CapabilityReference, Checkpoint, CheckpointId,
    CheckpointState, CheckpointWrite, CompiledGraph, Digest, FencingEpoch, GraphBarrierDisposition,
    GraphExecutionLimits, GraphNode, GraphReducer, GraphReducerError, GraphReducerInput,
    GraphReducerReference, GraphRoute, GraphRoutes, GraphSchemaValidationError,
    GraphSchemaValidator, JournalHead, JournalSequence, NodeActivation, NodeControl, NodeId,
    NodeInvocationBindings, NodeStateChange, NodeStateUpdate, PendingNodeResult,
    PendingNodeResultIntent, PrincipalIdentity, ReadyNodes, RouteId, RunFence, SchemaReference,
    Superstep, Timestamp, Version,
};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct State {
    prefix: Vec<i32>,
    updates: Vec<Entry>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Entry {
    node: String,
    value: i32,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Update {
    value: i32,
}

fn digest(domain: &[u8], value: &Value) -> Digest {
    let mut bytes = domain.to_vec();
    bytes.extend_from_slice(canonical_reference(value).as_bytes());
    Digest::sha256(bytes)
}
fn schema<T: JsonSchema>(name: &str) -> SchemaReference {
    let wire = serde_json::to_value(schemars::schema_for!(T)).unwrap();
    SchemaReference::new(
        format!("https://schemas.example.com/property/{name}/1.0.0")
            .parse()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(canonical_reference(&wire)),
    )
}
fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/property".parse().unwrap(),
            "graph-property".parse().unwrap(),
        ),
        CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
    )
}
fn nodes(values: &[&str]) -> ReadyNodes {
    ReadyNodes::try_new(values.iter().map(|value| NodeId::new(*value).unwrap())).unwrap()
}
fn graph(order: [u32; 6]) -> CompiledGraph {
    let mut definitions: Vec<_> = ["alpha", "beta", "delta", "gamma"]
        .into_iter()
        .map(|name| {
            GraphNode::new(
                NodeId::new(name).unwrap(),
                None,
                GraphRoutes::try_new([
                    GraphRoute::new(
                        RouteId::new(format!("{name}.left")).unwrap(),
                        nodes(&["finish.a"]),
                    )
                    .unwrap(),
                    GraphRoute::new(
                        RouteId::new(format!("{name}.right")).unwrap(),
                        nodes(&["finish.b"]),
                    )
                    .unwrap(),
                ])
                .unwrap(),
                None,
                false,
            )
            .unwrap()
        })
        .collect();
    for name in ["finish.a", "finish.b"] {
        definitions.push(
            GraphNode::new(
                NodeId::new(name).unwrap(),
                None,
                GraphRoutes::empty(),
                None,
                true,
            )
            .unwrap(),
        );
    }
    let mut indices = [0, 1, 2, 3, 4, 5];
    indices.sort_by_key(|index| (order[*index], *index));
    let definitions: Vec<_> = indices
        .into_iter()
        .map(|index| definitions[index].clone())
        .collect();
    CompiledGraph::compile(
        identity("properties.graph"),
        schema::<State>("input"),
        schema::<State>("state"),
        schema::<Update>("update"),
        schema::<State>("output"),
        GraphReducerReference::new(
            identity("properties.reducer"),
            Digest::sha256(b"ordered-append-v1"),
        ),
        nodes(&["alpha", "beta", "delta", "gamma"]),
        definitions,
        GraphExecutionLimits::new(Superstep::new(32).unwrap(), 4).unwrap(),
    )
    .unwrap()
}
fn checkpoint_id(value: u64) -> CheckpointId {
    format!("01912345-6789-7abc-8def-{value:012x}")
        .parse()
        .unwrap()
}
fn journal(sequence: u64) -> JournalHead {
    JournalHead::new(
        "tenant-property".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap(),
        JournalSequence::new(sequence).unwrap(),
        format!("01912345-6789-7abc-8def-{sequence:012x}")
            .parse()
            .unwrap(),
        Timestamp::from_unix_micros(i64::try_from(sequence).unwrap() * 1_000_000).unwrap(),
        Digest::sha256(sequence.to_be_bytes()),
    )
}
fn bounded(value: impl Serialize) -> BoundedJson {
    BoundedJson::try_from_value(serde_json::to_value(value).unwrap()).unwrap()
}
fn initial(graph: &CompiledGraph, prefix: Vec<i32>) -> Checkpoint {
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        bounded(State {
            prefix,
            updates: vec![],
        }),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        journal(1).tenant_id().clone(),
        journal(1).run_id(),
        checkpoint_id(101),
        graph.reference(),
        state,
        graph.entry_nodes().clone(),
    )
    .unwrap();
    Checkpoint::commit(write, journal(1)).unwrap()
}
struct Schemas {
    state: SchemaReference,
    update: SchemaReference,
}
impl GraphSchemaValidator for Schemas {
    fn validate(
        &self,
        reference: &SchemaReference,
        value: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        let valid = if reference == &self.state {
            serde_json::from_value::<State>(value.as_value().clone()).is_ok()
        } else if reference == &self.update {
            serde_json::from_value::<Update>(value.as_value().clone()).is_ok()
        } else {
            return Err(GraphSchemaValidationError::Unavailable);
        };
        if valid {
            Ok(())
        } else {
            Err(GraphSchemaValidationError::Rejected)
        }
    }
}
struct Reducer(GraphReducerReference);
impl GraphReducer for Reducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.0
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        let mut state: State = serde_json::from_value(state.as_value().clone())
            .map_err(|_| GraphReducerError::Rejected)?;
        for input in updates {
            let update: Update = serde_json::from_value(input.update().data().as_value().clone())
                .map_err(|_| GraphReducerError::Rejected)?;
            state.updates.push(Entry {
                node: input.node_id().as_str().into(),
                value: update.value,
            });
        }
        BoundedJson::try_from_value(
            serde_json::to_value(state).map_err(|_| GraphReducerError::Rejected)?,
        )
        .map_err(|_| GraphReducerError::ResourceLimit)
    }
}
fn result(
    graph: &CompiledGraph,
    base: &Checkpoint,
    index: usize,
    value: i32,
    changed: bool,
    route: &str,
) -> PendingNodeResult {
    let name = ["alpha", "beta", "delta", "gamma"][index];
    let activation = NodeActivation::for_ready_root(base, NodeId::new(name).unwrap()).unwrap();
    let state = if changed {
        NodeStateChange::Update {
            update: NodeStateUpdate::new(graph.update_schema().clone(), bounded(Update { value }))
                .unwrap(),
        }
    } else {
        NodeStateChange::Unchanged
    };
    let intent = PendingNodeResultIntent::new(
        activation,
        state,
        NodeControl::Route {
            route_id: RouteId::new(format!("{name}.{route}")).unwrap(),
        },
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    PendingNodeResult::commit(
        intent,
        RunFence::new(
            base.tenant_id().clone(),
            base.run_id(),
            "01912345-6789-7abc-8def-0123456789a2".parse().unwrap(),
            FencingEpoch::new(1).unwrap(),
        ),
        journal(u64::try_from(index).unwrap() + 2),
    )
    .unwrap()
}
fn verify_checkpoint(checkpoint: &Checkpoint) {
    let wire = serde_json::to_value(checkpoint).unwrap();
    let state = &wire["state"];
    let expected_state = digest(
        b"stateknot-checkpoint-state-v1\0",
        &json!({
            "schema": state["schema"],
            "data_digest": Digest::sha256(canonical_reference(&state["data"])),
        }),
    );
    assert_eq!(checkpoint.state().digest(), expected_state);
    let intent = digest(
        b"stateknot-checkpoint-intent-v1\0",
        &json!({
            "tenant_id": wire["tenant_id"],
            "run_id": wire["run_id"],
            "checkpoint_id": wire["checkpoint_id"],
            "superstep": wire["superstep"],
            "graph": wire["graph"],
            "state_digest": expected_state,
            "ready_nodes": wire["ready_nodes"],
            "parent": wire.get("parent").unwrap_or(&Value::Null),
        }),
    );
    assert_eq!(checkpoint.intent_digest(), intent);
    assert_eq!(
        checkpoint.digest(),
        digest(
            b"stateknot-checkpoint-v1\0",
            &json!({"intent_digest": intent, "journal_head": wire["journal_head"]})
        )
    );
    let expected = canonical_reference(&wire);
    let actual = CanonicalJson::new(&bounded(checkpoint)).unwrap();
    assert_eq!(actual.as_str(), expected);
    let restored: Checkpoint = serde_json::from_slice(expected.as_bytes()).unwrap();
    assert_eq!(&restored, checkpoint);
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn graph_insertion_and_result_order_match_state_route_and_checkpoint_models(
        prefix in collection::vec(any::<i32>(), 0..9),
        values in any::<[i32; 4]>(),
        changed in any::<[bool; 4]>(),
        right in any::<[bool; 4]>(),
        order in any::<[u32; 4]>(),
        graph_order in any::<[u32; 6]>(),
    ) {
        let canonical_graph = graph([0; 6]);
        let graph = graph(graph_order);
        prop_assert_eq!(&graph, &canonical_graph);
        prop_assert_eq!(CanonicalJson::new(&bounded(&graph)).unwrap(), CanonicalJson::new(&bounded(&canonical_graph)).unwrap());
        let base = initial(&graph, prefix.clone());
        let results: Vec<_> = (0..4).map(|index| {
            result(&graph, &base, index, values[index], changed[index],
                if right[index] { "right" } else { "left" })
        }).collect();
        let schemas = Schemas {
            state: graph.state_schema().clone(),
            update: graph.update_schema().clone(),
        };
        let reducer = Reducer(graph.reducer().clone());
        let forward = graph.plan_barrier(&base, &results, checkpoint_id(102), &schemas, &reducer).unwrap();
        let mut indices = [0, 1, 2, 3];
        indices.sort_by_key(|index| (order[*index], *index));
        // Reorder the same committed facts; journal anchors do not change.
        let permuted: Vec<_> = indices.into_iter().map(|index| results[index].clone()).collect();
        let reordered = graph.plan_barrier(&base, &permuted, checkpoint_id(102), &schemas, &reducer).unwrap();
        prop_assert_eq!(&forward, &reordered);
        let names = ["alpha", "beta", "delta", "gamma"];
        let expected = State {
            prefix,
            updates: (0..4).filter(|index| changed[*index]).map(|index| Entry {
                node: names[index].into(), value: values[index],
            }).collect(),
        };
        prop_assert_eq!(forward.barrier().successor().state().data().as_value(), &serde_json::to_value(expected).unwrap());
        let routes: BTreeSet<_> = right.into_iter().map(|right| {
            if right { "finish.b" } else { "finish.a" }
        }).collect();
        prop_assert_eq!(forward.barrier().successor().ready_nodes(), &nodes(&routes.into_iter().collect::<Vec<_>>()));
        prop_assert_eq!(forward.disposition(), &GraphBarrierDisposition::Continue);
        verify_checkpoint(&base);
        let committed = Checkpoint::commit(forward.barrier().successor().clone(), journal(6)).unwrap();
        let reordered = Checkpoint::commit(reordered.barrier().successor().clone(), journal(6)).unwrap();
        prop_assert_eq!(CanonicalJson::new(&bounded(&committed)).unwrap(), CanonicalJson::new(&bounded(&reordered)).unwrap());
        prop_assert_eq!(committed.digest(), reordered.digest());
        verify_checkpoint(&committed);
        verify_checkpoint(&reordered);
    }
}

fn checkpoint_graph() -> CompiledGraph {
    let ids: Vec<String> = (0..13).map(|index| format!("step{index:02}")).collect();
    let definitions = ids.iter().enumerate().map(|(index, name)| {
        let successor = ids.get(index + 1).map(|next| nodes(&[next]));
        GraphNode::new(
            NodeId::new(name.clone()).unwrap(),
            successor,
            GraphRoutes::empty(),
            None,
            index == 12,
        )
        .unwrap()
    });
    CompiledGraph::compile(
        identity("properties.checkpoints"),
        schema::<std::collections::BTreeMap<String, i32>>("map-input"),
        schema::<std::collections::BTreeMap<String, i32>>("map-state"),
        schema::<std::collections::BTreeMap<String, i32>>("map-update"),
        schema::<std::collections::BTreeMap<String, i32>>("map-output"),
        GraphReducerReference::new(
            identity("properties.map-reducer"),
            Digest::sha256(b"replace-map-v1"),
        ),
        nodes(&[&ids[0]]),
        definitions,
        GraphExecutionLimits::new(Superstep::new(32).unwrap(), 1).unwrap(),
    )
    .unwrap()
}
fn map_states() -> impl Strategy<Value = Vec<std::collections::BTreeMap<String, i32>>> {
    let text = collection::vec(any::<char>(), 0..17)
        .prop_map(|chars| chars.into_iter().collect::<String>());
    collection::vec(collection::btree_map(text, any::<i32>(), 0..9), 1..14)
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn checkpoint_lineage_with_unicode_state_matches_each_canonical_preimage(states in map_states()) {
        let graph = checkpoint_graph();
        let mut previous: Option<Checkpoint> = None;
        for (index, data) in states.into_iter().enumerate() {
            let state = CheckpointState::new(graph.state_schema().clone(), bounded(&data)).unwrap();
            let ready = nodes(&[&format!("step{index:02}")]);
            let id = checkpoint_id(200 + u64::try_from(index).unwrap());
            let write = if let Some(parent) = &previous {
                CheckpointWrite::successor(id, parent, state, ready).unwrap()
            } else {
                CheckpointWrite::initial(journal(1).tenant_id().clone(), journal(1).run_id(),
                    id, graph.reference(), state, ready).unwrap()
            };
            let checkpoint = Checkpoint::commit(write, journal(u64::try_from(index).unwrap() + 1)).unwrap();
            prop_assert_eq!(checkpoint.superstep().get(), u64::try_from(index).unwrap());
            prop_assert_eq!(checkpoint.parent().cloned(), previous.as_ref().map(Checkpoint::head));
            prop_assert_eq!(checkpoint.state().data().as_value(), &serde_json::to_value(&data).unwrap());
            verify_checkpoint(&checkpoint);
            let mut altered = serde_json::to_value(&checkpoint).unwrap();
            altered["state"]["data"] = json!({"tampered": true});
            prop_assert!(serde_json::from_value::<Checkpoint>(altered).is_err());
            previous = Some(checkpoint);
        }
    }
}
