// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! The actual compound event producer against the production offline registry.
use serde_json::{Value, json};
use stateknot_core::{
    BoundedJson, Checkpoint, CheckpointState, CheckpointWrite, CompiledGraph, Digest, FencingEpoch,
    GraphFrameCall, GraphFrameEntryPlan, GraphSchemaValidationError, JournalHead, JournalSequence,
    RunFence, Timestamp,
};
use stateknot_runtime::{JsonSchemaRegistryBuilder, JsonSchemaRegistryLimits};

#[test]
fn actual_frame_entry_payload_matches_the_pinned_closed_production_schema() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-graph-frame-call-v1.json"
    ))
    .unwrap();
    let parent: CompiledGraph = serde_json::from_value(fixture["compiled"].clone()).unwrap();
    let target: CompiledGraph = serde_json::from_value(fixture["child"].clone()).unwrap();
    let call: GraphFrameCall = serde_json::from_value(fixture["call"].clone()).unwrap();
    let head = JournalHead::new(
        "frame-schema-tenant".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap(),
        JournalSequence::FIRST,
        "01912345-6789-7abc-8def-0123456789a2".parse().unwrap(),
        Timestamp::from_unix_micros(1).unwrap(),
        Digest::sha256(b"schema test journal"),
    );
    let parent = Checkpoint::commit(
        CheckpointWrite::initial(
            head.tenant_id().clone(),
            head.run_id(),
            "01912345-6789-7abc-8def-0123456789a3".parse().unwrap(),
            parent.reference(),
            CheckpointState::new(
                parent.state_schema().clone(),
                BoundedJson::try_from(json!({"value":0})).unwrap(),
            )
            .unwrap(),
            parent.entry_nodes().clone(),
        )
        .unwrap(),
        head.clone(),
    )
    .unwrap();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &serde_json::from_value(fixture["compiled"].clone()).unwrap(),
        &parent,
        &target,
        "01912345-6789-7abc-8def-0123456789a4".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789a5".parse().unwrap(),
        RunFence::new(
            head.tenant_id().clone(),
            head.run_id(),
            "01912345-6789-7abc-8def-0123456789a6".parse().unwrap(),
            FencingEpoch::FIRST,
        ),
    )
    .unwrap();
    let (reference, document) = GraphFrameEntryPlan::event_schema().unwrap();
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    builder.register(reference.clone(), document).unwrap();
    let registry = builder.build().unwrap();
    let payload = plan.payload().unwrap();
    assert_eq!(payload.schema(), &reference);
    registry
        .validate_bounded(&reference, payload.data())
        .unwrap();
    let value = payload.data().as_value();
    let mut extra = value.clone();
    extra["extra"] = json!(false);
    let mut missing = value.clone();
    missing
        .as_object_mut()
        .unwrap()
        .remove("checkpoint_intent_digest");
    let mut version = value.clone();
    version["version"] = json!(2);
    let mut digest = value.clone();
    digest["intent_digest"] = json!("arbitrary text");
    let mut namespace = value.clone();
    namespace["frame"]["namespace"] = json!("");
    let positional = json!(
        value
            .as_object()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>()
    );
    for rejected in [extra, missing, version, digest, namespace, positional] {
        assert!(matches!(
            registry.validate_bounded(&reference, &BoundedJson::try_from(rejected).unwrap()),
            Err(GraphSchemaValidationError::Rejected)
        ));
    }
    let stale = stateknot_core::SchemaReference::new(
        reference.id().clone(),
        reference.version(),
        Digest::sha256(b"stale schema"),
    );
    assert!(matches!(
        registry.validate_bounded(&stale, payload.data()),
        Err(GraphSchemaValidationError::Unavailable)
    ));
}
