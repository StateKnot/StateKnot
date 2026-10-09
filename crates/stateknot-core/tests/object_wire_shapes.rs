// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Object shapes remain strict through nested validated domain records.

use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use stateknot_core::{
    AgentRequest, BoundedJson, Checkpoint, ChildRunAdmissionIntent, ChildRunJoinRequest,
    ContentMetadata, ContentPart, JournalEventIntent, ModelEventKind, Money, PrincipalIdentity,
    ResolvedBudget, SchemaReference, ScopeSet, ToolAuthorizationReceipt, ToolDescriptor, ToolInput,
};

fn positive_wire(name: &str) -> Value {
    let inventory: Value =
        serde_json::from_str(include_str!("fixtures/core-public-type-inventory-v1.json")).unwrap();
    let vector = &inventory["types"][name]["vectors"][0];
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(vector["fixture"].as_str().unwrap());
    let document: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    document
        .pointer(vector["pointer"].as_str().unwrap())
        .unwrap()
        .clone()
}

fn nested_object_requires_an_object<T: DeserializeOwned, I: DeserializeOwned>(
    name: &str,
    pointer: &str,
    fields: &[&str],
) {
    let mut wire = positive_wire(name);
    assert!(serde_json::from_value::<T>(wire.clone()).is_ok());
    let reference = wire.pointer(pointer).unwrap();
    assert!(serde_json::from_value::<I>(reference.clone()).is_ok());
    let sequence = Value::Array(
        fields
            .iter()
            .map(|field| reference.get(field).unwrap().clone())
            .collect(),
    );
    *wire.pointer_mut(pointer).unwrap() = sequence;
    assert!(serde_json::from_value::<T>(wire.clone()).is_err());
    assert!(serde_json::from_slice::<T>(&serde_json::to_vec(&wire).unwrap()).is_err());
}

fn nested_schema_requires_an_object<T: DeserializeOwned>(name: &str, pointer: &str) {
    nested_object_requires_an_object::<T, SchemaReference>(
        name,
        pointer,
        &["id", "version", "digest"],
    );
}

#[test]
fn authorization_receipt_keeps_tool_and_policy_owner_objects() {
    for pointer in ["/tool/owner", "/policy/owner"] {
        nested_object_requires_an_object::<ToolAuthorizationReceipt, PrincipalIdentity>(
            "ToolAuthorizationReceipt",
            pointer,
            &["issuer", "subject"],
        );
    }
}

#[test]
fn resolved_budget_keeps_money_objects_inside_its_typed_collection() {
    for pointer in ["/costs/0", "/costs/1"] {
        nested_object_requires_an_object::<ResolvedBudget, Money>(
            "ResolvedBudget",
            pointer,
            &["currency", "micro_units"],
        );
    }
}

#[test]
fn content_part_keeps_the_metadata_object() {
    nested_object_requires_an_object::<ContentPart, ContentMetadata>(
        "ContentPart",
        "/content/metadata",
        &["source", "trust", "security_label", "redaction"],
    );
}

#[test]
fn agent_request_rejects_a_positional_input_schema() {
    nested_schema_requires_an_object::<AgentRequest>("AgentRequest", "/input_schema");
}

#[test]
fn tool_input_rejects_a_positional_schema() {
    nested_schema_requires_an_object::<ToolInput>("ToolInput", "/schema");
}

#[test]
fn tool_descriptor_keeps_both_nested_schema_objects() {
    for pointer in ["/input_schema", "/output_schema"] {
        nested_schema_requires_an_object::<ToolDescriptor>("ToolDescriptor", pointer);
    }
}

#[test]
fn journal_intent_rejects_a_positional_payload_schema() {
    nested_schema_requires_an_object::<JournalEventIntent>("JournalEventIntent", "/payload/schema");
}

#[test]
fn checkpoint_keeps_graph_and_state_schema_objects() {
    for pointer in ["/graph/state_schema", "/state/schema"] {
        nested_schema_requires_an_object::<Checkpoint>("Checkpoint", pointer);
    }
}

#[test]
fn child_admission_keeps_nested_authority_graph_request_and_state_schemas() {
    for pointer in [
        "/child/authority/evidence/schema",
        "/child/descriptor/input_schema",
        "/child/descriptor/output_schema",
        "/child/graph/state_schema",
        "/child/request/input_schema",
        "/initial_state/schema",
        "/key/parent/base_checkpoint/graph/state_schema",
    ] {
        nested_schema_requires_an_object::<ChildRunAdmissionIntent>(
            "ChildRunAdmissionIntent",
            pointer,
        );
    }
}

#[test]
fn child_join_keeps_the_parent_checkpoint_schema_object() {
    nested_schema_requires_an_object::<ChildRunJoinRequest>(
        "ChildRunJoinRequest",
        "/keys/0/parent/base_checkpoint/graph/state_schema",
    );
}

#[test]
fn opaque_json_and_typed_collection_keep_their_declared_array_shapes() {
    let array = json!([{ "type": "started", "content": [] }, "plain", 7]);
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<BoundedJson>(array.clone()).unwrap())
            .unwrap(),
        array
    );
    let scopes = positive_wire("ScopeSet");
    assert!(scopes.is_array());
    let parsed: ScopeSet = serde_json::from_value(scopes.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), scopes);
}

#[test]
fn named_model_event_payload_requires_its_declared_object_shape() {
    let mut wire = positive_wire("ModelEventKind");
    assert_eq!(wire["type"], "started");
    let provenance = wire["content"]["provenance"].clone();
    assert!(serde_json::from_value::<ModelEventKind>(wire.clone()).is_ok());
    wire["content"] = json!([provenance]);
    assert!(serde_json::from_value::<ModelEventKind>(wire.clone()).is_err());
    assert!(serde_json::from_slice::<ModelEventKind>(&serde_json::to_vec(&wire).unwrap()).is_err());
}
