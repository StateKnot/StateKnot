// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for durable model invocation history.

use schemars::schema_for;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    AttemptId, Checkpoint, Digest, DurationMillis, EventId, Failure, FailureCategory, FailureCode,
    FailureId, FailureMessage, FailureOrigin, GraphNamespace, InvocationId, JournalHead,
    JournalSequence, ModelDescriptor, ModelError, ModelErrorPhase, ModelErrorProvenance,
    ModelInvocation, ModelInvocationHead, ModelInvocationHistoryVerifier, ModelInvocationIntent,
    ModelInvocationRevision, ModelInvocationStatus, ModelInvocationTransition,
    ModelInvocationTransitionKind, ModelRequest, ModelResponse, NodeActivation, NodeId,
    RetryAdvice, Timestamp,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-invocation/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    revisions: WireFixtures,
    expected: ExpectedHistory,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedHistory {
    intent_digest: Digest,
    records: Vec<ExpectedRecord>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExpectedRecord {
    revision: ModelInvocationRevision,
    status: ModelInvocationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<AttemptId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition: Option<ModelInvocationTransitionKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_digest: Option<Digest>,
    digest: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-invocation-v1.json"))
        .expect("canonical model invocation fixture must be valid JSON")
}

fn fixture_value(path: &[&str], source: &str) -> Value {
    let mut value: Value = serde_json::from_str(source).unwrap();
    for component in path {
        value = match component.parse::<usize>() {
            Ok(index) => value[index].clone(),
            Err(_) => value[*component].clone(),
        };
    }
    value
}

fn checkpoint() -> Checkpoint {
    from_value(fixture_value(
        &["checkpoints", "0"],
        include_str!("fixtures/core-checkpoint-v1.json"),
    ))
    .unwrap()
}

fn descriptor() -> ModelDescriptor {
    from_value(fixture_value(
        &["descriptors", "valid", "0", "model"],
        include_str!("fixtures/core-agent-v1.json"),
    ))
    .unwrap()
}

fn request() -> ModelRequest {
    from_value(fixture_value(
        &["requests", "valid", "0"],
        include_str!("fixtures/core-model-request-v1.json"),
    ))
    .unwrap()
}

fn invocation_id() -> InvocationId {
    "01912345-6789-7abc-8def-0123456789d0".parse().unwrap()
}

fn attempt(suffix: &str) -> AttemptId {
    format!("01912345-6789-7abc-8def-0123456789{suffix}")
        .parse()
        .unwrap()
}

fn intent() -> ModelInvocationIntent {
    let checkpoint = checkpoint();
    ModelInvocationIntent::new(
        NodeActivation::new(
            checkpoint.head(),
            GraphNamespace::root(),
            NodeId::new("reason").unwrap(),
            Digest::sha256(b"model-node-input"),
        ),
        invocation_id(),
        descriptor(),
        request(),
    )
    .unwrap()
}

fn journal(intent: &ModelInvocationIntent, sequence: u64) -> JournalHead {
    let base = intent
        .activation()
        .base_checkpoint()
        .journal_head()
        .recorded_at();
    let offset = i64::try_from(sequence - 1).unwrap() * 1_000_000;
    let recorded_at = Timestamp::from_unix_micros(base.unix_micros() + offset).unwrap();
    let event_id: EventId = format!("01912345-6789-7abc-8def-0123456789{:02x}", 0xd0 + sequence)
        .parse()
        .unwrap();
    JournalHead::new(
        intent.tenant_id().clone(),
        intent.run_id(),
        JournalSequence::new(sequence).unwrap(),
        event_id,
        recorded_at,
        Digest::sha256(sequence.to_be_bytes()),
    )
}

fn response(attempt_id: AttemptId) -> ModelResponse {
    let descriptor = descriptor();
    let request = request();
    let mut value = fixture_value(
        &["responses", "valid", "0"],
        include_str!("fixtures/core-model-response-v1.json"),
    );
    value["provenance"]["attempt_id"] = json!(attempt_id);
    value["provenance"]["model"] = to_value(descriptor.metadata().identity()).unwrap();
    let response = from_value::<ModelResponse>(value).unwrap();
    response.validate_for(&descriptor, &request).unwrap();
    response
}

fn retryable_error(attempt_id: AttemptId) -> ModelError {
    let descriptor = descriptor();
    ModelError::new(
        Failure::new(
            "01912345-6789-7abc-8def-0123456789b8"
                .parse::<FailureId>()
                .unwrap(),
            FailureCategory::DependencyUnavailable,
            FailureCode::new("model.dependency_unavailable").unwrap(),
            FailureOrigin::new("model.provider").unwrap(),
            FailureMessage::new("The model provider is temporarily unavailable.").unwrap(),
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(1_000).unwrap(),
            },
        )
        .unwrap(),
        ModelErrorPhase::Dispatch,
        ModelErrorProvenance::new(
            attempt_id,
            descriptor.metadata().identity().clone(),
            None,
            None,
            None,
        ),
        None,
    )
}

fn history() -> (ModelInvocationIntent, Vec<ModelInvocation>) {
    let intent = intent();
    let prepared = ModelInvocation::prepare(intent.clone(), journal(&intent, 2)).unwrap();
    let first_attempt = attempt("ab");
    let executing = prepared
        .advance(
            ModelInvocationTransition::StartAttempt {
                attempt_id: first_attempt,
            },
            journal(&intent, 3),
        )
        .unwrap();
    let failed = executing
        .advance(
            ModelInvocationTransition::RecordError {
                error: retryable_error(first_attempt),
            },
            journal(&intent, 4),
        )
        .unwrap();
    let second_attempt = attempt("ac");
    let retried = failed
        .advance(
            ModelInvocationTransition::StartAttempt {
                attempt_id: second_attempt,
            },
            journal(&intent, 5),
        )
        .unwrap();
    let committed = retried
        .advance(
            ModelInvocationTransition::RecordResponse {
                response: response(second_attempt),
            },
            journal(&intent, 6),
        )
        .unwrap();
    (
        intent,
        vec![prepared, executing, failed, retried, committed],
    )
}

fn assert_wire_fixtures<T>(fixtures: WireFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        let decoded = from_value::<T>(expected.clone())
            .unwrap_or_else(|error| panic!("{type_name} rejected {expected}: {error}"));
        assert_eq!(to_value(decoded).unwrap(), expected);
    }
    for invalid in fixtures.invalid {
        assert!(
            from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

#[test]
fn canonical_model_invocation_revision_matches_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<ModelInvocationRevision>(fixture.revisions, "ModelInvocationRevision");
}

#[test]
fn canonical_model_intent_and_retry_history_freeze_every_integrity_value() {
    let fixture = load_fixture();
    let (intent, records) = history();
    assert_eq!(intent.intent_digest(), fixture.expected.intent_digest);
    assert_eq!(records.len(), fixture.expected.records.len());

    let restored_intent = from_value::<ModelInvocationIntent>(to_value(&intent).unwrap()).unwrap();
    assert_eq!(restored_intent, intent);

    let actual: Vec<ExpectedRecord> = records
        .iter()
        .map(|record| ExpectedRecord {
            revision: record.revision(),
            status: record.status(),
            attempt_id: record.attempt_id(),
            transition: record.transition().map(ModelInvocationTransition::kind),
            transition_digest: record.transition_digest(),
            digest: record.digest(),
        })
        .collect();
    assert_eq!(
        to_value(&actual).unwrap(),
        to_value(&fixture.expected.records).unwrap()
    );

    let mut verifier = ModelInvocationHistoryVerifier::new();
    for record in &records {
        let wire = to_value(record).unwrap();
        let restored = from_value::<ModelInvocation>(wire.clone()).unwrap();
        assert_eq!(to_value(restored).unwrap(), wire);
        verifier.verify_next(record).unwrap();
    }
    assert_eq!(verifier.head(), Some(records.last().unwrap().head()));

    for record in &records[1..] {
        let previous = record.previous().unwrap();
        let wire = to_value(previous).unwrap();
        let restored = from_value::<ModelInvocationHead>(wire.clone()).unwrap();
        assert_eq!(to_value(restored).unwrap(), wire);
    }
}

#[test]
fn canonical_model_invocation_wires_fail_closed_after_tampering() {
    let (intent, records) = history();

    let mut changed_request = to_value(&intent).unwrap();
    changed_request["request"]["instructions"][0]["content"]["content"]["text"] =
        json!("changed prompt");
    assert!(from_value::<ModelInvocationIntent>(changed_request).is_err());

    let mut changed_state = to_value(&records[1]).unwrap();
    changed_state["state"]["attempt_id"] = json!(attempt("ad"));
    assert!(from_value::<ModelInvocation>(changed_state).is_err());

    let mut changed_transition = to_value(&records[1]).unwrap();
    changed_transition["transition"]["attempt_id"] = json!(attempt("ad"));
    assert!(from_value::<ModelInvocation>(changed_transition).is_err());

    let mut changed_previous = to_value(&records[4]).unwrap();
    changed_previous["previous"]["digest"] = json!(records[0].digest());
    assert!(from_value::<ModelInvocation>(changed_previous).is_err());

    let mut extra = to_value(&records[0]).unwrap();
    extra["unsafe_extension"] = Value::Bool(true);
    assert!(from_value::<ModelInvocation>(extra).is_err());
}

#[test]
fn model_invocation_schema_objects_remain_closed() {
    for schema in [
        to_value(schema_for!(ModelInvocationIntent)).unwrap(),
        to_value(schema_for!(ModelInvocationHead)).unwrap(),
        to_value(schema_for!(ModelInvocation)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }
}
