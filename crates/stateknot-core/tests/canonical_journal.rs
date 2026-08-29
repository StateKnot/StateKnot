// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for journal and execution ownership.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, from_value, json, to_value};
use stateknot_core::{
    FencingEpoch, JournalAppend, JournalAuthorityError, JournalChainVerifier, JournalEvent,
    JournalEventIntent, JournalEventKind, JournalEventSource, JournalExpectation, JournalHead,
    JournalPayload, JournalSequence, RunFence, RunLease, RunLeaseValidationError, SchemaReference,
    Timestamp,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-journal/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    fencing_epochs: WireFixtures,
    journal_sequences: WireFixtures,
    event_kinds: WireFixtures,
    schema_reference: Value,
    fence: Value,
    lease: Value,
    payloads: Vec<Value>,
    canonical_payloads: Vec<CanonicalPayload>,
    intents: Vec<Value>,
    expectations: Vec<Value>,
    appends: Vec<Value>,
    events: Vec<Value>,
    raw_invalid_payloads: Vec<String>,
    raw_invalid_events: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPayload {
    text: String,
    digest: String,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-journal-v1.json"))
        .expect("canonical journal fixture must be valid JSON")
}

fn assert_wire_value<T>(expected: &Value, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    let decoded = from_value::<T>(expected.clone())
        .unwrap_or_else(|error| panic!("{type_name} rejected {expected}: {error}"));
    assert_eq!(&to_value(decoded).unwrap(), expected);
}

fn assert_wire_fixtures<T>(fixtures: WireFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        assert_wire_value::<T>(&expected, type_name);
    }
    for invalid in fixtures.invalid {
        assert!(
            from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

#[test]
fn canonical_journal_scalars_match_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<FencingEpoch>(fixture.fencing_epochs, "FencingEpoch");
    assert_wire_fixtures::<JournalSequence>(fixture.journal_sequences, "JournalSequence");
    assert_wire_fixtures::<JournalEventKind>(fixture.event_kinds, "JournalEventKind");
}

#[test]
fn canonical_lease_snapshot_is_closed_and_preserves_exclusive_expiry() {
    let fixture = load_fixture();
    assert_wire_value::<SchemaReference>(&fixture.schema_reference, "SchemaReference");
    assert_wire_value::<RunFence>(&fixture.fence, "RunFence");
    assert_wire_value::<RunLease>(&fixture.lease, "RunLease");

    let lease = from_value::<RunLease>(fixture.lease.clone()).unwrap();
    let before_expiry = "2030-01-01T00:00:09.999999Z".parse::<Timestamp>().unwrap();
    assert_eq!(lease.validate_write(lease.fence(), before_expiry), Ok(()));
    assert!(matches!(
        lease.validate_write(lease.fence(), lease.expires_at()),
        Err(RunLeaseValidationError::Expired { .. })
    ));

    let mut zero_epoch = fixture.fence.clone();
    zero_epoch["epoch"] = json!("0");
    assert!(from_value::<RunFence>(zero_epoch).is_err());

    let mut invalid_timing = fixture.lease.clone();
    invalid_timing["expires_at"] = invalid_timing["renewed_at"].clone();
    assert!(from_value::<RunLease>(invalid_timing).is_err());

    let mut extra = fixture.lease;
    extra["owner"] = json!("untrusted-worker");
    assert!(from_value::<RunLease>(extra).is_err());
}

#[test]
fn canonical_payload_bytes_and_digests_are_frozen() {
    let fixture = load_fixture();
    assert_eq!(fixture.payloads.len(), fixture.canonical_payloads.len());
    for (value, expected) in fixture
        .payloads
        .iter()
        .zip(fixture.canonical_payloads.iter())
    {
        assert_wire_value::<JournalPayload>(value, "JournalPayload");
        let payload = from_value::<JournalPayload>(value.clone()).unwrap();
        let canonical = payload.canonical_json().unwrap();
        assert_eq!(canonical.as_str(), expected.text);
        assert_eq!(payload.digest().to_string(), expected.digest);
        assert_eq!(canonical.digest(), payload.digest());
    }

    let mut unsafe_integer = fixture.payloads[0].clone();
    unsafe_integer["data"] = json!(9_007_199_254_740_992_u64);
    assert!(from_value::<JournalPayload>(unsafe_integer).is_err());

    let mut extra = fixture.payloads[0].clone();
    extra["digest"] = json!(fixture.canonical_payloads[0].digest);
    assert!(from_value::<JournalPayload>(extra).is_err());

    for invalid in fixture.raw_invalid_payloads {
        assert!(
            serde_json::from_str::<JournalPayload>(&invalid).is_err(),
            "JournalPayload accepted raw wire {invalid}"
        );
    }
}

#[test]
fn canonical_intents_expectations_and_appends_are_tamper_evident() {
    let fixture = load_fixture();
    for expected in &fixture.intents {
        assert_wire_value::<JournalEventIntent>(expected, "JournalEventIntent");
    }
    for expected in &fixture.expectations {
        assert_wire_value::<JournalExpectation>(expected, "JournalExpectation");
    }
    for expected in &fixture.appends {
        assert_wire_value::<JournalAppend>(expected, "JournalAppend");
    }

    let lease = from_value::<RunLease>(fixture.lease).unwrap();
    let control = from_value::<JournalAppend>(fixture.appends[0].clone()).unwrap();
    assert_eq!(
        control.validate_worker_lease(&lease, lease.acquired_at()),
        Err(JournalAuthorityError::ControlPlaneSource)
    );
    let worker = from_value::<JournalAppend>(fixture.appends[1].clone()).unwrap();
    assert_eq!(
        worker.validate_worker_lease(&lease, lease.acquired_at()),
        Ok(())
    );

    let mut wrong_digest = fixture.intents[0].clone();
    wrong_digest["intent_digest"] = json!(fixture.canonical_payloads[0].digest);
    assert!(from_value::<JournalEventIntent>(wrong_digest).is_err());

    let mut crossed_fence = fixture.intents[1].clone();
    crossed_fence["source"]["fence"]["tenant_id"] = json!("other-tenant");
    assert!(from_value::<JournalEventIntent>(crossed_fence).is_err());

    let mut crossed_head = fixture.appends[1].clone();
    crossed_head["expectation"]["head"]["run_id"] = json!("01912345-6789-7abc-8def-0123456789bf");
    assert!(from_value::<JournalAppend>(crossed_head).is_err());
}

#[test]
fn canonical_event_chain_rebuilds_exactly_and_rejects_mutation() {
    let fixture = load_fixture();
    let mut verifier = JournalChainVerifier::new();
    for (index, expected) in fixture.events.iter().enumerate() {
        assert_wire_value::<JournalEvent>(expected, "JournalEvent");
        let event = from_value::<JournalEvent>(expected.clone()).unwrap();
        verifier.verify_next(&event).unwrap();

        let append = from_value::<JournalAppend>(fixture.appends[index].clone()).unwrap();
        let rebuilt = JournalEvent::commit(append, event.recorded_at()).unwrap();
        assert_eq!(rebuilt, event);
    }
    let final_event = from_value::<JournalEvent>(fixture.events[1].clone()).unwrap();
    assert_eq!(verifier.head(), Some(&final_event.head()));

    for field in ["payload_digest", "intent_digest", "digest"] {
        let mut mutated = fixture.events[0].clone();
        mutated[field] = json!(fixture.canonical_payloads[1].digest);
        assert!(
            from_value::<JournalEvent>(mutated).is_err(),
            "JournalEvent accepted mutated {field}"
        );
    }

    let mut missing_predecessor = fixture.events[1].clone();
    missing_predecessor
        .as_object_mut()
        .unwrap()
        .remove("previous_digest");
    assert!(from_value::<JournalEvent>(missing_predecessor).is_err());

    for invalid in fixture.raw_invalid_events {
        assert!(
            serde_json::from_str::<JournalEvent>(&invalid).is_err(),
            "JournalEvent accepted raw wire {invalid}"
        );
    }
}

#[test]
fn journal_schemas_publish_closed_objects_and_closed_source_variants() {
    for schema in [
        to_value(schemars::schema_for!(RunFence)).unwrap(),
        to_value(schemars::schema_for!(RunLease)).unwrap(),
        to_value(schemars::schema_for!(JournalPayload)).unwrap(),
        to_value(schemars::schema_for!(JournalEventIntent)).unwrap(),
        to_value(schemars::schema_for!(JournalHead)).unwrap(),
        to_value(schemars::schema_for!(JournalAppend)).unwrap(),
        to_value(schemars::schema_for!(JournalEvent)).unwrap(),
    ] {
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
    }

    let source_schema = to_value(schemars::schema_for!(JournalEventSource)).unwrap();
    assert_eq!(source_schema["oneOf"].as_array().unwrap().len(), 2);
    for variant in source_schema["oneOf"].as_array().unwrap() {
        assert_eq!(variant["additionalProperties"], false);
    }

    let epoch_schema = to_value(schemars::schema_for!(FencingEpoch)).unwrap();
    let sequence_schema = to_value(schemars::schema_for!(JournalSequence)).unwrap();
    assert_eq!(epoch_schema["pattern"], "^[1-9][0-9]{0,18}$");
    assert_eq!(sequence_schema["pattern"], "^[1-9][0-9]{0,18}$");
}
