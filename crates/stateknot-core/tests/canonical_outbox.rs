// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixture for transactional outbox records.

use schemars::schema_for;
use serde::Deserialize;
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    AttemptId, BoundedJson, CanonicalJson, DeliveryFence, DeliveryId, DestinationId, Digest,
    DurationMillis, EventId, Failure, FailureCategory, FailureCode, FailureId, FailureMessage,
    FailureOrigin, FencingEpoch, JournalEventKind, JournalHead, JournalPayload, JournalSequence,
    JsonLimits, OutboxAttempt, OutboxAttemptCompletion, OutboxAttemptOutcome, OutboxAttemptStart,
    OutboxAttemptStartHead, OutboxDelivery, OutboxDeliveryHead, OutboxDeliveryIntent,
    OutboxDestinationRef, RetryAdvice, RunId, SchemaId, SchemaReference, TenantId, Timestamp,
    Version,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-outbox/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    expected: ExpectedDigests,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    intent: Digest,
    delivery: Digest,
    start: Digest,
    acknowledgement: Digest,
    acknowledged_wire: Digest,
    failure: Digest,
    failed_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-outbox-v1.json"))
        .expect("canonical outbox fixture must be valid JSON")
}

fn id<T: std::str::FromStr>(suffix: u8) -> T
where
    T::Err: std::fmt::Debug,
{
    format!("01912345-6789-7abc-8def-0123456789{suffix:02x}")
        .parse()
        .unwrap()
}

fn timestamp(offset_micros: i64) -> Timestamp {
    Timestamp::from_unix_micros(1_893_456_000_000_000 + offset_micros).unwrap()
}

fn payload() -> JournalPayload {
    JournalPayload::new(
        SchemaReference::new(
            "https://stateknot.github.io/schema/a2a/task-update/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"canonical-a2a-task-update-schema"),
        ),
        JournalEventKind::new("a2a-task-update").unwrap(),
        BoundedJson::try_from_value(json!({
            "context_id": "context-17",
            "state": "completed",
            "task_id": "task-42"
        }))
        .unwrap(),
    )
    .unwrap()
}

fn delivery() -> OutboxDelivery {
    let tenant_id = TenantId::new("tenant-canonical").unwrap();
    let run_id = id::<RunId>(0x10);
    let origin_event_id = id::<EventId>(0x11);
    let intent = OutboxDeliveryIntent::new(
        tenant_id.clone(),
        run_id,
        id::<DeliveryId>(0x12),
        origin_event_id,
        OutboxDestinationRef::new(
            tenant_id.clone(),
            id::<DestinationId>(0x13),
            Digest::sha256(b"canonical-destination-snapshot"),
        ),
        payload(),
        timestamp(86_400_000_000),
    )
    .unwrap();
    OutboxDelivery::commit(
        intent,
        JournalHead::new(
            tenant_id,
            run_id,
            JournalSequence::new(7).unwrap(),
            origin_event_id,
            timestamp(0),
            Digest::sha256(b"canonical-origin-event"),
        ),
    )
    .unwrap()
}

fn start(delivery: &OutboxDelivery) -> OutboxAttemptStart {
    OutboxAttemptStart::new(
        delivery,
        DeliveryFence::new(
            delivery.intent().tenant_id().clone(),
            delivery.intent().run_id(),
            delivery.intent().delivery_id(),
            id::<AttemptId>(0x20),
            FencingEpoch::FIRST,
        ),
        timestamp(1_000_000),
        timestamp(31_000_000),
    )
    .unwrap()
}

fn failure_completion(start: &OutboxAttemptStart) -> OutboxAttemptCompletion {
    let failure = Failure::new(
        id::<FailureId>(0x30),
        FailureCategory::Internal,
        FailureCode::new("a2a.push_unavailable").unwrap(),
        FailureOrigin::new("stateknot.protocol.a2a").unwrap(),
        FailureMessage::new("Remote notification endpoint is unavailable").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(2_500).unwrap(),
        },
    )
    .unwrap();
    OutboxAttemptCompletion::fail(start, failure, timestamp(3_000_000)).unwrap()
}

fn canonical_wire_digest(value: Value) -> Digest {
    CanonicalJson::new(
        &BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap(),
    )
    .unwrap()
    .digest()
}

#[test]
fn canonical_outbox_freezes_success_and_failure_wires() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    let delivery = delivery();
    let start = start(&delivery);
    let acknowledgement = OutboxAttemptCompletion::acknowledge(
        &start,
        Some(Digest::sha256(b"canonical-a2a-acknowledgement")),
        timestamp(2_000_000),
    )
    .unwrap();
    let failure = failure_completion(&start);
    let acknowledged =
        OutboxAttempt::restore(start.clone(), Some(acknowledgement.clone())).unwrap();
    let failed = OutboxAttempt::restore(start.clone(), Some(failure.clone())).unwrap();

    assert_eq!(
        [
            delivery.intent().intent_digest(),
            delivery.digest(),
            start.digest(),
            acknowledgement.digest(),
            canonical_wire_digest(to_value(&acknowledged).unwrap()),
            failure.digest(),
            canonical_wire_digest(to_value(&failed).unwrap()),
        ],
        [
            fixture.expected.intent,
            fixture.expected.delivery,
            fixture.expected.start,
            fixture.expected.acknowledgement,
            fixture.expected.acknowledged_wire,
            fixture.expected.failure,
            fixture.expected.failed_wire,
        ]
    );

    assert_eq!(
        from_value::<OutboxDeliveryIntent>(to_value(delivery.intent()).unwrap()).unwrap(),
        delivery.intent().clone()
    );
    assert_eq!(
        from_value::<OutboxDelivery>(to_value(&delivery).unwrap()).unwrap(),
        delivery
    );
    assert_eq!(
        from_value::<OutboxAttemptStartHead>(to_value(start.head()).unwrap()).unwrap(),
        start.head()
    );
    assert_eq!(
        from_value::<OutboxAttempt>(to_value(&acknowledged).unwrap())
            .unwrap()
            .status(),
        acknowledged.status()
    );
}

#[test]
fn canonical_outbox_fails_closed_after_tampering() {
    let delivery = delivery();
    let start = start(&delivery);
    let completion = failure_completion(&start);

    let mut changed_origin = to_value(&delivery).unwrap();
    changed_origin["origin"]["event_id"] = json!(id::<EventId>(0x14));
    assert!(from_value::<OutboxDelivery>(changed_origin).is_err());

    let mut changed_fence = to_value(&start).unwrap();
    changed_fence["fence"]["epoch"] = json!("2");
    assert!(from_value::<OutboxAttemptStart>(changed_fence).is_err());

    let mut changed_retry = to_value(&completion).unwrap();
    changed_retry["outcome"]["failure"]["retry_advice"] = json!({"kind": "never"});
    assert!(from_value::<OutboxAttemptCompletion>(changed_retry).is_err());

    let mut extra = to_value(delivery.intent()).unwrap();
    extra["credential"] = json!("must-not-be-persisted");
    assert!(from_value::<OutboxDeliveryIntent>(extra).is_err());
}

#[test]
fn outbox_schema_objects_remain_closed_and_attempts_are_bounded() {
    for schema in [
        to_value(schema_for!(OutboxDestinationRef)).unwrap(),
        to_value(schema_for!(OutboxDeliveryIntent)).unwrap(),
        to_value(schema_for!(OutboxDelivery)).unwrap(),
        to_value(schema_for!(OutboxDeliveryHead)).unwrap(),
        to_value(schema_for!(DeliveryFence)).unwrap(),
        to_value(schema_for!(OutboxAttemptStart)).unwrap(),
        to_value(schema_for!(OutboxAttemptStartHead)).unwrap(),
        to_value(schema_for!(OutboxAttemptCompletion)).unwrap(),
        to_value(schema_for!(OutboxAttempt)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }

    let outcome = to_value(schema_for!(OutboxAttemptOutcome)).unwrap();
    assert!(outcome.get("oneOf").is_some());
}
