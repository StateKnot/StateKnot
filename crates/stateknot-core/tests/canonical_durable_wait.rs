// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixture for durable interrupts and timers.

use schemars::schema_for;
use serde::Deserialize;
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    BoundedJson, CanonicalJson, Digest, DurableTimer, DurableTimerHead, DurableTimerRecord,
    EventId, InterruptId, InterruptRecord, InterruptRequest, InterruptRequestHead,
    InterruptRequestIntent, InterruptResolution, InterruptResolutionIntent, InterruptResolver,
    IssuerId, JournalEventKind, JournalHead, JournalPayload, JournalSequence, JsonLimits,
    PrincipalIdentity, RunId, RunInterruptKind, RunTimerKind, SchemaId, SchemaReference, Scope,
    ScopeSet, SubjectId, TenantId, TimerFiring, TimerFiringIntent, TimerId,
    TimerRegistrationIntent, Timestamp, Version, WaitRegistrationIntent,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-durable-wait/1.0.0";
const BASE_MICROS: i64 = 1_893_456_000_000_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    expected: ExpectedDigests,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    interrupt_intent: Digest,
    interrupt_request: Digest,
    resolution_intent: Digest,
    resolution: Digest,
    interrupt_record_wire: Digest,
    timer_intent: Digest,
    timer: Digest,
    firing_intent: Digest,
    firing: Digest,
    timer_record_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-durable-wait-v1.json"))
        .expect("canonical durable wait fixture must be valid JSON")
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
    Timestamp::from_unix_micros(BASE_MICROS + offset_micros).unwrap()
}

fn payload(kind: &str, value: Value) -> JournalPayload {
    JournalPayload::new(
        SchemaReference::new(
            format!("https://stateknot.github.io/schema/wait/{kind}/1.0.0")
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(format!("canonical-{kind}-schema")),
        ),
        JournalEventKind::new(kind).unwrap(),
        BoundedJson::try_from_value(value).unwrap(),
    )
    .unwrap()
}

fn principal() -> PrincipalIdentity {
    PrincipalIdentity::new(
        "https://issuer.example.com/tenant"
            .parse::<IssuerId>()
            .unwrap(),
        "incident-approver".parse::<SubjectId>().unwrap(),
    )
}

fn scopes(values: &[&str]) -> ScopeSet {
    ScopeSet::try_new(values.iter().map(|value| value.parse::<Scope>().unwrap())).unwrap()
}

fn journal_head(
    tenant_id: TenantId,
    run_id: RunId,
    sequence: u64,
    event_id: EventId,
    recorded_at: Timestamp,
) -> JournalHead {
    JournalHead::new(
        tenant_id,
        run_id,
        JournalSequence::new(sequence).unwrap(),
        event_id,
        recorded_at,
        Digest::sha256(format!("canonical-wait-event-{sequence}")),
    )
}

fn interrupt_history() -> (InterruptRequest, InterruptResolution, InterruptRecord) {
    let tenant_id = TenantId::new("tenant-canonical").unwrap();
    let run_id = id::<RunId>(0x50);
    let request_event_id = id::<EventId>(0x51);
    let intent = InterruptRequestIntent::new(
        tenant_id.clone(),
        run_id,
        id::<InterruptId>(0x52),
        request_event_id,
        RunInterruptKind::Approval,
        payload(
            "approval-request",
            json!({"action": "deploy", "environment": "production"}),
        ),
        Digest::sha256(b"canonical-deploy-action"),
        Some(principal()),
        scopes(&["agent.approve", "run.resolve"]),
        Some(timestamp(600_000_000)),
    )
    .unwrap();
    let request = InterruptRequest::commit(
        intent,
        journal_head(
            tenant_id.clone(),
            run_id,
            11,
            request_event_id,
            timestamp(0),
        ),
    )
    .unwrap();
    let resolution_event_id = id::<EventId>(0x53);
    let resolution_intent = InterruptResolutionIntent::new(
        &request,
        resolution_event_id,
        payload(
            "approval-resolution",
            json!({"approved": true, "comment": "change reviewed"}),
        ),
        InterruptResolver::new(
            principal(),
            scopes(&["agent.approve", "audit.read", "run.resolve"]),
        ),
    )
    .unwrap();
    let resolution = InterruptResolution::commit(
        resolution_intent,
        journal_head(
            tenant_id,
            run_id,
            12,
            resolution_event_id,
            timestamp(120_000_000),
        ),
    )
    .unwrap();
    let record = InterruptRecord::restore(request.clone(), Some(resolution.clone())).unwrap();
    (request, resolution, record)
}

fn timer_history() -> (DurableTimer, TimerFiring, DurableTimerRecord) {
    let tenant_id = TenantId::new("tenant-canonical").unwrap();
    let run_id = id::<RunId>(0x50);
    let registration_event_id = id::<EventId>(0x60);
    let intent = TimerRegistrationIntent::new(
        tenant_id.clone(),
        run_id,
        id::<TimerId>(0x61),
        registration_event_id,
        RunTimerKind::RetryBackoff,
        timestamp(30_000_000),
    )
    .unwrap();
    let timer = DurableTimer::commit(
        intent,
        journal_head(
            tenant_id.clone(),
            run_id,
            13,
            registration_event_id,
            timestamp(0),
        ),
    )
    .unwrap();
    let firing_event_id = id::<EventId>(0x62);
    let firing_intent = TimerFiringIntent::new(&timer, firing_event_id).unwrap();
    let firing = TimerFiring::commit(
        firing_intent,
        journal_head(
            tenant_id,
            run_id,
            14,
            firing_event_id,
            timestamp(30_000_000),
        ),
    )
    .unwrap();
    let record = DurableTimerRecord::restore(timer.clone(), Some(firing.clone())).unwrap();
    (timer, firing, record)
}

fn canonical_wire_digest(value: Value) -> Digest {
    CanonicalJson::new(
        &BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap(),
    )
    .unwrap()
    .digest()
}

#[test]
fn canonical_durable_wait_freezes_all_integrity_layers() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    let (request, resolution, interrupt_record) = interrupt_history();
    let (timer, firing, timer_record) = timer_history();

    assert_eq!(
        [
            request.intent().intent_digest(),
            request.digest(),
            resolution.intent().intent_digest(),
            resolution.digest(),
            canonical_wire_digest(to_value(&interrupt_record).unwrap()),
            timer.intent().intent_digest(),
            timer.digest(),
            firing.intent().intent_digest(),
            firing.digest(),
            canonical_wire_digest(to_value(&timer_record).unwrap()),
        ],
        [
            fixture.expected.interrupt_intent,
            fixture.expected.interrupt_request,
            fixture.expected.resolution_intent,
            fixture.expected.resolution,
            fixture.expected.interrupt_record_wire,
            fixture.expected.timer_intent,
            fixture.expected.timer,
            fixture.expected.firing_intent,
            fixture.expected.firing,
            fixture.expected.timer_record_wire,
        ]
    );

    assert_eq!(
        from_value::<InterruptRecord>(to_value(&interrupt_record).unwrap()).unwrap(),
        interrupt_record
    );
    assert_eq!(
        from_value::<DurableTimerRecord>(to_value(&timer_record).unwrap()).unwrap(),
        timer_record
    );
}

#[test]
fn canonical_durable_wait_fails_closed_after_tampering() {
    let (request, resolution, _) = interrupt_history();
    let (timer, firing, _) = timer_history();

    let mut changed_principal = to_value(request.head()).unwrap();
    changed_principal["required_principal"]["subject"] = json!("different-approver");
    assert!(from_value::<InterruptRequestHead>(changed_principal).is_err());

    let mut changed_resolution_event = to_value(&resolution).unwrap();
    changed_resolution_event["journal"]["event_id"] = json!(id::<EventId>(0x54));
    assert!(from_value::<InterruptResolution>(changed_resolution_event).is_err());

    let mut changed_timer_head = to_value(timer.head()).unwrap();
    changed_timer_head["marker"]["due_at"] = json!(timestamp(31_000_000));
    assert!(from_value::<DurableTimerHead>(changed_timer_head).is_err());

    let mut changed_firing = to_value(&firing).unwrap();
    changed_firing["digest"] = json!(Digest::sha256(b"substituted-firing"));
    assert!(from_value::<TimerFiring>(changed_firing).is_err());
}

#[test]
fn durable_wait_schema_objects_are_closed_and_variants_are_explicit() {
    for schema in [
        to_value(schema_for!(InterruptRequestIntent)).unwrap(),
        to_value(schema_for!(InterruptRequest)).unwrap(),
        to_value(schema_for!(InterruptRequestHead)).unwrap(),
        to_value(schema_for!(InterruptResolver)).unwrap(),
        to_value(schema_for!(InterruptResolutionIntent)).unwrap(),
        to_value(schema_for!(InterruptResolution)).unwrap(),
        to_value(schema_for!(InterruptRecord)).unwrap(),
        to_value(schema_for!(TimerRegistrationIntent)).unwrap(),
        to_value(schema_for!(DurableTimer)).unwrap(),
        to_value(schema_for!(DurableTimerHead)).unwrap(),
        to_value(schema_for!(TimerFiringIntent)).unwrap(),
        to_value(schema_for!(TimerFiring)).unwrap(),
        to_value(schema_for!(DurableTimerRecord)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }

    let registration = to_value(schema_for!(WaitRegistrationIntent)).unwrap();
    assert!(registration.get("oneOf").is_some());
}
