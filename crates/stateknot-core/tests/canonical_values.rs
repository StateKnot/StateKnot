// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Type-level positive/negative wire and canonical round trips for core values.

use std::{collections::BTreeSet, fmt::Debug};

use proptest::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    AgentSubmissionKey, ArtifactId, AttemptId, AuthorizationReceiptId, BoundedJson, ByteCount,
    CanonicalJson, CapabilityName, CheckpointId, CurrencyCode, DeliveryId, DestinationId, Digest,
    DurationMillis, EventId, ExecutionCount, FailureId, InterruptId, InvocationId, IssuerId,
    MessageId, Money, PrincipalIdentity, QuarantineId, RunId, SchedulerReservationId,
    SchedulerShardId, SchemaId, SchemaReference, Scope, ScopeSet, SkillActingWindowId,
    SkillActivationApprovalId, SubjectId, TenantId, ThreadId, TimerId, Timestamp, TokenCount,
    Version,
};
use uuid::Uuid;

fn canonical(value: Value) -> CanonicalJson {
    CanonicalJson::new(&BoundedJson::try_from_value(value).unwrap()).unwrap()
}

fn round_trip<T: DeserializeOwned + Serialize + Eq + Debug>(wire: &Value) {
    let expected = canonical(wire.clone());
    let decoded: T = serde_json::from_value(wire.clone()).unwrap();
    let encoded = serde_json::to_value(&decoded).unwrap();
    assert_eq!(&encoded, wire);
    assert_eq!(canonical(encoded), expected);
    let reparsed: T = serde_json::from_slice(expected.as_bytes()).unwrap();
    assert_eq!(reparsed, decoded);
    assert_eq!(canonical(serde_json::to_value(reparsed).unwrap()), expected);
}

fn fixture<T: DeserializeOwned + Serialize + Eq + Debug>(
    source: &str,
    schema: &str,
    section: &str,
    text_field: Option<&str>,
) {
    let document = BoundedJson::from_str(source).unwrap();
    let document = document.as_value();
    assert_eq!(document["schema"], schema);
    let valid = document[section]["valid"].as_array().unwrap();
    let invalid = document[section]["invalid"].as_array().unwrap();
    assert!(!valid.is_empty());
    assert!(!invalid.is_empty());
    for value in valid {
        let wire = text_field.map_or(value, |field| &value[field]);
        round_trip::<T>(wire);
    }
    for value in invalid {
        assert!(
            serde_json::from_value::<T>(value.clone()).is_err(),
            "{} accepted negative fixture {value}",
            std::any::type_name::<T>()
        );
    }
}

macro_rules! value_fixtures {
    ($( $name:ident: $ty:ty => ($family:literal, $version:literal, $section:literal, $field:expr) ),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                fixture::<$ty>(
                    include_str!(concat!("fixtures/core-", $family, "-v", $version, ".json")),
                    concat!("https://stateknot.github.io/schema/test-fixture/core-", $family, "/", $version, ".0.0"),
                    $section,
                    $field,
                );
            }
        )+
    };
}

value_fixtures! {
    tenant_id: TenantId => ("identifiers", "1", "tenants", None),
    scheduler_shard_id: SchedulerShardId => ("identifiers", "1", "shards", None),
    agent_submission_key: AgentSubmissionKey => ("identifiers", "1", "submission_keys", None),
    version: Version => ("scalars", "1", "versions", Some("text")),
    digest: Digest => ("scalars", "1", "digests", Some("text")),
    timestamp: Timestamp => ("time", "2", "timestamps", Some("text")),
    duration_millis: DurationMillis => ("time", "2", "durations_millis", Some("text")),
    token_count: TokenCount => ("accounting", "1", "counts", None),
    byte_count: ByteCount => ("accounting", "1", "counts", None),
    execution_count: ExecutionCount => ("accounting", "1", "counts", None),
    currency_code: CurrencyCode => ("accounting", "1", "currencies", None),
    money: Money => ("accounting", "1", "money", None),
    issuer_id: IssuerId => ("identity", "1", "issuers", None),
    subject_id: SubjectId => ("identity", "1", "subjects", None),
    principal_identity: PrincipalIdentity => ("identity", "1", "principal_identities", None),
    schema_id: SchemaId => ("schema", "1", "ids", None),
    schema_reference: SchemaReference => ("schema", "1", "references", None),
    capability_name: CapabilityName => ("authorization", "1", "capability_names", None),
    scope: Scope => ("authorization", "1", "scopes", None),
    scope_set: ScopeSet => ("authorization", "1", "scope_sets", None),
}

macro_rules! generated_ids {
    ($( $module:ident: $ty:ty ),+ $(,)?) => {
        const GENERATED_TYPES: &[&str] = &[$(stringify!($ty)),+];
        $(
            mod $module {
                use super::*;

                #[test]
                fn canonical_fixture() {
                    fixture::<$ty>(
                        include_str!("fixtures/core-identifiers-v1.json"),
                        "https://stateknot.github.io/schema/test-fixture/core-identifiers/1.0.0",
                        "uuid_v7",
                        None,
                    );
                }

                proptest! {
                    #![proptest_config(ProptestConfig::with_cases(256))]
                    #[test]
                    fn arbitrary_uuid_bits_preserve_identity_and_reject_other_versions(mut bytes in any::<[u8; 16]>()) {
                        bytes[6] = (bytes[6] & 0x0f) | 0x70;
                        bytes[8] = (bytes[8] & 0x3f) | 0x80;
                        let uuid = Uuid::from_bytes(bytes);
                        let id = <$ty>::from_uuid(uuid).unwrap();
                        prop_assert_eq!(id.as_uuid(), &uuid);
                        prop_assert_eq!(id.to_string().parse::<$ty>().unwrap(), id);
                        round_trip::<$ty>(&Value::String(id.to_string()));

                        for version in 0..=15 {
                            if version == 7 { continue; }
                            let mut wrong = bytes;
                            wrong[6] = (wrong[6] & 0x0f) | (version << 4);
                            let wrong = Uuid::from_bytes(wrong);
                            prop_assert!(<$ty>::from_uuid(wrong).is_err());
                            prop_assert!(serde_json::from_value::<$ty>(Value::String(wrong.to_string())).is_err());
                        }
                        for variant in [0x00, 0x40, 0xc0] {
                            let mut wrong = bytes;
                            wrong[8] = (wrong[8] & 0x3f) | variant;
                            let wrong = Uuid::from_bytes(wrong);
                            prop_assert!(<$ty>::from_uuid(wrong).is_err());
                            prop_assert!(serde_json::from_value::<$ty>(Value::String(wrong.to_string())).is_err());
                        }
                    }
                }
            }
        )+
    };
}

generated_ids! {
    run_id: RunId,
    thread_id: ThreadId,
    event_id: EventId,
    failure_id: FailureId,
    message_id: MessageId,
    artifact_id: ArtifactId,
    authorization_receipt_id: AuthorizationReceiptId,
    skill_activation_approval_id: SkillActivationApprovalId,
    skill_acting_window_id: SkillActingWindowId,
    invocation_id: InvocationId,
    interrupt_id: InterruptId,
    timer_id: TimerId,
    delivery_id: DeliveryId,
    destination_id: DestinationId,
    checkpoint_id: CheckpointId,
    quarantine_id: QuarantineId,
    attempt_id: AttemptId,
    scheduler_reservation_id: SchedulerReservationId,
}

#[test]
fn every_macro_generated_identifier_has_typed_fixture_and_property_coverage() {
    let declared = include_str!("../src/ids.rs")
        .split("define_generated_id!(")
        .skip(1)
        .map(|tail| tail.split_once(',').unwrap().0.trim())
        .collect::<BTreeSet<_>>();
    let covered = GENERATED_TYPES.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(declared, covered);
    assert_eq!(covered.len(), GENERATED_TYPES.len());
}
