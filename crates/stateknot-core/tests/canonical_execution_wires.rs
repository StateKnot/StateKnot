// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Typed readers for complete durable execution wires, including nested values.
//! Existing family tests also compare their original constructors with this
//! document and retain the earlier domain-separated digest expectations.

use std::{any::type_name, collections::BTreeSet, sync::LazyLock};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use stateknot_core::*;

static FIXTURE: LazyLock<Value> = LazyLock::new(|| {
    let document = BoundedJson::from_str_with_limits(
        include_str!("fixtures/core-execution-wires-v1.json"),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    assert_eq!(
        document.as_value()["schema"],
        "https://stateknot.github.io/schema/test-fixture/core-execution-wires/1.0.0"
    );
    document.into_value()
});

fn wire(pointer: &str) -> &Value {
    FIXTURE
        .pointer(pointer)
        .unwrap_or_else(|| panic!("missing execution vector {pointer}"))
}

fn round_trip<T: DeserializeOwned + Serialize>(pointer: &str) {
    let expected = wire(pointer);
    let decoded = serde_json::from_value::<T>(expected.clone())
        .unwrap_or_else(|error| panic!("{} at {pointer}: {error}", type_name::<T>()));
    assert_eq!(&serde_json::to_value(&decoded).unwrap(), expected);
    let bounded =
        BoundedJson::try_from_value_with_limits(expected.clone(), JsonLimits::MAXIMUM).unwrap();
    let canonical = CanonicalJson::new(&bounded).unwrap();
    let restored: T = serde_json::from_slice(canonical.as_bytes()).unwrap();
    let restored = BoundedJson::try_from_value_with_limits(
        serde_json::to_value(restored).unwrap(),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    let restored = CanonicalJson::new(&restored).unwrap();
    assert_eq!(restored.as_bytes(), canonical.as_bytes());
    assert_eq!(restored.digest(), canonical.digest());
}

fn reject<T: DeserializeOwned>(invalid: &Value, reason: &str) {
    assert!(
        serde_json::from_value::<T>(invalid.clone()).is_err(),
        "{} accepted {reason}: {invalid}",
        type_name::<T>()
    );
}

fn closed_object<T: DeserializeOwned + Serialize>(pointer: &str, checked_fields: &[&str]) {
    round_trip::<T>(pointer);
    let expected = wire(pointer);
    let object = expected.as_object().unwrap();
    for invalid in [
        Value::Null,
        json!(false),
        json!(7),
        json!("wire"),
        json!([]),
    ] {
        reject::<T>(&invalid, "non-object wire");
    }
    let mut extra = object.clone();
    assert!(
        extra
            .insert("untrusted_authority".into(), json!(true))
            .is_none()
    );
    reject::<T>(&Value::Object(extra), "unknown authority field");

    // Decode raw JSON too: Value would already have discarded duplicate keys.
    for (key, value) in object {
        let encoded = serde_json::to_string(expected).unwrap();
        let duplicate = format!(
            "{{{}:{},{}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap(),
            &encoded[1..]
        );
        assert!(
            serde_json::from_str::<T>(&duplicate).is_err(),
            "{} at {pointer} accepted duplicate {key}",
            type_name::<T>()
        );
    }
    for field in checked_fields {
        let mut substituted = expected.clone();
        assert!(
            substituted.get(field).is_some(),
            "missing checked field {field}"
        );
        substituted[field] = json!(Digest::sha256(b"substituted execution evidence"));
        reject::<T>(&substituted, "substituted checked digest");
        let mut omitted = object.clone();
        omitted.remove(*field).unwrap();
        reject::<T>(&Value::Object(omitted), "omitted checked digest");
    }
}

macro_rules! object_wires {
    ($( $name:ident: $ty:ty => [$($pointer:literal),+] / [$($field:literal),*] ),+ $(,)?) => {
        const OBJECT_TYPES: &[&str] = &[$(stringify!($ty)),+];
        $(
            #[test]
            fn $name() {
                let checked_fields: &[&str] = &[$($field),*];
                $(closed_object::<$ty>($pointer, checked_fields);)+
            }
        )+
    };
}

object_wires! {
    compiled_graph: CompiledGraph => ["/graph/graph"] / ["definition_digest"],
    graph_execution_limits: GraphExecutionLimits => ["/graph/graph/limits"] / [],
    graph_node: GraphNode => ["/graph/graph/nodes/0", "/graph/graph/nodes/1", "/graph/graph/nodes/2"] / [],
    graph_reducer_reference: GraphReducerReference => ["/graph/graph/reducer"] / [],
    graph_route: GraphRoute => ["/graph/graph/nodes/2/routes/0", "/graph/graph/nodes/2/routes/1"] / [],
    checkpoint_head: CheckpointHead => ["/barrier/barrier/base_checkpoint"] / [],
    checkpoint_state: CheckpointState => ["/barrier/barrier/successor/state"] / ["digest"],
    checkpoint_write: CheckpointWrite => ["/barrier/barrier/successor"] / ["intent_digest"],
    graph_reference: GraphReference => ["/barrier/barrier/base_checkpoint/graph"] / [],
    checkpoint_barrier: CheckpointBarrier => ["/barrier/barrier"] / ["intent_digest"],
    node_activation: NodeActivation => ["/node_result/head/activation"] / [],
    run_fence: RunFence => ["/node_result/head/fence"] / [],
    journal_head: JournalHead => ["/node_result/head/journal_head"] / [],
    node_attempt: NodeAttempt => ["/node_attempt/attempts/0", "/node_attempt/attempts/1", "/node_attempt/attempts/2"] / [],
    node_attempt_start: NodeAttemptStart => ["/node_attempt/attempts/0/start"] / ["activation_digest", "digest"],
    node_attempt_start_head: NodeAttemptStartHead => ["/node_attempt/start_head"] / ["digest"],
    node_attempt_completion: NodeAttemptCompletion => ["/node_attempt/attempts/1/completion", "/node_attempt/attempts/2/completion"] / ["digest"],
    node_attempt_outcome: NodeAttemptOutcome => ["/node_attempt/attempts/1/completion/outcome", "/node_attempt/attempts/2/completion/outcome"] / [],
    node_control: NodeControl => ["/durable_wait/controls/0", "/durable_wait/controls/1", "/durable_wait/controls/2", "/durable_wait/controls/3"] / [],
    node_state_change: NodeStateChange => ["/node_result/result/intent/state_change"] / [],
    node_state_update: NodeStateUpdate => ["/node_result/result/intent/state_change/update"] / ["digest"],
    node_terminal_output: NodeTerminalOutput => ["/durable_wait/controls/3/output"] / ["digest"],
    node_wait: NodeWait => ["/durable_wait/controls/2/waits/0", "/durable_wait/controls/2/waits/1"] / [],
    node_invocation_binding: NodeInvocationBinding => ["/node_result/result/intent/bindings/0", "/model_invocation/node_binding"] / [],
    pending_node_result: PendingNodeResult => ["/node_result/result"] / ["digest"],
    pending_node_result_head: PendingNodeResultHead => ["/node_result/head"] / [],
    pending_node_result_intent: PendingNodeResultIntent => ["/node_result/result/intent"] / ["intent_digest"],
    delivery_fence: DeliveryFence => ["/outbox/attempts/0/start/fence"] / [],
    outbox_destination: OutboxDestinationRef => ["/outbox/delivery/intent/destination"] / [],
    outbox_delivery: OutboxDelivery => ["/outbox/delivery"] / ["digest"],
    outbox_delivery_intent: OutboxDeliveryIntent => ["/outbox/delivery/intent"] / ["intent_digest"],
    outbox_delivery_head: OutboxDeliveryHead => ["/outbox/delivery_head"] / ["digest"],
    outbox_attempt: OutboxAttempt => ["/outbox/attempts/0", "/outbox/attempts/1", "/outbox/attempts/2"] / [],
    outbox_attempt_start: OutboxAttemptStart => ["/outbox/attempts/0/start"] / ["digest"],
    outbox_attempt_start_head: OutboxAttemptStartHead => ["/outbox/start_head"] / ["digest"],
    outbox_attempt_completion: OutboxAttemptCompletion => ["/outbox/attempts/1/completion", "/outbox/attempts/2/completion"] / ["digest"],
    outbox_attempt_outcome: OutboxAttemptOutcome => ["/outbox/attempts/1/completion/outcome", "/outbox/attempts/2/completion/outcome"] / [],
    interrupt_record: InterruptRecord => ["/durable_wait/interrupt", "/durable_wait/pending_interrupt"] / [],
    interrupt_request: InterruptRequest => ["/durable_wait/interrupt/request"] / ["digest"],
    interrupt_request_head: InterruptRequestHead => ["/durable_wait/interrupt/resolution/intent/request"] / ["digest"],
    interrupt_request_intent: InterruptRequestIntent => ["/durable_wait/interrupt/request/intent"] / ["intent_digest"],
    interrupt_resolution: InterruptResolution => ["/durable_wait/interrupt/resolution"] / ["digest"],
    interrupt_resolution_intent: InterruptResolutionIntent => ["/durable_wait/interrupt/resolution/intent"] / ["intent_digest"],
    interrupt_resolver: InterruptResolver => ["/durable_wait/interrupt/resolution/intent/resolver"] / [],
    durable_timer: DurableTimer => ["/durable_wait/timer/timer"] / ["digest"],
    durable_timer_head: DurableTimerHead => ["/durable_wait/timer/firing/intent/timer"] / ["digest"],
    durable_timer_record: DurableTimerRecord => ["/durable_wait/timer", "/durable_wait/pending_timer"] / [],
    timer_firing: TimerFiring => ["/durable_wait/timer/firing"] / ["digest"],
    timer_firing_intent: TimerFiringIntent => ["/durable_wait/timer/firing/intent"] / ["intent_digest"],
    timer_registration_intent: TimerRegistrationIntent => ["/durable_wait/timer/timer/intent"] / ["intent_digest"],
    wait_registration_intent: WaitRegistrationIntent => ["/durable_wait/registrations/0", "/durable_wait/registrations/1"] / [],
    durable_wait: DurableWait => ["/durable_wait/waits/0", "/durable_wait/waits/1"] / [],
    model_invocation: ModelInvocation => ["/model_invocation/records/0", "/model_invocation/records/1", "/model_invocation/records/2", "/model_invocation/records/3", "/model_invocation/records/4"] / ["digest"],
    model_invocation_intent: ModelInvocationIntent => ["/model_invocation/records/0/intent"] / ["intent_digest"],
    model_invocation_head: ModelInvocationHead => ["/model_invocation/records/1/previous", "/model_invocation/records/2/previous", "/model_invocation/records/4/previous"] / [],
    model_invocation_state: ModelInvocationState => ["/model_invocation/records/0/state", "/model_invocation/records/1/state", "/model_invocation/records/2/state", "/model_invocation/records/4/state"] / [],
    model_invocation_transition: ModelInvocationTransition => ["/model_invocation/records/1/transition", "/model_invocation/records/2/transition", "/model_invocation/records/4/transition"] / [],
    tool_invocation: ToolInvocation => ["/tool_invocation/records/0", "/tool_invocation/records/1", "/tool_invocation/records/2"] / ["digest"],
    tool_invocation_intent: ToolInvocationIntent => ["/tool_invocation/records/0/intent"] / ["intent_digest"],
    tool_invocation_head: ToolInvocationHead => ["/tool_invocation/records/1/previous", "/tool_invocation/records/2/previous"] / [],
    tool_invocation_state: ToolInvocationState => ["/tool_invocation/records/0/state", "/tool_invocation/records/1/state", "/tool_invocation/records/2/state"] / [],
    tool_invocation_transition: ToolInvocationTransition => ["/tool_invocation/records/1/transition", "/tool_invocation/records/2/transition"] / [],
}

fn collection<T: DeserializeOwned + Serialize>(pointer: &str, empty_allowed: bool) {
    round_trip::<T>(pointer);
    for invalid in [
        Value::Null,
        json!(false),
        json!(7),
        json!("wire"),
        json!({}),
    ] {
        reject::<T>(&invalid, "non-array wire");
    }
    let entries = wire(pointer).as_array().unwrap();
    assert!(!entries.is_empty());
    reject::<T>(&json!([entries[0], entries[0]]), "duplicate identity");
    assert_eq!(
        serde_json::from_value::<T>(json!([])).is_ok(),
        empty_allowed
    );
}

#[test]
fn identity_collections_preserve_wire_and_reject_duplicate_identity() {
    collection::<GraphRoutes>("/graph/graph/nodes/2/routes", true);
    collection::<ReadyNodes>("/graph/graph/entry_nodes", true);
    collection::<BarrierResultHeads>("/barrier/barrier/result_heads", false);
    collection::<NodeInvocationBindings>("/node_result/result/intent/bindings", true);
    collection::<NodeWaits>("/durable_wait/controls/2/waits", false);
}

#[test]
fn typed_object_matrix_has_no_duplicate_types() {
    let types = OBJECT_TYPES.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(types.len(), OBJECT_TYPES.len());
}

fn empty_variant<T: DeserializeOwned + Serialize>(tag: &str, variant: &str) {
    let expected = json!({tag: variant});
    let decoded: T = serde_json::from_value(expected.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    for extra in [Value::Null, json!(true), json!({"scope": "admin"})] {
        let mut changed = expected.clone();
        changed["untrusted_authority"] = extra;
        reject::<T>(&changed, "extra field on empty tagged variant");
    }
    let duplicate = format!("{{\"{tag}\":\"{variant}\",\"{tag}\":\"{variant}\"}}");
    assert!(serde_json::from_str::<T>(&duplicate).is_err());
    reject::<T>(
        &json!({tag: variant, "value": []}),
        "unexpected value field",
    );
    reject::<T>(&json!(variant), "untagged unit variant");
    reject::<T>(&json!([variant]), "sequence in place of tagged object");
}

#[test]
fn all_current_empty_tagged_unit_decoders_reject_extra_fields() {
    empty_variant::<NodeControl>("kind", "continue");
    empty_variant::<NodeStateChange>("kind", "unchanged");
    empty_variant::<ModelInvocationState>("status", "prepared");
    empty_variant::<ToolInvocationState>("status", "prepared");
    empty_variant::<JournalEventSource>("kind", "control_plane");
    empty_variant::<JournalExpectation>("kind", "empty");
    // RetryAdvice already has a strict map visitor; retain it as a control.
    empty_variant::<RetryAdvice>("kind", "never");
    empty_variant::<RetryAdvice>("kind", "reconcile_first");

    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/core-run-lifecycle-v1.json")).unwrap();
    let provenance = serde_json::from_value(fixture["provenance"].clone()).unwrap();
    let pending =
        RunLifecycle::admitted(provenance, "2030-01-01T00:00:00.000000Z".parse().unwrap());
    let expected = serde_json::to_value(pending).unwrap();
    let decoded: RunLifecycle = serde_json::from_value(expected.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    let mut changed = expected;
    changed["state"]["untrusted_authority"] = json!(true);
    reject::<RunLifecycle>(&changed, "extra field on pending run state");

    // The old digest remains unchanged when Serde discards an extra field.
    // The enclosing durable records must reject that input before hashing it.
    let mut model = wire("/model_invocation/records/0").clone();
    model["state"]["untrusted_authority"] = json!(true);
    reject::<ModelInvocation>(&model, "extra field on prepared model record");
    let mut tool = wire("/tool_invocation/records/0").clone();
    tool["state"]["untrusted_authority"] = json!(true);
    reject::<ToolInvocation>(&tool, "extra field on prepared tool record");
}

#[test]
fn compact_result_head_retains_its_context_bound_validation_contract() {
    let result: PendingNodeResult =
        serde_json::from_value(wire("/node_result/result").clone()).unwrap();
    let head: PendingNodeResultHead =
        serde_json::from_value(wire("/node_result/head").clone()).unwrap();
    assert_eq!(head, result.head());

    // The trusted compact-head constructor checks scope and journal ordering;
    // it does not restore a complete result. Full record verification and exact
    // head comparison remain required before using the reference at a barrier.
    let mut substituted = wire("/node_result/head").clone();
    substituted["digest"] = json!(Digest::sha256(b"substituted result reference"));
    let substituted: PendingNodeResultHead = serde_json::from_value(substituted).unwrap();
    assert_ne!(substituted, result.head());
    let mut corrupted = wire("/node_result/result").clone();
    corrupted["digest"] = json!(substituted.digest());
    reject::<PendingNodeResult>(&corrupted, "substituted full result digest");
}

#[test]
fn empty_variant_schema_baseline_allows_only_reviewed_media_input_changes() {
    // These fingerprints were captured on a312b0c2 before changing readers.
    // The map visitor preserved all seven pins. RFC-0023 later corrects three
    // nested media input schemas; retain the actual old pins and require the
    // reviewed before/after mapping, rather than rewriting this old fixture.
    let media: Value = serde_json::from_str(include_str!(
        "fixtures/core-media-type-input-schemas-v1.json"
    ))
    .unwrap();
    let mut reviewed = 0;
    macro_rules! unchanged_schema {
        ($($ty:ty),+) => { $(
            let bounded = BoundedJson::try_from_value_with_limits(
                serde_json::to_value(schemars::schema_for!($ty)).unwrap(),
                JsonLimits::MAXIMUM,
            ).unwrap();
            let canonical = CanonicalJson::new(&bounded).unwrap();
            let old = FIXTURE["schema_pins"][stringify!($ty)].as_str().unwrap();
            let expected = if let Some(change) = media["changes"].get(stringify!($ty)) {
                assert_eq!(change["before"].as_str().unwrap(), old);
                reviewed += 1;
                change["after"].as_str().unwrap()
            } else { old };
            assert_eq!(
                canonical.digest().to_string(),
                expected,
                "{} schema changed", stringify!($ty),
            );
        )+ };
    }
    unchanged_schema!(
        NodeControl,
        NodeStateChange,
        JournalEventSource,
        JournalExpectation,
        RunLifecycle,
        ModelInvocationState,
        ToolInvocationState
    );
    assert_eq!(reviewed, 3);
}
