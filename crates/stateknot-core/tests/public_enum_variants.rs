// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Every currently serialized public enum alternative has a canonical vector.
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use stateknot_core::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};
static FIXTURE: LazyLock<Value> = LazyLock::new(|| {
    BoundedJson::from_str_with_limits(
        include_str!("fixtures/core-public-enum-variants-v1.json"),
        JsonLimits::MAXIMUM,
    )
    .unwrap()
    .into_value()
});
fn cases(schema: &Value) -> Vec<Value> {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values.clone();
    }
    if let Some(value) = schema.get("const") {
        return vec![value.clone()];
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, property) in properties {
            if let Some(value) = property.get("const") {
                return vec![json!({key:value})];
            }
        }
    }
    for keyword in ["oneOf", "anyOf"] {
        if let Some(items) = schema.get(keyword).and_then(Value::as_array) {
            return items.iter().flat_map(cases).collect();
        }
    }
    panic!("unreviewed enum schema representation")
}
fn case_matches(case: &Value, wire: &Value) -> bool {
    match case {
        Value::Object(keys) => keys.iter().all(|(key, value)| wire.get(key) == Some(value)),
        _ => wire == case,
    }
}
#[path = "support/object_fields.rs"]
mod object_fields;
use object_fields::ObjectFieldValues;

fn canonical(wire: Value) -> CanonicalJson {
    CanonicalJson::new(&BoundedJson::try_from_value_with_limits(wire, JsonLimits::MAXIMUM).unwrap())
        .unwrap()
}
fn verify<T: DeserializeOwned + Serialize + JsonSchema>(name: &str) {
    let vectors = FIXTURE["types"][name].as_array().unwrap();
    assert!(!vectors.is_empty());
    let schema = serde_json::to_value(schemars::schema_for!(T)).unwrap();
    let expected: BTreeSet<_> = cases(&schema)
        .iter()
        .map(|case| serde_json::to_string(case).unwrap())
        .collect();
    let actual: BTreeSet<_> = vectors
        .iter()
        .map(|vector| serde_json::to_string(&vector["case"]).unwrap())
        .collect();
    assert_eq!(
        expected, actual,
        "{name} missing or changed enum alternative"
    );
    assert_eq!(
        actual.len(),
        vectors.len(),
        "{name} duplicate enum alternative"
    );
    for vector in vectors {
        let wire = &vector["wire"];
        assert!(case_matches(&vector["case"], wire));
        let typed: T = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&typed).unwrap(), *wire);
        let expected = canonical(wire.clone());
        let digest: Digest = serde_json::from_value(vector["canonical_digest"].clone()).unwrap();
        assert_eq!(expected.digest(), digest);
        let restored: T = serde_json::from_slice(expected.as_bytes()).unwrap();
        assert_eq!(canonical(serde_json::to_value(restored).unwrap()), expected);
        if let Some(object) = wire.as_object() {
            let fields: ObjectFieldValues =
                serde_json::from_slice(&serde_json::to_vec(&typed).unwrap()).unwrap();
            let sequence = Value::Array(fields.0);
            assert!(serde_json::from_value::<T>(sequence.clone()).is_err());
            assert!(serde_json::from_slice::<T>(&serde_json::to_vec(&sequence).unwrap()).is_err());
            let mut extra = object.clone();
            assert!(
                extra
                    .insert("unreviewed_variant_field".into(), Value::Bool(true))
                    .is_none()
            );
            assert!(serde_json::from_value::<T>(Value::Object(extra)).is_err());
            for (key, value) in object {
                let key = serde_json::to_string(key).unwrap();
                let value = serde_json::to_string(value).unwrap();
                let raw = serde_json::to_string(wire).unwrap();
                let duplicate = format!("{{{key}:{value},{}", &raw[1..]);
                assert!(
                    serde_json::from_str::<T>(&duplicate).is_err(),
                    "{name} duplicate key {key}"
                );
            }
        }
    }
}
macro_rules! enum_readers {
    ($( $test:ident: $ty:ty ),+ $(,)?) => {
        const TYPED_ENUMS: &[&str] = &[$(stringify!($ty)),+];
        $(#[test] fn $test() { verify::<$ty>(stringify!($ty)); })+
    };
}
enum_readers! {
    agent_structured_output_strategy: AgentStructuredOutputStrategy,
    agent_tool_concurrency: AgentToolConcurrency,
    artifact_modality: ArtifactModality,
    budget_dimension: BudgetDimension,
    capability_kind: CapabilityKind,
    content_part: ContentPart,
    content_source: ContentSource,
    content_trust: ContentTrust,
    durable_wait: DurableWait,
    extension_value: ExtensionValue,
    failure_category: FailureCategory,
    instruction_content: InstructionContent,
    journal_event_source: JournalEventSource,
    journal_expectation: JournalExpectation,
    message_producer: MessageProducer,
    message_producer_kind: MessageProducerKind,
    message_role: MessageRole,
    model_capability_issue: ModelCapabilityIssue,
    model_error_phase: ModelErrorPhase,
    model_event_kind: ModelEventKind,
    model_finish_reason: ModelFinishReason,
    model_invocation_state: ModelInvocationState,
    model_invocation_status: ModelInvocationStatus,
    model_invocation_transition: ModelInvocationTransition,
    model_invocation_transition_kind: ModelInvocationTransitionKind,
    model_modality: ModelModality,
    model_output_delta: ModelOutputDelta,
    model_output_delta_kind: ModelOutputDeltaKind,
    model_output_item: ModelOutputItem,
    model_output_start: ModelOutputStart,
    model_response_mode: ModelResponseMode,
    model_structured_output_level: ModelStructuredOutputLevel,
    model_text_output_format: ModelTextOutputFormat,
    model_tool_choice: ModelToolChoice,
    model_tool_outcome: ModelToolOutcome,
    model_tool_selection: ModelToolSelection,
    model_usage_field: ModelUsageField,
    node_attempt_outcome: NodeAttemptOutcome,
    node_attempt_status: NodeAttemptStatus,
    node_control: NodeControl,
    node_control_kind: NodeControlKind,
    node_invocation_binding: NodeInvocationBinding,
    node_invocation_binding_kind: NodeInvocationBindingKind,
    node_state_change: NodeStateChange,
    node_wait: NodeWait,
    outbox_attempt_outcome: OutboxAttemptOutcome,
    outbox_attempt_status: OutboxAttemptStatus,
    outbox_delivery_status: OutboxDeliveryStatus,
    redaction_state: RedactionState,
    retry_advice: RetryAdvice,
    run_interrupt_kind: RunInterruptKind,
    run_status: RunStatus,
    run_timer_kind: RunTimerKind,
    run_transition: RunTransition,
    run_transition_kind: RunTransitionKind,
    run_wait: RunWait,
    skill_acting_window_revocation_reason: SkillActingWindowRevocationReason,
    skill_activation_source: SkillActivationSource,
    tool_authorization_operation: ToolAuthorizationOperation,
    tool_cancellation_support: ToolCancellationSupport,
    tool_error_phase: ToolErrorPhase,
    tool_external_effect: ToolExternalEffect,
    tool_idempotency: ToolIdempotency,
    tool_invocation_limit: ToolInvocationLimit,
    tool_invocation_state: ToolInvocationState,
    tool_invocation_status: ToolInvocationStatus,
    tool_invocation_transition: ToolInvocationTransition,
    tool_invocation_transition_kind: ToolInvocationTransitionKind,
    tool_resource_access: ToolResourceAccess,
    tool_risk: ToolRisk,
    wait_registration_intent: WaitRegistrationIntent,
}

#[test]
fn every_public_serialized_enum_has_exactly_one_closed_entry() {
    let inventory: Value =
        serde_json::from_str(include_str!("fixtures/core-public-type-inventory-v1.json")).unwrap();
    let expected: BTreeSet<_> = inventory["types"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, entry)| entry["kind"] == "enum" && entry["mode"] == "read_write")
        .map(|(name, _)| name.as_str())
        .collect();
    let actual: BTreeSet<_> = FIXTURE["types"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(actual, TYPED_ENUMS.iter().copied().collect());
    assert_eq!(actual.len(), 71);
    assert_eq!(
        FIXTURE["types"]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_array().unwrap().len())
            .sum::<usize>(),
        298
    );
}

fn selected_wire(name: &str) -> Value {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let inventory: Value = serde_json::from_slice(
        &std::fs::read(fixtures.join("core-public-type-inventory-v1.json")).unwrap(),
    )
    .unwrap();
    let vector = &inventory["types"][name]["vectors"][0];
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(fixtures.join(vector["fixture"].as_str().unwrap())).unwrap(),
    )
    .unwrap();
    fixture
        .pointer(vector["pointer"].as_str().unwrap())
        .unwrap()
        .clone()
}
fn typed<T: DeserializeOwned>(name: &str) -> T {
    serde_json::from_value(selected_wire(name)).unwrap()
}
fn constructor_case<T: Serialize>(
    values: &mut BTreeMap<String, Value>,
    name: &str,
    tag: &str,
    value: T,
) {
    assert!(
        values
            .insert(
                format!("{name}/{tag}"),
                serde_json::to_value(value).unwrap()
            )
            .is_none()
    );
}

macro_rules! constructed {
    ($values:expr, $ty:ty, $tag:literal, $value:expr) => {
        constructor_case::<$ty>($values, stringify!($ty), $tag, $value);
    };
}

fn model_constructor_cases(values: &mut BTreeMap<String, Value>) {
    constructed!(
        values,
        ModelCapabilityIssue,
        "output_modality",
        ModelCapabilityIssue::OutputModality {
            required: ModelModality::Image
        }
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "tool_calling",
        ModelCapabilityIssue::ToolCalling {}
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "tool_calls_per_response",
        ModelCapabilityIssue::ToolCallsPerResponse {
            required: ExecutionCount::new(2),
            available: ExecutionCount::new(1)
        }
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "tool_choice",
        ModelCapabilityIssue::ToolChoice {
            required: ModelToolChoice::Required
        }
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "strict_tool_arguments",
        ModelCapabilityIssue::StrictToolArguments {}
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "reasoning_summary",
        ModelCapabilityIssue::ReasoningSummary {}
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "context_tokens",
        ModelCapabilityIssue::ContextTokens {
            required: TokenCount::new(10),
            available: None
        }
    );
    constructed!(
        values,
        ModelCapabilityIssue,
        "input_tokens",
        ModelCapabilityIssue::InputTokens {
            required: TokenCount::new(10),
            available: Some(TokenCount::new(1))
        }
    );
    let metadata: ContentMetadata =
        serde_json::from_value(selected_wire("ModelOutputStart")["content"]["metadata"].clone())
            .unwrap();
    constructed!(
        values,
        ModelOutputStart,
        "reasoning_summary",
        ModelOutputStart::reasoning_summary(None, metadata).unwrap()
    );
}

fn control_constructor_cases(values: &mut BTreeMap<String, Value>) {
    constructed!(
        values,
        NodeStateChange,
        "unchanged",
        NodeStateChange::Unchanged
    );
    constructed!(
        values,
        OutboxAttemptStatus,
        "delivering",
        OutboxAttemptStatus::Delivering
    );
    constructed!(
        values,
        OutboxDeliveryStatus,
        "dead_letter",
        OutboxDeliveryStatus::DeadLetter
    );
    constructed!(
        values,
        OutboxDeliveryStatus,
        "delivering",
        OutboxDeliveryStatus::Delivering
    );
    constructed!(
        values,
        OutboxDeliveryStatus,
        "retry_scheduled",
        OutboxDeliveryStatus::RetryScheduled
    );
}

fn lifecycle_constructor_cases(values: &mut BTreeMap<String, Value>) {
    let observed_at: Timestamp = "2030-01-01T00:00:08.000000Z".parse().unwrap();
    constructed!(
        values,
        RunTransition,
        "wait",
        RunTransition::Wait {
            waits: typed("RunWaits")
        }
    );
    constructed!(
        values,
        RunTransition,
        "resolve_interrupt",
        RunTransition::ResolveInterrupt {
            interrupt_id: "01912345-6789-7abc-8def-0123456789ad".parse().unwrap(),
            resolved_at: observed_at
        }
    );
    constructed!(
        values,
        RunTransition,
        "fire_timer",
        RunTransition::FireTimer {
            timer_id: "01912345-6789-7abc-8def-0123456789ae".parse().unwrap(),
            fired_at: observed_at
        }
    );
    constructed!(
        values,
        RunTransition,
        "request_cancellation",
        RunTransition::RequestCancellation {
            request: typed("RunCancellationRequest")
        }
    );
    constructed!(
        values,
        RunTransition,
        "confirm_cancellation",
        RunTransition::ConfirmCancellation {
            completed_at: observed_at,
            usage: BudgetUsage::zero()
        }
    );
    constructed!(
        values,
        RunTransition,
        "succeed",
        RunTransition::Succeed {
            result: typed("AgentResult")
        }
    );
    constructed!(
        values,
        RunTransition,
        "fail",
        RunTransition::Fail {
            failure: typed("RunFailure")
        }
    );
}

fn tool_constructor_cases(values: &mut BTreeMap<String, Value>) {
    let failure: ToolError = typed("ToolError");
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/core-tool-runtime-v1.json")).unwrap();
    let unknown: ToolError = serde_json::from_value(fixture["errors"]["valid"][1].clone()).unwrap();
    constructed!(
        values,
        ToolInvocationState,
        "failed",
        ToolInvocationState::Failed {
            error: failure.clone()
        }
    );
    constructed!(
        values,
        ToolInvocationState,
        "unknown",
        ToolInvocationState::Unknown {
            error: unknown.clone()
        }
    );
    constructed!(
        values,
        ToolInvocationTransition,
        "record_error",
        ToolInvocationTransition::RecordError {
            error: failure.clone()
        }
    );
    constructed!(
        values,
        ToolInvocationTransition,
        "reconcile_result",
        ToolInvocationTransition::ReconcileResult {
            result: typed("ToolResult")
        }
    );
    constructed!(
        values,
        ToolInvocationTransition,
        "reconcile_error",
        ToolInvocationTransition::ReconcileError { error: failure }
    );
    constructed!(
        values,
        ToolInvocationTransitionKind,
        "reconcile_error",
        ToolInvocationTransitionKind::ReconcileError
    );
    constructed!(
        values,
        ToolInvocationTransitionKind,
        "reconcile_result",
        ToolInvocationTransitionKind::ReconcileResult
    );
}

#[test]
fn newly_frozen_alternatives_match_their_public_constructors() {
    let mut values = BTreeMap::new();
    model_constructor_cases(&mut values);
    control_constructor_cases(&mut values);
    lifecycle_constructor_cases(&mut values);
    tool_constructor_cases(&mut values);
    assert_eq!(values.len(), 28);
    let mut matched = BTreeSet::new();
    for vectors in FIXTURE["types"].as_object().unwrap().values() {
        for vector in vectors.as_array().unwrap() {
            if let Some(key) = vector["source"]["constructor_case"].as_str() {
                assert_eq!(&values[key], &vector["wire"], "constructor drift: {key}");
                assert!(matched.insert(key));
            }
        }
    }
    assert_eq!(matched, values.keys().map(String::as_str).collect());
}
