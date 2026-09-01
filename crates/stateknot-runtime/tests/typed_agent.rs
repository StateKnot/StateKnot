// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public typed-agent construction, binding, and durable-result contract tests.

use std::sync::Arc;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};
use stateknot_core::{
    AgentArtifacts, AgentDescriptor, AgentExecutionConfig, AgentResult, AgentResultProvenance,
    AgentStructuredOutputStrategy, AgentToolConcurrency, BoundedJson, BudgetLimits, BudgetUsage,
    Digest, ExecutionCount, JsonLimits, ModelCapabilities, ModelDescriptor, ModelModalities,
    ModelModality, ModelStructuredOutputCapabilities, ModelTokenLimits, ModelToolCapabilities,
    SchemaId, SchemaReference, Timestamp, Version,
};
use stateknot_runtime::{
    AgentBuilder, JsonSchemaRegistryBuilder, TypedAgentBindError, TypedAgentInputError,
};

const CORE_AGENT_FIXTURE: &str =
    include_str!("../../stateknot-core/tests/fixtures/core-agent-v1.json");
const AGENT_RUNTIME_FIXTURE: &str =
    include_str!("../../stateknot-core/tests/fixtures/core-agent-runtime-v1.json");
const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

#[derive(Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentRequest {
    incident_id: String,
    question: String,
}

#[derive(Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentReport {
    summary: String,
    severity: String,
}

fn source_descriptor() -> AgentDescriptor {
    let fixture: Value = serde_json::from_str(CORE_AGENT_FIXTURE).unwrap();
    serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
}

fn profile(accepts_every_object: bool) -> (SchemaReference, Value) {
    let id = "https://schemas.example.com/typed-agent/provider-profile/1.0.0";
    let document = if accepts_every_object {
        json!({
            "$schema": DIALECT,
            "$id": id,
            "type": "object"
        })
    } else {
        json!({
            "$schema": DIALECT,
            "$id": id,
            "type": "object",
            "required": ["provider_extension_that_generated_schemas_do_not_have"]
        })
    };
    let canonical = serde_json_canonicalizer::to_vec(&document).unwrap();
    (
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(canonical),
        ),
        document,
    )
}

fn builder<I, O>(profile: SchemaReference) -> AgentBuilder<I, O> {
    let source = source_descriptor();
    let text = ModelModalities::try_new([ModelModality::Text]).unwrap();
    let capabilities = ModelCapabilities::new(
        text.clone(),
        text,
        true,
        ModelToolCapabilities::unsupported(),
        ModelStructuredOutputCapabilities::json_schema(profile),
        false,
        ModelTokenLimits::unknown(),
    )
    .unwrap();
    let model = ModelDescriptor::new(source.model().metadata().clone(), capabilities).unwrap();
    let execution = AgentExecutionConfig::new(
        AgentStructuredOutputStrategy::ModelNative,
        ExecutionCount::new(4),
        ExecutionCount::new(1),
        ExecutionCount::ZERO,
        AgentToolConcurrency::sequential(),
    )
    .unwrap();
    AgentBuilder::new(
        source.metadata().clone(),
        "https://schemas.example.com/typed-agent/input/1.0.0"
            .parse()
            .unwrap(),
        "https://schemas.example.com/typed-agent/output/1.0.0"
            .parse()
            .unwrap(),
        model,
        source.instructions().clone(),
        execution,
    )
}

#[test]
fn generated_schemas_are_draft_2020_12_and_digest_pinned() {
    let (profile, _) = profile(true);
    let definition = builder::<IncidentRequest, IncidentReport>(profile)
        .build()
        .unwrap();

    for (reference, document) in [
        (
            definition.descriptor().input_schema(),
            definition.input_schema_document(),
        ),
        (
            definition.descriptor().output_schema(),
            definition.output_schema_document(),
        ),
    ] {
        assert_eq!(document["$schema"], DIALECT);
        assert_eq!(document["$id"], reference.id().as_str());
        assert_eq!(reference.version(), Version::new(1, 0, 0));
        assert_eq!(
            reference.digest(),
            Digest::sha256(serde_json_canonicalizer::to_vec(document).unwrap())
        );
    }
}

#[test]
fn definition_binds_only_after_generated_and_profile_schemas_are_frozen() {
    let (profile, profile_document) = profile(true);
    let definition = builder::<IncidentRequest, IncidentReport>(profile.clone())
        .build()
        .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas.register(profile, profile_document).unwrap();
    let schemas = definition.register_schemas(schemas).unwrap();
    let agent = definition.bind(Arc::new(schemas.build().unwrap())).unwrap();

    let request = agent
        .prepare_request(
            &IncidentRequest {
                incident_id: "INC-42".to_owned(),
                question: "Summarize the evidence".to_owned(),
            },
            BudgetLimits::empty(),
        )
        .unwrap();
    assert_eq!(request.input_schema(), agent.descriptor().input_schema());
    assert_eq!(request.input().as_value()["incident_id"], "INC-42");
}

#[test]
fn binding_fails_closed_when_provider_profile_is_missing_or_rejects_schema() {
    let (missing_profile, _) = profile(true);
    let missing = builder::<IncidentRequest, IncidentReport>(missing_profile.clone())
        .build()
        .unwrap();
    let schemas = missing
        .register_schemas(JsonSchemaRegistryBuilder::default())
        .unwrap()
        .build()
        .unwrap();
    assert!(matches!(
        missing.bind(Arc::new(schemas)),
        Err(TypedAgentBindError::SchemaUnavailable { schema }) if *schema == missing_profile
    ));

    let (rejecting_profile, rejecting_document) = profile(false);
    let rejected = builder::<IncidentRequest, IncidentReport>(rejecting_profile.clone())
        .build()
        .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas
        .register(rejecting_profile.clone(), rejecting_document)
        .unwrap();
    let schemas = rejected.register_schemas(schemas).unwrap().build().unwrap();
    assert!(matches!(
        rejected.bind(Arc::new(schemas)),
        Err(TypedAgentBindError::ProfileRejected { profile, .. }) if *profile == rejecting_profile
    ));
}

struct SchemaIntegerButSerializesString;

impl Serialize for SchemaIntegerButSerializesString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("not-an-integer")
    }
}

impl JsonSchema for SchemaIntegerButSerializesString {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SchemaIntegerButSerializesString".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({ "type": "integer" })
    }
}

#[test]
fn typed_input_schema_mismatch_and_byte_exhaustion_fail_before_admission() {
    let (profile, profile_document) = profile(true);
    let mismatch = builder::<SchemaIntegerButSerializesString, IncidentReport>(profile.clone())
        .build()
        .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas
        .register(profile.clone(), profile_document.clone())
        .unwrap();
    let schemas = mismatch.register_schemas(schemas).unwrap().build().unwrap();
    let mismatch = mismatch.bind(Arc::new(schemas)).unwrap();
    assert_eq!(
        mismatch.prepare_request(&SchemaIntegerButSerializesString, BudgetLimits::empty()),
        Err(TypedAgentInputError::SchemaRejected)
    );

    let limits = JsonLimits::try_new(64, 8, 16, 32, 48, 32).unwrap();
    let bounded = builder::<IncidentRequest, IncidentReport>(profile)
        .with_input_json_limits(limits)
        .build()
        .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas
        .register(
            bounded
                .descriptor()
                .model()
                .capabilities()
                .structured_output()
                .schema_profile()
                .unwrap()
                .clone(),
            profile_document,
        )
        .unwrap();
    let schemas = bounded.register_schemas(schemas).unwrap().build().unwrap();
    let bounded = bounded.bind(Arc::new(schemas)).unwrap();
    assert_eq!(
        bounded.prepare_request(
            &IncidentRequest {
                incident_id: "I".repeat(80),
                question: "Q".to_owned(),
            },
            BudgetLimits::empty(),
        ),
        Err(TypedAgentInputError::ResourceLimit)
    );
}

#[test]
fn typed_result_decode_rechecks_durable_identity_budget_and_output_schema() {
    let (profile, profile_document) = profile(true);
    let definition = builder::<IncidentRequest, IncidentReport>(profile.clone())
        .build()
        .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas.register(profile, profile_document).unwrap();
    let schemas = definition
        .register_schemas(schemas)
        .unwrap()
        .build()
        .unwrap();
    let agent = definition.bind(Arc::new(schemas)).unwrap();
    let request = agent
        .prepare_request(
            &IncidentRequest {
                incident_id: "INC-42".to_owned(),
                question: "Summarize the evidence".to_owned(),
            },
            BudgetLimits::empty(),
        )
        .unwrap();

    let fixture: Value = serde_json::from_str(AGENT_RUNTIME_FIXTURE).unwrap();
    let base: BudgetLimits =
        serde_json::from_value(fixture["base_budget_layers"][0].clone()).unwrap();
    let observed = "2029-12-31T23:59:58.000000Z".parse::<Timestamp>().unwrap();
    let resolved = request
        .resolve_for(agent.descriptor(), &[base], observed)
        .unwrap();
    let provenance: AgentResultProvenance =
        serde_json::from_value(fixture["result_provenances"]["valid"][0].clone()).unwrap();
    let usage: BudgetUsage =
        serde_json::from_value(fixture["results"]["valid"][0]["usage"].clone()).unwrap();
    let expected = IncidentReport {
        summary: "Database latency caused the incident.".to_owned(),
        severity: "high".to_owned(),
    };
    let output = BoundedJson::from_slice(&serde_json::to_vec(&expected).unwrap()).unwrap();
    let result = AgentResult::for_invocation(
        provenance.clone(),
        "2029-12-31T23:59:59.000000Z".parse().unwrap(),
        agent.descriptor(),
        output,
        AgentArtifacts::empty(),
        usage,
    )
    .unwrap();

    assert_eq!(
        agent
            .decode_result(&result, &provenance, &request, &resolved)
            .unwrap(),
        expected
    );
}

#[test]
fn typed_agent_is_send_sync_and_contains_no_request_data_in_debug() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<stateknot_runtime::TypedAgent<IncidentRequest, IncidentReport>>();

    let (profile, profile_document) = profile(true);
    let definition = builder::<IncidentRequest, IncidentReport>(profile.clone())
        .build()
        .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas.register(profile, profile_document).unwrap();
    let schemas = definition
        .register_schemas(schemas)
        .unwrap()
        .build()
        .unwrap();
    let agent = definition.bind(Arc::new(schemas)).unwrap();
    let debug = format!("{agent:?}");
    assert!(!debug.contains("INC-42"));
    assert!(!debug.contains("Summarize the evidence"));
}
