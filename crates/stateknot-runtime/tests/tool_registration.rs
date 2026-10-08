// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public typed Tool boundary tested against the production offline registries.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use stateknot_core::{
    BoundedJson, BudgetUsage, CancellationSignal, Digest, DurationMillis, ResolvedBudget,
    SchemaReference, TenantId, Timestamp, Tool, ToolAdapter, ToolAdapterBuildError, ToolContext,
    ToolDescriptor, ToolError, ToolInput, ToolOutput, ToolSchemaRole, Version,
};
use stateknot_runtime::{
    JsonSchemaRegistry, JsonSchemaRegistryBuilder, JsonSchemaRegistryError,
    ToolProviderRegistryBuilder, ToolProviderRegistryError,
};

#[derive(Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectionalInput {
    #[serde(rename(deserialize = "incoming", serialize = "outgoing"))]
    value: String,
    #[serde(skip_deserializing)]
    local: String,
}

#[derive(Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectionalOutput {
    #[serde(rename(deserialize = "incoming", serialize = "outgoing"))]
    value: String,
    #[serde(skip_serializing)]
    // Retained only to prove that application-local state is absent on the wire.
    #[allow(dead_code)]
    local: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    optional: Option<String>,
}

struct FixtureTool<I, O> {
    descriptor: ToolDescriptor,
    calls: Arc<AtomicUsize>,
    output: fn(I) -> O,
}

impl<I, O> Tool for FixtureTool<I, O>
where
    I: DeserializeOwned + JsonSchema + Send + 'static,
    O: Serialize + JsonSchema + Send + 'static,
{
    type Input = I;
    type Output = O;

    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        _context: ToolContext,
        input: I,
    ) -> stateknot_core::BoxFuture<'_, Result<ToolOutput<O>, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = (self.output)(input);
        Box::pin(async move { Ok(ToolOutput::inline(output)) })
    }
}

fn descriptor(input: SchemaReference, output: SchemaReference) -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let base: ToolDescriptor =
        serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap();
    ToolDescriptor::new(
        base.metadata().clone(),
        input,
        output,
        base.semantics().clone(),
        base.resources().clone(),
        base.invocation().clone(),
        base.limits().clone(),
    )
    .unwrap()
}

fn registry() -> (JsonSchemaRegistry, ToolDescriptor) {
    let mut builder = JsonSchemaRegistryBuilder::default();
    let input = builder
        .register_rust_type::<DirectionalInput>(
            "https://schemas.example.com/tool/input/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    let output = builder
        .register_rust_output_type::<DirectionalOutput>(
            "https://schemas.example.com/tool/output/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    (builder.build().unwrap(), descriptor(input, output))
}

fn tool(
    descriptor: ToolDescriptor,
    calls: &Arc<AtomicUsize>,
) -> FixtureTool<DirectionalInput, DirectionalOutput> {
    FixtureTool {
        descriptor,
        calls: Arc::clone(calls),
        output: |input| {
            assert!(input.local.is_empty());
            let optional = (input.value == "with-optional").then(|| "optional".to_owned());
            DirectionalOutput {
                value: input.value,
                local: "private-local-field".into(),
                optional,
            }
        },
    }
}

fn context(descriptor: &ToolDescriptor) -> ToolContext {
    let observed_at: Timestamp = "2029-12-31T23:59:59.000000Z".parse().unwrap();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    let budget: ResolvedBudget =
        serde_json::from_value(fixture["resolved"]["valid"][0].clone()).unwrap();
    ToolContext::new(
        TenantId::new("tenant-production").unwrap(),
        "01912345-6789-7abc-8def-0123456789ae".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789af".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789ad".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789ab".parse().unwrap(),
        descriptor,
        budget.remaining(&BudgetUsage::zero(), observed_at).unwrap(),
        DurationMillis::new(30_000).unwrap(),
        observed_at,
        Instant::now(),
        CancellationSignal::never(),
    )
    .unwrap()
}

#[tokio::test]
async fn directional_schemas_register_and_dispatch_the_actual_serde_wire_shape() {
    let (schemas, descriptor) = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = ToolAdapter::new(tool(descriptor.clone(), &calls), schemas).unwrap();
    let mut providers = ToolProviderRegistryBuilder::new();
    providers.register(Arc::new(adapter)).unwrap();
    let provider = providers.build().resolve(&descriptor).unwrap();
    let context = context(&descriptor);
    let input = ToolInput::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({"incoming": "value"})).unwrap(),
    )
    .unwrap();
    let result = provider.call(context.clone(), input).await.unwrap();
    result.validate_for(&context, &descriptor).unwrap();
    assert_eq!(result.output().as_value(), &json!({"outgoing": "value"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn conditional_optional_output_is_valid_when_present() {
    let (schemas, descriptor) = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = ToolAdapter::new(tool(descriptor.clone(), &calls), schemas).unwrap();
    let input = ToolInput::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({"incoming": "with-optional"})).unwrap(),
    )
    .unwrap();
    let context = context(&descriptor);
    let result = stateknot_core::ErasedTool::call(&adapter, context.clone(), input)
        .await
        .unwrap();
    result.validate_for(&context, &descriptor).unwrap();
    assert_eq!(
        result.output().as_value(),
        &json!({"outgoing": "with-optional", "optional": "optional"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn incorrect_input_wire_shape_is_rejected_before_application_dispatch() {
    let (schemas, descriptor) = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = ToolAdapter::new(tool(descriptor.clone(), &calls), schemas).unwrap();
    let input = ToolInput::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({"outgoing": "value"})).unwrap(),
    )
    .unwrap();
    let error = stateknot_core::ErasedTool::call(&adapter, context(&descriptor), input)
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().code().as_str(),
        "stateknot.tool.input_schema_invalid"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn output_pinned_with_the_legacy_input_generator_is_rejected_before_dispatch() {
    let (schemas, correct) = registry();
    let mut builder = JsonSchemaRegistryBuilder::default();
    let input = correct.input_schema().clone();
    let input_document = serde_json::from_slice(schemas.canonical_bytes(&input).unwrap()).unwrap();
    builder.register(input.clone(), input_document).unwrap();
    let wrong_output = builder
        .register_rust_type::<DirectionalOutput>(
            correct.output_schema().id().clone(),
            correct.output_schema().version(),
        )
        .unwrap();
    assert_ne!(&wrong_output, correct.output_schema());
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        ToolAdapter::new(
            tool(descriptor(input, wrong_output), &calls),
            builder.build().unwrap()
        ),
        Err(ToolAdapterBuildError::SchemaContract {
            role: ToolSchemaRole::Output,
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn missing_or_substituted_descriptor_schema_is_rejected_before_dispatch() {
    let (schemas, correct) = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let missing = SchemaReference::new(
        correct.input_schema().id().clone(),
        correct.input_schema().version(),
        Digest::sha256(b"not-installed"),
    );
    for (input, output, role) in [
        (
            missing,
            correct.output_schema().clone(),
            ToolSchemaRole::Input,
        ),
        (
            correct.output_schema().clone(),
            correct.output_schema().clone(),
            ToolSchemaRole::Input,
        ),
        (
            correct.input_schema().clone(),
            correct.input_schema().clone(),
            ToolSchemaRole::Output,
        ),
    ] {
        match ToolAdapter::new(tool(descriptor(input, output), &calls), schemas.clone()) {
            Err(ToolAdapterBuildError::SchemaContract { role: rejected, .. }) => {
                assert_eq!(rejected, role);
            }
            _ => panic!("substituted schema registered"),
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn non_object_input_cannot_enter_the_typed_tool_registry() {
    let mut builder = JsonSchemaRegistryBuilder::default();
    let input = builder
        .register_rust_type::<String>(
            "https://schemas.example.com/tool/scalar/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = FixtureTool::<String, String> {
        descriptor: descriptor(input.clone(), input),
        calls: Arc::clone(&calls),
        output: |value| value,
    };
    assert!(matches!(
        ToolAdapter::new(tool, builder.build().unwrap()),
        Err(ToolAdapterBuildError::SchemaContract {
            role: ToolSchemaRole::Input,
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[derive(Serialize)]
struct InvalidOutput;

impl JsonSchema for InvalidOutput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "InvalidOutput".into()
    }
    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": "invalid-type"})
    }
}

#[test]
fn invalid_generated_schema_is_rejected_by_the_production_meta_schema() {
    let mut builder = JsonSchemaRegistryBuilder::default();
    assert!(matches!(
        builder.register_rust_output_type::<InvalidOutput>(
            "https://schemas.example.com/tool/invalid/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        ),
        Err(JsonSchemaRegistryError::InvalidSchema { .. })
    ));
    assert!(matches!(
        builder.build(),
        Err(JsonSchemaRegistryError::Empty)
    ));
}

#[test]
fn changed_descriptor_cannot_resolve_an_existing_executable_binding() {
    let (schemas, correct) = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = ToolAdapter::new(tool(correct.clone(), &calls), schemas).unwrap();
    let mut providers = ToolProviderRegistryBuilder::new();
    providers.register(Arc::new(adapter)).unwrap();
    let providers = providers.build();
    let changed = descriptor(
        correct.input_schema().clone(),
        correct.input_schema().clone(),
    );
    assert!(matches!(
        providers.resolve(&changed),
        Err(ToolProviderRegistryError::DescriptorMismatch { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
