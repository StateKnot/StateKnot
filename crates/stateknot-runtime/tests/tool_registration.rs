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
    ArtifactRepresentation, BoundedJson, BudgetUsage, CancellationSignal, CapabilityLifecycle,
    Digest, DurationMillis, Failure, ResolvedBudget, SchemaReference, TenantId, Timestamp, Tool,
    ToolAdapter, ToolAdapterBuildError, ToolContext, ToolDescriptor, ToolError, ToolInput,
    ToolOutput, ToolSchemaRole, Version,
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

#[derive(Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct NestedCoreInput {
    reference: SchemaReference,
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

async fn verify_core_optional_output<O>(name: &str, output: fn(DirectionalInput) -> O, cases: usize)
where
    O: Serialize + JsonSchema + Send + 'static,
{
    let mut builder = JsonSchemaRegistryBuilder::default();
    let input = builder
        .register_rust_type::<DirectionalInput>(
            "https://schemas.example.com/core/input/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    let output_reference = builder
        .register_rust_output_type::<O>(
            format!("https://schemas.example.com/core/{name}/2.0.0")
                .parse()
                .unwrap(),
            Version::new(2, 0, 0),
        )
        .unwrap();
    let schemas = builder.build().unwrap();
    let correct = descriptor(input.clone(), output_reference.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = ToolAdapter::new(
        FixtureTool::<DirectionalInput, O> {
            descriptor: correct.clone(),
            calls: Arc::clone(&calls),
            output,
        },
        schemas.clone(),
    )
    .unwrap();
    let mut providers = ToolProviderRegistryBuilder::new();
    providers.register(Arc::new(adapter)).unwrap();
    let provider = providers.build().resolve(&correct).unwrap();
    for index in 0..cases {
        let value = index.to_string();
        let expected = serde_json::to_value(output(DirectionalInput {
            value: value.clone(),
            local: String::new(),
        }))
        .unwrap();
        let context = context(&correct);
        let input = ToolInput::new(
            input.clone(),
            BoundedJson::try_from_value(json!({"incoming": value})).unwrap(),
        )
        .unwrap();
        let result = provider.call(context.clone(), input).await.unwrap();
        result.validate_for(&context, &correct).unwrap();
        assert_eq!(result.output().as_value(), &expected);
    }
    assert_eq!(calls.load(Ordering::SeqCst), cases);

    // These exact documents were generated by the already-qualified parent
    // source, not reconstructed by mutating the corrected schema in this test.
    let baseline: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-public-output-schema-inventory-v1.json"
    ))
    .unwrap();
    let mut old = baseline["regression_baseline"]["schemas"][name]["document"].clone();
    old["$id"] = json!(output_reference.id().as_str());
    let bytes = serde_json_canonicalizer::to_vec(&old).unwrap();
    let legacy = SchemaReference::new(
        output_reference.id().clone(),
        output_reference.version(),
        Digest::sha256(&bytes),
    );
    assert_ne!(legacy.digest(), output_reference.digest());
    let mut old_builder = JsonSchemaRegistryBuilder::default();
    old_builder
        .register(
            input.clone(),
            serde_json::from_slice(schemas.canonical_bytes(&input).unwrap()).unwrap(),
        )
        .unwrap();
    old_builder.register(legacy.clone(), old).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        ToolAdapter::new(
            FixtureTool::<DirectionalInput, O> {
                descriptor: descriptor(input, legacy),
                calls: Arc::clone(&calls),
                output,
            },
            old_builder.build().unwrap()
        ),
        Err(ToolAdapterBuildError::SchemaContract {
            role: ToolSchemaRole::Output,
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn core_optional_producers_dispatch_and_reject_the_actual_old_output_pins() {
    verify_core_optional_output::<Failure>(
        "Failure",
        |input| {
            let fixture: Value = serde_json::from_str(include_str!(
                "../../stateknot-core/tests/fixtures/core-failure-v1.json"
            ))
            .unwrap();
            serde_json::from_value(
                fixture["failures"]["valid"][input.value.parse::<usize>().unwrap()].clone(),
            )
            .unwrap()
        },
        3,
    )
    .await;
    verify_core_optional_output::<ToolError>(
        "ToolError",
        |input| {
            let fixture: Value = serde_json::from_str(include_str!(
                "../../stateknot-core/tests/fixtures/core-tool-runtime-v1.json"
            ))
            .unwrap();
            let index: usize = input.value.parse().unwrap();
            let error: ToolError =
                serde_json::from_value(fixture["errors"]["valid"][index.min(1)].clone()).unwrap();
            if index == 2 {
                let wire = include_str!(
                    "../../stateknot-core/tests/fixtures/core-admission-transcript-wires-v1.json"
                );
                let wire: Value = serde_json::from_str(wire).unwrap();
                error
                    .with_recovery_handle(
                        serde_json::from_value(wire["transcript"]["recovery_handle"].clone())
                            .unwrap(),
                    )
                    .unwrap()
            } else {
                error
            }
        },
        3,
    )
    .await;
    verify_core_optional_output::<CapabilityLifecycle>(
        "CapabilityLifecycle",
        |input| {
            let fixture: Value = serde_json::from_str(include_str!(
                "../../stateknot-core/tests/fixtures/core-capability-v1.json"
            ))
            .unwrap();
            let index: usize = input.value.parse().unwrap();
            let variant = match index {
                0..=2 => index,
                3 => 1,
                _ => 2,
            };
            let mut wire = fixture["lifecycles"]["valid"][variant].clone();
            if index == 3 {
                wire.as_object_mut().unwrap().remove("sunset_at");
                wire.as_object_mut().unwrap().remove("replacement");
            } else if index == 4 {
                wire["replacement"] = fixture["lifecycles"]["valid"][1]["replacement"].clone();
            }
            serde_json::from_value(wire).unwrap()
        },
        5,
    )
    .await;
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

#[tokio::test]
async fn nested_core_object_shape_agrees_with_the_real_tool_input_schema() {
    let mut builder = JsonSchemaRegistryBuilder::default();
    let input = builder
        .register_rust_type::<NestedCoreInput>(
            "https://schemas.example.com/tool/nested-input/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    let output = builder
        .register_rust_output_type::<SchemaReference>(
            "https://schemas.example.com/tool/nested-output/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    let descriptor = descriptor(input.clone(), output);
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = ToolAdapter::new(
        FixtureTool::<NestedCoreInput, SchemaReference> {
            descriptor: descriptor.clone(),
            calls: Arc::clone(&calls),
            output: |input| input.reference,
        },
        builder.build().unwrap(),
    )
    .unwrap();
    let mut providers = ToolProviderRegistryBuilder::new();
    providers.register(Arc::new(adapter)).unwrap();
    let provider = providers.build().resolve(&descriptor).unwrap();
    let reference = serde_json::to_value(&input).unwrap();
    let positional = json!({
        "reference": [reference["id"], reference["version"], reference["digest"]],
    });
    assert!(serde_json::from_value::<NestedCoreInput>(positional.clone()).is_err());
    assert!(
        serde_json::from_slice::<NestedCoreInput>(&serde_json::to_vec(&positional).unwrap())
            .is_err()
    );
    let invalid = ToolInput::new(
        input.clone(),
        BoundedJson::try_from_value(positional).unwrap(),
    )
    .unwrap();
    let error = provider
        .call(context(&descriptor), invalid)
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().code().as_str(),
        "stateknot.tool.input_schema_invalid"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let valid = ToolInput::new(
        input,
        BoundedJson::try_from_value(json!({"reference": reference})).unwrap(),
    )
    .unwrap();
    let context = context(&descriptor);
    let result = provider.call(context.clone(), valid).await.unwrap();
    result.validate_for(&context, &descriptor).unwrap();
    assert_eq!(result.output().as_value(), &reference);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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

#[tokio::test]
async fn mixed_case_media_type_dispatches_and_actual_old_input_pin_fails_at_startup() {
    let mut builder = JsonSchemaRegistryBuilder::default();
    let input = builder
        .register_rust_type::<ArtifactRepresentation>(
            "https://schemas.example.com/artifact/input/2.0.0"
                .parse()
                .unwrap(),
            Version::new(2, 0, 0),
        )
        .unwrap();
    let output = builder
        .register_rust_output_type::<String>(
            "https://schemas.example.com/artifact/output/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
        )
        .unwrap();
    let schemas = builder.build().unwrap();
    let correct = descriptor(input.clone(), output.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let make_tool = |descriptor, calls| FixtureTool::<ArtifactRepresentation, String> {
        descriptor,
        calls,
        output: |artifact| artifact.media_type().as_str().to_owned(),
    };
    let adapter = ToolAdapter::new(
        make_tool(correct.clone(), Arc::clone(&calls)),
        schemas.clone(),
    )
    .unwrap();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-artifact-v1.json"
    ))
    .unwrap();
    let mut wire = fixture["representations"]["valid"][0].clone();
    wire["media_type"] = json!("Application/PDF");
    let context = context(&correct);
    let result = stateknot_core::ErasedTool::call(
        &adapter,
        context.clone(),
        ToolInput::new(input.clone(), BoundedJson::try_from_value(wire).unwrap()).unwrap(),
    )
    .await
    .unwrap();
    result.validate_for(&context, &correct).unwrap();
    assert_eq!(result.output().as_value(), &json!("application/pdf"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Generated from the exact previous source, not fabricated by weakening
    // the corrected schema. Core also verifies the original document pin.
    let baseline: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-media-type-input-schemas-v1.json"
    ))
    .unwrap();
    let mut old = baseline["artifact_input_document"].clone();
    old["$id"] = json!(input.id().as_str());
    let legacy = SchemaReference::new(
        input.id().clone(),
        input.version(),
        Digest::sha256(&serde_json_canonicalizer::to_vec(&old).unwrap()),
    );
    assert_ne!(legacy.digest(), input.digest());
    let mut old_builder = JsonSchemaRegistryBuilder::default();
    old_builder.register(legacy.clone(), old).unwrap();
    old_builder
        .register(
            output.clone(),
            serde_json::from_slice(schemas.canonical_bytes(&output).unwrap()).unwrap(),
        )
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        ToolAdapter::new(
            make_tool(descriptor(legacy, output), Arc::clone(&calls)),
            old_builder.build().unwrap()
        ),
        Err(ToolAdapterBuildError::SchemaContract {
            role: ToolSchemaRole::Input,
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
