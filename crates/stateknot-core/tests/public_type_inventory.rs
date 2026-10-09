// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Closed root-export inventory: compiler-checked Serde boundaries, typed wire
//! round trips, and current generated schema pins. These are current-source
//! vectors, not historical N-1/N-2 records or exhaustive variant/fuzz coverage.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use stateknot_core::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::LazyLock,
};

#[path = "support/object_fields.rs"]
mod object_fields;
use object_fields::ObjectFieldValues;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    schema: String,
    types: BTreeMap<String, TypeEntry>,
    non_types: BTreeMap<String, Export>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypeEntry {
    module: String,
    kind: String,
    rust_type: String,
    mode: String,
    vectors: Vec<Vector>,
    schema_digest: Option<Digest>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Vector {
    fixture: String,
    pointer: String,
    canonical_digest: Digest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Export {
    module: String,
    kind: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputInventory {
    schema: String,
    contract: String,
    draft: String,
    types: BTreeMap<String, Digest>,
    regression_baseline: OutputRegressionBaseline,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputRegressionBaseline {
    source_commit: String,
    schemas: BTreeMap<String, OutputRegressionSchema>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputRegressionSchema {
    document: Value,
    canonical_digest: Digest,
}

static OUTPUT_INVENTORY: LazyLock<OutputInventory> = LazyLock::new(|| {
    let json = BoundedJson::from_str_with_limits(
        include_str!("fixtures/core-public-output-schema-inventory-v1.json"),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    serde_json::from_value(json.into_value()).unwrap()
});

static INVENTORY: LazyLock<Inventory> = LazyLock::new(|| {
    let json = BoundedJson::from_str_with_limits(
        include_str!("fixtures/core-public-type-inventory-v1.json"),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    serde_json::from_value(json.into_value()).unwrap()
});
static DOCUMENTS: LazyLock<BTreeMap<String, Value>> = LazyLock::new(|| {
    let mut documents = BTreeMap::new();
    for entry in INVENTORY.types.values() {
        for vector in &entry.vectors {
            assert!(
                vector.fixture.starts_with("core-")
                    && Path::new(&vector.fixture).extension() == Some(std::ffi::OsStr::new("json"))
            );
            assert_eq!(Path::new(&vector.fixture).components().count(), 1);
            documents.entry(vector.fixture.clone()).or_insert_with(|| {
                let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures")
                    .join(&vector.fixture);
                let source = std::fs::read_to_string(path).unwrap();
                assert!(source.len() <= 512 * 1024);
                BoundedJson::from_str_with_limits(&source, JsonLimits::MAXIMUM)
                    .unwrap()
                    .into_value()
            });
        }
    }
    documents
});
fn canonical(value: Value) -> CanonicalJson {
    CanonicalJson::new(
        &BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap(),
    )
    .unwrap()
}
fn schema_pin<T: JsonSchema>(name: &str) {
    let schema = serde_json::to_value(schemars::schema_for!(T)).unwrap();
    assert_eq!(
        Some(canonical(schema).digest()),
        INVENTORY.types[name].schema_digest,
        "{name} schema changed"
    );
    let output = schemars::generate::SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<T>();
    assert_eq!(
        canonical(serde_json::to_value(output).unwrap()).digest(),
        OUTPUT_INVENTORY.types[name],
        "{name} output schema changed"
    );
}
fn verify_reader<T: DeserializeOwned + Serialize + JsonSchema>(name: &str) {
    let entry = &INVENTORY.types[name];
    assert_eq!(entry.mode, "read_write");
    assert!(!entry.vectors.is_empty(), "{name} has no positive wire");
    schema_pin::<T>(name);
    for vector in &entry.vectors {
        let wire = DOCUMENTS[&vector.fixture].pointer(&vector.pointer).unwrap();
        let typed: T = serde_json::from_value(wire.clone())
            .unwrap_or_else(|e| panic!("{name}: {}{}: {e}", vector.fixture, vector.pointer));
        assert_eq!(&serde_json::to_value(&typed).unwrap(), wire, "{name}");
        let expected = canonical(wire.clone());
        assert_eq!(
            expected.digest(),
            vector.canonical_digest,
            "{name} wire pin changed"
        );
        let restored: T = serde_json::from_slice(expected.as_bytes()).unwrap();
        assert_eq!(
            canonical(serde_json::to_value(restored).unwrap()),
            expected,
            "{name}"
        );
        if wire.is_string() || wire.is_array() {
            let mut invalid = vec![Value::Null, json!(true), json!(7), json!({})];
            invalid.push(if wire.is_string() {
                json!([])
            } else {
                json!("wire")
            });
            for value in invalid {
                assert!(
                    serde_json::from_value::<T>(value).is_err(),
                    "{name} accepted an incompatible wire shape"
                );
            }
        }
        // Raw JSON maps keep duplicate keys; Value alone would discard them.
        // The bounded JSON value and extension map have intentionally open keys.
        if let Some(object) = wire
            .as_object()
            .filter(|_| name != "BoundedJson" && name != "Extensions")
        {
            let fields: ObjectFieldValues =
                serde_json::from_slice(&serde_json::to_vec(&typed).unwrap()).unwrap();
            let mut extended = fields.0.clone();
            extended.push(Value::Null);
            let truncated = fields.0[..fields.0.len().saturating_sub(1)].to_vec();
            for fields in [fields.0.clone(), Vec::new(), truncated, extended] {
                let sequence = Value::Array(fields);
                assert!(
                    serde_json::from_slice::<T>(&serde_json::to_vec(&sequence).unwrap()).is_err(),
                    "{name} accepted positional JSON text"
                );
                assert!(
                    serde_json::from_value::<T>(sequence).is_err(),
                    "{name} accepted a positional JSON value"
                );
            }
            for invalid in [Value::Null, json!(true), json!(7), json!("wire")] {
                assert!(
                    serde_json::from_value::<T>(invalid).is_err(),
                    "{name} accepted a scalar in place of an object"
                );
            }
            let mut extra = object.clone();
            assert!(
                extra
                    .insert("untrusted_authority".into(), json!(true))
                    .is_none()
            );
            assert!(
                serde_json::from_value::<T>(Value::Object(extra)).is_err(),
                "{name} accepted an unknown field"
            );
            let encoded = serde_json::to_string(wire).unwrap();
            for (key, value) in object {
                let duplicate = format!(
                    "{{{}:{},{}",
                    serde_json::to_string(key).unwrap(),
                    serde_json::to_string(value).unwrap(),
                    &encoded[1..]
                );
                assert!(
                    serde_json::from_str::<T>(&duplicate).is_err(),
                    "{name} accepted duplicate {key}"
                );
            }
        }
    }
}

macro_rules! readers {
    ($( $test:ident: $ty:ty ),+ $(,)?) => {
        const READERS: &[&str] = &[$(stringify!($ty)),+];
        $(#[test] fn $test() { verify_reader::<$ty>(stringify!($ty)); })+
    };
}
// Two possible marker implementations make inference ambiguous if the bound
// exists. This fails compilation when a reviewed Rust-only type gains Serde.
macro_rules! cannot_implement {
    ($ty:ty : $bound:path) => {{
        trait AmbiguousIfImplemented<Marker> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
        struct Implemented;
        impl<T: ?Sized + $bound> AmbiguousIfImplemented<Implemented> for T {}
        let _ = <$ty as AmbiguousIfImplemented<_>>::check;
    }};
}
macro_rules! rust_only {
    ($($ty:ty),+ $(,)?) => {
        const RUST_ONLY: &[&str] = &[$(stringify!($ty)),+];
        #[test]
        fn reviewed_rust_only_instantiations_have_no_owned_wire_impl() {
            $(
                cannot_implement!($ty: serde::Serialize);
                cannot_implement!($ty: serde::de::DeserializeOwned);
            )+
        }
    };
}

include!("support/public_readers.rs");

rust_only! {
    AgentAdmissionAuthorityError,
    AgentAdmissionBudgetLayerError,
    AgentAdmissionError,
    AgentAdmissionIntentError,
    AgentArtifactsError,
    AgentDescriptorError,
    AgentExecutionConfigError,
    AgentInstructionsError,
    AgentRequestValidationError,
    AgentResultError,
    AgentResultProvenanceError,
    AgentResultValidationError,
    AgentSubmissionKeyError,
    AgentToolsError,
    ArtifactDescriptionError,
    ArtifactNameError,
    ArtifactParentsError,
    ArtifactRefError,
    ArtifactRepresentationError,
    BarrierResultHeadsError,
    BoundedJsonError,
    BoxFuture<'static, ()>,
    BoxStream<'static, ()>,
    BudgetEvaluationError,
    BudgetNarrowingError,
    BudgetResolutionError,
    BudgetUsageBuilder,
    BudgetUsageError,
    CancellationSignal,
    CanonicalJson,
    CanonicalJsonError,
    CapabilityDescriptionError,
    CapabilityLifecycleError,
    CapabilityLifecycleState,
    CapabilityMetadataError,
    CapabilityNameError,
    CapabilityTitleError,
    CheckpointBarrierError,
    CheckpointBarrierIntegrityError,
    CheckpointError,
    CheckpointHeadError,
    CheckpointIntegrityError,
    CheckpointLineageError,
    CheckpointLineageVerifier,
    CheckpointStateError,
    CheckpointWriteError,
    ChildRunAdmissionIntentError,
    ChildRunBudgetError,
    ChildRunJoinError,
    ChildRunKeyError,
    ChildRunPolicyError,
    CostCollectionError,
    CountParseError,
    CumulativeBudgetReservationError,
    CurrencyCodeError,
    DigestAlgorithm,
    DigestError,
    DurableWaitError,
    DurationMillisError,
    ExtensionKeyError,
    ExtensionKeyKind,
    ExtensionLimit,
    ExtensionLimits,
    ExtensionLimitsError,
    ExtensionsError,
    FailureBuildError,
    FailureDetailsError,
    FailureIdentifierError,
    FailureMessageError,
    FencingEpochError,
    GeneratedIdError,
    GraphBarrierDisposition,
    GraphBarrierPlan,
    GraphBarrierPlanError,
    GraphCompileError,
    GraphFrameCompileError,
    GraphFrameError,
    GraphComposition,
    GraphCompositionError,
    GraphExecutionLimitsError,
    GraphNamespaceError,
    GraphNodeError,
    GraphReducerError,
    GraphReducerInput<'static>,
    GraphRouteError,
    GraphRoutesError,
    GraphSchemaValidationError,
    GraphSubgraphCall,
    GraphValueKind,
    InstructionError,
    InstructionNameError,
    IssuerIdError,
    JournalAppendError,
    JournalAuthorityError,
    JournalChainError,
    JournalChainVerifier,
    JournalEventError,
    JournalEventKindError,
    JournalIntegrityError,
    JournalIntentError,
    JournalPayloadError,
    JournalSequenceError,
    JsonLimit,
    JsonLimits,
    JsonLimitsError,
    JsonStats,
    LanguageTagError,
    MediaTypeError,
    MessageError,
    MessagePartsError,
    ModelCapabilitiesError,
    ModelCapabilityMismatchError,
    ModelContext,
    ModelContextError,
    ModelDescriptorError,
    ModelErrorValidationError,
    ModelEventAccumulator<'static>,
    ModelEventError,
    ModelEventStreamError,
    ModelInvocationError,
    ModelInvocationHeadError,
    ModelInvocationHistoryError,
    ModelInvocationHistoryVerifier,
    ModelInvocationIntegrityError,
    ModelInvocationIntentError,
    ModelInvocationRevisionError,
    ModelModalitiesError,
    ModelOutputItemError,
    ModelOutputItemKind,
    ModelProviderIdentifierError,
    ModelProviderReplayError,
    ModelProviderReplayFormatError,
    ModelRequestBuilder,
    ModelRequestError,
    ModelRequestLimitsError,
    ModelRequirementsError,
    ModelResponseError,
    ModelStopReason,
    ModelStreamChunkError,
    ModelStructuredOutputCapabilitiesError,
    ModelTokenLimitsError,
    ModelToolCallProposalError,
    ModelToolCapabilitiesError,
    ModelToolChoicesError,
    ModelToolFailureError,
    ModelToolRequirementsError,
    ModelTranscriptError,
    ModelTranscriptTurnError,
    ModelUsageError,
    MoneyArithmeticError,
    NodeActivationError,
    NodeAttemptError,
    NodeAttemptHistoryError,
    NodeAttemptHistoryVerifier,
    NodeAttemptIntegrityError,
    NodeDispatchReason,
    NodeIdError,
    NodeInvocationBindingError,
    NodeInvocationBindingsError,
    NodeStateUpdateError,
    NodeTerminalOutputError,
    NodeWaitsError,
    OutboxAttemptError,
    OutboxAttemptHistoryError,
    OutboxAttemptHistoryVerifier,
    OutboxAttemptIntegrityError,
    OutboxDeliveryError,
    OutboxDeliveryIntegrityError,
    PendingNodeResultError,
    PendingNodeResultIntegrityError,
    PendingNodeResultIntentError,
    ReadyNodeRecoveryError,
    ReadyNodeRecoveryPlan,
    ReadyNodeRecoveryPlanner,
    ReadyNodesError,
    RecoveryNode,
    RecoveryNodeKind,
    RetentionClassError,
    RouteIdError,
    RunCancellationError,
    RunFailureError,
    RunInterruptError,
    RunLeaseError,
    RunLeaseValidationError,
    RunLifecycleError,
    RunTimerError,
    RunTransitionError,
    RunWaitsError,
    SchedulerShardIdError,
    SchemaIdError,
    ScopeError,
    ScopeSetError,
    SecurityLabelError,
    SharedStateSubgraph,
    SkillActingWindowError,
    SkillActivationStoreError,
    SkillActivationStoreFailure,
    SkillAuthorizationSubjectError,
    SubjectIdError,
    SuperstepError,
    TenantIdError,
    TextContentError,
    TimestampError,
    ToolAdapter<(), ()>,
    ToolAdapterBuildError,
    ToolArtifactBinding,
    ToolArtifactsError,
    ToolAuthorizationReceiptError,
    ToolAuthorizationReceiptSinkError,
    ToolAuthorizationReceiptSinkFailure,
    ToolContext,
    ToolContextBindingError,
    ToolContextError,
    ToolDescriptorError,
    ToolErrorBuildError,
    ToolErrorValidationError,
    ToolExecutionLimitsError,
    ToolExecutionSemanticsError,
    ToolIdempotencyKey,
    ToolInputError,
    ToolInputValidationError,
    ToolInvocationError,
    ToolInvocationHeadError,
    ToolInvocationHistoryError,
    ToolInvocationHistoryVerifier,
    ToolInvocationIntegrityError,
    ToolInvocationIntentError,
    ToolInvocationRevisionError,
    ToolOutput<()>,
    ToolProgressError,
    ToolProgressEventValidationError,
    ToolProgressReporter,
    ToolProgressSinkError,
    ToolProgressUpdateError,
    ToolReconciliationContext,
    ToolReconciliationContextBindingError,
    ToolReconciliationContextError,
    ToolReconciliationObservation,
    ToolReconciliationObservationError,
    ToolReconciliationProbeError,
    ToolReconciliationProbeErrorBuildError,
    ToolRecoveryHandleError,
    ToolResultValidationError,
    ToolSchemaRole,
    ToolSchemaValidationError,
    ToolStopReason,
    VersionComponent,
    VersionError,
}

#[test]
fn output_only_types_preserve_producer_wires_and_have_no_owned_reader() {
    fn serializable<T: Serialize>() {}
    serializable::<BudgetRemaining>();
    serializable::<GraphNodeSource>();
    cannot_implement!(BudgetRemaining: serde::de::DeserializeOwned);
    cannot_implement!(GraphNodeSource: serde::de::DeserializeOwned);
    schema_pin::<BudgetRemaining>("BudgetRemaining");
    for name in ["BudgetRemaining", "GraphNodeSource"] {
        let entry = &INVENTORY.types[name];
        assert_eq!(entry.mode, "write_only");
        assert!(!entry.vectors.is_empty());
        for vector in &entry.vectors {
            let wire = DOCUMENTS[&vector.fixture].pointer(&vector.pointer).unwrap();
            assert_eq!(canonical(wire.clone()).digest(), vector.canonical_digest);
        }
    }
    // Real constructors are compared with these wires by reservation and
    // graph-composition unit tests, without inventing a deserialization API.
    assert!(INVENTORY.types["GraphNodeSource"].schema_digest.is_none());
}

fn identifier(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !text.as_bytes()[0].is_ascii_digit()
}
fn root_exports(source: &str) -> Result<BTreeMap<String, String>, &'static str> {
    let mut exports = BTreeMap::new();
    let mut declaration = String::new();
    for line in source.lines().map(str::trim) {
        if declaration.is_empty() {
            if !line.starts_with("pub ") {
                continue;
            }
            if !line.starts_with("pub use ") {
                return Err("unclassified direct public declaration");
            }
        }
        declaration.push_str(line);
        if !line.ends_with(';') {
            continue;
        }
        let body = declaration
            .strip_prefix("pub use ")
            .ok_or("invalid export prefix")?;
        let (module, names) = body
            .split_once("::{")
            .ok_or("unsupported public export syntax")?;
        if !identifier(module) {
            return Err("unsupported public module path");
        }
        let names = names
            .strip_suffix("};")
            .ok_or("unsupported public export suffix")?;
        for name in names.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            if !identifier(name) {
                return Err("public alias or glob needs explicit inventory review");
            }
            if exports.insert(name.into(), module.into()).is_some() {
                return Err("duplicate root export");
            }
        }
        declaration.clear();
    }
    if !declaration.is_empty() {
        return Err("unfinished public export");
    }
    Ok(exports)
}

#[test]
fn every_named_root_export_has_exactly_one_reviewed_classification() {
    assert_eq!(
        INVENTORY.schema,
        "https://stateknot.github.io/schema/test-fixture/core-public-type-inventory/1.0.0"
    );
    assert_eq!(
        OUTPUT_INVENTORY.schema,
        "https://stateknot.github.io/schema/test-fixture/core-public-output-schema-inventory/1.0.0"
    );
    assert_eq!(OUTPUT_INVENTORY.contract, "serialize");
    assert_eq!(OUTPUT_INVENTORY.draft, "2020-12");
    assert_eq!(
        OUTPUT_INVENTORY.regression_baseline.source_commit,
        "83802cb3202bf9cb860c6357a94abc80408b1f88"
    );
    assert_eq!(
        OUTPUT_INVENTORY
            .regression_baseline
            .schemas
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["CapabilityLifecycle", "Failure", "ToolError"])
    );
    for schema in OUTPUT_INVENTORY.regression_baseline.schemas.values() {
        assert_eq!(
            canonical(schema.document.clone()).digest(),
            schema.canonical_digest
        );
    }
    assert_eq!(
        OUTPUT_INVENTORY
            .types
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        READERS.iter().copied().chain(["BudgetRemaining"]).collect()
    );
    let actual = root_exports(include_str!("../src/lib.rs")).unwrap();
    let mut expected = BTreeMap::new();
    let mut modes = BTreeMap::<&str, usize>::new();
    for (name, entry) in &INVENTORY.types {
        assert!(matches!(
            entry.kind.as_str(),
            "struct" | "enum" | "type" | "macro_or_reexport"
        ));
        assert!(
            expected
                .insert(name.clone(), entry.module.clone())
                .is_none()
        );
        *modes.entry(entry.mode.as_str()).or_default() += 1;
        let groups = match entry.mode.as_str() {
            "read_write" => READERS,
            "write_only" => &["BudgetRemaining", "GraphNodeSource"],
            "none" => RUST_ONLY,
            other => panic!("unreviewed Serde classification {other}"),
        };
        assert!(
            groups
                .iter()
                .any(|ty| ty.split_whitespace().collect::<String>()
                    == entry.rust_type.split_whitespace().collect::<String>()),
            "{name} lacks compiler evidence"
        );
        if entry.mode == "none" {
            assert!(entry.vectors.is_empty() && entry.schema_digest.is_none());
        }
    }
    for (name, entry) in &INVENTORY.non_types {
        assert!(matches!(entry.kind.as_str(), "trait" | "const"));
        assert!(
            expected
                .insert(name.clone(), entry.module.clone())
                .is_none()
        );
    }
    assert_eq!(
        actual, expected,
        "root API changed: classify new exports and add typed evidence"
    );
    assert_eq!(
        modes,
        BTreeMap::from([("none", 248), ("read_write", 312), ("write_only", 2)])
    );
    let names = READERS
        .iter()
        .chain(RUST_ONLY)
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), READERS.len() + RUST_ONLY.len());
    assert_eq!(expected.len(), 577);
}

#[test]
fn export_inventory_parser_fails_closed_on_unreviewed_syntax() {
    assert_eq!(
        root_exports("pub use foo::{Existing, NewType};").unwrap(),
        BTreeMap::from([
            ("Existing".into(), "foo".into()),
            ("NewType".into(), "foo".into())
        ])
    );
    for source in [
        "pub mod new_module;",
        "pub struct NewType;",
        "pub use foo::*;",
        "pub use foo::{Existing as Alias};",
        "pub use foo::{Existing, Existing};",
        "pub use foo::{Existing",
    ] {
        assert!(
            root_exports(source).is_err(),
            "accepted unsupported export {source}"
        );
    }
}

#[test]
fn scalar_tags_are_frozen_from_their_own_public_enum_variants() {
    let document: Value = serde_json::from_str(include_str!(
        "fixtures/core-admission-transcript-wires-v1.json"
    ))
    .unwrap();
    assert_eq!(
        document["public_tags"],
        json!({
            "NodeAttemptStatus": NodeAttemptStatus::Succeeded,
            "OutboxAttemptStatus": OutboxAttemptStatus::Acknowledged,
            "OutboxDeliveryStatus": OutboxDeliveryStatus::Pending,
            "NodeInvocationBindingKind": NodeInvocationBindingKind::Tool,
            "ModelUsageField": ModelUsageField::InputTokens,
            "ToolInvocationStatus": ToolInvocationStatus::Prepared,
            "ModelInvocationStatus": ModelInvocationStatus::Prepared,
            "ToolInvocationTransitionKind": ToolInvocationTransitionKind::StartAttempt,
            "ModelInvocationTransitionKind": ModelInvocationTransitionKind::StartAttempt,
            "NodeControlKind": NodeControlKind::Continue
        })
    );
}
