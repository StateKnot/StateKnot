// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded qualification oracles shared by libFuzzer and seed regression tests.

#![forbid(unsafe_code)]

use jsonschema::Validator;
use schemars::{JsonSchema, generate::SchemaSettings};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::*;
use stateknot_runtime::{
    JsonSchemaRegistryBuilder, JsonSchemaRegistryError, JsonSchemaRegistryLimits,
};
use std::sync::LazyLock;

/// The harness checks this before parsing, independently of libFuzzer flags.
pub const MAX_INPUT_BYTES: usize = 128 * 1024;

/// Checks strict parsing, resource statistics, compact round trips and RFC 8785.
pub fn check_bounded_json(data: &[u8]) {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    for limits in [
        JsonLimits::try_new(128, 4, 8, 32, 32, 8).unwrap(),
        JsonLimits::DEFAULT,
        JsonLimits::MAXIMUM,
    ] {
        let Ok(value) = BoundedJson::from_slice_with_limits(data, limits) else {
            continue;
        };
        let compact = serde_json::to_vec(&value).unwrap();
        assert_eq!(value.stats().compact_bytes(), compact.len());
        assert!(value.stats().compact_bytes() <= limits.max_bytes());
        assert!(value.stats().max_depth() <= limits.max_depth());
        assert!(value.stats().nodes() <= limits.max_nodes());
        let restored = BoundedJson::from_slice_with_limits(&compact, limits).unwrap();
        assert_eq!(value, restored);
        assert_eq!(value.stats(), restored.stats());
        if let Ok(canonical) = CanonicalJson::new(&value) {
            assert_eq!(canonical.digest(), Digest::sha256(canonical.as_bytes()));
            let restored =
                BoundedJson::from_slice_with_limits(canonical.as_bytes(), JsonLimits::MAXIMUM)
                    .unwrap();
            // JCS can change a float token into an integer token outside the
            // safe-integer input subset. Check canonical bytes rather than
            // imposing a second CanonicalJson input classification on it.
            assert_eq!(
                canonical.as_bytes(),
                serde_json_canonicalizer::to_vec(restored.as_value()).unwrap()
            );
        }
    }
}

struct ReaderSchemas {
    input: Validator,
    output: Validator,
}

fn reader<T: DeserializeOwned + Serialize>(
    data: &[u8],
    input: &Value,
    schemas: &ReaderSchemas,
) -> bool {
    let Ok(typed) = serde_json::from_slice::<T>(data) else {
        return false;
    };
    // Check after the real reader: validating first would hide unsupported
    // representations that Serde accepts despite the declared input contract.
    assert!(
        schemas.input.is_valid(input),
        "{} reader/input-schema drift",
        std::any::type_name::<T>()
    );
    let output = serde_json::to_vec(&typed).unwrap();
    let bounded = BoundedJson::from_slice_with_limits(&output, JsonLimits::MAXIMUM).unwrap();
    assert!(
        schemas.output.is_valid(bounded.as_value()),
        "{} producer/schema drift",
        std::any::type_name::<T>()
    );
    let restored: T = serde_json::from_slice(&output).expect("producer/reader drift");
    assert_eq!(output, serde_json::to_vec(&restored).unwrap());
    if let Ok(canonical) = CanonicalJson::new(&bounded) {
        let restored: T =
            serde_json::from_slice(canonical.as_bytes()).expect("canonical/reader drift");
        let restored = BoundedJson::try_from_value_with_limits(
            serde_json::to_value(restored).unwrap(),
            JsonLimits::MAXIMUM,
        )
        .unwrap();
        assert_eq!(
            canonical.as_bytes(),
            serde_json_canonicalizer::to_vec(restored.as_value()).unwrap()
        );
    }
    true
}

fn input_validator<T: JsonSchema>() -> Validator {
    let schema = SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator()
        .into_root_schema_for::<T>();
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .offline()
        .build(schema.as_value())
        .expect("generated input schema must compile offline")
}

fn output_validator<T: JsonSchema>() -> Validator {
    let schema = SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<T>();
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .offline()
        .build(schema.as_value())
        .expect("generated producer schema must compile offline")
}

macro_rules! readers {
    ($( $test:ident: $ty:ty ),+ $(,)?) => {
        /// The same closed type list used by the permanent Core wire inventory.
        pub const READERS: &[&str] = &[$(stringify!($ty)),+];
        $(fn $test(data: &[u8], input: &Value) -> bool {
            static SCHEMAS: LazyLock<ReaderSchemas> = LazyLock::new(|| ReaderSchemas {
                input: input_validator::<$ty>(),
                output: output_validator::<$ty>(),
            });
            reader::<$ty>(data, input, &SCHEMAS)
        })+
        fn dispatch(name: &str, data: &[u8], input: &Value) -> bool {
            match name {
                $(stringify!($ty) => $test(data, input),)+
                _ => false,
            }
        }
    };
}

include!("../../crates/stateknot-core/tests/support/public_readers.rs");

/// Checks a stable `RustTypeName\nraw JSON` frame; type additions don't renumber
/// retained reproducers. The strict bounded JSON gate runs before typed Serde.
pub fn core_readers(data: &[u8]) -> bool {
    if data.len() > MAX_INPUT_BYTES {
        return false;
    }
    let Some(separator) = data.iter().position(|b| *b == b'\n') else {
        return false;
    };
    let Ok(name) = std::str::from_utf8(&data[..separator]) else {
        return false;
    };
    let input = &data[separator + 1..];
    let Ok(bounded) = BoundedJson::from_slice(input) else {
        return false;
    };
    dispatch(name, input, bounded.as_value())
}

const SCHEMA_ID: &str = "https://stateknot.github.io/schema/fuzz/registry/1.0.0";
const SECOND_ID: &str = "https://stateknot.github.io/schema/fuzz/registry/second/1.0.0";

/// Exercises the real offline registry, pin/URI rejection, failed-registration
/// atomicity, duplicate identity, byte/count limits and bounded validation.
pub fn schema_registry(data: &[u8]) -> bool {
    if data.len() > MAX_INPUT_BYTES {
        return false;
    }
    let limits = JsonLimits::try_new(MAX_INPUT_BYTES, 16, 128, 2048, 64 * 1024, 128).unwrap();
    let Ok(envelope) = BoundedJson::from_slice_with_limits(data, limits) else {
        return false;
    };
    let Some(mut document) = envelope.as_value().get("schema").cloned() else {
        return false;
    };
    let Some(instance) = envelope.as_value().get("instance").cloned() else {
        return false;
    };
    let instance = BoundedJson::try_from_value_with_limits(instance, limits).unwrap();
    let bad_pin = envelope.as_value().get("bad_pin") == Some(&Value::Bool(true));
    if let Some(object) = document.as_object_mut() {
        object.entry("$schema").or_insert_with(|| {
            Value::String("https://json-schema.org/draft/2020-12/schema".into())
        });
        object
            .entry("$id")
            .or_insert_with(|| Value::String(SCHEMA_ID.into()));
    }
    let Ok(canonical) = serde_json_canonicalizer::to_vec(&document) else {
        return false;
    };
    let reference = SchemaReference::new(
        SCHEMA_ID.parse().unwrap(),
        Version::new(1, 0, 0),
        if bad_pin {
            Digest::sha256(b"wrong pin")
        } else {
            Digest::sha256(&canonical)
        },
    );
    let ceilings = JsonSchemaRegistryLimits::new(2, 32 * 1024, 48 * 1024).unwrap();
    let mut builder = JsonSchemaRegistryBuilder::new(ceilings);
    let registered = builder.register(reference.clone(), document.clone());
    if bad_pin || registered.is_err() {
        assert!(registered.is_err());
        assert!(matches!(
            builder.build(),
            Err(JsonSchemaRegistryError::Empty)
        ));
        return false;
    }
    assert!(
        builder
            .register(reference.clone(), document.clone())
            .is_err()
    );
    let wrong_reference = SchemaReference::new(
        reference.id().clone(),
        reference.version(),
        Digest::sha256(b"absent pin"),
    );
    assert!(
        builder
            .register(wrong_reference.clone(), document.clone())
            .is_err()
    );
    let mut second = document.clone();
    second["$id"] = Value::String(SECOND_ID.into());
    let second_bytes = serde_json_canonicalizer::to_vec(&second).unwrap();
    let second_reference = SchemaReference::new(
        SECOND_ID.parse().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(&second_bytes),
    );
    let second_registered = builder.register(second_reference.clone(), second).is_ok();
    assert!(builder.register(reference.clone(), document).is_err());
    let Ok(registry) = builder.build() else {
        // A meta-valid document can still contain unresolved/offline references
        // or an invalid regular expression. Compilation must reject it.
        return false;
    };
    assert_eq!(registry.len(), 1 + usize::from(second_registered));
    let expected_bytes = canonical.len()
        + if second_registered {
            second_bytes.len()
        } else {
            0
        };
    assert_eq!(registry.total_bytes(), expected_bytes);
    assert!(expected_bytes <= ceilings.maximum_total_bytes());
    assert_eq!(
        registry.canonical_bytes(&reference),
        Some(canonical.as_slice())
    );
    assert_eq!(registry.contains(&second_reference), second_registered);
    assert_eq!(registry.canonical_bytes(&wrong_reference), None);
    assert_eq!(
        registry.validate_bounded(&wrong_reference, &instance),
        Err(GraphSchemaValidationError::Unavailable)
    );
    let result = registry.validate_bounded(&reference, &instance);
    let compact = serde_json::to_vec(&instance).unwrap();
    let restored = BoundedJson::from_slice_with_limits(&compact, limits).unwrap();
    assert_eq!(result, registry.validate_bounded(&reference, &restored));
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, path::Path};

    #[test]
    fn all_inventory_vectors_reach_the_matching_reader() {
        let fixtures =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates/stateknot-core/tests/fixtures");
        let inventory: Value = serde_json::from_slice(
            &std::fs::read(fixtures.join("core-public-type-inventory-v1.json")).unwrap(),
        )
        .unwrap();
        let mut names = BTreeSet::new();
        let mut failures = Vec::new();
        for (name, entry) in inventory["types"].as_object().unwrap() {
            if entry["mode"] != "read_write" {
                continue;
            }
            names.insert(name.as_str());
            for vector in entry["vectors"].as_array().unwrap() {
                let file: Value = serde_json::from_slice(
                    &std::fs::read(fixtures.join(vector["fixture"].as_str().unwrap())).unwrap(),
                )
                .unwrap();
                let wire = file.pointer(vector["pointer"].as_str().unwrap()).unwrap();
                let mut data = format!("{name}\n").into_bytes();
                data.extend(serde_json::to_vec(wire).unwrap());
                if !matches!(std::panic::catch_unwind(|| core_readers(&data)), Ok(true)) {
                    failures.push(name.clone());
                }
            }
        }
        assert_eq!(names, READERS.iter().copied().collect());
        assert!(failures.is_empty(), "reader/schema failures: {failures:?}");
    }

    #[test]
    fn every_enum_variant_reaches_its_actual_input_and_output_oracles() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../crates/stateknot-core/tests/fixtures/core-public-enum-variants-v1.json"
        ))
        .unwrap();
        let mut tested = 0;
        for (name, vectors) in fixture["types"].as_object().unwrap() {
            assert!(READERS.contains(&name.as_str()));
            for vector in vectors.as_array().unwrap() {
                let mut frame = format!("{name}\n").into_bytes();
                frame.extend(serde_json::to_vec(&vector["wire"]).unwrap());
                assert!(core_readers(&frame), "{name} {}", vector["case"]);
                tested += 1;
            }
        }
        assert_eq!(fixture["types"].as_object().unwrap().len(), 71);
        assert_eq!(tested, 298);
    }

    #[test]
    fn scoped_frame_substitution_and_journal_order_reproducers_are_rejected() {
        for data in [
            include_bytes!("../seeds/core_readers/GraphFrameIdentity-invalid-ancestor").as_slice(),
            include_bytes!("../seeds/core_readers/GraphFrameCheckpointHead-before-origin")
                .as_slice(),
            include_bytes!("../seeds/core_readers/GraphFrameCheckpoint-sibling-digest").as_slice(),
            include_bytes!("../seeds/core_readers/GraphFrameCheckpoint-duplicate-frame").as_slice(),
        ] {
            assert!(!core_readers(data));
        }
    }

    #[test]
    fn strict_json_and_numeric_normalization_controls() {
        for input in [
            b"null".as_slice(),
            b"0.0",
            b"-0",
            b"10000000000000000.0",
            br#"{"k":"\uD83D\uDE00"}"#,
        ] {
            check_bounded_json(input);
        }
        for input in [
            br#"{"k":0,"k":1}"#.as_slice(),
            br#""\uD800""#,
            b"[",
            b"null true",
            &[0xff],
        ] {
            assert!(BoundedJson::from_slice(input).is_err());
            check_bounded_json(input);
        }
    }

    #[test]
    fn registry_success_rejection_and_pin_controls() {
        assert!(schema_registry(
            br#"{"schema":{"type":"string"},"instance":"ok"}"#
        ));
        assert!(schema_registry(
            br#"{"schema":{"type":"integer"},"instance":"bad"}"#
        ));
        assert!(!schema_registry(
            br#"{"schema":{"type":"string"},"instance":"ok","bad_pin":true}"#
        ));
        assert!(!schema_registry(
            br#"{"schema":{"type":42},"instance":null}"#
        ));
        assert!(!schema_registry(
            br#"{"schema":{"$ref":"https://untrusted.invalid/a"},"instance":null}"#
        ));
    }

    #[test]
    fn retained_reader_variants_reach_their_real_producers() {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds/core_readers");
        let mut tested = 0;
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            if name.starts_with("Failure-")
                || name.starts_with("ToolError-")
                || name.starts_with("CapabilityLifecycle-")
            {
                assert!(
                    core_readers(&std::fs::read(entry.path()).unwrap()),
                    "{name}"
                );
                tested += 1;
            }
        }
        assert_eq!(tested, 11);
    }

    #[test]
    fn retained_malformed_timestamp_is_rejected_by_the_real_reader() {
        assert!(!core_readers(include_bytes!(
            "../seeds/core_readers/RunTransition-invalid-timestamp"
        )));
    }

    #[test]
    fn retained_positional_objects_are_rejected_by_the_real_readers() {
        for seed in [
            include_bytes!("../seeds/core_readers/SchemaReference-positional").as_slice(),
            include_bytes!("../seeds/core_readers/ToolInput-positional-schema").as_slice(),
            include_bytes!("../seeds/core_readers/ChildRunAdmissionIntent-positional-schema")
                .as_slice(),
        ] {
            assert!(!core_readers(seed));
        }
    }
}
