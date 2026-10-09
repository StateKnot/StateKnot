// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Independent tree models for nested JSON, canonical bytes and extension limits.

use proptest::{collection, prelude::*};
use serde_json::Value;
use stateknot_core::{
    BoundedJson, BoundedJsonError, CanonicalJson, Digest, ExtensionKey, ExtensionLimits,
    ExtensionValue, Extensions, JsonLimits, SchemaReference, Version,
};

fn text() -> impl Strategy<Value = String> {
    collection::vec(any::<char>(), 0..24).prop_map(|chars| chars.into_iter().collect())
}

fn tree() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i32>().prop_map(|value| Value::Number(value.into())),
        text().prop_map(Value::String),
    ]
    .prop_recursive(6, 96, 8, |inner| {
        prop_oneof![
            collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
            collection::btree_map(text(), inner, 0..8)
                .prop_map(|entries| Value::Object(entries.into_iter().collect())),
        ]
    })
}

// Independent structural accounting: punctuation/scalar encodings, container
// depth, entries, value nodes (excluding keys), decoded string and key bytes.
fn dimensions(value: &Value) -> [usize; 6] {
    let mut measured = [0; 6];
    let mut pending = vec![(value, 0)];
    while let Some((value, parent_depth)) = pending.pop() {
        measured[3] += 1;
        match value {
            Value::Null => measured[0] += 4,
            Value::Bool(value) => measured[0] += if *value { 4 } else { 5 },
            Value::Number(value) => measured[0] += value.to_string().len(),
            Value::String(value) => {
                measured[0] += serde_json::to_string(value).unwrap().len();
                measured[4] = measured[4].max(value.len());
            }
            Value::Array(values) => {
                let depth = parent_depth + 1;
                measured[0] += 2 + values.len().saturating_sub(1);
                measured[1] = measured[1].max(depth);
                measured[2] = measured[2].max(values.len());
                pending.extend(values.iter().map(|value| (value, depth)));
            }
            Value::Object(values) => {
                let depth = parent_depth + 1;
                measured[0] += 2 + values.len().saturating_sub(1);
                measured[1] = measured[1].max(depth);
                measured[2] = measured[2].max(values.len());
                for (key, value) in values {
                    measured[0] += serde_json::to_string(key).unwrap().len() + 1;
                    measured[5] = measured[5].max(key.len());
                    pending.push((value, depth));
                }
            }
        }
    }
    measured
}

fn limits([bytes, depth, entries, nodes, string, key]: [usize; 6]) -> JsonLimits {
    JsonLimits::try_new(bytes, depth, entries, nodes, string, key).unwrap()
}

fn canonical_reference(value: &Value) -> String {
    match value {
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_reference)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            let fields = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        canonical_reference(value)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", fields.join(","))
        }
        // This strategy uses interoperable integer leaves, so no float
        // normalization or unsafe-integer classification is assumed here.
        _ => serde_json::to_string(value).unwrap(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn nested_tree_statistics_and_jcs_match_independent_models(value in tree()) {
        let measured = dimensions(&value);
        let encoded = serde_json::to_vec(&value).unwrap();
        prop_assert_eq!(measured[0], encoded.len());
        let exact = limits(measured.map(|value| value.max(1)));
        let bounded = BoundedJson::from_slice_with_limits(&encoded, exact).unwrap();
        let materialized = BoundedJson::try_from_value_with_limits(value.clone(), exact).unwrap();
        prop_assert_eq!(&bounded, &materialized);
        prop_assert_eq!(bounded.stats(), materialized.stats());
        prop_assert_eq!(bounded.stats().compact_bytes(), measured[0]);
        prop_assert_eq!(bounded.stats().max_depth(), measured[1]);
        prop_assert_eq!(bounded.stats().nodes(), measured[3]);
        let expected = canonical_reference(&value);
        let canonical = CanonicalJson::new(&bounded).unwrap();
        prop_assert_eq!(canonical.as_str(), expected.as_str());
        prop_assert_eq!(canonical.digest(), Digest::sha256(expected.as_bytes()));
        let restored: BoundedJson = serde_json::from_slice(canonical.as_bytes()).unwrap();
        prop_assert_eq!(restored.as_value(), &value);
        prop_assert_eq!(CanonicalJson::new(&restored).unwrap(), canonical);
    }

    #[test]
    fn all_six_narrowed_dimensions_match_an_independent_acceptance_model(
        value in tree(),
        profile in (1..16385usize, 1..9usize, 1..9usize, 1..257usize, 1..97usize, 1..97usize),
    ) {
        let (bytes, depth, entries, nodes, string, key) = profile;
        let ceilings = [bytes, depth, entries, nodes, string, key];
        let measured = dimensions(&value);
        let accepted = measured.iter().zip(ceilings).all(|(actual, ceiling)| *actual <= ceiling);
        let configured = limits(ceilings);
        let encoded = serde_json::to_vec(&value).unwrap();
        prop_assert_eq!(BoundedJson::from_slice_with_limits(&encoded, configured).is_ok(), accepted);
        prop_assert_eq!(BoundedJson::try_from_value_with_limits(value, configured).is_ok(), accepted);
    }

    #[test]
    fn exact_nested_limits_reject_every_one_byte_or_count_tightening(value in tree()) {
        let measured = dimensions(&value);
        let exact = measured.map(|value| value.max(1));
        let encoded = serde_json::to_vec(&value).unwrap();
        prop_assert!(BoundedJson::from_slice_with_limits(&encoded, limits(exact)).is_ok());
        for dimension in 0..6 {
            if measured[dimension] > 1 {
                let mut narrowed = exact;
                narrowed[dimension] -= 1;
                prop_assert!(BoundedJson::from_slice_with_limits(&encoded, limits(narrowed)).is_err());
                prop_assert!(BoundedJson::try_from_value_with_limits(value.clone(), limits(narrowed)).is_err());
            }
        }
        let mut padded = encoded.clone();
        padded.push(b' ');
        prop_assert_eq!(BoundedJson::from_slice_with_limits(&padded, limits(exact)), Err(BoundedJsonError::InputTooLarge {
            maximum: encoded.len(), actual: padded.len(),
        }));
    }

    #[test]
    fn extensions_restrict_previously_wider_nested_values_without_bypassing_json_limits(
        value in tree(),
        profile in (1..16385usize, 1..9usize, 1..9usize, 1..257usize, 1..97usize, 1..97usize),
        schema_bound in any::<bool>(),
    ) {
        let (bytes, depth, entries, nodes, string, key) = profile;
        let ceilings = [bytes, depth, entries, nodes, string, key];
        let accepted = dimensions(&value).iter().zip(ceilings).all(|(actual, ceiling)| *actual <= ceiling);
        let bounded = BoundedJson::try_from_value(value).unwrap();
        let extension = if schema_bound {
            ExtensionValue::schema_bound(SchemaReference::new(
                "https://schemas.example.com/nested/1.0.0".parse().unwrap(),
                Version::new(1, 0, 0), Digest::sha256(b"synthetic schema declaration"),
            ), bounded)
        } else {
            ExtensionValue::opaque(bounded)
        };
        let entry = (ExtensionKey::new("com.example.nested").unwrap(), extension);
        let configured = ExtensionLimits::try_new(1, ExtensionLimits::HARD_MAXIMUM.max_total_bytes(),
            ExtensionLimits::HARD_MAXIMUM.max_key_bytes(), limits(ceilings)).unwrap();
        prop_assert_eq!(Extensions::try_new_with_limits([entry.clone()], configured).is_ok(), accepted);
        let wide = Extensions::try_new([entry]).unwrap();
        let restricted = wide.clone().try_restrict(configured);
        prop_assert_eq!(restricted.is_ok(), accepted);
        if let Ok(restricted) = restricted { prop_assert_eq!(restricted, wide); }
    }
}
