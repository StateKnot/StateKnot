// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Print the current directional schema inventory for explicit review; never bless
//! or rewrite a checked-in fixture as part of tests or qualification.

use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::json;
use stateknot_core::*;
use std::collections::BTreeMap;

fn pin<T: JsonSchema>(deserialize: bool) -> Digest {
    let settings = SchemaSettings::draft2020_12();
    let schema = if deserialize {
        settings.for_deserialize()
    } else {
        settings.for_serialize()
    }
    .into_generator()
    .into_root_schema_for::<T>();
    let value = BoundedJson::try_from_value_with_limits(
        serde_json::to_value(schema).unwrap(),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    CanonicalJson::new(&value).unwrap().digest()
}

macro_rules! readers {
    ($( $test:ident: $ty:ty ),+ $(,)?) => {
        fn pins(deserialize: bool) -> BTreeMap<&'static str, Digest> {
            BTreeMap::from([$( (stringify!($ty), pin::<$ty>(deserialize)),)+])
        }
    };
}
include!("../../crates/stateknot-core/tests/support/public_readers.rs");

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert!(
        args.is_empty() || args == ["--deserialize"],
        "usage: schema_pins [--deserialize]"
    );
    let deserialize = !args.is_empty();
    let mut types = pins(deserialize);
    assert!(
        types
            .insert("BudgetRemaining", pin::<BudgetRemaining>(deserialize))
            .is_none()
    );
    assert_eq!(types.len(), 314);
    println!("{}", serde_json::to_string_pretty(&json!({
        "schema": if deserialize { "https://stateknot.github.io/schema/test-fixture/core-public-input-schema-candidates/1.0.0" } else { "https://stateknot.github.io/schema/test-fixture/core-public-output-schema-inventory/1.0.0" },
        "contract": if deserialize { "deserialize" } else { "serialize" },
        "draft": "2020-12",
        "types": types
    })).unwrap());
}
