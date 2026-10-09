// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Print the current output-profile inventory for explicit review; never bless
//! or rewrite a checked-in fixture as part of tests or qualification.

use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::json;
use stateknot_core::*;
use std::collections::BTreeMap;

fn pin<T: JsonSchema>() -> Digest {
    let schema = SchemaSettings::draft2020_12()
        .for_serialize()
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
        fn pins() -> BTreeMap<&'static str, Digest> {
            BTreeMap::from([$( (stringify!($ty), pin::<$ty>()),)+])
        }
    };
}
include!("../../crates/stateknot-core/tests/support/public_readers.rs");

fn main() {
    let mut types = pins();
    assert!(
        types
            .insert("BudgetRemaining", pin::<BudgetRemaining>())
            .is_none()
    );
    assert_eq!(types.len(), 314);
    println!("{}", serde_json::to_string_pretty(&json!({
        "schema": "https://stateknot.github.io/schema/test-fixture/core-public-output-schema-inventory/1.0.0",
        "contract": "serialize",
        "draft": "2020-12",
        "types": types
    })).unwrap());
}
