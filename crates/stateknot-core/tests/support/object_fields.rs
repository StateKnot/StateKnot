// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use serde::Deserialize;
use serde_json::Value;

// Keep real serializer declaration order; Value's sorted keys would produce an
// unrelated field permutation and miss Serde's positional reader path.
pub(crate) struct ObjectFieldValues(pub(crate) Vec<Value>);

impl<'de> Deserialize<'de> for ObjectFieldValues {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> serde::de::Visitor<'de> for Fields {
            type Value = ObjectFieldValues;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object producer")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some((_, value)) = map.next_entry::<String, Value>()? {
                    fields.push(value);
                }
                Ok(ObjectFieldValues(fields))
            }
        }
        deserializer.deserialize_map(Fields)
    }
}
