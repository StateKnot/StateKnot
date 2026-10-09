// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Independent UTF-16 canonical-byte model for interoperable integer JSON trees.

use serde_json::Value;

pub(crate) fn canonical_reference(value: &Value) -> String {
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
