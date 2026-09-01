// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned public-safe journal schema for model/tool attempt execution.

use serde_json::Value;
use stateknot_core::{Digest, SchemaId, SchemaIdError, SchemaReference, Version};
use thiserror::Error;

use crate::{JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Stable identity of the v1 invocation execution event-data schema.
pub const STANDARD_INVOCATION_EXECUTION_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/invocation-execution-event/1.0.0";

const DOCUMENT: &str = include_str!("../schemas/invocation-execution-event-1.0.0.json");

/// Returns the immutable reference and embedded JSON Schema document.
///
/// # Errors
///
/// Returns an internal release-artifact failure only if embedded bytes are
/// malformed, non-canonicalizable, or use an invalid stable identifier.
pub fn standard_invocation_execution_event_schema()
-> Result<(SchemaReference, Value), StandardInvocationExecutionSchemaError> {
    let document = serde_json::from_str::<Value>(DOCUMENT)
        .map_err(|_| StandardInvocationExecutionSchemaError::EmbeddedDocument)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StandardInvocationExecutionSchemaError::Canonicalization)?;
    let id = STANDARD_INVOCATION_EXECUTION_EVENT_SCHEMA_ID
        .parse::<SchemaId>()
        .map_err(StandardInvocationExecutionSchemaError::identifier)?;
    Ok((
        SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical)),
        document,
    ))
}

/// Registers the standard invocation event schema in a startup registry.
///
/// # Errors
///
/// Returns an embedded-definition or frozen-registry validation failure.
pub fn register_standard_invocation_execution_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, StandardInvocationExecutionSchemaRegistrationError> {
    let (reference, document) = standard_invocation_execution_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Internal embedded invocation-schema release-artifact failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardInvocationExecutionSchemaError {
    /// Embedded schema is invalid JSON.
    #[error("embedded invocation execution event schema is invalid JSON")]
    EmbeddedDocument,
    /// Embedded schema cannot be canonicalized.
    #[error("embedded invocation execution event schema cannot be canonicalized")]
    Canonicalization,
    /// Stable HTTPS schema identifier is invalid.
    #[error("embedded invocation execution event schema identifier is invalid: {source}")]
    Identifier {
        /// Exact identifier validation error.
        #[source]
        source: SchemaIdError,
    },
}

impl StandardInvocationExecutionSchemaError {
    const fn identifier(source: SchemaIdError) -> Self {
        Self::Identifier { source }
    }
}

/// Startup failure while adding the standard invocation schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardInvocationExecutionSchemaRegistrationError {
    /// Embedded release artifact was malformed.
    #[error(transparent)]
    Definition(#[from] StandardInvocationExecutionSchemaError),
    /// Target registry rejected the immutable resource.
    #[error(transparent)]
    Registry(#[from] JsonSchemaRegistryError),
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stateknot_core::BoundedJson;

    use super::*;
    use crate::JsonSchemaRegistryLimits;

    #[test]
    fn standard_schema_is_digest_pinned_and_closes_model_tool_shapes() {
        let (reference, document) = standard_invocation_execution_event_schema().unwrap();
        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        let model = BoundedJson::try_from_value(json!({
            "operation": "model_attempt_started",
            "binding_kind": "model",
            "invocation_id": "01912345-6789-7abc-8def-0123456789ab",
            "attempt_id": "01912345-6789-7abc-8def-0123456789ac",
            "intent_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }))
        .unwrap();
        registry.validate_bounded(&reference, &model).unwrap();

        let crossed = BoundedJson::try_from_value(json!({
            "operation": "tool_result_committed",
            "binding_kind": "model",
            "invocation_id": "01912345-6789-7abc-8def-0123456789ab",
            "attempt_id": "01912345-6789-7abc-8def-0123456789ac",
            "intent_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }))
        .unwrap();
        assert!(registry.validate_bounded(&reference, &crossed).is_err());
    }
}
