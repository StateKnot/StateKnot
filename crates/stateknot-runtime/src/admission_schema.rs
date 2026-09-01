// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned public-safe journal schema used by atomic Agent admission.

use serde_json::Value;
use stateknot_core::{Digest, SchemaId, SchemaIdError, SchemaReference, Version};
use thiserror::Error;

use crate::{JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Stable identity of the v1 Agent-admission event-data schema.
pub const STANDARD_AGENT_ADMISSION_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/agent-admission-event/1.0.0";

const DOCUMENT: &str = include_str!("../schemas/agent-admission-event-1.0.0.json");

/// Returns the immutable reference and owned JSON Schema document shipped with
/// this runtime release.
///
/// # Errors
///
/// Returns [`StandardAgentAdmissionSchemaError`] only when the embedded release
/// artifact is internally malformed.
pub fn standard_agent_admission_event_schema()
-> Result<(SchemaReference, Value), StandardAgentAdmissionSchemaError> {
    let document = serde_json::from_str::<Value>(DOCUMENT)
        .map_err(|_| StandardAgentAdmissionSchemaError::EmbeddedDocument)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StandardAgentAdmissionSchemaError::Canonicalization)?;
    let id = STANDARD_AGENT_ADMISSION_EVENT_SCHEMA_ID
        .parse::<SchemaId>()
        .map_err(StandardAgentAdmissionSchemaError::identifier)?;
    let reference = SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical));
    Ok((reference, document))
}

/// Registers the standard Agent-admission event schema in a startup registry.
///
/// # Errors
///
/// Returns an embedded-definition error or the registry's duplicate, resource,
/// dialect, digest, and reference-resolution failures.
pub fn register_standard_agent_admission_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, StandardAgentAdmissionSchemaRegistrationError> {
    let (reference, document) = standard_agent_admission_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Internal release-artifact failure for the standard admission schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardAgentAdmissionSchemaError {
    /// The embedded JSON document cannot be parsed.
    #[error("embedded Agent-admission event schema is invalid JSON")]
    EmbeddedDocument,
    /// The embedded document cannot be represented as RFC 8785 bytes.
    #[error("embedded Agent-admission event schema cannot be canonicalized")]
    Canonicalization,
    /// The embedded stable HTTPS identifier is invalid.
    #[error("embedded Agent-admission event schema identifier is invalid: {source}")]
    Identifier {
        /// Exact identifier validation error.
        #[source]
        source: SchemaIdError,
    },
}

impl StandardAgentAdmissionSchemaError {
    const fn identifier(source: SchemaIdError) -> Self {
        Self::Identifier { source }
    }
}

/// Startup failure while adding the standard admission schema to a registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardAgentAdmissionSchemaRegistrationError {
    /// The embedded release artifact was malformed.
    #[error(transparent)]
    Definition(#[from] StandardAgentAdmissionSchemaError),
    /// The target registry rejected the immutable resource.
    #[error(transparent)]
    Registry(#[from] JsonSchemaRegistryError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonSchemaRegistryLimits;
    use serde_json::json;
    use stateknot_core::BoundedJson;

    #[test]
    fn standard_schema_is_digest_pinned_registry_valid_and_closed() {
        let (reference, document) = standard_agent_admission_event_schema().unwrap();
        assert_eq!(
            reference.id().as_str(),
            STANDARD_AGENT_ADMISSION_EVENT_SCHEMA_ID
        );
        assert_eq!(reference.version(), Version::new(1, 0, 0));

        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        let valid = json!({
            "operation": "agent_admitted",
            "intent_digest": "a".repeat(64),
            "graph_digest": "b".repeat(64),
            "policy_digest": "c".repeat(64),
            "input_digest": "d".repeat(64)
        });
        registry
            .validate_bounded(
                &reference,
                &BoundedJson::try_from_value(valid.clone()).unwrap(),
            )
            .unwrap();

        let mut extra = valid;
        extra["request"] = json!({"secret": true});
        assert!(
            registry
                .validate_bounded(&reference, &BoundedJson::try_from_value(extra).unwrap())
                .is_err()
        );
    }
}
