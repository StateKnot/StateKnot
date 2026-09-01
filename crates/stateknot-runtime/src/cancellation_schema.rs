// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned public-safe journal schema for durable cancellation acknowledgement.

use serde_json::Value;
use stateknot_core::{Digest, SchemaId, SchemaIdError, SchemaReference, Version};
use thiserror::Error;

use crate::{JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Stable identity of the v1 agent cancellation event-data schema.
pub const STANDARD_AGENT_CANCELLATION_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0";

const DOCUMENT: &str = include_str!("../schemas/agent-cancellation-event-1.0.0.json");

/// Returns the immutable reference and owned JSON Schema document shipped with
/// this runtime release.
///
/// # Errors
///
/// Returns [`StandardAgentCancellationSchemaError`] only when the embedded
/// release artifact is internally malformed.
pub fn standard_agent_cancellation_event_schema()
-> Result<(SchemaReference, Value), StandardAgentCancellationSchemaError> {
    let document = serde_json::from_str::<Value>(DOCUMENT)
        .map_err(|_| StandardAgentCancellationSchemaError::EmbeddedDocument)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StandardAgentCancellationSchemaError::Canonicalization)?;
    let id = STANDARD_AGENT_CANCELLATION_EVENT_SCHEMA_ID
        .parse::<SchemaId>()
        .map_err(StandardAgentCancellationSchemaError::identifier)?;
    let reference = SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical));
    Ok((reference, document))
}

/// Registers the standard agent cancellation event schema in a startup registry.
///
/// # Errors
///
/// Returns an embedded-definition error or the registry's duplicate, resource,
/// dialect, digest, and reference-resolution failures.
pub fn register_standard_agent_cancellation_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, StandardAgentCancellationSchemaRegistrationError> {
    let (reference, document) = standard_agent_cancellation_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Internal release-artifact failure for the embedded standard schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardAgentCancellationSchemaError {
    /// The embedded JSON document cannot be parsed.
    #[error("embedded agent cancellation event schema is invalid JSON")]
    EmbeddedDocument,
    /// The embedded document cannot be represented as RFC 8785 bytes.
    #[error("embedded agent cancellation event schema cannot be canonicalized")]
    Canonicalization,
    /// The embedded stable HTTPS identifier is invalid.
    #[error("embedded agent cancellation event schema identifier is invalid: {source}")]
    Identifier {
        /// Exact identifier validation error.
        #[source]
        source: SchemaIdError,
    },
}

impl StandardAgentCancellationSchemaError {
    const fn identifier(source: SchemaIdError) -> Self {
        Self::Identifier { source }
    }
}

/// Startup failure while adding the standard cancellation schema to a registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardAgentCancellationSchemaRegistrationError {
    /// The embedded release artifact was malformed.
    #[error(transparent)]
    Definition(#[from] StandardAgentCancellationSchemaError),
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
    fn standard_schema_is_digest_pinned_and_registry_valid() {
        let (reference, document) = standard_agent_cancellation_event_schema().unwrap();
        assert_eq!(
            reference.id().as_str(),
            STANDARD_AGENT_CANCELLATION_EVENT_SCHEMA_ID
        );
        assert_eq!(reference.version(), Version::new(1, 0, 0));

        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        assert!(registry.contains(&reference));
    }

    #[test]
    fn standard_schema_accepts_only_the_closed_confirmation_shape() {
        let (reference, document) = standard_agent_cancellation_event_schema().unwrap();
        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        let valid = json!({
            "operation": "agent_cancellation_confirmed",
            "graph_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "checkpoint_id": "01912345-6789-7abc-8def-0123456789ab",
            "superstep": "1",
            "failure_id": "01912345-6789-7abc-8def-0123456789ac"
        });
        registry
            .validate_bounded(
                &reference,
                &BoundedJson::try_from_value(valid.clone()).unwrap(),
            )
            .unwrap();

        let mut leaked = valid;
        leaked["usage"] = json!({"model_turns": 1});
        assert!(
            registry
                .validate_bounded(&reference, &BoundedJson::try_from_value(leaked).unwrap())
                .is_err()
        );
    }
}
