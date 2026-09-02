// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned public-safe journal schema for Agent service control mutations.

use serde_json::Value;
use stateknot_core::{Digest, SchemaId, SchemaIdError, SchemaReference, Version};
use thiserror::Error;

use crate::{JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Stable identity of the v1 Agent service control event-data schema.
pub const STANDARD_AGENT_SERVICE_CONTROL_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/agent-service-control-event/1.0.0";

const DOCUMENT: &str = include_str!("../schemas/agent-service-control-event-1.0.0.json");

/// Returns the immutable reference and owned JSON Schema document shipped with
/// this runtime release.
///
/// # Errors
///
/// Returns [`StandardAgentServiceControlSchemaError`] only when the embedded
/// release artifact is internally malformed.
pub fn standard_agent_service_control_event_schema()
-> Result<(SchemaReference, Value), StandardAgentServiceControlSchemaError> {
    let document = serde_json::from_str::<Value>(DOCUMENT)
        .map_err(|_| StandardAgentServiceControlSchemaError::EmbeddedDocument)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StandardAgentServiceControlSchemaError::Canonicalization)?;
    let id = STANDARD_AGENT_SERVICE_CONTROL_EVENT_SCHEMA_ID
        .parse::<SchemaId>()
        .map_err(StandardAgentServiceControlSchemaError::identifier)?;
    let reference = SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical));
    Ok((reference, document))
}

/// Registers the standard Agent service control schema in a startup registry.
///
/// # Errors
///
/// Returns an embedded-definition error or the registry's duplicate, resource,
/// dialect, digest, and reference-resolution failures.
pub fn register_standard_agent_service_control_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, StandardAgentServiceControlSchemaRegistrationError> {
    let (reference, document) = standard_agent_service_control_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Internal release-artifact failure for the embedded standard schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardAgentServiceControlSchemaError {
    /// The embedded JSON document cannot be parsed.
    #[error("embedded Agent service control event schema is invalid JSON")]
    EmbeddedDocument,
    /// The embedded document cannot be represented as RFC 8785 bytes.
    #[error("embedded Agent service control event schema cannot be canonicalized")]
    Canonicalization,
    /// The embedded stable HTTPS identifier is invalid.
    #[error("embedded Agent service control event schema identifier is invalid: {source}")]
    Identifier {
        /// Exact identifier validation error.
        #[source]
        source: SchemaIdError,
    },
}

impl StandardAgentServiceControlSchemaError {
    const fn identifier(source: SchemaIdError) -> Self {
        Self::Identifier { source }
    }
}

/// Startup failure while adding the standard Agent service control schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardAgentServiceControlSchemaRegistrationError {
    /// The embedded release artifact was malformed.
    #[error(transparent)]
    Definition(#[from] StandardAgentServiceControlSchemaError),
    /// The target registry rejected the immutable resource.
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
    fn standard_schema_is_digest_pinned_closed_and_registry_valid() {
        let (reference, document) = standard_agent_service_control_event_schema().unwrap();
        assert_eq!(
            reference.id().as_str(),
            STANDARD_AGENT_SERVICE_CONTROL_EVENT_SCHEMA_ID
        );
        assert_eq!(reference.version(), Version::new(1, 0, 0));

        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        let valid = json!({
            "operation": "agent_cancellation_requested",
            "admission_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "policy_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "decision_digest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "failure_id": "01912345-6789-7abc-8def-0123456789ab"
        });
        registry
            .validate_bounded(
                &reference,
                &BoundedJson::try_from_value(valid.clone()).unwrap(),
            )
            .unwrap();

        let mut leaked = valid;
        leaked["principal"] = json!("private-user");
        assert!(
            registry
                .validate_bounded(&reference, &BoundedJson::try_from_value(leaked).unwrap())
                .is_err()
        );
    }
}
