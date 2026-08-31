// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned public-safe journal schema used by the durable graph driver.

use serde_json::Value;
use stateknot_core::{Digest, SchemaId, SchemaIdError, SchemaReference, Version};
use thiserror::Error;

use crate::{JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Stable identity of the v1 durable graph-driver event-data schema.
pub const STANDARD_GRAPH_DRIVER_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/graph-driver-event/1.0.0";

const DOCUMENT: &str = include_str!("../schemas/graph-driver-event-1.0.0.json");

/// Returns the immutable reference and owned JSON Schema document shipped with
/// this runtime release.
///
/// The reference digest is computed from RFC 8785 bytes, so modifying the
/// embedded document without changing its URI/version changes the returned
/// reference and is rejected by any deployment that pinned the prior value.
///
/// # Errors
///
/// Returns [`StandardGraphDriverSchemaError`] only if the release artifact is
/// internally malformed.
pub fn standard_graph_driver_event_schema()
-> Result<(SchemaReference, Value), StandardGraphDriverSchemaError> {
    let document = serde_json::from_str::<Value>(DOCUMENT)
        .map_err(|_| StandardGraphDriverSchemaError::EmbeddedDocument)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StandardGraphDriverSchemaError::Canonicalization)?;
    let id = STANDARD_GRAPH_DRIVER_EVENT_SCHEMA_ID
        .parse::<SchemaId>()
        .map_err(StandardGraphDriverSchemaError::identifier)?;
    let reference = SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical));
    Ok((reference, document))
}

/// Registers the standard graph-driver event schema in a startup registry.
///
/// # Errors
///
/// Returns an embedded-definition error or the registry's ordinary duplicate,
/// resource, dialect, digest, and reference-resolution failures.
pub fn register_standard_graph_driver_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, StandardGraphDriverSchemaRegistrationError> {
    let (reference, document) = standard_graph_driver_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Internal release-artifact failure for the embedded standard schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardGraphDriverSchemaError {
    /// The embedded JSON document cannot be parsed.
    #[error("embedded graph-driver event schema is invalid JSON")]
    EmbeddedDocument,
    /// The embedded document cannot be represented as RFC 8785 bytes.
    #[error("embedded graph-driver event schema cannot be canonicalized")]
    Canonicalization,
    /// The embedded stable HTTPS identifier is invalid.
    #[error("embedded graph-driver event schema identifier is invalid: {source}")]
    Identifier {
        /// Exact identifier validation error.
        #[source]
        source: SchemaIdError,
    },
}

impl StandardGraphDriverSchemaError {
    const fn identifier(source: SchemaIdError) -> Self {
        Self::Identifier { source }
    }
}

/// Startup failure while adding the standard driver schema to a registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardGraphDriverSchemaRegistrationError {
    /// The embedded release artifact was malformed.
    #[error(transparent)]
    Definition(#[from] StandardGraphDriverSchemaError),
    /// The target registry rejected the immutable resource.
    #[error(transparent)]
    Registry(#[from] JsonSchemaRegistryError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonSchemaRegistryLimits;

    #[test]
    fn standard_schema_is_digest_pinned_and_registry_valid() {
        let (reference, document) = standard_graph_driver_event_schema().unwrap();
        assert_eq!(
            reference.id().as_str(),
            STANDARD_GRAPH_DRIVER_EVENT_SCHEMA_ID
        );
        assert_eq!(reference.version(), Version::new(1, 0, 0));

        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        assert!(registry.contains(&reference));
    }
}
