// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned public-safe journal schema used by graph lifecycle commits.

use serde_json::Value;
use stateknot_core::{Digest, SchemaId, SchemaIdError, SchemaReference, Version};
use thiserror::Error;

use crate::{JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Stable identity of the v1 graph lifecycle event-data schema.
pub const STANDARD_GRAPH_LIFECYCLE_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/graph-lifecycle-event/1.0.0";

const DOCUMENT: &str = include_str!("../schemas/graph-lifecycle-event-1.0.0.json");

/// Returns the immutable reference and owned JSON Schema document shipped with
/// this runtime release.
///
/// # Errors
///
/// Returns [`StandardGraphLifecycleSchemaError`] only when the embedded
/// release artifact is internally malformed.
pub fn standard_graph_lifecycle_event_schema()
-> Result<(SchemaReference, Value), StandardGraphLifecycleSchemaError> {
    let document = serde_json::from_str::<Value>(DOCUMENT)
        .map_err(|_| StandardGraphLifecycleSchemaError::EmbeddedDocument)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StandardGraphLifecycleSchemaError::Canonicalization)?;
    let id = STANDARD_GRAPH_LIFECYCLE_EVENT_SCHEMA_ID
        .parse::<SchemaId>()
        .map_err(StandardGraphLifecycleSchemaError::identifier)?;
    let reference = SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical));
    Ok((reference, document))
}

/// Registers the standard graph lifecycle event schema in a startup registry.
///
/// # Errors
///
/// Returns an embedded-definition error or the registry's duplicate, resource,
/// dialect, digest, and reference-resolution failures.
pub fn register_standard_graph_lifecycle_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, StandardGraphLifecycleSchemaRegistrationError> {
    let (reference, document) = standard_graph_lifecycle_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Internal release-artifact failure for the embedded standard schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardGraphLifecycleSchemaError {
    /// The embedded JSON document cannot be parsed.
    #[error("embedded graph lifecycle event schema is invalid JSON")]
    EmbeddedDocument,
    /// The embedded document cannot be represented as RFC 8785 bytes.
    #[error("embedded graph lifecycle event schema cannot be canonicalized")]
    Canonicalization,
    /// The embedded stable HTTPS identifier is invalid.
    #[error("embedded graph lifecycle event schema identifier is invalid: {source}")]
    Identifier {
        /// Exact identifier validation error.
        #[source]
        source: SchemaIdError,
    },
}

impl StandardGraphLifecycleSchemaError {
    const fn identifier(source: SchemaIdError) -> Self {
        Self::Identifier { source }
    }
}

/// Startup failure while adding the standard lifecycle schema to a registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StandardGraphLifecycleSchemaRegistrationError {
    /// The embedded release artifact was malformed.
    #[error(transparent)]
    Definition(#[from] StandardGraphLifecycleSchemaError),
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
        let (reference, document) = standard_graph_lifecycle_event_schema().unwrap();
        assert_eq!(
            reference.id().as_str(),
            STANDARD_GRAPH_LIFECYCLE_EVENT_SCHEMA_ID
        );
        assert_eq!(reference.version(), Version::new(1, 0, 0));

        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        assert!(registry.contains(&reference));
    }

    #[test]
    fn standard_schema_accepts_only_the_three_closed_lifecycle_shapes() {
        let (reference, document) = standard_graph_lifecycle_event_schema().unwrap();
        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        builder.register(reference.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        let base = json!({
            "operation": "graph_barrier_waiting",
            "graph_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "checkpoint_id": "01912345-6789-7abc-8def-0123456789ab",
            "superstep": "1",
            "successor_checkpoint_id": "01912345-6789-7abc-8def-0123456789ac",
            "successor_superstep": "2",
            "disposition": "wait",
            "wait_count": 1
        });
        registry
            .validate_bounded(
                &reference,
                &BoundedJson::try_from_value(base.clone()).unwrap(),
            )
            .unwrap();

        let success = json!({
            "operation": "graph_barrier_succeeded",
            "graph_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "checkpoint_id": "01912345-6789-7abc-8def-0123456789ab",
            "superstep": "1",
            "successor_checkpoint_id": "01912345-6789-7abc-8def-0123456789ac",
            "successor_superstep": "2",
            "disposition": "succeeded",
            "output_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        });
        registry
            .validate_bounded(&reference, &BoundedJson::try_from_value(success).unwrap())
            .unwrap();

        let failure = json!({
            "operation": "graph_run_failed",
            "graph_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "checkpoint_id": "01912345-6789-7abc-8def-0123456789ab",
            "superstep": "1",
            "disposition": "failed",
            "failure_id": "01912345-6789-7abc-8def-0123456789ad",
            "in_flight": 0,
            "failed": 1,
            "exhausted": 0,
            "unsupported": 0
        });
        registry
            .validate_bounded(&reference, &BoundedJson::try_from_value(failure).unwrap())
            .unwrap();

        let mut crossed = base;
        crossed["output_digest"] =
            json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert!(
            registry
                .validate_bounded(&reference, &BoundedJson::try_from_value(crossed).unwrap())
                .is_err()
        );
    }
}
