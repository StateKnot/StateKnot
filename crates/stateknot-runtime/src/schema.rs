// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Offline, digest-pinned JSON Schema 2020-12 registry.

use std::{collections::HashMap, sync::Arc};

use jsonschema::{Draft, Registry, Validator};
use serde_json::Value;
use stateknot_core::{
    BoundedJson, Digest, GraphSchemaValidationError, GraphSchemaValidator, SchemaReference,
};
use thiserror::Error;

const MEBIBYTE: usize = 1024 * 1024;
const DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

/// Resource ceilings applied while an executable schema registry is built.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonSchemaRegistryLimits {
    maximum_schemas: usize,
    maximum_schema_bytes: usize,
    maximum_total_bytes: usize,
}

impl JsonSchemaRegistryLimits {
    /// Absolute implementation ceiling for registered resources.
    pub const HARD_MAXIMUM_SCHEMAS: usize = 4096;
    /// Absolute implementation ceiling for one canonical schema document.
    pub const HARD_MAXIMUM_SCHEMA_BYTES: usize = 8 * MEBIBYTE;
    /// Absolute implementation ceiling for all canonical schema documents.
    pub const HARD_MAXIMUM_TOTAL_BYTES: usize = 256 * MEBIBYTE;

    /// Constructs positive limits within implementation ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`JsonSchemaRegistryLimitsError`] for zero, inconsistent, or
    /// implementation-exceeding limits.
    pub const fn new(
        maximum_schemas: usize,
        maximum_schema_bytes: usize,
        maximum_total_bytes: usize,
    ) -> Result<Self, JsonSchemaRegistryLimitsError> {
        if maximum_schemas == 0 {
            return Err(JsonSchemaRegistryLimitsError::ZeroSchemas);
        }
        if maximum_schema_bytes == 0 {
            return Err(JsonSchemaRegistryLimitsError::ZeroSchemaBytes);
        }
        if maximum_total_bytes < maximum_schema_bytes {
            return Err(JsonSchemaRegistryLimitsError::TotalBelowSingleSchema);
        }
        if maximum_schemas > Self::HARD_MAXIMUM_SCHEMAS {
            return Err(JsonSchemaRegistryLimitsError::TooManySchemas {
                maximum: Self::HARD_MAXIMUM_SCHEMAS,
                actual: maximum_schemas,
            });
        }
        if maximum_schema_bytes > Self::HARD_MAXIMUM_SCHEMA_BYTES {
            return Err(JsonSchemaRegistryLimitsError::SchemaBytesTooLarge {
                maximum: Self::HARD_MAXIMUM_SCHEMA_BYTES,
                actual: maximum_schema_bytes,
            });
        }
        if maximum_total_bytes > Self::HARD_MAXIMUM_TOTAL_BYTES {
            return Err(JsonSchemaRegistryLimitsError::TotalBytesTooLarge {
                maximum: Self::HARD_MAXIMUM_TOTAL_BYTES,
                actual: maximum_total_bytes,
            });
        }
        Ok(Self {
            maximum_schemas,
            maximum_schema_bytes,
            maximum_total_bytes,
        })
    }

    /// Returns the configured resource-count ceiling.
    #[must_use]
    pub const fn maximum_schemas(self) -> usize {
        self.maximum_schemas
    }

    /// Returns the configured per-document canonical byte ceiling.
    #[must_use]
    pub const fn maximum_schema_bytes(self) -> usize {
        self.maximum_schema_bytes
    }

    /// Returns the configured aggregate canonical byte ceiling.
    #[must_use]
    pub const fn maximum_total_bytes(self) -> usize {
        self.maximum_total_bytes
    }
}

impl Default for JsonSchemaRegistryLimits {
    fn default() -> Self {
        Self {
            maximum_schemas: 1024,
            maximum_schema_bytes: 2 * MEBIBYTE,
            maximum_total_bytes: 64 * MEBIBYTE,
        }
    }
}

/// Invalid executable schema-registry limits.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JsonSchemaRegistryLimitsError {
    /// A registry could contain no resources.
    #[error("schema registry maximum schema count must be positive")]
    ZeroSchemas,
    /// A schema document could contain no bytes.
    #[error("schema registry per-schema byte maximum must be positive")]
    ZeroSchemaBytes,
    /// The aggregate ceiling could not contain one maximum-size document.
    #[error("schema registry total byte maximum is below its per-schema maximum")]
    TotalBelowSingleSchema,
    /// The resource-count ceiling exceeded the implementation bound.
    #[error("schema registry maximum is {actual}; implementation maximum is {maximum}")]
    TooManySchemas {
        /// Absolute implementation maximum.
        maximum: usize,
        /// Rejected configured value.
        actual: usize,
    },
    /// The per-document byte ceiling exceeded the implementation bound.
    #[error("schema byte maximum is {actual}; implementation maximum is {maximum}")]
    SchemaBytesTooLarge {
        /// Absolute implementation maximum.
        maximum: usize,
        /// Rejected configured value.
        actual: usize,
    },
    /// The aggregate byte ceiling exceeded the implementation bound.
    #[error("schema aggregate byte maximum is {actual}; implementation maximum is {maximum}")]
    TotalBytesTooLarge {
        /// Absolute implementation maximum.
        maximum: usize,
        /// Rejected configured value.
        actual: usize,
    },
}

#[derive(Clone)]
struct PendingSchema {
    reference: SchemaReference,
    document: Value,
    canonical: Arc<[u8]>,
}

/// Mutable startup-only builder for an immutable offline schema registry.
///
/// Every document must name the exact reference URI in `$id`, explicitly use
/// JSON Schema 2020-12, match the reference's SHA-256 digest after RFC 8785
/// canonicalization, and pass meta-schema validation before it is retained.
/// The final build eagerly compiles every validator and rejects unresolved
/// references. No schema retrieval path exists on [`JsonSchemaRegistry`].
#[derive(Debug)]
pub struct JsonSchemaRegistryBuilder {
    limits: JsonSchemaRegistryLimits,
    schemas: Vec<PendingSchema>,
    by_reference: HashMap<SchemaReference, usize>,
    by_id: HashMap<String, SchemaReference>,
    total_bytes: usize,
}

impl JsonSchemaRegistryBuilder {
    /// Creates an empty startup builder with explicit resource ceilings.
    #[must_use]
    pub fn new(limits: JsonSchemaRegistryLimits) -> Self {
        Self {
            limits,
            schemas: Vec::new(),
            by_reference: HashMap::new(),
            by_id: HashMap::new(),
            total_bytes: 0,
        }
    }

    /// Creates an empty startup builder with production defaults.
    #[must_use]
    pub fn with_default_limits() -> Self {
        Self::new(JsonSchemaRegistryLimits::default())
    }

    /// Registers one owned schema document after integrity and dialect checks.
    ///
    /// A URI may identify only one immutable resource in a registry. This is
    /// stricter than keying by the full reference because JSON Schema `$ref`
    /// resolution uses the URI and cannot select a version or digest sideband.
    ///
    /// # Errors
    ///
    /// Returns [`JsonSchemaRegistryError`] for duplicate identity, resource
    /// exhaustion, canonicalization/digest drift, an invalid `$id`/dialect, or
    /// a meta-schema violation.
    pub fn register(
        &mut self,
        reference: SchemaReference,
        document: Value,
    ) -> Result<(), JsonSchemaRegistryError> {
        if self.schemas.len() == self.limits.maximum_schemas {
            return Err(JsonSchemaRegistryError::TooManySchemas {
                maximum: self.limits.maximum_schemas,
                actual: self.limits.maximum_schemas + 1,
            });
        }
        if self.by_reference.contains_key(&reference) {
            return Err(JsonSchemaRegistryError::DuplicateReference {
                reference: Box::new(reference),
            });
        }
        let id = reference.id().as_str();
        if let Some(existing) = self.by_id.get(id) {
            return Err(JsonSchemaRegistryError::DuplicateId {
                existing: Box::new(existing.clone()),
                rejected: Box::new(reference),
            });
        }

        let object = document
            .as_object()
            .ok_or(JsonSchemaRegistryError::SchemaRootNotObject)?;
        match object.get("$schema").and_then(Value::as_str) {
            Some(DRAFT_2020_12) => {}
            _ => return Err(JsonSchemaRegistryError::UnsupportedDialect),
        }
        if object.get("$id").and_then(Value::as_str) != Some(id) {
            return Err(JsonSchemaRegistryError::IdMismatch {
                reference: Box::new(reference),
            });
        }

        jsonschema::draft202012::meta::validate(&document).map_err(|source| {
            JsonSchemaRegistryError::InvalidSchema {
                diagnostic: source.to_string().into_boxed_str(),
            }
        })?;
        let canonical = serde_json_canonicalizer::to_vec(&document)
            .map_err(|_| JsonSchemaRegistryError::Canonicalization)?;
        if canonical.len() > self.limits.maximum_schema_bytes {
            return Err(JsonSchemaRegistryError::SchemaTooLarge {
                maximum: self.limits.maximum_schema_bytes,
                actual: canonical.len(),
            });
        }
        if Digest::sha256(&canonical) != reference.digest() {
            return Err(JsonSchemaRegistryError::DigestMismatch {
                reference: Box::new(reference),
            });
        }
        let total_bytes = self.total_bytes.checked_add(canonical.len()).ok_or(
            JsonSchemaRegistryError::AggregateTooLarge {
                maximum: self.limits.maximum_total_bytes,
                actual: usize::MAX,
            },
        )?;
        if total_bytes > self.limits.maximum_total_bytes {
            return Err(JsonSchemaRegistryError::AggregateTooLarge {
                maximum: self.limits.maximum_total_bytes,
                actual: total_bytes,
            });
        }

        let index = self.schemas.len();
        self.by_id.insert(id.to_owned(), reference.clone());
        self.by_reference.insert(reference.clone(), index);
        self.schemas.push(PendingSchema {
            reference,
            document,
            canonical: canonical.into(),
        });
        self.total_bytes = total_bytes;
        Ok(())
    }

    /// Freezes all resources and eagerly compiles offline validators.
    ///
    /// # Errors
    ///
    /// Returns [`JsonSchemaRegistryError`] when the registry is empty, a URI
    /// cannot be indexed, or any local `$ref` cannot be resolved and compiled.
    pub fn build(self) -> Result<JsonSchemaRegistry, JsonSchemaRegistryError> {
        if self.schemas.is_empty() {
            return Err(JsonSchemaRegistryError::Empty);
        }

        let mut registry_builder = Registry::new().draft(Draft::Draft202012);
        for schema in &self.schemas {
            registry_builder = registry_builder
                .add(schema.reference.id().as_str(), schema.document.clone())
                .map_err(|source| JsonSchemaRegistryError::RegistryPreparation {
                    diagnostic: source.to_string().into_boxed_str(),
                })?;
        }
        let registry = registry_builder.prepare().map_err(|source| {
            JsonSchemaRegistryError::RegistryPreparation {
                diagnostic: source.to_string().into_boxed_str(),
            }
        })?;

        let mut entries = HashMap::with_capacity(self.schemas.len());
        for schema in self.schemas {
            let validator = jsonschema::draft202012::options()
                .with_registry(&registry)
                .should_validate_formats(true)
                .offline()
                .build(&schema.document)
                .map_err(|source| JsonSchemaRegistryError::ValidatorCompilation {
                    reference: Box::new(schema.reference.clone()),
                    diagnostic: source.to_string().into_boxed_str(),
                })?;
            entries.insert(
                schema.reference,
                CompiledSchema {
                    canonical: schema.canonical,
                    validator,
                },
            );
        }

        Ok(JsonSchemaRegistry {
            inner: Arc::new(JsonSchemaRegistryInner {
                entries,
                total_bytes: self.total_bytes,
            }),
        })
    }
}

impl Default for JsonSchemaRegistryBuilder {
    fn default() -> Self {
        Self::with_default_limits()
    }
}

impl std::fmt::Debug for PendingSchema {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingSchema")
            .field("reference", &self.reference)
            .field("canonical_bytes", &self.canonical.len())
            .finish_non_exhaustive()
    }
}

struct CompiledSchema {
    canonical: Arc<[u8]>,
    validator: Validator,
}

struct JsonSchemaRegistryInner {
    entries: HashMap<SchemaReference, CompiledSchema>,
    total_bytes: usize,
}

/// Immutable, shareable registry of eagerly compiled offline validators.
#[derive(Clone)]
pub struct JsonSchemaRegistry {
    inner: Arc<JsonSchemaRegistryInner>,
}

impl JsonSchemaRegistry {
    /// Returns whether the exact URI, version, and digest are installed.
    #[must_use]
    pub fn contains(&self, reference: &SchemaReference) -> bool {
        self.inner.entries.contains_key(reference)
    }

    /// Returns the number of immutable schema resources.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.entries.len()
    }

    /// Returns whether the frozen registry contains no resources.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.entries.is_empty()
    }

    /// Returns the total retained canonical schema bytes.
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.inner.total_bytes
    }

    /// Returns the exact canonical document bytes for trusted diagnostics.
    #[must_use]
    pub fn canonical_bytes(&self, reference: &SchemaReference) -> Option<&[u8]> {
        self.inner
            .entries
            .get(reference)
            .map(|entry| entry.canonical.as_ref())
    }

    /// Validates a bounded instance and retains no instance data.
    ///
    /// # Errors
    ///
    /// Returns [`GraphSchemaValidationError::Unavailable`] when the complete
    /// reference is absent and [`GraphSchemaValidationError::Rejected`] when
    /// the instance violates the compiled contract.
    pub fn validate_bounded(
        &self,
        reference: &SchemaReference,
        value: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        let entry = self
            .inner
            .entries
            .get(reference)
            .ok_or(GraphSchemaValidationError::Unavailable)?;
        if entry.validator.is_valid(value.as_value()) {
            Ok(())
        } else {
            Err(GraphSchemaValidationError::Rejected)
        }
    }
}

impl std::fmt::Debug for JsonSchemaRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JsonSchemaRegistry")
            .field("schemas", &self.len())
            .field("total_bytes", &self.total_bytes())
            .finish_non_exhaustive()
    }
}

impl GraphSchemaValidator for JsonSchemaRegistry {
    fn validate(
        &self,
        schema: &SchemaReference,
        value: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        self.validate_bounded(schema, value)
    }
}

/// Startup-time failure while constructing an offline schema registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JsonSchemaRegistryError {
    /// Runtime execution without any trusted schema would fail open.
    #[error("schema registry must contain at least one resource")]
    Empty,
    /// The configured resource count was exhausted.
    #[error("schema registry contains {actual} resources; maximum is {maximum}")]
    TooManySchemas {
        /// Configured maximum.
        maximum: usize,
        /// First rejected count.
        actual: usize,
    },
    /// The complete immutable reference was registered twice.
    #[error("schema reference was registered more than once")]
    DuplicateReference {
        /// Repeated reference.
        reference: Box<SchemaReference>,
    },
    /// One URI attempted to identify multiple immutable resources.
    #[error("schema URI was reused by another version or digest")]
    DuplicateId {
        /// First immutable reference.
        existing: Box<SchemaReference>,
        /// Rejected immutable reference.
        rejected: Box<SchemaReference>,
    },
    /// A schema document was not an object and could not carry identity.
    #[error("schema document root must be an object")]
    SchemaRootNotObject,
    /// The exact mandatory dialect declaration was absent.
    #[error("schema must declare JSON Schema 2020-12")]
    UnsupportedDialect,
    /// `$id` did not equal the normalized pinned URI.
    #[error("schema $id does not match its immutable reference")]
    IdMismatch {
        /// Expected immutable reference.
        reference: Box<SchemaReference>,
    },
    /// The schema did not satisfy the official meta-schema.
    #[error("schema violates JSON Schema 2020-12: {diagnostic}")]
    InvalidSchema {
        /// Startup-safe schema diagnostic; no runtime instance is involved.
        diagnostic: Box<str>,
    },
    /// RFC 8785 serialization unexpectedly failed.
    #[error("schema canonicalization failed")]
    Canonicalization,
    /// Canonical bytes exceeded the configured single-resource limit.
    #[error("canonical schema is {actual} bytes; maximum is {maximum}")]
    SchemaTooLarge {
        /// Configured maximum.
        maximum: usize,
        /// Observed canonical bytes.
        actual: usize,
    },
    /// Canonical schema bytes did not match the pinned digest.
    #[error("canonical schema digest does not match its immutable reference")]
    DigestMismatch {
        /// Rejected immutable reference.
        reference: Box<SchemaReference>,
    },
    /// Aggregate canonical bytes exceeded the configured limit.
    #[error("schema registry contains {actual} canonical bytes; maximum is {maximum}")]
    AggregateTooLarge {
        /// Configured maximum.
        maximum: usize,
        /// Observed or saturated aggregate.
        actual: usize,
    },
    /// The local resource graph could not be prepared.
    #[error("schema registry resource preparation failed: {diagnostic}")]
    RegistryPreparation {
        /// Startup-safe schema diagnostic.
        diagnostic: Box<str>,
    },
    /// One exact schema could not compile with offline reference resolution.
    #[error("schema validator compilation failed: {diagnostic}")]
    ValidatorCompilation {
        /// Exact resource that failed compilation.
        reference: Box<SchemaReference>,
        /// Startup-safe schema diagnostic.
        diagnostic: Box<str>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stateknot_core::{SchemaId, Version};

    use super::*;

    fn reference(id: &str, document: &Value) -> SchemaReference {
        let canonical = serde_json_canonicalizer::to_vec(document).unwrap();
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            "1.0.0".parse::<Version>().unwrap(),
            Digest::sha256(canonical),
        )
    }

    #[test]
    fn registry_is_offline_digest_pinned_and_resolves_local_resources() {
        let address_id = "https://schemas.example.com/address/1.0.0";
        let address = json!({
            "$schema": DRAFT_2020_12,
            "$id": address_id,
            "type": "object",
            "properties": {"postcode": {"type": "string", "pattern": "^[0-9]{5}$"}},
            "required": ["postcode"],
            "additionalProperties": false
        });
        let user_id = "https://schemas.example.com/user/1.0.0";
        let user = json!({
            "$schema": DRAFT_2020_12,
            "$id": user_id,
            "type": "object",
            "properties": {
                "email": {"type": "string", "format": "email"},
                "address": {"$ref": address_id}
            },
            "required": ["email", "address"],
            "additionalProperties": false
        });
        let address_reference = reference(address_id, &address);
        let user_reference = reference(user_id, &user);
        let mut builder = JsonSchemaRegistryBuilder::default();
        builder
            .register(address_reference.clone(), address)
            .unwrap();
        builder.register(user_reference.clone(), user).unwrap();
        let registry = builder.build().unwrap();

        let valid = BoundedJson::try_from_value(json!({
            "email": "agent@example.com",
            "address": {"postcode": "31000"}
        }))
        .unwrap();
        let bad_format = BoundedJson::try_from_value(json!({
            "email": "not-an-email",
            "address": {"postcode": "31000"}
        }))
        .unwrap();
        let bad_reference_value = BoundedJson::try_from_value(json!({
            "email": "agent@example.com",
            "address": {"postcode": "broken"}
        }))
        .unwrap();

        assert_eq!(registry.len(), 2);
        assert!(registry.total_bytes() > 0);
        assert!(registry.canonical_bytes(&address_reference).is_some());
        assert_eq!(registry.validate_bounded(&user_reference, &valid), Ok(()));
        assert_eq!(
            registry.validate_bounded(&user_reference, &bad_format),
            Err(GraphSchemaValidationError::Rejected)
        );
        assert_eq!(
            registry.validate_bounded(&user_reference, &bad_reference_value),
            Err(GraphSchemaValidationError::Rejected)
        );
    }

    #[test]
    fn registration_rejects_digest_dialect_id_and_uri_aliases() {
        let id = "https://schemas.example.com/state/1.0.0";
        let document = json!({
            "$schema": DRAFT_2020_12,
            "$id": id,
            "type": "object"
        });
        let exact = reference(id, &document);

        let mut mismatch = JsonSchemaRegistryBuilder::default();
        let wrong = SchemaReference::new(
            exact.id().clone(),
            exact.version(),
            Digest::sha256(b"wrong"),
        );
        assert!(matches!(
            mismatch.register(wrong, document.clone()),
            Err(JsonSchemaRegistryError::DigestMismatch { .. })
        ));

        let mut dialect = document.clone();
        dialect["$schema"] = json!("https://json-schema.org/draft/2019-09/schema");
        assert_eq!(
            JsonSchemaRegistryBuilder::default().register(reference(id, &dialect), dialect),
            Err(JsonSchemaRegistryError::UnsupportedDialect)
        );

        let mut wrong_id = document.clone();
        wrong_id["$id"] = json!("https://schemas.example.com/other/1.0.0");
        assert!(matches!(
            JsonSchemaRegistryBuilder::default().register(reference(id, &wrong_id), wrong_id),
            Err(JsonSchemaRegistryError::IdMismatch { .. })
        ));

        let mut duplicate = JsonSchemaRegistryBuilder::default();
        duplicate.register(exact.clone(), document.clone()).unwrap();
        let alias =
            SchemaReference::new(exact.id().clone(), "2.0.0".parse().unwrap(), exact.digest());
        assert!(matches!(
            duplicate.register(alias, document),
            Err(JsonSchemaRegistryError::DuplicateId { .. })
        ));
    }

    #[test]
    fn build_rejects_unresolved_network_reference() {
        let id = "https://schemas.example.com/offline/1.0.0";
        let document = json!({
            "$schema": DRAFT_2020_12,
            "$id": id,
            "$ref": "https://schemas.example.com/not-installed/1.0.0"
        });
        let mut builder = JsonSchemaRegistryBuilder::default();
        builder
            .register(reference(id, &document), document)
            .unwrap();
        assert!(matches!(
            builder.build(),
            Err(JsonSchemaRegistryError::ValidatorCompilation { .. }
                | JsonSchemaRegistryError::RegistryPreparation { .. })
        ));
    }

    #[test]
    fn absent_full_reference_is_unavailable() {
        let id = "https://schemas.example.com/value/1.0.0";
        let document = json!({
            "$schema": DRAFT_2020_12,
            "$id": id,
            "type": "integer"
        });
        let installed = reference(id, &document);
        let mut builder = JsonSchemaRegistryBuilder::default();
        builder.register(installed.clone(), document).unwrap();
        let registry = builder.build().unwrap();
        let missing = SchemaReference::new(
            installed.id().clone(),
            installed.version(),
            Digest::sha256(b"different"),
        );
        let value = BoundedJson::try_from_value(json!(1)).unwrap();
        assert_eq!(
            registry.validate_bounded(&missing, &value),
            Err(GraphSchemaValidationError::Unavailable)
        );
    }

    #[test]
    fn limits_fail_closed() {
        assert_eq!(
            JsonSchemaRegistryLimits::new(0, 1, 1),
            Err(JsonSchemaRegistryLimitsError::ZeroSchemas)
        );
        assert_eq!(
            JsonSchemaRegistryLimits::new(1, 2, 1),
            Err(JsonSchemaRegistryLimitsError::TotalBelowSingleSchema)
        );

        let limits = JsonSchemaRegistryLimits::new(1, 1024, 1024).unwrap();
        let first_id = "https://schemas.example.com/one/1.0.0";
        let first = json!({"$schema": DRAFT_2020_12, "$id": first_id});
        let second_id = "https://schemas.example.com/two/1.0.0";
        let second = json!({"$schema": DRAFT_2020_12, "$id": second_id});
        let mut builder = JsonSchemaRegistryBuilder::new(limits);
        builder
            .register(reference(first_id, &first), first)
            .unwrap();
        assert_eq!(
            builder.register(reference(second_id, &second), second),
            Err(JsonSchemaRegistryError::TooManySchemas {
                maximum: 1,
                actual: 2
            })
        );
    }
}
