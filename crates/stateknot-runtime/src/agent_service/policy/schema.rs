// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::PolicyError;
use crate::JsonSchemaRegistryBuilder;
use serde_json::Value;
use stateknot_core::{Digest, SchemaReference, Version};

/// Returns the offline digest-pinned admission evidence schema for this profile.
///
/// # Errors
/// Rejects an internally malformed embedded release artifact.
pub fn agent_policy_evidence_schema() -> Result<(SchemaReference, Value), PolicyError> {
    let document: Value = serde_json::from_str(include_str!(
        "../../../schemas/agent-policy-evidence-1.0.0.json"
    ))
    .map_err(|_| PolicyError)?;
    let id = document["$id"]
        .as_str()
        .ok_or(PolicyError)?
        .parse()
        .map_err(|_| PolicyError)?;
    let canonical = serde_json_canonicalizer::to_vec(&document).map_err(|_| PolicyError)?;
    Ok((
        SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical)),
        document,
    ))
}

/// Registers the evidence schema before freezing an executable registry.
///
/// # Errors
/// Rejects malformed schema or any registry resource/duplicate/digest failure.
pub fn register_agent_policy_evidence_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, PolicyError> {
    let (reference, document) = agent_policy_evidence_schema()?;
    builder
        .register(reference.clone(), document)
        .map_err(|_| PolicyError)?;
    Ok(reference)
}
