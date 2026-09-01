// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Constructs both first-party model adapters without dispatching a request.

use std::{error::Error, sync::Arc};

use serde_json::{Value, json};
use stateknot_core::{
    BoxFuture, CapabilityDescription, CapabilityIdentity, CapabilityKind, CapabilityLifecycle,
    CapabilityMetadata, CapabilityName, CapabilityReference, Digest, Extensions, IssuerId, Model,
    ModelCapabilities, ModelDescriptor, ModelModalities, ModelModality, ModelProviderModelId,
    ModelStructuredOutputCapabilities, ModelTokenLimits, ModelToolCapabilities, PrincipalIdentity,
    SchemaId, SchemaReference, ScopeSet, SecurityLabel, SubjectId, Version,
};
use stateknot_integrations::{
    AnthropicMessagesModel, ApiKey, ApiKeyProvider, ApiKeyResolutionError, OpenAiResponsesModel,
    ProviderEndpoint, ProviderHttpOptions,
};
use stateknot_runtime::JsonSchemaRegistryBuilder;

const VERSION: Version = Version::new(1, 0, 0);
const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

struct EnvironmentApiKey(&'static str);

impl ApiKeyProvider for EnvironmentApiKey {
    fn resolve(
        &self,
        _context: &stateknot_core::ModelContext,
    ) -> BoxFuture<'_, Result<ApiKey, ApiKeyResolutionError>> {
        let variable = self.0;
        Box::pin(async move {
            let value = std::env::var(variable).map_err(|_| ApiKeyResolutionError::Unavailable)?;
            ApiKey::new(value).map_err(|_| ApiKeyResolutionError::PermissionDenied)
        })
    }
}

fn schema_resource(id: &str, document: Value) -> Result<(SchemaReference, Value), Box<dyn Error>> {
    let canonical = serde_json_canonicalizer::to_vec(&document)?;
    Ok((
        SchemaReference::new(id.parse::<SchemaId>()?, VERSION, Digest::sha256(canonical)),
        document,
    ))
}

fn model(
    owner: &PrincipalIdentity,
    name: &str,
    profile: SchemaReference,
) -> Result<ModelDescriptor, Box<dyn Error>> {
    let metadata = CapabilityMetadata::new(
        CapabilityIdentity::new(
            owner.clone(),
            CapabilityReference::new(CapabilityName::new(name)?, VERSION),
        ),
        CapabilityKind::Model,
        None,
        CapabilityDescription::new("Pinned text model binding")?,
        CapabilityLifecycle::active(),
        ScopeSet::empty(),
        Extensions::default(),
    )?;
    let text = ModelModalities::try_new([ModelModality::Text])?;
    Ok(ModelDescriptor::new(
        metadata,
        ModelCapabilities::new(
            text.clone(),
            text,
            true,
            ModelToolCapabilities::unsupported(),
            ModelStructuredOutputCapabilities::json_schema(profile),
            false,
            ModelTokenLimits::unknown(),
        )?,
    )?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot".parse::<IssuerId>()?,
        "model-registry".parse::<SubjectId>()?,
    );
    let profile_id = "https://schemas.example.com/providers/json-schema-profile/1.0.0";
    let (profile, profile_document) = schema_resource(
        profile_id,
        json!({
            "$schema": DIALECT,
            "$id": profile_id,
            "type": "object"
        }),
    )?;
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas.register(profile.clone(), profile_document)?;
    let schemas = Arc::new(schemas.build()?);
    let schemas_for_openai: Arc<dyn stateknot_core::ModelSchemaRegistry> = schemas.clone();
    let schemas_for_anthropic: Arc<dyn stateknot_core::ModelSchemaRegistry> = schemas;

    let openai = OpenAiResponsesModel::new(
        model(&owner, "models.openai-primary", profile.clone())?,
        ModelProviderModelId::new("provider-model-snapshot-id")?,
        SecurityLabel::new("tenant/model-output")?,
        schemas_for_openai,
        Arc::new(EnvironmentApiKey("OPENAI_API_KEY")),
        ProviderEndpoint::https("https://api.openai.com/v1/")?,
        ProviderHttpOptions::default(),
    )?;
    let anthropic = AnthropicMessagesModel::new(
        model(&owner, "models.anthropic-primary", profile)?,
        ModelProviderModelId::new("provider-model-snapshot-id")?,
        SecurityLabel::new("tenant/model-output")?,
        schemas_for_anthropic,
        Arc::new(EnvironmentApiKey("ANTHROPIC_API_KEY")),
        ProviderEndpoint::https("https://api.anthropic.com/v1/")?,
        ProviderHttpOptions::default(),
    )?;

    // Constructors perform no network I/O and resolve no credentials. The
    // durable invocation executor supplies an attempt-scoped ModelContext.
    println!("openai={:?}", openai.descriptor().metadata().identity());
    println!(
        "anthropic={:?}",
        anthropic.descriptor().metadata().identity()
    );
    Ok(())
}
