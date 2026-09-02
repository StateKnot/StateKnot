// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable, authorization-first MCP 2026-07-28 Resource server surface.

use std::{borrow::Cow, collections::BTreeMap, fmt, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        DiscoverResult, Implementation, ListResourceTemplatesResult, ListResourcesResult,
        PaginatedRequestParams, ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse,
        ReadResourceResult, Resource, ResourceContents, ResourceTemplate, ResourcesCapability,
        ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
};
use serde_json::Value;
use stateknot_core::{BoxFuture, Digest};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    McpServerApplicationOptions, McpServerCacheScope, McpServerInputRequired, McpServerPrincipal,
    mcp_server_principal,
    mcp_server_tool::{append_digest_part, validate_inbound_mrtr},
};

const MEBIBYTE: usize = 1024 * 1024;
const MAX_URI_BYTES: usize = 4096;
const MAX_NAME_BYTES: usize = 512;
const MAX_DISPLAY_BYTES: usize = 16 * 1024;
const MAX_SCOPE_COUNT: usize = 128;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_CONTENTS: usize = 256;
const MAX_TEXT_BYTES: usize = 4 * MEBIBYTE;
const MAX_BINARY_BYTES: usize = 12 * MEBIBYTE;
const MAX_RESULT_BYTES: usize = 32 * MEBIBYTE;

/// One statically discoverable MCP Resource.
#[derive(Clone, Debug)]
pub struct McpServerResourceDefinition {
    uri: Arc<str>,
    name: Arc<str>,
    title: Option<Arc<str>>,
    description: Option<Arc<str>>,
    mime_type: Option<Arc<str>>,
    size: Option<u64>,
    required_scopes: Arc<[Box<str>]>,
}

impl McpServerResourceDefinition {
    /// Creates a bounded absolute URI and programmatic name.
    pub fn new(
        uri: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        let uri = uri.into();
        let name = name.into();
        validate_uri(&uri)?;
        validate_name(&name)?;
        Ok(Self {
            uri: Arc::from(uri),
            name: Arc::from(name),
            title: None,
            description: None,
            mime_type: None,
            size: None,
            required_scopes: Arc::from([]),
        })
    }

    /// Adds a human-readable title.
    pub fn with_title(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.title = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds a human-readable description.
    pub fn with_description(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.description = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds a syntactically valid MIME type.
    pub fn with_mime_type(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.mime_type = Some(Arc::from(validate_mime(value.into())?));
        Ok(self)
    }

    /// Adds the raw content size when known.
    #[must_use]
    pub const fn with_size(mut self, value: u64) -> Self {
        self.size = Some(value);
        self
    }

    /// Requires every listed OAuth-style scope for discovery and reads.
    pub fn with_required_scopes(
        mut self,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.required_scopes = validate_scopes(scopes)?;
        Ok(self)
    }

    /// Returns the exact resource URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the programmatic name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns required scopes in canonical order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(AsRef::as_ref)
    }

    fn to_protocol(&self) -> Resource {
        let mut resource = Resource::new(self.uri.to_string(), self.name.to_string());
        resource.title = self.title.as_deref().map(str::to_owned);
        resource.description = self.description.as_deref().map(str::to_owned);
        resource.mime_type = self.mime_type.as_deref().map(str::to_owned);
        resource.size = self.size;
        resource
    }
}

/// One statically discoverable RFC 6570 Resource template.
#[derive(Clone, Debug)]
pub struct McpServerResourceTemplateDefinition {
    uri_template: Arc<str>,
    name: Arc<str>,
    title: Option<Arc<str>>,
    description: Option<Arc<str>>,
    mime_type: Option<Arc<str>>,
    required_scopes: Arc<[Box<str>]>,
}

impl McpServerResourceTemplateDefinition {
    /// Creates a structurally validated absolute URI template.
    pub fn new(
        uri_template: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        let uri_template = uri_template.into();
        let name = name.into();
        validate_uri_template(&uri_template)?;
        validate_name(&name)?;
        Ok(Self {
            uri_template: Arc::from(uri_template),
            name: Arc::from(name),
            title: None,
            description: None,
            mime_type: None,
            required_scopes: Arc::from([]),
        })
    }

    /// Adds a human-readable title.
    pub fn with_title(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.title = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds a human-readable description.
    pub fn with_description(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.description = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds a syntactically valid MIME type.
    pub fn with_mime_type(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.mime_type = Some(Arc::from(validate_mime(value.into())?));
        Ok(self)
    }

    /// Requires every listed scope for template discovery.
    pub fn with_required_scopes(
        mut self,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, McpServerResourceDefinitionError> {
        self.required_scopes = validate_scopes(scopes)?;
        Ok(self)
    }

    /// Returns the exact template string.
    #[must_use]
    pub fn uri_template(&self) -> &str {
        &self.uri_template
    }

    /// Returns required scopes in canonical order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(AsRef::as_ref)
    }

    fn to_protocol(&self) -> ResourceTemplate {
        let mut template =
            ResourceTemplate::new(self.uri_template.to_string(), self.name.to_string());
        template.title = self.title.as_deref().map(str::to_owned);
        template.description = self.description.as_deref().map(str::to_owned);
        template.mime_type = self.mime_type.as_deref().map(str::to_owned);
        template
    }
}

/// Invalid Resource or template definition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceDefinitionError {
    /// URI must be absolute, bounded, and control-free.
    #[error("invalid MCP resource URI")]
    InvalidUri,
    /// Template syntax was malformed or not absolute.
    #[error("invalid MCP resource URI template")]
    InvalidUriTemplate,
    /// A programmatic name was empty, padded, oversized, or control-containing.
    #[error("invalid MCP resource name")]
    InvalidName,
    /// Display text was empty, padded, oversized, or control-containing.
    #[error("invalid MCP resource display text")]
    InvalidDisplayText,
    /// MIME type syntax was invalid.
    #[error("invalid MCP resource MIME type")]
    InvalidMimeType,
    /// Too many required scopes were supplied.
    #[error("too many required MCP resource scopes")]
    TooManyScopes,
    /// One required scope was invalid.
    #[error("invalid required MCP resource scope")]
    InvalidScope,
    /// One required scope was duplicated.
    #[error("duplicate required MCP resource scope")]
    DuplicateScope,
}

fn validate_uri(value: &str) -> Result<(), McpServerResourceDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_URI_BYTES
        || value.chars().any(char::is_control)
        || value
            .parse::<http::Uri>()
            .map_or(true, |uri| uri.scheme().is_none())
    {
        return Err(McpServerResourceDefinitionError::InvalidUri);
    }
    Ok(())
}

fn validate_uri_template(value: &str) -> Result<(), McpServerResourceDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_URI_BYTES
        || value.chars().any(char::is_control)
        || !value.contains('{')
    {
        return Err(McpServerResourceDefinitionError::InvalidUriTemplate);
    }
    let prefix = value.split('{').next().unwrap_or_default();
    if prefix
        .parse::<http::Uri>()
        .map_or(true, |uri| uri.scheme().is_none())
    {
        return Err(McpServerResourceDefinitionError::InvalidUriTemplate);
    }
    let mut inside = false;
    let mut expression_len = 0_usize;
    for character in value.chars() {
        match character {
            '{' if !inside => {
                inside = true;
                expression_len = 0;
            }
            '}' if inside && expression_len > 0 => inside = false,
            '{' | '}' => return Err(McpServerResourceDefinitionError::InvalidUriTemplate),
            _ if inside => {
                if !(character.is_ascii_alphanumeric()
                    || matches!(
                        character,
                        '_' | '.' | ',' | ':' | '*' | '+' | '#' | '/' | ';' | '?' | '&' | '%'
                    ))
                {
                    return Err(McpServerResourceDefinitionError::InvalidUriTemplate);
                }
                expression_len += 1;
            }
            _ => {}
        }
    }
    if inside {
        return Err(McpServerResourceDefinitionError::InvalidUriTemplate);
    }
    Ok(())
}

fn validate_name(value: &str) -> Result<(), McpServerResourceDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_NAME_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(McpServerResourceDefinitionError::InvalidName);
    }
    Ok(())
}

fn validate_display(value: String) -> Result<String, McpServerResourceDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_DISPLAY_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(McpServerResourceDefinitionError::InvalidDisplayText);
    }
    Ok(value)
}

fn validate_mime(value: String) -> Result<String, McpServerResourceDefinitionError> {
    if value.len() > 255 || value.trim() != value || value.parse::<mime::Mime>().is_err() {
        return Err(McpServerResourceDefinitionError::InvalidMimeType);
    }
    Ok(value)
}

fn validate_scopes(
    scopes: impl IntoIterator<Item = impl Into<String>>,
) -> Result<Arc<[Box<str>]>, McpServerResourceDefinitionError> {
    let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
    if scopes.len() > MAX_SCOPE_COUNT {
        return Err(McpServerResourceDefinitionError::TooManyScopes);
    }
    if scopes.iter().any(|scope| {
        scope.is_empty()
            || scope.len() > MAX_SCOPE_BYTES
            || scope
                .bytes()
                .any(|byte| !matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
    }) {
        return Err(McpServerResourceDefinitionError::InvalidScope);
    }
    scopes.sort_unstable();
    if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(McpServerResourceDefinitionError::DuplicateScope);
    }
    Ok(scopes
        .into_iter()
        .map(String::into_boxed_str)
        .collect::<Vec<_>>()
        .into())
}

/// Resource-count ceilings for a frozen catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpServerResourceCatalogLimits {
    maximum_resources: usize,
    maximum_templates: usize,
}

impl McpServerResourceCatalogLimits {
    /// Absolute ceiling for either catalog class.
    pub const HARD_MAXIMUM_ENTRIES: usize = 4096;

    /// Constructs positive limits within the hard ceiling.
    pub const fn new(
        maximum_resources: usize,
        maximum_templates: usize,
    ) -> Result<Self, McpServerResourceCatalogLimitsError> {
        if maximum_resources == 0 || maximum_templates == 0 {
            return Err(McpServerResourceCatalogLimitsError::ZeroLimit);
        }
        if maximum_resources > Self::HARD_MAXIMUM_ENTRIES
            || maximum_templates > Self::HARD_MAXIMUM_ENTRIES
        {
            return Err(McpServerResourceCatalogLimitsError::AboveHardMaximum);
        }
        Ok(Self {
            maximum_resources,
            maximum_templates,
        })
    }
}

impl Default for McpServerResourceCatalogLimits {
    fn default() -> Self {
        Self {
            maximum_resources: 1024,
            maximum_templates: 1024,
        }
    }
}

/// Invalid Resource catalog limits.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceCatalogLimitsError {
    /// Counts must be positive.
    #[error("MCP resource catalog limits must be positive")]
    ZeroLimit,
    /// A count exceeded the implementation ceiling.
    #[error("MCP resource catalog limit exceeds the hard maximum")]
    AboveHardMaximum,
}

/// Startup-only builder for an immutable Resource catalog.
#[derive(Debug)]
pub struct McpServerResourceCatalogBuilder {
    limits: McpServerResourceCatalogLimits,
    resources: BTreeMap<String, McpServerResourceDefinition>,
    templates: BTreeMap<String, McpServerResourceTemplateDefinition>,
}

impl McpServerResourceCatalogBuilder {
    /// Creates an empty catalog builder.
    #[must_use]
    pub fn new(limits: McpServerResourceCatalogLimits) -> Self {
        Self {
            limits,
            resources: BTreeMap::new(),
            templates: BTreeMap::new(),
        }
    }

    /// Registers one exact Resource.
    pub fn register_resource(
        &mut self,
        definition: McpServerResourceDefinition,
    ) -> Result<(), McpServerResourceCatalogError> {
        if self.resources.len() == self.limits.maximum_resources {
            return Err(McpServerResourceCatalogError::TooManyResources);
        }
        if self.resources.contains_key(definition.uri()) {
            return Err(McpServerResourceCatalogError::DuplicateResource);
        }
        self.resources
            .insert(definition.uri().to_owned(), definition);
        Ok(())
    }

    /// Registers one Resource template.
    pub fn register_template(
        &mut self,
        definition: McpServerResourceTemplateDefinition,
    ) -> Result<(), McpServerResourceCatalogError> {
        if self.templates.len() == self.limits.maximum_templates {
            return Err(McpServerResourceCatalogError::TooManyTemplates);
        }
        if self.templates.contains_key(definition.uri_template()) {
            return Err(McpServerResourceCatalogError::DuplicateTemplate);
        }
        self.templates
            .insert(definition.uri_template().to_owned(), definition);
        Ok(())
    }

    /// Freezes a non-empty, stable-order catalog.
    pub fn build(self) -> Result<McpServerResourceCatalog, McpServerResourceCatalogError> {
        if self.resources.is_empty() && self.templates.is_empty() {
            return Err(McpServerResourceCatalogError::Empty);
        }
        let mut digest_material = Vec::new();
        for definition in self.resources.values() {
            append_digest_part(&mut digest_material, definition.uri().as_bytes());
            append_digest_part(&mut digest_material, definition.name().as_bytes());
        }
        for definition in self.templates.values() {
            append_digest_part(&mut digest_material, definition.uri_template().as_bytes());
        }
        let digest = Digest::sha256(digest_material).to_string();
        Ok(McpServerResourceCatalog {
            inner: Arc::new(McpServerResourceCatalogInner {
                resources: self.resources,
                templates: self.templates,
                digest: Arc::from(
                    digest
                        .strip_prefix("sha256:")
                        .expect("StateKnot SHA-256 digest has stable prefix"),
                ),
            }),
        })
    }
}

impl Default for McpServerResourceCatalogBuilder {
    fn default() -> Self {
        Self::new(McpServerResourceCatalogLimits::default())
    }
}

/// Immutable Resource and template metadata.
#[derive(Clone)]
pub struct McpServerResourceCatalog {
    inner: Arc<McpServerResourceCatalogInner>,
}

struct McpServerResourceCatalogInner {
    resources: BTreeMap<String, McpServerResourceDefinition>,
    templates: BTreeMap<String, McpServerResourceTemplateDefinition>,
    digest: Arc<str>,
}

impl McpServerResourceCatalog {
    /// Returns an exact static Resource definition.
    #[must_use]
    pub fn get(&self, uri: &str) -> Option<&McpServerResourceDefinition> {
        self.inner.resources.get(uri)
    }

    /// Returns the number of exact Resources.
    #[must_use]
    pub fn resource_count(&self) -> usize {
        self.inner.resources.len()
    }

    /// Returns the number of templates.
    #[must_use]
    pub fn template_count(&self) -> usize {
        self.inner.templates.len()
    }

    fn contains_scoped_entries(&self) -> bool {
        self.inner
            .resources
            .values()
            .any(|value| !value.required_scopes.is_empty())
            || self
                .inner
                .templates
                .values()
                .any(|value| !value.required_scopes.is_empty())
    }
}

impl fmt::Debug for McpServerResourceCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerResourceCatalog")
            .field("resources", &self.resource_count())
            .field("templates", &self.template_count())
            .field("catalog_digest", &self.inner.digest)
            .finish()
    }
}

/// Invalid catalog mutation or build.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceCatalogError {
    /// Neither Resources nor templates were registered.
    #[error("MCP resource catalog is empty")]
    Empty,
    /// An exact URI appeared twice.
    #[error("duplicate MCP resource URI")]
    DuplicateResource,
    /// A URI template appeared twice.
    #[error("duplicate MCP resource template")]
    DuplicateTemplate,
    /// Exact Resource count reached its configured ceiling.
    #[error("too many MCP resources")]
    TooManyResources,
    /// Template count reached its configured ceiling.
    #[error("too many MCP resource templates")]
    TooManyTemplates,
}

/// One bounded Resource content item.
#[derive(Clone, Debug)]
pub struct McpServerResourceContent {
    inner: ResourceContents,
    wire_bytes: usize,
}

impl McpServerResourceContent {
    /// Creates bounded text content.
    pub fn text(
        uri: impl Into<String>,
        mime_type: Option<impl Into<String>>,
        text: impl Into<String>,
    ) -> Result<Self, McpServerResourceContentError> {
        let uri = uri.into();
        let text = text.into();
        validate_content_uri(&uri)?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(McpServerResourceContentError::TooLarge);
        }
        let mime_type = mime_type
            .map(Into::into)
            .map(validate_content_mime)
            .transpose()?;
        Self::from_protocol(ResourceContents::TextResourceContents {
            uri,
            mime_type,
            text,
            meta: None,
        })
    }

    /// Creates bounded blob content with canonical standard Base64.
    pub fn blob(
        uri: impl Into<String>,
        mime_type: Option<impl Into<String>>,
        bytes: impl AsRef<[u8]>,
    ) -> Result<Self, McpServerResourceContentError> {
        let uri = uri.into();
        validate_content_uri(&uri)?;
        let bytes = bytes.as_ref();
        if bytes.len() > MAX_BINARY_BYTES {
            return Err(McpServerResourceContentError::TooLarge);
        }
        let mime_type = mime_type
            .map(Into::into)
            .map(validate_content_mime)
            .transpose()?;
        Self::from_protocol(ResourceContents::BlobResourceContents {
            uri,
            mime_type,
            blob: STANDARD.encode(bytes),
            meta: None,
        })
    }

    /// Validates Resource content represented as protocol JSON.
    pub fn try_from_protocol_json(value: Value) -> Result<Self, McpServerResourceContentError> {
        if canonical_len(&value).map_or(true, |bytes| bytes > MAX_RESULT_BYTES) {
            return Err(McpServerResourceContentError::TooLarge);
        }
        let inner =
            serde_json::from_value(value).map_err(|_| McpServerResourceContentError::Invalid)?;
        Self::from_protocol(inner)
    }

    fn from_protocol(inner: ResourceContents) -> Result<Self, McpServerResourceContentError> {
        match &inner {
            ResourceContents::TextResourceContents {
                uri,
                mime_type,
                text,
                ..
            } => {
                validate_content_uri(uri)?;
                if text.len() > MAX_TEXT_BYTES {
                    return Err(McpServerResourceContentError::TooLarge);
                }
                if let Some(mime_type) = mime_type {
                    validate_content_mime(mime_type.clone())?;
                }
            }
            ResourceContents::BlobResourceContents {
                uri,
                mime_type,
                blob,
                ..
            } => {
                validate_content_uri(uri)?;
                if let Some(mime_type) = mime_type {
                    validate_content_mime(mime_type.clone())?;
                }
                let decoded = STANDARD
                    .decode(blob)
                    .map_err(|_| McpServerResourceContentError::Invalid)?;
                if decoded.len() > MAX_BINARY_BYTES {
                    return Err(McpServerResourceContentError::TooLarge);
                }
            }
            _ => return Err(McpServerResourceContentError::Invalid),
        }
        let value =
            serde_json::to_value(&inner).map_err(|_| McpServerResourceContentError::Invalid)?;
        let wire_bytes =
            canonical_len(&value).map_err(|_| McpServerResourceContentError::Invalid)?;
        Ok(Self { inner, wire_bytes })
    }
}

/// Invalid or oversized Resource content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceContentError {
    /// URI, MIME, Base64, or wire shape was invalid.
    #[error("invalid MCP resource content")]
    Invalid,
    /// Content exceeded a hard byte ceiling.
    #[error("MCP resource content is too large")]
    TooLarge,
}

fn validate_content_uri(value: &str) -> Result<(), McpServerResourceContentError> {
    validate_uri(value).map_err(|_| McpServerResourceContentError::Invalid)
}

fn validate_content_mime(value: String) -> Result<String, McpServerResourceContentError> {
    validate_mime(value).map_err(|_| McpServerResourceContentError::Invalid)
}

/// Bounded complete Resource read result.
#[derive(Clone, Debug)]
pub struct McpServerResourceResult {
    contents: Arc<[McpServerResourceContent]>,
    ttl_ms: u64,
    cache_scope: McpServerCacheScope,
}

impl McpServerResourceResult {
    /// Creates a non-empty result with explicit cache policy.
    pub fn new(
        contents: impl IntoIterator<Item = McpServerResourceContent>,
        cache_ttl: Duration,
        cache_scope: McpServerCacheScope,
    ) -> Result<Self, McpServerResourceResultError> {
        let contents = contents.into_iter().collect::<Vec<_>>();
        if contents.is_empty() {
            return Err(McpServerResourceResultError::Empty);
        }
        if contents.len() > MAX_CONTENTS {
            return Err(McpServerResourceResultError::TooManyContents);
        }
        if contents
            .iter()
            .try_fold(0_usize, |total, item| total.checked_add(item.wire_bytes))
            .is_none_or(|total| total > MAX_RESULT_BYTES)
        {
            return Err(McpServerResourceResultError::TooLarge);
        }
        if cache_ttl > McpServerApplicationOptions::HARD_MAXIMUM_CACHE_TTL {
            return Err(McpServerResourceResultError::InvalidCacheTtl);
        }
        let ttl_ms = u64::try_from(cache_ttl.as_millis())
            .map_err(|_| McpServerResourceResultError::InvalidCacheTtl)?;
        Ok(Self {
            contents: contents.into(),
            ttl_ms,
            cache_scope,
        })
    }

    fn into_protocol(self) -> ReadResourceResult {
        ReadResourceResult::new(
            self.contents
                .iter()
                .map(|value| value.inner.clone())
                .collect(),
        )
        .with_ttl_ms(self.ttl_ms)
        .with_cache_scope(self.cache_scope.into())
    }
}

/// Invalid Resource result.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceResultError {
    /// A successful read must contain at least one item.
    #[error("MCP resource result is empty")]
    Empty,
    /// Too many content items were returned.
    #[error("too many MCP resource contents")]
    TooManyContents,
    /// Aggregate output exceeded a hard byte ceiling.
    #[error("MCP resource result is too large")]
    TooLarge,
    /// Cache TTL exceeded the hard ceiling.
    #[error("invalid MCP resource cache TTL")]
    InvalidCacheTtl,
}

/// Complete or input-required Resource outcome.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum McpServerResourceOutcome {
    /// Resource read completed.
    Complete(McpServerResourceResult),
    /// Client-side MRTR input is required.
    InputRequired(McpServerInputRequired),
}

impl From<McpServerResourceResult> for McpServerResourceOutcome {
    fn from(value: McpServerResourceResult) -> Self {
        Self::Complete(value)
    }
}

impl From<McpServerInputRequired> for McpServerResourceOutcome {
    fn from(value: McpServerInputRequired) -> Self {
        Self::InputRequired(value)
    }
}

/// StateKnot-owned inbound Resource read.
#[derive(Clone, Debug)]
pub struct McpServerResourceRead {
    uri: Arc<str>,
    input_responses: Arc<BTreeMap<String, Value>>,
    request_state: Option<Arc<str>>,
}

impl McpServerResourceRead {
    /// Returns the requested URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns untrusted MRTR responses.
    #[must_use]
    pub fn input_responses(&self) -> &BTreeMap<String, Value> {
        &self.input_responses
    }

    /// Returns untrusted echoed request state.
    #[must_use]
    pub fn request_state(&self) -> Option<&str> {
        self.request_state.as_deref()
    }
}

/// Authenticated Resource execution context.
#[derive(Clone, Debug)]
pub struct McpServerResourceContext {
    principal: McpServerPrincipal,
    client_capabilities: Arc<Value>,
    cancellation: CancellationToken,
    request_state_binding: Arc<[u8]>,
}

impl McpServerResourceContext {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns request-scoped client capabilities.
    #[must_use]
    pub fn client_capabilities(&self) -> &Value {
        &self.client_capabilities
    }

    /// Returns principal/operation/URI-bound request-state associated data.
    #[must_use]
    pub fn request_state_binding(&self) -> &[u8] {
        &self.request_state_binding
    }

    /// Returns whether cancellation was observed.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Waits for cooperative cancellation.
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}

/// Reads exact or dynamically routed Resources.
pub trait McpServerResourceReader: Send + Sync + 'static {
    /// Reads one authorized, bounded URI.
    fn read(
        &self,
        request: McpServerResourceRead,
        context: McpServerResourceContext,
    ) -> BoxFuture<'_, Result<McpServerResourceOutcome, McpServerResourceReaderError>>;
}

/// Reader failure category without arbitrary diagnostic leakage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceReaderError {
    /// The URI is not available in this authorization context.
    #[error("MCP resource was not found")]
    NotFound,
    /// A dependency is temporarily unavailable.
    #[error("MCP resource dependency is unavailable")]
    Unavailable,
    /// A reader invariant failed.
    #[error("MCP resource read failed internally")]
    Internal,
    /// Cooperative cancellation was observed.
    #[error("MCP resource read was cancelled")]
    Cancelled,
}

/// Owned facts for decoded Resource authorization.
#[derive(Clone, Debug)]
pub struct McpServerResourceAuthorizationRequest {
    principal: McpServerPrincipal,
    request: McpServerResourceRead,
    exact_definition: Option<McpServerResourceDefinition>,
}

impl McpServerResourceAuthorizationRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the untrusted requested URI and MRTR data.
    #[must_use]
    pub const fn request(&self) -> &McpServerResourceRead {
        &self.request
    }

    /// Returns an exact registered definition, if one exists.
    #[must_use]
    pub const fn exact_definition(&self) -> Option<&McpServerResourceDefinition> {
        self.exact_definition.as_ref()
    }
}

/// Decoded Resource authorization policy.
pub trait McpServerResourceAuthorization: Send + Sync + 'static {
    /// Authorizes before existence is disclosed or reader code runs.
    fn authorize(
        &self,
        request: McpServerResourceAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerResourceAuthorizationError>>;
}

/// Explicit policy that allows every scope-qualified read.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowMcpServerResourceAuthorization;

impl McpServerResourceAuthorization for AllowMcpServerResourceAuthorization {
    fn authorize(
        &self,
        _request: McpServerResourceAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerResourceAuthorizationError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Public-safe Resource authorization failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceAuthorizationError {
    /// Policy denied the read.
    #[error("MCP resource read is forbidden")]
    Forbidden,
    /// The policy authority is unavailable.
    #[error("MCP resource authorization is unavailable")]
    Unavailable,
}

/// Resource-only production handler. Use the composite application to expose
/// Resources together with Tools and Prompts.
#[derive(Clone)]
pub struct McpServerResourceService {
    catalog: McpServerResourceCatalog,
    options: McpServerApplicationOptions,
    reader: Arc<dyn McpServerResourceReader>,
    authorization: Arc<dyn McpServerResourceAuthorization>,
}

impl McpServerResourceService {
    /// Creates a service with explicit reader and authorization policy.
    pub fn new<R, A>(
        catalog: McpServerResourceCatalog,
        options: McpServerApplicationOptions,
        reader: R,
        authorization: A,
    ) -> Result<Self, McpServerResourceServiceBuildError>
    where
        R: McpServerResourceReader,
        A: McpServerResourceAuthorization,
    {
        Self::with_shared(catalog, options, Arc::new(reader), Arc::new(authorization))
    }

    /// Creates a service with already shared boundaries.
    pub fn with_shared(
        catalog: McpServerResourceCatalog,
        options: McpServerApplicationOptions,
        reader: Arc<dyn McpServerResourceReader>,
        authorization: Arc<dyn McpServerResourceAuthorization>,
    ) -> Result<Self, McpServerResourceServiceBuildError> {
        if catalog.contains_scoped_entries()
            && matches!(options.cache_scope, McpServerCacheScope::Public)
        {
            return Err(McpServerResourceServiceBuildError::PublicCacheWithScopedEntries);
        }
        Ok(Self {
            catalog,
            options,
            reader,
            authorization,
        })
    }

    fn scope_tag(&self, principal: &McpServerPrincipal) -> String {
        if matches!(self.options.cache_scope, McpServerCacheScope::Public) {
            return "public".to_owned();
        }
        let mut material = principal.subject().as_bytes().to_vec();
        for scope in principal.scopes() {
            append_digest_part(&mut material, scope.as_bytes());
        }
        digest_hex(material)
    }

    fn page_cursor(&self, kind: &str, scope: &str, offset: usize) -> String {
        format!(
            "v1.{kind}.{}.{}.{}",
            self.catalog.inner.digest, scope, offset
        )
    }

    fn parse_cursor(&self, cursor: &str, kind: &str, scope: &str) -> Result<usize, ErrorData> {
        if cursor.len() > 224 {
            return Err(invalid_cursor());
        }
        let mut parts = cursor.split('.');
        let valid = parts.next() == Some("v1")
            && parts.next() == Some(kind)
            && parts.next() == Some(self.catalog.inner.digest.as_ref())
            && parts.next() == Some(scope);
        let offset = parts.next().and_then(|value| value.parse().ok());
        if !valid || offset.is_none() || parts.next().is_some() {
            return Err(invalid_cursor());
        }
        Ok(offset.expect("validated cursor offset exists"))
    }

    fn resources_page(
        &self,
        principal: &McpServerPrincipal,
        cursor: Option<&str>,
    ) -> Result<(Vec<Resource>, Option<String>), ErrorData> {
        let scope = self.scope_tag(principal);
        let visible = self
            .catalog
            .inner
            .resources
            .values()
            .filter(|definition| {
                definition
                    .required_scopes()
                    .all(|required| principal.has_scope(required))
            })
            .collect::<Vec<_>>();
        let start = cursor.map_or(Ok(0), |value| self.parse_cursor(value, "resources", &scope))?;
        if start > visible.len() {
            return Err(invalid_cursor());
        }
        let end = start
            .saturating_add(self.options.page_size)
            .min(visible.len());
        let values = visible[start..end]
            .iter()
            .map(|value| value.to_protocol())
            .collect();
        let next = (end < visible.len()).then(|| self.page_cursor("resources", &scope, end));
        Ok((values, next))
    }

    fn templates_page(
        &self,
        principal: &McpServerPrincipal,
        cursor: Option<&str>,
    ) -> Result<(Vec<ResourceTemplate>, Option<String>), ErrorData> {
        let scope = self.scope_tag(principal);
        let visible = self
            .catalog
            .inner
            .templates
            .values()
            .filter(|definition| {
                definition
                    .required_scopes()
                    .all(|required| principal.has_scope(required))
            })
            .collect::<Vec<_>>();
        let start = cursor.map_or(Ok(0), |value| self.parse_cursor(value, "templates", &scope))?;
        if start > visible.len() {
            return Err(invalid_cursor());
        }
        let end = start
            .saturating_add(self.options.page_size)
            .min(visible.len());
        let values = visible[start..end]
            .iter()
            .map(|value| value.to_protocol())
            .collect();
        let next = (end < visible.len()).then(|| self.page_cursor("templates", &scope, end));
        Ok((values, next))
    }

    async fn dispatch(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        validate_uri(&request.uri).map_err(|_| resource_not_found())?;
        let principal = mcp_server_principal(&context)
            .cloned()
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let exact = self.catalog.get(&request.uri).cloned();
        if exact.as_ref().is_some_and(|definition| {
            definition
                .required_scopes()
                .any(|scope| !principal.has_scope(scope))
        }) {
            return Err(resource_not_found());
        }
        let input_responses = request.input_responses.unwrap_or_default();
        validate_inbound_mrtr(&input_responses, request.request_state.as_deref())?;
        let read = McpServerResourceRead {
            uri: Arc::from(request.uri),
            input_responses: Arc::new(input_responses),
            request_state: request.request_state.map(Arc::from),
        };
        let authorization = McpServerResourceAuthorizationRequest {
            principal: principal.clone(),
            request: read.clone(),
            exact_definition: exact,
        };
        match self.authorization.authorize(authorization).await {
            Ok(()) => {}
            Err(McpServerResourceAuthorizationError::Forbidden) => {
                return Err(resource_not_found());
            }
            Err(McpServerResourceAuthorizationError::Unavailable) => {
                return Err(ErrorData::internal_error(
                    "MCP resource authorization is unavailable",
                    None,
                ));
            }
        }
        let execution_context = resource_context(&context, principal, read.uri())?;
        match self.reader.read(read, execution_context).await {
            Ok(McpServerResourceOutcome::Complete(result)) => Ok(result.into_protocol().into()),
            Ok(McpServerResourceOutcome::InputRequired(result)) => {
                Ok(result.into_protocol().into())
            }
            Err(McpServerResourceReaderError::NotFound) => Err(resource_not_found()),
            Err(McpServerResourceReaderError::Unavailable) => Err(ErrorData::internal_error(
                "MCP resource dependency is unavailable",
                None,
            )),
            Err(McpServerResourceReaderError::Internal) => {
                Err(ErrorData::internal_error("MCP resource read failed", None))
            }
            Err(McpServerResourceReaderError::Cancelled) => Err(ErrorData::internal_error(
                "MCP resource read was cancelled",
                None,
            )),
        }
    }
}

impl fmt::Debug for McpServerResourceService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerResourceService")
            .field("catalog", &self.catalog)
            .field("options", &self.options)
            .field("reader", &"[READER]")
            .field("authorization", &"[POLICY]")
            .finish_non_exhaustive()
    }
}

impl ServerHandler for McpServerResourceService {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_info(&self) -> ServerInfo {
        let mut capabilities = ServerCapabilities::default();
        capabilities.resources = Some(ResourcesCapability::default());
        let mut info = ServerInfo::new(capabilities).with_server_info(Implementation::new(
            self.options.server_name.to_string(),
            self.options.server_version.to_string(),
        ));
        if let Some(instructions) = &self.options.instructions {
            info = info.with_instructions(instructions.to_string());
        }
        info
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        Ok(
            DiscoverResult::from_server_info(vec![ProtocolVersion::V_2026_07_28], self.get_info())
                .with_ttl_ms(self.options.cache_ttl_ms)
                .with_cache_scope(self.options.cache_scope.into()),
        )
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let principal = mcp_server_principal(&context)
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let (resources, next_cursor) = self.resources_page(
            principal,
            request.as_ref().and_then(|value| value.cursor.as_deref()),
        )?;
        let mut result = ListResourcesResult::with_all_items(resources)
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into());
        result.next_cursor = next_cursor;
        Ok(result)
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let principal = mcp_server_principal(&context)
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let (templates, next_cursor) = self.templates_page(
            principal,
            request.as_ref().and_then(|value| value.cursor.as_deref()),
        )?;
        let mut result = ListResourceTemplatesResult::with_all_items(templates)
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into());
        result.next_cursor = next_cursor;
        Ok(result)
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        self.dispatch(request, context).await
    }
}

/// Invalid Resource service configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerResourceServiceBuildError {
    /// Scope-filtered entries require principal-private caching.
    #[error("scope-restricted MCP resources require a private catalog cache")]
    PublicCacheWithScopedEntries,
}

fn resource_context(
    context: &RequestContext<RoleServer>,
    principal: McpServerPrincipal,
    uri: &str,
) -> Result<McpServerResourceContext, ErrorData> {
    let capabilities = serde_json::to_value(context.client_capabilities().unwrap_or_default())
        .map_err(|_| ErrorData::internal_error("Failed to retain client capabilities", None))?;
    let mut binding = b"stateknot/mcp-server/request-state/resource/v1".to_vec();
    append_digest_part(&mut binding, principal.subject().as_bytes());
    append_digest_part(&mut binding, uri.as_bytes());
    Ok(McpServerResourceContext {
        principal,
        client_capabilities: Arc::new(capabilities),
        cancellation: context.ct.clone(),
        request_state_binding: Digest::sha256(binding).as_bytes().to_vec().into(),
    })
}

fn digest_hex(value: impl AsRef<[u8]>) -> String {
    let digest = Digest::sha256(value).to_string();
    digest
        .strip_prefix("sha256:")
        .expect("StateKnot SHA-256 digest has stable prefix")
        .to_owned()
}

fn invalid_cursor() -> ErrorData {
    ErrorData::invalid_params("Invalid resource catalog cursor", None)
}

fn resource_not_found() -> ErrorData {
    ErrorData::resource_not_found("Resource not found or unavailable", None)
}

fn canonical_len(value: &Value) -> Result<usize, serde_json::Error> {
    serde_json_canonicalizer::to_vec(value).map(|bytes| bytes.len())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use http::{Request, StatusCode};
    use http_body_util::{BodyExt as _, Full};
    use serde_json::json;
    use tower_service::Service as _;

    use super::*;
    use crate::{McpServerAuthentication, McpServerHttpOptions, McpServerHttpService};

    #[derive(Clone)]
    struct StaticReader {
        calls: Arc<AtomicUsize>,
    }

    impl McpServerResourceReader for StaticReader {
        fn read(
            &self,
            request: McpServerResourceRead,
            execution_context: McpServerResourceContext,
        ) -> BoxFuture<'_, Result<McpServerResourceOutcome, McpServerResourceReaderError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let uri = request.uri().to_owned();
            let subject = execution_context.principal().subject().to_owned();
            Box::pin(async move {
                let item = McpServerResourceContent::text(
                    uri,
                    Some("text/plain"),
                    format!("hello {subject}"),
                )
                .map_err(|_| McpServerResourceReaderError::Internal)?;
                McpServerResourceResult::new(
                    [item],
                    Duration::from_secs(60),
                    McpServerCacheScope::Private,
                )
                .map(Into::into)
                .map_err(|_| McpServerResourceReaderError::Internal)
            })
        }
    }

    fn service(calls: &Arc<AtomicUsize>) -> McpServerHttpService<McpServerResourceService> {
        let mut builder = McpServerResourceCatalogBuilder::default();
        builder
            .register_resource(McpServerResourceDefinition::new("test://alpha", "Alpha").unwrap())
            .unwrap();
        builder
            .register_template(
                McpServerResourceTemplateDefinition::new("test://items/{id}", "Item").unwrap(),
            )
            .unwrap();
        let options = McpServerApplicationOptions::new(
            "stateknot-resource-test",
            "0.0.0",
            1,
            Duration::from_secs(60),
            McpServerCacheScope::Private,
        )
        .unwrap();
        let handler = McpServerResourceService::new(
            builder.build().unwrap(),
            options,
            StaticReader {
                calls: calls.clone(),
            },
            AllowMcpServerResourceAuthorization,
        )
        .unwrap();
        McpServerHttpService::new(
            handler,
            McpServerHttpOptions::loopback(32125).unwrap(),
            McpServerAuthentication::anonymous_loopback(),
        )
        .unwrap()
    }

    fn request(method: &str, mut params: Value) -> Request<Full<Bytes>> {
        let name = params.get("uri").and_then(Value::as_str).map(str::to_owned);
        params.as_object_mut().unwrap().insert(
            "_meta".to_owned(),
            json!({
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "0" },
                "io.modelcontextprotocol/clientCapabilities": {}
            }),
        );
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        }))
        .unwrap();
        let mut builder = Request::builder()
            .method("POST")
            .uri("http://127.0.0.1:32125/mcp")
            .header("host", "127.0.0.1:32125")
            .header("origin", "http://127.0.0.1:32125")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method);
        if let Some(name) = name {
            builder = builder.header("mcp-name", name);
        }
        builder.body(Full::new(Bytes::from(body))).unwrap()
    }

    async fn invoke(
        service: &mut McpServerHttpService<McpServerResourceService>,
        method: &str,
        params: Value,
    ) -> (StatusCode, Value) {
        let response = service.call(request(method, params)).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[test]
    fn catalog_rejects_duplicates_and_malformed_templates() {
        assert!(matches!(
            McpServerResourceTemplateDefinition::new("relative/{id}", "bad"),
            Err(McpServerResourceDefinitionError::InvalidUriTemplate)
        ));
        let mut builder = McpServerResourceCatalogBuilder::default();
        let resource = McpServerResourceDefinition::new("test://same", "Same").unwrap();
        builder.register_resource(resource.clone()).unwrap();
        assert!(matches!(
            builder.register_resource(resource),
            Err(McpServerResourceCatalogError::DuplicateResource)
        ));
    }

    #[tokio::test]
    async fn resource_catalog_and_reads_pass_through_the_http_boundary() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(&calls);
        let (status, listed) = invoke(&mut service, "resources/list", json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            listed.pointer("/result/resources/0/uri"),
            Some(&json!("test://alpha"))
        );
        let (status, read) = invoke(
            &mut service,
            "resources/read",
            json!({ "uri": "test://alpha" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            read.pointer("/result/contents/0/text"),
            Some(&json!("hello anonymous"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
