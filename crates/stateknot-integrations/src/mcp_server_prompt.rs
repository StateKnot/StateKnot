// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable, authorization-first MCP 2026-07-28 Prompt and Completion surface.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    sync::Arc,
};

use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CompleteRequestParams, CompleteResult, CompletionInfo, DiscoverResult,
        GetPromptRequestParams, GetPromptResponse, GetPromptResult, Implementation,
        ListPromptsResult, PaginatedRequestParams, Prompt, PromptArgument, PromptMessage,
        PromptsCapability, ProtocolVersion, Reference, Role, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
};
use serde_json::{Map, Value};
use stateknot_core::{BoxFuture, Digest};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    McpServerApplicationOptions, McpServerCacheScope, McpServerContent, McpServerInputRequired,
    McpServerPrincipal, mcp_server_principal,
    mcp_server_tool::{append_digest_part, validate_inbound_mrtr},
};

const MEBIBYTE: usize = 1024 * 1024;
const MAX_NAME_BYTES: usize = 64;
const MAX_DISPLAY_BYTES: usize = 16 * 1024;
const MAX_ARGUMENTS: usize = 128;
const MAX_ARGUMENT_VALUE_BYTES: usize = 64 * 1024;
const MAX_SCOPE_COUNT: usize = 128;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_MESSAGES: usize = 256;
const MAX_RESULT_BYTES: usize = 32 * MEBIBYTE;
const MAX_COMPLETION_VALUES: usize = 100;
const MAX_COMPLETION_VALUE_BYTES: usize = 4096;

/// One bounded Prompt argument definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerPromptArgument {
    name: Arc<str>,
    title: Option<Arc<str>>,
    description: Option<Arc<str>>,
    required: bool,
}

impl McpServerPromptArgument {
    /// Creates a portable argument name.
    pub fn new(name: impl Into<String>) -> Result<Self, McpServerPromptDefinitionError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self {
            name: Arc::from(name),
            title: None,
            description: None,
            required: false,
        })
    }

    /// Adds a human-readable title.
    pub fn with_title(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerPromptDefinitionError> {
        self.title = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds a human-readable description.
    pub fn with_description(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerPromptDefinitionError> {
        self.description = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Marks the argument as required or optional.
    #[must_use]
    pub const fn with_required(mut self, value: bool) -> Self {
        self.required = value;
        self
    }

    /// Returns the programmatic name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the argument is required.
    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }

    fn to_protocol(&self) -> PromptArgument {
        let mut argument = PromptArgument::new(self.name.to_string()).with_required(self.required);
        argument.title = self.title.as_deref().map(str::to_owned);
        argument.description = self.description.as_deref().map(str::to_owned);
        argument
    }
}

/// One immutable Prompt definition.
#[derive(Clone, Debug)]
pub struct McpServerPromptDefinition {
    name: Arc<str>,
    title: Option<Arc<str>>,
    description: Option<Arc<str>>,
    arguments: Arc<[McpServerPromptArgument]>,
    required_scopes: Arc<[Box<str>]>,
}

impl McpServerPromptDefinition {
    /// Creates a portable Prompt name with no arguments.
    pub fn new(name: impl Into<String>) -> Result<Self, McpServerPromptDefinitionError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self {
            name: Arc::from(name),
            title: None,
            description: None,
            arguments: Arc::from([]),
            required_scopes: Arc::from([]),
        })
    }

    /// Adds a display title.
    pub fn with_title(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerPromptDefinitionError> {
        self.title = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds a display description.
    pub fn with_description(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerPromptDefinitionError> {
        self.description = Some(Arc::from(validate_display(value.into())?));
        Ok(self)
    }

    /// Adds unique bounded argument definitions.
    pub fn with_arguments(
        mut self,
        arguments: impl IntoIterator<Item = McpServerPromptArgument>,
    ) -> Result<Self, McpServerPromptDefinitionError> {
        let arguments = arguments.into_iter().collect::<Vec<_>>();
        if arguments.len() > MAX_ARGUMENTS {
            return Err(McpServerPromptDefinitionError::TooManyArguments);
        }
        let mut names = HashSet::with_capacity(arguments.len());
        if arguments
            .iter()
            .any(|argument| !names.insert(argument.name().to_owned()))
        {
            return Err(McpServerPromptDefinitionError::DuplicateArgument);
        }
        self.arguments = arguments.into();
        Ok(self)
    }

    /// Requires every listed OAuth-style scope for discovery and rendering.
    pub fn with_required_scopes(
        mut self,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, McpServerPromptDefinitionError> {
        self.required_scopes = validate_scopes(scopes)?;
        Ok(self)
    }

    /// Returns the exact Prompt name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns arguments in registered order.
    pub fn arguments(&self) -> impl ExactSizeIterator<Item = &McpServerPromptArgument> {
        self.arguments.iter()
    }

    /// Returns required scopes in canonical order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(AsRef::as_ref)
    }

    fn to_protocol(&self) -> Prompt {
        let arguments = (!self.arguments.is_empty()).then(|| {
            self.arguments
                .iter()
                .map(McpServerPromptArgument::to_protocol)
                .collect()
        });
        let mut prompt = Prompt::new(
            self.name.to_string(),
            self.description.as_deref().map(str::to_owned),
            arguments,
        );
        prompt.title = self.title.as_deref().map(str::to_owned);
        prompt
    }
}

/// Invalid Prompt definition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptDefinitionError {
    /// Name was outside the portable MCP grammar.
    #[error("invalid MCP prompt or argument name")]
    InvalidName,
    /// Display text was empty, padded, oversized, or control-containing.
    #[error("invalid MCP prompt display text")]
    InvalidDisplayText,
    /// Too many arguments were supplied.
    #[error("too many MCP prompt arguments")]
    TooManyArguments,
    /// An argument name appeared twice.
    #[error("duplicate MCP prompt argument")]
    DuplicateArgument,
    /// Too many required scopes were supplied.
    #[error("too many required MCP prompt scopes")]
    TooManyScopes,
    /// A required scope was invalid.
    #[error("invalid required MCP prompt scope")]
    InvalidScope,
    /// A required scope appeared twice.
    #[error("duplicate required MCP prompt scope")]
    DuplicateScope,
}

fn validate_name(value: &str) -> Result<(), McpServerPromptDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_NAME_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'/' | b'-'))
    {
        return Err(McpServerPromptDefinitionError::InvalidName);
    }
    Ok(())
}

fn validate_display(value: String) -> Result<String, McpServerPromptDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_DISPLAY_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(McpServerPromptDefinitionError::InvalidDisplayText);
    }
    Ok(value)
}

fn validate_scopes(
    scopes: impl IntoIterator<Item = impl Into<String>>,
) -> Result<Arc<[Box<str>]>, McpServerPromptDefinitionError> {
    let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
    if scopes.len() > MAX_SCOPE_COUNT {
        return Err(McpServerPromptDefinitionError::TooManyScopes);
    }
    if scopes.iter().any(|scope| {
        scope.is_empty()
            || scope.len() > MAX_SCOPE_BYTES
            || scope
                .bytes()
                .any(|byte| !matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
    }) {
        return Err(McpServerPromptDefinitionError::InvalidScope);
    }
    scopes.sort_unstable();
    if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(McpServerPromptDefinitionError::DuplicateScope);
    }
    Ok(scopes
        .into_iter()
        .map(String::into_boxed_str)
        .collect::<Vec<_>>()
        .into())
}

/// Prompt count ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpServerPromptCatalogLimits {
    maximum_prompts: usize,
}

impl McpServerPromptCatalogLimits {
    /// Absolute Prompt count ceiling.
    pub const HARD_MAXIMUM_PROMPTS: usize = 4096;

    /// Constructs a positive bounded count.
    pub const fn new(maximum_prompts: usize) -> Result<Self, McpServerPromptCatalogLimitsError> {
        if maximum_prompts == 0 {
            return Err(McpServerPromptCatalogLimitsError::ZeroLimit);
        }
        if maximum_prompts > Self::HARD_MAXIMUM_PROMPTS {
            return Err(McpServerPromptCatalogLimitsError::AboveHardMaximum);
        }
        Ok(Self { maximum_prompts })
    }
}

impl Default for McpServerPromptCatalogLimits {
    fn default() -> Self {
        Self {
            maximum_prompts: 1024,
        }
    }
}

/// Invalid Prompt catalog limits.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptCatalogLimitsError {
    /// Prompt count must be positive.
    #[error("MCP prompt catalog limit must be positive")]
    ZeroLimit,
    /// Prompt count exceeded the hard ceiling.
    #[error("MCP prompt catalog limit exceeds the hard maximum")]
    AboveHardMaximum,
}

/// Startup-only Prompt catalog builder.
#[derive(Debug)]
pub struct McpServerPromptCatalogBuilder {
    limits: McpServerPromptCatalogLimits,
    prompts: BTreeMap<String, McpServerPromptDefinition>,
}

impl McpServerPromptCatalogBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new(limits: McpServerPromptCatalogLimits) -> Self {
        Self {
            limits,
            prompts: BTreeMap::new(),
        }
    }

    /// Registers one unique Prompt.
    pub fn register(
        &mut self,
        definition: McpServerPromptDefinition,
    ) -> Result<(), McpServerPromptCatalogError> {
        if self.prompts.len() == self.limits.maximum_prompts {
            return Err(McpServerPromptCatalogError::TooManyPrompts);
        }
        if self.prompts.contains_key(definition.name()) {
            return Err(McpServerPromptCatalogError::DuplicatePrompt);
        }
        self.prompts
            .insert(definition.name().to_owned(), definition);
        Ok(())
    }

    /// Freezes a non-empty stable-order catalog.
    pub fn build(self) -> Result<McpServerPromptCatalog, McpServerPromptCatalogError> {
        if self.prompts.is_empty() {
            return Err(McpServerPromptCatalogError::Empty);
        }
        let mut material = Vec::new();
        for prompt in self.prompts.values() {
            append_digest_part(&mut material, prompt.name().as_bytes());
            for argument in prompt.arguments() {
                append_digest_part(&mut material, argument.name().as_bytes());
                material.push(u8::from(argument.required()));
            }
        }
        let digest = digest_hex(material);
        Ok(McpServerPromptCatalog {
            inner: Arc::new(McpServerPromptCatalogInner {
                prompts: self.prompts,
                digest: Arc::from(digest),
            }),
        })
    }
}

impl Default for McpServerPromptCatalogBuilder {
    fn default() -> Self {
        Self::new(McpServerPromptCatalogLimits::default())
    }
}

/// Immutable Prompt catalog.
#[derive(Clone)]
pub struct McpServerPromptCatalog {
    inner: Arc<McpServerPromptCatalogInner>,
}

struct McpServerPromptCatalogInner {
    prompts: BTreeMap<String, McpServerPromptDefinition>,
    digest: Arc<str>,
}

impl McpServerPromptCatalog {
    /// Returns an exact definition.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&McpServerPromptDefinition> {
        self.inner.prompts.get(name)
    }

    /// Returns the Prompt count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.prompts.len()
    }

    /// Returns whether the catalog is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.prompts.is_empty()
    }

    fn contains_scoped_prompts(&self) -> bool {
        self.inner
            .prompts
            .values()
            .any(|value| !value.required_scopes.is_empty())
    }
}

impl fmt::Debug for McpServerPromptCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerPromptCatalog")
            .field("prompts", &self.len())
            .field("catalog_digest", &self.inner.digest)
            .finish()
    }
}

/// Invalid Prompt catalog mutation or build.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptCatalogError {
    /// At least one Prompt is required.
    #[error("MCP prompt catalog is empty")]
    Empty,
    /// A name appeared twice.
    #[error("duplicate MCP prompt")]
    DuplicatePrompt,
    /// Prompt count reached its configured ceiling.
    #[error("too many MCP prompts")]
    TooManyPrompts,
}

/// Role for one returned Prompt message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptRole {
    /// User message.
    User,
    /// Assistant message.
    Assistant,
}

/// One bounded Prompt message.
#[derive(Clone, Debug)]
pub struct McpServerPromptMessage {
    role: McpServerPromptRole,
    content: McpServerContent,
}

impl McpServerPromptMessage {
    /// Creates a message from validated `StateKnot` content.
    #[must_use]
    pub const fn new(role: McpServerPromptRole, content: McpServerContent) -> Self {
        Self { role, content }
    }

    fn to_protocol(&self) -> PromptMessage {
        let role = match self.role {
            McpServerPromptRole::User => Role::User,
            McpServerPromptRole::Assistant => Role::Assistant,
        };
        PromptMessage::new(role, self.content.protocol_clone())
    }
}

/// Bounded complete Prompt result.
#[derive(Clone, Debug)]
pub struct McpServerPromptResult {
    description: Option<Arc<str>>,
    messages: Arc<[McpServerPromptMessage]>,
}

impl McpServerPromptResult {
    /// Creates a non-empty bounded message sequence.
    pub fn new(
        messages: impl IntoIterator<Item = McpServerPromptMessage>,
    ) -> Result<Self, McpServerPromptResultError> {
        let messages = messages.into_iter().collect::<Vec<_>>();
        if messages.is_empty() {
            return Err(McpServerPromptResultError::Empty);
        }
        if messages.len() > MAX_MESSAGES {
            return Err(McpServerPromptResultError::TooManyMessages);
        }
        if messages
            .iter()
            .try_fold(0_usize, |total, message| {
                total.checked_add(message.content.wire_bytes())
            })
            .is_none_or(|total| total > MAX_RESULT_BYTES)
        {
            return Err(McpServerPromptResultError::TooLarge);
        }
        Ok(Self {
            description: None,
            messages: messages.into(),
        })
    }

    /// Adds a bounded result description.
    pub fn with_description(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, McpServerPromptResultError> {
        self.description =
            Some(Arc::from(validate_display(value.into()).map_err(|_| {
                McpServerPromptResultError::InvalidDescription
            })?));
        Ok(self)
    }

    fn into_protocol(self) -> GetPromptResult {
        let mut result = GetPromptResult::new(
            self.messages
                .iter()
                .map(McpServerPromptMessage::to_protocol)
                .collect(),
        );
        result.description = self.description.as_deref().map(str::to_owned);
        result
    }
}

/// Invalid Prompt result.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptResultError {
    /// A complete Prompt requires at least one message.
    #[error("MCP prompt result is empty")]
    Empty,
    /// Too many messages were returned.
    #[error("too many MCP prompt messages")]
    TooManyMessages,
    /// Aggregate content exceeded a hard ceiling.
    #[error("MCP prompt result is too large")]
    TooLarge,
    /// Result description was invalid.
    #[error("invalid MCP prompt result description")]
    InvalidDescription,
}

/// Complete or input-required Prompt outcome.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum McpServerPromptOutcome {
    /// Rendering completed.
    Complete(McpServerPromptResult),
    /// Client-side MRTR input is required.
    InputRequired(McpServerInputRequired),
}

impl From<McpServerPromptResult> for McpServerPromptOutcome {
    fn from(value: McpServerPromptResult) -> Self {
        Self::Complete(value)
    }
}

impl From<McpServerInputRequired> for McpServerPromptOutcome {
    fn from(value: McpServerInputRequired) -> Self {
        Self::InputRequired(value)
    }
}

/// StateKnot-owned inbound Prompt render request.
#[derive(Clone, Debug)]
pub struct McpServerPromptRender {
    name: Arc<str>,
    arguments: Arc<Map<String, Value>>,
    input_responses: Arc<BTreeMap<String, Value>>,
    request_state: Option<Arc<str>>,
}

impl McpServerPromptRender {
    /// Returns the Prompt name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns syntactically decoded, initially untrusted arguments.
    #[must_use]
    pub fn arguments(&self) -> &Map<String, Value> {
        &self.arguments
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

/// Authenticated Prompt execution context.
#[derive(Clone, Debug)]
pub struct McpServerPromptContext {
    principal: McpServerPrincipal,
    client_capabilities: Arc<Value>,
    cancellation: CancellationToken,
    request_state_binding: Arc<[u8]>,
}

impl McpServerPromptContext {
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

    /// Returns principal/Prompt/arguments-bound request-state associated data.
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

/// Renders one Prompt after authorization and argument validation.
pub trait McpServerPromptRenderer: Send + Sync + 'static {
    /// Renders a bounded request.
    fn render(
        &self,
        request: McpServerPromptRender,
        context: McpServerPromptContext,
    ) -> BoxFuture<'_, Result<McpServerPromptOutcome, McpServerPromptRendererError>>;
}

/// Renderer failure category without arbitrary diagnostic leakage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptRendererError {
    /// The Prompt is not available.
    #[error("MCP prompt was not found")]
    NotFound,
    /// A dependency is unavailable.
    #[error("MCP prompt dependency is unavailable")]
    Unavailable,
    /// A renderer invariant failed.
    #[error("MCP prompt rendering failed internally")]
    Internal,
    /// Cooperative cancellation was observed.
    #[error("MCP prompt rendering was cancelled")]
    Cancelled,
}

/// Owned facts for decoded Prompt authorization.
#[derive(Clone, Debug)]
pub struct McpServerPromptAuthorizationRequest {
    principal: McpServerPrincipal,
    request: McpServerPromptRender,
    definition: Option<McpServerPromptDefinition>,
}

impl McpServerPromptAuthorizationRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the untrusted decoded request.
    #[must_use]
    pub const fn request(&self) -> &McpServerPromptRender {
        &self.request
    }

    /// Returns the exact registered definition, if one exists.
    #[must_use]
    pub const fn definition(&self) -> Option<&McpServerPromptDefinition> {
        self.definition.as_ref()
    }
}

/// Decoded Prompt authorization policy.
pub trait McpServerPromptAuthorization: Send + Sync + 'static {
    /// Authorizes before existence or argument diagnostics are disclosed.
    fn authorize(
        &self,
        request: McpServerPromptAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerPromptAuthorizationError>>;
}

/// Explicit policy that admits every scope-qualified render.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowMcpServerPromptAuthorization;

impl McpServerPromptAuthorization for AllowMcpServerPromptAuthorization {
    fn authorize(
        &self,
        _request: McpServerPromptAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerPromptAuthorizationError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Public-safe Prompt authorization failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptAuthorizationError {
    /// Policy denied rendering.
    #[error("MCP prompt rendering is forbidden")]
    Forbidden,
    /// Policy authority is unavailable.
    #[error("MCP prompt authorization is unavailable")]
    Unavailable,
}

/// Completion target without an SDK type in the public API.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpServerCompletionReference {
    /// Prompt name.
    Prompt(Box<str>),
    /// Resource template URI.
    ResourceTemplate(Box<str>),
}

/// Authenticated, bounded Completion request.
#[derive(Clone, Debug)]
pub struct McpServerCompletionRequest {
    principal: McpServerPrincipal,
    reference: McpServerCompletionReference,
    argument_name: Box<str>,
    argument_value: Box<str>,
    context_arguments: Arc<BTreeMap<String, String>>,
}

impl McpServerCompletionRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the target Prompt or Resource template.
    #[must_use]
    pub const fn reference(&self) -> &McpServerCompletionReference {
        &self.reference
    }

    /// Returns the argument being completed.
    #[must_use]
    pub fn argument_name(&self) -> &str {
        &self.argument_name
    }

    /// Returns the current prefix/value.
    #[must_use]
    pub fn argument_value(&self) -> &str {
        &self.argument_value
    }

    /// Returns previously resolved string arguments.
    #[must_use]
    pub fn context_arguments(&self) -> &BTreeMap<String, String> {
        &self.context_arguments
    }
}

/// Bounded Completion result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerCompletionResult {
    values: Arc<[Box<str>]>,
    total: Option<u32>,
    has_more: bool,
}

impl McpServerCompletionResult {
    /// Creates at most 100 unique values with consistent pagination metadata.
    pub fn new(
        values: impl IntoIterator<Item = impl Into<String>>,
        total: Option<u32>,
        has_more: bool,
    ) -> Result<Self, McpServerCompletionResultError> {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        if values.len() > MAX_COMPLETION_VALUES {
            return Err(McpServerCompletionResultError::TooManyValues);
        }
        if values.iter().any(|value| {
            value.len() > MAX_COMPLETION_VALUE_BYTES || value.chars().any(char::is_control)
        }) {
            return Err(McpServerCompletionResultError::InvalidValue);
        }
        let mut unique = HashSet::with_capacity(values.len());
        if values.iter().any(|value| !unique.insert(value.as_str())) {
            return Err(McpServerCompletionResultError::DuplicateValue);
        }
        let value_count =
            u32::try_from(values.len()).expect("MCP completion result contains at most 100 values");
        if total.is_some_and(|total| total < value_count)
            || (has_more && total == Some(value_count))
        {
            return Err(McpServerCompletionResultError::InvalidPagination);
        }
        Ok(Self {
            values: values
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
            total,
            has_more,
        })
    }

    fn into_protocol(self) -> CompleteResult {
        let values = self.values.iter().map(ToString::to_string).collect();
        let completion = CompletionInfo::with_pagination(values, self.total, self.has_more)
            .expect("StateKnot completion limits are within the MCP hard maximum");
        CompleteResult::new(completion)
    }
}

/// Invalid Completion output.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerCompletionResultError {
    /// More than 100 values were supplied.
    #[error("too many MCP completion values")]
    TooManyValues,
    /// One value was oversized or control-containing.
    #[error("invalid MCP completion value")]
    InvalidValue,
    /// One value appeared more than once.
    #[error("duplicate MCP completion value")]
    DuplicateValue,
    /// Total and has-more metadata were inconsistent.
    #[error("invalid MCP completion pagination")]
    InvalidPagination,
}

/// Produces Completion suggestions and owns their authorization.
pub trait McpServerCompletionProvider: Send + Sync + 'static {
    /// Completes one bounded request.
    fn complete(
        &self,
        request: McpServerCompletionRequest,
    ) -> BoxFuture<'_, Result<McpServerCompletionResult, McpServerCompletionError>>;
}

/// Completion failure category.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerCompletionError {
    /// Target or argument is not available in this authorization context.
    #[error("MCP completion target is unavailable")]
    NotFoundOrForbidden,
    /// Provider dependency is unavailable.
    #[error("MCP completion provider is unavailable")]
    Unavailable,
    /// Provider invariant failed.
    #[error("MCP completion failed internally")]
    Internal,
}

/// Prompt-only production handler. Use the composite application to combine
/// it with Tools and Resources.
#[derive(Clone)]
pub struct McpServerPromptService {
    catalog: McpServerPromptCatalog,
    options: McpServerApplicationOptions,
    renderer: Arc<dyn McpServerPromptRenderer>,
    authorization: Arc<dyn McpServerPromptAuthorization>,
    completion: Option<Arc<dyn McpServerCompletionProvider>>,
}

impl McpServerPromptService {
    /// Creates a service with explicit renderer and authorization policy.
    pub fn new<R, A>(
        catalog: McpServerPromptCatalog,
        options: McpServerApplicationOptions,
        renderer: R,
        authorization: A,
    ) -> Result<Self, McpServerPromptServiceBuildError>
    where
        R: McpServerPromptRenderer,
        A: McpServerPromptAuthorization,
    {
        Self::with_shared(
            catalog,
            options,
            Arc::new(renderer),
            Arc::new(authorization),
        )
    }

    /// Creates a service with already shared boundaries.
    pub fn with_shared(
        catalog: McpServerPromptCatalog,
        options: McpServerApplicationOptions,
        renderer: Arc<dyn McpServerPromptRenderer>,
        authorization: Arc<dyn McpServerPromptAuthorization>,
    ) -> Result<Self, McpServerPromptServiceBuildError> {
        if catalog.contains_scoped_prompts()
            && matches!(options.cache_scope, McpServerCacheScope::Public)
        {
            return Err(McpServerPromptServiceBuildError::PublicCacheWithScopedPrompts);
        }
        Ok(Self {
            catalog,
            options,
            renderer,
            authorization,
            completion: None,
        })
    }

    /// Adds a Completion provider.
    #[must_use]
    pub fn with_completion_provider<C>(mut self, completion: C) -> Self
    where
        C: McpServerCompletionProvider,
    {
        self.completion = Some(Arc::new(completion));
        self
    }

    /// Adds an already shared Completion provider.
    #[must_use]
    pub fn with_shared_completion_provider(
        mut self,
        completion: Arc<dyn McpServerCompletionProvider>,
    ) -> Self {
        self.completion = Some(completion);
        self
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

    fn page(
        &self,
        principal: &McpServerPrincipal,
        cursor: Option<&str>,
    ) -> Result<(Vec<Prompt>, Option<String>), ErrorData> {
        let scope = self.scope_tag(principal);
        let visible = self
            .catalog
            .inner
            .prompts
            .values()
            .filter(|definition| {
                definition
                    .required_scopes()
                    .all(|required| principal.has_scope(required))
            })
            .collect::<Vec<_>>();
        let start = cursor.map_or(Ok(0), |value| self.parse_cursor(value, &scope))?;
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
        let next = (end < visible.len())
            .then(|| format!("v1.{}.{}.{}", self.catalog.inner.digest, scope, end));
        Ok((values, next))
    }

    fn parse_cursor(&self, cursor: &str, scope: &str) -> Result<usize, ErrorData> {
        if cursor.len() > 192 {
            return Err(invalid_cursor());
        }
        let mut parts = cursor.split('.');
        let valid = parts.next() == Some("v1")
            && parts.next() == Some(self.catalog.inner.digest.as_ref())
            && parts.next() == Some(scope);
        let offset = parts.next().and_then(|value| value.parse().ok());
        if !valid || offset.is_none() || parts.next().is_some() {
            return Err(invalid_cursor());
        }
        Ok(offset.expect("validated cursor offset exists"))
    }

    async fn dispatch_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        validate_name(&request.name).map_err(|_| unavailable_prompt())?;
        let principal = mcp_server_principal(&context)
            .cloned()
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let definition = self.catalog.get(&request.name).cloned();
        if definition.as_ref().is_some_and(|definition| {
            definition
                .required_scopes()
                .any(|scope| !principal.has_scope(scope))
        }) {
            return Err(unavailable_prompt());
        }
        let input_responses = request.input_responses.unwrap_or_default();
        validate_inbound_mrtr(&input_responses, request.request_state.as_deref())?;
        let render = McpServerPromptRender {
            name: Arc::from(request.name),
            arguments: Arc::new(request.arguments.unwrap_or_default()),
            input_responses: Arc::new(input_responses),
            request_state: request.request_state.map(Arc::from),
        };
        let authorization = McpServerPromptAuthorizationRequest {
            principal: principal.clone(),
            request: render.clone(),
            definition: definition.clone(),
        };
        match self.authorization.authorize(authorization).await {
            Ok(()) => {}
            Err(McpServerPromptAuthorizationError::Forbidden) => {
                return Err(unavailable_prompt());
            }
            Err(McpServerPromptAuthorizationError::Unavailable) => {
                return Err(ErrorData::internal_error(
                    "MCP prompt authorization is unavailable",
                    None,
                ));
            }
        }
        let Some(definition) = definition else {
            return Err(unavailable_prompt());
        };
        validate_arguments(&definition, render.arguments())?;
        let execution_context = prompt_context(&context, principal, &render)?;
        match self.renderer.render(render, execution_context).await {
            Ok(McpServerPromptOutcome::Complete(result)) => Ok(result.into_protocol().into()),
            Ok(McpServerPromptOutcome::InputRequired(result)) => Ok(result.into_protocol().into()),
            Err(McpServerPromptRendererError::NotFound) => Err(unavailable_prompt()),
            Err(McpServerPromptRendererError::Unavailable) => Err(ErrorData::internal_error(
                "MCP prompt dependency is unavailable",
                None,
            )),
            Err(McpServerPromptRendererError::Internal) => Err(ErrorData::internal_error(
                "MCP prompt rendering failed",
                None,
            )),
            Err(McpServerPromptRendererError::Cancelled) => Err(ErrorData::internal_error(
                "MCP prompt rendering was cancelled",
                None,
            )),
        }
    }

    async fn dispatch_completion(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let Some(provider) = &self.completion else {
            return Err(ErrorData::invalid_params(
                "Completion is not available",
                None,
            ));
        };
        let principal = mcp_server_principal(&context)
            .cloned()
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let reference = match request.r#ref {
            Reference::Prompt(value) => {
                validate_name(&value.name).map_err(|_| unavailable_completion())?;
                if let Some(definition) = self.catalog.get(&value.name)
                    && definition
                        .required_scopes()
                        .any(|scope| !principal.has_scope(scope))
                {
                    return Err(unavailable_completion());
                }
                McpServerCompletionReference::Prompt(value.name.into_boxed_str())
            }
            Reference::Resource(value) => {
                if value.uri.is_empty()
                    || value.uri.len() > 4096
                    || value.uri.chars().any(char::is_control)
                {
                    return Err(unavailable_completion());
                }
                McpServerCompletionReference::ResourceTemplate(value.uri.into_boxed_str())
            }
            _ => return Err(unavailable_completion()),
        };
        validate_completion_string(&request.argument.name)?;
        validate_completion_string(&request.argument.value)?;
        let context_arguments = request
            .context
            .and_then(|value| value.arguments)
            .unwrap_or_default();
        if context_arguments.len() > MAX_ARGUMENTS
            || context_arguments.iter().any(|(name, value)| {
                validate_completion_string(name).is_err()
                    || validate_completion_string(value).is_err()
            })
        {
            return Err(ErrorData::invalid_params(
                "Invalid completion context",
                None,
            ));
        }
        let completion_request = McpServerCompletionRequest {
            principal,
            reference,
            argument_name: request.argument.name.into_boxed_str(),
            argument_value: request.argument.value.into_boxed_str(),
            context_arguments: Arc::new(context_arguments.into_iter().collect()),
        };
        match provider.complete(completion_request).await {
            Ok(result) => Ok(result.into_protocol()),
            Err(McpServerCompletionError::NotFoundOrForbidden) => Err(unavailable_completion()),
            Err(McpServerCompletionError::Unavailable) => Err(ErrorData::internal_error(
                "MCP completion provider is unavailable",
                None,
            )),
            Err(McpServerCompletionError::Internal) => {
                Err(ErrorData::internal_error("MCP completion failed", None))
            }
        }
    }
}

impl fmt::Debug for McpServerPromptService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerPromptService")
            .field("catalog", &self.catalog)
            .field("options", &self.options)
            .field("renderer", &"[RENDERER]")
            .field("authorization", &"[POLICY]")
            .field("completion", &self.completion.is_some())
            .finish_non_exhaustive()
    }
}

impl ServerHandler for McpServerPromptService {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_info(&self) -> ServerInfo {
        let mut capabilities = ServerCapabilities::default();
        capabilities.prompts = Some(PromptsCapability::default());
        if self.completion.is_some() {
            capabilities.completions = Some(Map::new());
        }
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

    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        let principal = mcp_server_principal(&context)
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let (prompts, next_cursor) = self.page(
            principal,
            request.as_ref().and_then(|value| value.cursor.as_deref()),
        )?;
        let mut result = ListPromptsResult::with_all_items(prompts)
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into());
        result.next_cursor = next_cursor;
        Ok(result)
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        self.dispatch_prompt(request, context).await
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        self.dispatch_completion(request, context).await
    }
}

/// Invalid Prompt service configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPromptServiceBuildError {
    /// Scope-filtered Prompts require principal-private caching.
    #[error("scope-restricted MCP prompts require a private catalog cache")]
    PublicCacheWithScopedPrompts,
}

fn validate_arguments(
    definition: &McpServerPromptDefinition,
    arguments: &Map<String, Value>,
) -> Result<(), ErrorData> {
    if arguments.len() > MAX_ARGUMENTS {
        return Err(invalid_arguments());
    }
    let definitions = definition
        .arguments()
        .map(|value| (value.name(), value))
        .collect::<HashMap<_, _>>();
    if arguments.iter().any(|(name, value)| {
        !definitions.contains_key(name.as_str())
            || value.as_str().is_none_or(|value| {
                value.len() > MAX_ARGUMENT_VALUE_BYTES || value.chars().any(char::is_control)
            })
    }) || definitions
        .values()
        .any(|definition| definition.required() && !arguments.contains_key(definition.name()))
    {
        return Err(invalid_arguments());
    }
    Ok(())
}

fn prompt_context(
    context: &RequestContext<RoleServer>,
    principal: McpServerPrincipal,
    request: &McpServerPromptRender,
) -> Result<McpServerPromptContext, ErrorData> {
    let capabilities = serde_json::to_value(context.client_capabilities().unwrap_or_default())
        .map_err(|_| ErrorData::internal_error("Failed to retain client capabilities", None))?;
    let arguments = serde_json_canonicalizer::to_vec(&Value::Object((*request.arguments).clone()))
        .map_err(|_| ErrorData::internal_error("Failed to bind MCP request state", None))?;
    let mut binding = b"stateknot/mcp-server/request-state/prompt/v1".to_vec();
    append_digest_part(&mut binding, principal.subject().as_bytes());
    append_digest_part(&mut binding, request.name().as_bytes());
    append_digest_part(&mut binding, &arguments);
    Ok(McpServerPromptContext {
        principal,
        client_capabilities: Arc::new(capabilities),
        cancellation: context.ct.clone(),
        request_state_binding: Digest::sha256(binding).as_bytes().to_vec().into(),
    })
}

fn validate_completion_string(value: &str) -> Result<(), ErrorData> {
    if value.len() > MAX_ARGUMENT_VALUE_BYTES || value.chars().any(char::is_control) {
        return Err(ErrorData::invalid_params(
            "Invalid completion argument",
            None,
        ));
    }
    Ok(())
}

fn digest_hex(value: impl AsRef<[u8]>) -> String {
    let digest = Digest::sha256(value).to_string();
    digest
        .strip_prefix("sha256:")
        .expect("StateKnot SHA-256 digest has stable prefix")
        .to_owned()
}

fn invalid_cursor() -> ErrorData {
    ErrorData::invalid_params("Invalid prompt catalog cursor", None)
}

fn unavailable_prompt() -> ErrorData {
    ErrorData::invalid_params("Unknown or unavailable prompt", None)
}

fn invalid_arguments() -> ErrorData {
    ErrorData::invalid_params("Prompt arguments are invalid", None)
}

fn unavailable_completion() -> ErrorData {
    ErrorData::invalid_params("Unknown or unavailable completion target", None)
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
    struct GreetingRenderer {
        calls: Arc<AtomicUsize>,
    }

    impl McpServerPromptRenderer for GreetingRenderer {
        fn render(
            &self,
            request: McpServerPromptRender,
            _context: McpServerPromptContext,
        ) -> BoxFuture<'_, Result<McpServerPromptOutcome, McpServerPromptRendererError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let name = request
                .arguments()
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            Box::pin(async move {
                let content = McpServerContent::text(format!("Hello {name}"))
                    .map_err(|_| McpServerPromptRendererError::Internal)?;
                McpServerPromptResult::new([McpServerPromptMessage::new(
                    McpServerPromptRole::User,
                    content,
                )])
                .map(Into::into)
                .map_err(|_| McpServerPromptRendererError::Internal)
            })
        }
    }

    #[derive(Clone, Copy)]
    struct NameCompletion;

    impl McpServerCompletionProvider for NameCompletion {
        fn complete(
            &self,
            request: McpServerCompletionRequest,
        ) -> BoxFuture<'_, Result<McpServerCompletionResult, McpServerCompletionError>> {
            let prefix = request.argument_value().to_owned();
            Box::pin(async move {
                McpServerCompletionResult::new([format!("{prefix}da")], Some(1), false)
                    .map_err(|_| McpServerCompletionError::Internal)
            })
        }
    }

    fn service(calls: &Arc<AtomicUsize>) -> McpServerHttpService<McpServerPromptService> {
        let argument = McpServerPromptArgument::new("name")
            .unwrap()
            .with_required(true);
        let prompt = McpServerPromptDefinition::new("greeting")
            .unwrap()
            .with_arguments([argument])
            .unwrap();
        let mut builder = McpServerPromptCatalogBuilder::default();
        builder.register(prompt).unwrap();
        let options = McpServerApplicationOptions::new(
            "stateknot-prompt-test",
            "0.0.0",
            10,
            std::time::Duration::from_secs(60),
            McpServerCacheScope::Private,
        )
        .unwrap();
        let handler = McpServerPromptService::new(
            builder.build().unwrap(),
            options,
            GreetingRenderer {
                calls: calls.clone(),
            },
            AllowMcpServerPromptAuthorization,
        )
        .unwrap()
        .with_completion_provider(NameCompletion);
        McpServerHttpService::new(
            handler,
            McpServerHttpOptions::loopback(32126).unwrap(),
            McpServerAuthentication::anonymous_loopback(),
        )
        .unwrap()
    }

    fn request(method: &str, mut params: Value) -> Request<Full<Bytes>> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned);
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
            .uri("http://127.0.0.1:32126/mcp")
            .header("host", "127.0.0.1:32126")
            .header("origin", "http://127.0.0.1:32126")
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
        service: &mut McpServerHttpService<McpServerPromptService>,
        method: &str,
        params: Value,
    ) -> (StatusCode, Value) {
        let response = service.call(request(method, params)).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[test]
    fn prompt_catalog_rejects_duplicate_arguments() {
        let argument = McpServerPromptArgument::new("same").unwrap();
        assert!(matches!(
            McpServerPromptDefinition::new("prompt")
                .unwrap()
                .with_arguments([argument.clone(), argument]),
            Err(McpServerPromptDefinitionError::DuplicateArgument)
        ));
    }

    #[tokio::test]
    async fn prompt_arguments_are_validated_before_rendering() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(&calls);
        let (status, invalid) = invoke(
            &mut service,
            "prompts/get",
            json!({ "name": "greeting", "arguments": {} }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid.pointer("/error/code"), Some(&json!(-32602)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let (status, result) = invoke(
            &mut service,
            "prompts/get",
            json!({ "name": "greeting", "arguments": { "name": "Ada" } }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            result.pointer("/result/messages/0/content/text"),
            Some(&json!("Hello Ada"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let (status, completion) = invoke(
            &mut service,
            "completion/complete",
            json!({
                "ref": { "type": "ref/prompt", "name": "greeting" },
                "argument": { "name": "name", "value": "A" }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            completion.pointer("/result/completion/values/0"),
            Some(&json!("Ada"))
        );
    }
}
