// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded first-party protocol adapters for the provider-neutral `StateKnot` core.
//!
//! Provider wire models remain private to this crate. Each model attempt makes
//! exactly one upstream HTTP exchange: redirects and client retries are
//! disabled so the durable runtime remains the only retry authority.

#![forbid(unsafe_code)]

mod adapter;
mod anthropic;
mod credential;
mod http;
mod mcp;
mod mcp_client;
mod mcp_oauth;
mod mcp_server;
mod mcp_server_app;
mod mcp_server_prompt;
mod mcp_server_resource;
mod mcp_server_tool;
mod openai;
mod sse;

pub use adapter::ModelAdapterBuildError;
pub use anthropic::AnthropicMessagesModel;
pub use credential::{ApiKey, ApiKeyError, ApiKeyProvider, ApiKeyResolutionError, StaticApiKey};
pub use http::{
    ProviderEndpoint, ProviderEndpointError, ProviderHttpOptions, ProviderHttpOptionsError,
};
pub use mcp::{
    AnonymousMcpAuthorization, McpAuthorization, McpAuthorizationError, McpAuthorizationProvider,
    McpHttpOptions, McpHttpOptionsError, McpRemoteTool, McpRemoteToolBuildError, McpSchemaRegistry,
    McpServerIdentity, McpServerIdentityError, StaticMcpBearerAuthorization,
};
pub use mcp_client::{
    MCP_PROTOCOL_VERSION_2026_07_28, McpCachePolicy, McpClient, McpClientAuthorizationChallenge,
    McpClientAuthorizationChallengeStatus, McpClientAuthorizationProvider,
    McpClientAuthorizationRequest, McpClientAuthorizationRetry, McpClientIdentity,
    McpClientIdentityError, McpClientOptions, McpClientOptionsError, McpClientServer,
    McpCompleteToolResult, McpInputRequired, McpNotification, McpRejectedTool, McpRemoteError,
    McpTool, McpToolCall, McpToolCallResponse, McpToolCatalog, McpToolPage, McpToolRejectionReason,
    StatelessMcpClientError,
};
pub use mcp_oauth::{
    InMemoryMcpOAuthCredentialStore, InMemoryMcpOAuthStateStore, McpOAuthAuthorization,
    McpOAuthCredentialRefreshGuard, McpOAuthCredentialStore, McpOAuthOptions, McpOAuthOptionsError,
    McpOAuthRegistration, McpOAuthResource, McpOAuthResourceError, McpOAuthStateStore,
    McpOAuthStoredAuthorizationState, McpOAuthStoredCredentials, McpOAuthUserAgent,
    McpOAuthUserAgentError, McpOAuthUserAuthorizationRequest,
};
pub use mcp_server::{
    AllowMcpServerAdmission, McpServerAdmissionControl, McpServerAdmissionError,
    McpServerAdmissionRequest, McpServerAuthentication, McpServerAuthenticationError,
    McpServerAuthenticationRequest, McpServerAuthenticator, McpServerBearerChallenge,
    McpServerBearerChallengeError, McpServerBearerCredential, McpServerBearerCredentialError,
    McpServerHttpOptions, McpServerHttpOptionsError, McpServerHttpService,
    McpServerHttpServiceBuildError, McpServerPrincipal, McpServerPrincipalError,
    mcp_server_principal,
};
pub use mcp_server_app::{
    McpServerApplication, McpServerApplicationBuildError, McpServerApplicationBuilder,
};
pub use mcp_server_prompt::{
    AllowMcpServerPromptAuthorization, McpServerCompletionError, McpServerCompletionProvider,
    McpServerCompletionReference, McpServerCompletionRequest, McpServerCompletionResult,
    McpServerCompletionResultError, McpServerPromptArgument, McpServerPromptAuthorization,
    McpServerPromptAuthorizationError, McpServerPromptAuthorizationRequest, McpServerPromptCatalog,
    McpServerPromptCatalogBuilder, McpServerPromptCatalogError, McpServerPromptCatalogLimits,
    McpServerPromptCatalogLimitsError, McpServerPromptContext, McpServerPromptDefinition,
    McpServerPromptDefinitionError, McpServerPromptMessage, McpServerPromptOutcome,
    McpServerPromptRender, McpServerPromptRenderer, McpServerPromptRendererError,
    McpServerPromptResult, McpServerPromptResultError, McpServerPromptRole, McpServerPromptService,
    McpServerPromptServiceBuildError,
};
pub use mcp_server_resource::{
    AllowMcpServerResourceAuthorization, McpServerResourceAuthorization,
    McpServerResourceAuthorizationError, McpServerResourceAuthorizationRequest,
    McpServerResourceCatalog, McpServerResourceCatalogBuilder, McpServerResourceCatalogError,
    McpServerResourceCatalogLimits, McpServerResourceCatalogLimitsError, McpServerResourceContent,
    McpServerResourceContentError, McpServerResourceContext, McpServerResourceDefinition,
    McpServerResourceDefinitionError, McpServerResourceOutcome, McpServerResourceRead,
    McpServerResourceReader, McpServerResourceReaderError, McpServerResourceResult,
    McpServerResourceResultError, McpServerResourceService, McpServerResourceServiceBuildError,
    McpServerResourceTemplateDefinition,
};
pub use mcp_server_tool::{
    AllowMcpServerToolAuthorization, McpServerApplicationOptions, McpServerApplicationOptionsError,
    McpServerCacheScope, McpServerContent, McpServerContentError, McpServerInputRequest,
    McpServerInputRequired, McpServerInputRequiredError, McpServerRequestStateCodec,
    McpServerRequestStateCodecBuildError, McpServerRequestStateError,
    McpServerRequiredClientCapability, McpServerToolAnnotations, McpServerToolAuthorization,
    McpServerToolAuthorizationError, McpServerToolAuthorizationRequest, McpServerToolCall,
    McpServerToolContext, McpServerToolDefinition, McpServerToolDefinitionError,
    McpServerToolHandler, McpServerToolHandlerError, McpServerToolOutcome,
    McpServerToolProgressError, McpServerToolRegistry, McpServerToolRegistryBuilder,
    McpServerToolRegistryError, McpServerToolRegistryLimits, McpServerToolRegistryLimitsError,
    McpServerToolResult, McpServerToolResultError, McpServerToolService,
    McpServerToolServiceBuildError,
};
pub use openai::OpenAiResponsesModel;
