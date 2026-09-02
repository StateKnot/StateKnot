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
pub use openai::OpenAiResponsesModel;
