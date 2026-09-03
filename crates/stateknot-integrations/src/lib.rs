// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded first-party protocol adapters for the provider-neutral `StateKnot` core.
//!
//! Provider wire models remain private to this crate. Each model attempt makes
//! exactly one upstream HTTP exchange: redirects and client retries are
//! disabled so the durable runtime remains the only retry authority.

#![forbid(unsafe_code)]

mod a2a_client;
mod a2a_contract;
mod a2a_remote_agent;
mod a2a_server;
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

pub use a2a_client::{
    A2aAgentCardEndpoint, A2aAgentCardTrust, A2aBearerTokenProvider, A2aClient,
    A2aClientAttemptIdentity, A2aClientAuthorizationError, A2aClientAuthorizationRequest,
    A2aClientBuildError, A2aClientEndpointError, A2aClientError, A2aClientErrorKind,
    A2aClientEventStream, A2aClientInterfacePin, A2aClientOperation, A2aClientOptions,
    A2aClientOptionsError, A2aClientSecurity, A2aClientSecurityError, StaticA2aBearerToken,
    a2a_agent_card_digest,
};
pub use a2a_contract::{
    A2A_AGENT_CARD_PATH, A2A_BINDING_HTTP_JSON, A2A_BINDING_JSONRPC, A2A_PROTOCOL_VERSION_1_0,
    A2aAgentCapabilities, A2aAgentCard, A2aAgentCardBuilder, A2aAgentCardSignature,
    A2aAgentExtension, A2aAgentInterface, A2aAgentSkill, A2aArtifact, A2aArtifactUpdate,
    A2aBinding, A2aCancelTaskRequest, A2aContractError, A2aDeletePushConfigRequest,
    A2aGetPushConfigRequest, A2aGetTaskRequest, A2aListPushConfigsRequest, A2aListTasksRequest,
    A2aMessage, A2aMessageRole, A2aPart, A2aPartContent, A2aPartRef, A2aPushAuthentication,
    A2aPushConfig, A2aPushConfigPage, A2aSecret, A2aSecurityScheme, A2aSendConfiguration,
    A2aSendMessageRequest, A2aSendMessageResponse, A2aStatusUpdate, A2aStreamEvent,
    A2aSubscribeTaskRequest, A2aTask, A2aTaskPage, A2aTaskState, A2aTaskStatus,
};
pub use a2a_remote_agent::{
    A2aRemoteAgent, A2aRemoteAgentBuildError, A2aRemoteAgentDelivery, A2aRemoteAgentRecovery,
    A2aRemoteAgentRecoveryError, A2aRemoteAgentRecoveryMode, A2aSchemaRegistry,
};
pub use a2a_server::{
    A2aEventStream, A2aRequestContext, A2aServer, A2aServerAdmissionControl,
    A2aServerAdmissionError, A2aServerAdmissionRequest, A2aServerAuthenticationError,
    A2aServerAuthenticationRequest, A2aServerAuthenticator, A2aServerAuthorizationError,
    A2aServerAuthorizationRequest, A2aServerAuthorizer, A2aServerBuildError, A2aServerHttpOptions,
    A2aServerHttpOptionsError, A2aServerOperation, A2aServerPrincipal, A2aServerPrincipalError,
    A2aTaskService, A2aTaskServiceCapabilities, A2aTaskServiceError, AllowA2aServerAdmission,
    AllowA2aServerAuthorization,
};
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
