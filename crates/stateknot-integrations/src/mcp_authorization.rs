// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Trusted, input-aware authorization for durable MCP tool attempts.

use std::{fmt, sync::Arc};

use serde_json::Value;
use stateknot_core::{BoundedJson, BoxFuture, Digest, ToolContext, ToolDescriptor, ToolInput};
use thiserror::Error;

use crate::{McpAuthorization, McpAuthorizationError, ProviderEndpoint};

/// Computes an RFC 8785 SHA-256 pin from the complete raw MCP Tool object.
///
/// Retain the reviewed object in a release manifest. Do not obtain the expected
/// pin from live untrusted discovery. Unknown extensions are included, unlike
/// reserializing a typed SDK Tool which may discard them.
pub fn mcp_tool_descriptor_digest(value: &Value) -> Result<Digest, McpToolDigestError> {
    if !value.is_object() {
        return Err(McpToolDigestError);
    }
    BoundedJson::try_from_value(value.clone()).map_err(|_| McpToolDigestError)?;
    serde_json_canonicalizer::to_vec(value)
        .map(Digest::sha256)
        .map_err(|_| McpToolDigestError)
}

/// Invalid or over-limit remote Tool descriptor.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid MCP Tool descriptor for approval")]
pub struct McpToolDigestError;

/// Immutable operator approval plus a mandatory per-attempt authority source.
///
/// This is a trusted host binding, not a bearer capability or evidence supplied
/// by a remote Worker. Version the local Tool descriptor when policy changes.
#[derive(Clone)]
pub struct McpToolApproval {
    pub(crate) digest: Digest,
    pub(crate) authorizer: Arc<dyn McpToolAuthorizer>,
}

impl McpToolApproval {
    /// Binds a reviewed complete remote descriptor to an input-aware authorizer.
    pub fn new(digest: Digest, authorizer: Arc<dyn McpToolAuthorizer>) -> Self {
        Self { digest, authorizer }
    }
}

impl fmt::Debug for McpToolApproval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpToolApproval")
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Exact, already schema-validated request presented only to trusted host code.
///
/// These references cannot be replaced by a policy implementation. The input
/// that it approves is the input dispatched. Nothing in this context is sent
/// to the remote endpoint except the approved Tool arguments and credential.
/// Context identities originate in the host; they are not authentication of
/// an external caller. Resolve Run principal/consent from trusted host storage.
pub struct McpToolAuthorizationRequest<'a> {
    pub(crate) context: &'a ToolContext,
    pub(crate) descriptor: &'a ToolDescriptor,
    pub(crate) input: &'a ToolInput,
    pub(crate) endpoint: &'a ProviderEndpoint,
    pub(crate) remote_name: &'a str,
    pub(crate) digest: Digest,
}

impl McpToolAuthorizationRequest<'_> {
    /// Exact tenant, Run, logical invocation, physical attempt and deadline.
    pub const fn context(&self) -> &ToolContext {
        self.context
    }

    /// Frozen local capability identity, risk, resource policy and limits.
    pub const fn descriptor(&self) -> &ToolDescriptor {
        self.descriptor
    }

    /// Exact locally validated arguments; inspect resource targets here.
    pub const fn input(&self) -> &ToolInput {
        self.input
    }

    /// Operator-selected destination; never chosen by the arguments or Worker.
    pub const fn endpoint(&self) -> &ProviderEndpoint {
        self.endpoint
    }

    /// Exact remote Tool name frozen at startup.
    pub const fn remote_name(&self) -> &str {
        self.remote_name
    }

    /// Reviewed complete raw Tool descriptor pin verified at startup.
    pub const fn tool_digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for McpToolAuthorizationRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpToolAuthorizationRequest")
            .field("invocation_id", &self.context.invocation_id())
            .field("attempt_id", &self.context.attempt_id())
            .field("tool_digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Mandatory host policy for an approved MCP Tool binding.
///
/// No allow-all default, grant caching, or fallback credential is supplied.
/// Startup should use discovery-only authority. Each call must check trusted
/// tenant/principal/consent, capability, target arguments and current policy
/// before resolving a short-lived, audience- and resource-scoped credential.
/// Never forward a control-plane or upstream provider token to the Worker.
///
/// Both methods run under finite deadlines. Call authorization is serialized
/// with credential installation and transport dispatch; a denial or unavailable
/// policy produces a pre-dispatch `ToolError`, not an ambiguous external write.
pub trait McpToolAuthorizer: Send + Sync + 'static {
    /// Resolves least-privilege discovery credentials for this local binding.
    fn resolve_startup(
        &self,
        descriptor: &ToolDescriptor,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>>;

    /// Authorizes the exact input and returns only this exchange's credential.
    fn authorize_call<'a>(
        &'a self,
        request: McpToolAuthorizationRequest<'a>,
    ) -> BoxFuture<'a, Result<McpAuthorization, McpAuthorizationError>>;
}
