// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Separately authorized, non-retryable known-effect failure evidence.

use std::{fmt, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stateknot_core::{
    AttemptId, BoxFuture, Digest, EventId, Failure, FailureCategory, FailureCode, FailureId,
    FailureMessage, FailureOrigin, InvocationId, RetryAdvice, RunId, ToolError, ToolErrorPhase,
    ToolErrorProvenance, ToolExternalEffect, ToolInvocationRevision,
};
use stateknot_integrations::{
    McpServerContent, McpServerPrincipal, McpServerToolCall, McpServerToolContext,
    McpServerToolDefinition, McpServerToolDefinitionError, McpServerToolHandler,
    McpServerToolHandlerError, McpServerToolOutcome, McpServerToolResult,
};
use stateknot_runtime::JsonSchemaRegistry;
use stateknot_store_postgres::PostgresStore;

use super::{
    McpReconciliationError, McpReconciliationGrant, McpToolReconciler, ReconciledOutcome,
    ReconciliationStore, ReconciliationTarget, digest_schema, id_schema,
};

const TOOL: &str = "stateknot_reconcile_tool_error_v1";
const SCOPE: &str = "stateknot:reconcile-error";
const EVENT: &str = "mcp-tool-error-reconciled";

/// Authoritatively resolved write effect; uncertainty cannot enter this profile.
/// Neither variant proves that the provider charged nothing or that retry is safe.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpKnownToolEffect {
    /// The intended write was not applied (not a claim that execution never started).
    NotApplied,
    /// The intended write was applied despite the failed outcome.
    Applied,
}

impl McpKnownToolEffect {
    const fn core(self) -> ToolExternalEffect {
        match self {
            Self::NotApplied => ToolExternalEffect::NotApplied,
            Self::Applied => ToolExternalEffect::Applied,
        }
    }
}

/// Closed v1 failure-evidence request, without retry, usage or provenance authority.
/// Retain every field and both IDs across lost responses. The evidence policy must
/// approve the failure message for public Run/Agent surfaces before granting access.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpErrorReconciliationRequest {
    /// Stable event/receipt identity for this logical evidence submission.
    pub event_id: EventId,
    /// Run in the tenant derived by the trusted authorizer.
    pub run_id: RunId,
    /// Exact original logical Tool invocation.
    pub invocation_id: InvocationId,
    /// Exact original physical Tool attempt.
    pub attempt_id: AttemptId,
    /// Original Unknown revision, encoded as a canonical decimal string.
    pub expected_revision: ToolInvocationRevision,
    /// Original Unknown record checksum.
    pub expected_digest: Digest,
    /// Stable identity of the authoritative failure occurrence.
    pub failure_id: FailureId,
    /// Known failure category; ambiguous external outcomes are rejected.
    pub failure_category: FailureCategory,
    /// Bounded application-owned failure classification.
    pub failure_code: FailureCode,
    /// Bounded public-safe message; shape validation alone does not prove confidentiality.
    pub failure_message: FailureMessage,
    /// Evidence about whether the original intended write was applied.
    pub external_effect: McpKnownToolEffect,
}

impl fmt::Debug for McpErrorReconciliationRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpErrorReconciliationRequest")
            .field("event_id", &self.event_id)
            .field("invocation_id", &self.invocation_id)
            .field("expected_revision", &self.expected_revision)
            .finish_non_exhaustive()
    }
}

/// Mandatory failure/effect evidence policy, independent of success-result authority.
/// Authenticate the namespaced subject, map tenant/principal, check this exact
/// resource and authoritative effect evidence, and approve the public message.
/// Deny partial/uncertain effects; a Worker claim or HTTP status is not evidence.
/// Runs before every database lookup, including duplicate receipt recovery.
pub trait McpErrorReconciliationAuthorizer: Send + Sync + 'static {
    /// Authorizes this exact immutable failure/effect submission.
    fn authorize(
        &self,
        principal: McpServerPrincipal,
        request: McpErrorReconciliationRequest,
    ) -> BoxFuture<'_, Result<McpReconciliationGrant, McpReconciliationError>>;
}

/// Privileged MCP Tool that resolves Unknown to Failed without dispatching a provider.
///
/// The host fixes execution phase, origin and `RetryAdvice::Never`. Only known
/// `Applied`/`NotApplied` effects are accepted; no arbitrary `ToolError`, artifacts,
/// usage, recovery handles or automatic retries are imported. Existing successful
/// result reconciliation retains its separate tool, scope and frozen wire schema.
#[derive(Clone)]
pub struct McpToolErrorReconciler {
    backend: ReconciliationStore,
    authorizer: Arc<dyn McpErrorReconciliationAuthorizer>,
}

impl McpToolErrorReconciler {
    /// Requires the exact v1 error audit schema in the offline registry.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        authorizer: Arc<dyn McpErrorReconciliationAuthorizer>,
    ) -> Result<Self, McpReconciliationError> {
        Ok(Self {
            backend: ReconciliationStore::new(store, schemas, &Self::audit_schema(), EVENT)?,
            authorizer,
        })
    }

    /// Frozen error audit schema, registered using version 1.0.0 and RFC 8785 bytes.
    pub fn audit_schema() -> Value {
        let mut schema = McpToolReconciler::audit_schema();
        schema["$id"] =
            json!("https://stknot.com/schemas/runtime/mcp-tool-error-reconciliation/1.0.0");
        schema
    }

    /// Separately scoped, closed MCP definition for known failure evidence.
    pub fn definition() -> Result<McpServerToolDefinition, McpServerToolDefinitionError> {
        McpServerToolDefinition::new(TOOL, json!({"type":"object", "additionalProperties":false,
            "required":["event_id","run_id","invocation_id","attempt_id","expected_revision","expected_digest",
                "failure_id","failure_category","failure_code","failure_message","external_effect"],
            "properties": {
                "event_id":id_schema(), "run_id":id_schema(), "invocation_id":id_schema(), "attempt_id":id_schema(),
                "expected_revision":{"type":"string","pattern":"^(0|[1-9][0-9]{0,18})$"},
                "expected_digest":digest_schema(), "failure_id":id_schema(),
                "failure_category":{"type":"string","enum":["invalid_input","unauthenticated","permission_denied",
                    "policy_denied","not_found","conflict","unsupported","rate_limited","deadline_exceeded",
                    "cancelled","dependency_unavailable","data_corruption","internal"]},
                "failure_code":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[a-z][a-z0-9_-]*(\\.[a-z][a-z0-9_-]*)*$"},
                "failure_message":{"type":"string","minLength":1,"maxLength":FailureMessage::MAX_BYTES},
                "external_effect":{"type":"string","enum":["not_applied","applied"]}
            }}))?
            .with_required_scopes([SCOPE])?
            .with_output_schema(json!({"type":"object", "additionalProperties":false,
                "required":["event_id","invocation_digest","revision"], "properties":{
                    "event_id":id_schema(), "invocation_digest":digest_schema(),
                    "revision":{"type":"string","pattern":"^(0|[1-9][0-9]{0,18})$"}
                }}))
    }

    async fn reconcile(
        &self,
        principal: McpServerPrincipal,
        request: McpErrorReconciliationRequest,
    ) -> Result<Value, McpReconciliationError> {
        if !principal.has_scope(SCOPE) {
            return Err(McpReconciliationError::Denied);
        }
        let grant = self
            .authorizer
            .authorize(principal.clone(), request.clone())
            .await?;
        let failure = Failure::new(
            request.failure_id,
            request.failure_category,
            request.failure_code.clone(),
            FailureOrigin::new("mcp.reconciliation")
                .map_err(|_| McpReconciliationError::Invalid)?,
            request.failure_message.clone(),
            RetryAdvice::Never,
        )
        .map_err(|_| McpReconciliationError::Invalid)?;
        let request_digest = Digest::sha256(
            serde_json_canonicalizer::to_vec(&json!({
                "tool":TOOL, "subject":principal.subject(), "tenant":grant.caller.tenant_id(),
                "principal":grant.caller.principal(), "request":request
            }))
            .map_err(|_| McpReconciliationError::Invalid)?,
        );
        let target = ReconciliationTarget {
            event_id: request.event_id,
            run_id: request.run_id,
            invocation_id: request.invocation_id,
            attempt_id: request.attempt_id,
            expected_revision: request.expected_revision,
            expected_digest: request.expected_digest,
        };
        let expected = self.backend.load(&grant, &target).await?;
        let error = ToolError::new(
            failure,
            ToolErrorPhase::Execution,
            request.external_effect.core(),
            ToolErrorProvenance::new(
                request.invocation_id,
                request.attempt_id,
                expected.intent().descriptor().metadata().identity().clone(),
            ),
        )
        .map_err(|_| McpReconciliationError::Invalid)?;
        expected
            .validate_reconciliation_error(&error)
            .map_err(|_| McpReconciliationError::Invalid)?;
        self.backend
            .apply(
                &grant,
                &target,
                &expected,
                ReconciledOutcome::Error(error),
                request_digest,
            )
            .await
    }
}

impl McpServerToolHandler for McpToolErrorReconciler {
    fn call(
        &self,
        call: McpServerToolCall,
        context: McpServerToolContext,
    ) -> BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>> {
        Box::pin(async move {
            if context.is_cancelled() {
                return Err(McpServerToolHandlerError::Cancelled);
            }
            let operation = async {
                if call.name() != TOOL
                    || !call.input_responses().is_empty()
                    || call.request_state().is_some()
                {
                    return Err(McpReconciliationError::Invalid);
                }
                let request = serde_json::from_value(Value::Object(call.arguments().clone()))
                    .map_err(|_| McpReconciliationError::Invalid)?;
                self.reconcile(context.principal().clone(), request).await
            };
            let result = tokio::select! {
                biased;
                () = context.cancelled() => return Err(McpServerToolHandlerError::Cancelled),
                result = tokio::time::timeout(Duration::from_secs(15), operation) => result.unwrap_or(Err(McpReconciliationError::Unavailable)),
            };
            let result = match result {
                Ok(receipt) => McpServerToolResult::structured([], receipt),
                Err(error) => {
                    McpServerToolResult::error([McpServerContent::text(error.to_string())
                        .map_err(|_| McpServerToolHandlerError::Internal)?])
                }
            }
            .map_err(|_| McpServerToolHandlerError::Internal)?;
            Ok(result.into())
        })
    }
}
