// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Authenticated MCP ingress for authoritative inline Tool result reconciliation.

use std::{fmt, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stateknot_core::{
    AttemptId, BoundedJson, BoxFuture, CapabilityIdentity, Digest, EventId, InvocationId,
    JournalAppend, JournalEvent, JournalEventIntent, JournalEventKind, JournalExpectation,
    JournalPayload, RunFence, RunId, SchemaReference, ToolArtifacts, ToolInvocation,
    ToolInvocationRevision, ToolInvocationStatus, ToolInvocationTransition, ToolResult,
    ToolResultProvenance,
};
use stateknot_integrations::{
    McpServerContent, McpServerPrincipal, McpServerToolCall, McpServerToolContext,
    McpServerToolDefinition, McpServerToolDefinitionError, McpServerToolHandler,
    McpServerToolHandlerError, McpServerToolOutcome, McpServerToolResult,
};
use stateknot_runtime::{AgentServiceCaller, JsonSchemaRegistry};
use stateknot_store_postgres::{PostgresStore, StoreError};
use thiserror::Error;

const TOOL: &str = "stateknot_reconcile_tool_result_v1";
const SCOPE: &str = "stateknot:reconcile-result";
const EVENT: &str = "mcp-tool-result-reconciled";

/// Closed wire request. Identity, fence, policy and schemas are host-selected.
/// Retain these exact values and the event ID when the response is lost.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpReconciliationRequest {
    /// Durable receipt/event identity selected once per logical submission.
    pub event_id: EventId,
    /// Target Run, within the tenant derived by trusted authorization.
    pub run_id: RunId,
    /// Original logical Tool invocation.
    pub invocation_id: InvocationId,
    /// Original physical attempt, never a new execution attempt.
    pub attempt_id: AttemptId,
    /// Exact unresolved revision (canonical decimal string on the wire).
    pub expected_revision: ToolInvocationRevision,
    /// Exact unresolved record checksum.
    pub expected_digest: Digest,
    /// Authoritative successful output; no artifact or arbitrary error import.
    pub output: BoundedJson,
}

impl fmt::Debug for McpReconciliationRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpReconciliationRequest")
            .field("event_id", &self.event_id)
            .field("invocation_id", &self.invocation_id)
            .field("expected_revision", &self.expected_revision)
            .finish_non_exhaustive()
    }
}

/// Trusted policy decision and authenticated subject-to-tenant mapping.
#[derive(Clone, Debug)]
pub struct McpReconciliationGrant {
    /// Caller resolved from verified MCP identity, never request arguments.
    pub caller: AgentServiceCaller,
    /// Exact authorizing policy implementation.
    pub policy: CapabilityIdentity,
    /// Retained policy artifact checksum.
    pub policy_digest: Digest,
    /// Retained evidence/decision checksum.
    pub decision_digest: Digest,
}

/// Mandatory resource- and evidence-aware authorization, before any DB lookup.
///
/// Map the authenticator's namespaced subject to a trusted tenant/principal.
/// Check this exact target and result against authoritative provider/operations
/// evidence. A Worker scope or shape-valid output alone is not proof. Do not
/// grant ordinary compute/write Worker credentials reconciliation authority.
/// This hook is bounded by the handler deadline; no allow-all default exists.
pub trait McpReconciliationAuthorizer: Send + Sync + 'static {
    /// Authorizes evidence submission, including every duplicate request.
    fn authorize(
        &self,
        principal: McpServerPrincipal,
        request: McpReconciliationRequest,
    ) -> BoxFuture<'_, Result<McpReconciliationGrant, McpReconciliationError>>;
}

/// Closed, payload-safe ingress failures. No raw SQL/provider error is exposed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpReconciliationError {
    /// Identity or evidence policy denied this exact operation.
    #[error("reconciliation.denied")]
    Denied,
    /// The request or output failed the pinned contract.
    #[error("reconciliation.invalid")]
    Invalid,
    /// The target/attempt/revision or retained receipt does not match.
    #[error("reconciliation.conflict")]
    Conflict,
    /// Another Worker currently owns the Run; no forced takeover is performed.
    #[error("reconciliation.busy")]
    Busy,
    /// Outcome may have committed; retry only the identical request/event ID.
    #[error("reconciliation.unavailable")]
    Unavailable,
}

// Consume and discard private database diagnostics at the public boundary.
#[allow(clippy::needless_pass_by_value)]
fn store_error(error: StoreError) -> McpReconciliationError {
    match error {
        StoreError::LeaseHeld => McpReconciliationError::Busy,
        StoreError::EventIdConflict
        | StoreError::ProjectionIntentConflict
        | StoreError::NoActiveLease
        | StoreError::LeaseExpired
        | StoreError::RunNotFound
        | StoreError::ToolInvocationNotFound
        | StoreError::StaleToolInvocationHead
        | StoreError::ToolInvocationCommitConflict
        | StoreError::StaleCheckpointHead
        | StoreError::RunNotRunnable
        | StoreError::StaleFence => McpReconciliationError::Conflict,
        _ => McpReconciliationError::Unavailable,
    }
}

/// A privileged control-plane MCP Tool; expose only on an authenticated endpoint.
///
/// No provider registry is held and no external write can be dispatched. The
/// host claims (never supersedes) a Run lease, commits reconciliation and its
/// authorization audit atomically, then releases its lease. Timeout/drop may
/// leave a lease until expiry; identical receipt recovery needs no new lease.
#[derive(Clone)]
pub struct McpToolReconciler {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    authorizer: Arc<dyn McpReconciliationAuthorizer>,
    event_schema: SchemaReference,
}

impl McpToolReconciler {
    /// Binds host-only database authority and an offline schema registry.
    /// The built-in audit schema must be registered unchanged at startup.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        authorizer: Arc<dyn McpReconciliationAuthorizer>,
    ) -> Result<Self, McpReconciliationError> {
        let document = Self::audit_schema();
        let canonical = serde_json_canonicalizer::to_vec(&document)
            .map_err(|_| McpReconciliationError::Invalid)?;
        let event_schema = SchemaReference::new(
            "https://stknot.com/schemas/runtime/mcp-tool-reconciliation/1.0.0"
                .parse()
                .map_err(|_| McpReconciliationError::Invalid)?,
            stateknot_core::Version::new(1, 0, 0),
            Digest::sha256(&canonical),
        );
        if schemas.canonical_bytes(&event_schema) != Some(canonical.as_slice()) {
            return Err(McpReconciliationError::Invalid);
        }
        Ok(Self {
            store,
            schemas,
            authorizer,
            event_schema,
        })
    }

    /// Frozen audit schema; register it using its `$id`, version 1.0.0 and RFC 8785 digest.
    pub fn audit_schema() -> Value {
        json!({"$schema":"https://json-schema.org/draft/2020-12/schema",
        "$id":"https://stknot.com/schemas/runtime/mcp-tool-reconciliation/1.0.0",
        "type":"object", "additionalProperties":false,
        "required":["request_digest","policy_digest","decision_digest","policy","principal"],
        "properties": {
            "request_digest":digest_schema(), "policy_digest":digest_schema(), "decision_digest":digest_schema(),
            "policy":{"type":"object"}, "principal":{"type":"object"}
        }})
    }

    /// Scoped, closed MCP definition for the v1 inline-success operation.
    pub fn definition() -> Result<McpServerToolDefinition, McpServerToolDefinitionError> {
        McpServerToolDefinition::new(TOOL, json!({"type":"object", "additionalProperties":false,
            "required":["event_id","run_id","invocation_id","attempt_id","expected_revision","expected_digest","output"],
            "properties": {
                "event_id":id_schema(), "run_id":id_schema(), "invocation_id":id_schema(), "attempt_id":id_schema(),
                "expected_revision":{"type":"string","pattern":"^(0|[1-9][0-9]{0,18})$"},
                "expected_digest":digest_schema(), "output":{}
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
        request: McpReconciliationRequest,
    ) -> Result<Value, McpReconciliationError> {
        if !principal.has_scope(SCOPE) {
            return Err(McpReconciliationError::Denied);
        }
        let grant = self
            .authorizer
            .authorize(principal.clone(), request.clone())
            .await?;
        let tenant = grant.caller.tenant_id();
        let request_digest = Digest::sha256(serde_json_canonicalizer::to_vec(&json!({
            "subject":principal.subject(), "tenant":tenant, "principal":grant.caller.principal(), "request":request
        })).map_err(|_| McpReconciliationError::Invalid)?);
        let (_, expected) = self
            .store
            .load_tool_invocation_revision(
                tenant,
                request.run_id,
                request.invocation_id,
                request.expected_revision,
            )
            .await
            .map_err(store_error)?;
        if expected.digest() != request.expected_digest
            || expected.attempt_id() != Some(request.attempt_id)
            || expected.status() != ToolInvocationStatus::Unknown
        {
            return Err(McpReconciliationError::Conflict);
        }
        let result = ToolResult::new(
            ToolResultProvenance::new(
                request.invocation_id,
                request.attempt_id,
                expected.intent().descriptor().metadata().identity().clone(),
            ),
            expected.intent().descriptor().output_schema().clone(),
            request.output.clone(),
            ToolArtifacts::empty(),
        );
        expected
            .validate_reconciliation_result(&result)
            .map_err(|_| McpReconciliationError::Invalid)?;
        self.schemas
            .validate_bounded(result.output_schema(), result.output())
            .map_err(|_| McpReconciliationError::Invalid)?;
        if let Some(receipt) = self
            .recover(&grant, &request, &expected, request_digest)
            .await?
        {
            return Ok(receipt);
        }
        let lease = self
            .store
            .claim_lease(tenant, request.run_id, AttemptId::generate())
            .await
            .map_err(store_error)?;
        let result = self
            .commit(
                lease.lease().fence(),
                &grant,
                &request,
                &expected,
                result,
                request_digest,
            )
            .await;
        // A lost release acknowledgement cannot invalidate a committed receipt.
        // The lease remains bounded and any superseding owner is never released.
        let _ = self.store.release_lease(lease.lease().fence()).await;
        result
    }

    async fn recover(
        &self,
        grant: &McpReconciliationGrant,
        request: &McpReconciliationRequest,
        expected: &ToolInvocation,
        request_digest: Digest,
    ) -> Result<Option<Value>, McpReconciliationError> {
        let next = request
            .expected_revision
            .checked_next()
            .ok_or(McpReconciliationError::Conflict)?;
        match self
            .store
            .load_tool_invocation_revision(
                grant.caller.tenant_id(),
                request.run_id,
                request.invocation_id,
                next,
            )
            .await
        {
            Ok((event, invocation)) => {
                if event.event_id() != request.event_id
                    || event.payload().kind().as_str() != EVENT
                    || event.payload().schema() != &self.event_schema
                    || event.payload().data().as_value()["request_digest"] != json!(request_digest)
                    || invocation.previous() != Some(&expected.head())
                    || invocation.status() != ToolInvocationStatus::Committed
                {
                    return Err(McpReconciliationError::Conflict);
                }
                Ok(Some(receipt(&event, &invocation)))
            }
            Err(StoreError::ToolInvocationNotFound) => Ok(None),
            Err(error) => Err(store_error(error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit(
        &self,
        fence: &RunFence,
        grant: &McpReconciliationGrant,
        request: &McpReconciliationRequest,
        expected: &ToolInvocation,
        result: ToolResult,
        request_digest: Digest,
    ) -> Result<Value, McpReconciliationError> {
        // Recheck receipt after acquiring the lease: another submission may have won.
        if let Some(receipt) = self
            .recover(grant, request, expected, request_digest)
            .await?
        {
            return Ok(receipt);
        }
        let data = BoundedJson::try_from_value(json!({
            "request_digest":request_digest, "principal":grant.caller.principal(),
            "policy":grant.policy, "policy_digest":grant.policy_digest, "decision_digest":grant.decision_digest
        })).map_err(|_| McpReconciliationError::Invalid)?;
        self.schemas
            .validate_bounded(&self.event_schema, &data)
            .map_err(|_| McpReconciliationError::Invalid)?;
        let payload = JournalPayload::new(
            self.event_schema.clone(),
            JournalEventKind::new(EVENT).map_err(|_| McpReconciliationError::Invalid)?,
            data,
        )
        .map_err(|_| McpReconciliationError::Invalid)?;
        for _ in 0..4 {
            let run = self
                .store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .map_err(store_error)?;
            let head = run
                .journal_head()
                .cloned()
                .ok_or(McpReconciliationError::Conflict)?;
            let intent = JournalEventIntent::worker(
                fence.tenant_id().clone(),
                fence.run_id(),
                request.event_id,
                fence.clone(),
                payload.clone(),
            )
            .map_err(|_| McpReconciliationError::Invalid)?;
            let append = JournalAppend::new(JournalExpectation::exact(head), intent)
                .map_err(|_| McpReconciliationError::Invalid)?;
            match self
                .store
                .advance_tool_invocation(
                    append,
                    &expected.head(),
                    ToolInvocationTransition::ReconcileResult {
                        result: result.clone(),
                    },
                )
                .await
            {
                Ok(outcome) => return Ok(receipt(outcome.event(), outcome.invocation())),
                Err(StoreError::StaleJournalHead) => {}
                Err(error) => return Err(store_error(error)),
            }
        }
        Err(McpReconciliationError::Busy)
    }
}

fn id_schema() -> Value {
    json!({"type":"string","format":"uuid","maxLength":36,
        "pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"})
}
fn digest_schema() -> Value {
    json!({"type":"string","minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"})
}
fn receipt(event: &JournalEvent, invocation: &ToolInvocation) -> Value {
    json!({"event_id":event.event_id(), "invocation_digest":invocation.digest(), "revision":invocation.revision()})
}

impl McpServerToolHandler for McpToolReconciler {
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
