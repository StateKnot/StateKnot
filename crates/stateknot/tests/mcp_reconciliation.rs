// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real authenticated HTTP, `PostgreSQL` and withheld-receipt qualification.
#![allow(
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::similar_names
)]

use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
    response::Response,
};
use serde_json::{Value, json};
use stateknot::{core::*, integrations::*, postgres::*, runtime::*, *};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.test/operations".parse().unwrap(),
            "reconciliation-test".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn reference(schema: &Value) -> SchemaReference {
    SchemaReference::new(
        schema["$id"].as_str().unwrap().parse().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(schema).unwrap()),
    )
}

fn schemas() -> (JsonSchemaRegistry, SchemaReference, SchemaReference) {
    let input = json!({"$schema":"https://json-schema.org/draft/2020-12/schema", "$id":"https://schemas.example.test/reconcile-input/1.0.0", "type":"object"});
    let output = json!({"$schema":"https://json-schema.org/draft/2020-12/schema", "$id":"https://schemas.example.test/reconcile-output/1.0.0",
        "type":"object", "additionalProperties":false,"properties":{"status":{"const":"applied"},"receipt_id":{"type":"string"}}, "required":["status"]});
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    for schema in [&input, &output, &McpToolReconciler::audit_schema()] {
        builder.register(reference(schema), schema.clone()).unwrap();
    }
    (
        builder.build().unwrap(),
        reference(&input),
        reference(&output),
    )
}

async fn store() -> Option<PostgresStore> {
    let url = match std::env::var("STATEKNOT_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent)
            if std::env::var_os("STATEKNOT_REQUIRE_POSTGRES_TESTS").is_none() =>
        {
            return None;
        }
        _ => panic!("mandatory reconciliation PostgreSQL URL missing or invalid"),
    };
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    Some(PostgresStore::connect(&url, options).await.unwrap())
}

fn payload(input: &SchemaReference) -> JournalPayload {
    JournalPayload::new(
        input.clone(),
        JournalEventKind::new("reconciliation-fixture").unwrap(),
        BoundedJson::try_from_value(json!({"fixture":true})).unwrap(),
    )
    .unwrap()
}

fn append(fence: &RunFence, head: JournalHead, schema: &SchemaReference) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::worker(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            fence.clone(),
            payload(schema),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn unknown(
    store: &PostgresStore,
    tenant: TenantId,
    input: &SchemaReference,
    output: &SchemaReference,
) -> (McpReconciliationRequest, RunFence) {
    let graph = CompiledGraph::compile(
        capability("reconciliation-graph"),
        input.clone(),
        input.clone(),
        input.clone(),
        output.clone(),
        GraphReducerReference::new(
            capability("reconciliation-reducer"),
            Digest::sha256(b"reducer"),
        ),
        ReadyNodes::try_new([NodeId::new("write").unwrap()]).unwrap(),
        [GraphNode::new(
            NodeId::new("write").unwrap(),
            None,
            GraphRoutes::empty(),
            None,
            true,
        )
        .unwrap()],
        GraphExecutionLimits::new(Superstep::new(4).unwrap(), 1).unwrap(),
    )
    .unwrap();
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let run = RunId::generate();
    let admitted = store
        .admit_run(AgentResultProvenance::new(
            tenant.clone(),
            run,
            ThreadId::generate(),
            InvocationId::generate(),
            capability("agent"),
        ))
        .await
        .unwrap();
    let cp = store
        .append_control_plane_checkpoint(
            JournalAppend::new(
                JournalExpectation::empty(),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    payload(input),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            CheckpointWrite::initial(
                tenant.clone(),
                run,
                CheckpointId::generate(),
                graph.reference(),
                CheckpointState::new(
                    input.clone(),
                    BoundedJson::try_from_value(json!({})).unwrap(),
                )
                .unwrap(),
                graph.entry_nodes().clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let mut descriptor = fixture["descriptors"]["valid"][0].clone();
    descriptor["input_schema"] = json!(input);
    descriptor["output_schema"] = json!(output);
    descriptor["semantics"] = json!({"risk":"non_idempotent_write", "idempotency":"unsupported", "status_query":true,"compensation":false});
    descriptor["resources"] = json!({"network":"read_write","filesystem":"none","credentials":false,"dynamic_code":false});
    descriptor["invocation"] = json!({"cancellation":"cooperative","max_progress_events":"0"});
    let descriptor: ToolDescriptor = serde_json::from_value(descriptor).unwrap();
    let id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            append(&fence, cp.event().head(), input),
            ToolInvocationIntent::new(
                NodeActivation::for_ready_root(cp.checkpoint(), NodeId::new("write").unwrap())
                    .unwrap(),
                id,
                descriptor.clone(),
                ToolInput::new(
                    input.clone(),
                    BoundedJson::try_from_value(json!({"private":"not-for-receipt"})).unwrap(),
                )
                .unwrap(),
                descriptor.limits().clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let attempt = AttemptId::generate();
    let started = store
        .advance_tool_invocation(
            append(&fence, prepared.event().head(), input),
            &prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: attempt,
            },
        )
        .await
        .unwrap();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::AmbiguousExternalOutcome,
        FailureCode::new("fixture.lost_response").unwrap(),
        FailureOrigin::new("tool.fixture").unwrap(),
        FailureMessage::new("Test provider response was lost.").unwrap(),
        RetryAdvice::ReconcileFirst,
    )
    .unwrap();
    let error = ToolError::new(
        failure,
        ToolErrorPhase::Execution,
        ToolExternalEffect::Unknown,
        ToolErrorProvenance::new(id, attempt, descriptor.metadata().identity().clone()),
    )
    .unwrap();
    let unknown = store
        .advance_tool_invocation(
            append(&fence, started.event().head(), input),
            &started.invocation().head(),
            ToolInvocationTransition::RecordError { error },
        )
        .await
        .unwrap();
    assert_eq!(unknown.invocation().status(), ToolInvocationStatus::Unknown);
    (
        McpReconciliationRequest {
            event_id: EventId::generate(),
            run_id: run,
            invocation_id: id,
            attempt_id: attempt,
            expected_revision: unknown.invocation().revision(),
            expected_digest: unknown.invocation().digest(),
            output: BoundedJson::try_from_value(json!({"status":"applied"})).unwrap(),
        },
        fence,
    )
}

struct Auth;
impl McpServerAuthenticator for Auth {
    fn authenticate(
        &self,
        request: McpServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<McpServerPrincipal, McpServerAuthenticationError>> {
        Box::pin(async move {
            let subject = match request.credential().expose_secret() {
                "ops-a" => "issuer/ops-a",
                "ops-b" => "issuer/ops-b",
                "ops-other-tenant" => "issuer/other",
                "ordinary-worker" => {
                    return Ok(
                        McpServerPrincipal::new("issuer/worker", ["compute:double"]).unwrap()
                    );
                }
                _ => return Err(McpServerAuthenticationError::InvalidCredential),
            };
            Ok(McpServerPrincipal::new(subject, ["stateknot:reconcile-result"]).unwrap())
        })
    }
}

struct Policy {
    tenant: TenantId,
    mode: AtomicUsize,
    calls: AtomicUsize,
}
impl McpReconciliationAuthorizer for Policy {
    fn authorize(
        &self,
        principal: McpServerPrincipal,
        _request: McpReconciliationRequest,
    ) -> BoxFuture<'_, Result<McpReconciliationGrant, McpReconciliationError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.mode.load(Ordering::SeqCst) == 1 {
                return Err(McpReconciliationError::Denied);
            }
            let tenant = if principal.subject() == "issuer/other" {
                TenantId::new("unrelated-tenant").unwrap()
            } else {
                self.tenant.clone()
            };
            let identity = PrincipalIdentity::new(
                "https://issuer.example.test/operations".parse().unwrap(),
                principal.subject().parse().unwrap(),
            );
            Ok(McpReconciliationGrant {
                caller: AgentServiceCaller::new(tenant, identity),
                policy: capability("result-attestation-policy"),
                policy_digest: Digest::sha256(b"retained policy"),
                decision_digest: Digest::sha256(b"retained fixture decision"),
            })
        })
    }
}

#[derive(Default)]
struct Loss {
    committed: Notify,
    release: Notify,
}
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
    loss: Arc<Loss>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.loss.release.notify_waiters();
        self.task.abort();
    }
}
impl Server {
    async fn start(store: PostgresStore, schemas: JsonSchemaRegistry, policy: Arc<Policy>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut registry = McpServerToolRegistryBuilder::default();
        registry
            .register(
                McpToolReconciler::definition().unwrap(),
                McpToolReconciler::new(store, schemas, policy).unwrap(),
            )
            .unwrap();
        let handler = McpServerToolService::new(
            registry.build().unwrap(),
            McpServerApplicationOptions::new(
                "stateknot-reconciliation",
                "1.0.0",
                8,
                Duration::from_secs(30),
                McpServerCacheScope::Private,
            )
            .unwrap(),
            AllowMcpServerToolAuthorization,
        )
        .unwrap();
        let http = McpServerHttpService::new(
            handler,
            McpServerHttpOptions::loopback(address.port()).unwrap(),
            McpServerAuthentication::bearer(
                Auth,
                McpServerBearerChallenge::new("reconciliation", None::<String>).unwrap(),
            ),
        )
        .unwrap();
        let loss = Arc::new(Loss::default());
        let gate = loss.clone();
        let app = Router::new()
            .fallback_service(http)
            .layer(middleware::from_fn(move |request: Request, next: Next| {
                let gate = gate.clone();
                async move {
                    let lose = request.headers().contains_key("x-fixture-withhold-receipt");
                    let response: Response = next.run(request).await;
                    if lose {
                        gate.committed.notify_one();
                        gate.release.notified().await;
                    }
                    response
                }
            }));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}/mcp/"),
            task,
            loss,
        }
    }
}

async fn call(url: &str, token: &str, arguments: Value, lose: bool) -> Value {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut request = client
        .post(url)
        .bearer_auth(token)
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("MCP-Method", "tools/call")
        .header("MCP-Name", "stateknot_reconcile_tool_result_v1")
        .header("Accept", "application/json, text/event-stream");
    if lose {
        request = request.header("x-fixture-withhold-receipt", "1");
    }
    let response = request.json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
        "name":"stateknot_reconcile_tool_result_v1","arguments":arguments,"_meta":{
            "io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"operator","version":"1.0.0"},
            "io.modelcontextprotocol/clientCapabilities":{}
        }}})).send().await.unwrap();
    let status = response.status();
    if !status.is_success() {
        return json!({"error":{"http_status":status.as_u16(), "fixture_body":response.text().await.unwrap()}});
    }
    let body = response.text().await.unwrap();
    serde_json::from_str(&body)
        .unwrap_or_else(|_| panic!("unexpected successful fixture response: {body}"))
}

fn error(response: &Value, code: &str) {
    assert_eq!(response["result"]["isError"], true, "{response}");
    assert_eq!(response["result"]["content"][0]["text"], code, "{response}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_reconciliation_is_atomic_fenced_and_recovers_a_lost_http_receipt() {
    let Some(store) = store().await else { return };
    let (schemas, input, output) = schemas();
    let tenant = TenantId::new(format!("reconciliation-{}", RunId::generate())).unwrap();
    let policy = Arc::new(Policy {
        tenant: tenant.clone(),
        mode: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    let server = Server::start(store.clone(), schemas.clone(), policy.clone()).await;
    let (request, old_fence) = Box::pin(unknown(&store, tenant.clone(), &input, &output)).await;
    let args = json!(request);
    let before = store
        .load_run(&tenant, request.run_id)
        .await
        .unwrap()
        .journal_head()
        .cloned()
        .unwrap();

    // Authentication precedes parsing, and an ordinary execution token has no reconciliation authority.
    let unauthorized = reqwest::Client::new()
        .post(&server.url)
        .header("Content-Type", "application/json")
        .body("invalid JSON")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);
    let ordinary = call(&server.url, "ordinary-worker", args.clone(), false).await;
    assert!(ordinary.get("error").is_some());
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    policy.mode.store(1, Ordering::SeqCst);
    error(
        &call(&server.url, "ops-a", args.clone(), false).await,
        "reconciliation.denied",
    );
    let mut missing = args.clone();
    missing["run_id"] = json!(RunId::generate());
    error(
        &call(&server.url, "ops-a", missing, false).await,
        "reconciliation.denied",
    );
    policy.mode.store(0, Ordering::SeqCst);
    error(
        &call(&server.url, "ops-other-tenant", args.clone(), false).await,
        "reconciliation.conflict",
    );
    let mut stale = args.clone();
    stale["attempt_id"] = json!(AttemptId::generate());
    error(
        &call(&server.url, "ops-a", stale, false).await,
        "reconciliation.conflict",
    );
    let mut stale = args.clone();
    stale["expected_digest"] = json!(Digest::sha256(b"wrong revision"));
    error(
        &call(&server.url, "ops-a", stale, false).await,
        "reconciliation.conflict",
    );
    let mut bad = args.clone();
    bad["output"] = json!({"status":"forged"});
    error(
        &call(&server.url, "ops-a", bad, false).await,
        "reconciliation.invalid",
    );
    let mut forged = args.clone();
    forged["fence"] = json!(old_fence);
    assert!(
        call(&server.url, "ops-a", forged, false)
            .await
            .get("error")
            .is_some()
    );
    error(
        &call(&server.url, "ops-a", args.clone(), false).await,
        "reconciliation.busy",
    );
    assert_eq!(
        store
            .load_run(&tenant, request.run_id)
            .await
            .unwrap()
            .journal_head(),
        Some(&before)
    );
    store.release_lease(&old_fence).await.unwrap();

    // Drop the real HTTP client only after the handler committed, but before its response was sent.
    let pending = {
        let url = server.url.clone();
        let args = args.clone();
        tokio::spawn(async move { call(&url, "ops-a", args, true).await })
    };
    tokio::time::timeout(Duration::from_secs(5), server.loss.committed.notified())
        .await
        .unwrap();
    let committed = store
        .load_tool_invocation(&tenant, request.run_id, request.invocation_id)
        .await
        .unwrap();
    assert_eq!(committed.status(), ToolInvocationStatus::Committed);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    server.loss.release.notify_one();
    let recovered = call(&server.url, "ops-a", args.clone(), false).await;
    let receipt = recovered["result"]["structuredContent"].clone();
    assert_eq!(
        receipt,
        json!({"event_id":request.event_id,"invocation_digest":committed.digest(),"revision":committed.revision()})
    );
    assert!(!receipt.to_string().contains("not-for-receipt"));
    let (audit, exact) = store
        .load_tool_invocation_revision(
            &tenant,
            request.run_id,
            request.invocation_id,
            committed.revision(),
        )
        .await
        .unwrap();
    assert_eq!(exact.head(), committed.head());
    assert_eq!(
        audit.payload().kind().as_str(),
        "mcp-tool-result-reconciled"
    );
    assert_eq!(
        audit.payload().data().as_value()["policy_digest"],
        json!(Digest::sha256(b"retained policy"))
    );
    assert!(
        !audit
            .payload()
            .data()
            .as_value()
            .to_string()
            .contains("status")
    );
    assert_eq!(audit.event_id(), request.event_id);

    let mut joins = Vec::new();
    for _ in 0..24 {
        let url = server.url.clone();
        let args = args.clone();
        joins.push(tokio::spawn(async move {
            call(&url, "ops-a", args, false).await
        }));
    }
    for join in joins {
        assert_eq!(join.await.unwrap()["result"]["structuredContent"], receipt);
    }
    assert_eq!(
        store
            .load_run(&tenant, request.run_id)
            .await
            .unwrap()
            .journal_head(),
        Some(committed.journal_head())
    );
    let mut changed = args.clone();
    changed["event_id"] = json!(EventId::generate());
    error(
        &call(&server.url, "ops-a", changed, false).await,
        "reconciliation.conflict",
    );
    error(
        &call(&server.url, "ops-b", args.clone(), false).await,
        "reconciliation.conflict",
    );
    let mut changed = args.clone();
    changed["output"] = json!({"status":"applied", "receipt_id":"different-valid-result"});
    error(
        &call(&server.url, "ops-a", changed, false).await,
        "reconciliation.conflict",
    );

    // Race the first submissions too, not just reads of an already committed receipt.
    let (racing, racing_fence) = Box::pin(unknown(&store, tenant.clone(), &input, &output)).await;
    store.release_lease(&racing_fence).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(24));
    let mut racers = Vec::new();
    for _ in 0..24 {
        let url = server.url.clone();
        let arguments = json!(racing);
        let barrier = barrier.clone();
        racers.push(tokio::spawn(async move {
            barrier.wait().await;
            call(&url, "ops-a", arguments, false).await
        }));
    }
    let mut successes = Vec::new();
    for racer in racers {
        let response = racer.await.unwrap();
        if response["result"]["isError"] == true {
            error(&response, "reconciliation.busy");
        } else {
            successes.push(response["result"]["structuredContent"].clone());
        }
    }
    assert!(!successes.is_empty());
    let resolved = store
        .load_tool_invocation(&tenant, racing.run_id, racing.invocation_id)
        .await
        .unwrap();
    assert_eq!(resolved.status(), ToolInvocationStatus::Committed);
    assert_eq!(
        resolved.revision(),
        racing.expected_revision.checked_next().unwrap()
    );
    let racing_receipt =
        call(&server.url, "ops-a", json!(racing), false).await["result"]["structuredContent"]
            .clone();
    for success in successes {
        assert_eq!(success, racing_receipt);
    }
    assert_eq!(
        store
            .load_run(&tenant, racing.run_id)
            .await
            .unwrap()
            .journal_head(),
        Some(resolved.journal_head())
    );

    // A fresh service instance recovers the receipt while a different live Worker owns the Run.
    let next = store
        .claim_lease(&tenant, request.run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(next.fence().epoch() > old_fence.epoch());
    let replacement = Server::start(store.clone(), schemas, policy.clone()).await;
    assert_eq!(
        call(&replacement.url, "ops-a", args.clone(), false).await["result"]["structuredContent"],
        receipt
    );
    assert!(matches!(
        store
            .append_worker(
                append(&old_fence, committed.journal_head().clone(), &input),
                RunProjection::Unchanged
            )
            .await,
        Err(StoreError::StaleFence)
    ));
    store.release_lease(next.fence()).await.unwrap();
    // Denial still succeeds without any available database: no existence lookup precedes policy.
    store.close().await;
    policy.mode.store(1, Ordering::SeqCst);
    error(
        &call(&replacement.url, "ops-a", args, false).await,
        "reconciliation.denied",
    );
    println!(
        "\nSTATEKNOT_MCP_RECONCILIATION_EVIDENCE={{\"profile\":\"mcp-result-reconciliation-v1\",\"schema\":24,\"authenticated_http\":true,\"authorization_before_lookup\":true,\"atomic_audit\":true,\"lost_http_receipt\":true,\"duplicate_receipts\":24,\"concurrent_first_submissions\":24,\"fresh_service_recovery\":true,\"stale_fence_rejected\":true,\"invariants\":\"passed\"}}"
    );
}

#[test]
fn wire_request_is_closed_and_debug_does_not_expose_output() {
    let request = McpReconciliationRequest {
        event_id: EventId::generate(),
        run_id: RunId::generate(),
        invocation_id: InvocationId::generate(),
        attempt_id: AttemptId::generate(),
        expected_revision: ToolInvocationRevision::new(2).unwrap(),
        expected_digest: Digest::sha256(b"unknown"),
        output: BoundedJson::try_from_value(json!({"secret":"private-evidence"})).unwrap(),
    };
    assert!(!format!("{request:?}").contains("private-evidence"));
    let mut value = json!(request);
    value["tenant_id"] = json!("another-tenant");
    assert!(serde_json::from_value::<McpReconciliationRequest>(value).is_err());
    let mut value = json!(request);
    value["expected_revision"] = json!("9223372036854775808");
    assert!(serde_json::from_value::<McpReconciliationRequest>(value).is_err());
}

#[test]
fn reconciliation_definition_requires_explicit_scope_and_closed_shapes() {
    let definition = McpToolReconciler::definition().unwrap();
    assert_eq!(
        definition.required_scopes().collect::<Vec<_>>(),
        ["stateknot:reconcile-result"]
    );
    assert_eq!(definition.input_schema()["additionalProperties"], false);
    assert_eq!(
        definition.output_schema().unwrap()["additionalProperties"],
        false
    );
}
