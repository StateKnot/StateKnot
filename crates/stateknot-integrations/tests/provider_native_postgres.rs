// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real HTTP adapter -> durable invocation ledger -> terminal Agent evidence.
#![allow(clippy::too_many_lines, clippy::wildcard_imports)]

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use stateknot_core::*;
use stateknot_integrations::{
    ApiKey, DeepSeekResponsesModel, OpenAiResponsesModel, ProviderEndpoint, ProviderHttpOptions,
    StaticApiKey,
};
use stateknot_runtime::agent_policy::{
    AgentResourcePolicy, PolicyArtifact, PolicyDocument, RunAccessTarget, RunPermission, RunRule,
    SubmissionRule,
};
use stateknot_runtime::*;
use stateknot_store_postgres::{PostgresStore, PostgresStoreOptions, PostgresTransportSecurity};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/stateknot".parse().unwrap(),
            "provider-native-http-test".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn schema(name: &str, document: Value) -> (SchemaReference, Value) {
    let id = format!("https://stknot.com/schemas/tests/{name}/1.0.0");
    let mut document = document;
    document["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
    document["$id"] = json!(id);
    let reference = SchemaReference::new(
        id.parse().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(&document).unwrap()),
    );
    (reference, document)
}

struct NoTools(AgentToolPolicyReference);

impl AgentToolPolicy for NoTools {
    fn reference(&self) -> &AgentToolPolicyReference {
        &self.0
    }

    fn evaluate(
        &self,
        _: AgentToolPolicyContext,
    ) -> BoxFuture<'_, Result<AgentToolPolicyDecision, AgentToolPolicyError>> {
        Box::pin(async { Err(AgentToolPolicyError::InvalidEvidence) })
    }
}

async fn loopback_provider(deepseek: bool) -> (ProviderEndpoint, oneshot::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/v1/")).unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut captured = Vec::new();
        let mut buffer = [0_u8; 2048];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            captured.extend_from_slice(&buffer[..read]);
            if let Some(header_end) = captured.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&captured[..header_end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                if captured.len() >= header_end + 4 + length {
                    break;
                }
            }
        }
        let mut response = json!({
            "id": "resp_durable_01",
            "model": "provider-model-v1",
            "status": "completed",
            "output": [{
                "id": "message_durable_01",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "{\"answer\":\"durable\"}", "annotations": []}]
            }],
            "usage": {
                "input_tokens": 10,
                "input_tokens_details": {"cached_tokens": 2},
                "output_tokens": 3,
                "output_tokens_details": {"reasoning_tokens": 1},
                "total_tokens": 13
            }
        });
        if deepseek {
            response["output"][0]["phase"] = json!("final_answer");
        }
        let body = serde_json::to_vec(&response).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
        tx.send(captured).unwrap();
    });
    (endpoint, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openai_http_response_is_durably_priced_and_terminal_on_postgres() {
    Box::pin(qualify_provider_native(false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deepseek_http_response_is_durably_priced_and_terminal_on_postgres() {
    Box::pin(qualify_provider_native(true)).await;
}

async fn qualify_provider_native(deepseek: bool) {
    let url = match std::env::var("STATEKNOT_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) if std::env::var_os("STATEKNOT_REQUIRE_POSTGRES_TESTS").is_none() => return,
        Err(error) => panic!("mandatory PostgreSQL URL is missing or invalid: {error}"),
    };
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options).await.unwrap();
    let tenant = TenantId::new(format!("provider-http-{}", RunId::generate())).unwrap();
    let (input_schema, input_document) = schema(
        "provider-http-input",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["question"],
            "properties": {"question": {"type": "string", "minLength": 1}}
        }),
    );
    let (output_schema, output_document) = schema(
        "provider-http-output",
        json!({
            "type": "object", "additionalProperties": false,
            "required": ["answer"],
            "properties": {"answer": {"type": "string"}}
        }),
    );
    let (profile, profile_document) = schema("provider-http-profile", json!({"type": "object"}));
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    let template: AgentDescriptor =
        serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap();
    let modalities = ModelModalities::try_new([ModelModality::Text]).unwrap();
    let model = ModelDescriptor::new(
        template.model().metadata().clone(),
        ModelCapabilities::new(
            modalities.clone(),
            modalities,
            !deepseek,
            ModelToolCapabilities::unsupported(),
            ModelStructuredOutputCapabilities::json_schema(profile.clone()),
            false,
            ModelTokenLimits::new(
                Some(TokenCount::new(128_000)),
                Some(TokenCount::new(120_000)),
                Some(TokenCount::new(8_000)),
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let execution = AgentExecutionConfig::new(
        AgentStructuredOutputStrategy::ModelNative,
        ExecutionCount::new(1),
        ExecutionCount::ZERO,
        ExecutionCount::ZERO,
        AgentToolConcurrency::sequential(),
    )
    .unwrap();
    let descriptor = AgentDescriptor::new(
        template.metadata().clone(),
        input_schema.clone(),
        output_schema.clone(),
        model.clone(),
        template.instructions().clone(),
        AgentTools::empty(),
        execution,
        template.budget_limits().clone(),
    )
    .unwrap();
    let accounting = Arc::new(
        ModelTokenAccounting::new(
            capability("accounting.provider-http"),
            model.metadata().identity().clone(),
            ModelProviderModelId::new("provider-model-v1").unwrap(),
            ModelTokenRateCard {
                currency: "USD".parse().unwrap(),
                input_per_million: 2_000_000,
                cached_input_per_million: 500_000,
                output_per_million: 8_000_000,
            },
        )
        .unwrap(),
    );
    let definition = ProviderNativeAgentGraph::compile(
        descriptor,
        capability("graphs.provider-http"),
        capability("reducers.provider-http"),
        "https://stknot.com/schemas/tests/provider-http-state/1.0.0"
            .parse()
            .unwrap(),
        "tenant/provider-http-input".parse().unwrap(),
        Arc::new(NoTools(AgentToolPolicyReference::new(
            capability("policies.no-tools"),
            Digest::sha256(b"no tools v1"),
        ))),
        accounting.clone(),
    )
    .unwrap();

    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    for (reference, document) in [
        (input_schema.clone(), input_document),
        (output_schema, output_document),
        (profile, profile_document),
    ] {
        schemas.register(reference, document).unwrap();
    }
    definition.register_schema(&mut schemas).unwrap();
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut schemas).unwrap();
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
    register_standard_agent_admission_event_schema(&mut schemas).unwrap();
    register_standard_agent_service_control_event_schema(&mut schemas).unwrap();
    stateknot_runtime::agent_policy::register_agent_policy_evidence_schema(&mut schemas).unwrap();
    register_standard_invocation_execution_event_schema(&mut schemas).unwrap();
    let schemas = schemas.build().unwrap();
    let (endpoint, captured) = loopback_provider(deepseek).await;
    let adapter: Arc<dyn Model> = if deepseek {
        Arc::new(
            DeepSeekResponsesModel::new(
                model,
                ModelProviderModelId::new("provider-model-v1").unwrap(),
                "tenant/model-output".parse().unwrap(),
                Arc::new(schemas.clone()),
                Arc::new(StaticApiKey::new(ApiKey::new("test-only-key").unwrap())),
                endpoint,
                ProviderHttpOptions::default(),
            )
            .unwrap(),
        )
    } else {
        Arc::new(
            OpenAiResponsesModel::new(
                model,
                ModelProviderModelId::new("provider-model-v1").unwrap(),
                "tenant/model-output".parse().unwrap(),
                Arc::new(schemas.clone()),
                Arc::new(StaticApiKey::new(ApiKey::new("test-only-key").unwrap())),
                endpoint,
                ProviderHttpOptions::default(),
            )
            .unwrap(),
        )
    };
    let mut models = ModelProviderRegistryBuilder::new();
    models.register(adapter).unwrap();
    let executor = DurableInvocationExecutor::new(
        store.clone(),
        schemas.clone(),
        models.build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(
            ProviderNativeAgentBudgetProvider::new(definition.clone(), store.clone()).unwrap(),
        ),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas.clone());
    definition
        .register_executable(&mut registry, store.clone(), executor, schemas)
        .unwrap();
    let registry = registry.build().unwrap();
    store
        .register_graph_definition(tenant.clone(), definition.graph().clone())
        .await
        .unwrap();

    let mut budget_fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-runtime-v1.json"
    ))
    .unwrap();
    budget_fixture["base_budget_layers"][0]["deadline"] = json!("2099-01-01T00:00:00.000000Z");
    let limits: BudgetLimits =
        serde_json::from_value(budget_fixture["base_budget_layers"][0].clone()).unwrap();
    let caller = AgentServiceCaller::new(
        tenant.clone(),
        capability("policies.provider-http").owner().clone(),
    );
    let key = AgentSubmissionKey::new(format!("provider-http-{}", EventId::generate())).unwrap();
    let policy = Arc::new(
        AgentResourcePolicy::new(
            PolicyArtifact::new(PolicyDocument {
                format_version: 1,
                policy: capability("policies.provider-http"),
                valid_until: "2099-01-01T00:00:00.000000Z".parse().unwrap(),
                submissions: vec![SubmissionRule {
                    tenant: tenant.clone(),
                    principal: caller.principal().clone(),
                    agent: definition.descriptor().metadata().identity().clone(),
                    input_schema: input_schema.clone(),
                    granted_scopes: ScopeSet::empty(),
                    budget_limits: limits,
                }],
                runs: vec![RunRule {
                    tenant: tenant.clone(),
                    principal: caller.principal().clone(),
                    operation: RunPermission::Read,
                    target: RunAccessTarget::Submission(key.digest_for(&tenant)),
                }],
            })
            .unwrap(),
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let mut deployments = AgentServiceRegistryBuilder::new();
    deployments.register(Arc::new(definition.clone())).unwrap();
    let service =
        AgentServiceV1::new(store.clone(), registry.clone(), deployments.build(), policy).unwrap();
    let request = AgentRequest::new(
        input_schema,
        BoundedJson::try_from_value(json!({"question": "Return a short answer"})).unwrap(),
        BudgetLimits::empty(),
    );
    let admitted = service
        .submit(
            caller.clone(),
            &key,
            definition.descriptor().metadata().identity(),
            request.clone(),
        )
        .await
        .unwrap();
    let run_id = admitted.snapshot().provenance().run_id();
    assert!(matches!(
        service.load(caller.clone(), run_id).await,
        Err(AgentServiceError::Authorization(
            AgentServiceAuthorizationError::Denied
        ))
    ));
    let lease = store
        .claim_lease(&tenant, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        driver.drive(lease.fence().clone(), CancellationSignal::never()),
    )
    .await
    .unwrap()
    .unwrap();
    let outcome = outcome.into_parts().0;
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = outcome else {
        panic!(
            "real provider response must reach the terminal barrier: {outcome:?}; lifecycle: {:?}",
            store.load_run(&tenant, run_id).await.unwrap().lifecycle()
        )
    };
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            definition.clone(),
            store.clone(),
        )),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    lifecycle.commit_barrier(*handoff).await.unwrap();
    let request_bytes = captured.await.unwrap();
    let request_text = String::from_utf8(request_bytes).unwrap();
    assert!(request_text.starts_with("POST /v1/responses HTTP/1.1"));
    assert!(
        request_text.contains("Authorization: Bearer test-only-key")
            || request_text.contains("authorization: Bearer test-only-key")
    );
    let run = store.load_run(&tenant, run_id).await.unwrap();
    let usage = run.lifecycle().result().unwrap().usage();
    assert_eq!(usage.model_turns(), ExecutionCount::new(1));
    assert_eq!(usage.input_tokens(), TokenCount::new(10));
    assert_eq!(usage.output_tokens(), TokenCount::new(3));
    assert_eq!(
        usage
            .known_costs()
            .get("USD".parse().unwrap())
            .unwrap()
            .micro_units(),
        41
    );
    let checkpoint = store
        .load_current_checkpoint(&tenant, run_id)
        .await
        .unwrap()
        .unwrap();
    let state = definition.restore_state(checkpoint.state()).unwrap();
    let ProviderNativeAgentPhase::Model { plan } = state.phase() else {
        panic!("one-turn Agent must retain its model plan")
    };
    let invocation = store
        .load_model_invocation(&tenant, run_id, plan.invocation_id())
        .await
        .unwrap();
    let ModelInvocationState::Committed { response } = invocation.state() else {
        panic!("the real HTTP response must be committed in the invocation ledger")
    };
    assert_eq!(response.usage().input_tokens(), TokenCount::new(10));
    assert_eq!(
        response.usage().cached_input_tokens(),
        Some(TokenCount::new(2))
    );
    assert_eq!(response.usage().output_tokens(), TokenCount::new(3));
    let snapshot = service.load_by_key(caller, &key).await.unwrap();
    assert_eq!(snapshot.status(), RunStatus::Succeeded);
    let repeated = service
        .submit(
            AgentServiceCaller::new(
                tenant.clone(),
                capability("policies.provider-http").owner().clone(),
            ),
            &key,
            definition.descriptor().metadata().identity(),
            request,
        )
        .await
        .unwrap();
    assert!(matches!(repeated, AgentRunAdmissionOutcome::Idempotent(_)));
    assert_eq!(repeated.snapshot().provenance().run_id(), run_id);
    store.close().await;
}
