// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! One recoverable, authorized, priced `DeepSeek` Agent run.
//!
//! Run with `cargo run -p stateknot --example deepseek_agent --locked -- config.json`.
//! The JSON file is the retained operator policy/request artifact; credentials
//! come only from `DATABASE_URL` and `DEEPSEEK_API_KEY`. `PostgreSQL` migrations
//! are explicit unless the operator opts into `STATEKNOT_DEV_MODE=true`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stateknot::runtime::agent_policy::{
    AgentResourcePolicy, PolicyArtifact, PolicyDocument, RunAccessTarget, RunPermission, RunRule,
    SubmissionRule,
};
use stateknot::{
    agent_maintenance::{
        AgentMaintenanceMutationOptions, AgentMaintenanceReadiness, AgentMaintenanceReadinessError,
    },
    agent_worker::{AgentWorkerExecutionOptions, AgentWorkerReadiness, AgentWorkerReadinessError},
    core::{
        AgentExecutionConfig, AgentInstructions, AgentStructuredOutputStrategy, AgentSubmissionKey,
        AgentToolConcurrency, BoxFuture, BudgetLimits, CapabilityDescription, CapabilityIdentity,
        CapabilityKind, CapabilityLifecycle, CapabilityMetadata, CapabilityName,
        CapabilityReference, ContentMetadata, ContentSource, ContentTrust, Digest, ExecutionCount,
        Extensions, Instruction, InstructionContent, InstructionIdentity, InstructionName,
        InstructionProvenance, ModelCapabilities, ModelDescriptor, ModelModalities, ModelModality,
        ModelProviderModelId, ModelStructuredOutputCapabilities, ModelTokenLimits,
        ModelToolCapabilities, PrincipalIdentity, RedactionState, ResolvedBudget, SchemaReference,
        ScopeSet, SecurityLabel, TenantId, TextContent, Timestamp, TokenCount, Version,
    },
    in_process_agent::{
        InProcessAgentBinding, InProcessAgentDependencies, InProcessAgentRequest,
        InProcessAgentRun, InProcessAgentRunOptions, InProcessAgentRuntime,
        InProcessAgentRuntimeOptions,
    },
    integrations::{
        ApiKey, DeepSeekResponsesModel, ProviderEndpoint, ProviderHttpOptions, StaticApiKey,
    },
    postgres::{PostgresStore, PostgresStoreConfig},
    runtime::{
        self, AgentBuilder, AgentServiceCaller, AgentServiceRegistryBuilder, AgentToolPolicy,
        AgentToolPolicyContext, AgentToolPolicyDecision, AgentToolPolicyError,
        AgentToolPolicyReference, DurableInvocationExecutor, DurableInvocationExecutorOptions,
        ExecutableGraphRegistryBuilder, JsonSchemaRegistryBuilder, ModelProviderRegistryBuilder,
        ModelTokenAccounting, ModelTokenRateCard, ProviderNativeAgentBudgetProvider,
        ProviderNativeAgentGraph, ProviderNativeAgentLifecycleEvidence,
        ToolProviderRegistryBuilder,
    },
};
use std::{
    error::Error,
    io::{self, Read},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const VERSION: Version = Version::new(1, 0, 0);
const DEEPSEEK_ENDPOINT: &str = "https://api.deepseek.com/";
const PROFILE_ID: &str = "https://stknot.com/schemas/examples/deepseek-schema-profile/1.0.0";
const MAX_CONFIG_BYTES: usize = 64 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    tenant: TenantId,
    agent_owner: PrincipalIdentity,
    caller: PrincipalIdentity,
    submission_key: AgentSubmissionKey,
    question: String,
    policy_valid_until: Timestamp,
    budget_limits: BudgetLimits,
    provider_model_id: ModelProviderModelId,
    rate_card: ModelTokenRateCard,
}

#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct ChatRequest {
    question: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ChatResponse {
    answer: String,
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

struct Dependencies(PostgresStore);

impl AgentWorkerReadiness for Dependencies {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentWorkerReadinessError>> {
        Box::pin(async {
            self.0
                .health_check()
                .await
                .map_err(|_| AgentWorkerReadinessError)
        })
    }
}

impl AgentMaintenanceReadiness for Dependencies {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>> {
        Box::pin(async {
            self.0
                .health_check()
                .await
                .map_err(|_| AgentMaintenanceReadinessError)
        })
    }
}

fn capability(owner: &PrincipalIdentity, name: &str) -> Result<CapabilityIdentity, Box<dyn Error>> {
    Ok(CapabilityIdentity::new(
        owner.clone(),
        CapabilityReference::new(CapabilityName::new(name)?, VERSION),
    ))
}

fn metadata(
    owner: &PrincipalIdentity,
    name: &str,
    kind: CapabilityKind,
) -> Result<CapabilityMetadata, Box<dyn Error>> {
    Ok(CapabilityMetadata::new(
        capability(owner, name)?,
        kind,
        None,
        CapabilityDescription::new("Recoverable DeepSeek-backed Agent")?,
        CapabilityLifecycle::active(),
        ScopeSet::empty(),
        Extensions::default(),
    )?)
}

fn schema_profile() -> Result<(SchemaReference, serde_json::Value), Box<dyn Error>> {
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": PROFILE_ID,
        "type": "object"
    });
    let reference = SchemaReference::new(
        PROFILE_ID.parse()?,
        VERSION,
        Digest::sha256(serde_json_canonicalizer::to_vec(&document)?),
    );
    Ok((reference, document))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pass one JSON config path"))?;
    if std::env::args().len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "pass exactly one JSON config path",
        )
        .into());
    }
    let mut input = std::fs::File::open(path)?.take((MAX_CONFIG_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes)?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "config exceeds 64 KiB").into());
    }
    let config: Config = serde_json::from_slice(&bytes)?;
    if config.question.trim().is_empty() || config.provider_model_id.as_str() != "deepseek-flash" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "nonempty question and exact deepseek-flash model are required",
        )
        .into());
    }
    // Refuse partial or expired policy limits before any provider/DB operation.
    ResolvedBudget::resolve(&[config.budget_limits.clone()])?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let now = Timestamp::from_unix_micros(i64::try_from(now.as_micros())?)?;
    if config.policy_valid_until <= now {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "policy has expired").into());
    }
    let key = ApiKey::new(std::env::var("DEEPSEEK_API_KEY")?)?;
    let store = PostgresStore::connect_config(PostgresStoreConfig::from_env()?).await?;
    store.health_check().await?;
    let result = Box::pin(execute(
        config,
        ProviderEndpoint::https(DEEPSEEK_ENDPOINT)?,
        key,
        store,
    ))
    .await?;
    println!("{}", result.answer);
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn execute(
    config: Config,
    endpoint: ProviderEndpoint,
    key: ApiKey,
    store: PostgresStore,
) -> Result<ChatResponse, Box<dyn Error>> {
    let (profile, profile_document) = schema_profile()?;
    let text = ModelModalities::try_new([ModelModality::Text])?;
    let model = ModelDescriptor::new(
        metadata(
            &config.agent_owner,
            "models.deepseek-flash",
            CapabilityKind::Model,
        )?,
        ModelCapabilities::new(
            text.clone(),
            text,
            false,
            ModelToolCapabilities::unsupported(),
            ModelStructuredOutputCapabilities::json_schema(profile.clone()),
            false,
            ModelTokenLimits::new(
                Some(TokenCount::new(128_000)),
                Some(TokenCount::new(120_000)),
                Some(TokenCount::new(8_000)),
            )?,
        )?,
    )?;
    let instruction_metadata = ContentMetadata::new(
        ContentSource::Application,
        ContentTrust::ApplicationControlled,
        SecurityLabel::new("internal/config")?,
        RedactionState::NotApplied,
    );
    let instructions = AgentInstructions::try_new([Instruction::new(
        InstructionIdentity::new(InstructionName::new("deepseek.answer")?, VERSION),
        InstructionContent::from(TextContent::new(
            "Answer the user's question. Return only a JSON object with one string field named answer.",
            None,
            instruction_metadata,
        )?),
        InstructionProvenance::new(config.agent_owner.clone()),
    )?])?;
    let definition = AgentBuilder::<ChatRequest, ChatResponse>::new(
        metadata(
            &config.agent_owner,
            "agents.deepseek-chat",
            CapabilityKind::Agent,
        )?,
        "https://stknot.com/schemas/examples/deepseek-chat/input/1.0.0".parse()?,
        "https://stknot.com/schemas/examples/deepseek-chat/output/1.0.0".parse()?,
        model.clone(),
        instructions,
        AgentExecutionConfig::new(
            AgentStructuredOutputStrategy::ModelNative,
            ExecutionCount::new(1),
            ExecutionCount::ZERO,
            ExecutionCount::ZERO,
            AgentToolConcurrency::sequential(),
        )?,
    )
    .build()?;
    let accounting = Arc::new(ModelTokenAccounting::new(
        capability(&config.agent_owner, "accounting.deepseek-chat")?,
        model.metadata().identity().clone(),
        config.provider_model_id.clone(),
        config.rate_card,
    )?);
    let graph = ProviderNativeAgentGraph::compile(
        definition.descriptor().clone(),
        capability(&config.agent_owner, "graphs.deepseek-chat")?,
        capability(&config.agent_owner, "reducers.deepseek-chat")?,
        "https://stknot.com/schemas/examples/deepseek-chat/state/1.0.0".parse()?,
        SecurityLabel::new("tenant/user-input")?,
        Arc::new(NoTools(AgentToolPolicyReference::new(
            capability(&config.agent_owner, "policies.no-tools")?,
            Digest::sha256(b"stateknot.examples.deepseek-chat.no-tools.v1"),
        ))),
        accounting,
    )?;
    let mut schemas =
        definition.register_schemas(JsonSchemaRegistryBuilder::with_default_limits())?;
    schemas.register(profile, profile_document)?;
    graph.register_schema(&mut schemas)?;
    runtime::register_standard_graph_driver_event_schema(&mut schemas)?;
    runtime::register_standard_graph_lifecycle_event_schema(&mut schemas)?;
    runtime::register_standard_agent_cancellation_event_schema(&mut schemas)?;
    runtime::register_standard_agent_admission_event_schema(&mut schemas)?;
    runtime::register_standard_agent_service_control_event_schema(&mut schemas)?;
    runtime::agent_policy::register_agent_policy_evidence_schema(&mut schemas)?;
    runtime::register_standard_invocation_execution_event_schema(&mut schemas)?;
    let schemas = schemas.build()?;
    let codec = definition.bind(Arc::new(schemas.clone()))?;

    let adapter = DeepSeekResponsesModel::new(
        model,
        config.provider_model_id,
        SecurityLabel::new("tenant/model-output")?,
        Arc::new(schemas.clone()),
        Arc::new(StaticApiKey::new(key)),
        endpoint,
        ProviderHttpOptions::default(),
    )?;
    let mut models = ModelProviderRegistryBuilder::new();
    models.register(Arc::new(adapter))?;
    let executor = DurableInvocationExecutor::new(
        store.clone(),
        schemas.clone(),
        models.build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(ProviderNativeAgentBudgetProvider::new(
            graph.clone(),
            store.clone(),
        )?),
        DurableInvocationExecutorOptions::default(),
    )?;
    let mut executable = ExecutableGraphRegistryBuilder::new(schemas.clone());
    graph.register_executable(&mut executable, store.clone(), executor, schemas)?;
    let executable = executable.build()?;
    store
        .register_graph_definition(config.tenant.clone(), graph.graph().clone())
        .await?;

    let caller = AgentServiceCaller::new(config.tenant.clone(), config.caller.clone());
    let mut deployments = AgentServiceRegistryBuilder::new();
    deployments.register(Arc::new(graph.clone()))?;
    let policy = Arc::new(AgentResourcePolicy::new(
        PolicyArtifact::new(PolicyDocument {
            format_version: 1,
            policy: capability(&config.agent_owner, "policies.deepseek-chat")?,
            valid_until: config.policy_valid_until,
            submissions: vec![SubmissionRule {
                tenant: config.tenant.clone(),
                principal: config.caller.clone(),
                agent: codec.descriptor().metadata().identity().clone(),
                input_schema: codec.descriptor().input_schema().clone(),
                granted_scopes: ScopeSet::empty(),
                budget_limits: config.budget_limits,
            }],
            runs: vec![RunRule {
                tenant: config.tenant.clone(),
                principal: config.caller,
                operation: RunPermission::Read,
                target: RunAccessTarget::Submission(
                    config.submission_key.digest_for(&config.tenant),
                ),
            }],
        })?,
        Duration::from_secs(60),
    )?);
    let dependencies = Arc::new(Dependencies(store.clone()));
    let binding = InProcessAgentBinding::tenant(
        store.clone(),
        executable,
        deployments.build(),
        policy,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            graph,
            store.clone(),
        )),
        config.tenant,
        AgentWorkerExecutionOptions::default(),
        AgentMaintenanceMutationOptions::default(),
    )?;
    let mut runtime = InProcessAgentRuntime::start(
        binding,
        InProcessAgentDependencies {
            worker: dependencies.clone(),
            maintenance: dependencies,
        },
        InProcessAgentRuntimeOptions::default(),
    )
    .await?;
    let agent = runtime
        .agent(codec, caller)?
        .with_options(InProcessAgentRunOptions::new(
            Duration::from_millis(50),
            Duration::from_secs(120),
        )?);
    let result = agent
        .run(InProcessAgentRequest::new(
            config.submission_key,
            ChatRequest {
                question: config.question,
            },
            BudgetLimits::empty(),
        ))
        .await;
    drop(agent);
    let report = runtime.shutdown().await?;
    store.close().await;
    if report.failure.is_some()
        || report.worker.is_none_or(|result| result.is_err())
        || report.maintenance.is_none_or(|result| result.is_err())
    {
        return Err(io::Error::other("Agent roles did not drain cleanly").into());
    }
    match result? {
        InProcessAgentRun::Succeeded { output, .. } => Ok(output),
        InProcessAgentRun::Pending { .. } => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "run remains durable; retry with the same config and submission key",
        )
        .into()),
        _ => Err(io::Error::other(
            "durable Agent run did not succeed; inspect its authorized snapshot",
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateknot::core::{EventId, RunId};
    use stateknot::postgres::{PostgresStoreOptions, PostgresTransportSecurity};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn example_executes_and_recovers_without_a_second_provider_call() {
        let url = match std::env::var("STATEKNOT_TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var_os("STATEKNOT_REQUIRE_POSTGRES_TESTS").is_none() => return,
            Err(error) => panic!("mandatory PostgreSQL URL is missing: {error}"),
        };
        let options = PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled);
        PostgresStore::migrate_database(&url, options.clone())
            .await
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("deepseek_agent.config.json")).unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let now = i64::try_from(now.as_micros()).unwrap();
        value["tenant"] = json!(format!("deepseek-example-{}", RunId::generate()));
        value["submission_key"] = json!(format!("deepseek-example-{}", EventId::generate()));
        value["budget_limits"]["deadline"] = json!(
            Timestamp::from_unix_micros(now + 3_600_000_000)
                .unwrap()
                .to_string()
        );
        value["policy_valid_until"] = json!(
            Timestamp::from_unix_micros(now + 86_400_000_000)
                .unwrap()
                .to_string()
        );
        let config: Config = serde_json::from_value(value).unwrap();
        ResolvedBudget::resolve(&[config.budget_limits.clone()]).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = ProviderEndpoint::loopback_http(&format!(
            "http://{}/v1/",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut captured = Vec::new();
            let mut buffer = [0_u8; 2048];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                captured.extend_from_slice(&buffer[..read]);
                if let Some(header_end) = captured.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&captured[..header_end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap();
                    if captured.len() >= header_end + 4 + length {
                        break;
                    }
                }
            }
            let body = serde_json::to_vec(&json!({
                "id": "resp_deepseek_example_01",
                "model": "deepseek-flash",
                "status": "completed",
                "output": [{
                    "type": "message", "id": "message_01", "status": "completed",
                    "role": "assistant", "phase": "final_answer",
                    "content": [{"type": "output_text", "text": "{\"answer\":\"OK\"}", "annotations": []}]
                }],
                "usage": {
                    "input_tokens": 30, "input_tokens_details": {"cached_tokens": 0},
                    "output_tokens": 10, "output_tokens_details": {"reasoning_tokens": 0},
                    "total_tokens": 40
                }
            }))
            .unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.shutdown().await.unwrap();
            captured
        });
        let key = ApiKey::new("test-only-key").unwrap();
        let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
        let result = execute(config.clone(), endpoint.clone(), key, store)
            .await
            .unwrap();
        assert_eq!(result.answer, "OK");
        let captured = String::from_utf8(server.await.unwrap()).unwrap();
        assert!(captured.starts_with("POST /v1/responses HTTP/1.1"));
        assert!(captured.contains("Bearer test-only-key"));

        let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
        let replay = execute(
            config.clone(),
            endpoint.clone(),
            ApiKey::new("invalid-key-on-replay").unwrap(),
            store,
        )
        .await
        .unwrap();
        assert_eq!(replay.answer, "OK");

        let store = PostgresStore::connect(&url, options).await.unwrap();
        let mut changed = config;
        changed.question.push_str(" changed");
        assert!(
            execute(
                changed,
                endpoint,
                ApiKey::new("invalid-key").unwrap(),
                store
            )
            .await
            .is_err()
        );
    }
}
