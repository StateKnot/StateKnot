// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
pub(super) const TOKEN: &str = "http-only-fixture-token";
pub(super) fn bounded(value: Value) -> BoundedJson {
    BoundedJson::try_from_value(value).unwrap()
}
fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.test/agent-http".parse().unwrap(),
            "http-tests".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}
struct Deployment {
    descriptor: AgentDescriptor,
    graph: CompiledGraph,
}
impl AgentServiceDeployment for Deployment {
    fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }
    fn graph(&self) -> &CompiledGraph {
        &self.graph
    }
    fn initial_state(&self) -> Result<CheckpointState, AgentServiceDeploymentError> {
        Ok(CheckpointState::new(
            self.graph.state_schema().clone(),
            bounded(json!({"value":1})),
        )
        .unwrap())
    }
}
struct Reducer(GraphReducerReference);
impl GraphReducer for Reducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.0
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        _: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        Ok(state.clone())
    }
}
struct Node {
    graph: GraphReference,
    id: NodeId,
    schema: SchemaReference,
    calls: Arc<AtomicUsize>,
}
impl GraphNodeExecutor for Node {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }
    fn node_id(&self) -> &NodeId {
        &self.id
    }
    fn execute(
        &self,
        _: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                NodeControl::Terminal {
                    output: NodeTerminalOutput::new(
                        self.schema.clone(),
                        bounded(json!({"value":1})),
                    )
                    .unwrap(),
                },
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}
pub(super) struct Auth {
    caller: AgentServiceCaller,
    pub block: AtomicBool,
    pub entered: Notify,
    pub revoked: AtomicBool,
}
impl AgentHttpAuthenticator for Auth {
    fn authenticate(
        &self,
        credential: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>> {
        Box::pin(async move {
            if self.revoked.load(Ordering::SeqCst) {
                return Err(AgentHttpAuthenticationError::Unauthenticated);
            }
            if self.block.load(Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            let mut caller = self.caller.clone();
            let operations = match credential.expose_secret() {
                TOKEN => vec![
                    AgentHttpOperation::Submit,
                    AgentHttpOperation::Read,
                    AgentHttpOperation::Cancel,
                ],
                "read-only-fixture" => vec![AgentHttpOperation::Read],
                "other-tenant-fixture" => {
                    caller = AgentServiceCaller::new(
                        TenantId::new("other-http-tenant").unwrap(),
                        caller.principal().clone(),
                    );
                    vec![AgentHttpOperation::Read]
                }
                _ => return Err(AgentHttpAuthenticationError::Unauthenticated),
            };
            Ok(AgentHttpPrincipal::new(caller, operations))
        })
    }
}
struct Policy {
    caller: AgentServiceCaller,
    submission: AgentServiceSubmissionGrant,
    run: AgentServiceRunGrant,
    denied: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}
impl AgentServiceAuthorizer for Policy {
    fn authorize_submission(
        &self,
        context: AgentServiceSubmissionAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceSubmissionGrant, AgentServiceAuthorizationError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.denied.load(Ordering::SeqCst) || context.caller() != &self.caller {
                return Err(AgentServiceAuthorizationError::Denied);
            }
            Ok(self.submission.clone())
        })
    }
    fn authorize_run(
        &self,
        context: AgentServiceRunAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceRunGrant, AgentServiceAuthorizationError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.denied.load(Ordering::SeqCst) || context.caller() != &self.caller {
                return Err(AgentServiceAuthorizationError::Denied);
            }
            Ok(self.run.clone())
        })
    }
}
pub(super) struct Fixture {
    pub store: PostgresStore,
    pub service: AgentServiceV1,
    pub caller: AgentServiceCaller,
    pub schema: SchemaReference,
    agent: CapabilityIdentity,
    pub auth: Arc<Auth>,
    pub denied: Arc<AtomicBool>,
    pub policy_calls: Arc<AtomicUsize>,
    pub node_calls: Arc<AtomicUsize>,
    pub executable: ExecutableGraphRegistry,
    pub deployments: AgentServiceRegistry,
    pub policy: Arc<dyn AgentServiceAuthorizer>,
}
impl Fixture {
    pub(super) async fn new() -> Option<Self> {
        Self::for_tenant(TenantId::new(format!("http-{}", RunId::generate())).unwrap()).await
    }
    pub(super) async fn for_tenant(tenant: TenantId) -> Option<Self> {
        Self::for_graph(tenant, "http-graph").await
    }
    pub(super) async fn for_graph(tenant: TenantId, graph_name: &str) -> Option<Self> {
        let url = match std::env::var("STATEKNOT_TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(std::env::VarError::NotPresent)
                if std::env::var_os("STATEKNOT_REQUIRE_POSTGRES_TESTS").is_none() =>
            {
                return None;
            }
            Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => {
                panic!("mandatory Agent HTTP PostgreSQL URL missing or invalid")
            }
        };
        let options = PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled);
        PostgresStore::migrate_database(&url, options.clone())
            .await
            .unwrap();
        let store = PostgresStore::connect(&url, options).await.unwrap();
        let document = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://stknot.com/schemas/tests/agent-http-value/1.0.0","type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false});
        let schema = SchemaReference::new(
            document["$id"].as_str().unwrap().parse().unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(serde_json_canonicalizer::to_vec(&document).unwrap()),
        );
        let id = NodeId::new("finish").unwrap();
        let graph = CompiledGraph::compile(
            capability(graph_name),
            schema.clone(),
            schema.clone(),
            schema.clone(),
            schema.clone(),
            GraphReducerReference::new(
                capability("http-reducer"),
                Digest::sha256(b"http-reducer-v1"),
            ),
            ReadyNodes::try_new([id.clone()]).unwrap(),
            [GraphNode::new(id.clone(), None, GraphRoutes::empty(), None, true).unwrap()],
            GraphExecutionLimits::new(Superstep::new(4).unwrap(), 1).unwrap(),
        )
        .unwrap();
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
        let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
        schemas.register(schema.clone(), document).unwrap();
        register_standard_graph_driver_event_schema(&mut schemas).unwrap();
        register_standard_agent_admission_event_schema(&mut schemas).unwrap();
        register_standard_agent_service_control_event_schema(&mut schemas).unwrap();
        let mut registry = ExecutableGraphRegistryBuilder::new(schemas.build().unwrap());
        registry.register_graph(graph.clone()).unwrap();
        registry
            .register_reducer(Arc::new(Reducer(graph.reducer().clone())))
            .unwrap();
        let node_calls = Arc::new(AtomicUsize::new(0));
        registry
            .register_node(Arc::new(Node {
                graph: graph.reference(),
                id,
                schema: schema.clone(),
                calls: node_calls.clone(),
            }))
            .unwrap();
        let raw: Value = serde_json::from_str(include_str!(
            "../../../stateknot-core/tests/fixtures/core-agent-v1.json"
        ))
        .unwrap();
        let mut descriptor = raw["descriptors"]["valid"][0].clone();
        descriptor["input_schema"] = serde_json::to_value(&schema).unwrap();
        descriptor["output_schema"] = serde_json::to_value(&schema).unwrap();
        descriptor["metadata"]["identity"] =
            serde_json::to_value(capability("http-agent")).unwrap();
        let descriptor: AgentDescriptor = serde_json::from_value(descriptor).unwrap();
        let agent = descriptor.metadata().identity().clone();
        let principal = capability("http-policy").owner().clone();
        let caller = AgentServiceCaller::new(tenant, principal.clone());
        let evidence = JournalPayload::new(
            schema.clone(),
            JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND).unwrap(),
            bounded(json!({"value":1})),
        )
        .unwrap();
        let authority = AgentAdmissionAuthority::new(
            principal.clone(),
            descriptor.metadata().required_scopes().clone(),
            capability("http-policy"),
            Digest::sha256(b"http-policy-v1"),
            evidence,
        )
        .unwrap();
        let mut budgets: Value = serde_json::from_str(include_str!(
            "../../../stateknot-core/tests/fixtures/core-agent-runtime-v1.json"
        ))
        .unwrap();
        budgets["base_budget_layers"][0]["deadline"] = json!("2099-01-01T00:00:00.000000Z");
        let budget = AgentAdmissionBudgetLayer::new(
            capability("http-budget"),
            authority.evidence().digest(),
            serde_json::from_value(budgets["base_budget_layers"][0].clone()).unwrap(),
        )
        .unwrap();
        let denied = Arc::new(AtomicBool::new(false));
        let policy_calls = Arc::new(AtomicUsize::new(0));
        let policy = Arc::new(Policy {
            caller: caller.clone(),
            submission: AgentServiceSubmissionGrant::new(authority, vec![budget]),
            run: AgentServiceRunGrant::new(
                principal,
                capability("http-run-policy"),
                Digest::sha256(b"http-run-policy-v1"),
                Digest::sha256(b"http-run-allow-v1"),
            ),
            denied: denied.clone(),
            calls: policy_calls.clone(),
        });
        let mut deployments = AgentServiceRegistryBuilder::new();
        deployments
            .register(Arc::new(Deployment { descriptor, graph }))
            .unwrap();
        let executable = registry.build().unwrap();
        let deployments = deployments.build();
        let service = AgentServiceV1::new(
            store.clone(),
            executable.clone(),
            deployments.clone(),
            policy.clone(),
        )
        .unwrap();
        let auth = Arc::new(Auth {
            caller: caller.clone(),
            block: AtomicBool::new(false),
            entered: Notify::new(),
            revoked: AtomicBool::new(false),
        });
        Some(Self {
            store,
            service,
            caller,
            schema,
            agent,
            auth,
            denied,
            policy_calls,
            node_calls,
            executable,
            deployments,
            policy,
        })
    }
    pub(super) fn submission(&self) -> AgentHttpSubmission {
        AgentHttpSubmission {
            submission_key: AgentSubmissionKey::new(format!("http-{}", EventId::generate()))
                .unwrap(),
            agent: self.agent.clone(),
            request: AgentRequest::new(
                self.schema.clone(),
                bounded(json!({"value":1})),
                BudgetLimits::empty(),
            ),
        }
    }
}
