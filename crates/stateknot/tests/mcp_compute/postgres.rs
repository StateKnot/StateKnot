// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::{io::Write as _, process::Stdio};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _};

struct Reducer(GraphReducerReference);
impl GraphReducer for Reducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.0
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        let mut state = state.as_value().clone();
        for update in updates {
            state["value"] = update.update().data().as_value()["value"].clone();
        }
        Ok(BoundedJson::try_from_value(state).unwrap())
    }
}

async fn registry(
    graph: &CompiledGraph,
    schemas: JsonSchemaRegistry,
    address: std::net::SocketAddr,
) -> ExecutableGraphRegistry {
    let client = client(address).await;
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas.clone());
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(Reducer(graph.reducer().clone())))
        .unwrap();
    for id in ["double", "finish"] {
        let node = McpComputeNode::connect(
            client.clone(),
            binding(graph, id),
            schemas.clone(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        registry.register_node(Arc::new(node)).unwrap();
    }
    registry.build().unwrap()
}

async fn store() -> Option<PostgresStore> {
    let url = match std::env::var("STATEKNOT_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent)
            if std::env::var_os("STATEKNOT_REQUIRE_POSTGRES_TESTS").is_none() =>
        {
            return None;
        }
        Err(std::env::VarError::NotPresent) => panic!("mandatory compute PostgreSQL URL missing"),
        Err(std::env::VarError::NotUnicode(_)) => panic!("compute PostgreSQL URL must be Unicode"),
    };
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    Some(PostgresStore::connect(&url, options).await.unwrap())
}

async fn start(store: &PostgresStore, graph: &CompiledGraph) -> (TenantId, RunId) {
    let tenant = TenantId::new(format!("compute-{}", RunId::generate())).unwrap();
    let run = RunId::generate();
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let admitted = store
        .admit_run(AgentResultProvenance::new(
            tenant.clone(),
            run,
            ThreadId::generate(),
            InvocationId::generate(),
            capability("compute-agent"),
        ))
        .await
        .unwrap();
    let checkpoint = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        graph.reference(),
        CheckpointState::new(
            graph.state_schema().clone(),
            BoundedJson::try_from_value(json!({"value":7,"secret":"database-password-sentinel"}))
                .unwrap(),
        )
        .unwrap(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    let payload = JournalPayload::new(
        graph.input_schema().clone(),
        JournalEventKind::new("compute-test-start").unwrap(),
        BoundedJson::try_from_value(json!({"value":7})).unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant.clone(), run, EventId::generate(), payload)
            .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            append,
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            checkpoint,
        )
        .await
        .unwrap();
    (tenant, run)
}

fn driver(
    store: &PostgresStore,
    registry: ExecutableGraphRegistry,
    events: u32,
) -> DurableGraphDriver {
    DurableGraphDriver::new(
        store.clone(),
        registry,
        DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            events,
            Duration::from_secs(2),
            Duration::from_secs(10),
            3,
            Duration::from_millis(25),
        )
        .unwrap(),
    )
    .unwrap()
}

struct FixtureAuth;
impl McpServerAuthenticator for FixtureAuth {
    fn authenticate(
        &self,
        request: McpServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<McpServerPrincipal, McpServerAuthenticationError>> {
        Box::pin(async move {
            // Hermetic fixture only, not a production credential verifier.
            if request.credential().expose_secret() != "compute-only-fixture-token" {
                return Err(McpServerAuthenticationError::InvalidCredential);
            }
            Ok(McpServerPrincipal::new("compute-control-plane", ["compute:double"]).unwrap())
        })
    }
}

struct Double;
impl McpServerToolHandler for Double {
    fn call(
        &self,
        call: McpServerToolCall,
        _context: McpServerToolContext,
    ) -> BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>> {
        Box::pin(async move {
            assert_eq!(
                call.arguments().len(),
                1,
                "no runtime metadata or secret fields"
            );
            let value = call.arguments()["value"]
                .as_u64()
                .unwrap()
                .checked_mul(2)
                .unwrap();
            Ok(McpServerToolResult::structured([], json!({"value":value}))
                .unwrap()
                .into())
        })
    }
}

#[tokio::test]
#[ignore = "owned subprocess fixture; parent test supplies an empty environment"]
async fn compute_worker_child() {
    assert_eq!(std::env::var("STATEKNOT_COMPUTE_CHILD").as_deref(), Ok("1"));
    for name in [
        "STATEKNOT_TEST_DATABASE_URL",
        "DATABASE_URL",
        "PGPASSWORD",
        "PGPASSFILE",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
    ] {
        assert!(
            std::env::var_os(name).is_none(),
            "Worker must not inherit privileged credentials"
        );
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut tools = McpServerToolRegistryBuilder::default();
    tools
        .register(
            McpServerToolDefinition::new("double", value_schema())
                .unwrap()
                .with_output_schema(value_schema())
                .unwrap()
                .with_required_scopes(["compute:double"])
                .unwrap(),
            Double,
        )
        .unwrap();
    let handler = McpServerToolService::new(
        tools.build().unwrap(),
        McpServerApplicationOptions::new(
            "compute-worker",
            "1.0.0",
            8,
            Duration::from_secs(30),
            McpServerCacheScope::Private,
        )
        .unwrap(),
        AllowMcpServerToolAuthorization,
    )
    .unwrap();
    let service = McpServerHttpService::new(
        handler,
        McpServerHttpOptions::loopback(address.port()).unwrap(),
        McpServerAuthentication::bearer(
            FixtureAuth,
            McpServerBearerChallenge::new("compute", None::<String>).unwrap(),
        ),
    )
    .unwrap();
    println!("STATEKNOT_COMPUTE_WORKER_READY={address}");
    std::io::stdout().flush().unwrap();
    axum::serve(listener, Router::new().fallback_service(service))
        .await
        .unwrap();
}

async fn worker_process() -> (tokio::process::Child, std::net::SocketAddr) {
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "postgres::compute_worker_child",
            "--nocapture",
        ])
        .env_clear()
        .env("STATEKNOT_COMPUTE_CHILD", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Windows requires its loader directory, never a user configuration path.
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", root);
    }
    let mut child = command.spawn().unwrap();
    let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap().take(8192)).lines();
    let address = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(address) = line.strip_prefix("STATEKNOT_COMPUTE_WORKER_READY=") {
                return address.parse().unwrap();
            }
        }
        panic!("Worker exited before bounded startup handshake");
    })
    .await
    .unwrap();
    (child, address)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_compute_worker_recovery_and_authority_boundary() {
    let Some(store) = store().await else {
        return;
    };
    let (mut child, address) = worker_process().await;
    let (graph, schemas) = fixture();
    let (tenant, run) = start(&store, &graph).await;
    let registry1 = registry(&graph, schemas.clone(), address).await;
    let first_lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let first = driver(&store, registry1, 3)
        .drive(first_lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(first.outcome(), GraphDriveOutcome::Yielded { .. }));
    assert_eq!(first.report().node_attempts_completed(), 1);
    let checkpoint = store
        .load_current_checkpoint(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.superstep().get(), 1);
    assert_eq!(
        checkpoint.state().data().as_value(),
        &json!({"value":14,"secret":"database-password-sentinel"})
    );

    // A fresh executable registry replays a noninitial checkpoint, then yields
    // with a committed terminal node result but before the lifecycle barrier.
    let registry2 = registry(&graph, schemas.clone(), address).await;
    let lease2 = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(lease2.fence().epoch() > first_lease.fence().epoch());
    let second = driver(&store, registry2, 2)
        .drive(lease2.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        second.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    assert_eq!(second.report().node_attempts_completed(), 1);
    assert_eq!(second.report().replay().barriers_replayed(), 1);

    let registry3 = registry(&graph, schemas, address).await;
    // Stop the remote process: the pending result MUST suffice for recovery.
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(!status.success());
    let lease3 = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let third = driver(&store, registry3, 3)
        .drive(lease3.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = third.outcome() else {
        panic!("terminal barrier must not contact the stopped Worker")
    };
    let GraphBarrierDisposition::Terminal { output } = handoff.plan().disposition() else {
        panic!("host selected terminal mode")
    };
    assert_eq!(output.data().as_value(), &json!({"value":28}));
    assert_eq!(third.report().node_attempts_started(), 0);
    store.release_lease(lease3.fence()).await.unwrap();
    qualify_stale_and_hostile(&store).await;
    store.close().await;
    println!(
        "\nSTATEKNOT_COMPUTE_WORKER_EVIDENCE={{\"profile\":\"mcp-compute-worker-v1\",\"schema\":24,\"separate_process\":true,\"no_inherited_credentials\":true,\"first_party_mcp_server\":true,\"noninitial_replay\":true,\"pending_recovery_without_worker\":true,\"stale_fence_rejected\":true,\"hostile_result_rejected\":true,\"worker_reaped\":true,\"invariants\":\"passed\"}}"
    );
}

async fn qualify_stale_and_hostile(store: &PostgresStore) {
    let server = Server::start().await;
    let (graph, schemas) = fixture();
    let registry = registry(&graph, schemas, server.address).await;
    let (tenant, run) = start(store, &graph).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    server.peer.mode.store(12, Ordering::SeqCst);
    let old_driver = driver(store, registry.clone(), 3);
    let fence = lease.fence().clone();
    let old =
        tokio::spawn(async move { old_driver.drive(fence, CancellationSignal::never()).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while server.calls() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    store.release_lease(lease.fence()).await.unwrap();
    let successor = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(successor.fence().epoch() > lease.fence().epoch());
    server.peer.gate.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(5), old)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(&error,GraphDriverError::Store {source} if matches!(source.as_ref(),StoreError::StaleFence)),
        "{error:?}"
    );
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .unwrap()
            .superstep()
            .get(),
        0
    );
    store.release_lease(successor.fence()).await.unwrap();

    server.peer.mode.store(1, Ordering::SeqCst);
    let (tenant, run) = start(store, &graph).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = driver(store, registry, 3)
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(result.outcome(), GraphDriveOutcome::Blocked(_)));
    let checkpoint = store
        .load_current_checkpoint(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.superstep().get(), 0);
    assert_eq!(checkpoint.state().data().as_value()["value"], 7);
    store.release_lease(lease.fence()).await.unwrap();
}
