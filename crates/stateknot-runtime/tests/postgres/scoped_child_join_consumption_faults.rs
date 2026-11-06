// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real scoped result/Join-consumption COMMIT cuts and cold Driver recovery.
//! The child runs the existing application before either fault is armed.
use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, parent_control, publish_ready},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;

const SELECTED: &str = "scoped_join_close_process_faults::fresh_join::consumption_faults::scoped_join_consumption_commit_loss_survives_real_kill_and_cold_driver";
const LEASE: Duration = Duration::from_secs(12);

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    key: ChildRunKey,
    fence: RunFence,
    publication: ChildRunJoinHead,
}

fn driver_options() -> DurableGraphDriverOptions {
    DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        1_024,
        LEASE / 4,
        Duration::from_secs(15 * 60),
        3,
        Duration::from_millis(25),
    )
    .unwrap()
}

async fn connect_store(port: Option<u16>, pool: u32) -> PostgresStore {
    let mut connection: PgConnectOptions =
        std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    if let Some(port) = port {
        connection = connection.host("127.0.0.1").port(port);
    }
    PostgresStore::connect(
        connection.to_url_lossy().as_str(),
        PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled)
            .with_pool_size(1, pool)
            .with_acquire_timeout(Duration::from_secs(30))
            .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
            .with_lease_timing(LEASE, Duration::from_secs(5 * 60)),
    )
    .await
    .unwrap()
}

// Pause the actual resumed application callback after its typed Join read, but
// before Driver may commit its result. This creates an observed before image
// which already includes the legal F2 caller rebind and Start. No SQL clock or
// persisted production state is edited by the rendezvous.
struct PauseBeforeResult {
    inner: Arc<dyn GraphNodeExecutor>,
    resume: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl GraphNodeExecutor for PauseBeforeResult {
    fn graph(&self) -> &GraphReference {
        self.inner.graph()
    }
    fn node_id(&self) -> &NodeId {
        self.inner.node_id()
    }
    fn supports_child_join(&self) -> bool {
        true
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            assert!(context.child_join().is_some());
            let result = self.inner.execute(context).await?;
            let resume = self.resume.lock().await.take().unwrap();
            publish_ready(&json!({"phase":"before_scoped_result_commit"}));
            resume.await.unwrap();
            Ok(result)
        })
    }
}

fn pause_registry(
    deployment: &ExecutableGraphRegistry,
    fixture: &DriverFixture,
    leaf: &CompiledGraph,
    native: &ProviderNativeFixture,
    resume: tokio::sync::oneshot::Receiver<()>,
) -> ExecutableGraphRegistry {
    let mut builder = ExecutableGraphRegistryBuilder::new(deployment.schemas().clone());
    for graph in [&fixture.graph, native.definition.graph()] {
        builder
            .register_reducer(Arc::new(RetainedChildReducer(
                deployment.resolve(&graph.reference()).unwrap().clone(),
            )))
            .unwrap();
    }
    let mut resume = Some(resume);
    for graph in [&fixture.graph, leaf, native.definition.graph()] {
        builder.register_graph(graph.clone()).unwrap();
        let executable = deployment.resolve(&graph.reference()).unwrap();
        for node in graph.nodes() {
            let Some(executor) = executable.node_executor(node.node_id()) else {
                continue;
            };
            if graph == leaf && node.node_id().as_str() == "Step_A" {
                builder
                    .register_node(Arc::new(PauseBeforeResult {
                        inner: executor,
                        resume: tokio::sync::Mutex::new(resume.take()),
                    }))
                    .unwrap();
            } else {
                builder.register_node(executor).unwrap();
            }
        }
    }
    assert!(resume.is_none());
    builder.build().unwrap()
}

// This setup performs fresh Runtime suspension, actual Model/Tool execution,
// terminal settlement, and publication. It uses the positive test's existing
// graph, schema, executor, DIRECT-ledger and admission builders.
#[allow(clippy::too_many_lines)]
async fn prepare_published(store: &PostgresStore) -> (Scenario, Snapshot) {
    let native = provider_native_fixture_with_budget(
        store.clone(),
        false,
        false,
        false,
        None,
        NativeBudgetMode::Durable,
    );
    let (fixture, leaf) = fresh_graphs(native.definition.graph(), native.definition.descriptor());
    let schemas = fresh_schemas(&fixture, &leaf, &native);
    let tenant = tenant("scoped-child-join-consumption-fault");
    let ids = AgentRunIds::generate();
    let run = ids.run_id();
    let request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let parent_intent = request.intent();
    let child_ids = AgentRunIds::generate();
    let candidate = stateknot_core::AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            tenant.clone(),
            child_ids.run_id(),
            child_ids.thread_id(),
            child_ids.invocation_id(),
            native.definition.descriptor(),
        ),
        native.definition.descriptor().clone(),
        AgentRequest::new(
            native.input_schema.clone(),
            BoundedJson::try_from_value(json!({"question":"Run the real child application."}))
                .unwrap(),
            BudgetLimits::empty()
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_input_bytes(ByteCount::new(65_536))
                .with_output_bytes(ByteCount::new(65_536)),
        ),
        parent_intent.budget_layers().iter().cloned(),
        native.definition.graph().reference(),
        parent_intent.authority().clone(),
    )
    .unwrap();
    let counters = FreshCounters::new();
    let direct = Arc::new(FreshDirectEvidence {
        store: store.clone(),
        key: Arc::new(tokio::sync::Mutex::new(None)),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let deployment = fresh_registry(
        store, &fixture, &leaf, &native, &candidate, &schemas, &direct, &counters,
    );
    for graph in [&fixture.graph, &leaf, native.definition.graph()] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    DurableAgentAdmission::new(store.clone(), deployment.clone())
        .unwrap()
        .admit(request)
        .await
        .unwrap();
    let first = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), deployment, driver_options())
        .unwrap()
        .with_direct_usage_evidence(direct.clone());
    let outcome = driver
        .drive(first.lease().fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::ChildJoin(request) = outcome.outcome() else {
        panic!("expected fresh scoped suspension: {outcome:?}");
    };
    assert_eq!(request.keys().len(), 1);
    let key = request.keys()[0].clone();
    assert!(!key.parent().graph_namespace().is_root());
    assert_eq!(direct.key.lock().await.as_ref(), Some(&key));
    assert_eq!(counters.launched.load(Ordering::SeqCst), 1);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 0);
    assert!(
        store
            .load_run(&tenant, run)
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    assert!(matches!(
        store.claim_lease(&tenant, run, AttemptId::generate()).await,
        Err(StoreError::RunNotRunnable)
    ));
    let child_run = candidate.provenance().run_id();
    let child_lease = store
        .claim_lease(&tenant, child_run, AttemptId::generate())
        .await
        .unwrap();
    let child_loop = DurableAgentLoop::new(
        store.clone(),
        native.registry.clone(),
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            native.definition.clone(),
            store.clone(),
        )),
        driver_options(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = child_loop
        .run(
            child_lease.lease().fence().clone(),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert!(matches!(result.outcome(), AgentLoopOutcome::Succeeded(_)));
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 1);
    let child = store.load_run(&tenant, child_run).await.unwrap();
    assert_eq!(child.lifecycle().status(), RunStatus::Succeeded);
    assert_eq!(
        child
            .lifecycle()
            .terminal_usage()
            .unwrap()
            .model_turns()
            .get(),
        2
    );
    assert_eq!(
        child
            .lifecycle()
            .terminal_usage()
            .unwrap()
            .tool_calls()
            .get(),
        1
    );
    let reconciled = DurableChildReconciler::new(
        store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(reconciled.items().len(), 1);
    reconciled.items()[0].result().unwrap();
    let published = DurableChildJoinPublisher::new(
        store.clone(),
        schemas,
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(published.items().len(), 1);
    published.items()[0].result().unwrap();
    let join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    assert!(join.consumed().is_none());
    assert_eq!(
        join.binding().unwrap().terminals()[0].usage(),
        child.lifecycle().terminal_usage().unwrap()
    );
    let publication = join.head().unwrap().clone();
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(fence.epoch() > first.lease().fence().epoch());
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let settled = child_facts(snapshot(&pool, &tenant).await, child_run);
    pool.close().await;
    (
        Scenario {
            key,
            fence,
            publication,
        },
        settled,
    )
}

#[allow(clippy::too_many_lines)]
async fn actor(input: Value) {
    let resume = parent_control();
    let scenario: Scenario = serde_json::from_value(input["scenario"].clone()).unwrap();
    let cut = input["role"].as_str() == Some("cut");
    let port = if cut {
        Some(u16::try_from(input["proxy_port"].as_u64().unwrap()).unwrap())
    } else {
        None
    };
    let store = connect_store(port, if cut { 1 } else { 8 }).await;
    let native = provider_native_fixture_with_budget(
        store.clone(),
        false,
        false,
        false,
        None,
        NativeBudgetMode::Durable,
    );
    let (fixture, leaf) = fresh_graphs(native.definition.graph(), native.definition.descriptor());
    let schemas = fresh_schemas(&fixture, &leaf, &native);
    let candidate = store
        .load_child_run(&scenario.key)
        .await
        .unwrap()
        .intent()
        .child()
        .clone();
    let counters = FreshCounters::new();
    let direct = Arc::new(FreshDirectEvidence {
        store: store.clone(),
        key: Arc::new(tokio::sync::Mutex::new(Some(scenario.key.clone()))),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let deployment = fresh_registry(
        &store, &fixture, &leaf, &native, &candidate, &schemas, &direct, &counters,
    );
    let deployment = if cut {
        pause_registry(&deployment, &fixture, &leaf, &native, resume)
    } else {
        assert_eq!(input["role"].as_str(), Some("recover"));
        publish_ready(&json!({"phase":"cold_before_driver"}));
        resume.await.unwrap();
        deployment
    };
    let fence = if cut {
        scenario.fence.clone()
    } else {
        serde_json::from_value(input["replacement_fence"].clone()).unwrap()
    };
    let driver = DurableGraphDriver::new(store.clone(), deployment, driver_options())
        .unwrap()
        .with_direct_usage_evidence(direct);
    let outcome = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(!cut, "instrumented consumption returned before writer kill");
    assert!(matches!(
        outcome.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    let committed = input["already_committed"].as_bool().unwrap();
    assert_eq!(counters.launched.load(Ordering::SeqCst), 0);
    assert_eq!(
        counters.resumed.load(Ordering::SeqCst),
        usize::from(!committed)
    );
    assert_eq!(counters.leaf_tail.load(Ordering::SeqCst), 1);
    assert_eq!(counters.root_tail.load(Ordering::SeqCst), 1);
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.report().frame_returns_committed(), 1);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let stable = business_facts(
        snapshot(&pool, scenario.key.parent().tenant_id()).await,
        fence.run_id(),
    );
    let again = driver
        .drive(fence, CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        again.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(again.report().node_attempts_started(), 0);
    assert_eq!(again.report().frame_returns_committed(), 0);
    assert_eq!(
        stable,
        business_facts(
            snapshot(&pool, scenario.key.parent().tenant_id()).await,
            scenario.fence.run_id()
        )
    );
    assert_eq!(
        counters.resumed.load(Ordering::SeqCst),
        usize::from(!committed)
    );
    pool.close().await;
    store.close().await;
}

async fn backend_disconnected(pool: &sqlx_postgres::PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let gone: bool =
                query_scalar("SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                    .bind(pid)
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if gone {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("owned killed writer backend must disconnect");
}

// Read every physical attempt through the provider's bounded owning cursor.
async fn retained_attempt_history(
    store: &PostgresStore,
    activation: &stateknot_core::NodeActivation,
) -> Vec<stateknot_core::NodeAttempt> {
    let mut records = Vec::new();
    let mut cursor = None;
    let bound = stateknot_core::ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE;
    for _ in 0..bound {
        let page = Box::pin(store.load_node_attempt_history_page(
            activation,
            cursor.as_ref(),
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        ))
        .await
        .unwrap();
        assert!(!page.records().is_empty() || !page.has_more());
        records.extend_from_slice(page.records());
        assert!(
            records.len() <= bound,
            "physical history exceeded the original attempt bound"
        );
        if !page.has_more() {
            return records;
        }
        let next = page.next_cursor().expect("nonempty owning page cursor");
        assert!(
            cursor
                .as_ref()
                .is_none_or(
                    |old: &stateknot_core::NodeAttempt| old.start().fence().epoch()
                        < next.start().fence().epoch()
                )
        );
        cursor = Some(next);
    }
    panic!("bounded physical history pagination did not terminate");
}

// These four facts must either all be absent or identify the same completion.
async fn consumption_bundle(store: &PostgresStore, scenario: &Scenario) -> Option<Value> {
    let join = store
        .load_child_join(scenario.key.parent())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(join.head(), Some(&scenario.publication));
    let pending = match store.load_pending_node_result(scenario.key.parent()).await {
        Ok(result) => Some(result),
        Err(StoreError::PendingNodeResultNotFound) => None,
        Err(error) => panic!("pending result failed integrity validation: {error:?}"),
    };
    let history = Box::pin(retained_attempt_history(store, scenario.key.parent())).await;
    let completed: Vec<_> = history
        .as_slice()
        .iter()
        .filter(|r| r.completion().is_some())
        .collect();
    if let Some(head) = join.consumed() {
        let pending = pending.unwrap();
        assert_eq!(pending.journal_head(), head);
        assert_eq!(pending.intent().child_join(), Some(&scenario.publication));
        assert_eq!(completed.len(), 1);
        let completion = completed[0].completion().unwrap();
        assert_eq!(completed[0].status(), NodeAttemptStatus::Succeeded);
        assert_eq!(completion.journal_head(), head);
        Some(json!({"join":{
            "request":join.request(),"registration":join.registration(),
            "binding":join.binding(),"head":join.head(),"consumed":join.consumed()
        },"pending":pending,"attempt":completed[0]}))
    } else {
        assert!(
            pending.is_none(),
            "result without original Join consumption"
        );
        assert!(
            completed.is_empty(),
            "completion without original Join consumption"
        );
        None
    }
}

fn assert_atomic_row_delta(before: &Snapshot, after: &Snapshot) {
    let expected: std::collections::BTreeSet<_> = [
        "child_run_join_consumptions",
        "child_run_joins",
        "node_attempt_completions",
        "pending_node_results",
        "run_events",
        "runs",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let changed: std::collections::BTreeSet<_> = before
        .iter()
        .filter(|(table, rows)| after.get(*table) != Some(*rows))
        .map(|(table, _)| table.clone())
        .collect();
    assert_eq!(
        changed, expected,
        "consumption COMMIT changed unrelated owning facts"
    );
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for table in [
        "child_run_join_consumptions",
        "node_attempt_completions",
        "pending_node_results",
        "run_events",
    ] {
        let before_rows: Value = serde_json::from_str(&before[table]).unwrap();
        let after_rows: Value = serde_json::from_str(&after[table]).unwrap();
        let before_rows = before_rows.as_array().unwrap();
        let after_rows = after_rows.as_array().unwrap();
        assert_eq!(
            after_rows.len(),
            before_rows.len() + 1,
            "one exact appended row required in {table}"
        );
        assert!(
            before_rows.iter().all(|row| after_rows.contains(row)),
            "immutable rows changed in {table}"
        );
    }
}

#[allow(clippy::too_many_lines)]
async fn run_cut(
    store: &PostgresStore,
    pool: &sqlx_postgres::PgPool,
    connection: &PgConnectOptions,
    cut: Cut,
) {
    let (scenario, settled_child) = Box::pin(prepare_published(store)).await;
    let tenant = scenario.key.parent().tenant_id();
    let run = scenario.fence.run_id();
    assert!(consumption_bundle(store, &scenario).await.is_none());
    let mut proxy =
        CommitProxy::start(connection, cut, TransactionTarget::ChildJoinConsumption).await;
    let mut writer = TestProcess::spawn(
        SELECTED,
        &json!({"role":"cut","scenario":scenario,"proxy_port":proxy.port()}),
    );
    assert_eq!(
        writer.ready().await.unwrap()["phase"],
        "before_scoped_result_commit"
    );
    let before = business_facts(snapshot(pool, tenant).await, run);
    assert!(consumption_bundle(store, &scenario).await.is_none());
    // The exact F2 caller and scoped node Start are already durable. The writer
    // has also executed the published callback once; rollback does not undo I/O.
    let history = Box::pin(retained_attempt_history(store, scenario.key.parent())).await;
    assert_eq!(history.as_slice().len(), 2);
    assert_eq!(history.as_slice()[1].start().fence(), &scenario.fence);
    assert_eq!(history.as_slice()[1].status(), NodeAttemptStatus::Executing);
    assert!(history.as_slice()[1].completion().is_none());
    writer.resume();
    proxy.wait_cut(cut).await;
    let backend = proxy.backend_pid();
    let at_cut = business_facts(snapshot(pool, tenant).await, run);
    let committed = cut == Cut::CommitResponse;
    let original = consumption_bundle(store, &scenario).await;
    if committed {
        assert!(original.is_some());
        assert_atomic_row_delta(&before, &at_cut);
        // The unacknowledged COMMIT has completed the scoped node, but no
        // barrier/Return or downstream application work can yet have run.
        assert!(
            store
                .load_graph_frame_return(tenant, run, scenario.key.parent().graph_namespace())
                .await
                .unwrap()
                .is_none()
        );
        let current = store.load_run(tenant, run).await.unwrap();
        let join = store
            .load_child_join(scenario.key.parent())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.journal_head(), join.consumed());
        assert_eq!(
            json!(history.as_slice()[1].start()),
            original.as_ref().unwrap()["attempt"]["start"]
        );
    } else {
        assert!(original.is_none());
        assert_eq!(
            at_cut, before,
            "unforwarded consumption COMMIT leaked any business facts"
        );
    }
    writer.kill_and_reap().await;
    proxy.stop().await;
    backend_disconnected(pool, backend).await;
    assert_eq!(business_facts(snapshot(pool, tenant).await, run), at_cut);
    assert_eq!(consumption_bundle(store, &scenario).await, original);
    let expires = store
        .load_run(tenant, run)
        .await
        .unwrap()
        .lease()
        .unwrap()
        .expires_at();
    tokio::time::timeout(Duration::from_secs(45), async {
        while store.observe_database_clock().await.unwrap() < expires {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real public lease expiry must precede replacement claim");
    let replacement = store
        .claim_lease(tenant, run, AttemptId::generate())
        .await
        .unwrap();
    let fence = replacement.lease().fence().clone();
    assert!(fence.epoch() > scenario.fence.epoch());
    assert_ne!(fence.attempt_id(), scenario.fence.attempt_id());
    let before_stale = snapshot(pool, tenant).await;
    assert!(matches!(
        store
            .renew_lease(&scenario.fence, replacement.lease().expires_at())
            .await,
        Err(StoreError::StaleFence)
    ));
    assert_eq!(snapshot(pool, tenant).await, before_stale);
    let mut recovery = TestProcess::spawn(
        SELECTED,
        &json!({"role":"recover","scenario":scenario,"already_committed":committed,"replacement_fence":fence}),
    );
    assert_eq!(
        recovery.ready().await.unwrap()["phase"],
        "cold_before_driver"
    );
    assert_eq!(
        snapshot(pool, tenant).await,
        before_stale,
        "cold registry construction mutated durable facts"
    );
    recovery.resume();
    recovery.wait_success().await;
    let final_bundle = consumption_bundle(store, &scenario).await.unwrap();
    if let Some(original) = original {
        assert_eq!(
            final_bundle, original,
            "cold Driver must reuse exact completed result and original consumption"
        );
    } else {
        assert_eq!(final_bundle["attempt"]["start"]["fence"], json!(fence));
    }
    let final_history = Box::pin(retained_attempt_history(store, scenario.key.parent())).await;
    assert_eq!(
        final_history.as_slice().len(),
        if committed { 2 } else { 3 }
    );
    assert_eq!(
        final_history.as_slice()[0].start().head(),
        history.as_slice()[0].start().head()
    );
    assert_eq!(
        final_history.as_slice()[1].start().head(),
        history.as_slice()[1].start().head()
    );
    assert!(final_history.as_slice()[0].completion().is_none());
    assert_eq!(
        final_history.as_slice()[1].completion().is_some(),
        committed
    );
    let final_rows = snapshot(pool, tenant).await;
    let consumptions: Value =
        serde_json::from_str(&final_rows["child_run_join_consumptions"]).unwrap();
    assert_eq!(
        consumptions.as_array().unwrap().len(),
        1,
        "only the original publication may be consumed once"
    );
    let returned = store
        .load_graph_frame_return(tenant, run, scenario.key.parent().graph_namespace())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(returned.completion().start().fence(), &fence);
    assert_eq!(
        returned.completion().outcome().result(),
        Some(&returned.result().head())
    );
    assert!(
        store
            .load_active_graph_frame(tenant, run)
            .await
            .unwrap()
            .is_none()
    );
    let parent = store.load_run(tenant, run).await.unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    assert!(parent.lifecycle().result().is_none());
    assert_eq!(parent.checkpoint().unwrap().superstep().get(), 1);
    let child_run = store
        .load_child_run(&scenario.key)
        .await
        .unwrap()
        .intent()
        .child()
        .provenance()
        .run_id();
    assert_eq!(
        child_facts(snapshot(pool, tenant).await, child_run),
        settled_child
    );
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
        .unwrap();
    println!(
        "\nSTATEKNOT_SCOPED_JOIN_CONSUMPTION_FAULT_EVIDENCE={}",
        json!({
            "profile":"scoped-join-consumption-process-fault-v1",
            "cut":if committed {"commit_response_withheld"} else {"commit_not_forwarded"},
            "commit_forwarded":committed,"commit_response_forwarded":false,
            "writer_killed_and_reaped":true,"cold_registry_and_pool":true,"real_lease_expiry":true,
            "higher_fence_driver":true,"result_and_original_join_atomic":true,
            "cold_published_callback_calls":i32::from(!committed),
            "fresh_child_model_calls":2,"fresh_child_tool_calls":1,"cold_child_io":0,
            "original_completed_reused":committed,"one_scoped_return":true,"root_active_handoff":true,
            "invariants":"passed","postgres":version,
            "termination":if cfg!(unix) {"SIGKILL"} else {"Child::kill"},
            "os":std::env::consts::OS,"arch":std::env::consts::ARCH
        })
    );
}

#[tokio::test]
async fn scoped_join_consumption_commit_loss_survives_real_kill_and_cold_driver() {
    if let Ok(input) = std::env::var(INPUT_ENV) {
        actor(serde_json::from_str(&input).unwrap()).await;
        return;
    }
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let connection = std::env::var(DATABASE_URL_ENV)
        .ok()
        .map(|url| url.parse::<PgConnectOptions>().unwrap());
    if let Some(connection) = &connection {
        let _ = loopback_target(connection);
    }
    let Some(store) = test_store_with_lease_duration(LEASE).await else {
        return;
    };
    let connection = connection.unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
        Box::pin(run_cut(&store, &pool, &connection, cut)).await;
    }
    pool.close().await;
    store.close().await;
}
