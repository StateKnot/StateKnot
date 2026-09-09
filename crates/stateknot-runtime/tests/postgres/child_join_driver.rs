// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_runtime::{
    DurableChildJoinPublisher, DurableChildReconciler, DurableChildReconcilerOptions,
};

struct JoinExecutor {
    store: PostgresStore,
    graph: GraphReference,
    intent: ChildRunAdmissionIntent,
    schemas: JsonSchemaRegistry,
    launched: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
    join_again: bool,
}
impl GraphNodeExecutor for JoinExecutor {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }
    fn node_id(&self) -> &NodeId {
        self.intent.key().parent().node_id()
    }
    fn supports_child_join(&self) -> bool {
        true
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            let request = ChildRunJoinRequest::new([self.intent.key().clone()]).unwrap();
            if let Some(join) = context.child_join() {
                assert_eq!(join.binding().request(), &request);
                let child = join.load_child(self.intent.key().slot()).await.unwrap();
                assert!(matches!(
                    child.status(),
                    RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
                ));
                if let Some(result) = child.result() {
                    assert_eq!(result.output().as_value(), &json!({"ok": true}));
                }
                assert!(matches!(
                    join.load_child(&ChildRunSlot::new("not-sealed").unwrap())
                        .await,
                    Err(StoreError::ChildJoinRejected)
                ));
                self.resumed.fetch_add(1, Ordering::SeqCst);
                if self.join_again {
                    return Ok(GraphNodeExecution::child_join(request));
                }
                return Ok(GraphNodeExecution::new(
                    NodeStateChange::Unchanged,
                    NodeControl::Continue,
                    NodeInvocationBindings::empty(),
                    BudgetUsage::zero(),
                ));
            }
            self.launched.fetch_add(1, Ordering::SeqCst);
            let a = context.attempt().activation();
            let run = self
                .store
                .load_run(a.tenant_id(), a.run_id())
                .await
                .unwrap();
            let append = JournalAppend::new(
                JournalExpectation::exact(run.journal_head().unwrap().clone()),
                JournalEventIntent::worker(
                    a.tenant_id().clone(),
                    a.run_id(),
                    EventId::generate(),
                    context.attempt().fence().clone(),
                    payload("child-run-admitted"),
                )
                .unwrap(),
            )
            .unwrap();
            let (child_append, checkpoint) = child_write(&self.intent);
            let child = Box::pin(self.store.admit_child_run(
                self.intent.clone(),
                context.attempt(),
                append,
                child_append,
                checkpoint,
                BudgetUsage::zero(),
                &self.schemas,
            ))
            .await
            .unwrap();
            assert_eq!(child.record().intent().key(), self.intent.key());
            Ok(GraphNodeExecution::child_join(request))
        })
    }
}

fn registry(
    store: &PostgresStore,
    fixture: &PreparationFixture,
    intent: &ChildRunAdmissionIntent,
    launched: &Arc<AtomicUsize>,
    resumed: &Arc<AtomicUsize>,
    join_again: bool,
) -> ExecutableGraphRegistry {
    let mut registry =
        ExecutableGraphRegistryBuilder::new(fixture.driver.registry.schemas().clone());
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: fixture.driver.graph.reducer().clone(),
        }))
        .unwrap();
    for graph in [&fixture.driver.graph, &fixture.child_driver.graph] {
        registry.register_graph(graph.clone()).unwrap();
        for node in graph.nodes() {
            if graph == &fixture.driver.graph && node.node_id() == intent.key().parent().node_id() {
                registry
                    .register_node(Arc::new(JoinExecutor {
                        store: store.clone(),
                        graph: graph.reference(),
                        intent: intent.clone(),
                        schemas: fixture.driver.registry.schemas().clone(),
                        launched: launched.clone(),
                        resumed: resumed.clone(),
                        join_again,
                    }))
                    .unwrap();
            } else {
                registry
                    .register_node(
                        fixture
                            .driver
                            .registry
                            .resolve(&graph.reference())
                            .unwrap()
                            .node_executor(node.node_id())
                            .unwrap(),
                    )
                    .unwrap();
            }
        }
    }
    registry.build().unwrap()
}

async fn prepare(
    store: &PostgresStore,
    name: &str,
) -> (PreparationFixture, ChildRunAdmissionIntent) {
    let fixture = Box::pin(setup(store, name)).await;
    store
        .register_graph_definition(
            fixture
                .parent
                .admission()
                .intent()
                .provenance()
                .tenant_id()
                .clone(),
            fixture.child_driver.graph.clone(),
        )
        .await
        .unwrap();
    let intent = fixture
        .prepare(
            &fixture.child(),
            store.observe_database_clock().await.unwrap(),
        )
        .unwrap();
    (fixture, intent)
}

fn publisher(store: &PostgresStore, schemas: &JsonSchemaRegistry) -> DurableChildJoinPublisher {
    DurableChildJoinPublisher::new(
        store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
}

async fn reconcile(store: &PostgresStore, schemas: &JsonSchemaRegistry, tenant: &TenantId) {
    let tick = DurableChildReconciler::new(
        store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert!(!tick.items().is_empty());
    for item in tick.items() {
        item.result().unwrap();
    }
}

fn evidence(intent: &stateknot_core::AgentAdmissionIntent) -> StaticLifecycleEvidence {
    StaticLifecycleEvidence {
        terminal: GraphTerminalEvidence::new(
            intent.descriptor().clone(),
            intent.request().clone(),
            intent.budget().clone(),
            AgentArtifacts::empty(),
            BudgetUsage::builder()
                .model_attempts(ExecutionCount::new(1))
                .model_turns(ExecutionCount::new(1))
                .input_bytes(ByteCount::new(1024))
                .output_bytes(ByteCount::new(1024))
                .build()
                .unwrap(),
        ),
        failure: None,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn automatic_join_suspends_runs_child_recovers_parent_and_consumes_once() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (fixture, intent) = Box::pin(prepare(&store, "join-driver-e2e")).await;
    let key = intent.key();
    let a = key.parent();
    let launched = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let deployment = registry(&store, &fixture, &intent, &launched, &resumed, false);
    let lease = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap();
    let loop_ = DurableAgentLoop::new(
        store.clone(),
        deployment.clone(),
        Arc::new(evidence(fixture.parent.admission().intent())),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let first = loop_
        .run(lease.lease().fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let AgentLoopOutcome::ChildJoin(request) = first.outcome() else {
        panic!("{first:?}");
    };
    assert_eq!(request.keys(), &[key.clone()]);
    assert_eq!(first.report().node_attempts_started(), 1);
    assert_eq!(first.report().node_attempts_completed(), 0);
    assert_eq!(launched.load(Ordering::SeqCst), 1);
    let parent = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    assert!(parent.lease().is_none());
    assert!(parent.lifecycle().waits().is_none());
    assert!(matches!(
        store
            .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    assert!(
        publisher(&store, deployment.schemas())
            .tick(a.tenant_id().clone(), None, CancellationSignal::never())
            .await
            .unwrap()
            .items()
            .is_empty()
    );
    drop(loop_);
    let child_lease = store
        .claim_lease(
            a.tenant_id(),
            intent.child().provenance().run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let child_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.child_driver.registry.clone(),
        Arc::new(evidence(intent.child())),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let child = child_loop
        .run(
            child_lease.lease().fence().clone(),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert!(matches!(child.outcome(), AgentLoopOutcome::Succeeded(_)));
    reconcile(&store, deployment.schemas(), a.tenant_id()).await;
    let published = publisher(&store, deployment.schemas())
        .tick(a.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(published.items().len(), 1);
    published.items()[0].result().unwrap();
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_none()
    );
    // Reconnect the pool and rebuild executable bindings; no in-memory continuation.
    let reconnected = test_store().await.unwrap();
    let rebuilt = registry(&reconnected, &fixture, &intent, &launched, &resumed, false);
    let next = reconnected
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap();
    assert_ne!(next.lease().fence(), lease.lease().fence());
    let loop_ = DurableAgentLoop::new(
        reconnected.clone(),
        rebuilt,
        Arc::new(evidence(fixture.parent.admission().intent())),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = loop_
        .run(next.lease().fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(
        matches!(result.outcome(), AgentLoopOutcome::Succeeded(_)),
        "{result:?}"
    );
    assert_eq!(launched.load(Ordering::SeqCst), 1);
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.child_driver.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.child_driver.second_calls.load(Ordering::SeqCst), 1);
    assert!(
        reconnected
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_some()
    );
    let attempts = reconnected
        .load_node_attempt_history_page(a, None, NodeAttemptHistoryPageSize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(attempts.records().len(), 2);
    assert_eq!(
        attempts.records()[0].status(),
        stateknot_core::NodeAttemptStatus::Executing
    );
    assert_eq!(
        attempts.records()[1].status(),
        stateknot_core::NodeAttemptStatus::Succeeded
    );
    let final_parent = reconnected
        .load_run(a.tenant_id(), a.run_id())
        .await
        .unwrap();
    assert_eq!(
        final_parent
            .lifecycle()
            .terminal_usage()
            .unwrap()
            .model_turns()
            .get(),
        2
    );
    assert!(
        reconnected
            .load_current_checkpoint(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .unwrap()
            .superstep()
            .get()
            > 0
    );
    assert!(
        publisher(&reconnected, deployment.schemas())
            .tick(a.tenant_id().clone(), None, CancellationSignal::never())
            .await
            .unwrap()
            .items()
            .is_empty()
    );
    reconnected.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn automatic_join_refuses_rejoin_and_recovers_failed_child_without_respending() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (fixture, intent) = Box::pin(prepare(&store, "join-driver-rejoin")).await;
    let a = intent.key().parent();
    let launched = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let deployment = registry(&store, &fixture, &intent, &launched, &resumed, true);
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        deployment.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        driver
            .drive(fence, CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        GraphDriveOutcome::ChildJoin(_)
    ));
    fail(
        &store,
        a.tenant_id(),
        intent.child().provenance().run_id(),
        BudgetUsage::zero(),
    )
    .await
    .unwrap();
    reconcile(&store, deployment.schemas(), a.tenant_id()).await;
    publisher(&store, deployment.schemas())
        .tick(a.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap()
        .items()[0]
        .result()
        .unwrap();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let bad = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await;
    assert!(
        matches!(bad, Err(GraphDriverError::Store { source }) if matches!(*source, StoreError::ChildJoinRejected))
    );
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_none()
    );
    store.release_lease(&fence).await.unwrap();
    let rebuilt = registry(&store, &fixture, &intent, &launched, &resumed, false);
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let result =
        DurableGraphDriver::new(store.clone(), rebuilt, DurableGraphDriverOptions::default())
            .unwrap()
            .drive(fence.clone(), CancellationSignal::never())
            .await
            .unwrap();
    assert!(
        matches!(
            result.outcome(),
            GraphDriveOutcome::LifecycleBarrierReady(_)
        ),
        "{result:?}"
    );
    assert_eq!(launched.load(Ordering::SeqCst), 1);
    assert_eq!(resumed.load(Ordering::SeqCst), 2);
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_some()
    );
    store.release_lease(&fence).await.unwrap();
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn automatic_join_registration_rollback_replays_spawn_and_cancellation_drains_without_consuming()
 {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (fixture, intent) = Box::pin(prepare(&store, "join-driver-rollback-cancel")).await;
    let a = intent.key().parent();
    let launched = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let deployment = registry(&store, &fixture, &intent, &launched, &resumed, false);
    let driver = DurableGraphDriver::new(
        store.clone(),
        deployment.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let pool = sql_pool().await;
    inject_failure(&pool, "runs", "UPDATE", &format!("NEW.run_id='{}'::uuid AND OLD.lease_attempt_id IS NOT NULL AND NEW.lease_attempt_id IS NULL", a.run_id())).await;
    let result = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await;
    clear_failure(&pool, "runs").await;
    assert!(
        matches!(result, Err(GraphDriverError::Store { source }) if matches!(*source, StoreError::Database { .. }))
    );
    assert!(store.load_child_join(a).await.unwrap().is_none());
    let original = store.load_child_run(intent.key()).await.unwrap();
    store.release_lease(&fence).await.unwrap();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let retry = driver
        .drive(fence, CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(retry.outcome(), GraphDriveOutcome::ChildJoin(_)));
    assert_eq!(retry.report().node_attempts_completed(), 0);
    assert_eq!(
        store.load_child_run(intent.key()).await.unwrap().spawn(),
        original.spawn()
    );
    assert_eq!(
        store
            .list_child_run_identities(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(launched.load(Ordering::SeqCst), 2);
    cancellation::cancel_run(&store, a.tenant_id(), a.run_id()).await;
    reconcile(&store, deployment.schemas(), a.tenant_id()).await;
    cancellation::confirm(
        &store,
        a.tenant_id(),
        intent.child().provenance().run_id(),
        BudgetUsage::zero(),
    )
    .await
    .unwrap();
    reconcile(&store, deployment.schemas(), a.tenant_id()).await;
    let published = publisher(&store, deployment.schemas())
        .tick(a.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(published.items().is_empty());
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let result = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::CancellationRequested(_)
    ));
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    let join = store.load_child_join(a).await.unwrap().unwrap();
    assert!(join.head().is_none());
    assert!(join.consumed().is_none());
    cancellation::confirm(&store, a.tenant_id(), a.run_id(), BudgetUsage::zero())
        .await
        .unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn join_publisher_pages_past_failure_recovers_after_restart_and_keeps_tenant_scope() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("join-publisher-pages");
    let mut requests = Vec::new();
    for _ in 0..17 {
        let fixture = Box::pin(setup_in_tenant(&store, tenant_id.clone())).await;
        let mut value = Box::pin(started_fixture(&store, fixture)).await;
        let spawned = spawn(&store, &value).await.unwrap();
        value.head = spawned.record().spawn().head();
        let request = ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
        store
            .register_child_join(
                request.clone(),
                &value.node,
                parent_append(&value, "child-join-registered"),
            )
            .await
            .unwrap();
        settle(&store, &value, value.intent.child().provenance().run_id()).await;
        requests.push(request);
    }
    let schemas = driver_fixture().registry.schemas().clone();
    let worker = publisher(&store, &schemas);
    let pool = sql_pool().await;
    inject_failure(
        &pool,
        "child_run_join_bindings",
        "INSERT",
        &format!(
            "NEW.parent_run_id='{}'::uuid",
            requests[0].activation().run_id()
        ),
    )
    .await;
    let page = worker
        .tick(tenant_id.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    clear_failure(&pool, "child_run_join_bindings").await;
    assert_eq!(page.items().len(), 16);
    assert!(page.items()[0].result().is_err());
    for item in &page.items()[1..] {
        item.result().unwrap();
    }
    let tail = publisher(&store, &schemas)
        .tick(
            tenant_id.clone(),
            Some(page.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(tail.items().len(), 1);
    assert_eq!(tail.items()[0].request(), &requests[16]);
    tail.items()[0].result().unwrap();
    assert!(matches!(
        worker
            .tick(
                tenant("wrong-tenant"),
                Some(page.cursor().clone()),
                CancellationSignal::never()
            )
            .await,
        Err(StoreError::ChildJoinRejected)
    ));
    let recovered = publisher(&store, &schemas)
        .tick(tenant_id, None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(recovered.items().len(), 1);
    assert_eq!(recovered.items()[0].request(), &requests[0]);
    recovered.items()[0].result().unwrap();
    for request in requests {
        let join = store
            .load_child_join(request.activation())
            .await
            .unwrap()
            .unwrap();
        assert!(join.head().is_some());
        assert!(join.consumed().is_none());
    }
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn join_preparation_timeout_never_dispatches_or_fabricates_a_node_completion() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, child) = setup_join(&store, "join-preflight-timeout").await;
    store
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    settle(&store, &value, child).await;
    store
        .publish_child_join(&request, publish_append(&store, &request).await)
        .await
        .unwrap();
    let launched = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let deployment = registry(
        &store,
        &value.fixture,
        &value.intent,
        &launched,
        &resumed,
        false,
    );
    let a = request.activation();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let pool = sql_pool().await;
    let mut blocker = pool.begin().await.unwrap();
    query("LOCK TABLE stateknot.child_run_join_bindings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        128,
        Duration::from_secs(1),
        Duration::from_millis(100),
        3,
        Duration::from_millis(10),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), deployment.clone(), options).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        driver.drive(fence.clone(), CancellationSignal::never()),
    )
    .await;
    blocker.rollback().await.unwrap();
    assert!(matches!(
        result.unwrap(),
        Err(GraphDriverError::ChildJoinPreparationIncomplete)
    ));
    assert_eq!(launched.load(Ordering::SeqCst), 0);
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_none()
    );
    let attempts = store
        .load_node_attempt_history_page(a, None, NodeAttemptHistoryPageSize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(attempts.records().len(), 2);
    assert!(
        attempts
            .records()
            .iter()
            .all(|attempt| attempt.status() == stateknot_core::NodeAttemptStatus::Executing)
    );
    store.release_lease(&fence).await.unwrap();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let result = DurableGraphDriver::new(
        store.clone(),
        deployment,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .drive(fence.clone(), CancellationSignal::never())
    .await
    .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    store.release_lease(&fence).await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn published_join_cannot_dispatch_through_a_non_join_executor_binding() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, child) = setup_join(&store, "join-missing-executor-contract").await;
    store
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    settle(&store, &value, child).await;
    store
        .publish_child_join(&request, publish_append(&store, &request).await)
        .await
        .unwrap();
    let a = request.activation();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        value.fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let result = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await;
    assert!(
        matches!(result, Err(GraphDriverError::Store { source }) if matches!(*source, StoreError::ChildJoinRejected))
    );
    assert_eq!(value.fixture.driver.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(value.fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_none()
    );
    store.release_lease(&fence).await.unwrap();
    store.close().await;
}
