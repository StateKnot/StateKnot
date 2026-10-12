// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable Driver recovery keeps complete mutation acknowledgements alive.

use super::*;

#[tokio::test]
async fn execution_renewal_ack_drains_before_completed_node_can_commit() {
    assert_execution_renewal_exit(ExecutionRenewalExit::Completed).await;
}

#[tokio::test]
async fn execution_renewal_ack_drains_before_external_shutdown_can_release() {
    assert_execution_renewal_exit(ExecutionRenewalExit::Shutdown).await;
}

#[derive(Clone, Copy)]
enum ExecutionRenewalExit {
    Completed,
    Shutdown,
}

#[allow(clippy::too_many_lines)]
async fn assert_execution_renewal_exit(exit: ExecutionRenewalExit) {
    use crate::commit_proxy::{CommitProxy, Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
    let fixture = execution_lease_fixture();
    let tenant_id = tenant("execution-lease-ack-exit");
    let ids = AgentRunIds::generate();
    admit_root_fixture(&store, &fixture.driver, &tenant_id, ids).await;
    let connection = std::env::var(DATABASE_URL_ENV)
        .unwrap()
        .parse::<sqlx_postgres::PgConnectOptions>()
        .unwrap()
        .statement_cache_capacity(0);
    let mut proxy = CommitProxy::start_after_matching_commits(
        &connection,
        Cut::CommitResponse,
        TransactionTarget::LeaseRenewal,
        1,
    )
    .await;
    let proxied = PostgresStore::connect(
        connection
            .host("127.0.0.1")
            .port(proxy.port())
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    let driver = recovery_driver(&proxied, fixture.driver.registry.clone());
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let shutdown = DirectShutdown {
        requested: Arc::new(AtomicBool::new(false)),
        notify: Arc::new(tokio::sync::Notify::new()),
    };
    let signal = CancellationSignal::new(shutdown.clone());
    let fence = lease.fence().clone();
    let mut driving = tokio::spawn(async move { driver.drive(fence, signal).await });
    tokio::select! {
        () = proxy.wait_cut(Cut::CommitResponse) => {},
        result = &mut driving => panic!("Driver ended before the execution renewal cut: {result:?}"),
    }
    assert_eq!(fixture.driver.first_calls.load(Ordering::SeqCst), 1);
    let committed = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(committed.lease().unwrap().fence(), lease.fence());
    assert!(committed.lease().unwrap().expires_at() > lease.expires_at());
    let cancelled = fixture
        .cancellation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    assert!(!cancelled.is_cancelled());
    match exit {
        ExecutionRenewalExit::Completed => {
            fixture.completion.notify_one();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !fixture.completed.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        ExecutionRenewalExit::Shutdown => {
            shutdown.requested.store(true, Ordering::Release);
            shutdown.notify.notify_waiters();
            tokio::time::timeout(Duration::from_secs(5), cancelled.cancelled())
                .await
                .unwrap();
            assert!(!fixture.completed.load(Ordering::Acquire));
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !driving.is_finished(),
        "Driver returned before the renewal ACK"
    );
    assert_eq!(
        proxy.messages_while_ack_held(),
        0,
        "the original renewal still owns its connection until its ACK is read"
    );
    let held = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(held.lease(), committed.lease());
    assert_eq!(held.journal_head(), committed.journal_head());
    assert_eq!(held.checkpoint(), committed.checkpoint());
    proxy.resume_ack();
    let result = tokio::time::timeout(Duration::from_secs(10), driving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let current = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert!(current.lease().is_none());
    assert_eq!(current.checkpoint(), committed.checkpoint());
    assert!(!current.is_quarantined());
    match exit {
        ExecutionRenewalExit::Completed => {
            assert!(matches!(
                result.outcome(),
                GraphDriveOutcome::Yielded { .. }
            ));
            assert_eq!(
                current.journal_head().unwrap().sequence().get(),
                committed.journal_head().unwrap().sequence().get() + 1
            );
        }
        ExecutionRenewalExit::Shutdown => {
            assert!(matches!(
                result.outcome(),
                GraphDriveOutcome::Cancelled { .. }
            ));
            assert_eq!(current.journal_head(), committed.journal_head());
        }
    }
    assert_eq!(fixture.driver.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    proxied.close().await;
    proxy.stop().await;
    store.close().await;
}

#[tokio::test]
async fn execution_renewal_ack_outlives_old_deadline_while_application_stops() {
    use crate::commit_proxy::{CommitProxy, Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = execution_lease_fixture();
    let tenant_id = tenant("execution-lease-ack-deadline");
    let ids = AgentRunIds::generate();
    admit_root_fixture(&store, &fixture.driver, &tenant_id, ids).await;
    let connection: sqlx_postgres::PgConnectOptions = std::env::var(DATABASE_URL_ENV)
        .unwrap()
        .parse::<sqlx_postgres::PgConnectOptions>()
        .unwrap()
        .statement_cache_capacity(0);
    // The first actual renewal belongs to prepare_execution_lease. Hold the
    // second actual renewal after the originally authorized node has launched.
    let mut proxy = CommitProxy::start_after_matching_commits(
        &connection,
        Cut::CommitResponse,
        TransactionTarget::LeaseRenewal,
        1,
    )
    .await;
    let proxied = PostgresStore::connect(
        connection
            .host("127.0.0.1")
            .port(proxy.port())
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    let driver = recovery_driver(&proxied, fixture.driver.registry.clone());
    // Connection/catalog preparation does not belong to the execution guard.
    // Acquire the same original two-second lease only after that setup.
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let fence = lease.fence().clone();
    let mut driving =
        tokio::spawn(async move { driver.drive(fence, CancellationSignal::never()).await });
    tokio::select! {
        () = proxy.wait_cut(Cut::CommitResponse) => {},
        result = &mut driving => panic!("Driver ended before the execution renewal cut: {result:?}"),
    }
    assert_eq!(fixture.driver.first_calls.load(Ordering::SeqCst), 1);
    let cancelled = fixture
        .cancellation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    assert!(!cancelled.is_cancelled());
    let committed = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    let renewed = committed.lease().unwrap();
    assert_eq!(renewed.fence(), lease.fence());
    assert!(renewed.expires_at() > lease.expires_at());
    wait_database_boundary(&store, renewed.expires_at()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        cancelled.is_cancelled(),
        "original execution guard stops application work"
    );
    assert!(
        !driving.is_finished(),
        "execution guardian discarded the committed renewal ACK: {:?}",
        (&mut driving).await.unwrap()
    );
    proxy.resume_ack();
    let error = tokio::time::timeout(Duration::from_secs(10), driving)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, stateknot_runtime::GraphDriverError::Store { source }
        if matches!(*source, StoreError::LeaseExpired))
    );
    let current = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(current.lease(), committed.lease());
    assert_eq!(current.journal_head(), committed.journal_head());
    assert_eq!(current.checkpoint(), committed.checkpoint());
    assert!(!current.is_quarantined());
    assert_eq!(fixture.driver.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    proxied.close().await;
    proxy.stop().await;
    store.close().await;
}

#[tokio::test]
async fn recovery_guardian_failure_stops_dispatch_while_original_fence_is_live() {
    use crate::commit_proxy::{Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
    let fixture = driver_fixture();
    let tenant_id = tenant("root-live-lease-recovery-guardian");
    let ids = AgentRunIds::generate();
    admit_root_fixture(&store, &fixture, &tenant_id, ids).await;
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (mut proxy, proxied) = recovery_proxy(&store, TransactionTarget::GraphNodeStart).await;
    let driver = recovery_driver(&proxied, fixture.registry.clone());
    let fence = lease.fence().clone();
    let mut driving =
        tokio::spawn(async move { driver.drive(fence, CancellationSignal::never()).await });
    proxy.wait_cut(Cut::CommitResponse).await;
    let started = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    // An independent trusted owner retains the same fence with the original
    // thirty-second duration. Guardian failure must stop dispatch even when
    // expiry and higher-fence rejection would not independently stop it.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let observation = store.observe_live_lease(lease.fence()).await.unwrap();
    let expiry =
        Timestamp::from_unix_micros(observation.observed_at().unix_micros() + 30_000_000).unwrap();
    store.renew_lease(lease.fence(), expiry).await.unwrap();
    tokio::time::sleep(Duration::from_secs(16)).await;
    let observation = store.observe_live_lease(lease.fence()).await.unwrap();
    assert_eq!(observation.lease().fence(), lease.fence());
    assert!(
        !driving.is_finished(),
        "outer guardian discarded a Start ACK with a live original fence: {:?}",
        (&mut driving).await.unwrap()
    );
    proxy.resume_ack();
    let error = tokio::time::timeout(Duration::from_secs(10), driving)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, stateknot_runtime::GraphDriverError::Store { source }
        if matches!(*source, StoreError::Database {
            operation: "live lease observation",
            source: sqlx_core::Error::PoolTimedOut,
        }))
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let current = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert!(current.lease().is_none());
    assert_eq!(current.journal_head(), started.journal_head());
    assert_eq!(current.checkpoint(), started.checkpoint());
    assert!(!current.is_quarantined());
    proxied.close().await;
    proxy.stop().await;
    store.close().await;
}

#[tokio::test]
async fn recovery_start_ack_survives_guardian_failure_without_successor_dispatch() {
    use crate::commit_proxy::{Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("root-start-recovery-guardian");
    let ids = AgentRunIds::generate();
    admit_root_fixture(&store, &fixture, &tenant_id, ids).await;
    let checkpoint = store
        .load_current_checkpoint(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    let activation = invocation_activation(&checkpoint);
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (mut proxy, proxied) = recovery_proxy(&store, TransactionTarget::GraphNodeStart).await;
    let driver = recovery_driver(&proxied, fixture.registry.clone());
    let fence = lease.fence().clone();
    let mut driving =
        tokio::spawn(async move { driver.drive(fence, CancellationSignal::never()).await });
    proxy.wait_cut(Cut::CommitResponse).await;
    let started = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert!(
        !driving.is_finished(),
        "outer guardian discarded the committed Start ACK: {:?}",
        (&mut driving).await.unwrap()
    );
    let successor = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(successor.fence().epoch() > lease.fence().epoch());
    proxy.resume_ack();
    let error = tokio::time::timeout(Duration::from_secs(10), driving)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, stateknot_runtime::GraphDriverError::Store { source }
        if matches!(*source, StoreError::StaleFence))
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let current = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(current.lease(), Some(&successor));
    assert_eq!(current.journal_head(), started.journal_head());
    assert_eq!(current.checkpoint(), started.checkpoint());
    assert!(!current.is_quarantined());
    let history = store
        .load_node_attempt_history_page(
            &activation,
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().fence(), lease.fence());
    assert_eq!(
        history.records()[0].status(),
        stateknot_core::NodeAttemptStatus::Executing
    );
    proxied.close().await;
    proxy.stop().await;
    store.close().await;
}

#[tokio::test]
async fn recovery_quarantine_ack_survives_outer_lease_guardian_failure() {
    use crate::commit_proxy::{Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = cancellation_commit_fixture(&store).await;
    let fence = fixture.handoff.lease().fence().clone();
    corrupt_cancellation_frame_stack(&fence).await;
    let (mut proxy, proxied) = recovery_proxy(&store, TransactionTarget::RunQuarantine).await;
    let driver = cancellation_fixture_driver(&proxied, &fixture.nested);
    let owner = fence.clone();
    let mut driving =
        tokio::spawn(async move { driver.drive(owner, CancellationSignal::never()).await });
    proxy.wait_cut(Cut::CommitResponse).await;
    let isolated = store
        .load_run_quarantine(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(isolated.request().expected_fence(), Some(&fence));
    assert_eq!(
        isolated.request().expectation(),
        &JournalExpectation::exact(fixture.handoff.journal_head().clone())
    );
    let committed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(committed.is_quarantined());
    assert!(committed.lease().is_none());
    assert_eq!(
        committed.journal_head(),
        Some(fixture.handoff.journal_head())
    );
    // Keep the original thirty-second pool acquisition bound. The guardian
    // actually fails while the owning recovery awaits its committed SQL ACK.
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert!(
        !driving.is_finished(),
        "outer guardian discarded the committed quarantine ACK: {:?}",
        (&mut driving).await.unwrap()
    );
    proxy.resume_ack();
    let error = tokio::time::timeout(Duration::from_secs(10), driving)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, stateknot_runtime::GraphDriverError::Store { source }
        if matches!(*source, StoreError::RunQuarantined))
    );
    assert_eq!(fixture.nested.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.nested.root_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await
            .unwrap(),
        isolated
    );
    assert!(
        store
            .load_graph_frame_closure(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_none()
    );
    proxied.close().await;
    proxy.stop().await;
    store.close().await;
}

async fn recovery_proxy(
    store: &PostgresStore,
    target: crate::commit_proxy::TransactionTarget,
) -> (crate::commit_proxy::CommitProxy, PostgresStore) {
    use crate::commit_proxy::{CommitProxy, Cut};
    let connection: sqlx_postgres::PgConnectOptions =
        std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let proxy = CommitProxy::start(&connection, Cut::CommitResponse, target).await;
    let proxied = PostgresStore::connect(
        connection
            .host("127.0.0.1")
            .port(proxy.port())
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    (proxy, proxied)
}

fn recovery_driver(store: &PostgresStore, registry: ExecutableGraphRegistry) -> DurableGraphDriver {
    DurableGraphDriver::new(
        store.clone(),
        registry,
        DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            2,
            Duration::from_millis(100),
            Duration::from_secs(15 * 60),
            3,
            Duration::from_millis(25),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn admit_root_fixture(
    store: &PostgresStore,
    fixture: &DriverFixture,
    tenant_id: &TenantId,
    ids: AgentRunIds,
) {
    store
        .register_graph_definition(tenant_id.clone(), fixture.graph.clone())
        .await
        .unwrap();
    DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            fixture,
            tenant_id.clone(),
            ids,
            fixture.graph.output_schema().clone(),
            fixture.graph.input_schema().clone(),
        ))
        .await
        .unwrap();
}

struct ExecutionLeaseFixture {
    driver: DriverFixture,
    cancellation: Arc<std::sync::Mutex<Option<CancellationSignal>>>,
    completion: Arc<tokio::sync::Notify>,
    completed: Arc<AtomicBool>,
}

struct IdleLeaseNode {
    graph: GraphReference,
    node_id: NodeId,
    calls: Arc<AtomicUsize>,
    cancellation: Arc<std::sync::Mutex<Option<CancellationSignal>>>,
    completion: Arc<tokio::sync::Notify>,
    completed: Arc<AtomicBool>,
}

impl GraphNodeExecutor for IdleLeaseNode {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.cancellation.lock().unwrap() = Some(context.cancellation().clone());
        Box::pin(async move {
            self.completion.notified().await;
            self.completed.store(true, Ordering::Release);
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                NodeControl::Continue,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

fn execution_lease_fixture() -> ExecutionLeaseFixture {
    let mut driver = driver_fixture();
    let cancellation = Arc::new(std::sync::Mutex::new(None));
    let completion = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let mut registry = ExecutableGraphRegistryBuilder::new(driver.registry.schemas().clone());
    registry.register_graph(driver.graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: driver.graph.reducer().clone(),
        }))
        .unwrap();
    registry
        .register_node(Arc::new(IdleLeaseNode {
            graph: driver.graph.reference(),
            node_id: NodeId::new("Step_A").unwrap(),
            calls: Arc::clone(&driver.first_calls),
            cancellation: Arc::clone(&cancellation),
            completion: Arc::clone(&completion),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: driver.graph.reference(),
            node_id: NodeId::new("Step.B").unwrap(),
            behavior: TestNodeBehavior::Terminal(driver.graph.output_schema().clone()),
            delay: Duration::ZERO,
            calls: Arc::clone(&driver.second_calls),
        }))
        .unwrap();
    driver.registry = registry.build().unwrap();
    ExecutionLeaseFixture {
        driver,
        cancellation,
        completion,
        completed,
    }
}
