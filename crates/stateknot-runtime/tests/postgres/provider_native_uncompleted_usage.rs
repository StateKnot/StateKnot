// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real SDK commits outlive unfinished Driver attempts, without another Tool call.

use super::*;
use stateknot_core::{NodeAttempt, NodeAttemptStart, NodeAttemptStartHead, ToolInvocation};
use stateknot_runtime::ExecutableGraph;

use crate::commit_proxy::{CommitProxy, Cut};
use sqlx_core::{connection::ConnectOptions, query::query, query_scalar::query_scalar};
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions};

#[derive(Default)]
struct Pause {
    entered: tokio::sync::Notify,
    dropped: tokio::sync::Notify,
    starts: std::sync::Mutex<Vec<NodeAttemptStartHead>>,
}

struct PausedReturn(Arc<Pause>);
impl Drop for PausedReturn {
    fn drop(&mut self) {
        self.0.dropped.notify_one();
    }
}

struct PauseAfterSdk {
    inner: Arc<dyn GraphNodeExecutor>,
    pause: Arc<Pause>,
}
impl GraphNodeExecutor for PauseAfterSdk {
    fn graph(&self) -> &GraphReference {
        self.inner.graph()
    }
    fn node_id(&self) -> &NodeId {
        self.inner.node_id()
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            let start = context.attempt().clone();
            let execution = self.inner.execute(context).await?;
            let _guard = PausedReturn(Arc::clone(&self.pause));
            self.pause.starts.lock().unwrap().push(start);
            self.pause.entered.notify_one();
            // The real executor has committed its SDK terminal. The Driver has
            // received no execution result and cannot commit a Node completion.
            future::pending::<()>().await;
            Ok(execution)
        })
    }
}

struct OriginalReducer(ExecutableGraph);
impl GraphReducer for OriginalReducer {
    fn reference(&self) -> &GraphReducerReference {
        self.0.reducer().reference()
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        self.0.reducer().reduce(state, updates)
    }
}

fn composition(
    store: &PostgresStore,
    fixture: &ProviderNativeFixture,
    pause: Option<&Arc<Pause>>,
) -> ExecutableGraphRegistry {
    let definition = &fixture.definition;
    let tool = definition.descriptor().tools().iter().next().unwrap();
    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(ProviderNativeScriptedModel {
            tool_count: 1,
            descriptor: definition.descriptor().model().clone(),
            tool: tool.clone(),
            output_schema: definition.descriptor().output_schema().clone(),
            calls: Arc::clone(&fixture.model_calls),
            transcript_lengths: Arc::clone(&fixture.transcript_lengths),
            remaining_model_turns: Arc::clone(&fixture.remaining_model_turns),
            failed_outcomes: Arc::clone(&fixture.failed_outcomes),
        }))
        .unwrap();
    let mut tools = ToolProviderRegistryBuilder::new();
    if pause.is_some() {
        tools
            .register(Arc::new(ProviderNativeLookupTool {
                descriptor: tool.clone(),
                calls: Arc::clone(&fixture.tool_calls),
                fail: false,
            }))
            .unwrap();
    }
    // The cold composition has no Tool provider. It must consume the actual
    // original committed record rather than resolve or redispatch that call.
    let schemas = fixture.registry.schemas().clone();
    let executor = DurableInvocationExecutor::with_clock(
        store.clone(),
        schemas.clone(),
        models.build(),
        tools.build(),
        Arc::new(
            ProviderNativeAgentBudgetProvider::new(definition.clone(), store.clone()).unwrap(),
        ),
        Arc::new(FixedInvocationClock {
            observed_at: "2029-01-01T00:00:00.000000Z".parse().unwrap(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let mut builder = ExecutableGraphRegistryBuilder::new(schemas.clone());
    definition
        .register_executable(&mut builder, store.clone(), executor, schemas.clone())
        .unwrap();
    let original = builder.build().unwrap();
    let Some(pause) = pause else {
        return original;
    };
    let executable = original.resolve(&definition.graph().reference()).unwrap();
    let mut builder = ExecutableGraphRegistryBuilder::new(schemas);
    builder.register_graph(definition.graph().clone()).unwrap();
    builder
        .register_reducer(Arc::new(OriginalReducer(executable.clone())))
        .unwrap();
    for node in definition.graph().nodes() {
        let inner = executable.node_executor(node.node_id()).unwrap();
        if node.node_id().as_str() == stateknot_runtime::PROVIDER_NATIVE_TOOLS_NODE_ID {
            builder
                .register_node(Arc::new(PauseAfterSdk {
                    inner,
                    pause: Arc::clone(pause),
                }))
                .unwrap();
        } else {
            builder.register_node(inner).unwrap();
        }
    }
    builder.build().unwrap()
}

async fn history(store: &PostgresStore, activation: &NodeActivation) -> Vec<NodeAttempt> {
    let mut records = Vec::new();
    let mut cursor = None;
    loop {
        let page = store
            .load_node_attempt_history_page(
                activation,
                cursor.as_ref(),
                NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
            )
            .await
            .unwrap();
        records.extend_from_slice(page.records());
        if !page.has_more() {
            return records;
        }
        cursor = Some(page.next_cursor().unwrap());
    }
}

#[derive(Clone, Copy, Debug)]
enum HistoryReadFault {
    CancelQuery,
    DriverDeadline,
}

async fn history_query_waiting(pool: &PgPool, backend: i32, blocker: i32) -> bool {
    query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE pid = $1 AND state = 'active' AND wait_event_type = 'Lock'
              AND query LIKE '%FROM stateknot.node_attempts%'
              AND query LIKE '%ORDER BY journal_sequence ASC%'
              AND $2 = ANY(pg_blocking_pids(pid))
        )",
    )
    .bind(backend)
    .bind(blocker)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn wait_for_history_query(pool: &PgPool, backend: i32, blocker: i32, waiting: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if history_query_waiting(pool, backend, blocker).await == waiting {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the exact Driver history query must reach the expected lock state");
}

fn history_fault_driver_options(fault: HistoryReadFault) -> DurableGraphDriverOptions {
    let options = DurableGraphDriverOptions::default();
    let HistoryReadFault::DriverDeadline = fault else {
        return options;
    };
    DurableGraphDriverOptions::new(
        options.replay_limits(),
        options.maximum_durable_events(),
        options.lease_renewal_interval(),
        Duration::from_secs(2),
        options.maximum_mutation_attempts(),
        options.mutation_retry_initial_delay(),
    )
    .unwrap()
}

async fn apply_history_fault(
    controller: &PgPool,
    backend: i32,
    blocker: i32,
    options: DurableGraphDriverOptions,
    fault: HistoryReadFault,
) -> bool {
    match fault {
        HistoryReadFault::CancelQuery => {
            let sent: bool = query_scalar("SELECT pg_cancel_backend($1)")
                .bind(backend)
                .fetch_one(controller)
                .await
                .unwrap();
            wait_for_history_query(controller, backend, blocker, false).await;
            sent
        }
        HistoryReadFault::DriverDeadline => {
            // The node timer started before this blocked SELECT. Wait past it
            // while the actual PostgreSQL statement is still waiting.
            tokio::time::sleep(options.node_execution_timeout() + Duration::from_millis(100)).await;
            history_query_waiting(controller, backend, blocker).await
        }
    }
}

async fn interrupt_history_preparation(
    store: &PostgresStore,
    fixture: &ProviderNativeFixture,
    activation: &NodeActivation,
    actor: &RunFence,
    fault: HistoryReadFault,
) -> NodeAttemptStartHead {
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let (mut proxy, history_reached, forward_history) =
        CommitProxy::start_before_node_history(&connection).await;
    let proxied = PostgresStore::connect(
        connection
            .clone()
            .host("127.0.0.1")
            .port(proxy.port())
            .statement_cache_capacity(0)
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    let controller = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(connection)
        .await
        .unwrap();
    let options = history_fault_driver_options(fault);
    let driver = DurableGraphDriver::new(
        proxied.clone(),
        composition(&proxied, fixture, None),
        options,
    )
    .unwrap();
    let fence = actor.clone();
    let mut driving =
        tokio::spawn(async move { driver.drive(fence, CancellationSignal::never()).await });
    tokio::select! {
        () = proxy.wait_cut(Cut::CommitResponse) => {},
        result = &mut driving => panic!("Driver returned before the real Node Start cut: {result:?}"),
    }
    let committed = store
        .load_run(actor.tenant_id(), actor.run_id())
        .await
        .unwrap();
    proxy.resume_ack();
    tokio::time::timeout(Duration::from_secs(3), history_reached)
        .await
        .expect("the Driver must reach its ordered history read after the Start ACK")
        .unwrap();
    let mut lock = controller.begin().await.unwrap();
    // Holding the exact Parse avoids blocking lease preparation, which
    // authenticates the previous checkpoint and can read the same table.
    query("LOCK TABLE stateknot.node_attempts IN ACCESS EXCLUSIVE MODE NOWAIT")
        .execute(&mut *lock)
        .await
        .unwrap();
    let blocker: i32 = query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let backend = proxy.backend_pid();
    forward_history.send(()).unwrap();
    wait_for_history_query(&controller, backend, blocker, true).await;
    let fault_observed = apply_history_fault(&controller, backend, blocker, options, fault).await;
    // Unblock writes before waiting for the Drive result. The lock itself must
    // not prevent an incorrect implementation from writing Failed completion.
    lock.rollback().await.unwrap();
    assert!(
        fault_observed,
        "the requested preparation fault must occur: {fault:?}"
    );
    let result = tokio::time::timeout(Duration::from_secs(10), &mut driving)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            result,
            Err(GraphDriverError::DirectUsageEvidence {
                source: GraphLifecycleEvidenceError::TemporarilyUnavailable,
            })
        ),
        "history preparation must return a Drive error: {result:?}"
    );
    assert_eq!(
        store
            .load_run(actor.tenant_id(), actor.run_id())
            .await
            .unwrap()
            .journal_head(),
        committed.journal_head(),
        "preparation faults cannot publish any completion event"
    );
    let attempts = history(store, activation).await;
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.completion().is_none())
    );
    let start = attempts.last().unwrap().start().head();
    assert_eq!(start.fence(), actor);
    assert_eq!(Some(start.journal_head()), committed.journal_head());
    proxied.close().await;
    proxy.stop().await;
    controller.close().await;
    start
}

fn expected_tool_usage(tool: &ToolInvocation) -> BudgetUsage {
    let ToolInvocationState::Committed { result } = tool.state() else {
        panic!("the real SDK Tool terminal must be committed");
    };
    BudgetUsage::builder()
        .tool_calls(ExecutionCount::new(1))
        .write_calls(ExecutionCount::new(1))
        .input_bytes(ByteCount::new(
            tool.intent().input().value().stats().compact_bytes() as u64,
        ))
        .output_bytes(ByteCount::new(
            result.output().stats().compact_bytes() as u64
        ))
        .artifact_bytes(result.artifacts().total_bytes())
        .known_costs(KnownCosts::try_new([Money::new("USD".parse().unwrap(), 11)]).unwrap())
        .build()
        .unwrap()
}

async fn interrupt_three_attempts(
    store: &PostgresStore,
    fixture: &ProviderNativeFixture,
    tenant: &TenantId,
    run_id: RunId,
) -> (Vec<NodeAttemptStartHead>, RunFence) {
    let mut actor = store
        .claim_lease(tenant, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let pause = Arc::new(Pause::default());
    for _ in 0..3 {
        let driver = DurableGraphDriver::new(
            store.clone(),
            composition(store, fixture, Some(&pause)),
            DurableGraphDriverOptions::default(),
        )
        .unwrap();
        let mut driving = driver.drive(actor.clone(), CancellationSignal::never());
        tokio::select! {
            () = pause.entered.notified() => {},
            result = &mut driving => panic!("Driver returned before the Node completion gap: {result:?}"),
            () = tokio::time::sleep(Duration::from_secs(90)) => panic!("real SDK Tool did not commit"),
        }
        drop(driving);
        tokio::time::timeout(Duration::from_secs(5), pause.dropped.notified())
            .await
            .unwrap();
        drop(driver);
        actor = store
            .supersede_lease(tenant, run_id, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
    }
    let starts = pause.starts.lock().unwrap().clone();
    (starts, actor)
}

async fn reject_noncurrent_contexts(
    executor: &dyn GraphNodeExecutor,
    starts: &[NodeAttemptStartHead],
    checkpoint: &Checkpoint,
) -> (bool, bool) {
    let activation = starts[0].activation();
    let stale_rejected = executor
        .execute(
            GraphNodeContext::new(
                starts[1].clone(),
                Arc::new(checkpoint.clone()),
                CancellationSignal::never(),
            )
            .unwrap(),
        )
        .await
        .is_err();
    // A deliberately crossed head is a negative input, never a durable row.
    let wrong = NodeAttemptStart::new(
        activation.clone(),
        AttemptId::generate(),
        starts[2].fence().clone(),
        starts[2].journal_head().clone(),
    )
    .unwrap()
    .head();
    let wrong_rejected = executor
        .execute(
            GraphNodeContext::new(
                wrong,
                Arc::new(checkpoint.clone()),
                CancellationSignal::never(),
            )
            .unwrap(),
        )
        .await
        .is_err();
    (stale_rejected, wrong_rejected)
}

fn assert_first_completion_usage(
    attempts: &[NodeAttempt],
    starts: &[NodeAttemptStartHead],
    tool: &ToolInvocation,
) -> NodeAttemptStartHead {
    assert_eq!(attempts.len(), starts.len() + 1);
    let (completed, interrupted) = attempts.split_last().unwrap();
    for (attempt, start) in interrupted.iter().zip(starts) {
        assert_eq!(attempt.start().head(), *start);
        assert!(attempt.completion().is_none());
    }
    assert_eq!(
        completed.completion().unwrap().usage(),
        &expected_tool_usage(tool),
        "the first completion must charge exactly the original Tool usage"
    );
    completed.start().head()
}

async fn continue_cold(
    store: &PostgresStore,
    fixture: &ProviderNativeFixture,
    starts: &[NodeAttemptStartHead],
    actor: &RunFence,
    checkpoint: Checkpoint,
    tool: ToolInvocation,
) {
    let activation = starts[0].activation();
    let cold = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    let registry = composition(&cold, fixture, None);
    let executor = registry
        .resolve(&fixture.definition.graph().reference())
        .unwrap()
        .node_executor(activation.node_id())
        .unwrap();
    let journal = cold
        .load_run(actor.tenant_id(), actor.run_id())
        .await
        .unwrap()
        .journal_head()
        .cloned();
    let (stale_rejected, wrong_rejected) =
        reject_noncurrent_contexts(executor.as_ref(), starts, &checkpoint).await;
    assert_eq!(
        cold.load_run(actor.tenant_id(), actor.run_id())
            .await
            .unwrap()
            .journal_head()
            .cloned(),
        journal
    );

    let driver = DurableGraphDriver::new(
        cold.clone(),
        registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let outcome = driver
        .drive(actor.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = outcome.into_parts().0 else {
        panic!("cold continuation must reach terminal barrier");
    };
    let attempts = history(&cold, activation).await;
    let completed = assert_first_completion_usage(&attempts, starts, &tool);
    assert!(
        stale_rejected,
        "an older actual context must not receive the new proof"
    );
    assert!(
        wrong_rejected,
        "a head absent from the real history must be rejected"
    );
    assert!(
        executor
            .execute(
                GraphNodeContext::new(completed, Arc::new(checkpoint), CancellationSignal::never())
                    .unwrap()
            )
            .await
            .is_err(),
        "a completed current attempt cannot be classified as unfinished"
    );
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(*fixture.transcript_lengths.lock().await, vec![0, 1]);
    DurableGraphLifecycle::new(
        cold.clone(),
        registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            fixture.definition.clone(),
            cold.clone(),
        )),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
    .commit_barrier(*handoff)
    .await
    .unwrap();
    let run = cold
        .load_run(actor.tenant_id(), actor.run_id())
        .await
        .unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Succeeded);
    assert_eq!(
        run.lifecycle()
            .result()
            .unwrap()
            .usage()
            .known_costs()
            .get("USD".parse().unwrap()),
        Some(Money::new("USD".parse().unwrap(), 25))
    );
    cold.close().await;
}

fn priced_fixture(store: &PostgresStore) -> ProviderNativeFixture {
    provider_native_fixture_with_accounting(
        store.clone(),
        false,
        false,
        true,
        None,
        NativeBudgetMode::Durable,
        Arc::new(PricedInvocationAccounting {
            unpriced: false,
            reference: AgentInvocationAccountingReference::new(
                capability("provider-native-priced-accounting"),
                Digest::sha256(b"provider-native model 7 tool 11 USD micros v1"),
            ),
        }),
        1,
    )
}

async fn qualify(store: &PostgresStore) {
    let fixture = priced_fixture(store);
    let tenant = tenant("provider-native-uncompleted");
    let ids = AgentRunIds::generate();
    store
        .register_graph_definition(tenant.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();
    DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &fixture,
            tenant.clone(),
            ids,
        ))
        .await
        .unwrap();
    let (mut starts, mut actor) =
        interrupt_three_attempts(store, &fixture, &tenant, ids.run_id()).await;
    assert_eq!(starts.len(), 3);
    let activation = starts[0].activation();
    let original = history(store, activation).await;
    assert_eq!(original.len(), 3);
    assert!(
        original
            .iter()
            .all(|attempt| attempt.completion().is_none())
    );
    let first_page = store
        .load_node_attempt_history_page(
            activation,
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        first_page.has_more(),
        "absence cannot be proved from the first two records"
    );
    assert!(matches!(
        store.load_pending_node_result(activation).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));
    let checkpoint = store
        .load_current_checkpoint(&tenant, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    let state = fixture
        .definition
        .restore_state(checkpoint.state())
        .unwrap();
    let ProviderNativeAgentPhase::Tools { plans, .. } = state.phase() else {
        panic!("original Tools checkpoint");
    };
    let tool = store
        .load_tool_invocation(&tenant, ids.run_id(), plans[0].invocation_id())
        .await
        .unwrap();
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    assert!(tool.journal_head().sequence() < starts[1].journal_head().sequence());

    let activation = activation.clone();
    for fault in [
        HistoryReadFault::CancelQuery,
        HistoryReadFault::DriverDeadline,
    ] {
        starts
            .push(interrupt_history_preparation(store, &fixture, &activation, &actor, fault).await);
        let replacement = store
            .supersede_lease(&tenant, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        assert!(replacement.epoch() > actor.epoch());
        actor = replacement;
    }
    assert!(matches!(
        store.load_pending_node_result(&activation).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));
    let cold_continuation = continue_cold(store, &fixture, &starts, &actor, checkpoint, tool);
    Box::pin(cold_continuation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_tool_without_node_completion_is_charged_once_after_cold_recovery() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    Box::pin(qualify(&store)).await;
    store.close().await;
}
