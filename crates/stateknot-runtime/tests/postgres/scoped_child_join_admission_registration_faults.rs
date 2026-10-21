// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Scoped admission/registration protocol cuts, real writer death and cold drive.
//! Each cut is observed by an independent reader; no exception fakes a COMMIT.
use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, parent_control, publish_ready, ready_and_park},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_core::{
    AgentAdmissionIntent, BudgetDimension, BudgetEvaluationError, CumulativeBudgetReservation,
    CumulativeBudgetReservationError, GraphFrameCheckpoint,
};

const WORKER: &str = "scoped_join_close_process_faults::fresh_join::admission_registration_faults::scoped_admission_registration_fault_worker";

#[derive(Clone, Copy, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Admission,
    Registration,
}
impl Operation {
    const fn target(self) -> TransactionTarget {
        match self {
            Self::Admission => TransactionTarget::ChildAdmission,
            Self::Registration => TransactionTarget::ChildJoinRegistration,
        }
    }
    const fn name(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Registration => "registration",
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    operation: Operation,
    tenant: TenantId,
    run: RunId,
    candidate: AgentAdmissionIntent,
    key: Option<ChildRunKey>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Prepared {
    intent: ChildRunAdmissionIntent,
    frame: GraphFrameCheckpoint,
    owner: NodeAttemptStartHead,
    observed: JournalHead,
    direct: BudgetUsage,
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

fn replacement_candidate(candidate: &AgentAdmissionIntent) -> AgentAdmissionIntent {
    let ids = AgentRunIds::generate();
    AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            candidate.provenance().tenant_id().clone(),
            ids.run_id(),
            ids.thread_id(),
            ids.invocation_id(),
            candidate.descriptor(),
        ),
        candidate.descriptor().clone(),
        candidate.request().clone(),
        candidate.budget_layers().iter().cloned(),
        candidate.graph().clone(),
        candidate.authority().clone(),
    )
    .unwrap()
}

// Reserve ceilings are policy, never spent usage. Reproduce the original
// inherited retry ceiling through public request resolution, using the actual
// durable DIRECT observation that the callback is about to pass to admission.
// This fixture has one unexecuted child: compare one proposed ceiling, rather
// than adding a second reservation when the Store returns a historical ACK.
fn assert_retry_reservation_capacity(
    parent: &AgentAdmission,
    child: &AgentAdmissionIntent,
    observed: &JournalHead,
    direct: &BudgetUsage,
) {
    let budget = parent.intent().budget();
    assert_eq!(budget.retries().get(), 25);
    assert_eq!(child.budget().retries().get(), 4);
    let layers = child
        .budget_layers()
        .iter()
        .map(|layer| layer.limits().clone())
        .collect::<Vec<_>>();
    // Setting the request-local ceiling to the unchanged parent ceiling is
    // equivalent to v4's omitted retry limit: descriptor/layers still narrow
    // every dimension. No counterfactual usage or wider parent is constructed.
    let inherited = AgentRequest::new(
        child.request().input_schema().clone(),
        child.request().input().clone(),
        child
            .request()
            .budget_limits()
            .clone()
            .with_retries(budget.retries()),
    )
    .resolve_for(child.descriptor(), &layers, observed.recorded_at())
    .unwrap();
    assert_eq!(inherited.retries(), budget.retries());
    let spent_only =
        CumulativeBudgetReservation::check_capacity(budget, direct, &[], observed.recorded_at())
            .unwrap();
    assert_eq!(
        spent_only.retries().get(),
        budget.retries().get() - direct.retries().get()
    );
    let original_reservation = CumulativeBudgetReservation::from_budget(&inherited).unwrap();
    let original_capacity = CumulativeBudgetReservation::check_capacity(
        budget,
        direct,
        &[original_reservation],
        observed.recorded_at(),
    );
    match direct.retries().get() {
        0 => {
            // The original F1 reaches the protocol cut before a caller rebind.
            original_capacity.unwrap();
        }
        1 => {
            assert!(matches!(
                original_capacity,
                Err(CumulativeBudgetReservationError::Capacity(
                    BudgetEvaluationError::ExecutionLimitExceeded {
                        dimension: BudgetDimension::Retries,
                        limit,
                        actual,
                    }
                )) if limit == budget.retries()
                    && actual.get() == limit.get() + direct.retries().get()
            ));
        }
        retries => panic!("unexpected caller rebind count at admission: {retries}"),
    }
    // The complete narrowed child reservation must fit all dimensions. Keeping
    // this projection temporary prevents reservation ceilings becoming spend.
    CumulativeBudgetReservation::check_capacity(
        budget,
        direct,
        &[CumulativeBudgetReservation::from_budget(child.budget()).unwrap()],
        observed.recorded_at(),
    )
    .unwrap();
}

// A fresh event/checkpoint candidate is deliberately generated for every retry.
// The Store must return the original facts on the historical idempotent path.
async fn admit(
    store: &PostgresStore,
    prepared: &Prepared,
    intent: ChildRunAdmissionIntent,
    schemas: &JsonSchemaRegistry,
) -> (ChildRunCommitOutcome, Value) {
    let key = intent.key();
    let child_id = intent.child().provenance().run_id();
    let parent_append = worker_append_kind(
        key.tenant_id(),
        key.parent_run_id(),
        prepared.observed.clone(),
        prepared.owner.fence(),
        "child-run-admitted",
    );
    let child_append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            child_id,
            EventId::generate(),
            payload(AgentAdmission::JOURNAL_EVENT_KIND),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        key.tenant_id().clone(),
        child_id,
        CheckpointId::generate(),
        intent.child_graph().reference(),
        intent.initial_state().clone(),
        intent.child_graph().entry_nodes().clone(),
    )
    .unwrap();
    let candidates = json!({
        "parent_event": parent_append.intent().event_id(),
        "child_event": child_append.intent().event_id(),
        "checkpoint": checkpoint.checkpoint_id(),
        "child": child_id
    });
    let outcome = Box::pin(store.admit_child_run(
        intent,
        &prepared.owner,
        parent_append,
        child_append,
        checkpoint,
        prepared.direct.clone(),
        schemas,
    ))
    .await
    .unwrap();
    (outcome, candidates)
}

struct FaultJoinExecutor {
    inner: FreshJoinExecutor,
    operation: Operation,
    pause: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}
impl GraphNodeExecutor for FaultJoinExecutor {
    fn graph(&self) -> &GraphReference {
        &self.inner.graph
    }
    fn node_id(&self) -> &NodeId {
        &self.inner.node
    }
    fn supports_child_join(&self) -> bool {
        true
    }
    #[allow(clippy::too_many_lines)]
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            if context.child_join().is_some() {
                return self.inner.execute(context).await;
            }
            assert_eq!(self.inner.launched.fetch_add(1, Ordering::SeqCst), 0);
            let activation = context.attempt().activation();
            assert!(!activation.graph_namespace().is_root());
            let key = ChildRunKey::new(activation.clone(), ChildRunSlot::new("analysis").unwrap())
                .unwrap();
            *self.inner.key.lock().await = Some(key.clone());
            let parent = self
                .inner
                .store
                .load_agent_admission(activation.tenant_id(), activation.run_id())
                .await
                .unwrap();
            let active = self
                .inner
                .store
                .load_active_graph_frame(activation.tenant_id(), activation.run_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(active.checkpoint().checkpoint(), context.checkpoint());
            assert_eq!(
                active
                    .checkpoint()
                    .activation(self.inner.node.clone())
                    .unwrap(),
                *activation
            );
            assert_eq!(
                active.run().lease().unwrap().fence(),
                context.attempt().fence()
            );
            let intent = ChildRunAdmissionIntent::for_frame(
                parent.admission(),
                active.checkpoint(),
                key.clone(),
                self.inner.child.clone(),
                self.inner.child_graph.clone(),
                self.inner.child_state.clone(),
            )
            .unwrap();
            let current = self
                .inner
                .store
                .load_run(activation.tenant_id(), activation.run_id())
                .await
                .unwrap();
            let observed = current.journal_head().unwrap().clone();
            let direct = self
                .inner
                .direct
                .usage_at(activation.tenant_id(), activation.run_id(), &observed)
                .await
                .unwrap();
            assert_retry_reservation_capacity(
                parent.admission(),
                &self.inner.child,
                &observed,
                &direct,
            );
            let prepared = Prepared {
                intent: intent.clone(),
                frame: active.checkpoint().clone(),
                owner: context.attempt().clone(),
                observed,
                direct,
            };
            let pause = self.pause.lock().await.take();
            if self.operation == Operation::Admission {
                if let Some(pause) = pause {
                    publish_ready(&json!({"phase":"before_admission", "prepared":prepared}));
                    pause.await.unwrap();
                }
                let (outcome, _) = Box::pin(admit(
                    &self.inner.store,
                    &prepared,
                    intent,
                    &self.inner.schemas,
                ))
                .await;
                assert_eq!(outcome.record().intent().key(), &key);
            } else {
                let (outcome, _) = Box::pin(admit(
                    &self.inner.store,
                    &prepared,
                    intent,
                    &self.inner.schemas,
                ))
                .await;
                assert_eq!(outcome.record().intent().key(), &key);
                if let Some(pause) = pause {
                    publish_ready(&json!({"phase":"before_registration", "prepared":prepared}));
                    pause.await.unwrap();
                }
            }
            Ok(GraphNodeExecution::child_join(
                ChildRunJoinRequest::new([key]).unwrap(),
            ))
        })
    }
}

// Same closed deployment as the positive; only the exact declared leaf executor
// owns the process rendezvous. Provider-native execution is retained unchanged.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn fault_registry(
    store: &PostgresStore,
    fixture: &DriverFixture,
    leaf: &CompiledGraph,
    native: &ProviderNativeFixture,
    scenario: &Scenario,
    candidate: &AgentAdmissionIntent,
    child_state: &CheckpointState,
    schemas: &JsonSchemaRegistry,
    direct: &Arc<FreshDirectEvidence>,
    counters: &FreshCounters,
    pause: Option<tokio::sync::oneshot::Receiver<()>>,
) -> ExecutableGraphRegistry {
    let mut pause = pause;
    let mut builder = ExecutableGraphRegistryBuilder::new(schemas.clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: fixture.graph.reducer().clone(),
        }))
        .unwrap();
    let child = native
        .registry
        .resolve(&native.definition.graph().reference())
        .unwrap();
    builder
        .register_reducer(Arc::new(RetainedChildReducer(child.clone())))
        .unwrap();
    builder
        .register_graph(native.definition.graph().clone())
        .unwrap();
    for node in native.definition.graph().nodes() {
        builder
            .register_node(child.node_executor(node.node_id()).unwrap())
            .unwrap();
    }
    for graph in [&fixture.graph, leaf] {
        builder.register_graph(graph.clone()).unwrap();
        for node in graph.nodes() {
            if graph
                .frame_calls()
                .is_some_and(|p| p.call(node.node_id()).is_some())
            {
                continue;
            }
            if graph == leaf && node.node_id().as_str() == "Step_A" {
                builder
                    .register_node(Arc::new(FaultJoinExecutor {
                        inner: FreshJoinExecutor {
                            store: store.clone(),
                            graph: graph.reference(),
                            node: node.node_id().clone(),
                            child: candidate.clone(),
                            child_graph: native.definition.graph().clone(),
                            child_state: child_state.clone(),
                            schemas: schemas.clone(),
                            direct: Arc::clone(direct),
                            key: Arc::clone(&direct.key),
                            launched: Arc::clone(&counters.launched),
                            resumed: Arc::clone(&counters.resumed),
                        },
                        operation: scenario.operation,
                        pause: tokio::sync::Mutex::new(pause.take()),
                    }))
                    .unwrap();
            } else {
                builder
                    .register_node(Arc::new(FreshCountedNode {
                        inner: TestNodeExecutor {
                            graph: graph.reference(),
                            node_id: node.node_id().clone(),
                            behavior: if node.allows_terminal() {
                                TestNodeBehavior::Terminal(graph.output_schema().clone())
                            } else {
                                TestNodeBehavior::Continue
                            },
                            delay: Duration::ZERO,
                            calls: Arc::clone(if graph == leaf {
                                &counters.leaf_tail
                            } else {
                                &counters.root_tail
                            }),
                        },
                        child_join: graph.child_runs().is_some_and(|p| {
                            p.declarations()
                                .iter()
                                .any(|d| d.node_id() == node.node_id())
                        }),
                    }))
                    .unwrap();
            }
        }
    }
    assert!(pause.is_none());
    builder.build().unwrap()
}

fn short_driver_options() -> DurableGraphDriverOptions {
    DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        1024,
        Duration::from_secs(2),
        Duration::from_secs(90),
        3,
        Duration::from_millis(25),
    )
    .unwrap()
}

async fn worker_store(input: &Value) -> PostgresStore {
    let mut options: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let cut = input["role"] == "cut";
    if cut {
        options = options
            .host("127.0.0.1")
            .port(u16::try_from(input["proxy_port"].as_u64().unwrap()).unwrap());
    }
    PostgresStore::connect(
        options.to_url_lossy().as_str(),
        PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled)
            .with_pool_size(1, if cut { 1 } else { 8 })
            .with_acquire_timeout(Duration::from_secs(30))
            .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
            .with_lease_timing(Duration::from_secs(8), Duration::from_secs(5 * 60)),
    )
    .await
    .unwrap()
}

async fn claim_after_real_expiry(store: &PostgresStore, scenario: &Scenario) -> RunFence {
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            match store
                .claim_lease(&scenario.tenant, scenario.run, AttemptId::generate())
                .await
            {
                Ok(claim) => break claim.lease().fence().clone(),
                Err(StoreError::LeaseHeld) => tokio::time::sleep(Duration::from_millis(25)).await,
                Err(error) => {
                    panic!("cold claim must only wait for the actual lease expiry: {error:?}")
                }
            }
        }
    })
    .await
    .expect("killed writer's public short lease must actually expire")
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_admission_registration_fault_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    let input: Value = serde_json::from_str(&input).unwrap();
    let role = input["role"].as_str().unwrap();
    let control = parent_control();
    let scenario: Scenario = serde_json::from_value(input["scenario"].clone()).unwrap();
    let store = worker_store(&input).await;
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
    let direct = Arc::new(FreshDirectEvidence {
        store: store.clone(),
        key: Arc::new(tokio::sync::Mutex::new(scenario.key.clone())),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let counters = FreshCounters::new();
    let candidate = if role == "cut" {
        scenario.candidate.clone()
    } else {
        replacement_candidate(&scenario.candidate)
    };
    let candidate_id = candidate.provenance().run_id();
    // Provider-native initial_state contains freshly generated logical message
    // and model-plan identities. They belong to the immutable spawn intent;
    // recovering an existing logical key must retain its exact original state.
    // Only an actually absent ownership record may prepare a new initial state.
    let child_state = if let Some(key) = &scenario.key {
        match store.load_child_run(key).await {
            Ok(original) => original.intent().initial_state().clone(),
            Err(StoreError::ChildRunNotFound) => native.definition.initial_state().unwrap(),
            Err(error) => {
                panic!("retained child initial state failed owning validation: {error:?}")
            }
        }
    } else {
        native.definition.initial_state().unwrap()
    };
    let registry = fault_registry(
        &store,
        &fixture,
        &leaf,
        &native,
        &scenario,
        &candidate,
        &child_state,
        &schemas,
        &direct,
        &counters,
        if role == "cut" {
            Some(control)
        } else {
            drop(control);
            None
        },
    );
    let fence = claim_after_real_expiry(&store, &scenario).await;
    let driver = DurableGraphDriver::new(store.clone(), registry, short_driver_options())
        .unwrap()
        .with_direct_usage_evidence(direct.clone());
    let result = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert_ne!(
        role, "cut",
        "real transaction must remain at protocol cut until SIGKILL"
    );
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
    if role == "register" {
        let GraphDriveOutcome::ChildJoin(request) = result.outcome() else {
            panic!("cold registration: {result:?}");
        };
        assert_eq!(request.keys(), &[scenario.key.clone().unwrap()]);
        assert_eq!(counters.launched.load(Ordering::SeqCst), 1);
        assert_eq!(counters.resumed.load(Ordering::SeqCst), 0);
        assert_eq!(counters.leaf_tail.load(Ordering::SeqCst), 0);
        assert_eq!(counters.root_tail.load(Ordering::SeqCst), 0);
        let registered = store
            .load_run(&scenario.tenant, scenario.run)
            .await
            .unwrap();
        let registered_direct = direct
            .usage_at(
                &scenario.tenant,
                scenario.run,
                registered.journal_head().unwrap(),
            )
            .await
            .unwrap();
        // Registration itself adds no retry. This authenticates that the
        // cold callback exercised the typed 1 + 25 > 25 negative above.
        assert_eq!(registered_direct.retries().get(), 1);
        ready_and_park(json!({
            "phase":"registered", "request":request, "fence":fence,
            "candidate_child":candidate_id, "launched":1, "resumed":0,
            "direct_retries":registered_direct.retries().get(),
        }))
        .await;
    } else {
        assert_eq!(role, "resume");
        assert!(matches!(
            result.outcome(),
            GraphDriveOutcome::LifecycleBarrierReady(_)
        ));
        assert_eq!(counters.launched.load(Ordering::SeqCst), 0);
        assert_eq!(counters.resumed.load(Ordering::SeqCst), 1);
        assert_eq!(counters.leaf_tail.load(Ordering::SeqCst), 1);
        assert_eq!(counters.root_tail.load(Ordering::SeqCst), 1);
        assert_eq!(result.report().frame_returns_committed(), 1);
        let stable = store
            .load_run(&scenario.tenant, scenario.run)
            .await
            .unwrap();
        let head = stable.journal_head().unwrap().clone();
        let again = driver
            .drive(fence.clone(), CancellationSignal::never())
            .await
            .unwrap();
        assert!(matches!(
            again.outcome(),
            GraphDriveOutcome::LifecycleBarrierReady(_)
        ));
        assert_eq!(again.report().node_attempts_started(), 0);
        assert_eq!(again.report().frame_returns_committed(), 0);
        assert_eq!(counters.resumed.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .load_run(&scenario.tenant, scenario.run)
                .await
                .unwrap()
                .journal_head(),
            Some(&head)
        );
        ready_and_park(json!({"phase":"returned", "fence":fence,"launched":0,"resumed":1})).await;
    }
}

fn ownership_fingerprint(record: &stateknot_store_postgres::ChildRunRecord) -> Value {
    json!({
        "intent":record.intent(), "spawn":record.spawn(), "ancestors":record.ancestors(),
        "admission":record.child().admission(), "event":record.child().event(),
        "checkpoint":record.child().checkpoint(),
    })
}

async fn wait_backend_gone(pool: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let gone: bool =
                query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
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
    .expect("only the owned disconnected proxy backend must disappear");
}

async fn historical_admission_ack(
    store: &PostgresStore,
    prepared: &Prepared,
    schemas: &JsonSchemaRegistry,
) -> Value {
    let candidate = replacement_candidate(prepared.intent.child());
    let original = store.load_child_run(prepared.intent.key()).await.unwrap();
    let parent = store
        .load_agent_admission(
            prepared.intent.key().tenant_id(),
            prepared.intent.key().parent_run_id(),
        )
        .await
        .unwrap();
    let intent = ChildRunAdmissionIntent::for_frame(
        parent.admission(),
        &prepared.frame,
        prepared.intent.key().clone(),
        candidate,
        prepared.intent.child_graph().clone(),
        original.intent().initial_state().clone(),
    )
    .unwrap();
    // An unforwarded admission selected no durable initial state. In that
    // rollback case the cold first successful admission chose the retained
    // state; historical ACKs must authenticate that actual committed intent.
    assert_eq!(intent.spawn_digest(), original.intent().spawn_digest());
    let (outcome, candidates) = Box::pin(admit(store, prepared, intent, schemas)).await;
    assert!(matches!(outcome, ChildRunCommitOutcome::Idempotent(_)));
    let record = outcome.record();
    assert_ne!(
        candidates["parent_event"],
        serde_json::to_value(record.spawn().event_id()).unwrap()
    );
    assert_ne!(
        candidates["child_event"],
        serde_json::to_value(record.child().event().event_id()).unwrap()
    );
    assert_ne!(
        candidates["checkpoint"],
        serde_json::to_value(record.child().checkpoint().checkpoint_id()).unwrap()
    );
    assert_ne!(
        candidates["child"],
        serde_json::to_value(record.intent().child().provenance().run_id()).unwrap()
    );
    ownership_fingerprint(record)
}

async fn historical_registration_ack(store: &PostgresStore, prepared: &Prepared) -> JournalEvent {
    let key = prepared.intent.key();
    let before = store.load_child_join(key.parent()).await.unwrap().unwrap();
    let candidate_event = EventId::generate();
    let append = JournalAppend::new(
        JournalExpectation::exact(prepared.observed.clone()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            key.parent_run_id(),
            candidate_event,
            prepared.owner.fence().clone(),
            payload("child-join-registered"),
        )
        .unwrap(),
    )
    .unwrap();
    let ack = store
        .register_child_join(before.request().clone(), &prepared.owner, append)
        .await
        .unwrap();
    assert!(matches!(ack, ChildJoinCommitOutcome::Idempotent(_)));
    assert_eq!(ack.record().registration(), before.registration());
    assert_ne!(ack.record().registration().event_id(), candidate_event);
    assert_eq!(ack.record().request(), before.request());
    ack.record().registration().clone()
}

fn assert_single_row(rows: &Snapshot, table: &str) {
    let value: Value = serde_json::from_str(rows.get(table).unwrap()).unwrap();
    assert_eq!(
        value.as_array().unwrap().len(),
        1,
        "exactly one durable {table}"
    );
}

fn row_count(rows: &Snapshot, table: &str) -> usize {
    serde_json::from_str::<Value>(rows.get(table).unwrap())
        .unwrap()
        .as_array()
        .unwrap()
        .len()
}

#[allow(clippy::too_many_lines)]
async fn qualify_case(
    store: &PostgresStore,
    pool: &PgPool,
    connection: &PgConnectOptions,
    operation: Operation,
    cut: Cut,
) {
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
    let tenant = tenant(&format!(
        "g2-{}-{}",
        operation.name(),
        if cut == Cut::BeforeCommit {
            "request"
        } else {
            "response"
        }
    ));
    let ids = AgentRunIds::generate();
    let root_request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let root = root_request.intent();
    let child_ids = AgentRunIds::generate();
    let candidate = AgentAdmissionIntent::new(
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
            BoundedJson::try_from_value(
                json!({"question":"Execute the original child after real writer loss."}),
            )
            .unwrap(),
            BudgetLimits::empty()
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_retries(ExecutionCount::new(4))
                .with_input_bytes(ByteCount::new(65_536))
                .with_output_bytes(ByteCount::new(65_536)),
        ),
        root.budget_layers().iter().cloned(),
        native.definition.graph().reference(),
        root.authority().clone(),
    )
    .unwrap();
    let mut scenario = Scenario {
        operation,
        tenant: tenant.clone(),
        run: ids.run_id(),
        candidate,
        key: None,
    };
    let direct = Arc::new(FreshDirectEvidence {
        store: store.clone(),
        key: Arc::new(tokio::sync::Mutex::new(None)),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let registry = fault_registry(
        store,
        &fixture,
        &leaf,
        &native,
        &scenario,
        &scenario.candidate,
        &native.definition.initial_state().unwrap(),
        &schemas,
        &direct,
        &FreshCounters::new(),
        None,
    );
    for graph in [&fixture.graph, &leaf, native.definition.graph()] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), registry)
        .unwrap()
        .admit(root_request)
        .await
        .unwrap()
        .stored()
        .clone();
    let mut proxy = CommitProxy::start(connection, cut, operation.target()).await;
    let mut writer = TestProcess::spawn(
        WORKER,
        &json!({"role":"cut","scenario":scenario,"proxy_port":proxy.port()}),
    );
    let ready = writer.ready().await.unwrap();
    assert_eq!(
        ready["phase"],
        if operation == Operation::Admission {
            "before_admission"
        } else {
            "before_registration"
        }
    );
    let prepared: Prepared = serde_json::from_value(ready["prepared"].clone()).unwrap();
    let key = prepared.intent.key().clone();
    scenario.key = Some(key.clone());
    assert_eq!(key.parent_run_id(), scenario.run);
    assert_eq!(prepared.intent.child(), &scenario.candidate);
    assert!(!key.parent().graph_namespace().is_root());
    let initial_fence = prepared.owner.fence().clone();
    let frame = prepared.frame.frame().clone();
    let before = snapshot(pool, &tenant).await;
    let before_child = if operation == Operation::Registration {
        Some(ownership_fingerprint(
            &store.load_child_run(&key).await.unwrap(),
        ))
    } else {
        None
    };
    assert!(store.load_child_join(key.parent()).await.unwrap().is_none());
    writer.resume();
    proxy.wait_cut(cut).await;
    let pid = proxy.backend_pid();
    let at_cut = snapshot(pool, &tenant).await;
    if cut == Cut::BeforeCommit {
        assert_eq!(
            business_facts(at_cut.clone(), scenario.run),
            business_facts(before, scenario.run),
            "unforwarded target COMMIT leaked composite business facts"
        );
        assert_eq!(
            store
                .load_run(&tenant, scenario.run)
                .await
                .unwrap()
                .lease()
                .unwrap()
                .fence(),
            &initial_fence
        );
        assert!(store.load_child_join(key.parent()).await.unwrap().is_none());
        if operation == Operation::Admission {
            assert!(matches!(
                store.load_child_run(&key).await,
                Err(StoreError::ChildRunNotFound)
            ));
            assert!(
                store
                    .list_child_run_identities(&tenant, scenario.run)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    } else if operation == Operation::Admission {
        assert_single_row(&at_cut, "child_run_ownership");
        assert_eq!(
            store.load_child_run(&key).await.unwrap().intent(),
            &prepared.intent
        );
        assert!(store.load_child_join(key.parent()).await.unwrap().is_none());
        assert_eq!(
            store
                .load_run(&tenant, scenario.run)
                .await
                .unwrap()
                .lease()
                .unwrap()
                .fence(),
            &initial_fence
        );
    } else {
        let joined = store.load_child_join(key.parent()).await.unwrap().unwrap();
        assert_eq!(joined.request().keys(), &[key.clone()]);
        assert!(
            joined.binding().is_none() && joined.head().is_none() && joined.consumed().is_none()
        );
        assert!(
            store
                .load_run(&tenant, scenario.run)
                .await
                .unwrap()
                .lease()
                .is_none()
        );
        assert_eq!(
            Some(ownership_fingerprint(
                &store.load_child_run(&key).await.unwrap()
            )),
            before_child
        );
    }
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
    if cut == Cut::CommitResponse {
        assert_eq!(proxy.messages_while_ack_held(), 0);
    }
    writer.kill_and_reap().await;
    proxy.stop().await;
    wait_backend_gone(pool, pid).await;
    assert_eq!(
        business_facts(snapshot(pool, &tenant).await, scenario.run),
        business_facts(at_cut, scenario.run),
        "kill/disconnect changed independently observed committed business facts"
    );

    // An admission ACK is a historical owning read. It grants no fresh work on
    // the old physical fence, even if its candidate identities differ.
    let mut original_child = None;
    if operation == Operation::Registration || cut == Cut::CommitResponse {
        original_child = Some(Box::pin(historical_admission_ack(store, &prepared, &schemas)).await);
    }
    let registered_fence;
    if operation == Operation::Registration && cut == Cut::CommitResponse {
        registered_fence = initial_fence.clone();
        let stable = business_facts(snapshot(pool, &tenant).await, scenario.run);
        historical_registration_ack(store, &prepared).await;
        assert!(matches!(
            store
                .claim_lease(&tenant, scenario.run, AttemptId::generate())
                .await,
            Err(StoreError::RunNotRunnable)
        ));
        assert_eq!(
            stable,
            business_facts(snapshot(pool, &tenant).await, scenario.run)
        );
    } else {
        let mut recovery =
            TestProcess::spawn(WORKER, &json!({"role":"register","scenario":scenario}));
        let recovered = recovery.ready().await.unwrap();
        assert_eq!(recovered["phase"], "registered");
        assert_eq!(recovered["launched"], 1);
        assert_eq!(recovered["resumed"], 0);
        assert_eq!(recovered["direct_retries"], 1);
        registered_fence = serde_json::from_value::<RunFence>(recovered["fence"].clone()).unwrap();
        assert!(registered_fence.epoch() > initial_fence.epoch());
        let original = store.load_child_run(&key).await.unwrap();
        if let Some(expected) = &original_child {
            assert_eq!(&ownership_fingerprint(&original), expected);
        }
        original_child = Some(ownership_fingerprint(&original));
        if cut == Cut::CommitResponse || operation == Operation::Registration {
            assert_ne!(
                recovered["candidate_child"],
                serde_json::to_value(original.intent().child().provenance().run_id()).unwrap()
            );
        } else {
            assert_eq!(
                recovered["candidate_child"],
                serde_json::to_value(original.intent().child().provenance().run_id()).unwrap()
            );
        }
        recovery.kill_and_reap().await;
    }
    let original = store.load_child_run(&key).await.unwrap();
    assert_eq!(Some(ownership_fingerprint(&original)), original_child);
    let child_run = original.intent().child().provenance().run_id();
    let join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    let registration = join.registration().clone();
    assert_eq!(join.request().keys(), &[key.clone()]);
    assert!(join.binding().is_none() && join.head().is_none() && join.consumed().is_none());
    assert!(
        store
            .load_run(&tenant, scenario.run)
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    assert!(matches!(
        store
            .claim_lease(&tenant, scenario.run, AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, scenario.run)
            .await
            .unwrap()
            .as_ref(),
        Some(admitted.checkpoint())
    );
    let active = store
        .load_active_graph_frame(&tenant, scenario.run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint().frame(), &frame);
    assert_eq!(active.checkpoint().head(), prepared.frame.head());
    let account = store
        .load_child_budget_account(&tenant, scenario.run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    let original_reservation = account.children()[0].reservation().clone();
    assert_eq!(account.children()[0].key(), &key);
    assert!(account.children()[0].settlement().is_none());
    let identities = store
        .list_child_run_identities(&tenant, scenario.run)
        .await
        .unwrap();
    assert_eq!(identities.len(), 1);
    assert_eq!(identities[0].1, child_run);

    // Real application, authenticated priced terminal, original reconciler and
    // publisher. No control-plane terminal substitutes for child execution.
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
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let child_result = child_loop
        .run(
            child_lease.lease().fence().clone(),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert!(matches!(
        child_result.outcome(),
        AgentLoopOutcome::Succeeded(_)
    ));
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 1);
    let child = store.load_run(&tenant, child_run).await.unwrap();
    assert_eq!(child.lifecycle().status(), RunStatus::Succeeded);
    let usage = child.lifecycle().terminal_usage().unwrap().clone();
    assert_eq!(usage.model_turns().get(), 2);
    assert_eq!(usage.tool_calls().get(), 1);
    assert_eq!(usage.unpriced_cost_events(), ExecutionCount::ZERO);
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
    let publication = DurableChildJoinPublisher::new(
        store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(publication.items().len(), 1);
    publication.items()[0].result().unwrap();
    let published = store.load_child_join(key.parent()).await.unwrap().unwrap();
    assert_eq!(published.registration(), &registration);
    assert_eq!(published.binding().unwrap().terminals()[0].usage(), &usage);
    let publication_head = published.head().unwrap().clone();
    let child_snapshot = child_facts(snapshot(pool, &tenant).await, child_run);
    assert_eq!(row_count(&child_snapshot, "model_invocations"), 2);
    assert_eq!(row_count(&child_snapshot, "tool_invocations"), 1);
    let mut resumed = TestProcess::spawn(WORKER, &json!({"role":"resume","scenario":scenario}));
    let returned = resumed.ready().await.unwrap();
    assert_eq!(returned["phase"], "returned");
    assert_eq!(returned["launched"], 0);
    assert_eq!(returned["resumed"], 1);
    let final_fence: RunFence = serde_json::from_value(returned["fence"].clone()).unwrap();
    assert!(final_fence.epoch() > registered_fence.epoch());
    let consumed = store.load_child_join(key.parent()).await.unwrap().unwrap();
    assert_eq!(consumed.registration(), &registration);
    assert_eq!(consumed.head(), Some(&publication_head));
    let result = store.load_pending_node_result(key.parent()).await.unwrap();
    assert_eq!(consumed.consumed(), Some(result.journal_head()));
    assert_eq!(result.intent().child_join(), Some(&publication_head));
    let history = Box::pin(retained_attempt_history(store, key.parent())).await;
    let expected_attempts = if operation == Operation::Registration && cut == Cut::CommitResponse {
        2
    } else {
        3
    };
    assert_eq!(history.as_slice().len(), expected_attempts);
    assert_eq!(history.as_slice()[0].start().head(), prepared.owner);
    for attempt in &history.as_slice()[..expected_attempts - 1] {
        assert_eq!(attempt.status(), NodeAttemptStatus::Executing);
        assert!(attempt.completion().is_none());
    }
    let completed = history.as_slice().last().unwrap();
    assert_eq!(completed.start().fence(), &final_fence);
    assert_eq!(completed.status(), NodeAttemptStatus::Succeeded);
    assert_eq!(
        completed.completion().unwrap().journal_head(),
        result.journal_head()
    );
    let frame_return = store
        .load_graph_frame_return(&tenant, scenario.run, key.parent().graph_namespace())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame_return.completion().start().fence(), &final_fence);
    assert!(
        store
            .load_active_graph_frame(&tenant, scenario.run)
            .await
            .unwrap()
            .is_none()
    );
    let final_parent = store.load_run(&tenant, scenario.run).await.unwrap();
    assert_eq!(final_parent.lifecycle().status(), RunStatus::Active);
    assert!(final_parent.lifecycle().result().is_none());
    assert_eq!(
        child_snapshot,
        child_facts(snapshot(pool, &tenant).await, child_run)
    );
    assert_eq!(
        Some(ownership_fingerprint(
            &store.load_child_run(&key).await.unwrap()
        )),
        original_child
    );
    let final_snapshot = snapshot(pool, &tenant).await;
    for table in [
        "child_run_ownership",
        "child_run_joins",
        "child_run_join_bindings",
        "child_run_join_consumptions",
        "child_run_settlements",
    ] {
        assert_single_row(&final_snapshot, table);
    }
    assert_eq!(row_count(&final_snapshot, "graph_frame_entries"), 1);
    assert_eq!(row_count(&final_snapshot, "graph_frame_returns"), 1);
    let final_account = store
        .load_child_budget_account(&tenant, scenario.run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_account.children().len(), 1);
    assert_eq!(
        final_account.children()[0].reservation(),
        &original_reservation
    );
    assert!(final_account.children()[0].settlement().is_some());
    // Return does not erase the original owning records. Replaying the original
    // node/fence and new IDs is allowed only to recover the original ACK.
    let before_ack = business_facts(snapshot(pool, &tenant).await, scenario.run);
    assert_eq!(
        Some(Box::pin(historical_admission_ack(store, &prepared, &schemas)).await),
        original_child
    );
    assert_eq!(
        historical_registration_ack(store, &prepared).await,
        registration
    );
    assert_eq!(
        before_ack,
        business_facts(snapshot(pool, &tenant).await, scenario.run)
    );
    // Historical ACK recovery must not turn the dead writer's fence into a
    // fresh launch grant. The real public Driver refuses it before callbacks.
    let rejected_counters = FreshCounters::new();
    let rejected_direct = Arc::new(FreshDirectEvidence {
        store: store.clone(),
        key: Arc::new(tokio::sync::Mutex::new(Some(key.clone()))),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let rejected_registry = fault_registry(
        store,
        &fixture,
        &leaf,
        &native,
        &scenario,
        &replacement_candidate(&scenario.candidate),
        original.intent().initial_state(),
        &schemas,
        &rejected_direct,
        &rejected_counters,
        None,
    );
    let rejected_driver =
        DurableGraphDriver::new(store.clone(), rejected_registry, short_driver_options())
            .unwrap()
            .with_direct_usage_evidence(rejected_direct);
    let before_stale = business_facts(snapshot(pool, &tenant).await, scenario.run);
    assert!(
        matches!(rejected_driver.drive(initial_fence,CancellationSignal::never()).await,
        Err(GraphDriverError::Store {source}) if matches!(source.as_ref(),StoreError::StaleFence))
    );
    assert_eq!(rejected_counters.launched.load(Ordering::SeqCst), 0);
    assert_eq!(rejected_counters.resumed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected_counters.leaf_tail.load(Ordering::SeqCst), 0);
    assert_eq!(rejected_counters.root_tail.load(Ordering::SeqCst), 0);
    assert_eq!(
        before_stale,
        business_facts(snapshot(pool, &tenant).await, scenario.run)
    );
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 1);
    resumed.kill_and_reap().await;
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
        .unwrap();
    println!(
        "\nSTATEKNOT_SCOPED_ADMISSION_REGISTRATION_FAULT_EVIDENCE={}",
        json!({
            "profile":"scoped-admission-registration-fault-v1", "operation":operation.name(),
            "cut":if cut==Cut::BeforeCommit {"commit_not_forwarded"} else {"commit_response_withheld"},
            "commit_forwarded":cut==Cut::CommitResponse,"commit_response_forwarded":false,
            "real_writer_killed":true,"real_short_lease_expiry_for_unregistered":!(operation==Operation::Registration&&cut==Cut::CommitResponse),
            "cold_driver_registry_connection":true,"historical_admission_and_registration_ack_after_return":true,
            "original_frame_retained":true,"single_child_account_reservation":true,
            "unpublished_claim_run_not_runnable":true,"real_child_succeeded":true,"model_calls":2,"tool_calls":1,
            "single_registration_publication_consumption_settlement":true,"real_return_root_active_handoff":true,
            "old_fence_fresh_driver_stale_fence_before_callbacks":true,"postgres":version,
            "termination":if cfg!(unix) {"SIGKILL"} else {"Child::kill"},"invariants":"passed",
            "os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        })
    );
}

#[tokio::test]
async fn scoped_admission_and_join_registration_commit_cuts_survive_kill_and_cold_drive() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let connection = std::env::var(DATABASE_URL_ENV)
        .ok()
        .map(|url| url.parse::<PgConnectOptions>().unwrap());
    if let Some(connection) = &connection {
        let _ = loopback_target(connection);
    }
    let Some(store) = test_store().await else {
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    for operation in [Operation::Admission, Operation::Registration] {
        for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
            Box::pin(qualify_case(
                &store,
                &pool,
                connection.as_ref().unwrap(),
                operation,
                cut,
            ))
            .await;
        }
    }
    pool.close().await;
    store.close().await;
}
