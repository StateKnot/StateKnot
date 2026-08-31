// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` durable graph-driver tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use stateknot_core::{
    AgentResultProvenance, AttemptId, BoundedJson, BoxFuture, BudgetUsage, CancellationSignal,
    CapabilityIdentity, CapabilityName, CapabilityReference, CheckpointId, CheckpointState,
    CheckpointWrite, CompiledGraph, Digest, EventId, GraphBarrierDisposition, GraphExecutionLimits,
    GraphNode, GraphReducer, GraphReducerError, GraphReducerInput, GraphReducerReference,
    GraphReference, GraphRoutes, InvocationId, IssuerId, JournalAppend, JournalEventIntent,
    JournalEventKind, JournalExpectation, JournalPayload, NodeControl, NodeId,
    NodeInvocationBindings, NodeStateChange, NodeTerminalOutput, PrincipalIdentity, QuarantineId,
    ReadyNodes, RunFence, RunId, RunTransition, SchemaId, SchemaReference, SubjectId, Superstep,
    TenantId, ThreadId, Version,
};
use stateknot_runtime::{
    DurableGraphDriver, DurableGraphDriverOptions, ExecutableGraphRegistry,
    ExecutableGraphRegistryBuilder, GraphDriveOutcome, GraphDriverError, GraphNodeContext,
    GraphNodeExecution, GraphNodeExecutionError, GraphNodeExecutor, JsonSchemaRegistryBuilder,
    JsonSchemaRegistryLimits, register_standard_graph_driver_event_schema,
};
use stateknot_store_postgres::{
    CheckpointCommitOutcome, CorruptionQuarantineContext, GraphDefinitionRegistrationOutcome,
    GraphReplayLimits, LeaseReleaseOutcome, NodeAttemptCommitOutcome, PostgresStore,
    PostgresStoreOptions, PostgresTransportSecurity, RunProjection, StoreError,
};

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";
static DATABASE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
enum TestNodeBehavior {
    Continue,
    Terminal(SchemaReference),
}

struct TestNodeExecutor {
    graph: GraphReference,
    node_id: NodeId,
    behavior: TestNodeBehavior,
    delay: Duration,
    calls: Arc<AtomicUsize>,
}

impl GraphNodeExecutor for TestNodeExecutor {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn execute(
        &self,
        _: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let control = match &self.behavior {
                TestNodeBehavior::Continue => NodeControl::Continue,
                TestNodeBehavior::Terminal(schema) => NodeControl::Terminal {
                    output: NodeTerminalOutput::new(
                        schema.clone(),
                        BoundedJson::try_from_value(json!({"ok": true})).unwrap(),
                    )
                    .unwrap(),
                },
            };
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                control,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

struct TestReducer {
    reference: GraphReducerReference,
}

impl GraphReducer for TestReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }

    fn reduce(
        &self,
        state: &BoundedJson,
        _: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        Ok(state.clone())
    }
}

struct DriverFixture {
    graph: CompiledGraph,
    registry: ExecutableGraphRegistry,
    first_calls: Arc<AtomicUsize>,
    second_calls: Arc<AtomicUsize>,
}

fn driver_fixture() -> DriverFixture {
    driver_fixture_with_first_delay(Duration::ZERO)
}

fn driver_fixture_with_first_delay(first_delay: Duration) -> DriverFixture {
    let (input_schema, input_document) = schema("driver-input");
    let (state_schema, state_document) = state_schema();
    let (update_schema, update_document) = schema("driver-update");
    let (output_schema, output_document) = schema("driver-output");
    let reducer_reference = GraphReducerReference::new(
        capability("driver-reducer"),
        Digest::sha256(b"stateknot runtime integration reducer v1"),
    );
    let first_id = NodeId::new("Step_A").unwrap();
    let second_id = NodeId::new("Step.B").unwrap();
    let graph = CompiledGraph::compile(
        capability("driver-graph"),
        input_schema.clone(),
        state_schema,
        update_schema,
        output_schema.clone(),
        reducer_reference.clone(),
        ReadyNodes::try_new([first_id.clone()]).unwrap(),
        [
            GraphNode::new(
                first_id.clone(),
                Some(ReadyNodes::try_new([second_id.clone()]).unwrap()),
                GraphRoutes::empty(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(second_id.clone(), None, GraphRoutes::empty(), None, true).unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap();

    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    for (reference, document) in [
        (input_schema, input_document),
        (graph.state_schema().clone(), state_document),
        (graph.update_schema().clone(), update_document),
        (output_schema.clone(), output_document),
    ] {
        schemas.register(reference, document).unwrap();
    }
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    let schemas = schemas.build().unwrap();

    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let graph_reference = graph.reference();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas);
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: reducer_reference,
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference.clone(),
            node_id: first_id,
            behavior: TestNodeBehavior::Continue,
            delay: first_delay,
            calls: Arc::clone(&first_calls),
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference,
            node_id: second_id,
            behavior: TestNodeBehavior::Terminal(output_schema),
            delay: Duration::ZERO,
            calls: Arc::clone(&second_calls),
        }))
        .unwrap();

    DriverFixture {
        graph,
        registry: registry.build().unwrap(),
        first_calls,
        second_calls,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_advances_then_replays_a_noninitial_checkpoint_before_terminal_handoff() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-replay");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;

    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        3,
        Duration::from_secs(10),
        Duration::from_secs(60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry.clone(), options).unwrap();
    let first = driver
        .drive(first_lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        first.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(first.report().durable_events(), 3);
    assert_eq!(first.report().node_attempts_started(), 1);
    assert_eq!(first.report().node_attempts_completed(), 1);
    assert_eq!(first.report().barriers_committed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap()
            .unwrap()
            .superstep()
            .get(),
        1
    );

    let second_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let second = driver
        .drive(second_lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = second.outcome() else {
        panic!("second superstep must reach a terminal lifecycle handoff")
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    assert_eq!(handoff.plan().barrier().successor().superstep().get(), 2);
    assert_eq!(second.report().replay().barriers_replayed(), 1);
    assert_eq!(second.report().replay().checkpoints_validated(), 2);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.release_lease(second_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Released
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_fence_inflight_start_is_never_executed_twice() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-inflight");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        Digest::sha256(b"runtime driver in-flight integration evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(lease.fence().clone(), context)
        .await
        .unwrap();
    let plan = recovery.plan_ready_nodes().await.unwrap();
    let node_id = NodeId::new("Step_A").unwrap();
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        EventId::generate(),
        plan.journal_head().clone(),
        lease.fence().clone(),
    );
    let started = store
        .start_recovered_node_attempt(append, &plan, &node_id, AttemptId::generate())
        .await
        .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    drop(recovery);

    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let outcome = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::Blocked(blocked) = outcome.outcome() else {
        panic!("same-fence unfinished start must block duplicate execution")
    };
    assert_eq!(blocked.blockers().in_flight(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.release_lease(lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Released
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_running_node_renews_its_lease_until_completion() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(1)).await else {
        return;
    };
    let fixture = driver_fixture_with_first_delay(Duration::from_millis(1_300));
    let tenant_id = tenant("runtime-driver-renewal");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        3,
        Duration::from_millis(100),
        Duration::from_secs(3),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry, options).unwrap();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert!(result.report().lease_renewals() >= 5);
    assert_eq!(result.report().node_attempts_completed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn near_expiry_lease_is_refreshed_before_node_code_is_launched() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(1)).await else {
        return;
    };
    let fixture = driver_fixture_with_first_delay(Duration::from_millis(500));
    let tenant_id = tenant("runtime-driver-preflight-renewal");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    tokio::time::sleep(Duration::from_millis(750)).await;

    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        2,
        Duration::from_millis(200),
        Duration::from_secs(2),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry, options).unwrap();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();

    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert!(result.report().lease_renewals() >= 2);
    assert_eq!(result.report().node_attempts_completed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_initial_checkpoint_state_is_quarantined_before_node_launch() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-invalid-initial-state");
    let run_id = RunId::generate();
    start_run_with_state(
        &store,
        &fixture.graph,
        tenant_id.clone(),
        run_id,
        json!({"step": 1}),
    )
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap();

    let error = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphDriverError::Store { source }
            if matches!(source.as_ref(), StoreError::RunQuarantined)
    ));
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let quarantined = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(quarantined.is_quarantined());
    assert!(quarantined.lease().is_none());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successor_fence_takes_over_an_unfinished_attempt_once() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-takeover");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        Digest::sha256(b"runtime driver takeover integration evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(first_lease.fence().clone(), context)
        .await
        .unwrap();
    let plan = recovery.plan_ready_nodes().await.unwrap();
    let node_id = NodeId::new("Step_A").unwrap();
    let started = store
        .start_recovered_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                plan.journal_head().clone(),
                first_lease.fence().clone(),
            ),
            &plan,
            &node_id,
            AttemptId::generate(),
        )
        .await
        .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    drop(recovery);

    let successor = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        2,
        Duration::from_secs(10),
        Duration::from_secs(60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry, options).unwrap();
    let result = driver
        .drive(successor.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(result.report().node_attempts_started(), 1);
    assert_eq!(result.report().node_attempts_completed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

async fn test_store() -> Option<PostgresStore> {
    test_store_with_lease_duration(Duration::from_secs(30)).await
}

async fn test_store_with_lease_duration(lease_duration: Duration) -> Option<PostgresStore> {
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(lease_duration, Duration::from_secs(5 * 60));
    PostgresStore::migrate_database(&database_url, options.clone())
        .await
        .expect("migrations must succeed");
    Some(
        PostgresStore::connect(&database_url, options)
            .await
            .expect("test PostgreSQL must connect with an exact schema"),
    )
}

async fn start_run(
    store: &PostgresStore,
    graph: &CompiledGraph,
    tenant_id: TenantId,
    run_id: RunId,
) -> CheckpointCommitOutcome {
    start_run_with_state(store, graph, tenant_id, run_id, json!({"step": "initial"})).await
}

async fn start_run_with_state(
    store: &PostgresStore,
    graph: &CompiledGraph,
    tenant_id: TenantId,
    run_id: RunId,
    state_value: Value,
) -> CheckpointCommitOutcome {
    let registered = store
        .register_graph_definition(tenant_id.clone(), graph.clone())
        .await
        .unwrap();
    assert!(matches!(
        registered,
        GraphDefinitionRegistrationOutcome::Registered(_)
            | GraphDefinitionRegistrationOutcome::Idempotent(_)
    ));
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(state_value).unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        tenant_id.clone(),
        run_id,
        CheckpointId::generate(),
        graph.reference(),
        state,
        graph.entry_nodes().clone(),
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(tenant_id, run_id, EventId::generate()),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            write,
        )
        .await
        .unwrap()
}

fn schema(name: &str) -> (SchemaReference, Value) {
    let id = format!("https://stknot.com/schemas/tests/{name}/1.0.0");
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object"
    });
    let canonical = serde_json_canonicalizer::to_vec(&document).unwrap();
    (
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(canonical),
        ),
        document,
    )
}

fn state_schema() -> (SchemaReference, Value) {
    let id = "https://stknot.com/schemas/tests/driver-state/1.0.0";
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object",
        "properties": {"step": {"type": "string"}},
        "required": ["step"],
        "additionalProperties": false
    });
    let canonical = serde_json_canonicalizer::to_vec(&document).unwrap();
    (
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(canonical),
        ),
        document,
    )
}

fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/stateknot"
                .parse::<IssuerId>()
                .unwrap(),
            "runtime-driver-tests".parse::<SubjectId>().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn provenance(tenant_id: TenantId, run_id: RunId) -> AgentResultProvenance {
    AgentResultProvenance::new(
        tenant_id,
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        capability("driver-agent"),
    )
}

fn tenant(prefix: &str) -> TenantId {
    TenantId::new(format!("{prefix}-{}", RunId::generate())).unwrap()
}

fn test_payload() -> JournalPayload {
    JournalPayload::new(
        SchemaReference::new(
            "https://stknot.com/schemas/tests/control-event/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"runtime integration control event schema"),
        ),
        JournalEventKind::new("runtime-integration").unwrap(),
        BoundedJson::try_from_value(json!({"test": true})).unwrap(),
    )
    .unwrap()
}

fn control_append(tenant_id: TenantId, run_id: RunId, event_id: EventId) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant_id, run_id, event_id, test_payload()).unwrap(),
    )
    .unwrap()
}

fn worker_append(
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    head: stateknot_core::JournalHead,
    fence: RunFence,
) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::worker(tenant_id, run_id, event_id, fence, test_payload()).unwrap(),
    )
    .unwrap()
}
