// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_core::{
    GraphComposition, GraphNodeSource, GraphRoute, GraphSubgraphCall, NodeStateUpdate, RouteId,
    SharedStateSubgraph, TimerFiringIntent,
};
use std::{collections::BTreeSet, sync::Mutex};

type Trace = Arc<Mutex<Vec<(NodeId, u64)>>>;

#[tokio::test]
async fn composed_call_rejects_a_changed_template_before_any_executor_runs() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let trace = Trace::default();
    let (original, _) = composition_fixture(3, 2, 16, &trace);
    let (_, registry) = composition_fixture(2, 2, 16, &trace);
    let tenant_id = tenant("composition-version-pin");
    let run_id = RunId::generate();
    start_run(&store, original.graph(), tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        driver
            .drive(lease.fence().clone(), CancellationSignal::never())
            .await,
        Err(GraphDriverError::ExecutableGraphUnavailable { .. })
    ));
    assert!(trace.lock().unwrap().is_empty());
    store.release_lease(lease.fence()).await.unwrap();
    assert!(
        !store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

#[tokio::test]
async fn composed_node_orphan_is_taken_over_under_the_next_fence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let trace = Trace::default();
    let (composition, registry) = composition_fixture(2, 1, 16, &trace);
    let tenant_id = tenant("composition-orphan");
    let run_id = RunId::generate();
    start_run(&store, composition.graph(), tenant_id.clone(), run_id).await;
    let original = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let checkpoint = store
        .load_current_checkpoint(&tenant_id, run_id)
        .await
        .unwrap()
        .unwrap();
    let node_id = checkpoint.ready_nodes().iter().next().unwrap().clone();
    let activation = NodeActivation::for_ready_root(&checkpoint, node_id.clone()).unwrap();
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        EventId::generate(),
        stored.journal_head().unwrap().clone(),
        original.fence().clone(),
    );
    store
        .start_node_attempt(append, activation.clone(), AttemptId::generate())
        .await
        .unwrap();
    // Crash after start but before executor launch; no completion or pending result.
    store.release_lease(original.fence()).await.unwrap();
    let takeover = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(takeover.fence().epoch() > original.fence().epoch());
    let driver = DurableGraphDriver::new(store.clone(), registry, quantum(2)).unwrap();
    let recovered = driver
        .drive(takeover.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        recovered.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    assert_eq!(trace.lock().unwrap().as_slice(), &[(node_id, 0)]);
    let attempts = store
        .load_node_attempt_history_page(
            &activation,
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(attempts.records().len(), 2);
    assert_eq!(attempts.records()[1].start().activation(), &activation);
    store.close().await;
}

fn id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}
fn ids(values: &[&str]) -> ReadyNodes {
    ReadyNodes::try_new(values.iter().map(|v| id(v))).unwrap()
}
fn route(name: &str, target: &str) -> GraphRoute {
    GraphRoute::new(RouteId::new(name).unwrap(), ids(&[target])).unwrap()
}
fn terminal(name: &str) -> GraphNode {
    GraphNode::new(id(name), None, GraphRoutes::empty(), None, true).unwrap()
}

struct CompositionReducer {
    reference: GraphReducerReference,
}
impl GraphReducer for CompositionReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        Ok(updates
            .last()
            .map_or_else(|| state.clone(), |update| update.update().data().clone()))
    }
}

struct CompositionNode {
    graph: GraphReference,
    node_id: NodeId,
    source: Option<GraphNodeSource>,
    update_schema: SchemaReference,
    output_schema: SchemaReference,
    stop_after: u16,
    trace: Trace,
}
impl GraphNodeExecutor for CompositionNode {
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
        Box::pin(async move {
            assert_eq!(context.checkpoint().graph(), &self.graph);
            assert_eq!(context.attempt().activation().node_id(), &self.node_id);
            assert!(context.attempt().activation().graph_namespace().is_root());
            self.trace
                .lock()
                .unwrap()
                .push((self.node_id.clone(), context.checkpoint().superstep().get()));
            let mut change = NodeStateChange::Unchanged;
            let control = if let Some(source) = &self.source {
                if source.node_id().as_str() == "work" {
                    change = NodeStateChange::Update {
                        update: NodeStateUpdate::new(
                            self.update_schema.clone(),
                            BoundedJson::try_from_value(
                                json!({"step": format!("iteration-{}", source.iteration())}),
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                    };
                    NodeControl::Continue
                } else {
                    assert_eq!(
                        context.checkpoint().state().data().as_value()["step"],
                        format!("iteration-{}", source.iteration())
                    );
                    let route = if source.iteration() + 1 >= self.stop_after {
                        "accept"
                    } else {
                        "retry"
                    };
                    NodeControl::Route {
                        route_id: source
                            .route_id(&RouteId::new(route).unwrap())
                            .unwrap()
                            .clone(),
                    }
                }
            } else if self.node_id == id("exhausted") {
                return Err(GraphNodeExecutionError::new(
                    test_failure(
                        "graph.loop_exhausted",
                        "The review loop exhausted its iteration budget.",
                    ),
                    BudgetUsage::zero(),
                )
                .unwrap());
            } else {
                NodeControl::Terminal {
                    output: NodeTerminalOutput::new(
                        self.output_schema.clone(),
                        BoundedJson::try_from_value(json!({"ok": true})).unwrap(),
                    )
                    .unwrap(),
                }
            };
            Ok(GraphNodeExecution::new(
                change,
                control,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

fn composition_fixture(
    iterations: u16,
    stop_after: u16,
    steps: u64,
    trace: &Trace,
) -> (GraphComposition, ExecutableGraphRegistry) {
    let base = driver_fixture();
    let reducer = GraphReducerReference::new(
        capability("composition-reducer"),
        Digest::sha256(b"replace last ordered update v1"),
    );
    let compile = |name, entry, nodes| {
        CompiledGraph::compile(
            capability(name),
            base.graph.input_schema().clone(),
            base.graph.state_schema().clone(),
            base.graph.update_schema().clone(),
            base.graph.output_schema().clone(),
            reducer.clone(),
            entry,
            nodes,
            GraphExecutionLimits::new(Superstep::new(steps).unwrap(), 1).unwrap(),
        )
        .unwrap()
    };
    let body = compile(
        "composition-body",
        ids(&["work"]),
        vec![
            GraphNode::new(
                id("work"),
                Some(ids(&["decide"])),
                GraphRoutes::empty(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                id("decide"),
                None,
                GraphRoutes::try_new([route("accept", "done"), route("retry", "again")]).unwrap(),
                None,
                false,
            )
            .unwrap(),
            terminal("done"),
            terminal("again"),
        ],
    );
    let parent = compile(
        "composition-parent",
        ids(&["review"]),
        vec![
            GraphNode::new(
                id("review"),
                None,
                GraphRoutes::try_new([route("done", "finish"), route("again", "exhausted")])
                    .unwrap(),
                None,
                false,
            )
            .unwrap(),
            terminal("finish"),
            terminal("exhausted"),
        ],
    );
    let composition = GraphComposition::compile(
        parent,
        [GraphSubgraphCall::bounded_loop(
            id("review"),
            SharedStateSubgraph::new(body).unwrap(),
            id("again"),
            iterations,
        )
        .unwrap()],
    )
    .unwrap();
    let graph = composition.graph();
    let mut registry = ExecutableGraphRegistryBuilder::new(base.registry.schemas().clone());
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(CompositionReducer { reference: reducer }))
        .unwrap();
    for node in graph.nodes() {
        registry
            .register_node(Arc::new(CompositionNode {
                graph: graph.reference(),
                node_id: node.node_id().clone(),
                source: composition.node_source(node.node_id()).cloned(),
                update_schema: graph.update_schema().clone(),
                output_schema: graph.output_schema().clone(),
                stop_after,
                trace: trace.clone(),
            }))
            .unwrap();
    }
    (composition, registry.build().unwrap())
}

fn quantum(events: u16) -> DurableGraphDriverOptions {
    DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        events.into(),
        Duration::from_secs(10),
        Duration::from_secs(60),
        3,
        Duration::from_millis(25),
    )
    .unwrap()
}

async fn admit_composition(
    store: &PostgresStore,
    graph: &CompiledGraph,
    tenant_id: TenantId,
    run_id: RunId,
) -> GraphTerminalEvidence {
    let evidence = terminal_evidence(graph);
    let provenance = AgentResultProvenance::for_agent(
        tenant_id,
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        evidence.descriptor(),
    );
    Box::pin(start_run_with_provenance(
        store,
        graph,
        provenance,
        json!({"step": "initial"}),
    ))
    .await;
    evidence
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pending_composed_nodes_survive_driver_recreation_and_exit_early() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let trace = Trace::default();
    let (composition, _) = composition_fixture(3, 2, 16, &trace);
    let tenant_id = tenant("composition-replay");
    let run_id = RunId::generate();
    let evidence = admit_composition(&store, composition.graph(), tenant_id.clone(), run_id).await;
    let mut finished = false;
    for pass in 0..12 {
        // Discard all executable bindings between quanta, including a committed
        // pending result at checkpoint zero before its first barrier.
        let (rebuilt, registry) = composition_fixture(3, 2, 16, &trace);
        assert_eq!(rebuilt.graph().reference(), composition.graph().reference());
        let lease = store
            .claim_lease(&tenant_id, run_id, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        let driver = DurableGraphDriver::new(
            store.clone(),
            registry.clone(),
            quantum(if pass == 0 { 2 } else { 3 }),
        )
        .unwrap();
        let result = driver
            .drive(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap();
        if pass == 0 {
            assert_eq!(result.report().node_attempts_completed(), 1);
            assert_eq!(result.report().barriers_committed(), 0);
            assert_eq!(
                store
                    .load_current_checkpoint(&tenant_id, run_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .superstep()
                    .get(),
                0
            );
        }
        let (outcome, _) = result.into_parts();
        match outcome {
            GraphDriveOutcome::Yielded { .. } => {}
            GraphDriveOutcome::LifecycleBarrierReady(handoff) => {
                let lifecycle = DurableGraphLifecycle::new(
                    store.clone(),
                    registry,
                    Arc::new(StaticLifecycleEvidence {
                        terminal: evidence.clone(),
                        failure: None,
                    }),
                    DurableGraphLifecycleOptions::default(),
                )
                .unwrap();
                lifecycle.commit_barrier(*handoff.clone()).await.unwrap();
                assert!(matches!(
                    lifecycle.commit_barrier(*handoff).await.unwrap(),
                    GraphBarrierLifecycleOutcome::Succeeded(
                        BarrierCommitOutcome::Idempotent { .. }
                    )
                ));
                finished = true;
                break;
            }
            other => panic!("unexpected composed outcome: {other:?}"),
        }
    }
    assert!(finished);
    let calls = trace.lock().unwrap().clone();
    assert_eq!(calls.len(), 5); // two iterations of two nodes, then parent finish
    assert_eq!(
        calls
            .iter()
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>()
            .len(),
        5
    );
    assert_eq!(
        calls.iter().map(|(_, step)| *step).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    let checkpoint = store
        .load_current_checkpoint(&tenant_id, run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        checkpoint.state().data().as_value(),
        &json!({"step": "iteration-1"})
    );
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Succeeded
    );
    store.close().await;
}

#[tokio::test]
async fn bounded_composition_exhaustion_runs_the_explicit_failure_exit() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let trace = Trace::default();
    let (composition, registry) = composition_fixture(2, 99, 16, &trace);
    let tenant_id = tenant("composition-exhaustion");
    let run_id = RunId::generate();
    let terminal = admit_composition(&store, composition.graph(), tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let agent = DurableAgentLoop::new(
        store.clone(),
        registry,
        Arc::new(StaticLifecycleEvidence {
            terminal,
            failure: Some(GraphFailureEvidence::new(
                test_failure(
                    "graph.loop_exhausted",
                    "The review loop exhausted its iteration budget.",
                ),
                BudgetUsage::zero(),
            )),
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        agent
            .run(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        AgentLoopOutcome::Failed(_)
    ));
    let calls = trace.lock().unwrap().clone();
    assert_eq!(calls.len(), 5);
    assert_eq!(calls.last().unwrap().0, id("exhausted"));
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(
        stored
            .lifecycle()
            .terminal_failure()
            .unwrap()
            .code()
            .as_str(),
        "graph.loop_exhausted"
    );
    store.close().await;
}

#[tokio::test]
async fn recovered_global_step_limit_blocks_dispatch_and_commits_replayable_failure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let trace = Trace::default();
    let (composition, registry) = composition_fixture(3, 99, 2, &trace);
    let tenant_id = tenant("composition-step-limit");
    let run_id = RunId::generate();
    let terminal = admit_composition(&store, composition.graph(), tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(store.clone(), registry.clone(), quantum(6)).unwrap();
    assert!(matches!(
        driver
            .drive(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let recovered = DurableGraphDriver::new(
        store.clone(),
        registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .drive(lease.fence().clone(), CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(recovered.report().replay().barriers_replayed(), 2);
    assert_eq!(recovered.report().node_attempts_started(), 0);
    let GraphDriveOutcome::Blocked(handoff) = recovered.into_parts().0 else {
        panic!("step ceiling must be supervised");
    };
    assert!(handoff.blockers().superstep_limit_reached());
    assert_eq!(trace.lock().unwrap().len(), 2);
    let unavailable = DurableGraphLifecycle::new(
        store.clone(),
        registry.clone(),
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(unavailable.resolve_blocked(*handoff.clone()).await.is_err());
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        registry,
        Arc::new(StaticLifecycleEvidence {
            terminal,
            failure: Some(GraphFailureEvidence::new(
                test_failure("unused.provider_code", "Trusted cumulative usage."),
                BudgetUsage::zero(),
            )),
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    lifecycle.resolve_blocked(*handoff.clone()).await.unwrap();
    assert!(matches!(
        unavailable.resolve_blocked(*handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Failed(stateknot_store_postgres::AppendOutcome::Idempotent(
            _
        ))
    ));
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(
        stored
            .lifecycle()
            .terminal_failure()
            .unwrap()
            .code()
            .as_str(),
        "runtime.graph.superstep_limit_reached"
    );
    assert!(stored.lease().is_none());
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn composed_wait_returns_to_parent_only_after_durable_timer_resolution() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let base = driver_fixture();
    let make = |name, entry, nodes| {
        CompiledGraph::compile(
            capability(name),
            base.graph.input_schema().clone(),
            base.graph.state_schema().clone(),
            base.graph.update_schema().clone(),
            base.graph.output_schema().clone(),
            base.graph.reducer().clone(),
            entry,
            nodes,
            base.graph.limits(),
        )
        .unwrap()
    };
    let child = make(
        "wait-composition-body",
        ids(&["pause"]),
        vec![
            GraphNode::new(
                id("pause"),
                None,
                GraphRoutes::empty(),
                Some(ids(&["done"])),
                false,
            )
            .unwrap(),
            terminal("done"),
        ],
    );
    let parent = make(
        "wait-composition-parent",
        ids(&["call"]),
        vec![
            GraphNode::new(
                id("call"),
                None,
                GraphRoutes::try_new([route("done", "finish")]).unwrap(),
                None,
                false,
            )
            .unwrap(),
            terminal("finish"),
        ],
    );
    let composition = GraphComposition::compile(
        parent,
        [GraphSubgraphCall::once(
            id("call"),
            SharedStateSubgraph::new(child).unwrap(),
        )],
    )
    .unwrap();
    let tenant_id = tenant("composition-wait");
    let run_id = RunId::generate();
    let terminal = admit_composition(&store, composition.graph(), tenant_id.clone(), run_id).await;
    let admitted = store.load_run(&tenant_id, run_id).await.unwrap();
    let due_at =
        Timestamp::from_unix_micros(admitted.lifecycle().changed_at().unix_micros() + 5_000_000)
            .unwrap();
    let timer_id = TimerId::generate();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ExecutableGraphRegistryBuilder::new(base.registry.schemas().clone());
    registry
        .register_graph(composition.graph().clone())
        .unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: base.graph.reducer().clone(),
        }))
        .unwrap();
    for node in composition.graph().nodes() {
        registry
            .register_node(Arc::new(TestNodeExecutor {
                graph: composition.graph().reference(),
                node_id: node.node_id().clone(),
                behavior: if composition.node_source(node.node_id()).is_some() {
                    TestNodeBehavior::Wait(
                        NodeWaits::try_new([NodeWait::timer(
                            timer_id,
                            RunTimerKind::Sleep,
                            due_at,
                        )])
                        .unwrap(),
                    )
                } else {
                    TestNodeBehavior::Terminal(composition.graph().output_schema().clone())
                },
                delay: Duration::ZERO,
                calls: calls.clone(),
            }))
            .unwrap();
    }
    let registry = registry.build().unwrap();
    let agent = DurableAgentLoop::new(
        store.clone(),
        registry.clone(),
        Arc::new(StaticLifecycleEvidence {
            terminal: terminal.clone(),
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(matches!(
        agent
            .run(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        AgentLoopOutcome::Waiting(_)
    ));
    let checkpoint = store
        .load_current_checkpoint(&tenant_id, run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.ready_nodes(), &ids(&["finish"]));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(5_010)).await;
    let timer = store
        .load_durable_timer(&tenant_id, run_id, timer_id)
        .await
        .unwrap();
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    let event_id = EventId::generate();
    let append = JournalAppend::new(
        JournalExpectation::exact(stored.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(tenant_id.clone(), run_id, event_id, test_payload())
            .unwrap(),
    )
    .unwrap();
    store
        .fire_timer(
            append,
            stored.lifecycle().revision(),
            TimerFiringIntent::new(&timer, event_id).unwrap(),
        )
        .await
        .unwrap();
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let resumed = DurableAgentLoop::new(
        store.clone(),
        registry,
        Arc::new(StaticLifecycleEvidence {
            terminal,
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        resumed
            .run(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        AgentLoopOutcome::Succeeded(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    store.close().await;
}
