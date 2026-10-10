// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Actual nested Driver storage, accounting and application dispatch.

use super::*;
use sqlx_core::connection::ConnectOptions;
use stateknot_core::{
    ByteCount, ExecutionCount, GraphFrameCall, GraphFrameCallPolicy, GraphRoute,
    JournalEventSource, RouteId,
};
use stateknot_core::{
    InterruptId, InterruptResolutionIntent, InterruptResolver, RunInterruptKind, ScopeSet,
};
use stateknot_store_postgres::AppendOutcome;

struct NestedFixture {
    driver: DriverFixture,
    leaf: CompiledGraph,
    leaf_calls: Arc<AtomicUsize>,
    root_calls: Arc<AtomicUsize>,
    intermediate_calls: Vec<Arc<AtomicUsize>>,
    graphs: Vec<CompiledGraph>,
}

fn nested_fixture() -> NestedFixture {
    nested_fixture_with_depth(1)
}

fn nested_fixture_with_depth(depth: u8) -> NestedFixture {
    nested_fixture_with_behavior(depth, &TestNodeBehavior::Continue)
}

fn nested_fixture_with_behavior(depth: u8, first_behavior: &TestNodeBehavior) -> NestedFixture {
    nested_fixture_with_retry(depth, first_behavior, None)
}

#[allow(clippy::too_many_lines)]
fn nested_fixture_with_retry(
    depth: u8,
    first_behavior: &TestNodeBehavior,
    retry_delay: Option<DurationMillis>,
) -> NestedFixture {
    let ordinary = driver_fixture();
    let nodes = if matches!(first_behavior, TestNodeBehavior::Wait(_)) {
        vec![
            GraphNode::new(
                NodeId::new("Step_A").unwrap(),
                None,
                GraphRoutes::empty(),
                Some(ReadyNodes::try_new([NodeId::new("Step.B").unwrap()]).unwrap()),
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("Step.B").unwrap(),
                None,
                GraphRoutes::empty(),
                None,
                true,
            )
            .unwrap(),
        ]
    } else {
        ordinary.graph.nodes().to_vec()
    };
    let leaf = CompiledGraph::compile(
        capability("nested-driver-leaf"),
        ordinary.graph.state_schema().clone(),
        ordinary.graph.state_schema().clone(),
        ordinary.graph.update_schema().clone(),
        ordinary.graph.output_schema().clone(),
        ordinary.graph.reducer().clone(),
        ordinary.graph.entry_nodes().clone(),
        nodes,
        ordinary.graph.limits(),
    )
    .unwrap();
    let mut builder = ExecutableGraphRegistryBuilder::new(ordinary.registry.schemas().clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: leaf.reducer().clone(),
        }))
        .unwrap();
    for node in leaf.nodes() {
        let first = node.node_id() == &NodeId::new("Step_A").unwrap();
        if let Some(delay) = retry_delay.filter(|_| first) {
            builder
                .register_node(Arc::new(RetryOnceNodeExecutor {
                    graph: leaf.reference(),
                    node_id: node.node_id().clone(),
                    calls: Arc::clone(&ordinary.first_calls),
                    delay,
                }))
                .unwrap();
            continue;
        }
        builder
            .register_node(Arc::new(TestNodeExecutor {
                graph: leaf.reference(),
                node_id: node.node_id().clone(),
                behavior: if first {
                    first_behavior.clone()
                } else {
                    TestNodeBehavior::Terminal(leaf.output_schema().clone())
                },
                delay: Duration::ZERO,
                calls: Arc::clone(if first {
                    &ordinary.first_calls
                } else {
                    &ordinary.second_calls
                }),
            }))
            .unwrap();
    }
    builder.register_graph(leaf.clone()).unwrap();
    let mut graphs = vec![leaf.clone()];
    let mut target = leaf.clone();
    let mut intermediate_calls = Vec::new();
    let root_calls = Arc::new(AtomicUsize::new(0));
    for level in 0..depth {
        let call_id = NodeId::new("call").unwrap();
        let finish_id = NodeId::new("finish").unwrap();
        let route = RouteId::new("return").unwrap();
        let call = GraphFrameCall::new(
            call_id.clone(),
            NodeId::new("nested.slot").unwrap(),
            &target,
            route.clone(),
        )
        .unwrap();
        let parent = CompiledGraph::compile(
            capability(&format!("nested-driver-parent-{level}")),
            if level + 1 == depth {
                ordinary.graph.input_schema().clone()
            } else {
                leaf.state_schema().clone()
            },
            leaf.state_schema().clone(),
            leaf.output_schema().clone(),
            leaf.output_schema().clone(),
            leaf.reducer().clone(),
            ReadyNodes::try_new([call_id.clone()]).unwrap(),
            [
                GraphNode::new(
                    call_id,
                    None,
                    GraphRoutes::try_new([GraphRoute::new(
                        route,
                        ReadyNodes::try_new([finish_id.clone()]).unwrap(),
                    )
                    .unwrap()])
                    .unwrap(),
                    None,
                    false,
                )
                .unwrap(),
                GraphNode::new(finish_id.clone(), None, GraphRoutes::empty(), None, true).unwrap(),
            ],
            GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
        )
        .unwrap()
        .with_frame_calls(GraphFrameCallPolicy::new(depth, 16, [call]).unwrap())
        .unwrap();
        let calls = if level + 1 == depth {
            Arc::clone(&root_calls)
        } else {
            let calls = Arc::new(AtomicUsize::new(0));
            intermediate_calls.push(Arc::clone(&calls));
            calls
        };
        builder
            .register_node(Arc::new(TestNodeExecutor {
                graph: parent.reference(),
                node_id: finish_id,
                behavior: TestNodeBehavior::Terminal(parent.output_schema().clone()),
                delay: Duration::ZERO,
                calls,
            }))
            .unwrap();
        builder.register_graph(parent.clone()).unwrap();
        graphs.push(parent.clone());
        target = parent;
    }
    NestedFixture {
        driver: DriverFixture {
            graph: target,
            registry: builder.build().unwrap(),
            first_calls: Arc::clone(&ordinary.first_calls),
            second_calls: Arc::clone(&ordinary.second_calls),
        },
        leaf,
        leaf_calls: ordinary.first_calls,
        root_calls,
        intermediate_calls,
        graphs,
    }
}

struct RetryOnceNodeExecutor {
    graph: GraphReference,
    node_id: NodeId,
    calls: Arc<AtomicUsize>,
    delay: DurationMillis,
}

impl GraphNodeExecutor for RetryOnceNodeExecutor {
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
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let failure = Failure::new(
                    FailureId::generate(),
                    FailureCategory::Internal,
                    FailureCode::new("nested.safe_retry").unwrap(),
                    FailureOrigin::new("stateknot.runtime.integration").unwrap(),
                    FailureMessage::new("Zero-IO fixture retry is safe.").unwrap(),
                    RetryAdvice::SafeAfter { delay: self.delay },
                )
                .unwrap();
                return Err(GraphNodeExecutionError::new(failure, BudgetUsage::zero()).unwrap());
            }
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                NodeControl::Continue,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

#[allow(clippy::too_many_lines)]
fn serial_sibling_fixture() -> NestedFixture {
    let base = nested_fixture();
    let leaf = base.leaf;
    let mut builder = ExecutableGraphRegistryBuilder::new(base.driver.registry.schemas().clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: leaf.reducer().clone(),
        }))
        .unwrap();
    for node in leaf.nodes() {
        let first = node.node_id().as_str() == "Step_A";
        builder
            .register_node(Arc::new(TestNodeExecutor {
                graph: leaf.reference(),
                node_id: node.node_id().clone(),
                behavior: if first {
                    TestNodeBehavior::Continue
                } else {
                    TestNodeBehavior::Terminal(leaf.output_schema().clone())
                },
                delay: Duration::ZERO,
                calls: Arc::clone(if first {
                    &base.leaf_calls
                } else {
                    &base.driver.second_calls
                }),
            }))
            .unwrap();
    }
    builder.register_graph(leaf.clone()).unwrap();
    let first = NodeId::new("call.first").unwrap();
    let second = NodeId::new("call.second").unwrap();
    let finish = NodeId::new("finish").unwrap();
    let first_return = RouteId::new("first.return").unwrap();
    let second_return = RouteId::new("second.return").unwrap();
    let root = CompiledGraph::compile(
        capability("nested-driver-serial-siblings"),
        base.driver.graph.input_schema().clone(),
        leaf.state_schema().clone(),
        leaf.output_schema().clone(),
        leaf.output_schema().clone(),
        leaf.reducer().clone(),
        ReadyNodes::try_new([first.clone()]).unwrap(),
        [
            GraphNode::new(
                first.clone(),
                None,
                GraphRoutes::try_new([GraphRoute::new(
                    first_return.clone(),
                    ReadyNodes::try_new([second.clone()]).unwrap(),
                )
                .unwrap()])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                second.clone(),
                None,
                GraphRoutes::try_new([GraphRoute::new(
                    second_return.clone(),
                    ReadyNodes::try_new([finish.clone()]).unwrap(),
                )
                .unwrap()])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(finish.clone(), None, GraphRoutes::empty(), None, true).unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap()
    .with_frame_calls(
        GraphFrameCallPolicy::new(
            1,
            2,
            [
                GraphFrameCall::new(
                    first,
                    NodeId::new("first.slot").unwrap(),
                    &leaf,
                    first_return,
                )
                .unwrap(),
                GraphFrameCall::new(
                    second,
                    NodeId::new("second.slot").unwrap(),
                    &leaf,
                    second_return,
                )
                .unwrap(),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    builder
        .register_node(Arc::new(TestNodeExecutor {
            graph: root.reference(),
            node_id: finish,
            behavior: TestNodeBehavior::Terminal(root.output_schema().clone()),
            delay: Duration::ZERO,
            calls: Arc::clone(&base.root_calls),
        }))
        .unwrap();
    builder.register_graph(root.clone()).unwrap();
    NestedFixture {
        driver: DriverFixture {
            graph: root.clone(),
            registry: builder.build().unwrap(),
            first_calls: Arc::clone(&base.leaf_calls),
            second_calls: base.driver.second_calls,
        },
        graphs: vec![leaf.clone(), root],
        leaf,
        leaf_calls: base.leaf_calls,
        root_calls: base.root_calls,
        intermediate_calls: Vec::new(),
    }
}

async fn admit_nested_fixture(
    store: &PostgresStore,
    fixture: &NestedFixture,
    tenant_id: &TenantId,
    ids: AgentRunIds,
) {
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_complete_direct_evidence_leaves_no_frame_or_application_dispatch() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-direct-unavailable");
    store
        .register_graph_definition(tenant_id.clone(), fixture.leaf)
        .await
        .unwrap();
    store
        .register_graph_definition(tenant_id.clone(), fixture.driver.graph.clone())
        .await
        .unwrap();
    let facade =
        DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone()).unwrap();
    let admitted = facade
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let source = admitted.run().journal_head().unwrap().clone();
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(UnavailableLifecycleEvidence));
    let error = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            GraphDriverError::DirectUsageEvidence {
                source: GraphLifecycleEvidenceError::Unavailable,
            }
        ),
        "actual error: {error}"
    );
    let actual = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(actual.journal_head(), Some(&source));
    assert_eq!(actual.checkpoint(), admitted.run().checkpoint());
    assert!(actual.lease().is_none());
    assert!(!actual.is_quarantined());
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    store.close().await;
}

/// Complete actual fixture ledger, restricted to the explicit zero-IO test
/// executors. Journal bytes and every created checkpoint are measured; ordinary
/// completion usage is independently reloaded, and unknown event kinds reject.
/// This adapter never substitutes the active-frame minimum usage for total usage.
struct AdmissionDirectEvidence {
    store: PostgresStore,
}

impl AdmissionDirectEvidence {
    #[allow(clippy::too_many_lines)]
    fn usage_at<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
        head: &'a stateknot_core::JournalHead,
    ) -> BoxFuture<'a, Result<BudgetUsage, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            let admitted = self
                .store
                .load_agent_admission(tenant, run)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let mut event_bytes = 0_u64;
            let mut checkpoint_bytes = 0_u64;
            let mut graph_steps = 0_u64;
            let mut graph_depth = 1_u64;
            let mut retries = 0_u64;
            let mut cursor = None;
            loop {
                let page = self
                    .store
                    .load_journal_page(
                        tenant,
                        run,
                        cursor.as_ref(),
                        stateknot_store_postgres::JournalPageSize::new(256).unwrap(),
                    )
                    .await
                    .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                for event in page.events() {
                    if event.sequence().get() > head.sequence().get() {
                        return Err(GraphLifecycleEvidenceError::Unavailable);
                    }
                    event_bytes += serde_json_canonicalizer::to_vec(event).unwrap().len() as u64;
                    let data: Value = serde_json::to_value(event.payload().data()).unwrap();
                    match event.payload().kind().as_str() {
                        "agent-admitted" => {
                            if event != admitted.event() {
                                return Err(GraphLifecycleEvidenceError::Corrupt);
                            }
                            checkpoint_bytes +=
                                serde_json_canonicalizer::to_vec(admitted.checkpoint())
                                    .unwrap()
                                    .len() as u64;
                        }
                        "graph-node-attempt-started"
                        | "graph-node-attempt-succeeded"
                        | "graph-node-attempt-failed" => {
                            let id = data["attempt_id"].as_str().unwrap().parse().unwrap();
                            let attempt = self
                                .store
                                .load_node_attempt(tenant, &run, id)
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                            // This fixture's only application executors are the explicit
                            // zero-IO executors. Persisted attempt usage must agree.
                            if !["Step_A", "Step.B", "finish"]
                                .contains(&attempt.start().activation().node_id().as_str())
                                || attempt.completion().is_some_and(|completion| {
                                    completion.usage() != &BudgetUsage::zero()
                                })
                            {
                                return Err(GraphLifecycleEvidenceError::Unavailable);
                            }
                        }
                        "graph-frame-entered" => {
                            let frame: stateknot_core::GraphFrameIdentity =
                                serde_json::from_value(data["frame"].clone()).unwrap();
                            let entry = self
                                .store
                                .load_graph_frame_entry(tenant, run, frame.namespace())
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                            if entry.event() != event {
                                return Err(GraphLifecycleEvidenceError::Corrupt);
                            }
                            checkpoint_bytes += (serde_json_canonicalizer::to_vec(
                                entry.entry().checkpoint().checkpoint(),
                            )
                            .unwrap()
                            .len()
                                + serde_json_canonicalizer::to_vec(
                                    &entry.entry().checkpoint().head(),
                                )
                                .unwrap()
                                .len()) as u64;
                            graph_steps += 1;
                            graph_depth = graph_depth
                                .max(frame.namespace().as_str().split('/').count() as u64 + 1);
                        }
                        "graph-frame-barrier-committed" => {
                            let namespace =
                                serde_json::from_value(data["graph_namespace"].clone()).unwrap();
                            let base =
                                serde_json::from_value(data["base_checkpoint_id"].clone()).unwrap();
                            let barrier = self
                                .store
                                .load_graph_frame_barrier(tenant, run, &namespace, base)
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?
                                .ok_or(GraphLifecycleEvidenceError::Unavailable)?;
                            if barrier.event() != event {
                                return Err(GraphLifecycleEvidenceError::Corrupt);
                            }
                            checkpoint_bytes += (serde_json_canonicalizer::to_vec(
                                barrier.checkpoint().checkpoint(),
                            )
                            .unwrap()
                            .len()
                                + serde_json_canonicalizer::to_vec(&barrier.checkpoint().head())
                                    .unwrap()
                                    .len()) as u64;
                            graph_steps += 1;
                        }
                        "graph-frame-returned" => {
                            graph_steps += 1;
                        }
                        "graph-frame-caller-rebound" => {
                            graph_steps += 1;
                            retries += 1;
                        }
                        "root-graph-barrier-committed" => {
                            let id =
                                serde_json::from_value(data["successor_checkpoint_id"].clone())
                                    .unwrap();
                            let checkpoint = self
                                .store
                                .load_checkpoint(tenant, run, id)
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                            checkpoint_bytes +=
                                serde_json_canonicalizer::to_vec(&checkpoint).unwrap().len() as u64;
                            graph_steps += 1;
                        }
                        "nested-wait-resolved" => {
                            let id = data["interrupt_id"]
                                .as_str()
                                .ok_or(GraphLifecycleEvidenceError::Corrupt)?
                                .parse()
                                .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
                            let record = self
                                .store
                                .load_interrupt_record(tenant, run, id)
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                            if event.source() != &JournalEventSource::ControlPlane
                                || !record.resolution().is_some_and(|resolution| {
                                    resolution.journal() == &event.head()
                                        && resolution.intent().resolution_payload()
                                            == &test_payload()
                                })
                            {
                                return Err(GraphLifecycleEvidenceError::Corrupt);
                            }
                        }
                        "runtime-integration" => {
                            let run_state = self
                                .store
                                .load_run(tenant, run)
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                            if event.payload() != &test_payload()
                                || event.source().worker_fence().is_some()
                                || !run_state.lifecycle().cancellation_request().is_some_and(
                                    |request| {
                                        request.failure().caused_by_event_id()
                                            == Some(event.event_id())
                                    },
                                )
                            {
                                return Err(GraphLifecycleEvidenceError::Unavailable);
                            }
                        }
                        "graph-frames-closed" => {
                            let closure = self
                                .store
                                .load_graph_frame_closure(tenant, run)
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?
                                .ok_or(GraphLifecycleEvidenceError::Unavailable)?;
                            if closure.event() != event {
                                return Err(GraphLifecycleEvidenceError::Corrupt);
                            }
                        }
                        _ => return Err(GraphLifecycleEvidenceError::Unavailable),
                    }
                }
                let next = page
                    .next_cursor()
                    .ok_or(GraphLifecycleEvidenceError::Unavailable)?;
                if next == *head {
                    break;
                }
                if !page.has_more() {
                    return Err(GraphLifecycleEvidenceError::Unavailable);
                }
                cursor = Some(next);
            }
            let usage = BudgetUsage::builder()
                .graph_depth(ExecutionCount::new(graph_depth))
                .graph_steps(ExecutionCount::new(graph_steps))
                .retries(ExecutionCount::new(retries))
                .event_bytes(ByteCount::new(event_bytes))
                .checkpoint_bytes(ByteCount::new(checkpoint_bytes))
                .build()
                .unwrap();
            Ok(usage)
        })
    }
}

impl GraphLifecycleEvidenceProvider for AdmissionDirectEvidence {
    fn direct_usage_evidence(
        &self,
        context: stateknot_runtime::GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<
        '_,
        Result<stateknot_runtime::GraphDirectUsageEvidence, GraphLifecycleEvidenceError>,
    > {
        Box::pin(async move {
            let usage = self
                .usage_at(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                    context.journal_head(),
                )
                .await?;
            Ok(stateknot_runtime::GraphDirectUsageEvidence::new(
                context, usage,
            ))
        })
    }
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        UnavailableLifecycleEvidence.terminal_evidence(context)
    }
    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            let tenant = context.provenance().tenant_id();
            let run = context.provenance().run_id();
            let usage = self.usage_at(tenant, run, context.journal_head()).await?;
            let page = self
                .store
                .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let failed = page
                .events()
                .iter()
                .rev()
                .find(|event| event.payload().kind().as_str() == "graph-node-attempt-failed")
                .ok_or(GraphLifecycleEvidenceError::Unavailable)?;
            let data: Value = serde_json::to_value(failed.payload().data()).unwrap();
            let attempt = self
                .store
                .load_node_attempt(
                    tenant,
                    &run,
                    data["attempt_id"].as_str().unwrap().parse().unwrap(),
                )
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let failure = attempt
                .completion()
                .and_then(|completion| completion.outcome().failure())
                .cloned()
                .ok_or(GraphLifecycleEvidenceError::Unavailable)?;
            Ok(GraphFailureEvidence::new(failure, usage))
        })
    }
    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            let usage = self
                .usage_at(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                    context.journal_head(),
                )
                .await?;
            Ok(GraphCancellationEvidence::new(usage))
        })
    }
}

struct RetainedDirectContext {
    ledger: AdmissionDirectEvidence,
    original: tokio::sync::Mutex<Option<stateknot_runtime::GraphDirectUsageEvidenceContext>>,
}

impl GraphLifecycleEvidenceProvider for RetainedDirectContext {
    fn direct_usage_evidence(
        &self,
        context: stateknot_runtime::GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<
        '_,
        Result<stateknot_runtime::GraphDirectUsageEvidence, GraphLifecycleEvidenceError>,
    > {
        Box::pin(async move {
            let actual = self.ledger.direct_usage_evidence(context).await?;
            let mut original = self.original.lock().await;
            let retained = original
                .get_or_insert_with(|| actual.context().clone())
                .clone();
            Ok(stateknot_runtime::GraphDirectUsageEvidence::new(
                retained,
                actual.usage().clone(),
            ))
        })
    }
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.terminal_evidence(context)
    }
    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.failure_evidence(context)
    }
    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.cancellation_evidence(context)
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retained_direct_context_rejects_new_journal_without_quarantine_or_repeat_execution() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-direct-stale-context");
    admit_nested_fixture(&store, &fixture, &tenant_id, ids).await;
    let root = store
        .load_run(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .checkpoint()
        .unwrap()
        .clone();
    let bad = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(RetainedDirectContext {
        ledger: AdmissionDirectEvidence {
            store: store.clone(),
        },
        original: tokio::sync::Mutex::new(None),
    }));
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let rejected = bad
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap_err();
    assert!(matches!(
        rejected,
        GraphDriverError::InvalidDirectUsageEvidence
    ));
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert!(run.lease().is_none());
    assert!(!run.is_quarantined());
    assert_eq!(run.checkpoint(), Some(&root));
    let leaf = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leaf.checkpoint().checkpoint().superstep().get(), 0);
    assert_eq!(leaf.caller().fence(), first.fence());
    let page = store
        .load_journal_page(
            &tenant_id,
            ids.run_id(),
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.events().len(), 4);
    assert_eq!(
        page.events().last().unwrap().payload().kind().as_str(),
        "graph-node-attempt-succeeded"
    );
    assert_eq!(
        run.journal_head(),
        Some(&page.events().last().unwrap().head())
    );
    let healthy = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let second = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let resumed = healthy
        .drive(second.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        resumed.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(resumed.report().frame_callers_rebound(), 1);
    assert_eq!(resumed.report().whole_replay_passes(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert!(
        !store
            .load_run(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

#[tokio::test]
async fn framework_entry_and_scoped_application_start_share_original_run() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-entry-start");
    store
        .register_graph_definition(tenant_id.clone(), fixture.leaf.clone())
        .await
        .unwrap();
    store
        .register_graph_definition(tenant_id.clone(), fixture.driver.graph.clone())
        .await
        .unwrap();
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        3,
        Duration::from_secs(10),
        Duration::from_secs(15 * 60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), options)
        .unwrap()
        .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
            store: store.clone(),
        }));
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
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
    assert_eq!(result.report().durable_events(), 3);
    assert_eq!(result.report().node_attempts_started(), 1);
    assert_eq!(result.report().node_attempts_completed(), 1);
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        active.checkpoint().frame().target(),
        &fixture.leaf.reference()
    );
    assert_eq!(active.run().checkpoint(), admitted.run().checkpoint());
    assert_eq!(active.open_frames().len(), 1);
    assert!(active.caller().activation().graph_namespace().is_root());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    let activation = active
        .checkpoint()
        .activation(NodeId::new("Step_A").unwrap())
        .unwrap();
    let pending = store.load_pending_node_result(&activation).await.unwrap();
    assert_eq!(
        pending.intent().activation().graph_namespace(),
        active.checkpoint().frame().namespace()
    );
    store.close().await;
}

#[tokio::test]
async fn nested_leaf_terminal_returns_before_parent_terminal_handoff() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-return-parent-continuation");
    store
        .register_graph_definition(tenant_id.clone(), fixture.leaf.clone())
        .await
        .unwrap();
    store
        .register_graph_definition(tenant_id.clone(), fixture.driver.graph.clone())
        .await
        .unwrap();
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = result.outcome() else {
        panic!("actual outcome: {:?}", result.outcome());
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    assert_eq!(
        handoff.plan().barrier().base_checkpoint().superstep().get(),
        1
    );
    assert_eq!(
        handoff.plan().barrier().base_checkpoint().graph(),
        &fixture.driver.graph.reference()
    );
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.report().node_attempts_started(), 3);
    assert_eq!(result.report().durable_events(), 11);
    assert_eq!(result.report().whole_replay_passes(), 1);
    assert_eq!(result.report().frame_entries_committed(), 1);
    assert_eq!(result.report().frame_returns_committed(), 1);
    let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_ne!(run.checkpoint(), admitted.run().checkpoint());
    assert_eq!(run.checkpoint().unwrap().superstep().get(), 1);
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(run.lifecycle().result().is_none());
    store.close().await;
}

#[tokio::test]
async fn seven_actual_levels_use_default_stack_and_one_whole_claim_replay() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture_with_depth(7);
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-seven-actual");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(result.report().durable_events(), 47);
    assert_eq!(result.report().node_attempts_started(), 9);
    assert_eq!(result.report().frame_entries_committed(), 7);
    assert_eq!(result.report().frame_returns_committed(), 7);
    assert_eq!(result.report().frame_barriers_committed(), 14);
    assert_eq!(result.report().whole_replay_passes(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    for calls in &fixture.intermediate_calls {
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .load_run(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn higher_fence_rebinds_only_leaf_then_each_resumed_parent() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture_with_depth(2);
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-higher-fence");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let evidence: Arc<dyn GraphLifecycleEvidenceProvider> = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let bounded = |events| {
        DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            events,
            Duration::from_secs(10),
            Duration::from_secs(15 * 60),
            3,
            Duration::from_millis(25),
        )
        .unwrap()
    };
    let first_driver =
        DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), bounded(2))
            .unwrap()
            .with_direct_usage_evidence(Arc::clone(&evidence));
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = first_driver
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(result.report().frame_entries_committed(), 2);
    let before = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.open_frames().len(), 2);
    for frame in before.open_frames() {
        assert_eq!(frame.caller().fence(), first.fence());
    }
    let second_driver =
        DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), bounded(3))
            .unwrap()
            .with_direct_usage_evidence(Arc::clone(&evidence));
    let second = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert_ne!(second.fence(), first.fence());
    let result = second_driver
        .drive(second.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(result.report().frame_callers_rebound(), 1);
    assert_eq!(result.report().frame_entries_committed(), 0);
    assert_eq!(result.report().node_attempts_started(), 1);
    assert_eq!(result.report().whole_replay().frames_validated(), 2);
    let mixed = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mixed.open_frames()[0].caller(),
        before.open_frames()[0].caller()
    );
    assert_eq!(mixed.open_frames()[1].caller().fence(), second.fence());
    assert_ne!(
        mixed.open_frames()[1].caller().attempt_id(),
        before.open_frames()[1].caller().attempt_id()
    );
    assert_eq!(mixed.checkpoint().frame(), before.checkpoint().frame());
    assert_eq!(mixed.run().checkpoint(), admitted.run().checkpoint());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    let final_driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(evidence);
    let third = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = final_driver
        .drive(third.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(result.report().frame_callers_rebound(), 2);
    assert_eq!(result.report().frame_returns_committed(), 2);
    assert_eq!(result.report().whole_replay_passes(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.intermediate_calls[0].load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .load_run(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_failure_closes_all_original_callers_before_root_terminal() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture_with_behavior(2, &TestNodeBehavior::Fail);
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-failure-close");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let evidence: Arc<dyn GraphLifecycleEvidenceProvider> = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::clone(&evidence));
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::Blocked(handoff) = result.outcome() else {
        panic!("actual outcome {:?}", result.outcome());
    };
    assert_eq!(handoff.root_checkpoint(), &admitted.checkpoint().head());
    assert!(handoff.leaf().is_some());
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    let original_callers = active
        .open_frames()
        .iter()
        .map(|frame| frame.caller().clone())
        .collect::<Vec<_>>();
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.driver.registry.clone(),
        evidence,
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let registration = lifecycle
        .resolve_blocked(handoff.as_ref().clone())
        .await
        .unwrap();
    let GraphBarrierLifecycleOutcome::FailureClosing(outcome) = registration else {
        panic!("actual outcome {registration:?}");
    };
    let original_failure = outcome.record().failure().clone();
    let direct = outcome.record().direct_usage().clone();
    let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert!(run.lease().is_none());
    assert!(run.lifecycle().terminal_failure().is_none());
    assert_eq!(run.checkpoint(), admitted.run().checkpoint());
    let closer = stateknot_runtime::DurableRunFailureCloser::new(
        store.clone(),
        fixture.driver.registry.schemas().clone(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let tick = closer
        .tick(tenant_id.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(tick.items().len(), 1);
    assert!(
        tick.items()[0].result().is_ok(),
        "actual full-frame failure completion: {:?}",
        tick.items()[0].result()
    );
    let closure = store
        .load_graph_frame_closure(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closure.failure().id(), original_failure.id());
    assert_eq!(closure.direct_usage(), &direct);
    assert_eq!(closure.completions().len(), original_callers.len());
    for (completion, caller) in closure.completions().iter().zip(&original_callers) {
        assert_eq!(completion.start(), caller);
    }
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(
        run.lifecycle().terminal_failure().unwrap().id(),
        original_failure.id()
    );
    assert_eq!(run.checkpoint(), admitted.run().checkpoint());
    assert!(run.lifecycle().result().is_none());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.intermediate_calls[0].load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_cancellation_closes_original_fences_then_control_plane_confirms_and_recovers_ack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture_with_depth(2);
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-cancel-close");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let evidence: Arc<dyn GraphLifecycleEvidenceProvider> = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        2,
        Duration::from_secs(10),
        Duration::from_secs(15 * 60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), options)
        .unwrap()
        .with_direct_usage_evidence(Arc::clone(&evidence));
    let original = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let yielded = driver
        .drive(original.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        yielded.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    let callers = active
        .open_frames()
        .iter()
        .map(|frame| frame.caller().clone())
        .collect::<Vec<_>>();
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("nested.cancelled").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("The actual nested run was cancelled.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event);
    let request = RunCancellationRequest::new(
        failure.clone(),
        store.observe_database_clock().await.unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(active.run().journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(tenant_id.clone(), ids.run_id(), event, test_payload())
            .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane(
            append,
            RunProjection::transition(
                active.run().lifecycle().revision(),
                RunTransition::RequestCancellation { request },
            ),
        )
        .await
        .unwrap();
    let cancellation = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = driver
        .drive(cancellation.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::CancellationRequested(handoff) = result.outcome() else {
        panic!("actual outcome {:?}", result.outcome());
    };
    assert_eq!(handoff.checkpoint(), &admitted.checkpoint().head());
    assert_eq!(handoff.leaf(), Some(&active.checkpoint().head()));
    assert_eq!(result.report().frame_callers_rebound(), 0);
    assert_eq!(result.report().node_attempts_started(), 0);
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.driver.registry,
        evidence,
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let outcome = lifecycle
        .confirm_cancellation(handoff.as_ref().clone())
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Committed(_))
    ));
    let closure = store
        .load_graph_frame_closure(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closure.failure().id(), failure.id());
    assert_eq!(closure.completions().len(), callers.len());
    for (completion, caller) in closure.completions().iter().zip(&callers) {
        assert_eq!(completion.start(), caller);
    }
    let final_run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(final_run.lifecycle().status(), RunStatus::Cancelled);
    assert!(final_run.lease().is_none());
    assert_eq!(final_run.checkpoint(), admitted.run().checkpoint());
    assert!(final_run.lifecycle().result().is_none());
    assert_eq!(
        final_run.lifecycle().terminal_failure().unwrap().id(),
        failure.id()
    );
    assert_eq!(
        final_run.lifecycle().terminal_usage(),
        Some(closure.direct_usage())
    );
    let final_page = store
        .load_journal_page(
            &tenant_id,
            ids.run_id(),
            Some(&closure.event().head()),
            JournalPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(final_page.events().len(), 1);
    assert_eq!(
        final_page.events()[0].source(),
        &JournalEventSource::ControlPlane
    );
    assert_eq!(final_page.events()[0].event_id(), handoff.event_id());
    assert!(matches!(
        store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await,
        Err(stateknot_store_postgres::StoreError::RunNotRunnable)
    ));
    let retry = lifecycle
        .confirm_cancellation(handoff.as_ref().clone())
        .await
        .unwrap();
    assert!(matches!(
        retry,
        GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Idempotent(_))
    ));
    let after = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(after.journal_head(), final_run.journal_head());
    assert_eq!(
        serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
        serde_json_canonicalizer::to_vec(final_run.lifecycle()).unwrap()
    );
    assert!(after.lease().is_none());
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.intermediate_calls[0].load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_wait_releases_once_resolves_all_conditions_and_resumes_actual_leaf() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let principal = PrincipalIdentity::new(
        "https://issuer.example.com/nested-driver".parse().unwrap(),
        "resumer".parse().unwrap(),
    );
    let ids_wait = [InterruptId::generate(), InterruptId::generate()];
    let waits = NodeWaits::try_new(ids_wait.map(|id| {
        NodeWait::interrupt(
            id,
            RunInterruptKind::Approval,
            test_payload(),
            Digest::sha256(b"actual nested wait"),
            Some(principal.clone()),
            ScopeSet::default(),
            None,
        )
    }))
    .unwrap();
    let fixture = nested_fixture_with_behavior(1, &TestNodeBehavior::Wait(waits));
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-driver-wait-resume");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let evidence: Arc<dyn GraphLifecycleEvidenceProvider> = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(evidence);
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let outcome = driver
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::FrameWaiting(wait) = outcome.outcome() else {
        panic!("actual outcome {:?}", outcome.outcome());
    };
    let (stateknot_store_postgres::GraphFrameBarrierCommitOutcome::Committed(wait)
    | stateknot_store_postgres::GraphFrameBarrierCommitOutcome::Idempotent(wait)) = wait.as_ref()
    else {
        panic!("unsupported scoped barrier outcome");
    };
    assert_eq!(wait.waits().unwrap().unwrap().len(), 2);
    assert_eq!(outcome.report().frame_entries_committed(), 1);
    assert_eq!(outcome.report().frame_barriers_committed(), 1);
    let saved = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.run().lifecycle().status(), RunStatus::Waiting);
    assert!(saved.run().lease().is_none());
    assert_eq!(saved.run().checkpoint(), admitted.run().checkpoint());
    assert_eq!(saved.run().unresolved_wait_count(), 2);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    for (index, id) in ids_wait.into_iter().enumerate() {
        let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
        let request = store
            .load_interrupt_request(&tenant_id, ids.run_id(), id)
            .await
            .unwrap();
        assert_eq!(request.journal(), &wait.event().head());
        let event_id = EventId::generate();
        let intent = InterruptResolutionIntent::new(
            &request,
            event_id,
            test_payload(),
            InterruptResolver::new(principal.clone(), ScopeSet::default()),
        )
        .unwrap();
        let payload = JournalPayload::new(
            test_payload().schema().clone(),
            JournalEventKind::new("nested-wait-resolved").unwrap(),
            BoundedJson::try_from_value(json!({"interrupt_id":id.to_string()})).unwrap(),
        )
        .unwrap();
        let append = JournalAppend::new(
            JournalExpectation::exact(run.journal_head().unwrap().clone()),
            JournalEventIntent::control_plane(tenant_id.clone(), ids.run_id(), event_id, payload)
                .unwrap(),
        )
        .unwrap();
        let actual = store
            .resolve_interrupt(append.clone(), run.lifecycle().revision(), intent.clone())
            .await
            .unwrap();
        let ack = store
            .resolve_interrupt(append, run.lifecycle().revision(), intent)
            .await
            .unwrap();
        assert_eq!(actual.record(), ack.record());
        let current = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
        assert_eq!(current.checkpoint(), admitted.run().checkpoint());
        assert!(current.lease().is_none());
        assert_eq!(
            current.unresolved_wait_count(),
            1 - u8::try_from(index).unwrap()
        );
        if index == 0 {
            assert_eq!(current.lifecycle().status(), RunStatus::Waiting);
            assert!(matches!(
                store
                    .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
                    .await,
                Err(StoreError::RunNotRunnable)
            ));
        } else {
            assert_eq!(current.lifecycle().status(), RunStatus::Active);
        }
        let active = store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.checkpoint().head(), saved.checkpoint().head());
        assert_eq!(active.caller(), saved.caller());
    }
    let second = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(second.fence().epoch() > first.fence().epoch());
    let resumed = driver
        .drive(second.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        resumed.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(resumed.report().whole_replay_passes(), 1);
    assert_eq!(resumed.report().whole_replay().waits_validated(), 1);
    assert_eq!(resumed.report().frame_callers_rebound(), 1);
    assert_eq!(resumed.report().frame_returns_committed(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .load_run(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

fn nested_quantum(quantum: u32) -> DurableGraphDriverOptions {
    DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        quantum,
        Duration::from_secs(10),
        Duration::from_secs(15 * 60),
        3,
        Duration::from_millis(25),
    )
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn shared_quantum_bounds_seven_level_entry_and_higher_fence_rebind() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture_with_depth(7);
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-shared-entry-quantum");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        nested_quantum(2),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let outcome = driver
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        outcome.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    assert_eq!(outcome.report().durable_events(), 2);
    assert_eq!(outcome.report().frame_entries_committed(), 2);
    assert_eq!(outcome.report().node_attempts_started(), 0);
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.open_frames().len(), 2);
    assert!(active.run().lease().is_none());
    assert_eq!(active.run().checkpoint(), admitted.run().checkpoint());
    let callers = active
        .open_frames()
        .iter()
        .map(|frame| frame.caller().clone())
        .collect::<Vec<_>>();
    let second = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let resumed = driver
        .drive(second.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        resumed.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    assert_eq!(resumed.report().durable_events(), 2);
    assert_eq!(resumed.report().frame_entries_committed(), 1);
    assert_eq!(resumed.report().frame_callers_rebound(), 1);
    assert_eq!(resumed.report().node_attempts_started(), 0);
    let final_active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_active.open_frames().len(), 3);
    assert_eq!(final_active.open_frames()[0].caller(), &callers[0]);
    assert_eq!(
        final_active.open_frames()[1].caller().fence(),
        second.fence()
    );
    assert_eq!(final_active.caller().fence(), second.fence());
    assert_eq!(final_active.run().checkpoint(), admitted.run().checkpoint());
    assert!(final_active.run().lease().is_none());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .intermediate_calls
            .iter()
            .all(|calls| calls.load(Ordering::SeqCst) == 0)
    );
    assert!(!final_active.run().is_quarantined());
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn shared_quantum_retains_terminal_leaf_before_return_then_resumes_parent_once() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-shared-return-quantum");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let evidence: Arc<dyn GraphLifecycleEvidenceProvider> = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    for quantum in [3, 2, 3, 2] {
        let driver = DurableGraphDriver::new(
            store.clone(),
            fixture.driver.registry.clone(),
            nested_quantum(quantum),
        )
        .unwrap()
        .with_direct_usage_evidence(Arc::clone(&evidence));
        let lease = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        let outcome = driver
            .drive(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap();
        assert!(matches!(
            outcome.outcome(),
            GraphDriveOutcome::Yielded { .. }
        ));
        assert_eq!(outcome.report().durable_events(), quantum);
        assert_eq!(outcome.report().frame_returns_committed(), 0);
        let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
        assert_eq!(run.checkpoint(), admitted.run().checkpoint());
        assert!(run.lease().is_none());
    }
    let terminal = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert!(terminal.checkpoint().checkpoint().ready_nodes().is_empty());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(evidence);
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let outcome = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        outcome.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(outcome.report().frame_returns_committed(), 1);
    assert_eq!(outcome.report().frame_callers_rebound(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .load_run(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_start_lost_commit_ack_retries_exact_original_without_application_launch() {
    use crate::commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target};
    use sqlx_core::connection::ConnectOptions;
    use sqlx_postgres::PgConnectOptions;
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let backend = loopback_target(&connection);
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-start-commit-ack");
    for graph in &fixture.graphs {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        ))
        .await
        .unwrap()
        .stored()
        .clone();
    let ledger = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let initial = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        nested_quantum(2),
    )
    .unwrap()
    .with_direct_usage_evidence(ledger.clone());
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let outcome = initial
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        outcome.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    assert_eq!(outcome.report().frame_entries_committed(), 1);
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let usage = ledger
        .usage_at(
            &tenant_id,
            ids.run_id(),
            active.run().journal_head().unwrap(),
        )
        .await
        .unwrap();
    let rebound = store
        .rebind_graph_frame_caller(
            active.checkpoint().frame(),
            AttemptId::generate(),
            EventId::generate(),
            lease.fence().clone(),
            active.run().journal_head().unwrap().clone(),
            usage,
        )
        .await
        .unwrap();
    let (source, caller) = match rebound {
        NodeAttemptCommitOutcome::Committed { event, attempt } => {
            (event.head(), attempt.start().head())
        }
        _ => panic!("fresh actual caller rebind"),
    };
    let mut proxy = CommitProxy::start(
        &connection,
        Cut::CommitResponse,
        TransactionTarget::GraphNodeStart,
    )
    .await;
    let proxied = PostgresStore::connect(
        connection
            .clone()
            .host("127.0.0.1")
            .port(proxy.port())
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    let driver = DurableGraphDriver::new(
        proxied.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(ledger);
    let fence = lease.fence().clone();
    let driving =
        tokio::spawn(async move { driver.drive(fence, CancellationSignal::never()).await });
    proxy.wait_cut(Cut::CommitResponse).await;
    let durable_page = store
        .load_journal_page(
            &tenant_id,
            ids.run_id(),
            Some(&source),
            JournalPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(durable_page.events().len(), 1);
    assert!(!durable_page.has_more());
    let event = &durable_page.events()[0];
    assert_eq!(
        event.payload().kind().as_str(),
        "graph-node-attempt-started"
    );
    assert_eq!(event.source().worker_fence(), Some(lease.fence()));
    let id: AttemptId = event.payload().data().as_value()["attempt_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let original = store
        .load_node_attempt(&tenant_id, &ids.run_id(), id)
        .await
        .unwrap();
    assert_eq!(
        original.status(),
        stateknot_core::NodeAttemptStatus::Executing
    );
    assert_eq!(original.start().journal_head(), &event.head());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    let port = proxy.port();
    proxy.stop().await;
    // This single replacement session forwards plaintext loopback bytes only.
    // It enables the real bounded Driver retry after the first durable COMMIT
    // response was withheld; no protocol data or credentials are logged.
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    let reconnect = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await.unwrap();
        let mut server = tokio::net::TcpStream::connect(backend).await.unwrap();
        tokio::io::copy_bidirectional(&mut client, &mut server).await
    });
    let recovered = tokio::time::timeout(Duration::from_secs(60), driving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let GraphDriveOutcome::Blocked(handoff) = recovered.outcome() else {
        panic!("actual outcome {:?}", recovered.outcome());
    };
    assert_eq!(handoff.blockers().in_flight(), 1);
    assert_eq!(recovered.report().durable_events(), 1);
    assert_eq!(recovered.report().node_attempts_started(), 0);
    assert_eq!(recovered.report().node_attempts_completed(), 0);
    assert_eq!(recovered.report().node_batches_started(), 0);
    assert!(recovered.report().mutation_retries() >= 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    let after = store
        .load_node_attempt(&tenant_id, &ids.run_id(), id)
        .await
        .unwrap();
    assert_eq!(after.start().head(), original.start().head());
    assert!(after.completion().is_none());
    let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(run.journal_head(), Some(&event.head()));
    assert_eq!(run.checkpoint(), admitted.run().checkpoint());
    assert!(!run.is_quarantined());
    let leaf = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leaf.caller().head(), caller);
    assert_eq!(leaf.checkpoint().head(), active.checkpoint().head());
    proxied.close().await;
    reconnect.abort();
    let _ = reconnect.await;
    store.close().await;
}

#[tokio::test]
async fn serial_siblings_finish_in_distinct_namespaces_before_root_terminal() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = serial_sibling_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-serial-siblings");
    admit_nested_fixture(&store, &fixture, &tenant_id, ids).await;
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = result.outcome() else {
        panic!("actual outcome {:?}", result.outcome());
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    assert_eq!(
        handoff.plan().barrier().base_checkpoint().superstep().get(),
        2
    );
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.report().node_attempts_started(), 5);
    assert_eq!(result.report().frame_entries_committed(), 2);
    assert_eq!(result.report().frame_returns_committed(), 2);
    assert_eq!(result.report().frame_barriers_committed(), 4);
    assert_eq!(result.report().whole_replay_passes(), 1);
    let page = store
        .load_journal_page(
            &tenant_id,
            ids.run_id(),
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    let frames: Vec<stateknot_core::GraphFrameIdentity> = page
        .events()
        .iter()
        .filter(|event| event.payload().kind().as_str() == "graph-frame-entered")
        .map(|event| {
            serde_json::from_value(event.payload().data().as_value()["frame"].clone()).unwrap()
        })
        .collect();
    assert_eq!(frames.len(), 2);
    assert_ne!(frames[0].namespace(), frames[1].namespace());
    for frame in &frames {
        let entry = store
            .load_graph_frame_entry(&tenant_id, ids.run_id(), frame.namespace())
            .await
            .unwrap();
        assert_eq!(entry.entry().checkpoint().frame(), frame);
        let returned = store
            .load_graph_frame_return(&tenant_id, ids.run_id(), frame.namespace())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(returned.terminal_checkpoint().frame(), frame);
    }
    let run = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(run.checkpoint().unwrap().superstep().get(), 2);
    assert!(!run.is_quarantined());
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_safe_after_releases_exact_gate_and_retries_under_higher_fence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let delay = DurationMillis::new(20_000).unwrap();
    let fixture = nested_fixture_with_retry(1, &TestNodeBehavior::Continue, Some(delay));
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-scoped-safe-after");
    admit_nested_fixture(&store, &fixture, &tenant_id, ids).await;
    let admitted = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let deferred = driver
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::Deferred {
        not_before,
        schedule,
    } = deferred.outcome()
    else {
        panic!("actual outcome {:?}", deferred.outcome());
    };
    assert!(matches!(
        schedule,
        stateknot_store_postgres::DelayedRetryScheduleOutcome::Scheduled { .. }
    ));
    assert_eq!(schedule.not_before(), *not_before);
    assert_eq!(deferred.report().frame_entries_committed(), 1);
    assert_eq!(deferred.report().node_attempts_started(), 1);
    assert_eq!(deferred.report().node_attempts_completed(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    let gated = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert!(gated.lease().is_none());
    assert_eq!(gated.scheduler_not_before(), Some(*not_before));
    assert_eq!(gated.scheduler_ready_at(), admitted.scheduler_ready_at());
    assert_eq!(gated.checkpoint(), admitted.checkpoint());
    assert!(!gated.is_quarantined());
    let leaf = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leaf.caller().fence(), first.fence());
    let page = store
        .load_journal_page(
            &tenant_id,
            ids.run_id(),
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    let failed_event = page
        .events()
        .iter()
        .find(|event| event.payload().kind().as_str() == "graph-node-attempt-failed")
        .unwrap();
    let failed_id: AttemptId = failed_event.payload().data().as_value()["attempt_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let failed = store
        .load_node_attempt(&tenant_id, &ids.run_id(), failed_id)
        .await
        .unwrap();
    let completion = failed.completion().unwrap();
    assert_eq!(completion.journal_head(), &failed_event.head());
    assert_eq!(
        completion.outcome().failure().unwrap().retry_advice(),
        RetryAdvice::SafeAfter { delay }
    );
    assert_eq!(
        not_before.unix_micros(),
        failed_event.recorded_at().unix_micros() + delay.as_i64() * 1000
    );
    assert!(store.observe_database_clock().await.unwrap() < *not_before);
    assert!(matches!(
        store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotYetAvailable)
    ));
    let until = Instant::now() + Duration::from_secs(90);
    while store.observe_database_clock().await.unwrap() < *not_before {
        assert!(
            Instant::now() < until,
            "actual DB clock did not reach the original retry boundary"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let second = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(second.fence().epoch() > first.fence().epoch());
    let before = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(before.journal_head(), gated.journal_head());
    assert_eq!(before.checkpoint(), gated.checkpoint());
    assert_eq!(before.scheduler_ready_at(), gated.scheduler_ready_at());
    assert!(before.scheduler_not_before().is_none());
    let resumed = driver
        .drive(second.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        resumed.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(resumed.report().frame_callers_rebound(), 1);
    assert_eq!(resumed.report().frame_returns_committed(), 1);
    assert_eq!(resumed.report().whole_replay_passes(), 1);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    let all = store
        .load_journal_page(
            &tenant_id,
            ids.run_id(),
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    let ordinary: Vec<_> = all
        .events()
        .iter()
        .filter(|event| event.payload().kind().as_str() == "graph-node-attempt-started")
        .collect();
    let mut logical = Vec::new();
    for event in ordinary {
        let id = event.payload().data().as_value()["attempt_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let attempt = store
            .load_node_attempt(&tenant_id, &ids.run_id(), id)
            .await
            .unwrap();
        if attempt.start().activation().node_id().as_str() == "Step_A" {
            logical.push(attempt);
        }
    }
    assert_eq!(logical.len(), 2);
    assert_eq!(
        logical[0].start().activation(),
        logical[1].start().activation()
    );
    assert_eq!(logical[0].start().fence(), first.fence());
    assert_eq!(logical[1].start().fence(), second.fence());
    assert_ne!(
        logical[0].start().attempt_id(),
        logical[1].start().attempt_id()
    );
    assert_eq!(
        logical[0].completion().unwrap().digest(),
        completion.digest()
    );
    assert_eq!(
        logical[1].status(),
        stateknot_core::NodeAttemptStatus::Succeeded
    );
    assert!(
        !store
            .load_run(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    assert!(
        store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .is_none()
    );
    store.close().await;
}

#[derive(Clone)]
struct DirectShutdown {
    requested: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl stateknot_core::CancellationObserver for DirectShutdown {
    fn is_cancelled(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            loop {
                let notified = self.notify.notified();
                if self.is_cancelled() {
                    return;
                }
                notified.await;
            }
        })
    }
}

struct PausedDirectEvidence {
    ledger: AdmissionDirectEvidence,
    calls: AtomicUsize,
    waiting: Arc<tokio::sync::Notify>,
    pending_call: Option<usize>,
    wait_until: Option<Timestamp>,
}

impl GraphLifecycleEvidenceProvider for PausedDirectEvidence {
    fn direct_usage_evidence(
        &self,
        context: stateknot_runtime::GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<
        '_,
        Result<stateknot_runtime::GraphDirectUsageEvidence, GraphLifecycleEvidenceError>,
    > {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.pending_call == Some(call) {
                self.waiting.notify_one();
                return future::pending().await;
            }
            if let Some(boundary) = self.wait_until {
                self.waiting.notify_one();
                loop {
                    let observed = self
                        .ledger
                        .store
                        .observe_database_clock()
                        .await
                        .map_err(|_| GraphLifecycleEvidenceError::TemporarilyUnavailable)?;
                    if observed >= boundary {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            self.ledger.direct_usage_evidence(context).await
        })
    }

    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.terminal_evidence(context)
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.failure_evidence(context)
    }

    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.cancellation_evidence(context)
    }
}

#[derive(Clone, Copy)]
enum PendingDirectStop {
    Shutdown,
    Timeout,
    DurableCancellation,
}

#[tokio::test]
async fn pending_direct_usage_observes_shutdown_preserves_actual_completed_work() {
    assert_pending_direct_stop(PendingDirectStop::Shutdown).await;
}

#[tokio::test]
async fn pending_direct_usage_observes_original_node_timeout_without_fabricating_usage() {
    assert_pending_direct_stop(PendingDirectStop::Timeout).await;
}

#[tokio::test]
async fn pending_direct_usage_observes_durable_cancel_preserving_actual_completed_work() {
    assert_pending_direct_stop(PendingDirectStop::DurableCancellation).await;
}

fn assert_pending_direct_stop(stop: PendingDirectStop) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let fixture = nested_fixture();
        let ids = AgentRunIds::generate();
        let tenant_id = tenant("nested-direct-shutdown");
        admit_nested_fixture(&store, &fixture, &tenant_id, ids).await;
        let waiting = Arc::new(tokio::sync::Notify::new());
        let driver = pending_direct_driver(&store, &fixture, Arc::clone(&waiting), stop);
        let original = store
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
        let fence = original.fence().clone();
        let mut task = tokio::spawn(async move { driver.drive(fence, signal).await });
        tokio::time::timeout(Duration::from_secs(60), waiting.notified())
            .await
            .expect("actual completed work must reach the second DIRECT observation");
        let active = store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .unwrap();
        let activation = active
            .checkpoint()
            .activation(NodeId::new("Step_A").unwrap())
            .unwrap();
        let completed = store.load_pending_node_result(&activation).await.unwrap();
        let before = active.run().clone();
        assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
        let expected = match stop {
            PendingDirectStop::DurableCancellation => {
                request_direct_cancellation(
                    &store,
                    &before,
                    "nested.direct.cancel.control",
                    "Cancellation preserves already-durable accounting.",
                )
                .await;
                store.load_run(&tenant_id, ids.run_id()).await.unwrap()
            }
            PendingDirectStop::Shutdown => {
                shutdown.requested.store(true, Ordering::Release);
                shutdown.notify.notify_waiters();
                before.clone()
            }
            PendingDirectStop::Timeout => before.clone(),
        };
        let result = tokio::time::timeout(
            match stop {
                PendingDirectStop::Shutdown => Duration::from_secs(2),
                _ => Duration::from_secs(4),
            },
            &mut task,
        )
        .await
        .expect("pending read-only DIRECT observation must respect its original control bound")
        .unwrap();
        match stop {
            PendingDirectStop::Timeout => assert!(matches!(
                result,
                Err(GraphDriverError::DirectUsageEvidence {
                    source: GraphLifecycleEvidenceError::TemporarilyUnavailable,
                })
            )),
            PendingDirectStop::Shutdown => assert!(matches!(
                result.unwrap().outcome(),
                GraphDriveOutcome::Cancelled { .. }
            )),
            PendingDirectStop::DurableCancellation => assert!(matches!(
                result.unwrap().outcome(),
                GraphDriveOutcome::CancellationRequested(_)
            )),
        }
        assert_completed_direct_stop_preserved(
            &store,
            &fixture,
            &active,
            &completed,
            &expected,
            original.fence(),
            stop,
        )
        .await;
        store.close().await;
    })
}

fn pending_direct_driver(
    store: &PostgresStore,
    fixture: &NestedFixture,
    waiting: Arc<tokio::sync::Notify>,
    stop: PendingDirectStop,
) -> DurableGraphDriver {
    let evidence = Arc::new(PausedDirectEvidence {
        ledger: AdmissionDirectEvidence {
            store: store.clone(),
        },
        calls: AtomicUsize::new(0),
        waiting,
        pending_call: Some(2),
        wait_until: None,
    });
    DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        match stop {
            PendingDirectStop::Timeout => DurableGraphDriverOptions::new(
                GraphReplayLimits::default(),
                1024,
                Duration::from_secs(10),
                Duration::from_secs(2),
                3,
                Duration::from_millis(25),
            )
            .unwrap(),
            _ => DurableGraphDriverOptions::default(),
        },
    )
    .unwrap()
    .with_direct_usage_evidence(evidence)
}

fn assert_completed_direct_stop_preserved<'a>(
    store: &'a PostgresStore,
    fixture: &'a NestedFixture,
    active: &'a stateknot_store_postgres::StoredActiveGraphFrame,
    completed: &'a stateknot_core::PendingNodeResult,
    expected: &'a stateknot_store_postgres::StoredRun,
    fence: &'a RunFence,
    stop: PendingDirectStop,
) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let provenance = active.run().lifecycle().provenance();
        let tenant_id = provenance.tenant_id();
        let run_id = provenance.run_id();
        let after = store.load_run(tenant_id, run_id).await.unwrap();
        assert!(!after.is_quarantined());
        match stop {
            PendingDirectStop::DurableCancellation => {
                assert_eq!(after.lease().unwrap().fence(), fence);
            }
            _ => assert!(after.lease().is_none()),
        }
        assert_eq!(after.journal_head(), expected.journal_head());
        assert_eq!(after.checkpoint(), expected.checkpoint());
        assert_eq!(
            serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
            serde_json_canonicalizer::to_vec(expected.lifecycle()).unwrap()
        );
        let still_active = store
            .load_active_graph_frame(tenant_id, run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(still_active.checkpoint().head(), active.checkpoint().head());
        let still_completed = store
            .load_pending_node_result(completed.intent().activation())
            .await
            .unwrap();
        assert_eq!(&still_completed, completed);
        assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    })
}

#[tokio::test]
async fn slow_cancellation_direct_evidence_maintains_original_fence_past_original_lease() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = nested_fixture_with_depth(2);
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("nested-direct-cancel-slow");
    let admitted_deadline =
        admit_cancellation_deadline_fixture(&store, &fixture, &tenant_id, ids).await;
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        2,
        Duration::from_millis(100),
        Duration::from_secs(15 * 60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), options)
        .unwrap()
        .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
            store: store.clone(),
        }));
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(matches!(
        driver
            .drive(first.fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    let failure_id = request_direct_cancellation(
        &store,
        active.run(),
        "nested.direct.cancel",
        "Cancellation retains complete delayed accounting.",
    )
    .await;
    let cancellation = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let cancellation_result = driver
        .drive(cancellation.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::CancellationRequested(handoff) = cancellation_result.outcome() else {
        panic!("actual outcome {:?}", cancellation_result.outcome());
    };
    let observed = store
        .observe_live_lease(cancellation.fence())
        .await
        .unwrap();
    let original_expiry = observed.lease().expires_at();
    let outcome = confirm_slow_direct_cancellation(
        &store,
        &fixture,
        handoff,
        original_expiry.max(admitted_deadline),
    )
    .await;
    assert!(matches!(
        outcome,
        GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Committed(_))
    ));
    assert!(store.observe_database_clock().await.unwrap() >= original_expiry);
    assert!(store.observe_database_clock().await.unwrap() >= admitted_deadline);
    assert_eq!(
        store
            .load_agent_admission(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .admission()
            .intent()
            .budget()
            .deadline(),
        admitted_deadline
    );
    assert_complete_scoped_cancellation(&store, &fixture, &active, failure_id).await;
    store.close().await;
}

fn confirm_slow_direct_cancellation<'a>(
    store: &'a PostgresStore,
    fixture: &'a NestedFixture,
    handoff: &'a stateknot_runtime::GraphCancellationHandoff,
    boundary: Timestamp,
) -> BoxFuture<'a, GraphBarrierLifecycleOutcome> {
    Box::pin(async move {
        let lifecycle = DurableGraphLifecycle::new(
            store.clone(),
            fixture.driver.registry.clone(),
            Arc::new(PausedDirectEvidence {
                ledger: AdmissionDirectEvidence {
                    store: store.clone(),
                },
                calls: AtomicUsize::new(0),
                waiting: Arc::new(tokio::sync::Notify::new()),
                pending_call: None,
                wait_until: Some(boundary),
            }),
            DurableGraphLifecycleOptions::default(),
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(20),
            lifecycle.confirm_cancellation(handoff.clone()),
        )
        .await
        .expect("bounded slow cancellation observation")
        .unwrap()
    })
}

fn admit_cancellation_deadline_fixture<'a>(
    store: &'a PostgresStore,
    fixture: &'a NestedFixture,
    tenant_id: &'a TenantId,
    ids: AgentRunIds,
) -> BoxFuture<'a, Timestamp> {
    Box::pin(async move {
        for graph in &fixture.graphs {
            store
                .register_graph_definition(tenant_id.clone(), graph.clone())
                .await
                .unwrap();
        }
        let admitted_deadline = Timestamp::from_unix_micros(
            store.observe_database_clock().await.unwrap().unix_micros() + 8_000_000,
        )
        .unwrap();
        let base = durable_admission_request(
            &fixture.driver,
            tenant_id.clone(),
            ids,
            fixture.driver.graph.output_schema().clone(),
            fixture.driver.graph.input_schema().clone(),
        );
        let intent = base.intent();
        let request = DurableAgentAdmissionRequest::new(
            tenant_id.clone(),
            ids,
            intent.descriptor().clone(),
            AgentRequest::new(
                intent.request().input_schema().clone(),
                intent.request().input().clone(),
                intent
                    .request()
                    .budget_limits()
                    .clone()
                    .with_deadline(admitted_deadline),
            ),
            intent.budget_layers().to_vec(),
            intent.graph().clone(),
            intent.authority().clone(),
            base.initial_state().clone(),
        )
        .unwrap();
        DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone())
            .unwrap()
            .admit(request)
            .await
            .unwrap();
        admitted_deadline
    })
}

fn assert_complete_scoped_cancellation<'a>(
    store: &'a PostgresStore,
    fixture: &'a NestedFixture,
    active: &'a stateknot_store_postgres::StoredActiveGraphFrame,
    failure_id: FailureId,
) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let provenance = active.run().lifecycle().provenance();
        let tenant_id = provenance.tenant_id();
        let run_id = provenance.run_id();
        let closure = store
            .load_graph_frame_closure(tenant_id, run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closure.failure().id(), failure_id);
        assert_eq!(closure.completions().len(), active.open_frames().len());
        let final_run = store.load_run(tenant_id, run_id).await.unwrap();
        assert_eq!(final_run.lifecycle().status(), RunStatus::Cancelled);
        assert!(final_run.lease().is_none());
        assert!(!final_run.is_quarantined());
        assert_eq!(
            final_run.lifecycle().terminal_usage(),
            Some(closure.direct_usage())
        );
        assert_eq!(
            final_run.lifecycle().terminal_failure().unwrap().id(),
            failure_id
        );
        assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    })
}

async fn request_direct_cancellation(
    store: &PostgresStore,
    run: &stateknot_store_postgres::StoredRun,
    code: &str,
    message: &str,
) -> FailureId {
    let provenance = run.lifecycle().provenance();
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new(code).unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new(message).unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event);
    let failure_id = failure.id();
    let request =
        RunCancellationRequest::new(failure, store.observe_database_clock().await.unwrap())
            .unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(run.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    provenance.tenant_id().clone(),
                    provenance.run_id(),
                    event,
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                run.lifecycle().revision(),
                RunTransition::RequestCancellation { request },
            ),
        )
        .await
        .unwrap();
    failure_id
}

#[tokio::test]
async fn agent_loop_pending_cancellation_direct_observes_shutdown_without_closing_incomplete_usage()
{
    assert_cancellation_direct_control(true).await;
}

#[tokio::test]
async fn agent_loop_pending_cancellation_direct_observes_original_node_timeout_then_retries_complete_usage()
 {
    assert_cancellation_direct_control(false).await;
}

fn assert_cancellation_direct_control(shutdown_requested: bool) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let fixture = nested_fixture();
        let tenant_id = tenant("nested-direct-lifecycle-control");
        let ids = AgentRunIds::generate();
        admit_nested_fixture(&store, &fixture, &tenant_id, ids).await;
        enter_cancellation_fixture(&store, &fixture, &tenant_id, ids).await;
        let active = store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .unwrap();
        request_direct_cancellation(
            &store,
            active.run(),
            "nested.direct.cancel.control",
            "Cancellation preserves already-durable accounting.",
        )
        .await;
        let lease = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        let before = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
        let failure_id = before
            .lifecycle()
            .cancellation_request()
            .unwrap()
            .failure()
            .id();
        let waiting = Arc::new(tokio::sync::Notify::new());
        let agent_loop = pending_cancellation_agent_loop(
            &store,
            &fixture,
            Arc::clone(&waiting),
            shutdown_requested,
        );
        let shutdown = DirectShutdown {
            requested: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        };
        let signal = CancellationSignal::new(shutdown.clone());
        let loop_task = agent_loop.clone();
        let fence = lease.fence().clone();
        let mut task = tokio::spawn(async move { loop_task.run(fence, signal).await });
        tokio::time::timeout(Duration::from_secs(30), waiting.notified())
            .await
            .expect("scoped cancellation DIRECT observation reached");
        if shutdown_requested {
            shutdown.requested.store(true, Ordering::Release);
            shutdown.notify.notify_waiters();
        }
        let result = tokio::time::timeout(
            if shutdown_requested {
                Duration::from_secs(2)
            } else {
                Duration::from_secs(4)
            },
            &mut task,
        )
        .await
        .expect("scoped lifecycle DIRECT control bound")
        .unwrap();
        if shutdown_requested {
            assert!(matches!(
                result.unwrap().outcome(),
                AgentLoopOutcome::RecoveryReleased(_)
            ));
        } else {
            assert!(
                matches!(result, Err(AgentLoopError::Lifecycle { source }) if matches!(*source,
            stateknot_runtime::GraphLifecycleError::Evidence(GraphLifecycleEvidenceError::TemporarilyUnavailable)))
            );
        }
        assert_scoped_cancellation_pending(&store, &before, &active).await;
        let retry_lease = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        assert!(retry_lease.fence().epoch() > lease.fence().epoch());
        let retried = agent_loop
            .run(retry_lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap();
        assert!(matches!(
            retried.outcome(),
            AgentLoopOutcome::CancellationConfirmed(AppendOutcome::Committed(_))
        ));
        assert_complete_scoped_cancellation(&store, &fixture, &active, failure_id).await;
        store.close().await;
    })
}

fn pending_cancellation_agent_loop(
    store: &PostgresStore,
    fixture: &NestedFixture,
    waiting: Arc<tokio::sync::Notify>,
    shutdown_requested: bool,
) -> DurableAgentLoop {
    let evidence = Arc::new(PausedDirectEvidence {
        ledger: AdmissionDirectEvidence {
            store: store.clone(),
        },
        calls: AtomicUsize::new(0),
        waiting,
        pending_call: Some(1),
        wait_until: None,
    });
    let timeout = if shutdown_requested {
        Duration::from_secs(15 * 60)
    } else {
        Duration::from_secs(2)
    };
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        1024,
        Duration::from_secs(10),
        timeout,
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    DurableAgentLoop::new(
        store.clone(),
        fixture.driver.registry.clone(),
        evidence,
        options,
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}

fn enter_cancellation_fixture<'a>(
    store: &'a PostgresStore,
    fixture: &'a NestedFixture,
    tenant_id: &'a TenantId,
    ids: AgentRunIds,
) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let driver = DurableGraphDriver::new(
            store.clone(),
            fixture.driver.registry.clone(),
            DurableGraphDriverOptions::new(
                GraphReplayLimits::default(),
                2,
                Duration::from_secs(10),
                Duration::from_secs(15 * 60),
                3,
                Duration::from_millis(25),
            )
            .unwrap(),
        )
        .unwrap()
        .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
            store: store.clone(),
        }));
        let original = store
            .claim_lease(tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        assert!(matches!(
            driver
                .drive(original.fence().clone(), CancellationSignal::never())
                .await
                .unwrap()
                .outcome(),
            GraphDriveOutcome::Yielded { .. }
        ));
    })
}

fn assert_scoped_cancellation_pending<'a>(
    store: &'a PostgresStore,
    before: &'a stateknot_store_postgres::StoredRun,
    active: &'a stateknot_store_postgres::StoredActiveGraphFrame,
) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let provenance = before.lifecycle().provenance();
        let tenant_id = provenance.tenant_id();
        let run_id = provenance.run_id();
        let after = store.load_run(tenant_id, run_id).await.unwrap();
        assert!(after.lease().is_none());
        assert!(!after.is_quarantined());
        assert_eq!(after.journal_head(), before.journal_head());
        assert_eq!(after.checkpoint(), before.checkpoint());
        assert_eq!(
            serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
            serde_json_canonicalizer::to_vec(before.lifecycle()).unwrap()
        );
        assert!(
            store
                .load_graph_frame_closure(tenant_id, run_id)
                .await
                .unwrap()
                .is_none()
        );
        let still_active = store
            .load_active_graph_frame(tenant_id, run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(still_active.checkpoint().head(), active.checkpoint().head());
    })
}

struct CancellationCommitFixture {
    nested: NestedFixture,
    active: stateknot_store_postgres::StoredActiveGraphFrame,
    handoff: stateknot_runtime::GraphCancellationHandoff,
    failure_id: FailureId,
}

fn cancellation_commit_fixture(store: &PostgresStore) -> BoxFuture<'_, CancellationCommitFixture> {
    Box::pin(async move {
        let nested = nested_fixture();
        let ids = AgentRunIds::generate();
        let tenant_id = tenant("nested-cancel-commit-guardian");
        admit_nested_fixture(store, &nested, &tenant_id, ids).await;
        let driver = cancellation_fixture_driver(store, &nested);
        let first = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        assert!(matches!(
            driver
                .drive(first.fence().clone(), CancellationSignal::never())
                .await
                .unwrap()
                .outcome(),
            GraphDriveOutcome::Yielded { .. }
        ));
        let active = store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .unwrap();
        let failure_id = request_direct_cancellation(
            store,
            active.run(),
            "nested.cancel.commit.guardian",
            "Retain complete cancellation through commit response faults.",
        )
        .await;
        let lease = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        let stopped = driver
            .drive(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap();
        let GraphDriveOutcome::CancellationRequested(handoff) = stopped.outcome() else {
            panic!("actual outcome {:?}", stopped.outcome());
        };
        CancellationCommitFixture {
            nested,
            active,
            handoff: handoff.as_ref().clone(),
            failure_id,
        }
    })
}

fn cancellation_fixture_driver(
    store: &PostgresStore,
    fixture: &NestedFixture,
) -> DurableGraphDriver {
    DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
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
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }))
}

fn cancellation_fault_lifecycle(
    store: &PostgresStore,
    fixture: &CancellationCommitFixture,
    evidence: Arc<PausedDirectEvidence>,
) -> DurableGraphLifecycle {
    DurableGraphLifecycle::new(
        store.clone(),
        fixture.nested.driver.registry.clone(),
        evidence,
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}

fn wait_database_boundary(store: &PostgresStore, boundary: Timestamp) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if store.observe_database_clock().await.unwrap() >= boundary {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("actual database clock reaches the original boundary");
    })
}

struct CancellationAckProbe {
    proxy: super::commit_proxy::CommitProxy,
    proxied: PostgresStore,
    lifecycle: DurableGraphLifecycle,
    evidence: Arc<PausedDirectEvidence>,
    closing: tokio::task::JoinHandle<
        Result<GraphBarrierLifecycleOutcome, stateknot_runtime::GraphLifecycleError>,
    >,
    backend: std::net::SocketAddr,
}

fn cancellation_ack_probe<'a>(
    store: &'a PostgresStore,
    fixture: &'a CancellationCommitFixture,
    target: super::commit_proxy::TransactionTarget,
    boundary: Option<Timestamp>,
) -> BoxFuture<'a, CancellationAckProbe> {
    Box::pin(async move {
        use super::commit_proxy::{CommitProxy, Cut, loopback_target};
        let connection: sqlx_postgres::PgConnectOptions =
            std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
        let backend = loopback_target(&connection);
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
        let evidence = Arc::new(PausedDirectEvidence {
            ledger: AdmissionDirectEvidence {
                store: store.clone(),
            },
            calls: AtomicUsize::new(0),
            waiting: Arc::new(tokio::sync::Notify::new()),
            pending_call: None,
            wait_until: boundary,
        });
        let lifecycle = cancellation_fault_lifecycle(&proxied, fixture, Arc::clone(&evidence));
        let confirming = lifecycle.clone();
        let original = fixture.handoff.clone();
        let closing = tokio::spawn(async move { confirming.confirm_cancellation(original).await });
        CancellationAckProbe {
            proxy,
            proxied,
            lifecycle,
            evidence,
            closing,
            backend,
        }
    })
}

#[tokio::test]
async fn scoped_cancellation_committed_closure_ack_survives_guardian_fault_then_recovers_exactly() {
    use super::commit_proxy::{Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = cancellation_commit_fixture(&store).await;
    let mut probe =
        cancellation_ack_probe(&store, &fixture, TransactionTarget::GraphFrameClosure, None).await;
    probe.proxy.wait_cut(Cut::CommitResponse).await;
    let fence = fixture.handoff.lease().fence();
    let closure = store
        .load_graph_frame_closure(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .unwrap();
    let committed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(committed.journal_head(), Some(&closure.event().head()));
    assert!(committed.lease().is_none());
    assert_eq!(
        committed.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    tokio::time::sleep(Duration::from_secs(6)).await; // Original acquire timeout is five seconds.
    assert!(
        !probe.closing.is_finished(),
        "guardian discarded committed closure ACK: {:?}",
        (&mut probe.closing).await.unwrap()
    );
    finish_closure_ack_probe(&store, &fixture, probe, closure.event().head()).await;
    store.close().await;
}

fn finish_closure_ack_probe<'a>(
    store: &'a PostgresStore,
    fixture: &'a CancellationCommitFixture,
    mut probe: CancellationAckProbe,
    closure_head: stateknot_core::JournalHead,
) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let fence = fixture.handoff.lease().fence();
        let port = probe.proxy.port();
        probe.proxy.stop().await;
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        let reconnect = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.unwrap();
            let mut server = tokio::net::TcpStream::connect(probe.backend).await.unwrap();
            tokio::io::copy_bidirectional(&mut client, &mut server).await
        });
        let outcome = tokio::time::timeout(Duration::from_secs(30), probe.closing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            outcome,
            GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Committed(_))
        ));
        assert_eq!(probe.evidence.calls.load(Ordering::SeqCst), 1);
        let terminal = store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap();
        let again = probe
            .lifecycle
            .confirm_cancellation(fixture.handoff.clone())
            .await
            .unwrap();
        assert!(matches!(
            again,
            GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Idempotent(_))
        ));
        assert_eq!(probe.evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .load_graph_frame_closure(fence.tenant_id(), fence.run_id())
                .await
                .unwrap()
                .unwrap()
                .event()
                .head(),
            closure_head
        );
        assert_eq!(
            store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .unwrap()
                .journal_head(),
            terminal.journal_head()
        );
        assert_complete_scoped_cancellation(
            store,
            &fixture.nested,
            &fixture.active,
            fixture.failure_id,
        )
        .await;
        probe.proxied.close().await;
        reconnect.abort();
        let _ = reconnect.await;
    })
}

#[tokio::test]
async fn scoped_cancellation_renewal_commit_ack_outlives_original_expiry_without_dropping_sql() {
    use super::commit_proxy::{Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = cancellation_commit_fixture(&store).await;
    let fence = fixture.handoff.lease().fence();
    let original_expiry = store
        .observe_live_lease(fence)
        .await
        .unwrap()
        .lease()
        .expires_at();
    let evidence_boundary =
        Timestamp::from_unix_micros(original_expiry.unix_micros() + 300_000).unwrap();
    let mut probe = cancellation_ack_probe(
        &store,
        &fixture,
        TransactionTarget::LeaseRenewal,
        Some(evidence_boundary),
    )
    .await;
    probe.proxy.wait_cut(Cut::CommitResponse).await;
    let renewed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(renewed.lease().unwrap().fence(), fence);
    assert!(renewed.lease().unwrap().expires_at() > original_expiry);
    assert_eq!(renewed.journal_head(), Some(fixture.handoff.journal_head()));
    wait_database_boundary(
        &store,
        Timestamp::from_unix_micros(original_expiry.unix_micros() + 100_000).unwrap(),
    )
    .await;
    assert!(
        !probe.closing.is_finished(),
        "original expiry discarded committed renewal ACK: {:?}",
        (&mut probe.closing).await.unwrap()
    );
    probe.proxy.resume_ack();
    let outcome = tokio::time::timeout(Duration::from_secs(30), probe.closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome,
        GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Committed(_))
    ));
    assert_complete_scoped_cancellation(
        &store,
        &fixture.nested,
        &fixture.active,
        fixture.failure_id,
    )
    .await;
    probe.proxied.close().await;
    probe.proxy.stop().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_cancellation_guardian_lost_lease_before_write_preserves_history_without_quarantine()
{
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = cancellation_commit_fixture(&store).await;
    let fence = fixture.handoff.lease().fence();
    let before = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let waiting = Arc::new(tokio::sync::Notify::new());
    let evidence = Arc::new(PausedDirectEvidence {
        ledger: AdmissionDirectEvidence {
            store: store.clone(),
        },
        calls: AtomicUsize::new(0),
        waiting: Arc::clone(&waiting),
        pending_call: Some(1),
        wait_until: None,
    });
    let lifecycle = cancellation_fault_lifecycle(&store, &fixture, Arc::clone(&evidence));
    let original = fixture.handoff.clone();
    let closing = tokio::spawn(async move { lifecycle.confirm_cancellation(original).await });
    tokio::time::timeout(Duration::from_secs(10), waiting.notified())
        .await
        .unwrap();
    store.release_lease(fence).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, stateknot_runtime::GraphLifecycleError::Store { source } if matches!(*source, StoreError::NoActiveLease))
    );
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
    assert_scoped_cancellation_pending(&store, &before, &fixture.active).await;
    assert_eq!(fixture.nested.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.nested.root_calls.load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
async fn scoped_cancellation_quarantine_commit_ack_survives_guardian_failure() {
    use super::commit_proxy::{Cut, TransactionTarget};
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = cancellation_commit_fixture(&store).await;
    let fence = fixture.handoff.lease().fence();
    corrupt_cancellation_frame_stack(fence).await;
    let mut probe =
        cancellation_ack_probe(&store, &fixture, TransactionTarget::RunQuarantine, None).await;
    probe.proxy.wait_cut(Cut::CommitResponse).await;
    let isolated = store
        .load_run_quarantine(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(isolated.request().expected_fence(), Some(fence));
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
    // The original one-connection pool acquire bound is thirty seconds. The
    // guardian must actually fail while validation still awaits its mutation ACK.
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert!(
        !probe.closing.is_finished(),
        "guardian discarded committed quarantine ACK: {:?}",
        (&mut probe.closing).await.unwrap()
    );
    probe.proxy.resume_ack();
    let error = tokio::time::timeout(Duration::from_secs(10), probe.closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, stateknot_runtime::GraphLifecycleError::Store { source }
        if matches!(*source, StoreError::RunQuarantined))
    );
    assert_eq!(probe.evidence.calls.load(Ordering::SeqCst), 0);
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
    assert_eq!(fixture.nested.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.nested.root_calls.load(Ordering::SeqCst), 0);
    probe.proxied.close().await;
    probe.proxy.stop().await;
    store.close().await;
}

fn corrupt_cancellation_frame_stack(fence: &RunFence) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        let connection: sqlx_postgres::PgConnectOptions =
            std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
        let pool = sqlx_postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(connection)
            .await
            .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        sqlx_core::query::query("ALTER TABLE stateknot.graph_frame_stacks DISABLE TRIGGER ALL")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let changed = sqlx_core::query::query(
            "UPDATE stateknot.graph_frame_stacks SET lifetime_starts=lifetime_starts+1 WHERE tenant_id=$1 AND run_id=$2"
        ).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid())
            .execute(&mut *transaction).await.unwrap();
        assert_eq!(changed.rows_affected(), 1);
        sqlx_core::query::query("ALTER TABLE stateknot.graph_frame_stacks ENABLE TRIGGER ALL")
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        pool.close().await;
    })
}
