// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Fresh scoped child suspension must run the actual child application to success.
//! Covers first suspension, published recovery and once-only scoped consumption.
use super::*;
use stateknot_core::ChildRunJoinHead;
use stateknot_runtime::{
    DurableChildJoinPublisher, DurableChildReconciler, DurableChildReconcilerOptions,
    ExecutableGraph, GraphDirectUsageEvidence, GraphDirectUsageEvidenceContext,
};

type RetainedKey = Arc<tokio::sync::Mutex<Option<ChildRunKey>>>;

struct FreshCountedNode {
    inner: TestNodeExecutor,
    child_join: bool,
}
impl GraphNodeExecutor for FreshCountedNode {
    fn graph(&self) -> &GraphReference {
        self.inner.graph()
    }
    fn node_id(&self) -> &NodeId {
        self.inner.node_id()
    }
    fn supports_child_join(&self) -> bool {
        self.child_join
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        self.inner.execute(context)
    }
}

fn edit_parent(mut rows: Snapshot, parent: RunId, edit: impl FnOnce(&mut Value)) -> Snapshot {
    let id = parent.to_string();
    let mut runs: Value = serde_json::from_str(&rows["runs"]).unwrap();
    let list = runs.as_array_mut().unwrap();
    let mut matching = list
        .iter_mut()
        .filter(|row| row["run_id"].as_str() == Some(id.as_str()));
    let row = matching.next().unwrap();
    assert!(matching.next().is_none());
    edit(row);
    list.sort_by_key(|row| serde_json::to_string(row).unwrap());
    rows.insert("runs".into(), serde_json::to_string(&runs).unwrap());
    rows
}

fn business_facts(rows: Snapshot, parent: RunId) -> Snapshot {
    edit_parent(rows, parent, |row| {
        for field in [
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "updated_at",
        ] {
            row.as_object_mut().unwrap().remove(field);
        }
    })
}

fn child_facts(mut rows: Snapshot, child: RunId) -> Snapshot {
    let id = child.to_string();
    for encoded in rows.values_mut() {
        let mut value: Value = serde_json::from_str(encoded).unwrap();
        value.as_array_mut().unwrap().retain(|row| {
            row["run_id"].as_str() == Some(id.as_str())
                || row["child_run_id"].as_str() == Some(id.as_str())
        });
        *encoded = serde_json::to_string(&value).unwrap();
    }
    rows
}

fn assert_exact_release_snapshot(before: Snapshot, after: Snapshot, parent: RunId) {
    let mut released_at = Value::Null;
    let after = edit_parent(after, parent, |row| {
        released_at = row["scheduler_ready_at"].clone();
        assert!(released_at.is_string());
        assert_eq!(row["updated_at"], released_at);
    });
    let expected = edit_parent(before, parent, |row| {
        for field in [
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "scheduler_not_before",
        ] {
            row[field] = Value::Null;
        }
        row["scheduler_ready_at"] = released_at.clone();
        row["updated_at"] = released_at;
    });
    assert_eq!(
        after, expected,
        "whole tenant may change only through exact-fence lease release"
    );
}

// Preserve the actual provider-native reducer when composing the closed parent
// deployment; no substitute state algorithm or new production interface is used.
struct RetainedChildReducer(ExecutableGraph);
impl GraphReducer for RetainedChildReducer {
    fn reference(&self) -> &GraphReducerReference {
        self.0.reducer().reference()
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        inputs: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        self.0.reducer().reduce(state, inputs)
    }
}

// This ledger is deliberately only for the parent Root plus its one frame.
// All its application nodes are zero-I/O. Native child Model/Tool usage is
// authenticated separately and enters the parent once through settlement.
struct FreshDirectEvidence {
    store: PostgresStore,
    key: RetainedKey,
    root: GraphReference,
    leaf: GraphReference,
    calls: Arc<AtomicUsize>,
}
impl FreshDirectEvidence {
    #[allow(clippy::too_many_lines)]
    fn usage_at<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
        head: &'a JournalHead,
    ) -> BoxFuture<'a, Result<BudgetUsage, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            let admitted = self
                .store
                .load_agent_admission(tenant, run)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            if admitted.admission().intent().graph() != &self.root {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let page = self
                .store
                .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            if page.has_more()
                || page.events().last().map(JournalEvent::head).as_ref() != Some(head)
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let mut event_bytes = 0_u64;
            let mut checkpoint_bytes = 0_u64;
            let mut steps = 0_u64;
            let mut depth = 1_u64;
            let mut retries = 0_u64;
            // Reuse only complete successful owning reads within this exact page.
            // Every invocation reloads admission and journal; no record grants authority.
            let mut children: Vec<stateknot_store_postgres::ChildRunRecord> = Vec::new();
            let mut attempts: Vec<stateknot_core::NodeAttempt> = Vec::new();
            let mut joins: Vec<(
                stateknot_core::NodeActivation,
                stateknot_store_postgres::ChildJoinRecord,
            )> = Vec::new();
            for event in page.events() {
                event_bytes += serde_json_canonicalizer::to_vec(event).unwrap().len() as u64;
                let data = event.payload().data().as_value();
                match event.payload().kind().as_str() {
                    "agent-admitted" => {
                        if event != admitted.event() {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
                        checkpoint_bytes += serde_json_canonicalizer::to_vec(admitted.checkpoint())
                            .unwrap()
                            .len() as u64;
                    }
                    "graph-node-attempt-started" | "graph-node-attempt-succeeded" => {
                        let attempt_id: AttemptId =
                            data["attempt_id"].as_str().unwrap().parse().unwrap();
                        let index = if let Some(index) = attempts
                            .iter()
                            .position(|attempt| attempt.start().attempt_id() == attempt_id)
                        {
                            index
                        } else {
                            attempts.push(
                                self.store
                                    .load_node_attempt(tenant, &run, attempt_id)
                                    .await
                                    .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?,
                            );
                            attempts.len() - 1
                        };
                        let attempt = &attempts[index];
                        let activation = attempt.start().activation();
                        if ![&self.root, &self.leaf].contains(&activation.base_checkpoint().graph())
                            || !["Step_A", "Step.B", "finish"]
                                .contains(&activation.node_id().as_str())
                            || attempt
                                .completion()
                                .is_some_and(|c| c.usage() != &BudgetUsage::zero())
                        {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
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
                        if entry.event() != event
                            || entry.entry().checkpoint().checkpoint().graph() != &self.leaf
                        {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
                        checkpoint_bytes += (serde_json_canonicalizer::to_vec(
                            entry.entry().checkpoint().checkpoint(),
                        )
                        .unwrap()
                        .len()
                            + serde_json_canonicalizer::to_vec(&entry.entry().checkpoint().head())
                                .unwrap()
                                .len()) as u64;
                        depth = 2;
                        steps += 1;
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
                            .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
                        if barrier.event() != event {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
                        checkpoint_bytes +=
                            (serde_json_canonicalizer::to_vec(barrier.checkpoint().checkpoint())
                                .unwrap()
                                .len()
                                + serde_json_canonicalizer::to_vec(&barrier.checkpoint().head())
                                    .unwrap()
                                    .len()) as u64;
                        steps += 1;
                    }
                    "graph-frame-returned" => {
                        steps += 1;
                    }
                    "graph-frame-caller-rebound" => {
                        steps += 1;
                        retries += 1;
                    }
                    "root-graph-barrier-committed" => {
                        let id = serde_json::from_value(data["successor_checkpoint_id"].clone())
                            .unwrap();
                        let checkpoint = self
                            .store
                            .load_checkpoint(tenant, run, id)
                            .await
                            .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                        checkpoint_bytes +=
                            serde_json_canonicalizer::to_vec(&checkpoint).unwrap().len() as u64;
                        steps += 1;
                    }
                    "child-run-admitted" | "child-run-settled" => {
                        let key = self
                            .key
                            .lock()
                            .await
                            .clone()
                            .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
                        let index = if let Some(index) = children
                            .iter()
                            .position(|record| record.intent().key() == &key)
                        {
                            index
                        } else {
                            children.push(
                                self.store
                                    .load_child_run(&key)
                                    .await
                                    .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?,
                            );
                            children.len() - 1
                        };
                        let child = &children[index];
                        if key.tenant_id() != tenant || key.parent_run_id() != run {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
                        if event.payload().kind().as_str() == "child-run-admitted" {
                            if child.spawn() != event {
                                return Err(GraphLifecycleEvidenceError::Corrupt);
                            }
                        } else if event.source().worker_fence().is_some()
                            || child.settlement().is_none()
                            || data["operation"] != "child_run_settled"
                            || data["ownership_digest"]
                                != key.digest().to_string().trim_start_matches("sha256:")
                        {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
                    }
                    "child-join-registered" | "child-join-published" => {
                        let key = self
                            .key
                            .lock()
                            .await
                            .clone()
                            .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
                        let index = if let Some(index) = joins
                            .iter()
                            .position(|(activation, _)| activation == key.parent())
                        {
                            index
                        } else {
                            let join = self
                                .store
                                .load_child_join(key.parent())
                                .await
                                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?
                                .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
                            joins.push((key.parent().clone(), join));
                            joins.len() - 1
                        };
                        let join = &joins[index].1;
                        if (event.payload().kind().as_str() == "child-join-registered"
                            && join.registration() != event)
                            || (event.payload().kind().as_str() == "child-join-published"
                                && join.head().map(ChildRunJoinHead::journal_head)
                                    != Some(&event.head()))
                        {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
                    }
                    // No provider, arbitrary control-plane terminal, retry, wait
                    // or unpriced event can be counted as zero in this fixture.
                    _ => return Err(GraphLifecycleEvidenceError::Unavailable),
                }
            }
            BudgetUsage::builder()
                .graph_depth(ExecutionCount::new(depth))
                .graph_steps(ExecutionCount::new(steps))
                .retries(ExecutionCount::new(retries))
                .concurrent_branches(ExecutionCount::new(1))
                .fan_out(ExecutionCount::new(1))
                .input_bytes(ByteCount::new(
                    admitted
                        .admission()
                        .intent()
                        .request()
                        .input()
                        .stats()
                        .compact_bytes() as u64,
                ))
                .event_bytes(ByteCount::new(event_bytes))
                .checkpoint_bytes(ByteCount::new(checkpoint_bytes))
                .build()
                .map_err(|_| GraphLifecycleEvidenceError::Corrupt)
        })
    }
}
impl GraphLifecycleEvidenceProvider for FreshDirectEvidence {
    fn direct_usage_evidence(
        &self,
        context: GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphDirectUsageEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let usage = self
                .usage_at(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                    context.journal_head(),
                )
                .await?;
            Ok(GraphDirectUsageEvidence::new(context, usage))
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
        UnavailableLifecycleEvidence.failure_evidence(context)
    }
    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        UnavailableLifecycleEvidence.cancellation_evidence(context)
    }
}

struct FreshJoinExecutor {
    store: PostgresStore,
    graph: GraphReference,
    node: NodeId,
    child: stateknot_core::AgentAdmissionIntent,
    child_graph: CompiledGraph,
    child_state: CheckpointState,
    schemas: JsonSchemaRegistry,
    direct: Arc<FreshDirectEvidence>,
    key: RetainedKey,
    launched: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
}
impl GraphNodeExecutor for FreshJoinExecutor {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }
    fn node_id(&self) -> &NodeId {
        &self.node
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
            let activation = context.attempt().activation();
            assert!(!activation.graph_namespace().is_root());
            let key = ChildRunKey::new(activation.clone(), ChildRunSlot::new("analysis").unwrap())
                .unwrap();
            if let Some(join) = context.child_join() {
                assert_eq!(join.binding().request().keys(), &[key.clone()]);
                let terminal = join.load_child(key.slot()).await.unwrap();
                assert_eq!(terminal.status(), RunStatus::Succeeded);
                assert!(terminal.result().is_some());
                assert_eq!(self.resumed.fetch_add(1, Ordering::SeqCst), 0);
                return Ok(GraphNodeExecution::new(
                    NodeStateChange::Unchanged,
                    NodeControl::Continue,
                    NodeInvocationBindings::empty(),
                    BudgetUsage::zero(),
                ));
            }
            assert_eq!(self.launched.fetch_add(1, Ordering::SeqCst), 0);
            let parent = self
                .store
                .load_agent_admission(activation.tenant_id(), activation.run_id())
                .await
                .unwrap();
            let active = self
                .store
                .load_active_graph_frame(activation.tenant_id(), activation.run_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(active.checkpoint().checkpoint(), context.checkpoint());
            assert_eq!(
                active.checkpoint().activation(self.node.clone()).unwrap(),
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
                self.child.clone(),
                self.child_graph.clone(),
                self.child_state.clone(),
            )
            .unwrap();
            *self.key.lock().await = Some(key);
            let current = self
                .store
                .load_run(activation.tenant_id(), activation.run_id())
                .await
                .unwrap();
            let head = current.journal_head().unwrap();
            let direct = self
                .direct
                .usage_at(activation.tenant_id(), activation.run_id(), head)
                .await
                .unwrap();
            let child_append = JournalAppend::new(
                JournalExpectation::empty(),
                JournalEventIntent::control_plane(
                    activation.tenant_id().clone(),
                    self.child.provenance().run_id(),
                    EventId::generate(),
                    payload(AgentAdmission::JOURNAL_EVENT_KIND),
                )
                .unwrap(),
            )
            .unwrap();
            let child_checkpoint = CheckpointWrite::initial(
                activation.tenant_id().clone(),
                self.child.provenance().run_id(),
                CheckpointId::generate(),
                self.child_graph.reference(),
                self.child_state.clone(),
                self.child_graph.entry_nodes().clone(),
            )
            .unwrap();
            let admitted = Box::pin(self.store.admit_child_run(
                intent.clone(),
                context.attempt(),
                worker_append_kind(
                    activation.tenant_id(),
                    activation.run_id(),
                    head.clone(),
                    context.attempt().fence(),
                    "child-run-admitted",
                ),
                child_append,
                child_checkpoint,
                direct,
                &self.schemas,
            ))
            .await
            .unwrap();
            assert!(matches!(admitted, ChildRunCommitOutcome::Committed(_)));
            assert_eq!(admitted.record().intent().key(), intent.key());
            Ok(GraphNodeExecution::child_join(
                ChildRunJoinRequest::new([intent.key().clone()]).unwrap(),
            ))
        })
    }
}

fn fresh_graphs(child: &CompiledGraph, agent: &AgentDescriptor) -> (DriverFixture, CompiledGraph) {
    let mut fixture = driver_fixture();
    let t = &fixture.graph;
    let leaf = declared(
        CompiledGraph::compile(
            capability("g2-fresh-scoped-leaf"),
            t.state_schema().clone(),
            t.state_schema().clone(),
            t.update_schema().clone(),
            t.output_schema().clone(),
            t.reducer().clone(),
            t.entry_nodes().clone(),
            t.nodes().iter().cloned(),
            t.limits(),
        )
        .unwrap(),
        "Step_A",
        child,
        agent,
    );
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        &leaf,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    let root = CompiledGraph::compile(
        capability("g2-fresh-scoped-root"),
        t.input_schema().clone(),
        t.state_schema().clone(),
        t.output_schema().clone(),
        t.output_schema().clone(),
        t.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("call").unwrap()]).unwrap(),
        [
            GraphNode::new(
                NodeId::new("call").unwrap(),
                None,
                GraphRoutes::try_new([GraphRoute::new(
                    RouteId::new("return").unwrap(),
                    ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap(),
                )
                .unwrap()])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::empty(),
                None,
                true,
            )
            .unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap()
    .with_frame_calls(GraphFrameCallPolicy::new(7, 8, [call]).unwrap())
    .unwrap();
    fixture.graph = declared(root, "finish", child, agent);
    (fixture, leaf)
}

fn fresh_schemas(
    fixture: &DriverFixture,
    leaf: &CompiledGraph,
    child: &ProviderNativeFixture,
) -> JsonSchemaRegistry {
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    // Copy exact existing canonical documents; no networking or guessed schema.
    let mut references = vec![
        fixture.graph.input_schema().clone(),
        fixture.graph.state_schema().clone(),
        fixture.graph.update_schema().clone(),
        fixture.graph.output_schema().clone(),
    ];
    references.extend([
        leaf.input_schema().clone(),
        leaf.state_schema().clone(),
        leaf.update_schema().clone(),
        leaf.output_schema().clone(),
    ]);
    let graph = child.definition.graph();
    references.extend([
        graph.input_schema().clone(),
        graph.state_schema().clone(),
        graph.update_schema().clone(),
        graph.output_schema().clone(),
    ]);
    for tool in child.definition.descriptor().tools() {
        references.extend([tool.input_schema().clone(), tool.output_schema().clone()]);
    }
    let mut copied = Vec::new();
    for reference in references {
        if copied.contains(&reference) {
            continue;
        }
        let bytes = fixture
            .registry
            .schemas()
            .canonical_bytes(&reference)
            .or_else(|| child.registry.schemas().canonical_bytes(&reference))
            .unwrap();
        builder
            .register(reference.clone(), serde_json::from_slice(bytes).unwrap())
            .unwrap();
        copied.push(reference);
    }
    register_standard_graph_driver_event_schema(&mut builder).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut builder).unwrap();
    register_standard_agent_cancellation_event_schema(&mut builder).unwrap();
    register_standard_agent_admission_event_schema(&mut builder).unwrap();
    register_standard_invocation_execution_event_schema(&mut builder).unwrap();
    stateknot_runtime::register_standard_run_failure_close_event_schema(&mut builder).unwrap();
    stateknot_runtime::register_standard_child_join_event_schema(&mut builder).unwrap();
    stateknot_runtime::register_standard_child_reconciliation_event_schema(&mut builder).unwrap();
    builder.build().unwrap()
}

struct FreshCounters {
    launched: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
    leaf_tail: Arc<AtomicUsize>,
    root_tail: Arc<AtomicUsize>,
}
impl FreshCounters {
    fn new() -> Self {
        Self {
            launched: Arc::new(AtomicUsize::new(0)),
            resumed: Arc::new(AtomicUsize::new(0)),
            leaf_tail: Arc::new(AtomicUsize::new(0)),
            root_tail: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn fresh_registry(
    store: &PostgresStore,
    fixture: &DriverFixture,
    leaf: &CompiledGraph,
    child: &ProviderNativeFixture,
    candidate: &stateknot_core::AgentAdmissionIntent,
    schemas: &JsonSchemaRegistry,
    direct: &Arc<FreshDirectEvidence>,
    counters: &FreshCounters,
) -> ExecutableGraphRegistry {
    let mut builder = ExecutableGraphRegistryBuilder::new(schemas.clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: fixture.graph.reducer().clone(),
        }))
        .unwrap();
    let child_executable = child
        .registry
        .resolve(&child.definition.graph().reference())
        .unwrap();
    builder
        .register_reducer(Arc::new(RetainedChildReducer(child_executable.clone())))
        .unwrap();
    builder
        .register_graph(child.definition.graph().clone())
        .unwrap();
    for node in child.definition.graph().nodes() {
        builder
            .register_node(child_executable.node_executor(node.node_id()).unwrap())
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
                    .register_node(Arc::new(FreshJoinExecutor {
                        store: store.clone(),
                        graph: graph.reference(),
                        node: node.node_id().clone(),
                        child: candidate.clone(),
                        child_graph: child.definition.graph().clone(),
                        child_state: child.definition.initial_state().unwrap(),
                        schemas: schemas.clone(),
                        direct: Arc::clone(direct),
                        key: Arc::clone(&direct.key),
                        launched: Arc::clone(&counters.launched),
                        resumed: Arc::clone(&counters.resumed),
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
    builder.build().unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn fresh_scoped_child_join_runs_real_child_publishes_consumes_and_returns() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
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
    let tenant = tenant("scoped-child-join-g2-fresh");
    let ids = AgentRunIds::generate();
    let run = ids.run_id();
    let root_request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let parent_intent = root_request.intent();
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
        &store, &fixture, &leaf, &native, &candidate, &schemas, &direct, &counters,
    );
    for graph in [&fixture.graph, &leaf, native.definition.graph()] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admission = DurableAgentAdmission::new(store.clone(), deployment.clone())
        .unwrap()
        .admit(root_request)
        .await
        .unwrap()
        .stored()
        .clone();
    println!("G2_FRESH_SETUP store=true registry=true admission=true");
    let f1 = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, &tenant).await;
    let driver = DurableGraphDriver::new(
        store.clone(),
        deployment.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(direct.clone());
    assert_eq!(counters.launched.load(Ordering::SeqCst), 0);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 0);
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
    println!(
        "G2_FRESH_BEFORE launched={} resumed={} model={} tool={} root={run}",
        counters.launched.load(Ordering::SeqCst),
        counters.resumed.load(Ordering::SeqCst),
        native.model_calls.load(Ordering::SeqCst),
        native.tool_calls.load(Ordering::SeqCst)
    );
    let first = driver.drive(f1.clone(), CancellationSignal::never()).await;
    println!("G2_FRESH_RESULT {first:?}");
    if matches!(&first, Err(GraphDriverError::ScopedChildJoinUnsupported)) {
        assert_eq!(counters.launched.load(Ordering::SeqCst), 0);
        assert_eq!(counters.resumed.load(Ordering::SeqCst), 0);
        assert_eq!(native.model_calls.load(Ordering::SeqCst), 0);
        assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(direct.calls.load(Ordering::SeqCst), 0);
        assert!(
            store
                .load_active_graph_frame(&tenant, run)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .list_child_run_identities(&tenant, run)
                .await
                .unwrap()
                .is_empty()
        );
        assert_exact_release_snapshot(before, snapshot(&pool, &tenant).await, run);
        println!(
            "G2_FRESH_GUARD_CONFIRMED scoped_child_join_unsupported callbacks=0 child_ownership=0 frames=0 direct=0"
        );
    }
    // The error branch above always reaches this failing positive expectation.
    // It can authenticate the expected natural RED, never count it as Green.
    let first = first.expect("G2_FRESH_EXPECT_CHILD_JOIN: fresh public Driver must enter the frame and suspend on the actual callback request");
    let GraphDriveOutcome::ChildJoin(request) = first.outcome() else {
        panic!("expected real scoped ChildJoin: {first:?}");
    };
    assert_eq!(request.keys().len(), 1);
    let key = request.keys()[0].clone();
    assert_eq!(Some(&key), direct.key.lock().await.as_ref());
    assert!(!key.parent().graph_namespace().is_root());
    assert_eq!(counters.launched.load(Ordering::SeqCst), 1);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 0);
    let owning = store.load_child_run(&key).await.unwrap();
    assert_eq!(owning.intent().child(), &candidate);
    assert_eq!(owning.spawn().source().worker_fence(), Some(&f1));
    let original_join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    assert_eq!(original_join.request(), request.as_ref());
    assert!(
        original_join.head().is_none()
            && original_join.binding().is_none()
            && original_join.consumed().is_none()
    );
    let parent = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    assert!(parent.lease().is_none() && parent.lifecycle().waits().is_none());
    assert!(matches!(
        store.claim_lease(&tenant, run, AttemptId::generate()).await,
        Err(StoreError::RunNotRunnable)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .as_ref(),
        Some(admission.checkpoint())
    );
    println!("G2_FRESH_SUSPENDED original_owner=executing child_admitted=1 parent_active=true");

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
    assert!(
        matches!(child_result.outcome(), AgentLoopOutcome::Succeeded(_)),
        "real child application: {child_result:?}"
    );
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 1);
    let child = store.load_run(&tenant, child_run).await.unwrap();
    assert_eq!(child.lifecycle().status(), RunStatus::Succeeded);
    assert!(child.lease().is_none());
    let child_usage = child.lifecycle().terminal_usage().unwrap().clone();
    assert_eq!(child_usage.model_turns().get(), 2);
    assert_eq!(child_usage.tool_calls().get(), 1);
    assert_eq!(child_usage.unpriced_cost_events(), ExecutionCount::ZERO);
    assert!(child_result.report().node_attempts_started() > 0);
    assert!(child_result.report().node_attempts_completed() > 0);
    let terminal = serde_json::to_value(child.lifecycle()).unwrap();
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
    assert_eq!(reconciled.items()[0].key(), &key);
    reconciled.items()[0].result().unwrap();
    let published = DurableChildJoinPublisher::new(
        store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(published.items().len(), 1);
    published.items()[0].result().unwrap();
    let published_join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    let publication = published_join.head().unwrap().clone();
    assert_eq!(published_join.registration(), original_join.registration());
    assert_eq!(
        published_join.binding().unwrap().terminals()[0].usage(),
        &child_usage
    );
    assert!(published_join.consumed().is_none());
    let settled_child_facts = child_facts(snapshot(&pool, &tenant).await, child_run);
    println!(
        "G2_FRESH_CHILD_CONFIRMED model_calls=2 tool_calls=1 child_succeeded=true published=true"
    );

    drop(driver);
    drop(child_loop);
    // Reconnect and rebuild parent bindings; the resumed handle is derived from
    // durable owning facts, never an in-memory saved GraphChildJoin.
    let reconnected = test_store().await.unwrap();
    let resumed_direct = Arc::new(FreshDirectEvidence {
        store: reconnected.clone(),
        key: Arc::clone(&direct.key),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let rebuilt = fresh_registry(
        &reconnected,
        &fixture,
        &leaf,
        &native,
        &candidate,
        &schemas,
        &resumed_direct,
        &counters,
    );
    let f2 = reconnected
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(f2.epoch() > f1.epoch());
    let resumed_driver = DurableGraphDriver::new(
        reconnected.clone(),
        rebuilt,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(resumed_direct.clone());
    let resumed_result = resumed_driver
        .drive(f2.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = resumed_result.outcome() else {
        panic!("actual Root terminal handoff: {resumed_result:?}");
    };
    assert_eq!(counters.launched.load(Ordering::SeqCst), 1);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(counters.leaf_tail.load(Ordering::SeqCst), 1);
    assert_eq!(counters.root_tail.load(Ordering::SeqCst), 1);
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(resumed_result.report().frame_returns_committed(), 1);
    let consumed = reconnected
        .load_child_join(key.parent())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(consumed.head(), Some(&publication));
    assert_eq!(consumed.registration(), original_join.registration());
    let consumed_head = consumed.consumed().unwrap();
    let attempts = reconnected
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert!(!attempts.has_more());
    assert_eq!(attempts.records().len(), 2);
    assert_eq!(attempts.records()[0].start().fence(), &f1);
    assert_eq!(attempts.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(attempts.records()[0].completion().is_none());
    assert_eq!(attempts.records()[1].start().fence(), &f2);
    assert_eq!(attempts.records()[1].status(), NodeAttemptStatus::Succeeded);
    assert_eq!(
        attempts.records()[1].completion().unwrap().journal_head(),
        consumed_head
    );
    let result = reconnected
        .load_pending_node_result(key.parent())
        .await
        .unwrap();
    assert_eq!(result.journal_head(), consumed_head);
    assert_eq!(result.intent().child_join(), Some(&publication));
    let returned = reconnected
        .load_graph_frame_return(&tenant, run, key.parent().graph_namespace())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(returned.completion().start().fence(), &f2);
    assert_eq!(
        returned.completion().outcome().result(),
        Some(&returned.result().head())
    );
    assert!(
        reconnected
            .load_active_graph_frame(&tenant, run)
            .await
            .unwrap()
            .is_none()
    );
    let final_parent = reconnected.load_run(&tenant, run).await.unwrap();
    assert_eq!(final_parent.lifecycle().status(), RunStatus::Active);
    assert!(final_parent.lifecycle().result().is_none());
    assert_eq!(final_parent.checkpoint().unwrap().superstep().get(), 1);
    assert_eq!(
        settled_child_facts,
        child_facts(snapshot(&pool, &tenant).await, child_run)
    );
    assert_eq!(
        serde_json::to_value(
            reconnected
                .load_run(&tenant, child_run)
                .await
                .unwrap()
                .lifecycle()
        )
        .unwrap(),
        terminal
    );
    let direct_usage = resumed_direct
        .usage_at(&tenant, run, final_parent.journal_head().unwrap())
        .await
        .unwrap();
    assert_eq!(direct_usage.model_turns(), ExecutionCount::ZERO);
    assert_eq!(direct_usage.tool_calls(), ExecutionCount::ZERO);
    assert_eq!(
        reconnected
            .include_child_usage(&tenant, run, direct_usage.clone())
            .await
            .unwrap(),
        direct_usage
            .checked_accumulate(&child_usage.cumulative_only())
            .unwrap()
    );
    let stable = business_facts(snapshot(&pool, &tenant).await, run);
    let again = resumed_driver
        .drive(f2, CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(again_handoff) = again.outcome() else {
        panic!("same Root handoff retry: {again:?}");
    };
    assert_eq!(
        again_handoff.plan().barrier().base_checkpoint(),
        handoff.plan().barrier().base_checkpoint()
    );
    assert_eq!(again.report().node_attempts_started(), 0);
    assert_eq!(again.report().frame_returns_committed(), 0);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(stable, business_facts(snapshot(&pool, &tenant).await, run));
    println!(
        "G2_FRESH_COMPLETE consumed_once=true real_return=true root_active_handoff=true model_calls=2 tool_calls=1"
    );
    pool.close().await;
    reconnected.close().await;
    store.close().await;
}

#[path = "scoped_child_join_partial_spawn.rs"]
mod partial_spawn;

#[path = "scoped_child_join_admission_registration_faults.rs"]
mod admission_registration_faults;

#[path = "scoped_child_join_consumption_faults.rs"]
mod consumption_faults;

#[path = "scoped_child_join_parent_terminal.rs"]
mod parent_terminal;
