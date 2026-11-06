// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Expected normal scoped-Join resume contract; source-only RED author input.
//! A green result requires application callback, exact consumption and real Return.
use super::*;
use stateknot_core::{ChildRunJoinBinding, ChildRunJoinHead, RunFailure};
use stateknot_runtime::{GraphDirectUsageEvidence, GraphDirectUsageEvidenceContext};
use stateknot_store_postgres::LeaseClaimOutcome;

struct ActiveJoinFixture {
    key: ChildRunKey,
    owner: NodeAttemptStartHead,
    original_fence: RunFence,
    root_checkpoint: Checkpoint,
    request: ChildRunJoinRequest,
    registration: JournalEvent,
    child: StoredAgentAdmission,
    registered_direct: BudgetUsage,
}

#[allow(clippy::too_many_lines)]
async fn active_registered_join(store: &PostgresStore) -> ActiveJoinFixture {
    let definitions = graphs();
    let tenant = tenant("scoped-join-active-resume");
    let ids = AgentRunIds::generate();
    let (fixture, leaf, isolated, agent) = definitions;
    let run = ids.run_id();
    for graph in [&fixture.graph, &leaf, &isolated] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let intent = request.intent();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            tenant.clone(),
            run,
            request.admission_event_id(),
            JournalPayload::new(
                test_payload().schema().clone(),
                JournalEventKind::new(AgentAdmission::JOURNAL_EVENT_KIND).unwrap(),
                BoundedJson::try_from_value(json!({"intent_digest":intent.intent_digest()}))
                    .unwrap(),
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        tenant.clone(),
        run,
        request.initial_checkpoint_id(),
        fixture.graph.reference(),
        request.initial_state().clone(),
        fixture.graph.entry_nodes().clone(),
    )
    .unwrap();
    let admitted = Box::pin(store.admit_agent_run(
        intent.clone(),
        append,
        checkpoint,
        fixture.registry.schemas(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        admitted,
        AgentAdmissionCommitOutcome::Committed(_)
    ));
    let admission = admitted.stored();
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &fixture.graph.frame_calls().unwrap().calls()[0],
        &fixture.graph,
        admission.checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let entered = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        admission.event().head(),
        initial_usage(admission),
        fixture.registry.schemas(),
    ))
    .await
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(entry) = entered else {
        panic!("fresh frame entry")
    };
    let dispatch =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap()
            .finish(entry.event().head(), entry.event().recorded_at())
            .unwrap();
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            tenant.clone(),
            run,
            EventId::generate(),
            entry.event().head(),
            fence.clone(),
        ),
        &dispatch,
        &NodeId::new("Step_A").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let owner = started.attempt().start().head();
    let parent = admission.admission().intent();
    let child = AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            tenant.clone(),
            RunId::generate(),
            ThreadId::generate(),
            InvocationId::generate(),
            &agent,
        ),
        agent,
        AgentRequest::new(
            isolated.input_schema().clone(),
            BoundedJson::try_from_value(json!({"step":"child"})).unwrap(),
            parent
                .request()
                .budget_limits()
                .clone()
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_input_bytes(ByteCount::new(512))
                .with_output_bytes(ByteCount::new(512)),
        ),
        parent.budget_layers().iter().cloned(),
        isolated.reference(),
        parent.authority().clone(),
    )
    .unwrap();
    let child_intent = ChildRunAdmissionIntent::for_frame(
        admission.admission(),
        entry.entry().checkpoint(),
        ChildRunKey::new(
            owner.activation().clone(),
            ChildRunSlot::new("analysis").unwrap(),
        )
        .unwrap(),
        child,
        isolated.clone(),
        CheckpointState::new(
            isolated.state_schema().clone(),
            BoundedJson::try_from_value(json!({"step":"child"})).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint())
        .unwrap()
        .len()
        + serde_json_canonicalizer::to_vec(entry.entry().checkpoint().checkpoint())
            .unwrap()
            .len()
        + serde_json_canonicalizer::to_vec(&entry.entry().checkpoint().head())
            .unwrap()
            .len();
    let direct_at = |event_bytes| {
        BudgetUsage::builder()
            .graph_depth(ExecutionCount::new(2))
            .graph_steps(ExecutionCount::new(2))
            .concurrent_branches(ExecutionCount::new(1))
            .fan_out(ExecutionCount::new(1))
            .event_bytes(ByteCount::new(event_bytes as u64))
            .checkpoint_bytes(ByteCount::new(checkpoint_bytes as u64))
            .input_bytes(ByteCount::new(
                serde_json_canonicalizer::to_vec(parent.request().input())
                    .unwrap()
                    .len() as u64,
            ))
            .build()
            .unwrap()
    };
    let key = child_intent.key().clone();
    let child_ids = child_intent.child().provenance();
    let spawned = Box::pin(
        store.admit_child_run(
            child_intent.clone(),
            &owner,
            worker_append_kind(
                &tenant,
                run,
                started.attempt().start().journal_head().clone(),
                &fence,
                "child-run-admitted",
            ),
            JournalAppend::new(
                JournalExpectation::empty(),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    child_ids.run_id(),
                    EventId::generate(),
                    payload(AgentAdmission::JOURNAL_EVENT_KIND),
                )
                .unwrap(),
            )
            .unwrap(),
            CheckpointWrite::initial(
                tenant.clone(),
                child_ids.run_id(),
                CheckpointId::generate(),
                child_intent.child().graph().clone(),
                child_intent.initial_state().clone(),
                isolated.entry_nodes().clone(),
            )
            .unwrap(),
            direct_at(complete_journal(store, &tenant, run, 3).await),
            fixture.registry.schemas(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(spawned, ChildRunCommitOutcome::Committed(_)));
    let joined = Box::pin(store.register_child_join(
        ChildRunJoinRequest::new([key.clone()]).unwrap(),
        &owner,
        worker_append_kind(
            &tenant,
            run,
            spawned.record().spawn().head(),
            &fence,
            "child-join-registered",
        ),
    ))
    .await
    .unwrap();
    assert!(matches!(joined, ChildJoinCommitOutcome::Committed(_)));
    assert!(joined.record().head().is_none());
    let current = store.load_run(&tenant, run).await.unwrap();
    assert!(current.lease().is_none());
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    ActiveJoinFixture {
        key,
        owner,
        original_fence: fence,
        root_checkpoint: admission.checkpoint().clone(),
        request: joined.record().request().clone(),
        registration: joined.record().registration().clone(),
        child: spawned.record().child().clone(),
        registered_direct: direct_at(complete_journal(store, &tenant, run, 5).await),
    }
}

struct CountedNode {
    inner: TestNodeExecutor,
    child_join: bool,
}

impl GraphNodeExecutor for CountedNode {
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
        // These are ordinary successful fixture executors, not panic traps or
        // synthetic failures. Any accidental launch is observed by their counter.
        self.inner.execute(context)
    }
}

// This is the existing root JoinExecutor resume algorithm, specialized to the
// already admitted/published scoped fixture. The no-handle branch re-registers
// the existing sealed request; it never supplies a fake child handle or result.
struct PublishedJoinExecutor {
    graph: GraphReference,
    key: ChildRunKey,
    request: ChildRunJoinRequest,
    binding: ChildRunJoinBinding,
    publication: ChildRunJoinHead,
    terminal: Value,
    f2: RunFence,
    launched: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
}
impl GraphNodeExecutor for PublishedJoinExecutor {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }
    fn node_id(&self) -> &NodeId {
        self.key.parent().node_id()
    }
    fn supports_child_join(&self) -> bool {
        true
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            assert_eq!(context.attempt().activation(), self.key.parent());
            assert_eq!(context.attempt().fence(), &self.f2);
            assert_eq!(
                context.checkpoint().head(),
                *self.key.parent().base_checkpoint()
            );
            let Some(join) = context.child_join() else {
                self.launched.fetch_add(1, Ordering::SeqCst);
                return Ok(GraphNodeExecution::child_join(self.request.clone()));
            };
            assert_eq!(join.head(), &self.publication);
            assert_eq!(join.binding(), &self.binding);
            assert_eq!(join.binding().request(), &self.request);
            assert_eq!(join.binding().terminals().len(), 1);
            let child = join.load_child(self.key.slot()).await.unwrap();
            assert_eq!(serde_json::to_value(child).unwrap(), self.terminal);
            assert!(matches!(
                join.load_child(&ChildRunSlot::new("not-sealed").unwrap())
                    .await,
                Err(StoreError::ChildJoinRejected)
            ));
            assert_eq!(self.resumed.fetch_add(1, Ordering::SeqCst), 0);
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                NodeControl::Continue,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn resume_registry(
    value: &ActiveJoinFixture,
    publication: &ChildRunJoinHead,
    binding: &ChildRunJoinBinding,
    terminal: &Value,
    f2: &RunFence,
    launched: &Arc<AtomicUsize>,
    resumed: &Arc<AtomicUsize>,
    leaf_tail: &Arc<AtomicUsize>,
    parent_tail: &Arc<AtomicUsize>,
    unrelated: &Arc<AtomicUsize>,
) -> ExecutableGraphRegistry {
    let (fixture, leaf, isolated, _) = graphs();
    let mut builder = ExecutableGraphRegistryBuilder::new(fixture.registry.schemas().clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: fixture.graph.reducer().clone(),
        }))
        .unwrap();
    for graph in [&fixture.graph, &leaf, &isolated] {
        builder.register_graph(graph.clone()).unwrap();
        for node in graph.nodes() {
            if graph
                .frame_calls()
                .is_some_and(|calls| calls.call(node.node_id()).is_some())
            {
                continue;
            }
            if graph == &leaf && node.node_id() == value.key.parent().node_id() {
                builder
                    .register_node(Arc::new(PublishedJoinExecutor {
                        graph: graph.reference(),
                        key: value.key.clone(),
                        request: value.request.clone(),
                        binding: binding.clone(),
                        publication: publication.clone(),
                        terminal: terminal.clone(),
                        f2: f2.clone(),
                        launched: Arc::clone(launched),
                        resumed: Arc::clone(resumed),
                    }))
                    .unwrap();
                continue;
            }
            let calls = if graph == &leaf {
                leaf_tail
            } else if graph == &fixture.graph {
                parent_tail
            } else {
                unrelated
            };
            builder
                .register_node(Arc::new(CountedNode {
                    inner: TestNodeExecutor {
                        graph: graph.reference(),
                        node_id: node.node_id().clone(),
                        behavior: if node.allows_terminal() {
                            TestNodeBehavior::Terminal(graph.output_schema().clone())
                        } else {
                            TestNodeBehavior::Continue
                        },
                        delay: Duration::ZERO,
                        calls: Arc::clone(calls),
                    },
                    child_join: graph.child_runs().is_some_and(|policy| {
                        policy
                            .declarations()
                            .iter()
                            .any(|d| d.node_id() == node.node_id())
                    }),
                }))
                .unwrap();
        }
    }
    builder.build().unwrap()
}

// Complete fixture-specific, zero-I/O Run-wide DIRECT ledger. The original seven
// exact measured events and all original checkpoint/input bytes are retained.
// New normal dispatch work is measured from durable owning facts, using the
// algorithm already exercised by nested_driver::AdmissionDirectEvidence. Unknown
// event kinds, non-zero application usage and crossed provenance fail closed.
struct ResumeDirectEvidence {
    store: PostgresStore,
    original: Vec<JournalEvent>,
    baseline: BudgetUsage,
    calls: Arc<AtomicUsize>,
}
impl ResumeDirectEvidence {
    #[allow(clippy::too_many_lines)]
    fn usage_at<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
        head: &'a JournalHead,
    ) -> BoxFuture<'a, Result<BudgetUsage, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            let page = self
                .store
                .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            // This bounded fixture fits one page. Do not silently truncate or
            // infer usage from an active-frame floor if that assumption changes.
            if page.has_more()
                || page.events().len() < self.original.len()
                || page.events()[..self.original.len()] != self.original
                || page.events().last().map(JournalEvent::head).as_ref() != Some(head)
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let mut event_bytes = 0_u64;
            let mut checkpoint_bytes = 0_u64;
            let mut graph_steps = 0_u64;
            let mut retries = 0_u64;
            for event in &page.events()[self.original.len()..] {
                event_bytes += serde_json_canonicalizer::to_vec(event).unwrap().len() as u64;
                let data = event.payload().data().as_value();
                match event.payload().kind().as_str() {
                    "graph-node-attempt-started" | "graph-node-attempt-succeeded" => {
                        let id = data["attempt_id"].as_str().unwrap().parse().unwrap();
                        let attempt = self
                            .store
                            .load_node_attempt(tenant, &run, id)
                            .await
                            .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
                        if !["Step_A", "Step.B", "finish"]
                            .contains(&attempt.start().activation().node_id().as_str())
                            || attempt.completion().is_some_and(|completion| {
                                completion.usage() != &BudgetUsage::zero()
                            })
                        {
                            return Err(GraphLifecycleEvidenceError::Unavailable);
                        }
                    }
                    "graph-frame-caller-rebound" => {
                        graph_steps += 1;
                        retries += 1;
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
                        checkpoint_bytes +=
                            (serde_json_canonicalizer::to_vec(barrier.checkpoint().checkpoint())
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
                        graph_steps += 1;
                    }
                    _ => return Err(GraphLifecycleEvidenceError::Unavailable),
                }
            }
            let delta = BudgetUsage::builder()
                .graph_steps(ExecutionCount::new(graph_steps))
                .retries(ExecutionCount::new(retries))
                .event_bytes(ByteCount::new(event_bytes))
                .checkpoint_bytes(ByteCount::new(checkpoint_bytes))
                .build()
                .unwrap();
            self.baseline
                .checked_accumulate(&delta)
                .map_err(|_| GraphLifecycleEvidenceError::Corrupt)
        })
    }
}
impl GraphLifecycleEvidenceProvider for ResumeDirectEvidence {
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

fn edit_parent_run(mut rows: Snapshot, parent: RunId, edit: impl FnOnce(&mut Value)) -> Snapshot {
    let parent = parent.to_string();
    let mut runs: Value = serde_json::from_str(&rows["runs"]).unwrap();
    let runs = runs.as_array_mut().unwrap();
    let mut matching = runs
        .iter_mut()
        .filter(|row| row["run_id"].as_str() == Some(parent.as_str()));
    let row = matching.next().unwrap();
    assert!(matching.next().is_none());
    edit(row);
    runs.sort_by_key(|row| serde_json::to_string(row).unwrap());
    rows.insert("runs".into(), serde_json::to_string(runs).unwrap());
    rows
}

async fn database_clock(pool: &PgPool) -> Timestamp {
    let micros: i64 =
        query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint")
            .fetch_one(pool)
            .await
            .unwrap();
    Timestamp::from_unix_micros(micros).unwrap()
}

fn assert_exact_release_snapshot(before: Snapshot, after: Snapshot, parent: RunId) {
    let mut released_at = Value::Null;
    let after = edit_parent_run(after, parent, |row| {
        // Public owning reads separately bound this exact ready instant to
        // the Driver's database-clock interval and original F2 epoch.
        released_at = row["scheduler_ready_at"].clone();
        assert!(released_at.is_string());
        assert_eq!(row["updated_at"], released_at);
    });
    let expected = edit_parent_run(before, parent, |row| {
        // Store::release_lease preserves the epoch and changes exactly these
        // seven columns for a runnable cancellation_requested Run.
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
        "only exact-F2 lease release cleanup is allowed"
    );
}

fn business_facts(rows: Snapshot, parent: RunId) -> Snapshot {
    edit_parent_run(rows, parent, |row| {
        // A live same-F2 guardian may renew these ownership times. All
        // scheduler, fencing, lifecycle, journal and child facts remain exact.
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

async fn assert_owner_and_root(store: &PostgresStore, value: &ActiveJoinFixture) {
    let history = Box::pin(store.load_node_attempt_history_page(
        value.key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), value.owner);
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(value.key.tenant_id(), value.key.parent_run_id())
            .await
            .unwrap()
            .as_ref(),
        Some(&value.root_checkpoint)
    );
    let owned = Box::pin(store.load_child_run(&value.key)).await.unwrap();
    assert_eq!(owned.intent().key(), &value.key);
    assert_eq!(
        owned.spawn().source().worker_fence(),
        Some(&value.original_fence)
    );
    let joined = Box::pin(store.load_child_join(value.key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(joined.request(), &value.request);
    assert_eq!(joined.registration(), &value.registration);
    assert!(joined.consumed().is_none());
}

async fn settle_zero_io_child_and_publish(
    store: &PostgresStore,
    value: &ActiveJoinFixture,
) -> stateknot_core::ChildRunJoinHead {
    let child = &value.child;
    let provenance = child.admission().intent().provenance();
    let current = store
        .load_run(provenance.tenant_id(), provenance.run_id())
        .await
        .unwrap();
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    assert!(current.lease().is_none());
    let activation = stateknot_core::NodeActivation::for_ready_root(
        child.checkpoint(),
        NodeId::new("Step_A").unwrap(),
    )
    .unwrap();
    let history = Box::pin(store.load_node_attempt_history_page(
        &activation,
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert!(history.records().is_empty());
    // A trusted control-plane terminal for a child that did no node/provider
    // work, matching the existing Store Join fixture. This is not evidence of
    // real application child execution; that belongs to the later full feature.
    let usage = initial_usage(child)
        .checked_accumulate(
            &BudgetUsage::builder()
                .input_bytes(ByteCount::new(
                    serde_json_canonicalizer::to_vec(child.admission().intent().request().input())
                        .unwrap()
                        .len() as u64,
                ))
                .build()
                .unwrap(),
        )
        .unwrap();
    let failure = RunFailure::new(
        test_failure(
            "test.scoped_join.zero_io_child",
            "Stop a child before any application or provider work.",
        ),
        store.observe_database_clock().await.unwrap(),
        usage,
    )
    .unwrap();
    store
        .append_control_plane(
            control(
                provenance.tenant_id(),
                provenance.run_id(),
                current.journal_head().unwrap().clone(),
                "test-zero-io-child-failed",
            ),
            RunProjection::transition(
                current.lifecycle().revision(),
                RunTransition::Fail { failure },
            ),
        )
        .await
        .unwrap();
    let parent = store
        .load_run(value.key.tenant_id(), value.key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    let settled = Box::pin(store.settle_child_run(
        &value.key,
        control(
            value.key.tenant_id(),
            value.key.parent_run_id(),
            parent.journal_head().unwrap().clone(),
            "child-run-settled",
        ),
    ))
    .await
    .unwrap();
    assert!(matches!(
        settled,
        ChildRunSettlementOutcome::Committed { .. }
    ));
    let parent = store
        .load_run(value.key.tenant_id(), value.key.parent_run_id())
        .await
        .unwrap();
    let publication = Box::pin(store.publish_child_join(
        &value.request,
        control(
            value.key.tenant_id(),
            value.key.parent_run_id(),
            parent.journal_head().unwrap().clone(),
            "child-join-published",
        ),
    ))
    .await
    .unwrap();
    assert!(matches!(publication, ChildJoinCommitOutcome::Committed(_)));
    publication.record().head().unwrap().clone()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn published_scoped_join_f2_resumes_consumes_returns_and_continues_parent() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(active_registered_join(&store)).await;
    let tenant = value.key.tenant_id();
    let run = value.key.parent_run_id();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let unpublished_before = snapshot(&pool, tenant).await;
    Box::pin(assert_owner_and_root(&store, &value)).await;
    let unpublished = Box::pin(store.load_child_join(value.key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert!(unpublished.head().is_none());
    assert!(unpublished.binding().is_none());
    println!(
        "STATEKNOT_SCOPED_JOIN_V3_UNPUBLISHED_TENANT_BEFORE={}",
        serde_json::to_string(&unpublished_before).unwrap()
    );
    let unpublished_claim = store.claim_lease(tenant, run, AttemptId::generate()).await;
    println!("STATEKNOT_SCOPED_JOIN_V3_UNPUBLISHED_CLAIM={unpublished_claim:?}");
    let unpublished_after = snapshot(&pool, tenant).await;
    println!(
        "STATEKNOT_SCOPED_JOIN_V3_UNPUBLISHED_TENANT_AFTER={}",
        serde_json::to_string(&unpublished_after).unwrap()
    );
    assert_eq!(
        unpublished_after, unpublished_before,
        "an unpublished Join claim must leave every tenant row unchanged; actual: {unpublished_claim:?}"
    );
    // guard_child_join_run rejects Active + leased + unpublished via SKC06,
    // which StoreError::database maps precisely to RunNotRunnable. SKC07 is
    // reserved for an unconsumed checkpoint/success or invalid result binding.
    assert!(
        matches!(&unpublished_claim, Err(StoreError::RunNotRunnable)),
        "expected the exact unpublished Join waiting rejection; actual: {unpublished_claim:?}"
    );
    let publication = Box::pin(settle_zero_io_child_and_publish(&store, &value)).await;
    Box::pin(assert_owner_and_root(&store, &value)).await;
    let original = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(8).unwrap())
        .await
        .unwrap();
    assert!(!original.has_more());
    assert_eq!(original.events().len(), 7);
    assert_eq!(
        original.events().last().unwrap().head(),
        *publication.journal_head()
    );
    let extra = complete_journal(&store, tenant, run, 7).await as u64
        - value.registered_direct.event_bytes().get();
    let baseline = value
        .registered_direct
        .checked_accumulate(
            &BudgetUsage::builder()
                .event_bytes(ByteCount::new(extra))
                .build()
                .unwrap(),
        )
        .unwrap();
    let initial_child = store
        .load_run(
            tenant,
            value.child.admission().intent().provenance().run_id(),
        )
        .await
        .unwrap();
    assert_eq!(initial_child.lifecycle().status(), RunStatus::Failed);
    let expected_terminal = serde_json::to_value(initial_child.lifecycle()).unwrap();
    let joined = Box::pin(store.load_child_join(value.key.parent()))
        .await
        .unwrap()
        .unwrap();
    let binding = joined.binding().unwrap().clone();
    assert_eq!(joined.head(), Some(&publication));
    assert!(joined.consumed().is_none());
    let account = Box::pin(store.load_child_budget_account(tenant, run))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    let child_usage = account.children()[0].settlement().unwrap().usage().clone();
    let original_children = serde_json::to_value(account.children()).unwrap();
    store.close().await;

    // Reconnect and rebuild the deployment. Neither an original executor future
    // nor an in-memory GraphChildJoin is carried across this recovery boundary.
    let store = test_store().await.unwrap();
    let claimed = store
        .claim_lease(tenant, run, AttemptId::generate())
        .await
        .unwrap();
    assert!(matches!(claimed, LeaseClaimOutcome::Claimed(_)));
    let f2 = claimed.lease().fence().clone();
    assert!(f2.epoch() > value.original_fence.epoch());
    assert_ne!(f2.attempt_id(), value.original_fence.attempt_id());
    let active = Box::pin(store.load_active_graph_frame(tenant, run))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.run().lifecycle().status(), RunStatus::Active);
    assert_eq!(
        active.checkpoint().checkpoint().head(),
        *value.key.parent().base_checkpoint()
    );
    assert_eq!(
        active.checkpoint().frame().namespace(),
        value.key.parent().graph_namespace()
    );
    let lifecycle = serde_json::to_value(active.run().lifecycle()).unwrap();
    let launched = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let leaf_tail = Arc::new(AtomicUsize::new(0));
    let parent_tail = Arc::new(AtomicUsize::new(0));
    let unrelated = Arc::new(AtomicUsize::new(0));
    let evidence_calls = Arc::new(AtomicUsize::new(0));
    let registry = resume_registry(
        &value,
        &publication,
        &binding,
        &expected_terminal,
        &f2,
        &launched,
        &resumed,
        &leaf_tail,
        &parent_tail,
        &unrelated,
    );
    for reference in [
        value.root_checkpoint.graph(),
        active.checkpoint().frame().target(),
        value.child.admission().intent().graph(),
    ] {
        let definition = store
            .load_graph_definition(tenant, reference)
            .await
            .unwrap();
        assert_eq!(
            registry.resolve(reference).unwrap().graph(),
            definition.graph()
        );
    }
    let evidence = Arc::new(ResumeDirectEvidence {
        store: store.clone(),
        original: original.events().to_vec(),
        baseline,
        calls: Arc::clone(&evidence_calls),
    });
    let driver = DurableGraphDriver::new(
        store.clone(),
        registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(evidence.clone());
    let before = snapshot(&pool, tenant).await;
    println!(
        "STATEKNOT_SCOPED_JOIN_V3_TENANT_BEFORE={}",
        serde_json::to_string(&before).unwrap()
    );
    let child_id = value
        .child
        .admission()
        .intent()
        .provenance()
        .run_id()
        .to_string();
    let rows: Value = serde_json::from_str(&before["runs"]).unwrap();
    let child_row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["run_id"].as_str() == Some(child_id.as_str()))
        .unwrap()
        .clone();
    let started_at = database_clock(&pool).await;
    let result = driver.drive(f2.clone(), CancellationSignal::never()).await;
    let finished_at = database_clock(&pool).await;
    let after = snapshot(&pool, tenant).await;
    println!(
        "STATEKNOT_SCOPED_JOIN_V3_TENANT_AFTER={}",
        serde_json::to_string(&after).unwrap()
    );
    let drive_result = match result {
        Err(error) => {
            println!("STATEKNOT_SCOPED_JOIN_V3_ACTUAL_ERROR={error:?}");
            assert_eq!(launched.load(Ordering::SeqCst), 0);
            assert_eq!(resumed.load(Ordering::SeqCst), 0);
            assert_eq!(leaf_tail.load(Ordering::SeqCst), 0);
            assert_eq!(parent_tail.load(Ordering::SeqCst), 0);
            assert_eq!(unrelated.load(Ordering::SeqCst), 0);
            assert_eq!(evidence_calls.load(Ordering::SeqCst), 0);
            Box::pin(assert_owner_and_root(&store, &value)).await;
            let current = store.load_run(tenant, run).await.unwrap();
            assert_eq!(
                serde_json::to_value(current.lifecycle()).unwrap(),
                lifecycle
            );
            assert!(!current.is_quarantined());
            assert!(current.lease().is_none());
            assert_eq!(current.last_fencing_epoch(), Some(f2.epoch()));
            let released = current.scheduler_ready_at().unwrap();
            assert!(released >= started_at && released <= finished_at);
            assert!(released >= claimed.lease().renewed_at());
            assert_exact_release_snapshot(before, after, run);
            pool.close().await;
            store.close().await;
            assert!(matches!(
                error,
                stateknot_runtime::GraphDriverError::ScopedChildJoinUnsupported
            ));
            panic!(
                "expected real scoped Join resume/consume/Return; current natural G1 RED: {error:?}"
            );
        }
        Ok(drive_result) => drive_result,
    };
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = drive_result.outcome() else {
        panic!("expected actual parent terminal handoff after scoped Return: {drive_result:?}");
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    assert_eq!(
        handoff.plan().barrier().base_checkpoint().graph(),
        value.root_checkpoint.graph()
    );
    assert_eq!(launched.load(Ordering::SeqCst), 0);
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(leaf_tail.load(Ordering::SeqCst), 1);
    assert_eq!(parent_tail.load(Ordering::SeqCst), 1);
    assert_eq!(unrelated.load(Ordering::SeqCst), 0);
    assert!(evidence_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(drive_result.report().frame_entries_committed(), 0);
    assert_eq!(drive_result.report().frame_returns_committed(), 1);
    assert_eq!(drive_result.report().frame_barriers_committed(), 2);
    let history = Box::pin(store.load_node_attempt_history_page(
        value.key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 2);
    assert_eq!(history.records()[0].start().head(), value.owner);
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    assert_eq!(history.records()[1].start().fence(), &f2);
    assert_eq!(history.records()[1].status(), NodeAttemptStatus::Succeeded);
    let completed = history.records()[1].completion().unwrap();
    let pending = Box::pin(store.load_pending_node_result(value.key.parent()))
        .await
        .unwrap();
    assert_eq!(pending.intent().child_join(), Some(&publication));
    assert_eq!(pending.journal_head(), completed.journal_head());
    assert_eq!(completed.outcome().result(), Some(&pending.head()));
    let retained = Box::pin(store.load_child_join(value.key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.request(), &value.request);
    assert_eq!(retained.registration(), &value.registration);
    assert_eq!(retained.binding(), Some(&binding));
    assert_eq!(retained.head(), Some(&publication));
    assert_eq!(retained.consumed(), Some(pending.journal_head()));
    let barrier = Box::pin(store.load_graph_frame_barrier(
        tenant,
        run,
        value.key.parent().graph_namespace(),
        value.key.parent().base_checkpoint().checkpoint_id(),
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(barrier.barrier().result_heads(), &[pending.head()]);
    let returned =
        Box::pin(store.load_graph_frame_return(tenant, run, value.key.parent().graph_namespace()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        returned.terminal_checkpoint().frame(),
        active.checkpoint().frame()
    );
    assert_eq!(
        returned.result().intent().activation().node_id().as_str(),
        "call"
    );
    assert!(
        returned
            .result()
            .intent()
            .activation()
            .graph_namespace()
            .is_root()
    );
    assert_eq!(returned.completion().start().fence(), &f2);
    assert_eq!(
        returned.completion().outcome().result(),
        Some(&returned.result().head())
    );
    assert!(
        Box::pin(store.load_active_graph_frame(tenant, run))
            .await
            .unwrap()
            .is_none()
    );
    let current = store.load_run(tenant, run).await.unwrap();
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    assert_eq!(
        serde_json::to_value(current.lifecycle()).unwrap(),
        lifecycle
    );
    assert!(!current.is_quarantined());
    assert_eq!(current.checkpoint().unwrap().superstep().get(), 1);
    let account = Box::pin(store.load_child_budget_account(tenant, run))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(account.children()).unwrap(),
        original_children
    );
    let direct = evidence
        .usage_at(tenant, run, current.journal_head().unwrap())
        .await
        .unwrap();
    let total = Box::pin(store.include_child_usage(tenant, run, direct.clone()))
        .await
        .unwrap();
    assert_eq!(
        total,
        direct
            .checked_accumulate(&child_usage.cumulative_only())
            .unwrap()
    );
    let child_after = store
        .load_run(
            tenant,
            value.child.admission().intent().provenance().run_id(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(child_after.lifecycle()).unwrap(),
        expected_terminal
    );
    let rows: Value = serde_json::from_str(&after["runs"]).unwrap();
    let retained_child = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["run_id"].as_str() == Some(child_id.as_str()))
        .unwrap();
    assert_eq!(retained_child, &child_row);
    // A second public drive at the same handed-off Root must recover the same
    // terminal plan without redispatching, consuming or returning anything.
    let terminal_snapshot = snapshot(&pool, tenant).await;
    let again = driver.drive(f2, CancellationSignal::never()).await.unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(again_handoff) = again.outcome() else {
        panic!("terminal retry must return the actual same Root handoff");
    };
    assert_eq!(
        again_handoff.plan().barrier().base_checkpoint(),
        handoff.plan().barrier().base_checkpoint()
    );
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(leaf_tail.load(Ordering::SeqCst), 1);
    assert_eq!(parent_tail.load(Ordering::SeqCst), 1);
    assert_eq!(again.report().frame_returns_committed(), 0);
    assert_eq!(again.report().node_attempts_started(), 0);
    assert_eq!(again.report().node_attempts_completed(), 0);
    // Same-F2 guardian lease timestamps may advance; all business facts must be exact.
    assert_eq!(
        business_facts(terminal_snapshot, run),
        business_facts(snapshot(&pool, tenant).await, run)
    );
    pool.close().await;
    store.close().await;
}

mod profile_bounds {
    // Copyright 2026 StateKnot contributors
    // SPDX-License-Identifier: Apache-2.0

    //! Public Driver boundaries for scoped child-Join execution.
    use super::*;
    use stateknot_store_postgres::{
        RunQuarantineCause, RunQuarantineCommitOutcome, RunQuarantineComponent,
        RunQuarantineRequest,
    };

    struct CountUnavailableDirect(Arc<AtomicUsize>);

    impl GraphLifecycleEvidenceProvider for CountUnavailableDirect {
        fn direct_usage_evidence(
            &self,
            _context: GraphDirectUsageEvidenceContext,
        ) -> BoxFuture<'_, Result<GraphDirectUsageEvidence, GraphLifecycleEvidenceError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
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

    async fn bounds_pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
            .await
            .unwrap()
    }

    fn fresh_registry(
        fixture: &DriverFixture,
        leaf: &CompiledGraph,
        isolated: &CompiledGraph,
        calls: &Arc<AtomicUsize>,
    ) -> ExecutableGraphRegistry {
        let mut builder = ExecutableGraphRegistryBuilder::new(fixture.registry.schemas().clone());
        builder
            .register_reducer(Arc::new(TestReducer {
                reference: fixture.graph.reducer().clone(),
            }))
            .unwrap();
        for graph in [&fixture.graph, leaf, isolated] {
            builder.register_graph(graph.clone()).unwrap();
            for node in graph.nodes() {
                if graph
                    .frame_calls()
                    .is_some_and(|policy| policy.call(node.node_id()).is_some())
                {
                    continue;
                }
                builder
                    .register_node(Arc::new(CountedNode {
                        inner: TestNodeExecutor {
                            graph: graph.reference(),
                            node_id: node.node_id().clone(),
                            behavior: if node.allows_terminal() {
                                TestNodeBehavior::Terminal(graph.output_schema().clone())
                            } else {
                                TestNodeBehavior::Continue
                            },
                            delay: Duration::ZERO,
                            calls: Arc::clone(calls),
                        },
                        // This target deliberately lacks the typed child-Join binding.
                        child_join: graph.reference() != leaf.reference()
                            && graph.child_runs().is_some_and(|policy| {
                                policy
                                    .declarations()
                                    .iter()
                                    .any(|declaration| declaration.node_id() == node.node_id())
                            }),
                    }))
                    .unwrap();
            }
        }
        builder.build().unwrap()
    }

    #[tokio::test]
    async fn fresh_frame_entry_with_unsupported_child_executor_rejects_before_work() {
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let (fixture, leaf, isolated, _) = graphs();
        let calls = Arc::new(AtomicUsize::new(0));
        let evidence_calls = Arc::new(AtomicUsize::new(0));
        let registry = fresh_registry(&fixture, &leaf, &isolated, &calls);
        let tenant = tenant("scoped-join-g1-fresh-entry-bounds");
        let ids = AgentRunIds::generate();
        let run = ids.run_id();
        for graph in [&fixture.graph, &leaf, &isolated] {
            store
                .register_graph_definition(tenant.clone(), graph.clone())
                .await
                .unwrap();
        }
        let admitted = DurableAgentAdmission::new(store.clone(), registry.clone())
            .unwrap()
            .admit(durable_admission_request(
                &fixture,
                tenant.clone(),
                ids,
                fixture.graph.output_schema().clone(),
                fixture.graph.input_schema().clone(),
            ))
            .await
            .unwrap()
            .stored()
            .clone();
        let claimed = store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap();
        assert!(matches!(claimed, LeaseClaimOutcome::Claimed(_)));
        let fence = claimed.lease().fence().clone();
        let pool = bounds_pool().await;
        let before = snapshot(&pool, &tenant).await;
        let started = database_clock(&pool).await;
        let driver = DurableGraphDriver::new(
            store.clone(),
            registry,
            DurableGraphDriverOptions::default(),
        )
        .unwrap()
        .with_direct_usage_evidence(Arc::new(CountUnavailableDirect(Arc::clone(
            &evidence_calls,
        ))));
        let result = driver
            .drive(fence.clone(), CancellationSignal::never())
            .await;
        let finished = database_clock(&pool).await;
        assert!(
            matches!(
                &result,
                Err(stateknot_runtime::GraphDriverError::ScopedChildJoinUnsupported)
            ),
            "fresh Root entry must reject the unsupported child executor: {result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(evidence_calls.load(Ordering::SeqCst), 0);
        assert_eq!(driver.execution_activity().active_nodes(), 0);
        assert!(
            store
                .load_active_graph_frame(&tenant, run)
                .await
                .unwrap()
                .is_none()
        );
        let current = store.load_run(&tenant, run).await.unwrap();
        assert_eq!(current.journal_head(), Some(&admitted.event().head()));
        assert_eq!(current.checkpoint(), admitted.run().checkpoint());
        assert_eq!(current.lifecycle().status(), RunStatus::Active);
        assert!(!current.is_quarantined());
        assert!(current.lease().is_none());
        assert_eq!(current.last_fencing_epoch(), Some(fence.epoch()));
        let released = current.scheduler_ready_at().unwrap();
        assert!(released >= started && released <= finished);
        assert!(released >= claimed.lease().renewed_at());
        assert_exact_release_snapshot(before, snapshot(&pool, &tenant).await, run);
        pool.close().await;
        store.close().await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn published_scoped_join_quarantined_child_refuses_resume_before_new_start() {
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let value = Box::pin(active_registered_join(&store)).await;
        let publication = Box::pin(settle_zero_io_child_and_publish(&store, &value)).await;
        let tenant = value.key.tenant_id();
        let run = value.key.parent_run_id();
        let joined = Box::pin(store.load_child_join(value.key.parent()))
            .await
            .unwrap()
            .unwrap();
        let binding = joined.binding().unwrap().clone();
        assert_eq!(joined.head(), Some(&publication));
        assert!(joined.consumed().is_none());
        let child_id = value.child.admission().intent().provenance().run_id();
        let child = store.load_run(tenant, child_id).await.unwrap();
        assert_eq!(child.lifecycle().status(), RunStatus::Failed);
        let terminal = serde_json::to_value(child.lifecycle()).unwrap();
        let claimed = store
            .claim_lease(tenant, run, AttemptId::generate())
            .await
            .unwrap();
        assert!(matches!(claimed, LeaseClaimOutcome::Claimed(_)));
        let f2 = claimed.lease().fence().clone();
        assert!(f2.epoch() > value.original_fence.epoch());
        let quarantine = RunQuarantineRequest::new(
            tenant.clone(),
            child_id,
            QuarantineId::generate(),
            JournalExpectation::exact(child.journal_head().unwrap().clone()),
            RunQuarantineCause::OperatorPolicy,
            RunQuarantineComponent::new("scoped-join-g1-published-child-bounds").unwrap(),
            Digest::sha256(b"public operator quarantine before scoped Join resume"),
        )
        .unwrap();
        assert!(matches!(
            store.quarantine_run(quarantine.clone()).await.unwrap(),
            RunQuarantineCommitOutcome::Committed(_)
        ));
        assert_eq!(
            store
                .load_run_quarantine(tenant, child_id)
                .await
                .unwrap()
                .request(),
            &quarantine
        );
        assert!(
            store
                .load_run(tenant, child_id)
                .await
                .unwrap()
                .is_quarantined()
        );
        let launched = Arc::new(AtomicUsize::new(0));
        let resumed = Arc::new(AtomicUsize::new(0));
        let leaf_tail = Arc::new(AtomicUsize::new(0));
        let parent_tail = Arc::new(AtomicUsize::new(0));
        let unrelated = Arc::new(AtomicUsize::new(0));
        let evidence_calls = Arc::new(AtomicUsize::new(0));
        let registry = resume_registry(
            &value,
            &publication,
            &binding,
            &terminal,
            &f2,
            &launched,
            &resumed,
            &leaf_tail,
            &parent_tail,
            &unrelated,
        );
        let driver = DurableGraphDriver::new(
            store.clone(),
            registry,
            DurableGraphDriverOptions::default(),
        )
        .unwrap()
        .with_direct_usage_evidence(Arc::new(CountUnavailableDirect(Arc::clone(
            &evidence_calls,
        ))));
        let pool = bounds_pool().await;
        // The public child quarantine is already committed in this exact baseline.
        // No subsequent parent quarantine, Start, rebind or other business write is exempt.
        let before = snapshot(&pool, tenant).await;
        let started = database_clock(&pool).await;
        let result = driver.drive(f2.clone(), CancellationSignal::never()).await;
        let finished = database_clock(&pool).await;
        assert!(
            matches!(
                &result,
                Err(stateknot_runtime::GraphDriverError::Store { source })
                    if matches!(source.as_ref(), StoreError::RunQuarantined)
            ),
            "actual child quarantine must refuse fresh published resume: {result:?}"
        );
        for calls in [
            &launched,
            &resumed,
            &leaf_tail,
            &parent_tail,
            &unrelated,
            &evidence_calls,
        ] {
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
        assert_eq!(driver.execution_activity().active_nodes(), 0);
        Box::pin(assert_owner_and_root(&store, &value)).await;
        let current = store.load_run(tenant, run).await.unwrap();
        assert_eq!(current.lifecycle().status(), RunStatus::Active);
        assert!(!current.is_quarantined());
        assert!(current.lease().is_none());
        assert_eq!(current.last_fencing_epoch(), Some(f2.epoch()));
        let released = current.scheduler_ready_at().unwrap();
        assert!(released >= started && released <= finished);
        assert!(released >= claimed.lease().renewed_at());
        assert_exact_release_snapshot(before, snapshot(&pool, tenant).await, run);
        pool.close().await;
        store.close().await;
    }

    fn boundary_call_graph(
        template: &CompiledGraph,
        name: &str,
        target: &CompiledGraph,
        root: bool,
    ) -> CompiledGraph {
        let call = GraphFrameCall::new(
            NodeId::new("call").unwrap(),
            NodeId::new("bounds.slot").unwrap(),
            target,
            RouteId::new("return").unwrap(),
        )
        .unwrap();
        CompiledGraph::compile(
            capability(name),
            if root {
                template.input_schema().clone()
            } else {
                target.state_schema().clone()
            },
            template.state_schema().clone(),
            template.update_schema().clone(),
            template.output_schema().clone(),
            template.reducer().clone(),
            template.entry_nodes().clone(),
            template.nodes().iter().cloned(),
            template.limits(),
        )
        .unwrap()
        .with_frame_calls(GraphFrameCallPolicy::new(7, 8, [call]).unwrap())
        .unwrap()
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn active_frame_unsupported_child_executor_rejects_before_f2_caller_rebind() {
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let (mut fixture, leaf, isolated, _) = graphs();
        let intermediate = boundary_call_graph(
            &fixture.graph,
            "g1-bounds-normal-intermediate",
            &leaf,
            false,
        );
        assert!(intermediate.child_runs().is_none());
        assert!(leaf.child_runs().is_some());
        let root =
            boundary_call_graph(&fixture.graph, "g1-bounds-active-root", &intermediate, true)
                .with_child_runs(fixture.graph.child_runs().unwrap().clone())
                .unwrap();
        fixture.graph = root;
        let calls = Arc::new(AtomicUsize::new(0));
        let evidence_calls = Arc::new(AtomicUsize::new(0));
        let mut builder = ExecutableGraphRegistryBuilder::new(fixture.registry.schemas().clone());
        builder
            .register_reducer(Arc::new(TestReducer {
                reference: fixture.graph.reducer().clone(),
            }))
            .unwrap();
        for graph in [&fixture.graph, &intermediate, &leaf, &isolated] {
            builder.register_graph(graph.clone()).unwrap();
            for node in graph.nodes() {
                if graph
                    .frame_calls()
                    .is_some_and(|policy| policy.call(node.node_id()).is_some())
                {
                    continue;
                }
                builder
                    .register_node(Arc::new(CountedNode {
                        inner: TestNodeExecutor {
                            graph: graph.reference(),
                            node_id: node.node_id().clone(),
                            behavior: if node.allows_terminal() {
                                TestNodeBehavior::Terminal(graph.output_schema().clone())
                            } else {
                                TestNodeBehavior::Continue
                            },
                            delay: Duration::ZERO,
                            calls: Arc::clone(&calls),
                        },
                        // This target deliberately lacks the typed child-Join binding.
                        child_join: graph.reference() != leaf.reference()
                            && graph.child_runs().is_some_and(|policy| {
                                policy
                                    .declarations()
                                    .iter()
                                    .any(|declaration| declaration.node_id() == node.node_id())
                            }),
                    }))
                    .unwrap();
            }
        }
        let registry = builder.build().unwrap();
        let tenant = tenant("scoped-join-g1-active-target-bounds");
        let ids = AgentRunIds::generate();
        let run = ids.run_id();
        for graph in [&fixture.graph, &intermediate, &leaf, &isolated] {
            store
                .register_graph_definition(tenant.clone(), graph.clone())
                .await
                .unwrap();
        }
        let admitted = DurableAgentAdmission::new(store.clone(), registry.clone())
            .unwrap()
            .admit(durable_admission_request(
                &fixture,
                tenant.clone(),
                ids,
                fixture.graph.output_schema().clone(),
                fixture.graph.input_schema().clone(),
            ))
            .await
            .unwrap()
            .stored()
            .clone();
        let first = store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap();
        assert!(matches!(first, LeaseClaimOutcome::Claimed(_)));
        let f1 = first.lease().fence().clone();
        let entry_plan = GraphFrameEntryPlan::for_root(
            &fixture.graph.frame_calls().unwrap().calls()[0],
            &fixture.graph,
            admitted.checkpoint(),
            &intermediate,
            CheckpointId::generate(),
            AttemptId::generate(),
            f1.clone(),
        )
        .unwrap();
        let entered = Box::pin(store.enter_graph_frame(
            entry_plan,
            EventId::generate(),
            admitted.event().head(),
            initial_usage(&admitted),
            registry.schemas(),
        ))
        .await
        .unwrap();
        assert!(matches!(
            entered,
            GraphFrameEntryCommitOutcome::Committed(_)
        ));
        let active = Box::pin(store.load_active_graph_frame(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.open_frames().len(), 1);
        assert_eq!(
            active.checkpoint().frame().target(),
            &intermediate.reference()
        );
        assert_eq!(active.caller().fence(), &f1);
        let original_caller = active.caller().head();
        let original_frame = active.checkpoint().head();
        assert!(matches!(
            store.release_lease(&f1).await.unwrap(),
            stateknot_store_postgres::LeaseReleaseOutcome::Released
        ));
        let claimed = store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap();
        assert!(matches!(claimed, LeaseClaimOutcome::Claimed(_)));
        let f2 = claimed.lease().fence().clone();
        assert!(f2.epoch() > f1.epoch());
        assert_ne!(f2.attempt_id(), f1.attempt_id());
        let current = Box::pin(store.load_active_graph_frame(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.caller().head(), original_caller);
        assert_ne!(current.caller().fence(), &f2);
        let plan = ReadyNodeRecoveryPlanner::for_frame(current.checkpoint().clone(), f2.clone())
            .unwrap()
            .finish(
                current.run().journal_head().unwrap().clone(),
                store.observe_database_clock().await.unwrap(),
            )
            .unwrap();
        assert_eq!(plan.nodes().len(), 1);
        assert_eq!(
            plan.nodes()[0].kind(),
            stateknot_core::RecoveryNodeKind::Dispatchable
        );
        let target_call = intermediate.frame_calls().unwrap().calls()[0].clone();
        assert_eq!(
            plan.nodes()[0].activation().node_id(),
            target_call.node_id()
        );
        assert_eq!(target_call.target(), &leaf.reference());
        let driver = DurableGraphDriver::new(
            store.clone(),
            registry,
            DurableGraphDriverOptions::default(),
        )
        .unwrap()
        .with_direct_usage_evidence(Arc::new(CountUnavailableDirect(Arc::clone(
            &evidence_calls,
        ))));
        let pool = bounds_pool().await;
        let before = snapshot(&pool, &tenant).await;
        let started = database_clock(&pool).await;
        let result = driver.drive(f2.clone(), CancellationSignal::never()).await;
        let finished = database_clock(&pool).await;
        assert!(
            matches!(
                &result,
                Err(stateknot_runtime::GraphDriverError::ScopedChildJoinUnsupported)
            ),
            "unsupported next frame must be rejected before rebinding its active F1 caller: {result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(evidence_calls.load(Ordering::SeqCst), 0);
        assert_eq!(driver.execution_activity().active_nodes(), 0);
        let after = Box::pin(store.load_active_graph_frame(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.open_frames().len(), 1);
        assert_eq!(after.checkpoint().head(), original_frame);
        assert_eq!(after.caller().head(), original_caller);
        assert_eq!(after.caller().fence(), &f1);
        let current = store.load_run(&tenant, run).await.unwrap();
        assert_eq!(current.journal_head(), active.run().journal_head());
        assert_eq!(current.checkpoint(), admitted.run().checkpoint());
        assert_eq!(current.lifecycle().status(), RunStatus::Active);
        assert!(!current.is_quarantined());
        assert!(current.lease().is_none());
        assert_eq!(current.last_fencing_epoch(), Some(f2.epoch()));
        let released = current.scheduler_ready_at().unwrap();
        assert!(released >= started && released <= finished);
        assert!(released >= claimed.lease().renewed_at());
        assert_exact_release_snapshot(before, snapshot(&pool, &tenant).await, run);
        pool.close().await;
        store.close().await;
    }
}
