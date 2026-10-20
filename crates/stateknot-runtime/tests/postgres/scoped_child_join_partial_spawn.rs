// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Two actual callbacks separated by SIGKILL and an actually expired public lease.
//! Candidate only: current G1 rejects before the first kill point; no qualification is implied.
use super::*;
use crate::process_harness::{INPUT_ENV, TestProcess, parent_control, publish_ready};
use stateknot_store_postgres::ChildRunRecord;

const PARTIAL_SELECTED: &str = "scoped_join_close_process_faults::fresh_join::partial_spawn::two_slot_partial_spawn_survives_real_process_loss_without_duplicate_accounts";
const SLOTS: [&str; 2] = ["analysis", "review"];
const MAX_HISTORY_PAGES: usize = 2;

// The single-slot positive ledger cannot authenticate both ownership events.
// This narrow variant retains its closed zero-I/O parent rules and resolves
// every child event through the existing complete authenticated account.
struct PartialDirectEvidence {
    store: PostgresStore,
    root: GraphReference,
    leaf: GraphReference,
    calls: Arc<AtomicUsize>,
}
impl PartialDirectEvidence {
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
            let account = self
                .store
                .load_child_budget_account(tenant, run)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let keys = account
                .as_ref()
                .map(|a| {
                    a.children()
                        .iter()
                        .map(|e| e.key().clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
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
            // Reuse complete owning records only inside this exact observation.
            // Every call still authenticates admission, account and the requested
            // journal head afresh; these records never authorize a later write.
            let mut children: Vec<ChildRunRecord> = Vec::new();
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
                        let mut matched = None;
                        for key in &keys {
                            let index = if let Some(index) = children
                                .iter()
                                .position(|record| record.intent().key() == key)
                            {
                                index
                            } else {
                                children.push(
                                    self.store
                                        .load_child_run(key)
                                        .await
                                        .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?,
                                );
                                children.len() - 1
                            };
                            let record = &children[index];
                            let admitted = event.payload().kind().as_str() == "child-run-admitted";
                            if (admitted && record.spawn() == event)
                                || (!admitted
                                    && data["ownership_digest"]
                                        == key.digest().to_string().trim_start_matches("sha256:"))
                            {
                                if matched.is_some() {
                                    return Err(GraphLifecycleEvidenceError::Corrupt);
                                }
                                matched = Some((key.clone(), record.clone()));
                            }
                        }
                        let (key, child) = matched.ok_or(GraphLifecycleEvidenceError::Corrupt)?;
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
                        let key = keys.first().ok_or(GraphLifecycleEvidenceError::Corrupt)?;
                        if keys.iter().any(|other| other.parent() != key.parent()) {
                            return Err(GraphLifecycleEvidenceError::Corrupt);
                        }
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
impl GraphLifecycleEvidenceProvider for PartialDirectEvidence {
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

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct FirstProof {
    key: ChildRunKey,
    fence: RunFence,
    frame: stateknot_core::GraphFrameCheckpointHead,
    immutable: Value,
    account_entry: Value,
    caller: NodeAttemptStartHead,
    entry: Value,
}

#[derive(Clone)]
enum PartialMode {
    ParkAfterFirst,
    Recover(Arc<FirstProof>),
}

fn immutable_child(record: &ChildRunRecord) -> Value {
    json!({
        "intent": record.intent(), "spawn": record.spawn(), "ancestors": record.ancestors(),
        "admission": record.child().admission(), "admission_event": record.child().event(),
        "initial_checkpoint": record.child().checkpoint()
    })
}

fn immutable_entry(entry: &stateknot_store_postgres::StoredGraphFrameEntry) -> Value {
    json!({"event":entry.event(), "start":entry.entry().start(),
        "checkpoint":entry.entry().checkpoint(), "digest":entry.digest()})
}

fn short_driver_options() -> DurableGraphDriverOptions {
    DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        1_024,
        Duration::from_millis(100),
        Duration::from_secs(15 * 60),
        3,
        Duration::from_millis(25),
    )
    .unwrap()
}

fn two_slot_graphs(child: &ProviderNativeFixture) -> (DriverFixture, CompiledGraph) {
    let (mut fixture, leaf) = fresh_graphs(child.definition.graph(), child.definition.descriptor());
    let leaf = leaf
        .with_child_runs(
            GraphChildRunPolicy::new(
                ChildRunTopologyLimits::new(2, 8, 4).unwrap(),
                SLOTS.map(|slot| {
                    ChildRunDeclaration::new(
                        NodeId::new("Step_A").unwrap(),
                        ChildRunSlot::new(slot).unwrap(),
                        child.definition.descriptor(),
                        child.definition.graph(),
                    )
                    .unwrap()
                }),
            )
            .unwrap(),
        )
        .unwrap();
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        &leaf,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    fixture.graph = fixture
        .graph
        .with_frame_calls(GraphFrameCallPolicy::new(7, 8, [call]).unwrap())
        .unwrap();
    (fixture, leaf)
}

fn child_template(
    parent: &AgentAdmissionIntent,
    child: &ProviderNativeFixture,
) -> AgentAdmissionIntent {
    let ids = AgentRunIds::generate();
    AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            parent.provenance().tenant_id().clone(),
            ids.run_id(),
            ids.thread_id(),
            ids.invocation_id(),
            child.definition.descriptor(),
        ),
        child.definition.descriptor().clone(),
        AgentRequest::new(
            child.input_schema.clone(),
            BoundedJson::try_from_value(
                json!({"question":"Execute this stable partial-spawn slot."}),
            )
            .unwrap(),
            // Reserve a finite stable slice for each slot, including zero
            // write/delegation/artifact/currency ceilings for this known-free
            // read-only application. Inheriting the entire parent ceiling for
            // each child would make two simultaneous reservations impossible.
            BudgetLimits::empty()
                .with_model_attempts(ExecutionCount::new(4))
                .with_model_turns(ExecutionCount::new(2))
                .with_input_tokens(TokenCount::new(128))
                .with_cached_input_tokens(TokenCount::new(32))
                .with_reasoning_tokens(TokenCount::new(32))
                .with_output_tokens(TokenCount::new(128))
                .with_tool_calls(ExecutionCount::new(1))
                .with_write_calls(ExecutionCount::ZERO)
                .with_remote_agent_delegations(ExecutionCount::ZERO)
                .with_retries(ExecutionCount::new(2))
                .with_artifact_bytes(ByteCount::ZERO)
                .with_costs(
                    stateknot_core::CostLimits::try_new([
                        Money::new("CNY".parse().unwrap(), 0),
                        Money::new("USD".parse().unwrap(), 0),
                    ])
                    .unwrap(),
                )
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_input_bytes(ByteCount::new(65_536))
                .with_output_bytes(ByteCount::new(65_536)),
        ),
        parent.budget_layers().iter().cloned(),
        child.definition.graph().reference(),
        parent.authority().clone(),
    )
    .unwrap()
}

fn fresh_candidate(template: &AgentAdmissionIntent) -> AgentAdmissionIntent {
    let ids = AgentRunIds::generate();
    AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            template.provenance().tenant_id().clone(),
            ids.run_id(),
            ids.thread_id(),
            ids.invocation_id(),
            template.descriptor(),
        ),
        template.descriptor().clone(),
        template.request().clone(),
        template.budget_layers().iter().cloned(),
        template.graph().clone(),
        template.authority().clone(),
    )
    .unwrap()
}

struct PartialExecutor {
    store: PostgresStore,
    pool: PgPool,
    graph: GraphReference,
    node: NodeId,
    template: AgentAdmissionIntent,
    child_graph: CompiledGraph,
    child_state: CheckpointState,
    schemas: JsonSchemaRegistry,
    direct: Arc<PartialDirectEvidence>,
    mode: PartialMode,
    launched: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
    model_calls: Arc<AtomicUsize>,
    tool_calls: Arc<AtomicUsize>,
}

impl GraphNodeExecutor for PartialExecutor {
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
            let keys = SLOTS.map(|slot| {
                ChildRunKey::new(activation.clone(), ChildRunSlot::new(slot).unwrap()).unwrap()
            });
            let request = ChildRunJoinRequest::new(keys.clone()).unwrap();
            if let Some(join) = context.child_join() {
                assert_eq!(join.binding().request(), &request);
                for key in &keys {
                    let terminal = join.load_child(key.slot()).await.unwrap();
                    assert_eq!(terminal.status(), RunStatus::Succeeded);
                    assert!(terminal.result().is_some());
                }
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
            if let PartialMode::Recover(first) = &self.mode {
                assert_eq!(first.key, keys[0]);
                assert_eq!(active.checkpoint().head(), first.frame);
                assert!(context.attempt().fence().epoch() > first.fence.epoch());
                assert_eq!(immutable_entry(active.entry()), first.entry);
                assert_eq!(active.caller().activation(), first.caller.activation());
                assert_eq!(active.caller().fence(), context.attempt().fence());
            }
            for (index, key) in keys.iter().enumerate() {
                let candidate = fresh_candidate(&self.template);
                let candidate_run = candidate.provenance().run_id();
                // Provider-native initial_state creates new message/plan IDs.
                // Cold ACK recovery must retain the originally admitted state,
                // while every other input still comes from the stable template.
                let (child_state, original_spawn) = match (&self.mode, index) {
                    (PartialMode::Recover(first), 0) => {
                        let original = self.store.load_child_run(key).await.unwrap();
                        assert_eq!(original.intent().key(), key);
                        assert_eq!(immutable_child(&original), first.immutable);
                        (
                            original.intent().initial_state().clone(),
                            Some(original.intent().spawn_digest()),
                        )
                    }
                    _ => (self.child_state.clone(), None),
                };
                let intent = ChildRunAdmissionIntent::for_frame(
                    parent.admission(),
                    active.checkpoint(),
                    key.clone(),
                    candidate,
                    self.child_graph.clone(),
                    child_state.clone(),
                )
                .unwrap();
                if let Some(expected) = original_spawn {
                    // A changed request, authority, budget, graph or any other
                    // spawn input is a conflict; do not replace it from storage.
                    assert_eq!(intent.spawn_digest(), expected);
                }
                let replay_before = if index == 0 && matches!(&self.mode, PartialMode::Recover(_)) {
                    Some((
                        business_facts(
                            snapshot(&self.pool, activation.tenant_id()).await,
                            activation.run_id(),
                        ),
                        self.store
                            .load_child_budget_account(activation.tenant_id(), activation.run_id())
                            .await
                            .unwrap()
                            .unwrap()
                            .canonical_bytes()
                            .unwrap(),
                    ))
                } else {
                    None
                };
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
                        candidate_run,
                        EventId::generate(),
                        payload(AgentAdmission::JOURNAL_EVENT_KIND),
                    )
                    .unwrap(),
                )
                .unwrap();
                let checkpoint = CheckpointWrite::initial(
                    activation.tenant_id().clone(),
                    candidate_run,
                    CheckpointId::generate(),
                    self.child_graph.reference(),
                    child_state,
                    self.child_graph.entry_nodes().clone(),
                )
                .unwrap();
                let outcome = Box::pin(self.store.admit_child_run(
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
                    checkpoint,
                    direct,
                    &self.schemas,
                ))
                .await
                .unwrap();
                if index == 0 {
                    match &self.mode {
                        PartialMode::ParkAfterFirst => {
                            assert!(matches!(outcome, ChildRunCommitOutcome::Committed(_)));
                            assert_eq!(outcome.record().intent(), &intent);
                            assert!(
                                self.store
                                    .load_child_join(key.parent())
                                    .await
                                    .unwrap()
                                    .is_none()
                            );
                            let account = self
                                .store
                                .load_child_budget_account(
                                    activation.tenant_id(),
                                    activation.run_id(),
                                )
                                .await
                                .unwrap()
                                .unwrap();
                            assert_eq!(account.children().len(), 1);
                            assert!(account.children()[0].settlement().is_none());
                            assert_eq!(self.model_calls.load(Ordering::SeqCst), 0);
                            assert_eq!(self.tool_calls.load(Ordering::SeqCst), 0);
                            publish_ready(
                                &json!({"phase":"partial_admission_committed", "key":key,
                                "fence":context.attempt().fence(), "frame":active.checkpoint().head(), "pid":std::process::id()}),
                            );
                            // Driver, its guardian and every Store handle stay alive until
                            // the controller SIGKILLs this actual OS actor. No unwind/drop.
                            std::future::pending::<()>().await;
                            unreachable!("only OS kill may leave the partial-spawn park");
                        }
                        PartialMode::Recover(first) => {
                            assert!(matches!(outcome, ChildRunCommitOutcome::Idempotent(_)));
                            assert_eq!(immutable_child(outcome.record()), first.immutable);
                            assert_eq!(
                                outcome.record().intent().spawn_digest(),
                                intent.spawn_digest()
                            );
                            assert_ne!(
                                outcome.record().intent().child().provenance().run_id(),
                                candidate_run
                            );
                            assert_ne!(
                                outcome.record().intent().child().provenance().thread_id(),
                                intent.child().provenance().thread_id()
                            );
                            assert_ne!(
                                outcome
                                    .record()
                                    .intent()
                                    .child()
                                    .provenance()
                                    .invocation_id(),
                                intent.child().provenance().invocation_id()
                            );
                            assert_ne!(
                                outcome.record().intent().canonical_bytes().unwrap(),
                                intent.canonical_bytes().unwrap()
                            );
                            assert!(matches!(
                                self.store
                                    .load_run(activation.tenant_id(), candidate_run)
                                    .await,
                                Err(StoreError::RunNotFound)
                            ));
                            let account = self
                                .store
                                .load_child_budget_account(
                                    activation.tenant_id(),
                                    activation.run_id(),
                                )
                                .await
                                .unwrap()
                                .unwrap();
                            assert_eq!(account.children().len(), 1);
                            assert_eq!(
                                serde_json::to_value(account.entry(key).unwrap()).unwrap(),
                                first.account_entry
                            );
                            let (before_rows, before_account) = replay_before.unwrap();
                            assert_eq!(account.canonical_bytes().unwrap(), before_account);
                            assert_eq!(
                                business_facts(
                                    snapshot(&self.pool, activation.tenant_id()).await,
                                    activation.run_id()
                                ),
                                before_rows,
                                "idempotent admission must not append any tenant fact or reserve again"
                            );
                            assert!(
                                self.store
                                    .load_child_join(key.parent())
                                    .await
                                    .unwrap()
                                    .is_none()
                            );
                        }
                    }
                } else {
                    assert!(matches!(outcome, ChildRunCommitOutcome::Committed(_)));
                    assert_eq!(outcome.record().intent(), &intent);
                    assert_eq!(
                        outcome.record().spawn().source().worker_fence(),
                        Some(context.attempt().fence())
                    );
                }
            }
            let account = self
                .store
                .load_child_budget_account(activation.tenant_id(), activation.run_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(account.children().len(), 2);
            for key in &keys {
                assert!(account.entry(key).unwrap().settlement().is_none());
            }
            Ok(GraphNodeExecution::child_join(request))
        })
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn partial_registry(
    store: &PostgresStore,
    pool: &PgPool,
    fixture: &DriverFixture,
    leaf: &CompiledGraph,
    child: &ProviderNativeFixture,
    template: &AgentAdmissionIntent,
    schemas: &JsonSchemaRegistry,
    direct: &Arc<PartialDirectEvidence>,
    counters: &FreshCounters,
    mode: &PartialMode,
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
                .is_some_and(|policy| policy.call(node.node_id()).is_some())
            {
                continue;
            }
            if graph == leaf && node.node_id().as_str() == "Step_A" {
                builder
                    .register_node(Arc::new(PartialExecutor {
                        store: store.clone(),
                        pool: pool.clone(),
                        graph: graph.reference(),
                        node: node.node_id().clone(),
                        template: template.clone(),
                        child_graph: child.definition.graph().clone(),
                        child_state: child.definition.initial_state().unwrap(),
                        schemas: schemas.clone(),
                        direct: Arc::clone(direct),
                        mode: mode.clone(),
                        launched: Arc::clone(&counters.launched),
                        resumed: Arc::clone(&counters.resumed),
                        model_calls: Arc::clone(&child.model_calls),
                        tool_calls: Arc::clone(&child.tool_calls),
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
    }
    builder.build().unwrap()
}

async fn observer_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}

async fn lease_expired(pool: &PgPool, tenant: &TenantId, run: RunId) -> bool {
    // A database-clock observation only: no lease mutation, forced time or SQL expiry.
    query_scalar("SELECT clock_timestamp() >= lease_expires_at FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2")
        .bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(pool).await.unwrap()
}

async fn claim_after_expiry(store: &PostgresStore, pool: &PgPool, first: &FirstProof) -> RunFence {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let original = store
            .load_run(first.key.tenant_id(), first.key.parent_run_id())
            .await
            .unwrap();
        assert_eq!(original.lease().unwrap().fence(), &first.fence);
        if lease_expired(pool, first.key.tenant_id(), first.key.parent_run_id()).await {
            match store
                .claim_lease(
                    first.key.tenant_id(),
                    first.key.parent_run_id(),
                    AttemptId::generate(),
                )
                .await
            {
                Ok(claim) => return claim.lease().fence().clone(),
                // A renewal already sent before SIGKILL may finish on the
                // backend after an earlier read. Re-observe actual expiry;
                // never force time, release the old fence or ignore an error.
                Err(StoreError::LeaseHeld) => {}
                Err(error) => panic!("cold claim after actual expiry: {error:?}"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "original process lease never became claimable"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn count_kind(store: &PostgresStore, tenant: &TenantId, run: RunId, kind: &str) -> usize {
    let page = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
        .await
        .unwrap();
    assert!(!page.has_more());
    page.events()
        .iter()
        .filter(|event| event.payload().kind().as_str() == kind)
        .count()
}

#[allow(clippy::too_many_lines)]
async fn actor(input: &Value) {
    let resume = parent_control();
    let store = test_store_with_lease_duration(Duration::from_secs(2))
        .await
        .unwrap();
    let pool = observer_pool().await;
    let tenant: TenantId = serde_json::from_value(input["tenant"].clone()).unwrap();
    let run: RunId = serde_json::from_value(input["run"].clone()).unwrap();
    let root_checkpoint: Checkpoint =
        serde_json::from_value(input["root_checkpoint"].clone()).unwrap();
    let native = provider_native_fixture_with_budget(
        store.clone(),
        false,
        false,
        false,
        None,
        NativeBudgetMode::Durable,
    );
    let (fixture, leaf) = two_slot_graphs(&native);
    let schemas = fresh_schemas(&fixture, &leaf, &native);
    let parent = store.load_agent_admission(&tenant, run).await.unwrap();
    assert_eq!(parent.checkpoint(), &root_checkpoint);
    assert_eq!(
        parent.admission().intent().graph(),
        &fixture.graph.reference()
    );
    let template = child_template(parent.admission().intent(), &native);
    let mode = match input["role"].as_str().unwrap() {
        "first" => PartialMode::ParkAfterFirst,
        "recover" => PartialMode::Recover(Arc::new(
            serde_json::from_value(input["proof"].clone()).unwrap(),
        )),
        other => panic!("unexpected actor role: {other}"),
    };
    let counters = FreshCounters::new();
    let direct = Arc::new(PartialDirectEvidence {
        store: store.clone(),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let registry = partial_registry(
        &store, &pool, &fixture, &leaf, &native, &template, &schemas, &direct, &counters, &mode,
    );
    let fence = match &mode {
        PartialMode::ParkAfterFirst => store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone(),
        PartialMode::Recover(first) => claim_after_expiry(&store, &pool, first).await,
    };
    let driver = DurableGraphDriver::new(store.clone(), registry, short_driver_options())
        .unwrap()
        .with_direct_usage_evidence(direct);
    let suspended = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await
        .expect("partial-spawn recovery requires the implemented fresh scoped Driver profile");
    let PartialMode::Recover(first) = mode else {
        panic!("first actor passed its required kill point");
    };
    assert!(fence.epoch() > first.fence.epoch());
    let GraphDriveOutcome::ChildJoin(request) = suspended.outcome() else {
        panic!("expected scoped Join: {suspended:?}");
    };
    assert_eq!(request.keys().len(), 2);
    assert!(request.keys().contains(&first.key));
    let keys = request.keys().to_vec();
    let registered = store
        .load_child_join(first.key.parent())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(registered.request(), request.as_ref());
    assert_eq!(
        registered.registration().source().worker_fence(),
        Some(&fence)
    );
    assert!(
        registered.head().is_none()
            && registered.binding().is_none()
            && registered.consumed().is_none()
    );
    let live = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(live.lifecycle().status(), RunStatus::Active);
    assert!(live.lease().is_none() && live.lifecycle().waits().is_none());
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
        Some(&root_checkpoint)
    );
    assert_eq!(
        store
            .load_active_graph_frame(&tenant, run)
            .await
            .unwrap()
            .unwrap()
            .checkpoint()
            .head(),
        first.frame
    );
    assert_eq!(
        immutable_child(&store.load_child_run(&first.key).await.unwrap()),
        first.immutable
    );
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
    let mut usage = BudgetUsage::zero();
    for key in &keys {
        let owned = store.load_child_run(key).await.unwrap();
        let child = owned.intent().child().provenance().run_id();
        let child_fence = store
            .claim_lease(&tenant, child, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let loop_ = DurableAgentLoop::new(
            store.clone(),
            native.registry.clone(),
            Arc::new(ProviderNativeAgentLifecycleEvidence::new(
                native.definition.clone(),
                store.clone(),
            )),
            short_driver_options(),
            DurableGraphLifecycleOptions::default(),
        )
        .unwrap();
        let result = loop_
            .run(child_fence, CancellationSignal::never())
            .await
            .unwrap();
        assert!(
            matches!(result.outcome(), AgentLoopOutcome::Succeeded(_)),
            "real child: {result:?}"
        );
        assert!(
            result.report().node_attempts_started() > 0
                && result.report().node_attempts_completed() > 0
        );
        let stored = store.load_run(&tenant, child).await.unwrap();
        assert_eq!(stored.lifecycle().status(), RunStatus::Succeeded);
        assert!(stored.lease().is_none());
        let child_usage = stored.lifecycle().terminal_usage().unwrap();
        assert_eq!(child_usage.model_turns().get(), 2);
        assert_eq!(child_usage.tool_calls().get(), 1);
        assert_eq!(child_usage.unpriced_cost_events(), ExecutionCount::ZERO);
        usage = usage
            .checked_accumulate(&child_usage.cumulative_only())
            .unwrap();
    }
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 4);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 2);
    let reconciled = DurableChildReconciler::new(
        store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(reconciled.items().len(), 2);
    for item in reconciled.items() {
        item.result().unwrap();
    }
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
    let published_join = store
        .load_child_join(first.key.parent())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published_join.registration(), registered.registration());
    let publication = published_join.head().unwrap().clone();
    assert_eq!(
        published_join.binding().unwrap().request().keys(),
        keys.as_slice()
    );
    assert_eq!(published_join.binding().unwrap().terminals().len(), 2);
    let account = store
        .load_child_budget_account(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 2);
    assert_eq!(account.delegated_usage().unwrap(), usage);
    let mut original_entry = serde_json::to_value(account.entry(&first.key).unwrap()).unwrap();
    assert!(!original_entry["settlement"].is_null());
    original_entry["settlement"] = Value::Null;
    assert_eq!(original_entry, first.account_entry);
    for entry in account.children() {
        let child = store.load_child_run(entry.key()).await.unwrap();
        assert_eq!(entry.child(), child.intent().child().provenance());
        assert_eq!(entry.spawn_digest(), child.intent().spawn_digest());
        assert_eq!(entry.settlement(), child.settlement());
        assert!(entry.settlement().is_some());
    }
    let account_bytes = account.canonical_bytes().unwrap();
    let before_resume = snapshot(&pool, &tenant).await;
    drop(driver);
    // The original killed lease and takeover used the public two-second
    // profile. Once publication permits F3, use the ordinary thirty-second
    // profile for complete post-consumption observations and same-fence retry.
    let cold = test_store().await.unwrap();
    let cold_direct = Arc::new(PartialDirectEvidence {
        store: cold.clone(),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let registry = partial_registry(
        &cold,
        &pool,
        &fixture,
        &leaf,
        &native,
        &template,
        &schemas,
        &cold_direct,
        &counters,
        &PartialMode::Recover(first.clone()),
    );
    let f3 = cold
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(f3.epoch() > fence.epoch());
    let cold_driver =
        DurableGraphDriver::new(cold.clone(), registry, DurableGraphDriverOptions::default())
            .unwrap()
            .with_direct_usage_evidence(cold_direct.clone());
    let completed = cold_driver
        .drive(f3.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = completed.outcome() else {
        panic!("Root handoff: {completed:?}");
    };
    assert_eq!(counters.launched.load(Ordering::SeqCst), 1);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(counters.leaf_tail.load(Ordering::SeqCst), 1);
    assert_eq!(counters.root_tail.load(Ordering::SeqCst), 1);
    assert_eq!(completed.report().frame_returns_committed(), 1);
    let consumed = cold
        .load_child_join(first.key.parent())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(consumed.registration(), registered.registration());
    assert_eq!(consumed.head(), Some(&publication));
    let consumption = consumed.consumed().unwrap();
    let pending = cold
        .load_pending_node_result(first.key.parent())
        .await
        .unwrap();
    assert_eq!(pending.journal_head(), consumption);
    assert_eq!(pending.intent().child_join(), Some(&publication));
    // The provider permits at most two decoded attempts per page. Keep
    // the full three-epoch oracle by following its exact full-record cursor.
    let mut history = Vec::new();
    let mut cursor = None;
    for page_index in 0..MAX_HISTORY_PAGES {
        let page = cold
            .load_node_attempt_history_page(
                first.key.parent(),
                cursor.as_ref(),
                NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !page.records().is_empty(),
            "three-attempt history cannot end in an empty page"
        );
        let next = page.next_cursor();
        if let (Some(previous), Some(next)) = (cursor.as_ref(), next.as_ref()) {
            assert!(
                next.start().journal_head().sequence() > previous.start().journal_head().sequence()
            );
        }
        history.extend_from_slice(page.records());
        if !page.has_more() {
            break;
        }
        assert!(
            page_index + 1 < MAX_HISTORY_PAGES,
            "physical history exceeds the expected bounded three attempts"
        );
        assert!(
            next.is_some(),
            "a nonfinal page must provide the exact next cursor"
        );
        cursor = next;
    }
    assert_eq!(history.len(), 3);
    for (index, expected) in [&first.fence, &fence, &f3].into_iter().enumerate() {
        assert_eq!(history[index].start().fence(), expected);
        assert_eq!(
            history[index].status(),
            if index == 2 {
                NodeAttemptStatus::Succeeded
            } else {
                NodeAttemptStatus::Executing
            }
        );
        if index < 2 {
            assert!(history[index].completion().is_none());
        }
    }
    assert_eq!(history[2].completion().unwrap().journal_head(), consumption);
    let returned = cold
        .load_graph_frame_return(&tenant, run, first.key.parent().graph_namespace())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(returned.completion().start().fence(), &f3);
    assert_eq!(
        returned.completion().start().activation(),
        first.caller.activation()
    );
    assert_eq!(
        returned.completion().outcome().result(),
        Some(&returned.result().head())
    );
    assert_eq!(
        immutable_entry(
            &cold
                .load_graph_frame_entry(&tenant, run, first.key.parent().graph_namespace())
                .await
                .unwrap()
        ),
        first.entry
    );
    assert!(
        cold.load_active_graph_frame(&tenant, run)
            .await
            .unwrap()
            .is_none()
    );
    let final_parent = cold.load_run(&tenant, run).await.unwrap();
    assert_eq!(final_parent.lifecycle().status(), RunStatus::Active);
    assert!(final_parent.lifecycle().result().is_none());
    assert_eq!(final_parent.checkpoint().unwrap().superstep().get(), 1);
    for key in &keys {
        let child = cold
            .load_child_run(key)
            .await
            .unwrap()
            .intent()
            .child()
            .provenance()
            .run_id();
        assert_eq!(
            child_facts(before_resume.clone(), child),
            child_facts(snapshot(&pool, &tenant).await, child)
        );
    }
    let direct_usage = cold_direct
        .usage_at(&tenant, run, final_parent.journal_head().unwrap())
        .await
        .unwrap();
    assert_eq!(direct_usage.model_turns(), ExecutionCount::ZERO);
    assert_eq!(direct_usage.tool_calls(), ExecutionCount::ZERO);
    assert_eq!(
        cold.include_child_usage(&tenant, run, direct_usage.clone())
            .await
            .unwrap(),
        direct_usage.checked_accumulate(&usage).unwrap()
    );
    assert_eq!(
        cold.load_child_budget_account(&tenant, run)
            .await
            .unwrap()
            .unwrap()
            .canonical_bytes()
            .unwrap(),
        account_bytes
    );
    for (kind, expected) in [
        ("child-run-admitted", 2),
        ("child-run-settled", 2),
        ("child-join-registered", 1),
        ("child-join-published", 1),
        ("graph-frame-entered", 1),
        ("graph-frame-returned", 1),
        ("graph-frame-caller-rebound", 2),
    ] {
        assert_eq!(
            count_kind(&cold, &tenant, run, kind).await,
            expected,
            "once journal kind: {kind}"
        );
    }
    let stable = business_facts(snapshot(&pool, &tenant).await, run);
    let repeat = cold_driver
        .drive(f3, CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(repeated) = repeat.outcome() else {
        panic!("repeat handoff: {repeat:?}");
    };
    assert_eq!(
        repeated.plan().barrier().base_checkpoint(),
        handoff.plan().barrier().base_checkpoint()
    );
    assert_eq!(repeat.report().node_attempts_started(), 0);
    assert_eq!(repeat.report().frame_returns_committed(), 0);
    assert_eq!(counters.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(native.model_calls.load(Ordering::SeqCst), 4);
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 2);
    assert_eq!(stable, business_facts(snapshot(&pool, &tenant).await, run));
    publish_ready(
        &json!({"phase":"partial_spawn_recovered", "fence":fence, "resume_fence":history[2].start().fence(),
        "original":first.immutable, "keys":keys, "model_calls":4, "tool_calls":2, "children":2,
        "registered_once":true, "consumed_once":true, "returned_once":true, "root_active_handoff":true, "pid":std::process::id()}),
    );
    resume.await.unwrap();
    pool.close().await;
    cold.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn two_slot_partial_spawn_survives_real_process_loss_without_duplicate_accounts() {
    if let Ok(input) = std::env::var(INPUT_ENV) {
        Box::pin(actor(&serde_json::from_str::<Value>(&input).unwrap())).await;
        return;
    }
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let pool = observer_pool().await;
    let native = provider_native_fixture_with_budget(
        store.clone(),
        false,
        false,
        false,
        None,
        NativeBudgetMode::Durable,
    );
    let (fixture, leaf) = two_slot_graphs(&native);
    let schemas = fresh_schemas(&fixture, &leaf, &native);
    let tenant = tenant("scoped-child-join-g2-partial-process");
    let ids = AgentRunIds::generate();
    let run = ids.run_id();
    // The controller only admits Root. Both child admissions must originate
    // inside the actual scoped executor under Driver-owned physical Starts.
    let request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let counters = FreshCounters::new();
    let direct = Arc::new(PartialDirectEvidence {
        store: store.clone(),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let candidate = child_template(request.intent(), &native);
    let deployment = partial_registry(
        &store,
        &pool,
        &fixture,
        &leaf,
        &native,
        &candidate,
        &schemas,
        &direct,
        &counters,
        &PartialMode::ParkAfterFirst,
    );
    for graph in [&fixture.graph, &leaf, native.definition.graph()] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), deployment)
        .unwrap()
        .admit(request)
        .await
        .unwrap()
        .stored()
        .clone();
    let mut first_actor = TestProcess::spawn(
        PARTIAL_SELECTED,
        &json!({"role":"first", "tenant":tenant,
        "run":run, "root_checkpoint":admitted.checkpoint()}),
    );
    let ready = first_actor
        .ready()
        .await
        .expect("first actual scoped callback must commit slot1 and remain parked");
    assert_eq!(ready["phase"], "partial_admission_committed");
    let first_pid = ready["pid"].as_u64().unwrap();
    assert_ne!(first_pid, u64::from(std::process::id()));
    let key: ChildRunKey = serde_json::from_value(ready["key"].clone()).unwrap();
    let fence: RunFence = serde_json::from_value(ready["fence"].clone()).unwrap();
    let frame = serde_json::from_value(ready["frame"].clone()).unwrap();
    assert_eq!(key.slot().as_str(), SLOTS[0]);
    assert_eq!(key.parent_run_id(), run);
    assert!(!key.parent().graph_namespace().is_root());
    let record = store.load_child_run(&key).await.unwrap();
    assert_eq!(record.spawn().source().worker_fence(), Some(&fence));
    assert_eq!(record.child().run().lifecycle().status(), RunStatus::Active);
    assert!(record.settlement().is_none());
    assert!(store.load_child_join(key.parent()).await.unwrap().is_none());
    assert_eq!(
        store
            .list_child_run_identities(&tenant, run)
            .await
            .unwrap()
            .len(),
        1
    );
    let account = store
        .load_child_budget_account(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    let active = store
        .load_active_graph_frame(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.caller().fence(), &fence);
    assert_eq!(active.entry().entry().start().fence(), &fence);
    assert_eq!(active.checkpoint().head(), frame);
    let proof = FirstProof {
        key: key.clone(),
        fence: fence.clone(),
        frame,
        immutable: immutable_child(&record),
        account_entry: serde_json::to_value(account.entry(&key).unwrap()).unwrap(),
        caller: active.caller().head(),
        entry: immutable_entry(active.entry()),
    };
    println!(
        "G2_PARTIAL_ORIGINAL {}",
        json!({"key":key, "fence":fence, "first_pid":first_pid,
        "proof_digest":Digest::sha256(serde_json_canonicalizer::to_vec(&proof).unwrap())})
    );
    let at_cut = business_facts(snapshot(&pool, &tenant).await, run);
    let killed = first_actor.kill_and_reap().await;
    assert!(!killed.success());
    assert_eq!(business_facts(snapshot(&pool, &tenant).await, run), at_cut);
    let original_lease = store
        .load_run(&tenant, run)
        .await
        .unwrap()
        .lease()
        .unwrap()
        .clone();
    assert_eq!(original_lease.fence(), &fence);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !lease_expired(&pool, &tenant, run).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "actual short public lease never expired"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let expired_lease = store
        .load_run(&tenant, run)
        .await
        .unwrap()
        .lease()
        .unwrap()
        .clone();
    assert_eq!(expired_lease.fence(), &fence);
    assert_eq!(business_facts(snapshot(&pool, &tenant).await, run), at_cut);
    println!(
        "G2_PARTIAL_REAL_KILL first_child_committed=true join_absent=true lease_actually_expired=true first_pid={first_pid} original_expiry={} observed_expiry={} killed={killed:?}",
        original_lease.expires_at(),
        expired_lease.expires_at()
    );
    let mut cold_actor = TestProcess::spawn(
        PARTIAL_SELECTED,
        &json!({"role":"recover", "tenant":tenant,
        "run":run, "root_checkpoint":admitted.checkpoint(), "proof":proof}),
    );
    let completed = cold_actor
        .ready()
        .await
        .expect("fresh OS process must complete actual partial-spawn recovery");
    assert_eq!(completed["phase"], "partial_spawn_recovered");
    let cold_pid = completed["pid"].as_u64().unwrap();
    assert_ne!(cold_pid, first_pid);
    assert_ne!(cold_pid, u64::from(std::process::id()));
    assert_eq!(completed["original"], proof.immutable);
    assert_eq!(completed["model_calls"], 4);
    assert_eq!(completed["tool_calls"], 2);
    assert_eq!(completed["children"], 2);
    for name in [
        "registered_once",
        "consumed_once",
        "returned_once",
        "root_active_handoff",
    ] {
        assert_eq!(completed[name], true);
    }
    assert_eq!(
        immutable_child(&store.load_child_run(&key).await.unwrap()),
        proof.immutable
    );
    assert_eq!(
        store
            .list_child_run_identities(&tenant, run)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        native.model_calls.load(Ordering::SeqCst),
        0,
        "controller never dispatches child I/O"
    );
    assert_eq!(native.tool_calls.load(Ordering::SeqCst), 0);
    cold_actor.resume();
    cold_actor.wait_success().await;
    println!(
        "G2_PARTIAL_COMPLETE real_process_loss=true two_slots=true original_child_retained=true accounts_once=true first_pid={first_pid} cold_pid={cold_pid}"
    );
    pool.close().await;
    store.close().await;
}
