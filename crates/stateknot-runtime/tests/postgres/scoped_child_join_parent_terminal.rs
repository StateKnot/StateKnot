// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Complete the real fresh scoped Join through the parent's terminal transaction.
//! Parent nodes perform no provider IO; the settled Native child supplies the
//! actual two model turns and one tool call, included once by the coordinator.
use super::*;

struct ParentTerminalEvidence {
    direct: Arc<FreshDirectEvidence>,
    terminal_calls: Arc<AtomicUsize>,
}

impl ParentTerminalEvidence {
    // Extend the existing closed Root/frame journal ledger with the actual
    // materialized terminal node outputs. Re-reading an original result does
    // not materialize another application output or incur another charge.
    async fn usage_at(
        &self,
        tenant: &TenantId,
        run: RunId,
        head: &JournalHead,
    ) -> Result<BudgetUsage, GraphLifecycleEvidenceError> {
        let direct = self.direct.usage_at(tenant, run, head).await?;
        let page = self
            .direct
            .store
            .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
            .await
            .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
        if page.has_more() || page.events().last().map(JournalEvent::head).as_ref() != Some(head) {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let mut activations = Vec::new();
        let mut output_bytes = 0_u64;
        for event in page.events() {
            if event.payload().kind().as_str() != "graph-node-attempt-succeeded" {
                continue;
            }
            let attempt_id = event.payload().data().as_value()["attempt_id"]
                .as_str()
                .ok_or(GraphLifecycleEvidenceError::Corrupt)?
                .parse()
                .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
            let attempt = self
                .direct
                .store
                .load_node_attempt(tenant, &run, attempt_id)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let activation = attempt.start().activation();
            let completion = attempt
                .completion()
                .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
            let result = self
                .direct
                .store
                .load_pending_node_result(activation)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            if attempt.status() != NodeAttemptStatus::Succeeded
                || completion.journal_head() != &event.head()
                || result.journal_head() != &event.head()
                || result.intent().activation() != activation
                || completion.outcome().result() != Some(&result.head())
                || activations.contains(activation)
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            activations.push(activation.clone());
            if let NodeControl::Terminal { output } = result.intent().control() {
                output_bytes = output_bytes
                    .checked_add(output.data().stats().compact_bytes() as u64)
                    .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
            }
        }
        direct
            .checked_accumulate(
                &BudgetUsage::builder()
                    .output_bytes(ByteCount::new(output_bytes))
                    .build()
                    .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?,
            )
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)
    }
}

impl GraphLifecycleEvidenceProvider for ParentTerminalEvidence {
    fn direct_usage_evidence(
        &self,
        context: GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphDirectUsageEvidence, GraphLifecycleEvidenceError>> {
        self.direct.calls.fetch_add(1, Ordering::SeqCst);
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
        self.terminal_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let tenant = context.provenance().tenant_id();
            let run = context.provenance().run_id();
            let stored = self
                .direct
                .store
                .load_run(tenant, run)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let admitted = self
                .direct
                .store
                .load_agent_admission(tenant, run)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let source = admitted.admission().intent();
            let checkpoint = self
                .direct
                .store
                .load_current_checkpoint(tenant, run)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?
                .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
            if source.provenance() != context.provenance()
                || source.graph() != context.graph()
                || context.graph() != &self.direct.root
                || stored.lifecycle().provenance() != source.provenance()
                || stored.lifecycle().status() != RunStatus::Active
                || stored.lifecycle().revision() != context.expected_revision()
                || checkpoint.head() != *context.checkpoint()
                || context.successor_superstep().get() != checkpoint.superstep().get() + 1
                || context.successor_checkpoint_id() == checkpoint.checkpoint_id()
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let activation =
                NodeActivation::for_ready_root(&checkpoint, NodeId::new("finish").unwrap())
                    .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
            let result = self
                .direct
                .store
                .load_pending_node_result(&activation)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let NodeControl::Terminal { output } = result.intent().control() else {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            };
            if output.schema() != context.output_schema()
                || output.digest() != context.output_digest()
                || result.intent().activation() != &activation
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let usage = self
                .usage_at(
                    tenant,
                    run,
                    stored
                        .journal_head()
                        .ok_or(GraphLifecycleEvidenceError::Corrupt)?,
                )
                .await?;
            // DIRECT remains model/tool zero. DurableGraphLifecycle adds the
            // authenticated settled child subtree before AgentResult validation.
            if usage.model_turns() != ExecutionCount::ZERO
                || usage.model_attempts() != ExecutionCount::ZERO
                || usage.tool_calls() != ExecutionCount::ZERO
                || usage.unpriced_cost_events() != ExecutionCount::ZERO
                || usage.output_bytes().get() < output.data().stats().compact_bytes() as u64
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            Ok(GraphTerminalEvidence::new(
                source.descriptor().clone(),
                source.request().clone(),
                source.budget().clone(),
                AgentArtifacts::empty(),
                usage,
            ))
        })
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

fn application_calls(counters: &FreshCounters, child: &ProviderNativeFixture) -> [usize; 6] {
    [
        counters.launched.load(Ordering::SeqCst),
        counters.resumed.load(Ordering::SeqCst),
        counters.leaf_tail.load(Ordering::SeqCst),
        counters.root_tail.load(Ordering::SeqCst),
        child.model_calls.load(Ordering::SeqCst),
        child.tool_calls.load(Ordering::SeqCst),
    ]
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn fresh_scoped_child_join_parent_succeeds_and_recovers_original_terminal_ack_once() {
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
    let tenant = tenant("scoped-child-join-g2-parent-terminal");
    let ids = AgentRunIds::generate();
    let run = ids.run_id();
    let request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let child_ids = AgentRunIds::generate();
    let child_intent = stateknot_core::AgentAdmissionIntent::new(
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
                json!({"question":"Run the real child to parent success."}),
            )
            .unwrap(),
            BudgetLimits::empty()
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_input_bytes(ByteCount::new(65_536))
                .with_output_bytes(ByteCount::new(65_536)),
        ),
        request.intent().budget_layers().iter().cloned(),
        native.definition.graph().reference(),
        request.intent().authority().clone(),
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
    let evidence = Arc::new(ParentTerminalEvidence {
        direct: Arc::clone(&direct),
        terminal_calls: Arc::new(AtomicUsize::new(0)),
    });
    let registry = fresh_registry(
        &store,
        &fixture,
        &leaf,
        &native,
        &child_intent,
        &schemas,
        &direct,
        &counters,
    );
    for graph in [&fixture.graph, &leaf, native.definition.graph()] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), registry.clone())
        .unwrap()
        .admit(request)
        .await
        .unwrap()
        .stored()
        .clone();
    let f1 = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let parent_loop = DurableAgentLoop::new(
        store.clone(),
        registry,
        evidence.clone(),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let suspended = parent_loop
        .run(f1.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let AgentLoopOutcome::ChildJoin(join) = suspended.outcome() else {
        panic!("first public parent loop must suspend on the real child: {suspended:?}");
    };
    assert_eq!(join.keys().len(), 1);
    let key = join.keys()[0].clone();
    assert_eq!(Some(&key), direct.key.lock().await.as_ref());
    assert!(!key.parent().graph_namespace().is_root());
    assert_eq!(application_calls(&counters, &native), [1, 0, 0, 0, 0, 0]);
    assert_eq!(evidence.terminal_calls.load(Ordering::SeqCst), 0);
    let suspended_parent = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(suspended_parent.lifecycle().status(), RunStatus::Active);
    assert!(suspended_parent.lease().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .as_ref(),
        Some(admitted.checkpoint())
    );
    let original_join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    let child_run = child_intent.provenance().run_id();
    let child_fence = store
        .claim_lease(&tenant, child_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
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
        .run(child_fence, CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        child_result.outcome(),
        AgentLoopOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let child = store.load_run(&tenant, child_run).await.unwrap();
    assert_eq!(child.lifecycle().status(), RunStatus::Succeeded);
    assert!(child.lease().is_none());
    let child_usage = child.lifecycle().terminal_usage().unwrap().clone();
    assert_eq!(child_usage.model_turns().get(), 2);
    assert_eq!(child_usage.tool_calls().get(), 1);
    assert_eq!(child_usage.unpriced_cost_events(), ExecutionCount::ZERO);
    assert_eq!(application_calls(&counters, &native), [1, 0, 0, 0, 2, 1]);
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
    assert!(published_join.consumed().is_none());
    assert_eq!(
        published_join.binding().unwrap().terminals()[0].usage(),
        &child_usage
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let settled_child = child_facts(snapshot(&pool, &tenant).await, child_run);
    let settled_account = store
        .load_child_budget_account(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled_account.children().len(), 1);
    assert!(settled_account.children()[0].settlement().is_some());
    assert_eq!(
        settled_account.delegated_usage().unwrap(),
        child_usage.cumulative_only()
    );
    drop(parent_loop);
    drop(child_loop);
    drop(evidence);
    drop(direct);

    // Reconnect and reconstruct every parent binding. Only the immutable key
    // is retained; the actual Join handle is recovered from durable ownership.
    let cold = test_store().await.unwrap();
    let cold_direct = Arc::new(FreshDirectEvidence {
        store: cold.clone(),
        key: Arc::new(tokio::sync::Mutex::new(Some(key.clone()))),
        root: fixture.graph.reference(),
        leaf: leaf.reference(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let terminal_calls = Arc::new(AtomicUsize::new(0));
    let cold_evidence = Arc::new(ParentTerminalEvidence {
        direct: Arc::clone(&cold_direct),
        terminal_calls: Arc::clone(&terminal_calls),
    });
    let rebuilt = fresh_registry(
        &cold,
        &fixture,
        &leaf,
        &native,
        &child_intent,
        &schemas,
        &cold_direct,
        &counters,
    );
    let f2 = cold
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(f2.epoch() > f1.epoch());
    let driver = DurableGraphDriver::new(
        cold.clone(),
        rebuilt.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(cold_evidence.clone());
    let before_old_fence = snapshot(&pool, &tenant).await;
    let obsolete_authority_result = driver.drive(f1.clone(), CancellationSignal::never()).await;
    assert!(matches!(
        obsolete_authority_result,
        Err(GraphDriverError::Store { source }) if matches!(source.as_ref(), StoreError::StaleFence)
    ));
    assert_eq!(before_old_fence, snapshot(&pool, &tenant).await);
    assert_eq!(application_calls(&counters, &native), [1, 0, 0, 0, 2, 1]);
    assert_eq!(cold_direct.calls.load(Ordering::SeqCst), 0);
    assert_eq!(terminal_calls.load(Ordering::SeqCst), 0);
    let handoff_result = driver
        .drive(f2.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let (outcome, report) = handoff_result.into_parts();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = outcome else {
        panic!("cold F2 must reach the actual Root terminal handoff: {outcome:?}");
    };
    let handoff = *handoff;
    assert_eq!(report.frame_returns_committed(), 1);
    assert_eq!(application_calls(&counters, &native), [1, 1, 1, 1, 2, 1]);
    let consumed = cold.load_child_join(key.parent()).await.unwrap().unwrap();
    assert_eq!(consumed.head(), Some(&publication));
    assert_eq!(consumed.registration(), original_join.registration());
    let consumed_head = consumed.consumed().unwrap().clone();
    let history = cold
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 2);
    assert_eq!(history.records()[0].start().fence(), &f1);
    assert!(history.records()[0].completion().is_none());
    assert_eq!(history.records()[1].start().fence(), &f2);
    assert_eq!(
        history.records()[1].completion().unwrap().journal_head(),
        &consumed_head
    );
    let returned = cold
        .load_graph_frame_return(&tenant, run, key.parent().graph_namespace())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(returned.completion().start().fence(), &f2);
    assert!(
        cold.load_active_graph_frame(&tenant, run)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        settled_child,
        child_facts(snapshot(&pool, &tenant).await, child_run)
    );
    let GraphBarrierDisposition::Terminal { output } = handoff.plan().disposition() else {
        panic!("the actual Root plan must contain its final output");
    };
    let original_output = output.clone();
    let before_terminal = cold.load_run(&tenant, run).await.unwrap();
    assert_eq!(before_terminal.lifecycle().status(), RunStatus::Active);
    assert!(before_terminal.lifecycle().result().is_none());
    let direct_usage = cold_evidence
        .usage_at(&tenant, run, handoff.journal_head())
        .await
        .unwrap();
    assert_eq!(direct_usage.model_turns(), ExecutionCount::ZERO);
    assert_eq!(direct_usage.model_attempts(), ExecutionCount::ZERO);
    assert_eq!(direct_usage.tool_calls(), ExecutionCount::ZERO);
    // The leaf's Step.B and the Root finish each materialized the same real
    // fixture output once. No child output bytes enter this DIRECT ledger.
    assert_eq!(
        direct_usage.output_bytes().get(),
        2 * original_output.data().stats().compact_bytes() as u64
    );
    let expected_total = direct_usage
        .checked_accumulate(&child_usage.cumulative_only())
        .unwrap();
    let coordinator = DurableGraphLifecycle::new(
        cold.clone(),
        rebuilt.clone(),
        cold_evidence.clone(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    // This is the public coordinator operation invoked by AgentLoop for this
    // exact handoff. Retain the original occurrence to test terminal ACK retry.
    let committed = coordinator.commit_barrier(handoff.clone()).await.unwrap();
    let GraphBarrierLifecycleOutcome::Succeeded(receipt) = committed else {
        panic!("parent must actually commit Succeeded: {committed:?}");
    };
    assert!(matches!(&receipt, BarrierCommitOutcome::Committed { .. }));
    assert_eq!(terminal_calls.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.event().event_id(), handoff.event_id());
    assert_eq!(receipt.event().source().worker_fence(), Some(&f2));
    let parent = cold.load_run(&tenant, run).await.unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Succeeded);
    assert!(parent.lease().is_none());
    assert!(parent.scheduler_ready_at().is_none());
    assert!(parent.scheduler_not_before().is_none());
    assert!(!parent.is_quarantined());
    let result = parent.lifecycle().result().unwrap();
    assert_eq!(
        result.provenance(),
        admitted.admission().intent().provenance()
    );
    assert_eq!(result.output_schema(), original_output.schema());
    assert_eq!(result.output(), original_output.data());
    assert_eq!(result.usage(), &expected_total);
    assert_eq!(result.usage().model_turns().get(), 2);
    assert_eq!(result.usage().tool_calls().get(), 1);
    assert_eq!(result.usage().unpriced_cost_events(), ExecutionCount::ZERO);
    assert_eq!(parent.lifecycle().terminal_usage(), Some(&expected_total));
    assert_eq!(parent.journal_head(), Some(&receipt.event().head()));
    assert_eq!(
        cold.load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .as_ref(),
        Some(receipt.checkpoint())
    );
    let original_result_bytes = serde_json_canonicalizer::to_vec(result).unwrap();
    let final_account = cold
        .load_child_budget_account(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_account.children(), settled_account.children());
    assert_eq!(
        final_account.delegated_usage().unwrap(),
        child_usage.cumulative_only()
    );
    assert_eq!(final_account.direct_head(), &receipt.event().head());
    assert_eq!(final_account.direct_usage(), &direct_usage);
    let root_receipt = cold
        .load_root_graph_barrier(handoff.plan().barrier().base_checkpoint())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root_receipt.event(), receipt.event());
    assert_eq!(root_receipt.checkpoint(), receipt.checkpoint());
    assert_eq!(
        root_receipt.committed_lifecycle().status(),
        RunStatus::Succeeded
    );
    assert_eq!(
        serde_json_canonicalizer::to_vec(root_receipt.committed_lifecycle().result().unwrap())
            .unwrap(),
        original_result_bytes
    );
    assert_eq!(
        settled_child,
        child_facts(snapshot(&pool, &tenant).await, child_run)
    );
    let terminal_snapshot = snapshot(&pool, &tenant).await;
    drop(coordinator);
    drop(driver);
    drop(cold_evidence);

    let retry_store = test_store().await.unwrap();
    let retry_coordinator = DurableGraphLifecycle::new(
        retry_store.clone(),
        rebuilt.clone(),
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    // A fresh coordinator recovers the complete original receipt without
    // evidence callbacks, dispatch, new accounting, or a newly minted event.
    for _ in 0..2 {
        let retry = retry_coordinator
            .commit_barrier(handoff.clone())
            .await
            .unwrap();
        let GraphBarrierLifecycleOutcome::Succeeded(ack) = retry else {
            panic!("original terminal handoff must recover its Succeeded ACK: {retry:?}");
        };
        assert!(matches!(&ack, BarrierCommitOutcome::Idempotent { .. }));
        assert_eq!(ack.event(), receipt.event());
        assert_eq!(ack.checkpoint(), receipt.checkpoint());
        assert_eq!(terminal_snapshot, snapshot(&pool, &tenant).await);
        assert_eq!(application_calls(&counters, &native), [1, 1, 1, 1, 2, 1]);
        assert_eq!(terminal_calls.load(Ordering::SeqCst), 1);
    }
    let terminal_loop = DurableAgentLoop::new(
        retry_store.clone(),
        rebuilt,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    // Startup observes the live lease before recovery. A terminal Run fails
    // its runnable guard before lease/fence authority or application dispatch.
    for fence in [f1, f2] {
        let retry = terminal_loop.run(fence, CancellationSignal::never()).await;
        assert!(matches!(
            retry,
            Err(AgentLoopError::Driver { source })
                if matches!(source.as_ref(), GraphDriverError::Store { source }
                    if matches!(source.as_ref(), StoreError::RunNotRunnable))
        ));
        assert_eq!(terminal_snapshot, snapshot(&pool, &tenant).await);
        assert_eq!(application_calls(&counters, &native), [1, 1, 1, 1, 2, 1]);
    }
    assert!(matches!(
        retry_store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    assert_eq!(terminal_snapshot, snapshot(&pool, &tenant).await);
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            retry_store
                .load_run(&tenant, run)
                .await
                .unwrap()
                .lifecycle()
                .result()
                .unwrap()
        )
        .unwrap(),
        original_result_bytes
    );
    assert_eq!(
        retry_store
            .load_child_join(key.parent())
            .await
            .unwrap()
            .unwrap()
            .consumed(),
        Some(&consumed_head)
    );
    assert_eq!(
        retry_store
            .load_child_budget_account(&tenant, run)
            .await
            .unwrap()
            .unwrap(),
        final_account
    );
    let repeated_settlement = DurableChildReconciler::new(
        retry_store.clone(),
        schemas.clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert!(repeated_settlement.items().is_empty());
    assert_eq!(terminal_snapshot, snapshot(&pool, &tenant).await);
    let repeated_publication = DurableChildJoinPublisher::new(
        retry_store.clone(),
        schemas,
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(tenant.clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert!(repeated_publication.items().is_empty());
    assert_eq!(terminal_snapshot, snapshot(&pool, &tenant).await);
    assert_eq!(application_calls(&counters, &native), [1, 1, 1, 1, 2, 1]);
    println!(concat!(
        "G2_PARENT_TERMINAL_COMPLETE root_succeeded=true child_model=2 child_tool=1 ",
        "parent_direct_model=0 child_charged_once=true original_ack_idempotent=2 ",
        "old_fence_stale=true terminal_dispatch_run_not_runnable=2 whole_tenant_unchanged=true"
    ));
    pool.close().await;
    retry_store.close().await;
    cold.close().await;
    store.close().await;
}

// A closed fixture ledger adapter: the original complete DIRECT prefix is
// measured by FreshDirectEvidence, then authenticated byte-for-byte on every
// observation. Only this actual cancellation event may extend that prefix.
struct PublishedJoinCancellationEvidence {
    store: PostgresStore,
    key: ChildRunKey,
    prefix: Vec<JournalEvent>,
    published_direct: BudgetUsage,
    request_event: JournalEvent,
    request: stateknot_core::RunCancellationRequest,
    direct_calls: AtomicUsize,
    other_calls: AtomicUsize,
}

impl PublishedJoinCancellationEvidence {
    async fn usage_at(
        &self,
        tenant: &TenantId,
        run: RunId,
        head: &JournalHead,
    ) -> Result<BudgetUsage, GraphLifecycleEvidenceError> {
        if tenant != self.key.tenant_id()
            || run != self.key.parent_run_id()
            || head != &self.request_event.head()
        {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let admitted = self
            .store
            .load_agent_admission(tenant, run)
            .await
            .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
        let current = self
            .store
            .load_run(tenant, run)
            .await
            .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
        let page = self
            .store
            .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
            .await
            .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
        if page.has_more()
            || page.events().len() != self.prefix.len() + 1
            || &page.events()[..self.prefix.len()] != self.prefix.as_slice()
            || page.events().last() != Some(&self.request_event)
            || self.prefix.first() != Some(admitted.event())
            || current.lifecycle().provenance() != admitted.admission().intent().provenance()
            || current.lifecycle().status() != RunStatus::CancellationRequested
            || current.journal_head() != Some(head)
            || serde_json_canonicalizer::to_vec(&current.lifecycle().cancellation_request())
                .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?
                != serde_json_canonicalizer::to_vec(&Some(&self.request))
                    .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?
            || self.request.failure().caused_by_event_id() != Some(self.request_event.event_id())
            || self.request_event.source() != &stateknot_core::JournalEventSource::ControlPlane
            || self.request_event.payload().kind().as_str()
                != "published-scoped-join-cancellation-requested"
        {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let request_bytes = serde_json_canonicalizer::to_vec(&self.request_event)
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?
            .len() as u64;
        self.published_direct
            .checked_accumulate(
                &BudgetUsage::builder()
                    .event_bytes(ByteCount::new(request_bytes))
                    .build()
                    .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?,
            )
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)
    }
}

impl GraphLifecycleEvidenceProvider for PublishedJoinCancellationEvidence {
    fn direct_usage_evidence(
        &self,
        context: GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphDirectUsageEvidence, GraphLifecycleEvidenceError>> {
        self.direct_calls.fetch_add(1, Ordering::SeqCst);
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
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        UnavailableLifecycleEvidence.terminal_evidence(context)
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        UnavailableLifecycleEvidence.failure_evidence(context)
    }

    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        UnavailableLifecycleEvidence.cancellation_evidence(context)
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn published_unconsumed_scoped_join_cancellation_preserves_owner_and_usage_without_resume() {
    use futures_util::FutureExt;
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
    let tenant = tenant("scoped-published-join-cancellation-red");
    let ids = AgentRunIds::generate();
    let run = ids.run_id();
    let request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let child_ids = AgentRunIds::generate();
    let child_intent = stateknot_core::AgentAdmissionIntent::new(
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
                json!({"question":"Run the real child before requesting parent cancellation."}),
            )
            .unwrap(),
            BudgetLimits::empty()
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_input_bytes(ByteCount::new(65_536))
                .with_output_bytes(ByteCount::new(65_536)),
        ),
        request.intent().budget_layers().iter().cloned(),
        native.definition.graph().reference(),
        request.intent().authority().clone(),
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
    let evidence = Arc::new(ParentTerminalEvidence {
        direct: Arc::clone(&direct),
        terminal_calls: Arc::new(AtomicUsize::new(0)),
    });
    let registry = fresh_registry(
        &store,
        &fixture,
        &leaf,
        &native,
        &child_intent,
        &schemas,
        &direct,
        &counters,
    );
    for graph in [&fixture.graph, &leaf, native.definition.graph()] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admitted = DurableAgentAdmission::new(store.clone(), registry.clone())
        .unwrap()
        .admit(request)
        .await
        .unwrap()
        .stored()
        .clone();
    let f1 = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let parent_loop = DurableAgentLoop::new(
        store.clone(),
        registry.clone(),
        evidence.clone(),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let suspended = parent_loop
        .run(f1.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let AgentLoopOutcome::ChildJoin(join) = suspended.outcome() else {
        panic!("first public parent loop must suspend on the real child: {suspended:?}");
    };
    assert_eq!(join.keys().len(), 1);
    let key = join.keys()[0].clone();
    assert_eq!(Some(&key), direct.key.lock().await.as_ref());
    assert!(!key.parent().graph_namespace().is_root());
    assert_eq!(application_calls(&counters, &native), [1, 0, 0, 0, 0, 0]);
    assert_eq!(evidence.terminal_calls.load(Ordering::SeqCst), 0);
    let suspended_parent = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(suspended_parent.lifecycle().status(), RunStatus::Active);
    assert!(suspended_parent.lease().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .as_ref(),
        Some(admitted.checkpoint())
    );
    let original_join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    assert_eq!(
        original_join.request(),
        &ChildRunJoinRequest::new([key.clone()]).unwrap()
    );
    assert!(original_join.head().is_none());
    assert!(original_join.binding().is_none());
    assert!(original_join.consumed().is_none());
    let child_run = child_intent.provenance().run_id();
    let child_fence = store
        .claim_lease(&tenant, child_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
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
        .run(child_fence, CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        child_result.outcome(),
        AgentLoopOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let child = store.load_run(&tenant, child_run).await.unwrap();
    assert_eq!(child.lifecycle().status(), RunStatus::Succeeded);
    assert!(child.lease().is_none());
    let child_usage = child.lifecycle().terminal_usage().unwrap().clone();
    assert_eq!(child_usage.model_turns().get(), 2);
    assert_eq!(child_usage.tool_calls().get(), 1);
    assert_eq!(child_usage.unpriced_cost_events(), ExecutionCount::ZERO);
    assert_eq!(application_calls(&counters, &native), [1, 0, 0, 0, 2, 1]);
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
    assert!(published_join.consumed().is_none());
    assert_eq!(
        published_join.binding().unwrap().terminals()[0].usage(),
        &child_usage
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let settled_child = child_facts(snapshot(&pool, &tenant).await, child_run);
    let settled_account = store
        .load_child_budget_account(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled_account.children().len(), 1);
    assert!(settled_account.children()[0].settlement().is_some());
    assert_eq!(
        settled_account.delegated_usage().unwrap(),
        child_usage.cumulative_only()
    );
    // Publish while the parent is still Active. Requesting cancellation first
    // would legitimately make publication RunNotRunnable and is a different case.
    let settled_record = store.load_child_run(&key).await.unwrap();
    assert_eq!(settled_record.settlement().unwrap().usage(), &child_usage);
    assert_eq!(published_join.binding().unwrap().terminals().len(), 1);
    let before_request = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(before_request.lifecycle().status(), RunStatus::Active);
    assert!(before_request.lease().is_none());
    assert_eq!(
        before_request.journal_head(),
        Some(publication.journal_head())
    );
    let owner_history = store
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert!(!owner_history.has_more());
    assert_eq!(owner_history.records().len(), 1);
    let original_owner = owner_history.records()[0].start().clone();
    assert_eq!(original_owner.fence(), &f1);
    assert_eq!(
        owner_history.records()[0].status(),
        NodeAttemptStatus::Executing
    );
    assert!(owner_history.records()[0].completion().is_none());
    assert!(matches!(
        store.load_pending_node_result(key.parent()).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));
    let open_frame = store
        .load_active_graph_frame(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(open_frame.open_frames().len(), 1);
    let original_caller = open_frame.open_frames()[0].caller().clone();
    let publication_binding =
        serde_json_canonicalizer::to_vec(published_join.binding().unwrap()).unwrap();
    let prefix_page = store
        .load_journal_page(&tenant, run, None, JournalPageSize::new(256).unwrap())
        .await
        .unwrap();
    assert!(!prefix_page.has_more());
    assert_eq!(
        prefix_page.events().last().map(JournalEvent::head).as_ref(),
        Some(publication.journal_head())
    );
    let prefix = prefix_page.events().to_vec();
    let published_direct = direct
        .usage_at(&tenant, run, publication.journal_head())
        .await
        .unwrap();
    let prefix_bytes: u64 = prefix
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len() as u64)
        .sum();
    assert_eq!(published_direct.event_bytes().get(), prefix_bytes);
    assert_eq!(published_direct.model_turns(), ExecutionCount::ZERO);
    assert_eq!(published_direct.model_attempts(), ExecutionCount::ZERO);
    assert_eq!(published_direct.tool_calls(), ExecutionCount::ZERO);
    assert_eq!(
        published_direct.unpriced_cost_events(),
        ExecutionCount::ZERO
    );
    let request_event_id = EventId::generate();
    let request = cancellation(
        store.observe_database_clock().await.unwrap(),
        request_event_id,
    );
    let requested = store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(publication.journal_head().clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    request_event_id,
                    payload("published-scoped-join-cancellation-requested"),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                before_request.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: request.clone(),
                },
            ),
        )
        .await
        .unwrap();
    let request_event = requested.event().clone();
    assert_eq!(
        request_event.previous_digest(),
        Some(publication.journal_head().digest())
    );
    let requested_run = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(
        requested_run.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(
        serde_json::to_value(requested_run.lifecycle().cancellation_request()).unwrap(),
        serde_json::to_value(Some(&request)).unwrap()
    );
    assert!(requested_run.lease().is_none());
    let cancellation_evidence = Arc::new(PublishedJoinCancellationEvidence {
        store: store.clone(),
        key: key.clone(),
        prefix,
        published_direct: published_direct.clone(),
        request_event: request_event.clone(),
        request: request.clone(),
        direct_calls: AtomicUsize::new(0),
        other_calls: AtomicUsize::new(0),
    });
    // Validate the complete actual cancellation ledger before the expected RED
    // boundary; an unavailable reader is not a valid RED for owner-stop support.
    let cancellation_direct = cancellation_evidence
        .usage_at(&tenant, run, &request_event.head())
        .await
        .unwrap();
    assert_eq!(
        cancellation_direct.event_bytes().get(),
        prefix_bytes
            + serde_json_canonicalizer::to_vec(&request_event)
                .unwrap()
                .len() as u64
    );
    let expected_usage = cancellation_direct
        .checked_accumulate(&child_usage.cumulative_only())
        .unwrap();
    let f2 = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(f2.epoch() > f1.epoch());
    assert_eq!(application_calls(&counters, &native), [1, 0, 0, 0, 2, 1]);
    println!(
        "PUBLISHED_JOIN_CANCEL_SETUP status_before_request={:?} publication={} consumed={} owner={:?} child_model={} child_tool={} cancellation_bytes={}",
        before_request.lifecycle().status(),
        published_join.head().is_some(),
        published_join.consumed().is_some(),
        owner_history.records()[0].status(),
        child_usage.model_turns().get(),
        child_usage.tool_calls().get(),
        serde_json_canonicalizer::to_vec(&request_event)
            .unwrap()
            .len()
    );
    let cancelling_loop = DurableAgentLoop::new(
        store.clone(),
        registry,
        cancellation_evidence.clone(),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    // Never time out/drop a mutation ACK: the original Loop operation is fully
    // awaited, and its error/panic is retained until exact-claim cleanup finishes.
    let actual =
        std::panic::AssertUnwindSafe(cancelling_loop.run(f2.clone(), CancellationSignal::never()))
            .catch_unwind()
            .await;
    let observed = match &actual {
        Ok(Err(AgentLoopError::Driver { source })) => match source.as_ref() {
            GraphDriverError::Store { source }
                if matches!(source.as_ref(), StoreError::InvalidRunFailureClose) =>
            {
                "Driver(Store(InvalidRunFailureClose))"
            }
            _ => "other_driver_error",
        },
        Ok(Err(_)) => "other_agent_loop_error",
        Ok(Ok(_)) => "agent_loop_outcome",
        Err(_) => "agent_loop_panic",
    };
    let confirmed = matches!(&actual, Ok(Ok(result)) if matches!(result.outcome(),
        AgentLoopOutcome::CancellationConfirmed(stateknot_store_postgres::AppendOutcome::Committed(_))));
    let after_run = store.load_run(&tenant, run).await.unwrap();
    let after_join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    let after_history = store
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    let after_owner_result_absent = matches!(
        store.load_pending_node_result(key.parent()).await,
        Err(StoreError::PendingNodeResultNotFound)
    );
    let after_account = store
        .load_child_budget_account(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    let after_child = child_facts(snapshot(&pool, &tenant).await, child_run);
    let after_checkpoint = store.load_current_checkpoint(&tenant, run).await.unwrap();
    let active_after = store.load_active_graph_frame(&tenant, run).await.unwrap();
    let closure = store.load_graph_frame_closure(&tenant, run).await.unwrap();
    let final_page = store
        .load_journal_page(
            &tenant,
            run,
            Some(&request_event.head()),
            JournalPageSize::new(3).unwrap(),
        )
        .await
        .unwrap();
    // Even an unexpected result is retired before the decisive contract verdict.
    // Only our still-current exact claim may be released; never touch a replacement.
    let claim_retired = match after_run.lease() {
        None => true,
        Some(lease) if lease.fence() == &f2 => store.release_lease(&f2).await.is_ok(),
        Some(_) => false,
    };
    let cold = test_store().await.unwrap();
    let cold_closure = cold.load_graph_frame_closure(&tenant, run).await.unwrap();
    let cold_join = cold.load_child_join(key.parent()).await.unwrap().unwrap();
    let cold_run = cold.load_run(&tenant, run).await.unwrap();
    let callbacks = (
        cancellation_evidence.direct_calls.load(Ordering::SeqCst),
        cancellation_evidence.other_calls.load(Ordering::SeqCst),
    );
    let applications = application_calls(&counters, &native);
    eprintln!(
        "PUBLISHED_JOIN_CANCEL_OBSERVED result={observed} root={:?} closure={} consumed={} callbacks={callbacks:?} applications={applications:?} claim_retired={claim_retired}",
        after_run.lifecycle().status(),
        closure.is_some(),
        after_join.consumed().is_some()
    );
    drop(cancelling_loop);
    drop(parent_loop);
    drop(child_loop);
    drop(evidence);
    drop(cancellation_evidence);
    drop(direct);
    pool.close().await;
    cold.close().await;
    store.close().await;

    assert!(
        claim_retired,
        "only the original claimed fence may be retired"
    );
    // Expected current-profile RED site, after real publication/cancellation,
    // complete original operation ACKs and all Store resources have retired.
    assert!(
        confirmed,
        "published unconsumed Join must close the whole frame and confirm Root cancellation; actual={observed}"
    );
    assert_eq!(applications, [1, 0, 0, 0, 2, 1]);
    assert_eq!(callbacks, (1, 0));
    assert_eq!(after_run.lifecycle().status(), RunStatus::Cancelled);
    assert!(after_run.lease().is_none());
    assert!(cold_run.lease().is_none());
    assert_eq!(
        serde_json::to_value(after_run.lifecycle().cancellation_request()).unwrap(),
        serde_json::to_value(Some(&request)).unwrap()
    );
    assert_eq!(
        serde_json::to_value(after_run.lifecycle().terminal_failure()).unwrap(),
        serde_json::to_value(Some(request.failure())).unwrap()
    );
    assert_eq!(
        after_run.lifecycle().terminal_usage(),
        Some(&expected_usage)
    );
    assert_eq!(
        after_run.lifecycle().revision().get(),
        requested_run.lifecycle().revision().get() + 1
    );
    assert_eq!(after_checkpoint.as_ref(), Some(admitted.checkpoint()));
    assert_eq!(after_run.checkpoint(), requested_run.checkpoint());
    assert!(active_after.is_none());
    assert!(!after_history.has_more());
    assert_eq!(after_history.records().len(), 1);
    assert_eq!(after_history.records()[0].start(), &original_owner);
    assert_eq!(
        after_history.records()[0].status(),
        NodeAttemptStatus::Executing
    );
    assert!(after_history.records()[0].completion().is_none());
    assert!(after_owner_result_absent);
    for joined in [&after_join, &cold_join] {
        assert_eq!(joined.request(), published_join.request());
        assert_eq!(joined.registration(), published_join.registration());
        assert_eq!(joined.head(), Some(&publication));
        assert!(joined.consumed().is_none());
        assert_eq!(
            serde_json_canonicalizer::to_vec(joined.binding().unwrap()).unwrap(),
            publication_binding
        );
    }
    assert_eq!(after_account.children(), settled_account.children());
    assert_eq!(
        after_account.delegated_usage().unwrap(),
        child_usage.cumulative_only()
    );
    assert_eq!(after_account.direct_usage(), &cancellation_direct);
    assert_eq!(settled_child, after_child);
    let closure = closure.expect("whole scoped cancellation has a real owning closure");
    let cold_closure = cold_closure.expect("cold owning read restores the complete closure");
    assert_eq!(closure.event(), cold_closure.event());
    assert_eq!(closure.root_checkpoint(), &admitted.checkpoint().head());
    assert_eq!(closure.direct_usage(), &cancellation_direct);
    assert_eq!(
        serde_json::to_value(closure.failure()).unwrap(),
        serde_json::to_value(request.failure()).unwrap()
    );
    assert_eq!(closure.completions().len(), 1);
    assert_eq!(closure.completions()[0].start(), &original_caller);
    assert_eq!(closure.completions()[0].status(), NodeAttemptStatus::Failed);
    assert!(!final_page.has_more());
    assert_eq!(final_page.events().len(), 2);
    assert_eq!(&final_page.events()[0], closure.event());
    assert_eq!(
        closure.event().sequence().get(),
        request_event.sequence().get() + 1
    );
    assert_eq!(
        closure.event().previous_digest(),
        Some(request_event.digest())
    );
    let confirmation = &final_page.events()[1];
    assert_eq!(
        confirmation.payload().kind().as_str(),
        "agent-cancellation-confirmed"
    );
    assert_eq!(
        confirmation.previous_digest(),
        Some(closure.event().digest())
    );
    assert_eq!(
        confirmation.sequence().get(),
        request_event.sequence().get() + 2
    );
    assert_eq!(after_run.journal_head(), Some(&confirmation.head()));
    assert_eq!(
        serde_json::to_value(cold_run.lifecycle()).unwrap(),
        serde_json::to_value(after_run.lifecycle()).unwrap()
    );
    assert_eq!(cold_run.journal_head(), after_run.journal_head());
}

// The older v1/v2 fault campaigns above remain byte-for-byte intact. These
// cases prepare a real public application before exercising the v3 Store close
// and cold Runtime finalizer; they do not claim an interrupted Driver mutation.
mod published_stop_faults {
    use super::*;
    use stateknot_core::{ChildRunBudgetAccount, RunLifecycle};
    use stateknot_runtime::{
        DurableFrameCancellationFinalizer, FrameCancellationOutcome, agent_maintenance::*,
    };
    use stateknot_store_postgres::{AppendOutcome, ChildJoinRecord};

    const CLOSE_CASE: &str = "scoped_join_close_process_faults::fresh_join::parent_terminal::published_stop_faults::published_scoped_join_close_commit_loss_survives_real_kill_and_cold_maintenance";
    const TERMINAL_CASE: &str = "scoped_join_close_process_faults::fresh_join::parent_terminal::published_stop_faults::published_scoped_join_terminal_commit_loss_survives_real_kill_and_id_only_cold_retry";

    struct Seed {
        tenant: TenantId,
        run: RunId,
        native: ProviderNativeFixture,
        fixture: DriverFixture,
        leaf: CompiledGraph,
        schemas: JsonSchemaRegistry,
        request: DurableAgentAdmissionRequest,
        child_intent: stateknot_core::AgentAdmissionIntent,
    }

    struct Published {
        key: ChildRunKey,
        owner: NodeAttemptStartHead,
        owner_start: Value,
        caller: Value,
        root_checkpoint: Checkpoint,
        join: ChildJoinRecord,
        child_usage: BudgetUsage,
        account: ChildRunBudgetAccount,
        prefix: Vec<JournalEvent>,
        direct: BudgetUsage,
        first_fence: RunFence,
    }

    struct Fixture {
        published: Published,
        request: RunCancellationRequest,
        request_event: JournalEvent,
        revision: RunRevision,
        lifecycle: Value,
        fence: RunFence,
        direct: BudgetUsage,
        event_id: EventId,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(deny_unknown_fields)]
    struct CloseCandidate {
        tenant: TenantId,
        run: RunId,
        event_id: EventId,
        observed: JournalHead,
        revision: RunRevision,
        direct: BudgetUsage,
    }

    #[derive(serde::Deserialize)]
    #[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
    enum WorkerInput {
        CloseWriter {
            candidate: CloseCandidate,
            proxy_port: u16,
        },
        CloseRetry {
            candidate: CloseCandidate,
        },
        Existing {
            tenant: TenantId,
            run: RunId,
        },
        Maintenance {
            tenant: TenantId,
            run: RunId,
        },
        TerminalWriter {
            tenant: TenantId,
            run: RunId,
            proxy_port: u16,
        },
        TerminalCold {
            tenant: TenantId,
            run: RunId,
        },
    }

    fn seed(store: &PostgresStore) -> Seed {
        let native = provider_native_fixture_with_budget(
            store.clone(),
            false,
            false,
            false,
            None,
            NativeBudgetMode::Durable,
        );
        let (fixture, leaf) =
            fresh_graphs(native.definition.graph(), native.definition.descriptor());
        let schemas = fresh_schemas(&fixture, &leaf, &native);
        let tenant = tenant("published-v3-stop-process-fault");
        let ids = AgentRunIds::generate();
        let run = ids.run_id();
        let request = durable_admission_request(
            &fixture,
            tenant.clone(),
            ids,
            fixture.graph.output_schema().clone(),
            fixture.graph.input_schema().clone(),
        );
        let child_ids = AgentRunIds::generate();
        let child_intent = stateknot_core::AgentAdmissionIntent::new(
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
                    json!({"question":"Run the real child before requesting parent cancellation."}),
                )
                .unwrap(),
                BudgetLimits::empty()
                    .with_event_bytes(ByteCount::new(1_048_576))
                    .with_checkpoint_bytes(ByteCount::new(1_048_576))
                    .with_graph_steps(ExecutionCount::new(8))
                    .with_input_bytes(ByteCount::new(65_536))
                    .with_output_bytes(ByteCount::new(65_536)),
            ),
            request.intent().budget_layers().iter().cloned(),
            native.definition.graph().reference(),
            request.intent().authority().clone(),
        )
        .unwrap();
        Seed {
            tenant,
            run,
            native,
            fixture,
            leaf,
            schemas,
            request,
            child_intent,
        }
    }

    struct Suspended {
        admitted: StoredAgentAdmission,
        key: ChildRunKey,
        fence: RunFence,
        direct: Arc<FreshDirectEvidence>,
        counters: FreshCounters,
    }

    async fn suspend(store: &PostgresStore, seed: &Seed) -> Suspended {
        let tenant = &seed.tenant;
        let run = seed.run;
        let native = &seed.native;
        let fixture = &seed.fixture;
        let leaf = &seed.leaf;
        let schemas = &seed.schemas;
        let child_intent = &seed.child_intent;
        let counters = FreshCounters::new();
        let direct = Arc::new(FreshDirectEvidence {
            store: store.clone(),
            key: Arc::new(tokio::sync::Mutex::new(None)),
            root: fixture.graph.reference(),
            leaf: leaf.reference(),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let evidence = Arc::new(ParentTerminalEvidence {
            direct: Arc::clone(&direct),
            terminal_calls: Arc::new(AtomicUsize::new(0)),
        });
        let registry = fresh_registry(
            store,
            fixture,
            leaf,
            native,
            child_intent,
            schemas,
            &direct,
            &counters,
        );
        for graph in [&fixture.graph, leaf, native.definition.graph()] {
            store
                .register_graph_definition(tenant.clone(), graph.clone())
                .await
                .unwrap();
        }
        let admitted = DurableAgentAdmission::new(store.clone(), registry.clone())
            .unwrap()
            .admit(seed.request.clone())
            .await
            .unwrap()
            .stored()
            .clone();
        let f1 = store
            .claim_lease(tenant, run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let parent_loop = DurableAgentLoop::new(
            store.clone(),
            registry.clone(),
            evidence.clone(),
            DurableGraphDriverOptions::default(),
            DurableGraphLifecycleOptions::default(),
        )
        .unwrap();
        let suspended = parent_loop
            .run(f1.clone(), CancellationSignal::never())
            .await
            .unwrap();
        let AgentLoopOutcome::ChildJoin(join) = suspended.outcome() else {
            panic!("first public parent loop must suspend on the real child: {suspended:?}");
        };
        assert_eq!(join.keys().len(), 1);
        let key = join.keys()[0].clone();
        assert_eq!(Some(&key), direct.key.lock().await.as_ref());
        assert!(!key.parent().graph_namespace().is_root());
        assert_eq!(application_calls(&counters, native), [1, 0, 0, 0, 0, 0]);
        assert_eq!(evidence.terminal_calls.load(Ordering::SeqCst), 0);
        let suspended_parent = store.load_run(tenant, run).await.unwrap();
        assert_eq!(suspended_parent.lifecycle().status(), RunStatus::Active);
        assert!(suspended_parent.lease().is_none());
        assert_eq!(
            store
                .load_current_checkpoint(tenant, run)
                .await
                .unwrap()
                .as_ref(),
            Some(admitted.checkpoint())
        );
        Box::pin(unpublished_join(store, &key)).await;

        drop(parent_loop);
        drop(evidence);
        drop(registry);
        Suspended {
            admitted,
            key,
            fence: f1,
            direct,
            counters,
        }
    }

    async fn unpublished_join(store: &PostgresStore, key: &ChildRunKey) {
        let join = Box::pin(store.load_child_join(key.parent()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            join.request(),
            &ChildRunJoinRequest::new([key.clone()]).unwrap()
        );
        assert!(join.head().is_none());
        assert!(join.binding().is_none());
        assert!(join.consumed().is_none());
    }

    async fn settle_and_publish(
        store: &PostgresStore,
        seed: &Seed,
        suspended: &Suspended,
    ) -> (ChildJoinRecord, BudgetUsage) {
        let tenant = &seed.tenant;
        let key = suspended.key.clone();
        let native = &seed.native;
        let counters = &suspended.counters;
        let child_intent = &seed.child_intent;
        let schemas = &seed.schemas;
        let original_join = store.load_child_join(key.parent()).await.unwrap().unwrap();
        let child_run = child_intent.provenance().run_id();
        let child_fence = store
            .claim_lease(tenant, child_run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
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
            .run(child_fence, CancellationSignal::never())
            .await
            .unwrap();
        assert!(matches!(
            child_result.outcome(),
            AgentLoopOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
        ));
        let child = store.load_run(tenant, child_run).await.unwrap();
        assert_eq!(child.lifecycle().status(), RunStatus::Succeeded);
        assert!(child.lease().is_none());
        let child_usage = child.lifecycle().terminal_usage().unwrap().clone();
        assert_eq!(child_usage.model_turns().get(), 2);
        assert_eq!(child_usage.tool_calls().get(), 1);
        assert_eq!(child_usage.unpriced_cost_events(), ExecutionCount::ZERO);
        assert_eq!(application_calls(counters, native), [1, 0, 0, 0, 2, 1]);
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
        assert_eq!(published_join.registration(), original_join.registration());
        assert!(published_join.consumed().is_none());
        assert_eq!(
            published_join.binding().unwrap().terminals()[0].usage(),
            &child_usage
        );

        drop(child_loop);
        (published_join, child_usage)
    }

    async fn published_facts(
        store: &PostgresStore,
        seed: &Seed,
        suspended: &Suspended,
        join: ChildJoinRecord,
        child_usage: BudgetUsage,
    ) -> Published {
        let key = &suspended.key;
        let tenant = &seed.tenant;
        let run = seed.run;
        let publication = join.head().unwrap();
        let parent = store.load_run(tenant, run).await.unwrap();
        assert_eq!(parent.lifecycle().status(), RunStatus::Active);
        assert!(parent.lease().is_none());
        assert_eq!(parent.journal_head(), Some(publication.journal_head()));
        let (owner, owner_start, caller) =
            Box::pin(published_owner(store, key, &suspended.fence)).await;
        let account = Box::pin(store.load_child_budget_account(tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(account.children().len(), 1);
        assert!(account.children()[0].settlement().is_some());
        assert_eq!(
            account.delegated_usage().unwrap(),
            child_usage.cumulative_only()
        );
        let child = Box::pin(store.load_child_run(key)).await.unwrap();
        assert_eq!(child.settlement().unwrap().usage(), &child_usage);
        assert_eq!(join.binding().unwrap().terminals().len(), 1);
        let page = store
            .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
            .await
            .unwrap();
        assert!(!page.has_more());
        assert_eq!(
            page.events().last().map(JournalEvent::head).as_ref(),
            Some(publication.journal_head())
        );
        let direct = suspended
            .direct
            .usage_at(tenant, run, publication.journal_head())
            .await
            .unwrap();
        let bytes: u64 = page
            .events()
            .iter()
            .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len() as u64)
            .sum();
        assert_eq!(direct.event_bytes().get(), bytes);
        assert_eq!(direct.model_turns(), ExecutionCount::ZERO);
        assert_eq!(direct.model_attempts(), ExecutionCount::ZERO);
        assert_eq!(direct.tool_calls(), ExecutionCount::ZERO);
        assert_eq!(direct.unpriced_cost_events(), ExecutionCount::ZERO);
        Published {
            key: key.clone(),
            owner,
            owner_start,
            caller,
            root_checkpoint: suspended.admitted.checkpoint().clone(),
            join,
            child_usage,
            account,
            prefix: page.events().to_vec(),
            direct,
            first_fence: suspended.fence.clone(),
        }
    }

    async fn published_owner(
        store: &PostgresStore,
        key: &ChildRunKey,
        fence: &RunFence,
    ) -> (NodeAttemptStartHead, Value, Value) {
        let tenant = key.tenant_id();
        let run = key.parent_run_id();
        let history = Box::pin(store.load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        ))
        .await
        .unwrap();
        assert!(!history.has_more());
        assert_eq!(history.records().len(), 1);
        let owner = history.records()[0].start();
        assert_eq!(owner.fence(), fence);
        assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
        assert!(history.records()[0].completion().is_none());
        assert!(matches!(
            store.load_pending_node_result(key.parent()).await,
            Err(StoreError::PendingNodeResultNotFound)
        ));
        let active = Box::pin(store.load_active_graph_frame(tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.open_frames().len(), 1);

        (
            owner.head(),
            serde_json::to_value(owner).unwrap(),
            serde_json::to_value(active.open_frames()[0].caller()).unwrap(),
        )
    }

    async fn request_cancellation(store: &PostgresStore, published: Published) -> Fixture {
        let tenant = published.key.tenant_id();
        let run = published.key.parent_run_id();
        let parent = store.load_run(tenant, run).await.unwrap();
        assert_eq!(parent.lifecycle().status(), RunStatus::Active);
        let request_id = EventId::generate();
        let request = cancellation(store.observe_database_clock().await.unwrap(), request_id);
        let appended = store
            .append_control_plane(
                JournalAppend::new(
                    JournalExpectation::exact(
                        published.join.head().unwrap().journal_head().clone(),
                    ),
                    JournalEventIntent::control_plane(
                        tenant.clone(),
                        run,
                        request_id,
                        payload("published-scoped-join-cancellation-requested"),
                    )
                    .unwrap(),
                )
                .unwrap(),
                RunProjection::transition(
                    parent.lifecycle().revision(),
                    RunTransition::RequestCancellation {
                        request: request.clone(),
                    },
                ),
            )
            .await
            .unwrap();
        let request_event = appended.event().clone();
        let reader = PublishedJoinCancellationEvidence {
            store: store.clone(),
            key: published.key.clone(),
            prefix: published.prefix.clone(),
            published_direct: published.direct.clone(),
            request_event: request_event.clone(),
            request: request.clone(),
            direct_calls: AtomicUsize::new(0),
            other_calls: AtomicUsize::new(0),
        };
        // A failed reader/setup cannot qualify as a lost-COMMIT contract failure.
        let direct = reader
            .usage_at(tenant, run, &request_event.head())
            .await
            .unwrap();
        assert_eq!(reader.direct_calls.load(Ordering::SeqCst), 0);
        let requested = store.load_run(tenant, run).await.unwrap();
        assert_eq!(
            requested.lifecycle().status(),
            RunStatus::CancellationRequested
        );
        assert!(requested.lease().is_none());
        assert_eq!(requested.journal_head(), Some(&request_event.head()));
        let fence = store
            .claim_lease(tenant, run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        assert!(fence.epoch() > published.first_fence.epoch());
        Fixture {
            published,
            request,
            request_event,
            revision: requested.lifecycle().revision(),
            lifecycle: serde_json::to_value(requested.lifecycle()).unwrap(),
            fence,
            direct,
            event_id: EventId::generate(),
        }
    }

    async fn prepare_v3(store: &PostgresStore) -> Fixture {
        let seed = seed(store);
        let suspended = Box::pin(suspend(store, &seed)).await;
        let (join, usage) = Box::pin(settle_and_publish(store, &seed, &suspended)).await;
        assert_eq!(
            application_calls(&suspended.counters, &seed.native),
            [1, 0, 0, 0, 2, 1]
        );
        let published = Box::pin(published_facts(store, &seed, &suspended, join, usage)).await;
        let fixture = Box::pin(request_cancellation(store, published)).await;
        drop(suspended);
        drop(seed);
        fixture
    }

    fn candidate(fixture: &Fixture) -> CloseCandidate {
        CloseCandidate {
            tenant: fixture.published.key.tenant_id().clone(),
            run: fixture.published.key.parent_run_id(),
            event_id: fixture.event_id,
            observed: fixture.request_event.head(),
            revision: fixture.revision,
            direct: fixture.direct.clone(),
        }
    }

    async fn close_candidate(
        store: &PostgresStore,
        candidate: &CloseCandidate,
    ) -> GraphFrameClosureCommitOutcome {
        Box::pin(store.close_graph_frames(
            &candidate.tenant,
            candidate.run,
            candidate.event_id,
            candidate.observed.clone(),
            candidate.revision,
            candidate.direct.clone(),
        ))
        .await
        .unwrap()
    }

    async fn retained_published(store: &PostgresStore, published: &Published) {
        let key = &published.key;
        let history = Box::pin(store.load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        ))
        .await
        .unwrap();
        assert!(!history.has_more());
        assert_eq!(history.records().len(), 1);
        assert_eq!(
            serde_json::to_value(history.records()[0].start()).unwrap(),
            published.owner_start
        );
        assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
        assert!(history.records()[0].completion().is_none());
        assert!(matches!(
            store.load_pending_node_result(key.parent()).await,
            Err(StoreError::PendingNodeResultNotFound)
        ));
        let join = Box::pin(store.load_child_join(key.parent()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(join.request(), published.join.request());
        assert_eq!(join.registration(), published.join.registration());
        assert_eq!(join.head(), published.join.head());
        assert!(join.consumed().is_none());
        assert_eq!(
            serde_json_canonicalizer::to_vec(join.binding().unwrap()).unwrap(),
            serde_json_canonicalizer::to_vec(published.join.binding().unwrap()).unwrap()
        );
        let child = Box::pin(store.load_child_run(key)).await.unwrap();
        assert_eq!(child.settlement().unwrap().usage(), &published.child_usage);
        assert_eq!(
            store
                .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap()
                .as_ref(),
            Some(&published.root_checkpoint)
        );
    }

    async fn closure_bytes(pool: &PgPool, fixture: &Fixture) -> Vec<u8> {
        let key = &fixture.published.key;
        let bytes: Vec<u8> = query_scalar(
            "SELECT closure_bytes FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2",
        ).bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid())
            .fetch_one(pool).await.unwrap();
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["version"], 3);
        assert_eq!(wire["intent"]["version"], 3);
        assert_eq!(
            wire["intent"]["stopped_join"],
            json!({
                "owner":fixture.published.owner,
                "request_digest":fixture.published.join.request().digest(),
                "registration":fixture.published.join.registration().head(),
                "binding":fixture.published.join.binding().unwrap(),
                "publication":fixture.published.join.head().unwrap(),
            })
        );
        assert_eq!(
            wire["intent"]["direct_usage"],
            serde_json::to_value(&fixture.direct).unwrap()
        );
        assert_eq!(
            wire["intent"]["observed_head"],
            serde_json::to_value(fixture.request_event.head()).unwrap()
        );
        bytes
    }

    async fn owning_v3(store: &PostgresStore, fixture: &Fixture) -> StoredGraphFrameClosure {
        let published = &fixture.published;
        let key = &published.key;
        let closure =
            Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
                .await
                .unwrap()
                .unwrap();
        check_closure_event(&closure, fixture);
        assert!(
            Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
                .await
                .unwrap()
                .is_none()
        );
        Box::pin(retained_published(store, published)).await;
        closure
    }

    fn check_closure_event(closure: &StoredGraphFrameClosure, fixture: &Fixture) {
        let published = &fixture.published;
        assert_eq!(closure.event().event_id(), fixture.event_id);
        assert_eq!(closure.root_checkpoint(), &published.root_checkpoint.head());
        assert_eq!(closure.direct_usage(), &fixture.direct);
        assert_eq!(
            serde_json::to_value(closure.failure()).unwrap(),
            serde_json::to_value(fixture.request.failure()).unwrap()
        );
        assert_eq!(closure.completions().len(), 1);
        assert_eq!(
            serde_json::to_value(closure.completions()[0].start()).unwrap(),
            published.caller
        );
        assert_eq!(closure.completions()[0].status(), NodeAttemptStatus::Failed);
        assert_ne!(closure.completions()[0].start(), &published.owner);
        assert_eq!(
            closure.event().sequence().get(),
            fixture.request_event.sequence().get() + 1
        );
        assert_eq!(
            closure.event().previous_digest(),
            Some(fixture.request_event.digest())
        );
        assert_eq!(
            closure.event().payload().kind().as_str(),
            "graph-frames-closed"
        );
        assert_eq!(
            closure.event().payload().schema(),
            &PostgresStore::graph_frame_published_join_stop_event_schema()
                .unwrap()
                .0
        );
    }

    async fn check_closed(store: &PostgresStore, fixture: &Fixture) -> StoredGraphFrameClosure {
        let closure = Box::pin(owning_v3(store, fixture)).await;
        let key = &fixture.published.key;
        let run = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(run.lifecycle()).unwrap(),
            fixture.lifecycle
        );
        assert_eq!(run.journal_head(), Some(&closure.event().head()));
        assert_eq!(run.last_fencing_epoch(), Some(fixture.fence.epoch()));
        assert!(run.lease().is_none());
        assert!(!run.is_quarantined());
        assert_eq!(
            Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
                .await
                .unwrap()
                .unwrap(),
            fixture.published.account
        );
        closure
    }

    fn cancellation_schemas() -> JsonSchemaRegistry {
        let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
        stateknot_runtime::register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
        schemas.build().unwrap()
    }

    fn cancelled(outcome: FrameCancellationOutcome) -> (JournalEvent, &'static str) {
        let FrameCancellationOutcome::Cancelled(append) = outcome else {
            panic!("a durable v3 cancellation must not become NotApplicable");
        };
        let kind = match append.as_ref() {
            AppendOutcome::Committed(_) => "committed",
            AppendOutcome::Idempotent(_) => "idempotent",
            _ => panic!("unexpected cancellation append"),
        };
        (append.event().clone(), kind)
    }

    async fn terminal_event(
        store: &PostgresStore,
        fixture: &Fixture,
        closure: &StoredGraphFrameClosure,
    ) -> JournalEvent {
        let key = &fixture.published.key;
        let page = store
            .load_journal_page(
                key.tenant_id(),
                key.parent_run_id(),
                Some(&closure.event().head()),
                JournalPageSize::new(1).unwrap(),
            )
            .await
            .unwrap();
        assert!(!page.has_more());
        assert_eq!(page.events().len(), 1);
        let event = &page.events()[0];
        assert_eq!(event.sequence().get(), closure.event().sequence().get() + 1);
        assert_eq!(event.previous_digest(), Some(closure.event().digest()));
        assert_eq!(
            event.source(),
            &stateknot_core::JournalEventSource::ControlPlane
        );
        assert_eq!(
            event.payload().kind().as_str(),
            "agent-cancellation-confirmed"
        );
        assert_eq!(
            event.payload().schema(),
            &stateknot_runtime::standard_agent_cancellation_event_schema()
                .unwrap()
                .0
        );
        cancellation_schemas()
            .validate_bounded(event.payload().schema(), event.payload().data())
            .unwrap();
        let graph = fixture
            .published
            .root_checkpoint
            .graph()
            .definition_digest()
            .to_string();
        assert_eq!(
            event.payload().data().as_value(),
            &json!({
                "operation":"agent_cancellation_confirmed",
                "graph_digest":graph.strip_prefix("sha256:").unwrap(),
                "checkpoint_id":fixture.published.root_checkpoint.checkpoint_id().to_string(),
                "superstep":fixture.published.root_checkpoint.superstep().get().to_string(),
                "failure_id":closure.failure().id().to_string(),
            })
        );
        event.clone()
    }

    async fn check_terminal(store: &PostgresStore, fixture: &Fixture) -> JournalEvent {
        let closure = Box::pin(owning_v3(store, fixture)).await;
        let event = Box::pin(terminal_event(store, fixture, &closure)).await;
        let key = &fixture.published.key;
        let run = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        let total = fixture
            .direct
            .checked_accumulate(&fixture.published.child_usage.cumulative_only())
            .unwrap();
        assert_ne!(total, fixture.direct);
        assert_eq!(run.lifecycle().status(), RunStatus::Cancelled);
        assert_eq!(run.journal_head(), Some(&event.head()));
        assert_eq!(run.last_fencing_epoch(), Some(fixture.fence.epoch()));
        assert!(run.lease().is_none());
        assert!(!run.is_quarantined());
        assert!(run.scheduler_ready_at().is_none());
        assert!(run.scheduler_not_before().is_none());
        assert!(run.lifecycle().result().is_none());
        assert_eq!(run.lifecycle().revision().get(), fixture.revision.get() + 1);
        assert_eq!(run.lifecycle().terminal_usage(), Some(&total));
        let original: RunLifecycle = serde_json::from_value(fixture.lifecycle.clone()).unwrap();
        let expected = original
            .apply(RunTransition::ConfirmCancellation {
                completed_at: run.lifecycle().changed_at(),
                usage: total.clone(),
            })
            .unwrap();
        assert_eq!(
            serde_json::to_value(run.lifecycle()).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert!(run.lifecycle().changed_at() >= closure.event().recorded_at());
        assert!(run.lifecycle().changed_at() <= event.recorded_at());
        let account =
            Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            account,
            fixture
                .published
                .account
                .observe_direct(event.head(), fixture.direct.clone())
                .unwrap()
        );
        assert_eq!(
            Box::pin(store.include_child_usage(
                key.tenant_id(),
                key.parent_run_id(),
                fixture.direct.clone()
            ))
            .await
            .unwrap(),
            total
        );
        event
    }
    fn table_rows(snapshot: &Snapshot, table: &str) -> Vec<Value> {
        serde_json::from_str(&snapshot[table]).unwrap()
    }

    fn same_parent_columns_except(
        before: &Snapshot,
        after: &Snapshot,
        table: &str,
        key: &str,
        parent: RunId,
        allowed: &[&str],
    ) {
        let parent = parent.to_string();
        let project = |facts: &Snapshot| {
            let mut rows = table_rows(facts, table);
            let mut matches = 0;
            for row in &mut rows {
                if row[key].as_str() == Some(parent.as_str()) {
                    matches += 1;
                    for field in allowed {
                        assert!(row.as_object_mut().unwrap().remove(*field).is_some());
                    }
                }
            }
            assert_eq!(matches, 1, "one actual target row in {table}");
            rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
            rows
        };
        assert_eq!(
            project(after),
            project(before),
            "immutable columns/other rows: {table}"
        );
    }

    fn terminal_snapshot_delta(before: &Snapshot, after: &Snapshot, parent: RunId) {
        assert_eq!(
            before.keys().collect::<Vec<_>>(),
            after.keys().collect::<Vec<_>>()
        );
        for (table, rows) in before {
            if !["runs", "run_events", "child_run_budget_accounts"].contains(&table.as_str()) {
                assert_eq!(
                    after.get(table),
                    Some(rows),
                    "unchanged whole tenant table: {table}"
                );
            }
        }
        same_parent_columns_except(
            before,
            after,
            "runs",
            "run_id",
            parent,
            &[
                "journal_sequence",
                "journal_event_id",
                "journal_recorded_at",
                "journal_digest",
                "lifecycle_bytes",
                "lifecycle_revision",
                "lifecycle_status",
                "changed_at",
                "scheduler_ready_at",
                "scheduler_not_before",
                "lease_attempt_id",
                "lease_acquired_at",
                "lease_renewed_at",
                "lease_expires_at",
                "wait_set_digest",
                "unresolved_wait_count",
                "next_timer_due_at",
                "next_interrupt_expiry_at",
                "updated_at",
            ],
        );
        same_parent_columns_except(
            before,
            after,
            "child_run_budget_accounts",
            "parent_run_id",
            parent,
            &[
                "account_digest",
                "account_bytes",
                "journal_sequence",
                "journal_event_id",
                "journal_recorded_at",
                "journal_digest",
            ],
        );
        let parent_text = parent.to_string();
        let runs = table_rows(after, "runs");
        let run = runs
            .iter()
            .find(|row| row["run_id"].as_str() == Some(parent_text.as_str()))
            .unwrap();
        assert_eq!(run["updated_at"], run["journal_recorded_at"]);
        assert_eq!(run["unresolved_wait_count"], 0);
        for field in [
            "scheduler_ready_at",
            "scheduler_not_before",
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "wait_set_digest",
            "next_timer_due_at",
            "next_interrupt_expiry_at",
        ] {
            assert!(run[field].is_null(), "terminal row retained {field}");
        }
        let original = table_rows(before, "run_events");
        let current = table_rows(after, "run_events");
        assert_eq!(current.len(), original.len() + 1);
        for row in &original {
            assert!(
                current.contains(row),
                "an original tenant event was changed or lost"
            );
        }
        let added: Vec<_> = current
            .iter()
            .filter(|row| !original.contains(row))
            .collect();
        assert_eq!(added.len(), 1);
        let parent = parent.to_string();
        assert_eq!(added[0]["run_id"].as_str(), Some(parent.as_str()));
    }

    async fn raw_closure_bytes(pool: &PgPool, tenant: &TenantId, run: RunId) -> Vec<u8> {
        query_scalar("SELECT closure_bytes FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2")
            .bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(pool).await.unwrap()
    }

    async fn connection(proxy_port: Option<u16>) -> PostgresStore {
        // The inherited helper proxies only its exact writer role. Normalizing
        // this local input preserves its original TLS/pool/lease/timeout policy.
        worker_store(&proxy_port.map_or_else(
            || json!({"role":"cold"}),
            |port| json!({"role":"writer", "proxy_port":port}),
        ))
        .await
    }

    async fn close_worker(candidate: CloseCandidate, proxy_port: Option<u16>) {
        let resume = parent_control();
        let store = connection(proxy_port).await;
        if proxy_port.is_some() {
            publish_ready(&json!({"phase":"writer_before_close", "pid":std::process::id()}));
            resume.await.unwrap();
            let outcome = Box::pin(close_candidate(&store, &candidate)).await;
            panic!("v3 close returned before the required lost-COMMIT kill: {outcome:?}");
        }
        // BeforeCommit has no durable closure. Only this complete original
        // candidate may create it; zero/stale candidate reuse is forbidden here.
        let outcome = Box::pin(close_candidate(&store, &candidate)).await;
        assert!(matches!(
            outcome,
            GraphFrameClosureCommitOutcome::Committed(_)
        ));
        let original = fingerprint(outcome.record());
        store.close().await;
        publish_ready(
            &json!({"phase":"cold_close_committed", "pid":std::process::id(), "original":original}),
        );
        resume.await.unwrap();
    }

    async fn existing_worker(tenant: TenantId, run: RunId) {
        let resume = parent_control();
        let store = connection(None).await;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
            .await
            .unwrap();
        let before = snapshot(&pool, &tenant).await;
        let original = Box::pin(store.load_graph_frame_closure(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        let bytes = raw_closure_bytes(&pool, &tenant, run).await;
        assert_eq!(
            original.event().payload().schema(),
            &PostgresStore::graph_frame_published_join_stop_event_schema()
                .unwrap()
                .0
        );
        let retry_id = EventId::generate();
        assert_ne!(retry_id, original.event().event_id());
        let retry = Box::pin(store.close_graph_frames(
            &tenant,
            run,
            retry_id,
            original.event().head(),
            RunRevision::ZERO,
            BudgetUsage::zero(),
        ))
        .await
        .unwrap();
        assert!(matches!(retry, GraphFrameClosureCommitOutcome::Existing(_)));
        assert_eq!(fingerprint(retry.record()), fingerprint(&original));
        let reloaded = Box::pin(store.load_graph_frame_closure(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fingerprint(&reloaded), fingerprint(&original));
        assert_eq!(raw_closure_bytes(&pool, &tenant, run).await, bytes);
        assert_eq!(snapshot(&pool, &tenant).await, before);
        let observation = json!({"phase":"cold_existing_ack_verified", "pid":std::process::id(),
            "original":fingerprint(&reloaded), "wire_digest":Digest::sha256(&bytes)});
        pool.close().await;
        store.close().await;
        publish_ready(&observation);
        resume.await.unwrap();
    }

    async fn terminal_worker(tenant: TenantId, run: RunId, proxy_port: Option<u16>) {
        let resume = parent_control();
        let store = connection(proxy_port).await;
        let closure = Box::pin(store.load_graph_frame_closure(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            closure.event().payload().schema(),
            &PostgresStore::graph_frame_published_join_stop_event_schema()
                .unwrap()
                .0
        );
        let finalizer = DurableFrameCancellationFinalizer::new(
            store.clone(),
            cancellation_schemas(),
            DurableGraphLifecycleOptions::default(),
        )
        .unwrap();
        if proxy_port.is_some() {
            publish_ready(&json!({"phase":"writer_before_confirmation", "pid":std::process::id()}));
            resume.await.unwrap();
            let result = finalizer.resume(&tenant, run).await;
            panic!(
                "v3 terminal confirmation returned before the required process kill: {result:?}"
            );
        }
        let (first, first_kind) = cancelled(finalizer.resume(&tenant, run).await.unwrap());
        let (second, second_kind) = cancelled(finalizer.resume(&tenant, run).await.unwrap());
        assert_eq!(second_kind, "idempotent");
        assert_eq!(
            second, first,
            "cold resume must authenticate every original ACK byte"
        );
        drop(finalizer);
        store.close().await;
        publish_ready(
            &json!({"phase":"cold_terminal_ack_verified", "pid":std::process::id(),
            "first_append":first_kind, "second_append":second_kind,
            "first_event":first, "second_event":second}),
        );
        resume.await.unwrap();
    }

    async fn worker(input: Value) {
        match serde_json::from_value::<WorkerInput>(input).unwrap() {
            WorkerInput::CloseWriter {
                candidate,
                proxy_port,
            } => {
                Box::pin(close_worker(candidate, Some(proxy_port))).await;
            }
            WorkerInput::CloseRetry { candidate } => {
                Box::pin(close_worker(candidate, None)).await;
            }
            WorkerInput::Existing { tenant, run } => {
                Box::pin(existing_worker(tenant, run)).await;
            }
            WorkerInput::Maintenance { tenant, run } => {
                Box::pin(maintenance_worker(ColdInput { tenant, run })).await;
            }
            WorkerInput::TerminalWriter {
                tenant,
                run,
                proxy_port,
            } => {
                Box::pin(terminal_worker(tenant, run, Some(proxy_port))).await;
            }
            WorkerInput::TerminalCold { tenant, run } => {
                Box::pin(terminal_worker(tenant, run, None)).await;
            }
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ColdInput {
        tenant: TenantId,
        run: RunId,
    }

    async fn connect_existing() -> PostgresStore {
        connection(None).await
    }
    fn maintenance_schemas() -> JsonSchemaRegistry {
        let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
        stateknot_runtime::register_standard_agent_deadline_event_schema(&mut schemas).unwrap();
        stateknot_runtime::register_standard_child_reconciliation_event_schema(&mut schemas)
            .unwrap();
        stateknot_runtime::register_standard_child_join_event_schema(&mut schemas).unwrap();
        stateknot_runtime::register_standard_run_failure_close_event_schema(&mut schemas).unwrap();
        stateknot_runtime::register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
        for (reference, document) in [
            PostgresStore::graph_frame_closure_event_schema().unwrap(),
            PostgresStore::graph_frame_join_stop_event_schema().unwrap(),
            PostgresStore::graph_frame_published_join_stop_event_schema().unwrap(),
        ] {
            schemas.register(reference, document).unwrap();
        }
        schemas.build().unwrap()
    }

    struct ActualDependencies {
        store: PostgresStore,
        input: ColdInput,
    }

    impl AgentMaintenanceReadiness for ActualDependencies {
        fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>> {
            Box::pin(async move {
                self.store
                    .observe_database_clock()
                    .await
                    .map_err(|_| AgentMaintenanceReadinessError)?;
                let proof = Box::pin(
                    self.store
                        .load_graph_frame_closure(&self.input.tenant, self.input.run),
                )
                .await
                .map_err(|_| AgentMaintenanceReadinessError)?;
                if proof.is_none() {
                    return Err(AgentMaintenanceReadinessError);
                }
                Ok(())
            })
        }
    }

    async fn maintenance_worker(input: ColdInput) {
        let resume = parent_control();
        let store = connect_existing().await;
        let before = store.load_run(&input.tenant, input.run).await.unwrap();
        assert_eq!(
            before.lifecycle().status(),
            RunStatus::CancellationRequested
        );
        assert!(before.lease().is_none());
        assert!(!before.is_quarantined());
        let binding = AgentMaintenanceBinding::new(
            store.clone(),
            maintenance_schemas(),
            vec![input.tenant.clone()],
            AgentMaintenanceMutationOptions::default(),
        )
        .unwrap();
        let host = Arc::new(ActualDependencies {
            store: store.clone(),
            input: ColdInput {
                tenant: input.tenant.clone(),
                run: input.run,
            },
        });
        let mut role = AgentMaintenance::start(binding, host, AgentMaintenanceOptions::default())
            .await
            .unwrap();
        let health = role.health();
        // This is the existing maintenance fixture's status-convergence bound.
        // Timeout is an observation: stop/join and report it before the final expect.
        let observed = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let run = store.load_run(&input.tenant, input.run).await.unwrap();
                let report = health.report();
                let visited = AgentMaintenanceJob::ALL
                    .into_iter()
                    .all(|job| report.job(job).completed_ticks > 0);
                if (run.lifecycle().status() == RunStatus::Cancelled && visited)
                    || report.failure.is_some()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let report = role.shutdown().await.unwrap();
        let after = store.load_run(&input.tenant, input.run).await.unwrap();
        let converged = observed.is_ok()
            && after.lifecycle().status() == RunStatus::Cancelled
            && AgentMaintenanceJob::ALL
                .into_iter()
                .all(|job| report.job(job).completed_ticks > 0);
        let jobs: Vec<Value> = AgentMaintenanceJob::ALL
            .into_iter()
            .map(|job| {
                json!({
                    "job":format!("{job:?}"), "completed_ticks":report.job(job).completed_ticks,
                    "items":report.job(job).items, "item_failures":report.job(job).item_failures,
                })
            })
            .collect();
        let observation = json!({
            "pid":std::process::id(), "status":after.lifecycle().status(),
            "converged_within_existing_30s":converged,
            "started_ticks":report.started_ticks, "readiness_failures":report.readiness_failures,
            "forced_ticks":report.forced_ticks, "failure":format!("{:?}",report.failure),
            "jobs":jobs, "health":format!("{:?}",health.status()),
            "active_ticks":health.active_ticks(),
        });
        store.close().await;
        // The real role has joined; keep only this owned test process alive so the
        // controller receives the complete actual report and observes successful exit.
        publish_ready(&observation);
        resume.await.unwrap();
    }

    fn close_snapshot_delta(before: &Snapshot, after: &Snapshot, parent: RunId) {
        assert_eq!(
            before.keys().collect::<Vec<_>>(),
            after.keys().collect::<Vec<_>>()
        );
        for (table, rows) in before {
            if ![
                "runs",
                "run_events",
                "graph_frame_stacks",
                "graph_frame_closures",
                "graph_frame_closed_callers",
            ]
            .contains(&table.as_str())
            {
                assert_eq!(
                    after.get(table),
                    Some(rows),
                    "unchanged close table: {table}"
                );
            }
        }
        same_parent_columns_except(
            before,
            after,
            "runs",
            "run_id",
            parent,
            &[
                "journal_sequence",
                "journal_event_id",
                "journal_recorded_at",
                "journal_digest",
                "lease_attempt_id",
                "lease_acquired_at",
                "lease_renewed_at",
                "lease_expires_at",
                "scheduler_not_before",
                "updated_at",
            ],
        );
        same_parent_columns_except(
            before,
            after,
            "graph_frame_stacks",
            "run_id",
            parent,
            &["active_namespace", "active_frame_identity_digest"],
        );
        let parent_text = parent.to_string();
        for table in ["graph_frame_closures", "graph_frame_closed_callers"] {
            assert!(table_rows(before, table).is_empty());
            let rows = table_rows(after, table);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["run_id"].as_str(), Some(parent_text.as_str()));
        }
        let original = table_rows(before, "run_events");
        let current = table_rows(after, "run_events");
        assert_eq!(current.len(), original.len() + 1);
        assert!(original.iter().all(|row| current.contains(row)));
        let added: Vec<_> = current
            .iter()
            .filter(|row| !original.contains(row))
            .collect();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0]["run_id"].as_str(), Some(parent_text.as_str()));
    }

    struct Closed {
        closure: StoredGraphFrameClosure,
        bytes: Vec<u8>,
        snapshot: Snapshot,
    }

    async fn closed_state(store: &PostgresStore, pool: &PgPool, fixture: &Fixture) -> Closed {
        let closure = Box::pin(check_closed(store, fixture)).await;
        let bytes = closure_bytes(pool, fixture).await;
        let snapshot = snapshot(pool, fixture.published.key.tenant_id()).await;
        Closed {
            closure,
            bytes,
            snapshot,
        }
    }

    struct Killed {
        snapshot: Snapshot,
        writer_pid: u64,
        signal: i32,
    }

    fn exit_signal(status: std::process::ExitStatus) -> Option<i32> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        }
        #[cfg(not(unix))]
        {
            let _ = status;
            None
        }
    }

    async fn kill_at_cut(
        store: &PostgresStore,
        pool: &PgPool,
        connection: &PgConnectOptions,
        fixture: &Fixture,
        cut: Cut,
        closed: Option<&Closed>,
    ) -> Killed {
        let key = &fixture.published.key;
        let before = snapshot(pool, key.tenant_id()).await;
        let (case, target, input, phase) = if closed.is_some() {
            (
                TERMINAL_CASE,
                TransactionTarget::GraphFrameCancellationConfirmation,
                json!({"role":"terminal_writer", "tenant":key.tenant_id(), "run":key.parent_run_id()}),
                "writer_before_confirmation",
            )
        } else {
            (
                CLOSE_CASE,
                TransactionTarget::GraphFrameClosure,
                json!({"role":"close_writer", "candidate":candidate(fixture)}),
                "writer_before_close",
            )
        };
        let mut proxy = CommitProxy::start(connection, cut, target).await;
        let mut input = input;
        input["proxy_port"] = json!(proxy.port());
        let mut writer = TestProcess::spawn(case, &input);
        let ready = writer.ready().await.unwrap();
        assert_eq!(ready["phase"], phase);
        let writer_pid = ready["pid"].as_u64().unwrap();
        assert_ne!(writer_pid, u64::from(std::process::id()));
        writer.resume();
        proxy.wait_cut(cut).await;
        let backend_pid = proxy.backend_pid();
        let observed = snapshot(pool, key.tenant_id()).await;
        Box::pin(inspect_cut(
            store, pool, fixture, cut, closed, &before, &observed,
        ))
        .await;
        assert_eq!(snapshot(pool, key.tenant_id()).await, observed);
        assert_eq!(proxy.messages_while_ack_held(), 0);
        let killed = writer.kill_and_reap().await;
        let signal =
            exit_signal(killed).expect("v3 commit-loss qualification requires real Unix SIGKILL");
        assert_eq!(signal, 9);
        proxy.stop().await;
        wait_backend_disconnect(pool, backend_pid).await;
        assert_eq!(
            snapshot(pool, key.tenant_id()).await,
            observed,
            "kill/reap and backend rollback must preserve the actual cut state"
        );
        Killed {
            snapshot: observed,
            writer_pid,
            signal,
        }
    }

    async fn inspect_cut(
        store: &PostgresStore,
        pool: &PgPool,
        fixture: &Fixture,
        cut: Cut,
        closed: Option<&Closed>,
        before: &Snapshot,
        observed: &Snapshot,
    ) {
        let key = &fixture.published.key;
        if cut == Cut::BeforeCommit {
            assert_eq!(
                observed, before,
                "unforwarded COMMIT cannot expose any tenant write"
            );
            if closed.is_none() {
                assert!(
                    Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        } else if let Some(closed) = closed {
            let _event = Box::pin(check_terminal(store, fixture)).await;
            terminal_snapshot_delta(&closed.snapshot, observed, key.parent_run_id());
        } else {
            let _closure = Box::pin(check_closed(store, fixture)).await;
            let _bytes = closure_bytes(pool, fixture).await;
            close_snapshot_delta(before, observed, key.parent_run_id());
        }
        if let Some(closed) = closed {
            let original = Box::pin(owning_v3(store, fixture)).await;
            assert_eq!(fingerprint(&original), fingerprint(&closed.closure));
            assert_eq!(closure_bytes(pool, fixture).await, closed.bytes);
        }
    }

    async fn cold_process(case: &str, input: &Value, writer_pid: u64) -> Value {
        let mut cold = TestProcess::spawn(case, input);
        let observation = cold.ready().await.unwrap();
        let pid = observation["pid"].as_u64().unwrap();
        assert_ne!(pid, u64::from(std::process::id()));
        assert_ne!(pid, writer_pid);
        cold.resume();
        cold.wait_success().await;
        observation
    }

    fn check_maintenance(observation: &Value) {
        assert_eq!(observation["health"], "Stopped");
        assert_eq!(observation["active_ticks"], 0);
        assert_eq!(observation["forced_ticks"], 0);
        assert_eq!(observation["failure"], "None");
        assert_eq!(observation["readiness_failures"], 0);
        assert_eq!(observation["converged_within_existing_30s"], true);
        assert_eq!(
            observation["status"],
            serde_json::to_value(RunStatus::Cancelled).unwrap()
        );
        let jobs = observation["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), AgentMaintenanceJob::ALL.len());
        assert!(!jobs.is_empty());
        assert!(
            observation["started_ticks"].as_u64().unwrap() >= u64::try_from(jobs.len()).unwrap()
        );
        for job in jobs {
            assert!(job["completed_ticks"].as_u64().unwrap() > 0);
            assert_eq!(job["item_failures"], 0);
        }
    }

    fn check_ack(observation: &Value, event: &JournalEvent, expected_first: &str) {
        assert_eq!(observation["phase"], "cold_terminal_ack_verified");
        assert_eq!(observation["first_append"], expected_first);
        assert_eq!(observation["second_append"], "idempotent");
        assert_eq!(
            observation["first_event"],
            serde_json::to_value(event).unwrap()
        );
        assert_eq!(observation["second_event"], observation["first_event"]);
    }

    async fn close_leg(
        store: &PostgresStore,
        pool: &PgPool,
        connection: &PgConnectOptions,
        fixture: &Fixture,
        cut: Cut,
    ) -> Value {
        let key = &fixture.published.key;
        let before = snapshot(pool, key.tenant_id()).await;
        let killed = Box::pin(kill_at_cut(store, pool, connection, fixture, cut, None)).await;
        if cut == Cut::BeforeCommit {
            let retry = cold_process(
                CLOSE_CASE,
                &json!({"role":"close_retry", "candidate":candidate(fixture)}),
                killed.writer_pid,
            )
            .await;
            assert_eq!(retry["phase"], "cold_close_committed");
            let closure = Box::pin(check_closed(store, fixture)).await;
            assert_eq!(retry["original"], fingerprint(&closure));
        }
        let closed = Box::pin(closed_state(store, pool, fixture)).await;
        close_snapshot_delta(&before, &closed.snapshot, key.parent_run_id());
        if cut == Cut::CommitResponse {
            assert_eq!(closed.snapshot, killed.snapshot);
        }
        let original = cold_process(
            CLOSE_CASE,
            &json!({"role":"existing", "tenant":key.tenant_id(), "run":key.parent_run_id()}),
            killed.writer_pid,
        )
        .await;
        assert_eq!(original["phase"], "cold_existing_ack_verified");
        assert_eq!(original["original"], fingerprint(&closed.closure));
        assert_eq!(
            original["wire_digest"],
            serde_json::to_value(Digest::sha256(&closed.bytes)).unwrap()
        );
        assert_eq!(snapshot(pool, key.tenant_id()).await, closed.snapshot);
        let maintenance = cold_process(
            CLOSE_CASE,
            &json!({"role":"maintenance", "tenant":key.tenant_id(), "run":key.parent_run_id()}),
            killed.writer_pid,
        )
        .await;
        check_maintenance(&maintenance);
        let event = Box::pin(check_terminal(store, fixture)).await;
        let after = snapshot(pool, key.tenant_id()).await;
        terminal_snapshot_delta(&closed.snapshot, &after, key.parent_run_id());
        let ack = cold_process(
            CLOSE_CASE,
            &json!({"role":"terminal_cold", "tenant":key.tenant_id(), "run":key.parent_run_id()}),
            killed.writer_pid,
        )
        .await;
        check_ack(&ack, &event, "idempotent");
        assert_eq!(closure_bytes(pool, fixture).await, closed.bytes);
        assert_eq!(snapshot(pool, key.tenant_id()).await, after);
        evidence(
            "closure",
            cut,
            &killed,
            fixture,
            &closed,
            &event,
            &json!({"existing_ack":original, "maintenance":maintenance, "terminal_ack":ack}),
        )
    }

    async fn terminal_leg(
        store: &PostgresStore,
        pool: &PgPool,
        connection: &PgConnectOptions,
        fixture: &Fixture,
        cut: Cut,
    ) -> Value {
        let key = &fixture.published.key;
        let initial = snapshot(pool, key.tenant_id()).await;
        let committed = Box::pin(close_candidate(store, &candidate(fixture))).await;
        assert!(matches!(
            committed,
            GraphFrameClosureCommitOutcome::Committed(_)
        ));
        let closed = Box::pin(closed_state(store, pool, fixture)).await;
        assert_eq!(
            fingerprint(committed.record()),
            fingerprint(&closed.closure)
        );
        close_snapshot_delta(&initial, &closed.snapshot, key.parent_run_id());
        let killed = Box::pin(kill_at_cut(
            store,
            pool,
            connection,
            fixture,
            cut,
            Some(&closed),
        ))
        .await;
        let ack = cold_process(
            TERMINAL_CASE,
            &json!({"role":"terminal_cold", "tenant":key.tenant_id(), "run":key.parent_run_id()}),
            killed.writer_pid,
        )
        .await;
        let event = Box::pin(check_terminal(store, fixture)).await;
        check_ack(
            &ack,
            &event,
            if cut == Cut::BeforeCommit {
                "committed"
            } else {
                "idempotent"
            },
        );
        let after = snapshot(pool, key.tenant_id()).await;
        terminal_snapshot_delta(&closed.snapshot, &after, key.parent_run_id());
        if cut == Cut::CommitResponse {
            assert_eq!(after, killed.snapshot);
        }
        assert_eq!(closure_bytes(pool, fixture).await, closed.bytes);
        assert_eq!(snapshot(pool, key.tenant_id()).await, after);
        evidence(
            "terminal_confirmation",
            cut,
            &killed,
            fixture,
            &closed,
            &event,
            &ack,
        )
    }

    fn evidence(
        target: &str,
        cut: Cut,
        killed: &Killed,
        fixture: &Fixture,
        closed: &Closed,
        event: &JournalEvent,
        cold: &Value,
    ) -> Value {
        json!({
            "profile":"published-join-v3-commit-loss-v1", "target":target,
            "cut":if cut == Cut::BeforeCommit {"commit_not_forwarded"} else {"commit_response_withheld"},
            "commit_forwarded":cut == Cut::CommitResponse, "commit_response_forwarded":false,
            "writer_killed_and_reaped":true, "signal":killed.signal, "writer_pid":killed.writer_pid,
            "cold":cold, "closure_version":3, "Join_published":true, "Join_consumed":false,
            "owner_status":"Executing", "owner_completion":false, "owner_result":false,
            "child_model_turns":fixture.published.child_usage.model_turns(),
            "child_tool_calls":fixture.published.child_usage.tool_calls(),
            "child_unpriced_cost_events":fixture.published.child_usage.unpriced_cost_events(),
            "closure":fingerprint(&closed.closure), "closure_bytes_sha256":Digest::sha256(&closed.bytes),
            "terminal_event":event, "invariants":"passed",
            "qualification":"public-application-setup/store-close/runtime-finalizer",
            "driver_mutation_interrupted":false, "os":std::env::consts::OS, "arch":std::env::consts::ARCH,
        })
    }

    async fn run_leg(
        store: &PostgresStore,
        pool: &PgPool,
        connection: &PgConnectOptions,
        case: &str,
        cut: Cut,
    ) -> Value {
        use futures_util::FutureExt;
        let fixture = Box::pin(prepare_v3(store)).await;
        let actual = std::panic::AssertUnwindSafe(async {
            if case == CLOSE_CASE {
                Box::pin(close_leg(store, pool, connection, &fixture, cut)).await
            } else {
                assert_eq!(case, TERMINAL_CASE);
                Box::pin(terminal_leg(store, pool, connection, &fixture, cut)).await
            }
        })
        .catch_unwind()
        .await;
        let key = &fixture.published.key;
        let current = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        // On an unexpected assertion/child error, retire only our still-current
        // F2 claim, after TestProcess has killed/reaped its owned subprocesses.
        let retired = match current.lease() {
            None => true,
            Some(lease) if lease.fence() == &fixture.fence => {
                store.release_lease(&fixture.fence).await.is_ok()
            }
            Some(_) => false,
        };
        let result = match actual {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        assert!(retired, "only the original exact F2 claim may be retired");
        result
    }

    async fn controller(case: &str) {
        use futures_util::FutureExt;
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
        let actual = std::panic::AssertUnwindSafe(async {
            let mut evidence = Vec::new();
            for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
                let result = Box::pin(run_leg(
                    &store,
                    &pool,
                    connection.as_ref().unwrap(),
                    case,
                    cut,
                ))
                .await;
                evidence.push(result);
            }
            let version: String = query_scalar("SHOW server_version")
                .fetch_one(&pool)
                .await
                .unwrap();
            (evidence, version)
        })
        .catch_unwind()
        .await;
        // No successful evidence marker precedes owned pool/Store retirement.
        // TestProcess Drop kills/reaps every unfinished child on the panic path.
        pool.close().await;
        store.close().await;
        let (evidence, version) = match actual {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        for mut row in evidence {
            row["postgres"] = json!(version);
            row["case"] = json!(case);
            println!("\nSTATEKNOT_PUBLISHED_JOIN_V3_COMMIT_LOSS_EVIDENCE={row}");
        }
    }

    #[tokio::test]
    async fn published_scoped_join_close_commit_loss_survives_real_kill_and_cold_maintenance() {
        if let Ok(input) = std::env::var(INPUT_ENV) {
            Box::pin(worker(serde_json::from_str(&input).unwrap())).await;
            return;
        }
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        Box::pin(controller(CLOSE_CASE)).await;
    }

    #[tokio::test]
    async fn published_scoped_join_terminal_commit_loss_survives_real_kill_and_id_only_cold_retry()
    {
        if let Ok(input) = std::env::var(INPUT_ENV) {
            Box::pin(worker(serde_json::from_str(&input).unwrap())).await;
            return;
        }
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        Box::pin(controller(TERMINAL_CASE)).await;
    }
}
