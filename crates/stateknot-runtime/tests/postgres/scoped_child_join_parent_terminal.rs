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
