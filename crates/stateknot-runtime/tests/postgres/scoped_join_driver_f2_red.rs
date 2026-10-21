// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Future-contract tracer at the real Driver seam; no lifecycle confirmation.
use super::*;
use stateknot_runtime::{GraphDirectUsageEvidence, GraphDirectUsageEvidenceContext};
use stateknot_store_postgres::LeaseClaimOutcome;

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

fn matching_registry(calls: &Arc<AtomicUsize>) -> ExecutableGraphRegistry {
    let (fixture, leaf, isolated, _) = graphs();
    let mut builder = ExecutableGraphRegistryBuilder::new(fixture.registry.schemas().clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: fixture.graph.reducer().clone(),
        }))
        .unwrap();
    for graph in [&fixture.graph, &leaf, &isolated] {
        assert_eq!(graph.reducer(), fixture.graph.reducer());
        builder.register_graph(graph.clone()).unwrap();
        for node in graph.nodes() {
            if graph
                .frame_calls()
                .is_some_and(|policy| policy.call(node.node_id()).is_some())
            {
                continue;
            }
            let child_join = graph.child_runs().is_some_and(|policy| {
                policy
                    .declarations()
                    .iter()
                    .any(|declaration| declaration.node_id() == node.node_id())
            });
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
                    child_join,
                }))
                .unwrap();
        }
    }
    builder.build().unwrap()
}

struct CountedEvidence {
    calls: Arc<AtomicUsize>,
    direct: BudgetUsage,
}

impl GraphLifecycleEvidenceProvider for CountedEvidence {
    fn direct_usage_evidence(
        &self,
        context: GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphDirectUsageEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let direct = self.direct.clone();
        Box::pin(async move { Ok(GraphDirectUsageEvidence::new(context, direct)) })
    }

    fn terminal_evidence(
        &self,
        _: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
    }

    fn failure_evidence(
        &self,
        _: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
    }

    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
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

async fn assert_original_owner_and_join(store: &PostgresStore, scenario: &Scenario) {
    let key = &scenario.key;
    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), scenario.owner);
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    let joined = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(joined.request(), &scenario.join_request);
    assert_eq!(joined.registration(), &scenario.join_registration);
    assert!(joined.binding().is_none());
    assert!(joined.head().is_none());
    assert!(joined.consumed().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        scenario.root_checkpoint
    );
    let owned = Box::pin(store.load_child_run(key)).await.unwrap();
    assert_eq!(owned.settlement().unwrap().usage(), &scenario.child_usage);
    assert!(
        Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn settled_registered_scoped_join_cancellation_yields_a_real_f2_driver_handoff_without_dispatch()
 {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    // The existing public Store fixture actually commits F1 scoped Start,
    // child ownership, unpublished Join, cancellation, child terminal and full
    // settlement, and independently accounts its exact seven parent events.
    let scenario = Box::pin(prepare(&store)).await;
    let key = &scenario.key;
    let active_before =
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        active_before.checkpoint().frame().namespace(),
        key.parent().graph_namespace()
    );
    assert_eq!(
        &active_before.checkpoint().checkpoint().head(),
        key.parent().base_checkpoint()
    );
    Box::pin(assert_original_owner_and_join(&store, &scenario)).await;
    let account = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap();
    assert_eq!(
        Box::pin(store.include_child_usage(
            key.tenant_id(),
            key.parent_run_id(),
            scenario.direct.clone()
        ))
        .await
        .unwrap(),
        scenario
            .direct
            .checked_accumulate(&scenario.child_usage.cumulative_only())
            .unwrap(),
    );
    let application_calls = Arc::new(AtomicUsize::new(0));
    let evidence_calls = Arc::new(AtomicUsize::new(0));
    let registry = matching_registry(&application_calls);
    let owned = Box::pin(store.load_child_run(key)).await.unwrap();
    for reference in [
        scenario.root_checkpoint.graph(),
        active_before.checkpoint().frame().target(),
        owned.intent().child().graph(),
    ] {
        let actual = store
            .load_graph_definition(key.tenant_id(), reference)
            .await
            .unwrap();
        assert_eq!(registry.resolve(reference).unwrap().graph(), actual.graph());
    }
    let claimed = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap();
    assert!(matches!(claimed, LeaseClaimOutcome::Claimed(_)));
    let f2 = claimed.lease().fence().clone();
    assert!(f2.epoch() > scenario.fence.epoch());
    assert_ne!(f2.attempt_id(), scenario.fence.attempt_id());
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, key.tenant_id()).await;
    let driver = DurableGraphDriver::new(
        store.clone(),
        registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(CountedEvidence {
        calls: Arc::clone(&evidence_calls),
        direct: scenario.direct.clone(),
    }));
    let drive_started_at = database_clock(&pool).await;
    let result = driver.drive(f2.clone(), CancellationSignal::never()).await;
    if let Err(error) = &result {
        // Preserve the actual current error; no particular failure point or
        // current error variant is made part of the future contract.
        println!("STATEKNOT_SCOPED_JOIN_F2_DRIVER_OBSERVATION={error:?}");
    }
    let drive_finished_at = database_clock(&pool).await;
    assert!(drive_finished_at >= drive_started_at);
    assert_eq!(application_calls.load(Ordering::SeqCst), 0);
    assert_eq!(evidence_calls.load(Ordering::SeqCst), 0);
    let current = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(current.lifecycle()).unwrap(),
        scenario.lifecycle
    );
    assert_eq!(
        current.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(current.last_fencing_epoch(), Some(f2.epoch()));
    assert!(!current.is_quarantined());
    let after = snapshot(&pool, key.tenant_id()).await;
    if result.is_err() {
        // Rejection may release only the original F2. It cannot silently
        // reacquire, quarantine, or change business/scheduler facts otherwise.
        assert!(current.lease().is_none());
        let released_at = current.scheduler_ready_at().unwrap();
        assert!(released_at >= drive_started_at);
        assert!(released_at <= drive_finished_at);
        assert!(released_at >= claimed.lease().renewed_at());
        assert!(current.scheduler_not_before().is_none());
        assert_exact_release_snapshot(before, after, key.parent_run_id());
        assert!(matches!(
            store.release_lease(&f2).await.unwrap(),
            LeaseReleaseOutcome::Idempotent
        ));
    } else {
        // A successful handoff retains live F2 and every scheduler field.
        assert_eq!(current.lease().unwrap().fence(), &f2);
        store.observe_live_lease(&f2).await.unwrap();
        assert_eq!(
            business_facts(after, key.parent_run_id()),
            business_facts(before, key.parent_run_id()),
            "cancellation handoff must not change tenant business facts",
        );
    }
    Box::pin(assert_original_owner_and_join(&store, &scenario)).await;
    assert_eq!(
        Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap(),
        account,
    );
    let active_after =
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(active_after.checkpoint(), active_before.checkpoint());
    assert_eq!(active_after.caller().head(), active_before.caller().head());
    assert_eq!(
        active_after.caller_binding_digest(),
        active_before.caller_binding_digest()
    );
    assert_eq!(
        active_after.open_frames().len(),
        active_before.open_frames().len()
    );
    for (after, before) in active_after
        .open_frames()
        .iter()
        .zip(active_before.open_frames())
    {
        assert_eq!(after.checkpoint(), before.checkpoint());
        assert_eq!(after.entry_digest(), before.entry_digest());
        assert_eq!(after.caller(), before.caller());
        assert_eq!(
            after.caller_binding_digest(),
            before.caller_binding_digest()
        );
    }
    let result =
        result.expect("a settled registered scoped Join cancellation yields a real Driver handoff");
    let GraphDriveOutcome::CancellationRequested(handoff) = result.outcome() else {
        panic!(
            "expected cancellation handoff without application dispatch: {:?}",
            result.outcome()
        );
    };
    assert_eq!(handoff.lease().fence(), &f2);
    assert_eq!(handoff.checkpoint(), &scenario.root_checkpoint.head());
    assert_eq!(handoff.leaf(), Some(&active_before.checkpoint().head()));
    assert_eq!(handoff.journal_head(), &scenario.observed);
    assert_eq!(handoff.expected_revision(), scenario.revision);
    assert_eq!(
        handoff.failure_id(),
        current
            .lifecycle()
            .cancellation_request()
            .unwrap()
            .failure()
            .id()
    );
    let report = result.report();
    assert!(report.whole_replay_passes() >= 1);
    assert!(report.whole_replay().frame_checkpoints_validated() >= 1);
    assert_eq!(report.frame_entries_committed(), 0);
    assert_eq!(report.frame_returns_committed(), 0);
    assert_eq!(report.frame_callers_rebound(), 0);
    assert_eq!(report.node_attempts_started(), 0);
    assert_eq!(report.node_attempts_completed(), 0);
    assert_eq!(report.node_batches_started(), 0);
    assert_eq!(report.durable_events(), 0);
    assert!(matches!(
        store.release_lease(&f2).await.unwrap(),
        LeaseReleaseOutcome::Released
    ));
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn settled_registered_scoped_join_agent_loop_closes_frames_and_confirms_root_once() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let scenario = Box::pin(prepare(&store)).await;
    let key = &scenario.key;
    Box::pin(assert_original_owner_and_join(&store, &scenario)).await;
    let active = Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    let original_start = history.records()[0].start().clone();
    assert_eq!(original_start.head(), scenario.owner);
    assert_eq!(original_start.fence(), &scenario.fence);
    let account = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    let expected_usage = scenario
        .direct
        .checked_accumulate(&scenario.child_usage.cumulative_only())
        .unwrap();
    assert_ne!(expected_usage, scenario.direct);
    let claim = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap();
    assert!(matches!(claim, LeaseClaimOutcome::Claimed(_)));
    let f2 = claim.lease().fence().clone();
    assert!(f2.epoch() > scenario.fence.epoch());
    assert_ne!(f2.attempt_id(), scenario.fence.attempt_id());
    let pending = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(pending.lifecycle().revision(), scenario.revision);
    assert_eq!(
        serde_json::to_value(pending.lifecycle()).unwrap(),
        scenario.lifecycle
    );
    let request = pending.lifecycle().cancellation_request().unwrap();
    let original_request = serde_json::to_value(request).unwrap();
    let original_failure = serde_json::to_value(request.failure()).unwrap();
    let failure_id = request.failure().id();
    let applications = Arc::new(AtomicUsize::new(0));
    let evidence_calls = Arc::new(AtomicUsize::new(0));
    let registry = matching_registry(&applications);
    let owned = Box::pin(store.load_child_run(key)).await.unwrap();
    for reference in [
        scenario.root_checkpoint.graph(),
        active.checkpoint().frame().target(),
        owned.intent().child().graph(),
    ] {
        let definition = store
            .load_graph_definition(key.tenant_id(), reference)
            .await
            .unwrap();
        assert_eq!(
            registry.resolve(reference).unwrap().graph(),
            definition.graph()
        );
    }
    let schemas = registry.schemas().clone();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, key.tenant_id()).await;
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        registry,
        Arc::new(CountedEvidence {
            calls: Arc::clone(&evidence_calls),
            direct: scenario.direct.clone(),
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = agent_loop
        .run(f2.clone(), CancellationSignal::never())
        .await
        .expect(
            "the real F2 AgentLoop closes the registered Join frames and confirms cancellation",
        );
    let AgentLoopOutcome::CancellationConfirmed(
        stateknot_store_postgres::AppendOutcome::Committed(confirmation),
    ) = result.outcome()
    else {
        panic!(
            "expected an actual fresh cancellation confirmation: {:?}",
            result.outcome()
        );
    };
    assert_eq!(applications.load(Ordering::SeqCst), 0);
    // Every other CountedEvidence method fails. Success plus this exact count
    // therefore requires one real DIRECT observation and no other evidence call.
    assert_eq!(evidence_calls.load(Ordering::SeqCst), 1);
    // This is the Driver report. The lifecycle's two control-plane events
    // are authenticated independently below, outside its dispatch counters.
    let report = result.report();
    assert!(report.whole_replay_passes() >= 1);
    assert!(report.whole_replay().frame_checkpoints_validated() >= 1);
    assert_eq!(report.frame_entries_committed(), 0);
    assert_eq!(report.frame_returns_committed(), 0);
    assert_eq!(report.frame_callers_rebound(), 0);
    assert_eq!(report.node_attempts_started(), 0);
    assert_eq!(report.node_attempts_completed(), 0);
    assert_eq!(report.node_batches_started(), 0);
    assert_eq!(report.durable_events(), 0);

    let terminal = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        terminal.lifecycle().revision().get(),
        scenario.revision.get() + 1
    );
    assert_eq!(terminal.checkpoint(), pending.checkpoint());
    assert_eq!(terminal.last_fencing_epoch(), Some(f2.epoch()));
    assert!(terminal.lease().is_none());
    assert!(!terminal.is_quarantined());
    assert!(terminal.scheduler_ready_at().is_none());
    assert!(terminal.scheduler_not_before().is_none());
    assert!(terminal.lifecycle().result().is_none());
    assert_eq!(terminal.lifecycle().terminal_usage(), Some(&expected_usage));
    assert_eq!(
        serde_json::to_value(terminal.lifecycle().cancellation_request().unwrap()).unwrap(),
        original_request
    );
    assert_eq!(
        serde_json::to_value(terminal.lifecycle().terminal_failure().unwrap()).unwrap(),
        original_failure
    );
    assert_eq!(terminal.journal_head(), Some(&confirmation.head()));
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        scenario.root_checkpoint
    );

    let closure = Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closure.direct_usage(), &scenario.direct);
    assert_eq!(closure.failure().id(), failure_id);
    assert_eq!(
        serde_json::to_value(closure.failure()).unwrap(),
        original_failure
    );
    assert_eq!(closure.completions().len(), active.open_frames().len());
    assert_eq!(closure.completions().len(), 1);
    for (completion, frame) in closure.completions().iter().zip(active.open_frames()) {
        assert_eq!(completion.start(), frame.caller());
        assert_eq!(completion.start().fence(), frame.caller().fence());
        assert_eq!(completion.start().fence(), &scenario.fence);
        assert_ne!(completion.start(), &scenario.owner);
        assert_eq!(completion.status(), NodeAttemptStatus::Failed);
        assert_eq!(completion.usage(), &BudgetUsage::zero());
        assert_eq!(completion.journal_head(), &closure.event().head());
    }
    let page = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            Some(&scenario.observed),
            JournalPageSize::new(3).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    assert_eq!(page.events().len(), 2);
    assert_eq!(&page.events()[0], closure.event());
    assert_eq!(&page.events()[1], confirmation);
    assert_eq!(
        closure.event().sequence().get(),
        scenario.observed.sequence().get() + 1
    );
    assert_eq!(
        confirmation.sequence().get(),
        scenario.observed.sequence().get() + 2
    );
    assert_eq!(
        closure.event().previous_digest(),
        Some(scenario.observed.digest())
    );
    assert_eq!(
        confirmation.previous_digest(),
        Some(closure.event().digest())
    );
    for event in page.events() {
        assert_eq!(
            event.source(),
            &stateknot_core::JournalEventSource::ControlPlane
        );
    }
    assert_eq!(
        closure.event().payload().kind().as_str(),
        "graph-frames-closed"
    );
    assert_eq!(
        closure.event().payload().schema().id().as_str(),
        "https://stknot.com/schemas/store/graph-frame-closure-event/2.0.0"
    );
    assert_eq!(
        closure.event().payload().schema().version(),
        Version::new(2, 0, 0)
    );
    assert_eq!(closure.event().payload().data().as_value()["version"], 2);
    assert_eq!(
        confirmation.payload().kind().as_str(),
        "agent-cancellation-confirmed"
    );
    assert_eq!(
        confirmation.payload().schema(),
        &stateknot_runtime::standard_agent_cancellation_event_schema()
            .unwrap()
            .0
    );
    schemas
        .validate_bounded(
            confirmation.payload().schema(),
            confirmation.payload().data(),
        )
        .unwrap();
    let graph_digest = scenario
        .root_checkpoint
        .graph()
        .definition_digest()
        .to_string();
    assert_eq!(
        confirmation.payload().data().as_value(),
        &json!({
            "operation":"agent_cancellation_confirmed",
            "graph_digest":graph_digest.strip_prefix("sha256:").unwrap(),
            "checkpoint_id":scenario.root_checkpoint.checkpoint_id().to_string(),
            "superstep":scenario.root_checkpoint.superstep().get().to_string(),
            "failure_id":failure_id.to_string(),
        })
    );
    assert!(terminal.lifecycle().changed_at() >= closure.event().recorded_at());
    assert!(terminal.lifecycle().changed_at() <= confirmation.recorded_at());
    let bytes = v2_bytes(&pool, &scenario).await;
    let wire: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        wire["event"],
        serde_json::to_value(closure.event()).unwrap()
    );
    assert_eq!(
        wire["intent"]["root"],
        serde_json::to_value(scenario.root_checkpoint.head()).unwrap()
    );
    assert_eq!(
        wire["intent"]["observed_head"],
        serde_json::to_value(&scenario.observed).unwrap()
    );
    assert_eq!(wire["intent"]["lifecycle"], scenario.lifecycle);
    assert_eq!(
        wire["intent"]["direct_usage"],
        serde_json::to_value(&scenario.direct).unwrap()
    );
    assert_eq!(
        wire["intent"]["delegated_usage"],
        serde_json::to_value(scenario.child_usage.cumulative_only()).unwrap()
    );
    assert_eq!(
        wire["intent"]["stopped_join"]["owner"],
        serde_json::to_value(&scenario.owner).unwrap()
    );
    assert_eq!(
        wire["intent"]["stopped_join"]["request_digest"],
        serde_json::to_value(scenario.join_request.digest()).unwrap()
    );
    assert_eq!(
        wire["intent"]["stopped_join"]["registration"],
        serde_json::to_value(scenario.join_registration.head()).unwrap()
    );
    assert_eq!(wire["intent"]["failure"], original_failure);
    assert!(wire["intent"]["failure_close"].is_null());
    assert_eq!(
        wire["intent"]["frames"],
        json!(
            active
                .open_frames()
                .iter()
                .map(|frame| json!({
                    "checkpoint":frame.checkpoint(),
                    "entry_digest":frame.entry_digest(),
                    "caller":frame.caller(),
                    "caller_binding_digest":frame.caller_binding_digest(),
                }))
                .collect::<Vec<_>>()
        )
    );
    assert_eq!(
        wire["intent"]["child_account"],
        json!({
            "digest":account.digest(),
            "direct_usage":account.direct_usage(),
            "direct_head":account.direct_head(),
        })
    );

    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), scenario.owner);
    assert_eq!(history.records()[0].start().fence(), &scenario.fence);
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    let join = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(join.request(), &scenario.join_request);
    assert_eq!(join.registration(), &scenario.join_registration);
    assert!(join.binding().is_none());
    assert!(join.head().is_none());
    assert!(join.consumed().is_none());
    assert!(
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .is_none()
    );
    let child = Box::pin(store.load_child_run(key)).await.unwrap();
    assert_eq!(child.settlement().unwrap().usage(), &scenario.child_usage);
    let settled = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.children(), account.children());
    assert_eq!(settled.direct_usage(), &scenario.direct);
    assert_eq!(settled.direct_head(), &confirmation.head());
    assert_eq!(
        settled.delegated_usage().unwrap(),
        scenario.child_usage.cumulative_only()
    );
    let mut original_account = serde_json::to_value(&account).unwrap();
    let mut final_account = serde_json::to_value(&settled).unwrap();
    for field in ["direct_usage", "direct_head", "digest"] {
        original_account.as_object_mut().unwrap().remove(field);
        final_account.as_object_mut().unwrap().remove(field);
    }
    assert_eq!(final_account, original_account);
    assert_eq!(
        Box::pin(store.include_child_usage(
            key.tenant_id(),
            key.parent_run_id(),
            scenario.direct.clone()
        ))
        .await
        .unwrap(),
        expected_usage
    );

    let after = snapshot(&pool, key.tenant_id()).await;
    assert_eq!(
        after.keys().collect::<Vec<_>>(),
        before.keys().collect::<Vec<_>>()
    );
    for (table, facts) in &before {
        if ![
            "runs",
            "run_events",
            "graph_frame_closures",
            "graph_frame_closed_callers",
            "graph_frame_stacks",
            "child_run_budget_accounts",
        ]
        .contains(&table.as_str())
        {
            assert_eq!(
                after.get(table),
                Some(facts),
                "untouched tenant table: {table}"
            );
        }
    }
    let table_len = |facts: &Snapshot, table: &str| {
        serde_json::from_str::<Vec<Value>>(&facts[table])
            .unwrap()
            .len()
    };
    assert_eq!(table_len(&before, "graph_frame_closures"), 0);
    assert_eq!(table_len(&after, "graph_frame_closures"), 1);
    assert_eq!(table_len(&before, "graph_frame_closed_callers"), 0);
    assert_eq!(
        table_len(&after, "graph_frame_closed_callers"),
        active.open_frames().len()
    );
    for table in ["graph_frame_stacks", "child_run_budget_accounts"] {
        assert_eq!(table_len(&before, table), 1);
        assert_eq!(table_len(&after, table), 1);
    }
    let parent_run = key.parent_run_id().to_string();
    let other_runs = |facts: &Snapshot| {
        serde_json::from_str::<Vec<Value>>(&facts["runs"])
            .unwrap()
            .into_iter()
            .filter(|row| row["run_id"].as_str() != Some(parent_run.as_str()))
            .collect::<Vec<_>>()
    };
    assert_eq!(other_runs(&after), other_runs(&before));
    let events_before: Vec<Value> = serde_json::from_str(&before["run_events"]).unwrap();
    let events_after: Vec<Value> = serde_json::from_str(&after["run_events"]).unwrap();
    assert_eq!(events_after.len(), events_before.len() + 2);
    for event in events_before {
        assert!(events_after.contains(&event));
    }
    // Forbidden fresh claims and ordinary writes are attempted through public
    // Store APIs; all owning reads and denied mutations retain terminal facts.
    assert!(matches!(
        store
            .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    for fence in [&scenario.fence, &f2] {
        assert!(matches!(
            store
                .append_worker(
                    worker_append_kind(
                        key.tenant_id(),
                        key.parent_run_id(),
                        confirmation.head(),
                        fence,
                        "forbidden-after-cancellation"
                    ),
                    RunProjection::Unchanged,
                )
                .await,
            Err(StoreError::NoActiveLease)
        ));
    }
    assert_eq!(snapshot(&pool, key.tenant_id()).await, after);
    let verified = Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fingerprint(&verified), fingerprint(&closure));
    assert_eq!(v2_bytes(&pool, &scenario).await, bytes);
    assert_eq!(snapshot(&pool, key.tenant_id()).await, after);
    assert_eq!(applications.load(Ordering::SeqCst), 0);
    assert_eq!(evidence_calls.load(Ordering::SeqCst), 1);
    println!(
        "STATEKNOT_SCOPED_JOIN_F2_LIFECYCLE_EVIDENCE={}",
        json!({
            "profile":"scoped-registered-join-agent-loop-cancellation-v1",
            "qualification":"same-process-real-AgentLoop-only",
            "terminal_status":terminal.lifecycle().status(),
            "terminal_revision":terminal.lifecycle().revision(),
            "closure_head":closure.event().head(),
            "confirmation_head":confirmation.head(),
            "actual_new_parent_events":page.events().len(),
            "closure_version":wire["version"],
            "closure_bytes_digest":Digest::sha256(&bytes),
            "direct":closure.direct_usage(),
            "delegated":settled.delegated_usage().unwrap(),
            "terminal_usage":terminal.lifecycle().terminal_usage(),
            "ordinary_owner_status":history.records()[0].status(),
            "ordinary_owner_completed":history.records()[0].completion().is_some(),
            "join_published":join.head().is_some(),
            "join_consumed":join.consumed().is_some(),
            "application_calls":applications.load(Ordering::SeqCst),
            "evidence_calls":evidence_calls.load(Ordering::SeqCst),
        })
    );
    pool.close().await;
    store.close().await;
}
