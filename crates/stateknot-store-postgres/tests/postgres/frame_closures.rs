// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Complete framework aborts preserve original decisions and physical starts.
use super::frame_barriers::{enter, pool};
use super::*;
use stateknot_core::{GraphFrameEntryPlan, NodeWait, NodeWaits, RunFence};
use stateknot_store_postgres::GraphFrameClosureCommitOutcome;
use stateknot_store_postgres::{StoredGraphFrameEntry, StoredRun};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn whole_closure_cancels_real_caller_without_replacing_its_fence_or_root_state() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, _, fence) = Box::pin(enter(&store, "frame-close-cancel", false)).await;
    let before = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let original = cancellation_request(before.journal_head().unwrap().recorded_at());
    let transition = RunTransition::RequestCancellation {
        request: original.clone(),
    };
    let append = control_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        JournalExpectation::exact(before.journal_head().unwrap().clone()),
        33_001,
    );
    store
        .append_control_plane(
            append,
            RunProjection::transition(before.lifecycle().revision(), transition),
        )
        .await
        .unwrap();
    let requested = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let observed = requested.journal_head().unwrap().clone();
    let revision = requested.lifecycle().revision();
    let event_id = EventId::generate();
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        event_id,
        observed.clone(),
        revision,
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        closed,
        GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let record = closed.record();
    assert_eq!(record.completions().len(), 1);
    let completed = &record.completions()[0];
    assert_eq!(completed.start(), &entry.entry().start().head());
    assert_eq!(completed.start().fence(), &fence);
    assert_eq!(completed.status(), NodeAttemptStatus::Failed);
    assert_eq!(completed.usage(), &BudgetUsage::zero());
    assert_eq!(
        serde_json_canonicalizer::to_vec(record.failure()).unwrap(),
        serde_json_canonicalizer::to_vec(original.failure()).unwrap()
    );
    let reloaded = store
        .load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            entry.entry().start().attempt_id(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json_canonicalizer::to_vec(reloaded.completion().unwrap()).unwrap(),
        serde_json_canonicalizer::to_vec(completed).unwrap()
    );
    assert!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    let saved = Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.digest(), record.digest());
    let retry = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        entry.event().head(),
        admission.run().lifecycle().revision(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, GraphFrameClosureCommitOutcome::Existing(_)));
    assert_eq!(retry.record().digest(), record.digest());
    let db = pool().await;
    let ordinary: i64 = query_scalar(
        "SELECT count(*) FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(ordinary, 0);
    let results: i64 = query_scalar(
        "SELECT count(*) FROM stateknot.pending_node_results WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(results, 0);
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    assert!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    // Root termination cannot rewrite the frozen COMPLETE direct usage.
    let rejected = confirm(&store, &fence, BudgetUsage::zero()).await;
    assert!(matches!(
        rejected,
        Err(StoreError::IncompleteChildAccounting)
    ));
    confirm(&store, &fence, record.direct_usage().clone())
        .await
        .unwrap();
    let terminal = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        terminal.lifecycle().terminal_usage(),
        Some(record.direct_usage())
    );
    let after = Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.digest(), record.digest());
    assert!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
            .await
            .is_err()
    );
    store.verify_schema().await.unwrap();
    store.close().await;
}

async fn request_cancel(store: &PostgresStore, fence: &RunFence) -> StoredRun {
    let before = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    store
        .append_control_plane(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(before.journal_head().unwrap().clone()),
                33_010,
            ),
            RunProjection::transition(
                before.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(before.journal_head().unwrap().recorded_at()),
                },
            ),
        )
        .await
        .unwrap();
    store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
}
async fn confirm(
    store: &PostgresStore,
    fence: &RunFence,
    usage: BudgetUsage,
) -> Result<AppendOutcome, StoreError> {
    let before = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    store
        .append_control_plane(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(before.journal_head().unwrap().clone()),
                33_011,
            ),
            RunProjection::transition(
                before.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: before.journal_head().unwrap().recorded_at(),
                    usage,
                },
            ),
        )
        .await
}
fn failure_append(fence: &RunFence, head: JournalHead, worker: bool) -> JournalAppend {
    let example = payload(33_012);
    let kind = if worker {
        "run-failure-close-requested"
    } else {
        "run-failure-close-completed"
    };
    let payload = JournalPayload::new(
        example.schema().clone(),
        JournalEventKind::new(kind).unwrap(),
        BoundedJson::try_from(json!({"version":1})).unwrap(),
    )
    .unwrap();
    let intent = if worker {
        JournalEventIntent::worker(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            fence.clone(),
            payload,
        )
    } else {
        JournalEventIntent::control_plane(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            payload,
        )
    }
    .unwrap();
    JournalAppend::new(JournalExpectation::exact(head), intent).unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn sealed_failure_closes_current_caller_and_root_retains_original_failure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, _, old) = Box::pin(enter(&store, "frame-close-failure", false)).await;
    let fence = store
        .supersede_lease(old.tenant_id(), old.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let binding = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        fence.clone(),
        entry.event().head(),
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let usage = super::frame_callers::charged(&entry.direct_usage_after().unwrap(), &binding);
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("frame.original_failure").unwrap(),
        FailureOrigin::new("test.frame").unwrap(),
        FailureMessage::new("Original sealed failure survives closure and root termination.")
            .unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let decision = Box::pin(store.request_run_failure_close(
        &admission.checkpoint().head(),
        failure.clone(),
        usage.clone(),
        failure_append(&fence, binding.event().head(), true),
    ))
    .await
    .unwrap();
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(current.lease().is_none());
    // A sealed decision alone cannot hide still-open logical frames.
    assert!(matches!(
        Box::pin(store.complete_run_failure_close(failure_append(
            &fence,
            current.journal_head().unwrap().clone(),
            false
        )))
        .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        current.journal_head().unwrap().clone(),
        current.lifecycle().revision(),
        usage.clone(),
    ))
    .await
    .unwrap();
    assert_eq!(
        closed.record().completions()[0].start(),
        &binding.attempt().start().head()
    );
    assert_eq!(closed.record().completions()[0].start().fence(), &fence);
    assert_eq!(
        serde_json_canonicalizer::to_vec(closed.record().failure()).unwrap(),
        serde_json_canonicalizer::to_vec(&failure).unwrap()
    );
    assert!(
        store
            .load_node_attempt(
                old.tenant_id(),
                &old.run_id(),
                entry.entry().start().attempt_id()
            )
            .await
            .unwrap()
            .completion()
            .is_none()
    );
    Box::pin(store.complete_run_failure_close(failure_append(
        &fence,
        closed.record().event().head(),
        false,
    )))
    .await
    .unwrap();
    let done = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(done.lifecycle().status(), RunStatus::Failed);
    assert_eq!(done.lifecycle().terminal_usage(), Some(&usage));
    let loaded = Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.digest(), closed.record().digest());
    let original = store
        .load_run_failure_close(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .unwrap();
    assert!(original.completed_at().is_some());
    assert_eq!(
        original.registration().head(),
        decision.record().registration().head()
    );
    assert_eq!(
        serde_json_canonicalizer::to_vec(original.failure()).unwrap(),
        serde_json_canonicalizer::to_vec(&failure).unwrap()
    );
    let retry = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        entry.event().head(),
        admission.run().lifecycle().revision(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, GraphFrameClosureCommitOutcome::Existing(_)));
    assert_eq!(retry.record().digest(), loaded.digest());
    assert!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn waiting_closure_requires_authenticated_abandonment_and_preserves_wait_evidence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let waits = NodeWaits::try_new([NodeWait::timer(
        TimerId::generate(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(2_000_000_000_000_000).unwrap(),
    )])
    .unwrap();
    let (_, entry, graph, fence, plan, observed, revision) = Box::pin(
        super::frame_waits::prepared(&store, "frame-close-wait", waits),
    )
    .await;
    let saved = Box::pin(super::frame_waits::suspend(
        &store,
        plan,
        &fence,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &graph,
    ))
    .await;
    let waiting = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(
        Box::pin(store.close_graph_frames(
            fence.tenant_id(),
            fence.run_id(),
            EventId::generate(),
            saved.event().head(),
            waiting.lifecycle().revision(),
            saved.direct_usage_after().unwrap()
        ))
        .await
        .is_err()
    );
    let transition = RunTransition::RequestCancellation {
        request: cancellation_request(saved.event().recorded_at()),
    };
    store
        .append_control_plane_abandon_waits(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(saved.event().head()),
                33_015,
            ),
            waiting.lifecycle().revision(),
            transition,
        )
        .await
        .unwrap();
    let db = pool().await;
    let wait_before: String = query_scalar("SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id)::text FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        current.journal_head().unwrap().clone(),
        current.lifecycle().revision(),
        saved.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    confirm(&store, &fence, closed.record().direct_usage().clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Cancelled
    );
    let wait_after: String = query_scalar("SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id)::text FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(wait_after, wait_before);
    let loaded = Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.digest(), closed.record().digest());
    assert!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    db.close().await;
    store.close().await;
}

async fn seven(
    store: &PostgresStore,
    name: &str,
) -> (
    StoredAgentAdmission,
    Vec<StoredGraphFrameEntry>,
    RunFence,
    BudgetUsage,
) {
    let tenant = tenant(name);
    let run = RunId::generate();
    let (_, leaf, _) = super::frame_transactions::graphs();
    let mut definitions = vec![leaf];
    for level in (0..7).rev() {
        definitions.push(super::frame_transactions::caller_graph(
            &format!("close-level-{level}"),
            definitions.last().unwrap(),
            4096,
        ));
    }
    definitions.reverse();
    for definition in &definitions {
        store
            .register_graph_definition(tenant.clone(), definition.clone())
            .await
            .unwrap();
    }
    let admission = Box::pin(super::frame_transactions::admit(
        store,
        &tenant,
        run,
        &definitions[0],
        &definitions[1],
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let mut observed = admission.event().head();
    let mut usage = super::frame_transactions::initial_usage(&admission);
    let mut entries: Vec<StoredGraphFrameEntry> = Vec::new();
    for level in 0..7 {
        let parent = &definitions[level];
        let target = &definitions[level + 1];
        let call = &parent.frame_calls().unwrap().calls()[0];
        let plan = if let Some(previous) = entries.last() {
            GraphFrameEntryPlan::for_frame(
                call,
                parent,
                previous.entry().checkpoint(),
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        } else {
            GraphFrameEntryPlan::for_root(
                call,
                parent,
                admission.checkpoint(),
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        }
        .unwrap();
        let stateknot_store_postgres::GraphFrameEntryCommitOutcome::Committed(entry) =
            Box::pin(store.enter_graph_frame(
                plan,
                EventId::generate(),
                observed,
                usage,
                &AcceptGraphSchemas,
            ))
            .await
            .unwrap()
        else {
            panic!("fresh entry");
        };
        observed = entry.event().head();
        usage = entry.direct_usage_after().unwrap();
        entries.push(entry);
    }
    (admission, entries, fence, usage)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn seven_level_closure_preserves_mixed_physical_fences_and_lifetime_counter() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entries, old, usage) = Box::pin(seven(&store, "frame-close-seven")).await;
    let fence = store
        .supersede_lease(old.tenant_id(), old.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let binding = Box::pin(store.rebind_graph_frame_caller(
        entries.last().unwrap().entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        fence.clone(),
        entries.last().unwrap().event().head(),
        usage.clone(),
    ))
    .await
    .unwrap();
    let usage = super::frame_callers::charged(&usage, &binding);
    let requested = request_cancel(&store, &fence).await;
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        requested.journal_head().unwrap().clone(),
        requested.lifecycle().revision(),
        usage.clone(),
    ))
    .await
    .unwrap();
    assert_eq!(closed.record().completions().len(), 7);
    for (index, completion) in closed.record().completions().iter().enumerate() {
        let expected = if index == 6 {
            binding.attempt().start().head()
        } else {
            entries[index].entry().start().head()
        };
        assert_eq!(completion.start(), &expected);
        assert_eq!(
            completion.start().fence(),
            if index == 6 { &fence } else { &old }
        );
        let loaded = Box::pin(store.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            expected.attempt_id(),
        ))
        .await
        .unwrap();
        assert_eq!(loaded.completion().unwrap().digest(), completion.digest());
    }
    confirm(&store, &fence, usage).await.unwrap();
    let db = pool().await;
    let stack: (String, i32) = sqlx_core::query_as::query_as("SELECT active_namespace,lifetime_starts FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(stack, (String::new(), 7));
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    assert_eq!(
        Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .unwrap()
            .digest(),
        closed.record().digest()
    );
    assert!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    db.close().await;
    store.close().await;
}

async fn facts(db: &PgPool, fence: &RunFence) -> Vec<String> {
    let mut result = Vec::new();
    for table in [
        "runs",
        "run_events",
        "run_checkpoints",
        "graph_frame_stacks",
        "graph_frame_heads",
        "graph_frame_entries",
        "graph_frame_caller_bindings",
        "graph_frame_returns",
        "graph_frame_closures",
        "graph_frame_closed_callers",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "run_failure_closes",
    ] {
        result.push(query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM stateknot.{table} t WHERE tenant_id=$1 AND run_id=$2"))
            .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(db).await.unwrap());
    }
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_closers_recover_one_original_decision_with_unchanged_candidates() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-close-race", false)).await;
    let requested = request_cancel(&store, &fence).await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let fence = fence.clone();
        let observed = requested.journal_head().unwrap().clone();
        let revision = requested.lifecycle().revision();
        let usage = entry.direct_usage_after().unwrap();
        tasks.spawn(async move {
            let event_id = EventId::generate();
            for retry in 0..8 {
                let result = Box::pin(store.close_graph_frames(
                    fence.tenant_id(),
                    fence.run_id(),
                    event_id,
                    observed.clone(),
                    revision,
                    usage.clone(),
                ))
                .await;
                if retry < 7 && result.as_ref().is_err_and(StoreError::is_retryable) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                return result;
            }
            unreachable!("bounded retry returns its final outcome")
        });
    }
    let (mut committed, mut existing) = (0, 0);
    let mut digest = None;
    while let Some(result) = tasks.join_next().await {
        let outcome = result.unwrap().unwrap();
        match &outcome {
            GraphFrameClosureCommitOutcome::Committed(_) => committed += 1,
            GraphFrameClosureCommitOutcome::Existing(_) => existing += 1,
        }
        assert_eq!(
            *digest.get_or_insert(outcome.record().digest()),
            outcome.record().digest()
        );
    }
    assert_eq!((committed, existing), (1, 23));
    let db = pool().await;
    let counts: (i64, i64, i64) = query_as("SELECT (SELECT count(*) FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM stateknot.graph_frame_closed_callers WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM stateknot.run_events WHERE tenant_id=$1 AND run_id=$2 AND event_kind='graph-frames-closed')")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(counts, (1, 1, 1));
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn every_closure_write_and_deferred_guard_fault_rolls_back_the_whole_seven_frame_stack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entries, fence, usage) = Box::pin(seven(&store, "frame-close-faults")).await;
    let requested = request_cancel(&store, &fence).await;
    let db = pool().await;
    let before = facts(&db, &fence).await;
    query("CREATE FUNCTION stateknot.test_frame_closure_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected whole frame closure fault'; END $$").execute(&db).await.unwrap();
    let mut points = vec![
        ("run_events", "INSERT", String::new()),
        ("graph_frame_closures", "INSERT", String::new()),
        ("graph_frame_stacks", "UPDATE", String::new()),
        (
            "runs",
            "UPDATE",
            "WHEN (OLD.lease_attempt_id IS NOT NULL AND NEW.lease_attempt_id IS NULL)".to_owned(),
        ),
        (
            "runs",
            "UPDATE",
            "WHEN (NEW.journal_sequence<>OLD.journal_sequence)".to_owned(),
        ),
    ];
    for entry in &entries {
        points.push((
            "graph_frame_closed_callers",
            "INSERT",
            format!(
                "WHEN (NEW.graph_namespace='{}')",
                entry.entry().checkpoint().frame().namespace().as_str()
            ),
        ));
    }
    for (table, action, condition) in points {
        query(&format!("CREATE TRIGGER zz_test_frame_closure_fault BEFORE {action} ON stateknot.{table} FOR EACH ROW {condition} EXECUTE FUNCTION stateknot.test_frame_closure_fault()"))
            .execute(&db).await.unwrap();
        let outcome = Box::pin(store.close_graph_frames(
            fence.tenant_id(),
            fence.run_id(),
            EventId::generate(),
            requested.journal_head().unwrap().clone(),
            requested.lifecycle().revision(),
            usage.clone(),
        ))
        .await;
        query(&format!(
            "DROP TRIGGER zz_test_frame_closure_fault ON stateknot.{table}"
        ))
        .execute(&db)
        .await
        .unwrap();
        assert!(
            matches!(outcome, Err(StoreError::Database { .. })),
            "fault must hit {table} {condition}: {outcome:?}"
        );
        assert_eq!(
            facts(&db, &fence).await,
            before,
            "partial closure after {table} {condition}"
        );
    }
    query("CREATE CONSTRAINT TRIGGER zz_test_frame_closure_fault AFTER INSERT ON stateknot.graph_frame_closures DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_closure_fault()")
        .execute(&db).await.unwrap();
    let outcome = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        requested.journal_head().unwrap().clone(),
        requested.lifecycle().revision(),
        usage.clone(),
    ))
    .await;
    query("DROP TRIGGER zz_test_frame_closure_fault ON stateknot.graph_frame_closures")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_frame_closure_fault()")
        .execute(&db)
        .await
        .unwrap();
    assert!(matches!(outcome, Err(StoreError::Database { .. })));
    assert_eq!(
        facts(&db, &fence).await,
        before,
        "deferred failure must roll back all components"
    );
    assert_eq!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .unwrap()
            .open_frames()
            .len(),
        7
    );
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        requested.journal_head().unwrap().clone(),
        requested.lifecycle().revision(),
        usage,
    ))
    .await
    .unwrap();
    assert_eq!(closed.record().completions().len(), 7);
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn undecided_unpriced_stale_and_unfinished_ordinary_work_cannot_be_closed() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-close-negative", false)).await;
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(store.close_graph_frames(
            fence.tenant_id(),
            fence.run_id(),
            EventId::generate(),
            entry.event().head(),
            current.lifecycle().revision(),
            entry.direct_usage_after().unwrap()
        ))
        .await,
        Err(StoreError::RunNotRunnable)
    ));
    let planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap()
            .finish(entry.event().head(), entry.event().recorded_at())
            .unwrap();
    Box::pin(
        store.start_recovered_graph_frame_node_attempt(
            worker_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(entry.event().head()),
                fence.clone(),
                33_020,
            ),
            &planner,
            entry
                .entry()
                .checkpoint()
                .checkpoint()
                .ready_nodes()
                .iter()
                .next()
                .unwrap(),
            AttemptId::generate(),
        ),
    )
    .await
    .unwrap();
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(
            store.request_run_failure_close(
                &store
                    .load_current_checkpoint(fence.tenant_id(), fence.run_id())
                    .await
                    .unwrap()
                    .unwrap()
                    .head(),
                terminal_run_failure(
                    EventId::generate(),
                    current.journal_head().unwrap().recorded_at()
                )
                .failure()
                .clone(),
                entry.direct_usage_after().unwrap(),
                failure_append(&fence, current.journal_head().unwrap().clone(), true)
            )
        )
        .await,
        Err(StoreError::InvalidRunFailureClose)
    ));
    let requested = request_cancel(&store, &fence).await;
    let db = pool().await;
    let before = facts(&db, &fence).await;
    let unpriced = entry
        .direct_usage_after()
        .unwrap()
        .checked_accumulate(
            &BudgetUsage::builder()
                .unpriced_cost_events(stateknot_core::ExecutionCount::new(1))
                .build()
                .unwrap(),
        )
        .unwrap();
    for (head, usage) in [
        (entry.event().head(), entry.direct_usage_after().unwrap()),
        (requested.journal_head().unwrap().clone(), unpriced),
        (
            requested.journal_head().unwrap().clone(),
            entry.direct_usage_after().unwrap(),
        ),
    ] {
        assert!(
            Box::pin(store.close_graph_frames(
                fence.tenant_id(),
                fence.run_id(),
                EventId::generate(),
                head,
                requested.lifecycle().revision(),
                usage
            ))
            .await
            .is_err()
        );
        assert_eq!(facts(&db, &fence).await, before);
    }
    assert!(matches!(
        confirm(&store, &fence, entry.direct_usage_after().unwrap()).await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn damaged_closed_caller_projection_is_rejected_by_all_whole_readers_and_retries() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-close-corrupt", false)).await;
    let requested = request_cancel(&store, &fence).await;
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        requested.journal_head().unwrap().clone(),
        requested.lifecycle().revision(),
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let db = pool().await;
    let before = facts(&db, &fence).await;
    query("ALTER TABLE stateknot.graph_frame_closed_callers DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.graph_frame_closed_callers SET entry_digest=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(Digest::sha256(b"substituted entry").as_bytes()).execute(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_frame_closed_callers ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    let damaged = facts(&db, &fence).await;
    assert!(matches!(
        Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id())).await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id())).await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        Box::pin(store.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            entry.entry().start().attempt_id()
        ))
        .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        Box::pin(store.close_graph_frames(
            fence.tenant_id(),
            fence.run_id(),
            EventId::generate(),
            entry.event().head(),
            requested.lifecycle().revision(),
            BudgetUsage::zero()
        ))
        .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert_eq!(facts(&db, &fence).await, damaged);
    query("ALTER TABLE stateknot.graph_frame_closed_callers DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.graph_frame_closed_callers SET entry_digest=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(entry.digest().as_bytes()).execute(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_frame_closed_callers ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(facts(&db, &fence).await, before);
    assert_eq!(
        Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .unwrap()
            .digest(),
        closed.record().digest()
    );
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn populated_schema_32_upgrade_keeps_waits_returns_and_checksums_before_whole_closure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let original = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_frame_close_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url_with_name(&original, "postgres"))
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = database_url_with_name(&original, &name);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
    let waits = NodeWaits::try_new([NodeWait::timer(
        TimerId::generate(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(2_000_000_000_000_000).unwrap(),
    )])
    .unwrap();
    let (_, entry, graph, fence, plan, observed, revision) = Box::pin(
        super::frame_waits::prepared(&store, "frame-close-upgrade-wait", waits),
    )
    .await;
    let saved = Box::pin(super::frame_waits::suspend(
        &store,
        plan,
        &fence,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &graph,
    ))
    .await;
    let (_, prior_entry, prior_graph, prior_fence, prepared, prior_barrier) = Box::pin(
        super::frame_returns::finished(&store, "frame-close-upgrade-return"),
    )
    .await;
    let stateknot_store_postgres::GraphFrameReturnCommitOutcome::Committed(returned) =
        Box::pin(store.return_graph_frame(
            prepared,
            EventId::generate(),
            prior_fence.clone(),
            prior_barrier.event().head(),
            prior_barrier.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: prior_graph.reducer().clone(),
            },
        ))
        .await
        .unwrap()
    else {
        panic!("fresh upgrade return");
    };
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let checksums: Vec<(i64, Vec<u8>)> = query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=32 ORDER BY version",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    let before = facts(&fixture, &fence).await;
    let prior = facts(&fixture, &prior_fence).await;
    let waits_before: String = query_scalar("SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id)::text FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&fixture).await.unwrap();
    store.close().await;
    // Source reconstruction is distinct from retained N-1/N-2 executable qualification.
    sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_closures.sql"))
        .execute(&fixture)
        .await
        .unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        32
    );
    assert!(matches!(
        PostgresStore::connect(&url, options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    assert_eq!(checksums, query_as::<_, (i64, Vec<u8>)>("SELECT version,checksum FROM public._sqlx_migrations WHERE version<=32 ORDER BY version").fetch_all(&fixture).await.unwrap());
    assert_eq!(facts(&fixture, &fence).await, before);
    assert_eq!(facts(&fixture, &prior_fence).await, prior);
    assert_eq!(query_scalar::<_, String>("SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id)::text FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&fixture).await.unwrap(), waits_before);
    assert_eq!(
        Box::pin(upgraded.load_graph_frame_return(
            prior_fence.tenant_id(),
            prior_fence.run_id(),
            prior_entry.entry().checkpoint().frame().namespace()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        returned.digest()
    );
    let active = Box::pin(upgraded.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint(), saved.checkpoint());
    upgraded
        .append_control_plane_abandon_waits(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(saved.event().head()),
                33_030,
            ),
            active.run().lifecycle().revision(),
            RunTransition::RequestCancellation {
                request: cancellation_request(saved.event().recorded_at()),
            },
        )
        .await
        .unwrap();
    let current = upgraded
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let closed = Box::pin(upgraded.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        current.journal_head().unwrap().clone(),
        current.lifecycle().revision(),
        saved.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    confirm(&upgraded, &fence, closed.record().direct_usage().clone())
        .await
        .unwrap();
    let mut tx = fixture.begin().await.unwrap();
    assert!(
        sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_closures.sql"))
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        Box::pin(upgraded.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .unwrap()
            .digest(),
        closed.record().digest()
    );
    upgraded.verify_schema().await.unwrap();
    upgraded.close().await;
    fixture.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn expired_execution_deadline_and_exhausted_step_budget_still_allow_bounded_cleanup() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let tenant = tenant("frame-close-expired");
    let run = RunId::generate();
    let (parent, child, call) = super::frame_transactions::graphs();
    let deadline = timestamp_after(Duration::from_secs(8));
    let (_, limits) = agent_fixture_request_and_budget();
    let limits = limits
        .with_deadline(deadline)
        .with_graph_steps(stateknot_core::ExecutionCount::new(1));
    let admission = Box::pin(super::frame_transactions::admit_with_budget_override(
        &store,
        &tenant,
        run,
        &parent,
        &child,
        None,
        Some(limits),
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let stateknot_store_postgres::GraphFrameEntryCommitOutcome::Committed(entry) =
        Box::pin(store.enter_graph_frame(
            plan,
            EventId::generate(),
            admission.event().head(),
            super::frame_transactions::initial_usage(&admission),
            &AcceptGraphSchemas,
        ))
        .await
        .unwrap()
    else {
        panic!("fresh entry");
    };
    assert_eq!(entry.direct_usage_after().unwrap().graph_steps().get(), 1);
    let now = store.observe_database_clock().await.unwrap();
    if now < deadline {
        tokio::time::sleep(Duration::from_micros(
            u64::try_from(deadline.unix_micros() - now.unix_micros()).unwrap() + 1000,
        ))
        .await;
    }
    assert!(store.observe_database_clock().await.unwrap() >= deadline);
    assert!(store.observe_live_lease(&fence).await.is_err());
    let requested = request_cancel(&store, &fence).await;
    let closed = Box::pin(store.close_graph_frames(
        &tenant,
        run,
        EventId::generate(),
        requested.journal_head().unwrap().clone(),
        requested.lifecycle().revision(),
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(
        closed.record().direct_usage(),
        &entry.direct_usage_after().unwrap()
    );
    confirm(&store, &fence, closed.record().direct_usage().clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .load_run(&tenant, run)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Cancelled
    );
    assert_eq!(
        Box::pin(store.load_graph_frame_closure(&tenant, run))
            .await
            .unwrap()
            .unwrap()
            .digest(),
        closed.record().digest()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn real_unknown_tool_effect_and_executing_model_block_closure_without_replaying_work() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    for tool_case in [true, false] {
        let (_, entry, _, fence) = Box::pin(enter(
            &store,
            if tool_case {
                "frame-close-unknown-tool"
            } else {
                "frame-close-model"
            },
            false,
        ))
        .await;
        let planner =
            ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
                .unwrap()
                .finish(entry.event().head(), entry.event().recorded_at())
                .unwrap();
        let started = Box::pin(
            store.start_recovered_graph_frame_node_attempt(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(entry.event().head()),
                    fence.clone(),
                    33_040,
                ),
                &planner,
                entry
                    .entry()
                    .checkpoint()
                    .checkpoint()
                    .ready_nodes()
                    .iter()
                    .next()
                    .unwrap(),
                AttemptId::generate(),
            ),
        )
        .await
        .unwrap();
        let activation = started.attempt().start().activation().clone();
        let append = |head| {
            worker_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(head),
                fence.clone(),
                33_041,
            )
        };
        if tool_case {
            let intent =
                tool_invocation_intent_for_activation(activation, InvocationId::generate());
            let prepared = Box::pin(
                store.prepare_tool_invocation(append(started.event().head()), intent.clone()),
            )
            .await
            .unwrap();
            let attempt_id = AttemptId::generate();
            let executing = Box::pin(store.advance_tool_invocation(
                append(prepared.event().head()),
                &prepared.invocation().head(),
                ToolInvocationTransition::StartAttempt { attempt_id },
            ))
            .await
            .unwrap();
            let failure = Failure::new(
                FailureId::generate(),
                FailureCategory::AmbiguousExternalOutcome,
                "test.frame_unknown".parse().unwrap(),
                "test.frame".parse().unwrap(),
                "The original external effect requires reconciliation."
                    .parse()
                    .unwrap(),
                RetryAdvice::ReconcileFirst,
            )
            .unwrap();
            let error = ToolError::new(
                failure,
                ToolErrorPhase::Execution,
                ToolExternalEffect::Unknown,
                ToolErrorProvenance::new(
                    intent.invocation_id(),
                    attempt_id,
                    intent.descriptor().metadata().identity().clone(),
                ),
            )
            .unwrap();
            let unknown = Box::pin(store.advance_tool_invocation(
                append(executing.event().head()),
                &executing.invocation().head(),
                ToolInvocationTransition::RecordError { error },
            ))
            .await
            .unwrap();
            assert_eq!(unknown.invocation().status(), ToolInvocationStatus::Unknown);
        } else {
            let intent =
                model_invocation_intent_for_activation(activation, InvocationId::generate());
            let prepared =
                Box::pin(store.prepare_model_invocation(append(started.event().head()), intent))
                    .await
                    .unwrap();
            let executing = Box::pin(store.advance_model_invocation(
                append(prepared.event().head()),
                &prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: AttemptId::generate(),
                },
            ))
            .await
            .unwrap();
            assert_eq!(
                executing.invocation().status(),
                ModelInvocationStatus::Executing
            );
        }
        let requested = request_cancel(&store, &fence).await;
        let before = facts(&db, &fence).await;
        let outcome = Box::pin(store.close_graph_frames(
            fence.tenant_id(),
            fence.run_id(),
            EventId::generate(),
            requested.journal_head().unwrap().clone(),
            requested.lifecycle().revision(),
            entry.direct_usage_after().unwrap(),
        ))
        .await;
        if tool_case {
            assert!(matches!(
                outcome,
                Err(StoreError::CheckpointBlockedByToolInvocation)
            ));
        } else {
            assert!(matches!(
                outcome,
                Err(StoreError::CheckpointBlockedByModelInvocation)
            ));
        }
        assert_eq!(facts(&db, &fence).await, before);
        assert!(
            Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
                .await
                .unwrap()
                .is_none()
        );
    }
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_tool_and_model_terminal_bindings_reload_after_complete_frame_and_root_closure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) =
        Box::pin(enter(&store, "frame-close-providers-settled", false)).await;
    let planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap()
            .finish(entry.event().head(), entry.event().recorded_at())
            .unwrap();
    let started = Box::pin(
        store.start_recovered_graph_frame_node_attempt(
            worker_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(entry.event().head()),
                fence.clone(),
                33_050,
            ),
            &planner,
            entry
                .entry()
                .checkpoint()
                .checkpoint()
                .ready_nodes()
                .iter()
                .next()
                .unwrap(),
            AttemptId::generate(),
        ),
    )
    .await
    .unwrap();
    let activation = started.attempt().start().activation().clone();
    let append = |head| {
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(head),
            fence.clone(),
            33_051,
        )
    };
    let tool_intent =
        tool_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
    let prepared = Box::pin(
        store.prepare_tool_invocation(append(started.event().head()), tool_intent.clone()),
    )
    .await
    .unwrap();
    let tool_attempt = AttemptId::generate();
    let executing = Box::pin(store.advance_tool_invocation(
        append(prepared.event().head()),
        &prepared.invocation().head(),
        ToolInvocationTransition::StartAttempt {
            attempt_id: tool_attempt,
        },
    ))
    .await
    .unwrap();
    let tool = Box::pin(store.advance_tool_invocation(
        append(executing.event().head()),
        &executing.invocation().head(),
        ToolInvocationTransition::RecordResult {
            result: tool_result(&tool_intent, tool_attempt),
        },
    ))
    .await
    .unwrap();
    let model_intent =
        model_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
    let prepared =
        Box::pin(store.prepare_model_invocation(append(tool.event().head()), model_intent.clone()))
            .await
            .unwrap();
    let model_attempt = AttemptId::generate();
    let executing = Box::pin(store.advance_model_invocation(
        append(prepared.event().head()),
        &prepared.invocation().head(),
        ModelInvocationTransition::StartAttempt {
            attempt_id: model_attempt,
        },
    ))
    .await
    .unwrap();
    let response = model_response(&model_intent, model_attempt);
    let usage = BudgetUsage::builder()
        .model_attempts(stateknot_core::ExecutionCount::new(1))
        .model_turns(stateknot_core::ExecutionCount::new(1))
        .input_tokens(response.usage().input_tokens())
        .cached_input_tokens(
            response
                .usage()
                .cached_input_tokens()
                .unwrap_or(stateknot_core::TokenCount::new(0)),
        )
        .output_tokens(response.usage().output_tokens())
        .reasoning_tokens(
            response
                .usage()
                .reasoning_tokens()
                .unwrap_or(stateknot_core::TokenCount::new(0)),
        )
        .tool_calls(stateknot_core::ExecutionCount::new(1))
        .write_calls(stateknot_core::ExecutionCount::new(1))
        .build()
        .unwrap();
    let model = Box::pin(store.advance_model_invocation(
        append(executing.event().head()),
        &executing.invocation().head(),
        ModelInvocationTransition::RecordResponse { response },
    ))
    .await
    .unwrap();
    let bindings = NodeInvocationBindings::try_new(
        &activation,
        [
            NodeInvocationBinding::from_tool(tool.invocation()).unwrap(),
            NodeInvocationBinding::from_model(model.invocation()).unwrap(),
        ],
    )
    .unwrap();
    let intent = PendingNodeResultIntent::new(
        activation,
        NodeStateChange::Unchanged,
        super::frame_barriers::terminal(&graph),
        bindings,
    )
    .unwrap();
    Box::pin(store.succeed_node_attempt(
        append(model.event().head()),
        &started.attempt().start().head(),
        intent.clone(),
        usage.clone(),
    ))
    .await
    .unwrap();
    let requested = request_cancel(&store, &fence).await;
    let direct = entry
        .direct_usage_after()
        .unwrap()
        .checked_accumulate(&usage)
        .unwrap();
    let closed = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        requested.journal_head().unwrap().clone(),
        requested.lifecycle().revision(),
        direct.clone(),
    ))
    .await
    .unwrap();
    confirm(&store, &fence, direct).await.unwrap();
    assert_eq!(
        Box::pin(store.load_tool_invocation(
            fence.tenant_id(),
            fence.run_id(),
            tool_intent.invocation_id()
        ))
        .await
        .unwrap()
        .head(),
        tool.invocation().head()
    );
    assert_eq!(
        Box::pin(store.load_model_invocation(
            fence.tenant_id(),
            fence.run_id(),
            model_intent.invocation_id()
        ))
        .await
        .unwrap()
        .head(),
        model.invocation().head()
    );
    let pending = Box::pin(store.load_pending_node_result(intent.activation()))
        .await
        .unwrap();
    assert_eq!(pending.intent(), &intent);
    assert_eq!(pending.intent().bindings().len(), 2);
    assert_eq!(
        Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .unwrap()
            .digest(),
        closed.record().digest()
    );
    store.close().await;
}
