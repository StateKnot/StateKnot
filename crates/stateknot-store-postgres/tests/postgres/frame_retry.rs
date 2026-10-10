// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real scoped retry gates never grant launch authority.
use super::frame_barriers::{enter, pool, snapshot};
use super::frame_recovery::claimed_plan;
use super::*;
use stateknot_core::{GraphFrameCheckpoint, ReadyNodeRecoveryPlan, RunFence};

fn append(plan: &ReadyNodeRecoveryPlan, event: EventId, journal: JournalHead) -> JournalAppend {
    worker_append(
        plan.fence().tenant_id().clone(),
        plan.fence().run_id(),
        event,
        JournalExpectation::exact(journal),
        plan.fence().clone(),
        24_000,
    )
}

async fn fail(
    store: &PostgresStore,
    plan: &ReadyNodeRecoveryPlan,
    node: &NodeId,
    delay: i64,
) -> NodeAttemptCommitOutcome {
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(plan, EventId::generate(), plan.journal_head().clone()),
        plan,
        node,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.scoped_retry").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("A physical scoped attempt failed before its retry boundary.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(delay).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(event);
    Box::pin(store.fail_node_attempt(
        append(plan, event, started.event().head()),
        &started.attempt().start().head(),
        failure,
        BudgetUsage::zero(),
    ))
    .await
    .unwrap()
}

async fn deferred(
    store: &PostgresStore,
    name: &str,
    delay: i64,
) -> (GraphFrameCheckpoint, RunFence, ReadyNodeRecoveryPlan) {
    let (_, entry, _, fence) = Box::pin(enter(store, name, false)).await;
    let base = entry.entry().checkpoint().clone();
    let initial = Box::pin(claimed_plan(store, &base, &fence)).await;
    Box::pin(fail(
        store,
        &initial,
        initial.nodes()[0].activation().node_id(),
        delay,
    ))
    .await;
    let plan = Box::pin(claimed_plan(store, &base, &fence)).await;
    assert_eq!(plan.nodes()[0].kind(), RecoveryNodeKind::Deferred);
    (base, fence, plan)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_retry_gate_preserves_root_history_queue_and_requires_new_start() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (base, fence, plan) = Box::pin(deferred(&store, "frame-retry-gate", 3_000)).await;
    let admin = pool().await;
    let before = snapshot(&admin, &fence).await;
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let queue_age = run.scheduler_ready_at();
    let not_before = plan.earliest_deferred_at().unwrap();
    assert_eq!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Scheduled { not_before }
    );
    let sleeping = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(sleeping.lease().is_none());
    assert_eq!(sleeping.scheduler_ready_at(), queue_age);
    assert_eq!(sleeping.scheduler_not_before(), Some(not_before));
    assert_eq!(sleeping.checkpoint(), run.checkpoint());
    assert_eq!(sleeping.journal_head(), run.journal_head());
    let after = snapshot(&admin, &fence).await;
    assert_eq!(before.0, after.0);
    assert_eq!(before.2, after.2);
    assert!(matches!(
        store
            .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotYetAvailable)
    ));
    let page = store
        .load_runnable_run_page(
            fence.tenant_id(),
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        page.records()
            .iter()
            .all(|row| row.run().lifecycle().provenance().run_id() != fence.run_id())
    );
    assert_eq!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Idempotent { not_before }
    );
    assert_eq!(after, snapshot(&admin, &fence).await);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let page = store
            .load_runnable_run_page(
                fence.tenant_id(),
                None,
                RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
            )
            .await
            .unwrap();
        if let Some(candidate) = page
            .records()
            .iter()
            .find(|row| row.run().lifecycle().provenance().run_id() == fence.run_id())
        {
            assert_eq!(Some(candidate.ready_at()), queue_age);
            assert_eq!(candidate.available_at(), not_before);
            break;
        }
        assert!(Instant::now() < deadline, "retry never became due");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // ACK remains an observation even after the gate becomes due.
    assert_eq!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Idempotent { not_before }
    );
    let next = store
        .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap();
    let next_fence = next.lease().fence().clone();
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await,
        Err(StoreError::StaleFence)
    ));
    let fresh = Box::pin(claimed_plan(&store, &base, &next_fence)).await;
    assert_eq!(
        fresh.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SafeRetry)
    );
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&fresh, EventId::generate(), fresh.journal_head().clone()),
        &fresh,
        fresh.nodes()[0].activation().node_id(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        started.attempt().start().activation(),
        fresh.nodes()[0].activation()
    );
    assert_ne!(started.attempt().start().fence(), &fence);
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .checkpoint(),
        run.checkpoint()
    );
    admin.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_due_under_run_lock_keeps_lease_and_all_projections() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (base, fence, plan) = Box::pin(deferred(&store, "frame-retry-locked-due", 2_000)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let mut held = db.begin().await.unwrap();
    query("SELECT run_id FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let scheduling = store.schedule_graph_frame_delayed_retry_wakeup(&plan);
    let release = async {
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        held.commit().await.unwrap();
    };
    let (outcome, ()) = tokio::join!(Box::pin(scheduling), release);
    assert_eq!(
        outcome.unwrap(),
        DelayedRetryScheduleOutcome::Due {
            not_before: plan.earliest_deferred_at().unwrap()
        }
    );
    assert_eq!(before, snapshot(&db, &fence).await);
    let fresh = Box::pin(claimed_plan(&store, &base, &fence)).await;
    assert_eq!(
        fresh.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SafeRetry)
    );
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_rejects_actionable_inflight_and_root_entry_point() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-retry-actionable", false)).await;
    let base = entry.entry().checkpoint();
    let initial = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&initial)).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(
            &initial,
            EventId::generate(),
            initial.journal_head().clone(),
        ),
        &initial,
        initial.nodes()[0].activation().node_id(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let running = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert_eq!(running.nodes()[0].kind(), RecoveryNodeKind::InFlight);
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&running)).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    assert_eq!(start.attempt().status(), NodeAttemptStatus::Executing);
    let (_, _, plan) = Box::pin(deferred(&store, "frame-retry-root-rejected", 3_000)).await;
    assert!(matches!(
        store.schedule_delayed_retry_wakeup(&plan).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_lost_ack_24_way_converges_without_new_work() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, fence, plan) = Box::pin(deferred(&store, "frame-retry-concurrent", 3_000)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let plan = plan.clone();
        tasks.spawn(async move {
            for _ in 0..8 {
                match Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await {
                    Err(error) if error.is_retryable() => tokio::task::yield_now().await,
                    result => return result,
                }
            }
            panic!("bounded transaction retry exhausted");
        });
    }
    let mut scheduled = 0;
    let mut idempotent = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap().unwrap() {
            DelayedRetryScheduleOutcome::Scheduled { .. } => scheduled += 1,
            DelayedRetryScheduleOutcome::Idempotent { .. } => idempotent += 1,
            other => panic!("unexpected concurrent result {other:?}"),
        }
    }
    assert_eq!((scheduled, idempotent), (1, 23));
    let after = snapshot(&db, &fence).await;
    assert_eq!(before.0, after.0);
    assert_eq!(before.2, after.2);
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_rederives_history_and_rejects_fabricated_delay_or_future_clock() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-retry-fake-plan", false)).await;
    let base = entry.entry().checkpoint();
    let initial = Box::pin(claimed_plan(&store, base, &fence)).await;
    let failed = Box::pin(fail(
        &store,
        &initial,
        initial.nodes()[0].activation().node_id(),
        3_000,
    ))
    .await;
    let real = Box::pin(claimed_plan(&store, base, &fence)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let original = failed.attempt().completion().unwrap();
    let changed = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.fabricated_retry").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("Caller fabricated the retry metadata.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(4_000).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(original.journal_head().event_id());
    let completion = stateknot_core::NodeAttemptCompletion::fail(
        failed.attempt().start(),
        changed,
        BudgetUsage::zero(),
        original.journal_head().clone(),
    )
    .unwrap();
    let fake =
        stateknot_core::NodeAttempt::restore(failed.attempt().start().clone(), Some(completion))
            .unwrap();
    let mut planner =
        stateknot_core::ReadyNodeRecoveryPlanner::for_frame(base.clone(), fence.clone()).unwrap();
    planner.observe_attempt(&fake).unwrap();
    let fake_plan = planner
        .finish(real.journal_head().clone(), real.observed_at())
        .unwrap();
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&fake_plan)).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    let future = Timestamp::from_unix_micros(real.observed_at().unix_micros() + 1_000_000).unwrap();
    let mut planner =
        stateknot_core::ReadyNodeRecoveryPlanner::for_frame(base.clone(), fence.clone()).unwrap();
    planner.observe_attempt(failed.attempt()).unwrap();
    let future_plan = planner.finish(real.journal_head().clone(), future).unwrap();
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&future_plan)).await,
        Err(StoreError::DatabaseClockRegression)
    ));
    assert_eq!(before, snapshot(&db, &fence).await);
    assert!(
        !store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_stale_owner_expiry_and_journal_progress_are_operational() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    for (name, mode) in [
        ("frame-retry-stale-owner", 0),
        ("frame-retry-expired-owner", 1),
        ("frame-retry-stale-journal", 2),
    ] {
        let (_, fence, plan) = Box::pin(deferred(&store, name, 3_000)).await;
        match mode {
            0 => {
                store
                    .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
                    .await
                    .unwrap();
            }
            1 => {
                query("UPDATE stateknot.runs SET lease_acquired_at=clock_timestamp()-interval '2 seconds', lease_renewed_at=clock_timestamp()-interval '1 second', lease_expires_at=clock_timestamp()-interval '1 microsecond' WHERE tenant_id=$1 AND run_id=$2")
                .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
            }
            _ => {
                store
                    .append_worker(
                        append(&plan, EventId::generate(), plan.journal_head().clone()),
                        RunProjection::Unchanged,
                    )
                    .await
                    .unwrap();
            }
        }
        let before = snapshot(&db, &fence).await;
        let outcome = Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await;
        match mode {
            0 => assert!(matches!(outcome, Err(StoreError::StaleFence))),
            1 => assert!(matches!(outcome, Err(StoreError::LeaseExpired))),
            _ => assert!(matches!(outcome, Err(StoreError::StaleJournalHead))),
        }
        assert_eq!(before, snapshot(&db, &fence).await);
        assert!(
            !store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .unwrap()
                .is_quarantined()
        );
    }
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_slow_update_rechecks_due_and_rolls_back_release() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, fence, plan) = Box::pin(deferred(&store, "frame-retry-slow-update", 2_000)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    query("CREATE FUNCTION stateknot.test_retry_slow_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.scheduler_not_before IS NOT NULL AND OLD.scheduler_not_before IS NULL THEN PERFORM pg_sleep(2.1); END IF; RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE TRIGGER test_retry_slow_update BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_retry_slow_update()").execute(&db).await.unwrap();
    let result = Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await;
    query("DROP TRIGGER test_retry_slow_update ON stateknot.runs")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_retry_slow_update()")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(
        result.unwrap(),
        DelayedRetryScheduleOutcome::Due {
            not_before: plan.earliest_deferred_at().unwrap()
        }
    );
    assert_eq!(before, snapshot(&db, &fence).await);
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_slow_update_cannot_release_an_expired_lease() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_options(test_options(Duration::from_secs(2))).await else {
        return;
    };
    let (_, fence, plan) = Box::pin(deferred(&store, "frame-retry-slow-expiry", 3_000)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    query("CREATE FUNCTION stateknot.test_retry_expiring_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.scheduler_not_before IS NOT NULL AND OLD.scheduler_not_before IS NULL THEN PERFORM pg_sleep(2.1); END IF; RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE TRIGGER test_retry_expiring_update BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_retry_expiring_update()").execute(&db).await.unwrap();
    let result = Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await;
    query("DROP TRIGGER test_retry_expiring_update ON stateknot.runs")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_retry_expiring_update()")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        matches!(result, Err(StoreError::LeaseExpired)),
        "{result:?}"
    );
    assert_eq!(before, snapshot(&db, &fence).await);
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_retry_completed_sibling_and_multiple_delays_choose_only_earliest() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, template, _) = super::frame_transactions::graphs();
    let nodes = ["completed", "early", "later"];
    let graph = CompiledGraph::compile(
        template.identity().clone(),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        ReadyNodes::try_new(nodes.map(|node| NodeId::new(node).unwrap())).unwrap(),
        nodes.map(|node| {
            stateknot_core::GraphNode::new(
                NodeId::new(node).unwrap(),
                None,
                stateknot_core::GraphRoutes::default(),
                None,
                true,
            )
            .unwrap()
        }),
        stateknot_core::GraphExecutionLimits::new(template.limits().maximum_supersteps(), 3)
            .unwrap(),
    )
    .unwrap();
    let (_, entry, _, fence) = Box::pin(super::frame_barriers::enter_graph(
        &store,
        "frame-retry-siblings",
        graph.clone(),
        &[],
    ))
    .await;
    let base = entry.entry().checkpoint();
    let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
    let completed = Box::pin(super::frame_recovery::succeed_claimed(
        &store,
        &plan,
        &NodeId::new("completed").unwrap(),
        super::frame_barriers::terminal(&graph),
    ))
    .await;
    for (node, delay) in [("early", 3_000), ("later", 4_000)] {
        let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
        Box::pin(fail(&store, &plan, &NodeId::new(node).unwrap(), delay)).await;
    }
    let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert_eq!(plan.nodes()[0].result(), Some(&completed.head()));
    let early = plan.nodes()[1].not_before().unwrap();
    let later = plan.nodes()[2].not_before().unwrap();
    assert!(early < later);
    assert_eq!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Scheduled { not_before: early }
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let next = loop {
        match store
            .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
            .await
        {
            Ok(next) => break next,
            Err(StoreError::RunNotYetAvailable) => {
                assert!(Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => panic!("unexpected claim {other:?}"),
        }
    };
    let due = Box::pin(claimed_plan(&store, base, next.lease().fence())).await;
    assert_eq!(due.nodes()[0].kind(), RecoveryNodeKind::Completed);
    assert_eq!(
        due.nodes()[1].dispatch_reason(),
        Some(NodeDispatchReason::SafeRetry)
    );
    assert_eq!(due.nodes()[2].kind(), RecoveryNodeKind::Deferred);
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&due)).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    assert_eq!(
        store
            .load_pending_node_result(completed.intent().activation())
            .await
            .unwrap(),
        completed
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_retry_corruption_quarantine_rechecks_exact_live_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    for (component, table, assignment) in [
        (
            "start",
            "node_attempts",
            "start_digest=decode(repeat('11',32),'hex')",
        ),
        (
            "completion",
            "node_attempt_completions",
            "completion_digest=decode(repeat('22',32),'hex')",
        ),
        ("stack", "graph_frame_stacks", "lifetime_starts=2"),
        (
            "input",
            "node_attempts",
            "activation_input_digest=decode(repeat('33',32),'hex')",
        ),
    ] {
        for mode in 0..3 {
            let name = format!("frame-retry-corrupt-{component}-{mode}");
            let (_, fence, plan) = Box::pin(deferred(&store, &name, 3_000)).await;
            let context = CorruptionQuarantineContext::new(
                fence.tenant_id().clone(),
                fence.run_id(),
                QuarantineId::generate(),
                JournalExpectation::exact(plan.journal_head().clone()),
                Digest::sha256(b"retry actual corrupt component"),
            )
            .unwrap()
            .with_expected_fence(fence.clone())
            .unwrap();
            if mode == 1 {
                store
                    .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
                    .await
                    .unwrap();
            }
            if mode == 2 {
                query("UPDATE stateknot.runs SET lease_acquired_at=clock_timestamp()-interval '2 seconds', lease_renewed_at=clock_timestamp()-interval '1 second', lease_expires_at=clock_timestamp()-interval '1 microsecond' WHERE tenant_id=$1 AND run_id=$2")
                    .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
            }
            query(&format!(
                "ALTER TABLE stateknot.{table} DISABLE TRIGGER ALL"
            ))
            .execute(&db)
            .await
            .unwrap();
            // Only the ordinary ready activation is damaged, not the framework caller.
            let predicate = if table == "node_attempts" {
                " AND graph_namespace<>''"
            } else {
                ""
            };
            query(&format!("UPDATE stateknot.{table} SET {assignment} WHERE tenant_id=$1 AND run_id=$2{predicate}"))
                .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
            query(&format!("ALTER TABLE stateknot.{table} ENABLE TRIGGER ALL"))
                .execute(&db)
                .await
                .unwrap();
            let before = snapshot(&db, &fence).await;
            let result = Box::pin(store.with_corruption_quarantine(
                context,
                Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)),
            ))
            .await;
            match mode {
                0 => assert!(
                    matches!(result, Err(StoreError::RunQuarantined)),
                    "{component} {result:?}"
                ),
                1 => assert!(
                    matches!(result, Err(StoreError::StaleFence)),
                    "{component} {result:?}"
                ),
                _ => assert!(
                    matches!(result, Err(StoreError::LeaseExpired)),
                    "{component} {result:?}"
                ),
            }
            let run = store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .unwrap();
            assert_eq!(run.is_quarantined(), mode == 0);
            assert_eq!(run.scheduler_not_before(), None);
            let after = snapshot(&db, &fence).await;
            assert_eq!(before.0, after.0);
            assert_eq!(before.2, after.2);
            if mode != 0 {
                assert_eq!(before, after);
            }
        }
    }
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_retry_ack_observes_gate_without_proving_original_worker_attempt() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (base, fence, plan) = Box::pin(deferred(&store, "frame-retry-ack-limit", 3_000)).await;
    let history = store
        .load_node_attempt_history_page(
            plan.nodes()[0].activation(),
            None,
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    let changed_fence = RunFence::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        AttemptId::generate(),
        fence.epoch(),
    );
    let mut planner =
        stateknot_core::ReadyNodeRecoveryPlanner::for_frame(base, changed_fence.clone()).unwrap();
    for attempt in history.records() {
        let start = stateknot_core::NodeAttemptStart::new(
            attempt.start().activation().clone(),
            attempt.start().attempt_id(),
            changed_fence.clone(),
            attempt.start().journal_head().clone(),
        )
        .unwrap();
        let completion = attempt.completion().unwrap();
        let completion = stateknot_core::NodeAttemptCompletion::fail(
            &start,
            completion.outcome().failure().unwrap().clone(),
            BudgetUsage::zero(),
            completion.journal_head().clone(),
        )
        .unwrap();
        let changed = stateknot_core::NodeAttempt::restore(start, Some(completion)).unwrap();
        planner.observe_attempt(&changed).unwrap();
    }
    let observation = planner
        .finish(plan.journal_head().clone(), plan.observed_at())
        .unwrap();
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    // A fresh write needs the real attempt, even when the epoch matches.
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&observation)).await,
        Err(StoreError::StaleFence)
    ));
    assert_eq!(before, snapshot(&db, &fence).await);
    Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
        .await
        .unwrap();
    let after = snapshot(&db, &fence).await;
    // Lease release erased original worker-attempt evidence. This ACK grants
    // no launch and deliberately makes no claim to prove that erased identity.
    assert_eq!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&observation))
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Idempotent {
            not_before: plan.earliest_deferred_at().unwrap()
        }
    );
    assert_eq!(after, snapshot(&db, &fence).await);
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(
                &observation,
                EventId::generate(),
                observation.journal_head().clone()
            ),
            &observation,
            observation.nodes()[0].activation().node_id(),
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_retry_never_zero_delay_reconcile_and_physical_cap_remain_closed() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    for (name, advice, kind) in [
        (
            "frame-retry-never",
            RetryAdvice::Never,
            RecoveryNodeKind::Failed,
        ),
        (
            "frame-retry-zero",
            RetryAdvice::SafeAfter {
                delay: DurationMillis::ZERO,
            },
            RecoveryNodeKind::Dispatchable,
        ),
        (
            "frame-retry-reconcile",
            RetryAdvice::ReconcileFirst,
            RecoveryNodeKind::InFlight,
        ),
    ] {
        let (_, entry, _, fence) = Box::pin(enter(&store, name, false)).await;
        let base = entry.entry().checkpoint();
        let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
        let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&plan, EventId::generate(), plan.journal_head().clone()),
            &plan,
            plan.nodes()[0].activation().node_id(),
            AttemptId::generate(),
        ))
        .await
        .unwrap();
        let event = EventId::generate();
        let failure = Failure::new(
            FailureId::generate(),
            if matches!(&advice, RetryAdvice::ReconcileFirst) {
                FailureCategory::AmbiguousExternalOutcome
            } else {
                FailureCategory::DependencyUnavailable
            },
            FailureCode::new("node.scoped_policy").unwrap(),
            FailureOrigin::new("graph.integration").unwrap(),
            FailureMessage::new("Ordinary retry policy must retain its closed semantics.").unwrap(),
            advice,
        )
        .unwrap()
        .with_caused_by_event(event);
        let before = snapshot(&db, &fence).await;
        let completion = Box::pin(store.fail_node_attempt(
            append(&plan, event, started.event().head()),
            &started.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        ))
        .await;
        if kind == RecoveryNodeKind::InFlight {
            assert!(
                matches!(completion, Err(StoreError::InvalidNodeAttemptTransition)),
                "{completion:?}"
            );
            assert_eq!(before, snapshot(&db, &fence).await);
        } else {
            completion.unwrap();
        }
        let actual = Box::pin(claimed_plan(&store, base, &fence)).await;
        assert_eq!(actual.nodes()[0].kind(), kind);
        assert!(matches!(
            Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&actual)).await,
            Err(StoreError::InvalidDelayedRetryPlan)
        ));
    }
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-retry-physical-cap", false)).await;
    let base = entry.entry().checkpoint();
    let mut attempts = Vec::new();
    let mut journal = entry.event().head();
    for _ in 0..stateknot_core::ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
        let live = store.observe_live_lease(&fence).await.unwrap();
        let mut planner =
            stateknot_core::ReadyNodeRecoveryPlanner::for_frame(base.clone(), fence.clone())
                .unwrap();
        for attempt in &attempts {
            planner.observe_attempt(attempt).unwrap();
        }
        let plan = planner.finish(journal, live.observed_at()).unwrap();
        let failed = Box::pin(fail(
            &store,
            &plan,
            plan.nodes()[0].activation().node_id(),
            0,
        ))
        .await;
        journal = failed.event().head();
        attempts.push(failed.attempt().clone());
    }
    let exhausted = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert_eq!(exhausted.nodes()[0].kind(), RecoveryNodeKind::Exhausted);
    let before = snapshot(&db, &fence).await;
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&exhausted)).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(
                &exhausted,
                EventId::generate(),
                exhausted.journal_head().clone()
            ),
            &exhausted,
            exhausted.nodes()[0].activation().node_id(),
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    assert_eq!(before, snapshot(&db, &fence).await);
    let count: i64 = query_scalar("SELECT count(*) FROM stateknot.node_attempts WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace<>''")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(count, 64);
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_retry_far_future_gate_cannot_hide_original_deadline_cancellation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-retry-deadline");
    let run = RunId::generate();
    let (parent, child, call) = super::frame_transactions::graphs();
    let original_deadline = timestamp_after(Duration::from_secs(2));
    let (_, limits) = agent_fixture_request_and_budget();
    let admission = Box::pin(super::frame_transactions::admit_with_budget_override(
        &store,
        &tenant,
        run,
        &parent,
        &child,
        None,
        Some(limits.with_deadline(original_deadline)),
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let entry = stateknot_core::GraphFrameEntryPlan::for_root(
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
            entry,
            EventId::generate(),
            admission.event().head(),
            super::frame_transactions::initial_usage(&admission),
            &AcceptGraphSchemas,
        ))
        .await
        .unwrap()
    else {
        panic!("fresh deadline child");
    };
    let base = entry.entry().checkpoint();
    let initial = Box::pin(claimed_plan(&store, base, &fence)).await;
    Box::pin(fail(
        &store,
        &initial,
        initial.nodes()[0].activation().node_id(),
        60_000,
    ))
    .await;
    let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
    Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
        .await
        .unwrap();
    let limit = Instant::now() + Duration::from_secs(5);
    loop {
        let due = store
            .due_agent_deadlines_after(&tenant, None)
            .await
            .unwrap();
        if let Some(candidate) = due.iter().find(|candidate| candidate.run_id() == run) {
            assert_eq!(candidate.deadline(), original_deadline);
            break;
        }
        assert!(
            Instant::now() < limit,
            "a far-future retry gate hid deadline maintenance"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let canceled = Box::pin(store.request_agent_deadline_cancellation(
        &tenant,
        run,
        EventId::generate(),
        FailureId::generate(),
        &checkpoint_schema("retry-deadline-cancellation"),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap();
    assert!(matches!(
        canceled,
        stateknot_store_postgres::AgentDeadlineCancellationOutcome::Requested(_)
    ));
    let after = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(after.lifecycle().status(), RunStatus::CancellationRequested);
    assert_eq!(after.scheduler_not_before(), None);
    assert!(after.scheduler_ready_at().is_some());
    let page = store
        .load_runnable_run_page(
            &tenant,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        page.records()
            .iter()
            .any(|candidate| candidate.run().lifecycle().provenance().run_id() == run)
    );
    assert_eq!(after.checkpoint(), admission.run().checkpoint());
    assert_eq!(
        store
            .load_agent_admission(&tenant, run)
            .await
            .unwrap()
            .admission()
            .intent()
            .budget()
            .deadline(),
        original_deadline
    );
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await,
        Err(StoreError::RunNotRunnable)
    ));
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_retry_seven_level_noninitial_leaf_preserves_ancestry_and_root() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-retry-seven-noninitial");
    let run = RunId::generate();
    let (_, leaf, _) = super::frame_transactions::graphs();
    let leaf = CompiledGraph::compile(
        leaf.identity().clone(),
        leaf.input_schema().clone(),
        leaf.state_schema().clone(),
        leaf.update_schema().clone(),
        leaf.output_schema().clone(),
        leaf.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("first").unwrap()]).unwrap(),
        [
            stateknot_core::GraphNode::new(
                NodeId::new("first").unwrap(),
                Some(ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap()),
                stateknot_core::GraphRoutes::default(),
                None,
                false,
            )
            .unwrap(),
            stateknot_core::GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                stateknot_core::GraphRoutes::default(),
                None,
                true,
            )
            .unwrap(),
        ],
        leaf.limits(),
    )
    .unwrap();
    let mut definitions = vec![leaf];
    for level in (0..7).rev() {
        definitions.push(super::frame_transactions::caller_graph(
            &format!("retry-level-{level}"),
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
        &store,
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
    let mut journal = admission.event().head();
    let mut usage = super::frame_transactions::initial_usage(&admission);
    let mut previous: Option<stateknot_store_postgres::StoredGraphFrameEntry> = None;
    for level in 0..7 {
        let parent = &definitions[level];
        let child = &definitions[level + 1];
        let call = &parent.frame_calls().unwrap().calls()[0];
        let plan = if let Some(previous) = &previous {
            stateknot_core::GraphFrameEntryPlan::for_frame(
                call,
                parent,
                previous.entry().checkpoint(),
                child,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        } else {
            stateknot_core::GraphFrameEntryPlan::for_root(
                call,
                parent,
                admission.checkpoint(),
                child,
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
                journal,
                usage,
                &AcceptGraphSchemas,
            ))
            .await
            .unwrap()
        else {
            panic!("fresh level");
        };
        journal = entry.event().head();
        usage = entry.direct_usage_after().unwrap();
        previous = Some(entry);
    }
    let leaf = previous.unwrap();
    let graph = definitions.last().unwrap();
    let initial = Box::pin(claimed_plan(&store, leaf.entry().checkpoint(), &fence)).await;
    let first = Box::pin(super::frame_recovery::succeed_claimed(
        &store,
        &initial,
        &NodeId::new("first").unwrap(),
        NodeControl::Continue,
    ))
    .await;
    let prepared = super::frame_barriers::plan(graph, leaf.entry().checkpoint(), &first);
    let successor = Box::pin(super::frame_barriers::commit(
        &store, prepared, &fence, usage, graph,
    ))
    .await;
    assert_eq!(successor.checkpoint().checkpoint().superstep().get(), 1);
    let plan = Box::pin(claimed_plan(&store, successor.checkpoint(), &fence)).await;
    Box::pin(fail(&store, &plan, &NodeId::new("finish").unwrap(), 3_000)).await;
    let plan = Box::pin(claimed_plan(&store, successor.checkpoint(), &fence)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let outcome = Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan))
        .await
        .unwrap();
    let not_before = plan.earliest_deferred_at().unwrap();
    match outcome {
        DelayedRetryScheduleOutcome::Scheduled { not_before: actual } => {
            assert_eq!(actual, not_before);
            let sleeping = store.load_run(&tenant, run).await.unwrap();
            assert!(sleeping.lease().is_none());
            assert_eq!(sleeping.scheduler_not_before(), Some(not_before));
        }
        DelayedRetryScheduleOutcome::Due { not_before: actual } => {
            assert_eq!(actual, not_before);
            let live = store.observe_live_lease(&fence).await.unwrap();
            assert!(live.observed_at() >= not_before);
            assert_eq!(snapshot(&db, &fence).await, before);
            let owned = store.load_run(&tenant, run).await.unwrap();
            assert_eq!(owned.lease().unwrap().fence(), &fence);
            assert_eq!(owned.scheduler_not_before(), None);
        }
        other => {
            panic!("a first deep retry schedule cannot acknowledge an earlier gate: {other:?}")
        }
    }
    let after = snapshot(&db, &fence).await;
    assert_eq!(before.0, after.0);
    assert_eq!(before.2, after.2);
    let active = Box::pin(store.load_active_graph_frame(&tenant, run))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.open_frames().len(), 7);
    assert_eq!(active.checkpoint(), successor.checkpoint());
    assert_eq!(active.run().checkpoint(), admission.run().checkpoint());
    assert_eq!(active.run().journal_head(), Some(plan.journal_head()));
    db.close().await;
    store.close().await;
}

async fn root_deferred(store: &PostgresStore, name: &str) -> ReadyNodeRecoveryPlan {
    Box::pin(root_deferred_with_delay(store, name, 2_000)).await
}

async fn root_deferred_with_delay(
    store: &PostgresStore,
    name: &str,
    delay: i64,
) -> ReadyNodeRecoveryPlan {
    let tenant = tenant(name);
    let run = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(store, &tenant, run, 24_100)).await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let started = store
        .start_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                fence.clone(),
                24_101,
            ),
            pending_activation(
                checkpoint.checkpoint(),
                b"Root retry must retain final clock semantics",
            ),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.root_retry_clock").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The original Root scheduler shares final time validation.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(delay).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(event);
    let failed = store
        .fail_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                event,
                JournalExpectation::exact(started.event().head()),
                fence.clone(),
                24_102,
            ),
            &started.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant,
        run,
        QuarantineId::generate(),
        JournalExpectation::exact(failed.event().head()),
        Digest::sha256(b"Root final retry clock"),
    )
    .unwrap();
    store
        .begin_claimed_run_recovery(fence, context)
        .await
        .unwrap()
        .plan_ready_nodes()
        .await
        .unwrap()
}

#[tokio::test]
async fn root_and_scoped_retry_share_final_trigger_clock_without_crossed_authority() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let plan = Box::pin(root_deferred(&store, "root-retry-shared-final-clock")).await;
    assert!(matches!(
        Box::pin(store.schedule_graph_frame_delayed_retry_wakeup(&plan)).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));
    let db = pool().await;
    let fence = plan.fence();
    let before: String = query_scalar(
        "SELECT to_jsonb(r)::text FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    query("CREATE FUNCTION stateknot.test_root_retry_slow_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.scheduler_not_before IS NOT NULL AND OLD.scheduler_not_before IS NULL THEN PERFORM pg_sleep(2.1); END IF; RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE TRIGGER test_root_retry_slow_update BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_root_retry_slow_update()").execute(&db).await.unwrap();
    let result = store.schedule_delayed_retry_wakeup(&plan).await;
    query("DROP TRIGGER test_root_retry_slow_update ON stateknot.runs")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_root_retry_slow_update()")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(
        result.unwrap(),
        DelayedRetryScheduleOutcome::Due {
            not_before: plan.earliest_deferred_at().unwrap()
        }
    );
    let after: String = query_scalar(
        "SELECT to_jsonb(r)::text FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(before, after);
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_retry_rejects_fabricated_safe_after_from_public_planner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let real = Box::pin(root_deferred(&store, "root-retry-fabricated-safe-after")).await;
    let history = store
        .load_node_attempt_history_page(
            real.nodes()[0].activation(),
            None,
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    let attempt = &history.records()[0];
    let head = attempt.completion().unwrap().journal_head();
    let forged_failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.forged_root_delay").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("A public planner cannot replace the actual retry delay.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(3_600_000).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(head.event_id());
    let completion = stateknot_core::NodeAttemptCompletion::fail(
        attempt.start(),
        forged_failure,
        BudgetUsage::zero(),
        head.clone(),
    )
    .unwrap();
    let forged_attempt =
        stateknot_core::NodeAttempt::restore(attempt.start().clone(), Some(completion)).unwrap();
    let mut planner =
        ReadyNodeRecoveryPlanner::new(real.checkpoint().clone(), real.fence().clone()).unwrap();
    planner.observe_attempt(&forged_attempt).unwrap();
    let forged = planner
        .finish(real.journal_head().clone(), real.observed_at())
        .unwrap();
    let db = pool().await;
    let before: String = query_scalar(
        "SELECT to_jsonb(r)::text FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(real.fence().tenant_id().as_str())
    .bind(*real.fence().run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    let outcome = store.schedule_delayed_retry_wakeup(&forged).await;
    assert!(
        matches!(outcome, Err(StoreError::InvalidDelayedRetryPlan)),
        "{outcome:?}"
    );
    let after: String = query_scalar(
        "SELECT to_jsonb(r)::text FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(real.fence().tenant_id().as_str())
    .bind(*real.fence().run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(before, after);
    assert!(
        !store
            .load_run(real.fence().tenant_id(), real.fence().run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    db.close().await;
    store.close().await;
}

async fn root_snapshot(db: &PgPool, fence: &RunFence) -> (String, Vec<i64>) {
    let run = query_scalar(
        "SELECT to_jsonb(r)::text FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(db)
    .await
    .unwrap();
    let mut counts = Vec::new();
    for table in [
        "run_events",
        "run_checkpoints",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "run_attempt_claims",
    ] {
        counts.push(
            query_scalar(&format!(
                "SELECT count(*) FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2"
            ))
            .bind(fence.tenant_id().as_str())
            .bind(*fence.run_id().as_uuid())
            .fetch_one(db)
            .await
            .unwrap(),
        );
    }
    (run, counts)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_retry_rejects_fabricated_completed_sibling_and_reconciliation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("root-retry-fabricated-completed");
    let run = RunId::generate();
    let ready = ReadyNodes::try_new([
        NodeId::new("node-0001").unwrap(),
        NodeId::new("node-0002").unwrap(),
    ])
    .unwrap();
    let template = checkpoint_compiled_graph();
    let graph = CompiledGraph::compile(
        template.identity().clone(),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        ready.clone(),
        ready
            .iter()
            .cloned()
            .map(|node| GraphNode::new(node, None, GraphRoutes::empty(), None, true).unwrap()),
        GraphExecutionLimits::new(template.limits().maximum_supersteps(), 2).unwrap(),
    )
    .unwrap();
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let admitted = store
        .admit_run(provenance(tenant.clone(), run))
        .await
        .unwrap();
    let write = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        graph.reference(),
        checkpoint_state(&graph.reference(), 0),
        ready,
    )
    .unwrap();
    let checkpoint = store
        .append_control_plane_checkpoint(
            control_append(
                tenant.clone(),
                run,
                EventId::generate(),
                JournalExpectation::empty(),
                24_110,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            write,
        )
        .await
        .unwrap();
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let first = store
        .start_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                fence.clone(),
                24_111,
            ),
            NodeActivation::for_ready_root(
                checkpoint.checkpoint(),
                NodeId::new("node-0001").unwrap(),
            )
            .unwrap(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let second = store
        .start_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                EventId::generate(),
                JournalExpectation::exact(first.event().head()),
                fence.clone(),
                24_112,
            ),
            NodeActivation::for_ready_root(
                checkpoint.checkpoint(),
                NodeId::new("node-0002").unwrap(),
            )
            .unwrap(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.root_delayed_sibling").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("A delayed sibling cannot hide work that is still in flight.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(3_000).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(event);
    let failed = store
        .fail_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                event,
                JournalExpectation::exact(second.event().head()),
                fence.clone(),
                24_113,
            ),
            &second.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let live = store.observe_live_lease(&fence).await.unwrap();
    let result = stateknot_core::PendingNodeResult::commit(
        pending_result_intent(
            first.attempt().start().activation().clone(),
            NodeInvocationBindings::empty(),
        ),
        fence.clone(),
        failed.event().head(),
    )
    .unwrap();
    let completion = stateknot_core::NodeAttemptCompletion::succeed(
        first.attempt().start(),
        result.head(),
        BudgetUsage::zero(),
    )
    .unwrap();
    let forged_attempt =
        stateknot_core::NodeAttempt::restore(first.attempt().start().clone(), Some(completion))
            .unwrap();
    let mut planner =
        ReadyNodeRecoveryPlanner::new(checkpoint.checkpoint().clone(), fence.clone()).unwrap();
    planner.observe_result(&result).unwrap();
    planner.observe_attempt(&forged_attempt).unwrap();
    planner.observe_attempt(failed.attempt()).unwrap();
    let forged = planner
        .finish(failed.event().head(), live.observed_at())
        .unwrap();
    assert_eq!(forged.nodes()[0].kind(), RecoveryNodeKind::Completed);
    assert_eq!(forged.nodes()[1].kind(), RecoveryNodeKind::Deferred);
    let db = pool().await;
    let before = root_snapshot(&db, &fence).await;
    let context = CorruptionQuarantineContext::new(
        tenant.clone(),
        run,
        QuarantineId::generate(),
        JournalExpectation::exact(failed.event().head()),
        Digest::sha256(b"root fabricated completed decision"),
    )
    .unwrap()
    .with_expected_fence(fence.clone())
    .unwrap();
    let outcome = store
        .with_corruption_quarantine(context, store.schedule_delayed_retry_wakeup(&forged))
        .await;
    assert!(
        matches!(outcome, Err(StoreError::InvalidDelayedRetryPlan)),
        "{outcome:?}"
    );
    assert_eq!(before, root_snapshot(&db, &fence).await);
    assert!(!store.load_run(&tenant, run).await.unwrap().is_quarantined());
    let reconciliation = Failure::new(
        FailureId::generate(),
        FailureCategory::AmbiguousExternalOutcome,
        FailureCode::new("node.forged_reconciliation").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("A pure node completion cannot require external reconciliation.")
            .unwrap(),
        RetryAdvice::ReconcileFirst,
    )
    .unwrap()
    .with_caused_by_event(failed.event().head().event_id());
    assert!(matches!(
        stateknot_core::NodeAttemptCompletion::fail(
            first.attempt().start(),
            reconciliation.clone(),
            BudgetUsage::zero(),
            failed.event().head()
        ),
        Err(stateknot_core::NodeAttemptError::ReconciliationUnsupported)
    ));
    let event = EventId::generate();
    let outcome = store
        .fail_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                event,
                JournalExpectation::exact(failed.event().head()),
                fence.clone(),
                24_114,
            ),
            &first.attempt().start().head(),
            reconciliation.with_caused_by_event(event),
            BudgetUsage::zero(),
        )
        .await;
    assert!(
        matches!(outcome, Err(StoreError::InvalidNodeAttemptTransition)),
        "{outcome:?}"
    );
    assert_eq!(before, root_snapshot(&db, &fence).await);
    assert!(!store.load_run(&tenant, run).await.unwrap().is_quarantined());
    let event = EventId::generate();
    let completed = store
        .succeed_node_attempt(
            worker_append(
                tenant.clone(),
                run,
                event,
                JournalExpectation::exact(failed.event().head()),
                fence.clone(),
                24_115,
            ),
            &first.attempt().start().head(),
            pending_result_intent(
                first.attempt().start().activation().clone(),
                NodeInvocationBindings::empty(),
            ),
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant.clone(),
        run,
        QuarantineId::generate(),
        JournalExpectation::exact(completed.event().head()),
        Digest::sha256(b"actual Root completed sibling"),
    )
    .unwrap();
    let actual = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap()
        .plan_ready_nodes()
        .await
        .unwrap();
    assert_eq!(actual.nodes()[0].kind(), RecoveryNodeKind::Completed);
    assert_eq!(actual.nodes()[1].kind(), RecoveryNodeKind::Deferred);
    let durable = store
        .load_pending_node_result(first.attempt().start().activation())
        .await
        .unwrap();
    assert_eq!(actual.nodes()[0].result(), Some(&durable.head()));
    let before = root_snapshot(&db, &fence).await;
    assert_eq!(
        store.schedule_delayed_retry_wakeup(&actual).await.unwrap(),
        DelayedRetryScheduleOutcome::Scheduled {
            not_before: actual.earliest_deferred_at().unwrap(),
        }
    );
    assert_eq!(before.1, root_snapshot(&db, &fence).await.1);
    assert_eq!(
        store
            .load_pending_node_result(first.attempt().start().activation())
            .await
            .unwrap(),
        durable
    );
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_retry_ack_is_exact_read_only_and_cannot_prove_erased_worker_or_launch() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let plan = Box::pin(root_deferred(&store, "root-retry-ack-limit")).await;
    let fence = plan.fence();
    let history = store
        .load_node_attempt_history_page(
            plan.nodes()[0].activation(),
            None,
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    let original = &history.records()[0];
    let other_fence = RunFence::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        AttemptId::generate(),
        fence.epoch(),
    );
    let start = stateknot_core::NodeAttemptStart::new(
        original.start().activation().clone(),
        original.start().attempt_id(),
        other_fence.clone(),
        original.start().journal_head().clone(),
    )
    .unwrap();
    let original_completion = original.completion().unwrap();
    let completion = stateknot_core::NodeAttemptCompletion::fail(
        &start,
        original_completion.outcome().failure().unwrap().clone(),
        BudgetUsage::zero(),
        original_completion.journal_head().clone(),
    )
    .unwrap();
    let forged_attempt = stateknot_core::NodeAttempt::restore(start, Some(completion)).unwrap();
    let mut planner =
        ReadyNodeRecoveryPlanner::new(plan.checkpoint().clone(), other_fence).unwrap();
    planner.observe_attempt(&forged_attempt).unwrap();
    let observation = planner
        .finish(plan.journal_head().clone(), plan.observed_at())
        .unwrap();
    let db = pool().await;
    let before = root_snapshot(&db, fence).await;
    let outcome = store.schedule_delayed_retry_wakeup(&observation).await;
    assert!(
        matches!(outcome, Err(StoreError::StaleFence)),
        "{outcome:?}"
    );
    assert_eq!(before, root_snapshot(&db, fence).await);
    assert_eq!(
        store.schedule_delayed_retry_wakeup(&plan).await.unwrap(),
        DelayedRetryScheduleOutcome::Scheduled {
            not_before: plan.earliest_deferred_at().unwrap(),
        }
    );
    let released = root_snapshot(&db, fence).await;
    assert_eq!(
        store
            .schedule_delayed_retry_wakeup(&observation)
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Idempotent {
            not_before: plan.earliest_deferred_at().unwrap(),
        }
    );
    assert_eq!(released, root_snapshot(&db, fence).await);
    assert!(matches!(
        store
            .start_recovered_node_attempt(
                append(
                    &observation,
                    EventId::generate(),
                    observation.journal_head().clone()
                ),
                &observation,
                observation.nodes()[0].activation().node_id(),
                AttemptId::generate()
            )
            .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    assert_eq!(released, root_snapshot(&db, fence).await);
    let changed_head = JournalHead::new(
        plan.journal_head().tenant_id().clone(),
        plan.journal_head().run_id(),
        plan.journal_head().sequence(),
        EventId::generate(),
        plan.journal_head().recorded_at(),
        plan.journal_head().digest(),
    );
    let mut planner =
        ReadyNodeRecoveryPlanner::new(plan.checkpoint().clone(), plan.fence().clone()).unwrap();
    planner.observe_attempt(original).unwrap();
    let changed = planner.finish(changed_head, plan.observed_at()).unwrap();
    assert!(matches!(
        store.schedule_delayed_retry_wakeup(&changed).await,
        Err(StoreError::StaleJournalHead)
    ));
    assert_eq!(released, root_snapshot(&db, fence).await);
    assert!(
        !store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_retry_slow_update_after_expiry_preserves_original_projection_and_history() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let plan = Box::pin(root_deferred_with_delay(
        &store,
        "root-retry-expired-write",
        3_000,
    ))
    .await;
    let db = pool().await;
    let before = root_snapshot(&db, plan.fence()).await;
    query("CREATE FUNCTION stateknot.test_root_retry_expired_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.scheduler_not_before IS NOT NULL AND OLD.scheduler_not_before IS NULL THEN PERFORM pg_sleep(2.1); END IF; RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE TRIGGER test_root_retry_expired_update BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_root_retry_expired_update()").execute(&db).await.unwrap();
    let outcome = store.schedule_delayed_retry_wakeup(&plan).await;
    query("DROP TRIGGER test_root_retry_expired_update ON stateknot.runs")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_root_retry_expired_update()")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        matches!(outcome, Err(StoreError::LeaseExpired)),
        "{outcome:?}"
    );
    assert_eq!(before, root_snapshot(&db, plan.fence()).await);
    assert!(
        !store
            .load_run(plan.fence().tenant_id(), plan.fence().run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}
