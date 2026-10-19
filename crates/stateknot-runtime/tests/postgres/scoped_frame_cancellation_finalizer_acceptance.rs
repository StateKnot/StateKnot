// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public cold finalizer concurrency, raw sweep progress and owning v1/v2 boundaries.
use super::*;
use futures_util::FutureExt;
use stateknot_core::{CancellationObserver, RunLifecycle};
use stateknot_runtime::{
    DurableFrameCancellationFinalizer, FrameCancellationOutcome,
    register_standard_agent_cancellation_event_schema,
};
use stateknot_store_postgres::{
    AppendOutcome, GraphFrameClosureCursor, RunFailureCloseOutcome, RunQuarantineCause,
    RunQuarantineCommitOutcome, RunQuarantineComponent, RunQuarantineRequest, RunnableRunPageSize,
    StoredGraphFrameEntry,
};

fn finalizer(store: &PostgresStore) -> DurableFrameCancellationFinalizer {
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
    DurableFrameCancellationFinalizer::new(
        store.clone(),
        schemas.build().unwrap(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}

async fn independent_store(store: &PostgresStore) -> PostgresStore {
    PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap()
}

async fn acceptance_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}

fn ordered_ids(ordinal: u64) -> AgentRunIds {
    // Caller-retained, valid RFC UUIDv7 identities are accepted by the public
    // admission API. Only the Run key is controlled; its timestamp is no clock.
    let bits =
        (1_700_000_000_000_u128 << 80) | (7_u128 << 76) | (2_u128 << 62) | u128::from(ordinal);
    AgentRunIds::new(
        RunId::from_uuid(uuid::Uuid::from_u128(bits)).unwrap(),
        ThreadId::generate(),
        InvocationId::generate(),
        EventId::generate(),
        CheckpointId::generate(),
    )
}

async fn prepare_ordered(store: &PostgresStore, tenant: &TenantId, ordinal: u64) -> Scenario {
    let ids = ordered_ids(ordinal);
    let scenario = Box::pin(prepare_with_identity(store, graphs(), tenant.clone(), ids)).await;
    assert_eq!(scenario.key.parent_run_id(), ids.run_id());
    assert_eq!(scenario.key.tenant_id(), tenant);
    scenario
}

async fn close_prepared(store: &PostgresStore, scenario: &Scenario) -> StoredGraphFrameClosure {
    let result = Box::pin(store.close_graph_frames(
        scenario.key.tenant_id(),
        scenario.key.parent_run_id(),
        scenario.event_id,
        scenario.observed.clone(),
        scenario.revision,
        scenario.direct.clone(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        result,
        GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let closure = Box::pin(owning_close(store, scenario)).await;
    assert_eq!(fingerprint(&closure), fingerprint(result.record()));
    assert_eq!(closure.root_checkpoint(), &scenario.root_checkpoint.head());
    assert_eq!(
        serde_json::to_value(closure.original_cancellation_lifecycle().unwrap()).unwrap(),
        scenario.lifecycle
    );
    closure
}

fn cancelled_event(outcome: &FrameCancellationOutcome) -> &JournalEvent {
    let FrameCancellationOutcome::Cancelled(append) = outcome else {
        panic!("expected actual terminal append outcome: {outcome:?}");
    };
    append.event()
}

fn committed(outcome: &FrameCancellationOutcome) -> bool {
    matches!(outcome, FrameCancellationOutcome::Cancelled(append) if matches!(append.as_ref(), AppendOutcome::Committed(_)))
}

fn idempotent(outcome: &FrameCancellationOutcome) -> bool {
    matches!(outcome, FrameCancellationOutcome::Cancelled(append) if matches!(append.as_ref(), AppendOutcome::Idempotent(_)))
}

fn rows(snapshot: &Snapshot, table: &str) -> Vec<Value> {
    serde_json::from_str(snapshot.get(table).unwrap()).unwrap()
}

fn only_terminal_parent_changed(before: &Snapshot, after: &Snapshot, run: RunId) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (table, old) in before {
        if !["runs", "run_events", "child_run_budget_accounts"].contains(&table.as_str()) {
            assert_eq!(
                after.get(table).unwrap(),
                old,
                "unexpected tenant writes: {table}"
            );
        }
    }
    let id = run.to_string();
    for (table, column) in [
        ("runs", "run_id"),
        ("run_events", "run_id"),
        ("child_run_budget_accounts", "parent_run_id"),
    ] {
        let unchanged = |snapshot: &Snapshot| {
            rows(snapshot, table)
                .into_iter()
                .filter(|row| row[column].as_str() != Some(id.as_str()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            unchanged(after),
            unchanged(before),
            "other Run changed: {table}"
        );
    }
    let old = rows(before, "run_events");
    let new = rows(after, "run_events");
    assert_eq!(new.len(), old.len() + 1);
    assert!(old.iter().all(|event| new.contains(event)));
    let added: Vec<_> = new.iter().filter(|event| !old.contains(event)).collect();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0]["run_id"].as_str(), Some(id.as_str()));
}

#[allow(clippy::too_many_lines)]
async fn verify_terminal(
    store: &PostgresStore,
    scenario: &Scenario,
    closure: &StoredGraphFrameClosure,
    event: &JournalEvent,
) {
    let key = &scenario.key;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let total = scenario
        .direct
        .checked_accumulate(&scenario.child_usage.cumulative_only())
        .unwrap();
    let original: RunLifecycle = serde_json::from_value(scenario.lifecycle.clone()).unwrap();
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
    assert_eq!(run.lifecycle().terminal_usage(), Some(&total));
    assert_eq!(
        run.lifecycle().revision().get(),
        scenario.revision.get() + 1
    );
    assert_eq!(run.journal_head(), Some(&event.head()));
    assert_eq!(run.last_fencing_epoch(), Some(scenario.fence.epoch()));
    assert!(run.lease().is_none());
    assert!(!run.is_quarantined());
    assert!(run.scheduler_ready_at().is_none());
    assert!(run.scheduler_not_before().is_none());
    assert!(run.lifecycle().changed_at() >= closure.event().recorded_at());
    assert!(
        run.lifecycle().changed_at() < event.recorded_at(),
        "completion clock must differ from event persistence time"
    );
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
    let graph = scenario
        .root_checkpoint
        .graph()
        .definition_digest()
        .to_string();
    assert_eq!(
        event.payload().data().as_value(),
        &json!({
            "operation":"agent_cancellation_confirmed",
            "graph_digest":graph.strip_prefix("sha256:").unwrap(),
            "checkpoint_id":scenario.root_checkpoint.checkpoint_id().to_string(),
            "superstep":scenario.root_checkpoint.superstep().get().to_string(),
            "failure_id":closure.failure().id().to_string(),
        })
    );
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
    assert_eq!(page.events(), std::slice::from_ref(event));
    let saved = Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fingerprint(&saved), fingerprint(closure));
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        scenario.root_checkpoint
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
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    let join = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(join.request(), &scenario.join_request);
    assert_eq!(join.registration(), &scenario.join_registration);
    assert!(join.head().is_none() && join.binding().is_none() && join.consumed().is_none());
    assert_eq!(
        Box::pin(store.include_child_usage(
            key.tenant_id(),
            key.parent_run_id(),
            scenario.direct.clone()
        ))
        .await
        .unwrap(),
        total
    );
}

async fn quarantine_actual_run(store: &PostgresStore, tenant: &TenantId, run: RunId) {
    let before = store.load_run(tenant, run).await.unwrap();
    let request = RunQuarantineRequest::new(
        tenant.clone(),
        run,
        QuarantineId::generate(),
        JournalExpectation::exact(before.journal_head().unwrap().clone()),
        RunQuarantineCause::OperatorPolicy,
        RunQuarantineComponent::new("frame-finalizer-acceptance").unwrap(),
        Digest::sha256(b"actual public operator quarantine for finalizer acceptance"),
    )
    .unwrap();
    assert!(matches!(
        store.quarantine_run(request.clone()).await.unwrap(),
        RunQuarantineCommitOutcome::Committed(_)
    ));
    assert_eq!(
        store
            .load_run_quarantine(tenant, run)
            .await
            .unwrap()
            .request(),
        &request
    );
    let after = store.load_run(tenant, run).await.unwrap();
    assert!(after.is_quarantined());
    assert_eq!(
        serde_json::to_value(after.lifecycle()).unwrap(),
        serde_json::to_value(before.lifecycle()).unwrap()
    );
    assert_eq!(after.journal_head(), before.journal_head());
}

// Exact existing append lock statement, used only to identify the controlled
// waiting seam in pg_stat_activity. Whole-closure owning reads do not FOR UPDATE.
const APPEND_PARENT_LOCK_QUERY: &str = r"
SELECT
    tenant_id,
    run_id,
    thread_id,
    invocation_id,
    lifecycle_bytes,
    lifecycle_revision::text AS lifecycle_revision,
    lifecycle_status,
    admitted_at,
    changed_at,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    checkpoint_id,
    checkpoint_superstep,
    checkpoint_digest,
    fencing_epoch,
    lease_attempt_id,
    lease_acquired_at,
    lease_renewed_at,
    lease_expires_at,
    scheduler_ready_at,
    scheduler_not_before,
    wait_set_digest,
    unresolved_wait_count,
    next_timer_due_at,
    next_interrupt_expiry_at,
    quarantined_at
FROM stateknot.runs
WHERE tenant_id = $1 AND run_id = $2
FOR UPDATE
";
type AppendWait = (i32, String, String, String, Vec<i32>);

async fn controlled_store(store: &PostgresStore, observer: &PgPool) -> (PostgresStore, i32) {
    // Public Store configuration deliberately pins application_name=stateknot.
    // Identify the one real new session; never override production configuration.
    let before = query_scalar::<_, i32>("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name='stateknot' ORDER BY pid")
        .fetch_all(observer).await.unwrap();
    let selected = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    let after = query_scalar::<_, i32>("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name='stateknot' ORDER BY pid")
        .fetch_all(observer).await.unwrap();
    let added: Vec<_> = after
        .iter()
        .copied()
        .filter(|pid| !before.contains(pid))
        .collect();
    if added.len() != 1 {
        selected.close().await;
        panic!(
            "one public single-connection Store must add exactly one actual backend: before={before:?} after={after:?} added={added:?}"
        );
    }
    let pid = added[0];
    eprintln!(
        "STATEKNOT_FRAME_CANCELLATION_BACKEND_DISCOVERY_EVIDENCE={}",
        json!({
            "profile":"frame-cancellation-backend-discovery-v1",
            "application_name":"stateknot","before_pids":before,"after_pids":after,
            "added_pids":added,"selected_pid":pid,"pool_min":1,"pool_max":1,
        })
    );
    (selected, pid)
}

fn waits_for_holder(waiter: &AppendWait, all: &[AppendWait], holder: i32) -> bool {
    waiter.4.contains(&holder)
        || waiter.4.iter().any(|blocker| {
            all.iter()
                .any(|other| other.0 == *blocker && other.4.contains(&holder))
        })
}

#[allow(clippy::too_many_lines)]
async fn controlled_stale_candidates_recover_winner(store: &PostgresStore, pool: &PgPool) {
    let scenario = Box::pin(prepare(store)).await;
    let closure = Box::pin(close_prepared(store, &scenario)).await;
    let tenant = scenario.key.tenant_id();
    let run = scenario.key.parent_run_id();
    let wire = v2_bytes(pool, &scenario).await;
    let account = Box::pin(store.load_child_budget_account(tenant, run))
        .await
        .unwrap()
        .unwrap();
    let before = snapshot(pool, tenant).await;
    let (left_store, left_pid) = controlled_store(store, pool).await;
    let (right_store, right_pid) = controlled_store(store, pool).await;
    assert_ne!(left_pid, right_pid);
    // This external transaction only locks the actual parent row. It never
    // inserts/updates a Run, event, closure, account or any other fixture fact.
    let mut holder = Some(pool.begin().await.unwrap());
    let holder_pid: i32 = query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **holder.as_mut().unwrap())
        .await
        .unwrap();
    assert_ne!(holder_pid, left_pid);
    assert_ne!(holder_pid, right_pid);
    let locked: uuid::Uuid = query_scalar(
        "SELECT run_id FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .fetch_one(&mut **holder.as_mut().unwrap())
    .await
    .unwrap();
    assert_eq!(&locked, run.as_uuid());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let left = finalizer(&left_store);
    let right = finalizer(&right_store);
    let left_barrier = barrier.clone();
    let right_barrier = barrier.clone();
    let left_tenant = tenant.clone();
    let right_tenant = tenant.clone();
    let mut left_task = tokio::spawn(async move {
        left_barrier.wait().await;
        left.resume(&left_tenant, run).await
    });
    let mut right_task = tokio::spawn(async move {
        right_barrier.wait().await;
        right.resume(&right_tenant, run).await
    });
    let mut left_joined = false;
    let mut right_joined = false;
    let observed = std::panic::AssertUnwindSafe(async {
        let waiters = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let rows = sqlx_core::query_as::query_as::<_, AppendWait>(
                    "SELECT pid,COALESCE(state,''),COALESCE(wait_event_type,''),query,pg_blocking_pids(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid=ANY($1) ORDER BY pid"
                ).bind(vec![left_pid, right_pid]).fetch_all(pool).await.unwrap();
                if rows.len() == 2 && rows.iter().all(|row| {
                    row.1 == "active" && row.2 == "Lock"
                        && row.3.trim() == APPEND_PARENT_LOCK_QUERY.trim()
                        && waits_for_holder(row, &rows, holder_pid)
                }) {
                    break rows;
                }
                assert!(!left_task.is_finished() && !right_task.is_finished(),
                    "a finalizer ended before both fresh append candidates reached the held parent lock: {rows:?}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("both real append candidates must block at the identified parent row lock");
        // Both resumes have completed their owning preparation but neither
        // append can pass its first parent lock or create a terminal event yet.
        assert_eq!(snapshot(pool, tenant).await, before);
        Box::pin(owning_close(store, &scenario)).await;
        holder.take().unwrap().commit().await.unwrap();
        let left_result = tokio::time::timeout(Duration::from_secs(15), &mut left_task)
            .await.expect("left controlled finalizer did not finish after lock release");
        left_joined = true;
        let left_result = left_result.unwrap().unwrap();
        let right_result = tokio::time::timeout(Duration::from_secs(15), &mut right_task)
            .await.expect("right controlled finalizer did not finish after lock release");
        right_joined = true;
        (left_result, right_result.unwrap().unwrap(), waiters)
    }).catch_unwind().await;
    if !left_joined {
        left_task.abort();
        let _ = (&mut left_task).await;
    }
    if !right_joined {
        right_task.abort();
        let _ = (&mut right_task).await;
    }
    let holder_cleanup = if let Some(transaction) = holder.take() {
        transaction.rollback().await
    } else {
        Ok(())
    };
    left_store.close().await;
    right_store.close().await;
    let (left_result, right_result, waiters) = match (observed, holder_cleanup) {
        (Ok(value), Ok(())) => value,
        (Err(panic), cleanup) => {
            if let Err(error) = cleanup {
                eprintln!("controlled CAS holder cleanup also failed: {error}");
            }
            pool.close().await;
            store.close().await;
            std::panic::resume_unwind(panic);
        }
        (Ok(_), Err(error)) => {
            pool.close().await;
            store.close().await;
            panic!("controlled CAS holder rollback failed: {error}");
        }
    };
    assert_eq!(
        u8::from(committed(&left_result)) + u8::from(committed(&right_result)),
        1
    );
    assert_eq!(
        u8::from(idempotent(&left_result)) + u8::from(idempotent(&right_result)),
        1
    );
    let event = cancelled_event(&left_result);
    assert_eq!(cancelled_event(&right_result), event);
    Box::pin(verify_terminal(store, &scenario, &closure, event)).await;
    assert_eq!(
        Box::pin(store.load_child_budget_account(tenant, run))
            .await
            .unwrap()
            .unwrap(),
        account
            .observe_direct(event.head(), scenario.direct.clone())
            .unwrap()
    );
    only_terminal_parent_changed(&before, &snapshot(pool, tenant).await, run);
    assert_eq!(v2_bytes(pool, &scenario).await, wire);
    let actual_waiters: Vec<_> = waiters
        .iter()
        .map(|row| {
            json!({
                "pid":row.0,"state":row.1,"wait_event_type":row.2,
                "query":row.3,"blocking_pids":row.4,
            })
        })
        .collect();
    eprintln!(
        "STATEKNOT_FRAME_CANCELLATION_CONTROLLED_CAS_EVIDENCE={}",
        json!({
            "profile":"frame-cancellation-controlled-stale-candidates-v1",
            "tenant":tenant.as_str(),"run":run.to_string(),"holder_pid":holder_pid,
            "left_pid":left_pid,"right_pid":right_pid,"waiters":actual_waiters,
        "backend_identity":"same-database stateknot before/after PID sets with exactly one new single-pool session each",
            "closed_tenant_unchanged_before_release":true,
            "left_outcome":if committed(&left_result) { "Committed" } else { "Idempotent" },
            "right_outcome":if committed(&right_result) { "Committed" } else { "Idempotent" },
            "terminal_event_id":event.event_id().to_string(),"terminal_sequence":event.sequence().get(),
            "raw_stale_error_observed":false,
            "stale_candidate_path_evidence":"both exact append parent FOR UPDATE statements blocked before either terminal append; actual loser recovered winner ACK under the unchanged finite retry policy",
        })
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn independent_finalizers_converge_then_cold_ack_survives_child_quarantine() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let scenario = Box::pin(prepare(&store)).await;
    let closure = Box::pin(close_prepared(&store, &scenario)).await;
    let pool = acceptance_pool().await;
    let wire = v2_bytes(&pool, &scenario).await;
    let account = Box::pin(
        store.load_child_budget_account(scenario.key.tenant_id(), scenario.key.parent_run_id()),
    )
    .await
    .unwrap()
    .unwrap();
    let before = snapshot(&pool, scenario.key.tenant_id()).await;
    let left_store = independent_store(&store).await;
    let right_store = independent_store(&store).await;
    let left = finalizer(&left_store);
    let right = finalizer(&right_store);
    let barrier = tokio::sync::Barrier::new(2);
    let (left_result, right_result) = tokio::join!(
        async {
            barrier.wait().await;
            left.resume(scenario.key.tenant_id(), scenario.key.parent_run_id())
                .await
        },
        async {
            barrier.wait().await;
            right
                .resume(scenario.key.tenant_id(), scenario.key.parent_run_id())
                .await
        },
    );
    let left_result = left_result.unwrap();
    let right_result = right_result.unwrap();
    assert_eq!(
        u8::from(committed(&left_result)) + u8::from(committed(&right_result)),
        1
    );
    assert_eq!(
        u8::from(idempotent(&left_result)) + u8::from(idempotent(&right_result)),
        1
    );
    let event = cancelled_event(&left_result).clone();
    assert_eq!(cancelled_event(&right_result), &event);
    Box::pin(verify_terminal(&store, &scenario, &closure, &event)).await;
    assert_eq!(
        Box::pin(
            store.load_child_budget_account(scenario.key.tenant_id(), scenario.key.parent_run_id())
        )
        .await
        .unwrap()
        .unwrap(),
        account
            .observe_direct(event.head(), scenario.direct.clone())
            .unwrap()
    );
    let after = snapshot(&pool, scenario.key.tenant_id()).await;
    only_terminal_parent_changed(&before, &after, scenario.key.parent_run_id());
    assert_eq!(v2_bytes(&pool, &scenario).await, wire);
    drop(left);
    drop(right);
    left_store.close().await;
    right_store.close().await;
    let cold_store = independent_store(&store).await;
    let recovered = finalizer(&cold_store)
        .resume(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    assert!(idempotent(&recovered));
    assert_eq!(cancelled_event(&recovered), &event);
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, after);
    cold_store.close().await;
    let child = Box::pin(store.load_child_run(&scenario.key)).await.unwrap();
    Box::pin(quarantine_actual_run(
        &store,
        scenario.key.tenant_id(),
        child.intent().child().provenance().run_id(),
    ))
    .await;
    let quarantined = snapshot(&pool, scenario.key.tenant_id()).await;
    let later_store = independent_store(&store).await;
    let recovered = finalizer(&later_store)
        .resume(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    assert!(idempotent(&recovered));
    assert_eq!(cancelled_event(&recovered), &event);
    Box::pin(verify_terminal(&store, &scenario, &closure, &event)).await;
    assert_eq!(v2_bytes(&pool, &scenario).await, wire);
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, quarantined);
    eprintln!(
        "independent finalizers: committed=1 idempotent=1 cold_ack=1 child_quarantine_ack=1 revision_delta=1 strict_completion_clock=true"
    );
    later_store.close().await;
    Box::pin(controlled_stale_candidates_recover_winner(&store, &pool)).await;
    pool.close().await;
    store.close().await;
}

fn raw_ids(candidates: &[GraphFrameClosureCursor]) -> Vec<RunId> {
    candidates
        .iter()
        .map(GraphFrameClosureCursor::run_id)
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn raw_history_pages_advance_through_terminal_rows_and_revisit_late_keys() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-finalizer-raw-history");
    let worker = finalizer(&store);
    let pool = acceptance_pool().await;
    let mut scenarios = Vec::new();
    for ordinal in 1..=33 {
        let scenario = Box::pin(prepare_ordered(&store, &tenant, ordinal * 10)).await;
        let closure = Box::pin(close_prepared(&store, &scenario)).await;
        v2_bytes(&pool, &scenario).await;
        if ordinal <= 32 {
            let outcome = worker
                .resume(&tenant, scenario.key.parent_run_id())
                .await
                .unwrap();
            assert!(committed(&outcome));
            Box::pin(verify_terminal(
                &store,
                &scenario,
                &closure,
                cancelled_event(&outcome),
            ))
            .await;
        }
        scenarios.push(scenario);
    }
    let expected: Vec<_> = scenarios
        .iter()
        .map(|scenario| scenario.key.parent_run_id())
        .collect();
    let page = store.graph_frame_closure_page(&tenant, None).await.unwrap();
    assert_eq!(page.candidates().len(), 16);
    assert!(page.has_more());
    assert_eq!(raw_ids(page.candidates()), expected[..16]);
    let before = snapshot(&pool, &tenant).await;
    let first = worker
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(first.items().is_empty() && !first.is_cancelled());
    assert_eq!(first.cursor().unwrap().run_id(), expected[15]);
    assert_eq!(snapshot(&pool, &tenant).await, before);
    let first_cursor = first.cursor().unwrap().clone();
    let late = Box::pin(prepare_ordered(&store, &tenant, 5)).await;
    Box::pin(close_prepared(&store, &late)).await;
    let higher = Box::pin(prepare_ordered(&store, &tenant, 340)).await;
    Box::pin(close_prepared(&store, &higher)).await;
    assert!(late.key.parent_run_id() < first_cursor.run_id());
    assert!(higher.key.parent_run_id() > *expected.last().unwrap());
    let page = store
        .graph_frame_closure_page(&tenant, Some(&first_cursor))
        .await
        .unwrap();
    assert_eq!(page.candidates().len(), 16);
    assert!(page.has_more());
    assert_eq!(raw_ids(page.candidates()), expected[16..32]);
    let before = snapshot(&pool, &tenant).await;
    let second = worker
        .tick(
            tenant.clone(),
            Some(first_cursor),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert!(second.items().is_empty() && !second.is_cancelled());
    assert_eq!(second.cursor().unwrap().run_id(), expected[31]);
    assert_eq!(snapshot(&pool, &tenant).await, before);
    let page = store
        .graph_frame_closure_page(&tenant, second.cursor())
        .await
        .unwrap();
    assert_eq!(raw_ids(page.candidates()), vec![expected[32]]);
    assert!(!page.has_more());
    let exhausted = store
        .graph_frame_closure_page(&tenant, page.candidates().last())
        .await
        .unwrap();
    assert!(exhausted.candidates().is_empty() && !exhausted.has_more());
    let runnable = store
        .load_runnable_run_page(&tenant, None, RunnableRunPageSize::new(16).unwrap())
        .await
        .unwrap();
    assert!(runnable.records().is_empty() && !runnable.has_more());
    let before_claim = snapshot(&pool, &tenant).await;
    let refused = store
        .claim_lease(&tenant, expected[32], AttemptId::generate())
        .await;
    assert!(matches!(
        refused,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert_eq!(snapshot(&pool, &tenant).await, before_claim);
    let last = worker
        .tick(
            tenant.clone(),
            second.cursor().cloned(),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert!(last.cursor().is_none() && !last.is_cancelled());
    assert_eq!(last.items().len(), 1);
    assert_eq!(last.items()[0].candidate().run_id(), expected[32]);
    assert!(committed(last.items()[0].result().unwrap()));
    let after = snapshot(&pool, &tenant).await;
    only_terminal_parent_changed(&before_claim, &after, expected[32]);
    for scenario in [&late, &higher] {
        Box::pin(owning_close(&store, scenario)).await;
    }
    let mut cursor = None;
    let mut discovered = Vec::new();
    let mut page_lengths = Vec::new();
    for _ in 0..3 {
        let page = store
            .graph_frame_closure_page(&tenant, cursor.as_ref())
            .await
            .unwrap();
        page_lengths.push(page.candidates().len());
        let tick = worker
            .tick(tenant.clone(), cursor, CancellationSignal::never())
            .await
            .unwrap();
        assert!(!tick.is_cancelled());
        for item in tick.items() {
            assert!(committed(item.result().unwrap()));
            discovered.push(item.candidate().run_id());
        }
        cursor = tick.cursor().cloned();
    }
    assert_eq!(page_lengths, vec![16, 16, 3]);
    assert!(cursor.is_none());
    assert_eq!(
        discovered,
        vec![late.key.parent_run_id(), higher.key.parent_run_id()]
    );
    let foreign_tenant = super::tenant("frame-finalizer-foreign-cursor");
    let foreign = Box::pin(prepare_ordered(&store, &foreign_tenant, 10)).await;
    Box::pin(close_prepared(&store, &foreign)).await;
    let foreign_page = store
        .graph_frame_closure_page(&foreign_tenant, None)
        .await
        .unwrap();
    assert_eq!(foreign_page.candidates().len(), 1);
    let foreign_cursor = foreign_page.candidates()[0].clone();
    let local_before = snapshot(&pool, &tenant).await;
    let foreign_before = snapshot(&pool, &foreign_tenant).await;
    assert!(matches!(
        store
            .graph_frame_closure_page(&tenant, Some(&foreign_cursor))
            .await,
        Err(StoreError::InvalidGraphFrameClosureCursor)
    ));
    assert!(matches!(
        worker
            .tick(
                tenant.clone(),
                Some(foreign_cursor),
                CancellationSignal::never()
            )
            .await,
        Err(StoreError::InvalidGraphFrameClosureCursor)
    ));
    assert_eq!(snapshot(&pool, &tenant).await, local_before);
    assert_eq!(snapshot(&pool, &foreign_tenant).await, foreign_before);
    eprintln!(
        "raw finalizer pages: original=33 first=16/0items second=16/0items last=1/1item next_cycle=16,16,3 late_and_higher_discovered=2 foreign_rejected=true scheduler_closed_excluded=true direct_claim_rejected=true tenant={tenant}"
    );
    pool.close().await;
    store.close().await;
}

#[derive(Clone)]
struct Shutdown {
    requested: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            requested: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl CancellationObserver for Shutdown {
    fn is_cancelled(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.is_cancelled() {
                notified.await;
            }
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn admitted_terminal_ack_survives_shutdown_and_later_item_errors_do_not_starve() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-finalizer-shutdown-progress");
    let first = Box::pin(prepare_ordered(&store, &tenant, 10)).await;
    let first_closure = Box::pin(close_prepared(&store, &first)).await;
    let second = Box::pin(prepare_ordered(&store, &tenant, 20)).await;
    let second_closure = Box::pin(close_prepared(&store, &second)).await;
    let pool = acceptance_pool().await;
    let before_shutdown = snapshot(&pool, &tenant).await;
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    // Composed from the independently frozen terminal-fault proxy overlay.
    let mut proxy = CommitProxy::start(
        &connection,
        Cut::CommitResponse,
        TransactionTarget::GraphFrameCancellationConfirmation,
    )
    .await;
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
    let worker = finalizer(&proxied);
    let stop = Shutdown::new();
    let signal = CancellationSignal::new(stop.clone());
    let selected_tenant = tenant.clone();
    let mut running = tokio::spawn(async move { worker.tick(selected_tenant, None, signal).await });
    let mut joined = false;
    // Keep the admitted operation owned even if a held-cut assertion panics.
    let observed = std::panic::AssertUnwindSafe(async {
        tokio::select! {
            () = proxy.wait_cut(Cut::CommitResponse) => {},
            result = &mut running => {
                joined = true;
                panic!("tick returned before actual terminal ACK cut: {result:?}");
            },
        }
        let terminal = store
            .load_run(&tenant, first.key.parent_run_id())
            .await
            .unwrap();
        assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
        assert_eq!(
            terminal.lifecycle().revision().get(),
            first.revision.get() + 1
        );
        let actual = store
            .load_journal_page(
                &tenant,
                first.key.parent_run_id(),
                Some(&first_closure.event().head()),
                JournalPageSize::new(1).unwrap(),
            )
            .await
            .unwrap()
            .events()[0]
            .clone();
        stop.request();
        // Poll the actual task after cancellation while both real ACK frames
        // remain withheld. Timeout is the required pending observation here.
        match tokio::time::timeout(Duration::from_millis(100), &mut running).await {
            Err(_) => {}
            Ok(result) => {
                joined = true;
                panic!("shutdown returned while the admitted COMMIT ACK was held: {result:?}");
            }
        }
        proxy.resume_ack();
        let result = tokio::time::timeout(Duration::from_secs(15), &mut running)
            .await
            .expect("released terminal ACK did not finish the owned tick");
        joined = true;
        (result.unwrap().unwrap(), actual)
    })
    .catch_unwind()
    .await;
    if !joined {
        running.abort();
        let _ = (&mut running).await;
    }
    // wait_cut may already have joined a failed proxy task before panicking.
    // Preserve that original failure while still completing pool cleanup.
    let proxy_stopped = std::panic::AssertUnwindSafe(proxy.stop())
        .catch_unwind()
        .await;
    proxied.close().await;
    let (tick, actual) = match (observed, proxy_stopped) {
        (Ok(value), Ok(())) => value,
        (Err(panic), _) | (Ok(_), Err(panic)) => {
            pool.close().await;
            store.close().await;
            std::panic::resume_unwind(panic);
        }
    };
    assert!(tick.is_cancelled());
    assert_eq!(tick.items().len(), 1);
    assert_eq!(
        tick.items()[0].candidate().run_id(),
        first.key.parent_run_id()
    );
    assert!(committed(tick.items()[0].result().unwrap()));
    assert_eq!(cancelled_event(tick.items()[0].result().unwrap()), &actual);
    assert_eq!(tick.cursor().unwrap().run_id(), first.key.parent_run_id());
    only_terminal_parent_changed(
        &before_shutdown,
        &snapshot(&pool, &tenant).await,
        first.key.parent_run_id(),
    );
    Box::pin(owning_close(&store, &second)).await;
    Box::pin(verify_terminal(&store, &first, &first_closure, &actual)).await;
    let continued = finalizer(&store)
        .tick(
            tenant.clone(),
            tick.cursor().cloned(),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(continued.items().len(), 1);
    assert_eq!(
        continued.items()[0].candidate().run_id(),
        second.key.parent_run_id()
    );
    assert!(committed(continued.items()[0].result().unwrap()));
    assert!(continued.cursor().is_none());
    Box::pin(verify_terminal(
        &store,
        &second,
        &second_closure,
        cancelled_event(continued.items()[0].result().unwrap()),
    ))
    .await;
    let rejected = Box::pin(prepare_ordered(&store, &tenant, 30)).await;
    Box::pin(close_prepared(&store, &rejected)).await;
    let later = Box::pin(prepare_ordered(&store, &tenant, 40)).await;
    let later_closure = Box::pin(close_prepared(&store, &later)).await;
    Box::pin(quarantine_actual_run(
        &store,
        &tenant,
        rejected.key.parent_run_id(),
    ))
    .await;
    let before = snapshot(&pool, &tenant).await;
    let rejected_wire = v2_bytes(&pool, &rejected).await;
    let progressed = finalizer(&store)
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(!progressed.is_cancelled() && progressed.cursor().is_none());
    assert_eq!(progressed.items().len(), 2);
    assert_eq!(
        progressed.items()[0].candidate().run_id(),
        rejected.key.parent_run_id()
    );
    assert!(matches!(
        progressed.items()[0].result(),
        Err(StoreError::RunQuarantined)
    ));
    assert_eq!(
        progressed.items()[1].candidate().run_id(),
        later.key.parent_run_id()
    );
    assert!(committed(progressed.items()[1].result().unwrap()));
    Box::pin(verify_terminal(
        &store,
        &later,
        &later_closure,
        cancelled_event(progressed.items()[1].result().unwrap()),
    ))
    .await;
    let after = snapshot(&pool, &tenant).await;
    only_terminal_parent_changed(&before, &after, later.key.parent_run_id());
    assert_eq!(v2_bytes(&pool, &rejected).await, rejected_wire);
    assert_eq!(
        store
            .load_run(&tenant, rejected.key.parent_run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::CancellationRequested
    );
    eprintln!(
        "finalizer progress: actual_ack_retained=true shutdown_cursor_only_first=true untouched_next=true item_error=RunQuarantined later_item_committed=true"
    );
    pool.close().await;
    store.close().await;
}

#[allow(clippy::too_many_lines)]
async fn enter_without_children(
    store: &PostgresStore,
    label: &str,
) -> (StoredAgentAdmission, StoredGraphFrameEntry, RunFence) {
    let (fixture, leaf, isolated, _) = graphs();
    let tenant = tenant(label);
    let ids = AgentRunIds::generate();
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
    let admitted = Box::pin(
        store.admit_agent_run(
            request.intent().clone(),
            JournalAppend::new(
                JournalExpectation::empty(),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    ids.run_id(),
                    request.admission_event_id(),
                    JournalPayload::new(
                        test_payload().schema().clone(),
                        JournalEventKind::new(AgentAdmission::JOURNAL_EVENT_KIND).unwrap(),
                        BoundedJson::try_from_value(
                            json!({"intent_digest":request.intent().intent_digest()}),
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
            .unwrap(),
            CheckpointWrite::initial(
                tenant.clone(),
                ids.run_id(),
                request.initial_checkpoint_id(),
                fixture.graph.reference(),
                request.initial_state().clone(),
                fixture.graph.entry_nodes().clone(),
            )
            .unwrap(),
            fixture.registry.schemas(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        admitted,
        AgentAdmissionCommitOutcome::Committed(_)
    ));
    let admission = admitted.stored().clone();
    let fence = store
        .claim_lease(&tenant, ids.run_id(), AttemptId::generate())
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
        initial_usage(&admission),
        fixture.registry.schemas(),
    ))
    .await
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(entry) = entered else {
        panic!("the no-child fixture must enter its actual framework frame");
    };
    assert!(
        Box::pin(store.load_child_budget_account(&tenant, ids.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    (admission, entry, fence)
}

async fn raw_wire(pool: &PgPool, tenant: &TenantId, run: RunId) -> Vec<u8> {
    query_scalar(
        "SELECT closure_bytes FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn zero_child_v1_closure_is_finalized_and_sealed_failure_is_not_applicable() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let pool = acceptance_pool().await;
    let (admission, entry, fence) = Box::pin(enter_without_children(
        &store,
        "frame-finalizer-no-child-v1",
    ))
    .await;
    let before = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let cancel_id = EventId::generate();
    let request = cancellation(store.observe_database_clock().await.unwrap(), cancel_id);
    let requested = store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(before.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    cancel_id,
                    payload("test-no-child-frame-cancellation-requested"),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                before.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: request.clone(),
                },
            ),
        )
        .await
        .unwrap();
    assert!(matches!(requested, AppendOutcome::Committed(_)));
    let original = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let result = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        original.journal_head().unwrap().clone(),
        original.lifecycle().revision(),
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        result,
        GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let closure = Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closure.root_checkpoint(), &admission.checkpoint().head());
    assert_eq!(
        serde_json::to_value(closure.original_cancellation_lifecycle().unwrap()).unwrap(),
        serde_json::to_value(original.lifecycle()).unwrap()
    );
    assert_eq!(closure.failure().id(), request.failure().id());
    assert_eq!(closure.completions().len(), 1);
    assert_eq!(
        closure.completions()[0].start(),
        &entry.entry().start().head()
    );
    assert_eq!(closure.completions()[0].start().fence(), &fence);
    assert_eq!(closure.completions()[0].usage(), &BudgetUsage::zero());
    let wire = raw_wire(&pool, fence.tenant_id(), fence.run_id()).await;
    let decoded: Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(decoded["version"], 1);
    assert_eq!(decoded["intent"]["version"], 1);
    assert!(decoded["intent"]["failure_close"].is_null());
    assert!(decoded["intent"].get("stopped_join").is_none());
    assert!(
        Box::pin(store.load_child_budget_account(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    let before = snapshot(&pool, fence.tenant_id()).await;
    let outcome = finalizer(&store)
        .resume(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(committed(&outcome));
    let event = cancelled_event(&outcome);
    let terminal = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let expected = original
        .lifecycle()
        .clone()
        .apply(RunTransition::ConfirmCancellation {
            completed_at: terminal.lifecycle().changed_at(),
            usage: closure.direct_usage().clone(),
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(terminal.lifecycle()).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        terminal.lifecycle().terminal_usage(),
        Some(closure.direct_usage())
    );
    assert!(terminal.lifecycle().changed_at() < event.recorded_at());
    assert_eq!(event.sequence().get(), closure.event().sequence().get() + 1);
    assert_eq!(terminal.journal_head(), Some(&event.head()));
    assert!(terminal.lease().is_none() && !terminal.is_quarantined());
    assert_eq!(terminal.last_fencing_epoch(), Some(fence.epoch()));
    assert!(
        Box::pin(store.load_child_budget_account(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    only_terminal_parent_changed(
        &before,
        &snapshot(&pool, fence.tenant_id()).await,
        fence.run_id(),
    );
    assert_eq!(
        raw_wire(&pool, fence.tenant_id(), fence.run_id()).await,
        wire
    );
    let after = snapshot(&pool, fence.tenant_id()).await;
    let recovered = finalizer(&store)
        .resume(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(idempotent(&recovered));
    assert_eq!(cancelled_event(&recovered), event);
    assert_eq!(snapshot(&pool, fence.tenant_id()).await, after);

    let (admission, entry, fence) = Box::pin(enter_without_children(
        &store,
        "frame-finalizer-sealed-failure-v1",
    ))
    .await;
    let failure = test_failure(
        "test.frame.finalizer.sealed",
        "Retain this legal sealed Internal failure.",
    );
    assert_eq!(failure.category(), FailureCategory::Internal);
    let direct = entry.direct_usage_after().unwrap();
    let decision = Box::pin(store.request_run_failure_close(
        &admission.checkpoint().head(),
        failure.clone(),
        direct.clone(),
        worker_append_kind(
            fence.tenant_id(),
            fence.run_id(),
            entry.event().head(),
            &fence,
            "run-failure-close-requested",
        ),
    ))
    .await
    .unwrap();
    assert!(matches!(decision, RunFailureCloseOutcome::Committed(_)));
    let sealed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let result = Box::pin(store.close_graph_frames(
        fence.tenant_id(),
        fence.run_id(),
        EventId::generate(),
        sealed.journal_head().unwrap().clone(),
        sealed.lifecycle().revision(),
        direct,
    ))
    .await
    .unwrap();
    assert!(matches!(
        result,
        GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let closure = Box::pin(store.load_graph_frame_closure(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closure.root_checkpoint(), &admission.checkpoint().head());
    assert!(closure.original_cancellation_lifecycle().is_none());
    assert_eq!(
        serde_json::to_value(closure.failure()).unwrap(),
        serde_json::to_value(&failure).unwrap()
    );
    assert_eq!(closure.completions()[0].start().fence(), &fence);
    let wire = raw_wire(&pool, fence.tenant_id(), fence.run_id()).await;
    let decoded: Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(decoded["version"], 1);
    assert_eq!(decoded["intent"]["version"], 1);
    assert!(!decoded["intent"]["failure_close"].is_null());
    assert!(
        Box::pin(store.load_child_budget_account(fence.tenant_id(), fence.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    let before = snapshot(&pool, fence.tenant_id()).await;
    let result = finalizer(&store)
        .resume(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(matches!(result, FrameCancellationOutcome::NotApplicable));
    assert_eq!(snapshot(&pool, fence.tenant_id()).await, before);
    assert_eq!(
        raw_wire(&pool, fence.tenant_id(), fence.run_id()).await,
        wire
    );
    assert_eq!(snapshot(&pool, fence.tenant_id()).await, before);
    eprintln!(
        "finalizer owning v1: no_account=true cancellation_committed=true original_lifecycle_getter=Some sealed_Internal_original_lifecycle_getter=None sealed_NotApplicable=true sealed_whole_tenant_unchanged=true"
    );
    pool.close().await;
    store.close().await;
}
