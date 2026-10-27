// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! A real terminal commit overtakes the owning reader at its child row lock.
use super::*;
use tokio::task::JoinSet;

#[derive(Debug)]
struct Backend {
    pid: i32,
    database: String,
    started: String,
}

type LockWait = (i32, String, String, String, String, String, Vec<i32>);

#[derive(Debug)]
enum Completion {
    Terminal(Box<AppendOutcome>),
    Caller(Box<NodeAttemptCommitOutcome>),
}

async fn dedicated_store(store: &PostgresStore, observer: &PgPool) -> (PostgresStore, Backend) {
    // Production pins application_name=stateknot. Discover the one new real
    // session instead of changing that configuration or guessing a backend PID.
    let before = query_scalar::<_, i32>(
        "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name='stateknot' ORDER BY pid",
    )
    .fetch_all(observer)
    .await
    .unwrap();
    let selected = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store
            .options()
            .clone()
            .with_pool_size(1, 1)
            .with_transaction_timeouts(Duration::from_secs(10), Duration::from_secs(20)),
    )
    .await
    .unwrap();
    let after = query_as::<_, (i32, String, String)>(
        "SELECT pid,datname,backend_start::text FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND application_name='stateknot' ORDER BY pid",
    )
    .fetch_all(observer)
    .await
    .unwrap();
    let mut added = after.into_iter().filter(|row| !before.contains(&row.0));
    let (pid, database, started) = added
        .next()
        .expect("a public one-connection Store has a backend");
    assert!(added.next().is_none(), "exactly one new Store backend");
    (
        selected,
        Backend {
            pid,
            database,
            started,
        },
    )
}

fn waiting_on_run(row: &LockWait, backend: &Backend) -> bool {
    row.0 == backend.pid
        && row.1 == backend.database
        && row.2 == backend.started
        && row.3 == "active"
        && row.4 == "Lock"
        && row.5.contains("FROM stateknot.runs")
        && row.5.contains("WHERE tenant_id = $1 AND run_id = $2")
        && row.5.trim_end().ends_with("FOR UPDATE")
}

async fn child_lock_queue(
    observer: &PgPool,
    holder: i32,
    terminal: &Backend,
    caller: Option<&Backend>,
    tasks: &mut JoinSet<Completion>,
) -> Vec<LockWait> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let probe = async {
        loop {
            let rows = tokio::time::timeout_at(deadline, query_as::<_, LockWait>(
                "SELECT pid,datname,backend_start::text,COALESCE(state,''),COALESCE(wait_event_type,''),query,pg_blocking_pids(pid) FROM pg_stat_activity WHERE datname=current_database() AND application_name='stateknot' AND (pid=$1 OR pid=$2) ORDER BY pid",
            )
            .bind(terminal.pid)
            .bind(caller.map_or(terminal.pid, |value| value.pid))
            .fetch_all(observer))
            .await
            .expect("actual child-lock activity probe deadline")
            .unwrap();
            let terminal_wait = rows.iter().find(|row| row.0 == terminal.pid);
            let terminal_ready = terminal_wait
                .is_some_and(|row| waiting_on_run(row, terminal) && row.6.contains(&holder));
            let caller_ready = caller.is_none_or(|backend| {
                rows.iter()
                    .any(|row| waiting_on_run(row, backend) && row.6.contains(&terminal.pid))
            });
            if terminal_ready && caller_ready {
                return rows;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "exact terminal->holder and caller->terminal child lock chain: {rows:?}"
            );
            // Fresh autocommit activity reads drive readiness; elapsed time is
            // only a failure bound, never permission to advance a race phase.
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        rows = probe => rows,
        completed = tasks.join_next() => panic!(
            "participant completed before its actual child-lock rendezvous: {completed:?}"
        ),
    }
}

async fn assert_parent_is_locked(observer: &PgPool, tenant: &TenantId, run: RunId) {
    let mut probe = observer.begin().await.unwrap();
    let refused = query_scalar::<_, Uuid>(
        "SELECT run_id FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE NOWAIT",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .fetch_one(&mut *probe)
    .await
    .expect_err("F2 holds this exact parent while waiting behind the child terminal");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(sqlx_core::error::DatabaseError::code)
            .as_deref(),
        Some("55P03")
    );
    probe.rollback().await.unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn terminal_commit_overlaps_f2_caller_with_a_consistent_complete_account() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = Box::pin(started(&store, "pending-caller-terminal-overlap")).await;
    child_with_retries(&mut value, 0);
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let child = spawned.record().child().clone();
    let tenant = value.fence.tenant_id();
    let parent = value.fence.run_id();
    let child_id = child.admission().intent().provenance().run_id();
    let next = higher_fence(&store, &value).await;
    let direct = complete_usage(&store, &value).await;
    let account = store
        .load_child_budget_account(tenant, parent)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert!(account.children()[0].settlement().is_none());
    assert!(
        store
            .pending_child_settlements(tenant)
            .await
            .unwrap()
            .is_empty()
    );
    let current = store.load_run(tenant, child_id).await.unwrap();
    let terminal_id = EventId::generate();
    let at = store.observe_database_clock().await.unwrap();
    let failure = RunFailure::new(
        terminal_run_failure(terminal_id, at).failure().clone(),
        at,
        initial_usage(&child),
    )
    .unwrap();
    let expected_lifecycle = serde_json_canonicalizer::to_vec(
        &current
            .lifecycle()
            .clone()
            .apply(RunTransition::Fail {
                failure: failure.clone(),
            })
            .unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            tenant.clone(),
            child_id,
            terminal_id,
            payload("test-failed"),
        )
        .unwrap(),
    )
    .unwrap();
    let projection = RunProjection::transition(
        current.lifecycle().revision(),
        RunTransition::Fail { failure },
    );
    let observer = super::super::super::frame_barriers::pool().await;
    let (terminal_store, terminal_backend) = dedicated_store(&store, &observer).await;
    let (caller_store, caller_backend) = dedicated_store(&store, &observer).await;
    assert_ne!(terminal_backend.pid, caller_backend.pid);
    assert_eq!(terminal_backend.database, caller_backend.database);

    // The child holder is only a rendezvous: it locks this actual child row
    // and rolls back without changing any business fact.
    let mut holder = observer.begin().await.unwrap();
    let holder_pid: i32 = query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    assert_ne!(holder_pid, terminal_backend.pid);
    assert_ne!(holder_pid, caller_backend.pid);
    let held: Uuid = query_scalar(
        "SELECT run_id FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE",
    )
    .bind(tenant.as_str())
    .bind(*child_id.as_uuid())
    .fetch_one(&mut *holder)
    .await
    .unwrap();
    assert_eq!(held, *child_id.as_uuid());
    let mut tasks = JoinSet::new();
    let terminal_actor = terminal_store.clone();
    tasks.spawn(async move {
        Completion::Terminal(Box::new(
            Box::pin(terminal_actor.append_control_plane(append, projection))
                .await
                .expect("real child terminal transaction commits"),
        ))
    });
    child_lock_queue(&observer, holder_pid, &terminal_backend, None, &mut tasks).await;
    let caller_actor = caller_store.clone();
    let frame = value.frame.frame().clone();
    let fence = next.clone();
    let spawn_head = spawned.record().spawn().head();
    tasks.spawn(async move {
        Completion::Caller(Box::new(
            Box::pin(caller_actor.rebind_graph_frame_caller(
                &frame,
                AttemptId::generate(),
                EventId::generate(),
                fence,
                spawn_head,
                direct,
            ))
            .await
            .expect("F2 refreshes complete child evidence after acquiring the child lock"),
        ))
    });
    let queued = child_lock_queue(
        &observer,
        holder_pid,
        &terminal_backend,
        Some(&caller_backend),
        &mut tasks,
    )
    .await;
    // The terminal actor only targets child_id. With its waiter blocked and
    // the caller queued behind it, the exact parent's NOWAIT refusal proves
    // that F2 retains the parent lock across this child terminal commit.
    assert_parent_is_locked(&observer, tenant, parent).await;
    assert!(
        !store
            .load_run(tenant, child_id)
            .await
            .unwrap()
            .lifecycle()
            .status()
            .is_terminal()
    );
    assert!(
        store
            .pending_child_settlements(tenant)
            .await
            .unwrap()
            .is_empty()
    );
    eprintln!(
        "STATEKNOT_PENDING_CALLER_TERMINAL_QUEUE_EVIDENCE={}",
        json!({
            "database": terminal_backend.database, "tenant": tenant.as_str(),
            "parent_run": parent.to_string(), "child_run": child_id.to_string(),
            "holder_pid": holder_pid, "terminal_pid": terminal_backend.pid,
            "caller_pid": caller_backend.pid, "terminal_waits_first": true,
            "caller_waits_behind_terminal": true, "parent_nowait_sqlstate": "55P03",
            "actual_wait_rows": queued,
        })
    );
    holder.rollback().await.unwrap();
    let mut terminal = None;
    let mut rebound = None;
    for _ in 0..2 {
        let completed = tokio::time::timeout(Duration::from_secs(8), tasks.join_next())
            .await
            .expect("terminal and F2 must finish without a reverse parent/child deadlock")
            .unwrap()
            .unwrap();
        match completed {
            Completion::Terminal(result) => assert!(terminal.replace(*result).is_none()),
            Completion::Caller(result) => assert!(rebound.replace(*result).is_none()),
        }
    }
    assert!(tasks.is_empty());
    let terminal = terminal.unwrap();
    let rebound = rebound.unwrap();
    assert!(matches!(terminal, AppendOutcome::Committed(_)));
    assert!(matches!(
        rebound,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    assert_eq!(terminal.event().event_id(), terminal_id);
    assert_eq!(
        rebound.attempt().start().activation(),
        value.frame.frame().origin()
    );
    assert_eq!(rebound.attempt().start().fence(), &next);
    let retained = store.load_child_run(value.intent.key()).await.unwrap();
    assert_eq!(retained.spawn(), spawned.record().spawn());
    assert_eq!(retained.child().admission(), child.admission());
    assert_eq!(retained.child().event(), child.event());
    assert_eq!(retained.child().checkpoint(), child.checkpoint());
    assert_eq!(
        retained.child().run().journal_head(),
        Some(&terminal.event().head())
    );
    assert_eq!(
        retained.child().run().lifecycle().status(),
        RunStatus::Failed
    );
    assert_eq!(
        serde_json_canonicalizer::to_vec(retained.child().run().lifecycle()).unwrap(),
        expected_lifecycle
    );
    assert!(retained.settlement().is_none());
    assert_eq!(
        store
            .load_child_budget_account(tenant, parent)
            .await
            .unwrap()
            .as_ref(),
        Some(&account)
    );
    assert_eq!(
        store.pending_child_settlements(tenant).await.unwrap(),
        vec![value.intent.key().clone()]
    );
    assert_eq!(
        store
            .list_child_run_identities(tenant, parent)
            .await
            .unwrap(),
        vec![(value.intent.key().digest(), child_id)]
    );
    assert!(
        store
            .load_child_join(value.intent.key().parent())
            .await
            .unwrap()
            .is_none()
    );
    let active = store
        .load_active_graph_frame(tenant, parent)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint(), &value.frame);
    assert_eq!(active.caller(), rebound.attempt().start());
    let after = snapshot(&observer, tenant).await;
    let retry = Box::pin(store.rebind_graph_frame_caller(
        value.frame.frame(),
        AttemptId::generate(),
        EventId::generate(),
        next,
        value.head.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.event(), rebound.event());
    assert_eq!(retry.attempt().start(), rebound.attempt().start());
    assert_eq!(snapshot(&observer, tenant).await, after);
    eprintln!(
        "STATEKNOT_PENDING_CALLER_TERMINAL_OVERLAP_EVIDENCE={}",
        json!({
            "tenant": tenant.as_str(), "parent_run": parent.to_string(), "child_run": child_id.to_string(),
            "terminal_event": terminal_id.to_string(), "caller_event": rebound.event().event_id().to_string(),
            "real_terminal_committed": true, "caller_committed_after_child_lock": true,
            "canonical_lifecycle_matches": true, "account_and_pending_ceiling_unchanged": true,
            "full_owning_read_consistent": true, "idempotent_retry_zero_tenant_writes": true,
        })
    );
    terminal_store.close().await;
    caller_store.close().await;
    observer.close().await;
    store.close().await;
}
