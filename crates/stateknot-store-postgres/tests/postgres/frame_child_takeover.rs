// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Structural takeover retains real pending children and their reserved capacity.
use super::*;
use serde_json::Value;
use stateknot_core::{CumulativeBudgetReservation, ExecutionCount, GraphFrameIdentity};
use std::collections::BTreeMap;

type Snapshot = BTreeMap<String, Value>;

async fn snapshot(db: &PgPool, tenant: &TenantId) -> Snapshot {
    let tables = query_scalar::<_, String>(
        "SELECT c.table_name FROM information_schema.columns c JOIN information_schema.tables t USING (table_catalog,table_schema,table_name) WHERE c.table_schema='stateknot' AND c.column_name='tenant_id' AND t.table_type='BASE TABLE' ORDER BY c.table_name",
    )
    .fetch_all(db)
    .await
    .unwrap();
    let mut rows = BTreeMap::new();
    for table in tables {
        assert!(
            table
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        );
        let encoded = query_scalar::<_, String>(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM stateknot.{table} t WHERE tenant_id=$1",
        ))
        .bind(tenant.as_str())
        .fetch_one(db)
        .await
        .unwrap();
        let mut data: Value = serde_json::from_str(&encoded).unwrap();
        data.as_array_mut()
            .unwrap()
            .sort_by_key(|row| serde_json::to_string(row).unwrap());
        rows.insert(table, data);
    }
    rows
}

// These fixture applications have no provider I/O. Their complete original
// DIRECT observation is retained by Fixture; only real parent event bytes
// have been added by child admission or a zero-usage ordinary completion.
async fn complete_usage(store: &PostgresStore, value: &Fixture) -> BudgetUsage {
    let page = store
        .load_journal_page(
            value.fence.tenant_id(),
            value.fence.run_id(),
            None,
            JournalPageSize::new(32).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    let bytes = page
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len() as u64)
        .sum::<u64>();
    value
        .direct
        .checked_accumulate(
            &BudgetUsage::builder()
                .event_bytes(ByteCount::new(bytes - value.direct.event_bytes().get()))
                .build()
                .unwrap(),
        )
        .unwrap()
}

fn child_with_retries(value: &mut Fixture, retries: u64) {
    let original = value.intent.child();
    let child = AgentAdmissionIntent::new(
        original.provenance().clone(),
        original.descriptor().clone(),
        AgentRequest::new(
            original.request().input_schema().clone(),
            original.request().input().clone(),
            original
                .request()
                .budget_limits()
                .clone()
                .with_retries(ExecutionCount::new(retries)),
        ),
        original.budget_layers().iter().cloned(),
        original.graph().clone(),
        original.authority().clone(),
    )
    .unwrap();
    value.intent = ChildRunAdmissionIntent::for_frame(
        value.admission.admission(),
        &value.frame,
        value.intent.key().clone(),
        child,
        value.intent.child_graph().clone(),
        value.intent.initial_state().clone(),
    )
    .unwrap();
}

async fn higher_fence(store: &PostgresStore, value: &Fixture) -> RunFence {
    store.release_lease(&value.fence).await.unwrap();
    let next = store
        .claim_lease(
            value.fence.tenant_id(),
            value.fence.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(next.epoch() > value.fence.epoch());
    next
}

fn assert_only_structural_bundle(
    before: &Snapshot,
    mut after: Snapshot,
    run: RunId,
    result: &NodeAttemptCommitOutcome,
) {
    let event = result.event().event_id().to_string();
    let attempt = result.attempt().start().attempt_id().to_string();
    for (table, field, id) in [
        ("run_events", "event_id", &event),
        ("run_attempt_claims", "attempt_id", &attempt),
        ("node_attempts", "attempt_id", &attempt),
        ("graph_frame_caller_bindings", "caller_attempt_id", &attempt),
    ] {
        let rows = after.get_mut(table).unwrap().as_array_mut().unwrap();
        let count = rows.len();
        rows.retain(|row| row[field].as_str() != Some(id.as_str()));
        assert_eq!(rows.len() + 1, count, "exactly one structural {table} row");
    }
    let id = run.to_string();
    let original = before["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["run_id"].as_str() == Some(id.as_str()))
        .unwrap();
    let current = after
        .get_mut("runs")
        .unwrap()
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|row| row["run_id"].as_str() == Some(id.as_str()))
        .unwrap();
    for field in [
        "journal_sequence",
        "journal_event_id",
        "journal_recorded_at",
        "journal_digest",
        "updated_at",
    ] {
        current[field] = original[field].clone();
    }
    for rows in after.values_mut() {
        rows.as_array_mut()
            .unwrap()
            .sort_by_key(|row| serde_json::to_string(row).unwrap());
    }
    assert_eq!(&after, before, "no other tenant business fact may change");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pending_child_allows_only_capacity_checked_same_frame_caller_takeover() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = Box::pin(started(&store, "pending-caller-capacity-positive")).await;
    child_with_retries(&mut value, 0);
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let child = spawned.record().child().clone();
    assert!(!child.run().lifecycle().status().is_terminal());
    let next = higher_fence(&store, &value).await;
    let observed = spawned.record().spawn().head();
    let usage = complete_usage(&store, &value).await;
    let db = super::super::frame_barriers::pool().await;
    let before = snapshot(&db, next.tenant_id()).await;
    let account = store
        .load_child_budget_account(next.tenant_id(), next.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert!(account.children()[0].settlement().is_none());

    let wrong = GraphFrameIdentity::new(
        value.frame.frame().origin().clone(),
        value.frame.frame().slot().clone(),
        value.intent.child_graph().reference(),
    )
    .unwrap();
    assert_eq!(wrong.namespace(), value.frame.frame().namespace());
    assert_ne!(wrong.digest(), value.frame.frame().digest());
    let rejected = Box::pin(store.rebind_graph_frame_caller(
        &wrong,
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        observed.clone(),
        usage.clone(),
    ))
    .await;
    assert!(matches!(rejected, Err(StoreError::GraphFrameConflict)));
    let wrong_fence = RunFence::new(
        next.tenant_id().clone(),
        next.run_id(),
        AttemptId::generate(),
        next.epoch(),
    );
    let rejected = Box::pin(store.rebind_graph_frame_caller(
        value.frame.frame(),
        AttemptId::generate(),
        EventId::generate(),
        wrong_fence,
        observed.clone(),
        usage.clone(),
    ))
    .await;
    assert!(matches!(rejected, Err(StoreError::StaleFence)));
    assert_eq!(snapshot(&db, next.tenant_id()).await, before);

    let result = Box::pin(store.rebind_graph_frame_caller(
        value.frame.frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        observed,
        usage,
    ))
    .await
    .unwrap();
    assert!(matches!(result, NodeAttemptCommitOutcome::Committed { .. }));
    assert_eq!(
        result.attempt().start().activation(),
        value.frame.frame().origin()
    );
    assert_eq!(result.attempt().start().fence(), &next);
    let after = snapshot(&db, next.tenant_id()).await;
    assert_only_structural_bundle(&before, after.clone(), next.run_id(), &result);
    let retained = store.load_child_run(value.intent.key()).await.unwrap();
    assert_eq!(retained.spawn(), spawned.record().spawn());
    assert_eq!(retained.child().admission(), child.admission());
    assert_eq!(retained.child().event(), child.event());
    assert_eq!(retained.child().checkpoint(), child.checkpoint());
    assert_eq!(
        serde_json_canonicalizer::to_vec(retained.child().run().lifecycle()).unwrap(),
        serde_json_canonicalizer::to_vec(child.run().lifecycle()).unwrap(),
    );
    assert_eq!(
        store
            .load_child_budget_account(next.tenant_id(), next.run_id())
            .await
            .unwrap()
            .as_ref(),
        Some(&account)
    );
    let active = store
        .load_active_graph_frame(next.tenant_id(), next.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint(), &value.frame);
    assert_eq!(active.caller(), result.attempt().start());
    assert!(
        store
            .load_child_join(value.intent.key().parent())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_run(next.tenant_id(), next.run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(&result.event().head())
    );
    let retry = Box::pin(store.rebind_graph_frame_caller(
        value.frame.frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        value.head.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.event(), result.event());
    assert_eq!(retry.attempt().start(), result.attempt().start());
    assert_eq!(snapshot(&db, next.tenant_id()).await, after);
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn pending_reservations_and_unpriced_or_overrun_usage_block_caller_atomically() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "pending-caller-capacity-negative")).await;
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let next = higher_fence(&store, &value).await;
    let usage = complete_usage(&store, &value).await;
    let account = store
        .load_child_budget_account(next.tenant_id(), next.run_id())
        .await
        .unwrap()
        .unwrap();
    let budget = value.admission.admission().intent().budget();
    let retry_limit = budget.retries().get();
    assert!(retry_limit > 0);
    assert_eq!(
        account.children()[0].reservation().amount().retries().get(),
        retry_limit
    );
    assert!(account.children()[0].settlement().is_none());
    let now = store.observe_database_clock().await.unwrap();
    let next_direct = usage
        .checked_accumulate(
            &BudgetUsage::builder()
                .retries(ExecutionCount::new(1))
                .build()
                .unwrap(),
        )
        .unwrap();
    assert!(budget.remaining(&next_direct, now).is_ok());
    assert!(
        CumulativeBudgetReservation::check_capacity(
            budget,
            &next_direct,
            std::slice::from_ref(account.children()[0].reservation()),
            now,
        )
        .is_err()
    );
    let overrun = usage
        .checked_accumulate(
            &BudgetUsage::builder()
                .retries(ExecutionCount::new(retry_limit + 1))
                .build()
                .unwrap(),
        )
        .unwrap();
    let unpriced = usage
        .checked_accumulate(
            &BudgetUsage::builder()
                .unpriced_cost_events(ExecutionCount::new(1))
                .build()
                .unwrap(),
        )
        .unwrap();
    let db = super::super::frame_barriers::pool().await;
    let before = snapshot(&db, next.tenant_id()).await;
    for direct in [usage, overrun, unpriced] {
        let result = Box::pin(store.rebind_graph_frame_caller(
            value.frame.frame(),
            AttemptId::generate(),
            EventId::generate(),
            next.clone(),
            spawned.record().spawn().head(),
            direct,
        ))
        .await;
        assert!(
            matches!(result, Err(StoreError::GraphFrameLimitExceeded)),
            "{result:?}"
        );
        assert_eq!(snapshot(&db, next.tenant_id()).await, before);
    }
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn pending_child_still_blocks_ordinary_barrier_and_return_without_writes() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "pending-caller-no-barrier-bypass")).await;
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let intent = PendingNodeResultIntent::new(
        value.node.activation().clone(),
        NodeStateChange::Unchanged,
        super::super::frame_barriers::terminal(&value.graph),
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let completion = Box::pin(store.succeed_node_attempt(
        worker_append(
            value.fence.tenant_id().clone(),
            value.fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(spawned.record().spawn().head()),
            value.fence.clone(),
            42_501,
        ),
        &value.node,
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    let pending = store
        .load_pending_node_result(intent.activation())
        .await
        .unwrap();
    let plan = super::super::frame_barriers::plan(&value.graph, &value.frame, &pending);
    let direct = complete_usage(&store, &value).await;
    let reducer = IntegrationGraphReducer {
        reference: value.graph.reducer().clone(),
    };
    let db = super::super::frame_barriers::pool().await;
    let before = snapshot(&db, value.fence.tenant_id()).await;
    let blocked = Box::pin(store.commit_graph_frame_barrier(
        plan.clone(),
        EventId::generate(),
        value.fence.clone(),
        completion.event().head(),
        direct.clone(),
        &AcceptGraphSchemas,
        &reducer,
    ))
    .await;
    assert!(
        matches!(blocked, Err(StoreError::UnsettledChildRuns)),
        "{blocked:?}"
    );
    assert_eq!(snapshot(&db, value.fence.tenant_id()).await, before);
    // No terminal barrier was committed. A pure terminal plan is not a receipt
    // and cannot jump straight to Return while owned child work is pending.
    let blocked = Box::pin(store.return_graph_frame(
        plan,
        EventId::generate(),
        value.fence.clone(),
        completion.event().head(),
        direct,
        &AcceptGraphSchemas,
        &reducer,
    ))
    .await;
    assert!(
        matches!(blocked, Err(StoreError::GraphFrameConflict)),
        "{blocked:?}"
    );
    assert_eq!(snapshot(&db, value.fence.tenant_id()).await, before);
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn settled_overrun_and_unpriced_children_keep_their_budget_obligations() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = super::super::frame_barriers::pool().await;
    for unpriced in [false, true] {
        let value = Box::pin(started(
            &store,
            if unpriced {
                "pending-caller-unpriced-child"
            } else {
                "pending-caller-settled-overrun"
            },
        ))
        .await;
        let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
        let child = spawned.record().child();
        let provenance = child.admission().intent().provenance();
        let current = store
            .load_run(provenance.tenant_id(), provenance.run_id())
            .await
            .unwrap();
        let event = EventId::generate();
        let at = store.observe_database_clock().await.unwrap();
        // Preserve the complete actual zero-I/O admission floor. Known overrun
        // remains exact expenditure; unknown price cannot release a reservation.
        let extra = BudgetUsage::builder()
            .input_bytes(ByteCount::new(if unpriced {
                serde_json_canonicalizer::to_vec(child.admission().intent().request().input())
                    .unwrap()
                    .len() as u64
            } else {
                value
                    .admission
                    .admission()
                    .intent()
                    .budget()
                    .input_bytes()
                    .get()
                    + 1
            }))
            .unpriced_cost_events(ExecutionCount::new(u64::from(unpriced)))
            .build()
            .unwrap();
        let usage = initial_usage(child).checked_accumulate(&extra).unwrap();
        let failure =
            RunFailure::new(terminal_run_failure(event, at).failure().clone(), at, usage).unwrap();
        store
            .append_control_plane(
                JournalAppend::new(
                    JournalExpectation::exact(current.journal_head().unwrap().clone()),
                    JournalEventIntent::control_plane(
                        provenance.tenant_id().clone(),
                        provenance.run_id(),
                        event,
                        payload("test-failed"),
                    )
                    .unwrap(),
                )
                .unwrap(),
                RunProjection::transition(
                    current.lifecycle().revision(),
                    RunTransition::Fail { failure },
                ),
            )
            .await
            .unwrap();
        let parent = store
            .load_run(value.fence.tenant_id(), value.fence.run_id())
            .await
            .unwrap();
        let settlement = Box::pin(
            store.settle_child_run(
                value.intent.key(),
                JournalAppend::new(
                    JournalExpectation::exact(parent.journal_head().unwrap().clone()),
                    JournalEventIntent::control_plane(
                        value.fence.tenant_id().clone(),
                        value.fence.run_id(),
                        EventId::generate(),
                        payload("child-run-settled"),
                    )
                    .unwrap(),
                )
                .unwrap(),
            ),
        )
        .await;
        if unpriced {
            assert!(
                matches!(settlement, Err(StoreError::ChildRunSettlementUnavailable)),
                "{settlement:?}"
            );
        } else {
            assert!(
                matches!(
                    settlement,
                    Ok(stateknot_store_postgres::ChildRunSettlementOutcome::Committed { .. })
                ),
                "{settlement:?}"
            );
        }
        let next = higher_fence(&store, &value).await;
        let account = store
            .load_child_budget_account(next.tenant_id(), next.run_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(account.children()[0].settlement().is_none(), unpriced);
        let before = snapshot(&db, next.tenant_id()).await;
        let parent = store
            .load_run(next.tenant_id(), next.run_id())
            .await
            .unwrap();
        let result = Box::pin(store.rebind_graph_frame_caller(
            value.frame.frame(),
            AttemptId::generate(),
            EventId::generate(),
            next.clone(),
            parent.journal_head().unwrap().clone(),
            complete_usage(&store, &value).await,
        ))
        .await;
        assert!(
            matches!(result, Err(StoreError::GraphFrameLimitExceeded)),
            "{result:?}"
        );
        assert_eq!(snapshot(&db, next.tenant_id()).await, before);
        assert_eq!(
            store
                .load_child_budget_account(next.tenant_id(), next.run_id())
                .await
                .unwrap()
                .as_ref(),
            Some(&account),
        );
    }
    db.close().await;
    store.close().await;
}

#[path = "frame_child_takeover_concurrency.rs"]
mod concurrency;
