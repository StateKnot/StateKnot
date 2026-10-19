// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! A Run cancellation retains the exact scoped child owner and original receipt.
use super::*;
use stateknot_store_postgres::{
    ChildCancellationDelivery, ChildCancellationOutcome, ChildCancellationReceipt,
};

async fn delivery_candidate(
    store: &PostgresStore,
    value: &Fixture,
    head: &JournalHead,
) -> (JournalAppend, RunCancellationRequest) {
    let event = EventId::generate();
    let request = cancellation_request(store.observe_database_clock().await.unwrap());
    let request = RunCancellationRequest::new(
        request.failure().clone().with_caused_by_event(event),
        request.requested_at(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(head.clone()),
        JournalEventIntent::control_plane(
            value.intent.key().tenant_id().clone(),
            value.intent.child().provenance().run_id(),
            event,
            payload("child-run-cancellation-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    (append, request)
}

async fn cancel_parent(store: &PostgresStore, value: &Fixture) -> JournalHead {
    let key = value.intent.key();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    store
        .append_control_plane(
            join::control_append(store, value, "test-cancellation-requested").await,
            RunProjection::transition(
                parent.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(store.observe_database_clock().await.unwrap()),
                },
            ),
        )
        .await
        .unwrap()
        .event()
        .head()
}

async fn terminate_child(
    store: &PostgresStore,
    value: &Fixture,
    admission: &StoredAgentAdmission,
    receipt: &ChildCancellationReceipt,
) -> JournalHead {
    let key = value.intent.key();
    let child = value.intent.child().provenance().run_id();
    let journal = store
        .load_journal_page(
            key.tenant_id(),
            child,
            None,
            JournalPageSize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    assert_eq!(journal.events().len(), 2);
    let bytes: usize = journal
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum();
    // This child has executed no nodes or providers. The initial checkpoint,
    // actual two journal events and request are its complete prior usage.
    let usage = BudgetUsage::builder()
        .checkpoint_bytes(initial_usage(admission).checkpoint_bytes())
        .event_bytes(ByteCount::new(bytes as u64))
        .input_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(value.intent.child().request().input())
                .unwrap()
                .len() as u64,
        ))
        .build()
        .unwrap();
    let terminal = store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(receipt.head().clone()),
                JournalEventIntent::control_plane(
                    key.tenant_id().clone(),
                    child,
                    EventId::generate(),
                    payload("test-child-cancellation-confirmed"),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                receipt.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: store.observe_database_clock().await.unwrap(),
                    usage,
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .load_run(key.tenant_id(), child)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Cancelled
    );
    terminal.event().head()
}

async fn assert_parent_owner(store: &PostgresStore, value: &Fixture, head: &JournalHead) {
    let key = value.intent.key();
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(head)
    );
    assert_eq!(
        store
            .load_active_graph_frame(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap()
            .checkpoint(),
        &value.frame
    );
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        *value.admission.checkpoint()
    );
}

#[tokio::test]
async fn scoped_child_cancellation_recovers_the_original_receipt_after_child_termination() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-child-cancellation")).await;
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let key = value.intent.key();
    let child = value.intent.child().provenance().run_id();
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let cancellation = Box::pin(cancel_parent(&store, &value)).await;
    assert_eq!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap(),
        [key.clone()]
    );
    assert!(
        store
            .pending_child_cancellations_after(&tenant("scoped-cancellation-other-tenant"), None)
            .await
            .unwrap()
            .is_empty()
    );
    let queued = Box::pin(store.load_child_cancellation(key)).await.unwrap();
    assert_eq!(queued.key(), key);
    assert_eq!(queued.child_run_id(), child);
    assert_eq!(queued.parent_head(), &cancellation);
    assert!(queued.receipt().is_none());
    let original_head = spawned.record().child().run().journal_head().unwrap();
    let (append, request) = delivery_candidate(&store, &value, original_head).await;
    let delivered = Box::pin(store.deliver_child_cancellation(key, append, request))
        .await
        .unwrap();
    assert!(matches!(delivered, ChildCancellationDelivery::Committed(_)));
    let receipt = delivered.record().receipt().unwrap();
    assert_eq!(receipt.outcome(), ChildCancellationOutcome::Requested);
    assert_eq!(
        receipt.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(receipt.head().run_id(), child);
    assert_eq!(receipt.head().tenant_id(), key.tenant_id());
    assert!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    let terminal = Box::pin(terminate_child(
        &store,
        &value,
        spawned.record().child(),
        receipt,
    ))
    .await;
    let (retry_append, retry_request) = delivery_candidate(&store, &value, original_head).await;
    let recovered = Box::pin(store.deliver_child_cancellation(key, retry_append, retry_request))
        .await
        .unwrap();
    assert!(matches!(
        recovered,
        ChildCancellationDelivery::Idempotent(_)
    ));
    let recovered_receipt = recovered.record().receipt().unwrap();
    assert_eq!(recovered_receipt.head(), receipt.head());
    assert_eq!(
        serde_json_canonicalizer::to_vec(recovered_receipt.lifecycle()).unwrap(),
        serde_json_canonicalizer::to_vec(receipt.lifecycle()).unwrap()
    );
    assert_eq!(recovered_receipt.delivered_at(), receipt.delivered_at());
    assert_eq!(recovered_receipt.outcome(), receipt.outcome());
    let retained = Box::pin(store.load_child_cancellation(key)).await.unwrap();
    assert_eq!(retained.receipt().unwrap().head(), receipt.head());
    assert_eq!(
        store
            .load_run(key.tenant_id(), child)
            .await
            .unwrap()
            .journal_head(),
        Some(&terminal)
    );
    Box::pin(assert_parent_owner(&store, &value, &cancellation)).await;
    assert_eq!(
        store
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap(),
        account
    );
    assert_eq!(
        Box::pin(store.load_child_run(key)).await.unwrap().spawn(),
        spawned.record().spawn()
    );
    store.close().await;
}

async fn drain_registered_join_child(
    store: &PostgresStore,
    value: &Fixture,
    child: &StoredAgentAdmission,
) -> BudgetUsage {
    Box::pin(cancel_parent(store, value)).await;
    let (append, request) =
        delivery_candidate(store, value, child.run().journal_head().unwrap()).await;
    let delivered = Box::pin(store.deliver_child_cancellation(value.intent.key(), append, request))
        .await
        .unwrap();
    let receipt = delivered.record().receipt().unwrap();
    assert_eq!(receipt.outcome(), ChildCancellationOutcome::Requested);
    Box::pin(terminate_child(store, value, child, receipt)).await;
    let append = join::control_append(store, value, "child-run-settled").await;
    let settled = Box::pin(store.settle_child_run(value.intent.key(), append))
        .await
        .unwrap();
    let stateknot_store_postgres::ChildRunSettlementOutcome::Committed { record, .. } = settled
    else {
        panic!("fresh cancelled child settlement");
    };
    record.settlement().unwrap().usage().clone()
}

async fn registered_join_cancellation_direct(
    store: &PostgresStore,
    value: &Fixture,
) -> BudgetUsage {
    let key = value.intent.key();
    let journal = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            None,
            JournalPageSize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    // Admission, frame entry, ordinary Start, spawn, Join, cancellation, settlement.
    assert_eq!(journal.events().len(), 7);
    let event_bytes = journal
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum::<usize>();
    // No provider/Tool work or node output occurred. There are exactly two
    // physical starts (framework caller and ordinary owner), with no takeover.
    BudgetUsage::builder()
        .graph_depth(stateknot_core::ExecutionCount::new(2))
        .graph_steps(stateknot_core::ExecutionCount::new(2))
        .concurrent_branches(stateknot_core::ExecutionCount::new(1))
        .fan_out(stateknot_core::ExecutionCount::new(1))
        .event_bytes(ByteCount::new(event_bytes as u64))
        .checkpoint_bytes(value.direct.checkpoint_bytes())
        .input_bytes(value.direct.input_bytes())
        .build()
        .unwrap()
}

async fn registered_join_tenant_facts(
    db: &PgPool,
    tenant: &TenantId,
) -> std::collections::BTreeMap<String, String> {
    let tables = query_scalar::<_, String>(
        "SELECT quote_ident(c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='stateknot' AND c.relkind IN ('r','p') AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped) ORDER BY c.relname",
    )
    .fetch_all(db)
    .await
    .unwrap();
    let mut facts = std::collections::BTreeMap::new();
    for table in tables {
        let rows = query_scalar::<_, String>(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM stateknot.{table} t WHERE tenant_id=$1"
        ))
        .bind(tenant.as_str())
        .fetch_one(db)
        .await
        .unwrap();
        facts.insert(table, rows);
    }
    facts
}

async fn assert_stopped_registered_join_owner(
    store: &PostgresStore,
    value: &Fixture,
    registered: &stateknot_store_postgres::ChildJoinRecord,
    closed: &stateknot_store_postgres::StoredGraphFrameClosure,
) {
    let key = value.intent.key();
    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), value.node);
    assert!(history.records()[0].completion().is_none());
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    let join = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(join.request(), registered.request());
    assert_eq!(join.registration(), registered.registration());
    assert!(join.binding().is_none());
    assert!(join.head().is_none());
    assert!(join.consumed().is_none());
    assert_eq!(closed.completions().len(), 1);
    assert_ne!(closed.completions()[0].start(), &value.node);
    // None is the public owning reader's verified Root stack result.
    assert!(
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .is_none()
    );
    let loaded = Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.digest(), closed.digest());
    assert_eq!(loaded.event(), closed.event());
    assert_eq!(loaded.direct_usage(), closed.direct_usage());
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        *value.admission.checkpoint()
    );
    let owner = Box::pin(store.load_child_run(key)).await.unwrap();
    assert_eq!(owner.intent(), &value.intent);
    assert!(owner.settlement().is_some());
}

fn assert_registered_join_original_facts(
    before: &std::collections::BTreeMap<String, String>,
    after: &std::collections::BTreeMap<String, String>,
) {
    for table in [
        "agent_admissions",
        "run_checkpoints",
        "run_attempt_claims",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "child_run_budget_accounts",
        "child_run_ownership",
        "child_run_terminals",
        "child_run_settlements",
        "child_run_joins",
        "child_run_join_bindings",
        "child_run_join_consumptions",
        "child_run_cancellations",
        "child_run_cancellation_receipts",
    ] {
        assert!(before.contains_key(table));
        assert_eq!(
            after.get(table),
            before.get(table),
            "retained tenant facts: {table}"
        );
    }
}

#[tokio::test]
async fn registered_scoped_join_cancellation_closes_frames_and_retains_its_executing_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-join-cancellation-close")).await;
    let key = value.intent.key();
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let request = stateknot_core::ChildRunJoinRequest::new([key.clone()]).unwrap();
    let registered = Box::pin(store.register_child_join(
        request,
        &value.node,
        join_append(&value, &spawned.record().spawn().head()),
    ))
    .await
    .unwrap();
    let child_usage = Box::pin(drain_registered_join_child(
        &store,
        &value,
        spawned.record().child(),
    ))
    .await;
    let direct = Box::pin(registered_join_cancellation_direct(&store, &value)).await;
    assert_ne!(direct, BudgetUsage::zero());
    assert_eq!(
        direct.unpriced_cost_events(),
        stateknot_core::ExecutionCount::ZERO
    );
    let expected_total = direct
        .checked_accumulate(&child_usage.cumulative_only())
        .unwrap();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        parent.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(parent.lease().is_none());
    let db = super::super::frame_barriers::pool().await;
    let before = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let closed = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        parent.journal_head().unwrap().clone(),
        parent.lifecycle().revision(),
        direct.clone(),
    ))
    .await;
    if closed.is_err() {
        assert_eq!(
            registered_join_tenant_facts(&db, key.tenant_id()).await,
            before
        );
    }
    // The e372184 baseline returned InvalidRunFailureClose at this transaction.
    // Retain the positive contract that exposed that original refusal.
    let closed =
        closed.expect("a cancelled registered Join with settled children can close frames");
    assert!(matches!(
        closed,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Committed(_)
    ));
    assert_eq!(closed.record().direct_usage(), &direct);
    Box::pin(assert_stopped_registered_join_owner(
        &store,
        &value,
        registered.record(),
        closed.record(),
    ))
    .await;
    let after = registered_join_tenant_facts(&db, key.tenant_id()).await;
    assert_registered_join_original_facts(&before, &after);
    assert_eq!(
        Box::pin(store.include_child_usage(key.tenant_id(), key.parent_run_id(), direct))
            .await
            .unwrap(),
        expected_total
    );
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn registered_scoped_join_close_recovers_original_ack_without_reviving_worker_authority() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started_with_sibling(
        &store,
        "scoped-join-close-ack-precedence",
        true,
    ))
    .await;
    let key = value.intent.key();
    let sibling = NodeId::new("sibling").unwrap();
    // Preserve a real, originally dispatchable ordinary sibling. No start is
    // fabricated for it, and the registered Join's owner remains the original F1.
    let dispatch = ReadyNodeRecoveryPlanner::for_frame(value.frame.clone(), value.fence.clone())
        .unwrap()
        .finish(
            value.head.clone(),
            store.observe_database_clock().await.unwrap(),
        )
        .unwrap();
    assert!(
        dispatch
            .nodes()
            .iter()
            .find(|node| node.activation().node_id() == &sibling)
            .unwrap()
            .dispatch_reason()
            .is_some()
    );
    assert_eq!(value.node.fence(), &value.fence);
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let registered = Box::pin(store.register_child_join(
        stateknot_core::ChildRunJoinRequest::new([key.clone()]).unwrap(),
        &value.node,
        join_append(&value, &spawned.record().spawn().head()),
    ))
    .await
    .unwrap();
    let _child_usage = Box::pin(drain_registered_join_child(
        &store,
        &value,
        spawned.record().child(),
    ))
    .await;
    let direct = Box::pin(registered_join_cancellation_direct(&store, &value)).await;
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        parent.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(parent.lease().is_none());
    let closed = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        parent.journal_head().unwrap().clone(),
        parent.lifecycle().revision(),
        direct,
    ))
    .await
    .unwrap();
    assert!(matches!(
        closed,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let original = closed.record();
    Box::pin(assert_stopped_registered_join_owner(
        &store,
        &value,
        registered.record(),
        original,
    ))
    .await;
    let db = super::super::frame_barriers::pool().await;
    let retained = registered_join_tenant_facts(&db, key.tenant_id()).await;

    let retry_event = EventId::generate();
    let stale_revision = value.admission.run().lifecycle().revision();
    let different_direct = BudgetUsage::zero();
    assert_ne!(retry_event, original.event().event_id());
    assert_ne!(&value.head, &original.event().head());
    assert_ne!(stale_revision, parent.lifecycle().revision());
    assert_ne!(&different_direct, original.direct_usage());
    // Authentication of the committed v2 close precedes every fresh candidate
    // field. A new event and stale observations must recover the original ACK.
    let recovered = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        retry_event,
        value.head.clone(),
        stale_revision,
        different_direct,
    ))
    .await
    .unwrap();
    assert!(matches!(
        recovered,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Existing(_)
    ));
    assert_eq!(recovered.record().digest(), original.digest());
    assert_eq!(recovered.record().event(), original.event());
    assert_eq!(recovered.record().direct_usage(), original.direct_usage());
    assert_eq!(
        serde_json_canonicalizer::to_vec(recovered.record().failure()).unwrap(),
        serde_json_canonicalizer::to_vec(original.failure()).unwrap()
    );
    assert_eq!(
        serde_json_canonicalizer::to_vec(&recovered.record().completions()).unwrap(),
        serde_json_canonicalizer::to_vec(&original.completions()).unwrap()
    );
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        retained,
        "duplicate close ACK must create no tenant fact"
    );

    let closed_head = original.event().head();
    assert!(matches!(
        Box::pin(store.append_worker(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                JournalExpectation::exact(closed_head.clone()),
                value.fence.clone(),
                42_100,
            ),
            RunProjection::Unchanged,
        ))
        .await,
        Err(StoreError::NoActiveLease)
    ));
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        retained,
        "released F1 must not append a fresh worker event"
    );

    // A fresh event/attempt for the actual ready sibling cannot reuse F1.
    // Lifecycle rejection precedes the closed-frame and missing-lease checks.
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                JournalExpectation::exact(closed_head.clone()),
                value.fence.clone(),
                42_101,
            ),
            &dispatch,
            &sibling,
            AttemptId::generate(),
        ))
        .await,
        Err(StoreError::RunNotRunnable)
    ));
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        retained,
        "a rejected fresh sibling Start must preserve every tenant fact"
    );

    let completion_event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("node.scoped_join_after_close").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The stopped scoped Join owner cannot complete after closure.")
            .unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(completion_event);
    // This is a valid proposed public failure intent, not a fabricated durable
    // completion. The original scoped frame is closed before fence authority.
    assert!(matches!(
        Box::pin(store.fail_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                completion_event,
                JournalExpectation::exact(closed_head),
                value.fence.clone(),
                42_102,
            ),
            &value.node,
            failure,
            BudgetUsage::zero(),
        ))
        .await,
        Err(StoreError::GraphFrameConflict)
    ));
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        retained,
        "a rejected ordinary completion must preserve every tenant fact"
    );
    Box::pin(assert_stopped_registered_join_owner(
        &store,
        &value,
        registered.record(),
        original,
    ))
    .await;
    let after = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert!(after.lease().is_none());
    assert_eq!(
        serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
        serde_json_canonicalizer::to_vec(parent.lifecycle()).unwrap()
    );
    assert_eq!(after.journal_head(), Some(&original.event().head()));
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        retained
    );
    db.close().await;
    store.close().await;
}

async fn registered_join_cancellation_direct_before_settlement(
    store: &PostgresStore,
    value: &Fixture,
) -> BudgetUsage {
    let key = value.intent.key();
    let journal = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            None,
            JournalPageSize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    // Admission, frame entry, ordinary Start, spawn, Join and cancellation.
    // Child cancellation/termination do not append to this parent journal.
    assert_eq!(journal.events().len(), 6);
    let event_bytes = journal
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum::<usize>();
    // Complete parent DIRECT: two physical starts and no provider/node output.
    // Child work is excluded; this does not substitute the structural floor.
    BudgetUsage::builder()
        .graph_depth(stateknot_core::ExecutionCount::new(2))
        .graph_steps(stateknot_core::ExecutionCount::new(2))
        .concurrent_branches(stateknot_core::ExecutionCount::new(1))
        .fan_out(stateknot_core::ExecutionCount::new(1))
        .event_bytes(ByteCount::new(event_bytes as u64))
        .checkpoint_bytes(value.direct.checkpoint_bytes())
        .input_bytes(value.direct.input_bytes())
        .build()
        .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn registered_scoped_join_close_requires_child_settlement_before_stopping_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-join-close-needs-settlement")).await;
    let key = value.intent.key();
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let child = spawned.record().child();
    let registered = Box::pin(store.register_child_join(
        stateknot_core::ChildRunJoinRequest::new([key.clone()]).unwrap(),
        &value.node,
        join_append(&value, &spawned.record().spawn().head()),
    ))
    .await
    .unwrap();
    let observed = Box::pin(cancel_parent(&store, &value)).await;
    assert_eq!(observed.sequence().get(), 6);
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(parent.journal_head(), Some(&observed));
    assert_eq!(
        parent.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(parent.lease().is_none());
    assert_eq!(
        store
            .load_run(key.tenant_id(), value.intent.child().provenance().run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    let live_direct = Box::pin(registered_join_cancellation_direct_before_settlement(
        &store, &value,
    ))
    .await;
    assert_ne!(live_direct, BudgetUsage::zero());
    let db = super::super::frame_barriers::pool().await;
    let live_facts = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let physical_starts: serde_json::Value =
        serde_json::from_str(live_facts.get("node_attempts").unwrap()).unwrap();
    assert_eq!(physical_starts.as_array().unwrap().len(), 2);
    let refused = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        observed.clone(),
        parent.lifecycle().revision(),
        live_direct.clone(),
    ))
    .await;
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        live_facts,
        "a live child refusal must preserve every tenant fact"
    );
    assert!(matches!(
        refused,
        Err(StoreError::ChildRunSettlementUnavailable)
    ));
    Box::pin(assert_parent_owner(&store, &value, &observed)).await;

    let (append, request) =
        delivery_candidate(&store, &value, child.run().journal_head().unwrap()).await;
    let delivered = Box::pin(store.deliver_child_cancellation(key, append, request))
        .await
        .unwrap();
    let receipt = delivered.record().receipt().unwrap();
    assert_eq!(receipt.outcome(), ChildCancellationOutcome::Requested);
    Box::pin(terminate_child(&store, &value, child, receipt)).await;
    let terminal_child = store
        .load_run(key.tenant_id(), value.intent.child().provenance().run_id())
        .await
        .unwrap();
    assert_eq!(terminal_child.lifecycle().status(), RunStatus::Cancelled);
    assert!(
        Box::pin(store.load_child_run(key))
            .await
            .unwrap()
            .settlement()
            .is_none()
    );
    let parent_after_terminal = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(parent_after_terminal.journal_head(), Some(&observed));
    assert_eq!(
        parent_after_terminal.lifecycle().revision(),
        parent.lifecycle().revision()
    );
    let unsettled_direct = Box::pin(registered_join_cancellation_direct_before_settlement(
        &store, &value,
    ))
    .await;
    assert_eq!(unsettled_direct, live_direct);
    let terminal_facts = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let refused = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        observed.clone(),
        parent_after_terminal.lifecycle().revision(),
        unsettled_direct,
    ))
    .await;
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        terminal_facts,
        "a terminal-but-unsettled child refusal must preserve every tenant fact"
    );
    assert!(matches!(
        refused,
        Err(StoreError::ChildRunSettlementUnavailable)
    ));
    Box::pin(assert_parent_owner(&store, &value, &observed)).await;

    let append = join::control_append(&store, &value, "child-run-settled").await;
    let settled = Box::pin(store.settle_child_run(key, append)).await.unwrap();
    let stateknot_store_postgres::ChildRunSettlementOutcome::Committed { event, record } = settled
    else {
        panic!("fresh terminal child settlement");
    };
    assert_eq!(event.sequence().get(), 7);
    let child_usage = record.settlement().unwrap().usage().clone();
    assert_eq!(
        terminal_child.lifecycle().terminal_usage(),
        Some(&child_usage)
    );
    // Retain the original final helper's exact seven-event assertion.
    let direct = Box::pin(registered_join_cancellation_direct(&store, &value)).await;
    direct.validate_monotonic_after(&live_direct).unwrap();
    let expected_total = direct
        .checked_accumulate(&child_usage.cumulative_only())
        .unwrap();
    let account = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert_eq!(
        account.delegated_usage().unwrap(),
        child_usage.cumulative_only()
    );
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(parent.journal_head(), Some(&event.head()));
    let before = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let closed = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        parent.journal_head().unwrap().clone(),
        parent.lifecycle().revision(),
        direct.clone(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        closed,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Committed(_)
    ));
    assert_eq!(closed.record().direct_usage(), &direct);
    Box::pin(assert_stopped_registered_join_owner(
        &store,
        &value,
        registered.record(),
        closed.record(),
    ))
    .await;
    let after = registered_join_tenant_facts(&db, key.tenant_id()).await;
    assert_registered_join_original_facts(&before, &after);
    assert_eq!(
        Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap(),
        Some(account)
    );
    assert_eq!(
        Box::pin(store.include_child_usage(key.tenant_id(), key.parent_run_id(), direct))
            .await
            .unwrap(),
        expected_total
    );
    db.close().await;
    store.close().await;
}
