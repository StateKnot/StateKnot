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

async fn published_join_fixture(
    store: &PostgresStore,
    name: &str,
) -> (Fixture, stateknot_store_postgres::ChildJoinRecord) {
    let value = Box::pin(started(store, name)).await;
    let spawned = Box::pin(spawn(store, &value)).await.unwrap();
    let request = stateknot_core::ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
    Box::pin(store.register_child_join(
        request.clone(),
        &value.node,
        join_append(&value, &spawned.record().spawn().head()),
    ))
    .await
    .unwrap();
    Box::pin(join::settle(store, &value, spawned.record().child())).await;
    let parent = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    assert!(parent.lease().is_none());
    let append = join::control_append(store, &value, "child-join-published").await;
    let published = Box::pin(store.publish_child_join(&request, append))
        .await
        .unwrap();
    assert!(matches!(
        published,
        stateknot_store_postgres::ChildJoinCommitOutcome::Committed(_)
    ));
    assert!(published.record().binding().is_some());
    assert!(published.record().head().is_some());
    assert!(published.record().consumed().is_none());
    let owner = Box::pin(store.load_child_run(value.intent.key()))
        .await
        .unwrap();
    assert!(owner.settlement().is_some());
    assert_eq!(
        owner.settlement().unwrap().usage().unpriced_cost_events(),
        stateknot_core::ExecutionCount::ZERO
    );
    (value, published.record().clone())
}

async fn assert_published_join_owner(
    store: &PostgresStore,
    db: &PgPool,
    value: &Fixture,
    published: &stateknot_store_postgres::ChildJoinRecord,
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
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    let retained = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.request(), published.request());
    assert_eq!(retained.registration(), published.registration());
    assert_eq!(retained.binding(), published.binding());
    assert_eq!(retained.head(), published.head());
    assert!(retained.consumed().is_none());
    let counts: (i64, i64, i64) = query_as(
        "SELECT (SELECT count(*) FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2), (SELECT count(*) FROM stateknot.pending_node_results WHERE tenant_id=$1 AND run_id=$2), (SELECT count(*) FROM stateknot.child_run_join_consumptions WHERE tenant_id=$1 AND parent_run_id=$2)",
    )
    .bind(key.tenant_id().as_str())
    .bind(*key.parent_run_id().as_uuid())
    .fetch_one(db)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0, 0));
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .as_ref(),
        Some(value.admission.checkpoint())
    );
}

fn assert_same_published_close(
    actual: &stateknot_store_postgres::StoredGraphFrameClosure,
    original: &stateknot_store_postgres::StoredGraphFrameClosure,
) {
    assert_eq!(actual.digest(), original.digest());
    assert_eq!(actual.event(), original.event());
    assert_eq!(actual.root_checkpoint(), original.root_checkpoint());
    assert_eq!(actual.direct_usage(), original.direct_usage());
    assert_eq!(
        serde_json_canonicalizer::to_vec(actual.failure()).unwrap(),
        serde_json_canonicalizer::to_vec(original.failure()).unwrap()
    );
    assert_eq!(
        serde_json_canonicalizer::to_vec(&actual.completions()).unwrap(),
        serde_json_canonicalizer::to_vec(&original.completions()).unwrap()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn published_scoped_join_cancellation_preserves_the_whole_owner_and_original_ack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, published) = Box::pin(published_join_fixture(
        &store,
        "published-join-cancellation-ack",
    ))
    .await;
    let key = value.intent.key();
    let account = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    let child = Box::pin(store.load_child_run(key)).await.unwrap();
    let child_usage = child.settlement().unwrap().usage().cumulative_only();
    assert_ne!(child_usage, BudgetUsage::zero());
    assert_eq!(account.delegated_usage().unwrap(), child_usage);
    let requested_head = Box::pin(cancel_parent(&store, &value)).await;
    let requested = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        requested.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(requested.journal_head(), Some(&requested_head));
    let direct = Box::pin(join::direct(&store, &value, 2, &BudgetUsage::zero())).await;
    let expected_total = direct.checked_accumulate(&child_usage).unwrap();
    let db = super::super::frame_barriers::pool().await;
    Box::pin(assert_published_join_owner(&store, &db, &value, &published)).await;
    let before = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let closed = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        requested_head,
        requested.lifecycle().revision(),
        direct.clone(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        closed,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let original = closed.record();
    assert_eq!(original.direct_usage(), &direct);
    assert_eq!(original.completions().len(), 1);
    assert_ne!(original.completions()[0].start(), &value.node);
    let (schema, _) = PostgresStore::graph_frame_published_join_stop_event_schema().unwrap();
    assert_eq!(original.event().payload().schema(), &schema);
    assert_eq!(
        original.event().payload().kind().as_str(),
        "graph-frames-closed"
    );
    let (wire, intent_digest): (Vec<u8>, String) = query_as(
        "SELECT closure_bytes, 'sha256:' || encode(intent_digest, 'hex') FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid())
    .fetch_one(&db).await.unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(wire["version"], json!(3));
    assert_eq!(wire["intent"]["version"], json!(3));
    assert_eq!(
        wire["intent"]["stopped_join"],
        json!({
            "owner": value.node,
            "request_digest": published.request().digest(),
            "registration": published.registration().head(),
            "binding": published.binding().unwrap(),
            "publication": published.head().unwrap(),
        })
    );
    let event_payload = serde_json::to_value(original.event().payload()).unwrap();
    assert_eq!(
        event_payload["data"],
        json!({
            "version": 3,
            "intent_digest": intent_digest,
            "frame_count": 1,
        })
    );
    Box::pin(assert_published_join_owner(&store, &db, &value, &published)).await;
    assert!(
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .is_none()
    );
    let after = registered_join_tenant_facts(&db, key.tenant_id()).await;
    assert_registered_join_original_facts(&before, &after);
    assert_eq!(
        Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap(),
        Some(account.clone())
    );
    for _ in 0..2 {
        assert_eq!(
            Box::pin(store.include_child_usage(
                key.tenant_id(),
                key.parent_run_id(),
                direct.clone(),
            ))
            .await
            .unwrap(),
            expected_total
        );
    }
    let retained = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let recovered = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        value.head.clone(),
        value.admission.run().lifecycle().revision(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        recovered,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Existing(_)
    ));
    assert_same_published_close(recovered.record(), original);
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        retained
    );
    assert!(matches!(
        Box::pin(store.append_worker(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                JournalExpectation::exact(original.event().head()),
                value.fence.clone(),
                42_200
            ),
            RunProjection::Unchanged,
        ))
        .await,
        Err(StoreError::NoActiveLease)
    ));
    let dispatch = ReadyNodeRecoveryPlanner::for_frame(value.frame.clone(), value.fence.clone())
        .unwrap()
        .finish(
            value.head.clone(),
            store.observe_database_clock().await.unwrap(),
        )
        .unwrap();
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                JournalExpectation::exact(original.event().head()),
                value.fence.clone(),
                42_201
            ),
            &dispatch,
            key.parent().node_id(),
            AttemptId::generate(),
        ))
        .await,
        Err(StoreError::RunNotRunnable)
    ));
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("node.published_join_after_close").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("A stopped published Join owner cannot complete.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event);
    assert!(matches!(
        Box::pin(store.fail_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                event,
                JournalExpectation::exact(original.event().head()),
                value.fence.clone(),
                42_202
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
        retained
    );
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let confirmed = store
        .append_control_plane(
            join::control_append(&store, &value, "test-cancellation-confirmed").await,
            RunProjection::transition(
                parent.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: store.observe_database_clock().await.unwrap(),
                    usage: expected_total.clone(),
                },
            ),
        )
        .await
        .unwrap();
    let terminal = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(terminal.lifecycle().terminal_usage(), Some(&expected_total));
    let final_head = confirmed.event().head();
    assert_eq!(terminal.journal_head(), Some(&final_head));
    let terminal_account =
        Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(terminal_account.children(), account.children());
    assert_eq!(
        terminal_account.delegated_usage().unwrap(),
        account.delegated_usage().unwrap()
    );
    assert_eq!(terminal_account.direct_head(), &final_head);
    assert_eq!(terminal_account.direct_usage(), &direct);
    assert!(terminal_account.direct_head().sequence() > account.direct_head().sequence());
    store
        .quarantine_run(quarantine_request(
            key.tenant_id().clone(),
            key.parent_run_id(),
            QuarantineId::generate(),
            JournalExpectation::exact(final_head),
            RunQuarantineCause::ProjectionMismatch,
            "test.published_join_history",
            b"explicit retained history quarantine",
        ))
        .await
        .unwrap();
    let url = std::env::var(DATABASE_URL_ENV).unwrap();
    let fresh_store = PostgresStore::connect(&url, store.options().clone())
        .await
        .unwrap();
    let quarantined = fresh_store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert!(quarantined.is_quarantined());
    assert_eq!(quarantined.lifecycle().status(), RunStatus::Cancelled);
    let historical =
        Box::pin(fresh_store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    assert_same_published_close(&historical, original);
    Box::pin(assert_published_join_owner(
        &fresh_store,
        &db,
        &value,
        &published,
    ))
    .await;
    assert_eq!(
        Box::pin(fresh_store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap(),
        Some(terminal_account)
    );
    let quarantined_facts = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let replay = Box::pin(fresh_store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        value.head.clone(),
        value.admission.run().lifecycle().revision(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        replay,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Existing(_)
    ));
    assert_same_published_close(replay.record(), original);
    assert_eq!(
        registered_join_tenant_facts(&db, key.tenant_id()).await,
        quarantined_facts
    );
    fresh_store.close().await;
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn published_join_stop_profile_rejects_quarantine_consumption_and_replacement_without_writes()
{
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = super::super::frame_barriers::pool().await;
    for mode in ["quarantine", "replacement", "consumed"] {
        let (value, published) = Box::pin(published_join_fixture(
            &store,
            &format!("published-join-stop-{mode}"),
        ))
        .await;
        let key = value.intent.key();
        let fence = store
            .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        assert!(fence.epoch() > value.fence.epoch());
        let mut output = BudgetUsage::zero();
        let starts = if mode == "quarantine" {
            2
        } else {
            let successor = Box::pin(join::resume(&store, &value, &fence)).await;
            assert_ne!(successor.start().head(), value.node);
            assert_eq!(successor.start().activation(), key.parent());
            if mode == "consumed" {
                let intent = PendingNodeResultIntent::new(
                    successor.start().activation().clone(),
                    NodeStateChange::Unchanged,
                    super::super::frame_barriers::terminal(&value.graph),
                    NodeInvocationBindings::empty(),
                )
                .unwrap()
                .with_child_join(published.head().unwrap().clone())
                .unwrap();
                output = BudgetUsage::builder()
                    .output_bytes(ByteCount::new(
                        serde_json_canonicalizer::to_vec(&json!({"count":7,"source":"terminal"}))
                            .unwrap()
                            .len() as u64,
                    ))
                    .build()
                    .unwrap();
                let completed = Box::pin(store.succeed_node_attempt(
                    worker_append(
                        key.tenant_id().clone(),
                        key.parent_run_id(),
                        EventId::generate(),
                        JournalExpectation::exact(successor.start().journal_head().clone()),
                        fence.clone(),
                        42_210,
                    ),
                    &successor.start().head(),
                    intent,
                    output.clone(),
                ))
                .await
                .unwrap();
                let consumed = Box::pin(store.load_child_join(key.parent()))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(consumed.head(), published.head());
                assert_eq!(consumed.consumed(), Some(&completed.event().head()));
                let pending = Box::pin(store.load_pending_node_result(key.parent()))
                    .await
                    .unwrap();
                assert_eq!(pending.intent().activation(), key.parent());
                assert_eq!(pending.fence(), &fence);
                assert_eq!(pending.intent().child_join(), published.head());
            }
            3
        };
        let observed = Box::pin(cancel_parent(&store, &value)).await;
        let run = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        assert_eq!(run.lifecycle().status(), RunStatus::CancellationRequested);
        assert_eq!(run.journal_head(), Some(&observed));
        let direct = Box::pin(join::direct(&store, &value, starts, &output)).await;
        let context = CorruptionQuarantineContext::new(
            key.tenant_id().clone(),
            key.parent_run_id(),
            QuarantineId::generate(),
            JournalExpectation::exact(observed.clone()),
            Digest::sha256(b"published Join stop profile observation"),
        )
        .unwrap();
        let session = store
            .begin_claimed_run_recovery(fence.clone(), context)
            .await
            .unwrap();
        if mode == "quarantine" {
            store
                .quarantine_run(quarantine_request(
                    key.tenant_id().clone(),
                    key.parent_run_id(),
                    QuarantineId::generate(),
                    JournalExpectation::exact(observed.clone()),
                    RunQuarantineCause::ProjectionMismatch,
                    "test.published_join_fresh",
                    b"explicit fresh stop quarantine",
                ))
                .await
                .unwrap();
        }
        let before = registered_join_tenant_facts(&db, key.tenant_id()).await;
        let refused =
            Box::pin(session.validate_registered_join_frame_cancellation(&value.frame.head()))
                .await;
        if mode == "quarantine" {
            assert!(matches!(refused, Err(StoreError::RunQuarantined)));
        } else {
            assert!(matches!(refused, Err(StoreError::InvalidRunFailureClose)));
        }
        assert_eq!(
            registered_join_tenant_facts(&db, key.tenant_id()).await,
            before,
            "profile refusal must not write: {mode}"
        );
        if mode != "consumed" {
            let closed = Box::pin(store.close_graph_frames(
                key.tenant_id(),
                key.parent_run_id(),
                EventId::generate(),
                observed,
                run.lifecycle().revision(),
                direct,
            ))
            .await;
            if mode == "quarantine" {
                assert!(matches!(closed, Err(StoreError::RunQuarantined)));
            } else {
                assert!(matches!(closed, Err(StoreError::InvalidRunFailureClose)));
            }
            assert_eq!(
                registered_join_tenant_facts(&db, key.tenant_id()).await,
                before,
                "fresh close refusal must not write: {mode}"
            );
        }
        // A consumed Join with a completed ordinary attempt is not this stop
        // profile. The existing v1 no-unfinished-work closure remains legal;
        // this test deliberately does not reject that separate public path.
        drop(session);
        let after = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        if after.lease().is_some_and(|lease| lease.fence() == &fence) {
            store.release_lease(&fence).await.unwrap();
        }
    }
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn published_join_stop_sql_proof_and_late_faults_roll_back_the_whole_compound() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, _) = Box::pin(published_join_fixture(
        &store,
        "published-join-stop-sql-rollback",
    ))
    .await;
    let key = value.intent.key();
    let observed = Box::pin(cancel_parent(&store, &value)).await;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let direct = Box::pin(join::direct(&store, &value, 2, &BudgetUsage::zero())).await;
    let db = super::super::frame_barriers::pool().await;
    let before = registered_join_tenant_facts(&db, key.tenant_id()).await;
    // This BEFORE fault changes only a well-shaped private proof. Every original
    // production trigger remains enabled. The writer subsequently inserts all
    // caller components, closes the stack, clears the lease and updates the Run
    // head before SET CONSTRAINTS reaches the actual v3 complete-proof guard.
    let mut proof_observations = Vec::new();
    // SKG02 is normally mapped to GraphFrameCompoundRequired. A two-column
    // relay in a dedicated test schema invokes the unchanged real guard and
    // preserves its original code/message/detail through a diagnostic P0001.
    for path in [
        "{intent,stopped_join,publication,binding_digest}",
        "{intent,stopped_join,binding,digest}",
    ] {
        query("CREATE SCHEMA stateknot_test_published_join_stop")
            .execute(&db)
            .await
            .unwrap();
        query("CREATE TABLE stateknot_test_published_join_stop.guard_probe (tenant_id text NOT NULL, run_id uuid NOT NULL)")
            .execute(&db).await.unwrap();
        query("CREATE TRIGGER test_published_join_stop_guard_probe AFTER INSERT ON stateknot_test_published_join_stop.guard_probe FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_closure_complete()")
            .execute(&db).await.unwrap();
        query(r"
CREATE FUNCTION stateknot_test_published_join_stop.observe_original_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE whole stateknot.graph_frame_closures%ROWTYPE; original_code text; original_message text; original_detail text;
BEGIN
 SELECT * INTO whole FROM stateknot.graph_frame_closures WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF (SELECT count(*) FROM stateknot.graph_frame_closed_callers WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id)<>whole.frame_count
  OR NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND active_namespace='' AND active_frame_identity_digest IS NULL)
  OR NOT EXISTS (SELECT 1 FROM stateknot.runs WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND lease_attempt_id IS NULL AND journal_sequence=whole.journal_sequence AND journal_digest=whole.journal_digest)
 THEN RAISE EXCEPTION 'fixture did not reach the complete v3 transaction' USING ERRCODE='P0001'; END IF;
 BEGIN
  INSERT INTO stateknot_test_published_join_stop.guard_probe VALUES (NEW.tenant_id,NEW.run_id);
 EXCEPTION WHEN SQLSTATE 'SKG02' THEN
  GET STACKED DIAGNOSTICS original_code=RETURNED_SQLSTATE, original_message=MESSAGE_TEXT, original_detail=PG_EXCEPTION_DETAIL;
  RAISE EXCEPTION 'actual v3 complete guard [%]: %',original_code,original_message USING ERRCODE='P0001',DETAIL=original_detail;
 END;
 RAISE EXCEPTION 'fixture wrong proof was not rejected by the actual complete guard' USING ERRCODE='P0001';
END $$
").execute(&db).await.unwrap();
        query("CREATE CONSTRAINT TRIGGER aa_test_published_join_stop_guard_observer AFTER INSERT ON stateknot.graph_frame_closures DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot_test_published_join_stop.observe_original_guard()")
            .execute(&db).await.unwrap();
        let guard_order: Vec<String> = query_scalar(
            "SELECT tgname::text FROM pg_trigger WHERE tgrelid='stateknot.graph_frame_closures'::regclass AND tgname IN ('aa_test_published_join_stop_guard_observer','frame_closures_complete') AND tgenabled='O' ORDER BY tgname COLLATE \"C\"",
        ).fetch_all(&db).await.unwrap();
        query(&format!(r"
CREATE FUNCTION stateknot_test_published_join_stop.mutate_proof() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE wire jsonb; previous text;
BEGIN
 wire:=convert_from(NEW.closure_bytes,'UTF8')::jsonb;
 IF wire->>'version' IS DISTINCT FROM '3' OR wire->'intent'->>'version' IS DISTINCT FROM '3'
 THEN RAISE EXCEPTION 'fixture requires a real v3 writer' USING ERRCODE='P0001'; END IF;
 previous:=wire#>>'{path}';
 IF previous IS NULL OR previous='sha256:'||repeat('0',64)
 THEN RAISE EXCEPTION 'fixture requires a distinct complete digest' USING ERRCODE='P0001'; END IF;
 NEW.closure_bytes:=convert_to(jsonb_set(wire,'{path}',to_jsonb('sha256:'||repeat('0',64)),false)::text,'UTF8');
 RETURN NEW;
END $$
")).execute(&db).await.unwrap();
        query("CREATE TRIGGER test_published_join_stop_proof_fault BEFORE INSERT ON stateknot.graph_frame_closures FOR EACH ROW EXECUTE FUNCTION stateknot_test_published_join_stop.mutate_proof()")
            .execute(&db).await.unwrap();
        let outcome = Box::pin(store.close_graph_frames(
            key.tenant_id(),
            key.parent_run_id(),
            EventId::generate(),
            observed.clone(),
            run.lifecycle().revision(),
            direct.clone(),
        ))
        .await;
        // Restore the test-only catalog before inspecting any verdict.
        query(
            "DROP TRIGGER test_published_join_stop_proof_fault ON stateknot.graph_frame_closures",
        )
        .execute(&db)
        .await
        .unwrap();
        query("DROP FUNCTION stateknot_test_published_join_stop.mutate_proof()")
            .execute(&db)
            .await
            .unwrap();

        query("DROP TRIGGER aa_test_published_join_stop_guard_observer ON stateknot.graph_frame_closures")
            .execute(&db).await.unwrap();
        query("DROP FUNCTION stateknot_test_published_join_stop.observe_original_guard()")
            .execute(&db)
            .await
            .unwrap();
        query("DROP TABLE stateknot_test_published_join_stop.guard_probe")
            .execute(&db)
            .await
            .unwrap();
        query("DROP SCHEMA stateknot_test_published_join_stop")
            .execute(&db)
            .await
            .unwrap();
        let retained = registered_join_tenant_facts(&db, key.tenant_id()).await;
        let active =
            Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id())).await;
        let saved =
            Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id())).await;
        proof_observations.push((path, guard_order, outcome, retained, active, saved));
    }
    // Same-table deferred triggers execute in name order. This final fault is
    // after the real frame_closures_complete trigger, and checks all components
    // exist before aborting. It cannot turn an early shape failure into success.
    query("CREATE SCHEMA stateknot_test_published_join_stop")
        .execute(&db)
        .await
        .unwrap();
    query(r"
CREATE FUNCTION stateknot_test_published_join_stop.final_fault() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE whole stateknot.graph_frame_closures%ROWTYPE; wire jsonb;
BEGIN
 SELECT * INTO whole FROM stateknot.graph_frame_closures WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 wire:=convert_from(whole.closure_bytes,'UTF8')::jsonb;
 IF wire->>'version' IS DISTINCT FROM '3'
  OR (SELECT count(*) FROM stateknot.graph_frame_closed_callers WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id)<>whole.frame_count
  OR NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND active_namespace='' AND active_frame_identity_digest IS NULL)
  OR NOT EXISTS (SELECT 1 FROM stateknot.runs WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND lease_attempt_id IS NULL AND journal_sequence=whole.journal_sequence AND journal_digest=whole.journal_digest)
  OR NOT EXISTS (SELECT 1 FROM stateknot.run_events WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND sequence=whole.journal_sequence AND event_kind='graph-frames-closed' AND projection_digest=whole.compound_digest)
 THEN RAISE EXCEPTION 'fixture did not reach all v3 components' USING ERRCODE='P0001'; END IF;
 RAISE EXCEPTION 'injected after complete published Join closure' USING ERRCODE='P0001';
END $$
").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER zz_test_published_join_stop_final_fault AFTER INSERT ON stateknot.graph_frame_closures DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot_test_published_join_stop.final_fault()")
        .execute(&db).await.unwrap();
    let enabled: Vec<String> = query_scalar(
        "SELECT tgname::text FROM pg_trigger WHERE tgrelid='stateknot.graph_frame_closures'::regclass AND tgname IN ('frame_closures_complete','zz_test_published_join_stop_final_fault') AND tgenabled='O' ORDER BY tgname COLLATE \"C\"",
    ).fetch_all(&db).await.unwrap();
    let outcome = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        observed.clone(),
        run.lifecycle().revision(),
        direct.clone(),
    ))
    .await;
    query("DROP TRIGGER zz_test_published_join_stop_final_fault ON stateknot.graph_frame_closures")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot_test_published_join_stop.final_fault()")
        .execute(&db)
        .await
        .unwrap();
    query("DROP SCHEMA stateknot_test_published_join_stop")
        .execute(&db)
        .await
        .unwrap();
    let late_facts = registered_join_tenant_facts(&db, key.tenant_id()).await;
    let closed = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        EventId::generate(),
        observed,
        run.lifecycle().revision(),
        direct,
    ))
    .await;
    let schema_result = PostgresStore::graph_frame_published_join_stop_event_schema();
    let catalog_result = store.verify_schema().await;
    db.close().await;
    store.close().await;
    // All test DDL and connections are retired before the negative verdicts.
    for (path, guard_order, rejected, retained, active, saved) in proof_observations {
        assert_eq!(
            guard_order,
            [
                "aa_test_published_join_stop_guard_observer",
                "frame_closures_complete"
            ]
        );
        let Err(StoreError::Database { operation, source }) = rejected else {
            panic!("wrong published proof must reach the SQL guard: {rejected:?}");
        };
        assert_eq!(operation, "whole frame closure deferred components");
        let failure = source
            .as_database_error()
            .expect("actual PostgreSQL proof guard");
        assert_eq!(failure.code().as_deref(), Some("P0001"));
        assert_eq!(
            failure.message(),
            "actual v3 complete guard [SKG02]: published Join stop publication or binding mismatch"
        );
        let postgres = failure
            .try_downcast_ref::<sqlx_postgres::PgDatabaseError>()
            .expect("the original guard diagnostic must remain PostgreSQL");
        assert_eq!(postgres.detail().unwrap_or_default(), "");
        assert_eq!(
            retained, before,
            "the actual deferred v3 guard must undo all components: {path}"
        );
        assert!(saved.unwrap().is_none());
        assert_eq!(active.unwrap().unwrap().checkpoint(), &value.frame);
    }
    assert_eq!(
        enabled,
        [
            "frame_closures_complete",
            "zz_test_published_join_stop_final_fault"
        ]
    );
    let Err(StoreError::Database { operation, source }) = outcome else {
        panic!("late v3 fault must abort the Store transaction: {outcome:?}");
    };
    assert_eq!(operation, "whole frame closure deferred components");
    let failure = source
        .as_database_error()
        .expect("actual PostgreSQL deferred fault");
    assert_eq!(failure.code().as_deref(), Some("P0001"));
    assert_eq!(
        failure.message(),
        "injected after complete published Join closure"
    );
    assert_eq!(
        late_facts, before,
        "the late fault must undo the entire event/caller/stack/lease compound"
    );
    let closed = closed.unwrap();
    assert!(matches!(
        closed,
        stateknot_store_postgres::GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let (schema, _) = schema_result.unwrap();
    assert_eq!(closed.record().event().payload().schema(), &schema);
    catalog_result.unwrap();
}

mod frozen_direct_costs {
    use super::*;

    async fn priced_response(
        store: &PostgresStore,
        fence: &RunFence,
        start: &stateknot_core::NodeAttemptStartHead,
        head: &JournalHead,
    ) -> (JournalHead, BudgetUsage) {
        let intent = model_invocation_intent_for_activation(
            start.activation().clone(),
            InvocationId::generate(),
        );
        let prepared = store
            .prepare_model_invocation(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(head.clone()),
                    fence.clone(),
                    42_300,
                ),
                intent.clone(),
            )
            .await
            .unwrap();
        let physical = AttemptId::generate();
        let executing = store
            .advance_model_invocation(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    fence.clone(),
                    42_301,
                ),
                &prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: physical,
                },
            )
            .await
            .unwrap();
        let response = model_response(&intent, physical);
        // The fixture provider prices this actual committed response at USD11 micro-units.
        // Pricing is explicit trusted test evidence, not inferred from a zero-I/O child.
        let costs = serde_json::from_value(json!([{"currency":"USD","micro_units":"11"}])).unwrap();
        let usage = BudgetUsage::builder()
            .model_attempts(stateknot_core::ExecutionCount::new(1))
            .model_turns(stateknot_core::ExecutionCount::new(1))
            .input_tokens(response.usage().input_tokens())
            .cached_input_tokens(
                response
                    .usage()
                    .cached_input_tokens()
                    .unwrap_or(stateknot_core::TokenCount::ZERO),
            )
            .output_tokens(response.usage().output_tokens())
            .reasoning_tokens(
                response
                    .usage()
                    .reasoning_tokens()
                    .unwrap_or(stateknot_core::TokenCount::ZERO),
            )
            .known_costs(costs)
            .build()
            .unwrap();
        let committed = store
            .advance_model_invocation(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(executing.event().head()),
                    fence.clone(),
                    42_302,
                ),
                &executing.invocation().head(),
                ModelInvocationTransition::RecordResponse { response },
            )
            .await
            .unwrap();
        assert_eq!(
            committed.invocation().status(),
            ModelInvocationStatus::Committed
        );
        let loaded = store
            .load_model_invocation(fence.tenant_id(), fence.run_id(), intent.invocation_id())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&loaded).unwrap(),
            serde_json::to_value(committed.invocation()).unwrap()
        );
        (committed.event().head(), usage)
    }

    async fn child_complete_usage(
        store: &PostgresStore,
        child: &StoredAgentAdmission,
        event: EventId,
        model_usage: &BudgetUsage,
    ) -> BudgetUsage {
        let owner = child.admission().intent().provenance();
        let journal = store
            .load_journal_page(
                owner.tenant_id(),
                owner.run_id(),
                None,
                JournalPageSize::new(16).unwrap(),
            )
            .await
            .unwrap();
        assert!(!journal.has_more());
        assert_eq!(journal.events().last().unwrap().event_id(), event);
        let event_bytes: usize = journal
            .events()
            .iter()
            .map(|row| serde_json_canonicalizer::to_vec(row).unwrap().len())
            .sum();
        // This child committed no graph barrier. Count its real model response,
        // complete pre-terminal journal, initial checkpoint and admitted input.
        BudgetUsage::builder()
            .graph_depth(stateknot_core::ExecutionCount::new(1))
            .concurrent_branches(stateknot_core::ExecutionCount::new(1))
            .fan_out(stateknot_core::ExecutionCount::new(1))
            .event_bytes(ByteCount::new(event_bytes as u64))
            .checkpoint_bytes(initial_usage(child).checkpoint_bytes())
            .input_bytes(ByteCount::new(
                serde_json_canonicalizer::to_vec(child.admission().intent().request().input())
                    .unwrap()
                    .len() as u64,
            ))
            .build()
            .unwrap()
            .checked_accumulate(model_usage)
            .unwrap()
    }

    async fn commit_priced_child_failure(
        store: &PostgresStore,
        child: &StoredAgentAdmission,
        usage: &BudgetUsage,
    ) {
        let owner = child.admission().intent().provenance();
        let run = store
            .load_run(owner.tenant_id(), owner.run_id())
            .await
            .unwrap();
        let event = EventId::generate();
        let at = store.observe_database_clock().await.unwrap();
        store
            .append_control_plane(
                JournalAppend::new(
                    JournalExpectation::exact(run.journal_head().unwrap().clone()),
                    JournalEventIntent::control_plane(
                        owner.tenant_id().clone(),
                        owner.run_id(),
                        event,
                        payload("test-priced-child-failed"),
                    )
                    .unwrap(),
                )
                .unwrap(),
                RunProjection::transition(
                    run.lifecycle().revision(),
                    RunTransition::Fail {
                        failure: RunFailure::new(
                            terminal_run_failure(event, at).failure().clone(),
                            at,
                            usage.clone(),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await
            .unwrap();
    }

    async fn settle_priced_child(
        store: &PostgresStore,
        value: &Fixture,
        child: &StoredAgentAdmission,
    ) -> BudgetUsage {
        let owner = child.admission().intent().provenance();
        let fence = store
            .claim_lease(owner.tenant_id(), owner.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let plan = ReadyNodeRecoveryPlanner::new(child.checkpoint().clone(), fence.clone())
            .unwrap()
            .finish(
                child.event().head(),
                store.observe_database_clock().await.unwrap(),
            )
            .unwrap();
        let started = Box::pin(store.start_recovered_node_attempt(
            worker_append(
                owner.tenant_id().clone(),
                owner.run_id(),
                EventId::generate(),
                JournalExpectation::exact(child.event().head()),
                fence.clone(),
                42_303,
            ),
            &plan,
            child.checkpoint().ready_nodes().iter().next().unwrap(),
            AttemptId::generate(),
        ))
        .await
        .unwrap();
        let (head, model_usage) = Box::pin(priced_response(
            store,
            &fence,
            &started.attempt().start().head(),
            &started.event().head(),
        ))
        .await;
        let event = EventId::generate();
        let at = store.observe_database_clock().await.unwrap();
        Box::pin(store.fail_node_attempt(
            worker_append(
                owner.tenant_id().clone(),
                owner.run_id(),
                event,
                JournalExpectation::exact(head),
                fence,
                42_304,
            ),
            &started.attempt().start().head(),
            terminal_run_failure(event, at).failure().clone(),
            model_usage.clone(),
        ))
        .await
        .unwrap();
        let usage = Box::pin(child_complete_usage(store, child, event, &model_usage)).await;
        Box::pin(commit_priced_child_failure(store, child, &usage)).await;
        let append = join::control_append(store, value, "child-run-settled").await;
        let settled = Box::pin(store.settle_child_run(value.intent.key(), append.clone()))
            .await
            .unwrap();
        let stateknot_store_postgres::ChildRunSettlementOutcome::Committed { record, .. } = settled
        else {
            panic!("fresh priced child settlement");
        };
        assert_eq!(record.settlement().unwrap().usage(), &usage);
        assert!(matches!(
            Box::pin(store.settle_child_run(value.intent.key(), append))
                .await
                .unwrap(),
            stateknot_store_postgres::ChildRunSettlementOutcome::Idempotent { .. }
        ));
        usage
    }

    async fn reject_wrong_total_without_writes(
        store: &PostgresStore,
        cold: &PostgresStore,
        db: &PgPool,
        value: &Fixture,
        original: &stateknot_store_postgres::StoredGraphFrameClosure,
        account: &stateknot_core::ChildRunBudgetAccount,
    ) {
        let key = value.intent.key();
        let delegated = account.delegated_usage().unwrap();
        let total = original
            .direct_usage()
            .checked_accumulate(&delegated)
            .unwrap();
        let costs = serde_json::from_value(json!([{"currency":"USD","micro_units":"1"}])).unwrap();
        let extra = BudgetUsage::builder().known_costs(costs).build().unwrap();
        let wrong_total = total.checked_accumulate(&extra).unwrap();
        assert_ne!(wrong_total, total);
        assert!(wrong_total.checked_subtract_cumulative(&delegated).is_ok());
        let parent = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        let append = join::control_append(store, value, "test-frame-priced-wrong-total").await;
        let projection = RunProjection::transition(
            parent.lifecycle().revision(),
            RunTransition::ConfirmCancellation {
                completed_at: store.observe_database_clock().await.unwrap(),
                usage: wrong_total,
            },
        );
        let before = registered_join_tenant_facts(db, key.tenant_id()).await;
        let rejected = store.append_control_plane(append, projection).await;
        assert!(
            matches!(rejected, Err(StoreError::IncompleteChildAccounting)),
            "a subtractable wrong total must fail exact frozen DIRECT validation: {rejected:?}"
        );
        assert_eq!(
            registered_join_tenant_facts(db, key.tenant_id()).await,
            before
        );
        assert_eq!(
            cold.load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
            Some(account.clone())
        );
        let loaded = Box::pin(cold.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
        assert_same_published_close(&loaded, original);
        assert_eq!(
            registered_join_tenant_facts(db, key.tenant_id()).await,
            before
        );
    }

    async fn commit_priced_parent_terminal(
        store: &PostgresStore,
        cold: &PostgresStore,
        db: &PgPool,
        value: &Fixture,
        original: &stateknot_store_postgres::StoredGraphFrameClosure,
        account: &stateknot_core::ChildRunBudgetAccount,
    ) -> Result<
        (
            stateknot_store_postgres::AppendOutcome,
            JournalAppend,
            RunProjection,
            BudgetUsage,
        ),
        StoreError,
    > {
        let key = value.intent.key();
        let direct = original.direct_usage();
        let total = direct
            .checked_accumulate(&account.delegated_usage().unwrap())
            .unwrap();
        let parent = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        let append =
            join::control_append(store, value, "test-frame-priced-cancellation-confirmed").await;
        let projection = RunProjection::transition(
            parent.lifecycle().revision(),
            RunTransition::ConfirmCancellation {
                completed_at: store.observe_database_clock().await.unwrap(),
                usage: total.clone(),
            },
        );
        let before = registered_join_tenant_facts(db, key.tenant_id()).await;
        let observed = store
            .append_control_plane(append.clone(), projection.clone())
            .await;
        println!(
            "\nSTATEKNOT_FRAME_FROZEN_DIRECT_COST_EVIDENCE={}",
            json!({
                "phase":"terminal", "terminal_committed":observed.is_ok(),
                "wrong_total_rejected_without_writes":true,
                "actual_error":observed.as_ref().err().map(|error| format!("{error:?}")),
                "frozen_direct_costs":direct.known_costs(),
                "actual_delegated_costs":account.delegated_usage().unwrap().known_costs(),
                "original_close_event":original.event().event_id(),
            })
        );
        let ack = match observed {
            Ok(ack) => ack,
            Err(error) => {
                assert_eq!(
                    registered_join_tenant_facts(db, key.tenant_id()).await,
                    before
                );
                assert_eq!(
                    store
                        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
                        .await
                        .unwrap(),
                    Some(account.clone())
                );
                let loaded =
                    Box::pin(cold.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
                        .await
                        .unwrap()
                        .unwrap();
                assert_same_published_close(&loaded, original);
                assert_eq!(
                    registered_join_tenant_facts(db, key.tenant_id()).await,
                    before
                );
                return Err(error);
            }
        };
        Ok((ack, append, projection, total))
    }

    async fn terminal_and_replay(
        store: &PostgresStore,
        cold: &PostgresStore,
        db: &PgPool,
        value: &Fixture,
        original: &stateknot_store_postgres::StoredGraphFrameClosure,
        account: &stateknot_core::ChildRunBudgetAccount,
    ) -> Result<(), StoreError> {
        Box::pin(reject_wrong_total_without_writes(
            store, cold, db, value, original, account,
        ))
        .await;
        let (ack, append, projection, total) = Box::pin(commit_priced_parent_terminal(
            store, cold, db, value, original, account,
        ))
        .await?;
        let key = value.intent.key();
        let direct = original.direct_usage();
        let expected = account
            .observe_direct(ack.event().head(), direct.clone())
            .unwrap();
        let terminal = cold
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
        assert_eq!(terminal.lifecycle().terminal_usage(), Some(&total));
        assert_eq!(terminal.journal_head(), Some(&ack.event().head()));
        let actual = cold
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.direct_head(), &ack.event().head());
        assert_eq!(actual.direct_usage(), direct);
        assert!(actual.direct_usage().known_costs().is_empty());
        assert_eq!(actual.children(), account.children());
        assert_eq!(
            actual.delegated_usage().unwrap(),
            account.delegated_usage().unwrap()
        );
        let retained = registered_join_tenant_facts(db, key.tenant_id()).await;
        let loaded = Box::pin(cold.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
        assert_same_published_close(&loaded, original);
        let replay = cold.append_control_plane(append, projection).await.unwrap();
        assert_eq!(replay.event(), ack.event());
        let replay = Box::pin(cold.close_graph_frames(
            key.tenant_id(),
            key.parent_run_id(),
            EventId::generate(),
            value.head.clone(),
            value.admission.run().lifecycle().revision(),
            BudgetUsage::zero(),
        ))
        .await
        .unwrap();
        assert!(matches!(
            &replay,
            stateknot_store_postgres::GraphFrameClosureCommitOutcome::Existing(_)
        ));
        assert_same_published_close(replay.record(), original);
        assert_eq!(
            cold.load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
            Some(expected)
        );
        assert!(
            cold.load_run_failure_close(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            registered_join_tenant_facts(db, key.tenant_id()).await,
            retained
        );
        Ok(())
    }

    async fn register_and_settle_priced_child(
        store: &PostgresStore,
        value: &Fixture,
    ) -> (
        stateknot_store_postgres::ChildJoinCommitOutcome,
        stateknot_store_postgres::ChildRunRecord,
        stateknot_core::ChildRunBudgetAccount,
    ) {
        let key = value.intent.key();
        let spawned = Box::pin(spawn(store, value)).await.unwrap();
        let registered = Box::pin(store.register_child_join(
            stateknot_core::ChildRunJoinRequest::new([key.clone()]).unwrap(),
            &value.node,
            join_append(value, &spawned.record().spawn().head()),
        ))
        .await
        .unwrap();
        let usage = Box::pin(settle_priced_child(store, value, spawned.record().child())).await;
        assert_eq!(
            serde_json::to_value(usage.known_costs()).unwrap(),
            json!([{"currency":"USD","micro_units":"11"}])
        );
        assert_eq!(
            usage.unpriced_cost_events(),
            stateknot_core::ExecutionCount::ZERO
        );
        let child = Box::pin(store.load_child_run(key)).await.unwrap();
        assert_eq!(child.settlement().unwrap().usage(), &usage);
        assert_eq!(child.child().run().lifecycle().status(), RunStatus::Failed);
        assert_eq!(
            child.child().run().lifecycle().terminal_usage(),
            Some(&usage)
        );
        let account = store
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(account.children().len(), 1);
        assert_eq!(account.children()[0].key(), key);
        assert_eq!(account.children()[0].settlement().unwrap().usage(), &usage);
        assert_eq!(account.delegated_usage().unwrap(), usage.cumulative_only());
        (registered, child, account)
    }

    async fn characterize(
        store: &PostgresStore,
        cold: &PostgresStore,
        db: &PgPool,
    ) -> Result<(), StoreError> {
        let value = Box::pin(started(store, "frame-frozen-direct-priced-child")).await;
        let key = value.intent.key();
        let (registered, child, account) =
            Box::pin(register_and_settle_priced_child(store, &value)).await;
        let requested = Box::pin(cancel_parent(store, &value)).await;
        let direct = Box::pin(join::direct(store, &value, 2, &BudgetUsage::zero())).await;
        assert!(direct.known_costs().is_empty());

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
            requested,
            parent.lifecycle().revision(),
            direct.clone(),
        ))
        .await
        .unwrap();
        assert!(matches!(
            &closed,
            stateknot_store_postgres::GraphFrameClosureCommitOutcome::Committed(_)
        ));
        assert_eq!(closed.record().direct_usage(), &direct);
        assert!(closed.record().original_cancellation_lifecycle().is_some());
        assert_eq!(
            closed.record().event().payload().data().as_value()["version"],
            json!(2)
        );
        Box::pin(assert_stopped_registered_join_owner(
            store,
            &value,
            registered.record(),
            closed.record(),
        ))
        .await;
        let loaded = Box::pin(cold.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
        assert_same_published_close(&loaded, closed.record());
        assert_eq!(
            cold.load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
            Some(account.clone())
        );
        let total = direct
            .checked_accumulate(&account.delegated_usage().unwrap())
            .unwrap();
        assert_eq!(
            Box::pin(store.include_child_usage(
                key.tenant_id(),
                key.parent_run_id(),
                direct.clone()
            ))
            .await
            .unwrap(),
            total
        );
        assert!(
            store
                .load_run_failure_close(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap()
                .is_none()
        );
        println!(
            "\nSTATEKNOT_FRAME_FROZEN_DIRECT_COST_EVIDENCE={}",
            json!({
                "phase":"setup_verified", "failure_close_absent":true,
                "closure_version":2, "frozen_direct_costs":direct.known_costs(),
                "actual_child_costs":child.settlement().unwrap().usage().known_costs(),
                "actual_delegated_costs":account.delegated_usage().unwrap().known_costs(),
                "closure_ack_reloaded":true, "original_owner_retained":true,
            })
        );
        Box::pin(terminal_and_replay(
            store,
            cold,
            db,
            &value,
            closed.record(),
            &account,
        ))
        .await
    }

    #[tokio::test]
    async fn closed_frame_cancellation_preserves_empty_direct_costs_with_priced_child() {
        let _guard = DATABASE_TEST_MUTEX.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let url = std::env::var(DATABASE_URL_ENV).unwrap();
        let cold = PostgresStore::connect(&url, store.options().clone())
            .await
            .unwrap();
        let db = super::super::super::frame_barriers::pool().await;
        let (worker, reloaded, facts) = (store.clone(), cold.clone(), db.clone());
        let observed =
            tokio::spawn(async move { Box::pin(characterize(&worker, &reloaded, &facts)).await })
                .await;
        // Drain the owned test task and close both Store pools and the facts pool
        // before reporting either a setup panic or the terminal characterization.
        cold.close().await;
        db.close().await;
        store.close().await;
        observed
            .expect("frame-cost setup and owning assertions must finish")
            .expect("a correct whole-frame total must retain the original frozen DIRECT");
    }
}
