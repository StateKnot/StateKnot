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
