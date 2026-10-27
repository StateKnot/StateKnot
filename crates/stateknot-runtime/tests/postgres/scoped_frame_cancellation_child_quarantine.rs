// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Fresh cancellation observes child quarantine; an existing terminal ACK stays historical.
use super::*;
use stateknot_store_postgres::{
    AppendOutcome, RunQuarantineCause, RunQuarantineCommitOutcome, RunQuarantineComponent,
    RunQuarantineRequest,
};

async fn quarantine_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}

async fn closed_fixture(store: &PostgresStore) -> (Scenario, StoredGraphFrameClosure) {
    let scenario = Box::pin(prepare(store)).await;
    let key = &scenario.key;
    let outcome = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        scenario.event_id,
        scenario.observed.clone(),
        scenario.revision,
        scenario.direct.clone(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let closure = Box::pin(owning_close(store, &scenario)).await;
    assert_eq!(fingerprint(&closure), fingerprint(outcome.record()));
    (scenario, closure)
}

fn confirmation_payload(scenario: &Scenario, closure: &StoredGraphFrameClosure) -> JournalPayload {
    let (schema, document) = stateknot_runtime::standard_agent_cancellation_event_schema().unwrap();
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    builder.register(schema.clone(), document).unwrap();
    let registry = builder.build().unwrap();
    let graph = scenario
        .root_checkpoint
        .graph()
        .definition_digest()
        .to_string();
    let data = BoundedJson::try_from_value(json!({
        "operation": "agent_cancellation_confirmed",
        "graph_digest": graph.strip_prefix("sha256:").unwrap(),
        "checkpoint_id": scenario.root_checkpoint.checkpoint_id().to_string(),
        "superstep": scenario.root_checkpoint.superstep().get().to_string(),
        "failure_id": closure.failure().id().to_string(),
    }))
    .unwrap();
    registry.validate_bounded(&schema, &data).unwrap();
    JournalPayload::new(
        schema,
        JournalEventKind::new("agent-cancellation-confirmed").unwrap(),
        data,
    )
    .unwrap()
}

fn confirmation_append(
    scenario: &Scenario,
    closure: &StoredGraphFrameClosure,
    event_id: EventId,
    payload: JournalPayload,
) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(closure.event().head()),
        JournalEventIntent::control_plane(
            scenario.key.tenant_id().clone(),
            scenario.key.parent_run_id(),
            event_id,
            payload,
        )
        .unwrap(),
    )
    .unwrap()
}

async fn actual_total(store: &PostgresStore, scenario: &Scenario) -> BudgetUsage {
    let total = Box::pin(store.include_child_usage(
        scenario.key.tenant_id(),
        scenario.key.parent_run_id(),
        scenario.direct.clone(),
    ))
    .await
    .unwrap();
    assert_eq!(
        total,
        scenario
            .direct
            .checked_accumulate(&scenario.child_usage.cumulative_only())
            .unwrap()
    );
    assert_ne!(total, scenario.direct);
    total
}

async fn quarantine_settled_child(store: &PostgresStore, scenario: &Scenario) {
    let record = Box::pin(store.load_child_run(&scenario.key)).await.unwrap();
    let child = record.child();
    let child_id = child.admission().intent().provenance().run_id();
    assert_eq!(child.run().lifecycle().status(), RunStatus::Cancelled);
    assert!(!child.run().is_quarantined());
    assert_eq!(record.settlement().unwrap().usage(), &scenario.child_usage);
    let request = RunQuarantineRequest::new(
        scenario.key.tenant_id().clone(),
        child_id,
        QuarantineId::generate(),
        JournalExpectation::exact(child.run().journal_head().unwrap().clone()),
        RunQuarantineCause::OperatorPolicy,
        RunQuarantineComponent::new("closed-frame-child-quarantine").unwrap(),
        Digest::sha256(b"public child quarantine after immutable frame closure"),
    )
    .unwrap();
    assert!(matches!(
        store.quarantine_run(request.clone()).await.unwrap(),
        RunQuarantineCommitOutcome::Committed(_)
    ));
    let retained = store
        .load_run_quarantine(scenario.key.tenant_id(), child_id)
        .await
        .unwrap();
    assert_eq!(retained.request(), &request);
    let quarantined = store
        .load_run(scenario.key.tenant_id(), child_id)
        .await
        .unwrap();
    assert!(quarantined.is_quarantined());
    assert_eq!(
        serde_json::to_value(quarantined.lifecycle()).unwrap(),
        serde_json::to_value(child.run().lifecycle()).unwrap()
    );
    assert_eq!(quarantined.journal_head(), child.run().journal_head());
    assert!(quarantined.lease().is_none());
}

#[tokio::test]
async fn fresh_closed_frame_confirmation_rejects_quarantined_settled_child_without_writes() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (scenario, closure) = Box::pin(closed_fixture(&store)).await;
    let pool = quarantine_pool().await;
    let wire = v2_bytes(&pool, &scenario).await;
    Box::pin(quarantine_settled_child(&store, &scenario)).await;
    let before = snapshot(&pool, scenario.key.tenant_id()).await;
    // Historical owning evidence and priced usage remain readable. The fresh
    // terminal transaction must separately lock and reject the quarantined child.
    let verified = Box::pin(owning_close(&store, &scenario)).await;
    assert_eq!(fingerprint(&verified), fingerprint(&closure));
    let total = Box::pin(actual_total(&store, &scenario)).await;
    let append = confirmation_append(
        &scenario,
        &verified,
        EventId::generate(),
        confirmation_payload(&scenario, &verified),
    );
    let result = store
        .append_control_plane(
            append,
            RunProjection::transition(
                scenario.revision,
                RunTransition::ConfirmCancellation {
                    completed_at: store.observe_database_clock().await.unwrap(),
                    usage: total,
                },
            ),
        )
        .await;
    let after = snapshot(&pool, scenario.key.tenant_id()).await;
    eprintln!(
        "fresh closed-frame child quarantine: rejected={} whole_tenant_unchanged={}",
        matches!(&result, Err(StoreError::RunQuarantined)),
        before == after,
    );
    assert!(
        matches!(&result, Err(StoreError::RunQuarantined)),
        "fresh terminal append must reject the quarantined settled child: {result:?}"
    );
    assert_eq!(after, before, "rejected terminal append wrote tenant facts");
    assert_eq!(v2_bytes(&pool, &scenario).await, wire);
    let retained = Box::pin(owning_close(&store, &scenario)).await;
    assert_eq!(fingerprint(&retained), fingerprint(&closure));
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, before);
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn committed_closed_frame_confirmation_ack_survives_later_settled_child_quarantine() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (scenario, closure) = Box::pin(closed_fixture(&store)).await;
    let pool = quarantine_pool().await;
    let wire = v2_bytes(&pool, &scenario).await;
    let total = Box::pin(actual_total(&store, &scenario)).await;
    let outcome = store
        .append_control_plane(
            confirmation_append(
                &scenario,
                &closure,
                EventId::generate(),
                confirmation_payload(&scenario, &closure),
            ),
            RunProjection::transition(
                scenario.revision,
                RunTransition::ConfirmCancellation {
                    completed_at: store.observe_database_clock().await.unwrap(),
                    usage: total.clone(),
                },
            ),
        )
        .await
        .unwrap();
    let AppendOutcome::Committed(committed) = outcome else {
        panic!("the original real terminal append must commit");
    };
    Box::pin(quarantine_settled_child(&store, &scenario)).await;
    let before = snapshot(&pool, scenario.key.tenant_id()).await;
    // Rebuild from persisted event/lifecycle evidence rather than retaining the
    // original append or assuming event.recorded_at is the completion clock.
    let terminal = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        terminal.lifecycle().revision().get(),
        scenario.revision.get() + 1
    );
    assert_eq!(terminal.lifecycle().terminal_usage(), Some(&total));
    assert_eq!(
        store
            .load_current_checkpoint(scenario.key.tenant_id(), scenario.key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        scenario.root_checkpoint
    );
    assert!(terminal.lease().is_none());
    assert!(!terminal.is_quarantined());
    let page = store
        .load_journal_page(
            scenario.key.tenant_id(),
            scenario.key.parent_run_id(),
            Some(&closure.event().head()),
            JournalPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    assert_eq!(page.events(), std::slice::from_ref(&committed));
    let actual = &page.events()[0];
    assert_eq!(terminal.journal_head(), Some(&actual.head()));
    assert_eq!(
        actual.sequence().get(),
        closure.event().sequence().get() + 1
    );
    assert_eq!(actual.previous_digest(), Some(closure.event().digest()));
    assert_eq!(
        actual.source(),
        &stateknot_core::JournalEventSource::ControlPlane
    );
    assert_eq!(actual.payload(), &confirmation_payload(&scenario, &closure));
    assert!(terminal.lifecycle().changed_at() >= closure.event().recorded_at());
    assert!(terminal.lifecycle().changed_at() <= actual.recorded_at());
    let historical = Box::pin(
        store.load_graph_frame_closure(scenario.key.tenant_id(), scenario.key.parent_run_id()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fingerprint(&historical), fingerprint(&closure));
    assert_eq!(Box::pin(actual_total(&store, &scenario)).await, total);
    let retry = confirmation_append(
        &scenario,
        &historical,
        actual.event_id(),
        actual.payload().clone(),
    );
    let projection = RunProjection::transition(
        scenario.revision,
        RunTransition::ConfirmCancellation {
            completed_at: terminal.lifecycle().changed_at(),
            usage: terminal.lifecycle().terminal_usage().unwrap().clone(),
        },
    );
    let recovered = store.append_control_plane(retry, projection).await.unwrap();
    assert!(matches!(&recovered, AppendOutcome::Idempotent(event) if event == actual));
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, before);
    assert_eq!(v2_bytes(&pool, &scenario).await, wire);
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, before);
    pool.close().await;
    store.close().await;
}
