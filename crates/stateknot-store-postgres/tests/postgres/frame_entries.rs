// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Reserved compound kinds cannot be recorded with a legacy single projection.
use super::*;
use stateknot_core::GraphFrameEntryPlan;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn compound_frame_entry_rejects_ordinary_append_start_and_checkpoint_without_partial_facts() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-entry-reserved");
    let run = RunId::generate();
    let initial = Box::pin(start_run_with_checkpoint(&store, &tenant, run, 21_000)).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let attempt_id = AttemptId::generate();
    let activation = pending_activation(initial.checkpoint(), b"reserved compound");
    let (schema, _) = GraphFrameEntryPlan::event_schema().unwrap();
    // Reservation is by kind, even when an ordinary caller supplies another
    // bounded payload. The absent guard would accept this through legacy APIs.
    let payload = JournalPayload::new(
        schema,
        GraphFrameEntryPlan::EVENT_KIND.parse().unwrap(),
        BoundedJson::try_from(json!({"reserved":true})).unwrap(),
    )
    .unwrap();
    let worker = || {
        JournalAppend::new(
            JournalExpectation::exact(initial.event().head()),
            JournalEventIntent::worker(
                tenant.clone(),
                run,
                EventId::generate(),
                lease.fence().clone(),
                payload.clone(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let control = JournalAppend::new(
        JournalExpectation::exact(initial.event().head()),
        JournalEventIntent::control_plane(
            tenant.clone(),
            run,
            EventId::generate(),
            payload.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_control_plane(control, RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert!(matches!(
        store
            .append_worker(worker(), RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert!(matches!(
        store
            .start_node_attempt(worker(), activation, attempt_id)
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    // Test both initial-checkpoint paths on a second Run with no checkpoint.
    // All ordinary inputs and lifecycle/fence gates remain valid.
    let empty_run = RunId::generate();
    let admission = store
        .admit_run(provenance(tenant.clone(), empty_run))
        .await
        .unwrap();
    let projection = || {
        RunProjection::transition(
            admission.lifecycle().revision(),
            RunTransition::Start {
                started_at: admission.lifecycle().admitted_at(),
            },
        )
    };
    let control = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            tenant.clone(),
            empty_run,
            EventId::generate(),
            payload.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_control_plane_checkpoint(
                control,
                projection(),
                initial_checkpoint_write(tenant.clone(), empty_run, CheckpointId::generate())
            )
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let unchanged = store.load_run(&tenant, empty_run).await.unwrap();
    assert_eq!(
        serde_json::to_vec(unchanged.lifecycle()).unwrap(),
        serde_json::to_vec(admission.lifecycle()).unwrap()
    );
    assert!(unchanged.journal_head().is_none() && unchanged.checkpoint().is_none());
    let started = store
        .append_control_plane(
            control_append(
                tenant.clone(),
                empty_run,
                EventId::generate(),
                JournalExpectation::empty(),
                21_001,
            ),
            projection(),
        )
        .await
        .unwrap();
    let empty_lease = store
        .claim_lease(&tenant, empty_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let worker = JournalAppend::new(
        JournalExpectation::exact(started.event().head()),
        JournalEventIntent::worker(
            tenant.clone(),
            empty_run,
            EventId::generate(),
            empty_lease.fence().clone(),
            payload,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker_checkpoint(
                worker,
                RunProjection::unchanged(),
                initial_checkpoint_write(tenant.clone(), empty_run, CheckpointId::generate())
            )
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let unchanged = store.load_run(&tenant, empty_run).await.unwrap();
    assert_eq!(unchanged.journal_head(), Some(&started.event().head()));
    assert!(unchanged.checkpoint().is_none());
    let loaded = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(loaded.journal_head(), Some(&initial.event().head()));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .as_ref(),
        Some(initial.checkpoint())
    );
    assert!(matches!(
        store.load_node_attempt(&tenant, &run, attempt_id).await,
        Err(StoreError::NodeAttemptNotFound)
    ));
    let url = std::env::var(DATABASE_URL_ENV).unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    for (table, expected) in [
        ("run_events", 1_i64),
        ("run_checkpoints", 1),
        ("node_attempts", 0),
        ("run_attempt_claims", 0),
    ] {
        let actual = query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2"
        ))
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(actual, expected, "no partial {table} evidence");
    }
    pool.close().await;
    store.close().await;
}
