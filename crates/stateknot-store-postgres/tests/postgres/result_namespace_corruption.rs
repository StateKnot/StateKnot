// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Root result paging must not turn a corrupted namespace into missing work.
use super::*;
use stateknot_core::{PendingNodeResult, RunFence};

async fn committed_root_result(
    store: &PostgresStore,
    prefix: &str,
) -> (Checkpoint, RunFence, PendingNodeResult) {
    let tenant_id = tenant(prefix);
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(store, &tenant_id, run_id, 24_000)).await;
    let fence = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), &[]);
    let committed = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id,
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                fence.clone(),
                24_001,
            ),
            pending_result_intent(activation, NodeInvocationBindings::empty()),
        )
        .await
        .unwrap();
    assert!(matches!(
        committed,
        PendingNodeResultCommitOutcome::Committed { .. }
    ));
    (
        checkpoint.checkpoint().clone(),
        fence,
        committed.result().clone(),
    )
}

async fn replace_result_namespace(
    db: &PgPool,
    activation: &NodeActivation,
    previous: &GraphNamespace,
    replacement: &GraphNamespace,
) {
    // Inject one stored projection contradiction without dropping constraints
    // or changing catalog trigger state. Commit/rollback restores the role.
    let mut tx = db.begin().await.unwrap();
    query("SET LOCAL session_replication_role = replica")
        .execute(&mut *tx)
        .await
        .unwrap();
    let changed = query(
        "UPDATE stateknot.pending_node_results SET graph_namespace = $6 \
         WHERE tenant_id = $1 AND run_id = $2 AND base_checkpoint_id = $3 \
           AND node_id = $4 AND graph_namespace = $5",
    )
    .bind(activation.tenant_id().as_str())
    .bind(*activation.run_id().as_uuid())
    .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
    .bind(activation.node_id().as_str())
    .bind(previous.as_str())
    .bind(replacement.as_str())
    .execute(&mut *tx)
    .await
    .unwrap()
    .rows_affected();
    tx.commit().await.unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        query_scalar::<_, String>("SELECT current_setting('session_replication_role')")
            .fetch_one(db)
            .await
            .unwrap(),
        "origin"
    );
}

#[tokio::test]
async fn root_pending_result_page_rejects_a_relabelled_namespace() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let (checkpoint, fence, result) =
        Box::pin(committed_root_result(&store, "root-result-namespace")).await;
    let size = PendingNodeResultPageSize::new(1).unwrap();
    let healthy = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await
        .unwrap();
    assert_eq!(healthy.records(), std::slice::from_ref(&result));
    assert!(!healthy.has_more());
    let alien = GraphNamespace::new("f".repeat(64)).unwrap();
    let activation = result.intent().activation();
    replace_result_namespace(&db, activation, activation.graph_namespace(), &alien).await;
    let rejected = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await;
    // Restore even when the current reader erroneously returns an empty page.
    replace_result_namespace(&db, activation, &alien, activation.graph_namespace()).await;
    let restored = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await
        .unwrap();
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(matches!(rejected, Err(StoreError::CorruptData { .. })));
    assert_eq!(restored.records(), std::slice::from_ref(&result));
    assert_eq!(run.journal_head(), Some(result.journal_head()));
    assert!(!run.is_quarantined());
    assert_eq!(run.lease().unwrap().fence(), &fence);
}

#[tokio::test]
async fn claimed_root_result_namespace_corruption_quarantines_the_current_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let (checkpoint, fence, result) = Box::pin(committed_root_result(
        &store,
        "claimed-root-result-namespace",
    ))
    .await;
    let quarantine_id = QuarantineId::generate();
    let expectation = JournalExpectation::exact(result.journal_head().clone());
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        quarantine_id,
        expectation.clone(),
        Digest::sha256(b"claimed Root result namespace contradiction"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let size = PendingNodeResultPageSize::new(1).unwrap();
    let healthy = recovery
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await
        .unwrap();
    assert_eq!(healthy.records(), std::slice::from_ref(&result));
    let alien = GraphNamespace::new("f".repeat(64)).unwrap();
    let activation = result.intent().activation();
    replace_result_namespace(&db, activation, activation.graph_namespace(), &alien).await;
    let rejected = recovery
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await;
    replace_result_namespace(&db, activation, &alien, activation.graph_namespace()).await;
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let quarantine = store
        .load_run_quarantine(fence.tenant_id(), fence.run_id())
        .await;
    let blocked = recovery.plan_ready_nodes().await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(matches!(rejected, Err(StoreError::RunQuarantined)));
    assert!(run.is_quarantined());
    assert!(run.lease().is_none());
    let quarantine = quarantine.unwrap();
    assert_eq!(quarantine.request().quarantine_id(), quarantine_id);
    assert_eq!(quarantine.request().expected_fence(), Some(&fence));
    assert_eq!(quarantine.request().expectation(), &expectation);
    assert_eq!(
        quarantine.request().cause(),
        RunQuarantineCause::IntegrityFailure
    );
    assert!(matches!(blocked, Err(StoreError::RunQuarantined)));
}

async fn committed_parallel_root_results(store: &PostgresStore) -> (Checkpoint, RunFence) {
    let template = checkpoint_compiled_graph();
    let ready =
        ReadyNodes::try_new(["node-0001", "node-0002"].map(|name| NodeId::new(name).unwrap()))
            .unwrap();
    let graph = CompiledGraph::compile(
        template.identity().clone(),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        ready.clone(),
        template.nodes().iter().cloned(),
        GraphExecutionLimits::new(template.limits().maximum_supersteps(), 2).unwrap(),
    )
    .unwrap();
    let tenant_id = tenant("root-result-cursor-namespace");
    let run_id = RunId::generate();
    store
        .register_graph_definition(tenant_id.clone(), graph.clone())
        .await
        .unwrap();
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let reference = graph.reference();
    let checkpoint = store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                24_100,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            CheckpointWrite::initial(
                tenant_id.clone(),
                run_id,
                CheckpointId::generate(),
                reference.clone(),
                checkpoint_state(&reference, 0),
                ready,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let fence = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    Box::pin(commit_ready_results(
        store,
        checkpoint.checkpoint(),
        &fence,
        24_101,
    ))
    .await;
    (checkpoint.checkpoint().clone(), fence)
}

#[tokio::test]
async fn root_result_continuation_rejects_a_relabelled_cursor_and_quarantines() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let (checkpoint, fence) = Box::pin(committed_parallel_root_results(&store)).await;
    let size = PendingNodeResultPageSize::new(1).unwrap();
    let first = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await
        .unwrap();
    assert_eq!(first.records().len(), 1);
    assert!(first.has_more());
    let cursor = first.next_cursor().unwrap();
    let next = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), Some(&cursor), size)
        .await
        .unwrap();
    assert_eq!(next.records().len(), 1);
    assert!(!next.has_more());
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(first.snapshot_journal_head().clone()),
        Digest::sha256(b"claimed Root continuation namespace contradiction"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let alien = GraphNamespace::new("f".repeat(64)).unwrap();
    let activation = cursor.after().activation();
    replace_result_namespace(&db, activation, activation.graph_namespace(), &alien).await;
    let raw = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), Some(&cursor), size)
        .await;
    let claimed = recovery
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), Some(&cursor), size)
        .await;
    replace_result_namespace(&db, activation, &alien, activation.graph_namespace()).await;
    let restored = store
        .load_unconsumed_pending_node_result_page(&checkpoint.head(), None, size)
        .await
        .unwrap();
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let quarantine = store
        .load_run_quarantine(fence.tenant_id(), fence.run_id())
        .await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(matches!(raw, Err(StoreError::CorruptData { .. })));
    assert!(matches!(claimed, Err(StoreError::RunQuarantined)));
    assert_eq!(restored.records(), first.records());
    assert!(run.is_quarantined());
    assert!(run.lease().is_none());
    assert_eq!(quarantine.unwrap().request().expected_fence(), Some(&fence));
}
