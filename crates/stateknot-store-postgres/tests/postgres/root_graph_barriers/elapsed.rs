// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! New-source V3 Root cases. These fixtures require fresh qualification and do
//! not transfer receipts from the recovered, incomplete earlier source tree.

use super::*;
use stateknot_store_postgres::GraphWaitEffect;

async fn prepare_v3(
    store: &PostgresStore,
    plan: GraphBarrierPlan,
    fence: &RunFence,
) -> (JournalAppend, RootGraphBarrierIntent) {
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let event = EventId::generate();
    let intent = store
        .prepare_worker_graph_barrier(plan, run.lifecycle().clone(), event, None, None)
        .await
        .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::worker(
            fence.tenant_id().clone(),
            fence.run_id(),
            event,
            fence.clone(),
            intent.payload().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    (append, intent)
}

async fn clock(db: &PgPool) -> i64 {
    query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(db)
        .await
        .unwrap()
}

#[tokio::test]
async fn root_mixed_originals_register_only_remaining_conditions_and_keep_waiting_ack() {
    Box::pin(root_mixed_originals()).await;
}

#[allow(clippy::too_many_lines)]
async fn root_mixed_originals() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-v3-mixed")).await;
    let now = clock(&db).await;
    let elapsed_id = TimerId::generate();
    let future_id = TimerId::generate();
    let interrupt_id = InterruptId::generate();
    let waits = NodeWaits::try_new([
        NodeWait::timer(
            elapsed_id,
            RunTimerKind::Sleep,
            Timestamp::from_unix_micros(now - 1).unwrap(),
        ),
        NodeWait::timer(
            future_id,
            RunTimerKind::Sleep,
            Timestamp::from_unix_micros(now + 60_000_000).unwrap(),
        ),
        interrupt(interrupt_id),
    ])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait {
            waits: waits.clone(),
        },
        false,
    ))
    .await;
    let pending = serde_json_canonicalizer::to_vec(&result).unwrap();
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepare_v3(&store, plan.clone(), &fence)).await;
    let record = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    assert_eq!(record.wait_effect(), Some(GraphWaitEffect::Waiting));
    assert_eq!(record.disposition(), plan.disposition());
    assert_eq!(record.waits().len(), 2);
    assert_eq!(record.committed_lifecycle().status(), RunStatus::Waiting);
    assert_eq!(
        record.committed_lifecycle().revision().get(),
        record.original_lifecycle().revision().get() + 1
    );
    assert_eq!(
        record.event().payload().schema().version(),
        Version::new(3, 0, 0)
    );
    assert!(matches!(
        store
            .load_durable_timer(fence.tenant_id(), fence.run_id(), elapsed_id)
            .await,
        Err(StoreError::WaitRegistrationNotFound)
    ));
    let profiles: Vec<String> = query_scalar("SELECT profile FROM stateknot.graph_wait_conditions WHERE tenant_id=$1 AND run_id=$2 ORDER BY condition_ordinal")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_all(&db).await.unwrap();
    assert_eq!(
        profiles,
        ["graph_elapsed", "graph_registered", "graph_registered"]
    );
    let request = store
        .load_interrupt_request(fence.tenant_id(), fence.run_id(), interrupt_id)
        .await
        .unwrap();
    let resolution_id = EventId::generate();
    store
        .resolve_interrupt(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                resolution_id,
                JournalExpectation::exact(record.event().head()),
                96_350,
            ),
            record.committed_lifecycle().revision(),
            InterruptResolutionIntent::new(
                &request,
                resolution_id,
                payload(96_351),
                InterruptResolver::new(
                    PrincipalIdentity::new(
                        "https://issuer.example.com/root-v3".parse().unwrap(),
                        "approver".parse().unwrap(),
                    ),
                    ScopeSet::empty(),
                ),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let before_ack = snapshot(&db, &fence).await;
    let ack = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &Unavailable,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    let RootGraphBarrierCommitOutcome::Idempotent(ack) = ack else {
        panic!("original Waiting ACK");
    };
    assert_eq!(ack.digest(), record.digest());
    assert_eq!(ack.wait_effect(), Some(GraphWaitEffect::Waiting));
    assert_eq!(ack.waits(), record.waits());
    assert_eq!(snapshot(&db, &fence).await, before_ack);
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(current.unresolved_wait_count(), 1);
    assert!(current.lease().is_none());
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store
                .load_pending_node_result(result.intent().activation())
                .await
                .unwrap()
        )
        .unwrap(),
        pending
    );
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn expired_root_interrupt_cannot_turn_mixed_wait_into_ready_or_partially_commit() {
    Box::pin(expired_root_interrupt()).await;
}

async fn expired_root_interrupt() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-v3-expired-interrupt")).await;
    let due = Timestamp::from_unix_micros(clock(&db).await + 500_000).unwrap();
    let waits = NodeWaits::try_new([
        NodeWait::timer(TimerId::generate(), RunTimerKind::Sleep, due),
        NodeWait::interrupt(
            InterruptId::generate(),
            RunInterruptKind::Approval,
            payload(96_352),
            Digest::sha256(b"expired Root action"),
            None,
            ScopeSet::empty(),
            Some(due),
        ),
    ])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let (append, intent) = Box::pin(prepare_v3(
        &store,
        planned(&graph, admission.checkpoint(), &result),
        &fence,
    ))
    .await;
    let before = snapshot(&db, &fence).await;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert!(clock(&db).await >= due.unix_micros());
    let outcome = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    assert!(matches!(
        outcome,
        Err(StoreError::InvalidWaitRegistrationBatch)
    ));
    assert_eq!(snapshot(&db, &fence).await, before);
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    assert_eq!(current.lease().unwrap().fence(), &fence);
    assert!(!current.is_quarantined());
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn elapsed_root_missing_inventory_or_changed_positive_source_blocks_read_and_ack() {
    Box::pin(elapsed_root_original_corruption()).await;
}

async fn elapsed_root_original_corruption() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-v3-original-corruption")).await;
    let waits = NodeWaits::try_new([NodeWait::timer(
        TimerId::generate(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(clock(&db).await - 1).unwrap(),
    )])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let (append, intent) = Box::pin(prepare_v3(
        &store,
        planned(&graph, admission.checkpoint(), &result),
        &fence,
    ))
    .await;
    let record = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    assert_eq!(record.wait_effect(), Some(GraphWaitEffect::Ready));
    let original: Vec<u8> = query_scalar(
        "SELECT bundle_bytes FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&original).unwrap();
    for field in [
        "wait_inventory",
        "elapsed_agent_source",
        "admission_digest",
        "condition_checksum",
    ] {
        let mut changed = wire.clone();
        match field {
            "wait_inventory" => {
                changed.as_object_mut().unwrap().remove(field).unwrap();
            }
            "elapsed_agent_source" => {
                changed["intent"]
                    .as_object_mut()
                    .unwrap()
                    .remove(field)
                    .unwrap();
            }
            "admission_digest" => {
                changed["intent"]["elapsed_agent_source"][field] =
                    json!(Digest::sha256(b"changed original admission"));
            }
            _ => {
                changed["wait_inventory"]["conditions"][0]["checksum"] =
                    json!(Digest::sha256(b"changed original condition"));
            }
        }
        let changed = serde_json_canonicalizer::to_vec(&changed).unwrap();
        assert_ne!(changed, original);
        replace_bundle(&db, &admission.checkpoint().head(), &changed).await;
        let before = snapshot(&db, &fence).await;
        assert!(matches!(
            store
                .load_root_graph_barrier(&admission.checkpoint().head())
                .await,
            Err(StoreError::CorruptData { .. })
        ));
        assert!(matches!(
            store.recover_worker_graph_barrier(&append, &intent).await,
            Err(StoreError::CorruptData { .. })
        ));
        assert_eq!(snapshot(&db, &fence).await, before);
        replace_bundle(&db, &admission.checkpoint().head(), &original).await;
    }
    assert_eq!(
        store
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await
            .unwrap()
            .unwrap()
            .digest(),
        record.digest()
    );
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_ready_checks_original_expiry_after_the_actual_deferred_guard() {
    Box::pin(root_ready_original_expiry()).await;
}

#[allow(clippy::too_many_lines)]
async fn root_ready_original_expiry() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(preparation_store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let (admission, graph, first) =
        Box::pin(admitted(&preparation_store, "root-v3-ready-expiry")).await;
    let waits = NodeWaits::try_new([NodeWait::timer(
        TimerId::generate(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(clock(&db).await - 1).unwrap(),
    )])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &preparation_store,
        admission.checkpoint(),
        &first,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let (original_append, intent) = Box::pin(prepare_v3(
        &preparation_store,
        planned(&graph, admission.checkpoint(), &result),
        &first,
    ))
    .await;
    preparation_store.close().await;
    let store = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        test_options(std::time::Duration::from_secs(2)),
    )
    .await
    .unwrap();
    query("CREATE SEQUENCE public.test_root_ready_expiry_witness")
        .execute(&db)
        .await
        .unwrap();
    query("CREATE FUNCTION public.test_root_ready_expiry_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); PERFORM nextval('public.test_root_ready_expiry_witness'); RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_root_ready_expiry_delay AFTER INSERT ON stateknot.root_graph_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.test_root_ready_expiry_delay()").execute(&db).await.unwrap();
    let lease = store
        .supersede_lease(first.tenant_id(), first.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let append = JournalAppend::new(
        original_append.expectation().clone(),
        JournalEventIntent::worker(
            first.tenant_id().clone(),
            first.run_id(),
            original_append.intent().event_id(),
            lease.fence().clone(),
            intent.payload().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let before = snapshot(&db, lease.fence()).await;
    let outcome = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    let witness: bool = query_scalar("SELECT is_called FROM public.test_root_ready_expiry_witness")
        .fetch_one(&db)
        .await
        .unwrap();
    for sql in [
        "DROP TRIGGER test_root_ready_expiry_delay ON stateknot.root_graph_barriers",
        "DROP FUNCTION public.test_root_ready_expiry_delay()",
        "DROP SEQUENCE public.test_root_ready_expiry_witness",
    ] {
        query(sql).execute(&db).await.unwrap();
    }
    assert!(
        witness,
        "the original transaction must reach the real deferred delay"
    );
    assert!(clock(&db).await > lease.expires_at().unix_micros());
    assert!(
        matches!(outcome, Err(StoreError::LeaseExpired)),
        "{outcome:?}"
    );
    assert_eq!(snapshot(&db, lease.fence()).await, before);
    let current = store
        .load_run(first.tenant_id(), first.run_id())
        .await
        .unwrap();
    assert_eq!(current.lease(), Some(&lease));
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    assert!(!current.is_quarantined());
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_ready_reloads_actual_successor_bytes_before_returning_the_receipt() {
    Box::pin(root_ready_actual_successor()).await;
}

async fn root_ready_actual_successor() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-v3-actual-successor")).await;
    let waits = NodeWaits::try_new([NodeWait::timer(
        TimerId::generate(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(clock(&db).await - 1).unwrap(),
    )])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let (append, intent) = Box::pin(prepare_v3(
        &store,
        planned(&graph, admission.checkpoint(), &result),
        &fence,
    ))
    .await;
    query("CREATE SEQUENCE public.test_root_checkpoint_bytes_witness")
        .execute(&db)
        .await
        .unwrap();
    // PostgreSQL JSON projection guards still see the same complete object.
    // A real owning read must reject the substituted noncanonical disk bytes.
    query("CREATE FUNCTION public.test_root_checkpoint_bytes() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.graph_namespace='' AND NEW.parent_checkpoint_id IS NOT NULL THEN NEW.checkpoint_bytes:=NEW.checkpoint_bytes||convert_to(' ','UTF8'); PERFORM nextval('public.test_root_checkpoint_bytes_witness'); END IF; RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE TRIGGER test_root_checkpoint_bytes BEFORE INSERT ON stateknot.run_checkpoints FOR EACH ROW EXECUTE FUNCTION public.test_root_checkpoint_bytes()").execute(&db).await.unwrap();
    let before = snapshot(&db, &fence).await;
    let outcome = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    let witness: bool =
        query_scalar("SELECT is_called FROM public.test_root_checkpoint_bytes_witness")
            .fetch_one(&db)
            .await
            .unwrap();
    for sql in [
        "DROP TRIGGER test_root_checkpoint_bytes ON stateknot.run_checkpoints",
        "DROP FUNCTION public.test_root_checkpoint_bytes()",
        "DROP SEQUENCE public.test_root_checkpoint_bytes_witness",
    ] {
        query(sql).execute(&db).await.unwrap();
    }
    assert!(
        witness,
        "the original successor INSERT must reach the real substitution"
    );
    assert!(
        matches!(outcome, Err(StoreError::CorruptData { .. })),
        "{outcome:?}"
    );
    assert_eq!(snapshot(&db, &fence).await, before);
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.lease().unwrap().fence(), &fence);
    assert!(!current.is_quarantined());
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}
