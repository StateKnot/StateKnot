// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Original elapsed conditions coexist with real, independently resolved waits.
use super::*;
use stateknot_core::GraphBarrierDisposition;
use stateknot_store_postgres::GraphWaitEffect;

#[tokio::test]
async fn original_condition_corruption_blocks_complete_read_and_ack_without_writes() {
    Box::pin(original_condition_corruption()).await;
}

#[allow(clippy::too_many_lines)]
async fn original_condition_corruption() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    let due = Timestamp::from_unix_micros(now + 500_000).unwrap();
    let timer = TimerId::generate();
    let waits = NodeWaits::try_new([NodeWait::timer(timer, RunTimerKind::Sleep, due)]).unwrap();
    let (_, entry, graph, fence, plan, observed, revision) =
        Box::pin(prepared(&store, "original-condition-corruption", waits)).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let GraphFrameBarrierCommitOutcome::Committed(saved) = Box::pin(store.commit_graph_frame_wait(
        plan.clone(),
        EventId::generate(),
        fence.clone(),
        observed.clone(),
        entry.direct_usage_after().unwrap(),
        revision,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap() else {
        panic!("actual elapsed Ready corpus");
    };
    assert_eq!(saved.wait_effect(), Some(GraphWaitEffect::Ready));
    let original: Vec<u8> = query_scalar("SELECT condition_bytes FROM stateknot.graph_wait_conditions WHERE tenant_id=$1 AND wait_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*timer.as_uuid()).fetch_one(&db).await.unwrap();
    let wire: Value = serde_json::from_slice(&original).unwrap();
    let forged_digest = json!(Digest::sha256(b"forged original condition").to_string());
    for (name, path, replacement) in [
        ("ordinal", vec!["ordinal"], json!(1)),
        ("owner", vec!["owner", "digest"], forged_digest.clone()),
        ("owner_wait_index", vec!["result_wait_index"], json!(1)),
        (
            "full_specification",
            vec!["specification_digest"],
            forged_digest.clone(),
        ),
        (
            "source",
            vec!["source"],
            serde_json::to_value(saved.event().head()).unwrap(),
        ),
        (
            "original_intent",
            vec!["registration_intent_digest"],
            forged_digest,
        ),
    ] {
        let mut changed = wire.clone();
        let mut slot = &mut changed;
        for key in path {
            slot = &mut slot[key];
        }
        assert!(!slot.is_null(), "replace actual condition field: {name}");
        *slot = replacement;
        let changed = serde_json_canonicalizer::to_vec(&changed).unwrap();
        assert_ne!(changed, original, "fault must change the real bytes");
        replace_condition_bytes(&db, &fence, timer, &changed).await;
        assert_bad_condition_blocks(
            &store, &db, &entry, &saved, &graph, &fence, &plan, &observed, revision,
        )
        .await;
        replace_condition_bytes(&db, &fence, timer, &original).await;
        assert_eq!(
            Box::pin(store.load_graph_frame_barrier(
                fence.tenant_id(),
                fence.run_id(),
                saved.checkpoint().frame().namespace(),
                entry.entry().checkpoint().checkpoint().checkpoint_id()
            ))
            .await
            .unwrap()
            .unwrap()
            .digest(),
            saved.digest()
        );
        println!("ORIGINAL_CONDITION_FAULT_REJECTED={name}");
    }
    let mut noncanonical = original.clone();
    noncanonical.push(b' ');
    replace_condition_bytes(&db, &fence, timer, &noncanonical).await;
    assert_bad_condition_blocks(
        &store, &db, &entry, &saved, &graph, &fence, &plan, &observed, revision,
    )
    .await;
    replace_condition_bytes(&db, &fence, timer, &original).await;
    let original_row: String = query_scalar("SELECT row_to_json(c)::text FROM stateknot.graph_wait_conditions c WHERE tenant_id=$1 AND wait_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*timer.as_uuid()).fetch_one(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_wait_conditions DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("DELETE FROM stateknot.graph_wait_conditions WHERE tenant_id=$1 AND wait_id=$2")
        .bind(fence.tenant_id().as_str())
        .bind(*timer.as_uuid())
        .execute(&db)
        .await
        .unwrap();
    query("ALTER TABLE stateknot.graph_wait_conditions ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert_bad_condition_blocks(
        &store, &db, &entry, &saved, &graph, &fence, &plan, &observed, revision,
    )
    .await;
    let columns: String = query_scalar("SELECT string_agg(quote_ident(attname),',' ORDER BY attnum) FROM pg_attribute WHERE attrelid='stateknot.graph_wait_conditions'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated=''")
        .fetch_one(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_wait_conditions DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query(&format!("INSERT INTO stateknot.graph_wait_conditions ({columns}) SELECT {columns} FROM jsonb_populate_record(NULL::stateknot.graph_wait_conditions,$1::jsonb)"))
        .bind(original_row).execute(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_wait_conditions ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(
        Box::pin(store.load_graph_frame_barrier(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            entry.entry().checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        saved.digest()
    );
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(!run.is_quarantined());
    assert_eq!(run.lifecycle().status(), RunStatus::Active);
    assert_eq!(run.lease().unwrap().fence(), &fence);
    assert!(matches!(
        store
            .load_durable_timer(fence.tenant_id(), fence.run_id(), timer)
            .await,
        Err(StoreError::WaitRegistrationNotFound)
    ));
    store.verify_schema().await.unwrap();
    println!("ORIGINAL_CONDITION_CORPUS_VALID_AFTER_RESTORE=8_faults_no_write_no_quarantine");
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}

async fn replace_condition_bytes(db: &PgPool, fence: &RunFence, timer: TimerId, bytes: &[u8]) {
    query("ALTER TABLE stateknot.graph_wait_conditions DISABLE TRIGGER USER")
        .execute(db)
        .await
        .unwrap();
    query("UPDATE stateknot.graph_wait_conditions SET condition_bytes=$3 WHERE tenant_id=$1 AND wait_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*timer.as_uuid()).bind(bytes).execute(db).await.unwrap();
    query("ALTER TABLE stateknot.graph_wait_conditions ENABLE TRIGGER USER")
        .execute(db)
        .await
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn assert_bad_condition_blocks(
    store: &PostgresStore,
    db: &PgPool,
    entry: &stateknot_store_postgres::StoredGraphFrameEntry,
    saved: &StoredGraphFrameBarrier,
    graph: &CompiledGraph,
    fence: &RunFence,
    plan: &GraphFrameBarrierPlan,
    observed: &JournalHead,
    revision: RunRevision,
) {
    let before = waits_snapshot(db, fence).await;
    let condition_before:Option<String>=query_scalar("SELECT row_to_json(c)::text FROM stateknot.graph_wait_conditions c WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_optional(db).await.unwrap();
    assert!(matches!(
        Box::pin(store.load_graph_frame_barrier(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            entry.entry().checkpoint().checkpoint().checkpoint_id()
        ))
        .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        Box::pin(store.load_graph_frame_checkpoint(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            saved.checkpoint().checkpoint().checkpoint_id()
        ))
        .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        Box::pin(store.commit_graph_frame_wait(
            plan.clone(),
            EventId::generate(),
            fence.clone(),
            observed.clone(),
            entry.direct_usage_after().unwrap(),
            revision,
            &NoSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert_eq!(waits_snapshot(db, fence).await, before);
    let condition_after:Option<String>=query_scalar("SELECT row_to_json(c)::text FROM stateknot.graph_wait_conditions c WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_optional(db).await.unwrap();
    assert_eq!(condition_before, condition_after);
}

#[tokio::test]
async fn maximum_original_wait_inventory_retains_all_sixty_four_real_owners() {
    Box::pin(maximum_original_wait_inventory()).await;
}

#[allow(clippy::too_many_lines)]
async fn maximum_original_wait_inventory() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    let due = Timestamp::from_unix_micros(now + 500_000).unwrap();
    let original = NodeWaits::try_new(
        (0..NodeWaits::MAX_LEN)
            .map(|_| NodeWait::timer(TimerId::generate(), RunTimerKind::Sleep, due)),
    )
    .unwrap();
    let (_, entry, graph, fence, plan, observed, revision) = Box::pin(prepared(
        &store,
        "maximum-original-wait-inventory",
        original.clone(),
    ))
    .await;
    let activation = plan.barrier().result_heads()[0].activation().clone();
    let original_pending = serde_json_canonicalizer::to_vec(
        &store.load_pending_node_result(&activation).await.unwrap(),
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let GraphFrameBarrierCommitOutcome::Committed(saved) = Box::pin(store.commit_graph_frame_wait(
        plan.clone(),
        EventId::generate(),
        fence.clone(),
        observed.clone(),
        entry.direct_usage_after().unwrap(),
        revision,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap() else {
        panic!("actual maximum inventory commit");
    };
    assert_eq!(saved.wait_effect(), Some(GraphWaitEffect::Ready));
    assert_eq!(
        saved.disposition(),
        GraphBarrierDisposition::Wait { waits: original }
    );
    assert!(saved.waits().unwrap().unwrap().is_empty());
    let ordinals:Vec<(i16,i16)>=query_as("SELECT condition_ordinal,result_wait_index FROM stateknot.graph_wait_conditions WHERE tenant_id=$1 AND run_id=$2 ORDER BY condition_ordinal").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_all(&db).await.unwrap();
    assert_eq!(ordinals, (0..64).map(|i| (i, i)).collect::<Vec<_>>());
    let registrations:i64=query_scalar("SELECT count(*)::bigint FROM stateknot.run_wait_registrations WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(registrations, 0);
    let GraphFrameBarrierCommitOutcome::Idempotent(ack) = Box::pin(store.commit_graph_frame_wait(
        plan,
        EventId::generate(),
        fence.clone(),
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &NoSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap() else {
        panic!("maximum inventory exact ACK");
    };
    assert_eq!(ack.digest(), saved.digest());
    assert_eq!(ack.disposition(), saved.disposition());
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store.load_pending_node_result(&activation).await.unwrap()
        )
        .unwrap(),
        original_pending
    );
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(!current.is_quarantined());
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    assert_eq!(current.lease().unwrap().fence(), &fence);
    println!("MAXIMUM_ORIGINAL_WAIT_INVENTORY=64_actual_elapsed_0_registrations_exact_ack");
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn mixed_original_conditions_release_only_the_real_remaining_registrations() {
    Box::pin(mixed_original_conditions()).await;
}

#[allow(clippy::too_many_lines)]
async fn mixed_original_conditions() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    let elapsed_id = TimerId::generate();
    let future_id = TimerId::generate();
    let interrupt_id = InterruptId::generate();
    let elapsed_due = Timestamp::from_unix_micros(now + 500_000).unwrap();
    let future_due = Timestamp::from_unix_micros(now + 30_000_000).unwrap();
    let waits = NodeWaits::try_new([
        NodeWait::timer(elapsed_id, RunTimerKind::Sleep, elapsed_due),
        interrupt(interrupt_id),
        NodeWait::timer(future_id, RunTimerKind::Sleep, future_due),
    ])
    .unwrap();
    let (_, entry, graph, fence, plan, observed, revision) =
        Box::pin(prepared(&store, "mixed-original-conditions", waits.clone())).await;
    let activation = plan.barrier().result_heads()[0].activation().clone();
    let pending = store.load_pending_node_result(&activation).await.unwrap();
    let pending_bytes = serde_json_canonicalizer::to_vec(&pending).unwrap();
    let delay = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(delay.elapsed() >= Duration::from_millis(600));
    let event_id = EventId::generate();
    let GraphFrameBarrierCommitOutcome::Committed(saved) = Box::pin(store.commit_graph_frame_wait(
        plan.clone(),
        event_id,
        fence.clone(),
        observed.clone(),
        entry.direct_usage_after().unwrap(),
        revision,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap() else {
        panic!("fresh mixed whole wait")
    };
    assert_eq!(saved.wait_effect(), Some(GraphWaitEffect::Waiting));
    assert_eq!(saved.disposition(), GraphBarrierDisposition::Wait { waits });
    assert!(saved.event().recorded_at() >= elapsed_due);
    assert!(saved.event().recorded_at() < future_due);
    assert_eq!(saved.waits().unwrap().unwrap().len(), 2);
    let metadata: Vec<(String,i64)> = query_as(
        "SELECT profile,count(*)::bigint FROM stateknot.graph_wait_conditions WHERE tenant_id=$1 AND run_id=$2 GROUP BY profile ORDER BY profile"
    ).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_all(&db).await.unwrap();
    assert_eq!(
        metadata,
        vec![("graph_elapsed".into(), 1), ("graph_registered".into(), 2)]
    );
    assert!(matches!(
        store
            .load_durable_timer(fence.tenant_id(), fence.run_id(), elapsed_id)
            .await,
        Err(StoreError::WaitRegistrationNotFound)
    ));
    let waiting = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(waiting.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(waiting.unresolved_wait_count(), 2);
    assert!(waiting.lease().is_none());
    assert!(!waiting.is_quarantined());
    let request = store
        .load_interrupt_request(fence.tenant_id(), fence.run_id(), interrupt_id)
        .await
        .unwrap();
    assert!(
        InterruptResolutionIntent::new(
            &request,
            EventId::generate(),
            payload(96_001),
            InterruptResolver::new(principal(), ScopeSet::default())
        )
        .is_err()
    );
    let resolution_id = EventId::generate();
    let resolution = InterruptResolutionIntent::new(
        &request,
        resolution_id,
        payload(96_002),
        InterruptResolver::new(principal(), scopes()),
    )
    .unwrap();
    let append = control_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        resolution_id,
        JournalExpectation::exact(saved.event().head()),
        96_003,
    );
    let resolved = store
        .resolve_interrupt(
            append.clone(),
            waiting.lifecycle().revision(),
            resolution.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .resolve_interrupt(append, waiting.lifecycle().revision(), resolution)
            .await
            .unwrap()
            .record(),
        resolved.record()
    );
    let partial = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(partial.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(partial.unresolved_wait_count(), 1);
    let timer = store
        .load_durable_timer(fence.tenant_id(), fence.run_id(), future_id)
        .await
        .unwrap();
    assert_eq!(timer.intent().due_at(), future_due);
    let firing_id = EventId::generate();
    let firing = TimerFiringIntent::new(&timer, firing_id).unwrap();
    let append = control_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        firing_id,
        JournalExpectation::exact(resolved.event().head()),
        96_004,
    );
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    if now < future_due.unix_micros() {
        assert!(matches!(
            store
                .fire_timer(
                    append.clone(),
                    partial.lifecycle().revision(),
                    firing.clone()
                )
                .await,
            Err(StoreError::InvalidTimerFiring)
        ));
        tokio::time::sleep(Duration::from_micros(
            u64::try_from(future_due.unix_micros() - now + 100_000).unwrap(),
        ))
        .await;
    }
    let fired = store
        .fire_timer(
            append.clone(),
            partial.lifecycle().revision(),
            firing.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .fire_timer(append, partial.lifecycle().revision(), firing)
            .await
            .unwrap()
            .record(),
        fired.record()
    );
    let active = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(active.lifecycle().status(), RunStatus::Active);
    assert_eq!(active.unresolved_wait_count(), 0);
    assert!(active.lease().is_none());
    let newer = store
        .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(newer.epoch() > fence.epoch());
    let GraphFrameBarrierCommitOutcome::Idempotent(ack) = Box::pin(store.commit_graph_frame_wait(
        plan,
        EventId::generate(),
        fence.clone(),
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &NoSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap() else {
        panic!("old whole mixed ACK")
    };
    assert_eq!(ack.digest(), saved.digest());
    assert_eq!(ack.wait_effect(), Some(GraphWaitEffect::Waiting));
    assert_eq!(ack.waits().unwrap().unwrap().len(), 2);
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store.load_pending_node_result(&activation).await.unwrap()
        )
        .unwrap(),
        pending_bytes
    );
    let actual = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(actual.lease().unwrap().fence(), &newer);
    assert_eq!(actual.journal_head(), Some(&fired.event().head()));
    assert!(!actual.is_quarantined());
    println!(
        "MIXED_ELAPSED_WAIT_RECEIPT={}",
        serde_json::json!({"original_due":elapsed_due,"actual_event":saved.event().head(),"full_whole_digest":saved.digest(),"original_conditions":3,"original_elapsed":1,"actual_registrations":2,"partial_remaining":1,"real_firing":fired.event().head(),"original_ack_effect":"waiting","higher_fence":newer,"no_fake_elapsed_registration":true})
    );
    store.release_lease(&newer).await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn expired_original_interrupt_blocks_elapsed_ready_without_any_partial_commit() {
    Box::pin(expired_original_interrupt()).await;
}
#[allow(clippy::too_many_lines)]
async fn expired_original_interrupt() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    let due = Timestamp::from_unix_micros(now + 500_000).unwrap();
    let interrupt_id = InterruptId::generate();
    let waits = NodeWaits::try_new([
        NodeWait::timer(TimerId::generate(), RunTimerKind::Sleep, due),
        NodeWait::interrupt(
            interrupt_id,
            RunInterruptKind::Approval,
            payload(96_010),
            Digest::sha256(b"expired original action"),
            Some(principal()),
            scopes(),
            Some(due),
        ),
    ])
    .unwrap();
    let (_, entry, graph, fence, plan, observed, revision) =
        Box::pin(prepared(&store, "expired-original-interrupt", waits)).await;
    let activation = plan.barrier().result_heads()[0].activation().clone();
    let pending = serde_json_canonicalizer::to_vec(
        &store.load_pending_node_result(&activation).await.unwrap(),
    )
    .unwrap();
    let before = waits_snapshot(&db, &fence).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let outcome = Box::pin(store.commit_graph_frame_wait(
        plan.clone(),
        EventId::generate(),
        fence.clone(),
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
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
    assert_eq!(waits_snapshot(&db, &fence).await, before);
    let count: i64 = query_scalar(
        "SELECT count(*) FROM stateknot.graph_wait_conditions WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store.load_pending_node_result(&activation).await.unwrap()
        )
        .unwrap(),
        pending
    );
    let actual = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(actual.lifecycle().status(), RunStatus::Active);
    assert_eq!(actual.lease().unwrap().fence(), &fence);
    assert!(!actual.is_quarantined());
    assert!(matches!(
        store
            .load_interrupt_request(fence.tenant_id(), fence.run_id(), interrupt_id)
            .await,
        Err(StoreError::WaitRegistrationNotFound)
    ));
    println!(
        "EXPIRED_INTERRUPT_NO_PARTIAL_WAIT_RECEIPT={}",
        serde_json::json!({"original_due":due,"original_pending_unchanged":true,"original_fence":fence,"conditions":0,"no_approval":true,"no_quarantine":true})
    );
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}
