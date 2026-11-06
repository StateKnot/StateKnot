// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Fresh elapsed Wait commits authenticate the durable final transcript.

use super::*;

#[tokio::test]
async fn substituted_canonical_scoped_wait_wire_rolls_back_the_complete_commit() {
    Box::pin(substituted_canonical_scoped_wait_wire()).await;
}

async fn substituted_canonical_scoped_wait_wire() {
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
        Box::pin(prepared(&store, "fresh-elapsed-wire-substitution", waits)).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    install_wire_substitution(&db).await;
    let before = complete_snapshot(&db).await;
    let outcome = Box::pin(store.commit_graph_frame_wait(
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
    .await;
    remove_wire_substitution(&db).await;
    let after = complete_snapshot(&db).await;
    println!("SCOPED_CANONICAL_WIRE_FAULT_OUTCOME={outcome:?}");
    assert!(matches!(outcome, Err(StoreError::CorruptData { .. })));
    assert_eq!(before, after, "all durable business rows must roll back");
    let actual = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(actual.lifecycle().status(), RunStatus::Active);
    assert_eq!(actual.lease().unwrap().fence(), &fence);
    assert!(!actual.is_quarantined());
    assert!(matches!(
        store
            .load_durable_timer(fence.tenant_id(), fence.run_id(), timer)
            .await,
        Err(StoreError::WaitRegistrationNotFound)
    ));
    println!(
        "SCOPED_CANONICAL_WIRE_FAULT_RECEIPT=original_due_preserved_full_rollback_no_quarantine"
    );
    assert_original_wait_succeeds(&store, &entry, &graph, &fence, plan, observed, revision).await;
    store.release_lease(&fence).await.unwrap();
    db.close().await;
    store.close().await;
}

async fn assert_original_wait_succeeds(
    store: &PostgresStore,
    entry: &stateknot_store_postgres::StoredGraphFrameEntry,
    graph: &CompiledGraph,
    fence: &RunFence,
    plan: GraphFrameBarrierPlan,
    observed: JournalHead,
    revision: RunRevision,
) {
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
        panic!("original Ready succeeds once the mutation is removed");
    };
    assert_eq!(
        saved.wait_effect(),
        Some(stateknot_store_postgres::GraphWaitEffect::Ready)
    );
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
        panic!("actual original whole Ready ACK");
    };
    assert_eq!(saved.digest(), ack.digest());
    assert_eq!(saved.event(), ack.event());
    assert_eq!(saved.checkpoint(), ack.checkpoint());
    println!(
        "SCOPED_ORIGINAL_READY_AFTER_FAULT=complete_fresh_then_exact_old_ack_without_callback"
    );
}

async fn complete_snapshot(db: &PgPool) -> Vec<(String, Option<String>)> {
    let tables: Vec<String> = query_scalar(
        "SELECT quote_ident(tablename) FROM pg_tables WHERE schemaname='stateknot' ORDER BY tablename",
    )
    .fetch_all(db)
    .await
    .unwrap();
    let mut rows = Vec::new();
    for table in tables {
        let bytes = snapshot_table(db, &table).await;
        rows.push((table, bytes));
    }
    rows
}

async fn snapshot_table(db: &PgPool, table: &str) -> Option<String> {
    query_scalar(&format!(
        "SELECT jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text)::text FROM stateknot.{table} t"
    ))
    .fetch_one(db)
    .await
    .unwrap()
}

async fn install_wire_substitution(db: &PgPool) {
    query(r#"CREATE FUNCTION public.test_scoped_canonical(data jsonb) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE value text;
    BEGIN
      CASE jsonb_typeof(data)
      WHEN 'object' THEN SELECT '{'||coalesce(string_agg(to_json(key)::text||':'||public.test_scoped_canonical(item),',' ORDER BY key COLLATE "C"),'')||'}' INTO value FROM jsonb_each(data) x(key,item);
      WHEN 'array' THEN SELECT '['||coalesce(string_agg(public.test_scoped_canonical(item),',' ORDER BY ordinal),'')||']' INTO value FROM jsonb_array_elements(data) WITH ORDINALITY x(item,ordinal);
      ELSE value:=data::text;
      END CASE;
      RETURN value;
    END $$"#).execute(db).await.unwrap();
    query(r"CREATE FUNCTION public.test_scoped_wire_substitution() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE original jsonb; changed bytea;
    BEGIN
      original:=convert_from(NEW.barrier_bytes,'UTF8')::jsonb;
      IF original->>'version'<>'3' OR original->>'scope_intent_digest' IS NULL
         OR NEW.barrier_bytes<>convert_to(public.test_scoped_canonical(original),'UTF8') THEN
        RAISE EXCEPTION 'fault fixture requires actual canonical V3 scope intent';
      END IF;
      changed:=convert_to(public.test_scoped_canonical(jsonb_set(original,'{scope_intent_digest}',to_jsonb(repeat('0',64)))),'UTF8');
      IF changed=NEW.barrier_bytes THEN RAISE EXCEPTION 'fault fixture must change actual bytes'; END IF;
      NEW.barrier_bytes:=changed;
      RETURN NEW;
    END $$").execute(db).await.unwrap();
    query("CREATE TRIGGER test_scoped_wire_substitution BEFORE INSERT ON stateknot.graph_frame_barriers FOR EACH ROW EXECUTE FUNCTION public.test_scoped_wire_substitution()")
        .execute(db).await.unwrap();
}

async fn remove_wire_substitution(db: &PgPool) {
    for sql in [
        "DROP TRIGGER test_scoped_wire_substitution ON stateknot.graph_frame_barriers",
        "DROP FUNCTION public.test_scoped_wire_substitution()",
        "DROP FUNCTION public.test_scoped_canonical(jsonb)",
    ] {
        query(sql).execute(db).await.unwrap();
    }
}

#[tokio::test]
async fn substituted_ready_lease_cannot_extend_the_original_commit_authority() {
    Box::pin(substituted_ready_lease()).await;
}

async fn substituted_ready_lease() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(preparation_store) = test_store().await else {
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
    let (_, entry, graph, fence, plan, observed, revision) = Box::pin(prepared(
        &preparation_store,
        "fresh-elapsed-lease-substitution",
        waits,
    ))
    .await;
    preparation_store.close().await;
    // The actual short lease begins at the public takeover below. Full graph
    // registration, admission, Entry and successful Pending preparation use
    // the existing ordinary fixture lease, without extending the tested lease.
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let store = PostgresStore::connect(&database_url, test_options(Duration::from_secs(2)))
        .await
        .expect("the original two-second lease fixture must connect exactly");
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Install faults and capture every table before the original short lease.
    // Public Active-to-Active takeover writes only Run lease/queue metadata;
    // refresh the entire actual Run table after its committed outcome.
    install_lease_substitution(&db).await;
    let mut before = complete_snapshot(&db).await;
    let original_lease = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let fence = original_lease.fence().clone();
    let original_expiry = original_lease.expires_at();
    let runs = before
        .iter_mut()
        .find(|(table, _)| table == "runs")
        .expect("complete snapshot must include the entire runs table");
    runs.1 = snapshot_table(&db, "runs").await;
    let outcome = Box::pin(store.commit_graph_frame_wait(
        plan,
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
    let trigger_witness: i64 = query_scalar(
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.test_scoped_ready_lease_witness",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    remove_lease_substitution(&db).await;
    let final_now: i64 =
        query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
            .fetch_one(&db)
            .await
            .unwrap();
    assert!(final_now > original_expiry.unix_micros());
    println!("SCOPED_READY_ORIGINAL_LEASE_FAULT_OUTCOME={outcome:?}");
    assert_eq!(
        trigger_witness, 2,
        "expiry mutation and actual deferred delay ran"
    );
    assert!(matches!(
        outcome,
        Err(StoreError::CorruptData {
            record: "fresh frame Wait Run projection"
        })
    ));
    assert_eq!(before, complete_snapshot(&db).await);
    let actual = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(actual.lease(), Some(&original_lease));
    assert_eq!(actual.lifecycle().status(), RunStatus::Active);
    assert!(!actual.is_quarantined());
    println!(
        "SCOPED_READY_ORIGINAL_LEASE_FAULT_RECEIPT={}",
        json!({"original_due":due,"original_expiry":original_expiry,"final_database_clock":final_now,"original_lease_seconds":2,"deferred_delay_seconds":2.1,"actual_trigger_witness":trigger_witness,"complete_rollback":true,"no_quarantine":true})
    );
    db.close().await;
    store.close().await;
}

async fn install_lease_substitution(db: &PgPool) {
    // Sequences retain a fixture witness through business-transaction rollback.
    // They are outside the complete StateKnot business-table snapshot.
    query("CREATE SEQUENCE public.test_scoped_ready_lease_witness")
        .execute(db)
        .await
        .unwrap();
    query("CREATE FUNCTION public.test_scoped_ready_lease_substitution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.journal_sequence>OLD.journal_sequence AND NEW.lifecycle_status='active' AND NEW.lease_attempt_id=OLD.lease_attempt_id THEN NEW.lease_expires_at:=OLD.lease_expires_at+interval '30 seconds'; PERFORM nextval('public.test_scoped_ready_lease_witness'); END IF; RETURN NEW; END $$")
        .execute(db).await.unwrap();
    query("CREATE TRIGGER test_scoped_ready_lease_substitution BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION public.test_scoped_ready_lease_substitution()")
        .execute(db).await.unwrap();
    query("CREATE FUNCTION public.test_scoped_ready_lease_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); PERFORM nextval('public.test_scoped_ready_lease_witness'); RETURN NEW; END $$")
        .execute(db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_scoped_ready_lease_delay AFTER INSERT ON stateknot.graph_frame_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.test_scoped_ready_lease_delay()")
        .execute(db).await.unwrap();
}

async fn remove_lease_substitution(db: &PgPool) {
    for sql in [
        "DROP TRIGGER test_scoped_ready_lease_delay ON stateknot.graph_frame_barriers",
        "DROP FUNCTION public.test_scoped_ready_lease_delay()",
        "DROP TRIGGER test_scoped_ready_lease_substitution ON stateknot.runs",
        "DROP FUNCTION public.test_scoped_ready_lease_substitution()",
        "DROP SEQUENCE public.test_scoped_ready_lease_witness",
    ] {
        query(sql).execute(db).await.unwrap();
    }
}
