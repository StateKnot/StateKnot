// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Current-source V1 reconstruction, not a historical-release executable.
use super::*;
use sqlx_core::{connection::ConnectOptions, raw_sql::raw_sql};
use sqlx_postgres::{PgConnectOptions, PgSslMode};

const V1: &str = include_str!("../../../migrations/0034_root_graph_barriers.sql");

struct Fixture {
    admin: PgPool,
    db: PgPool,
    name: String,
    url: String,
    options: PostgresStoreOptions,
}

impl Fixture {
    async fn create(options: PostgresStoreOptions) -> Self {
        let base: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
        assert!(matches!(base.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let base = base.ssl_mode(PgSslMode::Disable);
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base.clone().database("postgres"))
            .await
            .unwrap();
        let name = format!(
            "stateknot_v35_{}",
            RunId::generate().to_string().replace('-', "")
        );
        query(&format!("CREATE DATABASE {name} TEMPLATE template0"))
            .execute(&admin)
            .await
            .unwrap();
        let connect = base.database(&name);
        let db = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(connect.clone())
            .await
            .unwrap();
        assert_eq!(
            query_scalar::<_, i64>("SELECT count(*) FROM pg_tables WHERE schemaname='stateknot'")
                .fetch_one(&db)
                .await
                .unwrap(),
            0
        );
        let url = connect.to_url_lossy().to_string();
        PostgresStore::migrate_database(&url, options.clone())
            .await
            .unwrap();
        query("SET TimeZone='UTC'").execute(&db).await.unwrap();
        Self {
            admin,
            db,
            name,
            url,
            options,
        }
    }

    async fn restore_v35(&self) {
        raw_sql(include_str!(
            "../../fixtures/revert_elapsed_graph_waits.sql"
        ))
        .execute(&self.db)
        .await
        .unwrap();
        assert_eq!(
            query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
                .fetch_one(&self.db)
                .await
                .unwrap(),
            35
        );
    }

    async fn restore_v1(&self) {
        self.restore_v35().await;
        for name in [
            "guard_root_graph_barrier_complete",
            "guard_root_graph_barrier_projection",
        ] {
            let start = V1
                .find(&format!("CREATE FUNCTION stateknot.{name}()"))
                .unwrap();
            let end = V1[start..].find("$$;").unwrap() + start + 3;
            let original =
                V1[start..end].replacen("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1);
            raw_sql(&original).execute(&self.db).await.unwrap();
        }
        let start = V1
            .find("CONSTRAINT root_graph_barriers_shape CHECK (")
            .unwrap();
        let end = V1[start..].find("\n ),").unwrap() + start + 3;
        query(
            "ALTER TABLE stateknot.root_graph_barriers DROP CONSTRAINT root_graph_barriers_shape",
        )
        .execute(&self.db)
        .await
        .unwrap();
        query(&format!(
            "ALTER TABLE stateknot.root_graph_barriers ADD {}",
            &V1[start..end]
        ))
        .execute(&self.db)
        .await
        .unwrap();
        query("DELETE FROM public._sqlx_migrations WHERE version=35")
            .execute(&self.db)
            .await
            .unwrap();
        assert_eq!(
            query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
                .fetch_one(&self.db)
                .await
                .unwrap(),
            34
        );
    }

    async fn cleanup(self) {
        self.db.close().await;
        query(&format!("DROP DATABASE {}", self.name))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

struct Existing {
    graph: CompiledGraph,
    append: JournalAppend,
    intent: RootGraphBarrierIntent,
    digest: Digest,
}

async fn continue_terminal(store: &PostgresStore, db: &PgPool) -> Vec<Existing> {
    let (admission, graph, fence) = Box::pin(admitted(store, "v35-legacy-terminal")).await;
    let (first, _) = Box::pin(succeed(
        store,
        admission.checkpoint(),
        &fence,
        NodeControl::Continue,
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &first);
    let (append, intent) = Box::pin(prepared(store, &graph, plan, &fence, None)).await;
    let continued = Box::pin(commit(store, &graph, append.clone(), intent.clone())).await;
    let first = Existing {
        graph: graph.clone(),
        append,
        intent,
        digest: continued.digest(),
    };
    let output = NodeTerminalOutput::new(
        graph.output_schema().clone(),
        BoundedJson::try_from(json!({"summary":"actual v1 terminal"})).unwrap(),
    )
    .unwrap();
    let (last, model) = Box::pin(succeed(
        store,
        continued.checkpoint(),
        &fence,
        NodeControl::Terminal {
            output: output.clone(),
        },
        true,
    ))
    .await;
    let result = Box::pin(result_with_pool(
        store, db, &admission, &last, &output, &model,
    ))
    .await;
    let plan = planned(&graph, continued.checkpoint(), &last);
    let (append, intent) = Box::pin(prepared(store, &graph, plan, &fence, Some(result))).await;
    let terminal = Box::pin(commit(store, &graph, append.clone(), intent.clone())).await;
    vec![
        first,
        Existing {
            graph,
            append,
            intent,
            digest: terminal.digest(),
        },
    ]
}

async fn wait(store: &PostgresStore) -> Existing {
    let (admission, graph, fence) = Box::pin(admitted(store, "v35-legacy-wait")).await;
    let (result, _) = Box::pin(succeed(
        store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait {
            waits: NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap(),
        },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(store, &graph, plan, &fence, None)).await;
    let saved = Box::pin(commit(store, &graph, append.clone(), intent.clone())).await;
    Existing {
        graph,
        append,
        intent,
        digest: saved.digest(),
    }
}

async fn result_with_pool(
    store: &PostgresStore,
    db: &PgPool,
    admission: &StoredAgentAdmission,
    last: &PendingNodeResult,
    output: &NodeTerminalOutput,
    model: &BudgetUsage,
) -> AgentResult {
    let source = admission.admission().intent();
    let owner = source.provenance();
    let journal = store
        .load_journal_page(
            owner.tenant_id(),
            owner.run_id(),
            None,
            JournalPageSize::new(128).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    let event_bytes = journal
        .events()
        .iter()
        .map(|e| u64::try_from(serde_json_canonicalizer::to_vec(e).unwrap().len()).unwrap())
        .sum();
    let checkpoint_bytes: i64 = query_scalar("SELECT sum(octet_length(checkpoint_bytes))::bigint FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2").bind(owner.tenant_id().as_str()).bind(*owner.run_id().as_uuid()).fetch_one(db).await.unwrap();
    let usage = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(1))
        .graph_steps(ExecutionCount::new(2))
        .model_attempts(model.model_attempts())
        .model_turns(model.model_turns())
        .input_tokens(model.input_tokens())
        .cached_input_tokens(model.cached_input_tokens())
        .output_tokens(model.output_tokens())
        .reasoning_tokens(model.reasoning_tokens())
        .known_costs(model.known_costs().clone())
        .input_bytes(ByteCount::new(
            u64::try_from(source.request().input().stats().compact_bytes()).unwrap(),
        ))
        .output_bytes(ByteCount::new(
            u64::try_from(output.data().stats().compact_bytes()).unwrap(),
        ))
        .event_bytes(ByteCount::new(event_bytes))
        .checkpoint_bytes(ByteCount::new(u64::try_from(checkpoint_bytes).unwrap()))
        .build()
        .unwrap();
    let result = AgentResult::new(
        owner.clone(),
        last.journal_head().recorded_at(),
        output.schema().clone(),
        output.data().clone(),
        AgentArtifacts::empty(),
        usage,
    )
    .unwrap();
    result
        .validate_for(
            owner,
            source.request(),
            source.descriptor(),
            source.budget(),
        )
        .unwrap();
    result
}

async fn facts(db: &PgPool) -> Vec<(String, i64, String)> {
    let names = query_scalar::<_, String>(
        "SELECT tablename FROM pg_tables WHERE schemaname='stateknot' AND tablename<>'graph_wait_conditions' ORDER BY tablename",
    )
    .fetch_all(db)
    .await
    .unwrap();
    let mut values = Vec::new();
    for name in names {
        let quoted = name.replace('"', "\"\"");
        let sql = format!(
            "SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(hash,'' ORDER BY hash),''),'UTF8')),'hex') FROM (SELECT encode(sha256(convert_to(row_to_json(t)::text,'UTF8')),'hex') hash FROM stateknot.\"{quoted}\" t) rows"
        );
        let (count, digest) = query_as(&sql).fetch_one(db).await.unwrap();
        values.push((name, count, digest));
    }
    values
}

async fn checksums(db: &PgPool) -> Vec<(i64, Vec<u8>, bool)> {
    query_as("SELECT version,checksum,success FROM public._sqlx_migrations WHERE version<=34 ORDER BY version").fetch_all(db).await.unwrap()
}

async fn function_owners(db: &PgPool) -> Vec<(String, i64, i64, Option<String>)> {
    query_as("SELECT proname,oid::bigint,proowner::bigint,proacl::text FROM pg_proc WHERE pronamespace='stateknot'::regnamespace AND proname LIKE 'guard_root_graph_barrier_%' ORDER BY proname").fetch_all(db).await.unwrap()
}

#[tokio::test]
async fn populated_source34_upgrade_preserves_full_v1_and_lost_ack_before_callbacks() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let fixture = Fixture::create(options).await;
    let store = PostgresStore::connect(&fixture.url, fixture.options.clone())
        .await
        .unwrap();
    let mut existing = Box::pin(continue_terminal(&store, &fixture.db)).await;
    existing.push(Box::pin(wait(&store)).await);
    store.close().await;
    fixture.restore_v1().await;
    let before = facts(&fixture.db).await;
    let migrations = checksums(&fixture.db).await;
    let owners = function_owners(&fixture.db).await;
    assert_eq!(migrations.len(), 34);
    assert_eq!(owners.len(), 4);
    assert_eq!(query_scalar::<_, i64>("SELECT count(*) FROM stateknot.root_graph_barriers WHERE convert_from(bundle_bytes,'UTF8')::jsonb->>'version'='1'").fetch_one(&fixture.db).await.unwrap(), 3);
    assert!(matches!(
        PostgresStore::connect(&fixture.url, fixture.options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(&fixture.url, fixture.options.clone())
        .await
        .unwrap();
    assert_eq!(facts(&fixture.db).await, before);
    assert_eq!(checksums(&fixture.db).await, migrations);
    assert_eq!(function_owners(&fixture.db).await, owners);
    let upgraded = PostgresStore::connect(&fixture.url, fixture.options.clone())
        .await
        .unwrap();
    upgraded.verify_schema().await.unwrap();
    for item in existing {
        let base = item.intent.plan().barrier().base_checkpoint();
        let loaded = upgraded
            .load_root_graph_barrier(base)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.digest(), item.digest);
        assert_eq!(loaded.barrier(), item.intent.plan().barrier());
        assert_eq!(loaded.disposition(), item.intent.plan().disposition());
        assert!(loaded.terminal_validation_inputs().is_none());
        let retry = Box::pin(upgraded.append_worker_graph_barrier(
            item.append,
            item.intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: item.graph.reducer().clone(),
            },
        ))
        .await
        .unwrap();
        let RootGraphBarrierCommitOutcome::Idempotent(recovered) = retry else {
            panic!("historical ACK must recover before unavailable callbacks")
        };
        assert_eq!(recovered.digest(), item.digest);
    }
    assert_eq!(facts(&fixture.db).await, before);
    upgraded.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn populated_source35_upgrade_preserves_registration_bytes_and_legacy_identity() {
    Box::pin(populated_source35_upgrade()).await;
}

#[allow(clippy::too_many_lines)]
async fn populated_source35_upgrade() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let fixture = Fixture::create(options).await;
    let store = PostgresStore::connect(&fixture.url, fixture.options.clone())
        .await
        .unwrap();
    // The public legacy constructor intentionally emits V1 bytes. Their
    // original registration facts are valid and remain byte-for-byte retained.
    let mut existing = Box::pin(continue_terminal(&store, &fixture.db)).await;
    existing.push(Box::pin(wait(&store)).await);
    store.close().await;
    fixture.restore_v35().await;
    let before = facts(&fixture.db).await;
    let migrations: Vec<(i64, Vec<u8>, bool)> =
        query_as("SELECT version,checksum,success FROM public._sqlx_migrations ORDER BY version")
            .fetch_all(&fixture.db)
            .await
            .unwrap();
    assert_eq!(migrations.len(), 35);
    assert!(migrations.iter().all(|(_, _, success)| *success));
    let registration_count: i64 =
        query_scalar("SELECT count(*) FROM stateknot.run_wait_registrations")
            .fetch_one(&fixture.db)
            .await
            .unwrap();
    assert!(registration_count > 0);
    assert!(matches!(
        PostgresStore::connect(&fixture.url, fixture.options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(&fixture.url, fixture.options.clone())
        .await
        .unwrap();
    assert_eq!(facts(&fixture.db).await, before);
    let retained: Vec<(i64, Vec<u8>, bool)> = query_as(
        "SELECT version,checksum,success FROM public._sqlx_migrations WHERE version<=35 ORDER BY version")
        .fetch_all(&fixture.db).await.unwrap();
    assert_eq!(retained, migrations);
    let legacy_count: i64 = query_scalar(
        "SELECT count(*) FROM stateknot.graph_wait_conditions WHERE profile='legacy_registration'",
    )
    .fetch_one(&fixture.db)
    .await
    .unwrap();
    assert_eq!(legacy_count, registration_count);
    let mismatches: i64 = query_scalar("SELECT count(*) FROM stateknot.graph_wait_conditions c FULL JOIN stateknot.run_wait_registrations w ON c.tenant_id=w.tenant_id AND c.wait_id=w.wait_id WHERE c.tenant_id IS NULL OR w.tenant_id IS NULL OR c.profile<>'legacy_registration' OR c.run_id<>w.run_id OR c.wait_kind<>w.wait_kind OR c.interrupt_kind IS DISTINCT FROM w.interrupt_kind OR c.timer_kind IS DISTINCT FROM w.timer_kind OR c.due_at IS DISTINCT FROM w.due_at OR c.expires_at IS DISTINCT FROM w.expires_at OR c.intent_digest<>w.intent_digest OR c.registration_digest<>w.record_digest OR c.journal_sequence<>w.registration_sequence OR c.journal_event_id<>w.registration_event_id OR c.journal_recorded_at<>w.registered_at OR c.journal_digest<>w.registration_event_digest OR c.graph_namespace IS NOT NULL OR c.owner_node_id IS NOT NULL OR c.condition_bytes IS NOT NULL")
        .fetch_one(&fixture.db).await.unwrap();
    assert_eq!(mismatches, 0);
    let upgraded = PostgresStore::connect(&fixture.url, fixture.options.clone())
        .await
        .unwrap();
    upgraded.verify_schema().await.unwrap();
    for item in existing {
        let base = item.intent.plan().barrier().base_checkpoint();
        let loaded = upgraded
            .load_root_graph_barrier(base)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.digest(), item.digest);
        let retry = Box::pin(upgraded.append_worker_graph_barrier(
            item.append,
            item.intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: item.graph.reducer().clone(),
            },
        ))
        .await
        .unwrap();
        assert!(matches!(
            retry,
            RootGraphBarrierCommitOutcome::Idempotent(_)
        ));
    }
    assert_eq!(facts(&fixture.db).await, before);

    // Fresh original-condition evidence makes source removal invalid. It must
    // reject atomically rather than falsely recreating a schema35 fixture.
    let (admission, graph, fence) =
        Box::pin(admitted(&upgraded, "v36-refuse-source-removal")).await;
    let (result, _) = Box::pin(succeed(
        &upgraded,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait {
            waits: NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap(),
        },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let current = upgraded
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let event = EventId::generate();
    let intent = upgraded
        .prepare_worker_graph_barrier(plan, current.lifecycle().clone(), event, None, None)
        .await
        .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
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
    let saved = Box::pin(commit(&upgraded, &graph, append, intent)).await;
    let preserved = facts(&fixture.db).await;
    let mut tx = fixture.db.begin().await.unwrap();
    assert!(
        raw_sql(include_str!(
            "../../fixtures/revert_elapsed_graph_waits.sql"
        ))
        .execute(&mut *tx)
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(facts(&fixture.db).await, preserved);
    upgraded.verify_schema().await.unwrap();
    assert_eq!(
        upgraded
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await
            .unwrap()
            .unwrap()
            .digest(),
        saved.digest()
    );
    upgraded.close().await;
    fixture.cleanup().await;
}
