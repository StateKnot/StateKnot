// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Isolated logical backup/restore drill for a real consumed child Join.

use super::*;
use crate::commit_proxy::loopback_target;
use sqlx_core::{connection::ConnectOptions, query::query, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_core::ChildRunKey;
use stateknot_store_postgres::JournalPage;
use std::{
    io::Write,
    process::{Command, Stdio},
};

const CONTAINER_ENV: &str = "STATEKNOT_TEST_POSTGRES_CONTAINER";
const REQUIRE_ENV: &str = "STATEKNOT_REQUIRE_BACKUP_RESTORE_TESTS";

#[derive(Debug, PartialEq)]
pub(super) struct Snapshot {
    parent: Value,
    child: Value,
    ownership: Value,
    account: Value,
    join: Value,
    attempt: Value,
    result: Value,
    checkpoint: Checkpoint,
    parent_journal: JournalPage,
    child_journal: JournalPage,
}

async fn journal(store: &PostgresStore, tenant: &TenantId, run: RunId) -> JournalPage {
    let page = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(100).unwrap())
        .await
        .unwrap();
    assert!(!page.has_more(), "backup fixture journal must be complete");
    page
}

pub(super) async fn snapshot(
    store: &PostgresStore,
    key: &ChildRunKey,
    attempt_id: AttemptId,
) -> Snapshot {
    let owned = store.load_child_run(key).await.unwrap();
    let child_id = owned.child().admission().intent().provenance().run_id();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let child = store.load_run(key.tenant_id(), child_id).await.unwrap();
    let join = store.load_child_join(key.parent()).await.unwrap().unwrap();
    let attempt = store
        .load_node_attempt(key.tenant_id(), &key.parent_run_id(), attempt_id)
        .await
        .unwrap();
    let result = store.load_pending_node_result(key.parent()).await.unwrap();
    let checkpoint = store
        .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    Snapshot {
        parent: json!({
            "lifecycle": parent.lifecycle(),
            "journal_head": parent.journal_head(),
            "lease": parent.lease(),
            "checkpoint": parent.checkpoint().map(|pointer| json!({
                "id": pointer.checkpoint_id(),
                "superstep": pointer.superstep().get(),
                "digest": pointer.digest()
            }))
        }),
        child: json!({
            "lifecycle": child.lifecycle(),
            "journal_head": child.journal_head(),
            "lease": child.lease()
        }),
        ownership: json!({
            "intent": owned.intent(),
            "spawn": owned.spawn().head(),
            "ancestors": owned.ancestors(),
            "settlement": owned.settlement()
        }),
        account: serde_json::to_value(
            store
                .load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        join: json!({
            "request": join.request(),
            "head": join.head(),
            "consumed": join.consumed()
        }),
        attempt: json!(attempt),
        result: json!(result),
        checkpoint,
        parent_journal: journal(store, key.tenant_id(), key.parent_run_id()).await,
        child_journal: journal(store, key.tenant_id(), child_id).await,
    }
}

fn require_container() -> Option<String> {
    let container = match std::env::var(CONTAINER_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_ENV).is_none() => {
            return None;
        }
        Err(error) => panic!("mandatory PostgreSQL container is missing: {error}"),
    };
    assert!(
        !container.is_empty()
            && container.len() <= 64
            && container
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            && container.as_bytes()[0].is_ascii_alphanumeric(),
        "qualification container identity is invalid"
    );
    Some(container)
}

fn dump(container: &str, database: &str) -> Vec<u8> {
    let output = Command::new("docker")
        .args([
            "exec",
            container,
            "pg_dump",
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--username=postgres",
            "--dbname",
            database,
        ])
        .output()
        .expect("pinned PostgreSQL image must provide pg_dump");
    assert!(
        output.status.success(),
        "pg_dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.stdout.is_empty() && output.stdout.len() < 100 * 1024 * 1024);
    output.stdout
}

fn restore(container: &str, database: &str, archive: &[u8]) {
    let mut process = Command::new("docker")
        .args([
            "exec",
            "-i",
            container,
            "pg_restore",
            "--no-owner",
            "--no-acl",
            "--exit-on-error",
            "--single-transaction",
            "--username=postgres",
            "--dbname",
            database,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("pinned PostgreSQL image must provide pg_restore");
    process.stdin.take().unwrap().write_all(archive).unwrap();
    let output = process.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "pg_restore failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn database_url(options: &PgConnectOptions, name: &str) -> String {
    options.clone().database(name).to_url_lossy().to_string()
}

async fn create_database(pool: &sqlx_postgres::PgPool, name: &str) {
    query(&format!("CREATE DATABASE \"{name}\""))
        .execute(pool)
        .await
        .unwrap();
}

async fn drop_database(pool: &sqlx_postgres::PgPool, name: &str) {
    query(&format!("DROP DATABASE \"{name}\""))
        .execute(pool)
        .await
        .unwrap();
}

pub(super) async fn prepare_consumed_join(
    source: &PostgresStore,
    name: &str,
) -> (ChildRunKey, AttemptId) {
    let (value, request, child_id) = setup_join(source, name).await;
    let key = value.intent.key().clone();
    let activation = request.activation();
    source
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    settle(source, &value, child_id).await;
    let published = source
        .publish_child_join(&request, publish_append(source, &request).await)
        .await
        .unwrap();
    let fence = source
        .claim_lease(
            activation.tenant_id(),
            activation.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let run = source
        .load_run(activation.tenant_id(), activation.run_id())
        .await
        .unwrap();
    let started = source
        .start_node_attempt(
            worker_append(
                activation.tenant_id().clone(),
                activation.run_id(),
                EventId::generate(),
                run.journal_head().unwrap().clone(),
                fence.clone(),
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let NodeAttemptCommitOutcome::Committed { attempt, .. } = started else {
        panic!("fresh node attempt required");
    };
    let intent = PendingNodeResultIntent::new(
        activation.clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap()
    .with_child_join(published.record().head().unwrap().clone())
    .unwrap();
    source
        .succeed_node_attempt(
            worker_append(
                activation.tenant_id().clone(),
                activation.run_id(),
                EventId::generate(),
                attempt.start().journal_head().clone(),
                fence,
            ),
            &attempt.start().head(),
            intent,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    (key, attempt.start().attempt_id())
}

pub(super) async fn replay_consumed_join(restored: &PostgresStore, key: &ChildRunKey) {
    let owned = restored.load_child_run(key).await.unwrap();
    let rebuilt = declared_parent(&driver_fixture(), owned.intent().child().descriptor());
    let run = restored
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let resumed = DurableGraphDriver::new(
        restored.clone(),
        rebuilt.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .drive(
        run.lease().unwrap().fence().clone(),
        CancellationSignal::never(),
    )
    .await
    .unwrap();
    assert!(matches!(
        resumed.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(rebuilt.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rebuilt.second_calls.load(Ordering::SeqCst), 1);
    assert!(
        restored
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap()
            .superstep()
            .get()
            > 0
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn consumed_child_join_survives_isolated_backup_restore() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(container) = require_container() else {
        return;
    };
    let base: PgConnectOptions = std::env::var(DATABASE_URL_ENV)
        .expect("backup/restore qualification requires PostgreSQL")
        .parse()
        .unwrap();
    let _ = loopback_target(&base);
    let pool = sql_pool().await;
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    let identity = uuid::Uuid::now_v7().simple().to_string();
    let source_name = format!("stateknot_restore_source_{identity}");
    let target_name = format!("stateknot_restore_target_{identity}");
    create_database(&pool, &source_name).await;
    create_database(&pool, &target_name).await;
    let source_url = database_url(&base, &source_name);
    let target_url = database_url(&base, &target_name);
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_acquire_timeout(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(Duration::from_secs(5 * 60), Duration::from_secs(5 * 60));
    PostgresStore::migrate_database(&source_url, options.clone())
        .await
        .unwrap();
    let source = PostgresStore::connect(&source_url, options.clone())
        .await
        .unwrap();
    let (key, attempt_id) =
        Box::pin(prepare_consumed_join(&source, "backup-restore-child-join")).await;
    let before = Box::pin(snapshot(&source, &key, attempt_id)).await;
    assert!(!before.join["consumed"].is_null());
    assert!(!before.result.is_null());
    source.close().await;

    let archive = dump(&container, &source_name);
    let archive_digest = Digest::sha256(&archive);
    restore(&container, &target_name, &archive);
    let restored = PostgresStore::connect(&target_url, options).await.unwrap();
    let after = Box::pin(snapshot(&restored, &key, attempt_id)).await;
    assert_eq!(after, before, "restored durable Join evidence changed");
    assert!(matches!(
        restored
            .load_run(&tenant("unrelated-restore-tenant"), key.parent_run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    Box::pin(replay_consumed_join(&restored, &key)).await;
    restored.close().await;
    drop_database(&pool, &target_name).await;
    drop_database(&pool, &source_name).await;
    pool.close().await;
    println!(
        "\nSTATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE={}",
        json!({
            "profile": "child-join-logical-restore-v1",
            "source_isolated": true,
            "restore_isolated": true,
            "archive_sha256": archive_digest,
            "archive_bytes": archive.len(),
            "schema_verified": true,
            "exact_durable_snapshot_verified": true,
            "cross_tenant_refusal_verified": true,
            "noninitial_replay_verified": true,
            "invariants": "passed",
            "postgres": version,
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH
        })
    );
}
