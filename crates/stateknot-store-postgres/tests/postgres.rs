// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` migration, transaction, idempotency, and fencing tests.

use std::time::Duration;

use serde_json::json;
use sqlx_core::query::query;
use sqlx_postgres::PgPoolOptions;
use stateknot_core::{
    AgentResultProvenance, AttemptId, BoundedJson, CapabilityIdentity, CapabilityName,
    CapabilityReference, Digest, EventId, InvocationId, IssuerId, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, JournalPayload, PrincipalIdentity,
    RunId, RunTransition, SchemaId, SchemaReference, SubjectId, TenantId, ThreadId, Timestamp,
    Version,
};
use stateknot_store_postgres::{
    AdmissionOutcome, AppendOutcome, JournalPageSize, LeaseClaimOutcome, LeaseReleaseOutcome,
    LeaseRenewalOutcome, PostgresStore, PostgresStoreOptions, PostgresTransportSecurity,
    RunProjection, StoreError,
};

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";
static DATABASE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn test_store() -> Option<PostgresStore> {
    test_store_with_lease_duration(Duration::from_secs(30)).await
}

async fn test_store_with_lease_duration(lease_duration: Duration) -> Option<PostgresStore> {
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let options = test_options(lease_duration);
    PostgresStore::migrate_database(&database_url, options.clone())
        .await
        .expect("migrations must succeed");
    let store = PostgresStore::connect(&database_url, options)
        .await
        .expect("test PostgreSQL must connect with an exact schema");
    Some(store)
}

fn test_options(lease_duration: Duration) -> PostgresStoreOptions {
    PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 48)
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(lease_duration, Duration::from_secs(5 * 60))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_claim_rejects_clock_before_latest_renewal() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("claim-clock-regression");
    let run_id = RunId::generate();
    let attempt_id = AttemptId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    store
        .claim_lease(&tenant_id, run_id, attempt_id)
        .await
        .unwrap();

    let updated = query(
        "UPDATE stateknot.runs \
         SET lease_renewed_at = clock_timestamp() + interval '1 minute', \
             lease_expires_at = clock_timestamp() + interval '2 minutes' \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .execute(&administration)
    .await
    .expect("future durable renewal observation must be injected")
    .rows_affected();
    assert_eq!(updated, 1);

    assert!(matches!(
        store.claim_lease(&tenant_id, run_id, attempt_id).await,
        Err(StoreError::DatabaseClockRegression)
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_renewal_retry_confirms_only_the_already_committed_expiry() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_millis(100)).await else {
        return;
    };
    let tenant_id = tenant("renewal-expiry");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let lease = claim.lease();
    let desired_expiry = Timestamp::from_unix_micros(
        lease
            .expires_at()
            .unix_micros()
            .checked_add(100_000)
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .renew_lease(lease.fence(), desired_expiry)
            .await
            .unwrap(),
        LeaseRenewalOutcome::Renewed(_)
    ));

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        store
            .renew_lease(lease.fence(), desired_expiry)
            .await
            .unwrap(),
        LeaseRenewalOutcome::Idempotent(_)
    ));
    let later_expiry =
        Timestamp::from_unix_micros(desired_expiry.unix_micros().checked_add(100_000).unwrap())
            .unwrap();
    assert!(matches!(
        store.renew_lease(lease.fence(), later_expiry).await,
        Err(StoreError::LeaseExpired)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_after_event_insert_rolls_back_event_and_head_together() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("atomicity");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_atomic_append_rollback")
        .execute(&administration)
        .await
        .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_atomic_append_rollback CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();

    let append_result = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                1,
            ),
            RunProjection::unchanged(),
        )
        .await;

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_atomic_append_rollback")
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
    assert!(matches!(append_result, Err(StoreError::Database { .. })));

    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(run.journal_head().is_none());
    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .expect("rolled-back append must leave a valid empty journal");
    assert!(page.events().is_empty());
    assert!(!page.has_more());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_connection_refuses_an_unmigrated_database() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_schema_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated test database must be created");

    assert!(matches!(
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30))).await,
        Err(StoreError::SchemaNotMigrated)
    ));
    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("isolated database migration must succeed");
    let store = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("runtime connection must accept the exact migrated schema");
    store.close().await;

    query(&format!("DROP DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated test database must be dropped");
    administration.close().await;
}

fn database_url_with_name(database_url: &str, database_name: &str) -> String {
    let (prefix, current_database) = database_url
        .rsplit_once('/')
        .expect("test PostgreSQL URL must contain a database path");
    let query = current_database
        .find('?')
        .map_or("", |index| &current_database[index..]);
    format!("{prefix}/{database_name}{query}")
}

fn provenance(tenant_id: TenantId, run_id: RunId) -> AgentResultProvenance {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot"
            .parse::<IssuerId>()
            .unwrap(),
        "integration-registry".parse::<SubjectId>().unwrap(),
    );
    let agent = CapabilityIdentity::new(
        owner,
        CapabilityReference::new(
            CapabilityName::new("integration-agent").unwrap(),
            Version::new(1, 0, 0),
        ),
    );
    AgentResultProvenance::new(
        tenant_id,
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        agent,
    )
}

fn tenant(prefix: &str) -> TenantId {
    TenantId::new(format!("{prefix}-{}", RunId::generate())).unwrap()
}

fn payload(index: u64) -> JournalPayload {
    let schema = SchemaReference::new(
        "https://stateknot.github.io/schema/integration-event/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"stateknot integration event schema v1"),
    );
    JournalPayload::new(
        schema,
        JournalEventKind::new("integration-event").unwrap(),
        BoundedJson::try_from_value(json!({"index": index.to_string()})).unwrap(),
    )
    .unwrap()
}

fn control_append(
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    expectation: JournalExpectation,
    index: u64,
) -> JournalAppend {
    let intent =
        JournalEventIntent::control_plane(tenant_id, run_id, event_id, payload(index)).unwrap();
    JournalAppend::new(expectation, intent).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migrations_admission_projection_idempotency_and_pages() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    PostgresStore::migrate_database(&database_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration retry must be safe");
    store
        .verify_schema()
        .await
        .expect("schema verification retry must be safe");

    let tenant_id = tenant("admission");
    let run_id = RunId::generate();
    let provenance = provenance(tenant_id.clone(), run_id);
    let admitted = store
        .admit_run(provenance.clone())
        .await
        .expect("admission must commit");
    assert!(matches!(admitted, AdmissionOutcome::Committed(_)));
    let admitted_lifecycle = admitted.lifecycle().clone();
    let retry = store
        .admit_run(provenance)
        .await
        .expect("admission retry must converge");
    assert!(matches!(retry, AdmissionOutcome::Idempotent(_)));

    let started_at = Timestamp::from_unix_micros(
        admitted_lifecycle
            .admitted_at()
            .unix_micros()
            .checked_add(1)
            .unwrap(),
    )
    .unwrap();
    let active = admitted_lifecycle
        .clone()
        .apply(RunTransition::Start { started_at })
        .unwrap();
    let start_transition = RunTransition::Start { started_at };
    let future_transition = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                0,
            ),
            RunProjection::transition(
                admitted_lifecycle.revision(),
                RunTransition::Start {
                    started_at: Timestamp::MAX,
                },
            ),
        )
        .await;
    assert!(matches!(
        future_transition,
        Err(StoreError::LifecycleObservationAfterCommit)
    ));
    let unchanged = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(unchanged.lifecycle().revision().get(), 0);
    assert!(unchanged.journal_head().is_none());
    let first_event_id = EventId::generate();
    let first = control_append(
        tenant_id.clone(),
        run_id,
        first_event_id,
        JournalExpectation::empty(),
        1,
    );
    let committed = store
        .append_control_plane(
            first,
            RunProjection::transition(admitted_lifecycle.revision(), start_transition.clone()),
        )
        .await
        .expect("first event and lifecycle must commit atomically");
    assert!(matches!(committed, AppendOutcome::Committed(_)));
    assert_eq!(committed.event().sequence().get(), 1);

    let retry = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                first_event_id,
                JournalExpectation::empty(),
                1,
            ),
            RunProjection::transition(admitted_lifecycle.revision(), start_transition),
        )
        .await
        .expect("lost acknowledgement retry must converge before projection checks");
    assert!(matches!(retry, AppendOutcome::Idempotent(_)));

    let conflict = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                first_event_id,
                JournalExpectation::empty(),
                999,
            ),
            RunProjection::unchanged(),
        )
        .await;
    assert!(matches!(conflict, Err(StoreError::EventIdConflict)));

    let stale = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                2,
            ),
            RunProjection::unchanged(),
        )
        .await;
    assert!(matches!(stale, Err(StoreError::StaleJournalHead)));

    let invalid_transition = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                3,
            ),
            RunProjection::transition(active.revision(), RunTransition::Start { started_at }),
        )
        .await;
    assert!(matches!(
        invalid_transition,
        Err(StoreError::InvalidLifecycleTransition)
    ));

    let second = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                2,
            ),
            RunProjection::unchanged(),
        )
        .await
        .expect("exact successor must commit");
    assert_eq!(second.event().sequence().get(), 2);

    let first_page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(1).unwrap())
        .await
        .expect("first page must validate");
    assert_eq!(first_page.events().len(), 1);
    assert!(first_page.has_more());
    let first_cursor = first_page.events()[0].head();
    let final_page = store
        .load_journal_page(
            &tenant_id,
            run_id,
            Some(&first_cursor),
            JournalPageSize::new(10).unwrap(),
        )
        .await
        .expect("suffix page must validate to the run head");
    assert_eq!(final_page.events().len(), 1);
    assert!(!final_page.has_more());
    assert_eq!(final_page.events()[0].digest(), second.event().digest());

    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().revision().get(), 1);
    assert_eq!(stored.journal_head(), Some(&second.event().head()));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn leases_fence_late_workers_and_preserve_lost_ack_retries() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("lease");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let first_attempt = AttemptId::generate();
    let first_claim = store
        .claim_lease(&tenant_id, run_id, first_attempt)
        .await
        .unwrap();
    assert!(matches!(first_claim, LeaseClaimOutcome::Claimed(_)));
    let first_lease = first_claim.lease().clone();
    assert_eq!(first_lease.fence().epoch().get(), 1);
    let claim_retry = store
        .claim_lease(&tenant_id, run_id, first_attempt)
        .await
        .unwrap();
    assert!(matches!(claim_retry, LeaseClaimOutcome::Idempotent(_)));
    assert!(matches!(
        store
            .claim_lease(&tenant_id, run_id, AttemptId::generate())
            .await,
        Err(StoreError::LeaseHeld)
    ));

    let first_event_id = EventId::generate();
    let first_worker_append = || {
        JournalAppend::new(
            JournalExpectation::empty(),
            JournalEventIntent::worker(
                tenant_id.clone(),
                run_id,
                first_event_id,
                first_lease.fence().clone(),
                payload(10),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let first_event = store
        .append_worker(first_worker_append(), RunProjection::unchanged())
        .await
        .unwrap();

    let desired_expiry = Timestamp::from_unix_micros(
        first_lease
            .expires_at()
            .unix_micros()
            .checked_add(1_000_000)
            .unwrap(),
    )
    .unwrap();
    let renewed = store
        .renew_lease(first_lease.fence(), desired_expiry)
        .await
        .unwrap();
    assert!(matches!(renewed, LeaseRenewalOutcome::Renewed(_)));
    let renewal_retry = store
        .renew_lease(first_lease.fence(), desired_expiry)
        .await
        .unwrap();
    assert!(matches!(renewal_retry, LeaseRenewalOutcome::Idempotent(_)));

    assert_eq!(
        store.release_lease(first_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Released
    );
    assert_eq!(
        store.release_lease(first_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Idempotent
    );
    let lost_ack = store
        .append_worker(first_worker_append(), RunProjection::unchanged())
        .await
        .expect("committed event must remain observable after lease release");
    assert!(matches!(lost_ack, AppendOutcome::Idempotent(_)));

    let stale_new_event = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            first_lease.fence().clone(),
            payload(11),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker(stale_new_event, RunProjection::unchanged())
            .await,
        Err(StoreError::NoActiveLease)
    ));

    let second_claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let second_lease = second_claim.lease().clone();
    assert_eq!(second_lease.fence().epoch().get(), 2);
    assert_ne!(
        second_lease.fence().attempt_id(),
        first_lease.fence().attempt_id()
    );
    let stale_after_takeover = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            first_lease.fence().clone(),
            payload(12),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker(stale_after_takeover, RunProjection::unchanged())
            .await,
        Err(StoreError::StaleFence)
    ));

    let forced = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .expect("trusted takeover must supersede an unexpired lease");
    let forced_lease = forced.lease();
    assert_eq!(forced_lease.fence().epoch().get(), 3);
    let forced_retry = store
        .supersede_lease(&tenant_id, run_id, forced_lease.fence().attempt_id())
        .await
        .unwrap();
    assert!(matches!(forced_retry, LeaseClaimOutcome::Idempotent(_)));

    let second_stale = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            second_lease.fence().clone(),
            payload(13),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker(second_stale, RunProjection::unchanged())
            .await,
        Err(StoreError::StaleFence)
    ));

    let current = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            forced_lease.fence().clone(),
            payload(14),
        )
        .unwrap(),
    )
    .unwrap();
    let second_event = store
        .append_worker(current, RunProjection::unchanged())
        .await
        .expect("current fence must commit");
    assert_eq!(second_event.event().sequence().get(), 2);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appenders_converge_to_one_contiguous_history() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("concurrency");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for index in 0..100_u64 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        tasks.push(tokio::spawn(async move {
            let event_id = EventId::generate();
            loop {
                let run = store.load_run(&tenant_id, run_id).await.unwrap();
                let expectation = run
                    .journal_head()
                    .map_or_else(JournalExpectation::empty, |head| {
                        JournalExpectation::exact(head.clone())
                    });
                let append =
                    control_append(tenant_id.clone(), run_id, event_id, expectation, index);
                match store
                    .append_control_plane(append, RunProjection::unchanged())
                    .await
                {
                    Ok(outcome) => return outcome.event().sequence(),
                    Err(StoreError::StaleJournalHead) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected concurrent append failure: {error}"),
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("appender task must not panic");
    }

    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(128).unwrap())
        .await
        .expect("complete concurrent history must validate");
    assert_eq!(page.events().len(), 100);
    assert!(!page.has_more());
    for (index, event) in page.events().iter().enumerate() {
        assert_eq!(event.sequence().get(), u64::try_from(index).unwrap() + 1);
    }
    store.close().await;
}
