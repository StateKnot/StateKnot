// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Only fixture-owned records: two admitted runs and every event in both journals.
/// This does not claim to snapshot unrelated database tables or other tests' tenants.
#[derive(Clone)]
pub(super) struct Ledger {
    store: PostgresStore,
    runs: Arc<[(TenantId, RunId)]>,
    schema: SchemaReference,
    writes: Arc<AtomicUsize>,
}

#[derive(Debug, Eq, PartialEq)]
struct Snapshot {
    // StoredRun lacks Eq; its complete derived Debug includes every persisted projection field.
    // JournalEvent Eq below compares the full actual row/intent/payload/checksum, not just a head/count.
    run: String,
    journal: Vec<JournalEvent>,
}

fn capability() -> CapabilityIdentity {
    CapabilityIdentity::new(
        identity("ingress-ledger"),
        CapabilityReference::new(
            CapabilityName::new("bound-ingress").unwrap(),
            Version::new(1, 0, 0),
        ),
    )
}

impl Ledger {
    async fn create() -> Option<Self> {
        let url = match std::env::var("STATEKNOT_TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(std::env::VarError::NotPresent)
                if std::env::var_os("STATEKNOT_REQUIRE_POSTGRES_TESTS").is_none() =>
            {
                eprintln!("STATEKNOT_MCP_BOUND_STATE_POSTGRES_SKIPPED: no database configured");
                return None;
            }
            _ => panic!("mandatory bound-state PostgreSQL URL missing or invalid"),
        };
        let options = PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled);
        Box::pin(PostgresStore::migrate_database(&url, options.clone()))
            .await
            .unwrap();
        let store = Box::pin(PostgresStore::connect(&url, options))
            .await
            .unwrap();
        let nonce = RunId::generate();
        let tenants = [
            TenantId::new(format!("bound-ingress-a-{nonce}")).unwrap(),
            TenantId::new(format!("bound-ingress-b-{nonce}")).unwrap(),
        ];
        let runs = tenants
            .into_iter()
            .map(|tenant| (tenant, RunId::generate()))
            .collect::<Vec<_>>();
        let schema = SchemaReference::new(
            "https://schemas.example.test/bound-ingress/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"bound-ingress-fixture-schema-v1"),
        );
        let ledger = Self {
            store,
            runs: runs.into(),
            schema,
            writes: Arc::new(AtomicUsize::new(0)),
        };
        for (tenant, run) in ledger.runs.iter() {
            Box::pin(ledger.store.admit_run(AgentResultProvenance::new(
                tenant.clone(),
                *run,
                ThreadId::generate(),
                InvocationId::generate(),
                capability(),
            )))
            .await
            .unwrap();
            ledger
                .append_data(tenant, *run, json!({"kind":"baseline","tenant":tenant}))
                .await;
        }
        let initial = ledger.snapshot().await;
        assert!(initial.iter().all(|snapshot| snapshot.journal.len() == 1));
        Some(ledger)
    }
    async fn append_data(&self, tenant: &TenantId, run: RunId, data: Value) {
        let stored = Box::pin(self.store.load_run(tenant, run)).await.unwrap();
        let expectation = stored
            .journal_head()
            .cloned()
            .map_or_else(JournalExpectation::empty, JournalExpectation::exact);
        let payload = JournalPayload::new(
            self.schema.clone(),
            JournalEventKind::new("bound-ingress-observed").unwrap(),
            BoundedJson::try_from_value(data).unwrap(),
        )
        .unwrap();
        let intent =
            JournalEventIntent::control_plane(tenant.clone(), run, EventId::generate(), payload)
                .unwrap();
        Box::pin(self.store.append_control_plane(
            JournalAppend::new(expectation, intent).unwrap(),
            RunProjection::unchanged(),
        ))
        .await
        .unwrap();
    }
    pub(super) async fn append(&self, tenant: &TenantId) {
        let run = self
            .runs
            .iter()
            .find(|(candidate, _)| candidate == tenant)
            .expect("host policy chooses one of the two trusted fixture tenants")
            .1;
        self.append_data(
            tenant,
            run,
            json!({"kind":"mcp-bound-continuation","tenant":tenant}),
        )
        .await;
        self.writes.fetch_add(1, Ordering::SeqCst);
    }
    async fn snapshot(&self) -> Vec<Snapshot> {
        let mut result = Vec::new();
        for (tenant, run) in self.runs.iter() {
            let stored = Box::pin(self.store.load_run(tenant, *run)).await.unwrap();
            assert!(
                stored.checkpoint().is_none(),
                "fixture has no graph/checkpoint side ledger"
            );
            assert!(
                stored.lease().is_none(),
                "fixture uses the control-plane append contract, not a worker lease"
            );
            let mut journal = Vec::new();
            let mut cursor = None;
            loop {
                // A deliberately small page proves complete traversal rather than a head-only comparison.
                let page = Box::pin(self.store.load_journal_page(
                    tenant,
                    *run,
                    cursor.as_ref(),
                    JournalPageSize::new(2).unwrap(),
                ))
                .await
                .unwrap();
                journal.extend(page.events().iter().cloned());
                if !page.has_more() {
                    break;
                }
                cursor = page.next_cursor();
                assert!(cursor.is_some());
            }
            result.push(Snapshot {
                run: format!("{stored:?}"),
                journal,
            });
        }
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_three_consumers_rebind_refusals_preserve_two_complete_fixture_ledgers() {
    let Some(ledger) = Box::pin(Ledger::create()).await else {
        return;
    };
    let tenant_a = ledger.runs[0].0.clone();
    let tenant_b = ledger.runs[1].0.clone();
    let baseline = ledger.snapshot().await;
    let host = Host::with_ledger(Mapping::new(tenant_a.clone()), Some(ledger.clone()));
    let server = Server::start(host.clone(), Mount::Composite, true, false, false).await;
    for consumer in Mount::CONSUMERS {
        let (method, params) = consumer.operation();
        let start = host.current();
        let token = state(&server.invoke(method, params.clone()).await);
        let before = ledger.snapshot().await;
        let before_writes = ledger.writes.load(Ordering::SeqCst);
        complete(
            &server
                .invoke(method, continued(params.clone(), &token))
                .await,
        );
        let positive = ledger.snapshot().await;
        assert_eq!(ledger.writes.load(Ordering::SeqCst), before_writes + 1);
        assert_eq!(positive[0].journal.len(), before[0].journal.len() + 1);
        assert_ne!(positive[0], before[0]);
        assert_eq!(positive[1], before[1]);
        assert_eq!(positive[0].journal.last().unwrap().tenant_id(), &tenant_a);
        for change in ["tenant", "effective", "generation", "aba"] {
            let mut rebound = host.current();
            rebound.generation += 1;
            match change {
                "tenant" => rebound.tenant = tenant_b.clone(),
                "effective" => {
                    rebound.tenant = tenant_a.clone();
                    rebound.effective = identity("other-ingress-operator");
                }
                "generation" => {}
                "aba" => {
                    let mut away = rebound.clone();
                    away.tenant = tenant_b.clone();
                    host.set(away);
                    rebound.tenant = start.tenant.clone();
                    rebound.effective = start.effective.clone();
                    rebound.generation += 1;
                }
                _ => unreachable!(),
            }
            assert_eq!(rebound.scopes, start.scopes);
            assert_eq!(
                rebound
                    .principal()
                    .host_binding()
                    .unwrap()
                    .authenticated_identity(),
                start
                    .principal()
                    .host_binding()
                    .unwrap()
                    .authenticated_identity()
            );
            host.set(rebound);
            let unchanged = ledger.snapshot().await;
            let count = host.count();
            let writes = ledger.writes.load(Ordering::SeqCst);
            invalid(
                &server
                    .invoke(method, continued(params.clone(), &token))
                    .await,
            );
            assert_eq!(
                host.count(),
                count,
                "{consumer:?}/{change}: business callback ran"
            );
            assert_eq!(
                ledger.writes.load(Ordering::SeqCst),
                writes,
                "{consumer:?}/{change}: store adapter ran"
            );
            assert_eq!(
                ledger.snapshot().await,
                unchanged,
                "{consumer:?}/{change}: a complete tenant ledger changed"
            );
        }
        let mut restored = host.current();
        restored.tenant = tenant_a.clone();
        restored.effective = start.effective;
        restored.generation += 1;
        host.set(restored);
        let fresh = state(&server.invoke(method, params.clone()).await);
        complete(&server.invoke(method, continued(params, &fresh)).await);
    }
    let final_snapshot = ledger.snapshot().await;
    assert_eq!(ledger.writes.load(Ordering::SeqCst), 6);
    assert_eq!(host.0.completions.load(Ordering::SeqCst), 6);
    assert_eq!(
        final_snapshot[0].journal.len(),
        baseline[0].journal.len() + 6
    );
    assert_eq!(final_snapshot[1], baseline[1]);
    eprintln!(
        "\nSTATEKNOT_MCP_BOUND_STATE_POSTGRES: consumers=3 positive_commits=6 refused_rebinds=12 tenant_ledgers=2 complete_journals=true"
    );
}
