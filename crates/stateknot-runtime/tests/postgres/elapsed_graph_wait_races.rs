// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Concurrent complete Agent/scoped writers claim one tenant-wide Wait identity.

use super::*;

struct ScopedWaitRaceCase {
    fixture: NestedFixture,
    ids: AgentRunIds,
    original: Arc<OnceLock<(Timestamp, Timestamp, NodeActivation)>>,
    pending_bytes: Vec<u8>,
    original_scope: stateknot_store_postgres::StoredActiveGraphFrame,
    lifecycle_bytes: Vec<u8>,
    first_fence: RunFence,
}

async fn prepare_scoped_wait_contender(
    store: &PostgresStore,
    tenant_id: &TenantId,
    timer_id: TimerId,
) -> ScopedWaitRaceCase {
    let original = Arc::new(OnceLock::new());
    let fixture = absolute_wait_fixture(store, true, timer_id, &original);
    let ids = AgentRunIds::generate();
    admit_nested_fixture(store, &fixture, tenant_id, ids).await;
    let defaults = DurableGraphDriverOptions::default();
    let options = DurableGraphDriverOptions::new(
        defaults.replay_limits(),
        3,
        defaults.lease_renewal_interval(),
        defaults.node_execution_timeout(),
        defaults.maximum_mutation_attempts(),
        defaults.mutation_retry_initial_delay(),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), options)
        .unwrap()
        .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
            store: store.clone(),
        }));
    let first = store
        .claim_lease(tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let result = Box::pin(driver.drive(first.clone(), CancellationSignal::never()))
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(result.report().durable_events(), 3);
    assert_eq!(result.report().node_attempts_started(), 1);
    assert_eq!(result.report().node_attempts_completed(), 1);
    let (created, due, activation) = original.get().unwrap();
    assert_eq!(due.unix_micros() - created.unix_micros(), 500_000);
    let pending = store.load_pending_node_result(activation).await.unwrap();
    assert_eq!(pending.fence(), &first);
    let run = store.load_run(tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Active);
    assert!(run.lease().is_none());
    ScopedWaitRaceCase {
        fixture,
        ids,
        original_scope: store
            .load_active_graph_frame(tenant_id, ids.run_id())
            .await
            .unwrap()
            .unwrap(),
        pending_bytes: serde_json_canonicalizer::to_vec(&pending).unwrap(),
        lifecycle_bytes: serde_json_canonicalizer::to_vec(run.lifecycle()).unwrap(),
        first_fence: first,
        original,
    }
}

#[allow(clippy::too_many_lines)]
async fn race_scoped_wait_contender(
    store: PostgresStore,
    tenant_id: TenantId,
    case: ScopedWaitRaceCase,
    ready: Arc<tokio::sync::Barrier>,
) -> bool {
    let defaults = DurableGraphDriverOptions::default();
    let options = DurableGraphDriverOptions::new(
        defaults.replay_limits(),
        2,
        defaults.lease_renewal_interval(),
        defaults.node_execution_timeout(),
        defaults.maximum_mutation_attempts(),
        defaults.mutation_retry_initial_delay(),
    )
    .unwrap();
    let driver =
        DurableGraphDriver::new(store.clone(), case.fixture.driver.registry.clone(), options)
            .unwrap()
            .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
                store: store.clone(),
            }));
    let fresh = store
        .claim_lease(&tenant_id, case.ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(fresh.epoch() > case.first_fence.epoch());
    let (created, due, activation) = case.original.get().unwrap();
    assert_eq!(due.unix_micros() - created.unix_micros(), 500_000);
    assert!(
        store
            .observe_live_lease(&fresh)
            .await
            .unwrap()
            .observed_at()
            > *due
    );
    ready.wait().await;
    let result = Box::pin(driver.drive(fresh.clone(), CancellationSignal::never())).await;
    let succeeded = match result {
        Ok(receipt) => {
            assert!(matches!(
                receipt.outcome(),
                GraphDriveOutcome::Yielded {
                    release: LeaseReleaseOutcome::Released
                }
            ));
            assert_eq!(receipt.report().durable_events(), 2);
            assert_eq!(receipt.report().frame_barriers_committed(), 1);
            assert_eq!(receipt.report().node_attempts_started(), 0);
            true
        }
        Err(GraphDriverError::Store { source }) => {
            assert!(
                matches!(*source, StoreError::InvalidWaitRegistrationBatch),
                "a losing complete WaitID contender must report its exact conflict: {source:?}"
            );
            false
        }
        other => panic!("unexpected complete Wait identity race outcome: {other:?}"),
    };
    let base = case.original_scope.checkpoint();
    let record = store
        .load_graph_frame_barrier(
            &tenant_id,
            case.ids.run_id(),
            base.frame().namespace(),
            base.checkpoint().checkpoint_id(),
        )
        .await
        .unwrap();
    assert_eq!(record.is_some(), succeeded);
    if let Some(record) = record {
        assert_eq!(
            record.wait_effect(),
            Some(stateknot_store_postgres::GraphWaitEffect::Ready)
        );
        assert_eq!(record.waits().unwrap(), Some(Vec::new()));
        assert!(record.event().recorded_at() >= *due);
    } else {
        let active = store
            .load_active_graph_frame(&tenant_id, case.ids.run_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.checkpoint().head(), base.head());
    }
    let current = store.load_run(&tenant_id, case.ids.run_id()).await.unwrap();
    assert!(!current.is_quarantined());
    assert_eq!(
        serde_json_canonicalizer::to_vec(current.lifecycle()).unwrap(),
        case.lifecycle_bytes
    );
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store.load_pending_node_result(activation).await.unwrap()
        )
        .unwrap(),
        case.pending_bytes
    );
    assert_eq!(case.fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(case.fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        store
            .load_run_quarantine(&tenant_id, case.ids.run_id())
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    if let Some(lease) = current.lease() {
        assert_eq!(lease.fence(), &fresh);
        store.release_lease(&fresh).await.unwrap();
    }
    eprintln!(
        "SCOPED_WAIT_ID_RACE_RECEIPT: {}",
        json!({"tenant":tenant_id,"run":case.ids.run_id(),"original_due":due,
            "original_activation":activation,"original_fence":case.first_fence,
            "higher_fence":fresh,"committed":succeeded,"original_pending_unchanged":true,
            "original_lifecycle_unchanged":true,"node_calls":1,"successor_calls":0,"no_quarantine":true})
    );
    succeeded
}

#[tokio::test]
async fn concurrent_scoped_elapsed_writers_claim_one_tenant_wide_wait_identity() {
    Box::pin(concurrent_scoped_wait_identity()).await;
}

async fn concurrent_scoped_wait_identity() {
    const CONTENDERS: usize = 24;
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("elapsed-scoped-identity-race");
    let timer_id = TimerId::generate();
    let mut cases = Vec::with_capacity(CONTENDERS);
    for _ in 0..CONTENDERS {
        cases.push(Box::pin(prepare_scoped_wait_contender(&store, &tenant_id, timer_id)).await);
    }
    let delay = Instant::now();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(delay.elapsed() >= Duration::from_millis(600));
    let ready = Arc::new(tokio::sync::Barrier::new(CONTENDERS));
    let mut tasks = tokio::task::JoinSet::new();
    for case in cases {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let ready = Arc::clone(&ready);
        tasks.spawn(async move {
            Box::pin(race_scoped_wait_contender(store, tenant_id, case, ready)).await
        });
    }
    let mut winners = 0;
    let mut completed = 0;
    while let Some(receipt) = tasks.join_next().await {
        winners += usize::from(receipt.unwrap());
        completed += 1;
    }
    assert_eq!(completed, CONTENDERS);
    assert_eq!(winners, 1);
    eprintln!(
        "SCOPED_WAIT_ID_RACE_COMPLETE: {}",
        json!({"tenant":tenant_id,"timer":timer_id,"contenders":CONTENDERS,"winners":winners,
            "original_due_delta_us":500_000,"actual_delay_ms":600})
    );
    store.close().await;
}

struct RootWaitRaceCase {
    fixture: NestedFixture,
    ids: AgentRunIds,
    original: Arc<OnceLock<(Timestamp, Timestamp, NodeActivation)>>,
    pending_bytes: Vec<u8>,
    lifecycle_bytes: Vec<u8>,
    first_fence: RunFence,
}

async fn prepare_root_wait_contender(
    store: &PostgresStore,
    tenant_id: &TenantId,
    timer_id: TimerId,
) -> RootWaitRaceCase {
    let original = Arc::new(OnceLock::new());
    // This is a separate legal future-only legacy producer. The elapsed scoped
    // producer retains its original 500ms due; no recovery window is extended.
    let fixture = absolute_wait_fixture_with_offset(store, false, timer_id, &original, 60_000_000);
    let ids = AgentRunIds::generate();
    admit_nested_fixture(store, &fixture, tenant_id, ids).await;
    let defaults = DurableGraphDriverOptions::default();
    let options = DurableGraphDriverOptions::new(
        defaults.replay_limits(),
        2,
        defaults.lease_renewal_interval(),
        defaults.node_execution_timeout(),
        defaults.maximum_mutation_attempts(),
        defaults.mutation_retry_initial_delay(),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), options)
        .unwrap()
        .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
            store: store.clone(),
        }));
    let first = store
        .claim_lease(tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let receipt = Box::pin(driver.drive(first.clone(), CancellationSignal::never()))
        .await
        .unwrap();
    assert!(matches!(
        receipt.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(receipt.report().durable_events(), 2);
    assert_eq!(receipt.report().node_attempts_started(), 1);
    assert_eq!(receipt.report().node_attempts_completed(), 1);
    let (created, due, activation) = original.get().unwrap();
    assert_eq!(due.unix_micros() - created.unix_micros(), 60_000_000);
    let pending = store.load_pending_node_result(activation).await.unwrap();
    assert_eq!(pending.fence(), &first);
    let run = store.load_run(tenant_id, ids.run_id()).await.unwrap();
    assert!(run.lease().is_none());
    RootWaitRaceCase {
        fixture,
        ids,
        pending_bytes: serde_json_canonicalizer::to_vec(&pending).unwrap(),
        lifecycle_bytes: serde_json_canonicalizer::to_vec(run.lifecycle()).unwrap(),
        first_fence: first,
        original,
    }
}

#[allow(clippy::too_many_lines)]
async fn race_root_wait_contender(
    store: PostgresStore,
    tenant_id: TenantId,
    timer_id: TimerId,
    case: RootWaitRaceCase,
    ready: Arc<tokio::sync::Barrier>,
) -> bool {
    let evidence = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let driver = DurableGraphDriver::new(
        store.clone(),
        case.fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(evidence.clone());

    let fresh = store
        .claim_lease(&tenant_id, case.ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(fresh.epoch() > case.first_fence.epoch());
    let (created, due, activation) = case.original.get().unwrap();
    assert_eq!(due.unix_micros() - created.unix_micros(), 60_000_000);
    ready.wait().await;
    let observation = Box::pin(driver.drive(fresh.clone(), CancellationSignal::never()))
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = observation.outcome() else {
        panic!(
            "legacy Root Wait requires its authentic handoff: {:?}",
            observation.outcome()
        )
    };
    assert_eq!(
        handoff.plan().disposition(),
        &GraphBarrierDisposition::Wait {
            waits: NodeWaits::try_new([NodeWait::timer(timer_id, RunTimerKind::Sleep, *due)])
                .unwrap()
        }
    );
    // Keep an actual legacy V1 producer in this race. The normal current
    // lifecycle adapter writes V3, which would test only new/new arbitration.
    let original_lifecycle = handoff.original_lifecycle().clone();
    let (plan, journal, event_id, _, lease) = handoff.as_ref().clone().into_parts();
    let intent = stateknot_store_postgres::RootGraphBarrierIntent::new(
        plan,
        original_lifecycle,
        event_id,
        None,
    )
    .unwrap();
    let append = stateknot_core::JournalAppend::new(
        stateknot_core::JournalExpectation::exact(journal),
        stateknot_core::JournalEventIntent::worker(
            tenant_id.clone(),
            case.ids.run_id(),
            event_id,
            lease.fence().clone(),
            intent.payload().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let committed = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        case.fixture.driver.registry.schemas(),
        &TestReducer {
            reference: case.fixture.driver.graph.reducer().clone(),
        },
    ))
    .await;
    let succeeded = match committed {
        Ok(stateknot_store_postgres::RootGraphBarrierCommitOutcome::Committed(receipt)) => {
            assert_eq!(receipt.waits().len(), 1);
            assert!(receipt.event().recorded_at() < *due);
            assert_eq!(
                receipt.event().payload().schema().version(),
                Version::new(1, 0, 0)
            );
            true
        }
        Err(StoreError::WaitRegistrationCommitConflict) => false,
        other => panic!("unexpected legacy/full elapsed identity competition: {other:?}"),
    };
    let current = store.load_run(&tenant_id, case.ids.run_id()).await.unwrap();
    assert!(!current.is_quarantined());
    let record = store
        .load_root_graph_barrier(activation.base_checkpoint())
        .await
        .unwrap();
    assert_eq!(record.is_some(), succeeded);
    if let Some(record) = record {
        assert_eq!(record.disposition(), handoff.plan().disposition());
        assert_eq!(record.waits().len(), 1);
        assert_eq!(current.lifecycle().status(), RunStatus::Waiting);
        assert!(current.lease().is_none());
        assert_eq!(
            record.event().payload().schema().version(),
            Version::new(1, 0, 0)
        );
        let timer = store
            .load_durable_timer(&tenant_id, case.ids.run_id(), timer_id)
            .await
            .unwrap();
        assert_eq!(timer.marker().due_at(), *due);
    } else {
        assert_eq!(
            serde_json_canonicalizer::to_vec(current.lifecycle()).unwrap(),
            case.lifecycle_bytes
        );
        let current_pointer = current.checkpoint().unwrap();
        assert_eq!(
            current_pointer.checkpoint_id(),
            activation.base_checkpoint().checkpoint_id()
        );
        assert_eq!(
            current_pointer.digest(),
            activation.base_checkpoint().digest()
        );
        assert_eq!(
            current_pointer.superstep(),
            activation.base_checkpoint().superstep()
        );
        assert!(matches!(
            store
                .load_durable_timer(&tenant_id, case.ids.run_id(), timer_id)
                .await,
            Err(StoreError::WaitRegistrationNotFound)
        ));
    }
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store.load_pending_node_result(activation).await.unwrap()
        )
        .unwrap(),
        case.pending_bytes
    );
    assert_eq!(case.fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(case.fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        store
            .load_run_quarantine(&tenant_id, case.ids.run_id())
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    if let Some(lease) = current.lease() {
        assert_eq!(lease.fence(), &fresh);
        store.release_lease(&fresh).await.unwrap();
    }
    eprintln!(
        "ROOT_LEGACY_WAIT_ID_RACE_RECEIPT: {}",
        json!({"tenant":tenant_id,"run":case.ids.run_id(),"wait_id":timer_id,"original_due":due,"committed":succeeded,"pending_unchanged":true,"node_calls":1,"successor_calls":0,"no_quarantine":true})
    );
    succeeded
}

#[tokio::test]
async fn legacy_registered_and_elapsed_wait_writers_compete_for_one_identity() {
    Box::pin(legacy_registered_and_elapsed_wait_competition()).await;
}

async fn legacy_registered_and_elapsed_wait_competition() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    // Both deterministic commit orders and an actual overlapping pair traverse
    // the original complete legacy writer and complete scoped V3 writer.
    for mode in ["legacy_first", "elapsed_first", "concurrent"] {
        let tenant_id = tenant("legacy-elapsed-identity");
        let timer_id = TimerId::generate();
        let legacy = Box::pin(prepare_root_wait_contender(&store, &tenant_id, timer_id)).await;
        let elapsed = Box::pin(prepare_scoped_wait_contender(&store, &tenant_id, timer_id)).await;
        let delay = Instant::now();
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(delay.elapsed() >= Duration::from_millis(600));
        let (legacy_won, elapsed_won) = match mode {
            "legacy_first" => {
                let ready = Arc::new(tokio::sync::Barrier::new(1));
                let legacy_won = Box::pin(race_root_wait_contender(
                    store.clone(),
                    tenant_id.clone(),
                    timer_id,
                    legacy,
                    Arc::clone(&ready),
                ))
                .await;
                let elapsed_won = Box::pin(race_scoped_wait_contender(
                    store.clone(),
                    tenant_id.clone(),
                    elapsed,
                    ready,
                ))
                .await;
                assert!(legacy_won);
                assert!(!elapsed_won);
                (legacy_won, elapsed_won)
            }
            "elapsed_first" => {
                let ready = Arc::new(tokio::sync::Barrier::new(1));
                let elapsed_won = Box::pin(race_scoped_wait_contender(
                    store.clone(),
                    tenant_id.clone(),
                    elapsed,
                    Arc::clone(&ready),
                ))
                .await;
                let legacy_won = Box::pin(race_root_wait_contender(
                    store.clone(),
                    tenant_id.clone(),
                    timer_id,
                    legacy,
                    ready,
                ))
                .await;
                assert!(!legacy_won);
                assert!(elapsed_won);
                (legacy_won, elapsed_won)
            }
            "concurrent" => {
                let ready = Arc::new(tokio::sync::Barrier::new(2));
                tokio::join!(
                    Box::pin(race_root_wait_contender(
                        store.clone(),
                        tenant_id.clone(),
                        timer_id,
                        legacy,
                        Arc::clone(&ready)
                    )),
                    Box::pin(race_scoped_wait_contender(
                        store.clone(),
                        tenant_id.clone(),
                        elapsed,
                        ready
                    ))
                )
            }
            _ => unreachable!(),
        };
        assert_eq!(usize::from(legacy_won) + usize::from(elapsed_won), 1);
        eprintln!(
            "LEGACY_ELAPSED_WAIT_ID_RACE_COMPLETE: {}",
            json!({"tenant":tenant_id,"wait_id":timer_id,"mode":mode,"legacy_won":legacy_won,"elapsed_won":elapsed_won,"elapsed_original_due_delta_us":500_000,"actual_delay_ms":600})
        );
    }
    store.close().await;
}
