// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real-store evidence for read-only preparation, not durable child execution.

use super::*;
use stateknot_core::{
    ChildRunAdmissionIntent, ChildRunAdmissionIntentError, ChildRunKey, ChildRunSlot,
};
use stateknot_runtime::ChildAdmissionPreparationError;
use stateknot_store_postgres::StoredAgentAdmission;

struct PreparationFixture {
    driver: DriverFixture,
    facade: DurableAgentAdmission,
    parent: StoredAgentAdmission,
}

async fn setup(store: &PostgresStore, name: &str) -> PreparationFixture {
    let driver = driver_fixture();
    let tenant_id = tenant(name);
    store
        .register_graph_definition(tenant_id.clone(), driver.graph.clone())
        .await
        .unwrap();
    let facade = DurableAgentAdmission::new(store.clone(), driver.registry.clone()).unwrap();
    let request = durable_admission_request(
        &driver,
        tenant_id,
        AgentRunIds::generate(),
        driver.graph.output_schema().clone(),
        driver.graph.input_schema().clone(),
    );
    let parent = Box::pin(facade.admit(request))
        .await
        .unwrap()
        .stored()
        .clone();
    PreparationFixture {
        driver,
        facade,
        parent,
    }
}

impl PreparationFixture {
    fn child(&self) -> DurableAgentAdmissionRequest {
        durable_admission_request(
            &self.driver,
            self.parent
                .admission()
                .intent()
                .provenance()
                .tenant_id()
                .clone(),
            AgentRunIds::generate(),
            self.driver.graph.output_schema().clone(),
            self.driver.graph.input_schema().clone(),
        )
    }

    fn key(checkpoint: &Checkpoint) -> ChildRunKey {
        ChildRunKey::new(
            NodeActivation::for_ready_root(
                checkpoint,
                checkpoint.ready_nodes().iter().next().unwrap().clone(),
            )
            .unwrap(),
            ChildRunSlot::new("analysis").unwrap(),
        )
        .unwrap()
    }

    fn prepare(
        &self,
        child: &DurableAgentAdmissionRequest,
        now: Timestamp,
    ) -> Result<ChildRunAdmissionIntent, ChildAdmissionPreparationError> {
        self.facade.prepare_child(
            &self.parent,
            self.parent.checkpoint(),
            Self::key(self.parent.checkpoint()),
            child.intent().clone(),
            child.initial_state().clone(),
            now,
        )
    }
}

#[tokio::test]
async fn preparation_replays_candidate_identity_without_creating_child_runs() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = Box::pin(setup(&store, "child-preparation-read-only")).await;
    let clock = store.observe_database_clock().await.unwrap();
    let first = fixture.child();
    let second = fixture.child();
    let intent = fixture.prepare(&first, clock).unwrap();
    let replay = fixture.prepare(&second, clock).unwrap();
    assert_eq!(intent.spawn_digest(), replay.spawn_digest());
    assert_ne!(
        intent.child().provenance().run_id(),
        replay.child().provenance().run_id()
    );
    let restored: ChildRunAdmissionIntent =
        serde_json::from_slice(&intent.canonical_bytes().unwrap()).unwrap();
    let recreated =
        DurableAgentAdmission::new(store.clone(), fixture.driver.registry.clone()).unwrap();
    recreated
        .validate_child_preparation(
            &fixture.parent,
            fixture.parent.checkpoint(),
            &restored,
            clock,
        )
        .unwrap();
    for request in [first, second] {
        assert!(matches!(
            store
                .load_run(
                    request.intent().provenance().tenant_id(),
                    request.intent().provenance().run_id()
                )
                .await,
            Err(StoreError::RunNotFound)
        ));
    }
    let parent = store
        .load_agent_admission(intent.key().tenant_id(), intent.key().parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        parent.run().journal_head(),
        fixture.parent.run().journal_head()
    );
    assert_eq!(fixture.driver.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
async fn preparation_refuses_schema_rejection_and_unavailable_deployment() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = Box::pin(setup(&store, "child-preparation-schema")).await;
    let clock = store.observe_database_clock().await.unwrap();
    let child = fixture.child();
    let bad_state = CheckpointState::new(
        fixture.driver.graph.state_schema().clone(),
        BoundedJson::try_from(json!({"step":42})).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        fixture.facade.prepare_child(
            &fixture.parent,
            fixture.parent.checkpoint(),
            PreparationFixture::key(fixture.parent.checkpoint()),
            child.intent().clone(),
            bad_state,
            clock
        ),
        Err(ChildAdmissionPreparationError::Intent(
            ChildRunAdmissionIntentError::Schema(_)
        ))
    ));
    let intent = fixture.prepare(&child, clock).unwrap();
    let unrelated = provider_native_fixture(store.clone());
    let unavailable = DurableAgentAdmission::new(store.clone(), unrelated.registry).unwrap();
    assert!(matches!(
        unavailable.validate_child_preparation(
            &fixture.parent,
            fixture.parent.checkpoint(),
            &intent,
            clock
        ),
        Err(ChildAdmissionPreparationError::ExecutableUnavailable)
    ));
    assert!(matches!(
        store
            .load_run(
                child.intent().provenance().tenant_id(),
                child.intent().provenance().run_id()
            )
            .await,
        Err(StoreError::RunNotFound)
    ));
    store.close().await;
}

#[tokio::test]
async fn cancelled_parent_cannot_validate_an_earlier_preparation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = Box::pin(setup(&store, "child-preparation-cancelled")).await;
    let child = fixture.child();
    let intent = fixture
        .prepare(&child, store.observe_database_clock().await.unwrap())
        .unwrap();
    let live = fixture.parent.run();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("runtime.child.preparation.cancelled").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("Parent cancellation requested.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let request = RunCancellationRequest::new(failure, live.lifecycle().changed_at()).unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(live.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    intent.key().tenant_id().clone(),
                    intent.key().parent_run_id(),
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                live.lifecycle().revision(),
                RunTransition::RequestCancellation { request },
            ),
        )
        .await
        .unwrap();
    let fresh = store
        .load_agent_admission(intent.key().tenant_id(), intent.key().parent_run_id())
        .await
        .unwrap();
    assert!(matches!(
        fixture.facade.validate_child_preparation(
            &fresh,
            fixture.parent.checkpoint(),
            &intent,
            store.observe_database_clock().await.unwrap()
        ),
        Err(ChildAdmissionPreparationError::ParentNotActive)
    ));
    assert!(matches!(
        store
            .load_run(
                child.intent().provenance().tenant_id(),
                child.intent().provenance().run_id()
            )
            .await,
        Err(StoreError::RunNotFound)
    ));
    store.close().await;
}

#[tokio::test]
async fn advanced_parent_requires_current_checkpoint_and_new_activation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = Box::pin(setup(&store, "child-preparation-advanced")).await;
    let child = fixture.child();
    let intent = fixture
        .prepare(&child, store.observe_database_clock().await.unwrap())
        .unwrap();
    let lease = store
        .claim_lease(
            intent.key().tenant_id(),
            intent.key().parent_run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        driver
            .drive(lease.fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    // The driver commits the ordinary first barrier and leaves the terminal
    // barrier to lifecycle supervision. The parent remains Active on a genuine
    // noninitial checkpoint; no fabricated checkpoint or child dispatch is used.
    let fresh = store
        .load_agent_admission(intent.key().tenant_id(), intent.key().parent_run_id())
        .await
        .unwrap();
    let current = store
        .load_current_checkpoint(intent.key().tenant_id(), intent.key().parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(current.digest(), fixture.parent.checkpoint().digest());
    let clock = store.observe_database_clock().await.unwrap();
    assert!(matches!(
        fixture.facade.validate_child_preparation(
            &fresh,
            fixture.parent.checkpoint(),
            &intent,
            clock
        ),
        Err(ChildAdmissionPreparationError::CurrentCheckpointMismatch)
    ));
    assert!(matches!(
        fixture
            .facade
            .validate_child_preparation(&fresh, &current, &intent, clock),
        Err(ChildAdmissionPreparationError::Intent(
            ChildRunAdmissionIntentError::ParentCheckpointMismatch
        ))
    ));
    let next = fixture
        .facade
        .prepare_child(
            &fresh,
            &current,
            PreparationFixture::key(&current),
            child.intent().clone(),
            child.initial_state().clone(),
            clock,
        )
        .unwrap();
    assert_ne!(next.spawn_digest(), intent.spawn_digest());
    store.release_lease(lease.fence()).await.unwrap();
    store.close().await;
}
