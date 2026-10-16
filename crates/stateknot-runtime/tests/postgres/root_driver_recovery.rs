// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Fresh Drivers replay complete Root dispositions with real Agent admission.

use super::*;
use stateknot_core::{
    InterruptId, InterruptResolutionIntent, InterruptResolver, RunInterruptKind, ScopeSet,
};

async fn admit(
    store: &PostgresStore,
    fixture: &DriverFixture,
    tenant: &TenantId,
    ids: AgentRunIds,
) {
    store
        .register_graph_definition(tenant.clone(), fixture.graph.clone())
        .await
        .unwrap();
    DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(durable_admission_request(
            fixture,
            tenant.clone(),
            ids,
            fixture.graph.output_schema().clone(),
            fixture.graph.input_schema().clone(),
        ))
        .await
        .unwrap();
}

fn driver(store: &PostgresStore, fixture: &DriverFixture, quantum: u32) -> DurableGraphDriver {
    DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            quantum,
            Duration::from_secs(10),
            Duration::from_secs(15 * 60),
            3,
            Duration::from_millis(25),
        )
        .unwrap(),
    )
    .unwrap()
}

fn interrupt_fixture(id: InterruptId, principal: &PrincipalIdentity) -> DriverFixture {
    let original = driver_fixture();
    let first = NodeId::new("Step_A").unwrap();
    let second = NodeId::new("Step.B").unwrap();
    let graph = CompiledGraph::compile(
        capability("root-driver-wait-restart"),
        original.graph.input_schema().clone(),
        original.graph.state_schema().clone(),
        original.graph.update_schema().clone(),
        original.graph.output_schema().clone(),
        original.graph.reducer().clone(),
        ReadyNodes::try_new([first.clone()]).unwrap(),
        [
            GraphNode::new(
                first.clone(),
                None,
                GraphRoutes::empty(),
                Some(ReadyNodes::try_new([second.clone()]).unwrap()),
                false,
            )
            .unwrap(),
            GraphNode::new(second.clone(), None, GraphRoutes::empty(), None, true).unwrap(),
        ],
        original.graph.limits(),
    )
    .unwrap();
    let waits = NodeWaits::try_new([NodeWait::interrupt(
        id,
        RunInterruptKind::Approval,
        test_payload(),
        Digest::sha256(b"actual Root driver interrupt restart"),
        Some(principal.clone()),
        ScopeSet::default(),
        None,
    )])
    .unwrap();
    let mut registry = ExecutableGraphRegistryBuilder::new(original.registry.schemas().clone());
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: graph.reducer().clone(),
        }))
        .unwrap();
    for (node, behavior, calls) in [
        (first, TestNodeBehavior::Wait(waits), &original.first_calls),
        (
            second,
            TestNodeBehavior::Terminal(graph.output_schema().clone()),
            &original.second_calls,
        ),
    ] {
        registry
            .register_node(Arc::new(TestNodeExecutor {
                graph: graph.reference(),
                node_id: node,
                behavior,
                delay: Duration::ZERO,
                calls: Arc::clone(calls),
            }))
            .unwrap();
    }
    DriverFixture {
        graph,
        registry: registry.build().unwrap(),
        first_calls: original.first_calls,
        second_calls: original.second_calls,
    }
}

async fn resolve(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    interrupt: InterruptId,
    principal: PrincipalIdentity,
) {
    let current = store.load_run(tenant, run).await.unwrap();
    let request = store
        .load_interrupt_request(tenant, run, interrupt)
        .await
        .unwrap();
    let event = EventId::generate();
    let intent = InterruptResolutionIntent::new(
        &request,
        event,
        test_payload(),
        InterruptResolver::new(principal, ScopeSet::default()),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(tenant.clone(), run, event, test_payload()).unwrap(),
    )
    .unwrap();
    let actual = store
        .resolve_interrupt(
            append.clone(),
            current.lifecycle().revision(),
            intent.clone(),
        )
        .await
        .unwrap();
    let ack = store
        .resolve_interrupt(append, current.lifecycle().revision(), intent)
        .await
        .unwrap();
    assert_eq!(actual.record(), ack.record());
}

async fn assert_resumed(
    store: &PostgresStore,
    fixture: &DriverFixture,
    tenant: &TenantId,
    run: RunId,
    checkpoint: CheckpointId,
    resumed: &stateknot_runtime::GraphDriveResult,
) {
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = resumed.outcome() else {
        panic!("actual resumed Root outcome {:?}", resumed.outcome());
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    let expected = Box::pin(store.load_checkpoint(tenant, run, checkpoint))
        .await
        .unwrap();
    assert_eq!(handoff.plan().barrier().base_checkpoint(), &expected.head());
    assert_eq!(resumed.report().whole_replay_passes(), 1);
    assert_eq!(resumed.report().node_attempts_started(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 1);
    assert!(!store.load_run(tenant, run).await.unwrap().is_quarantined());
}

async fn commit_and_resolve_wait(
    store: &PostgresStore,
    fixture: &DriverFixture,
    tenant: &TenantId,
    ids: AgentRunIds,
    interrupt: InterruptId,
    principal: PrincipalIdentity,
    handoff: stateknot_runtime::GraphLifecycleBarrierHandoff,
) -> CheckpointId {
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry.clone(),
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let committed = lifecycle.commit_barrier(handoff.clone()).await.unwrap();
    let GraphBarrierLifecycleOutcome::Waiting(wait) = committed else {
        panic!("actual whole Root Wait must commit");
    };
    assert_eq!(wait.waits().len(), 1);
    let saved = store.load_run(tenant, ids.run_id()).await.unwrap();
    assert_eq!(saved.lifecycle().status(), RunStatus::Waiting);
    assert!(saved.lease().is_none());
    Box::pin(resolve(store, tenant, ids.run_id(), interrupt, principal)).await;
    let resolved = store.load_run(tenant, ids.run_id()).await.unwrap();
    assert_eq!(resolved.lifecycle().status(), RunStatus::Active);
    assert_eq!(resolved.unresolved_wait_count(), 0);
    assert_eq!(resolved.checkpoint(), saved.checkpoint());
    assert!(matches!(
        lifecycle.commit_barrier(handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Waiting(WaitCheckpointCommitOutcome::Idempotent { .. })
    ));
    saved.checkpoint().unwrap().checkpoint_id()
}

#[tokio::test]
async fn root_continue_yield_then_fresh_higher_fence_driver_replays_whole_bundle() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("root-driver-continue-restart");
    let ids = AgentRunIds::generate();
    Box::pin(admit(&store, &fixture, &tenant, ids)).await;
    let first = store
        .claim_lease(&tenant, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let initial = driver(&store, &fixture, 3)
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        initial.outcome(),
        GraphDriveOutcome::Yielded { .. }
    ));
    assert_eq!(initial.report().durable_events(), 3);
    assert_eq!(initial.report().whole_replay_passes(), 1);
    let saved = store.load_run(&tenant, ids.run_id()).await.unwrap();
    assert_eq!(saved.checkpoint().unwrap().superstep().get(), 1);
    assert!(saved.lease().is_none());
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let next = store
        .claim_lease(&tenant, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(next.fence().epoch() > first.fence().epoch());
    let resumed = driver(&store, &fixture, 1024)
        .drive(next.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    Box::pin(assert_resumed(
        &store,
        &fixture,
        &tenant,
        ids.run_id(),
        saved.checkpoint().unwrap().checkpoint_id(),
        &resumed,
    ))
    .await;
    store.close().await;
}

#[tokio::test]
async fn root_wait_resolution_and_original_ack_then_fresh_driver_replay() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let principal = PrincipalIdentity::new(
        "https://issuer.example.com/root-driver".parse().unwrap(),
        "approver".parse().unwrap(),
    );
    let interrupt = InterruptId::generate();
    let fixture = interrupt_fixture(interrupt, &principal);
    let tenant = tenant("root-driver-wait-restart");
    let ids = AgentRunIds::generate();
    Box::pin(admit(&store, &fixture, &tenant, ids)).await;
    let first = store
        .claim_lease(&tenant, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let initial = driver(&store, &fixture, 1024)
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = initial.into_parts().0 else {
        panic!("actual Root Wait handoff required");
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Wait { .. }
    ));
    let checkpoint = Box::pin(commit_and_resolve_wait(
        &store, &fixture, &tenant, ids, interrupt, principal, *handoff,
    ))
    .await;
    let next = store
        .claim_lease(&tenant, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(next.fence().epoch() > first.fence().epoch());
    let resumed = driver(&store, &fixture, 1024)
        .drive(next.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    Box::pin(assert_resumed(
        &store,
        &fixture,
        &tenant,
        ids.run_id(),
        checkpoint,
        &resumed,
    ))
    .await;
    store.close().await;
}
